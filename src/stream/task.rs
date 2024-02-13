use std::time::Duration;

use crate::http_client::RangeRefused;
use crate::http_client::RangeSupported;
use crate::http_client::Size;
use crate::http_client::StreamingClient;
use crate::network::BandwidthLim;
use crate::network::Network;
use crate::store::WriterToken;
use crate::target::StreamTarget;

use futures::FutureExt;
use tokio::sync::mpsc;
use tracing::info;
use tracing::instrument;
use tracing::warn;

use super::reporting::ReportTx;
use super::Error;
use futures_concurrency::future::Race;

mod race_results;
pub(crate) mod retry;
use race_results::*;

#[derive(Debug)]
pub enum StreamDone {
    DownloadedAll,
    Canceld,
}

// the writer will limit how fast we receive data using backpressure
#[instrument(ret, err, skip_all)]
pub(crate) async fn new(
    url: http::Uri,
    mut target: StreamTarget,
    report_tx: ReportTx,
    mut seek_rx: mpsc::Receiver<(u64, WriterToken)>,
    restriction: Option<Network>,
    bandwidth_lim: BandwidthLim,
    stream_size: Size,
    mut writer_token: WriterToken,
    mut retry: retry::Decider,
    timeout: Duration,
) -> Result<StreamDone, Error> {
    let mut client = loop {
        let receive_seek = seek_rx.recv().map(Res1::Seek);
        let build_client = StreamingClient::new(
            url.clone(),
            restriction.clone(),
            bandwidth_lim.clone(),
            stream_size.clone(),
            &mut target,
            report_tx.clone(),
            &mut retry,
            timeout,
        )
        .map(Res1::NewClient);

        match (build_client, receive_seek).race().await {
            Res1::NewClient(client) => break client?,
            Res1::Seek(None) => return Ok(StreamDone::Canceld),
            Res1::Seek(Some((pos, token))) => {
                writer_token = token;
                target.set_pos(pos)
            }
        }
    };

    loop {
        client = match client {
            StreamingClient::RangesSupported(client) => {
                let res = stream_ranges(
                    client,
                    &mut target,
                    &report_tx,
                    &mut seek_rx,
                    writer_token,
                    &mut retry,
                    timeout,
                )
                .await;
                match res {
                    RangeRes::Done => return Ok(StreamDone::DownloadedAll),
                    RangeRes::Canceld => return Ok(StreamDone::Canceld),
                    RangeRes::Err(e) => return Err(e),
                    RangeRes::RefuseRange(new_client) => StreamingClient::RangesRefused(new_client),
                }
            }
            StreamingClient::RangesRefused(client) => {
                match stream_all(
                    client,
                    &stream_size,
                    &mut seek_rx,
                    &mut target,
                    &report_tx,
                    writer_token,
                    &mut retry,
                    timeout,
                )
                .await
                {
                    AllRes::Error(e) => return Err(e),
                    AllRes::StreamDone(reason) => return Ok(reason),
                    AllRes::SeekPerformed((new_client, new_token)) => {
                        writer_token = new_token;
                        new_client
                    }
                }
            }
        };
    }
}

enum AllRes {
    Error(Error),
    SeekPerformed((StreamingClient, WriterToken)),
    StreamDone(StreamDone),
}

async fn stream_all(
    client_without_range: RangeRefused,
    stream_size: &Size,
    seek_rx: &mut mpsc::Receiver<(u64, WriterToken)>,
    target: &mut StreamTarget,
    report_tx: &ReportTx,
    mut writer_token: WriterToken,
    retry: &mut retry::Decider,
    timeout: Duration,
) -> AllRes {
    let builder = client_without_range.builder();
    let receive_seek = seek_rx.recv().map(Res2::Seek);
    let mut reader = client_without_range.into_reader();
    let write = reader
        .stream_to_writer(target, writer_token, timeout)
        .map(Res2::Write);

    let pos = match (write, receive_seek).race().await {
        Res2::Seek(Some((pos, token))) => {
            writer_token = token;
            pos
        }
        Res2::Seek(None) => return AllRes::StreamDone(StreamDone::Canceld),
        Res2::Write(Err(e)) => return AllRes::Error(Error::HttpClient(e)),
        Res2::Write(Ok(())) => {
            info!("At end of stream, waiting for seek");
            tracing::error!("Could be missing first part of the stream (very rare) if the server only stopped serving range requests half way through and we miss where seeking beyond the start when starting");
            stream_size.mark_stream_end(target.pos());
            match seek_rx.recv().await {
                Some((pos, token)) => {
                    writer_token = token;
                    pos
                }
                None => return AllRes::StreamDone(StreamDone::Canceld),
            }
        }
    };

    // do not concurrently wait for seeks, since we probably wont
    // get range support so any seek would translate to starting the stream
    // again. Which is wat we are doing here.
    let client = loop {
        let err = match builder.clone().connect(target, &report_tx, timeout).await {
            Ok(client) => break client,
            Err(err) => err,
        };
        match retry.could_succeed(err) {
            retry::CouldSucceed::Yes => retry.ready().await,
            retry::CouldSucceed::No(e) => return AllRes::Error(Error::HttpClient(e)),
        };
    };
    target.set_pos(pos);
    AllRes::SeekPerformed((client, writer_token))
}

#[derive(Debug)]
enum RangeRes {
    Done,
    Canceld,
    Err(Error),
    RefuseRange(RangeRefused),
}

#[instrument(level = "debug", skip(client, target, seek_rx, report_tx), ret)]
async fn stream_ranges(
    mut client: RangeSupported,
    target: &mut StreamTarget,
    report_tx: &ReportTx,
    seek_rx: &mut mpsc::Receiver<(u64, WriterToken)>,
    mut writer_token: WriterToken,
    retry: &mut retry::Decider,
    timeout: Duration,
) -> RangeRes {
    loop {
        let client_builder = client.builder();

        let next_seek = loop {
            let stream = process_one_stream(client, target, writer_token, timeout).map(Into::into);
            let get_seek = seek_rx.recv().map(Res3::Seek);
            client = match (stream, get_seek).race().await {
                Res3::Seek(None) => return RangeRes::Canceld,
                Res3::Seek(Some(seek)) => break Some(seek),
                Res3::StreamRangesSupported(client) => client,
                Res3::StreamRangesRefused(client) => {
                    warn!("Got not seekable stream");
                    return RangeRes::RefuseRange(client);
                }
                Res3::StreamDone => return RangeRes::Done,
                Res3::StreamError(Error::HttpClient(e)) => match retry.could_succeed(e) {
                    retry::CouldSucceed::Yes => {
                        retry.ready().await;
                        break None;
                    }
                    retry::CouldSucceed::No(e) => {
                        return RangeRes::Err(Error::HttpClient(e));
                    }
                },
                Res3::StreamError(unrecoverable_err) => {
                    return RangeRes::Err(unrecoverable_err);
                }
            }
        };

        if let Some((pos, token)) = next_seek {
            writer_token = token;
            target.set_pos(pos);
        }

        client = loop {
            let get_seek = seek_rx.recv().map(Res4::Seek);
            let get_client_at_new_pos = client_builder
                .clone()
                .connect(target, report_tx, timeout)
                .map(Into::into);

            match (get_client_at_new_pos, get_seek).race().await {
                Res4::Seek(None) => return RangeRes::Canceld,
                Res4::Seek(Some((pos, token))) => {
                    writer_token = token;
                    target.set_pos(pos)
                }
                Res4::GetClientError(e) => return RangeRes::Err(e),
                Res4::GotRangesSupported(client) => break client,
                Res4::GotRangesRefused(client) => return RangeRes::RefuseRange(client),
            }
        }
    }
}

#[instrument(level = "debug", skip_all)]
async fn process_one_stream(
    client: RangeSupported,
    target: &mut StreamTarget,
    writer_token: WriterToken,
    timeout: Duration,
) -> Result<Option<StreamingClient>, Error> {
    let mut reader = client.into_reader();
    reader
        .stream_to_writer(target, writer_token, timeout)
        .await
        .map_err(Error::HttpClient)?;

    let Some(next_range) = target.next_range(&reader.stream_size()).await else {
        info!("at end of stream: returning");
        return Ok(None);
    };

    let res = reader
        .try_into_client()
        .expect("should not read less then we requested")
        .try_get_range(next_range, timeout)
        .await;

    match res {
        Ok(new_client) => Ok(Some(new_client)),
        Err(e) => return Err(Error::HttpClient(e)),
    }
}
