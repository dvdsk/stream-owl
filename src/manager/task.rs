use std::collections::HashMap;

use futures::FutureExt;
use futures_concurrency::future::Race;
use tokio::sync::mpsc::{error::SendError, Receiver, Sender, UnboundedSender};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{AbortHandle, JoinSet};
use tracing::{info, trace};

use crate::stream::StreamEnded;
use crate::{stream, ManagedStreamHandle, StreamError, StreamId};

use super::stream::StreamConfig;
use super::Callbacks;

mod race_results;
use race_results::Res;

pub(crate) enum Command {
    AddStream {
        handle_tx: oneshot::Sender<ManagedStreamHandle>,
        config: StreamConfig,
        url: http::Uri,
    },
    CancelStream(StreamId),
}

async fn wait_forever() -> StreamEnded {
    futures::pending!();
    unreachable!()
}

struct Tomato {
    cmd_tx: Sender<Command>,
    err_tx: UnboundedSender<(StreamId, StreamError)>,
    report_tx: mpsc::Sender<stream::Report>,
    streams: JoinSet<StreamEnded>,
    abort_handles: HashMap<StreamId, AbortHandle>,
}

impl Tomato {
    async fn add_stream(
        &mut self,
        handle_tx: oneshot::Sender<ManagedStreamHandle>,
        config: StreamConfig,
        url: http::Uri,
    ) -> Result<(), crate::store::Error> {
        let cmd_tx = self.cmd_tx.clone();
        let report_tx = self.report_tx.clone();
        let (handle, stream_task) = async move {
            let id = StreamId::new();
            let (handle, stream_task) = config.start(url, cmd_tx, report_tx).await?;
            let stream_task = stream_task.map(move |res| StreamEnded { res, id });
            Result::<_, crate::store::Error>::Ok((handle, stream_task))
        }
        .await?;

        let abort_handle = self.streams.spawn(stream_task);
        self.abort_handles.insert(handle.id(), abort_handle);
        if let Err(_) = handle_tx.send(handle) {
            trace!("add_stream canceld on user side");
            // dropping the handle here will cancel the streams task
        }

        Ok(())
    }

    fn new(
        cmd_tx: Sender<Command>,
        err_tx: UnboundedSender<(StreamId, StreamError)>,
    ) -> (Receiver<stream::Report>, Self) {
        let (report_tx, mut report_rx) = mpsc::channel(12);
        let mut streams = JoinSet::new();
        streams.spawn(wait_forever());
        let mut abort_handles = HashMap::new();

        (
            report_rx,
            Self {
                cmd_tx,
                err_tx,
                report_tx,
                streams,
                abort_handles,
            },
        )
    }
}

pub(super) async fn run(
    cmd_tx: Sender<Command>,
    mut cmd_rx: Receiver<Command>,
    err_tx: UnboundedSender<(StreamId, StreamError)>,
    callbacks: Callbacks,
) -> super::Error {
    use Command::*;

    let (mut report_rx, mut tomato) = Tomato::new(cmd_tx, err_tx);

    loop {
        let new_report = report_rx.recv().map(Res::from);
        let new_cmd = cmd_rx.recv().map(Res::from);
        let stream_err = tomato.streams.join_next().map(Res::from);

        match (new_cmd, stream_err, new_report).race().await {
            Res::StreamReport { id, report } => todo!(),
            Res::NewCmd(AddStream {
                handle_tx,
                config,
                url,
            }) => tomato.add_stream(
                handle_tx,
                config,
                url,
            )
            .await
            .unwrap(),
            Res::NewCmd(CancelStream(id)) => {
                if let Some(handle) = tomato.abort_handles.remove(&id) {
                    handle.abort();
                }
            }
            Res::StreamError { id, error } => {
                if let Err(SendError((id, error))) = tomato.err_tx.send((id, error)) {
                    tracing::error!("stream {id:?} ran into an error, it could not be send to API user as the error stream receive part has been dropped. Error was: {error:?}")
                }
            }
            Res::StreamComplete { id } => {
                info!("stream: {id:?} completed")
            }
            Res::Dropped => (),
        }
    }
}
