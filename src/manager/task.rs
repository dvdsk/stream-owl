use std::collections::HashMap;

use derivative::Derivative;
use futures::FutureExt;
use futures_concurrency::future::Race;
use tokio::sync::mpsc::{error::SendError, Receiver, Sender, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::{AbortHandle, JoinSet};
use tracing::{info, trace};

use crate::stream::StreamEnded;
use crate::{ManagedStreamHandle, StreamError, StreamId, RangeCallback, BandwidthCallback, LogCallback};

use super::stream::StreamConfig;
use super::Callbacks;

mod bandwidth;
mod race_results;
use race_results::Res;

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) enum Command<F: RangeCallback> {
    AddStream {
        #[derivative(Debug = "ignore")]
        handle_tx: oneshot::Sender<ManagedStreamHandle<F>>,
        config: StreamConfig,
        url: http::Uri,
    },
    CancelStream(StreamId),
}

async fn wait_forever() -> StreamEnded {
    futures::pending!();
    unreachable!()
}

struct Tomato<L: LogCallback, B: BandwidthCallback, R: RangeCallback> {
    bandwidth_ctrl: bandwidth::Controller,

    cmd_tx: Sender<Command<R>>,
    err_tx: UnboundedSender<(StreamId, StreamError)>,
    streams: JoinSet<StreamEnded>,
    abort_handles: HashMap<StreamId, AbortHandle>,
    callbacks: Callbacks<L, B, R>,
}

impl<L: LogCallback, B: BandwidthCallback, R: RangeCallback> Tomato<L, B, R> {
    async fn add_stream(
        &mut self,
        handle_tx: oneshot::Sender<ManagedStreamHandle<R>>,
        config: StreamConfig,
        url: http::Uri,
    ) -> Result<(), crate::store::Error> {
        let cmd_tx = self.cmd_tx.clone();
        let callbacks = self.callbacks.clone();
        let (handle, stream_task) = async move {
            let id = StreamId::new();
            let (handle, stream_task) = config.start(url, cmd_tx, callbacks).await?;
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
        cmd_tx: Sender<Command<R>>,
        err_tx: UnboundedSender<(StreamId, StreamError)>,
        callbacks: Callbacks<L, B, R>,
    ) -> Self {
        let mut streams = JoinSet::new();
        streams.spawn(wait_forever());
        Self {
            cmd_tx,
            err_tx,
            streams,
            abort_handles: HashMap::new(),
            callbacks,
            bandwidth_ctrl: bandwidth::Controller,
        }
    }
}

pub(super) async fn run<L: LogCallback, B: BandwidthCallback, R: RangeCallback>(
    cmd_tx: Sender<Command<R>>,
    mut cmd_rx: Receiver<Command<R>>,
    err_tx: UnboundedSender<(StreamId, StreamError)>,
    callbacks: Callbacks<L, B, R>,
) -> super::Error {
    use Command::*;

    let mut tomato = Tomato::new(cmd_tx, err_tx, callbacks);

    loop {
        let new_cmd = cmd_rx.recv().map(Res::from);
        let stream_err = tomato.streams.join_next().map(Res::from);

        match (new_cmd, stream_err).race().await {
            Res::NewCmd(AddStream {
                handle_tx,
                config,
                url,
            }) => tomato.add_stream(handle_tx, config, url).await.unwrap(),
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
