use std::collections::HashMap;

use derivative::Derivative;
use futures::FutureExt;
use futures_concurrency::future::Race;
use tokio::sync::mpsc::{error::SendError, Receiver, Sender, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::{AbortHandle, JoinSet};
use tracing::{info, trace};

use crate::stream::StreamEnded;
use crate::{IdBandwidthCallback, IdLogCallback, IdRangeCallback, StreamError, StreamId};

use super::stream::{ManagedHandle, ManagedHandleTrait, StreamConfig};
use super::Callbacks;

mod bandwidth;
mod race_results;
use race_results::Res;

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) enum Command {
    AddStream {
        #[derivative(Debug = "ignore")]
        handle_tx: oneshot::Sender<ManagedHandle>,
        config: StreamConfig,
        url: http::Uri,
    },
    CancelStream(StreamId),
}

async fn wait_forever() -> StreamEnded {
    futures::pending!();
    unreachable!()
}

struct State<L: IdLogCallback, B: IdBandwidthCallback, R: IdRangeCallback> {
    bandwidth: bandwidth::Controller,

    cmd_tx: Sender<Command>,
    err_tx: UnboundedSender<(StreamId, StreamError)>,
    streams: JoinSet<StreamEnded>,
    abort_handles: HashMap<StreamId, AbortHandle>,
    callbacks: Callbacks<L, bandwidth::WrappedCallback<B>, R>,
}

impl<L: IdLogCallback, B: IdBandwidthCallback, R: IdRangeCallback> State<L, B, R> {
    async fn add_stream(
        &mut self,
        handle_tx: oneshot::Sender<ManagedHandle>,
        config: StreamConfig,
        url: http::Uri,
    ) -> Result<(), crate::store::Error> {
        let id = StreamId::new();
        let cmd_tx = self.cmd_tx.clone();
        let callbacks = self.callbacks.wrap(id);
        let (handle, stream_task) = async move {
            let (handle, stream_task) = config.start(url, cmd_tx, callbacks).await?;
            let stream_task = stream_task.map(move |res| StreamEnded { res, id });
            Result::<_, crate::store::Error>::Ok((handle, stream_task))
        }
        .await?;

        let abort_handle = self.streams.spawn(stream_task);
        self.abort_handles.insert(handle.id(), abort_handle);
        let handle = ManagedHandle::from_generic(handle);
        if let Err(_) = handle_tx.send(handle) {
            trace!("add_stream canceld on user side");
            // dropping the handle here will cancel the streams task
        }

        Ok(())
    }

    fn new(
        cmd_tx: Sender<Command>,
        err_tx: UnboundedSender<(StreamId, StreamError)>,
        callbacks: Callbacks<L, B, R>,
    ) -> Self {
        let mut streams = JoinSet::new();
        streams.spawn(wait_forever());

        let Callbacks {
            retry_log,
            bandwidth,
            range,
        } = callbacks;
        let (bandwidth_control, bandwidth) = bandwidth::Controller::new(bandwidth);
        let callbacks = Callbacks {
            retry_log,
            bandwidth,
            range,
        };

        Self {
            cmd_tx,
            err_tx,
            streams,
            abort_handles: HashMap::new(),
            callbacks,
            bandwidth: bandwidth_control,
        }
    }
}

pub(super) async fn run<L: IdLogCallback, B: IdBandwidthCallback, R: IdRangeCallback>(
    cmd_tx: Sender<Command>,
    mut cmd_rx: Receiver<Command>,
    err_tx: UnboundedSender<(StreamId, StreamError)>,
    callbacks: Callbacks<L, B, R>,
) -> super::Error {
    use Command::*;

    let mut state = State::new(cmd_tx, err_tx, callbacks);

    loop {
        let new_cmd = cmd_rx.recv().map(Res::from);
        let stream_err = state.streams.join_next().map(Res::from);
        let bandwidth = state.bandwidth.wait_for_update().map(Res::Bandwidth);

        match (new_cmd, stream_err, bandwidth).race().await {
            Res::Bandwidth(update) => state.bandwidth.handle_update((), update),
            Res::NewCmd(AddStream {
                handle_tx,
                config,
                url,
            }) => state.add_stream(handle_tx, config, url).await.unwrap(),
            Res::NewCmd(CancelStream(id)) => {
                if let Some(handle) = state.abort_handles.remove(&id) {
                    handle.abort();
                }
            }
            Res::StreamError { id, error } => {
                if let Err(SendError((id, error))) = state.err_tx.send((id, error)) {
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
