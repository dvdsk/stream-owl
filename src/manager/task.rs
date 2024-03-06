use derivative::Derivative;
use futures::FutureExt;
use futures_concurrency::future::Race;
use std::collections::HashMap;
use tokio::sync::mpsc::{error::SendError, Receiver, Sender, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::{AbortHandle, JoinSet};
use tracing::{info, trace};

use crate::stream::{self, StreamEnded};
use crate::{IdBandwidthCallback, IdLogCallback, IdRangeCallback, StreamError, StreamId};

use super::handle::HandleOp;
use super::stream::StreamConfig;
use super::{CallWithId, Callbacks, ManagedHandle};

pub(crate) mod bandwidth;
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
    Handle(HandleOp),
}

async fn wait_forever() -> StreamEnded {
    futures::pending!();
    unreachable!()
}

pub(crate) struct State<L: IdLogCallback, B: IdBandwidthCallback, R: IdRangeCallback> {
    bandwidth: bandwidth::Controller,

    cmd_tx: Sender<Command>,
    err_tx: UnboundedSender<(StreamId, StreamError)>,
    stream_tasks: JoinSet<StreamEnded>,
    stream_handles: HashMap<StreamId, (AbortHandle, stream::Handle<CallWithId<R>>)>,
    callbacks: Callbacks<L, B, R>,
}

impl<L, B, R> State<L, B, R>
where
    L: IdLogCallback,
    B: IdBandwidthCallback,
    R: IdRangeCallback,
{
    async fn add_stream(
        &mut self,
        handle_tx: oneshot::Sender<ManagedHandle>,
        config: StreamConfig,
        url: http::Uri,
    ) -> Result<(), crate::store::Error> {
        let id = StreamId::new();
        let cmd_tx = self.cmd_tx.clone();
        let callbacks = self.callbacks.wrap(id);
        let (config, alloc_guard) = self
            .bandwidth
            .register(id, config, &mut self.stream_handles)
            .await;
        let (handle, stream_task) = async move {
            let (handle, stream_task) = config.start(url, cmd_tx, callbacks).await?;
            let stream_task = stream_task
                .inspect(move |_| drop(alloc_guard)) // free up the allocated bandwidth
                .map(move |res| StreamEnded { res, id });
            Result::<_, crate::store::Error>::Ok((handle, stream_task))
        }
        .await?;

        let abort_handle = self.stream_tasks.spawn(stream_task);
        self.stream_handles.insert(id, (abort_handle, handle));
        if let Err(_) = handle_tx.send(ManagedHandle {
            id,
            cmd_tx: self.cmd_tx.clone(),
            bandwidth_tx: self.bandwidth.get_tx(),
        }) {
            trace!("add_stream canceld on user side");
            // dropping the handle here will cancel the streams task
        }

        Ok(())
    }

    pub(crate) fn new(
        cmd_tx: Sender<Command>,
        err_tx: UnboundedSender<(StreamId, StreamError)>,
        bandwidth_ctrl: bandwidth::Controller,
        callbacks: Callbacks<L, B, R>,
    ) -> Self {
        let mut stream_tasks = JoinSet::new();
        stream_tasks.spawn(wait_forever());

        Self {
            cmd_tx,
            err_tx,
            stream_tasks,
            stream_handles: HashMap::new(),
            callbacks,
            bandwidth: bandwidth_ctrl,
        }
    }

    async fn handle_op(&mut self, op: HandleOp) {
        let Some((_, handle)) = self.stream_handles.get_mut(op.id()) else {
            assert!(
                op.tx_closed(),
                "ManagedHandle can not be dropped if tx is still open"
            );
            return;
        };

        // handle could be dropped while operation is running
        let _ignore_err = match op {
            HandleOp::Pause { tx, .. } => tx.send(handle.pause().await),
            HandleOp::Unpause { tx, .. } => tx.send(handle.unpause().await),
            HandleOp::TryGetReader { tx, .. } => tx.send(handle.try_get_reader()).map_err(|_| ()),
            HandleOp::MigrateToLimitedMemBackend { tx, size, .. } => tx
                .send(handle.migrate_to_limited_mem_backend(size).await)
                .map_err(|_| ()),
            HandleOp::MigrateToUnlimitedMemBackend { tx, .. } => tx
                .send(handle.migrate_to_unlimited_mem_backend().await)
                .map_err(|_| ()),
            HandleOp::MigrateToDiskBackend { path, tx, .. } => tx
                .send(handle.migrate_to_disk_backend(path).await)
                .map_err(|_| ()),
            HandleOp::Flush { tx, .. } => tx.send(handle.flush().await).map_err(|_| ()),
        };
    }
}

pub(super) async fn run<L: IdLogCallback, B: IdBandwidthCallback, R: IdRangeCallback>(
    mut state: State<L, B, R>,
    mut cmd_rx: Receiver<Command>,
) -> super::Error {
    use Command::*;

    loop {
        let new_cmd = cmd_rx.recv().map(Res::from);
        let stream_err = state.stream_tasks.join_next().map(Res::from);
        let bandwidth = state.bandwidth.wait_for_update().map(Res::Bandwidth);

        match (new_cmd, stream_err, bandwidth).race().await {
            Res::Bandwidth(update) => {
                state
                    .bandwidth
                    .handle_update(&mut state.stream_handles, update)
                    .await
            }
            Res::NewCmd(AddStream {
                handle_tx,
                config,
                url,
            }) => state.add_stream(handle_tx, config, url).await.unwrap(),
            Res::NewCmd(CancelStream(id)) => {
                state.bandwidth.remove(id, &mut state.stream_handles).await;
                if let Some((abort, _)) = state.stream_handles.remove(&id) {
                    abort.abort();
                }
            }
            Res::NewCmd(Handle(op)) => state.handle_op(op).await,
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
