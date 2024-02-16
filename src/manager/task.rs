use std::collections::HashMap;

use futures::FutureExt;
use futures_concurrency::future::Race;
use tokio::sync::mpsc::{error::SendError, Receiver, Sender, UnboundedSender};
use tokio::sync::oneshot;
use tokio::task::{AbortHandle, JoinError, JoinSet};
use tracing::{info, trace, warn};

use crate::stream::{StreamBuilder, StreamEnded};
use crate::{stream, ManagedStreamHandle, StreamError, StreamId};

pub(crate) enum Command {
    AddStream {
        handle_tx: oneshot::Sender<ManagedStreamHandle>,
        builder: StreamBuilder<true>,
        id: StreamId,
    },
    CancelStream(StreamId),
}

enum Res {
    StreamComplete { id: StreamId },
    StreamError { id: StreamId, error: StreamError },
    NewCmd(Command),
    Dropped,
}

impl From<Option<Command>> for Res {
    fn from(value: Option<Command>) -> Self {
        match value {
            Some(cmd) => Res::NewCmd(cmd),
            None => Res::Dropped,
        }
    }
}

impl From<Option<Result<StreamEnded, JoinError>>> for Res {
    fn from(value: Option<Result<StreamEnded, JoinError>>) -> Self {
        let StreamEnded { id, res } = value
            .expect("streams JoinSet should never be empty")
            .expect("stream should never panic");
        if let Err(error) = res {
            Res::StreamError { id, error }
        } else {
            Res::StreamComplete { id }
        }
    }
}

async fn wait_forever() -> StreamEnded {
    futures::pending!();
    unreachable!()
}

pub(super) async fn run(
    cmd_tx: Sender<Command>,
    mut cmd_rx: Receiver<Command>,
    err_tx: UnboundedSender<(StreamId, StreamError)>,
) -> super::Error {
    use Command::*;

    let mut streams = JoinSet::new();
    streams.spawn(wait_forever());
    let mut abort_handles = HashMap::new();

    loop {
        let new_cmd = cmd_rx.recv().map(Res::from);
        let stream_err = streams.join_next().map(Res::from);

        match (new_cmd, stream_err).race().await {
            Res::NewCmd(AddStream {
                id,
                handle_tx,
                builder,
            }) => add_stream(
                &mut streams,
                &mut abort_handles,
                handle_tx,
                cmd_tx.clone(),
                id,
                builder,
            )
            .await
            .unwrap(),
            Res::NewCmd(CancelStream(id)) => {
                if let Some(handle) = abort_handles.remove(&id) {
                    handle.abort();
                }
            }
            Res::StreamError { id, error } => {
                if let Err(SendError((id, error))) = err_tx.send((id, error)) {
                    warn!("stream {id:?} ran into an error, it could not be send to API user as the error stream receive part has been dropped. Error was: {error:?}")
                }
            }
            Res::StreamComplete { id } => {
                info!("stream: {id:?} completed")
            }
            Res::Dropped => (),
        }
    }
}

async fn add_stream(
    streams: &mut JoinSet<stream::StreamEnded>,
    abort_handles: &mut HashMap<StreamId, AbortHandle>,
    handle_tx: oneshot::Sender<ManagedStreamHandle>,
    cmd_tx: Sender<Command>,
    id: StreamId,
    builder: StreamBuilder<true>,
) -> Result<(), crate::store::Error> {
    let (handle, stream_task) = async move {
        let (handle, stream_task) = builder.start().await?;
        let stream_task = stream_task.map(move |res| StreamEnded { res, id });
        let handle = ManagedStreamHandle {
            cmd_manager: cmd_tx,
            handle,
        };
        Result::<_, crate::store::Error>::Ok((handle, stream_task))
    }
    .await?;

    let abort_handle = streams.spawn(stream_task);
    abort_handles.insert(handle.id(), abort_handle);
    if let Err(_) = handle_tx.send(handle) {
        trace!("add_stream canceld on user side");
        // dropping the handle here will cancel the streams task
    }

    Ok(())
}
