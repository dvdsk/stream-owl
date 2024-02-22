use tokio::task::JoinError;

use crate::stream::StreamEnded;
use crate::{StreamError, StreamId};

use super::Command;

pub(super) enum Res {
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
