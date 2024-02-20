use tokio::task::JoinError;

use crate::stream::StreamEnded;
use crate::{StreamId, StreamError, RangeCallback};

use super::Command;


pub(super) enum Res<R: RangeCallback> {
    StreamComplete {
        id: StreamId,
    },
    StreamError {
        id: StreamId,
        error: StreamError,
    },
    NewCmd(Command<R>),
    Dropped,
}

impl<R: RangeCallback> From<Option<Command<R>>> for Res<R> {
    fn from(value: Option<Command<R>>) -> Self {
        match value {
            Some(cmd) => Res::NewCmd(cmd),
            None => Res::Dropped,
        }
    }
}

impl<R: RangeCallback> From<Option<Result<StreamEnded, JoinError>>> for Res<R> {
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
