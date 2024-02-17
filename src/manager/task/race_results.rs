use tokio::task::JoinError;

use crate::stream::{StreamEnded, Report};
use crate::{StreamId, StreamError};

use super::Command;


pub(super) enum Res {
    StreamReport {
        id: StreamId,
        report: Report,
    },
    StreamComplete {
        id: StreamId,
    },
    StreamError {
        id: StreamId,
        error: StreamError,
    },
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

impl From<Option<Report>> for Res {
    fn from(report: Option<Report>) -> Self {
        let report = report.expect("report_rx is in the same scope as the recv call");
        Res::StreamReport { id: todo!(), report }
    }
}
