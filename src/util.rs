use core::ops::Range;
use std::future::Future;

use futures::{pin_mut, FutureExt};
use futures_concurrency::future::Race;
use tokio::select;
use tokio::sync::mpsc;

mod vecdeque;
pub(crate) use vecdeque::{vecd, VecDequeExt};

use crate::{StreamCanceld, StreamError};

pub(crate) trait RangeLen {
    fn len(&self) -> u64;
}

impl RangeLen for Range<u64> {
    fn len(&self) -> u64 {
        self.end - self.start
    }
}

pub(crate) enum MaybeLimited<T> {
    Limited(T),
    NotLimited,
}

pub(crate) async fn pausable<F>(
    task: F,
    mut pause_rx: mpsc::Receiver<bool>,
    start_paused: bool,
) -> Result<StreamCanceld, StreamError>
where
    F: Future<Output = Result<StreamCanceld, StreamError>>,
{
    enum Res {
        Pause(Option<bool>),
        Task(Result<StreamCanceld, StreamError>),
    }

    if start_paused {
        match pause_rx.recv().await {
            Some(true) => unreachable!("handle should send pause only if unpaused"),
            Some(false) => (),
            None => return Ok(StreamCanceld),
        }
    }

    pin_mut!(task);
    loop {
        let get_pause = pause_rx.recv().map(Res::Pause);
        let task_ends = task.as_mut().map(Res::Task);
        match (get_pause, task_ends).race().await {
            Res::Pause(None) => return Ok(StreamCanceld),
            Res::Pause(Some(true)) => loop {
                match pause_rx.recv().await {
                    Some(true) => unreachable!("handle should send pause only if unpaused"),
                    Some(false) => break,
                    None => return Ok(StreamCanceld),
                }
            },
            Res::Pause(Some(false)) => unreachable!("handle should send unpause only if paused"),
            Res::Task(result) => return result,
        }
    }
}
