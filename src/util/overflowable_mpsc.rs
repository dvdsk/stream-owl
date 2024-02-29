use core::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use thiserror;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;

#[derive(Debug, Clone)]
pub struct Sender<T: fmt::Debug> {
    sender: mpsc::Sender<T>,
    missed: Arc<AtomicUsize>,
}

impl<T: fmt::Debug> Sender<T> {
    pub(crate) fn try_send(&mut self, val: T) -> Result<(), TrySendError<T>> {
        let res = self.try_send(val);
        if let Err(TrySendError::Full(_)) = res {
            self.missed.fetch_add(1, Ordering::Relaxed);
            Ok(())
        } else {
            res
        }
    }
}

#[derive(Debug)]
pub struct Receiver<T> {
    sender: mpsc::Receiver<T>,
    missed: Arc<AtomicUsize>,
}

#[derive(thiserror::Error, Debug)]
pub enum TryRecvError<T> {
    #[error(transparent)]
    Inner(#[from] mpsc::error::TryRecvError),
    #[error("Queue filled up, missed: {n_missed} messages")]
    Missed { n_missed: usize, message: T },
}

impl<T> Receiver<T> {
    pub(crate) fn try_recv(&mut self) -> Result<T, TryRecvError<T>> {
        if let Ok(message) = self.try_recv() {
            let n_missed = self.missed.swap(0, Ordering::Relaxed);
            if n_missed > 0 {
                Err(TryRecvError::Missed { n_missed, message })
            } else {
                Ok(message)
            }
        } else {
            Ok(self
                .sender
                .blocking_recv()
                .ok_or(tokio::sync::mpsc::error::TryRecvError::Disconnected)?)
        }
    }
}
