use std::ops::Range;

use derivative::Derivative;
use rangemap::RangeSet;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{debug, instrument, trace};

use crate::RangeCallback;

/// Ranges are never empty
#[derive(Debug, Clone)]
pub enum RangeUpdate {
    Replaced {
        removed: Range<u64>,
        new: Range<u64>,
    },
    Removed(Range<u64>),
    Added(Range<u64>),
    StreamClosed,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Receiver {
    #[derivative(Debug = "ignore")]
    rx: mpsc::UnboundedReceiver<RangeUpdate>,
    #[derivative(Debug = "ignore")]
    ranges: RangeSet<u64>,
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(super) struct Sender<R: RangeCallback> {
    watch_sender: mpsc::UnboundedSender<RangeUpdate>,
    #[derivative(Debug = "ignore")]
    range_callback: R,
}

// initial range is 0..0
pub(super) fn channel<R: RangeCallback>(range_callback: R) -> (Sender<R>, Receiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    (
        Sender {
            watch_sender: tx,
            range_callback,
        },
        Receiver {
            rx,
            ranges: RangeSet::new(),
        },
    )
}

/// Stream closed while requested range is not present
/// Data will never be received
pub(super) struct StreamWasClosed;
impl Receiver {
    fn process_update(&mut self, update: RangeUpdate) {
        match update {
            RangeUpdate::Replaced { removed, new } => {
                self.ranges.remove(removed);
                self.ranges.insert(new);
            }
            RangeUpdate::Removed(range) => self.ranges.remove(range),
            RangeUpdate::Added(range) => self.ranges.insert(range),
            RangeUpdate::StreamClosed => (),
        }
    }

    /// blocks till at least one byte is available at `needed_pos`.
    #[instrument(level = "trace")]
    pub(super) async fn wait_for(&mut self, needed_pos: u64) -> Result<(), StreamWasClosed> {
        loop {
            let old_update = match self.rx.try_recv() {
                Ok(old_update) => old_update,
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            };
            self.process_update(old_update);
        }

        debug!("Waiting for data at {needed_pos} to become available");
        while !self.ranges.contains(&needed_pos) {
            trace!("blocking read until range available");
            let Some(update) = self.rx.recv().await else {
                return Err(StreamWasClosed);
            };
            self.process_update(update);
        }
        Ok(())
    }
}

impl<R: RangeCallback> Sender<R> {
    pub(super) fn send(&self, update: RangeUpdate) {
        match &update {
            RangeUpdate::Replaced { removed: prev, new } => {
                assert!(!prev.is_empty() && !new.is_empty())
            }
            RangeUpdate::Removed(r) => assert!(!r.is_empty()),
            RangeUpdate::Added(r) => assert!(!r.is_empty()),
            RangeUpdate::StreamClosed => (),
        }

        tracing::trace!("sending range update: {update:?}");
        if let Err(_) = self.watch_sender.send(update.clone()) {
            tracing::debug!("Could not send new range, receiver dropped");
        }
        self.range_callback.perform(update);
    }

    #[instrument(level = "debug", skip(self))]
    pub(super) fn send_diff(&self, mut prev: RangeSet<u64>, new: RangeSet<u64>) {
        tracing::trace!("sending range change: {prev:?}->{new:?}");
        // new is a subset of prev
        let to_remove = {
            for range in new.into_iter() {
                prev.remove(range);
            }
            prev
        };

        for range in to_remove {
            self.send(RangeUpdate::Removed(range))
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub(super) fn close(&mut self) {
        self.send(RangeUpdate::StreamClosed)
    }
}
