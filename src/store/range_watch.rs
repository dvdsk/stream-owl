use std::ops::Range;

use derivative::Derivative;
use rangemap::RangeSet;
use tokio::sync::mpsc;
use tracing::{instrument, trace};

use crate::stream::{Report, ReportTx};

/// Ranges are never empty
#[derive(Debug, Clone)]
pub enum RangeUpdate {
    /* TODO: rename prev to removed <dvdsk> */
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

#[derive(Clone)]
pub(super) struct Sender {
    watch_sender: mpsc::UnboundedSender<RangeUpdate>,
    report_tx: ReportTx,
}

// initial range is 0..0
pub(super) fn channel(report_tx: ReportTx) -> (Sender, Receiver) {
    let (tx, rx) = mpsc::unbounded_channel();
    (
        Sender {
            watch_sender: tx,
            report_tx,
        },
        Receiver {
            rx,
            ranges: RangeSet::new(),
        },
    )
}

pub(super) struct StreamWasClosed;
impl Receiver {
    fn process_update(&mut self, update: RangeUpdate) -> Result<(), StreamWasClosed> {
        match update {
            RangeUpdate::Replaced { removed, new } => {
                self.ranges.remove(removed);
                self.ranges.insert(new);
            }
            RangeUpdate::Removed(range) => self.ranges.remove(range),
            RangeUpdate::Added(range) => self.ranges.insert(range),
            RangeUpdate::StreamClosed => return Err(StreamWasClosed),
        }
        Ok(())
    }

    /// blocks till at least one byte is available at `needed_pos`.
    #[instrument(level = "trace")]
    pub(super) async fn wait_for(&mut self, needed_pos: u64) -> Result<(), StreamWasClosed> {
        while let Ok(old_update) = self.rx.try_recv() {
            self.process_update(old_update)?;
        }

        while !self.ranges.contains(&needed_pos) {
            trace!("blocking read until range available");
            let Some(update) = self.rx.recv().await else {
                unreachable!("Receiver and Sender should drop at the same time")
            };
            self.process_update(update)?;
        }
        Ok(())
    }
}

impl Sender {
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
        if let Err(_) = self.report_tx.send(Report::Range(update)) {
            tracing::debug!(
                "Could not report new range to callbacks, stream must have been dropped"
            );
        }
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
