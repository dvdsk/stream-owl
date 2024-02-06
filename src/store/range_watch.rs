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
    Changed { prev: Range<u64>, new: Range<u64> },
    Removed(Range<u64>),
    Added(Range<u64>),
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

pub(super) struct PlaceHolder(
    mpsc::UnboundedReceiver<RangeUpdate>,
    std::sync::mpsc::Receiver<Report>,
);

/// Used by migrate as placeholders for the channel connected
/// to the reader and range callbacks.
// a better way would be to make the stores generic over
// whether they are being migrated too or not. However at a 
// cost of a bit more memory this works fine.
pub(super) fn placeholder() -> (Sender, PlaceHolder) {
    let (tx1, rx1) = mpsc::unbounded_channel();
    let (tx2, rx2) = std::sync::mpsc::channel();
    (
        Sender {
            watch_sender: tx1,
            report_tx: tx2,
        },
        PlaceHolder(rx1, rx2),
    )
}

impl Receiver {
    /// blocks till at least one byte is available at `needed_pos`.
    #[instrument(level = "trace")]
    pub(super) async fn wait_for(&mut self, needed_pos: u64) {
        while !self.ranges.contains(&needed_pos) {
            trace!("blocking read until range available");
            let Some(update) = self.rx.recv().await else {
                unreachable!("Receiver and Sender should drop at the same time")
            };
            match update {
                RangeUpdate::Changed { prev, new } => {
                    self.ranges.remove(prev);
                    self.ranges.insert(new);
                }
                RangeUpdate::Removed(range) => self.ranges.remove(range),
                RangeUpdate::Added(range) => self.ranges.insert(range),
            }
        }
    }
}

impl Sender {
    pub(super) fn send(&self, update: RangeUpdate) {
        tracing::trace!("sending range update: {update:?}");
        if let Err(_) = self.watch_sender.send(update.clone()) {
            tracing::debug!("Could not send new range, receiver dropped");
        } else {
            self.report_tx
                .send(Report::Range(update))
                .expect("report receiver is only closed on user callback panick")
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub(super) fn add(&self, range: Range<u64>) {
        assert!(!range.is_empty());
        self.send(RangeUpdate::Added(range));
    }

    #[instrument(level = "debug", skip(self))]
    pub(super) fn remove(&self, range: Range<u64>) {
        assert!(!range.is_empty());
        self.send(RangeUpdate::Removed(range));
    }

    #[instrument(level = "debug", skip(self))]
    pub(super) fn change(&self, prev: Range<u64>, new: Range<u64>) {
        assert!(!prev.is_empty());
        assert!(!new.is_empty());
        self.send(RangeUpdate::Changed { prev, new });
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
            self.remove(range)
        }
    }
}
