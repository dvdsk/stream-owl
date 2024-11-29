use crate::StreamId;

use super::AllocationInfo;
use super::Bandwidth;
use std::collections::HashMap;

#[derive(Debug)]
pub enum Change {
    Added(Bandwidth),
    Removed(Bandwidth),
}

#[derive(Debug)]
pub enum Investigation {
    StreamLimit {
        changes: HashMap<StreamId, Change>,
        investigated_stream: StreamId,
    },
    TotalLimit {
        changes: HashMap<StreamId, Change>,
    },
    /// nothing happened, any bandwidth decrease it the result of:
    ///  - Total bandwidth decreasing
    ///  - A stream getting a tighter upstream limit
    Neutral,
    /// a stream was removed or added
    /// so we cannot use this as base measurement
    Spoiled,
}

impl Investigation {
    pub(crate) fn variant(&self) -> &'static str {
        match self {
            Investigation::StreamLimit { .. } => "StreamLimit",
            Investigation::TotalLimit { .. } => "TotalLimit",
            Investigation::Neutral => "Neutral",
            Investigation::Spoiled => "Spoiled",
        }
    }

    pub(crate) fn apply(&self, allocations: &mut HashMap<StreamId, AllocationInfo>) {
        use Investigation::{StreamLimit, TotalLimit};
        let (StreamLimit { changes, .. } | TotalLimit { changes }) = self else {
            return;
        };

        for (id, change) in changes {
            let allocation = allocations
                .get_mut(id)
                .expect("allocations should not have changed since changes where made");
            match change {
                Change::Added(bandwidth) => allocation.allocated += bandwidth,
                Change::Removed(bandwidth) => allocation.allocated -= bandwidth,
            }
        }
    }

    pub(crate) fn undo(&mut self, allocations: &mut HashMap<StreamId, AllocationInfo>) {
        match self {
            Self::Neutral | Self::Spoiled => (),
            Self::StreamLimit { changes, .. } | Self::TotalLimit { changes } => {
                for (id, change) in changes {
                    let Some(allocation) = allocations.get_mut(id) else {
                        continue;
                    };
                    match change {
                        Change::Added(bw) => allocation.allocated -= *bw,
                        Change::Removed(bw) => allocation.allocated += *bw,
                    }
                }
                *self = Self::Neutral;
            }
        }
    }

    pub(crate) fn spoil(&mut self, allocations: &mut HashMap<StreamId, AllocationInfo>) {
        self.undo(allocations);
        *self = Self::Spoiled;
    }
}

#[derive(Debug)]
pub(crate) enum NextInvestigation {
    TotalBandwidth,
    StreamBandwidth,
    Neutral,
}

impl NextInvestigation {
    pub(crate) fn next(&self) -> NextInvestigation {
        match self {
            Self::TotalBandwidth => Self::StreamBandwidth,
            Self::StreamBandwidth => Self::TotalBandwidth,
            Self::Neutral => unreachable!(),
        }
    }
}
