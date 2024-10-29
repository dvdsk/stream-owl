use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;

use tokio::task::AbortHandle;

use super::allocation::Bandwidth;
use super::{divide, BandwidthInfo, Change, Controller, Investigation, Priority};
use crate::manager::task::bandwidth::allocation::{AllocationInfo, Limit};
use crate::manager::task::bandwidth::NextInvestigation;
use crate::{RangeCallback, StreamHandle, StreamId};

impl Controller {
    pub(super) fn bandwidth_update(&mut self, stream: StreamId, new: usize) {
        if let Investigation::Spoiled = self.investigation {
            return;
        }

        self.bandwidth_by_id
            .entry(stream)
            .and_modify(|bandwidth| {
                let limit_was = self
                    .allocations
                    .get(&stream)
                    .expect("if its in bandwidth_by_id it must be in a list")
                    .curr_io_limit;
                bandwidth.since_last_sweep.push(new as Bandwidth);
                bandwidth.newest_update = Instant::now();

                let distance = new as f32 / limit_was as f32;
                let distance = distance.max(1.0);
                bandwidth.steadyness = bandwidth.steadyness * 0.9 + distance * 0.1;
                bandwidth.steadyness = bandwidth.steadyness.max(1.0);
            })
            .or_insert(BandwidthInfo {
                // Initially low as a new stream is expected to rapidly change
                // bandwidth
                steadyness: 0.5,
                newest_update: Instant::now(),
                since_last_sweep: Vec::new(),
                prev_normal_sweep: None,
                // is updated at the start of the sweep
                this_sweep: 0,
            });
    }

    pub(super) async fn sweep<R: RangeCallback>(
        &mut self,
        handles: &mut HashMap<StreamId, (AbortHandle, StreamHandle<R>)>,
    ) {
        use Investigation as I;
        if self
            .bandwidth_by_id
            .iter()
            .any(|(_, bw)| bw.newest_update < self.last_sweep)
        {
            return;
        }

        let bw_tomato: HashMap<_, _> = self
            .bandwidth_by_id
            .iter_mut()
            .map(|(id, info)| {
                (
                    id,
                    info.since_last_sweep.drain(..).sum::<Bandwidth>()
                        / info.since_last_sweep.len().max(1) as Bandwidth,
                )
            })
            .collect();
        let prev_bw = self.previous_sweeps_bw.front().copied();
        let curr_bw: Bandwidth = bw_tomato.values().sum();

        match &mut self.investigation {
            I::Spoiled => return,
            I::StreamLimit { ref stream, .. } => {
                if let Some((bw, allocation)) = self
                    .bandwidth_by_id
                    .get(stream)
                    .zip(self.allocations.get_mut(stream))
                {
                    if bw.newest_update > self.last_sweep {
                        if let Some(prev_normal_sweep) = bw.prev_normal_sweep {
                            if prev_normal_sweep > bw.this_sweep {
                                allocation.upstream_limit = Limit::Guess(bw.this_sweep);
                            } else {
                                allocation.upstream_limit = Limit::Unknown;
                            }
                        }
                    }
                }
            }
            I::TotalLimit { .. } => {
                // only note increases, a decrease could be because of
                // a stream limit
                if Some(curr_bw) > prev_bw {
                    self.previous_sweeps_bw.push_front(curr_bw);
                    self.previous_sweeps_bw.truncate(5);
                }
            }
            I::Neutral => {
                self.previous_sweeps_bw.push_front(curr_bw);
                self.previous_sweeps_bw.truncate(5);

                if Some(curr_bw) < prev_bw {
                    let lost = prev_bw.expect("if guards this") - curr_bw;
                    self.remove_bw_at_and_below(Priority::highest(), lost);
                }

                match self.next_investigation.next() {
                    NextInvestigation::TotalBandwidth => {
                        if self.bandwidth_lim.got_space(curr_bw) {
                            self.investigate_total_limit();
                        }
                    }
                    NextInvestigation::StreamBandwidth => {
                        self.investigate_stream_limit();
                    }
                }

                for bw in self.bandwidth_by_id.values_mut() {
                    bw.prev_normal_sweep = Some(bw.this_sweep);
                }
            }
        }

        self.investigation.undo(&mut self.allocations);
        self.apply_new_limits(handles).await;
    }

    /// grow faster if the previous few increase was achieved
    /// if we just started double the increase
    fn optimal_total_bw_perbutation(&self) -> Bandwidth {
        let after = self
            .previous_sweeps_bw
            .get(1)
            .copied()
            .expect("just set in fn sweep");
        let Some(before) = self.previous_sweeps_bw.get(2).copied() else {
            return after / 1;
        };

        let target_increase = self
            .previous_total_bw_perbutation
            .expect("if before is Some then this is the second time this fn is called");

        if before > after {
            return after / 10;
        }

        let increase = 0.9 * (after - before) as f32;
        if increase as u32 >= target_increase {
            return target_increase * 2;
        } else {
            return target_increase / 2;
        }
    }

    /// make a small increase to the bandwidth division to find out
    /// if the total available bandwidth has increased
    pub(super) fn investigate_total_limit(&mut self) {
        let perbutation = self.optimal_total_bw_perbutation();
        self.previous_total_bw_perbutation = Some(perbutation);

        // want each stream to get some bit of extra bandwidth, how much
        // is determined by its growability and steadyness. If its more
        // steady it gets more allocated
        let (changes, _left_over) = divide::spread_prioritize_steadiest(
            perbutation,
            &self.bandwidth_by_id,
            &mut self.allocations,
        );

        let changes = changes
            .into_iter()
            .map(|(id, amount)| (id, Change::Added(amount)))
            .collect();
        self.investigation = Investigation::TotalLimit { changes };
        self.investigation.apply(&mut self.allocations);
    }

    /// make a small perbutation to the bandwidth division to find
    /// if a stream has more upstream bandwidth
    pub(super) fn investigate_stream_limit(&mut self) {
        assert!(self.allocations.len() > 0);

        // try every stream while finding the next stream to check
        let to_check: Option<&mut AllocationInfo> = None;
        for _ in 0..self.orderd_streamids.len() {
            let index = (self.last_index_checked + 1) % self.allocations.len();
            self.last_index_checked = index;

            let stream_id = self.orderd_streamids[index];
            let to_check = self.allocations.get_mut(&stream_id);

            if to_check.is_some() {
                break;
            }
        }

        let Some(to_check) = to_check else {
            return;
        };

        let five_percent_of_allocation = (to_check.allocated as f32 * 0.05) as Bandwidth;
        let perbutation = five_percent_of_allocation;
        let list = self.allocations.iter().map(|(_, info)| info);

        let mut changes: HashMap<_, _> = divide::take(list, perbutation)
            .into_iter()
            .map(|(id, amount)| (id, Change::Removed(amount)))
            .collect();
        changes.insert(to_check.id, Change::Added(perbutation));
        self.investigation = Investigation::StreamLimit {
            changes,
            stream: to_check.id,
        };
        self.investigation.apply(&mut self.allocations);
    }

    /// In case of overload the drop `super::Update` message be missed, this is
    /// a backup mechanic that does not rely on the drop `super::Update`
    pub(super) async fn remove_allocs_that_should_have_been_dropped<R: RangeCallback>(
        &mut self,
        handles: &mut HashMap<StreamId, (AbortHandle, StreamHandle<R>)>,
    ) {
        let should_be_dropped: Vec<_> = self
            .allocations
            .iter()
            .filter(|(_, info)| !info.stream_still_exists.load(Ordering::Relaxed))
            .map(|(id, _)| id)
            .copied()
            .collect();

        for stream_id in should_be_dropped {
            self.remove(stream_id, handles).await;
        }
    }
}
