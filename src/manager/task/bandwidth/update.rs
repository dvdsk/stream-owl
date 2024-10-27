use std::collections::HashMap;
use std::sync::atomic::Ordering;

use tokio::task::AbortHandle;

use super::allocation::Bandwidth;
use super::{divide, BandwidthInfo, Controller, Priority};
use crate::manager::task::bandwidth::allocation::{self, AllocationInfo, Allocations};
use crate::{RangeCallback, StreamHandle, StreamId};

impl Controller {
    pub(super) fn bandwidth_update(&mut self, stream: StreamId, new: usize) {
        self.bandwidth_by_id
            .entry(stream)
            .and_modify(|bandwidth| {
                let limit_was = self
                    .allocations
                    .get(&stream)
                    .expect("if its in bandwidth_by_id it must be in a list")
                    .curr_io_limit;
                bandwidth.update(new as u32, limit_was);
            })
            .or_insert(BandwidthInfo {
                curr: new as u32,
                // Initially low as a new stream is expected to rapidly change
                // bandwidth
                steadyness: 0.5,
            });
    }

    pub(super) async fn sweep<R: RangeCallback>(
        &mut self,
        handles: &mut HashMap<StreamId, (AbortHandle, StreamHandle<R>)>,
    ) {
        self.undo_bw_perbutations();

        let curr_bw: u32 = self.bandwidth_by_id.values().map(BandwidthInfo::curr).sum();
        let prev_bw = self
            .previous_sweeps_bw
            .front()
            .copied()
            .unwrap_or_else(|| Bandwidth::MAX.min(self.bandwidth_lim.unwrap()));
        self.previous_sweeps_bw.push_front(curr_bw);
        self.previous_sweeps_bw.truncate(5);

        if prev_bw > curr_bw {
            let lost = prev_bw - curr_bw;
            self.remove_bw_at_and_below(Priority::highest(), lost);
        } else {
            let got = curr_bw - prev_bw;
            self.divide_new_bandwidth(got);
        }

        if self.next_check_available_bw && self.bandwidth_lim.could_increase(curr_bw) {
            self.note_stream_limits();
            self.perbutate_to_find_total_bw();
            self.next_check_available_bw = false;
        } else {
            self.prebutate_to_find_stream_max_bw();
            self.next_check_available_bw = true;
        }

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
    pub(super) fn perbutate_to_find_total_bw(&mut self) {
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
        for (id, change) in &changes {
            self.allocations
                .get_mut(id)
                .expect("nothing removed since changes was made")
                .allocated += change;
        }

        self.stream_perbutations = changes;
    }

    fn undo_bw_perbutations(&mut self) {
        for (id, change) in &mut self.stream_perbutations {
            self.allocations
                .get_mut(&id)
                .expect("nothing removed since changes was made")
                .allocated -= *change;
        }
    }

    /// make a small perbutation to the bandwidth division to find
    /// if a stream has more upstream bandwidth
    pub(super) fn prebutate_to_find_stream_max_bw(&mut self) {
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
        let perbutation = self
            .optimal_total_bw_perbutation()
            .min(five_percent_of_allocation);
        let list = self.allocations.drain().map(|(_, info)| info).collect();
        let allocations = Allocations::new(list, &mut self.allocations);
        let mut changes = divide::take(&allocations, perbutation);
        drop(allocations);
        changes.insert(to_check.id, perbutation);

        to_check.allocated += perbutation;
    }

    /// In case of overload the drop `super::Update` message be missed, this is
    /// a backup mechanic that does not rely on the drop `super::Update`
    pub(super) async fn remove_allocs_that_failed_to_do_so<R: RangeCallback>(
        &mut self,
        handles: &mut HashMap<StreamId, (AbortHandle, StreamHandle<R>)>,
    ) {
        let should_still_be_dropped: Vec<_> = self
            .allocations
            .iter()
            .filter(|(_, info)| !info.stream_still_exists.load(Ordering::Relaxed))
            .map(|(id, _)| id)
            .copied()
            .collect();

        for stream_id in should_still_be_dropped {
            self.remove(stream_id, handles).await;
        }
    }
}
