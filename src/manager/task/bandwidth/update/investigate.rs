use std::collections::HashMap;

use super::super::allocation::Bandwidth;
use super::super::{divide, investigate::Change, investigate::Investigation, Controller};
use crate::StreamId;

impl Controller {
    /// make a small increase to the bandwidth division to find out
    /// if the total available bandwidth has increased
    pub(super) fn investigate_total_limit(&mut self, curr_bw: Bandwidth) {
        let perbutation = self.optimal_total_bw_perbutation(curr_bw);
        self.previous_total_bw_perbutation = Some(perbutation);
        if perbutation == 0 {
            self.investigation = Investigation::Neutral;
            return;
        }

        // want each stream to get some bit of extra bandwidth, how much
        // is determined by its growability and steadyness. If its more
        // steady it gets more allocated
        let (changes, _left_over) = divide::spread_prioritize_steadiest(
            perbutation,
            &self.bandwidth_by_id,
            &self.allocations,
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
        assert!(!self.allocations.is_empty());

        struct ToCheck {
            id: StreamId,
            allocated: Bandwidth,
        }

        // try every stream while finding the next stream to check
        let mut to_check: Option<ToCheck> = None;
        for _ in 0..self.orderd_streamids.len() {
            let index = (self.last_index_checked + 1) % self.allocations.len();
            self.last_index_checked = index;

            let stream_id = self.orderd_streamids[index];
            if let Some(info) = self.allocations.get(&stream_id) {
                to_check = Some(ToCheck {
                    id: stream_id,
                    allocated: info.allocated,
                });
                break;
            }
        }

        let Some(to_check) = to_check else {
            return;
        };

        let five_percent_of_allocation = (to_check.allocated as f32 * 0.05) as Bandwidth;
        let perbutation = five_percent_of_allocation;
        let list = self.allocations.values();

        let mut changes: HashMap<_, _> = divide::take(list, perbutation)
            .into_iter()
            .map(|(id, amount)| (id, Change::Removed(amount)))
            .collect();
        changes.insert(to_check.id, Change::Added(perbutation));
        self.investigation = Investigation::StreamLimit {
            changes,
            investigated_stream: to_check.id,
        };
        self.investigation.apply(&mut self.allocations);
    }

    /// grow faster if the previous few increase was achieved
    /// if we just started double the increase
    fn optimal_total_bw_perbutation(&self, curr_bw: Bandwidth) -> Bandwidth {
        let after = curr_bw;
        let Some(before) = self.previous_sweeps_bw.get(1).copied() else {
            return after.max(2); // double (if zero set to 2)
        };

        let previous_increase = self
            .previous_total_bw_perbutation
            .expect("if before is Some then this is the second time this fn is called");

        if before > after {
            return after / 10;
        }

        let increase = (after - before) as f32;
        let largely_met = (increase * 0.9) as u32 >= previous_increase;

        if largely_met {
            previous_increase * 2
        } else {
            previous_increase / 2
        }
    }
}
