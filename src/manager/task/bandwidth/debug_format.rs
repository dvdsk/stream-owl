use super::allocation::AllocationInfo;
use super::{BandwidthInfo, Controller};

impl std::fmt::Debug for Controller {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut bandwidth_by_id = self.bandwidth_by_id.iter().collect::<Vec<_>>();
        bandwidth_by_id.sort_by_key(|(id, _)| **id);
        let mut allocations = self.allocations.iter().collect::<Vec<_>>();
        allocations.sort_by_key(|(id, _)| **id);

        f.debug_struct("Controller")
            .field("bandwidth_lim", &self.bandwidth_lim)
            .field("previous_sweeps_bw", &self.previous_sweeps_bw)
            .field(
                "previous_total_bw_perbutation",
                &self.previous_total_bw_perbutation,
            )
            // .field("last_sweep", &self.last_sweep.elapsed())
            // .field("next_sweep", &self.next_sweep.elapsed())
            .field("next_investigation", &self.next_investigation)
            .field("investigation", &self.investigation)
            .field("last_index_checked", &self.last_index_checked)
            .field("bandwidth_by_id", &bandwidth_by_id)
            // .field("allocated_by_prio", &self.allocated_by_prio)
            // .field("orderd_streamids", &self.orderd_streamids)
            .field("allocations", &allocations)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for BandwidthInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BandwidthInfo")
            .field("since_last_sweep", &self.since_last_sweep)
            // .field("steadyness", &self.steadyness)
            .field("prev_normal_sweep", &self.prev_normal_sweep)
            .field("prev_sweep", &self.prev_sweep)
            // .field("newest_update", &self.newest_update.elapsed())
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for AllocationInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AllocationInfo")
            .field("allocated", &self.allocated)
            .field("upstream_limit", &self.upstream_limit)
            .finish_non_exhaustive()
    }
}
