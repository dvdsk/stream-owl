use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::allocation::Bandwidth;
use super::{investigate::Investigation, BandwidthInfo, Controller, LimitBandwidthById, Priority};
use crate::manager::task::bandwidth::allocation::Limit;
use crate::manager::task::bandwidth::investigate::NextInvestigation;
use crate::StreamId;

mod investigate;

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
                since_last_sweep: vec![new as Bandwidth],
                prev_normal_sweep: None,
                prev_sweep: None,
            });
    }

    pub(super) async fn sweep(&mut self, handles: &impl LimitBandwidthById) {
        if self.no_update_since_last_sweep() {
            return;
        }

        let mean_bandwidths = self.mean_bandwidths();
        let prev_mean_bandwidth = self.previous_sweeps_bw.front().copied();
        let curr_mean_bandwidth: Bandwidth = mean_bandwidths.values().sum();

        let next_investigation = match self.investigation {
            Investigation::Spoiled => NextInvestigation::Neutral,
            Investigation::StreamLimit {
                investigated_stream,
                ..
            } => {
                self.record_stream_limit(investigated_stream, &mean_bandwidths);
                NextInvestigation::Neutral
            }
            Investigation::TotalLimit { .. } => {
                self.record_total_limit(curr_mean_bandwidth, prev_mean_bandwidth)
            }
            Investigation::Neutral => {
                self.record_then_adjust_allocations(curr_mean_bandwidth);
                self.next_investigation.next()
            }
        };

        self.investigation.undo(&mut self.allocations);
        self.prep_next_investigation(next_investigation, curr_mean_bandwidth);

        self.prep_bandwidth_info_for_next_sweep(mean_bandwidths);
        self.apply_new_limits(handles).await;
    }

    fn no_update_since_last_sweep(&self) -> bool {
        self.bandwidth_by_id
            .iter()
            .any(|(_, bw)| bw.newest_update < self.last_sweep)
    }

    fn mean_bandwidths(&mut self) -> HashMap<StreamId, Bandwidth> {
        self.bandwidth_by_id
            .iter_mut()
            .map(|(id, info)| {
                (
                    *id,
                    info.since_last_sweep.iter().sum::<Bandwidth>()
                        / info.since_last_sweep.len().max(1) as Bandwidth,
                )
            })
            .collect()
    }

    fn prep_bandwidth_info_for_next_sweep(&mut self, mean_bandwidths: HashMap<StreamId, u32>) {
        for (id, info) in self.bandwidth_by_id.iter_mut() {
            let mean = *mean_bandwidths.get(id).expect("guarded by early return");
            info.since_last_sweep.clear();
            if let Investigation::Neutral = self.investigation {
                info.prev_normal_sweep = Some(mean);
            } else {
                info.prev_sweep = Some(mean);
            }
        }
    }

    fn prep_next_investigation(
        &mut self,
        next_investigation: NextInvestigation,
        curr_mean_bandwidth: u32,
    ) {
        match next_investigation {
            NextInvestigation::TotalBandwidth => {
                if self.bandwidth_lim.got_space(curr_mean_bandwidth) {
                    self.investigate_total_limit(curr_mean_bandwidth);
                }
            }
            NextInvestigation::StreamBandwidth => {
                self.investigate_stream_limit();
            }
            NextInvestigation::Neutral => (),
        }
    }

    fn record_stream_limit(
        &mut self,
        investigated_stream: StreamId,
        mean_bandwidth: &HashMap<StreamId, Bandwidth>,
    ) {
        let Some((bandwidth, allocation)) = self
            .bandwidth_by_id
            .get(&investigated_stream)
            .zip(self.allocations.get_mut(&investigated_stream))
        else {
            return;
        };

        if bandwidth.newest_update < self.last_sweep {
            return;
        }

        let Some(prev_sweep) = bandwidth.prev_normal_sweep else {
            return;
        };

        let this_sweep = *mean_bandwidth
            .get(&investigated_stream)
            .expect("guarded by early return");

        if prev_sweep > this_sweep {
            allocation.upstream_limit = Limit::Guess(this_sweep);
        } else {
            allocation.upstream_limit = Limit::Unknown;
        }
    }

    fn record_total_limit(
        &mut self,
        curr_mean_bandwidth: u32,
        prev_mean_bandwidth: Option<u32>,
    ) -> NextInvestigation {
        // Total stream limit investigation increases many streams bandwidth
        // limit. This can lead to a decrease of total bandwidth if one of the
        // streams has an upstream limit which we are not aware off.
        if Some(curr_mean_bandwidth) > prev_mean_bandwidth {
            self.previous_sweeps_bw.push_front(curr_mean_bandwidth);
            self.previous_sweeps_bw.truncate(5);
            NextInvestigation::TotalBandwidth
        } else {
            NextInvestigation::Neutral
        }
    }

    fn record_then_adjust_allocations(&mut self, curr_mean_bandwidth: Bandwidth) {
        self.previous_sweeps_bw.push_front(curr_mean_bandwidth);
        self.previous_sweeps_bw.truncate(5);

        if curr_mean_bandwidth < self.total_allocated() {
            // minimum bandwidth is 1 byte per second
            let overcommited = self.total_allocated().saturating_sub(curr_mean_bandwidth);
            self.free_up_till_and_including(Priority::highest(), overcommited);
        }
    }

    /// In case of overload the drop `super::Update` message be missed, this is
    /// a backup mechanic that does not rely on the drop `super::Update`
    pub(super) async fn remove_allocs_that_should_have_been_dropped(
        &mut self,
        handles: &impl LimitBandwidthById,
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
