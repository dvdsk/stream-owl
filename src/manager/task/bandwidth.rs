use futures::FutureExt;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::error::TrySendError;

use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tokio::time;

mod allocation;
mod divide;
mod investigate;
#[cfg(test)]
mod test;
mod update;

use crate::manager::stream::StreamConfig;
use crate::manager::task::bandwidth::allocation::Limit;
use crate::network::BandwidthAllowed;
use crate::{BandwidthLimit, IdBandwidthCallback, StreamHandle, StreamId};

use allocation::{AllocationInfo, Allocations, Bandwidth};

pub trait LimitBandwidthById {
    async fn limit_bandwidth(&self, id: StreamId, limit: BandwidthLimit);
}

impl<R: crate::RangeCallback> LimitBandwidthById
    for HashMap<StreamId, (AbortHandle, StreamHandle<R>)>
{
    async fn limit_bandwidth(&self, id: StreamId, limit: BandwidthLimit) {
        let Some((_, handle)) = self.get(&id) else {
            return;
        };

        handle.limit_bandwidth(limit).await
    }
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum Priority {
    High = 2,
    #[default]
    Normal = 1,
    Low = 0,
}
impl Priority {
    fn lowest() -> Self {
        Self::Low
    }

    fn highest() -> Self {
        Self::High
    }

    // needed as long as std::iter::Step is nightly
    fn from_usize(n: usize) -> Self {
        match n {
            0 => Self::Low,
            1 => Self::Normal,
            2 => Self::High,
            _ => unreachable!("3usize does not encode a Priority"),
        }
    }
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self as usize).cmp(&(*other as usize))
    }
}

#[expect(dead_code, reason = "handles not yet written")]
pub(crate) enum Update {
    StreamBandwidth {
        id: StreamId,
        bandwidth: usize,
    },
    NewPriority {
        id: StreamId,
        priority: usize,
    },
    NewStreamLimit {
        id: StreamId,
        bandwidth: BandwidthAllowed,
    },
    NewGlobalLimit {
        bandwidth: BandwidthAllowed,
    },
    Scheduled,
    Drop(StreamId),
}

#[derive(Clone)]
pub(crate) struct WrappedCallback<B: IdBandwidthCallback> {
    inner: B,
    tx: mpsc::Sender<Update>,
}

impl<B: IdBandwidthCallback> IdBandwidthCallback for WrappedCallback<B> {
    fn perform(&mut self, id: StreamId, bandwidth: usize) {
        let res = self.tx.try_send(Update::StreamBandwidth { id, bandwidth });

        // Only warn once. The failure is probably caused by an
        // overloaded system or blocking too long. Lets not contribute to that
        static SHOULD_WARN: AtomicBool = AtomicBool::new(true);
        if let Err(TrySendError::Full(_)) = res {
            if SHOULD_WARN
                .compare_exchange(true, false, Ordering::Relaxed, Ordering::Relaxed)
                .unwrap_or(false)
            {
                tracing::warn!("bandwidth update lagging behind")
            }
        }

        self.inner.perform(id, bandwidth)
    }
}

#[derive(Debug)]
struct BandwidthInfo {
    pub since_last_sweep: Vec<Bandwidth>,
    pub steadyness: f32,
    pub prev_normal_sweep: Option<Bandwidth>,
    pub prev_sweep: Option<Bandwidth>,
    pub newest_update: Instant,
}

impl BandwidthInfo {
    fn mean_since_last_sweep(&self) -> Bandwidth {
        self.since_last_sweep.iter().sum::<Bandwidth>() / self.since_last_sweep.len() as Bandwidth
    }

    /// Get an estimate of this streams bandwidth
    fn get(&self) -> Bandwidth {
        self.prev_normal_sweep
            .or(self.prev_sweep)
            .unwrap_or_else(|| {
                assert!(
                    !self.since_last_sweep.is_empty(),
                    "prev_sweep is Some if this is zero"
                );
                self.mean_since_last_sweep()
            })
    }
}

pub(crate) struct Controller {
    rx: mpsc::Receiver<Update>,
    tx: mpsc::Sender<Update>,
    bandwidth_lim: BandwidthAllowed,
    previous_sweeps_bw: VecDeque<Bandwidth>,
    previous_total_bw_perbutation: Option<Bandwidth>,
    /// bandwidth that streams where using that where
    /// removed since the last sweep
    last_sweep: Instant,
    next_sweep: Instant,
    next_investigation: investigate::NextInvestigation,

    investigation: investigate::Investigation,
    /// last stream checked for more bandwidth in list `bandwidth_by_id`
    last_index_checked: usize,
    /// list of StreamId in some order
    bandwidth_by_id: HashMap<StreamId, BandwidthInfo>,

    allocated_by_prio: BTreeMap<Priority, HashSet<StreamId>>,
    orderd_streamids: Vec<StreamId>,
    allocations: HashMap<StreamId, AllocationInfo>,
}

impl Controller {
    pub(crate) fn get_tx(&self) -> mpsc::Sender<Update> {
        self.tx.clone()
    }

    #[must_use]
    pub(super) async fn wait_for_update(&mut self) -> Update {
        let external_update = self
            .rx
            .recv()
            .map(|r| r.expect("should drop before last tx closes"));
        let next_sweep = time::sleep_until(self.next_sweep.into()).map(|_| Update::Scheduled);

        use futures_concurrency::future::Race;
        (external_update, next_sweep).race().await
    }

    pub(super) async fn handle_update(
        &mut self,
        handles: &impl LimitBandwidthById,
        update: Update,
    ) {
        match update {
            Update::StreamBandwidth { id, bandwidth } => self.bandwidth_update(id, bandwidth),
            Update::NewPriority { .. } => todo!(),
            Update::NewStreamLimit { .. } => todo!(),
            Update::NewGlobalLimit { .. } => todo!(),
            Update::Scheduled => {
                self.remove_allocs_that_should_have_been_dropped(handles)
                    .await;
                self.sweep(handles).await;
                // TODO: Should probably be dynamic <dvdsk>
                self.next_sweep = Instant::now() + Duration::from_millis(200);
            }
            Update::Drop(stream_id) => self.remove(stream_id, handles).await,
        }
    }

    #[must_use]
    pub(crate) fn new<B>(
        bandwidth_callback: B,
        bandwidth_lim: BandwidthAllowed,
    ) -> (Self, WrappedCallback<B>)
    where
        B: IdBandwidthCallback,
    {
        let (tx, rx) = mpsc::channel(10);
        let bandwidth = WrappedCallback {
            inner: bandwidth_callback,
            tx: tx.clone(),
        };

        (
            Self {
                rx,
                tx,
                bandwidth_lim,
                next_sweep: Instant::now() + Duration::ZERO,
                previous_sweeps_bw: VecDeque::new(),
                previous_total_bw_perbutation: None,

                last_index_checked: 0,
                orderd_streamids: Vec::new(),
                bandwidth_by_id: HashMap::new(),
                allocated_by_prio: BTreeMap::new(),
                allocations: HashMap::new(),
                investigation: investigate::Investigation::Neutral,
                last_sweep: Instant::now(),
                next_investigation: investigate::NextInvestigation::TotalBandwidth,
            },
            bandwidth,
        )
    }

    /* TODO: Similar thing for paused streams unpause would then send
     * trigger stealing taking back the bandwidth <03-03-24, dvdsk> */
    pub(crate) async fn remove(&mut self, id: StreamId, handles: &impl LimitBandwidthById) {
        let pos = self
            .orderd_streamids
            .iter()
            .position(|i| *i == id)
            .expect("elements only removed here");
        self.orderd_streamids.remove(pos);
        let info = self.allocations.remove(&id);
        for list in self.allocated_by_prio.values_mut() {
            list.remove(&id);
        }

        self.bandwidth_by_id.remove(&id);
        let Some(info) = info else { return };

        self.investigation.spoil(&mut self.allocations);

        if self.allocations.len() == 1 {
            // remove all limits
            self.divide_new_bandwidth(Bandwidth::MAX);
            self.apply_new_limits(handles).await;
        } else {
            self.divide_new_bandwidth(info.allocated);
            self.apply_new_limits(handles).await;
        }
    }

    #[must_use]
    pub(crate) async fn register(
        &mut self,
        id: StreamId,
        config: StreamConfig,
        handles: &impl LimitBandwidthById,
    ) -> (StreamConfig, allocation::AllocationGuard) {
        let allocated_bw = if self.allocations.is_empty() {
            Bandwidth::MAX // unlimited bandwidth
        } else if self.allocations.len() == 1 {
            let (id, info) = self
                .allocations
                .iter_mut()
                .next()
                .expect("in sync with bandwidth_by_id");
            let total_bw = self
                .bandwidth_by_id
                .get(id)
                .map(BandwidthInfo::get)
                .unwrap_or(10_000); // could not yet have had any bandwidth report
            info.allocated = total_bw / 2;
            total_bw / 2
        } else {
            let init_limit = config.bandwidth.unwrap();
            self.remove_bw_at_and_below(config.priority, init_limit)
        };

        let guard = allocation::AllocationGuard {
            tx: self.tx.clone(),
            stream_still_exists: Arc::new(AtomicBool::new(true)),
            id,
        };
        let info = AllocationInfo {
            id,
            curr_io_limit: allocated_bw,
            allocated: allocated_bw,
            upstream_limit: Limit::Unknown,
            target: config.bandwidth.clone(),
            stream_still_exists: guard.stream_still_exists.clone(),
        };
        dbg!(id, &info);

        self.orderd_streamids.push(id);
        self.allocations.insert(id, info);
        self.allocated_by_prio
            .entry(config.priority)
            .and_modify(|list| {
                list.insert(id);
            })
            .or_insert(HashSet::from([id]));

        self.apply_new_limits(handles).await;
        (config, guard)
    }

    async fn apply_new_limits(&mut self, handles: &impl LimitBandwidthById) {
        for (
            id,
            AllocationInfo {
                allocated: new_bw,
                curr_io_limit,
                ..
            },
        ) in self
            .allocations
            .iter_mut()
            .filter(|(_, info)| info.allocated != info.curr_io_limit)
        {
            // TODO handle unlimited download speed
            let limit = BandwidthLimit(NonZeroU32::new(*new_bw).unwrap());
            handles.limit_bandwidth(*id, limit).await;
            *curr_io_limit = *new_bw;
        }
    }

    fn remove_bw_at_and_below(&mut self, priority: Priority, limit: u32) -> u32 {
        // if there is only one other stream we currently have no guess of the
        // total bandwidth available. Since that stream could very well be at its
        // upstream limit.
        if self.n_streams() <= 1 {
            return limit;
        }

        // Take needed bandwidth from priorities lower then this stream up to
        // this streams priority. Start at the biggest allocation for each
        // priority. Take everything from the lower priorities if we need it.
        // Divide evenly for streams at the same priority.
        let mut still_needed = limit;
        for p in ((Priority::lowest() as usize)..(priority as usize)).map(Priority::from_usize) {
            let mut allocations = self.allocations_at_priority(p);
            if allocations.total_bandwidth() > still_needed {
                let changes = divide::take(allocations.iter(), still_needed);
                for info in allocations.iter_mut() {
                    info.allocated += changes.get(&info.id).copied().unwrap_or_default();
                }
                return limit;
            } else {
                let got = allocations.free_all();
                still_needed -= got;
            }
        }

        // could not take enough from lower priority levels, take from same priority
        // starting with the biggest stopping once done
        let allocations = self.allocations_at_priority(priority);
        still_needed -= divide::spread(allocations, still_needed);
        limit - still_needed
    }

    fn n_streams(&self) -> usize {
        self.allocated_by_prio.values().map(|list| list.len()).sum()
    }

    fn divide_new_bandwidth(&mut self, mut to_divide: u32) {
        // spend first at highest priority
        let priorities: Vec<_> = self.allocated_by_prio.keys().copied().collect();
        for priority in priorities {
            let mut allocations = self.allocations_at_priority(priority);
            let unknown_limit = allocations.split_off_not(|info| info.upstream_limit.have_guess());
            let bandwith_left_over = {
                let guessed_limit = allocations;
                divide::divide_new_bw(guessed_limit, to_divide)
            };

            to_divide = bandwith_left_over;
            if to_divide == 0 {
                return;
            }

            let unknown_limit = Allocations::new(unknown_limit, &mut self.allocations);
            let left_over = divide::divide_new_bw(unknown_limit, to_divide);
            to_divide = left_over;
            if to_divide == 0 {
                return;
            }
        }
    }

    /// Returned object must be drained back into self before it drops
    #[must_use]
    fn allocations_at_priority(&mut self, p: Priority) -> Allocations<'_> {
        let list = self
            .allocated_by_prio
            .get(&p)
            .into_iter()
            .flatten()
            .map(|id| {
                self.allocations
                    .remove(id)
                    .expect("by_prio and alloctions should be in sync")
            })
            .collect();
        Allocations::new(list, &mut self.allocations)
    }
}
