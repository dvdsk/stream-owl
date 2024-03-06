use futures::FutureExt;
use std::collections::{BTreeMap, HashMap};
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

use crate::manager::stream::StreamConfig;
use crate::manager::task::bandwidth::allocation::Limit;
use crate::network::BandwidthAllowed;
use crate::{BandwidthLimit, IdBandwidthCallback, RangeCallback, StreamHandle, StreamId};

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
        (*self as usize).partial_cmp(&(*other as usize))
    }
}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (*self as usize).cmp(&(*other as usize))
    }
}

pub(crate) enum Update {
    StreamUpdate {
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
        let res = self.tx.try_send(Update::StreamUpdate { id, bandwidth });

        // only warn once, the failure is probably caused by an
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

pub(crate) struct Controller {
    rx: mpsc::Receiver<Update>,
    tx: mpsc::Sender<Update>,
    bandwidth_lim: BandwidthAllowed,
    next_sweep: Instant,
    allocated_by_prio: BTreeMap<Priority, allocation::Allocations>,
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

    pub(super) async fn handle_update<R: RangeCallback>(
        &mut self,
        handles: &mut HashMap<StreamId, (AbortHandle, StreamHandle<R>)>,
        update: Update,
    ) {
        match update {
            Update::StreamUpdate { id, bandwidth } => todo!(),
            Update::NewPriority { id, priority } => todo!(),
            Update::NewStreamLimit { id, bandwidth } => todo!(),
            Update::NewGlobalLimit { bandwidth } => todo!(),
            Update::Scheduled => todo!(), // should also check if any of the
            // streams has been dropped and
            // unreported
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
                next_sweep: Instant::now() + Duration::MAX,
                allocated_by_prio: BTreeMap::new(),
            },
            bandwidth,
        )
    }

    /* TODO: Similar thing for paused streams unpause would then send
     * trigger stealing taking back the bandwidth <03-03-24, dvdsk> */
    pub(crate) async fn remove<R: RangeCallback>(
        &mut self,
        id: StreamId,
        handles: &mut HashMap<StreamId, (AbortHandle, StreamHandle<R>)>,
    ) {
        let mut info = None;
        for list in self.allocated_by_prio.values_mut() {
            info = info.or(list.remove(id));
        }

        let Some(info) = info else { return };

        self.divide_known_bandwidth(info.allocated);
        /* TODO: needs to skip paused streams <03-03-24, dvdsk> */
        self.apply_new_limits(handles).await;
    }

    #[must_use]
    pub(crate) async fn register<R: RangeCallback>(
        &mut self,
        id: StreamId,
        mut config: StreamConfig,
        handles: &mut HashMap<StreamId, (AbortHandle, StreamHandle<R>)>,
    ) -> (StreamConfig, allocation::AllocationGuard) {
        use BandwidthAllowed as B;
        let init_limit = match config.bandwidth {
            B::UnLimited => 10_000,
            B::Limited(limit) => limit.0.get().min(10_000),
        };
        let allocated_bw = self.allocate_bw_limited(config.priority, init_limit);
        config.bandwidth = BandwidthAllowed::Limited(BandwidthLimit(
            NonZeroU32::new(allocated_bw).expect("allocated_bandwidth should never be zero"),
        ));

        let guard = allocation::AllocationGuard {
            tx: self.tx.clone(),
            stream_still_exists: Arc::new(AtomicBool::new(true)),
            id,
        };
        let info = allocation::AllocationInfo {
            id,
            curr_io_limit: allocated_bw,
            allocated: allocated_bw,
            upstream_limit: Limit::Unknown,
            target: config.bandwidth.clone(),
            stream_still_exists: guard.stream_still_exists.clone(),
        };

        if let Some(list) = self.allocated_by_prio.get_mut(&config.priority) {
            list.insert(info);
        } else {
            self.allocated_by_prio
                .insert(config.priority, allocation::Allocations::new(info));
        }

        self.apply_new_limits(handles).await;
        (config, guard)
    }

    async fn apply_new_limits<R: RangeCallback>(
        &self,
        handles: &mut HashMap<StreamId, (AbortHandle, StreamHandle<R>)>,
    ) {
        for allocation::AllocationInfo {
            allocated: new_bw,
            id,
            ..
        } in self
            .allocated_by_prio
            .iter()
            .flat_map(|(_, list)| list)
            .filter(|info| info.allocated != info.curr_io_limit)
        {
            let Some((_, handle)) = handles.get(id) else {
                continue;
            };
            handle
                .limit_bandwidth(BandwidthLimit(
                    NonZeroU32::new(*new_bw).expect("todo handle unlimited dl speed"),
                ))
                .await
        }
    }

    fn allocate_bw_limited(&mut self, priority: Priority, limit: u32) -> u32 {
        // if there is only one other stream or no stream we have no idea of the total
        // bandwidth available
        if self.n_streams() < 2 {
            return limit;
        }

        // take needed space starting at the biggest allocation in the lowest prio.
        // if we need all, take all from everyone. If we need a bit divide the leftovers
        // never surpassing the previous value
        let mut still_needed = limit;
        for p in ((Priority::lowest() as usize)..(priority as usize)).map(Priority::from_usize) {
            let Some(list) = self.allocated_by_prio.get_mut(&p) else {
                continue;
            };

            let allocced = list.total_allocated();
            if allocced > still_needed {
                Self::redivide(list, still_needed);
                return limit;
            } else {
                let got = self.take_all(p);
                still_needed -= got;
            }
        }

        // could not take enough from lower prio levels, take from same prio
        // starting with the biggest stopping once done
        still_needed -= self.redivide_fair(priority, still_needed);
        limit - still_needed
    }

    fn n_streams(&self) -> usize {
        self.allocated_by_prio
            .iter()
            .map(|(_, list)| list.len())
            .sum()
    }

    fn take_all(&mut self, p: Priority) -> u32 {
        let Some(list) = self.allocated_by_prio.get_mut(&p) else {
            return 0;
        };

        let mut freed = 0;
        for info in list {
            freed += info.allocated;
            info.allocated = 0;
        }
        freed
    }

    fn divide_known_bandwidth(&mut self, mut to_divide: u32) {
        // spend first at highest prio
        for list in self.allocated_by_prio.values_mut().rev() {
            let guessed_limit = list
                .iter_mut()
                .filter(|info| info.upstream_limit.have_guess());
            let left_over = divide::divide_new_bw(guessed_limit, to_divide);
            to_divide = left_over;
            if to_divide == 0 {
                return;
            }

            let unknown_limit = list.iter_mut().filter(|info| info.upstream_limit.unknown());
            let left_over = divide::divide_new_bw(unknown_limit, to_divide);
            to_divide = left_over;
            if to_divide == 0 {
                return;
            }
        }
    }
}
