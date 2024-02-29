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

use crate::manager::stream::StreamConfig;
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

type Bandwidth = u32;
#[derive(Debug, Clone)]
struct AllocationInfo {
    id: StreamId,
    target: BandwidthAllowed,
    curr_io_limit: Bandwidth,
    stream_still_exists: Arc<AtomicBool>,
}

/// dropping this will free up the allocated bandwidth
pub(crate) struct AllocationGuard {
    stream_still_exists: Arc<AtomicBool>,
    tx: mpsc::Sender<Update>,
    id: StreamId,
}

impl Drop for AllocationGuard {
    fn drop(&mut self) {
        let _ignore_error = self.tx.try_send(Update::Drop(self.id));
        // if the send fails the backup atomic will ensure
        // this streams allocation gets cleared eventually
        self.stream_still_exists.store(false, Ordering::Relaxed);
    }
}

pub(crate) struct Controller {
    rx: mpsc::Receiver<Update>,
    tx: mpsc::Sender<Update>,
    bandwidth_lim: BandwidthAllowed,
    next_sweep: Instant,
    // we where using this but no longer need it
    // might still be up for graps though
    leftover: Bandwidth,
    allocated_by_prio: BTreeMap<Priority, BTreeMap<Bandwidth, AllocationInfo>>,
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

    pub(super) fn handle_update<R: RangeCallback>(
        &mut self,
        handles: &mut HashMap<StreamId, (AbortHandle, StreamHandle<R>)>,
        update: Update,
    ) {
        match update {
            Update::StreamUpdate { id, bandwidth } => todo!(),
            Update::NewPriority { id, priority } => todo!(),
            Update::NewStreamLimit { id, bandwidth } => todo!(),
            Update::NewGlobalLimit { bandwidth } => todo!(),
            Update::Scheduled => todo!(),
            Update::Drop(stream_id) => todo!(),
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
                leftover: 0,
                allocated_by_prio: BTreeMap::new(),
            },
            bandwidth,
        )
    }

    pub(crate) fn remove(&self, id: StreamId) {
        todo!()
    }

    // Todo add guard that unregisters if registration is dropped
    #[must_use]
    pub(crate) async fn register<R: RangeCallback>(
        &mut self,
        id: StreamId,
        mut config: StreamConfig,
        handles: &mut HashMap<StreamId, (AbortHandle, StreamHandle<R>)>,
    ) -> (StreamConfig, AllocationGuard) {
        use BandwidthAllowed as B;
        let init_limit = match config.bandwidth {
            B::UnLimited => 10_000,
            B::Limited(limit) => limit.0.get().min(10_000),
        };
        let allocated_bw = self.allocate_bw_limited(config.priority, init_limit);
        config.bandwidth = BandwidthAllowed::Limited(BandwidthLimit(
            NonZeroU32::new(allocated_bw).expect("allocated_bandwidth should never be zero"),
        ));

        let guard = AllocationGuard {
            tx: self.tx.clone(),
            stream_still_exists: Arc::new(AtomicBool::new(true)),
            id,
        };
        let info = AllocationInfo {
            id,
            curr_io_limit: allocated_bw,
            target: config.bandwidth.clone(),
            stream_still_exists: guard.stream_still_exists.clone(),
        };

        if let Some(list) = self.allocated_by_prio.get_mut(&config.priority) {
            let existing = list.insert(allocated_bw, info);
            assert!(existing.is_none());
        } else {
            let mut list = BTreeMap::new();
            list.insert(allocated_bw, info);
            self.allocated_by_prio.insert(config.priority, list);
        }

        self.apply_new_limits(handles).await;
        (config, guard)
    }

    async fn apply_new_limits<R: RangeCallback>(
        &self,
        handles: &mut HashMap<StreamId, (AbortHandle, StreamHandle<R>)>,
    ) {
        for (new_bandwidth, AllocationInfo { id, .. }) in self
            .allocated_by_prio
            .iter()
            .flat_map(|(_, list)| list)
            .filter(|(allocated, info)| **allocated != info.curr_io_limit)
        {
            let Some((_, handle)) = handles.get(id) else {
                continue;
            };
            handle
                .limit_bandwidth(BandwidthLimit(
                    NonZeroU32::new(*new_bandwidth).expect("todo handle unlimited dl speed"),
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
            let allocced = self.total_allocated_at(p);
            if allocced < still_needed {
                let leftover = self.redevide(p, still_needed - allocced);
                self.leftover += leftover;
                return limit;
            } else {
                let got = self.take_all(p);
                still_needed -= got;
            }
        }

        // could not take enough from lower prio levels, take from same prio
        // starting with the biggest stopping once done
        let allocced = self.total_allocated_at(priority);
        let n_streams = self
            .allocated_by_prio
            .get(&priority)
            .map(BTreeMap::len)
            .unwrap_or(0) as u32;
        let mean = allocced / n_streams;
        let Some(list) = self.allocated_by_prio.get(&priority) else {
            return limit - still_needed; // out of options return what we got
        };



        todo!()
    }

    fn redivide_fair(&mut self, p: Priority, wished_for: u32) -> u32 {
        let Some(list) = self.allocated_by_prio.get_mut(&p) else {
            return 0;
        };

        let Some(biggest) = list.pop_last() else {
            return 0;
        };
        let Some(next_biggest) = list.pop_last() else {
            if biggest.0 / 2 >= wished_for {
                list.insert(biggest.0 /2, biggest.1);
                return biggest.0 / 2;
            } else {
                list.insert(biggest.0 - wished_for, biggest.1);
                return wished_for;
            }
        };

        let still_needed = wished_for;
        let mut stack = vec![biggest, next_biggest];
        if biggest.0 - next_biggest.0 >= wished_for {
            list.insert(biggest.0 - wished_for, biggest.1);
            list.insert(next_biggest.0, next_biggest.1);
            return wished_for;
        }

        let Some(next) = list.pop_last() else {
            let possible = (biggest.0 + next_biggest.0) / stack.len() as u32;
            list.insert(possible, biggest.1);
            list.insert(possible, next_biggest.1);
            return possible;
        };

        stack.push(next);
        0
    }

    fn total_allocated_at(&self, priority: Priority) -> u32 {
        self.allocated_by_prio
            .get(&priority)
            .map(|list| list.iter().map(|(bandwidth, _)| bandwidth).sum())
            .unwrap_or(0)
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
        let zeroed_budget = std::mem::take(list)
            .into_iter()
            .inspect(|(bandwidth, _)| freed += bandwidth)
            .map(|(_, val)| (0, val));
        list.extend(zeroed_budget);

        freed
    }

    /// returns the undividable leftovers
    fn redevide(&mut self, p: Priority, allocced: u32) -> u32 {
        let list = self
            .allocated_by_prio
            .get_mut(&p)
            .expect("there bandwidth allocated here");

        let mut to_divide = allocced;
        loop {
            let new_budget = to_divide / (list.len() as u32);
            let leftover = list
                .range(0..new_budget)
                .map(|(bandwidth, _)| bandwidth)
                .sum::<u32>();

            if leftover == 0 {
                break;
            }
            to_divide += allocced + leftover;
        }

        let new_budget = to_divide / (list.len() as u32);
        let over_budget: Vec<_> = list.range_mut(new_budget..).map(|(key, _)| *key).collect();
        for over_budget in over_budget.into_iter() {
            let entry = list
                .remove(&over_budget)
                .expect("map didnt change between collect and this");
            let existing = list.insert(new_budget, entry);
            assert!(existing.is_none());
        }
        let leftover = to_divide % (list.len() as u32);
        leftover
    }
}
