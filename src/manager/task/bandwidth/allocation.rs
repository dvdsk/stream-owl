use std::collections::HashMap;
use std::sync::atomic::Ordering;

use crate::network::BandwidthAllowed;
use crate::StreamId;

use super::Update;

use tokio::sync::mpsc;

use std::sync::atomic::AtomicBool;

use std::sync::Arc;

pub(crate) type Bandwidth = u32;

#[derive(Debug, Clone)]
pub(crate) enum Limit {
    Guess(Bandwidth),
    Unknown,
}

impl Limit {
    pub(crate) fn have_guess(&self) -> bool {
        matches!(self, Limit::Guess(_))
    }
}

#[derive(Debug, Clone)]
pub(crate) struct AllocationInfo {
    pub(crate) id: StreamId,
    pub(crate) target: BandwidthAllowed,
    pub(crate) curr_io_limit: Bandwidth,
    pub(crate) upstream_limit: Limit,
    pub(crate) allocated: Bandwidth,
    pub(crate) stream_still_exists: Arc<AtomicBool>,
}

impl AllocationInfo {
    pub(crate) fn allocated(&self) -> Bandwidth {
        self.allocated
    }

    /// Increase in bandwidth allowed without going over the bandwidth limit,
    /// current guess of any upstream limit or current allocation * 2. Unless
    /// that allocation is smaller then 5KB
    pub(crate) fn until_limit(&self) -> Bandwidth {
        self.best_limit() - self.allocated
    }

    /// Max bandwidth we can allocate without going over the bandwidth limit,
    /// current guess of any upstream limit or current allocation * 2. Unless
    /// that allocation is smaller then 5KB
    pub(crate) fn best_limit(&self) -> Bandwidth {
        let factor = 2;
        match self.upstream_limit {
            Limit::Guess(limit) => {
                if let BandwidthAllowed::Limited(max) = self.target {
                    assert!(limit <= max.0.get());
                }
                assert!(limit >= self.allocated);
                assert!(limit >= self.curr_io_limit);
                limit
            }
            Limit::Unknown => 10_000.max(self.allocated * factor),
        }
    }
    pub(crate) fn is_placeholder(&self) -> bool {
        self.id == StreamId::placeholder()
    }
}

/// When dropped this moves all the allocation info back into the
/// allocation HashMap which this borrows
pub struct Allocations<'a> {
    list: Vec<AllocationInfo>,
    /// when going out of scope drain back
    drain: &'a mut HashMap<StreamId, AllocationInfo>,
}

impl<'a> Allocations<'a> {
    pub fn new(
        list: Vec<AllocationInfo>,
        drain: &'a mut HashMap<StreamId, AllocationInfo>,
    ) -> Self {
        Self { list, drain }
    }
    pub fn free_all(&mut self) -> u32 {
        self.list
            .iter_mut()
            .map(|info| {
                let freed = info.allocated;
                info.allocated = 0;
                freed
            })
            .sum()
    }

    pub fn total_bandwidth(&self) -> u32 {
        self.list.iter().map(AllocationInfo::allocated).sum()
    }

    pub fn iter(&self) -> impl Iterator<Item = &AllocationInfo> + Clone {
        self.list.iter()
    }
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut AllocationInfo> {
        self.list.iter_mut()
    }

    pub fn insert_placeholder(&mut self, allocated: u32) {
        self.list.push(AllocationInfo {
            id: StreamId::placeholder(),
            target: crate::network::BandwidthAllowed::UnLimited,
            curr_io_limit: 0,
            allocated,
            stream_still_exists: Arc::new(AtomicBool::new(true)),
            upstream_limit: Limit::Unknown,
        })
    }
    pub fn insert(&mut self, item: AllocationInfo) {
        self.list.push(item)
    }
    #[must_use]
    pub fn remove_biggest(&mut self) -> Option<AllocationInfo> {
        let index = self
            .iter()
            .enumerate()
            .max_by_key(|(_, info)| info.allocated)
            .map(|(idx, _)| idx)?;

        Some(self.list.swap_remove(index))
    }
    #[must_use]
    pub(crate) fn remove_smallest(&mut self) -> Option<AllocationInfo> {
        let index = self
            .iter()
            .enumerate()
            .min_by_key(|(_, info)| info.allocated)
            .map(|(idx, _)| idx)?;

        Some(self.list.swap_remove(index))
    }
    pub fn extend(&mut self, iter: impl Iterator<Item = AllocationInfo>) {
        self.list.extend(iter)
    }

    /// moves the elements for which the predicate `pred` returns false
    /// into a Vec which is returned
    pub(crate) fn split_off_not(
        &mut self,
        pred: impl FnMut(&AllocationInfo) -> bool,
    ) -> Vec<AllocationInfo> {
        let list = std::mem::take(&mut self.list);
        let (list_true, list_false) = list.into_iter().partition(pred);
        self.list = list_true;
        list_false
    }
}

impl<'a> Drop for Allocations<'a> {
    fn drop(&mut self) {
        self.drain.extend(
            self.list
                .drain(..)
                .filter(|info| !info.is_placeholder())
                .map(|info| (info.id, info)),
        )
    }
}

/// dropping this will free up the allocated bandwidth
pub(crate) struct AllocationGuard {
    pub(crate) stream_still_exists: Arc<AtomicBool>,
    pub(crate) tx: mpsc::Sender<Update>,
    pub(crate) id: StreamId,
}

impl Drop for AllocationGuard {
    fn drop(&mut self) {
        let _ignore_error = self.tx.try_send(Update::Drop(self.id));
        // if the send fails the backup atomic will ensure
        // this streams allocation gets cleared eventually
        self.stream_still_exists.store(false, Ordering::Relaxed);
    }
}
