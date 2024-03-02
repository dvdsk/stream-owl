use std::ops::{Range, RangeBounds};
use std::slice;
use std::sync::atomic::Ordering;

use crate::network::BandwidthAllowed;
use crate::StreamId;

use super::Update;

use tokio::sync::mpsc;

use std::sync::atomic::AtomicBool;

use std::sync::Arc;

pub(crate) type Bandwidth = u32;

#[derive(Debug, Clone)]
pub(crate) struct AllocationInfo {
    pub(crate) id: StreamId,
    pub(crate) target: BandwidthAllowed,
    pub(crate) curr_io_limit: Bandwidth,
    pub(crate) allocated: Bandwidth,
    pub(crate) stream_still_exists: Arc<AtomicBool>,
}

/// is sorted on increasing bandwidth
#[derive(Debug, Default)]
pub(crate) struct Allocations(Vec<AllocationInfo>);

impl Allocations {
    pub(crate) fn insert(&mut self, info: AllocationInfo) {
        let idx = match self
            .0
            .binary_search_by_key(&info.allocated, |info| info.allocated)
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        self.0.insert(idx, info)
    }

    pub(crate) fn len(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn total_allocated(&self) -> Bandwidth {
        self.0.iter().map(|info| info.allocated).sum()
    }

    pub(crate) fn new(info: AllocationInfo) -> Allocations {
        Self(vec![info])
    }

    pub(crate) fn pop_biggest(&mut self) -> Option<AllocationInfo> {
        self.0.pop()
    }

    pub(crate) fn range(&self, range: Range<Bandwidth>) -> slice::Iter<AllocationInfo> {
        let start = match self
            .0
            .binary_search_by_key(&range.start, |info| info.allocated)
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        };
        let end = match self
            .0
            .binary_search_by_key(&range.end, |info| info.allocated)
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        self.0[start..end].iter()
    }

    pub(crate) fn range_mut(
        &mut self,
        range: impl RangeBounds<Bandwidth>,
    ) -> slice::IterMut<AllocationInfo> {
        let start = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => *i + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let start = match self.0.binary_search_by_key(&start, |info| info.allocated) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        let end = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => *i - 1,
            std::ops::Bound::Unbounded => u32::MAX,
        };
        let end = match self.0.binary_search_by_key(&end, |info| info.allocated) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        self.0[start..end].iter_mut()
    }
}

impl<'a> Extend<AllocationInfo> for &'a mut Allocations {
    fn extend<T: IntoIterator<Item = AllocationInfo>>(&mut self, iter: T) {
        for info in iter {
            self.insert(info);
        }
    }
}

impl<'a> IntoIterator for &'a Allocations {
    type Item = &'a AllocationInfo;
    type IntoIter = slice::Iter<'a, AllocationInfo>;

    fn into_iter(self) -> Self::IntoIter {
        (&self.0).into_iter()
    }
}

impl<'a> IntoIterator for &'a mut Allocations {
    type Item = &'a mut AllocationInfo;
    type IntoIter = slice::IterMut<'a, AllocationInfo>;

    fn into_iter(self) -> Self::IntoIter {
        (&mut self.0).into_iter()
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
