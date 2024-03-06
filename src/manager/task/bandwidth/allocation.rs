use std::ops::{Deref, DerefMut, Range, RangeBounds};
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
pub(crate) enum Limit {
    Guess(Bandwidth),
    Unknown,
}

impl Limit {
    pub(crate) fn have_guess(&self) -> bool {
        matches!(self, Limit::Guess(_))
    }

    pub(crate) fn unknown(&self) -> bool {
        !self.have_guess()
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
        self.0.iter().map(AllocationInfo::allocated).sum()
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

    pub(crate) fn range_mut(&mut self, range: impl RangeBounds<Bandwidth>) -> IterMut {
        let start = match range.start_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => *i + 1,
            std::ops::Bound::Unbounded => 0,
        };
        let start = match self
            .0
            .binary_search_by_key(&start, AllocationInfo::allocated)
        {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        let end = match range.end_bound() {
            std::ops::Bound::Included(i) => *i,
            std::ops::Bound::Excluded(i) => *i - 1,
            std::ops::Bound::Unbounded => u32::MAX,
        };
        let end = match self.0.binary_search_by_key(&end, AllocationInfo::allocated) {
            Ok(idx) => idx,
            Err(idx) => idx,
        };

        IterMut(self.0[start..end].iter_mut())
    }

    /// panics if the order of the list changed
    pub(crate) fn biggest_mut(&mut self) -> Option<Biggest> {
        if self.is_empty() {
            None
        } else {
            Some(Biggest(&mut self.0))
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn remove(&mut self, id: StreamId) -> Option<AllocationInfo> {
        let (idx, _) = self.0.iter().enumerate().find(|(_, info)| info.id == id)?;
        Some(self.0.remove(idx))
    }

    pub(crate) fn iter(&self) -> slice::Iter<AllocationInfo> {
        self.0.iter()
    }

    /// at the end of the iteration the order is restored
    pub(crate) fn iter_mut(&mut self) -> IterMut {
        IterMut(self.0.iter_mut())
    }
}

pub(crate) struct Biggest<'a>(&'a mut Vec<AllocationInfo>);

impl<'a> Deref for Biggest<'a> {
    type Target = AllocationInfo;

    fn deref(&self) -> &Self::Target {
        &self.0[self.0.len() - 1]
    }
}

impl<'a> DerefMut for Biggest<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        let idx = self.0.len() - 1;
        &mut self.0[idx]
    }
}

impl<'a> Drop for Biggest<'a> {
    fn drop(&mut self) {
        let len = self.0.len();
        let list = &self.0;

        let last = list.get(len - 1);
        let before_last = list.get(len - 2);
        let in_order = last
            .zip(before_last)
            .map(|(last, before_last)| last.allocated >= before_last.allocated)
            .unwrap_or(true);
        assert!(in_order, "Allocations must stay in order, thus the biggest element may not become smaller then the second biggest")
    }
}

pub(crate) struct IterMut<'a>(slice::IterMut<'a, AllocationInfo>);

impl<'a> Iterator for IterMut<'a> {
    type Item = &'a mut AllocationInfo;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.0.next();
        if res.is_none() {
            // end of iter, fixup order
            let list = std::mem::take(&mut self.0);
            list.into_slice()
                .sort_unstable_by_key(|info| info.allocated)
        }
        res
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
