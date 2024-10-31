use rangemap::set::RangeSet;
use tracing::instrument;

use crate::store::CapacityBounds;
use crate::util::{MaybeLimited, RangeLen};

use super::super::Store;

use std::ops::Range;

/* TODO: Change to iter first from center to end 
 * then from center to begin <dvdsk noreply@davidsk.dev> */
pub(crate) struct RangeListIterator {
    pub(crate) list: Vec<Range<u64>>,
    pub(crate) dist_from_center: isize,
    pub(crate) mul: isize,
}

impl Iterator for RangeListIterator {
    type Item = Range<u64>;

    fn next(&mut self) -> Option<Self::Item> {
        let center = self.list.len() / 2;
        let idx = center as isize + self.mul * self.dist_from_center;

        self.dist_from_center += 1;
        self.mul *= 1;
        Some(self.list[idx as usize].clone())
    }
}

pub(crate) fn iter_by_importance(range_list: Vec<Range<u64>>) -> RangeListIterator {
    RangeListIterator {
        list: range_list,
        dist_from_center: 0,
        mul: 1,
    }
}

pub(crate) fn correct_for_capacity(
    needed_from_src: Vec<Range<u64>>,
    capacity: &CapacityBounds,
) -> RangeSet<u64> {
    let CapacityBounds::Limited(capacity) = capacity else {
        return RangeSet::from_iter(needed_from_src);
    };

    let mut free_capacity = capacity.get();
    let mut res = RangeSet::new();

    for mut range in iter_by_importance(needed_from_src) {
        if range.len() <= free_capacity {
            res.insert(range.clone());
            free_capacity -= range.len();
        } else {
            range.start = range.end - free_capacity;
            res.insert(range.clone());
            break;
        }
    }
    res
}

/// Get up to the number of ranges supported by the target around the
/// currently being read range. Prioritizes the currently being read range
/// and the ranges after it.
#[instrument(level = "trace", skip_all, ret)]
pub(super) fn ranges_we_can_take(src: &Store, target: &Store) -> Vec<Range<u64>> {
    let range_list: Vec<Range<u64>> = src.ranges().iter().cloned().collect();

    let res = range_list.binary_search_by_key(&src.last_read_pos(), |range| range.start);
    let range_currently_being_read = match res {
        Ok(pos_is_range_start) => pos_is_range_start,
        Err(pos_is_after_range_start) => pos_is_after_range_start,
    };

    let mut taking = Vec::with_capacity(range_list.len());

    let center = range_currently_being_read;
    if let MaybeLimited::Limited(n) = target.n_supported_ranges() {
        let end = range_list.len().min(center.saturating_add(n.get()));
        taking.extend_from_slice(&range_list[center..end]);

        let n_left = n.get().saturating_sub(taking.len());
        let start = center.saturating_sub(n_left);
        taking.extend_from_slice(&range_list[start..center]);
    } else {
        taking.extend_from_slice(&range_list[center..]);
        taking.extend_from_slice(&range_list[..center]);
    };

    taking
}
