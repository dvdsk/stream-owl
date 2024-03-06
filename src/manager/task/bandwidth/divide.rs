use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::StreamId;

use super::allocation::Bandwidth;
use super::{allocation::AllocationInfo, allocation::Allocations, Priority};

impl super::Controller {
    /// takes from the highest allocations first
    /// returns the undividable leftovers
    pub(super) fn redivide(list: &mut Allocations, needed: u32) {
        let not_needed = list.total_allocated() - needed;

        // proposed maximum allocation for each stream based on mean
        // probably to low, we will adjust it shortly
        let mut curr_proposal = not_needed / list.len() as u32;

        // some streams are already allocated lower then the `proposed_budget`
        // those will not be affected we already have the bandwidth between
        // them and the proposal.

        // could take a few loops to get right, lets not take too many pefection
        // is the enemy of good
        for _ in 0..5 {
            let under_budget = list
                .range(0..curr_proposal)
                .map(|info| info.allocated)
                .sum::<u32>();

            let n_affected = list.range(curr_proposal..u32::MAX).count() as u32;
            let next_proposal = (not_needed - under_budget) / n_affected;
            if next_proposal == curr_proposal {
                break;
            } else {
                curr_proposal = next_proposal;
            }
        }

        let final_budget = curr_proposal;
        for over_budget in list.range_mut(final_budget..) {
            over_budget.allocated = final_budget;
        }

        let remainder = not_needed - list.total_allocated();
        list.biggest_mut() // order did not change so this was the biggest element
            .expect("list has non zero allocated, thus must be non empty")
            .allocated += remainder
    }

    pub(super) fn redivide_fair(&mut self, p: Priority, wished_for: u32) -> u32 {
        let Some(list) = self.allocated_by_prio.get_mut(&p) else {
            return 0;
        };

        take_flattening_the_top(wished_for, list)
    }
}

/// list must be a sorted slice
/// Takes the minimum from the top allocations until:
///  A: The required amount is freed
///  B: The required amount is equal to the largest
fn take_flattening_the_top(wished_for: u32, mut list: &mut Allocations) -> u32 {
    let placeholder = placeholder_info(wished_for);
    list.insert(placeholder);

    // visualize the list as N bars. Each loop we slice off the top of M bars
    // (we keep those in `flat top`. We start with M = 1, and increase M in
    // steps of one. We slice the M bars off such that they are the same height
    // as the highest next bar not in M. This grows M by one.
    let mut sliced_off = 0;
    let mut flat_top = vec![list
        .pop_biggest()
        .expect("at least placeholder is in the list")];
    let mut flat_top_height = flat_top[0].allocated;

    let (final_height, left_over) = loop {
        let Some(next) = list.pop_biggest() else {
            // since we added the placeholder with the wished allocation we
            // must at least slice off the placeholders allocated bytes
            let last_slice_size = wished_for - sliced_off;
            // everything is flat now so shave off what we still need
            let slice_y = last_slice_size.div_ceil(flat_top.len() as u32);
            let left_over = slice_y * (flat_top.len() as u32) - last_slice_size;

            let final_height = flat_top_height - slice_y;
            break (final_height, left_over);
        };

        let next_height = next.allocated;
        let slice_y = flat_top_height - next_height;
        let about_to_slice_off = slice_y * flat_top.len() as u32;

        // about to slice off more then we need, change the slice_y
        if sliced_off + about_to_slice_off >= wished_for {
            list.insert(next);
            let last_slice_size = wished_for - sliced_off;
            let slice_y = last_slice_size.div_ceil(flat_top.len() as u32);
            let left_over = slice_y * (flat_top.len() as u32) - last_slice_size;

            let final_height = flat_top_height - slice_y;
            break (final_height, left_over);
        }

        // do the slice
        sliced_off += about_to_slice_off;
        flat_top_height = next_height;
        flat_top.push(next);
    };

    for item in &mut flat_top {
        item.allocated = final_height;
    }

    let placeholder = flat_top
        .iter()
        .find(is_placeholder)
        .expect("we put it in at the top");
    let freed = placeholder.allocated + left_over;
    list.extend(flat_top.into_iter().filter(not_placeholder));
    return freed;
}

fn is_placeholder(item: &&AllocationInfo) -> bool {
    item.id == StreamId::placeholder()
}

fn not_placeholder(item: &AllocationInfo) -> bool {
    item.id != StreamId::placeholder()
}

fn placeholder_info(wished_for: u32) -> AllocationInfo {
    AllocationInfo {
        id: StreamId::placeholder(),
        target: crate::network::BandwidthAllowed::UnLimited,
        curr_io_limit: 0,
        allocated: wished_for,
        stream_still_exists: Arc::new(AtomicBool::new(true)),
        upstream_limit: super::allocation::Limit::Unknown,
    }
}

struct FlatBottom<'a> {
    limit: Bandwidth,
    height: Bandwidth,
    list: Vec<&'a mut AllocationInfo>,
}

impl<'a> FlatBottom<'a> {
    fn new(smallest: &'a mut AllocationInfo) -> Self {
        Self {
            height: smallest.allocated,
            limit: smallest.best_limit(),
            list: vec![smallest],
        }
    }

    fn len(&self) -> u32 {
        self.list.len() as u32
    }

    fn push(&mut self, new: &'a mut AllocationInfo) {
        self.list.push(new)
    }

    fn remove_any_at_limit(&mut self, curr_height: Bandwidth) {
        while let Some((idx, _)) = self
            .list
            .iter()
            .enumerate()
            .find(|(_, info)| info.best_limit() == curr_height)
        {
            let at_limit = self.list.remove(idx);
            at_limit.allocated = curr_height;
            eprintln!("removing: {}", curr_height);
        }

        self.height = curr_height.max(self.list[0].allocated);
        self.limit = self
            .list
            .iter()
            .map(|info| info.best_limit())
            .min()
            .expect("flat_bottom is never empty");
    }

    fn apply(&mut self, final_height: u32) {
        for item in &mut self.list {
            item.allocated = final_height;
        }
    }
}

/// returns left over bandwidth
pub(crate) fn divide_new_bw<'a>(
    mut iter: impl Iterator<Item = &'a mut AllocationInfo>,
    to_divide: Bandwidth,
) -> Bandwidth {
    let Some(smallest) = iter.next() else {
        return 0;
    };

    let mut spread_out = 0;
    let mut flat = FlatBottom::new(smallest);

    let (final_height, left_over) = loop {
        let Some(next) = iter.next() else {
            break divide_within_flat(&mut flat, &mut spread_out, to_divide);
        };

        let next_height = next.allocated.min(flat.limit);
        let slice_y = next_height - flat.height;
        let about_to_spread_out = slice_y * flat.len();

        // about to slice off more then we need, change the slice_y
        if spread_out + about_to_spread_out >= to_divide {
            let last_slice_size = to_divide - spread_out;
            let slice_y = last_slice_size.div_ceil(flat.len());
            let left_over = slice_y * flat.len() - last_slice_size;

            let final_height = flat.height - slice_y;
            break (final_height, left_over);
        }

        spread_out += about_to_spread_out;
        flat.push(next);
        flat.remove_any_at_limit(next_height);
    };

    flat.apply(final_height);
    left_over
}

fn divide_within_flat(
    flat: &mut FlatBottom<'_>,
    spread_out: &mut u32,
    to_divide: u32,
) -> (u32, u32) {
    while flat.len() > 1 {
        let ideal_slice_size = to_divide - *spread_out;
        let ideal_slice = ideal_slice_size / flat.len();
        let next_height = flat.height + ideal_slice;

        if next_height > flat.limit {
            let slice_y = flat.limit - flat.height;
            let about_to_spread_out = slice_y * flat.len();
            *spread_out += about_to_spread_out;
            flat.remove_any_at_limit(flat.limit);
        } else {
            break;
        }
    }
    let ideal_slice_size = to_divide - *spread_out;
    let ideal_slice_y = ideal_slice_size / flat.len();
    let final_height = (flat.height + ideal_slice_y).min(flat.limit);
    let slice_y = final_height - flat.height;
    let about_to_spread_out = slice_y * flat.len();
    let left_over = to_divide - *spread_out - about_to_spread_out;

    (final_height, left_over)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use crate::manager::task::bandwidth::allocation::{self, Bandwidth};
    use crate::StreamId;

    use super::*;

    fn info(bw: Bandwidth) -> AllocationInfo {
        AllocationInfo {
            id: StreamId::new(),
            target: crate::network::BandwidthAllowed::UnLimited,
            curr_io_limit: 0,
            allocated: bw,
            stream_still_exists: Arc::new(AtomicBool::new(true)),
            upstream_limit: allocation::Limit::Unknown,
        }
    }

    mod redivide {
        use crate::manager::task::bandwidth::Controller;

        use super::*;

        fn do_test<const N: usize>(existing_bw: [u32; N], needed: u32, resulting_bw: [u32; N]) {
            let mut list = Allocations::default();
            (&mut list).extend(existing_bw.into_iter().map(info));

            Controller::redivide(&mut list, needed);
            let list: Vec<_> = list.into_iter().map(|info| info.allocated).collect();
            assert_eq!(list.as_slice(), &resulting_bw);
            assert_eq!(
                existing_bw.iter().sum::<u32>(),
                resulting_bw.iter().sum::<u32>() + needed
            );
        }

        #[test]
        fn big_then_small() {
            do_test([100, 10, 5], 80, [5, 10, 20])
        }

        #[test]
        fn needs_two_loops() {
            do_test([91, 19, 5], 80, [5, 15, 15])
        }

        #[test]
        fn one_element() {
            do_test([10], 5, [5])
        }
    }

    mod flatten_top {
        use super::*;

        fn do_test<const N: usize>(
            existing_bw: [u32; N],
            wished_for: u32,
            resulting_bw: [u32; N],
            got: u32,
        ) {
            let mut list = Allocations::default();
            (&mut list).extend(existing_bw.into_iter().map(info));

            let res = take_flattening_the_top(wished_for, &mut list);
            let list: Vec<_> = list.into_iter().map(|info| info.allocated).collect();
            assert_eq!(list.as_slice(), &resulting_bw);
            assert_eq!(res, got, "allocated bandwidth is wrong");
            assert_eq!(
                existing_bw.iter().sum::<u32>(),
                resulting_bw.iter().sum::<u32>() + got
            );
        }

        #[test]
        fn not_enough_bw() {
            do_test([20, 30, 32], 32, [20, 20, 20], 22)
        }

        #[test]
        fn already_flat() {
            do_test([32], 32, [16], 16)
        }

        #[test]
        fn one_smaller_stream() {
            do_test([10], 32, [5], 5)
        }

        #[test]
        fn big_then_small() {
            do_test([100, 10, 5], 80, [5, 10, 50], 50)
        }

        #[test]
        fn no_bw() {
            do_test([0], 80, [0], 0)
        }
    }

    mod divide_new {
        use crate::manager::task::bandwidth::allocation::Limit;

        use super::*;

        fn info((bw, limit): (Bandwidth, Limit)) -> AllocationInfo {
            AllocationInfo {
                id: StreamId::new(),
                target: crate::network::BandwidthAllowed::UnLimited,
                curr_io_limit: 0,
                allocated: bw,
                stream_still_exists: Arc::new(AtomicBool::new(true)),
                upstream_limit: limit,
            }
        }

        fn g(n: Bandwidth) -> Limit {
            Limit::Guess(n)
        }

        fn u() -> Limit {
            Limit::Unknown
        }

        fn do_test<const N: usize>(
            existing_bw: [(Bandwidth, Limit); N],
            to_divide: Bandwidth,
            resulting_bw: [Bandwidth; N],
            left_over: Bandwidth,
        ) {
            let mut list = Allocations::default();
            (&mut list).extend(existing_bw.clone().into_iter().map(info));

            let res = divide_new_bw(list.iter_mut(), to_divide);
            let list: Vec<_> = list.into_iter().map(|info| info.allocated).collect();
            assert_eq!(list.as_slice(), &resulting_bw);
            assert_eq!(res, left_over);
            assert_eq!(
                existing_bw.iter().map(|(bw, _)| bw).sum::<u32>() + to_divide,
                resulting_bw.iter().sum::<u32>() + left_over,
                "sum of existing allocations + new != new allocations + left over"
            );
        }

        #[test]
        fn no_bw() {
            do_test([(0, g(40))], 80, [40], 40)
        }

        #[test]
        fn no_bw_no_lim() {
            do_test([(0, u())], 80, [80], 0)
        }

        #[test]
        fn no_leftovers() {
            do_test([(0, g(5)), (10, u()), (20, g(25))], 20, [5, 22, 22], 1)
        }

        #[test]
        fn large_leftovers() {
            do_test([(0, g(5)), (10, u()), (20, g(25))], 50, [5, 50, 25], 0)
        }
    }
}
