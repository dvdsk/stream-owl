use std::collections::BTreeMap;
use std::iter;
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

    mod spread_perbutation {
        use crate::manager::task::bandwidth::BandwidthInfo;

        use super::*;

        fn alloc_info(((_, growable), id): ((f32, Bandwidth), StreamId)) -> AllocationInfo {
            AllocationInfo {
                id,
                target: crate::network::BandwidthAllowed::UnLimited,
                curr_io_limit: 100,
                allocated: 100,
                stream_still_exists: Arc::new(AtomicBool::new(true)),
                upstream_limit: allocation::Limit::Guess(100 + growable),
            }
        }

        fn bw_info((streadyness, _): (f32, Bandwidth)) -> BandwidthInfo {
            BandwidthInfo {
                curr: 100,
                steadyness: streadyness,
            }
        }

        fn do_test<const N: usize>(
            existing_bw: [(f32, Bandwidth); N],
            perbutation: Bandwidth,
            resulting_bw: [Bandwidth; N],
            left_over: Bandwidth,
        ) {
            let mut list = Allocations::default();
            let ids: Vec<_> = existing_bw.iter().map(|_| StreamId::new()).collect();

            (&mut list).extend(
                existing_bw
                    .clone()
                    .into_iter()
                    .zip(ids.iter().cloned())
                    .map(alloc_info),
            );
            let mut allocated_by_prio = [(Priority::highest(), list)].into_iter().collect();
            let increasing_steadyness: Vec<_> = ids
                .clone()
                .into_iter()
                .zip(existing_bw.clone().into_iter().map(bw_info))
                .collect();
            let increasing_steadyness_borrow: Vec<_> = increasing_steadyness
                .iter()
                .map(|(id, info)| (id, info))
                .collect();

            // is sorted
            increasing_steadyness_borrow.iter().fold(0, |last, curr| {
                assert!(curr.1.curr >= last);
                curr.1.curr
            });

            let res = spread_perbutation(
                perbutation,
                increasing_steadyness_borrow,
                &mut allocated_by_prio,
            );
            let list = allocated_by_prio.remove(&Priority::highest()).unwrap();
            let extra: Vec<_> = ids
                .into_iter()
                .map(|id| list.get(id).unwrap().allocated - 100)
                .collect();
            assert_eq!(extra.as_slice(), &resulting_bw);
            assert_eq!(res, left_over);
            assert_eq!(perbutation, extra.iter().sum::<Bandwidth>() + left_over);
        }

        #[test]
        fn all_super_steady() {
            do_test([(0.99, 40), (0.99, 10), (0.90, 30)], 9, [3, 4, 2], 0)
        }

        #[test]
        fn high_with_little_bw_low_with_lots() {
            do_test([(0.99, 5), (0.99, 10), (0.85, 40)], 20, [5, 10, 5], 0)
        }

        #[test]
        fn long_list() {
            do_test(
                [
                    (0.99, 5),
                    (0.98, 10),
                    (0.95, 40),
                    (0.93, 5),
                    (0.91, 10),
                    (0.90, 40),
                    (0.85, 40),
                    (0.83, 5),
                    (0.81, 10),
                    (0.80, 40),
                ],
                20,
                [4, 4, 3, 3, 3, 3, 0, 0, 0, 0],
                0,
            )
        }

        #[test]
        fn not_enough_room() {
            do_test(
                [
                    (0.99, 1),
                    (0.98, 1),
                    (0.95, 1),
                    (0.93, 1),
                    (0.91, 1),
                    (0.90, 1),
                    (0.85, 1),
                    (0.83, 1),
                    (0.81, 1),
                    (0.80, 1),
                ],
                20,
                [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
                0,
            )
        }
    }
}

pub(crate) fn spread_perbutation(
    perbutation: u32,
    decreasing_steadyness: Vec<(&StreamId, &super::BandwidthInfo)>,
    allocated_by_prio: &mut BTreeMap<Priority, Allocations>,
) -> Bandwidth {
    let mut decreasing_steadyness = decreasing_steadyness.into_iter();
    let best = decreasing_steadyness.next().unwrap();
    let spread_factor = 6.0;

    let shares: Vec<_> = iter::once((best.0, 1.0))
        .chain(decreasing_steadyness.map(|(id, bandwidth)| {
            let dist_to_best = dbg!(best.1.steadyness) - dbg!(bandwidth.steadyness);
            let mul = dbg!(dist_to_best) * spread_factor;
            let share = 1.0 - mul;
            (id, share)
        }))
        .collect();
    let total: f32 = shares.iter().map(|(_, share)| share).sum();
    dbg!(&shares, total);

    let mut leftover = perbutation;
    let mut total_percentage_left = 1.0;
    let mut unused_percentage = 0.0;
    for (id, share) in shares {
        let alloc = get_alloc_by_id(*id, allocated_by_prio);

        let allocated_percentage = share / total;
        let percentage_of_leftover = allocated_percentage / total_percentage_left;
        let percentage_of_unused = percentage_of_leftover * unused_percentage;
        unused_percentage -= percentage_of_unused;
        // rescale left over unused

        let share_percentage = allocated_percentage + percentage_of_unused;
        dbg!(share_percentage, allocated_percentage, percentage_of_unused);
        let share_bw = perbutation as f32 * share_percentage;
        let final_share = (share_bw as u32).min(alloc.until_limit());

        dbg!(final_share, share_bw, share_percentage);
        alloc.allocated += final_share;
        leftover -= final_share;

        let new_unused = share_bw - final_share as f32;
        let new_unused_percentage = new_unused / perbutation as f32;
        unused_percentage += new_unused_percentage;
        total_percentage_left -= allocated_percentage;
    }

    leftover
}

// fn max_steadyness(increasing_steadyness: Vec<(&StreamId, &super::BandwidthInfo)>) -> f32 {
//     let min = match increasing_steadyness.len() {
//         1 => 1.0,
//         2 => 0.9,
//         3 => 0.85,
//         4 => 0.82,
//
//     }
// }

fn get_alloc_by_id(
    id: StreamId,
    allocated_by_prio: &mut BTreeMap<Priority, Allocations>,
) -> &mut AllocationInfo {
    allocated_by_prio
        .values_mut()
        .find(|list| list.get(id).is_some())
        .expect("if an allocation in bandwidth_by_id it must be in here too")
        .get_mut(id)
        .expect("see prev")
}
