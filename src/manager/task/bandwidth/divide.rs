use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::StreamId;

use super::{allocation::AllocationInfo, allocation::Allocations, Priority};

impl super::Controller {
    /// returns the undividable leftovers
    pub(super) fn redivide(&mut self, p: Priority, allocced: u32) -> u32 {
        let list = self
            .allocated_by_prio
            .get_mut(&p)
            .expect("there bandwidth allocated here");

        let mut to_divide = allocced;
        loop {
            let new_budget = to_divide / (list.len() as u32);
            let leftover = list
                .range(0..new_budget)
                .map(|info| info.allocated)
                .sum::<u32>();

            if leftover == 0 {
                break;
            }
            to_divide += allocced + leftover;
        }

        let new_budget = to_divide / (list.len() as u32);
        for over_budget in list.range_mut(new_budget..) {
            over_budget.allocated = new_budget;
        }

        let leftover = to_divide % (list.len() as u32);
        leftover
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
        flat_top_height = next.allocated;
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
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::Arc;

    use crate::manager::task::bandwidth::allocation::Bandwidth;
    use crate::StreamId;

    use super::*;

    fn info(bw: Bandwidth) -> AllocationInfo {
        AllocationInfo {
            id: StreamId::new(),
            target: crate::network::BandwidthAllowed::UnLimited,
            curr_io_limit: 0,
            allocated: bw,
            stream_still_exists: Arc::new(AtomicBool::new(true)),
        }
    }

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
    fn empty() {
        do_test([0], 80, [0], 0)
    }
}
