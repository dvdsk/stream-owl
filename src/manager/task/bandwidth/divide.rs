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

        let Some(biggest) = list.pop_biggest() else {
            return 0;
        };
        // let Some(next_biggest) = list.pop_biggest() else {
        //     let mut only_element = biggest;
        //     if wished_for <= only_element.allocated / 2 {
        //         only_element.allocated -= wished_for;
        //         list.insert(only_element);
        //         return wished_for;
        //     } else {
        //         only_element.allocated = only_element.allocated / 2;
        //         let allocated = only_element.allocated;
        //         list.insert(only_element);
        //         return allocated;
        //     }
        // };

        take_flattening_the_top(wished_for, biggest, list)
    }
}

/// list must be a sorted slice
/// Takes the minimum from the top allocations until:
///  A: The required amount is freed
///  B: The required amount is equal to the largest
fn take_flattening_the_top(
    wished_for: u32,
    biggest: AllocationInfo,
    mut list: &mut Allocations,
) -> u32 {
    // let placeholder = placeholder_info(wished_for);
    // list.insert(placeholder);

    // visualize the stack as N bars. Each loop we slice
    // off the top M bars. They therefore each time have the same
    // height. We slice them off such that they are of equal height
    // with the next highest bar. This results in M+1 new top height
    // bars.
    let total = list.total_allocated();
    let mut sliced_off = 0;
    let mut flat_top = vec![biggest];
    let mut flat_top_height = flat_top[0].allocated;

    let (final_height, freed) = loop {
        let Some(next) = list.pop_biggest() else {
            // everything is flat now, should be flat with the
            // new allocation added => mean
            let total_bw = sliced_off + flat_top_height * flat_top.len() as u32;
            let final_height = total_bw / (flat_top.len() as u32 + 1);
            let left_over = total_bw % (flat_top.len() as u32 + 1);
            break (final_height, final_height + left_over);
        };

        let next_height = next.allocated;
        // let next_height = next_height.min(wished_for);
        // if next_height < wished_for {
        //     dbg!();
        //     return 0;
        // }

        let slice_y = flat_top_height - next_height;
        let about_to_slice_off = slice_y * flat_top.len() as u32;

        // about to slice off more then we need, change the slice_y
        if sliced_off + about_to_slice_off >= wished_for {
            dbg!();
            let last_slice_size = wished_for - sliced_off;
            let slice_y = last_slice_size / flat_top.len() as u32;

            let final_height = flat_top_height - slice_y;
            break (final_height, wished_for);
        }

        // let left_in_list = total - sliced_off;
        // if dbg!(next_height < wished_for) && dbg!(left_in_list < wished_for) {
        //     dbg!();
        //     let total_bw = sliced_off + flat_top_height * flat_top.len() as u32;
        //     let final_height = total_bw / (flat_top.len() as u32 + 1);
        //     let left_over = total_bw % (flat_top.len() as u32 + 1);
        //     break (final_height, final_height + left_over);
        // }

        // do the slice
        sliced_off += about_to_slice_off;
        flat_top_height = next.allocated;
        flat_top.push(next);
    };

    for item in &mut flat_top {
        item.allocated = final_height;
    }

    list.extend(flat_top.into_iter().filter(not_placeholder));
    return freed;
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
        let biggest = list.pop_biggest().expect("list size > 1");

        let res = take_flattening_the_top(wished_for, biggest, &mut list);
        let list: Vec<_> = list.into_iter().map(|info| info.allocated).collect();
        assert_eq!(list.as_slice(), &resulting_bw);
        assert_eq!(res, got);
    }

    #[test]
    fn not_enough_bw() {
        do_test([20, 30, 32], 32, [20, 20, 20], 22)
    }

    #[test]
    fn one_stream() {
        do_test([32], 32, [16], 16)
    }

    #[test]
    fn big_then_small() {
        do_test([100, 10, 5], 80, [50, 10, 5], 50)
    }
}
