use std::collections::HashMap;
use std::iter;

use crate::StreamId;

use super::allocation::AllocationInfo;
use super::allocation::Allocations;
use super::allocation::Bandwidth;

#[cfg(test)]
mod tests;

/// Frees up `needed` bandwidth from allocations at a lower priority.
pub(super) fn take(mut allocations: Allocations, needed: u32) {
    // visualize the allocations as a hillside. We need an amount of dirt.
    // This algorithm digs it up starting at the hills top. Eventually we
    // will have the right amount of dirt and be left with a flat topped
    // hill.
    //
    // To translate the analogy:
    // - The hill is a sorted list of the current bandwidth allocations
    //   the largest allocation forming the peak of the hill.
    // - The height in is the maximum bandwidth of a stream.
    // - The amount of dirt is the total bandwidth freed
    let biggest_peak_idx = allocations
        .iter()
        .enumerate()
        .max_by_key(|(_, info)| info.allocated)
        .map(|(idx, _)| idx);
    let planned_hill_size = allocations.total_bandwidth() - needed;

    // Guess the new maximum height for the mountain. This assumes the
    // mountain is a cube. If its not we get too much dirt and we increase
    // the height.
    let hill_width = allocations.numb_streams();
    let mut proposed_height = planned_hill_size / hill_width;

    // Could take a few loops to get right, lets not take too many, we may
    // be on a deadline
    for _ in 0..5 {
        let dirt_under_height = allocations
            .iter_bandwidth_range(0..proposed_height)
            .map(|info| info.allocated)
            .sum::<u32>();

        let hill_width_above_height = allocations
            .iter_bandwidth_range(proposed_height..u32::MAX)
            .count() as u32;

        // Ignore the part of the hill under the previously proposed height
        // and make a new proposal for a height above which to dig all the
        // dirt away
        let next = (planned_hill_size - dirt_under_height) / hill_width_above_height;
        if next == proposed_height {
            break;
        } else {
            proposed_height = next;
        }
    }

    let final_height = proposed_height;
    for too_high in allocations.iter_mut_bandwidth_range(final_height..) {
        too_high.allocated = final_height;
    }

    // we may have a little dirt left, not enough to spread out though
    // give it to what was the biggest peak
    let final_hill_size = allocations.total_bandwidth();
    let left_over_dirt = final_hill_size - planned_hill_size;
    if let Some(biggest_peak) = biggest_peak_idx.and_then(|index| allocations.get_mut(index)) {
        biggest_peak.allocated += left_over_dirt
    }
}

pub(super) fn spread(allocations: Allocations, wished_for: u32) -> u32 {
    take_flattening_the_top(wished_for, allocations)
}

/// list must be a sorted slice
/// Takes the minimum from the top allocations until:
///  A: The required amount is freed
///  B: The required amount is equal to the largest
fn take_flattening_the_top<'a>(wished_for: u32, mut list: Allocations) -> u32 {
    list.insert_placeholder(wished_for);

    // visualize the list as N bars. Each loop we slice off the top of M bars
    // (we keep those in `flat top`. We start with M = 1, and increase M in
    // steps of one. We slice the M bars off such that they are the same height
    // as the highest next bar not in M. This grows M by one.
    let mut sliced_off = 0;
    let mut flat_top = vec![list
        .remove_biggest()
        .expect("at least placeholder is in the list")];
    let mut flat_top_height = flat_top[0].allocated();

    let (final_height, left_over) = loop {
        let Some(next) = list.remove_biggest() else {
            // since we added the placeholder with the wished allocation we
            // must at least slice off the placeholders allocated bytes
            let last_slice_size = wished_for - sliced_off;
            // everything is flat now so shave off what we still need
            let slice_y = last_slice_size.div_ceil(flat_top.len() as u32);
            let left_over = slice_y * (flat_top.len() as u32) - last_slice_size;

            let final_height = flat_top_height - slice_y;
            break (final_height, left_over);
        };

        let next_height = next.allocated();
        let slice_y = flat_top_height - next_height;
        let about_to_slice_off = slice_y * flat_top.len() as u32;

        // about to slice off more than we need, change the slice_y
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
        .find(|info| info.is_placeholder())
        .expect("we put it in at the top");
    let freed = placeholder.allocated + left_over;
    list.extend(flat_top.into_iter().filter(|info| !info.is_placeholder()));
    return freed;
}

struct FlatTop {
    // limit: Bandwidth,
    // height: Bandwidth,
    in_top: Vec<AllocationInfo>,
    // removed: Vec<AllocationInfo>,
}

impl Drop for FlatTop {
    fn drop(&mut self) {
        if self.in_top.is_empty() {
            //&& self.removed.is_empty() {
            return;
        }

        eprintln!("items should have been reinserted into the allocations list");
        dbg!(&self.in_top); //, &self.removed);
        panic!();
    }
}

struct FlatBottom {
    limit: Bandwidth,
    height: Bandwidth,
    in_bottom: Vec<AllocationInfo>,
    removed: Vec<AllocationInfo>,
}

impl Drop for FlatBottom {
    fn drop(&mut self) {
        if self.in_bottom.is_empty() && self.removed.is_empty() {
            return;
        }

        eprintln!("items should have been reinserted into the allocations list");
        dbg!(&self.in_bottom, &self.removed);
        panic!();
    }
}

impl FlatBottom {
    fn new(smallest: AllocationInfo) -> Self {
        Self {
            height: smallest.allocated,
            limit: smallest.best_limit(),
            in_bottom: vec![smallest],
            removed: Vec::new(),
        }
    }

    fn len(&self) -> u32 {
        self.in_bottom.len() as u32
    }

    /// You must return the pushed value to the allocations list
    /// before dropping flat. Use [`Self::drain`] for that.
    fn push(&mut self, new: AllocationInfo) {
        assert!(new.allocated >= self.height);
        self.in_bottom.push(new)
    }

    /// Must be called before dropping flat
    fn drain(&mut self, mut allocations: Allocations) {
        allocations.extend(self.removed.drain(..));
        allocations.extend(self.in_bottom.drain(..));
        dbg!();
    }

    fn remove_any_at_limit(&mut self, curr_height: Bandwidth) {
        while let Some((idx, _)) = self
            .in_bottom
            .iter()
            .enumerate()
            .find(|(_, info)| info.best_limit() == curr_height)
        {
            let mut at_limit = self.in_bottom.remove(idx);
            at_limit.allocated = curr_height;
            self.removed.push(at_limit);
            eprintln!("removing: {}", curr_height);
        }

        self.height = curr_height.max(self.in_bottom[0].allocated);
        self.limit = self
            .in_bottom
            .iter()
            .map(|info| info.best_limit())
            .min()
            .expect("flat_bottom is never empty");
    }

    fn apply(&mut self, final_height: u32) {
        for item in &mut self.in_bottom {
            item.allocated = final_height;
        }
    }
}

/// returns left over bandwidth
pub(crate) fn divide_new_bw(mut allocations: Allocations, to_divide: Bandwidth) -> Bandwidth {
    // take care to re-insert every item after modification
    let Some(smallest) = allocations.remove_smallest() else {
        return 0;
    };

    let mut spread_out = 0;
    let mut flat = FlatBottom::new(smallest);

    let (final_height, left_over) = loop {
        let Some(next) = allocations.remove_smallest() else {
            break divide_within_flat(&mut flat, &mut spread_out, to_divide);
        };

        let next_height = next.allocated().min(flat.limit);
        let slice_y = next_height - flat.height;
        let about_to_spread_out = slice_y * flat.len();

        // about to slice off more than we need, change the slice_y
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
    flat.drain(allocations);
    left_over
}

fn divide_within_flat(flat: &mut FlatBottom, spread_out: &mut u32, to_divide: u32) -> (u32, u32) {
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

/// Take the given Bandwidth (perbutation) and spread it out. Give streams that
/// are steady (have not dropped their spread recently) and are farthest from
/// their limit the most extra bandwidth.
///
/// Returns the bandwidth that could not be spread out
pub(crate) fn spread_perbutation(
    perbutation: Bandwidth,
    decreasing_steadyness: Vec<(&StreamId, &super::BandwidthInfo)>,
    allocations: &mut HashMap<StreamId, AllocationInfo>,
) -> Bandwidth {
    let mut decreasing_steadyness = decreasing_steadyness.into_iter();
    let best = decreasing_steadyness.next().unwrap();
    let spread_factor = 6.0; // lower is more spread out

    let mut ratios: Vec<(StreamId, f32)> = iter::once((*best.0, 1.0))
        .chain(decreasing_steadyness.map(|(id, item)| {
            let dist_to_best = best.1.steadyness - item.steadyness;
            let mul = dist_to_best * spread_factor;
            let ratio = (1.0 - mul).max(0.0);
            (*id, ratio)
        }))
        .collect();
    let mut total: f32 = ratios.iter().map(|(_, ratio)| ratio).sum();

    ratios.sort_unstable_by(|(a_id, a_ratio), (b_id, b_ratio)| {
        let a_free = allocations
            .get(a_id)
            .expect("nothing has been removed")
            .until_limit();
        let a_key = a_ratio * (a_free as f32);

        let b_free = allocations
            .get(b_id)
            .expect("nothing has been removed")
            .until_limit();
        let b_key = b_ratio * (b_free as f32);
        a_key.total_cmp(&b_key)
    });

    let mut unused = 0;
    for (id, ratio) in ratios.iter().rev() {
        let share = ratio / total;
        let naive_allocation = (share * (perbutation as f32)) as u32;

        let info = allocations.get_mut(&id).expect("nothing has been removed");
        let possible_alloction = naive_allocation.min(info.until_limit());

        unused += naive_allocation - possible_alloction;
        if unused > 0 {
            let unused_share = (unused as f32) / (naive_allocation as f32);
            total += unused_share;
        }

        info.allocated += possible_alloction;
    }

    // TODO is this not always zero?
    dbg!(unused)
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
