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

mod take {
    use super::*;

    fn do_test<const N: usize>(existing_bw: [u32; N], needed: u32, resulting_bw: [u32; N]) {
        let mut list: HashMap<_, _> = existing_bw
            .into_iter()
            .map(info)
            .map(|info| (info.id, info))
            .collect();

        let changes = take(list.values(), needed);
        for (id, change) in changes {
            list.entry(id).and_modify(|info| info.allocated -= change);
        }

        let mut list: Vec<_> = list.into_iter().collect();
        list.sort_by_key(|(id, _)| *id);
        let list: Vec<_> = list.into_iter().map(|(_, info)| info.allocated).collect();
        assert_eq!(list.as_slice(), &resulting_bw);
        assert_eq!(
            existing_bw.iter().sum::<u32>(),
            resulting_bw.iter().sum::<u32>() + needed
        );
    }

    #[test]
    fn big_then_small() {
        // should take all bandwidth from the biggest
        // leave the small alone
        do_test([100, 10, 5], 80, [20, 10, 5])
    }

    #[test]
    fn needs_two_loops() {
        do_test([91, 19, 5], 80, [15, 15, 5])
    }

    #[test]
    fn one_element() {
        do_test([10], 5, [5])
    }
}

mod flatten_top {
    use std::collections::BTreeMap;

    use super::*;

    fn do_test<const N: usize>(
        existing_bw: [u32; N],
        wished_for: u32,
        resulting_bw: [u32; N],
        got: u32,
    ) {
        let mut drain = HashMap::new();
        let list = existing_bw.into_iter().map(info).collect();
        let allocations = Allocations::new(list, &mut drain);

        let res = take_flattening_the_top(wished_for, allocations);
        let list: Vec<_> = drain
            .into_iter()
            .collect::<BTreeMap<_, _>>() // sort it
            .into_iter()
            .map(|(_, info)| info.allocated)
            .collect();
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
        do_test([100, 10, 5], 80, [50, 10, 5], 50)
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
        let mut drain = HashMap::new();
        let list = existing_bw.iter().cloned().map(info).collect();
        let allocations = Allocations::new(list, &mut drain);
        let res = divide_new_bw(allocations, to_divide);

        let mut list: Vec<_> = drain.into_iter().collect();
        list.sort_by_key(|(id, _)| *id);
        let list: Vec<_> = list.iter_mut().map(|(_, info)| info.allocated).collect();
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
    use std::time::Instant;

    use crate::manager::task::bandwidth::BandwidthInfo;

    use super::*;

    #[derive(Debug, Clone)]
    struct TestEntry {
        steadyness: f32,
        till_upstream_limit: Bandwidth,
    }

    fn alloc_info((entry, id): (TestEntry, StreamId)) -> AllocationInfo {
        AllocationInfo {
            id,
            target: crate::network::BandwidthAllowed::UnLimited,
            curr_io_limit: 100,
            allocated: 100,
            stream_still_exists: Arc::new(AtomicBool::new(true)),
            upstream_limit: allocation::Limit::Guess(100 + entry.till_upstream_limit),
        }
    }

    fn bw_info(entry: TestEntry) -> BandwidthInfo {
        BandwidthInfo {
            steadyness: entry.steadyness,
            since_last_sweep: Vec::new(),
            prev_normal_sweep: None,
            newest_update: Instant::now(),
            prev_sweep: None,
        }
    }

    // existing_bw is: (steadyness, bandwidth_limit)
    fn do_test<const N: usize>(
        existing_bw: [TestEntry; N],
        perbutation: Bandwidth,
        expected_resulting_bw: [Bandwidth; N],
        expected_left_over: Bandwidth,
    ) {
        let mut allocations = HashMap::new();
        let ids: Vec<_> = existing_bw.iter().map(|_| StreamId::new()).collect();
        allocations.extend(
            existing_bw
                .clone()
                .into_iter()
                .zip(ids.iter().cloned())
                .map(alloc_info)
                .map(|info| (info.id, info)),
        );

        let bw_info: HashMap<_, _> = ids
            .clone()
            .into_iter()
            .zip(existing_bw.clone().into_iter().map(bw_info))
            .collect();

        let (changes, left_over) =
            spread_prioritize_steadiest(perbutation, &bw_info, &mut allocations);
        dbg!(&changes);
        let bandwidth_increases: Vec<_> = ids
            .into_iter()
            .map(|id| changes.get(&id).unwrap())
            .copied()
            .collect();
        assert_eq!(bandwidth_increases.as_slice(), &expected_resulting_bw);
        assert_eq!(
            perbutation,
            bandwidth_increases.iter().sum::<Bandwidth>() + left_over,
            "perbutation: {perbutation}, should be equal to sum(bandwidth_increases): {} + left over: {left_over}", bandwidth_increases.iter().sum::<Bandwidth>()
        );
        assert_eq!(left_over, expected_left_over);
    }

    #[test]
    #[rustfmt::skip]
    fn all_super_steady() {
        do_test(
            [
                TestEntry { steadyness: 0.99, till_upstream_limit: 40 },
                TestEntry { steadyness: 0.99, till_upstream_limit: 10 },
                TestEntry { steadyness: 0.90, till_upstream_limit: 30 },
            ],
            9,
            [4, 4, 1],
            0,
        )
    }

    #[test]
    #[rustfmt::skip]
    fn high_with_little_bw_low_with_lots() {
        do_test(
            [
                 TestEntry { steadyness: 0.99, till_upstream_limit: 5 },
                 TestEntry { steadyness: 0.99, till_upstream_limit: 10 },
                 TestEntry { steadyness: 0.85, till_upstream_limit: 40 },
            ],
            20,
            [5, 10, 5],
            0,
        )
    }

    #[test]
    #[rustfmt::skip]
    fn long_list() {
        do_test(
            [
                 TestEntry { steadyness: 0.99, till_upstream_limit: 5 },
                 TestEntry { steadyness: 0.98, till_upstream_limit: 10 }, 
                 TestEntry { steadyness: 0.95, till_upstream_limit: 40 }, 
                 TestEntry { steadyness: 0.93, till_upstream_limit: 5 },
                 TestEntry { steadyness: 0.91, till_upstream_limit: 10 },
                 TestEntry { steadyness: 0.90, till_upstream_limit: 40 },
                 TestEntry { steadyness: 0.85, till_upstream_limit: 40 },
                 TestEntry { steadyness: 0.83, till_upstream_limit: 5 },
                 TestEntry { steadyness: 0.81, till_upstream_limit: 10 },
                 TestEntry { steadyness: 0.80, till_upstream_limit: 40 },
            ],
            20,
            [5, 5, 4, 3, 2, 1, 0, 0, 0, 0],
            0,
        )
    }

    #[test]
    #[rustfmt::skip]
    fn not_enough_room() {
        do_test(
            [
                TestEntry { steadyness: 0.99, till_upstream_limit: 1},
                TestEntry { steadyness: 0.98, till_upstream_limit: 1},
                TestEntry { steadyness: 0.95, till_upstream_limit: 1},
                TestEntry { steadyness: 0.93, till_upstream_limit: 1},
                TestEntry { steadyness: 0.91, till_upstream_limit: 1},
                TestEntry { steadyness: 0.90, till_upstream_limit: 1},
                TestEntry { steadyness: 0.85, till_upstream_limit: 1},
                TestEntry { steadyness: 0.83, till_upstream_limit: 1},
                TestEntry { steadyness: 0.81, till_upstream_limit: 1},
                TestEntry { steadyness: 0.80, till_upstream_limit: 1},
            ],
            20,
            [1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
            10,
        )
    }
}
