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
        let mut drain = HashMap::new();
        let list = existing_bw.into_iter().map(info).collect();

        {
            let allocations = Allocations::new(list, &mut drain);
            take(allocations, needed);
        }

        let mut list: Vec<_> = drain.into_iter().collect();
        list.sort_by_key(|(id, _)| *id);
        dbg!(&list);
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

        let res =
            spread_perbutation(perbutation, increasing_steadyness_borrow, &mut allocations);
        let extra: Vec<_> = ids
            .into_iter()
            .map(|id| allocations.get(&id).unwrap().allocated - 100)
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

