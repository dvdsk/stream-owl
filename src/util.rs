use core::ops::Range;

mod vecdeque;
pub(crate) use vecdeque::{VecDequeExt, vecd};

pub(crate) trait RangeLen {
    fn len(&self) -> u64;
}

impl RangeLen for Range<u64> {
    fn len(&self) -> u64 {
        self.end - self.start
    }
}
