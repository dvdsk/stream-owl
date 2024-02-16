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

pub(crate) enum MaybeLimited<T> {
    Limited(T),
    NotLimited
}

pub(crate) fn fmt_non_printable_option<T>(
    retry_logger: &Option<T>,
    fmt: &mut std::fmt::Formatter,
) -> std::result::Result<(), std::fmt::Error> {
    if retry_logger.is_some() {
        fmt.write_str("Some(-not printable-)")
    } else {
        fmt.write_str("None")
    }
}
