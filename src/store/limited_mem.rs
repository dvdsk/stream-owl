///
/// while the buffer is double ended we abstract it as a single
/// slice of memory.
///
/// the buffer:
/// ------------------------------------------------------------------
/// |                                                                |
/// ------------------------------------------------------------------
/// ^
/// alreadyread
///
///
///
///
///
///
///
///
///
///
use derivative::Derivative;
use std::collections::{TryReserveError, VecDeque};
use std::num::NonZeroUsize;
use std::ops::Range;
use tracing::{debug, instrument, trace};

use rangemap::set::RangeSet;

use crate::util::{MaybeLimited, VecDequeExt};
use crate::RangeUpdate;

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Memory {
    #[derivative(Debug = "ignore")]
    buffer: VecDeque<u8>,
    pub(super) available_for_writing: usize,
    /// range of positions available, end non inclusive
    /// positions are measured from the absolute start of the stream
    range: Range<u64>,
    last_read_pos: u64,
}

#[derive(thiserror::Error, Debug)]
#[error("Refusing write while in the middle of a seek")]
pub struct SeekInProgress;

#[derive(thiserror::Error, Debug)]
#[error("Could not get enough memory from the OS")]
pub struct CouldNotAllocate(#[from] TryReserveError);

impl Memory {
    pub(super) fn new(max_cap: usize) -> Result<Self, CouldNotAllocate> {
        assert_ne!(max_cap, 0, "max_capacity must be at least larger then zero");
        let mut buffer = VecDeque::new();
        buffer.try_reserve_exact(max_cap)?;

        Ok(Self {
            last_read_pos: 0,
            buffer,
            available_for_writing: 0,
            range: 0..0,
        })
    }

    pub(super) fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    fn make_space_clearing_buffer(&mut self, pos: u64, data: &[u8]) -> (usize, Range<u64>) {
        self.buffer.clear();
        self.last_read_pos = pos;
        let removed = self.range.clone();
        self.range = pos..pos;
        let to_write = data.len().min(self.buffer.capacity());
        self.available_for_writing = self.buffer.capacity() - to_write;
        (to_write, removed)
    }

    fn make_space_draining_read(&mut self, data: &[u8]) -> (usize, Range<u64>) {
        let bytes_buf_can_grow = self.buffer.capacity() - self.buffer.len();
        let to_free = self
            .available_for_writing
            .min(data.len().saturating_sub(bytes_buf_can_grow));
        self.available_for_writing -= to_free;

        self.buffer.drain(..to_free);
        let removed = self.range.start..self.range.start + (to_free as u64);
        self.range.start += to_free as u64;
        let to_write = to_free + bytes_buf_can_grow;
        (to_write, removed)
    }

    /// `pos` must be the position of the first byte in buf in the stream.
    /// For the first write at the start this should be 0
    #[tracing::instrument(level="trace", skip(data), fields(buf_len = data.len()))]
    pub(super) async fn write_at(&mut self, data: &[u8], pos: u64) -> (usize, RangeUpdate) {
        assert!(!data.is_empty());
        let to_write;
        let removed;

        if pos != self.range.end {
            debug!("making space by clearing the whole buffer");
            (to_write, removed) = self.make_space_clearing_buffer(pos, data);
        } else {
            trace!("making space by draining part of the buffer");
            (to_write, removed) = self.make_space_draining_read(data);
        }

        self.buffer.extend(data[0..to_write].iter());
        let written = to_write;
        self.range.end += written as u64;

        let update = if removed.is_empty() {
            RangeUpdate::Added(self.range.clone())
        } else {
            RangeUpdate::Replaced {
                removed,
                new: self.range.clone(),
            }
        };
        (written, update)
    }

    /// we must only get here if there is data in the mem store for us
    #[instrument(skip(self, buf))]
    pub(super) fn read_at(&mut self, buf: &mut [u8], pos: u64) -> usize {
        debug_assert!(
            pos >= self.range.start,
            "No data in store at offset: {pos}, range: {:?}",
            self.range
        );

        let relative_pos = pos - self.range.start;
        let n_read = self.buffer.copy_starting_at(relative_pos as usize, buf);
        self.available_for_writing += n_read;

        tracing::info!(
            "relative_pos: {relative_pos}, pos: {pos}, range_start: {}",
            self.range.start
        );
        tracing::info!("buf: {:?}", buf);
        self.last_read_pos = pos + n_read as u64;
        n_read
    }
    pub(super) fn ranges(&self) -> RangeSet<u64> {
        let mut res = RangeSet::new();
        if !self.range.is_empty() {
            res.insert(self.range.clone());
        }
        res
    }
    pub(super) fn gapless_from_till(&self, pos: u64, last_seek: u64) -> bool {
        self.range.contains(&pos) && self.range.contains(&last_seek)
    }

    pub(super) fn last_read_pos(&self) -> u64 {
        self.last_read_pos
    }
    pub(super) fn n_supported_ranges(&self) -> MaybeLimited<NonZeroUsize> {
        MaybeLimited::Limited(NonZeroUsize::new(1).expect("1 is not zero"))
    }
}
