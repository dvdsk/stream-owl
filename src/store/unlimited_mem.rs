use derivative::Derivative;
use std::collections::TryReserveError;
use std::num::NonZeroUsize;
use std::ops::Range;
use tracing::{debug, instrument};

use rangemap::set::RangeSet;

use crate::RangeUpdate;

mod range_store;
use range_store::RangeStore;

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Memory {
    #[derivative(Debug = "ignore")]
    buffer: RangeStore,
    /// the range currently being added to
    active_range: Range<u64>,
    last_read_pos: u64,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Not critical
    #[error("Refusing write while in the middle of a seek")]
    SeekInProgress,
    #[error("Could not get enough memory from the OS")]
    CouldNotAllocate(#[from] CouldNotAllocate),
}

// needed in store::Error as SeekInProgress is separated from
// all other errors there
#[derive(thiserror::Error, Debug)]
#[error("Could not get enough memory from the OS")]
pub struct CouldNotAllocate(#[from] TryReserveError);

impl Memory {
    pub(super) fn new() -> Self {
        Self {
            last_read_pos: 0,
            buffer: RangeStore::new(),
            active_range: 0..0,
        }
    }

    #[tracing::instrument(level="trace", skip(buf), fields(buf_len = buf.len()))]
    pub(super) async fn write_at(
        &mut self,
        buf: &[u8],
        pos: u64,
    ) -> Result<(NonZeroUsize, RangeUpdate), Error> {
        assert!(!buf.is_empty());
        if pos != self.active_range.end {
            debug!("refusing write: position not at current range end, seek must be in progress");
            return Err(Error::SeekInProgress);
        }

        self.buffer.append_at(pos, buf).map_err(CouldNotAllocate)?;
        let written = buf.len();

        self.active_range.end += written as u64;
        let update = RangeUpdate::Added(self.active_range.clone());
        let written =
            NonZeroUsize::new(written).expect("should never be passed a zero sized write");
        return Ok((written, update));
    }

    /// we must only get here if there is data in the mem store for us
    pub(super) fn read_at(&mut self, buf: &mut [u8], pos: u64) -> usize {
        debug_assert!(
            pos >= self.active_range.start,
            "No data in store at offset: {pos}"
        );

        let n_copied = self.buffer.copy_at(pos, buf);
        self.last_read_pos = pos;
        n_copied
    }
    pub(super) fn ranges(&self) -> RangeSet<u64> {
        self.buffer.ranges()
    }
    pub(super) fn gapless_from_till(&self, pos: u64, last_seek: u64) -> bool {
        self.buffer
            .ranges()
            .gaps(&(pos..last_seek))
            .next()
            .is_none()
    }
    pub(super) fn last_read_pos(&self) -> u64 {
        self.last_read_pos
    }
    pub(super) fn n_supported_ranges(&self) -> usize {
        usize::MAX
    }
    #[instrument(level = "debug")]
    pub(super) fn writer_jump(&mut self, to_pos: u64) {
        debug_assert!(!self.active_range.contains(&to_pos));

        self.active_range = to_pos..to_pos;
        self.last_read_pos = to_pos;
    }
}
