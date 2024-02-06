use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::{instrument, trace};

mod chunk_size;
use chunk_size::ChunkSize;
pub(crate) use chunk_size::ChunkSizeBuilder;

macro_rules! tracing_record {
    ($range:ident) => {
        tracing::Span::current().record(
            stringify!($range),
            $range
                .as_ref()
                .map(|Range { start, end }| format!("{start}..{end}")),
        );
    };
}

use crate::http_client::Size;
use crate::store::StoreWriter;

#[derive(Debug)]
pub(crate) struct StreamTarget {
    /// on seek this pos is updated, in between seeks
    /// it increments with the number of bytes written
    pos: AtomicU64,
    store: StoreWriter,
    pub(crate) chunk_size: ChunkSize,
}

impl StreamTarget {
    pub(crate) fn new(store: StoreWriter, start_pos: u64, chunk_size: ChunkSize) -> Self {
        Self {
            store,
            pos: AtomicU64::new(start_pos),
            chunk_size,
        }
    }

    pub(crate) fn pos(&self) -> u64 {
        self.pos.load(Ordering::Acquire)
    }

    pub(crate) fn set_pos(&self, pos: u64) {
        self.pos.store(pos, Ordering::Release);
        trace!("set stream target position to: {pos}");
    }

    pub(crate) fn increase_pos(&self, bytes: usize) {
        let prev = self.pos.fetch_add(bytes as u64, Ordering::Release);
        let new = prev + bytes as u64;
        trace!("increased target pos: {prev} -> {new}");
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(closest_beyond_curr_pos, closest_to_start),
        ret
    )]
    pub(crate) async fn next_range(&mut self, stream_size: &Size) -> Option<Range<u64>> {
        let chunk_size = self.chunk_size.calc() as u64;
        let Some(stream_end) = stream_size.known() else {
            let start = self.pos();
            let end = self.pos() + chunk_size;
            return Some(start..end);
        };

        let ranges = self.store.curr_store.lock().await.ranges();
        let limit_to_chunk_size = |Range { start, end }| {
            let len: u64 = end - start;
            let len = len.min(chunk_size);
            Range {
                start,
                end: start + len,
            }
        };

        assert!(
            stream_end >= self.pos(),
            "pos ({}) should not be bigger then stream_end: {stream_end}",
            self.pos()
        );
        let closest_beyond_curr_pos = ranges
            .gaps(&(self.pos()..stream_end))
            .next()
            .map(limit_to_chunk_size);
        tracing_record!(closest_beyond_curr_pos);

        let closest_to_start = ranges
            .gaps(&(0..stream_end))
            .next()
            .map(limit_to_chunk_size);
        tracing_record!(closest_to_start);

        closest_beyond_curr_pos.or(closest_to_start)
    }
}

impl StreamTarget {
    #[instrument(level = "trace", skip(self, buf), fields(buf_len = buf.len()))]
    pub(crate) async fn append(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        // only this function modifies pos,
        // only need to read threads own writes => relaxed ordering
        let mut written = 0;

        while written < buf.len() {
            let res = self
                .store
                .write_at(buf, self.pos.load(Ordering::Relaxed))
                .await;

            written += match res {
                Ok(bytes) => bytes.get(),
                Err(other) => {
                    todo!("handle other error: {other:?}")
                }
            };
        }

        self.increase_pos(written);
        Ok(written)
    }
}
