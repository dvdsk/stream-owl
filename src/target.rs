use std::ops::Range;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use tracing::{debug, info, instrument, trace};

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
use crate::store::WriterToken;
use crate::{BandwidthCallback, RangeCallback};

#[derive(Debug)]
pub(crate) struct StreamTarget<B: BandwidthCallback, R: RangeCallback> {
    /// on seek this pos is updated, in between seeks
    /// it increments with the number of bytes written
    pos: AtomicU64,
    store: StoreWriter<R>,
    pub(crate) bandwidth: Arc<AtomicUsize>,
    pub(crate) bandwidth_callback: B,
    pub(crate) writer_token: WriterToken,
    pub(crate) chunk_size: ChunkSize,
}

impl<B: BandwidthCallback, R: RangeCallback> StreamTarget<B, R> {
    pub(crate) fn new(
        store: StoreWriter<R>,
        start_pos: u64,
        chunk_size: ChunkSizeBuilder,
        writer_token: WriterToken,
        bandwidth_callback: B,
    ) -> Self {
        let bandwidth = Arc::new(AtomicUsize::new(1000));
        let chunk_size = chunk_size.build(bandwidth.clone());
        Self {
            store,
            writer_token,
            bandwidth,
            pos: AtomicU64::new(start_pos),
            chunk_size,
            bandwidth_callback,
        }
    }

    pub(crate) fn pos(&self) -> u64 {
        self.pos.load(Ordering::Acquire)
    }

    pub(crate) fn set_pos(&self, pos: u64) {
        self.pos.store(pos, Ordering::Release);
        trace!("set stream target position to: {pos}");
    }

    #[instrument(level = "trace", skip(self))]
    pub(crate) fn increase_pos(&self, bytes: usize) {
        let prev = self.pos.fetch_add(bytes as u64, Ordering::Release);
        let new = prev + bytes as u64;
        trace!("increased target pos: {prev} -> {new}");
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(closest_beyond_curr_pos, closest_to_start)
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

        let result = closest_beyond_curr_pos.or(closest_to_start);
        if let Some(ref range) = result {
            debug!(
                "next range to stream from server: {}..{}",
                range.start, range.end
            );
        } else {
            info!("no more data to stream from server");
        }
        result
    }
}

impl<B: BandwidthCallback, R: RangeCallback> StreamTarget<B, R> {
    /// can be cancelled at any time by a (stream) seek
    #[instrument(level = "trace", skip(self, buf), fields(buf_len = buf.len()))]
    pub(crate) async fn append(&mut self, buf: &[u8]) -> Result<usize, std::io::Error> {
        let mut total_written = 0;

        while total_written < buf.len() {
            let res = self
                .store
                .write_at(buf, self.pos.load(Ordering::Acquire), self.writer_token)
                .await;

            let just_written = match res {
                Ok(bytes) => bytes,
                Err(other) => {
                    todo!("handle other error: {other:?}")
                }
            };
            self.increase_pos(just_written);
            total_written += just_written;
        }

        Ok(total_written)
    }
}
