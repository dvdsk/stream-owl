use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;

use crate::network::{BandwidthAllowed, BandwidthLimit, Network};
use crate::store::StorageChoice;
use crate::stream::retry::{RetryDurLimit, RetryLimit};
use crate::target::ChunkSizeBuilder;
use crate::{StreamBuilder, StreamId};

use super::Callbacks;

#[derive(Derivative, Default, Clone)]
#[derivative(Debug)]
pub struct StreamConfig {
    pub(crate) url: http::Uri,
    storage: StorageChoice,
    initial_prefetch: usize,
    chunk_size: ChunkSizeBuilder,
    restriction: Option<Network>,
    start_paused: bool,
    bandwidth: BandwidthAllowed,

    retry_disabled: bool,
    max_retries: RetryLimit,
    max_retry_dur: RetryDurLimit,
    timeout: Duration,
}

impl StreamConfig {
    pub fn to_unlimited_mem(mut self) -> StreamConfig {
        self.storage = StorageChoice::MemUnlimited;
        self
    }
    pub fn to_limited_mem(mut self, max_size: usize) -> StreamConfig {
        self.storage = StorageChoice::MemLimited(max_size);
        self
    }
    pub fn to_disk(mut self, path: PathBuf) -> StreamConfig {
        self.storage = StorageChoice::Disk(path);
        self
    }
    /// Default is false
    pub fn start_paused(mut self, start_paused: bool) -> Self {
        self.start_paused = start_paused;
        self
    }
    /// Default is 10_000 bytes
    pub fn with_prefetch(mut self, prefetch: usize) -> Self {
        self.initial_prefetch = prefetch;
        self
    }
    pub fn with_fixed_chunk_size(mut self, chunk_size: NonZeroUsize) -> Self {
        self.chunk_size = ChunkSizeBuilder::new_fixed(chunk_size);
        self
    }
    pub fn with_max_dynamic_chunk_size(mut self, max: NonZeroUsize) -> Self {
        self.chunk_size = ChunkSizeBuilder::new_dynamic_with_max(max);
        self
    }
    /// By default there is no bandwidth limit
    pub fn with_bandwidth_limit(mut self, bandwidth: BandwidthLimit) -> Self {
        self.bandwidth = BandwidthAllowed::Limited(bandwidth);
        self
    }
}

fn forward_call_with_id<T: 'static>(
    callback: &Option<Arc<dyn Fn(StreamId, T) + Send + Sync>>,
    id: StreamId,
) -> Option<Box<dyn FnMut(T) + Send>> {
    let call = callback.as_ref()?.clone();
    let wrapped = move |val: T| (call)(id, val);
    let wrapped = Box::new(wrapped);
    let wrapped = wrapped as Box<dyn FnMut(T) + Send>;
    Some(wrapped)
}

impl StreamConfig {
    pub(crate) fn into_stream_builder(
        self,
        id: StreamId,
        url: http::Uri,
        callbacks: &Callbacks,
    ) -> StreamBuilder<true> {
        let Callbacks {
            retry_log_callback,
            bandwidth_callback,
            range_callback,
        } = callbacks;

        let retry_log_callback = forward_call_with_id(retry_log_callback, id);
        let bandwidth_callback = forward_call_with_id(bandwidth_callback, id);
        let range_callback = forward_call_with_id(range_callback, id);

        StreamBuilder {
            url,
            storage: Some(self.storage),
            initial_prefetch: self.initial_prefetch,
            chunk_size: self.chunk_size,
            restriction: self.restriction,
            start_paused: self.start_paused,
            bandwidth: self.bandwidth,
            retry_disabled: self.retry_disabled,
            max_retries: self.max_retries,
            max_retry_dur: self.max_retry_dur,
            timeout: self.timeout,

            retry_log_callback,
            bandwidth_callback,
            range_callback,
        }
    }
}
