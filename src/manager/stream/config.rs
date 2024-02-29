use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::Duration;

use derivative::Derivative;

use crate::manager::task::bandwidth::Priority;
use crate::network::{BandwidthAllowed, BandwidthLimit, Network};
use crate::store::StorageChoice;
use crate::retry::{RetryDurLimit, RetryLimit};
use crate::target::ChunkSizeBuilder;

#[derive(Derivative, Default, Clone)]
#[derivative(Debug)]
pub struct StreamConfig {
    pub(crate) url: http::Uri,
    pub(crate) storage: StorageChoice,
    pub(crate) initial_prefetch: usize,
    pub(crate) chunk_size: ChunkSizeBuilder,
    pub(crate) restriction: Option<Network>,
    pub(crate) start_paused: bool,
    pub(crate) bandwidth: BandwidthAllowed,
    pub(crate) priority: Priority,

    pub(crate) retry_disabled: bool,
    pub(crate) max_retries: RetryLimit,
    pub(crate) max_retry_dur: RetryDurLimit,
    pub(crate) timeout: Duration,
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
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
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
