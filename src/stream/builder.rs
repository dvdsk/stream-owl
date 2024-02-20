use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;
use std::future::Future;
use tokio::sync::{mpsc, Mutex};

use crate::http_client::Size;
use crate::network::{BandwidthAllowed, BandwidthLim, BandwidthLimit, Network};
use crate::store::{self, StorageChoice, WriterToken};
use crate::target::{ChunkSizeBuilder, StreamTarget};
use crate::{util, BandwidthCallback, LogCallback, RangeCallback, StreamCanceld};

use super::{task, Error, Handle};
use crate::retry::{RetryDurLimit, RetryLimit};

#[derive(Derivative)]
#[derivative(Debug)]
pub struct StreamBuilder<const STORAGE_SET: bool, L, B, R> {
    pub(crate) url: http::Uri,
    pub(crate) storage: Option<StorageChoice>,
    pub(crate) initial_prefetch: usize,
    pub(crate) chunk_size: ChunkSizeBuilder,
    pub(crate) restriction: Option<Network>,
    pub(crate) start_paused: bool,
    pub(crate) bandwidth: BandwidthAllowed,

    pub(crate) retry_disabled: bool,
    pub(crate) max_retries: RetryLimit,
    pub(crate) max_retry_dur: RetryDurLimit,
    pub(crate) timeout: Duration,

    #[derivative(Debug(format_with = "crate::util::fmt_non_printable_option"))]
    pub(crate) retry_log_callback: Option<L>,
    #[derivative(Debug(format_with = "crate::util::fmt_non_printable_option"))]
    pub(crate) bandwidth_callback: Option<B>,
    #[derivative(Debug(format_with = "crate::util::fmt_non_printable_option"))]
    pub(crate) range_callback: Option<R>,
}

impl<L: LogCallback, B: BandwidthCallback, R> StreamBuilder<false, L, B, R> {
    pub fn new(url: http::Uri) -> StreamBuilder<false, L, B, R> {
        StreamBuilder {
            url,
            storage: None,
            initial_prefetch: 10_000,
            chunk_size: ChunkSizeBuilder::default(),
            restriction: None,
            start_paused: false,
            bandwidth: BandwidthAllowed::default(),
            retry_disabled: false,
            max_retries: RetryLimit::default(),
            max_retry_dur: RetryDurLimit::default(),
            timeout: Duration::from_secs(2),

            bandwidth_callback: None,
            retry_log_callback: None,
            range_callback: None,
        }
    }
}

impl<L: LogCallback, B: BandwidthCallback, R> StreamBuilder<false, L, B, R> {
    pub fn to_unlimited_mem(mut self) -> StreamBuilder<true, L, B, R> {
        self.storage = Some(StorageChoice::MemUnlimited);
        StreamBuilder {
            url: self.url,
            storage: self.storage,
            initial_prefetch: self.initial_prefetch,
            chunk_size: self.chunk_size,
            restriction: self.restriction,
            start_paused: self.start_paused,
            bandwidth: self.bandwidth,
            retry_disabled: self.retry_disabled,
            max_retries: self.max_retries,
            max_retry_dur: self.max_retry_dur,
            retry_log_callback: self.retry_log_callback,
            bandwidth_callback: self.bandwidth_callback,
            range_callback: self.range_callback,
            timeout: self.timeout,
        }
    }
    pub fn to_limited_mem(mut self, max_size: usize) -> StreamBuilder<true, L, B, R> {
        self.storage = Some(StorageChoice::MemLimited(max_size));
        StreamBuilder {
            url: self.url,
            storage: self.storage,
            initial_prefetch: self.initial_prefetch,
            chunk_size: self.chunk_size,
            restriction: self.restriction,
            start_paused: self.start_paused,
            bandwidth: self.bandwidth,
            retry_disabled: self.retry_disabled,
            max_retries: self.max_retries,
            max_retry_dur: self.max_retry_dur,
            retry_log_callback: self.retry_log_callback,
            bandwidth_callback: self.bandwidth_callback,
            range_callback: self.range_callback,
            timeout: self.timeout,
        }
    }
    pub fn to_disk(mut self, path: PathBuf) -> StreamBuilder<true, L, B, R> {
        self.storage = Some(StorageChoice::Disk(path));
        StreamBuilder {
            url: self.url,
            storage: self.storage,
            initial_prefetch: self.initial_prefetch,
            chunk_size: self.chunk_size,
            restriction: self.restriction,
            start_paused: self.start_paused,
            bandwidth: self.bandwidth,
            retry_disabled: self.retry_disabled,
            max_retries: self.max_retries,
            max_retry_dur: self.max_retry_dur,
            retry_log_callback: self.retry_log_callback,
            bandwidth_callback: self.bandwidth_callback,
            range_callback: self.range_callback,
            timeout: self.timeout,
        }
    }
}

impl<const STORAGE_SET: bool, L, B, R> StreamBuilder<STORAGE_SET, L, B, R> {
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
    /// By default all networks are allowed
    pub fn with_network_restriction(mut self, allowed_network: Network) -> Self {
        self.restriction = Some(allowed_network);
        self
    }
    /// By default there is no bandwidth limit
    pub fn with_bandwidth_limit(mut self, bandwidth: BandwidthLimit) -> Self {
        self.bandwidth = BandwidthAllowed::Limited(bandwidth);
        self
    }
    /// Default is false
    pub fn retry_disabled(mut self, retry_disabled: bool) -> Self {
        self.retry_disabled = retry_disabled;
        self
    }
    /// How often the **same error** may happen without an error free window
    /// before giving up and returning an error to the user.
    ///
    /// If this number is passed the stream is aborted and an error
    /// returned to the user.
    ///
    /// By default this period is unbounded (infinite)
    pub fn with_max_retries(mut self, n: usize) -> Self {
        self.max_retries = RetryLimit::new(n);
        self
    }
    /// How long the **same error** may keep happening without some error
    /// free time in between.
    ///
    /// If this duration is passed the stream is aborted and an error
    /// returned to the user.
    ///
    /// By default this period is unbounded (infinite)
    pub fn with_max_retry_duration(mut self, duration: Duration) -> Self {
        self.max_retry_dur = RetryDurLimit::new(duration);
        self
    }

    /// Perform an callback whenever a retry happens. Useful to log
    /// errors.
    pub fn with_retry_callback(mut self, logger: L) -> Self {
        self.retry_log_callback = Some(logger);
        self
    }

    /// How long an operation may hang before we abort it
    ///
    /// By default this is 2 seconds
    pub fn with_timeout(mut self, duration: Duration) -> Self {
        self.timeout = duration;
        self
    }
}

impl<L: LogCallback, B: BandwidthCallback, R: RangeCallback> StreamBuilder<true, L, B, R> {
    #[tracing::instrument(skip(self))] // TODO undo skip self
    pub async fn start(
        self,
    ) -> Result<
        (
            Handle<R>,
            impl Future<Output = Result<StreamCanceld, Error>> + Send + 'static,
        ),
        crate::store::Error,
    > {
        let stream_size = Size::default();

        let range_callback = |_| (); // TODO actually use the closure set in the builder
        let (store_reader, store_writer) = match self.storage.expect("must chose storage option") {
            StorageChoice::Disk(path) => {
                store::new_disk_backed(path, stream_size.clone(), range_callback).await?
            }
            StorageChoice::MemLimited(limit) => {
                store::new_limited_mem_backed(limit, stream_size.clone(), range_callback)?
            }
            StorageChoice::MemUnlimited => {
                store::new_unlimited_mem_backed(stream_size.clone(), range_callback)
            }
        };

        let (seek_tx, seek_rx) = mpsc::channel(12);
        let (pause_tx, pause_rx) = mpsc::channel(12);
        let (bandwidth_lim, bandwidth_lim_tx) = BandwidthLim::new(self.bandwidth);

        let handle = Handle {
            prefetch: self.initial_prefetch,
            seek_tx,
            is_paused: self.start_paused,
            store_writer: todo!(), //store_writer.clone(),
            store_reader: Arc::new(Mutex::new(store_reader)),
            pause_tx,
            bandwidth_lim_tx,
        };

        let retry = if self.retry_disabled {
            crate::retry::Decider::disabled()
        } else {
            crate::retry::Decider::with_limits(
                self.max_retries,
                self.max_retry_dur,
                report_tx.clone(),
            )
        };

        let target = StreamTarget::new(store_writer, 0, self.chunk_size, WriterToken::first());

        let stream_task = task::restarting_on_seek(
            self.url,
            target,
            report_tx,
            seek_rx,
            self.restriction,
            bandwidth_lim,
            stream_size,
            retry,
            self.timeout,
        );

        /* TODO: For managed stream do not use the reporting task, instead
         * use the managers rx/tx pair and have the reporting_task running on
         * the manager <16-02-24, dvdsk> */
        let stream_task = util::pausable(stream_task, pause_rx, self.start_paused);
        Ok((handle, stream_task))
    }
}
