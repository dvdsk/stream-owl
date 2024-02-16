use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;
use futures::{pin_mut, FutureExt};
use futures_concurrency::future::Race;
use std::future::Future;
use tokio::sync::{mpsc, Mutex};

use crate::http_client::{self, Size};
use crate::network::{BandwidthAllowed, BandwidthLim, BandwidthLimit, Network};
use crate::store::{self, WriterToken, StorageChoice};
use crate::target::{ChunkSizeBuilder, StreamTarget};
use crate::StreamCanceld;

use super::reporting::RangeUpdate;
use super::retry::{RetryDurLimit, RetryLimit};
use super::{reporting, task, Error, Handle};

#[derive(Derivative)]
#[derivative(Debug)]
pub struct StreamBuilder<const STORAGE_SET: bool> {
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
    pub(crate) retry_log_callback: Option<Box<dyn FnMut(Arc<http_client::Error>) + Send>>,
    #[derivative(Debug(format_with = "crate::util::fmt_non_printable_option"))]
    pub(crate) bandwidth_callback: Option<Box<dyn FnMut(usize) + Send>>,
    #[derivative(Debug(format_with = "crate::util::fmt_non_printable_option"))]
    pub(crate) range_callback: Option<Box<dyn FnMut(RangeUpdate) + Send>>,
}

impl StreamBuilder<false> {
    pub fn new(url: http::Uri) -> StreamBuilder<false> {
        StreamBuilder {
            url,
            storage: None,
            initial_prefetch: 10_000,
            chunk_size: ChunkSizeBuilder::default(),
            restriction: None,
            start_paused: false,
            bandwidth: BandwidthAllowed::default(),
            bandwidth_callback: None,
            retry_disabled: false,
            max_retries: RetryLimit::default(),
            max_retry_dur: RetryDurLimit::default(),
            retry_log_callback: None,
            timeout: Duration::from_secs(2),
            range_callback: None,
        }
    }
}

impl StreamBuilder<false> {
    pub fn to_unlimited_mem(mut self) -> StreamBuilder<true> {
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
    pub fn to_limited_mem(mut self, max_size: usize) -> StreamBuilder<true> {
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
    pub fn to_disk(mut self, path: PathBuf) -> StreamBuilder<true> {
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

impl<const STORAGE_SET: bool> StreamBuilder<STORAGE_SET> {
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
    pub fn with_retry_callback(
        mut self,
        logger: impl FnMut(Arc<http_client::Error>) + Send + 'static,
    ) -> Self {
        self.retry_log_callback = Some(Box::new(logger));
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

impl StreamBuilder<true> {
    #[tracing::instrument]
    pub async fn start(
        self,
    ) -> Result<
        (
            Handle,
            impl Future<Output = Result<StreamCanceld, Error>> + Send + 'static,
        ),
        crate::store::Error,
    > {
        let stream_size = Size::default();
        let (report_tx, report_rx) = std::sync::mpsc::channel();

        let (store_reader, store_writer) = match self.storage.expect("must chose storage option") {
            StorageChoice::Disk(path) => {
                store::new_disk_backed(path, stream_size.clone(), report_tx.clone()).await?
            }
            StorageChoice::MemLimited(limit) => {
                store::new_limited_mem_backed(limit, stream_size.clone(), report_tx.clone())?
            }
            StorageChoice::MemUnlimited => {
                store::new_unlimited_mem_backed(stream_size.clone(), report_tx.clone())
            }
        };

        let (seek_tx, seek_rx) = mpsc::channel(12);
        let (pause_tx, pause_rx) = mpsc::channel(12);
        let (bandwidth_lim, bandwidth_lim_tx) = BandwidthLim::new(self.bandwidth);

        let reporting_task = reporting::setup(
            report_rx,
            self.bandwidth_callback,
            self.range_callback,
            self.retry_log_callback,
        );

        let handle = Handle {
            prefetch: self.initial_prefetch,
            seek_tx,
            is_paused: self.start_paused,
            store_writer: store_writer.clone(),
            store_reader: Arc::new(Mutex::new(store_reader)),
            pause_tx,
            bandwidth_lim_tx,
        };

        let retry = if self.retry_disabled {
            task::retry::Decider::disabled()
        } else {
            task::retry::Decider::with_limits(
                self.max_retries,
                self.max_retry_dur,
                report_tx.clone(),
            )
        };

        let chunk_size = self.chunk_size.build();
        let target = StreamTarget::new(store_writer, 0, chunk_size, WriterToken::first());

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

        let stream_task = pausable(stream_task, pause_rx, self.start_paused);
        let stream_task = (stream_task, reporting_task).race();
        Ok((handle, stream_task))
    }
}

async fn pausable<F>(
    task: F,
    mut pause_rx: mpsc::Receiver<bool>,
    start_paused: bool,
) -> Result<StreamCanceld, Error>
where
    F: Future<Output = Result<StreamCanceld, Error>>,
{
    enum Res {
        Pause(Option<bool>),
        Task(Result<StreamCanceld, Error>),
    }

    if start_paused {
        match pause_rx.recv().await {
            Some(true) => unreachable!("handle should send pause only if unpaused"),
            Some(false) => (),
            None => return Ok(StreamCanceld),
        }
    }

    pin_mut!(task);
    loop {
        let get_pause = pause_rx.recv().map(Res::Pause);
        let task_ends = task.as_mut().map(Res::Task);
        match (get_pause, task_ends).race().await {
            Res::Pause(None) => return Ok(StreamCanceld),
            Res::Pause(Some(true)) => loop {
                match pause_rx.recv().await {
                    Some(true) => unreachable!("handle should send pause only if unpaused"),
                    Some(false) => break,
                    None => return Ok(StreamCanceld),
                }
            },
            Res::Pause(Some(false)) => unreachable!("handle should send unpause only if paused"),
            Res::Task(result) => return result,
        }
    }
}
