use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;
use futures::{pin_mut, FutureExt};
use futures_concurrency::future::Race;
use std::future::Future;
use tokio::sync::{mpsc, Mutex};
use tracing::debug;

use crate::http_client::{self, Size};
use crate::network::{BandwidthAllowed, BandwidthLim, BandwidthLimit, Network};
use crate::store;
use crate::target::{ChunkSizeBuilder, StreamTarget};
use crate::{manager, StreamDone, StreamId};

use super::reporting::RangeUpdate;
use super::retry::{RetryDurLimit, RetryLimit};
use super::{reporting, task, Error, Handle, ManagedHandle, StreamEnded};

#[derive(Debug)]
enum StorageChoice {
    Disk(PathBuf),
    MemLimited(NonZeroUsize),
    MemUnlimited,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct StreamBuilder<const STORAGE_SET: bool> {
    url: http::Uri,
    storage: Option<StorageChoice>,
    initial_prefetch: usize,
    chunk_size: ChunkSizeBuilder,
    restriction: Option<Network>,
    start_paused: bool,
    bandwidth: BandwidthAllowed,

    retry_disabled: bool,
    max_retries: RetryLimit,
    max_retry_dur: RetryDurLimit,
    timeout: Duration,

    #[derivative(Debug(format_with = "fmt_non_printable_option"))]
    retry_log_callback: Option<Box<dyn FnMut(Arc<http_client::Error>) + Send>>,
    #[derivative(Debug(format_with = "fmt_non_printable_option"))]
    bandwidth_callback: Option<Box<dyn FnMut(usize) + Send>>,
    #[derivative(Debug(format_with = "fmt_non_printable_option"))]
    range_callback: Option<Box<dyn FnMut(RangeUpdate) + Send>>,
}

fn fmt_non_printable_option<T>(
    retry_logger: &Option<T>,
    fmt: &mut std::fmt::Formatter,
) -> std::result::Result<(), std::fmt::Error> {
    if retry_logger.is_some() {
        fmt.write_str("Some(-not printable-)")
    } else {
        fmt.write_str("None")
    }
}

impl StreamBuilder<false> {
    pub fn new(url: http::Uri) -> StreamBuilder<false> {
        StreamBuilder {
            url,
            storage: None,
            initial_prefetch: 10_000,
            chunk_size: ChunkSizeBuilder::new_dynamic(),
            restriction: None,
            start_paused: false,
            bandwidth: BandwidthAllowed::UnLimited,
            bandwidth_callback: None,
            retry_disabled: false,
            max_retries: RetryLimit::Unlimited,
            max_retry_dur: RetryDurLimit::Unlimited,
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
    pub fn to_limited_mem(mut self, max_size: NonZeroUsize) -> StreamBuilder<true> {
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
    pub(crate) async fn start_managed(
        self,
        manager_tx: mpsc::Sender<manager::Command>,
    ) -> Result<
        (
            ManagedHandle,
            impl Future<Output = StreamEnded> + Send + 'static,
        ),
        crate::store::Error,
    > {
        let id = StreamId::new();
        let (handle, stream_task) = self.start().await?;
        let stream_task = stream_task.map(|res| StreamEnded { res, id });
        let handle = ManagedHandle {
            cmd_manager: manager_tx,
            handle,
        };
        Ok((handle, stream_task))
    }

    #[tracing::instrument]
    pub async fn start(
        self,
    ) -> Result<
        (
            Handle,
            impl Future<Output = Result<StreamDone, Error>> + Send + 'static,
        ),
        crate::store::Error,
    > {
        let stream_size = Size::default();
        let (store_reader, store_writer) = match self.storage.expect("must chose storage option") {
            StorageChoice::Disk(path) => store::new_disk_backed(path, stream_size.clone()).await?,
            StorageChoice::MemLimited(limit) => {
                store::new_limited_mem_backed(limit, stream_size.clone())?
            }
            StorageChoice::MemUnlimited => store::new_unlimited_mem_backed(stream_size.clone()),
        };

        let (seek_tx, seek_rx) = mpsc::channel(12);
        let (pause_tx, pause_rx) = mpsc::channel(12);
        let (bandwidth_lim, bandwidth_lim_tx) = BandwidthLim::new(self.bandwidth);

        let mut handle = Handle {
            prefetch: self.initial_prefetch,
            seek_tx,
            is_paused: false,
            store: store_reader.curr_store.clone(),
            capacity_watch: store_writer.capacity_watcher.clone(),
            store_reader: Arc::new(Mutex::new(store_reader)),
            pause_tx,
            bandwidth_lim_tx,
        };

        if self.start_paused {
            handle.pause().await;
        }

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
        let target = StreamTarget::new(store_writer, 0, chunk_size);

        let stream_task = task::new(
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

        let stream_task = pausable(stream_task, pause_rx);
        let stream_task = (stream_task, reporting_task).race();
        Ok((handle, stream_task))
    }
}

async fn pausable<F>(task: F, mut pause_rx: mpsc::Receiver<bool>) -> Result<StreamDone, Error>
where
    F: Future<Output = Result<StreamDone, Error>>,
{
    enum Res {
        Pause(Option<bool>),
        Task(Result<StreamDone, Error>),
    }

    pin_mut!(task);
    loop {
        let get_pause = pause_rx.recv().map(Res::Pause);
        let task_ends = task.as_mut().map(Res::Task);
        match (get_pause, task_ends).race().await {
            Res::Pause(None) => return Ok(StreamDone::Canceld),
            Res::Pause(Some(true)) => loop {
                match pause_rx.recv().await {
                    Some(true) => debug!("already paused"),
                    Some(false) => break,
                    None => return Ok(StreamDone::Canceld),
                }
            },
            Res::Pause(Some(false)) => debug!("not paused"),
            Res::Task(result) => return result,
        }
    }
}
