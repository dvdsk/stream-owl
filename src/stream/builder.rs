use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::{pin_mut, FutureExt};
use futures_concurrency::future::Race;
use std::future::Future;
use tokio::sync::{mpsc, Mutex};
use tracing::debug;

use crate::http_client::Size;
use crate::network::{Bandwidth, BandwidthAllowed, BandwidthLim, Network};
use crate::store;
use crate::{manager, StreamDone, StreamId};

use super::retry::{RetryDurLimit, RetryLimit};
use super::{task, Error, Handle, ManagedHandle, StreamEnded};

#[derive(Debug)]
enum StorageChoice {
    Disk(PathBuf),
    MemLimited(NonZeroUsize),
    MemUnlimited,
}

#[derive(Debug)]
pub struct StreamBuilder<const STORAGE_SET: bool> {
    url: http::Uri,
    storage: Option<StorageChoice>,
    initial_prefetch: usize,
    restriction: Option<Network>,
    start_paused: bool,
    bandwidth: BandwidthAllowed,
    retry_disabled: bool,
    max_retries: RetryLimit,
    max_retry_dur: RetryDurLimit,
    timeout: Duration,
}

impl StreamBuilder<false> {
    pub fn new(url: http::Uri) -> StreamBuilder<false> {
        StreamBuilder {
            url,
            storage: None,
            initial_prefetch: 10_000,
            restriction: None,
            start_paused: false,
            bandwidth: BandwidthAllowed::UnLimited,
            retry_disabled: false,
            max_retries: RetryLimit::Unlimited,
            max_retry_dur: RetryDurLimit::Unlimited,
            timeout: Duration::from_secs(2),
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
            restriction: self.restriction,
            start_paused: self.start_paused,
            bandwidth: self.bandwidth,
            retry_disabled: self.retry_disabled,
            max_retries: self.max_retries,
            max_retry_dur: self.max_retry_dur,
            timeout: self.timeout,
        }
    }
    pub fn to_limited_mem(mut self, max_size: NonZeroUsize) -> StreamBuilder<true> {
        self.storage = Some(StorageChoice::MemLimited(max_size));
        StreamBuilder {
            url: self.url,
            storage: self.storage,
            initial_prefetch: self.initial_prefetch,
            restriction: self.restriction,
            start_paused: self.start_paused,
            bandwidth: self.bandwidth,
            retry_disabled: self.retry_disabled,
            max_retries: self.max_retries,
            max_retry_dur: self.max_retry_dur,
            timeout: self.timeout,
        }
    }
    pub fn to_disk(mut self, path: PathBuf) -> StreamBuilder<true> {
        self.storage = Some(StorageChoice::Disk(path));
        StreamBuilder {
            url: self.url,
            storage: self.storage,
            initial_prefetch: self.initial_prefetch,
            restriction: self.restriction,
            start_paused: self.start_paused,
            bandwidth: self.bandwidth,
            retry_disabled: self.retry_disabled,
            max_retries: self.max_retries,
            max_retry_dur: self.max_retry_dur,
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
    /// By default all networks are allowed
    pub fn with_network_restriction(mut self, allowed_network: Network) -> Self {
        self.restriction = Some(allowed_network);
        self
    }
    /// By default there is no bandwidth limit
    pub fn with_bandwidth_limit(mut self, bandwidth: Bandwidth) -> Self {
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
            task::retry::Decider::with_limits(self.max_retries, self.max_retry_dur)
        };

        let stream_task = task::new(
            self.url,
            store_writer,
            seek_rx,
            self.restriction,
            bandwidth_lim,
            stream_size,
            retry,
            self.timeout
        );
        let stream_task = pausable(stream_task, pause_rx);
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
