use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc::{self, Sender};
use tokio::sync::Mutex;

use crate::http_client::Size;
use crate::manager::Command;
use crate::network::BandwidthLim;
use crate::store::{self, StorageChoice, WriterToken};
use crate::target::StreamTarget;
use crate::{
    util, BandwidthCallback, LogCallback, RangeCallback, StreamCanceld, StreamError, StreamHandle, stream,
};

mod config;
pub use config::StreamConfig;

use super::WrappedCallbacks;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Id(usize);

impl Id {
    pub(crate) fn new() -> Self {
        // zero is reserved
        static NEXT_ID: AtomicUsize = AtomicUsize::new(1);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self(id)
    }

    pub(crate) const fn placeholder() -> Id {
        Self(0)
    }
}

impl StreamConfig {
    #[tracing::instrument]
    pub async fn start<L: LogCallback, B: BandwidthCallback, R: RangeCallback>(
        self,
        url: http::Uri,
        manager_tx: Sender<Command>,
        callbacks: WrappedCallbacks<L, B, R>,
    ) -> Result<
        (
            stream::Handle<R>,
            impl Future<Output = Result<StreamCanceld, StreamError>> + Send + 'static,
        ),
        crate::store::Error,
    > {
        let stream_size = Size::default();
        let (store_reader, store_writer) = match self.storage {
            StorageChoice::Disk(path) => {
                store::new_disk_backed(path, stream_size.clone(), callbacks.range).await?
            }
            StorageChoice::MemLimited(limit) => {
                store::new_limited_mem_backed(limit, stream_size.clone(), callbacks.range)?
            }
            StorageChoice::MemUnlimited => {
                store::new_unlimited_mem_backed(stream_size.clone(), callbacks.range)
            }
        };

        let (seek_tx, seek_rx) = mpsc::channel(12);
        let (pause_tx, pause_rx) = mpsc::channel(12);
        let (bandwidth_lim, bandwidth_lim_tx) = BandwidthLim::new(self.bandwidth);

        let stream_handle = StreamHandle {
            prefetch: self.initial_prefetch,
            seek_tx,
            is_paused: self.start_paused,
            store_writer: store_writer.clone(),
            store_reader: Arc::new(Mutex::new(store_reader)),
            pause_tx,
            bandwidth_lim_tx,
        };

        let retry = if self.retry_disabled {
            crate::retry::Decider::disabled(callbacks.retry_log)
        } else {
            crate::retry::Decider::with_limits(
                self.max_retries,
                self.max_retry_dur,
                callbacks.retry_log,
            )
        };

        let target = StreamTarget::new(
            store_writer,
            0,
            self.chunk_size,
            WriterToken::first(),
            callbacks.bandwidth,
        );

        let stream_task = crate::stream::task::restarting_on_seek(
            url,
            target,
            seek_rx,
            self.restriction,
            bandwidth_lim,
            stream_size,
            retry,
            self.timeout,
        );

        let stream_task = util::pausable(stream_task, pause_rx, self.start_paused);
        Ok((stream_handle, stream_task))
    }
}
