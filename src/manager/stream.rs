use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use derivative::Derivative;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::Mutex;

use crate::http_client::Size;
use crate::manager::Command;
use crate::network::BandwidthLim;
use crate::store::{self, StorageChoice, WriterToken};
use crate::target::StreamTarget;
use crate::{
    util, BandwidthCallback, LogCallback, RangeCallback, StreamCanceld, StreamError, StreamHandle,
};

mod config;
pub use config::StreamConfig;

use super::WrappedCallbacks;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Id(usize);

impl Id {
    pub(crate) fn new() -> Self {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self(id)
    }
}

// macro_rules! managed {
//     ($fn_name:ident $($param:ident: $t:ty),*$(; $returns:ty)?) => {
//         pub fn $fn_name(&mut self, $($param: $t),*) $(-> $returns)? {
//             self.handle.$fn_name($($param),*)
//         }
//     };
// }

pub struct ManagedHandle {
    inner: Box<dyn ManagedHandleTrait>,
}

impl ManagedHandle {
    pub(crate) fn from_generic<F: RangeCallback>(generic: InnerManagedHandle<F>) -> Self {
        Self {
            inner: Box::new(generic) as Box<dyn ManagedHandleTrait>,
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct InnerManagedHandle<F: RangeCallback> {
    /// allows the handle to send a message
    /// to the manager to drop the streams future
    /// or increase/decrease priority.
    #[derivative(Debug = "ignore")]
    pub(crate) cmd_manager: Sender<Command>,
    pub(crate) handle: StreamHandle<F>,
}

impl<F: RangeCallback> Drop for InnerManagedHandle<F> {
    fn drop(&mut self) {
        self.cmd_manager
            .try_send(Command::CancelStream(self.id()))
            .expect("could not cancel stream task when handle was dropped")
    }
}

#[async_trait]
pub(crate) trait ManagedHandleTrait {
    fn set_priority(&mut self, _arg: i32);
    fn id(&self) -> Id;
    // async fn pause(&mut self);
    // async fn unpause(&mut self);
}

impl ManagedHandle {
    pub fn set_priority(&mut self, _arg: i32) {
        todo!()
    }

    pub fn id(&self) -> Id {
        todo!()
    }
}

// macro_rules! managed_async {
//     ($fn_name:ident $($param:ident: $t:ty),*$(; $returns:ty)?) => {
//         async fn $fn_name(&mut self, $($param: $t),*) $(-> $returns)? {
//             self.handle.$fn_name($($param),*).await
//         }
//     };
// }

#[async_trait]
impl<F: RangeCallback> ManagedHandleTrait for InnerManagedHandle<F> {
    fn set_priority(&mut self, _arg: i32) {
        todo!()
    }

    fn id(&self) -> Id {
        todo!()
    }
    // async fn pause(&mut self) {
    //     // self.handle.pause().await
    // }
    // managed_async! {unpause}
}
/* TODO: make this work again <22-02-24> */
//
//     managed_async! {limit_bandwidth bandwidth: BandwidthLimit}
//     managed_async! {remove_bandwidth_limit}
//     managed_async! {migrate_to_limited_mem_backend max_cap: usize; Result<(), MigrationError>}
//     managed_async! {migrate_to_unlimited_mem_backend ; Result<(), MigrationError>}
//     managed_async! {migrate_to_disk_backend path: PathBuf; Result<(), MigrationError>}
//     managed_async! {flush ; Result<(), StreamError>}
//
//     managed! {try_get_reader; Result<crate::reader::Reader, GetReaderError>}
// }
//
// impl<F: crate::IdRangeCallback> ManagedHandle<F> {
//     blocking! {pause - pause_blocking}
//     blocking! {unpause - unpause_blocking}
//     blocking! {limit_bandwidth - limit_bandwidth_blocking bandwidth: BandwidthLimit}
//     blocking! {remove_bandwidth_limit - remove_bandwidth_limit_blocking}
//     blocking! {migrate_to_limited_mem_backend - migrate_to_limited_mem_backend_blocking max_cap: usize; Result<(), MigrationError>}
//     blocking! {migrate_to_unlimited_mem_backend - migrate_to_unlimited_mem_backend_blocking ; Result<(), MigrationError>}
//     blocking! {migrate_to_disk_backend - migrate_to_disk_backend_blocking path: PathBuf; Result<(), MigrationError>}
//     blocking! {flush - flush_blocking; Result<(), StreamError>}
// }

impl StreamConfig {
    #[tracing::instrument]
    pub async fn start<L: LogCallback, B: BandwidthCallback, R: RangeCallback>(
        self,
        url: http::Uri,
        manager_tx: Sender<Command>,
        callbacks: WrappedCallbacks<L, B, R>,
    ) -> Result<
        (
            InnerManagedHandle<R>,
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
        let handle = InnerManagedHandle {
            cmd_manager: manager_tx,
            handle: stream_handle,
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

        /* TODO: For managed stream do not use the reporting task, instead
         * use the managers rx/tx pair and have the reporting_task running on
         * the manager <16-02-24, dvdsk> */
        let stream_task = util::pausable(stream_task, pause_rx, self.start_paused);
        Ok((handle, stream_task))
    }
}
