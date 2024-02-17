use std::future::Future;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use derivative::Derivative;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::Mutex;

use crate::http_client::Size;
use crate::manager::Command;
use crate::network::{BandwidthLim, BandwidthLimit};
use crate::store::migrate::MigrationError;
use crate::store::{self, StorageChoice, WriterToken};
use crate::stream::{blocking, Report};
use crate::stream::GetReaderError;
use crate::target::StreamTarget;
use crate::{util, StreamCanceld, StreamError, StreamHandle};

mod config;
pub use config::StreamConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Id(usize);

impl Id {
    pub(crate) fn new() -> Self {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self(id)
    }
}
macro_rules! managed_async {
    ($fn_name:ident $($param:ident: $t:ty),*$(; $returns:ty)?) => {
        pub async fn $fn_name(&mut self, $($param: $t),*) $(-> $returns)? {
            self.handle.$fn_name($($param),*).await
        }
    };
}

macro_rules! managed {
    ($fn_name:ident $($param:ident: $t:ty),*$(; $returns:ty)?) => {
        pub fn $fn_name(&mut self, $($param: $t),*) $(-> $returns)? {
            self.handle.$fn_name($($param),*)
        }
    };
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ManagedHandle {
    /// allows the handle to send a message
    /// to the manager to drop the streams future
    /// or increase/decrease priority.
    #[derivative(Debug = "ignore")]
    pub(crate) cmd_manager: Sender<Command>,
    pub(crate) handle: StreamHandle,
}

impl Drop for ManagedHandle {
    fn drop(&mut self) {
        self.cmd_manager
            .try_send(Command::CancelStream(self.id()))
            .expect("could not cancel stream task when handle was dropped")
    }
}

impl ManagedHandle {
    pub fn set_priority(&mut self, _arg: i32) {
        todo!()
    }

    pub fn id(&self) -> Id {
        todo!()
    }

    managed_async! {pause}
    managed_async! {unpause}
    managed_async! {limit_bandwidth bandwidth: BandwidthLimit}
    managed_async! {remove_bandwidth_limit}
    managed_async! {migrate_to_limited_mem_backend max_cap: usize; Result<(), MigrationError>}
    managed_async! {migrate_to_unlimited_mem_backend ; Result<(), MigrationError>}
    managed_async! {migrate_to_disk_backend path: PathBuf; Result<(), MigrationError>}
    managed_async! {flush ; Result<(), StreamError>}

    managed! {try_get_reader; Result<crate::reader::Reader, GetReaderError>}
}
impl ManagedHandle {
    blocking! {pause - pause_blocking}
    blocking! {unpause - unpause_blocking}
    blocking! {limit_bandwidth - limit_bandwidth_blocking bandwidth: BandwidthLimit}
    blocking! {remove_bandwidth_limit - remove_bandwidth_limit_blocking}
    blocking! {migrate_to_limited_mem_backend - migrate_to_limited_mem_backend_blocking max_cap: usize; Result<(), MigrationError>}
    blocking! {migrate_to_unlimited_mem_backend - migrate_to_unlimited_mem_backend_blocking ; Result<(), MigrationError>}
    blocking! {migrate_to_disk_backend - migrate_to_disk_backend_blocking path: PathBuf; Result<(), MigrationError>}
    blocking! {flush - flush_blocking; Result<(), StreamError>}
}

impl StreamConfig {
    #[tracing::instrument]
    pub async fn start(
        self,
        url: http::Uri,
        manager_tx: Sender<Command>,
        report_tx: Sender<Report>,
    ) -> Result<
        (
            ManagedHandle,
            impl Future<Output = Result<StreamCanceld, StreamError>> + Send + 'static,
        ),
        crate::store::Error,
    > {
        let stream_size = Size::default();

        let (store_reader, store_writer) = match self.storage {
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

        let stream_handle = StreamHandle {
            prefetch: self.initial_prefetch,
            seek_tx,
            is_paused: self.start_paused,
            store_writer: store_writer.clone(),
            store_reader: Arc::new(Mutex::new(store_reader)),
            pause_tx,
            bandwidth_lim_tx,
        };
        let handle = ManagedHandle {
            cmd_manager: manager_tx,
            handle: stream_handle,
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

        let stream_task = crate::stream::task::restarting_on_seek(
            url,
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
