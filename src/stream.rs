use std::any::Any;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use derivative::Derivative;
use tokio::sync::mpsc::{self, Sender};
use tokio::sync::Mutex as TokioMutex;
use tracing::{info, instrument};

use crate::manager::Command;
use crate::network::{BandwidthAllowed, BandwidthLimit, BandwidthTx};
use crate::reader::{CouldNotCreateRuntime, Reader};
use crate::store::migrate::MigrationError;
use crate::store::{migrate, StoreReader, StoreWriter};
use crate::{http_client, store};

mod builder;
mod drop;
mod reporting;
mod task;

pub use builder::StreamBuilder;
pub use reporting::RangeUpdate;
pub(crate) use reporting::{Report, ReportTx};
pub(crate) use task::retry;
pub use task::StreamDone;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Communicating with stream server ran into an issue: {0}")]
    HttpClient(#[from] http_client::Error),
    #[error("Could not write to storage, io error: {0:?}")]
    Writing(std::io::Error),
    #[error("Error flushing store to durable storage: {0:?}")]
    Flushing(store::Error),
    #[error("Callback provided by the user panicked: {0:?}")]
    UserCallbackPanicked(Box<dyn Any + Send + 'static>),
}

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct Id(usize);

impl Id {
    pub(super) fn new() -> Self {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self(id)
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ManagedHandle {
    /// allows the handle to send a message
    /// to the manager to drop the streams future
    /// or increase/decrease priority.
    #[derivative(Debug = "ignore")]
    cmd_manager: Sender<Command>,
    handle: Handle,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Handle {
    prefetch: usize,
    #[derivative(Debug = "ignore")]
    seek_tx: mpsc::Sender<u64>,
    #[derivative(Debug = "ignore")]
    pause_tx: mpsc::Sender<bool>,
    #[derivative(Debug = "ignore")]
    bandwidth_lim_tx: BandwidthTx,
    is_paused: bool,

    #[derivative(Debug(format_with = "mutex_in_use"))]
    store_reader: Arc<TokioMutex<StoreReader>>,
    store_writer: StoreWriter,
}

fn mutex_in_use(
    store: &Arc<TokioMutex<StoreReader>>,
    fmt: &mut std::fmt::Formatter,
) -> std::result::Result<(), std::fmt::Error> {
    let reader_in_use = store.try_lock().is_err();
    if reader_in_use {
        fmt.write_str("yes")
    } else {
        fmt.write_str("no")
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GetReaderError {
    #[error(
        "Can only have one reader at the time, please drop the 
            existing one before getting a new one"
    )]
    ReaderInUse,
    #[error("Could not set up a runtime to get a reader in a blocking fashion: {0}")]
    CreationFailed(CouldNotCreateRuntime),
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

macro_rules! blocking {
    ($name:ident - $new_name:ident $($param:ident: $t:ty),* $(; $ret:ty)?) => {
        /// blocking variant
        ///
        /// # Panics
        ///
        /// This function panics if called within an asynchronous execution
        /// context.
        pub fn $new_name(&mut self, $($param: $t),*) $(-> $ret)? {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(self.$name($($param),*))
        }
    };
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
    managed_async! {use_limited_mem_backend max_cap: NonZeroUsize; Result<(), MigrationError>}
    managed_async! {use_unlimited_mem_backend ; Result<(), MigrationError>}
    managed_async! {use_disk_backend path: PathBuf; Result<(), MigrationError>}
    managed_async! {flush ; Result<(), Error>}

    managed! {try_get_reader; Result<crate::reader::Reader, GetReaderError>}
}
impl ManagedHandle {
    blocking! {pause - pause_blocking}
    blocking! {unpause - unpause_blocking}
    blocking! {limit_bandwidth - limit_bandwidth_blocking bandwidth: BandwidthLimit}
    blocking! {remove_bandwidth_limit - remove_bandwidth_limit_blocking}
    blocking! {use_limited_mem_backend - use_mem_backend_blocking max_cap: NonZeroUsize; Result<(), MigrationError>}
    blocking! {use_unlimited_mem_backend - use_unlimited_mem_backend_blocking ; Result<(), MigrationError>}
    blocking! {use_disk_backend - use_disk_backend_blocking path: PathBuf; Result<(), MigrationError>}
    blocking! {flush - flush_blocking; Result<(), Error>}
}

impl Drop for ManagedHandle {
    fn drop(&mut self) {
        self.cmd_manager
            .try_send(Command::CancelStream(self.id()))
            .expect("could not cancel stream task when handle was dropped")
    }
}

impl Handle {
    pub async fn limit_bandwidth(&self, bandwidth: BandwidthLimit) {
        self.bandwidth_lim_tx
            .send(BandwidthAllowed::Limited(bandwidth))
            .await
            .expect("rx is part of Task, which should not drop before Handle");
    }

    pub async fn remove_bandwidth_limit(&self) {
        self.bandwidth_lim_tx
            .send(BandwidthAllowed::UnLimited)
            .await
            .expect("rx is part of Task, which should not drop before Handle");
    }

    pub async fn pause(&mut self) {
        if !self.is_paused {
            self.pause_tx
                .send(true)
                .await
                .expect("rx is part of Task, which should not drop before Handle");
            self.is_paused = true;
            info!("pausing stream")
        }
    }

    pub async fn unpause(&mut self) {
        if self.is_paused {
            self.pause_tx
                .send(false)
                .await
                .expect("rx is part of Task, which should not drop before Handle");
            self.is_paused = false;
        }
        info!("unpausing stream")
    }

    #[instrument(level = "debug", ret, err(Debug))]
    pub fn try_get_reader(&mut self) -> Result<crate::reader::Reader, GetReaderError> {
        let store = self
            .store_reader
            .clone()
            .try_lock_owned()
            .map_err(|_| GetReaderError::ReaderInUse)?;
        Reader::new(self.prefetch, self.seek_tx.clone(), store)
            .map_err(GetReaderError::CreationFailed)
    }

    pub async fn use_limited_mem_backend(
        &mut self,
        max_cap: NonZeroUsize,
    ) -> Result<(), MigrationError> {
        migrate::to_mem(&mut self.store_writer, max_cap).await
    }

    pub async fn use_unlimited_mem_backend(&mut self) -> Result<(), MigrationError> {
        migrate::to_unlimited_mem(&mut self.store_writer).await
    }

    pub async fn use_disk_backend(&mut self, path: PathBuf) -> Result<(), MigrationError> {
        migrate::to_disk(&mut self.store_writer, path).await
    }

    /// Only does something when the store actually supports flush
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn flush(&mut self) -> Result<(), Error> {
        self.store_writer.flush().await.map_err(Error::Flushing)
    }
}

/// blocking implementations of the async functions above
impl Handle {
    blocking! {pause - pause_blocking}
    blocking! {unpause - unpause_blocking}
    blocking! {limit_bandwidth - limit_bandwidth_blocking bandwidth: BandwidthLimit}
    blocking! {remove_bandwidth_limit - remove_bandwidth_limit_blocking}
    blocking! {use_limited_mem_backend - use_mem_backend_blocking max_cap: NonZeroUsize; Result<(), MigrationError>}
    blocking! {use_disk_backend - use_disk_backend_blocking path: PathBuf; Result<(), MigrationError>}
    blocking! {flush - flush_blocking ; Result<(), Error>}
}

#[must_use]
pub struct StreamEnded {
    pub(super) res: Result<StreamDone, Error>,
    pub(super) id: Id,
}
