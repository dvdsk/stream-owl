use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;

use derivative::Derivative;
use tokio::sync::mpsc;
use tokio::sync::Mutex as TokioMutex;
use tracing::{info, instrument};

use crate::network::{BandwidthAllowed, BandwidthLimit, BandwidthTx};
use crate::reader::{CouldNotCreateRuntime, Reader};
use crate::store::migrate::MigrationError;
pub use crate::store::range_watch::RangeUpdate;
use crate::store::{migrate, StoreReader, StoreWriter, WriterToken};
use crate::{http_client, store, StreamId};

mod builder;
mod drop;
pub(crate) mod task;

pub use builder::StreamBuilder;
pub use task::StreamCanceld;

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

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Handle<F: crate::RangeCallback> {
    pub(crate) prefetch: usize,
    #[derivative(Debug = "ignore")]
    pub(crate) seek_tx: mpsc::Sender<(u64, WriterToken)>,
    #[derivative(Debug = "ignore")]
    pub(crate) pause_tx: mpsc::Sender<bool>,
    #[derivative(Debug = "ignore")]
    pub(crate) bandwidth_lim_tx: BandwidthTx,
    pub(crate) is_paused: bool,

    #[derivative(Debug(format_with = "mutex_in_use"))]
    pub(crate) store_reader: Arc<TokioMutex<StoreReader>>,
    // used for flushing and migrations only
    pub(crate) store_writer: StoreWriter<F>,
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
pub(crate) use blocking;

impl<F: crate::RangeCallback> Handle<F> {
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
            info!("unpausing stream")
        }
    }

    #[instrument(level = "debug", skip(self), ret, err(Debug))] // TODO remove skip self
    pub fn try_get_reader(&mut self) -> Result<crate::reader::Reader, GetReaderError> {
        let store = self
            .store_reader
            .clone()
            .try_lock_owned()
            .map_err(|_| GetReaderError::ReaderInUse)?;
        Reader::new(self.prefetch, self.seek_tx.clone(), store)
            .map_err(GetReaderError::CreationFailed)
    }

    pub async fn migrate_to_limited_mem_backend(
        &mut self,
        max_cap: usize,
    ) -> Result<(), MigrationError> {
        migrate::to_mem(&mut self.store_writer, max_cap).await
    }

    pub async fn migrate_to_unlimited_mem_backend(&mut self) -> Result<(), MigrationError> {
        migrate::to_unlimited_mem(&mut self.store_writer).await
    }

    pub async fn migrate_to_disk_backend(&mut self, path: PathBuf) -> Result<(), MigrationError> {
        migrate::to_disk(&mut self.store_writer, path).await
    }

    /// Only does something when the store actually supports flush
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn flush(&mut self) -> Result<(), Error> {
        self.store_writer.flush().await.map_err(Error::Flushing)
    }
}

/// blocking implementations of the async functions above
impl<F: crate::RangeCallback> Handle<F> {
    blocking! {pause - pause_blocking}
    blocking! {unpause - unpause_blocking}
    blocking! {limit_bandwidth - limit_bandwidth_blocking bandwidth: BandwidthLimit}
    blocking! {remove_bandwidth_limit - remove_bandwidth_limit_blocking}
    blocking! {migrate_to_limited_mem_backend - migrate_to_limited_mem_backend_bocking max_cap: usize; Result<(), MigrationError>}
    blocking! {migrate_to_unlimited_mem_backend - migrate_to_unlimited_mem_backend_blocking ; Result<(), MigrationError>}
    blocking! {migrate_to_disk_backend - migrate_to_disk_backend_blocking path: PathBuf; Result<(), MigrationError>}
    blocking! {flush - flush_blocking ; Result<(), Error>}
}

#[must_use]
pub struct StreamEnded {
    pub(super) res: Result<StreamCanceld, Error>,
    pub(super) id: StreamId,
}
