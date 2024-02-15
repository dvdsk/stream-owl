use derivative::Derivative;
use futures::FutureExt;
use futures_concurrency::future::Race;
use rangemap::RangeSet;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::instrument;

pub mod disk;
pub mod limited_mem;
pub mod migrate;
pub(crate) mod range_watch;
pub mod unlimited_mem;

mod capacity;
pub(crate) use capacity::Bounds as CapacityBounds;
use capacity::CapacityNotifier;
pub(crate) use capacity::{CapacityWatcher, WriterToken};

use crate::http_client::Size;
use crate::stream::ReportTx;
use crate::RangeUpdate;

#[derive(Debug)]
pub(crate) struct StoreReader {
    pub(super) capacity: CapacityNotifier,
    pub(crate) curr_store: Arc<Mutex<Store>>,
    curr_range: range_watch::Receiver,
    stream_size: Size,
}

#[derive(Derivative)]
#[derivative(Debug, Clone)]
pub(crate) struct StoreWriter {
    pub(crate) curr_store: Arc<Mutex<Store>>,
    pub(crate) capacity_watcher: CapacityWatcher,
    #[derivative(Debug = "ignore")]
    range_watch: range_watch::Sender,
}

impl Drop for StoreWriter {
    fn drop(&mut self) {
        self.range_watch.close()
    }
}

#[derive(Debug)]
pub(crate) enum Store {
    Disk(disk::Disk),
    MemLimited(limited_mem::Memory),
    MemUnlimited(unlimited_mem::Memory),
}

#[derive(Debug, Clone)]
pub(super) enum StoreVariant {
    Disk,
    MemLimited,
    MemUnlimited,
}

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Could not create limited mem store: {0}")]
    CreatingMemoryLimited(#[from] limited_mem::CouldNotAllocate),
    #[error("Error working with limited mem store: {0}")]
    MemoryUnlimited(unlimited_mem::CouldNotAllocate),
    #[error("Error in disk store: {0}")]
    Disk(#[from] disk::Error),
    #[error("Could not create disk store: {0}")]
    OpeningDisk(#[from] disk::OpenError),
}

fn store_handles(
    store: Store,
    rx: range_watch::Receiver,
    capacity: CapacityNotifier,
    capacity_watcher: CapacityWatcher,
    range_watch: range_watch::Sender,
    stream_size: Size,
) -> (StoreReader, StoreWriter) {
    let curr_store = Arc::new(Mutex::new(store));
    (
        StoreReader {
            curr_range: rx,
            curr_store: curr_store.clone(),
            stream_size: stream_size.clone(),
            capacity,
        },
        StoreWriter {
            curr_store,
            capacity_watcher,
            range_watch,
        },
    )
}

#[tracing::instrument]
pub(crate) async fn new_disk_backed(
    path: PathBuf,
    stream_size: Size,
    report_tx: ReportTx,
) -> Result<(StoreReader, StoreWriter), disk::OpenError> {
    let (capacity_watcher, capacity) = capacity::new();
    let (tx, rx) = range_watch::channel(report_tx);

    let (disk, already_downloaded) = disk::Disk::new(path).await?;
    for range in already_downloaded {
        tx.send(RangeUpdate::Added(range))
    }
    Ok(store_handles(
        Store::Disk(disk),
        rx,
        capacity,
        capacity_watcher,
        tx,
        stream_size,
    ))
}

#[tracing::instrument]
pub(crate) fn new_limited_mem_backed(
    max_cap: usize,
    stream_size: Size,
    report_tx: ReportTx,
) -> Result<(StoreReader, StoreWriter), limited_mem::CouldNotAllocate> {
    let (capacity_watcher, capacity) = capacity::new();
    let (tx, rx) = range_watch::channel(report_tx);
    let mem = limited_mem::Memory::new(max_cap)?;
    Ok(store_handles(
        Store::MemLimited(mem),
        rx,
        capacity,
        capacity_watcher,
        tx,
        stream_size,
    ))
}

#[tracing::instrument]
pub(crate) fn new_unlimited_mem_backed(
    stream_size: Size,
    report_tx: ReportTx,
) -> (StoreReader, StoreWriter) {
    let (capacity_watcher, capacity) = capacity::new();
    let (tx, rx) = range_watch::channel(report_tx);
    let mem = unlimited_mem::Memory::new();
    store_handles(
        Store::MemUnlimited(mem),
        rx,
        capacity,
        capacity_watcher,
        tx,
        stream_size,
    )
}

impl StoreWriter {
    /// Note this is not an implementation of the std::io::Write tait. Ok(0) does
    /// **not** mean we will never be able to write again.
    #[instrument(level = "trace", skip(self, buf))]
    pub(crate) async fn write_at(
        &mut self,
        buf: &[u8],
        pos: u64,
        token: WriterToken,
    ) -> Result<usize, Error> {
        self.capacity_watcher.wait_for_space(token).await;
        // if a migration happens while we are here then we could get
        // into store::write_at, without it having free capacity.
        // In that case write_at will return zero (which is fine)
        let (n_read, range_update) = self
            .curr_store
            .lock()
            .await
            .write_at(buf, pos, Some(&mut self.capacity_watcher))
            .await?;

        self.range_watch.send(range_update);
        Ok(n_read)
    }
    /// Only does something when the store actually supports flush
    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn flush(&mut self) -> Result<(), Error> {
        self.curr_store.lock().await.flush().await
    }

    pub async fn variant(&self) -> StoreVariant {
        self.curr_store.lock().await.variant().await
    }
}

impl StoreReader {
    /// Returns number of bytes read, 0 means end of file.
    /// This reads as soon as bytes are available.
    #[tracing::instrument(level = "trace", skip(buf), fields(buf_len = buf.len()), ret)]
    pub(crate) async fn read_at(&mut self, buf: &mut [u8], pos: u64) -> Result<usize, ReadError> {
        enum Res {
            StreamWasClosed,
            RangeReady,
            PosBeyondEOF,
        }

        let Self {
            curr_range,
            stream_size,
            ..
        } = self;

        // There is no race condition between lock and wait_for_range because:
        //  - a migration will never remove the byte directly after pos
        //  - a new write can not free up the byte at pos
        // Thus the write will never return 0 bytes read when it passes wait_for_range.
        let watch_eof_pos = stream_size.eof_smaller_then(pos).map(|_| Res::PosBeyondEOF);
        let wait_for_range = curr_range.wait_for(pos).map(|res| match res {
            Ok(_) => Res::RangeReady,
            Err(_) => Res::StreamWasClosed,
        });

        let res = (wait_for_range, watch_eof_pos).race().await;
        match res {
            Res::PosBeyondEOF | Res::StreamWasClosed => Err(ReadError::EndOfStream),
            Res::RangeReady => {
                let n_read = self
                    .curr_store
                    .lock()
                    .await
                    .read_at(buf, pos, Some(&mut self.capacity))
                    .await?;
                Ok(n_read)
            }
        }
    }

    /// refers to the size of the stream if it was complete
    pub(crate) fn stream_size(&self) -> Size {
        self.stream_size.clone()
    }
}

impl Store {
    pub(crate) async fn variant(&self) -> StoreVariant {
        match self {
            Self::Disk(_) => StoreVariant::Disk,
            Self::MemLimited(_) => StoreVariant::MemLimited,
            Self::MemUnlimited(_) => StoreVariant::MemUnlimited,
        }
    }
    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) async fn flush(&mut self) -> Result<(), Error> {
        if let Store::Disk(disk_store) = self {
            disk_store.flush().await.map_err(Error::Disk)
        } else {
            Ok(())
        }
    }
}

#[derive(Debug)]
pub(crate) struct SeekInProgress;

#[derive(thiserror::Error, Debug)]
pub(crate) enum ReadError {
    #[error(transparent)]
    Store(#[from] Error),
    #[error("End of stream reached")]
    EndOfStream,
}

macro_rules! forward_impl {
    ($v:vis $fn_name:ident, $($param:ident: $t:ty),*; $returns:ty) => {
        impl Store {
            $v fn $fn_name(&self, $($param: $t),*) -> $returns {
                match self {
                    Self::Disk(inner) => inner.$fn_name($($param),*),
                    Self::MemUnlimited(inner) => inner.$fn_name($($param),*),
                    Self::MemLimited(inner) => inner.$fn_name($($param),*),
                }
            }
        }
    };
}

forward_impl!(pub(crate) gapless_from_till, pos: u64, last_seek: u64; bool);
forward_impl!(pub(crate) ranges,; RangeSet<u64>);
forward_impl!(last_read_pos,; u64);
forward_impl!(n_supported_ranges,; crate::util::MaybeLimited<NonZeroUsize>);

impl Store {
    /// capacity_watch can be left out when migrating as migration
    /// will only write upto the capacity of the underlying storage
    #[instrument(level = "debug", skip(buf), err)]
    pub(crate) async fn write_at(
        &mut self,
        buf: &[u8],
        pos: u64,
        capacity_watch: Option<&mut CapacityWatcher>,
    ) -> Result<(usize, RangeUpdate), Error> {
        match self {
            Store::Disk(inner) => inner.write_at(buf, pos).await.map_err(Error::Disk),
            Store::MemLimited(inner) => {
                let res = inner.write_at(buf, pos).await;
                if let Some(capacity_watch) = capacity_watch {
                    if inner.available_for_writing == 0 {
                        capacity_watch.out_of_capacity();
                    }
                }
                Ok(res)
            }
            Store::MemUnlimited(inner) => inner
                .write_at(buf, pos)
                .await
                .map_err(Error::MemoryUnlimited),
        }
    }
    pub(crate) async fn read_at(
        &mut self,
        buf: &mut [u8],
        pos: u64,
        update_capacity: Option<&mut CapacityNotifier>,
    ) -> Result<usize, Error> {
        match self {
            Self::Disk(inner) => inner.read_at(buf, pos).await.map_err(Error::Disk),
            Self::MemLimited(inner) => {
                let n_read = inner.read_at(buf, pos);
                if let Some(capacity) = update_capacity {
                    if inner.available_for_writing > 0 {
                        capacity.send_available_for_all()
                    }
                }
                Ok(n_read)
            }
            Self::MemUnlimited(inner) => Ok(inner.read_at(buf, pos)),
        }
    }

    fn capacity(&self) -> CapacityBounds {
        match self {
            Store::Disk(_) => CapacityBounds::Unlimited,
            Store::MemLimited(inner) => CapacityBounds::Limited(
                NonZeroU64::new(inner.capacity() as u64)
                    .expect("LimitedMem capacity is set via NonZeroU64"),
            ),
            Store::MemUnlimited(_) => CapacityBounds::Unlimited,
        }
    }

    fn can_write(&self) -> bool {
        match self {
            Store::Disk(_) => true,
            Store::MemLimited(inner) => inner.available_for_writing > 0,
            Store::MemUnlimited(_) => true,
        }
    }
}
