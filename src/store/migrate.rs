use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::PathBuf;

use rangemap::set::RangeSet;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::Mutex;
use tracing::{instrument, trace};

use super::disk;
use super::disk::Disk;
use super::limited_mem;
use super::unlimited_mem;
use super::StoreWriter;
use super::{Store, StoreVariant};
use crate::store;
use crate::util::RangeLen;

mod range_list;

#[instrument(skip(store_writer), ret)]
pub(crate) async fn to_mem(
    store_writer: &mut StoreWriter,
    max_cap: NonZeroUsize,
) -> Result<(), MigrationError> {
    if let StoreVariant::MemLimited = store_writer.variant().await {
        return Ok(());
    }

    let mem = limited_mem::Memory::new(max_cap)?;
    migrate(store_writer, Store::MemLimited(mem)).await
}

#[instrument(skip(store_writer), ret)]
pub(crate) async fn to_unlimited_mem(store_writer: &mut StoreWriter) -> Result<(), MigrationError> {
    if let StoreVariant::MemUnlimited = store_writer.variant().await {
        return Ok(());
    }

    let mem = unlimited_mem::Memory::new();
    migrate(store_writer, Store::MemUnlimited(mem)).await
}

#[instrument(skip(store_writer), ret)]
pub(crate) async fn to_disk(
    store_writer: &mut StoreWriter,
    path: PathBuf,
) -> Result<(), MigrationError> {
    if let StoreVariant::Disk = store_writer.variant().await {
        return Ok(());
    }

    let (disk, _) = Disk::new(path).await?;
    migrate(store_writer, Store::Disk(disk)).await
}

#[derive(thiserror::Error, Debug)]
pub enum MigrationError {
    #[error("Can not start migration, failed to create disk store: {0}")]
    DiskCreation(#[from] disk::OpenError),
    #[error("Can not start migration, failed to create limited memory store: {0}")]
    MemLimited(#[from] limited_mem::CouldNotAllocate),
    #[error("Error during pre-migration, failed to read from current store: {0}")]
    PreMigrateRead(store::Error),
    #[error("Error during pre-migration, failed to write to new store: {0}")]
    PreMigrateWrite(store::Error),
    #[error("Error during migration, failed to read from current store: {0}")]
    MigrateRead(store::Error),
    #[error("Error during migration, failed to write to new store: {0}")]
    MigrateWrite(store::Error),
    #[error("Migration task panicked, reason unknown :(")]
    Panic(RecvError),
}

#[instrument(skip_all, ret)]
async fn migrate(store_writer: &mut StoreWriter, mut target: Store) -> Result<(), MigrationError> {
    let StoreWriter {
        curr_store,
        capacity_watcher,
        range_watch,
    } = store_writer;
    pre_migrate(&curr_store, &mut target).await?;
    let mut curr = curr_store.lock().await;
    finish_migration(&mut curr, &mut target).await?;

    let target_ref = &mut target;
    std::mem::swap(&mut *curr, target_ref);
    let old = target;

    let old_ranges = old.ranges();
    capacity_watcher.re_init_from(curr.can_write());
    range_watch.send_diff(old_ranges, curr.ranges());

    Ok(())
}

#[instrument(skip_all, ret, err)]
async fn pre_migrate(curr: &Mutex<Store>, target: &mut Store) -> Result<(), MigrationError> {
    let mut buf = Vec::with_capacity(4096);
    let mut on_target = RangeSet::new();
    let capacity = target.capacity();
    loop {
        let mut src = curr.lock().await;
        let needed_from_src = range_list::ranges_we_can_take(&src, target);
        let needed_form_src = range_list::correct_for_capacity(needed_from_src, &capacity);

        let Some(missing) = missing(&on_target, &needed_form_src) else {
            return Ok(());
        };
        trace!("copying range missing on target: {missing:?}");
        let len = missing.len().min(4096);
        buf.resize(len as usize, 0u8);
        // TODO this read should not change the src capacity
        src.read_at(&mut buf, missing.start, None)
            .await
            .map_err(MigrationError::PreMigrateRead)?;
        drop(src);

        target
            .write_at(&buf, missing.start, None)
            .await
            .map_err(MigrationError::PreMigrateWrite)?;
        on_target.insert(missing);
    }
}

#[instrument(skip_all, ret)]
async fn finish_migration(curr: &mut Store, target: &mut Store) -> Result<(), MigrationError> {
    let mut buf = Vec::with_capacity(4096);
    let mut on_disk = RangeSet::new();
    loop {
        let in_mem = curr.ranges();
        let Some(missing_on_disk) = missing(&on_disk, &in_mem) else {
            return Ok(());
        };
        let len = missing_on_disk.len().min(4096);
        buf.resize(len as usize, 0u8);
        curr.read_at(&mut buf, missing_on_disk.start, None)
            .await
            .map_err(MigrationError::MigrateRead)?;

        target
            .write_at(&buf, missing_on_disk.start, None)
            .await
            .map_err(MigrationError::MigrateWrite)?;
        on_disk.insert(missing_on_disk);
    }
}

/// return a range that exists in b but not in a
#[instrument(level = "debug", ret)]
fn missing(a: &RangeSet<u64>, b: &RangeSet<u64>) -> Option<Range<u64>> {
    let mut in_b = b.iter();
    loop {
        let missing_in_a = a.gaps(in_b.next()?).next();
        if missing_in_a.is_some() {
            return missing_in_a;
        }
    }
}
