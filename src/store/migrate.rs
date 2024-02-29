use std::ops::Range;
use std::path::PathBuf;

use rangemap::set::RangeSet;
use tokio::sync::Mutex;
use tracing::{debug, info};
use tracing::{instrument, trace};

use super::disk;
use super::disk::Disk;
use super::limited_mem;
use super::unlimited_mem;
use super::StoreWriter;
use super::{Store, StoreVariant};
use crate::store;
use crate::util::RangeLen;

mod cancel;
mod range_list;

#[instrument(skip(store_writer))]
pub(crate) async fn to_mem<F: crate::RangeCallback>(
    store_writer: &mut StoreWriter<F>,
    max_cap: usize,
) -> Result<(), MigrationError> {
    if let StoreVariant::MemLimited = store_writer.variant().await {
        return Ok(());
    }

    let mem = limited_mem::Memory::new(max_cap)?;
    migrate(store_writer, Store::MemLimited(mem), cancel::GuardNotNeeded).await
}

#[instrument(skip(store_writer))]
pub(crate) async fn to_unlimited_mem<F: crate::RangeCallback>(
    store_writer: &mut StoreWriter<F>,
) -> Result<(), MigrationError> {
    if let StoreVariant::MemUnlimited = store_writer.variant().await {
        return Ok(());
    }

    let mem = unlimited_mem::Memory::new();
    migrate(
        store_writer,
        Store::MemUnlimited(mem),
        cancel::GuardNotNeeded,
    )
    .await
}

#[instrument(skip(store_writer))]
pub(crate) async fn to_disk<F: crate::RangeCallback>(
    store_writer: &mut StoreWriter<F>,
    path: PathBuf,
) -> Result<(), MigrationError> {
    if let StoreVariant::Disk = store_writer.variant().await {
        return Ok(());
    }

    let guard = cancel::NewStoreCleanupGuard { path: path.clone() };
    let (disk, _) = Disk::new(path).await?;
    migrate(store_writer, Store::Disk(disk), guard).await
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
    #[error("Something went wrong while cleaning up no longer needed files: {0}")]
    Cleanup(disk::Error),
}

/// cancel safe however will block shortly while cleaning up
#[instrument(skip_all)]
async fn migrate<F, G>(
    store_writer: &mut StoreWriter<F>,
    mut target: Store,
    cleanup_new_on_cancel: G,
) -> Result<(), MigrationError>
where
    F: crate::RangeCallback,
    G: Sized,
{
    let StoreWriter {
        curr_store,
        capacity_watcher,
        range_watch,
    } = store_writer;

    pre_migrate(&curr_store, &mut target).await?;
    debug!("finished pre-migration, acquiring exclusive access to store");
    let mut curr = curr_store.lock().await;
    finish_migration(&mut curr, &mut target).await?;
    debug!("finished data migration");

    let target_ref = &mut target;
    std::mem::swap(&mut *curr, target_ref);
    let old = target;

    let old_ranges = old.ranges();
    if curr.can_write() {
        capacity_watcher.send_available_for_all();
    } else {
        capacity_watcher.out_of_capacity();
    }

    debug!("updating callbacks and range_watch");
    range_watch.send_diff(old_ranges, curr.ranges());
    std::mem::forget(cleanup_new_on_cancel);

    if let Store::Disk(old_disk_store) = old {
        let continue_cleanup_on_cancel = cancel::NewStoreCleanupGuard {
            path: old_disk_store.path.clone(),
        };
        old_disk_store
            .cleanup()
            .await
            .map_err(MigrationError::Cleanup)?;
        std::mem::forget(continue_cleanup_on_cancel);
    }
    info!("migration done");
    Ok(())
}

#[instrument(skip_all)]
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

#[instrument(skip_all)]
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
fn missing(a: &RangeSet<u64>, b: &RangeSet<u64>) -> Option<Range<u64>> {
    let mut in_b = b.iter();
    loop {
        let missing_in_a = a.gaps(in_b.next()?).next();
        if missing_in_a.is_some() {
            return missing_in_a;
        }
    }
}
