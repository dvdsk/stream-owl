use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use rangemap::set::RangeSet;
use tokio::sync::oneshot::error::RecvError;
use tokio::sync::Mutex;
use tracing::{instrument, trace};

use super::capacity::CapacityWatcher;
use super::disk;
use super::disk::Disk;
use super::limited_mem;
use super::unlimited_mem;
use super::{range_watch, Store, StoreVariant};
use crate::store;
use crate::store::migrate::range_list::RangeLen;

mod range_list;

#[instrument(skip(store, capacity_watch), ret)]
pub(crate) async fn to_mem(
    store: Arc<Mutex<Store>>,
    capacity_watch: CapacityWatcher,
    max_cap: NonZeroUsize,
) -> Result<(), MigrationError> {
    if let StoreVariant::MemLimited = store.lock().await.variant().await {
        return Ok(());
    }

    // is swapped out before migration finishes
    let (watch_placeholder, _) = range_watch::channel();
    let mem = limited_mem::Memory::new(max_cap, watch_placeholder)?;

    migrate(store.clone(), Store::MemLimited(mem), capacity_watch).await
}

#[instrument(skip(store, capacity_watch), ret)]
pub(crate) async fn to_unlimited_mem(
    store: Arc<Mutex<Store>>,
    capacity_watch: CapacityWatcher,
) -> Result<(), MigrationError> {
    if let StoreVariant::MemUnlimited = store.lock().await.variant().await {
        return Ok(());
    }

    // is swapped out before migration finishes
    let (watch_placeholder, _) = range_watch::channel();
    let mem = unlimited_mem::Memory::new(watch_placeholder);
    migrate(store.clone(), Store::MemUnlimited(mem), capacity_watch).await
}

#[instrument(skip(store, capacity_watch), ret)]
pub(crate) async fn to_disk(
    store: Arc<Mutex<Store>>,
    capacity_watch: CapacityWatcher,
    path: PathBuf,
) -> Result<(), MigrationError> {
    if let StoreVariant::Disk = store.lock().await.variant().await {
        return Ok(());
    }

    // is swapped out before migration finishes
    let (watch_placeholder, _) = range_watch::channel();
    let disk = Disk::new(path, watch_placeholder).await?;
    migrate(store.clone(), Store::Disk(disk), capacity_watch).await
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
async fn migrate(
    curr: Arc<Mutex<Store>>,
    mut target: Store,
    capacity: CapacityWatcher,
) -> Result<(), MigrationError> {
    pre_migrate(&curr, &mut target).await?;
    let mut curr = curr.lock().await;
    finish_migration(&mut curr, &mut target).await?;

    let target_ref = &mut target;
    std::mem::swap(&mut *curr, target_ref);
    let old = target;

    let range_watch = old.into_range_watch();
    curr.set_range_tx(range_watch);
    capacity.re_init_from(curr.can_write());

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
        // TODO this read should not change the src capacity
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
