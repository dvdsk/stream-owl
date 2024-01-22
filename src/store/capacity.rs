/// This limits the writer from writing if there is no space. The reader
/// can free up space. Only works under two assumptions, which are currently true:
///  - There is only one reader and writer
///  - Before the writer calls `CapacityWatcher::wait_for_space` the store its
///    writing too has updated the capacity using `Capacity::remove`.
///
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use derivative::Derivative;
use tokio::sync::Notify;
use tracing::instrument;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Bounds {
    Unlimited,
    Limited(NonZeroU64),
}

pub(crate) type CapacityNotifier = CapacityWatcher;

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) struct CapacityWatcher {
    has_free_capacity: Arc<AtomicBool>,
    #[derivative(Debug = "ignore")]
    new_free_capacity: Arc<Notify>,

    migrating: Arc<AtomicBool>,
    #[derivative(Debug = "ignore")]
    migration_done: Arc<Notify>,
}

/// Ends the migrating lock once it is dropped 
pub(crate) struct MigrateLock<'a> {
    migrating: &'a AtomicBool,
    migration_done: &'a Notify,
}

impl<'a> Drop for MigrateLock<'a> {
    fn drop(&mut self) {
        self.migrating.store(false, Ordering::Release);
        self.migration_done.notify_waiters();
    }
}

impl CapacityWatcher {
    /// blocks till capacity is available
    /// does not synchronize so only works
    /// with sequential access
    #[tracing::instrument(level = "trace", skip_all)]
    pub(crate) async fn wait_for_space(&self) {
        let migration_done = self.migration_done.notified();
        if self.migrating.load(Ordering::Acquire) {
            tracing::debug!("waiting for migration to complete");
            migration_done.await;
        }

        let new_free_capacity = self.new_free_capacity.notified();
        if self.has_free_capacity.load(Ordering::Acquire) {
            return;
        }

        tracing::trace!("waiting for space");
        // if we get notified then space got freed up
        new_free_capacity.await;
    }

    /// used in migrations
    /// called to stop writers passing wait_for_space
    /// prevents a writer from getting access for the old store
    /// and then writing in the new store where no space is left
    pub(crate) fn start_migration<'a>(&'a self) -> MigrateLock<'a> {
        self.migrating.store(true, Ordering::Release);
        MigrateLock {
            migrating: &self.migrating,
            migration_done: &self.migration_done,
        }
    }

    pub(crate) fn re_init_from(&self, can_write: bool) {
        self.has_free_capacity.store(can_write, Ordering::Release);
        if can_write {
            self.new_free_capacity.notify_waiters()
        }
    }

    pub(crate) fn out_of_capacity(&self) {
        tracing::trace!("store is out of capacity");
        self.has_free_capacity.store(false, Ordering::Release);
    }

    #[instrument(level = "trace", skip(self))]
    pub(crate) fn send_available(&mut self) {
        tracing::trace!("store has capacity again");
        self.has_free_capacity.store(true, Ordering::Release);
        self.new_free_capacity.notify_waiters()
    }

    #[instrument(level = "trace", skip(self))]
    pub(crate) fn reset(&mut self) {
        self.send_available()
    }
}

#[instrument(level = "debug", ret)]
pub(crate) fn new() -> (CapacityWatcher, CapacityNotifier) {
    let cap_watch = CapacityWatcher {
        new_free_capacity: Arc::new(Notify::new()),
        has_free_capacity: Arc::new(AtomicBool::new(true)),
        migrating: Arc::new(AtomicBool::new(false)),
        migration_done: Arc::new(Notify::new()),
    };
    (cap_watch.clone(), cap_watch)
}
