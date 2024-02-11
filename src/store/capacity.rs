/// This limits the writer from writing if there is no space. The reader
/// can free up space. Only works under two assumptions, which are currently true:
///  - There is only one reader and writer
///  - Before the writer calls `CapacityWatcher::wait_for_space` the store its
///    writing too has updated the capacity using `Capacity::remove`.
///
/// ** The normal situation **
///
/// #########      (succeeded now)     #########        (blocked)
/// # write # -- out_of_capacity() --> # write # -> wait_for_space()
/// #########                          #########             |
///                                                         |
/// ########                                                V
/// # read # --- send_available --------------------> unblocks writer
/// ########
///
/// ** Seek **
///
/// #########                          #########        (blocked)
/// # write # -- out_of_capacity() --> # write # -> wait_for_space() 
/// #########                          #########
///
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
}

impl CapacityWatcher {
    /// blocks till capacity is available
    /// does not synchronize so only works
    /// with sequential access
    #[tracing::instrument(level = "trace", skip_all)]
    pub(crate) async fn wait_for_space(&self) {
        let new_free_capacity = self.new_free_capacity.notified();
        if self.has_free_capacity.load(Ordering::Acquire) {
            dbg!(std::thread::sleep(std::time::Duration::from_secs(1)));
            return;
        }

        tracing::trace!("waiting for space");
        // if we get notified then space got freed up
        new_free_capacity.await;
    }

    pub(crate) fn re_init_from(&self, can_write: bool) {
        self.has_free_capacity.store(can_write, Ordering::Release);
        if can_write {
            self.new_free_capacity.notify_one()
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
        self.new_free_capacity.notify_one();
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
    };
    (cap_watch.clone(), cap_watch)
}
