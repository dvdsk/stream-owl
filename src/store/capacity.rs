/// This limits the writer from writing if there is no space. The reader
/// can free up space. Only works when there is only one reader and writer.
///
/// ** The normal situation **
///
/// #########      (succeeded now)     #########        (blocked)
/// # write # -- out_of_capacity() --> # write # -> wait_for_space(token)
/// #########                          #########             ^
///                                                          |
/// ########                                                 |
/// # read # --- send_available_for_all() ------------> unblocks writer
/// ########
///
/// ** Seek **
///
/// #########                          #########        (blocked)
/// # write # -- out_of_capacity() --> # write # -> wait_for_space(token)
/// #########                          #########
///
/// ########                                                 
/// # seek # --- send_available_for(token) ----------> unblocks writer
/// ########                                                 |
///                                                          |
///                                                          V
///                                    #########        (blocked)
///                                    # write # -> wait_for_space_at(new_pos)
///                                    #########
///
///
use std::future::pending;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

use derivative::Derivative;
use tokio::sync::Notify;
use tracing::instrument;

#[derive(Debug, Clone, Copy)]
pub(crate) enum Bounds {
    Unlimited,
    Limited(NonZeroU64),
}

/// Only a stream with the correct token may access the writer. Upon
/// seek tokens are changed. This way an old in progress write will
/// not use up the space freed by the seek. See module store::capacity for 
/// more info.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WriterToken(pub(crate) u8);

impl WriterToken {
    pub(crate) fn first() -> Self {
        Self(0b0000_0001)
    }

    pub(crate) fn switch(&mut self) {
        self.0 = if self.0 == 0b0000_0001 {
            0b0000_0010
        } else {
            0b0000_0001
        };
    }
}

pub(crate) type CapacityNotifier = CapacityWatcher;

/// Tracks which token will get access. Three states:
/// - token 0b0000_0001 may write
/// - token 0b0000_0010 may write
/// - there is no capacity no one may write
#[derive(Debug)]
struct CapacityMask(AtomicU8);

impl CapacityMask {
    fn new() -> Self {
        Self(AtomicU8::new(0b1111_1111))
    }
    fn is_available_for(&self, mask: u8) -> bool {
        (self.0.load(Ordering::Acquire) & mask) == mask
    }
    fn mark_available_for(&self, mask: u8) {
        self.0.store(mask, Ordering::Release)
    }
    fn mark_available_for_all(&self) {
        self.0.store(0b1111_1111, Ordering::Release)
    }
    fn mark_empty(&self) {
        self.0.store(0, Ordering::Release)
    }
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) struct CapacityWatcher {
    free_capacity: Arc<CapacityMask>,
    #[derivative(Debug = "ignore")]
    new_free_capacity: Arc<Notify>,
}

impl CapacityWatcher {
    /// blocks till capacity is available
    /// does not synchronize so only works
    /// with sequential access
    #[tracing::instrument(level = "trace", skip_all)]
    pub(crate) async fn wait_for_space(&self, writer_mask: WriterToken) {
        let new_free_capacity = self.new_free_capacity.notified();
        if self.free_capacity.is_available_for(writer_mask.0) {
            return;
        }

        tracing::trace!("waiting for space");
        // if we get notified then space got freed up
        new_free_capacity.await;
        if !self.free_capacity.is_available_for(writer_mask.0) {
            pending().await // seek just happened, gonna be cancelled soon
        }
    }

    pub(crate) fn out_of_capacity(&self) {
        tracing::trace!("store is out of capacity");
        self.free_capacity.mark_empty();
    }

    #[instrument(level = "trace", skip(self))]
    pub(crate) fn send_available_for(&mut self, mask: WriterToken) {
        tracing::trace!("store has capacity again");
        self.free_capacity.mark_available_for(mask.0);
        self.new_free_capacity.notify_waiters();
    }

    #[instrument(level = "trace", skip(self))]
    pub(crate) fn send_available_for_all(&mut self) {
        tracing::trace!("store has capacity again");
        self.free_capacity.mark_available_for_all();
        self.new_free_capacity.notify_waiters();
    }
}

#[instrument(level = "debug", ret)]
pub(crate) fn new() -> (CapacityWatcher, CapacityNotifier) {
    let cap_watch = CapacityWatcher {
        new_free_capacity: Arc::new(Notify::new()),
        free_capacity: Arc::new(CapacityMask::new()),
    };
    (cap_watch.clone(), cap_watch)
}
