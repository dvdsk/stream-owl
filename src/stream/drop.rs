use std::thread;

use tokio::runtime::{self, RuntimeFlavor};
use tracing::{debug, instrument, warn};

use crate::StreamHandle;

fn drop_in_new_thread<F: crate::RangeCallback>(handle: &mut StreamHandle<F>) {
    debug!("dropping StreamHandle using new thread for async runtime");
    thread::scope(|s| {
        s.spawn(|| {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    warn!("Could not flush storage as Runtime creation failed, error: {e}");
                    return;
                }
            };
            let res = rt.block_on(handle.flush());
            if let Err(err) = res {
                warn!("Lost some progress, flushing storage failed: {err}")
            }
        });
    });
}

fn drop_in_current_rt<F: crate::RangeCallback>(
    handle: &mut StreamHandle<F>,
    rt: runtime::Handle,
) {
    debug!("dropping StreamHandle using current runtime");
    tokio::task::block_in_place(move || {
        if let Err(err) = rt.block_on(handle.flush()) {
            warn!("Lost some progress, flushing storage failed: {err}")
        }
    });
}

fn drop_in_new_runtime<F: crate::RangeCallback>(handle: &mut StreamHandle<F>) {
    debug!("dropping StreamHandle in new runtime in current thread");
    let rt = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            warn!("Could not flush storage as Runtime creation failed, error: {e}");
            return;
        }
    };
    let res = rt.block_on(handle.flush());
    if let Err(err) = res {
        warn!("Lost some progress, flushing storage failed: {err}")
    }
}

impl<F: crate::RangeCallback> Drop for StreamHandle<F> {
    #[instrument(skip(self))] // TODO remove skip self
    fn drop(&mut self) {
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            if let RuntimeFlavor::CurrentThread = rt.runtime_flavor() {
                drop_in_new_thread(self)
            } else {
                drop_in_current_rt(self, rt)
            }
        } else {
            drop_in_new_runtime(self)
        };
    }
}
