use std::thread;

use tokio::runtime::{self, RuntimeFlavor};
use tracing::{instrument, warn, debug};

use crate::StreamHandle;

fn drop_in_new_thread(handle: &mut StreamHandle) {
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

fn drop_in_current_rt(handle: &mut StreamHandle, rt: runtime::Handle) {
    debug!("dropping StreamHandle using current runtime");
    tokio::task::block_in_place(move || {
        if let Err(err) = rt.block_on(handle.flush()) {
            warn!("Lost some progress, flushing storage failed: {err}")
        }
    });
}

fn drop_in_new_runtime(handle: &mut StreamHandle) {
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

impl Drop for StreamHandle {
    #[instrument]
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
