use std::fs;
use std::path::PathBuf;

use crate::store::disk::progress;

pub(super) struct NewStoreCleanupGuard {
    pub(super) path: PathBuf,
}

pub(super) struct GuardNotNeeded;

impl Drop for GuardNotNeeded {
    fn drop(&mut self) {}
}

impl Drop for NewStoreCleanupGuard {
    fn drop(&mut self) {
        let progress_path = progress::progress_path(self.path.clone());
        let _ignore_err = fs::remove_file(&self.path);
        let _ignore_err = fs::remove_file(progress_path);
    }
}
