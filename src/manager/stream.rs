use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use derivative::Derivative;
use tokio::sync::mpsc::Sender;

use crate::manager::Command;
use crate::network::BandwidthLimit;
use crate::store::migrate::MigrationError;
use crate::stream::GetReaderError;
use crate::{StreamError, StreamHandle};
use crate::stream::blocking;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Id(usize);

impl Id {
    pub(crate) fn new() -> Self {
        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        Self(id)
    }
}
macro_rules! managed_async {
    ($fn_name:ident $($param:ident: $t:ty),*$(; $returns:ty)?) => {
        pub async fn $fn_name(&mut self, $($param: $t),*) $(-> $returns)? {
            self.handle.$fn_name($($param),*).await
        }
    };
}

macro_rules! managed {
    ($fn_name:ident $($param:ident: $t:ty),*$(; $returns:ty)?) => {
        pub fn $fn_name(&mut self, $($param: $t),*) $(-> $returns)? {
            self.handle.$fn_name($($param),*)
        }
    };
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ManagedHandle {
    /// allows the handle to send a message
    /// to the manager to drop the streams future
    /// or increase/decrease priority.
    #[derivative(Debug = "ignore")]
    pub(crate) cmd_manager: Sender<Command>,
    pub(crate) handle: StreamHandle,
}

impl Drop for ManagedHandle {
    fn drop(&mut self) {
        self.cmd_manager
            .try_send(Command::CancelStream(self.id()))
            .expect("could not cancel stream task when handle was dropped")
    }
}

impl ManagedHandle {
    pub fn set_priority(&mut self, _arg: i32) {
        todo!()
    }

    pub fn id(&self) -> Id {
        todo!()
    }

    managed_async! {pause}
    managed_async! {unpause}
    managed_async! {limit_bandwidth bandwidth: BandwidthLimit}
    managed_async! {remove_bandwidth_limit}
    managed_async! {migrate_to_limited_mem_backend max_cap: usize; Result<(), MigrationError>}
    managed_async! {migrate_to_unlimited_mem_backend ; Result<(), MigrationError>}
    managed_async! {migrate_to_disk_backend path: PathBuf; Result<(), MigrationError>}
    managed_async! {flush ; Result<(), StreamError>}

    managed! {try_get_reader; Result<crate::reader::Reader, GetReaderError>}
}
impl ManagedHandle {
    blocking! {pause - pause_blocking}
    blocking! {unpause - unpause_blocking}
    blocking! {limit_bandwidth - limit_bandwidth_blocking bandwidth: BandwidthLimit}
    blocking! {remove_bandwidth_limit - remove_bandwidth_limit_blocking}
    blocking! {migrate_to_limited_mem_backend - migrate_to_limited_mem_backend_blocking max_cap: usize; Result<(), MigrationError>}
    blocking! {migrate_to_unlimited_mem_backend - migrate_to_unlimited_mem_backend_blocking ; Result<(), MigrationError>}
    blocking! {migrate_to_disk_backend - migrate_to_disk_backend_blocking path: PathBuf; Result<(), MigrationError>}
    blocking! {flush - flush_blocking; Result<(), StreamError>}
}
