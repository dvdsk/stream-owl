mod manager;
mod network;
mod reader;
pub mod store;

pub mod http_client;
mod stream;
/// Glue between store and stream/http_client
mod target;

mod util;

/// internal use only! in time move this to tests/common/common.rs
/// for now RA needs it here and we need RA
#[doc(hidden)]
pub mod testing;

pub use stream::{StreamBuilder, RangeUpdate};

pub use manager::Error as ManagerError;
pub use manager::Manager;
pub use stream::Error as StreamError;
pub use stream::StreamDone;

pub use network::{list_interfaces, BandwidthLimit};
pub use reader::Reader;
pub use stream::Handle as StreamHandle;
pub use stream::Id as StreamId;
pub use stream::ManagedHandle as ManagedStreamHandle;
