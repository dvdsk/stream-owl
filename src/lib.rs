#![recursion_limit = "150"]
mod manager;
mod network;
mod reader;
pub mod store;

pub mod http_client;
mod retry;
mod stream;
/// Glue between store and stream/http_client
mod target;

mod util;

/// internal use only! in time move this to tests/common/common.rs
/// for now RA needs it here and we need RA
#[doc(hidden)]
pub mod testing;

pub use stream::{RangeUpdate, StreamBuilder};

pub use stream::Error as StreamError;
pub use stream::Handle as StreamHandle;
pub use stream::StreamCanceld;

pub use network::{list_interfaces, BandwidthLimit};
pub use reader::Reader;

pub use manager::stream::Id as StreamId;
pub use manager::stream::ManagedHandle as ManagedStreamHandle;
pub use manager::Error as ManagerError;
pub use manager::Manager;

// doing this instead of type aliases which are unstable
macro_rules! callback {
    ($name:ident, $arg:ty) => {
        trait $name: Send + Clone + 'static {
            fn perform(&mut self, update: $arg);
        }

        impl<C: FnMut($arg) + Send + Clone + 'static> $name for C {
            fn perform(&mut self, update: $arg) {
                (self)(update)
            }
        }
    };
}

callback!(RangeCallback, RangeUpdate);
callback!(LogCallback, std::sync::Arc<http_client::Error>);
callback!(BandwidthCallback, usize);

#[derive(Debug, Clone)]
pub(crate) struct Placeholder;

macro_rules! placeholder {
    ($trait:ident, $arg:ty) => {
        impl $trait for Placeholder {
            fn perform(&mut self, val: $arg) {
                tracing::trace!("callback is none")
            }
        }
    };
}

placeholder!(RangeCallback, crate::RangeUpdate);
placeholder!(LogCallback, std::sync::Arc<crate::http_client::Error>);
placeholder!(BandwidthCallback, usize);
