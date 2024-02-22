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

#[derive(Debug, Clone)]
pub struct Placeholder;

// doing this instead of type aliases which are unstable
macro_rules! callback {
    ($name:ident, $($arg_ident:ident: $arg_ty:ty),+) => {
        pub trait $name: Send + Clone + 'static {
            fn perform(&mut self, $($arg_ident: $arg_ty),+);
        }

        impl<C: FnMut($($arg_ty),+) + Send + Clone + 'static> $name for C {
            fn perform(&mut self, $($arg_ident: $arg_ty),+) {
                (self)($($arg_ident),+)
            }
        }

        impl $name for Placeholder {
            #![allow(unused_variables)]
            fn perform(&mut self, $($arg_ident: $arg_ty),+) {
                tracing::trace!("callback is none")
            }
        }
    };
}

callback!(RangeCallback, update: RangeUpdate);
callback!(LogCallback, error: std::sync::Arc<http_client::Error>);
callback!(BandwidthCallback, bandwidth: usize);

callback!(IdRangeCallback, id: StreamId, update: RangeUpdate);
callback!(
    IdLogCallback,
    id: StreamId, error: std::sync::Arc<http_client::Error>
);
callback!(IdBandwidthCallback, id: StreamId, bandwidth: usize);

pub type UnconfiguredSB = StreamBuilder<false, Placeholder, Placeholder, Placeholder>;
