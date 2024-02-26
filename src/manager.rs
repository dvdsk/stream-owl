use std::time::Duration;

use derivative::Derivative;
use tokio::sync::{mpsc, oneshot};

use crate::network::{BandwidthAllowed, BandwidthLimit, Network};
use crate::retry::{RetryDurLimit, RetryLimit};
use crate::{
    BandwidthCallback, IdBandwidthCallback, IdLogCallback, IdRangeCallback, LogCallback,
    RangeCallback, RangeUpdate,
};

mod builder;
pub mod stream;
mod task;
pub(crate) use task::Command;

use self::builder::ManagerBuilder;
use self::stream::{ManagedHandle, StreamConfig};
use crate::Placeholder;

#[derive(Debug)]
pub struct Error;

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) struct Callbacks<L: IdLogCallback, B: IdBandwidthCallback, R: IdRangeCallback> {
    #[derivative(Debug = "ignore")]
    retry_log: L,
    #[derivative(Debug = "ignore")]
    bandwidth: B,
    #[derivative(Debug = "ignore")]
    range: R,
}

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) struct WrappedCallbacks<L: LogCallback, B: BandwidthCallback, R: RangeCallback> {
    #[derivative(Debug = "ignore")]
    retry_log: L,
    #[derivative(Debug = "ignore")]
    bandwidth: B,
    #[derivative(Debug = "ignore")]
    range: R,
}

#[derive(Clone)]
struct CallWithId<F: Clone> {
    callback: F,
    id: crate::StreamId,
}

macro_rules! id_callback {
    ($trait_with_id:ident, $trait_without_id:ident, $arg:ty) => {
        impl<F: $trait_with_id> $trait_without_id for CallWithId<F> {
            fn perform(&mut self, update: $arg) {
                self.callback.perform(self.id, update)
            }
        }
    };
}

id_callback! {IdRangeCallback, RangeCallback, RangeUpdate}
id_callback! {IdBandwidthCallback, BandwidthCallback, usize}
id_callback! {IdLogCallback, LogCallback, std::sync::Arc<crate::http_client::Error>}

impl<L, B, R> Callbacks<L, B, R>
where
    L: IdLogCallback,
    B: IdBandwidthCallback,
    R: IdRangeCallback,
{
    fn wrap(
        &self,
        id: crate::StreamId,
    ) -> WrappedCallbacks<CallWithId<L>, CallWithId<B>, CallWithId<R>> {
        let Callbacks {
            retry_log,
            bandwidth,
            range,
        } = self.clone();
        WrappedCallbacks {
            retry_log: CallWithId {
                callback: retry_log,
                id,
            },
            bandwidth: CallWithId {
                callback: bandwidth,
                id,
            },
            range: CallWithId {
                callback: range,
                id,
            },
        }
    }
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Manager {
    cmd_tx: mpsc::Sender<task::Command>,

    stream_defaults: StreamConfig,
    restriction: Option<Network>,
    total_bandwidth: BandwidthAllowed,

    retry_disabled: bool,
    max_retries: RetryLimit,
    max_retry_dur: RetryDurLimit,
    timeout: Duration,
}

impl Manager {
    pub fn builder() -> ManagerBuilder<Placeholder, Placeholder, Placeholder> {
        ManagerBuilder::default()
    }

    /// panics if called from an async context
    pub fn add(&mut self, url: http::Uri) -> stream::ManagedHandle {
        let config = self.stream_defaults.clone();
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .blocking_send(Command::AddStream {
                url,
                handle_tx: tx,
                config,
            })
            .expect("manager task should still run");
        rx.blocking_recv().unwrap()
    }

    /// panics if called from an async context
    pub fn add_with_options(
        &mut self,
        url: http::Uri,
        configurator: impl FnOnce(StreamConfig) -> StreamConfig,
    ) -> ManagedHandle {
        let config = self.stream_defaults.clone();
        let config = (configurator)(config);
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .blocking_send(Command::AddStream {
                url,
                handle_tx: tx,
                config,
            })
            .expect("manager task should still run");
        rx.blocking_recv().unwrap()
    }

    pub fn limit_bandwidth(&mut self, _bandwidth: BandwidthLimit) {
        todo!();
    }
}
