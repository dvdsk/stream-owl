use std::time::Duration;

use derivative::Derivative;
use tokio::sync::{mpsc, oneshot};

use crate::network::{BandwidthAllowed, Network};
use crate::retry::{RetryDurLimit, RetryLimit};
use crate::{
    BandwidthCallback, BandwidthLimit, IdBandwidthCallback, IdLogCallback, IdRangeCallback,
    LogCallback, RangeCallback, RangeUpdate,
};

mod builder;
mod handle;
pub mod stream;
mod task;
pub use handle::ManagedHandle;
pub(crate) use task::Command;

use self::builder::ManagerBuilder;
use self::stream::StreamConfig;
use self::task::bandwidth;
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

/* TODO: drop causes task to shutdown? <dvdsk> */
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Manager {
    cmd_tx: mpsc::Sender<task::Command>,
    bandwidth_tx: mpsc::Sender<bandwidth::Update>,

    stream_defaults: StreamConfig,
    restriction: Option<Network>,

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
    pub async fn add(&mut self, url: http::Uri) -> ManagedHandle {
        let config = self.stream_defaults.clone();
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::AddStream {
                url,
                handle_tx: tx,
                config,
            })
            .await
            .expect("manager task should still run");
        rx.await.expect("manager task should still run")
    }

    /// panics if called from an async context
    pub async fn add_with_options(
        &mut self,
        url: http::Uri,
        configurator: impl FnOnce(StreamConfig) -> StreamConfig,
    ) -> ManagedHandle {
        let config = self.stream_defaults.clone();
        let config = (configurator)(config);
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .send(Command::AddStream {
                url,
                handle_tx: tx,
                config,
            })
            .await
            .expect("manager task should still run");
        rx.await.expect("manger task should still run")
    }

    pub async fn limit_bandwidth(&mut self, limit: BandwidthLimit) {
        let bandwidth = BandwidthAllowed::Limited(limit);
        self.bandwidth_tx
            .send(bandwidth::Update::NewGlobalLimit { bandwidth })
            .await
            .expect("")
    }
}
