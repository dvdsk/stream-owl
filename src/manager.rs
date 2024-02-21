use std::time::Duration;

use derivative::Derivative;
use tokio::sync::{mpsc, oneshot};

use crate::network::{BandwidthAllowed, BandwidthLimit, Network};
use crate::retry::{RetryDurLimit, RetryLimit};
use crate::{BandwidthCallback, LogCallback, RangeCallback};

mod builder;
pub mod stream;
mod task;
pub(crate) use task::Command;

use crate::Placeholder;
use self::builder::ManagerBuilder;
use self::stream::StreamConfig;

#[derive(Debug)]
pub struct Error;

#[derive(Derivative, Clone)]
#[derivative(Debug)]
pub(crate) struct Callbacks<L: LogCallback, B: BandwidthCallback, R: RangeCallback> {
    #[derivative(Debug = "ignore")]
    retry_log: L,
    #[derivative(Debug = "ignore")]
    bandwidth: B,
    #[derivative(Debug = "ignore")]
    range: R,
}

#[derive(Derivative)]
#[derivative(Debug)]
pub struct Manager<R: RangeCallback> {
    cmd_tx: mpsc::Sender<task::Command<R>>,

    stream_defaults: StreamConfig,
    restriction: Option<Network>,
    total_bandwidth: BandwidthAllowed,

    retry_disabled: bool,
    max_retries: RetryLimit,
    max_retry_dur: RetryDurLimit,
    timeout: Duration,
}

impl<R: RangeCallback> Manager<R> {
    pub fn builder() -> ManagerBuilder<Placeholder, Placeholder, Placeholder> {
        ManagerBuilder::default()
    }

    /// panics if called from an async context
    pub fn add(&mut self, url: http::Uri) -> stream::ManagedHandle<R> {
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
    ) -> stream::ManagedHandle<R> {
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
