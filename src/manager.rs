use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;
use tokio::sync::{mpsc, oneshot};

use crate::network::{BandwidthAllowed, BandwidthLimit, Network};
use crate::stream::retry::{RetryDurLimit, RetryLimit};
use crate::{http_client, RangeUpdate, StreamId};

mod builder;
mod config;
pub mod stream;
mod task;
pub(crate) use task::Command;

use self::builder::ManagerBuilder;
use self::config::StreamConfig;

#[derive(Debug)]
pub struct Error;

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Callbacks {
    #[derivative(Debug(format_with = "crate::util::fmt_non_printable_option"))]
    retry_log_callback: Option<Arc<dyn Fn(StreamId, Arc<http_client::Error>) + Send + Sync>>,
    #[derivative(Debug(format_with = "crate::util::fmt_non_printable_option"))]
    bandwidth_callback: Option<Arc<dyn Fn(StreamId, usize) + Send + Sync>>,
    #[derivative(Debug(format_with = "crate::util::fmt_non_printable_option"))]
    range_callback: Option<Arc<dyn Fn(StreamId, RangeUpdate) + Send + Sync>>,
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

    callbacks: Callbacks,
}

impl Manager {
    pub fn builder() -> ManagerBuilder {
        ManagerBuilder::default()
    }

    /// panics if called from an async context
    pub fn add(&mut self, url: http::Uri) -> stream::ManagedHandle {
        let id = StreamId::new();
        let config = self.stream_defaults.clone();
        let stream = config.into_stream_builder(id, url, &mut self.callbacks);
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .blocking_send(Command::AddStream {
                builder: stream,
                handle_tx: tx,
                id,
            })
            .expect("manager task should still run");
        rx.blocking_recv().unwrap()
    }

    /// panics if called from an async context
    pub fn add_with_options(
        &mut self,
        url: http::Uri,
        configurator: impl FnOnce(StreamConfig) -> StreamConfig,
    ) -> stream::ManagedHandle {
        let id = StreamId::new();
        let config = self.stream_defaults.clone();
        let config = (configurator)(config);
        let stream = config.into_stream_builder(id, url, &mut self.callbacks);
        let (tx, rx) = oneshot::channel();
        self.cmd_tx
            .blocking_send(Command::AddStream {
                builder: stream,
                handle_tx: tx,
                id,
            })
            .expect("manager task should still run");
        rx.blocking_recv().unwrap()
    }

    pub fn limit_bandwidth(&mut self, _bandwidth: BandwidthLimit) {
        todo!();
    }
}
