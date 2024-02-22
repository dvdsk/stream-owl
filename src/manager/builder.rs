use std::time::Duration;

use derivative::Derivative;
use futures::Future;
use tokio::sync::mpsc;

use crate::network::{BandwidthAllowed, Network};
use crate::retry::{RetryDurLimit, RetryLimit};
use crate::{
    Manager, ManagerError, StreamError, StreamId, Placeholder, IdLogCallback, IdRangeCallback, IdBandwidthCallback,
};

use super::Callbacks;
use super::StreamConfig;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct ManagerBuilder<L, B, R> {
    stream_defaults: StreamConfig,
    restriction: Option<Network>,
    total_bandwidth: BandwidthAllowed,

    retry_disabled: bool,
    max_retries: RetryLimit,
    max_retry_dur: RetryDurLimit,
    timeout: Duration,

    #[derivative(Debug = "ignore")]
    retry_log_callback: L,
    #[derivative(Debug = "ignore")]
    bandwidth_callback: B,
    #[derivative(Debug = "ignore")]
    range_callback: R,
}


impl Default for ManagerBuilder<Placeholder, Placeholder, Placeholder> {
    fn default() -> Self {
        Self {
            stream_defaults: StreamConfig::default(),
            restriction: None,
            total_bandwidth: BandwidthAllowed::default(),
            retry_disabled: false,
            max_retries: RetryLimit::default(),
            max_retry_dur: RetryDurLimit::default(),
            timeout: Duration::from_secs(3),
            retry_log_callback: Placeholder,
            bandwidth_callback: Placeholder,
            range_callback: Placeholder,
        }
    }
}

impl<L, B, R> ManagerBuilder<L, B, R>
where
    L: IdLogCallback,
    B: IdBandwidthCallback,
    R: IdRangeCallback,
{
    pub fn with_stream_defaults(mut self, defaults: StreamConfig) -> Self {
        self.stream_defaults = defaults;
        self
    }
    pub fn with_maximum_total_bandwidth(mut self, network: Network) -> Self {
        self.restriction = Some(network);
        self
    }

    /// Default is false
    pub fn retry_disabled(mut self, retry_disabled: bool) -> Self {
        self.retry_disabled = retry_disabled;
        self
    }
    /// How often the **same error** may happen without an error free window
    /// before giving up and returning an error to the user.
    ///
    /// If this number is passed the stream is aborted and an error
    /// returned to the user.
    ///
    /// By default this period is unbounded (infinite)
    pub fn with_max_retries(mut self, n: usize) -> Self {
        self.max_retries = RetryLimit::new(n);
        self
    }
    /// How long the **same error** may keep happening without some error
    /// free time in between.
    ///
    /// If this duration is passed the stream is aborted and an error
    /// returned to the user.
    ///
    /// By default this period is unbounded (infinite)
    pub fn with_max_retry_duration(mut self, duration: Duration) -> Self {
        self.max_retry_dur = RetryDurLimit::new(duration);
        self
    }

    /// How long an operation may hang before we abort it
    ///
    /// By default this is 2 seconds
    pub fn with_timeout(mut self, duration: Duration) -> Self {
        self.timeout = duration;
        self
    }
    /// By default all networks are allowed
    pub fn with_network_restriction(mut self, allowed_network: Network) -> Self {
        self.restriction = Some(allowed_network);
        self
    }

    /// Perform an callback whenever a retry happens. Useful to log
    /// errors.
    pub fn with_retry_callback<NewL: IdLogCallback>(
        self,
        callback: NewL,
    ) -> ManagerBuilder<NewL, B, R> {
        ManagerBuilder {
            stream_defaults: self.stream_defaults,
            restriction: self.restriction,
            total_bandwidth: self.total_bandwidth,
            retry_disabled: self.retry_disabled,
            max_retries: self.max_retries,
            max_retry_dur: self.max_retry_dur,
            timeout: self.timeout,
            retry_log_callback: callback,
            bandwidth_callback: self.bandwidth_callback,
            range_callback: self.range_callback,
        }
    }

    /// Perform an callback whenever the bandwidth has an update
    pub fn with_range_callback<NewR: IdRangeCallback>(
        self,
        callback: NewR,
    ) -> ManagerBuilder<L, B, NewR> {
        ManagerBuilder {
            stream_defaults: self.stream_defaults,
            restriction: self.restriction,
            total_bandwidth: self.total_bandwidth,
            retry_disabled: self.retry_disabled,
            max_retries: self.max_retries,
            max_retry_dur: self.max_retry_dur,
            timeout: self.timeout,
            retry_log_callback: self.retry_log_callback,
            bandwidth_callback: self.bandwidth_callback,
            range_callback: callback,
        }
    }

    /// Perform an callback whenever the range locally available
    /// has changed
    pub fn with_bandwidth_callback<NewB: IdBandwidthCallback>(
        self,
        callback: NewB,
    ) -> ManagerBuilder<L, NewB, R> {
        ManagerBuilder {
            stream_defaults: self.stream_defaults,
            restriction: self.restriction,
            total_bandwidth: self.total_bandwidth,
            retry_disabled: self.retry_disabled,
            max_retries: self.max_retries,
            max_retry_dur: self.max_retry_dur,
            timeout: self.timeout,
            retry_log_callback: self.retry_log_callback,
            bandwidth_callback: callback,
            range_callback: self.range_callback,
        }
    }

    pub fn build(
        self,
    ) -> (
        Manager,
        impl Future<Output = ManagerError>,
        mpsc::UnboundedReceiver<(StreamId, StreamError)>,
    ) {
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        let (err_tx, err_rx) = mpsc::unbounded_channel();

        let callbacks = Callbacks {
            retry_log: self.retry_log_callback,
            bandwidth: self.bandwidth_callback,
            range: self.range_callback,
        };
        (
            Manager {
                cmd_tx: cmd_tx.clone(),
                stream_defaults: self.stream_defaults,

                restriction: self.restriction,
                total_bandwidth: self.total_bandwidth,
                retry_disabled: self.retry_disabled,
                max_retries: self.max_retries,
                max_retry_dur: self.max_retry_dur,
                timeout: self.timeout,
            },
            super::task::run(cmd_tx, cmd_rx, err_tx, callbacks),
            err_rx,
        )
    }
}
