use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::http_client::Error;
use crate::stream::{Report, ReportTx};
use derivative::Derivative;
use http::StatusCode;
use tracing::{debug, instrument, trace, warn};

#[derive(Derivative)]
#[derivative(Debug)]
struct Event {
    error: Arc<Error>,
    #[derivative(Debug(format_with = "fmt_at_field"))]
    original_error: Instant,
    #[derivative(Debug(format_with = "fmt_at_field"))]
    last_occurrence: Instant,
    approach: Backoff,
    n_occurred: usize,
}

impl Event {
    fn next_try_in(&mut self) -> Duration {
        let since_last_try = self.last_occurrence.elapsed();
        self.approach
            .curr_retry_dur()
            .saturating_sub(since_last_try)
    }

    fn update(&mut self) {
        self.last_occurrence = Instant::now();
        self.n_occurred += 1;
        match self.approach {
            Backoff::Constant(_) => todo!(),
            Backoff::Fibonacci {
                ref mut before_last,
                ref mut last,
                ..
            } => {
                let new_last = *last + *before_last;
                *before_last = *last;
                *last = new_last;
            }
        }
    }
}

fn fmt_at_field(
    instant: &Instant,
    fmt: &mut std::fmt::Formatter,
) -> std::result::Result<(), std::fmt::Error> {
    fmt.write_fmt(format_args!("{}ms ago", instant.elapsed().as_millis()))
}

#[derive(Debug)]
enum Backoff {
    Constant(Duration),
    Fibonacci {
        last: Duration,
        before_last: Duration,
        max: Duration,
    },
}

impl Backoff {
    fn fib(base: Duration, max: Duration) -> Self {
        Self::Fibonacci {
            last: base,
            before_last: Duration::ZERO,
            max,
        }
    }
    fn max_retry_dur(&self) -> Duration {
        match self {
            Backoff::Constant(d) => *d,
            Backoff::Fibonacci { max, .. } => *max,
        }
    }
    fn curr_retry_dur(&self) -> Duration {
        match self {
            Backoff::Constant(d) => *d,
            Backoff::Fibonacci {
                last,
                before_last,
                max,
            } => (*max).min(*last + *before_last),
        }
    }
}

pub(crate) enum CouldSucceed {
    Yes,
    No(Error),
}

macro_rules! limit_type {
    ($name:ident: $type:ty, $forbidden:expr, $max:expr) => {
        #[derive(Debug, Default, Clone)]
        pub(crate) enum $name {
            #[default]
            Unlimited,
            Limited($type),
        }

        impl $name {
            pub fn new(val: $type) -> Self {
                if val == $forbidden {
                    panic!("Can not be {:?}", $forbidden);
                }

                if val == $max {
                    Self::Unlimited
                } else {
                    Self::Limited(val)
                }
            }
        }
    };
}

limit_type!(RetryLimit: usize, 0, usize::MAX);
limit_type!(RetryDurLimit: Duration, Duration::ZERO, Duration::MAX);

#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Decider {
    recent: Vec<Event>,
    log_to_user: Option<ReportTx>,
    retry_disabled: bool,
    /// how often the **same error** may happen before we give up
    retry_limit: RetryLimit,
    /// how long the **same error** may happen before we give up
    max_retry_dur: RetryDurLimit,
}

impl Decider {
    pub(crate) fn with_limits(
        max_retries: RetryLimit,
        max_retry_dur: RetryDurLimit,
        log_to_user: ReportTx,
    ) -> Self {
        Self {
            retry_disabled: false,
            recent: Vec::new(),
            log_to_user: Some(log_to_user),
            retry_limit: max_retries,
            max_retry_dur,
        }
    }

    pub(crate) fn disabled() -> Decider {
        Self {
            retry_disabled: true,
            log_to_user: None,
            recent: Vec::new(),
            retry_limit: RetryLimit::default(),
            max_retry_dur: RetryDurLimit::default(),
        }
    }

    fn n_occurred(&self, error: &Error) -> usize {
        self.recent
            .iter()
            .find(|event| event.error.as_ref() == error)
            .map(|event| event.n_occurred)
            .unwrap_or(0)
    }

    fn since_first_occurred(&self, error: &Error) -> Duration {
        self.recent
            .iter()
            .find(|event| event.error.as_ref() == error)
            .map(|event| event.original_error.elapsed())
            .unwrap_or(Duration::ZERO)
    }

    fn remove_stale_entries(&mut self) {
        let to_remove: Vec<_> = self
            .recent
            .iter()
            .enumerate()
            .filter(|(_, event)| {
                event.last_occurrence.elapsed() > 5 * event.approach.max_retry_dur()
            })
            .map(|(idx, _)| idx)
            .collect();

        for idx in to_remove.into_iter().rev() {
            let removed = self.recent.remove(idx);
            trace!("removed old error: {removed:?}");
        }
    }

    #[instrument(level = "debug")]
    fn register(&mut self, error: Arc<Error>, approach: Backoff) {
        self.remove_stale_entries();

        let existing = self.recent.iter_mut().find(|event| event.error == error);
        if let Some(existing) = existing {
            warn!("Stream error occurred again: {error}, will retry");
            existing.update()
        } else {
            warn!("Error during stream: {error}, will retry");
            self.recent.push(Event {
                error,
                original_error: Instant::now(),
                last_occurrence: Instant::now(),
                approach,
                n_occurred: 1,
            });
        }
    }

    #[instrument(level = "debug")]
    fn register_or_reject(&mut self, err: Error, approach: Backoff) -> CouldSucceed {
        match self.retry_limit {
            RetryLimit::Limited(allowed) if self.n_occurred(&err) >= allowed => {
                return CouldSucceed::No(err)
            }
            _ => (),
        }

        match self.max_retry_dur {
            RetryDurLimit::Limited(allowed) if self.since_first_occurred(&err) >= allowed => {
                return CouldSucceed::No(err)
            }
            _ => (),
        }

        let error = Arc::new(err);
        if let Some(ref mut report_tx) = self.log_to_user {
            let _ignore_closed_channel = report_tx.send(Report::RetriedError(error.clone()));
        }
        self.register(error, approach);
        CouldSucceed::Yes
    }

    #[instrument(level = "debug", skip(self))]
    pub fn could_succeed(&mut self, err: Error) -> CouldSucceed {
        if self.retry_disabled {
            return CouldSucceed::No(err);
        }

        use Error as E;
        const MAX_RETY_INTERVAL: Duration = Duration::from_secs(5 * 60);
        const RETRY_FAST: Duration = Duration::from_millis(200);
        const RETRY_SLOW: Duration = Duration::from_millis(2000);

        match err {
            E::SocketCreation(_) | E::SocketConfig(_) => {
                self.register_or_reject(err, Backoff::Constant(RETRY_SLOW))
            }
            E::Connecting(_)
            | E::DnsResolve(_)
            | E::SendingRequest(_)
            | E::Handshake(_)
            | E::ReadingBody(_) => {
                self.register_or_reject(err, Backoff::fib(RETRY_FAST, MAX_RETY_INTERVAL))
            }
            E::StatusNotOk {
                code:
                    StatusCode::INTERNAL_SERVER_ERROR
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::GATEWAY_TIMEOUT,
                ..
            } => self.register_or_reject(err, Backoff::fib(RETRY_SLOW, MAX_RETY_INTERVAL)),
            E::Response(_) | E::WritingData(_) => todo!(),
            E::Http(_)
            | E::Restricting(_)
            | E::InvalidHost(_)
            | E::DnsEmpty
            | E::BrokenRedirectLocation(_)
            | E::MissingRedirectLocation
            | E::InvalidUriRedirectLocation(_)
            | E::TooManyRedirects
            | E::MissingFrame
            | E::StatusNotOk { .. }
            | E::UrlWithoutHost => CouldSucceed::No(err),
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn ready(&mut self) {
        if let Some(until_next_try) = self.recent.iter_mut().map(Event::next_try_in).max() {
            debug!("retrying in {} ms", until_next_try.as_millis());
            tokio::time::sleep(until_next_try).await
        }
    }
}
