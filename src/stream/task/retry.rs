use std::time::{Duration, Instant};

use crate::http_client::Error;
use derivative::Derivative;
use http::StatusCode;
use tracing::{debug, instrument, warn};

#[derive(Derivative)]
#[derivative(Debug)]
struct Event {
    error: Error,
    #[derivative(Debug(format_with = "fmt_at_field"))]
    original_error: Instant,
    #[derivative(Debug(format_with = "fmt_at_field"))]
    last_occurrence: Instant,
    approach: Backoff,
}

impl Event {
    fn retry_period(&self) -> Duration {
        match self.approach {
            Backoff::Constant(d) => d,
            Backoff::Fibonacci {
                last,
                before_last,
                max,
            } => max.min(last + before_last),
        }
    }

    fn next_try_in(&mut self) -> Duration {
        let since_last_try = self.last_occurrence.elapsed();
        self.retry_period().saturating_sub(since_last_try)
    }

    fn update(&mut self) {
        self.last_occurrence = Instant::now();
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

#[derive(Debug, Default)]
pub(super) struct Decider {
    recent: Vec<Event>,
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
}

pub(super) enum CouldSucceed {
    Yes,
    No(super::Error),
}

impl Decider {
    fn remove_stale_entries(&mut self) {
        let to_remove: Vec<_> = self
            .recent
            .iter()
            .enumerate()
            .filter(|(_, event)| event.last_occurrence.elapsed() > 5 * event.retry_period())
            .map(|(idx, _)| idx)
            .collect();

        for idx in to_remove.into_iter().rev() {
            self.recent.remove(idx);
        }
    }

    #[instrument(level = "debug")]
    fn update(&mut self, error: Error, approach: Backoff) {
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
            });
        }
    }

    #[instrument(level = "debug", skip(self))]
    pub fn could_succeed(&mut self, err: Error) -> CouldSucceed {
        use Error as E;
        const MAX_RETY_INTERVAL: Duration = Duration::from_secs(5 * 60);

        match err {
            E::SocketCreation(_) | E::SocketConfig(_) => {
                self.update(err, Backoff::Constant(Duration::from_secs(5)));
                CouldSucceed::Yes
            }
            E::Connecting(_)
            | E::DnsResolve(_)
            | E::SendingRequest(_)
            | E::Handshake(_)
            | E::ReadingBody(_)
            | E::EmptyingBody(_) => {
                self.update(
                    err,
                    Backoff::fib(Duration::from_millis(200), MAX_RETY_INTERVAL),
                );
                CouldSucceed::Yes
            }
            E::StatusNotOk {
                code:
                    StatusCode::INTERNAL_SERVER_ERROR
                    | StatusCode::SERVICE_UNAVAILABLE
                    | StatusCode::GATEWAY_TIMEOUT,
                ..
            } => {
                self.update(err, Backoff::fib(Duration::from_secs(2), MAX_RETY_INTERVAL));
                CouldSucceed::Yes
            }

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
            | E::UrlWithoutHost => CouldSucceed::No(super::Error::HttpClient(err)),
        }
    }

    pub async fn ready(&mut self) {
        if let Some(until_next_try) = self.recent.iter_mut().map(Event::next_try_in).max() {
            debug!("retrying in {} ms", until_next_try.as_millis());
            tokio::time::sleep(until_next_try).await
        }
    }
}
