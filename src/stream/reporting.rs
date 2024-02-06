use std::sync::Arc;

use tokio::task;

pub use crate::store::range_watch::RangeUpdate;
use crate::{http_client, StreamDone};

pub(crate) enum Report {
    Bandwidth(usize),
    Range(RangeUpdate),
    RetriedError(Arc<http_client::Error>),
}

pub(crate) type ReportTx = std::sync::mpsc::Sender<Report>;

pub async fn setup(
    report_rx: std::sync::mpsc::Receiver<Report>,
    bandwidth_callback: Option<Box<dyn FnMut(usize) + Send>>,
    range_callback: Option<Box<dyn FnMut(RangeUpdate) + Send>>,
    retry_log_callback: Option<Box<dyn FnMut(Arc<http_client::Error>) + Send>>,
) -> Result<StreamDone, crate::StreamError> {
    let mut bandwidth_callback = bandwidth_callback.unwrap_or_else(|| Box::new(|_| {}));
    let mut range_callback = range_callback.unwrap_or_else(|| Box::new(|_| {}));
    let mut retry_log_callback = retry_log_callback.unwrap_or_else(|| Box::new(|_| {}));

    let res = task::spawn_blocking(move || {
        for report in report_rx.iter() {
            if let Report::Range(RangeUpdate::StreamClosed) = report {
                break;
            }

            match report {
                Report::Bandwidth(update) => (bandwidth_callback)(update),
                Report::Range(update) => (range_callback)(update),
                Report::RetriedError(update) => (retry_log_callback)(update),
            }
        }
    })
    .await;

    match res {
        Err(join_error) if join_error.is_panic() => {
            let panic = join_error.into_panic();
            Err(crate::StreamError::UserCallbackPanicked(panic))
        }
        _ => {
            std::future::pending::<()>().await;
            unreachable!("reporting loop never returns unless an error happens")
        }
    }
}
