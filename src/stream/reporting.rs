use std::sync::Arc;

use tokio::task;
use tracing::debug;

pub use crate::store::range_watch::RangeUpdate;
use crate::{http_client, StreamCanceld};


#[derive(Debug, Clone)]
pub(crate) enum Report {
    Bandwidth(usize),
    Range(RangeUpdate),
    RetriedError(Arc<http_client::Error>),
}

pub(crate) type ReportTx = overflowable_mpsc::Sender<Report>;

pub async fn setup(
    mut report_rx: tokio::sync::mpsc::Receiver<Report>,
    bandwidth_callback: Option<Box<dyn FnMut(usize) + Send>>,
    range_callback: Option<Box<dyn crate::RangeCallback>>,
    retry_log_callback: Option<Box<dyn FnMut(Arc<http_client::Error>) + Send>>,
) -> Result<StreamCanceld, crate::StreamError> {
    let mut bandwidth_callback = bandwidth_callback.unwrap_or_else(|| Box::new(|_| {}));
    let mut range_callback = range_callback.unwrap_or_else(|| Box::new(|_| {}));
    let mut retry_log_callback = retry_log_callback.unwrap_or_else(|| Box::new(|_| {}));

    let res = task::spawn_blocking(move || loop {
        let Some(report) = report_rx.blocking_recv() else {
            debug!("report_tx dropped, stream must be canceld");
            return;
        };
        if let Report::Range(RangeUpdate::StreamClosed) = report {
            break;
        }

        match report {
            Report::Bandwidth(update) => (bandwidth_callback)(update),
            Report::Range(update) => (range_callback)(update),
            Report::RetriedError(update) => (retry_log_callback)(update),
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
