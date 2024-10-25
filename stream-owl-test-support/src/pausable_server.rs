use std::io;
use std::net::SocketAddr;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use axum::body::Body;
use axum::extract::State;
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use http::{StatusCode, Uri};
use hyper::body::Incoming;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tower::Service;
use tower_http::trace::TraceLayer;
use tracing::{instrument, error};

use crate::test_data;

mod conn;
pub use conn::{ConnControls, TestConn};

struct ControllableServer {
    test_data: Vec<u8>,
    pause_controls: ServerControls,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Any,
    ByteRequested(u64),
}

impl Event {
    fn active(&self, range: &Range<u64>) -> bool {
        match self {
            Event::ByteRequested(n) => range.contains(n),
            Event::Any => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Action {
    Cut { at: u64 },
    Crash,
    Pause,
    Drop,
}

struct DropRequest;
impl Action {
    async fn perform(
        &self,
        state: &ControllableServer,
        range: &mut Range<u64>,
    ) -> Result<(), DropRequest> {
        match self {
            Action::Crash => {
                panic!("Crash requested from test server")
            }
            Action::Cut { at } => range.end = *at,
            Action::Pause => {
                tracing::warn!("Test server waiting to be unpaused");
                state.pause_controls.notify.notified().await;
            }
            Action::Drop => return Err(DropRequest),
        }

        Ok(())
    }
}

#[derive(Debug)]
struct InnerControls {
    on_event: Vec<(Event, Action)>,
}

#[derive(Debug, Clone)]
pub struct ServerControls {
    inner: Arc<Mutex<InnerControls>>,
    notify: Arc<Notify>,
}

impl ServerControls {
    pub fn new() -> Self {
        let inner = InnerControls {
            on_event: Vec::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub fn push(&self, event: Event, action: Action) {
        self.inner.lock().unwrap().on_event.push((event, action))
    }

    pub fn push_front(&self, event: Event, action: Action) {
        self.inner
            .lock()
            .unwrap()
            .on_event
            .insert(0, (event, action))
    }

    /// unpauses and remove any future plain pauses
    pub fn unpause_all(&self) {
        tracing::warn!("Unpausing debug server");
        let mut inner = self.inner.lock().unwrap();
        let to_remove: Vec<_> = inner
            .on_event
            .iter()
            .enumerate()
            .filter(|(_, (event, action))| {
                (*action == Action::Pause || *action == Action::Drop) && *event == Event::Any
            })
            .map(|(idx, _)| idx)
            .collect();
        for to_remove in to_remove.into_iter().rev() {
            inner.on_event.remove(to_remove);
        }

        self.notify.notify_one();
    }
}

use axum_macros::debug_handler;
#[debug_handler]
#[instrument(level = "debug", skip(state))]
async fn handler(
    State(state): State<Arc<ControllableServer>>,
    headers: http::HeaderMap,
) -> Response<Body> {
    let range = headers.get("Range").unwrap();
    let range = range
        .to_str()
        .unwrap()
        .strip_prefix("bytes=")
        .unwrap()
        .split_once("-")
        .unwrap();
    let start: u64 = range.0.parse().unwrap();
    let stop: u64 = range.1.parse().unwrap();

    let actions: Vec<_> = {
        let range = start..stop;
        let inner = state.pause_controls.inner.lock().unwrap();
        inner
            .on_event
            .iter()
            .filter(|(event, _)| event.active(&range))
            .map(|(_, action)| action)
            .cloned()
            .collect()
    };

    let mut range = start..stop;
    for action in actions {
        if let Err(DropRequest) = action.perform(&state, &mut range).await {
            return Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::empty())
                .unwrap();
        }
    }

    let start = start.min(state.test_data.len() as u64);
    let stop = stop.min(state.test_data.len() as u64);
    let data = state.test_data[start as usize..stop as usize].to_owned();
    let total = state.test_data.len();

    Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header("Content-Range", format!("bytes {start}-{stop}/{total}"))
        .header("Accept-Ranges", "bytes")
        .body(Body::from(data))
        .unwrap()
}

/// # Panics
/// Must be run within a tokio runtime, if it does not this fn will panic
pub fn pausable_server(
    test_file_size: u64,
    pause_controls: ServerControls,
    conn_controls: ConnControls,
) -> (Uri, JoinHandle<Result<(), std::io::Error>>) {
    let shared_state = Arc::new(ControllableServer {
        test_data: test_data(test_file_size as u32),
        pause_controls,
    });

    let app = Router::new()
        .route("/stream_test", get(handler))
        .with_state(Arc::clone(&shared_state))
        .layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    // fn this can not be async since then we can not pass this function
    // to setup_reader_test (impl trait forbidden in fn trait return type)
    // therefore we jump through from_std
    let listener = std::net::TcpListener::bind(addr).unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = sever_loop(app, listener, conn_controls);
    let server = tokio::task::spawn(server);

    let uri: Uri = format!("http://localhost:{port}/stream_test")
        .parse()
        .unwrap();

    tracing::debug!("testserver listening on on {}", uri);
    (uri, server)
}

async fn sever_loop(
    app: Router,
    listener: tokio::net::TcpListener,
    conn_controls: ConnControls,
) -> Result<(), io::Error> {
    use hyper_util::rt::TokioIo;
    let mut make_service = app.into_make_service();
    loop {
        let conn_controls = conn_controls.clone();
        let (socket, _addr) = listener.accept().await.unwrap();
        let tower_service = make_service.call(&socket).await.unwrap();

        tokio::spawn(async move {
            let hyper_service =
                hyper::service::service_fn(move |request: axum::http::Request<Incoming>| {
                    tower_service.clone().call(request)
                });

            let socket = TestConn::new(socket, conn_controls);
            let socket = TokioIo::new(socket);
            let res = hyper::server::conn::http1::Builder::new()
                .serve_connection(socket, hyper_service)
                .await;

            if let Err(ref e) = res {
                if !format!("{e:?}").contains("Test server forced a disconnect") {
                    error!("Test server ran into a critical error: {e:?}")
                }
            }
        });
    }
}
