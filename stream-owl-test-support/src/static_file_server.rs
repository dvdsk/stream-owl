use std::future::IntoFuture;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Mutex;

use axum::routing::get_service;
use axum::Router;
use http::Uri;
use tokio::task::JoinHandle;
use tower_http::services::ServeFile;
use tower_http::trace::TraceLayer;

use crate::test_data;

fn gen_file_if_not_there(len: u64) -> PathBuf {
    static PATH: Mutex<Option<PathBuf>> = Mutex::new(None);
    let mut path = PATH.lock().unwrap();

    if let Some(ref path) = *path {
        if path.metadata().unwrap().len() == len {
            return path.clone();
        }
    }

    let mut new_path = std::env::temp_dir();
    new_path.push("stream_owl_test_source_.data");

    if new_path.is_file() {
        if new_path.metadata().unwrap().len() == len {
            return new_path;
        }
    }

    std::fs::write(&new_path, test_data(len as u32)).unwrap();
    *path = Some(new_path.clone());
    new_path
}

/// # Panics
/// Must be run within a tokio runtime, if it does not this fn will panic
pub fn static_file_server(test_file_size: u64) -> (Uri, JoinHandle<Result<(), std::io::Error>>) {
    let test_data_path = gen_file_if_not_there(test_file_size);
    let serve_file = ServeFile::new(test_data_path);
    let app = Router::new().route("/stream_test", get_service(serve_file));

    let addr = SocketAddr::from(([127, 0, 0, 1], 0));
    // fn this can not be async since then we can not pass this function
    // to setup_reader_test (impl trait forbidden in fn trait return type)
    // therefore we jump through from_std
    let listener = std::net::TcpListener::bind(addr).unwrap();
    listener.set_nonblocking(true).unwrap();
    let listener = tokio::net::TcpListener::from_std(listener).unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = axum::serve(listener, app.layer(TraceLayer::new_for_http()));
    let server = tokio::task::spawn(server.into_future());

    let uri: Uri = format!("http://localhost:{port}/stream_test")
        .parse()
        .unwrap();

    tracing::debug!("testserver listening on on {}", uri);
    (uri, server)
}
