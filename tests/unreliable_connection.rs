#![recursion_limit = "256"]

use http::Uri;
use std::io::Read;
use std::net::SocketAddr;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;
use stream_owl::{http_client, StreamBuilder, StreamCanceld};
use stream_owl_test_support::{pausable_server, setup_reader_test, ConnControls};
use stream_owl_test_support::{ServerControls, TestEnded};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Notify;
use tokio::task::{self, JoinHandle};
use tracing::info;

#[test]
fn conn_drops_spaced_out() {
    let (retry_tx, retry_rx) = mpsc::channel();
    let configure = {
        move |b: StreamBuilder<false, _, _, _>| {
            b.with_prefetch(0)
                .to_unlimited_mem()
                .with_max_retries(3)
                .with_retry_callback(move |e| retry_tx.send(e).unwrap())
        }
    };

    let disconn_at = vec![3000, 5000, 8000];
    let conn_controls = ConnControls::new(disconn_at);
    let server_controls = ServerControls::new();
    let test_file_size = 10_000u32;
    let test_done = Arc::new(Notify::new());

    let (runtime_thread, mut handle) = {
        let server_controls = server_controls.clone();
        let conn_controls = conn_controls.clone();
        setup_reader_test(
            &test_done,
            test_file_size,
            configure,
            move |size| {
                pausable_server(size, server_controls, conn_controls)
            },
        )
    };

    let mut reader = handle.try_get_reader().unwrap();
    thread::spawn(move || {
        reader
            .read_exact(&mut vec![0; test_file_size as usize])
            .unwrap();
        test_done.notify_one();
    });

    use TestEnded::{StreamReturned, TestDone};
    let test_ended = runtime_thread.join().unwrap();
    let errors_retried: Vec<_> = retry_rx.iter().collect();

    assert!(errors_retried.len() == 3);
    assert!(errors_retried
        .iter()
        .all(|err| matches!(err.as_ref(), &http_client::Error::ReadingBody(_))));
    match test_ended {
        TestDone | StreamReturned(Ok(StreamCanceld)) => (),
        other => panic!("runtime should return with TestDone, it returned with {other:?}"),
    }
}

macro_rules! bad_server {
    ($server_loop:ident) => {
        fn bad_server() -> (Uri, JoinHandle<Result<(), std::io::Error>>) {
            let addr = SocketAddr::from(([127, 0, 0, 1], 0));
            // fn this can not be async since then we can not pass this function
            // to setup_reader_test (impl trait forbidden in fn trait return type)
            // therefore we jump through from_std
            let listener = std::net::TcpListener::bind(addr).unwrap();
            listener.set_nonblocking(true).unwrap();
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let port = listener.local_addr().unwrap().port();
            let server = $server_loop(listener);
            let server = task::spawn(server);
            let uri: Uri = format!("http://localhost:{port}/stream_test")
                .parse()
                .unwrap();
            tracing::debug!("(purposefully broken) testserver listening on on {}", uri);

            (uri, server)
        }
    };
}

async fn server_hangs_loop(listener: tokio::net::TcpListener) -> Result<(), std::io::Error> {
    while let Ok((mut stream, _)) = listener.accept().await {
        info!("bad server got conn, will sleep on it forever");
        task::spawn(async move {
            let _buf_reader = BufReader::new(&mut stream);
            tokio::time::sleep(Duration::from_secs(9999)).await;
        });
    }
    unreachable!()
}

#[test]
fn server_hangs() {
    let configure = {
        move |b: StreamBuilder<false, _, _, _>| {
            b.with_prefetch(0)
                .to_unlimited_mem()
                .with_max_retries(2)
                .with_timeout(Duration::from_millis(100))
        }
    };

    let test_file_size = 10_000u32;
    let test_done = Arc::new(Notify::new());

    bad_server!(server_hangs_loop);
    let (runtime_thread, mut handle) = {
        setup_reader_test(
            &test_done,
            test_file_size,
            configure,
            move |_| bad_server(),
        )
    };

    let mut reader = handle.try_get_reader().unwrap();
    thread::spawn(move || {
        reader
            .read_exact(&mut vec![0; test_file_size as usize])
            .unwrap();
        test_done.notify_one();
    });

    use stream_owl::http_client::error::SendingRequest::TimedOut;
    use stream_owl::http_client::Error::SendingRequest;
    use stream_owl::StreamError::HttpClient;
    use TestEnded::StreamReturned;
    let test_ended = runtime_thread.join().unwrap();
    match test_ended {
        StreamReturned(Err(HttpClient(SendingRequest(TimedOut)))) => (),
        other => panic!("runtime should return with TestDone, it returned with {other:?}"),
    }
}

async fn http200only_loop(listener: tokio::net::TcpListener) -> Result<(), std::io::Error> {
    while let Ok((mut stream, addr)) = listener.accept().await {
        info!("Testserver got connection from: {addr}");
        task::spawn(async move {
            info!("handling conn");
            'outer: loop {
                let buf_reader = BufReader::new(&mut stream);
                let mut lines = buf_reader.lines();
                let mut http_request = Vec::new();

                loop {
                    let Some(line) = lines.next_line().await.unwrap() else {
                        tracing::warn!("Connection was closed");
                        break 'outer;
                    };
                    if line.is_empty() {
                        break;
                    }
                    http_request.push(line);
                }

                let response = "HTTP/1.1 200 OK\r\n\r\n";
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.flush().await.unwrap();
                info!("bad server handled: {http_request:?}");
            }
        });
    }
    unreachable!()
}

#[test]
fn body_is_empty() {
    let configure = {
        move |b: StreamBuilder<false, _, _, _>| {
            b.with_prefetch(0)
                .to_unlimited_mem()
                .with_max_retries(2)
                .with_timeout(Duration::from_millis(200))
        }
    };

    let test_file_size = 10_000u32;
    let test_done = Arc::new(Notify::new());

    bad_server!(http200only_loop);
    let (runtime_thread, mut handle) = {
        setup_reader_test(
            &test_done,
            test_file_size,
            configure,
            move |_| bad_server(),
        )
    };

    let mut reader = handle.try_get_reader().unwrap();
    thread::spawn(move || {
        reader
            .read_exact(&mut vec![0; test_file_size as usize])
            .unwrap();
        test_done.notify_one();
    });

    use stream_owl::http_client::error::ReadingBody::TimedOut;
    use stream_owl::http_client::Error::ReadingBody;
    use stream_owl::StreamError::HttpClient;
    use TestEnded::StreamReturned;
    let test_ended = runtime_thread.join().unwrap();
    match test_ended {
        StreamReturned(Err(HttpClient(ReadingBody(TimedOut)))) => (),
        other => panic!("runtime should return with TestDone, it returned with {other:?}"),
    }
}
