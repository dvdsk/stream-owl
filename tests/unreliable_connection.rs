use http::Uri;
use std::io::Read;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use stream_owl::testing::{ServerControls, TestEnded};
use stream_owl::{testing, StreamBuilder, StreamDone};
use testing::ConnControls;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::Notify;
use tokio::task::{self, JoinHandle};
use tracing::info;

#[test]
fn conn_drops_spaced_out() {
    let configure = { move |b: StreamBuilder<false>| b.with_prefetch(0).to_unlimited_mem() };

    let disconn_at = vec![3000, 5000, 8000];
    let conn_controls = ConnControls::new(disconn_at);
    let server_controls = ServerControls::new();
    let test_file_size = 10_000u32;
    let test_done = Arc::new(Notify::new());

    let (runtime_thread, mut handle) = {
        let server_controls = server_controls.clone();
        let conn_controls = conn_controls.clone();
        testing::setup_reader_test(&test_done, test_file_size, configure, move |size| {
            testing::pausable_server(size, server_controls, conn_controls)
        })
    };

    let mut reader = handle.try_get_reader().unwrap();
    reader
        .read_exact(&mut vec![0; test_file_size as usize])
        .unwrap();
    test_done.notify_one();

    use StreamDone::DownloadedAll;
    use TestEnded::{StreamReturned, TestDone};
    let test_ended = runtime_thread.join().unwrap();
    match test_ended {
        TestDone | StreamReturned(Ok(DownloadedAll)) => (),
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

async fn http200only_loop(listener: tokio::net::TcpListener) -> Result<(), std::io::Error> {
    while let Ok((mut stream, _)) = listener.accept().await {
        info!("bad server got conn");
        task::spawn(async move {
            let buf_reader = BufReader::new(&mut stream);
            let mut lines = buf_reader.lines();
            let mut http_request = Vec::new();
            while let Ok(Some(line)) = lines.next_line().await {
                http_request.push(line);
            }
            info!("bad server got: {http_request:?}");

            let response = "HTTP/1.1 200 OK\r\n\r\n";
            stream.write_all(response.as_bytes()).await.unwrap();
        });
    }
    unreachable!()
}

async fn server_hangs_loop(listener: tokio::net::TcpListener) -> Result<(), std::io::Error> {
    while let Ok((mut stream, _)) = listener.accept().await {
        info!("bad server got conn, will sleep on it forever");
        task::spawn(async move {
            let buf_reader = BufReader::new(&mut stream);
            tokio::time::sleep(Duration::from_secs(9999)).await;
        });
    }
    unreachable!()
}

#[test]
fn server_hangs() {
    let configure = { move |b: StreamBuilder<false>| b.with_prefetch(0).to_unlimited_mem() };

    let test_file_size = 10_000u32;
    let test_done = Arc::new(Notify::new());

    bad_server!(server_hangs_loop);
    let (runtime_thread, mut handle) = {
        testing::setup_reader_test(&test_done, test_file_size, configure, move |_| bad_server())
    };

    let mut reader = handle.try_get_reader().unwrap();
    reader
        .read_exact(&mut vec![0; test_file_size as usize])
        .unwrap();
    test_done.notify_one();

    use StreamDone::DownloadedAll;
    use TestEnded::{StreamReturned, TestDone};
    let test_ended = runtime_thread.join().unwrap();
    match test_ended {
        TestDone | StreamReturned(Ok(DownloadedAll)) => (),
        other => panic!("runtime should return with TestDone, it returned with {other:?}"),
    }
}
