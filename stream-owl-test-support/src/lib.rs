use std::ops::Range;
use std::path::PathBuf;
use std::sync::{mpsc, Arc};
use std::thread;

use futures::FutureExt;
use futures_concurrency::future::Race;
use tokio::runtime::Runtime;
use tokio::sync::Notify;
use tokio::task::{JoinError, JoinHandle};

use stream_owl::{
    BandwidthCallback, LogCallback, RangeCallback, StreamBuilder, StreamCanceld,
    StreamError, StreamHandle, UnconfiguredSB,
};

mod pausable_server;
pub use pausable_server::{pausable_server, Action, ConnControls, Event, ServerControls};

mod static_file_server;
pub use static_file_server::static_file_server;

pub mod tracing_setup;

pub fn gen_file_path() -> PathBuf {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    let mut rng = thread_rng();
    let mut name = "stream_owl_test_download_".to_owned();
    name.extend((0..8).map(|_| rng.sample(Alphanumeric) as char));

    let mut dir = std::env::temp_dir();
    dir.push(name);
    dir
}

pub fn test_data_range(range: Range<u32>) -> Vec<u8> {
    range
        .into_iter()
        .step_by(4)
        .flat_map(|n| n.to_ne_bytes())
        .collect()
}

pub fn test_data(bytes: u32) -> Vec<u8> {
    test_data_range(0..bytes)
}

pub fn setup_reader_test<L, B, R>(
    test_done: &Arc<Notify>,
    test_file_size: u32,
    configure: impl FnOnce(UnconfiguredSB) -> StreamBuilder<true, L, B, R> + Send + 'static,
    server: impl FnOnce(u64) -> (http::Uri, JoinHandle<Result<(), std::io::Error>>) + Send + 'static,
) -> (thread::JoinHandle<TestEnded>, StreamHandle<R>)
where
    L: LogCallback,
    B: BandwidthCallback,
    R: RangeCallback,
{
    let (runtime_thread, handle) = {
        let test_done = test_done.clone();
        let (tx, rx) = mpsc::channel();
        let runtime_thread = thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let (uri, server) = server(test_file_size as u64);

                let builder = StreamBuilder::new(uri);
                let (handle, stream) = configure(builder).start().await.unwrap();
                tx.send(handle).unwrap();

                let server = server.map(TestEnded::ServerCrashed);
                let stream = stream.map(TestEnded::StreamReturned);
                let done = wait_for_test_done(test_done);
                (server, stream, done).race().await
            })
        });
        let handle = rx.recv().unwrap();
        (runtime_thread, handle)
    };
    (runtime_thread, handle)
}

#[derive(Debug)]
pub enum TestEnded {
    ServerCrashed(Result<Result<(), std::io::Error>, JoinError>),
    StreamReturned(Result<StreamCanceld, StreamError>),
    TestDone,
}

impl TestEnded {
    pub fn assert_no_errors(&self) {
        match self {
            TestEnded::ServerCrashed(_) => panic!("test server crashed"),
            TestEnded::StreamReturned(Err(e)) => panic!("stream returned with error: {e}"),
            _ => (),
        }
    }
}

impl PartialEq for TestEnded {
    fn eq(&self, other: &Self) -> bool {
        use TestEnded as T;
        match (self, other) {
            (T::ServerCrashed(res1), T::ServerCrashed(res2)) => {
                format!("{:?}", res1) == format!("{:?}", res2)
            }
            (T::StreamReturned(res1), T::StreamReturned(res2)) => {
                format!("{:?}", res1) == format!("{:?}", res2)
            }
            (T::TestDone, T::TestDone) => true,
            _ => false,
        }
    }
}

async fn wait_for_test_done(test_done: Arc<Notify>) -> TestEnded {
    test_done.notified().await;
    tracing::info!("Test done");
    TestEnded::TestDone
}


