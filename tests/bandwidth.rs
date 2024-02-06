use std::io::Read;
use std::sync::Arc;
use std::time::{Duration, Instant};

use stream_owl::testing::TestEnded;
use stream_owl::{testing, BandwidthLimit, StreamBuilder, StreamDone};
use tokio::sync::Notify;

#[test]
fn stream_not_faster_then_limit() {
    testing::setup_tracing();
    let configure = {
        move |b: StreamBuilder<false>| {
            b.with_prefetch(0)
                .to_unlimited_mem()
                .with_bandwidth_limit(BandwidthLimit::kbytes(20).unwrap())
        }
    };

    let test_file_size = 100_000u32;
    let test_done = Arc::new(Notify::new());

    let start = Instant::now();
    let (runtime_thread, mut handle) = {
        testing::setup_reader_test(&test_done, test_file_size, configure, move |size| {
            testing::static_file_server(size)
        })
    };

    let mut reader = handle.try_get_reader().unwrap();
    reader.read_exact(&mut vec![0; 100_000]).unwrap();

    assert!(
        start.elapsed() > Duration::from_millis(40),
        "elapsed: {:?}",
        start.elapsed()
    );

    test_done.notify_one();

    std::mem::drop(handle);
    std::mem::drop(reader);
    let test_ended = runtime_thread.join().unwrap();
    assert!(matches!(
        test_ended,
        TestEnded::StreamReturned(Ok(StreamDone::DownloadedAll))
    ));
}

#[test]
#[ignore = "not yet implemented"]
fn stream_speeds_up_if_limit_increased() {
    todo!()
}
