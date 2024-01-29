use std::io::Read;
use std::sync::Arc;
use stream_owl::testing::{ServerControls, TestEnded};
use stream_owl::{testing, StreamBuilder, StreamDone};
use testing::ConnControls;
use tokio::sync::Notify;

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

#[ignore = "not yet implemented"]
#[test]
fn conn_drops_closely_spaced() {
    todo!()
}
