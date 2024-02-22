use std::io::{Read, Seek};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use stream_owl::testing::{test_data_range, Action, Event, ServerControls, TestEnded};
use stream_owl::{testing, StreamBuilder, StreamCanceld};
use testing::ConnControls;
use tokio::sync::Notify;
use tracing::info;

#[test]
fn after_seeking_forward_download_still_completes() {
    let test_dl_path = stream_owl::testing::gen_file_path();
    let configure = {
        let path = test_dl_path.clone();
        move |b: StreamBuilder<false, _, _, _>| b.with_prefetch(0).to_disk(path).start_paused(true)
    };

    let conn_controls = ConnControls::new(Vec::new());
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
    reader.seek(std::io::SeekFrom::Start(8_000)).unwrap();
    server_controls.unpause_all();

    handle.unpause_blocking();

    let test_ended = runtime_thread.join().unwrap();
    match test_ended {
        testing::TestEnded::StreamReturned(Ok(StreamCanceld)) => (),
        other => panic!("runtime should return with StreamReturned, it returned with {other:?}"),
    }

    std::mem::drop(handle); // triggers flush, could also call flush on handle
    let downloaded = std::fs::read(test_dl_path).unwrap();
    assert!(downloaded == testing::test_data(test_file_size as u32));
}

#[test]
fn resumes() {
    let test_dl_path = stream_owl::testing::gen_file_path();
    let configure = {
        let path = test_dl_path.clone();
        move |b: StreamBuilder<false, _, _, _>| {
            b.with_prefetch(0)
                .to_disk(path)
                .start_paused(true)
                .with_fixed_chunk_size(NonZeroUsize::new(1000).unwrap())
        }
    };

    {
        let conn_controls = ConnControls::new(Vec::new());
        let server_controls = ServerControls::new();
        server_controls.push(Event::Any, Action::Pause);
        server_controls.push(Event::ByteRequested(5_000), Action::Cut { at: 5_000 });

        let test_file_size = 5_000u32;
        let test_done = Arc::new(Notify::new());

        let (runtime_thread, mut handle) = {
            let server_controls = server_controls.clone();
            let conn_controls = conn_controls.clone();
            testing::setup_reader_test(&test_done, test_file_size, configure.clone(), move |size| {
                testing::pausable_server(size, server_controls, conn_controls)
            })
        };

        let mut reader = handle.try_get_reader().unwrap();
        reader.seek(std::io::SeekFrom::Start(2_000)).unwrap();
        handle.unpause_blocking();
        // seek is not instant, make sure the stream task has
        // processed it.
        thread::sleep(Duration::from_secs(1));
        server_controls.unpause_all();

        // when this reads the last byte (byte 5k in the stream). The
        // test server will crash, making sure we do not read further.
        reader.read_exact(&mut vec![0; 3_000]).unwrap();
        test_done.notify_one();

        let test_ended = runtime_thread.join().unwrap();
        assert_eq!(test_ended, TestEnded::TestDone);
    }
    assert_read_part_downloaded(test_dl_path);
    info!("Part 2 of test, checking if read uses made progress");

    let conn_controls = ConnControls::new(Vec::new());
    let server_controls = ServerControls::new();
    let test_file_size = 5_000u32;
    let test_done = Arc::new(Notify::new());
    let (runtime_thread, mut handle) = {
        let server_controls = server_controls.clone();
        let conn_controls = conn_controls.clone();
        testing::setup_reader_test(&test_done, test_file_size, configure, move |size| {
            testing::pausable_server(size, server_controls, conn_controls)
        })
    };

    // if we can read 3k at 2k without unpausing the stream
    // then we restored correctly
    let mut reader = handle.try_get_reader().unwrap();
    reader.seek(std::io::SeekFrom::Start(2_000)).unwrap();
    let mut buf = vec![0u8; 3_000];
    reader.read_exact(&mut buf).unwrap();

    assert_eq!(buf, testing::test_data_range(2_000..5_000));
    test_done.notify_one();

    let test_ended = runtime_thread.join().unwrap();
    assert_eq!(test_ended, TestEnded::TestDone);
}

fn assert_read_part_downloaded(test_dl_path: std::path::PathBuf) {
    let on_disk = std::fs::read(test_dl_path).unwrap();
    let downloaded = &on_disk[2_000..5_000];
    assert_eq!(downloaded, test_data_range(2_000..5_000));
}
