use std::io::{Read, Seek};
use std::num::NonZeroUsize;
use std::sync::Arc;

use stream_owl::testing::{Action, ConnControls, Event, ServerControls};
use stream_owl::{testing, StreamBuilder};
use testing::TestEnded;
use tokio::sync::Notify;

#[test]
fn migrate_to_disk() {
    testing::setup_tracing();

    let test_dl_path = stream_owl::testing::gen_file_path();
    let configure = |b: StreamBuilder<false>| {
        b.with_prefetch(0)
            .to_unlimited_mem()
            .with_fixed_chunk_size(NonZeroUsize::new(1000).unwrap())
            .start_paused(true)
    };

    let conn_controls = ConnControls::new(Vec::new());
    let server_controls = ServerControls::new();
    server_controls.push(Event::ByteRequested(3000), Action::Cut { at: 3000 });

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
    reader.seek(std::io::SeekFrom::Start(1_000)).unwrap();
    handle.unpause_blocking();
    server_controls.push(Event::ByteRequested(0), Action::Pause);
    server_controls.unpause_all();
    reader.read_exact(&mut vec![0; 1_000]).unwrap();

    handle
        .migrate_to_disk_backend_blocking(test_dl_path.clone())
        .unwrap();
    reader.read_exact(&mut vec![0; 1_000]).unwrap();

    reader.seek(std::io::SeekFrom::Start(1_000)).unwrap();
    reader.read_exact(&mut vec![0; 2_000]).unwrap();

    test_done.notify_one();
    runtime_thread.join().unwrap().assert_no_errors();
    handle.pause_blocking();
    handle.flush_blocking().unwrap();

    let downloaded = std::fs::read(test_dl_path).unwrap();
    let correct = {
        let mut test_data = testing::test_data(downloaded.len() as u32);
        test_data[0..1000].copy_from_slice(&[0; 1000]);
        test_data
    };
    assert_eq_arrays(&downloaded, &correct)
}

fn assert_eq_arrays(downloaded: &[u8], correct: &[u8]) {
    if downloaded == correct {
        return;
    }

    for (i, (d, c)) in downloaded
        .into_iter()
        .copied()
        .zip(correct.into_iter().copied())
        .enumerate()
    {
        if d != c {
            const C: usize = 5;
            let start = i.saturating_sub(C);
            let end = (i + C).min(downloaded.len());
            panic!(
                "downloaded not equal to correct at index: {i}
                   context: 
                   \tdownloaded[{start}..{end}] = {:?}
                   \tcorrect[{start}..{end}]    = {:?}",
                &downloaded[start..end],
                &correct[start..end]
            );
        }
    }
}

#[ignore = "todo"]
#[test]
fn completed_stream_migrated_to_lim_store_streams_again() {}

#[test]
fn file_is_deleted_when_migrating_from_disk_store() {
    testing::setup_tracing();

    let test_dl_path = stream_owl::testing::gen_file_path();
    let configure = {
        let path = test_dl_path.clone();
        move |b: StreamBuilder<false>| b.with_prefetch(0).to_disk(path)
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
    reader
        .read_exact(&mut vec![0; test_file_size as usize])
        .unwrap();
    // at this point everything must be downloaded

    handle.migrate_to_unlimited_mem_backend_blocking().unwrap();

    test_done.notify_one();
    assert_eq!(TestEnded::TestDone, runtime_thread.join().unwrap());

    assert!(!test_dl_path.exists());
}
