#![recursion_limit = "256"]

use std::io::{Read, Seek};
use std::num::NonZeroUsize;
use std::sync::Arc;

use stream_owl::StreamBuilder;
use stream_owl_test_support::{gen_file_path, pausable_server, setup_reader_test};
use stream_owl_test_support::{Action, ConnControls, Event, ServerControls, TestEnded};
use tokio::sync::Notify;

#[test]
fn migrate_to_disk() {
    let test_dl_path = gen_file_path();
    let configure = |b: StreamBuilder<false, _, _, _>| {
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
        setup_reader_test(&test_done, test_file_size, configure, move |size| {
            pausable_server(size, server_controls, conn_controls)
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

    handle.pause_blocking();
    handle.flush_blocking().unwrap();

    test_done.notify_one();
    runtime_thread.join().unwrap().assert_no_errors();

    // verify that the first 1000 bytes have not been downloaded
    // while the part read before migration has and the part post
    // migration has too
    let downloaded = std::fs::read(test_dl_path).unwrap();
    let correct = {
        let mut test_data = stream_owl_test_support::test_data(3000);
        test_data[0..1000].copy_from_slice(&[0; 1000]);
        test_data
    };
    assert_eq_arrays(&downloaded[0..3000], &correct)
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

#[test]
fn unlimited_migrated_to_lim_store_streams_again() {
    let configure =
        { move |b: StreamBuilder<false, _, _, _>| b.with_prefetch(0).to_unlimited_mem() };

    let conn_controls = ConnControls::new(Vec::new());
    let server_controls = ServerControls::new();
    let test_file_size = 10_000u32;
    let test_done = Arc::new(Notify::new());

    let (runtime_thread, mut handle) = {
        let server_controls = server_controls.clone();
        let conn_controls = conn_controls.clone();
        setup_reader_test(&test_done, test_file_size, configure, move |size| {
            pausable_server(size, server_controls, conn_controls)
        })
    };

    let mut reader = handle.try_get_reader().unwrap();
    reader
        .read_exact(&mut vec![0; test_file_size as usize])
        .unwrap();
    // at this point everything must be downloaded

    handle
        .migrate_to_limited_mem_backend_bocking((test_file_size / 10) as usize)
        .unwrap();
    reader
        .seek(std::io::SeekFrom::Start((test_file_size / 2) as u64))
        .unwrap();
    reader.read_exact(&mut vec![0; 1_000 as usize]).unwrap();
    reader.seek(std::io::SeekFrom::Start(0)).unwrap();
    reader.read_exact(&mut vec![0; 1_000 as usize]).unwrap();

    test_done.notify_one();
    assert_eq!(TestEnded::TestDone, runtime_thread.join().unwrap());
}

#[test]
fn file_is_deleted_when_migrating_from_disk_store() {
    let test_dl_path = gen_file_path();
    let configure = {
        let path = test_dl_path.clone();
        move |b: StreamBuilder<false, _, _, _>| b.with_prefetch(0).to_disk(path)
    };

    let conn_controls = ConnControls::new(Vec::new());
    let server_controls = ServerControls::new();
    let test_file_size = 10_000u32;
    let test_done = Arc::new(Notify::new());

    let (runtime_thread, mut handle) = {
        let server_controls = server_controls.clone();
        let conn_controls = conn_controls.clone();
        setup_reader_test(&test_done, test_file_size, configure, move |size| {
            pausable_server(size, server_controls, conn_controls)
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
