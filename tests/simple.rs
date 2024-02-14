use std::io::Read;
use std::io::Seek;
use std::num::NonZeroUsize;
use std::sync::Arc;

use stream_owl::testing;
use stream_owl::Reader;
use stream_owl::StreamBuilder;
use tokio::sync::Notify;
use tracing::info;
use tracing::instrument;

#[cfg(test)]
mod xlimited_memory {
    use super::*;

    #[test]
    fn seek_from_all_sides_works() {
        let configure = |b: StreamBuilder<false>| {
            b.with_prefetch(0)
                .to_limited_mem(NonZeroUsize::new(1000).unwrap())
        };
        seek_test(configure);
    }
}

#[cfg(test)]
mod unlimited_memory {
    use super::*;

    #[test]
    fn seek_from_all_sides_works() {
        let configure = |b: StreamBuilder<false>| b.with_prefetch(0).to_unlimited_mem();
        seek_test(configure);
    }
}

#[cfg(test)]
mod disk {
    use super::*;

    #[test]
    fn seek_from_all_sides_works() {
        let configure = |b: StreamBuilder<false>| {
            let path = stream_owl::testing::gen_file_path();
            b.with_prefetch(0).to_disk(path)
        };
        seek_test(configure);
    }
}

fn seek_test(configure: fn(StreamBuilder<false>) -> StreamBuilder<true>) {
    let test_file_size = 10_000u32;
    let test_done = Arc::new(Notify::new());
    let (runtime_thread, mut handle) = testing::setup_reader_test(
        &test_done,
        test_file_size,
        configure,
        testing::static_file_server,
    );

    let mut reader = handle.try_get_reader().unwrap();
    assert_pos(&mut reader, 0);
    reader.seek(std::io::SeekFrom::Start(40)).unwrap();
    assert_pos(&mut reader, 40);
    // note reading 4 bytes here shift curr poss by 4
    reader.seek(std::io::SeekFrom::Current(36)).unwrap();
    assert_pos(&mut reader, 80);
    reader.seek(std::io::SeekFrom::End(40)).unwrap();
    assert_pos(&mut reader, test_file_size - 40);
    info!("done testing");
    test_done.notify_one();

    runtime_thread.join().unwrap().assert_no_errors()
}

#[instrument(skip(reader))]
fn assert_pos(reader: &mut Reader, bytes_from_start: u32) {
    let mut numb_buf = [0, 0, 0, 0];
    info!("preparing read");
    reader.read_exact(&mut numb_buf).unwrap();
    info!("done with read");
    let numb = u32::from_ne_bytes(numb_buf);
    let correct = bytes_from_start;
    assert_eq!(
        numb, correct,
        "expected: {correct} got {numb} at {bytes_from_start} bytes from start.\nRaw bytes: {numb_buf:?}"
    );
}
