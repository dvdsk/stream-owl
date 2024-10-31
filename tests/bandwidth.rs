#![recursion_limit = "200"]

use std::io::Read;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::{Duration, Instant};

use stream_owl::{BandwidthLimit, StreamBuilder};
use stream_owl_test_support::{setup_reader_test, static_file_server};
use tokio::sync::Notify;
use tracing::info;

#[test]
fn bw_stream_not_faster_then_limit() {
    let configure = {
        move |b: StreamBuilder<false, _, _, _>| {
            b.with_prefetch(0)
                .to_unlimited_mem()
                .with_fixed_chunk_size(NonZeroUsize::new(100_000).unwrap())
                .with_bandwidth_limit(BandwidthLimit::kbytes(20).unwrap())
        }
    };

    let test_file_size = 100_000u32;
    let test_done = Arc::new(Notify::new());

    let start = Instant::now();
    let (runtime_thread, mut handle) = {
        setup_reader_test(&test_done, test_file_size, configure, move |size| {
            static_file_server(size)
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
    runtime_thread.join().unwrap().assert_no_errors();
}

fn test_run(spd_limit: u32) -> Duration {
    let configure = {
        move |b: StreamBuilder<false, _, _, _>| {
            b.with_prefetch(0)
                .to_unlimited_mem()
                .with_fixed_chunk_size(NonZeroUsize::new(100_000).unwrap())
                .with_bandwidth_limit(BandwidthLimit::kbytes(spd_limit).unwrap())
        }
    };

    let test_file_size = 500_000u32;
    let test_done = Arc::new(Notify::new());

    let (runtime_thread, mut handle) = {
        setup_reader_test(&test_done, test_file_size, configure, move |size| {
            static_file_server(size)
        })
    };

    // warmup
    dbg!(spd_limit);
    let mut reader = handle.try_get_reader().unwrap();
    reader.read_exact(&mut vec![0; 100_000]).unwrap();

    dbg!(spd_limit);
    let start = Instant::now();
    reader.read_exact(&mut vec![0; 400_000]).unwrap();
    let elapsed = start.elapsed();

    dbg!(spd_limit);
    test_done.notify_one();

    dbg!(spd_limit);
    std::mem::drop(handle);
    std::mem::drop(reader);
    dbg!(spd_limit);
    runtime_thread.join().unwrap().assert_no_errors();
    elapsed
}

#[test]
fn bw_higher_limit_faster_speed() {
    stream_owl_test_support::tracing_setup::basic();
    let factor = 3f32;
    let base = 100;
    let high = ((base as f32) * factor) as u32;
    let slow_spd = test_run(base);
    info!("done with slow test");
    let high_spd = test_run(high);

    assert!(
        slow_spd > high_spd.mul_f32(factor * 0.9),
        "high speed should be around {factor} as fast as slow. Instead they took: slow_spd: {slow_spd:?}, high_spd: {high_spd:?}"
    );
}

#[ignore = "not yet implemented"]
#[test]
fn increasing_limit_leads_to_speedup() {
    todo!("use bandwidth monitor")
}
