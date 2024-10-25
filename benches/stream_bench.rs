use criterion::{criterion_group, criterion_main, Criterion};
use stream_owl_test_support::{setup_reader_test, static_file_server};

use std::io::Read;
use std::sync::Arc;

use stream_owl::StreamBuilder;
use tokio::sync::Notify;

fn stream(stream_size: u32) {
    let configure =
        { move |b: StreamBuilder<false, _, _, _>| b.with_prefetch(0).to_unlimited_mem() };

    let test_done = Arc::new(Notify::new());
    let (_runtime_thread, mut handle) = {
        setup_reader_test(&test_done, stream_size, configure, move |size| {
            static_file_server(size)
        })
    };

    let mut reader = handle.try_get_reader().unwrap();
    reader.read_exact(&mut vec![0; 100_000]).unwrap();
    test_done.notify_one();
}

fn stream_bench(c: &mut Criterion) {
    c.bench_function("stream 100k", |b| b.iter(|| stream(100_000)));
    // c.bench_function("stream 10k", |b| b.iter(|| stream(10_000)));
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = stream_bench
}
criterion_main!(benches);
