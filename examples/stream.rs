#![recursion_limit = "256"]

use std::error::Error;
use std::io::Read;
use std::num::NonZeroUsize;
use std::sync::Arc;

use stream_owl::StreamBuilder;
use stream_owl_test_support::tracing_setup;
use stream_owl_test_support::{setup_reader_test, static_file_server};
use tokio::sync::Notify;

fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let _guard = tracing_setup::opentelemetry();

    let configure = {
        move |b: StreamBuilder<false, _, _, _>| {
            b.with_prefetch(0)
                .to_unlimited_mem()
                .with_fixed_chunk_size(NonZeroUsize::new(10_000).unwrap())
        }
    };

    let test_done = Arc::new(Notify::new());
    let (_runtime_thread, mut handle) = {
        setup_reader_test(&test_done, 100_000, configure, move |size| {
            static_file_server(size)
        })
    };

    let mut reader = handle.try_get_reader()?;
    reader.read_exact(&mut vec![0; 100_000])?;
    test_done.notify_one();

    Ok(())
}
