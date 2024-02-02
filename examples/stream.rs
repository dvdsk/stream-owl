use std::error::Error;
use std::io::Read;
use std::sync::Arc;

use stream_owl::{testing, StreamBuilder};
use tokio::sync::Notify;

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name("report_example")
        .install_simple()?;
        // .install_batch(opentelemetry_sdk::runtime::Tokio)?;
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(opentelemetry)
        .try_init()?;

    let configure = { move |b: StreamBuilder<false>| b.with_prefetch(0).to_unlimited_mem() };

    let test_done = Arc::new(Notify::new());
    let (_runtime_thread, mut handle) = {
        testing::setup_reader_test(&test_done, 100_000, configure, move |size| {
            testing::static_file_server(size)
        })
    };

    let mut reader = handle.try_get_reader()?;
    reader.read_exact(&mut vec![0; 100_000])?;
    test_done.notify_one();

    opentelemetry::global::shutdown_tracer_provider();
    Ok(())
}
