use std::time::Duration;

use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::filter;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::time::uptime;
use tracing_subscriber::prelude::*;

use opentelemetry::trace::TracerProvider;
use opentelemetry_sdk::runtime;

pub fn basic() {
    let filter = filter::EnvFilter::builder()
        .with_regex(true)
        .try_from_env()
        .unwrap_or_else(|_| {
            filter::EnvFilter::builder()
                .parse("stream_owl=debug,tower=info,info")
                .unwrap()
        });

    let fmt = fmt::layer()
        .with_timer(uptime())
        .pretty()
        .with_line_number(true)
        .with_test_writer();
    let fmt = fmt.with_filter(filter);

    // let console_layer = console_subscriber::spawn();
    let _ignore_err = tracing_subscriber::registry()
        // .with(console_layer)
        .with(fmt)
        .try_init();
}

/// does cleanup when dropped
pub struct OpenTelemetryGuard;

impl Drop for OpenTelemetryGuard {
    fn drop(&mut self) {
        opentelemetry::global::shutdown_tracer_provider();
    }
}

#[must_use]
pub fn opentelemetry() -> OpenTelemetryGuard {
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_endpoint("http://localhost:4317")
        .with_timeout(Duration::from_secs(3));

    let tracer = opentelemetry_otlp::new_pipeline()
        .tracing()
        .with_exporter(exporter)
        .install_batch(runtime::Tokio)
        .unwrap()
        .tracer("trace demo");
    let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let filter = filter::EnvFilter::builder()
        .with_regex(true)
        .try_from_env()
        .unwrap_or_else(|_| {
            filter::EnvFilter::builder()
                .parse("stream_owl=debug,tower=info,info")
                .unwrap()
        });

    let fmt = fmt::layer()
        .with_timer(uptime())
        .pretty()
        .with_line_number(true)
        .with_test_writer();
    let fmt = fmt.with_filter(filter);

    // let console_layer = console_subscriber::spawn();
    let _ignore_err = tracing_subscriber::registry()
        .with(otel_layer)
        .with(fmt)
        .try_init();

    OpenTelemetryGuard
}
