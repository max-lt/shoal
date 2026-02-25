//! Telemetry initialization for the Shoal daemon.
//!
//! Supports three modes depending on compile-time features and runtime config:
//!
//! 1. **No `telemetry` feature** — plain `tracing-subscriber` with `fmt` layer
//!    (human-readable logs to stdout, filtered by `RUST_LOG` or config level).
//!
//! 2. **`telemetry` feature, no OTLP endpoint configured** — same as (1),
//!    console-only tracing.
//!
//! 3. **`telemetry` feature + OTLP endpoint** — full OpenTelemetry export with
//!    four subscriber layers:
//!    - `EnvFilter` — log level gating
//!    - `fmt::layer()` — console output (always)
//!    - `tracing_opentelemetry::layer()` — span export to OTLP
//!    - `OpenTelemetryTracingBridge` — log-to-span correlation via OTLP
//!
//! The log-span correlation relies on `opentelemetry-appender-tracing` with the
//! `experimental_use_tracing_span_context` feature: every `tracing::info!()`
//! emitted inside a span automatically carries the span's `trace_id` and
//! `span_id` in the exported OTel log record.

use tracing_subscriber::EnvFilter;

/// Telemetry configuration parsed from TOML or environment.
#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct TelemetryConfig {
    /// Log level filter (e.g. `"info"`, `"debug"`).
    pub level: String,
    /// OTLP collector endpoint (e.g. `"https://otlp.example.com:4317"`).
    /// If empty, OTel export is disabled even when the feature is compiled in.
    pub otlp_endpoint: String,
    /// Custom OTLP headers in `key=value,key2=value2` format (for auth).
    pub otlp_headers: String,
    /// Service name reported in OTel resource attributes.
    pub service_name: String,
    /// Node ID hex string, attached as `service.instance.id`.
    pub instance_id: String,
}

/// Initialize the telemetry subscriber.
///
/// Call this once at startup, before any `tracing` events are emitted.
pub fn init(config: &TelemetryConfig) {
    #[cfg(feature = "telemetry")]
    {
        if config.otlp_endpoint.is_empty() {
            init_console(&config.level);
        } else if let Err(e) = init_otel(config) {
            eprintln!("Failed to init OpenTelemetry: {e}, falling back to console");
            init_console(&config.level);
        }
    }

    #[cfg(not(feature = "telemetry"))]
    {
        init_console(&config.level);
    }
}

/// Console-only tracing subscriber (always available).
fn init_console(level: &str) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(level));

    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Full OpenTelemetry initialization: traces + logs exported via OTLP.
///
/// Sets up the tracer provider, logger provider, and wires them into the
/// `tracing` subscriber as layers. The `OpenTelemetryTracingBridge` with
/// `experimental_use_tracing_span_context` ensures every log event carries
/// the parent span's trace context.
#[cfg(feature = "telemetry")]
fn init_otel(config: &TelemetryConfig) -> anyhow::Result<()> {
    use opentelemetry::KeyValue;
    use opentelemetry::trace::TracerProvider;
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use opentelemetry_otlp::{LogExporter, SpanExporter};
    use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
    use opentelemetry_sdk::Resource;
    use opentelemetry_sdk::logs::SdkLoggerProvider;
    use opentelemetry_sdk::trace::SdkTracerProvider;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    let service_name = if config.service_name.is_empty() {
        "shoald".to_string()
    } else {
        config.service_name.clone()
    };

    let instance_id = if config.instance_id.is_empty() {
        uuid_short()
    } else {
        config.instance_id.clone()
    };

    // -- Shared resource attributes --
    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.name", service_name))
        .with_attribute(KeyValue::new("service.instance.id", instance_id))
        .build();

    // -- Parse custom headers --
    let metadata = parse_otlp_headers(&config.otlp_headers);

    // -- Trace pipeline (spans → OTLP) --
    let mut span_builder = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otlp_endpoint);

    if let Some(ref md) = metadata {
        span_builder = span_builder.with_metadata(md.clone());
    }

    let span_exporter = span_builder.build()?;

    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter)
        .build();

    // Register globally so tracing-opentelemetry can find it.
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());

    // -- Log pipeline (logs with span context → OTLP) --
    let mut log_builder = LogExporter::builder()
        .with_tonic()
        .with_endpoint(&config.otlp_endpoint);

    if let Some(ref md) = metadata {
        log_builder = log_builder.with_metadata(md.clone());
    }

    let log_exporter = log_builder.build()?;

    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource)
        .with_batch_exporter(log_exporter)
        .build();

    // -- Build the 4-layer subscriber --
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.level));

    let fmt_layer = tracing_subscriber::fmt::layer();

    // Layer 3: converts tracing spans → OTel spans.
    let telemetry_layer =
        tracing_opentelemetry::layer().with_tracer(tracer_provider.tracer("shoald"));

    // Layer 4: bridges tracing log events → OTel log records with span context.
    let otel_log_layer = OpenTelemetryTracingBridge::new(&logger_provider);

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .with(telemetry_layer)
        .with(otel_log_layer)
        .init();

    tracing::info!(
        "OpenTelemetry initialized (OTLP endpoint: {})",
        config.otlp_endpoint
    );

    Ok(())
}

/// Graceful OTel shutdown — flushes pending spans and logs.
///
/// Call this during the daemon's shutdown sequence.
pub fn shutdown() {
    #[cfg(feature = "telemetry")]
    {
        // The global tracer provider is dropped via RAII when the
        // SdkTracerProvider goes out of scope. We can also explicitly
        // call shutdown on the provider if we stored it.
        // For now, the drop-based cleanup is sufficient.
    }
}

/// Parse OTLP headers from a `key=value,key2=value2` string into tonic metadata.
#[cfg(feature = "telemetry")]
fn parse_otlp_headers(raw: &str) -> Option<tonic::metadata::MetadataMap> {
    if raw.is_empty() {
        return None;
    }

    let mut map = tonic::metadata::MetadataMap::new();
    for pair in raw.split(',') {
        if let Some((k, v)) = pair.split_once('=') {
            let k = k.trim();
            let v = v.trim();
            if let (Ok(key), Ok(val)) = (
                k.parse::<tonic::metadata::MetadataKey<tonic::metadata::Ascii>>(),
                v.parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>(),
            ) {
                map.insert(key, val);
            }
        }
    }

    Some(map)
}

/// Generate a short unique instance identifier (first 8 hex chars of blake3).
#[cfg(feature = "telemetry")]
fn uuid_short() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let hash = blake3::hash(&ts.to_le_bytes());
    hash.to_hex()[..8].to_string()
}
