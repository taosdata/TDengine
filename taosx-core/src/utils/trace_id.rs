use tracing;
use tracing_core::Subscriber;
use tracing_subscriber::registry::LookupSpan;

pub fn set_trace_id() {
    let current_span = tracing::span::Span::current();
    if current_span.has_field("traceId") {
        current_span.record("traceId", current_span.id().unwrap().into_u64());
    }
}
