use tracing;
use tracing_core::Subscriber;
use tracing_subscriber::registry::LookupSpan;

pub fn set_trace_id() {
    let current_span = tracing::span::Span::current();
    if current_span.has_field("TID") {
        let span_id = current_span.id().unwrap().into_u64();
        let trace_id = format!("{:016X}", span_id);
        current_span.record("TID", trace_id);
    }
}
