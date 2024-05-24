use std::cell::RefCell;
use std::io;
use std::marker::PhantomData;
use std::sync::atomic::Ordering;

use chrono::prelude::*;
use metrics::atomics::AtomicU64;
use rand::random;
use tracing::{Span, Subscriber};
use tracing_core::span::{Attributes, Id, Record};
use tracing_core::{Event, Level};
use tracing_subscriber::fmt::format::DefaultFields;
use tracing_subscriber::fmt::{FormatFields, FormattedFields, MakeWriter};
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{LookupSpan, Scope};
use tracing_subscriber::{layer, Registry};

const TRACE_STR: &str = "TRACE";
const DEBUG_STR: &str = "DEBUG";
const INFO_STR: &str = "INFO";
const WARN_STR: &str = "WARN";
const ERROR_STR: &str = "ERROR";

/// Hex string representation of Trace ID stored in its [extensions]
#[derive(Clone)]
pub struct TraceID {
    pub id: String,
}

/// Hex string representation of Query ID stored in its [extensions]
#[derive(Clone)]
pub struct DataTraceID {
    pub hex: String,
}

pub struct TaosXLayer<S, N = DefaultFields, W = fn() -> io::Stdout> {
    fmt_fields: N,
    make_writer: W,
    _inner: PhantomData<fn(S)>,
}

impl<S> TaosXLayer<S> {
    pub fn new() -> Self {
        Self::default()
    }
}

impl<S> Default for TaosXLayer<S> {
    fn default() -> Self {
        TaosXLayer {
            fmt_fields: DefaultFields::default(),
            make_writer: io::stdout,
            _inner: PhantomData,
        }
    }
}

impl<S, N, W> TaosXLayer<S, N, W> {
    pub fn with_writer<W2>(self, make_writer: W2) -> TaosXLayer<S, N, W2>
    where
        W2: for<'writer> MakeWriter<'writer> + 'static,
    {
        TaosXLayer {
            fmt_fields: self.fmt_fields,
            make_writer,
            _inner: self._inner,
        }
    }
}

impl<S, N, W> layer::Layer<S> for TaosXLayer<S, N, W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
    W: for<'writer> MakeWriter<'writer> + 'static,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found, this is a bug");
        let mut extensions = span.extensions_mut();
        if let Some(_) = attrs.fields().field("TID") {
            if extensions.get_mut::<TraceID>().is_none() {
                let u32_id = random::<u32>();
                let hex_id = format!("{:#08x}", u32_id);
                extensions.insert(TraceID { id: hex_id });
            }
        }
        if extensions.get_mut::<FormattedFields<N>>().is_none() {
            let mut fields = FormattedFields::<N>::new(String::new());
            if self
                .fmt_fields
                .format_fields(fields.as_writer(), attrs)
                .is_ok()
            {
                extensions.insert(fields);
            } else {
                eprintln!(
                    "[tracing-subscriber] Unable to format the following event, ignoring: {:?}",
                    attrs
                );
            }
        }
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found, this is a bug");
        let mut extensions = span.extensions_mut();
        if let Some(fields) = extensions.get_mut::<FormattedFields<N>>() {
            let _ = self.fmt_fields.add_fields(fields, values);
            return;
        }

        let mut fields = FormattedFields::<N>::new(String::new());
        if self
            .fmt_fields
            .format_fields(fields.as_writer(), values)
            .is_ok()
        {
            extensions.insert(fields);
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        thread_local! {
            static BUF: RefCell<String> = RefCell::new(String::new());
        }

        BUF.with(|buf| {
            let borrow = buf.try_borrow_mut();
            let mut a;
            let mut b;
            let mut buf = match borrow {
                Ok(buf) => {
                    a = buf;
                    &mut *a
                }
                _ => {
                    b = String::new();
                    &mut b
                }
            };

            // Part 1: timestamp
            Self::fmt_timestamp(&mut buf);
            // Part 2: level
            let metadata = event.metadata();
            let level = Self::fmt_level(metadata.level());
            if !level.is_empty() {
                buf.push_str(level);
                buf.push(' ');
            }
            // Part 3: mod name(target)
            Self::fmt_mod(&mut buf, metadata.target(), metadata.line());
            // Part 4 and Part 5:  span and TID or DTID
            if let Some(scope) = ctx.event_scope(event) {
                Self::fmt_span_and_trace_id(&mut buf, scope);
            }
            // Part 6: write event content
            buf.push_str(" ");
            let mut fake_fields = FormattedFields::<N>::new(String::new());
            self.fmt_fields.format_fields(fake_fields.as_writer(), event).expect("write event content error");
            buf.push_str(fake_fields.fields.as_str());
            buf.push('\n');
            // put all to writer
            let mut writer = self.make_writer.make_writer_for(event.metadata());
            let res = io::Write::write_all(&mut writer, buf.as_bytes());
            if let Err(e) = res {
                eprintln!("[TaosXLayer] Unable to write an event to the Writer for this Subscriber! Error: {}\n", e);
            }
            buf.clear();
        });
    }
}

impl<S, N, W> TaosXLayer<S, N, W>
where
    N: 'static + for<'writer> FormatFields<'writer>,
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: 'static + for<'writer> MakeWriter<'writer>,
{
    fn fmt_timestamp(buf: &mut String) {
        let local: DateTime<Local> = Local::now();
        let s = local.format("%m/%d %H:%M:%S.%6f ").to_string();
        buf.push_str(s.as_str())
    }
    fn fmt_level<'a>(level: &Level) -> &'a str {
        match *level {
            Level::TRACE => TRACE_STR,
            Level::DEBUG => DEBUG_STR,
            Level::INFO => INFO_STR,
            Level::WARN => WARN_STR,
            Level::ERROR => ERROR_STR,
        }
    }
    fn fmt_span_and_trace_id(buf: &mut String, scope: Scope<S>) {
        let mut span_buf = String::new();
        let mut trace_buf = String::new();
        span_buf.push('[');
        for span in scope.from_root() {
            // collect span fields
            span_buf.push_str(span.name());
            let extension = span.extensions();
            let fields = &extension
                .get::<FormattedFields<N>>()
                .expect("will never be `None`");
            if !fields.is_empty() {
                span_buf.push('{');
                span_buf.push_str(fields.as_str());
                span_buf.push('}');
            }
            span_buf.push('-');
            span_buf.push('>');
            // collect trace id
            if let Some(trace_id) = extension.get::<TraceID>() {
                trace_buf.push_str("TID:");
                trace_buf.push_str(trace_id.id.as_str());
                trace_buf.push(',')
            }
            // collect query id
            if let Some(query_id) = extension.get::<DataTraceID>() {
                trace_buf.push_str("DTID:");
                trace_buf.push_str(query_id.hex.as_str());
                trace_buf.push(',')
            }
        }
        span_buf.pop();
        span_buf.pop();
        span_buf.push(']');
        if !trace_buf.is_empty() {
            trace_buf.pop();
            buf.push(' ');
            buf.push_str(trace_buf.as_str());
        }
        buf.push(' ');
        buf.push_str(span_buf.as_str());
    }

    #[inline]
    fn fmt_mod(buf: &mut String, long_mod: &str, line_opt: Option<u32>) {
        buf.push('[');
        let i_opt = long_mod.rfind(":");
        match i_opt {
            Some(i) => {
                let (_, short_mod) = long_mod.split_at(i + 1);
                buf.push_str(short_mod);
            }
            None => {
                buf.push_str(long_mod);
            }
        }
        if let Some(line) = line_opt {
            buf.push(':');
            buf.push_str(line.to_string().as_str());
        }
        buf.push(']');
    }
}

/// Explicitly set a trace ID for current span.
pub fn set_trace_id_for_current_span(tid: &str) {
    tracing::dispatcher::get_default(|dispatch| {
        let registry = dispatch
            .downcast_ref::<Registry>()
            .expect("no global default dispatcher found");
        if let Some((id, _meta)) = dispatch.current_span().into_inner() {
            let span = registry.span(&id).unwrap();
            let mut ext = span.extensions_mut();
            ext.replace(TraceID {
                id: String::from(tid),
            });
        }
    });
}

/// Find the first trace ID in current span chain, and set it to provided span.
pub fn attach_trace_id(target_span: &Span) {
    tracing::dispatcher::get_default(|dispatch| {
        let registry = dispatch
            .downcast_ref::<Registry>()
            .expect("no global default dispatcher found");
        if let Some((id, _meta)) = dispatch.current_span().into_inner() {
            let cur_span = registry.span(&id).unwrap();
            let scope = cur_span.scope();
            for sp in scope.from_root() {
                let ext = sp.extensions();
                if let Some(tid) = ext.get::<TraceID>() {
                    let target_span_id = target_span.id().expect("failed to get span id");
                    let target_span_ref = registry
                        .span(&target_span_id)
                        .expect("failed to get span by id");
                    let mut target_ext = target_span_ref.extensions_mut();
                    target_ext.insert(tid.clone());
                    break;
                }
            }
        }
    });
}

pub fn set_data_trace_id_for_current_span(trace_id: &TraceStreamId) {
    tracing::dispatcher::get_default(|dispatch| {
        let registry = dispatch
            .downcast_ref::<Registry>()
            .expect("no global default dispatcher found");
        if let Some((id, _meta)) = dispatch.current_span().into_inner() {
            let hex_trace_id = trace_id.to_string();
            let span = registry.span(&id).unwrap();
            let mut ext = span.extensions_mut();
            ext.replace(DataTraceID { hex: hex_trace_id });
        }
    });
}

/// Stream Trace ID is 16 bits random number in hex format.
pub fn create_stream_trace_id() -> String {
    let id = random::<u16>();
    let mut hex_str = format!("{:#06x}", id);
    // remove heading "0x"
    hex_str.remove(0);
    hex_str.remove(0);
    hex_str
}

#[inline]
pub fn create_data_trace_id(stream_trace_id: u64, batch_number: u32) -> u64 {
    stream_trace_id + (u64::from(batch_number) << 16)
}

pub fn get_data_trace_id_str(data_trace_id: u64) -> String {
    let mut s = format!("{:#018x}", data_trace_id);
    s.truncate(14);
    s
}

/// Convert hex format stream id to u64 stream id
#[inline]
pub fn get_stream_id_u64(stream_id: &str) -> u64 {
    let id = u64::from_str_radix(stream_id, 16).unwrap();
    id << 48
}

#[derive(Debug, Clone)]
pub struct RequestID {
    inner: std::sync::Arc<AtomicU64>,
}

impl RequestID {
    pub fn new(initial_value: u64) -> Self {
        RequestID {
            inner: std::sync::Arc::new(AtomicU64::new(initial_value)),
        }
    }

    pub fn trace_id_str(&self) -> String {
        get_data_trace_id_str(self.inner.load(Ordering::SeqCst))
    }

    pub fn next(&self) -> u64 {
        self.inner.fetch_add(1, Ordering::Acquire) + 1
    }
}

/// Stream Trace ID is 16 bits random number in hex format.
///
/// It is used to identify the stream of data.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct TraceStreamId(u16);

impl std::fmt::Display for TraceStreamId {
    /// Display the stream id in hex format.
    ///
    /// For example, 0x1234_0000_0000_0000 will be displayed as 1234.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:04x}", self.0)
    }
}

impl std::fmt::Debug for TraceStreamId {
    /// Format the stream id in hex format.
    ///
    /// If the alternate flag is set, the stream id will be displayed with 0x prefix.
    ///
    /// For example, stream 0x1234 will be displayed as 0x1234 with "{:#}".
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if f.alternate() {
            write!(f, "{:#04x}", self.0)
        } else {
            write!(f, "{:04x}", self.0)
        }
    }
}

impl TraceStreamId {
    /// Create a new stream id with the given id.
    #[inline]
    pub fn new(id: u16) -> Self {
        TraceStreamId(id)
    }

    /// Create a random stream id.
    #[inline]
    pub fn random() -> Self {
        let id = random::<u16>();
        TraceStreamId(id)
    }

    #[inline]
    pub fn from_hex(hex: &str) -> Self {
        let id = u16::from_str_radix(hex, 16).unwrap();
        TraceStreamId(id)
    }

    #[inline]
    pub fn with_data_id(&self, data_id: u32) -> TraceDataId {
        TraceDataId((u64::from(self.0) << 48) | (u64::from(data_id) << 16))
    }

    #[inline]
    pub fn to_data_id(&self) -> TraceDataId {
        TraceDataId((self.0 as u64) << 48)
    }

    #[inline]
    pub fn as_u64(&self) -> u64 {
        (self.0 as u64) << 48
    }

    #[inline]
    pub fn to_request_id(&self) -> RequestID {
        RequestID::new((self.0 as u64) << 48)
    }
}

/// Data Trace ID is 48 bits stream id + 16 bits data id in hex format.
///
/// Data Trance ID contains 2-bytes stream id + 4-bytes data id (u32).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct TraceDataId(pub u64);

impl std::fmt::Display for TraceDataId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#014x}", self.0 >> 16)
    }
}
impl std::fmt::Debug for TraceDataId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#014x}", self.0 >> 16)
    }
}

impl std::ops::Deref for TraceDataId {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl TraceDataId {
    /// Get the stream id from the data trace id.
    pub fn stream_id(self) -> TraceStreamId {
        TraceStreamId((self.0 >> 48) as u16)
    }

    /// Get the data id(or batch id) from the data trace id.
    pub fn data_id(self) -> u32 {
        (self.0 & 0x0000_FFFF_FFFF_FFFF >> 16) as u32
    }

    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// It is used to generate next data id for the same stream id.
    ///
    /// The data id is increased by 1. For example,
    ///
    /// ```rust
    /// use taosx_core::utils::trace::TraceDataId;
    /// let trace_data_id = TraceDataId(0x1234_0000_5678_0000);
    /// let next_data_id = trace_data_id.next();
    /// println!("{}", next_data_id);
    /// assert_eq!(next_data_id, TraceDataId(0x1234_0000_5679_0000));
    /// ```
    ///
    /// It's not safe when the data id is overflowed (> u32::MAX).
    pub fn next(self) -> Self {
        TraceDataId(self.0 + (1 << 16))
    }
}

/// Request ID to TDengine: 2-bytes stream id + 4-bytes data id + 2-bytes request id.
///
/// Request ID is used to identify the request in TDengine(both with taosc or http).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct TraceRequestId(pub u64);

impl std::fmt::Display for TraceRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#018x}", self.0)
    }
}
impl std::fmt::Debug for TraceRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#018x}", self.0)
    }
}

impl TraceRequestId {
    /// Get the stream id from the data trace id.
    pub fn stream_id(self) -> TraceStreamId {
        TraceStreamId((self.0 >> 48) as u16)
    }

    /// Get the data id(or batch id) from the data trace id.
    pub fn data_id(self) -> TraceDataId {
        TraceDataId(self.0)
    }

    /// Get the underlying u64 of the current request id.
    pub fn into_inner(self) -> u64 {
        self.0
    }

    /// It is used to generate next data id for the same stream id.
    ///
    /// The data id is increased by 1. For example,
    ///
    /// ```rust
    /// use taosx_core::utils::trace::TraceRequestId;
    /// let id = TraceRequestId(0x1234_0000_5678_0000);
    /// let next = id.next();
    /// println!("{}", next);
    /// assert_eq!(next.to_string(), "0x1234000056780001");
    /// assert_eq!(next, TraceRequestId(0x1234_0000_5678_0001));
    /// ```
    ///
    /// It's not safe when the request id is overflowed (> u16::MAX).
    pub fn next(self) -> Self {
        TraceRequestId(self.0 + 1)
    }
}
