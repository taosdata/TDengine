use std::cell::RefCell;
use std::io;
use std::marker::PhantomData;

use chrono::prelude::*;
use rand::random;
use tracing::Subscriber;
use tracing_core::{Event, Level};
use tracing_core::span::{Attributes, Id, Record};
use tracing_subscriber::fmt::{FormatFields, FormattedFields, MakeWriter};
use tracing_subscriber::fmt::format::DefaultFields;
use tracing_subscriber::layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::{LookupSpan, Scope};

const TRACE_STR: &str = "TRACE";
const DEBUG_STR: &str = "DEBUG";
const INFO_STR: &str = " INFO";
const WARN_STR: &str = " WARN";
const ERROR_STR: &str = "ERROR";


/// Hex string representation of Trace ID stored in its [extensions]
struct TraceID {
    pub hex: String,
}

/// Hex string representation of Query ID stored in its [extensions]
struct QueryID {
    pub hex: String,
}

pub struct TaosXLayer<
    S,
    N = DefaultFields,
    W = fn() -> io::Stdout> {
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

impl<S, N, W> layer::Layer<S> for TaosXLayer<S, N, W> where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'writer> FormatFields<'writer> + 'static,
    W: for<'writer> MakeWriter<'writer> + 'static,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let span = ctx.span(id).expect("Span not found, this is a bug");
        let mut extensions = span.extensions_mut();
        if let Some(_) = attrs.fields().field("TID") {
            let u32_id = random::<u32>();
            let hex_id = format!("{:#08x}", u32_id);
            extensions.insert(TraceID { hex: hex_id });
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
            buf.push_str(level);
            buf.push(' ');
            // Part 3: thread name
            let current_thread = std::thread::current();
            if let Some(name) = current_thread.name() {
                buf.push('[');
                buf.push_str(name);
                buf.push(']');
            }
            // Part 4 and Part 5:  span and TID or QID
            if let Some(scope) = ctx.event_scope(event) {
                buf.push(' ');
                Self::fmt_span_and_trace_id(&mut buf, scope);
            }
            // Part 6: write event content
            buf.push_str(" - ");
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

impl<S, N, W> TaosXLayer<S, N, W> where
    N: 'static + for<'writer> FormatFields<'writer>,
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: 'static + for<'writer> MakeWriter<'writer> {
    fn fmt_timestamp(buf: &mut String) {
        let local: DateTime<Local> = Local::now();
        let s = local.format("%Y-%m-%d %H:%M:%S.%6f ").to_string();
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
                trace_buf.push_str(trace_id.hex.as_str());
                trace_buf.push(',')
            }
            // collect query id
            if let Some(query_id) = extension.get::<QueryID>() {
                trace_buf.push_str("QID:");
                trace_buf.push_str(query_id.hex.as_str());
                trace_buf.push(',')
            }
        }
        span_buf.pop();
        span_buf.pop();
        span_buf.push(']');
        buf.push_str(span_buf.as_str());
        if !trace_buf.is_empty() {
            trace_buf.pop();
            buf.push(' ');
            buf.push_str(trace_buf.as_str());
        }
    }
}