use std::io;
use std::marker::PhantomData;

use chrono::prelude::*;
use tracing::Subscriber;
use tracing_core::Event;
use tracing_core::span::{Attributes, Id};
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;

struct TraceID {
    id: u64,
}

struct DataTraceId {
    id: u64,
}

pub struct TaosXLayer<S, W = fn() -> io::Stdout> {
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
            make_writer: io::stdout,
            _inner: PhantomData,
        }
    }
}

impl<S, W> TaosXLayer<S, W> {
    pub fn with_writer<W2>(self, make_writer: W2) -> TaosXLayer<S, W2>
        where
            W2: for<'writer> MakeWriter<'writer> + 'static,
    {
        TaosXLayer {
            make_writer,
            _inner: self._inner,
        }
    }

    fn format_timestamp(buf: &mut String) {
        let local: DateTime<Local> = Local::now();
        let s = local.format("%Y-%m-%d %H:%M:%S.%6f ").to_string();
        buf.push_str(s.as_str())
    }
}

impl<S, W> layer::Layer<S> for TaosXLayer<S, W> where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: for<'writer> MakeWriter<'writer> + 'static,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if let Some(field) = attrs.fields().field("TID") {
            println!("==============found field TID=====");
            let span = ctx.span(id).unwrap();
            let mut ext = span.extensions_mut();
            ext.insert(TraceID { id: 123 });
            println!("=============set traceId 123========");
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let mut buf = String::new();
        // Part 1
        Self::format_timestamp(&mut buf);
        // Part 2
        if let Some(span) = ctx.lookup_current() {
            let ext = span.extensions();
            let traceId = ext.get::<TraceID>();
            if let Some(tid) = traceId {
                println!("=======get traceId {:?}=========", tid.id);
            } else {
                println!("============can't find tracId====");
            }
        }



        buf.push('\n');
        let mut writer = self.make_writer.make_writer_for(event.metadata());
        let res = io::Write::write_all(&mut writer, buf.as_bytes());
    }
}