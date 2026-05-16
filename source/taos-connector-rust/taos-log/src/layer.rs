use std::collections::BTreeMap;
use std::marker::PhantomData;

use chrono::{DateTime, Local};
use tracing::field::{self, Visit};
use tracing::Event;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::registry::{LookupSpan, Scope};
use tracing_subscriber::Registry;

use crate::writer::RollingFileAppender;
use crate::QidManager;

#[derive(Clone)]
struct RecordFields(BTreeMap<String, String>, Option<String>);

pub struct TaosLayer<Q, S = Registry, M = RollingFileAppender> {
    make_writer: M,
    #[cfg(feature = "ansi")]
    with_ansi: bool,
    with_location: bool,
    _s: PhantomData<fn(S)>,
    _q: PhantomData<Q>,
}

impl<Q, S, M> TaosLayer<Q, S, M> {
    pub fn new(make_writer: M) -> Self {
        Self {
            make_writer,
            #[cfg(feature = "ansi")]
            with_ansi: false,
            with_location: false,
            _s: PhantomData,
            _q: PhantomData,
        }
    }

    #[cfg(feature = "ansi")]
    pub fn with_ansi(self) -> Self {
        Self {
            with_ansi: true,
            ..self
        }
    }

    pub fn with_location(self) -> Self {
        Self {
            with_location: true,
            ..self
        }
    }

    fn fmt_timestamp(&self, buf: &mut String) {
        let local: DateTime<Local> = Local::now();
        let s = local.format("%m/%d %H:%M:%S.%6f ").to_string();
        #[cfg(feature = "ansi")]
        let s = if self.with_ansi {
            nu_ansi_term::Color::DarkGray.paint(s).to_string()
        } else {
            s
        };
        buf.push_str(s.as_str());
    }

    fn fmt_thread_id(&self, buf: &mut String) {
        let s = format!("{:0>8}", thread_id::get());
        #[cfg(feature = "ansi")]
        let s = if self.with_ansi {
            nu_ansi_term::Color::DarkGray.paint(s).to_string()
        } else {
            s
        };
        buf.push_str(s.as_str());
    }

    fn fmt_level(&self, buf: &mut String, level: &tracing::Level) {
        buf.push(' ');
        let level_str = match *level {
            tracing::Level::TRACE => "TRACE",
            tracing::Level::DEBUG => "DEBUG",
            tracing::Level::INFO => "INFO ",
            tracing::Level::WARN => "WARN ",
            tracing::Level::ERROR => "ERROR",
        }
        .to_string();
        #[cfg(feature = "ansi")]
        let level_str = if self.with_ansi {
            match *level {
                tracing::Level::TRACE => nu_ansi_term::Color::Purple.paint(level_str),
                tracing::Level::DEBUG => nu_ansi_term::Color::Blue.paint(level_str),
                tracing::Level::INFO => nu_ansi_term::Color::Green.paint(level_str),
                tracing::Level::WARN => nu_ansi_term::Color::Yellow.paint(level_str),
                tracing::Level::ERROR => nu_ansi_term::Color::Red.paint(level_str),
            }
            .to_string()
        } else {
            level_str
        };
        buf.push_str(&level_str);
    }

    fn fmt_fields_and_qid(&self, buf: &mut String, event: &Event, scope: Option<Scope<S>>)
    where
        S: for<'s> LookupSpan<'s>,
        Q: QidManager,
    {
        let mut kvs = BTreeMap::new();

        let is_from_log = event.metadata().target() == "log";

        let mut message = None;
        event.record(&mut RecordVisit {
            kvs: &mut kvs,
            message: &mut message,
            is_from_log,
        });

        if !is_from_log {
            kvs.insert("mod".to_string(), event.metadata().target().to_string());
        }

        let mut qid_field = None;

        let print_stacktrace = tracing::enabled!(tracing::Level::DEBUG);

        let mut spans = vec![];
        if let Some(scope) = scope {
            for span in scope.from_root() {
                if print_stacktrace {
                    spans.push(format_str(span.name()));
                }

                {
                    let qid = span.extensions().get::<Q>().cloned();
                    if let Some(qid) = qid {
                        qid_field.replace(qid);
                    }
                }

                {
                    let fields = span.extensions().get::<RecordFields>().cloned();
                    if let Some(fields) = fields {
                        kvs.extend(fields.0.into_iter());
                    }
                }
            }
        }

        if let Some(qid) = qid_field {
            buf.push(' ');
            buf.push_str(&format!("QID:{}", qid.display()));
        }

        if !kvs.is_empty() {
            buf.push(' ');
            let mut fields = Vec::new();
            if let Some(target) = kvs.remove("mod") {
                fields.push(format!("mod:{target}"));
            }
            fields.extend(kvs.into_iter().map(|(k, v)| format!("{k}:{v}")));
            let kvs = fields.join(", ");
            #[cfg(feature = "ansi")]
            let kvs = if self.with_ansi {
                nu_ansi_term::Color::DarkGray.paint(kvs).to_string()
            } else {
                kvs
            };
            buf.push_str(&kvs);
        }

        if let Some(message) = message {
            buf.push_str(", ");
            buf.push_str(&message);
        }

        if print_stacktrace && !spans.is_empty() {
            buf.push_str(", ");
            let s = format!("stack:{}", spans.join("->"));
            #[cfg(feature = "ansi")]
            let s = if self.with_ansi {
                nu_ansi_term::Color::DarkGray.paint(s).to_string()
            } else {
                s
            };
            buf.push_str(&s);
        }

        if self.with_location {
            let meta = event.metadata();
            if let (Some(file), Some(line)) = (meta.file(), meta.line()) {
                buf.push(' ');
                let s = format!("at {file}:{line}");
                #[cfg(feature = "ansi")]
                let s = if self.with_ansi {
                    nu_ansi_term::Color::DarkGray.paint(s).to_string()
                } else {
                    s
                };
                buf.push_str(&s);
            }
        }
    }
}

impl<Q, S, M> tracing_subscriber::Layer<S> for TaosLayer<Q, S, M>
where
    Q: QidManager,
    S: tracing::subscriber::Subscriber + for<'a> LookupSpan<'a>,
    M: for<'writer> MakeWriter<'writer> + 'static,
{
    fn on_new_span(
        &self,
        attrs: &tracing::span::Attributes<'_>,
        id: &tracing::span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx
            .span(id)
            .expect("Span not found, this is a bug in tracing");
        let qid = match span
            .parent()
            .as_ref()
            .and_then(|p| p.extensions().get::<Q>().cloned())
        {
            Some(qid) => qid,
            None => Q::init(),
        };
        let mut extensions = span.extensions_mut();
        extensions.replace(qid);

        if extensions.get_mut::<RecordFields>().is_none() {
            let mut fields = BTreeMap::new();
            let mut message = None;
            attrs.values().record(&mut RecordVisit {
                kvs: &mut fields,
                message: &mut message,
                is_from_log: false,
            });
            extensions.replace(RecordFields(fields, message));
        }
    }

    fn on_record(
        &self,
        id: &tracing::span::Id,
        values: &tracing::span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let span = ctx
            .span(id)
            .expect("Span not found, this is a bug in tracing");
        let mut extensions = span.extensions_mut();
        match extensions.get_mut::<RecordFields>() {
            Some(RecordFields(fields, message)) => {
                values.record(&mut RecordVisit {
                    kvs: fields,
                    message,
                    is_from_log: false,
                });
            }
            None => {
                let mut fields = BTreeMap::new();
                let mut message = None;
                values.record(&mut RecordVisit {
                    kvs: &mut fields,
                    message: &mut message,
                    is_from_log: false,
                });
                extensions.replace(RecordFields(fields, message));
            }
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        thread_local! {
            static BUF: std::cell::RefCell<String> = const { std::cell::RefCell::new(String::new()) };
        }

        BUF.with(|buf| {
            let borrow = buf.try_borrow_mut();
            let mut a;
            let mut b;
            let buf = match borrow {
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
            self.fmt_timestamp(buf);
            // Part 2: process id
            self.fmt_thread_id(buf);
            // Part 3: level
            let metadata = event.metadata();
            self.fmt_level(buf, metadata.level());
            // Part 4 and Part 5:  span and QID
            self.fmt_fields_and_qid(buf, event, ctx.event_scope(event));
            // Part 6: write event content
            buf.push('\n');
            // put all to writer
            let mut writer = self.make_writer.make_writer_for(metadata);
            let res = std::io::Write::write_all(&mut writer, buf.as_bytes());
            if let Err(e) = res {
                eprintln!("[TaosLayer] Unable to write an event to the Writer for this Subscriber! Error: {e}\n");
            }
            buf.clear();
        });
    }
}

pub struct RecordVisit<'a> {
    kvs: &'a mut BTreeMap<String, String>,
    message: &'a mut Option<String>,
    is_from_log: bool,
}

impl Visit for RecordVisit<'_> {
    fn record_str(&mut self, field: &field::Field, value: &str) {
        if field.name() == "message" {
            self.message.replace(value.to_string());
            return;
        }

        if self.is_from_log && field.name() == "log.target" {
            self.kvs.insert("mod".to_string(), format_str(value));
            return;
        }

        if !self.is_from_log || !field.name().starts_with("log.") {
            self.kvs.insert(format_str(field.name()), format_str(value));
        }
    }

    fn record_debug(&mut self, field: &field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message.replace(format!("{value:?}"));
            return;
        }

        if self.is_from_log && field.name() == "log.target" {
            self.kvs.insert("mod".to_string(), field.name().to_string());
        }

        if !self.is_from_log || !field.name().starts_with("log.") {
            self.kvs
                .insert(format_str(field.name()), format!("{value:?}"));
        }
    }
}

fn format_str(value: &str) -> String {
    if value.contains(' ') {
        format!("{value:?}")
    } else {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use tracing_subscriber::fmt::MakeWriter;

    use crate::fake::Qid;
    use crate::layer::TaosLayer;
    use crate::utils::{QidMetadataGetter, QidMetadataSetter, Span};
    use crate::QidManager;

    struct TestWriter(std::sync::mpsc::Sender<String>);

    impl<'a> MakeWriter<'a> for TestWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            Self(self.0.clone())
        }
    }

    impl std::io::Write for TestWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.send(String::from_utf8(buf.to_vec()).unwrap()).ok();
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn layer_test() {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;

        let (tx, rx) = std::sync::mpsc::channel();
        let _guard = tracing_subscriber::registry()
            .with(TaosLayer::<Qid, _, _>::new(TestWriter(tx.clone())))
            .set_default();

        tracing::info_span!("outer", "k" = "kkk").in_scope(|| {
            tracing::info!(port = 1111, "this is a test info log");
            let s = rx.recv().unwrap();
            assert_eq!(&s[38..], "INFO  QID:0x7fffffffffffffff mod:taos_log::layer::tests, k:kkk, port:1111, this is a test info log\n");
            // test qid init
            let qid: Qid = Span.get_qid().unwrap();
            assert_eq!(qid.get(), 9223372036854775807);
            Span.set_qid(&Qid::from(999));
            tracing::info_span!("inner", "a" = "aaa").in_scope(|| {
                tracing::info!(ip = "127.0.0.1","this is a test inner info log");
                let s = rx.recv().unwrap();
                assert_eq!(&s[38..], "INFO  QID:0x3e7 mod:taos_log::layer::tests, a:aaa, ip:127.0.0.1, k:kkk, this is a test inner info log\n");
                // test qid inherit
                let qid: Qid = Span.get_qid().unwrap();
                assert_eq!(qid.get(), 999);
            })
        });
    }
}
