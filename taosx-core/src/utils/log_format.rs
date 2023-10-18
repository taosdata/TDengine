use std::fmt;

use tracing_core::{Event, Level, Subscriber};
use tracing_subscriber::fmt::{
    FmtContext,
    format::{self, FormatEvent, FormatFields, Writer},
    FormattedFields,
};
use tracing_subscriber::fmt::time::{FormatTime, SystemTime};
use tracing_subscriber::registry::LookupSpan;

pub struct TaosXLogFormatter<T = SystemTime> {
    pub timer: T,
}

impl<T> TaosXLogFormatter<T> {
    fn format_timestamp(&self, writer: &mut Writer<'_>) -> fmt::Result
        where T: FormatTime, {
        self.timer.format_time(writer)?;
        writer.write_char(' ')
    }
}

const TRACE_STR: &str = "TRACE";
const DEBUG_STR: &str = "DEBUG";
const INFO_STR: &str = " INFO";
const WARN_STR: &str = " WARN";
const ERROR_STR: &str = "ERROR";

fn fmt_level<'a>(level: &Level) -> &'a str {
    match *level {
        Level::TRACE => TRACE_STR,
        Level::DEBUG => DEBUG_STR,
        Level::INFO => INFO_STR,
        Level::WARN => WARN_STR,
        Level::ERROR => ERROR_STR,
    }
}

impl<S, N, T> FormatEvent<S, N> for TaosXLogFormatter<T>
    where
        S: Subscriber + for<'a> LookupSpan<'a>,
        N: for<'a> FormatFields<'a> + 'static,
        T: FormatTime
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: format::Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        // part 1: timestamp
        self.format_timestamp(&mut writer)?;

        // part 2: level
        let metadata = event.metadata();
        let level_str = fmt_level(metadata.level());
        write!(&mut writer, "{} ", metadata.level())?;

        // part3: threadId:threadName
        let current_thread = std::thread::current();
        match current_thread.name() {
            Some(name) => {
                write!(writer, "[{:?}#{}]", current_thread.id(), name)?;
            }
            None => {
                write!(writer, "[{:?}]", current_thread.id())?;
            }
        }

        // part4: Trace ID
        event.metadata()

        // Format all the spans in the event's span context.
        if let Some(scope) = ctx.event_scope() {
            for span in scope.from_root() {
                write!(writer, "{}", span.name())?;

                // `FormattedFields` is a formatted representation of the span's
                // fields, which is stored in its extensions by the `fmt` layer's
                // `new_span` method. The fields will have been formatted
                // by the same field formatter that's provided to the event
                // formatter in the `FmtContext`.
                let ext = span.extensions();
                let fields = &ext
                    .get::<FormattedFields<N>>()
                    .expect("will never be `None`");

                // Skip formatting the fields if the span had no fields.
                if !fields.is_empty() {
                    write!(writer, "{{{}}}", fields)?;
                }
                write!(writer, ": ")?;
            }
        }

        // Write fields on the event
        ctx.field_format().format_fields(writer.by_ref(), event)?;

        writeln!(writer)
    }
}
