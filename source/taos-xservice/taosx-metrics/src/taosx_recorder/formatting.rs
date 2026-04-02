use metrics::Label;

pub fn write_type_line(buffer: &mut String, name: &str, metric_type: &str) {
    buffer.push_str("# TYPE ");
    buffer.push_str(name);
    buffer.push(' ');
    buffer.push_str(metric_type);
    buffer.push('\n');
}

pub fn write_metric_line<T>(
    buffer: &mut String,
    name: &str,
    suffix: Option<&'static str>,
    labels: Vec<Label>,
    value: T,
) where
    T: std::fmt::Display,
{
    buffer.push_str(name);
    if let Some(suffix) = suffix {
        buffer.push('_');
        buffer.push_str(suffix);
    }

    if !labels.is_empty() {
        buffer.push('{');

        let mut first = true;
        for label in labels {
            if first {
                first = false;
            } else {
                buffer.push(',');
            }
            let name = label.key();
            let value = label.value();
            buffer.push_str(name);
            buffer.push_str("=\"");
            buffer.push_str(value.to_string().as_str());
            buffer.push('"');
        }

        buffer.push('}');
    }

    buffer.push(' ');
    buffer.push_str(value.to_string().as_str());
    buffer.push('\n');
}
