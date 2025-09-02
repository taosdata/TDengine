use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use arrow::array::{
    ArrayBuilder, ArrayRef, RecordBatch, StringBuilder, TimestampNanosecondBuilder,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use taosx_core::utils::codec;

use super::{
    client::Message,
    topic::{TopicParser, TopicPattern},
};

pub fn build_schema(topic_pattern: Option<&TopicPattern>) -> Schema {
    let mut fields = vec![
        Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
        Field::new("payload", DataType::Utf8, false),
    ];

    if let Some(keys) = topic_pattern.map(|p| {
        p.keys()
            .into_iter()
            .map(|k| Field::new(k, DataType::Utf8, false))
    }) {
        fields.extend(keys);
    }

    let meta = HashMap::from_iter([
        ("version".to_string(), "1.0".to_string()),
        ("stream".to_string(), "flat".to_string()),
        ("ack".to_string(), "lush".to_string()),
    ]);

    Schema::new_with_metadata(Vec::from_iter(fields), meta)
}

pub struct RecordBatchBuilder<P> {
    schema: Arc<Schema>,
    ts: TimestampNanosecondBuilder,
    // topic: StringBuilder,
    // qos: UInt8Builder,
    payload: StringBuilder,

    codec_err_count: usize,
    codec_processor: P,
    topic_parser: Option<TopicParser>,
}

impl<P> RecordBatchBuilder<P>
where
    P: codec::Processor,
{
    pub fn new(
        schema: Arc<Schema>,
        codec_processor: P,
        topic_pattern: Option<TopicPattern>,
        capacity: usize,
    ) -> Self {
        Self {
            schema,
            ts: TimestampNanosecondBuilder::with_capacity(capacity),
            // topic: StringBuilder::with_capacity(capacity, capacity * 20),
            // qos: UInt8Builder::with_capacity(capacity),
            payload: StringBuilder::with_capacity(capacity, capacity * 100),
            codec_err_count: 0,
            codec_processor,
            topic_parser: topic_pattern.map(TopicParser::new),
        }
    }

    pub fn build<I>(&mut self, messages: I) -> anyhow::Result<RecordBatch>
    where
        I: IntoIterator<Item = Message>,
    {
        let mut error = None;
        for message in messages {
            let payload = match self.codec_processor.process(message.payload.to_vec()) {
                Ok(payload) => {
                    self.codec_err_count = 0;
                    payload
                }
                Err(e) => {
                    tracing::error!("codec process message error: {e:#}");
                    self.codec_err_count += 1;
                    if self.codec_err_count < 3 {
                        error = Some(e);
                        continue;
                    }
                    return Err(e);
                }
            };
            match String::from_utf8(payload) {
                Ok(payload) => {
                    self.payload.append_value(payload);
                }
                Err(e) => {
                    tracing::error!(
                        "parse mqtt payload to string error, skip this message: {:?}",
                        bytes::Bytes::from_owner(e.into_bytes())
                    );
                    continue;
                }
            }
            if let Some(topic_parser) = self.topic_parser.as_mut() {
                topic_parser.append_value(&message.topic)?;
            }

            self.ts.append_value(message.ts);
        }
        if self.payload.is_empty() {
            if let Some(e) = error.take() {
                return Err(e);
            }
        }

        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(self.ts.finish()),
            // Arc::new(self.topic.finish()),
            // Arc::new(self.qos.finish()),
            Arc::new(self.payload.finish()),
        ];
        if let Some(topic_parser) = self.topic_parser.as_mut() {
            for array in topic_parser.finish() {
                columns.push(Arc::new(array))
            }
        }

        RecordBatch::try_new(self.schema.clone(), columns).context("build record batch error")
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{StringArray, TimestampNanosecondArray};
    use bytes::Bytes;

    use super::*;

    #[test]
    fn build_schema_test() -> anyhow::Result<()> {
        let pattern = "a/_/c".parse()?;
        let schema = build_schema(Some(&pattern));
        assert_eq!(
            schema,
            Schema::new_with_metadata(
                vec![
                    Field::new("ts", DataType::Timestamp(TimeUnit::Nanosecond, None), false),
                    Field::new("payload", DataType::Utf8, false),
                    Field::new("a", DataType::Utf8, false),
                    Field::new("c", DataType::Utf8, false),
                ],
                HashMap::from_iter([
                    ("version".to_string(), "1.0".to_string()),
                    ("stream".to_string(), "flat".to_string()),
                    ("ack".to_string(), "lush".to_string()),
                ])
            )
        );

        let schema = Arc::new(schema);
        let mut builder = RecordBatchBuilder::new(schema.clone(), (), Some(pattern), 1024);
        assert_eq!(
            builder.build([
                Message {
                    ts: 0,
                    topic: "a1/b1/c1".to_string(),
                    qos: 0,
                    payload: Bytes::from_static(b"{}"),
                },
                Message {
                    ts: 1,
                    topic: "a2/b2/c2".to_string(),
                    qos: 1,
                    payload: Bytes::from_static(b"{}"),
                },
            ])?,
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(TimestampNanosecondArray::from(vec![0, 1])),
                    Arc::new(StringArray::from(vec!["{}", "{}"])),
                    Arc::new(StringArray::from(vec!["a1", "a2"])),
                    Arc::new(StringArray::from(vec!["c1", "c2"]))
                ]
            )?
        );
        Ok(())
    }
}
