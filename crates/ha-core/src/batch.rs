use std::sync::{Arc, LazyLock};

use anyhow::Context;
use arrow::array::{
    RecordBatch, StringArray, TimestampMillisecondArray, UInt64Array, timezone::Tz,
};
use arrow_schema::{ArrowError, DataType, Field, TimeUnit};

use crate::types::{Response, RpcRecord};

pub static SCHEMA: LazyLock<Arc<arrow_schema::Schema>> = LazyLock::new(|| {
    Arc::new(arrow_schema::Schema::new(vec![
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("action", DataType::Utf8, false),
        Field::new("context", DataType::Utf8, false),
        Field::new("req_id", DataType::UInt64, false),
    ]))
});

pub fn build_raw_batch(
    ts: i64,
    action: &str,
    context: &str,
    req_id: u64,
) -> Result<RecordBatch, ArrowError> {
    let schema = SCHEMA.clone();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(arrow::array::TimestampMillisecondArray::from(vec![ts])),
            Arc::new(arrow::array::StringArray::from(vec![action])),
            Arc::new(arrow::array::StringArray::from(vec![context])),
            Arc::new(arrow::array::UInt64Array::from(vec![req_id])),
        ],
    )
}

pub fn build_batch(action: &str, context: &str, req_id: u64) -> Result<RecordBatch, ArrowError> {
    build_raw_batch(
        chrono::Utc::now().timestamp_millis(),
        action,
        context,
        req_id,
    )
}

pub fn build_ok_batch(
    action: &str,
    context: impl serde::Serialize,
    req_id: u64,
) -> anyhow::Result<RecordBatch> {
    let context = serde_json::to_string(&Response::Data(context))
        .context("serialize build batch context error")?;
    build_batch(action, &context, req_id).context("build ok batch error")
}

pub fn build_failed_batch(
    action: &str,
    error: impl Into<String>,
    req_id: u64,
) -> anyhow::Result<RecordBatch> {
    let context = serde_json::to_string(&Response::<()>::Fail(error.into()))
        .context("serialize build batch context error")?;
    build_batch(action, &context, req_id).context("build ok batch error")
}

pub struct BatchIter<'a> {
    ts: &'a TimestampMillisecondArray,
    tz: Tz,

    action: &'a StringArray,
    context: &'a StringArray,
    req_id: &'a UInt64Array,

    rows: usize,
    current_row: usize,
}

impl<'a> BatchIter<'a> {
    pub fn new(batch: &'a RecordBatch) -> anyhow::Result<Self> {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .context("Failed to downcast timestamp column")?;
        let action = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("Failed to downcast action column")?;
        let context = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .context("Failed to downcast context column")?;
        let req_id = batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .context("Failed to downcast request ID column")?;

        let tz = ts
            .timezone()
            .unwrap_or("UTC")
            .parse()
            .context("Parse timezone error")?;

        Ok(Self {
            ts,
            tz,
            action,
            context,
            req_id,
            rows: batch.num_rows(),
            current_row: 0,
        })
    }
}

impl<'a> Iterator for BatchIter<'a> {
    type Item = RpcRecord<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row >= self.rows {
            return None;
        }
        let row = self.current_row;
        self.current_row += 1;
        Some(RpcRecord {
            ts: self.ts.value_as_datetime_with_tz(row, self.tz)?,
            action: self.action.value(row),
            context: self.context.value(row),
            req_id: self.req_id.value(row),
        })
    }
}
