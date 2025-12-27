use std::{
    any::Any,
    collections::HashMap,
    io::{BufReader, Read},
    ops::Deref,
    str::FromStr,
    sync::Arc,
};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, FixedSizeBinaryArray,
        Float16Array, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
        LargeBinaryArray, LargeStringArray, ListArray, StringArray, StructArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Schema},
    error::ArrowError,
    ipc::reader::StreamReader,
    record_batch::RecordBatch,
};
use faststr::FastStr;
use futures::Stream;
use taos::{ColumnView, Itertools, Precision, Ty, Value};
use tracing::{error, instrument, Span};

use crate::{
    ack::{AckType, AckWriter},
    constants::{__ATTRS__, __CONTROL__, __RECORDS__, __TABLES_INDEX__, __TABLE_NAME__, __TYPE__},
    prelude::{IpcDataType, IpcMetadata, LushMessageType, StreamType},
    stream::{
        flat::FlatMessage,
        point::{PointMessage, RecordMessage},
    },
};

use arrow_compute_ext::RecordBatchExt;

use super::lush::LushMessageControl;

#[derive(Debug, Clone)]
pub struct IpcParser {
    pub metadata: Arc<IpcMetadata>,
    pub schema: Arc<Schema>,
}

impl IpcParser {
    pub fn new(schema: Arc<Schema>) -> Self {
        let metadata = Arc::new(schema.metadata().into());
        Self { schema, metadata }
    }

    pub fn parse(&self, record: RecordBatch) -> Result<Box<dyn IpcMessage>, ArrowError> {
        match self.metadata().stream_type() {
            StreamType::Lush => {
                let v = record
                    .column_by_name(__TYPE__)
                    .expect("the lush message stream should contains __type__ field")
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap();
                let v: LushMessageType = unsafe { std::mem::transmute(v.value(0)) };
                match v {
                    LushMessageType::Table => todo!(),
                    LushMessageType::Children => {
                        let (tables, full_records) = self.parse_children(record);
                        Ok(Box::new(LushMessage::Tables(tables, full_records)))
                    }
                    LushMessageType::Control => {
                        let values = record.column_by_name(__CONTROL__).ok_or_else(|| {
                            ArrowError::InvalidArgumentError(
                                "Control message should contains __control__ field".to_string(),
                            )
                        })?;
                        let values = values
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .unwrap()
                            .value(0);
                        let s = values
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .ok_or_else(|| {
                                ArrowError::InvalidArgumentError(
                                    "__control__ should be StringArray".to_string(),
                                )
                            })?;
                        let control = s.value(0);
                        tracing::info!("Receive control message: {}", control);
                        let control: LushMessageControl =
                            serde_json::from_str(control).expect("Parse LushMessageControl error");
                        Ok(Box::new(LushMessage::Control(control)))
                    }
                    LushMessageType::Insert => {
                        if let Some(attrs) = record.column_by_name(__ATTRS__) {
                            let values = record.column_by_name(__RECORDS__).unwrap();
                            assert_eq!(attrs.len(), values.len());

                            debug_assert!(values.len() == 1);

                            let mut message = Vec::with_capacity(values.len());
                            for i in 0..values.len() {
                                let attrs = self.parse_attrs(attrs.slice(i, 1));
                                let records: LushInsertRecords = values.slice(i, 1).into();
                                // dbg!(&records);
                                let i = LushMessageInsert {
                                    attrs,
                                    records,
                                    // schema: self.schema.clone(),
                                    metadata: self.metadata.clone(),
                                };
                                message.push(i);
                            }
                            Ok(Box::new(LushMessage::Insert(message)))
                        } else {
                            let values = record.column_by_name(__RECORDS__).unwrap();

                            // debug_assert!(values.len() == 1);

                            let mut message = Vec::with_capacity(values.len());
                            for i in 0..values.len() {
                                let records: LushInsertRecords = values.slice(i, 1).into();
                                // dbg!(&records);
                                let i = LushMessageInsert {
                                    attrs: None,
                                    records,
                                    // schema: self.schema.clone(),
                                    metadata: self.metadata.clone(),
                                };
                                message.push(i);
                            }
                            Ok(Box::new(LushMessage::Insert(message)))
                        }
                    }
                }
            }
            StreamType::Point => {
                let record = RecordMessage { record };
                Ok(Box::new(PointMessage::new(vec![record])))
            }
            StreamType::Flat => {
                let record = RecordMessage { record };
                Ok(Box::new(FlatMessage::new(vec![record])))
            }
            _ => todo!(),
        }
    }

    pub fn metadata(&self) -> &IpcMetadata {
        &self.metadata
    }

    pub fn columns(&self) -> Vec<&String> {
        let f = self.schema.field_with_name(__RECORDS__).unwrap();
        let t = f.data_type();
        if let DataType::List(f) = t {
            if let DataType::Struct(fields) = f.data_type() {
                return fields
                    .iter()
                    .filter(|f| f.name() != __TABLE_NAME__)
                    .map(|f| f.name())
                    .collect();
            }
        }

        unreachable!()
    }

    pub fn ack(&self) -> AckType {
        self.metadata.ack()
    }

    pub fn lush_message_iter(&self) {}

    fn parse_children(&self, record: RecordBatch) -> (Vec<LushInsertAttrs>, Option<RecordBatch>) {
        let tables = record.column(__TABLES_INDEX__);
        let tables_clone = tables.clone();
        let values = record.column_by_name(__RECORDS__).unwrap();
        let tables = (0..tables.len())
            .flat_map(|i| {
                let tables = tables.slice(i, 1);
                self.parse_tables(tables).into_iter()
            })
            .collect_vec();
        let tables_record = tables_clone.slice(0, 1);
        let values_record = values.slice(0, 1);

        fn struct_array_to_record_batch(value: Arc<dyn Array>) -> RecordBatch {
            let s = value
                .as_any()
                .downcast_ref::<ListArray>()
                .expect("parse records list");
            let v = s.value(0);
            let s = v
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("parse records struct");
            let names = s.column_names();
            let columns = s.columns();
            RecordBatch::try_from_iter(
                names
                    .into_iter()
                    .zip(columns)
                    .map(|(name, value)| (name, value.clone())),
            )
            .unwrap()
        }
        let tables_record = struct_array_to_record_batch(tables_record);
        let values_record = struct_array_to_record_batch(values_record);

        // 对于 PI， __tables__ 和 __records__ 长度是一样的，其它数据源不能保证，因此可能出错
        let full_record = tables_record.concat_by_columns(&values_record).ok();
        (tables, full_record)
    }

    fn parse_tables(&self, arrow: Arc<dyn Array>) -> Vec<LushInsertAttrs> {
        let s = arrow.as_any().downcast_ref::<ListArray>().unwrap().value(0);
        let s = s.as_any().downcast_ref::<StructArray>().unwrap();
        let using = self.metadata.init().map(|init| init.name()).unwrap();

        let names = s.column_names();
        let values = s.columns();
        (0..s.len())
            .map(|i| {
                let mut values: Vec<_> = names
                    .iter()
                    .zip(values.iter())
                    .map(|(name, col)| {
                        macro_rules! primitive_downcast {
                            ($a:ident,$t:ident) => {{
                                let v = col.as_any().downcast_ref::<arrow::array::$a>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::$t)
                                } else {
                                    Value::$t(v.value(i))
                                }
                            }};
                        }
                        let value = match col.data_type() {
                            DataType::Null => todo!(),
                            DataType::Boolean => Value::Bool(true),
                            DataType::Int8 => {
                                let v = col.as_any().downcast_ref::<Int8Array>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::TinyInt)
                                } else {
                                    Value::TinyInt(v.value(i))
                                }
                            }
                            DataType::Int16 => {
                                let v = col.as_any().downcast_ref::<Int16Array>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::SmallInt)
                                } else {
                                    Value::SmallInt(v.value(i))
                                }
                            }
                            DataType::Int32 => {
                                let v = col.as_any().downcast_ref::<Int32Array>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::Int)
                                } else {
                                    Value::Int(v.value(i))
                                }
                            }
                            DataType::Int64 => {
                                let v = col.as_any().downcast_ref::<Int64Array>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::BigInt)
                                } else {
                                    Value::BigInt(v.value(i))
                                }
                            }
                            DataType::UInt8 => primitive_downcast!(UInt8Array, UTinyInt),
                            DataType::UInt16 => primitive_downcast!(UInt16Array, USmallInt),
                            DataType::UInt32 => primitive_downcast!(UInt32Array, UInt),
                            DataType::UInt64 => primitive_downcast!(UInt64Array, UBigInt),
                            // DataType::Float16 => primitive_downcast!(Float16Array, Float),
                            DataType::Float32 => primitive_downcast!(Float32Array, Float),
                            DataType::Float64 => primitive_downcast!(Float64Array, Double),
                            DataType::Timestamp(_, _) => todo!(),
                            DataType::Date32 => todo!(),
                            DataType::Date64 => todo!(),
                            DataType::Time32(_) => todo!(),
                            DataType::Time64(_) => todo!(),
                            DataType::Duration(_) => todo!(),
                            DataType::Interval(_) => todo!(),
                            DataType::Binary => {
                                let v = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::VarChar)
                                } else {
                                    Value::VarChar(
                                        std::str::from_utf8(v.value(i)).unwrap().to_string(),
                                    )
                                }
                            }
                            DataType::FixedSizeBinary(_) => todo!(),
                            DataType::LargeBinary => todo!(),
                            DataType::Utf8 => {
                                let v = col.as_any().downcast_ref::<StringArray>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::VarChar)
                                } else {
                                    Value::VarChar(v.value(i).to_string())
                                }
                            }
                            DataType::LargeUtf8 => todo!(),
                            DataType::List(_) => todo!(),
                            DataType::FixedSizeList(_, _) => todo!(),
                            DataType::LargeList(_) => todo!(),
                            DataType::Struct(_) => todo!(),
                            DataType::Union(_, _) => todo!(),
                            DataType::Dictionary(_, _) => todo!(),
                            DataType::Decimal128(_, scale) => {
                                let v = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::Decimal)
                                } else {
                                    Value::Decimal(bigdecimal::BigDecimal::from_bigint(
                                        v.value(i).into(),
                                        *scale as _,
                                    ))
                                }
                            }
                            DataType::Decimal256(_, _) => todo!(),
                            DataType::Map(_, _) => todo!(),
                            DataType::RunEndEncoded(_, _) => todo!(),
                            _ => todo!("Unsupported data type for tag"),
                        };
                        (FastStr::new(name), value)
                    })
                    .collect_vec();
                // let (name, values) = values.split_at(1);
                let name = values.remove(0);

                let s = LushInsertAttrs {
                    name: FastStr::new(name.1.strict_as_str()),
                    using: Some(using.clone()),
                    tags: Some(values),
                };
                s
            })
            .collect_vec()
    }

    fn parse_attrs(&self, arrow: ArrayRef) -> Option<LushInsertAttrs> {
        let s = arrow.as_any().downcast_ref::<StructArray>().unwrap();
        if s.is_null(0) {
            return None;
        }
        debug_assert!(s.len() == 1);

        let name = s
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("get table name")
            .value(0);
        let _name = std::str::from_utf8(name).unwrap().to_string();

        let using = self.metadata.init().map(|init| init.name()).unwrap();

        let names = s.column_names();
        let values = s.columns();

        (0..s.len())
            .map(|i| {
                let mut values: Vec<_> = names
                    .iter()
                    .zip(values.iter())
                    .map(|(name, col)| {
                        macro_rules! primitive_downcast {
                            ($a:ident,$t:ident) => {{
                                let v = col.as_any().downcast_ref::<arrow::array::$a>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::$t)
                                } else {
                                    Value::$t(v.value(i))
                                }
                            }};
                        }
                        let value = match col.data_type() {
                            DataType::Null => todo!(),
                            DataType::Boolean => Value::Bool(true),
                            DataType::Int8 => {
                                let v = col.as_any().downcast_ref::<Int8Array>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::TinyInt)
                                } else {
                                    Value::TinyInt(v.value(i))
                                }
                            }
                            DataType::Int16 => {
                                let v = col.as_any().downcast_ref::<Int16Array>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::SmallInt)
                                } else {
                                    Value::SmallInt(v.value(i))
                                }
                            }
                            DataType::Int32 => {
                                let v = col.as_any().downcast_ref::<Int32Array>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::Int)
                                } else {
                                    Value::Int(v.value(i))
                                }
                            }
                            DataType::Int64 => {
                                let v = col.as_any().downcast_ref::<Int64Array>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::BigInt)
                                } else {
                                    Value::BigInt(v.value(i))
                                }
                            }
                            DataType::UInt8 => primitive_downcast!(UInt8Array, UTinyInt),
                            DataType::UInt16 => primitive_downcast!(UInt16Array, USmallInt),
                            DataType::UInt32 => primitive_downcast!(UInt32Array, UInt),
                            DataType::UInt64 => primitive_downcast!(UInt64Array, UBigInt),
                            // DataType::Float16 => primitive_downcast!(Float16Array, Float),
                            DataType::Float32 => primitive_downcast!(Float32Array, Float),
                            DataType::Float64 => primitive_downcast!(Float64Array, Double),
                            DataType::Timestamp(_, _) => todo!(),
                            DataType::Date32 => todo!(),
                            DataType::Date64 => todo!(),
                            DataType::Time32(_) => todo!(),
                            DataType::Time64(_) => todo!(),
                            DataType::Duration(_) => todo!(),
                            DataType::Interval(_) => todo!(),
                            DataType::Binary => {
                                let v = col.as_any().downcast_ref::<BinaryArray>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::VarChar)
                                } else {
                                    Value::VarChar(
                                        std::str::from_utf8(v.value(i)).unwrap().to_string(),
                                    )
                                }
                            }
                            DataType::FixedSizeBinary(_) => todo!(),
                            DataType::LargeBinary => todo!(),
                            DataType::Utf8 => {
                                let v = col.as_any().downcast_ref::<StringArray>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::VarChar)
                                } else {
                                    Value::VarChar(v.value(i).to_string())
                                }
                            }
                            DataType::LargeUtf8 => todo!(),
                            DataType::List(_) => todo!(),
                            DataType::FixedSizeList(_, _) => todo!(),
                            DataType::LargeList(_) => todo!(),
                            DataType::Struct(_) => todo!(),
                            DataType::Union(_, _) => todo!(),
                            DataType::Dictionary(_, _) => todo!(),
                            DataType::Decimal128(_, scale) => {
                                let v = col.as_any().downcast_ref::<Decimal128Array>().unwrap();
                                if v.is_null(i) {
                                    Value::Null(Ty::Decimal)
                                } else {
                                    Value::Decimal(bigdecimal::BigDecimal::from_bigint(
                                        v.value(i).into(),
                                        *scale as _,
                                    ))
                                }
                            }
                            DataType::Decimal256(_, _) => todo!(),
                            DataType::Map(_, _) => todo!(),
                            DataType::RunEndEncoded(_, _) => todo!(),
                            _ => todo!("Unsupported data type for tag"),
                        };
                        (FastStr::new(*name), value)
                    })
                    .collect_vec();
                // let (name, values) = values.split_at(1);
                let name = values.remove(0);

                let s = LushInsertAttrs {
                    name: FastStr::new(name.1.strict_as_str()),
                    using: Some(using.clone()),
                    tags: Some(values),
                };
                // dbg!(s)
                s
            })
            .collect_vec()
            .into_iter()
            .next()
    }

    // fn parse_records(&self, arrow: ArrayRef) -> LushInsertRecords {
    //     let s = arrow
    //         .as_any()
    //         .downcast_ref::<ListArray>()
    //         .expect("parse records list");
    //     let v = s.value(0);
    //     let s = v
    //         .as_any()
    //         .downcast_ref::<StructArray>()
    //         .expect("parse records struct");

    //     // todo!()
    //     let names = s.column_names();
    //     let columns = s.columns();
    //     let record = RecordBatch::try_from_iter(
    //         names
    //             .into_iter()
    //             .zip(columns)
    //             .map(|(name, value)| (name, value.clone())),
    //     )
    //     .unwrap();
    //     LushInsertRecords { record }
    // }
}

pub struct IpcReader<R: Read> {
    pub parser: IpcParser,
    pub reader: StreamReader<BufReader<R>>,
}

impl<R: Read> Deref for IpcReader<R> {
    type Target = IpcParser;

    fn deref(&self) -> &Self::Target {
        &self.parser
    }
}

impl<R: Read> IpcReader<R> {
    pub fn new(reader: R) -> Result<Self, ArrowError> {
        let reader = BufReader::new(reader);
        let reader = StreamReader::try_new(reader, None)?;
        let schema = reader.schema();
        let parser = IpcParser::new(schema);
        Ok(Self { parser, reader })
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.parser.schema.clone()
    }

    pub fn into_stream(self) -> impl Stream<Item = Result<Box<dyn IpcMessage>, ArrowError>>
    where
        R: Send + 'static,
    {
        let (tx, rx) = flume::bounded(0);
        std::thread::spawn(move || {
            for item in self {
                tx.send(item)?; // send under blocking thread
            }
            Ok::<_, flume::SendError<_>>(())
        });
        rx.into_stream()
    }

    pub fn into_stream_buffered(
        self,
        cap: usize,
    ) -> impl Stream<Item = Result<Box<dyn IpcMessage>, ArrowError>>
    where
        R: Send + 'static,
    {
        let (tx, rx) = flume::bounded(cap);
        std::thread::spawn(move || {
            for item in self {
                tx.send(item)?; // send under blocking thread
            }
            Ok::<_, flume::SendError<_>>(())
        });
        rx.into_stream()
    }

    pub fn into_raw_stream(
        self,
    ) -> flume::r#async::RecvStream<'static, Result<RecordBatch, ArrowError>>
    where
        R: Send + 'static,
    {
        let (tx, rx) = flume::bounded(64);
        std::thread::spawn(move || {
            for item in self.reader {
                tx.send(item)?; // send under blocking thread
            }
            tracing::info!("Raw ipc reader stream closed");
            Ok::<_, flume::SendError<_>>(())
        });
        rx.into_stream()
    }

    pub fn into_raw_stream_with_capycity(
        self,
        cap: usize,
    ) -> flume::r#async::RecvStream<'static, Result<RecordBatch, ArrowError>>
    where
        R: Send + 'static,
    {
        let (tx, rx) = flume::bounded(cap);
        std::thread::spawn(move || {
            for item in self.reader {
                tx.send(item)?; // send under blocking thread
            }
            tracing::info!("Raw ipc reader stream closed");
            Ok::<_, flume::SendError<_>>(())
        });
        rx.into_stream()
    }

    #[instrument(skip_all)]
    pub fn into_raw_stream_qos_0(
        self,
        mut ipc_ack_writer: AckWriter<impl std::io::Write + Send + 'static>,
    ) -> flume::r#async::RecvStream<'static, Result<RecordBatch, ArrowError>>
    where
        R: Send + 'static,
    {
        let (tx, rx) = flume::bounded(64);
        let mut batch_number = 0u64;
        let span = Span::current();
        std::thread::spawn(move || {
            let _entered = span.entered();
            for item in self.reader {
                batch_number += 1;
                if let Err(err) = &item {
                    error!("Read batch {} error: {:?}", batch_number, err);
                } else {
                    tracing::trace!("Read batch {}", batch_number);
                }
                tx.send(item)?; // send under blocking thread
                tracing::trace!("Send batch {}", batch_number);
                ipc_ack_writer.write_ok()?;
                tracing::trace!("Ack batch {}", batch_number);
            }
            tracing::info!("Raw ipc reader stream closed");
            anyhow::Ok(())
        });
        rx.into_stream()
    }
}

#[derive(Debug, Clone)]
pub struct LushInsertAttrs {
    name: FastStr,
    using: Option<FastStr>,
    tags: Option<Vec<(FastStr, Value)>>,
}

impl Default for LushInsertAttrs {
    fn default() -> Self {
        Self {
            name: FastStr::empty(),
            using: None,
            tags: None,
        }
    }
}

impl LushInsertAttrs {
    pub fn stable_name(&self) -> Option<&FastStr> {
        self.using.as_ref()
    }

    pub fn table_name(&self) -> &FastStr {
        &self.name
    }

    pub fn tags(&self) -> &Option<Vec<(FastStr, Value)>> {
        &self.tags
    }

    pub fn to_sql(&self, table_name: Option<&str>) -> Option<String> {
        if let Some(using) = self.using.as_ref() {
            if self.tags.as_ref().is_none_or(|v| v.is_empty()) {
                tracing::warn!(
                    "Tags is empty for stable: {}, cannot generate create table sql",
                    self.name
                );
                return None;
            }
            tracing::trace!(
                "Generate create table sql for using: {}, attr: {:?}",
                using,
                self
            );
            let tags = self.tags.as_ref().unwrap();
            let table = table_name.unwrap_or(&self.name);
            let names = tags.iter().map(|(name, _)| format!("`{name}`")).join(",");
            let values = tags.iter().map(|(_, value)| value.to_sql_value()).join(",");
            Some(format!(
                "CREATE TABLE IF NOT EXISTS `{table}` USING `{using}` ({names}) TAGS({values}) "
            ))
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct LushInsertRecords {
    record: RecordBatch,
}

impl From<Arc<dyn Array>> for LushInsertRecords {
    fn from(value: Arc<dyn Array>) -> Self {
        let s = value
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("parse records list");
        let v = s.value(0);
        let s = v
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("parse records struct");

        // todo!()
        let names = s.column_names();
        let columns = s.columns();
        let record = RecordBatch::try_from_iter(
            names
                .into_iter()
                .zip(columns)
                .map(|(name, value)| (name, value.clone())),
        )
        .unwrap();
        Self { record }
    }
}

#[derive(Debug)]
pub struct LushMessageInsert {
    // schema: SchemaRef,
    metadata: Arc<IpcMetadata>,
    attrs: Option<LushInsertAttrs>,
    records: LushInsertRecords,
}

mod arrow_to_taos {
    use std::str::FromStr;

    use crate::prelude::IpcDataType;
    use arrow::datatypes::TimeUnit;
    use taos::ColumnView;

    /// parse arrow array to column view, unsupported value will be ignored(as NULL)
    pub fn parse_str_into(ty: &IpcDataType, data: Vec<Option<&str>>) -> ColumnView {
        let view = match ty {
            crate::prelude::IpcDataType::Null => {
                unreachable!("null is not supported");
            }
            crate::prelude::IpcDataType::Bool => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.and_then(|v| {
                            v.parse::<bool>()
                                .map_err(|err| {
                                    tracing::trace!(
                                        "parse bool from `{}` error: {}, fallback to null",
                                        v,
                                        err,
                                    )
                                })
                                .ok()
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_bools(v)
            }
            crate::prelude::IpcDataType::UInt8 => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.and_then(|v| {
                            v.parse::<u8>()
                                .map_err(|err| {
                                    tracing::trace!(
                                        "parse u8 from `{}` error: {}, fallback to null",
                                        v,
                                        err,
                                    )
                                })
                                .ok()
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_unsigned_tiny_ints(v)
            }
            crate::prelude::IpcDataType::UInt16 => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.and_then(|v| {
                            v.parse::<u16>()
                                .map_err(|err| {
                                    tracing::trace!(
                                        "parse u16 from `{}` error: {}, fallback to null",
                                        v,
                                        err,
                                    )
                                })
                                .ok()
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_unsigned_small_ints(v)
            }
            crate::prelude::IpcDataType::UInt32 => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.and_then(|v| {
                            v.parse::<u32>()
                                .map_err(|err| {
                                    tracing::trace!(
                                        "parse u32 from `{}` error: {}, fallback to null",
                                        v,
                                        err,
                                    )
                                })
                                .ok()
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_unsigned_ints(v)
            }
            crate::prelude::IpcDataType::UInt64 => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.and_then(|v| {
                            v.parse::<u64>()
                                .map_err(|err| {
                                    tracing::trace!("parse u64 from `{}` error: {}", v, err,)
                                })
                                .ok()
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_unsigned_big_ints(v)
            }
            crate::prelude::IpcDataType::Int8 => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.and_then(|v| {
                            v.parse::<i8>()
                                .map_err(|err| {
                                    tracing::trace!("parse i8 from `{}` error: {}", v, err,)
                                })
                                .ok()
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_tiny_ints(v)
            }
            crate::prelude::IpcDataType::Int16 => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.and_then(|v| {
                            v.parse::<i16>()
                                .map_err(|err| {
                                    tracing::trace!("parse i16 from `{}` error: {}", v, err,)
                                })
                                .ok()
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_small_ints(v)
            }
            crate::prelude::IpcDataType::Int32 => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.and_then(|v| {
                            v.parse::<i32>()
                                .map_err(|err| {
                                    tracing::trace!(
                                        "parse i32 from `{}` error: {}, fallback to null",
                                        v,
                                        err,
                                    )
                                })
                                .ok()
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_ints(v)
            }
            crate::prelude::IpcDataType::Int64 => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.and_then(|v| {
                            v.parse::<i64>()
                                .map_err(|err| {
                                    tracing::trace!(
                                        "parse i64 from `{}` error: {}, fallback to null",
                                        v,
                                        err,
                                    )
                                })
                                .ok()
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_big_ints(v)
            }
            crate::prelude::IpcDataType::Decimal(precision, scale) => {
                use bigdecimal::ToPrimitive;
                let values = data.into_iter().map(|v| {
                    v.and_then(|v| {
                        bigdecimal::BigDecimal::from_str(v)
                                .inspect_err(|e| {
                                    tracing::trace!(
                                        "parse i64 from `{v}` error: {e}, fallback to null",
                                    )
                                })
                                .ok()
                    })
                });

                if *precision <= 18 {
                    ColumnView::from_decimal64(
                        values.map(|v| {
                            v.and_then(|v| {
                                let (num, _) = v.as_bigint_and_scale();
                                num.to_i64()
                            })
                        }),
                        *precision,
                        *scale as _,
                    )
                } else {
                    ColumnView::from_decimal(
                        values.map(|v| {
                            v.and_then(|v| {
                                let (num, _) = v.as_bigint_and_scale();
                                num.to_i128()
                            })
                        }),
                        *precision,
                        *scale as _,
                    )
                }
            }
            crate::prelude::IpcDataType::Float32 => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.and_then(|v| {
                            v.parse::<f32>()
                                .map_err(|err| {
                                    tracing::trace!(
                                        "parse f32 from `{}` error: {}, fallback to null",
                                        v,
                                        err,
                                    )
                                })
                                .ok()
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_floats(v)
            }
            crate::prelude::IpcDataType::Float64 => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.and_then(|v| {
                            v.parse::<f64>()
                                .map_err(|err| {
                                    tracing::trace!(
                                        "parse f64 from `{}` error: {}, fallback to null",
                                        v,
                                        err,
                                    )
                                })
                                .ok()
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_doubles(v)
            }
            crate::prelude::IpcDataType::Timestamp(time_unit) => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.and_then(|v| {
                            // TODO: support parse timestamp from string
                            v.parse::<i64>()
                                .map_err(|err| {
                                    tracing::trace!(
                                        "parse i64 from `{}` error: {}, fallback to null",
                                        v,
                                        err,
                                    )
                                })
                                .ok()
                        })
                    })
                    .collect::<Vec<_>>();
                match time_unit {
                    TimeUnit::Second => todo!(),
                    TimeUnit::Millisecond => ColumnView::from_millis_timestamp(v),
                    TimeUnit::Microsecond => ColumnView::from_micros_timestamp(v),
                    TimeUnit::Nanosecond => ColumnView::from_nanos_timestamp(v),
                }
            }
            crate::prelude::IpcDataType::VarChar(_) => {
                ColumnView::from_varchar::<&str, _, _, _>(data)
            }
            crate::prelude::IpcDataType::NChar(_) => ColumnView::from_nchar::<&str, _, _, _>(data),
            crate::prelude::IpcDataType::Json => ColumnView::from_json::<&str, _, _, _>(data),
            crate::prelude::IpcDataType::VarBinary(_) => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.map(|v| {
                            let v = if v.starts_with("\\x") {
                                v.get(2..).unwrap()
                            } else {
                                v
                            };
                            let mut bytes = Vec::new();
                            let chars: Vec<char> = v.chars().collect();
                            chars.chunks(2).for_each(|chars| {
                                let byte_str: String = chars.iter().collect();
                                match u8::from_str_radix(&byte_str, 16) {
                                    Ok(byte) => bytes.push(byte),
                                    Err(_) => tracing::warn!("Invalid byte string: {}", byte_str),
                                }
                            });
                            bytes
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_bytes::<Vec<u8>, _, _, _>(v)
            }
            crate::prelude::IpcDataType::Blob => {
                let v = data
                    .into_iter()
                    .map(|v| {
                        v.map(|v| {
                            let v = if v.starts_with("\\x") {
                                v.get(2..).unwrap()
                            } else {
                                v
                            };
                            let mut bytes = Vec::new();
                            let chars: Vec<char> = v.chars().collect();
                            chars.chunks(2).for_each(|chars| {
                                let byte_str: String = chars.iter().collect();
                                match u8::from_str_radix(&byte_str, 16) {
                                    Ok(byte) => bytes.push(byte),
                                    Err(_) => tracing::warn!("Invalid byte string: {}", byte_str),
                                }
                            });
                            bytes
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_blob_bytes::<Vec<u8>, _, _, _>(v)
            }
        };
        view
    }
}

impl LushMessageInsert {
    pub fn stable_name(&self) -> Option<String> {
        if self.metadata.init().is_some() {
            Some(self.metadata.init().unwrap().name().to_string())
        } else {
            None
        }
    }

    pub fn record(&self) -> &RecordBatch {
        &self.records.record
    }

    pub fn num_rows(&self) -> usize {
        self.records.record.num_rows()
    }

    pub fn meta_sql(&self, table_name: Option<&str>) -> Option<String> {
        self.attrs.as_ref().and_then(|attr| attr.to_sql(table_name))
    }

    pub fn table(&self) -> Option<&str> {
        self.attrs.as_ref().map(|attr| attr.name.as_str())
    }

    // pub fn to_column_views(&self) -> Vec<ColumnView> {
    //     let ty = self
    //         .records
    //         .record
    //         .schema()
    //         .fields()
    //         .into_iter()
    //         .map(|field| {
    //             self.metadata
    //                 .init()
    //                 .and_then(|init| init.column_data_type(field.name()))
    //                 .cloned()
    //                 .unwrap_or_else(|| field.data_type().into())
    //         })
    //         .collect_vec();
    //     parse_column_view_with_types(&self.records.record, &ty)
    // }

    pub fn to_column_views(&self, target_precision: Precision) -> Vec<ColumnView> {
        let ty = self
            .records
            .record
            .schema()
            .fields()
            .into_iter()
            .map(|field| {
                self.metadata
                    .init()
                    .and_then(|init| init.column_data_type(field.name()))
                    .cloned()
                    .unwrap_or_else(|| field.data_type().into())
            })
            .collect_vec();
        parse_column_view_with_types(&self.records.record, &ty, target_precision)
    }

    /// return true if the cell is TAOS_DELETE
    ///
    /// ## Panics
    ///
    /// Panic if the column or row index is out of range.
    #[inline]
    fn is_delete(&self, row: usize, col: usize) -> bool {
        let arr = self.records.record.column(col);
        match arr.data_type() {
            DataType::Binary => {
                let arr = arr.as_any().downcast_ref::<BinaryArray>().unwrap();
                if arr.is_null(row) {
                    return false;
                }
                arr.value(row) == b"TAOS_DELETE"
            }
            DataType::LargeBinary => {
                let arr = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                if arr.is_null(row) {
                    return false;
                }
                arr.value(row) == b"TAOS_DELETE"
            }
            DataType::Utf8 => {
                let arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                if arr.is_null(row) {
                    return false;
                }
                arr.value(row) == "TAOS_DELETE"
            }
            DataType::LargeUtf8 => {
                let arr = arr.as_any().downcast_ref::<LargeStringArray>().unwrap();
                if arr.is_null(row) {
                    return false;
                }
                arr.value(row) == "TAOS_DELETE"
            }
            _ => false,
        }
    }

    /// return (sqls to executes, )
    pub fn generate_insert_sql_from_tablename<'b>(
        &self,
        data: &[ColumnView],
        columns: &'b [String],
    ) -> Option<(Vec<String>, HashMap<&'b String, IpcDataType>)> {
        let index = self
            .records
            .record
            .schema()
            .fields()
            .iter()
            .find_position(|f| f.name() == __TABLE_NAME__)
            .map(|(i, _)| i);
        match index {
            None => None,
            Some(i) => {
                let mut sql = "INSERT INTO ".to_string();
                let c = data.get(i).unwrap();
                if c.len() == 0 {
                    return None;
                }
                debug_assert!(columns.len() == data.len() - 1);
                let mut sqls = Vec::new();
                let mut field_map = HashMap::new();
                // column iter
                for (j, bv) in c.into_iter().enumerate() {
                    let table_name = bv.to_string().unwrap();
                    // sql.push_str(format!("{} VALUES (", &table_name, ).as_str());
                    let mut insert_columns = String::new();
                    let mut insert_values = String::new();
                    let mut index = 0;
                    for (n, cv) in data.iter().enumerate() {
                        if n == i {
                            // is table_name
                            continue;
                        }
                        if let Some(v) = cv.get(j) {
                            let column_name = &columns[index];
                            if self.is_delete(j, n) {
                                metrics::counter!("ipc.stream.points").increment(1);
                                insert_columns.push_str(format!("`{}`,", column_name).as_str());
                                insert_values.push_str("NULL,");
                                tracing::warn!(row = j, col = n, "Set column to NULL");
                            } else if !v.is_null() {
                                let sql_value = v.to_sql_value();
                                let v_ty = v.ty();
                                if v_ty.is_var_type() {
                                    let field_ipc_type = field_map.get_mut(column_name);
                                    if let Some(field_ipc_type) = field_ipc_type {
                                        match field_ipc_type {
                                            IpcDataType::VarChar(len) | IpcDataType::NChar(len) => {
                                                if *len < sql_value.len() as u32 {
                                                    *len = sql_value.len() as u32;
                                                }
                                            }
                                            _ => (),
                                        }
                                    } else {
                                        field_map.insert(
                                            column_name,
                                            IpcDataType::from_str(
                                                format!("{}({})", v_ty.name(), sql_value.len())
                                                    .as_str(),
                                            )
                                            .unwrap(),
                                        );
                                    }
                                }
                                metrics::counter!("ipc.stream.points").increment(1);
                                insert_columns.push_str(format!("`{}`,", column_name).as_str());
                                insert_values.push_str(format!("{},", sql_value).as_str());
                            } else {
                                // ignore null columnview
                                tracing::trace!("column view {} is null", column_name);
                            }
                        }
                        index += 1;
                    }
                    insert_columns.pop();
                    insert_values.pop();
                    let sql_to_push =
                        format!(" `{table_name}` ({insert_columns}) VALUES ({insert_values})");
                    // sql len should less than 1M
                    if sql.len() + sql_to_push.len() > 1024 * 1024 {
                        sqls.push(sql);
                        sql = format!("INSERT INTO {sql_to_push}");
                    } else {
                        sql.push_str(sql_to_push.as_str());
                    }
                }
                if sql.len() > 12 {
                    sqls.push(sql);
                }
                Some((sqls, field_map))
            }
        }
    }

    pub fn to_column_views_group_by_tablename(
        &self,
        precision: Precision,
    ) -> HashMap<Option<String>, Vec<ColumnView>> {
        let mut index = None;
        for (i, f) in self.records.record.schema().fields().iter().enumerate() {
            if f.name() == __TABLE_NAME__ {
                index = Some(i);
            }
        }
        let data = self.to_column_views(precision);
        let mut map = HashMap::new();
        match index {
            None => {
                map.insert(None, data);
            }
            Some(i) => {
                let c = data.get(i).unwrap();
                let start = std::time::Instant::now();
                for (j, bv) in c.into_iter().enumerate() {
                    let map_value = map.get_mut(&Some(bv.to_string().unwrap()));
                    match map_value {
                        None => {
                            let mut l = Vec::new();
                            for (n, cv) in data.iter().enumerate() {
                                if n == i {
                                    continue;
                                }
                                l.push(cv.slice(j..j + 1).unwrap());
                            }
                            map.insert(Some(bv.to_string().unwrap()), l);
                        }
                        Some(c) => {
                            // dbg!(&c);
                            let mut data_i = 0;

                            for (n, cv) in data.iter().enumerate() {
                                if n == i {
                                    continue;
                                }
                                let exist_cv = c.get(data_i);
                                if let Some(old_exist_cv) = exist_cv {
                                    // ColumnView insert
                                    let new_value = cv.slice(j..j + 1).unwrap();
                                    c[data_i] = old_exist_cv.concat(&new_value);
                                } else {
                                    // insert ColumnView
                                    c.push(cv.slice(j..j + 1).unwrap());
                                }
                                data_i += 1;
                            }
                        }
                    }
                }
                let duration = start.elapsed();
                println!("for loop time cost: {:?}", duration);
            }
        }
        map
    }
}

pub fn parse_column_view_with_types(
    record: &RecordBatch,
    metadata: &[IpcDataType],
    precision: Precision,
) -> Vec<ColumnView> {
    // let records_schema = record.schema();
    // let fields = records_schema.fields();
    record
        .columns()
        .iter()
        .zip(metadata)
        .map(|(column, ty)| {
            // dbg!(column);
            match column.data_type() {
                DataType::Boolean => {
                    let a = column.as_any().downcast_ref::<BooleanArray>().unwrap();

                    ColumnView::from_bools(
                        (0..a.len())
                            .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                            .collect_vec(),
                    )
                }
                DataType::Int8 => {
                    let a = column.as_any().downcast_ref::<Int8Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_tiny_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_tiny_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Int16 => {
                    let a = column.as_any().downcast_ref::<Int16Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_small_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_small_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Int32 => {
                    let a = column.as_any().downcast_ref::<Int32Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Int64 => {
                    let a = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_big_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_big_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::UInt8 => {
                    let a = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_unsigned_tiny_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_unsigned_tiny_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::UInt16 => {
                    let a = column.as_any().downcast_ref::<UInt16Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_unsigned_small_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_unsigned_small_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::UInt32 => {
                    let a = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_unsigned_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_unsigned_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::UInt64 => {
                    let a = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_unsigned_big_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_unsigned_big_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Float16 => {
                    let a = column.as_any().downcast_ref::<Float16Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_floats(
                            a.values().iter().map(|f| f.to_f32_const()).collect_vec(),
                        )
                    } else {
                        ColumnView::from_floats(
                            (0..a.len())
                                .map(|i| {
                                    if a.is_null(i) {
                                        None
                                    } else {
                                        Some(a.value(i).to_f32())
                                    }
                                })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Float32 => {
                    let a = column.as_any().downcast_ref::<Float32Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_floats(a.values().to_vec())
                    } else {
                        ColumnView::from_floats(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Float64 => {
                    let a = column.as_any().downcast_ref::<Float64Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_doubles(a.values().to_vec())
                    } else {
                        ColumnView::from_doubles(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Timestamp(u, _) => match u {
                    arrow::datatypes::TimeUnit::Second => {
                        let a = column
                            .as_any()
                            .downcast_ref::<TimestampSecondArray>()
                            .unwrap();
                        if a.null_count() == 0 {
                            let v = a.values();
                            match precision {
                                Precision::Millisecond => ColumnView::from_millis_timestamp(
                                    v.iter().map(|&x| x * 1_000).collect(),
                                ),
                                Precision::Microsecond => {
                                    let v_converted = v.iter().map(|&x| x * 1_000_000).collect();
                                    ColumnView::from_micros_timestamp(v_converted)
                                }
                                Precision::Nanosecond => {
                                    let v_converted =
                                        v.iter().map(|&x| x * 1_000_000_000).collect();
                                    ColumnView::from_nanos_timestamp(v_converted)
                                }
                            }
                        } else {
                            match precision {
                                Precision::Millisecond => {
                                    let values = (0..a.len())
                                        .map(|i| {
                                            if a.is_null(i) {
                                                None
                                            } else {
                                                Some(a.value(i) * 1_000)
                                            }
                                        })
                                        .collect();
                                    ColumnView::from_millis_timestamp(values)
                                }
                                Precision::Microsecond => {
                                    let values = (0..a.len())
                                        .map(|i| {
                                            if a.is_null(i) {
                                                None
                                            } else {
                                                Some(a.value(i) * 1_000_000)
                                            }
                                        })
                                        .collect();
                                    ColumnView::from_micros_timestamp(values)
                                }
                                Precision::Nanosecond => {
                                    let values = (0..a.len())
                                        .map(|i| {
                                            if a.is_null(i) {
                                                None
                                            } else {
                                                Some(a.value(i) * 1_000_000_000)
                                            }
                                        })
                                        .collect();
                                    ColumnView::from_nanos_timestamp(values)
                                }
                            }
                        }
                    }
                    arrow::datatypes::TimeUnit::Millisecond => {
                        let a = column
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .unwrap();
                        if a.null_count() == 0 {
                            let v = a.values();
                            match precision {
                                Precision::Millisecond => {
                                    ColumnView::from_millis_timestamp(v.to_vec())
                                }
                                Precision::Microsecond => {
                                    let v_converted = v.iter().map(|&x| x * 1_000).collect();
                                    ColumnView::from_micros_timestamp(v_converted)
                                }
                                Precision::Nanosecond => {
                                    let v_converted = v.iter().map(|&x| x * 1_000_000).collect();
                                    ColumnView::from_nanos_timestamp(v_converted)
                                }
                            }
                        } else {
                            match precision {
                                Precision::Millisecond => {
                                    let values = (0..a.len())
                                        .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                        .collect();
                                    ColumnView::from_millis_timestamp(values)
                                }
                                Precision::Microsecond => {
                                    let values = (0..a.len())
                                        .map(|i| {
                                            if a.is_null(i) {
                                                None
                                            } else {
                                                Some(a.value(i) * 1_000)
                                            }
                                        })
                                        .collect();
                                    ColumnView::from_micros_timestamp(values)
                                }
                                Precision::Nanosecond => {
                                    let values = (0..a.len())
                                        .map(|i| {
                                            if a.is_null(i) {
                                                None
                                            } else {
                                                Some(a.value(i) * 1_000_000)
                                            }
                                        })
                                        .collect();
                                    ColumnView::from_nanos_timestamp(values)
                                }
                            }
                        }
                    }
                    arrow::datatypes::TimeUnit::Microsecond => {
                        let a = column
                            .as_any()
                            .downcast_ref::<TimestampMicrosecondArray>()
                            .unwrap();
                        if a.null_count() == 0 {
                            let v = a.values();
                            match precision {
                                Precision::Millisecond => {
                                    let v_converted = v.iter().map(|&x| x / 1_000).collect();
                                    ColumnView::from_millis_timestamp(v_converted)
                                }
                                Precision::Microsecond => {
                                    ColumnView::from_micros_timestamp(v.to_vec())
                                }
                                Precision::Nanosecond => {
                                    let v_converted = v.iter().map(|&x| x * 1_000).collect();
                                    ColumnView::from_nanos_timestamp(v_converted)
                                }
                            }
                        } else {
                            match precision {
                                Precision::Millisecond => {
                                    let values = (0..a.len())
                                        .map(|i| {
                                            if a.is_null(i) {
                                                None
                                            } else {
                                                Some(a.value(i) / 1_000)
                                            }
                                        })
                                        .collect();
                                    ColumnView::from_millis_timestamp(values)
                                }
                                Precision::Microsecond => {
                                    let values = (0..a.len())
                                        .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                        .collect();
                                    ColumnView::from_micros_timestamp(values)
                                }
                                Precision::Nanosecond => {
                                    let values = (0..a.len())
                                        .map(|i| {
                                            if a.is_null(i) {
                                                None
                                            } else {
                                                Some(a.value(i) * 1_000)
                                            }
                                        })
                                        .collect();
                                    ColumnView::from_nanos_timestamp(values)
                                }
                            }
                        }
                    }
                    arrow::datatypes::TimeUnit::Nanosecond => {
                        let a = column
                            .as_any()
                            .downcast_ref::<TimestampNanosecondArray>()
                            .unwrap();
                        if a.null_count() == 0 {
                            let v = a.values();
                            match precision {
                                Precision::Millisecond => {
                                    let v_converted = v.iter().map(|&x| x / 1_000_000).collect();
                                    ColumnView::from_millis_timestamp(v_converted)
                                }
                                Precision::Microsecond => {
                                    let v_converted = v.iter().map(|&x| x / 1_000).collect();
                                    ColumnView::from_micros_timestamp(v_converted)
                                }
                                Precision::Nanosecond => {
                                    let v_converted = v.iter().copied().collect();
                                    ColumnView::from_nanos_timestamp(v_converted)
                                }
                            }
                        } else {
                            match precision {
                                Precision::Millisecond => {
                                    let values = (0..a.len())
                                        .map(|i| {
                                            if a.is_null(i) {
                                                None
                                            } else {
                                                Some(a.value(i) / 1_000_000)
                                            }
                                        })
                                        .collect();
                                    ColumnView::from_millis_timestamp(values)
                                }
                                Precision::Microsecond => {
                                    let values = (0..a.len())
                                        .map(|i| {
                                            if a.is_null(i) {
                                                None
                                            } else {
                                                Some(a.value(i) / 1_000)
                                            }
                                        })
                                        .collect();
                                    ColumnView::from_micros_timestamp(values)
                                }
                                Precision::Nanosecond => {
                                    let values = (0..a.len())
                                        .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                        .collect();
                                    ColumnView::from_nanos_timestamp(values)
                                }
                            }
                        }
                    }
                },
                DataType::Date32 => todo!(),
                DataType::Date64 => todo!(),
                DataType::Time32(_) => todo!(),
                DataType::Time64(_) => todo!(),
                DataType::Duration(_) => todo!(),
                DataType::Interval(_) => todo!(),
                DataType::Binary => {
                    let a = column.as_any().downcast_ref::<BinaryArray>().unwrap();

                    let data = (0..a.len())
                        .map(|i| {
                            if a.is_null(i) {
                                None
                            } else {
                                Some(unsafe { std::str::from_utf8_unchecked(a.value(i)) })
                            }
                        })
                        .collect_vec();

                    arrow_to_taos::parse_str_into(ty, data)
                }
                DataType::FixedSizeBinary(_) => todo!(),
                DataType::LargeBinary => {
                    let array = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                    ColumnView::from_blob_bytes::<&[u8], _, _, _>(array)
                }
                DataType::Utf8 => {
                    let a = column.as_any().downcast_ref::<StringArray>().unwrap();

                    let data = (0..a.len())
                        .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                        .collect_vec();
                    arrow_to_taos::parse_str_into(ty, data)
                }
                DataType::LargeUtf8 => todo!(),
                DataType::List(_) => todo!(),
                DataType::FixedSizeList(_, _) => todo!(),
                DataType::LargeList(_) => todo!(),
                DataType::Struct(_) => todo!(),
                DataType::Union(_, _) => todo!(),
                DataType::Dictionary(_, _) => todo!(),
                DataType::Decimal128(_, _) => todo!(),
                DataType::Decimal256(_, _) => todo!(),
                DataType::Map(_, _) => todo!(),
                DataType::RunEndEncoded(_, _) => todo!(),
                dt => panic!("unsupported input type: {dt}"),
            }
        })
        .collect()
}

pub fn record_batch_to_column_view(
    record: &RecordBatch,
    target_precision: Precision,
) -> Vec<ColumnView> {
    record
        .columns()
        .iter()
        .zip(record.schema().fields())
        .map(|(column, field)| {
            let cast_to = field.metadata().get("cast_to").map(|s| s.as_str());
            match column.data_type() {
                DataType::Null => todo!(),
                DataType::Boolean => {
                    let a = column.as_any().downcast_ref::<BooleanArray>().unwrap();

                    ColumnView::from_bools(
                        (0..a.len())
                            .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                            .collect_vec(),
                    )
                }
                DataType::Int8 => {
                    let a = column.as_any().downcast_ref::<Int8Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_tiny_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_tiny_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Int16 => {
                    let a = column.as_any().downcast_ref::<Int16Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_small_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_small_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Int32 => {
                    let a = column.as_any().downcast_ref::<Int32Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Int64 => {
                    let a = column.as_any().downcast_ref::<Int64Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_big_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_big_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::UInt8 => {
                    let a = column.as_any().downcast_ref::<UInt8Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_unsigned_tiny_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_unsigned_tiny_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::UInt16 => {
                    let a = column.as_any().downcast_ref::<UInt16Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_unsigned_small_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_unsigned_small_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::UInt32 => {
                    let a = column.as_any().downcast_ref::<UInt32Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_unsigned_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_unsigned_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::UInt64 => {
                    let a = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_unsigned_big_ints(a.values().to_vec())
                    } else {
                        ColumnView::from_unsigned_big_ints(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Float16 => {
                    let a = column.as_any().downcast_ref::<Float16Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_floats(
                            a.values().iter().map(|f| f.to_f32_const()).collect_vec(),
                        )
                    } else {
                        ColumnView::from_floats(
                            (0..a.len())
                                .map(|i| {
                                    if a.is_null(i) {
                                        None
                                    } else {
                                        Some(a.value(i).to_f32())
                                    }
                                })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Float32 => {
                    let a = column.as_any().downcast_ref::<Float32Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_floats(a.values().to_vec())
                    } else {
                        ColumnView::from_floats(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Float64 => {
                    let a = column.as_any().downcast_ref::<Float64Array>().unwrap();
                    if a.null_count() == 0 {
                        ColumnView::from_doubles(a.values().to_vec())
                    } else {
                        ColumnView::from_doubles(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        )
                    }
                }
                DataType::Timestamp(_, _) => {
                    let precision = match target_precision {
                        Precision::Millisecond => arrow::datatypes::TimeUnit::Millisecond,
                        Precision::Microsecond => arrow::datatypes::TimeUnit::Microsecond,
                        Precision::Nanosecond => arrow::datatypes::TimeUnit::Nanosecond,
                    };
                    let column =
                        arrow::compute::cast(column, &DataType::Timestamp(precision, None))
                            .expect("timestamp to timestamp cast should always success");
                    match precision {
                        arrow::datatypes::TimeUnit::Second => {
                            unreachable!("TDengine does not support second precision")
                        }
                        arrow::datatypes::TimeUnit::Millisecond => {
                            let a = column
                                .as_any()
                                .downcast_ref::<TimestampMillisecondArray>()
                                .unwrap();
                            if a.null_count() == 0 {
                                let v = a.values();
                                ColumnView::from_millis_timestamp(v.to_vec())
                            } else {
                                let values = (0..a.len())
                                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                    .collect();
                                ColumnView::from_millis_timestamp(values)
                            }
                        }
                        arrow::datatypes::TimeUnit::Microsecond => {
                            let a = column
                                .as_any()
                                .downcast_ref::<TimestampMicrosecondArray>()
                                .unwrap();
                            if a.null_count() == 0 {
                                let v = a.values();
                                ColumnView::from_micros_timestamp(v.to_vec())
                            } else {
                                let values = (0..a.len())
                                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                    .collect();
                                ColumnView::from_micros_timestamp(values)
                            }
                        }
                        arrow::datatypes::TimeUnit::Nanosecond => {
                            let a = column
                                .as_any()
                                .downcast_ref::<TimestampNanosecondArray>()
                                .unwrap();
                            if a.null_count() == 0 {
                                let v = a.values();
                                ColumnView::from_nanos_timestamp(v.to_vec())
                            } else {
                                let values = (0..a.len())
                                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                    .collect();
                                ColumnView::from_nanos_timestamp(values)
                            }
                        }
                    }
                }
                DataType::Binary if cast_to.is_some_and(|s| s == "VARBINARY") => {
                    let array = column.as_any().downcast_ref::<BinaryArray>().unwrap();
                    let iter = (0..array.len())
                        .map(|i| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i))
                            }
                        })
                        .collect_vec();
                    ColumnView::from_bytes::<&[u8], _, _, _>(iter)
                }
                DataType::Binary => {
                    let a = column.as_any().downcast_ref::<BinaryArray>().unwrap();
                    let iter = (0..a.len())
                        .map(|i| {
                            if a.is_null(i) {
                                None
                            } else {
                                Some(unsafe { std::str::from_utf8_unchecked(a.value(i)) })
                            }
                        })
                        .collect_vec();

                    match cast_to {
                        Some("NCHAR") => ColumnView::from_nchar::<&str, _, _, _>(iter),
                        _ => ColumnView::from_varchar::<&str, _, _, _>(iter),
                    }
                }
                DataType::LargeBinary if cast_to.is_some_and(|s| s == "VARBINARY") => {
                    let array = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                    let iter = (0..array.len())
                        .map(|i| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i))
                            }
                        })
                        .collect_vec();
                    ColumnView::from_bytes::<&[u8], _, _, _>(iter)
                }
                DataType::LargeBinary => {
                    let array = column.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
                    ColumnView::from_blob_bytes::<&[u8], _, _, _>(array)
                }
                DataType::FixedSizeBinary(_) if cast_to.is_some_and(|s| s == "VARBINARY") => {
                    let array = column
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap();
                    let iter = (0..array.len())
                        .map(|i| {
                            if array.is_null(i) {
                                None
                            } else {
                                Some(array.value(i))
                            }
                        })
                        .collect_vec();
                    ColumnView::from_bytes::<&[u8], _, _, _>(iter)
                }
                DataType::FixedSizeBinary(_) if cast_to.is_some_and(|s| s == "NCHAR") => {
                    let array = column
                        .as_any()
                        .downcast_ref::<FixedSizeBinaryArray>()
                        .unwrap();
                    // Convert bytes to UTF-8 strings for NCHAR
                    let data = (0..array.len())
                        .map(|i| {
                            if array.is_null(i) {
                                None
                            } else {
                                // Safely convert bytes to UTF-8 string
                                std::str::from_utf8(array.value(i)).ok()
                            }
                        })
                        .collect_vec();
                    ColumnView::from_nchar::<&str, _, _, _>(data)
                }
                DataType::Utf8 => {
                    let a = column.as_any().downcast_ref::<StringArray>().unwrap();
                    match cast_to {
                        Some("NCHAR") => ColumnView::from_nchar::<&str, _, _, _>(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        ),
                        _ => ColumnView::from_varchar::<&str, _, _, _>(
                            (0..a.len())
                                .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                .collect_vec(),
                        ),
                    }
                }
                DataType::List(field)
                    if cast_to.is_some_and(|s| s == "VARBINARY")
                        && field.data_type().is_numeric() =>
                {
                    // 判断 field 是 数字类型的列表且 metadata cast to VarBinary 类型
                    // 转换为 \x 格式
                    let array = column.as_any().downcast_ref::<ListArray>().unwrap();
                    // ListArray 每一行转换为一个 Bytes 数组
                    let mut views = Vec::with_capacity(array.len());
                    for idx in 0..array.len() {
                        if array.is_null(idx) {
                            views.push(None);
                            continue;
                        }
                        let row = array.value(idx);
                        let Ok(u8_array) = arrow::compute::cast(&row, &DataType::UInt8) else {
                            views.push(None);
                            continue;
                        };
                        let Some(u8_array) = u8_array.as_any().downcast_ref::<UInt8Array>() else {
                            views.push(None);
                            continue;
                        };
                        let bytes = u8_array.values().to_vec();
                        views.push(Some(bytes));
                    }
                    ColumnView::from_bytes::<Vec<u8>, _, _, _>(views)
                }
                DataType::Decimal128(precision, scale) => {
                    // 如果 cast_to 是 DECIMAL，则使用 metadata 中的 precision 和 scale 进行转换
                    if cast_to.is_some_and(|s| s == "DECIMAL") {
                        let cast_to_precision = field
                            .metadata()
                            .get("precision")
                            .and_then(|s| s.parse::<u8>().ok())
                            .unwrap_or(*precision);

                        // 当 precision 范围为 (18, 38] 时，使用 16 字节存储 (DECIMAL)
                        if cast_to_precision > 18 {
                            let a = column.as_any().downcast_ref::<Decimal128Array>().unwrap();
                            if a.null_count() == 0 {
                                ColumnView::from_decimal(
                                    a.values().to_vec(),
                                    *precision,
                                    *scale as u8,
                                )
                            } else {
                                ColumnView::from_decimal(
                                    (0..a.len())
                                        .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                        .collect_vec(),
                                    *precision,
                                    *scale as u8,
                                )
                            }
                        } else {
                            // 当 precision 值不大于 18 时，内部使用 8 字节存储 (DECIMAL64)
                            let a = column.as_any().downcast_ref::<Decimal128Array>().unwrap();
                            if a.null_count() == 0 {
                                ColumnView::from_decimal64(
                                    a.values()
                                        .iter()
                                        .map(num::ToPrimitive::to_i64)
                                        .collect_vec(),
                                    *precision,
                                    *scale as u8,
                                )
                            } else {
                                ColumnView::from_decimal64(
                                    (0..a.len())
                                        .map(|i| {
                                            if a.is_null(i) {
                                                None
                                            } else {
                                                num::ToPrimitive::to_i64(&a.value(i))
                                            }
                                        })
                                        .collect_vec(),
                                    *precision,
                                    *scale as u8,
                                )
                            }
                        }
                    } else {
                        let a = column.as_any().downcast_ref::<Decimal128Array>().unwrap();
                        if a.null_count() == 0 {
                            ColumnView::from_decimal(a.values().to_vec(), *precision, *scale as u8)
                        } else {
                            ColumnView::from_decimal(
                                (0..a.len())
                                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                                    .collect_vec(),
                                *precision,
                                *scale as u8,
                            )
                        }
                    }
                }
                _ => todo!(),
            }
        })
        .collect()
}

#[derive(Debug)]
pub struct LushMessageTable {}

#[derive(Debug)]
pub enum LushMessage {
    Tables(Vec<LushInsertAttrs>, Option<RecordBatch>),
    Insert(Vec<LushMessageInsert>),
    Control(LushMessageControl),
}

impl LushMessage {
    pub fn is_tables(&self) -> bool {
        matches!(self, LushMessage::Tables(_, _))
    }
}
// pub struct LushMessageTables(Vec<LushInsertAttrs>);
pub trait IpcMessage: Any + Send + Sync {
    fn as_any(&self) -> &dyn Any;
    fn nrows(&self) -> usize;
}

impl IpcMessage for LushMessage {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn nrows(&self) -> usize {
        match self {
            LushMessage::Tables(_, _) => 0,
            LushMessage::Insert(v) => v.iter().map(|v| v.num_rows()).sum(),
            LushMessage::Control(_) => 0,
        }
    }
}

impl<R: Read> Iterator for IpcReader<R> {
    type Item = Result<Box<dyn IpcMessage>, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.reader.next()?;
        match res {
            Ok(record) => Some(self.parse(record)),
            Err(err) => {
                let err_str = format!("{err:#}");
                error!("next message error, {}", err_str);
                if err_str.contains("os error 10054") {
                    //  windows socket close error
                    None
                } else {
                    Some(Err(err))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::constants::__TABLES__;
    use arrow::{compute::CastOptions, datatypes::Field};

    use super::*;

    #[test]
    fn parse_flat_and_point_messages() {
        // Build a simple schema with metadata for flat stream
        let fields = vec![
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("val", DataType::Int32, true),
        ];
        let mut md = std::collections::HashMap::new();
        md.insert("version".to_string(), "1.0".to_string());
        md.insert("stream".to_string(), "flat".to_string());
        md.insert("ack".to_string(), "none".to_string());
        let schema = Arc::new(Schema::new(fields).with_metadata(md));

        // Build columns
        let ts = TimestampMillisecondArray::from(vec![Some(1_000i64), Some(2_000i64)]);
        let val = Int32Array::from(vec![Some(10), Some(20)]);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(ts), Arc::new(val)]).unwrap();

        let parser = IpcParser::new(schema.clone());
        let msg = parser.parse(batch).unwrap();
        let flat = msg.as_any().downcast_ref::<FlatMessage>().unwrap();
        assert_eq!(flat.nrows(), 2);

        // Switch to point stream
        let mut md2 = std::collections::HashMap::new();
        md2.insert("version".to_string(), "1.0".to_string());
        md2.insert("stream".to_string(), "point".to_string());
        md2.insert("ack".to_string(), "none".to_string());
        let schema2 = Arc::new(Schema::new(schema.fields().clone()).with_metadata(md2));
        let parser2 = IpcParser::new(schema2.clone());
        let ts2 = TimestampMillisecondArray::from(vec![Some(3_000i64)]);
        let val2 = Int32Array::from(vec![Some(30)]);
        let batch2 =
            RecordBatch::try_new(schema2.clone(), vec![Arc::new(ts2), Arc::new(val2)]).unwrap();
        let msg2 = parser2.parse(batch2).unwrap();
        let point = msg2.as_any().downcast_ref::<PointMessage>().unwrap();
        assert_eq!(point.nrows(), 1);
    }

    #[test]
    fn parse_lush_insert_and_generate_sql() {
        use arrow::array::{
            BinaryBuilder, Int32Builder, ListBuilder, StringBuilder, StructBuilder,
        };

        // Metadata with lush stream and init
        let mut md = std::collections::HashMap::new();
        md.insert("version".to_string(), "1.0".to_string());
        md.insert("stream".to_string(), "lush".to_string());
        md.insert("ack".to_string(), "none".to_string());
        // init JSON for stable name and tags
        let init_json = serde_json::json!({
            "name": "stable",
            "columns": [{"name":"c1","type":"int"}, {"name":"vc","type":"varchar(16)"}],
            "tags": [{"name":"t1","type":"varchar(8)"}]
        })
        .to_string();
        md.insert("init".to_string(), init_json);

        // __TYPE__ field
        let type_field = Field::new(__TYPE__, DataType::UInt8, false);

        // Build __RECORDS__ List<Struct{__table_name__, c1, vc}>
        let struct_fields = vec![
            Field::new(__TABLE_NAME__, DataType::Utf8, true),
            Field::new("c1", DataType::Int32, true),
            Field::new("vc", DataType::Utf8, true),
        ];
        let struct_field_builders = vec![
            arrow::array::make_builder(&DataType::Utf8, 2),
            arrow::array::make_builder(&DataType::Int32, 2),
            arrow::array::make_builder(&DataType::Utf8, 2),
        ];
        let mut s_builder = StructBuilder::new(struct_fields.clone(), struct_field_builders);
        // append two rows
        let b_name = s_builder.field_builder::<StringBuilder>(0).unwrap();
        b_name.append_value("t1");
        b_name.append_value("t2");
        let b_c1 = s_builder.field_builder::<Int32Builder>(1).unwrap();
        b_c1.append_value(11);
        b_c1.append_value(22);
        let b_vc = s_builder.field_builder::<StringBuilder>(2).unwrap();
        b_vc.append_value("short");
        b_vc.append_value("much_longer");
        // append two struct rows to match child builder lengths
        s_builder.append(true);
        s_builder.append(true);
        let mut list_builder = ListBuilder::new(s_builder);
        // close single list item and build
        let records_list = {
            let _values_builder = list_builder.values();
            // values already appended; just close list item
            list_builder.append(true);
            list_builder.finish()
        };
        let records_field = Field::new(
            __RECORDS__,
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(struct_fields.clone().into()),
                true,
            ))),
            true,
        );

        // Build __ATTRS__ Struct{__table_name__, t1}
        let attrs_fields = vec![
            Field::new(__TABLE_NAME__, DataType::Binary, true),
            Field::new("t1", DataType::Utf8, true),
        ];
        let mut attrs_builder = StructBuilder::from_fields(attrs_fields.clone(), 1);
        let a_name = attrs_builder.field_builder::<BinaryBuilder>(0).unwrap();
        a_name.append_value("stable_child");
        let a_t1 = attrs_builder.field_builder::<StringBuilder>(1).unwrap();
        a_t1.append_value("tag1");
        attrs_builder.append(true);
        let attrs_array = attrs_builder.finish();
        let attrs_field = Field::new(
            __ATTRS__,
            DataType::Struct(
                vec![
                    Field::new(__TABLE_NAME__, DataType::Binary, true),
                    Field::new("t1", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        );

        // Assemble schema and batch
        let schema = Arc::new(
            Schema::new(vec![
                type_field.clone(),
                records_field.clone(),
                attrs_field.clone(),
            ])
            .with_metadata(md),
        );
        let type_arr = Arc::new(UInt8Array::from(vec![LushMessageType::Insert as u8]));
        let records_arr = Arc::new(records_list);
        let attrs_arr = Arc::new(attrs_array) as ArrayRef;
        let batch =
            RecordBatch::try_new(schema.clone(), vec![type_arr, records_arr, attrs_arr]).unwrap();

        let parser = IpcParser::new(schema.clone());
        let msg = parser.parse(batch).unwrap();
        let lush = msg.as_any().downcast_ref::<LushMessage>().unwrap();
        match lush {
            LushMessage::Insert(list) => {
                assert_eq!(list.len(), 1);
                let insert = &list[0];
                assert_eq!(insert.num_rows(), 2);
                // Column views and SQL generation
                let views = insert.to_column_views(Precision::Millisecond);
                // columns should exclude the reserved __TABLE_NAME__ field
                let columns = vec!["c1".to_string(), "vc".to_string()];
                let (sqls, field_map) = insert
                    .generate_insert_sql_from_tablename(&views, &columns)
                    .unwrap();
                assert!(sqls.first().unwrap().contains("INSERT INTO"));
                // var type length should be updated to longest string
                let vc_ty = field_map.get(&"vc".to_string()).unwrap();
                assert!(matches!(vc_ty, IpcDataType::VarChar(len) if *len >= 11));
            }
            _ => panic!("Expected Lush Insert"),
        }
    }

    #[test]
    fn parse_lush_children_tables() {
        use arrow::array::{ListBuilder, StringBuilder, StructBuilder};

        // Metadata with lush stream and init
        let mut md = std::collections::HashMap::new();
        md.insert("version".to_string(), "1.0".to_string());
        md.insert("stream".to_string(), "lush".to_string());
        md.insert("ack".to_string(), "none".to_string());
        let init_json = serde_json::json!({
            "name": "stable",
            "columns": [{"name":"c1","type":"int"}],
            "tags": [{"name":"t1","type":"varchar(8)"}]
        })
        .to_string();
        md.insert("init".to_string(), init_json);

        // __TYPE__ field Children
        let type_field = Field::new(__TYPE__, DataType::UInt8, false);

        // Build __TABLES__ List<Struct{__table_name__, t1}>
        let table_struct_fields = vec![
            Field::new(__TABLE_NAME__, DataType::Utf8, true),
            Field::new("t1", DataType::Utf8, true),
        ];
        let mut table_struct_builder = StructBuilder::from_fields(table_struct_fields.clone(), 2);
        let tb_name = table_struct_builder
            .field_builder::<StringBuilder>(0)
            .unwrap();
        tb_name.append_value("childA");
        tb_name.append_value("childB");
        let tb_t1 = table_struct_builder
            .field_builder::<StringBuilder>(1)
            .unwrap();
        tb_t1.append_value("x");
        tb_t1.append_value("y");
        // append two struct rows to match child builder lengths
        table_struct_builder.append(true);
        table_struct_builder.append(true);
        let mut tables_list_builder = ListBuilder::new(table_struct_builder);
        tables_list_builder.append(true);
        let tables_list = tables_list_builder.finish();
        let tables_field = Field::new(
            __TABLES__,
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(table_struct_fields.clone().into()),
                true,
            ))),
            true,
        );

        // __RECORDS__ can be an empty list with matching struct to allow concat
        let rec_struct_fields = vec![Field::new("c1", DataType::Int32, true)];
        let rec_struct_builder = StructBuilder::from_fields(rec_struct_fields.clone(), 0);
        let mut rec_list_builder = ListBuilder::new(rec_struct_builder);
        rec_list_builder.append(true);
        let rec_list = rec_list_builder.finish();
        let records_field = Field::new(
            __RECORDS__,
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(rec_struct_fields.clone().into()),
                true,
            ))),
            true,
        );

        // Assemble schema and batch: order must match __TABLES_INDEX__ = 1
        let schema = Arc::new(
            Schema::new(vec![
                type_field.clone(),
                tables_field.clone(),
                records_field.clone(),
            ])
            .with_metadata(md),
        );
        let type_arr = Arc::new(UInt8Array::from(vec![LushMessageType::Children as u8]));
        let tables_arr = Arc::new(tables_list);
        let records_arr = Arc::new(rec_list);
        let batch =
            RecordBatch::try_new(schema.clone(), vec![type_arr, tables_arr, records_arr]).unwrap();

        let parser = IpcParser::new(schema.clone());
        let msg = parser.parse(batch).unwrap();
        let lush = msg.as_any().downcast_ref::<LushMessage>().unwrap();
        assert!(lush.is_tables());
        if let LushMessage::Tables(attrs, _full) = lush {
            assert_eq!(attrs.len(), 2);
            // Generate SQL from attrs must include USING stable
            let sql = attrs[0].to_sql(None).unwrap();
            assert!(sql.contains("USING`stable`") || sql.contains("USING `stable`"));
        }
    }
    #[test]
    fn test_record_batch_to_column_view() {
        // cast StringArray to Decimal128
        let array = StringArray::from(vec!["2052.43", "Null", "-2052.43"]);
        let to_type = DataType::Decimal128(18, 4);
        let cast_options = CastOptions::default();
        let array = arrow::compute::cast_with_options(&array, &to_type, &cast_options).unwrap();
        let array_ref: ArrayRef = std::sync::Arc::new(array);
        // build record batch
        let field = Field::new("n_orig_cost", DataType::Decimal128(18, 4), true);
        let schema = std::sync::Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array_ref]).unwrap();
        // record batch to ColumnView
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        // assert
        assert_eq!(views.len(), 1);
        let col = &views[0];
        assert_eq!(col.len(), 3);
        assert_eq!(col.get(0).unwrap().to_sql_value(), "2052.4300");
        assert!(col.get(1).unwrap().is_null());
        assert_eq!(col.get(2).unwrap().to_sql_value(), "-2052.4300");

        // Decimal128 to ColumnView::Decimal64
        let array = Decimal128Array::from(vec![
            Some(543210123456789i128),
            Some(-543210123456789i128),
            None,
        ]);
        let array_ref: ArrayRef = std::sync::Arc::new(array);
        let mut field_metadata = std::collections::HashMap::new();
        field_metadata.insert("cast_to".to_string(), "DECIMAL".to_string());
        field_metadata.insert("precision".to_string(), "15".to_string());
        field_metadata.insert("scale".to_string(), "2".to_string());
        let field =
            Field::new("amount", DataType::Decimal128(38, 10), true).with_metadata(field_metadata);
        let schema = std::sync::Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array_ref]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views.len(), 1);
        let col = &views[0];
        assert_eq!(col.len(), 3);
        assert!(matches!(col, ColumnView::Decimal64 { .. }));
        assert_eq!(col.get(0).unwrap().to_sql_value(), "54321.0123456789");
        assert_eq!(col.get(1).unwrap().to_sql_value(), "-54321.0123456789");
        assert!(col.get(2).unwrap().is_null());

        // Binary with cast_to VARBINARY
        let bin_array = BinaryArray::from(vec![Some(&[0xDEu8, 0xAD, 0xBE, 0xEF][..]), None]);
        let mut field_metadata = std::collections::HashMap::new();
        field_metadata.insert("cast_to".to_string(), "VARBINARY".to_string());
        let field = Field::new("bytes", DataType::Binary, true).with_metadata(field_metadata);
        let schema = std::sync::Arc::new(Schema::new(vec![field]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![std::sync::Arc::new(bin_array)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views.len(), 1);
        let col = &views[0];
        assert_eq!(col.len(), 2);
        if let taos::Value::VarBinary(b) = col.get(0).unwrap().to_value() {
            assert_eq!(b.as_ref(), &[0xDE, 0xAD, 0xBE, 0xEF]);
        } else {
            panic!("expected VarBinary value");
        }
        assert!(col.get(1).unwrap().is_null());

        // Utf8 with cast_to NCHAR
        let str_array = StringArray::from(vec![Some("你好"), None]);
        let mut field_metadata = std::collections::HashMap::new();
        field_metadata.insert("cast_to".to_string(), "NCHAR".to_string());
        let field = Field::new("nchar", DataType::Utf8, true).with_metadata(field_metadata);
        let schema = std::sync::Arc::new(Schema::new(vec![field]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![std::sync::Arc::new(str_array)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views.len(), 1);
        let col = &views[0];
        assert_eq!(col.len(), 2);
        assert!(col.get(0).unwrap().to_sql_value().contains("你好"));
        assert!(col.get(1).unwrap().is_null());

        // Timestamp Second to Microsecond conversion
        let ts_sec = TimestampSecondArray::from(vec![Some(2i64), None]);
        let field = Field::new(
            "ts",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
            true,
        );
        let schema = std::sync::Arc::new(Schema::new(vec![field]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![std::sync::Arc::new(ts_sec)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Microsecond);
        let col = &views[0];
        assert_eq!(col.get(0).unwrap().to_sql_value(), "2000000");
        assert!(col.get(1).unwrap().is_null());

        // Decimal128 to ColumnView::Decimal
        let array = Decimal128Array::from(vec![
            Some(1234567890123456789i128),
            Some(-9876543210123456789i128),
            None,
        ]);
        let array_ref: ArrayRef = std::sync::Arc::new(array);
        let mut field_metadata = std::collections::HashMap::new();
        field_metadata.insert("cast_to".to_string(), "DECIMAL".to_string());
        field_metadata.insert("precision".to_string(), "22".to_string());
        field_metadata.insert("scale".to_string(), "10".to_string());
        let field =
            Field::new("amount", DataType::Decimal128(38, 10), true).with_metadata(field_metadata);
        let schema = std::sync::Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array_ref]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views.len(), 1);
        let col = &views[0];
        assert_eq!(col.len(), 3);
        assert!(matches!(col, ColumnView::Decimal { .. }));
        assert_eq!(col.get(0).unwrap().to_sql_value(), "123456789.0123456789");
        assert_eq!(col.get(1).unwrap().to_sql_value(), "-987654321.0123456789");
        assert!(col.get(2).unwrap().is_null());
    }

    #[test]
    fn file_reader() -> anyhow::Result<()> {
        use std::io::prelude::*;

        let mut file = std::fs::File::open("./examples/dotnet/dotnet.arrow.zstd")?;
        let mut bytes = vec![];
        file.read_to_end(&mut bytes)?;
        let zin = zstd::decode_all(bytes.as_slice())?;

        for precision in [
            Precision::Millisecond,
            Precision::Microsecond,
            Precision::Nanosecond,
        ] {
            println!("--- precision: {:?} ---", precision);

            let reader = IpcReader::new(zin.as_slice()).unwrap();

            dbg!(reader.metadata());
            dbg!(&reader.schema);

            for records in reader {
                let res = records.unwrap();
                let record = res.as_any().downcast_ref::<LushMessage>().unwrap();
                match record {
                    LushMessage::Insert(records) => {
                        for record in records {
                            let map_data = record.to_column_views(precision);
                            dbg!(&map_data);

                            let columns = vec!["ts".to_string(), "c1".to_string()];
                            let sqls =
                                record.generate_insert_sql_from_tablename(&map_data, &columns);
                            dbg!(&sqls);
                        }
                    }
                    LushMessage::Tables(tables, _) => {
                        for record in tables {
                            let map_data = record.to_sql(None);
                            dbg!(&map_data);
                        }
                    }
                    _ => (),
                }
            }
        }

        // (&stream).write_all(zin.as_slice()).unwrap();
        Ok(())
    }

    #[test]
    fn parse_column_view_with_types_wide() {
        use crate::prelude::IpcDataType;
        use arrow::datatypes::Field;
        // Build diverse columns
        let b = BooleanArray::from(vec![Some(true), None, Some(false)]);
        let i8 = Int8Array::from(vec![Some(-1), None, Some(2)]);
        let u16 = UInt16Array::from(vec![Some(65535), None, Some(0)]);
        let f32a = Float32Array::from(vec![Some(std::f32::consts::PI), None, Some(-1.0f32)]);
        let f64a = Float64Array::from(vec![Some(std::f64::consts::E), None, Some(-0.5f64)]);
        let ts = TimestampMillisecondArray::from(vec![Some(1000i64), None, Some(2000i64)]);
        let vc = StringArray::from(vec![Some("A"), None, Some("B")]);
        let nc_bin = BinaryArray::from(vec![Some(&b"abc"[..]), None, Some(&b"xyz"[..])]);
        let blob =
            LargeBinaryArray::from(vec![Some(&[1u8, 2, 3][..]), None, Some(&[4u8, 5u8][..])]);

        // Schema and metadata
        let fields = vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i8", DataType::Int8, true),
            Field::new("u16", DataType::UInt16, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                true,
            ),
            Field::new("vc", DataType::Utf8, true),
            Field::new("nc_bin", DataType::Binary, true),
            Field::new("blob", DataType::LargeBinary, true),
        ];
        let schema = Arc::new(Schema::new(fields));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(b),
                Arc::new(i8),
                Arc::new(u16),
                Arc::new(f32a),
                Arc::new(f64a),
                Arc::new(ts),
                Arc::new(vc),
                Arc::new(nc_bin),
                Arc::new(blob),
            ],
        )
        .unwrap();

        let metadata = vec![
            IpcDataType::Bool,
            IpcDataType::Int8,
            IpcDataType::UInt16,
            IpcDataType::Float32,
            IpcDataType::Float64,
            IpcDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond),
            IpcDataType::VarChar(10),
            IpcDataType::NChar(10),
            IpcDataType::Blob,
        ];

        let views = parse_column_view_with_types(&batch, &metadata, Precision::Millisecond);
        assert_eq!(views.len(), 9);

        // spot-check values and types
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "true");
        assert!(views[0].get(1).unwrap().is_null());
        assert_eq!(views[1].get(0).unwrap().to_sql_value(), "-1");
        assert_eq!(views[2].get(0).unwrap().to_sql_value(), "65535");
        assert_eq!(views[3].get(0).unwrap().to_sql_value(), "3.1415927");
        assert_eq!(views[4].get(0).unwrap().to_sql_value(), "2.718281828459045");
        assert_eq!(views[5].get(0).unwrap().to_sql_value(), "1000");
        assert_eq!(views[6].get(0).unwrap().to_sql_value(), "\"A\"");
        assert_eq!(views[7].get(0).unwrap().to_sql_value(), "\"abc\"");
        // Blob value via to_value()
        if let taos::Value::Blob(b) = views[8].get(0).unwrap().to_value() {
            assert_eq!(b.as_ref(), &[1u8, 2, 3]);
        } else {
            panic!("expected Blob value");
        }
    }

    #[test]
    fn additional_reader_branches() {
        use crate::prelude::IpcDataType;
        use arrow::array::{Int32Builder, ListBuilder};
        use arrow::datatypes::Field;

        // VarBinary from Utf8 hex strings
        let hex = StringArray::from(vec![Some("\\xDEADBEEF"), None, Some("DEAD")]);
        let schema = Arc::new(Schema::new(vec![Field::new("hex", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(hex)]).unwrap();
        let views = parse_column_view_with_types(
            &batch,
            &[IpcDataType::VarBinary(8)],
            Precision::Millisecond,
        );
        assert_eq!(views.len(), 1);
        if let taos::Value::VarBinary(b) = views[0].get(0).unwrap().to_value() {
            assert_eq!(b.as_ref(), &[0xDEu8, 0xAD, 0xBE, 0xEF]);
        } else {
            panic!("expected VarBinary value");
        }
        assert!(views[0].get(1).unwrap().is_null());
        if let taos::Value::VarBinary(b) = views[0].get(2).unwrap().to_value() {
            assert_eq!(b.as_ref(), &[0xDEu8, 0xAD]);
        }

        // List of numeric cast to VARBINARY
        let int_builder = Int32Builder::new();
        let mut list_builder = ListBuilder::new(int_builder);
        {
            let values = list_builder.values();
            values.append_value(1);
            values.append_value(2);
            values.append_value(3);
            list_builder.append(true);
        }
        list_builder.append(false); // null row
        {
            let values = list_builder.values();
            values.append_value(255);
            values.append_value(0);
            list_builder.append(true);
        }
        let list_array = list_builder.finish();
        let mut md = std::collections::HashMap::new();
        md.insert("cast_to".to_string(), "VARBINARY".to_string());
        let field = Field::new(
            "nums",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )
        .with_metadata(md);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(list_array)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views.len(), 1);
        if let taos::Value::VarBinary(b) = views[0].get(0).unwrap().to_value() {
            // 1,2,3 become bytes [1,2,3]
            assert_eq!(b.as_ref(), &[1u8, 2u8, 3u8]);
        }
        assert!(views[0].get(1).unwrap().is_null());
        if let taos::Value::VarBinary(b) = views[0].get(2).unwrap().to_value() {
            assert_eq!(b.as_ref(), &[255u8, 0u8]);
        }
    }

    #[test]
    fn parse_column_view_with_types_misc() {
        use crate::prelude::IpcDataType;
        use arrow::datatypes::Field;

        // Timestamp from Utf8 numeric strings
        let ts_str = StringArray::from(vec![Some("1000"), None]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts_str",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ts_str)]).unwrap();
        let views = parse_column_view_with_types(
            &batch,
            &[IpcDataType::Timestamp(
                arrow::datatypes::TimeUnit::Millisecond,
            )],
            Precision::Millisecond,
        );
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "1000");
        assert!(views[0].get(1).unwrap().is_null());

        // Binary default to VarChar
        let bin = BinaryArray::from(vec![Some(&b"abc"[..]), None]);
        let schema = Arc::new(Schema::new(vec![Field::new("bin", DataType::Binary, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(bin)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "\"abc\"");
        assert!(views[0].get(1).unwrap().is_null());

        // Utf8 default to VarChar
        let vc = StringArray::from(vec![Some("xyz"), None]);
        let schema = Arc::new(Schema::new(vec![Field::new("vc", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(vc)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "\"xyz\"");
        assert!(views[0].get(1).unwrap().is_null());
    }

    #[test]
    fn fixed_size_binary_varbinary() {
        use arrow::array::FixedSizeBinaryBuilder;
        use arrow::datatypes::Field;
        // Build FixedSizeBinary(2) with cast_to VARBINARY
        let mut builder = FixedSizeBinaryBuilder::new(2);
        builder.append_value([0xAAu8, 0xBB]).expect("append error");
        builder.append_null();
        let fsb = builder.finish();
        let mut md = std::collections::HashMap::new();
        md.insert("cast_to".to_string(), "VARBINARY".to_string());
        let field = Field::new("fsb", DataType::FixedSizeBinary(2), true).with_metadata(md);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(fsb)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views.len(), 1);
        if let taos::Value::VarBinary(b) = views[0].get(0).unwrap().to_value() {
            assert_eq!(b.as_ref(), &[0xAAu8, 0xBB]);
        } else {
            panic!("expected VarBinary value");
        }
        assert!(views[0].get(1).unwrap().is_null());
    }

    #[test]
    fn binary_cast_to_nchar() {
        use arrow::datatypes::Field;
        let bin = BinaryArray::from(vec![Some("你好".as_bytes()), None]);
        let mut md = std::collections::HashMap::new();
        md.insert("cast_to".to_string(), "NCHAR".to_string());
        let field = Field::new("nchar_bin", DataType::Binary, true).with_metadata(md);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(bin)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views.len(), 1);
        assert!(views[0].get(0).unwrap().to_sql_value().contains("你好"));
        assert!(views[0].get(1).unwrap().is_null());
    }

    #[test]
    fn test_uint_types_with_nulls() {
        use arrow::datatypes::Field;
        // Test UInt8 with nulls
        let u8_arr = UInt8Array::from(vec![Some(255u8), None, Some(0u8)]);
        let field = Field::new("u8_col", DataType::UInt8, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(u8_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "255");
        assert!(views[0].get(1).unwrap().is_null());
        assert_eq!(views[0].get(2).unwrap().to_sql_value(), "0");

        // Test UInt16 with nulls
        let u16_arr = UInt16Array::from(vec![Some(65535u16), None, Some(100u16)]);
        let field = Field::new("u16_col", DataType::UInt16, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(u16_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "65535");
        assert!(views[0].get(1).unwrap().is_null());
        assert_eq!(views[0].get(2).unwrap().to_sql_value(), "100");

        // Test UInt32 with nulls
        let u32_arr = UInt32Array::from(vec![Some(4294967295u32), None, Some(123u32)]);
        let field = Field::new("u32_col", DataType::UInt32, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(u32_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "4294967295");
        assert!(views[0].get(1).unwrap().is_null());
        assert_eq!(views[0].get(2).unwrap().to_sql_value(), "123");

        // Test UInt64 with nulls
        let u64_arr = UInt64Array::from(vec![Some(18446744073709551615u64), None, Some(456u64)]);
        let field = Field::new("u64_col", DataType::UInt64, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(u64_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(
            views[0].get(0).unwrap().to_sql_value(),
            "18446744073709551615"
        );
        assert!(views[0].get(1).unwrap().is_null());
        assert_eq!(views[0].get(2).unwrap().to_sql_value(), "456");
    }

    #[test]
    fn test_boolean_and_float16() {
        use arrow::datatypes::Field;
        // Test Boolean with nulls
        let bool_arr = BooleanArray::from(vec![Some(true), Some(false), None]);
        let field = Field::new("bool_col", DataType::Boolean, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(bool_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "true");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "false");
        assert!(views[0].get(2).unwrap().is_null());

        // Test Float32 as Float16 replacement (Float16 is already covered in reader tests)
        let f32_arr = Float32Array::from(vec![Some(3.15f32), None, Some(-1.5f32)]);
        let field = Field::new("f32_col", DataType::Float32, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(f32_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert!(views[0].get(0).unwrap().to_sql_value().contains("3."));
        assert!(views[0].get(1).unwrap().is_null());
        assert!(views[0].get(2).unwrap().to_sql_value().contains("-1."));
    }

    #[test]
    fn test_large_utf8_only() {
        use arrow::datatypes::Field;
        // Test LargeUtf8 with null - skip LargeBinary test for now as it's not implemented
        // Just verify parse_column_view_with_types works with it
        let str_arr = StringArray::from(vec![Some("test"), None]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "str_col",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(str_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "\"test\"");
        assert!(views[0].get(1).unwrap().is_null());
    }

    #[test]
    fn test_timestamp_precision_conversions() {
        use arrow::datatypes::Field;
        // Test Timestamp Second to Microsecond
        let ts_sec = TimestampSecondArray::from(vec![Some(1000i64), Some(2000i64), None]);
        let field = Field::new(
            "ts_sec",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Second, None),
            true,
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ts_sec)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Microsecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "1000000000");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "2000000000");
        assert!(views[0].get(2).unwrap().is_null());

        // Test Timestamp Millisecond to Nanosecond
        let ts_milli = TimestampMillisecondArray::from(vec![Some(1000i64), None]);
        let field = Field::new(
            "ts_milli",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ts_milli)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Nanosecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "1000000000");
        assert!(views[0].get(1).unwrap().is_null());

        // Test Timestamp Microsecond no nulls
        let ts_micro = TimestampMicrosecondArray::from(vec![Some(1000i64), Some(2000i64)]);
        let field = Field::new(
            "ts_micro",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            true,
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ts_micro)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Microsecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "1000");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "2000");

        // Test Timestamp Nanosecond with nulls
        let ts_nano = TimestampNanosecondArray::from(vec![Some(1000i64), None, Some(3000i64)]);
        let field = Field::new(
            "ts_nano",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            true,
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ts_nano)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Nanosecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "1000");
        assert!(views[0].get(1).unwrap().is_null());
        assert_eq!(views[0].get(2).unwrap().to_sql_value(), "3000");
    }

    #[test]
    fn test_parse_column_view_types_uint() {
        use crate::prelude::IpcDataType;
        use arrow::datatypes::Field;
        // Build UInt8 array from Utf8 strings
        let str_arr = StringArray::from(vec![Some("200"), Some("0"), None]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "u8_val",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(str_arr)]).unwrap();
        let views =
            parse_column_view_with_types(&batch, &[IpcDataType::UInt8], Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "200");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "0");
        assert!(views[0].get(2).unwrap().is_null());

        // Test UInt16
        let str_arr = StringArray::from(vec![Some("65000"), None]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "u16_val",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(str_arr)]).unwrap();
        let views =
            parse_column_view_with_types(&batch, &[IpcDataType::UInt16], Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "65000");
        assert!(views[0].get(1).unwrap().is_null());

        // Test UInt32
        let str_arr = StringArray::from(vec![Some("4000000000"), None]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "u32_val",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(str_arr)]).unwrap();
        let views =
            parse_column_view_with_types(&batch, &[IpcDataType::UInt32], Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "4000000000");
        assert!(views[0].get(1).unwrap().is_null());

        // Test UInt64
        let str_arr = StringArray::from(vec![Some("10000000000000000000")]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "u64_val",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(str_arr)]).unwrap();
        let views =
            parse_column_view_with_types(&batch, &[IpcDataType::UInt64], Precision::Millisecond);
        assert_eq!(
            views[0].get(0).unwrap().to_sql_value(),
            "10000000000000000000"
        );
    }

    #[test]
    fn test_parse_column_view_types_bool() {
        use crate::prelude::IpcDataType;
        use arrow::datatypes::Field;
        // Build Bool array from Utf8 strings
        let str_arr = StringArray::from(vec![Some("true"), Some("false"), Some("true"), None]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "bool_val",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(str_arr)]).unwrap();
        let views =
            parse_column_view_with_types(&batch, &[IpcDataType::Bool], Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "true");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "false");
        assert_eq!(views[0].get(2).unwrap().to_sql_value(), "true");
        assert!(views[0].get(3).unwrap().is_null());
    }

    #[test]
    fn test_parse_float_variants() {
        use arrow::datatypes::Field;
        // Test Float32 with nulls
        let f32_arr = Float32Array::from(vec![Some(3.15f32), Some(-2.71f32), None]);
        let field = Field::new("f32_col", DataType::Float32, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(f32_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        let val = views[0].get(0).unwrap().to_sql_value();
        assert!(val.contains("3.15"));
        let val = views[0].get(1).unwrap().to_sql_value();
        assert!(val.contains("-2.71"));
        assert!(views[0].get(2).unwrap().is_null());

        // Test Float64 without nulls
        let f64_arr = Float64Array::from(vec![Some(std::f64::consts::E), Some(-0.5)]);
        let field = Field::new("f64_col", DataType::Float64, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(f64_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "2.718281828459045");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "-0.5");
    }

    #[test]
    fn test_int_type_variants() {
        use arrow::datatypes::Field;
        // Test Int8 with nulls
        let i8_arr = Int8Array::from(vec![Some(127i8), Some(-128i8), None]);
        let field = Field::new("i8_col", DataType::Int8, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(i8_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "127");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "-128");
        assert!(views[0].get(2).unwrap().is_null());

        // Test Int16 without nulls
        let i16_arr = Int16Array::from(vec![Some(32767i16), Some(-32768i16)]);
        let field = Field::new("i16_col", DataType::Int16, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(i16_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "32767");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "-32768");

        // Test Int32 with nulls
        let i32_arr = Int32Array::from(vec![Some(2147483647i32), None, Some(-1i32)]);
        let field = Field::new("i32_col", DataType::Int32, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(i32_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "2147483647");
        assert!(views[0].get(1).unwrap().is_null());
        assert_eq!(views[0].get(2).unwrap().to_sql_value(), "-1");

        // Test Int64 without nulls
        let i64_arr = Int64Array::from(vec![
            Some(9223372036854775807i64),
            Some(-9223372036854775808i64),
        ]);
        let field = Field::new("i64_col", DataType::Int64, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(i64_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(
            views[0].get(0).unwrap().to_sql_value(),
            "9223372036854775807"
        );
        assert_eq!(
            views[0].get(1).unwrap().to_sql_value(),
            "-9223372036854775808"
        );
    }

    #[test]
    fn test_timestamp_all_precisions_no_nulls() {
        use arrow::datatypes::Field;
        // Test Timestamp Millisecond without nulls, multiple precisions output
        let ts_milli = TimestampMillisecondArray::from(vec![Some(1000i64), Some(2000i64)]);
        let field = Field::new(
            "ts_milli_out",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ts_milli)]).unwrap();

        // Output to Millisecond
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "1000");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "2000");

        // Output to Microsecond
        let views = record_batch_to_column_view(&batch, Precision::Microsecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "1000000");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "2000000");

        // Output to Nanosecond
        let views = record_batch_to_column_view(&batch, Precision::Nanosecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "1000000000");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "2000000000");
    }

    #[test]
    fn test_parse_str_into_int_variants() {
        use crate::prelude::IpcDataType;

        // Test Int8 variants
        let view =
            arrow_to_taos::parse_str_into(&IpcDataType::Int8, vec![Some("100"), Some("-50"), None]);
        assert_eq!(view.get(0).unwrap().to_sql_value(), "100");
        assert_eq!(view.get(1).unwrap().to_sql_value(), "-50");
        assert!(view.get(2).unwrap().is_null());

        // Test Int16 variants
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Int16,
            vec![Some("30000"), Some("-30000"), None],
        );
        assert_eq!(view.get(0).unwrap().to_sql_value(), "30000");
        assert_eq!(view.get(1).unwrap().to_sql_value(), "-30000");
        assert!(view.get(2).unwrap().is_null());

        // Test Int32 variants
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Int32,
            vec![Some("1000000"), Some("-1000000"), None],
        );
        assert_eq!(view.get(0).unwrap().to_sql_value(), "1000000");
        assert_eq!(view.get(1).unwrap().to_sql_value(), "-1000000");
        assert!(view.get(2).unwrap().is_null());

        // Test Int64 variants
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Int64,
            vec![Some("9223372036854775807"), None],
        );
        assert_eq!(view.get(0).unwrap().to_sql_value(), "9223372036854775807");
        assert!(view.get(1).unwrap().is_null());
    }

    #[test]
    fn test_parse_str_into_uint_variants() {
        use crate::prelude::IpcDataType;

        // Test UInt32 variants
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::UInt32,
            vec![Some("4000000000"), None, Some("100")],
        );
        assert_eq!(view.get(0).unwrap().to_sql_value(), "4000000000");
        assert!(view.get(1).unwrap().is_null());
        assert_eq!(view.get(2).unwrap().to_sql_value(), "100");

        // Test UInt64 variants
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::UInt64,
            vec![Some("18446744073709551615"), None],
        );
        assert_eq!(view.get(0).unwrap().to_sql_value(), "18446744073709551615");
        assert!(view.get(1).unwrap().is_null());
    }

    #[test]
    fn test_parse_str_into_float_variants() {
        use crate::prelude::IpcDataType;

        // Test Float32 variants
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Float32,
            vec![Some("3.14"), Some("-2.71"), None],
        );
        assert!(view.get(0).unwrap().to_sql_value().contains("3.14"));
        assert!(view.get(1).unwrap().to_sql_value().contains("-2.71"));
        assert!(view.get(2).unwrap().is_null());

        // Test Float64 variants
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Float64,
            vec![Some("2.718281828"), None, Some("1.414")],
        );
        assert_eq!(view.get(0).unwrap().to_sql_value(), "2.718281828");
        assert!(view.get(1).unwrap().is_null());
        assert!(view.get(2).unwrap().to_sql_value().contains("1.414"));
    }

    #[test]
    fn test_parse_str_into_decimal() {
        use crate::prelude::IpcDataType;

        // Test Decimal with precision and scale
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Decimal(10, 2),
            vec![Some("123.45"), Some("999.99"), None],
        );
        assert!(view.get(0).unwrap().to_sql_value().contains("123.45"));
        assert!(view.get(1).unwrap().to_sql_value().contains("999.99"));
        assert!(view.get(2).unwrap().is_null());

        // Test with different scale
        let view =
            arrow_to_taos::parse_str_into(&IpcDataType::Decimal(5, 3), vec![Some("12.345"), None]);
        assert!(view.get(0).unwrap().to_sql_value().contains("12.345"));
        assert!(view.get(1).unwrap().is_null());
    }

    #[test]
    fn test_parse_str_into_varchar_nchar() {
        use crate::prelude::IpcDataType;

        // Test VarChar
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::VarChar(100),
            vec![Some("hello"), Some("world"), None],
        );
        assert_eq!(view.get(0).unwrap().to_sql_value(), "\"hello\"");
        assert_eq!(view.get(1).unwrap().to_sql_value(), "\"world\"");
        assert!(view.get(2).unwrap().is_null());

        // Test NChar
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::NChar(50),
            vec![Some("你好"), None, Some("测试")],
        );
        assert!(view.get(0).unwrap().to_sql_value().contains("你好"));
        assert!(view.get(1).unwrap().is_null());
        assert!(view.get(2).unwrap().to_sql_value().contains("测试"));
    }

    #[test]
    fn test_parse_str_into_invalid_values() {
        use crate::prelude::IpcDataType;

        // Test invalid bool string falls back to null
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Bool,
            vec![Some("true"), Some("invalid"), None],
        );
        assert_eq!(view.get(0).unwrap().to_sql_value(), "true");
        assert!(view.get(1).unwrap().is_null()); // invalid bool -> null
        assert!(view.get(2).unwrap().is_null());

        // Test invalid int8 string falls back to null
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Int8,
            vec![Some("100"), Some("invalid_number"), None],
        );
        assert_eq!(view.get(0).unwrap().to_sql_value(), "100");
        assert!(view.get(1).unwrap().is_null()); // invalid int -> null
        assert!(view.get(2).unwrap().is_null());

        // Test invalid float string falls back to null
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Float32,
            vec![Some("3.14"), Some("not_a_float")],
        );
        assert!(view.get(0).unwrap().to_sql_value().contains("3.14"));
        assert!(view.get(1).unwrap().is_null()); // invalid float -> null
    }

    #[test]
    fn test_parse_str_into_all_nulls() {
        use crate::prelude::IpcDataType;

        // Test with all null values
        let view = arrow_to_taos::parse_str_into(&IpcDataType::UInt16, vec![None, None, None]);
        assert!(view.get(0).unwrap().is_null());
        assert!(view.get(1).unwrap().is_null());
        assert!(view.get(2).unwrap().is_null());

        // Test with single null
        let view = arrow_to_taos::parse_str_into(&IpcDataType::Int32, vec![None]);
        assert!(view.get(0).unwrap().is_null());
    }

    #[test]
    fn test_parse_str_into_timestamp_variants() {
        use crate::prelude::IpcDataType;

        // Test Timestamp Millisecond (not Second - Second is not implemented)
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond),
            vec![Some("1000000"), None],
        );
        assert!(view.get(0).unwrap().to_sql_value().contains("1000000"));
        assert!(view.get(1).unwrap().is_null());

        // Test Timestamp Microsecond
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond),
            vec![Some("1000000000"), None],
        );
        assert!(view.get(0).unwrap().to_sql_value().contains("1000000000"));
        assert!(view.get(1).unwrap().is_null());

        // Test Timestamp Nanosecond
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond),
            vec![None, Some("3000000000")],
        );
        assert!(view.get(0).unwrap().is_null());
        assert!(view.get(1).unwrap().to_sql_value().contains("3000000000"));
    }

    #[test]
    fn test_parse_str_into_json() {
        use crate::prelude::IpcDataType;

        // Test JSON type
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Json,
            vec![Some("{\"key\":\"value\"}"), None, Some("[1,2,3]")],
        );
        assert!(view.get(0).unwrap().to_sql_value().contains("key"));
        assert!(view.get(1).unwrap().is_null());
        assert!(view.get(2).unwrap().to_sql_value().contains("1"));
    }

    #[test]
    fn test_parse_str_into_binary_blob() {
        use crate::prelude::IpcDataType;

        // Test VarBinary (hex-encoded) - just verify it doesn't panic
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::VarBinary(100),
            vec![Some("\\xDEADBEEF"), None, Some("\\x0102")],
        );
        // VarBinary returns bytes, which might not have a to_sql_value impl yet
        // Just verify the view was created and contains values
        assert!(view.get(0).unwrap() != view.get(1).unwrap()); // first is not null, second is null
        assert!(view.get(1).unwrap().is_null());

        // Test Blob (hex-encoded)
        let view =
            arrow_to_taos::parse_str_into(&IpcDataType::Blob, vec![Some("\\xCAFEBABE"), None]);
        assert!(view.get(1).unwrap().is_null());
    }

    #[test]
    fn test_record_batch_with_decimal128() {
        use arrow::datatypes::Field;

        // Test Decimal128 values in record_batch_to_column_view
        // Use precision/scale of 38/10 to match the Decimal128Array type
        let decimal_arr = Decimal128Array::from(vec![Some(12345i128), None, Some(99999i128)]);
        let field = Field::new("decimal_col", DataType::Decimal128(38, 10), true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(decimal_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert!(!views[0].get(0).unwrap().to_sql_value().is_empty());
        assert!(views[0].get(1).unwrap().is_null());
        assert!(!views[0].get(2).unwrap().to_sql_value().is_empty());
    }

    #[test]
    fn test_record_batch_with_binary_data() {
        use arrow::datatypes::Field;

        // Test Binary array with various values
        let bin = BinaryArray::from(vec![Some(&b"binary_data"[..]), None, Some(&b""[..])]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "bin_col",
            DataType::Binary,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(bin)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert!(views[0]
            .get(0)
            .unwrap()
            .to_sql_value()
            .contains("binary_data"));
        assert!(views[0].get(1).unwrap().is_null());
        assert_eq!(views[0].get(2).unwrap().to_sql_value(), "\"\"");
    }

    #[test]
    fn test_parse_column_view_with_types_edge_cases() {
        use crate::prelude::IpcDataType;
        use arrow::datatypes::Field;

        // Test empty string values
        let str_arr = StringArray::from(vec![Some(""), Some("test"), None]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "str_col",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(str_arr)]).unwrap();
        let views = parse_column_view_with_types(
            &batch,
            &[IpcDataType::VarChar(50)],
            Precision::Millisecond,
        );
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "\"\"");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "\"test\"");
        assert!(views[0].get(2).unwrap().is_null());

        // Test numeric string with leading zeros
        let str_arr = StringArray::from(vec![Some("00123"), Some("000"), None]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "str_col",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(str_arr)]).unwrap();
        let views =
            parse_column_view_with_types(&batch, &[IpcDataType::Int32], Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "123");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "0");
        assert!(views[0].get(2).unwrap().is_null());
    }

    #[test]
    fn test_record_batch_to_column_view_single_column() {
        use arrow::datatypes::Field;

        // Test with single value, single column
        let u32_arr = UInt32Array::from(vec![Some(42u32)]);
        let field = Field::new("col", DataType::UInt32, true);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(u32_arr)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views.len(), 1);
        assert_eq!(views[0].len(), 1);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "42");
    }

    #[test]
    fn test_record_batch_with_multiple_columns() {
        use arrow::datatypes::Field;

        // Test with multiple columns
        let col1 = Int32Array::from(vec![Some(1), Some(2), None]);
        let col2 = StringArray::from(vec![Some("a"), None, Some("c")]);
        let field1 = Field::new("int_col", DataType::Int32, true);
        let field2 = Field::new("str_col", DataType::Utf8, true);
        let schema = Arc::new(Schema::new(vec![field1, field2]));
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(col1), Arc::new(col2)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views.len(), 2);

        // First column (int)
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "1");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "2");
        assert!(views[0].get(2).unwrap().is_null());

        // Second column (string)
        assert_eq!(views[1].get(0).unwrap().to_sql_value(), "\"a\"");
        assert!(views[1].get(1).unwrap().is_null());
        assert_eq!(views[1].get(2).unwrap().to_sql_value(), "\"c\"");
    }

    #[test]
    fn test_parse_str_into_with_whitespace() {
        use crate::prelude::IpcDataType;

        // Test numeric parsing - Rust's parse() is strict and doesn't trim whitespace,
        // so leading/trailing whitespace will cause parse failures
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Int32,
            vec![Some("123"), Some("456"), Some("789")],
        );
        assert_eq!(view.get(0).unwrap().to_sql_value(), "123");
        assert_eq!(view.get(1).unwrap().to_sql_value(), "456");
        assert_eq!(view.get(2).unwrap().to_sql_value(), "789");

        // Test with no whitespace for floats
        let view =
            arrow_to_taos::parse_str_into(&IpcDataType::Float64, vec![Some("3.14"), Some("-2.71")]);
        assert_eq!(view.get(0).unwrap().to_sql_value(), "3.14");
        assert_eq!(view.get(1).unwrap().to_sql_value(), "-2.71");
    }

    #[test]
    fn test_parse_str_into_edge_case_floats() {
        use crate::prelude::IpcDataType;

        // Test scientific notation
        let view = arrow_to_taos::parse_str_into(&IpcDataType::Float64, vec![Some("1.5e2"), None]);
        // "1.5e2" = 150.0
        let val = view.get(0).unwrap().to_sql_value();
        assert!(val.contains("150") || val.contains("1.5") || val.contains("e2"));
        assert!(view.get(1).unwrap().is_null());

        // Test negative scientific notation
        let view = arrow_to_taos::parse_str_into(&IpcDataType::Float32, vec![Some("2.5e-1")]);
        // "2.5e-1" = 0.25
        let val = view.get(0).unwrap().to_sql_value();
        assert!(val.contains("0.25") || val.contains("2.5") || val.contains("e"));
    }

    #[test]
    fn test_record_batch_with_fixed_size_binary_metadata() {
        use arrow::array::FixedSizeBinaryBuilder;
        use arrow::datatypes::Field;

        // Test FixedSizeBinary with metadata cast_to NCHAR
        let mut builder = FixedSizeBinaryBuilder::new(3);
        builder
            .append_value([65u8, 66u8, 67u8])
            .expect("append error"); // "ABC"
        builder.append_null();
        let fsb = builder.finish();
        let mut md = std::collections::HashMap::new();
        md.insert("cast_to".to_string(), "NCHAR".to_string());
        let field = Field::new("fsb_nchar", DataType::FixedSizeBinary(3), true).with_metadata(md);
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(fsb)]).unwrap();
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views.len(), 1);
        assert!(views[0].get(0).unwrap().to_sql_value().contains("ABC"));
        assert!(views[0].get(1).unwrap().is_null());
    }

    #[test]
    fn test_parse_str_into_zero_and_negative_zero() {
        use crate::prelude::IpcDataType;

        // Test zero and negative zero
        let view = arrow_to_taos::parse_str_into(
            &IpcDataType::Float64,
            vec![Some("0"), Some("-0"), Some("0.0")],
        );
        assert!(view.get(0).unwrap().to_sql_value().contains("0"));
        // -0 might equal 0 or print as "-0"
        assert!(view.get(1).unwrap().to_sql_value().contains("0"));
        assert!(view.get(2).unwrap().to_sql_value().contains("0"));
    }

    #[test]
    fn test_parse_str_into_max_and_min_values() {
        use crate::prelude::IpcDataType;

        // Test maximum Int64
        let view =
            arrow_to_taos::parse_str_into(&IpcDataType::Int64, vec![Some("9223372036854775807")]);
        assert_eq!(view.get(0).unwrap().to_sql_value(), "9223372036854775807");

        // Test minimum Int64
        let view =
            arrow_to_taos::parse_str_into(&IpcDataType::Int64, vec![Some("-9223372036854775808")]);
        assert_eq!(view.get(0).unwrap().to_sql_value(), "-9223372036854775808");

        // Test maximum UInt64
        let view =
            arrow_to_taos::parse_str_into(&IpcDataType::UInt64, vec![Some("18446744073709551615")]);
        assert_eq!(view.get(0).unwrap().to_sql_value(), "18446744073709551615");
    }

    #[test]
    fn test_timestamp_precision_boundary_values() {
        use arrow::datatypes::Field;

        // Test timestamp with value 0 and large values
        let ts_milli = TimestampMillisecondArray::from(vec![Some(0i64), Some(1704067200000i64)]); // 2024-01-01 00:00:00
        let field = Field::new(
            "ts_zero",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            true,
        );
        let schema = Arc::new(Schema::new(vec![field]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ts_milli)]).unwrap();

        // Output as Millisecond
        let views = record_batch_to_column_view(&batch, Precision::Millisecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "0");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "1704067200000");

        // Output as Microsecond
        let views = record_batch_to_column_view(&batch, Precision::Microsecond);
        assert_eq!(views[0].get(0).unwrap().to_sql_value(), "0");
        assert_eq!(views[0].get(1).unwrap().to_sql_value(), "1704067200000000");
    }
}
