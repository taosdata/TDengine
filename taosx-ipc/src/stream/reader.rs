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
        Array, ArrayRef, BinaryArray, BooleanArray, Float16Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray,
        ListArray, StringArray, StructArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
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
                        return Ok(Box::new(LushMessage::Tables(tables, full_records)));
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
                                ArrowError::InvalidArgumentError(format!(
                                    "__control__ should be StringArray"
                                ))
                            })?;
                        let control = s.value(0);
                        tracing::info!("Receive control message: {}", control);
                        let control: LushMessageControl =
                            serde_json::from_str(control).expect("Parse LushMessageControl error");
                        return Ok(Box::new(LushMessage::Control(control)));
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
                            return Ok(Box::new(LushMessage::Insert(message)));
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
                            return Ok(Box::new(LushMessage::Insert(message)));
                        }
                    }
                }
            }
            StreamType::Point => {
                let record = RecordMessage { record };
                return Ok(Box::new(PointMessage::new(vec![record])));
            }
            StreamType::Flat => {
                let record = RecordMessage { record };
                return Ok(Box::new(FlatMessage::new(vec![record])));
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
                            DataType::Decimal128(_, _) => todo!(),
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
                            DataType::Decimal128(_, _) => todo!(),
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
        let reader = StreamReader::try_new(reader, None)?;
        let schema = reader.schema();
        let parser = IpcParser::new(schema);
        Ok(Self { parser, reader })
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
            let tags = self.tags.as_ref().unwrap();
            let table = if table_name.is_none() {
                &self.name
            } else {
                table_name.unwrap()
            };
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
                        v.and_then(|v| {
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
                            Some(bytes)
                        })
                    })
                    .collect::<Vec<_>>();
                ColumnView::from_bytes::<Vec<u8>, _, _, _>(v)
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

    pub fn to_column_views(&self) -> Vec<ColumnView> {
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
                    .map(Clone::clone)
                    .unwrap()
            })
            .collect_vec();
        parse_column_view_with_types(&self.records.record, &ty)
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
        data: &Vec<ColumnView>,
        columns: &'b Vec<String>,
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
                let mut sql = format!("INSERT INTO ");
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
                                metrics::counter!("ipc.stream.points", 1);
                                insert_columns.push_str(format!("`{}`,", column_name).as_str());
                                insert_values.push_str("NULL,");
                                tracing::warn!(row = j, col = n, "Set column to NULL");
                            } else if !v.is_null() {
                                let sql_value = v.to_sql_value();
                                let v_ty = v.ty();
                                if v_ty.is_var_type() {
                                    let field_ipc_type = field_map.get_mut(column_name);
                                    if field_ipc_type.is_some() {
                                        let field_ipc_type = field_ipc_type.unwrap();
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
                                metrics::counter!("ipc.stream.points", 1);
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

    pub fn to_column_views_group_by_tablename(&self) -> HashMap<Option<String>, Vec<ColumnView>> {
        let mut index = None;
        for (i, f) in self.records.record.schema().fields().iter().enumerate() {
            if f.name() == __TABLE_NAME__ {
                index = Some(i);
            }
        }
        let data = self.to_column_views();
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
                                if exist_cv.is_none() {
                                    // insert ColumnView
                                    c.push(cv.slice(j..j + 1).unwrap());
                                } else {
                                    // ColumnView insert
                                    let old_exist_cv = exist_cv.unwrap();
                                    let new_value = cv.slice(j..j + 1).unwrap();
                                    c[data_i] = old_exist_cv.concat(&new_value);
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
                    arrow::datatypes::TimeUnit::Second => todo!(),
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
                DataType::LargeBinary => todo!(),
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
        .map(|(column, field)| match column.data_type() {
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
                    arrow::compute::cast(column, &DataType::Timestamp(precision.clone(), None))
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

                match field.metadata().get("cast_to").map(|s| s.as_str()) {
                    Some("NCHAR") => ColumnView::from_nchar::<&str, _, _, _>(iter),
                    _ => ColumnView::from_varchar::<&str, _, _, _>(iter),
                }
            }
            DataType::Utf8 => {
                let a = column.as_any().downcast_ref::<StringArray>().unwrap();
                match field.metadata().get("cast_to").map(|s| s.as_str()) {
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
            _ => todo!(),
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
}

impl IpcMessage for LushMessage {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl<R: Read> Iterator for IpcReader<R> {
    type Item = Result<Box<dyn IpcMessage>, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.reader.next()?;
        // let res = loop {
        //     debug!("Getting next");
        //     if let Some(res) = self.reader.next() {
        //         break res;
        //     }
        // };
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

#[test]
#[ignore] // todo: fix this test
fn file_reader() -> anyhow::Result<()> {
    use std::io::prelude::*;

    // #[cfg(not(target_os = "windows"))]
    // let stream = std::os::unix::net::UnixStream::connect("../taosx.sock")?;
    // #[cfg(target_os = "windows")]
    // let stream = std::net::TcpStream::connect("127.0.0.1:6051")?;

    let mut file = std::fs::File::open("./examples/dotnet/dotnet.arrow.zstd")?;
    let mut bytes = vec![];
    file.read_to_end(&mut bytes)?;
    let zin = zstd::decode_all(bytes.as_slice())?;

    let reader = IpcReader::new(zin.as_slice()).unwrap();

    dbg!(reader.metadata());
    dbg!(&reader.schema);

    for records in reader {
        let res = records.unwrap();
        let record = res.as_any().downcast_ref::<LushMessage>().unwrap();
        match record {
            LushMessage::Insert(records) => {
                for record in records {
                    let map_data = record.to_column_views();
                    dbg!(&map_data);

                    let columns = vec!["ts".to_string(), "c1".to_string()];
                    let sqls = record.generate_insert_sql_from_tablename(&map_data, &columns);
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

    // (&stream).write_all(zin.as_slice()).unwrap();
    Ok(())
}
