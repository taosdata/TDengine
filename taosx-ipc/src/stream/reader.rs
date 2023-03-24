use std::{any::Any, collections::HashMap, io::Read, sync::Arc};

use arrow::{
    array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Float16Array, Float32Array, Float64Array,
        Int16Array, Int32Array, Int64Array, Int8Array, ListArray, StringArray, StructArray,
        TimestampMillisecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    },
    datatypes::{DataType, Schema},
    error::ArrowError,
    ipc::reader::StreamReader,
    record_batch::RecordBatch,
};
use taos_query::prelude::Itertools;
use taos_query::prelude::{ColumnView, Ty, Value};

use crate::{
    ack::AckType,
    constants::{__ATTRS__, __RECORDS__, __TABLES__INDEX__, __TABLE_NAME__, __TYPE__},
    prelude::{IpcMetadata, LushMessageType, StreamType},
    stream::point::PointMessage,
};

pub struct IpcReader<R: Read> {
    metadata: IpcMetadata,
    schema: Arc<Schema>,
    reader: StreamReader<R>,
}

impl<R: Read> IpcReader<R> {
    pub fn new(reader: R) -> Result<Self, ArrowError> {
        let reader = StreamReader::try_new(reader, None)?;
        let schema = reader.schema();
        let metadata = schema.metadata().into();
        Ok(Self {
            metadata,
            schema,
            reader,
        })
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
                            DataType::Union(_, _, _) => todo!(),
                            DataType::Dictionary(_, _) => todo!(),
                            DataType::Decimal128(_, _) => todo!(),
                            DataType::Decimal256(_, _) => todo!(),
                            DataType::Map(_, _) => todo!(),
                            DataType::RunEndEncoded(_, _) => todo!(),
                            _ => todo!("Unsupported data type for tag"),
                        };
                        (name.to_string(), value)
                    })
                    .collect_vec();
                // let (name, values) = values.split_at(1);
                let name = values.remove(0);

                let s = LushInsertAttrs {
                    name: name.1.strict_as_str().to_string(),
                    using: Some(using.to_string()),
                    tags: Some(values),
                };
                dbg!(s)
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
        let name = std::str::from_utf8(name).unwrap().to_string();

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
                            DataType::Union(_, _, _) => todo!(),
                            DataType::Dictionary(_, _) => todo!(),
                            DataType::Decimal128(_, _) => todo!(),
                            DataType::Decimal256(_, _) => todo!(),
                            DataType::Map(_, _) => todo!(),
                            DataType::RunEndEncoded(_, _) => todo!(),
                            _ => todo!("Unsupported data type for tag"),
                        };
                        (name.to_string(), value)
                    })
                    .collect_vec();
                // let (name, values) = values.split_at(1);
                let name = values.remove(0);

                let s = LushInsertAttrs {
                    name: name.1.strict_as_str().to_string(),
                    using: Some(using.to_string()),
                    tags: Some(values),
                };
                dbg!(s)
            })
            .collect_vec()
            .into_iter()
            .next()
    }
}

#[derive(Debug)]
pub struct LushInsertAttrs {
    name: String,
    using: Option<String>,
    tags: Option<Vec<(String, Value)>>,
}

impl LushInsertAttrs {
    pub fn to_sql(&self, table_name: Option<String>) -> Option<String> {
        if let Some(using) = self.using.as_ref() {
            let tags = self.tags.as_ref().unwrap();
            let table = if table_name.is_none() {
                &self.name
            } else {
                table_name.as_ref().unwrap()
            };
            let names = tags.iter().map(|(name, _)| format!("`{name}`")).join(",");
            let values = tags.iter().map(|(_, value)| value.to_sql_value()).join(",");
            Some(format!(
                "CREATE TABLE IF NOT EXISTS `{table}` USING `{using}` ({names}) TAGS({values})"
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

impl From<Arc<dyn Array>> for LushInsertAttrs {
    fn from(value: Arc<dyn Array>) -> Self {
        let s = value
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("parse attrs struct");
        assert!(s.len() == 1);
        let name = s
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("get table name")
            .value(0);
        let name = std::str::from_utf8(name).unwrap().to_string();

        let array = s
            .column(1)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("get table name");
        let (using, tags) = if array.is_null(0) {
            (None, None)
        } else {
            let tags = s
                .column(2)
                .as_any()
                .downcast_ref::<StructArray>()
                .expect("get tags");
            let values: Vec<_> = tags
                .column_names()
                .into_iter()
                .zip(tags.columns())
                .map(|(name, col)| {
                    macro_rules! primitive_downcast {
                        ($a:ident,$t:ident) => {{
                            let v = col.as_any().downcast_ref::<arrow::array::$a>().unwrap();
                            if v.is_null(0) {
                                Value::Null(Ty::$t)
                            } else {
                                Value::$t(v.value(0))
                            }
                        }};
                    }
                    let value = match col.data_type() {
                        DataType::Null => todo!(),
                        DataType::Boolean => Value::Bool(true),
                        DataType::Int8 => {
                            let v = col.as_any().downcast_ref::<Int8Array>().unwrap();
                            if v.is_null(0) {
                                Value::Null(Ty::TinyInt)
                            } else {
                                Value::TinyInt(v.value(0))
                            }
                        }
                        DataType::Int16 => {
                            let v = col.as_any().downcast_ref::<Int16Array>().unwrap();
                            if v.is_null(0) {
                                Value::Null(Ty::SmallInt)
                            } else {
                                Value::SmallInt(v.value(0))
                            }
                        }
                        DataType::Int32 => {
                            let v = col.as_any().downcast_ref::<Int32Array>().unwrap();
                            if v.is_null(0) {
                                Value::Null(Ty::Int)
                            } else {
                                Value::Int(v.value(0))
                            }
                        }
                        DataType::Int64 => {
                            let v = col.as_any().downcast_ref::<Int64Array>().unwrap();
                            if v.is_null(0) {
                                Value::Null(Ty::BigInt)
                            } else {
                                Value::BigInt(v.value(0))
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
                            if v.is_null(0) {
                                Value::Null(Ty::VarChar)
                            } else {
                                Value::VarChar(std::str::from_utf8(v.value(0)).unwrap().to_string())
                            }
                        }
                        DataType::FixedSizeBinary(_) => todo!(),
                        DataType::LargeBinary => todo!(),
                        DataType::Utf8 => {
                            let v = col.as_any().downcast_ref::<StringArray>().unwrap();
                            if v.is_null(0) {
                                Value::Null(Ty::VarChar)
                            } else {
                                Value::VarChar(v.value(0).to_string())
                            }
                        }
                        DataType::LargeUtf8 => todo!(),
                        DataType::List(_) => todo!(),
                        DataType::FixedSizeList(_, _) => todo!(),
                        DataType::LargeList(_) => todo!(),
                        DataType::Struct(_) => todo!(),
                        DataType::Union(_, _, _) => todo!(),
                        DataType::Dictionary(_, _) => todo!(),
                        DataType::Decimal128(_, _) => todo!(),
                        DataType::Decimal256(_, _) => todo!(),
                        DataType::Map(_, _) => todo!(),
                        DataType::RunEndEncoded(_, _) => todo!(),
                        _ => todo!("Unsupported data type for tag"),
                    };
                    (name.to_string(), value)
                })
                .collect();

            (
                Some(std::str::from_utf8(array.value(0)).unwrap().to_string()),
                Some(values),
            )
        };
        Self { name, using, tags }
    }
}

#[derive(Debug)]
pub struct LushMessageInsert {
    attrs: Option<LushInsertAttrs>,
    records: LushInsertRecords,
}

impl LushMessageInsert {
    pub fn num_rows(&self) -> usize {
        self.records.record.num_rows()
    }

    pub fn meta_sql(&self, table_name: Option<String>) -> Option<String> {
        self.attrs.as_ref().and_then(|attr| attr.to_sql(table_name))
    }

    pub fn table(&self) -> Option<&str> {
        self.attrs.as_ref().map(|attr| attr.name.as_str())
    }

    pub fn to_column_views(&self) -> Vec<ColumnView> {
        record_batch_to_cloumn_view(&self.records.record)
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
                            dbg!(&c);
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
            }
        }
        map
    }
}

pub fn record_batch_to_cloumn_view(record: &RecordBatch) -> Vec<ColumnView> {
    record
        .columns()
        .iter()
        .map(|column| {
            // dbg!(column);
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
                    arrow::datatypes::TimeUnit::Microsecond => todo!(),
                    arrow::datatypes::TimeUnit::Nanosecond => todo!(),
                },
                DataType::Date32 => todo!(),
                DataType::Date64 => todo!(),
                DataType::Time32(_) => todo!(),
                DataType::Time64(_) => todo!(),
                DataType::Duration(_) => todo!(),
                DataType::Interval(_) => todo!(),
                DataType::Binary => {
                    let a = column.as_any().downcast_ref::<BinaryArray>().unwrap();

                    ColumnView::from_varchar::<&str, _, _, _>(
                        (0..a.len())
                            .map(|i| {
                                if a.is_null(i) {
                                    None
                                } else {
                                    Some(unsafe { std::str::from_utf8_unchecked(a.value(i)) })
                                }
                            })
                            .collect_vec(),
                    )
                }
                DataType::FixedSizeBinary(_) => todo!(),
                DataType::LargeBinary => todo!(),
                DataType::Utf8 => {
                    let a = column.as_any().downcast_ref::<StringArray>().unwrap();
                    ColumnView::from_varchar::<&str, _, _, _>(
                        (0..a.len())
                            .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                            .collect_vec(),
                    )
                }
                DataType::LargeUtf8 => todo!(),
                DataType::List(_) => todo!(),
                DataType::FixedSizeList(_, _) => todo!(),
                DataType::LargeList(_) => todo!(),
                DataType::Struct(_) => todo!(),
                DataType::Union(_, _, _) => todo!(),
                DataType::Dictionary(_, _) => todo!(),
                DataType::Decimal128(_, _) => todo!(),
                DataType::Decimal256(_, _) => todo!(),
                DataType::Map(_, _) => todo!(),
                DataType::RunEndEncoded(_, _) => todo!(),
            }
        })
        .collect()
}

#[derive(Debug)]
pub struct LushMessageTable {}

#[derive(Debug)]
pub enum LushMessage {
    Tables(Vec<LushInsertAttrs>),
    Insert(Vec<LushMessageInsert>),
}
// pub struct LushMessageTables(Vec<LushInsertAttrs>);
pub trait IpcMessage: Any + Send {
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
        println!("Next message in the stream");
        let res = self.reader.next()?;

        if let Ok(record) = res {
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
                            let tables = record.column(__TABLES__INDEX__);

                            let tables = (0..tables.len())
                                .flat_map(|i| {
                                    let tables = tables.slice(i, 1);
                                    self.parse_tables(tables).into_iter()
                                })
                                .collect_vec();
                            return Some(Ok(Box::new(LushMessage::Tables(tables))));
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
                                    dbg!(&records);
                                    let i = LushMessageInsert { attrs, records };
                                    message.push(i);
                                }
                                return Some(Ok(Box::new(LushMessage::Insert(message))));
                            } else {
                                let values = record.column_by_name(__RECORDS__).unwrap();

                                // debug_assert!(values.len() == 1);

                                let mut message = Vec::with_capacity(values.len());
                                for i in 0..values.len() {
                                    let records: LushInsertRecords = values.slice(i, 1).into();
                                    dbg!(&records);
                                    let i = LushMessageInsert {
                                        attrs: None,
                                        records,
                                    };
                                    message.push(i);
                                }
                                return Some(Ok(Box::new(LushMessage::Insert(message))));
                            }
                        }
                    }
                }
                StreamType::Point => {
                    let values = record.column_by_name(__RECORDS__).unwrap();
                    let mut records = Vec::with_capacity(values.len());
                    for i in 0..values.len() {
                        let record = values.slice(i, 1).into();
                        records.push(record);
                    }
                    return Some(Ok(Box::new(PointMessage::new(records))));
                }
                _ => todo!(),
            }
        }
        None
    }
}

#[test]
fn file_reader() -> anyhow::Result<()> {
    use std::io::prelude::*;

    #[cfg(not(target_os = "windows"))]
    let stream = std::os::unix::net::UnixStream::connect("../taosx.sock")?;
    #[cfg(target_os = "windows")]
    let stream = std::net::TcpStream::connect("127.0.0.1:6051")?;

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
                    let map_data = record.to_column_views_group_by_tablename();
                    dbg!(&map_data);
                }
            }
            LushMessage::Tables(tables) => {
                for record in tables {
                    let map_data = record.to_sql(None);
                    dbg!(&map_data);
                }
            }
        }
    }

    (&stream).write_all(zin.as_slice()).unwrap();
    Ok(())
}
