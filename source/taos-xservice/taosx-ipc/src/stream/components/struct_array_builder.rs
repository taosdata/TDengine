use std::{
    any::{Any, TypeId},
    borrow::Cow,
};

use arrow::{
    array::{
        ArrayBuilder, BinaryBuilder, StringBuilder, StructArray, StructBuilder,
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
        TimestampSecondBuilder, make_builder,
    },
    datatypes::{Field, TimeUnit},
    error::ArrowError,
};

use arrow::datatypes::DataType as ArrowDataType;

pub struct StructArrayBuilder {
    fields: Vec<Field>,
    builder: StructBuilder,
    batch: usize,
    index: Option<usize>,
}

impl StructArrayBuilder {
    pub fn new(fields: Vec<Field>, capacity: usize) -> Self {
        let field_builders = fields
            .iter()
            .map(|f| make_builder(f.data_type(), capacity))
            .collect();
        let builder = StructBuilder::new(fields.clone(), field_builders);
        Self {
            fields,
            builder,
            batch: 0,
            index: None,
        }
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.builder.len()
    }

    pub fn append_null_row(&mut self) -> &mut Self {
        self.next_n(1);
        for _ in 0..self.fields.len() {
            self.append_null();
        }
        self
    }

    /// Fill nulls to the end of row.
    pub fn fill_nulls_to_end(&mut self) -> &mut Self {
        if let Some(index) = self.index {
            for _ in index..self.fields.len() {
                self.append_null();
            }
        }
        self
    }

    pub fn append_null(&mut self) -> &mut Self {
        let idx = match self.index {
            None => {
                self.next_n(1);
                0
            }
            Some(i) => {
                if i >= self.fields.len() {
                    self.next_n(1);
                    0
                } else {
                    self.index.unwrap()
                }
            }
        };
        let dt = self.fields[idx].data_type();

        macro_rules! primitive_append {
            ($a:ident, $t:ident) => {{
                let b = self.builder.field_builder::<arrow::array::$a>(idx).unwrap();
                b.append_null();
            }};
        }
        match dt {
            ArrowDataType::Null => todo!(),
            ArrowDataType::Boolean => primitive_append!(BooleanBuilder, bool),
            ArrowDataType::Int8 => primitive_append!(Int8Builder, i8),
            ArrowDataType::Int16 => primitive_append!(Int16Builder, i16),
            ArrowDataType::Int32 => primitive_append!(Int32Builder, i32),
            ArrowDataType::Int64 => primitive_append!(Int64Builder, i64),
            ArrowDataType::UInt8 => primitive_append!(UInt8Builder, u8),
            ArrowDataType::UInt16 => primitive_append!(UInt16Builder, u16),
            ArrowDataType::UInt32 => primitive_append!(UInt32Builder, u32),
            ArrowDataType::UInt64 => primitive_append!(UInt64Builder, u64),
            ArrowDataType::Float16 => primitive_append!(Float32Builder, f32),
            ArrowDataType::Float32 => primitive_append!(Float32Builder, f32),
            ArrowDataType::Float64 => primitive_append!(Float64Builder, f64),
            ArrowDataType::Timestamp(unit, _) => match unit {
                TimeUnit::Microsecond => self
                    .builder
                    .field_builder::<TimestampMicrosecondBuilder>(idx)
                    .unwrap()
                    .append_null(),
                TimeUnit::Second => self
                    .builder
                    .field_builder::<TimestampSecondBuilder>(idx)
                    .unwrap()
                    .append_null(),
                TimeUnit::Millisecond => self
                    .builder
                    .field_builder::<TimestampMillisecondBuilder>(idx)
                    .unwrap()
                    .append_null(),
                TimeUnit::Nanosecond => self
                    .builder
                    .field_builder::<TimestampNanosecondBuilder>(idx)
                    .unwrap()
                    .append_null(),
            },
            ArrowDataType::Binary => self
                .builder
                .field_builder::<BinaryBuilder>(idx)
                .unwrap()
                .append_null(),
            ArrowDataType::Utf8 => {
                self.builder
                    .field_builder::<StringBuilder>(idx)
                    .unwrap()
                    .append_null();
            }
            _ => {
                panic!("Unsupported data type: {dt:?}");
            }
        };
        self.index.replace(idx + 1);

        self
    }

    pub fn append(&mut self, value: &dyn Any) -> Result<&mut Self, ArrowError> {
        let idx = match self.index {
            None => {
                self.next_n(1);
                0
            }
            Some(i) => {
                if i >= self.fields.len() {
                    self.next_n(1);
                    0
                } else {
                    self.index.unwrap()
                }
            }
        };
        let dt = self.fields[idx].data_type();

        macro_rules! primitive_append {
            ($a:ident, $t:ident) => {{
                let v = value.downcast_ref::<$t>().unwrap();
                let b = self.builder.field_builder::<arrow::array::$a>(idx).unwrap();
                b.append_value(*v);
            }};
        }
        match dt {
            ArrowDataType::Null => todo!(),
            ArrowDataType::Boolean => primitive_append!(BooleanBuilder, bool),
            ArrowDataType::Int8 => primitive_append!(Int8Builder, i8),
            ArrowDataType::Int16 => primitive_append!(Int16Builder, i16),
            ArrowDataType::Int32 => primitive_append!(Int32Builder, i32),
            ArrowDataType::Int64 => primitive_append!(Int64Builder, i64),
            ArrowDataType::UInt8 => primitive_append!(UInt8Builder, u8),
            ArrowDataType::UInt16 => primitive_append!(UInt16Builder, u16),
            ArrowDataType::UInt32 => primitive_append!(UInt32Builder, u32),
            ArrowDataType::UInt64 => primitive_append!(UInt64Builder, u64),
            ArrowDataType::Float16 => primitive_append!(Float32Builder, f32),
            ArrowDataType::Float32 => primitive_append!(Float32Builder, f32),
            ArrowDataType::Float64 => primitive_append!(Float64Builder, f64),
            ArrowDataType::Timestamp(unit, _) => {
                let v = value.downcast_ref::<i64>().unwrap();
                match unit {
                    TimeUnit::Microsecond => {
                        let b = self
                            .builder
                            .field_builder::<TimestampMicrosecondBuilder>(idx)
                            .unwrap();
                        b.append_value(*v);
                    }
                    TimeUnit::Second => {
                        let b = self
                            .builder
                            .field_builder::<TimestampSecondBuilder>(idx)
                            .unwrap();
                        b.append_value(*v);
                    }
                    TimeUnit::Millisecond => {
                        let b = self
                            .builder
                            .field_builder::<TimestampMillisecondBuilder>(idx)
                            .unwrap();
                        b.append_value(*v);
                    }
                    TimeUnit::Nanosecond => {
                        let b = self
                            .builder
                            .field_builder::<TimestampNanosecondBuilder>(idx)
                            .unwrap();
                        b.append_value(*v);
                    }
                }
            }
            ArrowDataType::Binary => {
                let b = self.builder.field_builder::<BinaryBuilder>(idx).unwrap();
                match value.type_id() {
                    t if t == TypeId::of::<&str>() => {
                        b.append_value(value.downcast_ref::<&str>().unwrap())
                    }
                    t if t == TypeId::of::<&&str>() => {
                        b.append_value(value.downcast_ref::<&&str>().unwrap())
                    }
                    t if t == TypeId::of::<String>() => {
                        b.append_value(value.downcast_ref::<String>().unwrap())
                    }
                    t if t == TypeId::of::<&String>() => {
                        b.append_value(value.downcast_ref::<&String>().unwrap())
                    }
                    t if t == TypeId::of::<&&String>() => {
                        b.append_value(value.downcast_ref::<&&String>().unwrap())
                    }
                    t if t == TypeId::of::<[u8]>() => {
                        b.append_value(value.downcast_ref::<&[u8]>().unwrap())
                    }
                    t => panic!("Unsupported binary input type: {t:?}, {value:?}"),
                }
            }
            ArrowDataType::Utf8 => {
                // let v = value.downcast::<String>().unwrap();
                let b = self.builder.field_builder::<StringBuilder>(idx).unwrap();

                match value.type_id() {
                    t if t == TypeId::of::<&str>() => {
                        b.append_value(value.downcast_ref::<&str>().unwrap())
                    }
                    t if t == TypeId::of::<&&str>() => {
                        b.append_value(value.downcast_ref::<&&str>().unwrap())
                    }
                    t if t == TypeId::of::<String>() => {
                        b.append_value(value.downcast_ref::<String>().unwrap())
                    }
                    t if t == TypeId::of::<&String>() => {
                        b.append_value(value.downcast_ref::<&String>().unwrap())
                    }
                    t if t == TypeId::of::<&&String>() => {
                        b.append_value(value.downcast_ref::<&&String>().unwrap())
                    }
                    t if t == TypeId::of::<Box<[u8]>>() => b.append_value({
                        std::str::from_utf8(value.downcast_ref::<&[u8]>().unwrap()).unwrap()
                    }),
                    t => panic!("Unsupported binary input type: {t:?}, {value:?}"),
                }
            }
            _ => {
                return Err(ArrowError::NotYetImplemented(format!(
                    "Unsupported data type to append: {dt:?}"
                )));
            }
        };
        if let Some(v) = self.index.as_mut() {
            *v += 1
        }

        Ok(self)
    }

    pub fn append_values(&mut self, value: &dyn Any, len: usize) -> Result<&mut Self, ArrowError> {
        let idx = match self.index {
            None => {
                self.next_n(len);
                0
            }
            Some(i) => {
                if i >= self.fields.len() {
                    self.next_n(len);
                    0
                } else {
                    self.index.unwrap()
                }
            }
        };
        let dt = self.fields[idx].data_type();
        // self.index += 1;
        macro_rules! primitive_append {
            ($a:ident, $t:ident) => {{
                let v = value.downcast_ref::<&[$t]>().unwrap();
                let builder = self.builder.field_builder::<arrow::array::$a>(idx).unwrap();
                let is_valid = vec![true; v.len()];
                let _ = builder.append_values(*v, &is_valid);
            }};
        }
        match dt {
            ArrowDataType::Null => unreachable!(),
            ArrowDataType::Boolean => primitive_append!(BooleanBuilder, bool),
            ArrowDataType::Int8 => primitive_append!(Int8Builder, i8),
            ArrowDataType::Int16 => primitive_append!(Int16Builder, i16),
            ArrowDataType::Int32 => primitive_append!(Int32Builder, i32),
            ArrowDataType::Int64 => primitive_append!(Int64Builder, i64),
            ArrowDataType::UInt8 => primitive_append!(UInt8Builder, u8),
            ArrowDataType::UInt16 => primitive_append!(UInt16Builder, u16),
            ArrowDataType::UInt32 => primitive_append!(UInt32Builder, u32),
            ArrowDataType::UInt64 => primitive_append!(UInt64Builder, u64),
            ArrowDataType::Float16 => primitive_append!(Float32Builder, f32),
            ArrowDataType::Float32 => primitive_append!(Float32Builder, f32),
            ArrowDataType::Float64 => primitive_append!(Float64Builder, f64),
            ArrowDataType::Timestamp(unit, _) => {
                let v = value.downcast_ref::<&[i64]>().unwrap();
                let is_valid = vec![true; v.len()];
                match unit {
                    TimeUnit::Microsecond => self
                        .builder
                        .field_builder::<TimestampMicrosecondBuilder>(idx)
                        .unwrap()
                        .append_values(v, &is_valid),
                    TimeUnit::Second => self
                        .builder
                        .field_builder::<TimestampSecondBuilder>(idx)
                        .unwrap()
                        .append_values(v, &is_valid),
                    TimeUnit::Millisecond => self
                        .builder
                        .field_builder::<TimestampMillisecondBuilder>(idx)
                        .unwrap()
                        .append_values(v, &is_valid),
                    TimeUnit::Nanosecond => self
                        .builder
                        .field_builder::<TimestampNanosecondBuilder>(idx)
                        .unwrap()
                        .append_values(v, &is_valid),
                }
            }
            ArrowDataType::Binary => {
                let b = self.builder.field_builder::<BinaryBuilder>(idx).unwrap();
                macro_rules! append {
                    ($t:ty) => {{
                        for v in *value.downcast_ref::<$t>().unwrap() {
                            b.append_value(*v)
                        }
                    }};
                }
                match value.type_id() {
                    t if t == TypeId::of::<&[&str]>() => append!(&[&str]),
                    t if t == TypeId::of::<&&str>() => {
                        b.append_value(value.downcast_ref::<&&str>().unwrap())
                    }
                    t if t == TypeId::of::<String>() => {
                        b.append_value(value.downcast_ref::<String>().unwrap())
                    }
                    t if t == TypeId::of::<&String>() => {
                        b.append_value(value.downcast_ref::<&String>().unwrap())
                    }
                    t if t == TypeId::of::<&&String>() => {
                        b.append_value(value.downcast_ref::<&&String>().unwrap())
                    }
                    t if t == TypeId::of::<[u8]>() => {
                        b.append_value(value.downcast_ref::<&[u8]>().unwrap())
                    }
                    t => panic!("Unsupported binary input type: {t:?}, {value:?}"),
                }
            }
            ArrowDataType::Utf8 => {
                // let v = value.downcast::<String>().unwrap();
                let b = self.builder.field_builder::<StringBuilder>(idx).unwrap();
                macro_rules! append {
                    ($t:ty) => {{
                        for v in value.downcast_ref::<$t>().unwrap().iter() {
                            b.append_value(v)
                        }
                    }};
                }
                macro_rules! append_bytes {
                    ($t:ty) => {{
                        for v in value.downcast_ref::<$t>().unwrap().iter() {
                            b.append_value(unsafe { std::str::from_utf8_unchecked(v) })
                        }
                    }};
                }
                match value.type_id() {
                    t if t == TypeId::of::<Vec<&str>>() => append!(Vec<&str>),
                    t if t == TypeId::of::<Vec<String>>() => append!(Vec<String>),
                    t if t == TypeId::of::<&[&str]>() => append!(&[&str]),
                    t if t == TypeId::of::<&[&String]>() => append!(&[&String]),
                    t if t == TypeId::of::<Vec<&[u8]>>() => append_bytes!(Vec<&[u8]>),
                    t if t == TypeId::of::<Vec<Vec<u8>>>() => append_bytes!(Vec<Vec<u8>>),
                    t if t == TypeId::of::<Vec<&Vec<u8>>>() => append_bytes!(Vec<&Vec<u8>>),
                    t if t == TypeId::of::<&&[&str]>() => append!(&&[&str]),
                    t if t == TypeId::of::<Vec<Cow<str>>>() => append!(Vec<Cow<str>>),
                    t => panic!(
                        "Unsupported binary input type: {t:?}, {value:?}, use &Vec<String>, &Vec<&str>, &[&str] or &[&[u8]]"
                    ),
                }
            }
            _ => {
                panic!("Unsupported data type: {dt:?}");
            }
        };
        self.index.replace(idx + 1);
        Ok(self)
    }

    pub fn finish(&mut self) -> StructArray {
        if self.builder.len() == 0 {
            self.builder.append(true);
        }
        self.builder.finish()
    }

    #[inline]
    fn next_n(&mut self, n: usize) -> &mut Self {
        if !self.index.map(|index| index == 0).unwrap_or_default() {
            for _ in 0..n {
                self.builder.append(true);
            }
            self.batch = n;
            self.index = Some(0);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BinaryArray, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array,
        Int32Array, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::DataType;

    fn assert_not_yet_implemented<T>(result: Result<T, ArrowError>, expected: &str) {
        if let Err(ArrowError::NotYetImplemented(message)) = result {
            assert!(
                message.contains(expected),
                "error message {message:?} should mention {expected:?}"
            );
        } else {
            panic!("expected ArrowError::NotYetImplemented");
        }
    }

    #[test]
    fn builder() -> anyhow::Result<()> {
        let fields = vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ];
        let mut builder = StructArrayBuilder::new(fields, 2);

        dbg!(["abc", "def"].type_id());

        // A two-rows item.
        builder
            // .next_item()
            .append_null()
            .append_null()
            // next row
            .next_n(1) // the builder will call it under the hood so this is optional
            .append(&"Hello" as &dyn Any)?
            .append(&1i32 as _)?
            .append_values(&vec!["abc", "def"], 2)?
            .append_values(&[1i32, 2].as_slice(), 2)?
            .append_values(&vec!["abc".to_string(), "def".to_string()], 2)?
            .append_values(&[1i32, 2].as_slice(), 2)?
            .append_values(&["abc", "def"].as_slice(), 2)?
            .append_values(&[1i32, 2].as_slice(), 2)?
            .append_values(&vec![b"abc".as_slice(), b"def".as_slice()], 2)?
            .append_values(&[1i32, 2].as_slice(), 2)?
            .append_values(&vec![b"abc".as_slice(), b"def".as_slice()], 2)?
            .append_values(&[1i32, 2].as_slice(), 2)?;

        let array = builder.finish();
        // assert!(array.value_length(0) == 2);
        dbg!(&array);
        Ok(())
    }

    #[test]
    fn append_various_types_and_finish_empty() -> anyhow::Result<()> {
        // Cover more branches: primitives, timestamps, utf8, binary and finish() empty path
        let fields = vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i", DataType::Int32, true),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new("s", DataType::Utf8, true),
            Field::new("bin", DataType::Binary, true),
        ];
        let mut builder = StructArrayBuilder::new(fields, 2);

        // Append a full row with diverse types
        builder
            .append(&true as &dyn Any)?
            .append(&123i32 as &dyn Any)?
            .append(&(42_000i64) as &dyn Any)?
            .append(&"hello" as &dyn Any)?
            .append(&"bin".to_string() as &dyn Any)?;

        // Append using slices for primitives
        builder
            .next_n(2)
            .append_values(&[false, true].as_slice(), 2)?
            .append_values(&[1i32, 2].as_slice(), 2)?
            .append_values(&[10_000i64, 20_000].as_slice(), 2)?
            .append_values(&["a", "b"].as_slice(), 2)?
            .append_values(&["a", "b"].as_slice(), 2)?;

        let array = builder.finish();
        assert_eq!(array.len(), 3);

        // Append a null row then finish to ensure lengths align
        let fields2 = vec![Field::new("v", DataType::Utf8, true)];
        let mut builder2 = StructArrayBuilder::new(fields2, 1);
        builder2.append_null_row();
        let array2 = builder2.finish();
        assert_eq!(array2.len(), 1);
        Ok(())
    }

    #[test]
    fn append_binary_string_variants() -> anyhow::Result<()> {
        // Ensure different type_id branches for Binary and Utf8 are covered
        let fields = vec![
            Field::new("bin", DataType::Binary, true),
            Field::new("s", DataType::Utf8, true),
        ];
        let mut builder = StructArrayBuilder::new(fields, 2);

        // Binary accepts &str and String
        builder
            .append(&"abc" as &dyn Any)?
            .append(&"xyz".to_string() as &dyn Any)?;

        // Utf8 accepts &str and String
        builder
            .next_n(1)
            .append(&"hello" as &dyn Any)?
            .append(&"world".to_string() as &dyn Any)?;

        let array = builder.finish();
        assert_eq!(array.len(), 2);
        Ok(())
    }

    #[test]
    fn append_more_primitives_struct_builder() -> anyhow::Result<()> {
        // Exercise additional primitive branches for StructArrayBuilder
        let fields = vec![
            Field::new("u8", DataType::UInt8, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new("s", DataType::Utf8, true),
        ];
        let mut builder = StructArrayBuilder::new(fields, 1);
        builder
            .append(&(7u8) as &dyn Any)?
            .append(&(std::f64::consts::E) as &dyn Any)?
            .append(&(123i64) as &dyn Any)?
            .append(&"ok" as &dyn Any)?;
        let array = builder.finish();
        assert_eq!(array.len(), 1);
        Ok(())
    }

    #[test]
    fn append_nulls_and_fill_nulls_keep_struct_row_boundaries() -> anyhow::Result<()> {
        let fields = vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
            Field::new("payload", DataType::Binary, true),
            Field::new(
                "observed_at",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                true,
            ),
        ];
        let mut builder = StructArrayBuilder::new(fields, 3);

        builder
            .append(&"first" as &dyn Any)?
            .append(&10i32 as &dyn Any)?
            .fill_nulls_to_end()
            .append_null_row()
            .append(&"third" as &dyn Any)?
            .append_null()
            .append(&"bytes" as &dyn Any)?
            .append(&1_234i64 as &dyn Any)?;

        let array = builder.finish();
        assert_eq!(array.len(), 3);

        let names = array
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "first");
        assert!(names.is_null(1));
        assert_eq!(names.value(2), "third");

        let values = array
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.value(0), 10);
        assert!(values.is_null(1));
        assert!(values.is_null(2));

        let payloads = array
            .column(2)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert!(payloads.is_null(0));
        assert!(payloads.is_null(1));
        assert_eq!(payloads.value(2), b"bytes");

        let timestamps = array
            .column(3)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert!(timestamps.is_null(0));
        assert!(timestamps.is_null(1));
        assert_eq!(timestamps.value(2), 1_234);

        Ok(())
    }

    #[test]
    fn append_values_aligns_supported_columns_across_rows() -> anyhow::Result<()> {
        let fields = vec![
            Field::new("flag", DataType::Boolean, true),
            Field::new("i8", DataType::Int8, true),
            Field::new("i16", DataType::Int16, true),
            Field::new("i64", DataType::Int64, true),
            Field::new("u8", DataType::UInt8, true),
            Field::new("u32", DataType::UInt32, true),
            Field::new("u64", DataType::UInt64, true),
            Field::new("f32", DataType::Float32, true),
            Field::new("f64", DataType::Float64, true),
            Field::new("ts_s", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new(
                "ts_us",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "ts_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new("label", DataType::Utf8, true),
            Field::new("payload", DataType::Binary, true),
        ];
        let mut builder = StructArrayBuilder::new(fields, 2);
        let labels = vec![Cow::Borrowed("left"), Cow::Owned("right".to_string())];

        builder
            .append_values(&[true, false].as_slice(), 2)?
            .append_values(&[-8i8, 8i8].as_slice(), 2)?
            .append_values(&[-16i16, 16i16].as_slice(), 2)?
            .append_values(&[-64i64, 64i64].as_slice(), 2)?
            .append_values(&[8u8, 9u8].as_slice(), 2)?
            .append_values(&[32u32, 33u32].as_slice(), 2)?
            .append_values(&[64u64, 65u64].as_slice(), 2)?
            .append_values(&[1.25f32, -2.5f32].as_slice(), 2)?
            .append_values(&[3.5f64, -4.75f64].as_slice(), 2)?
            .append_values(&[11i64, 22i64].as_slice(), 2)?
            .append_values(&[1_100i64, 2_200i64].as_slice(), 2)?
            .append_values(&[1_000_000i64, 2_000_000i64].as_slice(), 2)?
            .append_values(&labels, 2)?
            .append_values(&["bin-left", "bin-right"].as_slice(), 2)?;

        let array = builder.finish();
        assert_eq!(array.len(), 2);

        let flags = array
            .column(0)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(flags.value(0));
        assert!(!flags.value(1));

        let i8s = array
            .column(1)
            .as_any()
            .downcast_ref::<Int8Array>()
            .unwrap();
        assert_eq!(i8s.value(0), -8);
        assert_eq!(i8s.value(1), 8);

        let i16s = array
            .column(2)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap();
        assert_eq!(i16s.value(0), -16);
        assert_eq!(i16s.value(1), 16);

        let i64s = array
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(i64s.value(0), -64);
        assert_eq!(i64s.value(1), 64);

        let u8s = array
            .column(4)
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        assert_eq!(u8s.value(0), 8);
        assert_eq!(u8s.value(1), 9);

        let u32s = array
            .column(5)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(u32s.value(0), 32);
        assert_eq!(u32s.value(1), 33);

        let u64s = array
            .column(6)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(u64s.value(0), 64);
        assert_eq!(u64s.value(1), 65);

        let f32s = array
            .column(7)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap();
        assert_eq!(f32s.value(0), 1.25);
        assert_eq!(f32s.value(1), -2.5);

        let f64s = array
            .column(8)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(f64s.value(0), 3.5);
        assert_eq!(f64s.value(1), -4.75);

        let seconds = array
            .column(9)
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap();
        assert_eq!(seconds.value(0), 11);
        assert_eq!(seconds.value(1), 22);

        let micros = array
            .column(10)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(micros.value(0), 1_100);
        assert_eq!(micros.value(1), 2_200);

        let nanos = array
            .column(11)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(nanos.value(0), 1_000_000);
        assert_eq!(nanos.value(1), 2_000_000);

        let strings = array
            .column(12)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(strings.value(0), "left");
        assert_eq!(strings.value(1), "right");

        let binaries = array
            .column(13)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap();
        assert_eq!(binaries.value(0), b"bin-left");
        assert_eq!(binaries.value(1), b"bin-right");

        Ok(())
    }

    #[test]
    fn append_reports_safe_unsupported_types() {
        let fields = vec![Field::new("date", DataType::Date32, true)];
        let mut append_builder = StructArrayBuilder::new(fields, 1);
        assert_not_yet_implemented(
            append_builder.append(&1i32 as &dyn Any).map(|_| ()),
            "Date32",
        );
    }
}
