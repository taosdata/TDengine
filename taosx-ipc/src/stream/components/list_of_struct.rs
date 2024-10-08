use std::{
    any::{Any, TypeId},
    borrow::Cow,
};

use arrow::{
    array::{
        make_builder, ArrayBuilder, BinaryBuilder, ListBuilder, StringBuilder, StructBuilder,
        TimestampMicrosecondBuilder, TimestampMillisecondBuilder, TimestampNanosecondBuilder,
        TimestampSecondBuilder,
    },
    datatypes::{Field, TimeUnit},
    error::ArrowError,
};

use arrow::datatypes::DataType as ArrowDataType;

pub struct ListOfStructBuilder {
    fields: Vec<Field>,
    builder: ListBuilder<StructBuilder>,
    batch: usize,
    index: Option<usize>,
}

impl ListOfStructBuilder {
    pub fn new(fields: Vec<Field>, capacity: usize) -> Self {
        let field_builders = fields
            .iter()
            .map(|f| make_builder(f.data_type(), capacity))
            .collect();
        let values_builder = StructBuilder::new(fields.clone(), field_builders);
        let builder = ListBuilder::new(values_builder);
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

    pub fn values(&mut self) -> &mut StructBuilder {
        self.builder.values()
    }

    pub fn append_null_row(&mut self) -> &mut Self {
        self.next_n(1);
        for _i in 0..self.fields.len() {
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
                let b = self
                    .builder
                    .values()
                    .field_builder::<arrow::array::$a>(idx)
                    .unwrap();
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
                    .values()
                    .field_builder::<TimestampMicrosecondBuilder>(idx)
                    .unwrap()
                    .append_null(),
                TimeUnit::Second => self
                    .builder
                    .values()
                    .field_builder::<TimestampSecondBuilder>(idx)
                    .unwrap()
                    .append_null(),
                TimeUnit::Millisecond => self
                    .builder
                    .values()
                    .field_builder::<TimestampMillisecondBuilder>(idx)
                    .unwrap()
                    .append_null(),
                TimeUnit::Nanosecond => self
                    .builder
                    .values()
                    .field_builder::<TimestampNanosecondBuilder>(idx)
                    .unwrap()
                    .append_null(),
            },
            ArrowDataType::Binary => self
                .builder
                .values()
                .field_builder::<BinaryBuilder>(idx)
                .unwrap()
                .append_null(),
            ArrowDataType::Utf8 => {
                // dbg!(&self.builder);
                self.builder
                    .values()
                    .field_builder::<StringBuilder>(idx)
                    .unwrap()
                    .append_null();
            }
            ArrowDataType::Date32 => todo!(),
            ArrowDataType::Date64 => todo!(),
            ArrowDataType::Time32(_) => todo!(),
            ArrowDataType::Time64(_) => todo!(),
            ArrowDataType::Duration(_) => todo!(),
            ArrowDataType::Interval(_) => todo!(),
            ArrowDataType::FixedSizeBinary(_) => todo!(),
            ArrowDataType::LargeBinary => todo!(),
            ArrowDataType::LargeUtf8 => todo!(),
            ArrowDataType::List(_) => todo!(),
            ArrowDataType::FixedSizeList(_, _) => todo!(),
            ArrowDataType::LargeList(_) => todo!(),
            ArrowDataType::Struct(_) => todo!(),
            ArrowDataType::Union(_, _) => todo!(),
            ArrowDataType::Dictionary(_, _) => todo!(),
            ArrowDataType::Decimal128(_, _) => todo!(),
            ArrowDataType::Decimal256(_, _) => todo!(),
            ArrowDataType::Map(_, _) => todo!(),
            ArrowDataType::RunEndEncoded(_, _) => todo!(),
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
                let b = self
                    .builder
                    .values()
                    .field_builder::<arrow::array::$a>(idx)
                    .unwrap();
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
                            .values()
                            .field_builder::<TimestampMicrosecondBuilder>(idx)
                            .unwrap();
                        b.append_value(*v);
                    }
                    TimeUnit::Second => {
                        let b = self
                            .builder
                            .values()
                            .field_builder::<TimestampSecondBuilder>(idx)
                            .unwrap();
                        b.append_value(*v);
                    }
                    TimeUnit::Millisecond => {
                        let b = self
                            .builder
                            .values()
                            .field_builder::<TimestampMillisecondBuilder>(idx)
                            .unwrap();
                        b.append_value(*v);
                    }
                    TimeUnit::Nanosecond => {
                        let b = self
                            .builder
                            .values()
                            .field_builder::<TimestampNanosecondBuilder>(idx)
                            .unwrap();
                        b.append_value(*v);
                    }
                }
            }
            ArrowDataType::Binary => {
                let b = self
                    .builder
                    .values()
                    .field_builder::<BinaryBuilder>(idx)
                    .unwrap();
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
                let b = self
                    .builder
                    .values()
                    .field_builder::<StringBuilder>(idx)
                    .unwrap();

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
            ArrowDataType::Date32 => todo!(),
            ArrowDataType::Date64 => todo!(),
            ArrowDataType::Time32(_) => todo!(),
            ArrowDataType::Time64(_) => todo!(),
            ArrowDataType::Duration(_) => todo!(),
            ArrowDataType::Interval(_) => todo!(),
            ArrowDataType::FixedSizeBinary(_) => todo!(),
            ArrowDataType::LargeBinary => todo!(),
            ArrowDataType::LargeUtf8 => todo!(),
            ArrowDataType::List(_) => todo!(),
            ArrowDataType::FixedSizeList(_, _) => todo!(),
            ArrowDataType::LargeList(_) => todo!(),
            ArrowDataType::Struct(_) => todo!(),
            ArrowDataType::Union(_, _) => todo!(),
            ArrowDataType::Dictionary(_, _) => todo!(),
            ArrowDataType::Decimal128(_, _) => todo!(),
            ArrowDataType::Decimal256(_, _) => todo!(),
            ArrowDataType::Map(_, _) => todo!(),
            ArrowDataType::RunEndEncoded(_, _) => todo!(),
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
                let builder = self
                    .builder
                    .values()
                    .field_builder::<arrow::array::$a>(idx)
                    .unwrap();
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
                        .values()
                        .field_builder::<TimestampMicrosecondBuilder>(idx)
                        .unwrap()
                        .append_values(v, &is_valid),
                    TimeUnit::Second => self
                        .builder
                        .values()
                        .field_builder::<TimestampSecondBuilder>(idx)
                        .unwrap()
                        .append_values(v, &is_valid),
                    TimeUnit::Millisecond => self
                        .builder
                        .values()
                        .field_builder::<TimestampMillisecondBuilder>(idx)
                        .unwrap()
                        .append_values(v, &is_valid),
                    TimeUnit::Nanosecond => self
                        .builder
                        .values()
                        .field_builder::<TimestampNanosecondBuilder>(idx)
                        .unwrap()
                        .append_values(v, &is_valid),
                }
            }
            ArrowDataType::Binary => {
                let b = self
                    .builder
                    .values()
                    .field_builder::<BinaryBuilder>(idx)
                    .unwrap();
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
                let b = self
                    .builder
                    .values()
                    .field_builder::<StringBuilder>(idx)
                    .unwrap();
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
                    t => panic!("Unsupported binary input type: {t:?}, {value:?}, use &Vec<String>, &Vec<&str>, &[&str] or &[&[u8]]"),
                }
            }
            ArrowDataType::Date32 => todo!(),
            ArrowDataType::Date64 => todo!(),
            ArrowDataType::Time32(_) => todo!(),
            ArrowDataType::Time64(_) => todo!(),
            ArrowDataType::Duration(_) => todo!(),
            ArrowDataType::Interval(_) => todo!(),
            ArrowDataType::FixedSizeBinary(_) => todo!(),
            ArrowDataType::LargeBinary => todo!(),
            ArrowDataType::LargeUtf8 => todo!(),
            ArrowDataType::List(_) => todo!(),
            ArrowDataType::FixedSizeList(_, _) => todo!(),
            ArrowDataType::LargeList(_) => todo!(),
            ArrowDataType::Struct(_) => todo!(),
            ArrowDataType::Union(_, _) => todo!(),
            ArrowDataType::Dictionary(_, _) => todo!(),
            ArrowDataType::Decimal128(_, _) => todo!(),
            ArrowDataType::Decimal256(_, _) => todo!(),
            ArrowDataType::Map(_, _) => todo!(),
            ArrowDataType::RunEndEncoded(_, _) => todo!(),
        };
        self.index.replace(idx + 1);
        Ok(self)
    }

    pub fn finish(&mut self) -> arrow::array::GenericListArray<i32> {
        if self.builder.len() == 0 {
            self.builder.append(true);
        }
        self.builder.finish()
    }

    #[inline]
    fn next_n(&mut self, n: usize) -> &mut Self {
        if !self.index.map(|index| index == 0).unwrap_or_default() {
            for _ in 0..n {
                self.builder.values().append(true);
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
    use arrow::datatypes::DataType;

    #[test]
    fn builder() -> anyhow::Result<()> {
        let fields = vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
        ];
        let mut builder = ListOfStructBuilder::new(fields, 2);

        dbg!(["abc", "def"].type_id());

        // A two-rows item.
        builder
            .append_null_row()
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
}
