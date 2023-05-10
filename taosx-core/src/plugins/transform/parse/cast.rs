use std::{collections::HashMap, str::FromStr, sync::Arc};

use arrow::{
    array::{
        Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use chrono::TimeZone;
use serde::{Deserialize, Serialize};
use taosx_ipc::prelude::IpcDataType;
use thiserror::Error;

use super::Parse;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Cast {
    r#as: IpcDataType,
    with: Option<String>,
    tz: Option<String>,
    alias: Option<String>,
}

impl Cast {
    pub fn new(r#as: IpcDataType) -> Self {
        Self {
            r#as,
            with: None,
            tz: None,
            alias: None,
        }
    }
    pub fn alias(mut self, alias: impl ToString) -> Self {
        self.alias.replace(alias.to_string());
        self
    }
}

#[derive(Debug, Error)]
pub enum CastFromStrError {
    #[error("Unknown cast type {0:?}")]
    UnknownCastType(String),
}

impl FromStr for Cast {
    type Err = CastFromStrError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        IpcDataType::from_str(s)
            .map(Self::new)
            .map_err(CastFromStrError::UnknownCastType)
    }
}

impl Parse for Cast {
    fn num_rows_will_be_changed(&self) -> bool {
        false
    }

    fn num_columns_will_be_changed(&self) -> bool {
        false
    }

    fn parse_array(
        &self,
        field: &arrow::datatypes::Field,
        array: &arrow::array::ArrayRef,
    ) -> Result<(arrow::record_batch::RecordBatch, Option<Vec<usize>>), super::ParseError> {
        // let options = CastOptions;
        let (field, array) = self.parse_scalar(field, array)?;
        let schema = Schema::new(vec![field]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![array])?;
        Ok((batch, None))
    }

    fn is_scala(&self) -> bool {
        true
    }

    fn parse_scalar(
        &self,
        field: &Field,
        array: &arrow::array::ArrayRef,
    ) -> Result<(Field, arrow::array::ArrayRef), super::ParseError> {
        let mut m = HashMap::new();
        m.insert("name".to_string(), field.name().to_string());

        let name = self
            .alias
            .as_deref()
            .unwrap_or_else(|| field.name().as_str());
        let dt = self.r#as.arrow_data_type();
        let field = Field::new(name, dt, true).with_metadata(m);

        let array = if self.r#as == IpcDataType::Timestamp {
            if let Some(with) = self.with.as_deref() {
                let strings = arrow::compute::cast(array, &DataType::Utf8)?;
                let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();

                let tz = self.tz.as_deref().unwrap_or("UTC");

                let array = Int64Array::from_iter(strings.iter().map(|s| {
                    s.and_then(|s| {
                        if with.contains("%z") {
                            chrono::DateTime::parse_from_str(s, with)
                                .ok()
                                .map(|ts| ts.timestamp_millis())
                        } else {
                            chrono_tz::Tz::from_str(&tz)
                                .expect("Invalid tz")
                                .datetime_from_str(s, with)
                                .ok()
                                .map(|ts| ts.timestamp_millis())
                        }
                    })
                }));
                Arc::new(array)
            } else {
                use arrow::datatypes::TimeUnit::*;
                use DataType::*;
                if let Timestamp(unit, _) = array.data_type() {
                    // let array =
                    match unit {
                        Second => Arc::new(Int64Array::from_iter(
                            array
                                .as_any()
                                .downcast_ref::<TimestampSecondArray>()
                                .unwrap()
                                .iter()
                                .map(|ts| ts.map(|ts| ts * 1000)),
                        )),
                        Millisecond => Arc::new(Int64Array::from_iter(
                            array
                                .as_any()
                                .downcast_ref::<TimestampMillisecondArray>()
                                .unwrap()
                                .iter(),
                        )),
                        Microsecond => Arc::new(Int64Array::from_iter(
                            array
                                .as_any()
                                .downcast_ref::<TimestampMicrosecondArray>()
                                .unwrap()
                                .iter(),
                        )),
                        Nanosecond => Arc::new(Int64Array::from_iter(
                            array
                                .as_any()
                                .downcast_ref::<TimestampNanosecondArray>()
                                .unwrap()
                                .iter(),
                        )),
                    }
                } else {
                    arrow::compute::cast(array, field.data_type())?
                }
            }
        } else {
            arrow::compute::cast(array, field.data_type())?
        };
        Ok((field, array))
    }
}

#[cfg(test)]
mod tests {
    use arrow::{array::ArrayRef, datatypes::Field};

    use super::*;

    #[test]
    fn json_extract() {
        let parser = Cast::new(IpcDataType::Int64).alias("b");

        let field = Field::new("a", DataType::Utf8, false);
        let array: ArrayRef = Arc::new(StringArray::from(vec![r#"1"#, r#"2"#]));

        // let records = RecordBatch::try_from_iter(vec![("a", b.clone()), ("b", b)]).unwrap();

        let (records, indices) = parser.parse_array(&field, &array).unwrap();

        dbg!(&records);
        dbg!(&indices);
        assert_eq!(records.schema().field(0).name(), "b");
        assert_eq!(records.num_columns(), 1);
        assert_eq!(records.num_rows(), 2);
        assert_eq!(indices, None);
    }
}
