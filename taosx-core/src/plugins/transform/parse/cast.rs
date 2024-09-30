use std::{collections::HashMap, str::FromStr, sync::Arc};

use arrow::{
    array::{
        ArrayRef, Int64Array, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    },
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use arrow_cast_guess_precision::cast;
use chrono::{format, DateTime, ParseResult};
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};
use taosx_ipc::prelude::IpcDataType;
use thiserror::Error;

use super::Parse;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Cast {
    r#as: IpcDataType,
    #[serde(skip_serializing_if = "Option::is_none")]
    with: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tz: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
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

    pub fn r#as(&self) -> &IpcDataType {
        &self.r#as
    }

    #[allow(dead_code)]
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

    fn parse_scalar(
        &self,
        field: &Field,
        array: &arrow::array::ArrayRef,
    ) -> Result<(Field, arrow::array::ArrayRef), super::ParseError> {
        let mut m = HashMap::new();
        m.insert("name".to_string(), field.name().to_string());
        m.insert("cast_from".to_string(), field.data_type().to_string());
        match self.r#as {
            IpcDataType::VarChar(len) | IpcDataType::NChar(len) => {
                m.insert("length".to_string(), len.to_string());
                m.insert("cast_to".to_string(), self.r#as.ty().name().to_string());
            }
            IpcDataType::Json => {
                m.insert("cast_to".to_string(), self.r#as.ty().name().to_string());
            }
            _ => (),
        }

        let name = self
            .alias
            .as_deref()
            .unwrap_or_else(|| field.name().as_str());
        let dt = self.r#as.arrow_data_type();
        let field = Field::new(name, dt, true).with_metadata(m);

        let array = if let IpcDataType::Timestamp(unit) = &self.r#as {
            if let Some(with) = self.with.as_deref() {
                let strings = cast(array, &DataType::Utf8)?;
                let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();

                let tz = self.tz.as_deref().unwrap_or("UTC");
                let iter = strings.iter().map(|s| {
                    s.and_then(|s| {
                        if with.contains("%z") {
                            chrono::DateTime::parse_from_str(s, with)
                                .ok()
                                .map(|ts| match unit {
                                    arrow::datatypes::TimeUnit::Second => ts.timestamp(),
                                    arrow::datatypes::TimeUnit::Millisecond => {
                                        ts.timestamp_millis()
                                    }
                                    arrow::datatypes::TimeUnit::Microsecond => {
                                        ts.timestamp_micros()
                                    }
                                    arrow::datatypes::TimeUnit::Nanosecond => {
                                        ts.timestamp_nanos_opt().unwrap_or(0)
                                    }
                                })
                        } else {
                            let tz = chrono_tz::Tz::from_str(tz).expect("Invalid tz");
                            parse_str_without_tz(s, with, &tz)
                                .ok()
                                .map(|ts| match unit {
                                    arrow::datatypes::TimeUnit::Second => ts.timestamp(),
                                    arrow::datatypes::TimeUnit::Millisecond => {
                                        ts.timestamp_millis()
                                    }
                                    arrow::datatypes::TimeUnit::Microsecond => {
                                        ts.timestamp_micros()
                                    }
                                    arrow::datatypes::TimeUnit::Nanosecond => {
                                        ts.timestamp_nanos_opt().unwrap_or(0)
                                    }
                                })
                        }
                    })
                });

                match unit {
                    arrow::datatypes::TimeUnit::Second => {
                        Arc::new(TimestampSecondArray::from_iter(iter)) as ArrayRef
                    }
                    arrow::datatypes::TimeUnit::Millisecond => {
                        Arc::new(TimestampMillisecondArray::from_iter(iter)) as ArrayRef
                    }
                    arrow::datatypes::TimeUnit::Microsecond => {
                        Arc::new(TimestampMicrosecondArray::from_iter(iter)) as ArrayRef
                    }
                    arrow::datatypes::TimeUnit::Nanosecond => {
                        Arc::new(TimestampNanosecondArray::from_iter(iter)) as ArrayRef
                    }
                }
            } else if matches!(array.data_type(), DataType::Utf8 | DataType::LargeUtf8) {
                // check if it is timestamp.
                let r = arrow::compute::cast(array, field.data_type())?;
                if r.null_count() > 0 {
                    // means some was not casted to timestamp.
                    let l = arrow::compute::cast(array, &DataType::Int64)?;
                    if l.null_count() == l.len() {
                        // all nulls, skip
                        r
                    } else {
                        // parse int to timestamp.
                        let l = arrow::compute::cast(array, &DataType::Int64)?;
                        let l = l.as_any().downcast_ref::<Int64Array>().unwrap();

                        use arrow::datatypes::TimeUnit::*;
                        let l = match unit {
                            Second => Arc::new(TimestampSecondArray::from_iter(l)) as ArrayRef,
                            Millisecond => {
                                Arc::new(TimestampMillisecondArray::from_iter(l)) as ArrayRef
                            }
                            Microsecond => {
                                Arc::new(TimestampMicrosecondArray::from_iter(l)) as ArrayRef
                            }
                            Nanosecond => {
                                Arc::new(TimestampNanosecondArray::from_iter(l)) as ArrayRef
                            }
                        };
                        let cmp = arrow::compute::is_null(&r)?;
                        arrow::compute::kernels::zip::zip(&cmp, &l, &r)?
                    }
                } else {
                    r
                }
            } else {
                cast(array, field.data_type())?
            }
        } else {
            cast(array, field.data_type())?
        };
        Ok((field, array))
    }
}

fn parse_str_without_tz(s: &str, fmt: &str, tz: &Tz) -> ParseResult<DateTime<Tz>> {
    let mut parsed = format::Parsed::new();
    chrono::format::parse(&mut parsed, s, format::strftime::StrftimeItems::new(fmt))?;
    parsed.to_datetime_with_timezone(tz)
}

#[cfg(test)]
mod tests {
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
