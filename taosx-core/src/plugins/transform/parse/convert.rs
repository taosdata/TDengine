use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use arrow::array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Schema};

use super::{Parse, ParseError};

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone, PartialEq)]
pub struct Convert {
    convert: HashMap<String, String>,
}

impl Parse for Convert {
    fn parse_array(
        &self,
        field: &arrow_schema::Field,
        array: &arrow::array::ArrayRef,
    ) -> Result<(arrow::array::RecordBatch, Option<Vec<usize>>), ParseError> {
        if !matches!(array.data_type(), DataType::Utf8) {
            return Err(ParseError::MapNotUtf8);
        }
        let array = array
            .as_any()
            .downcast_ref::<StringArray>()
            .context("convert map array to string array error")?;

        let default = self.convert.get("__taosx_default").map(|s| s.as_str());
        let column = StringArray::from_iter(array.iter().map(|s| {
            s.map(|k| {
                self.convert
                    .get(k)
                    .map_or(default.unwrap_or(k), |v| v.as_str())
            })
        }));

        Ok((
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![field.clone()])),
                vec![Arc::new(column)],
            )?,
            None,
        ))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::ArrayRef;
    use arrow_schema::Field;

    use super::*;

    #[test]
    fn parse_array_test() -> anyhow::Result<()> {
        let map = Convert {
            convert: HashMap::from_iter([("abc".to_string(), "def".to_string())]),
        };
        let input: ArrayRef = Arc::new(StringArray::from(vec!["abc", "123"]));
        let (batch, _) = map.parse_array(&Field::new("name", DataType::Utf8, false), &input)?;
        assert_eq!(batch, {
            let output: ArrayRef = Arc::new(StringArray::from(vec!["def", "123"]));
            RecordBatch::try_from_iter(vec![("name", output)])?
        });

        let input: ArrayRef = Arc::new(StringArray::from(vec![Some("abc"), None, Some("123")]));
        let (batch, _) = map.parse_array(&Field::new("name", DataType::Utf8, true), &input)?;
        assert_eq!(batch, {
            let output: ArrayRef =
                Arc::new(StringArray::from(vec![Some("def"), None, Some("123")]));
            RecordBatch::try_from_iter_with_nullable(vec![("name", output, true)])?
        });
        Ok(())
    }

    #[test]
    fn parse_array_default_test() -> anyhow::Result<()> {
        let map = Convert {
            convert: HashMap::from_iter([
                ("abc".to_string(), "def".to_string()),
                ("__taosx_default".to_string(), "lmn".to_string()),
            ]),
        };

        let input: ArrayRef = Arc::new(StringArray::from(vec!["abc", "123"]));
        let (batch, _) = map.parse_array(&Field::new("name", DataType::Utf8, false), &input)?;
        assert_eq!(batch, {
            let output: ArrayRef = Arc::new(StringArray::from(vec!["def", "lmn"]));
            RecordBatch::try_from_iter(vec![("name", output)])?
        });

        Ok(())
    }
}
