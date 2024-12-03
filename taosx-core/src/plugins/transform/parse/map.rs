use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use arrow::array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Schema};

use super::{Parse, ParseError};

#[derive(Debug, serde::Deserialize, serde::Serialize, Clone)]
pub struct Map {
    map: HashMap<String, String>,
}

impl Parse for Map {
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

        let column = StringArray::from_iter(array.iter().map(|s| s.and_then(|k| self.map.get(k))));

        Ok((
            RecordBatch::try_new(
                Arc::new(Schema::new(vec![field.clone()])),
                vec![Arc::new(column)],
            )?,
            None,
        ))
    }
}
