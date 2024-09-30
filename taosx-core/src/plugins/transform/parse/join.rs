use arrow::{
    array::{Array, ArrayRef, ListArray, StringArray},
    record_batch::RecordBatch,
};
use arrow_schema::DataType;
use arrow_schema::{Field, Schema};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::{ArrayForTaos, Parse};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Join {
    join: String,
}

impl Parse for Join {
    fn parse_array(
        &self,
        field: &Field,
        array: &ArrayRef,
    ) -> Result<(RecordBatch, Option<Vec<usize>>), super::ParseError> {
        // downcast field values to ListArray
        let array_list = match array.as_any().downcast_ref::<ListArray>() {
            Some(array) => array,
            None => {
                return Err(super::ParseError::UnsupportedDataType(
                    field.data_type().clone(),
                ))
            }
        };
        // loop and join
        let data: Vec<Option<String>> = array_list
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap()
            .iter()
            .map(|value| {
                value.map(|value| {
                    value
                        .taos_values()
                        .iter()
                        .map(|item| item.to_string().unwrap_or("".to_string()))
                        .join(&self.join)
                })
            })
            .collect();
        // new array
        let array = Arc::new(StringArray::from_iter(data));
        let schema = Arc::new(Schema::new(vec![Field::new(
            field.name(),
            DataType::Utf8,
            true,
        )]));
        let record = RecordBatch::try_new(schema, vec![array])?;
        Ok((record, None))
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::Int32Type;

    use super::*;

    #[test]
    fn test_join() {
        let array = ["abcdef", "gh", "ijklm nop", "qrst.uvw"];
        let join = String::from(",");

        let string = array.join(join.as_str());

        assert_eq!(string, "abcdef,gh,ijklm nop,qrst.uvw");
    }

    #[test]
    fn test_join_by_sep() {
        let join = r#"{
            "join": ","
        }"#;
        let join: Join = serde_json::from_str(join).unwrap();
        dbg!(join.clone());

        let field = Field::new("field_name_1", DataType::Utf8, false);

        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            None,
            Some(vec![Some(3), None, Some(5)]),
            Some(vec![Some(6), Some(7)]),
        ];
        let array: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(data));

        let (records, _) = join.parse_array(&field, &array).unwrap();
        dbg!(&records);
        assert_eq!(records.num_rows(), 4);
        assert_eq!(records.num_columns(), 1);
    }
}
