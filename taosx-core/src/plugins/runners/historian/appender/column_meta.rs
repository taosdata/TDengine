use std::collections::HashMap;
use std::str::FromStr;

use arrow_schema::{Field, Schema};
use tiberius::Row;

use taosx_ipc::prelude::IpcDataType;

pub struct ColumnMeta {
    pub column_name: String,
    pub type_name: String,
    pub precision: i32,
    pub nullable: bool,
}

impl ColumnMeta {
    pub fn try_new(row: &Row) -> anyhow::Result<Self> {
        let column_name = row.try_get::<&str, _>("COLUMN_NAME")?.unwrap().to_string();
        let type_name = row.try_get::<&str, _>("TYPE_NAME")?.unwrap().to_string();
        let precision = row.try_get::<i32, _>("PRECISION")?.unwrap();
        let is_nullable = row.try_get::<i16, _>("NULLABLE")?.unwrap();
        let nullable = match is_nullable {
            0 => false,
            1 => true,
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported nullable type: {}",
                    is_nullable
                ));
            }
        };

        Ok(Self {
            column_name,
            type_name,
            precision,
            nullable,
        })
    }

    pub fn get_ipc_type(&self) -> anyhow::Result<IpcDataType> {
        let type_name = self.type_name.as_str();
        let precision = self.precision;

        let db_type = match type_name {
            "datetime2" => "timestamp(ms)".to_string(),
            "nvarchar" => format!("varchar({})", precision).to_string(),
            "tinyint" => "tinyint".to_string(),
            "int" => "int".to_string(),
            "float" => "double".to_string(),
            _ => anyhow::bail!(
                "unsupported data type: {}, precision: {}",
                type_name,
                precision
            ),
        };

        IpcDataType::from_str(db_type.as_str()).map_err(|err| {
            anyhow::anyhow!(
                "failed to convert data type: {}, precision: {}, cause: {}",
                type_name,
                precision,
                err.to_string()
            )
        })
    }
}

pub fn to_schema(columns: Vec<ColumnMeta>) -> anyhow::Result<Schema> {
    let mut fields = Vec::new();
    for col in columns {
        let col_name = col.column_name;
        let arrow_type = to_arrow_data_type(col.type_name)?;
        let nullable = col.nullable;

        fields.push(Field::new(col_name, arrow_type, nullable));
    }

    // schema
    let mut metadata = HashMap::new();
    metadata.insert(String::from("version"), String::from("1.0"));
    metadata.insert(String::from("stream"), String::from("flat"));
    metadata.insert(String::from("ack"), String::from("lush"));

    let schema = arrow_schema::Schema::new(fields).with_metadata(metadata);
    Ok(schema)
}

fn to_arrow_data_type(type_name: String) -> anyhow::Result<arrow::datatypes::DataType> {
    let arrow_type = match type_name.as_str() {
        "datetime2" => {
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
        }
        "nvarchar" => arrow::datatypes::DataType::Utf8,
        "float" => arrow::datatypes::DataType::Float64,
        "tinyint" => arrow::datatypes::DataType::UInt8,
        "int" => arrow::datatypes::DataType::Int32,
        &_ => {
            return Err(anyhow::anyhow!(
                "Unsupported data type: {}",
                type_name.as_str()
            ));
        }
    };

    Ok(arrow_type)
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_to_arrow_data_type() {
        let arrow_type = super::to_arrow_data_type("datetime2".to_string()).unwrap();
        assert_eq!(
            arrow_type,
            arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None)
        );

        let arrow_type = super::to_arrow_data_type("nvarchar".to_string()).unwrap();
        assert_eq!(arrow_type, arrow::datatypes::DataType::Utf8);

        let arrow_type = super::to_arrow_data_type("float".to_string()).unwrap();
        assert_eq!(arrow_type, arrow::datatypes::DataType::Float64);

        let arrow_type = super::to_arrow_data_type("tinyint".to_string()).unwrap();
        assert_eq!(arrow_type, arrow::datatypes::DataType::UInt8);

        let arrow_type = super::to_arrow_data_type("int".to_string()).unwrap();
        assert_eq!(arrow_type, arrow::datatypes::DataType::Int32);
    }
}
