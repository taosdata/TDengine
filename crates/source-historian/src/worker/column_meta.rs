use std::collections::HashMap;
use std::str::FromStr;

use arrow_schema::{Field, Schema};
use tiberius::Row;

use taosx_ipc::prelude::IpcDataType;

pub struct ColumnMeta {
    pub(crate) column_name: String,
    pub(crate) type_name: String,
    pub(crate) precision: i32,
    pub(crate) nullable: bool,
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
            // !!!Attention: tinyint is unsigned in SQL Server
            "tinyint" => "u8".to_string(),
            "int" => "int".to_string(),
            "float" => "double".to_string(),
            "binary" | "varbinary" => format!("varbinary({})", precision).to_string(),
            "image" => anyhow::bail!("blob data type not supported"),
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

    pub fn build_schema_with_vec(columns: Vec<ColumnMeta>) -> anyhow::Result<Schema> {
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

        let schema = Schema::new(fields).with_metadata(metadata);
        Ok(schema)
    }
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
    use super::*;
    use arrow_schema::TimeUnit;

    #[test]
    fn test_get_ipc_data_type() {
        // given
        let column_meta = ColumnMeta {
            column_name: "id".to_string(),
            type_name: "int".to_string(),
            precision: 11,
            nullable: false,
        };
        // when
        let ipc_data_type = column_meta.get_ipc_type().unwrap();
        // then
        assert_eq!(ipc_data_type, IpcDataType::Int32);

        // given
        let column_meta = ColumnMeta {
            column_name: "name".to_string(),
            type_name: "nvarchar".to_string(),
            precision: 255,
            nullable: true,
        };
        // when
        let ipc_data_type = column_meta.get_ipc_type().unwrap();
        // then
        assert_eq!(ipc_data_type, IpcDataType::VarChar(255));

        // given
        let column_meta = ColumnMeta {
            column_name: "created_at".to_string(),
            type_name: "datetime2".to_string(),
            precision: 0,
            nullable: false,
        };
        // when
        let ipc_data_type = column_meta.get_ipc_type().unwrap();
        // then
        assert_eq!(ipc_data_type, IpcDataType::Timestamp(TimeUnit::Millisecond));

        // given
        let column_meta = ColumnMeta {
            column_name: "unknown".to_string(),
            type_name: "unknown".to_string(),
            precision: 0,
            nullable: false,
        };
        // when
        let ipc_data_type = column_meta.get_ipc_type();
        // then
        assert!(ipc_data_type.is_err());
        assert_eq!(
            ipc_data_type.unwrap_err().to_string(),
            "unsupported data type: unknown, precision: 0"
        );
    }

    #[test]
    fn test_build_schema_with_column_meta_vec() {
        let columns = vec![
            ColumnMeta {
                column_name: "id".to_string(),
                type_name: "int".to_string(),
                precision: 11,
                nullable: false,
            },
            ColumnMeta {
                column_name: "name".to_string(),
                type_name: "nvarchar".to_string(),
                precision: 255,
                nullable: true,
            },
            ColumnMeta {
                column_name: "created_at".to_string(),
                type_name: "datetime2".to_string(),
                precision: 0,
                nullable: false,
            },
        ];

        let schema = ColumnMeta::build_schema_with_vec(columns).unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.metadata().len(), 3);
        assert_eq!(schema.metadata().get("version").unwrap(), "1.0");
        assert_eq!(schema.metadata().get("stream").unwrap(), "flat");
        assert_eq!(schema.metadata().get("ack").unwrap(), "lush");
        let f = schema.fields().first().unwrap();
        assert_eq!(f.name(), "id");
        assert_eq!(f.data_type(), &arrow::datatypes::DataType::Int32);
        assert!(!f.is_nullable());

        let f = schema.fields().get(1).unwrap();
        assert_eq!(f.name(), "name");
        assert_eq!(f.data_type(), &arrow::datatypes::DataType::Utf8);
        assert!(f.is_nullable());

        let f = schema.fields().get(2).unwrap();
        assert_eq!(f.name(), "created_at");
        assert_eq!(
            f.data_type(),
            &arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert!(!f.is_nullable());
    }

    #[test]
    fn test_to_arrow_data_type() {
        let arrow_type = to_arrow_data_type("datetime2".to_string()).unwrap();
        assert_eq!(
            arrow_type,
            arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, None)
        );

        let arrow_type = to_arrow_data_type("nvarchar".to_string()).unwrap();
        assert_eq!(arrow_type, arrow::datatypes::DataType::Utf8);

        let arrow_type = to_arrow_data_type("float".to_string()).unwrap();
        assert_eq!(arrow_type, arrow::datatypes::DataType::Float64);

        let arrow_type = to_arrow_data_type("tinyint".to_string()).unwrap();
        assert_eq!(arrow_type, arrow::datatypes::DataType::UInt8);

        let arrow_type = to_arrow_data_type("int".to_string()).unwrap();
        assert_eq!(arrow_type, arrow::datatypes::DataType::Int32);

        let arrow_type = to_arrow_data_type("unknown".to_string());
        assert!(arrow_type.is_err());
        assert_eq!(
            arrow_type.unwrap_err().to_string(),
            "Unsupported data type: unknown"
        );
    }
}
