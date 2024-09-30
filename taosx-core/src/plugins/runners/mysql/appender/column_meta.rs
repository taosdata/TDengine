use arrow::datatypes::{DataType, TimeUnit};
use taosx_ipc::stream::writer::IpcDataType;

// use taosx_ipc::prelude::IpcDataType;

#[allow(dead_code)]
pub struct ColumnMeta {
    pub column_name: String,
    pub type_name: String,
}

impl ColumnMeta {
    pub fn try_new(column_name: String, type_name: String) -> anyhow::Result<Self> {
        Ok(Self {
            column_name,
            type_name,
        })
    }

    pub fn get_ipc_type(&self) -> anyhow::Result<IpcDataType> {
        match self.type_name.as_str() {
            // 整型数
            "TINYINT" => Ok(IpcDataType::Int8),
            "TINYINT UNSIGNED" => Ok(IpcDataType::UInt8),
            "SMALLINT" => Ok(IpcDataType::Int16),
            "SMALLINT UNSIGNED" => Ok(IpcDataType::UInt16),
            "MEDIUMINT" => Ok(IpcDataType::Int32),
            "MEDIUMINT UNSIGNED" => Ok(IpcDataType::UInt32),
            "INT" => Ok(IpcDataType::Int32),
            "INT UNSIGNED" => Ok(IpcDataType::UInt32),
            "BIGINT" => Ok(IpcDataType::Int64),
            "BIGINT UNSIGNED" => Ok(IpcDataType::UInt64),
            // 浮点数
            "FLOAT" => Ok(IpcDataType::Float32),
            "DOUBLE" => Ok(IpcDataType::Float64),
            "DECIMAL" => Ok(IpcDataType::NChar(50)),
            // 字符串
            "CHAR" => Ok(IpcDataType::NChar(50)),
            "VARCHAR" => Ok(IpcDataType::NChar(50)),
            "BINARY" => Ok(IpcDataType::NChar(50)),
            "VARBINARY" => Ok(IpcDataType::NChar(50)),
            "TINYBLOB" => Ok(IpcDataType::NChar(50)),
            "BLOB" => Ok(IpcDataType::NChar(50)),
            "MEDIUMBLOB" => Ok(IpcDataType::NChar(50)),
            "LONGBLOB" => Ok(IpcDataType::NChar(50)),
            "TINYTEXT" => Ok(IpcDataType::NChar(50)),
            "TEXT" => Ok(IpcDataType::NChar(50)),
            "MEDUIMTEXT" => Ok(IpcDataType::NChar(50)),
            "LONGTEXT" => Ok(IpcDataType::NChar(50)),
            // 日期时间
            "DATE" => Ok(IpcDataType::NChar(50)),
            "TIME" => Ok(IpcDataType::NChar(50)),
            "DATETIME" => Ok(IpcDataType::NChar(50)),
            "TIMESTAMP" => Ok(IpcDataType::Timestamp(TimeUnit::Nanosecond)),
            "YEAR" => Ok(IpcDataType::Int16),
            // 二进制
            "BIT" => Ok(IpcDataType::UInt8),
            // 其他
            _ => anyhow::bail!("unsupported data type: {}", self.type_name),
        }
    }
}

pub fn to_arrow_data_type(type_name: String) -> anyhow::Result<DataType> {
    match type_name.as_str() {
        // 整型数
        "TINYINT" => Ok(DataType::Int8),
        "TINYINT UNSIGNED" => Ok(DataType::UInt8),
        "SMALLINT" => Ok(DataType::Int16),
        "SMALLINT UNSIGNED" => Ok(DataType::UInt16),
        "MEDIUMINT" => Ok(DataType::Int32),
        "MEDIUMINT UNSIGNED" => Ok(DataType::UInt32),
        "INT" => Ok(DataType::Int32),
        "INT UNSIGNED" => Ok(DataType::UInt32),
        "BIGINT" => Ok(DataType::Int64),
        "BIGINT UNSIGNED" => Ok(DataType::UInt64),
        // 浮点数
        "FLOAT" => Ok(DataType::Float32),
        "DOUBLE" => Ok(DataType::Float64),
        "DECIMAL" => Ok(DataType::Utf8),
        // 字符串
        "CHAR" => Ok(DataType::Utf8),
        "VARCHAR" => Ok(DataType::Utf8),
        "BINARY" => Ok(DataType::Utf8),
        "VARBINARY" => Ok(DataType::Utf8),
        "TINYBLOB" => Ok(DataType::Utf8),
        "BLOB" => Ok(DataType::Utf8),
        "MEDIUMBLOB" => Ok(DataType::Utf8),
        "LONGBLOB" => Ok(DataType::Utf8),
        "TINYTEXT" => Ok(DataType::Utf8),
        "TEXT" => Ok(DataType::Utf8),
        "MEDUIMTEXT" => Ok(DataType::Utf8),
        "LONGTEXT" => Ok(DataType::Utf8),
        // 日期时间
        "DATE" => Ok(DataType::Utf8),
        "TIME" => Ok(DataType::Utf8),
        "DATETIME" => Ok(DataType::Utf8),
        "TIMESTAMP" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        "YEAR" => Ok(DataType::UInt16),
        // 二进制
        "BIT" => Ok(DataType::UInt8),
        // 其他
        _ => anyhow::bail!("unsupported data type: {}", type_name),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_meta() {
        let column_meta = ColumnMeta::try_new("id".to_string(), "INT".to_string()).unwrap();
        assert_eq!(column_meta.column_name, "id");
        assert_eq!(column_meta.type_name, "INT");
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int32);
    }

    #[test]
    fn test_to_arrow_data_type() {
        assert_eq!(
            to_arrow_data_type("TINYINT".to_string()).unwrap(),
            DataType::Int8
        );
        assert_eq!(
            to_arrow_data_type("TINYINT UNSIGNED".to_string()).unwrap(),
            DataType::UInt8
        );
        assert_eq!(
            to_arrow_data_type("SMALLINT".to_string()).unwrap(),
            DataType::Int16
        );
        assert_eq!(
            to_arrow_data_type("SMALLINT UNSIGNED".to_string()).unwrap(),
            DataType::UInt16
        );
        assert_eq!(
            to_arrow_data_type("MEDIUMINT".to_string()).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            to_arrow_data_type("MEDIUMINT UNSIGNED".to_string()).unwrap(),
            DataType::UInt32
        );
        assert_eq!(
            to_arrow_data_type("INT".to_string()).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            to_arrow_data_type("INT UNSIGNED".to_string()).unwrap(),
            DataType::UInt32
        );
        assert_eq!(
            to_arrow_data_type("BIGINT".to_string()).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            to_arrow_data_type("BIGINT UNSIGNED".to_string()).unwrap(),
            DataType::UInt64
        );
        assert_eq!(
            to_arrow_data_type("FLOAT".to_string()).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            to_arrow_data_type("DOUBLE".to_string()).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            to_arrow_data_type("DECIMAL".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("CHAR".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("VARCHAR".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("BINARY".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("VARBINARY".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("TINYBLOB".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("BLOB".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("MEDIUMBLOB".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("LONGBLOB".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("TINYTEXT".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("TEXT".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("MEDUIMTEXT".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("LONGTEXT".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("DATE".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("TIME".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("DATETIME".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("TIMESTAMP".to_string()).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            to_arrow_data_type("BIT".to_string()).unwrap(),
            DataType::UInt8
        );
        assert!(to_arrow_data_type("UNKNOWN".to_string()).is_err());
    }
}
