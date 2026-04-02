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
            // 布尔值
            "BOOL" => Ok(IpcDataType::NChar(50)),
            // 字符
            "CHAR" => Ok(IpcDataType::NChar(50)),
            // 整型数
            "SMALLINT" | "SMALLSERIAL" | "INT2" => Ok(IpcDataType::Int16),
            "INT" | "SERIAL" | "INT4" => Ok(IpcDataType::Int32),
            "BIGINT" | "BIGSERIAL" | "INT8" => Ok(IpcDataType::Int64),
            // 浮点数
            "REAL" | "FLOAT4" => Ok(IpcDataType::Float32),
            "DOUBLE PRECISION" | "FLOAT8" => Ok(IpcDataType::Float64),
            "NUMERIC" => Ok(IpcDataType::NChar(50)),
            // 字符串
            "VARCHAR" | "CHAR(N)" | "TEXT" | "NAME" | "CITEXT" => Ok(IpcDataType::NChar(50)),
            // 字节数组
            "BYTEA" => Ok(IpcDataType::VarBinary(50)),
            // 日期时间
            "DATE" => Ok(IpcDataType::NChar(50)),
            "TIME" => Ok(IpcDataType::NChar(50)),
            "TIMESTAMP" => Ok(IpcDataType::NChar(50)),
            "TIMESTAMPTZ" => Ok(IpcDataType::Timestamp(TimeUnit::Nanosecond)),
            // uuid
            "UUID" => Ok(IpcDataType::NChar(50)),
            // 二进制数组
            "BIT" | "VARBIT" => Ok(IpcDataType::NChar(50)),
            // json
            "JSON" | "JSONB" => Ok(IpcDataType::NChar(50)),
            // Others
            "INTERVAL" => Ok(IpcDataType::NChar(50)),
            "INT8RANGE" | "INT4RANGE" | "TSRANGE" | "TSTZRANGE" | "DATERANGE" | "NUMRANGE" => {
                Ok(IpcDataType::NChar(50))
            }
            "MONEY" => Ok(IpcDataType::NChar(50)),
            "LTREE" => Ok(IpcDataType::NChar(50)),
            "LQUERY" => Ok(IpcDataType::NChar(50)),
            "TIMETZ" => Ok(IpcDataType::NChar(50)),
            "INET" | "CIDR" => Ok(IpcDataType::NChar(50)),
            "MACADDR" => Ok(IpcDataType::NChar(50)),
            // 其他
            _ => anyhow::bail!("unsupported data type: {}", self.type_name),
        }
    }
}

pub fn to_arrow_data_type(type_name: String) -> anyhow::Result<DataType> {
    match type_name.as_str() {
        // 布尔值
        "BOOL" => Ok(DataType::Utf8),
        // 字符
        "CHAR" => Ok(DataType::Utf8),
        // 整型数
        "SMALLINT" | "SMALLSERIAL" | "INT2" => Ok(DataType::Int16),
        "INT" | "SERIAL" | "INT4" => Ok(DataType::Int32),
        "BIGINT" | "BIGSERIAL" | "INT8" => Ok(DataType::Int64),
        // 浮点数
        "REAL" | "FLOAT4" => Ok(DataType::Float32),
        "DOUBLE PRECISION" | "FLOAT8" => Ok(DataType::Float64),
        "NUMERIC" => Ok(DataType::Utf8),
        // 字符串
        "VARCHAR" | "CHAR(N)" | "TEXT" | "NAME" | "CITEXT" => Ok(DataType::Utf8),
        "BYTEA" => Ok(DataType::Binary),
        // 日期时间
        "DATE" => Ok(DataType::Utf8),
        "TIME" => Ok(DataType::Utf8),
        "TIMESTAMP" => Ok(DataType::Utf8),
        "TIMESTAMPTZ" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        // uuid
        "UUID" => Ok(DataType::Utf8),
        // 二进制数组
        "BIT" | "VARBIT" => Ok(DataType::Utf8),
        // json
        "JSON" | "JSONB" => Ok(DataType::Utf8),
        // Others
        "INTERVAL" => Ok(DataType::Utf8),
        "INT8RANGE" | "INT4RANGE" | "TSRANGE" | "TSTZRANGE" | "DATERANGE" | "NUMRANGE" => {
            Ok(DataType::Utf8)
        }
        "MONEY" => Ok(DataType::Utf8),
        "LTREE" => Ok(DataType::Utf8),
        "LQUERY" => Ok(DataType::Utf8),
        "TIMETZ" => Ok(DataType::Utf8),
        "INET" | "CIDR" => Ok(DataType::Utf8),
        "MACADDR" => Ok(DataType::Utf8),
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
    fn test_get_ipc_type() {
        let column_meta = ColumnMeta::try_new("id".to_string(), "BOOL".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "CHAR".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "SMALLINT".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int16);

        let column_meta = ColumnMeta::try_new("id".to_string(), "SMALLSERIAL".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int16);

        let column_meta = ColumnMeta::try_new("id".to_string(), "INT2".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int16);

        let column_meta = ColumnMeta::try_new("id".to_string(), "INT".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int32);

        let column_meta = ColumnMeta::try_new("id".to_string(), "SERIAL".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int32);

        let column_meta = ColumnMeta::try_new("id".to_string(), "INT4".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int32);

        let column_meta = ColumnMeta::try_new("id".to_string(), "BIGINT".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int64);

        let column_meta = ColumnMeta::try_new("id".to_string(), "BIGSERIAL".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int64);

        let column_meta = ColumnMeta::try_new("id".to_string(), "INT8".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int64);

        let column_meta = ColumnMeta::try_new("id".to_string(), "REAL".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Float32);

        let column_meta = ColumnMeta::try_new("id".to_string(), "FLOAT4".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Float32);

        let column_meta =
            ColumnMeta::try_new("id".to_string(), "DOUBLE PRECISION".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Float64);

        let column_meta = ColumnMeta::try_new("id".to_string(), "FLOAT8".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Float64);

        let column_meta = ColumnMeta::try_new("id".to_string(), "NUMERIC".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "VARCHAR".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "CHAR(N)".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "TEXT".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "NAME".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "CITEXT".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "BYTEA".to_string()).unwrap();
        assert_eq!(
            column_meta.get_ipc_type().unwrap(),
            IpcDataType::VarBinary(50)
        );

        let column_meta = ColumnMeta::try_new("id".to_string(), "DATE".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "TIME".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "TIMESTAMP".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "TIMESTAMPTZ".to_string()).unwrap();
        assert_eq!(
            column_meta.get_ipc_type().unwrap(),
            IpcDataType::Timestamp(TimeUnit::Nanosecond)
        );

        let column_meta = ColumnMeta::try_new("id".to_string(), "UUID".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "BIT".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "VARBIT".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "JSON".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "JSONB".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "INTERVAL".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "INT8RANGE".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "INT4RANGE".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "TSRANGE".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "TSTZRANGE".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "DATERANGE".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "NUMRANGE".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "MONEY".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "LTREE".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "LQUERY".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "TIMETZ".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "INET".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "CIDR".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "MACADDR".to_string()).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), "UNKNOWN".to_string()).unwrap();
        assert!(column_meta.get_ipc_type().is_err());
    }

    #[test]
    fn test_to_arrow_data_type() {
        assert_eq!(
            to_arrow_data_type("BOOL".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("CHAR".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("SMALLINT".to_string()).unwrap(),
            DataType::Int16
        );
        assert_eq!(
            to_arrow_data_type("SMALLSERIAL".to_string()).unwrap(),
            DataType::Int16
        );
        assert_eq!(
            to_arrow_data_type("INT2".to_string()).unwrap(),
            DataType::Int16
        );
        assert_eq!(
            to_arrow_data_type("INT".to_string()).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            to_arrow_data_type("SERIAL".to_string()).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            to_arrow_data_type("INT4".to_string()).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            to_arrow_data_type("BIGINT".to_string()).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            to_arrow_data_type("BIGSERIAL".to_string()).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            to_arrow_data_type("INT8".to_string()).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            to_arrow_data_type("REAL".to_string()).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            to_arrow_data_type("FLOAT4".to_string()).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            to_arrow_data_type("DOUBLE PRECISION".to_string()).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            to_arrow_data_type("FLOAT8".to_string()).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            to_arrow_data_type("NUMERIC".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("VARCHAR".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("CHAR(N)".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("TEXT".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("NAME".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("CITEXT".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("BYTEA".to_string()).unwrap(),
            DataType::Binary
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
            to_arrow_data_type("TIMESTAMP".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("TIMESTAMPTZ".to_string()).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            to_arrow_data_type("UUID".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("BIT".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("VARBIT".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("JSON".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("JSONB".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("INTERVAL".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("INT8RANGE".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("INT4RANGE".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("TSRANGE".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("TSTZRANGE".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("DATERANGE".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("NUMRANGE".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("MONEY".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("LTREE".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("LQUERY".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("TIMETZ".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("INET".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("CIDR".to_string()).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type("MACADDR".to_string()).unwrap(),
            DataType::Utf8
        );
        assert!(to_arrow_data_type("UNKNOWN".to_string()).is_err());
    }
}
