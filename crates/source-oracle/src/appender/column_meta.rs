use arrow::datatypes::{DataType, TimeUnit};
use oracle::sql_type::OracleType;
use taosx_ipc::stream::writer::IpcDataType;

pub struct ColumnMeta {
    #[allow(dead_code)]
    pub column_name: String,
    pub column_type: OracleType,
}

impl ColumnMeta {
    pub fn try_new(column_name: String, column_type: OracleType) -> anyhow::Result<Self> {
        Ok(Self {
            column_name,
            column_type,
        })
    }

    pub fn get_ipc_type(&self) -> anyhow::Result<IpcDataType> {
        match self.column_type {
            // 字符串
            OracleType::Varchar2(_) => Ok(IpcDataType::NChar(50)),
            OracleType::NVarchar2(_) => Ok(IpcDataType::NChar(50)),
            OracleType::Char(_) => Ok(IpcDataType::NChar(50)),
            OracleType::NChar(_) => Ok(IpcDataType::NChar(50)),
            OracleType::Rowid => Ok(IpcDataType::NChar(50)),
            // 浮点数
            OracleType::BinaryFloat => Ok(IpcDataType::Float32),
            OracleType::BinaryDouble => Ok(IpcDataType::Float64),
            OracleType::Number(_, _) => Ok(IpcDataType::NChar(50)),
            OracleType::Float(_) => Ok(IpcDataType::NChar(50)),
            // 日期时间
            OracleType::Date => Ok(IpcDataType::NChar(50)),
            OracleType::Timestamp(_) => Ok(IpcDataType::Timestamp(TimeUnit::Nanosecond)),
            OracleType::TimestampTZ(_) => Ok(IpcDataType::Timestamp(TimeUnit::Nanosecond)),
            OracleType::TimestampLTZ(_) => Ok(IpcDataType::Timestamp(TimeUnit::Nanosecond)),
            OracleType::IntervalDS(_, _) => Ok(IpcDataType::NChar(50)),
            OracleType::IntervalYM(_) => Ok(IpcDataType::NChar(50)),
            // 大文本
            OracleType::CLOB => Ok(IpcDataType::NChar(50)),
            OracleType::NCLOB => Ok(IpcDataType::NChar(50)),
            OracleType::BLOB => Ok(IpcDataType::NChar(50)),
            OracleType::BFILE => Ok(IpcDataType::NChar(50)),
            OracleType::RefCursor => Ok(IpcDataType::NChar(50)),
            OracleType::Boolean => Ok(IpcDataType::NChar(50)),
            OracleType::Object(_) => Ok(IpcDataType::NChar(50)),
            OracleType::Long => Ok(IpcDataType::NChar(50)),
            OracleType::Json => Ok(IpcDataType::NChar(50)),
            OracleType::Xml => Ok(IpcDataType::NChar(50)),
            // 字节数组
            OracleType::Raw(_) => Ok(IpcDataType::VarBinary(128)),
            OracleType::LongRaw => Ok(IpcDataType::VarBinary(512)),
            // 整型数，meta 信息不准确，它可能是 Number 类型变化而来
            OracleType::Int64 => Ok(IpcDataType::NChar(50)),
            OracleType::UInt64 => Ok(IpcDataType::NChar(50)),
            // 其他
            // _ => anyhow::bail!("unsupported data type: {:?}", self.column_type),
        }
    }
}

pub fn to_arrow_data_type(column_type: &OracleType) -> anyhow::Result<DataType> {
    match column_type {
        // 字符串
        OracleType::Varchar2(_) => Ok(DataType::Utf8),
        OracleType::NVarchar2(_) => Ok(DataType::Utf8),
        OracleType::Char(_) => Ok(DataType::Utf8),
        OracleType::NChar(_) => Ok(DataType::Utf8),
        OracleType::Rowid => Ok(DataType::Utf8),
        // 浮点数
        OracleType::BinaryFloat => Ok(DataType::Float32),
        OracleType::BinaryDouble => Ok(DataType::Float64),
        OracleType::Number(_, _) => Ok(DataType::Utf8),
        OracleType::Float(_) => Ok(DataType::Utf8),
        // 日期时间
        OracleType::Date => Ok(DataType::Utf8),
        OracleType::Timestamp(_) => Ok(DataType::Utf8),
        OracleType::TimestampTZ(_) => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        OracleType::TimestampLTZ(_) => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        OracleType::IntervalDS(_, _) => Ok(DataType::Utf8),
        OracleType::IntervalYM(_) => Ok(DataType::Utf8),
        // 大文本
        OracleType::CLOB => Ok(DataType::Utf8),
        OracleType::NCLOB => Ok(DataType::Utf8),
        OracleType::BLOB => Ok(DataType::Utf8),
        OracleType::BFILE => Ok(DataType::Utf8),
        OracleType::RefCursor => Ok(DataType::Utf8),
        OracleType::Boolean => Ok(DataType::Utf8),
        OracleType::Object(_) => Ok(DataType::Utf8),
        OracleType::Long => Ok(DataType::Utf8),
        OracleType::Json => Ok(DataType::Utf8),
        OracleType::Xml => Ok(DataType::Utf8),
        // 字节数组
        OracleType::Raw(_) => Ok(DataType::Binary),
        OracleType::LongRaw => Ok(DataType::Binary),
        // 整型数
        OracleType::Int64 => Ok(DataType::Utf8),
        OracleType::UInt64 => Ok(DataType::Utf8),
        // 其他
        // _ => anyhow::bail!("unsupported data type: {:?}", column_type),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_meta() {
        let column_meta = ColumnMeta::try_new("id".to_string(), OracleType::Varchar2(10)).unwrap();
        assert_eq!(column_meta.column_name, "id");
        assert_eq!(column_meta.column_type, OracleType::Varchar2(10));
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));
    }

    #[test]
    fn test_to_arrow_data_type() {
        assert_eq!(
            to_arrow_data_type(&OracleType::Varchar2(10)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::NVarchar2(10)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::Char(10)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::NChar(10)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::Rowid).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::Raw(10)).unwrap(),
            DataType::Binary
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::BinaryFloat).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::BinaryDouble).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::Number(10, 2)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::Float(10)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::Date).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::Timestamp(10)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::TimestampTZ(10)).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::TimestampLTZ(10)).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::IntervalDS(10, 2)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::IntervalYM(10)).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::CLOB).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::NCLOB).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::BLOB).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::BFILE).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::RefCursor).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::Boolean).unwrap(),
            DataType::Utf8
        );
        // assert_eq!(to_arrow_data_type(&OracleType::Object).unwrap(), DataType::Utf8);
        assert_eq!(
            to_arrow_data_type(&OracleType::Long).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::LongRaw).unwrap(),
            DataType::Binary
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::Json).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::Int64).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&OracleType::UInt64).unwrap(),
            DataType::Utf8
        );
        // assert_eq!(to_arrow_data_type("UNKNOWN".to_string()).is_err(), true);
    }
}
