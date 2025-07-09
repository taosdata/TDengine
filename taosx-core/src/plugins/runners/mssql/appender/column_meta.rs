use anyhow::Ok;
use arrow::datatypes::DataType;
use arrow_schema::TimeUnit;
use taosx_ipc::stream::writer::IpcDataType;
use tiberius::ColumnType;

pub struct ColumnMeta {
    #[allow(dead_code)]
    pub column_name: String,
    pub column_type: ColumnType,
}

impl ColumnMeta {
    pub fn try_new(column_name: String, column_type: ColumnType) -> anyhow::Result<Self> {
        Ok(Self {
            column_name,
            column_type,
        })
    }

    pub fn get_ipc_type(&self) -> anyhow::Result<IpcDataType> {
        match self.column_type {
            // The column doesn't have a specified type.
            ColumnType::Null => Ok(IpcDataType::NChar(50)),
            // bit
            ColumnType::Bit => Ok(IpcDataType::NChar(50)),
            ColumnType::Bitn => Ok(IpcDataType::NChar(50)),
            // 整型数
            ColumnType::Int1 => Ok(IpcDataType::Int8),
            ColumnType::Int2 => Ok(IpcDataType::Int16),
            ColumnType::Int4 => Ok(IpcDataType::Int32),
            ColumnType::Int8 | ColumnType::Intn => Ok(IpcDataType::Int64),
            // 浮点数
            ColumnType::Float4 => Ok(IpcDataType::Float32),
            ColumnType::Float8 | ColumnType::Floatn => Ok(IpcDataType::Float64),
            ColumnType::Decimaln | ColumnType::Numericn => Ok(IpcDataType::NChar(50)),
            // 字符串
            ColumnType::NChar => Ok(IpcDataType::NChar(50)),
            ColumnType::BigChar => Ok(IpcDataType::NChar(50)),
            ColumnType::NVarchar => Ok(IpcDataType::NChar(50)),
            ColumnType::BigVarChar => Ok(IpcDataType::NChar(50)),
            ColumnType::Text => Ok(IpcDataType::NChar(50)),
            ColumnType::NText => Ok(IpcDataType::NChar(50)),
            // 字节数组
            ColumnType::BigBinary => Ok(IpcDataType::VarBinary(50)),
            ColumnType::BigVarBin => Ok(IpcDataType::VarBinary(50)),
            ColumnType::Image => Ok(IpcDataType::VarBinary(50)),
            // 日期时间
            ColumnType::Datetime
            | ColumnType::Datetime2
            | ColumnType::Datetime4
            | ColumnType::Datetimen => Ok(IpcDataType::NChar(50)),
            ColumnType::DatetimeOffsetn => Ok(IpcDataType::Timestamp(TimeUnit::Nanosecond)),
            ColumnType::Daten => Ok(IpcDataType::NChar(50)),
            ColumnType::Timen => Ok(IpcDataType::NChar(50)),
            // 其他特殊类型
            ColumnType::Money | ColumnType::Money4 => Ok(IpcDataType::NChar(50)),
            ColumnType::Guid => Ok(IpcDataType::NChar(50)),
            ColumnType::Xml => Ok(IpcDataType::NChar(50)),
            ColumnType::Udt => Ok(IpcDataType::NChar(50)),
            ColumnType::SSVariant => Ok(IpcDataType::NChar(50)),
            // 其他
            // _ => anyhow::bail!("unsupported data type: {:?}", self.column_type),
        }
    }
}

pub fn to_arrow_data_type(column_type: &ColumnType) -> anyhow::Result<DataType> {
    match column_type {
        // The column doesn't have a specified type.
        ColumnType::Null => Ok(DataType::Utf8),
        // bit
        ColumnType::Bit => Ok(DataType::Utf8),
        ColumnType::Bitn => Ok(DataType::Utf8),
        // 整型数
        ColumnType::Int1 => Ok(DataType::Int8),
        ColumnType::Int2 => Ok(DataType::Int16),
        ColumnType::Int4 => Ok(DataType::Int32),
        ColumnType::Int8 | ColumnType::Intn => Ok(DataType::Int64),
        // 浮点数
        ColumnType::Float4 => Ok(DataType::Float32),
        ColumnType::Float8 | ColumnType::Floatn => Ok(DataType::Float64),
        ColumnType::Decimaln | ColumnType::Numericn => Ok(DataType::Utf8),
        // 字符串
        ColumnType::NChar => Ok(DataType::Utf8),
        ColumnType::BigChar => Ok(DataType::Utf8),
        ColumnType::NVarchar => Ok(DataType::Utf8),
        ColumnType::BigVarChar => Ok(DataType::Utf8),
        ColumnType::Text => Ok(DataType::Utf8),
        ColumnType::NText => Ok(DataType::Utf8),
        // 字节数组
        ColumnType::BigBinary => Ok(DataType::Binary),
        ColumnType::BigVarBin => Ok(DataType::Binary),
        ColumnType::Image => Ok(DataType::Binary),
        // 日期时间
        ColumnType::Datetime
        | ColumnType::Datetime2
        | ColumnType::Datetime4
        | ColumnType::Datetimen => Ok(DataType::Utf8),
        ColumnType::DatetimeOffsetn => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        ColumnType::Daten => Ok(DataType::Utf8),
        ColumnType::Timen => Ok(DataType::Utf8),
        // 其他特殊类型
        ColumnType::Money | ColumnType::Money4 => Ok(DataType::Utf8),
        ColumnType::Guid => Ok(DataType::Utf8),
        ColumnType::Xml => Ok(DataType::Utf8),
        ColumnType::Udt => Ok(DataType::Utf8),
        ColumnType::SSVariant => Ok(DataType::Utf8),
        // 其他
        // _ => anyhow::bail!("unsupported data type: {:?}", column_type),
    }
}

#[cfg(test)]
mod tests {
    use tiberius::ColumnType;

    use super::*;

    #[test]
    fn test_column_meta() {
        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::NVarchar).unwrap();
        assert_eq!(column_meta.column_name, "id");
        assert_eq!(column_meta.column_type, ColumnType::NVarchar);
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));
    }

    #[test]
    fn test_get_ipc_type() {
        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Null).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Bit).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Bitn).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Int1).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int8);

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Int2).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int16);

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Int4).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int32);

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Int8).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int64);

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Intn).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Int64);

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Float4).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Float32);

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Float8).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Float64);

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Floatn).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::Float64);

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Decimaln).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Numericn).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::NChar).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::BigChar).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::NVarchar).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::BigVarChar).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::BigBinary).unwrap();
        assert_eq!(
            column_meta.get_ipc_type().unwrap(),
            IpcDataType::VarBinary(50)
        );

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::BigVarBin).unwrap();
        assert_eq!(
            column_meta.get_ipc_type().unwrap(),
            IpcDataType::VarBinary(50)
        );

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Text).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::NText).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Datetime).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Datetime2).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Datetime4).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Datetimen).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta =
            ColumnMeta::try_new("id".to_string(), ColumnType::DatetimeOffsetn).unwrap();
        assert_eq!(
            column_meta.get_ipc_type().unwrap(),
            IpcDataType::Timestamp(TimeUnit::Nanosecond)
        );

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Daten).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Timen).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Money).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Money4).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Guid).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Xml).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Udt).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::Image).unwrap();
        assert_eq!(
            column_meta.get_ipc_type().unwrap(),
            IpcDataType::VarBinary(50)
        );

        let column_meta = ColumnMeta::try_new("id".to_string(), ColumnType::SSVariant).unwrap();
        assert_eq!(column_meta.get_ipc_type().unwrap(), IpcDataType::NChar(50));
    }

    #[test]
    fn test_to_arrow_data_type() {
        assert_eq!(
            to_arrow_data_type(&ColumnType::Null).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Bit).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Bitn).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Int1).unwrap(),
            DataType::Int8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Int2).unwrap(),
            DataType::Int16
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Int4).unwrap(),
            DataType::Int32
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Int8).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Intn).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Float4).unwrap(),
            DataType::Float32
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Float8).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Floatn).unwrap(),
            DataType::Float64
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Decimaln).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Numericn).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::NChar).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::BigChar).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::NVarchar).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::BigVarChar).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::BigBinary).unwrap(),
            DataType::Binary
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::BigVarBin).unwrap(),
            DataType::Binary
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Text).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::NText).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Datetime).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Datetime2).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Datetime4).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Datetimen).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::DatetimeOffsetn).unwrap(),
            DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Daten).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Timen).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Money).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Money4).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Guid).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Xml).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Udt).unwrap(),
            DataType::Utf8
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::Image).unwrap(),
            DataType::Binary
        );
        assert_eq!(
            to_arrow_data_type(&ColumnType::SSVariant).unwrap(),
            DataType::Utf8
        );
    }
}
