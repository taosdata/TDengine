use anyhow::Context;
use serde::{Deserialize, Serialize};
use taosx_core::{DataSet, OptionSet};
use taosx_ipc::prelude::IpcDataType;

pub const PSPACE_POINT: &str = "PSPACE_POINT";

#[derive(Debug, Deserialize, Serialize)]
pub struct PspacePoint {
    pub id: u64,
    pub name: String,
    pub r#type: String, // e.g. PSPACE_ANALOG
    pub long_name: String,
    pub desc: Option<String>,
    data_type: Option<String>, // e.g. psDataType_UInt8
}

impl PspacePoint {
    pub fn data_type(&self) -> Option<IpcDataType> {
        self.data_type.as_deref().and_then(|dt| {
            to_ipc_data_type(dt)
                .map_err(|e| {
                    tracing::warn!("failed to convert pSpace data type '{}': {}", dt, e);
                    e
                })
                .ok()
        })
    }
}

/// pSpace 的数据点 -> DataSet
impl From<PspacePoint> for DataSet {
    fn from(val: PspacePoint) -> Self {
        let mut ds = DataSet {
            id: val.id.to_string(),
            name: Some(val.name),
            category: Some(PSPACE_POINT.to_string()),
            r#type: Some(val.r#type),
            options: Some(vec![]),
            format: None,
        };
        // map long_name to dataset option
        if let Some(options) = ds.options.as_mut() {
            options.push(OptionSet {
                name: "long_name".to_string(),
                display: val.long_name,
                description: None,
                required: false,
            });
        }
        // map desc to dataset option if exists
        if let Some(desc) = val.desc
            && let Some(options) = ds.options.as_mut()
        {
            options.push(OptionSet {
                name: "desc".to_string(),
                display: desc,
                description: None,
                required: false,
            });
        }
        // map data_type to dataset option if exists (keep original pSpace type name)
        if let Some(data_type) = val.data_type
            && let Some(options) = ds.options.as_mut()
        {
            options.push(OptionSet {
                name: "data_type".to_string(),
                display: data_type,
                description: None,
                required: false,
            });
        }
        ds
    }
}

impl TryFrom<DataSet> for PspacePoint {
    type Error = anyhow::Error;

    fn try_from(value: DataSet) -> Result<Self, Self::Error> {
        let id = value.id.parse::<u64>().context("invalid pSpace point id")?;
        let name = value
            .name
            .ok_or(anyhow::anyhow!("missing pSpace point name"))?;
        let r#type = value
            .r#type
            .ok_or(anyhow::anyhow!("missing pSpace point type"))?;
        let options = value
            .options
            .ok_or(anyhow::anyhow!("missing pSpace point options"))?;
        let long_name = options
            .iter()
            .find(|o| o.name == "long_name")
            .ok_or(anyhow::anyhow!("missing pSpace point long_name option"))?
            .display
            .clone();
        let desc = options
            .iter()
            .find(|o| o.name == "desc")
            .map(|o| o.display.clone());
        let data_type = options
            .iter()
            .find(|o| o.name == "data_type")
            .map(|o| o.display.clone());
        //

        Ok(PspacePoint {
            id,
            name,
            r#type,
            long_name,
            desc,
            data_type,
        })
    }
}

/// Convert pSpace data type name (from `PsDataTypeEnum.getName()`) to `IpcDataType`.
pub fn to_ipc_data_type(pspace_data_type: &str) -> anyhow::Result<IpcDataType> {
    match pspace_data_type {
        "psDataType_Empty" => Ok(IpcDataType::Null),
        "psDataType_Bool" => Ok(IpcDataType::Bool),
        "psDataType_Int8" => Ok(IpcDataType::Int8),
        "psDataType_UInt8" => Ok(IpcDataType::UInt8),
        "psDataType_Int16" => Ok(IpcDataType::Int16),
        "psDataType_UInt16" => Ok(IpcDataType::UInt16),
        "psDataType_Int32" => Ok(IpcDataType::Int32),
        "psDataType_UInt32" => Ok(IpcDataType::UInt32),
        "psDataType_Int64" => Ok(IpcDataType::Int64),
        "psDataType_UInt64" => Ok(IpcDataType::UInt64),
        "psDataType_Float" => Ok(IpcDataType::Float32),
        "psDataType_Double" => Ok(IpcDataType::Float64),
        "psDataType_Time" => Ok(IpcDataType::Timestamp(arrow_schema::TimeUnit::Millisecond)),
        "psDataType_String" => Ok(IpcDataType::VarChar(1024)),
        "psDataType_WString" => Ok(IpcDataType::NChar(1024)),
        "psDataType_Blob" => Ok(IpcDataType::Blob),
        other => anyhow::bail!("unknown pSpace data type: {}", other),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pspace_point_conversion() {
        // Deserialize from pSpace plugin JSON, then convert to DataSet
        let json = r#"{"id":150019,"name":"气温","type":"PS_ANALOG","long_name":"\\北京\\朝阳\\气温","desc":""}"#;
        let point: PspacePoint =
            serde_json::from_str(json).expect("should deserialize pSpace point JSON");
        assert_eq!(point.id, 150019);
        assert_eq!(point.name, "气温");
        assert_eq!(point.r#type, "PS_ANALOG");
        assert_eq!(point.long_name, r"\北京\朝阳\气温");
        assert_eq!(point.desc.as_deref(), Some(""));

        // PspacePoint -> DataSet
        let ds: DataSet = point.into();
        assert_eq!(ds.id, "150019");
        assert_eq!(ds.name.as_deref(), Some("气温"));
        assert_eq!(ds.category.as_deref(), Some(PSPACE_POINT));
        assert_eq!(ds.r#type.as_deref(), Some("PS_ANALOG"));

        let options = ds.options.as_ref().unwrap();
        assert_eq!(options.len(), 2);
        assert_eq!(options[0].name, "long_name");
        assert_eq!(options[0].display, r"\北京\朝阳\气温");
        assert_eq!(options[1].name, "desc");
        assert_eq!(options[1].display, "");

        // DataSet -> PspacePoint round-trip
        let restored = PspacePoint::try_from(ds).expect("should convert back to PspacePoint");
        assert_eq!(restored.id, 150019);
        assert_eq!(restored.name, "气温");
        assert_eq!(restored.r#type, "PS_ANALOG");
        assert_eq!(restored.long_name, r"\北京\朝阳\气温");
        assert_eq!(restored.desc.as_deref(), Some(""));

        // desc = null: JSON without desc field
        let json_no_desc =
            r#"{"id":150020,"name":"湿度","type":"PS_ANALOG","long_name":"\\北京\\朝阳\\湿度"}"#;
        let point2: PspacePoint =
            serde_json::from_str(json_no_desc).expect("should deserialize JSON without desc");
        assert!(point2.desc.is_none());
        assert!(point2.data_type.is_none());

        let ds2: DataSet = point2.into();
        let options2 = ds2.options.as_ref().unwrap();
        assert_eq!(options2.len(), 1);
        assert_eq!(options2[0].name, "long_name");

        let restored2 = PspacePoint::try_from(ds2).expect("should convert back to PspacePoint");
        assert!(restored2.desc.is_none());
        assert!(restored2.data_type.is_none());

        // Error cases: missing name
        let bad_ds = DataSet {
            id: "1".to_string(),
            name: None,
            category: None,
            r#type: None,
            options: Some(vec![]),
            format: None,
        };
        assert!(PspacePoint::try_from(bad_ds).is_err());

        // Error cases: missing type
        let bad_ds2 = DataSet {
            id: "1".to_string(),
            name: Some("n".to_string()),
            category: None,
            r#type: None,
            options: Some(vec![]),
            format: None,
        };
        assert!(PspacePoint::try_from(bad_ds2).is_err());

        // Error cases: invalid id
        let bad_ds3 = DataSet {
            id: "not_a_number".to_string(),
            name: Some("n".to_string()),
            category: None,
            r#type: Some("PS_ANALOG".to_string()),
            options: Some(vec![]),
            format: None,
        };
        assert!(PspacePoint::try_from(bad_ds3).is_err());
    }

    #[test]
    fn test_pspace_point_with_data_type() {
        // JSON with data_type field (pSpace format)
        let json = r#"{"id":150021,"name":"压力","type":"PS_ANALOG","long_name":"\\北京\\朝阳\\压力","desc":"pressure","data_type":"psDataType_Float"}"#;
        let point: PspacePoint =
            serde_json::from_str(json).expect("should deserialize JSON with data_type");
        assert_eq!(point.id, 150021);
        assert_eq!(point.data_type.as_deref(), Some("psDataType_Float"));

        // data_type() converts pSpace type to IpcDataType
        assert_eq!(point.data_type(), Some(IpcDataType::Float32));

        // PspacePoint -> DataSet: data_type keeps original pSpace format
        let ds: DataSet = point.into();
        let options = ds.options.as_ref().unwrap();
        assert_eq!(options.len(), 3); // long_name, desc, data_type
        assert_eq!(options[2].name, "data_type");
        assert_eq!(options[2].display, "psDataType_Float"); // raw pSpace type preserved

        // DataSet -> PspacePoint round-trip: data_type preserved in pSpace format
        let restored = PspacePoint::try_from(ds).expect("should convert back");
        assert_eq!(restored.data_type.as_deref(), Some("psDataType_Float"));
        assert_eq!(restored.data_type(), Some(IpcDataType::Float32));
        assert_eq!(restored.desc.as_deref(), Some("pressure"));

        // Test other data type conversions via data_type() method
        let json2 = r#"{"id":150022,"name":"计数","type":"PS_ANALOG","long_name":"\\北京\\计数","data_type":"psDataType_Int32"}"#;
        let point2: PspacePoint = serde_json::from_str(json2).unwrap();
        assert_eq!(point2.data_type(), Some(IpcDataType::Int32));
        let ds2: DataSet = point2.into();
        let dt2 = ds2
            .options
            .as_ref()
            .unwrap()
            .iter()
            .find(|o| o.name == "data_type")
            .unwrap();
        assert_eq!(dt2.display, "psDataType_Int32"); // preserved

        // Round-trip still works
        let restored2 = PspacePoint::try_from(ds2).unwrap();
        assert_eq!(restored2.data_type(), Some(IpcDataType::Int32));

        let json3 = r#"{"id":150023,"name":"名称","type":"PS_STRING","long_name":"\\北京\\名称","data_type":"psDataType_String"}"#;
        let point3: PspacePoint = serde_json::from_str(json3).unwrap();
        assert_eq!(point3.data_type(), Some(IpcDataType::VarChar(1024)));
    }

    #[test]
    fn test_to_ipc_data_type_all_variants() {
        // Test every known pSpace data type mapping
        assert_eq!(
            to_ipc_data_type("psDataType_Empty").unwrap(),
            IpcDataType::Null
        );
        assert_eq!(
            to_ipc_data_type("psDataType_Bool").unwrap(),
            IpcDataType::Bool
        );
        assert_eq!(
            to_ipc_data_type("psDataType_Int8").unwrap(),
            IpcDataType::Int8
        );
        assert_eq!(
            to_ipc_data_type("psDataType_UInt8").unwrap(),
            IpcDataType::UInt8
        );
        assert_eq!(
            to_ipc_data_type("psDataType_Int16").unwrap(),
            IpcDataType::Int16
        );
        assert_eq!(
            to_ipc_data_type("psDataType_UInt16").unwrap(),
            IpcDataType::UInt16
        );
        assert_eq!(
            to_ipc_data_type("psDataType_Int32").unwrap(),
            IpcDataType::Int32
        );
        assert_eq!(
            to_ipc_data_type("psDataType_UInt32").unwrap(),
            IpcDataType::UInt32
        );
        assert_eq!(
            to_ipc_data_type("psDataType_Int64").unwrap(),
            IpcDataType::Int64
        );
        assert_eq!(
            to_ipc_data_type("psDataType_UInt64").unwrap(),
            IpcDataType::UInt64
        );
        assert_eq!(
            to_ipc_data_type("psDataType_Float").unwrap(),
            IpcDataType::Float32
        );
        assert_eq!(
            to_ipc_data_type("psDataType_Double").unwrap(),
            IpcDataType::Float64
        );
        assert_eq!(
            to_ipc_data_type("psDataType_Time").unwrap(),
            IpcDataType::Timestamp(arrow_schema::TimeUnit::Millisecond)
        );
        assert_eq!(
            to_ipc_data_type("psDataType_String").unwrap(),
            IpcDataType::VarChar(1024)
        );
        assert_eq!(
            to_ipc_data_type("psDataType_WString").unwrap(),
            IpcDataType::NChar(1024)
        );
        assert_eq!(
            to_ipc_data_type("psDataType_Blob").unwrap(),
            IpcDataType::Blob
        );
    }

    #[test]
    fn test_to_ipc_data_type_unknown() {
        let err = to_ipc_data_type("psDataType_Unknown").unwrap_err();
        assert!(err.to_string().contains("unknown pSpace data type"));

        let err2 = to_ipc_data_type("").unwrap_err();
        assert!(err2.to_string().contains("unknown pSpace data type"));
    }

    #[test]
    fn test_pspace_point_data_type_method_unknown() {
        // When data_type string is unknown, data_type() should return None (logs warning)
        let point = PspacePoint {
            id: 1,
            name: "test".to_string(),
            r#type: "PS_ANALOG".to_string(),
            long_name: "test".to_string(),
            desc: None,
            data_type: Some("psDataType_Unknown".to_string()),
        };
        assert_eq!(point.data_type(), None);
    }

    #[test]
    fn test_pspace_point_data_type_method_none() {
        let point = PspacePoint {
            id: 1,
            name: "test".to_string(),
            r#type: "PS_ANALOG".to_string(),
            long_name: "test".to_string(),
            desc: None,
            data_type: None,
        };
        assert_eq!(point.data_type(), None);
    }
}
