use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct DataSourceValidation {
    pub valid: bool,
    pub support: bool,
    pub data_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl DataSourceValidation {
    pub fn invalid(data_source: String, message: String) -> DataSourceValidation {
        DataSourceValidation {
            valid: false,
            support: false,
            data_source,
            version: None,
            message: Option::from(message),
        }
    }

    pub fn unknown() -> Self {
        Self {
            valid: false,
            support: false,
            data_source: "unknown".to_string(),
            version: None,
            message: Option::from("unknown data source".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization() {
        // serialize
        let data = r#"
        {
            "valid": true,
            "support": true,
            "data_source": "kafka"
        }"#;
        let v: DataSourceValidation = serde_json::from_str(data).unwrap();
        assert_eq!(true, v.valid);
        assert_eq!(true, v.support);
        assert_eq!("kafka", v.data_source);
        assert_eq!(None, v.version);
        assert_eq!(None, v.message);

        // deserialize
        let dsv = DataSourceValidation {
            valid: false,
            support: true,
            data_source: "kafka".to_string(),
            version: None,
            message: None,
        };
        let json = serde_json::to_string(&dsv).unwrap();
        print!("{}", json);
        assert_eq!(
            r#"{"valid":false,"support":true,"data_source":"kafka"}"#,
            json
        );
    }
}
