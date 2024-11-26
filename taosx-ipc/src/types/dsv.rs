use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataSourceValidation {
    pub valid: bool,
    pub support: bool,
    pub data_source: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub namespaces: Option<Vec<String>>,
}

impl DataSourceValidation {
    pub fn valid<S: Into<String>>(data_source: S, version: Option<String>) -> DataSourceValidation {
        Self {
            valid: true,
            support: true,
            data_source: data_source.into(),
            version,
            message: None,
            namespaces: None,
        }
    }

    pub fn invalid<S: Into<String>>(data_source: S, message: String) -> DataSourceValidation {
        DataSourceValidation {
            valid: false,
            support: false,
            data_source: data_source.into(),
            version: None,
            message: Option::from(message),
            namespaces: None,
        }
    }

    pub fn unknown() -> Self {
        Self {
            valid: false,
            support: false,
            data_source: "unknown".to_string(),
            version: None,
            message: Option::from("unknown data source".to_string()),
            namespaces: None,
        }
    }

    pub fn ok(&self) -> anyhow::Result<()> {
        match (self.valid, self.support) {
            (true, true) => Ok(()),
            (false, _) => {
                debug_assert!(self.message.is_some());
                Err(anyhow::anyhow!(
                    "Data source {} is invalid since {}",
                    self.data_source,
                    self.message.as_ref().unwrap()
                ))
            }
            (_, false) => {
                debug_assert!(self.message.is_some());
                Err(anyhow::anyhow!(
                    "Data source {} connection is valid but not supported, since {}",
                    self.data_source,
                    self.message.as_ref().unwrap()
                ))
            }
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
        assert!(v.valid);
        assert!(v.support);
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
            namespaces: None,
        };
        let json = serde_json::to_string(&dsv).unwrap();
        print!("{}", json);
        assert_eq!(
            r#"{"valid":false,"support":true,"data_source":"kafka"}"#,
            json
        );
    }
}
