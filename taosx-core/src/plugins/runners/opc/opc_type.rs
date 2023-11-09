use std::str::FromStr;
use serde::Serialize;
use taos::Dsn;

#[derive(Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum OpcType {
    OPCUA,
    OPCDA,
    FAKE,
}

impl OpcType {
    /// valid dsn driver:
    /// opcua:// -> OPCUA
    /// opcda:// -> OPCDA
    /// fake:// -> FAKE
    /// opc+ua:// -> OPCUA
    /// opc+da:// -> OPCDA
    pub fn from_dsn(dsn: &Dsn) -> anyhow::Result<Self> {
        let fake = dsn.params.get("fake").is_some();
        if fake {
            return Ok(Self::FAKE);
        }

        let opc_type = dsn.driver.as_str();
        let protocol = dsn.protocol.clone();
        match opc_type {
            "opcua" => Ok(Self::OPCUA),
            "opcda" => Ok(Self::OPCDA),
            "fake" => Ok(Self::FAKE),
            "opc" => {
                match protocol.as_deref() {
                    Some("ua") => Ok(Self::OPCUA),
                    Some("da") => Ok(Self::OPCDA),
                    _ => anyhow::bail!("invalid opc protocol"),
                }
            }
            _ => anyhow::bail!("invalid opc type"),
        }
    }
}

impl FromStr for OpcType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "opcua" => Ok(Self::OPCUA),
            "opcda" => Ok(Self::OPCDA),
            "fake" => Ok(Self::FAKE),
            _ => Err(s.to_string()),
        }
    }
}
