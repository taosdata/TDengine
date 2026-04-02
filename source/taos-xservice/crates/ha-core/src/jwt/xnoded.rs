use anyhow::Context;

use crate::types::XnodedId;

static SECRET: &[u8] = &[
    251, 218, 95, 137, 166, 39, 142, 67, 171, 241, 125, 12, 81, 232, 181, 255, 224, 93, 189, 151,
    177, 241, 164, 46, 39, 93, 33, 24, 189, 144, 54, 253,
];

pub struct XnodedToken(String);

impl From<String> for XnodedToken {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for XnodedToken {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl XnodedToken {
    pub fn jwt_decode(&self) -> anyhow::Result<XnodedId> {
        let data = super::jwt_decode(&self.0, SECRET).context("decode xnoded token error")?;
        Ok(data)
    }
}

pub fn jwt_encode(xnoded_id: &XnodedId) -> Result<String, jsonwebtoken::errors::Error> {
    super::jwt_encode(xnoded_id, SECRET)
}
