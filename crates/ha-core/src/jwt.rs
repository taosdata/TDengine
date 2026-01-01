use std::sync::LazyLock;

use anyhow::Context;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};

use crate::types::XnodedId;

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
        static SECRET: &[u8] = &[
            251, 218, 95, 137, 166, 39, 142, 67, 171, 241, 125, 12, 81, 232, 181, 255, 224, 93,
            189, 151, 177, 241, 164, 46, 39, 93, 33, 24, 189, 144, 54, 253,
        ];
        static VALIDATION: LazyLock<Validation> = LazyLock::new(|| {
            let mut validation = Validation::new(Algorithm::default());
            validation.required_spec_claims.clear();
            validation
        });
        let token = jsonwebtoken::decode::<XnodedId>(
            &self.0,
            &DecodingKey::from_secret(SECRET),
            &VALIDATION,
        )
        .context("decode xnoded token error")?;
        Ok(token.claims)
    }
}

pub fn xnoded_jwt_encode(xnoded_id: &XnodedId) -> Result<String, jsonwebtoken::errors::Error> {
    static SECRET: &[u8] = &[
        251, 218, 95, 137, 166, 39, 142, 67, 171, 241, 125, 12, 81, 232, 181, 255, 224, 93, 189,
        151, 177, 241, 164, 46, 39, 93, 33, 24, 189, 144, 54, 253,
    ];

    jsonwebtoken::encode(
        &Header::default(),
        xnoded_id,
        &EncodingKey::from_secret(SECRET),
    )
}
