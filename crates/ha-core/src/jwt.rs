use std::sync::LazyLock;

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};

pub mod agent;
pub mod xnoded;

type Result<T> = std::result::Result<T, jsonwebtoken::errors::Error>;

static VALIDATION: LazyLock<Validation> = LazyLock::new(|| {
    let mut validation = Validation::new(Algorithm::default());
    validation.required_spec_claims.clear();
    validation
});

fn jwt_encode<T>(claims: &T, secret: &[u8]) -> Result<String>
where
    T: serde::Serialize,
{
    jsonwebtoken::encode(
        &Header::default(),
        claims,
        &EncodingKey::from_secret(secret),
    )
}

fn jwt_decode<T>(token: &str, secret: &[u8]) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let token = jsonwebtoken::decode::<T>(token, &DecodingKey::from_secret(secret), &VALIDATION)?;
    Ok(token.claims)
}
