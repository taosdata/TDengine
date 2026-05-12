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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
    struct Claims {
        sub: i64,
        name: String,
    }

    #[test]
    fn jwt_encode_decode_roundtrips_claims_without_required_registered_fields() {
        let claims = Claims {
            sub: 42,
            name: "agent".to_string(),
        };

        let token = jwt_encode(&claims, b"secret").unwrap();
        let decoded: Claims = jwt_decode(&token, b"secret").unwrap();

        assert_eq!(decoded, claims);
    }

    #[test]
    fn jwt_decode_rejects_wrong_secret() {
        let claims = Claims {
            sub: 7,
            name: "xnoded".to_string(),
        };
        let token = jwt_encode(&claims, b"secret-a").unwrap();

        let result = jwt_decode::<Claims>(&token, b"secret-b");

        assert!(result.is_err());
    }

    #[test]
    fn jwt_decode_rejects_malformed_token() {
        let result = jwt_decode::<Claims>("not-a-jwt", b"secret");

        assert!(result.is_err());
    }
}
