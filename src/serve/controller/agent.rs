//! Agent - user should register agent in taosX service to connect a local service \
//! to remote taosX/taosExplorer/TDengine.
//!
use std::{
    fmt::Display,
    sync::{LazyLock, OnceLock},
};

use anyhow::Context;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use tracing::debug;
use utoipa::{IntoParams, ToSchema};

static GRPC_SSL_CA_CERTIFICATE: OnceLock<String> = OnceLock::new();

pub fn set_grpc_ssl_ca_certificate(ca: impl Into<String>) {
    if GRPC_SSL_CA_CERTIFICATE.set(ca.into()).is_ok() {
        debug!("Set grpc ssl ca certificate");
    }
}

pub fn get_grpc_ssl_ca_certificate() -> Option<&'static str> {
    GRPC_SSL_CA_CERTIFICATE.get().map(|s| s.as_str())
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Created,
    Connected,
    Disconnected,
    Outdated,
    /// All below states are **deprecated**.
    /// Use connected, disconnected instead.
    ///
    /// Lease these here for activities compatibility.
    Online,
    Offline,
    Pending,
    Alive,
    Idle,
    // #[@deprecated(note = "use `transferring` instead")]
    Busy,
    Transferring,
    Error,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AgentWithToken {
    pub id: i64,
    pub token: AgentToken,
    pub ca: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct AgentConnectors(Vec<String>);

impl TryFrom<String> for AgentConnectors {
    type Error = serde_json::Error;
    fn try_from(value: String) -> Result<Self, serde_json::Error> {
        serde_json::from_str(&value)
    }
}
/// Raw Agent repr.
#[derive(Debug, Clone, ToSchema, Serialize, Deserialize)]
pub struct Agent {
    pub id: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, IntoParams)]
pub struct AgentUpdates {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AgentClaims {
    /// The agent id
    pub sub: i64,
    /// Unix epoch in seconds for created time.
    pub iat: i64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AgentToken(pub String);

impl AgentToken {
    pub fn jwt_decode(&self) -> anyhow::Result<AgentClaims> {
        static VALIDATION: LazyLock<Validation> = LazyLock::new(|| {
            let mut validation = Validation::new(Algorithm::default());
            validation.required_spec_claims.clear();
            validation
        });
        static SECRET: &[u8] = &[
            126, 222, 130, 137, 43, 122, 41, 173, 144, 146, 116, 138, 153, 244, 251, 99, 50, 55,
            140, 238, 218, 232, 15, 161, 226, 54, 130, 40, 211, 234, 111, 171,
        ];
        Ok(jsonwebtoken::decode(
            &self.0,
            &DecodingKey::from_secret(SECRET),
            &VALIDATION.clone(),
        )
        .context("decode agent token error")?
        .claims)
    }
}

impl std::ops::Deref for AgentToken {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T: AsRef<[u8]>> From<T> for AgentToken {
    fn from(value: T) -> Self {
        Self(String::from_utf8_lossy(value.as_ref()).to_string())
    }
}

impl Display for AgentToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Deserialize, Serialize, Debug, Clone, Copy, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ActivityOrder {
    /// asc order
    Asc,
    /// desc order
    Desc,
}
impl Display for ActivityOrder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActivityOrder::Asc => f.write_str("asc"),
            ActivityOrder::Desc => f.write_str("desc"),
        }
    }
}
