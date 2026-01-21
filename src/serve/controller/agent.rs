//! Agent - user should register agent in taosX service to connect a local service \
//! to remote taosX/taosExplorer/TDengine.
//!
use std::{fmt::Display, sync::OnceLock};

use ha_core::jwt::agent::AgentToken;
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
