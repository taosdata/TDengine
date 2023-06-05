//! Agent - user should register agent in taosX service to connect a local service \
//! to remote taosX/taosExplorer/TDengine.
//!
use std::fmt::Display;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use itertools::Itertools;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sqlx::{Decode, Encode, FromRow};
use tokio_util::sync::CancellationToken;
use utoipa::{IntoParams, ToSchema};

use super::Task;

// pub struct IpcWorker {}

pub struct AgentWorker {
    cancel: CancellationToken,
    task: Task,
}

impl AgentWorker {
    pub fn spawn(&self) {
        if self.task.from.starts_with("opc") {
            // opc
        }
    }
    pub fn sender(&self) {}
    pub fn send(&self) {}
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy, ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
pub enum AgentStatus {
    Created,
    Alive,
    Error,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
pub enum AgentActivity {
    Create,
    Connect,
    Offline,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AgentWithToken {
    pub id: i64,
    pub token: AgentToken,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
pub struct AgentConnectors(Vec<String>);

impl TryFrom<String> for AgentConnectors {
    type Error = serde_json::Error;
    fn try_from(value: String) -> Result<Self, serde_json::Error> {
        serde_json::from_str(&value)
    }
}
/// Raw Agent repr.
#[derive(Debug, Clone, ToSchema, Serialize, Deserialize, FromRow, Encode, Decode)]
pub struct Agent {
    pub id: i64,

    pub dsn: String,
    pub name: String,
    pub cluster_id: String,
    pub user_id: String,

    created_at: DateTime<Utc>,
    last_modified_at: Option<DateTime<Utc>>,
    status: Option<AgentStatus>,
}

impl Agent {
    pub fn jwt_claims(&self) -> AgentClaims {
        AgentClaims {
            sub: self.id,
            iat: self.created_at.timestamp(),
        }
    }
    pub fn jwt_encode(&self, secret: impl AsRef<[u8]>) -> String {
        self.jwt_claims().jwt_encode(secret)
    }

    pub fn with_token(&self, secret: impl AsRef<[u8]>) -> AgentWithToken {
        let token = self.jwt_encode(secret);
        AgentWithToken {
            id: self.id,
            token: AgentToken(token),
        }
    }
}

/// Create a new Agent from some properties.
#[derive(Debug, Deserialize, ToSchema)]
pub struct AgentProps {
    pub dsn: String,
    pub name: String,
    pub cluster_id: String,
    pub user_id: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema, IntoParams)]
pub struct AgentUpdates {
    pub name: String,
}

impl AgentUpdates {
    pub fn update_agent_with(&self, id: i64) -> String {
        format!("UPDATE agents SET `name` = {} WHERE id = {id}", self.name)
    }
}
#[derive(Debug, Serialize, Deserialize)]
pub struct AgentClaims {
    /// The agent id
    pub sub: i64,
    /// Unix epoch in seconds for created time.
    pub iat: i64,
}

impl AgentClaims {
    pub fn jwt_encode(&self, secret: impl AsRef<[u8]>) -> String {
        jsonwebtoken::encode(
            &Header::default(),
            self,
            &EncodingKey::from_secret(secret.as_ref()),
        )
        .expect("JWT token generation failed")
    }
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AgentToken(pub String);

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
#[derive(Debug, thiserror::Error)]
pub struct AgentTokenError {
    token: String,
    source: jsonwebtoken::errors::Error,
}

impl Display for AgentTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!(
            "Decoding agent token `{}` error: {:?}",
            self.token, self.source
        ))
    }
}
impl AgentToken {
    pub fn jwt_decode(&self, secret: impl AsRef<[u8]>) -> Result<AgentClaims, AgentTokenError> {
        jsonwebtoken::decode(
            self.0.as_str(),
            &DecodingKey::from_secret(secret.as_ref()),
            &Validation::new(Algorithm::default()),
        )
        .map_err(|source| AgentTokenError {
            token: self.0.clone(),
            source,
        })
        .map(|data| data.claims)
    }
}
