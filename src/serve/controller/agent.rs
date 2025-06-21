//! Agent - user should register agent in taosX service to connect a local service \
//! to remote taosX/taosExplorer/TDengine.
//!
use std::{borrow::Cow, convert::Infallible, fmt::Display, str::FromStr, sync::OnceLock};

use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sqlx::{Decode, Encode, FromRow, Type, encode::IsNull, sqlite::SqliteArgumentValue};
use tracing::{Instrument, debug};
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

#[derive(Debug, Serialize, Deserialize, Clone, Copy, ToSchema, Type)]
#[serde(rename_all = "snake_case")]
#[sqlx(rename_all = "snake_case")]
pub enum AgentStatus {
    Created,
    Connected,
    Disconnected,
    Outdated,
    /// All belows states are **deprecated**.
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[derive(sqlx::Type)]
#[sqlx(rename_all = "snake_case")]
pub enum AgentActivity {
    Create,
    Connect,
    Disconnected,
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
pub struct Activity {
    pub id: i64,
    pub at: chrono::DateTime<Utc>,
    pub level: LevelFilter,
    pub activity: String,
    pub status: String,
    pub context: Option<Context>,
}

impl Activity {
    pub fn new<T: ToString>(
        id: i64,
        at: DateTime<Utc>,
        level: LevelFilter,
        activity: impl Into<String>,
        status: impl Into<String>,
        context: impl Into<Option<T>>,
    ) -> Self {
        Activity {
            id,
            at,
            level,
            activity: activity.into(),
            status: status.into(),
            context: context.into().map(|v| {
                let v = v.to_string();
                v.parse().unwrap()
            }),
        }
    }

    pub fn info(id: i64, activity: impl Into<String>, status: impl Into<String>) -> Self {
        Self::new::<String>(id, Utc::now(), LevelFilter::Info, activity, status, None)
    }
}

#[test]
fn test_activity() {
    let _ = Activity::new(1, Utc::now(), LevelFilter::Info, "a", "b", "c");
}

#[derive(Debug, Clone, Serialize, Deserialize, FromRow, ToSchema)]
#[serde(rename_all = "snake_case")]
pub struct AgentActivityItem {
    pub id: i64,
    at: chrono::DateTime<Utc>,
    level: LevelFilter,
    activity: String,
    status: String,
    // #[serde(deserialize_with = "deserialize_context")]
    context: Option<Context>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Context(serde_json::Value);

impl From<serde_json::Value> for Context {
    fn from(value: serde_json::Value) -> Self {
        Self(value)
    }
}
impl From<&str> for Context {
    fn from(value: &str) -> Self {
        Self::from_str(value).unwrap()
    }
}
impl From<String> for Context {
    fn from(value: String) -> Self {
        Self::from_str(&value).unwrap()
    }
}

impl Display for Context {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            serde_json::Value::String(s) => f.write_str(s),
            v => write!(f, "{}", v),
        }
    }
}

impl FromStr for Context {
    type Err = Infallible;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Context(
            s.parse()
                .unwrap_or(serde_json::Value::String(s.to_string())),
        ))
    }
}
impl Type<sqlx::Sqlite> for Context {
    fn type_info() -> <sqlx::Sqlite as sqlx::Database>::TypeInfo {
        <String as sqlx::Type<sqlx::Sqlite>>::type_info()
    }
}

impl<'r> sqlx::Decode<'r, sqlx::Sqlite> for Context {
    fn decode(
        value: <sqlx::Sqlite as sqlx::database::Database>::ValueRef<'r>,
    ) -> Result<Self, sqlx::error::BoxDynError> {
        let value: String = sqlx::Decode::<sqlx::Sqlite>::decode(value)?;

        // now you can parse this into your type (assuming there is a `FromStr`)

        Ok(value.parse()?)
    }
}
impl<'q> Encode<'q, sqlx::Sqlite> for Context {
    fn encode(
        self,
        args: &mut Vec<SqliteArgumentValue<'q>>,
    ) -> Result<IsNull, sqlx::error::BoxDynError> {
        args.push(SqliteArgumentValue::Text(Cow::Owned(self.to_string())));

        Ok(IsNull::No)
    }

    fn encode_by_ref(
        &self,
        args: &mut Vec<SqliteArgumentValue<'q>>,
    ) -> Result<IsNull, sqlx::error::BoxDynError> {
        args.push(SqliteArgumentValue::Text(Cow::Owned(self.to_string())));

        Ok(IsNull::No)
    }
}

// fn deserialize_context<'de, D>(deserializer: D) -> Result<Option<Context>, D::Error>
// where
//     D: Deserializer<'de>,
// {
//     let value: Option<String> = Option::deserialize(deserializer)?;
//     Ok(value
//         .map(|s| serde_json::Value::from_str(&s).unwrap_or(serde_json::Value::String(s)))
//         .map(Context))
// }

#[derive(Debug, Serialize, ToSchema)]
pub struct AgentWithToken {
    pub id: i64,
    pub token: AgentToken,
    pub ca: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    last_modified_at: Option<DateTime<Utc>>,
    #[sqlx(skip)]
    pub status: Option<AgentStatus>,
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
            ca: get_grpc_ssl_ca_certificate().map(|s| s.to_string()),
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

// impl AgentUpdates {
//     pub fn update_agent_with(&self, id: i64) -> String {
//         format!("UPDATE agents SET `name` = {} WHERE id = {id}", self.name)
//     }
// }
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
            "Decoding agent token `{}` error: {}",
            self.token, self.source
        ))
    }
}
impl AgentToken {
    pub fn jwt_decode(&self, secret: impl AsRef<[u8]>) -> Result<AgentClaims, AgentTokenError> {
        lazy_static::lazy_static! {
            static ref VALIDATION: Validation = {
                let mut validation = Validation::new(Algorithm::default());
                validation.required_spec_claims.clear();
                validation
            };
        }
        // dbg!(std::str::from_utf8(secret.as_ref()).unwrap());
        jsonwebtoken::decode(
            self.0.as_str(),
            &DecodingKey::from_secret(secret.as_ref()),
            &VALIDATION,
        )
        .map_err(|source| AgentTokenError {
            token: self.0.clone(),
            source,
        })
        .map(|data| data.claims)
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ToSchema, sqlx::Type)]
#[repr(u8)]
#[serde(rename_all = "snake_case")]
pub enum LevelFilter {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
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
#[derive(Debug, Deserialize, Serialize, IntoParams, ToSchema, Default)]
pub struct AgentActivityFilter {
    /// activities created time
    pub since: Option<DateTime<Utc>>,
    /// activities level
    pub level: Option<LevelFilter>,
    /// records limit number
    pub limit: Option<usize>,
    /// record order by config
    pub order: Option<ActivityOrder>,
}

impl AgentActivityFilter {
    pub fn condition(&self) -> String {
        let since = self.since.as_ref().map(|s| format!("`at` >= '{}'", s));
        let level = self.level.map(|l| format!("`level` <= {}", l as u8));
        let cond = since.into_iter().chain(level).fold(None, |acc, i| {
            if let Some(acc) = acc {
                Some(format!("{acc} AND {i}"))
            } else {
                Some(i)
            }
        });
        let limit = self.limit.unwrap_or(10);

        let order = self.order.unwrap_or(ActivityOrder::Desc);
        match cond {
            Some(cond) => format!("AND {cond} ORDER BY `at` {order} LIMIT {limit}"),
            None => format!("ORDER BY `at` {order} LIMIT {limit}"),
        }
    }
}

impl super::TaskController {
    pub async fn agent_activities(
        &self,
        agent_id: i64,
        filter: &AgentActivityFilter,
    ) -> anyhow::Result<Vec<AgentActivityItem>> {
        let cond = filter.condition();
        let sql = format!("select * from agent_activities where `id` = {agent_id} {cond}");
        debug!("sql: {sql}");
        let items = sqlx::query_as(&sql).fetch_all(&self.pool).await?;
        Ok(items)
    }

    pub async fn all_agents_activities(&self) -> anyhow::Result<Vec<AgentActivityItem>> {
        let cond = AgentActivityFilter {
            since: None,
            level: None,
            limit: Some(10000),
            order: Some(ActivityOrder::Asc),
        }
        .condition();
        let sql = format!(
            "select * from (select *, row_number() over (partition by id order by at desc) as rn from agent_activities) r where r.rn<=5 {cond}"
        );
        let items = sqlx::query_as(&sql)
            .fetch_all(&self.pool)
            .in_current_span()
            .await?;
        Ok(items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_all_agents_activities() {
        let cond = AgentActivityFilter {
            since: None,
            level: None,
            limit: Some(10000),
            order: Some(ActivityOrder::Desc),
        }
        .condition();
        let sql = format!(
            "select * from (select *, row_number() over (partition by id order by at desc) as rn from agent_activities) r where r.rn<=5 {cond}"
        );
        println!("{}", sql);
    }
}
