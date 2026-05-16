#![recursion_limit = "256"]

use std::cmp;
use std::fmt::{Debug, Display};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono_tz::Tz;
use dashmap::DashMap;
use futures::stream::{SplitSink, SplitStream};
use futures::StreamExt;
use futures_util::SinkExt;
use once_cell::sync::OnceCell;
use query::Error as QueryError;
use rand::seq::SliceRandom;
use rand::Rng;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::WebPkiServerVerifier;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct};
use taos_query::prelude::Code;
use taos_query::util::{generate_req_id, Edition};
use taos_query::{DsnError, IntoDsn, RawError, RawResult};
use tokio::time;
use tokio_tungstenite::tungstenite::error::ProtocolError;
use tokio_tungstenite::tungstenite::extensions::DeflateConfig;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;

pub mod stmt;
pub use stmt::Stmt;

pub mod stmt2;
pub use stmt2::Stmt2;
pub(crate) use stmt2::Stmt2Inner;

pub mod consumer;
pub use consumer::{Consumer, Offset, TmqBuilder};

pub mod query;
use query::WsConnReq;
pub use query::{ResultSet, Taos};
pub(crate) use taos_query::block_in_place_or_global;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{
    connect_async_tls_with_config, Connector, MaybeTlsStream, WebSocketStream,
};

use crate::query::asyn::WS_ERROR_NO;
use crate::query::messages::{ToMessage, WsRecv, WsRecvData, WsSend};
use crate::query::{send_conn_request, ConnOption};

type Version = String;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsStreamReader = SplitStream<WsStream>;
type WsStreamSender = SplitSink<WsStream, Message>;

const CONNECTOR_INFO: &str = concat!(
    "rust-ws-v",
    env!("CARGO_PKG_VERSION"),
    "-",
    env!("GIT_COMMIT_ID")
);

#[derive(Debug, Clone)]
pub enum WsAuth {
    Token(String),
    Plain(String, String),
}

const DEFAULT_RETRY_RETRIES: u32 = 5;
const DEFAULT_RETRY_BACKOFF_MS: u64 = 200;
const DEFAULT_RETRY_BACKOFF_MAX_MS: u64 = 2000;

#[derive(Debug, Clone)]
struct RetryPolicy {
    retries: u32,
    backoff_ms: u64,
    backoff_max_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TlsMode {
    VerifyCa,
    VerifyIdentity,
}

impl std::str::FromStr for TlsMode {
    type Err = DsnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "verify_ca" => Ok(TlsMode::VerifyCa),
            "verify_identity" => Ok(TlsMode::VerifyIdentity),
            _ => Err(DsnError::InvalidParam("tls_mode".into(), s.into())),
        }
    }
}

#[derive(Debug, Clone)]
enum TlsVersion {
    TLSv1_2,
    TLSv1_3,
}

impl std::str::FromStr for TlsVersion {
    type Err = DsnError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "tlsv1.2" => Ok(TlsVersion::TLSv1_2),
            "tlsv1.3" => Ok(TlsVersion::TLSv1_3),
            _ => Err(DsnError::InvalidParam("tls_version".into(), s.into())),
        }
    }
}

#[derive(Debug, Clone)]
struct TlsConfig {
    mode: Option<TlsMode>,
    versions: Option<Vec<TlsVersion>>,
    certs: Option<Vec<CertificateDer<'static>>>,
}

#[derive(Debug, Clone)]
pub struct TaosBuilder {
    https: Arc<AtomicBool>,
    addrs: Vec<String>,
    current_addr_index: Arc<AtomicUsize>,
    auth: WsAuth,
    database: Option<String>,
    server_version: OnceCell<String>,
    conn_mode: Option<u32>,
    compression: bool,
    retry_policy: RetryPolicy,
    tz: Option<Tz>,
    conn_options: DashMap<i32, Option<String>>,
    tcp_nodelay: bool,
    read_timeout: Duration,
    conn_timeout: Duration,
    version_prefer: String,
    user_ip: Option<String>,
    user_app: Option<String>,
    tls_config: Option<TlsConfig>,
    connector_info: String,
    totp_code: Option<String>,
    bearer_token: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EndpointType {
    Ws,
    Stmt,
    Tmq,
}

#[derive(Debug, thiserror::Error)]
pub struct Error {
    code: Code,
    source: anyhow::Error,
}

impl Error {
    pub const fn errno(&self) -> Code {
        self.code
    }

    pub fn errstr(&self) -> String {
        self.source.to_string()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.source.to_string())
    }
}

impl From<DsnError> for Error {
    fn from(err: DsnError) -> Self {
        Error {
            code: Code::FAILED,
            source: err.into(),
        }
    }
}

impl From<query::asyn::Error> for Error {
    fn from(err: query::asyn::Error) -> Self {
        Error {
            code: Code::FAILED,
            source: err.into(),
        }
    }
}

impl taos_query::TBuilder for TaosBuilder {
    type Target = Taos;

    fn available_params() -> &'static [&'static str] {
        &["token"]
    }

    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        Self::from_dsn(dsn.into_dsn()?)
    }

    fn client_version() -> &'static str {
        "0"
    }

    fn ping(&self, taos: &mut Self::Target) -> RawResult<()> {
        taos_query::Queryable::exec(taos, "select server_version()").map(|_| ())
    }

    fn ready(&self) -> bool {
        true
    }

    fn build(&self) -> RawResult<Self::Target> {
        block_in_place_or_global(Taos::from_builder(self.clone()))
    }

    fn server_version(&self) -> RawResult<&str> {
        if let Some(v) = self.server_version.get() {
            Ok(v.as_str())
        } else {
            let conn = self.build()?;
            use taos_query::prelude::sync::Queryable;
            let v: String = Queryable::query_one(&conn, "select server_version()")?.unwrap();
            Ok(match self.server_version.try_insert(v) {
                Ok(v) | Err((v, _)) => v.as_str(),
            })
        }
    }

    fn is_enterprise_edition(&self) -> RawResult<bool> {
        let addr = self.active_addr();
        if addr.matches(".cloud.tdengine.com").next().is_some()
            || addr.matches(".cloud.taosdata.com").next().is_some()
        {
            return Ok(true);
        }

        let taos = self.build()?;

        use taos_query::prelude::sync::Queryable;
        let grant: RawResult<Option<(String, bool)>> = Queryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        );

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                Queryable::query_one(&taos, "show grants");

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                tracing::warn!(
                    "Can't check enterprise edition with either \"show cluster\" or \"show grants\""
                );
                Edition::new("unknown", true)
            }
        };
        Ok(edition.is_enterprise_edition())
    }

    fn get_edition(&self) -> RawResult<Edition> {
        let addr = self.active_addr();
        if addr.matches(".cloud.tdengine.com").next().is_some()
            || addr.matches(".cloud.taosdata.com").next().is_some()
        {
            let edition = Edition::new("cloud", false);
            return Ok(edition);
        }

        let taos = self.build()?;

        use taos_query::prelude::sync::Queryable;
        let grant: RawResult<Option<(String, bool)>> = Queryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        );

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                Queryable::query_one(&taos, "show grants");

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    expired.trim() == "false" || expired.trim() == "unlimited",
                )
            } else {
                tracing::warn!(
                    "Can't check enterprise edition with either \"show cluster\" or \"show grants\""
                );
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
    }
}

#[async_trait::async_trait]
impl taos_query::AsyncTBuilder for TaosBuilder {
    type Target = Taos;

    fn from_dsn<D: IntoDsn>(dsn: D) -> RawResult<Self> {
        Self::from_dsn(dsn.into_dsn()?)
    }

    fn client_version() -> &'static str {
        "0"
    }

    async fn ping(&self, taos: &mut Self::Target) -> RawResult<()> {
        taos_query::AsyncQueryable::exec(taos, "select server_version()")
            .await
            .map(|_| ())
    }

    async fn ready(&self) -> bool {
        true
    }

    async fn build(&self) -> RawResult<Self::Target> {
        Taos::from_builder(self.clone()).await
    }

    async fn server_version(&self) -> RawResult<&str> {
        if let Some(v) = self.server_version.get() {
            Ok(v.as_str())
        } else {
            let conn = <Self as taos_query::AsyncTBuilder>::build(self).await?;
            use taos_query::prelude::AsyncQueryable;
            let v: String = AsyncQueryable::query_one(&conn, "select server_version()")
                .await?
                .unwrap();
            Ok(match self.server_version.try_insert(v) {
                Ok(v) | Err((v, _)) => v.as_str(),
            })
        }
    }

    async fn is_enterprise_edition(&self) -> RawResult<bool> {
        use taos_query::prelude::AsyncQueryable;

        let taos = self.build().await?;
        // Ensure server is ready
        taos.exec("select server_version()").await?;

        let addr = self.active_addr();
        if addr.matches(".cloud.tdengine.com").next().is_some()
            || addr.matches(".cloud.taosdata.com").next().is_some()
        {
            return Ok(true);
        }

        let grant: RawResult<Option<(String, bool)>> = AsyncQueryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        )
        .await;

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                AsyncQueryable::query_one(&taos, "show grants").await;

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    // Valid choices: false/unlimited, otherwise expired.
                    !(expired.trim() == "false" || expired.trim() == "unlimited"),
                )
            } else {
                tracing::warn!(
                    "Can't check enterprise edition with either \"show cluster\" or \"show grants\""
                );
                Edition::new("unknown", true)
            }
        };

        Ok(edition.is_enterprise_edition())
    }

    async fn get_edition(&self) -> RawResult<Edition> {
        use taos_query::prelude::AsyncQueryable;

        let taos = self.build().await?;
        // Ensure server is ready
        taos.exec("select server_version()").await?;

        let addr = self.active_addr();
        if addr.matches(".cloud.tdengine.com").next().is_some()
            || addr.matches(".cloud.taosdata.com").next().is_some()
        {
            let edition = Edition::new("cloud", false);
            return Ok(edition);
        }

        let grant: RawResult<Option<(String, bool)>> = AsyncQueryable::query_one(
            &taos,
            "select version, (expire_time < now) from information_schema.ins_cluster",
        )
        .await;

        let edition = if let Ok(Some((edition, expired))) = grant {
            Edition::new(edition, expired)
        } else {
            let grant: RawResult<Option<(String, (), String)>> =
                AsyncQueryable::query_one(&taos, "show grants").await;

            if let Ok(Some((edition, _, expired))) = grant {
                Edition::new(
                    edition.trim(),
                    // Valid choices: false/unlimited, otherwise expired.
                    !(expired.trim() == "false" || expired.trim() == "unlimited"),
                )
            } else {
                tracing::warn!(
                    "Can't check enterprise edition with either \"show cluster\" or \"show grants\""
                );
                Edition::new("unknown", true)
            }
        };
        Ok(edition)
    }
}

impl TaosBuilder {
    fn scheme(&self) -> &'static str {
        if self.https.load(Ordering::SeqCst) {
            "wss"
        } else {
            "ws"
        }
    }

    fn set_https(&self, https: bool) {
        self.https.store(https, Ordering::SeqCst);
    }

    pub fn from_dsn<T: IntoDsn>(dsn: T) -> RawResult<Self> {
        let mut dsn = dsn.into_dsn()?;

        let is_https = match (dsn.driver.as_str(), dsn.protocol.as_deref()) {
            ("ws" | "http", _) | ("taos" | "taosws" | "tmq", Some("ws" | "http") | None) => false,
            ("wss" | "https", _) | ("taos" | "taosws" | "tmq", Some("wss" | "https")) => true,
            _ => Err(DsnError::InvalidDriver(dsn.to_string()))?,
        };

        let https = Arc::new(AtomicBool::new(is_https));

        let conn_mode = match dsn.params.get("conn_mode") {
            Some(s) => match s.parse::<u32>() {
                Ok(num) => Some(num),
                Err(_) => Err(DsnError::InvalidDriver(dsn.to_string()))?,
            },
            None => None,
        };

        let token = dsn.params.remove("token");

        let mut addrs = Vec::with_capacity(dsn.addresses.len());
        for addr in &dsn.addresses {
            let addr = if addr.host.as_deref() == Some("localhost") && addr.port.is_none() {
                "localhost:6041".to_string()
            } else {
                addr.to_string()
            };
            addrs.push(addr);
        }

        if addrs.is_empty() {
            addrs.push("localhost:6041".to_string());
        }

        addrs.shuffle(&mut rand::rng());

        let compression = dsn
            .params
            .remove("compression")
            .and_then(|s| {
                if s.trim().is_empty() {
                    Some(true)
                } else {
                    s.trim().parse::<bool>().ok()
                }
            })
            .unwrap_or(false);

        let retries = dsn
            .remove("conn_retries")
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(DEFAULT_RETRY_RETRIES);

        let backoff_ms = dsn
            .remove("retry_backoff_ms")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_RETRY_BACKOFF_MS);

        let backoff_max_ms = dsn
            .remove("retry_backoff_max_ms")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(DEFAULT_RETRY_BACKOFF_MAX_MS);

        let retry_policy = RetryPolicy {
            retries,
            backoff_ms,
            backoff_max_ms,
        };

        let tz = dsn
            .remove("timezone")
            .map(|s| {
                s.parse::<Tz>()
                    .map_err(|_| DsnError::InvalidParam("timezone".to_string(), s.clone()))
            })
            .transpose()?;

        let tcp_nodelay = dsn
            .remove("tcp_nodelay")
            .and_then(|s| {
                if s.trim().is_empty() {
                    Some(true)
                } else {
                    s.trim().parse::<bool>().ok()
                }
            })
            .unwrap_or(true);

        let read_timeout = dsn
            .remove("read_timeout")
            .and_then(|s| s.parse::<u64>().ok())
            .map_or(Duration::from_secs(300), Duration::from_secs);

        let conn_timeout = dsn
            .remove("conn_timeout")
            .and_then(|s| s.parse::<u64>().ok())
            .map_or(Duration::from_secs(10), Duration::from_secs);

        let version_prefer = match dsn.remove("version_prefer").as_deref().map(str::trim) {
            None | Some("2.x") => "2.x".to_string(),
            Some("3.x") => "3.x".to_string(),
            Some(v) => {
                return Err(
                    DsnError::InvalidParam("version_prefer".to_string(), v.to_string()).into(),
                )
            }
        };

        let user_ip = dsn.remove("user_ip").map(|s| s.trim().to_string());
        let user_app = dsn.remove("user_app").map(|s| s.trim().to_string());

        let tls_config = if is_https {
            let mode = dsn.remove("tls_mode").map(|s| s.parse()).transpose()?;

            let versions = dsn
                .remove("tls_version")
                .map(|s| s.split(',').map(|s| s.parse()).collect())
                .transpose()?;

            let certs = if mode.is_some() {
                match dsn.remove("tls_ca").as_deref().map(str::trim) {
                    None | Some("") => return Err(DsnError::RequireParam("tls_ca".into()).into()),
                    Some(tls_ca) => Some(parse_ca_to_certs(tls_ca)?),
                }
            } else {
                None
            };

            match (mode, versions, certs) {
                (None, None, None) => None,
                (mode, versions, certs) => Some(TlsConfig {
                    mode,
                    versions,
                    certs,
                }),
            }
        } else {
            None
        };

        let connector_info = dsn
            .remove("connector_info")
            .unwrap_or_else(|| CONNECTOR_INFO.to_string());

        let totp_code = dsn.remove("totp_code").map(|s| s.trim().to_string());
        let bearer_token = dsn.remove("bearer_token").map(|s| s.trim().to_string());

        let auth = if let Some(token) = token {
            WsAuth::Token(token)
        } else {
            let username = dsn.username.unwrap_or_else(|| "root".to_string());
            let password = dsn.password.unwrap_or_else(|| "taosdata".to_string());
            WsAuth::Plain(username, password)
        };

        Ok(TaosBuilder {
            https,
            addrs,
            auth,
            database: dsn.subject,
            server_version: OnceCell::new(),
            conn_mode,
            compression,
            retry_policy,
            current_addr_index: Arc::new(AtomicUsize::new(0)),
            tz,
            conn_options: DashMap::new(),
            tcp_nodelay,
            read_timeout,
            conn_timeout,
            version_prefer,
            user_ip,
            user_app,
            tls_config,
            connector_info,
            totp_code,
            bearer_token,
        })
    }

    pub(crate) async fn connect(&self) -> RawResult<(WsStream, Version)> {
        self.connect_with_cb(
            EndpointType::Ws,
            send_conn_request(self.build_conn_request(), self.conn_timeout),
        )
        .await
    }

    pub(crate) async fn connect_with_ty(&self, ty: EndpointType) -> RawResult<(WsStream, Version)> {
        self.connect_with_opt_cb::<fn(&mut WsStream) -> Pin<Box<dyn Future<Output = RawResult<()>> + Send + '_>>>(ty, None).await
    }

    pub(crate) async fn connect_with_cb<F>(
        &self,
        ty: EndpointType,
        cb: F,
    ) -> RawResult<(WsStream, Version)>
    where
        F: for<'a> Fn(&'a mut WsStream) -> Pin<Box<dyn Future<Output = RawResult<()>> + Send + 'a>>,
    {
        self.connect_with_opt_cb(ty, Some(cb)).await
    }

    async fn connect_with_opt_cb<F>(
        &self,
        ty: EndpointType,
        cb: Option<F>,
    ) -> RawResult<(WsStream, Version)>
    where
        F: for<'a> Fn(&'a mut WsStream) -> Pin<Box<dyn Future<Output = RawResult<()>> + Send + 'a>>,
    {
        let mut config = WebSocketConfig::default();
        config.max_frame_size = None;
        config.max_message_size = None;
        if self.compression {
            cfg_if::cfg_if! {
                if #[cfg(feature = "deflate")] {
                    tracing::trace!("WebSocket compression enabled");
                    config.compression = Some(DeflateConfig::default());
                } else {
                    tracing::warn!("WebSocket compression is not supported, missing `deflate` feature");
                }
            }
        }

        let mut last_err = None;
        let connector = self.build_tls_connector()?;

        for _ in 0..self.addrs.len() {
            let mut url = self.to_url(ty);
            for i in 0..=self.retry_policy.retries {
                tracing::trace!("connecting to TDengine WebSocket server, url: {url}");
                match connect_async_tls_with_config(
                    &url,
                    Some(config),
                    self.tcp_nodelay,
                    connector.clone(),
                )
                .await
                {
                    Ok((mut ws_stream, _)) => {
                        if ty == EndpointType::Stmt {
                            return Ok((ws_stream, String::new()));
                        }

                        macro_rules! call {
                            ($func:expr, $ctx:expr) => {
                                match $func.await {
                                    Ok(res) => res,
                                    Err(err) => {
                                        tracing::warn!("failed to {:?}: {}", $ctx, err);
                                        let code = err.code();
                                        last_err = Some(err);
                                        if code != WS_ERROR_NO::WEBSOCKET_DISCONNECTED.as_code() {
                                            break;
                                        }
                                        continue;
                                    }
                                }
                            };
                        }

                        let version = call!(
                            self.send_version_request(&mut ws_stream),
                            "send version request"
                        );

                        if let Some(ref cb) = cb {
                            call!(cb(&mut ws_stream), "call callback");
                        }

                        if !self.conn_options.is_empty() {
                            call!(
                                self.send_options_connection_request(&mut ws_stream),
                                "send options_connection request"
                            );
                        }

                        return Ok((ws_stream, version));
                    }
                    Err(err) => {
                        tracing::warn!("failed to connect to {url}, err: {err:?}");
                        if matches!(&err, WsError::Protocol(ProtocolError::WrongHttpVersion))
                            || matches!(&err, WsError::Http(resp) if resp.status() == 307)
                        {
                            self.set_https(true);
                            url = url.replace("ws://", "wss://");
                            last_err = Some(QueryError::from(err).into());
                            continue;
                        } else if matches!(&err, WsError::Http(resp) if resp.status() == 400 || resp.status() == 404)
                        {
                            url = match ty {
                                EndpointType::Ws => self.to_query_url(),
                                EndpointType::Stmt => self.to_stmt_url(),
                                EndpointType::Tmq => self.to_tmq_url(),
                            };
                            last_err = Some(QueryError::from(err).into());
                            continue;
                        } else if matches!(&err, WsError::Http(resp) if resp.status() == 401) {
                            last_err = Some(QueryError::Unauthorized(url).into());
                            break;
                        }
                        last_err = Some(QueryError::from(err).into());
                    }
                }

                tracing::warn!("failed to connect to {}, retrying...({})", url, i + 1);

                let base_delay = cmp::min(
                    self.retry_policy.backoff_max_ms,
                    self.retry_policy.backoff_ms * 2u64.saturating_pow(i),
                );

                let jitter = 1.0 + rand::rng().random_range(-0.2..=0.2);
                let delay = (base_delay as f64 * jitter) as u64;

                tokio::time::sleep(Duration::from_millis(delay)).await;
            }

            let cur_idx = self.current_addr_index.load(Ordering::Relaxed);
            let next_idx = (cur_idx + 1) % self.addrs.len();
            self.current_addr_index.store(next_idx, Ordering::Relaxed);
        }

        tracing::error!("failed to connect to all addresses: {:?}", self.addrs);

        if let Some(err) = last_err {
            Err(err)
        } else {
            Err(RawError::from_code(WS_ERROR_NO::WEBSOCKET_ERROR.as_code())
                .context("failed to connect to all addresses"))
        }
    }

    fn build_tls_connector(&self) -> RawResult<Option<Connector>> {
        if self.tls_config.is_none() {
            return Ok(None);
        }

        let tls_cfg = self.tls_config.as_ref().unwrap();
        let protocol_versions = match &tls_cfg.versions {
            Some(versions) => {
                let mut protocol_versions = Vec::with_capacity(versions.len());
                for version in versions {
                    match version {
                        TlsVersion::TLSv1_2 => protocol_versions.push(&rustls::version::TLS12),
                        TlsVersion::TLSv1_3 => protocol_versions.push(&rustls::version::TLS13),
                    }
                }
                protocol_versions
            }
            None => vec![&rustls::version::TLS13],
        };

        let mut root_store = rustls::RootCertStore::empty();
        if tls_cfg.mode.is_some() {
            if let Some(certs) = &tls_cfg.certs {
                let (num_added, num_ignored) =
                    root_store.add_parsable_certificates(certs.iter().cloned());
                tracing::trace!("added {num_added} valid certificates, ignored {num_ignored} invalid certificates");
                if num_ignored > 0 {
                    return Err(RawError::new(
                        WS_ERROR_NO::TLS_ERROR.as_code(),
                        format!("provided {num_ignored} CA certificates are invalid"),
                    ));
                }
            }
        }

        let root_store = Arc::new(root_store);
        let mut client_config = ClientConfig::builder_with_protocol_versions(&protocol_versions)
            .with_root_certificates(root_store.clone())
            .with_no_client_auth();

        if let Some(mode) = tls_cfg.mode {
            if mode == TlsMode::VerifyCa {
                let inner = WebPkiServerVerifier::builder(root_store)
                    .build()
                    .map_err(|e| {
                        RawError::new(
                            WS_ERROR_NO::TLS_ERROR.as_code(),
                            format!("failed to build WebPkiServerVerifier: {e}"),
                        )
                    })?;
                let verifier = Arc::new(NoServerNameVerification::new(inner));
                client_config.dangerous().set_certificate_verifier(verifier);
            }
        } else {
            let verifier = Arc::new(NoCertificateVerification {});
            client_config.dangerous().set_certificate_verifier(verifier);
        }

        Ok(Some(Connector::Rustls(Arc::new(client_config))))
    }

    async fn send_version_request(&self, ws_stream: &mut WsStream) -> RawResult<Version> {
        send_request_with_timeout(ws_stream, WsSend::Version.to_msg(), self.conn_timeout).await?;

        let version_fut = async {
            let max_non_version_cnt = 5;
            let mut non_version_cnt = 0;
            loop {
                if let Some(res) = ws_stream.next().await {
                    let message = res.map_err(handle_disconnect_error)?;
                    tracing::trace!("send_version_request, received message: {message}");
                    match message {
                        Message::Text(text) => {
                            let resp: WsRecv = serde_json::from_str(&text).map_err(|err| {
                                RawError::any(err)
                                    .with_code(WS_ERROR_NO::DE_ERROR.as_code())
                                    .context("invalid json response")
                            })?;
                            let (_, data, ok) = resp.ok();
                            ok?;
                            match data {
                                WsRecvData::Version { version } => {
                                    return Ok(version);
                                }
                                _ => return Ok(self.version_prefer.clone()),
                            }
                        }
                        Message::Ping(bytes) => {
                            ws_stream
                                .send(Message::Pong(bytes))
                                .await
                                .map_err(handle_disconnect_error)?;

                            non_version_cnt += 1;
                            if non_version_cnt >= max_non_version_cnt {
                                return Ok(self.version_prefer.clone());
                            }
                        }
                        _ => return Ok(self.version_prefer.clone()),
                    }
                } else {
                    return Err(RawError::from_code(
                        WS_ERROR_NO::WEBSOCKET_DISCONNECTED.as_code(),
                    ));
                }
            }
        };

        let version = match time::timeout(self.conn_timeout, version_fut).await {
            Ok(Ok(ver)) => ver,
            Ok(Err(err)) => return Err(err),
            Err(_) => self.version_prefer.clone(),
        };

        tracing::trace!("send_version_request, server version: {version}");

        Ok(version)
    }

    async fn send_options_connection_request(&self, ws_stream: &mut WsStream) -> RawResult<()> {
        let mut options = Vec::with_capacity(self.conn_options.len());
        for entry in &self.conn_options {
            options.push(ConnOption {
                option: *entry.key(),
                value: entry.value().clone(),
            });
        }

        let req = WsSend::OptionsConnection {
            req_id: generate_req_id(),
            options,
        };

        send_request_with_timeout(ws_stream, req.to_msg(), self.conn_timeout).await?;

        loop {
            let res = time::timeout(self.conn_timeout, ws_stream.next())
                .await
                .map_err(|_| {
                    RawError::from_code(WS_ERROR_NO::RECV_MESSAGE_TIMEOUT.as_code())
                        .context("timeout waiting for options_connection response")
                })?;

            let Some(res) = res else {
                return Err(RawError::from_code(
                    WS_ERROR_NO::WEBSOCKET_DISCONNECTED.as_code(),
                ));
            };

            let message = res.map_err(handle_disconnect_error)?;
            tracing::trace!("send_options_connection_request, received message: {message}");

            match message {
                Message::Text(text) => {
                    let resp: WsRecv = serde_json::from_str(&text).map_err(|err| {
                        RawError::any(err)
                            .with_code(WS_ERROR_NO::DE_ERROR.as_code())
                            .context("invalid json response")
                    })?;
                    let (_, data, ok) = resp.ok();
                    ok?;
                    match data {
                        WsRecvData::OptionsConnection { .. } => return Ok(()),
                        WsRecvData::Version { .. } => {}
                        _ => {
                            return Err(RawError::from_string(format!(
                                "unexpected options_connection response: {data:?}"
                            )))
                        }
                    }
                }
                Message::Ping(bytes) => {
                    ws_stream
                        .send(Message::Pong(bytes))
                        .await
                        .map_err(handle_disconnect_error)?;
                }
                _ => {
                    return Err(RawError::from_string(format!(
                        "unexpected message during options_connection: {message:?}"
                    )))
                }
            }
        }
    }

    pub(crate) fn build_conn_request(&self) -> WsConnReq {
        let (user, password) = match &self.auth {
            WsAuth::Token(_) => ("root", "taosdata"),
            WsAuth::Plain(user, pass) => (user.as_str(), pass.as_str()),
        };

        WsConnReq {
            user: Some(user.to_string()),
            password: Some(password.to_string()),
            db: self.database.clone(),
            mode: (self.conn_mode == Some(1)).then_some(0), // for adapter, 0 is bi mode
            tz: self.tz.map(|s| s.to_string()),
            ip: self.user_ip.clone(),
            app: self.user_app.clone(),
            connector: self.connector_info.clone(),
            totp_code: self.totp_code.clone(),
            bearer_token: self.bearer_token.clone(),
        }
    }

    #[inline]
    pub(crate) fn to_url(&self, ty: EndpointType) -> String {
        match ty {
            EndpointType::Tmq => self.to_tmq_url(),
            _ => self.to_ws_url(),
        }
    }

    #[inline]
    pub(crate) fn to_ws_url(&self) -> String {
        self.format_url("ws")
    }

    #[inline]
    pub(crate) fn to_query_url(&self) -> String {
        self.format_url("rest/ws")
    }

    #[inline]
    pub(crate) fn to_stmt_url(&self) -> String {
        self.format_url("rest/stmt")
    }

    #[inline]
    pub(crate) fn to_tmq_url(&self) -> String {
        self.format_url("rest/tmq")
    }

    fn format_url(&self, path: &str) -> String {
        let addr = self.active_addr();
        match &self.auth {
            WsAuth::Token(token) => {
                format!("{}://{}/{}?token={}", self.scheme(), addr, path, token)
            }
            WsAuth::Plain(_, _) => {
                format!("{}://{}/{}", self.scheme(), addr, path)
            }
        }
    }

    #[inline]
    fn active_addr(&self) -> &String {
        let cur_addr_idx = self.current_addr_index.load(Ordering::Relaxed);
        &self.addrs[cur_addr_idx]
    }
}

async fn send_request_with_timeout(
    ws_stream: &mut WsStream,
    message: Message,
    timeout: Duration,
) -> RawResult<()> {
    tracing::trace!("sending request, message: {message:?}");

    time::timeout(timeout, ws_stream.send(message))
        .await
        .map_err(|_| {
            RawError::from_code(WS_ERROR_NO::SEND_MESSAGE_TIMEOUT.as_code())
                .context("timeout sending request")
        })?
        .map_err(handle_disconnect_error)
}

#[inline]
fn handle_disconnect_error(err: WsError) -> RawError {
    match err {
        WsError::ConnectionClosed
        | WsError::AlreadyClosed
        | WsError::Io(_)
        | WsError::Tls(_)
        | WsError::Protocol(_) => {
            RawError::from_code(WS_ERROR_NO::WEBSOCKET_DISCONNECTED.as_code())
                .context("WebSocket connection disconnected")
        }
        _ => RawError::any(err)
            .with_code(WS_ERROR_NO::WEBSOCKET_ERROR.as_code())
            .context("WebSocket error"),
    }
}

#[derive(Debug)]
pub struct NoCertificateVerification;

impl ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        use rustls::SignatureScheme::*;
        vec![
            RSA_PKCS1_SHA1,
            ECDSA_SHA1_Legacy,
            RSA_PKCS1_SHA256,
            ECDSA_NISTP256_SHA256,
            RSA_PKCS1_SHA384,
            ECDSA_NISTP384_SHA384,
            RSA_PKCS1_SHA512,
            ECDSA_NISTP521_SHA512,
            RSA_PSS_SHA256,
            RSA_PSS_SHA384,
            RSA_PSS_SHA512,
            ED25519,
            ED448,
        ]
    }
}

#[derive(Debug)]
pub struct NoServerNameVerification {
    inner: Arc<WebPkiServerVerifier>,
}

impl NoServerNameVerification {
    pub fn new(inner: Arc<WebPkiServerVerifier>) -> Self {
        Self { inner }
    }
}

impl ServerCertVerifier for NoServerNameVerification {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self.inner.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            Ok(scv) => Ok(scv),
            Err(rustls::Error::InvalidCertificate(cert_err)) => match cert_err {
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. } => {
                    tracing::warn!(
                        "TLS ignore hostname, server_name: {server_name:?}, err: {cert_err:?}"
                    );
                    Ok(ServerCertVerified::assertion())
                }
                _ => {
                    tracing::error!("TLS invalid certificate: {cert_err:?}");
                    Err(rustls::Error::InvalidCertificate(cert_err))
                }
            },
            Err(e) => {
                tracing::error!("TLS certificate verification failed: {e:?}");
                Err(e)
            }
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
    }
}

fn parse_ca_to_certs(input: &str) -> Result<Vec<CertificateDer<'static>>, DsnError> {
    use rustls::pki_types::pem::PemObject;

    if input.starts_with("-----BEGIN CERTIFICATE-----") {
        CertificateDer::pem_slice_iter(input.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| DsnError::InvalidParam("tls_ca".into(), format!("parse PEM failed: {e}")))
    } else {
        CertificateDer::pem_file_iter(std::path::Path::new(input))
            .map_err(|e| {
                DsnError::InvalidParam("tls_ca".into(), format!("open PEM file failed: {e}"))
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                DsnError::InvalidParam("tls_ca".into(), format!("parse PEM file failed: {e}"))
            })
    }
}

#[cfg(test)]
mod tests {
    use futures::{SinkExt, StreamExt};
    use taos_query::prelude::*;

    use crate::query::messages::{ToMessage, WsRecv, WsRecvData, WsSend};
    use crate::*;

    #[tokio::test]
    async fn test_connect() -> Result<(), anyhow::Error> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let builder = TaosBuilder::from_dsn("ws://localhost:6041")?;
        let (ws, _) = builder.connect().await?;
        let (mut sender, mut reader) = ws.split();

        sender.send(WsSend::Version.to_msg()).await?;

        loop {
            if let Some(Ok(message)) = reader.next().await {
                let text = message.to_text().unwrap();
                let recv: WsRecv = serde_json::from_str(text).unwrap();
                assert_eq!(recv.code, 0);
                if let WsRecvData::Version { version, .. } = recv.data {
                    tracing::info!("server version: {version}");
                    break;
                }
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_connect_enable_tcp_nodelay() -> Result<(), anyhow::Error> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let cases = ["true", "false", ""];
        for tcp_nodelay_val in cases {
            let dsn = format!("ws://localhost:6041?tcp_nodelay={tcp_nodelay_val}");
            let builder = TaosBuilder::from_dsn(dsn)?;
            let (ws, _) = builder.connect().await?;
            let (mut sender, mut reader) = ws.split();

            sender.send(WsSend::Version.to_msg()).await?;

            loop {
                if let Some(Ok(message)) = reader.next().await {
                    let text = message.to_text().unwrap();
                    let recv: WsRecv = serde_json::from_str(text).unwrap();
                    assert_eq!(recv.code, 0);
                    if let WsRecvData::Version { version, .. } = recv.data {
                        tracing::info!("server version: {version}");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    #[test]
    fn test_conn_timeout() -> Result<(), anyhow::Error> {
        let builder = TaosBuilder::from_dsn("ws://localhost:6041?conn_timeout=20")?;
        assert_eq!(builder.conn_timeout, Duration::from_secs(20));

        let builder = TaosBuilder::from_dsn("ws://localhost:6041")?;
        assert_eq!(builder.conn_timeout, Duration::from_secs(10));

        let builder = TaosBuilder::from_dsn("ws://localhost:6041?conn_timeout=2x")?;
        assert_eq!(builder.conn_timeout, Duration::from_secs(10));

        Ok(())
    }

    #[test]
    fn test_version_prefer() -> Result<(), anyhow::Error> {
        let builder = TaosBuilder::from_dsn("ws://localhost:6041")?;
        assert_eq!(builder.version_prefer, "2.x");

        let builder = TaosBuilder::from_dsn("ws://localhost:6041?version_prefer=2.x")?;
        assert_eq!(builder.version_prefer, "2.x");

        let builder = TaosBuilder::from_dsn("ws://localhost:6041?version_prefer=3.x")?;
        assert_eq!(builder.version_prefer, "3.x");

        let err = TaosBuilder::from_dsn("ws://localhost:6041?version_prefer=4.x").unwrap_err();
        assert!(err
            .message()
            .contains("invalid parameter for version_prefer: 4.x"),);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recv_version_resp_timeout() -> anyhow::Result<()> {
        use serde::Deserialize;
        use taos_query::util::ws_proxy::*;

        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::ERROR)
            .try_init();

        let intercept: InterceptFn = {
            Arc::new(move |msg, _ctx| {
                if let Message::Text(text) = msg {
                    if text.contains("version") {
                        tokio::task::block_in_place(|| {
                            std::thread::sleep(std::time::Duration::from_millis(1500));
                        });
                    }
                }
                ProxyAction::Forward
            })
        };

        let _proxy = WsProxy::start("127.0.0.1:8901", "ws://localhost:6041/ws", intercept).await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8901?conn_timeout=1&version_prefer=3.x")?
            .build()
            .await?;
        assert_eq!(taos.version(), "3.x");

        taos.exec_many([
            "drop database if exists test_1762842183",
            "create database test_1762842183",
            "use test_1762842183",
            "create table t0 (ts timestamp, c1 int)",
            "insert into t0 values (1726803356466, 1)",
        ])
        .await?;

        #[derive(Debug, Deserialize)]
        struct Row {
            ts: i64,
            c1: i32,
        }

        let rows: Vec<Row> = taos
            .query("select * from t0")
            .await?
            .deserialize()
            .try_collect()
            .await?;

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].ts, 1726803356466);
        assert_eq!(rows[0].c1, 1);

        taos.exec("drop database if exists test_1762842183").await?;

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recv_version_resp_return_ping() -> anyhow::Result<()> {
        use serde_json::{json, Value};
        use warp::ws::Message;
        use warp::Filter;

        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::ERROR)
            .try_init();

        let routes = warp::path("ws")
            .and(warp::ws())
            .map(move |ws: warp::ws::Ws| {
                ws.on_upgrade(move |ws| async move {
                    let (mut ws_tx, mut ws_rx) = ws.split();

                    for _ in 0..5 {
                        let _ = ws_tx.send(Message::ping(b"HELLO")).await;
                    }

                    while let Some(res) = ws_rx.next().await {
                        let message = res.unwrap();
                        tracing::info!("ws recv message: {message:?}");

                        if message.is_text() {
                            let text = message.to_str().unwrap();
                            let req: Value = serde_json::from_str(text).unwrap();
                            let req_id = req
                                .get("args")
                                .and_then(|v| v.get("req_id"))
                                .and_then(Value::as_u64)
                                .unwrap_or(0);

                            if text.contains("conn") {
                                let data = json!({
                                    "code": 0,
                                    "message": "message",
                                    "action": "conn",
                                    "req_id": req_id
                                });
                                let _ = ws_tx.send(Message::text(data.to_string())).await;
                            }
                        }
                    }
                })
            });

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let (_, server) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 8902), async move {
                let _ = shutdown_rx.await;
            });

        tokio::spawn(server);

        let taos = TaosBuilder::from_dsn("ws://127.0.0.1:8902?version_prefer=3.x")?
            .build()
            .await?;
        assert_eq!(taos.version(), "3.x");

        let _ = shutdown_tx.send(());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recv_version_resp_return_pong() -> anyhow::Result<()> {
        use serde_json::{json, Value};
        use warp::ws::Message;
        use warp::Filter;

        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::ERROR)
            .try_init();

        let routes = warp::path("ws")
            .and(warp::ws())
            .map(move |ws: warp::ws::Ws| {
                ws.on_upgrade(move |ws| async move {
                    let (mut ws_tx, mut ws_rx) = ws.split();

                    let _ = ws_tx.send(Message::pong(b"HELLO")).await;

                    while let Some(res) = ws_rx.next().await {
                        let message = res.unwrap();
                        tracing::info!("ws recv message: {message:?}");

                        if message.is_text() {
                            let text = message.to_str().unwrap();
                            let req: Value = serde_json::from_str(text).unwrap();
                            let req_id = req
                                .get("args")
                                .and_then(|v| v.get("req_id"))
                                .and_then(Value::as_u64)
                                .unwrap_or(0);

                            if text.contains("conn") {
                                let data = json!({
                                    "code": 0,
                                    "message": "message",
                                    "action": "conn",
                                    "req_id": req_id
                                });
                                let _ = ws_tx.send(Message::text(data.to_string())).await;
                            }
                        }
                    }
                })
            });

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let (_, server) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 8903), async move {
                let _ = shutdown_rx.await;
            });

        tokio::spawn(server);

        let taos = TaosBuilder::from_dsn("ws://127.0.0.1:8903?version_prefer=3.x")?
            .build()
            .await?;
        assert_eq!(taos.version(), "3.x");

        let _ = shutdown_tx.send(());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recv_version_resp_return_conn() -> anyhow::Result<()> {
        use serde_json::{json, Value};
        use warp::ws::Message;
        use warp::Filter;

        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::ERROR)
            .try_init();

        let routes = warp::path("ws")
            .and(warp::ws())
            .map(move |ws: warp::ws::Ws| {
                ws.on_upgrade(move |ws| async move {
                    let (mut ws_tx, mut ws_rx) = ws.split();

                    while let Some(res) = ws_rx.next().await {
                        let message = res.unwrap();
                        tracing::info!("ws recv message: {message:?}");

                        if message.is_text() {
                            let text = message.to_str().unwrap();
                            let req: Value = serde_json::from_str(text).unwrap();
                            let req_id = req
                                .get("args")
                                .and_then(|v| v.get("req_id"))
                                .and_then(Value::as_u64)
                                .unwrap_or(0);

                            if text.contains("version") || text.contains("conn") {
                                let data = json!({
                                    "code": 0,
                                    "message": "message",
                                    "action": "conn",
                                    "req_id": req_id
                                });
                                let _ = ws_tx.send(Message::text(data.to_string())).await;
                            }
                        }
                    }
                })
            });

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let (_, server) =
            warp::serve(routes).bind_with_graceful_shutdown(([127, 0, 0, 1], 8906), async move {
                let _ = shutdown_rx.await;
            });

        tokio::spawn(server);

        let taos = TaosBuilder::from_dsn("ws://127.0.0.1:8906?version_prefer=3.x")?
            .build()
            .await?;
        assert_eq!(taos.version(), "3.x");

        let _ = shutdown_tx.send(());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recv_conn_resp_timeout() -> anyhow::Result<()> {
        use taos_query::util::ws_proxy::*;

        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::ERROR)
            .try_init();

        let intercept: InterceptFn = {
            Arc::new(move |_msg, _ctx| {
                tokio::task::block_in_place(|| {
                    std::thread::sleep(std::time::Duration::from_secs(2));
                });
                ProxyAction::Forward
            })
        };

        let _proxy = WsProxy::start("127.0.0.1:8904", "ws://localhost:6041/ws", intercept).await;

        let err = TaosBuilder::from_dsn("ws://localhost:8904?conn_timeout=1")?
            .build()
            .await
            .unwrap_err();
        assert_eq!(err.code(), WS_ERROR_NO::RECV_MESSAGE_TIMEOUT.as_code());
        assert!(err
            .to_string()
            .contains("timeout waiting for conn response"));

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_recv_options_connection_resp_timeout() -> anyhow::Result<()> {
        use serde_json::Value;
        use taos_query::util::ws_proxy::*;

        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::ERROR)
            .try_init();

        let intercept: InterceptFn = {
            Arc::new(move |msg, ctx| {
                if let Message::Text(text) = msg {
                    let req = serde_json::from_str::<Value>(text).unwrap();
                    let action = req.get("action").and_then(|v| v.as_str()).unwrap_or("");
                    if action == "options_connection" {
                        ctx.req_count += 1;
                        if ctx.req_count == 2 {
                            tokio::task::block_in_place(|| {
                                std::thread::sleep(std::time::Duration::from_secs(2));
                            });
                        }
                    }
                } else if let Message::Binary(_) = msg {
                    return ProxyAction::Restart;
                }

                ProxyAction::Forward
            })
        };

        let _proxy = WsProxy::start("127.0.0.1:8905", "ws://localhost:6041/ws", intercept).await;

        let taos = TaosBuilder::from_dsn("ws://localhost:8905?conn_timeout=1")?
            .build()
            .await?;

        taos.client()
            .options_connection(&[ConnOption {
                option: 1,
                value: Some("UTC".to_string()),
            }])
            .await?;

        let err = taos.exec("show databases").await.unwrap_err();
        assert_eq!(err.code(), WS_ERROR_NO::CONN_CLOSED.as_code());
        assert!(err.to_string().contains("WebSocket connection is closed"));

        Ok(())
    }

    #[tokio::test]
    async fn test_report_user_ip_and_app() -> anyhow::Result<()> {
        #[derive(Debug, serde::Deserialize)]
        struct Record {
            user_ip: String,
            user_app: String,
        }

        {
            let taos = TaosBuilder::from_dsn(
                "ws://localhost:6041?user_ip=192.168.1.1&user_app=rust_test",
            )?
            .build()
            .await?;

            tokio::time::sleep(Duration::from_secs(2)).await;

            let mut rs = taos.query("show connections").await?;
            let records: Vec<Record> = rs.deserialize().try_collect().await?;
            let found = records
                .iter()
                .any(|record| record.user_ip == "192.168.1.1" && record.user_app == "rust_test");
            assert!(found);
        }

        {
            let taos = TaosBuilder::from_dsn(
                "ws://localhost:6041?user_ip=  192.168.1.2  &user_app=  rust_test  ",
            )?
            .build()
            .await?;

            tokio::time::sleep(Duration::from_secs(2)).await;

            let mut rs = taos.query("show connections").await?;
            let records: Vec<Record> = rs.deserialize().try_collect().await?;
            let found = records
                .iter()
                .any(|record| record.user_ip == "192.168.1.2" && record.user_app == "rust_test");
            assert!(found);
        }

        Ok(())
    }

    #[cfg(feature = "test-new-feat")]
    #[tokio::test]
    async fn test_report_connector_info() -> anyhow::Result<()> {
        #[derive(Debug, serde::Deserialize)]
        struct Record {
            connector_info: String,
        }

        {
            let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
                .build()
                .await?;

            tokio::time::sleep(Duration::from_secs(2)).await;

            let mut rs = taos.query("show connections").await?;
            let records: Vec<Record> = rs.deserialize().try_collect().await?;
            let found = records
                .iter()
                .any(|record| record.connector_info == CONNECTOR_INFO);
            assert!(found);
        }

        {
            let taos = TaosBuilder::from_dsn("ws://localhost:6041?connector_info=rust-0.0.1")?
                .build()
                .await?;

            tokio::time::sleep(Duration::from_secs(2)).await;

            let mut rs = taos.query("show connections").await?;
            let records: Vec<Record> = rs.deserialize().try_collect().await?;
            let found = records
                .iter()
                .any(|record| record.connector_info == "rust-0.0.1");
            assert!(found);
        }

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[tokio::test]
    async fn test_connect_with_totp() -> anyhow::Result<()> {
        use taos_query::util::totp::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec("drop user totp_user").await.ok();

        let totp_seed = generate_totp_seed(64);
        taos.exec(format!(
            "create user totp_user pass 'totp_pass_1' totpseed '{totp_seed}'"
        ))
        .await?;

        let totp_secret = generate_totp_secret(totp_seed.as_bytes());
        let totp_code = generate_totp_code(&totp_secret);

        let taost = TaosBuilder::from_dsn(format!(
            "ws://totp_user:totp_pass_1@localhost:6041?totp_code={totp_code}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        taos.exec("drop user totp_user").await?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[tokio::test]
    async fn test_connect_with_totp_twice() -> anyhow::Result<()> {
        use taos_query::util::totp::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec("drop user tw_totp_user").await.ok();

        let totp_seed = generate_totp_seed(64);
        taos.exec(format!(
            "create user tw_totp_user pass 'totp_pass_1' totpseed '{totp_seed}'"
        ))
        .await?;

        let totp_secret = generate_totp_secret(totp_seed.as_bytes());
        let totp_code = generate_totp_code(&totp_secret);

        let taost = TaosBuilder::from_dsn(format!(
            "ws://tw_totp_user:totp_pass_1@localhost:6041?totp_code={totp_code}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!(
            "ws://tw_totp_user:totp_pass_1@localhost:6041?totp_code={totp_code}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        taos.exec("drop user tw_totp_user").await?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[tokio::test]
    async fn test_connect_with_invalid_totp_code() -> anyhow::Result<()> {
        let err = TaosBuilder::from_dsn("ws://localhost:6041?totp_code=xxx")?
            .build()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Invalid TOTP code"));
        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[tokio::test]
    async fn test_connect_with_token() -> anyhow::Result<()> {
        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec("drop user token_user").await.ok();
        taos.exec("create user token_user pass 'token_pass_1'")
            .await?;

        let mut rs = taos
            .query("create token test_bearer_token from user token_user")
            .await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows.len(), 1);
        let token = &rows[0];

        let taost = TaosBuilder::from_dsn(format!("ws://localhost:6041?bearer_token={token}"))?
            .build()
            .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!("ws://localhost:6041?bearer_token={token}"))?
            .build()
            .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        taos.exec("drop user token_user").await?;

        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[tokio::test]
    async fn test_connect_with_invalid_token() -> anyhow::Result<()> {
        let err = TaosBuilder::from_dsn("ws://localhost:6041?bearer_token=xxx")?
            .build()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Invalid token"));
        Ok(())
    }

    #[cfg(feature = "test-enterprise")]
    #[tokio::test]
    async fn test_connect_with_totp_and_token() -> anyhow::Result<()> {
        use taos_query::util::totp::*;

        let taos = TaosBuilder::from_dsn("ws://localhost:6041")?
            .build()
            .await?;

        taos.exec("drop user tt_totp_user").await.ok();

        let totp_seed = generate_totp_seed(255);
        taos.exec(format!(
            "create user tt_totp_user pass 'totp_pass_1' totpseed '{totp_seed}'"
        ))
        .await?;

        let totp_secret = generate_totp_secret(totp_seed.as_bytes());
        let totp_code = generate_totp_code(&totp_secret);

        let taost = TaosBuilder::from_dsn(format!(
            "ws://tt_totp_user:totp_pass_1@localhost:6041?totp_code={totp_code}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        taos.exec("drop user tt_token_user").await.ok();
        taos.exec("create user tt_token_user pass 'token_pass_1'")
            .await?;

        let mut rs = taos
            .query("create token test_tt_bearer_token from user tt_token_user")
            .await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows.len(), 1);
        let token = &rows[0];

        let taost = TaosBuilder::from_dsn(format!("ws://localhost:6041?bearer_token={token}"))?
            .build()
            .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!(
            "ws://tt_token_user:token_pass_1@localhost:6041?totp_code={totp_code}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!(
            "ws://tt_totp_user:totp_pass_1@localhost:6041?bearer_token={token}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        let taost = TaosBuilder::from_dsn(format!(
             "ws://tt_totp_user:totp_pass_1@localhost:6041?totp_code={totp_code}&bearer_token={token}"
        ))?
        .build()
        .await?;

        let mut rs = taost.query("select 1").await?;
        let rows: Vec<String> = rs.deserialize().try_collect().await?;
        assert_eq!(rows, vec!["1".to_string()]);

        taos.exec_many(["drop user tt_totp_user", "drop user tt_token_user"])
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tls_tests {
    use super::*;
    use std::path::Path;
    use taos_query::AsyncTBuilder;

    #[tokio::test]
    async fn test_parse_ws_with_tls_params_ignored() -> anyhow::Result<()> {
        let dsn = "ws://localhost:6041?tls_mode=bad&tls_version=TLSv1.1&tls_ca=some_ca";
        let taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        assert!(taos.builder.tls_config.is_none());
        Ok(())
    }

    #[cfg(feature = "rustls-ring-crypto-provider")]
    #[tokio::test]
    async fn test_build_wss_with_tls_versions() -> anyhow::Result<()> {
        let cases = [
            "wss://localhost:6445",
            "wss://localhost:6445?tls_version=TLSv1.2",
            "wss://localhost:6445?tls_version=TLSv1.3",
            "wss://localhost:6445?tls_version=tlsv1.2,tlsv1.3",
            "wss://localhost:6445?tls_version=TLSV1.3,TLSV1.2",
            "wss://localhost:6445?tls_version=TlSv1.3,tLSv1.2,TLsv1.3,tlSv1.2",
        ];
        for dsn in cases {
            let _taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        }
        Ok(())
    }

    #[cfg(feature = "rustls-ring-crypto-provider")]
    #[tokio::test]
    async fn test_tls_verify_identity() -> anyhow::Result<()> {
        let tls_ca = include_str!("../tests/certs/ca_verify_identity.crt");

        let ca_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/ca_verify_identity.crt");
        let tls_ca_path = ca_path.to_str().unwrap();

        let tls_ca_cases = [
            format!("wss://localhost:6445?tls_mode=verify_identity&tls_version=tlsv1.2,tlsv1.3&tls_ca={tls_ca}"),
            format!("wss://localhost:6445?tls_mode=VERIFY_IDENTITY&tls_version=TLSV1.2,TLSV1.3&tls_ca={tls_ca}"),
            format!("wss://localhost:6445?tls_mode=VeRify_IdenTity&tls_version=tlsV1.2,TlsV1.3&tls_ca={tls_ca}"),
            format!("wss://localhost:6445?tls_mode=  VeRify_IdenTity  &tls_version=  TlsV1.2,  TlsV1.3  &tls_ca=  {tls_ca}  "),
            format!("wss://localhost:6445?tls_mode=verify_identity&tls_ca={tls_ca}&tls_version=tlsv1.2"),
            format!("wss://localhost:6445?tls_ca={tls_ca}&tls_mode=verify_identity&tls_version=tlsv1.3"),
            format!("wss://localhost:6445?tls_ca={tls_ca}&tls_mode=verify_identity"),
            format!("wss://localhost:6445?tls_mode=verify_identity&tls_version=TLSv1.3,TLSv1.3&tls_ca={tls_ca}"),
        ];

        let tls_ca_path_cases = [
            format!("wss://localhost:6445?tls_mode=verify_identity&tls_version=tlsv1.2,tlsv1.3&tls_ca={tls_ca_path}"),
            format!("wss://localhost:6445?tls_mode=VERIFY_IDENTITY&tls_version=TLSV1.2,TLSV1.3&tls_ca={tls_ca_path}"),
            format!("wss://localhost:6445?tls_mode=VeRify_IdenTity&tls_version=tlsV1.2,TlsV1.3&tls_ca={tls_ca_path}"),
            format!("wss://localhost:6445?tls_mode=  VeRify_IdenTity  &tls_version=  TlsV1.2,  TlsV1.3  &tls_ca=  {tls_ca_path}  "),
            format!("wss://localhost:6445?tls_mode=verify_identity&tls_ca={tls_ca_path}&tls_version=tlsv1.2"),
            format!("wss://localhost:6445?tls_ca={tls_ca_path}&tls_mode=verify_identity&tls_version=tlsv1.3"),
            format!("wss://localhost:6445?tls_ca={tls_ca_path}&tls_mode=verify_identity"),
            format!("wss://localhost:6445?tls_mode=verify_identity&tls_version=TLSv1.3,TLSv1.3&tls_ca={tls_ca_path}"),
        ];

        for dsn in tls_ca_cases {
            let _taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        }

        for dsn in tls_ca_path_cases {
            let _taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        }

        Ok(())
    }

    #[cfg(feature = "rustls-ring-crypto-provider")]
    #[tokio::test]
    async fn test_tls_verify_ca() -> anyhow::Result<()> {
        let tls_ca = include_str!("../tests/certs/ca_verify_ca.crt");

        let ca_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/ca_verify_ca.crt");
        let tls_ca_path = ca_path.to_str().unwrap();

        let tls_ca_cases = [
            format!("wss://localhost:6446?tls_mode=verify_ca&tls_version=tlsv1.2,tlsv1.3&tls_ca={tls_ca}"),
            format!("wss://localhost:6446?tls_mode=VERIFY_CA&tls_version=TLSV1.2,TLSV1.3&tls_ca={tls_ca}"),
            format!("wss://localhost:6446?tls_mode=VeRify_Ca&tls_version=tlsV1.2,TlsV1.3&tls_ca={tls_ca}"),
            format!("wss://localhost:6446?tls_mode=  VeRify_Ca  &tls_version=  TlsV1.2,  TlsV1.3  &tls_ca=  {tls_ca}  "),
            format!("wss://localhost:6446?tls_mode=verify_ca&tls_ca={tls_ca}&tls_version=tlsv1.2"),
            format!("wss://localhost:6446?tls_ca={tls_ca}&tls_mode=verify_ca&tls_version=tlsv1.3"),
            format!("wss://localhost:6446?tls_ca={tls_ca}&tls_mode=verify_ca"),
            format!("wss://localhost:6446?tls_mode=verify_ca&tls_version=TLSv1.3,TLSv1.3&tls_ca={tls_ca}"),
        ];

        let tls_ca_path_cases = [
            format!("wss://localhost:6446?tls_mode=verify_ca&tls_version=tlsv1.2,tlsv1.3&tls_ca={tls_ca_path}"),
            format!("wss://localhost:6446?tls_mode=VERIFY_CA&tls_version=TLSV1.2,TLSV1.3&tls_ca={tls_ca_path}"),
            format!("wss://localhost:6446?tls_mode=VeRify_Ca&tls_version=tlsV1.2,TlsV1.3&tls_ca={tls_ca_path}"),
            format!("wss://localhost:6446?tls_mode=  VeRify_Ca  &tls_version=  TlsV1.2,  TlsV1.3  &tls_ca=  {tls_ca_path}  "),
            format!("wss://localhost:6446?tls_mode=verify_ca&tls_ca={tls_ca_path}&tls_version=tlsv1.2"),
            format!("wss://localhost:6446?tls_ca={tls_ca_path}&tls_mode=verify_ca&tls_version=tlsv1.3"),
            format!("wss://localhost:6446?tls_ca={tls_ca_path}&tls_mode=verify_ca"),
            format!("wss://localhost:6446?tls_mode=verify_ca&tls_version=TLSv1.3,TLSv1.3&tls_ca={tls_ca_path}"),
        ];

        for dsn in tls_ca_cases {
            let _taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        }

        for dsn in tls_ca_path_cases {
            let _taos = TaosBuilder::from_dsn(dsn)?.build().await?;
        }

        Ok(())
    }

    #[cfg(feature = "rustls-ring-crypto-provider")]
    #[tokio::test]
    async fn test_tls_san_mismatch() -> anyhow::Result<()> {
        let tls_ca = include_str!("../tests/certs/ca_verify_identity.crt");
        let err = TaosBuilder::from_dsn(format!(
            "wss://127.0.0.1:6445?tls_mode=verify_identity&tls_version=tlsv1.3&tls_ca={tls_ca}"
        ))?
        .build()
        .await
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("invalid peer certificate: certificate not valid for name"));
        Ok(())
    }

    #[cfg(feature = "rustls-ring-crypto-provider")]
    #[tokio::test]
    async fn test_tls12_version_mismatch() -> anyhow::Result<()> {
        let tls_ca = include_str!("../tests/certs/ca_verify_ca.crt");
        let err = TaosBuilder::from_dsn(format!(
            "wss://localhost:6447?tls_mode=verify_ca&tls_version=tlsv1.3&tls_ca={tls_ca}"
        ))?
        .build()
        .await
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("received fatal alert: ProtocolVersion"));
        Ok(())
    }

    #[cfg(feature = "rustls-ring-crypto-provider")]
    #[tokio::test]
    async fn test_tls13_version_mismatch() -> anyhow::Result<()> {
        let tls_ca = include_str!("../tests/certs/ca_verify_ca.crt");
        let err = TaosBuilder::from_dsn(format!(
            "wss://localhost:6448?tls_mode=verify_ca&tls_version=tlsv1.2&tls_ca={tls_ca}"
        ))?
        .build()
        .await
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("received fatal alert: ProtocolVersion"));
        Ok(())
    }

    #[cfg(feature = "rustls-ring-crypto-provider")]
    #[tokio::test]
    async fn test_tls_ca_mismatch() -> anyhow::Result<()> {
        let tls_ca = include_str!("../tests/certs/ca_mismatch.crt");
        let err = TaosBuilder::from_dsn(format!(
            "wss://localhost:6445?tls_mode=verify_identity&tls_version=TLSv1.3&tls_ca={tls_ca}"
        ))?
        .build()
        .await
        .unwrap_err();
        assert!(err
            .to_string()
            .contains("invalid peer certificate: BadSignature"));
        Ok(())
    }

    #[test]
    fn test_parse_invalid_tls_mode() {
        let dsn = "wss://localhost:6041?tls_mode=bad";
        let err = TaosBuilder::from_dsn(dsn).unwrap_err();
        assert!(err.to_string().contains("invalid parameter for tls_mode"));
    }

    #[test]
    fn test_parse_invalid_tls_version() {
        let dsn = "wss://localhost:6041?tls_version=TLSv1.1";
        let err = TaosBuilder::from_dsn(dsn).unwrap_err();
        assert!(err
            .to_string()
            .contains("invalid parameter for tls_version"));
    }

    #[test]
    fn test_parse_invalid_tls_ca_content() {
        let err = TaosBuilder::from_dsn("wss://localhost:6041?tls_mode=verify_ca&tls_version=TLSv1.3&tls_ca=-----BEGIN CERTIFICATE-----BAD-----END CERTIFICATE-----").unwrap_err();
        assert!(err.to_string().contains("parse PEM failed"));

        let ca_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/invalid.pem");
        let tls_ca_path = ca_path.to_str().unwrap();

        let err = TaosBuilder::from_dsn(format!(
            "wss://localhost:6041?tls_mode=verify_ca&tls_version=TLSv1.3&tls_ca={tls_ca_path}"
        ))
        .unwrap_err();
        assert!(err.to_string().contains("parse PEM file failed"));
    }

    #[test]
    fn test_parse_invalid_tls_ca_path() {
        let err = TaosBuilder::from_dsn("wss://localhost:6041?tls_mode=verify_ca&tls_version=TLSv1.3&tls_ca=/path/not/exist.crt").unwrap_err();
        assert!(err.to_string().contains("open PEM file failed"));
    }

    #[test]
    fn test_parse_verify_ca_without_ca_err() {
        let cases = [
            "wss://localhost:6041?tls_mode=verify_ca&tls_version=TLSv1.3",
            "wss://localhost:6041?tls_mode=verify_ca&tls_version=TLSv1.3&tls_ca",
            "wss://localhost:6041?tls_mode=verify_ca&tls_version=TLSv1.3&tls_ca=",
            "wss://localhost:6041?tls_mode=verify_ca&tls_version=TLSv1.3&tls_ca=  ",
        ];
        for dsn in cases {
            let err = TaosBuilder::from_dsn(dsn).unwrap_err();
            assert!(err.to_string().contains("requires parameter: tls_ca"));
        }
    }

    #[test]
    fn test_parse_verify_identity_without_ca_err() {
        let cases = [
            "wss://localhost:6041?tls_mode=verify_identity&tls_version=TLSv1.3",
            "wss://localhost:6041?tls_mode=verify_identity&tls_version=TLSv1.3&tls_ca",
            "wss://localhost:6041?tls_mode=verify_identity&tls_version=TLSv1.3&tls_ca=",
            "wss://localhost:6041?tls_mode=verify_identity&tls_version=TLSv1.3&tls_ca=  ",
        ];
        for dsn in cases {
            let err = TaosBuilder::from_dsn(dsn).unwrap_err();
            assert!(err.to_string().contains("requires parameter: tls_ca"));
        }
    }
}

#[cfg(feature = "rustls-ring-crypto-provider")]
#[cfg(test)]
mod cloud_tests {
    use futures::{SinkExt, StreamExt};

    use crate::query::messages::{ToMessage, WsRecv, WsRecvData, WsSend};
    use crate::*;

    #[tokio::test]
    async fn test_conncet() -> Result<(), anyhow::Error> {
        let _ = tracing_subscriber::fmt()
            .with_file(true)
            .with_line_number(true)
            .with_max_level(tracing::Level::INFO)
            .compact()
            .try_init();

        let url = std::env::var("TDENGINE_CLOUD_URL");
        if url.is_err() {
            tracing::warn!("TDENGINE_CLOUD_URL is not set, skip test_conncet");
            return Ok(());
        }

        let token = std::env::var("TDENGINE_CLOUD_TOKEN");
        if token.is_err() {
            tracing::warn!("TDENGINE_CLOUD_TOKEN is not set, skip test_conncet");
            return Ok(());
        }

        let dsn = format!("{}/rust_test?token={}", url.unwrap(), token.unwrap());
        let builder = TaosBuilder::from_dsn(dsn)?;
        let (ws, _) = builder.connect().await?;
        let (mut sender, mut reader) = ws.split();

        sender.send(WsSend::Version.to_msg()).await?;

        loop {
            if let Some(Ok(message)) = reader.next().await {
                let text = message.to_text().unwrap();
                let recv: WsRecv = serde_json::from_str(text).unwrap();
                assert_eq!(recv.code, 0);
                if let WsRecvData::Version { version, .. } = recv.data {
                    tracing::info!("server version: {version}");
                    break;
                }
            }
        }

        Ok(())
    }
}
