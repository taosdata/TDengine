use actix_web::{HttpRequest, HttpResponse, ResponseError, body::BoxBody, web::Json};
use anyhow::bail;
use arrow::array::RecordBatch;
use arrow_flight::error::FlightError;
use ha_rpc_client::client::HaRpcClient;
use taos::{Code, Dsn};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

use crate::{Args, oauth, sql::query, x_api::types::Xnode};

pub mod agent;
pub mod datasource;
pub mod proxy;
pub mod tasks;
pub mod transform;
mod types;
pub mod ws;

#[derive(Debug, serde::Serialize)]
pub struct Fail {
    pub code: Code,
    pub message: String,
}

impl From<taos::Error> for Fail {
    fn from(err: taos::Error) -> Self {
        Self {
            code: err.code(),
            message: err.message().to_string(),
        }
    }
}

impl Fail {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            code: Code::FAILED,
            message: message.into(),
        }
    }
}

type Result<T> = std::result::Result<T, Error>;
type JsonResult<T> = std::result::Result<Json<T>, Error>;
type FlightResult = std::result::Result<RecordBatch, FlightError>;

#[derive(Debug)]
pub struct Error(anyhow::Error);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:#}", self.0)
    }
}

impl From<anyhow::Error> for Error {
    fn from(value: anyhow::Error) -> Self {
        Self(value)
    }
}

impl ResponseError for Error {
    fn status_code(&self) -> http::StatusCode {
        http::StatusCode::INTERNAL_SERVER_ERROR
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        HttpResponse::InternalServerError().json(Fail::new(format!("{self:#}")))
    }
}

pub(crate) async fn get_dsn(args: &Args, req: &HttpRequest) -> anyhow::Result<Dsn> {
    let Some(auth) = oauth::middleware::extract_auth_from_request(req)
        .await
        .map_err(|e| anyhow::anyhow!("extract auth error: {e}"))?
    else {
        bail!("auth info not found");
    };

    args.build_dsn(&auth)
        .map_err(|e| anyhow::anyhow!("build dsn error: {e}"))
}

pub async fn get_one_client(
    dsn: &Dsn,
    cancel: CancellationToken,
) -> Result<Option<(HaRpcClient, flume::Receiver<FlightResult>)>> {
    get_client(None, dsn, cancel).await
}

pub async fn get_client(
    xnode_id: Option<i32>,
    dsn: &Dsn,
    cancel: CancellationToken,
) -> Result<Option<(HaRpcClient, flume::Receiver<FlightResult>)>> {
    let xnodes = query::<Xnode>(dsn, "SHOW XNODES").await?;
    for Xnode { id, url, status } in &xnodes {
        if status != "online" {
            continue;
        }
        if xnode_id.is_some_and(|v| v != *id) {
            continue;
        }
        let addr = if url.starts_with("http") {
            url.to_string()
        } else {
            format!("http://{url}")
        };
        let endpoint: Endpoint = match Channel::from_shared(addr) {
            Ok(endpoint) => endpoint,
            Err(e) => {
                tracing::error!("Failed to create endpoint for xnode {}: {}", id, e);
                continue;
            }
        };
        let channel = match endpoint.connect().await {
            Ok(channel) => channel,
            Err(e) => {
                tracing::error!("Failed to connect to xnode {}: {}", id, e);
                continue;
            }
        };
        let (event_tx, event_rx) = flume::bounded(1);
        match ha_rpc_client::create_guest(channel, event_tx, cancel.clone()).await {
            Ok(client) => return Ok(Some((client, event_rx))),
            Err(e) => {
                tracing::error!(
                    "Failed to create guest for xnode {}: {:#}",
                    id,
                    anyhow::Error::new(e)
                );
            }
        }
    }
    Ok(None)
}
