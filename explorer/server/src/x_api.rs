use actix_web::{HttpRequest, HttpResponse, ResponseError, body::BoxBody, web::Json};
use anyhow::{Context, bail};
use arrow::array::RecordBatch;
use arrow_flight::error::FlightError;
use ha_rpc_client::client::HaRpcClient;
use taos::{Code, Dsn};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};
use url::Url;

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
        self.0.fmt(f)
    }
}

impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        Self(err)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.source()
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
    via: Option<i64>,
    cancel: CancellationToken,
) -> Result<Option<HaRpcClient>> {
    get_client(None, dsn, via, None, cancel).await
}

pub async fn get_client(
    xnode_id: Option<i32>,
    dsn: &Dsn,
    via: Option<i64>,
    message_tx: Option<flume::Sender<FlightResult>>,
    cancel: CancellationToken,
) -> Result<Option<HaRpcClient>> {
    let xnodes = query::<Xnode>(dsn, "SHOW XNODES WHERE STATUS = 'online'").await?;
    for Xnode { id, url, .. } in &xnodes {
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
        let cancel = cancel.child_token();
        tokio::spawn({
            let cancel = cancel.clone();
            let xnode_id = *id;
            let message_tx = message_tx.clone();
            async move {
                while let Some(Ok(event)) = cancel.run_until_cancelled(event_rx.recv_async()).await
                {
                    match &message_tx {
                        Some(tx) => {
                            if tx.send_async(event).await.is_err() {
                                break;
                            }
                        }
                        None => {
                            if let Err(e) = event {
                                tracing::error!(
                                    xnode_id,
                                    "Failed to receive event from xnode: {e}"
                                );
                            }
                        }
                    }
                }
            }
        });
        let (client, via) =
            match ha_rpc_client::create_guest(channel, event_tx, cancel.clone()).await {
                Ok(client) => {
                    tracing::info!(xnode_id = id, "Created guest client for xnode");
                    match via {
                        Some(via) => (client, via),
                        None => return Ok(Some(client)),
                    }
                }
                Err(e) => {
                    cancel.cancel();
                    tracing::error!(
                        "Failed to create guest for xnode {}: {:#}",
                        id,
                        anyhow::Error::new(e)
                    );
                    continue;
                }
            };
        match client.list_agents().await {
            Ok(agents) => {
                if agents
                    .iter()
                    .any(|v| v.id == via && v.status.is_connected())
                {
                    return Ok(Some(client));
                }
            }
            Err(e) => {
                cancel.cancel();
                tracing::error!(
                    "Failed to list agents for xnode {}: {:#}",
                    id,
                    anyhow::Error::new(e)
                );
                continue;
            }
        }
    }
    Ok(None)
}

pub async fn get_x_url(args: &Args, req: &HttpRequest, api: &str) -> Result<Option<String>> {
    let dsn = get_dsn(args, req).await?;
    let cancel = CancellationToken::new();
    let _guard = cancel.drop_guard_ref();
    let client = get_one_client(&dsn, None, cancel.clone())
        .await?
        .context("no available xnode found")?;

    let mut ports = client
        .get_x_http_port()
        .await
        .context("Failed to get x http port")?
        .context("x http port not set")?;
    let port = ports.pop().context("x http port not set")?;

    let mut xnodes = query::<Xnode>(&dsn, "SHOW XNODES WHERE STATUS = 'online'")
        .await
        .context("show xnodes error")?;
    if let Some(xnode) = xnodes.pop() {
        let url = if xnode.url.starts_with("http") {
            xnode.url.to_string()
        } else {
            format!("http://{}", xnode.url)
        };
        let mut url = Url::parse(&url).context("x api not invalid url")?;
        url.set_port(Some(port))
            .map_err(|_| anyhow::anyhow!("set x url port error"))?;
        return Ok(Some(format!("{}{api}?{}", url, req.query_string())));
    }
    Ok(None)
}

pub async fn x_addrs(args: &Args, req: &HttpRequest) -> Result<Vec<String>> {
    let dsn = get_dsn(args, req).await?;
    let xnodes = query::<Xnode>(&dsn, "SHOW XNODES")
        .await
        .context("show xnodes error")?
        .into_iter()
        .map(|v| {
            let url = v.url;
            if url.starts_with("http") {
                url.to_string()
            } else {
                format!("http://{url}")
            }
        })
        .collect();
    Ok(xnodes)
}
