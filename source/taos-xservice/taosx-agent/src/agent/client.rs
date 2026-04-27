use std::{
    collections::HashMap, net::SocketAddr, ops::RangeInclusive, pin::Pin, str::FromStr, sync::Arc,
    task::Poll, time::Duration,
};

use arrow::{
    array::{ArrayRef, RecordBatch, StringArray, TimestampMillisecondArray, UInt64Array},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
};
use arrow_flight::{
    Action as FlightAction, FlightClient, encode::FlightDataEncoderBuilder, error::FlightError,
    flight_service_client::FlightServiceClient,
};
use chrono::Utc;
use futures::{StreamExt, TryStreamExt};
use ha_core::{activity::Activity, consts::*, types::HaTask, utils::next_req_id};
use hyper::Uri;
use hyper_util::rt::TokioIo;
use snafu::{OptionExt, ResultExt};
use taos::Dsn;
use taosx_core::{
    CheckResponse, DataSetsReq, Fail, HeartbeatResponse, ListResponse, PutFileReq,
    QueryDataSourceReq, QueryDataSourceResp, RespAction, Response, SampleResponse,
    SplitTaskResponse, list_datasets_from,
};
use taosx_task::{sample::get_sample, split_job::plan_task, validate::validate_dsn};
use tokio::net::{TcpSocket, TcpStream, lookup_host};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint};
use tower::{BoxError, Service};
use tracing::instrument;

use crate::{
    agent::{Task, do_put_file},
    runner::Action,
};

#[derive(Debug, snafu::Snafu)]
pub enum Error {
    #[snafu(display("Failed to build TLS configuration"))]
    BuildTlsConfig { source: tonic::transport::Error },
    #[snafu(display("Failed to connect to channel: {endpoint}"))]
    ChannelConnect {
        endpoint: String,
        source: tonic::transport::Error,
    },
    #[snafu(display("Failed to handshake with server"))]
    Handshake {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(display("Failed to parse handshake response"))]
    InvalidHandshakeResp { source: serde_json::Error },
    #[snafu(display("Failed to parse uri: {uri}"))]
    InvalidUri { uri: Uri },
    #[snafu(display("Failed to connect socket"))]
    SocketConnect { source: std::io::Error },
    #[snafu(display("Do action error"))]
    DoAction {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(display("Failed to add header"))]
    AddHeader {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(display("Failed to parse endpoint: {endpoint}"))]
    InvalidEndpoint {
        endpoint: String,
        source: tonic::transport::Error,
    },
    #[snafu(display("Failed to serialize activity"))]
    SerializeActivity { source: serde_json::Error },
    #[snafu(display("Collect action response error"))]
    CollectActionResp {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(transparent)]
    JsonToDsn { source: anyhow::Error },
    #[snafu(display("Do exchange error"))]
    DoExchange {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(display("Fetch exchange item error"))]
    FetchExchangeItem {
        source: arrow_flight::error::FlightError,
    },
    #[snafu(display("Invalid payload for action: {action}"))]
    InvalidActionPayload {
        action: String,
        source: serde_json::Error,
    },
    #[snafu(display("Invalid DSN: {dsn}"))]
    InvalidDsn { dsn: String, source: taos::DsnError },
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub struct Client {
    pub client: FlightClient,
    pub agent_id: i64,
    endpoint: String,
}

/// Builds the base endpoint configuration without TLS.
///
/// Used for HTTP connections and as the base for the insecure HTTPS connector.
fn build_base_endpoint(endpoint: &str, tcp_keepalive: Option<Duration>) -> Result<Endpoint> {
    Endpoint::try_from(endpoint.to_string())
        .context(InvalidEndpointSnafu { endpoint })
        .map(|ep| {
            ep.keep_alive_while_idle(true)
                .tcp_keepalive(tcp_keepalive)
                .http2_keep_alive_interval(Duration::from_secs(13))
                .keep_alive_timeout(Duration::from_secs(120))
        })
}

fn build_insecure_connector_endpoint(
    endpoint: &str,
    tcp_keepalive: Option<Duration>,
) -> Result<Endpoint> {
    let origin = Endpoint::try_from(endpoint.to_string())
        .context(InvalidEndpointSnafu { endpoint })?
        .uri()
        .clone();
    let endpoint = endpoint.replacen("https://", "http://", 1);
    build_base_endpoint(&endpoint, tcp_keepalive).map(|ep| ep.origin(origin))
}

fn build_endpoint(
    endpoint: &str,
    ca: Option<Certificate>,
    tcp_keepalive: Option<Duration>,
    is_https: bool,
) -> Result<Endpoint> {
    let mut builder = build_base_endpoint(endpoint, tcp_keepalive)?;
    if is_https && let Some(ca) = ca {
        builder = builder
            .tls_config(
                ClientTlsConfig::new()
                    .with_native_roots()
                    .ca_certificate(ca),
            )
            .context(BuildTlsConfigSnafu)?;

        // For HTTPS without CA, the insecure connector is used at connect time;
        // no tls_config is set on the Endpoint.
    }
    Ok(builder)
}

async fn new_channel(
    endpoint: String,
    ports: Option<RangeInclusive<u16>>,
    ca: Option<Certificate>,
) -> Result<Channel> {
    cfg_if::cfg_if! {
        if #[cfg(windows)] {
           let tcp_keepalive = None;
        } else {
           let tcp_keepalive = Some(Duration::from_secs(5));
        }
    };

    let is_https = endpoint.starts_with("https://");
    let is_insecure = is_https && ca.is_none();

    if is_insecure {
        // HTTPS without CA: use a custom TLS connector that skips cert verification.
        //
        // The internal tonic endpoint uses `http://` so tonic doesn't reject or double-wrap
        // the connection; the custom connector below performs the actual TLS handshake.
        let ep = build_insecure_connector_endpoint(&endpoint, tcp_keepalive)?;
        return ep
            .connect_with_connector(InsecureGrpcConnector::new_with_ports(ports))
            .await
            .context(ChannelConnectSnafu { endpoint });
    }

    let endpoint_builder = build_endpoint(&endpoint, ca, tcp_keepalive, is_https)?;
    match ports {
        Some(ports) => {
            endpoint_builder
                .connect_with_connector(Svc::new(ports))
                .await
        }
        None => endpoint_builder.connect().await,
    }
    .context(ChannelConnectSnafu { endpoint })
}

struct Svc {
    ports: RangeInclusive<u16>,
}

impl Svc {
    fn new(ports: RangeInclusive<u16>) -> Self {
        Self { ports }
    }
}

impl Service<Uri> for Svc {
    type Response = TokioIo<TcpStream>;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        match TcpSocket::new_v4() {
            Ok(socket) => {
                let mut bound = false;
                for port in self.ports.clone() {
                    if let Err(err) = socket.bind(format!("0.0.0.0:{port}").parse().unwrap()) {
                        match err.kind() {
                            std::io::ErrorKind::AddrInUse => {
                                continue;
                            }
                            _ => {
                                tracing::error!("{err:#}");
                                return Box::pin(async move { Err(Box::new(err) as _) });
                            }
                        }
                    } else {
                        bound = true;
                        break;
                    };
                }

                if !bound {
                    return Box::pin(async {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::AddrInUse,
                            "all configured local ports are in use",
                        )
                        .into())
                    });
                }

                Box::pin(async move {
                    Ok(TokioIo::new({
                        let addr = parse_addr(&uri).context(InvalidUriSnafu { uri })?;
                        socket.connect(addr).await.context(SocketConnectSnafu)?
                    }))
                })
            }
            Err(err) => Box::pin(async move { Err(Box::new(err) as _) }),
        }
    }
}

fn parse_addr(uri: &Uri) -> Option<SocketAddr> {
    let host = uri.host()?;
    let port = uri.port_u16()?;
    let addr = format!("{}:{}", host, port);
    addr.parse().ok()
}

/// A tower [`Service`] that establishes a TLS connection without verifying the peer certificate.
///
/// Optionally binds to a local port range before connecting, mirroring the behavior of [`Svc`].
/// Used when HTTPS is requested but no CA is configured.
#[derive(Clone)]
struct InsecureGrpcConnector {
    tls: tokio_rustls::TlsConnector,
    ports: Option<RangeInclusive<u16>>,
}

impl std::fmt::Debug for InsecureGrpcConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsecureGrpcConnector")
            .finish_non_exhaustive()
    }
}

impl InsecureGrpcConnector {
    fn new_with_ports(ports: Option<RangeInclusive<u16>>) -> Self {
        let mut config = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerifyCertVerifier))
            .with_no_client_auth();
        // Advertise HTTP/2 so tonic negotiates the correct protocol.
        config.alpn_protocols = vec![b"h2".to_vec()];
        Self {
            tls: tokio_rustls::TlsConnector::from(Arc::new(config)),
            ports,
        }
    }
}

impl Service<Uri> for InsecureGrpcConnector {
    type Response = TokioIo<tokio_rustls::client::TlsStream<TcpStream>>;
    type Error = BoxError;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, uri: Uri) -> Self::Future {
        let tls = self.tls.clone();
        let ports = self.ports.clone();
        Box::pin(async move {
            let host = uri.host().ok_or("missing host in uri")?.to_string();
            let port = uri.port_u16().unwrap_or(443);
            let domain = tokio_rustls::rustls::pki_types::ServerName::try_from(host.clone())
                .map_err(|e| format!("invalid server name '{host}': {e}"))?
                .to_owned();
            let tcp = if let Some(ports) = ports {
                let socket = TcpSocket::new_v4()?;
                let mut bound = false;
                for p in ports {
                    match socket.bind(format!("0.0.0.0:{p}").parse()?) {
                        Ok(()) => {
                            bound = true;
                            break;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => continue,
                        Err(e) => return Err(Box::new(e) as BoxError),
                    }
                }
                if !bound {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::AddrInUse,
                        "all configured local ports are in use",
                    )
                    .into());
                }
                let target = lookup_host((host.as_str(), port))
                    .await?
                    .find(|addr| addr.is_ipv4())
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::AddrNotAvailable,
                            format!("no IPv4 address resolved for {host}:{port}"),
                        )
                    })?;
                socket.connect(target).await?
            } else {
                TcpStream::connect((host.as_str(), port)).await?
            };
            let tls_stream = tls.connect(domain, tcp).await?;
            Ok(TokioIo::new(tls_stream))
        })
    }
}

/// A rustls certificate verifier that accepts any server certificate.
///
/// Used when HTTPS is requested but no CA is configured. The connection is still
/// encrypted; only peer certificate verification is skipped.
#[derive(Debug)]
struct NoVerifyCertVerifier;

impl rustls::client::danger::ServerCertVerifier for NoVerifyCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

impl Client {
    pub async fn new(
        endpoint: &str,
        token: &str,
        ca: Option<Certificate>,
        ports: &Option<RangeInclusive<u16>>,
    ) -> Result<Self> {
        let endpoint = endpoint.to_string();
        let token = token.to_string();
        let is_https = endpoint.starts_with("https://");
        let verify_mode = if !is_https {
            "none"
        } else if ca.is_some() {
            "config"
        } else {
            "insecure"
        };
        tracing::debug!(
            endpoint = %endpoint,
            transport = if is_https { "https" } else { "http" },
            verify_mode,
            "connecting to taosx server"
        );

        let channel = new_channel(endpoint.clone(), ports.clone(), ca).await?;

        tracing::info!(
            endpoint = %endpoint,
            transport = if is_https { "https" } else { "http" },
            verify_mode,
            "connected to taosx server"
        );
        let inner = FlightServiceClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        let mut client = FlightClient::new_from_inner(inner);
        client
            .add_header("x-token", &token)
            .context(AddHeaderSnafu)?;
        client
            .add_header("x-version", crate::build::PKG_VERSION)
            .context(AddHeaderSnafu)?;
        let result = client
            .handshake(token.to_string())
            .await
            .context(HandshakeSnafu)?;
        let agent: i64 = serde_json::from_slice(&result).context(InvalidHandshakeRespSnafu)?;

        Ok(Self {
            client,
            agent_id: agent,
            endpoint,
        })
    }

    pub fn agent_id(&self) -> i64 {
        self.agent_id
    }
    pub async fn push_status(&mut self, status: &Activity) -> Result<()> {
        let status_bytes = serde_json::to_vec(status).context(SerializeActivitySnafu)?;
        let action = FlightAction::new(ACTION_TASK_STATUS, status_bytes);
        let _resp: Vec<_> = self
            .client
            .do_action(action)
            .await
            .context(DoActionSnafu)?
            .try_collect()
            .await
            .context(CollectActionRespSnafu)?;
        Ok(())
    }

    pub async fn get_taosx_monitor_config(&mut self) -> Option<HashMap<String, String>> {
        tracing::info!("Get monitor config from server");
        let action = FlightAction::new(ACTION_GET_MONITOR_CONFIG, "GetMonitorConfig");
        let result = match self.client.do_action(action).await {
            Ok(res) => res,
            Err(e) => {
                tracing::error!("Can't get monitor config from server: {e}");
                return None;
            }
        };
        let result: std::result::Result<Vec<bytes::Bytes>, arrow_flight::error::FlightError> =
            result.try_collect().await;
        if let Err(err) = result {
            tracing::error!("Can't get monitor config from server: {err:#}");
            return None;
        }
        let resp = result.unwrap();
        if resp.is_empty() {
            tracing::error!("Can't get monitor config from server");
            return None;
        }
        let config = serde_json::from_slice::<HashMap<String, String>>(&resp[0]);
        if let Err(err) = config {
            tracing::error!("Can't deserialize response data: {err:#}");
            return None;
        }
        let config = config.unwrap();
        tracing::info!("Got config from server: {:?}", &config);
        Some(config)
    }

    #[instrument(skip_all)]
    pub async fn process_actions(
        mut self,
        sender: flume::Sender<Action>,
        resp_tx: flume::Sender<RespAction>,
        resp_rx: flume::Receiver<RespAction>,
    ) -> Result<()> {
        tracing::info!("Wait tasks from server");
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("action", DataType::Utf8, false),
            Field::new("context", DataType::Utf8, true),
            Field::new("req_id", DataType::UInt64, false),
        ]));

        let req = FlightDataEncoderBuilder::new().with_schema(schema).build(
            resp_rx
                .into_stream()
                .map(|action| resp_action_to_arrow(action).map_err(FlightError::Arrow)),
        );
        let mut stream = self
            .client
            .do_exchange(req)
            .await
            .context(DoExchangeSnafu)?;
        let mut max_server_delay = None;
        while let Some(res) = stream.try_next().await.context(FetchExchangeItemSnafu)? {
            let rows = res.num_rows();
            let ts = res
                .column(0)
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let action = res
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let context = res
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let req_id = res
                .column(3)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            for _ in 0..rows {
                let (ts, action, context, req_id) = (
                    ts.value_as_datetime_with_tz(
                        0,
                        ts.timezone().unwrap_or("UTC").parse().unwrap(),
                    )
                    .unwrap(),
                    action.value(0),
                    context.value(0),
                    req_id.value(0),
                );

                #[derive(serde::Deserialize)]
                struct TaskWithId {
                    id: i64,
                    job_id: i64,
                }
                match action {
                    ACTION_RUN => {
                        let task: Task = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        tracing::info!(task_id = task.id, job_id = task.job_id, "Start task");
                        if sender.send_async(Action::Run(task)).await.is_err() {
                            return Ok(());
                        }
                    }
                    ACTION_STOP => {
                        let task: TaskWithId = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        tracing::info!(task_id = task.id, job_id = task.job_id, "Stop task");
                        if sender
                            .send_async(Action::Stop(task.id, task.job_id))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    ACTION_CANCEL => {
                        let task: TaskWithId = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        tracing::info!(task_id = task.id, job_id = task.job_id, "Cancel task");
                        if sender
                            .send_async(Action::Cancel(task.id, task.job_id))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    ACTION_LIST_DATA_SETS => {
                        tracing::info!("List datasets request received");
                        let req: DataSetsReq = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        let resp_tx = resp_tx.clone();

                        tokio::spawn(async move {
                            let sets = list_datasets_from(&req).await.map_err(Fail::new);
                            resp_tx
                                .send_async(RespAction::ListOk(ListResponse {
                                    req_id,
                                    req,
                                    res: sets,
                                }))
                                .await
                                .ok();
                        });
                    }
                    ACTION_CHECK => {
                        tracing::info!("Check data source request received");

                        let dsn =
                            Dsn::from_str(context).context(InvalidDsnSnafu { dsn: context })?;
                        let resp_tx = resp_tx.clone();

                        tokio::spawn(async move {
                            let dsv = validate_dsn(&dsn).await;
                            resp_tx
                                .send_async(RespAction::CheckOk(CheckResponse {
                                    req_id,
                                    req: dsn.to_string(),
                                    res: dsv,
                                }))
                                .await
                                .ok();
                        });
                    }
                    ACTION_GET_SAMPLE => {
                        tracing::info!("Sample data source request received");
                        let dsn =
                            Dsn::from_str(context).context(InvalidDsnSnafu { dsn: context })?;
                        let resp_tx = resp_tx.clone();
                        tokio::spawn(async move {
                            let sample = get_sample(&dsn).await;
                            let res = match sample {
                                Ok(sample) => match serde_json::to_string(&sample) {
                                    Ok(s) => Response::Ok(s),
                                    Err(err) => Response::Err(Fail::new(anyhow::anyhow!(
                                        "failed to serialize sample data, cause: {}",
                                        err
                                    ))),
                                },
                                Err(err) => Response::Err(Fail::new(err)),
                            };

                            resp_tx
                                .send_async(RespAction::SampleOk(SampleResponse {
                                    req_id,
                                    req: dsn.to_string(),
                                    res,
                                }))
                                .await
                                .ok();
                        });
                    }
                    ACTION_SPLIT_TASK => {
                        tracing::info!("Split task request received");
                        let task: HaTask = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        let resp_tx = resp_tx.clone();
                        tokio::spawn(async move {
                            let tasks = plan_task(task.clone()).await;
                            let res = match tasks {
                                Ok(res) => Response::Ok(res),
                                Err(err) => Response::Err(Fail::new(err)),
                            };

                            resp_tx
                                .send_async(RespAction::SplitTaskOk(SplitTaskResponse {
                                    req_id,
                                    req: task,
                                    res,
                                }))
                                .await
                                .ok();
                        });
                    }
                    ACTION_PUT_FILE => {
                        let req: PutFileReq = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        let resp_tx = resp_tx.clone();
                        tracing::info!("Put file request received");
                        tokio::spawn(do_put_file(req, req_id, resp_tx));
                    }
                    ACTION_QUERY_DATA_SOURCE => {
                        tracing::info!(?req_id, "[query-data-source]: {}", &context);
                        let req: QueryDataSourceReq = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        let resp_tx = resp_tx.clone();
                        tracing::info!("Query data source request received");
                        tokio::spawn(async move {
                            let result = taosx_core::plugins::query_data_source(req).await;
                            match result {
                                Ok(output) => {
                                    tracing::info!(?req_id, "Query data source ok");
                                    let _ = resp_tx
                                        .send_async(RespAction::QueryDataSourceOk(
                                            QueryDataSourceResp {
                                                req_id,
                                                output: Response::Ok(output),
                                            },
                                        ))
                                        .await;
                                }
                                Err(err) => {
                                    tracing::error!(?req_id, "Query data source error: {err:#}");
                                    let _ = resp_tx
                                        .send_async(RespAction::QueryDataSourceOk(
                                            QueryDataSourceResp {
                                                req_id,
                                                output: Response::Err(Fail::new(err)),
                                            },
                                        ))
                                        .await;
                                }
                            }
                        });
                    }
                    MESSAGE_HEARTBEAT => {
                        let resp = HeartbeatResponse {
                            req: ts.naive_utc().and_utc(),
                            res: Utc::now(),
                        };
                        if resp_tx
                            .send_async(RespAction::HeartbeatOk(req_id, resp))
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }
                    MESSAGE_HEARTBEAT_OK => {
                        let resp: HeartbeatResponse = serde_json::from_str(context)
                            .context(InvalidActionPayloadSnafu { action })?;
                        let delay = resp.duration().num_milliseconds();
                        if Some(delay) > max_server_delay {
                            max_server_delay = Some(delay);
                            tracing::info!(
                                endpoint = self.endpoint,
                                "Server is alive, delay: {}ms",
                                resp.duration().num_milliseconds()
                            );
                        }
                    }
                    ACTION_EXIT => {
                        tracing::info!("Received exit command");
                        if sender.send_async(Action::Exit).await.is_err() {
                            return Ok(());
                        }
                    }
                    action => {
                        tracing::error!("Unknown action {action}");
                    }
                }
            }
        }

        Ok(())
    }
}

fn resp_action_to_arrow(action: RespAction) -> std::result::Result<RecordBatch, ArrowError> {
    match action {
        RespAction::Heartbeat(req_id) => {
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let context: ArrayRef = Arc::new(StringArray::from_iter([Option::<String>::None]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([MESSAGE_HEARTBEAT]));
            tracing::debug!("Send heartbeat request: {req_id}");
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::HeartbeatOk(req_id, resp) => {
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let context =
                serde_json::to_string(&resp).map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([MESSAGE_HEARTBEAT_OK]));
            tracing::debug!("Send heartbeat response: {req_id}");
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::TaskError(_) => unreachable!(),
        RespAction::ListOk(sets) => {
            let req_id = sets.req_id;
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let context =
                serde_json::to_string(&sets).map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([ACTION_LIST_DATA_SETS]));
            let req_id_arr: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));
            let item = RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id_arr),
            ]);

            tracing::debug!(
                "RespAction::ListOk, result len: {}, req_id: {}",
                sets.res.as_ref().map_or(0, |r| r.len()),
                req_id
            );
            item
        }
        RespAction::CheckOk(response) => {
            let req_id = response.req_id;
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let context = serde_json::to_string(&response)
                .map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([ACTION_CHECK]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::SampleOk(resp) => {
            let req_id = resp.req_id;
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([ACTION_GET_SAMPLE]));
            let context =
                serde_json::to_string(&resp).map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::SplitTaskOk(resp) => {
            let req_id = resp.req_id;
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([ACTION_SPLIT_TASK]));
            let context =
                serde_json::to_string(&resp).map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::PutFileOk(resp) => {
            let req_id = resp.req_id;
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([ACTION_PUT_FILE]));
            let context =
                serde_json::to_string(&resp).map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::QueryDataSourceOk(resp) => {
            let req_id = resp.req_id;
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let action: ArrayRef =
                Arc::new(StringArray::from_iter_values([ACTION_QUERY_DATA_SOURCE]));
            let context =
                serde_json::to_string(&resp).map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([req_id]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::AgentActivity(activity) => {
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let context = serde_json::to_string(&activity)
                .map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let action: ArrayRef =
                Arc::new(StringArray::from_iter_values([MESSAGE_AGENT_ACTIVITY]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([next_req_id()]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::TaskActivity(activity) => {
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let context = serde_json::to_string(&activity)
                .map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([MESSAGE_TASK_ACTIVITY]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([next_req_id()]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::TaskMetrics(metrics) => {
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let action: ArrayRef = Arc::new(StringArray::from_iter_values([MESSAGE_TASK_METRICS]));
            let context = serde_json::to_string(&metrics)
                .map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([next_req_id()]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
        RespAction::Metrics(metrics_event) => {
            let val = Arc::new(TimestampMillisecondArray::from_iter_values([
                Utc::now().timestamp_millis()
            ])) as ArrayRef;
            let action: ArrayRef =
                Arc::new(StringArray::from_iter_values([MESSAGE_METRICS_EVENTS]));
            let context = serde_json::to_string(&metrics_event)
                .map_err(|e| ArrowError::JsonError(e.to_string()))?;
            let context: ArrayRef = Arc::new(StringArray::from_iter_values([context]));
            let req_id: ArrayRef = Arc::new(UInt64Array::from_iter_values([next_req_id()]));

            RecordBatch::try_from_iter(vec![
                ("ts", val),
                ("action", action),
                ("context", context),
                ("req_id", req_id),
            ])
        }
    }
}

#[cfg(test)]
mod agent_tests {
    use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
    use arrow_flight::{
        Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
        HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    };
    use futures::stream::BoxStream;
    use hyper::http;
    use std::net::SocketAddr;
    use std::process::{self, Command};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::sync::oneshot;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::async_trait;
    use tonic::service::LayerExt;
    use tonic::transport::{Endpoint, Identity, Server, ServerTlsConfig};
    use tonic::{Request, Response, Status, Streaming};
    use tower::Layer;
    use tower::Service;

    use super::*;

    static CONNECTOR_WAS_USED: AtomicBool = AtomicBool::new(false);

    #[ignore]
    #[tokio::test]
    async fn test_new_channel() {
        let endpoint = String::from("0.0.0.0:6030");
        let ports = Some(9000..=9099);
        let _channel = new_channel(endpoint, ports, None).await.unwrap();

        // get the current process ID
        let pid = process::id();

        // get listening ports by netstat
        let output = Command::new("netstat")
            .arg("-anp")
            .output()
            .expect("Failed to execute command");
        let output_str = String::from_utf8_lossy(&output.stdout);
        let filtered_output: Vec<&str> = output_str
            .lines()
            .filter(|line| line.contains(&pid.to_string()))
            .collect();
        println!("Listening ports:\n{:?}", filtered_output);
    }

    #[test]
    fn build_endpoint_http_has_no_tls() {
        let ep =
            build_endpoint("http://localhost:6030", None, None, false).expect("build endpoint");
        assert_eq!(ep.uri().scheme_str(), Some("http"));
    }

    #[test]
    fn build_endpoint_https_without_ca_preserves_scheme() {
        let ep =
            build_endpoint("https://localhost:6030", None, None, true).expect("build endpoint");
        assert_eq!(ep.uri().scheme_str(), Some("https"));
    }

    #[test]
    fn build_endpoint_https_with_ca_preserves_scheme() {
        let ca_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/tls/ca.pem");
        if !ca_path.exists() {
            // Skip when the test CA is not present in this worktree.
            return;
        }
        let pem = std::fs::read(&ca_path).expect("read ca.pem");
        let cert = Certificate::from_pem(pem);
        let ep = build_endpoint("https://localhost:6030", Some(cert), None, true)
            .expect("build endpoint");
        assert_eq!(ep.uri().scheme_str(), Some("https"));
    }

    #[test]
    fn endpoint_uri_is_preserved_for_bare_http() {
        let raw = "http://taosx-host:6030";
        let ep = Endpoint::try_from(raw.to_string()).expect("parse endpoint");
        assert_eq!(ep.uri().scheme_str(), Some("http"));
    }

    #[derive(Clone)]
    struct FailConnector;

    impl Service<Uri> for FailConnector {
        type Response = TokioIo<TcpStream>;
        type Error = BoxError;
        type Future =
            Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _uri: Uri) -> Self::Future {
            Box::pin(async {
                CONNECTOR_WAS_USED.store(true, Ordering::SeqCst);
                Err(std::io::Error::other("custom connector reached").into())
            })
        }
    }

    #[tokio::test]
    async fn https_endpoint_without_ca_reaches_custom_connector() {
        CONNECTOR_WAS_USED.store(false, Ordering::SeqCst);
        let endpoint = build_insecure_connector_endpoint("https://localhost:6030", None)
            .expect("build endpoint");
        let _err = endpoint
            .connect_with_connector(FailConnector)
            .await
            .expect_err("custom connector should fail");
        assert!(
            CONNECTOR_WAS_USED.load(Ordering::SeqCst),
            "endpoint rejected https before the custom connector ran"
        );
    }

    #[derive(Debug, Default)]
    struct TestFlightService;

    #[async_trait]
    impl FlightService for TestFlightService {
        type HandshakeStream = BoxStream<'static, std::result::Result<HandshakeResponse, Status>>;
        type ListFlightsStream = BoxStream<'static, std::result::Result<FlightInfo, Status>>;
        type DoGetStream = BoxStream<'static, std::result::Result<FlightData, Status>>;
        type DoPutStream = BoxStream<'static, std::result::Result<PutResult, Status>>;
        type DoExchangeStream = BoxStream<'static, std::result::Result<FlightData, Status>>;
        type DoActionStream = BoxStream<'static, std::result::Result<arrow_flight::Result, Status>>;
        type ListActionsStream = BoxStream<'static, std::result::Result<ActionType, Status>>;

        async fn handshake(
            &self,
            mut request: Request<Streaming<HandshakeRequest>>,
        ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
            let Some(req) = request.get_mut().message().await? else {
                return Err(Status::aborted("missing handshake payload"));
            };
            let response = HandshakeResponse {
                protocol_version: req.protocol_version,
                payload: req.payload,
            };
            Ok(Response::new(Box::pin(futures::stream::once(async move {
                Ok(response)
            }))))
        }

        async fn list_flights(
            &self,
            _request: Request<Criteria>,
        ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
            Err(Status::unimplemented("list_flights"))
        }

        async fn get_flight_info(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> std::result::Result<Response<FlightInfo>, Status> {
            Err(Status::unimplemented("get_flight_info"))
        }

        async fn poll_flight_info(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> std::result::Result<Response<PollInfo>, Status> {
            Err(Status::unimplemented("poll_flight_info"))
        }

        async fn get_schema(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> std::result::Result<Response<SchemaResult>, Status> {
            Err(Status::unimplemented("get_schema"))
        }

        async fn do_get(
            &self,
            _request: Request<Ticket>,
        ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
            Err(Status::unimplemented("do_get"))
        }

        async fn do_put(
            &self,
            _request: Request<Streaming<FlightData>>,
        ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
            Err(Status::unimplemented("do_put"))
        }

        async fn do_exchange(
            &self,
            _request: Request<Streaming<FlightData>>,
        ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
            Err(Status::unimplemented("do_exchange"))
        }

        async fn do_action(
            &self,
            _request: Request<Action>,
        ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
            Err(Status::unimplemented("do_action"))
        }

        async fn list_actions(
            &self,
            _request: Request<Empty>,
        ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
            Err(Status::unimplemented("list_actions"))
        }
    }

    #[derive(Clone)]
    struct CaptureScheme<S> {
        inner: S,
        request_scheme: Arc<Mutex<Option<String>>>,
    }

    #[derive(Clone)]
    struct CaptureSchemeLayer {
        request_scheme: Arc<Mutex<Option<String>>>,
    }

    impl<S> Layer<S> for CaptureSchemeLayer {
        type Service = CaptureScheme<S>;

        fn layer(&self, inner: S) -> Self::Service {
            CaptureScheme {
                inner,
                request_scheme: self.request_scheme.clone(),
            }
        }
    }

    impl<S, ReqBody> Service<http::Request<ReqBody>> for CaptureScheme<S>
    where
        S: Service<http::Request<ReqBody>> + Clone,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = S::Future;

        fn poll_ready(
            &mut self,
            cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, req: http::Request<ReqBody>) -> Self::Future {
            *self.request_scheme.lock().expect("lock request scheme") =
                req.uri().scheme_str().map(str::to_string);
            self.inner.call(req)
        }
    }

    async fn start_test_tls_flight_server(
        request_scheme: Arc<Mutex<Option<String>>>,
    ) -> (SocketAddr, oneshot::Sender<()>) {
        let cert_path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/tls/server.pem");
        let key_path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../tests/tls/server.key");
        let cert = std::fs::read_to_string(cert_path).expect("read server cert");
        let key = std::fs::read_to_string(key_path).expect("read server key");
        let identity = Identity::from_pem(cert, key);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind tls test listener");
        let addr = listener.local_addr().expect("listener local addr");
        let incoming = TcpListenerStream::new(listener);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let service = CaptureSchemeLayer {
            request_scheme: request_scheme.clone(),
        }
        .named_layer(FlightServiceServer::new(TestFlightService));

        tokio::spawn(async move {
            Server::builder()
                .tls_config(ServerTlsConfig::new().identity(identity))
                .expect("build tls config")
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("run tls flight server");
        });

        (addr, shutdown_tx)
    }

    #[tokio::test]
    async fn insecure_https_channel_can_complete_flight_handshake() {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let request_scheme = Arc::new(Mutex::new(None));
        let (addr, shutdown_tx) = start_test_tls_flight_server(request_scheme.clone()).await;
        let channel = new_channel(format!("https://{addr}"), None, None)
            .await
            .expect("connect insecure https channel");
        let mut client = FlightServiceClient::new(channel);

        let handshake = client.handshake(futures::stream::once(async {
            HandshakeRequest {
                protocol_version: 0,
                payload: bytes::Bytes::from_static(b"ping"),
            }
        }));

        let response = handshake.await;
        let _ = shutdown_tx.send(());

        assert!(
            response.is_ok(),
            "expected insecure https handshake to succeed, got: {response:?}"
        );
        assert_eq!(
            request_scheme
                .lock()
                .expect("lock request scheme")
                .as_deref(),
            Some("https")
        );
    }

    /// All ports in the configured range are pre-bound, so `Svc::call()` must return an error
    /// instead of silently proceeding with an OS-assigned ephemeral port.
    #[tokio::test]
    async fn svc_returns_error_when_all_ports_exhausted() {
        let listener = std::net::TcpListener::bind("0.0.0.0:0").expect("bind listener");
        let port = listener.local_addr().expect("local addr").port();

        let mut svc = Svc::new(port..=port);
        let uri: Uri = "http://127.0.0.1:1234".parse().expect("parse uri");
        let result = svc.call(uri).await;
        drop(listener);

        assert!(result.is_err(), "expected error when all ports are in use");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("all configured local ports are in use"),
            "unexpected error message: {msg}"
        );
    }

    /// All ports in the configured range are pre-bound, so `InsecureGrpcConnector::call()` must
    /// return an error instead of silently proceeding with an OS-assigned ephemeral port.
    #[tokio::test]
    async fn insecure_connector_returns_error_when_all_ports_exhausted() {
        // The rustls ring provider must be installed before constructing InsecureGrpcConnector.
        // Ignore the error if another test already installed it.
        let _ = rustls::crypto::ring::default_provider().install_default();

        let listener = std::net::TcpListener::bind("0.0.0.0:0").expect("bind listener");
        let port = listener.local_addr().expect("local addr").port();

        let mut connector = InsecureGrpcConnector::new_with_ports(Some(port..=port));
        let uri: Uri = "https://127.0.0.1:1234".parse().expect("parse uri");
        let result = connector.call(uri).await;
        drop(listener);

        assert!(result.is_err(), "expected error when all ports are in use");
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("all configured local ports are in use"),
            "unexpected error message: {msg}"
        );
    }
}
