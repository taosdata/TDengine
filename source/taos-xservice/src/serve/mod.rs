use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::{sync::Arc, time::Duration};

use actix_cors::Cors;
use actix_multipart::form::MultipartFormConfig;
use actix_web::{
    App, HttpResponse, HttpServer, Responder, get,
    web::{Data, PayloadConfig, ServiceConfig},
};
use anyhow::{Context, Result};
use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use rustls::{
    ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject as _},
};
use serde::{Deserialize, Serialize};
use socket2::{Domain, Socket, Type};
use taosx_core::global::XNODE_HTTP_PORTS;
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use tracing_actix_web::TracingLogger;
use utils::ip::{is_support_ipv6, str_to_socket_addr};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use self::scheduler::agent::AgentSpawnSender;
use self::{
    routes::utils::handle_get_heap,
    rpc::AgentRpcChannel,
    scheduler::{SchedulerNotifier, SchedulerNotify, TaskScheduler, agent::AgentWorker},
};
use crate::executor_worker_threads;
use crate::serve::controller::agent::{
    ActivityOrder, Agent, AgentConnectors, AgentStatus, AgentUpdates, AgentWithToken,
};
use crate::serve::opc::AddPointReq;
use crate::serve::opc::GetPointsHeaderReq;
use crate::serve::opc::PointDetail;
use controller::*;
use data_sources::*;
use taoslog::middleware::TaosRootSpanBuilder;
use taosx_core::plugins::transform::sample::DsSampleIn;
use taosx_core::utils::trace::Qid;
use task::*;

mod backup;
mod controller;
mod data_sources;
mod metrics;
pub mod monitor;
mod privileges;
mod routes;
mod rpc;
#[allow(unused)]
mod scheduler;
pub(crate) mod task;

#[cfg(test)]
mod cli_tests;
#[cfg(test)]
pub mod tests;
pub mod utils;

const TAOSX_REST_API_DEFAULT_PORT: u16 = 6050;
const TAOSX_GRPC_DEFAULT_PORT: u16 = 6055;

#[derive(Deserialize, Clone, Debug, Hash, PartialEq, Eq, ToSchema)]
pub struct DataSetsReq {
    pub from: Option<String>,
    pub from_json: Option<serde_json::Value>,
    pub via: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    pub categories: Vec<String>,
    pub offset: usize,
    pub limit: usize,
}

// #[derive(Parser, Debug, Clone, Default)]
// pub(super) struct Cli {
//     #[clap(flatten)]
//     config_args: ServeOpts,
// }

#[derive(Parser, Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
pub(super) struct Cli {
    /// Listen to ip:port address.
    #[clap(short = 'l', long, env = "LISTEN")]
    pub listen: Option<String>,

    /// SSL server certificate path for rest api.
    #[clap(long, requires("ssl_key"))]
    pub ssl_cert: Option<String>,
    /// SSL key path for rest api.
    #[clap(long, requires("ssl_ca"))]
    pub ssl_key: Option<String>,
    /// SSL CA certificate path for rest api.
    #[clap(long, requires("ssl_cert"))]
    pub ssl_ca: Option<String>,

    /// Grpc listen to ip:port address.
    ///
    #[clap(short = 'g', long, env = "TAOSX_GRPC")]
    pub grpc: Option<String>,

    /// Grpc SSL certificate path. If none, fallback to rest api SSL cert.
    #[clap(long, env = "GRPC_SSL_CERT", requires("grpc_ssl_key"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc_ssl_cert: Option<String>,
    /// Grpc SSL key path. If none, fallback to rest api SSL key.
    #[clap(long, env = "GRPC_SSL_KEY", requires("grpc_ssl_ca"))]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grpc_ssl_key: Option<String>,

    /// Grpc CA certificate path.
    #[clap(long, env = "GRPC_SSL_CA", requires("grpc_ssl_cert"))]
    pub grpc_ssl_ca: Option<String>,

    /// Database URL.
    #[clap(short = 'D', long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    #[clap(hide = true)]
    pub secret_prefix: Option<String>,

    #[clap(long)]
    pub do_not_resume: Option<bool>,

    #[clap(hide = true)]
    pub replica: Option<Vec<String>>,

    #[clap(flatten)]
    #[serde(skip)]
    pub verbose: Option<Verbosity<InfoLevel>>,

    #[clap(long, env = "REPEAT_INTERVAL")]
    pub repeat_interval: Option<u64>,

    #[clap(long, env = "TAOSX_REQUEST_TIMEOUT")]
    pub request_timeout: Option<u64>,

    #[clap(long, hide = true)]
    pub rest_api_threads: Option<usize>,

    #[clap(long, hide = true)]
    pub grpc_threads: Option<usize>,

    #[clap(long, hide = true)]
    pub scheduler_threads: Option<usize>,
}

impl Cli {
    pub fn merge_from(&mut self, rhs: Self) -> &mut Self {
        macro_rules! update_if_none {
            // 处理多个字段
            ($($f:ident),+) => {
                $(
                    if self.$f.is_none() {
                        self.$f = rhs.$f.clone();
                    }
                )+
            };
        }
        update_if_none!(listen, ssl_cert, ssl_key, ssl_ca);
        update_if_none!(database_url, secret_prefix, do_not_resume, request_timeout);
        update_if_none!(grpc, grpc_ssl_cert, grpc_ssl_key, grpc_ssl_ca);
        update_if_none!(scheduler_threads, rest_api_threads, grpc_threads);
        self
    }
}

fn configure(store: Data<TaskControllerRef>) -> impl FnOnce(&mut ServiceConfig) {
    |config: &mut ServiceConfig| {
        config
            .app_data(store)
            .service(metrics::metrics_exporter)
            .service(metrics::metrics_desc)
            .service(data_source_collection)
            .service(get_point_options)
            .service(list_all_parser_plugins)
            .service(download_all_data_set_file)
            .service(download_pi_default_config)
            .service(download_point_template_file)
            .service(data_sources::is_csv_valid)
            .service(init_download_file_task_get)
            .service(init_download_file_task_post)
            .service(check_point_file_ready)
            .service(download_point_file)
            .service(page_point_data)
            .service(opc::get_point_header)
            .service(opc::append_point)
            .service(handle_get_heap)
            .service(get_tmq_task_vgroup_progress)
            .service(get_tmq_task_table_progress)
            .service(check_exists_files)
            .service(download_files)
            .service(upload_files)
            .service(privileges::privileges_migrate)
            .service(privileges::privileges_export)
            .service(privileges::privileges_import)
            .service(metrics::profile)
            .service(filemeta)
            .service(health)
            .service(backup::get_backup_points)
            .service(kafka::seek_to_end);
    }
}

#[get("/health")]
async fn health() -> impl Responder {
    let socket = Socket::new(Domain::IPV4, Type::STREAM, None);
    match socket {
        Ok(socket) => {
            let socket_addr = SocketAddr::from(([0, 0, 0, 0], 6055));
            let bind_result = socket.bind(&socket_addr.into());
            match bind_result {
                Ok(_) => HttpResponse::InternalServerError()
                    .json("The 6055 port provided to the agent is not listening"),
                Err(_) => HttpResponse::Ok().json("ok"),
            }
        }
        Err(_) => HttpResponse::InternalServerError().json("socket error"),
    }
}

impl Cli {
    #[inline]
    pub fn get_listen_address(&self) -> Result<Vec<SocketAddr>> {
        match self.listen.as_ref() {
            Some(addr) => Ok(str_to_socket_addr(addr)?),
            None => {
                if is_support_ipv6() {
                    Ok(vec![SocketAddr::from((
                        Ipv6Addr::UNSPECIFIED,
                        TAOSX_REST_API_DEFAULT_PORT,
                    ))])
                } else {
                    Ok(vec![SocketAddr::from((
                        Ipv4Addr::UNSPECIFIED,
                        TAOSX_REST_API_DEFAULT_PORT,
                    ))])
                }
            }
        }
    }

    pub fn get_listen_port(&self) -> u16 {
        if let Some(addr) = self.listen.as_ref() {
            addr.split(':')
                .next_back()
                .map(|port| port.parse().unwrap_or(TAOSX_REST_API_DEFAULT_PORT))
                .unwrap_or(TAOSX_REST_API_DEFAULT_PORT)
        } else {
            TAOSX_REST_API_DEFAULT_PORT
        }
    }

    #[instrument(skip_all)]
    pub(super) async fn controller(
        &self,
        scheduler: TaskScheduler,
        _max_activities_per_entity: usize,
    ) -> Result<TaskControllerRef> {
        if let Some(interval) = self.repeat_interval {
            tracing::debug!("initial repeat interval");
            let dur = Duration::from_secs(interval);
            controller::trigger::init_repeat_interval(dur);
        }

        Ok(TaskControllerRef::new(scheduler))
    }

    pub(super) async fn channels(
        &self,
    ) -> (
        AgentWorker,
        AgentRpcChannel,
        AgentSpawnSender,
        SchedulerNotifier,
    ) {
        let (agent_action_sender, agent_action_receiver) = tokio::sync::broadcast::channel(1024);
        let (agent_notify_sender, agent_notify_receiver) = tokio::sync::broadcast::channel(1024);
        let (agent_spawn_sender, agent_spawn_receiver) = flume::bounded(0);
        let (scheduler_notify_sender, _) = tokio::sync::broadcast::channel::<SchedulerNotify>(1024);
        let scheduler_notify_sender = Arc::new(scheduler_notify_sender);

        let agent_worker = AgentWorker::new(
            agent_action_sender,
            agent_notify_receiver,
            scheduler_notify_sender.clone(),
            agent_spawn_receiver,
        )
        .await;
        let agent_rpc_channel = AgentRpcChannel::new(agent_action_receiver, agent_notify_sender);
        (
            agent_worker,
            agent_rpc_channel,
            agent_spawn_sender,
            scheduler_notify_sender,
        )
    }

    pub(super) async fn scheduler(
        &self,
        scheduler_notify_sender: SchedulerNotifier,
        agent_worker: AgentWorker,
    ) -> Result<TaskScheduler> {
        let scheduler = TaskScheduler::new(scheduler_notify_sender, agent_worker).await?;
        Ok(scheduler)
    }

    fn load_certs(&self) -> anyhow::Result<Option<ServerConfig>> {
        if let Some((cert, key)) = self.ssl_cert.as_deref().zip(self.ssl_key.as_deref()) {
            tracing::info!("Enable TLS on REST API");
            // init server config builder with safe defaults
            let config = ServerConfig::builder().with_no_client_auth();

            let cert = taosx_core::utils::cert::parse_certificate_to_string(cert)?;
            let key = taosx_core::utils::cert::parse_certificate_to_string(key)?;

            let mut cert = std::io::BufReader::new(cert.as_bytes());
            let mut key = std::io::BufReader::new(key.as_bytes());
            // convert files to key/cert objects
            let cert_chain = CertificateDer::pem_reader_iter(&mut cert)
                .collect::<Result<Vec<_>, _>>()
                .context("Invalid ssl cert")?;
            let key = PrivateKeyDer::from_pem_reader(&mut key).context("Invalid ssl key")?;

            let mut tls_config = config
                .with_single_cert(cert_chain, key)
                .context("Invalid cert chain")?;
            tls_config.alpn_protocols = vec![b"http/1.1".to_vec()];
            return Ok(Some(tls_config));
        }
        Ok(None)
    }

    pub(super) async fn api(
        self,
        controller: TaskControllerRef,
        grpc_handle: tokio::task::JoinHandle<Result<()>>,
        monitor: monitor::Monitor,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        let span = tracing::info_span!("server", addr = self.listen).entered();
        let store_cloned = controller.clone();
        let store = Data::new(controller);

        #[derive(OpenApi)]
        #[openapi(
            components(
                schemas(
                    Failed,
                    DataSourceInput,
                    CloudTarget,
                    Transformer,
                    DataIn,
                    Agent,
                    AgentUpdates,
                    AgentWithToken,
                    AgentStatus,
                    AgentConnectors,
                    DataSetsReq,
                    LangQuery,
                    Lang,
                    UploadForm,
                    FileMetaRequest,
                    ActivityOrder,
                    DsSampleIn,
                    DsSampleOut,
                    TaskBatchReq,
                    PointDetail,
                    GetPointsHeaderReq,
                    AddPointReq,
                    crate::serve::backup::BackupPoint,
                ),
                responses(
                )
            ),
            paths(
                task::upload_files,
                task::filemeta,
                task::check_exists_files,
                task::download_files,
                metrics::profile,
                metrics::metrics_desc,
                data_source_collection,
                get_point_options,
                list_all_parser_plugins,
                download_all_data_set_file,
                init_download_file_task_get,
                init_download_file_task_post,
                check_point_file_ready,
                download_point_file,
                download_point_template_file,
                data_sources::is_csv_valid,
                page_point_data,
                opc::get_point_header,
                opc::append_point,
                privileges::privileges_migrate,
                privileges::privileges_export,
                privileges::privileges_import,
                crate::serve::backup::get_backup_points,
                crate::serve::data_sources::kafka::seek_to_end,
            ),
            tags(
                (name = "tasks", description = "Task management endpoints"),
                (name = "data sources", description = "Data in/out"),
                (name = "transform", description = "Transform simulation"),
                (name = "privileges", description = "Migrate Passwords and Privileges"),
                (name = "backup", description = "Backup"),
                (name = "replica", description = "Replica Monitor"),
            ),
        )]
        struct ApiDoc;

        let openapi = ApiDoc::openapi();
        let handle = monitor.init();
        let recorder = Data::new(handle);
        let addrs = self.get_listen_address()?;
        XNODE_HTTP_PORTS
            .set(addrs.iter().map(|addr| addr.port()).collect())
            .ok();
        let tls = self.load_certs()?;

        let server = HttpServer::new(move || {
            let cors = Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header();
            // This factory closure is called on each worker thread independently.
            App::new()
                .wrap(cors)
                .wrap(TracingLogger::<TaosRootSpanBuilder<Qid>>::new())
                .app_data(recorder.clone())
                .app_data(PayloadConfig::new(usize::MAX))
                .app_data(
                    MultipartFormConfig::default()
                        .memory_limit(1024 * 1024 * 100) // memory limit set to 100M
                        .total_limit(usize::MAX),
                ) // payload set to 2G
                .configure(configure(store.clone()))
                .service(
                    SwaggerUi::new("/swagger-ui/{_:.*}")
                        .url("/api-doc/openapi.json", openapi.clone()),
                )
        });

        let server = {
            fn handle_error(
                err: impl std::fmt::Debug,
                addr: impl std::fmt::Display,
            ) -> anyhow::Error {
                tracing::error!("Start HTTP server error: {:?} (addr: {})", err, addr);
                anyhow::format_err!("Start HTTP server error: {err:?} (addr: {addr})")
            }

            if let Some(tls) = tls {
                addrs.into_iter().try_fold(server, |server, addr| {
                    server
                        .bind_rustls_0_23(addr, tls.clone())
                        .map(|s| {
                            s.workers(
                                self.rest_api_threads
                                    .unwrap_or_else(|| executor_worker_threads(0)),
                            )
                        })
                        .map_err(|err| handle_error(err, addr))
                })?
            } else {
                addrs.into_iter().try_fold(server, |server, addr| {
                    server
                        .bind(addr)
                        .map(|s| {
                            s.workers(
                                self.rest_api_threads
                                    .unwrap_or_else(|| executor_worker_threads(0)),
                            )
                        })
                        .map_err(|err| handle_error(err, addr))
                })?
            }
        };
        let server = server.run();

        tokio::select! {
            rs = server => {
                tracing::info!("server stopped, stopped by: {:?}", rs);
            },
            rs = grpc_handle => {
                tracing::info!("flight RPC service stopped, stopped by: {:?}", rs);
            }
            signal = wait_signal() => {
                tracing::info!("Signal triggered: {signal:?}");
            }
        };
        cancel_token.cancel();
        store_cloned.shutdown().await?;
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(store_cloned);
        span.exit();
        Ok(())
    }

    pub(super) async fn grpc(
        self,
        controller: TaskControllerRef,
        channel: AgentRpcChannel,
        spawn_sender: AgentSpawnSender,
        monitor: monitor::Monitor,
        cancel_token: CancellationToken,
    ) -> Result<()> {
        let mut flight = rpc::RpcConfig::default();
        if let Some(addr) = self.grpc.as_ref() {
            flight.tcp = str_to_socket_addr(addr)?;
        }
        if let Some(ssl_cert) = self.grpc_ssl_cert.as_ref().or(self.ssl_cert.as_ref()) {
            flight.ssl_cert = Some(ssl_cert.into());
        }
        if let Some(ssl_key) = self.grpc_ssl_key.as_ref().or(self.ssl_key.as_ref()) {
            flight.ssl_key = Some(ssl_key.into());
        }
        if let Some(ssl_ca) = self.grpc_ssl_ca.as_ref().or(self.ssl_ca.as_ref()) {
            flight.ssl_ca = Some(ssl_ca.into());
        }

        let addr = flight
            .tcp
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<_>>()
            .join(",");
        flight
            .serve_with_controller(controller, channel, spawn_sender, monitor, cancel_token)
            .await
            .map_err(|err| {
                tracing::error!("grpc(addr:{:?}) init error: {:?}", addr, err);
                err
            })?;

        Ok(())
    }
}

#[derive(Debug)]
pub enum Signal {
    Interrupt,
    Terminate,
    Hangup,
    Quit,
}

#[cfg(unix)]
async fn wait_signal() -> std::io::Result<Signal> {
    use futures_ext::select::{Select4, select4};
    use tokio::signal::unix::{SignalKind, signal};
    match select4(
        signal(SignalKind::interrupt())?.recv(),
        signal(SignalKind::terminate())?.recv(),
        signal(SignalKind::hangup())?.recv(),
        signal(SignalKind::quit())?.recv(),
    )
    .await
    {
        Select4::T1(_) => Ok(Signal::Interrupt),
        Select4::T2(_) => Ok(Signal::Terminate),
        Select4::T3(_) => Ok(Signal::Hangup),
        Select4::T4(_) => Ok(Signal::Quit),
    }
}

#[cfg(not(unix))]
async fn wait_signal() -> std::io::Result<Signal> {
    tokio::signal::ctrl_c().await;
    Ok(Signal::Interrupt)
}
