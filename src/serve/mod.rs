use std::net::SocketAddr;
use std::{sync::Arc, time::Duration};

use actix_cors::Cors;
use actix_multipart::form::MultipartFormConfig;
use actix_web::web;
use actix_web::{
    get,
    web::{resource, Data, PayloadConfig, ServiceConfig},
    App, HttpResponse, HttpServer, Responder,
};
use anyhow::Result;
use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use serde::{Deserialize, Serialize};
use socket2::{Domain, Socket, Type};
use taoslog::middleware::TaosRootSpanBuilder;
use taosx_core::utils::trace::Qid;
use tracing::{info, instrument, Instrument};
use tracing_actix_web::TracingLogger;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use controller::*;
use data_sources::*;
use taosx_core::plugins::transform::sample::DsSampleIn;
pub use task::check_parser_timestamp_precision;
use task::*;

use crate::build;
use crate::serve::controller::agent::{
    Activity, ActivityOrder, Agent, AgentActivityFilter, AgentConnectors, AgentProps, AgentStatus,
    AgentToken, AgentUpdates, AgentWithToken, LevelFilter,
};
use crate::serve::opc::AddPointReq;
use crate::serve::opc::GetPointsHeaderReq;
use crate::serve::opc::PointDetail;

use self::scheduler::agent::AgentSpawnSender;
use self::{
    agent::{create_agent, delete_agent, get_agent_activities, get_agents, update_agent},
    routes::{cluster::get_cluster_connector_transferred, utils::handle_get_heap},
    rpc::AgentRpcChannel,
    scheduler::{
        agent::AgentWorker, runner::AgentIntegrationChannel, SchedulerNotifier, SchedulerNotify,
        TaskScheduler,
    },
};

mod agent;
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
pub mod tests;

#[derive(Deserialize, Clone, Debug, Hash, PartialEq, Eq, ToSchema)]
pub struct DataSetsReq {
    from: String,
    pub via: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pattern: Option<String>,
    categories: Vec<String>,
    offset: usize,
    limit: usize,
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

    /// Grpc listen to ip:port address.
    ///
    #[clap(short = 'g', long, env = "GRPC")]
    pub grpc: Option<String>,

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
}

impl Cli {
    pub fn merge_from(&mut self, rhs: Self) -> &mut Self {
        macro_rules! update_if_none {
            ($f:ident) => {
                if self.$f.is_none() {
                    self.$f = rhs.$f;
                }
            };
        }
        update_if_none!(listen);
        update_if_none!(database_url);
        update_if_none!(secret_prefix);
        update_if_none!(do_not_resume);
        update_if_none!(request_timeout);
        update_if_none!(grpc);
        self
    }
}

fn configure(store: Data<TaskControllerRef>) -> impl FnOnce(&mut ServiceConfig) {
    |config: &mut ServiceConfig| {
        config
            .app_data(store)
            .service(get_tasks)
            .service(get_tasks_count)
            .service(create_task)
            .service(update_task)
            .service(delete_tasks)
            .service(delete_task)
            .service(get_task_by_id)
            .service(get_task_offsets_by_id)
            .service(start_tasks)
            .service(start_task)
            .service(stop_tasks)
            .service(stop_task)
            .service(metrics::metrics_exporter)
            .service(metrics::metrics_desc)
            .service(get_sample)
            .service(data_source_is_valid)
            .service(data_source_sink_is_valid)
            .service(data_sources_in)
            .service(data_sources_in_one)
            .service(data_source_collection)
            .service(data_source_sample)
            .service(list_all_parser_plugins)
            .service(download_all_data_set_file)
            .service(download_pi_default_config)
            .service(download_point_template_file)
            .service(check_point_file_valid)
            .service(init_download_file_task)
            .service(check_point_file_ready)
            .service(download_point_file)
            .service(page_point_data)
            .service(opc::get_point_header)
            .service(opc::append_point)
            .service(create_agent)
            .service(update_agent)
            .service(delete_agent)
            .service(get_agents)
            .service(get_agent_activities)
            .service(get_cluster_connector_transferred)
            .service(handle_get_heap)
            .service(get_task_activities_by_id)
            .service(get_task_metrics)
            .service(get_tmq_task_vgroup_progress)
            .service(get_tmq_task_table_progress)
            .service(download_files)
            .service(upload_files)
            .service(privileges::privileges_migrate)
            .service(privileges::privileges_export)
            .service(privileges::privileges_import)
            .service(metrics::profile)
            .service(filemeta)
            .service(health);
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
    pub fn get_database_url(&self) -> String {
        if let Some(path) = self.database_url.as_deref() {
            path.to_string()
        } else if let Ok(url) = std::env::var("DATABASE_URL") {
            url
        } else if let Ok(root) = std::env::var("TAOSX_DATA_DIR") {
            format!("sqlite:{}/{}x.db", root, build::CUS_PROMPT)
        } else {
            format!("sqlite:{}x.db", build::CUS_PROMPT)
        }
    }

    #[inline]
    pub fn get_listen_address(&self) -> String {
        match self.listen.as_ref() {
            Some(addr) => addr.clone(),
            None => "0.0.0.0:6050".to_string(),
        }
    }
    #[inline]
    pub fn get_grpc_address(&self) -> String {
        match self.grpc.as_ref() {
            Some(addr) => addr.clone(),
            None => "0.0.0.0:6055".to_string(),
        }
    }

    #[instrument(skip_all)]
    pub(super) async fn controller(
        &self,
        scheduler: TaskScheduler,
        max_activities_per_entity: usize,
    ) -> Result<TaskControllerRef> {
        if let Some(interval) = self.repeat_interval {
            tracing::debug!("initial repeat interval");
            let dur = Duration::from_secs(interval);
            controller::trigger::init_repeat_interval(dur);
        }
        let database_url = self.get_database_url();
        tracing::debug!(db = database_url, "create database connection");
        let controller =
            TaskControllerRef::from_sqlite(&database_url, scheduler, max_activities_per_entity)
                .in_current_span()
                .await?;

        if !self.do_not_resume.unwrap_or(false) {
            info!("resume all tasks");
            let controller = controller.clone();
            tokio::spawn(
                async move {
                    if let Err(err) = controller.start_all_with_schedule().await {
                        tracing::error!("resume all tasks error: {}", err);
                    }
                }
                .in_current_span(),
            );
        }
        Ok(controller)
    }

    pub(super) async fn channels(
        &self,
    ) -> (
        AgentIntegrationChannel,
        AgentRpcChannel,
        AgentSpawnSender,
        SchedulerNotifier,
    ) {
        let (agent_activity_sender, agent_activity_receiver) =
            tokio::sync::broadcast::channel(1024);
        let (agent_notify_sender, agent_notify_receiver) = tokio::sync::broadcast::channel(1024);
        let (agent_spawn_sender, agent_spawn_receiver) = flume::bounded(0);
        let (scheduler_notify_sender, _) = tokio::sync::broadcast::channel::<SchedulerNotify>(1024);
        let scheduler_notify_sender = Arc::new(scheduler_notify_sender);

        let weak_notify_sender = Arc::downgrade(&scheduler_notify_sender);

        let agent_worker = AgentWorker::new(
            agent_activity_sender,
            agent_notify_receiver,
            weak_notify_sender,
            agent_spawn_receiver,
        )
        .await;
        let agent_integration_channel = AgentIntegrationChannel::Server(agent_worker);
        let agent_rpc_channel = AgentRpcChannel::new(agent_activity_receiver, agent_notify_sender);
        (
            agent_integration_channel,
            agent_rpc_channel,
            agent_spawn_sender,
            scheduler_notify_sender,
        )
    }

    pub(super) async fn scheduler(
        &self,
        scheduler_notify_sender: SchedulerNotifier,
        agent_integration_channel: AgentIntegrationChannel,
    ) -> Result<TaskScheduler> {
        let scheduler =
            TaskScheduler::new(scheduler_notify_sender, agent_integration_channel).await?;
        Ok(scheduler)
    }

    // pub fn

    pub(super) async fn api(
        self,
        controller: TaskControllerRef,
        grpc_handle: tokio::task::JoinHandle<Result<()>>,
        monitor: monitor::Monitor,
    ) -> Result<()> {
        let span = tracing::info_span!("server", addr = self.listen).entered();
        let store_cloned = controller.clone();
        let store = Data::new(controller);

        #[derive(OpenApi)]
        #[openapi(
            components(
                schemas(
                    TaskDetail,
                    NewTask,
                    UpdateTask,
                    Labels,
                    Task,
                    TaskActivity,
                    Failed,
                    DataSourceInput,
                    DataSourceDefinition,
                    ProtocolItem,
                    Param,
                    GroupedParams,
                    DataSourceOptions,
                    OptionDef,
                    Protocol,
                    DataSourceType,
                    CloudTarget,
                    Transformer,
                    DataIn,
                    Authentication,
                    Hint,
                    HintDefinition,
                    Definitions,
                    AuthItem,
                    Agent,
                    AgentFilter,
                    AgentProps,
                    AgentUpdates,
                    AgentWithToken,
                    AgentStatus,
                    AgentToken,
                    AgentConnectors,
                    DataSetsReq,
                    ConnectorTransferred,
                    DatasetsDefinition,
                    LangQuery,
                    Lang,
                    UploadForm,
                    FileMetaRequest,
                    AgentActivityFilter,
                    Activity,
                    LevelFilter,
                    ActivityOrder,
                    DsSampleIn,
                    DsSampleOut,

                    TaskBatchReq,

                    PointDetail,
                    GetPointsHeaderReq,
                    AddPointReq,

                ),
                responses(
                )
            ),
            paths(
                task::get_tasks,
                task::get_tasks_count,
                task::create_task,
                task::update_task,
                task::delete_tasks,
                task::delete_task,
                task::start_tasks,
                task::start_task,
                task::stop_tasks,
                task::stop_task,
                task::get_task_by_id,
                task::get_task_offsets_by_id,
                task::get_task_activities_by_id,
                task::upload_files,
                task::filemeta,
                task::download_files,
                metrics::profile,
                metrics::metrics_desc,
                data_source_is_valid,
                data_source_sink_is_valid,
                data_sources_in,
                data_sources_in_one,
                data_source_collection,
                data_source_sample,
                list_all_parser_plugins,
                download_all_data_set_file,
                init_download_file_task,
                check_point_file_ready,
                download_point_file,
                download_point_template_file,
                page_point_data,
                opc::get_point_header,
                opc::append_point,

                agent::create_agent,
                agent::update_agent,
                agent::delete_agent,
                agent::get_agents,
                agent::get_agent_activities,

                privileges::privileges_migrate,
                privileges::privileges_export,
                privileges::privileges_import,

                routes::cluster::get_cluster_connector_transferred,
            ),
            tags(
                (name = "tasks", description = "Task management endpoints"),
                (name = "data sources", description = "Data in/out"),
                (name = "transform", description = "Transform simulation"),
                (name = "agents", description = "Agents Management"),
                (name = "cluster", description = "Cluster Information"),
                (name = "privileges", description = "Migrate Passwords and Privileges"),
            ),
        )]
        struct ApiDoc;
        assert!(!controller::DATA_SOURCE_DEFINITIONS.is_empty());

        let openapi = ApiDoc::openapi();
        let handle = monitor.init();
        let recorder = Data::new(handle);
        let addr = self.get_listen_address();
        let addr = addr.as_str();
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
                .service(
                    resource("/metrics/task/{task_id}")
                        .route(web::get().to(metrics::ws::send_task_metrics)),
                )
        })
        .bind(addr)
        .map_err(|err| anyhow::format_err!("Start HTTP server error: {err} (addr: {addr})"))?
        .run();

        tokio::select! {
            _ = server => {
                tracing::info!("server stopped");
                // done;
            },
            _ = grpc_handle => {
                tracing::info!("flight RPC service stopped");
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Ctrl+C triggered");
            }
        };
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
    ) -> Result<()> {
        let mut flight = rpc::RpcConfig::default();
        if let Some(grpc) = self.grpc.as_ref() {
            let addr = grpc.parse()?;
            flight.tcp.replace(addr);
        }

        flight
            .serve_with_controller(controller, channel, spawn_sender, monitor)
            .await?;
        Ok(())
    }
}
