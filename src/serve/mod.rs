use std::{sync::Arc, time::Duration};

use actix_cors::Cors;
use actix_multipart::form::MultipartFormConfig;
use anyhow::Result;

use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};

use actix_web::web;
use actix_web::{
    middleware::Compat,
    web::{resource, Data, PayloadConfig, ServiceConfig},
    App, HttpServer,
};

use metrics_tracing_context::TracingContextLayer;
use metrics_util::layers::{FanoutBuilder, Layer};
use serde::{Deserialize, Serialize};
use tracing::info;
use tracing_actix_web::TracingLogger;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use task::*;

mod agent;
mod controller;
mod data_sources;
mod metrics;
mod middleware;
mod routes;
mod rpc;
#[allow(unused)]
mod scheduler;
pub(crate) mod task;
#[cfg(test)]
pub mod tests;

pub use task::check_parser_timestamp_precision;

use controller::*;
use data_sources::*;

use crate::serve::controller::agent::{
    Activity, ActivityOrder, Agent, AgentActivityFilter, AgentConnectors, AgentProps, AgentStatus,
    AgentToken, AgentUpdates, AgentWithToken, LevelFilter,
};
use crate::serve::middleware::TaosXRootSpanBuilder;

use self::{
    agent::{create_agent, delete_agent, get_agent_activities, get_agents, update_agent},
    routes::cluster::get_cluster_connector_transferred,
    rpc::AgentRpcChannel,
    scheduler::{
        agent::AgentWorker, runner::AgentIntegrationChannel, SchedulerNotifier, SchedulerNotify,
        TaskScheduler,
    },
};

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

    #[clap(flatten)]
    #[serde(skip)]
    pub verbose: Option<Verbosity<InfoLevel>>,
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
            .service(delete_task)
            .service(get_task_by_id)
            .service(get_task_offsets_by_id)
            .service(start_task)
            .service(stop_task)
            .service(metrics::metrics_exporter)
            .service(metrics::metrics_desc)
            .service(data_source_is_valid)
            .service(data_sources_in)
            .service(data_sources_in_one)
            .service(data_source_collection)
            .service(data_source_sample)
            .service(download_all_data_set_file)
            .service(create_agent)
            .service(update_agent)
            .service(delete_agent)
            .service(get_agents)
            .service(get_agent_activities)
            .service(get_cluster_connector_transferred)
            .service(get_task_activities_by_id)
            .service(get_task_metrics)
            .service(download_files)
            .service(upload_files)
            .service(metrics::profile)
            .service(filemeta);
    }
}

impl Cli {
    pub(super) async fn controller(&self, scheduler: TaskScheduler) -> Result<TaskControllerRef> {
        let database_url = if let Some(path) = self.database_url.as_deref() {
            path.to_string()
        } else if let Ok(url) = std::env::var("DATABASE_URL") {
            url
        } else if let Ok(root) = std::env::var("TAOSX_DATA_DIR") {
            format!("sqlite:{}/taosx.db", root)
        } else {
            "sqlite:taosx.db".to_string()
        };
        let controller = TaskControllerRef::from_sqlite(&database_url, scheduler).await?;

        if !self.do_not_resume.unwrap_or(false) {
            info!("resume all tasks");
            let controller = controller.clone();
            tokio::spawn(async move {
                if let Err(err) = controller.start_all_with_schedule().await {
                    tracing::error!("resume all tasks error: {}", err);
                }
            });
        }
        Ok(controller)
    }

    pub(super) async fn channels(
        &self,
    ) -> (AgentIntegrationChannel, AgentRpcChannel, SchedulerNotifier) {
        let (agent_activity_sender, agent_activity_receiver) =
            tokio::sync::broadcast::channel(1024);
        let (agent_notify_sender, agent_notify_receiver) = tokio::sync::broadcast::channel(1024);
        let (scheduler_notify_sender, _) = tokio::sync::broadcast::channel::<SchedulerNotify>(1024);
        let scheduler_notify_sender = Arc::new(scheduler_notify_sender);

        let weak_notify_sender = Arc::downgrade(&scheduler_notify_sender);

        let agent_worker = AgentWorker::new(
            agent_activity_sender,
            agent_notify_receiver,
            weak_notify_sender,
        )
        .await;
        let agent_integration_channel = AgentIntegrationChannel::Server(agent_worker);
        let agent_rpc_channel = AgentRpcChannel::new(agent_activity_receiver, agent_notify_sender);
        (
            agent_integration_channel,
            agent_rpc_channel,
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
    ) -> Result<()> {
        let span = tracing::info_span!("server", addr = self.listen).entered();
        let store_cloned = controller.clone();
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
                ),
                responses(
                )
            ),
            paths(
                task::get_tasks,
                task::get_tasks_count,
                task::create_task,
                task::update_task,
                task::delete_task,
                task::start_task,
                task::stop_task,
                task::get_task_by_id,
                task::get_task_offsets_by_id,
                task::get_task_activities_by_id,
                task::upload_files,
                task::filemeta,
                task::download_files,
                task::get_task_metrics,

                metrics::metrics_exporter,
                metrics::profile,
                metrics::metrics_desc,

                data_source_is_valid,
                data_sources_in,
                data_sources_in_one,
                data_source_collection,
                data_source_sample,
                download_all_data_set_file,

                agent::create_agent,
                agent::update_agent,
                agent::delete_agent,
                agent::get_agents,
                agent::get_agent_activities,

                routes::cluster::get_cluster_connector_transferred,

            ),
            tags(
                (name = "tasks", description = "Task management endpoints"),
                (name = "data sources", description = "Data in/out"),
                (name = "transform", description = "Transform simulation"),
                (name = "agents", description = "Agents Management"),
                (name = "cluster", description = "Cluster Information"),
            ),
        )]
        struct ApiDoc;

        let store = Data::new(controller);

        assert!(!controller::DATA_SOURCE_DEFINITIONS.is_empty());

        let openapi = ApiDoc::openapi();

        let metrics_recorder = metrics::Metrics::default().init()?;
        let handle = metrics_recorder.handle();

        let debugging_recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = Data::new(debugging_recorder.snapshotter());

        let metrics_allowed_labels = ["task.id", "client.address"];
        let recorder =
            TracingContextLayer::only_allow(&metrics_allowed_labels).layer(metrics_recorder);
        let debugging =
            TracingContextLayer::only_allow(&metrics_allowed_labels).layer(debugging_recorder);

        let fanout = FanoutBuilder::default()
            .add_recorder(recorder)
            .add_recorder(debugging)
            .build();
        ::metrics::set_boxed_recorder(Box::new(fanout))?;

        let recorder = Data::new(handle);
        let addr = self.listen.as_deref().unwrap_or("0.0.0.0:6050");
        let server = HttpServer::new(move || {
            let cors = Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header();
            // This factory closure is called on each worker thread independently.
            App::new()
                .wrap(cors)
                .wrap(Compat::new(TracingLogger::<TaosXRootSpanBuilder>::new()))
                .app_data(recorder.clone())
                .app_data(snapshotter.clone())
                .app_data(PayloadConfig::new(std::usize::MAX))
                .app_data(
                    MultipartFormConfig::default()
                        .memory_limit(1024 * 1024 * 100) // memory limit set to 100M
                        .total_limit(std::usize::MAX),
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
    ) -> Result<()> {
        let mut flight = rpc::RpcConfig::default();
        if let Some(grpc) = self.grpc.as_ref() {
            let addr = grpc.parse()?;
            flight.tcp.replace(addr);
        }

        flight.serve_with_controller(controller, channel).await?;
        Ok(())
    }
}
