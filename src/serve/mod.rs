use std::path::PathBuf;

use actix_cors::Cors;
use actix_multipart::form::MultipartFormConfig;
use anyhow::Result;

use clap::Parser;

use actix_web::{
    web::{Data, PayloadConfig, ServiceConfig},
    App, HttpServer,
};
use metrics_tracing_context::TracingContextLayer;
use metrics_util::layers::{FanoutBuilder, Layer};
use serde::Deserialize;
use tracing::info;
use tracing_actix_web::TracingLogger;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use task::*;

mod agent;
mod controller;
mod data_sources;
mod metrics;
mod routes;
mod rpc;
mod task;
pub use task::check_parser_timestamp_precision;

use controller::*;
use data_sources::*;

use crate::serve::controller::agent::{
    Activity, ActivityOrder, Agent, AgentActivityFilter, AgentConnectors, AgentProps, AgentStatus,
    AgentToken, AgentUpdates, AgentWithToken, LevelFilter,
};

use self::{
    agent::{create_agent, delete_agent, get_agent_activities, get_agents, update_agent},
    routes::cluster::get_cluster_connector_transferred,
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

#[derive(Parser, Debug)]
pub(super) struct Cli {
    /// Listen to ip:port address.
    #[clap(short = 'l', long, default_value = "0.0.0.0:6050")]
    listen: String,

    #[clap(short = 'D', long)]
    database_url: Option<String>,

    /// Do not resume a
    #[clap(long)]
    do_not_resume: bool,

    // #[clap(short = 'D', long)]
    // data_dir: Option<PathBuf>,
    #[clap(short = 'L', long)]
    log_dir: Option<PathBuf>,
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            listen: "0.0.0.0:6050".parse().unwrap(),
            database_url: None,
            log_dir: None,
            do_not_resume: false,
        }
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
            .service(data_sources_in)
            .service(data_sources_in_one)
            .service(data_source_collection)
            .service(create_agent)
            .service(update_agent)
            .service(delete_agent)
            .service(get_agents)
            .service(get_agent_activities)
            .service(get_cluster_connector_transferred)
            .service(get_task_activities_by_id)
            .service(get_task_metrics_by_id)
            .service(download_files)
            .service(upload_files)
            .service(filemeta);
    }
}
impl Cli {
    pub(super) async fn run_with(
        self,
        _opts: super::GlobalOpts,
        _rt: impl Into<Option<tokio::runtime::Runtime>>,
    ) -> Result<()> {
        let span = tracing::info_span!("server", addr = self.listen).entered();
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
                task::get_task_metrics_by_id,

                metrics::metrics_exporter,

                data_sources_in,
                data_sources_in_one,
                data_source_collection,

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
                (name = "agents", description = "Agents Management"),
                (name = "cluster", description = "Cluster Information"),
            ),
        )]
        struct ApiDoc;

        let database_url = if let Some(path) = self.database_url.as_deref() {
            path.to_string()
        } else if let Ok(url) = std::env::var("DATABASE_URL") {
            url
        } else {
            "sqlite:taosx.db".to_string()
        };

        let controller = TaskControllerRef::from_sqlite(&database_url).await?;

        if !self.do_not_resume {
            info!("resume all tasks");
            controller.start_all_with_schedule().await?;
        }

        let rpc_controller_ref = controller.clone();

        let store = Data::new(controller);

        // let task_ctl: TaskControllerRef = store.clone().into_inner().into();
        let store_cloned = store.clone();
        // // Make instance variable of ApiDoc so all worker threads gets the same instance.
        let openapi = ApiDoc::openapi();

        let metrics_recorder = metrics::Metrics::default().init()?;
        let handle = metrics_recorder.handle();

        let debugging_recorder = metrics_util::debugging::DebuggingRecorder::new();
        let snapshotter = Data::new(debugging_recorder.snapshotter());

        let metrics_allowed_labels = ["task.id", "request_id", "client.address"];
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

        let addr = self.listen.as_str();
        let server = HttpServer::new(move || {
            let cors = Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header();
            // This factory closure is called on each worker thread independently.
            App::new()
                .wrap(cors)
                .wrap(TracingLogger::default())
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
        })
        .bind(addr)
        .map_err(|err| anyhow::format_err!("Start HTTP server error: {err} (addr: {addr})"))?
        .run();

        let flight = rpc::RpcConfig::default();

        tokio::select! {
            _ = server => {
                tracing::info!("server stopped");
                // done;
            },
            _ = flight.serve_with_controller(rpc_controller_ref) => {
                tracing::info!("flight RPC service stopped");
            }
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Ctrl+C triggered");
            }
        };
        store_cloned.stop_all().await?;
        drop(store_cloned);
        span.exit();

        Ok(())
    }
}
