use std::path::PathBuf;

use actix_cors::Cors;
use anyhow::Result;

use clap::Parser;

use actix_web::{
    middleware::Logger,
    web::{Data, ServiceConfig},
    App, HttpServer,
};
use serde::Deserialize;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use task::*;

mod agent;
mod controller;
mod data_sources;
mod metrics;
mod rpc;
mod task;

use controller::*;
use data_sources::*;

use crate::serve::controller::agent::{
    Agent, AgentConnectors, AgentProps, AgentStatus, AgentUpdates, AgentWithToken,
};

use self::agent::{create_agent, delete_agent, get_agents, update_agent};


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
            .service(data_source_collection)
            .service(create_agent)
            .service(update_agent)
            .service(delete_agent)
            .service(get_agents);
    }
}
impl Cli {
    pub(super) async fn run_with(
        self,
        _opts: super::GlobalOpts,
        rt: impl Into<Option<tokio::runtime::Runtime>>,
    ) -> Result<()> {
        #[derive(OpenApi)]
        #[openapi(
            components(
                schemas(
                    TaskDetail,
                    NewTask,
                    UpdateTask,
                    Labels,
                    Task,
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
                    AgentConnectors,
                    DataSetsReq,
                    // DataSet,

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

                metrics::metrics_exporter,

                data_sources_in,
                data_source_collection,

                agent::create_agent,
                agent::update_agent,
                agent::delete_agent,
                agent::get_agents

            ),
            tags(
                (name = "tasks", description = "Task management endpoints"),
                (name = "data sources", description = "Data in/out"),
                (name = "agents", description = "Agents Management"),
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
            log::info!("resume all tasks");
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
        ::metrics::set_boxed_recorder(Box::new(metrics_recorder))?;

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
                .wrap(Logger::default())
                .app_data(recorder.clone())
                .configure(configure(store.clone()))
                .service(
                    SwaggerUi::new("/swagger-ui/{_:.*}")
                        .url("/api-doc/openapi.json", openapi.clone()),
                )
        })
        .bind(addr)
        .map_err(|err| anyhow::format_err!("Start HTTP server error: {err} (addr: {addr})"))?
        .run();

        let mut flight = rpc::RpcConfig::default();

        tokio::select! {
            _ = server => {
                log::info!("server stopped");
                // done;
            },
            _ = flight.serve_with_controller(rpc_controller_ref) => {
                log::info!("flight RPC service stopped");
            }
            _ = tokio::signal::ctrl_c() => {
                log::info!("Ctrl+C triggered");
            }
        };
        store_cloned.stop_all().await?;
        drop(store_cloned);

        Ok(())
    }
}
