use std::path::PathBuf;

use anyhow::Result;

use clap::Parser;

use actix_web::{
    middleware::Logger,
    web::{Data, ServiceConfig},
    App, HttpServer,
};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use task::*;

mod data_sources;
mod metrics;
mod task;

use data_sources::*;
#[derive(Parser, Debug)]
pub(super) struct Cli {
    #[clap(short = 'l', long, default_value = "0.0.0.0:6050")]
    listen: String,
    #[clap(short = 'D', long)]
    database_url: Option<String>,

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
        }
    }
}

fn configure(store: Data<TaskController>) -> impl FnOnce(&mut ServiceConfig) {
    |config: &mut ServiceConfig| {
        config
            .app_data(store)
            // .service(search_tasks)
            .service(get_tasks)
            .service(get_tasks_count)
            .service(create_task)
            .service(update_task)
            .service(delete_task)
            .service(replicate)
            .service(subscribe)
            .service(get_task_by_id)
            .service(start_task)
            .service(stop_task)
            .service(metrics::metrics_exporter)
            .service(data_in_sources)
            .service(data_in_sources_validate)
            .service(data_in_new_task)
            .service(data_in_task_list)
            .service(data_in_get_task_by_id)
            .service(data_in_start_task)
            .service(data_in_stop_task)
            .service(data_in_delete_task)
            // .service(update_task)
            ;
    }
}

impl Cli {
    pub(super) async fn run_with(
        self,
        _opts: super::GlobalOpts,
        rt: tokio::runtime::Runtime,
    ) -> Result<()> {
        #[derive(OpenApi)]
        #[openapi(
            components(
                schemas(
                    // NewReplicate,
                    // NewSubscribe,
                    NewTask,
                    UpdateTask,
                    Cluster,
                    StreamType,
                    Task,
                    Failed,
                    DataSourceInput,
                    CloudTarget,
                    Transformer,
                    DataIn,
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
                // task::replicate,
                // task::subscribe,
                metrics::metrics_exporter,
                data_in_sources,
                data_in_sources_validate,
                data_in_new_task,
                data_in_task_list,
                data_in_get_task_by_id,
                data_in_start_task,
                data_in_stop_task,
                data_in_delete_task,
            ),
            tags(
                (name = "tasks", description = "Task management endpoints"),
                (name = "data sources", description = "Data in/out"),

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

        let controller = TaskController::from_sqlite(&database_url)
            .await?
            .with_runtime(rt);

        let store = Data::new(controller);
        let store_cloned = store.clone();
        let tasks_mgr = store.clone();
        tokio::spawn(async move { tasks_mgr.start_all().await });
        // // Make instance variable of ApiDoc so all worker threads gets the same instance.
        let openapi = ApiDoc::openapi();

        let metrics_recorder = metrics::Metrics::default().init()?;
        let handle = metrics_recorder.handle();
        ::metrics::set_boxed_recorder(Box::new(metrics_recorder))?;

        let recorder = Data::new(handle);

        let server = HttpServer::new(move || {
            // This factory closure is called on each worker thread independently.
            App::new()
                .wrap(Logger::default())
                .app_data(recorder.clone())
                .configure(configure(store.clone()))
                .service(
                    SwaggerUi::new("/swagger-ui/{_:.*}")
                        .url("/api-doc/openapi.json", openapi.clone()),
                )
        })
        .bind(self.listen.as_str())?
        .run();

        tokio::select! {
            _ = server => {
                 log::info!("server stopped");
                // done;
            },
            _ = tokio::signal::ctrl_c() => {
                 log::info!("Ctrl+C triggered");
            }
        };
        store_cloned.stop_all().await?;
        drop(store_cloned);

        Ok(())
    }
}
