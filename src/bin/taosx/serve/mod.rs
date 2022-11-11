use std::path::PathBuf;

use anyhow::Result;

use clap::Parser;

use actix_web::{middleware::Logger, web::Data, App, HttpServer};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use task::*;

mod metrics;
mod task;

#[derive(Parser, Debug)]
pub(super) struct Cli {
    #[clap(short = 'l', long, default_value = "127.0.0.1:6050")]
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
            listen: "127.0.0.1:6050".parse().unwrap(),
            database_url: None,
            log_dir: None,
        }
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
                NewReplicate,
                NewSubscribe,
                NewTask,
                Cluster,
                StreamType,
                Task,
                Failed
            ),
            responses(
            )
            ),
            paths(
                task::get_tasks,
                task::get_tasks_count,
                task::create_task,
                task::delete_task,
                task::start_task,
                task::stop_task,
                task::get_task_by_id,
                // task::replicate,
                // task::subscribe,
                metrics::metrics_exporter
            ),
            tags(
                (name = "tasks", description = "Task management endpoints")
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
                .configure(task::configure(store.clone()))
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
