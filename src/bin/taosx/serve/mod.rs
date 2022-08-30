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
    data_dir: Option<PathBuf>,
    #[clap(short = 'L', long)]
    log_dir: Option<PathBuf>,
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:6050".parse().unwrap(),
            data_dir: Default::default(),
            log_dir: Default::default(),
        }
    }
}

impl Cli {
    pub(super) async fn run_with(self, _opts: super::GlobalOpts) -> Result<()> {
        #[derive(OpenApi)]
        #[openapi(
            handlers(
                task::get_tasks,
                task::create_task,
                task::delete_task,
                task::get_task_by_id,
                task::replicate,
                task::subscribe,
            ),
            components(
                Task,
                NewReplicate,
                NewSubscribe,
                NewTask,
                Cluster,
                StreamType,
                Failed
            ),
            tags(
                (name = "tasks", description = "Task management endpoints")
            ),
        )]
        struct ApiDoc;

        let controller = std::thread::spawn(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .max_blocking_threads(1024)
                .build()
                .unwrap();
            runtime.spawn_blocking(|| {});
            TaskController::from_sqlite("sqlite:taosx.db", runtime)
        })
        .join()
        .unwrap()
        .await?;

        let store = Data::new(controller);
        let store_cloned = store.clone();
        // // Make instance variable of ApiDoc so all worker threads gets the same instance.
        let openapi = ApiDoc::openapi();

        let server = HttpServer::new(move || {
            // This factory closure is called on each worker thread independently.
            App::new()
                .wrap(Logger::default())
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
                store_cloned.clear().await?;
                // done;
            },
            _ = tokio::signal::ctrl_c() => {
                 log::info!("Ctrl+C triggered");
                // done
                store_cloned.clear().await?;
                drop(store_cloned);
            }
        };

        Ok(())
    }
}
