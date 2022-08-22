use std::{net::IpAddr, path::PathBuf};

use anyhow::Result;
use taos::*;

use taosx::{local_to_taos, query_to_csv, query_to_parquet, tmq_to_local, tmq_to_td};

use clap::Parser;

use std::{
    error::Error,
    future::{self, Ready},
    net::Ipv4Addr,
};

use actix_web::{
    dev::{Service, ServiceRequest, ServiceResponse, Transform},
    middleware::Logger,
    web::Data,
    App, HttpResponse, HttpServer,
};
use futures::future::LocalBoxFuture;
use utoipa::{
    openapi::security::{ApiKey, ApiKeyValue, SecurityScheme},
    Modify, OpenApi,
};
use utoipa_swagger_ui::SwaggerUi;

use task::*;

mod task;
mod metrics;

const API_KEY_NAME: &str = "taosx-key";
const API_KEY: &str = "taosx-rocks";

/// Require api key middlware will actually require valid api key
struct RequireApiKey;

impl<S> Transform<S, ServiceRequest> for RequireApiKey
where
    S: Service<
        ServiceRequest,
        Response = ServiceResponse<actix_web::body::BoxBody>,
        Error = actix_web::Error,
    >,
    S::Future: 'static,
{
    type Response = ServiceResponse<actix_web::body::BoxBody>;
    type Error = actix_web::Error;
    type Transform = ApiKeyMiddleware<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        future::ready(Ok(ApiKeyMiddleware {
            service,
            log_only: false,
        }))
    }
}

/// Log api key middleware only logs about missing or invalid api keys
struct LogApiKey;

impl<S> Transform<S, ServiceRequest> for LogApiKey
where
    S: Service<
        ServiceRequest,
        Response = ServiceResponse<actix_web::body::BoxBody>,
        Error = actix_web::Error,
    >,
    S::Future: 'static,
{
    type Response = ServiceResponse<actix_web::body::BoxBody>;
    type Error = actix_web::Error;
    type Transform = ApiKeyMiddleware<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        future::ready(Ok(ApiKeyMiddleware {
            service,
            log_only: true,
        }))
    }
}

struct ApiKeyMiddleware<S> {
    service: S,
    log_only: bool,
}

impl<S> Service<ServiceRequest> for ApiKeyMiddleware<S>
where
    S: Service<
        ServiceRequest,
        Response = ServiceResponse<actix_web::body::BoxBody>,
        Error = actix_web::Error,
    >,
    S::Future: 'static,
{
    type Response = ServiceResponse<actix_web::body::BoxBody>;
    type Error = actix_web::Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, actix_web::Error>>;

    fn poll_ready(
        &self,
        ctx: &mut core::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(ctx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let response = |req: ServiceRequest, response: HttpResponse| -> Self::Future {
            Box::pin(async { Ok(req.into_response(response)) })
        };

        match req.headers().get(API_KEY_NAME) {
            Some(key) if key != API_KEY => {
                if self.log_only {
                    log::debug!("Incorrect api api provided!!!")
                } else {
                    return response(
                        req,
                        HttpResponse::Unauthorized().json(ErrorResponse::Unauthorized(
                            String::from("incorrect api key"),
                        )),
                    );
                }
            }
            None => {
                if self.log_only {
                    log::debug!("Missing api key!!!")
                } else {
                    return response(
                        req,
                        HttpResponse::Unauthorized()
                            .json(ErrorResponse::Unauthorized(String::from("missing api key"))),
                    );
                }
            }
            _ => (), // just passthrough
        }

        if self.log_only {
            log::debug!("Performing operation")
        }

        let future = self.service.call(req);

        Box::pin(async move {
            let response = future.await?;

            Ok(response)
        })
    }
}

#[derive(Parser, Debug)]
pub(super) struct Cli {
    #[clap(short, long)]
    host: IpAddr,
    #[clap(short, long)]
    port: u16,
    #[clap(short, long)]
    data_dir: Option<PathBuf>,
    #[clap(short, long)]
    log_dir: Option<PathBuf>,
}

impl Default for Cli {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".parse().unwrap(),
            port: 6050,
            data_dir: Default::default(),
            log_dir: Default::default(),
        }
    }
}

impl Cli {
    pub(super) async fn run_with(self, opts: super::GlobalOpts) -> Result<()> {
        #[derive(OpenApi)]
        #[openapi(
            handlers(
                task::get_tasks,
                task::create_task,
                task::delete_task,
                task::get_task_by_id,
                task::replicate,
                task::subscribe,
                // task::update_task,
                // task::search_tasks
            ),
            components(
                Task,
                NewReplicate,
                NewSubscribe,
                NewTask,
                Cluster,
                // TaskUpdateRequest,
                StreamType,
                ErrorResponse,
                Failed
            ),
            tags(
                (name = "tasks", description = "Task management endpoints")
            ),
            modifiers(&SecurityAddon)
        )]
        struct ApiDoc;

        struct SecurityAddon;

        impl Modify for SecurityAddon {
            fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
                let components = openapi.components.as_mut().unwrap(); // we can unwrap safely since there already is components registered.
                components.add_security_scheme(
                    "api_key",
                    SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::new(API_KEY_NAME))),
                )
            }
        }

        let store = Data::new(TaskController::new("sqlite:taosx.db").await?);
        // Make instance variable of ApiDoc so all worker threads gets the same instance.
        let openapi = ApiDoc::openapi();
        log::info!("start...");

        HttpServer::new(move || {
            // This factory closure is called on each worker thread independently.
            App::new()
                .wrap(Logger::default())
                .configure(task::configure(store.clone()))
                .service(
                    SwaggerUi::new("/swagger-ui/{_:.*}")
                        .url("/api-doc/openapi.json", openapi.clone()),
                )
        })
        .bind((self.host, self.port))?
        .run()
        .await?;

        Ok(())
    }
}
