// #![feature(btree_drain_filter)]
use awc::error::JsonPayloadError;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use log::LevelFilter;
use std::{fs::File, io::Read};

use actix_embed::Embed;
use actix_web::{
    error::{self, PayloadError},
    http::header::ContentType,
    web, App, HttpRequest, HttpResponse, HttpServer, Responder,
};
use futures_util::StreamExt as _;

use clap::Parser;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let file_path = "/etc/taos/explorer.toml";

    let args = if let Ok(mut file) = File::open(file_path) {
        let mut content = String::new();
        file.read_to_string(&mut content)?;
        let mut args: Args = toml::from_str(&content).unwrap();
        args.update_from(std::env::args());
        args
    } else {
        Args::parse()
    };
    let log_level = args
        .log_level
        .clone()
        .or(args.verbose.clone().map(|v| v.log_level_filter()))
        .unwrap_or(log::LevelFilter::Info);
    pretty_env_logger::formatted_timed_builder()
        .filter_level(log_level)
        .init();

    const EXPLORER_PORT: u16 = 6060;
    let port = args.port.unwrap_or(EXPLORER_PORT);
    let args = web::Data::new(args);
    HttpServer::new(move || {
        App::new()
            .app_data(args.clone())
            .route("/", web::get().to(index))
            .route("/api/x/{api:.*}", web::to(x_api))
            .route("/api/-/profile", web::to(profile))
            .route("/api-doc/openapi.json", web::to(x_api_doc))
            .route("/{route}", web::get().to(index))
            .service(Embed::new("/", &Asset))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await
}

async fn index() -> impl Responder {
    let index_html = Asset::get("index.html").unwrap();
    HttpResponse::Ok().content_type(ContentType::html()).body(
        std::str::from_utf8(index_html.data.as_ref())
            .unwrap()
            .to_string(),
    )
}

async fn profile(args: web::Data<Args>) -> impl Responder {
    HttpResponse::Ok().json(&args.profile)
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    XError(#[from] awc::error::SendRequestError),
    #[error(transparent)]
    PayloadError(#[from] PayloadError),
    #[error(transparent)]
    ApiDocError(#[from] JsonPayloadError),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
}

impl error::ResponseError for Error {}

async fn x_api(
    req: HttpRequest,
    api: web::Path<String>,
    args: web::Data<Args>,
    mut body: web::Payload,
) -> Result<HttpResponse, Error> {
    if args.x_api.is_none() {
        return Ok(HttpResponse::NotFound().finish());
    }
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item?);
    }
    let x = args.x_api.as_deref().unwrap();
    let url = format!("{x}/{api}?{}", req.query_string());
    let client = awc::Client::new();
    let method = req.method();
    let mut resp = client.request(method.clone(), url).send_body(bytes).await?;
    Ok(HttpResponse::Ok().body(resp.body().await?))
}
async fn x_api_doc(
    req: HttpRequest,
    args: web::Data<Args>,
    mut body: web::Payload,
) -> Result<HttpResponse, Error> {
    if args.x_api.is_none() {
        return Ok(HttpResponse::NotFound().finish());
    }
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item?);
    }
    let x = args.x_api.as_deref().unwrap();
    let url = format!("{x}/api-doc/openapi.json");
    let client = awc::Client::new();
    let method = req.method();
    let mut resp = client.request(method.clone(), url).send_body(bytes).await?;
    let mut api: serde_json::Value = resp.json().await?;
    if let Some(paths) = api.get_mut("paths") {
        assert!(paths.is_object());
        if let serde_json::Value::Object(paths) = paths {
            *paths = paths
                .into_iter()
                .map(|(k, v)| (format!("/x{k}"), v.clone()))
                .collect();
        }
    }
    Ok(HttpResponse::Ok().body(serde_json::to_string(&api)?))
}

#[derive(RustEmbed)]
#[folder = "../dist/"]
struct Asset;

#[derive(Parser, Debug, Clone, Deserialize, Serialize)]
struct Profile {
    /// Persist cluster url, rg.: http://192.168.0.201:16041
    #[clap(short, long, env = "EXPLORER_CLUSTER")]
    cluster: Option<String>,

    /// External link for Grafana TDinsight dashboard, use direct ip or hostname like: http://grafana:3000/d/tdinsight-3x/tdinsight-for-3-x?orgId=1&refresh=30s
    #[clap(short, long, env = "EXPLORER_DASHBOARD")]
    dashboard: Option<String>,
}

#[derive(Parser, Debug, Clone, Deserialize)]
#[clap(author, version, about, long_about = include_str!("../README.md"))]
struct Args {
    /// Port
    #[clap(
        short,
        long,
        default_value = "6060",
        global = true,
        env = "EXPLORER_PORT"
    )]
    #[serde(default)]
    port: Option<u16>,
    /// For verbosity logging.
    #[clap(flatten)]
    #[serde(skip)]
    verbose: Option<Verbosity<InfoLevel>>,

    /// For environment variable wised log level.
    #[clap(env = "EXPLORER_LOG_LEVEL", hide = true)]
    log_level: Option<LevelFilter>,

    /// API end point for data streaming task management.
    #[clap(short, long, env = "EXPLORER_X_API")]
    x_api: Option<String>,

    #[clap(flatten)]
    #[serde(flatten)]
    profile: Profile,
}
