use actix_cors::Cors;
use awc::error::JsonPayloadError;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use http_auth_basic::Credentials;
use log::LevelFilter;
use std::{fmt::Display, fs::File, io::Read, path::PathBuf};
use taos::*;
use tracing::{info, instrument, Level};
use tracing_actix_web::{RequestId, TracingLogger};
use tracing_awc::Tracing;
use tracing_subscriber::fmt::format::FmtSpan;

use actix_embed::Embed;
use actix_web::{
    error::{self, PayloadError},
    http::header::{ContentType, AUTHORIZATION},
    middleware::Logger,
    post, web, App, HttpMessage, HttpRequest, HttpResponse, HttpServer, Responder,
};

use clap::Parser;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};

fn log_level_to_tracing_level(level: LevelFilter) -> Option<Level> {
    match level {
        LevelFilter::Off => None,
        LevelFilter::Error => Some(Level::ERROR),
        LevelFilter::Warn => Some(Level::WARN),
        LevelFilter::Info => Some(Level::INFO),
        LevelFilter::Debug => Some(Level::DEBUG),
        LevelFilter::Trace => Some(Level::TRACE),
    }
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(target_os = "windows")]
    let mut file_path: PathBuf = std::path::Path::new("C:\\")
        .join(env!("CUS_NAME"))
        .join("cfg")
        .join("explorer.toml");
    #[cfg(not(target_os = "windows"))]
    let mut file_path = std::path::Path::new("/etc")
        .join(env!("CUS_PROMPT"))
        .join("explorer.toml");

    if let Ok(config) = ConfigPath::try_parse() {
        if let Some(value) = config.config_file {
            file_path = value;
        }
    }
    let args = if let Ok(mut file) = File::open(&file_path) {
        println!("Use configuration file path: {}", file_path.display());
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
    tracing_subscriber::fmt()
        .with_level(true)
        .with_file(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_span_events(FmtSpan::ACTIVE)
        .with_max_level(log_level_to_tracing_level(log_level))
        .pretty()
        .init();

    const EXPLORER_PORT: u16 = 6060;
    let port = args.port.unwrap_or(EXPLORER_PORT);
    let args = web::Data::new(args);

    info!("Explorer service at http://0.0.0.0:{port}");

    HttpServer::new(move || {
        let cors = Cors::default()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header();
        App::new()
            .wrap(TracingLogger::default())
            .wrap(Logger::default())
            .wrap(cors)
            .app_data(args.clone())
            .route("/", web::get().to(index))
            .service(rest_sql)
            .route("/api/x/{api:.*}", web::to(x_api))
            .route("/api/-/profile", web::to(profile))
            .route("/api-doc/openapi.json", web::to(x_api_doc))
            .route("/{route}", web::get().to(index))
            .service(Embed::new("/", &Asset))
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await?;
    Ok(())
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

#[post("/rest/sql")]
async fn rest_sql(args: web::Data<Args>, req: HttpRequest, sql: String) -> impl Responder {
    let header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .unwrap_or_default();
    match args.query(header, &sql).await {
        Ok(ok) => HttpResponse::Ok().json(ok),
        Err(err) => HttpResponse::InternalServerError().json(err),
    }
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

#[instrument(skip(req, body))]
async fn x_api(
    req: HttpRequest,
    req_id: RequestId,
    api: web::Path<String>,
    args: web::Data<Args>,
    mut body: web::Payload,
) -> Result<HttpResponse, Error> {
    req.method();
    if args.x_api.is_none() {
        return Ok(HttpResponse::NotFound().finish());
    }
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item?);
    }
    let x = args.x_api.as_deref().unwrap();
    let url = format!("{x}/{api}?{}", req.query_string());
    let client = awc::Client::builder().wrap(Tracing).finish();
    let method = req.method();
    let mut resp = client
        .request(method.clone(), url)
        .content_type(req.content_type())
        .send_body(bytes)
        .await?;
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
    /// Cluster endpoint. Use REST API like `http://192.168.0.201:16041` or native `taos://192.168.0.201:6030`.
    #[clap(
        short,
        long,
        env = "EXPLORER_CLUSTER",
        default_value = "taos://localhost:6030"
    )]
    cluster: Option<String>,

    /// External link for Grafana TDinsight dashboard, use direct ip or hostname like: http://grafana:3000/d/tdinsight-3x/tdinsight-for-3-x?orgId=1&refresh=30s
    #[clap(short, long, env = "EXPLORER_DASHBOARD")]
    dashboard: Option<String>,

    /// API end point for data streaming task management.
    #[clap(short, long, env = "EXPLORER_X_API")]
    x_api: Option<String>,
}

#[derive(Parser, Debug, Clone, Deserialize)]
struct ConfigPath {
    /// Configuration file
    #[clap(short = 'C', long, env = "EXPLORER_CONFIG_FILE")]
    config_file: Option<PathBuf>,
}

#[derive(Parser, Debug, Clone, Deserialize)]
#[clap(author, version, about, long_about = include_str!(env!("CUS_README")))]
struct Args {
    /// Configuration file
    #[clap(short = 'C', long, env = "EXPLORER_CONFIG_FILE")]
    config_file: Option<PathBuf>,
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

    #[clap(flatten)]
    #[serde(flatten)]
    profile: Profile,
}

impl Args {
    async fn query(&self, header: &str, sql: &str) -> Result<RestOkResponse, RestErrResponse> {
        log::info!("SQL: {sql}");
        //token
        let credentials =
            Credentials::from_header(header.to_string()).map_err(RestErrResponse::new)?;
        let mut dsn: Dsn = self
            .profile
            .cluster
            .as_deref()
            .unwrap_or("taos://localhost:6030")
            .parse()
            .map_err(RestErrResponse::new)?;
        dsn.username = Some(credentials.user_id);
        dsn.password = Some(credentials.password);
        let conn = TaosBuilder::from_dsn(dsn)?.build().await?;

        log::info!("Got connection, querying");
        let mut set = conn.query(sql).await?;
        let column_meta = set
            .fields()
            .iter()
            .map(|f| (f.name().to_string(), f.ty().to_string(), f.bytes()))
            .collect_vec();
        log::info!("Got fields {column_meta:?}, fetching data.");
        let data = set
            .to_records()
            .await?
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| match v {
                        taos::Value::Timestamp(ts) => {
                            serde_json::Value::String(ts.to_datetime_with_tz().to_rfc3339())
                        }
                        _ => v.to_json_value(),
                    })
                    .collect_vec()
            })
            .collect_vec();
        log::info!("SQL result: {data:?}");
        Ok(RestOkResponse {
            code: Code::Success,
            column_meta,
            rows: data.len() as _,
            data,
        })
    }
}
#[derive(Debug, serde::Serialize)]
struct RestOkResponse {
    code: Code,
    column_meta: Vec<(String, String, u32)>,
    data: Vec<Vec<serde_json::Value>>,
    rows: u64,
}
#[derive(Debug, serde::Serialize)]
struct RestErrResponse {
    code: Code,
    desc: String,
}
impl RestErrResponse {
    pub fn new(err: impl Display) -> Self {
        Self {
            code: Code::Failed,
            desc: err.to_string(),
        }
    }
}
impl From<taos::Error> for RestErrResponse {
    fn from(err: taos::Error) -> Self {
        let err_str = err.to_string();
        let parts = err_str.split_terminator(['[', ']']).collect_vec();
        // dbg!(parts);
        if parts.len() == 3 {
            let code = i32::from_str_radix(&parts[1][2..], 16).unwrap_or(0xFFFF);
            let desc = parts[2].to_string();

            RestErrResponse {
                code: Code::new(code),
                desc,
            }
        } else {
            RestErrResponse {
                code: Code::Failed,
                desc: err_str,
            }
        }
    }
}
