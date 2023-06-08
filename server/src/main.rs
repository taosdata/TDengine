use actix_cors::Cors;
use awc::error::JsonPayloadError;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use http_auth_basic::Credentials;
use log::LevelFilter;
use std::{fmt::Display, fs::File, io::Read, path::PathBuf, time::Duration};
use taos::*;
use tracing::{info, instrument, Level};
use tracing_actix_web::{RequestId, TracingLogger};
use tracing_awc::Tracing;
use tracing_subscriber::fmt::format::FmtSpan;

use actix_embed::Embed;
use actix_web::{
    error::{self, PayloadError},
    http::header::{ContentType, AUTHORIZATION},
    middleware::{self, Logger},
    post,
    web::{self},
    App, HttpMessage, HttpRequest, HttpResponse, HttpServer, Responder,
};
use awc::Client;

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
    let mut args = if let Ok(mut file) = File::open(&file_path) {
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
        .or(args.verbose.as_ref().map(|v| v.log_level_filter()))
        .unwrap_or(log::LevelFilter::Info);
    let subscriber = tracing_subscriber::fmt()
        .with_level(true)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_span_events(FmtSpan::ACTIVE)
        .with_max_level(log_level_to_tracing_level(log_level))
        .compact();
    if atty::is(atty::Stream::Stdout) {
        subscriber.pretty().init();
    } else {
        subscriber.with_ansi(false).init();
    }

    const EXPLORER_PORT: u16 = 6060;
    const EXPLORER_CLUSTER: &str = "http://localhost:6041";
    const EXPLORER_X_PAI: &str = "http://localhost:6050";
    args.port.get_or_insert(EXPLORER_PORT);
    args.profile
        .cluster
        .get_or_insert(EXPLORER_CLUSTER.to_string());
    args.profile.x_api.get_or_insert(EXPLORER_X_PAI.to_string());

    let port = args.port.unwrap();
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
            .wrap(middleware::Compress::default())
            .wrap(cors)
            .app_data(web::Data::new(Client::new()))
            .app_data(args.clone())
            // .route("/", web::get().to(index))
            .route("/rest/{path:.*}", web::to(rest_proxy))
            .route("/api/x/{api:.*}", web::to(x_api))
            .route("/api/-/license", web::to(renew_license))
            .route("/api/-/profile", web::to(profile))
            .route("/api-doc/openapi.json", web::to(x_api_doc))
            .service(
                Embed::new("/", &StaticAssets)
                    .index_file("index.html")
                    .fallback_handler(|_: &_| {
                        let embed = StaticAssets::get("index.html").unwrap();
                        HttpResponse::Ok()
                            .content_type(ContentType::html())
                            .body(embed.data)
                    }),
            )
    })
    .bind(("0.0.0.0", port))?
    .run()
    .await?;
    Ok(())
}

async fn profile(args: web::Data<Args>) -> impl Responder {
    HttpResponse::Ok().json(&args.profile)
}

#[post("/rest/sql")]
async fn rest_sql_builtin(args: web::Data<Args>, req: HttpRequest, sql: String) -> impl Responder {
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

#[derive(Debug, Deserialize)]
struct RenewLicense {
    active_code: Option<String>,
    c_active_code: Option<String>,
}

impl PartialEq for RenewLicense {
    fn eq(&self, other: &Self) -> bool {
        let l = match (&self.active_code, &other.active_code) {
            (Some(l), Some(r)) => l == r,
            _ => true,
        };
        if !l {
            return false;
        }
        match (&self.c_active_code, &other.c_active_code) {
            (Some(l), Some(r)) => l == r,
            _ => true,
        }
    }
}

async fn renew_license(
    args: web::Data<Args>,
    req: HttpRequest,
    body: web::Json<RenewLicense>,
) -> impl Responder {
    let header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .unwrap_or_default();
    match args.renew(header, &body).await {
        Ok(ok) => HttpResponse::Ok().json(ok),
        Err(err) => HttpResponse::InternalServerError().json(err),
    }
}

// #[post("/rest/{path:.*}")]
async fn rest_proxy(
    args: web::Data<Args>,
    client: web::Data<Client>,
    path: web::Path<(String,)>,
    req: HttpRequest,
    body: web::Payload,
) -> impl Responder {
    let (url,) = path.into_inner();
    let x = args.profile.cluster.as_deref().unwrap();
    let url = format!("{x}/rest/{url}");
    let method = req.method();
    let builder = client.request(method.clone(), url);
    let mut builder = builder.timeout(Duration::from_secs(std::u64::MAX));
    *builder.headers_mut() = req.headers().clone();
    match builder.send_stream(body).await {
        Ok(mut ok) => match ok.body().limit(1024 * 1024 * 1024).await {
            Ok(ok) => HttpResponse::Ok().body(ok),
            Err(err) => HttpResponse::InternalServerError().json(RestErrResponse {
                code: Code::Failed,
                desc: err.to_string(),
            }),
        },
        Err(err) => HttpResponse::InternalServerError().json(RestErrResponse {
            code: Code::Failed,
            desc: err.to_string(),
        }),
    }
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    XApi(#[from] awc::error::SendRequestError),
    #[error(transparent)]
    Payload(#[from] PayloadError),
    #[error(transparent)]
    ApiDoc(#[from] JsonPayloadError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
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
    if args.profile.x_api.is_none() {
        return Ok(HttpResponse::NotFound().finish());
    }
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item?);
    }
    let x = args.profile.x_api.as_deref().unwrap();
    let url = format!("{x}/{api}?{}", req.query_string());
    let client = awc::Client::builder().wrap(Tracing).finish();
    let method = req.method();
    let client = client
        .request(method.clone(), url)
        .timeout(Duration::from_secs(std::u64::MAX));
    let mut resp = client
        .content_type(req.content_type())
        .send_body(bytes)
        .await?;
    // match resp {
    //     Ok(mut ok) =>
    //         match ok.body().limit(1024 * 1024 * 1024).await {
    //             Ok(data) => Ok(HttpResponseBuilder::new(ok.status()).body(data)),
    //             Err(err) => Err(Error::PayloadError(err)),
    //         },
    //     Err(err) => Err(Error::XError(err)),
    // }
    Ok(HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(resp.body().await?))
}

async fn x_api_doc(
    req: HttpRequest,
    args: web::Data<Args>,
    mut body: web::Payload,
) -> Result<HttpResponse, Error> {
    if args.profile.x_api.is_none() {
        return Ok(HttpResponse::NotFound().finish());
    }
    let mut bytes = web::BytesMut::new();
    while let Some(item) = body.next().await {
        bytes.extend_from_slice(&item?);
    }
    let x = args.profile.x_api.as_deref().unwrap();
    let url = format!("{x}/api-doc/openapi.json");
    let client = awc::Client::new();
    let method = req.method();
    let client = client
        .request(method.clone(), url)
        .timeout(Duration::from_secs(std::u64::MAX));
    let mut resp = client.send_body(bytes).await?;
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
struct StaticAssets;

#[derive(Parser, Debug, Clone, Deserialize, Serialize)]
struct Profile {
    /// Cluster endpoint. Use taosAdapter endpoint like `http://192.168.0.201:16041`.
    #[clap(short, long, env = "EXPLORER_CLUSTER")]
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

shadow_rs::shadow!(build);

const CLAP_SHORT_VERSION: &str = if build::GIT_CLEAN && const_str::equal!("main", build::BRANCH) {
    const_format::concatcp!(
        build::PKG_VERSION,
        "-",
        build::SHORT_COMMIT,
        " (built ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME,
        ")"
    )
} else {
    const_format::concatcp!(
        build::PKG_VERSION,
        "-",
        build::BRANCH,
        "-",
        build::SHORT_COMMIT,
        "-dirty",
        " (built ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME,
        ")"
    )
};

#[derive(Parser, Debug, Clone, Deserialize)]
#[clap(name = env!("CUS_CLI_NAME"), author, version = CLAP_SHORT_VERSION, about, long_about = include_str!(env!("CUS_README")))]
struct Args {
    /// Configuration file
    #[clap(short = 'C', long, env = "EXPLORER_CONFIG_FILE")]
    config_file: Option<PathBuf>,
    /// Port
    #[clap(short, long, global = true, env = "EXPLORER_PORT")]
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

    async fn renew(
        &self,
        header: &str,
        license: &RenewLicense,
    ) -> Result<RestOkResponse, RestErrResponse> {
        if license.active_code.is_none() && license.c_active_code.is_none() {
            return Err(RestErrResponse {
                code: Code::Failed,
                desc: "active code or connector active code must exist at lease one".into(),
            });
        }
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

        if let Some(active_code) = license.active_code.as_ref() {
            let sql = format!("alter all dnodes 'activeCode' '{active_code}'");
            conn.exec(&sql).await?;
        }
        if let Some(active_code) = license.c_active_code.as_ref() {
            let sql = format!("alter all dnodes 'cActiveCode' '{active_code}'");
            conn.exec(&sql).await?;
        }
        let renewed = conn
            .query("show dnodes")
            .await?
            .deserialize::<RenewLicense>()
            .all(|l| async move { l.map(|l| l == *license).unwrap_or_default() })
            .await;

        if renewed {
            Ok(RestOkResponse {
                code: Code::Success,
                column_meta: Default::default(),
                rows: 0,
                data: Default::default(),
            })
        } else {
            Err(RestErrResponse {
                code: Code::Failed,
                desc: "Alter all dnodes success, but the `show dnodes` result is not consist with new license".to_string(),
            })
        }
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
