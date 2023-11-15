use actix_cors::Cors;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use http_auth_basic::Credentials;
use log::LevelFilter;
use std::{fmt::Display, fs::File, io::Read, path::PathBuf, time::Duration};
use taos::*;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{info, instrument, Level};
use tracing_actix_web::TracingLogger;

use actix_embed::Embed;
use actix_web::{
    error::{self, JsonPayloadError, PayloadError},
    http::header::{ContentType, AUTHORIZATION},
    middleware::{Compress, Logger},
    post,
    web::{self},
    App, HttpRequest, HttpResponse, HttpServer, Responder, ResponseError,
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
    let mut args = if let Ok(mut file) = File::open(&file_path) {
        info!("Use configuration file path: {}", file_path.display());
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
        .unwrap_or(LevelFilter::Info);

    let subscriber = tracing_subscriber::fmt()
        .with_level(true)
        .with_thread_ids(true)
        .with_thread_names(true)
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
    const EXPLORER_GRPC: &str = "http://localhost:6055";
    args.port.get_or_insert(EXPLORER_PORT);
    args.profile
        .cluster
        .get_or_insert(EXPLORER_CLUSTER.to_string());
    args.profile.x_api.get_or_insert(EXPLORER_X_PAI.to_string());
    args.profile.grpc.get_or_insert(EXPLORER_GRPC.to_string());

    let port = args.port.unwrap();
    let args = web::Data::new(args);
    let cors = args.cors.unwrap_or_default();

    info!("Explorer service at http://0.0.0.0:{port}");

    HttpServer::new(move || {
        let cors = if cors {
            Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header()
        } else {
            Cors::default()
                .allowed_origin_fn(|origin, req_head| {
                    req_head
                        .headers()
                        .get("Host")
                        .map(|host| origin.as_bytes().ends_with(host.as_bytes()))
                        .unwrap_or(false)
                })
                .allow_any_method()
                .allow_any_header()
                .max_age(3600)
        };
        App::new()
            .wrap(TracingLogger::default())
            .wrap(cors)
            .wrap(Logger::default())
            .wrap(Compress::default())
            .app_data(web::Data::new(reqwest::Client::new()))
            .app_data(args.clone())
            // .route("/", web::get().to(index))
            .route("/rest/{path:.*}", web::to(rest_proxy))
            .route("/api/x/{api:.*}", web::to(x_api))
            .route("/api/-/license", web::to(renew_license))
            .route("/api/-/profile", web::to(profile))
            .route("/api-doc/openapi.json", web::to(x_api_doc))
            .service(web::redirect("/docs", "/docs/"))
            .service(
                Embed::new("/docs/", &StaticAssets)
                    .index_file("index.html")
                    .fallback_handler(|_: &_| {
                        let embed = StaticAssets::get("docs/index.html").unwrap();
                        HttpResponse::Ok()
                            .content_type(ContentType::html())
                            .body(embed.data)
                    }),
            )
            .service(web::redirect("/docs-en", "/docs-en/"))
            .service(
                Embed::new("/docs-en/", &StaticAssets)
                    .index_file("index.html")
                    .fallback_handler(|_: &_| {
                        let embed = StaticAssets::get("docs-en/index.html").unwrap();
                        HttpResponse::Ok()
                            .content_type(ContentType::html())
                            .body(embed.data)
                    }),
            )
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
    .bind(("::1", port))?
    .run()
    .await?;
    Ok(())
}

async fn profile(args: web::Data<Args>, client: web::Data<reqwest::Client>) -> impl Responder {
    if args.profile.x_api.is_none() {
        return HttpResponse::Ok().json(&args.profile);
    }
    let mut profile = args.profile.clone();
    let x = args.profile.x_api.as_deref().unwrap();
    let url = format!("{x}/profile");
    let client = client.get(url);
    let client = client.timeout(Duration::from_secs(10));

    if let Ok(resp) = client.send().await {
        if let Ok(json) = resp.json::<serde_json::Value>().await {
            if let Some(version) = json.get("version") {
                profile
                    .version
                    .replace(version.as_str().unwrap_or_default().into());
            }
        }
    }
    HttpResponse::Ok().json(&profile)
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

async fn proxy(
    req: HttpRequest,
    payload: web::Payload,
    client: web::Data<reqwest::Client>,
    url: &str,
) -> Result<HttpResponse, actix_web::Error> {
    if req.headers().contains_key("upgrade") {
        // Websocket proxy.

        // Forward the request.
        let mut builder = reqwest::ClientBuilder::new().build().unwrap().get(url);
        let info = req.connection_info();
        if let Some(addr) = info.realip_remote_addr().or(info.peer_addr()) {
            builder = builder
                .header("X-Forward-For", addr)
                .header("X-Real-IP", addr);
        }
        for (key, value) in req.headers() {
            builder = builder.header(key, value);
        }
        let target_response = builder.send().await.unwrap();

        // Make sure the server is willing to accept the websocket.
        let status = target_response.status().as_u16();
        if status != 101 {
            return Err(actix_web::error::ErrorBadRequest(format!(
                "Unexpected status code from target: {}",
                status
            )));
        }

        // Copy headers from the target back to the client.
        let mut client_response = HttpResponse::SwitchingProtocols();
        client_response.upgrade("websocket");
        for (header, value) in target_response.headers() {
            client_response.insert_header((header.to_owned(), value.to_owned()));
        }

        let target_upgrade = target_response
            .upgrade()
            .await
            .map_err(error::ErrorInternalServerError)?;
        let (target_rx, mut target_tx) = tokio::io::split(target_upgrade);

        // Copy byte stream from the client to the target.
        tokio::task::spawn_local(async move {
            let mut client_stream = payload.map(|result| {
                result.map_err(|err| std::io::Error::new(std::io::ErrorKind::Other, err))
            });
            let mut client_read = tokio_util::io::StreamReader::new(&mut client_stream);
            let result = tokio::io::copy(&mut client_read, &mut target_tx).await;
            if let Err(err) = result {
                tracing::error!("Error proxying websocket client bytes to target: {err}")
            }
            tracing::info!("Websocket client closed");
        });

        // Copy byte stream from the target back to the client.
        let target_stream = tokio_util::io::ReaderStream::new(target_rx);
        Ok(client_response.streaming(target_stream))
    } else {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        tokio::task::spawn_local(async move {
            let mut payload = payload;
            while let Some(chunk) = payload.next().await {
                if let Err(err) = tx.send(chunk) {
                    tracing::warn!("Error sending payload chunk: {err}");
                }
            }
        });
        let mut builder = client
            .request(req.method().clone(), url)
            .timeout(Duration::from_secs(std::u64::MAX))
            .body(reqwest::Body::wrap_stream(UnboundedReceiverStream::new(rx)));
        let info = req.connection_info();
        if let Some(addr) = info.realip_remote_addr().or(info.peer_addr()) {
            builder = builder
                .header("X-Forward-For", addr)
                .header("X-Real-IP", addr);
        }
        for (k, v) in req.headers() {
            builder = builder.header(k, v);
        }
        let res = builder
            .send()
            .await
            .map_err(error::ErrorInternalServerError)?;
        let mut client_resp = HttpResponse::build(res.status());
        for (header_name, header_value) in res.headers().iter().filter(|(h, _)| *h != "connection")
        {
            client_resp.insert_header((header_name.clone(), header_value.clone()));
        }
        Ok(client_resp.streaming(res.bytes_stream()))
    }
}

async fn rest_proxy(
    args: web::Data<Args>,
    client: web::Data<reqwest::Client>,
    path: web::Path<String>,
    req: HttpRequest,
    payload: web::Payload,
) -> impl Responder {
    let x = args.profile.cluster.as_deref().unwrap();
    let query = req.query_string();
    let url = if query.is_empty() {
        format!("{x}/rest/{path}")
    } else {
        format!("{x}/rest/{path}?{query}")
    };

    proxy(req, payload, client, &url)
        .await
        .map_err(RestErrResponse::new)
}

#[derive(Debug, thiserror::Error)]
enum Error {
    #[error(transparent)]
    Payload(#[from] PayloadError),
    #[error(transparent)]
    ApiDoc(#[from] JsonPayloadError),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

impl error::ResponseError for Error {}

#[instrument(skip_all)]
async fn x_api(
    args: web::Data<Args>,
    client: web::Data<reqwest::Client>,
    api: web::Path<String>,
    req: HttpRequest,
    payload: web::Payload,
) -> impl Responder {
    if args.profile.x_api.is_none() {
        return Ok(HttpResponse::NotFound().finish());
    }
    let x = args.profile.x_api.as_deref().unwrap();
    let url = format!("{x}/{api}?{}", req.query_string());

    proxy(req, payload, client, &url)
        .await
        .map_err(RestErrResponse::new)
}

async fn x_api_doc(
    req: HttpRequest,
    client: web::Data<reqwest::Client>,
    args: web::Data<Args>,
    payload: web::Payload,
) -> Result<HttpResponse, RestErrResponse> {
    if args.profile.x_api.is_none() {
        return Ok(HttpResponse::NotFound().finish());
    }
    let x = args.profile.x_api.as_deref().unwrap();
    let url = format!("{x}/api-doc/openapi.json");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    tokio::task::spawn_local(async move {
        let mut payload = payload;
        while let Some(chunk) = payload.next().await {
            if let Err(err) = tx.send(chunk) {
                tracing::warn!("Error sending payload chunk: {err}");
            }
        }
    });
    let mut builder = client
        .request(req.method().clone(), url)
        .timeout(Duration::from_secs(std::u64::MAX))
        .body(reqwest::Body::wrap_stream(UnboundedReceiverStream::new(rx)));
    let info = req.connection_info();
    if let Some(addr) = info.realip_remote_addr().or(info.peer_addr()) {
        builder = builder
            .header("X-Forward-For", addr)
            .header("X-Real-IP", addr);
    }
    for (k, v) in req.headers() {
        builder = builder.header(k, v);
    }
    let res = builder
        .send()
        .await
        .map_err(error::ErrorInternalServerError)?;
    let mut client_resp = HttpResponse::build(res.status());
    for (header_name, header_value) in res.headers().iter().filter(|(h, _)| *h != "connection") {
        client_resp.insert_header((header_name.clone(), header_value.clone()));
    }
    // client_resp.
    let mut api: serde_json::Value = res.json().await.map_err(error::ErrorInternalServerError)?;
    if let Some(paths) = api.get_mut("paths") {
        assert!(paths.is_object());
        if let serde_json::Value::Object(paths) = paths {
            *paths = paths
                .into_iter()
                .map(|(k, v)| (format!("/api/x{k}"), v.clone()))
                .collect();
        }
    }
    Ok(client_resp.body(serde_json::to_string(&api)?))
}

#[derive(RustEmbed)]
#[folder = "../dist/"]
struct StaticAssets;

#[derive(Parser, Debug, Clone, Deserialize, Serialize, Default)]
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

    /// GRPC endpoint of taosX for agents.
    #[clap(short, long, env = "EXPLORER_GRPC")]
    grpc: Option<String>,

    /// taosX version
    #[clap(skip)]
    version: Option<String>,
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
        "version: ",
        build::TD_VERSION,
        "\ngit: ",
        build::BRANCH,
        "-",
        build::COMMIT_HASH,
        "\nbuild: core-",
        build::PKG_VERSION,
        " ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME
    )
} else {
    const_format::concatcp!(
        "version: ",
        build::TD_VERSION,
        "\ngit: ",
        build::BRANCH,
        "-",
        build::COMMIT_HASH,
        "\nbuild: core-dirty-",
        build::PKG_VERSION,
        " ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME
    )
};

#[derive(Parser, Debug, Clone, Deserialize, Default)]
#[clap(name = env!("CUS_CLI_NAME"), author, version = CLAP_SHORT_VERSION, about, long_about = include_str!(env!("CUS_README")))]
struct Args {
    /// Configuration file
    #[clap(short = 'C', long, env = "EXPLORER_CONFIG_FILE")]
    config_file: Option<PathBuf>,
    /// Port
    #[clap(short, long, global = true, env = "EXPLORER_PORT")]
    #[serde(default)]
    port: Option<u16>,

    /// Allow all origins or not.
    #[clap(skip)]
    #[serde(default)]
    cors: Option<bool>,

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
            code: Code::SUCCESS,
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
                code: Code::FAILED,
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
            if active_code.len() > 0 {
                let sql = format!("alter all dnodes 'activeCode' '{active_code}'");
                conn.exec(&sql).await.map_err(|err| {
                    RestErrResponse::new(format!("Invalid cluster activation code: {err:#}"))
                })?;
            }
        }
        if let Some(c_active_code) = license.c_active_code.as_ref() {
            if c_active_code.len() > 0 {
                let sql = format!("alter all dnodes 'cActiveCode' '{c_active_code}'");
                conn.exec(&sql).await.map_err(|err| {
                    RestErrResponse::new(format!("Invalid connector activation code: {err:#}"))
                })?;
            }
        }
        Ok(RestOkResponse {
            code: Code::SUCCESS,
            column_meta: Default::default(),
            rows: 0,
            data: Default::default(),
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
            code: Code::FAILED,
            desc: err.to_string(),
        }
    }
}

impl Display for RestErrResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {:#}", self.code, self.desc)
    }
}

impl ResponseError for RestErrResponse {
    fn error_response(&self) -> HttpResponse {
        HttpResponse::InternalServerError().json(self)
    }
}

impl From<actix_web::Error> for RestErrResponse {
    fn from(err: actix_web::Error) -> Self {
        Self {
            code: Code::FAILED,
            desc: format!("{:#}", err),
        }
    }
}
impl From<serde_json::Error> for RestErrResponse {
    fn from(err: serde_json::Error) -> Self {
        Self {
            code: Code::FAILED,
            desc: format!("{:#}", err),
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
                code: Code::FAILED,
                desc: err_str,
            }
        }
    }
}

#[cfg(test)]
mod tests;
