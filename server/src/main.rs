use actix_cors::Cors;
use actix_files::NamedFile;
use anyhow::Context;
use chrono::TimeZone;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use deadpool::managed::{Object, PoolError};
use favorites::FavoritesSql;
use geos::{Geom, Geometry};
use http_auth_basic::Credentials;
use log::LevelFilter;
use reqwest::RequestBuilder;
use rustls::server::ServerConfig;
use rustls_pemfile::{certs, private_key};
use serde_with::{serde_as, FromInto};
use std::{
    collections::HashMap,
    fmt::Display,
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
    str::FromStr,
    sync::OnceLock,
    time::Duration,
};
use taos::*;
use taos_query::Manager;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{error, info, instrument, Instrument};
use tracing_actix_web::TracingLogger;

use actix_embed::Embed;
use actix_web::{
    body::BoxBody,
    dev::{fn_service, ServiceRequest, ServiceResponse},
    error::{self, JsonPayloadError, PayloadError},
    http::header::{ContentType, AUTHORIZATION, X_FORWARDED_FOR},
    middleware::Compress,
    post,
    web::{self, Query},
    App, HttpRequest, HttpResponse, HttpServer, Responder, ResponseError,
};
use anyhow::bail;
use clap::Parser;
use qid::{headers_with_qid, Qid, DEFAULT_INSTANCE_ID, INSTANCE_ID};
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};
use taoslog::{
    layer::TaosLayer,
    middleware::TaosRootSpanBuilder,
    utils::{QidMetadataGetter, Span},
    writer::RollingFileAppender,
    QidManager,
};
use tracing::debug;
use tracing_subscriber::{
    filter,
    layer::{Layer, SubscriberExt},
    util::SubscriberInitExt,
};

mod favorites;
mod qid;
pub mod verification;

#[derive(Clone)]
struct UserPool {
    password: String,
    pool: deadpool::managed::Pool<Manager<TaosBuilder>>,
}

static TAOS_POOL: OnceLock<scc::HashMap<String, UserPool>> = OnceLock::new();

fn clear_pool(dsn: &Dsn, username: String) {
    let map = TAOS_POOL.get_or_init(scc::HashMap::new);
    let mut dsn_simple = dsn.clone();
    dsn_simple.username = Some(username);
    dsn_simple.password = None;
    tracing::info!("clear pool for {:?}", dsn_simple);
    map.remove(&dsn_simple.to_string());
}

async fn get_connection(dsn: &Dsn) -> Result<Object<Manager<TaosBuilder>>, String> {
    let map = TAOS_POOL.get_or_init(scc::HashMap::new);
    let mut dsn_simple = dsn.clone();
    dsn_simple.password = None;

    let user_pool = map
        .get(&dsn_simple.to_string())
        .filter(|pool| pool.password == dsn.password.clone().unwrap_or_default())
        .map(|pool| pool.pool.clone());

    if user_pool.is_some() {
        return user_pool.unwrap().get().await.map_err(|err| match err {
            PoolError::Backend(inner_err) => format!("{inner_err:#}"),
            err => format!("Failed to get connection: {err:#}"),
        });
    }

    let builder = taos::TaosBuilder::from_dsn(dsn);
    if builder.is_err() {
        tracing::error!("Failed to create taosbuilder: {:?}", builder.err());
        return Err("inner error: failed to get connection pool".to_string());
    }
    let pool = builder.unwrap().pool();
    if pool.is_err() {
        tracing::error!("Failed to create pool: {:?}", pool.err());
        return Err("inner error: failed to get connection pool".to_string());
    }

    let pool = pool.unwrap();
    let conn = pool.get().await.map_err(|err| match err {
        PoolError::Backend(inner_err) => format!("{inner_err:#}"),
        err => format!("Failed to get connection: {err:#}"),
    });

    if conn.is_ok() {
        let new_user_pool = UserPool {
            password: dsn.password.clone().unwrap_or_default(),
            pool: pool.clone(),
        };
        tracing::debug!("create new pool for {:?}", dsn);
        let _ = map.upsert(dsn_simple.to_string(), new_user_pool);
    }
    conn
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    // info!(env!("CUS_NAME"));

    #[cfg(target_os = "windows")]
    let path = format!("C:\\{}\\cfg", env!("CUS_NAME"));

    #[cfg(not(target_os = "windows"))]
    let path = format!("/etc/{}", env!("CUS_PROMPT"));

    let mut file_path = std::path::Path::new(&path).join("explorer.toml");

    if let Ok(config) = ConfigPath::try_parse() {
        if let Some(value) = config.config_file {
            file_path = value;
        }
    }
    let mut args = if let Ok(mut file) = File::open(&file_path) {
        let mut content = String::new();
        file.read_to_string(&mut content).context(format!(
            "Failed to read configuration from {}",
            file_path.display()
        ))?;
        let mut args: Args = toml::from_str(&content).unwrap();
        args.update_from(std::env::args());
        println!("Use configuration file path: {}", file_path.display());
        args
    } else {
        let args = Args::parse();
        println!("No configuration file found, use default arguments.");
        args
    };

    args.cfg_path = Some(path);

    // init instance id
    args.instance_id =
        Some(*INSTANCE_ID.get_or_init(|| args.instance_id.unwrap_or(DEFAULT_INSTANCE_ID)));

    let log_level = args
        .log
        .as_ref()
        .and_then(|opts| opts.level)
        .or(args.log_level)
        .or(args.verbose.as_ref().map(|v| v.log_level_filter()))
        .unwrap_or(log::LevelFilter::Info);

    match args.log.as_mut() {
        Some(opts) => {
            opts.level = Some(log_level);
            opts.merge_from(LogOpts::default());
        }
        None => {
            let opts = LogOpts {
                level: Some(log_level),
                ..Default::default()
            };
            args.log = Some(opts);
        }
    }

    let Some(LogOpts {
        path,
        compress,
        rotation_count,
        keep_days,
        rotation_size,
        reserved_disk_size,
        ..
    }) = args.log.clone()
    else {
        bail!("Log opts not found")
    };

    let log_level: filter::LevelFilter = match log_level {
        log::LevelFilter::Off => filter::LevelFilter::OFF,
        log::LevelFilter::Error => filter::LevelFilter::ERROR,
        log::LevelFilter::Warn => filter::LevelFilter::WARN,
        log::LevelFilter::Info => filter::LevelFilter::INFO,
        log::LevelFilter::Debug => filter::LevelFilter::DEBUG,
        log::LevelFilter::Trace => filter::LevelFilter::TRACE,
    };

    // init logger
    let mut layers = Vec::with_capacity(2);
    let appender = RollingFileAppender::builder(
        path.unwrap(),
        format!("{}explorer", env!("CUS_PROMPT")),
        *INSTANCE_ID.get().unwrap(),
    )
    .compress(compress.unwrap())
    .reserved_disk_size(&reserved_disk_size.unwrap())
    .rotation_count(rotation_count.unwrap())
    .keep_days(keep_days.unwrap())
    .rotation_size(&rotation_size.unwrap())
    .build()
    .unwrap();

    layers.push(
        TaosLayer::<Qid>::new(appender)
            .with_filter(log_level)
            .boxed(),
    );

    if cfg!(debug_assertions) {
        layers.push(
            TaosLayer::<Qid, _, _>::new(std::io::stdout)
                .with_ansi()
                .with_location()
                .with_filter(log_level)
                .boxed(),
        );
    }

    tracing_subscriber::registry().with(layers).init();

    let span = tracing::info_span!("main");
    let _entered = span.enter();

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
    let app_args = web::Data::new(args.clone());
    let cors = app_args.cors.unwrap_or_default();

    let data_dir = match args.data_dir {
        Some(path) => path,
        None => {
            if cfg!(windows) {
                format!("C:\\{}\\data\\explorer", env!("CUS_NAME"))
            } else {
                format!("/var/lib/{}/explorer", env!("CUS_PROMPT"))
            }
        }
    };
    let favorites = FavoritesSql::new(&data_dir).await?;

    args.data_dir = Some(data_dir);

    print_config_values(&args);

    let server = HttpServer::new(move || {
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
        let app = App::new()
            .wrap(TracingLogger::<TaosRootSpanBuilder<Qid>>::new())
            .wrap(cors)
            .wrap(Compress::default())
            .app_data(web::Data::new(reqwest::Client::new()))
            .app_data(app_args.clone())
            .app_data(web::Data::new(favorites.clone()))
            // .route("/", web::get().to(index))
            .route("/rest/{path:.*}", web::to(rest_proxy))
            .route("/api/x/{api:.*}", web::to(x_api))
            .route("/api/-/import", web::to(import))
            .route("/api/-/license", web::to(renew_license))
            .route("/api/-/profile", web::to(profile))
            .route("/api/-/captcha", web::get().to(generate_captcha_image))
            .route(
                "/api/-/verification-code",
                web::get().to(send_verification_code),
            )
            .route(
                "/api/-/verification-code",
                web::post().to(check_verification_code),
            )
            .route("/api/-/taosd-info", web::post().to(report_taosd_info))
            .route("/api/-/isbinding", web::to(check_binding))
            .route("/api-doc/openapi.json", web::to(x_api_doc))
            .route(
                "/api/-/password/{username}",
                web::post().to(modify_password),
            )
            .route(
                "/api/-/favorites/sql",
                web::post().to(favorites::add_favorites_sql),
            )
            .route(
                "/api/-/favorites/sql",
                web::get().to(favorites::get_favorites_sql_page),
            )
            .route(
                "/api/-/favorites/sql/{id}",
                web::delete().to(favorites::delete_favorites_sql),
            )
            .route(
                "/api/-/favorites/sql/{id}",
                web::patch().to(favorites::update_favorites_sql),
            )
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
            );

        if let Some(assets) = args.assets.clone() {
            let assets = assets.clone();
            app.service(
                actix_files::Files::new("/", assets)
                    .index_file("index.html")
                    .default_handler(fn_service(move |req: ServiceRequest| async {
                        let args = req.app_data::<web::Data<Args>>();
                        let assets = args.unwrap().assets.as_ref().unwrap().clone();
                        let (req, _) = req.into_parts();
                        let file = NamedFile::open_async(assets.join("index.html")).await?;
                        let res = file.into_response(&req);
                        Ok(ServiceResponse::new(req, res))
                    }))
                    .show_files_listing(),
            )
        } else {
            app.service(
                Embed::new("/", &StaticAssets)
                    .index_file("index.html")
                    .fallback_handler(|_: &_| {
                        let embed = StaticAssets::get("index.html").unwrap();
                        HttpResponse::Ok()
                            .content_type(ContentType::html())
                            .body(embed.data)
                    }),
            )
        }
    });

    let addr = args.addr.as_deref().unwrap_or("0.0.0.0");

    info!("Starting server at {addr}:{port}");

    let certificate = if args.ssl.is_some() {
        args.ssl
            .clone()
            .unwrap()
            .certificate
            .unwrap_or(String::from(""))
    } else {
        String::from("")
    };
    let certificate_key = if args.ssl.is_some() {
        args.ssl
            .clone()
            .unwrap()
            .certificate_key
            .unwrap_or(String::from(""))
    } else {
        String::from("")
    };

    // error reported when configuring only one file, so change it to '||' @zqsong
    let server = if !certificate.is_empty() || !certificate_key.is_empty() {
        let cert_file = File::open(&certificate).expect("Failed to open certificate file");
        let cert_key_file = File::open(&certificate_key).expect("Failed to open private key file");

        let cert = certs(&mut BufReader::new(cert_file)).try_collect()?;
        let cert_key = private_key(&mut BufReader::new(cert_key_file))?
            .ok_or_else(|| anyhow::anyhow!("No private key found in file {certificate_key}"))?;

        let config = ServerConfig::builder()
            // .with_safe_defaults()
            .with_no_client_auth()
            .with_single_cert(cert, cert_key)
            .expect("bad certificate/key");

        let server = server
            .bind_rustls_0_23((addr, port), config.clone())
            .with_context(|| format!("Bind address {addr}:{port} error"))?;

        if let Some(ipv6) = args.ipv6.as_deref() {
            server
                .bind_rustls_0_23((ipv6, port), config.clone())
                .with_context(|| format!("Bind IPv6 address [{ipv6}]:{port} error"))?
        } else {
            server
        }
    } else {
        let server = server
            .bind((addr, port))
            .with_context(|| format!("Bind address {addr}:{port} error"))?;

        if let Some(ipv6) = args.ipv6.as_deref() {
            server
                .bind((ipv6, port))
                .with_context(|| format!("Bind IPv6 address [{ipv6}]:{port} error"))?
        } else {
            server
        }
    };

    server.run().await?;
    Ok(())
}

fn print_config_values(args: &Args) {
    // TODO: use proc_macro to generate method to get all current configurations
    let v = serde_json::to_vec(args).unwrap();
    let map = serde_json::from_slice::<HashMap<String, serde_json::Value>>(&v).unwrap();
    let mut s = String::new();
    tracing::info!("explorer version: {}", build::PKG_VERSION);
    tracing::info!("commit id: {}", build::COMMIT_HASH);
    tracing::info!("build time: {}", build::BUILD_TIME);
    s += "global config\n";
    s += "=======================================================================\n";
    for (k, v) in map {
        if v.is_null() {
            continue;
        }
        s += &format!("{:<18}{:<22}{}\n", ' ', k, v);
    }
    s += "=======================================================================";
    tracing::info!("{s}");
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub(crate) struct R<T> {
    code: u32,
    data: Option<T>,
    msg: Option<String>,
}

impl<T> R<T> {
    pub(crate) fn success(data: T) -> Self {
        Self {
            code: 0,
            data: Some(data),
            msg: None,
        }
    }

    pub(crate) fn fail(code: u32, msg: impl Display) -> Self {
        Self {
            code,
            data: None,
            msg: Some(format!("{msg}")),
        }
    }

    pub(crate) fn internal(err: impl Display) -> Self {
        Self {
            code: 1,
            data: None,
            msg: Some(format!("{err:#}")),
        }
    }
}

impl<T> Display for R<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.msg.as_ref() {
            Some(msg) => write!(f, "[{}] {}", self.code, msg),
            None => write!(f, "[{}]", self.code),
        }
    }
}

impl<T> Responder for R<T>
where
    T: serde::Serialize,
{
    type Body = actix_web::body::BoxBody;

    fn respond_to(self, _req: &HttpRequest) -> HttpResponse<Self::Body> {
        HttpResponse::Ok().json(self)
    }
}

impl<T> ResponseError for R<T>
where
    T: std::fmt::Debug + serde::Serialize,
{
    fn status_code(&self) -> reqwest::StatusCode {
        match self.code {
            1 => reqwest::StatusCode::INTERNAL_SERVER_ERROR,
            _ => reqwest::StatusCode::OK,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        match self.code {
            1 => HttpResponse::InternalServerError().json(self.msg.clone().unwrap_or_default()),
            _ => HttpResponse::Ok().json(self),
        }
    }
}

/**
 * 检查当前 TDengine 是否已经绑定了手机号或邮箱。
 */
#[instrument(skip_all)]
async fn check_binding(args: web::Data<Args>) -> impl Responder {
    let binding_record_file =
        PathBuf::from(args.cfg_path.as_ref().unwrap()).join("explorer-register.cfg");
    let server = args.profile.cluster.as_deref().unwrap();
    let check_result = verification::check_phone_email_verified(&binding_record_file, server);
    match check_result {
        Ok(_) => HttpResponse::Ok().json(R::success(true)),
        Err(err) => {
            error!(
                "check {} in file {:?}, Failed to check binding: {}",
                server, binding_record_file, err
            );
            HttpResponse::Ok().json(R::success(false))
        }
    }
}

#[instrument(skip_all)]
async fn profile(args: web::Data<Args>, client: web::Data<reqwest::Client>) -> impl Responder {
    if args.profile.x_api.is_none() {
        return HttpResponse::Ok().json(&args.profile);
    }

    let mut qid = Span.get_qid::<Qid>().unwrap_or_else(Qid::init);
    qid.add_sequence_id();

    let mut profile = args.profile.clone();
    let x = args.profile.x_api.as_deref().unwrap();
    let url = format!("{x}/profile");
    tracing::debug!(url, "send request to taosx");
    let client = client.get(url).headers(headers_with_qid(&qid));
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

#[derive(Debug, Deserialize)]
struct VerificationReqBody {
    phone_email: Option<String>,
    verification_code: Option<String>,
    captcha: Option<String>,
    lang: Option<String>,
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TaosdInfoBody {
    phone_email: Option<String>,
    lang: Option<String>,
    taosd_version: Option<String>,
    cluster_id: Option<String>,
}

#[instrument(skip_all)]
async fn generate_captcha_image(params: web::Query<VerificationReqBody>) -> impl Responder {
    let captcha_key = format!("captcha-{}", params.phone_email.as_ref().unwrap());
    let img = verification::generate_captcha(captcha_key);

    HttpResponse::Ok()
        .content_type("image/png")
        .body(img.unwrap())
}

// phone_email=18600000000&captcha=1234
#[instrument(skip_all)]
async fn send_verification_code(
    args: web::Data<Args>,
    params: web::Query<VerificationReqBody>,
) -> impl Responder {
    if params.phone_email.is_none() || params.captcha.is_none() {
        return HttpResponse::BadRequest().json(RestErrResponse {
            code: Code::FAILED,
            desc: "phone_email and captcha is required".to_string(),
        });
    }

    let str_phone_email = params.phone_email.as_ref().unwrap();
    let str_captcha = params.captcha.as_ref().unwrap();
    if str_phone_email.is_empty() || str_captcha.is_empty() {
        return HttpResponse::Ok().json(R::<()>::fail(400, "captchaInputError".to_string()));
    }

    let captcha_key = format!("captcha-{}", str_phone_email);
    let captcha_check_result = verification::check_security_code(&captcha_key, str_captcha);
    if captcha_check_result != "pass" {
        return HttpResponse::Ok().json(R::<()>::fail(400, "captchaInputError".to_string()));
    }

    let lang_code = match params.lang.as_deref() {
        Some("zh") => "zh_CN",
        _ => "en_US",
    };
    let result = verification::send_verification_code_with_cloud_open_api(
        args.cloud_open_api.clone(),
        str_phone_email,
        lang_code,
    )
    .await;

    match result {
        Ok(200) => HttpResponse::Ok().json(R::success("")),
        Ok(code) => HttpResponse::Ok().json(R::<Option<()>>::fail(
            code,
            "post cloud api error".to_string(),
        )),
        Err(err) => {
            log::error!("Failed to send verification code: {:?}", err);
            HttpResponse::Ok().json(R::<Option<()>>::fail(501, err.to_string()))
        }
    }
}

#[instrument(skip_all)]
async fn check_verification_code(
    args: web::Data<Args>,
    body: web::Json<VerificationReqBody>,
) -> impl Responder {
    if body.phone_email.is_none() || body.verification_code.is_none() {
        return HttpResponse::Ok()
            .json(R::<()>::fail(400, "verificationCodeInputError".to_string()));
    }

    let str_phone_email = body.phone_email.as_ref().unwrap();
    let str_verification_code = body.verification_code.as_ref().unwrap();
    if str_phone_email.is_empty() || str_verification_code.is_empty() {
        return HttpResponse::Ok()
            .json(R::<()>::fail(400, "verificationCodeInputError".to_string()));
    }

    let result = verification::check_security_code(str_phone_email, str_verification_code);
    if result == "pass" {
        let binding_record_file =
            PathBuf::from(args.cfg_path.as_ref().unwrap()).join("explorer-register.cfg");
        let server = args.profile.cluster.as_deref().unwrap();
        verification::record_binding_phone_email(server, str_phone_email, &binding_record_file);

        let lang_code = match body.lang.as_deref() {
            Some("zh") => "zh_CN",
            _ => "en_US",
        };

        let report_result = verification::report_verification_status_to_cloud(
            args.cloud_open_api.clone(),
            str_phone_email,
            str_verification_code,
            lang_code,
            body.name.as_ref().unwrap(),
        )
        .await;

        match report_result {
            Ok(200) => {
                // 尝试用 root 用户获取 taosd 版本信息，上报
                let taosd_info = query_taosd_info_guess(&args).await;
                if let Some((cluster_id, taosd_version)) = taosd_info {
                    let r = verification::report_taosd_info_to_cloud(
                        args.cloud_open_api.clone(),
                        str_phone_email,
                        lang_code,
                        &cluster_id,
                        &taosd_version,
                    )
                    .await;
                    if r.is_err() {
                        log::error!(
                            "Failed to report the guessed taosd info to cloud: {:?}",
                            r.err()
                        );
                    }
                }
            }
            Ok(code) => {
                log::error!(
                    "Failed to upload verification status, response code: {}",
                    code
                );
            }
            Err(err) => {
                log::error!("Failed to upload verification status to cloud: {:?}", err);
            }
        }
    }

    HttpResponse::Ok().json(R::success(result))
}

async fn query_taosd_info_guess(args: &web::Data<Args>) -> Option<(String, String)> {
    let sql = "select id, CONCAT(server_version(), ' ', version) as version from information_schema.ins_cluster";
    match args.query_by_root(sql).await {
        Ok(ok) => {
            if let Some(taosd_info) = ok.data.first() {
                let cluster_id = taosd_info.first();
                let taosd_version = taosd_info.get(1);

                if cluster_id.is_some() && taosd_version.is_some() {
                    let cluster_id = cluster_id.unwrap().as_i64().unwrap().to_string();
                    let taosd_version = taosd_version.unwrap().as_str().unwrap().to_string();
                    return Some((cluster_id, taosd_version));
                }
            }
        }
        Err(err) => {
            log::error!("Failed to execute sql: {}, err:{:?}", sql, err);
        }
    }

    None
}

// restapi: 上报 taosd 信息
#[instrument(skip_all)]
async fn report_taosd_info(
    args: web::Data<Args>,
    body: web::Json<TaosdInfoBody>,
) -> impl Responder {
    let lang_code = match body.lang.as_deref() {
        Some("zh") => "zh_CN",
        _ => "en_US",
    };

    let report_result = verification::report_taosd_info_to_cloud(
        args.cloud_open_api.clone(),
        body.phone_email.as_ref().unwrap(),
        lang_code,
        body.cluster_id.as_ref().unwrap(),
        body.taosd_version.as_ref().unwrap(),
    )
    .await;
    if report_result.is_err() {
        log::error!(
            "Failed to report taosd info to cloud: {:?}",
            report_result.err()
        );
    }

    HttpResponse::Ok().json(R::success(""))
}

#[post("/rest/sql")]
async fn rest_sql_builtin(
    args: web::Data<Args>,
    req: HttpRequest,
    sql: String,
    query: Query<HashMap<String, String>>,
) -> impl Responder {
    let header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .unwrap_or_default();
    let tz = query.get("tz");
    match args.query(header, &sql, tz).await {
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

#[instrument(skip_all)]
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

fn real_ip_forward(req: &HttpRequest, mut builder: RequestBuilder) -> RequestBuilder {
    static X_REAL_IP: &str = "x-real-ip";
    let info = req.connection_info();
    let real_ip = info.realip_remote_addr().or(info.peer_addr());
    if !req.headers().contains_key(X_FORWARDED_FOR) && real_ip.is_some() {
        builder = builder.header(X_FORWARDED_FOR, real_ip.unwrap());
    }
    if !req.headers().contains_key(X_REAL_IP) && real_ip.is_some() {
        builder = builder.header(X_REAL_IP, real_ip.unwrap());
    }
    for (key, value) in req.headers() {
        builder = builder.header(key, value);
    }
    builder
}

async fn proxy(
    req: HttpRequest,
    payload: web::Payload,
    client: web::Data<reqwest::Client>,
    url: &str,
) -> Result<HttpResponse, actix_web::Error> {
    let mut qid = Span.get_qid::<Qid>().unwrap_or_else(Qid::init);
    qid.add_sequence_id();
    if req.headers().contains_key("upgrade") {
        // Websocket proxy.

        // Forward the request.
        let mut builder = client.get(url);
        builder = builder.headers(headers_with_qid(&qid));
        let builder = real_ip_forward(&req, builder);

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
            client_response.insert_header((header.as_str(), value.as_bytes()));
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

        debug!(url, "proxy to taosx");
        let builder = client
            .request(req.method().clone(), url)
            .headers(headers_with_qid(&qid))
            .timeout(Duration::from_secs(u64::MAX))
            .body(reqwest::Body::wrap_stream(UnboundedReceiverStream::new(rx)));
        let builder = real_ip_forward(&req, builder);
        builder
            .send()
            .await
            .map_err(error::ErrorInternalServerError)
            .map(reqwest_into_http_response)
            .inspect(|_| debug!("Got taosx proxy result"))
    }
}

#[instrument(skip_all)]
async fn modify_password(
    args: web::Data<Args>,
    _client: web::Data<reqwest::Client>,
    _path: web::Path<String>,
    req: HttpRequest,
    payload: web::Payload,
    username: web::Path<String>,
    query: Query<HashMap<String, String>>,
) -> impl Responder {
    let header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .unwrap_or_default();

    let sql = get_body_from_payload(payload).await.unwrap();
    let tz = query.get("tz");

    match args.query(header, &sql, tz).await {
        Ok(ok) => {
            // 清除 username 对应的 user_pool
            let _ = args.build_dsn(header).map(|dsn| {
                clear_pool(&dsn, username.to_string());
            });

            HttpResponse::Ok().json(ok)
        }
        Err(err) => HttpResponse::InternalServerError().json(err),
    }
}

#[instrument(skip_all)]
async fn rest_proxy(
    args: web::Data<Args>,
    _client: web::Data<reqwest::Client>,
    _path: web::Path<String>,
    req: HttpRequest,
    payload: web::Payload,
    query: Query<HashMap<String, String>>,
) -> impl Responder {
    // let x = args.profile.cluster.as_deref().unwrap();
    // let query = req.query_string();
    // let url = if query.is_empty() {
    //     format!("{x}/rest/{path}")
    // } else {
    //     format!("{x}/rest/{path}?{query}")
    // };
    // proxy(req, payload, client, &url).await.map_err(RestErrResponse::new)

    let header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .unwrap_or_default();

    let sql = get_body_from_payload(payload).await.unwrap();
    let tz = query.get("tz");
    match args.query(header, &sql, tz).await {
        Ok(ok) => HttpResponse::Ok().json(ok),
        Err(err) => HttpResponse::InternalServerError().json(err),
    }
}

async fn get_body_from_payload(mut payload: web::Payload) -> Result<String, RestErrResponse> {
    let mut bytes = web::BytesMut::new();
    while let Some(item) = payload.next().await {
        bytes.extend_from_slice(&item.unwrap());
    }
    String::from_utf8(bytes.to_vec()).map_err(|e| {
        eprintln!("Error converting body bytes to string: {:?}", e);
        RestErrResponse::new("Error converting body bytes to string")
    })
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

#[derive(Deserialize)]
struct ImportRequest {
    server: String,
    #[serde(default)]
    passwords: bool,
    #[serde(default)]
    privileges: bool,
    #[serde(default)]
    whitelist: bool,
}

#[instrument(skip_all)]
async fn import(
    args: web::Data<Args>,
    client: web::Data<reqwest::Client>,
    req: HttpRequest,
    import: web::Json<ImportRequest>,
    query: Query<HashMap<String, String>>,
) -> impl Responder {
    if args.profile.x_api.is_none() {
        return Ok(HttpResponse::NotFound().body("taosX API is required"));
    }
    let header = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|header| header.to_str().ok());
    if header.is_none() {
        return Ok(HttpResponse::Unauthorized().body("Authorization header not found"));
    }
    let header = header.unwrap();
    let tz = query.get("tz");
    match args.query(header, "select server_status()", tz).await {
        Ok(ok) => {
            if ok.code != Code::SUCCESS {
                return Ok(HttpResponse::InternalServerError().json(ok));
            }
        }
        Err(err) => return Err(RestErrResponse::new(err)),
    }
    let dsn = args.build_dsn(header)?;

    let migrate = serde_json::json!(
        {
            "from": import.server,
            "to": dsn.to_string(),
            "options": {
                "passwords": import.passwords,
                "privileges": import.privileges,
                "whitelist": import.whitelist
            }
        }
    );
    let x = args.profile.x_api.as_deref().unwrap();
    let url = format!("{x}/privileges/migrate");

    let mut qid = Span.get_qid::<Qid>().unwrap_or_else(Qid::init);
    qid.add_sequence_id();
    debug!(url, "proxy to taosx");
    client
        .post(url)
        .json(&migrate)
        .headers(headers_with_qid(&qid))
        .send()
        .await
        .map_err(RestErrResponse::new)
        .map(reqwest_into_http_response)
        .inspect(|_| debug!("Got proxy result"))
}

fn reqwest_into_http_response(res: reqwest::Response) -> HttpResponse {
    let mut client_resp = HttpResponse::build(res.status());
    for (header_name, header_value) in res.headers().iter().filter(|(h, _)| *h != "connection") {
        client_resp.insert_header((header_name.clone(), header_value.clone()));
    }
    client_resp.streaming(res.bytes_stream())
}

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

#[instrument(skip_all)]
async fn x_api_doc(
    req: HttpRequest,
    client: web::Data<reqwest::Client>,
    args: web::Data<Args>,
    payload: web::Payload,
) -> Result<HttpResponse, RestErrResponse> {
    if args.profile.x_api.is_none() {
        return Ok(HttpResponse::NotFound().finish());
    }
    let mut qid = Span.get_qid().unwrap_or_else(Qid::init);

    let x = args.profile.x_api.as_deref().unwrap();
    let url = format!("{x}/api-doc/openapi.json");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    tokio::task::spawn_local(
        async move {
            let mut payload = payload;
            while let Some(chunk) = payload.next().await {
                if let Err(err) = tx.send(chunk) {
                    tracing::warn!("Error sending payload chunk: {err}");
                }
            }
        }
        .in_current_span(),
    );

    qid.add_sequence_id();
    debug!(url, "proxy to taosx");
    let builder = client
        .request(req.method().clone(), url)
        .timeout(Duration::from_secs(u64::MAX))
        .headers(headers_with_qid(&qid))
        .body(reqwest::Body::wrap_stream(UnboundedReceiverStream::new(rx)));
    let builder = real_ip_forward(&req, builder);
    let res = builder
        .send()
        .await
        .map_err(error::ErrorInternalServerError)?;
    debug!("Got proxy result");
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

    #[clap(long, env = "EXPLORER_CLUSTER_NATIVE")]
    cluster_native: Option<String>,

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

const CLAP_SHORT_VERSION: &str = if build::GIT_CLEAN {
    const_format::concatcp!(
        "version: ",
        build::TD_VERSION,
        " (core-",
        build::PKG_VERSION,
        ")\ngit: ",
        build::COMMIT_HASH,
        "\nbuild: ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME
    )
} else {
    const_format::concatcp!(
        "version: ",
        build::TD_VERSION,
        " (core-dirty-",
        build::PKG_VERSION,
        ")\ngit: ",
        build::COMMIT_HASH,
        "\nbuild: ",
        build::BUILD_OS,
        " ",
        build::BUILD_TIME
    )
};

#[derive(Parser, Debug, Clone, Serialize, Deserialize, Default)]
#[clap(name = env!("CUS_CLI_NAME"), author, version = CLAP_SHORT_VERSION, about, long_about = include_str!(env!("CUS_README")))]
struct Args {
    /// Configuration file
    #[clap(short = 'C', long, env = "EXPLORER_CONFIG_FILE")]
    config_file: Option<PathBuf>,
    /// Port
    #[clap(short, long, global = true, env = "EXPLORER_PORT")]
    #[serde(default)]
    port: Option<u16>,

    #[clap(long, global = true, env = "EXPLORER_ADDR")]
    addr: Option<String>,

    #[clap(long, global = true, env = "EXPLORER_INSTANCE_ID")]
    #[serde(rename = "instanceId")]
    instance_id: Option<u8>,

    #[clap(flatten)]
    log: Option<LogOpts>,

    #[clap(long, global = true, env = "EXPLORER_IPV6")]
    ipv6: Option<String>,

    /// Allow all origins or not.
    #[clap(skip)]
    #[serde(default)]
    cors: Option<bool>,

    /// Static assets path for debug/test.
    #[clap(long, global = true, env = "EXPLORER_ASSETS")]
    #[serde(default)]
    assets: Option<PathBuf>,

    /// For verbosity logging.
    #[clap(flatten)]
    #[serde(skip)]
    verbose: Option<Verbosity<InfoLevel>>,

    /// For environment variable wised log level.
    #[clap(env = "EXPLORER_LOG_LEVEL", hide = true)]
    log_level: Option<LevelFilter>,

    cfg_path: Option<String>,

    #[clap(long, global = true)]
    cloud_open_api: Option<String>,

    #[clap(flatten)]
    #[serde(flatten)]
    profile: Profile,

    #[clap(flatten)]
    ssl: Option<Ssl>,

    #[clap(long, env = "EXPLORER_DATA_DIR")]
    #[serde(default)]
    data_dir: Option<String>,
}

#[serde_as]
#[derive(Parser, Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct LogOpts {
    #[clap(id = "log.path", long = "log.path", env = "EXPLORER_LOG_PATH")]
    path: Option<PathBuf>,
    #[clap(id = "log.level", long = "log.level", env = "EXPLORER_LOG_LEVEL")]
    level: Option<LevelFilter>,
    #[clap(
        id = "log.compress",
        long = "log.compress",
        env = "EXPLORER_LOG_COMPRESS",
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = compress_arg_parser,
    )]
    #[serde_as(as = "Option<FromInto<CompressType>>")]
    compress: Option<bool>,
    #[clap(
        id = "log.rotationCount",
        long = "log.rotationCount",
        env = "EXPLORER_LOG_ROTATION_COUNT"
    )]
    rotation_count: Option<u16>,
    #[clap(
        id = "log.keepDays",
        long = "log.keepDays",
        env = "EXPLORER_LOG_KEEP_DAYS"
    )]
    keep_days: Option<u16>,
    #[clap(
        id = "log.rotationSize",
        long = "log.rotationSize",
        env = "EXPLORER_LOG_ROTATION_SIZE"
    )]
    rotation_size: Option<String>,
    #[clap(
        id = "log.reservedDiskSize",
        long = "log.reservedDiskSize",
        env = "EXPLORER_LOG_RESERVED_DISK_SIZE"
    )]
    reserved_disk_size: Option<String>,
}

fn compress_arg_parser(value: &str) -> Result<bool, clap::Error> {
    match value.to_lowercase().as_str() {
        "0" | "false" => Ok(false),
        _ => Ok(true),
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
#[serde(untagged)]
enum CompressType {
    B(bool),
    N(u8),
}

impl From<CompressType> for bool {
    fn from(value: CompressType) -> Self {
        match value {
            CompressType::B(v) => v,
            CompressType::N(1) => true,
            CompressType::N(0) => false,
            _ => panic!("invalid compress value"),
        }
    }
}

impl From<bool> for CompressType {
    fn from(value: bool) -> Self {
        Self::B(value)
    }
}

impl LogOpts {
    fn merge_from(&mut self, rhs: Self) {
        macro_rules! update_if_none {
            ($field: ident) => {
                if self.$field.is_none() {
                    self.$field = rhs.$field
                }
            };
        }
        update_if_none!(path);
        update_if_none!(compress);
        update_if_none!(rotation_count);
        update_if_none!(keep_days);
        update_if_none!(rotation_size);
        update_if_none!(reserved_disk_size);
    }
}

impl Default for LogOpts {
    fn default() -> Self {
        Self {
            path: Some(get_default_log_path()),
            level: None,
            compress: Some(false),
            rotation_count: Some(30),
            keep_days: Some(30),
            rotation_size: Some("1GB".to_string()),
            reserved_disk_size: Some("1GB".to_string()),
        }
    }
}

fn get_default_log_path() -> PathBuf {
    if cfg!(windows) {
        PathBuf::from(format!("C:\\{}\\log", env!("CUS_NAME")))
    } else {
        PathBuf::from(format!("/var/log/{}", env!("CUS_PROMPT")))
    }
}

#[derive(Parser, Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
struct Ssl {
    /// SSL certificate
    #[clap(long, global = true, env = "CERTIFICATE")]
    certificate: Option<String>,

    /// SSL certificate key
    #[clap(long, global = true, env = "CERTIFICATE_KEY")]
    certificate_key: Option<String>,
}

impl Args {
    fn build_dsn(&self, auth: &str) -> Result<Dsn, RestErrResponse> {
        let credentials =
            Credentials::from_header(auth.to_string()).map_err(RestErrResponse::new)?;
        let mut dsn: Dsn = self
            .profile
            .cluster
            .as_deref()
            .unwrap_or("taos://localhost:6030")
            .parse()
            .map_err(RestErrResponse::new)?;
        dsn.username = Some(credentials.user_id);
        dsn.password = Some(credentials.password);
        Ok(dsn)
    }

    async fn query_inner(
        &self,
        dsn: Dsn,
        sql: &str,
        tz: Option<&String>,
    ) -> Result<RestOkResponse, RestErrResponse> {
        let mut qid = Span.get_qid::<Qid>().unwrap_or_else(Qid::init);
        // taos connection pool
        let conn = get_connection(&dsn).await.map_err(RestErrResponse::new)?;

        let tz = if let Some(tz) = tz {
            chrono_tz::Tz::from_str(tz).unwrap_or(chrono_tz::Tz::UTC)
        } else {
            chrono_tz::Tz::UTC
        };

        qid.add_sequence_id();
        debug!("Got connection, querying sql");
        let mut set = conn.query_with_req_id(sql, qid.get()).await?;
        debug!("Got sql result set");
        // dml and cud return empty set
        if set.fields().is_empty() {
            let affect_rows = set.affected_rows();
            return Ok(RestOkResponse {
                code: Code::SUCCESS,
                column_meta: vec![("affected_rows".to_string(), "int".to_string(), 4)],
                rows: 1,
                data: vec![vec![serde_json::Value::Number(affect_rows.into())]],
            });
        }
        let column_meta = set
            .fields()
            .iter()
            .map(|f| (f.name().to_string(), f.ty().to_string(), f.bytes()))
            .collect_vec();
        debug!("Got fields {column_meta:?}, fetching data.");
        let data = set
            .to_records()
            .await?
            .into_iter()
            .map(|row| {
                row.into_iter()
                    .map(|v| match v {
                        taos::Value::Timestamp(ts) => {
                            let ts_with_tz = tz.from_utc_datetime(&ts.to_naive_datetime());
                            serde_json::Value::String(ts_with_tz.to_rfc3339())
                        }
                        taos::Value::VarBinary(vb) => serde_json::Value::String(format!(
                            "\\x{}",
                            hex::encode(vb).to_uppercase()
                        )),
                        taos::Value::Geometry(geo) => {
                            serde_json::Value::String(parse_geometry_from_bytes(&geo))
                        }
                        _ => v.to_json_value(),
                    })
                    .collect_vec()
            })
            .collect_vec();
        debug!("SQL result: {data:?}");
        Ok(RestOkResponse {
            code: Code::SUCCESS,
            column_meta,
            rows: data.len() as _,
            data,
        })
    }

    async fn query(
        &self,
        header: &str,
        sql: &str,
        tz: Option<&String>,
    ) -> Result<RestOkResponse, RestErrResponse> {
        let dsn = self.build_dsn(header)?;
        self.query_inner(dsn, sql, tz).await
    }

    // 开源版想在登录前就获取TDengine版本信息，使用root用户尝试登录获取。
    async fn query_by_root(&self, sql: &str) -> Result<RestOkResponse, RestErrResponse> {
        let mut dsn: Dsn = self
            .profile
            .cluster
            .as_deref()
            .unwrap_or("taos://localhost:6030")
            .parse()
            .map_err(RestErrResponse::new)?;
        dsn.username = Some("root".to_string());
        dsn.password = Some("taosdata".to_string());

        self.query_inner(dsn, sql, None).await
    }

    async fn renew(
        &self,
        header: &str,
        license: &RenewLicense,
    ) -> Result<RestOkResponse, RestErrResponse> {
        let dsn = self.build_dsn(header)?;
        let conn = get_connection(&dsn).await.map_err(RestErrResponse::new)?;
        // server version
        let server_version = conn.server_version().await;
        let server_version = match server_version {
            Err(err) => {
                log::error!("Failed to get server version: {err}");
                return Err(RestErrResponse::new("Failed to get server version"));
            }
            Ok(version) => version,
        };
        let (a, b, c) = get_main_version_from_server_version(&server_version.to_string()).unwrap();
        // check version and use different function
        let mut qid: Qid = Span.get_qid().unwrap_or(Qid::init());
        if a > 3 || (a == 3 && b > 2) || (a == 3 && b == 2 && c >= 3) {
            if let Some(active_code) = license.active_code.as_ref() {
                if !active_code.is_empty() {
                    let sql = format!("alter cluster 'activeCode' '{active_code}'");
                    qid.add_sequence_id();
                    conn.exec_with_req_id(&sql, qid.get())
                        .await
                        .map_err(|err| {
                            RestErrResponse::new(format!("Invalid cluster active code: {err:#}"))
                        })?;
                }
            } else {
                return Err(RestErrResponse {
                    code: Code::FAILED,
                    desc: "active code must exist".into(),
                });
            }
        } else {
            if license.active_code.is_none() && license.c_active_code.is_none() {
                return Err(RestErrResponse {
                    code: Code::FAILED,
                    desc: "active code or connector active code must exist at lease one".into(),
                });
            }
            if let Some(active_code) = license.active_code.as_ref() {
                if !active_code.is_empty() {
                    let sql = format!("alter all dnodes 'activeCode' '{active_code}'");
                    qid.add_sequence_id();
                    debug!("exec sql");
                    conn.exec_with_req_id(&sql, qid.get())
                        .await
                        .map_err(|err| {
                            RestErrResponse::new(format!("Invalid cluster active code: {err:#}"))
                        })
                        .inspect(|_| debug!("Got sql result"))?;
                }
            }
            if let Some(c_active_code) = license.c_active_code.as_ref() {
                if !c_active_code.is_empty() {
                    let sql = format!("alter all dnodes 'cActiveCode' '{c_active_code}'");
                    qid.add_sequence_id();
                    debug!("Exec sql");
                    conn.exec_with_req_id(&sql, qid.get())
                        .await
                        .map_err(|err| {
                            RestErrResponse::new(format!("Invalid connector active code: {err:#}"))
                        })?;
                    debug!("Got sql result");
                }
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

/// Parse geometry from bytes to WKT. If failed, return the original bytes.
fn parse_geometry_from_bytes(geo: &[u8]) -> String {
    let result = Geometry::new_from_wkb(geo);
    match result {
        Ok(geo) => geo.to_wkt_precision(6).unwrap(),
        Err(_) => format!("{:?}", geo),
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

pub fn get_main_version_from_server_version(version: &String) -> anyhow::Result<(i32, i32, i32)> {
    let mut version_vec = version.splitn(4, '.').collect_vec();
    version_vec.truncate(3);
    let res = version_vec
        .into_iter()
        .map(|x| x.parse::<i32>())
        .collect_tuple();
    match res {
        Some((Ok(a), Ok(b), Ok(c))) => Ok((a, b, c)),
        _ => Err(anyhow::anyhow!("Invalid version string: {}", version)),
    }
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, str::FromStr};

    use chrono::TimeZone;
    use clap::CommandFactory;
    use log::LevelFilter;
    use taos::*;

    use crate::Args;

    #[test]
    fn test_timezone() {
        let tz = "Asia/Muscat";
        let offset = chrono_tz::Tz::from_str(tz).unwrap();
        dbg!(offset);

        let ts = taos_query::common::Timestamp::Milliseconds(1722255496870);
        let ts_with_tz = offset.from_utc_datetime(&ts.to_naive_datetime());
        dbg!(ts_with_tz);

        let str = serde_json::Value::String(ts_with_tz.to_rfc3339());
        dbg!(str);
    }

    #[test]
    fn parse_log_opts() {
        let s = r#"
            [log]
            path = "aaa"
            level = "warn"
            compress = true
            rotationCount = 33
            rotationSize = "3GB"
            reservedDiskSize = "30GB"
        "#;
        let args: Args = toml::from_str(s).unwrap();
        let log = args.log.unwrap();
        assert_eq!(log.path.unwrap(), PathBuf::from("aaa"));
        assert_eq!(log.level.unwrap(), LevelFilter::Warn);
        assert!(log.compress.unwrap());
        assert_eq!(log.rotation_count.unwrap(), 33);
        assert_eq!(log.rotation_size.unwrap(), "3GB");
        assert_eq!(log.reserved_disk_size.unwrap(), "30GB");
    }

    #[test]
    fn parse_log_opts_compress_number() {
        let s = r#"
            [log]
            path = "aaa"
            level = "warn"
            compress = 1
            rotationCount = 33
            rotationSize = "3GB"
            reservedDiskSize = "30GB"
        "#;
        let args: Args = toml::from_str(s).unwrap();
        let log = args.log.unwrap();
        assert_eq!(log.path.unwrap(), PathBuf::from("aaa"));
        assert_eq!(log.level.unwrap(), LevelFilter::Warn);
        assert!(log.compress.unwrap());
        assert_eq!(log.rotation_count.unwrap(), 33);
        assert_eq!(log.rotation_size.unwrap(), "3GB");
        assert_eq!(log.reserved_disk_size.unwrap(), "30GB");
    }

    #[test]
    fn parse_log_opts_clap() {
        let cli = format!("{}-explorer", env!("CUS_CLI_NAME"));
        let matches = Args::command()
            .try_get_matches_from([
                &cli,
                "--log.path",
                "/var/log/taos",
                "--log.level",
                "info",
                "--log.compress",
                "--log.rotationCount",
                "3",
                "--log.rotationSize",
                "3GB",
                "--log.reservedDiskSize",
                "3GB",
            ])
            .unwrap();
        assert_eq!(
            matches.get_one("log.path"),
            Some(&PathBuf::from("/var/log/taos"))
        );
        assert_eq!(matches.get_one("log.level"), Some(&log::LevelFilter::Info));
        assert_eq!(matches.get_one("log.compress"), Some(&true));
        assert_eq!(matches.get_one("log.rotationCount"), Some(&3u16));
        assert_eq!(
            matches.get_one("log.rotationSize"),
            Some(&"3GB".to_string())
        );
        assert_eq!(
            matches.get_one("log.reservedDiskSize"),
            Some(&"3GB".to_string())
        )
    }
}
