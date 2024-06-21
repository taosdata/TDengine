use actix_cors::Cors;
use actix_files::NamedFile;
use anyhow::Context;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use geos::{Geom, Geometry};
use http_auth_basic::Credentials;
use log::LevelFilter;
use reqwest::RequestBuilder;
use rustls::server::ServerConfig;
use rustls_pemfile::{certs, private_key};
use std::{
    fmt::Display,
    fs::File,
    io::{BufReader, Read},
    path::PathBuf,
    time::Duration,
};
use taos::*;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{error, info, instrument, Level};
use tracing_actix_web::TracingLogger;

use actix_embed::Embed;
use actix_web::{
    dev::{fn_service, ServiceRequest, ServiceResponse},
    error::{self, JsonPayloadError, PayloadError},
    http::header::{ContentType, AUTHORIZATION, X_FORWARDED_FOR},
    middleware::{Compress, Logger},
    post, web, App, HttpRequest, HttpResponse, HttpServer, Responder, ResponseError,
};

use clap::Parser;
use rust_embed::RustEmbed;
use serde::{Deserialize, Serialize};

pub mod verification;

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
    let log_level = args
        .log_level
        .or(args.verbose.as_ref().map(|v| v.log_level_filter()))
        .unwrap_or(LevelFilter::Info);
    args.cfg_path = Some(path);

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
    let app_args = web::Data::new(args.clone());
    let cors = app_args.cors.unwrap_or_default();

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
            .wrap(TracingLogger::default())
            .wrap(cors)
            .wrap(Logger::default())
            .wrap(Compress::default())
            .app_data(web::Data::new(reqwest::Client::new()))
            .app_data(app_args.clone())
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

#[derive(Debug, Serialize, Deserialize, Clone)]
struct R<T> {
    pub code: u32,
    pub data: Option<T>,
    pub msg: Option<String>,
}

impl<T> R<T> {
    fn success(data: T) -> Self {
        Self {
            code: 0,
            data: Some(data),
            msg: None,
        }
    }
    fn fail(code: u32, msg: String) -> Self {
        Self {
            code,
            data: None,
            msg: Some(msg),
        }
    }
}

/**
 * 检查当前 TDengine 是否已经绑定了手机号或邮箱。
 */
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

    match args.query_by_root("select CONCAT(server_version(), ' ', version) as version from information_schema.ins_cluster").await {
        Ok(ok) => {
            if let Some(version) = ok.data.get(0) {
                profile.taosd_version.replace(version.get(0).unwrap().as_str().unwrap().into());
            }
        }
        Err(err) => {
            log::error!("Failed to get taosd version: {:?}", err);
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

async fn generate_captcha_image(params: web::Query<VerificationReqBody>) -> impl Responder {
    let captcha_key = format!("captcha-{}", params.phone_email.as_ref().unwrap());
    let img = verification::generate_captcha(captcha_key);

    HttpResponse::Ok()
        .content_type("image/png")
        .body(img.unwrap())
}

// phone_email=18600000000&captcha=1234
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
        if report_result.is_err() {
            log::error!(
                "Failed to upload verification status to cloud: {:?}",
                report_result.err()
            );
        }
    }

    HttpResponse::Ok().json(R::success(result))
}

// restapi: 上报 taosd 信息
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
    if req.headers().contains_key("upgrade") {
        // Websocket proxy.

        // Forward the request.
        let builder = real_ip_forward(&req, client.get(url));

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
        let builder = client
            .request(req.method().clone(), url)
            .timeout(Duration::from_secs(u64::MAX))
            .body(reqwest::Body::wrap_stream(UnboundedReceiverStream::new(rx)));
        let builder = real_ip_forward(&req, builder);
        builder
            .send()
            .await
            .map_err(error::ErrorInternalServerError)
            .map(reqwest_into_http_response)
    }
}

async fn rest_proxy(
    args: web::Data<Args>,
    _client: web::Data<reqwest::Client>,
    _path: web::Path<String>,
    req: HttpRequest,
    payload: web::Payload,
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
    match args.query(header, &sql).await {
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
    match args.query(header, "select server_status()").await {
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

    client
        .post(url)
        .json(&migrate)
        .send()
        .await
        .map_err(RestErrResponse::new)
        .map(reqwest_into_http_response)
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
    let builder = client
        .request(req.method().clone(), url)
        .timeout(Duration::from_secs(u64::MAX))
        .body(reqwest::Body::wrap_stream(UnboundedReceiverStream::new(rx)));
    let builder = real_ip_forward(&req, builder);
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

    taosd_version: Option<String>,
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

    #[clap(long, global = true, env = "EXPLORER_ADDR")]
    addr: Option<String>,

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

    async fn query_inner(&self, dsn: Dsn, sql: &str) -> Result<RestOkResponse, RestErrResponse> {
        log::info!("SQL: {sql}");

        let conn = TaosBuilder::from_dsn(dsn)?.build().await?;

        log::info!("Got connection, querying");
        let mut set = conn.query(sql).await?;
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
        log::info!("SQL result: {data:?}");
        Ok(RestOkResponse {
            code: Code::SUCCESS,
            column_meta,
            rows: data.len() as _,
            data,
        })
    }

    async fn query(&self, header: &str, sql: &str) -> Result<RestOkResponse, RestErrResponse> {
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
        
        self.query_inner(dsn, sql).await
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
        
        self.query_inner(dsn, sql).await
    }

    async fn renew(
        &self,
        header: &str,
        license: &RenewLicense,
    ) -> Result<RestOkResponse, RestErrResponse> {
        // token
        let credentials =
            Credentials::from_header(header.to_string()).map_err(RestErrResponse::new)?;
        // connection
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
        if a > 3 || (a == 3 && b > 2) || (a == 3 && b == 2 && c >= 3) {
            if let Some(active_code) = license.active_code.as_ref() {
                if !active_code.is_empty() {
                    let sql = format!("alter cluster 'activeCode' '{active_code}'");
                    conn.exec(&sql).await.map_err(|err| {
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
                    conn.exec(&sql).await.map_err(|err| {
                        RestErrResponse::new(format!("Invalid cluster active code: {err:#}"))
                    })?;
                }
            }
            if let Some(c_active_code) = license.c_active_code.as_ref() {
                if !c_active_code.is_empty() {
                    let sql = format!("alter all dnodes 'cActiveCode' '{c_active_code}'");
                    conn.exec(&sql).await.map_err(|err| {
                        RestErrResponse::new(format!("Invalid connector active code: {err:#}"))
                    })?;
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
mod tests;
