use actix_cors::Cors;
use actix_files::NamedFile;
use actix_web::http::StatusCode;
use actix_web::{CustomizeResponder, http::header::ContentType};
use actix_web_rust_embed_responder::{EmbedResponse, IntoResponse};
use anyhow::Context;
use chrono::{SecondsFormat, TimeZone};
use clap_verbosity_flag::{InfoLevel, Verbosity};
use deadpool::managed::{Object, PoolError};
use faststr::FastStr;
use favorites::Storage;
use futures_util::StreamExt;
use geos::{Geom, Geometry};
use log::LevelFilter;
use reqwest::RequestBuilder;
use rustls::server::ServerConfig;
use rustls_pemfile::{certs, private_key};
use serde_with::{FromInto, serde_as};
use std::{
    borrow::Cow,
    collections::HashMap,
    ffi::OsString,
    fmt::Display,
    fs::File,
    io::{BufReader, Read},
    ops::Deref,
    path::PathBuf,
    str::FromStr,
    sync::{LazyLock, OnceLock},
    time::Duration,
};
use taos::{taos_query::common::RowView, *};
use taos_query::Manager;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{error, info, instrument};
use tracing_actix_web::TracingLogger;

use actix_web::{
    App, HttpRequest, HttpResponse, HttpResponseBuilder, HttpServer, Responder, ResponseError,
    body::BoxBody,
    dev::{ServiceRequest, ServiceResponse, fn_service},
    error,
    http::header::X_FORWARDED_FOR,
    middleware::Compress,
    route,
    web::{self, Query},
};
use anyhow::bail;
use awc::cookie::Cookie;
use clap::Parser;
use qid::{DEFAULT_INSTANCE_ID, INSTANCE_ID, Qid};
use rust_embed::{EmbeddedFile, RustEmbed};
use serde::{Deserialize, Serialize};
use taoslog::{
    QidManager,
    layer::TaosLayer,
    middleware::TaosRootSpanBuilder,
    utils::{QidMetadataGetter, Span},
    writer::RollingFileAppender,
};
use tracing::debug;
use tracing_subscriber::{
    filter,
    layer::{Layer, SubscriberExt},
    util::SubscriberInitExt,
};

use sql::need_limit;

use crate::{
    oauth::{SessionManager, middleware::TsdbCredential},
    security::SecurityConfig,
    utils::xor::TimeBasedXor,
    x_api::{
        agent::*, datasource::*, get_x_url, proxy::x_proxy, tasks::*, transform::*, ws::*, x_addrs,
    },
};

mod favorites;
mod monitor;
mod oauth;
mod qid;
mod security;
mod sql;
mod utils;
mod verification;
mod x_api;

litcrypt::use_litcrypt!("AeRohyohKee4saih9se7cu6ieHagh1ko");

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Clone)]
struct UserPool {
    password: String,
    pool: deadpool::managed::Pool<Manager<TaosBuilder>>,
}

static TAOS_POOL: OnceLock<scc::HashMap<String, UserPool>> = OnceLock::new();
static CONFIG_DIR: OnceLock<PathBuf> = OnceLock::new();

static EXPLORER_SKIP_REGISTER: LazyLock<bool> = LazyLock::new(|| {
    std::env::var("EXPLORER_SKIP_REGISTER").is_ok_and(|s| {
        let skip = matches!(s.as_str(), "" | "1" | "true" | "T" | "yes");
        if skip {
            tracing::info!("skip register by env");
        }
        skip
    }) || CONFIG_DIR.get().is_some_and(|config_dir| {
        std::fs::exists(config_dir.join("explorer-register.cfg")).unwrap_or_default()
    })
});

fn clear_pool(dsn: &Dsn, username: String) {
    let map = TAOS_POOL.get_or_init(scc::HashMap::new);
    let mut dsn_simple = dsn.clone();
    dsn_simple.username = Some(username);
    dsn_simple.password = None;
    tracing::info!("clear pool for {:?}", dsn_simple);
    map.remove(&dsn_simple.to_string());
}

async fn get_connection(dsn: &Dsn) -> anyhow::Result<Object<Manager<TaosBuilder>>> {
    let map = TAOS_POOL.get_or_init(scc::HashMap::new);
    let mut dsn_simple = dsn.clone();
    dsn_simple.password = None;

    let user_pool = map
        .get(&dsn_simple.to_string())
        .filter(|entry| entry.password == dsn.password.clone().unwrap_or_default())
        .map(|entry| entry.pool.clone());

    let pool_error = |err: PoolError<_>| -> anyhow::Error {
        match err {
            PoolError::Backend(inner_err) => {
                anyhow::anyhow!("failed to get {dsn} connection from pool: {inner_err:#}")
            }
            PoolError::Timeout(timeout_type) => anyhow::anyhow!(
                "Timeout {timeout_type:?} when connect to taosadapter, please check configuration item 'cluster' in explorer.toml"
            ),
            err => anyhow::anyhow!("Failed to get connection: {err:#}"),
        }
    };
    if let Some(pool) = user_pool {
        return pool.get().await.map_err(pool_error);
    }

    match taos::TaosBuilder::from_dsn(dsn) {
        Ok(builder) => {
            let pool = builder.pool();
            if pool.is_err() {
                tracing::error!("Failed to create pool: {:?}", pool.err());
                anyhow::bail!("inner error: failed to get connection pool".to_string());
            }

            let pool = pool.unwrap();
            let conn = pool.get().await.map_err(pool_error);

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
        Err(err) => {
            tracing::error!("Failed to create taosbuilder: {:?}", err);
            anyhow::bail!("inner error: failed to get connection pool");
        }
    }
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    // info!(env!("CUS_NAME"));
    dotenv::dotenv().ok();

    #[cfg(target_os = "windows")]
    let path = format!("C:\\{}\\cfg", env!("CANONICAL_CUS_NAME"));

    #[cfg(not(target_os = "windows"))]
    let path = format!("/etc/{}", env!("CUS_PROMPT"));

    let config = ConfigPath::parse();
    let file_path = if let Some(config_file) = config.config_file {
        if config_file.exists() {
            config_file
        } else {
            bail!(
                "Custom configuration file {} not found",
                config_file.display()
            );
        }
    } else {
        std::path::Path::new(&path).join("explorer.toml")
    };
    let _ = CONFIG_DIR.set(
        file_path
            .parent()
            .map_or_else(|| PathBuf::from("."), |p| p.to_path_buf()),
    );

    let mut args = if let Ok(mut file) = File::open(&file_path) {
        let mut content = String::new();
        file.read_to_string(&mut content).context(format!(
            "Failed to read configuration from {}",
            file_path.display()
        ))?;
        let mut args: Args = toml::from_str(&content).with_context(|| {
            format!("Failed to parse configuration from {}", file_path.display())
        })?;
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
    if rustls::crypto::ring::default_provider()
        .install_default()
        .is_err()
    {
        tracing::warn!("Failed to install default ring provider");
    }

    let span = tracing::info_span!("main");
    let _entered = span.enter();

    const EXPLORER_PORT: u16 = 6060;
    const EXPLORER_CLUSTER: &str = "http://localhost:6041";
    args.port.get_or_insert(EXPLORER_PORT);
    args.profile
        .cluster
        .get_or_insert(EXPLORER_CLUSTER.to_string());

    let port = args.port.unwrap();

    if let Some(ssl) = args.ssl.as_mut()
        && let Some(path) = ssl.rpc_ca_cert.clone()
    {
        let pem = tokio::fs::read(&path)
            .await
            .with_context(|| format!("read explorer rpc ca cert: {}", path.display()))?;
        ssl.rpc_ca_cert_pem = Some(std::sync::Arc::new(pem));
    }

    let monitor = monitor::Monitor::new(args.monitor.clone(), port);
    monitor.init();
    let app_args = web::Data::new(args.clone());
    let cors = app_args.cors.unwrap_or_default();

    let data_dir = match args.data_dir {
        Some(path) => path,
        None => {
            if cfg!(windows) {
                format!("C:\\{}\\data\\explorer", env!("CANONICAL_CUS_NAME"))
            } else {
                format!("/var/lib/{}/explorer", env!("CUS_PROMPT"))
            }
        }
    };
    let favorites = Storage::new(&data_dir).await?;

    // Load OAuth config from environment variables if present
    if let Some(oauth_config) = args.oauth.as_mut() {
        oauth_config.update_by_env();

        // Validate OAuth configuration
        if let Err(e) = oauth_config.validate() {
            tracing::error!("OAuth configuration validation failed: {}", e);
            anyhow::bail!("OAuth configuration error: {}", e);
        }
        if oauth_config.enabled {
            tracing::info!("OAuth 2.0/OIDC authentication is enabled");
            tracing::info!("OAuth provider: {}", oauth_config.provider);
            tracing::info!("OAuth issuer: {}", oauth_config.oidc.issuer_url);
        }
    }
    // Initialize OAuth components if enabled
    let oauth_client = if args.oauth.as_ref().is_some_and(|c| c.enabled) {
        let oauth_config = args.oauth.as_ref().unwrap();
        match oauth::OAuthClientEnum::new(oauth_config.clone()).await {
            Ok(client) => {
                tracing::info!(
                    "OAuth client initialized successfully (provider: {})",
                    oauth_config.provider
                );
                Some(client)
            }
            Err(e) => {
                tracing::error!("Failed to initialize OAuth client: {}", e);
                anyhow::bail!("OAuth initialization failed: {}", e);
            }
        }
    } else {
        None
    };

    let session_manager = oauth::SessionManager::new(
        app_args.deref().clone(),
        favorites.pool.clone(),
        args.security.load_encryption_key(),
    );
    args.session_manager = Some(session_manager.clone());

    args.data_dir = Some(data_dir);

    print_config_values(&args);

    // Start background session cleanup task if OAuth is enabled
    let session_mgr_clone = session_manager.clone();
    tokio::spawn(async move {
        tracing::info!("OAuth session cleanup task started (runs every hour)");
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(3600)); // Run every hour
        loop {
            interval.tick().await;
            if let Err(e) = session_mgr_clone.cleanup_expired_sessions().await {
                tracing::error!("Failed to cleanup expired OAuth sessions: {}", e);
            }
        }
    });

    let http_client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .http1_only()
        .build()
        .context("Failed to create reqwest client")?;
    let server = HttpServer::new(move || {
        let cors = if cors {
            Cors::default()
                .allow_any_origin()
                .allow_any_method()
                .allow_any_header()
                .supports_credentials()
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
                .supports_credentials()
                .max_age(3600)
        };
        let oauth_config = args.oauth.as_ref().cloned();
        let app = App::new()
            .wrap(TracingLogger::<TaosRootSpanBuilder<Qid>>::new())
            .wrap(cors)
            .wrap(Compress::default())
            .app_data(web::Data::new(http_client.clone()))
            .app_data(app_args.clone())
            .app_data(web::Data::new(favorites.clone()))
            .app_data(web::Data::new(session_manager.clone()))
            // .route("/", web::get().to(index))
            .route("/api/-/rest/{path:.*}", web::to(rest_proxy))
            // ===== x apis start =====
            .route("/api/x/tasks", web::get().to(get_tasks))
            .route("/api/x/tasks", web::post().to(create_task))
            .route("/api/x/tasks/export", web::get().to(export_task))
            .route("/api/x/tasks/import", web::post().to(import_task))
            .route("/api/x/tasks/{id}", web::patch().to(update_task))
            .route("/api/x/tasks/{id}", web::delete().to(delete_task))
            .route("/api/x/tasks/{id}", web::get().to(get_task))
            .route("/api/x/tasks/{id}/start", web::post().to(start_task))
            .route("/api/x/tasks/{id}/stop", web::post().to(stop_task))
            .route("/api/x/tasks/start", web::post().to(batch_start_tasks))
            .route("/api/x/tasks/stop", web::post().to(batch_stop_tasks))
            .route("/api/x/tasks/delete", web::post().to(batch_delete_tasks))
            .route(
                "/api/x/tasks/{task_id}/activities",
                web::get().to(get_task_activities),
            )
            .route(
                "/api/x/tasks/{task_id}/metrics",
                web::get().to(get_task_metrics),
            )
            .route(
                "/api/x/tasks/{task_id}/vgroup_progress",
                web::get().to(get_task_vgroup_progress),
            )
            .route(
                "/api/x/tasks/{task_id}/table_progress",
                web::get().to(get_task_table_progress),
            )
            .route("/api/x/ds/in/validate", web::post().to(validate))
            .route("/api/x/ds/in/sample", web::post().to(get_sample))
            // websockets
            .route(
                "/api/x/activities/tasks",
                web::get().to(get_ws_tasks_activities),
            )
            .route(
                "/api/x/activities/agents",
                web::get().to(get_ws_agents_activities),
            )
            .route(
                "/api/x/metrics/task/{task_id}",
                web::get().to(get_ws_metrics),
            )
            // Transform APIs
            .route("/api/x/transform/sample/flat", web::post().to(sample_flat))
            .route(
                "/api/x/transform/sample/flat/s_model/preview",
                web::post().to(stable_preview),
            )
            // agents
            .route("/api/x/agents", web::get().to(get_agents))
            .route("/api/x/agents", web::post().to(add_agent))
            .route("/api/x/agents/{agent_id}", web::get().to(get_agent))
            .route("/api/x/agents/{agent_id}", web::delete().to(del_agent))
            .route("/api/x/agents/{agent_id}", web::patch().to(edit_agent))
            .route(
                "/api/x/agents/{agent_id}/activities",
                web::get().to(agent_activities),
            )
            // others
            .route("/api/x/{api:.*}", web::to(x_proxy))
            // ====== x apis end =====
            .route("/grafana/{grafana_path:.*}", web::to(grafana_api))
            .route("/api/-/login", web::to(login))
            .route("/api/-/login/token", web::post().to(login_with_token))
            .route("/api/-/login-options", web::get().to(login_options))
            .route("/api/-/oauth/me", web::get().to(oauth::handlers::oauth_me))
            .route("/api/-/me", web::get().to(oauth::handlers::oauth_me))
            .route(
                "/api/-/logout",
                web::post().to(oauth::handlers::oauth_logout),
            )
            .route(
                "/api/-/oauth/logout",
                web::post().to(oauth::handlers::oauth_logout),
            )
            .configure(|cfg| {
                // Register OAuth routes if enabled
                if oauth_client.is_some() {
                    cfg.app_data(web::Data::new(oauth_client.clone().unwrap()))
                        .app_data(web::Data::new(oauth_config.unwrap_or_default()))
                        .route(
                            "/api/-/oauth/status",
                            web::get().to(oauth::handlers::oauth_status),
                        )
                        .route(
                            "/api/-/oauth/authorize",
                            web::get().to(oauth::handlers::oauth_authorize),
                        )
                        .route(
                            "/api/-/oauth/callback",
                            web::get().to(oauth::handlers::oauth_callback),
                        )
                        .route(
                            "/api/-/oauth/bind",
                            web::post().to(oauth::handlers::oauth_bind),
                        )
                        .route(
                            "/api/-/oauth/users",
                            web::get().to(oauth::handlers::oauth_exist_users),
                        )
                        .route(
                            "/api/-/oauth/revoke",
                            web::post().to(oauth::handlers::oauth_revoke),
                        )
                        .route(
                            "/api/-/oauth/fetch-users",
                            web::post().to(oauth::handlers::oauth_fetch_users),
                        )
                        .route(
                            "/api/-/oauth/sync-users",
                            web::post().to(oauth::handlers::oauth_sync_users),
                        );
                } else {
                    cfg.route(
                        "/api/-/oauth/{path:.*}",
                        web::to(oauth::handlers::oauth_disabled),
                    );
                }
            })
            .route(
                "/api/-/generate-token",
                web::get().to(oauth::handlers::self_provided_token),
            )
            .route("/api/-/import", web::to(import))
            .route("/api/-/license", web::to(renew_license))
            .route("/api/-/profile", web::to(profile))
            .route("/api/-/profile/totp/enable", web::post().to(totp_enable))
            .route("/api/-/profile/totp/disable", web::post().to(totp_disable))
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
            );

        if let Some(assets) = args.assets.clone() {
            let assets = assets.clone();
            app.service(
                actix_files::Files::new("/", assets)
                    .index_file("index.html")
                    .default_handler(fn_service(move |req: ServiceRequest| async {
                        let args = req.app_data::<web::Data<Args>>();
                        if args.is_none_or(|args| args.assets.is_none()) {
                            return Ok(req.error_response(error::ErrorNotFound("File not found")));
                        }
                        let assets = args.unwrap().assets.as_ref().unwrap().clone();
                        let (req, _) = req.into_parts();
                        let file = NamedFile::open_async(assets.join("index.html")).await?;
                        let res = file.into_response(&req);
                        Ok(ServiceResponse::new(req, res))
                    }))
                    .show_files_listing(),
            )
        } else {
            app.service(web::redirect("/docs", "/docs/"))
                .service(docs_assets)
                .service(web::redirect("/docs-en", "/docs-en/"))
                .service(docs_en_assets)
                .service(web::redirect("", "/"))
                .service(static_assets)
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
    fn status_code(&self) -> StatusCode {
        match self.code {
            1 => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::OK,
        }
    }

    fn error_response(&self) -> HttpResponse<BoxBody> {
        match self.code {
            1 => HttpResponse::InternalServerError().json(self.msg.clone().unwrap_or_default()),
            _ => HttpResponse::Ok().json(self),
        }
    }
}

macro_rules! x_url {
    ($args: expr, $req: expr, $api: literal) => {
        match get_x_url($args, $req, $api)
            .await
            .context("get x url error")
        {
            Ok(Some(url)) => url,
            Ok(None) => return Err(RestErrResponse::new("no available x node found")),
            Err(e) => return Err(RestErrResponse::new(e)),
        }
    };
    ($args: expr, $req: expr, $api: literal, $resp: expr) => {
        match get_x_url($args, $req, $api)
            .await
            .context("get x url error")
        {
            Ok(Some(url)) => url,
            Ok(None) => {
                tracing::error!("no available x node found");
                return Ok(HttpResponse::Ok().json($resp));
            }
            Err(e) => {
                tracing::error!("{e:#}");
                return Ok(HttpResponse::Ok().json($resp));
            }
        }
    };
}

/**
 * 检查当前 TDengine 是否已经绑定了手机号或邮箱。
 */
#[instrument(skip_all)]
async fn check_binding(args: web::Data<Args>) -> impl Responder {
    let is_bound = *EXPLORER_SKIP_REGISTER
        || args
            .cfg_path
            .as_ref()
            .zip(args.profile.cluster.as_deref())
            .and_then(|(cfg_path, server)| {
                let binding_record_file = PathBuf::from(cfg_path).join("explorer-register.cfg");
                verification::check_phone_email_verified(&binding_record_file, server)
                    .inspect_err(|err| {
                        error!(
                            "check {} in file {}, Failed to check binding: {}",
                            server,
                            binding_record_file.display(),
                            err
                        );
                    })
                    .ok()
            })
            .is_some();
    HttpResponse::Ok().json(R::success(is_bound))
}

#[instrument(skip_all)]
async fn profile(
    args: web::Data<Args>,
    req: HttpRequest,
    client: web::Data<reqwest::Client>,
) -> impl Responder {
    let mut qid = Span.get_qid::<Qid>().unwrap_or_else(Qid::init);
    qid.add_sequence_id();

    let mut profile = args.profile.clone();
    let url = x_url!(&args, &req, "profile", profile);
    tracing::debug!(url, "send request to taosx");
    let client = client
        .get(url)
        .headers(qid::headers_with_qid(&qid))
        .timeout(Duration::from_secs(10));

    if let Ok(resp) = client.send().await
        && let Ok(json) = resp.json::<serde_json::Value>().await
        && let Some(version) = json.get("version")
    {
        profile
            .version
            .replace(version.as_str().unwrap_or_default().into());
    }

    let x_addrs = match x_addrs(&args, &req).await {
        Ok(addrs) => addrs.join(","),
        Err(e) => return Err(RestErrResponse::new(e)),
    };
    profile.grpc = Some(x_addrs);

    Ok(HttpResponse::Ok().json(&profile))
}

#[derive(Debug, Deserialize)]
struct VerificationReqBody {
    phone_email: Option<String>,
    verification_code: Option<String>,
    captcha: Option<String>,
    lang: Option<String>,
    name: Option<String>,
    firstname: Option<String>,
    lastname: Option<String>,
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
    let phone_email = params.phone_email.as_deref().unwrap_or("").trim();
    if phone_email.is_empty() {
        return HttpResponse::BadRequest().json(RestErrResponse::new("phone_email is required"));
    }

    let captcha_key = format!("captcha-{phone_email}");
    let img = verification::generate_captcha(captcha_key);

    match img {
        Some(img) => HttpResponse::Ok().content_type("image/png").body(img),
        None => HttpResponse::InternalServerError()
            .json(RestErrResponse::new("Failed to generate captcha")),
    }
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
        args.cloud_open_api.as_deref(),
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
    db: web::Data<Storage>,
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
        if let Err(err) =
            verification::record_binding_phone_email(server, str_phone_email, &binding_record_file)
        {
            tracing::error!(
                "Failed to record binding phone/email {} in file {}: {}",
                str_phone_email,
                binding_record_file.display(),
                err
            );
        }

        let lang_code = match body.lang.as_deref() {
            Some("zh") => "zh_CN",
            _ => "en_US",
        };

        let mut firstname = body.firstname.as_deref();
        let mut lastname = body.lastname.as_deref();

        // 传递 name 说明前端是中文模式；
        // 如果 name 以中文字符开头，则把第一个字作为 lastname, 其余作为 firstname
        // 否则，整个 name 作为 firstname
        if body.name.is_some() {
            let name = body.name.as_deref().unwrap();
            let start_with_zh = regex::Regex::new(r"^[\u4e00-\u9fa5].+$").unwrap();
            if start_with_zh.is_match(name) {
                let ch0_len = name.chars().next().unwrap().len_utf8();
                firstname.replace(&name[ch0_len..]);
                lastname.replace(&name[0..ch0_len]);
            } else {
                firstname.replace(name);
                lastname.replace("");
            }
        }

        let report_result = verification::report_verification_status_to_cloud(
            args.cloud_open_api.as_deref(),
            str_phone_email,
            str_verification_code,
            lang_code,
            firstname.unwrap(),
            lastname.unwrap(),
        )
        .await;

        match report_result {
            Ok(200) => {
                // 尝试用 root 用户获取 taosd 版本信息，上报
                let taosd_info = query_taosd_info_guess(&args).await;
                if let Some((cluster_id, taosd_version)) = taosd_info {
                    info!(cluster_id, taosd_version, "Guessed taosd info");
                    if let Err(err) = db
                        .get_ref()
                        .upsert_registration(str_phone_email, &cluster_id, &taosd_version)
                        .await
                    {
                        error!(
                            "Failed to upsert registration for {}: {:?}",
                            str_phone_email, err
                        );
                    }
                    let r = verification::report_taosd_info_to_cloud(
                        args.cloud_open_api.as_deref(),
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

                if let (Some(cluster_id), Some(taosd_version)) = (cluster_id, taosd_version) {
                    let cluster_id = cluster_id.as_i64().unwrap().to_string();
                    let taosd_version = taosd_version.as_str().unwrap().to_string();
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
        args.cloud_open_api.as_deref(),
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

#[derive(Debug, Deserialize)]
struct RenewLicense {
    active_code: Option<String>,
    c_active_code: Option<String>,
    cls_enabled: Option<String>,
    cls_refresh_interval: Option<String>,
    cls_url: Option<String>,
    cls_license_id: Option<String>,
    cls_quota_slot_id: Option<String>,
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
        let r = match (&self.c_active_code, &other.c_active_code) {
            (Some(l), Some(r)) => l == r,
            _ => true,
        };
        if !r {
            return false;
        }
        let cls_enabled = match (&self.cls_enabled, &other.cls_enabled) {
            (Some(l), Some(r)) => l == r,
            _ => true,
        };
        if !cls_enabled {
            return false;
        }
        let cls_refresh_interval = match (&self.cls_refresh_interval, &other.cls_refresh_interval) {
            (Some(l), Some(r)) => l == r,
            _ => true,
        };
        if !cls_refresh_interval {
            return false;
        }
        let cls_url = match (&self.cls_url, &other.cls_url) {
            (Some(l), Some(r)) => l == r,
            _ => true,
        };
        if !cls_url {
            return false;
        }
        let cls_license_id = match (&self.cls_license_id, &other.cls_license_id) {
            (Some(l), Some(r)) => l == r,
            _ => true,
        };
        if !cls_license_id {
            return false;
        }
        match (&self.cls_quota_slot_id, &other.cls_quota_slot_id) {
            (Some(l), Some(r)) => l == r,
            _ => true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RenewLicenseMode {
    Classic,
    Cls,
}

impl RenewLicense {
    fn mode(&self) -> RenewLicenseMode {
        if self.cls_enabled.is_some()
            || self.cls_refresh_interval.is_some()
            || self.cls_url.is_some()
            || self.cls_license_id.is_some()
            || self.cls_quota_slot_id.is_some()
        {
            RenewLicenseMode::Cls
        } else {
            RenewLicenseMode::Classic
        }
    }
}

fn normalize_license_field(value: &Option<String>) -> Option<String> {
    value
        .as_ref()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('\'', "''")
}

const DEFAULT_CLS_QUOTA_SLOT_ID: &str = "tsdb-1";

fn build_cls_license_sqls(license: &RenewLicense) -> Result<Vec<String>, RestErrResponse> {
    let cls_enabled = normalize_license_field(&license.cls_enabled).ok_or(RestErrResponse {
        code: Code::FAILED,
        desc: "cls_enabled is required for CLS activation".into(),
    })?;
    let normalize_optional = |value: &Option<String>| {
        value
            .as_ref()
            .map(|value| value.trim().to_string())
            .unwrap_or_default()
    };
    let cls_refresh_interval = normalize_optional(&license.cls_refresh_interval);
    let cls_url = normalize_optional(&license.cls_url);
    let cls_license_id = normalize_optional(&license.cls_license_id);
    let cls_quota_slot_id = normalize_license_field(&license.cls_quota_slot_id)
        .unwrap_or_else(|| DEFAULT_CLS_QUOTA_SLOT_ID.to_string());

    Ok(vec![
        format!(
            "alter all dnodes 'clsRefreshInterval' '{}'",
            escape_sql_string(&cls_refresh_interval)
        ),
        format!(
            "alter all dnodes 'clsUrl' '{}'",
            escape_sql_string(&cls_url)
        ),
        format!(
            "alter all dnodes 'clsLicenseId' '{}'",
            escape_sql_string(&cls_license_id)
        ),
        format!(
            "alter all dnodes 'clsQuotaSlotId' '{}'",
            escape_sql_string(&cls_quota_slot_id)
        ),
        format!(
            "alter all dnodes 'clsEnabled' '{}'",
            escape_sql_string(&cls_enabled)
        ),
    ])
}

#[instrument(skip_all)]
async fn renew_license(
    args: web::Data<Args>,
    req: HttpRequest,
    body: web::Json<RenewLicense>,
) -> impl Responder {
    let auth = oauth::middleware::extract_auth_from_request(&req)
        .await
        .and_then(|auth| auth.ok_or_else(|| "No credentials found in header".to_string()));
    match auth {
        Ok(auth) => match args.renew(&auth, &body).await {
            Ok(ok) => HttpResponse::Ok().json(ok),
            Err(err) => HttpResponse::InternalServerError().json(err),
        },
        Err(err) => HttpResponse::Unauthorized().json(R::<()>::fail(401, err)),
    }
}

fn real_ip_forward(req: &HttpRequest, mut builder: RequestBuilder) -> RequestBuilder {
    static X_REAL_IP: &str = "x-real-ip";
    let info = req.connection_info();
    let real_ip = info.realip_remote_addr().or(info.peer_addr());
    if !req.headers().contains_key(X_FORWARDED_FOR)
        && let Some(real_ip) = real_ip
    {
        builder = builder.header(X_FORWARDED_FOR.as_str(), real_ip);
    }
    if !req.headers().contains_key(X_REAL_IP)
        && let Some(real_ip) = real_ip
    {
        builder = builder.header(X_REAL_IP, real_ip);
    }
    for (key, value) in req.headers() {
        // Convert header name/value to string form to avoid mixing http crate versions
        builder = builder.header(key.as_str(), value.to_str().unwrap_or_default());
    }
    builder
}

async fn proxy(
    req: HttpRequest,
    mut payload: web::Payload,
    client: &reqwest::Client,
    url: &str,
    append_headers: Option<reqwest::header::HeaderMap>,
) -> Result<HttpResponse, actix_web::Error> {
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    tokio::task::spawn_local(async move {
        while let Some(chunk) = payload.next().await {
            if let Err(err) = tx.send(chunk) {
                tracing::warn!("Error sending payload chunk: {err}");
            }
        }
    });

    debug!(url, "proxy to taosx");
    let method: reqwest::Method = req
        .method()
        .as_str()
        .parse()
        .unwrap_or(reqwest::Method::GET);
    let mut builder = client
        .request(method, url)
        .timeout(Duration::from_secs(u64::MAX))
        .body(reqwest::Body::wrap_stream(UnboundedReceiverStream::new(rx)));
    if let Some(headers) = append_headers {
        builder = builder.headers(headers);
    }
    builder = real_ip_forward(&req, builder);

    builder
        .send()
        .await
        .map_err(error::ErrorInternalServerError)
        .map(reqwest_into_http_response)
        .inspect(|_| debug!("Got taosx proxy result"))
}

#[instrument(skip_all)]
async fn modify_password(
    args: web::Data<Args>,
    _client: web::Data<reqwest::Client>,
    _path: web::Path<String>,
    req: HttpRequest,
    payload: web::Payload,
    query: Query<HashMap<String, String>>,
) -> impl Responder {
    let auth = oauth::middleware::extract_auth_from_request(&req)
        .await
        .and_then(|auth| auth.ok_or_else(|| "No credentials found in headers".to_string()));
    match auth {
        Ok(auth) => {
            let sql = get_body_from_payload(payload).await.unwrap();
            let tz = query.get("tz");

            match args.query(&auth, &sql, tz).await {
                Ok(ok) => {
                    // 清除 username 对应的 user_pool
                    let _ = args.build_dsn(&auth).map(|dsn| {
                        clear_pool(&dsn, auth.username.to_string());
                    });

                    HttpResponse::Ok().json(ok)
                }
                Err(err) => HttpResponse::InternalServerError().json(err),
            }
        }
        Err(err) => {
            return HttpResponse::Unauthorized().json(RestErrResponse::new(err));
        }
    }
}
#[derive(Debug, Deserialize)]
struct LoginBody {
    username: String,
    #[serde(default)]
    encrypted_password: Option<String>,
    #[serde(default)]
    password: Option<String>,
    #[serde(default)]
    captcha: Option<String>,
    #[serde(default)]
    totp_code: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginOptions {
    captcha_enabled: bool,
}

const CLOCK_OUT_OF_SYNC_DESC: &str = "clockOutOfSync";
const INVALID_ENCRYPTED_PASSWORD_DESC: &str = "invalidEncryptedPassword";

fn encrypted_password_error_response(err: &utils::xor::XorError) -> RestErrResponse {
    let desc = match err {
        utils::xor::XorError::Expired => CLOCK_OUT_OF_SYNC_DESC,
        utils::xor::XorError::Base64(_)
        | utils::xor::XorError::Utf8(_)
        | utils::xor::XorError::InvalidData
        | utils::xor::XorError::InvalidTimestamp => INVALID_ENCRYPTED_PASSWORD_DESC,
    };
    RestErrResponse::new(desc)
}

fn log_encrypted_password_login_failure(target: &str, err: &utils::xor::XorError) {
    error!(
        "Login failed, target: {}, errno: 0x{:04X}({})",
        target,
        err.code(),
        err
    );
}

#[instrument(skip_all)]
async fn login_options(args: web::Data<Args>) -> impl Responder {
    HttpResponse::Ok().json(R::success(LoginOptions {
        captcha_enabled: args.security.login_captcha_enabled(),
    }))
}

#[instrument(skip_all)]
async fn login(
    db: web::Data<Storage>,
    args: web::Data<Args>,
    session_manager: web::Data<SessionManager>,
    query: Query<HashMap<String, String>>,
    body: web::Json<LoginBody>,
) -> impl Responder {
    let xor_decoder = TimeBasedXor::new(args.security.xor_allowed_duration_secs());
    let body = body.into_inner();

    let username = body.username.trim().to_string();

    if args.security.login_captcha_enabled() {
        let captcha = body.captcha.as_deref().unwrap_or("").trim();
        if captcha.is_empty() {
            return HttpResponse::Unauthorized().json(RestErrResponse::new("captchaRequired"));
        }

        // Reuse the same captcha key format as the registration flow (/api/-/captcha).
        // That endpoint uses: key = format!("captcha-{}", phone_email)
        let captcha_key = format!("captcha-{username}");
        let captcha_check_result = verification::check_security_code(&captcha_key, captcha);
        if captcha_check_result != "pass" {
            return HttpResponse::Unauthorized().json(RestErrResponse::new("captchaInputError"));
        }
    }

    let is_plain_password = body.password.is_some();
    let password = if let Some(password) = body.password {
        // Plain password provided (for programmatic token acquisition)
        password
    } else if let Some(encrypted_password) = body.encrypted_password {
        // XOR-encrypted password (from web UI)
        match xor_decoder.decrypt(&encrypted_password) {
            Ok(password) => password,
            Err(err) => {
                log_encrypted_password_login_failure(&username, &err);
                return HttpResponse::Unauthorized().json(encrypted_password_error_response(&err));
            }
        }
    } else {
        return HttpResponse::Unauthorized().json(RestErrResponse::new(
            "Either password or encrypted_password is required",
        ));
    };
    let auth = TsdbCredential::basic(username, password);

    let tz = query.get("tz");
    let sql = "select server_version()";

    // Build DSN with optional TOTP code
    let dsn = match args.build_dsn_with_totp(&auth, body.totp_code.as_deref()) {
        Ok(dsn) => dsn,
        Err(err) => return HttpResponse::InternalServerError().json(err),
    };

    // Clear cached connection pool for this user to force fresh authentication.
    // This ensures TOTP-enabled users can't bypass verification via stale connections.
    clear_pool(&dsn, auth.username.clone());

    match args.query_with_dsn(&dsn, sql, tz).await {
        Ok(mut ok) => {
            let mut resp = HttpResponseBuilder::new(StatusCode::OK);

            // Extract server_version from query result for simplified response
            let server_version = ok
                .data
                .first()
                .and_then(|row| row.first())
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            if !is_plain_password {
                if *EXPLORER_SKIP_REGISTER {
                    ok.registered_user
                        .replace(FastStr::from_static_str("skipped"));
                } else if db.is_registered().await {
                    if let Some(subject) = favorites::TAOSX_VERIFICATION_SUBJECT.get() {
                        tracing::trace!(
                            subject = subject.as_str(),
                            "Append x-registered-user header"
                        );
                        ok.registered_user.replace(subject.clone());
                    } else {
                        tracing::error!("Expect verification subject exist");
                    }
                }
            }

            // For TOTP-authenticated logins, use taosx token for session so subsequent
            // REST proxy calls don't need TOTP codes (which expire every 30s).
            let session_auth = if body.totp_code.is_some() {
                // Ensure taosx token exists (create if needed using TOTP-authenticated DSN)
                if let Err(e) =
                    create_taosx_token_if_needed(&args, &dsn, &session_manager, &auth.username)
                        .await
                {
                    tracing::error!(
                        "Failed to create taosx token for {}: {:#}",
                        auth.username,
                        e
                    );
                }
                // Retrieve the stored taosx token
                match session_manager.get_taosx_token(&auth.username).await {
                    Ok(Some(token_value)) => {
                        tracing::info!(
                            "Using taosx token for TOTP user session: {}",
                            auth.username
                        );
                        TsdbCredential::basic(
                            auth.username.clone(),
                            format!("__token__{}", token_value),
                        )
                    }
                    _ => {
                        tracing::warn!(
                            "No taosx token found for TOTP user {}, session may not work for queries",
                            auth.username
                        );
                        auth.clone()
                    }
                }
            } else {
                auth.clone()
            };

            // Create session for basic auth and set session_id cookie
            const BASIC_AUTH_TTL: i64 = 3600; // 1 hour session expiration
            match session_manager
                .create_self_provided_session(&session_auth, Some(BASIC_AUTH_TTL))
                .await
            {
                Ok(session) => {
                    let session_id = session.session_id();
                    tracing::info!("Created basic auth session: {}", session_id);

                    ok.token.replace(session_id.to_string());

                    // Create a HttpOnly, Secure session cookie for session_id
                    let session_cookie = Cookie::build("session_id", session_id)
                        .path("/")
                        .http_only(true)
                        .same_site(awc::cookie::SameSite::Lax)
                        .max_age(actix_web::cookie::time::Duration::seconds(BASIC_AUTH_TTL))
                        .finish();

                    resp.append_header(("X-Explorer-Version", build::PKG_VERSION))
                        .cookie(session_cookie);

                    if is_plain_password {
                        return resp.json(serde_json::json!({
                            "code": 0,
                            "token": session_id,
                            "server_version": server_version,
                        }));
                    }
                }
                Err(e) => {
                    tracing::error!("Failed to create basic auth session: {:#}", e);
                    // Continue without session cookie for backward compatibility
                    resp.append_header(("X-Explorer-Version", build::PKG_VERSION));
                }
            }

            resp.json(ok)
        }
        Err(err) => {
            // Check if TSDB requires TOTP verification (TSDB_CODE_MND_WRONG_TOTP_CODE)
            let err_desc_lower = err.desc.to_lowercase();
            if err_desc_lower.contains("totp")
                || err_desc_lower.contains("two-factor")
                || err_desc_lower.contains("mfa")
            {
                tracing::info!("User requires TOTP verification");
                return HttpResponse::Unauthorized().json(serde_json::json!({
                    "code": err.code,
                    "desc": err.desc,
                    "need_totp": true,
                }));
            }
            tracing::error!("Failed to authenticate user: {:#}", err);
            if err.desc.contains("[0x0357]") {
                HttpResponse::Unauthorized().json(err)
            } else {
                HttpResponse::InternalServerError().json(err)
            }
        }
    }
}

// ========== Token Login ==========

#[derive(Debug, Deserialize)]
struct TokenLoginBody {
    token: String,
}

#[instrument(skip_all)]
async fn login_with_token(
    args: web::Data<Args>,
    session_manager: web::Data<SessionManager>,
    body: web::Json<TokenLoginBody>,
) -> impl Responder {
    let token_str = body.into_inner().token.trim().to_string();
    if token_str.is_empty() {
        return HttpResponse::BadRequest().json(RestErrResponse::new("Token is required"));
    }

    // Build DSN with token parameter
    let dsn = match args.build_dsn_with_token(&token_str) {
        Ok(dsn) => dsn,
        Err(err) => return HttpResponse::InternalServerError().json(err),
    };

    // Attempt connection with token
    match args
        .query_with_dsn(&dsn, "SELECT current_user()", None)
        .await
    {
        Ok(ok) => {
            // Extract current_user() from query result (format: "username@host")
            let current_user_raw = ok
                .data
                .first()
                .and_then(|row| row.first())
                .and_then(|v| v.as_str())
                .unwrap_or("");

            // Strip host part: "abc@zyyang" → "abc"
            let current_user = current_user_raw
                .split('@')
                .next()
                .unwrap_or(current_user_raw)
                .to_string();

            if current_user.is_empty() {
                return HttpResponse::Unauthorized()
                    .json(RestErrResponse::new("Failed to determine user from token"));
            }

            // Create session: use token as password placeholder since we don't have the real password
            let auth = TsdbCredential::basic(current_user.clone(), format!("__token__{token_str}"));
            match session_manager
                .create_self_provided_session(&auth, Some(3600))
                .await
            {
                Ok(session) => {
                    let session_id = session.session_id();
                    tracing::info!("Created token login session for user: {}", current_user);

                    let session_cookie = Cookie::build("session_id", session_id)
                        .path("/")
                        .http_only(true)
                        .same_site(awc::cookie::SameSite::Lax)
                        .max_age(actix_web::cookie::time::Duration::seconds(3600))
                        .finish();

                    HttpResponse::Ok()
                        .append_header(("X-Explorer-Version", build::PKG_VERSION))
                        .cookie(session_cookie)
                        .json(serde_json::json!({
                            "code": 0,
                            "token": session_id,
                            "username": current_user,
                        }))
                }
                Err(e) => {
                    tracing::error!("Failed to create token login session: {:#}", e);
                    HttpResponse::InternalServerError().json(RestErrResponse::new(format!(
                        "Failed to create session: {e:#}"
                    )))
                }
            }
        }
        Err(err) => {
            tracing::warn!("Token login failed: {:#}", err);
            HttpResponse::Unauthorized().json(serde_json::json!({
                "code": err.code,
                "desc": "Invalid or expired token",
            }))
        }
    }
}

// ========== TOTP Enable/Disable ==========

#[derive(Debug, Deserialize)]
struct TotpEnableBody {
    #[serde(default)]
    totp_code: Option<String>,
    /// Encrypted password (required for step 2 verification when session uses token auth)
    #[serde(default)]
    encrypted_password: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TotpDisableBody {
    totp_code: String,
    /// Encrypted password (required to verify TOTP code via user:pass+totp DSN)
    encrypted_password: String,
}

/// Helper: get TSDB credentials from current session.
async fn get_session_credentials(
    req: &HttpRequest,
    session_manager: &SessionManager,
) -> Result<(String, String), HttpResponse> {
    let session_id = oauth::middleware::extract_session_id_from_request(req).ok_or_else(|| {
        HttpResponse::Unauthorized().json(RestErrResponse::new("No session found"))
    })?;

    let session = session_manager
        .verify_session(&session_id)
        .await
        .map_err(|e| {
            HttpResponse::InternalServerError()
                .json(RestErrResponse::new(format!("Session error: {e:#}")))
        })?
        .ok_or_else(|| {
            HttpResponse::Unauthorized().json(RestErrResponse::new("Invalid or expired session"))
        })?;

    let username = session
        .get_tsdb_username()
        .ok_or_else(|| {
            HttpResponse::InternalServerError()
                .json(RestErrResponse::new("No TSDB username in session"))
        })?
        .to_string();

    let password = session_manager
        .get_decrypted_tsdb_password(&session)
        .map_err(|e| {
            HttpResponse::InternalServerError().json(RestErrResponse::new(format!(
                "Failed to decrypt credentials: {e:#}"
            )))
        })?
        .ok_or_else(|| {
            HttpResponse::InternalServerError()
                .json(RestErrResponse::new("No TSDB password in session"))
        })?;

    Ok((username, password))
}

#[instrument(skip_all)]
async fn totp_enable(
    args: web::Data<Args>,
    session_manager: web::Data<SessionManager>,
    req: HttpRequest,
    body: web::Json<TotpEnableBody>,
) -> impl Responder {
    let (username, password) = match get_session_credentials(&req, &session_manager).await {
        Ok(creds) => creds,
        Err(resp) => return resp,
    };

    let body = body.into_inner();

    if let Some(totp_code) = body.totp_code {
        // Step 2: Verify TOTP binding by connecting with user:pass + totp_code.
        // We must use the real password (not __token__) so TSDB actually validates the TOTP code.
        let real_password = if let Some(ref encrypted) = body.encrypted_password {
            let xor = TimeBasedXor::new(args.security.xor_allowed_duration_secs());
            match xor.decrypt(encrypted) {
                Ok(p) => p,
                Err(err) => {
                    return HttpResponse::BadRequest()
                        .json(encrypted_password_error_response(&err));
                }
            }
        } else if !password.starts_with("__token__") {
            // Session has real password (non-TOTP user enabling for the first time)
            password.clone()
        } else {
            return HttpResponse::BadRequest().json(RestErrResponse::new(
                "Password is required for TOTP verification",
            ));
        };

        let verify_auth = TsdbCredential::basic(username.clone(), real_password);
        let dsn = match args.build_dsn_with_totp(&verify_auth, Some(&totp_code)) {
            Ok(dsn) => dsn,
            Err(err) => return HttpResponse::InternalServerError().json(err),
        };

        match args.query_with_dsn(&dsn, "SELECT 1", None).await {
            Ok(_) => {
                tracing::info!("TOTP binding verified for user: {}", username);

                // Auto-create taosx token for this user
                if let Err(e) =
                    create_taosx_token_if_needed(&args, &dsn, &session_manager, &username).await
                {
                    tracing::error!("Failed to create taosx token for {}: {:#}", username, e);
                    // Don't fail the TOTP binding — the taosx token is a convenience feature
                }

                HttpResponse::Ok().json(R::<()>::success(()))
            }
            Err(err) => {
                tracing::warn!("TOTP verification failed for user {}: {:#}", username, err);
                HttpResponse::BadRequest().json(serde_json::json!({
                    "code": err.code,
                    "desc": "TOTP verification failed: invalid code",
                }))
            }
        }
    } else {
        // Step 1: Generate TOTP secret (uses session credentials — token auth is fine here,
        // we're just executing a SQL command, not verifying a TOTP code)
        let auth = TsdbCredential::basic(username.clone(), password.clone());
        let sql = format!("CREATE TOTP_SECRET FOR USER {}", username);
        match args.query(&auth, &sql, None).await {
            Ok(ok) => {
                let secret = ok
                    .data
                    .first()
                    .and_then(|row| row.first())
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                if secret.is_empty() {
                    return HttpResponse::InternalServerError()
                        .json(RestErrResponse::new("Failed to get TOTP secret from TSDB"));
                }

                HttpResponse::Ok().json(serde_json::json!({
                    "code": 0,
                    "data": {
                        "secret": secret,
                        "uri": format!(
                            "otpauth://totp/TDengine:{}?secret={}&issuer=TDengine",
                            username, secret
                        ),
                    },
                }))
            }
            Err(err) => {
                tracing::error!("Failed to create TOTP secret for {}: {:#}", username, err);
                HttpResponse::InternalServerError().json(err)
            }
        }
    }
}

/// Auto-create taosx token when TOTP is enabled, if not already existing.
async fn create_taosx_token_if_needed(
    args: &Args,
    dsn: &Dsn,
    session_manager: &SessionManager,
    username: &str,
) -> anyhow::Result<()> {
    let token_name = format!("__taosx_{}__", username);

    // Check if we already have this token in SQLite AND it works (no PROVIDER restriction)
    if let Some(token_value) = session_manager.get_taosx_token(username).await? {
        // Verify the token actually works by testing a connection
        let test_dsn_result = args.build_dsn_with_token(&token_value);
        if let Ok(test_dsn) = test_dsn_result
            && args
                .query_with_dsn(&test_dsn, "SELECT 1", None)
                .await
                .is_ok()
        {
            tracing::debug!("taosx token verified for user: {}", username);
            return Ok(());
        }
        // Token exists but doesn't work — drop and recreate
        tracing::warn!(
            "taosx token for user {} exists but is invalid, recreating",
            username
        );
        let drop_sql = format!("DROP TOKEN IF EXISTS {}", token_name);
        let _ = args.query_with_dsn(dsn, &drop_sql, None).await;
        session_manager.delete_taosx_token(username).await?;
    }

    // Check if it exists in TSDB (might be a stale/restricted token)
    let check_sql = format!(
        "SELECT name FROM information_schema.ins_tokens WHERE name = '{}' AND `user` = '{}'",
        token_name, username
    );
    let existing = args.query_with_dsn(dsn, &check_sql, None).await;
    if let Ok(ref res) = existing
        && !res.data.is_empty()
    {
        // Drop old token (may have PROVIDER restriction)
        tracing::info!("Dropping old taosx token in TSDB for user: {}", username);
        let drop_sql = format!("DROP TOKEN IF EXISTS {}", token_name);
        let _ = args.query_with_dsn(dsn, &drop_sql, None).await;
    }

    // Create the token in TSDB (no PROVIDER restriction to allow full user privileges)
    let create_sql = format!(
        "CREATE TOKEN IF NOT EXISTS {} FROM USER {} ENABLE 1 TTL 0 EXTRA_INFO '__auto__'",
        token_name, username
    );
    let result = args
        .query_with_dsn(dsn, &create_sql, None)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create taosx token in TSDB: {}", e.desc))?;

    let token_value = result
        .data
        .first()
        .and_then(|row| row.first())
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("TSDB did not return token value"))?;

    // Encrypt and store in SQLite
    session_manager
        .store_taosx_token(username, &token_name, token_value)
        .await?;

    tracing::info!("Created and stored taosx token for user: {}", username);
    Ok(())
}

#[instrument(skip_all)]
async fn totp_disable(
    args: web::Data<Args>,
    session_manager: web::Data<SessionManager>,
    req: HttpRequest,
    body: web::Json<TotpDisableBody>,
) -> impl Responder {
    let (username, _session_password) = match get_session_credentials(&req, &session_manager).await
    {
        Ok(creds) => creds,
        Err(resp) => return resp,
    };

    let body = body.into_inner();
    let totp_code = body.totp_code.trim().to_string();
    if totp_code.is_empty() {
        return HttpResponse::BadRequest().json(RestErrResponse::new("TOTP code is required"));
    }

    // Decrypt the user-provided password so TSDB actually validates the TOTP code.
    // Using session password (__token__) would bypass TOTP verification via bearer_token priority.
    let xor = TimeBasedXor::new(args.security.xor_allowed_duration_secs());
    let real_password = match xor.decrypt(&body.encrypted_password) {
        Ok(p) => p,
        Err(err) => {
            return HttpResponse::BadRequest().json(encrypted_password_error_response(&err));
        }
    };

    let auth = TsdbCredential::basic(username.clone(), real_password);

    // Verify TOTP code by connecting with user:pass + totp_code
    let dsn = match args.build_dsn_with_totp(&auth, Some(&totp_code)) {
        Ok(dsn) => dsn,
        Err(err) => return HttpResponse::InternalServerError().json(err),
    };

    // Use the verified connection to execute DROP TOTP_SECRET
    let drop_sql = format!("DROP TOTP_SECRET FROM USER {}", username);
    match args.query_with_dsn(&dsn, &drop_sql, None).await {
        Ok(_) => {
            tracing::info!("TOTP disabled for user: {}", username);
            HttpResponse::Ok().json(R::<()>::success(()))
        }
        Err(err) => {
            tracing::warn!("Failed to disable TOTP for {}: {:#}", username, err);
            let err_lower = err.desc.to_lowercase();
            if err_lower.contains("totp") {
                HttpResponse::BadRequest().json(serde_json::json!({
                    "code": err.code,
                    "desc": "TOTP verification failed: invalid code",
                }))
            } else {
                HttpResponse::InternalServerError().json(err)
            }
        }
    }
}

#[instrument(skip_all)]
async fn rest_proxy(
    db: web::Data<Storage>,
    args: web::Data<Args>,
    req: HttpRequest,
    payload: web::Payload,
    query: Query<HashMap<String, String>>,
) -> impl Responder {
    let auth = oauth::middleware::extract_auth_from_request(&req)
        .await
        .and_then(|auth| auth.ok_or_else(|| "No credentials found in headers".to_string()));
    if auth.is_err() {
        return HttpResponse::Unauthorized().json(RestErrResponse::new(auth.err().unwrap()));
    }
    let auth = auth.unwrap();

    // Check if XOR encryption is enabled via X-Enable-Xor header
    let enable_xor = req
        .headers()
        .get("X-Enable-Xor")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    let body = get_body_from_payload(payload).await.unwrap();

    // Decrypt SQL if XOR is enabled
    let sql = if enable_xor {
        // Use TimeBasedXor with 300 seconds (5 minutes) validity
        let xor = TimeBasedXor::new(300);
        match xor.decrypt(&body) {
            Ok(decrypted) => {
                if decrypted.is_empty() {
                    return HttpResponse::BadRequest().json(RestErrResponse {
                        code: Code::new(0x2703),
                        desc: "Decrypted SQL is empty".to_string(),
                    });
                }
                tracing::debug!("XOR decryption successful");
                decrypted
            }
            Err(e) => {
                let (code, desc) = match e {
                    utils::xor::XorError::Base64(_) => {
                        (0x2701, "Invalid Base64 encoding in request body")
                    }
                    utils::xor::XorError::Utf8(_) => {
                        (0x2702, "Decrypted content is not valid UTF-8")
                    }
                    utils::xor::XorError::InvalidData => {
                        (0x2701, "Invalid XOR encrypted data format")
                    }
                    utils::xor::XorError::InvalidTimestamp => {
                        (0x2701, "Invalid timestamp in XOR encrypted data")
                    }
                    utils::xor::XorError::Expired => (0x2701, "XOR encrypted data has expired"),
                };
                tracing::warn!("XOR decryption failed: {}", e);
                return HttpResponse::BadRequest().json(RestErrResponse {
                    code: Code::new(code),
                    desc: desc.to_string(),
                });
            }
        }
    } else {
        body
    };
    let tz = query.get("tz");
    match args.query(&auth, &sql, tz).await {
        Ok(ok) => {
            let mut resp = HttpResponseBuilder::new(StatusCode::OK);
            if db.is_registered().await {
                if let Some(subject) = favorites::TAOSX_VERIFICATION_SUBJECT.get() {
                    let subject = subject.as_str();
                    tracing::trace!(subject, "Append x-registered-user header");
                    resp.append_header(("X-Registered-User", subject));
                } else {
                    tracing::error!("Expect verification subject exist");
                }
            }
            resp.append_header(("X-Explorer-Version", build::PKG_VERSION));
            resp.json(ok)
        }
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
    let auth = oauth::middleware::extract_auth_from_request(&req)
        .await
        .and_then(|auth| auth.ok_or_else(|| "No credentials found in headers".to_string()));
    if auth.is_err() {
        return Err(RestErrResponse::new(auth.err().unwrap()));
    }
    let auth = auth.unwrap();
    let tz = query.get("tz");
    match args.query(&auth, "select server_status()", tz).await {
        Ok(ok) => {
            if ok.code != Code::SUCCESS {
                return Ok(HttpResponse::InternalServerError().json(ok));
            }
        }
        Err(err) => return Err(RestErrResponse::new(err)),
    }
    let dsn = args.build_dsn(&auth)?;

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
    let url = x_url!(&args, &req, "privileges/migrate");

    let mut qid = Span.get_qid::<Qid>().unwrap_or_else(Qid::init);
    qid.add_sequence_id();
    client
        .post(url)
        .json(&migrate)
        .headers(qid::headers_with_qid(&qid))
        .send()
        .await
        .map_err(RestErrResponse::new)
        .map(reqwest_into_http_response)
        .inspect(|_| debug!("Got proxy result"))
}

fn reqwest_into_http_response(res: reqwest::Response) -> HttpResponse {
    // Convert reqwest status to actix StatusCode to avoid http crate version mismatch
    let status =
        StatusCode::from_u16(res.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut client_resp = HttpResponse::build(status);
    for (header_name, header_value) in res.headers().iter().filter(|(h, _)| *h != "connection") {
        client_resp.insert_header((
            header_name.as_str(),
            header_value.to_str().unwrap_or_default(),
        ));
    }
    client_resp.streaming(res.bytes_stream())
}

static GRAFANA_API: OnceLock<String> = OnceLock::new();

#[instrument(skip_all)]
async fn grafana_api(
    args: web::Data<Args>,
    client: web::Data<reqwest::Client>,
    grafana_path: web::Path<String>,
    req: HttpRequest,
    payload: web::Payload,
) -> impl Responder {
    tracing::trace!("proxy grafana: {:?}", grafana_path);
    let grafana = args.profile.grafana.as_ref();
    if grafana.is_none()
        || grafana.unwrap().token.as_ref().is_none()
        || grafana.unwrap().dashboards.as_ref().is_none()
        || grafana.unwrap().dashboards.as_ref().unwrap().is_empty()
    {
        tracing::error!("Grafana API is required");
        return Ok(HttpResponse::NotFound().finish());
    }

    let grafana_api = GRAFANA_API.get_or_init(|| {
        let dashboards = grafana.unwrap().dashboards.as_ref().unwrap();
        let url = dashboards.values().next().unwrap();
        let re = regex::Regex::new(r"^(https?://[^/]+)").unwrap();
        let url = re
            .captures(url)
            .map(|cap| cap[1].to_string())
            .unwrap_or_else(|| url.to_string());
        url.to_string()
    });
    let url: String = format!(
        "{grafana_api}/grafana/{grafana_path}?{}",
        req.query_string()
    );
    let mut headers = reqwest::header::HeaderMap::new();
    let token = format!("Bearer {}", grafana.unwrap().token.as_ref().unwrap());
    headers.insert(
        reqwest::header::AUTHORIZATION,
        reqwest::header::HeaderValue::from_str(&token).unwrap(),
    );

    proxy(req, payload, &client, &url, Some(headers))
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
    let mut qid = Span.get_qid().unwrap_or_else(Qid::init);

    let url = x_url!(&args, &req, "api-doc/openapi.json");
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    use tracing::Instrument;
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
    let method: reqwest::Method = req
        .method()
        .as_str()
        .parse()
        .unwrap_or(reqwest::Method::GET);
    let builder = client
        .request(method, url)
        .timeout(Duration::from_secs(u64::MAX))
        .headers(qid::headers_with_qid(&qid))
        .body(reqwest::Body::wrap_stream(UnboundedReceiverStream::new(rx)));
    let builder = real_ip_forward(&req, builder);
    let res = builder
        .send()
        .await
        .map_err(error::ErrorInternalServerError)?;
    debug!("Got proxy result");
    // Convert reqwest status to actix StatusCode to avoid http crate version mismatch
    let status =
        StatusCode::from_u16(res.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut client_resp = HttpResponse::build(status);
    for (header_name, header_value) in res.headers().iter().filter(|(h, _)| *h != "connection") {
        client_resp.insert_header((
            header_name.as_str(),
            header_value.to_str().unwrap_or_default(),
        ));
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

/// Serve static assets with a prefix.
///
/// Each prefix is treated as a root path by responding with `index.html`.
///
/// In Explorer, this is used to serve the static assets for the UI and docs, including:
/// - ""
/// - "docs/"
/// - "docs-en/"
async fn static_assets_with_prefix(
    prefix: Option<&str>,
    path: &str,
) -> CustomizeResponder<EmbedResponse<EmbeddedFile>> {
    const COOKIE_ROUTE: &str = "route";
    // Treat path as a route by responding with `index.html`.
    //
    // Examples:
    // - ""
    // - "/about"
    // - "/about/"
    // - "/dataIn/task"
    if path.is_empty()
        || path.ends_with('/')
        || !path[path.rfind('/').map(|i| i + 1).unwrap_or(0)..].contains('.')
    {
        let index = prefix.map_or_else(
            || Cow::Borrowed("index.html"),
            |prefix| Cow::Owned(format!("{prefix}index.html")),
        );
        let cookie = Cookie::build(
            COOKIE_ROUTE,
            if path.is_empty() {
                Cow::Borrowed(prefix.unwrap_or(""))
            } else {
                prefix.map_or_else(
                    || Cow::Borrowed(path),
                    |prefix| Cow::Owned(format!("{prefix}{path}")),
                )
            },
        )
        .path("/")
        .same_site(awc::cookie::SameSite::Lax)
        .finish();
        tracing::info!("SPA route to {path}");
        return StaticAssets::get(&index)
            .into_response()
            .customize()
            .add_cookie(&cookie)
            .append_header(ContentType::html());
    }
    let path = prefix.map_or_else(
        || Cow::Borrowed(path),
        |prefix| Cow::Owned(format!("{prefix}{path}")),
    );

    if let Some(file) = StaticAssets::get(&path).or_else(|| {
        path.char_indices()
            .filter(|&(_, c)| c == '/')
            .filter_map(|(i, _)| {
                let part = &path[i + 1..];
                StaticAssets::get(part)
            })
            .next()
    }) {
        let mime = mime_guess::from_path(&*path).first_or_octet_stream();
        file.into_response()
            .customize()
            .append_header(("Content-Type", mime.essence_str()))
    } else {
        None.into_response().customize()
    }
}
/// For docs.
#[route("/docs/{path:.*}", method = "GET", method = "HEAD")]
async fn docs_assets(path: web::Path<String>) -> CustomizeResponder<EmbedResponse<EmbeddedFile>> {
    static_assets_with_prefix(Some("docs/"), path.as_str()).await
}
/// For docs.
#[route("/docs-en/{path:.*}", method = "GET", method = "HEAD")]
#[instrument(skip_all, fields(path = path.as_str()))]
async fn docs_en_assets(
    path: web::Path<String>,
) -> CustomizeResponder<EmbedResponse<EmbeddedFile>> {
    static_assets_with_prefix(Some("docs-en/"), path.as_str()).await
}

/// For static assets as a SPA website.
#[route("/{path:.*}", method = "GET", method = "HEAD")]
async fn static_assets(path: web::Path<String>) -> CustomizeResponder<EmbedResponse<EmbeddedFile>> {
    static_assets_with_prefix(None, path.as_str()).await
}

#[derive(Parser, Debug, Clone, Deserialize, Serialize, Default)]
struct Profile {
    /// Cluster endpoint. Use taosAdapter endpoint like `http://192.168.0.201:16041`.
    #[clap(long, env = "EXPLORER_CLUSTER")]
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

    #[clap(flatten)]
    grafana: Option<GrafanaConfig>,
}

#[derive(Parser, Debug, Clone, Deserialize, Serialize, Default)]
#[serde(default)]
struct GrafanaConfig {
    #[serde(skip_serializing)]
    token: Option<String>,
    #[clap(skip)]
    dashboards: Option<HashMap<String, String>>,
}

#[derive(Parser, Debug, Clone, Deserialize)]
#[clap(trailing_var_arg = true, disable_help_flag = true)]
struct ConfigPath {
    /// Configuration file
    #[clap(short = 'c', long, alias = "config", env = "EXPLORER_CONFIG_FILE")]
    config_file: Option<PathBuf>,

    #[clap(allow_hyphen_values = true)]
    raw_args: Vec<OsString>,
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

#[derive(Parser, Debug, Clone, Deserialize, Serialize, Default)]
#[clap(name = env!("CUS_CLI_NAME"), author, version = CLAP_SHORT_VERSION, about, long_about = include_str!(env!("CUS_README")))]
struct Args {
    /// Configuration file
    #[clap(short = 'c', long, alias = "config", env = "EXPLORER_CONFIG_FILE")]
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

    #[clap(flatten)]
    #[serde(default)]
    monitor: monitor::MonitorCfg,

    #[clap(skip)]
    oauth: Option<oauth::OAuthConfig>,

    #[clap(flatten)]
    #[serde(default)]
    security: SecurityConfig,

    #[clap(skip)]
    #[serde(skip)]
    session_manager: Option<SessionManager>,
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
            rotation_count: Some(3),
            keep_days: Some(3),
            rotation_size: Some("1GB".to_string()),
            reserved_disk_size: Some("1GB".to_string()),
        }
    }
}

fn get_default_log_path() -> PathBuf {
    if cfg!(windows) {
        PathBuf::from(format!("C:\\{}\\log", env!("CANONICAL_CUS_NAME")))
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

    /// CA bundle used when Explorer acts as a taosx gRPC HTTPS client.
    #[clap(long, global = true, env = "EXPLORER_RPC_CA_CERT")]
    rpc_ca_cert: Option<PathBuf>,

    /// CA PEM bytes loaded from `rpc_ca_cert` at startup. Populated programmatically so
    /// the file is read once instead of on every gRPC connection attempt.
    #[clap(skip)]
    #[serde(skip)]
    rpc_ca_cert_pem: Option<std::sync::Arc<Vec<u8>>>,
}

impl Args {
    fn build_dsn(&self, auth: &oauth::middleware::TsdbCredential) -> Result<Dsn, RestErrResponse> {
        tracing::debug!(
            auth = auth.auth_type.as_str(),
            "Building DSN from auth header: {}",
            auth
        );
        let mut dsn: Dsn = self
            .profile
            .cluster
            .as_deref()
            .unwrap_or("taos://localhost:6030")
            .parse()
            .map_err(RestErrResponse::new)?;
        // If password is a token placeholder (from TOTP or token login), use token auth
        if let Some(token) = auth.password.strip_prefix("__token__") {
            dsn.username = Some(auth.username.clone());
            dsn.set("bearer_token", token);
        } else {
            dsn.username = Some(auth.username.clone());
            dsn.password = Some(auth.password.clone());
        }
        Ok(dsn)
    }

    /// Build DSN with optional TOTP code for two-factor authentication.
    fn build_dsn_with_totp(
        &self,
        auth: &oauth::middleware::TsdbCredential,
        totp_code: Option<&str>,
    ) -> Result<Dsn, RestErrResponse> {
        let mut dsn = self.build_dsn(auth)?;
        if let Some(code) = totp_code {
            dsn.set("totp_code", code);
        }
        Ok(dsn)
    }

    /// Build DSN for token-based authentication (no username/password).
    fn build_dsn_with_token(&self, token: &str) -> Result<Dsn, RestErrResponse> {
        let mut dsn: Dsn = self
            .profile
            .cluster
            .as_deref()
            .unwrap_or("taos://localhost:6030")
            .parse()
            .map_err(RestErrResponse::new)?;
        dsn.set("bearer_token", token);
        Ok(dsn)
    }

    async fn query_inner(
        &self,
        dsn: &Dsn,
        sql: &str,
        tz: Option<&String>,
        req_id: u64,
    ) -> Result<RestOkResponse, RestErrResponse> {
        let query_started_at = std::time::Instant::now();
        // taos connection pool
        let conn = get_connection(dsn).await.map_err(RestErrResponse::new)?;

        let tz = if let Some(tz) = tz {
            chrono_tz::Tz::from_str(tz).unwrap_or(chrono_tz::Tz::UTC)
        } else {
            chrono_tz::Tz::UTC
        };

        debug!("Got connection, querying sql");
        let mut set = conn.query_with_req_id(sql, req_id).await?;
        debug!("Got sql result set");
        // dml and cud return empty set
        if set.fields().is_empty() {
            let affect_rows = set.affected_rows();
            return Ok(RestOkResponse {
                code: Code::SUCCESS,
                column_meta: vec![("affected_rows".to_string(), "int".to_string(), 4)],
                rows: 1,
                data: vec![vec![serde_json::Value::Number(affect_rows.into())]],
                timing: elapsed_as_nanos(query_started_at),
                ..Default::default()
            });
        }
        let precision = set.precision();
        let seconds_format = match precision {
            Precision::Millisecond => SecondsFormat::Millis,
            Precision::Microsecond => SecondsFormat::Micros,
            Precision::Nanosecond => SecondsFormat::Nanos,
        };
        let column_meta = set
            .fields()
            .iter()
            .map(|f| (f.name().to_string(), f.ty().to_string(), f.bytes()))
            .collect_vec();
        debug!("Got fields {column_meta:?}, fetching data.");

        let convert_value = |value: taos::Value| match value {
            taos::Value::Timestamp(ts) => {
                let ts_with_tz = tz.from_utc_datetime(&ts.to_naive_datetime());
                serde_json::Value::String(ts_with_tz.to_rfc3339_opts(seconds_format, true))
            }
            taos::Value::VarBinary(vb) => {
                serde_json::Value::String(format!("\\x{}", hex::encode(vb).to_uppercase()))
            }
            taos::Value::Geometry(geo) => {
                serde_json::Value::String(parse_geometry_from_bytes(&geo))
            }
            taos::Value::Float(f) => serde_json::Value::from(f),
            _ => value.to_json_value(),
        };
        let data = if need_limit(sql) {
            // select 语句如果不包含 limit，默认返回 1000 条
            set.rows()
                .take(1000)
                .map_ok(RowView::into_values)
                .try_collect::<Vec<_>>()
                .await?
        } else {
            set.to_records().await?
        };
        let data = data
            .into_iter()
            .map(|row| row.into_iter().map(convert_value).collect_vec())
            .collect_vec();
        debug!("SQL result: {data:?}");
        Ok(RestOkResponse {
            code: Code::SUCCESS,
            column_meta,
            rows: data.len() as _,
            data,
            timing: elapsed_as_nanos(query_started_at),
            ..Default::default()
        })
    }

    async fn query(
        &self,
        auth: &oauth::middleware::TsdbCredential,
        sql: &str,
        tz: Option<&String>,
    ) -> Result<RestOkResponse, RestErrResponse> {
        let dsn = self.build_dsn(auth)?;
        let mut qid = Span.get_qid::<Qid>().unwrap_or_else(Qid::init);
        qid.add_sequence_id();
        self.query_inner(&dsn, sql, tz, qid.get()).await
    }

    /// Execute a SQL query using a pre-built DSN (for TOTP/Token auth flows).
    async fn query_with_dsn(
        &self,
        dsn: &Dsn,
        sql: &str,
        tz: Option<&String>,
    ) -> Result<RestOkResponse, RestErrResponse> {
        let mut qid = Span.get_qid::<Qid>().unwrap_or_else(Qid::init);
        qid.add_sequence_id();
        self.query_inner(dsn, sql, tz, qid.get()).await
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

        let mut qid = Span.get_qid::<Qid>().unwrap_or_else(Qid::init);
        qid.add_sequence_id();

        self.query_inner(&dsn, sql, None, qid.get()).await
    }

    async fn renew(
        &self,
        auth: &oauth::middleware::TsdbCredential,
        license: &RenewLicense,
    ) -> Result<RestOkResponse, RestErrResponse> {
        let dsn = self.build_dsn(auth)?;
        let conn = get_connection(&dsn).await.map_err(RestErrResponse::new)?;
        let mut qid: Qid = Span.get_qid().unwrap_or(Qid::init());
        if license.mode() == RenewLicenseMode::Cls {
            for sql in build_cls_license_sqls(license)? {
                qid.add_sequence_id();
                conn.exec_with_req_id(&sql, qid.get())
                    .await
                    .map_err(RestErrResponse::new)?;
            }
            return Ok(RestOkResponse {
                code: Code::SUCCESS,
                rows: 0,
                ..Default::default()
            });
        }
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
            if let Some(active_code) = license.active_code.as_ref()
                && !active_code.is_empty()
            {
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
            if let Some(c_active_code) = license.c_active_code.as_ref()
                && !c_active_code.is_empty()
            {
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
        Ok(RestOkResponse {
            code: Code::SUCCESS,
            rows: 0,
            ..Default::default()
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

fn elapsed_as_nanos(started_at: std::time::Instant) -> u64 {
    started_at.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

#[derive(Debug, serde::Serialize, Default)]
struct RestOkResponse {
    code: Code,
    column_meta: Vec<(String, String, u32)>,
    data: Vec<Vec<serde_json::Value>>,
    rows: u64,
    timing: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    registered_user: Option<FastStr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    token: Option<String>,
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
            desc: format!("{err:#}"),
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
            desc: format!("{err:#}"),
        }
    }
}
impl From<serde_json::Error> for RestErrResponse {
    fn from(err: serde_json::Error) -> Self {
        Self {
            code: Code::FAILED,
            desc: format!("{err:#}"),
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
    use std::{
        io::{self, Write},
        path::PathBuf,
        str::FromStr,
        sync::{Arc, Mutex},
    };

    use chrono::TimeZone;
    use clap::CommandFactory;
    use log::LevelFilter;
    use taos::*;
    use tracing_subscriber::fmt::MakeWriter;

    use super::*;
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

    #[test]
    fn rest_ok_response_should_serialize_timing_field() {
        let resp = RestOkResponse {
            code: Code::SUCCESS,
            column_meta: vec![],
            data: vec![],
            rows: 0,
            timing: 0,
            registered_user: None,
            token: None,
        };

        let value = serde_json::to_value(resp).expect("serialize rest response");
        assert!(
            value.get("timing").is_some(),
            "rest/sql response should include timing field"
        );
    }

    #[test]
    fn encrypted_password_error_response_should_distinguish_clock_skew_from_bad_password() {
        let resp = encrypted_password_error_response(&utils::xor::XorError::Expired);

        assert_eq!(resp.desc, CLOCK_OUT_OF_SYNC_DESC);
    }

    #[test]
    fn encrypted_password_error_response_should_distinguish_invalid_payload_from_bad_password() {
        let resp = encrypted_password_error_response(&utils::xor::XorError::InvalidData);

        assert_eq!(resp.desc, INVALID_ENCRYPTED_PASSWORD_DESC);
    }

    #[derive(Clone, Default)]
    struct SharedLogWriter(Arc<Mutex<Vec<u8>>>);

    struct SharedLogWriteGuard(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedLogWriteGuard {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedLogWriter {
        type Writer = SharedLogWriteGuard;

        fn make_writer(&'a self) -> Self::Writer {
            SharedLogWriteGuard(self.0.clone())
        }
    }

    #[test]
    fn encrypted_password_login_failure_log_should_follow_taos_style() {
        let writer = SharedLogWriter::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .with_writer(writer.clone())
            .without_time()
            .with_ansi(false)
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            log_encrypted_password_login_failure("root", &utils::xor::XorError::Expired);
        });

        let logs = String::from_utf8(writer.0.lock().unwrap().clone()).unwrap();
        assert!(logs.contains("ERROR"));
        assert!(!logs.contains(" WARN "));
        assert!(!logs.contains(" INFO "));
        assert!(!logs.contains(" qid="));
        assert!(!logs.contains("target=\"root\""));
        assert!(!logs.contains("login_target"));
        assert!(logs.contains("Login failed, target: root, errno: 0x2701("));
        assert!(!logs.contains("errno: 0x2701 ("));
    }

    #[test]
    fn renew_license_should_detect_cls_mode_when_cls_fields_exist() {
        let license = RenewLicense {
            active_code: Some("cluster-code".into()),
            c_active_code: None,
            cls_enabled: Some("1".into()),
            cls_refresh_interval: Some("15".into()),
            cls_url: Some("http://192.168.2.158:6072".into()),
            cls_license_id: Some("lic-1".into()),
            cls_quota_slot_id: None,
        };

        assert_eq!(license.mode(), RenewLicenseMode::Cls);
    }

    #[test]
    fn build_cls_license_sqls_should_emit_all_five_alter_statements() {
        let statements = build_cls_license_sqls(&RenewLicense {
            active_code: None,
            c_active_code: None,
            cls_enabled: Some("1".into()),
            cls_refresh_interval: Some("15".into()),
            cls_url: Some("http://192.168.2.158:6072".into()),
            cls_license_id: Some("lic-1".into()),
            cls_quota_slot_id: Some("tsdb-9".into()),
        })
        .expect("cls statements");

        assert_eq!(
            statements,
            vec![
                "alter all dnodes 'clsRefreshInterval' '15'",
                "alter all dnodes 'clsUrl' 'http://192.168.2.158:6072'",
                "alter all dnodes 'clsLicenseId' 'lic-1'",
                "alter all dnodes 'clsQuotaSlotId' 'tsdb-9'",
                "alter all dnodes 'clsEnabled' '1'",
            ]
        );
    }

    #[test]
    fn build_cls_license_sqls_should_default_quota_slot_id_when_omitted() {
        let statements = build_cls_license_sqls(&RenewLicense {
            active_code: None,
            c_active_code: None,
            cls_enabled: Some("1".into()),
            cls_refresh_interval: Some("15".into()),
            cls_url: Some("http://192.168.2.158:6072".into()),
            cls_license_id: Some("lic-1".into()),
            cls_quota_slot_id: None,
        })
        .expect("cls statements");

        assert!(
            statements.contains(&"alter all dnodes 'clsQuotaSlotId' 'tsdb-1'".to_string()),
            "missing default clsQuotaSlotId statement: {statements:?}"
        );
    }

    #[ignore]
    #[test]
    fn test_build_bin() {
        let mut cmd = assert_cmd::Command::new("yarn");
        let assert = cmd.current_dir("../").arg("build:bin").assert();
        assert.success();
    }

    #[tokio::test]
    async fn test_connect_timeout_with_taos() -> anyhow::Result<(), anyhow::Error> {
        let profile = Profile {
            cluster: Some("http://no.exist:6041".to_string()),
            ..Default::default()
        };

        let args = Args {
            profile,
            ..Default::default()
        };

        // 默认用户名密码：root:taosdata
        let credential = oauth::middleware::TsdbCredential {
            auth_type: oauth::middleware::AuthType::Basic,
            username: "root".to_string(),
            password: "taosdata".to_string(),
        };
        let dsn = args.build_dsn(&credential).unwrap();

        // 清除旧数据
        let sql = "select * from `test_explorer`";
        let result = args.query_inner(&dsn, sql, None, 0).await;
        assert!(result.is_err());

        let err = result.unwrap_err();
        let error_message = err.desc;
        println!("Error message: {}", error_message);

        Ok(())
    }

    #[tokio::test]
    async fn test_timestamp_seconds_format_with_taos() -> anyhow::Result<(), anyhow::Error> {
        let profile = Profile {
            cluster: Some("http://localhost:6041".to_string()),
            ..Default::default()
        };

        let args = Args {
            profile,
            ..Default::default()
        };

        // 默认用户名密码：root:taosdata
        let credential = oauth::middleware::TsdbCredential {
            auth_type: oauth::middleware::AuthType::Basic,
            username: "root".to_string(),
            password: "taosdata".to_string(),
        };
        let dsn = args.build_dsn(&credential).unwrap();

        // 清除旧数据
        let sql = "DROP DATABASE IF EXISTS `test_ts_seconds_format`";
        let _ = args.query_inner(&dsn, sql, None, 0).await;

        // 创建数据库
        let db = "test_ts_seconds_format";
        let precisions = ["ms", "us", "ns"];
        let bj_expects = [
            [
                "2025-01-01T08:00:00.000+08:00",
                "2025-01-01T08:00:01.100+08:00",
                "2025-01-01T08:00:03.999+08:00",
            ],
            [
                "2025-01-01T08:00:00.000000+08:00",
                "2025-01-01T08:00:01.100000+08:00",
                "2025-01-01T08:00:03.999000+08:00",
            ],
            [
                "2025-01-01T08:00:00.000000000+08:00",
                "2025-01-01T08:00:01.100000000+08:00",
                "2025-01-01T08:00:03.999000000+08:00",
            ],
        ];
        let utc_expects = [
            [
                "2025-01-01T00:00:00.000Z",
                "2025-01-01T00:00:01.100Z",
                "2025-01-01T00:00:03.999Z",
            ],
            [
                "2025-01-01T00:00:00.000000Z",
                "2025-01-01T00:00:01.100000Z",
                "2025-01-01T00:00:03.999000Z",
            ],
            [
                "2025-01-01T00:00:00.000000000Z",
                "2025-01-01T00:00:01.100000000Z",
                "2025-01-01T00:00:03.999000000Z",
            ],
        ];
        for (idx, precision) in precisions.into_iter().enumerate() {
            let sql = format!("create database `{db}` precision '{precision}'");
            let result = args.query_inner(&dsn, &sql, None, 0).await.unwrap();
            assert_eq!(
                result.data.first().unwrap().first().unwrap().to_string(),
                "0"
            );

            // 创建超级表
            let sql =
                format!("CREATE STABLE `{db}`.`stb` (ts TIMESTAMP,v1 DOUBLE) TAGS (t1 VARCHAR(8))");
            let result = args.query_inner(&dsn, &sql, None, 0).await.unwrap();
            assert_eq!(
                result.data.first().unwrap().first().unwrap().to_string(),
                "0"
            );

            // 写入测试数据
            let sql = r#"insert into `test_ts_seconds_format`.`tb1` using `test_ts_seconds_format`.`stb` tags ('a')
                        values ('2025-01-01T00:00:00Z', 19.81) ('2025-01-01T00:00:01.100Z', 19.60)
                               ('2025-01-01T00:00:03.999Z', 19.25)"#;
            let tz = Some("Asia/Shanghai".to_string());
            let result = args.query_inner(&dsn, sql, tz.as_ref(), 0).await.unwrap();
            assert_eq!(
                result.data.first().unwrap().first().unwrap().to_string(),
                "3"
            );
            let sql = "select * from `test_ts_seconds_format`.`tb1`";
            let result = args.query_inner(&dsn, sql, tz.as_ref(), 0).await.unwrap();
            // 转成 string 来检查精度
            for j in [0, 1, 2] {
                assert_eq!(result.data[j][0].as_str().unwrap(), bj_expects[idx][j]);
            }

            let tz = Some("UTC".to_string());
            let result = args.query_inner(&dsn, sql, tz.as_ref(), 0).await.unwrap();
            // 转成 string 来检查精度
            for j in [0, 1, 2] {
                assert_eq!(result.data[j][0].as_str().unwrap(), utc_expects[idx][j]);
            }

            // 删除测试数据库
            let sql = "DROP DATABASE `test_ts_seconds_format`";
            args.query_inner(&dsn, sql, None, 0).await.unwrap();
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_float_to_json_precision_with_taos() -> anyhow::Result<(), anyhow::Error> {
        let profile = Profile {
            cluster: Some("http://localhost:6041".to_string()),
            ..Default::default()
        };

        let args = Args {
            profile,
            ..Default::default()
        };

        // 默认用户名密码：root:taosdata
        let credential = oauth::middleware::TsdbCredential {
            auth_type: oauth::middleware::AuthType::Basic,
            username: "root".to_string(),
            password: "taosdata".to_string(),
        };
        let dsn = args.build_dsn(&credential).unwrap();

        // 清除旧数据
        let sql = "DROP DATABASE IF EXISTS `test_explorer`";
        let _ = args.query_inner(&dsn, sql, None, 0).await;

        // 创建数据库
        let sql = "create database `test_explorer` vgroups 2 buffer 3";
        let result = args.query_inner(&dsn, sql, None, 0).await.unwrap();
        assert_eq!(
            result.data.first().unwrap().first().unwrap().to_string(),
            "0"
        );

        // 创建超级表
        let sql = "CREATE STABLE `test_explorer`.`stb_with_float` (ts TIMESTAMP,float_value FLOAT) TAGS (string_tag VARCHAR(8))";
        let result = args.query_inner(&dsn, sql, None, 0).await.unwrap();
        assert_eq!(
            result.data.first().unwrap().first().unwrap().to_string(),
            "0"
        );

        // 写入测试数据
        let sql = r#"insert into `test_explorer`.`t_with_float` using `test_explorer`.`stb_with_float` tags ('a')
                        values ('2025-01-01T00:00:00Z', 19.81) ('2025-01-01T00:00:01Z', 19.60)
                               ('2025-01-01T00:00:03Z', 19.25) ('2025-01-01T00:00:04Z', 19.50)"#;
        let result = args.query_inner(&dsn, sql, None, 0).await.unwrap();
        assert_eq!(
            result.data.first().unwrap().first().unwrap().to_string(),
            "4"
        );

        let sql = "select * from `test_explorer`.`t_with_float`";
        let result = args.query_inner(&dsn, sql, None, 0).await.unwrap();
        // 转成 string 来检查精度
        assert_eq!(
            result.data.first().unwrap().get(1).unwrap().to_string(),
            "19.81"
        );
        assert_eq!(
            result.data.get(1).unwrap().get(1).unwrap().to_string(),
            "19.6"
        );
        assert_eq!(
            result.data.get(2).unwrap().get(1).unwrap().to_string(),
            "19.25"
        );
        assert_eq!(
            result.data.get(3).unwrap().get(1).unwrap().to_string(),
            "19.5"
        );

        // 删除测试数据库
        let sql = "DROP DATABASE `test_explorer`";
        args.query_inner(&dsn, sql, None, 0).await.unwrap();

        Ok(())
    }

    /// Test parse monitor config from cli, env and config file.
    #[test]
    fn test_parse_monitor() {
        let args = Args::parse_from(["explorer", "--monitor-fqdn", "localhost"]).monitor;
        assert_eq!(args.monitor_fqdn, Some("localhost".to_string()));
        assert_eq!(args.monitor_port, 6043);
        assert_eq!(args.monitor_interval, 10);

        unsafe {
            std::env::set_var("MONITOR_FQDN", "fake1");
        }
        unsafe {
            std::env::set_var("MONITOR_PORT", "6044");
        }
        unsafe {
            std::env::set_var("MONITOR_INTERVAL", "5");
        }
        let args = Args::parse_from(["explorer"]).monitor;
        assert_eq!(args.monitor_fqdn, Some("fake1".to_string()));
        assert_eq!(args.monitor_port, 6044);
        assert_eq!(args.monitor_interval, 5);
        unsafe {
            std::env::remove_var("MONITOR_FQDN");
        }
        unsafe {
            std::env::remove_var("MONITOR_PORT");
        }
        unsafe {
            std::env::remove_var("MONITOR_INTERVAL");
        }

        let args = Args::parse_from([
            "explorer",
            "--monitor-fqdn",
            "localhost",
            "--monitor-port",
            "6043",
            "--monitor-interval",
            "2",
        ])
        .monitor;
        assert_eq!(args.monitor_fqdn, Some("localhost".to_string()));
        assert_eq!(args.monitor_port, 6043);
        assert_eq!(args.monitor_interval, 2);

        let args: Args = serde_json::from_str(
            r#"{
            }"#,
        )
        .unwrap();
        let args = args.monitor;
        assert!(args.monitor_fqdn.is_none());
        assert_eq!(args.monitor_port, 6043);
        assert_eq!(args.monitor_interval, 10);

        let args: Args = serde_json::from_str(
            r#"{
                "monitor": {
                    "fqdn": "fake2",
                    "port": 6045,
                    "interval": 3
                }
            }"#,
        )
        .unwrap();
        let args = args.monitor;
        assert_eq!(args.monitor_fqdn, Some("fake2".to_string()));
        assert_eq!(args.monitor_port, 6045);
        assert_eq!(args.monitor_interval, 3);

        let args: Args = serde_json::from_str(
            r#"{
                "monitor": {
                    "fqdn": "fake3"
                }
            }"#,
        )
        .unwrap();
        let args = args.monitor;
        assert_eq!(args.monitor_fqdn, Some("fake3".to_string()));
        assert_eq!(args.monitor_port, 6043);
        assert_eq!(args.monitor_interval, 10);
    }
}
