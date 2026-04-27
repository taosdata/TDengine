mod api;
mod controller;
mod entrypoint;
mod log;
mod tasks;

use std::{path::PathBuf, sync::Arc};

use anyhow::Context;
use clap::Parser;
use tracing::level_filters::LevelFilter;

/// HTTPS server configuration for the external control API.
#[derive(Debug, Clone, Default)]
pub struct HttpsConfig {
    pub enabled: bool,
    pub ca_path: Option<PathBuf>,
    pub certificate: Option<PathBuf>,
    pub certificate_key: Option<PathBuf>,
}

#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

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

#[derive(Debug, clap::Parser)]
#[clap(version = CLAP_SHORT_VERSION)]
struct Args {
    #[arg(short = 'c', long, env = "XNODED_CFG_DIR")]
    cfg_dir: String,
    #[arg(short = 'e', long, env = "XNODED_LEADER_EP")]
    leader_ep: String,
    #[arg(short = 'i', long, env = "XNODED_CLUSTER_ID")]
    cluster_id: String,
    #[command(flatten)]
    log: LogOpts,
    #[arg(short = 'l', long, env = "XNODED_LISTEN")]
    listen: Option<String>,
    #[arg(short = 'd', long, env = "XNODED_ENGINE_DSN")]
    taos_dsn: Option<String>,
    #[arg(short = 'u', long, env = "XNODED_USER_PASS")]
    user_pass: Option<String>,
    #[arg(short = 't', long, env = "XNODED_TOKEN")]
    token: Option<String>,
    #[arg(long, env = "XNODED_RPC_CA_CERT")]
    rpc_ca_cert: Option<PathBuf>,
    /// Enable TLS for the external control API server.
    #[arg(
        long,
        env = "XNODED_ENABLE_TLS",
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    enable_tls: bool,
    /// Path to the PEM CA certificate file for inbound TLS client verification.
    #[arg(long, env = "XNODED_TLS_CA_PATH")]
    tls_ca_path: Option<PathBuf>,
    /// Path to the PEM server certificate file for TLS.
    #[arg(long, env = "XNODED_TLS_SVR_CERT_PATH")]
    tls_svr_cert_path: Option<PathBuf>,
    /// Path to the PEM server private key file for TLS.
    #[arg(long, env = "XNODED_TLS_SVR_KEY_PATH")]
    tls_svr_key_path: Option<PathBuf>,
    #[arg(long, env = "XNODED_DEBUG_MEMORY_ONLY_TASKS", default_value_t = false)]
    debug_memory_only_tasks: bool,
}

#[derive(Parser, Debug, Clone, Default)]
struct LogOpts {
    /// Log path.
    #[arg(short = 'p', long = "log-path", alias = "path", env = "XNODED_LOG_DIR")]
    path: PathBuf,

    /// Log level.
    #[arg(
        short = 'v',
        long = "log-level",
        alias = "level",
        env = "XNODED_LOG_LEVEL"
    )]
    level: Option<LevelFilter>,

    /// Enable compress for log files.
    #[arg(
        short = 'z',
        long = "log-compress",
        alias = "compress",
        env = "XNODED_LOG_COMPRESS"
    )]
    compress: Option<bool>,

    /// Rotation count for log files.
    #[arg(
        short = 'n',
        long = "log-rotation-count",
        alias = "rotation-count",
        env = "XNODED_LOG_ROTATION_COUNT"
    )]
    rotation_count: Option<u16>,

    /// Keep days for log files.
    #[arg(
        short = 'k',
        long = "log-keep-days",
        alias = "keep-days",
        env = "XNODED_LOG_KEEP_DAYS"
    )]
    keep_days: Option<u16>,

    /// Rotation size for log files.
    #[arg(
        short = 's',
        long = "log-rotation-size",
        alias = "rotation-size",
        env = "XNODED_LOG_ROTATION_SIZE"
    )]
    rotation_size: Option<String>,

    /// Reserved disk size for log files.
    #[arg(
        short = 'r',
        long = "log-reserved-disk-size",
        alias = "reserved-disk-size",
        env = "XNODED_LOG_RESERVED_DISK_SIZE"
    )]
    reserved_disk_size: Option<String>,
}

fn finish_rustls_provider_install(
    result: std::result::Result<(), Arc<rustls::crypto::CryptoProvider>>,
) -> anyhow::Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(_) => {
            tracing::debug!("rustls crypto provider already installed");
            Ok(())
        }
    }
}

fn install_rustls_provider() -> anyhow::Result<()> {
    finish_rustls_provider_install(rustls::crypto::ring::default_provider().install_default())
}

fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();

    // Set a panic hook
    std::panic::set_hook(Box::new(|info| {
        // 正常打印 backtrace, 需要设置环境变量: RUST_BACKTRACE=1
        let backtrace = std::backtrace::Backtrace::capture();
        tracing::error!("panic occurred. {} {}", info, backtrace);
    }));

    log::init(&args).context("init logger error")?;
    install_rustls_provider()?;
    print_args(&args);

    let _guard = taosx_utils::defer::defer(|| tracing::info!("main exited"));

    if let Err(e) = entrypoint::run(args) {
        tracing::error!("failed to run entrypoint: {e:#}");
        return Err(e);
    }

    Ok(())
}

fn print_args(args: &Args) {
    tracing::info!("================config================");
    tracing::info!("cfg_dir: {}", args.cfg_dir);
    tracing::info!("leader_ep: {}", args.leader_ep);
    tracing::info!("cluster_id: {}", args.cluster_id);
    tracing::info!(
        "listen: {}",
        args.listen.as_deref().unwrap_or("0.0.0.0:6051")
    );
    if let Some(taos_dsn) = args.taos_dsn.as_ref() {
        tracing::info!("engine dsn: {}", taos_dsn);
    }
    if let Some(rpc_ca_cert) = args.rpc_ca_cert.as_ref() {
        tracing::info!("rpc_ca_cert: {}", rpc_ca_cert.display());
    }
    tracing::info!("enable_tls: {}", args.enable_tls);
    if args.enable_tls {
        if let Some(ca) = args.tls_ca_path.as_ref() {
            tracing::info!("tls_ca_path: {}", ca.display());
        }
        if let Some(cert) = args.tls_svr_cert_path.as_ref() {
            tracing::info!("tls_svr_cert_path: {}", cert.display());
        }
        if let Some(key) = args.tls_svr_key_path.as_ref() {
            tracing::info!("tls_svr_key_path: {}", key.display());
        }
    }
    if args.debug_memory_only_tasks {
        tracing::info!("debug.memory_only_tasks: true");
    }
    tracing::info!("log.path: {}", args.log.path.display());
    if let Some(level) = args.log.level.as_ref() {
        tracing::info!("log.level: {}", level);
    }
    if let Some(compress) = args.log.compress.as_ref() {
        tracing::info!("log.compress: {}", compress);
    }
    if let Some(rotation_count) = args.log.rotation_count.as_ref() {
        tracing::info!("log.rotation_count: {}", rotation_count);
    }
    if let Some(keep_days) = args.log.keep_days.as_ref() {
        tracing::info!("log.keep_days: {}", keep_days);
    }
    if let Some(rotation_size) = args.log.rotation_size.as_ref() {
        tracing::info!("log.rotation_size: {}", rotation_size);
    }
    if let Some(reserved_disk_size) = args.log.reserved_disk_size.as_ref() {
        tracing::info!("log.reserved_disk_size: {}", reserved_disk_size);
    }
    tracing::info!("================config================");
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::{
        ffi::OsString,
        path::{Path, PathBuf},
        sync::{Arc, Mutex, OnceLock},
    };
    use tracing::level_filters::LevelFilter;

    struct ScopedEnvVar {
        key: &'static str,
        original: Option<OsString>,
    }

    impl ScopedEnvVar {
        fn set(key: &'static str, value: &str) -> Self {
            let original = std::env::var_os(key);
            unsafe {
                std::env::set_var(key, value);
            }
            Self { key, original }
        }
    }

    impl Drop for ScopedEnvVar {
        fn drop(&mut self) {
            match self.original.as_ref() {
                Some(value) => unsafe { std::env::set_var(self.key, value) },
                None => unsafe { std::env::remove_var(self.key) },
            }
        }
    }

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn install_rustls_provider_is_idempotent() {
        install_rustls_provider().expect("first install should succeed");
        install_rustls_provider().expect("second install should succeed");
    }

    #[test]
    fn finish_rustls_provider_install_treats_already_installed_as_success() {
        let provider = rustls::crypto::ring::default_provider();
        let result = finish_rustls_provider_install(Err(Arc::new(provider)));
        assert!(
            result.is_ok(),
            "already-installed should not be treated as an error"
        );
    }

    #[test]
    fn scoped_env_var_restores_previous_value_on_drop() {
        let _guard = env_lock().lock().expect("lock env");
        unsafe {
            std::env::set_var("XNODED_TEST_ENV_GUARD", "before");
        }
        {
            let _scoped = ScopedEnvVar::set("XNODED_TEST_ENV_GUARD", "during");
            assert_eq!(
                std::env::var("XNODED_TEST_ENV_GUARD").as_deref(),
                Ok("during")
            );
        }
        assert_eq!(
            std::env::var("XNODED_TEST_ENV_GUARD").as_deref(),
            Ok("before")
        );
        unsafe {
            std::env::remove_var("XNODED_TEST_ENV_GUARD");
        }
    }

    #[test]
    fn scoped_env_var_removes_new_value_on_drop() {
        let _guard = env_lock().lock().expect("lock env");
        unsafe {
            std::env::remove_var("XNODED_TEST_ENV_GUARD");
        }
        {
            let _scoped = ScopedEnvVar::set("XNODED_TEST_ENV_GUARD", "during");
            assert_eq!(
                std::env::var("XNODED_TEST_ENV_GUARD").as_deref(),
                Ok("during")
            );
        }
        assert!(
            std::env::var("XNODED_TEST_ENV_GUARD").is_err(),
            "temporary env var should be removed on drop"
        );
    }

    #[test]
    fn args_parse_rpc_ca_cert_from_cli() {
        let args = Args::try_parse_from([
            "xnoded",
            "--cfg-dir",
            "/etc/taos",
            "--leader-ep",
            "127.0.0.1:7030",
            "--cluster-id",
            "cluster",
            "--log-path",
            ".",
            "--rpc-ca-cert",
            "/etc/certs/ca.pem",
        ])
        .expect("parse args");

        assert_eq!(
            args.rpc_ca_cert.as_deref(),
            Some(Path::new("/etc/certs/ca.pem"))
        );
    }

    #[test]
    fn args_parse_tls_enabled_from_cli() {
        let args = Args::try_parse_from([
            "xnoded",
            "--cfg-dir",
            "/etc/taos",
            "--leader-ep",
            "127.0.0.1:7030",
            "--cluster-id",
            "cluster",
            "--log-path",
            ".",
            "--enable-tls",
            "--tls-ca-path",
            "/etc/certs/ca.pem",
            "--tls-svr-cert-path",
            "/etc/certs/server.pem",
            "--tls-svr-key-path",
            "/etc/certs/server.key",
        ])
        .expect("parse args");

        assert!(args.enable_tls);
        assert_eq!(
            args.tls_ca_path.as_deref(),
            Some(Path::new("/etc/certs/ca.pem"))
        );
        assert_eq!(
            args.tls_svr_cert_path.as_deref(),
            Some(Path::new("/etc/certs/server.pem"))
        );
        assert_eq!(
            args.tls_svr_key_path.as_deref(),
            Some(Path::new("/etc/certs/server.key"))
        );
    }

    #[test]
    fn args_parse_tls_disabled_by_default() {
        let args = Args::try_parse_from([
            "xnoded",
            "--cfg-dir",
            "/etc/taos",
            "--leader-ep",
            "127.0.0.1:7030",
            "--cluster-id",
            "cluster",
            "--log-path",
            ".",
        ])
        .expect("parse args");

        assert!(!args.enable_tls);
        assert!(args.tls_ca_path.is_none());
        assert!(args.tls_svr_cert_path.is_none());
        assert!(args.tls_svr_key_path.is_none());
    }

    #[test]
    fn args_parse_tls_from_env() {
        let _guard = env_lock().lock().expect("lock env");
        let _enable_tls = ScopedEnvVar::set("XNODED_ENABLE_TLS", "1");
        let _tls_ca_path = ScopedEnvVar::set("XNODED_TLS_CA_PATH", "/etc/certs/ca.pem");
        let _tls_svr_cert_path =
            ScopedEnvVar::set("XNODED_TLS_SVR_CERT_PATH", "/etc/certs/server.pem");
        let _tls_svr_key_path =
            ScopedEnvVar::set("XNODED_TLS_SVR_KEY_PATH", "/etc/certs/server.key");

        let args = Args::try_parse_from([
            "xnoded",
            "--cfg-dir",
            "/etc/taos",
            "--leader-ep",
            "127.0.0.1:7030",
            "--cluster-id",
            "cluster",
            "--log-path",
            ".",
        ])
        .expect("parse args");

        assert!(args.enable_tls);
        assert_eq!(
            args.tls_ca_path.as_deref(),
            Some(Path::new("/etc/certs/ca.pem"))
        );
        assert_eq!(
            args.tls_svr_cert_path.as_deref(),
            Some(Path::new("/etc/certs/server.pem"))
        );
        assert_eq!(
            args.tls_svr_key_path.as_deref(),
            Some(Path::new("/etc/certs/server.key"))
        );
    }

    #[test]
    fn args_ignore_legacy_https_env_names() {
        let _guard = env_lock().lock().expect("lock env");
        let _legacy_enable = ScopedEnvVar::set("XNODED_HTTPS_ENABLE", "1");
        let _legacy_cert = ScopedEnvVar::set("XNODED_HTTPS_CERTIFICATE", "/etc/certs/server.pem");
        let _legacy_key =
            ScopedEnvVar::set("XNODED_HTTPS_CERTIFICATE_KEY", "/etc/certs/server.key");

        let args = Args::try_parse_from([
            "xnoded",
            "--cfg-dir",
            "/etc/taos",
            "--leader-ep",
            "127.0.0.1:7030",
            "--cluster-id",
            "cluster",
            "--log-path",
            ".",
        ])
        .expect("parse args");

        assert!(!args.enable_tls);
        assert!(args.tls_svr_cert_path.is_none());
        assert!(args.tls_svr_key_path.is_none());
    }

    #[test]
    fn args_accept_short_flags_for_runtime_fields() {
        let args = Args::try_parse_from([
            "xnoded",
            "-c",
            "/tmp/xnoded",
            "-e",
            "127.0.0.1:6050",
            "-i",
            "cluster-a",
            "-l",
            "127.0.0.1:6051",
            "-p",
            "/tmp/xnoded.log",
            "-v",
            "debug",
            "-d",
            "taos://root:taosdata@localhost:6030",
            "-u",
            "root:taosdata",
            "-t",
            "agent-token",
        ])
        .expect("short flags should parse");

        assert_eq!(args.cfg_dir, "/tmp/xnoded");
        assert_eq!(args.leader_ep, "127.0.0.1:6050");
        assert_eq!(args.cluster_id, "cluster-a");
        assert_eq!(args.listen.as_deref(), Some("127.0.0.1:6051"));
        assert_eq!(
            args.taos_dsn.as_deref(),
            Some("taos://root:taosdata@localhost:6030")
        );
        assert_eq!(args.user_pass.as_deref(), Some("root:taosdata"));
        assert_eq!(args.token.as_deref(), Some("agent-token"));
        assert_eq!(args.log.path, PathBuf::from("/tmp/xnoded.log"));
        assert_eq!(args.log.level, Some(LevelFilter::DEBUG));
    }

    #[test]
    fn args_accept_long_flags_for_log_fields() {
        let args = Args::try_parse_from([
            "xnoded",
            "--cfg-dir",
            "/tmp/xnoded",
            "--leader-ep",
            "127.0.0.1:6050",
            "--cluster-id",
            "cluster-a",
            "--log-path",
            "/tmp/xnoded.log",
            "--log-level",
            "debug",
            "--log-compress",
            "true",
            "--log-rotation-count",
            "7",
            "--log-keep-days",
            "30",
            "--log-rotation-size",
            "100MB",
            "--log-reserved-disk-size",
            "1GB",
        ])
        .expect("new log long flags should parse");

        assert_eq!(args.log.path, PathBuf::from("/tmp/xnoded.log"));
        assert_eq!(args.log.level, Some(LevelFilter::DEBUG));
        assert_eq!(args.log.compress, Some(true));
        assert_eq!(args.log.rotation_count, Some(7));
        assert_eq!(args.log.keep_days, Some(30));
        assert_eq!(args.log.rotation_size.as_deref(), Some("100MB"));
        assert_eq!(args.log.reserved_disk_size.as_deref(), Some("1GB"));
    }

    #[test]
    fn args_accept_old_long_flags_for_log_fields_as_aliases() {
        let args = Args::try_parse_from([
            "xnoded",
            "--cfg-dir",
            "/tmp/xnoded",
            "--leader-ep",
            "127.0.0.1:6050",
            "--cluster-id",
            "cluster-a",
            "--path",
            "/tmp/xnoded.log",
            "--level",
            "debug",
            "--compress",
            "true",
            "--rotation-count",
            "7",
            "--keep-days",
            "30",
            "--rotation-size",
            "100MB",
            "--reserved-disk-size",
            "1GB",
        ])
        .expect("legacy log long flags should keep parsing");

        assert_eq!(args.log.path, PathBuf::from("/tmp/xnoded.log"));
        assert_eq!(args.log.level, Some(LevelFilter::DEBUG));
        assert_eq!(args.log.compress, Some(true));
        assert_eq!(args.log.rotation_count, Some(7));
        assert_eq!(args.log.keep_days, Some(30));
        assert_eq!(args.log.rotation_size.as_deref(), Some("100MB"));
        assert_eq!(args.log.reserved_disk_size.as_deref(), Some("1GB"));
    }

    #[test]
    fn args_accept_debug_memory_only_tasks_flag() {
        let args = Args::try_parse_from([
            "xnoded",
            "--cfg-dir",
            "/tmp/xnoded",
            "--leader-ep",
            "127.0.0.1:6050",
            "--cluster-id",
            "cluster-a",
            "--log-path",
            "/tmp/xnoded.log",
            "--debug-memory-only-tasks",
        ])
        .expect("flag should parse");

        assert!(args.debug_memory_only_tasks);
    }
}
