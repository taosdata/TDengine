use tracing_subscriber::EnvFilter;

use taosx_core::set_env_data_dir;

use super::*;

pub fn tracing_subscriber_init() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("debug".parse()?))
        .with_file(true)
        .pretty()
        .try_init();
    set_env_data_dir("");
    Ok(())
}

#[test]
fn ssl() {
    let cli = Cli::parse_from([
        "",
        "--ssl-cert",
        "cert.pem",
        "--ssl-key",
        "key.pem",
        "--ssl-ca",
        "ca.pem",
    ]);
    assert_eq!(cli.ssl_cert, Some("cert.pem".to_string()));
    assert_eq!(cli.ssl_key, Some("key.pem".to_string()));
}

#[test]
fn merge_cli_options() {
    let mut cli = Cli::parse_from([
        "",
        "--ssl-cert",
        "cert.pem",
        "--ssl-key",
        "key.pem",
        "--ssl-ca",
        "ca.pem",
    ]);
    assert_eq!(cli.ssl_cert.as_deref(), Some("cert.pem"));
    assert_eq!(cli.ssl_key.as_deref(), Some("key.pem"));
    assert_eq!(cli.ssl_ca.as_deref(), Some("ca.pem"));

    let rhs = Cli::default();
    assert_eq!(rhs.ssl_cert, None);
    assert_eq!(rhs.ssl_key, None);

    cli.merge_from(rhs);

    assert_eq!(cli.ssl_cert.as_deref(), Some("cert.pem"));
    assert_eq!(cli.ssl_key.as_deref(), Some("key.pem"));
    assert_eq!(cli.ssl_ca.as_deref(), Some("ca.pem"));

    let mut lhs = Cli::parse_from(["", "--listen", "0.0.0.0:6050"]);

    assert_eq!(lhs.listen.as_deref(), Some("0.0.0.0:6050"));
    assert!(lhs.ssl_cert.is_none());

    lhs.merge_from(cli);
    assert_eq!(lhs.listen.as_deref(), Some("0.0.0.0:6050"));
    assert_eq!(lhs.ssl_cert.as_deref(), Some("cert.pem"));
    assert_eq!(lhs.ssl_key.as_deref(), Some("key.pem"));
    assert_eq!(lhs.ssl_ca.as_deref(), Some("ca.pem"));
}
