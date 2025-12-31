use crate::serve::{Cli, TAOSX_REST_API_DEFAULT_PORT};
use clap::Parser;

#[test]
fn test_cli_default() {
    let cli = Cli::default();
    assert!(cli.listen.is_none());
    assert!(cli.ssl_cert.is_none());
    assert!(cli.ssl_key.is_none());
    assert!(cli.grpc.is_none());
    assert!(cli.database_url.is_none());
}

#[test]
fn test_cli_parse_listen() {
    let cli = Cli::parse_from(["", "--listen", "127.0.0.1:8080"]);
    assert_eq!(cli.listen, Some("127.0.0.1:8080".to_string()));
}

#[test]
fn test_cli_parse_grpc() {
    let cli = Cli::parse_from(["", "--grpc", "0.0.0.0:6055"]);
    assert_eq!(cli.grpc, Some("0.0.0.0:6055".to_string()));
}

#[test]
fn test_cli_parse_database_url() {
    let cli = Cli::parse_from(["", "--database-url", "sqlite:test.db"]);
    assert_eq!(cli.database_url, Some("sqlite:test.db".to_string()));
}

#[test]
fn test_get_database_url_from_option() {
    let cli = Cli {
        database_url: Some("sqlite:custom.db".to_string()),
        ..Default::default()
    };
    assert_eq!(cli.get_database_url(), "sqlite:custom.db");
}

#[test]
fn test_get_database_url_default() {
    let cli = Cli::default();
    let url = cli.get_database_url();
    dbg!(&url);
    assert!(url.starts_with("sqlite:") && url.ends_with("x.db"));
}

#[test]
fn test_get_listen_port_default() {
    let cli = Cli::default();
    assert_eq!(cli.get_listen_port(), TAOSX_REST_API_DEFAULT_PORT);
}

#[test]
fn test_get_listen_port_custom() {
    let cli = Cli {
        listen: Some("127.0.0.1:8080".to_string()),
        ..Default::default()
    };
    assert_eq!(cli.get_listen_port(), 8080);
}

#[test]
fn test_get_listen_port_ipv6() {
    let cli = Cli {
        listen: Some("[::1]:9090".to_string()),
        ..Default::default()
    };
    assert_eq!(cli.get_listen_port(), 9090);
}

#[test]
fn test_get_listen_address_custom() {
    let cli = Cli {
        listen: Some("127.0.0.1:8080".to_string()),
        ..Default::default()
    };
    let addrs = cli.get_listen_address().unwrap();
    assert!(!addrs.is_empty());
    assert_eq!(addrs[0].port(), 8080);
}

#[test]
fn test_merge_from_empty() {
    let mut cli = Cli::parse_from(["", "--listen", "127.0.0.1:8080"]);
    let original_listen = cli.listen.clone();

    cli.merge_from(Cli::default());

    assert_eq!(cli.listen, original_listen);
}

#[test]
fn test_merge_from_with_values() {
    let mut cli = Cli::default();
    let rhs = Cli {
        listen: Some("0.0.0.0:6050".to_string()),
        database_url: Some("sqlite:test.db".to_string()),
        grpc: Some("0.0.0.0:6055".to_string()),
        ..Default::default()
    };

    cli.merge_from(rhs);

    assert_eq!(cli.listen, Some("0.0.0.0:6050".to_string()));
    assert_eq!(cli.database_url, Some("sqlite:test.db".to_string()));
    assert_eq!(cli.grpc, Some("0.0.0.0:6055".to_string()));
}

#[test]
fn test_merge_from_preserves_existing() {
    let mut cli = Cli {
        listen: Some("127.0.0.1:8080".to_string()),
        database_url: Some("sqlite:existing.db".to_string()),
        ..Default::default()
    };

    let rhs = Cli {
        listen: Some("0.0.0.0:6050".to_string()),
        database_url: Some("sqlite:new.db".to_string()),
        grpc: Some("0.0.0.0:6055".to_string()),
        ..Default::default()
    };

    cli.merge_from(rhs);

    // Existing values should be preserved
    assert_eq!(cli.listen, Some("127.0.0.1:8080".to_string()));
    assert_eq!(cli.database_url, Some("sqlite:existing.db".to_string()));
    // New values should be added
    assert_eq!(cli.grpc, Some("0.0.0.0:6055".to_string()));
}

#[test]
fn test_ssl_cert_requires_key() {
    // This should fail to parse because ssl_key is required when ssl_cert is set
    let result = Cli::try_parse_from(["", "--ssl-cert", "cert.pem"]);
    assert!(result.is_err());
}

#[test]
fn test_ssl_complete_options() {
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
    assert_eq!(cli.ssl_ca, Some("ca.pem".to_string()));
}

#[test]
fn test_grpc_ssl_options() {
    let cli = Cli::parse_from([
        "",
        "--grpc",
        "0.0.0.0:6055",
        "--ssl-cert",
        "cert.pem",
        "--ssl-key",
        "key.pem",
        "--ssl-ca",
        "ca.pem",
        "--grpc-ssl-cert",
        "grpc-cert.pem",
        "--grpc-ssl-key",
        "grpc-key.pem",
        "--grpc-ssl-ca",
        "grpc-ca.pem",
    ]);
    assert_eq!(cli.grpc_ssl_cert, Some("grpc-cert.pem".to_string()));
    assert_eq!(cli.grpc_ssl_key, Some("grpc-key.pem".to_string()));
    assert_eq!(cli.grpc_ssl_ca, Some("grpc-ca.pem".to_string()));
}

#[test]
fn test_repeat_interval() {
    let cli = Cli::parse_from(["", "--repeat-interval", "60"]);
    assert_eq!(cli.repeat_interval, Some(60));
}

#[test]
fn test_request_timeout() {
    let cli = Cli::parse_from(["", "--request-timeout", "30"]);
    assert_eq!(cli.request_timeout, Some(30));
}

#[test]
fn test_do_not_resume() {
    let cli = Cli::parse_from(["", "--do-not-resume", "true"]);
    assert_eq!(cli.do_not_resume, Some(true));
}

#[test]
fn test_env_variables() {
    // Test that DATABASE_URL env is recognized
    unsafe {
        std::env::set_var("DATABASE_URL", "sqlite:env.db");
    }
    let cli = Cli::default();
    let url = cli.get_database_url();
    assert_eq!(url, "sqlite:env.db");
    unsafe {
        std::env::remove_var("DATABASE_URL");
    }
}
