use assert_fs::fixture::FileWriteStr;
use std::net::{Ipv4Addr, SocketAddrV4, TcpListener, ToSocketAddrs};

fn is_free_tcp(port: u16) -> bool {
    let ipv4 = SocketAddrV4::new(Ipv4Addr::LOCALHOST, port);
    test_bind_tcp(ipv4).is_some()
}

// Try to bind to a socket using TCP
fn test_bind_tcp<A: ToSocketAddrs>(addr: A) -> Option<u16> {
    Some(TcpListener::bind(addr).ok()?.local_addr().ok()?.port())
}

fn get_free_port() -> u16 {
    for port in 6070..65535 {
        if is_free_tcp(port) {
            return port;
        }
    }
    panic!("No free port found");
}

/// Check ui artifacts exist by dist/index.html
fn dist_is_available() -> bool {
    let path = std::env::current_dir().unwrap();
    path.parent()
        .expect("server/../ always exist")
        .join("dist")
        .join("index.html")
        .exists()
}

#[test]
fn test_playwright() -> anyhow::Result<(), anyhow::Error> {
    if dist_is_available() {
        return Ok(());
    }
    let port = get_free_port();
    // config file
    let config_file = assert_fs::NamedTempFile::new("explorer.toml")?;
    config_file.write_str(&format!(
        r#"
port = {port}
addr = "127.0.0.1"
log_level = "info"
cluster = "http://localhost:6041"
x_api = "http://localhost:6050"
grpc = "http://localhost:6055"
cors = true
"#,
    ))?;

    let _explorer_thread = std::thread::spawn(move || {
        let mut cmd = assert_cmd::Command::cargo_bin("taos-explorer").unwrap();
        let assert = cmd
            .arg("-C")
            .arg(config_file.path().to_str().unwrap())
            .timeout(std::time::Duration::from_secs(30))
            .assert();
        assert.interrupted();
    });

    // Add your playwright tests here
    let assert = assert_cmd::Command::new("pnpm")
        .args(["exec", "playwright", "test"])
        .env("PLAYWRIGHT_BASE_URL", format!("http://localhost:{}", port))
        .assert();
    assert.success();
    Ok(())
}
