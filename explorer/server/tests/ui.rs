use assert_fs::fixture::FileWriteStr;
use std::{
    net::{Ipv4Addr, SocketAddrV4, TcpListener, ToSocketAddrs},
    path::Path,
};

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
fn dist_is_available(path: &Path) -> bool {
    path.join("dist").join("index.html").exists()
}

#[test]
#[ignore]
fn test_playwright() -> anyhow::Result<(), anyhow::Error> {
    let cwd = std::env::current_dir()?;
    let explorer_path = cwd.parent().expect("server/../ always exist");
    println!("Explorer binary path: {}", explorer_path.display());
    if !dist_is_available(explorer_path) {
        println!("dist/index.html does not exist, skipping ui test");
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

    let config_path = config_file.path().to_path_buf();

    let _explorer_thread = std::thread::spawn(move || {
        let mut cmd = assert_cmd::cargo::cargo_bin_cmd!("taos-explorer");

        println!("Running explorer with config: {}", config_path.display());
        let assert = cmd
            .arg("-c")
            .arg(&config_path)
            .env("EXPLORER_SKIP_REGISTER", "true")
            .timeout(std::time::Duration::from_secs(60))
            .assert();
        assert.interrupted();
    });

    println!("PLAYWRIGHT_BASE_URL=http://localhost:{port} pnpm exec playwright test --ui --debug");
    // std::thread::sleep(std::time::Duration::from_secs(5000)); // Wait for the explorer to start

    // Add your playwright tests here
    let assert = assert_cmd::Command::new("pnpm")
        // .args(["exec", "playwright", "test", "--ui", "--debug"])
        .args(["exec", "playwright", "test"])
        .env("PLAYWRIGHT_BASE_URL", format!("http://localhost:{}", port))
        .current_dir(explorer_path)
        .assert();
    let assert = assert.success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    println!("Playwright stdout:\n{}", stdout);
    println!("Playwright stderr:\n{}", stderr);
    assert.stdout(predicates::str::contains("passed"));
    Ok(())
}
