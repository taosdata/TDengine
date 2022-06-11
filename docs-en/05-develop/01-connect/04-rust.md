---
sidebar_label: Rust
title: Connect with Rust Connector
---

## Add Dependency

Add dependency to `Cargo.toml`. 

```toml title="Cargo.toml"
[dependencies]
libtaos = { version = "0.4.2"}
```

## Config

Run this command in your terminal to save TDengine cloud token as variables:

```bash
export TDENGINE_CLOUD_TOKEN=<token>
export TDENGINE_CLOUD_URL=<url>
```

<!-- exclude -->
:::note
Replace  <token\> and <url\> with cloud token and URL.
To obtain the value of cloud token and URL, please log in [TDengine Cloud](https://cloud.tdengine.com) and click "Connector" and then select "Rust".

:::
<!-- exclude-end -->

## Connect

Copy following code to `main.rs`.

```rust title="main.rs"
use libtaos::*;

fn main() {
    let token =  std::env::var("TDENGINE_CLOUD_TOKEN").unwrap();
    let url = std::env::var("TDENGINE_CLOUD_URL").unwrap();
    let dsn = url + "?token=" + &token;
    let taos = Taos::from_dsn(dsn)?;
    println!("connected");
}
```

Then you can execute `cargo run` to test the connection.