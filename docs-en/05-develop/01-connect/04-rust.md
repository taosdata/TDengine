---
sidebar_label: Rust
title: Connect with Rust Connector
---

## Add Dependency


``` title="Cargo.toml"

```

## Config

Run this command in your terminal to save TDengine cloud token as variables:

```bash
export TDENGINE_TOKEN=<token>
```

## Connect

```go
use libtaos::*;

fn main() {
    let token =  std::env::var("TDENGINE_TOKEN").unwrap();
    let dsn = format!("https://cloud.tdengine.com?token={}", token);
    let taos = Taos::from_dsn(dsn)?;
}
```