---
sidebar_label: Rust
title: Connect with Rust Connector
description: Connect to TDengine cloud service using Rust connector
---
<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->
## Create Project

```
cargo new --bin cloud-example
```
## Add Dependency

Add dependency to `Cargo.toml`. 

```toml title="Cargo.toml"
[package]
name = "cloud-example"
version = "0.1.0"
edition = "2021"

[dependencies]
taos = { version = "*", default-features = false, features = ["ws"] }
tokio = { version = "1", features = ["full"]}
anyhow = "1.0.0" 
```

## Config

Run this command in your terminal to save TDengine cloud token as variables:

<Tabs defaultValue="bash">
<TabItem value="bash" label="Bash">

```bash
export TDENGINE_CLOUD_DSN="<DSN>"
```

</TabItem>
<TabItem value="cmd" label="CMD">

```bash
set TDENGINE_CLOUD_DSN=<DSN>
```

</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_CLOUD_DSN='<DSN>'
```

</TabItem>
</Tabs>

<!-- exclude -->
:::note
Replace  <DSN\> with real TDengine cloud DSN. To obtain the real value, please log in [TDengine Cloud](https://cloud.tdengine.com) and click "Connector" and then select "Rust".

:::
<!-- exclude-end -->

## Connect

Copy following code to `main.rs`.

```rust title="main.rs"
{{#include docs/examples/rust/cloud-example/src/main.rs}}
```

Then you can execute `cargo run` to test the connection.  For how to write data and query data, please refer to <https://docs.tdengine.com/cloud/data-in/insert-data/> and <https://docs.tdengine.com/cloud/data-out/query-data/>.


For more details about how to write or query data via REST API, please check [REST API](https://docs.tdengine.com/cloud/programming/connector/rest-api/).
