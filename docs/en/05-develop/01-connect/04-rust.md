---
sidebar_label: Rust
title: Connect with Rust Connector
pagination_next: develop/insert-data
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Create Project

```
cargo new cloud_example
```
## Add Dependency

Add dependency to `Cargo.toml`. 

```toml title="Cargo.toml"
[dependencies]
[dependencies]
libtaos = { version="0.4.5-alpha.0", features=["rest"]}
tokio = { version = "1", features = ["full"]}
anyhow = "*"
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
set TDENGINE_CLOUD_DSN="<DSN>"
```

</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_CLOUD_DSN="<DSN>"
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
{{#include docs/examples/rust/cloud_example/src/main.rs}}
```

Then you can execute `cargo run` to test the connection.