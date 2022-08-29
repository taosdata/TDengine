---
sidebar_label: Rust
title: Connect with Rust Connector
description: Connect to TDengine cloud service using Rust connector
---
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

## Create Project

```
cargo new --bin cloud-example
```
## Add Dependency

Add dependency to `Cargo.toml`. 

```toml title="Cargo.toml"
{{#include docs/examples/rust/cloud-example/Cargo.toml}}
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
{{#include docs/examples/rust/cloud-example/src/main.rs}}
```

Then you can execute `cargo run` to test the connection. 

