---
sidebar_label: Rust
title: 使用 Rust 连接器建立连接
description: 使用 Rust 连接器建立和 TDengine Cloud 的连接
---
<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->
## 创建项目

```bash
cargo new --bin cloud-example
```

## 增加依赖

在 `Cargo.toml` 文件中增加下面的依赖：

```toml title="Cargo.toml"
[package]
name = "cloud-example"
version = "0.1.0"
edition = "2021"

[dependencies]
taos = { version = "*", default-features = false, features = ["ws", "ws-rustls"] }
tokio = { version = "1", features = ["full"]}
anyhow = "1.0.0" 
```

## 配置

在您的终端里面执行下面的命令设置 TDengine Cloud 的 DSN 为环境变量：

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
:::note IMPORTANT

替换 \<DSN> 为 您要访问的 TDengine Cloud 实例的 DSN 值，格式应该是 `wss://<cloud_endpoint>?token=<token>`。

获取实例的真实 `DSN` 的值，请登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”编程“菜单，然后选择”Rust“。

:::
<!-- exclude-end -->

## 建立连接

复制下面的代码到 `main.rs` 文件，执行`cargo run`观察运行结果。

```rust title="main.rs"
{{#include docs/examples/rust/cloud-example/src/main.rs}}
```

关于如何写入数据和查询数据，请参考[写入数据](https://docs.taosdata.com/cloud/programming/insert)和[查询数据](https://docs.taosdata.com/cloud/programming/query)。

更多关于 REST 接口的详情，请参考 [REST 接口](https://docs.taosdata.com/cloud/programming/client-libraries/rest-api/)。
