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
taos = { version = "*", default-features = false, features = ["ws"] }
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
:::note
替换 <DSN\> 为 真实的值，格式应该是 `https(<cloud_endpoint>)/?token=<token>`。

获取真实的 `DSN` 的值，请登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”编程“菜单，然后选择”Rust“。

:::
<!-- exclude-end -->

## 建立连接

复制下面的代码到 `main.rs` 文件。

```rust title="main.rs"
{{#include docs/examples/rust/cloud-example/src/main.rs}}
```

客户端连接建立连接以后，想了解更多写入数据和查询数据的内容，请参考 <https://docs.taosdata.com/cloud/data-in/insert-data/> and <https://docs.taosdata.com/cloud/data-out/query-data/>.

想知道更多通过 REST 接口写入数据的详情，请参考[REST 接口](https://docs.taosdata.com/cloud/programming/connector/rest-api/).
