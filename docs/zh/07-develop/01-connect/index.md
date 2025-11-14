---
title: 建立连接
sidebar_label: 建立连接
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import ConnJava from "./_connect_java.mdx";
import ConnGo from "./_connect_go.mdx";
import ConnRust from "./_connect_rust.mdx";
import ConnNode from "./_connect_node.mdx";
import ConnPythonNative from "./_connect_python.mdx";
import ConnCSNative from "./_connect_cs.mdx";
import ConnC from "./_connect_c.mdx";
import InstallOnLinux from "../../14-reference/05-connector/_linux_install.mdx";
import InstallOnWindows from "../../14-reference/05-connector/_windows_install.mdx";
import InstallOnMacOS from "../../14-reference/05-connector/_macos_install.mdx";
import VerifyLinux from "../../14-reference/05-connector/_verify_linux.mdx";
import VerifyMacOS from "../../14-reference/05-connector/_verify_macos.mdx";
import VerifyWindows from "../../14-reference/05-connector/_verify_windows.mdx";
import ConnectorType from "../../14-reference/05-connector/_connector_type.mdx";

<ConnectorType /> 

## 安装客户端驱动 taosc

如果选择原生连接，而且应用程序不在 TDengine TSDB 同一台服务器上运行，你需要先安装客户端驱动，否则可以跳过此一步。为避免客户端驱动和服务端不兼容，请使用一致的版本。

### 安装步骤

<Tabs defaultValue="linux" groupId="os">
  <TabItem value="linux" label="Linux">
    <InstallOnLinux />
  </TabItem>
  <TabItem value="windows" label="Windows">
    <InstallOnWindows />
  </TabItem>
  <TabItem value="macos" label="macOS">
    <InstallOnMacOS />
  </TabItem>
</Tabs>

### 安装验证

以上安装和配置完成后，并确认 TDengine TSDB 服务已经正常启动运行，此时可以执行安装包里带有的 TDengine TSDB 命令行程序 taos 进行登录。

<Tabs defaultValue="linux" groupId="os">
  <TabItem value="linux" label="Linux">
    <VerifyLinux />
  </TabItem>
  <TabItem value="windows" label="Windows">
    <VerifyWindows />
  </TabItem>
  <TabItem value="macos" label="macOS">
    <VerifyMacOS />
  </TabItem>
</Tabs>

## 安装连接器

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

如果使用 Maven 管理项目，只需在 pom.xml 中加入以下依赖。

```xml
<dependency>
  <groupId>com.taosdata.jdbc</groupId>
  <artifactId>taos-jdbcdriver</artifactId>
  <version>3.7.7</version>
</dependency>
```

</TabItem>
<TabItem label="Python" value="python">

- **安装前准备**
  - 安装 Python。新近版本 taospy 包要求 Python 3.6.2+。早期版本 taospy 包要求 Python 3.7+。taos-ws-py 包要求 Python 3.7+。如果系统上还没有 Python 可参考 [Python BeginnersGuide](https://wiki.python.org/moin/BeginnersGuide/Download) 安装。
  - 安装 [pip](https://pypi.org/project/pip/)。大部分情况下 Python 的安装包都自带了 pip 工具，如果没有请参考 [pip documentation](https://pip.pypa.io/en/stable/installation/) 安装。
  - 如果使用原生连接，还需 [安装客户端驱动](../connect/#安装客户端驱动-taosc)。客户端软件包含了 TDengine TSDB 客户端动态链接库 (libtaos.so 或 taos.dll) 和 TDengine TSDB CLI。

- **使用 pip 安装**
  - 卸载旧版本
        如果以前安装过旧版本的 Python 连接器，请提前卸载。

        ```
        pip3 uninstall taos taospy
        pip3 uninstall taos  taos-ws-py
        ```

  - 安装 `taospy`
    - 最新版本

            ```
            pip3 install taospy
            ```

    - 指定某个特定版本安装

            ```
            pip3 install taospy==2.8.6
            ```

    - 从 GitHub 安装

            ```
            pip3 install git+https://github.com/taosdata/taos-connector-python.git
            ```

        :::note 此安装包为原生连接器
  - 安装 `taos-ws-py`

        ```bash
        pip3 install taos-ws-py
        ```

        :::note 此安装包为 WebSocket 连接器
  - 同时安装 `taospy` 和 `taos-ws-py`

        ```bash
        pip3 install taospy[ws]
        ```

- **安装验证**
    <Tabs defaultValue="ws">
    <TabItem value="native" label="原生连接">
    对于原生连接，需要验证客户端驱动和 Python 连接器本身是否都正确安装。如果能成功导入 `taos` 模块，则说明已经正确安装了客户端驱动和 Python 连接器。可在 Python 交互式 Shell 中输入：

    ```python
    import taos
    ```

    </TabItem>
    <TabItem  value="ws" label="WebSocket 连接">
    对于 WebSocket 连接，只需验证是否能成功导入 `taosws` 模块。可在 Python 交互式 Shell 中输入：
    ```python
    import taosws
    ```
    </TabItem>
    </Tabs>

</TabItem>
<TabItem label="Go" value="go">

编辑 `go.mod` 添加 `driver-go` 依赖即可。

```go-mod title=go.mod
module goexample

go 1.17

require github.com/taosdata/driver-go/v3 latest
```

:::note
driver-go 使用 cgo 封装了 taosc 的 API。cgo 需要使用 GCC 编译 C 的源码。因此需要确保你的系统上有 GCC。

:::

</TabItem>
<TabItem label="Rust" value="rust">

编辑 `Cargo.toml` 添加 `taos` 依赖即可。

```toml title=Cargo.toml
[dependencies]
taos = { version = "*"}
```

:::info
Rust 连接器通过不同的特性区分不同的连接方式。默认同时支持原生连接和 WebSocket 连接，如果仅需要建立 WebSocket 连接，可设置 `ws` 特性：

```toml
taos = { version = "*", default-features = false, features = ["ws"] }
```

:::

</TabItem>
<TabItem label="Node.js" value="node">

- **安装前准备**
  - 安装 Node.js 开发环境，使用 14 以上版本。下载链接：[Download Node.js](https://nodejs.org/en/download)

- **安装**
  - 使用 npm 安装 Node.js 连接器

        ```
        npm install @tdengine/websocket
        ```

    :::note Node.js 目前只支持 WebSocket 连接
- **安装验证**
  - 新建安装验证目录，例如：`~/tdengine-test`，下载 GitHub 上 [nodejsChecker.js 源代码](https://github.com/taosdata/TDengine/tree/main/docs/examples/node/websocketexample/nodejsChecker.js) 到本地。
  - 在命令行中执行以下命令。

        ```bash
        npm init -y
        npm install @tdengine/websocket
        node nodejsChecker.js
        ```

  - 执行以上步骤后，在命令行会输出 nodeChecker.js 连接 TDengine TSDB 实例，并执行简单插入和查询的结果。

</TabItem>
<TabItem label="C#" value="csharp">

编辑项目配置文件中添加 [TDengine TSDB.Connector](https://www.nuget.org/packages/TDengine.Connector/) 的引用即可：

```xml title=csharp.csproj
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net6.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <StartupObject>TDengineExample.AsyncQueryExample</StartupObject>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="TDengine.Connector" Version="3.1.0" />
  </ItemGroup>

</Project>
```

也可通过 dotnet 命令添加：

```
dotnet add package TDengine.Connector
```

:::note
以下示例代码，均基于 dotnet6.0，如果使用其它版本，可能需要做适当调整。

:::

</TabItem>
<TabItem label="C" value="c">

如果已经安装了 TDengine TSDB 服务端软件或 TDengine TSDB 客户端驱动 taosc，那么已经安装了 C 连接器，无需额外操作。

</TabItem>
<TabItem label="REST API" value="rest">
使用 REST API 方式访问 TDengine TSDB，无需安装任何驱动和连接器。

</TabItem>
</Tabs>

## 建立连接

在执行这一步之前，请确保有一个正在运行的，且可以访问到的 TDengine TSDB，而且服务端的 FQDN 配置正确。以下示例代码，都假设 TDengine TSDB 安装在本机，且 FQDN（默认 localhost）和 serverPort（默认 6030）都使用默认配置。

### 连接参数

连接的配置项较多，因此在建立连接之前，我们能先介绍一下各语言连接器建立连接使用的参数。

<Tabs defaultValue="java" groupId="lang">
    <TabItem label="Java" value="java">
    Java 连接器建立连接的参数有 URL 和 Properties。  
    TDengine TSDB 的 JDBC URL 规范格式为：
    `jdbc:[TAOS|TAOS-WS]://[host_name]:[port]/[database_name]?[user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}|&batchfetch={batchfetch}]`  

    URL 和 Properties 的详细参数说明和如何使用详见 [url 规范](../../reference/connector/java/#url-规范)

    </TabItem>
    <TabItem label="Python" value="python">
    Python 连接器使用 `connect()` 方法来建立连接，下面是连接参数的具体说明：    
        - url： `taosAdapter` Websocket 服务的 URL。默认是 `localhost` 的 `6041` 端口。 
        - user： TDengine TSDB 用户名。默认是 `root`。  
        - password： TDengine TSDB 用户密码。默认是 `taosdata`。  
        - timeout： HTTP 请求超时时间。单位为秒。默认为 `socket._GLOBAL_DEFAULT_TIMEOUT`。一般无需配置。

    URL 的详细参数说明和如何使用详见 [url 规范](../../reference/connector/python/#url-规范)

    </TabItem>
    <TabItem label="Go" value="go">

    数据源名称具有通用格式，例如 [PEAR DB](http://pear.php.net/manual/en/package.database.db.intro-dsn.php)，但没有类型前缀（方括号表示可选）：

    ``` text
    [username[:password]@][protocol[(address)]]/[dbname][?param1=value1&...&paramN=valueN]
    ```

    完整形式的 DSN：

    ```text
    username:password@protocol(address)/dbname?param=value
    ```

    当使用 IPv6 地址时（v3.7.1 及以上版本支持），地址需要用方括号括起来，例如：

    ```text
    root:taosdata@ws([::1]:6041)/testdb
    ```

    支持的 DSN 参数如下

    原生连接：

    - `cfg` 指定 taos.cfg 目录。
    - `cgoThread` 指定 cgo 同时执行的数量，默认为系统核数。
    - `cgoAsyncHandlerPoolSize` 指定异步函数的 handle 大小，默认为 10000。
    - `timezone` 指定连接使用的时区，sql 解析以及查询结果都会按照此时区进行转换，只支持 IANA 时区格式，特殊字符需要进行编码，以上海时区（`Asia/Shanghai`）为例：`timezone=Asia%2FShanghai`。

    WebSocket 连接：

    - `enableCompression` 是否发送压缩数据，默认为 false 不发送压缩数据，如果传输数据使用压缩设置为 true。
    - `readTimeout` 读取数据的超时时间，默认为 5m。
    - `writeTimeout` 写入数据的超时时间，默认为 10s。
    - `timezone` 指定连接使用的时区，sql 解析以及查询结果都会按照此时区进行转换，只支持 IANA 时区格式，特殊字符需要进行编码，以上海时区（`Asia/Shanghai`）为例：`timezone=Asia%2FShanghai`。

    </TabItem>
    <TabItem label="Rust" value="rust">
Rust 连接器使用 DSN 来创建连接，DSN 描述字符串基本结构如下：

```text
<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|------------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  database  |  params               |
```

DSN 的详细说明和如何使用详见 [连接功能](../../reference/connector/rust/#连接功能)

    </TabItem>
    <TabItem label="Node.js" value="node">
    Node.js 连接器使用 DSN 来创建连接， DSN 描述字符串基本结构如下：

    ```text
    [+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
    |------------|---|-----------|-----------|------|------|------------|-----------------------|
    |   protocol |   | username  | password  | host | port |  database  |  params               |
    ```

    - **protocol**: 使用 websocket 协议建立连接。例如`ws://localhost:6041`
    - **username/password**: 数据库的用户名和密码。
    - **host/port**: 参数支持合法的域名或 IP 地址。`@tdengine/websocket` 同时支持 IPV4 和 IPV6 两种地址格式，对于 IPv6 地址，必须使用中括号括起来（例如 `[::1]` 或 `[2001:db8:1234:5678::1]`），以避免端口号解析冲突。
    - **database**: 数据库名称。
    - **params**: 其他参数。例如 token。

    - 完整 DSN 示例：
    
    ```js
        // IPV4:
        ws://root:taosdata@localhost:6041
    
        // IPV6:
        ws://root:taosdata@[::1]:6041
    ``` 

    </TabItem>

    <TabItem label="C#" value="csharp">
    ConnectionStringBuilder 使用 key-value 对方式设置连接参数，key 为参数名，value 为参数值，不同参数之间使用分号 `;` 分割。

    例如：

    ```csharp
    "protocol=WebSocket;host=127.0.0.1;port=6041;useSSL=false"
    ```
    支持的参数如下：

    - `host`：TDengine TSDB 运行实例的地址。
    - `port`：TDengine TSDB 运行实例的端口。
    - `username`：连接的用户名。
    - `password`：连接的密码。
    - `protocol`：连接的协议，可选值为 Native 或 WebSocket，默认为 Native。
    - `db`：连接的数据库。
    - `timezone`：时区，默认为本地时区。
    - `connTimeout`：连接超时时间，默认为 1 分钟。

    WebSocket 连接额外支持以下参数：

    - `readTimeout`：读取超时时间，默认为 5 分钟。
    - `writeTimeout`：发送超时时间，默认为 10 秒。
    - `token`：连接 TDengine TSDB cloud 的 token。
    - `useSSL`：是否使用 SSL 连接，默认为 false。
    - `enableCompression`：是否启用 WebSocket 压缩，默认为 false。
    - `autoReconnect`：是否自动重连，默认为 false。
    - `reconnectRetryCount`：重连次数，默认为 3。
    - `reconnectIntervalMs`：重连间隔毫秒时间，默认为 2000。
    </TabItem>
    <TabItem label="C" value="c">

C/C++ 连接器使用 `taos_connect()` 函数建立与 TDengine TSDB 数据库的连接。各参数说明如下：

- `host`：数据库服务器的主机名或 IP 地址。如果是本地数据库，可以使用 `"localhost"`。
- `user`：数据库登录用户名。
- `passwd`：对应用户名的登录密码。
- `db`：连接时默认使用的数据库名。如果不指定数据库，可以传递 `NULL` 或空字符串。
- `port`：数据库服务器监听的端口号。原生连接默认端口为 `6030`，WebSocket 连接默认端口为 `6041`。

WebSocket 连接需要先调用 `taos_options(TSDB_OPTION_DRIVER, "websocket")` 设置驱动类型，然后再调用 `taos_connect()` 建立连接。

原生连接还提供 `taos_connect_auth()` 函数，用于使用 MD5 加密的密码建立连接。该函数与 `taos_connect()` 功能相同，区别在于密码的处理方式，`taos_connect_auth()` 需要的是密码的 MD5 加密字符串。

    </TabItem>
<TabItem label="REST API" value="rest">
通过 REST API 方式访问 TDengine TSDB 时，应用程序直接与 taosAdapter 建立 HTTP 连接，建议使用连接池来管理连接。
使用 REST API 的参数具体可以参考：[http-请求格式](../../reference/connector/rest-api/#http-请求格式)

</TabItem>
</Tabs>

### WebSocket 连接

下面是各语言连接器建立 WebSocket 连接代码样例。演示了如何使用 WebSocket 连接方式连接到 TDengine TSDB 数据库，并对连接设定一些参数。整个过程主要涉及到数据库连接的建立和异常处理。

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">
```java
{{#include docs/examples/java/src/main/java/com/taos/example/WSConnectExample.java:main}}
```
    </TabItem>
    <TabItem label="Python" value="python">
```python
{{#include docs/examples/python/connect_websocket_examples.py:connect}}
```

SQLAlchemy 支持通过 `hosts` 参数配置多个服务器地址，实现负载均衡和故障转移功能。多个地址使用英文逗号分隔，格式为：`hosts=<host1>:<port1>,<host2>:<port2>,...`

```python
{{#include docs/examples/python/connect_websocket_sqlalchemy_examples.py:connect_sqlalchemy}}
```
    </TabItem>
    <TabItem label="Go" value="go">
```go
{{#include docs/examples/go/connect/wsexample/main.go}}
```
    </TabItem>
    <TabItem label="Rust" value="rust">
```rust
{{#include docs/examples/rust/restexample/examples/connect.rs}}
```
    </TabItem>
    <TabItem label="Node.js" value="node">
```js
{{#include docs/examples/node/websocketexample/sql_example.js:createConnect}}
```
    </TabItem>
    <TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/wsConnect/Program.cs:main}}
```
    </TabItem>
<TabItem label="C" value="c">
```c
{{#include docs/examples/c-ws-new/connect_example.c}}
```

</TabItem>
</Tabs>

### 原生连接

下面是各语言连接器建立原生连接代码样例。演示了如何使用原生连接方式连接到 TDengine TSDB 数据库，并对连接设定一些参数。整个过程主要涉及到数据库连接的建立和异常处理。

<Tabs defaultValue="java" groupId="lang">
    <TabItem label="Java" value="java">
```java
{{#include docs/examples/java/src/main/java/com/taos/example/JNIConnectExample.java:main}}
```
</TabItem>
<TabItem label="Python" value="python">
    <ConnPythonNative />
</TabItem>
<TabItem label="Go" value="go">
```go
{{#include docs/examples/go/connect/cgoexample/main.go}}
```
</TabItem>
<TabItem label="Rust" value="rust">
```rust
{{#include docs/examples/rust/nativeexample/examples/connect.rs}}
```
</TabItem>
<TabItem label="Node.js" value="node">
不支持
</TabItem>
<TabItem label="C#" value="csharp">
```csharp
{{#include docs/examples/csharp/connect/Program.cs:main}}
```
</TabItem>
<TabItem label="C" value="c">
    <ConnC />
</TabItem>

</Tabs>

:::tip
如果建立连接失败，大部分情况下是 FQDN 或防火墙的配置不正确，详细的排查方法请看[《常见问题及反馈》](https://docs.taosdata.com/train-faq/faq)中的“遇到错误 Unable to establish connection, 我怎么办？”

:::

## 连接池

有些连接器提供了连接池，或者可以与已有的连接池组件配合使用。使用连接池，应用程序可以快速地从连接池中获取可用连接，避免了每次操作时创建和销毁连接的开销。这不仅减少了资源消耗，还提高了响应速度。此外，连接池还支持对连接的管理，如最大连接数限制、连接的有效性检查，确保了连接的高效和可靠使用。我们**推荐使用连接池管理连接**。  
下面是各语言连接器的连接池支持代码样例。  

<Tabs defaultValue="java" groupId="lang">
    <TabItem label="Java" value="java">

**HikariCP**  

使用示例如下：

```java
{{#include docs/examples/java/src/main/java/com/taos/example/HikariDemo.java:connection_pool}}
```

> 通过 HikariDataSource.getConnection() 获取连接后，使用完成后需要调用 close() 方法，实际上它并不会关闭连接，只是放回连接池中。
> 更多 HikariCP 使用问题请查看[官方说明](https://github.com/brettwooldridge/HikariCP)。

**Druid**  

使用示例如下：

```java
{{#include docs/examples/java/src/main/java/com/taos/example/DruidDemo.java:connection_pool}}
```

> 更多 druid 使用问题请查看[官方说明](https://github.com/alibaba/druid)。

    </TabItem>
    
    <TabItem label="Python" value="python">

<details>
<summary>SQLAlchemy 连接池示例（推荐使用）</summary>

```python
{{#include docs/examples/python/sqlalchemy_demo.py}}
```

</details>

<details>
<summary>DBUtils 连接池示例</summary>

```python
{{#include docs/examples/python/dbutils_demo.py}}
```

</details>



    </TabItem>
    <TabItem label="Go" value="go">

使用 `sql.Open` 创建出来的连接已经实现了连接池，可以通过 API 设置连接池参数，样例如下

```go
{{#include docs/examples/go/connect/connpool/main.go:pool}}
```

    </TabItem>
    <TabItem label="Rust" value="rust">

在复杂应用中，建议启用连接池。`taos` 的连接池在异步模式下使用 `deadpool` 实现。

创建默认参数的连接池：

```rust
let pool: Pool<TaosBuilder> = TaosBuilder::from_dsn("taos:///")
    .unwrap()
    .pool()
    .unwrap();
```

使用连接池构造器自定义参数：

```rust
let pool: Pool<TaosBuilder> = Pool::builder(Manager::from_dsn("taos:///").unwrap().0)
    .max_size(88) // 最大连接数
    .build()
    .unwrap();
```

从连接池获取连接对象：

```rust
let taos = pool.get().await?;
```

    </TabItem>

</Tabs>
