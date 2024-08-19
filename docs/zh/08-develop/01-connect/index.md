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

TDengine 提供了丰富的应用程序开发接口，为了便于用户快速开发自己的应用，TDengine 支持了多种编程语言的连接器，其中官方连接器包括支持 C/C++、Java、Python、Go、Node.js、C#、Rust、Lua（社区贡献）和 PHP （社区贡献）的连接器。这些连接器支持使用原生接口（taosc）和 REST 接口（部分语言暂不支持）连接 TDengine 集群。社区开发者也贡献了多个非官方连接器，例如 ADO.NET 连接器、Lua 连接器和 PHP 连接器。另外 TDengine 还可以直接调用 taosadapter 提供的 REST API 接口，进行数据写入和查询操作。

## 连接方式

连接器建立连接的方式，TDengine 提供三种:

1. 通过客户端驱动程序 taosc 直接与服务端程序 taosd 建立连接，这种连接方式下文中简称 “原生连接”。
2. 通过 taosAdapter 组件提供的 REST API 建立与 taosd 的连接，这种连接方式下文中简称 “REST 连接”
3. 通过 taosAdapter 组件提供的 Websocket API 建立与 taosd 的连接，这种连接方式下文中简称 “Websocket 连接”

![TDengine connection type](connection-type-zh.webp)

无论使用何种方式建立连接，连接器都提供了相同或相似的 API 操作数据库，都可以执行 SQL 语句，只是初始化连接的方式稍有不同，用户在使用上不会感到什么差别。
各种连接方式和各语言连接器支持情况请参考：[连接器功能特性](../../reference/connector/#功能特性)

关键不同点在于：

1. 使用 原生连接，需要保证客户端的驱动程序 taosc 和服务端的 TDengine 版本配套。
2. 使用 REST 连接，用户无需安装客户端驱动程序 taosc，具有跨平台易用的优势，但是无法体验数据订阅和二进制数据类型等功能。另外与 原生连接 和 Websocket 连接相比，REST连接的性能最低。REST 接口是无状态的。在使用 REST 连接时，需要在 SQL 中指定表、超级表的数据库名称。  
3. 使用 Websocket 连接，用户也无需安装客户端驱动程序 taosc。
4. 连接云服务实例，必须使用 REST 连接 或 Websocket 连接。

**推荐使用 WebSocket 连接**

## 安装客户端驱动 taosc

如果选择原生连接，而且应用程序不在 TDengine 同一台服务器上运行，你需要先安装客户端驱动，否则可以跳过此一步。为避免客户端驱动和服务端不兼容，请使用一致的版本。

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

以上安装和配置完成后，并确认 TDengine 服务已经正常启动运行，此时可以执行安装包里带有的 TDengine 命令行程序 taos 进行登录。

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
  <version>3.3.0</version>
</dependency>
```

</TabItem>
<TabItem label="Python" value="python">

- **安装前准备**
    - 安装 Python。新近版本 taospy 包要求 Python 3.6.2+。早期版本 taospy 包要求 Python 3.7+。taos-ws-py 包要求 Python 3.7+。如果系统上还没有 Python 可参考 [Python BeginnersGuide](https://wiki.python.org/moin/BeginnersGuide/Download) 安装。
    - 安装 [pip](https://pypi.org/project/pip/)。大部分情况下 Python 的安装包都自带了 pip 工具， 如果没有请参考 [pip documentation](https://pip.pypa.io/en/stable/installation/) 安装。
    - 如果使用原生连接，还需[安装客户端驱动](../connect/#安装客户端驱动-taosc)。客户端软件包含了 TDengine 客户端动态链接库(libtaos.so 或 taos.dll) 和 TDengine CLI。

- **使用 pip 安装**
    - 卸载旧版本
        如果以前安装过旧版本的 Python 连接器, 请提前卸载。
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
            pip3 install taospy==2.3.0
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
        :::note 此安装包为 Websocket 连接器
    - 同时安装 `taospy` 和 `taos-ws-py`
        ```bash
        pip3 install taospy[ws]
        ```                     

  - **安装验证**
    <Tabs defaultValue="rest">
    <TabItem value="native" label="原生连接">
    对于原生连接，需要验证客户端驱动和 Python 连接器本身是否都正确安装。如果能成功导入 `taos` 模块，则说明已经正确安装了客户端驱动和 Python 连接器。可在 Python 交互式 Shell 中输入：
    ```python
    import taos
    ```
    </TabItem>
    <TabItem  value="rest" label="REST 连接">
    对于 REST 连接，只需验证是否能成功导入 `taosrest` 模块。可在 Python 交互式 Shell 中输入：
    ```python
    import taosrest
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
Rust 连接器通过不同的特性区分不同的连接方式。默认同时支持原生连接和 Websocket 连接，如果仅需要建立 Websocket 连接，可设置 `ws` 特性：

```toml
taos = { version = "*", default-features = false, features = ["ws"] }
```

:::

</TabItem>
<TabItem label="Node.js" value="node">

- **安装前准备**
    - 安装 Node.js 开发环境, 使用14以上版本。下载链接： https://nodejs.org/en/download/

- **安装**
    - 使用 npm 安装 Node.js 连接器
    ```
    npm install @tdengine/websocket
    ```
    :::note Node.js 目前只支持 Websocket 连接
- **安装验证**
    - 新建安装验证目录，例如：`~/tdengine-test`，下载 GitHub 上 [nodejsChecker.js 源代码](https://github.com/taosdata/TDengine/tree/main/docs/examples/node/websocketexample/nodejsChecker.js)到本地。
    - 在命令行中执行以下命令。
    ```bash
    npm init -y
    npm install @tdengine/websocket
    node nodejsChecker.js
    ```
    - 执行以上步骤后，在命令行会输出 nodeChecker.js 连接 TDengine 实例，并执行简单插入和查询的结果。



</TabItem>
<TabItem label="C#" value="csharp">

编辑项目配置文件中添加 [TDengine.Connector](https://www.nuget.org/packages/TDengine.Connector/) 的引用即可：

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

如果已经安装了 TDengine 服务端软件或 TDengine 客户端驱动 taosc， 那么已经安装了 C 连接器，无需额外操作。

</TabItem>
<TabItem label="REST API" value="rest">
使用 REST API 方式访问 TDengine，无需安装任何驱动和连接器。

</TabItem>
</Tabs>

## 建立连接

在执行这一步之前，请确保有一个正在运行的，且可以访问到的 TDengine，而且服务端的 FQDN 配置正确。以下示例代码，都假设 TDengine 安装在本机，且 FQDN（默认 localhost） 和 serverPort（默认 6030） 都使用默认配置。

### 连接参数
连接的配置项较多，因此在建立连接之前，我们能先介绍一下各语言连接器建立连接使用的参数。

<Tabs defaultValue="java" groupId="lang">
    <TabItem label="Java" value="java">
    Java 连接器建立连接的参数有 URL 和 Properties。  
    TDengine 的 JDBC URL 规范格式为：
    `jdbc:[TAOS|TAOS-RS]://[host_name]:[port]/[database_name]?[user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}|&batchfetch={batchfetch}]`  

    URL 和 Properties 的详细参数说明和如何使用详见 [url 规范](../../reference/connector/java/#url-规范)

    **注**：REST 连接中增加 `batchfetch` 参数并设置为 true，将开启 WebSocket 连接。

    </TabItem>
    <TabItem label="Python" value="python">
    Python 连接器使用 `connect()` 方法来建立连接，下面是连接参数的具体说明：    
        - url： `taosAdapter` REST 服务的 URL。默认是 `localhost` 的 `6041` 端口。 
        - user： TDengine 用户名。默认是 `root`。  
        - password： TDengine 用户密码。默认是 `taosdata`。  
        - timeout： HTTP 请求超时时间。单位为秒。默认为 `socket._GLOBAL_DEFAULT_TIMEOUT`。一般无需配置。

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

    支持的 DSN 参数如下

    原生连接：

    - `cfg` 指定 taos.cfg 目录
    - `cgoThread` 指定 cgo 同时执行的数量，默认为系统核数
    - `cgoAsyncHandlerPoolSize` 指定异步函数的 handle 大小，默认为 10000

    REST 连接：

    - `disableCompression` 是否接受压缩数据，默认为 true 不接受压缩数据，如果传输数据使用 gzip 压缩设置为 false。
    - `readBufferSize` 读取数据的缓存区大小默认为 4K（4096），当查询结果数据量多时可以适当调大该值。
    - `token` 连接云服务时使用的 token。
    - `skipVerify` 是否跳过证书验证，默认为 false 不跳过证书验证，如果连接的是不安全的服务设置为 true。

    WebSocket 连接：

    - `enableCompression` 是否发送压缩数据，默认为 false 不发送压缩数据，如果传输数据使用压缩设置为 true。
    - `readTimeout` 读取数据的超时时间，默认为 5m。
    - `writeTimeout` 写入数据的超时时间，默认为 10s。

    </TabItem>
    <TabItem label="Rust" value="rust">
Rust 连接器使用 DSN 来创建连接， DSN 描述字符串基本结构如下：

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
    - **host/port**: 主机地址和端口号。例如`localhost:6041`
    - **database**: 数据库名称。
    - **params**: 其他参数。 例如token。

    - 完整 DSN 示例：

    ```js
        ws://root:taosdata@localhost:6041
    ``` 
    </TabItem>

    <TabItem label="C#" value="csharp">
    ConnectionStringBuilder 使用 key-value 对方式设置连接参数，key 为参数名，value 为参数值，不同参数之间使用分号 `;` 分割。

    例如：

    ```csharp
    "protocol=WebSocket;host=127.0.0.1;port=6041;useSSL=false"
    ```
    支持的参数如下：

    - `host`：TDengine 运行实例的地址。
    - `port`：TDengine 运行实例的端口。
    - `username`：连接的用户名。
    - `password`：连接的密码。
    - `protocol`：连接的协议，可选值为 Native 或 WebSocket，默认为 Native。
    - `db`：连接的数据库。
    - `timezone`：时区，默认为本地时区。
    - `connTimeout`：连接超时时间，默认为 1 分钟。

    WebSocket 连接额外支持以下参数：

    - `readTimeout`：读取超时时间，默认为 5 分钟。
    - `writeTimeout`：发送超时时间，默认为 10 秒。
    - `token`：连接 TDengine cloud 的 token。
    - `useSSL`：是否使用 SSL 连接，默认为 false。
    - `enableCompression`：是否启用 WebSocket 压缩，默认为 false。
    - `autoReconnect`：是否自动重连，默认为 false。
    - `reconnectRetryCount`：重连次数，默认为 3。
    - `reconnectIntervalMs`：重连间隔毫秒时间，默认为 2000。
    </TabItem>
    <TabItem label="C" value="c">
C/C++ 语言连接器使用 `taos_connect()` 函数用于建立与 TDengine 数据库的连接。其参数详细说明如下：

- `host`：要连接的数据库服务器的主机名或IP地址。如果是本地数据库，可以使用 `"localhost"`。
- `user`：用于登录数据库的用户名。
- `passwd`：与用户名对应的密码。
- `db`：连接时默认选择的数据库名。如果不指定数据库，可以传递 `NULL` 或空字符串。
- `port`：数据库服务器监听的端口号。默认的端口号是 `6030`。

还提供了 `taos_connect_auth()` 函数用于使用 MD5 加密的密码建立与 TDengine 数据库的连接。此函数与 `taos_connect` 功能相同，不同之处在于密码的处理方式，`taos_connect_auth` 需要的是密码的 MD5 加密字符串。

    </TabItem>
<TabItem label="REST API" value="rest">
通过 REST API 方式访问 TDengine 时，应用程序直接与 taosAdapter 建立 HTTP 连接，建议使用连接池来管理连接。
使用 REST API 的参数具体可以参考：[http-请求格式](../../reference/connector/rest-api/#http-请求格式)

</TabItem>
</Tabs>

### Websocket 连接
下面是各语言连接器建立 Websocket 连接代码样例。演示了如何使用 Websocket 连接方式连接到 TDengine 数据库，并对连接设定一些参数。整个过程主要涉及到数据库连接的建立和异常处理。

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
不支持
</TabItem>    
<TabItem label="REST API" value="rest">
不支持

</TabItem>    
</Tabs>

### 原生连接
下面是各语言连接器建立原生连接代码样例。演示了如何使用原生连接方式连接到 TDengine 数据库，并对连接设定一些参数。整个过程主要涉及到数据库连接的建立和异常处理。
 
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

<TabItem label="REST API" value="rest">
不支持

</TabItem>   
</Tabs>

### REST 连接
下面是各语言连接器建立 REST 连接代码样例。演示了如何使用 REST 连接方式连接到 TDengine 数据库。整个过程主要涉及到数据库连接的建立和异常处理。
 
<Tabs defaultValue="java" groupId="lang">
    <TabItem label="Java" value="java">
```java
{{#include docs/examples/java/src/main/java/com/taos/example/RESTConnectExample.java:main}}
```
    </TabItem>
    <TabItem label="Python" value="python">
```python
{{#include docs/examples/python/connect_rest_example.py:connect}}
```
    </TabItem>
    <TabItem label="Go" value="go">
```go
{{#include docs/examples/go/connect/restexample/main.go}}
```
    </TabItem>
<TabItem label="Rust" value="rust">
不支持
</TabItem>
<TabItem label="Node.js" value="node">
不支持
</TabItem>
<TabItem label="C#" value="csharp">
不支持
</TabItem>
<TabItem label="C" value="c">
不支持
</TabItem>

<TabItem label="REST API" value="rest">
使用 REST API 方式访问 TDengine，由应用程序自主去建立 HTTP 连接。

</TabItem>       
</Tabs>


:::tip
如果建立连接失败，大部分情况下是 FQDN 或防火墙的配置不正确，详细的排查方法请看[《常见问题及反馈》](https://docs.taosdata.com/train-faq/faq)中的“遇到错误 Unable to establish connection, 我怎么办？”

:::


## 连接池
有些连接器提供了连接池，或者可以与已有的连接池组件配合使用。 使用连接池，应用程序可以快速地从连接池中获取可用连接，避免了每次操作时创建和销毁连接的开销。这不仅减少了资源消耗，还提高了响应速度。此外，连接池还支持对连接的管理，如最大连接数限制、连接的有效性检查，确保了连接的高效和可靠使用。我们**推荐使用连接池管理连接**。  
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
        <ConnPythonNative />
    </TabItem>
    <TabItem label="Go" value="go">

使用 `sql.Open` 创建出来的连接已经实现了连接池，可以通过 API 设置连接池参数，样例如下

```go
{{#include docs/examples/go/connect/connpool/main.go:pool}}
```

    </TabItem>
    <TabItem label="Rust" value="rust">

在复杂应用中，建议启用连接池。[taos] 的连接池默认（异步模式）使用 [deadpool] 实现。

如下，可以生成一个默认参数的连接池。

```rust
let pool: Pool<TaosBuilder> = TaosBuilder::from_dsn("taos:///")
    .unwrap()
    .pool()
    .unwrap();
```

同样可以使用连接池的构造器，对连接池参数进行设置：

```rust
let pool: Pool<TaosBuilder> = Pool::builder(Manager::from_dsn(self.dsn.clone()).unwrap().0)
    .max_size(88)  // 最大连接数
    .build()
    .unwrap();
```

在应用代码中，使用 `pool.get()?` 来获取一个连接对象 [Taos]。

```rust
let taos = pool.get()?;
```

    </TabItem>


</Tabs>
