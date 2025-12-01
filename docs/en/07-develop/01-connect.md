---
title: Connecting to TDengine
slug: /developer-guide/connecting-to-tdengine
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import ConnJava from "../assets/resources/_connect_java.mdx";
import ConnGo from "../assets/resources/_connect_go.mdx";
import ConnRust from "../assets/resources/_connect_rust.mdx";
import ConnNode from "../assets/resources/_connect_node.mdx";
import ConnPythonNative from "../assets/resources/_connect_python.mdx";
import ConnCSNative from "../assets/resources/_connect_cs.mdx";
import ConnC from "../assets/resources/_connect_c.mdx";
import InstallOnLinux from "../assets/resources/_linux_install.mdx";
import InstallOnWindows from "../assets/resources/_windows_install.mdx";
import InstallOnMacOS from "../assets/resources/_macos_install.mdx";
import VerifyLinux from "../assets/resources/_verify_linux.mdx";
import VerifyMacOS from "../assets/resources/_verify_macos.mdx";
import VerifyWindows from "../assets/resources/_verify_windows.mdx";
import ConnectorType from "../assets/resources/_connector_type.mdx";

<ConnectorType /> 

## Installing the Client Driver taosc

If you choose a native connection and your application is not running on the same server as TDengine, you need to install the client driver first; otherwise, you can skip this step. To avoid incompatibility between the client driver and the server, please use consistent versions.

### Installation Steps

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

### Installation Verification

After completing the above installation and configuration, and confirming that the TDengine service has started running normally, you can log in using the TDengine command-line program `taos` included in the installation package.

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

## Installing Connectors

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

If you are using Maven to manage your project, simply add the following dependency to your pom.xml.

```xml
<dependency>
  <groupId>com.taosdata.jdbc</groupId>
  <artifactId>taos-jdbcdriver</artifactId>
  <version>3.5.2</version>
</dependency>
```

</TabItem>

<TabItem label="Python" value="python">

- **Pre-installation Preparation**
  - Install Python. Recent versions of the taospy package require Python 3.6.2+. Earlier versions of the taospy package require Python 3.7+. The taos-ws-py package requires Python 3.7+. If Python is not already installed on your system, refer to [Python BeginnersGuide](https://wiki.python.org/moin/BeginnersGuide/Download) for installation.
  - Install [pip](https://pypi.org/project/pip/). In most cases, the Python installation package comes with the pip tool; if not, refer to the [pip documentation](https://pip.pypa.io/en/stable/installation/) for installation.
  - If using a native connection, you also need to [install the client driver](../connecting-to-tdengine/). The client software package includes the TDengine client dynamic link library (libtaos.so or taos.dll) and TDengine CLI.

- **Using pip to Install**
  - Uninstall old versions
  If you have previously installed old versions of the Python connector, please uninstall them first.

  ```shell
  pip3 uninstall taos taospy
  pip3 uninstall taos  taos-ws-py
  ```

  - Install `taospy`
    - Latest version

    ```shell
    pip3 install taospy
    ```

    - Install a specific version

    ```shell
    pip3 install taospy==2.8.6
    ```

    - Install from GitHub

    ```shell
    pip3 install git+https://github.com/taosdata/taos-connector-python.git
    ```

    Note: This package is for native connection
  - Install `taos-ws-py`

  ```bash
  pip3 install taos-ws-py
  ```

  Note: This package is for WebSocket connection
  - Install both `taospy` and `taos-ws-py`

  ```bash
  pip3 install taospy[ws]
  ```

- **Installation Verification**

<Tabs defaultValue="rest">
<TabItem value="native" label="Native Connection">

For native connections, it is necessary to verify that both the client driver and the Python connector itself are correctly installed. If the `taos` module can be successfully imported, then the client driver and Python connector are correctly installed. You can enter in the Python interactive Shell:

```python
import taos
```

</TabItem>

<TabItem  value="rest" label="REST Connection">
For REST connections, you only need to verify if the `taosrest` module can be successfully imported. You can enter in the Python interactive Shell:

```python
import taosrest
```

</TabItem>

<TabItem  value="ws" label="WebSocket Connection">

For WebSocket connections, you only need to verify if the `taosws` module can be successfully imported. You can enter in the Python interactive Shell:

```python
import taosws
```

</TabItem>
</Tabs>
</TabItem>

<TabItem label="Go" value="go">

Edit `go.mod` to add the `driver-go` dependency.

```go-mod title=go.mod
module goexample

go 1.17

require github.com/taosdata/driver-go/v3 latest
```

:::note

driver-go uses cgo to wrap the taosc API. cgo requires GCC to compile C source code. Therefore, make sure GCC is installed on your system.

:::

</TabItem>

<TabItem label="Rust" value="rust">

Edit `Cargo.toml` to add the `taos` dependency.

```toml title=Cargo.toml
[dependencies]
taos = { version = "*"}
```

:::info

The Rust connector distinguishes different connection methods through different features. It supports both native and WebSocket connections by default. If only a WebSocket connection is needed, set the `ws` feature:

```toml
taos = { version = "*", default-features = false, features = ["ws"] }
```

:::

</TabItem>

<TabItem label="Node.js" value="node">

- **Pre-installation Preparation**
  - Install the Node.js development environment, using version 14 or above. Download link: [Download Node.js](https://nodejs.org/en/download)

- **Installation**
  - Use npm to install the Node.js connector

  ```shell
  npm install @tdengine/websocket
  ```

  Note: Node.js currently only supports WebSocket connections

- **Installation Verification**
  - Create a verification directory, for example: `~/tdengine-test`, download the [nodejsChecker.js source code](https://github.com/taosdata/TDengine/tree/main/docs/examples/node/websocketexample/nodejsChecker.js) from GitHub to local.
  - Execute the following commands in the command line.

  ```bash
  npm init -y
  npm install @tdengine/websocket
  node nodejsChecker.js
  ```

  - After performing the above steps, the command line will output the results of nodeChecker.js connecting to the TDengine instance and performing simple insertion and query operations.

</TabItem>

<TabItem label="C#" value="csharp">

Edit the project configuration file to add a reference to [TDengine.Connector](https://www.nuget.org/packages/TDengine.Connector/):

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

You can also add it via the dotnet command:

```shell
dotnet add package TDengine.Connector
```

:::note

The following example code is based on dotnet6.0. If you are using another version, you may need to make appropriate adjustments.

:::

</TabItem>

<TabItem label="C" value="c">

If you have already installed the TDengine server software or the TDengine client driver taosc, then the C connector is already installed and no additional action is required.

</TabItem>

<TabItem label="REST API" value="rest">

To access TDengine using the REST API method, no drivers or connectors need to be installed.

</TabItem>
</Tabs>

## Establishing Connection

Before proceeding with this step, please ensure that there is a running TDengine that can be accessed, and that the server's FQDN is configured correctly. The following example code assumes that TDengine is installed on the local machine, and that the FQDN (default localhost) and serverPort (default 6030) are using the default configuration.

### Connection Parameters

There are many configuration options for connecting, so before establishing a connection, let's first introduce the parameters used by the connectors of each language to establish a connection.

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

The parameters for establishing a connection with the Java connector are URL and Properties.  
The JDBC URL format for TDengine is: `jdbc:[TAOS|TAOS-WS|TAOS-RS]://[host_name]:[port]/[database_name]?[user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}|&batchfetch={batchfetch}]`  

For detailed explanations of URL and Properties parameters and how to use them, see [URL specifications](../../tdengine-reference/client-libraries/java/#url-specification)

</TabItem>

<TabItem label="Python" value="python">

The Python connector uses the `connect()` method to establish a connection, here are the specific parameters for the connection:

- url: URL of the `taosAdapter` REST service. The default is port `6041` on `localhost`.
- user: TDengine username. The default is `root`.  
- password: TDengine user password. The default is `taosdata`.  
- timeout: HTTP request timeout in seconds. The default is `socket._GLOBAL_DEFAULT_TIMEOUT`. Generally, no configuration is needed.

For detailed explanations of URL parameters and how to use them, see [URL specifications](../../tdengine-reference/client-libraries/python/#url-specification)

</TabItem>

<TabItem label="Go" value="go">

The data source name has a generic format, similar to [PEAR DB](http://pear.php.net/manual/en/package.database.db.intro-dsn.php), but without the type prefix (brackets indicate optional):

```text
[username[:password]@][protocol[(address)]]/[dbname][?param1=value1&...&paramN=valueN]
```

Complete DSN format:

```text
username:password@protocol(address)/dbname?param=value
```

When using an IPv6 address (supported in v3.7.1 and above), the address needs to be enclosed in square brackets, for example:

```text
root:taosdata@ws([::1]:6041)/testdb
```

Supported DSN parameters are as follows:

Native connection:

- `cfg` specifies the taos.cfg directory.
- `cgoThread` specifies the number of cgo operations that can be executed concurrently, default is the number of system cores.
- `cgoAsyncHandlerPoolSize` specifies the size of the async function handler, default is 10000.
- `timezone` specifies the timezone used for the connection. Both SQL parsing and query results will be converted according to this timezone. Only IANA timezone formats are supported, and special characters need to be encoded. Taking the Shanghai timezone (`Asia/Shanghai`) as an example: `timezone=Asia%2FShanghai`.

REST connection:

- `disableCompression` whether to accept compressed data, default is true which means not accepting compressed data, set to false if data transmission uses gzip compression.
- `readBufferSize` the size of the buffer for reading data, default is 4K (4096), this value can be increased appropriately when the query result data volume is large.
- `token` the token used when connecting to cloud services.
- `skipVerify` whether to skip certificate verification, default is false which means not skipping certificate verification, set to true if connecting to an insecure service.
- `timezone` specifies the timezone used for the connection. Both SQL parsing and query results will be converted according to this timezone. Only IANA timezone formats are supported, and special characters need to be encoded. Taking the Shanghai timezone (`Asia/Shanghai`) as an example: `timezone=Asia%2FShanghai`.

WebSocket connection:

- `enableCompression` whether to send compressed data, default is false which means not sending compressed data, set to true if data transmission uses compression.
- `readTimeout` the timeout for reading data, default is 5m.
- `writeTimeout` the timeout for writing data, default is 10s.
- `timezone` specifies the timezone used for the connection. Both SQL parsing and query results will be converted according to this timezone. Only IANA timezone formats are supported, and special characters need to be encoded. Taking the Shanghai timezone (`Asia/Shanghai`) as an example: `timezone=Asia%2FShanghai`.

</TabItem>

<TabItem label="Rust" value="rust">

Rust connector uses DSN to create connections, the basic structure of the DSN description string is as follows:

```text
<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|------------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  database  |  params               |
```

For detailed explanation of DSN and how to use it, see [Connection Features](../../tdengine-reference/client-libraries/rust/)

</TabItem>

<TabItem label="Node.js" value="node">
Node.js connector uses DSN to create connections, the basic structure of the DSN description string is as follows:

```text
[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|------------|---|-----------|-----------|------|------|------------|-----------------------|
|   protocol |   | username  | password  | host | port |  database  |  params               |
```

- **protocol**: Establish a connection using the websocket protocol. For example, `ws://localhost:6041`
- **username/password**: Username and password for the database.
- **host/port**: The host_name parameter supports valid domain names or IP addresses. The `@tdengine/websocket` supports both IPv4 and IPv6 formats. For IPv6 addresses, square brackets must be used (e.g., [::1] or [2001:db8:1234:5678::1]) to avoid port number parsing conflicts.
- **database**: Database name.
- **params**: Other parameters. For example, token.

- Complete DSN example:

```js
  // IPV4:
  ws://root:taosdata@localhost:6041
    
  // IPV6:
  ws://root:taosdata@[::1]:6041
```

</TabItem>

<TabItem label="C#" value="csharp">

ConnectionStringBuilder uses a key-value pair method to set connection parameters, where key is the parameter name and value is the parameter value, separated by a semicolon `;`.

For example:

```csharp
"protocol=WebSocket;host=127.0.0.1;port=6041;useSSL=false"
```

Supported parameters are as follows:

- `host`: The address of the TDengine instance.
- `port`: The port of the TDengine instance.
- `username`: Username for the connection.
- `password`: Password for the connection.
- `protocol`: Connection protocol, options are Native or WebSocket, default is Native.
- `db`: Database to connect to.
- `timezone`: Time zone, default is the local time zone.
- `connTimeout`: Connection timeout, default is 1 minute.

Additional parameters supported for WebSocket connections:

- `readTimeout`: Read timeout, default is 5 minutes.
- `writeTimeout`: Send timeout, default is 10 seconds.
- `token`: Token for connecting to TDengine cloud.
- `useSSL`: Whether to use SSL connection, default is false.
- `enableCompression`: Whether to enable WebSocket compression, default is false.
- `autoReconnect`: Whether to automatically reconnect, default is false.
- `reconnectRetryCount`: Number of retries for reconnection, default is 3.
- `reconnectIntervalMs`: Reconnection interval in milliseconds, default is 2000.

</TabItem>

<TabItem label="C" value="c">

The C/C++ connector uses the `taos_connect()` function to establish a connection with the TDengine database. The parameters are explained below:

- `host`: The hostname or IP address of the database server. If it is a local database, you can use `"localhost"`.
- `user`: Database login username.
- `passwd`: The login password corresponding to the username.
- `db`: The default database name used when connecting. If you do not specify a database, you can pass `NULL` or an empty string.
- `port`: The port number that the database server listens on. The default port for native connections is `6030`, and the default port for WebSocket connections is `6041`.

For WebSocket connections, you need to call `taos_options(TSDB_OPTION_DRIVER, "websocket")` to set the driver type first, and then call `taos_connect()` to establish a connection.

Native connections also provide the `taos_connect_auth()` function, which is used to establish a connection using an MD5 encrypted password. This function has the same functionality as `taos_connect()`, the difference is how the password is handled. `taos_connect_auth()` requires the MD5 encrypted string of the password.

</TabItem>

<TabItem label="REST API" value="rest">

When accessing TDengine via REST API, the application directly establishes an HTTP connection with taosAdapter, and it is recommended to use a connection pool to manage connections.

For specific parameters using the REST API, refer to: [HTTP request format](../../tdengine-reference/client-libraries/rest-api/)

</TabItem>
</Tabs>

### WebSocket Connection

Below are code examples for establishing WebSocket connections in various language connectors. It demonstrates how to connect to the TDengine database using WebSocket and set some parameters for the connection. The whole process mainly involves establishing the database connection and handling exceptions.

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

SQLAlchemy supports configuring multiple server addresses through the `hosts` parameter to achieve load balancing and failover. Multiple addresses are separated by English commas, in the format: `hosts=<host1>:<port1>,<host2>:<port2>,...`

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

<TabItem label="REST API" value="rest">

Not supported

</TabItem>
</Tabs>

### Native Connection

Below are examples of code for establishing native connections in various languages. It demonstrates how to connect to the TDengine database using a native connection method and set some parameters for the connection. The entire process mainly involves establishing a database connection and handling exceptions.

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

Not supported

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

Not supported

</TabItem>
</Tabs>

### REST Connection

Below are examples of code for establishing REST connections in various languages. It demonstrates how to connect to the TDengine database using a REST connection method. The entire process mainly involves establishing a database connection and handling exceptions.

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

Not supported

</TabItem>

<TabItem label="Node.js" value="node">

Not supported

</TabItem>

<TabItem label="C#" value="csharp">

Not supported

</TabItem>

<TabItem label="C" value="c">

Not supported

</TabItem>

<TabItem label="REST API" value="rest">

Access TDengine using the REST API method, where the application independently establishes an HTTP connection.

</TabItem>
</Tabs>

:::tip
If the connection fails, in most cases it is due to incorrect FQDN or firewall settings. For detailed troubleshooting methods, please see ["Encountering the error 'Unable to establish connection, what should I do?'"](../../frequently-asked-questions/) in the "Common Questions and Feedback".

:::

## Connection Pool

Some connectors offer a connection pool, or can be used in conjunction with existing connection pool components. By using a connection pool, applications can quickly obtain available connections from the pool, avoiding the overhead of creating and destroying connections with each operation. This not only reduces resource consumption but also improves response speed. Additionally, connection pools support the management of connections, such as limiting the maximum number of connections and checking the validity of connections, ensuring efficient and reliable use of connections. We **recommend managing connections using a connection pool**.  
Below are code examples of connection pool support for various language connectors.  

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

**HikariCP**  

Example usage is as follows:

```java
{{#include docs/examples/java/src/main/java/com/taos/example/HikariDemo.java:connection_pool}}
```

> After obtaining a connection through HikariDataSource.getConnection(), you need to call the close() method after use, which actually does not close the connection but returns it to the pool.
> For more issues about using HikariCP, please see the [official documentation](https://github.com/brettwooldridge/HikariCP).

**Druid**  

Example usage is as follows:

```java
{{#include docs/examples/java/src/main/java/com/taos/example/DruidDemo.java:connection_pool}}
```

> For more issues about using Druid, please see the [official documentation](https://github.com/alibaba/druid).

</TabItem>

<TabItem label="Python" value="python">

<details>
<summary>SQLAlchemy connection pool example (recommended)</summary>

```python
{{#include docs/examples/python/sqlalchemy_demo.py}}
```

</details>

<details>
<summary>DBUtils Connection Pool Example</summary>

```python
{{#include docs/examples/python/dbutils_demo.py}}
```

</details>

</TabItem>

<TabItem label="Go" value="go">

Using `sql.Open` creates a connection that has already implemented a connection pool, and you can set connection pool parameters through the API, as shown in the example below

```go
{{#include docs/examples/go/connect/connpool/main.go:pool}}
```

</TabItem>

<TabItem label="Rust" value="rust">

In complex applications, it is recommended to enable connection pooling. The connection pool of `taos` is implemented using `deadpool` in asynchronous mode.

Create a connection pool with default parameters:

```rust
let pool: Pool<TaosBuilder> = TaosBuilder::from_dsn("taos:///")
    .unwrap()
    .pool()
    .unwrap();
```

Use the connection pool constructor to customize the parameters:

```rust
let pool: Pool<TaosBuilder> = Pool::builder(Manager::from_dsn("taos:///").unwrap().0)
    .max_size(88) // Maximum number of connections
    .build()
    .unwrap();
```

Get a connection object from the connection pool:

```rust
let taos = pool.get().await?;
```

</TabItem>
</Tabs>
