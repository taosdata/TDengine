---
title: Connecting to TDengine
slug: /developer-guide/connecting-to-tdengine
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import Image from '@theme/IdealImage';
import imgConnect from '../assets/connecting-to-tdengine-01.png';
import ConnJava from "./_connect_java.mdx";
import ConnGo from "./_connect_go.mdx";
import ConnRust from "./_connect_rust.mdx";
import ConnNode from "./_connect_node.mdx";
import ConnPythonNative from "./_connect_python.mdx";
import ConnCSNative from "./_connect_cs.mdx";
import ConnC from "./_connect_c.mdx";
import InstallOnLinux from "../14-reference/05-connector/_linux_install.mdx";
import InstallOnWindows from "../14-reference/05-connector/_windows_install.mdx";
import InstallOnMacOS from "../14-reference/05-connector/_macos_install.mdx";
import VerifyLinux from "../14-reference/05-connector/_verify_linux.mdx";
import VerifyMacOS from "../14-reference/05-connector/_verify_macos.mdx";
import VerifyWindows from "../14-reference/05-connector/_verify_windows.mdx";

TDengine provides a rich set of application development interfaces. To facilitate users in quickly developing their applications, TDengine supports various programming language connectors, including official connectors for C/C++, Java, Python, Go, Node.js, C#, Rust, Lua (community-contributed), and PHP (community-contributed). These connectors support connecting to TDengine clusters using native interfaces (taosc) and REST interfaces (not supported by some languages). Community developers have also contributed several unofficial connectors, such as the ADO.NET connector, Lua connector, and PHP connector. Additionally, TDengine can directly call the REST API provided by taosadapter for data writing and querying operations.

## Connection Methods

TDengine provides three methods for establishing connections:

1. Directly connect to the server program taosd using the client driver taosc; this method is referred to as "native connection."
2. Establish a connection to taosd via the REST API provided by the taosAdapter component; this method is referred to as "REST connection."
3. Establish a connection to taosd via the WebSocket API provided by the taosAdapter component; this method is referred to as "WebSocket connection."

<figure>
<Image img={imgConnect} alt="Connecting to TDengine"/>
<figcaption>Figure 1. Connecting to TDengine</figcaption>
</figure>

Regardless of the method used to establish a connection, the connectors provide similar API operations for databases and can execute SQL statements. The only difference lies in how the connection is initialized, and users should not notice any difference in usage. For various connection methods and language connector support, refer to: [Feature Support](../../tdengine-reference/client-libraries/#feature-support).

Key differences include:

1. For the native connection, it is necessary to ensure that the client driver taosc and the TDengine server version are compatible.
2. With the REST connection, users do not need to install the client driver taosc, which offers cross-platform ease of use; however, users cannot experience features like data subscription and binary data types. Moreover, REST connections have the lowest performance compared to native and WebSocket connections. The REST API is stateless, and when using REST connections, users must specify the database name for tables and supertables in SQL.
3. For the WebSocket connection, users also do not need to install the client driver taosc.
4. To connect to cloud service instances, users must use REST or WebSocket connections.

**It is recommended to use WebSocket connections.**

## Install the Client Driver taosc

If you choose the native connection and the application is not running on the same server as TDengine, you need to install the client driver first; otherwise, this step can be skipped. To avoid incompatibility between the client driver and server, please use the same version.

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

After the installation and configuration are complete, and ensuring that the TDengine service is running normally, you can execute the TDengine command-line program taos included in the installation package to log in.

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

## Install Connectors

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

If you are using Maven to manage the project, simply add the following dependency to the pom.xml.

```xml
<dependency>
  <groupId>com.taosdata.jdbc</groupId>
  <artifactId>taos-jdbcdriver</artifactId>
  <version>3.3.3</version>
</dependency>
```

</TabItem>

<TabItem label="Python" value="python">

- **Installation Prerequisites**
  - Install Python. The latest version of the taospy package requires Python 3.6.2+. Earlier versions require Python 3.7+. The taos-ws-py package requires Python 3.7+. If Python is not already installed on your system, refer to the [Python BeginnersGuide](https://wiki.python.org/moin/BeginnersGuide/Download) for installation.
  - Install [pip](https://pypi.org/project/pip/). Most Python installation packages come with the pip tool. If not, refer to the [pip documentation](https://pip.pypa.io/en/stable/installation/) for installation.
  - If you are using the native connection, you also need to [install the client driver](#install-the-client-driver-taosc). The client software includes the TDengine client dynamic link library (libtaos.so or taos.dll) and the TDengine CLI.
- **Install using pip**
  - Uninstall old versions
    If you previously installed an older version of the Python connector, please uninstall it first.

    ```shell
    pip3 uninstall taos taospy
    pip3 uninstall taos taos-ws-py
    ```

    - Install `taospy`
      - Latest version

        ```shell
        pip3 install taospy
        ```

      - Install a specific version

        ```shell
        pip3 install taospy==2.3.0
        ```

      - Install from GitHub

        ```shell
        pip3 install git+https://github.com/taosdata/taos-connector-python.git
        ```

    :::note This installation package is for the native connector.
    - Install `taos-ws-py`

      ```shell
      pip3 install taos-ws-py
      ```

    :::
    :::note This installation package is for the WebSocket connector.
    - Install both `taospy` and `taos-ws-py`

      ```shell
      pip3 install taospy[ws]
      ```

    :::
- **Installation Verification**

<Tabs defaultValue="rest">
<TabItem value="native" label="Native Connection">

For the native connection, verify that both the client driver and the Python connector are correctly installed. If you can successfully import the `taos` module, it indicates that the client driver and Python connector have been correctly installed. You can type the following in the Python interactive shell:

```python
import taos
```

</TabItem>

<TabItem  value="rest" label="REST Connection">

For the REST connection, you only need to verify whether you can successfully import the `taosrest` module. You can type the following in the Python interactive shell:

```python
import taosrest
```

</TabItem>

<TabItem  value="ws" label="WebSocket Connection">
For the WebSocket connection, you only need to verify whether you can successfully import the `taosws` module. You can type the following in the Python interactive shell:

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
driver-go uses cgo to wrap the taosc API. cgo requires GCC to compile C source code. Therefore, ensure that your system has GCC installed.

:::

</TabItem>

<TabItem label="Rust" value="rust">

Edit `Cargo.toml` to add the `taos` dependency.

```toml title=Cargo.toml
[dependencies]
taos = { version = "*"}
```

:::info
The Rust connector differentiates between connection methods through different features. By default, it supports both native and WebSocket connections. If you only need to establish a WebSocket connection, you can set the `ws` feature:

```toml
taos = { version = "*", default-features = false, features = ["ws"] }
```

:::

</TabItem>

<TabItem label="Node.js" value="node">

- **Installation Prerequisites**
  - Install the Node.js development environment, using version 14 or higher. [Download link](https://nodejs.org/en/download/)

- **Installation**
  - Install the Node.js connector using npm

    ```shell
    npm install @tdengine/websocket
    ```

  :::note Node.js currently only supports WebSocket connections.
- **Installation Verification**
  - Create an installation verification directory, for example: `~/tdengine-test`, and download the [nodejsChecker.js source code](https://github.com/taosdata/TDengine/tree/main/docs/examples/node/websocketexample/nodejsChecker.js) from GitHub to your local machine.
  - Execute the following command in the terminal.

    ```bash
    npm init -y
    npm install @tdengine/websocket
    node nodejsChecker.js
    ```

  - After executing the above steps, the command line will output the results of connecting to the TDengine instance and performing a simple insert and query.

</TabItem>

<TabItem label="C#" value="csharp">

Add the reference for [TDengine.Connector](https://www.nuget.org/packages/TDengine.Connector/) in the project configuration file:

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

You can also add it using the dotnet command:

```shell
dotnet add package TDengine.Connector
```

:::note
The following example code is based on dotnet 6.0; if you are using other versions, you may need to make appropriate adjustments.

:::

</TabItem>

<TabItem label="C" value="c">

If you have already installed the TDengine server software or the TDengine client driver taosc, then the C connector is already installed, and no additional action is required.

</TabItem>

<TabItem label="REST API" value="rest">

Using the REST API to access TDengine does not require the installation of any drivers or connectors.

</TabItem>
</Tabs>

## Establishing Connections

Before executing this step, ensure that there is a running and accessible TDengine instance, and that the server's FQDN is configured correctly. The following example code assumes that TDengine is installed on the local machine, with the FQDN (default localhost) and serverPort (default 6030) using default configurations.

### Connection Parameters

There are many configuration items for the connection. Before establishing the connection, we can introduce the parameters used by each language connector to establish a connection.

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

The parameters for establishing a connection with the Java connector include URL and Properties.  

The standard format for TDengine's JDBC URL is: `jdbc:[TAOS|TAOS-RS]://[host_name]:[port]/[database_name]?[user={user}|&password={password}|&charset={charset}|&cfgdir={config_dir}|&locale={locale}|&timezone={timezone}|&batchfetch={batchfetch}]`  

For detailed parameter descriptions of URL and Properties, and how to use them, refer to [URL Specification](../../tdengine-reference/client-libraries/java/#url-specification).

**Note**: Adding the `batchfetch` parameter and setting it to true in REST connections will enable the WebSocket connection.

</TabItem>

<TabItem label="Python" value="python">

The Python connector uses the `connect()` method to establish a connection. The specific descriptions of the connection parameters are as follows:

- url: The URL of the `taosAdapter` REST service. The default is port `6041` on `localhost`.
- user: The TDengine username. The default is `root`.  
- password: The TDengine user password. The default is `taosdata`.  
- timeout: The HTTP request timeout, in seconds. The default is `socket._GLOBAL_DEFAULT_TIMEOUT`, which usually does not need to be configured.

</TabItem>

<TabItem label="Go" value="go">

The data source name has a general format, such as [PEAR DB](http://pear.php.net/manual/en/package.database.db.intro-dsn.php), but without a type prefix (the brackets indicate that it is optional):

``` text
    [username[:password]@][protocol[(address)]]/[dbname][?param1=value1&...&paramN=valueN]
```

The complete form of the DSN:

```text
    username:password@protocol(address)/dbname?param=value
```

Supported DSN parameters include:

For native connections:

- `cfg`: Specifies the taos.cfg directory.
- `cgoThread`: Specifies the number of cgo threads to execute concurrently, defaulting to the number of system cores.
- `cgoAsyncHandlerPoolSize`: Specifies the size of the async function handler, defaulting to 10,000.

For REST connections:

- `disableCompression`: Whether to accept compressed data; default is true (does not accept compressed data). If using gzip compression for transmission, set to false.
- `readBufferSize`: The size of the read data buffer, defaulting to 4K (4096). This value can be increased for larger query results.
- `token`: The token used when connecting to cloud services.
- `skipVerify`: Whether to skip certificate verification; default is false (does not skip verification). Set to true if connecting to an insecure service.

For WebSocket connections:

- `enableCompression`: Whether to send compressed data; default is false (does not send compressed data). Set to true if using compression.
- `readTimeout`: The read timeout for data, defaulting to 5m.
- `writeTimeout`: The write timeout for data, defaulting to 10s.

</TabItem>

<TabItem label="Rust" value="rust">

The Rust connector uses DSN to create connections. The basic structure of the DSN description string is as follows:

```text
<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|------------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  database  |  params               |
```

For detailed DSN explanations and usage, refer to [Connection Functionality](../../tdengine-reference/client-libraries/rust/#connection-functionality).

</TabItem>

<TabItem label="Node.js" value="node">

The Node.js connector uses DSN to create connections. The basic structure of the DSN description string is as follows:

```text
    [+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
    |------------|---|-----------|-----------|------|------|------------|-----------------------|
    |   protocol |   | username  | password  | host | port |  database  |  params               |
```

- **protocol**: Establish a connection using the WebSocket protocol. For example, `ws://localhost:6041`.
- **username/password**: The database username and password.
- **host/port**: The host address and port number. For example, `localhost:6041`.
- **database**: The database name.
- **params**: Other parameters, such as token.

- Complete DSN example:

  ```js
  ws://root:taosdata@localhost:6041
  ```

</TabItem>

<TabItem label="C#" value="csharp">

The ConnectionStringBuilder sets connection parameters using a key-value approach, where the key is the parameter name and the value is the parameter value, separated by semicolons `;`.

For example:

```csharp
"protocol=WebSocket;host=127.0.0.1;port=6041;useSSL=false"
```

Supported parameters include:

- `host`: The address of the TDengine instance.
- `port`: The port of the TDengine instance.
- `username`: The username for the connection.
- `password`: The password for the connection.
- `protocol`: The connection protocol, with optional values of Native or WebSocket, defaulting to Native.
- `db`: The connected database.
- `timezone`: The timezone, defaulting to the local timezone.
- `connTimeout`: The connection timeout, defaulting to 1 minute.

WebSocket connections also support the following parameters:

- `readTimeout`: The read timeout, defaulting to 5 minutes.
- `writeTimeout`: The send timeout, defaulting to 10 seconds.
- `token`: The token for connecting to TDengine cloud.
- `useSSL`: Whether to use SSL for the connection, defaulting to false.
- `enableCompression`: Whether to enable WebSocket compression, defaulting to false.
- `autoReconnect`: Whether to automatically reconnect, defaulting to false.
- `reconnectRetryCount`: The number of reconnection attempts, defaulting to 3.
- `reconnectIntervalMs`: The reconnection interval in milliseconds, defaulting to 2000.

</TabItem>

<TabItem label="C" value="c">

**WebSocket Connection**  
The C/C++ language connector uses the `ws_connect()` function to establish a connection to the TDengine database. Its parameter is a DSN description string with the following basic structure:

```text
<driver>[+<protocol>]://[[<username>:<password>@]<host>:<port>][/<database>][?<p1>=<v1>[&<p2>=<v2>]]
|------|------------|---|-----------|-----------|------|------|------------|-----------------------|
|driver|   protocol |   | username  | password  | host | port |  database  |  params               |
```

For detailed DSN explanations and usage, refer to [DSN](../../tdengine-reference/client-libraries/cpp/#dsn).

**Native Connection**  
The C/C++ language connector uses the `taos_connect()` function to establish a connection to the TDengine database. The detailed parameter descriptions are as follows:

- `host`: The hostname or IP address of the database server to connect to. If it is a local database, you can use `"localhost"`.
- `user`: The username used to log in to the database.
- `passwd`: The password corresponding to the username.
- `db`: The default database name to select when connecting. If not specified, you can pass `NULL` or an empty string.
- `port`: The port number that the database server listens on. The default port number is `6030`.

There is also the `taos_connect_auth()` function for establishing a connection to the TDengine database using an MD5-encrypted password. This function works the same as `taos_connect`, but the difference lies in how the password is handled; `taos_connect_auth` requires the MD5 hash of the password.

</TabItem>

<TabItem label="REST API" value="rest">

When accessing TDengine via the REST API, the application directly establishes an HTTP connection with taosAdapter. It is recommended to use a connection pool to manage connections.

For specific parameters used in the REST API, refer to: [HTTP Request Format](../../tdengine-reference/client-libraries/rest-api/#http-request-format).

</TabItem>
</Tabs>

### WebSocket Connection

Below are code samples for establishing a WebSocket connection using each language connector. They demonstrate how to connect to the TDengine database using the WebSocket connection method and set some parameters for the connection. The entire process mainly involves establishing the database connection and handling exceptions.

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

```c
{{#include docs/examples/c-ws/connect_example.c}}
```

</TabItem>

<TabItem label="REST API" value="rest">

Not supported

</TabItem>
</Tabs>

### Native Connection

Below are code samples for establishing a native connection using each language connector. They demonstrate how to connect to the TDengine database using the native connection method and set some parameters for the connection. The entire process mainly involves establishing the database connection and handling exceptions.

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

Below are code samples for establishing a REST connection using each language connector. They demonstrate how to connect to the TDengine database using the REST connection method. The entire process mainly involves establishing the database connection and handling exceptions.

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

Using the REST API to access TDengine allows the application to independently establish HTTP connections.

</TabItem>
</Tabs>

:::tip

If the connection fails, in most cases, it is due to incorrect FQDN or firewall configuration. For detailed troubleshooting methods, refer to [Frequently Asked Questions](../../frequently-asked-questions/) under "If I encounter the error Unable to establish connection, what should I do?"

:::

## Connection Pool

Some connectors provide connection pools or can work with existing connection pool components. Using a connection pool allows applications to quickly obtain available connections from the pool, avoiding the overhead of creating and destroying connections for each operation. This not only reduces resource consumption but also improves response speed. Additionally, connection pools support connection management, such as limiting the maximum number of connections and checking connection validity, ensuring efficient and reliable use of connections. We **recommend using connection pools to manage connections**.

Below are code samples for connection pool support in various language connectors.  

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

**HikariCP**  

Usage example:

```java
{{#include docs/examples/java/src/main/java/com/taos/example/HikariDemo.java:connection_pool}}
```

> After obtaining a connection via HikariDataSource.getConnection(), you need to call the close() method after use; it does not actually close the connection, but returns it to the pool.
> For more issues related to HikariCP usage, refer to the [official documentation](https://github.com/brettwooldridge/HikariCP).

**Druid**  

Usage example:

```java
{{#include docs/examples/java/src/main/java/com/taos/example/DruidDemo.java:connection_pool}}
```

> For more issues related to Druid usage, refer to the [official documentation](https://github.com/alibaba/druid).

</TabItem>

<TabItem label="Python" value="python">
        <ConnPythonNative />
</TabItem>

<TabItem label="Go" value="go">

Using `sql.Open`, the created connection already implements a connection pool. You can set connection pool parameters via the API, as shown below:

```go
{{#include docs/examples/go/connect/connpool/main.go:pool}}
```

</TabItem>

<TabItem label="Rust" value="rust">

In complex applications, it is recommended to enable the connection pool. The [taos] connection pool, by default (asynchronous mode), uses [deadpool] for implementation.

Here is how to generate a connection pool with default parameters.

```rust
let pool: Pool<TaosBuilder> = TaosBuilder::from_dsn("taos:///").unwrap().pool().unwrap();
```

You can also use the pool constructor to set connection pool parameters:

```rust
let pool: Pool<TaosBuilder> = Pool::builder(Manager::from_dsn(self.dsn.clone()).unwrap().0)
    .max_size(88)  // Maximum number of connections
    .build()
    .unwrap();
```

In the application code, use `pool.get()?` to obtain a connection object from [Taos].

```rust
let taos = pool.get()?;
```

</TabItem>
</Tabs>
