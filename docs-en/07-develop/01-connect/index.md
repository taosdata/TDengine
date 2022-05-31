---
sidebar_label: Connect
title: Connect
description: "This document explains how to establish connections to TDengine, and briefly introduces how to install and use TDengine connectors."
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import ConnJava from "./\_connect_java.mdx";
import ConnGo from "./\_connect_go.mdx";
import ConnRust from "./\_connect_rust.mdx";
import ConnNode from "./\_connect_node.mdx";
import ConnPythonNative from "./\_connect_python.mdx";
import ConnCSNative from "./\_connect_cs.mdx";
import ConnC from "./\_connect_c.mdx";
import ConnR from "./\_connect_r.mdx";
import InstallOnWindows from "../../14-reference/03-connector/\_linux_install.mdx";
import InstallOnLinux from "../../14-reference/03-connector/\_windows_install.mdx";
import VerifyLinux from "../../14-reference/03-connector/\_verify_linux.mdx";
import VerifyWindows from "../../14-reference/03-connector/\_verify_windows.mdx";

Any application programs running on any kind of platform can access TDengine through the REST API provided by TDengine. For details, please refer to [REST API](/reference/rest-api/). Additionally, application programs can use the connectors of multiple programming languages including C/C++, Java, Python, Go, Node.js, C#, and Rust to access TDengine. This chapter describes how to establish a connection to TDengine and briefly introduces how to install and use connectors. For details about the connectors, please refer to [Connectors](/reference/connector/)

## Establish Connection

There are two ways for a connector to establish connections to TDengine:

1. Connection through the REST API provided by the taosAdapter component, this way is called "REST connection" hereinafter.
2. Connection through the TDengine client driver (taosc), this way is called "Native connection" hereinafter.

Key differences：

1. The TDengine client driver (taosc) has the highest performance with all the features of TDengine like [Parameter Binding](/reference/connector/cpp#parameter-binding-api), [Subscription](/reference/connector/cpp#subscription-and-consumption-api), etc.
2. The TDengine client driver (taosc) is not supported across all platforms, and applications built on taosc may need to be modified when updating taosc to newer versions.
3. The REST connection is more accessible with cross-platform support, however it results in a 30% performance downgrade.

## Install Client Driver taosc

If you are choosing to use the native connection and the the application is not on the same host as TDengine server, the TDengine client driver taosc needs to be installed on the application host. If choosing to use the REST connection or the application is on the same host as TDengine server, this step can be skipped. It's better to use same version of taosc as the TDengine server.

### Install

<Tabs defaultValue="linux" groupId="os">
  <TabItem value="linux" label="Linux">
    <InstallOnWindows />
  </TabItem>
  <TabItem value="windows" label="Windows">
    <InstallOnLinux />
  </TabItem>
</Tabs>

### Verify

After the above installation and configuration are done and making sure TDengine service is already started and in service, the TDengine command-line interface `taos` can be launched to access TDengine.

<Tabs defaultValue="linux" groupId="os">
  <TabItem value="linux" label="Linux">
    <VerifyLinux />
  </TabItem>
  <TabItem value="windows" label="Windows">
    <VerifyWindows />
  </TabItem>
</Tabs>

## Install Connectors

<Tabs groupId="lang">
<TabItem label="Java" value="java">
  
If `maven` is used to manage the projects, what needs to be done is only adding below dependency in `pom.xml`.

```xml
<dependency>
  <groupId>com.taosdata.jdbc</groupId>
  <artifactId>taos-jdbcdriver</artifactId>
  <version>2.0.38</version>
</dependency>
```

</TabItem>
<TabItem label="Python" value="python">

Install from PyPI using `pip`:

```
pip install taospy
```

Install from Git URL:

```
pip install git+https://github.com/taosdata/taos-connector-python.git
```

</TabItem>
<TabItem label="Go" value="go">

Just need to add `driver-go` dependency in `go.mod` .

```go-mod title=go.mod
module goexample

go 1.17

require github.com/taosdata/driver-go/v2 develop
```

:::note
`driver-go` uses `cgo` to wrap the APIs provided by taosc, while `cgo` needs `gcc` to compile source code in C language, so please make sure you have proper `gcc` on your system.

:::

</TabItem>
<TabItem label="Rust" value="rust">

Just need to add `libtaos` dependency in `Cargo.toml`.

```toml title=Cargo.toml
[dependencies]
libtaos = { version = "0.4.2"}
```

:::info
Rust connector uses different features to distinguish the way to establish connection. To establish REST connection, please enable `rest` feature.

```toml
libtaos = { version = "*", features = ["rest"] }
```

:::

</TabItem>
<TabItem label="Node.js" value="node">

Node.js connector provides different ways of establishing connections by providing different packages.

1. Install Node.js Native Connector

```
npm i td2.0-connector
```

:::note
It's recommend to use Node whose version is between `node-v12.8.0` and `node-v13.0.0`.
:::

2. Install Node.js REST Connector

```
npm i td2.0-rest-connector
```

</TabItem>
<TabItem label="C#" value="csharp">

Just need to add the reference to [TDengine.Connector](https://www.nuget.org/packages/TDengine.Connector/) in the project configuration file.

```xml title=csharp.csproj {12}
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net6.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <StartupObject>TDengineExample.AsyncQueryExample</StartupObject>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="TDengine.Connector" Version="1.0.6" />
  </ItemGroup>

</Project>
```

Or add by `dotnet` command.

```
dotnet add package TDengine.Connector
```

:::note
The sample code below are based on dotnet6.0, they may need to be adjusted if your dotnet version is not exactly same.

:::

</TabItem>
<TabItem label="R" value="r">

1. Download [taos-jdbcdriver-version-dist.jar](https://repo1.maven.org/maven2/com/taosdata/jdbc/taos-jdbcdriver/2.0.38/).
2. Install the dependency package `RJDBC`：

```R
install.packages("RJDBC")
```

</TabItem>
<TabItem label="C" value="c">

If the client driver (taosc) is already installed, then the C connector is already available.
<br/>

</TabItem>
</Tabs>

## Establish Connection

Prior to establishing connection, please make sure TDengine is already running and accessible. The following sample code assumes TDengine is running on the same host as the client program, with FQDN configured to "localhost" and serverPort configured to "6030".

<Tabs groupId="lang" defaultValue="java">
  <TabItem label="Java" value="java">
    <ConnJava />
  </TabItem>
  <TabItem label="Python" value="python">
    <ConnPythonNative />
  </TabItem>
  <TabItem label="Go" value="go">
      <ConnGo />
  </TabItem>
  <TabItem label="Rust" value="rust">
    <ConnRust />
  </TabItem>
  <TabItem label="Node.js" value="node">
    <ConnNode />
  </TabItem>
  <TabItem label="C#" value="csharp">
    <ConnCSNative />
  </TabItem>
  <TabItem label="R" value="r">
    <ConnR/>
  </TabItem>
  <TabItem label="C" value="c">
    <ConnC />
  </TabItem>
</Tabs>

:::tip
If the connection fails, in most cases it's caused by improper configuration for FQDN or firewall. Please refer to the section "Unable to establish connection" in [FAQ](https://docs.taosdata.com/train-faq/faq).

:::
