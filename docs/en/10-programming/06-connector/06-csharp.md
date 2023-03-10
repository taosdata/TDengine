---
sidebar_label: C#
title: TDengine C# Connector
description: Detailed guide for C# Connector
---

 `TDengine.Connector` is the official C# connector for TDengine. C# developers can develop applications to access TDengine instance data.

This article describes how to install `TDengine.Connector` in a Linux or Windows environment and connect to TDengine clusters via `TDengine.Connector` to perform basic operations such as data writing and querying.

The source code for `TDengine.Connector` is hosted on [GitHub](https://github.com/taosdata/taos-connector-dotnet/tree/3.0).

## Version support

Please refer to [version support list](/reference/connector#version-support)

## Installation

### Pre-installation

* Install the [.NET SDK](https://dotnet.microsoft.com/download)
* [Nuget Client](https://docs.microsoft.com/en-us/nuget/install-nuget-client-tools) (optional installation)
* Install TDengine client driver, please refer to [Install client driver](/reference/connector/#install-client-driver) for details

### Add `TDengine.Connector` through Nuget

```bash
dotnet add package TDengine.Connector
```

## Establishing a connection

``` XML
{{#include docs/examples/csharp/cloud-example/connect/connect.csproj}}
```

``` csharp
{{#include docs/examples/csharp/cloud-example/connect/Program.cs}}
```

## Usage examples

### Basic Insert and Query

``` XML
{{#include docs/examples/csharp/cloud-example/usage/usage.csproj}}
```

```C#
{{#include docs/examples/csharp/cloud-example/usage/Program.cs}}
```

### STMT Insert

``` XML
{{#include docs/examples/csharp/cloud-example/stmt/stmt.csproj}}
```

```C#
{{#include docs/examples/csharp/cloud-example/stmt/Program.cs}}
```

## Important Updates

| TDengine.Connector | Description |
|--------------------|--------------------------------|
|        3.0.2       | Support .NET Framework 4.5 and above. Support .Net standard 2.0. Nuget package includes dynamic library for WebSocket.|
|        3.0.1       | Support WebSocket and Cloud，With function query, insert, and parameter binding|
|        3.0.0       | Supports TDengine 3.0.0.0. TDengine 2.x is not supported. Added `TDengine.Impl.GetData()` interface to deserialize query results. |
|        1.0.7       | Fixed TDengine.Query() memory leak. |
| 1.0.6 | Fix schemaless bug in 1.0.4 and 1.0.5. |
| 1.0.5 | Fix Windows sync query Chinese error bug. | 1.0.4 | Fix schemaless bug.   |
| 1.0.4 | Add asynchronous query, subscription, and other functions. Fix the binding parameter bug.    |
| 1.0.3 | Add parameter binding, schemaless, JSON tag, etc. |
| 1.0.2 | Add connection management, synchronous query, error messages, etc.   |

## Other descriptions

### Third-party driver

`Taos` is an ADO.NET connector for TDengine, supporting Linux and Windows platforms. Community contributor `Maikebing@@maikebing contributes the connector`. Please refer to:

* Interface download:<https://github.com/maikebing/Maikebing.EntityFrameworkCore.Taos>

## Frequently Asked Questions

1. "Unable to establish connection", "Unable to resolve FQDN"

  Usually, it's caused by an incorrect FQDN configuration. Please refer to this section in the [FAQ](https://docs.tdengine.com/2.4/train-faq/faq/#2-how-to-handle-unable-to-establish-connection) to troubleshoot.

2. Unhandled exception. System.DllNotFoundException: Unable to load DLL 'taos' or one of its dependencies: The specified module cannot be found.

  This is usually because the program did not find the dependent client driver. The solution is to copy `C:\TDengine\driver\taos.dll` to the `C:\Windows\System32\` directory on Windows, and create the following soft link on Linux `ln -s /usr/local/taos/driver/libtaos.so.x.x .x.x /usr/lib/libtaos.so` will work.

## API Reference

[API Reference](https://docs.taosdata.com/api/connector-csharp/html/860d2ac1-dd52-39c9-e460-0829c4e5a40b.htm)
