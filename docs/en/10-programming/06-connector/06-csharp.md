---
sidebar_label: C#
title: TDengine C# Connector
description: Detailed guide for C# Connector
---

 `TDengine.Connector` is the official C# connector for TDengine. C# developers can develop applications to access TDengine instance data.

The source code for `TDengine.Connector` is hosted on [GitHub](https://github.com/taosdata/taos-connector-dotnet/tree/3.0).

## Installation

### Pre-installation

Install the .NET deployment SDK.

### Add TDengine.Connector through Nuget

```bash
dotnet add package TDengine.Connector
```

## Establishing a connection

``` XML
{{#include docs/examples/csharp/cloud-example/connect/connect.csproj}}
```

``` C#
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
| ------------------------- | ---------------------------------------------------------------- |
| 3.0.2 | Support .NET Framework 4.5 and above. Support .Net standard 2.0. Nuget package includes dynamic library for WebSocket.|
| 3.0.1 | Support connect to TDengine cloud service|

## API Reference

[API Reference](https://docs.taosdata.com/api/connector-csharp/html/860d2ac1-dd52-39c9-e460-0829c4e5a40b.htm)
