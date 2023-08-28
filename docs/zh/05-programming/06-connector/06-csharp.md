---
toc_max_heading_level: 4
sidebar_label: C#
title: C# Connector
---

`TDengine.Connector` 是 TDengine 提供的 C# 语言连接器。C# 开发人员可以通过它开发存取 TDengine 集群数据的 C# 应用软件。

本文介绍如何在 Linux 或 Windows 环境中安装 `TDengine.Connector`，并通过 `TDengine.Connector` 连接 TDengine 集群，进行数据写入、查询等基本操作。

`TDengine.Connector` 的源码托管在 [GitHub](https://github.com/taosdata/taos-connector-dotnet/tree/3.0)。

## 版本支持

请参考[版本支持列表](../#版本支持)

## 安装步骤

### 安装前准备

* 安装 [.NET SDK](https://dotnet.microsoft.com/download)
* [Nuget 客户端](https://docs.microsoft.com/en-us/nuget/install-nuget-client-tools) （可选安装）
* 安装 TDengine 客户端驱动，具体步骤请参考[安装客户端驱动](../#安装客户端驱动)

### 安装 TDengine.Connector

### 通过 Nuget 增加`TDengine.Connector`包

```bash
dotnet add package TDengine.Connector
```

## 建立连接

``` XML
{{#include docs/examples/csharp/cloud-example/connect/connect.csproj}}
```

``` csharp
{{#include docs/examples/csharp/cloud-example/connect/Program.cs}}
```

## 使用示例

### 基本插入和查询

``` XML
{{#include docs/examples/csharp/cloud-example/usage/usage.csproj}}
```

```C#
{{#include docs/examples/csharp/cloud-example/usage/Program.cs}}
```

### STMT 插入

``` XML
{{#include docs/examples/csharp/cloud-example/stmt/stmt.csproj}}
```

```C#
{{#include docs/examples/csharp/cloud-example/stmt/Program.cs}}
```

## 重要更新记录

| TDengine.Connector | 说明                           |
|--------------------|--------------------------------|
|        3.0.2       | 支持 .NET Framework 4.5 及以上，支持 .NET standard 2.0。Nuget Package 包含 WebSocket 动态库。 |
|        3.0.1       | 支持 WebSocket 和 Cloud，查询，插入，参数绑定。 |
|        3.0.0       | 支持 TDengine 3.0.0.0，不兼容 2.x。新增接口TDengine.Impl.GetData()，解析查询结果。 |
|        1.0.7       | 修复 TDengine.Query()内存泄露。 |
|        1.0.6       | 修复 schemaless 在 1.0.4 和 1.0.5 中失效 bug。 |
|        1.0.5       | 修复 Windows 同步查询中文报错 bug。   |
|        1.0.4       | 新增异步查询，订阅等功能。修复绑定参数 bug。    |
|        1.0.3       | 新增参数绑定、schemaless、 json tag等功能。 |
|        1.0.2       | 新增连接管理、同步查询、错误信息等功能。   |

## 其他说明

### 第三方驱动

[`IoTSharp.Data.Taos`](https://github.com/IoTSharp/EntityFrameworkCore.Taos) 是一个 TDengine 的 ADO.NET 连接器,其中包含了用于EntityFrameworkCore 的提供程序 IoTSharp.EntityFrameworkCore.Taos 和健康检查组件 IoTSharp.HealthChecks.Taos ，支持 Linux，Windows 平台。该连接器由社区贡献者`麦壳饼@@maikebing` 提供，具体请参考:

* 接口下载: <https://github.com/IoTSharp/EntityFrameworkCore.Taos>
* 用法说明:<https://www.taosdata.com/blog/2020/11/02/1901.html>

## 常见问题

1. "Unable to establish connection"，"Unable to resolve FQDN"

  一般是因为 FQDN 配置不正确。可以参考[如何彻底搞懂 TDengine 的 FQDN](https://www.taosdata.com/blog/2021/07/29/2741.html)解决。

2. Unhandled exception. System.DllNotFoundException: Unable to load DLL 'taos' or one of its dependencies: 找不到指定的模块。

  一般是因为程序没有找到依赖的客户端驱动。解决方法为：Windows 下可以将 `C:\TDengine\driver\taos.dll` 拷贝到 `C:\Windows\System32\ ` 目录下，Linux 下建立如下软链接 `ln -s /usr/local/taos/driver/libtaos.so.x.x.x.x /usr/lib/libtaos.so` 即可。

## API 参考

[API 参考](https://docs.taosdata.com/api/connector-csharp/html/860d2ac1-dd52-39c9-e460-0829c4e5a40b.htm)
