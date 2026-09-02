---
sidebar_label: 连接器参考手册
title: 连接器参考手册
description: TDengine 多语言连接器与 REST API 参考
---

import ConnectorType from "./resources/_connector_type.mdx";
import PlatformSupported from "./resources/_platform_supported.mdx";

<ConnectorType />

## 支持的平台

<PlatformSupported />

## 版本支持

TDengine 版本更新往往会增加新的功能特性，下表中的连接器版本为最佳适配版本。

| **TDengine 版本** | **Java** | **Python** | **Go** | **C#** | **Node.js** | **Rust** | **C/C++** |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **`v3.3.0.0` 及以上** | `3.3.0` 及以上 | taospy `2.7.15` 及以上，taos-ws-py `0.3.2` 及以上 | `3.5.5` 及以上 | `3.1.3` 及以上 | `3.1.0` 及以上 | 当前版本 | 与 TDengine 相同版本 |
| **`v3.0.0.0` 及以上** | `3.0.2` 以上 | 当前版本 | `3.0` 分支 | `3.0.0` | `3.1.0` | 当前版本 | 与 TDengine 相同版本 |
| **`v2.4.0.14` 及以上** | `2.0.38` | 当前版本 | develop 分支 | `1.0.2` - `1.0.6` | `2.0.10` - `2.0.12` | 当前版本 | 与 TDengine 相同版本 |
| **`v2.4.0.4` - `v2.4.0.13`** | `2.0.37` | 当前版本 | develop 分支 | `1.0.2` - `1.0.6` | `2.0.10` - `2.0.12` | 当前版本 | 与 TDengine 相同版本 |
| **`v2.2.x.x`** | `2.0.36` | 当前版本 | master 分支 | n/a | `2.0.7` - `2.0.9` | 当前版本 | 与 TDengine 相同版本 |
| **`v2.0.x.x`** | `2.0.34` | 当前版本 | master 分支 | n/a | `2.0.1` - `2.0.6` | 当前版本 | 与 TDengine 相同版本 |

## 功能特性

连接器对 TDengine 功能特性的支持对照如下。

### WebSocket / 原生连接

| **功能特性** | **Java** | **Python** | **Go** | **C#** | **Node.js** | **Rust** | **C/C++** |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **连接管理** | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 |
| **执行 SQL** | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 |
| **参数绑定** | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 |
| **数据订阅（TMQ）** | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 |
| **无模式写入** | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 | 支持 |

**说明**：Node.js 连接器不支持原生连接。

:::info
由于不同编程语言数据库框架规范不同，并不意味着所有 C/C++ 接口都需要对应封装支持。
:::

:::warning

- 无论选用何种编程语言的连接器，`v2.0` 及以上版本的 TDengine 推荐数据库应用的每个线程都建立独立连接，或基于线程建立连接池，以避免连接内的 `USE` 状态量在线程之间相互干扰（连接的查询和写入操作本身是线程安全的）。

:::

### REST API

支持 **执行 SQL**。

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import InstallOnLinux from "./resources/_linux_install.mdx";
import InstallOnWindows from "./resources/_windows_install.mdx";
import InstallOnMacOS from "./resources/_macos_install.mdx";
import VerifyWindows from "./resources/_verify_windows.mdx";
import VerifyLinux from "./resources/_verify_linux.mdx";
import VerifyMacOS from "./resources/_verify_macos.mdx";

## 安装客户端驱动

:::info
在没有安装 TDengine 服务端软件的系统上使用原生接口连接器，或使用 C/C++ WebSocket 连接器时，才需要安装客户端驱动。

:::

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

以上安装和配置完成后，并确认 TDengine 服务已经正常启动，可执行 `taos` shell 进行登录。

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

本章后续分别说明：

- [C/C++ Connector](./01-cpp.mdx)
- [Java Connector](./02-java.mdx)
- [Go Connector](./03-go.mdx)
- [Rust Connector](./04-rust.mdx)
- [Python Connector](./05-python.mdx)
- [Node.js Connector](./06-node.mdx)
- [C# Connector](./07-csharp.mdx)
- [R Language Connector](./08-r-lang.mdx)
- [ODBC](./09-odbc.mdx)
- [REST API](./10-rest-api.mdx)
