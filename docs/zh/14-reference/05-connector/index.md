---
sidebar_label: 连接器
title: 连接器参考手册
description: 详细介绍各种语言的连接器及 REST API
---

import ConnectorType from "./_connector_type.mdx";
import PlatformSupported from "./_platform_supported.mdx";

<ConnectorType /> 

## 支持的平台

<PlatformSupported /> 

## 版本支持

TDengine TSDB 版本更新往往会增加新的功能特性，列表中的连接器版本为连接器最佳适配版本。

| **TDengine TSDB 版本**      | **Java**    | **Python**                                  | **Go**       | **C#**        | **Node.js**     | **Rust** | **C/C++**            |
| ---------------------- | ----------- | ------------------------------------------- | ------------ | ------------- | --------------- | -------- | -------------------- |
| **3.3.0.0 及以上**     | 3.3.0 及以上 | taospy 2.7.15 及以上，taos-ws-py 0.3.2 及以上 | 3.5.5 及以上  | 3.1.3 及以上   | 3.1.0 及以上     | 当前版本 | 与 TDengine TSDB 相同版本 |
| **3.0.0.0 及以上**     | 3.0.2 以上   | 当前版本                                    | 3.0 分支     | 3.0.0         | 3.1.0           | 当前版本 | 与 TDengine TSDB 相同版本 |
| **2.4.0.14 及以上**    | 2.0.38      | 当前版本                                    | develop 分支 | 1.0.2 - 1.0.6 | 2.0.10 - 2.0.12 | 当前版本 | 与 TDengine TSDB 相同版本 |
| **2.4.0.4 - 2.4.0.13** | 2.0.37      | 当前版本                                    | develop 分支 | 1.0.2 - 1.0.6 | 2.0.10 - 2.0.12 | 当前版本 | 与 TDengine TSDB 相同版本 |
| **2.2.x.x**           | 2.0.36      | 当前版本                                    | master 分支  | n/a           | 2.0.7 - 2.0.9   | 当前版本 | 与 TDengine TSDB 相同版本 |
| **2.0.x.x**           | 2.0.34      | 当前版本                                    | master 分支  | n/a           | 2.0.1 - 2.0.6   | 当前版本 | 与 TDengine TSDB 相同版本 |

## 功能特性

连接器对 TDengine TSDB 功能特性的支持对照如下：


### WebSocket/原生 连接

| **功能特性**        | **Java** | **Python** | **Go** | **C#** | **Node.js** | **Rust** | **C/C++** |
| ------------------- | -------- | ---------- | ------ | ------ | ----------- | -------- | --------- |
| **连接管理**        | 支持     | 支持       | 支持   | 支持   | 支持        | 支持     | 支持      |
| **执行 SQL**        | 支持     | 支持       | 支持   | 支持   | 支持        | 支持     | 支持      |
| **参数绑定**        | 支持     | 支持       | 支持   | 支持   | 支持        | 支持     | 支持      |
| **数据订阅（TMQ）** | 支持     | 支持       | 支持   | 支持   | 支持        | 支持     | 支持      |
| **无模式写入**      | 支持     | 支持       | 支持   | 支持   | 支持        | 支持     | 支持      |

**备注**：Node.js 连接器不支持原生连接。

:::info
由于不同编程语言数据库框架规范不同，并不意味着所有 C/C++ 接口都需要对应封装支持。
:::


:::warning

- 无论选用何种编程语言的连接器，2.0 及以上版本的 TDengine TSDB 推荐数据库应用的每个线程都建立一个独立的连接，或基于线程建立连接池，以避免连接内的“USE statement”状态量在线程之间相互干扰（但连接的查询和写入操作都是线程安全的）。

:::

### REST API

支持 **执行 SQL**



import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import InstallOnLinux from "./_linux_install.mdx";
import InstallOnWindows from "./_windows_install.mdx";
import InstallOnMacOS from "./_macos_install.mdx";
import VerifyWindows from "./_verify_windows.mdx";
import VerifyLinux from "./_verify_linux.mdx";
import VerifyMacOS from "./_verify_macos.mdx";

## 安装客户端驱动

:::info
在没有安装 TDengine TSDB 服务端软件的系统上使用原生接口连接器或使用 C/C++ WebSocket 连接器，才需要安装客户端驱动。

:::

### 安装步骤

<Tabs defaultValue="linux" groupId="os">
  <TabItem value="linux" label="Linux">
    <InstallOnLinux />
  </TabItem>
  <TabItem value="windows" label="Windows">
    <InstallOnWindows />
  </TabItem>
  <TabItem value="macos" label="MacOS">
    <InstallOnMacOS />
  </TabItem>
</Tabs>

### 安装验证

以上安装和配置完成后，并确认 TDengine TSDB 服务已经正常启动运行，此时可以执行 TDengine TSDB CLI 工具进行登录。

<Tabs defaultValue="linux" groupId="os">
  <TabItem value="linux" label="Linux">
    <VerifyLinux />
  </TabItem>
  <TabItem value="windows" label="Windows">
    <VerifyWindows />
  </TabItem>
  <TabItem value="macos" label="MacOS">
    <VerifyMacOS />
  </TabItem>
</Tabs>

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
