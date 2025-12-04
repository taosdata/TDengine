---
sidebar_label: PI Backfill
title: “PI Backfill”数据源
description: 使用“PI Backfill”数据源导入数据到 TDengine Cloud 的实例
---

PI Backfill 数据写入，是通过连接代理把历史数据从 PI 系统写入到当前选择的 TDengine Cloud 实例。
该集成使用 AF SDK 从 PI 数据存档流式传输缓冲数据和查询历史数据，为流式传输数据设置 PI 和 AF 数据管道，并连接到 PI AF 以查询 AF 结构。它还能创建相应的表格，并通过安全的 RESTful API 将数据写入 TDengine Cloud 实例。

有关该解决方案的更多信息，请参阅 [TDengine 集成 PI 系统](https://tdengine.com/pi-system/)。

:::note 非常重要

首次创建 PI 数据源会在当前实例选择的价格方案上面产生额外费用。更多信息，请[联系咨询](https://cloud.taosdata.com)。

:::

## 先决条件

- 创建一个空数据库来存储 PI 系统数据。更多信息，请参阅 [数据库](../../../programming/model/#create-database)。
- 确保连接代理运行在与 PI 数据存档和 PI AF 服务器（可选）位于同一网络的机器上。更多信息，请参阅 [安装连接代理](../install-agent/)。
- 获取 PI 数据存档服务器的名称。
- （可选）如果要使用 PI AF，请获取 AF 数据库的名称。

## 具体步骤

1. 在 TDengine Cloud 中，在左边菜单中打开 **数据写入** 页面，在 **数据源** 选项卡上，单击 **添加数据源**打开新增页面。在**名称**输入框里面填写这个数据源的名称，并选择 **PI Backfill** 类型，在**代理**选择框里面选择已经创建的代理，如果没有创建代理，请点击旁边的**创建新的代理**按钮去创建新代理。
2. 在**目标数据库**里面选择一个当前所在的 TDengine Cloud 实例里面的数据库作为目标数据库。
3. PI 连接器支持两种连接方式：**PI Data Archive Only**: 不使用 AF 模式。此模式下直接填写 **PI 服务名**（服务器地址，通常使用主机名）。**PI Data Archive and Asset Framework (AF) Server**: 使用 AF SDK。此模式下除配置服务名外，还需要配置 PI 系统 (AF Server) 名称 (hostname) 和 AF 数据库名。
4. 可以点击**连通性检查**, 检查 Cloud 实例 与 PI 服务之间是否可以连通。
5. **监测点集** 可选择使用 CSV 文件模板或使用 **所有点**。
6. 在 **历史填充（Backfill）** 栏目里面，可以设置是否从当前 TDengine Cloud 实例已经有的数据开始，也可以设置以当前 TDengine Cloud 实例已经有的数据结束。另外需要设置历史回填的的开始时间和结束时间，如果结束时间没有填写，默认是当前时间。
7. 在**高级选项**卡片，可以配置以下信息：
   - **日志级别**: 日志级别，可选项为 DEBUG、INFO、WARN、ERROR。
   - **批次大小**: 单次发送的最大消息数或行数。
   - **批次延时**: 单次读取最大延时（单位为秒），当超时结束时，只要有数据，即使不满足 Batch Size，也立即发送。
8. 填写完成以上信息后，点击**新增**按钮，即可直接启动从 PI 系统到 TDengine Cloud 实例的数据同步。
