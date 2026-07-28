---
sidebar_label: 零代码数据写入
title: 零代码数据写入
description: 使用 taosExplorer 和 taosX 快速体验 MQTT 零代码数据写入
toc_max_heading_level: 4
---

在实际业务中，设备数据可能来自 MQTT、Kafka、OPC、PI System、CSV 文件或关系型数据库。手写采集程序虽然灵活，但需要处理连接、解析、字段映射、断点续传和错误恢复等细节。

TDengine 可以通过 taosX 和 taosExplorer 提供零代码数据写入能力。你只需要在 Web 页面中配置数据源、解析规则和目标表映射，就可以把外部数据持续写入 TDengine。

本章以 MQTT 为例，使用公共 MQTT Broker 和一条 JSON 电表消息，快速体验从“配置任务”到“查询入库数据”的完整流程。

## 前提条件

请先确认已经完成以下准备：

1. TDengine 服务已经启动。
2. taosExplorer 可以正常访问。
3. taosX 相关服务已经启动，taosExplorer 中可以进入“数据写入”页面。
4. 你可以访问外部 MQTT Broker。本文使用 EMQX 提供的公共 MQTT Broker。

如果你使用本快速上手中的 Docker 启动方式，通常已经包含 taosExplorer 和 taosX 相关组件。若页面不可用，请先检查容器端口和服务状态。

## 创建 MQTT 写入任务

打开 taosExplorer，进入“数据写入”页面，点击 **+ 新增数据源**，开始创建写入任务。

在任务基本信息中填写：

- 任务名称：`quick_mqtt_meter`。
- 任务类型：`MQTT`。
- 目标数据库：`test_mqtt`。如果数据库不存在，可以在页面中点击 **+ 创建数据库**。

## 配置连接信息

在连接配置中填写公共 MQTT Broker 信息：

- Broker 地址：`broker.emqx.io`。
- Broker 端口：`1883`。
- TLS 校验：关闭。
- 用户名和密码：留空。公共 Broker 不需要认证。

点击 **检查连通性**。如果页面提示“数据源可用”，说明 taosX 可以连接到 MQTT Broker。

## 配置 MQTT 订阅

在 MQTT 协议配置中填写：

- MQTT 协议版本：`3.1` 或页面默认值。
- Client ID：可以填写 `tdengine-quickstart`，也可以使用页面自动生成的值。
- 订阅主题和 QoS：`tdengine/quickstart/meter::0`。

订阅主题和 QoS 之间使用 `::` 分隔。上面的配置表示订阅 `tdengine/quickstart/meter` 主题，QoS 为 `0`。

## 配置 Payload 转换

在“消息体”或示例 Payload 输入框中填入下面的 JSON。它表示一条智能电表数据。

```json
{
  "ts": "2026-07-27T14:30:00+08:00",
  "id": 1,
  "current": 10.42,
  "phase": 1.38,
  "voltage": 220,
  "groupid": 7,
  "location": "beijing"
}
```

点击解析预览按钮，确认页面能够识别出 JSON 字段。然后在映射环节选择或创建目标超级表 `meters`。

如果需要创建超级表，可以按下面的结构配置字段和标签：

| 数据类型 | 名称 | 类型 | 说明 |
| ------------ | ------- | --- | --- |
| TIMESTAMP    | ts      | 字段 | 时间戳 |
| INT          | id      | 字段 | 电表 ID |
| DOUBLE       | current | 字段 | 电流值 |
| DOUBLE       | phase   | 字段 | 相位值 |
| INT          | voltage | 字段 | 电压值 |
| INT          | groupid | 标签 | 分组 ID |
| VARCHAR(128) | location | 标签 | 位置 |

完成字段映射后，点击 **提交**。页面会回到数据写入任务列表。

## 查看任务状态

任务提交后，观察任务状态。如果状态变为“运行中”，说明任务已经开始订阅 MQTT 主题，并等待消息写入 TDengine。

在任务列表中，你还可以查看写入速率、错误信息和最近运行状态。如果任务状态异常，可以进入详情页查看错误提示。

## 发送测试数据

你可以使用 [MQTTX](https://mqttx.app/zh) 或其他 MQTT 客户端向公共 Broker 发布测试消息。

发布配置如下：

- Broker 地址：`broker.emqx.io`。
- Broker 端口：`1883`。
- 发布主题：`tdengine/quickstart/meter`。
- 消息体：与上一步 Payload 示例保持一致。

为了避免相同时间戳数据被覆盖，重复测试时可以把 `ts` 改成当前时间。

```json
{
  "ts": "2026-07-27T14:31:00+08:00",
  "id": 1,
  "current": 10.58,
  "phase": 1.41,
  "voltage": 221,
  "groupid": 7,
  "location": "beijing"
}
```

## 查看写入结果

消息发布后，可以在 taosExplorer 的“数据浏览器”或 shell 中查询写入结果。

```sql
SELECT tbname, ts, current, voltage, phase, groupid, location
FROM test_mqtt.meters
ORDER BY ts DESC
LIMIT 5;
```

返回结果类似如下：

```text
 tbname |           ts            | current | voltage | phase | groupid | location |
==================================================================================
 t_1    | 2026-07-27 14:31:00.000 | 10.5800 |     221 | 1.410 |       7 | beijing  |
 t_1    | 2026-07-27 14:30:00.000 | 10.4200 |     220 | 1.380 |       7 | beijing  |
```

如果能查询到数据，说明 MQTT 消息已经通过 taosX 写入 TDengine。

## 常见问题

如果连通性检查失败，请检查：

- taosX 所在机器是否可以访问 `broker.emqx.io:1883`。
- MQTT Broker 地址、端口、TLS 和认证配置是否正确。
- 公司网络或云主机安全组是否限制了外部 MQTT 连接。

如果任务运行中但查询不到数据，请检查：

- MQTT 客户端发布的主题是否与任务订阅主题完全一致。
- Payload 是否能被解析，字段名是否与映射规则一致。
- 目标数据库和超级表是否创建成功。
- 查询的数据库名是否为 `test_mqtt`。

## 继续阅读

本章只演示最小 MQTT 写入流程。更多数据源和高级配置，请继续阅读以下文档：

- [数据接入与发布](../08-data-ingest-and-delivery/index.md)：零代码写入、数据发布与边云协同总览。
- [零代码数据写入](../08-data-ingest-and-delivery/01-no-code-ingestion/index.md)：支持的数据源、ETL 规则、健康状态与断点恢复。
- [MQTT](../08-data-ingest-and-delivery/01-no-code-ingestion/06-mqtt.mdx)：MQTT 数据接入完整配置说明。
- [Kafka](../08-data-ingest-and-delivery/01-no-code-ingestion/07-kafka.mdx)：Kafka 数据接入完整配置说明。
- [CSV](../08-data-ingest-and-delivery/01-no-code-ingestion/10-csv.mdx)：通过 CSV 文件导入数据。
- [OPC UA](../08-data-ingest-and-delivery/01-no-code-ingestion/04-opcua/index.md)：工业 OPC UA 数据接入。
- [可视化管理](./08-visual-management.md)：使用 taosExplorer 浏览数据、执行 SQL 和查看工具入口。
