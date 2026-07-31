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
3. taosX 相关服务已经启动，taosExplorer 左侧菜单可以进入 **数据写入** 页面。
4. 你可以访问外部 MQTT Broker。本文使用 EMQX 提供的公共 MQTT Broker。

如果你使用本快速上手中的 Docker 启动方式，通常已经包含 taosExplorer 和 taosX 相关组件。若页面不可用，请先检查容器端口和服务状态。

## 创建 MQTT 写入任务

打开 taosExplorer，进入左侧 **数据写入** 页面，点击 **创建新任务**，进入 **创建数据写入任务** 页面。

在 **基本信息** 中填写：

- **任务名称**：`quick_mqtt_meter`。
- **数据源类型**：`MQTT`。
- **目标数据库**：`test_mqtt`。如果数据库不存在，可以点击右侧 **创建数据库**。

**代理** 为可选项，本示例可留空。

## 配置 Broker 地址

在 **Broker 地址** 中填写公共 MQTT Broker 信息：

- **MQTT 地址**：`broker.emqx.io`。
- **MQTT 端口**：`1883`。

## 配置连接信息

在 **连接配置** 中填写：

- **MQTT 协议**：选择 `3.1`（页面默认值；也可选 `3.1.1` 或 `5.0`）。
- **客户端 ID**：页面会自动生成形如 `taosx_client_<8 位随机字符>` 的值，可直接使用，也可改成自定义值（同一 Broker 上需保证唯一）。
- **保活时间**、**清除会话**：保持默认即可（默认保活时间为 `60` 秒，清除会话为开启）。

在 **认证配置** 中填写：

- **用户名** 和 **密码**：留空。公共 Broker 不需要认证。
- **TLS 校验**：选择 **不开启**。

## 配置 MQTT 订阅

在 **采集配置** 中填写：

- **订阅主题及 QoS 配置**：`tdengine/quickstart/meter::0`。

输入格式为 `<topic>::<QoS>`，QoS 只能为 `0`、`1` 或 `2`。上面的配置表示订阅主题 `tdengine/quickstart/meter`，QoS 为 `0`。多个主题可用逗号分隔，例如 `topic1::0,topic2::1`。

本示例无需填写 **主题解析**、**数据压缩**、**字符编码** 等高级项，保持默认即可。

填写完成后，点击 **检查连通性**。如果页面提示 **数据源可用。**，说明 taosX 可以连接到 MQTT Broker。

## 配置 Payload 转换

在 **Payload 转换** 区域，找到 **示例消息体**，填入下面的 JSON。它表示一条智能电表数据。

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

点击 **解析** 环节的 **识别**（或预览图标），确认页面能够解析出 JSON 字段。然后在 **映射** 环节选择或创建目标超级表 `meters`。

如果需要新建超级表，点击 **创建超级表**，可按下面的结构配置普通列和标签：

| 数据类型 | 名称 | 类型 | 说明 |
| --- | --- | --- | --- |
| `TIMESTAMP` | `ts` | 字段 | 时间戳 |
| `DOUBLE` | `current` | 字段 | 电流值 |
| `DOUBLE` | `phase` | 字段 | 相位值 |
| `INT` | `voltage` | 字段 | 电压值 |
| `INT` | `groupid` | 标签 | 分组 ID |
| `VARCHAR(128)` | `location` | 标签 | 位置 |

超级表创建并选中后，在映射表中完成以下配置：

1. 在 **SubTableName**（类型为 `Tablename`）一行的 Expression 中填写 `t_{id}`，表示用消息体里的 `id` 字段生成子表名（例如 `id` 为 `1` 时，子表名为 `t_1`）。
2. 将其余列映射到对应的 JSON 字段（如 `ts`、`current`、`voltage`、`phase`、`groupid`、`location`）。

完成字段映射后，点击 **提交**。页面会回到 **数据写入任务** 列表。

## 查看任务状态

任务提交后，观察列表中的 **运行状态**。如果状态变为 **运行中**，说明任务已经开始订阅 MQTT 主题，并等待消息写入 TDengine。

在任务列表中，你还可以查看写入速率、错误信息和最近运行状态。如果任务状态异常，可以进入详情页查看错误提示。

## 发送测试数据

任务进入 **运行中** 后，可用 `mosquitto_pub`（[Eclipse Mosquitto](https://mosquitto.org/) 客户端）向公共 Broker 发布测试消息。若尚未安装，可先执行 `sudo apt install mosquitto-clients`（Debian/Ubuntu）或使用对应平台的 Mosquitto 客户端包。

发布示例：

```bash
mosquitto_pub -h broker.emqx.io -p 1883 -t 'tdengine/quickstart/meter' -q 0 -m '{
  "ts": "2026-07-27T14:31:00+08:00",
  "id": 1,
  "current": 10.58,
  "phase": 1.41,
  "voltage": 221,
  "groupid": 7,
  "location": "beijing"
}'
```

参数说明：

- `-h` / `-p`：Broker 地址与端口，对应 `broker.emqx.io:1883`。
- `-t`：发布主题，须与任务中 **订阅主题及 QoS 配置** 的主题一致（不含 `::QoS` 后缀）。
- `-q`：QoS，本示例为 `0`。
- `-m`：消息体，字段需与 **示例消息体** 及映射规则一致。

为了避免相同时间戳数据被覆盖，重复测试时可以把 `ts` 改成当前时间。也可以使用 [MQTTX](https://mqttx.app/zh) 等图形化客户端发布相同主题和消息体。

## 查看写入结果

消息发布后，可以在 taosExplorer 的 **数据浏览器** 或 shell 中查询写入结果。

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
- **MQTT 地址**、**MQTT 端口**、**TLS 校验** 和认证配置是否正确。
- 公司网络或云主机安全组是否限制了外部 MQTT 连接。

如果任务运行中但查询不到数据，请检查：

- MQTT 客户端发布的主题是否与任务 **订阅主题及 QoS 配置** 中的主题完全一致。
- **示例消息体** 是否能被 **识别**，字段名是否与映射规则一致。
- **SubTableName** 是否已填写，例如 `t_{id}`。
- 目标数据库和超级表是否创建成功。
- 查询的数据库名是否为 `test_mqtt`。

## 继续阅读

本章只演示最小 MQTT 写入流程。更多数据源和高级配置，请继续阅读以下文档：

- [数据接入与发布](../08-data-ingest-and-delivery/index.md)：零代码写入、数据发布与边云协同总览。
- [零代码数据写入](../08-data-ingest-and-delivery/01-no-code-ingestion/index.md)：支持的数据源、ETL 规则、健康状态与断点恢复。
- [MQTT](../08-data-ingest-and-delivery/01-no-code-ingestion/07-mqtt.mdx)：MQTT 数据接入完整配置说明。
- [Kafka](../08-data-ingest-and-delivery/01-no-code-ingestion/08-kafka.mdx)：Kafka 数据接入完整配置说明。
- [CSV](../08-data-ingest-and-delivery/01-no-code-ingestion/11-csv.mdx)：通过 CSV 文件导入数据。
- [OPC UA](../08-data-ingest-and-delivery/01-no-code-ingestion/05-opcua/index.md)：工业 OPC UA 数据接入。
- [可视化管理](./08-visual-management.md)：使用 taosExplorer 浏览数据、执行 SQL 和查看工具入口。
