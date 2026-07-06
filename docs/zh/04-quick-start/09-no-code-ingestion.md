---
sidebar_label: '零代码数据写入'
title: 零代码数据写入
toc_max_heading_level: 4
---

通过 taosX 组件，TDengine 支持从各种数据源导入数据到 TDengine 中。taosExplorer 组件提供了一个统一的界面，用户通过简单的配置和操作，就可以将数据从不同的数据源导入到 TDengine 中。已支持的数据源包括：TDengine, PI, OPC, InfluxDB, MQTT, Kafka, CSV, MySQL, PostgreSQL, Oracle, MongoDB 等。如果您期望 TDengine 新增对某个数据源的支持，欢迎与我们的支持团队联系。

下面，以 MQTT 为例，介绍如何使用 taosExplorer 创建数据写入任务，从 MQTT Broker 中订阅数据，并写入到 TDengine 中。

## 配置任务的基本信息

1. 打开 taosExplorer，点击左侧导航栏中的“数据写入”页面
1. 在“数据写入任务”标签页，点击“+ 新增数据源”按钮，即可进入任务配置页面
1. 配置 MQTT 任务的基本信息：

- 输入任务的名称，例如：test-mqtt
- 选择任务的类型：MQTT
- 选择目标数据库：test-mqtt, 如果不存在，可以直接点击“+ 创建数据库”进行创建

## 配置任务的连接和认证信息

1. MQTT Broker 地址：broker.emqx.io, 这里使用的是 EMQ 提供的[公共 MQTT 服务器](https://www.emqx.com/zh/mqtt/public-mqtt5-broker)
1. MQTT 端口：1883
1. TLS 校验：不开启
1. 用户名和密码不需要填写，因为公共 MQTT 服务器不需要认证

## 配置 MQTT 协议相关的信息

1. MQTT 协议：3.1
1. 客户端 ID: MQTT 客户端 ID, 以 taosx 为前缀，可以随意填写，例如：tdengine-1234
1. 订阅主题及 QoS 配置：主题和 QoS 之间必须以 `::` 分隔，例如：tdengine-topic1::0
1. 其它配置项使用默认配置即可
1. 完成以上配置后，请点击“检查连通性”按钮，如果按钮下方会展示“数据源可用”，即表示连通性检查通过

## 配置 Payload 转换

1. 为了简化配置，请直接在文本框中输入以下 JSON 格式的示例消息，它代表的是北京市 id 为 1 的智能电表的电压、电流、相位值：

```json
{ "id": 1, "current": 10.42, "phase": 1.38, "voltage":200, "groupid": 7, "location": "beijing" }
```

2. TDengine 支持对 MQTT 消息进行解析、提取、过滤后，映射至 TDengine 数据库的超级表中
3. 在“解析”环节，直接点击解析配置行最右侧的预览按钮，即可在页面的右侧预览解析结果
4. 在“映射”环节，可以在当前数据库中选择欲写入 MQTT 消息的超级表，如果不存在，可点击“创建超级表”按钮
5. 点击“创建超级表”按钮，按照以下字段、标签信息，创建超级表 meters, taosExplorer 会根据 JSON 解析的结果，自动填充列名，仅需根据示例数据，选择匹配的数据类型和字段类型（字段或标签）即可，详见下表

 | 数据类型 | 名称 | 说明 |
 |-----|------|-----|
 | TIMESTAMP | ts | 时间戳 |
 | INT | id | 字段，电表 id |
 | DOUBLE | current | 字段，电流值 |
 | DOUBLE | phase | 字段，相位值 |
 | INT | voltage | 字段，电压值 |
 | INT | groupid | 标签，组 id |
 | VARCHAR(128) | location | 标签，位置 |

6. 创建并选择超级表后，即可点击“提交”按钮

## 查看任务状态

任务提交后，会自动跳转至数据写入任务的列表页，观察任务的状态，如果状态切换至“运行中”，即可开始消费 MQTT 主题中的数据，并写入 TDengine.

## 发送测试数据

推荐使用 EMQ 提供 MQTT 客户端工具 [MQTTX](https://mqttx.app/zh), 发送测试数据，详情可参考：[MQTTX 快速验证](https://docs.emqx.com/zh/emqx/latest/getting-started/getting-started.html#%E9%80%9A%E8%BF%87-mqttx-%E5%BF%AB%E9%80%9F%E9%AA%8C%E8%AF%81)。

MQTT Broker 及主题应与以上 MQTT 任务的配置保持一致，详情如下所示：

- MQTT Broker 地址：broker.emqx.io
- MQTT Broker 端口：1883
- MQTT 主题：tdengine-topic1
- 示例数据：与上方配置“Payload 转换”时，填写的示例数据格式保持一致：

```json
{ "id": 1, "current": 10.42, "phase": 1.38, "voltage":200, "groupid": 7, "location": "beijing" }
```

## 查看数据

1. 发送测试数据后，您可以通过 taosExplorer 查看数据是否成功写入 TDengine
2. 在 taosExplorer 中，切换至“数据浏览器”页面，选择相应的数据库和超级表
3. 执行 SQL 查询，查看数据是否存在，例如：

```sql
SELECT * FROM `test-mqtt`.`meters`;
```

4. 如果有数据返回，说明数据已从 MQTT 主题成功写入到 TDengine 中。
5. 在“数据写入”任务列表中，您还可以当前任务的运行状态、数据写入速率、错误信息等。
