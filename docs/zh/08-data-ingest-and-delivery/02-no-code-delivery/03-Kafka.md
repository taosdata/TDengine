---
sidebar_label: Kafka
title: Kafka
description: 通过 taosExplorer 将 TMQ 数据发布到 Kafka
toc_max_heading_level: 4
---

Apache Kafka 是一个高吞吐、可扩展的分布式消息队列系统，常用于实时数据采集、日志传输、流式处理和事件驱动架构。TDengine 支持将 TMQ 订阅到的数据和元数据发布到 Kafka，便于将时序数据进一步对接到数据平台、实时计算引擎或下游业务系统。

通过 taosExplorer 创建 Kafka 数据发布任务后，系统会从指定的 TMQ 主题读取消息，并根据配置写入一个或多个 Kafka Topic。用户可以分别控制数据消息与元数据消息是否发布，也可以在保存前执行连通性检查和消息预览。

> **注意：本功能仅适用于 TDengine 企业版。**

## 确认企业版服务正常

- 确认 taosd 服务正常；
- 确认 taosAdapter 服务正常；
- 确认 taosX 已安装（`taosx --version`），以便支持数据发布能力。

## 准备 Kafka 环境

在创建发布任务前，用户需要先准备可访问的 Kafka 集群，并确认以下信息：

- Kafka `Bootstrap Servers` 地址；
- 目标 Topic 是否已存在；
- 如需认证，确认 SASL 或 SSL 相关参数和证书文件；
- 如需自动创建 Topic，确认 Broker 侧允许创建 Topic，且账号具备相应权限。

## 数据准备

通过命令行工具 `taos` 或管理界面 taosExplorer 执行 SQL 语句，创建数据库、超级表、主题，并写入数据，供后续发布任务使用。以下为简单示例：

```sql
CREATE DATABASE db VGROUPS 1;
CREATE TABLE db.meters (ts TIMESTAMP, f1 INT) TAGS (t1 INT);
CREATE TOPIC topic_meters AS SELECT ts, tbname, f1, t1 FROM db.meters;
INSERT INTO db.tb USING db.meters TAGS (1) VALUES (now, 1);
```

关于主题定义、消费位点和订阅参数的更多说明，请参考 [数据订阅](../../06-data-subscription/index.md)。

## 创建 Kafka 数据发布任务

用户可以在 taosExplorer 中进入“数据发布”页面，选择 Kafka 作为目标类型，并按顺序完成以下配置：

1. 填写任务名称；
2. 配置 TDengine TMQ 订阅参数；
3. 配置 Kafka 连接参数；
4. 配置数据 Topic、元数据 Topic 和消息 Key 模板；
5. 按需配置高级选项；
6. 执行连通性检查和预览；
7. 保存任务并启动发布。

创建成功后，任务大致按以下流程运行：

1. 从 `Topic DSN` 指定的 TMQ 地址订阅消息；
2. 根据“启用数据订阅”和“启用元数据订阅”的配置决定订阅哪些消息；
3. 连接目标 Kafka 集群；
4. 将数据消息写入数据 Topic，将元数据消息写入元数据 Topic；
5. 如启用了自动创建 Topic，则在目标 Topic 不存在时尝试自动创建；
6. 保存前可以执行连通性检查和预览，验证链路与消息格式。

## 基本配置说明

![基本信息配置区块](../../assets/no-code-delivery-kafka-01.webp)

上图展示了创建 Kafka 数据发布任务时的“基本信息”配置区块。

### 任务名称

- 默认值：无。
- 是否必填：是。
- 作用：标识当前数据发布任务。
- 建议：名称中包含源 Topic、目标用途和环境信息，便于运维定位。

示例：

- `prod-device-data-to-kafka`
- `tmq_order_meta_to_kafka`

![TDengine 数据订阅配置区块](../../assets/no-code-delivery-kafka-02.webp)

上图展示了 TDengine TMQ 订阅参数和订阅控制项配置区块。

### Topic DSN

- 默认值：无。
- 是否必填：是。
- 作用：指定 TMQ 的完整连接地址，是任务的数据来源。
- 建议：建议显式带上 `group.id`、`auto.offset.reset` 等关键订阅参数，便于任务行为可预测。
- 常见格式：

```text
tmq+ws://root:taosdata@localhost:6041/topic_meters
```

常用订阅参数包括：

- `group.id`：消费组 ID。建议在生产环境显式指定。
- `auto.offset.reset`：消费起始位置。在 taosExplorer 中由单独的“起始位置”必填字段控制，底层会对应到 `earliest` 或 `latest`。
- `with.meta`：是否同步元数据。
- `with.meta.delete`：是否同步元数据中的删除数据事件。
- `with.meta.drop`：是否同步元数据中的删表事件。
- `experimental.snapshot.enable`：是否同步已落盘数据。

示例：

```text
tmq+ws://root:taosdata@localhost:6041/topic_meters?group.id=pub-kafka-demo&auto.offset.reset=earliest&with.meta=true
```

### 起始位置

- 默认值：无。
- 是否必填：是。
- 作用：指定 TMQ 首次消费或无已提交位点时的起始消费位置。
- 建议：首次验证或补历史数据时选择 `earliest`，持续在线任务优先选择 `latest`。
- 可选值：`earliest`、`latest`。
- `earliest`：从最早可读位置开始消费，适合首次全量验证或补历史数据。
- `latest`：只消费新进入的消息，适合已上线的持续发布任务。

### 消费组 ID

- 默认值：空，通常由系统自动生成。
- 是否必填：否。
- 作用：标识 TMQ 消费组。
- 建议：生产环境使用固定值，便于控制消费位点和重启续传行为。

### 启用数据订阅

- 默认值：开启。
- 是否必填：否。
- 作用：控制是否发布 TMQ 中的行数据消息。
- 建议：默认保持开启；如果关闭，则应确认任务确实只需要发布元数据。
- 开启后，提交时必须配置“数据 Topic”。

### 启用元数据订阅

- 默认值：关闭。
- 是否必填：否。
- 作用：控制是否发布建表、删表、表结构变更等元数据消息。
- 建议：仅在下游需要感知建表、删表或结构变更时启用。
- 仅启用元数据订阅时，必须配置“元数据 Topic”。

### TSDB 数据

- 默认值：开启。
- 是否必填：否。
- 作用：控制是否同时订阅已持久化的 TSDB 数据，而不仅是 WAL 中的数据。
- 建议：根据上游 TMQ 设计和业务目标决定是否启用。

### 表删除操作

- 默认值：开启。
- 是否必填：否。
- 作用：控制是否转发删表事件。
- 建议：仅在下游需要同步表生命周期时保持开启。

### 数据删除操作

- 默认值：开启。
- 是否必填：否。
- 作用：控制是否转发删除数据事件。
- 建议：仅在下游需要同步删除行为时保持开启。

## Kafka 连接配置

![Kafka 连接配置区块](../../assets/no-code-delivery-kafka-03.webp)

上图展示了 Kafka Broker 连接与认证参数配置区块。

### Bootstrap Servers

- 默认值：无。
- 是否必填：是。
- 作用：指定 Kafka Broker 地址列表。
- 建议：至少填写 2 个可访问的 Broker 地址，并使用客户端实际可访问的网络地址，以提升可用性。
- 多个地址使用英文逗号分隔。

示例：

```text
127.0.0.1:9092,127.0.0.1:9093
```

### SASL 认证

- 默认值：不启用。
- 是否必填：否。
- 作用：配置 Kafka Broker 的 SASL 认证机制与认证参数。
- 建议：仅在 Kafka 集群要求认证时启用，并确保所选机制与 Broker 端配置一致。

支持的认证机制包括：

- `PLAIN`
- `SCRAM-SHA-256`
- `GSSAPI`

配置规则如下：

- 未选择时，表示不启用 SASL；
- 选择 `PLAIN` 或 `SCRAM-SHA-256` 时，需要配置用户名和密码；
- 选择 `GSSAPI` 时，需要配置 Kerberos 相关参数。

#### PLAIN 或 SCRAM-SHA-256

需要配置：

- 用户名；
- 密码。

#### GSSAPI

需要配置：

- Kerberos 服务名；
- Kerberos principal；
- Kerberos 初始化命令；
- Kerberos keytab 文件。

示例命令：

```text
kinit -R -t '%{sasl.kerberos.keytab}' -k %{sasl.kerberos.principal}
```

### SSL 认证

- 默认值：关闭。
- 是否必填：否。
- 作用：配置 Kafka Broker 的 SSL 证书校验与双向认证参数。
- 建议：生产环境启用 TLS 时，提前校验证书链、证书密码和私钥文件是否匹配。
- 开启后，需要配置 CA、客户端证书和客户端私钥等证书文件。
- 证书文件通常使用 PEM 格式。

常见参数包括：

- CA；
- CA 密码；
- 客户端证书；
- 客户端私钥。

## Kafka 发布配置

![Kafka 发布配置区块](../../assets/no-code-delivery-kafka-04.webp)

上图展示了 Kafka Topic、消息 Key 和 Topic 管理相关配置区块。

### 数据 Topic

- 默认值：`taosx.data.out`
- 是否必填：启用“数据订阅”时为是，否则为否。
- 作用：定义数据消息要写入的 Kafka Topic。
- 建议：命名中包含环境、业务域或源表信息，便于下游路由和运维定位。

支持的模板变量：

- `${database}`：源数据库名；
- `${table}`：子表名或普通表名；
- `${stable}`：超级表名；
- `${tmq_topic}`：TMQ 主题名；
- `${vgroup_id}`：vgroup ID；
- `${offset}`：消息偏移量。

示例：

- `taosx.data.out`
- `data.${database}.${table}`

### 元数据 Topic

- 默认值：空。
- 是否必填：仅启用“元数据订阅”且关闭“数据订阅”时为是，否则为否。
- 作用：定义元数据消息要写入的 Kafka Topic。
- 建议：如需将元数据与数据分流，建议单独指定 Topic；否则可以复用“数据 Topic”。
- 如果不填写，默认回退到“数据 Topic”。

### 数据 Key 模板

- 默认值：空。
- 是否必填：否。
- 作用：定义数据消息的 Kafka Key。
- 建议：如需按照库、表或设备维度进行分区，建议配置稳定的 Key 模板。

可用模板变量：

- `${database}`
- `${table}`
- `${stable}`
- `${tmq_topic}`
- `${vgroup_id}`

示例：

- `${database}.${table}`

### 元数据 Key 模板

- 默认值：空。
- 是否必填：否。
- 作用：定义元数据消息的 Kafka Key。
- 建议：如需保持与数据消息相同的分区策略，可直接复用“数据 Key 模板”。
- 为空时默认使用“数据 Key 模板”。

### 自动创建 Topic

- 默认值：关闭。
- 是否必填：否。
- 作用：当目标 Topic 不存在时，尝试在 Kafka 中自动创建。
- 建议：仅在 Broker 允许自动建 Topic 且当前账号具备权限时启用。
- 是否真正创建成功，还取决于 Kafka Broker 配置和当前账号权限。

开启后可额外配置：

- Topic 分区数；
- 副本因子。

### Topic 分区数

- 默认值：空，使用 Broker 默认配置。
- 是否必填：否。
- 作用：指定自动创建 Topic 时的分区数。
- 建议：根据下游消费并发和预期吞吐量规划分区数。
- 显示条件：启用“自动创建 Topic”后显示。
- 取值范围：1 到 1024。

### 副本因子

- 默认值：空，使用 Broker 默认配置。
- 是否必填：否。
- 作用：指定自动创建 Topic 时的副本因子。
- 建议：设置为不超过 Kafka 集群可用 Broker 数量，并与集群高可用策略保持一致。
- 显示条件：启用“自动创建 Topic”后显示。
- 取值范围：1 到 128。
- 该值不能超过 Kafka 集群中可用 Broker 数量。

## 高级选项

![高级选项配置区块](../../assets/no-code-delivery-kafka-05.webp)

上图展示了 Kafka Producer 的并发、批处理和扩展参数配置区块。

### 并发数

- 默认值：`1`
- 是否必填：否。
- 作用：控制 Kafka Producer 的最大并发数。
- 建议：初始可从 1 或 2 开始，在高吞吐场景下逐步上调。
- 取值范围：1 到 128。

### 队列超时（ms）

- 默认值：`30000`
- 是否必填：否。
- 作用：消息进入发送队列后的最大等待时间。
- 建议：网络波动较大时可适当调大；追求快速失败时可适当调小。

### 批次大小

- 默认值：`1000`
- 是否必填：否。
- 作用：每个 Kafka 批次最多包含的记录数。
- 建议：吞吐优先场景可逐步调大，低延迟场景保持较小值。
- 取值范围：1 到 100000。

### 批次超时（ms）

- 默认值：`1000`
- 是否必填：否。
- 作用：一个批次在发送前允许等待的最长时间。
- 建议：高吞吐场景可适当调大，低延迟场景可适当调小。

### Kafka 扩展参数

- 默认值：空。
- 是否必填：否。
- 作用：配置标准字段之外的 Kafka Producer 附加参数。
- 建议：仅填写明确了解含义的 Kafka 原生参数，避免与标准配置项冲突。

示例：

```text
compression.type=zstd
acks=all
linger.ms=100
```

常见用途包括：

- 启用消息压缩；
- 配置 `acks`；
- 调整 `linger.ms` 等发送策略。

## 保存前校验规则

提交任务时通常会执行以下逻辑校验：

1. 必须填写 `Topic DSN`，并显式选择“起始位置”；
2. “启用数据订阅”和“启用元数据订阅”不能同时关闭；
3. 如果启用了“数据订阅”，则必须填写“数据 Topic”；
4. 如果关闭了“数据订阅”但启用了“元数据订阅”，则必须填写“元数据 Topic”；
5. 提交前会执行 TMQ 与 Kafka 的连通性检查，检查失败时不能继续保存。

部分配置项会按条件动态显示。例如：

- 未选择 SASL 认证时，不显示 SASL 详细参数；
- 关闭 SSL 认证时，不显示 SSL 证书相关参数；
- 关闭“自动创建 Topic”时，不显示分区数和副本因子。

## 连通性检查与预览

### 连通性检查

![连通性检查区块](../../assets/no-code-delivery-kafka-06.webp)

上图展示了任务保存前的连通性检查入口。

保存或预览前，建议先执行连通性检查，以验证：

- TMQ 地址是否可访问；
- Kafka Broker 地址是否可访问；
- SASL 或 SSL 参数是否正确；
- 相关 Topic 和权限是否满足要求。

如检查失败，应优先确认网络、认证、地址和权限配置。

### 预览

![预览参数区块](../../assets/no-code-delivery-kafka-07.webp)

上图展示了预览数据条数和等待时间配置区块。

保存任务前，用户可以通过预览功能查看最终生成的 Kafka 消息样例。

常用预览参数包括：

- 数据条数：默认 `1`，范围 `1` 到 `100`；
- 等待时间（秒）：默认 `30`，范围 `1` 到 `300`。

预览结果通常展示以下字段：

- `topic`
- `key`
- `value`

如果在等待时间内未获取到数据，系统会提示当前条件下没有可预览的消息。

![Kafka 发布预览区块](../../assets/no-code-delivery-kafka-08.webp)

上图展示了 Kafka 发布预览结果区域。

## 验证发布结果

任务保存并启动后，用户可以通过 Kafka 自带工具或第三方客户端验证消息是否正常发布。

例如，使用 `kcat` 订阅目标 Topic：

```bash
kcat -b 127.0.0.1:9092 -t taosx.data.out -C
```

如配置了 Key 模板，消费结果中通常可以看到消息 Key 与消息体同时输出。用户可结合源库、表名、Topic 模板和偏移量字段确认发布结果是否符合预期。
