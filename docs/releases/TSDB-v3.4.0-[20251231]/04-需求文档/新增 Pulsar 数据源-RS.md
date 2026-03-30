# 新增 Pulsar 数据源-RS

## 1. 引言

Apache Pulsar 是一款开源的分布式发布-订阅消息系统，专为高性能、强一致性和弹性伸缩而设计。目前是 Kafka 的有力竞争者，也具有一定量的用户群体和生态，taosx 支持 Pulsar 数据同步可以扩展 tsdb 的生态系统。

### 1.1 术语与缩写名词

**Pulsar**：Apache Pulsar 是一款开源的分布式发布-订阅消息系统，专为高性能、强一致性和弹性伸缩而设计
**RecordBatch**：Apach Arrow 的核心数据结构，是一种高效的数据组织和交换格式。
**GCM 加密**：全称 Galois Counter Mode，是一种认证加密模式，它在提供数据机密性的同时，还能确保数据完整性和真实性。

### 1.2 相关文档资料

JIRA：[[Ume Tea] taosX: Add data source for Tuya IOT via Pulsar MQ](https%3A%2F%2Fjira.taosdata.com%3A18080%2Fbrowse%2FTS-7448) 
Pulsar 官网：https://pulsar.apache.org/
涂鸦文档：https://developer.tuya.com/cn/docs/iot/integrate-mq?id=Kavqdgattt1y2

### 1.3 优先级要求

1. 重要程度：高。
2. 期望交付时间：2025-11-11

### 1.4 版本要求

1. 版本类型：企业版闭源。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025-12-04 | 0.1 | 张贵川 | 文档编写 |

## 3. 需求目标

本需求源自 UmeTea 项目支持，目的是支持 Pulsar 数据源，以及客户涂鸦基于 Pulsar 定制的 Pulsar-Tuya 数据源。

## 4. 功能需求

| 序号 | **功能类别** | **功能名称** | 功能描述 |
| --- | --- | --- | --- |
| 采集 Pulsar 数据源 | 采集 Pulsar 数据源数据，并组装为特定的 RecordBatch 下发到数据流下游写入 tsdb |
| 命令行模式支持 Pulsar 数据源 | 命令行模式支持 Pulsar 数据源 |
| Agent 模式支持 Pulsar 数据源 | Agent 模式支持 Pulsar 数据源 |
| 解析涂鸦特定数据格式 | 涂鸦的 Pulsar 数据源有多种加密方式比如 GCM 和特定对接方式，按照对方提供的方式进行解析 |
| 命令行模式支持 Pulsar Tuya 数据源 | 命令行模式支持 Pulsar Tuya 数据源 |
| Agent 模式支持 Pulsar Tuya数据源 | Agent 模式支持 Pulsar Tuya 数据源 |
| 增加 Pulsar 数据源 | 在 DataIn 页面新增 Pulsar 数据源 |
| 增加 Pulsar-Tuya 数据源 | 在 DataIn 页面新增 Pulsar-Tuya 数据源 |


## 5. 性能需求

暂时不考虑，优先满足功能需求。尽量复用 kafka 采集框架，这样保证性能上能够尽量与 kafka 采集端靠齐。

## 6. 安全需求

1. 新增数据源需添加 Grant 条目，仅在 License 可用时允许添加数据源。
2. 数据源敏感信息不得记录日志，仅在运行时保存和使用。

## 7. 其他需求

- 易用性需求：
  - 支持页面配置操作
  - 支持命令行操作
- 测试需求
  - 页面上配置后可以正常进行数据同步
  - 命令行模式可以正常进行数据同步
  - Agent 模式可以正常进行数据同步
