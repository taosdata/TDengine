---
sidebar_label: 开发指南
title: 开发指南
description: TDengine 多语言连接、SQL、参数绑定、无模式写入、高效写入、UDF 与数据订阅编程指南
---

若你准备采用 TDengine 作为时序数据处理平台来开发应用，通常需要完成以下几项工作：

1. **确定连接方式**。无论使用何种编程语言，都可以通过 REST API 访问 TDengine；多数语言还提供专用连接器，便于在应用中完成连接、写入与查询。
2. **确定数据模型**。根据应用场景与数据特征，决定建立一个还是多个库；分清静态标签与采集量，建立正确的超级表，再按需建立子表。
3. **选择写入方式**。TDengine 支持标准 SQL 写入与参数绑定写入；同时也支持无模式（Schemaless）写入，可按行协议直接写入，减少手工建表成本。
4. **编写查询 SQL**。根据业务要求编写所需的查询语句，完成统计、过滤与分析。
5. **实时统计分析**。若需要基于时序数据做轻量级实时统计（包括各类监测看板），建议优先使用 TDengine 的 [流式计算](../07-stream-processing/index.md)，而不必额外部署 Spark、Flink 等复杂的流式计算系统。
6. **消费新增数据**。若应用中有模块需要消费已写入数据，并希望在新数据到达时及时获知，建议优先使用 TDengine 的 [数据订阅](../06-data-subscription/index.md)，而不必专门部署 Kafka 或其他消息队列软件。
7. **获取最新状态**。在许多场景下（如车辆管理），应用需要获取各数据采集点的最新状态，建议优先使用 TDengine 的 Cache 能力，而不必单独部署 Redis 等缓存软件。
8. **扩展计算能力**。若内置函数无法满足需求，可使用用户自定义函数（UDF）扩展计算逻辑。

本章按上述开发路径组织。为便于理解，TDengine 为各功能及所支持的编程语言提供了示例代码，位于 [示例代码](https://github.com/taosdata/TDengine/tree/main/docs/examples)；示例正确性由 CI 保障，脚本位于 [示例代码 CI](https://github.com/taosdata/TDengine/tree/main/tests/docs-examples-test)。

本章包含：

- [建立连接](./01-connect/index.md)：安装驱动与连接器，建立 WebSocket 或原生连接。
- [执行 SQL](./02-execute-sql.md)：建库建表、写入与查询。
- [参数绑定](./03-stmt.md)：STMT / STMT2 高效写入。
- [无模式写入](./04-schemaless.md)：InfluxDB / OpenTSDB 等行协议写入。
- [高效写入](./05-high-throughput.md)：连接器高效写入特性与性能要点。
- [UDF 编程接口](./06-udf.md)：C / Python 自定义函数。
- [数据订阅编程接口](./07-subscription-api.md)：TMQ 消费者 API 与各语言示例。
- [连接器参考手册](./08-connectors-reference/index.md)：各语言连接器与 REST API 详解。
- [错误码](./09-error-codes.md)：客户端与服务端错误码说明。

若需深入了解 SQL 语法，请参阅 [TDengine SQL](../05-tdengine-sql/index.md)。若需进一步了解各连接器用法，请参阅 [连接器参考手册](./08-connectors-reference/index.md)。若需将 TDengine 与 Grafana 等第三方系统集成，请参阅 [第三方工具](../13-ecosystem-integrations/index.md)。

开发过程中如遇问题，可在各页面下方通过 [反馈问题](https://github.com/taosdata/TDengine/issues/new/choose) 在 GitHub 提交 Issue。
