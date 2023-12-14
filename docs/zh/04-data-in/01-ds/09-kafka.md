---
sidebar_label: Kafka
title: “Kafka”数据源
description: 使用“Kafka”数据源导入数据到 TDengine Cloud 的实例
---

Kafka 数据写入，是通过连接代理把数据从 Kafka 服务器写入到当前选择的 TDengine Cloud 实例。

## 先决条件

- 创建一个空数据库来存储 Kafka 数据。更多信息，请参阅 [数据库](../../../programming/model/#create-database)。
- 确保连接代理运行在与 Kafka 服务器位于同一网络的机器上。更多信息，请参阅 [安装连接代理](../install-agent/)。

## 具体步骤

1. 在 TDengine Cloud 中，在左边菜单中打开 **数据写入** 页面，在 **数据源** 选项卡上，单击 **添加数据源**打开新增页面。在**名称**输入框里面填写这个数据源的名称，并选择 **Kafka** 类型，在**代理**选择框里面选择已经创建的代理，如果没有创建代理，请点击旁边的**创建新的代理**按钮去创建新代理。
2. 在**目标数据库**里面选择一个当前所在的 TDengine Cloud 实例里面的数据库作为目标数据库。
3. 在 **bootstrap-servers** 栏目里，配置 Kafka 的 bootstrap 服务器，例如 192.168.1.92:9092，这个是必填字段。
4. 可以点击**连通性检查**, 检查 Cloud 实例 与 Kafka 服务之间是否可以连通。
5. 在 **SSL 证书**栏目中，如果开启了 SSL 认证，请上传对应的客户端证书和客户端私钥文件。
6. 在 **Consumer** 栏目中，需要配置消费者的超时时间，消费者组的 ID 等参数，topics、topic_partitions 这 2 个参数至少填写一个，其他参数有默认值。
7. 如果消费的 Kafka 数据是 JSON 格式，可以配置 **Payload 解析**卡片，对数据进行解析转换；
8. 填写完以上信息后，点击提交按钮，即可启动从 Kafka 到 TDengine 的数据同步。
