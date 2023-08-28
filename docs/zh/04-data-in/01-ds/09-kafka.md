---
sidebar_label: Kafka
title: “Kafka”数据源
description: 使用“Kafka”数据源导入数据到 TDengine Cloud 的实例
---
Kafka 数据写入，是通过连接代理把数据从 Kafka 服务器写入到当前选择的 TDengine Cloud 实例。使用这种方式的前提条件是用户需要先在 InfluexDB 服务器上面下载安装相应的连接代理。

具体步骤如下：

1. 在**名称**输入框里面填写这个数据源的名称，并选择 **Kafka** 类型，在**代理**选择框里面选择已经创建的代理，如果没有创建代理，请点击旁边的**创建新的代理**按钮去创建新代理。
2. 在**目标数据库**里面选择一个当前所在的 TDengine Cloud 实例里面的数据库作为目标数据库。
3. 在 **bootstrap-servers** 栏目里，配置 Kafaka 的 bootstrap 服务器，例如192.168.1.92:9092，这个是必填字段。
4. 在 **SSL 证书**栏目中，如果开启了 SSL 认证，请上传对应的客户端证书和客户端私钥文件。
5. 在 **Consumer** 栏目中，需要配置消费者的超时时间，消费者组的ID等参数，topics、topic_partitions这2个参数至少填写一个，其他参数有默认值。
6. 如果消费的Kafka数据是JSON格式，可以配置 **Playload 解析**卡片，对数据进行解析转换；
7. 填写完以上信息后，点击提交按钮，即可启动从Kafka到TDengine的数据同步。
