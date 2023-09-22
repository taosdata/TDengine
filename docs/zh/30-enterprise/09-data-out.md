---
toc_max_heading_level: 4
title: 数据输出
sidebar_label: 数据输出
---

本节讲述如何使用 taosX 的命令行模式将数据从 TDengine接入到各种数据源。对于 taosX 的命令行参数解析，请参考 [taosX](../../reference/taosx)。您也可以使用 taos-explorer 的可视化界面进行数据接入，具体请参考[可视化管理](../explorer)。服务安装与部署请参考 [安装与部署](../../get-started)。


## Kafka

### 命令行参数

taosx 支持从 TDengine 的数据输出到 Kafka。命令如下所示：
```shell
taosx run -f "<TDengine-DSN>" -t "<Kafka-DSN>" 
```
或
```shell
taosx run -f "<TDengine-DSN>" -t "<Kafka-DSN>"
```
其中：
- -f或--from：TDengine 的 DSN
- -t或--to： Kafka 的 DSN
  
### TDengine DSN 配置

TDengine DSN  的完整配置如下：
```shell
tmq://user:password@host:port/db?table=table&topic_suffix=topic_suffix[&cols=cols[&tags=tags]][&start=start][&end=end][&ts=ts]
```
- table: 输出数据的超级表名，此字段为必填字段;
- topic_suffix: TMQ topic名字的后缀，此项为必填字段;
- cols: 超级表中被订阅的列，默认为所有字段，此项为可选字段;
- tags: 超级表中被订阅的标签，默认为所有标签，此项为可选字段；
- start: 订阅数据的开始时间，此项为可选字段;
- end: 订阅数据的结束时间，此项为可选字段;
- ts: 时间戳的列名，默认为ts，此项为可选字段。


### Kafka DSN 配置

Kafka DSN  的完整配置如下：
```shell
kafka://host:port/topic?[&ack_timeout=acktimeout][&batch_size=batchsize]
```
- ack_timeout: Kafka 消息消费的超时时间，此字段为可选字段，默认值为1秒;
- batch_size: 批量发送到 Kafka 的数据条数，此项为可选字段，默认值为1;


### 示例

从192.168.2.19上的TDengine输出数据到192.168.2.13服务器的Kafka实例中。

```bash
taosx run \
  -f "tmq://root:taosdata@192.168.2.19:6030/testkafka?table=meters&cols=ts,id,voltage,current,phase&tags=location&start=2023-09-06 00:00:00&end=2023-09-21 15:11:41&ts=ts&topic_suffix=kafkatest" \
  -t "kafka://192.168.2.13:9092/test_out?ack_timeout=1&batch_size=1"  \
  --verbose
```
