---
toc_max_heading_level: 4
title: Data Export
sidebar_label: Data Export
---

This article describes how to use taosX to export data from TDengine. For more information about taosX, see [taosX](../../reference/taosx/). You can also use taosExplorer to set up data ingestion. For more information, see [taosExplorer](../explorer/). For more information about installing taosX, see [Installation](../../get-started/).


## Kafka

### Command-Line Parameters

You can export data from TDengine to Kafka. The command is as follows:
```shell
taosx run -f "<TDengine-DSN>" -t "<Kafka-DSN>"
```
where:
- `-f` or `-from`: Specify the TDengine DSN.
- `-t` or `--to`: Specify the Kafka DSN.

### TDengine DSN Configuration

The DSN for TDengine configured as follows:

```shell
tmq://user:password@host:port/db?table=table&topic_suffix=topic_suffix[&cols=cols[&tags=tags]][&start=start][&end=end][&ts=ts]
```
- table: super table name，this field is required;
- topic_suffix: TMQ topic suffix，this field is required;
- cols: columns to be subscribed, default is all columns，this field is optional;
- tags: tags to be subscribed, default is all tags, this field is optional;
- start: start timestamp for the data subscribed，this field is optional;
- end: end timestamp for the data subscribed，this field is optional;
- ts: the column name of timestamp，default is ts，this field is optional;


### Kafka DSN Configuration

The DSN for Kafka configured as follows:

```shell
kafka://host:port/topic?[&ack_timeout=acktimeout][&batch_size=batchsize]
```
- ack_timeout: Kafka ack timeout in seconds，this field is optional;
- batch_size: Batch size，this field is optional;


### Example

A Kafka instance is located at 192.168.2.13. This configuration export data from a TDengine cluster located at 192.168.2.19 to the Kafka instance.

```shell
taosx run \
  -f "tmq://root:taosdata@192.168.2.19:6030/testkafka?table=meters&cols=ts,id,voltage,current,phase&tags=location&start=2023-09-06 00:00:00&end=2023-09-21 15:11:41&ts=ts&topic_suffix=kafkatest" \
  -t "kafka://192.168.2.13:9092/test_out?ack_timeout=1&batch_size=1"  \
  --verbose
```
