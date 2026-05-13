---
sidebar_label: MQTT
title: MQTT
toc_max_heading_level: 4
---

MQTT（Message Queuing Telemetry Transport）是一种轻量级的、基于发布/订阅模式的消息传输协议，广泛应用于物联网设备之间的通信。TDengine 支持将采集到的数据实时推送到 MQTT 服务器，实现高效的数据共享与联动。

通过配置 TDengine 的 MQTT 发布功能，用户可以方便地将传感器数据、设备状态等信息分发到各类终端。

> **注意：本功能仅适用于 TDengine 企业版。**

以下是配置和使用 TDengine MQTT 发布功能的基本步骤：

## 安装和配置 MQTT 服务器

在使用 TDengine 的 MQTT 发布功能之前，用户需要先安装并配置好 MQTT 服务器。常用的 MQTT 服务器有 Mosquitto、EMQX 等，用户可以根据实际需求选择合适的服务器。

## 确认企业版服务正常

- 确认 taosd 服务正常；
- 确认 taosx 已安装（`taosx --version`），以便支持数据的 MQTT 发布功能。

## 数据准备

通过命令行工具 `taos` 或管理界面 Explorer 执行 SQL 语句，创建数据库，超级表，主题，并写入数据，供下一步订阅使用。以下为简单示例：

```sql
create database db vgroups 1;
create table db.meters (ts timestamp, f1 int) tags(t1 int);
create topic topic_meters as select ts, tbname, f1, t1 from db.meters;
insert into db.tb using db.meters tags(1) values(now, 1);
```

## 创建 MQTT 数据发布任务

用户可以使用 `taosx` 将 TDengine 主题数据发布到 MQTT：

```bash
taosx run -f "tmq+ws://username:password@ip:port/topic?param=value..." -t "mqtt://ip:port?param=value..."
```

其中 `-f` 指定 TMQ 订阅的 DSN，`-t` 指定 MQTT broker 的 DSN。关于 taosx 和 DSN 的用法请参考 [taosX 组件文档](../../14-reference/01-components/04-taosx.md).

TMQ DSN 参数：

- `username`: 数据库用户名。
- `password`: 数据库密码。
- `ip` 和 `port`: 数据库连接使用的 IP 和端口。
- `topic`: TMQ 订阅的 topic 名称。
- `with.meta`: 是否同步元数据，如创建表，删除表，修改表，删除数据等操作，默认值为 `false` 表示不同步元数据。
- `with.meta.delete`: 是否同步元数据中的删除数据事件，此参数仅当 `with.meta` 参数启用时有效。
- `with.meta.drop`: 是否同步元数据中的删除表事件，此参数仅当 `with.meta` 参数启用时有效。
- `group.id`: TMQ 订阅参数，必填项，订阅的组 ID。
- `client.id`: TMQ 订阅参数，选填项，订阅的客户端 ID。
- `auto.offset.reset`: TMQ 订阅参数，订阅的起始位置。
- `experimental.snapshot.enable`: TMQ 订阅参数，如启用，可以同步已经落盘到 TSDB 时序数据存储文件中（即不在 WAL 中）的数据。如关闭，则只同步尚未落盘（即保存在 WAL 中）的数据。

更多 TMQ 订阅时的所有参数，详见 [数据订阅](../../07-develop/07-tmq.md)

MQTT DSN 参数：

- `ip`: MQTT broker 的 IP 地址。
- `port`: MQTT broker 的 端口。
- `version`: MQTT 协议版本，必填项，可选值 3.1/3.1.1/5.0。
- `qos`: MQTT QoS 服务质量，默认值 0。
- `client_id`: MQTT 客户端 ID，必填项，不同客户端的客户端 ID 必须不同。
- `topic`: 数据发布的 MQTT 目标 Topic，必填项。
- `meta_topic`: 元数据发布的目标 Topic，如果不指定，默认使用数据发布的 Topic。

在 MQTT 的 `topic` 和 `meta_topic` 中支持使用如下变量：

- `database`: 消息来源的数据库名，元数据和数据均包含。
- `tmq_topic`: 消息来源的 TMQ 主题名，元数据和数据均包含。
- `vgroup_id`: 消息来源 vgroup ID，元数据和数据均包含。
- `stable`: 消息来源的超级表名称，仅创建超级表，创建子表，删除超级表的元数据消息包含。
- `table`: 消息来源的表名/子表名，仅创建子表/普通表，修改表，删除子表/普通表以及数据类型的消息包含。

**注意**: 如果 topic 中包含了消息中不存在的变量，则当前消息不会被处理，即不会被发布到 MQTT broker。

在本示例中，可以写为：

```shell
taosx run \
  -f "tmq+ws://root:taosdata@localhost:6041/topic_meters?group.id=taosx-pub-test&auto.offset.reset=earliest" \
  -t "mqtt://mqtt.tdengine.com:1883?topic=test/topic_meters&qos=1&version=5.0&client_id=taosx-pub-meters"
```

## 验证数据发布

可使用 [MQTTX](https://github.com/emqx/MQTTX) 工具验证数据发布结果。以上示例对应消息体如下：

```json
{"data":{"ts":1756957064991,"tbname":"tb","f1":1,"t1":1},"offset":{"database":"db","topic":"topic_meters","vgroupId":2,"offset":8}}
```

通过以上步骤，用户可以轻松地配置和使用 TDengine 的 MQTT 发布功能，实现与物联网设备的高效数据交互。
