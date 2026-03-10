---
sidebar_label: MQTT
title: MQTT
toc_max_heading_level: 4
---

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

MQTT (Message Queuing Telemetry Transport) is a lightweight, publish/subscribe-based messaging protocol widely used for communication between IoT devices. TDengine supports real-time data push to MQTT servers, enabling efficient data sharing and integration.

By configuring TDengine's MQTT publishing feature, users can easily distribute sensor data, device status, and other information to various endpoints.

Below are the basic steps to configure and use TDengine's MQTT publishing feature:

## Install and Configure MQTT Server

Before using TDengine's MQTT publishing feature, users need to install and configure an MQTT server. Common MQTT servers include Mosquitto, EMQX, etc. Users can choose the appropriate server based on their needs.

## Ensure Enterprise Edition Services Are Running

- Ensure the `taosd` service is running properly.
- Ensure `taosx` is installed (`taosx --version`) to support MQTT data publishing.

## Prepare Data

Use the `taos` CLI tool or the Explorer management interface to execute SQL statements, create databases, super tables, topics, and insert data for subsequent subscription. Here is a simple example:

```sql
create database db vgroups 1;
create table db.meters (ts timestamp, f1 int) tags(t1 int);
create topic topic_meters as select ts, tbname, f1, t1 from db.meters;
insert into db.tb using db.meters tags(1) values(now, 1);
```

## Create MQTT Data Publishing Task

Users can use `taosx` to publish TDengine topic data to MQTT:

```bash
taosx run -f "tmq+ws://username:password@ip:port/topic?param=value..." -t "mqtt://ip:port?param=value..."
```

Here, `-f` specifies the TMQ subscription DSN, and `-t` specifies the MQTT broker DSN. For details on using taosx and DSN, refer to the [taosX Component Documentation](../../14-reference/01-components/04-taosx.md).

TMQ DSN parameters:

- `username`: Database username.
- `password`: Database password.
- `ip` and `port`: IP and port for database connection.
- `topic`: TMQ subscription topic name.
- `with.meta`: Whether to synchronize metadata such as table creation, deletion, modification, and data deletion. The default value is `false`, which means metadata is not synchronized.
- `with.meta.delete`: Whether to synchronize delete data events in the metadata. This parameter is only effective when the `with.meta` parameter is enabled.
- `with.meta.drop`: Whether to synchronize drop table events in the metadata. This parameter is only effective when the `with.meta` parameter is enabled.
- `group.id`: TMQ subscription parameter, required, the group ID for the subscription.
- `client.id`: TMQ subscription parameter, optional, the client ID for the subscription.
- `auto.offset.reset`: TMQ subscription parameter, starting position for the subscription.
- `experimental.snapshot.enable`: TMQ subscription parameter. If enabled, data already persisted to TSDB storage files (not in WAL) can be synchronized. If disabled, only data still in WAL will be synchronized.

For more TMQ subscription parameters, see [Data Subscription](../../07-develop/07-tmq.md).

MQTT DSN parameters:

- `ip`: IP address of the MQTT broker.
- `port`: Port of the MQTT broker.
- `version`: MQTT protocol version, required, options are 3.1/3.1.1/5.0.
- `qos`: MQTT QoS level, default is 0.
- `client_id`: MQTT client ID, required, must be unique for each client.
- `topic`: Target MQTT topic for data publishing, required.
- `meta_topic`: Target topic for metadata publishing. If not specified, defaults to the data publishing topic.

The following variables are supported in MQTT `topic` and `meta_topic`:

- `database`: Source database name, included in both metadata and data.
- `tmq_topic`: Source TMQ topic name, included in both metadata and data.
- `vgroup_id`: Source vgroup ID, included in both metadata and data.
- `stable`: Source super table name, included only in metadata messages for creating super tables, creating sub-tables, and deleting super tables.
- `table`: Source table/sub-table name, included only in messages for creating sub-tables/regular tables, modifying tables, deleting sub-tables/regular tables, and data messages.

**Note**: If the topic contains variables not present in the message, the message will not be processed or published to the MQTT broker.

In this example, the command can be written as:

```shell
taosx run \
  -f "tmq+ws://root:taosdata@localhost:6041/topic_meters?group.id=taosx-pub-test&auto.offset.reset=earliest" \
  -t "mqtt://mqtt.tdengine.com:1883?topic=test/topic_meters&qos=1&version=5.0&client_id=taosx-pub-meters"
```

## Verify Data Publishing

You can use the [MQTTX](https://github.com/emqx/MQTTX) tool to verify the data publishing result. The message body for the above example is as follows:

```json
{"data":{"ts":1756957064991,"tbname":"tb","f1":1,"t1":1},"offset":{"database":"db","topic":"topic_meters","vgroupId":2,"offset":8}}
```

By following the above steps, users can easily configure and use TDengine's MQTT publishing feature to achieve efficient data interaction with IoT devices.
