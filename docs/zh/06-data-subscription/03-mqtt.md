---
sidebar_label: MQTT 订阅
title: MQTT 订阅
description: 通过 MQTT 客户端连接 Bnode 订阅主题数据
toc_max_heading_level: 4
---

TDengine 从 `v3.3.7.0` 开始提供 MQTT 订阅功能。通过 MQTT 客户端连接 TDengine Bnode 服务，可直接订阅系统中已有主题的数据。

主要特性：

1. 协议支持：推荐使用 MQTT 5.0（亦兼容 MQTT 3.1 / 3.1.1；`sub-offset` 等用户属性依赖 MQTT 5.0）。
2. 身份验证：使用 TDengine 原生验证。
3. 主题管理：与标准 MQTT 协议不同，主题必须预先创建（因不支持消息发布，无法通过发布消息动态创建）。
4. 共享主题：形如 `$share/group_id/topic_name` 的主题被视为共享订阅，适用于需要负载均衡和高可用的场景。
5. 订阅位置：支持 `latest`、`earliest`（WAL 最早位置）；可通过订阅用户属性 `sub-offset=earliest` 指定，默认 `latest`。
6. 服务质量：支持 QoS 0、QoS 1。

## Bnode 节点管理

用户可通过 TDengine 的命令行工具 `taos` 管理 Bnode。执行下述命令前，请确保 `taos` 可正常连接集群。

### 创建 Bnode

```sql
CREATE BNODE ON DNODE {dnode_id}
```

一个 Dnode 上只能创建一个 Bnode。Bnode 创建成功后，会自动启动 Bnode 子进程 `taosmqtt`，默认在 6057 端口对外提供 MQTT 订阅服务；端口可在 `taos.cfg` 中通过参数 `mqttPort` 配置，详见 [taosd 配置参数](../12-operations-and-tooling/03-components/01-taosd.md#mqttport)。例如：`CREATE BNODE ON DNODE 1`。

### 查看 Bnode

列出集群中所有的数据订阅节点，包括其 `id`、`endpoint`、`create_time` 等属性。更完整字段见元数据表 [`INS_BNODES`](../05-tdengine-sql/09-system-info/01-meta.md#ins_bnodes)。

```sql
SHOW BNODES;

taos> SHOW BNODES;
     id    |   endpoint       | protocol |          create_time    |
====================================================================
     1     | 192.168.0.1:6057 | mqtt     | 2024-11-28 18:44:27.089 |
Query OK, 1 row(s) in set (0.037205s)
```

### 删除 Bnode

```sql
DROP BNODE ON DNODE {dnode_id}
```

删除 Bnode 将把 Bnode 从 TDengine 集群中移除，同时停止 `taosmqtt` 服务。

## 订阅数据示例

### 环境准备

```sql
CREATE DATABASE db VGROUPS 1;
CREATE TABLE db.meters (ts TIMESTAMP, f1 INT) TAGS (t1 INT);
CREATE TOPIC topic_meters AS SELECT ts, tbname, f1, t1 FROM db.meters;
INSERT INTO db.tb USING db.meters TAGS (1) VALUES (now, 1);
CREATE BNODE ON DNODE 1;
```

在 `taos` 中执行上面的 SQL，创建数据库、超级表、主题 `topic_meters`、Bnode，并写入一条数据供下一步订阅使用。

### 客户端订阅

可以使用兼容 MQTT 协议的客户端来订阅前一步环境中的数据，这里使用 Python `paho-mqtt` 举例说明（示例按 MQTT 5.0 编写，以便设置 `sub-offset`）：

在操作系统命令行中依次执行下面这些命令，即可订阅到上一步写入的数据；订阅成功后，若 `topic_meters` 主题中有新增写入，则会通过 MQTT 协议推送到客户端。

```shell
python3 -m venv .test-env
source .test-env/bin/activate
pip3 install paho-mqtt==2.1.0
python3 ./sub.py
```

其中 `sub.py` 文件的内容如下：

```python
import time
import paho.mqtt
import paho.mqtt.properties as p
import paho.mqtt.packettypes as pt
import paho.mqtt.client as mqttClient

def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)
    sub_properties = p.Properties(pt.PacketTypes.SUBSCRIBE)
    sub_properties.UserProperty = ('sub-offset', 'earliest')
    client.subscribe("$share/g1/topic_meters", qos=1, properties=sub_properties)

def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

if paho.mqtt.__version__[0] > '1':
    client = mqttClient.Client(mqttClient.CallbackAPIVersion.VERSION2, client_id="tmq_sub_cid", userdata=None, protocol=mqttClient.MQTTv5)
else:
    client = mqttClient.Client(client_id="tmq_sub_cid", userdata=None, protocol=mqttClient.MQTTv5)

client.on_connect = on_connect
client.username_pw_set("root", "taosdata")
client.connect("127.0.1.1", 6057)

client.on_subscribe = on_subscribe
client.on_message = on_message

client.loop_forever()
```

## 消息格式

上一节的示例中，会输出下面的信息：

```shell
CONNACK received with code Success.
Subscribed: 1 [ReasonCode(Suback, 'Granted QoS 1')]
topic_meters 1 b'{"topic":"topic_meters","db":"db","vid":2,"rows":[{"ts":1753086482326,"tbname":"tb","f1":1,"t1":1}]}'
```

其中第三行 `topic_meters` 是订阅的主题，`1` 是该条消息的 QoS 值，后面是 UTF-8 编码的 JSON 消息，其中 `rows` 是数据行的数组。
