---
sidebar_label: MQTT 数据订阅
title: MQTT 数据订阅
toc_max_heading_level: 4
---

TDengine TSDB v3.3.7.0 版本开始提供 MQTT 订阅功能，通过 MQTT 客户端连接 TDengine TSDB Bnode 服务，可直接订阅系统中已有主题的数据。

主要特性：

1. 协议支持：MQTT 5.0
2. 身份验证：使用 TDengine TSDB 原生验证
3. 主题管理：与标准 MQTT 协议不同，主题必须预先创建（因不支持消息发布，无法通过发布消息动态创建）
4. 共享主题：形如 $shared/group_id/topic_name 的主题被视为共享订阅，适用于需要负载均衡和高可用场景
5. 订阅位置：支持 latest，earliest (WAL 最早位置)
6. 服务质量：支持 QoS 0，QoS 1

## Bnode 节点管理

用户可通过 TDengine TSDB 的命令行工具 taos 进行 Bnode 的管理。执行下述命令都需要确保命令行工具 taos 工作正常。

### 创建 Bnode

```sql
CREATE BNODE ON DNODE {dnode_id}
```

一个 dnode 上只能创建一个 bnode。bnode 创建成功后，会自动启动 bnode 子进程 `taosmqtt`，默认在 6083 端口对外提供 MQTT 订阅服务，端口可在文件 taos.cfg 中通过参数 `mqttPort` 配置。例如：`create bnode on dnode 1`。

### 查看 Bnode

列出集群中所有的数据订阅节点，包括其 `id`, `endpoint`, `create_time`等属性。

```sql
SHOW BNODES;

taos> show bnodes;
     id    |   endpoint       |    protocol    |       create_time    | 
======================================================================
     1     | 192.168.0.1:6083 | mqtt        | 2024-11-28 18:44:27.089 | 
Query OK, 1 row(s) in set (0.037205s)
```

### 删除 Bnode

```sql
DROP BNODE ON DNODE {dnode_id}
```

删除 bnode 将把 bnode 从 TDengine TSDB 集群中移除，同时停止 taosmqtt 服务。

## 订阅数据示例

### 环境准备

```sql
create database db vgroups 1;
create table db.meters (ts timestamp, f1 int) tags(t1 int);
create topic topic_meters as select ts, tbname, f1, t1 from db.meters;
insert into db.tb using db.meters tags(1) values(now, 1);
create bnode on dnode 1;
```

在命令行工具 taos 中执行上面的 SQL 语句，创建数据库，超级表，主题 `topic_meters` ，bnode 节点，写入一条数据供下一步订阅使用。

### 客户端订阅

可以使用兼容 MQTT 协议 v5.0 版本的客户端来订阅前一步环境中的数据，这里使用 Python paho-mqtt 来举例说明：

在操作系统命令行界面中依次执行下面这些命令，便可以订阅到上一步中写入的数据；订阅成功后，如果 `topic_meters` 主题中有新增的写入数据，则会自动通过 MQTT 协议推送到客户端。

```shell
python3 -m venv .test-env
source .test-env/bin/activate
pip3 install paho-mqtt==2.1.0
python3 ./sub.py
```

其中 sub.py 文件的内容如下：

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
client.connect("127.0.1.1", 6083)

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

其中第三行 `topic_meters` 是我们订阅的主题，1 是这一条消息的 QoS 值，后面是一个 utf-8 编码的 JSON 消息，其中 `rows` 是数据行的数组。

