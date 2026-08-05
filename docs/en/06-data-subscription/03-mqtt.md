---
sidebar_label: MQTT Data Subscription
title: MQTT Data Subscription
description: Subscribe to topic data through an MQTT client connected to a Bnode
toc_max_heading_level: 4
---

Starting from `v3.3.7.0`, TDengine supports subscription over MQTT. Connect an MQTT client to a TDengine Bnode service to subscribe directly to data from existing topics.

Key Features:

1. Protocol Support: MQTT 5.0 is recommended. MQTT 3.1 and 3.1.1 are also supported, but user properties such as `sub-offset` require MQTT 5.0.
2. Authentication: Uses TDengine native authentication
3. Topic Management: Unlike the standard MQTT protocol, topics must be pre-created (as message publishing is not supported, topics cannot be dynamically created via message publishing)
4. Shared Topics: Topics in the format `$share/group_id/topic_name` are treated as shared subscriptions, suitable for scenarios requiring load balancing and high availability
5. Subscription Position: Supports `latest` and `earliest` (the earliest WAL position). Set the subscription user property `sub-offset=earliest` to request the earliest position; the default is `latest`.
6. Quality of Service: Supports QoS 0 and QoS 1

## Bnode Management

You manage Bnodes through the [`taos` CLI](../12-operations-and-tooling/04-tools/01-taos-cli.md).

### Create a Bnode

Use the following SQL statement to create a bnode:

```sql
CREATE BNODE ON DNODE <dnode_id>;
```

You can create only one bnode on each dnode. Once the bnode is successfully created, a bnode subprocess named `taosmqtt` is automatically started to provide MQTT subscription services.

The `taosmqtt` service uses port 6057 by default. You can modify the `mqttPort` parameter in `taos.cfg` to provide MQTT subscription services on a different port. See [taosd configuration parameters](../12-operations-and-tooling/03-components/01-taosd.md).

### View Bnodes

Use the following SQL statement to display information about the Bnodes in your cluster. For the complete field list, see [`INS_BNODES`](../05-tdengine-sql/09-system-info/01-meta.md#ins_bnodes).

```sql
SHOW BNODES;
```

Information similar to the following is displayed:

```text
taos> show bnodes;
     id    |   endpoint       |    protocol    |       create_time    | 
======================================================================
     1     | 192.168.0.1:6057 | mqtt        | 2024-11-28 18:44:27.089 | 
Query OK, 1 row(s) in set (0.037205s)
```

### Delete a Bnode

Use the following SQL statement to delete a bnode:

```sql
DROP BNODE ON DNODE <dnode_id>;
```

Deleting a bnode removes it from the TDengine cluster and stops the `taosmqtt` service.

## MQTT Data Subscription Example

This example creates test data in TDengine and subscribes to the data. You can use any compatible MQTT client; this example uses MQTT 5.0 with the Python `paho-mqtt` library so that it can set `sub-offset`.

### Create Test Data

In the taos CLI, run the following SQL statements to set up your environment for this example.

```sql
CREATE DATABASE db VGROUPS 1;
CREATE TABLE db.meters (ts TIMESTAMP, f1 INT) TAGS (t1 INT);
CREATE TOPIC topic_meters AS SELECT ts, tbname, f1, t1 FROM db.meters;
INSERT INTO db.tb USING db.meters TAGS (1) VALUES (now, 1);
CREATE BNODE ON DNODE 1;
```

This creates a database, a supertable, and a topic. It then inserts a test data point and creates a bnode.

### Create a Consumer

Write a consumer in Python as follows:

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

Save this program as `sub.py`.

### Subscribe to the Topic

Run the following commands to subscribe to the data in `topic_meters`:

```shell
python3 -m venv .test-env
source .test-env/bin/activate
pip3 install paho-mqtt==2.1.0
python3 ./sub.py
```

Once the subscription is successful, any new data written to the `topic_meters` topic will be automatically pushed to the client via MQTT.

## Message Format

With the example in the previous section, the following information shall be outputted:

```shell
CONNACK received with code Success.
Subscribed: 1 [ReasonCode(Suback, 'Granted QoS 1')]
topic_meters 1 b'{"topic":"topic_meters","db":"db","vid":2,"rows":[{"ts":1753086482326,"tbname":"tb","f1":1,"t1":1}]}'
```

In the third line, `topic_meters`, is the topic we subscribed to. 1 is the QoS value for this message, followed by a JSON message encoded in UTF-8, where `rows` is an array of data rows.
