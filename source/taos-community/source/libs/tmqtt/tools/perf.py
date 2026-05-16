import time
import json

import paho.mqtt
import paho.mqtt.properties as p
import paho.mqtt.packettypes as pt
import paho.mqtt.client as mqttClient
#from mqttClient import mqtt


def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)
    sub_properties = p.Properties(pt.PacketTypes.SUBSCRIBE)
    #sub_properties.UserProperty = ('sub-offset', 'earliest')
    sub_properties.UserProperty = ('sub-offset', 'earliest')
    sub_properties.UserProperty = ('proto', 'fbv')
    #client.subscribe("topic_meters", qos=1, properties=sub_properties)
    #client.subscribe("topic_meters", qos=2, properties=sub_properties)
    client.subscribe("$share/g3/topic_meters", qos=2, properties=sub_properties)
    #client.subscribe("$share/g1/stb_topic", qos=2, properties=sub_properties)
    #client.subscribe("$share/g1/db_topic", qos=2, properties=sub_properties)
    #client.unsubscribe("$share/g1/topic_meters")


def on_subscribe(client, userdata, mid, granted_qos, properties=None):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))

    client.subRows = 0
    client.start_time = time.time()

def on_message(client, userdata, msg):
    #print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
    jsonMsg = json.loads(str(msg.payload, encoding='utf-8'))
    msgRows = len(jsonMsg['rows'])
    client.subRows += msgRows
    #print("sub rows: ", client.subRows)
    if client.subRows > 999999:
        end_time = time.time()
        elapsed_time = end_time - client.start_time
        print("elapsed_time: ", elapsed_time)
        client.loop_stop()

# using MQTT version 5 here, for 3.1.1: MQTTv311, 3.1: MQTTv31
#client = mqttClient.Client(client_id="tmq_sub_client_id", userdata=None, protocol=mqttClient.MQTTv311)
#client = mqttClient.Client(client_id="tmq_sub_client_id", userdata=None, protocol=mqttClient.MQTTv31)
if paho.mqtt.__version__[0] > '1':
    client = mqttClient.Client(mqttClient.CallbackAPIVersion.VERSION2, client_id="tmq_sub_client_id3", userdata=None, protocol=mqttClient.MQTTv5)
else:
    client = mqttClient.Client(client_id="tmq_sub_client_id3", userdata=None, protocol=mqttClient.MQTTv5)
#client = mqttClient.Client()
client.on_connect = on_connect

# enable TLS for secure connection
#client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
client.username_pw_set("root", "taosdata")
client.connect("127.0.1.1", 6083)

client.on_subscribe = on_subscribe
client.on_message = on_message

#client.subscribe("topic_meters", qos=1)

client.loop_forever()
