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
