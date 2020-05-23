/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "mqttSystem.h"
#include "mqtt.h"
#include "mqttLog.h"
#include "os.h"
#include "taos.h"
#include "tglobal.h"
#include "tsocket.h"
#include "ttimer.h"
#include "MQTTAsync.h"
#include "tsclient.h"
#define ADDRESS "tcp://mqtt.eclipse.org:1883"
#define CLIENTID "ExampleClientSub"
#define TOPIC "MQTT Examples"
#define PAYLOAD "Hello World!"
#define QOS 1
#define TIMEOUT 10000L

MQTTAsync                   client;
MQTTAsync_connectOptions    conn_opts = MQTTAsync_connectOptions_initializer;
MQTTAsync_disconnectOptions disc_opts = MQTTAsync_disconnectOptions_initializer;
void*                       mqtt_conn=NULL;
int disc_finished = 0;
int subscribed = 0;
int finished = 0;
int can_exit = 0;

void connlost(void* context, char* cause) {
  MQTTAsync                client = (MQTTAsync)context;
  MQTTAsync_connectOptions conn_opts = MQTTAsync_connectOptions_initializer;
  int                      rc;

  mqttError("\nConnection lost\n");
  if (cause) mqttError("     cause: %s\n", cause);

  mqttPrint("Reconnecting\n");
  conn_opts.keepAliveInterval = 20;
  conn_opts.cleansession = 1;
  if ((rc = MQTTAsync_connect(client, &conn_opts)) != MQTTASYNC_SUCCESS) {
    mqttError("Failed to start connect, return code %d\n", rc);
    finished = 1;
  }
}

int msgarrvd(void* context, char* topicName, int topicLen, MQTTAsync_message* message) {
  mqttPrint("Message arrived\n");
  mqttPrint("     topic: %s\n", topicName);
  mqttPrint("   message: %.*s\n", message->payloadlen, (char*)message->payload);
  if (mqtt_conn == NULL) {
    taos_connect_a(NULL, "monitor", tsInternalPass, "", 0, mqttInitConnCb, &client, &mqtt_conn);
  }
  if (mqtt_conn != NULL) {
    taos_query_a(mqtt_conn, (char*)message->payload, mqtt_query_insert_callback, &client);
  }
  MQTTAsync_freeMessage(&message);
  MQTTAsync_free(topicName);
  return 1;
}
 void mqtt_query_insert_callback(void* param, TAOS_RES* result, int32_t code) {
  if (code < 0) {
     mqttError("mqtt:%p, save data failed, code:%s", mqtt_conn, tstrerror(code));
  } else if (code == 0) {
    mqttError("mqtt:%p, save data failed, affect rows:%d", mqtt_conn, code);
  } else {
    mqttError("mqtt:%p, save data   success, code:%s", mqtt_conn, tstrerror(code));
  }
}

void onDisconnectFailure(void* context, MQTTAsync_failureData* response) {
  mqttError("Disconnect failed, rc %d\n", response->code);
  disc_finished = 1;
}

void onDisconnect(void* context, MQTTAsync_successData* response) {
  mqttError("Successful disconnection\n");
  if (mqtt_conn != NULL) {
    taos_close(&(mqtt_conn));
    mqtt_conn = NULL;
  }
  disc_finished = 1;
}

void onSubscribe(void* context, MQTTAsync_successData* response) {
  mqttPrint("Subscribe succeeded\n");
  subscribed = 1;
 
}

void onSubscribeFailure(void* context, MQTTAsync_failureData* response) {
  mqttError("Subscribe failed, rc %d\n", response->code);
  finished = 1;
  if (mqtt_conn != NULL) {
    taos_close(mqtt_conn);
    mqtt_conn = NULL;
  }
}

void onConnectFailure(void* context, MQTTAsync_failureData* response) {
  mqttError("Connect failed, rc %d\n", response->code);
  finished = 1;
}

void onConnect(void* context, MQTTAsync_successData* response) {
  MQTTAsync                 client = (MQTTAsync)context;
  MQTTAsync_responseOptions opts = MQTTAsync_responseOptions_initializer;
  int                       rc;

  mqttPrint("Successful connection\n");

  mqttPrint("Subscribing to topic %s\nfor client %s using QoS%d\n\n",TOPIC, CLIENTID, QOS);
  opts.onSuccess = onSubscribe;
  opts.onFailure = onSubscribeFailure;
  opts.context = client;
  if ((rc = MQTTAsync_subscribe(client, TOPIC, QOS, &opts)) != MQTTASYNC_SUCCESS) {
    mqttError("Failed to start subscribe, return code %d\n", rc);
    finished = 1;
  }
}



int32_t mqttGetReqCount() { return 0; }
int32_t mqttInitSystem() {
  int rc = 0;
  if ((rc = MQTTAsync_create(&client, ADDRESS, CLIENTID, MQTTCLIENT_PERSISTENCE_NONE, NULL)) != MQTTASYNC_SUCCESS) {
    mqttError("Failed to create client, return code %d\n", rc);
    rc = EXIT_FAILURE;

  } else {
    if ((rc = MQTTAsync_setCallbacks(client, client, connlost, msgarrvd, NULL)) != MQTTASYNC_SUCCESS) {
      mqttError("Failed to set callbacks, return code %d\n", rc);
      rc = EXIT_FAILURE;
    } else {
      conn_opts.keepAliveInterval = 20;
      conn_opts.cleansession = 1;
      conn_opts.onSuccess = onConnect;
      conn_opts.onFailure = onConnectFailure;
      conn_opts.context = client;
      taos_init();
    }
  }
  return rc;
}

int32_t mqttStartSystem() {
  int rc = 0;
  mqttPrint("mqttStartSystem");
  if ((rc = MQTTAsync_connect(client, &conn_opts)) != MQTTASYNC_SUCCESS) {
    mqttError("Failed to start connect, return code %d\n", rc);
    rc = EXIT_FAILURE;
  } else {
    while (!subscribed && !finished) usleep(10000L);
    disc_opts.onSuccess = onDisconnect;
    disc_opts.onFailure = onDisconnectFailure;
    mqttPrint("Successful started\n");
 
  }
  return rc;
}


  void mqttInitConnCb(void* param, TAOS_RES* result, int32_t code) {
  if (code < 0) {
      mqttError("mqtt:%d, connect to database failed, reason:%s", code, tstrerror(code));
    taos_close(mqtt_conn);
    mqtt_conn = NULL;
    return;
  }
  mqttTrace("mqtt:%d, connect to database success, reason:%s", code, tstrerror(code));
  }


void mqttStopSystem() {
  int rc=0;
  if ((rc = MQTTAsync_disconnect(client, &disc_opts)) != MQTTASYNC_SUCCESS) {
    mqttError("Failed to start disconnect, return code %d\n", rc);
    rc = EXIT_FAILURE;
  } else {
    while (!disc_finished) {
      usleep(10000L);
    }
  }
  taos_close(mqtt_conn);
}

void mqttCleanUpSystem() {
  mqttPrint("mqttCleanUpSystem");
  MQTTAsync_destroy(&client);
  taos_cleanup(mqtt_conn);
}
