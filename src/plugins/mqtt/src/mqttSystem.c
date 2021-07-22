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
#include "os.h"
#include "mqtt.h"
#include "mqttInit.h"
#include "mqttLog.h"
#include "mqttPayload.h"
#include "tmqtt.h"
#include "posix_sockets.h"
#include "taos.h"
#include "tglobal.h"
#include "taoserror.h"

struct SMqttReconnectState tsMqttStatus = {0};
struct mqtt_client tsMqttClient = {0};
static pthread_t   tsMqttClientDaemonThread = {0};
static void*       tsMqttConnect = NULL;
static bool        tsMqttIsRuning = false;

int32_t mqttInitSystem() { return 0; }

int32_t mqttStartSystem() {
  tsMqttStatus.sendbufsz = MQTT_SEND_BUF_SIZE;
  tsMqttStatus.recvbufsz = MQTT_RECV_BUF_SIZE;
  tsMqttStatus.sendbuf = malloc(MQTT_SEND_BUF_SIZE);
  tsMqttStatus.recvbuf = malloc(MQTT_RECV_BUF_SIZE);
  tsMqttIsRuning = true;

  mqtt_init_reconnect(&tsMqttClient, mqttReconnectClient, &tsMqttStatus, mqttPublishCallback);
  if (pthread_create(&tsMqttClientDaemonThread, NULL, mqttClientRefresher, &tsMqttClient)) {
    mqttError("mqtt failed to start daemon.");
    mqttCleanupRes(EXIT_FAILURE, -1, NULL);
    return -1;
  }

  mqttInfo("mqtt listening for topic:%s messages", tsMqttTopic);
  return 0;
}

void mqttStopSystem() {
  if (tsMqttIsRuning) {
    tsMqttIsRuning = false;
    tsMqttClient.error = MQTT_ERROR_SOCKET_ERROR;

    taosMsleep(300);
    mqttCleanupRes(EXIT_SUCCESS, tsMqttClient.socketfd, &tsMqttClientDaemonThread);

    mqttInfo("mqtt is stopped");
  }
}

void mqttCleanUpSystem() {
  mqttStopSystem();
  mqttInfo("mqtt is cleaned up");
}

void mqttPublishCallback(void** unused, struct mqtt_response_publish* published) {
  const char* content = published->application_message;
  mqttDebug("receive mqtt message, size:%d", (int)published->application_message_size);

  if (tsMqttConnect == NULL) {
    tsMqttConnect = taos_connect(NULL, "_root", tsInternalPass, "", 0);
    if (tsMqttConnect == NULL) {
      mqttError("failed to connect to tdengine, reason:%s", tstrerror(terrno));
      return;
    } else {
      mqttInfo("successfully connected to the tdengine");
    }
  }

  mqttTrace("receive mqtt message, content:%s", content);

  char* sql = mqttConverJsonToSql((char*)content, (int)published->application_message_size);
  if (sql != NULL) {
    void* res = taos_query(tsMqttConnect, sql);
    int   code = taos_errno(res);
    if (code != 0) {
      mqttError("failed to exec sql, reason:%s sql:%s", tstrerror(code), sql);
    } else {
      mqttTrace("successfully to exec sql:%s", sql);
    }
    taos_free_result(res);
  } else {
    mqttError("failed to parse mqtt message");
  }
}

void* mqttClientRefresher(void* client) {
  setThreadName("mqttCliRefresh");

  while (tsMqttIsRuning) {
    mqtt_sync((struct mqtt_client*)client);
    taosMsleep(100);
  }

  mqttDebug("mqtt quit refresher");
  return NULL;
}

void mqttCleanupRes(int status, int sockfd, pthread_t* client_daemon) {
  mqttInfo("clean up mqtt module");
  if (sockfd != -1) {
    close(sockfd);
  }

  if (client_daemon != NULL) {
    pthread_cancel(*client_daemon);
  }
}

void mqttReconnectClient(struct mqtt_client* client, void** unused) {
  mqttInfo("mqtt tries to connect to the mqtt server");

  if (client->error != MQTT_ERROR_INITIAL_RECONNECT) {
    close(client->socketfd);
  }

  if (client->error != MQTT_ERROR_INITIAL_RECONNECT) {
    mqttError("mqtt client was in error state %s", mqtt_error_str(client->error));
  }

  int sockfd = open_nb_socket(tsMqttHostName, tsMqttPort);
  if (sockfd < 0) {
    mqttError("mqtt client failed to open socket %s:%s", tsMqttHostName, tsMqttPort);
    //mqttCleanupRes(EXIT_FAILURE, sockfd, NULL);
    return;
  }

  mqtt_reinit(client, sockfd, tsMqttStatus.sendbuf, tsMqttStatus.sendbufsz, tsMqttStatus.recvbuf, tsMqttStatus.recvbufsz);
  mqtt_connect(client, tsMqttClientId, NULL, NULL, 0, tsMqttUser, tsMqttPass, MQTT_CONNECT_CLEAN_SESSION, 400);
  mqtt_subscribe(client, tsMqttTopic, 0);
}
