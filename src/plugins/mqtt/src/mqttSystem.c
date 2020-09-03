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

#include "cJSON.h"
#include "mqtt.h"
#include "mqttInit.h"
#include "mqttLog.h"
#include "mqttPayload.h"
#include "os.h"
#include "posix_sockets.h"
#include "string.h"
#include "taos.h"
#include "tglobal.h"
#include "tmqtt.h"
#include "tsclient.h"
#include "tsocket.h"
#include "ttimer.h"
#include "mqttSystem.h"

#define MQTT_SEND_BUF_SIZE 102400
#define MQTT_RECV_BUF_SIZE 102400

struct mqtt_client       tsMqttClient = {0};
struct reconnect_state_t tsMqttStatus = {0};
static pthread_t         tsMqttClientDaemonThread = {0};
static void*             tsMqttConnect = NULL;
static bool              mqttIsRuning = false;

void  mqttPublishCallback(void** unused, struct mqtt_response_publish* published);
void  mqttCleanupRes(int status, int sockfd, pthread_t* client_daemon);
void* mqttClientRefresher(void* client);

int32_t mqttInitSystem() { return 0; }

int32_t mqttStartSystem() {
  tsMqttStatus.sendbufsz = MQTT_SEND_BUF_SIZE;
  tsMqttStatus.recvbufsz = MQTT_RECV_BUF_SIZE;
  tsMqttStatus.sendbuf = malloc(MQTT_SEND_BUF_SIZE);
  tsMqttStatus.recvbuf = malloc(MQTT_RECV_BUF_SIZE);
  mqttIsRuning = true;

  mqtt_init_reconnect(&tsMqttClient, mqttReconnectClient, &tsMqttStatus, mqttPublishCallback);
  if (pthread_create(&tsMqttClientDaemonThread, NULL, mqttClientRefresher, &tsMqttClient)) {
    mqttError("mqtt client failed to start daemon.");
    mqttCleanupRes(EXIT_FAILURE, -1, NULL);
    return -1;
  }

  mqttInfo("mqtt client listening for %s messages", tsMqttTopic);
  return 0;
}

void mqttStopSystem() {
  if (mqttIsRuning) {
    mqttIsRuning = false;
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
  /* note that published->topic_name is NOT null-terminated (here we'll change it to a c-string) */
  char* topic_name = (char*)malloc(published->topic_name_size + 1);
  memcpy(topic_name, published->topic_name, published->topic_name_size);
  topic_name[published->topic_name_size] = '\0';
  mqttInfo("received publish('%s'): %s", topic_name, (const char*)published->application_message);
  char _token[128] = {0};
  char _dbname[128] = {0};
  char _tablename[128] = {0};
  if (tsMqttConnect == NULL) {
    mqttInfo("connect database");
    taos_connect_a(NULL, "_root", tsInternalPass, "", 0, mqttInitConnCb, &tsMqttClient, &tsMqttConnect);
  }
  if (topic_name[1] == '/' && strncmp((char*)&topic_name[1], tsMqttTopic, strlen(tsMqttTopic)) == 0) {
    char* p_p_cmd_part[5] = {0};
    char  copystr[1024] = {0};
    strncpy(copystr, topic_name, MIN(1024, published->topic_name_size));
    char part_index = split(copystr, "/", p_p_cmd_part, 10);
    if (part_index < 4) {
      mqttError("The topic %s is't format '/path/token/dbname/table name/'. for expmle: '/taos/token/db/t'",
                topic_name);
    } else {
      strncpy(_token, p_p_cmd_part[1], 127);
      strncpy(_dbname, p_p_cmd_part[2], 127);
      strncpy(_tablename, p_p_cmd_part[3], 127);
      mqttInfo("part count=%d,access token:%s,database name:%s, table name:%s", part_index, _token, _dbname,
               _tablename);

      if (tsMqttConnect != NULL) {
        char* _sql = converJsonToSql((char*)published->application_message, _dbname, _tablename);
        mqttInfo("query:%s", _sql);
        taos_query_a(tsMqttConnect, _sql, mqttQueryInsertCallback, &tsMqttClient);
        mqttInfo("free sql:%s", _sql);
        free(_sql);
      }
    }
  }
  free(topic_name);
}

void* mqttClientRefresher(void* client) {
  while (mqttIsRuning) {
    mqtt_sync((struct mqtt_client*)client);
    taosMsleep(100);
  }

  mqttDebug("mqtt client quit refresher");
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

void mqttInitConnCb(void* param, TAOS_RES* result, int32_t code) {
  if (code < 0) {
    mqttError("mqtt:%d, connect to database failed, reason:%s", code, tstrerror(code));
    taos_close(tsMqttConnect);
    tsMqttConnect = NULL;
    return;
  }
  mqttDebug("mqtt:%d, connect to database success, reason:%s", code, tstrerror(code));
}

void mqttQueryInsertCallback(void* param, TAOS_RES* result, int32_t code) {
  if (code < 0) {
    mqttError("mqtt:%d, save data failed, code:%s", code, tstrerror(code));
  } else if (code == 0) {
    mqttError("mqtt:%d, save data failed, affect rows:%d", code, code);
  } else {
    mqttInfo("mqtt:%d, save data   success, code:%s", code, tstrerror(code));
  }
}

void mqttReconnectClient(struct mqtt_client* client, void** unused) {
  mqttInfo("mqtt client tries to connect to the server");

  if (client->error != MQTT_ERROR_INITIAL_RECONNECT) {
    close(client->socketfd);
  }

  if (client->error != MQTT_ERROR_INITIAL_RECONNECT) {
    mqttError("mqtt client was in error state %s", mqtt_error_str(client->error));
  }

  int sockfd = open_nb_socket("test.mosquitto.org", "1883");
  if (sockfd < 0) {
    mqttError("mqtt client failed to open socket %s:%s", tsMqttHostName, tsMqttPort);
    mqttCleanupRes(EXIT_FAILURE, sockfd, NULL);
  }

  // mqtt_reinit(client, sockfd, tsMqttStatus.sendbuf, tsMqttStatus.sendbufsz, tsMqttStatus.recvbuf, tsMqttStatus.recvbufsz);
  // mqtt_connect(client, tsMqttClientId, NULL, NULL, 0, tsMqttUser, tsMqttPass, MQTT_CONNECT_CLEAN_SESSION, 400);
  // mqtt_subscribe(client, tsMqttTopic, 0);

  mqtt_reinit(client, sockfd, tsMqttStatus.sendbuf, tsMqttStatus.sendbufsz, tsMqttStatus.recvbuf, tsMqttStatus.recvbufsz);
  mqtt_connect(client, tsMqttClientId, NULL, NULL, 0, NULL, NULL, MQTT_CONNECT_CLEAN_SESSION, 400);
  mqtt_subscribe(client, "datetime", 0);
}