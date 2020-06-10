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
struct mqtt_client       mqttClient = {0};
pthread_t                clientDaemonThread = {0};
void*                    mqttConnect=NULL;
struct reconnect_state_t recntStatus = {0};
char*                    topicPath=NULL;
int                      mttIsRuning = 1;

int32_t mqttInitSystem() {
  int   rc = 0;
  uint8_t sendbuf[2048];
  uint8_t recvbuf[1024];
  recntStatus.sendbuf = sendbuf;
  recntStatus.sendbufsz = sizeof(sendbuf);
  recntStatus.recvbuf = recvbuf;
  recntStatus.recvbufsz = sizeof(recvbuf);
  char* url = tsMqttBrokerAddress;
  recntStatus.user_name = strstr(url, "@") != NULL ? strbetween(url, "//", ":") : NULL;
  recntStatus.password = strstr(url, "@") != NULL ? strbetween(strstr(url, recntStatus.user_name), ":", "@") : NULL;

  if (strlen(url) == 0) {
    mqttTrace("mqtt module not init, url is null");
    return rc;
  }

  if (strstr(url, "@") != NULL) {
    recntStatus.hostname = strbetween(url, "@", ":");
  } else if (strstr(strstr(url, "://") + 3, ":") != NULL) {
    recntStatus.hostname = strbetween(url, "//", ":");

  } else {
    recntStatus.hostname = strbetween(url, "//", "/");
  }

  char* _begin_hostname = strstr(url, recntStatus.hostname);
  if (strstr(_begin_hostname, ":") != NULL) {
    recntStatus.port = strbetween(_begin_hostname, ":", "/");
  } else {
    recntStatus.port = strbetween("'1883'", "'", "'");
  }

  topicPath = strbetween(strstr(url, strstr(_begin_hostname, ":") != NULL ? recntStatus.port : recntStatus.hostname),
                         "/", "/");
  char* _topic = "+/+/+/";
  int   _tpsize = strlen(topicPath) + strlen(_topic) + 1;
  recntStatus.topic = calloc(1, _tpsize);
  sprintf(recntStatus.topic, "/%s/%s", topicPath, _topic);
  recntStatus.client_id = strlen(tsMqttBrokerClientId) < 3 ? tsMqttBrokerClientId : "taos_mqtt";
  mqttConnect = NULL;
  return rc;
}

int32_t mqttStartSystem() {
  int rc = 0;
  if (recntStatus.user_name != NULL && recntStatus.password != NULL) {
    mqttPrint("connecting to  mqtt://%s:%s@%s:%s/%s/", recntStatus.user_name, recntStatus.password,
              recntStatus.hostname, recntStatus.port, topicPath);
  } else if (recntStatus.user_name != NULL && recntStatus.password == NULL) {
    mqttPrint("connecting to  mqtt://%s@%s:%s/%s/", recntStatus.user_name, recntStatus.hostname, recntStatus.port,
              topicPath);
  }

  mqtt_init_reconnect(&mqttClient, mqttReconnectClient, &recntStatus, mqtt_PublishCallback);
  if (pthread_create(&clientDaemonThread, NULL, mqttClientRefresher, &mqttClient)) {
    mqttError("Failed to start client daemon.");
    mqttCleanup(EXIT_FAILURE, -1, NULL);
    rc = -1;
  } else {
    mqttPrint("listening for '%s' messages.", recntStatus.topic);
  }
  return rc;
}

void mqttStopSystem() {
  mqttClient.error = MQTT_ERROR_SOCKET_ERROR;
  mttIsRuning = 0;
  usleep(300000U);
  mqttCleanup(EXIT_SUCCESS, mqttClient.socketfd, &clientDaemonThread);
  mqttPrint("mqtt is stoped");
}

void mqttCleanUpSystem() {
  mqttPrint("starting to clean  up mqtt");
  free(recntStatus.user_name);
  free(recntStatus.password);
  free(recntStatus.hostname);
  free(recntStatus.port);
  free(recntStatus.topic);
  free(topicPath);
  mqttPrint("mqtt is cleaned up");
}

void mqtt_PublishCallback(void** unused, struct mqtt_response_publish* published) {
  mqttPrint("mqtt_PublishCallback");
  /* note that published->topic_name is NOT null-terminated (here we'll change it to a c-string) */
  char* topic_name = (char*)malloc(published->topic_name_size + 1);
  memcpy(topic_name, published->topic_name, published->topic_name_size);
  topic_name[published->topic_name_size] = '\0';
  mqttPrint("Received publish('%s'): %s", topic_name, (const char*)published->application_message);
  char _token[128] = {0};
  char _dbname[128] = {0};
  char _tablename[128] = {0};
  if (mqttConnect == NULL) {
    mqttPrint("connect database");
    taos_connect_a(NULL, "_root", tsInternalPass, "", 0, mqttInitConnCb, &mqttClient, &mqttConnect);
  }
  if (topic_name[1]=='/' &&  strncmp((char*)&topic_name[1], topicPath, strlen(topicPath)) == 0) {
    char* p_p_cmd_part[5] = {0};
    char  copystr[1024] = {0};
    strncpy(copystr, topic_name, MIN(1024, published->topic_name_size));
    char part_index = split(copystr, "/", p_p_cmd_part, 10);
    if (part_index < 4) {
      mqttError("The topic %s is't format '/path/token/dbname/table name/'. for expmle: '/taos/token/db/t'", topic_name);
    } else {
      strncpy(_token, p_p_cmd_part[1], 127);
      strncpy(_dbname, p_p_cmd_part[2], 127);
      strncpy(_tablename, p_p_cmd_part[3], 127);
      mqttPrint("part count=%d,access token:%s,database name:%s, table name:%s", part_index, _token, _dbname,
                _tablename);

      if (mqttConnect != NULL) {
        char* _sql = converJsonToSql((char*)published->application_message, _dbname, _tablename);
        mqttPrint("query:%s", _sql);
        taos_query_a(mqttConnect, _sql, mqttQueryInsertCallback, &mqttClient);
        mqttPrint("free sql:%s", _sql);
        free(_sql);
      }
    }
  }
  free(topic_name);
}

void* mqttClientRefresher(void* client) {
  while (mttIsRuning) {
    mqtt_sync((struct mqtt_client*)client);
    usleep(100000U);
  }
  mqttPrint("Exit  mqttClientRefresher");
  return NULL;
}

void mqttCleanup(int status, int sockfd, pthread_t* client_daemon) {
  mqttPrint("mqttCleanup");
  if (sockfd != -1) close(sockfd);
  if (client_daemon != NULL) pthread_cancel(*client_daemon);
}

void mqttInitConnCb(void* param, TAOS_RES* result, int32_t code) {
  if (code < 0) {
    mqttError("mqtt:%d, connect to database failed, reason:%s", code, tstrerror(code));
    taos_close(mqttConnect);
    mqttConnect = NULL;
    return;
  }
  mqttTrace("mqtt:%d, connect to database success, reason:%s", code, tstrerror(code));
}

void mqttQueryInsertCallback(void* param, TAOS_RES* result, int32_t code) {
  if (code < 0) {
    mqttError("mqtt:%d, save data failed, code:%s", code, tstrerror(code));
  } else if (code == 0) {
    mqttError("mqtt:%d, save data failed, affect rows:%d", code, code);
  } else {
    mqttPrint("mqtt:%d, save data   success, code:%s", code, tstrerror(code));
  }
}

void mqttReconnectClient(struct mqtt_client* client, void** reconnect_state_vptr) {
  mqttPrint("mqttReconnectClient");
  struct reconnect_state_t* reconnect_state = *((struct reconnect_state_t**)reconnect_state_vptr);

  /* Close the clients socket if this isn't the initial reconnect call */
  if (client->error != MQTT_ERROR_INITIAL_RECONNECT) {
    close(client->socketfd);
  }

  /* Perform error handling here. */
  if (client->error != MQTT_ERROR_INITIAL_RECONNECT) {
    mqttError("mqttReconnectClient: called while client was in error state \"%s\"", mqtt_error_str(client->error));
  }

  /* Open a new socket. */
  int sockfd = open_nb_socket(reconnect_state->hostname, reconnect_state->port);
  if (sockfd == -1) {
    mqttError("Failed to open socket: ");
    mqttCleanup(EXIT_FAILURE, sockfd, NULL);
  }

  /* Reinitialize the client. */
  mqtt_reinit(client, sockfd, reconnect_state->sendbuf, reconnect_state->sendbufsz, reconnect_state->recvbuf,
              reconnect_state->recvbufsz);

  /* Ensure we have a clean session */
  uint8_t connect_flags = MQTT_CONNECT_CLEAN_SESSION;
  /* Send connection request to the broker. */
  mqtt_connect(client, reconnect_state->client_id, NULL, NULL, 0, reconnect_state->user_name, reconnect_state->password,connect_flags, 400);

  /* Subscribe to the topic. */
  mqtt_subscribe(client, reconnect_state->topic, 0);
}