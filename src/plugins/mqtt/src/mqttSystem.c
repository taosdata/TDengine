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
struct mqtt_client       client;
pthread_t                client_daemon;
void*                    mqtt_conn;
struct reconnect_state_t reconnect_state;

int32_t mqttInitSystem() {
  int         rc = 0;
  const char* addr;
  const char* port;
  addr = tsMqttBrokerAddress;
  port = "1883";
  reconnect_state.hostname = addr;
  reconnect_state.port = port;
  reconnect_state.topic = TOPIC;
  uint8_t sendbuf[2048];
  uint8_t recvbuf[1024];
  reconnect_state.sendbuf = sendbuf;
  reconnect_state.sendbufsz = sizeof(sendbuf);
  reconnect_state.recvbuf = recvbuf;
  reconnect_state.recvbufsz = sizeof(recvbuf);
  taos_init();
  mqttPrint("mqttInitSystem %s", tsMqttBrokerAddress);
  return rc;
}

int32_t mqttStartSystem() {
  int rc = 0;
  mqtt_conn = NULL;
  mqtt_init_reconnect(&client, mqttReconnectClient, &reconnect_state, mqtt_PublishCallback);
  if (pthread_create(&client_daemon, NULL, mqttClientRefresher, &client)) {
    mqttError("Failed to start client daemon.");
    mqttCleanup(EXIT_FAILURE, -1, NULL);
    rc = -1;
  }
  mqttPrint("listening for '%s' messages.", TOPIC);
  return rc;
}

void mqttStopSystem() {
  mqttError("Injecting error: \"MQTT_ERROR_SOCKET_ERROR\"");
  client.error = MQTT_ERROR_SOCKET_ERROR;
}

void mqttCleanUpSystem() {
  mqttPrint("mqttCleanUpSystem");
  mqttCleanup(EXIT_SUCCESS, client.socketfd, &client_daemon);
  taos_cleanup(mqtt_conn);
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
  if (mqtt_conn == NULL) {
    mqttPrint("connect database");
    taos_connect_a(NULL, "monitor", tsInternalPass, "", 0, mqttInitConnCb, &client, &mqtt_conn);
  }
  if (strncmp(topic_name, "/taos/", 6) == 0) {
    char* p_p_cmd_part[5] = {0};
    char  copystr[1024] = {0};
    strncpy(copystr, topic_name, MIN(1024, published->topic_name_size));
    char part_index = split(copystr, "/", p_p_cmd_part, 10);
    if (part_index < 4) {
      mqttError("The topic %s is't format '%s'.", topic_name, TOPIC);
    } else {
      strncpy(_token, p_p_cmd_part[1], 127);
      strncpy(_dbname, p_p_cmd_part[2], 127);
      strncpy(_tablename, p_p_cmd_part[3], 127);
      mqttPrint("part count=%d,access token:%s,database name:%s, table name:%s", part_index, _token, _dbname,
                _tablename);

      if (mqtt_conn != NULL) {
        char* _sql = converJsonToSql((char*)published->application_message, _dbname, _tablename);
        mqttPrint("query:%s", _sql);
        taos_query_a(mqtt_conn, _sql, mqttQueryInsertCallback, &client);
        mqttPrint("free sql:%s", _sql);
        free(_sql);
      }
    }
  }
  free(topic_name);
}

void* mqttClientRefresher(void* client) {
  while (1) {
    mqtt_sync((struct mqtt_client*)client);
    usleep(100000U);
  }
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
    taos_close(mqtt_conn);
    mqtt_conn = NULL;
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

  /* Create an anonymous session */
  const char* client_id = NULL;
  /* Ensure we have a clean session */
  uint8_t connect_flags = MQTT_CONNECT_CLEAN_SESSION;
  /* Send connection request to the broker. */
  mqtt_connect(client, client_id, NULL, NULL, 0, NULL, NULL, connect_flags, 400);

  /* Subscribe to the topic. */
  mqtt_subscribe(client, reconnect_state->topic, 0);
}