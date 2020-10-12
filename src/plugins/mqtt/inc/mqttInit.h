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

#ifndef TDENGINE_MQTT_INIT_H
#define TDENGINE_MQTT_INIT_H
#ifdef __cplusplus
extern "C" {
#endif

/**
 * @file
 * A simple subscriber program that performs automatic reconnections.
 */
#include "mqtt.h"

#define QOS                1
#define TIMEOUT            10000L
#define MQTT_SEND_BUF_SIZE 102400
#define MQTT_RECV_BUF_SIZE 102400

/**
 * @brief A structure that I will use to keep track of some data needed
 *        to setup the connection to the broker.
 *
 * An instance of this struct will be created in my \c main(). Then, whenever
 * \ref mqttReconnectClient is called, this instance will be passed.
 */
typedef struct SMqttReconnectState {
  uint8_t* sendbuf;
  size_t   sendbufsz;
  uint8_t* recvbuf;
  size_t   recvbufsz;
} SMqttReconnectState;

/**
 * @brief My reconnect callback. It will reestablish the connection whenever
 *        an error occurs.
 */
void mqttReconnectClient(struct mqtt_client* client, void** reconnect_state_vptr);

/**
 * @brief The function will be called whenever a PUBLISH message is received.
 */
void mqttPublishCallback(void** unused, struct mqtt_response_publish* published);

/**
 * @brief The client's refresher. This function triggers back-end routines to
 *        handle ingress/egress traffic to the broker.
 *
 * @note All this function needs to do is call \ref __mqtt_recv and
 *       \ref __mqtt_send every so often. I've picked 100 ms meaning that
 *       client ingress/egress traffic will be handled every 100 ms.
 */
void* mqttClientRefresher(void* client);

/**
 * @brief Safelty closes the \p sockfd and cancels the \p client_daemon before \c exit.
 */
void mqttCleanupRes(int status, int sockfd, pthread_t* client_daemon);

#ifdef __cplusplus
}
#endif

#endif
