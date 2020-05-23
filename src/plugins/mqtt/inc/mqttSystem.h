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

#ifndef TDENGINE_MQTT_SYSTEM_H
#define TDENGINE_MQTT_SYSTEM_H
#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include "MQTTAsync.h"
#include "os.h"
#include "taos.h"
#include "tglobal.h"
#include "tsocket.h"
#include "ttimer.h"
#include "tsclient.h"
int32_t mqttInitSystem();
int32_t mqttStartSystem();
void    mqttStopSystem();
void    mqttCleanUpSystem();
char    split(char str[], char delims[], char** p_p_cmd_part, int max);
void    connlost(void* context, char* cause);
int     msgarrvd(void* context, char* topicName, int topicLen, MQTTAsync_message* message);
void    mqtt_query_insert_callback(void* param, TAOS_RES* result, int32_t code);
void    onDisconnectFailure(void* context, MQTTAsync_failureData* response);
void    onDisconnect(void* context, MQTTAsync_successData* response);
void    onSubscribe(void* context, MQTTAsync_successData* response);
void    onSubscribeFailure(void* context, MQTTAsync_failureData* response);
void    mqttInitConnCb(void* param, TAOS_RES* result, int32_t code);
#ifdef __cplusplus
}
#endif

#endif
