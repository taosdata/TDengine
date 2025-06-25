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

#ifndef _TD_TMQTT_BROKER_H_
#define _TD_TMQTT_BROKER_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

struct tmqtt;
typedef struct mqtt5__property tmqtt_property;

enum tmqttProtocol { mp_mqtt, mp_mqttsn, mp_websockets };

// Memory allocation.
void *tmqttCalloc(size_t nmemb, size_t size);
void  tmqttFree(void *mem);
void *tmqttMalloc(size_t size);
void *tmqttRealloc(void *ptr, size_t size);
char *tmqttStrdup(const char *s);

// Utility Functions
void tmqttLog(int level, const char *fmt, ...);

// Client Functions
const char *tmqttClientAddress(const struct tmqtt *client);
bool        tmqttClientCleanSession(const struct tmqtt *client);
const char *tmqttClientId(const struct tmqtt *client);
int         tmqttClientKeepalive(const struct tmqtt *client);
void       *tmqttClientCertificate(const struct tmqtt *client);
int         tmqttClientProtocol(const struct tmqtt *client);
int         tmqttClientProtocol_version(const struct tmqtt *client);
int         tmqttClientSubCount(const struct tmqtt *client);
const char *tmqttClientUsername(const struct tmqtt *client);
int         tmqttSetUsername(struct tmqtt *client, const char *username);

// Client control
int tmqttKickClientByClientid(const char *clientid, bool with_will);
int tmqttKickClientByUsername(const char *username, bool with_will);

// Publishing functions
int tmqttBrokerPublish(const char *clientid, const char *topic, int payloadlen, void *payload, int qos, bool retain,
                         tmqtt_property *properties);

int tmqttBrokerPublishCopy(const char *clientid, const char *topic, int payloadlen, const void *payload, int qos,
                              bool retain, tmqtt_property *properties);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TMQTT_BROKER_H_*/
