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

enum tmqtt_protocol { mp_mqtt, mp_mqttsn, mp_websockets };

// Memory allocation.
void *tmqtt_calloc(size_t nmemb, size_t size);
void  tmqtt_free(void *mem);
void *tmqtt_malloc(size_t size);
void *tmqtt_realloc(void *ptr, size_t size);
char *tmqtt_strdup(const char *s);

// Utility Functions
void tmqtt_log_printf(int level, const char *fmt, ...);

// Client Functions
const char *tmqtt_client_address(const struct tmqtt *client);
bool        tmqtt_client_clean_session(const struct tmqtt *client);
const char *tmqtt_client_id(const struct tmqtt *client);
int         tmqtt_client_keepalive(const struct tmqtt *client);
void       *tmqtt_client_certificate(const struct tmqtt *client);
int         tmqtt_client_protocol(const struct tmqtt *client);
int         tmqtt_client_protocol_version(const struct tmqtt *client);
int         tmqtt_client_sub_count(const struct tmqtt *client);
const char *tmqtt_client_username(const struct tmqtt *client);
int         tmqtt_set_username(struct tmqtt *client, const char *username);

// Client control
int tmqtt_kick_client_by_clientid(const char *clientid, bool with_will);
int tmqtt_kick_client_by_username(const char *username, bool with_will);

// Publishing functions
int tmqtt_broker_publish(const char *clientid, const char *topic, int payloadlen, void *payload, int qos, bool retain,
                         tmqtt_property *properties);

int tmqtt_broker_publish_copy(const char *clientid, const char *topic, int payloadlen, const void *payload, int qos,
                              bool retain, tmqtt_property *properties);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TMQTT_BROKER_H_*/
