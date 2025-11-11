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

#ifndef _TD_UTIL_TTQ_H_
#define _TD_UTIL_TTQ_H_

#ifdef __cplusplus
extern "C" {
#endif

#include <stdio.h>

#include "ttqTls.h"
#include "tmqttInt.h"
#include "ttq.h"
#ifdef WITH_BROKER
#include "tmqttBrokerInt.h"
#endif

int      tmqtt__check_keepalive(struct tmqtt *ttq);
uint16_t tmqtt__mid_generate(struct tmqtt *ttq);

int                     tmqtt__set_state(struct tmqtt *ttq, enum tmqtt_client_state state);
enum tmqtt_client_state tmqtt__get_state(struct tmqtt *ttq);
#ifndef WITH_BROKER
void tmqtt__set_request_disconnect(struct tmqtt *ttq, bool request_disconnect);
bool tmqtt__get_request_disconnect(struct tmqtt *ttq);
#endif

#ifdef WITH_TLS
int tmqtt__hex2bin_sha1(const char *hex, unsigned char **bin);
int tmqtt__hex2bin(const char *hex, unsigned char *bin, int bin_max_len);
#endif

int util__random_bytes(void *bytes, int count);

void util__increment_receive_quota(struct tmqtt *ttq);
void util__increment_send_quota(struct tmqtt *ttq);
void util__decrement_receive_quota(struct tmqtt *ttq);
void util__decrement_send_quota(struct tmqtt *ttq);

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_TTQ_H_*/
