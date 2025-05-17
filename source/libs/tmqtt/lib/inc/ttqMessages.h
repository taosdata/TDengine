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

#ifndef _TD_MESSAGES_TTQ_H_
#define _TD_MESSAGES_TTQ_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "tmqtt.h"
#include "tmqttInt.h"

void message__cleanup_all(struct tmqtt *ttq);
void message__cleanup(struct tmqtt_message_all **message);
int  message__delete(struct tmqtt *ttq, uint16_t mid, enum tmqtt_msg_direction dir, int qos);
int  message__queue(struct tmqtt *ttq, struct tmqtt_message_all *message, enum tmqtt_msg_direction dir);
void message__reconnect_reset(struct tmqtt *ttq, bool update_quota_only);
int  message__release_to_inflight(struct tmqtt *ttq, enum tmqtt_msg_direction dir);
int  message__remove(struct tmqtt *ttq, uint16_t mid, enum tmqtt_msg_direction dir, struct tmqtt_message_all **message,
                     int qos);
void message__retry_check(struct tmqtt *ttq);
int  message__out_update(struct tmqtt *ttq, uint16_t mid, enum tmqtt_msg_state state, int qos);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MESSAGES_TTQ_H_*/
