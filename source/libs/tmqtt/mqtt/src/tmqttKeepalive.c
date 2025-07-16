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

#include "tmqttBrokerInt.h"

#include <time.h>

static time_t last_keepalive_check = 0;

/* FIXME - this is the prototype for the future tree/trie based keepalive check implementation. */
int ttqKeepaliveAdd(struct tmqtt *context) {
  UNUSED(context);

  return TTQ_ERR_SUCCESS;
}

void ttqKeepaliveCheck(void) {
  struct tmqtt *context, *ctxt_tmp;

  if (last_keepalive_check + 5 < db.now_s) {
    last_keepalive_check = db.now_s;

    /* FIXME - this needs replacing with something more efficient */
    HASH_ITER(hh_sock, db.contexts_by_sock, context, ctxt_tmp) {
      if (context->sock != INVALID_SOCKET) {
        /* Local bridges never time out in this fashion. */
        if (!(context->keepalive) || context->bridge ||
            db.now_s - context->last_msg_in <= (time_t)(context->keepalive) * 3 / 2) {
        } else {
          /* Client has exceeded keepalive*1.5 */
          ttqDisconnect(context, TTQ_ERR_KEEPALIVE);
        }
      }
    }
  }
}

int ttqKeepaliveRemove(struct tmqtt *context) {
  UNUSED(context);

  return TTQ_ERR_SUCCESS;
}

void ttqKeepaliveRemoveAll(void) {}

int ttqKeepaliveUpdate(struct tmqtt *context) {
  context->last_msg_in = db.now_s;
  return TTQ_ERR_SUCCESS;
}
