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

#include "ttqAlias.h"
#include "ttqMemory.h"
#include "ttqPacket.h"
#include "ttqProperty.h"
#include "ttqTime.h"
#include "tthash.h"
#include "ttqUtil.h"

struct tmqtt *ttqCxtInit(ttq_sock_t sock) {
  struct tmqtt *context;
  char          address[1024];

  context = ttq_calloc(1, sizeof(struct tmqtt));
  if (!context) return NULL;

#ifdef WITH_EPOLL
  context->ident = id_client;
#endif
  tmqtt__set_state(context, ttq_cs_new);
  context->sock = sock;
  context->last_msg_in = db.now_s;
  context->next_msg_out = db.now_s + 60;
  context->keepalive = 60; /* Default to 60s */
  context->clean_start = true;
  context->id = NULL;
  context->last_mid = 0;
  context->will = NULL;
  context->username = NULL;
  context->password = NULL;
  context->listener = NULL;
  context->acl_list = NULL;
  context->retain_available = true;

  /* is_bridge records whether this client is a bridge or not. This could be
   * done by looking at context->bridge for bridges that we create ourself,
   * but incoming bridges need some other way of being recorded. */
  context->is_bridge = false;

  context->in_packet.payload = NULL;
  packet__cleanup(&context->in_packet);
  context->out_packet = NULL;
  context->current_out_packet = NULL;
  context->out_packet_count = 0;

  context->address = NULL;
  if ((int)sock >= 0) {
    if (!ttqNetSocketGetAddress(sock, address, 1024, &context->remote_port)) {
      context->address = ttq_strdup(address);
    }
    if (!context->address) {
      /* getpeername and inet_ntop failed and not a bridge */
      ttq_free(context);
      return NULL;
    }
  }
  context->bridge = NULL;
  context->msgs_in.inflight_maximum = db.config->max_inflight_messages;
  context->msgs_in.inflight_quota = db.config->max_inflight_messages;
  context->msgs_out.inflight_maximum = db.config->max_inflight_messages;
  context->msgs_out.inflight_quota = db.config->max_inflight_messages;
  // context->max_qos = 2;
  context->max_qos = 1;
#ifdef WITH_TLS
  context->ssl = NULL;
#endif

  if ((int)context->sock >= 0) {
    HASH_ADD(hh_sock, db.contexts_by_sock, sock, sizeof(context->sock), context);
  }
  return context;
}

static void ttqCxtCleanup_out_packets(struct tmqtt *context) {
  struct tmqtt__packet *packet;

  if (!context) return;

  if (context->current_out_packet) {
    packet__cleanup(context->current_out_packet);
    ttq_free(context->current_out_packet);
    context->current_out_packet = NULL;
  }
  while (context->out_packet) {
    packet__cleanup(context->out_packet);
    packet = context->out_packet;
    context->out_packet = context->out_packet->next;
    ttq_free(packet);
  }
  context->out_packet_count = 0;
}

void ttqCxtCleanup(struct tmqtt *context, bool force_free) {
  if (!context) return;

  if (force_free) {
    context->clean_start = true;
  }

  // alias__free_all(context);
  ttqCxtCleanup_out_packets(context);

  ttq_free(context->auth_method);
  context->auth_method = NULL;

  ttq_free(context->username);
  context->username = NULL;

  ttq_free(context->password);
  context->password = NULL;

  net__socket_close(context);
  if (force_free) {
    ttqSubCleanSession(context);
  }

  ttqDbMessageDelete(context, force_free);

  ttq_free(context->address);
  context->address = NULL;

  // ttqCxtSendWill(context);

  if (context->id) {
    ttqCxtRemoveFromById(context);
    ttq_free(context->id);
    context->id = NULL;
  }
  packet__cleanup(&(context->in_packet));
  ttqCxtCleanup_out_packets(context);
  tmq_ctx_cleanup(&context->tmq_context);

  if (force_free) {
    ttq_free(context);
  }
}
/*
void ttqCxtSendWill(struct tmqtt *ctxt) {
  if (ctxt->state != ttq_cs_disconnecting && ctxt->will) {
    if (ctxt->will_delay_interval > 0) {
      will_delay__add(ctxt);
      return;
    }

    if (tmqttAclCheck(ctxt, ctxt->will->msg.topic, (uint32_t)ctxt->will->msg.payloadlen, ctxt->will->msg.payload,
                        (uint8_t)ctxt->will->msg.qos, ctxt->will->msg.retain, TTQ_ACL_WRITE) == TTQ_ERR_SUCCESS) {

ttqDbMessageEasyQueue(ctxt, ctxt->will->msg.topic, (uint8_t)ctxt->will->msg.qos, (uint32_t)ctxt->will->msg.payloadlen,
                        ctxt->will->msg.payload, ctxt->will->msg.retain, ctxt->will->expiry_interval,
                        &ctxt->will->properties);
}
}
will__clear(ctxt);
}
*/
void ttqCxtDisconnect(struct tmqtt *context) {
  if (tmqtt__get_state(context) == ttq_cs_disconnected) {
    return;
  }

  // plugin__handle_disconnect(context, -1);

  // ttqCxtSendWill(context);
  net__socket_close(context);
  {
    if (context->session_expiry_interval == 0) {
      /* Client session is due to be expired now */
      if (context->will_delay_interval == 0) {
        /* This will be done later, after the will is published for delay>0. */
        ttqCxtAddToDisused(context);
      }
    } else {
      ttqSessionExpiryAdd(context);
    }
  }
  ttqKeepaliveRemove(context);
  tmqtt__set_state(context, ttq_cs_disconnected);
}

void ttqCxtAddToDisused(struct tmqtt *context) {
  if (context->state == ttq_cs_disused) return;

  tmqtt__set_state(context, ttq_cs_disused);

  if (context->id) {
    ttqCxtRemoveFromById(context);
    ttq_free(context->id);
    context->id = NULL;
  }

  context->for_free_next = db.ll_for_free;
  db.ll_for_free = context;
}

void ttqCxtFreeDisused(void) {
  struct tmqtt *context, *next;

  context = db.ll_for_free;
  db.ll_for_free = NULL;
  while (context) {
    next = context->for_free_next;
    ttqCxtCleanup(context, true);
    context = next;
  }
}

void ttqCxtAddToById(struct tmqtt *context) {
  if (context->in_by_id == false) {
    context->in_by_id = true;
    HASH_ADD_KEYPTR(hh_id, db.contexts_by_id, context->id, strlen(context->id), context);
  }
}

void ttqCxtRemoveFromById(struct tmqtt *context) {
  struct tmqtt *context_found;

  if (context->in_by_id == true && context->id) {
    HASH_FIND(hh_id, db.contexts_by_id, context->id, strlen(context->id), context_found);
    if (context_found) {
      HASH_DELETE(hh_id, db.contexts_by_id, context_found);
    }
    context->in_by_id = false;
  }
}
