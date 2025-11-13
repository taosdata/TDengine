#include "tmqttBrokerInt.h"

#define _GNU_SOURCE

#include <sys/socket.h>
#include <unistd.h>

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <ttlist.h>

#include "tmqttProto.h"
#include "ttqMemory.h"
#include "ttqPacket.h"
#include "ttqSend.h"
#include "ttqSystree.h"
#include "ttqTime.h"
#include "ttqUtil.h"

#ifdef WITH_PERSISTENCE
extern bool flag_db_backup;
#endif
extern bool flag_reload;
extern bool flag_tree_print;
extern int  run;

pid_t g_ppid;

static void ttq_ppid_init(void) { g_ppid = getppid(); }

static int ttq_ppid_changed(void) {
  extern pid_t g_ppid;
  if (getppid() != g_ppid) {
    return 1;
  }

  return 0;
}

static int single_publish(struct tmqtt *context, struct tmqtt_message_v5 *msg, uint32_t message_expiry) {
  struct tmqtt_msg_store *stored;
  uint16_t                mid;

  stored = ttq_calloc(1, sizeof(struct tmqtt_msg_store));
  if (stored == NULL) return TTQ_ERR_NOMEM;

  stored->topic = msg->topic;
  msg->topic = NULL;
  stored->retain = 0;
  stored->payloadlen = (uint32_t)msg->payloadlen;
  stored->payload = ttq_malloc(stored->payloadlen + 1);
  if (stored->payload == NULL) {
    ttqDbMsgStoreFree(stored);
    return TTQ_ERR_NOMEM;
  }
  /* Ensure payload is always zero terminated, this is the reason for the extra byte above */
  ((uint8_t *)stored->payload)[stored->payloadlen] = 0;
  memcpy(stored->payload, msg->payload, stored->payloadlen);

  if (msg->properties) {
    stored->properties = msg->properties;
    msg->properties = NULL;
  }

  if (ttqDbMessageStore(context, stored, message_expiry, 0, ttq_mo_broker)) return 1;

  if (msg->qos) {
    mid = tmqtt__mid_generate(context);
  } else {
    mid = 0;
  }
  return ttqDbMessageInsert(context, mid, ttq_md_out, (uint8_t)msg->qos, 0, stored, msg->properties, true);
}

static void read_message_expiry_interval(tmqtt_property **proplist, uint32_t *message_expiry) {
  tmqtt_property *p, *previous = NULL;

  *message_expiry = 0;

  if (!proplist) return;

  p = *proplist;
  while (p) {
    if (p->identifier == MQTT_PROP_MESSAGE_EXPIRY_INTERVAL) {
      *message_expiry = p->value.i32;
      if (p == *proplist) {
        *proplist = p->next;
      } else {
        previous->next = p->next;
      }
      property__free(&p);
      return;
    }
    previous = p;
    p = p->next;
  }
}

static void queue_plugin_msgs(void) {
  struct tmqtt_message_v5 *msg, *tmp;
  struct tmqtt            *context;
  uint32_t                 message_expiry;

  DL_FOREACH_SAFE(db.plugin_msgs, msg, tmp) {
    DL_DELETE(db.plugin_msgs, msg);

    read_message_expiry_interval(&msg->properties, &message_expiry);

    if (msg->clientid) {
      HASH_FIND(hh_id, db.contexts_by_id, msg->clientid, strlen(msg->clientid), context);
      if (context) {
        single_publish(context, msg, message_expiry);
      }
    } else {
      ttqDbMessageEasyQueue(NULL, msg->topic, (uint8_t)msg->qos, (uint32_t)msg->payloadlen, msg->payload, msg->retain,
                            message_expiry, &msg->properties);
    }
    ttq_free(msg->topic);
    ttq_free(msg->payload);
    tmqtt_property_free_all(&msg->properties);
    ttq_free(msg->clientid);
    ttq_free(msg);
  }
}

int ttqMainloop(struct tmqtt__listener_sock *listensock, int listensock_count) {
#ifdef WITH_PERSISTENCE
  time_t last_backup = tmqtt_time();
#endif
  int rc;

  db.now_s = tmqtt_time();
  db.now_real_s = time(NULL);

  ttq_ppid_init();

  while (run) {
    queue_plugin_msgs();
    ttqCxtFreeDisused();
    ttqKeepaliveCheck();

    rc = ttqMuxHandle(listensock, listensock_count);
    if (rc) return rc;

    ttqSessionExpiryCheck();
    // will_delay__check();
#ifdef WITH_PERSISTENCE
    if (db.config->persistence && db.config->autosave_interval) {
      if (db.config->autosave_on_changes) {
        if (db.persistence_changes >= db.config->autosave_interval) {
          persist__backup(false);
          db.persistence_changes = 0;
        }
      } else {
        if (last_backup + db.config->autosave_interval < db.now_s) {
          persist__backup(false);
          last_backup = db.now_s;
        }
      }
    }
#endif

#ifdef WITH_PERSISTENCE
    if (flag_db_backup) {
      persist__backup(false);
      flag_db_backup = false;
    }
#endif
    if (flag_reload) {
      ttq_log(NULL, TTQ_LOG_INFO, "Reloading config.");
      // ttqConfigRead(db.config, true);
      // tmqtt_security_cleanup(true);
      // tmqtt_security_init(true);
      // tmqtt_security_apply();
      ttqLogClose(db.config);
      ttqLogInit(db.config);
      flag_reload = false;
    }
    if (flag_tree_print) {
      ttqSubTreePrint(db.normal_subs, 0);
      ttqSubTreePrint(db.shared_subs, 0);
      flag_tree_print = false;
    }

    tmq_ctx_poll_msgs();

    if (ttq_ppid_changed()) {
      run = 0;
    }
  }

  return TTQ_ERR_SUCCESS;
}

void ttqDisconnect(struct tmqtt *context, int reason) {
  const char *id;

  if (context->state == ttq_cs_disconnected) {
    return;
  }

  if (db.config->connection_messages == true) {
    if (context->id) {
      id = context->id;
    } else {
      id = "<unknown>";
    }
    if (context->state != ttq_cs_disconnecting && context->state != ttq_cs_disconnect_with_will) {
      switch (reason) {
        case TTQ_ERR_SUCCESS:
          break;
        case TTQ_ERR_MALFORMED_PACKET:
          ttq_log(NULL, TTQ_LOG_NOTICE, "Client %s disconnected due to malformed packet.", id);
          break;
        case TTQ_ERR_PROTOCOL:
          ttq_log(NULL, TTQ_LOG_NOTICE, "Client %s disconnected due to protocol error.", id);
          break;
        case TTQ_ERR_CONN_LOST:
          ttq_log(NULL, TTQ_LOG_NOTICE, "Client %s closed its connection.", id);
          break;
        case TTQ_ERR_AUTH:
          ttq_log(NULL, TTQ_LOG_NOTICE, "Client %s disconnected, not authorised.", id);
          break;
        case TTQ_ERR_KEEPALIVE:
          ttq_log(NULL, TTQ_LOG_NOTICE, "Client %s has exceeded timeout, disconnecting.", id);
          break;
        case TTQ_ERR_OVERSIZE_PACKET:
          ttq_log(NULL, TTQ_LOG_NOTICE, "Client %s disconnected due to oversize packet.", id);
          break;
        case TTQ_ERR_PAYLOAD_SIZE:
          ttq_log(NULL, TTQ_LOG_NOTICE, "Client %s disconnected due to oversize payload.", id);
          break;
        case TTQ_ERR_NOMEM:
          ttq_log(NULL, TTQ_LOG_NOTICE, "Client %s disconnected due to out of memory.", id);
          break;
        case TTQ_ERR_NOT_SUPPORTED:
          ttq_log(NULL, TTQ_LOG_NOTICE,
                  "Client %s disconnected due to using not allowed feature (QoS too high, retain not supported, "
                  "or bad AUTH method).",
                  id);
          break;
        case TTQ_ERR_ADMINISTRATIVE_ACTION:
          ttq_log(NULL, TTQ_LOG_NOTICE, "Client %s been disconnected by administrative action.", id);
          break;
        case TTQ_ERR_ERRNO:
          ttq_log(NULL, TTQ_LOG_NOTICE, "Client %s disconnected: %s.", id, strerror(errno));
          break;
        default:
          ttq_log(NULL, TTQ_LOG_NOTICE, "Bad socket read/write on client %s: %s", id, tmqtt_strerror(reason));
          break;
      }
    } else {
      if (reason == TTQ_ERR_ADMINISTRATIVE_ACTION) {
        ttq_log(NULL, TTQ_LOG_NOTICE, "Client %s been disconnected by administrative action.", id);
      } else {
        ttq_log(NULL, TTQ_LOG_NOTICE, "Client %s disconnected.", id);
      }
    }
  }

  ttqMuxDelete(context);
  ttqCxtDisconnect(context);
}
