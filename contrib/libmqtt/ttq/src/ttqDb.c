#include "tmqttBrokerInt.h"

#include <stdio.h>
#include <ttlist.h>

#include "ttqMemory.h"
#include "ttqSend.h"
#include "ttqSystree.h"
#include "ttqTime.h"
#include "ttqUtil.h"

/**
 * Is this context ready to take more in flight messages right now?
 * @param context the client context of interest
 * @param qos qos for the packet of interest
 * @return true if more in flight are allowed.
 */
bool ttqDbReadyForFlight(struct tmqtt *context, enum tmqtt_msg_direction dir, int qos) {
  struct tmqtt_msg_data *msgs;
  bool                   valid_bytes;
  bool                   valid_count;

  if (dir == ttq_md_out) {
    msgs = &context->msgs_out;
  } else {
    msgs = &context->msgs_in;
  }

  if (msgs->inflight_maximum == 0 && db.config->max_inflight_bytes == 0) {
    return true;
  }

  if (qos == 0) {
    /* Deliver QoS 0 messages unless the queue is already full.
     * For QoS 0 messages the choice is either "inflight" or dropped.
     * There is no queueing option, unless the client is offline and
     * queue_qos0_messages is enabled.
     */
    if (db.config->max_queued_messages == 0 && db.config->max_inflight_bytes == 0) {
      return true;
    }
    valid_bytes =
        ((msgs->inflight_bytes - (ssize_t)db.config->max_inflight_bytes) < (ssize_t)db.config->max_queued_bytes);
    if (dir == ttq_md_out) {
      valid_count = context->out_packet_count < db.config->max_queued_messages;
    } else {
      valid_count = msgs->inflight_count - msgs->inflight_maximum < db.config->max_queued_messages;
    }

    if (db.config->max_queued_messages == 0) {
      return valid_bytes;
    }
    if (db.config->max_queued_bytes == 0) {
      return valid_count;
    }
  } else {
    valid_bytes = (ssize_t)msgs->inflight_bytes12 < (ssize_t)db.config->max_inflight_bytes;
    valid_count = msgs->inflight_quota > 0;

    if (msgs->inflight_maximum == 0) {
      return valid_bytes;
    }
    if (db.config->max_inflight_bytes == 0) {
      return valid_count;
    }
  }

  return valid_bytes && valid_count;
}

/**
 * For a given client context, are more messages allowed to be queued?
 * It is assumed that inflight checks and queue_qos0 checks have already
 * been made.
 * @param context client of interest
 * @param qos destination qos for the packet of interest
 * @return true if queuing is allowed, false if should be dropped
 */
bool ttqDbReadyForQueue(struct tmqtt *context, int qos, struct tmqtt_msg_data *msg_data) {
  int     source_count;
  int     adjust_count;
  long    source_bytes;
  ssize_t adjust_bytes = (ssize_t)db.config->max_inflight_bytes;
  bool    valid_bytes;
  bool    valid_count;

  if (db.config->max_queued_messages == 0 && db.config->max_queued_bytes == 0) {
    return true;
  }

  if (qos == 0 && db.config->queue_qos0_messages == false) {
    return false; /* This case is handled in ttqDbReadyForFlight() */
  } else {
    source_bytes = (ssize_t)msg_data->queued_bytes12;
    source_count = msg_data->queued_count12;
  }
  adjust_count = msg_data->inflight_maximum;

  /* nothing in flight for offline clients */
  if (context->sock == INVALID_SOCKET) {
    adjust_bytes = 0;
    adjust_count = 0;
  }

  valid_bytes = (source_bytes - (ssize_t)adjust_bytes) < (ssize_t)db.config->max_queued_bytes;
  valid_count = source_count - adjust_count < db.config->max_queued_messages;

  if (db.config->max_queued_bytes == 0) {
    return valid_count;
  }
  if (db.config->max_queued_messages == 0) {
    return valid_bytes;
  }

  return valid_bytes && valid_count;
}

void ttqDbMsgAddToInflightStats(struct tmqtt_msg_data *msg_data, struct tmqtt_client_msg *msg) {
  msg_data->inflight_count++;
  msg_data->inflight_bytes += msg->store->payloadlen;
  if (msg->qos != 0) {
    msg_data->inflight_count12++;
    msg_data->inflight_bytes12 += msg->store->payloadlen;
  }
}

static void db__msg_remove_from_inflight_stats(struct tmqtt_msg_data *msg_data, struct tmqtt_client_msg *msg) {
  msg_data->inflight_count--;
  msg_data->inflight_bytes -= msg->store->payloadlen;
  if (msg->qos != 0) {
    msg_data->inflight_count12--;
    msg_data->inflight_bytes12 -= msg->store->payloadlen;
  }
}

void ttqDbMsgAddToQueuedStats(struct tmqtt_msg_data *msg_data, struct tmqtt_client_msg *msg) {
  msg_data->queued_count++;
  msg_data->queued_bytes += msg->store->payloadlen;
  if (msg->qos != 0) {
    msg_data->queued_count12++;
    msg_data->queued_bytes12 += msg->store->payloadlen;
  }
}

static void db__msg_remove_from_queued_stats(struct tmqtt_msg_data *msg_data, struct tmqtt_client_msg *msg) {
  msg_data->queued_count--;
  msg_data->queued_bytes -= msg->store->payloadlen;
  if (msg->qos != 0) {
    msg_data->queued_count12--;
    msg_data->queued_bytes12 -= msg->store->payloadlen;
  }
}

int ttqDbOpen(struct tmqtt__config *config) {
  struct tmqtt__subhier *subhier;

  if (!config) return TTQ_ERR_INVAL;

  db.last_db_id = 0;

  db.contexts_by_id = NULL;
  db.contexts_by_sock = NULL;
  db.contexts_for_free = NULL;
#ifdef WITH_BRIDGE
  db.bridges = NULL;
  db.bridge_count = 0;
#endif

  /* Initialize the hashtable */
  db.clientid_index_hash = NULL;

  db.normal_subs = NULL;
  db.shared_subs = NULL;

  subhier = ttqSubAddHierEntry(NULL, &db.shared_subs, "", 0);
  if (!subhier) return TTQ_ERR_NOMEM;

  subhier = ttqSubAddHierEntry(NULL, &db.normal_subs, "", 0);
  if (!subhier) return TTQ_ERR_NOMEM;

  subhier = ttqSubAddHierEntry(NULL, &db.normal_subs, "$SYS", (uint16_t)strlen("$SYS"));
  if (!subhier) return TTQ_ERR_NOMEM;

    // retain__init();

    // TODO:
    // db.config->security_options.unpwd = NULL;

#ifdef WITH_PERSISTENCE
  if (persist__restore()) return 1;
#endif

  return TTQ_ERR_SUCCESS;
}

static void subhier_clean(struct tmqtt__subhier **subhier) {
  struct tmqtt__subhier *peer, *subhier_tmp;
  struct tmqtt__subleaf *leaf, *nextleaf;

  HASH_ITER(hh, *subhier, peer, subhier_tmp) {
    leaf = peer->subs;
    while (leaf) {
      nextleaf = leaf->next;
      ttq_free(leaf);
      leaf = nextleaf;
    }
    subhier_clean(&peer->children);
    ttq_free(peer->topic);

    HASH_DELETE(hh, *subhier, peer);
    ttq_free(peer);
  }
}

int ttqDbClose(void) {
  subhier_clean(&db.normal_subs);
  subhier_clean(&db.shared_subs);
  // retain__clean(&db.retains);
  ttqDbMsgStoreClean();

  return TTQ_ERR_SUCCESS;
}

void ttqDbMsgStoreAdd(struct tmqtt_msg_store *store) {
  store->next = db.msg_store;
  store->prev = NULL;
  if (db.msg_store) {
    db.msg_store->prev = store;
  }
  db.msg_store = store;
}

void ttqDbMsgStoreFree(struct tmqtt_msg_store *store) {
  int i;

  ttq_free(store->source_id);
  ttq_free(store->source_username);
  if (store->dest_ids) {
    for (i = 0; i < store->dest_id_count; i++) {
      ttq_free(store->dest_ids[i]);
    }
    ttq_free(store->dest_ids);
  }
  ttq_free(store->topic);
  tmqtt_property_free_all(&store->properties);
  ttq_free(store->payload);
  ttq_free(store);
}

void ttqDbMsgStoreRemove(struct tmqtt_msg_store *store) {
  if (store->prev) {
    store->prev->next = store->next;
    if (store->next) {
      store->next->prev = store->prev;
    }
  } else {
    db.msg_store = store->next;
    if (store->next) {
      store->next->prev = NULL;
    }
  }
  db.msg_store_count--;
  db.msg_store_bytes -= store->payloadlen;

  ttqDbMsgStoreFree(store);
}

void ttqDbMsgStoreClean(void) {
  struct tmqtt_msg_store *store, *next;
  ;

  store = db.msg_store;
  while (store) {
    next = store->next;
    ttqDbMsgStoreRemove(store);
    store = next;
  }
}

void ttqDbMsgStoreRefInc(struct tmqtt_msg_store *store) { store->ref_count++; }

void ttqDbMsgStoreRefDec(struct tmqtt_msg_store **store) {
  (*store)->ref_count--;
  if ((*store)->ref_count == 0) {
    ttqDbMsgStoreRemove(*store);
    *store = NULL;
  }
}

void ttqDbMsgStoreCompact(void) {
  struct tmqtt_msg_store *store, *next;

  store = db.msg_store;
  while (store) {
    next = store->next;
    if (store->ref_count < 1) {
      ttqDbMsgStoreRemove(store);
    }
    store = next;
  }
}

static void db__message_remove_from_inflight(struct tmqtt_msg_data *msg_data, struct tmqtt_client_msg *item) {
  if (!msg_data || !item) {
    return;
  }

  DL_DELETE(msg_data->inflight, item);
  if (item->store) {
    db__msg_remove_from_inflight_stats(msg_data, item);
    ttqDbMsgStoreRefDec(&item->store);
  }

  tmqtt_property_free_all(&item->properties);
  ttq_free(item);
}

static void db__message_remove_from_queued(struct tmqtt_msg_data *msg_data, struct tmqtt_client_msg *item) {
  if (!msg_data || !item) {
    return;
  }

  DL_DELETE(msg_data->queued, item);
  if (item->store) {
    ttqDbMsgStoreRefDec(&item->store);
  }

  tmqtt_property_free_all(&item->properties);
  ttq_free(item);
}

void ttqDbMessageDequeueFirst(struct tmqtt *context, struct tmqtt_msg_data *msg_data) {
  struct tmqtt_client_msg *msg;

  UNUSED(context);

  msg = msg_data->queued;
  DL_DELETE(msg_data->queued, msg);
  DL_APPEND(msg_data->inflight, msg);
  if (msg_data->inflight_quota > 0) {
    msg_data->inflight_quota--;
  }

  db__msg_remove_from_queued_stats(msg_data, msg);
  ttqDbMsgAddToInflightStats(msg_data, msg);
}

int ttqDbMessageDeleteOutgoing(struct tmqtt *context, uint16_t mid, enum tmqtt_msg_state expect_state, int qos) {
  struct tmqtt_client_msg *tail, *tmp;
  int                      msg_index = 0;

  if (!context) return TTQ_ERR_INVAL;

  DL_FOREACH_SAFE(context->msgs_out.inflight, tail, tmp) {
    msg_index++;
    if (tail->mid == mid) {
      if (tail->qos != qos) {
        return TTQ_ERR_PROTOCOL;
      } else if (qos == 2 && tail->state != expect_state) {
        return TTQ_ERR_PROTOCOL;
      }
      msg_index--;
      db__message_remove_from_inflight(&context->msgs_out, tail);
      break;
    }
  }

  DL_FOREACH_SAFE(context->msgs_out.queued, tail, tmp) {
    if (!ttqDbReadyForFlight(context, ttq_md_out, tail->qos)) {
      break;
    }

    msg_index++;
    tail->timestamp = db.now_s;
    switch (tail->qos) {
      case 0:
        tail->state = ttq_ms_publish_qos0;
        break;
      case 1:
        tail->state = ttq_ms_publish_qos1;
        break;
      case 2:
        tail->state = ttq_ms_publish_qos2;
        break;
    }
    ttqDbMessageDequeueFirst(context, &context->msgs_out);
  }
#ifdef WITH_PERSISTENCE
  db.persistence_changes++;
#endif

  return ttqDbMessageWriteInflightOutLatest(context);
}

int ttqDbMessageInsert(struct tmqtt *context, uint16_t mid, enum tmqtt_msg_direction dir, uint8_t qos, bool retain,
                       struct tmqtt_msg_store *stored, tmqtt_property *properties, bool update) {
  struct tmqtt_client_msg *msg;
  struct tmqtt_msg_data   *msg_data;
  enum tmqtt_msg_state     state = ttq_ms_invalid;
  int                      rc = 0;
  int                      i;
  char                   **dest_ids;

  if (!context) return TTQ_ERR_INVAL;
  if (!context->id)
    return TTQ_ERR_SUCCESS; /* Protect against unlikely "client is disconnected but not entirely freed" scenario */

  if (dir == ttq_md_out) {
    msg_data = &context->msgs_out;
  } else {
    msg_data = &context->msgs_in;
  }

  /* Check whether we've already sent this message to this client
   * for outgoing messages only.
   * If retain==true then this is a stale retained message and so should be
   * sent regardless. FIXME - this does mean retained messages will received
   * multiple times for overlapping subscriptions, although this is only the
   * case for SUBSCRIPTION with multiple subs in so is a minor concern.
   */
  if (context->protocol != ttq_p_mqtt5 && db.config->allow_duplicate_messages == false && dir == ttq_md_out &&
      retain == false && stored->dest_ids) {
    for (i = 0; i < stored->dest_id_count; i++) {
      if (stored->dest_ids[i] && !strcmp(stored->dest_ids[i], context->id)) {
        /* We have already sent this message to this client. */
        tmqtt_property_free_all(&properties);
        return TTQ_ERR_SUCCESS;
      }
    }
  }
  if (context->sock == INVALID_SOCKET) {
    /* Client is not connected only queue messages with QoS>0. */
    if (qos == 0 && !db.config->queue_qos0_messages) {
      if (!context->bridge) {
        tmqtt_property_free_all(&properties);
        return 2;
      } else {
        if (context->bridge->start_type != bst_lazy) {
          tmqtt_property_free_all(&properties);
          return 2;
        }
      }
    }
    if (context->bridge && context->bridge->clean_start_local == true) {
      tmqtt_property_free_all(&properties);
      return 2;
    }
  }

  if (context->sock != INVALID_SOCKET) {
    if (ttqDbReadyForFlight(context, dir, qos)) {
      if (dir == ttq_md_out) {
        switch (qos) {
          case 0:
            state = ttq_ms_publish_qos0;
            break;
          case 1:
            state = ttq_ms_publish_qos1;
            break;
          case 2:
            state = ttq_ms_publish_qos2;
            break;
        }
      } else {
        if (qos == 2) {
          state = ttq_ms_wait_for_pubrel;
        } else {
          tmqtt_property_free_all(&properties);
          return 1;
        }
      }
    } else if (qos != 0 && ttqDbReadyForQueue(context, qos, msg_data)) {
      state = ttq_ms_queued;
      rc = 2;
    } else {
      /* Dropping message due to full queue. */
      if (context->is_dropping == false) {
        context->is_dropping = true;
        ttq_log(NULL, TTQ_LOG_NOTICE, "Outgoing messages are being dropped for client %s.", context->id);
      }
      G_MSGS_DROPPED_INC();
      tmqtt_property_free_all(&properties);
      return 2;
    }
  } else {
    if (ttqDbReadyForQueue(context, qos, msg_data)) {
      state = ttq_ms_queued;
    } else {
      G_MSGS_DROPPED_INC();
      if (context->is_dropping == false) {
        context->is_dropping = true;
        ttq_log(NULL, TTQ_LOG_NOTICE, "Outgoing messages are being dropped for client %s.", context->id);
      }
      tmqtt_property_free_all(&properties);
      return 2;
    }
  }

#ifdef WITH_PERSISTENCE
  if (state == ttq_ms_queued) {
    db.persistence_changes++;
  }
#endif

  msg = ttq_calloc(1, sizeof(struct tmqtt_client_msg));
  if (!msg) return TTQ_ERR_NOMEM;
  msg->prev = NULL;
  msg->next = NULL;
  msg->store = stored;
  ttqDbMsgStoreRefInc(msg->store);
  msg->mid = mid;
  msg->timestamp = db.now_s;
  msg->direction = dir;
  msg->state = state;
  msg->dup = false;
  if (qos > context->max_qos) {
    msg->qos = context->max_qos;
  } else {
    msg->qos = qos;
  }
  msg->retain = retain;
  msg->properties = properties;

  if (state == ttq_ms_queued) {
    DL_APPEND(msg_data->queued, msg);
    ttqDbMsgAddToQueuedStats(msg_data, msg);
  } else {
    DL_APPEND(msg_data->inflight, msg);
    ttqDbMsgAddToInflightStats(msg_data, msg);
  }

  if (db.config->allow_duplicate_messages == false && dir == ttq_md_out && retain == false) {
    /* Record which client ids this message has been sent to so we can avoid duplicates.
     * Outgoing messages only.
     * If retain==true then this is a stale retained message and so should be
     * sent regardless. FIXME - this does mean retained messages will received
     * multiple times for overlapping subscriptions, although this is only the
     * case for SUBSCRIPTION with multiple subs in so is a minor concern.
     */
    dest_ids = ttq_realloc(stored->dest_ids, sizeof(char *) * (size_t)(stored->dest_id_count + 1));
    if (dest_ids) {
      stored->dest_ids = dest_ids;
      stored->dest_id_count++;
      stored->dest_ids[stored->dest_id_count - 1] = ttq_strdup(context->id);
      if (!stored->dest_ids[stored->dest_id_count - 1]) {
        return TTQ_ERR_NOMEM;
      }
    } else {
      return TTQ_ERR_NOMEM;
    }
  }

  if (dir == ttq_md_out && msg->qos > 0 && state != ttq_ms_queued) {
    util__decrement_send_quota(context);
  } else if (dir == ttq_md_in && msg->qos > 0 && state != ttq_ms_queued) {
    util__decrement_receive_quota(context);
  }

  if (dir == ttq_md_out && update) {
    rc = ttqDbMessageWriteInflightOutLatest(context);
    if (rc) return rc;
    rc = ttqDbMessageWriteQueuedOut(context);
    if (rc) return rc;
  }

  return rc;
}

int ttqDbMessageUpdateOutgoing(struct tmqtt *context, uint16_t mid, enum tmqtt_msg_state state, int qos) {
  struct tmqtt_client_msg *tail;

  DL_FOREACH(context->msgs_out.inflight, tail) {
    if (tail->mid == mid) {
      if (tail->qos != qos) {
        return TTQ_ERR_PROTOCOL;
      }
      tail->state = state;
      tail->timestamp = db.now_s;
      return TTQ_ERR_SUCCESS;
    }
  }
  return TTQ_ERR_NOT_FOUND;
}

static void ttqDbMessageDelete_list(struct tmqtt_client_msg **head) {
  struct tmqtt_client_msg *tail, *tmp;

  DL_FOREACH_SAFE(*head, tail, tmp) {
    DL_DELETE(*head, tail);
    ttqDbMsgStoreRefDec(&tail->store);
    tmqtt_property_free_all(&tail->properties);
    ttq_free(tail);
  }
  *head = NULL;
}

int ttqDbMessageDelete(struct tmqtt *context, bool force_free) {
  if (!context) return TTQ_ERR_INVAL;

  if (force_free || context->clean_start || (context->bridge && context->bridge->clean_start)) {
    ttqDbMessageDelete_list(&context->msgs_in.inflight);
    ttqDbMessageDelete_list(&context->msgs_in.queued);
    context->msgs_in.inflight_bytes = 0;
    context->msgs_in.inflight_bytes12 = 0;
    context->msgs_in.inflight_count = 0;
    context->msgs_in.inflight_count12 = 0;
    context->msgs_in.queued_bytes = 0;
    context->msgs_in.queued_bytes12 = 0;
    context->msgs_in.queued_count = 0;
    context->msgs_in.queued_count12 = 0;
  }

  if (force_free || (context->bridge && context->bridge->clean_start_local) ||
      (context->bridge == NULL && context->clean_start)) {
    ttqDbMessageDelete_list(&context->msgs_out.inflight);
    ttqDbMessageDelete_list(&context->msgs_out.queued);
    context->msgs_out.inflight_bytes = 0;
    context->msgs_out.inflight_bytes12 = 0;
    context->msgs_out.inflight_count = 0;
    context->msgs_out.inflight_count12 = 0;
    context->msgs_out.queued_bytes = 0;
    context->msgs_out.queued_bytes12 = 0;
    context->msgs_out.queued_count = 0;
    context->msgs_out.queued_count12 = 0;
  }

  return TTQ_ERR_SUCCESS;
}

int ttqDbMessageEasyQueue(struct tmqtt *context, const char *topic, uint8_t qos, uint32_t payloadlen,
                          const void *payload, int retain, uint32_t message_expiry_interval,
                          tmqtt_property **properties) {
  struct tmqtt_msg_store *stored;
  const char             *source_id;
  enum tmqtt_msg_origin   origin;

  if (!topic) return TTQ_ERR_INVAL;

  stored = ttq_calloc(1, sizeof(struct tmqtt_msg_store));
  if (stored == NULL) return TTQ_ERR_NOMEM;

  stored->topic = ttq_strdup(topic);
  if (stored->topic == NULL) {
    ttqDbMsgStoreFree(stored);
    return TTQ_ERR_INVAL;
  }

  stored->qos = qos;
  if (db.config->retain_available == false) {
    stored->retain = 0;
  } else {
    stored->retain = retain;
  }

  stored->payloadlen = payloadlen;
  if (payloadlen > 0) {
    stored->payload = ttq_malloc(stored->payloadlen + 1);
    if (stored->payload == NULL) {
      ttqDbMsgStoreFree(stored);
      return TTQ_ERR_NOMEM;
    }
    /* Ensure payload is always zero terminated, this is the reason for the extra byte above */
    ((uint8_t *)stored->payload)[stored->payloadlen] = 0;
    memcpy(stored->payload, payload, stored->payloadlen);
  }

  if (context && context->id) {
    source_id = context->id;
  } else {
    source_id = "";
  }
  if (properties) {
    stored->properties = *properties;
    *properties = NULL;
  }

  if (context) {
    origin = ttq_mo_client;
  } else {
    origin = ttq_mo_broker;
  }
  if (ttqDbMessageStore(context, stored, message_expiry_interval, 0, origin)) return 1;

  return ttqSubMessagesQueue(source_id, stored->topic, stored->qos, stored->retain, &stored);
}

/* This function requires topic to be allocated on the heap. Once called, it owns topic and will free it on error.
 * Likewise payload and properties. */
int ttqDbMessageStore(const struct tmqtt *source, struct tmqtt_msg_store *stored, uint32_t message_expiry_interval,
                      dbid_t store_id, enum tmqtt_msg_origin origin) {
  if (source && source->id) {
    stored->source_id = ttq_strdup(source->id);
  } else {
    stored->source_id = ttq_strdup("");
  }
  if (!stored->source_id) {
    ttq_log(NULL, TTQ_LOG_ERR, "Error: Out of memory.");
    ttqDbMsgStoreFree(stored);
    return TTQ_ERR_NOMEM;
  }

  if (source && source->username) {
    stored->source_username = ttq_strdup(source->username);
    if (!stored->source_username) {
      ttqDbMsgStoreFree(stored);
      return TTQ_ERR_NOMEM;
    }
  }
  if (source) {
    stored->source_listener = source->listener;
  }
  stored->mid = 0;
  stored->origin = origin;
  if (message_expiry_interval > 0) {
    stored->message_expiry_time = db.now_real_s + message_expiry_interval;
  } else {
    stored->message_expiry_time = 0;
  }

  stored->dest_ids = NULL;
  stored->dest_id_count = 0;
  db.msg_store_count++;
  db.msg_store_bytes += stored->payloadlen;

  if (!store_id) {
    stored->db_id = ++db.last_db_id;
  } else {
    stored->db_id = store_id;
  }

  ttqDbMsgStoreAdd(stored);

  return TTQ_ERR_SUCCESS;
}

int ttqDbMessageStore_find(struct tmqtt *context, uint16_t mid, struct tmqtt_client_msg **client_msg) {
  struct tmqtt_client_msg *cmsg;

  *client_msg = NULL;

  if (!context) return TTQ_ERR_INVAL;

  DL_FOREACH(context->msgs_in.inflight, cmsg) {
    if (cmsg->store->source_mid == mid) {
      *client_msg = cmsg;
      return TTQ_ERR_SUCCESS;
    }
  }

  DL_FOREACH(context->msgs_in.queued, cmsg) {
    if (cmsg->store->source_mid == mid) {
      *client_msg = cmsg;
      return TTQ_ERR_SUCCESS;
    }
  }

  return 1;
}

/* Called on reconnect to set outgoing messages to a sensible state and force a
 * retry, and to set incoming messages to expect an appropriate retry. */
static int ttqDbMessageReconnectReset_outgoing(struct tmqtt *context) {
  struct tmqtt_client_msg *msg, *tmp;

  context->msgs_out.inflight_bytes = 0;
  context->msgs_out.inflight_bytes12 = 0;
  context->msgs_out.inflight_count = 0;
  context->msgs_out.inflight_count12 = 0;
  context->msgs_out.queued_bytes = 0;
  context->msgs_out.queued_bytes12 = 0;
  context->msgs_out.queued_count = 0;
  context->msgs_out.queued_count12 = 0;
  context->msgs_out.inflight_quota = context->msgs_out.inflight_maximum;

  DL_FOREACH_SAFE(context->msgs_out.inflight, msg, tmp) {
    ttqDbMsgAddToInflightStats(&context->msgs_out, msg);
    if (msg->qos > 0) {
      util__decrement_send_quota(context);
    }

    switch (msg->qos) {
      case 0:
        msg->state = ttq_ms_publish_qos0;
        break;
      case 1:
        msg->state = ttq_ms_publish_qos1;
        break;
      case 2:
        if (msg->state == ttq_ms_wait_for_pubcomp) {
          msg->state = ttq_ms_resend_pubrel;
        } else {
          msg->state = ttq_ms_publish_qos2;
        }
        break;
    }
  }
  /* Messages received when the client was disconnected are put
   * in the ttq_ms_queued state. If we don't change them to the
   * appropriate "publish" state, then the queued messages won't
   * get sent until the client next receives a message - and they
   * will be sent out of order.
   */
  DL_FOREACH_SAFE(context->msgs_out.queued, msg, tmp) {
    ttqDbMsgAddToQueuedStats(&context->msgs_out, msg);
    if (ttqDbReadyForFlight(context, ttq_md_out, msg->qos)) {
      switch (msg->qos) {
        case 0:
          msg->state = ttq_ms_publish_qos0;
          break;
        case 1:
          msg->state = ttq_ms_publish_qos1;
          break;
        case 2:
          msg->state = ttq_ms_publish_qos2;
          break;
      }
      ttqDbMessageDequeueFirst(context, &context->msgs_out);
    }
  }

  return TTQ_ERR_SUCCESS;
}

/* Called on reconnect to set incoming messages to expect an appropriate retry. */
static int ttqDbMessageReconnectReset_incoming(struct tmqtt *context) {
  struct tmqtt_client_msg *msg, *tmp;

  context->msgs_in.inflight_bytes = 0;
  context->msgs_in.inflight_bytes12 = 0;
  context->msgs_in.inflight_count = 0;
  context->msgs_in.inflight_count12 = 0;
  context->msgs_in.queued_bytes = 0;
  context->msgs_in.queued_bytes12 = 0;
  context->msgs_in.queued_count = 0;
  context->msgs_in.queued_count12 = 0;
  context->msgs_in.inflight_quota = context->msgs_in.inflight_maximum;

  DL_FOREACH_SAFE(context->msgs_in.inflight, msg, tmp) {
    ttqDbMsgAddToInflightStats(&context->msgs_in, msg);
    if (msg->qos > 0) {
      util__decrement_receive_quota(context);
    }

    if (msg->qos != 2) {
      /* Anything <QoS 2 can be completely retried by the client at
       * no harm. */
      db__message_remove_from_inflight(&context->msgs_in, msg);
    } else {
      /* Message state can be preserved here because it should match
       * whatever the client has got. */
      msg->dup = 0;
    }
  }

  /* Messages received when the client was disconnected are put
   * in the ttq_ms_queued state. If we don't change them to the
   * appropriate "publish" state, then the queued messages won't
   * get sent until the client next receives a message - and they
   * will be sent out of order.
   */
  DL_FOREACH_SAFE(context->msgs_in.queued, msg, tmp) {
    msg->dup = 0;
    ttqDbMsgAddToQueuedStats(&context->msgs_in, msg);
    if (ttqDbReadyForFlight(context, ttq_md_in, msg->qos)) {
      switch (msg->qos) {
        case 0:
          msg->state = ttq_ms_publish_qos0;
          break;
        case 1:
          msg->state = ttq_ms_publish_qos1;
          break;
        case 2:
          msg->state = ttq_ms_publish_qos2;
          break;
      }
      ttqDbMessageDequeueFirst(context, &context->msgs_in);
    }
  }

  return TTQ_ERR_SUCCESS;
}

int ttqDbMessageReconnectReset(struct tmqtt *context) {
  int rc;

  rc = ttqDbMessageReconnectReset_outgoing(context);
  if (rc) return rc;
  return ttqDbMessageReconnectReset_incoming(context);
}

int ttqDbMessageRemoveIncoming(struct tmqtt *context, uint16_t mid) {
  struct tmqtt_client_msg *tail, *tmp;

  if (!context) return TTQ_ERR_INVAL;

  DL_FOREACH_SAFE(context->msgs_in.inflight, tail, tmp) {
    if (tail->mid == mid) {
      if (tail->store->qos != 2) {
        return TTQ_ERR_PROTOCOL;
      }
      db__message_remove_from_inflight(&context->msgs_in, tail);
      return TTQ_ERR_SUCCESS;
    }
  }

  return TTQ_ERR_NOT_FOUND;
}

int ttqDbMessageReleaseIncoming(struct tmqtt *context, uint16_t mid) {
  struct tmqtt_client_msg *tail, *tmp;
  int                      retain;
  char                    *topic;
  char                    *source_id;
  int                      msg_index = 0;
  bool                     deleted = false;
  int                      rc;

  if (!context) return TTQ_ERR_INVAL;

  DL_FOREACH_SAFE(context->msgs_in.inflight, tail, tmp) {
    msg_index++;
    if (tail->mid == mid) {
      if (tail->store->qos != 2) {
        return TTQ_ERR_PROTOCOL;
      }
      topic = tail->store->topic;
      retain = tail->retain;
      source_id = tail->store->source_id;

      /* topic==NULL should be a QoS 2 message that was
       * denied/dropped and is being processed so the client doesn't
       * keep resending it. That means we don't send it to other
       * clients. */
      if (topic == NULL) {
        db__message_remove_from_inflight(&context->msgs_in, tail);
        deleted = true;
      } else {
        rc = ttqSubMessagesQueue(source_id, topic, 2, retain, &tail->store);
        if (rc == TTQ_ERR_SUCCESS || rc == TTQ_ERR_NO_SUBSCRIBERS) {
          db__message_remove_from_inflight(&context->msgs_in, tail);
          deleted = true;
        } else {
          return 1;
        }
      }
    }
  }

  DL_FOREACH_SAFE(context->msgs_in.queued, tail, tmp) {
    if (ttqDbReadyForFlight(context, ttq_md_in, tail->qos)) {
      break;
    }

    msg_index++;
    tail->timestamp = db.now_s;

    if (tail->qos == 2) {
      send__pubrec(context, tail->mid, 0, NULL);
      tail->state = ttq_ms_wait_for_pubrel;
      ttqDbMessageDequeueFirst(context, &context->msgs_in);
    }
  }
  if (deleted) {
    return TTQ_ERR_SUCCESS;
  } else {
    return TTQ_ERR_NOT_FOUND;
  }
}

void ttqDbExpireAllMessages(struct tmqtt *context) {
  struct tmqtt_client_msg *msg, *tmp;

  DL_FOREACH_SAFE(context->msgs_out.inflight, msg, tmp) {
    if (msg->store->message_expiry_time && db.now_real_s > msg->store->message_expiry_time) {
      if (msg->qos > 0) {
        util__increment_send_quota(context);
      }
      db__message_remove_from_inflight(&context->msgs_out, msg);
    }
  }
  DL_FOREACH_SAFE(context->msgs_out.queued, msg, tmp) {
    if (msg->store->message_expiry_time && db.now_real_s > msg->store->message_expiry_time) {
      db__message_remove_from_queued(&context->msgs_out, msg);
    }
  }
  DL_FOREACH_SAFE(context->msgs_in.inflight, msg, tmp) {
    if (msg->store->message_expiry_time && db.now_real_s > msg->store->message_expiry_time) {
      if (msg->qos > 0) {
        util__increment_receive_quota(context);
      }
      db__message_remove_from_inflight(&context->msgs_in, msg);
    }
  }
  DL_FOREACH_SAFE(context->msgs_in.queued, msg, tmp) {
    if (msg->store->message_expiry_time && db.now_real_s > msg->store->message_expiry_time) {
      db__message_remove_from_queued(&context->msgs_in, msg);
    }
  }
}

static int db__message_write_inflight_out_single(struct tmqtt *context, struct tmqtt_client_msg *msg) {
  tmqtt_property *cmsg_props = NULL, *store_props = NULL;
  int             rc;
  uint16_t        mid;
  int             retries;
  int             retain;
  const char     *topic;
  uint8_t         qos;
  uint32_t        payloadlen;
  const void     *payload;
  uint32_t        expiry_interval;

  expiry_interval = 0;
  if (msg->store->message_expiry_time) {
    if (db.now_real_s > msg->store->message_expiry_time) {
      /* Message is expired, must not send. */
      if (msg->direction == ttq_md_out && msg->qos > 0) {
        util__increment_send_quota(context);
      }
      db__message_remove_from_inflight(&context->msgs_out, msg);
      return TTQ_ERR_SUCCESS;
    } else {
      expiry_interval = (uint32_t)(msg->store->message_expiry_time - db.now_real_s);
    }
  }
  mid = msg->mid;
  retries = msg->dup;
  retain = msg->retain;
  topic = msg->store->topic;
  qos = (uint8_t)msg->qos;
  payloadlen = msg->store->payloadlen;
  payload = msg->store->payload;
  cmsg_props = msg->properties;
  store_props = msg->store->properties;

  switch (msg->state) {
    case ttq_ms_publish_qos0:
      rc = send__publish(context, mid, topic, payloadlen, payload, qos, retain, retries, cmsg_props, store_props,
                         expiry_interval);
      if (rc == TTQ_ERR_SUCCESS || rc == TTQ_ERR_OVERSIZE_PACKET) {
        db__message_remove_from_inflight(&context->msgs_out, msg);
      } else {
        return rc;
      }
      break;

    case ttq_ms_publish_qos1:
      rc = send__publish(context, mid, topic, payloadlen, payload, qos, retain, retries, cmsg_props, store_props,
                         expiry_interval);
      if (rc == TTQ_ERR_SUCCESS) {
        msg->timestamp = db.now_s;
        msg->dup = 1; /* Any retry attempts are a duplicate. */
        msg->state = ttq_ms_wait_for_puback;
      } else if (rc == TTQ_ERR_OVERSIZE_PACKET) {
        db__message_remove_from_inflight(&context->msgs_out, msg);
      } else {
        return rc;
      }
      break;

    case ttq_ms_publish_qos2:
      rc = send__publish(context, mid, topic, payloadlen, payload, qos, retain, retries, cmsg_props, store_props,
                         expiry_interval);
      if (rc == TTQ_ERR_SUCCESS) {
        msg->timestamp = db.now_s;
        msg->dup = 1; /* Any retry attempts are a duplicate. */
        msg->state = ttq_ms_wait_for_pubrec;
      } else if (rc == TTQ_ERR_OVERSIZE_PACKET) {
        db__message_remove_from_inflight(&context->msgs_out, msg);
      } else {
        return rc;
      }
      break;

    case ttq_ms_resend_pubrel:
      rc = send__pubrel(context, mid, NULL);
      if (!rc) {
        msg->state = ttq_ms_wait_for_pubcomp;
      } else {
        return rc;
      }
      break;

    case ttq_ms_invalid:
    case ttq_ms_send_pubrec:
    case ttq_ms_resend_pubcomp:
    case ttq_ms_wait_for_puback:
    case ttq_ms_wait_for_pubrec:
    case ttq_ms_wait_for_pubrel:
    case ttq_ms_wait_for_pubcomp:
    case ttq_ms_queued:
      break;
  }
  return TTQ_ERR_SUCCESS;
}

int ttqDbMessageWriteInflightOutAll(struct tmqtt *context) {
  struct tmqtt_client_msg *tail, *tmp;
  int                      rc;

  if (context->state != ttq_cs_active || context->sock == INVALID_SOCKET) {
    return TTQ_ERR_SUCCESS;
  }

  DL_FOREACH_SAFE(context->msgs_out.inflight, tail, tmp) {
    rc = db__message_write_inflight_out_single(context, tail);
    if (rc) return rc;
  }
  return TTQ_ERR_SUCCESS;
}

int ttqDbMessageWriteInflightOutLatest(struct tmqtt *context) {
  struct tmqtt_client_msg *tail, *next;
  int                      rc;

  if (context->state != ttq_cs_active || context->sock == INVALID_SOCKET || context->msgs_out.inflight == NULL) {
    return TTQ_ERR_SUCCESS;
  }

  if (context->msgs_out.inflight->prev == context->msgs_out.inflight) {
    /* Only one message */
    return db__message_write_inflight_out_single(context, context->msgs_out.inflight);
  }

  /* Start at the end of the list and work backwards looking for the first
   * message in a non-publish state */
  tail = context->msgs_out.inflight->prev;
  while (tail != context->msgs_out.inflight &&
         (tail->state == ttq_ms_publish_qos0 || tail->state == ttq_ms_publish_qos1 ||
          tail->state == ttq_ms_publish_qos2)) {
    tail = tail->prev;
  }

  /* Tail is now either the head of the list, if that message is waiting for
   * publish, or the oldest message not waiting for a publish. In the latter
   * case, any pending publishes should be next after this message. */
  if (tail != context->msgs_out.inflight) {
    tail = tail->next;
  }

  while (tail) {
    next = tail->next;
    rc = db__message_write_inflight_out_single(context, tail);
    if (rc) return rc;
    tail = next;
  }
  return TTQ_ERR_SUCCESS;
}

int ttqDbMessageWriteQueuedIn(struct tmqtt *context) {
  struct tmqtt_client_msg *tail, *tmp;
  int                      rc;

  if (context->state != ttq_cs_active) {
    return TTQ_ERR_SUCCESS;
  }

  DL_FOREACH_SAFE(context->msgs_in.queued, tail, tmp) {
    if (context->msgs_in.inflight_maximum != 0 && context->msgs_in.inflight_quota == 0) {
      break;
    }

    if (tail->qos == 2) {
      tail->state = ttq_ms_send_pubrec;
      ttqDbMessageDequeueFirst(context, &context->msgs_in);
      rc = send__pubrec(context, tail->mid, 0, NULL);
      if (!rc) {
        tail->state = ttq_ms_wait_for_pubrel;
      } else {
        return rc;
      }
    }
  }
  return TTQ_ERR_SUCCESS;
}

int ttqDbMessageWriteQueuedOut(struct tmqtt *context) {
  struct tmqtt_client_msg *tail, *tmp;

  if (context->state != ttq_cs_active) {
    return TTQ_ERR_SUCCESS;
  }

  DL_FOREACH_SAFE(context->msgs_out.queued, tail, tmp) {
    if (!ttqDbReadyForFlight(context, ttq_md_out, tail->qos)) {
      break;
    }

    switch (tail->qos) {
      case 0:
        tail->state = ttq_ms_publish_qos0;
        break;
      case 1:
        tail->state = ttq_ms_publish_qos1;
        break;
      case 2:
        tail->state = ttq_ms_publish_qos2;
        break;
    }
    ttqDbMessageDequeueFirst(context, &context->msgs_out);
  }
  return TTQ_ERR_SUCCESS;
}
