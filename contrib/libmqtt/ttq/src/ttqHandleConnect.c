#include "tmqttBrokerInt.h"

#include <stdio.h>
#include <string.h>
#include <ttlist.h>

#include "tmqttProto.h"
#include "ttqMemory.h"
#include "ttqPacket.h"
#include "ttqProperty.h"
#include "ttqSend.h"
#include "ttqSystree.h"
#include "ttqTime.h"
#include "ttqTls.h"
#include "ttqUtil.h"

static char nibble_to_hex(uint8_t value) {
  if (value < 0x0A) {
    return (char)('0' + value);
  } else {
    return (char)(65 /*'A'*/ + value - 10);
  }
}

static char *client_id_gen(uint16_t *idlen, const char *auto_id_prefix, uint16_t auto_id_prefix_len) {
  char   *client_id;
  uint8_t rnd[16];
  int     i;
  int     pos;

  if (util__random_bytes(rnd, 16)) return NULL;

  *idlen = (uint16_t)(auto_id_prefix_len + 36);

  client_id = (char *)ttq_calloc((size_t)(*idlen) + 1, sizeof(char));
  if (!client_id) {
    return NULL;
  }
  if (auto_id_prefix) {
    memcpy(client_id, auto_id_prefix, auto_id_prefix_len);
  }

  pos = 0;
  for (i = 0; i < 16; i++) {
    client_id[auto_id_prefix_len + pos + 0] = nibble_to_hex(rnd[i] & 0x0F);
    client_id[auto_id_prefix_len + pos + 1] = nibble_to_hex((rnd[i] >> 4) & 0x0F);
    pos += 2;
    if (pos == 8 || pos == 13 || pos == 18 || pos == 23) {
      client_id[auto_id_prefix_len + pos] = '-';
      pos++;
    }
  }

  return client_id;
}

/* Remove any queued messages that are no longer allowed through ACL,
 * assuming a possible change of username. */
static void connection_check_acl(struct tmqtt *context, struct tmqtt_client_msg **head) {
  struct tmqtt_client_msg *msg_tail, *tmp;
  int                      access;

  DL_FOREACH_SAFE((*head), msg_tail, tmp) {
    if (msg_tail->direction == ttq_md_out) {
      access = TTQ_ACL_READ;
    } else {
      access = TTQ_ACL_WRITE;
    }
    if (tmqttAclCheck(context, msg_tail->store->topic, msg_tail->store->payloadlen, msg_tail->store->payload,
                      msg_tail->store->qos, msg_tail->store->retain, access) != TTQ_ERR_SUCCESS) {
      DL_DELETE((*head), msg_tail);
      ttqDbMsgStoreRefDec(&msg_tail->store);
      tmqtt_property_free_all(&msg_tail->properties);
      ttq_free(msg_tail);
    }
  }
}

int ttqCxtOnAuthorised(struct tmqtt *context, void *auth_data_out, uint16_t auth_data_out_len) {
  struct tmqtt          *found_context;
  struct tmqtt__subleaf *leaf;
  tmqtt_property        *connack_props = NULL;
  uint8_t                connect_ack = 0;
  int                    i;
  int                    rc;
  int                    in_quota, out_quota;
  uint16_t               in_maximum, out_maximum;

  /* Find if this client already has an entry. This must be done *after* any security checks. */
  HASH_FIND(hh_id, db.contexts_by_id, context->id, strlen(context->id), found_context);
  if (found_context) {
    /* Found a matching client */
    if (found_context->sock == INVALID_SOCKET) {
      /* Client is reconnecting after a disconnect */
      /* FIXME - does anything need to be done here? */
    } else {
      /* Client is already connected, disconnect old version. This is
       * done in ttqCxtCleanup() below. */
      if (db.config->connection_messages == true) {
        ttq_log(NULL, TTQ_LOG_ERR, "Client %s already connected, closing old connection.", context->id);
      }
    }

    if (context->clean_start == false && found_context->session_expiry_interval > 0) {
      if (context->protocol == ttq_p_mqtt311 || context->protocol == ttq_p_mqtt5) {
        connect_ack |= 0x01;
      }

      if (found_context->msgs_in.inflight || found_context->msgs_in.queued || found_context->msgs_out.inflight ||
          found_context->msgs_out.queued) {
        in_quota = context->msgs_in.inflight_quota;
        out_quota = context->msgs_out.inflight_quota;
        in_maximum = context->msgs_in.inflight_maximum;
        out_maximum = context->msgs_out.inflight_maximum;

        memcpy(&context->msgs_in, &found_context->msgs_in, sizeof(struct tmqtt_msg_data));
        memcpy(&context->msgs_out, &found_context->msgs_out, sizeof(struct tmqtt_msg_data));

        memset(&found_context->msgs_in, 0, sizeof(struct tmqtt_msg_data));
        memset(&found_context->msgs_out, 0, sizeof(struct tmqtt_msg_data));

        context->msgs_in.inflight_quota = in_quota;
        context->msgs_out.inflight_quota = out_quota;
        context->msgs_in.inflight_maximum = in_maximum;
        context->msgs_out.inflight_maximum = out_maximum;

        ttqDbMessageReconnectReset(context);
      }
      context->subs = found_context->subs;
      found_context->subs = NULL;
      context->sub_count = found_context->sub_count;
      found_context->sub_count = 0;
      context->last_mid = found_context->last_mid;

      for (i = 0; i < context->sub_count; i++) {
        if (context->subs[i]) {
          leaf = context->subs[i]->hier->subs;
          while (leaf) {
            if (leaf->context == found_context) {
              leaf->context = context;
            }
            leaf = leaf->next;
          }

          if (context->subs[i]->shared) {
            leaf = context->subs[i]->shared->subs;
            while (leaf) {
              if (leaf->context == found_context) {
                leaf->context = context;
              }
              leaf = leaf->next;
            }
          }
        }
      }
    }

    if (context->clean_start == true) {
      ttqSubCleanSession(found_context);
    }
    if ((found_context->protocol == ttq_p_mqtt5 && found_context->session_expiry_interval == 0) ||
        (found_context->protocol != ttq_p_mqtt5 && found_context->clean_start == true) ||
        (context->clean_start == true)) {
      // ttqCxtSendWill(found_context);
    }

    ttqSessionExpiryRemove(found_context);
    // will_delay__remove(found_context);
    // will__clear(found_context);

    found_context->clean_start = true;
    found_context->session_expiry_interval = 0;
    tmqtt__set_state(found_context, ttq_cs_duplicate);

    if (found_context->protocol == ttq_p_mqtt5) {
      ttq_send_disconnect(found_context, MQTT_RC_SESSION_TAKEN_OVER, NULL);
    }
    ttqDisconnect(found_context, TTQ_ERR_SUCCESS);
  }
  /* TODO:
  rc = acl__find_acls(context);
  if (rc) {
    free(auth_data_out);
    return rc;
  }
  */
  if (db.config->connection_messages == true) {
    if (context->is_bridge) {
      if (context->username) {
        ttq_log(NULL, TTQ_LOG_NOTICE, "New bridge connected from %s:%d as %s (p%d, c%d, k%d, u'%s').", context->address,
                context->remote_port, context->id, context->protocol, context->clean_start, context->keepalive,
                context->username);
      } else {
        ttq_log(NULL, TTQ_LOG_NOTICE, "New bridge connected from %s:%d as %s (p%d, c%d, k%d).", context->address,
                context->remote_port, context->id, context->protocol, context->clean_start, context->keepalive);
      }
    } else {
      if (context->username) {
        ttq_log(NULL, TTQ_LOG_NOTICE, "New client connected from %s:%d as %s (p%d, c%d, k%d, u'%s').", context->address,
                context->remote_port, context->id, context->protocol, context->clean_start, context->keepalive,
                context->username);
      } else {
        ttq_log(NULL, TTQ_LOG_NOTICE, "New client connected from %s:%d as %s (p%d, c%d, k%d).", context->address,
                context->remote_port, context->id, context->protocol, context->clean_start, context->keepalive);
      }
    }

    if (context->will) {
      ttq_log(NULL, TTQ_LOG_DEBUG, "Will message specified (%ld bytes) (r%d, q%d).",
              (long)context->will->msg.payloadlen, context->will->msg.retain, context->will->msg.qos);

      ttq_log(NULL, TTQ_LOG_DEBUG, "\t%s", context->will->msg.topic);
    } else {
      ttq_log(NULL, TTQ_LOG_DEBUG, "No will message specified.");
    }
  }

  context->ping_t = 0;
  context->is_dropping = false;

  connection_check_acl(context, &context->msgs_in.inflight);
  connection_check_acl(context, &context->msgs_in.queued);
  connection_check_acl(context, &context->msgs_out.inflight);
  connection_check_acl(context, &context->msgs_out.queued);

  ttqCxtAddToById(context);

#ifdef WITH_PERSISTENCE
  if (!context->clean_start) {
    db.persistence_changes++;
  }
#endif
  context->max_qos = context->listener->max_qos;

  if (db.config->max_keepalive && (context->keepalive > db.config->max_keepalive || context->keepalive == 0)) {
    context->keepalive = db.config->max_keepalive;
    if (context->protocol == ttq_p_mqtt5) {
      if (tmqtt_property_add_int16(&connack_props, MQTT_PROP_SERVER_KEEP_ALIVE, context->keepalive)) {
        rc = TTQ_ERR_NOMEM;
        goto error;
      }
    } else {
      ttqSendConnack(context, connect_ack, CONNACK_REFUSED_IDENTIFIER_REJECTED, NULL);
      rc = TTQ_ERR_INVAL;
      goto error;
    }
  }

  if (context->protocol == ttq_p_mqtt5) {
    if (context->listener->max_topic_alias > 0) {
      if (tmqtt_property_add_int16(&connack_props, MQTT_PROP_TOPIC_ALIAS_MAXIMUM, context->listener->max_topic_alias)) {
        rc = TTQ_ERR_NOMEM;
        goto error;
      }
    }
    if (context->assigned_id) {
      if (tmqtt_property_add_string(&connack_props, MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER, context->id)) {
        rc = TTQ_ERR_NOMEM;
        goto error;
      }
    }
    if (context->auth_method) {
      if (tmqtt_property_add_string(&connack_props, MQTT_PROP_AUTHENTICATION_METHOD, context->auth_method)) {
        rc = TTQ_ERR_NOMEM;
        goto error;
      }

      if (auth_data_out && auth_data_out_len > 0) {
        if (tmqtt_property_add_binary(&connack_props, MQTT_PROP_AUTHENTICATION_DATA, auth_data_out,
                                      auth_data_out_len)) {
          rc = TTQ_ERR_NOMEM;
          goto error;
        }
      }
    }
  }
  free(auth_data_out);
  auth_data_out = NULL;

  ttqKeepaliveAdd(context);

  tmqtt__set_state(context, ttq_cs_active);
  rc = ttqSendConnack(context, connect_ack, CONNACK_ACCEPTED, connack_props);
  tmqtt_property_free_all(&connack_props);
  if (rc) return rc;
  ttqDbExpireAllMessages(context);
  rc = ttqDbMessageWriteQueuedOut(context);
  if (rc) return rc;
  rc = ttqDbMessageWriteInflightOutAll(context);
  return rc;
error:
  free(auth_data_out);
  tmqtt_property_free_all(&connack_props);
  return rc;
}
/*
static int will__read(struct tmqtt *context, const char *client_id, struct tmqtt_message_all **will, uint8_t will_qos,
                      int will_retain) {
  int                       rc = TTQ_ERR_SUCCESS;
  size_t                    slen;
  uint16_t                  tlen;
  struct tmqtt_message_all *will_struct = NULL;
  char                     *will_topic_mount = NULL;
  uint16_t                  payloadlen;
  tmqtt_property           *properties = NULL;

  will_struct = ttq_calloc(1, sizeof(struct tmqtt_message_all));
  if (!will_struct) {
    rc = TTQ_ERR_NOMEM;
    goto error_cleanup;
  }
  if (context->protocol == PROTOCOL_VERSION_v5) {
    rc = property__read_all(CMD_WILL, &context->in_packet, &properties);
    if (rc) goto error_cleanup;

    rc = ttqPropertyProcessWill(context, will_struct, &properties);
    tmqtt_property_free_all(&properties);
    if (rc) goto error_cleanup;
  }
  rc = packet__read_string(&context->in_packet, &will_struct->msg.topic, &tlen);
  if (rc) goto error_cleanup;
  if (!tlen) {
    rc = TTQ_ERR_PROTOCOL;
    goto error_cleanup;
  }

  if (context->listener->mount_point) {
    slen = strlen(context->listener->mount_point) + strlen(will_struct->msg.topic) + 1;
    will_topic_mount = ttq_malloc(slen + 1);
    if (!will_topic_mount) {
      rc = TTQ_ERR_NOMEM;
      goto error_cleanup;
    }

    snprintf(will_topic_mount, slen, "%s%s", context->listener->mount_point, will_struct->msg.topic);
    will_topic_mount[slen] = '\0';

    ttq_free(will_struct->msg.topic);
    will_struct->msg.topic = will_topic_mount;
  }

  if (!strncmp(will_struct->msg.topic, "$CONTROL/", strlen("$CONTROL/"))) {
    rc = TTQ_ERR_ACL_DENIED;
    goto error_cleanup;
  }
  rc = tmqtt_pub_topic_check(will_struct->msg.topic);
  if (rc) goto error_cleanup;

  rc = packet__read_uint16(&context->in_packet, &payloadlen);
  if (rc) goto error_cleanup;

  will_struct->msg.payloadlen = payloadlen;
  if (will_struct->msg.payloadlen > 0) {
    if (db.config->message_size_limit && will_struct->msg.payloadlen > (int)db.config->message_size_limit) {
      ttq_log(NULL, TTQ_LOG_DEBUG, "Client %s connected with too large Will payload", client_id);
      if (context->protocol == ttq_p_mqtt5) {
        ttqSendConnack(context, 0, MQTT_RC_PACKET_TOO_LARGE, NULL);
      } else {
        ttqSendConnack(context, 0, CONNACK_REFUSED_NOT_AUTHORIZED, NULL);
      }
      rc = TTQ_ERR_PAYLOAD_SIZE;
      goto error_cleanup;
    }
    will_struct->msg.payload = ttq_malloc((size_t)will_struct->msg.payloadlen);
    if (!will_struct->msg.payload) {
      rc = TTQ_ERR_NOMEM;
      goto error_cleanup;
    }

    rc = packet__read_bytes(&context->in_packet, will_struct->msg.payload, (uint32_t)will_struct->msg.payloadlen);
    if (rc) goto error_cleanup;
  }

  will_struct->msg.qos = will_qos;
  will_struct->msg.retain = will_retain;

  *will = will_struct;
  return TTQ_ERR_SUCCESS;

error_cleanup:
  if (will_struct) {
    ttq_free(will_struct->msg.topic);
    ttq_free(will_struct->msg.payload);
    tmqtt_property_free_all(&will_struct->properties);
    ttq_free(will_struct);
  }
  return rc;
}
*/
int ttqSendConnack(struct tmqtt *context, uint8_t ack, uint8_t reason_code, const tmqtt_property *properties) {
  struct tmqtt__packet *packet = NULL;
  int                   rc;
  tmqtt_property       *connack_props = NULL;
  uint32_t              remaining_length;

  rc = tmqtt_property_copy_all(&connack_props, properties);
  if (rc) {
    return rc;
  }

  if (context->id) {
    ttq_log(NULL, TTQ_LOG_DEBUG, "Sending CONNACK to %s (%d, %d)", context->id, ack, reason_code);
  } else {
    ttq_log(NULL, TTQ_LOG_DEBUG, "Sending CONNACK to %s (%d, %d)", context->address, ack, reason_code);
  }

  remaining_length = 2;

  if (context->protocol == ttq_p_mqtt5) {
    if (reason_code < 128 && db.config->retain_available == false) {
      rc = tmqtt_property_add_byte(&connack_props, MQTT_PROP_RETAIN_AVAILABLE, 0);
      if (rc) {
        tmqtt_property_free_all(&connack_props);
        return rc;
      }
    }
    if (reason_code < 128 && db.config->max_packet_size > 0) {
      rc = tmqtt_property_add_int32(&connack_props, MQTT_PROP_MAXIMUM_PACKET_SIZE, db.config->max_packet_size);
      if (rc) {
        tmqtt_property_free_all(&connack_props);
        return rc;
      }
    }
    if (reason_code < 128 && db.config->max_inflight_messages > 0) {
      rc = tmqtt_property_add_int16(&connack_props, MQTT_PROP_RECEIVE_MAXIMUM, db.config->max_inflight_messages);
      if (rc) {
        tmqtt_property_free_all(&connack_props);
        return rc;
      }
    }
    if (context->listener->max_qos != 2) {
      rc = tmqtt_property_add_byte(&connack_props, MQTT_PROP_MAXIMUM_QOS, context->listener->max_qos);
      if (rc) {
        tmqtt_property_free_all(&connack_props);
        return rc;
      }
    }

    remaining_length += property__get_remaining_length(connack_props);
  }

  if (packet__check_oversize(context, remaining_length)) {
    tmqtt_property_free_all(&connack_props);
    return TTQ_ERR_OVERSIZE_PACKET;
  }

  packet = ttq_calloc(1, sizeof(struct tmqtt__packet));
  if (!packet) {
    tmqtt_property_free_all(&connack_props);
    return TTQ_ERR_NOMEM;
  }

  packet->command = CMD_CONNACK;
  packet->remaining_length = remaining_length;

  rc = packet__alloc(packet);
  if (rc) {
    tmqtt_property_free_all(&connack_props);
    ttq_free(packet);
    return rc;
  }
  packet__write_byte(packet, ack);
  packet__write_byte(packet, reason_code);
  if (context->protocol == ttq_p_mqtt5) {
    property__write_all(packet, connack_props, true);
  }
  tmqtt_property_free_all(&connack_props);

  return packet__queue(context, packet);
}

/* Process the incoming properties, we should be able to assume that only valid
 * properties for CONNECT are present here. */
static int property__process_connect(struct tmqtt *context, tmqtt_property **props) {
  tmqtt_property *p;

  p = *props;

  while (p) {
    if (p->identifier == MQTT_PROP_SESSION_EXPIRY_INTERVAL) {
      context->session_expiry_interval = p->value.i32;
    } else if (p->identifier == MQTT_PROP_RECEIVE_MAXIMUM) {
      if (p->value.i16 == 0) {
        return TTQ_ERR_PROTOCOL;
      }

      context->msgs_out.inflight_maximum = p->value.i16;
      context->msgs_out.inflight_quota = context->msgs_out.inflight_maximum;
    } else if (p->identifier == MQTT_PROP_MAXIMUM_PACKET_SIZE) {
      if (p->value.i32 == 0) {
        return TTQ_ERR_PROTOCOL;
      }
      context->maximum_packet_size = p->value.i32;
    }
    p = p->next;
  }

  return TTQ_ERR_SUCCESS;
}

int ttqHandleConnect(struct tmqtt *context) {
  char                      protocol_name[7];
  uint8_t                   protocol_version;
  uint8_t                   connect_flags;
  char                     *client_id = NULL;
  struct tmqtt_message_all *will_struct = NULL;
  uint8_t                   will, will_retain, will_qos, clean_start;
  uint8_t                   username_flag, password_flag;
  char                     *username = NULL, *password = NULL;
  int                       rc;
  uint16_t                  slen;
  tmqtt_property           *properties = NULL;
  void                     *auth_data = NULL;
  uint16_t                  auth_data_len = 0;
  void                     *auth_data_out = NULL;
  uint16_t                  auth_data_out_len = 0;
  bool                      allow_zero_length_clientid = false;

  G_CONNECTION_COUNT_INC();

  if (!context->listener) {
    return TTQ_ERR_INVAL;
  }

  if (context->state != ttq_cs_new) {
    ttq_log(NULL, TTQ_LOG_NOTICE, "Bad client %s sending multiple CONNECT messages.", context->id);
    rc = TTQ_ERR_PROTOCOL;
    goto handle_connect_error;
  }

  if (packet__read_uint16(&context->in_packet, &slen)) {
    rc = TTQ_ERR_PROTOCOL;
    goto handle_connect_error;
  }
  if (slen != 4 /* MQTT */ && slen != 6 /* MQIsdp */) {
    rc = TTQ_ERR_PROTOCOL;
    goto handle_connect_error;
  }
  if (packet__read_bytes(&context->in_packet, protocol_name, slen)) {
    rc = TTQ_ERR_PROTOCOL;
    goto handle_connect_error;
  }
  protocol_name[slen] = '\0';

  if (packet__read_byte(&context->in_packet, &protocol_version)) {
    rc = TTQ_ERR_PROTOCOL;
    goto handle_connect_error;
  }
  if (!strcmp(protocol_name, PROTOCOL_NAME_v31)) {
    if ((protocol_version & 0x7F) != PROTOCOL_VERSION_v31) {
      if (db.config->connection_messages == true) {
        ttq_log(NULL, TTQ_LOG_INFO, "Invalid protocol version %d in CONNECT from %s.", protocol_version,
                context->address);
      }
      ttqSendConnack(context, 0, CONNACK_REFUSED_PROTOCOL_VERSION, NULL);
      rc = TTQ_ERR_PROTOCOL;
      goto handle_connect_error;
    }
    context->protocol = ttq_p_mqtt31;
    if ((protocol_version & 0x80) == 0x80) {
      context->is_bridge = true;
    }
  } else if (!strcmp(protocol_name, PROTOCOL_NAME)) {
    if ((protocol_version & 0x7F) == PROTOCOL_VERSION_v311) {
      context->protocol = ttq_p_mqtt311;

      if ((protocol_version & 0x80) == 0x80) {
        context->is_bridge = true;
      }
    } else if ((protocol_version & 0x7F) == PROTOCOL_VERSION_v5) {
      context->protocol = ttq_p_mqtt5;
    } else {
      if (db.config->connection_messages == true) {
        ttq_log(NULL, TTQ_LOG_INFO, "Invalid protocol version %d in CONNECT from %s.", protocol_version,
                context->address);
      }
      ttqSendConnack(context, 0, CONNACK_REFUSED_PROTOCOL_VERSION, NULL);
      rc = TTQ_ERR_PROTOCOL;
      goto handle_connect_error;
    }
    if ((context->in_packet.command & 0x0F) != 0x00) {
      /* Reserved flags not set to 0, must disconnect. */
      rc = TTQ_ERR_PROTOCOL;
      goto handle_connect_error;
    }
  } else {
    if (db.config->connection_messages == true) {
      ttq_log(NULL, TTQ_LOG_INFO, "Invalid protocol \"%s\" in CONNECT from %s.", protocol_name, context->address);
    }
    rc = TTQ_ERR_PROTOCOL;
    goto handle_connect_error;
  }
  if ((protocol_version & 0x7F) != PROTOCOL_VERSION_v31 && context->in_packet.command != CMD_CONNECT) {
    return TTQ_ERR_MALFORMED_PACKET;
  }

  if (packet__read_byte(&context->in_packet, &connect_flags)) {
    rc = TTQ_ERR_PROTOCOL;
    goto handle_connect_error;
  }
  if (context->protocol == ttq_p_mqtt311 || context->protocol == ttq_p_mqtt5) {
    if ((connect_flags & 0x01) != 0x00) {
      rc = TTQ_ERR_PROTOCOL;
      goto handle_connect_error;
    }
  }

  clean_start = (connect_flags & 0x02) >> 1;
  /* session_expiry_interval will be overriden if the properties are read later */
  if (clean_start == false && protocol_version != PROTOCOL_VERSION_v5) {
    /* v3* has clean_start == false mean the session never expires */
    context->session_expiry_interval = UINT32_MAX;
  } else {
    context->session_expiry_interval = 0;
  }
  will = connect_flags & 0x04;
  will_qos = (connect_flags & 0x18) >> 3;
  if (will_qos == 3) {
    ttq_log(NULL, TTQ_LOG_INFO, "Invalid Will QoS in CONNECT from %s.", context->address);
    rc = TTQ_ERR_PROTOCOL;
    goto handle_connect_error;
  }
  will_retain = ((connect_flags & 0x20) == 0x20);
  password_flag = connect_flags & 0x40;
  username_flag = connect_flags & 0x80;

  if (will && will_retain && db.config->retain_available == false) {
    if (protocol_version == ttq_p_mqtt5) {
      ttqSendConnack(context, 0, MQTT_RC_RETAIN_NOT_SUPPORTED, NULL);
    }
    rc = TTQ_ERR_NOT_SUPPORTED;
    goto handle_connect_error;
  }

  if (packet__read_uint16(&context->in_packet, &(context->keepalive))) {
    rc = TTQ_ERR_PROTOCOL;
    goto handle_connect_error;
  }

  if (protocol_version == PROTOCOL_VERSION_v5) {
    rc = property__read_all(CMD_CONNECT, &context->in_packet, &properties);
    if (rc) goto handle_connect_error;
  }
  property__process_connect(context, &properties);

  if (will && will_qos > context->listener->max_qos) {
    if (protocol_version == ttq_p_mqtt5) {
      ttqSendConnack(context, 0, MQTT_RC_QOS_NOT_SUPPORTED, NULL);
    }
    rc = TTQ_ERR_NOT_SUPPORTED;
    goto handle_connect_error;
  }

  if (tmqtt_property_read_string(properties, MQTT_PROP_AUTHENTICATION_METHOD, &context->auth_method, false)) {
    tmqtt_property_read_binary(properties, MQTT_PROP_AUTHENTICATION_DATA, &auth_data, &auth_data_len, false);
  }

  tmqtt_property_free_all(&properties);

  if (packet__read_string(&context->in_packet, &client_id, &slen)) {
    rc = TTQ_ERR_PROTOCOL;
    goto handle_connect_error;
  }

  if (slen == 0) {
    if (context->protocol == ttq_p_mqtt31) {
      ttqSendConnack(context, 0, CONNACK_REFUSED_IDENTIFIER_REJECTED, NULL);
      rc = TTQ_ERR_PROTOCOL;
      goto handle_connect_error;
    } else { /* mqtt311/mqtt5 */
      ttq_free(client_id);
      client_id = NULL;
      /*
      if (db.config->per_listener_settings) {
        allow_zero_length_clientid = context->listener->security_options.allow_zero_length_clientid;
      } else {
        allow_zero_length_clientid = db.config->security_options.allow_zero_length_clientid;
      }
      */
      if ((context->protocol == ttq_p_mqtt311 && clean_start == 0) || allow_zero_length_clientid == false) {
        if (context->protocol == ttq_p_mqtt311) {
          ttqSendConnack(context, 0, CONNACK_REFUSED_IDENTIFIER_REJECTED, NULL);
        } else {
          ttqSendConnack(context, 0, MQTT_RC_UNSPECIFIED, NULL);
        }
        rc = TTQ_ERR_PROTOCOL;
        goto handle_connect_error;
      } else {
        /*
        if (db.config->per_listener_settings) {
          client_id = client_id_gen(&slen, context->listener->security_options.auto_id_prefix,
                                    context->listener->security_options.auto_id_prefix_len);
        } else {
          client_id = client_id_gen(&slen, db.config->security_options.auto_id_prefix,
                                    db.config->security_options.auto_id_prefix_len);
        }
        */
        if (!client_id) {
          rc = TTQ_ERR_NOMEM;
          goto handle_connect_error;
        }
        context->assigned_id = true;
      }
    }
  }

  /* clientid_prefixes check */
  if (db.config->clientid_prefixes) {
    if (strncmp(db.config->clientid_prefixes, client_id, strlen(db.config->clientid_prefixes))) {
      if (context->protocol == ttq_p_mqtt5) {
        ttqSendConnack(context, 0, MQTT_RC_NOT_AUTHORIZED, NULL);
      } else {
        ttqSendConnack(context, 0, CONNACK_REFUSED_NOT_AUTHORIZED, NULL);
      }
      rc = TTQ_ERR_AUTH;
      goto handle_connect_error;
    }
  }

  if (will) {
    // rc = will__read(context, client_id, &will_struct, will_qos, will_retain);
    // if (rc) goto handle_connect_error;
  } else {
    if (context->protocol == ttq_p_mqtt311 || context->protocol == ttq_p_mqtt5) {
      if (will_qos != 0 || will_retain != 0) {
        rc = TTQ_ERR_PROTOCOL;
        goto handle_connect_error;
      }
    }
  }

  if (username_flag) {
    rc = packet__read_string(&context->in_packet, &username, &slen);
    if (rc == TTQ_ERR_NOMEM) {
      rc = TTQ_ERR_NOMEM;
      goto handle_connect_error;
    } else if (rc != TTQ_ERR_SUCCESS) {
      if (context->protocol == ttq_p_mqtt31) {
        /* Username flag given, but no username. Ignore. */
        username_flag = 0;
      } else {
        rc = TTQ_ERR_PROTOCOL;
        goto handle_connect_error;
      }
    }
  } else {
    if (context->protocol == ttq_p_mqtt311 || context->protocol == ttq_p_mqtt31) {
      if (password_flag) {
        /* username_flag == 0 && password_flag == 1 is forbidden */
        ttq_log(NULL, TTQ_LOG_ERR, "Protocol error from %s: password without username, closing connection.", client_id);
        rc = TTQ_ERR_PROTOCOL;
        goto handle_connect_error;
      }
    }
  }
  if (password_flag) {
    rc = packet__read_binary(&context->in_packet, (uint8_t **)&password, &slen);
    if (rc == TTQ_ERR_NOMEM) {
      rc = TTQ_ERR_NOMEM;
      goto handle_connect_error;
    } else if (rc == TTQ_ERR_MALFORMED_PACKET) {
      if (context->protocol == ttq_p_mqtt31) {
        /* Password flag given, but no password. Ignore. */
      } else {
        rc = TTQ_ERR_PROTOCOL;
        goto handle_connect_error;
      }
    }
  }

  if (context->in_packet.pos != context->in_packet.remaining_length) {
    /* Surplus data at end of packet, this must be an error. */
    rc = TTQ_ERR_PROTOCOL;
    goto handle_connect_error;
  }

  /* Once context->id is set, if we return from this function with an error
   * we must make sure that context->id is freed and set to NULL, so that the
   * client isn't erroneously removed from the by_id hash table. */
  context->id = client_id;
  client_id = NULL;

  {
    /* FIXME - these ensure the tmqttClientId() and
     * tmqttClientUsername() functions work, but is hacky */
    context->username = username;
    context->password = password;
    username = NULL; /* Avoid free() in error: below. */
    password = NULL;
  }

  if (context->listener->use_username_as_clientid) {
    if (context->username) {
      ttq_free(context->id);
      context->id = ttq_strdup(context->username);
      if (!context->id) {
        rc = TTQ_ERR_NOMEM;
        goto handle_connect_error;
      }
    } else {
      if (context->protocol == ttq_p_mqtt5) {
        ttqSendConnack(context, 0, MQTT_RC_NOT_AUTHORIZED, NULL);
      } else {
        ttqSendConnack(context, 0, CONNACK_REFUSED_NOT_AUTHORIZED, NULL);
      }
      rc = TTQ_ERR_AUTH;
      goto handle_connect_error;
    }
  }
  context->clean_start = clean_start;
  context->will = will_struct;
  will_struct = NULL;

  if (context->auth_method) {
    rc = TTQ_ERR_NOT_SUPPORTED;
    // Client requested extended authentication
    ttqSendConnack(context, 0, MQTT_RC_BAD_AUTHENTICATION_METHOD, NULL);
    // ttq_free(context->id);
    // context->id = NULL;
    goto handle_connect_error;
    /*
    rc = tmqtt_security_auth_start(context, false, auth_data, auth_data_len, &auth_data_out, &auth_data_out_len);
    ttq_free(auth_data);
    auth_data = NULL;
    if (rc == TTQ_ERR_SUCCESS) {
      return ttqCxtOnAuthorised(context, auth_data_out, auth_data_out_len);
    } else if (rc == TTQ_ERR_AUTH_CONTINUE) {
      tmqtt__set_state(context, ttq_cs_authenticating);
      rc = ttqSendAuth(context, MQTT_RC_CONTINUE_AUTHENTICATION, auth_data_out, auth_data_out_len);
      free(auth_data_out);
      return rc;
    } else {
      free(auth_data_out);
      auth_data_out = NULL;
      // will__clear(context);
      if (rc == TTQ_ERR_AUTH) {
        ttqSendConnack(context, 0, MQTT_RC_NOT_AUTHORIZED, NULL);
        ttq_free(context->id);
        context->id = NULL;
        goto handle_connect_error;
      } else if (rc == TTQ_ERR_NOT_SUPPORTED) {
        // Client has requested extended authentication, but we don't support it.
        ttqSendConnack(context, 0, MQTT_RC_BAD_AUTHENTICATION_METHOD, NULL);
        ttq_free(context->id);
        context->id = NULL;
        goto handle_connect_error;
      } else {
        ttq_free(context->id);
        context->id = NULL;
        goto handle_connect_error;
      }
    }*/
  } else {
    if (!tmq_ctx_auth(&context->tmq_context, context->username, context->password)) {
      ttqSendConnack(context, 0, MQTT_RC_NOT_AUTHORIZED, NULL);
      rc = TTQ_ERR_AUTH;
      goto handle_connect_error;
    }

    context->tmq_context.context = context;

    return ttqCxtOnAuthorised(context, NULL, 0);
  }

handle_connect_error:
  tmqtt_property_free_all(&properties);
  ttq_free(auth_data);
  ttq_free(client_id);
  ttq_free(username);
  ttq_free(password);
  if (will_struct) {
    tmqtt_property_free_all(&will_struct->properties);
    ttq_free(will_struct->msg.payload);
    ttq_free(will_struct->msg.topic);
    ttq_free(will_struct);
  }
  if (context->will) {
    tmqtt_property_free_all(&context->will->properties);
    ttq_free(context->will->msg.payload);
    ttq_free(context->will->msg.topic);
    ttq_free(context->will);
    context->will = NULL;
  }

  // We return an error here which means the client is freed later on.
  context->clean_start = true;
  context->session_expiry_interval = 0;
  context->will_delay_interval = 0;
  return rc;
}
