#include "tmqttBrokerInt.h"

#include <stdio.h>
#include <string.h>

#include "tmqttProto.h"
#include "ttqMemory.h"
#include "ttqPacket.h"
#include "ttqProperty.h"

static int ttq_send_suback(struct tmqtt *context, uint16_t mid, uint32_t payloadlen, const void *payload) {
  struct tmqtt__packet *packet = NULL;
  int                   rc;
  tmqtt_property       *properties = NULL;

  ttq_log(NULL, TTQ_LOG_DEBUG, "Sending SUBACK to %s", context->id);

  packet = ttq_calloc(1, sizeof(struct tmqtt__packet));
  if (!packet) return TTQ_ERR_NOMEM;

  packet->command = CMD_SUBACK;
  packet->remaining_length = 2 + payloadlen;
  if (context->protocol == ttq_p_mqtt5) {
    packet->remaining_length += property__get_remaining_length(properties);
  }
  rc = packet__alloc(packet);
  if (rc) {
    ttq_free(packet);
    return rc;
  }
  packet__write_uint16(packet, mid);

  if (context->protocol == ttq_p_mqtt5) {
    /* We don't use Reason String or User Property yet. */
    property__write_all(packet, properties, true);
  }

  if (payloadlen) {
    packet__write_bytes(packet, payload, payloadlen);
  }

  return packet__queue(context, packet);
}

int ttqHandleSub(struct tmqtt *context) {
  int             rc = 0;
  int             rc2;
  uint16_t        mid;
  char           *sub;
  uint8_t         subscription_options;
  uint32_t        subscription_identifier = 0;
  uint8_t         qos;
  uint8_t         retain_handling = 0;
  uint8_t        *payload = NULL, *tmp_payload;
  uint32_t        payloadlen = 0;
  size_t          len;
  uint16_t        slen;
  char           *sub_mount;
  tmqtt_property *properties = NULL;
  bool            allowed;
  bool            earliest = false;
  int             proto_id = TMQ_PROTO_ID_JSON;

  if (!context) return TTQ_ERR_INVAL;

  if (context->state != ttq_cs_active) {
    return TTQ_ERR_PROTOCOL;
  }
  if (context->in_packet.command != (CMD_SUBSCRIBE | 2)) {
    return TTQ_ERR_MALFORMED_PACKET;
  }

  ttq_log(NULL, TTQ_LOG_DEBUG, "Received SUBSCRIBE from %s", context->id);

  if (context->protocol != ttq_p_mqtt31) {
    if ((context->in_packet.command & 0x0F) != 0x02) {
      return TTQ_ERR_MALFORMED_PACKET;
    }
  }
  if (packet__read_uint16(&context->in_packet, &mid)) return TTQ_ERR_MALFORMED_PACKET;
  if (mid == 0) return TTQ_ERR_MALFORMED_PACKET;

  if (context->protocol == ttq_p_mqtt5) {
    rc = property__read_all(CMD_SUBSCRIBE, &context->in_packet, &properties);
    if (rc) {
      // Would be better to make property__read_all() return TTQ_ERR_MALFORMED_PACKET
      if (rc == TTQ_ERR_PROTOCOL) {
        return TTQ_ERR_MALFORMED_PACKET;
      } else {
        return rc;
      }
    }

    if (tmqtt_property_read_varint(properties, MQTT_PROP_SUBSCRIPTION_IDENTIFIER, &subscription_identifier, false)) {
      if (subscription_identifier == 0) {
        // The identifier was set to 0, this is an error
        tmqtt_property_free_all(&properties);
        return TTQ_ERR_MALFORMED_PACKET;
      }
    }

    // read user properties in the original properties, new protos may be parsed here as well.
    char                 *strname = NULL, *strvalue = NULL;
    const tmqtt_property *prop =
        tmqtt_property_read_string_pair(properties, MQTT_PROP_USER_PROPERTY, &strname, &strvalue, false);
    while (prop) {
      if (!strcmp(strname, "sub-offset") && !strcmp(strvalue, "earliest")) {
        earliest = true;
      } else if (!strcmp(strname, "proto") && !strcmp(strvalue, "rawblock")) {
        proto_id = TMQ_PROTO_ID_RAWB;
      }

      free(strname);
      free(strvalue);
      strname = NULL;
      strvalue = NULL;

      prop = tmqtt_property_read_string_pair(prop, MQTT_PROP_USER_PROPERTY, &strname, &strvalue, true);
    }

    tmqtt_property_free_all(&properties);
  }

  while (context->in_packet.pos < context->in_packet.remaining_length) {
    sub = NULL;
    if (packet__read_string(&context->in_packet, &sub, &slen)) {
      ttq_free(payload);
      return TTQ_ERR_MALFORMED_PACKET;
    }

    if (sub) {
      if (!slen) {
        ttq_log(NULL, TTQ_LOG_INFO, "Empty subscription string from %s, disconnecting.", context->address);
        ttq_free(sub);
        ttq_free(payload);
        return TTQ_ERR_MALFORMED_PACKET;
      }
      if (tmqtt_sub_topic_check(sub)) {
        ttq_log(NULL, TTQ_LOG_INFO, "Invalid subscription string from %s, disconnecting.", context->address);
        ttq_free(sub);
        ttq_free(payload);
        return TTQ_ERR_MALFORMED_PACKET;
      }

      if (packet__read_byte(&context->in_packet, &subscription_options)) {
        ttq_free(sub);
        ttq_free(payload);
        return TTQ_ERR_MALFORMED_PACKET;
      }
      if (context->protocol == ttq_p_mqtt31 || context->protocol == ttq_p_mqtt311) {
        qos = subscription_options;
        if (context->is_bridge) {
          subscription_options = MQTT_SUB_OPT_RETAIN_AS_PUBLISHED | MQTT_SUB_OPT_NO_LOCAL;
        }
      } else {
        qos = subscription_options & 0x03;
        subscription_options &= 0xFC;

        if ((subscription_options & MQTT_SUB_OPT_NO_LOCAL) && !strncmp(sub, "$share/", 7)) {
          ttq_free(sub);
          ttq_free(payload);
          return TTQ_ERR_PROTOCOL;
        }
        retain_handling = (subscription_options & 0x30);
        if (retain_handling == 0x30 || (subscription_options & 0xC0) != 0) {
          ttq_free(sub);
          ttq_free(payload);
          return TTQ_ERR_MALFORMED_PACKET;
        }
      }
      if (qos > 2) {
        ttq_log(NULL, TTQ_LOG_INFO, "Invalid QoS in subscription command from %s, disconnecting.", context->address);
        ttq_free(sub);
        ttq_free(payload);
        return TTQ_ERR_MALFORMED_PACKET;
      }
      if (qos > context->max_qos) {
        qos = context->max_qos;
      }

      if (context->listener && context->listener->mount_point) {
        len = strlen(context->listener->mount_point) + slen + 1;
        sub_mount = ttq_malloc(len + 1);
        if (!sub_mount) {
          ttq_free(sub);
          ttq_free(payload);
          return TTQ_ERR_NOMEM;
        }
        snprintf(sub_mount, len, "%s%s", context->listener->mount_point, sub);
        sub_mount[len] = '\0';

        ttq_free(sub);
        sub = sub_mount;
      }
      ttq_log(NULL, TTQ_LOG_DEBUG, "\t%s (QoS %d)", sub, qos);

      allowed = true;
      /*
      rc2 = tmqttAclCheck(context, sub, 0, NULL, qos, false, TTQ_ACL_SUBSCRIBE);
      switch (rc2) {
        case TTQ_ERR_SUCCESS:
          break;
        case TTQ_ERR_NOT_FOUND:
        case TTQ_ERR_ACL_DENIED:
          allowed = false;
          if (context->protocol == ttq_p_mqtt5) {
            if (rc2 == TTQ_ERR_NOT_FOUND) {
              qos = MQTT_RC_TOPIC_NAME_INVALID;
            } else {
              qos = MQTT_RC_NOT_AUTHORIZED;
            }
          } else if (context->protocol == ttq_p_mqtt311) {
            qos = 0x80;
          }
          break;
        default:
          ttq_free(sub);
          return rc2;
      }
      */
      {
        const char *sharename = NULL;
        char       *local_sub;
        char      **topics;
        size_t      topiclen;

        rc2 = ttqSubTopicTokenise(sub, &local_sub, &topics, &sharename);
        if (rc2 > 0) {
          ttq_free(local_sub);
          ttq_free(topics);
          ttq_free(sub);
          return rc2;
        }

        topiclen = strlen(topics[0]);
        if (topiclen > UINT16_MAX) {
          ttq_free(local_sub);
          ttq_free(topics);
          ttq_free(sub);
          return TTQ_ERR_INVAL;
        }

        if (!tmq_ctx_topic_exists(&context->tmq_context, topics[1], context->id, sharename, earliest, proto_id, qos)) {
          qos = MQTT_RC_TOPIC_NAME_INVALID;
          qos = MQTT_RC_TOPIC_FILTER_INVALID;
          allowed = false;
        }

        ttq_free(local_sub);
        ttq_free(topics);
      }

      if (allowed) {
        rc2 = ttqSubAdd(context, sub, qos, subscription_identifier, subscription_options);
        if (rc2 > 0) {
          ttq_free(sub);
          return rc2;
        }
        /*
        if (context->protocol == ttq_p_mqtt311 || context->protocol == ttq_p_mqtt31) {
          if (rc2 == TTQ_ERR_SUCCESS || rc2 == TTQ_ERR_SUB_EXISTS) {
            if (retain__queue(context, sub, qos, 0)) rc = 1;
          }
        } else {
          if ((retain_handling == MQTT_SUB_OPT_SEND_RETAIN_ALWAYS) ||
              (rc2 == TTQ_ERR_SUCCESS && retain_handling == MQTT_SUB_OPT_SEND_RETAIN_NEW)) {
            if (retain__queue(context, sub, qos, subscription_identifier)) rc = 1;
          }
        }
        */
        ttq_log(NULL, TTQ_LOG_SUBSCRIBE, "%s %d %s", context->id, qos, sub);
      }
      ttq_free(sub);

      tmp_payload = ttq_realloc(payload, payloadlen + 1);
      if (tmp_payload) {
        payload = tmp_payload;
        payload[payloadlen] = qos;
        payloadlen++;
      } else {
        ttq_free(payload);

        return TTQ_ERR_NOMEM;
      }
    }
  }

  if (context->protocol != ttq_p_mqtt31) {
    if (payloadlen == 0) {
      /* No subscriptions specified, protocol error. */
      return TTQ_ERR_MALFORMED_PACKET;
    }
  }
  if (ttq_send_suback(context, mid, payloadlen, payload)) rc = 1;
  ttq_free(payload);

#ifdef WITH_PERSISTENCE
  db.persistence_changes++;
#endif

  if (context->current_out_packet == NULL) {
    rc = ttqDbMessageWriteQueuedOut(context);
    if (rc) return rc;
    rc = ttqDbMessageWriteInflightOutLatest(context);
    if (rc) return rc;
  }

  return rc;
}

static int ttq_send_unsuback(struct tmqtt *ttq, uint16_t mid, int reason_code_count, uint8_t *reason_codes,
                             const tmqtt_property *properties) {
  struct tmqtt__packet *packet = NULL;
  int                   rc;

  packet = ttq_calloc(1, sizeof(struct tmqtt__packet));
  if (!packet) return TTQ_ERR_NOMEM;

  packet->command = CMD_UNSUBACK;
  packet->remaining_length = 2;

  if (ttq->protocol == ttq_p_mqtt5) {
    packet->remaining_length += property__get_remaining_length(properties);
    packet->remaining_length += (uint32_t)reason_code_count;
  }

  rc = packet__alloc(packet);
  if (rc) {
    ttq_free(packet);
    return rc;
  }

  packet__write_uint16(packet, mid);

  if (ttq->protocol == ttq_p_mqtt5) {
    property__write_all(packet, properties, true);
    packet__write_bytes(packet, reason_codes, (uint32_t)reason_code_count);
  }

  return packet__queue(ttq, packet);
}

int ttqHandleUnsub(struct tmqtt *context) {
  uint16_t        mid;
  char           *sub;
  uint16_t        slen;
  int             rc;
  uint8_t         reason = 0;
  int             reason_code_count = 0;
  int             reason_code_max;
  uint8_t        *reason_codes = NULL, *reason_tmp;
  tmqtt_property *properties = NULL;
  bool            allowed;

  if (!context) return TTQ_ERR_INVAL;

  if (context->state != ttq_cs_active) {
    return TTQ_ERR_PROTOCOL;
  }
  if (context->in_packet.command != (CMD_UNSUBSCRIBE | 2)) {
    return TTQ_ERR_MALFORMED_PACKET;
  }
  ttq_log(NULL, TTQ_LOG_DEBUG, "Received UNSUBSCRIBE from %s", context->id);

  if (context->protocol != ttq_p_mqtt31) {
    if ((context->in_packet.command & 0x0F) != 0x02) {
      return TTQ_ERR_MALFORMED_PACKET;
    }
  }
  if (packet__read_uint16(&context->in_packet, &mid)) return TTQ_ERR_MALFORMED_PACKET;
  if (mid == 0) return TTQ_ERR_MALFORMED_PACKET;

  if (context->protocol == ttq_p_mqtt5) {
    rc = property__read_all(CMD_UNSUBSCRIBE, &context->in_packet, &properties);
    if (rc) {
      /* FIXME - it would be better if property__read_all() returned
       * TTQ_ERR_MALFORMED_PACKET, but this is would change the library
       * return codes so needs doc changes as well. */
      if (rc == TTQ_ERR_PROTOCOL) {
        return TTQ_ERR_MALFORMED_PACKET;
      } else {
        return rc;
      }
    }
    /* Immediately free, we don't do anything with User Property at the moment */
    tmqtt_property_free_all(&properties);
  }

  if (context->protocol == ttq_p_mqtt311 || context->protocol == ttq_p_mqtt5) {
    if (context->in_packet.pos == context->in_packet.remaining_length) {
      /* No topic specified, protocol error. */
      return TTQ_ERR_MALFORMED_PACKET;
    }
  }

  reason_code_max = 10;
  reason_codes = ttq_malloc((size_t)reason_code_max);
  if (!reason_codes) {
    return TTQ_ERR_NOMEM;
  }

  while (context->in_packet.pos < context->in_packet.remaining_length) {
    sub = NULL;
    if (packet__read_string(&context->in_packet, &sub, &slen)) {
      ttq_free(reason_codes);
      return TTQ_ERR_MALFORMED_PACKET;
    }

    if (!slen) {
      ttq_log(NULL, TTQ_LOG_INFO, "Empty unsubscription string from %s, disconnecting.", context->id);
      ttq_free(sub);
      ttq_free(reason_codes);
      return TTQ_ERR_MALFORMED_PACKET;
    }
    if (tmqtt_sub_topic_check(sub)) {
      ttq_log(NULL, TTQ_LOG_INFO, "Invalid unsubscription string from %s, disconnecting.", context->id);
      ttq_free(sub);
      ttq_free(reason_codes);
      return TTQ_ERR_MALFORMED_PACKET;
    }

    /* ACL check */
    allowed = true;
    rc = tmqttAclCheck(context, sub, 0, NULL, 0, false, TTQ_ACL_UNSUBSCRIBE);
    switch (rc) {
      case TTQ_ERR_SUCCESS:
        break;
      case TTQ_ERR_ACL_DENIED:
        allowed = false;
        reason = MQTT_RC_NOT_AUTHORIZED;
        break;
      default:
        ttq_free(sub);
        ttq_free(reason_codes);
        return rc;
    }

    {
      const char *sharename = NULL;
      char       *local_sub;
      char      **topics;
      size_t      topiclen;
      int         rc2;

      rc2 = ttqSubTopicTokenise(sub, &local_sub, &topics, &sharename);
      if (rc2 > 0) {
        ttq_free(local_sub);
        ttq_free(topics);
        ttq_free(sub);
        return rc2;
      }

      topiclen = strlen(topics[0]);
      if (topiclen > UINT16_MAX) {
        ttq_free(local_sub);
        ttq_free(topics);
        ttq_free(sub);
        return TTQ_ERR_INVAL;
      }

      if (!tmq_ctx_unsub_topic(&context->tmq_context, topics[1], context->id, sharename)) {
        reason = MQTT_RC_TOPIC_NAME_INVALID;

        allowed = false;
      }

      ttq_free(local_sub);
      ttq_free(topics);
    }

    ttq_log(NULL, TTQ_LOG_DEBUG, "\t%s", sub);
    if (allowed) {
      rc = ttqSubRemmove(context, sub, &reason);
    } else {
      rc = TTQ_ERR_SUCCESS;
    }
    ttq_log(NULL, TTQ_LOG_UNSUBSCRIBE, "%s %s", context->id, sub);
    ttq_free(sub);
    if (rc) {
      ttq_free(reason_codes);
      return rc;
    }

    reason_codes[reason_code_count] = reason;
    reason_code_count++;
    if (reason_code_count == reason_code_max) {
      reason_tmp = ttq_realloc(reason_codes, (size_t)(reason_code_max * 2));
      if (!reason_tmp) {
        ttq_free(reason_codes);
        return TTQ_ERR_NOMEM;
      }
      reason_codes = reason_tmp;
      reason_code_max *= 2;
    }
  }
#ifdef WITH_PERSISTENCE
  db.persistence_changes++;
#endif

  ttq_log(NULL, TTQ_LOG_DEBUG, "Sending UNSUBACK to %s", context->id);

  rc = ttq_send_unsuback(context, mid, reason_code_count, reason_codes, NULL);
  ttq_free(reason_codes);
  return rc;
}
