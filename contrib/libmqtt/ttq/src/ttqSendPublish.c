#include "ttqSend.h"

#include <string.h>

#include "tmqttBrokerInt.h"

#include "tmqttInt.h"
#include "tmqttProto.h"
#include "ttq.h"
#include "ttqLogging.h"
#include "ttqMemory.h"
#include "ttqNet.h"
#include "ttqPacket.h"
#include "ttqProperty.h"
#include "ttqSystree.h"

int send__publish(struct tmqtt *ttq, uint16_t mid, const char *topic, uint32_t payloadlen, const void *payload,
                  uint8_t qos, bool retain, bool dup, const tmqtt_property *cmsg_props,
                  const tmqtt_property *store_props, uint32_t expiry_interval) {
#ifdef WITH_BROKER
  size_t len;
#ifdef WITH_BRIDGE
  int                         i;
  struct tmqtt__bridge_topic *cur_topic;
  bool                        match;
  int                         rc;
  char                       *mapped_topic = NULL;
  char                       *topic_temp = NULL;
#endif
#endif

#if defined(WITH_BROKER) && defined(WITH_WEBSOCKETS)
  if (ttq->sock == INVALID_SOCKET && !ttq->wsi) return TTQ_ERR_NO_CONN;
#else
  if (ttq->sock == INVALID_SOCKET) return TTQ_ERR_NO_CONN;
#endif

  if (!ttq->retain_available) {
    retain = false;
  }

#ifdef WITH_BROKER
  if (ttq->listener && ttq->listener->mount_point) {
    len = strlen(ttq->listener->mount_point);
    if (len < strlen(topic)) {
      topic += len;
    } else {
      /* Invalid topic string. Should never happen, but silently swallow the message anyway. */
      return TTQ_ERR_SUCCESS;
    }
  }
#ifdef WITH_BRIDGE
  if (ttq->bridge && ttq->bridge->topics && ttq->bridge->topic_remapping) {
    for (i = 0; i < ttq->bridge->topic_count; i++) {
      cur_topic = &ttq->bridge->topics[i];
      if ((cur_topic->direction == bd_both || cur_topic->direction == bd_out) &&
          (cur_topic->remote_prefix || cur_topic->local_prefix)) {
        /* Topic mapping required on this topic if the message matches */

        rc = tmqtt_topic_matches_sub(cur_topic->local_topic, topic, &match);
        if (rc) {
          return rc;
        }
        if (match) {
          mapped_topic = ttq_strdup(topic);
          if (!mapped_topic) return TTQ_ERR_NOMEM;
          if (cur_topic->local_prefix) {
            /* This prefix needs removing. */
            if (!strncmp(cur_topic->local_prefix, mapped_topic, strlen(cur_topic->local_prefix))) {
              topic_temp = ttq_strdup(mapped_topic + strlen(cur_topic->local_prefix));
              ttq_free(mapped_topic);
              if (!topic_temp) {
                return TTQ_ERR_NOMEM;
              }
              mapped_topic = topic_temp;
            }
          }

          if (cur_topic->remote_prefix) {
            /* This prefix needs adding. */
            len = strlen(mapped_topic) + strlen(cur_topic->remote_prefix) + 1;
            topic_temp = ttq_malloc(len + 1);
            if (!topic_temp) {
              ttq_free(mapped_topic);
              return TTQ_ERR_NOMEM;
            }
            snprintf(topic_temp, len, "%s%s", cur_topic->remote_prefix, mapped_topic);
            topic_temp[len] = '\0';
            ttq_free(mapped_topic);
            mapped_topic = topic_temp;
          }
          ttq_log(NULL, TTQ_LOG_DEBUG, "Sending PUBLISH to %s (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))",
                  SAFE_PRINT(ttq->id), dup, qos, retain, mid, mapped_topic, (long)payloadlen);
          G_PUB_BYTES_SENT_INC(payloadlen);
          rc = send__real_publish(ttq, mid, mapped_topic, payloadlen, payload, qos, retain, dup, cmsg_props,
                                  store_props, expiry_interval);
          ttq_free(mapped_topic);
          return rc;
        }
      }
    }
  }
#endif
  ttq_log(NULL, TTQ_LOG_DEBUG, "Sending PUBLISH to %s (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))", SAFE_PRINT(ttq->id),
          dup, qos, retain, mid, topic, (long)payloadlen);
  G_PUB_BYTES_SENT_INC(payloadlen);
#else
  ttq_log(ttq, TTQ_LOG_DEBUG, "Client %s sending PUBLISH (d%d, q%d, r%d, m%d, '%s', ... (%ld bytes))",
          SAFE_PRINT(ttq->id), dup, qos, retain, mid, topic, (long)payloadlen);
#endif

  return send__real_publish(ttq, mid, topic, payloadlen, payload, qos, retain, dup, cmsg_props, store_props,
                            expiry_interval);
}

int send__real_publish(struct tmqtt *ttq, uint16_t mid, const char *topic, uint32_t payloadlen, const void *payload,
                       uint8_t qos, bool retain, bool dup, const tmqtt_property *cmsg_props,
                       const tmqtt_property *store_props, uint32_t expiry_interval) {
  struct tmqtt__packet *packet = NULL;
  unsigned int          packetlen;
  unsigned int          proplen = 0, varbytes;
  int                   rc;
  tmqtt_property        expiry_prop;

  if (topic) {
    packetlen = 2 + (unsigned int)strlen(topic) + payloadlen;
  } else {
    packetlen = 2 + payloadlen;
  }
  if (qos > 0) packetlen += 2; /* For message id */
  if (ttq->protocol == ttq_p_mqtt5) {
    proplen = 0;
    proplen += property__get_length_all(cmsg_props);
    proplen += property__get_length_all(store_props);
    if (expiry_interval > 0) {
      expiry_prop.next = NULL;
      expiry_prop.value.i32 = expiry_interval;
      expiry_prop.identifier = MQTT_PROP_MESSAGE_EXPIRY_INTERVAL;
      expiry_prop.client_generated = false;

      proplen += property__get_length_all(&expiry_prop);
    }

    varbytes = packet__varint_bytes(proplen);
    if (varbytes > 4) {
      /* FIXME - Properties too big, don't publish any - should remove some first really */
      cmsg_props = NULL;
      store_props = NULL;
      expiry_interval = 0;
    } else {
      packetlen += proplen + varbytes;
    }
  }
  if (packet__check_oversize(ttq, packetlen)) {
#ifdef WITH_BROKER
    ttq_log(NULL, TTQ_LOG_NOTICE, "Dropping too large outgoing PUBLISH for %s (%d bytes)", SAFE_PRINT(ttq->id),
            packetlen);
#else
    ttq_log(ttq, TTQ_LOG_NOTICE, "Dropping too large outgoing PUBLISH (%d bytes)", packetlen);
#endif
    return TTQ_ERR_OVERSIZE_PACKET;
  }

  packet = ttq_calloc(1, sizeof(struct tmqtt__packet));
  if (!packet) return TTQ_ERR_NOMEM;

  packet->mid = mid;
  packet->command = (uint8_t)(CMD_PUBLISH | (uint8_t)((dup & 0x1) << 3) | (uint8_t)(qos << 1) | retain);
  packet->remaining_length = packetlen;
  rc = packet__alloc(packet);
  if (rc) {
    ttq_free(packet);
    return rc;
  }
  /* Variable header (topic string) */
  if (topic) {
    packet__write_string(packet, topic, (uint16_t)strlen(topic));
  } else {
    packet__write_uint16(packet, 0);
  }
  if (qos > 0) {
    packet__write_uint16(packet, mid);
  }

  if (ttq->protocol == ttq_p_mqtt5) {
    packet__write_varint(packet, proplen);
    property__write_all(packet, cmsg_props, false);
    property__write_all(packet, store_props, false);
    if (expiry_interval > 0) {
      property__write_all(packet, &expiry_prop, false);
    }
  }

  /* Payload */
  if (payloadlen) {
    packet__write_bytes(packet, payload, payloadlen);
  }

  return packet__queue(ttq, packet);
}
