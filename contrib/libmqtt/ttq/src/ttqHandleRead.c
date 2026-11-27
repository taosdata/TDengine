#include "tmqttBrokerInt.h"

#include <stdio.h>
#include <string.h>

#include "tmqttProto.h"
#include "ttqPacket.h"
#include "ttqSend.h"
#include "ttqUtil.h"

static int ttq_handle_pingreq(struct tmqtt *ttq) {
  if (tmqtt__get_state(ttq) != ttq_cs_active) {
    return TTQ_ERR_PROTOCOL;
  }

  ttq_log(NULL, TTQ_LOG_DEBUG, "Received PINGREQ from %s", SAFE_PRINT(ttq->id));

  return ttq_send_pingresp(ttq);
}

static int ttq_handle_puback(struct tmqtt *ttq) {
  uint8_t         reason_code = 0;
  uint16_t        mid;
  int             rc;
  tmqtt_property *properties = NULL;
  int             qos;

  if (tmqtt__get_state(ttq) != ttq_cs_active) {
    return TTQ_ERR_PROTOCOL;
  }
  if (ttq->protocol != ttq_p_mqtt31) {
    if ((ttq->in_packet.command & 0x0F) != 0x00) {
      return TTQ_ERR_MALFORMED_PACKET;
    }
  }

  ttq_pthread_mutex_lock(&ttq->msgs_out.mutex);
  util__increment_send_quota(ttq);
  ttq_pthread_mutex_unlock(&ttq->msgs_out.mutex);

  rc = packet__read_uint16(&ttq->in_packet, &mid);
  if (rc) return rc;

  if (ttq->in_packet.command != CMD_PUBACK) {
    return TTQ_ERR_MALFORMED_PACKET;
  }
  qos = 1;
  if (mid == 0) {
    return TTQ_ERR_PROTOCOL;
  }

  if (ttq->protocol == ttq_p_mqtt5 && ttq->in_packet.remaining_length > 2) {
    rc = packet__read_byte(&ttq->in_packet, &reason_code);
    if (rc) {
      return rc;
    }

    if (ttq->in_packet.remaining_length > 3) {
      rc = property__read_all(CMD_PUBACK, &ttq->in_packet, &properties);
      if (rc) return rc;
    }

    if (reason_code != MQTT_RC_SUCCESS && reason_code != MQTT_RC_NO_MATCHING_SUBSCRIBERS &&
        reason_code != MQTT_RC_UNSPECIFIED && reason_code != MQTT_RC_IMPLEMENTATION_SPECIFIC &&
        reason_code != MQTT_RC_NOT_AUTHORIZED && reason_code != MQTT_RC_TOPIC_NAME_INVALID &&
        reason_code != MQTT_RC_PACKET_ID_IN_USE && reason_code != MQTT_RC_QUOTA_EXCEEDED &&
        reason_code != MQTT_RC_PAYLOAD_FORMAT_INVALID) {
      tmqtt_property_free_all(&properties);
      return TTQ_ERR_PROTOCOL;
    }
  }
  if (ttq->in_packet.pos < ttq->in_packet.remaining_length) {
    tmqtt_property_free_all(&properties);
    return TTQ_ERR_MALFORMED_PACKET;
  }

  ttq_log(NULL, TTQ_LOG_DEBUG, "Received %s from %s (Mid: %d, RC:%d)", "PUBACK", SAFE_PRINT(ttq->id), mid, reason_code);

  tmqtt_property_free_all(&properties);

  rc = ttqDbMessageDeleteOutgoing(ttq, mid, ttq_ms_wait_for_pubcomp, qos);
  if (rc == TTQ_ERR_NOT_FOUND) {
    ttq_log(ttq, TTQ_LOG_WARNING, "Warning: Received %s from %s for an unknown packet identifier %d.", "PUBACK",
            SAFE_PRINT(ttq->id), mid);
    return TTQ_ERR_SUCCESS;
  } else {
    return rc;
  }
}

static int property__process_disconnect(struct tmqtt *context, tmqtt_property **props) {
  tmqtt_property *p;
  for (p = *props; p; p = p->next) {
    if (p->identifier == MQTT_PROP_SESSION_EXPIRY_INTERVAL) {
      if (context->session_expiry_interval == 0 && p->value.i32 != 0) {
        return TTQ_ERR_PROTOCOL;
      }

      context->session_expiry_interval = p->value.i32;
    }
  }

  return TTQ_ERR_SUCCESS;
}

static int ttq_handle_disconnect(struct tmqtt *context) {
  int             rc;
  uint8_t         reason_code = 0;
  tmqtt_property *properties = NULL;

  if (!context) {
    return TTQ_ERR_INVAL;
  }

  if (context->in_packet.command != CMD_DISCONNECT) {
    return TTQ_ERR_MALFORMED_PACKET;
  }

  if (context->protocol == ttq_p_mqtt5 && context->in_packet.remaining_length > 0) {
    // TODO: handle reason code
    rc = packet__read_byte(&context->in_packet, &reason_code);
    if (rc) return rc;

    if (context->in_packet.remaining_length > 1) {
      rc = property__read_all(CMD_DISCONNECT, &context->in_packet, &properties);
      if (rc) return rc;
    }
  }
  rc = property__process_disconnect(context, &properties);
  if (rc) {
    tmqtt_property_free_all(&properties);
    return rc;
  }
  tmqtt_property_free_all(&properties);  // ignore all properties

  if (context->in_packet.pos != context->in_packet.remaining_length) {
    return TTQ_ERR_PROTOCOL;
  }
  ttq_log(NULL, TTQ_LOG_DEBUG, "Received DISCONNECT from %s", context->id);
  if (context->protocol == ttq_p_mqtt311 || context->protocol == ttq_p_mqtt5) {
    if ((context->in_packet.command & 0x0F) != 0x00) {
      ttqDisconnect(context, TTQ_ERR_PROTOCOL);
      return TTQ_ERR_PROTOCOL;
    }
  }

  if (reason_code == MQTT_RC_DISCONNECT_WITH_WILL_MSG) {
    tmqtt__set_state(context, ttq_cs_disconnect_with_will);
  } else {
    // clear will
    tmqtt__set_state(context, ttq_cs_disconnecting);
  }

  ttqDisconnect(context, TTQ_ERR_SUCCESS);

  return TTQ_ERR_SUCCESS;
}

int ttqHandlePacket(struct tmqtt *context) {
  int rc = TTQ_ERR_INVAL;

  if (!context) return TTQ_ERR_INVAL;

  switch ((context->in_packet.command) & 0xF0) {
    case CMD_PINGREQ:
      rc = ttq_handle_pingreq(context);
      break;
    case CMD_PUBACK:
      rc = ttq_handle_puback(context);
      break;
    case CMD_CONNECT:
      return ttqHandleConnect(context);
    case CMD_DISCONNECT:
      rc = ttq_handle_disconnect(context);
      break;
    case CMD_SUBSCRIBE:
      rc = ttqHandleSub(context);
      break;
    case CMD_UNSUBSCRIBE:
      rc = ttqHandleUnsub(context);
      break;
    case CMD_PINGRESP:
    case CMD_PUBCOMP:
    case CMD_PUBLISH:
    case CMD_PUBREC:
    case CMD_PUBREL:
    case CMD_AUTH:
    default:
      rc = TTQ_ERR_PROTOCOL;
  }

  if (ttq_p_mqtt5 == context->protocol) {
    if (TTQ_ERR_PROTOCOL == rc || TTQ_ERR_DUPLICATE_PROPERTY == rc) {
      ttq_send_disconnect(context, MQTT_RC_PROTOCOL_ERROR, NULL);
    } else if (TTQ_ERR_MALFORMED_PACKET == rc) {
      ttq_send_disconnect(context, MQTT_RC_MALFORMED_PACKET, NULL);
    } else if (TTQ_ERR_QOS_NOT_SUPPORTED == rc) {
      ttq_send_disconnect(context, MQTT_RC_QOS_NOT_SUPPORTED, NULL);
    } else if (TTQ_ERR_RETAIN_NOT_SUPPORTED == rc) {
      ttq_send_disconnect(context, MQTT_RC_RETAIN_NOT_SUPPORTED, NULL);
    } else if (TTQ_ERR_TOPIC_ALIAS_INVALID == rc) {
      ttq_send_disconnect(context, MQTT_RC_TOPIC_ALIAS_INVALID, NULL);
    } else if (TTQ_ERR_UNKNOWN == rc || TTQ_ERR_NOMEM == rc) {
      ttq_send_disconnect(context, MQTT_RC_UNSPECIFIED, NULL);
    }
  }

  return rc;
}
