#include "ttqSend.h"

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
#include "ttqTime.h"
#include "ttqUtil.h"

int send__pingreq(struct tmqtt *ttq) {
  int rc;

  ttq_log(NULL, TTQ_LOG_DEBUG, "Sending PINGREQ to %s", SAFE_PRINT(ttq->id));

  rc = send__simple_command(ttq, CMD_PINGREQ);
  if (rc == TTQ_ERR_SUCCESS) {
    ttq->ping_t = tmqtt_time();
  }
  return rc;
}

int ttq_send_pingresp(struct tmqtt *ttq) {
  ttq_log(NULL, TTQ_LOG_DEBUG, "Sending PINGRESP to %s", SAFE_PRINT(ttq->id));

  return send__simple_command(ttq, CMD_PINGRESP);
}

int send__puback(struct tmqtt *ttq, uint16_t mid, uint8_t reason_code, const tmqtt_property *properties) {
  ttq_log(NULL, TTQ_LOG_DEBUG, "Sending PUBACK to %s (m%d, rc%d)", SAFE_PRINT(ttq->id), mid, reason_code);

  util__increment_receive_quota(ttq);

  return send__command_with_mid(ttq, CMD_PUBACK, mid, false, reason_code, properties);
}

int send__pubcomp(struct tmqtt *ttq, uint16_t mid, const tmqtt_property *properties) {
  ttq_log(NULL, TTQ_LOG_DEBUG, "Sending PUBCOMP to %s (m%d)", SAFE_PRINT(ttq->id), mid);

  util__increment_receive_quota(ttq);

  return send__command_with_mid(ttq, CMD_PUBCOMP, mid, false, 0, properties);
}

int send__pubrec(struct tmqtt *ttq, uint16_t mid, uint8_t reason_code, const tmqtt_property *properties) {
  ttq_log(NULL, TTQ_LOG_DEBUG, "Sending PUBREC to %s (m%d, rc%d)", SAFE_PRINT(ttq->id), mid, reason_code);

  if (reason_code >= 0x80 && ttq->protocol == ttq_p_mqtt5) {
    util__increment_receive_quota(ttq);
  }

  return send__command_with_mid(ttq, CMD_PUBREC, mid, false, reason_code, properties);
}

int send__pubrel(struct tmqtt *ttq, uint16_t mid, const tmqtt_property *properties) {
  ttq_log(NULL, TTQ_LOG_DEBUG, "Sending PUBREL to %s (m%d)", SAFE_PRINT(ttq->id), mid);

  return send__command_with_mid(ttq, CMD_PUBREL | 2, mid, false, 0, properties);
}

/* For PUBACK, PUBCOMP, PUBREC, and PUBREL */
int send__command_with_mid(struct tmqtt *ttq, uint8_t command, uint16_t mid, bool dup, uint8_t reason_code,
                           const tmqtt_property *properties) {
  struct tmqtt__packet *packet = NULL;
  int                   rc;

  packet = ttq_calloc(1, sizeof(struct tmqtt__packet));
  if (!packet) return TTQ_ERR_NOMEM;

  packet->command = command;
  if (dup) {
    packet->command |= 8;
  }
  packet->remaining_length = 2;

  if (ttq->protocol == ttq_p_mqtt5) {
    if (reason_code != 0 || properties) {
      packet->remaining_length += 1;
    }

    if (properties) {
      packet->remaining_length += property__get_remaining_length(properties);
    }
  }

  rc = packet__alloc(packet);
  if (rc) {
    ttq_free(packet);
    return rc;
  }

  packet__write_uint16(packet, mid);

  if (ttq->protocol == ttq_p_mqtt5) {
    if (reason_code != 0 || properties) {
      packet__write_byte(packet, reason_code);
    }
    if (properties) {
      property__write_all(packet, properties, true);
    }
  }

  return packet__queue(ttq, packet);
}

/* For DISCONNECT, PINGREQ and PINGRESP */
int send__simple_command(struct tmqtt *ttq, uint8_t command) {
  struct tmqtt__packet *packet = NULL;
  int                   rc;

  packet = ttq_calloc(1, sizeof(struct tmqtt__packet));
  if (!packet) return TTQ_ERR_NOMEM;

  packet->command = command;
  packet->remaining_length = 0;

  rc = packet__alloc(packet);
  if (rc) {
    ttq_free(packet);
    return rc;
  }

  return packet__queue(ttq, packet);
}

int ttq_send_disconnect(struct tmqtt *ttq, uint8_t reason_code, const tmqtt_property *properties) {
  struct tmqtt__packet *packet = NULL;
  int                   rc;

  ttq_log(ttq, TTQ_LOG_DEBUG, "Sending DISCONNECT to %s (rc%d)", SAFE_PRINT(ttq->id), reason_code);

  packet = ttq_calloc(1, sizeof(struct tmqtt__packet));
  if (!packet) return TTQ_ERR_NOMEM;

  packet->command = CMD_DISCONNECT;
  if (ttq->protocol == ttq_p_mqtt5 && (reason_code != 0 || properties)) {
    packet->remaining_length = 1;
    if (properties) {
      packet->remaining_length += property__get_remaining_length(properties);
    }
  } else {
    packet->remaining_length = 0;
  }

  rc = packet__alloc(packet);
  if (rc) {
    ttq_free(packet);
    return rc;
  }
  if (ttq->protocol == ttq_p_mqtt5 && (reason_code != 0 || properties)) {
    packet__write_byte(packet, reason_code);
    if (properties) {
      property__write_all(packet, properties, true);
    }
  }

  return packet__queue(ttq, packet);
}
