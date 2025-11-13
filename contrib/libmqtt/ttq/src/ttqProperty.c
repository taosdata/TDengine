#include "ttqProperty.h"

#include <errno.h>
#include <string.h>

#include "tmqttProto.h"
#include "ttqLogging.h"
#include "ttqMemory.h"
#include "ttqPacket.h"

static int property__read(struct tmqtt__packet *packet, uint32_t *len, tmqtt_property *property) {
  int      rc;
  uint32_t property_identifier;
  uint8_t  byte;
  uint8_t  byte_count;
  uint16_t uint16;
  uint32_t uint32;
  uint32_t varint;
  char    *str1, *str2;
  uint16_t slen1, slen2;

  if (!property) return TTQ_ERR_INVAL;

  rc = packet__read_varint(packet, &property_identifier, NULL);
  if (rc) {
    return rc;
  }
  *len -= 1;

  memset(property, 0, sizeof(tmqtt_property));

  property->identifier = (int32_t)property_identifier;

  switch (property_identifier) {
    case MQTT_PROP_PAYLOAD_FORMAT_INDICATOR:
    case MQTT_PROP_REQUEST_PROBLEM_INFORMATION:
    case MQTT_PROP_REQUEST_RESPONSE_INFORMATION:
    case MQTT_PROP_MAXIMUM_QOS:
    case MQTT_PROP_RETAIN_AVAILABLE:
    case MQTT_PROP_WILDCARD_SUB_AVAILABLE:
    case MQTT_PROP_SUBSCRIPTION_ID_AVAILABLE:
    case MQTT_PROP_SHARED_SUB_AVAILABLE:
      rc = packet__read_byte(packet, &byte);
      if (rc) return rc;
      *len -= 1; /* byte */
      property->value.i8 = byte;
      break;

    case MQTT_PROP_SERVER_KEEP_ALIVE:
    case MQTT_PROP_RECEIVE_MAXIMUM:
    case MQTT_PROP_TOPIC_ALIAS_MAXIMUM:
    case MQTT_PROP_TOPIC_ALIAS:
      rc = packet__read_uint16(packet, &uint16);
      if (rc) return rc;
      *len -= 2; /* uint16 */
      property->value.i16 = uint16;
      break;

    case MQTT_PROP_MESSAGE_EXPIRY_INTERVAL:
    case MQTT_PROP_SESSION_EXPIRY_INTERVAL:
    case MQTT_PROP_WILL_DELAY_INTERVAL:
    case MQTT_PROP_MAXIMUM_PACKET_SIZE:
      rc = packet__read_uint32(packet, &uint32);
      if (rc) return rc;
      *len -= 4; /* uint32 */
      property->value.i32 = uint32;
      break;

    case MQTT_PROP_SUBSCRIPTION_IDENTIFIER:
      rc = packet__read_varint(packet, &varint, &byte_count);
      if (rc) return rc;
      *len -= byte_count;
      property->value.varint = varint;
      break;

    case MQTT_PROP_CONTENT_TYPE:
    case MQTT_PROP_RESPONSE_TOPIC:
    case MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER:
    case MQTT_PROP_AUTHENTICATION_METHOD:
    case MQTT_PROP_RESPONSE_INFORMATION:
    case MQTT_PROP_SERVER_REFERENCE:
    case MQTT_PROP_REASON_STRING:
      rc = packet__read_string(packet, &str1, &slen1);
      if (rc) return rc;
      *len = (*len) - 2 - slen1; /* uint16, string len */
      property->value.s.v = str1;
      property->value.s.len = slen1;
      break;

    case MQTT_PROP_AUTHENTICATION_DATA:
    case MQTT_PROP_CORRELATION_DATA:
      rc = packet__read_binary(packet, (uint8_t **)&str1, &slen1);
      if (rc) return rc;
      *len = (*len) - 2 - slen1; /* uint16, binary len */
      property->value.bin.v = str1;
      property->value.bin.len = slen1;
      break;

    case MQTT_PROP_USER_PROPERTY:
      rc = packet__read_string(packet, &str1, &slen1);
      if (rc) return rc;
      *len = (*len) - 2 - slen1; /* uint16, string len */

      rc = packet__read_string(packet, &str2, &slen2);
      if (rc) {
        ttq_free(str1);
        return rc;
      }
      *len = (*len) - 2 - slen2; /* uint16, string len */

      property->name.v = str1;
      property->name.len = slen1;
      property->value.s.v = str2;
      property->value.s.len = slen2;
      break;

    default:
#ifdef WITH_BROKER
      ttq_log(NULL, TTQ_LOG_DEBUG, "Unsupported property type: %d", property_identifier);
#endif
      return TTQ_ERR_MALFORMED_PACKET;
  }

  return TTQ_ERR_SUCCESS;
}

int property__read_all(int command, struct tmqtt__packet *packet, tmqtt_property **properties) {
  int             rc;
  uint32_t        proplen;
  tmqtt_property *p, *tail = NULL;

  rc = packet__read_varint(packet, &proplen, NULL);
  if (rc) return rc;

  *properties = NULL;

  /* The order of properties must be preserved for some types, so keep the
   * same order for all */
  while (proplen > 0) {
    p = ttq_calloc(1, sizeof(tmqtt_property));
    if (!p) {
      tmqtt_property_free_all(properties);
      return TTQ_ERR_NOMEM;
    }

    rc = property__read(packet, &proplen, p);
    if (rc) {
      ttq_free(p);
      tmqtt_property_free_all(properties);
      return rc;
    }

    if (!(*properties)) {
      *properties = p;
    } else {
      tail->next = p;
    }
    tail = p;
  }

  rc = tmqtt_property_check_all(command, *properties);
  if (rc) {
    tmqtt_property_free_all(properties);
    return rc;
  }
  return TTQ_ERR_SUCCESS;
}

void property__free(tmqtt_property **property) {
  if (!property || !(*property)) return;

  switch ((*property)->identifier) {
    case MQTT_PROP_CONTENT_TYPE:
    case MQTT_PROP_RESPONSE_TOPIC:
    case MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER:
    case MQTT_PROP_AUTHENTICATION_METHOD:
    case MQTT_PROP_RESPONSE_INFORMATION:
    case MQTT_PROP_SERVER_REFERENCE:
    case MQTT_PROP_REASON_STRING:
      ttq_free((*property)->value.s.v);
      break;

    case MQTT_PROP_AUTHENTICATION_DATA:
    case MQTT_PROP_CORRELATION_DATA:
      ttq_free((*property)->value.bin.v);
      break;

    case MQTT_PROP_USER_PROPERTY:
      ttq_free((*property)->name.v);
      ttq_free((*property)->value.s.v);
      break;

    case MQTT_PROP_PAYLOAD_FORMAT_INDICATOR:
    case MQTT_PROP_MESSAGE_EXPIRY_INTERVAL:
    case MQTT_PROP_SUBSCRIPTION_IDENTIFIER:
    case MQTT_PROP_SESSION_EXPIRY_INTERVAL:
    case MQTT_PROP_SERVER_KEEP_ALIVE:
    case MQTT_PROP_REQUEST_PROBLEM_INFORMATION:
    case MQTT_PROP_WILL_DELAY_INTERVAL:
    case MQTT_PROP_REQUEST_RESPONSE_INFORMATION:
    case MQTT_PROP_RECEIVE_MAXIMUM:
    case MQTT_PROP_TOPIC_ALIAS_MAXIMUM:
    case MQTT_PROP_TOPIC_ALIAS:
    case MQTT_PROP_MAXIMUM_QOS:
    case MQTT_PROP_RETAIN_AVAILABLE:
    case MQTT_PROP_MAXIMUM_PACKET_SIZE:
    case MQTT_PROP_WILDCARD_SUB_AVAILABLE:
    case MQTT_PROP_SUBSCRIPTION_ID_AVAILABLE:
    case MQTT_PROP_SHARED_SUB_AVAILABLE:
      /* Nothing to free */
      break;
  }

  ttq_free(*property);
  *property = NULL;
}

void tmqtt_property_free_all(tmqtt_property **property) {
  tmqtt_property *p, *next;

  if (!property) return;

  p = *property;
  while (p) {
    next = p->next;
    property__free(&p);
    p = next;
  }
  *property = NULL;
}

unsigned int property__get_length(const tmqtt_property *property) {
  if (!property) return 0;

  switch (property->identifier) {
    /* Byte */
    case MQTT_PROP_PAYLOAD_FORMAT_INDICATOR:
    case MQTT_PROP_REQUEST_PROBLEM_INFORMATION:
    case MQTT_PROP_REQUEST_RESPONSE_INFORMATION:
    case MQTT_PROP_MAXIMUM_QOS:
    case MQTT_PROP_RETAIN_AVAILABLE:
    case MQTT_PROP_WILDCARD_SUB_AVAILABLE:
    case MQTT_PROP_SUBSCRIPTION_ID_AVAILABLE:
    case MQTT_PROP_SHARED_SUB_AVAILABLE:
      return 2; /* 1 (identifier) + 1 byte */

    /* uint16 */
    case MQTT_PROP_SERVER_KEEP_ALIVE:
    case MQTT_PROP_RECEIVE_MAXIMUM:
    case MQTT_PROP_TOPIC_ALIAS_MAXIMUM:
    case MQTT_PROP_TOPIC_ALIAS:
      return 3; /* 1 (identifier) + 2 bytes */

    /* uint32 */
    case MQTT_PROP_MESSAGE_EXPIRY_INTERVAL:
    case MQTT_PROP_WILL_DELAY_INTERVAL:
    case MQTT_PROP_MAXIMUM_PACKET_SIZE:
    case MQTT_PROP_SESSION_EXPIRY_INTERVAL:
      return 5; /* 1 (identifier) + 4 bytes */

    /* varint */
    case MQTT_PROP_SUBSCRIPTION_IDENTIFIER:
      if (property->value.varint < 128) {
        return 2;
      } else if (property->value.varint < 16384) {
        return 3;
      } else if (property->value.varint < 2097152) {
        return 4;
      } else if (property->value.varint < 268435456) {
        return 5;
      } else {
        return 0;
      }

    /* binary */
    case MQTT_PROP_CORRELATION_DATA:
    case MQTT_PROP_AUTHENTICATION_DATA:
      return 3U + property->value.bin.len; /* 1 + 2 bytes (len) + X bytes (payload) */

    /* string */
    case MQTT_PROP_CONTENT_TYPE:
    case MQTT_PROP_RESPONSE_TOPIC:
    case MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER:
    case MQTT_PROP_AUTHENTICATION_METHOD:
    case MQTT_PROP_RESPONSE_INFORMATION:
    case MQTT_PROP_SERVER_REFERENCE:
    case MQTT_PROP_REASON_STRING:
      return 3U + property->value.s.len; /* 1 + 2 bytes (len) + X bytes (string) */

    /* string pair */
    case MQTT_PROP_USER_PROPERTY:
      return 5U + property->value.s.len + property->name.len; /* 1 + 2*(2 bytes (len) + X bytes (string))*/

    default:
      return 0;
  }
  return 0;
}

unsigned int property__get_length_all(const tmqtt_property *property) {
  const tmqtt_property *p;
  unsigned int          len = 0;

  p = property;
  while (p) {
    len += property__get_length(p);
    p = p->next;
  }
  return len;
}

/* Return the number of bytes we need to add on to the remaining length when
 * encoding these properties. */
unsigned int property__get_remaining_length(const tmqtt_property *props) {
  unsigned int proplen, varbytes;

  proplen = property__get_length_all(props);
  varbytes = packet__varint_bytes(proplen);
  return proplen + varbytes;
}

static int property__write(struct tmqtt__packet *packet, const tmqtt_property *property) {
  int rc;

  rc = packet__write_varint(packet, (uint32_t)property->identifier);
  if (rc) return rc;

  switch (property->identifier) {
    case MQTT_PROP_PAYLOAD_FORMAT_INDICATOR:
    case MQTT_PROP_REQUEST_PROBLEM_INFORMATION:
    case MQTT_PROP_REQUEST_RESPONSE_INFORMATION:
    case MQTT_PROP_MAXIMUM_QOS:
    case MQTT_PROP_RETAIN_AVAILABLE:
    case MQTT_PROP_WILDCARD_SUB_AVAILABLE:
    case MQTT_PROP_SUBSCRIPTION_ID_AVAILABLE:
    case MQTT_PROP_SHARED_SUB_AVAILABLE:
      packet__write_byte(packet, property->value.i8);
      break;

    case MQTT_PROP_SERVER_KEEP_ALIVE:
    case MQTT_PROP_RECEIVE_MAXIMUM:
    case MQTT_PROP_TOPIC_ALIAS_MAXIMUM:
    case MQTT_PROP_TOPIC_ALIAS:
      packet__write_uint16(packet, property->value.i16);
      break;

    case MQTT_PROP_MESSAGE_EXPIRY_INTERVAL:
    case MQTT_PROP_SESSION_EXPIRY_INTERVAL:
    case MQTT_PROP_WILL_DELAY_INTERVAL:
    case MQTT_PROP_MAXIMUM_PACKET_SIZE:
      packet__write_uint32(packet, property->value.i32);
      break;

    case MQTT_PROP_SUBSCRIPTION_IDENTIFIER:
      return packet__write_varint(packet, property->value.varint);

    case MQTT_PROP_CONTENT_TYPE:
    case MQTT_PROP_RESPONSE_TOPIC:
    case MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER:
    case MQTT_PROP_AUTHENTICATION_METHOD:
    case MQTT_PROP_RESPONSE_INFORMATION:
    case MQTT_PROP_SERVER_REFERENCE:
    case MQTT_PROP_REASON_STRING:
      packet__write_string(packet, property->value.s.v, property->value.s.len);
      break;

    case MQTT_PROP_AUTHENTICATION_DATA:
    case MQTT_PROP_CORRELATION_DATA:
      packet__write_uint16(packet, property->value.bin.len);
      packet__write_bytes(packet, property->value.bin.v, property->value.bin.len);
      break;

    case MQTT_PROP_USER_PROPERTY:
      packet__write_string(packet, property->name.v, property->name.len);
      packet__write_string(packet, property->value.s.v, property->value.s.len);
      break;

    default:
#ifdef WITH_BROKER
      ttq_log(NULL, TTQ_LOG_DEBUG, "Unsupported property type: %d", property->identifier);
#endif
      return TTQ_ERR_INVAL;
  }

  return TTQ_ERR_SUCCESS;
}

int property__write_all(struct tmqtt__packet *packet, const tmqtt_property *properties, bool write_len) {
  int                   rc;
  const tmqtt_property *p;

  if (write_len) {
    rc = packet__write_varint(packet, property__get_length_all(properties));
    if (rc) return rc;
  }

  p = properties;
  while (p) {
    rc = property__write(packet, p);
    if (rc) return rc;
    p = p->next;
  }

  return TTQ_ERR_SUCCESS;
}

int tmqtt_property_check_command(int command, int identifier) {
  switch (identifier) {
    case MQTT_PROP_PAYLOAD_FORMAT_INDICATOR:
    case MQTT_PROP_MESSAGE_EXPIRY_INTERVAL:
    case MQTT_PROP_CONTENT_TYPE:
    case MQTT_PROP_RESPONSE_TOPIC:
    case MQTT_PROP_CORRELATION_DATA:
      if (command != CMD_PUBLISH && command != CMD_WILL) {
        return TTQ_ERR_PROTOCOL;
      }
      break;

    case MQTT_PROP_SUBSCRIPTION_IDENTIFIER:
      if (command != CMD_PUBLISH && command != CMD_SUBSCRIBE) {
        return TTQ_ERR_PROTOCOL;
      }
      break;

    case MQTT_PROP_SESSION_EXPIRY_INTERVAL:
      if (command != CMD_CONNECT && command != CMD_CONNACK && command != CMD_DISCONNECT) {
        return TTQ_ERR_PROTOCOL;
      }
      break;

    case MQTT_PROP_AUTHENTICATION_METHOD:
    case MQTT_PROP_AUTHENTICATION_DATA:
      if (command != CMD_CONNECT && command != CMD_CONNACK && command != CMD_AUTH) {
        return TTQ_ERR_PROTOCOL;
      }
      break;

    case MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER:
    case MQTT_PROP_SERVER_KEEP_ALIVE:
    case MQTT_PROP_RESPONSE_INFORMATION:
    case MQTT_PROP_MAXIMUM_QOS:
    case MQTT_PROP_RETAIN_AVAILABLE:
    case MQTT_PROP_WILDCARD_SUB_AVAILABLE:
    case MQTT_PROP_SUBSCRIPTION_ID_AVAILABLE:
    case MQTT_PROP_SHARED_SUB_AVAILABLE:
      if (command != CMD_CONNACK) {
        return TTQ_ERR_PROTOCOL;
      }
      break;

    case MQTT_PROP_WILL_DELAY_INTERVAL:
      if (command != CMD_WILL) {
        return TTQ_ERR_PROTOCOL;
      }
      break;

    case MQTT_PROP_REQUEST_PROBLEM_INFORMATION:
    case MQTT_PROP_REQUEST_RESPONSE_INFORMATION:
      if (command != CMD_CONNECT) {
        return TTQ_ERR_PROTOCOL;
      }
      break;

    case MQTT_PROP_SERVER_REFERENCE:
      if (command != CMD_CONNACK && command != CMD_DISCONNECT) {
        return TTQ_ERR_PROTOCOL;
      }
      break;

    case MQTT_PROP_REASON_STRING:
      if (command == CMD_CONNECT || command == CMD_PUBLISH || command == CMD_SUBSCRIBE || command == CMD_UNSUBSCRIBE) {
        return TTQ_ERR_PROTOCOL;
      }
      break;

    case MQTT_PROP_RECEIVE_MAXIMUM:
    case MQTT_PROP_TOPIC_ALIAS_MAXIMUM:
    case MQTT_PROP_MAXIMUM_PACKET_SIZE:
      if (command != CMD_CONNECT && command != CMD_CONNACK) {
        return TTQ_ERR_PROTOCOL;
      }
      break;

    case MQTT_PROP_TOPIC_ALIAS:
      if (command != CMD_PUBLISH) {
        return TTQ_ERR_PROTOCOL;
      }
      break;

    case MQTT_PROP_USER_PROPERTY:
      break;

    default:
      return TTQ_ERR_PROTOCOL;
  }
  return TTQ_ERR_SUCCESS;
}

const char *tmqtt_property_identifier_to_string(int identifier) {
  switch (identifier) {
    case MQTT_PROP_PAYLOAD_FORMAT_INDICATOR:
      return "payload-format-indicator";
    case MQTT_PROP_MESSAGE_EXPIRY_INTERVAL:
      return "message-expiry-interval";
    case MQTT_PROP_CONTENT_TYPE:
      return "content-type";
    case MQTT_PROP_RESPONSE_TOPIC:
      return "response-topic";
    case MQTT_PROP_CORRELATION_DATA:
      return "correlation-data";
    case MQTT_PROP_SUBSCRIPTION_IDENTIFIER:
      return "subscription-identifier";
    case MQTT_PROP_SESSION_EXPIRY_INTERVAL:
      return "session-expiry-interval";
    case MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER:
      return "assigned-client-identifier";
    case MQTT_PROP_SERVER_KEEP_ALIVE:
      return "server-keep-alive";
    case MQTT_PROP_AUTHENTICATION_METHOD:
      return "authentication-method";
    case MQTT_PROP_AUTHENTICATION_DATA:
      return "authentication-data";
    case MQTT_PROP_REQUEST_PROBLEM_INFORMATION:
      return "request-problem-information";
    case MQTT_PROP_WILL_DELAY_INTERVAL:
      return "will-delay-interval";
    case MQTT_PROP_REQUEST_RESPONSE_INFORMATION:
      return "request-response-information";
    case MQTT_PROP_RESPONSE_INFORMATION:
      return "response-information";
    case MQTT_PROP_SERVER_REFERENCE:
      return "server-reference";
    case MQTT_PROP_REASON_STRING:
      return "reason-string";
    case MQTT_PROP_RECEIVE_MAXIMUM:
      return "receive-maximum";
    case MQTT_PROP_TOPIC_ALIAS_MAXIMUM:
      return "topic-alias-maximum";
    case MQTT_PROP_TOPIC_ALIAS:
      return "topic-alias";
    case MQTT_PROP_MAXIMUM_QOS:
      return "maximum-qos";
    case MQTT_PROP_RETAIN_AVAILABLE:
      return "retain-available";
    case MQTT_PROP_USER_PROPERTY:
      return "user-property";
    case MQTT_PROP_MAXIMUM_PACKET_SIZE:
      return "maximum-packet-size";
    case MQTT_PROP_WILDCARD_SUB_AVAILABLE:
      return "wildcard-subscription-available";
    case MQTT_PROP_SUBSCRIPTION_ID_AVAILABLE:
      return "subscription-identifier-available";
    case MQTT_PROP_SHARED_SUB_AVAILABLE:
      return "shared-subscription-available";
    default:
      return NULL;
  }
}

int tmqtt_string_to_property_info(const char *propname, int *identifier, int *type) {
  if (!propname) return TTQ_ERR_INVAL;

  if (!strcasecmp(propname, "payload-format-indicator")) {
    *identifier = MQTT_PROP_PAYLOAD_FORMAT_INDICATOR;
    *type = MQTT_PROP_TYPE_BYTE;
  } else if (!strcasecmp(propname, "message-expiry-interval")) {
    *identifier = MQTT_PROP_MESSAGE_EXPIRY_INTERVAL;
    *type = MQTT_PROP_TYPE_INT32;
  } else if (!strcasecmp(propname, "content-type")) {
    *identifier = MQTT_PROP_CONTENT_TYPE;
    *type = MQTT_PROP_TYPE_STRING;
  } else if (!strcasecmp(propname, "response-topic")) {
    *identifier = MQTT_PROP_RESPONSE_TOPIC;
    *type = MQTT_PROP_TYPE_STRING;
  } else if (!strcasecmp(propname, "correlation-data")) {
    *identifier = MQTT_PROP_CORRELATION_DATA;
    *type = MQTT_PROP_TYPE_BINARY;
  } else if (!strcasecmp(propname, "subscription-identifier")) {
    *identifier = MQTT_PROP_SUBSCRIPTION_IDENTIFIER;
    *type = MQTT_PROP_TYPE_VARINT;
  } else if (!strcasecmp(propname, "session-expiry-interval")) {
    *identifier = MQTT_PROP_SESSION_EXPIRY_INTERVAL;
    *type = MQTT_PROP_TYPE_INT32;
  } else if (!strcasecmp(propname, "assigned-client-identifier")) {
    *identifier = MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER;
    *type = MQTT_PROP_TYPE_STRING;
  } else if (!strcasecmp(propname, "server-keep-alive")) {
    *identifier = MQTT_PROP_SERVER_KEEP_ALIVE;
    *type = MQTT_PROP_TYPE_INT16;
  } else if (!strcasecmp(propname, "authentication-method")) {
    *identifier = MQTT_PROP_AUTHENTICATION_METHOD;
    *type = MQTT_PROP_TYPE_STRING;
  } else if (!strcasecmp(propname, "authentication-data")) {
    *identifier = MQTT_PROP_AUTHENTICATION_DATA;
    *type = MQTT_PROP_TYPE_BINARY;
  } else if (!strcasecmp(propname, "request-problem-information")) {
    *identifier = MQTT_PROP_REQUEST_PROBLEM_INFORMATION;
    *type = MQTT_PROP_TYPE_BYTE;
  } else if (!strcasecmp(propname, "will-delay-interval")) {
    *identifier = MQTT_PROP_WILL_DELAY_INTERVAL;
    *type = MQTT_PROP_TYPE_INT32;
  } else if (!strcasecmp(propname, "request-response-information")) {
    *identifier = MQTT_PROP_REQUEST_RESPONSE_INFORMATION;
    *type = MQTT_PROP_TYPE_BYTE;
  } else if (!strcasecmp(propname, "response-information")) {
    *identifier = MQTT_PROP_RESPONSE_INFORMATION;
    *type = MQTT_PROP_TYPE_STRING;
  } else if (!strcasecmp(propname, "server-reference")) {
    *identifier = MQTT_PROP_SERVER_REFERENCE;
    *type = MQTT_PROP_TYPE_STRING;
  } else if (!strcasecmp(propname, "reason-string")) {
    *identifier = MQTT_PROP_REASON_STRING;
    *type = MQTT_PROP_TYPE_STRING;
  } else if (!strcasecmp(propname, "receive-maximum")) {
    *identifier = MQTT_PROP_RECEIVE_MAXIMUM;
    *type = MQTT_PROP_TYPE_INT16;
  } else if (!strcasecmp(propname, "topic-alias-maximum")) {
    *identifier = MQTT_PROP_TOPIC_ALIAS_MAXIMUM;
    *type = MQTT_PROP_TYPE_INT16;
  } else if (!strcasecmp(propname, "topic-alias")) {
    *identifier = MQTT_PROP_TOPIC_ALIAS;
    *type = MQTT_PROP_TYPE_INT16;
  } else if (!strcasecmp(propname, "maximum-qos")) {
    *identifier = MQTT_PROP_MAXIMUM_QOS;
    *type = MQTT_PROP_TYPE_BYTE;
  } else if (!strcasecmp(propname, "retain-available")) {
    *identifier = MQTT_PROP_RETAIN_AVAILABLE;
    *type = MQTT_PROP_TYPE_BYTE;
  } else if (!strcasecmp(propname, "user-property")) {
    *identifier = MQTT_PROP_USER_PROPERTY;
    *type = MQTT_PROP_TYPE_STRING_PAIR;
  } else if (!strcasecmp(propname, "maximum-packet-size")) {
    *identifier = MQTT_PROP_MAXIMUM_PACKET_SIZE;
    *type = MQTT_PROP_TYPE_INT32;
  } else if (!strcasecmp(propname, "wildcard-subscription-available")) {
    *identifier = MQTT_PROP_WILDCARD_SUB_AVAILABLE;
    *type = MQTT_PROP_TYPE_BYTE;
  } else if (!strcasecmp(propname, "subscription-identifier-available")) {
    *identifier = MQTT_PROP_SUBSCRIPTION_ID_AVAILABLE;
    *type = MQTT_PROP_TYPE_BYTE;
  } else if (!strcasecmp(propname, "shared-subscription-available")) {
    *identifier = MQTT_PROP_SHARED_SUB_AVAILABLE;
    *type = MQTT_PROP_TYPE_BYTE;
  } else {
    return TTQ_ERR_INVAL;
  }
  return TTQ_ERR_SUCCESS;
}

static void property__add(tmqtt_property **proplist, struct mqtt5__property *prop) {
  tmqtt_property *p;

  if (!(*proplist)) {
    *proplist = prop;
  }

  p = *proplist;
  while (p->next) {
    p = p->next;
  }
  p->next = prop;
  prop->next = NULL;
}

int tmqtt_property_add_byte(tmqtt_property **proplist, int identifier, uint8_t value) {
  tmqtt_property *prop;

  if (!proplist) return TTQ_ERR_INVAL;
  if (identifier != MQTT_PROP_PAYLOAD_FORMAT_INDICATOR && identifier != MQTT_PROP_REQUEST_PROBLEM_INFORMATION &&
      identifier != MQTT_PROP_REQUEST_RESPONSE_INFORMATION && identifier != MQTT_PROP_MAXIMUM_QOS &&
      identifier != MQTT_PROP_RETAIN_AVAILABLE && identifier != MQTT_PROP_WILDCARD_SUB_AVAILABLE &&
      identifier != MQTT_PROP_SUBSCRIPTION_ID_AVAILABLE && identifier != MQTT_PROP_SHARED_SUB_AVAILABLE) {
    return TTQ_ERR_INVAL;
  }

  prop = ttq_calloc(1, sizeof(tmqtt_property));
  if (!prop) return TTQ_ERR_NOMEM;

  prop->client_generated = true;
  prop->identifier = identifier;
  prop->value.i8 = value;

  property__add(proplist, prop);
  return TTQ_ERR_SUCCESS;
}

int tmqtt_property_add_int16(tmqtt_property **proplist, int identifier, uint16_t value) {
  tmqtt_property *prop;

  if (!proplist) return TTQ_ERR_INVAL;
  if (identifier != MQTT_PROP_SERVER_KEEP_ALIVE && identifier != MQTT_PROP_RECEIVE_MAXIMUM &&
      identifier != MQTT_PROP_TOPIC_ALIAS_MAXIMUM && identifier != MQTT_PROP_TOPIC_ALIAS) {
    return TTQ_ERR_INVAL;
  }

  prop = ttq_calloc(1, sizeof(tmqtt_property));
  if (!prop) return TTQ_ERR_NOMEM;

  prop->client_generated = true;
  prop->identifier = identifier;
  prop->value.i16 = value;

  property__add(proplist, prop);
  return TTQ_ERR_SUCCESS;
}

int tmqtt_property_add_int32(tmqtt_property **proplist, int identifier, uint32_t value) {
  tmqtt_property *prop;

  if (!proplist) return TTQ_ERR_INVAL;
  if (identifier != MQTT_PROP_MESSAGE_EXPIRY_INTERVAL && identifier != MQTT_PROP_SESSION_EXPIRY_INTERVAL &&
      identifier != MQTT_PROP_WILL_DELAY_INTERVAL && identifier != MQTT_PROP_MAXIMUM_PACKET_SIZE) {
    return TTQ_ERR_INVAL;
  }

  prop = ttq_calloc(1, sizeof(tmqtt_property));
  if (!prop) return TTQ_ERR_NOMEM;

  prop->client_generated = true;
  prop->identifier = identifier;
  prop->value.i32 = value;

  property__add(proplist, prop);
  return TTQ_ERR_SUCCESS;
}

int tmqtt_property_add_varint(tmqtt_property **proplist, int identifier, uint32_t value) {
  tmqtt_property *prop;

  if (!proplist || value > 268435455) return TTQ_ERR_INVAL;
  if (identifier != MQTT_PROP_SUBSCRIPTION_IDENTIFIER) return TTQ_ERR_INVAL;

  prop = ttq_calloc(1, sizeof(tmqtt_property));
  if (!prop) return TTQ_ERR_NOMEM;

  prop->client_generated = true;
  prop->identifier = identifier;
  prop->value.varint = value;

  property__add(proplist, prop);
  return TTQ_ERR_SUCCESS;
}

int tmqtt_property_add_binary(tmqtt_property **proplist, int identifier, const void *value, uint16_t len) {
  tmqtt_property *prop;

  if (!proplist) return TTQ_ERR_INVAL;
  if (identifier != MQTT_PROP_CORRELATION_DATA && identifier != MQTT_PROP_AUTHENTICATION_DATA) {
    return TTQ_ERR_INVAL;
  }

  prop = ttq_calloc(1, sizeof(tmqtt_property));
  if (!prop) return TTQ_ERR_NOMEM;

  prop->client_generated = true;
  prop->identifier = identifier;

  if (len) {
    prop->value.bin.v = ttq_malloc(len);
    if (!prop->value.bin.v) {
      ttq_free(prop);
      return TTQ_ERR_NOMEM;
    }

    memcpy(prop->value.bin.v, value, len);
    prop->value.bin.len = len;
  }

  property__add(proplist, prop);
  return TTQ_ERR_SUCCESS;
}

int tmqtt_property_add_string(tmqtt_property **proplist, int identifier, const char *value) {
  tmqtt_property *prop;
  size_t          slen = 0;

  if (!proplist) return TTQ_ERR_INVAL;
  if (value) {
    slen = strlen(value);
    if (tmqtt_validate_utf8(value, (int)slen)) return TTQ_ERR_MALFORMED_UTF8;
  }

  if (identifier != MQTT_PROP_CONTENT_TYPE && identifier != MQTT_PROP_RESPONSE_TOPIC &&
      identifier != MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER && identifier != MQTT_PROP_AUTHENTICATION_METHOD &&
      identifier != MQTT_PROP_RESPONSE_INFORMATION && identifier != MQTT_PROP_SERVER_REFERENCE &&
      identifier != MQTT_PROP_REASON_STRING) {
    return TTQ_ERR_INVAL;
  }

  prop = ttq_calloc(1, sizeof(tmqtt_property));
  if (!prop) return TTQ_ERR_NOMEM;

  prop->client_generated = true;
  prop->identifier = identifier;
  if (value && slen > 0) {
    prop->value.s.v = ttq_strdup(value);
    if (!prop->value.s.v) {
      ttq_free(prop);
      return TTQ_ERR_NOMEM;
    }
    prop->value.s.len = (uint16_t)slen;
  }

  property__add(proplist, prop);
  return TTQ_ERR_SUCCESS;
}

int tmqtt_property_add_string_pair(tmqtt_property **proplist, int identifier, const char *name, const char *value) {
  tmqtt_property *prop;
  size_t          slen_name = 0, slen_value = 0;

  if (!proplist) return TTQ_ERR_INVAL;
  if (identifier != MQTT_PROP_USER_PROPERTY) return TTQ_ERR_INVAL;
  if (name) {
    slen_name = strlen(name);
    if (tmqtt_validate_utf8(name, (int)slen_name)) return TTQ_ERR_MALFORMED_UTF8;
  }
  if (value) {
    if (tmqtt_validate_utf8(value, (int)slen_value)) return TTQ_ERR_MALFORMED_UTF8;
  }

  prop = ttq_calloc(1, sizeof(tmqtt_property));
  if (!prop) return TTQ_ERR_NOMEM;

  prop->client_generated = true;
  prop->identifier = identifier;

  if (name) {
    prop->name.v = ttq_strdup(name);
    if (!prop->name.v) {
      ttq_free(prop);
      return TTQ_ERR_NOMEM;
    }
    prop->name.len = (uint16_t)strlen(name);
  }

  if (value) {
    prop->value.s.v = ttq_strdup(value);
    if (!prop->value.s.v) {
      ttq_free(prop->name.v);
      ttq_free(prop);
      return TTQ_ERR_NOMEM;
    }
    prop->value.s.len = (uint16_t)strlen(value);
  }

  property__add(proplist, prop);
  return TTQ_ERR_SUCCESS;
}

int tmqtt_property_check_all(int command, const tmqtt_property *properties) {
  const tmqtt_property *p, *tail;
  int                   rc;

  p = properties;

  while (p) {
    /* Validity checks */
    if (p->identifier == MQTT_PROP_REQUEST_PROBLEM_INFORMATION || p->identifier == MQTT_PROP_PAYLOAD_FORMAT_INDICATOR ||
        p->identifier == MQTT_PROP_REQUEST_RESPONSE_INFORMATION || p->identifier == MQTT_PROP_MAXIMUM_QOS ||
        p->identifier == MQTT_PROP_RETAIN_AVAILABLE || p->identifier == MQTT_PROP_WILDCARD_SUB_AVAILABLE ||
        p->identifier == MQTT_PROP_SUBSCRIPTION_ID_AVAILABLE || p->identifier == MQTT_PROP_SHARED_SUB_AVAILABLE) {
      if (p->value.i8 > 1) {
        return TTQ_ERR_PROTOCOL;
      }
    } else if (p->identifier == MQTT_PROP_MAXIMUM_PACKET_SIZE) {
      if (p->value.i32 == 0) {
        return TTQ_ERR_PROTOCOL;
      }
    } else if (p->identifier == MQTT_PROP_RECEIVE_MAXIMUM || p->identifier == MQTT_PROP_TOPIC_ALIAS) {
      if (p->value.i16 == 0) {
        return TTQ_ERR_PROTOCOL;
      }
    } else if (p->identifier == MQTT_PROP_RESPONSE_TOPIC) {
      if (tmqtt_pub_topic_check(p->value.s.v) != TTQ_ERR_SUCCESS) {
        return TTQ_ERR_PROTOCOL;
      }
    }

    /* Check for properties on incorrect commands */
    rc = tmqtt_property_check_command(command, p->identifier);
    if (rc) return rc;

    /* Check for duplicates */
    if (p->identifier != MQTT_PROP_USER_PROPERTY) {
      tail = p->next;
      while (tail) {
        if (p->identifier == tail->identifier) {
          return TTQ_ERR_DUPLICATE_PROPERTY;
        }
        tail = tail->next;
      }
    }

    p = p->next;
  }

  return TTQ_ERR_SUCCESS;
}

static const tmqtt_property *property__get_property(const tmqtt_property *proplist, int identifier, bool skip_first) {
  const tmqtt_property *p;
  bool                  is_first = true;

  p = proplist;

  while (p) {
    if (p->identifier == identifier) {
      if (!is_first || !skip_first) {
        return p;
      }
      is_first = false;
    }
    p = p->next;
  }
  return NULL;
}

int tmqtt_property_identifier(const tmqtt_property *property) {
  if (property == NULL) return 0;

  return property->identifier;
}

const tmqtt_property *tmqtt_property_next(const tmqtt_property *proplist) {
  if (proplist == NULL) return NULL;

  return proplist->next;
}

const tmqtt_property *tmqtt_property_read_byte(const tmqtt_property *proplist, int identifier, uint8_t *value,
                                               bool skip_first) {
  const tmqtt_property *p;
  if (!proplist) return NULL;

  p = property__get_property(proplist, identifier, skip_first);
  if (!p) return NULL;
  if (p->identifier != MQTT_PROP_PAYLOAD_FORMAT_INDICATOR && p->identifier != MQTT_PROP_REQUEST_PROBLEM_INFORMATION &&
      p->identifier != MQTT_PROP_REQUEST_RESPONSE_INFORMATION && p->identifier != MQTT_PROP_MAXIMUM_QOS &&
      p->identifier != MQTT_PROP_RETAIN_AVAILABLE && p->identifier != MQTT_PROP_WILDCARD_SUB_AVAILABLE &&
      p->identifier != MQTT_PROP_SUBSCRIPTION_ID_AVAILABLE && p->identifier != MQTT_PROP_SHARED_SUB_AVAILABLE) {
    return NULL;
  }

  if (value) *value = p->value.i8;

  return p;
}

const tmqtt_property *tmqtt_property_read_int16(const tmqtt_property *proplist, int identifier, uint16_t *value,
                                                bool skip_first) {
  const tmqtt_property *p;
  if (!proplist) return NULL;

  p = property__get_property(proplist, identifier, skip_first);
  if (!p) return NULL;
  if (p->identifier != MQTT_PROP_SERVER_KEEP_ALIVE && p->identifier != MQTT_PROP_RECEIVE_MAXIMUM &&
      p->identifier != MQTT_PROP_TOPIC_ALIAS_MAXIMUM && p->identifier != MQTT_PROP_TOPIC_ALIAS) {
    return NULL;
  }

  if (value) *value = p->value.i16;

  return p;
}

const tmqtt_property *tmqtt_property_read_int32(const tmqtt_property *proplist, int identifier, uint32_t *value,
                                                bool skip_first) {
  const tmqtt_property *p;
  if (!proplist) return NULL;

  p = property__get_property(proplist, identifier, skip_first);
  if (!p) return NULL;
  if (p->identifier != MQTT_PROP_MESSAGE_EXPIRY_INTERVAL && p->identifier != MQTT_PROP_SESSION_EXPIRY_INTERVAL &&
      p->identifier != MQTT_PROP_WILL_DELAY_INTERVAL && p->identifier != MQTT_PROP_MAXIMUM_PACKET_SIZE) {
    return NULL;
  }

  if (value) *value = p->value.i32;

  return p;
}

const tmqtt_property *tmqtt_property_read_varint(const tmqtt_property *proplist, int identifier, uint32_t *value,
                                                 bool skip_first) {
  const tmqtt_property *p;
  if (!proplist) return NULL;

  p = property__get_property(proplist, identifier, skip_first);
  if (!p) return NULL;
  if (p->identifier != MQTT_PROP_SUBSCRIPTION_IDENTIFIER) {
    return NULL;
  }

  if (value) *value = p->value.varint;

  return p;
}

const tmqtt_property *tmqtt_property_read_binary(const tmqtt_property *proplist, int identifier, void **value,
                                                 uint16_t *len, bool skip_first) {
  const tmqtt_property *p;
  if (!proplist || (value && !len) || (!value && len)) return NULL;

  if (value) *value = NULL;

  p = property__get_property(proplist, identifier, skip_first);
  if (!p) return NULL;
  if (p->identifier != MQTT_PROP_CORRELATION_DATA && p->identifier != MQTT_PROP_AUTHENTICATION_DATA) {
    return NULL;
  }

  if (value) {
    *len = p->value.bin.len;
    *value = calloc(1, *len + 1U);
    if (!(*value)) return NULL;

    memcpy(*value, p->value.bin.v, *len);
  }

  return p;
}

const tmqtt_property *tmqtt_property_read_string(const tmqtt_property *proplist, int identifier, char **value,
                                                 bool skip_first) {
  const tmqtt_property *p;
  if (!proplist) return NULL;

  p = property__get_property(proplist, identifier, skip_first);
  if (!p) return NULL;
  if (p->identifier != MQTT_PROP_CONTENT_TYPE && p->identifier != MQTT_PROP_RESPONSE_TOPIC &&
      p->identifier != MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER && p->identifier != MQTT_PROP_AUTHENTICATION_METHOD &&
      p->identifier != MQTT_PROP_RESPONSE_INFORMATION && p->identifier != MQTT_PROP_SERVER_REFERENCE &&
      p->identifier != MQTT_PROP_REASON_STRING) {
    return NULL;
  }

  if (value) {
    *value = calloc(1, (size_t)p->value.s.len + 1);
    if (!(*value)) return NULL;

    memcpy(*value, p->value.s.v, p->value.s.len);
  }

  return p;
}

const tmqtt_property *tmqtt_property_read_string_pair(const tmqtt_property *proplist, int identifier, char **name,
                                                      char **value, bool skip_first) {
  const tmqtt_property *p;
  if (!proplist) return NULL;

  if (name) *name = NULL;
  if (value) *value = NULL;

  p = property__get_property(proplist, identifier, skip_first);
  if (!p) return NULL;
  if (p->identifier != MQTT_PROP_USER_PROPERTY) return NULL;

  if (name) {
    *name = calloc(1, (size_t)p->name.len + 1);
    if (!(*name)) return NULL;
    memcpy(*name, p->name.v, p->name.len);
  }

  if (value) {
    *value = calloc(1, (size_t)p->value.s.len + 1);
    if (!(*value)) {
      if (name) {
        ttq_free(*name);
        *name = NULL;
      }
      return NULL;
    }
    memcpy(*value, p->value.s.v, p->value.s.len);
  }

  return p;
}

int tmqtt_property_copy_all(tmqtt_property **dest, const tmqtt_property *src) {
  tmqtt_property *pnew, *plast = NULL;

  if (!src) return TTQ_ERR_SUCCESS;
  if (!dest) return TTQ_ERR_INVAL;

  *dest = NULL;

  while (src) {
    pnew = calloc(1, sizeof(tmqtt_property));
    if (!pnew) {
      tmqtt_property_free_all(dest);
      return TTQ_ERR_NOMEM;
    }
    if (plast) {
      plast->next = pnew;
    } else {
      *dest = pnew;
    }
    plast = pnew;

    pnew->client_generated = src->client_generated;
    pnew->identifier = src->identifier;
    switch (pnew->identifier) {
      case MQTT_PROP_PAYLOAD_FORMAT_INDICATOR:
      case MQTT_PROP_REQUEST_PROBLEM_INFORMATION:
      case MQTT_PROP_REQUEST_RESPONSE_INFORMATION:
      case MQTT_PROP_MAXIMUM_QOS:
      case MQTT_PROP_RETAIN_AVAILABLE:
      case MQTT_PROP_WILDCARD_SUB_AVAILABLE:
      case MQTT_PROP_SUBSCRIPTION_ID_AVAILABLE:
      case MQTT_PROP_SHARED_SUB_AVAILABLE:
        pnew->value.i8 = src->value.i8;
        break;

      case MQTT_PROP_SERVER_KEEP_ALIVE:
      case MQTT_PROP_RECEIVE_MAXIMUM:
      case MQTT_PROP_TOPIC_ALIAS_MAXIMUM:
      case MQTT_PROP_TOPIC_ALIAS:
        pnew->value.i16 = src->value.i16;
        break;

      case MQTT_PROP_MESSAGE_EXPIRY_INTERVAL:
      case MQTT_PROP_SESSION_EXPIRY_INTERVAL:
      case MQTT_PROP_WILL_DELAY_INTERVAL:
      case MQTT_PROP_MAXIMUM_PACKET_SIZE:
        pnew->value.i32 = src->value.i32;
        break;

      case MQTT_PROP_SUBSCRIPTION_IDENTIFIER:
        pnew->value.varint = src->value.varint;
        break;

      case MQTT_PROP_CONTENT_TYPE:
      case MQTT_PROP_RESPONSE_TOPIC:
      case MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER:
      case MQTT_PROP_AUTHENTICATION_METHOD:
      case MQTT_PROP_RESPONSE_INFORMATION:
      case MQTT_PROP_SERVER_REFERENCE:
      case MQTT_PROP_REASON_STRING:
        pnew->value.s.len = src->value.s.len;
        pnew->value.s.v = src->value.s.v ? strdup(src->value.s.v) : (char *)calloc(1, 1);
        if (!pnew->value.s.v) {
          tmqtt_property_free_all(dest);
          return TTQ_ERR_NOMEM;
        }
        break;

      case MQTT_PROP_AUTHENTICATION_DATA:
      case MQTT_PROP_CORRELATION_DATA:
        pnew->value.bin.len = src->value.bin.len;
        pnew->value.bin.v = malloc(pnew->value.bin.len);
        if (!pnew->value.bin.v) {
          tmqtt_property_free_all(dest);
          return TTQ_ERR_NOMEM;
        }
        memcpy(pnew->value.bin.v, src->value.bin.v, pnew->value.bin.len);
        break;

      case MQTT_PROP_USER_PROPERTY:
        pnew->value.s.len = src->value.s.len;
        pnew->value.s.v = src->value.s.v ? strdup(src->value.s.v) : (char *)calloc(1, 1);
        if (!pnew->value.s.v) {
          tmqtt_property_free_all(dest);
          return TTQ_ERR_NOMEM;
        }

        pnew->name.len = src->name.len;
        pnew->name.v = src->name.v ? strdup(src->name.v) : (char *)calloc(1, 1);
        if (!pnew->name.v) {
          tmqtt_property_free_all(dest);
          return TTQ_ERR_NOMEM;
        }
        break;

      default:
        tmqtt_property_free_all(dest);
        return TTQ_ERR_INVAL;
    }

    src = src->next;
  }

  return TTQ_ERR_SUCCESS;
}
