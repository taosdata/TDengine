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

#ifndef _TD_TMQTT_PROTO_H_
#define _TD_TMQTT_PROTO_H_

#ifdef __cplusplus
extern "C" {
#endif

/*
 * MQTT values defined in the spec.
 */

#define PROTOCOL_NAME         "MQTT"
#define PROTOCOL_NAME_v31     "MQIsdp"
#define PROTOCOL_VERSION_v31  3
#define PROTOCOL_VERSION_v311 4
#define PROTOCOL_VERSION_v5   5

/* Message types */
#define CMD_CONNECT     0x10U
#define CMD_CONNACK     0x20U
#define CMD_PUBLISH     0x30U
#define CMD_PUBACK      0x40U
#define CMD_PUBREC      0x50U
#define CMD_PUBREL      0x60U
#define CMD_PUBCOMP     0x70U
#define CMD_SUBSCRIBE   0x80U
#define CMD_SUBACK      0x90U
#define CMD_UNSUBSCRIBE 0xA0U
#define CMD_UNSUBACK    0xB0U
#define CMD_PINGREQ     0xC0U
#define CMD_PINGRESP    0xD0U
#define CMD_DISCONNECT  0xE0U
#define CMD_AUTH        0xF0U

/* Extra cmd for distinguishing CONNECT and WILL properties */
#define CMD_WILL 0x100

enum mqtt311_connack_codes {
  CONNACK_ACCEPTED = 0,
  CONNACK_REFUSED_PROTOCOL_VERSION = 1,
  CONNACK_REFUSED_IDENTIFIER_REJECTED = 2,
  CONNACK_REFUSED_SERVER_UNAVAILABLE = 3,
  CONNACK_REFUSED_BAD_USERNAME_PASSWORD = 4,
  CONNACK_REFUSED_NOT_AUTHORIZED = 5,
};

enum mqtt5_return_codes {
  MQTT_RC_SUCCESS = 0,                  /* CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK, AUTH */
  MQTT_RC_NORMAL_DISCONNECTION = 0,     /* DISCONNECT */
  MQTT_RC_GRANTED_QOS0 = 0,             /* SUBACK */
  MQTT_RC_GRANTED_QOS1 = 1,             /* SUBACK */
  MQTT_RC_GRANTED_QOS2 = 2,             /* SUBACK */
  MQTT_RC_DISCONNECT_WITH_WILL_MSG = 4, /* DISCONNECT */
  MQTT_RC_NO_MATCHING_SUBSCRIBERS = 16, /* PUBACK, PUBREC */
  MQTT_RC_NO_SUBSCRIPTION_EXISTED = 17, /* UNSUBACK */
  MQTT_RC_CONTINUE_AUTHENTICATION = 24, /* AUTH */
  MQTT_RC_REAUTHENTICATE = 25,          /* AUTH */

  MQTT_RC_UNSPECIFIED = 128,                    /* CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT */
  MQTT_RC_MALFORMED_PACKET = 129,               /* CONNACK, DISCONNECT */
  MQTT_RC_PROTOCOL_ERROR = 130,                 /* DISCONNECT */
  MQTT_RC_IMPLEMENTATION_SPECIFIC = 131,        /* CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT */
  MQTT_RC_UNSUPPORTED_PROTOCOL_VERSION = 132,   /* CONNACK */
  MQTT_RC_CLIENTID_NOT_VALID = 133,             /* CONNACK */
  MQTT_RC_BAD_USERNAME_OR_PASSWORD = 134,       /* CONNACK */
  MQTT_RC_NOT_AUTHORIZED = 135,                 /* CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT */
  MQTT_RC_SERVER_UNAVAILABLE = 136,             /* CONNACK */
  MQTT_RC_SERVER_BUSY = 137,                    /* CONNACK, DISCONNECT */
  MQTT_RC_BANNED = 138,                         /* CONNACK */
  MQTT_RC_SERVER_SHUTTING_DOWN = 139,           /* DISCONNECT */
  MQTT_RC_BAD_AUTHENTICATION_METHOD = 140,      /* CONNACK */
  MQTT_RC_KEEP_ALIVE_TIMEOUT = 141,             /* DISCONNECT */
  MQTT_RC_SESSION_TAKEN_OVER = 142,             /* DISCONNECT */
  MQTT_RC_TOPIC_FILTER_INVALID = 143,           /* SUBACK, UNSUBACK, DISCONNECT */
  MQTT_RC_TOPIC_NAME_INVALID = 144,             /* CONNACK, PUBACK, PUBREC, DISCONNECT */
  MQTT_RC_PACKET_ID_IN_USE = 145,               /* PUBACK, SUBACK, UNSUBACK */
  MQTT_RC_PACKET_ID_NOT_FOUND = 146,            /* PUBREL, PUBCOMP */
  MQTT_RC_RECEIVE_MAXIMUM_EXCEEDED = 147,       /* DISCONNECT */
  MQTT_RC_TOPIC_ALIAS_INVALID = 148,            /* DISCONNECT */
  MQTT_RC_PACKET_TOO_LARGE = 149,               /* CONNACK, PUBACK, PUBREC, DISCONNECT */
  MQTT_RC_MESSAGE_RATE_TOO_HIGH = 150,          /* DISCONNECT */
  MQTT_RC_QUOTA_EXCEEDED = 151,                 /* PUBACK, PUBREC, SUBACK, DISCONNECT */
  MQTT_RC_ADMINISTRATIVE_ACTION = 152,          /* DISCONNECT */
  MQTT_RC_PAYLOAD_FORMAT_INVALID = 153,         /* CONNACK, PUBACK, PUBREC, DISCONNECT */
  MQTT_RC_RETAIN_NOT_SUPPORTED = 154,           /* CONNACK, DISCONNECT */
  MQTT_RC_QOS_NOT_SUPPORTED = 155,              /* CONNACK, DISCONNECT */
  MQTT_RC_USE_ANOTHER_SERVER = 156,             /* CONNACK, DISCONNECT */
  MQTT_RC_SERVER_MOVED = 157,                   /* CONNACK, DISCONNECT */
  MQTT_RC_SHARED_SUBS_NOT_SUPPORTED = 158,      /* SUBACK, DISCONNECT */
  MQTT_RC_CONNECTION_RATE_EXCEEDED = 159,       /* CONNACK, DISCONNECT */
  MQTT_RC_MAXIMUM_CONNECT_TIME = 160,           /* DISCONNECT */
  MQTT_RC_SUBSCRIPTION_IDS_NOT_SUPPORTED = 161, /* SUBACK, DISCONNECT */
  MQTT_RC_WILDCARD_SUBS_NOT_SUPPORTED = 162,    /* SUBACK, DISCONNECT */
};

enum mqtt5_property {
  MQTT_PROP_PAYLOAD_FORMAT_INDICATOR = 1,      /* Byte:		PUBLISH, Will Properties */
  MQTT_PROP_MESSAGE_EXPIRY_INTERVAL = 2,       /* 4 byte int:	PUBLISH, Will Properties */
  MQTT_PROP_CONTENT_TYPE = 3,                  /* UTF-8 string:	PUBLISH, Will Properties */
  MQTT_PROP_RESPONSE_TOPIC = 8,                /* UTF-8 string:	PUBLISH, Will Properties */
  MQTT_PROP_CORRELATION_DATA = 9,              /* Binary Data:	PUBLISH, Will Properties */
  MQTT_PROP_SUBSCRIPTION_IDENTIFIER = 11,      /* Variable byte int:	PUBLISH, SUBSCRIBE */
  MQTT_PROP_SESSION_EXPIRY_INTERVAL = 17,      /* 4 byte int:		CONNECT, CONNACK, DISCONNECT */
  MQTT_PROP_ASSIGNED_CLIENT_IDENTIFIER = 18,   /* UTF-8 string:		CONNACK */
  MQTT_PROP_SERVER_KEEP_ALIVE = 19,            /* 2 byte int:		CONNACK */
  MQTT_PROP_AUTHENTICATION_METHOD = 21,        /* UTF-8 string:		CONNECT, CONNACK, AUTH */
  MQTT_PROP_AUTHENTICATION_DATA = 22,          /* Binary Data:		CONNECT, CONNACK, AUTH */
  MQTT_PROP_REQUEST_PROBLEM_INFORMATION = 23,  /* Byte:			CONNECT */
  MQTT_PROP_WILL_DELAY_INTERVAL = 24,          /* 4 byte int:		Will properties */
  MQTT_PROP_REQUEST_RESPONSE_INFORMATION = 25, /* Byte:			CONNECT */
  MQTT_PROP_RESPONSE_INFORMATION = 26,         /* UTF-8 string:		CONNACK */
  MQTT_PROP_SERVER_REFERENCE = 28,             /* UTF-8 string:		CONNACK, DISCONNECT */
  MQTT_PROP_REASON_STRING = 31,                /* UTF-8 string:	All except Will properties */
  MQTT_PROP_RECEIVE_MAXIMUM = 33,              /* 2 byte int:		CONNECT, CONNACK */
  MQTT_PROP_TOPIC_ALIAS_MAXIMUM = 34,          /* 2 byte int:		CONNECT, CONNACK */
  MQTT_PROP_TOPIC_ALIAS = 35,                  /* 2 byte int:		PUBLISH */
  MQTT_PROP_MAXIMUM_QOS = 36,                  /* Byte:			CONNACK */
  MQTT_PROP_RETAIN_AVAILABLE = 37,             /* Byte:			CONNACK */
  MQTT_PROP_USER_PROPERTY = 38,                /* UTF-8 string pair:	All */
  MQTT_PROP_MAXIMUM_PACKET_SIZE = 39,          /* 4 byte int:		CONNECT, CONNACK */
  MQTT_PROP_WILDCARD_SUB_AVAILABLE = 40,       /* Byte:			CONNACK */
  MQTT_PROP_SUBSCRIPTION_ID_AVAILABLE = 41,    /* Byte:			CONNACK */
  MQTT_PROP_SHARED_SUB_AVAILABLE = 42,         /* Byte:			CONNACK */
};

enum mqtt5_property_type {
  MQTT_PROP_TYPE_BYTE = 1,
  MQTT_PROP_TYPE_INT16 = 2,
  MQTT_PROP_TYPE_INT32 = 3,
  MQTT_PROP_TYPE_VARINT = 4,
  MQTT_PROP_TYPE_BINARY = 5,
  MQTT_PROP_TYPE_STRING = 6,
  MQTT_PROP_TYPE_STRING_PAIR = 7
};

enum mqtt5_sub_options {
  MQTT_SUB_OPT_NO_LOCAL = 0x04,
  MQTT_SUB_OPT_RETAIN_AS_PUBLISHED = 0x08,
  MQTT_SUB_OPT_SEND_RETAIN_ALWAYS = 0x00,
  MQTT_SUB_OPT_SEND_RETAIN_NEW = 0x10,
  MQTT_SUB_OPT_SEND_RETAIN_NEVER = 0x20,
};

#define MQTT_MAX_PAYLOAD 268435455U

#ifdef __cplusplus
}
#endif

#endif /*_TD_TMQTT_PROTO_H_*/
