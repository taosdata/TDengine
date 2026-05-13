#include "tmqttInt.h"

#include <errno.h>
#include <string.h>

#include "tmqttProto.h"

const char *tmqtt_strerror(int ttq_errno) {
  switch (ttq_errno) {
    case TTQ_ERR_AUTH_CONTINUE:
      return "Continue with authentication.";
    case TTQ_ERR_NO_SUBSCRIBERS:
      return "No subscribers.";
    case TTQ_ERR_SUB_EXISTS:
      return "Subscription already exists.";
    case TTQ_ERR_CONN_PENDING:
      return "Connection pending.";
    case TTQ_ERR_SUCCESS:
      return "No error.";
    case TTQ_ERR_NOMEM:
      return "Out of memory.";
    case TTQ_ERR_PROTOCOL:
      return "A network protocol error occurred when communicating with the broker.";
    case TTQ_ERR_INVAL:
      return "Invalid arguments provided.";
    case TTQ_ERR_NO_CONN:
      return "The client is not currently connected.";
    case TTQ_ERR_CONN_REFUSED:
      return "The connection was refused.";
    case TTQ_ERR_NOT_FOUND:
      return "Message not found (internal error).";
    case TTQ_ERR_CONN_LOST:
      return "The connection was lost.";
    case TTQ_ERR_TLS:
      return "A TLS error occurred.";
    case TTQ_ERR_PAYLOAD_SIZE:
      return "Payload too large.";
    case TTQ_ERR_NOT_SUPPORTED:
      return "This feature is not supported.";
    case TTQ_ERR_AUTH:
      return "Authorisation failed.";
    case TTQ_ERR_ACL_DENIED:
      return "Access denied by ACL.";
    case TTQ_ERR_UNKNOWN:
      return "Unknown error.";
    case TTQ_ERR_ERRNO:
      return strerror(errno);
    case TTQ_ERR_EAI:
      return "Lookup error.";
    case TTQ_ERR_PROXY:
      return "Proxy error.";
    case TTQ_ERR_MALFORMED_UTF8:
      return "Malformed UTF-8";
    case TTQ_ERR_KEEPALIVE:
      return "Keepalive exceeded";
    case TTQ_ERR_LOOKUP:
      return "DNS Lookup failed";
    case TTQ_ERR_DUPLICATE_PROPERTY:
      return "Duplicate property in property list";
    case TTQ_ERR_TLS_HANDSHAKE:
      return "TLS handshake failed.";
    case TTQ_ERR_QOS_NOT_SUPPORTED:
      return "Requested QoS not supported on server.";
    case TTQ_ERR_OVERSIZE_PACKET:
      return "Packet larger than supported by the server.";
    case TTQ_ERR_OCSP:
      return "OCSP error.";
    default:
      if (ttq_errno >= 128) {
        // If ttq_errno is greater than 127,
        // a mqtt5_return_code error was used
        return tmqtt_reason_string(ttq_errno);
      } else {
        return "Unknown error.";
      }
  }
}

const char *tmqtt_connack_string(int connack_code) {
  switch (connack_code) {
    case 0:
      return "Connection Accepted.";
    case 1:
      return "Connection Refused: unacceptable protocol version.";
    case 2:
      return "Connection Refused: identifier rejected.";
    case 3:
      return "Connection Refused: broker unavailable.";
    case 4:
      return "Connection Refused: bad user name or password.";
    case 5:
      return "Connection Refused: not authorised.";
    default:
      return "Connection Refused: unknown reason.";
  }
}

const char *tmqtt_reason_string(int reason_code) {
  switch (reason_code) {
    case MQTT_RC_SUCCESS:
      return "Success";
    case MQTT_RC_GRANTED_QOS1:
      return "Granted QoS 1";
    case MQTT_RC_GRANTED_QOS2:
      return "Granted QoS 2";
    case MQTT_RC_DISCONNECT_WITH_WILL_MSG:
      return "Disconnect with Will Message";
    case MQTT_RC_NO_MATCHING_SUBSCRIBERS:
      return "No matching subscribers";
    case MQTT_RC_NO_SUBSCRIPTION_EXISTED:
      return "No subscription existed";
    case MQTT_RC_CONTINUE_AUTHENTICATION:
      return "Continue authentication";
    case MQTT_RC_REAUTHENTICATE:
      return "Re-authenticate";

    case MQTT_RC_UNSPECIFIED:
      return "Unspecified error";
    case MQTT_RC_MALFORMED_PACKET:
      return "Malformed Packet";
    case MQTT_RC_PROTOCOL_ERROR:
      return "Protocol Error";
    case MQTT_RC_IMPLEMENTATION_SPECIFIC:
      return "Implementation specific error";
    case MQTT_RC_UNSUPPORTED_PROTOCOL_VERSION:
      return "Unsupported Protocol Version";
    case MQTT_RC_CLIENTID_NOT_VALID:
      return "Client Identifier not valid";
    case MQTT_RC_BAD_USERNAME_OR_PASSWORD:
      return "Bad User Name or Password";
    case MQTT_RC_NOT_AUTHORIZED:
      return "Not authorized";
    case MQTT_RC_SERVER_UNAVAILABLE:
      return "Server unavailable";
    case MQTT_RC_SERVER_BUSY:
      return "Server busy";
    case MQTT_RC_BANNED:
      return "Banned";
    case MQTT_RC_SERVER_SHUTTING_DOWN:
      return "Server shutting down";
    case MQTT_RC_BAD_AUTHENTICATION_METHOD:
      return "Bad authentication method";
    case MQTT_RC_KEEP_ALIVE_TIMEOUT:
      return "Keep Alive timeout";
    case MQTT_RC_SESSION_TAKEN_OVER:
      return "Session taken over";
    case MQTT_RC_TOPIC_FILTER_INVALID:
      return "Topic Filter invalid";
    case MQTT_RC_TOPIC_NAME_INVALID:
      return "Topic Name invalid";
    case MQTT_RC_PACKET_ID_IN_USE:
      return "Packet Identifier in use";
    case MQTT_RC_PACKET_ID_NOT_FOUND:
      return "Packet Identifier not found";
    case MQTT_RC_RECEIVE_MAXIMUM_EXCEEDED:
      return "Receive Maximum exceeded";
    case MQTT_RC_TOPIC_ALIAS_INVALID:
      return "Topic Alias invalid";
    case MQTT_RC_PACKET_TOO_LARGE:
      return "Packet too large";
    case MQTT_RC_MESSAGE_RATE_TOO_HIGH:
      return "Message rate too high";
    case MQTT_RC_QUOTA_EXCEEDED:
      return "Quota exceeded";
    case MQTT_RC_ADMINISTRATIVE_ACTION:
      return "Administrative action";
    case MQTT_RC_PAYLOAD_FORMAT_INVALID:
      return "Payload format invalid";
    case MQTT_RC_RETAIN_NOT_SUPPORTED:
      return "Retain not supported";
    case MQTT_RC_QOS_NOT_SUPPORTED:
      return "QoS not supported";
    case MQTT_RC_USE_ANOTHER_SERVER:
      return "Use another server";
    case MQTT_RC_SERVER_MOVED:
      return "Server moved";
    case MQTT_RC_SHARED_SUBS_NOT_SUPPORTED:
      return "Shared Subscriptions not supported";
    case MQTT_RC_CONNECTION_RATE_EXCEEDED:
      return "Connection rate exceeded";
    case MQTT_RC_MAXIMUM_CONNECT_TIME:
      return "Maximum connect time";
    case MQTT_RC_SUBSCRIPTION_IDS_NOT_SUPPORTED:
      return "Subscription identifiers not supported";
    case MQTT_RC_WILDCARD_SUBS_NOT_SUPPORTED:
      return "Wildcard Subscriptions not supported";
    default:
      return "Unknown reason";
  }
}

int tmqtt_string_to_command(const char *str, int *cmd) {
  if (!strcasecmp(str, "connect")) {
    *cmd = CMD_CONNECT;
  } else if (!strcasecmp(str, "connack")) {
    *cmd = CMD_CONNACK;
  } else if (!strcasecmp(str, "publish")) {
    *cmd = CMD_PUBLISH;
  } else if (!strcasecmp(str, "puback")) {
    *cmd = CMD_PUBACK;
  } else if (!strcasecmp(str, "pubrec")) {
    *cmd = CMD_PUBREC;
  } else if (!strcasecmp(str, "pubrel")) {
    *cmd = CMD_PUBREL;
  } else if (!strcasecmp(str, "pubcomp")) {
    *cmd = CMD_PUBCOMP;
  } else if (!strcasecmp(str, "subscribe")) {
    *cmd = CMD_SUBSCRIBE;
  } else if (!strcasecmp(str, "unsubscribe")) {
    *cmd = CMD_UNSUBSCRIBE;
  } else if (!strcasecmp(str, "disconnect")) {
    *cmd = CMD_DISCONNECT;
  } else if (!strcasecmp(str, "auth")) {
    *cmd = CMD_AUTH;
  } else if (!strcasecmp(str, "will")) {
    *cmd = CMD_WILL;
  } else {
    return TTQ_ERR_INVAL;
  }
  return TTQ_ERR_SUCCESS;
}
