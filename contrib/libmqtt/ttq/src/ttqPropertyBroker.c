#include "tmqttBrokerInt.h"

#include <stdio.h>
#include <string.h>

#include "tmqttProto.h"
#include "ttqProperty.h"

int ttqPropertyProcessWill(struct tmqtt *context, struct tmqtt_message_all *msg, tmqtt_property **props) {
  tmqtt_property *p, *p_prev;
  tmqtt_property *msg_properties, *msg_properties_last;

  p = *props;
  p_prev = NULL;
  msg_properties = NULL;
  msg_properties_last = NULL;
  while (p) {
    switch (p->identifier) {
      case MQTT_PROP_CONTENT_TYPE:
      case MQTT_PROP_CORRELATION_DATA:
      case MQTT_PROP_PAYLOAD_FORMAT_INDICATOR:
      case MQTT_PROP_RESPONSE_TOPIC:
      case MQTT_PROP_USER_PROPERTY:
        if (msg_properties) {
          msg_properties_last->next = p;
          msg_properties_last = p;
        } else {
          msg_properties = p;
          msg_properties_last = p;
        }
        if (p_prev) {
          p_prev->next = p->next;
          p = p_prev->next;
        } else {
          *props = p->next;
          p = *props;
        }
        msg_properties_last->next = NULL;
        break;

      case MQTT_PROP_WILL_DELAY_INTERVAL:
        context->will_delay_interval = p->value.i32;
        p_prev = p;
        p = p->next;
        break;

      case MQTT_PROP_MESSAGE_EXPIRY_INTERVAL:
        msg->expiry_interval = p->value.i32;
        p_prev = p;
        p = p->next;
        break;

      default:
        msg->properties = msg_properties;
        return TTQ_ERR_PROTOCOL;
        break;
    }
  }

  msg->properties = msg_properties;
  return TTQ_ERR_SUCCESS;
}
