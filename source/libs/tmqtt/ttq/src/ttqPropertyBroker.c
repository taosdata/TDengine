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

#include "tmqttBrokerInt.h"

#include <stdio.h>
#include <string.h>

#include "ttqProperty.h"
#include "tmqttProto.h"

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
