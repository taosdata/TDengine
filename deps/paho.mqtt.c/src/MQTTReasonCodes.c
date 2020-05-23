/*******************************************************************************
 * Copyright (c) 2017, 2018 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v2.0
 * and Eclipse Distribution License v1.0 which accompany this distribution.
 *
 * The Eclipse Public License is available at
 *    https://www.eclipse.org/legal/epl-2.0/
 * and the Eclipse Distribution License is available at
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *******************************************************************************/

#include "MQTTReasonCodes.h"

#include "Heap.h"
#include "StackTrace.h"

#include <memory.h>

#define ARRAY_SIZE(a) (sizeof(a) / sizeof(a[0]))

static struct {
	enum MQTTReasonCodes value;
	const char* name;
} nameToString[] =
{
  {MQTTREASONCODE_SUCCESS, "SUCCESS"},
  {MQTTREASONCODE_NORMAL_DISCONNECTION, "Normal disconnection"},
  {MQTTREASONCODE_GRANTED_QOS_0, "Granted QoS 0"},
  {MQTTREASONCODE_GRANTED_QOS_1, "Granted QoS 1"},
  {MQTTREASONCODE_GRANTED_QOS_2, "Granted QoS 2"},
  {MQTTREASONCODE_DISCONNECT_WITH_WILL_MESSAGE, "Disconnect with Will Message"},
  {MQTTREASONCODE_NO_MATCHING_SUBSCRIBERS, "No matching subscribers"},
  {MQTTREASONCODE_NO_SUBSCRIPTION_FOUND, "No subscription found"},
  {MQTTREASONCODE_CONTINUE_AUTHENTICATION, "Continue authentication"},
  {MQTTREASONCODE_RE_AUTHENTICATE, "Re-authenticate"},
  {MQTTREASONCODE_UNSPECIFIED_ERROR, "Unspecified error"},
  {MQTTREASONCODE_MALFORMED_PACKET, "Malformed Packet"},
  {MQTTREASONCODE_PROTOCOL_ERROR, "Protocol error"},
  {MQTTREASONCODE_IMPLEMENTATION_SPECIFIC_ERROR, "Implementation specific error"},
  {MQTTREASONCODE_UNSUPPORTED_PROTOCOL_VERSION, "Unsupported Protocol Version"},
  {MQTTREASONCODE_CLIENT_IDENTIFIER_NOT_VALID, "Client Identifier not valid"},
  {MQTTREASONCODE_BAD_USER_NAME_OR_PASSWORD, "Bad User Name or Password"},
  {MQTTREASONCODE_NOT_AUTHORIZED, "Not authorized"},
  {MQTTREASONCODE_SERVER_UNAVAILABLE, "Server unavailable"},
  {MQTTREASONCODE_SERVER_BUSY, "Server busy"},
  {MQTTREASONCODE_BANNED, "Banned"},
  {MQTTREASONCODE_SERVER_SHUTTING_DOWN, "Server shutting down"},
  {MQTTREASONCODE_BAD_AUTHENTICATION_METHOD, "Bad authentication method"},
  {MQTTREASONCODE_KEEP_ALIVE_TIMEOUT, "Keep Alive timeout"},
  {MQTTREASONCODE_SESSION_TAKEN_OVER, "Session taken over"},
  {MQTTREASONCODE_TOPIC_FILTER_INVALID, "Topic filter invalid"},
  {MQTTREASONCODE_TOPIC_NAME_INVALID, "Topic name invalid"},
  {MQTTREASONCODE_PACKET_IDENTIFIER_IN_USE, "Packet Identifier in use"},
  {MQTTREASONCODE_PACKET_IDENTIFIER_NOT_FOUND, "Packet Identifier not found"},
  {MQTTREASONCODE_RECEIVE_MAXIMUM_EXCEEDED, "Receive Maximum exceeded"},
  {MQTTREASONCODE_TOPIC_ALIAS_INVALID, "Topic Alias invalid"},
  {MQTTREASONCODE_PACKET_TOO_LARGE, "Packet too large"},
  {MQTTREASONCODE_MESSAGE_RATE_TOO_HIGH, "Message rate too high"},
  {MQTTREASONCODE_QUOTA_EXCEEDED, "Quota exceeded"},
  {MQTTREASONCODE_ADMINISTRATIVE_ACTION, "Administrative action"},
  {MQTTREASONCODE_PAYLOAD_FORMAT_INVALID, "Payload format invalid"},
  {MQTTREASONCODE_RETAIN_NOT_SUPPORTED, "Retain not supported"},
  {MQTTREASONCODE_QOS_NOT_SUPPORTED, "QoS not supported"},
  {MQTTREASONCODE_USE_ANOTHER_SERVER, "Use another server"},
  {MQTTREASONCODE_SERVER_MOVED, "Server moved"},
  {MQTTREASONCODE_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED, "Shared subscriptions not supported"},
  {MQTTREASONCODE_CONNECTION_RATE_EXCEEDED, "Connection rate exceeded"},
  {MQTTREASONCODE_MAXIMUM_CONNECT_TIME, "Maximum connect time"},
  {MQTTREASONCODE_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED, "Subscription Identifiers not supported"},
  {MQTTREASONCODE_WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED, "Wildcard Subscriptions not supported"}
};

const char* MQTTReasonCode_toString(enum MQTTReasonCodes value)
{
  int i = 0;
  const char* result = NULL;

  for (i = 0; i < ARRAY_SIZE(nameToString); ++i)
  {
    if (nameToString[i].value == value)
    {
    	  result = nameToString[i].name;
    	  break;
    }
  }
  return result;
}








