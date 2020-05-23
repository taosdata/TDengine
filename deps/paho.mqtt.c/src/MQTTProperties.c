/*******************************************************************************
 * Copyright (c) 2017, 2020 IBM Corp. and others
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

#include "MQTTProperties.h"

#include "MQTTPacket.h"
#include "MQTTProtocolClient.h"
#include "Heap.h"
#include "StackTrace.h"

#include <memory.h>

#define ARRAY_SIZE(a) (sizeof(a) / sizeof(a[0]))

static struct nameToType
{
  enum MQTTPropertyCodes name;
  enum MQTTPropertyTypes type;
} namesToTypes[] =
{
  {MQTTPROPERTY_CODE_PAYLOAD_FORMAT_INDICATOR, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_MESSAGE_EXPIRY_INTERVAL, MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_CONTENT_TYPE, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_RESPONSE_TOPIC, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_CORRELATION_DATA, MQTTPROPERTY_TYPE_BINARY_DATA},
  {MQTTPROPERTY_CODE_SUBSCRIPTION_IDENTIFIER, MQTTPROPERTY_TYPE_VARIABLE_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_SESSION_EXPIRY_INTERVAL, MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_ASSIGNED_CLIENT_IDENTIFER, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_SERVER_KEEP_ALIVE, MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_AUTHENTICATION_METHOD, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_AUTHENTICATION_DATA, MQTTPROPERTY_TYPE_BINARY_DATA},
  {MQTTPROPERTY_CODE_REQUEST_PROBLEM_INFORMATION, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_WILL_DELAY_INTERVAL, MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_REQUEST_RESPONSE_INFORMATION, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_RESPONSE_INFORMATION, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_SERVER_REFERENCE, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_REASON_STRING, MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING},
  {MQTTPROPERTY_CODE_RECEIVE_MAXIMUM, MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_TOPIC_ALIAS_MAXIMUM, MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_TOPIC_ALIAS, MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_MAXIMUM_QOS, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_RETAIN_AVAILABLE, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_USER_PROPERTY, MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR},
  {MQTTPROPERTY_CODE_MAXIMUM_PACKET_SIZE, MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER},
  {MQTTPROPERTY_CODE_WILDCARD_SUBSCRIPTION_AVAILABLE, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_SUBSCRIPTION_IDENTIFIERS_AVAILABLE, MQTTPROPERTY_TYPE_BYTE},
  {MQTTPROPERTY_CODE_SHARED_SUBSCRIPTION_AVAILABLE, MQTTPROPERTY_TYPE_BYTE}
};


static char* datadup(const MQTTLenString* str)
{
	char* temp = malloc(str->len);
	if (temp)
		memcpy(temp, str->data, str->len);
	return temp;
}


int MQTTProperty_getType(enum MQTTPropertyCodes value)
{
  int i, rc = -1;

  for (i = 0; i < ARRAY_SIZE(namesToTypes); ++i)
  {
    if (namesToTypes[i].name == value)
    {
      rc = namesToTypes[i].type;
      break;
    }
  }
  return rc;
}


int MQTTProperties_len(MQTTProperties* props)
{
  /* properties length is an mbi */
  return (props == NULL) ? 1 : props->length + MQTTPacket_VBIlen(props->length);
}


/**
 * Add a new property to a property list
 * @param props the property list
 * @param prop the new property
 * @return code 0 is success
 */
int MQTTProperties_add(MQTTProperties* props, const MQTTProperty* prop)
{
  int rc = 0, type;

  if ((type = MQTTProperty_getType(prop->identifier)) < 0)
  {
	/*StackTrace_printStack(stdout);*/
    rc = MQTT_INVALID_PROPERTY_ID;
    goto exit;
  }
  else if (props->array == NULL)
  {
    props->max_count = 10;
    props->array = malloc(sizeof(MQTTProperty) * props->max_count);
  }
  else if (props->count == props->max_count)
  {
    props->max_count += 10;
    props->array = realloc(props->array, sizeof(MQTTProperty) * props->max_count);
  }

  if (props->array)
  {
    int len = 0;

    props->array[props->count++] = *prop;
    /* calculate length */
    switch (type)
    {
      case MQTTPROPERTY_TYPE_BYTE:
        len = 1;
        break;
      case MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER:
        len = 2;
        break;
      case MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER:
        len = 4;
        break;
      case MQTTPROPERTY_TYPE_VARIABLE_BYTE_INTEGER:
        len = MQTTPacket_VBIlen(prop->value.integer4);
        break;
      case MQTTPROPERTY_TYPE_BINARY_DATA:
      case MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING:
      case MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR:
        len = 2 + prop->value.data.len;
        props->array[props->count-1].value.data.data = datadup(&prop->value.data);
        if (type == MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR)
        {
          len += 2 + prop->value.value.len;
          props->array[props->count-1].value.value.data = datadup(&prop->value.value);
        }
        break;
    }
    props->length += len + 1; /* add identifier byte */
  }
  else
	  rc = PAHO_MEMORY_ERROR;

exit:
  return rc;
}


int MQTTProperty_write(char** pptr, MQTTProperty* prop)
{
  int rc = -1,
      type = -1;

  type = MQTTProperty_getType(prop->identifier);
  if (type >= MQTTPROPERTY_TYPE_BYTE && type <= MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR)
  {
    writeChar(pptr, prop->identifier);
    switch (type)
    {
      case MQTTPROPERTY_TYPE_BYTE:
        writeChar(pptr, prop->value.byte);
        rc = 1;
        break;
      case MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER:
        writeInt(pptr, prop->value.integer2);
        rc = 2;
        break;
      case MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER:
        writeInt4(pptr, prop->value.integer4);
        rc = 4;
        break;
      case MQTTPROPERTY_TYPE_VARIABLE_BYTE_INTEGER:
        rc = MQTTPacket_encode(*pptr, prop->value.integer4);
        *pptr += rc;
        break;
      case MQTTPROPERTY_TYPE_BINARY_DATA:
      case MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING:
        writeMQTTLenString(pptr, prop->value.data);
        rc = prop->value.data.len + 2; /* include length field */
        break;
      case MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR:
        writeMQTTLenString(pptr, prop->value.data);
        writeMQTTLenString(pptr, prop->value.value);
        rc = prop->value.data.len + prop->value.value.len + 4; /* include length fields */
        break;
    }
  }
  return rc + 1; /* include identifier byte */
}


int MQTTProperties_write(char** pptr, const MQTTProperties* properties)
{
  int rc = -1;
  int i = 0, len = 0;

  /* write the entire property list length first */
  if (properties == NULL)
  {
	*pptr += MQTTPacket_encode(*pptr, 0);
	rc = 1;
  }
  else
  {
    *pptr += MQTTPacket_encode(*pptr, properties->length);
    len = rc = 1;
    for (i = 0; i < properties->count; ++i)
    {
      rc = MQTTProperty_write(pptr, &properties->array[i]);
      if (rc < 0)
        break;
      else
        len += rc;
    }
    if (rc >= 0)
      rc = len;
  }
  return rc;
}


int MQTTProperty_read(MQTTProperty* prop, char** pptr, char* enddata)
{
  int type = -1,
    len = 0;

  prop->identifier = readChar(pptr);
  type = MQTTProperty_getType(prop->identifier);
  if (type >= MQTTPROPERTY_TYPE_BYTE && type <= MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR)
  {
    switch (type)
    {
      case MQTTPROPERTY_TYPE_BYTE:
        prop->value.byte = readChar(pptr);
        len = 1;
        break;
      case MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER:
        prop->value.integer2 = readInt(pptr);
        len = 2;
        break;
      case MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER:
        prop->value.integer4 = readInt4(pptr);
        len = 4;
        break;
      case MQTTPROPERTY_TYPE_VARIABLE_BYTE_INTEGER:
        len = MQTTPacket_decodeBuf(*pptr, &prop->value.integer4);
        *pptr += len;
        break;
      case MQTTPROPERTY_TYPE_BINARY_DATA:
      case MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING:
      case MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR:
        len = MQTTLenStringRead(&prop->value.data, pptr, enddata);
        prop->value.data.data = datadup(&prop->value.data);
        if (type == MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR)
        {
          len += MQTTLenStringRead(&prop->value.value, pptr, enddata);
          prop->value.value.data = datadup(&prop->value.value);
        }
        break;
    }
  }
  return len + 1; /* 1 byte for identifier */
}


int MQTTProperties_read(MQTTProperties* properties, char** pptr, char* enddata)
{
  int rc = 0;
  unsigned int remlength = 0;

  FUNC_ENTRY;
  /* we assume an initialized properties structure */
  if (enddata - (*pptr) > 0) /* enough length to read the VBI? */
  {
    *pptr += MQTTPacket_decodeBuf(*pptr, &remlength);
    properties->length = remlength;
    while (remlength > 0)
    {
      if (properties->count == properties->max_count)
      {
    	properties->max_count += 10;
    	if (properties->max_count == 10)
    	  properties->array = malloc(sizeof(MQTTProperty) * properties->max_count);
    	else
    	  properties->array = realloc(properties->array, sizeof(MQTTProperty) * properties->max_count);
      }
      if (properties->array == NULL)
      {
    	rc = PAHO_MEMORY_ERROR;
        goto exit;
      }
      remlength -= MQTTProperty_read(&properties->array[properties->count], pptr, enddata);
      properties->count++;
    }
    if (remlength == 0)
      rc = 1; /* data read successfully */
  }

  if (rc != 1 && properties->array != NULL)
  {
	  free(properties->array);
	  properties->array = NULL;
	  properties->max_count = properties->count = 0;
  }

exit:
  FUNC_EXIT_RC(rc);
  return rc;
}

struct {
	enum MQTTPropertyCodes value;
	const char* name;
} nameToString[] =
{
  {MQTTPROPERTY_CODE_PAYLOAD_FORMAT_INDICATOR, "PAYLOAD_FORMAT_INDICATOR"},
  {MQTTPROPERTY_CODE_MESSAGE_EXPIRY_INTERVAL, "MESSAGE_EXPIRY_INTERVAL"},
  {MQTTPROPERTY_CODE_CONTENT_TYPE, "CONTENT_TYPE"},
  {MQTTPROPERTY_CODE_RESPONSE_TOPIC, "RESPONSE_TOPIC"},
  {MQTTPROPERTY_CODE_CORRELATION_DATA, "CORRELATION_DATA"},
  {MQTTPROPERTY_CODE_SUBSCRIPTION_IDENTIFIER, "SUBSCRIPTION_IDENTIFIER"},
  {MQTTPROPERTY_CODE_SESSION_EXPIRY_INTERVAL, "SESSION_EXPIRY_INTERVAL"},
  {MQTTPROPERTY_CODE_ASSIGNED_CLIENT_IDENTIFER, "ASSIGNED_CLIENT_IDENTIFER"},
  {MQTTPROPERTY_CODE_SERVER_KEEP_ALIVE, "SERVER_KEEP_ALIVE"},
  {MQTTPROPERTY_CODE_AUTHENTICATION_METHOD, "AUTHENTICATION_METHOD"},
  {MQTTPROPERTY_CODE_AUTHENTICATION_DATA, "AUTHENTICATION_DATA"},
  {MQTTPROPERTY_CODE_REQUEST_PROBLEM_INFORMATION, "REQUEST_PROBLEM_INFORMATION"},
  {MQTTPROPERTY_CODE_WILL_DELAY_INTERVAL, "WILL_DELAY_INTERVAL"},
  {MQTTPROPERTY_CODE_REQUEST_RESPONSE_INFORMATION, "REQUEST_RESPONSE_INFORMATION"},
  {MQTTPROPERTY_CODE_RESPONSE_INFORMATION, "RESPONSE_INFORMATION"},
  {MQTTPROPERTY_CODE_SERVER_REFERENCE, "SERVER_REFERENCE"},
  {MQTTPROPERTY_CODE_REASON_STRING, "REASON_STRING"},
  {MQTTPROPERTY_CODE_RECEIVE_MAXIMUM, "RECEIVE_MAXIMUM"},
  {MQTTPROPERTY_CODE_TOPIC_ALIAS_MAXIMUM, "TOPIC_ALIAS_MAXIMUM"},
  {MQTTPROPERTY_CODE_TOPIC_ALIAS, "TOPIC_ALIAS"},
  {MQTTPROPERTY_CODE_MAXIMUM_QOS, "MAXIMUM_QOS"},
  {MQTTPROPERTY_CODE_RETAIN_AVAILABLE, "RETAIN_AVAILABLE"},
  {MQTTPROPERTY_CODE_USER_PROPERTY, "USER_PROPERTY"},
  {MQTTPROPERTY_CODE_MAXIMUM_PACKET_SIZE, "MAXIMUM_PACKET_SIZE"},
  {MQTTPROPERTY_CODE_WILDCARD_SUBSCRIPTION_AVAILABLE, "WILDCARD_SUBSCRIPTION_AVAILABLE"},
  {MQTTPROPERTY_CODE_SUBSCRIPTION_IDENTIFIERS_AVAILABLE, "SUBSCRIPTION_IDENTIFIERS_AVAILABLE"},
  {MQTTPROPERTY_CODE_SHARED_SUBSCRIPTION_AVAILABLE, "SHARED_SUBSCRIPTION_AVAILABLE"}
};

const char* MQTTPropertyName(enum MQTTPropertyCodes value)
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


void MQTTProperties_free(MQTTProperties* props)
{
  int i = 0;

  FUNC_ENTRY;
  if (props == NULL)
    goto exit;
  for (i = 0; i < props->count; ++i)
  {
    int id = props->array[i].identifier;
    int type = MQTTProperty_getType(id);

    switch (type)
    {
    case MQTTPROPERTY_TYPE_BINARY_DATA:
    	case MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING:
    	case MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR:
    	  free(props->array[i].value.data.data);
    	  if (type == MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR)
    	    free(props->array[i].value.value.data);
	  break;
    }
  }
  if (props->array)
    free(props->array);
  memset(props, '\0', sizeof(MQTTProperties)); /* zero all fields */
exit:
  FUNC_EXIT;
}


MQTTProperties MQTTProperties_copy(const MQTTProperties* props)
{
  int i = 0;
  MQTTProperties result = MQTTProperties_initializer;

  FUNC_ENTRY;
  for (i = 0; i < props->count; ++i)
  {
	int rc = 0;

	if ((rc = MQTTProperties_add(&result, &props->array[i])) != 0)
		Log(LOG_ERROR, -1, "Error from MQTTProperties add %d", rc);
  }

  FUNC_EXIT;
  return result;
}


int MQTTProperties_hasProperty(MQTTProperties *props, enum MQTTPropertyCodes propid)
{
	int i = 0;
	int found = 0;

	for (i = 0; i < props->count; ++i)
	{
		if (propid == props->array[i].identifier)
		{
			found = 1;
			break;
		}
	}
	return found;
}


int MQTTProperties_propertyCount(MQTTProperties *props, enum MQTTPropertyCodes propid)
{
	int i = 0;
	int count = 0;

	for (i = 0; i < props->count; ++i)
	{
		if (propid == props->array[i].identifier)
			count++;
	}
	return count;
}


int MQTTProperties_getNumericValueAt(MQTTProperties *props, enum MQTTPropertyCodes propid, int index)
{
	int i = 0;
	int rc = -9999999;
	int cur_index = 0;

	for (i = 0; i < props->count; ++i)
	{
		int id = props->array[i].identifier;

		if (id == propid)
		{
			if (cur_index < index)
			{
				cur_index++;
				continue;
			}
			switch (MQTTProperty_getType(id))
			{
			case MQTTPROPERTY_TYPE_BYTE:
				rc = props->array[i].value.byte;
				break;
			case MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER:
				rc = props->array[i].value.integer2;
				break;
			case MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER:
			case MQTTPROPERTY_TYPE_VARIABLE_BYTE_INTEGER:
				rc = props->array[i].value.integer4;
				break;
			default:
				rc = -999999;
				break;
			}
			break;
		}
	}
	return rc;
}


int MQTTProperties_getNumericValue(MQTTProperties *props, enum MQTTPropertyCodes propid)
{
	return MQTTProperties_getNumericValueAt(props, propid, 0);
}


MQTTProperty* MQTTProperties_getPropertyAt(MQTTProperties *props, enum MQTTPropertyCodes propid, int index)
{
	int i = 0;
	MQTTProperty* result = NULL;
	int cur_index = 0;

	for (i = 0; i < props->count; ++i)
	{
		int id = props->array[i].identifier;

		if (id == propid)
		{
			if (cur_index == index)
			{
				result = &props->array[i];
				break;
			}
			else
				cur_index++;
		}
	}
	return result;
}


MQTTProperty* MQTTProperties_getProperty(MQTTProperties *props, enum MQTTPropertyCodes propid)
{
	return MQTTProperties_getPropertyAt(props, propid, 0);
}
