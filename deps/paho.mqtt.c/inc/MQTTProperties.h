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

#if !defined(MQTTPROPERTIES_H)
#define MQTTPROPERTIES_H

#include "MQTTExportDeclarations.h"

#define MQTT_INVALID_PROPERTY_ID -2

/** The one byte MQTT V5 property indicator */
enum MQTTPropertyCodes {
  MQTTPROPERTY_CODE_PAYLOAD_FORMAT_INDICATOR = 1,  /**< The value is 1 */
  MQTTPROPERTY_CODE_MESSAGE_EXPIRY_INTERVAL = 2,   /**< The value is 2 */
  MQTTPROPERTY_CODE_CONTENT_TYPE = 3,              /**< The value is 3 */
  MQTTPROPERTY_CODE_RESPONSE_TOPIC = 8,            /**< The value is 8 */
  MQTTPROPERTY_CODE_CORRELATION_DATA = 9,          /**< The value is 9 */
  MQTTPROPERTY_CODE_SUBSCRIPTION_IDENTIFIER = 11,  /**< The value is 11 */
  MQTTPROPERTY_CODE_SESSION_EXPIRY_INTERVAL = 17,  /**< The value is 17 */
  MQTTPROPERTY_CODE_ASSIGNED_CLIENT_IDENTIFER = 18,/**< The value is 18 */
  MQTTPROPERTY_CODE_SERVER_KEEP_ALIVE = 19,        /**< The value is 19 */
  MQTTPROPERTY_CODE_AUTHENTICATION_METHOD = 21,    /**< The value is 21 */
  MQTTPROPERTY_CODE_AUTHENTICATION_DATA = 22,      /**< The value is 22 */
  MQTTPROPERTY_CODE_REQUEST_PROBLEM_INFORMATION = 23,/**< The value is 23 */
  MQTTPROPERTY_CODE_WILL_DELAY_INTERVAL = 24,      /**< The value is 24 */
  MQTTPROPERTY_CODE_REQUEST_RESPONSE_INFORMATION = 25,/**< The value is 25 */
  MQTTPROPERTY_CODE_RESPONSE_INFORMATION = 26,     /**< The value is 26 */
  MQTTPROPERTY_CODE_SERVER_REFERENCE = 28,         /**< The value is 28 */
  MQTTPROPERTY_CODE_REASON_STRING = 31,            /**< The value is 31 */
  MQTTPROPERTY_CODE_RECEIVE_MAXIMUM = 33,          /**< The value is 33*/
  MQTTPROPERTY_CODE_TOPIC_ALIAS_MAXIMUM = 34,      /**< The value is 34 */
  MQTTPROPERTY_CODE_TOPIC_ALIAS = 35,              /**< The value is 35 */
  MQTTPROPERTY_CODE_MAXIMUM_QOS = 36,              /**< The value is 36 */
  MQTTPROPERTY_CODE_RETAIN_AVAILABLE = 37,         /**< The value is 37 */
  MQTTPROPERTY_CODE_USER_PROPERTY = 38,            /**< The value is 38 */
  MQTTPROPERTY_CODE_MAXIMUM_PACKET_SIZE = 39,      /**< The value is 39 */
  MQTTPROPERTY_CODE_WILDCARD_SUBSCRIPTION_AVAILABLE = 40,/**< The value is 40 */
  MQTTPROPERTY_CODE_SUBSCRIPTION_IDENTIFIERS_AVAILABLE = 41,/**< The value is 41 */
  MQTTPROPERTY_CODE_SHARED_SUBSCRIPTION_AVAILABLE = 42/**< The value is 241 */
};

/**
 * Returns a printable string description of an MQTT V5 property code.
 * @param value an MQTT V5 property code.
 * @return the printable string description of the input property code.
 * NULL if the code was not found.
 */
LIBMQTT_API const char* MQTTPropertyName(enum MQTTPropertyCodes value);

/** The one byte MQTT V5 property type */
enum MQTTPropertyTypes {
  MQTTPROPERTY_TYPE_BYTE,
  MQTTPROPERTY_TYPE_TWO_BYTE_INTEGER,
  MQTTPROPERTY_TYPE_FOUR_BYTE_INTEGER,
  MQTTPROPERTY_TYPE_VARIABLE_BYTE_INTEGER,
  MQTTPROPERTY_TYPE_BINARY_DATA,
  MQTTPROPERTY_TYPE_UTF_8_ENCODED_STRING,
  MQTTPROPERTY_TYPE_UTF_8_STRING_PAIR
};

/**
 * Returns the MQTT V5 type code of an MQTT V5 property.
 * @param value an MQTT V5 property code.
 * @return the MQTT V5 type code of the input property. -1 if the code was not found.
 */
LIBMQTT_API int MQTTProperty_getType(enum MQTTPropertyCodes value);

/**
 * The data for a length delimited string
 */
typedef struct
{
	int len; /**< the length of the string */
	char* data; /**< pointer to the string data */
} MQTTLenString;


/**
 * Structure to hold an MQTT version 5 property of any type
 */
typedef struct
{
  enum MQTTPropertyCodes identifier; /**<  The MQTT V5 property id. A multi-byte integer. */
  /** The value of the property, as a union of the different possible types. */
  union {
    unsigned char byte;       /**< holds the value of a byte property type */
    unsigned short integer2;  /**< holds the value of a 2 byte integer property type */
    unsigned int integer4;    /**< holds the value of a 4 byte integer property type */
    struct {
      MQTTLenString data;  /**< The value of a string property, or the name of a user property. */
      MQTTLenString value; /**< The value of a user property. */
    };
  } value;
} MQTTProperty;

/**
 * MQTT version 5 property list
 */
typedef struct MQTTProperties
{
  int count;     /**< number of property entries in the array */
  int max_count; /**< max number of properties that the currently allocated array can store */
  int length;    /**< mbi: byte length of all properties */
  MQTTProperty *array;  /**< array of properties */
} MQTTProperties;

#define MQTTProperties_initializer {0, 0, 0, NULL}

/**
 * Returns the length of the properties structure when serialized ready for network transmission.
 * @param props an MQTT V5 property structure.
 * @return the length in bytes of the properties when serialized.
 */
int MQTTProperties_len(MQTTProperties* props);

/**
 * Add a property pointer to the property array.  There is no memory allocation.
 * @param props The property list to add the property to.
 * @param prop The property to add to the list.
 * @return 0 on success, -1 on failure.
 */
LIBMQTT_API int MQTTProperties_add(MQTTProperties* props, const MQTTProperty* prop);

/**
 * Serialize the given property list to a character buffer, e.g. for writing to the network.
 * @param pptr pointer to the buffer - move the pointer as we add data
 * @param properties pointer to the property list, can be NULL
 * @return whether the write succeeded or not: number of bytes written, or < 0 on failure.
 */
int MQTTProperties_write(char** pptr, const MQTTProperties* properties);

/**
 * Reads a property list from a character buffer into an array.
 * @param properties pointer to the property list to be filled. Should be initalized but empty.
 * @param pptr pointer to the character buffer.
 * @param enddata pointer to the end of the character buffer so we don't read beyond.
 * @return 1 if the properties were read successfully.
 */
int MQTTProperties_read(MQTTProperties* properties, char** pptr, char* enddata);

/**
 * Free all memory allocated to the property list, including any to individual properties.
 * @param properties pointer to the property list.
 */
LIBMQTT_API void MQTTProperties_free(MQTTProperties* properties);

/**
 * Copy the contents of a property list, allocating additional memory if needed.
 * @param props pointer to the property list.
 * @return the duplicated property list.
 */
LIBMQTT_API MQTTProperties MQTTProperties_copy(const MQTTProperties* props);

/**
 * Checks if property list contains a specific property.
 * @param props pointer to the property list.
 * @param propid the property id to check for.
 * @return 1 if found, 0 if not.
 */
LIBMQTT_API int MQTTProperties_hasProperty(MQTTProperties *props, enum MQTTPropertyCodes propid);

/**
 * Returns the number of instances of a property id. Most properties can exist only once.
 * User properties and subscription ids can exist more than once.
 * @param props pointer to the property list.
 * @param propid the property id to check for.
 * @return the number of times found.  Can be 0.
 */
LIBMQTT_API int MQTTProperties_propertyCount(MQTTProperties *props, enum MQTTPropertyCodes propid);

/**
 * Returns the integer value of a specific property.  The property given must be a numeric type.
 * @param props pointer to the property list.
 * @param propid the property id to check for.
 * @return the integer value of the property. -9999999 on failure.
 */
LIBMQTT_API int MQTTProperties_getNumericValue(MQTTProperties *props, enum MQTTPropertyCodes propid);

/**
 * Returns the integer value of a specific property when it's not the only instance.
 * The property given must be a numeric type.
 * @param props pointer to the property list.
 * @param propid the property id to check for.
 * @param index the instance number, starting at 0.
 * @return the integer value of the property. -9999999 on failure.
 */
LIBMQTT_API int MQTTProperties_getNumericValueAt(MQTTProperties *props, enum MQTTPropertyCodes propid, int index);

/**
 * Returns a pointer to the property structure for a specific property.
 * @param props pointer to the property list.
 * @param propid the property id to check for.
 * @return the pointer to the property structure if found.  NULL if not found.
 */
LIBMQTT_API MQTTProperty* MQTTProperties_getProperty(MQTTProperties *props, enum MQTTPropertyCodes propid);

/**
 * Returns a pointer to the property structure for a specific property when it's not the only instance.
 * @param props pointer to the property list.
 * @param propid the property id to check for.
 * @param index the instance number, starting at 0.
 * @return the pointer to the property structure if found.  NULL if not found.
 */
LIBMQTT_API MQTTProperty* MQTTProperties_getPropertyAt(MQTTProperties *props, enum MQTTPropertyCodes propid, int index);

#endif /* MQTTPROPERTIES_H */
