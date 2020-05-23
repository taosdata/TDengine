/*******************************************************************************
 * Copyright (c) 2009, 2012 IBM Corp.
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

/**
 * @file
 * \brief This structure represents a persistent data store, used to store 
 * outbound and inbound messages, in order to achieve reliable messaging.
 *
 * The MQTT Client persists QoS1 and QoS2 messages in order to meet the
 * assurances of delivery associated with these @ref qos levels. The messages 
 * are saved in persistent storage
 * The type and context of the persistence implementation are specified when 
 * the MQTT client is created (see MQTTClient_create()). The default 
 * persistence type (::MQTTCLIENT_PERSISTENCE_DEFAULT) uses a file system-based
 * persistence mechanism. The <i>persistence_context</i> argument passed to 
 * MQTTClient_create() when using the default peristence is a string 
 * representing the location of the persistence directory. If the context 
 * argument is NULL, the working directory will be used. 
 *
 * To use memory-based persistence, an application passes 
 * ::MQTTCLIENT_PERSISTENCE_NONE as the <i>persistence_type</i> to 
 * MQTTClient_create(). This can lead to message loss in certain situations, 
 * but can be appropriate in some cases (see @ref qos).
 *
 * Client applications can provide their own persistence mechanism by passing
 * ::MQTTCLIENT_PERSISTENCE_USER as the <i>persistence_type</i>. To implement a
 * custom persistence mechanism, the application must pass an initialized
 * ::MQTTClient_persistence structure as the <i>persistence_context</i> 
 * argument to MQTTClient_create().
 *
 * If the functions defined return an ::MQTTCLIENT_PERSISTENCE_ERROR then the 
 * state of the persisted data should remain as it was prior to the function 
 * being called. For example, if Persistence_put() returns 
 * ::MQTTCLIENT_PERSISTENCE_ERROR, then it is assumed tha tthe persistent store
 * does not contain the data that was passed to the function. Similarly,  if 
 * Persistence_remove() returns ::MQTTCLIENT_PERSISTENCE_ERROR then it is 
 * assumed that the data to be removed is still held in the persistent store.
 *
 * It is up to the persistence implementation to log any error information that
 * may be required to diagnose a persistence mechanism failure.
 */

/*
/// @cond EXCLUDE
*/
#if !defined(MQTTCLIENTPERSISTENCE_H)
#define MQTTCLIENTPERSISTENCE_H
/*
/// @endcond
*/

/**
  * This <i>persistence_type</i> value specifies the default file system-based 
  * persistence mechanism (see MQTTClient_create()).
  */
#define MQTTCLIENT_PERSISTENCE_DEFAULT 0
/**
  * This <i>persistence_type</i> value specifies a memory-based 
  * persistence mechanism (see MQTTClient_create()).
  */
#define MQTTCLIENT_PERSISTENCE_NONE 1
/**
  * This <i>persistence_type</i> value specifies an application-specific 
  * persistence mechanism (see MQTTClient_create()).
  */
#define MQTTCLIENT_PERSISTENCE_USER 2

/** 
  * Application-specific persistence functions must return this error code if 
  * there is a problem executing the function. 
  */
#define MQTTCLIENT_PERSISTENCE_ERROR -2

/**
  * @brief Initialize the persistent store.
  * 
  * Either open the existing persistent store for this client ID or create a new
  * one if one doesn't exist. If the persistent store is already open, return 
  * without taking any action.
  *
  * An application can use the same client identifier to connect to many
  * different servers. The <i>clientid</i> in conjunction with the 
  * <i>serverURI</i> uniquely identifies the persistence store required.
  *
  * @param handle The address of a pointer to a handle for this persistence 
  * implementation. This function must set handle to a valid reference to the 
  * persistence following a successful return. 
  * The handle pointer is passed as an argument to all the other
  * persistence functions. It may include the context parameter and/or any other
  * data for use by the persistence functions.
  * @param clientID The client identifier for which the persistent store should 
  * be opened.
  * @param serverURI The connection string specified when the MQTT client was
  * created (see MQTTClient_create()).
  * @param context A pointer to any data required to initialize the persistent
  * store (see ::MQTTClient_persistence).
  * @return Return 0 if the function completes successfully, otherwise return
  * ::MQTTCLIENT_PERSISTENCE_ERROR.
  */
typedef int (*Persistence_open)(void** handle, const char* clientID, const char* serverURI, void* context);

/**
  * @brief Close the persistent store referred to by the handle.
  *
  * @param handle The handle pointer from a successful call to 
  * Persistence_open().
  * @return Return 0 if the function completes successfully, otherwise return
  * ::MQTTCLIENT_PERSISTENCE_ERROR.
  */
typedef int (*Persistence_close)(void* handle); 

/**
  * @brief Put the specified data into the persistent store.
  *
  * @param handle The handle pointer from a successful call to 
  * Persistence_open().
  * @param key A string used as the key for the data to be put in the store. The
  * key is later used to retrieve data from the store with Persistence_get().
  * @param bufcount The number of buffers to write to the persistence store.
  * @param buffers An array of pointers to the data buffers associated with 
  * this <i>key</i>.
  * @param buflens An array of lengths of the data buffers. <i>buflen[n]</i> 
  * gives the length of <i>buffer[n]</i>.
  * @return Return 0 if the function completes successfully, otherwise return
  * ::MQTTCLIENT_PERSISTENCE_ERROR.
  */
typedef int (*Persistence_put)(void* handle, char* key, int bufcount, char* buffers[], int buflens[]);

/**
  * @brief Retrieve the specified data from the persistent store. 
  *
  * @param handle The handle pointer from a successful call to 
  * Persistence_open().
  * @param key A string that is the key for the data to be retrieved. This is 
  * the same key used to save the data to the store with Persistence_put().
  * @param buffer The address of a pointer to a buffer. This function sets the
  * pointer to point at the retrieved data, if successful.
  * @param buflen The address of an int that is set to the length of 
  * <i>buffer</i> by this function if successful.
  * @return Return 0 if the function completes successfully, otherwise return
  * ::MQTTCLIENT_PERSISTENCE_ERROR.
  */
typedef int (*Persistence_get)(void* handle, char* key, char** buffer, int* buflen);

/**
  * @brief Remove the data for the specified key from the store.
  *
  * @param handle The handle pointer from a successful call to 
  * Persistence_open().
  * @param key A string that is the key for the data to be removed from the
  * store. This is the same key used to save the data to the store with 
  * Persistence_put().
  * @return Return 0 if the function completes successfully, otherwise return
  * ::MQTTCLIENT_PERSISTENCE_ERROR.
  */
typedef int (*Persistence_remove)(void* handle, char* key);

/**
  * @brief Returns the keys in this persistent data store.
  *
  * @param handle The handle pointer from a successful call to 
  * Persistence_open().
  * @param keys The address of a pointer to pointers to strings. Assuming
  * successful execution, this function allocates memory to hold the returned
  * keys (strings used to store the data with Persistence_put()). It also 
  * allocates memory to hold an array of pointers to these strings. <i>keys</i>
  * is set to point to the array of pointers to strings.
  * @param nkeys A pointer to the number of keys in this persistent data store. 
  * This function sets the number of keys, if successful.
  * @return Return 0 if the function completes successfully, otherwise return
  * ::MQTTCLIENT_PERSISTENCE_ERROR.
  */
typedef int (*Persistence_keys)(void* handle, char*** keys, int* nkeys);

/**
  * @brief Clears the persistence store, so that it no longer contains any 
  * persisted data.
  *
  * @param handle The handle pointer from a successful call to 
  * Persistence_open().
  * @return Return 0 if the function completes successfully, otherwise return
  * ::MQTTCLIENT_PERSISTENCE_ERROR.
  */
typedef int (*Persistence_clear)(void* handle);

/**
  * @brief Returns whether any data has been persisted using the specified key.
  *
  * @param handle The handle pointer from a successful call to 
  * Persistence_open().
  * @param key The string to be tested for existence in the store.
  * @return Return 0 if the key was found in the store, otherwise return
  * ::MQTTCLIENT_PERSISTENCE_ERROR.
  */
typedef int (*Persistence_containskey)(void* handle, char* key);

/**
  * @brief A structure containing the function pointers to a persistence 
  * implementation and the context or state that will be shared across all 
  * the persistence functions.
  */
typedef struct {
  /** 
    * A pointer to any data required to initialize the persistent store.
    */
	void* context;
  /** 
    * A function pointer to an implementation of Persistence_open().
    */
	Persistence_open popen;
  /** 
    * A function pointer to an implementation of Persistence_close().
    */
	Persistence_close pclose;
  /**
    * A function pointer to an implementation of Persistence_put().
    */
	Persistence_put pput;
  /** 
    * A function pointer to an implementation of Persistence_get().
    */
	Persistence_get pget;
  /** 
    * A function pointer to an implementation of Persistence_remove().
    */
	Persistence_remove premove;
  /** 
    * A function pointer to an implementation of Persistence_keys().
    */
	Persistence_keys pkeys;
  /** 
    * A function pointer to an implementation of Persistence_clear().
    */
	Persistence_clear pclear;
  /** 
    * A function pointer to an implementation of Persistence_containskey().
    */
	Persistence_containskey pcontainskey;
} MQTTClient_persistence;

#endif
