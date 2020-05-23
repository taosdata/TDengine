/*******************************************************************************
 * Copyright (c) 2009, 2020 IBM Corp. and others
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
 *    Ian Craggs, Allan Stockdill-Mander - SSL updates
 *    Ian Craggs - multiple server connection support
 *    Ian Craggs - MQTT 3.1.1 support
 *    Ian Craggs - remove const from eyecatchers #168
 *******************************************************************************/

/**
 * @cond MQTTClient_internal
 * @mainpage MQTT Client Library Internals
 * In the beginning there was one MQTT C client library, MQTTClient, as implemented in MQTTClient.c
 * This library was designed to be easy to use for applications which didn't mind if some of the calls
 * blocked for a while.  For instance, the MQTTClient_connect call will block until a successful
 * connection has completed, or a connection has failed, which could be as long as the "connection
 * timeout" interval, whose default is 30 seconds.
 *
 * However in mobile devices and other windowing environments, blocking on the GUI thread is a bad
 * thing as it causes the user interface to freeze.  Hence a new API, MQTTAsync, implemented
 * in MQTTAsync.c, was devised.  There are no blocking calls in this library, so it is well suited
 * to GUI and mobile environments, at the expense of some extra complexity.
 *
 * Both libraries are designed to be sparing in the use of threads.  So multiple client objects are
 * handled by one or two threads, with a select call in Socket_getReadySocket(), used to determine
 * when a socket has incoming data.  This API is thread safe: functions may be called by multiple application
 * threads, with the exception of ::MQTTClient_yield and ::MQTTClient_receive, which are intended
 * for single threaded environments only.
 *
 * @endcond
 * @cond MQTTClient_main
 * @mainpage MQTT Client library for C
 * &copy; Copyright IBM Corp. 2009, 2020 and others
 *
 * @brief An MQTT client library in C.
 *
 * These pages describe the original more synchronous API which might be
 * considered easier to use.  Some of the calls will block.  For the new
 * totally asynchronous API where no calls block, which is especially suitable
 * for use in windowed environments, see the
 * <a href="../../MQTTAsync/html/index.html">MQTT C Client Asynchronous API Documentation</a>.
 * The MQTTClient API is not thread safe, whereas the MQTTAsync API is.
 *
 * An MQTT client application connects to MQTT-capable servers.
 * A typical client is responsible for collecting information from a telemetry
 * device and publishing the information to the server. It can also subscribe
 * to topics, receive messages, and use this information to control the
 * telemetry device.
 *
 * MQTT clients implement the published MQTT v3 protocol. You can write your own
 * API to the MQTT protocol using the programming language and platform of your
 * choice. This can be time-consuming and error-prone.
 *
 * To simplify writing MQTT client applications, this library encapsulates
 * the MQTT v3 protocol for you. Using this library enables a fully functional
 * MQTT client application to be written in a few lines of code.
 * The information presented here documents the API provided
 * by the MQTT Client library for C.
 *
 * <b>Using the client</b><br>
 * Applications that use the client library typically use a similar structure:
 * <ul>
 * <li>Create a client object</li>
 * <li>Set the options to connect to an MQTT server</li>
 * <li>Set up callback functions if multi-threaded (asynchronous mode)
 * operation is being used (see @ref async).</li>
 * <li>Subscribe to any topics the client needs to receive</li>
 * <li>Repeat until finished:</li>
 *     <ul>
 *     <li>Publish any messages the client needs to</li>
 *     <li>Handle any incoming messages</li>
 *     </ul>
 * <li>Disconnect the client</li>
 * <li>Free any memory being used by the client</li>
 * </ul>
 * Some simple examples are shown here:
 * <ul>
 * <li>@ref pubsync</li>
 * <li>@ref pubasync</li>
 * <li>@ref subasync</li>
 * </ul>
 * Additional information about important concepts is provided here:
 * <ul>
 * <li>@ref async</li>
 * <li>@ref wildcard</li>
 * <li>@ref qos</li>
 * <li>@ref tracing</li>
 * </ul>
 * @endcond
 */

/*
/// @cond EXCLUDE
*/
#if !defined(MQTTCLIENT_H)
#define MQTTCLIENT_H

#if defined(__cplusplus)
 extern "C" {
#endif

#include <stdio.h>
/*
/// @endcond
*/

#include "MQTTExportDeclarations.h"

#include "MQTTProperties.h"
#include "MQTTReasonCodes.h"
#include "MQTTSubscribeOpts.h"
#if !defined(NO_PERSISTENCE)
#include "MQTTClientPersistence.h"
#endif

/**
 * Return code: No error. Indicates successful completion of an MQTT client
 * operation.
 */
#define MQTTCLIENT_SUCCESS 0
/**
 * Return code: A generic error code indicating the failure of an MQTT client
 * operation.
 */
#define MQTTCLIENT_FAILURE -1

/* error code -2 is MQTTCLIENT_PERSISTENCE_ERROR */

/**
 * Return code: The client is disconnected.
 */
#define MQTTCLIENT_DISCONNECTED -3
/**
 * Return code: The maximum number of messages allowed to be simultaneously
 * in-flight has been reached.
 */
#define MQTTCLIENT_MAX_MESSAGES_INFLIGHT -4
/**
 * Return code: An invalid UTF-8 string has been detected.
 */
#define MQTTCLIENT_BAD_UTF8_STRING -5
/**
 * Return code: A NULL parameter has been supplied when this is invalid.
 */
#define MQTTCLIENT_NULL_PARAMETER -6
/**
 * Return code: The topic has been truncated (the topic string includes
 * embedded NULL characters). String functions will not access the full topic.
 * Use the topic length value to access the full topic.
 */
#define MQTTCLIENT_TOPICNAME_TRUNCATED -7
/**
 * Return code: A structure parameter does not have the correct eyecatcher
 * and version number.
 */
#define MQTTCLIENT_BAD_STRUCTURE -8
/**
 * Return code: A QoS value that falls outside of the acceptable range (0,1,2)
 */
#define MQTTCLIENT_BAD_QOS -9
/**
 * Return code: Attempting SSL connection using non-SSL version of library
 */
#define MQTTCLIENT_SSL_NOT_SUPPORTED -10
 /**
  * Return code: unrecognized MQTT version
  */
 #define MQTTCLIENT_BAD_MQTT_VERSION -11
/**
 * Return code: protocol prefix in serverURI should be tcp://, ssl://, ws:// or wss://
 * The TLS enabled prefixes (ssl, wss) are only valid if the TLS version of the library
 * is linked with.
 */
#define MQTTCLIENT_BAD_PROTOCOL -14
 /**
  * Return code: option not applicable to the requested version of MQTT
  */
 #define MQTTCLIENT_BAD_MQTT_OPTION -15
 /**
  * Return code: call not applicable to the requested version of MQTT
  */
 #define MQTTCLIENT_WRONG_MQTT_VERSION -16
 /**
  * Return code: 0 length will topic on connect
  */
 #define MQTTCLIENT_0_LEN_WILL_TOPIC -17


/**
 * Default MQTT version to connect with.  Use 3.1.1 then fall back to 3.1
 */
#define MQTTVERSION_DEFAULT 0
/**
 * MQTT version to connect with: 3.1
 */
#define MQTTVERSION_3_1 3
/**
 * MQTT version to connect with: 3.1.1
 */
#define MQTTVERSION_3_1_1 4
 /**
  * MQTT version to connect with: 5
  */
 #define MQTTVERSION_5 5
/**
 * Bad return code from subscribe, as defined in the 3.1.1 specification
 */
#define MQTT_BAD_SUBSCRIBE 0x80

/**
 *  Initialization options
 */
typedef struct
{
	/** The eyecatcher for this structure.  Must be MQTG. */
	char struct_id[4];
	/** The version number of this structure.  Must be 0 */
	int struct_version;
	/** 1 = we do openssl init, 0 = leave it to the application */
	int do_openssl_init;
} MQTTClient_init_options;

#define MQTTClient_init_options_initializer { {'M', 'Q', 'T', 'G'}, 0, 0 }

/**
 * Global init of mqtt library. Call once on program start to set global behaviour.
 * do_openssl_init - if mqtt library should initialize OpenSSL (1) or rely on the caller to do it before using the library (0)
 */
LIBMQTT_API void MQTTClient_global_init(MQTTClient_init_options* inits);

/**
 * A handle representing an MQTT client. A valid client handle is available
 * following a successful call to MQTTClient_create().
 */
typedef void* MQTTClient;
/**
 * A value representing an MQTT message. A delivery token is returned to the
 * client application when a message is published. The token can then be used to
 * check that the message was successfully delivered to its destination (see
 * MQTTClient_publish(),
 * MQTTClient_publishMessage(),
 * MQTTClient_deliveryComplete(),
 * MQTTClient_waitForCompletion() and
 * MQTTClient_getPendingDeliveryTokens()).
 */
typedef int MQTTClient_deliveryToken;
typedef int MQTTClient_token;

/**
 * A structure representing the payload and attributes of an MQTT message. The
 * message topic is not part of this structure (see MQTTClient_publishMessage(),
 * MQTTClient_publish(), MQTTClient_receive(), MQTTClient_freeMessage()
 * and MQTTClient_messageArrived()).
 */
typedef struct
{
	/** The eyecatcher for this structure.  must be MQTM. */
	char struct_id[4];
	/** The version number of this structure.  Must be 0 or 1
	 *  0 indicates no message properties */
	int struct_version;
	/** The length of the MQTT message payload in bytes. */
	int payloadlen;
	/** A pointer to the payload of the MQTT message. */
	void* payload;
	/**
     * The quality of service (QoS) assigned to the message.
     * There are three levels of QoS:
     * <DL>
     * <DT><B>QoS0</B></DT>
     * <DD>Fire and forget - the message may not be delivered</DD>
     * <DT><B>QoS1</B></DT>
     * <DD>At least once - the message will be delivered, but may be
     * delivered more than once in some circumstances.</DD>
     * <DT><B>QoS2</B></DT>
     * <DD>Once and one only - the message will be delivered exactly once.</DD>
     * </DL>
     */
	int qos;
	/**
     * The retained flag serves two purposes depending on whether the message
     * it is associated with is being published or received.
     *
     * <b>retained = true</b><br>
     * For messages being published, a true setting indicates that the MQTT
     * server should retain a copy of the message. The message will then be
     * transmitted to new subscribers to a topic that matches the message topic.
     * For subscribers registering a new subscription, the flag being true
     * indicates that the received message is not a new one, but one that has
     * been retained by the MQTT server.
     *
     * <b>retained = false</b> <br>
     * For publishers, this indicates that this message should not be retained
     * by the MQTT server. For subscribers, a false setting indicates this is
     * a normal message, received as a result of it being published to the
     * server.
     */
	int retained;
	/**
      * The dup flag indicates whether or not this message is a duplicate.
      * It is only meaningful when receiving QoS1 messages. When true, the
      * client application should take appropriate action to deal with the
      * duplicate message.
      */
	int dup;
	/** The message identifier is normally reserved for internal use by the
      * MQTT client and server.
      */
	int msgid;
	/**
	 * The MQTT V5 properties associated with the message.
	 */
	MQTTProperties properties;
} MQTTClient_message;

#define MQTTClient_message_initializer { {'M', 'Q', 'T', 'M'}, 1, 0, NULL, 0, 0, 0, 0, MQTTProperties_initializer }

/**
 * This is a callback function. The client application
 * must provide an implementation of this function to enable asynchronous
 * receipt of messages. The function is registered with the client library by
 * passing it as an argument to MQTTClient_setCallbacks(). It is
 * called by the client library when a new message that matches a client
 * subscription has been received from the server. This function is executed on
 * a separate thread to the one on which the client application is running.
 * @param context A pointer to the <i>context</i> value originally passed to
 * MQTTClient_setCallbacks(), which contains any application-specific context.
 * @param topicName The topic associated with the received message.
 * @param topicLen The length of the topic if there are one
 * more NULL characters embedded in <i>topicName</i>, otherwise <i>topicLen</i>
 * is 0. If <i>topicLen</i> is 0, the value returned by <i>strlen(topicName)</i>
 * can be trusted. If <i>topicLen</i> is greater than 0, the full topic name
 * can be retrieved by accessing <i>topicName</i> as a byte array of length
 * <i>topicLen</i>.
 * @param message The MQTTClient_message structure for the received message.
 * This structure contains the message payload and attributes.
 * @return This function must return 0 or 1 indicating whether or not
 * the message has been safely received by the client application. <br>
 * Returning 1 indicates that the message has been successfully handled.
 * To free the message storage, ::MQTTClient_freeMessage must be called.
 * To free the topic name storage, ::MQTTClient_free must be called.<br>
 * Returning 0 indicates that there was a problem. In this
 * case, the client library will reinvoke MQTTClient_messageArrived() to
 * attempt to deliver the message to the application again.
 * Do not free the message and topic storage when returning 0, otherwise
 * the redelivery will fail.
 */
typedef int MQTTClient_messageArrived(void* context, char* topicName, int topicLen, MQTTClient_message* message);

/**
 * This is a callback function. The client application
 * must provide an implementation of this function to enable asynchronous
 * notification of delivery of messages. The function is registered with the
 * client library by passing it as an argument to MQTTClient_setCallbacks().
 * It is called by the client library after the client application has
 * published a message to the server. It indicates that the necessary
 * handshaking and acknowledgements for the requested quality of service (see
 * MQTTClient_message.qos) have been completed. This function is executed on a
 * separate thread to the one on which the client application is running.
 * <b>Note:</b>MQTTClient_deliveryComplete() is not called when messages are
 * published at QoS0.
 * @param context A pointer to the <i>context</i> value originally passed to
 * MQTTClient_setCallbacks(), which contains any application-specific context.
 * @param dt The ::MQTTClient_deliveryToken associated with
 * the published message. Applications can check that all messages have been
 * correctly published by matching the delivery tokens returned from calls to
 * MQTTClient_publish() and MQTTClient_publishMessage() with the tokens passed
 * to this callback.
 */
typedef void MQTTClient_deliveryComplete(void* context, MQTTClient_deliveryToken dt);

/**
 * This is a callback function. The client application
 * must provide an implementation of this function to enable asynchronous
 * notification of the loss of connection to the server. The function is
 * registered with the client library by passing it as an argument to
 * MQTTClient_setCallbacks(). It is called by the client library if the client
 * loses its connection to the server. The client application must take
 * appropriate action, such as trying to reconnect or reporting the problem.
 * This function is executed on a separate thread to the one on which the
 * client application is running.
 * @param context A pointer to the <i>context</i> value originally passed to
 * MQTTClient_setCallbacks(), which contains any application-specific context.
 * @param cause The reason for the disconnection.
 * Currently, <i>cause</i> is always set to NULL.
 */
typedef void MQTTClient_connectionLost(void* context, char* cause);

/**
 * This function sets the callback functions for a specific client.
 * If your client application doesn't use a particular callback, set the
 * relevant parameter to NULL. Calling MQTTClient_setCallbacks() puts the
 * client into multi-threaded mode. Any necessary message acknowledgements and
 * status communications are handled in the background without any intervention
 * from the client application. See @ref async for more information.
 *
 * <b>Note:</b> The MQTT client must be disconnected when this function is
 * called.
 * @param handle A valid client handle from a successful call to
 * MQTTClient_create().
 * @param context A pointer to any application-specific context. The
 * the <i>context</i> pointer is passed to each of the callback functions to
 * provide access to the context information in the callback.
 * @param cl A pointer to an MQTTClient_connectionLost() callback
 * function. You can set this to NULL if your application doesn't handle
 * disconnections.
 * @param ma A pointer to an MQTTClient_messageArrived() callback
 * function. This callback function must be specified when you call
 * MQTTClient_setCallbacks().
 * @param dc A pointer to an MQTTClient_deliveryComplete() callback
 * function. You can set this to NULL if your application publishes
 * synchronously or if you do not want to check for successful delivery.
 * @return ::MQTTCLIENT_SUCCESS if the callbacks were correctly set,
 * ::MQTTCLIENT_FAILURE if an error occurred.
 */
LIBMQTT_API int MQTTClient_setCallbacks(MQTTClient handle, void* context, MQTTClient_connectionLost* cl,
									MQTTClient_messageArrived* ma, MQTTClient_deliveryComplete* dc);


/**
 * This is a callback function, which will be called when the a disconnect
 * packet is received from the server.  This applies to MQTT V5 and above only.
 * @param context A pointer to the <i>context</i> value originally passed to
 * ::MQTTAsync_setDisconnected(), which contains any application-specific context.
 * @param properties The MQTT V5 properties received with the disconnect, if any.
 * @param reasonCode The MQTT V5 reason code received with the disconnect.
 * Currently, <i>cause</i> is always set to NULL.
 */
typedef void MQTTClient_disconnected(void* context, MQTTProperties* properties,
		enum MQTTReasonCodes reasonCode);

/**
 * Sets the MQTTClient_disconnected() callback function for a client.  This will be called
 * if a disconnect packet is received from the server.  Only valid for MQTT V5 and above.
 * @param handle A valid client handle from a successful call to
 * MQTTClient_create().
 * @param context A pointer to any application-specific context. The
 * the <i>context</i> pointer is passed to each of the callback functions to
 * provide access to the context information in the callback.
 * @param co A pointer to an MQTTClient_disconnected() callback
 * function.  NULL removes the callback setting.
 * @return ::MQTTCLIENT_SUCCESS if the callbacks were correctly set,
 * ::MQTTCLIENT_FAILURE if an error occurred.
 */
LIBMQTT_API int MQTTClient_setDisconnected(MQTTClient handle, void* context, MQTTClient_disconnected* co);

/**
 * This is a callback function, the MQTT V5 version of MQTTClient_deliveryComplete().
 * The client application
 * must provide an implementation of this function to enable asynchronous
 * notification of the completed delivery of messages.
 * It is called by the client library after the client application has
 * published a message to the server. It indicates that the necessary
 * handshaking and acknowledgements for the requested quality of service (see
 * MQTTClient_message.qos) have been completed. This function is executed on a
 * separate thread to the one on which the client application is running.
 * <b>Note:</b> It is not called when messages are published at QoS0.
 * @param context A pointer to the <i>context</i> value originally passed to
 * MQTTClient_setCallbacks(), which contains any application-specific context.
 * @param dt The ::MQTTClient_deliveryToken associated with
 * the published message. Applications can check that all messages have been
 * correctly published by matching the delivery tokens returned from calls to
 * MQTTClient_publish() and MQTTClient_publishMessage() with the tokens passed
 * to this callback.
 * @param packet_type the last received packet type for this completion. For QoS 1
 * always PUBACK.  For QoS 2 could be PUBREC or PUBCOMP.
 * @param properties the MQTT V5 properties returned with the last packet from the server
 * @param reasonCode the reason code returned from the server
 */
typedef void MQTTClient_published(void* context, int dt, int packet_type, MQTTProperties* properties,
		enum MQTTReasonCodes reasonCode);

LIBMQTT_API int MQTTClient_setPublished(MQTTClient handle, void* context, MQTTClient_published* co);

/**
 * This function creates an MQTT client ready for connection to the
 * specified server and using the specified persistent storage (see
 * MQTTClient_persistence). See also MQTTClient_destroy().
 * @param handle A pointer to an ::MQTTClient handle. The handle is
 * populated with a valid client reference following a successful return from
 * this function.
 * @param serverURI A null-terminated string specifying the server to
 * which the client will connect. It takes the form <i>protocol://host:port</i>.
 * Currently, <i>protocol</i> must be <i>tcp</i> or <i>ssl</i>.
 * For <i>host</i>, you can
 * specify either an IP address or a host name. For instance, to connect to
 * a server running on the local machines with the default MQTT port, specify
 * <i>tcp://localhost:1883</i>.
 * @param clientId The client identifier passed to the server when the
 * client connects to it. It is a null-terminated UTF-8 encoded string.
 * @param persistence_type The type of persistence to be used by the client:
 * <br>
 * ::MQTTCLIENT_PERSISTENCE_NONE: Use in-memory persistence. If the device or
 * system on which the client is running fails or is switched off, the current
 * state of any in-flight messages is lost and some messages may not be
 * delivered even at QoS1 and QoS2.
 * <br>
 * ::MQTTCLIENT_PERSISTENCE_DEFAULT: Use the default (file system-based)
 * persistence mechanism. Status about in-flight messages is held in persistent
 * storage and provides some protection against message loss in the case of
 * unexpected failure.
 * <br>
 * ::MQTTCLIENT_PERSISTENCE_USER: Use an application-specific persistence
 * implementation. Using this type of persistence gives control of the
 * persistence mechanism to the application. The application has to implement
 * the MQTTClient_persistence interface.
 * @param persistence_context If the application uses
 * ::MQTTCLIENT_PERSISTENCE_NONE persistence, this argument is unused and should
 * be set to NULL. For ::MQTTCLIENT_PERSISTENCE_DEFAULT persistence, it
 * should be set to the location of the persistence directory (if set
 * to NULL, the persistence directory used is the working directory).
 * Applications that use ::MQTTCLIENT_PERSISTENCE_USER persistence set this
 * argument to point to a valid MQTTClient_persistence structure.
 * @return ::MQTTCLIENT_SUCCESS if the client is successfully created, otherwise
 * an error code is returned.
 */
LIBMQTT_API int MQTTClient_create(MQTTClient* handle, const char* serverURI, const char* clientId,
		int persistence_type, void* persistence_context);

/** Options for the ::MQTTClient_createWithOptions call */
typedef struct
{
	/** The eyecatcher for this structure.  must be MQCO. */
	char struct_id[4];
	/** The version number of this structure.  Must be 0 */
	int struct_version;
	/** Whether the MQTT version is 3.1, 3.1.1, or 5.  To use V5, this must be set.
	 *  MQTT V5 has to be chosen here, because during the create call the message persistence
	 *  is initialized, and we want to know whether the format of any persisted messages
	 *  is appropriate for the MQTT version we are going to connect with.  Selecting 3.1 or
	 *  3.1.1 and attempting to read 5.0 persisted messages will result in an error on create.  */
	int MQTTVersion;
} MQTTClient_createOptions;

#define MQTTClient_createOptions_initializer { {'M', 'Q', 'C', 'O'}, 0, MQTTVERSION_DEFAULT }

/**
 * A version of :MQTTClient_create() with additional options.
 * This function creates an MQTT client ready for connection to the
 * specified server and using the specified persistent storage (see
 * MQTTClient_persistence). See also MQTTClient_destroy().
 * @param handle A pointer to an ::MQTTClient handle. The handle is
 * populated with a valid client reference following a successful return from
 * this function.
 * @param serverURI A null-terminated string specifying the server to
 * which the client will connect. It takes the form <i>protocol://host:port</i>.
 * Currently, <i>protocol</i> must be <i>tcp</i> or <i>ssl</i>.
 * For <i>host</i>, you can
 * specify either an IP address or a host name. For instance, to connect to
 * a server running on the local machines with the default MQTT port, specify
 * <i>tcp://localhost:1883</i>.
 * @param clientId The client identifier passed to the server when the
 * client connects to it. It is a null-terminated UTF-8 encoded string.
 * @param persistence_type The type of persistence to be used by the client:
 * <br>
 * ::MQTTCLIENT_PERSISTENCE_NONE: Use in-memory persistence. If the device or
 * system on which the client is running fails or is switched off, the current
 * state of any in-flight messages is lost and some messages may not be
 * delivered even at QoS1 and QoS2.
 * <br>
 * ::MQTTCLIENT_PERSISTENCE_DEFAULT: Use the default (file system-based)
 * persistence mechanism. Status about in-flight messages is held in persistent
 * storage and provides some protection against message loss in the case of
 * unexpected failure.
 * <br>
 * ::MQTTCLIENT_PERSISTENCE_USER: Use an application-specific persistence
 * implementation. Using this type of persistence gives control of the
 * persistence mechanism to the application. The application has to implement
 * the MQTTClient_persistence interface.
 * @param persistence_context If the application uses
 * ::MQTTCLIENT_PERSISTENCE_NONE persistence, this argument is unused and should
 * be set to NULL. For ::MQTTCLIENT_PERSISTENCE_DEFAULT persistence, it
 * should be set to the location of the persistence directory (if set
 * to NULL, the persistence directory used is the working directory).
 * Applications that use ::MQTTCLIENT_PERSISTENCE_USER persistence set this
 * argument to point to a valid MQTTClient_persistence structure.
 * @param options additional options for the create.
 * @return ::MQTTCLIENT_SUCCESS if the client is successfully created, otherwise
 * an error code is returned.
 */
LIBMQTT_API int MQTTClient_createWithOptions(MQTTClient* handle, const char* serverURI, const char* clientId,
		int persistence_type, void* persistence_context, MQTTClient_createOptions* options);

/**
 * MQTTClient_willOptions defines the MQTT "Last Will and Testament" (LWT) settings for
 * the client. In the event that a client unexpectedly loses its connection to
 * the server, the server publishes the LWT message to the LWT topic on
 * behalf of the client. This allows other clients (subscribed to the LWT topic)
 * to be made aware that the client has disconnected. To enable the LWT
 * function for a specific client, a valid pointer to an MQTTClient_willOptions
 * structure is passed in the MQTTClient_connectOptions structure used in the
 * MQTTClient_connect() call that connects the client to the server. The pointer
 * to MQTTClient_willOptions can be set to NULL if the LWT function is not
 * required.
 */
typedef struct
{
	/** The eyecatcher for this structure.  must be MQTW. */
	char struct_id[4];
	/** The version number of this structure.  Must be 0 or 1
		   0 means there is no binary payload option
	 */
	int struct_version;
	/** The LWT topic to which the LWT message will be published. */
	const char* topicName;
	/** The LWT payload in string form. */
	const char* message;
	/**
	 * The retained flag for the LWT message (see MQTTClient_message.retained).
	 */
	int retained;
	/**
	 * The quality of service setting for the LWT message (see
	 * MQTTClient_message.qos and @ref qos).
	 */
	int qos;
  /** The LWT payload in binary form. This is only checked and used if the message option is NULL */
	struct
	{
		int len;            /**< binary payload length */
		const void* data;  /**< binary payload data */
	} payload;
} MQTTClient_willOptions;

#define MQTTClient_willOptions_initializer { {'M', 'Q', 'T', 'W'}, 1, NULL, NULL, 0, 0, {0, NULL} }

#define MQTT_SSL_VERSION_DEFAULT 0
#define MQTT_SSL_VERSION_TLS_1_0 1
#define MQTT_SSL_VERSION_TLS_1_1 2
#define MQTT_SSL_VERSION_TLS_1_2 3

/**
* MQTTClient_sslProperties defines the settings to establish an SSL/TLS connection using the
* OpenSSL library. It covers the following scenarios:
* - Server authentication: The client needs the digital certificate of the server. It is included
*   in a store containting trusted material (also known as "trust store").
* - Mutual authentication: Both client and server are authenticated during the SSL handshake. In
*   addition to the digital certificate of the server in a trust store, the client will need its own
*   digital certificate and the private key used to sign its digital certificate stored in a "key store".
* - Anonymous connection: Both client and server do not get authenticated and no credentials are needed
*   to establish an SSL connection. Note that this scenario is not fully secure since it is subject to
*   man-in-the-middle attacks.
*/
typedef struct
{
	/** The eyecatcher for this structure.  Must be MQTS */
	char struct_id[4];
	/** The version number of this structure.  Must be 0, or 1 to enable TLS version selection. */
	int struct_version;

	/** The file in PEM format containing the public digital certificates trusted by the client. */
	const char* trustStore;

	/** The file in PEM format containing the public certificate chain of the client. It may also include
	* the client's private key.
	*/
	const char* keyStore;

	/** If not included in the sslKeyStore, this setting points to the file in PEM format containing
	* the client's private key.
	*/
	const char* privateKey;
	/** The password to load the client's privateKey if encrypted. */
	const char* privateKeyPassword;

	/**
	* The list of cipher suites that the client will present to the server during the SSL handshake. For a
	* full explanation of the cipher list format, please see the OpenSSL on-line documentation:
	* http://www.openssl.org/docs/apps/ciphers.html#CIPHER_LIST_FORMAT
	* If this setting is ommitted, its default value will be "ALL", that is, all the cipher suites -excluding
	* those offering no encryption- will be considered.
	* This setting can be used to set an SSL anonymous connection ("aNULL" string value, for instance).
	*/
	const char* enabledCipherSuites;

    /** True/False option to enable verification of the server certificate **/
    int enableServerCertAuth;

    /** The SSL/TLS version to use. Specify one of MQTT_SSL_VERSION_DEFAULT (0),
    * MQTT_SSL_VERSION_TLS_1_0 (1), MQTT_SSL_VERSION_TLS_1_1 (2) or MQTT_SSL_VERSION_TLS_1_2 (3).
    * Only used if struct_version is >= 1.
    */
    int sslVersion;

    /**
     * Whether to carry out post-connect checks, including that a certificate
     * matches the given host name.
     * Exists only if struct_version >= 2
     */
    int verify;

    /**
     * From the OpenSSL documentation:
     * If CApath is not NULL, it points to a directory containing CA certificates in PEM format.
     * Exists only if struct_version >= 2
	 */
	const char* CApath;

    /**
     * Callback function for OpenSSL error handler ERR_print_errors_cb
     * Exists only if struct_version >= 3
     */
    int (*ssl_error_cb) (const char *str, size_t len, void *u);

    /**
     * Application-specific contex for OpenSSL error handler ERR_print_errors_cb
     * Exists only if struct_version >= 3
     */
    void* ssl_error_context;

	/**
	 * Callback function for setting TLS-PSK options. Parameters correspond to that of
	 * SSL_CTX_set_psk_client_callback, except for u which is the pointer ssl_psk_context.
	 * Exists only if struct_version >= 4
	 */
	unsigned int (*ssl_psk_cb) (const char *hint, char *identity, unsigned int max_identity_len, unsigned char *psk, unsigned int max_psk_len, void *u);

	/**
	 * Application-specific contex for ssl_psk_cb
	 * Exists only if struct_version >= 4
	 */
	void* ssl_psk_context;

	/**
	 * Don't load default SSL CA. Should be used together with PSK to make sure
	 * regular servers with certificate in place is not accepted.
	 * Exists only if struct_version >= 4
	 */
	int disableDefaultTrustStore;

} MQTTClient_SSLOptions;

#define MQTTClient_SSLOptions_initializer { {'M', 'Q', 'T', 'S'}, 4, NULL, NULL, NULL, NULL, NULL, 1, MQTT_SSL_VERSION_DEFAULT, 0, NULL, NULL, NULL, NULL, NULL, 0 }

/**
  * MQTTClient_libraryInfo is used to store details relating to the currently used
  * library such as the version in use, the time it was built and relevant openSSL
  * options.
  * There is one static instance of this struct in MQTTClient.c
  */

typedef struct
{
	const char* name;
	const char* value;
} MQTTClient_nameValue;

/**
  * This function returns version information about the library.
  * no trace information will be returned.
  * @return an array of strings describing the library.  The last entry is a NULL pointer.
  */
LIBMQTT_API MQTTClient_nameValue* MQTTClient_getVersionInfo(void);

/**
 * MQTTClient_connectOptions defines several settings that control the way the
 * client connects to an MQTT server.
 *
 * <b>Note:</b> Default values are not defined for members of
 * MQTTClient_connectOptions so it is good practice to specify all settings.
 * If the MQTTClient_connectOptions structure is defined as an automatic
 * variable, all members are set to random values and thus must be set by the
 * client application. If the MQTTClient_connectOptions structure is defined
 * as a static variable, initialization (in compliant compilers) sets all
 * values to 0 (NULL for pointers). A #keepAliveInterval setting of 0 prevents
 * correct operation of the client and so you <b>must</b> at least set a value
 * for #keepAliveInterval.
 */
typedef struct
{
	/** The eyecatcher for this structure.  must be MQTC. */
	char struct_id[4];
	/** The version number of this structure.  Must be 0, 1, 2, 3, 4, 5 or 6.
	 * 0 signifies no SSL options and no serverURIs
	 * 1 signifies no serverURIs
	 * 2 signifies no MQTTVersion
	 * 3 signifies no returned values
	 * 4 signifies no binary password option
	 * 5 signifies no maxInflightMessages and cleanstart
	 * 6 signifies no HTTP headers option
	 */
	int struct_version;
	/** The "keep alive" interval, measured in seconds, defines the maximum time
   * that should pass without communication between the client and the server
   * The client will ensure that at least one message travels across the
   * network within each keep alive period.  In the absence of a data-related
	 * message during the time period, the client sends a very small MQTT
   * "ping" message, which the server will acknowledge. The keep alive
   * interval enables the client to detect when the server is no longer
	 * available without having to wait for the long TCP/IP timeout.
	 */
	int keepAliveInterval;
	/**
   * This is a boolean value. The cleansession setting controls the behaviour
   * of both the client and the server at connection and disconnection time.
   * The client and server both maintain session state information. This
   * information is used to ensure "at least once" and "exactly once"
   * delivery, and "exactly once" receipt of messages. Session state also
   * includes subscriptions created by an MQTT client. You can choose to
   * maintain or discard state information between sessions.
   *
   * When cleansession is true, the state information is discarded at
   * connect and disconnect. Setting cleansession to false keeps the state
   * information. When you connect an MQTT client application with
   * MQTTClient_connect(), the client identifies the connection using the
   * client identifier and the address of the server. The server checks
   * whether session information for this client
   * has been saved from a previous connection to the server. If a previous
   * session still exists, and cleansession=true, then the previous session
   * information at the client and server is cleared. If cleansession=false,
   * the previous session is resumed. If no previous session exists, a new
   * session is started.
	 */
	int cleansession;
	/**
   * This is a boolean value that controls how many messages can be in-flight
   * simultaneously. Setting <i>reliable</i> to true means that a published
   * message must be completed (acknowledgements received) before another
   * can be sent. Attempts to publish additional messages receive an
   * ::MQTTCLIENT_MAX_MESSAGES_INFLIGHT return code. Setting this flag to
   * false allows up to 10 messages to be in-flight. This can increase
   * overall throughput in some circumstances.
	 */
	int reliable;
	/**
   * This is a pointer to an MQTTClient_willOptions structure. If your
   * application does not make use of the Last Will and Testament feature,
   * set this pointer to NULL.
   */
	MQTTClient_willOptions* will;
	/**
   * MQTT servers that support the MQTT v3.1.1 protocol provide authentication
   * and authorisation by user name and password. This is the user name
   * parameter.
   */
	const char* username;
	/**
   * MQTT servers that support the MQTT v3.1.1 protocol provide authentication
   * and authorisation by user name and password. This is the password
   * parameter.
   */
	const char* password;
	/**
   * The time interval in seconds to allow a connect to complete.
   */
	int connectTimeout;
	/**
	 * The time interval in seconds after which unacknowledged publish requests are
	 * retried during a TCP session.  With MQTT 3.1.1 and later, retries are
	 * not required except on reconnect.  0 turns off in-session retries, and is the
	 * recommended setting.  Adding retries to an already overloaded network only
	 * exacerbates the problem.
	 */
	int retryInterval;
	/**
   * This is a pointer to an MQTTClient_SSLOptions structure. If your
   * application does not make use of SSL, set this pointer to NULL.
   */
	MQTTClient_SSLOptions* ssl;
	/**
	 * The number of entries in the optional serverURIs array. Defaults to 0.
	 */
	int serverURIcount;
	/**
   * An optional array of null-terminated strings specifying the servers to
   * which the client will connect. Each string takes the form <i>protocol://host:port</i>.
   * <i>protocol</i> must be <i>tcp</i> or <i>ssl</i>. For <i>host</i>, you can
   * specify either an IP address or a host name. For instance, to connect to
   * a server running on the local machines with the default MQTT port, specify
   * <i>tcp://localhost:1883</i>.
   * If this list is empty (the default), the server URI specified on MQTTClient_create()
   * is used.
   */
	char* const* serverURIs;
	/**
	 * Sets the version of MQTT to be used on the connect.
	 * MQTTVERSION_DEFAULT (0) = default: start with 3.1.1, and if that fails, fall back to 3.1
	 * MQTTVERSION_3_1 (3) = only try version 3.1
	 * MQTTVERSION_3_1_1 (4) = only try version 3.1.1
	 * MQTTVERSION_5 (5) = only try version 5.0
	 */
	int MQTTVersion;
	/**
	 * Returned from the connect when the MQTT version used to connect is 3.1.1
	 */
	struct
	{
		const char* serverURI;     /**< the serverURI connected to */
		int MQTTVersion;     /**< the MQTT version used to connect with */
		int sessionPresent;  /**< if the MQTT version is 3.1.1, the value of sessionPresent returned in the connack */
	} returned;
	/**
   * Optional binary password.  Only checked and used if the password option is NULL
   */
	struct
	{
		int len;           /**< binary password length */
		const void* data;  /**< binary password data */
	} binarypwd;
	/**
	 * httpHeaders
	 */
	const MQTTClient_nameValue* httpHeaders;
	/**
	 * The maximum number of messages in flight
	 */
	int maxInflightMessages;
	/*
	 * MQTT V5 clean start flag.  Only clears state at the beginning of the session.
	 */
	int cleanstart;
} MQTTClient_connectOptions;

#define MQTTClient_connectOptions_initializer { {'M', 'Q', 'T', 'C'}, 7, 60, 1, 1, NULL, NULL, NULL, 30, 0, NULL, 0, NULL, MQTTVERSION_DEFAULT, {NULL, 0, 0}, {0, NULL}, NULL, -1, 0}

#define MQTTClient_connectOptions_initializer5 { {'M', 'Q', 'T', 'C'}, 7, 60, 0, 1, NULL, NULL, NULL, 30, 0, NULL, 0, NULL, MQTTVERSION_5, {NULL, 0, 0}, {0, NULL}, NULL, -1, 1}

/**
  * This function attempts to connect a previously-created client (see
  * MQTTClient_create()) to an MQTT server using the specified options. If you
  * want to enable asynchronous message and status notifications, you must call
  * MQTTClient_setCallbacks() prior to MQTTClient_connect().
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param options A pointer to a valid MQTTClient_connectOptions
  * structure.
  * @return ::MQTTCLIENT_SUCCESS if the client successfully connects to the
  * server. An error code is returned if the client was unable to connect to
  * the server.
  * Error codes greater than 0 are returned by the MQTT protocol:<br><br>
  * <b>1</b>: Connection refused: Unacceptable protocol version<br>
  * <b>2</b>: Connection refused: Identifier rejected<br>
  * <b>3</b>: Connection refused: Server unavailable<br>
  * <b>4</b>: Connection refused: Bad user name or password<br>
  * <b>5</b>: Connection refused: Not authorized<br>
  * <b>6-255</b>: Reserved for future use<br>
  */
LIBMQTT_API int MQTTClient_connect(MQTTClient handle, MQTTClient_connectOptions* options);

/** MQTT version 5.0 response information */
typedef struct MQTTResponse
{
	int version;                        /* the version number of this structure */
	enum MQTTReasonCodes reasonCode;    /* the MQTT 5.0 reason code returned */
	int reasonCodeCount;	            /* the number of reason codes.  Used for subscribeMany5 and unsubscribeMany5 */
	enum MQTTReasonCodes* reasonCodes;  /* a list of reason codes.  Used for subscribeMany5 and unsubscribeMany5 */
	MQTTProperties* properties;         /* optionally, the MQTT 5.0 properties returned */
} MQTTResponse;

#define MQTTResponse_initializer {1, MQTTREASONCODE_SUCCESS, 0, NULL, NULL}

/**
 * Frees the storage associated with the MQTT response.
 * @param response the response structure to be freed
 */
LIBMQTT_API void MQTTResponse_free(MQTTResponse response);

/**
  * Attempts to connect a previously-created client (see
  * MQTTClient_create()) to an MQTT server using MQTT version 5.0 and the specified options. If you
  * want to enable asynchronous message and status notifications, you must call
  * MQTTClient_setCallbacks() prior to MQTTClient_connect().
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param options A pointer to a valid MQTTClient_connectOptions
  * structure.
  * @param connectProperties the MQTT 5.0 connect properties to use
  * @return the MQTT 5.0 response information: error codes and properties.
  */
LIBMQTT_API MQTTResponse MQTTClient_connect5(MQTTClient handle, MQTTClient_connectOptions* options,
		MQTTProperties* connectProperties, MQTTProperties* willProperties);

/**
  * This function attempts to disconnect the client from the MQTT
  * server. In order to allow the client time to complete handling of messages
  * that are in-flight when this function is called, a timeout period is
  * specified. When the timeout period has expired, the client disconnects even
  * if there are still outstanding message acknowledgements.
  * The next time the client connects to the same server, any QoS 1 or 2
  * messages which have not completed will be retried depending on the
  * cleansession settings for both the previous and the new connection (see
  * MQTTClient_connectOptions.cleansession and MQTTClient_connect()).
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param timeout The client delays disconnection for up to this time (in
  * milliseconds) in order to allow in-flight message transfers to complete.
  * @return ::MQTTCLIENT_SUCCESS if the client successfully disconnects from
  * the server. An error code is returned if the client was unable to disconnect
  * from the server
  */
LIBMQTT_API int MQTTClient_disconnect(MQTTClient handle, int timeout);

LIBMQTT_API int MQTTClient_disconnect5(MQTTClient handle, int timeout, enum MQTTReasonCodes reason, MQTTProperties* props);

/**
  * This function allows the client application to test whether or not a
  * client is currently connected to the MQTT server.
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @return Boolean true if the client is connected, otherwise false.
  */
LIBMQTT_API int MQTTClient_isConnected(MQTTClient handle);


/* Subscribe is synchronous.  QoS list parameter is changed on return to granted QoSs.
   Returns return code, MQTTCLIENT_SUCCESS == success, non-zero some sort of error (TBD) */

/**
  * This function attempts to subscribe a client to a single topic, which may
  * contain wildcards (see @ref wildcard). This call also specifies the
  * @ref qos requested for the subscription
  * (see also MQTTClient_subscribeMany()).
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param topic The subscription topic, which may include wildcards.
  * @param qos The requested quality of service for the subscription.
  * @return ::MQTTCLIENT_SUCCESS if the subscription request is successful.
  * An error code is returned if there was a problem registering the
  * subscription.
  */
LIBMQTT_API int MQTTClient_subscribe(MQTTClient handle, const char* topic, int qos);

/**
  * This function attempts to subscribe an MQTT version 5.0 client to a single topic, which may
  * contain wildcards (see @ref wildcard). This call also specifies the
  * @ref qos requested for the subscription
  * (see also MQTTClient_subscribeMany()).
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param topic The subscription topic, which may include wildcards.
  * @param qos The requested quality of service for the subscription.
  * @param opts the MQTT 5.0 subscribe options to be used
  * @param props the MQTT 5.0 properties to be used
  * @return the MQTT 5.0 response information: error codes and properties.
  */
LIBMQTT_API MQTTResponse MQTTClient_subscribe5(MQTTClient handle, const char* topic, int qos,
		MQTTSubscribe_options* opts, MQTTProperties* props);

/**
  * This function attempts to subscribe a client to a list of topics, which may
  * contain wildcards (see @ref wildcard). This call also specifies the
  * @ref qos requested for each topic (see also MQTTClient_subscribe()).
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param count The number of topics for which the client is requesting
  * subscriptions.
  * @param topic An array (of length <i>count</i>) of pointers to
  * topics, each of which may include wildcards.
  * @param qos An array (of length <i>count</i>) of @ref qos
  * values. qos[n] is the requested QoS for topic[n].
  * @return ::MQTTCLIENT_SUCCESS if the subscription request is successful.
  * An error code is returned if there was a problem registering the
  * subscriptions.
  */
LIBMQTT_API int MQTTClient_subscribeMany(MQTTClient handle, int count, char* const* topic, int* qos);

/**
  * This function attempts to subscribe an MQTT version 5.0 client to a list of topics, which may
  * contain wildcards (see @ref wildcard). This call also specifies the
  * @ref qos requested for each topic (see also MQTTClient_subscribe()).
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param count The number of topics for which the client is requesting
  * subscriptions.
  * @param topic An array (of length <i>count</i>) of pointers to
  * topics, each of which may include wildcards.
  * @param qos An array (of length <i>count</i>) of @ref qos
  * values. qos[n] is the requested QoS for topic[n].
  * @param opts the MQTT 5.0 subscribe options to be used
  * @param props the MQTT 5.0 properties to be used
  * @return the MQTT 5.0 response information: error codes and properties.
  */
LIBMQTT_API MQTTResponse MQTTClient_subscribeMany5(MQTTClient handle, int count, char* const* topic,
		int* qos, MQTTSubscribe_options* opts, MQTTProperties* props);

/**
  * This function attempts to remove an existing subscription made by the
  * specified client.
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param topic The topic for the subscription to be removed, which may
  * include wildcards (see @ref wildcard).
  * @return ::MQTTCLIENT_SUCCESS if the subscription is removed.
  * An error code is returned if there was a problem removing the
  * subscription.
  */
LIBMQTT_API int MQTTClient_unsubscribe(MQTTClient handle, const char* topic);

/**
  * This function attempts to remove an existing subscription made by the
  * specified client using MQTT 5.0.
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param topic The topic for the subscription to be removed, which may
  * include wildcards (see @ref wildcard).
  * @param props the MQTT 5.0 properties to be used
  * @return the MQTT 5.0 response information: error codes and properties.
  */
LIBMQTT_API MQTTResponse MQTTClient_unsubscribe5(MQTTClient handle, const char* topic, MQTTProperties* props);

/**
  * This function attempts to remove existing subscriptions to a list of topics
  * made by the specified client.
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param count The number subscriptions to be removed.
  * @param topic An array (of length <i>count</i>) of pointers to the topics of
  * the subscriptions to be removed, each of which may include wildcards.
  * @return ::MQTTCLIENT_SUCCESS if the subscriptions are removed.
  * An error code is returned if there was a problem removing the subscriptions.
  */
LIBMQTT_API int MQTTClient_unsubscribeMany(MQTTClient handle, int count, char* const* topic);

/**
  * This function attempts to remove existing subscriptions to a list of topics
  * made by the specified client using MQTT version 5.0.
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param count The number subscriptions to be removed.
  * @param topic An array (of length <i>count</i>) of pointers to the topics of
  * the subscriptions to be removed, each of which may include wildcards.
  * @param props the MQTT 5.0 properties to be used
  * @return the MQTT 5.0 response information: error codes and properties.
  */
LIBMQTT_API MQTTResponse MQTTClient_unsubscribeMany5(MQTTClient handle, int count, char* const* topic, MQTTProperties* props);

/**
  * This function attempts to publish a message to a given topic (see also
  * MQTTClient_publishMessage()). An ::MQTTClient_deliveryToken is issued when
  * this function returns successfully. If the client application needs to
  * test for succesful delivery of QoS1 and QoS2 messages, this can be done
  * either asynchronously or synchronously (see @ref async,
  * ::MQTTClient_waitForCompletion and MQTTClient_deliveryComplete()).
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param topicName The topic associated with this message.
  * @param payloadlen The length of the payload in bytes.
  * @param payload A pointer to the byte array payload of the message.
  * @param qos The @ref qos of the message.
  * @param retained The retained flag for the message.
  * @param dt A pointer to an ::MQTTClient_deliveryToken. This is populated
  * with a token representing the message when the function returns
  * successfully. If your application does not use delivery tokens, set this
  * argument to NULL.
  * @return ::MQTTCLIENT_SUCCESS if the message is accepted for publication.
  * An error code is returned if there was a problem accepting the message.
  */
LIBMQTT_API int MQTTClient_publish(MQTTClient handle, const char* topicName, int payloadlen, const void* payload, int qos, int retained,
		MQTTClient_deliveryToken* dt);

/**
  * Attempts to publish a message to a given topic using MQTT version 5.0 (see also
  * MQTTClient_publishMessage5()). An ::MQTTClient_deliveryToken is issued when
  * this function returns successfully. If the client application needs to
  * test for succesful delivery of QoS1 and QoS2 messages, this can be done
  * either asynchronously or synchronously (see @ref async,
  * ::MQTTClient_waitForCompletion and MQTTClient_deliveryComplete()).
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param topicName The topic associated with this message.
  * @param payloadlen The length of the payload in bytes.
  * @param payload A pointer to the byte array payload of the message.
  * @param qos The @ref qos of the message.
  * @param retained The retained flag for the message.
  * @param properties the MQTT 5.0 properties to be used
  * @param dt A pointer to an ::MQTTClient_deliveryToken. This is populated
  * with a token representing the message when the function returns
  * successfully. If your application does not use delivery tokens, set this
  * argument to NULL.
  * @return the MQTT 5.0 response information: error codes and properties.
  */
LIBMQTT_API MQTTResponse MQTTClient_publish5(MQTTClient handle, const char* topicName, int payloadlen, const void* payload,
		int qos, int retained, MQTTProperties* properties, MQTTClient_deliveryToken* dt);
/**
  * This function attempts to publish a message to a given topic (see also
  * MQTTClient_publish()). An ::MQTTClient_deliveryToken is issued when
  * this function returns successfully. If the client application needs to
  * test for succesful delivery of QoS1 and QoS2 messages, this can be done
  * either asynchronously or synchronously (see @ref async,
  * ::MQTTClient_waitForCompletion and MQTTClient_deliveryComplete()).
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param topicName The topic associated with this message.
  * @param msg A pointer to a valid MQTTClient_message structure containing
  * the payload and attributes of the message to be published.
  * @param dt A pointer to an ::MQTTClient_deliveryToken. This is populated
  * with a token representing the message when the function returns
  * successfully. If your application does not use delivery tokens, set this
  * argument to NULL.
  * @return ::MQTTCLIENT_SUCCESS if the message is accepted for publication.
  * An error code is returned if there was a problem accepting the message.
  */
LIBMQTT_API int MQTTClient_publishMessage(MQTTClient handle, const char* topicName, MQTTClient_message* msg, MQTTClient_deliveryToken* dt);


/**
  * Attempts to publish a message to the given topic using MQTT version 5.0
  * (see also
  * MQTTClient_publish5()). An ::MQTTClient_deliveryToken is issued when
  * this function returns successfully. If the client application needs to
  * test for succesful delivery of QoS1 and QoS2 messages, this can be done
  * either asynchronously or synchronously (see @ref async,
  * ::MQTTClient_waitForCompletion and MQTTClient_deliveryComplete()).
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param topicName The topic associated with this message.
  * @param msg A pointer to a valid MQTTClient_message structure containing
  * the payload and attributes of the message to be published.
  * @param dt A pointer to an ::MQTTClient_deliveryToken. This is populated
  * with a token representing the message when the function returns
  * successfully. If your application does not use delivery tokens, set this
  * argument to NULL.
  * @return the MQTT 5.0 response information: error codes and properties.
  */
LIBMQTT_API MQTTResponse MQTTClient_publishMessage5(MQTTClient handle, const char* topicName, MQTTClient_message* msg,
		MQTTClient_deliveryToken* dt);

/**
  * This function is called by the client application to synchronize execution
  * of the main thread with completed publication of a message. When called,
  * MQTTClient_waitForCompletion() blocks execution until the message has been
  * successful delivered or the specified timeout has expired. See @ref async.
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param dt The ::MQTTClient_deliveryToken that represents the message being
  * tested for successful delivery. Delivery tokens are issued by the
  * publishing functions MQTTClient_publish() and MQTTClient_publishMessage().
  * @param timeout The maximum time to wait in milliseconds.
  * @return ::MQTTCLIENT_SUCCESS if the message was successfully delivered.
  * An error code is returned if the timeout expires or there was a problem
  * checking the token.
  */
LIBMQTT_API int MQTTClient_waitForCompletion(MQTTClient handle, MQTTClient_deliveryToken dt, unsigned long timeout);


/**
  * This function sets a pointer to an array of delivery tokens for
  * messages that are currently in-flight (pending completion).
  *
  * <b>Important note:</b> The memory used to hold the array of tokens is
  * malloc()'d in this function. The client application is responsible for
  * freeing this memory when it is no longer required.
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param tokens The address of a pointer to an ::MQTTClient_deliveryToken.
  * When the function returns successfully, the pointer is set to point to an
  * array of tokens representing messages pending completion. The last member of
  * the array is set to -1 to indicate there are no more tokens. If no tokens
  * are pending, the pointer is set to NULL.
  * @return ::MQTTCLIENT_SUCCESS if the function returns successfully.
  * An error code is returned if there was a problem obtaining the list of
  * pending tokens.
  */
LIBMQTT_API int MQTTClient_getPendingDeliveryTokens(MQTTClient handle, MQTTClient_deliveryToken **tokens);

/**
  * When implementing a single-threaded client, call this function periodically
  * to allow processing of message retries and to send MQTT keepalive pings.
  * If the application is calling MQTTClient_receive() regularly, then it is
  * not necessary to call this function.
  */
LIBMQTT_API void MQTTClient_yield(void);

/**
  * This function performs a synchronous receive of incoming messages. It should
  * be used only when the client application has not set callback methods to
  * support asynchronous receipt of messages (see @ref async and
  * MQTTClient_setCallbacks()). Using this function allows a single-threaded
  * client subscriber application to be written. When called, this function
  * blocks until the next message arrives or the specified timeout expires
  *(see also MQTTClient_yield()).
  *
  * <b>Important note:</b> The application must free() the memory allocated
  * to the topic and the message when processing is complete (see
  * MQTTClient_freeMessage()).
  * @param handle A valid client handle from a successful call to
  * MQTTClient_create().
  * @param topicName The address of a pointer to a topic. This function
  * allocates the memory for the topic and returns it to the application
  * by setting <i>topicName</i> to point to the topic.
  * @param topicLen The length of the topic. If the return code from this
  * function is ::MQTTCLIENT_TOPICNAME_TRUNCATED, the topic contains embedded
  * NULL characters and the full topic should be retrieved by using
  * <i>topicLen</i>.
  * @param message The address of a pointer to the received message. This
  * function allocates the memory for the message and returns it to the
  * application by setting <i>message</i> to point to the received message.
  * The pointer is set to NULL if the timeout expires.
  * @param timeout The length of time to wait for a message in milliseconds.
  * @return ::MQTTCLIENT_SUCCESS or ::MQTTCLIENT_TOPICNAME_TRUNCATED if a
  * message is received. ::MQTTCLIENT_SUCCESS can also indicate that the
  * timeout expired, in which case <i>message</i> is NULL. An error code is
  * returned if there was a problem trying to receive a message.
  */
LIBMQTT_API int MQTTClient_receive(MQTTClient handle, char** topicName, int* topicLen, MQTTClient_message** message,
		unsigned long timeout);

/**
  * This function frees memory allocated to an MQTT message, including the
  * additional memory allocated to the message payload. The client application
  * calls this function when the message has been fully processed. <b>Important
  * note:</b> This function does not free the memory allocated to a message
  * topic string. It is the responsibility of the client application to free
  * this memory using the MQTTClient_free() library function.
  * @param msg The address of a pointer to the ::MQTTClient_message structure
  * to be freed.
  */
LIBMQTT_API void MQTTClient_freeMessage(MQTTClient_message** msg);

/**
  * This function frees memory allocated by the MQTT C client library, especially the
  * topic name. This is needed on Windows when the client libary and application
  * program have been compiled with different versions of the C compiler.  It is
  * thus good policy to always use this function when freeing any MQTT C client-
  * allocated memory.
  * @param ptr The pointer to the client library storage to be freed.
  */
LIBMQTT_API void MQTTClient_free(void* ptr);

/**
  * This function frees the memory allocated to an MQTT client (see
  * MQTTClient_create()). It should be called when the client is no longer
  * required.
  * @param handle A pointer to the handle referring to the ::MQTTClient
  * structure to be freed.
  */
LIBMQTT_API void MQTTClient_destroy(MQTTClient* handle);


enum MQTTCLIENT_TRACE_LEVELS
{
	MQTTCLIENT_TRACE_MAXIMUM = 1,
	MQTTCLIENT_TRACE_MEDIUM,
	MQTTCLIENT_TRACE_MINIMUM,
	MQTTCLIENT_TRACE_PROTOCOL,
	MQTTCLIENT_TRACE_ERROR,
	MQTTCLIENT_TRACE_SEVERE,
	MQTTCLIENT_TRACE_FATAL,
};


/**
  * This function sets the level of trace information which will be
  * returned in the trace callback.
  * @param level the trace level required
  */
LIBMQTT_API void MQTTClient_setTraceLevel(enum MQTTCLIENT_TRACE_LEVELS level);


/**
  * This is a callback function prototype which must be implemented if you want
  * to receive trace information.
  * @param level the trace level of the message returned
  * @param message the trace message.  This is a pointer to a static buffer which
  * will be overwritten on each call.  You must copy the data if you want to keep
  * it for later.
  */
typedef void MQTTClient_traceCallback(enum MQTTCLIENT_TRACE_LEVELS level, char* message);

/**
  * This function sets the trace callback if needed.  If set to NULL,
  * no trace information will be returned.  The default trace level is
  * MQTTASYNC_TRACE_MINIMUM.
  * @param callback a pointer to the function which will handle the trace information
  */
LIBMQTT_API void MQTTClient_setTraceCallback(MQTTClient_traceCallback* callback);

/**
 * Returns a pointer to the string representation of the error or NULL.
 *
 * Do not free after use. Returns NULL if the error code is unknown.
 */
LIBMQTT_API const char* MQTTClient_strerror(int code);

#ifdef __cplusplus
     }
#endif

#endif

/**
  * @cond MQTTClient_main
  * @page async Asynchronous vs synchronous client applications
  * The client library supports two modes of operation. These are referred to
  * as <b>synchronous</b> and <b>asynchronous</b> modes. If your application
  * calls MQTTClient_setCallbacks(), this puts the client into asynchronous
  * mode, otherwise it operates in synchronous mode.
  *
  * In synchronous mode, the client application runs on a single thread.
  * Messages are published using the MQTTClient_publish() and
  * MQTTClient_publishMessage() functions. To determine that a QoS1 or QoS2
  * (see @ref qos) message has been successfully delivered, the application
  * must call the MQTTClient_waitForCompletion() function. An example showing
  * synchronous publication is shown in @ref pubsync. Receiving messages in
  * synchronous mode uses the MQTTClient_receive() function. Client applications
  * must call either MQTTClient_receive() or MQTTClient_yield() relatively
  * frequently in order to allow processing of acknowledgements and the MQTT
  * "pings" that keep the network connection to the server alive.
  *
  * In asynchronous mode, the client application runs on several threads. The
  * main program calls functions in the client library to publish and subscribe,
  * just as for the synchronous mode. Processing of handshaking and maintaining
  * the network connection is performed in the background, however.
  * Notifications of status and message reception are provided to the client
  * application using callbacks registered with the library by the call to
  * MQTTClient_setCallbacks() (see MQTTClient_messageArrived(),
  * MQTTClient_connectionLost() and MQTTClient_deliveryComplete()).
  * This API is not thread safe however - it is not possible to call it from multiple
  * threads without synchronization.  You can use the MQTTAsync API for that.
  *
  * @page wildcard Subscription wildcards
  * Every MQTT message includes a topic that classifies it. MQTT servers use
  * topics to determine which subscribers should receive messages published to
  * the server.
  *
  * Consider the server receiving messages from several environmental sensors.
  * Each sensor publishes its measurement data as a message with an associated
  * topic. Subscribing applications need to know which sensor originally
  * published each received message. A unique topic is thus used to identify
  * each sensor and measurement type. Topics such as SENSOR1TEMP,
  * SENSOR1HUMIDITY, SENSOR2TEMP and so on achieve this but are not very
  * flexible. If additional sensors are added to the system at a later date,
  * subscribing applications must be modified to receive them.
  *
  * To provide more flexibility, MQTT supports a hierarchical topic namespace.
  * This allows application designers to organize topics to simplify their
  * management. Levels in the hierarchy are delimited by the '/' character,
  * such as SENSOR/1/HUMIDITY. Publishers and subscribers use these
  * hierarchical topics as already described.
  *
  * For subscriptions, two wildcard characters are supported:
  * <ul>
  * <li>A '#' character represents a complete sub-tree of the hierarchy and
  * thus must be the last character in a subscription topic string, such as
  * SENSOR/#. This will match any topic starting with SENSOR/, such as
  * SENSOR/1/TEMP and SENSOR/2/HUMIDITY.</li>
  * <li> A '+' character represents a single level of the hierarchy and is
  * used between delimiters. For example, SENSOR/+/TEMP will match
  * SENSOR/1/TEMP and SENSOR/2/TEMP.</li>
  * </ul>
  * Publishers are not allowed to use the wildcard characters in their topic
  * names.
  *
  * Deciding on your topic hierarchy is an important step in your system design.
  *
  * @page qos Quality of service
  * The MQTT protocol provides three qualities of service for delivering
  * messages between clients and servers: "at most once", "at least once" and
  * "exactly once".
  *
  * Quality of service (QoS) is an attribute of an individual message being
  * published. An application sets the QoS for a specific message by setting the
  * MQTTClient_message.qos field to the required value.
  *
  * A subscribing client can set the maximum quality of service a server uses
  * to send messages that match the client subscriptions. The
  * MQTTClient_subscribe() and MQTTClient_subscribeMany() functions set this
  * maximum. The QoS of a message forwarded to a subscriber thus might be
  * different to the QoS given to the message by the original publisher.
  * The lower of the two values is used to forward a message.
  *
  * The three levels are:
  *
  * <b>QoS0, At most once:</b> The message is delivered at most once, or it
  * may not be delivered at all. Its delivery across the network is not
  * acknowledged. The message is not stored. The message could be lost if the
  * client is disconnected, or if the server fails. QoS0 is the fastest mode of
  * transfer. It is sometimes called "fire and forget".
  *
  * The MQTT protocol does not require servers to forward publications at QoS0
  * to a client. If the client is disconnected at the time the server receives
  * the publication, the publication might be discarded, depending on the
  * server implementation.
  *
  * <b>QoS1, At least once:</b> The message is always delivered at least once.
  * It might be delivered multiple times if there is a failure before an
  * acknowledgment is received by the sender. The message must be stored
  * locally at the sender, until the sender receives confirmation that the
  * message has been published by the receiver. The message is stored in case
  * the message must be sent again.
  *
  * <b>QoS2, Exactly once:</b> The message is always delivered exactly once.
  * The message must be stored locally at the sender, until the sender receives
  * confirmation that the message has been published by the receiver. The
  * message is stored in case the message must be sent again. QoS2 is the
  * safest, but slowest mode of transfer. A more sophisticated handshaking
  * and acknowledgement sequence is used than for QoS1 to ensure no duplication
  * of messages occurs.
  * @page pubsync Synchronous publication example
@code
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "MQTTClient.h"

#define ADDRESS     "tcp://mqtt.eclipse.org:1883"
#define CLIENTID    "ExampleClientPub"
#define TOPIC       "MQTT Examples"
#define PAYLOAD     "Hello World!"
#define QOS         1
#define TIMEOUT     10000L

int main(int argc, char* argv[])
{
    MQTTClient client;
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    MQTTClient_message pubmsg = MQTTClient_message_initializer;
    MQTTClient_deliveryToken token;
    int rc;

    if ((rc = MQTTClient_create(&client, ADDRESS, CLIENTID,
        MQTTCLIENT_PERSISTENCE_NONE, NULL)) != MQTTCLIENT_SUCCESS)
    {
         printf("Failed to create client, return code %d\n", rc);
         exit(EXIT_FAILURE);
    }

    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession = 1;
    if ((rc = MQTTClient_connect(client, &conn_opts)) != MQTTCLIENT_SUCCESS)
    {
        printf("Failed to connect, return code %d\n", rc);
        exit(EXIT_FAILURE);
    }

    pubmsg.payload = PAYLOAD;
    pubmsg.payloadlen = (int)strlen(PAYLOAD);
    pubmsg.qos = QOS;
    pubmsg.retained = 0;
    if ((rc = MQTTClient_publishMessage(client, TOPIC, &pubmsg, &token)) != MQTTCLIENT_SUCCESS)
    {
         printf("Failed to publish message, return code %d\n", rc);
         exit(EXIT_FAILURE);
    }

    printf("Waiting for up to %d seconds for publication of %s\n"
            "on topic %s for client with ClientID: %s\n",
            (int)(TIMEOUT/1000), PAYLOAD, TOPIC, CLIENTID);
    rc = MQTTClient_waitForCompletion(client, token, TIMEOUT);
    printf("Message with delivery token %d delivered\n", token);

    if ((rc = MQTTClient_disconnect(client, 10000)) != MQTTCLIENT_SUCCESS)
    	printf("Failed to disconnect, return code %d\n", rc);
    MQTTClient_destroy(&client);
    return rc;
}

  * @endcode
  *
  * @page pubasync Asynchronous publication example
@code{.c}
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "MQTTClient.h"

#if !defined(_WIN32)
#include <unistd.h>
#else
#include <windows.h>
#endif

#define ADDRESS     "tcp://mqtt.eclipse.org:1883"
#define CLIENTID    "ExampleClientPub"
#define TOPIC       "MQTT Examples"
#define PAYLOAD     "Hello World!"
#define QOS         1
#define TIMEOUT     10000L

MQTTClient_deliveryToken deliveredtoken;

void delivered(void *context, MQTTClient_deliveryToken dt)
{
    printf("Message with token value %d delivery confirmed\n", dt);
    deliveredtoken = dt;
}

int msgarrvd(void *context, char *topicName, int topicLen, MQTTClient_message *message)
{
    printf("Message arrived\n");
    printf("     topic: %s\n", topicName);
    printf("   message: %.*s\n", message->payloadlen, (char*)message->payload);
    MQTTClient_freeMessage(&message);
    MQTTClient_free(topicName);
    return 1;
}

void connlost(void *context, char *cause)
{
    printf("\nConnection lost\n");
    printf("     cause: %s\n", cause);
}

int main(int argc, char* argv[])
{
    MQTTClient client;
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    MQTTClient_message pubmsg = MQTTClient_message_initializer;
    MQTTClient_deliveryToken token;
    int rc;

    if ((rc = MQTTClient_create(&client, ADDRESS, CLIENTID,
        MQTTCLIENT_PERSISTENCE_NONE, NULL)) != MQTTCLIENT_SUCCESS)
    {
        printf("Failed to create client, return code %d\n", rc);
        rc = EXIT_FAILURE;
        goto exit;
    }

    if ((rc = MQTTClient_setCallbacks(client, NULL, connlost, msgarrvd, delivered)) != MQTTCLIENT_SUCCESS)
    {
        printf("Failed to set callbacks, return code %d\n", rc);
        rc = EXIT_FAILURE;
        goto destroy_exit;
    }

    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession = 1;
    if ((rc = MQTTClient_connect(client, &conn_opts)) != MQTTCLIENT_SUCCESS)
    {
        printf("Failed to connect, return code %d\n", rc);
        rc = EXIT_FAILURE;
        goto destroy_exit;
    }

    pubmsg.payload = PAYLOAD;
    pubmsg.payloadlen = (int)strlen(PAYLOAD);
    pubmsg.qos = QOS;
    pubmsg.retained = 0;
    deliveredtoken = 0;
    if ((rc = MQTTClient_publishMessage(client, TOPIC, &pubmsg, &token)) != MQTTCLIENT_SUCCESS)
    {
    	printf("Failed to publish message, return code %d\n", rc);
    	rc = EXIT_FAILURE;
    }
    else
    {
    	printf("Waiting for publication of %s\n"
            "on topic %s for client with ClientID: %s\n",
            PAYLOAD, TOPIC, CLIENTID);
    	while (deliveredtoken != token)
    	{
			#if defined(_WIN32)
				Sleep(100);
			#else
				usleep(10000L);
			#endif
    	}
    }

    if ((rc = MQTTClient_disconnect(client, 10000)) != MQTTCLIENT_SUCCESS)
    {
    	printf("Failed to disconnect, return code %d\n", rc);
    	rc = EXIT_FAILURE;
    }

destroy_exit:
    MQTTClient_destroy(&client);

exit:
    return rc;
}

  * @endcode
  * @page subasync Asynchronous subscription example
@code
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "MQTTClient.h"

#define ADDRESS     "tcp://mqtt.eclipse.org:1883"
#define CLIENTID    "ExampleClientSub"
#define TOPIC       "MQTT Examples"
#define PAYLOAD     "Hello World!"
#define QOS         1
#define TIMEOUT     10000L

volatile MQTTClient_deliveryToken deliveredtoken;

void delivered(void *context, MQTTClient_deliveryToken dt)
{
    printf("Message with token value %d delivery confirmed\n", dt);
    deliveredtoken = dt;
}

int msgarrvd(void *context, char *topicName, int topicLen, MQTTClient_message *message)
{
    printf("Message arrived\n");
    printf("     topic: %s\n", topicName);
    printf("   message: %.*s\n", message->payloadlen, (char*)message->payload);
    MQTTClient_freeMessage(&message);
    MQTTClient_free(topicName);
    return 1;
}

void connlost(void *context, char *cause)
{
    printf("\nConnection lost\n");
    printf("     cause: %s\n", cause);
}

int main(int argc, char* argv[])
{
    MQTTClient client;
    MQTTClient_connectOptions conn_opts = MQTTClient_connectOptions_initializer;
    int rc;

    if ((rc = MQTTClient_create(&client, ADDRESS, CLIENTID,
        MQTTCLIENT_PERSISTENCE_NONE, NULL)) != MQTTCLIENT_SUCCESS)
    {
        printf("Failed to create client, return code %d\n", rc);
        rc = EXIT_FAILURE;
        goto exit;
    }

    if ((rc = MQTTClient_setCallbacks(client, NULL, connlost, msgarrvd, delivered)) != MQTTCLIENT_SUCCESS)
    {
        printf("Failed to set callbacks, return code %d\n", rc);
        rc = EXIT_FAILURE;
        goto destroy_exit;
    }

    conn_opts.keepAliveInterval = 20;
    conn_opts.cleansession = 1;
    if ((rc = MQTTClient_connect(client, &conn_opts)) != MQTTCLIENT_SUCCESS)
    {
        printf("Failed to connect, return code %d\n", rc);
        rc = EXIT_FAILURE;
        goto destroy_exit;
    }

    printf("Subscribing to topic %s\nfor client %s using QoS%d\n\n"
           "Press Q<Enter> to quit\n\n", TOPIC, CLIENTID, QOS);
    if ((rc = MQTTClient_subscribe(client, TOPIC, QOS)) != MQTTCLIENT_SUCCESS)
    {
    	printf("Failed to subscribe, return code %d\n", rc);
    	rc = EXIT_FAILURE;
    }
    else
    {
    	int ch;
    	do
    	{
        	ch = getchar();
    	} while (ch!='Q' && ch != 'q');

        if ((rc = MQTTClient_unsubscribe(client, TOPIC)) != MQTTCLIENT_SUCCESS)
        {
        	printf("Failed to unsubscribe, return code %d\n", rc);
        	rc = EXIT_FAILURE;
        }
    }

    if ((rc = MQTTClient_disconnect(client, 10000)) != MQTTCLIENT_SUCCESS)
    {
    	printf("Failed to disconnect, return code %d\n", rc);
    	rc = EXIT_FAILURE;
    }
destroy_exit:
    MQTTClient_destroy(&client);
exit:
    return rc;
}

  * @endcode
  * @page tracing Tracing
  *
  * Runtime tracing is controlled by environment variables.
  *
  * Tracing is switched on by setting MQTT_C_CLIENT_TRACE.  A value of ON, or stdout, prints to
  * stdout, any other value is interpreted as a file name to use.
  *
  * The amount of trace detail is controlled with the MQTT_C_CLIENT_TRACE_LEVEL environment
  * variable - valid values are ERROR, PROTOCOL, MINIMUM, MEDIUM and MAXIMUM
  * (from least to most verbose).
  *
  * The variable MQTT_C_CLIENT_TRACE_MAX_LINES limits the number of lines of trace that are output
  * to a file.  Two files are used at most, when they are full, the last one is overwritten with the
  * new trace entries.  The default size is 1000 lines.
  *
  * ### MQTT Packet Tracing
  *
  * A feature that can be very useful is printing the MQTT packets that are sent and received.  To
  * achieve this, use the following environment variable settings:
  * @code
    MQTT_C_CLIENT_TRACE=ON
    MQTT_C_CLIENT_TRACE_LEVEL=PROTOCOL
  * @endcode
  * The output you should see looks like this:
  * @code
    20130528 155936.813 3 stdout-subscriber -> CONNECT cleansession: 1 (0)
    20130528 155936.813 3 stdout-subscriber <- CONNACK rc: 0
    20130528 155936.813 3 stdout-subscriber -> SUBSCRIBE msgid: 1 (0)
    20130528 155936.813 3 stdout-subscriber <- SUBACK msgid: 1
    20130528 155941.818 3 stdout-subscriber -> DISCONNECT (0)
  * @endcode
  * where the fields are:
  * 1. date
  * 2. time
  * 3. socket number
  * 4. client id
  * 5. direction (-> from client to server, <- from server to client)
  * 6. packet details
  *
  * ### Default Level Tracing
  *
  * This is an extract of a default level trace of a call to connect:
  * @code
    19700101 010000.000 (1152206656) (0)> MQTTClient_connect:893
    19700101 010000.000 (1152206656)  (1)> MQTTClient_connectURI:716
    20130528 160447.479 Connecting to serverURI localhost:1883
    20130528 160447.479 (1152206656)   (2)> MQTTProtocol_connect:98
    20130528 160447.479 (1152206656)    (3)> MQTTProtocol_addressPort:48
    20130528 160447.479 (1152206656)    (3)< MQTTProtocol_addressPort:73
    20130528 160447.479 (1152206656)    (3)> Socket_new:599
    20130528 160447.479 New socket 4 for localhost, port 1883
    20130528 160447.479 (1152206656)     (4)> Socket_addSocket:163
    20130528 160447.479 (1152206656)      (5)> Socket_setnonblocking:73
    20130528 160447.479 (1152206656)      (5)< Socket_setnonblocking:78 (0)
    20130528 160447.479 (1152206656)     (4)< Socket_addSocket:176 (0)
    20130528 160447.479 (1152206656)     (4)> Socket_error:95
    20130528 160447.479 (1152206656)     (4)< Socket_error:104 (115)
    20130528 160447.479 Connect pending
    20130528 160447.479 (1152206656)    (3)< Socket_new:683 (115)
    20130528 160447.479 (1152206656)   (2)< MQTTProtocol_connect:131 (115)
  * @endcode
  * where the fields are:
  * 1. date
  * 2. time
  * 3. thread id
  * 4. function nesting level
  * 5. function entry (>) or exit (<)
  * 6. function name : line of source code file
  * 7. return value (if there is one)
  *
  * ### Memory Allocation Tracing
  *
  * Setting the trace level to maximum causes memory allocations and frees to be traced along with
  * the default trace entries, with messages like the following:
  * @code
    20130528 161819.657 Allocating 16 bytes in heap at file /home/icraggs/workspaces/mqrtc/mqttv3c/src/MQTTPacket.c line 177 ptr 0x179f930

    20130528 161819.657 Freeing 16 bytes in heap at file /home/icraggs/workspaces/mqrtc/mqttv3c/src/MQTTPacket.c line 201, heap use now 896 bytes
  * @endcode
  * When the last MQTT client object is destroyed, if the trace is being recorded
  * and all memory allocated by the client library has not been freed, an error message will be
  * written to the trace.  This can help with fixing memory leaks.  The message will look like this:
  * @code
    20130528 163909.208 Some memory not freed at shutdown, possible memory leak
    20130528 163909.208 Heap scan start, total 880 bytes
    20130528 163909.208 Heap element size 32, line 354, file /home/icraggs/workspaces/mqrtc/mqttv3c/src/MQTTPacket.c, ptr 0x260cb00
    20130528 163909.208   Content
    20130528 163909.209 Heap scan end
  * @endcode
  * @endcond
  */
