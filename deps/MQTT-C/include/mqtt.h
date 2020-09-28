#ifndef __MQTT_H__
#define __MQTT_H__

/*
MIT License

Copyright(c) 2018 Liam Bindle

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files(the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions :

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#include <mqtt_pal.h>

/**
 * @file
 * @brief Declares all the MQTT-C functions and datastructures.
 * 
 * @note You should <code>\#include <mqtt.h></code>.
 * 
 * @example simple_publisher.c
 * A simple program to that publishes the current time whenever ENTER is pressed. 
 * 
 * Usage:
 * \code{.sh}
 * ./bin/simple_publisher [address [port [topic]]]
 * \endcode     
 * 
 * Where \c address is the address of the MQTT broker, \c port is the port number the 
 * MQTT broker is running on, and \c topic is the name of the topic to publish with. Note
 * that all these arguments are optional and the defaults are \c address = \c "test.mosquitto.org",
 * \c port = \c "1883", and \c topic = "datetime".
 * 
 * @example simple_subscriber.c
 * A simple program that subscribes to a single topic and prints all updates that are received.
 * 
 * Usage:
 * \code{.sh}
 * ./bin/simple_subscriber [address [port [topic]]]
 * \endcode   
 * 
 * Where \c address is the address of the MQTT broker, \c port is the port number the 
 * MQTT broker is running on, and \c topic is the name of the topic subscribe to. Note
 * that all these arguments are optional and the defaults are \c address = \c "test.mosquitto.org",
 * \c port = \c "1883", and \c topic = "datetime".  
 * 
 * @example reconnect_subscriber.c
 * Same program as \ref simple_subscriber.c, but using the automatic reconnect functionality. 
 * 
 * @example bio_publisher.c
 * Same program as \ref simple_publisher.c, but uses a unencrypted BIO socket.
 *
 * @example openssl_publisher.c
 * Same program as \ref simple_publisher.c, but over an encrypted connection using OpenSSL.
 * 
 * Usage:
 * \code{.sh}
 * ./bin/openssl_publisher ca_file [address [port [topic]]]
 * \endcode   
 * 
 * 
 * @defgroup api API
 * @brief Documentation of everything you need to know to use the MQTT-C client.
 * 
 * This module contains everything you need to know to use MQTT-C in your application.
 * For usage examples see:
 *     - @ref simple_publisher.c
 *     - @ref simple_subscriber.c
 *     - @ref reconnect_subscriber.c
 *     - @ref bio_publisher.c
 *     - @ref openssl_publisher.c
 * 
 * @note MQTT-C can be used in both single-threaded and multi-threaded applications. All 
 *       the functions in \ref api are thread-safe.
 * 
 * @defgroup packers Control Packet Serialization
 * @brief Developer documentation of the functions and datastructures used for serializing MQTT 
 *        control packets.
 * 
 * @defgroup unpackers Control Packet Deserialization
 * @brief Developer documentation of the functions and datastructures used for deserializing MQTT 
 *        control packets.
 * 
 * @defgroup details Utilities
 * @brief Developer documentation for the utilities used to implement the MQTT-C client.
 *
 * @note To deserialize a packet from a buffer use \ref mqtt_unpack_response (it's the only 
 *       function you need).
 */


 /**
  * @brief An enumeration of the MQTT control packet types. 
  * @ingroup unpackers
  *
  * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718021">
  * MQTT v3.1.1: MQTT Control Packet Types
  * </a>
  */
  enum MQTTControlPacketType {
    MQTT_CONTROL_CONNECT=1u,
    MQTT_CONTROL_CONNACK=2u,
    MQTT_CONTROL_PUBLISH=3u,
    MQTT_CONTROL_PUBACK=4u,
    MQTT_CONTROL_PUBREC=5u,
    MQTT_CONTROL_PUBREL=6u,
    MQTT_CONTROL_PUBCOMP=7u,
    MQTT_CONTROL_SUBSCRIBE=8u,
    MQTT_CONTROL_SUBACK=9u,
    MQTT_CONTROL_UNSUBSCRIBE=10u,
    MQTT_CONTROL_UNSUBACK=11u,
    MQTT_CONTROL_PINGREQ=12u,
    MQTT_CONTROL_PINGRESP=13u,
    MQTT_CONTROL_DISCONNECT=14u
};

/**
 * @brief The fixed header of an MQTT control packet.
 * @ingroup unpackers
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718020">
 * MQTT v3.1.1: Fixed Header
 * </a>
 */
struct mqtt_fixed_header {
    /** The type of packet. */
    enum MQTTControlPacketType control_type;

    /** The packets control flags.*/
    uint32_t  control_flags: 4;

    /** The remaining size of the packet in bytes (i.e. the size of variable header and payload).*/
    uint32_t remaining_length;
};

/**
 * @brief The protocol identifier for MQTT v3.1.1.
 * @ingroup packers
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718030">
 * MQTT v3.1.1: CONNECT Variable Header.
 * </a>  
 */
#define MQTT_PROTOCOL_LEVEL 0x04

/** 
 * @brief A macro used to declare the enum MQTTErrors and associated 
 *        error messages (the members of the num) at the same time.
 */
#define __ALL_MQTT_ERRORS(MQTT_ERROR)                    \
    MQTT_ERROR(MQTT_ERROR_NULLPTR)                       \
    MQTT_ERROR(MQTT_ERROR_CONTROL_FORBIDDEN_TYPE)        \
    MQTT_ERROR(MQTT_ERROR_CONTROL_INVALID_FLAGS)         \
    MQTT_ERROR(MQTT_ERROR_CONTROL_WRONG_TYPE)            \
    MQTT_ERROR(MQTT_ERROR_CONNECT_CLIENT_ID_REFUSED)     \
    MQTT_ERROR(MQTT_ERROR_CONNECT_NULL_WILL_MESSAGE)     \
    MQTT_ERROR(MQTT_ERROR_CONNECT_FORBIDDEN_WILL_QOS)    \
    MQTT_ERROR(MQTT_ERROR_CONNACK_FORBIDDEN_FLAGS)       \
    MQTT_ERROR(MQTT_ERROR_CONNACK_FORBIDDEN_CODE)        \
    MQTT_ERROR(MQTT_ERROR_PUBLISH_FORBIDDEN_QOS)         \
    MQTT_ERROR(MQTT_ERROR_SUBSCRIBE_TOO_MANY_TOPICS)     \
    MQTT_ERROR(MQTT_ERROR_MALFORMED_RESPONSE)            \
    MQTT_ERROR(MQTT_ERROR_UNSUBSCRIBE_TOO_MANY_TOPICS)   \
    MQTT_ERROR(MQTT_ERROR_RESPONSE_INVALID_CONTROL_TYPE) \
    MQTT_ERROR(MQTT_ERROR_CONNECT_NOT_CALLED)          \
    MQTT_ERROR(MQTT_ERROR_SEND_BUFFER_IS_FULL)           \
    MQTT_ERROR(MQTT_ERROR_SOCKET_ERROR)                  \
    MQTT_ERROR(MQTT_ERROR_MALFORMED_REQUEST)             \
    MQTT_ERROR(MQTT_ERROR_RECV_BUFFER_TOO_SMALL)         \
    MQTT_ERROR(MQTT_ERROR_ACK_OF_UNKNOWN)                \
    MQTT_ERROR(MQTT_ERROR_NOT_IMPLEMENTED)               \
    MQTT_ERROR(MQTT_ERROR_CONNECTION_REFUSED)            \
    MQTT_ERROR(MQTT_ERROR_SUBSCRIBE_FAILED)              \
    MQTT_ERROR(MQTT_ERROR_CONNECTION_CLOSED)             \
    MQTT_ERROR(MQTT_ERROR_INITIAL_RECONNECT)             \
    MQTT_ERROR(MQTT_ERROR_INVALID_REMAINING_LENGTH)      \
    MQTT_ERROR(MQTT_ERROR_CLEAN_SESSION_IS_REQUIRED)

/* todo: add more connection refused errors */

/** 
 * @brief A macro used to generate the enum MQTTErrors from 
 *        \ref __ALL_MQTT_ERRORS
 * @see __ALL_MQTT_ERRORS
*/
#define GENERATE_ENUM(ENUM) ENUM,

/** 
 * @brief A macro used to generate the error messages associated with 
 *        MQTTErrors from \ref __ALL_MQTT_ERRORS
 * @see __ALL_MQTT_ERRORS
*/
#define GENERATE_STRING(STRING) #STRING,


/** 
 * @brief An enumeration of error codes. Error messages can be retrieved by calling \ref mqtt_error_str.
 * @ingroup api
 * 
 * @see mqtt_error_str
 */
enum MQTTErrors {
    MQTT_ERROR_UNKNOWN=INT_MIN,
    __ALL_MQTT_ERRORS(GENERATE_ENUM)
    MQTT_OK = 1
};

/** 
 * @brief Returns an error message for error code, \p error.
 * @ingroup api
 * 
 * @param[in] error the error code.
 * 
 * @returns The associated error message.
 */
const char* mqtt_error_str(enum MQTTErrors error);

/**
 * @brief Pack a MQTT 16 bit integer, given a native 16 bit integer .
 * 
 * @param[out] buf the buffer that the MQTT integer will be written to.
 * @param[in] integer the native integer to be written to \p buf.
 * 
 * @warning This function provides no error checking.
 * 
 * @returns 2
*/
ssize_t __mqtt_pack_uint16(uint8_t *buf, uint16_t integer);

/**
 * @brief Unpack a MQTT 16 bit integer to a native 16 bit integer.
 * 
 * @param[in] buf the buffer that the MQTT integer will be read from.
 * 
 * @warning This function provides no error checking and does not modify \p buf.
 * 
 * @returns The native integer
*/
uint16_t __mqtt_unpack_uint16(const uint8_t *buf);

/**
 * @brief Pack a MQTT string, given a c-string \p str.
 * 
 * @param[out] buf the buffer that the MQTT string will be written to.
 * @param[in] str the c-string to be written to \p buf.
 * 
 * @warning This function provides no error checking.
 * 
 * @returns strlen(str) + 2
*/
ssize_t __mqtt_pack_str(uint8_t *buf, const char* str);

/** @brief A macro to get the MQTT string length from a c-string. */
#define __mqtt_packed_cstrlen(x) (2 + strlen(x))

/* RESPONSES */

/**
 * @brief An enumeration of the return codes returned in a CONNACK packet.
 * @ingroup unpackers
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Table_3.1_-">
 * MQTT v3.1.1: CONNACK return codes.
 * </a> 
 */
enum MQTTConnackReturnCode {
    MQTT_CONNACK_ACCEPTED = 0u,
    MQTT_CONNACK_REFUSED_PROTOCOL_VERSION = 1u,
    MQTT_CONNACK_REFUSED_IDENTIFIER_REJECTED = 2u,
    MQTT_CONNACK_REFUSED_SERVER_UNAVAILABLE = 3u,
    MQTT_CONNACK_REFUSED_BAD_USER_NAME_OR_PASSWORD = 4u,
    MQTT_CONNACK_REFUSED_NOT_AUTHORIZED = 5u
};

/**
 * @brief A connection response datastructure.
 * @ingroup unpackers
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718033">
 * MQTT v3.1.1: CONNACK - Acknowledgement connection response.
 * </a>
 */
struct mqtt_response_connack {
    /** 
     * @brief Allows client and broker to check if they have a consistent view about whether there is
     * already a stored session state.
    */
    uint8_t session_present_flag;

    /** 
     * @brief The return code of the connection request. 
     * 
     * @see MQTTConnackReturnCode
     */
    enum MQTTConnackReturnCode return_code;
};

 /**
  * @brief A publish packet received from the broker.
  * @ingroup unpackers
  * 
  * A publish packet is received from the broker when a client publishes to a topic that the 
  * \em {local client} is subscribed to.
  *
  * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718037"> 
  * MQTT v3.1.1: PUBLISH - Publish Message.
  * </a> 
  */
struct mqtt_response_publish {
    /** 
     * @brief The DUP flag. DUP flag is 0 if its the first attempt to send this publish packet. A DUP flag
     * of 1 means that this might be a re-delivery of the packet.
     */
    uint8_t dup_flag;

    /** 
     * @brief The quality of service level.
     * 
     * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Table_3.11_-">
     * MQTT v3.1.1: QoS Definitions
     * </a>
     */
    uint8_t qos_level;

    /** @brief The retain flag of this publish message. */
    uint8_t retain_flag;

    /** @brief Size of the topic name (number of characters). */
    uint16_t topic_name_size;

    /** 
     * @brief The topic name. 
     * @note topic_name is not null terminated. Therefore topic_name_size must be used to get the 
     *       string length.
     */
    const void* topic_name;

    /** @brief The publish message's packet ID. */
    uint16_t packet_id;

    /** @brief The publish message's application message.*/
    const void* application_message;

    /** @brief The size of the application message in bytes. */
    size_t application_message_size;
};

/**
 * @brief A publish acknowledgement for messages that were published with QoS level 1.
 * @ingroup unpackers
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718043">
 * MQTT v3.1.1: PUBACK - Publish Acknowledgement.
 * </a> 
 *
 */
struct mqtt_response_puback {
    /** @brief The published messages packet ID. */
    uint16_t packet_id;
};

/**
 * @brief The response packet to a PUBLISH packet with QoS level 2.
 * @ingroup unpackers
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718048">
 * MQTT v3.1.1: PUBREC - Publish Received.
 * </a> 
 *
 */
struct mqtt_response_pubrec {
    /** @brief The published messages packet ID. */
    uint16_t packet_id;
};

/**
 * @brief The response to a PUBREC packet.
 * @ingroup unpackers
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718053">
 * MQTT v3.1.1: PUBREL - Publish Release.
 * </a> 
 *
 */
struct mqtt_response_pubrel {
    /** @brief The published messages packet ID. */
    uint16_t packet_id;
};

/**
 * @brief The response to a PUBREL packet.
 * @ingroup unpackers
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718058">
 * MQTT v3.1.1: PUBCOMP - Publish Complete.
 * </a> 
 *
 */
struct mqtt_response_pubcomp {
    /** T@brief he published messages packet ID. */
    uint16_t packet_id;
};

/**
 * @brief An enumeration of subscription acknowledgement return codes.
 * @ingroup unpackers
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Figure_3.26_-">
 * MQTT v3.1.1: SUBACK Return Codes.
 * </a> 
 */
enum MQTTSubackReturnCodes {
    MQTT_SUBACK_SUCCESS_MAX_QOS_0 = 0u,
    MQTT_SUBACK_SUCCESS_MAX_QOS_1 = 1u,
    MQTT_SUBACK_SUCCESS_MAX_QOS_2 = 2u,
    MQTT_SUBACK_FAILURE           = 128u
};

/**
 * @brief The response to a subscription request.
 * @ingroup unpackers
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718068">
 * MQTT v3.1.1: SUBACK - Subscription Acknowledgement.
 * </a> 
 */
struct mqtt_response_suback {
    /** @brief The published messages packet ID. */
    uint16_t packet_id;

    /** 
     * Array of return codes corresponding to the requested subscribe topics.
     * 
     * @see MQTTSubackReturnCodes
     */
    const uint8_t *return_codes;

    /** The number of return codes. */
    size_t num_return_codes;
};

/**
 * @brief The brokers response to a UNSUBSCRIBE request.
 * @ingroup unpackers
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718077">
 * MQTT v3.1.1: UNSUBACK - Unsubscribe Acknowledgement.
 * </a> 
 */
struct mqtt_response_unsuback {
    /** @brief The published messages packet ID. */
    uint16_t packet_id;
};

/**
 * @brief The response to a ping request.
 * @ingroup unpackers
 * 
 * @note This response contains no members.
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718086">
 * MQTT v3.1.1: PINGRESP - Ping Response.
 * </a> 
 */
struct mqtt_response_pingresp {
  int dummy;
};

/**
 * @brief A struct used to deserialize/interpret an incoming packet from the broker.
 * @ingroup unpackers
 */
struct mqtt_response {
    /** @brief The mqtt_fixed_header of the deserialized packet. */
    struct mqtt_fixed_header fixed_header;

    /**
     * @brief A union of the possible responses from the broker.
     * 
     * @note The fixed_header contains the control type. This control type corresponds to the
     *       member of this union that should be accessed. For example if 
     *       fixed_header#control_type == \c MQTT_CONTROL_PUBLISH then 
     *       decoded#publish should be accessed.
     */
    union {
        struct mqtt_response_connack  connack;
        struct mqtt_response_publish  publish;
        struct mqtt_response_puback   puback;
        struct mqtt_response_pubrec   pubrec;
        struct mqtt_response_pubrel   pubrel;
        struct mqtt_response_pubcomp  pubcomp;
        struct mqtt_response_suback   suback;
        struct mqtt_response_unsuback unsuback;
        struct mqtt_response_pingresp pingresp;
    } decoded;
};

/**
 * @brief Deserialize the contents of \p buf into an mqtt_fixed_header object.
 * @ingroup unpackers
 * 
 * @note This function performs complete error checking and a positive return value
 *       means the entire mqtt_response can be deserialized from \p buf.
 * 
 * @param[out] response the response who's \ref mqtt_response.fixed_header will be initialized.
 * @param[in] buf the buffer.
 * @param[in] bufsz the total number of bytes in the buffer.
 * 
 * @returns The number of bytes that were consumed, or 0 if the buffer does not contain enough 
 *          bytes to parse the packet, or a negative value if there was a protocol violation.
 */
ssize_t mqtt_unpack_fixed_header(struct mqtt_response *response, const uint8_t *buf, size_t bufsz);

/**
 * @brief Deserialize a CONNACK response from \p buf.
 * @ingroup unpackers
 * 
 * @pre \ref mqtt_unpack_fixed_header must have returned a positive value and the control packet type
 *      must be \c MQTT_CONTROL_CONNACK.
 * 
 * @param[out] mqtt_response the mqtt_response that will be initialized.
 * @param[in] buf the buffer that contains the variable header and payload of the packet. The 
 *                first byte of \p buf should be the first byte of the variable header.
 * 
 * @relates mqtt_response_connack 
 * 
 * @returns The number of bytes that were consumed, or 0 if the buffer does not contain enough 
 *          bytes to parse the packet, or a negative value if there was a protocol violation.
 */
ssize_t mqtt_unpack_connack_response (struct mqtt_response *mqtt_response, const uint8_t *buf);

/**
 * @brief Deserialize a publish response from \p buf.
 * @ingroup unpackers
 * 
 * @pre \ref mqtt_unpack_fixed_header must have returned a positive value and the mqtt_response must
 *      have a control type of \c MQTT_CONTROL_PUBLISH.
 * 
 * @param[out] mqtt_response the response that is initialized from the contents of \p buf.
 * @param[in] buf the buffer with the incoming data.
 * 
 * @relates mqtt_response_publish 
 * 
 * @returns The number of bytes that were consumed, or 0 if the buffer does not contain enough 
 *          bytes to parse the packet, or a negative value if there was a protocol violation.
 */
ssize_t mqtt_unpack_publish_response (struct mqtt_response *mqtt_response, const uint8_t *buf);

/**
 * @brief Deserialize a PUBACK/PUBREC/PUBREL/PUBCOMP packet from \p buf.
 * @ingroup unpackers
 * 
 * @pre \ref mqtt_unpack_fixed_header must have returned a positive value and the mqtt_response must
 *      have a control type of \c MQTT_CONTROL_PUBACK, \c MQTT_CONTROL_PUBREC, \c MQTT_CONTROL_PUBREL
 *      or \c MQTT_CONTROL_PUBCOMP.
 * 
 * @param[out] mqtt_response the response that is initialized from the contents of \p buf.
 * @param[in] buf the buffer with the incoming data.
 *
 * @relates mqtt_response_puback mqtt_response_pubrec mqtt_response_pubrel mqtt_response_pubcomp
 * 
 * @returns The number of bytes that were consumed, or 0 if the buffer does not contain enough 
 *          bytes to parse the packet, or a negative value if there was a protocol violation.
 */
ssize_t mqtt_unpack_pubxxx_response(struct mqtt_response *mqtt_response, const uint8_t *buf);

/**
 * @brief Deserialize a SUBACK packet from \p buf.
 * @ingroup unpacker
 *  
 * @pre \ref mqtt_unpack_fixed_header must have returned a positive value and the mqtt_response must
 *      have a control type of \c MQTT_CONTROL_SUBACK.
 * 
 * @param[out] mqtt_response the response that is initialized from the contents of \p buf.
 * @param[in] buf the buffer with the incoming data.
 *
 * @relates mqtt_response_suback
 * 
 * @returns The number of bytes that were consumed, or 0 if the buffer does not contain enough 
 *          bytes to parse the packet, or a negative value if there was a protocol violation.
 */
ssize_t mqtt_unpack_suback_response(struct mqtt_response *mqtt_response, const uint8_t *buf);

/**
 * @brief Deserialize an UNSUBACK packet from \p buf.
 * @ingroup unpacker
 *  
 * @pre \ref mqtt_unpack_fixed_header must have returned a positive value and the mqtt_response must
 *      have a control type of \c MQTT_CONTROL_UNSUBACK.
 * 
 * @param[out] mqtt_response the response that is initialized from the contents of \p buf.
 * @param[in] buf the buffer with the incoming data.
 *
 * @relates mqtt_response_unsuback
 * 
 * @returns The number of bytes that were consumed, or 0 if the buffer does not contain enough 
 *          bytes to parse the packet, or a negative value if there was a protocol violation.
 */  
ssize_t mqtt_unpack_unsuback_response(struct mqtt_response *mqtt_response, const uint8_t *buf);

/**
 * @brief Deserialize a packet from the broker.
 * @ingroup unpackers
 * 
 * @param[out] response the mqtt_response that will be initialize from \p buf.
 * @param[in] buf the incoming data buffer.
 * @param[in] bufsz the number of bytes available in the buffer.
 * 
 * @relates mqtt_response
 * 
 * @returns The number of bytes consumed on success, zero \p buf does not contain enough bytes
 *          to deserialize the packet, a negative value if a protocol violation was encountered.  
 */
ssize_t mqtt_unpack_response(struct mqtt_response* response, const uint8_t *buf, size_t bufsz);

/* REQUESTS */

 /**
 * @brief Serialize an mqtt_fixed_header and write it to \p buf.
 * @ingroup packers
 * 
 * @note This function performs complete error checking and a positive return value
 *       guarantees the entire packet will fit into the given buffer.
 * 
 * @param[out] buf the buffer to write to.
 * @param[in] bufsz the maximum number of bytes that can be put in to \p buf.
 * @param[in] fixed_header the fixed header that will be serialized.
 * 
 * @returns The number of bytes written to \p buf, or 0 if \p buf is too small, or a 
 *          negative value if there was a protocol violation.
 */
ssize_t mqtt_pack_fixed_header(uint8_t *buf, size_t bufsz, const struct mqtt_fixed_header *fixed_header);

/**
 * @brief An enumeration of CONNECT packet flags.
 * @ingroup packers
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718030">
 * MQTT v3.1.1: CONNECT Variable Header.
 * </a> 
 */
enum MQTTConnectFlags {
    MQTT_CONNECT_RESERVED = 1u,
    MQTT_CONNECT_CLEAN_SESSION = 2u,
    MQTT_CONNECT_WILL_FLAG = 4u,
    MQTT_CONNECT_WILL_QOS_0 = (0u & 0x03) << 3,
    MQTT_CONNECT_WILL_QOS_1 = (1u & 0x03) << 3,
    MQTT_CONNECT_WILL_QOS_2 = (2u & 0x03) << 3,
    MQTT_CONNECT_WILL_RETAIN = 32u,
    MQTT_CONNECT_PASSWORD = 64u,
    MQTT_CONNECT_USER_NAME = 128u
};

/**
 * @brief Serialize a connection request into a buffer. 
 * @ingroup packers
 * 
 * @param[out] buf the buffer to pack the connection request packet into.
 * @param[in] bufsz the number of bytes left in \p buf.
 * @param[in] client_id the ID that identifies the local client. \p client_id can be NULL or an empty
 *                      string for Anonymous clients.
 * @param[in] will_topic the topic under which the local client's will message will be published.
 *                       Set to \c NULL for no will message. If \p will_topic is not \c NULL a
 *                       \p will_message must also be provided.
 * @param[in] will_message the will message to be published upon a unsuccessful disconnection of
 *                         the local client. Set to \c NULL if \p will_topic is \c NULL. 
 *                         \p will_message must \em not be \c NULL if \p will_topic is not 
 *                         \c NULL.
 * @param[in] will_message_size The size of \p will_message in bytes.
 * @param[in] user_name the username to be used to connect to the broker with. Set to \c NULL if 
 *                      no username is required.
 * @param[in] password the password to be used to connect to the broker with. Set to \c NULL if
 *                     no password is required.
 * @param[in] connect_flags additional MQTTConnectFlags to be set. The only flags that need to be
 *                          set manually are \c MQTT_CONNECT_CLEAN_SESSION,
 *                          \c MQTT_CONNECT_WILL_QOS_X (for \c X &isin; {0, 1, 2}), and 
 *                          \c MQTT_CONNECT_WILL_RETAIN. Set to 0 if no additional flags are 
 *                          required.
 * @param[in] keep_alive the keep alive time in seconds. It is the responsibility of the clinet 
 *                       to ensure packets are sent to the server \em {at least} this frequently.
 * 
 * @note If there is a \p will_topic and no additional \p connect_flags are given, then by 
 *       default \p will_message will be published at QoS level 0.
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718028">
 * MQTT v3.1.1: CONNECT - Client Requests a Connection to a Server.
 * </a>
 * 
 * @returns The number of bytes put into \p buf, 0 if \p buf is too small to fit the CONNECT 
 *          packet, a negative value if there was a protocol violation.
 */
ssize_t mqtt_pack_connection_request(uint8_t* buf, size_t bufsz, 
                                     const char* client_id,
                                     const char* will_topic,
                                     const void* will_message,
                                     size_t will_message_size,
                                     const char* user_name,
                                     const char* password,
                                     uint8_t connect_flags,
                                     uint16_t keep_alive);

/**
 * @brief An enumeration of the PUBLISH flags.
 * @ingroup packers
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718037">
 * MQTT v3.1.1: PUBLISH - Publish Message.
 * </a>
 */
enum MQTTPublishFlags {
    MQTT_PUBLISH_DUP = 8u,
    MQTT_PUBLISH_QOS_0 = ((0u << 1) & 0x06),
    MQTT_PUBLISH_QOS_1 = ((1u << 1) & 0x06),
    MQTT_PUBLISH_QOS_2 = ((2u << 1) & 0x06),
    MQTT_PUBLISH_QOS_MASK = ((3u << 1) & 0x06),
    MQTT_PUBLISH_RETAIN = 0x01
};

/**
 * @brief Serialize a PUBLISH request and put it in \p buf.
 * @ingroup packers
 * 
 * @param[out] buf the buffer to put the PUBLISH packet in.
 * @param[in] bufsz the maximum number of bytes that can be put into \p buf.
 * @param[in] topic_name the topic to publish \p application_message under.
 * @param[in] packet_id this packets packet ID.
 * @param[in] application_message the application message to be published.
 * @param[in] application_message_size the size of \p application_message in bytes.
 * @param[in] publish_flags The flags to publish \p application_message with. These include
 *                          the \c MQTT_PUBLISH_DUP flag, \c MQTT_PUBLISH_QOS_X (\c X &isin; 
 *                          {0, 1, 2}), and \c MQTT_PUBLISH_RETAIN flag.
 * 
 * @note The default QoS is level 0.
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718037">
 * MQTT v3.1.1: PUBLISH - Publish Message.
 * </a>
 * 
 * @returns The number of bytes put into \p buf, 0 if \p buf is too small to fit the PUBLISH 
 *          packet, a negative value if there was a protocol violation.
 */
ssize_t mqtt_pack_publish_request(uint8_t *buf, size_t bufsz,
                                  const char* topic_name,
                                  uint16_t packet_id,
                                  void* application_message,
                                  size_t application_message_size,
                                  uint8_t publish_flags);

/**
 * @brief Serialize a PUBACK, PUBREC, PUBREL, or PUBCOMP packet and put it in \p buf.
 * @ingroup packers
 * 
 * @param[out] buf the buffer to put the PUBXXX packet in.
 * @param[in] bufsz the maximum number of bytes that can be put into \p buf.
 * @param[in] control_type the type of packet. Must be one of: \c MQTT_CONTROL_PUBACK, 
 *                         \c MQTT_CONTROL_PUBREC, \c MQTT_CONTROL_PUBREL, 
 *                         or \c MQTT_CONTROL_PUBCOMP.
 * @param[in] packet_id the packet ID of the packet being acknowledged.
 * 
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718043">
 * MQTT v3.1.1: PUBACK - Publish Acknowledgement.
 * </a>
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718048">
 * MQTT v3.1.1: PUBREC - Publish Received.
 * </a>
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718053">
 * MQTT v3.1.1: PUBREL - Publish Released.
 * </a>
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718058">
 * MQTT v3.1.1: PUBCOMP - Publish Complete.
 * </a>
 * 
 * @returns The number of bytes put into \p buf, 0 if \p buf is too small to fit the PUBXXX 
 *          packet, a negative value if there was a protocol violation.
 */
ssize_t mqtt_pack_pubxxx_request(uint8_t *buf, size_t bufsz, 
                                 enum MQTTControlPacketType control_type,
                                 uint16_t packet_id);

/** 
 * @brief The maximum number topics that can be subscribed to in a single call to 
 *         mqtt_pack_subscribe_request.
 * @ingroup packers
 * 
 * @see mqtt_pack_subscribe_request
 */
#define MQTT_SUBSCRIBE_REQUEST_MAX_NUM_TOPICS 8

/** 
 * @brief Serialize a SUBSCRIBE packet and put it in \p buf.
 * @ingroup packers
 * 
 * @param[out] buf the buffer to put the SUBSCRIBE packet in.
 * @param[in] bufsz the maximum number of bytes that can be put into \p buf.
 * @param[in] packet_id the packet ID to be used.
 * @param[in] ... \c NULL terminated list of (\c {const char *topic_name}, \c {int max_qos_level})
 *                pairs.
 * 
 * @note The variadic arguments, \p ..., \em must be followed by a \c NULL. For example:
 * @code
 * ssize_t n = mqtt_pack_subscribe_request(buf, bufsz, 1234, "topic_1", 0, "topic_2", 2, NULL);
 * @endcode
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718063">
 * MQTT v3.1.1: SUBSCRIBE - Subscribe to Topics.
 * </a>
 * 
 * @returns The number of bytes put into \p buf, 0 if \p buf is too small to fit the SUBSCRIBE 
 *          packet, a negative value if there was a protocol violation.
 */
ssize_t mqtt_pack_subscribe_request(uint8_t *buf, size_t bufsz, 
                                    unsigned int packet_id, 
                                    ...); /* null terminated */

/** 
 * @brief The maximum number topics that can be subscribed to in a single call to 
 *         mqtt_pack_unsubscribe_request.
 * @ingroup packers
 * 
 * @see mqtt_pack_unsubscribe_request
 */
#define MQTT_UNSUBSCRIBE_REQUEST_MAX_NUM_TOPICS 8

/** 
 * @brief Serialize a UNSUBSCRIBE packet and put it in \p buf.
 * @ingroup packers
 * 
 * @param[out] buf the buffer to put the UNSUBSCRIBE packet in.
 * @param[in] bufsz the maximum number of bytes that can be put into \p buf.
 * @param[in] packet_id the packet ID to be used.
 * @param[in] ... \c NULL terminated list of \c {const char *topic_name}'s to unsubscribe from.
 * 
 * @note The variadic arguments, \p ..., \em must be followed by a \c NULL. For example:
 * @code
 * ssize_t n = mqtt_pack_unsubscribe_request(buf, bufsz, 4321, "topic_1", "topic_2", NULL);
 * @endcode
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718072">
 * MQTT v3.1.1: UNSUBSCRIBE - Unsubscribe from Topics.
 * </a>
 * 
 * @returns The number of bytes put into \p buf, 0 if \p buf is too small to fit the UNSUBSCRIBE 
 *          packet, a negative value if there was a protocol violation.
 */
ssize_t mqtt_pack_unsubscribe_request(uint8_t *buf, size_t bufsz, 
                                      unsigned int packet_id, 
                                      ...); /* null terminated */

/**
 * @brief Serialize a PINGREQ and put it into \p buf.
 * @ingroup packers
 * 
 * @param[out] buf the buffer to put the PINGREQ packet in.
 * @param[in] bufsz the maximum number of bytes that can be put into \p buf.
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718081">
 * MQTT v3.1.1: PINGREQ - Ping Request.
 * </a>
 * 
 * @returns The number of bytes put into \p buf, 0 if \p buf is too small to fit the PINGREQ
 *          packet, a negative value if there was a protocol violation.
 */
ssize_t mqtt_pack_ping_request(uint8_t *buf, size_t bufsz);

/**
 * @brief Serialize a DISCONNECT and put it into \p buf.
 * @ingroup packers
 * 
 * @param[out] buf the buffer to put the DISCONNECT packet in.
 * @param[in] bufsz the maximum number of bytes that can be put into \p buf.
 * 
 * @see <a href="http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718090">
 * MQTT v3.1.1: DISCONNECT - Disconnect Notification.
 * </a>
 * 
 * @returns The number of bytes put into \p buf, 0 if \p buf is too small to fit the DISCONNECT 
 *          packet, a negative value if there was a protocol violation.
 */
ssize_t mqtt_pack_disconnect(uint8_t *buf, size_t bufsz);


/**
 * @brief An enumeration of queued message states. 
 * @ingroup details
 */
enum MQTTQueuedMessageState {
    MQTT_QUEUED_UNSENT,
    MQTT_QUEUED_AWAITING_ACK,
    MQTT_QUEUED_COMPLETE
};

/**
 * @brief A message in a mqtt_message_queue.
 * @ingroup details
 */
struct mqtt_queued_message {
    /** @brief A pointer to the start of the message. */
    uint8_t *start;

    /** @brief The number of bytes in the message. */
    size_t size;


    /** @brief The state of the message. */
    enum MQTTQueuedMessageState state;

    /** 
     * @brief The time at which the message was sent..
     * 
     * @note A timeout will only occur if the message is in
     *       the MQTT_QUEUED_AWAITING_ACK \c state.
     */
    mqtt_pal_time_t time_sent;

    /**
     * @brief The control type of the message.
     */
    enum MQTTControlPacketType control_type;

    /** 
     * @brief The packet id of the message.
     * 
     * @note This field is only used if the associate \c control_type has a 
     *       \c packet_id field.
     */
    uint16_t packet_id;
};

/**
 * @brief A message queue.
 * @ingroup details
 * 
 * @note This struct is used internally to manage sending messages.
 * @note The only members the user should use are \c curr and \c curr_sz. 
 */
struct mqtt_message_queue {
    /** 
     * @brief The start of the message queue's memory block. 
     * 
     * @warning This member should \em not be manually changed.
     */
    void *mem_start;

    /** @brief The end of the message queue's memory block. */
    void *mem_end;

    /**
     * @brief A pointer to the position in the buffer you can pack bytes at.
     * 
     * @note Immediately after packing bytes at \c curr you \em must call
     *       mqtt_mq_register.
     */
    uint8_t *curr;

    /**
     * @brief The number of bytes that can be written to \c curr.
     * 
     * @note curr_sz will decrease by more than the number of bytes you write to 
     *       \c curr. This is because the mqtt_queued_message structs share the 
     *       same memory (and thus, a mqtt_queued_message must be allocated in 
     *       the message queue's memory whenever a new message is registered).  
     */
    size_t curr_sz;
    
    /**
     * @brief The tail of the array of mqtt_queued_messages's.
     * 
     * @note This member should not be used manually.
     */
    struct mqtt_queued_message *queue_tail;
};

/**
 * @brief Initialize a message queue.
 * @ingroup details
 * 
 * @param[out] mq The message queue to initialize.
 * @param[in] buf The buffer for this message queue.
 * @param[in] bufsz The number of bytes in the buffer. 
 * 
 * @relates mqtt_message_queue
 */
void mqtt_mq_init(struct mqtt_message_queue *mq, void *buf, size_t bufsz);

/**
 * @brief Clear as many messages from the front of the queue as possible.
 * @ingroup details
 * 
 * @note Calls to this function are the \em only way to remove messages from the queue.
 * 
 * @param mq The message queue.
 * 
 * @relates mqtt_message_queue
 */
void mqtt_mq_clean(struct mqtt_message_queue *mq);

/**
 * @brief Register a message that was just added to the buffer.
 * @ingroup details
 * 
 * @note This function should be called immediately following a call to a packer function
 *       that returned a positive value. The positive value (number of bytes packed) should
 *       be passed to this function.
 * 
 * @param mq The message queue.
 * @param[in] nbytes The number of bytes that were just packed.
 * 
 * @note This function will step mqtt_message_queue::curr and update mqtt_message_queue::curr_sz.
 * @relates mqtt_message_queue
 * 
 * @returns The newly added struct mqtt_queued_message.
 */
struct mqtt_queued_message* mqtt_mq_register(struct mqtt_message_queue *mq, size_t nbytes);

/**
 * @brief Find a message in the message queue.
 * @ingroup details
 * 
 * @param mq The message queue.
 * @param[in] control_type The control type of the message you want to find.
 * @param[in] packet_id The packet ID of the message you want to find. Set to \c NULL if you 
 *            don't want to specify a packet ID.
 * 
 * @relates mqtt_message_queue
 * @returns The found message. \c NULL if the message was not found.
 */
struct mqtt_queued_message* mqtt_mq_find(struct mqtt_message_queue *mq, enum MQTTControlPacketType control_type, uint16_t *packet_id);

/**
 * @brief Returns the mqtt_queued_message at \p index.
 * @ingroup details
 * 
 * @param mq_ptr A pointer to the message queue.
 * @param index The index of the message. 
 *
 * @returns The mqtt_queued_message at \p index.
 */
#define mqtt_mq_get(mq_ptr, index) (((struct mqtt_queued_message*) ((mq_ptr)->mem_end)) - 1 - index)

/**
 * @brief Returns the number of messages in the message queue, \p mq_ptr.
 * @ingroup details
 */
#define mqtt_mq_length(mq_ptr) (((struct mqtt_queued_message*) ((mq_ptr)->mem_end)) - (mq_ptr)->queue_tail)

/**
 * @brief Used internally to recalculate the \c curr_sz.
 * @ingroup details
 */
#define mqtt_mq_currsz(mq_ptr) (mq_ptr->curr >= (uint8_t*) ((mq_ptr)->queue_tail - 1)) ? 0 : ((uint8_t*) ((mq_ptr)->queue_tail - 1)) - (mq_ptr)->curr

/* CLIENT */

/**
 * @brief An MQTT client. 
 * @ingroup details
 * 
 * @note All members can be manipulated via the related functions.
 */
struct mqtt_client {
    /** @brief The socket connecting to the MQTT broker. */
    mqtt_pal_socket_handle socketfd;

    /** @brief The LFSR state used to generate packet ID's. */
    uint16_t pid_lfsr;

    /** @brief The keep-alive time in seconds. */
    uint16_t keep_alive;

    /** 
     * @brief A counter counting pings that have been sent to keep the connection alive. 
     * @see keep_alive
     */
    int number_of_keep_alives;

    /**
     * @brief The current sent offset.
     *
     * This is used to allow partial send commands.
     */
    size_t send_offset;

    /** 
     * @brief The timestamp of the last message sent to the buffer.
     * 
     * This is used to detect the need for keep-alive pings.
     * 
     * @see keep_alive
    */
    mqtt_pal_time_t time_of_last_send;

    /** 
     * @brief The error state of the client. 
     * 
     * error should be MQTT_OK for the entirety of the connection.
     * 
     * @note The error state will be MQTT_ERROR_CONNECT_NOT_CALLED until
     *       you call mqtt_connect.
     */
    enum MQTTErrors error;

    /** 
     * @brief The timeout period in seconds.
     * 
     * If the broker doesn't return an ACK within response_timeout seconds a timeout
     * will occur and the message will be retransmitted. 
     * 
     * @note The default value is 30 [seconds] but you can change it at any time.
     */
    int response_timeout;

    /** @brief A counter counting the number of timeouts that have occurred. */
    int number_of_timeouts;

    /**
     * @brief Approximately much time it has typically taken to receive responses from the 
     *        broker.
     * 
     * @note This is tracked using a exponential-averaging.
     */
    double typical_response_time;

    /**
     * @brief The callback that is called whenever a publish is received from the broker.
     * 
     * Any topics that you have subscribed to will be returned from the broker as 
     * mqtt_response_publish messages. All the publishes received from the broker will 
     * be passed to this function.
     * 
     * @note A pointer to publish_response_callback_state is always passed to the callback.
     *       Use publish_response_callback_state to keep track of any state information you 
     *       need.
     */
    void (*publish_response_callback)(void** state, struct mqtt_response_publish *publish);

    /**
     * @brief A pointer to any publish_response_callback state information you need.
     * 
     * @note A pointer to this pointer will always be publish_response_callback upon 
     *       receiving a publish message from the broker.
     */
    void* publish_response_callback_state;

    /**
     * @brief A user-specified callback, triggered on each \ref mqtt_sync, allowing
     *        the user to perform state inspections (and custom socket error detection)
     *        on the client.
     * 
     * This callback is triggered on each call to \ref mqtt_sync. If it returns MQTT_OK
     * then \ref mqtt_sync will continue normally (performing reads and writes). If it
     * returns an error then \ref mqtt_sync will not call reads and writes.
     * 
     * This callback can be used to perform custom error detection, namely platform
     * specific socket error detection, and force the client into an error state.
     * 
     * This member is always initialized to NULL but it can be manually set at any 
     * time.
     */
    enum MQTTErrors (*inspector_callback)(struct mqtt_client*);

    /**
     * @brief A callback that is called whenever the client is in an error state.
     * 
     * This callback is responsible for: application level error handling, closing
     * previous sockets, and reestabilishing the connection to the broker and 
     * session configurations (i.e. subscriptions).  
     */
    void (*reconnect_callback)(struct mqtt_client*, void**);

    /**
     * @brief A pointer to some state. A pointer to this member is passed to 
     *        \ref mqtt_client.reconnect_callback.
     */
    void* reconnect_state;

    /**
     * @brief The buffer where ingress data is temporarily stored.
     */
    struct {
        /** @brief The start of the receive buffer's memory. */
        uint8_t *mem_start;

        /** @brief The size of the receive buffer's memory. */
        size_t mem_size;

        /** @brief A pointer to the next writtable location in the receive buffer. */
        uint8_t *curr;

        /** @brief The number of bytes that are still writable at curr. */
        size_t curr_sz;
    } recv_buffer;

    /** 
     * @brief A variable passed to support thread-safety.
     * 
     * A pointer to this variable is passed to \c MQTT_PAL_MUTEX_LOCK, and
     * \c MQTT_PAL_MUTEX_UNLOCK.
     */
    mqtt_pal_mutex_t mutex;

    /** @brief The sending message queue. */
    struct mqtt_message_queue mq;
};

/**
 * @brief Generate a new next packet ID.
 * @ingroup details
 * 
 * Packet ID's are generated using a max-length LFSR.
 * 
 * @param client The MQTT client.
 * 
 * @returns The new packet ID that should be used.
 */
uint16_t __mqtt_next_pid(struct mqtt_client *client);

/**
 * @brief Handles egress client traffic.
 * @ingroup details
 * 
 * @param client The MQTT client.
 * 
 * @returns MQTT_OK upon success, an \ref MQTTErrors otherwise. 
 */
ssize_t __mqtt_send(struct mqtt_client *client);

/**
 * @brief Handles ingress client traffic.
 * @ingroup details
 * 
 * @param client The MQTT client.
 * 
 * @returns MQTT_OK upon success, an \ref MQTTErrors otherwise. 
 */
ssize_t __mqtt_recv(struct mqtt_client *client);

/**
 * @brief Function that does the actual sending and receiving of 
 *        traffic from the network.
 * @ingroup api
 * 
 * All the other functions in the @ref api simply stage messages for
 * being sent to the broker. This function does the actual sending of
 * those messages. Additionally this function receives traffic (responses and 
 * acknowledgements) from the broker and responds to that traffic accordingly.
 * Lastly this function also calls the \c publish_response_callback when
 * any \c MQTT_CONTROL_PUBLISH messages are received.
 * 
 * @pre mqtt_init must have been called.
 * 
 * @param[in,out] client The MQTT client.
 * 
 * @attention It is the responsibility of the application programmer to 
 *            call this function periodically. All functions in the @ref api are
 *            thread-safe so it is perfectly reasonable to have a thread dedicated
 *            to calling this function every 200 ms or so. MQTT-C can be used in single
 *            threaded application though by simply calling this functino periodically 
 *            inside your main thread. See @ref simple_publisher.c and @ref simple_subscriber.c
 *            for examples (specifically the \c client_refresher functions).
 * 
 * @returns MQTT_OK upon success, an \ref MQTTErrors otherwise. 
 */
enum MQTTErrors mqtt_sync(struct mqtt_client *client);

/**
 * @brief Initializes an MQTT client.
 * @ingroup api
 * 
 * This function \em must be called before any other API function calls.
 * 
 * @pre None.
 * 
 * @param[out] client The MQTT client.
 * @param[in] sockfd The socket file descriptor (or equivalent socket handle, e.g. BIO pointer 
 *            for OpenSSL sockets) connected to the MQTT broker.
 * @param[in] sendbuf A buffer that will be used for sending messages to the broker.
 * @param[in] sendbufsz The size of \p sendbuf in bytes.
 * @param[in] recvbuf A buffer that will be used for receiving messages from the broker.
 * @param[in] recvbufsz The size of \p recvbuf in bytes.
 * @param[in] publish_response_callback The callback to call whenever application messages
 *            are received from the broker. 
 * 
 * @post mqtt_connect must be called.
 * 
 * @note \p sockfd is a non-blocking TCP connection.
 * @note If \p sendbuf fills up completely during runtime a \c MQTT_ERROR_SEND_BUFFER_IS_FULL
 *       error will be set. Similarly if \p recvbuf is ever to small to receive a message from
 *       the broker an MQTT_ERROR_RECV_BUFFER_TOO_SMALL error will be set.
 * @note A pointer to \ref mqtt_client.publish_response_callback_state is always passed as the 
 *       \c state argument to \p publish_response_callback. Note that the second argument is 
 *       the mqtt_response_publish that was received from the broker.
 * 
 * @attention Only initialize an MQTT client once (i.e. don't call \ref mqtt_init or 
 *            \ref mqtt_init_reconnect more than once per client).
 * 
 * @returns \c MQTT_OK upon success, an \ref MQTTErrors otherwise.
 */
enum MQTTErrors mqtt_init(struct mqtt_client *client,
                          mqtt_pal_socket_handle sockfd,
                          uint8_t *sendbuf, size_t sendbufsz,
                          uint8_t *recvbuf, size_t recvbufsz,
                          void (*publish_response_callback)(void** state, struct mqtt_response_publish *publish));

/**
 * @brief Initializes an MQTT client and enables automatic reconnections.
 * @ingroup api
 * 
 * An alternative to \ref mqtt_init that allows the client to automatically reconnect to the 
 * broker after an error occurs (e.g. socket error or internal buffer overflows).
 * 
 * This is accomplished by calling the \p reconnect_callback whenever the client enters an error 
 * state. The job of the \p reconnect_callback is to: (1) perform error handling/logging, 
 * (2) clean up the old connection (i.e. close client->socketfd), (3) \ref mqtt_reinit the 
 * client, and (4) reconfigure the MQTT session by calling \ref mqtt_connect followed by other 
 * API calls such as \ref mqtt_subscribe.
 * 
 * The first argument to the \p reconnect_callback is the client (which will be in an error 
 * state) and the second argument is a pointer to a void pointer where you can store some state
 * information. Internally, MQTT-C calls the reconnect callback like so: 
 * 
 * \code 
 *     client->reconnect_callback(client, &client->reconnect_state)
 * \endcode
 * 
 * Note that the \p reconnect_callback is also called to setup the initial session. After
 * calling \ref mqtt_init_reconnect the client will be in the error state 
 * \c MQTT_ERROR_INITIAL_RECONNECT.
 * 
 * @pre None.
 * 
 * @param[in,out] client The MQTT client that will be initialized.
 * @param[in] reconnect_callback The callback that will be called to connect/reconnect the 
 *            client to the broker and perform application level error handling. 
 * @param[in] reconnect_state A pointer to some state data for your \p reconnect_callback.
 *            If your \p reconnect_callback does not require any state information set this
 *            to NULL. A pointer to the memory address where the client stores a copy of this
 *            pointer is passed as the second argumnet to \p reconnect_callback. 
 * @param[in] publish_response_callback The callback to call whenever application messages
 *            are received from the broker. 
 * 
 * @post Call \p reconnect_callback yourself, or call \ref mqtt_sync 
 *       (which will trigger the call to \p reconnect_callback).
 * 
 * @attention Only initialize an MQTT client once (i.e. don't call \ref mqtt_init or 
 *            \ref mqtt_init_reconnect more than once per client).
 *
 */
void mqtt_init_reconnect(struct mqtt_client *client,
                         void (*reconnect_callback)(struct mqtt_client *client, void** state),
                         void *reconnect_state,
                         void (*publish_response_callback)(void** state, struct mqtt_response_publish *publish));

/**
 * @brief Safely assign/reassign a socket and buffers to an new/existing client.
 * @ingroup api
 * 
 * This function also clears the \p client error state. Upon exiting this function
 * \c client->error will be \c MQTT_ERROR_CONNECT_NOT_CALLED (which will be cleared)
 * as soon as \ref mqtt_connect is called.
 * 
 * @pre This function must be called BEFORE \ref mqtt_connect. 
 * 
 * @param[in,out] client The MQTT client.
 * @param[in] socketfd The new socket connected to the broker. 
 * @param[in] sendbuf The buffer that will be used to buffer egress traffic to the broker.
 * @param[in] sendbufsz The size of \p sendbuf in bytes.
 * @param[in] recvbuf The buffer that will be used to buffer ingress traffic from the broker.
 * @param[in] recvbufsz The size of \p recvbuf in bytes.
 * 
 * @post Call \ref mqtt_connect.
 * 
 * @attention This function should be used in conjunction with clients that have been 
 *            initialzed with \ref mqtt_init_reconnect.  
 */
void mqtt_reinit(struct mqtt_client* client,
                 mqtt_pal_socket_handle socketfd,
                 uint8_t *sendbuf, size_t sendbufsz,
                 uint8_t *recvbuf, size_t recvbufsz);

/**
 * @brief Establishes a session with the MQTT broker.
 * @ingroup api
 * 
 * @pre mqtt_init must have been called.
 * 
 * @param[in,out] client The MQTT client.
 * @param[in] client_id The unique name identifying the client. (or NULL)
 * @param[in] will_topic The topic name of client's \p will_message. If no will message is 
 *            desired set to \c NULL.
 * @param[in] will_message The application message (data) to be published in the event the 
 *            client ungracefully disconnects. Set to \c NULL if \p will_topic is \c NULL.
 * @param[in] will_message_size The size of \p will_message in bytes.
 * @param[in] user_name The username to use when establishing the session with the MQTT broker.
 *            Set to \c NULL if a username is not required.
 * @param[in] password The password to use when establishing the session with the MQTT broker.
 *            Set to \c NULL if a password is not required.
 * @param[in] connect_flags Additional \ref MQTTConnectFlags to use when establishing the connection. 
 *            These flags are for forcing the session to start clean, 
 *            \c MQTT_CONNECT_CLEAN_SESSION, the QOS level to publish the \p will_message with 
 *            (provided \c will_message != \c NULL), MQTT_CONNECT_WILL_QOS_[0,1,2], and whether 
 *            or not the broker should retain the \c will_message, MQTT_CONNECT_WILL_RETAIN.
 * @param[in] keep_alive The keep-alive time in seconds. A reasonable value for this is 400 [seconds]. 
 * 
 * @returns \c MQTT_OK upon success, an \ref MQTTErrors otherwise.
 */
enum MQTTErrors mqtt_connect(struct mqtt_client *client,
                             const char* client_id,
                             const char* will_topic,
                             const void* will_message,
                             size_t will_message_size,
                             const char* user_name,
                             const char* password,
                             uint8_t connect_flags,
                             uint16_t keep_alive);

/* 
    todo: will_message should be a void*
*/

/**
 * @brief Publish an application message.
 * @ingroup api
 * 
 * Publishes an application message to the MQTT broker.
 * 
 * @pre mqtt_connect must have been called.
 * 
 * @param[in,out] client The MQTT client.
 * @param[in] topic_name The name of the topic.
 * @param[in] application_message The data to be published.
 * @param[in] application_message_size The size of \p application_message in bytes.
 * @param[in] publish_flags \ref MQTTPublishFlags to be used, namely the QOS level to 
 *            publish at (MQTT_PUBLISH_QOS_[0,1,2]) or whether or not the broker should 
 *            retain the publish (MQTT_PUBLISH_RETAIN).
 * 
 * @returns \c MQTT_OK upon success, an \ref MQTTErrors otherwise.
 */
enum MQTTErrors mqtt_publish(struct mqtt_client *client,
                             const char* topic_name,
                             void* application_message,
                             size_t application_message_size,
                             uint8_t publish_flags);

/**
 * @brief Acknowledge an ingree publish with QOS==1.
 * @ingroup details
 *
 * @param[in,out] client The MQTT client.
 * @param[in] packet_id The packet ID of the ingress publish being acknowledged.
 * 
 * @returns \c MQTT_OK upon success, an \ref MQTTErrors otherwise. 
 */
ssize_t __mqtt_puback(struct mqtt_client *client, uint16_t packet_id);

/**
 * @brief Acknowledge an ingree publish with QOS==2.
 * @ingroup details
 *
 * @param[in,out] client The MQTT client.
 * @param[in] packet_id The packet ID of the ingress publish being acknowledged.
 * 
 * @returns \c MQTT_OK upon success, an \ref MQTTErrors otherwise. 
 */
ssize_t __mqtt_pubrec(struct mqtt_client *client, uint16_t packet_id);

/**
 * @brief Acknowledge an ingree PUBREC packet.
 * @ingroup details
 *
 * @param[in,out] client The MQTT client.
 * @param[in] packet_id The packet ID of the ingress PUBREC being acknowledged.
 * 
 * @returns \c MQTT_OK upon success, an \ref MQTTErrors otherwise. 
 */
ssize_t __mqtt_pubrel(struct mqtt_client *client, uint16_t packet_id);

/**
 * @brief Acknowledge an ingree PUBREL packet.
 * @ingroup details
 *
 * @param[in,out] client The MQTT client.
 * @param[in] packet_id The packet ID of the ingress PUBREL being acknowledged.
 * 
 * @returns \c MQTT_OK upon success, an \ref MQTTErrors otherwise. 
 */
ssize_t __mqtt_pubcomp(struct mqtt_client *client, uint16_t packet_id);


/**
 * @brief Subscribe to a topic.
 * @ingroup api
 * 
 * @pre mqtt_connect must have been called.
 * 
 * @param[in,out] client The MQTT client.
 * @param[in] topic_name The name of the topic to subscribe to.
 * @param[in] max_qos_level The maximum QOS level with which the broker can send application
 *            messages for this topic.
 * 
 * @returns \c MQTT_OK upon success, an \ref MQTTErrors otherwise. 
 */
enum MQTTErrors mqtt_subscribe(struct mqtt_client *client,
                               const char* topic_name,
                               int max_qos_level);

/**
 * @brief Unsubscribe from a topic.
 * @ingroup api
 * 
 * @pre mqtt_connect must have been called.
 * 
 * @param[in,out] client The MQTT client.
 * @param[in] topic_name The name of the topic to unsubscribe from.
 * 
 * @returns \c MQTT_OK upon success, an \ref MQTTErrors otherwise. 
 */
enum MQTTErrors mqtt_unsubscribe(struct mqtt_client *client,
                                 const char* topic_name);

/**
 * @brief Ping the broker. 
 * @ingroup api
 * 
 * @pre mqtt_connect must have been called.
 * 
 * @param[in,out] client The MQTT client.
 * 
 * @returns \c MQTT_OK upon success, an \ref MQTTErrors otherwise.
 */
enum MQTTErrors mqtt_ping(struct mqtt_client *client);

/**
 * @brief Ping the broker without locking/unlocking the mutex. 
 * @see mqtt_ping
 */
enum MQTTErrors __mqtt_ping(struct mqtt_client *client);

/**
 * @brief Terminate the session with the MQTT broker. 
 * @ingroup api
 * 
 * @pre mqtt_connect must have been called.
 * 
 * @param[in,out] client The MQTT client.
 * 
 * @note To re-establish the session, mqtt_connect must be called.
 * 
 * @returns \c MQTT_OK upon success, an \ref MQTTErrors otherwise.
 */
enum MQTTErrors mqtt_disconnect(struct mqtt_client *client);

#endif
