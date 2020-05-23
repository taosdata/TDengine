/*******************************************************************************
 * Copyright (c) 2009, 2019 IBM Corp.
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
 *    Ian Craggs - MQTT 3.1.1 support
 *    Ian Craggs - big endian Linux reversed definition
 *    Ian Craggs - MQTT 5.0 support
 *******************************************************************************/

#if !defined(MQTTPACKET_H)
#define MQTTPACKET_H

#include "Socket.h"
#if defined(OPENSSL)
#include "SSLSocket.h"
#endif
#include "LinkedList.h"
#include "Clients.h"

typedef unsigned int bool;
typedef void* (*pf)(int, unsigned char, char*, size_t);

#include "MQTTProperties.h"
#include "MQTTReasonCodes.h"

enum errors
{
	MQTTPACKET_BAD = -4,
	MQTTPACKET_BUFFER_TOO_SHORT = -2,
	MQTTPACKET_READ_ERROR = -1,
	MQTTPACKET_READ_COMPLETE
};


enum msgTypes
{
	CONNECT = 1, CONNACK, PUBLISH, PUBACK, PUBREC, PUBREL,
	PUBCOMP, SUBSCRIBE, SUBACK, UNSUBSCRIBE, UNSUBACK,
	PINGREQ, PINGRESP, DISCONNECT, AUTH
};

#if defined(__linux__)
#include <endian.h>
#if __BYTE_ORDER == __BIG_ENDIAN
	#define REVERSED 1
#endif
#endif

/**
 * Bitfields for the MQTT header byte.
 */
typedef union
{
	/*unsigned*/ char byte;	/**< the whole byte */
#if defined(REVERSED)
	struct
	{
		unsigned int type : 4;	/**< message type nibble */
		bool dup : 1;			/**< DUP flag bit */
		unsigned int qos : 2;	/**< QoS value, 0, 1 or 2 */
		bool retain : 1;		/**< retained flag bit */
	} bits;
#else
	struct
	{
		bool retain : 1;		/**< retained flag bit */
		unsigned int qos : 2;	/**< QoS value, 0, 1 or 2 */
		bool dup : 1;			/**< DUP flag bit */
		unsigned int type : 4;	/**< message type nibble */
	} bits;
#endif
} Header;


/**
 * Data for a connect packet.
 */
typedef struct
{
	Header header;	/**< MQTT header byte */
	union
	{
		unsigned char all;	/**< all connect flags */
#if defined(REVERSED)
		struct
		{
			bool username : 1;			/**< 3.1 user name */
			bool password : 1; 			/**< 3.1 password */
			bool willRetain : 1;		/**< will retain setting */
			unsigned int willQoS : 2;	/**< will QoS value */
			bool will : 1;			/**< will flag */
			bool cleanstart : 1;	/**< cleansession flag */
			int : 1;	/**< unused */
		} bits;
#else
		struct
		{
			int : 1;	/**< unused */
			bool cleanstart : 1;	/**< cleansession flag */
			bool will : 1;			/**< will flag */
			unsigned int willQoS : 2;	/**< will QoS value */
			bool willRetain : 1;		/**< will retain setting */
			bool password : 1; 			/**< 3.1 password */
			bool username : 1;			/**< 3.1 user name */
		} bits;
#endif
	} flags;	/**< connect flags byte */

	char *Protocol, /**< MQTT protocol name */
		*clientID,	/**< string client id */
        *willTopic,	/**< will topic */
        *willMsg;	/**< will payload */

	int keepAliveTimer;		/**< keepalive timeout value in seconds */
	unsigned char version;	/**< MQTT version number */
} Connect;


/**
 * Data for a connack packet.
 */
typedef struct
{
	Header header; /**< MQTT header byte */
	union
	{
		unsigned char all;	/**< all connack flags */
#if defined(REVERSED)
		struct
		{
			unsigned int reserved : 7;	/**< message type nibble */
			bool sessionPresent : 1;    /**< was a session found on the server? */
		} bits;
#else
		struct
		{
			bool sessionPresent : 1;    /**< was a session found on the server? */
			unsigned int reserved : 7;	/**< message type nibble */
		} bits;
#endif
	} flags;	 /**< connack flags byte */
	unsigned char rc; /**< connack reason code */
	unsigned int MQTTVersion;  /**< the version of MQTT */
	MQTTProperties properties; /**< MQTT 5.0 properties.  Not used for MQTT < 5.0 */
} Connack;


/**
 * Data for a packet with header only.
 */
typedef struct
{
	Header header;	/**< MQTT header byte */
} MQTTPacket;


/**
 * Data for a suback packet.
 */
typedef struct
{
	Header header;	/**< MQTT header byte */
	int msgId;		/**< MQTT message id */
	int MQTTVersion;  /**< the version of MQTT */
	MQTTProperties properties; /**< MQTT 5.0 properties.  Not used for MQTT < 5.0 */
	List* qoss;		/**< list of granted QoSs (MQTT 3/4) / reason codes (MQTT 5) */
} Suback;


/**
 * Data for an MQTT V5 unsuback packet.
 */
typedef struct
{
	Header header;	/**< MQTT header byte */
	int msgId;		/**< MQTT message id */
	int MQTTVersion;  /**< the version of MQTT */
	MQTTProperties properties; /**< MQTT 5.0 properties.  Not used for MQTT < 5.0 */
	List* reasonCodes;	/**< list of reason codes */
} Unsuback;


/**
 * Data for a publish packet.
 */
typedef struct
{
	Header header;	/**< MQTT header byte */
	char* topic;	/**< topic string */
	int topiclen;
	int msgId;		/**< MQTT message id */
	char* payload;	/**< binary payload, length delimited */
	int payloadlen;	/**< payload length */
	int MQTTVersion;  /**< the version of MQTT */
	MQTTProperties properties; /**< MQTT 5.0 properties.  Not used for MQTT < 5.0 */
} Publish;


/**
 * Data for one of the ack packets.
 */
typedef struct
{
	Header header;	/**< MQTT header byte */
	int msgId;		/**< MQTT message id */
	unsigned char rc; /**< MQTT 5 reason code */
	int MQTTVersion;  /**< the version of MQTT */
	MQTTProperties properties; /**< MQTT 5.0 properties.  Not used for MQTT < 5.0 */
} Ack;

typedef Ack Puback;
typedef Ack Pubrec;
typedef Ack Pubrel;
typedef Ack Pubcomp;

int MQTTPacket_encode(char* buf, size_t length);
int MQTTPacket_decode(networkHandles* net, size_t* value);
int readInt(char** pptr);
char* readUTF(char** pptr, char* enddata);
unsigned char readChar(char** pptr);
void writeChar(char** pptr, char c);
void writeInt(char** pptr, int anInt);
void writeUTF(char** pptr, const char* string);
void writeData(char** pptr, const void* data, int datalen);

const char* MQTTPacket_name(int ptype);

void* MQTTPacket_Factory(int MQTTVersion, networkHandles* net, int* error);
int MQTTPacket_send(networkHandles* net, Header header, char* buffer, size_t buflen, int free, int MQTTVersion);
int MQTTPacket_sends(networkHandles* net, Header header, int count, char** buffers, size_t* buflens, int* frees, int MQTTVersion);

void* MQTTPacket_header_only(int MQTTVersion, unsigned char aHeader, char* data, size_t datalen);
int MQTTPacket_send_disconnect(Clients* client, enum MQTTReasonCodes reason, MQTTProperties* props);

void* MQTTPacket_publish(int MQTTVersion, unsigned char aHeader, char* data, size_t datalen);
void MQTTPacket_freePublish(Publish* pack);
int MQTTPacket_send_publish(Publish* pack, int dup, int qos, int retained, networkHandles* net, const char* clientID);
int MQTTPacket_send_puback(int MQTTVersion, int msgid, networkHandles* net, const char* clientID);
void* MQTTPacket_ack(int MQTTVersion, unsigned char aHeader, char* data, size_t datalen);

void MQTTPacket_freeAck(Ack* pack);
void MQTTPacket_freeSuback(Suback* pack);
void MQTTPacket_freeUnsuback(Unsuback* pack);
int MQTTPacket_send_pubrec(int MQTTVersion, int msgid, networkHandles* net, const char* clientID);
int MQTTPacket_send_pubrel(int MQTTVersion, int msgid, int dup, networkHandles* net, const char* clientID);
int MQTTPacket_send_pubcomp(int MQTTVersion, int msgid, networkHandles* net, const char* clientID);

void MQTTPacket_free_packet(MQTTPacket* pack);

void writeInt4(char** pptr, int anInt);
int readInt4(char** pptr);
void writeMQTTLenString(char** pptr, MQTTLenString lenstring);
int MQTTLenStringRead(MQTTLenString* lenstring, char** pptr, char* enddata);
int MQTTPacket_VBIlen(int rem_len);
int MQTTPacket_decodeBuf(char* buf, unsigned int* value);

#include "MQTTPacketOut.h"

#endif /* MQTTPACKET_H */
