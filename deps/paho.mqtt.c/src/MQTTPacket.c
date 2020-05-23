/*******************************************************************************
 * Copyright (c) 2009, 2020 IBM Corp.
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
 *    Ian Craggs - fix for issue 453
 *    Ian Craggs - MQTT 5.0 support
 *******************************************************************************/

/**
 * @file
 * \brief functions to deal with reading and writing of MQTT packets from and to sockets
 *
 * Some other related functions are in the MQTTPacketOut module
 */

#include "MQTTPacket.h"
#include "Log.h"
#if !defined(NO_PERSISTENCE)
	#include "MQTTPersistence.h"
#endif
#include "Messages.h"
#include "StackTrace.h"
#include "WebSocket.h"
#include "MQTTTime.h"

#include <stdlib.h>
#include <string.h>

#include "Heap.h"

#if !defined(min)
#define min(A,B) ( (A) < (B) ? (A):(B))
#endif

/**
 * List of the predefined MQTT v3/v5 packet names.
 */
static const char *packet_names[] =
{
	"RESERVED", "CONNECT", "CONNACK", "PUBLISH", "PUBACK", "PUBREC", "PUBREL",
	"PUBCOMP", "SUBSCRIBE", "SUBACK", "UNSUBSCRIBE", "UNSUBACK",
	"PINGREQ", "PINGRESP", "DISCONNECT", "AUTH"
};

const char** MQTTClient_packet_names = packet_names;


/**
 * Converts an MQTT packet code into its name
 * @param ptype packet code
 * @return the corresponding string, or "UNKNOWN"
 */
const char* MQTTPacket_name(int ptype)
{
	return (ptype >= 0 && ptype <= AUTH) ? packet_names[ptype] : "UNKNOWN";
}

/**
 * Array of functions to build packets, indexed according to packet code
 */
pf new_packets[] =
{
	NULL,	/**< reserved */
	NULL,	/**< MQTTPacket_connect*/
	MQTTPacket_connack, /**< CONNACK */
	MQTTPacket_publish,	/**< PUBLISH */
	MQTTPacket_ack, /**< PUBACK */
	MQTTPacket_ack, /**< PUBREC */
	MQTTPacket_ack, /**< PUBREL */
	MQTTPacket_ack, /**< PUBCOMP */
	NULL, /**< MQTTPacket_subscribe*/
	MQTTPacket_suback, /**< SUBACK */
	NULL, /**< MQTTPacket_unsubscribe*/
	MQTTPacket_unsuback, /**< UNSUBACK */
	MQTTPacket_header_only, /**< PINGREQ */
	MQTTPacket_header_only, /**< PINGRESP */
	MQTTPacket_ack,  /**< DISCONNECT */
	MQTTPacket_ack   /**< AUTH */
};


static char* readUTFlen(char** pptr, char* enddata, int* len);
static int MQTTPacket_send_ack(int MQTTVersion, int type, int msgid, int dup, networkHandles *net);

/**
 * Reads one MQTT packet from a socket.
 * @param socket a socket from which to read an MQTT packet
 * @param error pointer to the error code which is completed if no packet is returned
 * @return the packet structure or NULL if there was an error
 */
void* MQTTPacket_Factory(int MQTTVersion, networkHandles* net, int* error)
{
	char* data = NULL;
	static Header header;
	size_t remaining_length;
	int ptype;
	void* pack = NULL;
	size_t actual_len = 0;

	FUNC_ENTRY;
	*error = SOCKET_ERROR;  /* indicate whether an error occurred, or not */

	/* read the packet data from the socket */
	*error = WebSocket_getch(net, &header.byte);
	if (*error != TCPSOCKET_COMPLETE)   /* first byte is the header byte */
		goto exit; /* packet not read, *error indicates whether SOCKET_ERROR occurred */

	/* now read the remaining length, so we know how much more to read */
	if ((*error = MQTTPacket_decode(net, &remaining_length)) != TCPSOCKET_COMPLETE)
		goto exit; /* packet not read, *error indicates whether SOCKET_ERROR occurred */

	/* now read the rest, the variable header and payload */
	data = WebSocket_getdata(net, remaining_length, &actual_len);
	if (data == NULL)
	{
		*error = SOCKET_ERROR;
		goto exit; /* socket error */
	}

	if (actual_len != remaining_length)
		*error = TCPSOCKET_INTERRUPTED;
	else
	{
		ptype = header.bits.type;
		if (ptype < CONNECT || (MQTTVersion < MQTTVERSION_5 && ptype >= DISCONNECT) ||
				(MQTTVersion >= MQTTVERSION_5 && ptype > AUTH) ||
				new_packets[ptype] == NULL)
			Log(TRACE_MIN, 2, NULL, ptype);
		else
		{
			if ((pack = (*new_packets[ptype])(MQTTVersion, header.byte, data, remaining_length)) == NULL)
			{
				*error = SOCKET_ERROR; // was BAD_MQTT_PACKET;
				Log(LOG_ERROR, -1, "Bad MQTT packet, type %d", ptype);
			}
#if !defined(NO_PERSISTENCE)
			else if (header.bits.type == PUBLISH && header.bits.qos == 2)
			{
				int buf0len;
				char *buf = malloc(10);

				if (buf == NULL)
				{
					*error = SOCKET_ERROR;
					goto exit;
				}
				buf[0] = header.byte;
				buf0len = 1 + MQTTPacket_encode(&buf[1], remaining_length);
				*error = MQTTPersistence_put(net->socket, buf, buf0len, 1,
					&data, &remaining_length, header.bits.type, ((Publish *)pack)->msgId, 1, MQTTVersion);
				free(buf);
			}
#endif
		}
	}
	if (pack)
		net->lastReceived = MQTTTime_now();
exit:
	FUNC_EXIT_RC(*error);
	return pack;
}


/**
 * Sends an MQTT packet in one system call write
 * @param socket the socket to which to write the data
 * @param header the one-byte MQTT header
 * @param buffer the rest of the buffer to write (not including remaining length)
 * @param buflen the length of the data in buffer to be written
 * @param MQTTVersion the version of MQTT being used
 * @return the completion code (TCPSOCKET_COMPLETE etc)
 */
int MQTTPacket_send(networkHandles* net, Header header, char* buffer, size_t buflen, int freeData,
		int MQTTVersion)
{
	int rc = SOCKET_ERROR;
	size_t buf0len;
	char *buf;

	FUNC_ENTRY;
	buf0len = 1 + MQTTPacket_encode(NULL, buflen);
	buf = malloc(buf0len);
	if (buf == NULL)
	{
		rc = SOCKET_ERROR;
		goto exit;
	}
	buf[0] = header.byte;
	MQTTPacket_encode(&buf[1], buflen);

#if !defined(NO_PERSISTENCE)
	if (header.bits.type == PUBREL)
	{
		char* ptraux = buffer;
		int msgId = readInt(&ptraux);

		rc = MQTTPersistence_put(net->socket, buf, buf0len, 1, &buffer, &buflen,
			header.bits.type, msgId, 0, MQTTVersion);
	}
#endif
	rc = WebSocket_putdatas(net, &buf, &buf0len, 1, &buffer, &buflen, &freeData);

	if (rc == TCPSOCKET_COMPLETE)
		net->lastSent = MQTTTime_now();
	
	if (rc != TCPSOCKET_INTERRUPTED)
	  free(buf);

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Sends an MQTT packet from multiple buffers in one system call write
 * @param socket the socket to which to write the data
 * @param header the one-byte MQTT header
 * @param count the number of buffers
 * @param buffers the rest of the buffers to write (not including remaining length)
 * @param buflens the lengths of the data in the array of buffers to be written
 * @param the MQTT version being used
 * @return the completion code (TCPSOCKET_COMPLETE etc)
 */
int MQTTPacket_sends(networkHandles* net, Header header, int count, char** buffers, size_t* buflens,
		int* frees, int MQTTVersion)
{
	int i, rc = SOCKET_ERROR;
	size_t buf0len, total = 0;
	char *buf;

	FUNC_ENTRY;
	for (i = 0; i < count; i++)
		total += buflens[i];
	buf0len = 1 + MQTTPacket_encode(NULL, total);
	buf = malloc(buf0len);
	if (buf == NULL)
	{
		rc = SOCKET_ERROR;
		goto exit;
	}
	buf[0] = header.byte;
	MQTTPacket_encode(&buf[1], total);

#if !defined(NO_PERSISTENCE)
	if (header.bits.type == PUBLISH && header.bits.qos != 0)
	{   /* persist PUBLISH QoS1 and Qo2 */
		char *ptraux = buffers[2];
		int msgId = readInt(&ptraux);
		rc = MQTTPersistence_put(net->socket, buf, buf0len, count, buffers, buflens,
			header.bits.type, msgId, 0, MQTTVersion);
	}
#endif
	rc = WebSocket_putdatas(net, &buf, &buf0len, count, buffers, buflens, frees);

	if (rc == TCPSOCKET_COMPLETE)
		net->lastSent = MQTTTime_now();
	
	if (rc != TCPSOCKET_INTERRUPTED)
	  free(buf);
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Encodes the message length according to the MQTT algorithm
 * @param buf the buffer into which the encoded data is written
 * @param length the length to be encoded
 * @return the number of bytes written to buffer
 */
int MQTTPacket_encode(char* buf, size_t length)
{
	int rc = 0;

	FUNC_ENTRY;
	do
	{
		char d = length % 128;
		length /= 128;
		/* if there are more digits to encode, set the top bit of this digit */
		if (length > 0)
			d |= 0x80;
		if (buf)
			buf[rc++] = d;
		else
			rc++;
	} while (length > 0);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Decodes the message length according to the MQTT algorithm
 * @param socket the socket from which to read the bytes
 * @param value the decoded length returned
 * @return the number of bytes read from the socket
 */
int MQTTPacket_decode(networkHandles* net, size_t* value)
{
	int rc = SOCKET_ERROR;
	char c;
	int multiplier = 1;
	int len = 0;
#define MAX_NO_OF_REMAINING_LENGTH_BYTES 4

	FUNC_ENTRY;
	*value = 0;
	do
	{
		if (++len > MAX_NO_OF_REMAINING_LENGTH_BYTES)
		{
			rc = SOCKET_ERROR;	/* bad data */
			goto exit;
		}
		rc = WebSocket_getch(net, &c);
		if (rc != TCPSOCKET_COMPLETE)
				goto exit;
		*value += (c & 127) * multiplier;
		multiplier *= 128;
	} while ((c & 128) != 0);
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Calculates an integer from two bytes read from the input buffer
 * @param pptr pointer to the input buffer - incremented by the number of bytes used & returned
 * @return the integer value calculated
 */
int readInt(char** pptr)
{
	char* ptr = *pptr;
	int len = 256*((unsigned char)(*ptr)) + (unsigned char)(*(ptr+1));
	*pptr += 2;
	return len;
}


/**
 * Reads a "UTF" string from the input buffer.  UTF as in the MQTT v3 spec which really means
 * a length delimited string.  So it reads the two byte length then the data according to
 * that length.  The end of the buffer is provided too, so we can prevent buffer overruns caused
 * by an incorrect length.
 * @param pptr pointer to the input buffer - incremented by the number of bytes used & returned
 * @param enddata pointer to the end of the buffer not to be read beyond
 * @param len returns the calculcated value of the length bytes read
 * @return an allocated C string holding the characters read, or NULL if the length read would
 * have caused an overrun.
 *
 */
static char* readUTFlen(char** pptr, char* enddata, int* len)
{
	char* string = NULL;

	FUNC_ENTRY;
	if (enddata - (*pptr) > 1) /* enough length to read the integer? */
	{
		*len = readInt(pptr);
		if (&(*pptr)[*len] <= enddata)
		{
			if ((string = malloc(*len+1)) == NULL)
				goto exit;
			memcpy(string, *pptr, *len);
			string[*len] = '\0';
			*pptr += *len;
		}
	}
exit:
	FUNC_EXIT;
	return string;
}


/**
 * Reads a "UTF" string from the input buffer.  UTF as in the MQTT v3 spec which really means
 * a length delimited string.  So it reads the two byte length then the data according to
 * that length.  The end of the buffer is provided too, so we can prevent buffer overruns caused
 * by an incorrect length.
 * @param pptr pointer to the input buffer - incremented by the number of bytes used & returned
 * @param enddata pointer to the end of the buffer not to be read beyond
 * @return an allocated C string holding the characters read, or NULL if the length read would
 * have caused an overrun.
 */
char* readUTF(char** pptr, char* enddata)
{
	int len;
	return readUTFlen(pptr, enddata, &len);
}


/**
 * Reads one character from the input buffer.
 * @param pptr pointer to the input buffer - incremented by the number of bytes used & returned
 * @return the character read
 */
unsigned char readChar(char** pptr)
{
	unsigned char c = **pptr;
	(*pptr)++;
	return c;
}


/**
 * Writes one character to an output buffer.
 * @param pptr pointer to the output buffer - incremented by the number of bytes used & returned
 * @param c the character to write
 */
void writeChar(char** pptr, char c)
{
	**pptr = c;
	(*pptr)++;
}


/**
 * Writes an integer as 2 bytes to an output buffer.
 * @param pptr pointer to the output buffer - incremented by the number of bytes used & returned
 * @param anInt the integer to write
 */
void writeInt(char** pptr, int anInt)
{
	**pptr = (char)(anInt / 256);
	(*pptr)++;
	**pptr = (char)(anInt % 256);
	(*pptr)++;
}


/**
 * Writes a "UTF" string to an output buffer.  Converts C string to length-delimited.
 * @param pptr pointer to the output buffer - incremented by the number of bytes used & returned
 * @param string the C string to write
 */
void writeUTF(char** pptr, const char* string)
{
	size_t len = strlen(string);
	writeInt(pptr, (int)len);
	memcpy(*pptr, string, len);
	*pptr += len;
}


/**
 * Writes length delimited data to an output buffer
 * @param pptr pointer to the output buffer - incremented by the number of bytes used & returned
 * @param data the data to write
 * @param datalen the length of the data to write
 */
void writeData(char** pptr, const void* data, int datalen)
{
	writeInt(pptr, datalen);
	memcpy(*pptr, data, datalen);
	*pptr += datalen;
}


/**
 * Function used in the new packets table to create packets which have only a header.
 * @param MQTTVersion the version of MQTT
 * @param aHeader the MQTT header byte
 * @param data the rest of the packet
 * @param datalen the length of the rest of the packet
 * @return pointer to the packet structure
 */
void* MQTTPacket_header_only(int MQTTVersion, unsigned char aHeader, char* data, size_t datalen)
{
	static unsigned char header = 0;
	header = aHeader;
	return &header;
}


/**
 * Send an MQTT disconnect packet down a socket.
 * @param socket the open socket to send the data to
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_disconnect(Clients* client, enum MQTTReasonCodes reason, MQTTProperties* props)
{
	Header header;
	int rc = 0;

	FUNC_ENTRY;
	header.byte = 0;
	header.bits.type = DISCONNECT;

	if (client->MQTTVersion >= 5 && (props || reason != MQTTREASONCODE_SUCCESS))
	{
		size_t buflen = 1 + ((props == NULL) ? 0 : MQTTProperties_len(props));
		char *buf = NULL;
		char *ptr = NULL;

		if ((buf = malloc(buflen)) == NULL)
		{
			rc = SOCKET_ERROR;
			goto exit;
		}
		ptr = buf;
		writeChar(&ptr, reason);
		if (props)
			MQTTProperties_write(&ptr, props);
		if ((rc = MQTTPacket_send(&client->net, header, buf, buflen, 1,
				                   client->MQTTVersion)) != TCPSOCKET_INTERRUPTED)
			free(buf);
	}
	else
		rc = MQTTPacket_send(&client->net, header, NULL, 0, 0, client->MQTTVersion);
exit:
	Log(LOG_PROTOCOL, 28, NULL, client->net.socket, client->clientID, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Function used in the new packets table to create publish packets.
 * @param MQTTVersion
 * @param aHeader the MQTT header byte
 * @param data the rest of the packet
 * @param datalen the length of the rest of the packet
 * @return pointer to the packet structure
 */
void* MQTTPacket_publish(int MQTTVersion, unsigned char aHeader, char* data, size_t datalen)
{
	Publish* pack = NULL;
	char* curdata = data;
	char* enddata = &data[datalen];

	FUNC_ENTRY;
	if ((pack = malloc(sizeof(Publish))) == NULL)
		goto exit;
	memset(pack, '\0', sizeof(Publish));
	pack->MQTTVersion = MQTTVersion;
	pack->header.byte = aHeader;
	if ((pack->topic = readUTFlen(&curdata, enddata, &pack->topiclen)) == NULL) /* Topic name on which to publish */
	{
		free(pack);
		pack = NULL;
		goto exit;
	}
	if (pack->header.bits.qos > 0)  /* Msgid only exists for QoS 1 or 2 */
		pack->msgId = readInt(&curdata);
	else
		pack->msgId = 0;
	if (MQTTVersion >= MQTTVERSION_5)
	{
		MQTTProperties props = MQTTProperties_initializer;
		pack->properties = props;
		if (MQTTProperties_read(&pack->properties, &curdata, enddata) != 1)
		{
			if (pack->properties.array)
				free(pack->properties.array);
			if (pack)
				free(pack);
			pack = NULL; /* signal protocol error */
			goto exit;
		}
	}
	pack->payload = curdata;
	pack->payloadlen = (int)(datalen-(curdata-data));
exit:
	FUNC_EXIT;
	return pack;
}


/**
 * Free allocated storage for a publish packet.
 * @param pack pointer to the publish packet structure
 */
void MQTTPacket_freePublish(Publish* pack)
{
	FUNC_ENTRY;
	if (pack->topic != NULL)
		free(pack->topic);
	if (pack->MQTTVersion >= MQTTVERSION_5)
		MQTTProperties_free(&pack->properties);
	free(pack);
	FUNC_EXIT;
}


/**
 * Free allocated storage for an ack packet.
 * @param pack pointer to the publish packet structure
 */
void MQTTPacket_freeAck(Ack* pack)
{
	FUNC_ENTRY;
	if (pack->MQTTVersion >= MQTTVERSION_5)
		MQTTProperties_free(&pack->properties);
	free(pack);
	FUNC_EXIT;
}


/**
 * Send an MQTT acknowledgement packet down a socket.
 * @param MQTTVersion the version of MQTT being used
 * @param type the MQTT packet type e.g. SUBACK
 * @param msgid the MQTT message id to use
 * @param dup boolean - whether to set the MQTT DUP flag
 * @param net the network handle to send the data to
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
static int MQTTPacket_send_ack(int MQTTVersion, int type, int msgid, int dup, networkHandles *net)
{
	Header header;
	int rc = SOCKET_ERROR;
	char *buf = NULL;
	char *ptr = NULL;

	FUNC_ENTRY;
	if ((ptr = buf = malloc(2)) == NULL)
		goto exit;
	header.byte = 0;
	header.bits.type = type;
	header.bits.dup = dup;
	if (type == PUBREL)
	    header.bits.qos = 1;
	writeInt(&ptr, msgid);
	if ((rc = MQTTPacket_send(net, header, buf, 2, 1, MQTTVersion)) != TCPSOCKET_INTERRUPTED)
		free(buf);
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Send an MQTT PUBACK packet down a socket.
 * @param MQTTVersion the version of MQTT being used
 * @param msgid the MQTT message id to use
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_puback(int MQTTVersion, int msgid, networkHandles* net, const char* clientID)
{
	int rc = 0;

	FUNC_ENTRY;
	rc =  MQTTPacket_send_ack(MQTTVersion, PUBACK, msgid, 0, net);
	Log(LOG_PROTOCOL, 12, NULL, net->socket, clientID, msgid, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Free allocated storage for a suback packet.
 * @param pack pointer to the suback packet structure
 */
void MQTTPacket_freeSuback(Suback* pack)
{
	FUNC_ENTRY;
	if (pack->MQTTVersion >= MQTTVERSION_5)
		MQTTProperties_free(&pack->properties);
	if (pack->qoss != NULL)
		ListFree(pack->qoss);
	free(pack);
	FUNC_EXIT;
}


/**
 * Free allocated storage for a suback packet.
 * @param pack pointer to the suback packet structure
 */
void MQTTPacket_freeUnsuback(Unsuback* pack)
{
	FUNC_ENTRY;
	if (pack->MQTTVersion >= MQTTVERSION_5)
	{
		MQTTProperties_free(&pack->properties);
		if (pack->reasonCodes != NULL)
			ListFree(pack->reasonCodes);
	}
	free(pack);
	FUNC_EXIT;
}


/**
 * Send an MQTT PUBREC packet down a socket.
 * @param MQTTVersion the version of MQTT being used
 * @param msgid the MQTT message id to use
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_pubrec(int MQTTVersion, int msgid, networkHandles* net, const char* clientID)
{
	int rc = 0;

	FUNC_ENTRY;
	rc =  MQTTPacket_send_ack(MQTTVersion, PUBREC, msgid, 0, net);
	Log(LOG_PROTOCOL, 13, NULL, net->socket, clientID, msgid, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Send an MQTT PUBREL packet down a socket.
 * @param MQTTVersion the version of MQTT being used
 * @param msgid the MQTT message id to use
 * @param dup boolean - whether to set the MQTT DUP flag
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_pubrel(int MQTTVersion, int msgid, int dup, networkHandles* net, const char* clientID)
{
	int rc = 0;

	FUNC_ENTRY;
	rc = MQTTPacket_send_ack(MQTTVersion, PUBREL, msgid, dup, net);
	Log(LOG_PROTOCOL, 16, NULL, net->socket, clientID, msgid, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Send an MQTT PUBCOMP packet down a socket.
 * @param MQTTVersion the version of MQTT being used
 * @param msgid the MQTT message id to use
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_pubcomp(int MQTTVersion, int msgid, networkHandles* net, const char* clientID)
{
	int rc = 0;

	FUNC_ENTRY;
	rc = MQTTPacket_send_ack(MQTTVersion, PUBCOMP, msgid, 0, net);
	Log(LOG_PROTOCOL, 18, NULL, net->socket, clientID, msgid, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Function used in the new packets table to create acknowledgement packets.
 * @param MQTTVersion the version of MQTT being used
 * @param aHeader the MQTT header byte
 * @param data the rest of the packet
 * @param datalen the length of the rest of the packet
 * @return pointer to the packet structure
 */
void* MQTTPacket_ack(int MQTTVersion, unsigned char aHeader, char* data, size_t datalen)
{
	Ack* pack = NULL;
	char* curdata = data;
	char* enddata = &data[datalen];

	FUNC_ENTRY;
	if ((pack = malloc(sizeof(Ack))) == NULL)
		goto exit;
	pack->MQTTVersion = MQTTVersion;
	pack->header.byte = aHeader;
	if (pack->header.bits.type != DISCONNECT)
		pack->msgId = readInt(&curdata);
	if (MQTTVersion >= MQTTVERSION_5)
	{
		MQTTProperties props = MQTTProperties_initializer;

		pack->rc = MQTTREASONCODE_SUCCESS;
		pack->properties = props;

		if (datalen > 2)
			pack->rc = readChar(&curdata); /* reason code */

		if (datalen > 3)
		{
			if (MQTTProperties_read(&pack->properties, &curdata, enddata) != 1)
			{
				if (pack->properties.array)
					free(pack->properties.array);
				if (pack)
					free(pack);
				pack = NULL; /* signal protocol error */
				goto exit;
			}
		}
	}
exit:
	FUNC_EXIT;
	return pack;
}


/**
 * Send an MQTT PUBLISH packet down a socket.
 * @param pack a structure from which to get some values to use, e.g topic, payload
 * @param dup boolean - whether to set the MQTT DUP flag
 * @param qos the value to use for the MQTT QoS setting
 * @param retained boolean - whether to set the MQTT retained flag
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_publish(Publish* pack, int dup, int qos, int retained, networkHandles* net, const char* clientID)
{
	Header header;
	char *topiclen;
	int rc = SOCKET_ERROR;

	FUNC_ENTRY;
	topiclen = malloc(2);
	if (topiclen == NULL)
		goto exit;

	header.bits.type = PUBLISH;
	header.bits.dup = dup;
	header.bits.qos = qos;
	header.bits.retain = retained;
	if (qos > 0 || pack->MQTTVersion >= 5)
	{
		int buflen = ((qos > 0) ? 2 : 0) + ((pack->MQTTVersion >= 5) ? MQTTProperties_len(&pack->properties) : 0);
		char *ptr = NULL;
		char* bufs[4] = {topiclen, pack->topic, NULL, pack->payload};
		size_t lens[4] = {2, strlen(pack->topic), buflen, pack->payloadlen};
		int frees[4] = {1, 0, 1, 0};

		bufs[2] = ptr = malloc(buflen);
		if (ptr == NULL)
			goto exit_free;
		if (qos > 0)
			writeInt(&ptr, pack->msgId);
		if (pack->MQTTVersion >= 5)
			MQTTProperties_write(&ptr, &pack->properties);

		ptr = topiclen;
		writeInt(&ptr, (int)lens[1]);
		rc = MQTTPacket_sends(net, header, 4, bufs, lens, frees, pack->MQTTVersion);
		if (rc != TCPSOCKET_INTERRUPTED)
			free(bufs[2]);
	}
	else
	{
		char* ptr = topiclen;
		char* bufs[3] = {topiclen, pack->topic, pack->payload};
		size_t lens[3] = {2, strlen(pack->topic), pack->payloadlen};
		int frees[3] = {1, 0, 0};

		writeInt(&ptr, (int)lens[1]);
		rc = MQTTPacket_sends(net, header, 3, bufs, lens, frees, pack->MQTTVersion);
	}
	if (qos == 0)
		Log(LOG_PROTOCOL, 27, NULL, net->socket, clientID, retained, rc, pack->payloadlen,
				min(20, pack->payloadlen), pack->payload);
	else
		Log(LOG_PROTOCOL, 10, NULL, net->socket, clientID, pack->msgId, qos, retained, rc, pack->payloadlen,
				min(20, pack->payloadlen), pack->payload);
exit_free:
	if (rc != TCPSOCKET_INTERRUPTED)
		free(topiclen);
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Free allocated storage for a various packet tyoes
 * @param pack pointer to the suback packet structure
 */
void MQTTPacket_free_packet(MQTTPacket* pack)
{
	FUNC_ENTRY;
	if (pack->header.bits.type == PUBLISH)
		MQTTPacket_freePublish((Publish*)pack);
	/*else if (pack->header.type == SUBSCRIBE)
		MQTTPacket_freeSubscribe((Subscribe*)pack, 1);
	else if (pack->header.type == UNSUBSCRIBE)
		MQTTPacket_freeUnsubscribe((Unsubscribe*)pack);*/
	else
		free(pack);
	FUNC_EXIT;
}


/**
 * Writes an integer as 4 bytes to an output buffer.
 * @param pptr pointer to the output buffer - incremented by the number of bytes used & returned
 * @param anInt the integer to write
 */
void writeInt4(char** pptr, int anInt)
{
  **pptr = (char)(anInt / 16777216);
  (*pptr)++;
  anInt %= 16777216;
  **pptr = (char)(anInt / 65536);
  (*pptr)++;
  anInt %= 65536;
	**pptr = (char)(anInt / 256);
	(*pptr)++;
	**pptr = (char)(anInt % 256);
	(*pptr)++;
}


/**
 * Calculates an integer from two bytes read from the input buffer
 * @param pptr pointer to the input buffer - incremented by the number of bytes used & returned
 * @return the integer value calculated
 */
int readInt4(char** pptr)
{
	unsigned char* ptr = (unsigned char*)*pptr;
	int value = 16777216*(*ptr) + 65536*(*(ptr+1)) + 256*(*(ptr+2)) + (*(ptr+3));
	*pptr += 4;
	return value;
}


void writeMQTTLenString(char** pptr, MQTTLenString lenstring)
{
  writeInt(pptr, lenstring.len);
  memcpy(*pptr, lenstring.data, lenstring.len);
  *pptr += lenstring.len;
}


int MQTTLenStringRead(MQTTLenString* lenstring, char** pptr, char* enddata)
{
	int len = 0;

	/* the first two bytes are the length of the string */
	if (enddata - (*pptr) > 1) /* enough length to read the integer? */
	{
		lenstring->len = readInt(pptr); /* increments pptr to point past length */
		if (&(*pptr)[lenstring->len] <= enddata)
		{
			lenstring->data = (char*)*pptr;
			*pptr += lenstring->len;
			len = 2 + lenstring->len;
		}
	}
	return len;
}

/*
if (prop->value.integer4 >= 0 && prop->value.integer4 <= 127)
  len = 1;
else if (prop->value.integer4 >= 128 && prop->value.integer4 <= 16383)
  len = 2;
else if (prop->value.integer4 >= 16384 && prop->value.integer4 < 2097151)
  len = 3;
else if (prop->value.integer4 >= 2097152 && prop->value.integer4 < 268435455)
  len = 4;
*/
int MQTTPacket_VBIlen(int rem_len)
{
	int rc = 0;

	if (rem_len < 128)
		rc = 1;
	else if (rem_len < 16384)
		rc = 2;
	else if (rem_len < 2097152)
		rc = 3;
	else
		rc = 4;
  return rc;
}


/**
 * Decodes the message length according to the MQTT algorithm
 * @param getcharfn pointer to function to read the next character from the data source
 * @param value the decoded length returned
 * @return the number of bytes read from the socket
 */
int MQTTPacket_VBIdecode(int (*getcharfn)(char*, int), unsigned int* value)
{
	char c;
	int multiplier = 1;
	int len = 0;
#define MAX_NO_OF_REMAINING_LENGTH_BYTES 4

	*value = 0;
	do
	{
		int rc = MQTTPACKET_READ_ERROR;

		if (++len > MAX_NO_OF_REMAINING_LENGTH_BYTES)
		{
			rc = MQTTPACKET_READ_ERROR;	/* bad data */
			goto exit;
		}
		rc = (*getcharfn)(&c, 1);
		if (rc != 1)
			goto exit;
		*value += (c & 127) * multiplier;
		multiplier *= 128;
	} while ((c & 128) != 0);
exit:
	return len;
}


static char* bufptr;

int bufchar(char* c, int count)
{
	int i;

	for (i = 0; i < count; ++i)
		*c = *bufptr++;
	return count;
}


int MQTTPacket_decodeBuf(char* buf, unsigned int* value)
{
	bufptr = buf;
	return MQTTPacket_VBIdecode(bufchar, value);
}

