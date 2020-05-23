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
 *    Rong Xiang, Ian Craggs - C++ compatibility
 *    Ian Craggs - binary password and will payload
 *    Ian Craggs - MQTT 5.0 support
 *******************************************************************************/

/**
 * @file
 * \brief functions to deal with reading and writing of MQTT packets from and to sockets
 *
 * Some other related functions are in the MQTTPacket module
 */


#include "MQTTPacketOut.h"
#include "Log.h"
#include "StackTrace.h"

#include <string.h>
#include <stdlib.h>

#include "Heap.h"


/**
 * Send an MQTT CONNECT packet down a socket for V5 or later
 * @param client a structure from which to get all the required values
 * @param MQTTVersion the MQTT version to connect with
 * @param connectProperties MQTT V5 properties for the connect packet
 * @param willProperties MQTT V5 properties for the will message, if any
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_connect(Clients* client, int MQTTVersion,
	               MQTTProperties* connectProperties, MQTTProperties* willProperties)
{
	char *buf, *ptr;
	Connect packet;
	int rc = SOCKET_ERROR, len;

	FUNC_ENTRY;
	packet.header.byte = 0;
	packet.header.bits.type = CONNECT;

	len = ((MQTTVersion == MQTTVERSION_3_1) ? 12 : 10) + (int)strlen(client->clientID)+2;
	if (client->will)
		len += (int)strlen(client->will->topic)+2 + client->will->payloadlen+2;
	if (client->username)
		len += (int)strlen(client->username)+2;
	if (client->password)
		len += client->passwordlen+2;
	if (MQTTVersion >= MQTTVERSION_5)
	{
		len += MQTTProperties_len(connectProperties);
		if (client->will)
			len += MQTTProperties_len(willProperties);
	}

	ptr = buf = malloc(len);
	if (ptr == NULL)
		goto exit_nofree;
	if (MQTTVersion == MQTTVERSION_3_1)
	{
		writeUTF(&ptr, "MQIsdp");
		writeChar(&ptr, (char)MQTTVERSION_3_1);
	}
	else if (MQTTVersion == MQTTVERSION_3_1_1 || MQTTVersion == MQTTVERSION_5)
	{
		writeUTF(&ptr, "MQTT");
		writeChar(&ptr, (char)MQTTVersion);
	}
	else
		goto exit;

	packet.flags.all = 0;
	if (MQTTVersion >= MQTTVERSION_5)
		packet.flags.bits.cleanstart = client->cleanstart;
	else
		packet.flags.bits.cleanstart = client->cleansession;
	packet.flags.bits.will = (client->will) ? 1 : 0;
	if (packet.flags.bits.will)
	{
		packet.flags.bits.willQoS = client->will->qos;
		packet.flags.bits.willRetain = client->will->retained;
	}
	if (client->username)
		packet.flags.bits.username = 1;
	if (client->password)
		packet.flags.bits.password = 1;

	writeChar(&ptr, packet.flags.all);
	writeInt(&ptr, client->keepAliveInterval);
	if (MQTTVersion >= MQTTVERSION_5)
		MQTTProperties_write(&ptr, connectProperties);
	writeUTF(&ptr, client->clientID);
	if (client->will)
	{
		if (MQTTVersion >= MQTTVERSION_5)
			MQTTProperties_write(&ptr, willProperties);
		writeUTF(&ptr, client->will->topic);
		writeData(&ptr, client->will->payload, client->will->payloadlen);
	}
	if (client->username)
		writeUTF(&ptr, client->username);
	if (client->password)
		writeData(&ptr, client->password, client->passwordlen);

	rc = MQTTPacket_send(&client->net, packet.header, buf, len, 1, MQTTVersion);
	Log(LOG_PROTOCOL, 0, NULL, client->net.socket, client->clientID,
			MQTTVersion, client->cleansession, rc);
exit:
	if (rc != TCPSOCKET_INTERRUPTED)
		free(buf);
exit_nofree:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Function used in the new packets table to create connack packets.
 * @param MQTTVersion MQTT 5 or less?
 * @param aHeader the MQTT header byte
 * @param data the rest of the packet
 * @param datalen the length of the rest of the packet
 * @return pointer to the packet structure
 */
void* MQTTPacket_connack(int MQTTVersion, unsigned char aHeader, char* data, size_t datalen)
{
	Connack* pack = NULL;
	char* curdata = data;
	char* enddata = &data[datalen];

	FUNC_ENTRY;
	if ((pack = malloc(sizeof(Connack))) == NULL)
		goto exit;
	pack->MQTTVersion = MQTTVersion;
	pack->header.byte = aHeader;
	pack->flags.all = readChar(&curdata); /* connect flags */
	pack->rc = readChar(&curdata); /* reason code */
	if (MQTTVersion < MQTTVERSION_5)
	{
		if (datalen != 2)
		{
			free(pack);
			pack = NULL;
		}
	}
	else if (datalen > 2)
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
exit:
	FUNC_EXIT;
	return pack;
}


/**
 * Free allocated storage for a connack packet.
 * @param pack pointer to the connack packet structure
 */
void MQTTPacket_freeConnack(Connack* pack)
{
	FUNC_ENTRY;
	if (pack->MQTTVersion >= MQTTVERSION_5)
		MQTTProperties_free(&pack->properties);
	free(pack);
	FUNC_EXIT;
}


/**
 * Send an MQTT PINGREQ packet down a socket.
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_pingreq(networkHandles* net, const char* clientID)
{
	Header header;
	int rc = 0;

	FUNC_ENTRY;
	header.byte = 0;
	header.bits.type = PINGREQ;
	rc = MQTTPacket_send(net, header, NULL, 0, 0, MQTTVERSION_3_1_1);
	Log(LOG_PROTOCOL, 20, NULL, net->socket, clientID, rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Send an MQTT subscribe packet down a socket.
 * @param topics list of topics
 * @param qoss list of corresponding QoSs
 * @param msgid the MQTT message id to use
 * @param dup boolean - whether to set the MQTT DUP flag
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_subscribe(List* topics, List* qoss, MQTTSubscribe_options* opts, MQTTProperties* props,
		int msgid, int dup, Clients* client)
{
	Header header;
	char *data, *ptr;
	int rc = -1;
	ListElement *elem = NULL, *qosElem = NULL;
	int datalen, i = 0;

	FUNC_ENTRY;
	header.bits.type = SUBSCRIBE;
	header.bits.dup = dup;
	header.bits.qos = 1;
	header.bits.retain = 0;

	datalen = 2 + topics->count * 3; /* utf length + char qos == 3 */
	while (ListNextElement(topics, &elem))
		datalen += (int)strlen((char*)(elem->content));
	if (client->MQTTVersion >= MQTTVERSION_5)
		datalen += MQTTProperties_len(props);

	ptr = data = malloc(datalen);
	if (ptr == NULL)
		goto exit;
	writeInt(&ptr, msgid);

	if (client->MQTTVersion >= MQTTVERSION_5)
		MQTTProperties_write(&ptr, props);

	elem = NULL;
	while (ListNextElement(topics, &elem))
	{
		char subopts = 0;

		ListNextElement(qoss, &qosElem);
		writeUTF(&ptr, (char*)(elem->content));
		subopts = *(int*)(qosElem->content);
		if (client->MQTTVersion >= MQTTVERSION_5 && opts != NULL)
		{
			subopts |= (opts[i].noLocal << 2); /* 1 bit */
			subopts |= (opts[i].retainAsPublished << 3); /* 1 bit */
			subopts |= (opts[i].retainHandling << 4); /* 2 bits */
		}
		writeChar(&ptr, subopts);
		++i;
	}
	rc = MQTTPacket_send(&client->net, header, data, datalen, 1, client->MQTTVersion);
	Log(LOG_PROTOCOL, 22, NULL, client->net.socket, client->clientID, msgid, rc);
	if (rc != TCPSOCKET_INTERRUPTED)
		free(data);
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Function used in the new packets table to create suback packets.
 * @param MQTTVersion the version of MQTT
 * @param aHeader the MQTT header byte
 * @param data the rest of the packet
 * @param datalen the length of the rest of the packet
 * @return pointer to the packet structure
 */
void* MQTTPacket_suback(int MQTTVersion, unsigned char aHeader, char* data, size_t datalen)
{
	Suback* pack = NULL;
	char* curdata = data;
	char* enddata = &data[datalen];

	FUNC_ENTRY;
	if ((pack = malloc(sizeof(Suback))) == NULL)
		goto exit;
	pack->MQTTVersion = MQTTVersion;
	pack->header.byte = aHeader;
	pack->msgId = readInt(&curdata);
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
	pack->qoss = ListInitialize();
	while ((size_t)(curdata - data) < datalen)
	{
		unsigned int* newint;
		newint = malloc(sizeof(unsigned int));
		if (newint == NULL)
		{
			if (pack->properties.array)
				free(pack->properties.array);
			if (pack)
				free(pack);
			pack = NULL; /* signal protocol error */
			goto exit;
		}
		*newint = (unsigned int)readChar(&curdata);
		ListAppend(pack->qoss, newint, sizeof(unsigned int));
	}
	if (pack->qoss->count == 0)
	{
		if (pack->properties.array)
			free(pack->properties.array);
		if (pack)
			free(pack);
		ListFree(pack->qoss);
		pack = NULL;
	}
exit:
	FUNC_EXIT;
	return pack;
}


/**
 * Send an MQTT unsubscribe packet down a socket.
 * @param topics list of topics
 * @param msgid the MQTT message id to use
 * @param dup boolean - whether to set the MQTT DUP flag
 * @param socket the open socket to send the data to
 * @param clientID the string client identifier, only used for tracing
 * @return the completion code (e.g. TCPSOCKET_COMPLETE)
 */
int MQTTPacket_send_unsubscribe(List* topics, MQTTProperties* props, int msgid, int dup, Clients* client)
{
	Header header;
	char *data, *ptr;
	int rc = SOCKET_ERROR;
	ListElement *elem = NULL;
	int datalen;

	FUNC_ENTRY;
	header.bits.type = UNSUBSCRIBE;
	header.bits.dup = dup;
	header.bits.qos = 1;
	header.bits.retain = 0;

	datalen = 2 + topics->count * 2; /* utf length == 2 */
	while (ListNextElement(topics, &elem))
		datalen += (int)strlen((char*)(elem->content));
	if (client->MQTTVersion >= MQTTVERSION_5)
		datalen += MQTTProperties_len(props);
	ptr = data = malloc(datalen);
	if (ptr == NULL)
		goto exit;

	writeInt(&ptr, msgid);

	if (client->MQTTVersion >= MQTTVERSION_5)
		MQTTProperties_write(&ptr, props);

	elem = NULL;
	while (ListNextElement(topics, &elem))
		writeUTF(&ptr, (char*)(elem->content));
	rc = MQTTPacket_send(&client->net, header, data, datalen, 1, client->MQTTVersion);
	Log(LOG_PROTOCOL, 25, NULL, client->net.socket, client->clientID, msgid, rc);
	if (rc != TCPSOCKET_INTERRUPTED)
		free(data);
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Function used in the new packets table to create unsuback packets.
 * @param MQTTVersion the version of MQTT
 * @param aHeader the MQTT header byte
 * @param data the rest of the packet
 * @param datalen the length of the rest of the packet
 * @return pointer to the packet structure
 */
void* MQTTPacket_unsuback(int MQTTVersion, unsigned char aHeader, char* data, size_t datalen)
{
	Unsuback* pack = NULL;
	char* curdata = data;
	char* enddata = &data[datalen];

	FUNC_ENTRY;
	if ((pack = malloc(sizeof(Unsuback))) == NULL)
		goto exit;
	pack->MQTTVersion = MQTTVersion;
	pack->header.byte = aHeader;
	pack->msgId = readInt(&curdata);
	pack->reasonCodes = NULL;
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
		pack->reasonCodes = ListInitialize();
		while ((size_t)(curdata - data) < datalen)
		{
			enum MQTTReasonCodes* newrc;
			newrc = malloc(sizeof(enum MQTTReasonCodes));
			if (newrc == NULL)
			{
				if (pack->properties.array)
					free(pack->properties.array);
				if (pack)
					free(pack);
				pack = NULL; /* signal protocol error */
				goto exit;
			}
			*newrc = (enum MQTTReasonCodes)readChar(&curdata);
			ListAppend(pack->reasonCodes, newrc, sizeof(enum MQTTReasonCodes));
		}
		if (pack->reasonCodes->count == 0)
		{
			ListFree(pack->reasonCodes);
			if (pack->properties.array)
				free(pack->properties.array);
			if (pack)
				free(pack);
			pack = NULL;
		}
	}
exit:
	FUNC_EXIT;
	return pack;
}
