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
 *    Ian Craggs - fix for bug 413429 - connectionLost not called
 *    Ian Craggs - fix for bug 421103 - trying to write to same socket, in retry
 *    Rong Xiang, Ian Craggs - C++ compatibility
 *    Ian Craggs - turn off DUP flag for PUBREL - MQTT 3.1.1
 *    Ian Craggs - ensure that acks are not sent if write is outstanding on socket
 *    Ian Craggs - MQTT 5.0 support
 *******************************************************************************/

/**
 * @file
 * \brief Functions dealing with the MQTT protocol exchanges
 *
 * Some other related functions are in the MQTTProtocolOut module
 * */


#include <stdlib.h>
#include <string.h>

#include "MQTTProtocolClient.h"
#if !defined(NO_PERSISTENCE)
#include "MQTTPersistence.h"
#endif
#include "SocketBuffer.h"
#include "StackTrace.h"
#include "Heap.h"

#if !defined(min)
#define min(A,B) ( (A) < (B) ? (A):(B))
#endif

extern MQTTProtocol state;
extern ClientStates* bstate;

static void MQTTProtocol_storeQoS0(Clients* pubclient, Publish* publish);
static int MQTTProtocol_startPublishCommon(
		Clients* pubclient,
		Publish* publish,
		int qos,
		int retained);
static void MQTTProtocol_retries(START_TIME_TYPE now, Clients* client, int regardless);


/**
 * List callback function for comparing Message structures by message id
 * @param a first integer value
 * @param b second integer value
 * @return boolean indicating whether a and b are equal
 */
int messageIDCompare(void* a, void* b)
{
	Messages* msg = (Messages*)a;
	return msg->msgid == *(int*)b;
}


/**
 * Assign a new message id for a client.  Make sure it isn't already being used and does
 * not exceed the maximum.
 * @param client a client structure
 * @return the next message id to use, or 0 if none available
 */
int MQTTProtocol_assignMsgId(Clients* client)
{
	int start_msgid = client->msgID;
	int msgid = start_msgid;

	FUNC_ENTRY;
	msgid = (msgid == MAX_MSG_ID) ? 1 : msgid + 1;
	while (ListFindItem(client->outboundMsgs, &msgid, messageIDCompare) != NULL)
	{
		msgid = (msgid == MAX_MSG_ID) ? 1 : msgid + 1;
		if (msgid == start_msgid)
		{ /* we've tried them all - none free */
			msgid = 0;
			break;
		}
	}
	if (msgid != 0)
		client->msgID = msgid;
	FUNC_EXIT_RC(msgid);
	return msgid;
}


static void MQTTProtocol_storeQoS0(Clients* pubclient, Publish* publish)
{
	int len;
	pending_write* pw = NULL;

	FUNC_ENTRY;
	/* store the publication until the write is finished */
	if ((pw = malloc(sizeof(pending_write))) == NULL)
		goto exit;
	Log(TRACE_MIN, 12, NULL);
	if ((pw->p = MQTTProtocol_storePublication(publish, &len)) == NULL)
	{
		free(pw);
		goto exit;
	}
	pw->socket = pubclient->net.socket;
	if (!ListAppend(&(state.pending_writes), pw, sizeof(pending_write)+len))
	{
		free(pw->p);
		free(pw);
		goto exit;
	}
	/* we don't copy QoS 0 messages unless we have to, so now we have to tell the socket buffer where
	the saved copy is */
	if (SocketBuffer_updateWrite(pw->socket, pw->p->topic, pw->p->payload) == NULL)
		Log(LOG_SEVERE, 0, "Error updating write");
exit:
	FUNC_EXIT;
}


/**
 * Utility function to start a new publish exchange.
 * @param pubclient the client to send the publication to
 * @param publish the publication data
 * @param qos the MQTT QoS to use
 * @param retained boolean - whether to set the MQTT retained flag
 * @return the completion code
 */
static int MQTTProtocol_startPublishCommon(Clients* pubclient, Publish* publish, int qos, int retained)
{
	int rc = TCPSOCKET_COMPLETE;

	FUNC_ENTRY;
	rc = MQTTPacket_send_publish(publish, 0, qos, retained, &pubclient->net, pubclient->clientID);
	if (qos == 0 && rc == TCPSOCKET_INTERRUPTED)
		MQTTProtocol_storeQoS0(pubclient, publish);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Start a new publish exchange.  Store any state necessary and try to send the packet
 * @param pubclient the client to send the publication to
 * @param publish the publication data
 * @param qos the MQTT QoS to use
 * @param retained boolean - whether to set the MQTT retained flag
 * @param mm - pointer to the message to send
 * @return the completion code
 */
int MQTTProtocol_startPublish(Clients* pubclient, Publish* publish, int qos, int retained, Messages** mm)
{
	Publish p = *publish;
	int rc = 0;

	FUNC_ENTRY;
	if (qos > 0)
	{
		*mm = MQTTProtocol_createMessage(publish, mm, qos, retained, 0);
		ListAppend(pubclient->outboundMsgs, *mm, (*mm)->len);
		/* we change these pointers to the saved message location just in case the packet could not be written
		entirely; the socket buffer will use these locations to finish writing the packet */
		p.payload = (*mm)->publish->payload;
		p.topic = (*mm)->publish->topic;
		p.properties = (*mm)->properties;
		p.MQTTVersion = (*mm)->MQTTVersion;
	}
	rc = MQTTProtocol_startPublishCommon(pubclient, &p, qos, retained);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Copy and store message data for retries
 * @param publish the publication data
 * @param mm - pointer to the message data to store
 * @param qos the MQTT QoS to use
 * @param retained boolean - whether to set the MQTT retained flag
 * @param allocatePayload boolean - whether or not to malloc payload
 * @return pointer to the message data stored
 */
Messages* MQTTProtocol_createMessage(Publish* publish, Messages **mm, int qos, int retained, int allocatePayload)
{
	Messages* m = malloc(sizeof(Messages));

	FUNC_ENTRY;
	if (!m)
		goto exit;
	m->len = sizeof(Messages);
	if (*mm == NULL || (*mm)->publish == NULL)
	{
		int len1;
		*mm = m;
		if ((m->publish = MQTTProtocol_storePublication(publish, &len1)) == NULL)
		{
			free(m);
			goto exit;
		}
		m->len += len1;
		if (allocatePayload)
		{
			char *temp = m->publish->payload;

			if ((m->publish->payload = malloc(m->publish->payloadlen)) == NULL)
			{
				free(m);
				goto exit;
			}
			memcpy(m->publish->payload, temp, m->publish->payloadlen);
		}
	}
	else /* this is now never used, I think */
	{
		++(((*mm)->publish)->refcount);
		m->publish = (*mm)->publish;
	}
	m->msgid = publish->msgId;
	m->qos = qos;
	m->retain = retained;
	m->MQTTVersion = publish->MQTTVersion;
	if (m->MQTTVersion >= 5)
		m->properties = MQTTProperties_copy(&publish->properties);
	m->lastTouch = MQTTTime_now();
	if (qos == 2)
		m->nextMessageType = PUBREC;
exit:
	FUNC_EXIT;
	return m;
}


/**
 * Store message data for possible retry
 * @param publish the publication data
 * @param len returned length of the data stored
 * @return the publication stored
 */
Publications* MQTTProtocol_storePublication(Publish* publish, int* len)
{
	Publications* p = malloc(sizeof(Publications));

	FUNC_ENTRY;
	if (!p)
		goto exit;
	p->refcount = 1;
	*len = (int)strlen(publish->topic)+1;
	p->topic = publish->topic;
	publish->topic = NULL;
	*len += sizeof(Publications);
	p->topiclen = publish->topiclen;
	p->payloadlen = publish->payloadlen;
	p->payload = publish->payload;
	publish->payload = NULL;
	*len += publish->payloadlen;

	if ((ListAppend(&(state.publications), p, *len)) == NULL)
	{
		free(p);
		p = NULL;
	}
exit:
	FUNC_EXIT;
	return p;
}

/**
 * Remove stored message data.  Opposite of storePublication
 * @param p stored publication to remove
 */
void MQTTProtocol_removePublication(Publications* p)
{
	FUNC_ENTRY;
	if (p && --(p->refcount) == 0)
	{
		free(p->payload);
		free(p->topic);
		ListRemove(&(state.publications), p);
	}
	FUNC_EXIT;
}

/**
 * Process an incoming publish packet for a socket
 * The payload field of the packet has not been transferred to another buffer at this point.
 * If it's needed beyond the scope of this function, it has to be copied.
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handlePublishes(void* pack, int sock)
{
	Publish* publish = (Publish*)pack;
	Clients* client = NULL;
	char* clientid = NULL;
	int rc = TCPSOCKET_COMPLETE;

	FUNC_ENTRY;
	client = (Clients*)(ListFindItem(bstate->clients, &sock, clientSocketCompare)->content);
	clientid = client->clientID;
	Log(LOG_PROTOCOL, 11, NULL, sock, clientid, publish->msgId, publish->header.bits.qos,
					publish->header.bits.retain, publish->payloadlen, min(20, publish->payloadlen), publish->payload);

	if (publish->header.bits.qos == 0)
		Protocol_processPublication(publish, client, 1);
	else if (!Socket_noPendingWrites(sock))
		rc = SOCKET_ERROR; /* queue acks? */
	else if (publish->header.bits.qos == 1)
	{
	  /* send puback before processing the publications because a lot of return publications could fill up the socket buffer */
	  rc = MQTTPacket_send_puback(publish->MQTTVersion, publish->msgId, &client->net, client->clientID);
	  /* if we get a socket error from sending the puback, should we ignore the publication? */
	  Protocol_processPublication(publish, client, 1);
	}
	else if (publish->header.bits.qos == 2)
	{
		/* store publication in inbound list */
		int len;
		int already_received = 0;
		ListElement* listElem = NULL;
		Messages* m = malloc(sizeof(Messages));
		Publications* p = NULL;
		if (!m)
		{
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}
		p = MQTTProtocol_storePublication(publish, &len);

		m->publish = p;
		m->msgid = publish->msgId;
		m->qos = publish->header.bits.qos;
		m->retain = publish->header.bits.retain;
		m->MQTTVersion = publish->MQTTVersion;
		if (m->MQTTVersion >= MQTTVERSION_5)
			m->properties = MQTTProperties_copy(&publish->properties);
		m->nextMessageType = PUBREL;
		if ((listElem = ListFindItem(client->inboundMsgs, &(m->msgid), messageIDCompare)) != NULL)
		{   /* discard queued publication with same msgID that the current incoming message */
			Messages* msg = (Messages*)(listElem->content);
			MQTTProtocol_removePublication(msg->publish);
			if (msg->MQTTVersion >= MQTTVERSION_5)
				MQTTProperties_free(&msg->properties);
			ListInsert(client->inboundMsgs, m, sizeof(Messages) + len, listElem);
			ListRemove(client->inboundMsgs, msg);
			already_received = 1;
		} else
			ListAppend(client->inboundMsgs, m, sizeof(Messages) + len);
		rc = MQTTPacket_send_pubrec(publish->MQTTVersion, publish->msgId, &client->net, client->clientID);
		if (m->MQTTVersion >= MQTTVERSION_5 && already_received == 0)
		{
			Publish publish1;

			publish1.header.bits.qos = m->qos;
			publish1.header.bits.retain = m->retain;
			publish1.msgId = m->msgid;
			publish1.topic = m->publish->topic;
			publish1.topiclen = m->publish->topiclen;
			publish1.payload = m->publish->payload;
			publish1.payloadlen = m->publish->payloadlen;
			publish1.MQTTVersion = m->MQTTVersion;
			publish1.properties = m->properties;

			Protocol_processPublication(&publish1, client, 1);
			ListRemove(&(state.publications), m->publish);
			m->publish = NULL;
		} else
		{	/* allocate and copy payload data as it's needed for pubrel.
		       For other cases, it's done in Protocol_processPublication */
			char *temp = m->publish->payload;

			if ((m->publish->payload = malloc(m->publish->payloadlen)) == NULL)
			{
				rc = PAHO_MEMORY_ERROR;
				goto exit;
			}
			memcpy(m->publish->payload, temp, m->publish->payloadlen);
		}
		publish->topic = NULL;
	}
exit:
	MQTTPacket_freePublish(publish);
	FUNC_EXIT_RC(rc);
	return rc;
}

/**
 * Process an incoming puback packet for a socket
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handlePubacks(void* pack, int sock)
{
	Puback* puback = (Puback*)pack;
	Clients* client = NULL;
	int rc = TCPSOCKET_COMPLETE;

	FUNC_ENTRY;
	client = (Clients*)(ListFindItem(bstate->clients, &sock, clientSocketCompare)->content);
	Log(LOG_PROTOCOL, 14, NULL, sock, client->clientID, puback->msgId);

	/* look for the message by message id in the records of outbound messages for this client */
	if (ListFindItem(client->outboundMsgs, &(puback->msgId), messageIDCompare) == NULL)
		Log(TRACE_MIN, 3, NULL, "PUBACK", client->clientID, puback->msgId);
	else
	{
		Messages* m = (Messages*)(client->outboundMsgs->current->content);
		if (m->qos != 1)
			Log(TRACE_MIN, 4, NULL, "PUBACK", client->clientID, puback->msgId, m->qos);
		else
		{
			Log(TRACE_MIN, 6, NULL, "PUBACK", client->clientID, puback->msgId);
			#if !defined(NO_PERSISTENCE)
				rc = MQTTPersistence_remove(client,
						(m->MQTTVersion >= MQTTVERSION_5) ? PERSISTENCE_V5_PUBLISH_SENT : PERSISTENCE_PUBLISH_SENT,
								m->qos, puback->msgId);
			#endif
			MQTTProtocol_removePublication(m->publish);
			if (m->MQTTVersion >= MQTTVERSION_5)
				MQTTProperties_free(&m->properties);
			ListRemove(client->outboundMsgs, m);
		}
	}
	if (puback->MQTTVersion >= MQTTVERSION_5)
		MQTTProperties_free(&puback->properties);
	free(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Process an incoming pubrec packet for a socket
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handlePubrecs(void* pack, int sock)
{
	Pubrec* pubrec = (Pubrec*)pack;
	Clients* client = NULL;
	int rc = TCPSOCKET_COMPLETE;

	FUNC_ENTRY;
	client = (Clients*)(ListFindItem(bstate->clients, &sock, clientSocketCompare)->content);
	Log(LOG_PROTOCOL, 15, NULL, sock, client->clientID, pubrec->msgId);

	/* look for the message by message id in the records of outbound messages for this client */
	client->outboundMsgs->current = NULL;
	if (ListFindItem(client->outboundMsgs, &(pubrec->msgId), messageIDCompare) == NULL)
	{
		if (pubrec->header.bits.dup == 0)
			Log(TRACE_MIN, 3, NULL, "PUBREC", client->clientID, pubrec->msgId);
	}
	else
	{
		Messages* m = (Messages*)(client->outboundMsgs->current->content);
		if (m->qos != 2)
		{
			if (pubrec->header.bits.dup == 0)
				Log(TRACE_MIN, 4, NULL, "PUBREC", client->clientID, pubrec->msgId, m->qos);
		}
		else if (m->nextMessageType != PUBREC)
		{
			if (pubrec->header.bits.dup == 0)
				Log(TRACE_MIN, 5, NULL, "PUBREC", client->clientID, pubrec->msgId);
		}
		else
		{
			if (pubrec->MQTTVersion >= MQTTVERSION_5 && pubrec->rc >= MQTTREASONCODE_UNSPECIFIED_ERROR)
			{
				Log(TRACE_MIN, -1, "Pubrec error %d received for client %s msgid %d, not sending PUBREL",
						pubrec->rc, client->clientID, pubrec->msgId);
				#if !defined(NO_PERSISTENCE)
					rc = MQTTPersistence_remove(client,
							(pubrec->MQTTVersion >= MQTTVERSION_5) ? PERSISTENCE_V5_PUBLISH_SENT : PERSISTENCE_PUBLISH_SENT,
							m->qos, pubrec->msgId);
				#endif
				MQTTProtocol_removePublication(m->publish);
				if (m->MQTTVersion >= MQTTVERSION_5)
					MQTTProperties_free(&m->properties);
				ListRemove(client->outboundMsgs, m);
				(++state.msgs_sent);
			}
			else
			{
				rc = MQTTPacket_send_pubrel(pubrec->MQTTVersion, pubrec->msgId, 0, &client->net, client->clientID);
				m->nextMessageType = PUBCOMP;
				m->lastTouch = MQTTTime_now();
			}
		}
	}
	if (pubrec->MQTTVersion >= MQTTVERSION_5)
		MQTTProperties_free(&pubrec->properties);
	free(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Process an incoming pubrel packet for a socket
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handlePubrels(void* pack, int sock)
{
	Pubrel* pubrel = (Pubrel*)pack;
	Clients* client = NULL;
	int rc = TCPSOCKET_COMPLETE;

	FUNC_ENTRY;
	client = (Clients*)(ListFindItem(bstate->clients, &sock, clientSocketCompare)->content);
	Log(LOG_PROTOCOL, 17, NULL, sock, client->clientID, pubrel->msgId);

	/* look for the message by message id in the records of inbound messages for this client */
	if (ListFindItem(client->inboundMsgs, &(pubrel->msgId), messageIDCompare) == NULL)
	{
		if (pubrel->header.bits.dup == 0)
			Log(TRACE_MIN, 3, NULL, "PUBREL", client->clientID, pubrel->msgId);
		else if (!Socket_noPendingWrites(sock))
			rc = SOCKET_ERROR; /* queue acks? */
		else
			/* Apparently this is "normal" behaviour, so we don't need to issue a warning */
			rc = MQTTPacket_send_pubcomp(pubrel->MQTTVersion, pubrel->msgId, &client->net, client->clientID);
	}
	else
	{
		Messages* m = (Messages*)(client->inboundMsgs->current->content);
		if (m->qos != 2)
			Log(TRACE_MIN, 4, NULL, "PUBREL", client->clientID, pubrel->msgId, m->qos);
		else if (m->nextMessageType != PUBREL)
			Log(TRACE_MIN, 5, NULL, "PUBREL", client->clientID, pubrel->msgId);
		else if (!Socket_noPendingWrites(sock))
		  rc = SOCKET_ERROR; /* queue acks? */
		else
		{
			Publish publish;

			memset(&publish, '\0', sizeof(publish));
			/* send pubcomp before processing the publications because a lot of return publications could fill up the socket buffer */
			rc = MQTTPacket_send_pubcomp(pubrel->MQTTVersion, pubrel->msgId, &client->net, client->clientID);
			publish.header.bits.qos = m->qos;
			publish.header.bits.retain = m->retain;
			publish.msgId = m->msgid;
			if (m->publish)
			{
				publish.topic = m->publish->topic;
				publish.topiclen = m->publish->topiclen;
				publish.payload = m->publish->payload;
				publish.payloadlen = m->publish->payloadlen;
			}
			publish.MQTTVersion = m->MQTTVersion;
			if (publish.MQTTVersion >= MQTTVERSION_5)
				publish.properties = m->properties;
			else
				Protocol_processPublication(&publish, client, 0); /* only for 3.1.1 and lower */
			#if !defined(NO_PERSISTENCE)
				rc += MQTTPersistence_remove(client,
						(m->MQTTVersion >= MQTTVERSION_5) ? PERSISTENCE_V5_PUBLISH_RECEIVED : PERSISTENCE_PUBLISH_RECEIVED,
						m->qos, pubrel->msgId);
			#endif
			if (m->MQTTVersion >= MQTTVERSION_5)
				MQTTProperties_free(&m->properties);
			if (m->publish)
				ListRemove(&(state.publications), m->publish);
			ListRemove(client->inboundMsgs, m);
			++(state.msgs_received);
		}
	}
	if (pubrel->MQTTVersion >= MQTTVERSION_5)
		MQTTProperties_free(&pubrel->properties);
	free(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Process an incoming pubcomp packet for a socket
 * @param pack pointer to the publish packet
 * @param sock the socket on which the packet was received
 * @return completion code
 */
int MQTTProtocol_handlePubcomps(void* pack, int sock)
{
	Pubcomp* pubcomp = (Pubcomp*)pack;
	Clients* client = NULL;
	int rc = TCPSOCKET_COMPLETE;

	FUNC_ENTRY;
	client = (Clients*)(ListFindItem(bstate->clients, &sock, clientSocketCompare)->content);
	Log(LOG_PROTOCOL, 19, NULL, sock, client->clientID, pubcomp->msgId);

	/* look for the message by message id in the records of outbound messages for this client */
	if (ListFindItem(client->outboundMsgs, &(pubcomp->msgId), messageIDCompare) == NULL)
	{
		if (pubcomp->header.bits.dup == 0)
			Log(TRACE_MIN, 3, NULL, "PUBCOMP", client->clientID, pubcomp->msgId);
	}
	else
	{
		Messages* m = (Messages*)(client->outboundMsgs->current->content);
		if (m->qos != 2)
			Log(TRACE_MIN, 4, NULL, "PUBCOMP", client->clientID, pubcomp->msgId, m->qos);
		else
		{
			if (m->nextMessageType != PUBCOMP)
				Log(TRACE_MIN, 5, NULL, "PUBCOMP", client->clientID, pubcomp->msgId);
			else
			{
				Log(TRACE_MIN, 6, NULL, "PUBCOMP", client->clientID, pubcomp->msgId);
				#if !defined(NO_PERSISTENCE)
					rc = MQTTPersistence_remove(client,
							(m->MQTTVersion >= MQTTVERSION_5) ? PERSISTENCE_V5_PUBLISH_SENT : PERSISTENCE_PUBLISH_SENT,
							m->qos, pubcomp->msgId);
					if (rc != 0)
						Log(LOG_ERROR, -1, "Error removing PUBCOMP for client id %s msgid %d from persistence", client->clientID, pubcomp->msgId);
				#endif
				MQTTProtocol_removePublication(m->publish);
				if (m->MQTTVersion >= MQTTVERSION_5)
					MQTTProperties_free(&m->properties);
				ListRemove(client->outboundMsgs, m);
				(++state.msgs_sent);
			}
		}
	}
	if (pubcomp->MQTTVersion >= MQTTVERSION_5)
		MQTTProperties_free(&pubcomp->properties);
	free(pack);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * MQTT protocol keepAlive processing.  Sends PINGREQ packets as required.
 * @param now current time
 */
void MQTTProtocol_keepalive(START_TIME_TYPE now)
{
	ListElement* current = NULL;

	FUNC_ENTRY;
	ListNextElement(bstate->clients, &current);
	while (current)
	{
		Clients* client =	(Clients*)(current->content);
		ListNextElement(bstate->clients, &current);

		if (client->connected == 0 || client->keepAliveInterval == 0)
			continue;

		if (client->ping_outstanding == 1)
		{
			if (MQTTTime_difftime(now, client->net.lastPing) >= (long)(client->keepAliveInterval * 1000))
			{
				Log(TRACE_PROTOCOL, -1, "PINGRESP not received in keepalive interval for client %s on socket %d, disconnecting", client->clientID, client->net.socket);
				MQTTProtocol_closeSession(client, 1);
			}
		}
		else if (MQTTTime_difftime(now, client->net.lastSent) >= (long)(client->keepAliveInterval * 1000) ||
					MQTTTime_difftime(now, client->net.lastReceived) >= (long)(client->keepAliveInterval * 1000))
		{
			if (Socket_noPendingWrites(client->net.socket))
			{
				if (MQTTPacket_send_pingreq(&client->net, client->clientID) != TCPSOCKET_COMPLETE)
				{
					Log(TRACE_PROTOCOL, -1, "Error sending PINGREQ for client %s on socket %d, disconnecting", client->clientID, client->net.socket);
					MQTTProtocol_closeSession(client, 1);
				}
				else
				{
					client->net.lastPing = now;
					client->ping_outstanding = 1;
				}
			}
		}
	}
	FUNC_EXIT;
}


/**
 * MQTT retry processing per client
 * @param now current time
 * @param client - the client to which to apply the retry processing
 * @param regardless boolean - retry packets regardless of retry interval (used on reconnect)
 */
static void MQTTProtocol_retries(START_TIME_TYPE now, Clients* client, int regardless)
{
	ListElement* outcurrent = NULL;

	FUNC_ENTRY;

	if (!regardless && client->retryInterval <= 0) /* 0 or -ive retryInterval turns off retry except on reconnect */
		goto exit;

	while (client && ListNextElement(client->outboundMsgs, &outcurrent) &&
		   client->connected && client->good &&        /* client is connected and has no errors */
		   Socket_noPendingWrites(client->net.socket)) /* there aren't any previous packets still stacked up on the socket */
	{
		Messages* m = (Messages*)(outcurrent->content);
		if (regardless || MQTTTime_difftime(now, m->lastTouch) > (long)(max(client->retryInterval, 10) * 1000))
		{
			if (m->qos == 1 || (m->qos == 2 && m->nextMessageType == PUBREC))
			{
				Publish publish;
				int rc;

				Log(TRACE_MIN, 7, NULL, "PUBLISH", client->clientID, client->net.socket, m->msgid);
				publish.msgId = m->msgid;
				publish.topic = m->publish->topic;
				publish.payload = m->publish->payload;
				publish.payloadlen = m->publish->payloadlen;
				publish.properties = m->properties;
				publish.MQTTVersion = m->MQTTVersion;
				rc = MQTTPacket_send_publish(&publish, 1, m->qos, m->retain, &client->net, client->clientID);
				if (rc == SOCKET_ERROR)
				{
					client->good = 0;
					Log(TRACE_PROTOCOL, 29, NULL, client->clientID, client->net.socket,
												Socket_getpeer(client->net.socket));
					MQTTProtocol_closeSession(client, 1);
					client = NULL;
				}
				else
				{
					if (m->qos == 0 && rc == TCPSOCKET_INTERRUPTED)
						MQTTProtocol_storeQoS0(client, &publish);
					m->lastTouch = MQTTTime_now();
				}
			}
			else if (m->qos && m->nextMessageType == PUBCOMP)
			{
				Log(TRACE_MIN, 7, NULL, "PUBREL", client->clientID, client->net.socket, m->msgid);
				if (MQTTPacket_send_pubrel(m->MQTTVersion, m->msgid, 0, &client->net, client->clientID) != TCPSOCKET_COMPLETE)
				{
					client->good = 0;
					Log(TRACE_PROTOCOL, 29, NULL, client->clientID, client->net.socket,
							Socket_getpeer(client->net.socket));
					MQTTProtocol_closeSession(client, 1);
					client = NULL;
				}
				else
					m->lastTouch = MQTTTime_now();
			}
			/* break; why not do all retries at once? */
		}
	}
exit:
	FUNC_EXIT;
}


/**
 * MQTT retry protocol and socket pending writes processing.
 * @param now current time
 * @param doRetry boolean - retries as well as pending writes?
 * @param regardless boolean - retry packets regardless of retry interval (used on reconnect)
 */
void MQTTProtocol_retry(START_TIME_TYPE now, int doRetry, int regardless)
{
	ListElement* current = NULL;

	FUNC_ENTRY;
	ListNextElement(bstate->clients, &current);
	/* look through the outbound message list of each client, checking to see if a retry is necessary */
	while (current)
	{
		Clients* client = (Clients*)(current->content);
		ListNextElement(bstate->clients, &current);
		if (client->connected == 0)
			continue;
		if (client->good == 0)
		{
			MQTTProtocol_closeSession(client, 1);
			continue;
		}
		if (Socket_noPendingWrites(client->net.socket) == 0)
			continue;
		if (doRetry)
			MQTTProtocol_retries(now, client, regardless);
	}
	FUNC_EXIT;
}


/**
 * Free a client structure
 * @param client the client data to free
 */
void MQTTProtocol_freeClient(Clients* client)
{
	FUNC_ENTRY;
	/* free up pending message lists here, and any other allocated data */
	MQTTProtocol_freeMessageList(client->outboundMsgs);
	MQTTProtocol_freeMessageList(client->inboundMsgs);
	ListFree(client->messageQueue);
	free(client->clientID);
        client->clientID = NULL;
	if (client->will)
	{
		free(client->will->payload);
		free(client->will->topic);
		free(client->will);
                client->will = NULL;
	}
	if (client->username)
		free((void*)client->username);
	if (client->password)
		free((void*)client->password);
#if defined(OPENSSL)
	if (client->sslopts)
	{
		if (client->sslopts->trustStore)
			free((void*)client->sslopts->trustStore);
		if (client->sslopts->keyStore)
			free((void*)client->sslopts->keyStore);
		if (client->sslopts->privateKey)
			free((void*)client->sslopts->privateKey);
		if (client->sslopts->privateKeyPassword)
			free((void*)client->sslopts->privateKeyPassword);
		if (client->sslopts->enabledCipherSuites)
			free((void*)client->sslopts->enabledCipherSuites);
		if (client->sslopts->struct_version >= 2)
		{
			if (client->sslopts->CApath)
				free((void*)client->sslopts->CApath);
		}
		free(client->sslopts);
                client->sslopts = NULL;
	}
#endif
	/* don't free the client structure itself... this is done elsewhere */
	FUNC_EXIT;
}


/**
 * Empty a message list, leaving it able to accept new messages
 * @param msgList the message list to empty
 */
void MQTTProtocol_emptyMessageList(List* msgList)
{
	ListElement* current = NULL;

	FUNC_ENTRY;
	while (ListNextElement(msgList, &current))
	{
		Messages* m = (Messages*)(current->content);
		MQTTProtocol_removePublication(m->publish);
		if (m->MQTTVersion >= MQTTVERSION_5)
			MQTTProperties_free(&m->properties);
	}
	ListEmpty(msgList);
	FUNC_EXIT;
}


/**
 * Empty and free up all storage used by a message list
 * @param msgList the message list to empty and free
 */
void MQTTProtocol_freeMessageList(List* msgList)
{
	FUNC_ENTRY;
	MQTTProtocol_emptyMessageList(msgList);
	ListFree(msgList);
	FUNC_EXIT;
}


/**
* Copy no more than dest_size -1 characters from the string pointed to by src to the array pointed to by dest.
* The destination string will always be null-terminated.
* @param dest the array which characters copy to
* @param src the source string which characters copy from
* @param dest_size the size of the memory pointed to by dest: copy no more than this -1 (allow for null).  Must be >= 1
* @return the destination string pointer
*/
char* MQTTStrncpy(char *dest, const char *src, size_t dest_size)
{
  size_t count = dest_size;
  char *temp = dest;

  FUNC_ENTRY;
  if (dest_size < strlen(src))
    Log(TRACE_MIN, -1, "the src string is truncated");

  /* We must copy only the first (dest_size - 1) bytes */
  while (count > 1 && (*temp++ = *src++))
    count--;

  *temp = '\0';

  FUNC_EXIT;
  return dest;
}


/**
* Duplicate a string, safely, allocating space on the heap
* @param src the source string which characters copy from
* @return the duplicated, allocated string
*/
char* MQTTStrdup(const char* src)
{
	size_t mlen = strlen(src) + 1;
	char* temp = malloc(mlen);
	if (temp)
		MQTTStrncpy(temp, src, mlen);
	return temp;
}
