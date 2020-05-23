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
 *    Ian Craggs - async client updates
 *    Ian Craggs - fix for bug 432903 - queue persistence
 *    Ian Craggs - MQTT V5 updates
 *******************************************************************************/

/**
 * @file
 * \brief Functions that apply to persistence operations.
 *
 */

#include <stdio.h>
#include <string.h>

#include "MQTTPersistence.h"
#include "MQTTPersistenceDefault.h"
#include "MQTTProtocolClient.h"
#include "Heap.h"


static MQTTPersistence_qEntry* MQTTPersistence_restoreQueueEntry(char* buffer, size_t buflen, int MQTTVersion);
static void MQTTPersistence_insertInSeqOrder(List* list, MQTTPersistence_qEntry* qEntry, size_t size);

/**
 * Creates a ::MQTTClient_persistence structure representing a persistence implementation.
 * @param persistence the ::MQTTClient_persistence structure.
 * @param type the type of the persistence implementation. See ::MQTTClient_create.
 * @param pcontext the context for this persistence implementation. See ::MQTTClient_create.
 * @return 0 if success, #MQTTCLIENT_PERSISTENCE_ERROR otherwise.
 */
#include "StackTrace.h"

int MQTTPersistence_create(MQTTClient_persistence** persistence, int type, void* pcontext)
{
	int rc = 0;
	MQTTClient_persistence* per = NULL;

	FUNC_ENTRY;
#if !defined(NO_PERSISTENCE)
	switch (type)
	{
		case MQTTCLIENT_PERSISTENCE_NONE :
			per = NULL;
			break;
		case MQTTCLIENT_PERSISTENCE_DEFAULT :
			per = malloc(sizeof(MQTTClient_persistence));
			if ( per != NULL )
			{
				if ( pcontext != NULL )
				{
					if ((per->context = malloc(strlen(pcontext) + 1)) == NULL)
					{
						free(per);
						rc = PAHO_MEMORY_ERROR;
						goto exit;
					}
					strcpy(per->context, pcontext);
				}
				else
					per->context = ".";  /* working directory */
				/* file system functions */
				per->popen        = pstopen;
				per->pclose       = pstclose;
				per->pput         = pstput;
				per->pget         = pstget;
				per->premove      = pstremove;
				per->pkeys        = pstkeys;
				per->pclear       = pstclear;
				per->pcontainskey = pstcontainskey;
			}
			else
				rc = PAHO_MEMORY_ERROR;
			break;
		case MQTTCLIENT_PERSISTENCE_USER :
			per = (MQTTClient_persistence *)pcontext;
			if ( per == NULL || (per != NULL && (per->context == NULL || per->pclear == NULL ||
				per->pclose == NULL || per->pcontainskey == NULL || per->pget == NULL || per->pkeys == NULL ||
				per->popen == NULL || per->pput == NULL || per->premove == NULL)) )
				rc = MQTTCLIENT_PERSISTENCE_ERROR;
			break;
		default:
			rc = MQTTCLIENT_PERSISTENCE_ERROR;
			break;
	}
#endif

	*persistence = per;
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Open persistent store and restore any persisted messages.
 * @param client the client as ::Clients.
 * @param serverURI the URI of the remote end.
 * @return 0 if success, #MQTTCLIENT_PERSISTENCE_ERROR otherwise.
 */
int MQTTPersistence_initialize(Clients *c, const char *serverURI)
{
	int rc = 0;

	FUNC_ENTRY;
	if ( c->persistence != NULL )
	{
		rc = c->persistence->popen(&(c->phandle), c->clientID, serverURI, c->persistence->context);
		if ( rc == 0 )
			rc = MQTTPersistence_restore(c);
	}

	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Close persistent store.
 * @param client the client as ::Clients.
 * @return 0 if success, #MQTTCLIENT_PERSISTENCE_ERROR otherwise.
 */
int MQTTPersistence_close(Clients *c)
{
	int rc =0;

	FUNC_ENTRY;
	if (c->persistence != NULL)
	{
		rc = c->persistence->pclose(c->phandle);
		c->phandle = NULL;
#if !defined(NO_PERSISTENCE)
		if ( c->persistence->popen == pstopen )
			free(c->persistence);
#endif
		c->persistence = NULL;
	}

	FUNC_EXIT_RC(rc);
	return rc;
}

/**
 * Clears the persistent store.
 * @param client the client as ::Clients.
 * @return 0 if success, #MQTTCLIENT_PERSISTENCE_ERROR otherwise.
 */
int MQTTPersistence_clear(Clients *c)
{
	int rc = 0;

	FUNC_ENTRY;
	if (c->persistence != NULL)
		rc = c->persistence->pclear(c->phandle);

	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Restores the persisted records to the outbound and inbound message queues of the
 * client.
 * @param client the client as ::Clients.
 * @return 0 if success, #MQTTCLIENT_PERSISTENCE_ERROR otherwise.
 */
int MQTTPersistence_restore(Clients *c)
{
	int rc = 0;
	char **msgkeys = NULL,
		 *buffer = NULL;
	int nkeys, buflen;
	int i = 0;
	int msgs_sent = 0;
	int msgs_rcvd = 0;

	FUNC_ENTRY;
	if (c->persistence && (rc = c->persistence->pkeys(c->phandle, &msgkeys, &nkeys)) == 0)
	{
		while (rc == 0 && i < nkeys)
		{
			if (strncmp(msgkeys[i], PERSISTENCE_COMMAND_KEY, strlen(PERSISTENCE_COMMAND_KEY)) == 0 ||
				strncmp(msgkeys[i], PERSISTENCE_V5_COMMAND_KEY, strlen(PERSISTENCE_V5_COMMAND_KEY)) == 0)
			{
				;
			}
			else if (strncmp(msgkeys[i], PERSISTENCE_QUEUE_KEY, strlen(PERSISTENCE_QUEUE_KEY)) == 0 ||
					 strncmp(msgkeys[i], PERSISTENCE_V5_QUEUE_KEY, strlen(PERSISTENCE_V5_QUEUE_KEY)) == 0)
			{
				;
			}
			else if ((rc = c->persistence->pget(c->phandle, msgkeys[i], &buffer, &buflen)) == 0)
			{
				int data_MQTTVersion = MQTTVERSION_3_1_1;
				char* cur_key = msgkeys[i];
				MQTTPacket* pack = NULL;

				if (	strncmp(cur_key, PERSISTENCE_V5_PUBLISH_RECEIVED,
							strlen(PERSISTENCE_V5_PUBLISH_RECEIVED)) == 0)
				{
					data_MQTTVersion = MQTTVERSION_5;
					cur_key = PERSISTENCE_PUBLISH_RECEIVED;
				}
				else if (strncmp(cur_key, PERSISTENCE_V5_PUBLISH_SENT,
							strlen(PERSISTENCE_V5_PUBLISH_SENT)) == 0)
				{
					data_MQTTVersion = MQTTVERSION_5;
					cur_key = PERSISTENCE_PUBLISH_SENT;
				}
				else if (strncmp(cur_key, PERSISTENCE_V5_PUBREL,
							strlen(PERSISTENCE_V5_PUBREL)) == 0)
				{
					data_MQTTVersion = MQTTVERSION_5;
					cur_key = PERSISTENCE_PUBREL;
				}

				if (data_MQTTVersion == MQTTVERSION_5 && c->MQTTVersion < MQTTVERSION_5)
				{
					rc = MQTTCLIENT_PERSISTENCE_ERROR; /* can't restore version 5 data with a version 3 client */
					goto exit;
				}

				pack = MQTTPersistence_restorePacket(data_MQTTVersion, buffer, buflen);
				if ( pack != NULL )
				{
					if (strncmp(cur_key, PERSISTENCE_PUBLISH_RECEIVED,
							      strlen(PERSISTENCE_PUBLISH_RECEIVED)) == 0)
					{
						Publish* publish = (Publish*)pack;
						Messages* msg = NULL;
						publish->MQTTVersion = c->MQTTVersion;
						msg = MQTTProtocol_createMessage(publish, &msg, publish->header.bits.qos, publish->header.bits.retain, 1);
						msg->nextMessageType = PUBREL;
						/* order does not matter for persisted received messages */
						ListAppend(c->inboundMsgs, msg, msg->len);
						if (c->MQTTVersion >= MQTTVERSION_5)
						{
							free(msg->publish->payload);
							free(msg->publish->topic);
							msg->publish->payload = msg->publish->topic = NULL;
						}
						publish->topic = NULL;
						MQTTPacket_freePublish(publish);
						msgs_rcvd++;
					}
					else if (strncmp(cur_key, PERSISTENCE_PUBLISH_SENT,
						               strlen(PERSISTENCE_PUBLISH_SENT)) == 0)
					{
						Publish* publish = (Publish*)pack;
						Messages* msg = NULL;
						char *key = malloc(MESSAGE_FILENAME_LENGTH + 1);

						if (!key)
						{
							rc = PAHO_MEMORY_ERROR;
							goto exit;
						}
						publish->MQTTVersion = c->MQTTVersion;
						if (publish->MQTTVersion >= MQTTVERSION_5)
							sprintf(key, "%s%d", PERSISTENCE_V5_PUBREL, publish->msgId);
						else
							sprintf(key, "%s%d", PERSISTENCE_PUBREL, publish->msgId);
						msg = MQTTProtocol_createMessage(publish, &msg, publish->header.bits.qos, publish->header.bits.retain, 1);
						if (c->persistence->pcontainskey(c->phandle, key) == 0)
							/* PUBLISH Qo2 and PUBREL sent */
							msg->nextMessageType = PUBCOMP;
						/* else: PUBLISH QoS1, or PUBLISH QoS2 and PUBREL not sent */
						/* retry at the first opportunity */
						memset(&msg->lastTouch, '\0', sizeof(msg->lastTouch));
						MQTTPersistence_insertInOrder(c->outboundMsgs, msg, msg->len);
						publish->topic = NULL;
						MQTTPacket_freePublish(publish);
						free(key);
						msgs_sent++;
					}
					else if (strncmp(cur_key, PERSISTENCE_PUBREL, strlen(PERSISTENCE_PUBREL)) == 0)
					{
						/* orphaned PUBRELs ? */
						Pubrel* pubrel = (Pubrel*)pack;
						char *key = malloc(MESSAGE_FILENAME_LENGTH + 1);

						if (!key)
						{
							rc = PAHO_MEMORY_ERROR;
							goto exit;
						}
						pubrel->MQTTVersion = c->MQTTVersion;
						if (pubrel->MQTTVersion >= MQTTVERSION_5)
							sprintf(key, "%s%d", PERSISTENCE_V5_PUBLISH_SENT, pubrel->msgId);
						else
							sprintf(key, "%s%d", PERSISTENCE_PUBLISH_SENT, pubrel->msgId);
						if (c->persistence->pcontainskey(c->phandle, key) != 0)
							rc = c->persistence->premove(c->phandle, msgkeys[i]);
						free(pubrel);
						free(key);
					}
				}
				else  /* pack == NULL -> bad persisted record */
					rc = c->persistence->premove(c->phandle, msgkeys[i]);
			}
			if (buffer)
			{
				free(buffer);
				buffer = NULL;
			}
			if (msgkeys[i])
				free(msgkeys[i]);
			i++;
		}
		if (msgkeys)
			free(msgkeys);
	}
	Log(TRACE_MINIMUM, -1, "%d sent messages and %d received messages restored for client %s\n", 
		msgs_sent, msgs_rcvd, c->clientID);
	MQTTPersistence_wrapMsgID(c);
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Returns a MQTT packet restored from persisted data.
 * @param buffer the persisted data.
 * @param buflen the number of bytes of the data buffer.
 */
void* MQTTPersistence_restorePacket(int MQTTVersion, char* buffer, size_t buflen)
{
	void* pack = NULL;
	Header header;
	int fixed_header_length = 1, ptype, remaining_length = 0;
	char c;
	int multiplier = 1;
	extern pf new_packets[];

	FUNC_ENTRY;
	header.byte = buffer[0];
	/* decode the message length according to the MQTT algorithm */
	do
	{
		c = *(++buffer);
		remaining_length += (c & 127) * multiplier;
		multiplier *= 128;
		fixed_header_length++;
	} while ((c & 128) != 0);

	if ( (fixed_header_length + remaining_length) == buflen )
	{
		ptype = header.bits.type;
		if (ptype >= CONNECT && ptype <= DISCONNECT && new_packets[ptype] != NULL)
			pack = (*new_packets[ptype])(MQTTVersion, header.byte, ++buffer, remaining_length);
	}

	FUNC_EXIT;
	return pack;
}


/**
 * Inserts the specified message into the list, maintaining message ID order.
 * @param list the list to insert the message into.
 * @param content the message to add.
 * @param size size of the message.
 */
void MQTTPersistence_insertInOrder(List* list, void* content, size_t size)
{
	ListElement* index = NULL;
	ListElement* current = NULL;

	FUNC_ENTRY;
	while(ListNextElement(list, &current) != NULL && index == NULL)
	{
		if ( ((Messages*)content)->msgid < ((Messages*)current->content)->msgid )
			index = current;
	}

	ListInsert(list, content, size, index);
	FUNC_EXIT;
}


/**
 * Adds a record to the persistent store. This function must not be called for QoS0
 * messages.
 * @param socket the socket of the client.
 * @param buf0 fixed header.
 * @param buf0len length of the fixed header.
 * @param count number of buffers representing the variable header and/or the payload.
 * @param buffers the buffers representing the variable header and/or the payload.
 * @param buflens length of the buffers representing the variable header and/or the payload.
 * @param htype MQTT packet type - PUBLISH or PUBREL
 * @param msgId the message ID.
 * @param scr 0 indicates message in the sending direction; 1 indicates message in the
 * receiving direction.
 * @param the MQTT version being used (>= MQTTVERSION_5 means properties included)
 * @return 0 if success, #MQTTCLIENT_PERSISTENCE_ERROR otherwise.
 */
int MQTTPersistence_put(int socket, char* buf0, size_t buf0len, int count,
						char** buffers, size_t* buflens, int htype, int msgId, int scr, int MQTTVersion)
{
	int rc = 0;
	extern ClientStates* bstate;
	int nbufs, i;
	int* lens = NULL;
	char** bufs = NULL;
	char *key;
	Clients* client = NULL;

	FUNC_ENTRY;
	client = (Clients*)(ListFindItem(bstate->clients, &socket, clientSocketCompare)->content);
	if (client->persistence != NULL)
	{
		if ((key = malloc(MESSAGE_FILENAME_LENGTH + 1)) == NULL)
		{
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}
		nbufs = 1 + count;
		if ((lens = (int *)malloc(nbufs * sizeof(int))) == NULL)
		{
			free(key);
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}
		if ((bufs = (char **)malloc(nbufs * sizeof(char *))) == NULL)
		{
			free(key);
			free(lens);
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}
		lens[0] = (int)buf0len;
		bufs[0] = buf0;
		for (i = 0; i < count; i++)
		{
			lens[i+1] = (int)buflens[i];
			bufs[i+1] = buffers[i];
		}

		/* key */
		if (scr == 0)
		{  /* sending */
			char* key_id = PERSISTENCE_PUBLISH_SENT;

			if (htype == PUBLISH) /* PUBLISH QoS1 and QoS2*/
			{
				if (MQTTVersion >= MQTTVERSION_5)
					key_id = PERSISTENCE_V5_PUBLISH_SENT;
			}
			else if (htype == PUBREL) /* PUBREL */
			{
				if (MQTTVersion >= MQTTVERSION_5)
					key_id = PERSISTENCE_V5_PUBREL;
				else
					key_id = PERSISTENCE_PUBREL;
			}
			sprintf(key, "%s%d", key_id, msgId);
		}
		else if (scr == 1)  /* receiving PUBLISH QoS2 */
		{
			char* key_id = PERSISTENCE_PUBLISH_RECEIVED;

			if (MQTTVersion >= MQTTVERSION_5)
				key_id = PERSISTENCE_V5_PUBLISH_RECEIVED;
			sprintf(key, "%s%d", key_id, msgId);
		}

		rc = client->persistence->pput(client->phandle, key, nbufs, bufs, lens);

		free(key);
		free(lens);
		free(bufs);
	}

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Deletes a record from the persistent store.
 * @param client the client as ::Clients.
 * @param type the type of the persisted record: #PERSISTENCE_PUBLISH_SENT, #PERSISTENCE_PUBREL
 * or #PERSISTENCE_PUBLISH_RECEIVED.
 * @param qos the qos field of the message.
 * @param msgId the message ID.
 * @return 0 if success, #MQTTCLIENT_PERSISTENCE_ERROR otherwise.
 */
int MQTTPersistence_remove(Clients* c, char *type, int qos, int msgId)
{
	int rc = 0;

	FUNC_ENTRY;
	if (c->persistence != NULL)
	{
		char *key = malloc(MESSAGE_FILENAME_LENGTH + 1);

		if (!key)
		{
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}
		if (strcmp(type, PERSISTENCE_PUBLISH_SENT) == 0 ||
				strcmp(type, PERSISTENCE_V5_PUBLISH_SENT) == 0)
		{
			sprintf(key, "%s%d", PERSISTENCE_V5_PUBLISH_SENT, msgId) ;
			rc = c->persistence->premove(c->phandle, key);
			sprintf(key, "%s%d", PERSISTENCE_V5_PUBREL, msgId) ;
			rc += c->persistence->premove(c->phandle, key);
			sprintf(key, "%s%d", PERSISTENCE_PUBLISH_SENT, msgId) ;
			rc += c->persistence->premove(c->phandle, key);
			sprintf(key, "%s%d", PERSISTENCE_PUBREL, msgId) ;
			rc += c->persistence->premove(c->phandle, key);
		}
		else /* PERSISTENCE_PUBLISH_SENT && qos == 1 */
		{    /* or PERSISTENCE_PUBLISH_RECEIVED */

			sprintf(key, "%s%d", PERSISTENCE_V5_PUBLISH_RECEIVED, msgId);
			rc = c->persistence->premove(c->phandle, key);
			sprintf(key, "%s%d", PERSISTENCE_PUBLISH_RECEIVED, msgId);
			rc += c->persistence->premove(c->phandle, key);
		}
		free(key);
	}

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Checks whether the message IDs wrapped by looking for the largest gap between two consecutive
 * message IDs in the outboundMsgs queue.
 * @param client the client as ::Clients.
 */
void MQTTPersistence_wrapMsgID(Clients *client)
{
	ListElement* wrapel = NULL;
	ListElement* current = NULL;

	FUNC_ENTRY;
	if ( client->outboundMsgs->count > 0 )
	{
		int firstMsgID = ((Messages*)client->outboundMsgs->first->content)->msgid;
		int lastMsgID = ((Messages*)client->outboundMsgs->last->content)->msgid;
		int gap = MAX_MSG_ID - lastMsgID + firstMsgID;
		current = ListNextElement(client->outboundMsgs, &current);

		while(ListNextElement(client->outboundMsgs, &current) != NULL)
		{
			int curMsgID = ((Messages*)current->content)->msgid;
			int curPrevMsgID = ((Messages*)current->prev->content)->msgid;
			int curgap = curMsgID - curPrevMsgID;
			if ( curgap > gap )
			{
				gap = curgap;
				wrapel = current;
			}
		}
	}

	if ( wrapel != NULL )
	{
		/* put wrapel at the beginning of the queue */
		client->outboundMsgs->first->prev = client->outboundMsgs->last;
		client->outboundMsgs->last->next = client->outboundMsgs->first;
		client->outboundMsgs->first = wrapel;
		client->outboundMsgs->last = wrapel->prev;
		client->outboundMsgs->first->prev = NULL;
		client->outboundMsgs->last->next = NULL;
	}
	FUNC_EXIT;
}


#if !defined(NO_PERSISTENCE)
int MQTTPersistence_unpersistQueueEntry(Clients* client, MQTTPersistence_qEntry* qe)
{
	int rc = 0;
	char key[PERSISTENCE_MAX_KEY_LENGTH + 1];
	
	FUNC_ENTRY;
	if (client->MQTTVersion >= MQTTVERSION_5)
		sprintf(key, "%s%u", PERSISTENCE_V5_QUEUE_KEY, qe->seqno);
	else
		sprintf(key, "%s%u", PERSISTENCE_QUEUE_KEY, qe->seqno);
	if ((rc = client->persistence->premove(client->phandle, key)) != 0)
		Log(LOG_ERROR, 0, "Error %d removing qEntry from persistence", rc);
	FUNC_EXIT_RC(rc);
	return rc;
}


#define MAX_NO_OF_BUFFERS 9
int MQTTPersistence_persistQueueEntry(Clients* aclient, MQTTPersistence_qEntry* qe)
{
	int rc = 0;
	int bufindex = 0;
	char key[PERSISTENCE_MAX_KEY_LENGTH + 1];
	int lens[MAX_NO_OF_BUFFERS];
	void* bufs[MAX_NO_OF_BUFFERS];
	int props_allocated = 0;
		
	FUNC_ENTRY;
	bufs[bufindex] = &qe->msg->payloadlen;
	lens[bufindex++] = sizeof(qe->msg->payloadlen);
				
	bufs[bufindex] = qe->msg->payload;
	lens[bufindex++] = qe->msg->payloadlen;
		
	bufs[bufindex] = &qe->msg->qos;
	lens[bufindex++] = sizeof(qe->msg->qos);
		
	bufs[bufindex] = &qe->msg->retained;
	lens[bufindex++] = sizeof(qe->msg->retained);
		
	bufs[bufindex] = &qe->msg->dup;
	lens[bufindex++] = sizeof(qe->msg->dup);
				
	bufs[bufindex] = &qe->msg->msgid;
	lens[bufindex++] = sizeof(qe->msg->msgid);
						
	bufs[bufindex] = qe->topicName;
	lens[bufindex++] = (int)strlen(qe->topicName) + 1;
				
	bufs[bufindex] = &qe->topicLen;
	lens[bufindex++] = sizeof(qe->topicLen);

	if (aclient->MQTTVersion >= MQTTVERSION_5)  		/* persist properties */
	{
		MQTTProperties no_props = MQTTProperties_initializer;
		MQTTProperties* props = &no_props;
		int temp_len = 0;
		char* ptr = NULL;

		if (qe->msg->struct_version >= 1)
			props = &qe->msg->properties;

		temp_len = MQTTProperties_len(props);
		ptr = bufs[bufindex] = malloc(temp_len);
		if (!ptr)
		{
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}
		props_allocated = bufindex;
		rc = MQTTProperties_write(&ptr, props);
		lens[bufindex++] = temp_len;

		sprintf(key, "%s%u", PERSISTENCE_V5_QUEUE_KEY, ++aclient->qentry_seqno);
	}
	else
		sprintf(key, "%s%u", PERSISTENCE_QUEUE_KEY, ++aclient->qentry_seqno);

	qe->seqno = aclient->qentry_seqno;

	if ((rc = aclient->persistence->pput(aclient->phandle, key, bufindex, (char**)bufs, lens)) != 0)
		Log(LOG_ERROR, 0, "Error persisting queue entry, rc %d", rc);

	if (props_allocated != 0)
		free(bufs[props_allocated]);

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


static MQTTPersistence_qEntry* MQTTPersistence_restoreQueueEntry(char* buffer, size_t buflen, int MQTTVersion)
{
	MQTTPersistence_qEntry* qe = NULL;
	char* ptr = buffer;
	int data_size;
	
	FUNC_ENTRY;
	if ((qe = malloc(sizeof(MQTTPersistence_qEntry))) == NULL)
		goto exit;
	memset(qe, '\0', sizeof(MQTTPersistence_qEntry));
	
	if ((qe->msg = malloc(sizeof(MQTTPersistence_message))) == NULL)
	{
		free(qe);
		qe = NULL;
		goto exit;
	}
	memset(qe->msg, '\0', sizeof(MQTTPersistence_message));
	
	qe->msg->struct_version = 1;

	qe->msg->payloadlen = *(int*)ptr;
	ptr += sizeof(int);
	
	data_size = qe->msg->payloadlen;
	if ((qe->msg->payload = malloc(data_size)) == NULL)
	{
		free(qe->msg);
		free(qe);
		qe = NULL;
		goto exit;
	}
	memcpy(qe->msg->payload, ptr, data_size);
	ptr += data_size;
	
	qe->msg->qos = *(int*)ptr;
	ptr += sizeof(int);
	
	qe->msg->retained = *(int*)ptr;
	ptr += sizeof(int);
	
	qe->msg->dup = *(int*)ptr;
	ptr += sizeof(int);
	
	qe->msg->msgid = *(int*)ptr;
	ptr += sizeof(int);
	
	data_size = (int)strlen(ptr) + 1;	
	if ((qe->topicName = malloc(data_size)) == NULL)
	{
		free(qe->msg->payload);
		free(qe->msg);
		free(qe);
		qe = NULL;
		goto exit;
	}
	strcpy(qe->topicName, ptr);
	ptr += data_size;
	
	qe->topicLen = *(int*)ptr;
	ptr += sizeof(int);

	if (MQTTVersion >= MQTTVERSION_5 &&
		MQTTProperties_read(&qe->msg->properties, &ptr, buffer + buflen) != 1)
			Log(LOG_ERROR, -1, "Error restoring properties from persistence");

exit:
	FUNC_EXIT;
	return qe;
}


static void MQTTPersistence_insertInSeqOrder(List* list, MQTTPersistence_qEntry* qEntry, size_t size)
{
	ListElement* index = NULL;
	ListElement* current = NULL;

	FUNC_ENTRY;
	while (ListNextElement(list, &current) != NULL && index == NULL)
	{
		if (qEntry->seqno < ((MQTTPersistence_qEntry*)current->content)->seqno)
			index = current;
	}
	ListInsert(list, qEntry, size, index);
	FUNC_EXIT;
}


/**
 * Restores a queue of messages from persistence to memory
 * @param c the client as ::Clients - the client object to restore the messages to
 * @return return code, 0 if successful
 */
int MQTTPersistence_restoreMessageQueue(Clients* c)
{
	int rc = 0;
	char **msgkeys;
	int nkeys;
	int i = 0;
	int entries_restored = 0;

	FUNC_ENTRY;
	if (c->persistence && (rc = c->persistence->pkeys(c->phandle, &msgkeys, &nkeys)) == 0)
	{
		while (rc == 0 && i < nkeys)
		{
			char *buffer = NULL;
			int buflen;
					
			if (strncmp(msgkeys[i], PERSISTENCE_QUEUE_KEY, strlen(PERSISTENCE_QUEUE_KEY)) != 0 &&
				strncmp(msgkeys[i], PERSISTENCE_V5_QUEUE_KEY, strlen(PERSISTENCE_V5_QUEUE_KEY)) != 0)
			{
				; /* ignore if not a queue entry key */
			}
			else if ((rc = c->persistence->pget(c->phandle, msgkeys[i], &buffer, &buflen)) == 0)
			{
				int MQTTVersion =
					(strncmp(msgkeys[i], PERSISTENCE_V5_QUEUE_KEY, strlen(PERSISTENCE_V5_QUEUE_KEY)) == 0)
					? MQTTVERSION_5 : MQTTVERSION_3_1_1;
				MQTTPersistence_qEntry* qe = MQTTPersistence_restoreQueueEntry(buffer, buflen, MQTTVersion);
				
				if (qe)
				{	
					qe->seqno = atoi(strchr(msgkeys[i], '-')+1); /* key format is tag'-'seqno */
					MQTTPersistence_insertInSeqOrder(c->messageQueue, qe, sizeof(MQTTPersistence_qEntry));
					free(buffer);
					c->qentry_seqno = max(c->qentry_seqno, qe->seqno);
					entries_restored++;
				}
			}
			if (msgkeys[i])
			{
				free(msgkeys[i]);
			}
			i++;
		}
		if (msgkeys != NULL)
			free(msgkeys);
	}
	Log(TRACE_MINIMUM, -1, "%d queued messages restored for client %s", entries_restored, c->clientID);
	FUNC_EXIT_RC(rc);
	return rc;
}
#endif
