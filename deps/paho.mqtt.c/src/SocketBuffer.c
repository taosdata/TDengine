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
 *    Ian Craggs - fix for issue #244, issue #20
 *******************************************************************************/

/**
 * @file
 * \brief Socket buffering related functions
 *
 * Some other related functions are in the Socket module
 */
#include "SocketBuffer.h"
#include "LinkedList.h"
#include "Log.h"
#include "Messages.h"
#include "StackTrace.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "Heap.h"

#if defined(_WIN32) || defined(_WIN64)
#define iov_len len
#define iov_base buf
#endif

/**
 * Default input queue buffer
 */
static socket_queue* def_queue;

/**
 * List of queued input buffers
 */
static List* queues;

/**
 * List of queued write buffers
 */
static List writes;


int socketcompare(void* a, void* b);
int SocketBuffer_newDefQ(void);
void SocketBuffer_freeDefQ(void);
int pending_socketcompare(void* a, void* b);


/**
 * List callback function for comparing socket_queues by socket
 * @param a first integer value
 * @param b second integer value
 * @return boolean indicating whether a and b are equal
 */
int socketcompare(void* a, void* b)
{
	return ((socket_queue*)a)->socket == *(int*)b;
}


/**
 * Create a new default queue when one has just been used.
 */
int SocketBuffer_newDefQ(void)
{
	int rc = PAHO_MEMORY_ERROR;

	def_queue = malloc(sizeof(socket_queue));
	if (def_queue)
	{
		def_queue->buflen = 1000;
		def_queue->buf = malloc(def_queue->buflen);
		if (def_queue->buf)
		{
			def_queue->socket = def_queue->index = 0;
			def_queue->buflen = def_queue->datalen = def_queue->headerlen = 0;
			rc = 0;
		}
	}
	return rc;
}


/**
 * Initialize the socketBuffer module
 */
int SocketBuffer_initialize(void)
{
	int rc = 0;

	FUNC_ENTRY;
	rc = SocketBuffer_newDefQ();
	if (rc == 0)
	{
		if ((queues = ListInitialize()) == NULL)
			rc = PAHO_MEMORY_ERROR;
	}
	ListZero(&writes);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Free the default queue memory
 */
void SocketBuffer_freeDefQ(void)
{
	free(def_queue->buf);
	free(def_queue);
        def_queue = NULL;
}


/**
 * Terminate the socketBuffer module
 */
void SocketBuffer_terminate(void)
{
	ListElement* cur = NULL;
	ListEmpty(&writes);

	FUNC_ENTRY;
	while (ListNextElement(queues, &cur))
		free(((socket_queue*)(cur->content))->buf);
	ListFree(queues);
	SocketBuffer_freeDefQ();
	FUNC_EXIT;
}


/**
 * Cleanup any buffers for a specific socket
 * @param socket the socket to clean up
 */
void SocketBuffer_cleanup(int socket)
{
	FUNC_ENTRY;
	SocketBuffer_writeComplete(socket); /* clean up write buffers */
	if (ListFindItem(queues, &socket, socketcompare))
	{
		free(((socket_queue*)(queues->current->content))->buf);
		ListRemove(queues, queues->current->content);
	}
	if (def_queue->socket == socket)
	{
		def_queue->socket = def_queue->index = 0;
		def_queue->headerlen = def_queue->datalen = 0;
	}
	FUNC_EXIT;
}


/**
 * Get any queued data for a specific socket
 * @param socket the socket to get queued data for
 * @param bytes the number of bytes of data to retrieve
 * @param actual_len the actual length returned
 * @return the actual data
 */
char* SocketBuffer_getQueuedData(int socket, size_t bytes, size_t* actual_len)
{
	socket_queue* queue = NULL;

	FUNC_ENTRY;
	if (ListFindItem(queues, &socket, socketcompare))
	{  /* if there is queued data for this socket, add any data read to it */
		queue = (socket_queue*)(queues->current->content);
		*actual_len = queue->datalen;
	}
	else
	{
		*actual_len = 0;
		queue = def_queue;
	}
	if (bytes > queue->buflen)
	{
		if (queue->datalen > 0)
		{
			void* newmem = malloc(bytes);

			free(queue->buf);
			queue->buf = newmem;
			if (!newmem)
				goto exit;
			memcpy(newmem, queue->buf, queue->datalen);
		}
		else
			queue->buf = realloc(queue->buf, bytes);
		queue->buflen = bytes;
	}
exit:
	FUNC_EXIT;
	return queue->buf;
}


/**
 * Get any queued character for a specific socket
 * @param socket the socket to get queued data for
 * @param c the character returned if any
 * @return completion code
 */
int SocketBuffer_getQueuedChar(int socket, char* c)
{
	int rc = SOCKETBUFFER_INTERRUPTED;

	FUNC_ENTRY;
	if (ListFindItem(queues, &socket, socketcompare))
	{  /* if there is queued data for this socket, read that first */
		socket_queue* queue = (socket_queue*)(queues->current->content);
		if (queue->index < queue->headerlen)
		{
			*c = queue->fixed_header[(queue->index)++];
			Log(TRACE_MAX, -1, "index is now %d, headerlen %d", queue->index, (int)queue->headerlen);
			rc = SOCKETBUFFER_COMPLETE;
			goto exit;
		}
		else if (queue->index > 4)
		{
			Log(LOG_FATAL, -1, "header is already at full length");
			rc = SOCKET_ERROR;
			goto exit;
		}
	}
exit:
	FUNC_EXIT_RC(rc);
	return rc;  /* there was no queued char if rc is SOCKETBUFFER_INTERRUPTED*/
}


/**
 * A socket read was interrupted so we need to queue data
 * @param socket the socket to get queued data for
 * @param actual_len the actual length of data that was read
 */
void SocketBuffer_interrupted(int socket, size_t actual_len)
{
	socket_queue* queue = NULL;

	FUNC_ENTRY;
	if (ListFindItem(queues, &socket, socketcompare))
		queue = (socket_queue*)(queues->current->content);
	else /* new saved queue */
	{
		queue = def_queue;
		/* if SocketBuffer_queueChar() has not yet been called, then the socket number
		  in def_queue will not have been set.  Issue #244.
		  If actual_len == 0 then we may not need to do anything - I'll leave that
		  optimization for another time. */
		queue->socket = socket;
		ListAppend(queues, def_queue, sizeof(socket_queue)+def_queue->buflen);
		SocketBuffer_newDefQ();
	}
	queue->index = 0;
	queue->datalen = actual_len;
	FUNC_EXIT;
}


/**
 * A socket read has now completed so we can get rid of the queue
 * @param socket the socket for which the operation is now complete
 * @return pointer to the default queue data
 */
char* SocketBuffer_complete(int socket)
{
	FUNC_ENTRY;
	if (ListFindItem(queues, &socket, socketcompare))
	{
		socket_queue* queue = (socket_queue*)(queues->current->content);
		SocketBuffer_freeDefQ();
		def_queue = queue;
		ListDetach(queues, queue);
	}
	def_queue->socket = def_queue->index = 0;
	def_queue->headerlen = def_queue->datalen = 0;
	FUNC_EXIT;
	return def_queue->buf;
}


/**
 * Queued a Charactor to a specific socket
 * @param socket the socket for which to queue char for
 * @param c the character to queue
 */
void SocketBuffer_queueChar(int socket, char c)
{
	int error = 0;
	socket_queue* curq = def_queue;

	FUNC_ENTRY;
	if (ListFindItem(queues, &socket, socketcompare))
		curq = (socket_queue*)(queues->current->content);
	else if (def_queue->socket == 0)
	{
		def_queue->socket = socket;
		def_queue->index = 0;
		def_queue->datalen = 0;
	}
	else if (def_queue->socket != socket)
	{
		Log(LOG_FATAL, -1, "attempt to reuse socket queue");
		error = 1;
	}
	if (curq->index > 4)
	{
		Log(LOG_FATAL, -1, "socket queue fixed_header field full");
		error = 1;
	}
	if (!error)
	{
		curq->fixed_header[(curq->index)++] = c;
		curq->headerlen = curq->index;
	}
	Log(TRACE_MAX, -1, "queueChar: index is now %d, headerlen %d", curq->index, (int)curq->headerlen);
	FUNC_EXIT;
}


/**
 * A socket write was interrupted so store the remaining data
 * @param socket the socket for which the write was interrupted
 * @param count the number of iovec buffers
 * @param iovecs buffer array
 * @param frees a set of flags indicating which of the iovecs array should be freed
 * @param total total data length to be written
 * @param bytes actual data length that was written
 */
#if defined(OPENSSL)
int SocketBuffer_pendingWrite(int socket, SSL* ssl, int count, iobuf* iovecs, int* frees, size_t total, size_t bytes)
#else
int SocketBuffer_pendingWrite(int socket, int count, iobuf* iovecs, int* frees, size_t total, size_t bytes)
#endif
{
	int i = 0;
	pending_writes* pw = NULL;
	int rc = 0;

	FUNC_ENTRY;
	/* store the buffers until the whole packet is written */
	if ((pw = malloc(sizeof(pending_writes))) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	pw->socket = socket;
#if defined(OPENSSL)
	pw->ssl = ssl;
#endif
	pw->bytes = bytes;
	pw->total = total;
	pw->count = count;
	for (i = 0; i < count; i++)
	{
		pw->iovecs[i] = iovecs[i];
		pw->frees[i] = frees[i];
	}
	ListAppend(&writes, pw, sizeof(pw) + total);
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * List callback function for comparing pending_writes by socket
 * @param a first integer value
 * @param b second integer value
 * @return boolean indicating whether a and b are equal
 */
int pending_socketcompare(void* a, void* b)
{
	return ((pending_writes*)a)->socket == *(int*)b;
}


/**
 * Get any queued write data for a specific socket
 * @param socket the socket to get queued data for
 * @return pointer to the queued data or NULL
 */
pending_writes* SocketBuffer_getWrite(int socket)
{
	ListElement* le = ListFindItem(&writes, &socket, pending_socketcompare);
	return (le) ? (pending_writes*)(le->content) : NULL;
}


/**
 * A socket write has now completed so we can get rid of the queue
 * @param socket the socket for which the operation is now complete
 * @return completion code, boolean - was the queue removed?
 */
int SocketBuffer_writeComplete(int socket)
{
	return ListRemoveItem(&writes, &socket, pending_socketcompare);
}


/**
 * Update the queued write data for a socket in the case of QoS 0 messages.
 * @param socket the socket for which the operation is now complete
 * @param topic the topic of the QoS 0 write
 * @param payload the payload of the QoS 0 write
 * @return pointer to the updated queued data structure, or NULL
 */
pending_writes* SocketBuffer_updateWrite(int socket, char* topic, char* payload)
{
	pending_writes* pw = NULL;
	ListElement* le = NULL;

	FUNC_ENTRY;
	if ((le = ListFindItem(&writes, &socket, pending_socketcompare)) != NULL)
	{
		pw = (pending_writes*)(le->content);
		if (pw->count == 4)
		{
			pw->iovecs[2].iov_base = topic;
			pw->iovecs[3].iov_base = payload;
		}
	}

	FUNC_EXIT;
	return pw;
}
