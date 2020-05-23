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
 *******************************************************************************/

#if !defined(SOCKETBUFFER_H)
#define SOCKETBUFFER_H

#if defined(_WIN32) || defined(_WIN64)
#include <winsock2.h>
#else
#include <sys/socket.h>
#endif

#if defined(OPENSSL)
#include <openssl/ssl.h>
#endif

#if defined(_WIN32) || defined(_WIN64)
	typedef WSABUF iobuf;
#else
	typedef struct iovec iobuf;
#endif

typedef struct
{
	int socket;
	unsigned int index;
	size_t headerlen;
	char fixed_header[5];	/**< header plus up to 4 length bytes */
	size_t buflen, 			/**< total length of the buffer */
		datalen; 			/**< current length of data in buf */
	char* buf;
} socket_queue;

typedef struct
{
	int socket, count;
	size_t total;
#if defined(OPENSSL)
	SSL* ssl;
#endif
	size_t bytes;
	iobuf iovecs[5];
	int frees[5];
} pending_writes;

#define SOCKETBUFFER_COMPLETE 0
#if !defined(SOCKET_ERROR)
	#define SOCKET_ERROR -1
#endif
#define SOCKETBUFFER_INTERRUPTED -22 /* must be the same value as TCPSOCKET_INTERRUPTED */

int SocketBuffer_initialize(void);
void SocketBuffer_terminate(void);
void SocketBuffer_cleanup(int socket);
char* SocketBuffer_getQueuedData(int socket, size_t bytes, size_t* actual_len);
int SocketBuffer_getQueuedChar(int socket, char* c);
void SocketBuffer_interrupted(int socket, size_t actual_len);
char* SocketBuffer_complete(int socket);
void SocketBuffer_queueChar(int socket, char c);

#if defined(OPENSSL)
int SocketBuffer_pendingWrite(int socket, SSL* ssl, int count, iobuf* iovecs, int* frees, size_t total, size_t bytes);
#else
int SocketBuffer_pendingWrite(int socket, int count, iobuf* iovecs, int* frees, size_t total, size_t bytes);
#endif
pending_writes* SocketBuffer_getWrite(int socket);
int SocketBuffer_writeComplete(int socket);
pending_writes* SocketBuffer_updateWrite(int socket, char* topic, char* payload);

#endif
