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
 *    Ian Craggs - initial implementation and documentation
 *    Ian Craggs - async client updates
 *    Ian Craggs - fix for bug 484496
 *    Juergen Kosel, Ian Craggs - fix for issue #135
 *    Ian Craggs - issue #217
 *    Ian Craggs - fix for issue #186
 *    Ian Craggs - remove StackTrace print debugging calls
 *******************************************************************************/

/**
 * @file
 * \brief Socket related functions
 *
 * Some other related functions are in the SocketBuffer module
 */


#include "Socket.h"
#include "Log.h"
#include "SocketBuffer.h"
#include "Messages.h"
#include "StackTrace.h"
#if defined(OPENSSL)
#include "SSLSocket.h"
#endif

#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <ctype.h>

#include "Heap.h"

int Socket_setnonblocking(int sock);
int Socket_error(char* aString, int sock);
int Socket_addSocket(int newSd);
int isReady(int socket, fd_set* read_set, fd_set* write_set);
int Socket_writev(int socket, iobuf* iovecs, int count, unsigned long* bytes);
int Socket_close_only(int socket);
int Socket_continueWrite(int socket);
int Socket_continueWrites(fd_set* pwset);
char* Socket_getaddrname(struct sockaddr* sa, int sock);
int Socket_abortWrite(int socket);

#if defined(_WIN32) || defined(_WIN64)
#define iov_len len
#define iov_base buf
#endif

/**
 * Structure to hold all socket data for the module
 */
Sockets s;
static fd_set wset;

/**
 * Set a socket non-blocking, OS independently
 * @param sock the socket to set non-blocking
 * @return TCP call error code
 */
int Socket_setnonblocking(int sock)
{
	int rc;
#if defined(_WIN32) || defined(_WIN64)
	u_long flag = 1L;

	FUNC_ENTRY;
	rc = ioctl(sock, FIONBIO, &flag);
#else
	int flags;

	FUNC_ENTRY;
	if ((flags = fcntl(sock, F_GETFL, 0)))
		flags = 0;
	rc = fcntl(sock, F_SETFL, flags | O_NONBLOCK);
#endif
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Gets the specific error corresponding to SOCKET_ERROR
 * @param aString the function that was being used when the error occurred
 * @param sock the socket on which the error occurred
 * @return the specific TCP error code
 */
int Socket_error(char* aString, int sock)
{
	int err;

#if defined(_WIN32) || defined(_WIN64)
	err = WSAGetLastError();
#else
	err = errno;
#endif
	if (err != EINTR && err != EAGAIN && err != EINPROGRESS && err != EWOULDBLOCK)
	{
		if (strcmp(aString, "shutdown") != 0 || (err != ENOTCONN && err != ECONNRESET))
			Log(TRACE_MINIMUM, -1, "Socket error %s(%d) in %s for socket %d", strerror(err), err, aString, sock);
	}
	return err;
}


/**
 * Initialize the socket module
 */
void Socket_outInitialize(void)
{
#if defined(_WIN32) || defined(_WIN64)
	WORD    winsockVer = 0x0202;
	WSADATA wsd;

	FUNC_ENTRY;
	WSAStartup(winsockVer, &wsd);
#else
	FUNC_ENTRY;
	signal(SIGPIPE, SIG_IGN);
#endif

	SocketBuffer_initialize();
	s.clientsds = ListInitialize();
	s.connect_pending = ListInitialize();
	s.write_pending = ListInitialize();
	s.cur_clientsds = NULL;
	FD_ZERO(&(s.rset));														/* Initialize the descriptor set */
	FD_ZERO(&(s.pending_wset));
	s.maxfdp1 = 0;
	memcpy((void*)&(s.rset_saved), (void*)&(s.rset), sizeof(s.rset_saved));
	FUNC_EXIT;
}


/**
 * Terminate the socket module
 */
void Socket_outTerminate(void)
{
	FUNC_ENTRY;
	ListFree(s.connect_pending);
	ListFree(s.write_pending);
	ListFree(s.clientsds);
	SocketBuffer_terminate();
#if defined(_WIN32) || defined(_WIN64)
	WSACleanup();
#endif
	FUNC_EXIT;
}


/**
 * Add a socket to the list of socket to check with select
 * @param newSd the new socket to add
 */
int Socket_addSocket(int newSd)
{
	int rc = 0;

	FUNC_ENTRY;
	if (ListFindItem(s.clientsds, &newSd, intcompare) == NULL) /* make sure we don't add the same socket twice */
	{
		if (s.clientsds->count >= FD_SETSIZE)
		{
			Log(LOG_ERROR, -1, "addSocket: exceeded FD_SETSIZE %d", FD_SETSIZE);
			rc = SOCKET_ERROR;
		}
		else
		{
			int* pnewSd = (int*)malloc(sizeof(newSd));

			if (!pnewSd)
			{
				rc = PAHO_MEMORY_ERROR;
				goto exit;
			}
			*pnewSd = newSd;
			if (!ListAppend(s.clientsds, pnewSd, sizeof(newSd)))
			{
				free(pnewSd);
				rc = PAHO_MEMORY_ERROR;
				goto exit;
			}
			FD_SET(newSd, &(s.rset_saved));
			s.maxfdp1 = max(s.maxfdp1, newSd + 1);
			rc = Socket_setnonblocking(newSd);
			if (rc == SOCKET_ERROR)
				Log(LOG_ERROR, -1, "addSocket: setnonblocking");
		}
	}
	else
		Log(LOG_ERROR, -1, "addSocket: socket %d already in the list", newSd);

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 * Don't accept work from a client unless it is accepting work back, i.e. its socket is writeable
 * this seems like a reasonable form of flow control, and practically, seems to work.
 * @param socket the socket to check
 * @param read_set the socket read set (see select doc)
 * @param write_set the socket write set (see select doc)
 * @return boolean - is the socket ready to go?
 */
int isReady(int socket, fd_set* read_set, fd_set* write_set)
{
	int rc = 1;

	FUNC_ENTRY;
	if  (ListFindItem(s.connect_pending, &socket, intcompare) && FD_ISSET(socket, write_set))
		ListRemoveItem(s.connect_pending, &socket, intcompare);
	else
		rc = FD_ISSET(socket, read_set) && FD_ISSET(socket, write_set) && Socket_noPendingWrites(socket);
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 *  Returns the next socket ready for communications as indicated by select
 *  @param more_work flag to indicate more work is waiting, and thus a timeout value of 0 should
 *  be used for the select
 *  @param tp the timeout to be used for the select, unless overridden
 *  @return the socket next ready, or 0 if none is ready
 */
int Socket_getReadySocket(int more_work, struct timeval *tp, mutex_type mutex)
{
	int rc = 0;
	static struct timeval zero = {0L, 0L}; /* 0 seconds */
	static struct timeval one = {1L, 0L}; /* 1 second */
	struct timeval timeout = one;

	FUNC_ENTRY;
	Thread_lock_mutex(mutex);
	if (s.clientsds->count == 0)
		goto exit;

	if (more_work)
		timeout = zero;
	else if (tp)
		timeout = *tp;

	while (s.cur_clientsds != NULL)
	{
		if (isReady(*((int*)(s.cur_clientsds->content)), &(s.rset), &wset))
			break;
		ListNextElement(s.clientsds, &s.cur_clientsds);
	}

	if (s.cur_clientsds == NULL)
	{
		int rc1;
		fd_set pwset;

		memcpy((void*)&(s.rset), (void*)&(s.rset_saved), sizeof(s.rset));
		memcpy((void*)&(pwset), (void*)&(s.pending_wset), sizeof(pwset));
		/* Prevent performance issue by unlocking the socket_mutex while waiting for a ready socket. */
		Thread_unlock_mutex(mutex);
		rc = select(s.maxfdp1, &(s.rset), &pwset, NULL, &timeout);
		Thread_lock_mutex(mutex);
		if (rc == SOCKET_ERROR)
		{
			Socket_error("read select", 0);
			goto exit;
		}
		Log(TRACE_MAX, -1, "Return code %d from read select", rc);

		if (Socket_continueWrites(&pwset) == SOCKET_ERROR)
		{
			rc = 0;
			goto exit;
		}

		memcpy((void*)&wset, (void*)&(s.rset_saved), sizeof(wset));
		if ((rc1 = select(s.maxfdp1, NULL, &(wset), NULL, &zero)) == SOCKET_ERROR)
		{
			Socket_error("write select", 0);
			rc = rc1;
			goto exit;
		}
		Log(TRACE_MAX, -1, "Return code %d from write select", rc1);

		if (rc == 0 && rc1 == 0)
			goto exit; /* no work to do */

		s.cur_clientsds = s.clientsds->first;
		while (s.cur_clientsds != NULL)
		{
			int cursock = *((int*)(s.cur_clientsds->content));
			if (isReady(cursock, &(s.rset), &wset))
				break;
			ListNextElement(s.clientsds, &s.cur_clientsds);
		}
	}

	if (s.cur_clientsds == NULL)
		rc = 0;
	else
	{
		rc = *((int*)(s.cur_clientsds->content));
		ListNextElement(s.clientsds, &s.cur_clientsds);
	}
exit:
	Thread_unlock_mutex(mutex);
	FUNC_EXIT_RC(rc);
	return rc;
} /* end getReadySocket */


/**
 *  Reads one byte from a socket
 *  @param socket the socket to read from
 *  @param c the character read, returned
 *  @return completion code
 */
int Socket_getch(int socket, char* c)
{
	int rc = SOCKET_ERROR;

	FUNC_ENTRY;
	if ((rc = SocketBuffer_getQueuedChar(socket, c)) != SOCKETBUFFER_INTERRUPTED)
		goto exit;

	if ((rc = recv(socket, c, (size_t)1, 0)) == SOCKET_ERROR)
	{
		int err = Socket_error("recv - getch", socket);
		if (err == EWOULDBLOCK || err == EAGAIN)
		{
			rc = TCPSOCKET_INTERRUPTED;
			SocketBuffer_interrupted(socket, 0);
		}
	}
	else if (rc == 0)
		rc = SOCKET_ERROR; 	/* The return value from recv is 0 when the peer has performed an orderly shutdown. */
	else if (rc == 1)
	{
		SocketBuffer_queueChar(socket, *c);
		rc = TCPSOCKET_COMPLETE;
	}
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 *  Attempts to read a number of bytes from a socket, non-blocking. If a previous read did not
 *  finish, then retrieve that data.
 *  @param socket the socket to read from
 *  @param bytes the number of bytes to read
 *  @param actual_len the actual number of bytes read
 *  @return completion code
 */
char *Socket_getdata(int socket, size_t bytes, size_t* actual_len)
{
	int rc;
	char* buf;

	FUNC_ENTRY;
	if (bytes == 0)
	{
		buf = SocketBuffer_complete(socket);
		goto exit;
	}

	buf = SocketBuffer_getQueuedData(socket, bytes, actual_len);

	if ((rc = recv(socket, buf + (*actual_len), (int)(bytes - (*actual_len)), 0)) == SOCKET_ERROR)
	{
		rc = Socket_error("recv - getdata", socket);
		if (rc != EAGAIN && rc != EWOULDBLOCK)
		{
			buf = NULL;
			goto exit;
		}
	}
	else if (rc == 0) /* rc 0 means the other end closed the socket, albeit "gracefully" */
	{
		buf = NULL;
		goto exit;
	}
	else
		*actual_len += rc;

	if (*actual_len == bytes)
		SocketBuffer_complete(socket);
	else /* we didn't read the whole packet */
	{
		SocketBuffer_interrupted(socket, *actual_len);
		Log(TRACE_MAX, -1, "%d bytes expected but %d bytes now received", (int)bytes, (int)*actual_len);
	}
exit:
	FUNC_EXIT;
	return buf;
}


/**
 *  Indicate whether any data is pending outbound for a socket.
 *  @return boolean - true == data pending.
 */
int Socket_noPendingWrites(int socket)
{
	int cursock = socket;
	return ListFindItem(s.write_pending, &cursock, intcompare) == NULL;
}


/**
 *  Attempts to write a series of iovec buffers to a socket in *one* system call so that
 *  they are sent as one packet.
 *  @param socket the socket to write to
 *  @param iovecs an array of buffers to write
 *  @param count number of buffers in iovecs
 *  @param bytes number of bytes actually written returned
 *  @return completion code, especially TCPSOCKET_INTERRUPTED
 */
int Socket_writev(int socket, iobuf* iovecs, int count, unsigned long* bytes)
{
	int rc;

	FUNC_ENTRY;
	*bytes = 0L;
#if defined(_WIN32) || defined(_WIN64)
	rc = WSASend(socket, iovecs, count, (LPDWORD)bytes, 0, NULL, NULL);
	if (rc == SOCKET_ERROR)
	{
		int err = Socket_error("WSASend - putdatas", socket);
		if (err == EWOULDBLOCK || err == EAGAIN)
			rc = TCPSOCKET_INTERRUPTED;
	}
#else
/*#define TCPSOCKET_INTERRUPTED_TESTING
This section forces the occasional return of TCPSOCKET_INTERRUPTED,
for testing purposes only!
*/
#if defined(TCPSOCKET_INTERRUPTED_TESTING)
  static int i = 0;
	if (++i >= 10 && i < 21)
	{
		if (1)
		{
		  printf("Deliberately simulating TCPSOCKET_INTERRUPTED\n");
		  rc = TCPSOCKET_INTERRUPTED; /* simulate a network wait */
	  }
		else
		{
			printf("Deliberately simulating SOCKET_ERROR\n");
		  rc = SOCKET_ERROR;
		}
		/* should *bytes always be 0? */
		if (i == 20)
		{
		  printf("Shutdown socket\n");
		  shutdown(socket, SHUT_WR);
	  }
	}
	else
	{
#endif
	rc = writev(socket, iovecs, count);
	if (rc == SOCKET_ERROR)
	{
		int err = Socket_error("writev - putdatas", socket);
		if (err == EWOULDBLOCK || err == EAGAIN)
			rc = TCPSOCKET_INTERRUPTED;
	}
	else
		*bytes = rc;
#if defined(TCPSOCKET_INTERRUPTED_TESTING)
	}
#endif
#endif
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 *  Attempts to write a series of buffers to a socket in *one* system call so that they are
 *  sent as one packet.
 *  @param socket the socket to write to
 *  @param buf0 the first buffer
 *  @param buf0len the length of data in the first buffer
 *  @param count number of buffers
 *  @param buffers an array of buffers to write
 *  @param buflens an array of corresponding buffer lengths
 *  @return completion code, especially TCPSOCKET_INTERRUPTED
 */
int Socket_putdatas(int socket, char* buf0, size_t buf0len, int count, char** buffers, size_t* buflens, int* frees)
{
	unsigned long bytes = 0L;
	iobuf iovecs[5];
	int frees1[5];
	int rc = TCPSOCKET_INTERRUPTED, i;
	size_t total = buf0len;

	FUNC_ENTRY;
	if (!Socket_noPendingWrites(socket))
	{
		Log(LOG_SEVERE, -1, "Trying to write to socket %d for which there is already pending output", socket);
		rc = SOCKET_ERROR;
		goto exit;
	}

	for (i = 0; i < count; i++)
		total += buflens[i];

	iovecs[0].iov_base = buf0;
	iovecs[0].iov_len = (ULONG)buf0len;
	frees1[0] = 1; /* this buffer should be freed by SocketBuffer if the write is interrupted */
	for (i = 0; i < count; i++)
	{
		iovecs[i+1].iov_base = buffers[i];
		iovecs[i+1].iov_len = (ULONG)buflens[i];
		frees1[i+1] = frees[i];
	}

	if ((rc = Socket_writev(socket, iovecs, count+1, &bytes)) != SOCKET_ERROR)
	{
		if (bytes == total)
			rc = TCPSOCKET_COMPLETE;
		else
		{
			int* sockmem = (int*)malloc(sizeof(int));

			if (!sockmem)
			{
				rc = PAHO_MEMORY_ERROR;
				goto exit;
			}
			Log(TRACE_MIN, -1, "Partial write: %lu bytes of %lu actually written on socket %d",
					bytes, total, socket);
#if defined(OPENSSL)
			SocketBuffer_pendingWrite(socket, NULL, count+1, iovecs, frees1, total, bytes);
#else
			SocketBuffer_pendingWrite(socket, count+1, iovecs, frees1, total, bytes);
#endif
			*sockmem = socket;
			if (!ListAppend(s.write_pending, sockmem, sizeof(int)))
			{
				free(sockmem);
				rc = PAHO_MEMORY_ERROR;
				goto exit;
			}
			FD_SET(socket, &(s.pending_wset));
			rc = TCPSOCKET_INTERRUPTED;
		}
	}
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 *  Add a socket to the pending write list, so that it is checked for writing in select.  This is used
 *  in connect processing when the TCP connect is incomplete, as we need to check the socket for both
 *  ready to read and write states.
 *  @param socket the socket to add
 */
void Socket_addPendingWrite(int socket)
{
	FD_SET(socket, &(s.pending_wset));
}


/**
 *  Clear a socket from the pending write list - if one was added with Socket_addPendingWrite
 *  @param socket the socket to remove
 */
void Socket_clearPendingWrite(int socket)
{
	if (FD_ISSET(socket, &(s.pending_wset)))
		FD_CLR(socket, &(s.pending_wset));
}


/**
 *  Close a socket without removing it from the select list.
 *  @param socket the socket to close
 *  @return completion code
 */
int Socket_close_only(int socket)
{
	int rc;

	FUNC_ENTRY;
#if defined(_WIN32) || defined(_WIN64)
	if (shutdown(socket, SD_BOTH) == SOCKET_ERROR)
		Socket_error("shutdown", socket);
	if ((rc = closesocket(socket)) == SOCKET_ERROR)
		Socket_error("close", socket);
#else
	if (shutdown(socket, SHUT_WR) == SOCKET_ERROR)
		Socket_error("shutdown", socket);
	if ((rc = recv(socket, NULL, (size_t)0, 0)) == SOCKET_ERROR)
		Socket_error("shutdown", socket);
	if ((rc = close(socket)) == SOCKET_ERROR)
		Socket_error("close", socket);
#endif
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 *  Close a socket and remove it from the select list.
 *  @param socket the socket to close
 *  @return completion code
 */
void Socket_close(int socket)
{
	FUNC_ENTRY;
	Socket_close_only(socket);
	FD_CLR(socket, &(s.rset_saved));
	if (FD_ISSET(socket, &(s.pending_wset)))
		FD_CLR(socket, &(s.pending_wset));
	if (s.cur_clientsds != NULL && *(int*)(s.cur_clientsds->content) == socket)
		s.cur_clientsds = s.cur_clientsds->next;
	Socket_abortWrite(socket);
	SocketBuffer_cleanup(socket);
	ListRemoveItem(s.connect_pending, &socket, intcompare);
	ListRemoveItem(s.write_pending, &socket, intcompare);

	if (ListRemoveItem(s.clientsds, &socket, intcompare))
		Log(TRACE_MIN, -1, "Removed socket %d", socket);
	else
		Log(LOG_ERROR, -1, "Failed to remove socket %d", socket);
	if (socket + 1 >= s.maxfdp1)
	{
		/* now we have to reset s.maxfdp1 */
		ListElement* cur_clientsds = NULL;

		s.maxfdp1 = 0;
		while (ListNextElement(s.clientsds, &cur_clientsds))
			s.maxfdp1 = max(*((int*)(cur_clientsds->content)), s.maxfdp1);
		++(s.maxfdp1);
		Log(TRACE_MAX, -1, "Reset max fdp1 to %d", s.maxfdp1);
	}
	FUNC_EXIT;
}


/**
 *  Create a new socket and TCP connect to an address/port
 *  @param addr the address string
 *  @param port the TCP port
 *  @param sock returns the new socket
 *  @param timeout the timeout in milliseconds
 *  @return completion code
 */
#if defined(__GNUC__) && defined(__linux__)
int Socket_new(const char* addr, size_t addr_len, int port, int* sock, long timeout)
#else
int Socket_new(const char* addr, size_t addr_len, int port, int* sock)
#endif
{
	int type = SOCK_STREAM;
	char *addr_mem;
	struct sockaddr_in address;
#if defined(AF_INET6)
	struct sockaddr_in6 address6;
#endif
	int rc = SOCKET_ERROR;
#if defined(_WIN32) || defined(_WIN64)
	short family;
#else
	sa_family_t family = AF_INET;
#endif
	struct addrinfo *result = NULL;
	struct addrinfo hints = {0, AF_UNSPEC, SOCK_STREAM, IPPROTO_TCP, 0, NULL, NULL, NULL};

	FUNC_ENTRY;
	*sock = -1;
	memset(&address6, '\0', sizeof(address6));

	if (addr[0] == '[')
	{
		++addr;
		--addr_len;
	}

	if ((addr_mem = malloc( addr_len + 1u )) == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
	memcpy( addr_mem, addr, addr_len );
	addr_mem[addr_len] = '\0';

//#if defined(__GNUC__) && defined(__linux__)
#if 0
	struct gaicb ar = {addr_mem, NULL, &hints, NULL};
	struct gaicb *reqs[] = {&ar};

	unsigned long int seconds = timeout / 1000L;
	unsigned long int nanos = (timeout - (seconds * 1000L)) * 1000000L;
	struct timespec timeoutspec = {seconds, nanos};

	rc = getaddrinfo_a(GAI_NOWAIT, reqs, 1, NULL);
	if (rc == 0)
		rc = gai_suspend((const struct gaicb* const *) reqs, 1, &timeoutspec);

	if (rc == 0)
	{
		rc = gai_error(reqs[0]);
		result = ar.ar_result;
	}
#else
	rc = getaddrinfo(addr_mem, NULL, &hints, &result);
#endif

	if (rc == 0)
	{
		struct addrinfo* res = result;

		while (res)
		{	/* prefer ip4 addresses */
			if (res->ai_family == AF_INET || res->ai_next == NULL)
				break;
			res = res->ai_next;
		}

		if (res == NULL)
			rc = -1;
		else
#if defined(AF_INET6)
		if (res->ai_family == AF_INET6)
		{
			address6.sin6_port = htons(port);
			address6.sin6_family = family = AF_INET6;
			memcpy(&address6.sin6_addr, &((struct sockaddr_in6*)(res->ai_addr))->sin6_addr, sizeof(address6.sin6_addr));
		}
		else
#endif
		if (res->ai_family == AF_INET)
		{
			memset(&address.sin_zero, 0, sizeof(address.sin_zero));
			address.sin_port = htons(port);
			address.sin_family = family = AF_INET;
			address.sin_addr = ((struct sockaddr_in*)(res->ai_addr))->sin_addr;
		}
		else
			rc = -1;

		freeaddrinfo(result);
	}
	else
	  	Log(LOG_ERROR, -1, "getaddrinfo failed for addr %s with rc %d", addr_mem, rc);

	if (rc != 0)
		Log(LOG_ERROR, -1, "%s is not a valid IP address", addr_mem);
	else
	{
		*sock =	(int)socket(family, type, 0);
		if (*sock == INVALID_SOCKET)
			rc = Socket_error("socket", *sock);
		else
		{
#if defined(NOSIGPIPE)
			int opt = 1;

			if (setsockopt(*sock, SOL_SOCKET, SO_NOSIGPIPE, (void*)&opt, sizeof(opt)) != 0)
				Log(LOG_ERROR, -1, "Could not set SO_NOSIGPIPE for socket %d", *sock);
#endif
/*#define SMALL_TCP_BUFFER_TESTING
  This section sets the TCP send buffer to a small amount to provoke TCPSOCKET_INTERRUPTED
	return codes from send, for testing only!
*/
#if defined(SMALL_TCP_BUFFER_TESTING)
        if (1)
				{
					int optsend = 100; //2 * 1440;
					printf("Setting optsend to %d\n", optsend);
					if (setsockopt(*sock, SOL_SOCKET, SO_SNDBUF, (void*)&optsend, sizeof(optsend)) != 0)
						Log(LOG_ERROR, -1, "Could not set SO_SNDBUF for socket %d", *sock);
				}
#endif
			Log(TRACE_MIN, -1, "New socket %d for %s, port %d",	*sock, addr, port);
			if (Socket_addSocket(*sock) == SOCKET_ERROR)
				rc = Socket_error("addSocket", *sock);
			else
			{
				/* this could complete immmediately, even though we are non-blocking */
				if (family == AF_INET)
					rc = connect(*sock, (struct sockaddr*)&address, sizeof(address));
	#if defined(AF_INET6)
				else
					rc = connect(*sock, (struct sockaddr*)&address6, sizeof(address6));
	#endif
				if (rc == SOCKET_ERROR)
					rc = Socket_error("connect", *sock);
				if (rc == EINPROGRESS || rc == EWOULDBLOCK)
				{
					int* pnewSd = (int*)malloc(sizeof(int));

					if (!pnewSd)
					{
						rc = PAHO_MEMORY_ERROR;
						goto exit;
					}
					*pnewSd = *sock;
					if (!ListAppend(s.connect_pending, pnewSd, sizeof(int)))
					{
						free(pnewSd);
						rc = PAHO_MEMORY_ERROR;
						goto exit;
					}
					Log(TRACE_MIN, 15, "Connect pending");
				}
			}
            /* Prevent socket leak by closing unusable sockets,
               as reported in https://github.com/eclipse/paho.mqtt.c/issues/135 */
            if (rc != 0 && (rc != EINPROGRESS) && (rc != EWOULDBLOCK))
            {
            	Socket_close(*sock); /* close socket and remove from our list of sockets */
                *sock = -1; /* as initialized before */
            }
		}
	}

exit:
	if (addr_mem)
		free(addr_mem);

	FUNC_EXIT_RC(rc);
	return rc;
}


static Socket_writeComplete* writecomplete = NULL;

void Socket_setWriteCompleteCallback(Socket_writeComplete* mywritecomplete)
{
	writecomplete = mywritecomplete;
}



/**
 *  Continue an outstanding write for a particular socket
 *  @param socket that socket
 *  @return completion code: 0=incomplete, 1=complete, -1=socket error
 */
int Socket_continueWrite(int socket)
{
	int rc = 0;
	pending_writes* pw;
	unsigned long curbuflen = 0L, /* cumulative total of buffer lengths */
		bytes = 0L;
	int curbuf = -1, i;
	iobuf iovecs1[5];

	FUNC_ENTRY;
	pw = SocketBuffer_getWrite(socket);

#if defined(OPENSSL)
	if (pw->ssl)
	{
		rc = SSLSocket_continueWrite(pw);
		goto exit;
	}
#endif

	for (i = 0; i < pw->count; ++i)
	{
		if (pw->bytes <= curbuflen)
		{ /* if previously written length is less than the buffer we are currently looking at,
				add the whole buffer */
			iovecs1[++curbuf].iov_len = pw->iovecs[i].iov_len;
			iovecs1[curbuf].iov_base = pw->iovecs[i].iov_base;
		}
		else if (pw->bytes < curbuflen + pw->iovecs[i].iov_len)
		{ /* if previously written length is in the middle of the buffer we are currently looking at,
				add some of the buffer */
			size_t offset = pw->bytes - curbuflen;
			iovecs1[++curbuf].iov_len = pw->iovecs[i].iov_len - (ULONG)offset;
			iovecs1[curbuf].iov_base = (char*)pw->iovecs[i].iov_base + offset;
			break;
		}
		curbuflen += pw->iovecs[i].iov_len;
	}

	if ((rc = Socket_writev(socket, iovecs1, curbuf+1, &bytes)) != SOCKET_ERROR)
	{
		pw->bytes += bytes;
		if ((rc = (pw->bytes == pw->total)))
		{  /* topic and payload buffers are freed elsewhere, when all references to them have been removed */
			for (i = 0; i < pw->count; i++)
			{
				if (pw->frees[i])
                                {
					free(pw->iovecs[i].iov_base);
                                        pw->iovecs[i].iov_base = NULL;
                                }
			}
			rc = 1; /* signal complete */
			Log(TRACE_MIN, -1, "ContinueWrite: partial write now complete for socket %d", socket);
		}
		else
		{
			rc = 0; /* signal not complete */
			Log(TRACE_MIN, -1, "ContinueWrite wrote +%lu bytes on socket %d", bytes, socket);
		}
	}
	else /* if we got SOCKET_ERROR we need to clean up anyway - a partial write is no good anymore */
	{
		for (i = 0; i < pw->count; i++)
		{
			if (pw->frees[i])
                        {
				free(pw->iovecs[i].iov_base);
                                pw->iovecs[i].iov_base = NULL;
                        }
		}
	}
#if defined(OPENSSL)
exit:
#endif
	FUNC_EXIT_RC(rc);
	return rc;
}



/**
 *  Continue an outstanding write for a particular socket
 *  @param socket that socket
 *  @return completion code: 0=incomplete, 1=complete, -1=socket error
 */
int Socket_abortWrite(int socket)
{
	int i = -1, rc = 0;
	pending_writes* pw;

	FUNC_ENTRY;
	if ((pw = SocketBuffer_getWrite(socket)) == NULL)
	  goto exit;

#if defined(OPENSSL)
	if (pw->ssl)
		goto exit;
#endif

	for (i = 0; i < pw->count; i++)
	{
		if (pw->frees[i])
		{
			Log(TRACE_MIN, -1, "Cleaning in abortWrite for socket %d", socket);
			free(pw->iovecs[i].iov_base);
		}
	}
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}


/**
 *  Continue any outstanding writes for a socket set
 *  @param pwset the set of sockets
 *  @return completion code
 */
int Socket_continueWrites(fd_set* pwset)
{
	int rc1 = 0;
	ListElement* curpending = s.write_pending->first;

	FUNC_ENTRY;
	while (curpending && curpending->content)
	{
		int socket = *(int*)(curpending->content);
		int rc = 0;

		if (FD_ISSET(socket, pwset) && ((rc = Socket_continueWrite(socket)) != 0))
		{
			if (!SocketBuffer_writeComplete(socket))
				Log(LOG_SEVERE, -1, "Failed to remove pending write from socket buffer list");
			FD_CLR(socket, &(s.pending_wset));
			if (!ListRemove(s.write_pending, curpending->content))
			{
				Log(LOG_SEVERE, -1, "Failed to remove pending write from list");
				ListNextElement(s.write_pending, &curpending);
			}
			curpending = s.write_pending->current;

			if (writecomplete)
				(*writecomplete)(socket, rc);
		}
		else
			ListNextElement(s.write_pending, &curpending);
	}
	FUNC_EXIT_RC(rc1);
	return rc1;
}


/**
 *  Convert a numeric address to character string
 *  @param sa	socket numerical address
 *  @param sock socket
 *  @return the peer information
 */
char* Socket_getaddrname(struct sockaddr* sa, int sock)
{
/**
 * maximum length of the address string
 */
#define ADDRLEN INET6_ADDRSTRLEN+1
/**
 * maximum length of the port string
 */
#define PORTLEN 10
	static char addr_string[ADDRLEN + PORTLEN];

#if defined(_WIN32) || defined(_WIN64)
	int buflen = ADDRLEN*2;
	wchar_t buf[ADDRLEN*2];
	if (WSAAddressToStringW(sa, sizeof(struct sockaddr_in6), NULL, buf, (LPDWORD)&buflen) == SOCKET_ERROR)
		Socket_error("WSAAddressToString", sock);
	else
		wcstombs(addr_string, buf, sizeof(addr_string));
	/* TODO: append the port information - format: [00:00:00::]:port */
	/* strcpy(&addr_string[strlen(addr_string)], "what?"); */
#else
	struct sockaddr_in *sin = (struct sockaddr_in *)sa;
	inet_ntop(sin->sin_family, &sin->sin_addr, addr_string, ADDRLEN);
	sprintf(&addr_string[strlen(addr_string)], ":%d", ntohs(sin->sin_port));
#endif
	return addr_string;
}


/**
 *  Get information about the other end connected to a socket
 *  @param sock the socket to inquire on
 *  @return the peer information
 */
char* Socket_getpeer(int sock)
{
	struct sockaddr_in6 sa;
	socklen_t sal = sizeof(sa);

	if (getpeername(sock, (struct sockaddr*)&sa, &sal) == SOCKET_ERROR)
	{
		Socket_error("getpeername", sock);
		return "unknown";
	}

	return Socket_getaddrname((struct sockaddr*)&sa, sock);
}


#if defined(Socket_TEST)

int main(int argc, char *argv[])
{
	Socket_connect("127.0.0.1", 1883);
	Socket_connect("localhost", 1883);
	Socket_connect("loadsadsacalhost", 1883);
}

#endif
