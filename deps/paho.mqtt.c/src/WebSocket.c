/*******************************************************************************
 * Copyright (c) 2018, 2020 Wind River Systems, Inc. and others. All Rights Reserved.
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
 *    Keith Holman - initial implementation and documentation
 *    Ian Craggs - use memory tracking
 *    Ian Craggs - fix for one MQTT packet spread over >1 ws frame
 *******************************************************************************/

#include <stdint.h>
#include <stdio.h>
#include <string.h>
// for timeout process in WebSocket_proxy_connect()
#include <time.h>
#if defined(_WIN32) || defined(_WIN64)
#include <windows.h>
#else
#include <unistd.h>
#endif

#include "WebSocket.h"

#include "Base64.h"
#include "Log.h"
#include "SHA1.h"
#include "LinkedList.h"
#include "MQTTProtocolOut.h"
#include "SocketBuffer.h"
#include "StackTrace.h"

#if defined(__linux__)
#  include <endian.h>
#elif defined(__APPLE__)
#  include <libkern/OSByteOrder.h>
#  define htobe16(x) OSSwapHostToBigInt16(x)
#  define htobe32(x) OSSwapHostToBigInt32(x)
#  define htobe64(x) OSSwapHostToBigInt64(x)
#  define be16toh(x) OSSwapBigToHostInt16(x)
#  define be32toh(x) OSSwapBigToHostInt32(x)
#  define be64toh(x) OSSwapBigToHostInt64(x)
#elif defined(__FreeBSD__) || defined(__NetBSD__)
#  include <sys/endian.h>
#elif defined(_WIN32) || defined(_WIN64)
#  pragma comment(lib, "rpcrt4.lib")
#  include <Rpc.h>
#  define strncasecmp(s1,s2,c) _strnicmp(s1,s2,c)
#  define htonll(x) _byteswap_uint64(x)
#  define ntohll(x) _byteswap_uint64(x)

#  if BYTE_ORDER == LITTLE_ENDIAN
#    define htobe16(x)   htons(x)
#    define htobe32(x)   htonl(x)
#    define htobe64(x)   htonll(x)
#    define be16toh(x)   ntohs(x)
#    define be32toh(x)   ntohl(x)
#    define be64toh(x)   ntohll(x)
#  elif BYTE_ORDER == BIG_ENDIAN
#    define htobe16(x)   (x)
#    define htobe32(x)   (x)
#    define htobe64(x)   (x)
#    define be16toh(x)   (x)
#    define be32toh(x)   (x)
#    define be64toh(x)   (x)
#  else
#    error "unknown endian"
#  endif
   /* For Microsoft Visual Studio < 2015 */
#  if defined(_MSC_VER) && _MSC_VER < 1900
#    define snprintf _snprintf
#  endif
#endif

#if defined(OPENSSL)
#include "SSLSocket.h"
#include <openssl/rand.h>
#endif /* defined(OPENSSL) */
#include "Socket.h"

#define HTTP_PROTOCOL(x) x ? "https" : "http"

#if !(defined(_WIN32) || defined(_WIN64))
#if defined(LIBUUID)
#include <uuid/uuid.h>
#else /* if defined(USE_LIBUUID) */
#include <limits.h>
#include <stdlib.h>
#include <time.h>

/** @brief raw uuid type */
typedef unsigned char uuid_t[16];

/**
 * @brief generates a uuid, compatible with RFC 4122, version 4 (random)
 * @note Uses a very insecure algorithm but no external dependencies
 */
void uuid_generate( uuid_t out )
{
#if defined(OPENSSL)
	int rc = RAND_bytes( out, sizeof(uuid_t));
	if ( !rc )
#endif /* defined (OPENSSL) */
	{
		/* very insecure, but generates a random uuid */
		int i;
		srand(time(NULL));
		for ( i = 0; i < 16; ++i )
			out[i] = (unsigned char)(rand() % UCHAR_MAX);
		out[6] = (out[6] & 0x0f) | 0x40;
		out[8] = (out[8] & 0x3F) | 0x80;
	}
}

/** @brief converts a uuid to a string */
void uuid_unparse( uuid_t uu, char *out )
{
	int i;
	for ( i = 0; i < 16; ++i )
	{
		if ( i == 4 || i == 6 || i == 8 || i == 10 )
		{
			*out = '-';
			++out;
		}
		out += sprintf( out, "%02x", uu[i] );
	}
	*out = '\0';
}
#endif /* else if defined(LIBUUID) */
#endif /* if !(defined(_WIN32) || defined(_WIN64)) */

#include "Heap.h"

/** raw websocket frame data */
struct ws_frame
{
	size_t len; /**< length of frame */
	size_t pos; /**< current position within the buffer */
};

/** Current frame being processed */
struct ws_frame *last_frame = NULL;

/** Holds any received websocket frames, to be process */
static List* in_frames = NULL;

static char * frame_buffer = NULL;
static size_t frame_buffer_len = 0;
static size_t frame_buffer_index = 0;
static size_t frame_buffer_data_len = 0;

/* static function declarations */
static const char *WebSocket_strcasefind(
	const char *buf, const char *str, size_t len);

static char *WebSocket_getRawSocketData(
	networkHandles *net, size_t bytes, size_t* actual_len);

static void WebSocket_rewindData( void );

static void WebSocket_pong(
	networkHandles *net, char *app_data, size_t app_data_len);

static int WebSocket_receiveFrame(networkHandles *net,
	size_t bytes, size_t *actual_len );


/**
 * calculates the amount of data required for the websocket header
 *
 * this function is used to calculate how much offset is required before calling
 * @p WebSocket_putdatas, as that function will write data before the passed in
 * buffer
 *
 * @param[in,out]  net                 network connection
 * @param[in]      mask_data           whether to mask the data
 * @param[in]      data_len            amount of data in the payload
 *
 * @return the size in bytes of the websocket header required
 *
 * @see WebSocket_putdatas
 */
size_t WebSocket_calculateFrameHeaderSize(networkHandles *net, int mask_data, size_t data_len)
{
	int ret = 0;
	if ( net && net->websocket )
	{
		if ( data_len < 126u)
			ret = 2; /* header 2 bytes */
		else if ( data_len < 65536u )
			ret = 4; /* for extra 2-bytes for payload length */
		else if ( data_len < 0xFFFFFFFFFFFFFFFF )
			ret = 10; /* for extra 8-bytes for payload length */
		if ( mask_data & 0x1 )
			ret += sizeof(uint32_t); /* for mask */
	}
	return ret;
}


/**
 * @brief builds a websocket frame for data transmission
 *
 * write a websocket header and will mask the payload in all the passed in
 * buffers
 *
 * @param[in,out]  net                 network connection
 * @param[in]      opcode              websocket opcode for the packet
 * @param[in]      mask_data           whether to mask the data
 * @param[in,out]  buf0                first buffer, will write before this
 * @param[in]      buf0len             size of first buffer
 * @param[in]      count               number of payload buffers
 * @param[in,out]  buffers             array of payload buffers
 * @param[in]      buflens             array of payload buffer sizes
 * @param[in]      freeData            array indicating to free payload buffers
 *
 * @return amount of data to write to socket
 */
struct frameData {
	char* wsbuf0;
	size_t wsbuf0len;
	uint8_t mask[4];
};

static struct frameData WebSocket_buildFrame(networkHandles* net, int opcode, int mask_data,
	char** pbuf0, size_t* pbuf0len, int count, char** buffers, size_t* buflens)
{
	int buf_len = 0u;
	struct frameData rc;

	FUNC_ENTRY;
	memset(&rc, '\0', sizeof(rc));
	if ( net->websocket )
	{
		size_t ws_header_size = 0u;
		size_t data_len = 0L;
		int i;

		/* Calculate total length of MQTT buffers */
		data_len = *pbuf0len;
		for (i = 0; i < count; ++i)
			data_len += buflens[i];

		/* add space for websocket frame header */
		ws_header_size = WebSocket_calculateFrameHeaderSize(net, mask_data, data_len);
		if (*pbuf0)
		{
			rc.wsbuf0len = *pbuf0len + ws_header_size;
			rc.wsbuf0 = malloc(rc.wsbuf0len);
			if (rc.wsbuf0 == NULL)
				goto exit;
			memcpy(&rc.wsbuf0[ws_header_size], *pbuf0, *pbuf0len);
		}
		else
		{
			rc.wsbuf0 = malloc(ws_header_size);
			if (rc.wsbuf0 == NULL)
				goto exit;
			rc.wsbuf0len = ws_header_size;
		}

		/* generate mask, since we are a client */
#if defined(OPENSSL)
		RAND_bytes( &rc.mask[0], sizeof(rc.mask) );
#else /* if defined(OPENSSL) */
		rc.mask[0] = (rand() % UINT8_MAX);
		rc.mask[1] = (rand() % UINT8_MAX);
		rc.mask[2] = (rand() % UINT8_MAX);
		rc.mask[3] = (rand() % UINT8_MAX);
#endif /* else if defined(OPENSSL) */

		/* 1st byte */
		rc.wsbuf0[buf_len] = (char)(1 << 7); /* final flag */
		/* 3 bits reserved for negotiation of protocol */
		rc.wsbuf0[buf_len] |= (char)(opcode & 0x0F); /* op code */
		++buf_len;

		/* 2nd byte */
		rc.wsbuf0[buf_len] = (char)((mask_data & 0x1) << 7); /* masking bit */

		/* payload length */
		if ( data_len < 126u )
			rc.wsbuf0[buf_len++] |= data_len & 0x7F;

		/* 3rd byte & 4th bytes - extended payload length */
		else if ( data_len < 65536u )
		{
			uint16_t len = htobe16((uint16_t)data_len);
			rc.wsbuf0[buf_len++] |= (126u & 0x7F);
			memcpy( &rc.wsbuf0[buf_len], &len, 2u );
			buf_len += 2;
		}
		else if ( data_len < 0xFFFFFFFFFFFFFFFF )
		{
			uint64_t len = htobe64((uint64_t)data_len);
			rc.wsbuf0[buf_len++] |= (127u & 0x7F);
			memcpy( &rc.wsbuf0[buf_len], &len, 8 );
			buf_len += 8;
		}
		else
		{
			Log(TRACE_PROTOCOL, 1, "Data too large for websocket frame" );
			buf_len = -1;
		}

		/* masking key */
		if ( (mask_data & 0x1) && buf_len > 0 )
		{
			memcpy( &rc.wsbuf0[buf_len], &rc.mask, sizeof(uint32_t));
			buf_len += sizeof(uint32_t);
		}

		if ( mask_data & 0x1 ) 		/* mask data */
		{
			size_t idx = 0u;

			/* packet fixed header */
			for (i = (int)ws_header_size; i < (int)rc.wsbuf0len; ++i, ++idx)
				rc.wsbuf0[i] ^= rc.mask[idx % 4];

			/* variable data buffers */
			for (i = 0; i < count; ++i)
			{
				size_t j;
				for ( j = 0u; j < buflens[i]; ++j, ++idx )
					buffers[i][j] ^= rc.mask[idx % 4];
			}
		}
	}
exit:
	FUNC_EXIT_RC(buf_len);
	return rc;
}


static void WebSocket_unmaskData(uint8_t *mask, size_t idx, int count, char** buffers, size_t* buflens)
{
	int i;

	FUNC_ENTRY;
	for (i = 0; i < count; ++i)
	{
		size_t j;
		for ( j = 0u; j < buflens[i]; ++j, ++idx )
			buffers[i][j] ^= mask[idx % 4];
	}
	FUNC_EXIT;
}


/**
 * sends out a websocket request on the given uri
 *
 * @param[in]      net                 network connection
 * @param[in]      uri                 uri to connect to
 *
 * @retval SOCKET_ERROR                on failure
 * @retval 1                           on success
 *
 * @see WebSocket_upgrade
 */
int WebSocket_connect( networkHandles *net, const char *uri )
{
	int rc;
	char *buf = NULL;
	char *headers_buf = NULL;
	const MQTTClient_nameValue *headers = net->httpHeaders;
	int i, buf_len = 0;
	int headers_buf_len = 0;
	size_t hostname_len;
	int port = 80;
	const char *topic = NULL;
#if defined(_WIN32) || defined(_WIN64)
	UUID uuid;
#else /* if defined(_WIN32) || defined(_WIN64) */
	uuid_t uuid;
#endif /* else if defined(_WIN32) || defined(_WIN64) */

	FUNC_ENTRY;
	/* Generate UUID */
	if (net->websocket_key == NULL)
		net->websocket_key = malloc(25u);
	else
		net->websocket_key = realloc(net->websocket_key, 25u);
	if (net->websocket_key == NULL)
	{
		rc = PAHO_MEMORY_ERROR;
		goto exit;
	}
#if defined(_WIN32) || defined(_WIN64)
	ZeroMemory( &uuid, sizeof(UUID) );
	UuidCreate( &uuid );
	Base64_encode( net->websocket_key, 25u, (const b64_data_t*)&uuid, sizeof(UUID) );
#else /* if defined(_WIN32) || defined(_WIN64) */
	uuid_generate( uuid );
	Base64_encode( net->websocket_key, 25u, uuid, sizeof(uuid_t) );
#endif /* else if defined(_WIN32) || defined(_WIN64) */

	hostname_len = MQTTProtocol_addressPort(uri, &port, &topic);

	/* if no topic, use default */
	if ( !topic )
		topic = "/mqtt";

	if ( headers )
	{
		char *headers_buf_cur = NULL;
		while ( headers->name != NULL && headers->value != NULL )
		{
			headers_buf_len += (int)(strlen(headers->name) + strlen(headers->value) + 4);
			headers++;
		}
		headers_buf_len++;

		if ((headers_buf = malloc(headers_buf_len)) == NULL)
		{
			rc = PAHO_MEMORY_ERROR;
			goto exit;
		}
		headers = net->httpHeaders;
		headers_buf_cur = headers_buf;

		while ( headers->name != NULL && headers->value != NULL )
		{
			headers_buf_cur += sprintf(headers_buf_cur, "%s: %s\r\n", headers->name, headers->value);
			headers++;
		}
		*headers_buf_cur = '\0';
	}

	for ( i = 0; i < 2; ++i )
	{
		buf_len = snprintf( buf, (size_t)buf_len,
			"GET %s HTTP/1.1\r\n"
			"Host: %.*s:%d\r\n"
			"Upgrade: websocket\r\n"
			"Connection: Upgrade\r\n"
			"Origin: %s://%.*s:%d\r\n"
			"Sec-WebSocket-Key: %s\r\n"
			"Sec-WebSocket-Version: 13\r\n"
			"Sec-WebSocket-Protocol: mqtt\r\n"
			"%s"
			"\r\n", topic,
			(int)hostname_len, uri, port,
#if defined(OPENSSL)
			HTTP_PROTOCOL(net->ssl),
#else
			HTTP_PROTOCOL(0),
#endif
			
			(int)hostname_len, uri, port,
			net->websocket_key,
			headers_buf ? headers_buf : "");

		if ( i == 0 && buf_len > 0 )
		{
			++buf_len; /* need 1 extra byte for ending '\0' */
			if ((buf = malloc( buf_len )) == NULL)
			{
				rc = PAHO_MEMORY_ERROR;
				goto exit;
			}
		}
	}

	if (headers_buf)
		free( headers_buf );

	if ( buf )
	{
#if defined(OPENSSL)
		if (net->ssl)
			SSLSocket_putdatas(net->ssl, net->socket,
				buf, buf_len, 0, NULL, NULL, NULL );
		else
#endif
			Socket_putdatas( net->socket, buf, buf_len,
				0, NULL, NULL, NULL );
		free( buf );
		rc = 1;
	}
	else
	{
		free(net->websocket_key);
		net->websocket_key = NULL;
		rc = SOCKET_ERROR;
	}
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}

/**
 * closes a websocket connection
 *
 * @param[in,out]  net                 structure containing network connection
 * @param[in]      status_code         websocket close status code
 * @param[in]      reason              reason for closing connection (optional)
 */
void WebSocket_close(networkHandles *net, int status_code, const char *reason)
{
	FUNC_ENTRY;

	if ( net->websocket )
	{
		char *buf0;
		size_t buf0len = sizeof(uint16_t);
		uint16_t status_code_be;
		const int mask_data = 1; /* all frames from client must be masked */

		if ( status_code < WebSocket_CLOSE_NORMAL ||
			status_code > WebSocket_CLOSE_TLS_FAIL )
			status_code = WebSocket_CLOSE_GOING_AWAY;

		if ( reason )
			buf0len += strlen(reason);

		buf0 = malloc(buf0len);
		if ( !buf0 )
			goto exit;

		/* encode status code */
		status_code_be = htobe16((uint16_t)status_code);
		memcpy(buf0, &status_code_be, sizeof(uint16_t));

		/* encode reason, if provided */
		if ( reason )
			strcpy( &buf0[sizeof(uint16_t)], reason );

		WebSocket_buildFrame( net, WebSocket_OP_CLOSE, mask_data,
			&buf0, &buf0len, 0, NULL, NULL);

#if defined(OPENSSL)
		if (net->ssl)
			SSLSocket_putdatas(net->ssl, net->socket,
				buf0, buf0len, 0, NULL, NULL, NULL);
		else
#endif
			Socket_putdatas(net->socket, buf0, buf0len, 0,
				NULL, NULL, NULL);

		/* websocket connection is now closed */
		net->websocket = 0;
		free( buf0 );
	}
	if ( net->websocket_key )
	{
		free( net->websocket_key );
		net->websocket_key = NULL;
	}
exit:
	FUNC_EXIT;
}

/**
 * @brief receives 1 byte from a socket
 *
 * @param[in,out]  net                 network connection
 * @param[out]     c                   byte that was read
 *
 * @retval SOCKET_ERROR                on error
 * @retval TCPSOCKET_INTERRUPTED       no data available
 * @retval TCPSOCKET_COMPLETE          on success
 *
 * @see WebSocket_getdata
 */
int WebSocket_getch(networkHandles *net, char* c)
{
	int rc = SOCKET_ERROR;

	FUNC_ENTRY;
	if ( net->websocket )
	{
		struct ws_frame *frame = NULL;

		if ( in_frames && in_frames->first )
			frame = in_frames->first->content;

		if ( !frame )
		{
			size_t actual_len = 0u;
			rc =  WebSocket_receiveFrame( net, 1u, &actual_len );
			if ( rc != TCPSOCKET_COMPLETE )
				goto exit;

			/* we got a frame, let take off the top of queue */
			if ( in_frames->first )
				frame = in_frames->first->content;
		}

		/* set current working frame */
		if (frame && frame->len > frame->pos)
		{
			unsigned char *buf =
				(unsigned char *)frame + sizeof(struct ws_frame);
			*c = buf[frame->pos++];
			rc = TCPSOCKET_COMPLETE;
		}
	}
#if defined(OPENSSL)
	else if ( net->ssl )
		rc = SSLSocket_getch(net->ssl, net->socket, c);
#endif
	else
		rc = Socket_getch(net->socket, c);

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}

/**
 * @brief receives data from a socket.
 * It should receive all data from the socket that is immediately available.
 * Because it is encapsulated in websocket frames which cannot be
 *
 * @param[in,out]  net                 network connection
 * @param[in]      bytes               amount of data to get (0 if last packet)
 * @param[out]     actual_len          amount of data read
 *
 * @return a pointer to the read data
 *
 * @see WebSocket_getch
 */
char *WebSocket_getdata(networkHandles *net, size_t bytes, size_t* actual_len)
{
	char *rv = NULL;

	FUNC_ENTRY;
	if ( net->websocket )
	{
		struct ws_frame *frame = NULL;

		if ( bytes == 0u )
		{
			/* done with current frame, move it to last frame */
			if ( in_frames && in_frames->first )
				frame = in_frames->first->content;

			/* return the data from the next frame, if we have one */
			if ( frame )
			{
				rv = (char *)frame +
					sizeof(struct ws_frame) + frame->pos;
				*actual_len = frame->len - frame->pos;

				if ( last_frame )
					free( last_frame );
				last_frame = ListDetachHead(in_frames);
			}
			goto exit;
		}

		/* look at the first websocket frame */
		if ( in_frames && in_frames->first )
			frame = in_frames->first->content;

		/* no current frame, so let's go receive one for the network */
		if ( !frame )
		{
			const int rc =
				WebSocket_receiveFrame( net, bytes, actual_len );

			if ( rc == TCPSOCKET_COMPLETE && in_frames && in_frames->first)
				frame = in_frames->first->content;
		}

		if ( frame )
		{
			rv = (char *)frame + sizeof(struct ws_frame) + frame->pos;
			*actual_len = frame->len - frame->pos; /* use the rest of the frame */


			while (*actual_len < bytes) {
				const int rc = WebSocket_receiveFrame(net, bytes, actual_len);

				if (rc != TCPSOCKET_COMPLETE) {
					frame->pos = 0;
					break;
				}

				/* refresh pointers */
				frame = in_frames->first->content;
				rv = (char *)frame + sizeof(struct ws_frame) + frame->pos;
				*actual_len = frame->len - frame->pos; /* use the rest of the frame */

			} /* end while */


			if (*actual_len == bytes && in_frames)
			{
				if ( last_frame )
					free( last_frame );
				last_frame = ListDetachHead(in_frames);
			}
		}
	}
#if defined(OPENSSL)
	else if ( net->ssl )
		rv = SSLSocket_getdata(net->ssl, net->socket, bytes, actual_len);
#endif
	else
		rv = Socket_getdata(net->socket, bytes, actual_len);

exit:
	FUNC_EXIT_RC(rv);
	return rv;
}

void WebSocket_rewindData( void )
{
	frame_buffer_index = 0;
}

/**
 * reads raw socket data for underlying layers
 *
 * @param[in]      net                 network connection
 * @param[in]      bytes               number of bytes to read, 0 to complete packet
 * @param[in]      actual_len          amount of data read
 *
 * @return a buffer containing raw data
 */
char *WebSocket_getRawSocketData(networkHandles *net, size_t bytes, size_t* actual_len)
{
	char *rv = NULL;

	size_t bytes_requested = bytes;

	FUNC_ENTRY;
	if (bytes > 0)
	{
		if (frame_buffer_data_len - frame_buffer_index >= bytes)
		{
			*actual_len = bytes;
			rv = frame_buffer + frame_buffer_index;
			frame_buffer_index += bytes;

			goto exit;
		}
		else
		{
			bytes = bytes - (frame_buffer_data_len - frame_buffer_index);
		}
	}

	*actual_len = 0;
	
	// not enough data in the buffer, get data from socket
#if defined(OPENSSL)
	if ( net->ssl )
		rv = SSLSocket_getdata(net->ssl, net->socket, bytes, actual_len);
	else
#endif
		rv = Socket_getdata(net->socket, bytes, actual_len);

	// clear buffer
	if (bytes == 0)
	{
		frame_buffer_index = 0;
		frame_buffer_data_len = 0;
		frame_buffer_len = 0;
		
		free (frame_buffer);
		frame_buffer = NULL;
	}
	// append data to the buffer
	else if (rv != NULL && *actual_len != 0U)
	{
		// no buffer allocated
		if (!frame_buffer)
		{
			if ((frame_buffer = (char *)malloc(*actual_len)) == NULL)
			{
				rv = NULL;
				goto exit;
			}
			memcpy(frame_buffer, rv, *actual_len);

			frame_buffer_index = 0;
			frame_buffer_data_len = *actual_len;
			frame_buffer_len = *actual_len;
		}
		// buffer size is big enough
		else if (frame_buffer_data_len + *actual_len < frame_buffer_len)
		{
			memcpy(frame_buffer + frame_buffer_data_len, rv, *actual_len);
			frame_buffer_data_len += *actual_len;
		}
		// resize buffer
		else
		{
			frame_buffer = realloc(frame_buffer, frame_buffer_data_len + *actual_len);
			frame_buffer_len = frame_buffer_data_len + *actual_len;

			memcpy(frame_buffer + frame_buffer_data_len, rv, *actual_len);
			frame_buffer_data_len += *actual_len;
		}

		SocketBuffer_complete(net->socket);
	}
	else 
		goto exit;

	bytes = bytes_requested;
    
	// if possible, return data from the buffer
	if (bytes > 0)
	{
		if (frame_buffer_data_len - frame_buffer_index >= bytes)
		{
			*actual_len = bytes;
			rv = frame_buffer + frame_buffer_index;
			frame_buffer_index += bytes;
		}
		else
		{
			*actual_len = frame_buffer_data_len - frame_buffer_index;
			rv = frame_buffer + frame_buffer_index;
			frame_buffer_index += *actual_len;
		}
	}

exit:
	FUNC_EXIT;
	return rv;
}

/**
 * sends a "websocket pong" message
 *
 * @param[in]      net                 network connection
 * @param[in]      app_data            application data to put in payload
 * @param[in]      app_data_len        application data length
 */
void WebSocket_pong(networkHandles *net, char *app_data, size_t app_data_len)
{
	FUNC_ENTRY;
	if ( net->websocket )
	{
		char *buf0 = NULL;
		size_t buf0len = 0;
		int freeData = 0;
		const int mask_data = 1; /* all frames from client must be masked */

		WebSocket_buildFrame( net, WebSocket_OP_PONG, mask_data,
			&buf0, &buf0len, 1, &app_data, &app_data_len);

		Log(TRACE_PROTOCOL, 1, "Sending WebSocket PONG" );

#if defined(OPENSSL)
		if (net->ssl)
			SSLSocket_putdatas(net->ssl, net->socket, buf0,
				buf0len /*header_len + app_data_len*/, 1,
				&app_data, &app_data_len, &freeData);
		else
#endif
			Socket_putdatas(net->socket, buf0,
				buf0len /*header_len + app_data_len*/, 1,
				&app_data, &app_data_len, &freeData );

		/* clean up memory */
		free( buf0 );
	}
	FUNC_EXIT;
}

/**
 * writes data to a socket (websocket header will be prepended if required)
 *
 * @warning buf0 will be expanded (backwords before @p buf0 buffer, to add a
 * websocket frame header to the data if required).  So use
 * @p WebSocket_calculateFrameHeader, to determine if extra space is needed
 * before the @p buf0 pointer.
 *
 * @param[in,out]  net                 network connection
 * @param[in,out]  buf0                first buffer
 * @param[in]      buf0len             size of first buffer
 * @param[in]      count               number of payload buffers
 * @param[in,out]  buffers             array of paylaod buffers
 * @param[in]      buflens             array of payload buffer sizes
 * @param[in]      freeData            array indicating to free payload buffers
 *
 * @return amount of data wrote to socket
 *
 * @see WebSocket_calculateFrameHeaderSize
 */
int WebSocket_putdatas(networkHandles* net, char** buf0, size_t* buf0len,
	int count, char** buffers, size_t* buflens, int* freeData)
{
	const int mask_data = 1; /* must mask websocket data from client */
	int rc;

	FUNC_ENTRY;
	if (net->websocket)
	{
		struct frameData wsdata;

		wsdata = WebSocket_buildFrame(
			net, WebSocket_OP_BINARY, mask_data, buf0, buf0len,
			count, buffers, buflens);
#if defined(OPENSSL)
		if (net->ssl)
			rc = SSLSocket_putdatas(net->ssl, net->socket, wsdata.wsbuf0, wsdata.wsbuf0len, count, buffers, buflens, freeData);
		else
#endif
			rc = Socket_putdatas(net->socket, wsdata.wsbuf0, wsdata.wsbuf0len, count, buffers, buflens, freeData);

		if (rc != TCPSOCKET_INTERRUPTED)
		{
			if (mask_data)
				WebSocket_unmaskData(wsdata.mask, *buf0len, count, buffers, buflens);
			free(wsdata.wsbuf0); /* free temporary ws header */
		}
	}
	else
	{
#if defined(OPENSSL)
		if (net->ssl)
			rc = SSLSocket_putdatas(net->ssl, net->socket, *buf0, *buf0len, count, buffers, buflens, freeData);
		else
#endif
			rc = Socket_putdatas(net->socket, *buf0, *buf0len, count, buffers, buflens, freeData);
	}

	FUNC_EXIT_RC(rc);
	return rc;
}

/**
 * receives incoming socket data and parses websocket frames
 * Copes with socket reads returning partial websocket frames by using the
 * SocketBuffer mechanism.
 *
 * @param[in]      net                 network connection
 * @param[in]      bytes               amount of data to receive
 * @param[out]     actual_len          amount of data actually read
 *
 * @retval TCPSOCKET_COMPLETE          packet received
 * @retval TCPSOCKET_INTERRUPTED       incomplete packet received
 * @retval SOCKET_ERROR                an error was encountered
 */
int WebSocket_receiveFrame(networkHandles *net, size_t bytes, size_t *actual_len)
{
	struct ws_frame *res = NULL;
	int rc = TCPSOCKET_COMPLETE;
	int opcode = 0;

	FUNC_ENTRY;
	if ( !in_frames )
		in_frames = ListInitialize();

	/* see if there is frame currently on queue */
	if ( in_frames->first )
		res = in_frames->first->content;

	//while( !res )
	//{
		opcode = WebSocket_OP_BINARY;
		do
		{
			/* obtain all frames in the sequence */
			int is_final = 0;
			while ( is_final == 0 )
			{
				char *b;
				size_t len = 0u;
				int tmp_opcode;
				int has_mask;
				size_t cur_len = 0u;
				uint8_t mask[4] = { 0u, 0u, 0u, 0u };
				size_t payload_len;

				b = WebSocket_getRawSocketData(net, 2u, &len);
				if ( !b )
				{
					rc = TCPSOCKET_INTERRUPTED;
					goto exit;
				} 
				else if (len < 2u )
				{
					rc = TCPSOCKET_INTERRUPTED;
					goto exit;
				}

				/* 1st byte */
				is_final = (b[0] & 0xFF) >> 7;
				tmp_opcode = (b[0] & 0x0F);

				if ( tmp_opcode ) /* not a continuation frame */
					opcode = tmp_opcode;

				/* invalid websocket packet must return error */
				if ( opcode < WebSocket_OP_CONTINUE ||
				     opcode > WebSocket_OP_PONG ||
				     ( opcode > WebSocket_OP_BINARY &&
				       opcode < WebSocket_OP_CLOSE ) )
				{
					rc = SOCKET_ERROR;
					goto exit;
				}

				/* 2nd byte */
				has_mask = (b[1] & 0xFF) >> 7;
				payload_len = (b[1] & 0x7F);

				/* determine payload length */
				if ( payload_len == 126 )
				{
					/* If 126, the following 2 bytes interpreted as a
					      16-bit unsigned integer are the payload length. */
					b = WebSocket_getRawSocketData(net, 2u, &len);
					if ( !b )
					{
						rc = SOCKET_ERROR;
						goto exit;
					} 
					else if (len < 2u )
					{
						rc = TCPSOCKET_INTERRUPTED;
						goto exit;
					}
					/* convert from big endian 16 to host */
					payload_len = be16toh(*(uint16_t*)b);
				}
				else if ( payload_len == 127 )
				{
					 /* If 127, the following 8 bytes interpreted as a 64-bit unsigned integer (the
					      most significant bit MUST be 0) are the payload length */
					b = WebSocket_getRawSocketData(net, 8u, &len);
					if ( !b )
					{
						rc = SOCKET_ERROR;
						goto exit;
					} 
					else if (len < 8u )
					{
						rc = TCPSOCKET_INTERRUPTED;
						goto exit;
					}
					/* convert from big-endian 64 to host */
					payload_len = (size_t)be64toh(*(uint64_t*)b);
				}

				if ( has_mask )
				{
					uint8_t mask[4];
					b = WebSocket_getRawSocketData(net, 4u, &len);
					if ( !b )
					{
						rc = SOCKET_ERROR;
						goto exit;
					} 
					else if (len < 4u )
					{
						rc = TCPSOCKET_INTERRUPTED;
						goto exit;
					}
					memcpy( &mask[0], b, sizeof(uint32_t));
				}

				/* use the socket buffer to read in the whole websocket frame */
				b = WebSocket_getRawSocketData(net, payload_len, &len);

				if ( !b )
				{
					rc = SOCKET_ERROR;
					goto exit;
				} 
				else if (len < payload_len )
				{
					rc = TCPSOCKET_INTERRUPTED;
					goto exit;
				}

				/* unmask data */
				if ( has_mask )
				{
					size_t i;
					for ( i = 0u; i < payload_len; ++i )
						b[i] ^= mask[i % 4];
				}

				if ( res )
					cur_len = res->len;

				if (res == NULL)
				{
					if ((res = malloc( sizeof(struct ws_frame) + cur_len + len)) == NULL)
					{
						rc = PAHO_MEMORY_ERROR;
						goto exit;
					}
					res->pos = 0u;
				} else
				{
					if ((res = realloc( res, sizeof(struct ws_frame) + cur_len + len )) == NULL)
					{
						rc = PAHO_MEMORY_ERROR;
						goto exit;
					}
				}
				if (in_frames && in_frames->first)
					in_frames->first->content = res; /* realloc moves the data */
				memcpy( (unsigned char *)res + sizeof(struct ws_frame) + cur_len, b, len );
				res->len = cur_len + len;

				WebSocket_getRawSocketData(net, 0u, &len);
			}

			if ( opcode == WebSocket_OP_PING || opcode == WebSocket_OP_PONG )
			{
				/* respond to a "ping" with a "pong" */
				if ( opcode == WebSocket_OP_PING )
					WebSocket_pong( net,
						(char *)res + sizeof(struct ws_frame),
						res->len );

				/* discard message */
				free( res );
				res = NULL;
			}
			else if ( opcode == WebSocket_OP_CLOSE )
			{
				/* server end closed websocket connection */
				free( res );
				WebSocket_close( net, WebSocket_CLOSE_GOING_AWAY, NULL );
				rc = SOCKET_ERROR; /* closes socket */
				goto exit;
			}
		} while ( opcode == WebSocket_OP_PING || opcode == WebSocket_OP_PONG );
	//}

	if (in_frames->count == 0)
		ListAppend( in_frames, res, sizeof(struct ws_frame) + res->len);
	*actual_len = res->len - res->pos;

exit:
	if (rc == TCPSOCKET_INTERRUPTED)
	{
		WebSocket_rewindData();
	}

	FUNC_EXIT_RC(rc);
	return rc;
}

/**
 * case-insensitive string search
 *
 * similar to @p strcase, but takes a maximum length
 *
 * @param[in]      buf                 buffer to search
 * @param[in]      str                 string to find
 * @param[in]       len                length of the buffer
 *
 * @retval !NULL                       location of string found
 * @retval NULL                        string not found
 */
const char *WebSocket_strcasefind(const char *buf, const char *str, size_t len)
{
	const char *res = NULL;
	if ( buf && len > 0u && str )
	{
		const size_t str_len = strlen( str );
		while ( len >= str_len && !res )
		{
			if ( strncasecmp( buf, str, str_len ) == 0 )
				res = buf;
			++buf;
			--len;
		}
	}
	return res;
}

/**
 * releases resources used by the websocket sub-system
 */
void WebSocket_terminate( void )
{
	FUNC_ENTRY;
	/* clean up and un-processed websocket frames */
	if ( in_frames )
	{
		struct ws_frame *f = ListDetachHead( in_frames );
		while ( f )
		{
			free( f );
			f = ListDetachHead( in_frames );
		}
		ListFree( in_frames );
		in_frames = NULL;
	}
	if ( last_frame )
	{
		free( last_frame );
		last_frame = NULL;
	}
	
	if ( frame_buffer )
	{
		free( frame_buffer );
		frame_buffer = NULL;
	}
	
	frame_buffer_len = 0;
	frame_buffer_index = 0;
	frame_buffer_data_len = 0;

	Socket_outTerminate();
#if defined(OPENSSL)
	SSLSocket_terminate();
#endif
	FUNC_EXIT;
}

/**
 * handles the websocket upgrade response
 *
 * @param[in,out]  net                 network connection to upgrade
 *
 * @retval SOCKET_ERROR                failed to upgrade network connection
 * @retval TCPSOCKET_INTERRUPTED       upgrade not complete, but not failed.  Try again
 * @retval 1                           socket upgraded to use websockets
 *
 * @see WebSocket_connect
 */
int WebSocket_upgrade( networkHandles *net )
{
	static const char *const ws_guid =
		"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
	int rc = SOCKET_ERROR;

	FUNC_ENTRY;
	if ( net->websocket_key )
	{
		SHA_CTX ctx;
		char ws_key[62u] = { 0 };
		unsigned char sha_hash[SHA1_DIGEST_LENGTH];
		size_t rcv = 0u;
		char *read_buf;

		/* calculate the expected websocket key, expected from server */
		snprintf( ws_key, sizeof(ws_key), "%s%s", net->websocket_key, ws_guid );
		SHA1_Init( &ctx );
		SHA1_Update( &ctx, ws_key, strlen(ws_key));
		SHA1_Final( sha_hash, &ctx );
		Base64_encode( ws_key, sizeof(ws_key), sha_hash, SHA1_DIGEST_LENGTH );

		rc = TCPSOCKET_INTERRUPTED;
		read_buf = WebSocket_getRawSocketData( net, 12u, &rcv );

		if ((read_buf == NULL) || rcv < 12u) {
			Log(TRACE_PROTOCOL, 1, "WebSocket upgrade read not complete %lu", rcv );
			goto exit;
		}

		if (strncmp( read_buf, "HTTP/1.1", 8u ) == 0)
		{
			if (strncmp( &read_buf[9], "101", 3u ) != 0)
			{
				Log(TRACE_PROTOCOL, 1, "WebSocket HTTP rc %.3s", &read_buf[9]);
				rc = SOCKET_ERROR;
				goto exit;
			}
		}

		if (strncmp( read_buf, "HTTP/1.1 101", 12u ) == 0)
		{
			const char *p;

			read_buf = WebSocket_getRawSocketData(net, 1024u, &rcv );

			/* Did we read the whole response? */
			if (read_buf && rcv > 4 && memcmp(&read_buf[rcv-4], "\r\n\r\n", 4) != 0)
			{
				Log(TRACE_PROTOCOL, -1, "WebSocket HTTP upgrade response read not complete %lu", rcv);
				rc = SOCKET_ERROR;
				goto exit;
			}

			/* check for upgrade */
			p = WebSocket_strcasefind(
				read_buf, "Connection", rcv );
			if ( p )
			{
				const char *eol;
				eol = memchr( p, '\n', rcv-(read_buf-p) );
				if ( eol )
					p = WebSocket_strcasefind(
						p, "Upgrade", eol - p);
				else
					p = NULL;
			}

			/* check key hash */
			if ( p )
				p = WebSocket_strcasefind( read_buf,
					"sec-websocket-accept", rcv );
			if ( p )
			{
				const char *eol;
				eol = memchr( p, '\n', rcv-(read_buf-p) );
				if ( eol )
				{
					p = memchr( p, ':', eol-p );
					if ( p )
					{
						size_t hash_len = eol-p-1;
						while ( *p == ':' || *p == ' ' )
						{
							++p;
							--hash_len;
						}

						if ( strncmp( p, ws_key, hash_len ) != 0 )
							p = NULL;
					}
				}
				else
					p = NULL;
			}

			if ( p )
			{
				net->websocket = 1;
				Log(TRACE_PROTOCOL, 1, "WebSocket connection upgraded" );
				rc = 1;
			}
			else
			{
				Log(TRACE_PROTOCOL, 1, "WebSocket failed to upgrade connection" );
				rc = SOCKET_ERROR;
			}

			if ( net->websocket_key )
			{
				free(net->websocket_key);
				net->websocket_key = NULL;
			}

			/* indicate that we done with the packet */
			WebSocket_getRawSocketData( net, 0u, &rcv );
		}
	}

exit:
	FUNC_EXIT_RC(rc);
	return rc;
}

/**
 * Notify the IP address and port of the endpoint to proxy, and wait connection to endpoint.
 *
 * @param[in]  net               network connection to proxy.
 * @param[in]  ssl               enable ssl.
 * @param[in]  hostname          hostname of endpoint.
 *
 * @retval SOCKET_ERROR          failed to network connection
 * @retval 0                     connection to endpoint
 * 
 */
int WebSocket_proxy_connect( networkHandles *net, int ssl, const char *hostname)
{
	int port, i, rc = 0, buf_len=0;
	char *buf = NULL;
	size_t hostname_len, actual_len = 0; 
	time_t current, timeout;
	FUNC_ENTRY;
 
	hostname_len = MQTTProtocol_addressPort(hostname, &port, NULL);
	for ( i = 0; i < 2; ++i ) {
#if defined(OPENSSL)
		if(ssl) {
			if (net->https_proxy_auth) {
				buf_len = snprintf( buf, (size_t)buf_len, "CONNECT %.*s:%d HTTP/1.1\r\n"
					"Host: %.*s\r\n"
					"Proxy-authorization: Basic %s\r\n"
					"\r\n",
					(int)hostname_len, hostname, port,
					(int)hostname_len, hostname, net->https_proxy_auth);
			}
			else {
				buf_len = snprintf( buf, (size_t)buf_len, "CONNECT %.*s:%d HTTP/1.1\r\n"
					"Host: %.*s\r\n"
					"\r\n",
					(int)hostname_len, hostname, port,
					(int)hostname_len, hostname);
			}
		}
		else {
#endif
			if (net->http_proxy_auth) {
				buf_len = snprintf( buf, (size_t)buf_len, "CONNECT %.*s:%d HTTP/1.1\r\n"
					"Host: %.*s\r\n"
					"Proxy-authorization: Basic %s\r\n"
					"\r\n",
					(int)hostname_len, hostname, port,
					(int)hostname_len, hostname, net->http_proxy_auth);
			}
			else {
				buf_len = snprintf( buf, (size_t)buf_len, "CONNECT %.*s:%d HTTP/1.1\r\n"
					"Host: %.*s\r\n"
					"\r\n",
					(int)hostname_len, hostname, port,
					(int)hostname_len, hostname);
			}
#if defined(OPENSSL)
		}
#endif
		if ( i==0 && buf_len > 0 ) {
			++buf_len;
			if ((buf = malloc( buf_len )) == NULL)
			{
				rc = PAHO_MEMORY_ERROR;
				goto exit;
			}

		}  
	}

	Socket_putdatas( net->socket, buf, buf_len, 0, NULL, NULL, NULL );
	free(buf);
	buf = NULL;

	time(&timeout);
	timeout += (time_t)10;

	while(1) {
		buf = Socket_getdata(net->socket, (size_t)12, &actual_len);
		if(actual_len) {
			if ( (strncmp( buf, "HTTP/1.0 200", 12 ) != 0) &&  (strncmp( buf, "HTTP/1.1 200", 12 ) != 0) )
				rc = SOCKET_ERROR;
			break;
		}
		else {
			time(&current);
			if(current > timeout) {
				rc = SOCKET_ERROR;
				break;
			}
#if defined(_WIN32) || defined(_WIN64)
			Sleep(250);
#else
			usleep(250000);
#endif
		}
	}

	/* flash the SocketBuffer */
	actual_len = 1;
	while(actual_len)
		buf = Socket_getdata(net->socket, (size_t)1, &actual_len);
exit:
	FUNC_EXIT_RC(rc);
	return rc;
}
