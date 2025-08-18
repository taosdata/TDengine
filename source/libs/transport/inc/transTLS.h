/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef _TD_TRANSPORT_TLS_H
#define _TD_TRANSPORT_TLS_H

#include <openssl/err.h>
#include <openssl/ssl.h>

#include "taoserror.h"
#include "transComm.h"
#include "transLog.h"
#include "tversion.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  char*    certfile;  // certificate file path
  char*    keyfile;   // private key file path
  char*    cafile;    // CA file path
  char*    capath;    // CA directory path
  char*    psk_hint;  // PSK hint for TLS-PSK
  SSL_CTX* ssl_ctx;   // SSL context
} SSslCtx;

int32_t transTlsCtxCreate(const char* certPath, const char* keyPath, const char* caPath, int8_t cliMode,
                          SSslCtx** ppCtx);
void    transTlsCtxDestroy(SSslCtx* pCtx);

typedef struct {
  int32_t  cap;
  int32_t  len;
  int8_t   invalid;  // whether the buffer is invalid
  uint8_t* buf;      // buffer for encrypted data
} SSslBuffer;

typedef struct {
  SSslCtx* pTlsCtx;  // pointer to TLS context

  SSL*    ssl;       // SSL connection
  int32_t refCount;  // reference count
  int32_t status;    // connection status

  BIO* readBio;
  BIO* writeBio;  // BIO for reading and writing data

  void* pStream;
  void *pConn;

  SSslBuffer readBuf;  // buffer for reading data

  void (*connCb)(uv_connect_t* pStream, int32_t status);                        // callback for connection events
  void (*readCb)(uv_stream_t* pStream, ssize_t nread, const uv_buf_t* buffer);  // callback for write events
  void (*writeCb)(uv_write_t* pReq, int32_t status);                            // callback for write events
} STransTLS;

int32_t sslInit(SSslCtx* pCtx, STransTLS** ppTLs);
void    sslDestroy(STransTLS* pTLs);

void sslSetMode(STransTLS* pTls, int8_t cliMode);

int32_t sslConnect(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req);

int32_t sslWrite(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req, uv_buf_t* pBuf, int32_t nBuf,
                 void (*cb)(uv_write_t*, int));

int32_t sslRead(STransTLS* pTls, SConnBuffer* pBuf, int32_t nread, int8_t cliMode);

int8_t sslIsInited(STransTLS* pTls);

int32_t sslBufferInit(SSslBuffer* buf, int32_t cap);
int32_t sslBufferDestroy(SSslBuffer* buf);
void    sslBufferClear(SSslBuffer* buf);
int32_t sslBufferAppend(SSslBuffer* buf, int32_t len);
int32_t sslBufferRealloc(SSslBuffer* buf, int32_t newCap, uv_buf_t* uvbuf);
#ifdef __cplusplus
}
#endif

#endif  // _TD_TRANSPORT_TLS_H
