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

int32_t transTlsCtxCreate(const char* certPath, const char* keyPath, const char* caPath, SSslCtx** ppCtx);
void    transTlsCtxDestroy(SSslCtx* pCtx);

typedef struct {
  int32_t  cap;
  int32_t  len;
  int8_t   invalid;  // whether the buffer is invalid
  uint8_t* buf;      // buffer for encrypted data
} SSslBuffer;

int32_t sslBufferInit(SSslBuffer* buf, int32_t cap);
int32_t sslBufferDestroy(SSslBuffer* buf);
int32_t sslBufferClear(SSslBuffer* buf);
int32_t sslBufferRealloc(SSslBuffer* buf, int32_t newCap, uv_buf_t* uvbuf);

typedef struct {
  SSslCtx* pTlsCtx;  // pointer to TLS context

  SSL*    ssl;       // SSL connection
  int32_t refCount;  // reference count
  int32_t status;    // connection status

  BIO* readBio;
  BIO* writeBio;  // BIO for reading and writing data

  void* pStream;

  SSslBuffer readBuf;  // buffer for reading data

} STransTLS;

int32_t initSSL(SSslCtx* pCtx, STransTLS** ppTLs);
void    destroySSL(STransTLS* pTLs);

void setSSLMode(STransTLS* pTls, int8_t cliMode);

int32_t sslDoConnect(STransTLS* pTls);

int32_t sslDoWrite(STransTLS* pTls, uv_stream_t* stream, uv_write_t* req, uv_buf_t* pBuf, int32_t nBuf,
                   void (*cb)(uv_write_t*, int));

int32_t sslDoRead(STransTLS* pTls, SConnBuffer* pBuf, int32_t nread);

int32_t transTLSInit(void);

#ifdef __cplusplus
}
#endif

#endif  // _TD_TRANSPORT_TLS_H
