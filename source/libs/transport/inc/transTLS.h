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
} STransTLSCtx;

int32_t transTlsCtxCreate(const char* certPath, const char* keyPath, const char* caPath, STransTLSCtx** ppCtx);
void    transTlsCtxDestroy(STransTLSCtx* pCtx);

typedef struct {
  STransTLSCtx* pTlsCtx;  // pointer to TLS context

  SSL*    ssl;       // SSL connection
  int32_t refCount;  // reference count
  int32_t status;    // connection status

  BIO* readBio;
  BIO* writeBio;  // BIO for reading and writing data
} STransTLS;

int32_t initSSL(STransTLSCtx* pCtx, STransTLS** ppTLs);
void    destroySSL(STransTLS* pTLs);

void setSSLMode(STransTLS* pTls);

int32_t transTLSInit(void);
#ifdef __cplusplus
}
#endif

#endif  // _TD_TRANSPORT_TLS_H
