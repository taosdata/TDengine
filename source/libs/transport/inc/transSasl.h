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
#ifndef _TD_TRANSPORT_SASL_H
#define _TD_TRANSPORT_SASL_H

#if defined(LINUX)
#include <sasl/sasl.h>
#else
typedef struct {
  void* p;
} SASL_CTX;
typedef struct {
  void* p;
} SASL_Bio;

typedef struct {
  void* p;
} SASL_Arg;
#endif

#include "taoserror.h"
#include "transComm.h"
#include "transLog.h"
#include "tversion.h"

typedef struct {
  int32_t  cap;
  int32_t  len;
  int8_t   invalid;  // whether the buffer is invalid
  int8_t   ref;
  uint8_t* buf;  // buffer for encrypted data
} SSaslBuffer;

typedef struct {
  int32_t      state;
  int8_t       completed;
  sasl_conn_t* conn;

  char* authUser;

  void* pUvConn;

  SSaslBuffer in;
  SSaslBuffer out;
  SSaslBuffer authInfo;
  int8_t      isAuthed;
} SSaslConn;

#ifdef __cplusplus
extern "C" {
#endif

void saslLibInit();
void saslLibCleanup();

int32_t saslConnCreate(SSaslConn** ppConn, int8_t server);
int32_t saslConnInit(SSaslConn* pConn, int8_t isServer);
void    saslConnCleanup(SSaslConn* pConn);

void saslConnSetState(SSaslConn* pConn, int32_t state);
int32_t saslConnEncode(SSaslConn* pConn, const char* input, int32_t len, const char** output, unsigned* outputLen);
int32_t saslConnDecode(SSaslConn* pConn, const char* input, int32_t len, const char** output, unsigned* outputLen);

int32_t saslConnHandleAuth(SSaslConn* pConn, int8_t server, const char* input, int32_t len);

int8_t  saslConnShoudDoAuth(SSaslConn* pConn);
int32_t saslConnStartAuth(SSaslConn* pConn, int8_t server);
int32_t saslBufferInit(SSaslBuffer* buf, int32_t cap);
int32_t saslBufferAppend(SSaslBuffer* buf, uint8_t* data, int32_t len);
void    saslBufferCleanup(SSaslBuffer* buf);
void    saslBufferClear(SSaslBuffer* buf);

#ifdef __cplusplus
}
#endif

#endif  // _TD_TRANSPORT_SASL_H
