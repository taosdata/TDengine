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
  int32_t      state;
  int8_t       completed;
  sasl_conn_t* conn;

  char* authUser;
} SSaslConn;

#ifdef __cplusplus
extern "C" {
#endif

int32_t saslConnInit(SSaslConn** pConn, int8_t isServer);
void    saslConnCleanup(SSaslConn* pConn);

void saslConnSetState(SSaslConn* pConn, int32_t state);
int32_t saslConnEncode(SSaslConn* pConn, const char* input, int32_t len, const char** output, unsigned* outputLen);
int32_t saslConnDecode(SSaslConn* pConn, const char* input, int32_t len, const char** output, unsigned* outputLen);

int32_t saslConnHandleAuth(SSaslConn* pConn, const char* input, int32_t len);

#ifdef __cplusplus
}
#endif

#endif  // _TD_TRANSPORT_SASL_H
