/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef TDENGINE_TRPC_H
#define TDENGINE_TRPC_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stdint.h>
#include "taosdef.h"
#include "tmsg.h"

#define TAOS_CONN_SERVER 0
#define TAOS_CONN_CLIENT 1

extern int tsRpcHeadSize;

typedef struct SRpcConnInfo {
  uint32_t clientIp;
  uint16_t clientPort;
  uint32_t serverIp;
  char     user[TSDB_USER_LEN];
} SRpcConnInfo;

typedef struct SRpcMsg {
  tmsg_t  msgType;
  tmsg_t  expectMsgType;
  void *  pCont;
  int     contLen;
  int32_t code;
  void *  handle;   // rpc handle returned to app
  void *  ahandle;  // app handle set by client
  int     noResp;   // has response or not(default 0 indicate resp);

} SRpcMsg;

typedef struct SRpcInit {
  uint16_t localPort;     // local port
  char *   label;         // for debug purpose
  int      numOfThreads;  // number of threads to handle connections
  int      sessions;      // number of sessions allowed
  int8_t   connType;      // TAOS_CONN_UDP, TAOS_CONN_TCPC, TAOS_CONN_TCPS
  int      idleTime;      // milliseconds, 0 means idle timer is disabled

  // the following is for client app ecurity only
  char *user;     // user name
  char  spi;      // security parameter index
  char  encrypt;  // encrypt algorithm
  char *secret;   // key for authentication
  char *ckey;     // ciphering key

  // call back to process incoming msg, code shall be ignored by server app
  void (*cfp)(void *parent, SRpcMsg *, SEpSet *);

  // call back to retrieve the client auth info, for server app only
  int (*afp)(void *parent, char *tableId, char *spi, char *encrypt, char *secret, char *ckey);

  // call back to keep conn or not
  bool (*pfp)(void *parent, tmsg_t msgType);

  // to support Send messages multiple times on a link
  void *(*mfp)(void *parent, tmsg_t msgType);

  // call back  to handle except when query/fetch in progress
  bool (*efp)(void *parent, tmsg_t msgType);

  void *parent;
} SRpcInit;

int32_t rpcInit();
void    rpcCleanup();
void *  rpcOpen(const SRpcInit *pRpc);
void    rpcClose(void *);
void *  rpcMallocCont(int contLen);
void    rpcFreeCont(void *pCont);
void *  rpcReallocCont(void *ptr, int contLen);
void    rpcSendRequest(void *thandle, const SEpSet *pEpSet, SRpcMsg *pMsg, int64_t *rid);
void    rpcSendResponse(const SRpcMsg *pMsg);
void    rpcSendRedirectRsp(void *pConn, const SEpSet *pEpSet);
int     rpcGetConnInfo(void *thandle, SRpcConnInfo *pInfo);
void    rpcSendRecv(void *shandle, SEpSet *pEpSet, SRpcMsg *pReq, SRpcMsg *pRsp);
int     rpcReportProgress(void *pConn, char *pCont, int contLen);
void    rpcCancelRequest(int64_t rid);

// just release client conn to rpc instance, no close sock
void rpcReleaseHandle(void *handle, int8_t type);

void rpcRefHandle(void *handle, int8_t type);
void rpcUnrefHandle(void *handle, int8_t type);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TRPC_H
