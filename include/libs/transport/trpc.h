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
#define IsReq(pMsg)      (pMsg->msgType & 1U)

extern int tsRpcHeadSize;

typedef struct {
  uint32_t clientIp;
  uint16_t clientPort;
  char     user[TSDB_USER_LEN];
} SRpcConnInfo;

typedef struct SRpcHandleInfo {
  // rpc info
  void   *handle;         // rpc handle returned to app
  int64_t refId;          // refid, used by server
  int32_t noResp;         // has response or not(default 0, 0: resp, 1: no resp);
  int32_t persistHandle;  // persist handle or not

  // app info
  void *ahandle;  // app handle set by client
  void *wrapper;  // wrapper handle
  void *node;     // node mgmt handle

  // resp info
  void   *rsp;
  int32_t rspLen;
} SRpcHandleInfo;

typedef struct SRpcMsg {
  tmsg_t         msgType;
  void          *pCont;
  int32_t        contLen;
  int32_t        code;
  SRpcHandleInfo info;
  SRpcConnInfo   conn;
} SRpcMsg;

typedef void (*RpcCfp)(void *parent, SRpcMsg *, SEpSet *rf);
typedef int (*RpcAfp)(void *parent, char *tableId, char *spi, char *encrypt, char *secret, char *ckey);
///
// // SRpcMsg code
// REDIERE,
// NOT READY, EpSet
typedef bool (*RpcRfp)(int32_t code);

typedef struct SRpcInit {
  char     localFqdn[TSDB_FQDN_LEN];
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
  RpcCfp cfp;

  // call back to retrieve the client auth info, for server app only
  RpcAfp afp;

  // user defined retry func
  RpcRfp rfp;

  void *parent;
} SRpcInit;

typedef struct {
  void *val;
  int32_t (*clone)(void *src, void **dst);
  void (*freeFunc)(const void *arg);
} SRpcCtxVal;

typedef struct {
  int32_t msgType;
  void *  val;
  int32_t (*clone)(void *src, void **dst);
  void (*freeFunc)(const void *arg);
} SRpcBrokenlinkVal;

typedef struct {
  SHashObj *        args;
  SRpcBrokenlinkVal brokenVal;
} SRpcCtx;

int32_t rpcInit();
void    rpcCleanup();
void *  rpcOpen(const SRpcInit *pRpc);
void    rpcClose(void *);
void *  rpcMallocCont(int contLen);
void    rpcFreeCont(void *pCont);
void *  rpcReallocCont(void *ptr, int contLen);

// Because taosd supports multi-process mode
// These functions should not be used on the server side
// Please use tmsg<xx> functions, which are defined in tmsgcb.h
void rpcSendRequest(void *thandle, const SEpSet *pEpSet, SRpcMsg *pMsg, int64_t *rid);
void rpcSendResponse(const SRpcMsg *pMsg);
void rpcRegisterBrokenLinkArg(SRpcMsg *msg);
void rpcReleaseHandle(void *handle, int8_t type);  // just release client conn to rpc instance, no close sock

// These functions will not be called in the child process
void rpcSendRedirectRsp(void *pConn, const SEpSet *pEpSet);
void rpcSendRequestWithCtx(void *thandle, const SEpSet *pEpSet, SRpcMsg *pMsg, int64_t *rid, SRpcCtx *ctx);
int  rpcGetConnInfo(void *thandle, SRpcConnInfo *pInfo);
void rpcSendRecv(void *shandle, SEpSet *pEpSet, SRpcMsg *pReq, SRpcMsg *pRsp);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TRPC_H
