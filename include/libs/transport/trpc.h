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
#include "ttrace.h"

#define TAOS_CONN_SERVER 0
#define TAOS_CONN_CLIENT 1
#define IsReq(pMsg)      (pMsg->msgType & 1U)

extern int32_t tsRpcHeadSize;

typedef struct {
  uint32_t clientIp;
  uint16_t clientPort;
  int64_t  applyIndex;
  uint64_t applyTerm;
  char     user[TSDB_USER_LEN];

} SRpcConnInfo;

typedef struct SRpcHandleInfo {
  // rpc info
  void   *handle;         // rpc handle returned to app
  int64_t refId;          // refid, used by server
  int8_t  noResp;         // has response or not(default 0, 0: resp, 1: no resp)
  int8_t  persistHandle;  // persist handle or not
  int8_t  hasEpSet;
  int32_t cliVer;

  // app info
  void *ahandle;  // app handle set by client
  void *wrapper;  // wrapper handle
  void *node;     // node mgmt handle

  // resp info
  void   *rsp;
  int32_t rspLen;

  STraceId traceId;

  SRpcConnInfo conn;
  int8_t       forbiddenIp;

} SRpcHandleInfo;

typedef struct SRpcMsg {
  tmsg_t         msgType;
  void          *pCont;
  int32_t        contLen;
  int32_t        code;
  SRpcHandleInfo info;
} SRpcMsg;

typedef void (*RpcCfp)(void *parent, SRpcMsg *, SEpSet *epset);
typedef bool (*RpcRfp)(int32_t code, tmsg_t msgType);
typedef bool (*RpcTfp)(int32_t code, tmsg_t msgType);
typedef bool (*RpcFFfp)(tmsg_t msgType);
typedef void (*RpcDfp)(void *ahandle);

typedef struct SRpcInit {
  char     localFqdn[TSDB_FQDN_LEN];
  uint16_t localPort;     // local port
  char    *label;         // for debug purpose
  int32_t  numOfThreads;  // number of threads to handle connections
  int32_t  sessions;      // number of sessions allowed
  int8_t   connType;      // TAOS_CONN_UDP, TAOS_CONN_TCPC, TAOS_CONN_TCPS
  int32_t  idleTime;      // milliseconds, 0 means idle timer is disabled
  int32_t  compatibilityVer;

  int32_t retryMinInterval;  // retry init interval
  int32_t retryStepFactor;   // retry interval factor
  int32_t retryMaxInterval;  // retry max interval
  int64_t retryMaxTimeout;

  int32_t failFastThreshold;
  int32_t failFastInterval;

  int32_t compressSize;  // -1: no compress, 0 : all data compressed, size: compress data if larger than size
  int8_t  encryption;    // encrypt or not

  // the following is for client app ecurity only
  char *user;  // user name

  // call back to process incoming msg
  RpcCfp cfp;

  // retry not not for particular msg
  RpcRfp rfp;

  // set up timeout for particular msg
  RpcTfp tfp;

  // destroy client ahandle;
  RpcDfp dfp;
  // fail fast fp
  RpcFFfp ffp;

  int32_t connLimitNum;
  int32_t connLimitLock;
  int32_t timeToGetConn;
  int8_t  supportBatch;  // 0: no batch, 1. batch
  int32_t batchSize;
  void   *parent;
} SRpcInit;

typedef struct {
  void *val;
  int32_t (*clone)(void *src, void **dst);
} SRpcCtxVal;

typedef struct {
  int32_t msgType;
  void   *val;
  int32_t (*clone)(void *src, void **dst);
} SRpcBrokenlinkVal;

typedef struct {
  SHashObj         *args;
  SRpcBrokenlinkVal brokenVal;
  void (*freeFunc)(const void *arg);
} SRpcCtx;

int32_t rpcInit();
void    rpcCleanup();

void *rpcOpen(const SRpcInit *pRpc);
void  rpcClose(void *);
void  rpcCloseImpl(void *);
void *rpcMallocCont(int64_t contLen);
void  rpcFreeCont(void *pCont);
void *rpcReallocCont(void *ptr, int64_t contLen);

// Because taosd supports multi-process mode
// These functions should not be used on the server side
// Please use tmsg<xx> functions, which are defined in tmsgcb.h
int rpcSendRequest(void *thandle, const SEpSet *pEpSet, SRpcMsg *pMsg, int64_t *rid);
int rpcSendResponse(const SRpcMsg *pMsg);
int rpcRegisterBrokenLinkArg(SRpcMsg *msg);
int rpcReleaseHandle(void *handle, int8_t type);  // just release conn to rpc instance, no close sock

// These functions will not be called in the child process
int   rpcSendRequestWithCtx(void *thandle, const SEpSet *pEpSet, SRpcMsg *pMsg, int64_t *rid, SRpcCtx *ctx);
int   rpcSendRecv(void *shandle, SEpSet *pEpSet, SRpcMsg *pReq, SRpcMsg *pRsp);
int   rpcSendRecvWithTimeout(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp, int8_t *epUpdated,
                             int32_t timeoutMs);
int   rpcSetDefaultAddr(void *thandle, const char *ip, const char *fqdn);
void *rpcAllocHandle();
void  rpcSetIpWhite(void *thandl, void *arg);

int32_t rpcUtilSIpRangeToStr(SIpV4Range *pRange, char *buf);

int32_t rpcUtilSWhiteListToStr(SIpWhiteList *pWhiteList, char **ppBuf);
int32_t rpcCvtErrCode(int32_t code);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TRPC_H
