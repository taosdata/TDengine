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

#ifndef TD_ASTRA_RPC
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
  int8_t       notFreeAhandle;
  int8_t       compressed;
  int64_t      seqNum;  // msg seq
  int64_t      qId;     // queryId Get from client, other req's qId = -1;
  int32_t      refIdMgt;
  int32_t      msgType;
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
typedef bool (*RpcNoDelayfp)(tmsg_t msgType);
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

  RpcNoDelayfp noDelayFp;

  int32_t connLimitNum;
  int32_t connLimitLock;
  int32_t timeToGetConn;
  int8_t  supportBatch;  // 0: no batch, 1. batch
  int32_t shareConnLimit;
  int8_t  shareConn;             // 0: no share, 1. share
  int8_t  notWaitAvaliableConn;  // 1: wait to get, 0: no wait
  int8_t  startReadTimer;
  int64_t readTimeout;  // s

  void *parent;
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
  int64_t st;
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
int32_t rpcSendRequest(void *thandle, const SEpSet *pEpSet, SRpcMsg *pMsg, int64_t *rid);
int32_t rpcSendResponse(const SRpcMsg *pMsg);
int32_t rpcRegisterBrokenLinkArg(SRpcMsg *msg);
int32_t rpcReleaseHandle(void *handle, int8_t type, int32_t code);  // just release conn to rpc instance, no close sock

// These functions will not be called in the child process
int32_t rpcSendRequestWithCtx(void *thandle, const SEpSet *pEpSet, SRpcMsg *pMsg, int64_t *rid, SRpcCtx *ctx);
int32_t rpcSendRecv(void *shandle, SEpSet *pEpSet, SRpcMsg *pReq, SRpcMsg *pRsp);
int32_t rpcSendRecvWithTimeout(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp, int8_t *epUpdated,
                               int32_t timeoutMs);

int32_t rpcFreeConnById(void *shandle, int64_t connId);

int32_t rpcSetDefaultAddr(void *thandle, const char *ip, const char *fqdn);
int32_t rpcAllocHandle(int64_t *refId);
int32_t rpcSetIpWhite(void *thandl, void *arg);

int32_t rpcUtilSIpRangeToStr(SIpV4Range *pRange, char *buf);

int32_t rpcUtilSWhiteListToStr(SIpWhiteList *pWhiteList, char **ppBuf);
int32_t rpcCvtErrCode(int32_t code);

#else
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

typedef enum {
  TD_ASTRA_CLIENT = 1,
  TD_ASTRA_DSVR_CLIENT = 2,
  TD_ASTRA_DSVR_STA_CLIENT = 4,
  TD_ASTRA_DSVR_SYNC_CLIENT = 8,
  TD_ASTRA_DSVR = 16
} RPC_TYPE;

typedef struct SRpcHandleInfo {
  // rpc info
  void   *handle;         // rpc handle returned to app
  int64_t refId;          // refid, used by server
  int8_t  noResp;         // has response or not(default 0, 0: resp, 1: no resp)
  int8_t  persistHandle;  // persist handle or not
  int8_t  hasEpSet;
  int32_t cliVer;

  // app info
  void *ahandle;    // app handle set by client
  void *wrapper;    // wrapper handle
  void *node;       // node mgmt handle
#ifdef TD_ASTRA_32
  void *ahandleEx;  // app handle set by client
#endif

  // resp info
  void   *rsp;
  int32_t rspLen;

  STraceId traceId;

  SRpcConnInfo conn;
  int8_t       forbiddenIp;
  int8_t       notFreeAhandle;
  int8_t       compressed;
  int16_t      connType;
  int64_t      seq;
  int64_t      qId;
  int32_t      msgType;
  void        *reqWithSem;
  int32_t      refIdMgt;
} SRpcHandleInfo;

typedef struct SRpcMsg {
  tmsg_t         msgType;
  void          *pCont;
  int32_t        contLen;
  int32_t        code;
  int32_t        type;
  void          *parent;
  SRpcHandleInfo info;

} SRpcMsg;

typedef void (*RpcCfp)(void *parent, SRpcMsg *, SEpSet *epset);
typedef bool (*RpcRfp)(int32_t code, tmsg_t msgType);
typedef bool (*RpcTfp)(int32_t code, tmsg_t msgType);
typedef bool (*RpcFFfp)(tmsg_t msgType);
typedef bool (*RpcNoDelayfp)(tmsg_t msgType);
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

  RpcNoDelayfp noDelayFp;

  int32_t connLimitNum;
  int32_t connLimitLock;
  int32_t timeToGetConn;
  int8_t  supportBatch;  // 0: no batch, 1. batch
  int32_t batchSize;
  int8_t  notWaitAvaliableConn;  // 1: wait to get, 0: no wait
  int32_t shareConnLimit;
  int8_t  shareConn;  // 0: no share, 1. share
  int8_t  startReadTimer;
  int64_t readTimeout;  // s
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
  int64_t st;
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
int32_t rpcSendRequest(void *thandle, const SEpSet *pEpSet, SRpcMsg *pMsg, int64_t *rid);
int32_t rpcSendResponse(SRpcMsg *pMsg);
int32_t rpcRegisterBrokenLinkArg(SRpcMsg *msg);
int32_t rpcReleaseHandle(void *handle, int8_t type,
                         int32_t status);  // just release conn to rpc instance, no close sock

// These functions will not be called in the child process
int32_t rpcSendRequestWithCtx(void *thandle, const SEpSet *pEpSet, SRpcMsg *pMsg, int64_t *rid, SRpcCtx *ctx);
int32_t rpcSendRecv(void *shandle, SEpSet *pEpSet, SRpcMsg *pReq, SRpcMsg *pRsp);
int32_t rpcSendRecvWithTimeout(void *shandle, SEpSet *pEpSet, SRpcMsg *pMsg, SRpcMsg *pRsp, int8_t *epUpdated,
                               int32_t timeoutMs);

int32_t rpcFreeConnById(void *shandle, int64_t connId);

int32_t rpcSetDefaultAddr(void *thandle, const char *ip, const char *fqdn);
int32_t rpcAllocHandle(int64_t *refId);
int32_t rpcSetIpWhite(void *thandl, void *arg);

int32_t rpcUtilSIpRangeToStr(SIpV4Range *pRange, char *buf);

int32_t rpcUtilSWhiteListToStr(SIpWhiteList *pWhiteList, char **ppBuf);
int32_t rpcCvtErrCode(int32_t code);

#endif

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_TRPC_H
