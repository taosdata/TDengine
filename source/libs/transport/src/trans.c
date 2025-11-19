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

#include "transComm.h"
#include "transTLS.h"

#ifndef TD_ASTRA_RPC
void* (*taosInitHandle[])(SIpAddr* addr, char* label, int32_t numOfThreads, void* fp, void* pInit) = {transInitServer,
                                                                                                      transInitClient};

void (*taosCloseHandle[])(void* arg) = {transCloseServer, transCloseClient};

void (*taosRefHandle[])(void* handle) = {transRefSrvHandle, transRefCliHandle};
void (*taosUnRefHandle[])(void* handle) = {transUnrefSrvHandle, NULL};

int (*transReleaseHandle[])(void* handle, int32_t status) = {transReleaseSrvHandle, transReleaseCliHandle};

static int32_t transValidLocalFqdn(const char* localFqdn, SIpAddr* addr) {
  int32_t code = taosGetIpFromFqdn(tsEnableIpv6, localFqdn, addr);
  if (code != 0) {
    return TSDB_CODE_RPC_FQDN_ERROR;
  }
  return 0;
}
void* rpcOpen(const SRpcInit* pInit) {
  int32_t code = rpcInit();
  if (code != 0) {
    TAOS_CHECK_GOTO(code, NULL, _end);
  }

  SRpcInfo* pRpc = taosMemoryCalloc(1, sizeof(SRpcInfo));
  if (pRpc == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _end);
  }

  if (pInit->label) {
    int len = strlen(pInit->label) > sizeof(pRpc->label) ? sizeof(pRpc->label) : strlen(pInit->label);
    memcpy(pRpc->label, pInit->label, len);
  }

  pRpc->startReadTimer = pInit->startReadTimer;
  pRpc->compressSize = pInit->compressSize;
  if (pRpc->compressSize < 0) {
    pRpc->compressSize = -1;
  }

  pRpc->encryption = pInit->encryption;
  pRpc->compatibilityVer = pInit->compatibilityVer;

  pRpc->retryMinInterval = pInit->retryMinInterval;  // retry init interval
  pRpc->retryStepFactor = pInit->retryStepFactor;
  pRpc->retryMaxInterval = pInit->retryMaxInterval;
  pRpc->retryMaxTimeout = pInit->retryMaxTimeout;

  pRpc->failFastThreshold = pInit->failFastThreshold;
  pRpc->failFastInterval = pInit->failFastInterval;

  // register callback handle
  pRpc->cfp = pInit->cfp;
  pRpc->retry = pInit->rfp;
  pRpc->startTimer = pInit->tfp;
  pRpc->destroyFp = pInit->dfp;
  pRpc->failFastFp = pInit->ffp;
  pRpc->noDelayFp = pInit->noDelayFp;
  pRpc->connLimitNum = pInit->connLimitNum;
  if (pRpc->connLimitNum == 0) {
    pRpc->connLimitNum = 20;
  }

  pRpc->connLimitLock = pInit->connLimitLock;
  pRpc->supportBatch = pInit->supportBatch;
  pRpc->shareConnLimit = pInit->shareConnLimit;
  if (pRpc->shareConnLimit <= 0) {
    pRpc->shareConnLimit = BUFFER_LIMIT;
  }

  pRpc->readTimeout = pInit->readTimeout;
  if (pRpc->readTimeout < 0) {
    pRpc->readTimeout = INT64_MAX;
  }

  pRpc->numOfThreads = pInit->numOfThreads > TSDB_MAX_RPC_THREADS ? TSDB_MAX_RPC_THREADS : pInit->numOfThreads;
  if (pRpc->numOfThreads <= 0) {
    pRpc->numOfThreads = 1;
  }

  SIpAddr addr = {0};
  if (pInit->connType == TAOS_CONN_SERVER) {
    if ((code = transValidLocalFqdn(pInit->localFqdn, &addr)) != 0) {
      tError("invalid fqdn:%s, errmsg:%s", pInit->localFqdn, tstrerror(code));
      TAOS_CHECK_GOTO(code, NULL, _end);
    }
  }
  addr.port = pInit->localPort;

  pRpc->connType = pInit->connType;
  pRpc->idleTime = pInit->idleTime;
  pRpc->parent = pInit->parent;
  if (pInit->user) {
    tstrncpy(pRpc->user, pInit->user, sizeof(pRpc->user));
  }
  pRpc->timeToGetConn = pInit->timeToGetConn;
  if (pRpc->timeToGetConn == 0) {
    pRpc->timeToGetConn = 10 * 1000;
  }
  pRpc->notWaitAvaliableConn = pInit->notWaitAvaliableConn;
  pRpc->ipv6 = pInit->ipv6;

  if (pInit->enableSSL == 1) {
    code = transTlsCtxCreate(pInit, (SSslCtx**)&pRpc->pSSLContext);
    TAOS_CHECK_GOTO(code, NULL, _end);

    if (pRpc->pSSLContext == NULL) {
      tError("Failed to create SSL context for %s", pRpc->label);
      TAOS_CHECK_GOTO(TSDB_CODE_THIRDPARTY_ERROR, NULL, _end);
    }
    tInfo("TLS is enabled for %s", pRpc->label);
    pRpc->enableSSL = 1;
  } else {
    tInfo("TLS is not enabled for %s", pRpc->label);
    pRpc->enableSSL = 0;
  }

  pRpc->tcphandle = (*taosInitHandle[pRpc->connType])(&addr, pRpc->label, pRpc->numOfThreads, NULL, pRpc);

  if (pRpc->tcphandle == NULL) {
    tError("failed to init rpc handle");
    TAOS_CHECK_GOTO(terrno, NULL, _end);
  }
  pRpc->shareConn = pInit->shareConn;

  int64_t refId = transAddExHandle(transGetInstMgt(), pRpc);
  TAOS_UNUSED(transAcquireExHandle(transGetInstMgt(), refId));

  code = transCachePut(refId, (STrans*)pRpc);
  TAOS_CHECK_GOTO(code, NULL, _end);

  pRpc->refId = refId;

  return (void*)refId;
_end:
  if (pRpc->pSSLContext) {
    transTlsCtxDestroy((SSslCtx*)pRpc->pSSLContext);
  }
  taosMemoryFree(pRpc);
  terrno = code;

  return NULL;
}
void rpcClose(void* arg) {
  tInfo("start to close rpc");
  if (arg == NULL) {
    return;
  }
  transCacheRemoveByRefId((int64_t)arg);
  transRemoveExHandle(transGetInstMgt(), (int64_t)arg);
  transReleaseExHandle(transGetInstMgt(), (int64_t)arg);
  tInfo("end to close rpc");
  return;
}
void rpcCloseImpl(void* arg) {
  if (arg == NULL) return;
  SRpcInfo* pRpc = (SRpcInfo*)arg;
  if (pRpc->tcphandle != NULL) {
    (*taosCloseHandle[pRpc->connType])(pRpc->tcphandle);
  }
  transTlsCtxDestroy((SSslCtx*)pRpc->pSSLContext);
  taosMemoryFree(pRpc);
}

void* rpcMallocCont(int64_t contLen) {
  int64_t size = contLen + TRANS_MSG_OVERHEAD;
  char*   start = taosMemoryCalloc(1, size);
  if (start == NULL) {
    tError("failed to malloc msg, size:%" PRId64, size);
    return NULL;
  } else {
    tTrace("cont:%p, rpc malloc size:%" PRId64, start, size);
  }

  return start + TRANS_MSG_OVERHEAD;
}

void rpcFreeCont(void* cont) { transFreeMsg(cont); }

void* rpcReallocCont(void* ptr, int64_t contLen) {
  if (ptr == NULL) return rpcMallocCont(contLen);

  char*   st = (char*)ptr - TRANS_MSG_OVERHEAD;
  int64_t sz = contLen + TRANS_MSG_OVERHEAD;
  char*   nst = taosMemoryRealloc(st, sz);
  if (nst == NULL) {
    taosMemoryFree(st);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  } else {
    st = nst;
  }

  return st + TRANS_MSG_OVERHEAD;
}

int32_t rpcSendRequest(void* pInit, const SEpSet* pEpSet, SRpcMsg* pMsg, int64_t* pRid) {
  return transSendRequest(pInit, pEpSet, pMsg, NULL);
}
int32_t rpcSendRequestWithCtx(void* pInit, const SEpSet* pEpSet, SRpcMsg* pMsg, int64_t* pRid, SRpcCtx* pCtx) {
  if (pCtx != NULL || pMsg->info.handle != 0 || pMsg->info.noResp != 0 || pRid == NULL) {
    return transSendRequest(pInit, pEpSet, pMsg, pCtx);
  } else {
    return transSendRequestWithId(pInit, pEpSet, pMsg, pRid);
  }
}

int32_t rpcSendRequestWithId(void* pInit, const SEpSet* pEpSet, STransMsg* pReq, int64_t* transpointId) {
  return transSendRequestWithId(pInit, pEpSet, pReq, transpointId);
}

int32_t rpcSendRecv(void* pInit, SEpSet* pEpSet, SRpcMsg* pMsg, SRpcMsg* pRsp) {
  return transSendRecv(pInit, pEpSet, pMsg, pRsp);
}
int32_t rpcSendRecvWithTimeout(void* pInit, SEpSet* pEpSet, SRpcMsg* pMsg, SRpcMsg* pRsp, int8_t* epUpdated,
                               int32_t timeoutMs) {
  return transSendRecvWithTimeout(pInit, pEpSet, pMsg, pRsp, epUpdated, timeoutMs);
}
int32_t rpcFreeConnById(void* pInit, int64_t connId) { return transFreeConnById(pInit, connId); }

int32_t rpcSendResponse(const SRpcMsg* pMsg) { return transSendResponse(pMsg); }

void rpcRefHandle(void* handle, int8_t type) { (*taosRefHandle[type])(handle); }

void rpcUnrefHandle(void* handle, int8_t type) { (*taosUnRefHandle[type])(handle); }

int32_t rpcRegisterBrokenLinkArg(SRpcMsg* msg) { return transRegisterMsg(msg); }
int32_t rpcReleaseHandle(void* handle, int8_t type, int32_t status) {
  return (*transReleaseHandle[type])(handle, status);
}

// client only
int32_t rpcSetDefaultAddr(void* thandle, const char* ip, const char* fqdn) {
  // later
  return transSetDefaultAddr(thandle, ip, fqdn);
}
// server only
int32_t rpcSetIpWhite(void* thandle, void* arg) { return transSetIpWhiteList(thandle, arg, NULL); }

int32_t rpcAllocHandle(int64_t* refId) { return transAllocHandle(refId); }

int32_t rpcUtilSIpRangeToStr(SIpV4Range* pRange, char* buf) { return transUtilSIpRangeToStr(pRange, buf); }
int32_t rpcUtilSWhiteListToStr(SIpWhiteListDual* pWhiteList, char** ppBuf) {
  return transUtilSWhiteListToStr(pWhiteList, ppBuf);
}

int32_t rpcCvtErrCode(int32_t code) {
  if (code == TSDB_CODE_RPC_BROKEN_LINK || code == TSDB_CODE_RPC_NETWORK_UNAVAIL) {
    return TSDB_CODE_RPC_NETWORK_ERROR;
  }
  return code;
}

int32_t rpcInit() { return transInit(); }

void rpcCleanup(void) {
  transCleanup();
  transHttpEnvDestroy();

  return;
}
#else
#ifdef TD_ASTRA_RPC
void* (*taosInitHandle[])(uint32_t ip, uint32_t port, char* label, int32_t numOfThreads, void* fp, void* shandle) = {
    transInitServer, transInitClient};
void (*taosCloseHandle[])(void* arg) = {transCloseServer, transCloseClient};
int (*transReleaseHandle[])(void* handle, int32_t status) = {transReleaseSrvHandle, transReleaseCliHandle};
#else
void* (*taosInitHandle[])(uint32_t ip, uint32_t port, char* label, int32_t numOfThreads, void* fp, void* shandle) = {
    transInitServer, transInitClient};
void (*taosCloseHandle[])(void* arg) = {transCloseServer, transCloseClient};
int (*transReleaseHandle[])(void* handle) = {transReleaseSrvHandle, transReleaseCliHandle};
#endif

static int32_t transValidLocalFqdn(const char* localFqdn, SIpAddr* ip) { return 0; }
typedef struct {
  char*    lablset;
  RPC_TYPE type;
} SLableSet;
static SLableSet labelSet[] = {
    {"TSC", TD_ASTRA_CLIENT | TD_ASTRA_DSVR},
    {"DNODE-CLI", TD_ASTRA_DSVR_CLIENT | TD_ASTRA_DSVR},
    {"DNODE-STA-CLI", TD_ASTRA_DSVR_STA_CLIENT | TD_ASTRA_DSVR},
    {"DNODE-SYNC-CLI", TD_ASTRA_DSVR_SYNC_CLIENT | TD_ASTRA_DSVR},
    {"DND-S", TD_ASTRA_DSVR},
};

void* rpcOpen(const SRpcInit* pInit) {
  int32_t code = rpcInit();
  if (code != 0) {
    TAOS_CHECK_GOTO(code, NULL, _end);
  }

  SRpcInfo* pRpc = taosMemoryCalloc(1, sizeof(SRpcInfo));
  if (pRpc == NULL) {
    TAOS_CHECK_GOTO(terrno, NULL, _end);
  }
  if (pInit->label) {
    int len = strlen(pInit->label) > sizeof(pRpc->label) ? sizeof(pRpc->label) : strlen(pInit->label);
    memcpy(pRpc->label, pInit->label, len);
  }

  pRpc->compressSize = pInit->compressSize;
  if (pRpc->compressSize < 0) {
    pRpc->compressSize = -1;
  }

  pRpc->encryption = pInit->encryption;
  pRpc->compatibilityVer = pInit->compatibilityVer;

  pRpc->retryMinInterval = pInit->retryMinInterval;  // retry init interval
  pRpc->retryStepFactor = pInit->retryStepFactor;
  pRpc->retryMaxInterval = pInit->retryMaxInterval;
  pRpc->retryMaxTimeout = pInit->retryMaxTimeout;

  pRpc->failFastThreshold = pInit->failFastThreshold;
  pRpc->failFastInterval = pInit->failFastInterval;

  // register callback handle
  pRpc->cfp = pInit->cfp;
  pRpc->retry = pInit->rfp;
  pRpc->startTimer = pInit->tfp;
  pRpc->destroyFp = pInit->dfp;
  pRpc->failFastFp = pInit->ffp;
  pRpc->noDelayFp = pInit->noDelayFp;
  pRpc->connLimitNum = pInit->connLimitNum;
  if (pRpc->connLimitNum == 0) {
    pRpc->connLimitNum = 20;
  }

  pRpc->connLimitLock = pInit->connLimitLock;
  pRpc->supportBatch = pInit->supportBatch;
  pRpc->batchSize = pInit->batchSize;

  pRpc->numOfThreads = pInit->numOfThreads > TSDB_MAX_RPC_THREADS ? TSDB_MAX_RPC_THREADS : pInit->numOfThreads;
  if (pRpc->numOfThreads <= 0) {
    pRpc->numOfThreads = 1;
  }

  if (pInit->connType == TAOS_CONN_SERVER) {
    SIpAddr addr = {0};
    if ((code = transValidLocalFqdn(pInit->localFqdn, &ip)) != 0) {
      tError("invalid fqdn:%s, errmsg:%s", pInit->localFqdn, tstrerror(code));
      TAOS_CHECK_GOTO(code, NULL, _end);
    }
  }

  pRpc->connType = pInit->connType;
  pRpc->idleTime = pInit->idleTime;
  pRpc->parent = pInit->parent;
  if (pInit->user) {
    tstrncpy(pRpc->user, pInit->user, sizeof(pRpc->user));
  }
  pRpc->timeToGetConn = pInit->timeToGetConn;
  if (pRpc->timeToGetConn == 0) {
    pRpc->timeToGetConn = 10 * 1000;
  }
  pRpc->notWaitAvaliableConn = pInit->notWaitAvaliableConn;

  pRpc->tcphandle =
      (*taosInitHandle[pRpc->connType])(ip, pInit->localPort, pRpc->label, pRpc->numOfThreads, NULL, pRpc);

  if (pRpc->tcphandle == NULL) {
    // tError("failed to init rpc handle");
    // TAOS_CHECK_GOTO(terrno, NULL, _end);
  }
  for (int8_t i = 0; i < sizeof(labelSet) / sizeof(labelSet[0]); i++) {
    if (strcmp(labelSet[i].lablset, pRpc->label) == 0) {
      pRpc->type = labelSet[i].type;
      break;
    }
  }

  taosThreadMutexInit(&pRpc->sidMutx, NULL);
  pRpc->sidTable = taosHashInit(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);

  taosThreadMutexInit(&pRpc->seqMutex, NULL);
  pRpc->seqTable = taosHashInit(4096, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  pRpc->seq = 1;
  transUpdateCb(pRpc->type, pRpc);

  int64_t refId = transAddExHandle(transGetInstMgt(), pRpc);
  void*   tmp = transAcquireExHandle(transGetInstMgt(), refId);
  pRpc->refId = refId;
  return (void*)refId;
_end:
  taosMemoryFree(pRpc);
  terrno = code;

  return NULL;
}
void rpcClose(void* arg) {
  tInfo("start to close rpc");
  return;
}
void rpcCloseImpl(void* arg) {
  if (arg == NULL) return;
  return;
  // SRpcInfo* pRpc = (SRpcInfo*)arg;
  // if (pRpc->tcphandle != NULL) {
  //   (*taosCloseHandle[pRpc->connType])(pRpc->tcphandle);
  // }
  // taosMemoryFree(pRpc);
}

void* rpcMallocCont(int64_t contLen) {
  int64_t size = contLen + TRANS_MSG_OVERHEAD;
  char*   start = taosMemoryCalloc(1, size);
  if (start == NULL) {
    tError("failed to malloc msg, size:%" PRId64, size);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  } else {
    tTrace("malloc mem:%p size:%" PRId64, start, size);
  }

  return start + sizeof(STransMsgHead);
}

void rpcFreeCont(void* cont) { transFreeMsg(cont); }

void* rpcReallocCont(void* ptr, int64_t contLen) {
  if (ptr == NULL) return rpcMallocCont(contLen);

  char*   st = (char*)ptr - TRANS_MSG_OVERHEAD;
  int64_t sz = contLen + TRANS_MSG_OVERHEAD;
  char*   nst = taosMemoryRealloc(st, sz);
  if (nst == NULL) {
    taosMemoryFree(st);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  } else {
    st = nst;
  }

  return st + TRANS_MSG_OVERHEAD;
}

int32_t rpcSendRequest(void* shandle, const SEpSet* pEpSet, SRpcMsg* pMsg, int64_t* pRid) {
  return transSendRequest(shandle, pEpSet, pMsg, NULL);
#ifdef TD_ASTRA_RPC
  // return transSendRequest2(shandle, pEpSet, pMsg, NULL);
#else
  return transSendRequest(shandle, pEpSet, pMsg, NULL);
#endif
}
int32_t rpcSendRequestWithCtx(void* shandle, const SEpSet* pEpSet, SRpcMsg* pMsg, int64_t* pRid, SRpcCtx* pCtx) {
  if (pCtx != NULL || pMsg->info.handle != 0 || pMsg->info.noResp != 0 || pRid == NULL) {
    return transSendRequest(shandle, pEpSet, pMsg, pCtx);
  } else {
    return transSendRequestWithId(shandle, pEpSet, pMsg, pRid);
  }
#ifdef TD_ASTRA_RPC
  // if (pCtx != NULL || pMsg->info.handle != 0 || pMsg->info.noResp != 0 || pRid == NULL) {
  //   return transSendRequest2(shandle, pEpSet, pMsg, pCtx);
  // } else {
  //   return transSendRequestWithId2(shandle, pEpSet, pMsg, pRid);
  // }
#else
  if (pCtx != NULL || pMsg->info.handle != 0 || pMsg->info.noResp != 0 || pRid == NULL) {
    return transSendRequest(shandle, pEpSet, pMsg, pCtx);
  } else {
    return transSendRequestWithId(shandle, pEpSet, pMsg, pRid);
  }
#endif
}

int32_t rpcSendRequestWithId(void* shandle, const SEpSet* pEpSet, STransMsg* pReq, int64_t* transpointId) {
  return transSendRequestWithId(shandle, pEpSet, pReq, transpointId);
#ifdef TD_ASTRA_RPC
  // return transSendRequestWithId2(shandle, pEpSet, pReq, transpointId);
#else
  return transSendRequestWithId(shandle, pEpSet, pReq, transpointId);
#endif
}

int32_t rpcSendRecv(void* shandle, SEpSet* pEpSet, SRpcMsg* pMsg, SRpcMsg* pRsp) {
  return transSendRecv(shandle, pEpSet, pMsg, pRsp);
#ifdef TD_ASTRA_RPC
  // return transSendRecv2(shandle, pEpSet, pMsg, pRsp);
#else
  return transSendRecv(shandle, pEpSet, pMsg, pRsp);
#endif
}
int32_t rpcSendRecvWithTimeout(void* shandle, SEpSet* pEpSet, SRpcMsg* pMsg, SRpcMsg* pRsp, int8_t* epUpdated,
                               int32_t timeoutMs) {
  return transSendRecvWithTimeout(shandle, pEpSet, pMsg, pRsp, epUpdated, timeoutMs);
#ifdef TD_ASTRA_RPC
  // return transSendRecvWithTimeout2(shandle, pEpSet, pMsg, pRsp, epUpdated, timeoutMs);
#else
  return transSendRecvWithTimeout(shandle, pEpSet, pMsg, pRsp, epUpdated, timeoutMs);
#endif
}
int32_t rpcFreeConnById(void* shandle, int64_t connId) {
  return transFreeConnById(shandle, connId);
#ifdef TD_ASTRA_RPC
  // return transFreeConnById2(shandle, connId);
#else
  return transFreeConnById(shandle, connId);
#endif
}

int32_t rpcSendResponse(SRpcMsg* pMsg) {
  return transSendResponse(pMsg);
#ifdef TD_ASTRA_RPC
  // return transSendResponse2(pMsg);
#else
  return transSendResponse(pMsg);
#endif
}

int32_t rpcRegisterBrokenLinkArg(SRpcMsg* msg) {
  return transRegisterMsg(msg);
#ifdef TD_ASTRA_RPC
  // return transRegisterMsg2(msg);
#else
  return transRegisterMsg(msg);
#endif
}
int32_t rpcReleaseHandle(void* handle, int8_t type, int32_t status) {
  return (*transReleaseHandle[type])(handle, status);
}

// client only
int32_t rpcSetDefaultAddr(void* thandle, const char* ip, const char* fqdn) {
  // later
  return transSetDefaultAddr(thandle, ip, fqdn);
#ifdef TD_ASTRA_RPC
  // return transSetDefaultAddr2(thandle, ip, fqdn);
#else
  return transSetDefaultAddr(thandle, ip, fqdn);
#endif
}
// server only
int32_t rpcSetIpWhite(void* thandle, void* arg) {
  return transSetIpWhiteList(thandle, arg, NULL);
#ifdef TD_ASTRA_RPC
  // return transSetIpWhiteList2(thandle, arg, NULL);
#else
  return transSetIpWhiteList(thandle, arg, NULL);
#endif
}

int32_t rpcAllocHandle(int64_t* refId) { return transAllocHandle(refId); }

int32_t rpcUtilSIpRangeToStr(SIpV4Range* pRange, char* buf) { return transUtilSIpRangeToStr(pRange, buf); }
int32_t rpcUtilSWhiteListToStr(SIpWhiteList* pWhiteList, char** ppBuf) {
  return transUtilSWhiteListToStr(pWhiteList, ppBuf);
}

int32_t rpcCvtErrCode(int32_t code) {
  if (code == TSDB_CODE_RPC_BROKEN_LINK || code == TSDB_CODE_RPC_NETWORK_UNAVAIL) {
    return TSDB_CODE_RPC_NETWORK_ERROR;
  }
  return code;
}

int32_t rpcInit() { return transInit(); }

void rpcCleanup(void) {
  transCleanup();
  // transHttpEnvDestroy();

  return;
}

#endif