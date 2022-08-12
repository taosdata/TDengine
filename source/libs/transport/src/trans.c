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

#ifdef USE_UV

#include "transComm.h"

void* (*taosInitHandle[])(uint32_t ip, uint32_t port, char* label, int32_t numOfThreads, void* fp, void* shandle) = {
    transInitServer, transInitClient};

void (*taosCloseHandle[])(void* arg) = {transCloseServer, transCloseClient};

void (*taosRefHandle[])(void* handle) = {transRefSrvHandle, transRefCliHandle};
void (*taosUnRefHandle[])(void* handle) = {transUnrefSrvHandle, transUnrefCliHandle};

int (*transReleaseHandle[])(void* handle) = {transReleaseSrvHandle, transReleaseCliHandle};

static int32_t transValidLocalFqdn(const char* localFqdn, uint32_t* ip) {
  *ip = taosGetIpv4FromFqdn(localFqdn);
  if (*ip == 0xFFFFFFFF) {
    terrno = TSDB_CODE_RPC_FQDN_ERROR;
    return -1;
  }
  return 0;
}
void* rpcOpen(const SRpcInit* pInit) {
  rpcInit();

  SRpcInfo* pRpc = taosMemoryCalloc(1, sizeof(SRpcInfo));
  if (pRpc == NULL) {
    return NULL;
  }
  if (pInit->label) {
    tstrncpy(pRpc->label, pInit->label, strlen(pInit->label) + 1);
  }
  // register callback handle
  pRpc->cfp = pInit->cfp;
  pRpc->retry = pInit->rfp;
  pRpc->startTimer = pInit->tfp;

  if (pInit->connType == TAOS_CONN_SERVER) {
    pRpc->numOfThreads = pInit->numOfThreads > TSDB_MAX_RPC_THREADS ? TSDB_MAX_RPC_THREADS : pInit->numOfThreads;
  } else {
    pRpc->numOfThreads = pInit->numOfThreads > TSDB_MAX_RPC_THREADS ? TSDB_MAX_RPC_THREADS : pInit->numOfThreads;
  }

  uint32_t ip = 0;
  if (pInit->connType == TAOS_CONN_SERVER) {
    if (transValidLocalFqdn(pInit->localFqdn, &ip) != 0) {
      tError("invalid fqdn:%s, errmsg:%s", pInit->localFqdn, terrstr());
      taosMemoryFree(pRpc);
      return NULL;
    }
  }

  pRpc->connType = pInit->connType;
  pRpc->idleTime = pInit->idleTime;
  pRpc->tcphandle =
      (*taosInitHandle[pRpc->connType])(ip, pInit->localPort, pRpc->label, pRpc->numOfThreads, NULL, pRpc);
  if (pRpc->tcphandle == NULL) {
    taosMemoryFree(pRpc);
    return NULL;
  }
  pRpc->parent = pInit->parent;
  if (pInit->user) {
    memcpy(pRpc->user, pInit->user, strlen(pInit->user));
  }

  int64_t refId = transAddExHandle(transGetInstMgt(), pRpc);
  transAcquireExHandle(transGetInstMgt(), refId);
  pRpc->refId = refId;
  return (void*)refId;
}
void rpcClose(void* arg) {
  tInfo("start to close rpc");
  transRemoveExHandle(transGetInstMgt(), (int64_t)arg);
  transReleaseExHandle(transGetInstMgt(), (int64_t)arg);
  tInfo("rpc is closed");
  return;
}
void rpcCloseImpl(void* arg) {
  SRpcInfo* pRpc = (SRpcInfo*)arg;
  (*taosCloseHandle[pRpc->connType])(pRpc->tcphandle);
  taosMemoryFree(pRpc);
}

void* rpcMallocCont(int32_t contLen) {
  int32_t size = contLen + TRANS_MSG_OVERHEAD;
  char*   start = taosMemoryCalloc(1, size);
  if (start == NULL) {
    tError("failed to malloc msg, size:%d", size);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  } else {
    tTrace("malloc mem:%p size:%d", start, size);
  }

  return start + sizeof(STransMsgHead);
}

void rpcFreeCont(void* cont) {
  if (cont == NULL) return;
  taosMemoryFree((char*)cont - TRANS_MSG_OVERHEAD);
  tTrace("rpc free cont:%p", (char*)cont - TRANS_MSG_OVERHEAD);
}

void* rpcReallocCont(void* ptr, int32_t contLen) {
  if (ptr == NULL) return rpcMallocCont(contLen);

  char*   st = (char*)ptr - TRANS_MSG_OVERHEAD;
  int32_t sz = contLen + TRANS_MSG_OVERHEAD;
  st = taosMemoryRealloc(st, sz);
  if (st == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  return st + TRANS_MSG_OVERHEAD;
}

int32_t rpcReportProgress(void* pConn, char* pCont, int32_t contLen) { return -1; }
void    rpcCancelRequest(int64_t rid) { return; }

int rpcSendRequest(void* shandle, const SEpSet* pEpSet, SRpcMsg* pMsg, int64_t* pRid) {
  return transSendRequest(shandle, pEpSet, pMsg, NULL);
}
int rpcSendRequestWithCtx(void* shandle, const SEpSet* pEpSet, SRpcMsg* pMsg, int64_t* pRid, SRpcCtx* pCtx) {
  return transSendRequest(shandle, pEpSet, pMsg, pCtx);
}
int rpcSendRecv(void* shandle, SEpSet* pEpSet, SRpcMsg* pMsg, SRpcMsg* pRsp) {
  return transSendRecv(shandle, pEpSet, pMsg, pRsp);
}

int rpcSendResponse(const SRpcMsg* pMsg) { return transSendResponse(pMsg); }

void rpcRefHandle(void* handle, int8_t type) {
  assert(type == TAOS_CONN_SERVER || type == TAOS_CONN_CLIENT);
  (*taosRefHandle[type])(handle);
}

void rpcUnrefHandle(void* handle, int8_t type) {
  assert(type == TAOS_CONN_SERVER || type == TAOS_CONN_CLIENT);
  (*taosUnRefHandle[type])(handle);
}

int rpcRegisterBrokenLinkArg(SRpcMsg* msg) { return transRegisterMsg(msg); }
int rpcReleaseHandle(void* handle, int8_t type) {
  assert(type == TAOS_CONN_SERVER || type == TAOS_CONN_CLIENT);
  return (*transReleaseHandle[type])(handle);
}

int rpcSetDefaultAddr(void* thandle, const char* ip, const char* fqdn) {
  // later
  return transSetDefaultAddr(thandle, ip, fqdn);
}

void* rpcAllocHandle() { return (void*)transAllocHandle(); }

int32_t rpcInit() {
  transInit();
  return 0;
}
void rpcCleanup(void) {
  transCleanup();
  return;
}

#endif
