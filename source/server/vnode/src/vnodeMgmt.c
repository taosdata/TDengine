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

#define _DEFAULT_SOURCE
#include "os.h"

#include "vnodeMgmt.h"
#include "vnodeMgmtMsg.h"

typedef struct {
  SRpcMsg rpcMsg;
  char    pCont[];
} SVnMgmtMsg;

static struct {
  SWorkerPool pool;
  taos_queue  pQueue;
  int32_t (*msgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);
} tsVmgmt = {0};

static int32_t vnodeProcessMgmtStart(void *unused, SVnMgmtMsg *pMgmt, int32_t qtype) {
  SRpcMsg *pMsg = &pMgmt->rpcMsg;
  int32_t  msgType = pMsg->msgType;

  if (tsVmgmt.msgFp[msgType]) {
    vTrace("msg:%p, ahandle:%p type:%s will be processed", pMgmt, pMsg->ahandle, taosMsg[msgType]);
    return (*tsVmgmt.msgFp[msgType])(pMsg);
  } else {
    vError("msg:%p, ahandle:%p type:%s not processed since no handle", pMgmt, pMsg->ahandle, taosMsg[msgType]);
    return TSDB_CODE_DND_MSG_NOT_PROCESSED;
  }
}

static void vnodeSendMgmtEnd(void *unused, SVnMgmtMsg *pMgmt, int32_t qtype, int32_t code) {
  SRpcMsg *pMsg = &pMgmt->rpcMsg;
  SRpcMsg  rsp = {0};

  rsp.code = code;
  vTrace("msg:%p, is processed, code:0x%x", pMgmt, rsp.code);
  if (rsp.code != TSDB_CODE_DND_ACTION_IN_PROGRESS) {
    rsp.handle = pMsg->handle;
    rsp.pCont = NULL;
    rpcSendResponse(&rsp);
  }

  taosFreeQitem(pMsg);
}

static void vnodeInitMgmtReqFp() {
  tsVmgmt.msgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = vnodeProcessCreateVnodeMsg;
  tsVmgmt.msgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE]  = vnodeProcessAlterVnodeMsg;
  tsVmgmt.msgFp[TSDB_MSG_TYPE_MD_SYNC_VNODE]   = vnodeProcessSyncVnodeMsg;
  tsVmgmt.msgFp[TSDB_MSG_TYPE_MD_COMPACT_VNODE]= vnodeProcessCompactVnodeMsg;
  tsVmgmt.msgFp[TSDB_MSG_TYPE_MD_DROP_VNODE]   = vnodeProcessDropVnodeMsg;
  tsVmgmt.msgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = vnodeProcessAlterStreamReq;
}

static int32_t vnodeWriteToMgmtQueue(SRpcMsg *pMsg) {
  int32_t     size = sizeof(SVnMgmtMsg) + pMsg->contLen;
  SVnMgmtMsg *pMgmt = taosAllocateQitem(size);
  if (pMgmt == NULL) return TSDB_CODE_DND_OUT_OF_MEMORY;

  pMgmt->rpcMsg = *pMsg;
  pMgmt->rpcMsg.pCont = pMgmt->pCont;
  memcpy(pMgmt->pCont, pMsg->pCont, pMsg->contLen);
  taosWriteQitem(tsVmgmt.pQueue, TAOS_QTYPE_RPC, pMgmt);

  return TSDB_CODE_SUCCESS;
}

void vnodeProcessMgmtMsg(SRpcMsg *pMsg) {
  int32_t code = vnodeWriteToMgmtQueue(pMsg);
  if (code != TSDB_CODE_SUCCESS) {
    SRpcMsg rsp = {.handle = pMsg->handle, .code = code};
    rpcSendResponse(&rsp);
  }

  rpcFreeCont(pMsg->pCont);
}

int32_t vnodeInitMgmt() {
  vnodeInitMgmtReqFp();

  SWorkerPool *pPool = &tsVmgmt.pool;
  pPool->name = "vmgmt";
  pPool->startFp = (ProcessStartFp)vnodeProcessMgmtStart;
  pPool->endFp = (ProcessEndFp)vnodeSendMgmtEnd;
  pPool->min = 1;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  tsVmgmt.pQueue = tWorkerAllocQueue(pPool, NULL);

  vInfo("vmgmt is initialized, max worker %d", pPool->max);
  return TSDB_CODE_SUCCESS;
}

void vnodeCleanupMgmt() {
  tWorkerFreeQueue(&tsVmgmt.pool, tsVmgmt.pQueue);
  tWorkerCleanup(&tsVmgmt.pool);
  tsVmgmt.pQueue = NULL;
  vInfo("vmgmt is closed");
}
