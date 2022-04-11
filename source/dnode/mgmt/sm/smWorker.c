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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "smInt.h"

static inline void smSendRsp(SNodeMsg *pMsg, int32_t code) {
  SRpcMsg rsp = {.handle = pMsg->rpcMsg.handle,
                 .ahandle = pMsg->rpcMsg.ahandle,
                 .code = code,
                 .pCont = pMsg->pRsp,
                 .contLen = pMsg->rspLen};
  tmsgSendRsp(&rsp);
}

static void smProcessMonitorQueue(SQueueInfo *pInfo, SNodeMsg *pMsg) {
  SSnodeMgmt *pMgmt = pInfo->ahandle;

  dTrace("msg:%p, get from snode-monitor queue", pMsg);
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  int32_t  code = -1;

  if (pMsg->rpcMsg.msgType == TDMT_MON_SM_INFO) {
    code = smProcessGetMonSmInfoReq(pMgmt->pWrapper, pMsg);
  } else {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
  }

  if (pRpc->msgType & 1U) {
    if (code != 0 && terrno != 0) code = terrno;
    smSendRsp(pMsg, code);
  }

  dTrace("msg:%p, is freed, result:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
  rpcFreeCont(pRpc->pCont);
  taosFreeQitem(pMsg);
}

static void smProcessUniqueQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SSnodeMgmt *pMgmt = pInfo->ahandle;

  for (int32_t i = 0; i < numOfMsgs; i++) {
    SNodeMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);

    dTrace("msg:%p, get from snode-unique queue", pMsg);
    sndProcessUMsg(pMgmt->pSnode, &pMsg->rpcMsg);

    dTrace("msg:%p, is freed", pMsg);
    rpcFreeCont(pMsg->rpcMsg.pCont);
    taosFreeQitem(pMsg);
  }
}

static void smProcessSharedQueue(SQueueInfo *pInfo, SNodeMsg *pMsg) {
  SSnodeMgmt *pMgmt = pInfo->ahandle;

  dTrace("msg:%p, get from snode-shared queue", pMsg);
  sndProcessSMsg(pMgmt->pSnode, &pMsg->rpcMsg);

  dTrace("msg:%p, is freed", pMsg);
  rpcFreeCont(pMsg->rpcMsg.pCont);
  taosFreeQitem(pMsg);
}

int32_t smStartWorker(SSnodeMgmt *pMgmt) {
  pMgmt->uniqueWorkers = taosArrayInit(0, sizeof(SMultiWorker *));
  if (pMgmt->uniqueWorkers == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < tsNumOfSnodeUniqueThreads; i++) {
    SMultiWorker *pUniqueWorker = taosMemoryMalloc(sizeof(SMultiWorker));
    if (pUniqueWorker == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    SMultiWorkerCfg cfg = {.max = 1, .name = "snode-unique", .fp = smProcessUniqueQueue, .param = pMgmt};
    if (tMultiWorkerInit(pUniqueWorker, &cfg) != 0) {
      dError("failed to start snode-unique worker since %s", terrstr());
      return -1;
    }
    if (taosArrayPush(pMgmt->uniqueWorkers, &pUniqueWorker) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  SSingleWorkerCfg cfg = {.min = tsNumOfSnodeSharedThreads,
                          .max = tsNumOfSnodeSharedThreads,
                          .name = "snode-shared",
                          .fp = (FItem)smProcessSharedQueue,
                          .param = pMgmt};

  if (tSingleWorkerInit(&pMgmt->sharedWorker, &cfg)) {
    dError("failed to start snode shared-worker since %s", terrstr());
    return -1;
  }

  if (tsMultiProcess) {
    SSingleWorkerCfg mCfg = {
        .min = 1, .max = 1, .name = "snode-monitor", .fp = (FItem)smProcessMonitorQueue, .param = pMgmt};
    if (tSingleWorkerInit(&pMgmt->monitorWorker, &mCfg) != 0) {
      dError("failed to start snode-monitor worker since %s", terrstr());
      return -1;
    }
  }

  dDebug("snode workers are initialized");
  return 0;
}

void smStopWorker(SSnodeMgmt *pMgmt) {
  tSingleWorkerCleanup(&pMgmt->monitorWorker);
  for (int32_t i = 0; i < taosArrayGetSize(pMgmt->uniqueWorkers); i++) {
    SMultiWorker *pWorker = taosArrayGetP(pMgmt->uniqueWorkers, i);
    tMultiWorkerCleanup(pWorker);
  }
  taosArrayDestroy(pMgmt->uniqueWorkers);
  tSingleWorkerCleanup(&pMgmt->sharedWorker);
  dDebug("snode workers are closed");
}

static FORCE_INLINE int32_t smGetSWIdFromMsg(SRpcMsg *pMsg) {
  SMsgHead *pHead = pMsg->pCont;
  pHead->vgId = htonl(pHead->vgId);
  return pHead->vgId % tsNumOfSnodeUniqueThreads;
}

static FORCE_INLINE int32_t smGetSWTypeFromMsg(SRpcMsg *pMsg) {
  /*SMsgHead *pHead = pMsg->pCont;*/
  /*pHead->workerType = htonl(pHead->workerType);*/
  /*return pHead->workerType;*/
  return 0;
}

int32_t smProcessMgmtMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SSnodeMgmt   *pMgmt = pWrapper->pMgmt;
  SMultiWorker *pWorker = taosArrayGetP(pMgmt->uniqueWorkers, 0);
  if (pWorker == NULL) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dTrace("msg:%p, put into worker:%s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t smProcessMonitorMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SSnodeMgmt    *pMgmt = pWrapper->pMgmt;
  SSingleWorker *pWorker = &pMgmt->monitorWorker;

  dTrace("msg:%p, put into worker:%s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t smProcessUniqueMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SSnodeMgmt   *pMgmt = pWrapper->pMgmt;
  int32_t       index = smGetSWIdFromMsg(&pMsg->rpcMsg);
  SMultiWorker *pWorker = taosArrayGetP(pMgmt->uniqueWorkers, index);
  if (pWorker == NULL) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dTrace("msg:%p, put into worker:%s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t smProcessSharedMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  SSnodeMgmt    *pMgmt = pWrapper->pMgmt;
  SSingleWorker *pWorker = &pMgmt->sharedWorker;

  dTrace("msg:%p, put into worker:%s", pMsg, pWorker->name);
  taosWriteQitem(pWorker->queue, pMsg);
  return 0;
}

int32_t smProcessExecMsg(SMgmtWrapper *pWrapper, SNodeMsg *pMsg) {
  int32_t workerType = smGetSWTypeFromMsg(&pMsg->rpcMsg);
  if (workerType == SND_WORKER_TYPE__SHARED) {
    return smProcessSharedMsg(pWrapper, pMsg);
  } else {
    return smProcessUniqueMsg(pWrapper, pMsg);
  }
}
