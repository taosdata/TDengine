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

static void smProcessUniqueQueue(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs) {
  SSnodeMgmt *pMgmt = pInfo->ahandle;

  for (int32_t i = 0; i < numOfMsgs; i++) {
    SNodeMsg *pMsg = NULL;
    taosGetQitem(qall, (void **)&pMsg);

    dTrace("msg:%p, will be processed in snode unique queue", pMsg);
    sndProcessUMsg(pMgmt->pSnode, &pMsg->rpcMsg);

    dTrace("msg:%p, is freed", pMsg);
    rpcFreeCont(pMsg->rpcMsg.pCont);
    taosFreeQitem(pMsg);
  }
}

static void smProcessSharedQueue(SQueueInfo *pInfo, SNodeMsg *pMsg) {
  SSnodeMgmt *pMgmt = pInfo->ahandle;

  dTrace("msg:%p, will be processed in snode shared queue", pMsg);
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

  for (int32_t i = 0; i < SND_UNIQUE_THREAD_NUM; i++) {
    SMultiWorker *pUniqueWorker = malloc(sizeof(SMultiWorker));
    if (pUniqueWorker == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    SMultiWorkerCfg cfg = {.maxNum = 1, .name = "snode-unique", .fp = smProcessUniqueQueue, .param = pMgmt};

    if (tMultiWorkerInit(pUniqueWorker, &cfg) != 0) {
      dError("failed to start snode-unique worker since %s", terrstr());
      return -1;
    }
    if (taosArrayPush(pMgmt->uniqueWorkers, &pUniqueWorker) == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  SSingleWorkerCfg cfg = {.minNum = SND_SHARED_THREAD_NUM,
                          .maxNum = SND_SHARED_THREAD_NUM,
                          .name = "snode-shared",
                          .fp = (FItem)smProcessSharedQueue,
                          .param = pMgmt};

  if (tSingleWorkerInit(&pMgmt->sharedWorker, &cfg)) {
    dError("failed to start snode shared-worker since %s", terrstr());
    return -1;
  }

  return 0;
}

void smStopWorker(SSnodeMgmt *pMgmt) {
  for (int32_t i = 0; i < taosArrayGetSize(pMgmt->uniqueWorkers); i++) {
    SMultiWorker *pWorker = taosArrayGetP(pMgmt->uniqueWorkers, i);
    tMultiWorkerCleanup(pWorker);
  }
  taosArrayDestroy(pMgmt->uniqueWorkers);
  tSingleWorkerCleanup(&pMgmt->sharedWorker);
}

static FORCE_INLINE int32_t smGetSWIdFromMsg(SRpcMsg *pMsg) {
  SMsgHead *pHead = pMsg->pCont;
  pHead->streamTaskId = htonl(pHead->streamTaskId);
  return pHead->streamTaskId % SND_UNIQUE_THREAD_NUM;
}

static FORCE_INLINE int32_t smGetSWTypeFromMsg(SRpcMsg *pMsg) {
  SStreamExecMsgHead *pHead = pMsg->pCont;
  pHead->workerType = htonl(pHead->workerType);
  return pHead->workerType;
}

int32_t smProcessMgmtMsg(SSnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  SMultiWorker *pWorker = taosArrayGetP(pMgmt->uniqueWorkers, 0);
  if (pWorker == NULL) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dTrace("msg:%p, put into worker:%s", pMsg, pWorker->name);
  return taosWriteQitem(pWorker->queue, pMsg);
}

int32_t smProcessUniqueMsg(SSnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  int32_t       index = smGetSWIdFromMsg(&pMsg->rpcMsg);
  SMultiWorker *pWorker = taosArrayGetP(pMgmt->uniqueWorkers, index);
  if (pWorker == NULL) {
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  dTrace("msg:%p, put into worker:%s", pMsg, pWorker->name);
  return taosWriteQitem(pWorker->queue, pMsg);
}

int32_t smProcessSharedMsg(SSnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  SSingleWorker *pWorker = &pMgmt->sharedWorker;

  dTrace("msg:%p, put into worker:%s", pMsg, pWorker->name);
  return taosWriteQitem(pWorker->queue, pMsg);
}

int32_t smProcessExecMsg(SSnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  int32_t workerType = smGetSWTypeFromMsg(&pMsg->rpcMsg);
  if (workerType == SND_WORKER_TYPE__SHARED) {
    return smProcessSharedMsg(pMgmt, pMsg);
  } else {
    return smProcessUniqueMsg(pMgmt, pMsg);
  }
}
