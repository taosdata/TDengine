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
#include "dndWorker.h"

int32_t dndInitWorker(SDnode *pDnode, SDnodeWorker *pWorker, EWorkerType type, const char *name, int32_t minNum,
                      int32_t maxNum, void *queueFp) {
  if (pDnode == NULL || pWorker == NULL || name == NULL || minNum < 0 || maxNum <= 0 || queueFp == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

  pWorker->type = type;
  pWorker->name = name;
  pWorker->minNum = minNum;
  pWorker->maxNum = maxNum;
  pWorker->queueFp = queueFp;
  pWorker->pDnode = pDnode;

  if (pWorker->type == DND_WORKER_SINGLE) {
    SQWorkerPool *pPool = &pWorker->pool;
    pPool->name = name;
    pPool->min = minNum;
    pPool->max = maxNum;
    if (tQWorkerInit(pPool) != 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    pWorker->queue = tQWorkerAllocQueue(pPool, pDnode, (FItem)queueFp);
    if (pWorker->queue == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  } else if (pWorker->type == DND_WORKER_MULTI) {
    SWWorkerPool *pPool = &pWorker->mpool;
    pPool->name = name;
    pPool->max = maxNum;
    if (tWWorkerInit(pPool) != 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
    pWorker->queue = tWWorkerAllocQueue(pPool, pDnode, (FItems)queueFp);
    if (pWorker->queue == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  } else {
    terrno = TSDB_CODE_INVALID_PARA;
  }

  return 0;
}

void dndCleanupWorker(SDnodeWorker *pWorker) {
  while (!taosQueueEmpty(pWorker->queue)) {
    taosMsleep(10);
  }

  if (pWorker->type == DND_WORKER_SINGLE) {
    tQWorkerCleanup(&pWorker->pool);
    tQWorkerFreeQueue(&pWorker->pool, pWorker->queue);
  } else if (pWorker->type == DND_WORKER_MULTI) {
    tWWorkerCleanup(&pWorker->mpool);
    tWWorkerFreeQueue(&pWorker->mpool, pWorker->queue);
  } else {
  }
}

int32_t dndWriteMsgToWorker(SDnodeWorker *pWorker, void *pCont, int32_t contLen) {
  if (pWorker == NULL || pWorker->queue == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

  void *pMsg = NULL;
  if (contLen != 0) {
    pMsg = taosAllocateQitem(contLen);
    if (pMsg != NULL) {
      memcpy(pMsg, pCont, contLen);
    }
  } else {
    pMsg = pCont;
  }

  if (pMsg == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (taosWriteQitem(pWorker->queue, pMsg) != 0) {
    if (contLen != 0) {
      taosFreeQitem(pMsg);
    }
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}
