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

int32_t dndInitWorker(SDnode *pDnode, SDnodeWorker *pWorker, EDndWorkerType type, const char *name, int32_t minNum,
                      int32_t maxNum, FProcessItem fp) {
  if (pDnode == NULL || pWorker == NULL || name == NULL || minNum < 0 || maxNum <= 0 || fp == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

  pWorker->type = type;
  pWorker->name = name;
  pWorker->minNum = minNum;
  pWorker->maxNum = maxNum;
  pWorker->fp = fp;
  pWorker->pDnode = pDnode;

  if (pWorker->type == DND_WORKER_SINGLE) {
    SWorkerPool *pPool = &pWorker->pool;
    pPool->min = minNum;
    pPool->max = maxNum;
    if (tWorkerInit(pPool) != 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    pWorker->queue = tWorkerAllocQueue(&pPool, pDnode, fp);
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
  if (pWorker->type == DND_WORKER_SINGLE) {
    while (!taosQueueEmpty(pWorker->queue)) {
      taosMsleep(10);
    }
    tWorkerCleanup(&pWorker->pool);
    tWorkerFreeQueue(&pWorker->pool, pWorker->queue);
  }
}

int32_t dndWriteMsgToWorker(SDnodeWorker *pWorker, void *pCont, int32_t contLen) {
  if (pWorker == NULL || pWorker->queue == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return -1;
  }

  void *pMsg = taosAllocateQitem(contLen);
  if (pMsg == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  memcpy(pMsg, pCont, contLen);

  if (taosWriteQitem(pWorker, pMsg) != 0) {
    taosFreeItem(pMsg);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}