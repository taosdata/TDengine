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
#include "tprocess.h"
#include "taoserror.h"
#include "tlog.h"

#define SHM_DEFAULT_SIZE (20 * 1024 * 1024)
#define CEIL4(n)         (ceil((float)(n) / 4) * 4)

typedef struct SProcQueue {
  int32_t         head;
  int32_t         tail;
  int32_t         total;
  int32_t         avail;
  int32_t         items;
  char           *pBuffer;
  tsem_t          sem;
  pthread_mutex_t mutex;
} SProcQueue;

static SProcQueue *taosProcQueueInit(int32_t size) {
  int32_t bufSize = CEIL4(size);
  int32_t headSize = CEIL4(sizeof(SProcQueue));

  SProcQueue *pQueue = malloc(bufSize + headSize);
  if (pQueue == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pQueue->total = bufSize;
  pQueue->avail = bufSize;
  pQueue->head = 0;
  pQueue->tail = 0;
  pQueue->items = 0;
  pQueue->pBuffer = (char *)pQueue + headSize;

  if (pthread_mutex_init(&pQueue->mutex, NULL) != 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  tsem_init(&pQueue->sem, 0, 0);
  return pQueue;
}

static void taosProcQueueCleanup(SProcQueue *pQueue) {
  pthread_mutex_destroy(&pQueue->mutex);
  tsem_destroy(&pQueue->sem);
  free(pQueue);
}

static int32_t taosProcQueuePush(SProcQueue *pQueue, void *pItem, int32_t itemLen) {
  char   *pHead = NULL;
  char   *pBody1 = NULL;
  char   *pBody2 = NULL;
  int32_t body1Len = 0;
  int32_t body2Len = 0;
  int32_t fullLen = CEIL4(itemLen) + 4;

  pthread_mutex_lock(&pQueue->mutex);
  if (fullLen > pQueue->avail) {
    pthread_mutex_unlock(&pQueue->mutex);
    terrno = TSDB_CODE_OUT_OF_SHM_MEM;
    return -1;
  }

  if (pQueue->tail < pQueue->head) {
    pHead = pQueue->pBuffer + pQueue->tail;
    pBody1 = pQueue->pBuffer + pQueue->tail + 4;
    body1Len = itemLen;
    pQueue->tail += fullLen;
  } else {
    int32_t remain = pQueue->total - pQueue->tail;
    if (remain >= fullLen) {
      pHead = pQueue->pBuffer + pQueue->tail;
      pBody1 = pQueue->pBuffer + pQueue->tail + 4;
      body1Len = itemLen;
      pQueue->tail += fullLen;
    } else {
      if (remain == 0) {
        pHead = pQueue->pBuffer;
        pBody1 = pQueue->pBuffer + 4;
        body1Len = itemLen;
        pQueue->tail = fullLen;
      } else if (remain == 4) {
        pHead = pQueue->pBuffer + pQueue->tail;
        pBody1 = pQueue->pBuffer;
        body1Len = itemLen;
        pQueue->tail = fullLen - 4;
      } else {
        pHead = pQueue->pBuffer + pQueue->tail;
        pBody1 = pQueue->pBuffer + pQueue->tail + 4;
        body1Len = remain - 4;
        pBody2 = pQueue->pBuffer;
        body2Len = itemLen - body1Len;
        pQueue->tail = fullLen - body1Len;
      }
    }
  }

  *(int32_t *)(pHead) = fullLen;
  memcpy(pBody1, pItem, body1Len);
  if (pBody2 && body2Len != 0) {
    memcpy(pBody1, pItem + body1Len, body2Len);
  }

  pQueue->avail -= fullLen;
  pQueue->items++;

  pthread_mutex_unlock(&pQueue->mutex);
  tsem_post(&pQueue->sem);
  return 0;
}

static int32_t taosProcQueuePop(SProcQueue *pQueue, SBlockItem **ppItem) {
  tsem_wait(&pQueue->sem);

  pthread_mutex_lock(&pQueue->mutex);
  if (pQueue->total - pQueue->avail <= 0) {
    pthread_mutex_unlock(&pQueue->mutex);
    tsem_post(&pQueue->sem);
    terrno = TSDB_CODE_OUT_OF_SHM_MEM;
    return -1;
  }

  SBlockItem *pBlock = (SBlockItem *)(pQueue->pBuffer + pQueue->head);

  SBlockItem *pItem = malloc(pBlock->contLen);
  if (pItem == NULL) {
    pthread_mutex_unlock(&pQueue->mutex);
    tsem_post(&pQueue->sem);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (pQueue->head < pQueue->tail) {
    memcpy(pItem, pQueue->pBuffer + pQueue->head, pBlock->contLen);
    pQueue->head += pBlock->contLen;
  } else {
    int32_t remain = pQueue->total - pQueue->head;
    if (remain >= pBlock->contLen) {
      memcpy(pItem, pQueue->pBuffer + pQueue->head, pBlock->contLen);
      pQueue->head += pBlock->contLen;
    } else {
      memcpy(pItem, pQueue->pBuffer + pQueue->head, remain);
      memcpy(pItem + remain, pQueue->pBuffer, pBlock->contLen - remain);
      pQueue->head = pBlock->contLen - remain;
    }
  }

  pQueue->avail += pBlock->contLen;
  pQueue->items--;

  pItem->contLen = pBlock->contLen - 4;
  *ppItem = pItem;
  pthread_mutex_unlock(&pQueue->mutex);

  return 0;
}

SProcObj *taosProcInit(const SProcCfg *pCfg) {
  SProcObj *pProc = calloc(1, sizeof(SProcObj));
  if (pProc == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pProc->cfg = *pCfg;

  if (pProc->cfg.childQueueSize <= 0) {
    pProc->cfg.childQueueSize = SHM_DEFAULT_SIZE;
  }

  if (pProc->cfg.parentQueueSize <= 0) {
    pProc->cfg.parentQueueSize = SHM_DEFAULT_SIZE;
  }

  pProc->pChildQueue = taosProcQueueInit(pProc->cfg.childQueueSize);
  pProc->pParentQueue = taosProcQueueInit(pProc->cfg.parentQueueSize);

  return pProc;
}

static bool taosProcIsChild(SProcObj *pProc) { return pProc->pid == 0; }

static void taosProcThreadLoop(SProcQueue *pQueue, ProcFp procFp, void *pParent) {
  SBlockItem *pItem = NULL;

  while (1) {
    int32_t code = taosProcQueuePop(pQueue, &pItem);
    if (code < 0) {
      uDebug("queue:%p, got no message and exiting", pQueue);
      break;
    } else if (code < 0) {
      uTrace("queue:%p, got no message since %s", pQueue, terrstr());
      taosMsleep(1);
      continue;
    } else {
      (*procFp)(pParent, pItem);
    }
  }
}

static void *taosProcThreadChildLoop(void *param) {
  SProcObj *pProc = param;
  taosProcThreadLoop(pProc->pChildQueue, pProc->cfg.childFp, pProc->pParent);
  return NULL;
}

static void *taosProcThreadParentLoop(void *param) {
  SProcObj *pProc = param;
  taosProcThreadLoop(pProc->pParentQueue, pProc->cfg.parentFp, pProc->pParent);
  return NULL;
}

int32_t taosProcStart(SProcObj *pProc) {
  pthread_attr_t thAttr = {0};
  pthread_attr_init(&thAttr);
  pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

  bool isChild = taosProcIsChild(pProc);
  if (isChild || !pProc->testFlag) {
    if (pthread_create(&pProc->childThread, &thAttr, taosProcThreadChildLoop, pProc) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      uError("failed to create thread since %s", terrstr());
      return -1;
    }
  }

  if (!isChild || !pProc->testFlag) {
    if (pthread_create(&pProc->parentThread, &thAttr, taosProcThreadParentLoop, pProc) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      uError("failed to create thread since %s", terrstr());
      return -1;
    }
  }

  return 0;
}

void taosProcStop(SProcObj *pProc) {
  pProc->stopFlag = true;
  // todo join
}

void taosProcCleanup(SProcObj *pProc) {}

int32_t taosProcPushChild(SProcObj *pProc, void *pCont, int32_t contLen) {
  SProcQueue *pQueue = pProc->pChildQueue;
  taosProcQueuePush(pQueue, pCont, contLen);
}
