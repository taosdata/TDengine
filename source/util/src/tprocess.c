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
#include "tqueue.h"

#define SHM_DEFAULT_SIZE (20 * 1024 * 1024)
#define CEIL8(n)         (ceil((float)(n) / 8) * 8)
typedef void *(*ProcThreadFp)(void *param);

typedef struct SProcQueue {
  int32_t         head;
  int32_t         tail;
  int32_t         total;
  int32_t         avail;
  int32_t         items;
  char           *pBuffer;
  ProcMallocFp    mallocHeadFp;
  ProcFreeFp      freeHeadFp;
  ProcMallocFp    mallocBodyFp;
  ProcFreeFp      freeBodyFp;
  ProcConsumeFp   consumeFp;
  void           *pParent;
  tsem_t          sem;
  pthread_mutex_t mutex;
} SProcQueue;

typedef struct SProcObj {
  pthread_t   childThread;
  SProcQueue *pChildQueue;
  pthread_t   parentThread;
  SProcQueue *pParentQueue;
  int32_t     pid;
  bool        isChild;
  bool        stopFlag;
  bool        testFlag;
} SProcObj;

static SProcQueue *taosProcQueueInit(int32_t size) {
  if (size <= 0) size = SHM_DEFAULT_SIZE;

  int32_t bufSize = CEIL8(size);
  int32_t headSize = CEIL8(sizeof(SProcQueue));

  SProcQueue *pQueue = malloc(bufSize + headSize);
  if (pQueue == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (pthread_mutex_init(&pQueue->mutex, NULL) != 0) {
    free(pQueue);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (tsem_init(&pQueue->sem, 0, 0) != 0) {
    pthread_mutex_destroy(&pQueue->mutex);
    free(pQueue);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pQueue->head = 0;
  pQueue->tail = 0;
  pQueue->total = bufSize;
  pQueue->avail = bufSize;
  pQueue->items = 0;
  pQueue->pBuffer = (char *)pQueue + headSize;
  return pQueue;
}

static void taosProcQueueCleanup(SProcQueue *pQueue) {
  if (pQueue != NULL) {
    pthread_mutex_destroy(&pQueue->mutex);
    tsem_destroy(&pQueue->sem);
    free(pQueue);
  }
}

static int32_t taosProcQueuePush(SProcQueue *pQueue, char *pHead, int32_t rawHeadLen, char *pBody, int32_t rawBodyLen) {
  const int32_t headLen = CEIL8(rawHeadLen);
  const int32_t bodyLen = CEIL8(rawBodyLen);
  const int32_t fullLen = headLen + bodyLen + 8;

  pthread_mutex_lock(&pQueue->mutex);
  if (fullLen > pQueue->avail) {
    pthread_mutex_unlock(&pQueue->mutex);
    terrno = TSDB_CODE_OUT_OF_SHM_MEM;
    return -1;
  }

  if (pQueue->tail < pQueue->total) {
    *(int32_t *)(pQueue->pBuffer + pQueue->head) = headLen;
    *(int32_t *)(pQueue->pBuffer + pQueue->head + 4) = bodyLen;
  } else {
    *(int32_t *)(pQueue->pBuffer) = headLen;
    *(int32_t *)(pQueue->pBuffer + 4) = bodyLen;
  }

  if (pQueue->tail < pQueue->head) {
    memcpy(pQueue->pBuffer + pQueue->tail + 8, pHead, rawHeadLen);
    memcpy(pQueue->pBuffer + pQueue->tail + 8 + headLen, pBody, rawBodyLen);
    pQueue->tail = pQueue->tail + 8 + headLen + bodyLen;
  } else {
    int32_t remain = pQueue->total - pQueue->tail;
    if (remain == 0) {
      memcpy(pQueue->pBuffer + 8, pHead, rawHeadLen);
      memcpy(pQueue->pBuffer + 8 + headLen, pBody, rawBodyLen);
      pQueue->tail = 8 + headLen + bodyLen;
    } else if (remain == 8) {
      memcpy(pQueue->pBuffer, pHead, rawHeadLen);
      memcpy(pQueue->pBuffer + headLen, pBody, rawBodyLen);
      pQueue->tail = headLen + bodyLen;
    } else if (remain < 8 + headLen) {
      memcpy(pQueue->pBuffer + pQueue->head + 8, pHead, remain - 8);
      memcpy(pQueue->pBuffer, pHead + remain - 8, rawHeadLen - (remain - 8));
      memcpy(pQueue->pBuffer + headLen - (remain - 8), pBody, rawBodyLen);
      pQueue->tail = headLen - (remain - 8) + bodyLen;
    } else if (remain < 8 + bodyLen) {
      memcpy(pQueue->pBuffer + pQueue->head + 8, pHead, rawHeadLen);
      memcpy(pQueue->pBuffer + pQueue->head + 8 + headLen, pBody, remain - 8 - headLen);
      memcpy(pQueue->pBuffer, pBody + remain - 8 - headLen, rawBodyLen - (remain - 8 - headLen));
      pQueue->tail = bodyLen - (remain - 8 - headLen);
    } else {
      memcpy(pQueue->pBuffer + pQueue->head + 8, pHead, rawHeadLen);
      memcpy(pQueue->pBuffer + pQueue->head + headLen + 8, pBody, rawBodyLen);
      pQueue->tail = pQueue->head + headLen + bodyLen + 8;
    }
  }

  pQueue->avail -= fullLen;
  pQueue->items++;
  pthread_mutex_unlock(&pQueue->mutex);
  tsem_post(&pQueue->sem);

  // (*pQueue->freeHeadFp)(pHead);
  // (*pQueue->freeBodyFp)(pBody);
  return 0;
}

static int32_t taosProcQueuePop(SProcQueue *pQueue, void **ppHead, int32_t *pHeadLen, void **ppBody,
                                int32_t *pBodyLen) {
  tsem_wait(&pQueue->sem);

  pthread_mutex_lock(&pQueue->mutex);
  if (pQueue->total - pQueue->avail <= 0) {
    pthread_mutex_unlock(&pQueue->mutex);
    tsem_post(&pQueue->sem);
    terrno = TSDB_CODE_OUT_OF_SHM_MEM;
    return -1;
  }

  int32_t headLen = 0;
  int32_t bodyLen = 0;
  if (pQueue->head < pQueue->total) {
    headLen = *(int32_t *)(pQueue->pBuffer + pQueue->head);
    bodyLen = *(int32_t *)(pQueue->pBuffer + pQueue->head + 4);
  } else {
    headLen = *(int32_t *)(pQueue->pBuffer);
    bodyLen = *(int32_t *)(pQueue->pBuffer + 4);
  }

  void *pHead = (*pQueue->mallocHeadFp)(headLen);
  void *pBody = (*pQueue->mallocBodyFp)(bodyLen);
  if (pHead == NULL || pBody == NULL) {
    pthread_mutex_unlock(&pQueue->mutex);
    tsem_post(&pQueue->sem);
    (*pQueue->freeHeadFp)(pHead);
    (*pQueue->freeBodyFp)(pBody);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (pQueue->head < pQueue->tail) {
    memcpy(pHead, pQueue->pBuffer + pQueue->head + 8, headLen);
    memcpy(pBody, pQueue->pBuffer + pQueue->head + 8 + headLen, bodyLen);
    pQueue->head = pQueue->head + 8 + headLen + bodyLen;
  } else {
    int32_t remain = pQueue->total - pQueue->head;
    if (remain == 0) {
      memcpy(pHead, pQueue->pBuffer + 8, headLen);
      memcpy(pBody, pQueue->pBuffer + 8 + headLen, bodyLen);
      pQueue->head = 8 + headLen + bodyLen;
    } else if (remain == 8) {
      memcpy(pHead, pQueue->pBuffer, headLen);
      memcpy(pBody, pQueue->pBuffer + headLen, bodyLen);
      pQueue->head = headLen + bodyLen;
    } else if (remain < 8 + headLen) {
      memcpy(pHead, pQueue->pBuffer + pQueue->head + 8, remain - 8);
      memcpy(pHead + remain - 8, pQueue->pBuffer, headLen - (remain - 8));
      memcpy(pBody, pQueue->pBuffer + headLen - (remain - 8), bodyLen);
      pQueue->head = headLen - (remain - 8) + bodyLen;
    } else if (remain < 8 + bodyLen) {
      memcpy(pHead, pQueue->pBuffer + pQueue->head + 8, headLen);
      memcpy(pBody, pQueue->pBuffer + pQueue->head + 8 + headLen, remain - 8 - headLen);
      memcpy(pBody + remain - 8 - headLen, pQueue->pBuffer, bodyLen - (remain - 8 - headLen));
      pQueue->head = bodyLen - (remain - 8 - headLen);
    } else {
      memcpy(pHead, pQueue->pBuffer + pQueue->head + 8, headLen);
      memcpy(pBody, pQueue->pBuffer + pQueue->head + headLen + 8, bodyLen);
      pQueue->head = pQueue->head + headLen + bodyLen + 8;
    }
  }

  pQueue->avail = pQueue->avail + headLen + bodyLen + 8;
  pQueue->items--;
  pthread_mutex_unlock(&pQueue->mutex);

  *ppHead = pHead;
  *ppBody = pBody;
  *pHeadLen = headLen;
  *pBodyLen = bodyLen;
  return 0;
}

SProcObj *taosProcInit(const SProcCfg *pCfg) {
  SProcObj *pProc = calloc(1, sizeof(SProcObj));
  if (pProc == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pProc->pChildQueue = taosProcQueueInit(pCfg->childQueueSize);
  pProc->pParentQueue = taosProcQueueInit(pCfg->parentQueueSize);
  if (pProc->pChildQueue == NULL || pProc->pParentQueue == NULL) {
    taosProcQueueCleanup(pProc->pChildQueue);
    taosProcQueueCleanup(pProc->pParentQueue);
    free(pProc);
    return NULL;
  }

  pProc->testFlag = pCfg->testFlag;
  pProc->pChildQueue->pParent = pCfg->pParent;
  pProc->pChildQueue->mallocHeadFp = pCfg->childMallocHeadFp;
  pProc->pChildQueue->freeHeadFp = pCfg->childFreeHeadFp;
  pProc->pChildQueue->mallocBodyFp = pCfg->childMallocBodyFp;
  pProc->pChildQueue->freeBodyFp = pCfg->childFreeBodyFp;
  pProc->pChildQueue->consumeFp = pCfg->childConsumeFp;
  pProc->pParentQueue->pParent = pCfg->pParent;
  pProc->pParentQueue->mallocHeadFp = pCfg->parentdMallocHeadFp;
  pProc->pParentQueue->freeHeadFp = pCfg->parentFreeHeadFp;
  pProc->pParentQueue->mallocBodyFp = pCfg->parentMallocBodyFp;
  pProc->pParentQueue->freeBodyFp = pCfg->parentFreeBodyFp;
  pProc->pParentQueue->consumeFp = pCfg->parentConsumeFp;

  // todo
  pProc->isChild = 0;

  return pProc;
}

static void taosProcThreadLoop(SProcQueue *pQueue) {
  ProcConsumeFp consumeFp = pQueue->consumeFp;
  void         *pParent = pQueue->pParent;
  void         *pHead, *pBody;
  int32_t       headLen, bodyLen;

  while (1) {
    int32_t code = taosProcQueuePop(pQueue, &pHead, &headLen, &pBody, &bodyLen);
    if (code < 0) {
      uDebug("queue:%p, got no message and exiting", pQueue);
      break;
    } else if (code < 0) {
      uTrace("queue:%p, got no message since %s", pQueue, terrstr());
      taosMsleep(1);
      continue;
    } else {
      (*consumeFp)(pParent, pHead, headLen, pBody, bodyLen);
    }
  }
}

int32_t taosProcStart(SProcObj *pProc) {
  pthread_attr_t thAttr = {0};
  pthread_attr_init(&thAttr);
  pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (pProc->isChild || pProc->testFlag) {
    if (pthread_create(&pProc->childThread, &thAttr, (ProcThreadFp)taosProcThreadLoop, pProc->pChildQueue) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      uError("failed to create thread since %s", terrstr());
      return -1;
    }
  }

  if (!pProc->isChild || pProc->testFlag) {
    if (pthread_create(&pProc->parentThread, &thAttr, (ProcThreadFp)taosProcThreadLoop, pProc->pParentQueue) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      uError("failed to create thread since %s", terrstr());
      return -1;
    }
  }

  return 0;
}

void taosProcStop(SProcObj *pProc) {
  pProc->stopFlag = true;
  // todo
  // join
}

void taosProcCleanup(SProcObj *pProc) {
  if (pProc != NULL) {
    taosProcQueueCleanup(pProc->pChildQueue);
    taosProcQueueCleanup(pProc->pParentQueue);
    free(pProc);
  }
}

int32_t taosProcPutToChildQueue(SProcObj *pProc, void *pHead, int32_t headLen, void *pBody, int32_t bodyLen) {
  return taosProcQueuePush(pProc->pChildQueue, pHead, headLen, pBody, bodyLen);
}

int32_t taosProcPutToParentQueue(SProcObj *pProc, void *pHead, int32_t headLen, void *pBody, int32_t bodyLen) {
  return taosProcQueuePush(pProc->pParentQueue, pHead, headLen, pBody, bodyLen);
}
