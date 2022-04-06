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
#include "taos.h"
#include "taoserror.h"
#include "thash.h"
#include "tlog.h"
#include "tqueue.h"

#define SHM_DEFAULT_SIZE (20 * 1024 * 1024)
typedef void *(*ProcThreadFp)(void *param);

typedef struct SProcQueue {
  int32_t       head;
  int32_t       tail;
  int32_t       total;
  int32_t       avail;
  int32_t       items;
  char          name[8];
  TdThreadMutex mutex;
  tsem_t        sem;
  char          pBuffer[];
} SProcQueue;

typedef struct SProcObj {
  TdThread      thread;
  SProcQueue   *pChildQueue;
  SProcQueue   *pParentQueue;
  ProcConsumeFp childConsumeFp;
  ProcMallocFp  childMallocHeadFp;
  ProcFreeFp    childFreeHeadFp;
  ProcMallocFp  childMallocBodyFp;
  ProcFreeFp    childFreeBodyFp;
  ProcConsumeFp parentConsumeFp;
  ProcMallocFp  parentMallocHeadFp;
  ProcFreeFp    parentFreeHeadFp;
  ProcMallocFp  parentMallocBodyFp;
  ProcFreeFp    parentFreeBodyFp;
  void         *parent;
  const char   *name;
  SHashObj     *hash;
  int32_t       pid;
  bool          isChild;
  bool          stopFlag;
} SProcObj;

static inline int32_t CEIL8(int32_t v) {
  const int32_t c = ceil((float)(v) / 8) * 8;
  return c < 8 ? 8 : c;
}

static int32_t taosProcInitMutex(SProcQueue *pQueue) {
  TdThreadMutexAttr mattr = {0};

  if (taosThreadMutexAttrInit(&mattr) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to init mutex while init attr since %s", terrstr());
    return -1;
  }

  if (taosThreadMutexAttrSetPshared(&mattr, PTHREAD_PROCESS_SHARED) != 0) {
    taosThreadMutexAttrDestroy(&mattr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to init mutex while set shared since %s", terrstr());
    return -1;
  }

  if (taosThreadMutexInit(&pQueue->mutex, &mattr) != 0) {
    taosThreadMutexDestroy(&pQueue->mutex);
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to init mutex since %s", terrstr());
    return -1;
  }

  taosThreadMutexAttrDestroy(&mattr);
  return 0;
}

static int32_t taosProcInitSem(SProcQueue *pQueue) {
  if (tsem_init(&pQueue->sem, 1, 0) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to init sem");
    return -1;
  }

  return 0;
}

static SProcQueue *taosProcInitQueue(const char *name, bool isChild, char *ptr, int32_t size) {
  int32_t bufSize = size - CEIL8(sizeof(SProcQueue));
  if (bufSize <= 1024) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  SProcQueue *pQueue = (SProcQueue *)(ptr);

  if (!isChild) {
    if (taosProcInitMutex(pQueue) != 0) {
      return NULL;
    }

    if (taosProcInitSem(pQueue) != 0) {
      return NULL;
    }

    tstrncpy(pQueue->name, name, sizeof(pQueue->name));
    pQueue->head = 0;
    pQueue->tail = 0;
    pQueue->total = bufSize;
    pQueue->avail = bufSize;
    pQueue->items = 0;
  }

  return pQueue;
}

#if 0
static void taosProcDestroyMutex(SProcQueue *pQueue) {
  if (pQueue->mutex != NULL) {
    taosThreadMutexDestroy(pQueue->mutex);
    pQueue->mutex = NULL;
  }
}

static void taosProcDestroySem(SProcQueue *pQueue) {
  if (pQueue->sem != NULL) {
    tsem_destroy(pQueue->sem);
    pQueue->sem = NULL;
  }
}
#endif

static void taosProcCleanupQueue(SProcQueue *pQueue) {
#if 0  
  if (pQueue != NULL) {
    taosProcDestroyMutex(pQueue);
    taosProcDestroySem(pQueue);
  }
#endif
}

static int32_t taosProcQueuePush(SProcObj *pProc, SProcQueue *pQueue, const char *pHead, int16_t rawHeadLen,
                                 const char *pBody, int32_t rawBodyLen, int64_t handle, ProcFuncType ftype) {
  const int32_t headLen = CEIL8(rawHeadLen);
  const int32_t bodyLen = CEIL8(rawBodyLen);
  const int32_t fullLen = headLen + bodyLen + 8;

  taosThreadMutexLock(&pQueue->mutex);
  if (fullLen > pQueue->avail) {
    taosThreadMutexUnlock(&pQueue->mutex);
    terrno = TSDB_CODE_OUT_OF_SHM_MEM;
    return -1;
  }

  if (handle != 0 && ftype == PROC_REQ) {
    if (taosHashPut(pProc->hash, &handle, sizeof(int64_t), &handle, sizeof(int64_t)) != 0) {
      taosThreadMutexUnlock(&pQueue->mutex);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  const int32_t pos = pQueue->tail;
  if (pQueue->tail < pQueue->total) {
    *(int16_t *)(pQueue->pBuffer + pQueue->tail) = headLen;
    *(int8_t *)(pQueue->pBuffer + pQueue->tail + 2) = (int8_t)ftype;
    *(int32_t *)(pQueue->pBuffer + pQueue->tail + 4) = bodyLen;
  } else {
    *(int16_t *)(pQueue->pBuffer) = headLen;
    *(int8_t *)(pQueue->pBuffer + 2) = (int8_t)ftype;
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
      memcpy(pQueue->pBuffer + pQueue->tail + 8, pHead, remain - 8);
      memcpy(pQueue->pBuffer, pHead + remain - 8, rawHeadLen - (remain - 8));
      memcpy(pQueue->pBuffer + headLen - (remain - 8), pBody, rawBodyLen);
      pQueue->tail = headLen - (remain - 8) + bodyLen;
    } else if (remain < 8 + headLen + bodyLen) {
      memcpy(pQueue->pBuffer + pQueue->tail + 8, pHead, rawHeadLen);
      memcpy(pQueue->pBuffer + pQueue->tail + 8 + headLen, pBody, remain - 8 - headLen);
      memcpy(pQueue->pBuffer, pBody + remain - 8 - headLen, rawBodyLen - (remain - 8 - headLen));
      pQueue->tail = bodyLen - (remain - 8 - headLen);
    } else {
      memcpy(pQueue->pBuffer + pQueue->tail + 8, pHead, rawHeadLen);
      memcpy(pQueue->pBuffer + pQueue->tail + headLen + 8, pBody, rawBodyLen);
      pQueue->tail = pQueue->tail + headLen + bodyLen + 8;
    }
  }

  pQueue->avail -= fullLen;
  pQueue->items++;
  taosThreadMutexUnlock(&pQueue->mutex);
  tsem_post(&pQueue->sem);

  uTrace("proc:%s, push msg at pos:%d ftype:%d remain:%d, head:%d %p body:%d %p", pQueue->name, pos, ftype,
         pQueue->items, headLen, pHead, bodyLen, pBody);
  return 0;
}

static int32_t taosProcQueuePop(SProcQueue *pQueue, void **ppHead, int16_t *pHeadLen, void **ppBody, int32_t *pBodyLen,
                                ProcFuncType *pFuncType, ProcMallocFp mallocHeadFp, ProcFreeFp freeHeadFp,
                                ProcMallocFp mallocBodyFp, ProcFreeFp freeBodyFp) {
  tsem_wait(&pQueue->sem);

  taosThreadMutexLock(&pQueue->mutex);
  if (pQueue->total - pQueue->avail <= 0) {
    taosThreadMutexUnlock(&pQueue->mutex);
    terrno = TSDB_CODE_OUT_OF_SHM_MEM;
    return 0;
  }

  int16_t headLen = 0;
  int8_t  ftype = 0;
  int32_t bodyLen = 0;
  if (pQueue->head < pQueue->total) {
    headLen = *(int16_t *)(pQueue->pBuffer + pQueue->head);
    ftype = *(int8_t *)(pQueue->pBuffer + pQueue->head + 2);
    bodyLen = *(int32_t *)(pQueue->pBuffer + pQueue->head + 4);
  } else {
    headLen = *(int16_t *)(pQueue->pBuffer);
    ftype = *(int8_t *)(pQueue->pBuffer + 2);
    bodyLen = *(int32_t *)(pQueue->pBuffer + 4);
  }

  void *pHead = (*mallocHeadFp)(headLen);
  void *pBody = (*mallocBodyFp)(bodyLen);
  if (pHead == NULL || pBody == NULL) {
    taosThreadMutexUnlock(&pQueue->mutex);
    tsem_post(&pQueue->sem);
    (*freeHeadFp)(pHead);
    (*freeBodyFp)(pBody);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  const int32_t pos = pQueue->head;
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
    } else if (remain < 8 + headLen + bodyLen) {
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
  taosThreadMutexUnlock(&pQueue->mutex);

  *ppHead = pHead;
  *ppBody = pBody;
  *pHeadLen = headLen;
  *pBodyLen = bodyLen;
  *pFuncType = (ProcFuncType)ftype;

  uTrace("proc:%s, pop msg at pos:%d ftype:%d remain:%d, head:%d %p body:%d %p", pQueue->name, pos, ftype,
         pQueue->items, headLen, pHead, bodyLen, pBody);
  return 1;
}

SProcObj *taosProcInit(const SProcCfg *pCfg) {
  SProcObj *pProc = taosMemoryCalloc(1, sizeof(SProcObj));
  if (pProc == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  int32_t cstart = 0;
  int32_t csize = CEIL8(pCfg->shm.size / 2);
  int32_t pstart = csize;
  int32_t psize = CEIL8(pCfg->shm.size - pstart);
  if (pstart + psize > pCfg->shm.size) {
    psize -= 8;
  }

  pProc->name = pCfg->name;
  pProc->pChildQueue = taosProcInitQueue(pCfg->name, pCfg->isChild, (char *)pCfg->shm.ptr + cstart, csize);
  pProc->pParentQueue = taosProcInitQueue(pCfg->name, pCfg->isChild, (char *)pCfg->shm.ptr + pstart, psize);
  pProc->hash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pProc->pChildQueue == NULL || pProc->pParentQueue == NULL) {
    taosProcCleanupQueue(pProc->pChildQueue);
    taosMemoryFree(pProc);
    return NULL;
  }

  pProc->name = pCfg->name;
  pProc->parent = pCfg->parent;
  pProc->childMallocHeadFp = pCfg->childMallocHeadFp;
  pProc->childFreeHeadFp = pCfg->childFreeHeadFp;
  pProc->childMallocBodyFp = pCfg->childMallocBodyFp;
  pProc->childFreeBodyFp = pCfg->childFreeBodyFp;
  pProc->childConsumeFp = pCfg->childConsumeFp;
  pProc->parentMallocHeadFp = pCfg->parentMallocHeadFp;
  pProc->parentFreeHeadFp = pCfg->parentFreeHeadFp;
  pProc->parentMallocBodyFp = pCfg->parentMallocBodyFp;
  pProc->parentFreeBodyFp = pCfg->parentFreeBodyFp;
  pProc->parentConsumeFp = pCfg->parentConsumeFp;
  pProc->isChild = pCfg->isChild;

  uDebug("proc:%s, is initialized, isChild:%d child queue:%p parent queue:%p", pProc->name, pProc->isChild,
         pProc->pChildQueue, pProc->pParentQueue);

  return pProc;
}

static void taosProcThreadLoop(SProcObj *pProc) {
  void         *pHead, *pBody;
  int16_t       headLen;
  ProcFuncType  ftype;
  int32_t       bodyLen;
  SProcQueue   *pQueue;
  ProcConsumeFp consumeFp;
  ProcMallocFp  mallocHeadFp;
  ProcFreeFp    freeHeadFp;
  ProcMallocFp  mallocBodyFp;
  ProcFreeFp    freeBodyFp;

  if (pProc->isChild) {
    pQueue = pProc->pChildQueue;
    consumeFp = pProc->childConsumeFp;
    mallocHeadFp = pProc->childMallocHeadFp;
    freeHeadFp = pProc->childFreeHeadFp;
    mallocBodyFp = pProc->childMallocBodyFp;
    freeBodyFp = pProc->childFreeBodyFp;
  } else {
    pQueue = pProc->pParentQueue;
    consumeFp = pProc->parentConsumeFp;
    mallocHeadFp = pProc->parentMallocHeadFp;
    freeHeadFp = pProc->parentFreeHeadFp;
    mallocBodyFp = pProc->parentMallocBodyFp;
    freeBodyFp = pProc->parentFreeBodyFp;
  }

  uDebug("proc:%s, start to get msg from queue:%p", pProc->name, pQueue);

  while (1) {
    int32_t numOfMsgs = taosProcQueuePop(pQueue, &pHead, &headLen, &pBody, &bodyLen, &ftype, mallocHeadFp, freeHeadFp,
                                         mallocBodyFp, freeBodyFp);
    if (numOfMsgs == 0) {
      uDebug("proc:%s, get no msg from queue:%p and exit the proc thread", pProc->name, pQueue);
      break;
    } else if (numOfMsgs < 0) {
      uError("proc:%s, get no msg from queue:%p since %s", pProc->name, pQueue, terrstr());
      taosMsleep(1);
      continue;
    } else {
      (*consumeFp)(pProc->parent, pHead, headLen, pBody, bodyLen, ftype);
    }
  }
}

int32_t taosProcRun(SProcObj *pProc) {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (taosThreadCreate(&pProc->thread, &thAttr, (ProcThreadFp)taosProcThreadLoop, pProc) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to create thread since %s", terrstr());
    return -1;
  }

  uDebug("proc:%s, start to consume queue:%p, thread:%" PRId64, pProc->name, pProc->pChildQueue, pProc->thread);
  return 0;
}

static void taosProcStop(SProcObj *pProc) {
  if (!taosCheckPthreadValid(pProc->thread)) return;

  uDebug("proc:%s, start to join thread:%" PRId64, pProc->name, pProc->thread);
  SProcQueue *pQueue;
  if (pProc->isChild) {
    pQueue = pProc->pChildQueue;
  } else {
    pQueue = pProc->pParentQueue;
  }
  tsem_post(&pQueue->sem);
  taosThreadJoin(pProc->thread, NULL);
}

void taosProcCleanup(SProcObj *pProc) {
  if (pProc != NULL) {
    uDebug("proc:%s, start to clean up", pProc->name);
    taosProcStop(pProc);
    taosProcCleanupQueue(pProc->pChildQueue);
    taosProcCleanupQueue(pProc->pParentQueue);
    if (pProc->hash != NULL) {
      taosHashCleanup(pProc->hash);
      pProc->hash = NULL;
    }

    uDebug("proc:%s, is cleaned up", pProc->name);
    taosMemoryFree(pProc);
  }
}

int32_t taosProcPutToChildQ(SProcObj *pProc, const void *pHead, int16_t headLen, const void *pBody, int32_t bodyLen,
                            void *handle, ProcFuncType ftype) {
  return taosProcQueuePush(pProc, pProc->pChildQueue, pHead, headLen, pBody, bodyLen, (int64_t)handle, ftype);
}

void taosProcRemoveHandle(SProcObj *pProc, void *handle) {
  int64_t h = (int64_t)handle;
  taosThreadMutexLock(&pProc->pChildQueue->mutex);
  taosHashRemove(pProc->hash, &h, sizeof(int64_t));
  taosThreadMutexUnlock(&pProc->pChildQueue->mutex);
}

void taosProcCloseHandles(SProcObj *pProc, void (*HandleFp)(void *handle)) {
  taosThreadMutexLock(&pProc->pChildQueue->mutex);
  void *h = taosHashIterate(pProc->hash, NULL);
  while (h != NULL) {
    void *handle = *((void **)h);
    (*HandleFp)(handle);
  }
  taosThreadMutexUnlock(&pProc->pChildQueue->mutex);
}

void taosProcPutToParentQ(SProcObj *pProc, const void *pHead, int16_t headLen, const void *pBody, int32_t bodyLen,
                          ProcFuncType ftype) {
  int32_t retry = 0;
  while (taosProcQueuePush(pProc, pProc->pParentQueue, pHead, headLen, pBody, bodyLen, 0, ftype) != 0) {
    uInfo("proc:%s, failed to put msg to queue:%p since %s, retry:%d", pProc->name, pProc->pParentQueue, terrstr(), retry);
    retry++;
    taosMsleep(retry);
  }
}
