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

// todo
#include <sys/shm.h>
#include <sys/wait.h>

#define SHM_DEFAULT_SIZE (20 * 1024 * 1024)
#define CEIL8(n)         (ceil((float)(n) / 8) * 8)
typedef void *(*ProcThreadFp)(void *param);

typedef struct SProcQueue {
  int32_t          head;
  int32_t          tail;
  int32_t          total;
  int32_t          avail;
  int32_t          items;
  char            *pBuffer;
  ProcMallocFp     mallocHeadFp;
  ProcFreeFp       freeHeadFp;
  ProcMallocFp     mallocBodyFp;
  ProcFreeFp       freeBodyFp;
  ProcConsumeFp    consumeFp;
  void            *pParent;
  tsem_t           sem;
  TdThreadMutex   *mutex;
  int32_t          mutexShmid;
  int32_t          bufferShmid;
  const char      *name;
} SProcQueue;

typedef struct SProcObj {
  TdThread    childThread;
  SProcQueue *pChildQueue;
  TdThread    parentThread;
  SProcQueue *pParentQueue;
  const char *name;
  int32_t     pid;
  bool        isChild;
  bool        stopFlag;
  bool        testFlag;
} SProcObj;

static int32_t taosProcInitMutex(TdThreadMutex **ppMutex, int32_t *pShmid) {
  TdThreadMutex    *pMutex = NULL;
  TdThreadMutexAttr mattr = {0};
  int32_t           shmid = -1;
  int32_t           code = -1;

  if (pthread_mutexattr_init(&mattr) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to init mutex while init attr since %s", terrstr());
    goto _OVER;
  }

  if (pthread_mutexattr_setpshared(&mattr, PTHREAD_PROCESS_SHARED) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to init mutex while set shared since %s", terrstr());
    goto _OVER;
  }

  shmid = shmget(IPC_PRIVATE, sizeof(TdThreadMutex), 0600);
  if (shmid <= 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to init mutex while shmget since %s", terrstr());
    goto _OVER;
  }

  pMutex = (TdThreadMutex *)shmat(shmid, NULL, 0);
  if (pMutex == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to init mutex while shmat since %s", terrstr());
    goto _OVER;
  }

  if (taosThreadMutexInit(pMutex, &mattr) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to init mutex since %s", terrstr());
    goto _OVER;
  }

  code = 0;

_OVER:
  if (code != 0) {
    taosThreadMutexDestroy(pMutex);
    shmctl(shmid, IPC_RMID, NULL);
  } else {
    *ppMutex = pMutex;
    *pShmid = shmid;
  }

  pthread_mutexattr_destroy(&mattr);
  return code;
}

static void taosProcDestroyMutex(TdThreadMutex *pMutex, int32_t *pShmid) {
  if (pMutex != NULL) {
    taosThreadMutexDestroy(pMutex);
  }
  if (*pShmid > 0) {
    shmctl(*pShmid, IPC_RMID, NULL);
  }
}

static int32_t taosProcInitBuffer(void **ppBuffer, int32_t size) {
  int32_t shmid = shmget(IPC_PRIVATE, size, IPC_CREAT | 0600);
  if (shmid <= 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to init buffer while shmget since %s", terrstr());
    return -1;
  }

  void *shmptr = shmat(shmid, NULL, 0);
  if (shmptr == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    uError("failed to init buffer while shmat since %s", terrstr());
    shmctl(shmid, IPC_RMID, NULL);
    return -1;
  }

  *ppBuffer = shmptr;
  return shmid;
}

static void taosProcDestroyBuffer(void *pBuffer, int32_t *pShmid) {
  if (*pShmid > 0) {
    shmctl(*pShmid, IPC_RMID, NULL);
  }
}

static SProcQueue *taosProcQueueInit(int32_t size) {
  if (size <= 0) size = SHM_DEFAULT_SIZE;

  int32_t bufSize = CEIL8(size);
  int32_t headSize = CEIL8(sizeof(SProcQueue));

  SProcQueue *pQueue = NULL;
  int32_t     shmId = taosProcInitBuffer((void **)&pQueue, bufSize + headSize);
  if (shmId <= 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pQueue->bufferShmid = shmId;

  if (taosProcInitMutex(&pQueue->mutex, &pQueue->mutexShmid) != 0) {
    taosMemoryFree(pQueue);
    return NULL;
  }

  if (tsem_init(&pQueue->sem, 1, 0) != 0) {
    taosProcDestroyMutex(pQueue->mutex, &pQueue->mutexShmid);
    taosMemoryFree(pQueue);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (taosProcInitMutex(&pQueue->mutex, &pQueue->mutexShmid) != 0) {
    taosProcDestroyMutex(pQueue->mutex, &pQueue->mutexShmid);
    tsem_destroy(&pQueue->sem);
    taosMemoryFree(pQueue);
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
    uDebug("proc:%s, queue:%p clean up", pQueue->name, pQueue);
    taosProcDestroyMutex(pQueue->mutex, &pQueue->mutexShmid);
    tsem_destroy(&pQueue->sem);
    taosMemoryFree(pQueue);
  }
}

static int32_t taosProcQueuePush(SProcQueue *pQueue, char *pHead, int32_t rawHeadLen, char *pBody, int32_t rawBodyLen) {
  const int32_t headLen = CEIL8(rawHeadLen);
  const int32_t bodyLen = CEIL8(rawBodyLen);
  const int32_t fullLen = headLen + bodyLen + 8;

  taosThreadMutexLock(pQueue->mutex);
  if (fullLen > pQueue->avail) {
    taosThreadMutexUnlock(pQueue->mutex);
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
  taosThreadMutexUnlock(pQueue->mutex);
  tsem_post(&pQueue->sem);

  uTrace("proc:%s, push msg:%p:%d cont:%p:%d to queue:%p", pQueue->name, pHead, rawHeadLen, pBody, rawBodyLen, pQueue);
  return 0;
}

static int32_t taosProcQueuePop(SProcQueue *pQueue, void **ppHead, int32_t *pHeadLen, void **ppBody,
                                int32_t *pBodyLen) {
  tsem_wait(&pQueue->sem);

  taosThreadMutexLock(pQueue->mutex);
  if (pQueue->total - pQueue->avail <= 0) {
    taosThreadMutexUnlock(pQueue->mutex);
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
    taosThreadMutexUnlock(pQueue->mutex);
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
  taosThreadMutexUnlock(pQueue->mutex);

  *ppHead = pHead;
  *ppBody = pBody;
  *pHeadLen = headLen;
  *pBodyLen = bodyLen;

  uTrace("proc:%s, get msg:%p:%d cont:%p:%d from queue:%p", pQueue->name, pHead, headLen, pBody, bodyLen, pQueue);
  return 0;
}

SProcObj *taosProcInit(const SProcCfg *pCfg) {
  SProcObj *pProc = taosMemoryCalloc(1, sizeof(SProcObj));
  if (pProc == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pProc->name = pCfg->name;
  pProc->testFlag = pCfg->testFlag;

  pProc->pChildQueue = taosProcQueueInit(pCfg->childQueueSize);
  pProc->pParentQueue = taosProcQueueInit(pCfg->parentQueueSize);
  if (pProc->pChildQueue == NULL || pProc->pParentQueue == NULL) {
    taosProcQueueCleanup(pProc->pChildQueue);
    taosMemoryFree(pProc);
    return NULL;
  }

  pProc->pChildQueue->name = pCfg->name;
  pProc->pChildQueue->pParent = pCfg->pParent;
  pProc->pChildQueue->mallocHeadFp = pCfg->childMallocHeadFp;
  pProc->pChildQueue->freeHeadFp = pCfg->childFreeHeadFp;
  pProc->pChildQueue->mallocBodyFp = pCfg->childMallocBodyFp;
  pProc->pChildQueue->freeBodyFp = pCfg->childFreeBodyFp;
  pProc->pChildQueue->consumeFp = pCfg->childConsumeFp;
  pProc->pParentQueue->name = pCfg->name;
  pProc->pParentQueue->pParent = pCfg->pParent;
  pProc->pParentQueue->mallocHeadFp = pCfg->parentdMallocHeadFp;
  pProc->pParentQueue->freeHeadFp = pCfg->parentFreeHeadFp;
  pProc->pParentQueue->mallocBodyFp = pCfg->parentMallocBodyFp;
  pProc->pParentQueue->freeBodyFp = pCfg->parentFreeBodyFp;
  pProc->pParentQueue->consumeFp = pCfg->parentConsumeFp;

  uDebug("proc:%s, initialized, child queue:%p parent queue:%p", pProc->name, pProc->pChildQueue, pProc->pParentQueue);

  if (!pProc->testFlag) {
    pProc->pid = fork();
    if (pProc->pid == 0) {
      pProc->isChild = 1;
      uInfo("this is child process, pid:%d", pProc->pid);
    } else {
      pProc->isChild = 0;
      uInfo("this is parent process, pid:%d", pProc->pid);
    }
  }

  return pProc;
}

static void taosProcThreadLoop(SProcQueue *pQueue) {
  ProcConsumeFp consumeFp = pQueue->consumeFp;
  void         *pParent = pQueue->pParent;
  void         *pHead, *pBody;
  int32_t       headLen, bodyLen;

  uDebug("proc:%s, start to get message from queue:%p", pQueue->name, pQueue);

  while (1) {
    int32_t code = taosProcQueuePop(pQueue, &pHead, &headLen, &pBody, &bodyLen);
    if (code < 0) {
      uDebug("proc:%s, get no message from queue:%p and exiting", pQueue->name, pQueue);
      break;
    } else if (code < 0) {
      uTrace("proc:%s, get no message from queue:%p since %s", pQueue->name, pQueue, terrstr());
      taosMsleep(1);
      continue;
    } else {
      (*consumeFp)(pParent, pHead, headLen, pBody, bodyLen);
    }
  }
}

int32_t taosProcRun(SProcObj *pProc) {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (pProc->isChild || pProc->testFlag) {
    if (taosThreadCreate(&pProc->childThread, &thAttr, (ProcThreadFp)taosProcThreadLoop, pProc->pChildQueue) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      uError("failed to create thread since %s", terrstr());
      return -1;
    }
    uDebug("proc:%s, child start to consume queue:%p", pProc->name, pProc->pChildQueue);
  }

  if (!pProc->isChild || pProc->testFlag) {
    if (taosThreadCreate(&pProc->parentThread, &thAttr, (ProcThreadFp)taosProcThreadLoop, pProc->pParentQueue) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      uError("failed to create thread since %s", terrstr());
      return -1;
    }
    uDebug("proc:%s, parent start to consume queue:%p", pProc->name, pProc->pParentQueue);
  }

  return 0;
}

void taosProcStop(SProcObj *pProc) {
  pProc->stopFlag = true;
  // todo join
}

bool taosProcIsChild(SProcObj *pProc) { return pProc->isChild; }

void taosProcCleanup(SProcObj *pProc) {
  if (pProc != NULL) {
    uDebug("proc:%s, clean up", pProc->name);
    taosProcStop(pProc);
    taosProcQueueCleanup(pProc->pChildQueue);
    taosProcQueueCleanup(pProc->pParentQueue);
    taosMemoryFree(pProc);
  }
}

int32_t taosProcPutToChildQueue(SProcObj *pProc, void *pHead, int32_t headLen, void *pBody, int32_t bodyLen) {
  return taosProcQueuePush(pProc->pChildQueue, pHead, headLen, pBody, bodyLen);
}

int32_t taosProcPutToParentQueue(SProcObj *pProc, void *pHead, int32_t headLen, void *pBody, int32_t bodyLen) {
  return taosProcQueuePush(pProc->pParentQueue, pHead, headLen, pBody, bodyLen);
}
