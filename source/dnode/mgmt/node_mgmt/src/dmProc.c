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
#include "dmMgmt.h"

static inline int32_t CEIL8(int32_t v) { return ceil((float)(v) / 8) * 8; }

static int32_t dmInitProcMutex(SProcQueue *queue) {
  TdThreadMutexAttr mattr = {0};

  if (taosThreadMutexAttrInit(&mattr) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("node:%s, failed to init mutex while init attr since %s", queue->name, terrstr());
    return -1;
  }

  if (taosThreadMutexAttrSetPshared(&mattr, PTHREAD_PROCESS_SHARED) != 0) {
    taosThreadMutexAttrDestroy(&mattr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("node:%s, failed to init mutex while set shared since %s", queue->name, terrstr());
    return -1;
  }

  if (taosThreadMutexInit(&queue->mutex, &mattr) != 0) {
    taosThreadMutexAttrDestroy(&mattr);
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("node:%s, failed to init mutex since %s", queue->name, terrstr());
    return -1;
  }

  taosThreadMutexAttrDestroy(&mattr);
  return 0;
}

static int32_t dmInitProcSem(SProcQueue *queue) {
  if (tsem_init(&queue->sem, 1, 0) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("node:%s, failed to init sem since %s", queue->name, terrstr());
    return -1;
  }

  return 0;
}

static SProcQueue *dmInitProcQueue(SProc *proc, char *ptr, int32_t size) {
  SProcQueue *queue = (SProcQueue *)(ptr);

  int32_t bufSize = size - CEIL8(sizeof(SProcQueue));
  if (bufSize <= 1024) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (proc->ptype & DND_PROC_PARENT) {
    if (dmInitProcMutex(queue) != 0) {
      return NULL;
    }

    if (dmInitProcSem(queue) != 0) {
      return NULL;
    }

    tstrncpy(queue->name, proc->name, sizeof(queue->name));
    queue->head = 0;
    queue->tail = 0;
    queue->total = bufSize;
    queue->avail = bufSize;
    queue->items = 0;
  }

  return queue;
}

static void dmCleanupProcQueue(SProcQueue *queue) {}

static inline int32_t dmPushToProcQueue(SProc *proc, SProcQueue *queue, SRpcMsg *pMsg, EProcFuncType ftype) {
  const void   *pHead = pMsg;
  const void   *pBody = pMsg->pCont;
  const int16_t rawHeadLen = sizeof(SRpcMsg);
  const int32_t rawBodyLen = pMsg->contLen;
  const int16_t headLen = CEIL8(rawHeadLen);
  const int32_t bodyLen = CEIL8(rawBodyLen);
  const int32_t fullLen = headLen + bodyLen + 8;
  const int64_t handle = (int64_t)pMsg->info.handle;

  taosThreadMutexLock(&queue->mutex);
  if (fullLen > queue->avail) {
    taosThreadMutexUnlock(&queue->mutex);
    terrno = TSDB_CODE_OUT_OF_SHM_MEM;
    return -1;
  }

  if (ftype == DND_FUNC_REQ && IsReq(pMsg) && pMsg->code == 0 && handle != 0) {
    if (taosHashPut(proc->hash, &handle, sizeof(int64_t), &pMsg->info, sizeof(SRpcConnInfo)) != 0) {
      taosThreadMutexUnlock(&queue->mutex);
      return -1;
    }
  }

  const int32_t pos = queue->tail;
  if (queue->tail < queue->total) {
    *(int16_t *)(queue->pBuffer + queue->tail) = rawHeadLen;
    *(int8_t *)(queue->pBuffer + queue->tail + 2) = (int8_t)ftype;
    *(int32_t *)(queue->pBuffer + queue->tail + 4) = rawBodyLen;
  } else {
    *(int16_t *)(queue->pBuffer) = rawHeadLen;
    *(int8_t *)(queue->pBuffer + 2) = (int8_t)ftype;
    *(int32_t *)(queue->pBuffer + 4) = rawBodyLen;
  }

  if (queue->tail < queue->head) {
    memcpy(queue->pBuffer + queue->tail + 8, pHead, rawHeadLen);
    if (rawBodyLen > 0) memcpy(queue->pBuffer + queue->tail + 8 + headLen, pBody, rawBodyLen);
    queue->tail = queue->tail + 8 + headLen + bodyLen;
  } else {
    int32_t remain = queue->total - queue->tail;
    if (remain == 0) {
      memcpy(queue->pBuffer + 8, pHead, rawHeadLen);
      if (rawBodyLen > 0) memcpy(queue->pBuffer + 8 + headLen, pBody, rawBodyLen);
      queue->tail = 8 + headLen + bodyLen;
    } else if (remain == 8) {
      memcpy(queue->pBuffer, pHead, rawHeadLen);
      if (rawBodyLen > 0) memcpy(queue->pBuffer + headLen, pBody, rawBodyLen);
      queue->tail = headLen + bodyLen;
    } else if (remain < 8 + headLen) {
      memcpy(queue->pBuffer + queue->tail + 8, pHead, remain - 8);
      memcpy(queue->pBuffer, pHead + remain - 8, rawHeadLen - (remain - 8));
      if (rawBodyLen > 0) memcpy(queue->pBuffer + headLen - (remain - 8), pBody, rawBodyLen);
      queue->tail = headLen - (remain - 8) + bodyLen;
    } else if (remain < 8 + headLen + bodyLen) {
      memcpy(queue->pBuffer + queue->tail + 8, pHead, rawHeadLen);
      if (rawBodyLen > 0) memcpy(queue->pBuffer + queue->tail + 8 + headLen, pBody, remain - 8 - headLen);
      if (rawBodyLen > 0) memcpy(queue->pBuffer, pBody + remain - 8 - headLen, rawBodyLen - (remain - 8 - headLen));
      queue->tail = bodyLen - (remain - 8 - headLen);
    } else {
      memcpy(queue->pBuffer + queue->tail + 8, pHead, rawHeadLen);
      if (rawBodyLen > 0) memcpy(queue->pBuffer + queue->tail + headLen + 8, pBody, rawBodyLen);
      queue->tail = queue->tail + headLen + bodyLen + 8;
    }
  }

  queue->avail -= fullLen;
  queue->items++;
  taosThreadMutexUnlock(&queue->mutex);
  tsem_post(&queue->sem);

  dTrace("node:%s, push %s msg:%p type:%d handle:%p len:%d code:0x%x, pos:%d remain:%d", queue->name, dmFuncStr(ftype),
         pMsg, pMsg->msgType, pMsg->info.handle, pMsg->contLen, pMsg->code, pos, queue->items);
  return 0;
}

static int32_t dmPopFromProcQueue(SProcQueue *queue, SRpcMsg **ppMsg, EProcFuncType *pFuncType) {
  tsem_wait(&queue->sem);

  taosThreadMutexLock(&queue->mutex);
  if (queue->total - queue->avail <= 0) {
    taosThreadMutexUnlock(&queue->mutex);
    terrno = TSDB_CODE_OUT_OF_SHM_MEM;
    return 0;
  }

  int16_t rawHeadLen = 0;
  int8_t  ftype = 0;
  int32_t rawBodyLen = 0;
  if (queue->head < queue->total) {
    rawHeadLen = *(int16_t *)(queue->pBuffer + queue->head);
    ftype = *(int8_t *)(queue->pBuffer + queue->head + 2);
    rawBodyLen = *(int32_t *)(queue->pBuffer + queue->head + 4);
  } else {
    rawHeadLen = *(int16_t *)(queue->pBuffer);
    ftype = *(int8_t *)(queue->pBuffer + 2);
    rawBodyLen = *(int32_t *)(queue->pBuffer + 4);
  }
  int16_t headLen = CEIL8(rawHeadLen);
  int32_t bodyLen = CEIL8(rawBodyLen);

  void *pHead = taosAllocateQitem(headLen, DEF_QITEM);
  void *pBody = NULL;
  if (bodyLen > 0) pBody = rpcMallocCont(bodyLen);
  if (pHead == NULL || (bodyLen > 0 && pBody == NULL)) {
    taosThreadMutexUnlock(&queue->mutex);
    tsem_post(&queue->sem);
    taosFreeQitem(pHead);
    rpcFreeCont(pBody);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  const int32_t pos = queue->head;
  if (queue->head < queue->tail) {
    memcpy(pHead, queue->pBuffer + queue->head + 8, headLen);
    if (bodyLen > 0) memcpy(pBody, queue->pBuffer + queue->head + 8 + headLen, bodyLen);
    queue->head = queue->head + 8 + headLen + bodyLen;
  } else {
    int32_t remain = queue->total - queue->head;
    if (remain == 0) {
      memcpy(pHead, queue->pBuffer + 8, headLen);
      if (bodyLen > 0) memcpy(pBody, queue->pBuffer + 8 + headLen, bodyLen);
      queue->head = 8 + headLen + bodyLen;
    } else if (remain == 8) {
      memcpy(pHead, queue->pBuffer, headLen);
      if (bodyLen > 0) memcpy(pBody, queue->pBuffer + headLen, bodyLen);
      queue->head = headLen + bodyLen;
    } else if (remain < 8 + headLen) {
      memcpy(pHead, queue->pBuffer + queue->head + 8, remain - 8);
      memcpy((char *)pHead + remain - 8, queue->pBuffer, headLen - (remain - 8));
      if (bodyLen > 0) memcpy(pBody, queue->pBuffer + headLen - (remain - 8), bodyLen);
      queue->head = headLen - (remain - 8) + bodyLen;
    } else if (remain < 8 + headLen + bodyLen) {
      memcpy(pHead, queue->pBuffer + queue->head + 8, headLen);
      if (bodyLen > 0) memcpy(pBody, queue->pBuffer + queue->head + 8 + headLen, remain - 8 - headLen);
      if (bodyLen > 0) memcpy((char *)pBody + remain - 8 - headLen, queue->pBuffer, bodyLen - (remain - 8 - headLen));
      queue->head = bodyLen - (remain - 8 - headLen);
    } else {
      memcpy(pHead, queue->pBuffer + queue->head + 8, headLen);
      if (bodyLen > 0) memcpy(pBody, queue->pBuffer + queue->head + headLen + 8, bodyLen);
      queue->head = queue->head + headLen + bodyLen + 8;
    }
  }

  queue->avail = queue->avail + headLen + bodyLen + 8;
  queue->items--;
  taosThreadMutexUnlock(&queue->mutex);

  *ppMsg = pHead;
  (*ppMsg)->pCont = pBody;
  *pFuncType = (EProcFuncType)ftype;

  dTrace("node:%s, pop %s msg:%p type:%d handle:%p len:%d code:0x%x, pos:%d remain:%d", queue->name, dmFuncStr(ftype),
         (*ppMsg), (*ppMsg)->msgType, (*ppMsg)->info.handle, (*ppMsg)->contLen, (*ppMsg)->code, pos, queue->items);
  return 1;
}

int32_t dmInitProc(struct SMgmtWrapper *pWrapper) {
  SProc *proc = &pWrapper->proc;
  if (proc->name != NULL) return 0;

  proc->wrapper = pWrapper;
  proc->name = pWrapper->name;

  SShm   *shm = &proc->shm;
  int32_t cstart = 0;
  int32_t csize = CEIL8(shm->size / 2);
  int32_t pstart = csize;
  int32_t psize = CEIL8(shm->size - pstart);
  if (pstart + psize > shm->size) {
    psize -= 8;
  }

  proc->hash = taosHashInit(128, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  proc->cqueue = dmInitProcQueue(proc, (char *)shm->ptr + cstart, csize);
  proc->pqueue = dmInitProcQueue(proc, (char *)shm->ptr + pstart, psize);
  if (proc->cqueue == NULL || proc->pqueue == NULL || proc->hash == NULL) {
    dmCleanupProcQueue(proc->cqueue);
    dmCleanupProcQueue(proc->pqueue);
    taosHashCleanup(proc->hash);
    return -1;
  }

  dDebug("node:%s, proc is initialized, cqueue:%p pqueue:%p", proc->name, proc->cqueue, proc->pqueue);
  return 0;
}

static void *dmConsumChildQueue(void *param) {
  SProc        *proc = param;
  SMgmtWrapper *pWrapper = proc->wrapper;
  SProcQueue   *queue = proc->cqueue;
  int32_t       numOfMsgs = 0;
  int32_t       code = 0;
  EProcFuncType ftype = DND_FUNC_REQ;
  SRpcMsg      *pMsg = NULL;

  dDebug("node:%s, start to consume from cqueue", proc->name);
  do {
    numOfMsgs = dmPopFromProcQueue(queue, &pMsg, &ftype);
    if (numOfMsgs == 0) {
      dDebug("node:%s, get no msg from cqueue and exit thread", proc->name);
      break;
    }

    if (numOfMsgs < 0) {
      dError("node:%s, get no msg from cqueue since %s", proc->name, terrstr());
      taosMsleep(1);
      continue;
    }

    if (ftype != DND_FUNC_REQ) {
      dError("node:%s, invalid ftype:%d from cqueue", proc->name, ftype);
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
      continue;
    }

    code = dmProcessNodeMsg(pWrapper, pMsg);
    if (code != 0) {
      dError("node:%s, failed to process msg:%p since %s, put into pqueue", proc->name, pMsg, terrstr());
      SRpcMsg rsp = {
          .code = (terrno != 0 ? terrno : code),
          .pCont = pMsg->info.rsp,
          .contLen = pMsg->info.rspLen,
          .info = pMsg->info,
      };
      dmPutToProcPQueue(proc, &rsp, DND_FUNC_RSP);
      rpcFreeCont(pMsg->pCont);
      taosFreeQitem(pMsg);
    }
  } while (1);

  return NULL;
}

static void *dmConsumParentQueue(void *param) {
  SProc        *proc = param;
  SMgmtWrapper *pWrapper = proc->wrapper;
  SProcQueue   *queue = proc->pqueue;
  int32_t       numOfMsgs = 0;
  int32_t       code = 0;
  EProcFuncType ftype = DND_FUNC_REQ;
  SRpcMsg      *pMsg = NULL;

  dDebug("node:%s, start to consume from pqueue", proc->name);
  do {
    numOfMsgs = dmPopFromProcQueue(queue, &pMsg, &ftype);
    if (numOfMsgs == 0) {
      dDebug("node:%s, get no msg from pqueue and exit thread", proc->name);
      break;
    }

    if (numOfMsgs < 0) {
      dError("node:%s, get no msg from pqueue since %s", proc->name, terrstr());
      taosMsleep(1);
      continue;
    }

    if (ftype == DND_FUNC_RSP) {
      dmRemoveProcRpcHandle(proc, pMsg->info.handle);
      rpcSendResponse(pMsg);
    } else if (ftype == DND_FUNC_REGIST) {
      rpcRegisterBrokenLinkArg(pMsg);
    } else if (ftype == DND_FUNC_RELEASE) {
      dmRemoveProcRpcHandle(proc, pMsg->info.handle);
      rpcReleaseHandle(pMsg->info.handle, (int8_t)pMsg->code);
    } else {
      dError("node:%s, invalid ftype:%d from pqueue", proc->name, ftype);
      rpcFreeCont(pMsg->pCont);
    }

    taosFreeQitem(pMsg);
  } while (1);

  return NULL;
}

int32_t dmRunProc(SProc *proc) {
  TdThreadAttr thAttr = {0};
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);

  if (proc->ptype & DND_PROC_PARENT) {
    if (taosThreadCreate(&proc->pthread, &thAttr, dmConsumParentQueue, proc) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      dError("node:%s, failed to create pthread since %s", proc->name, terrstr());
      return -1;
    }
    dDebug("node:%s, thread:%" PRId64 " is created to consume pqueue", proc->name, proc->pthread);
  }

  if (proc->ptype & DND_PROC_CHILD) {
    if (taosThreadCreate(&proc->cthread, &thAttr, dmConsumChildQueue, proc) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      dError("node:%s, failed to create cthread since %s", proc->name, terrstr());
      return -1;
    }
    dDebug("node:%s, thread:%" PRId64 " is created to consume cqueue", proc->name, proc->cthread);
  }

  taosThreadAttrDestroy(&thAttr);
  return 0;
}

void dmStopProc(SProc *proc) {
  proc->stop = true;
  if (taosCheckPthreadValid(proc->pthread)) {
    dDebug("node:%s, start to join pthread:%" PRId64, proc->name, proc->pthread);
    tsem_post(&proc->pqueue->sem);
    taosThreadJoin(proc->pthread, NULL);
    taosThreadClear(&proc->pthread);
  }

  if (taosCheckPthreadValid(proc->cthread)) {
    dDebug("node:%s, start to join cthread:%" PRId64, proc->name, proc->cthread);
    tsem_post(&proc->cqueue->sem);
    taosThreadJoin(proc->cthread, NULL);
    taosThreadClear(&proc->cthread);
  }
}

void dmCleanupProc(struct SMgmtWrapper *pWrapper) {
  SProc *proc = &pWrapper->proc;
  if (proc->name == NULL) return;

  dDebug("node:%s, start to clean up proc", pWrapper->name);
  dmStopProc(proc);
  dmCleanupProcQueue(proc->cqueue);
  dmCleanupProcQueue(proc->pqueue);
  taosHashCleanup(proc->hash);
  proc->hash = NULL;
  dDebug("node:%s, proc is cleaned up", pWrapper->name);
}

void dmRemoveProcRpcHandle(SProc *proc, void *handle) {
  int64_t h = (int64_t)handle;
  taosThreadMutexLock(&proc->cqueue->mutex);
  taosHashRemove(proc->hash, &h, sizeof(int64_t));
  taosThreadMutexUnlock(&proc->cqueue->mutex);
}

void dmCloseProcRpcHandles(SProc *proc) {
  taosThreadMutexLock(&proc->cqueue->mutex);
  SRpcHandleInfo *pInfo = taosHashIterate(proc->hash, NULL);
  while (pInfo != NULL) {
    dError("node:%s, the child process dies and send an offline rsp to handle:%p", proc->name, pInfo->handle);
    SRpcMsg rpcMsg = {.info = *pInfo, .code = TSDB_CODE_NODE_OFFLINE};
    rpcSendResponse(&rpcMsg);
    pInfo = taosHashIterate(proc->hash, pInfo);
  }
  taosHashClear(proc->hash);
  taosThreadMutexUnlock(&proc->cqueue->mutex);
}

void dmPutToProcPQueue(SProc *proc, SRpcMsg *pMsg, EProcFuncType ftype) {
  int32_t retry = 0;
  while (1) {
    if (dmPushToProcQueue(proc, proc->pqueue, pMsg, ftype) == 0) {
      break;
    }

    if (retry == 10) {
      pMsg->code = terrno;
      if (pMsg->contLen > 0) {
        rpcFreeCont(pMsg->pCont);
        pMsg->pCont = NULL;
        pMsg->contLen = 0;
      }
      dError("node:%s, failed to push %s msg:%p type:%d handle:%p then discard data and return error", proc->name,
             dmFuncStr(ftype), pMsg, pMsg->msgType, pMsg->info.handle);
    } else {
      dError("node:%s, failed to push %s msg:%p type:%d handle:%p len:%d since %s, retry:%d", proc->name,
             dmFuncStr(ftype), pMsg, pMsg->msgType, pMsg->info.handle, pMsg->contLen, terrstr(), retry);
      retry++;
      taosMsleep(retry);
    }
  }
}

int32_t dmPutToProcCQueue(SProc *proc, SRpcMsg *pMsg, EProcFuncType ftype) {
  return dmPushToProcQueue(proc, proc->cqueue, pMsg, ftype);
}
