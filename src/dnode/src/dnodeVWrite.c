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
#include "tglobal.h"
#include "tqueue.h"
#include "tsdb.h"
#include "twal.h"
#include "tsync.h"
#include "vnode.h"
#include "syncInt.h"
#include "dnodeInt.h"

typedef struct {
  taos_qall qall;
  taos_qset qset;      // queue set
  int32_t   workerId;  // worker ID
  pthread_t thread;    // thread
} SWriteWorker;

typedef struct {
  SRspRet rspRet;
  SRpcMsg rpcMsg;
  int32_t processedCount;
  int32_t code;
  int32_t contLen;
  void *  pCont;
} SWriteMsg;

typedef struct {
  int32_t max;     // max number of workers
  int32_t nextId;  // from 0 to max-1, cyclic
  SWriteWorker *worker;
  pthread_mutex_t mutex;
} SWriteWorkerPool;

static SWriteWorkerPool tsVWriteWP;
static void *dnodeProcessWriteQueue(void *param);

int32_t dnodeInitVWrite() {
  tsVWriteWP.max = tsNumOfCores;
  tsVWriteWP.worker = (SWriteWorker *)tcalloc(sizeof(SWriteWorker), tsVWriteWP.max);
  if (tsVWriteWP.worker == NULL) return -1;
  pthread_mutex_init(&tsVWriteWP.mutex, NULL);

  for (int32_t i = 0; i < tsVWriteWP.max; ++i) {
    tsVWriteWP.worker[i].workerId = i;
  }

  dInfo("dnode vwrite is initialized, max worker %d", tsVWriteWP.max);
  return 0;
}

void dnodeCleanupVWrite() {
  for (int32_t i = 0; i < tsVWriteWP.max; ++i) {
    SWriteWorker *pWorker = tsVWriteWP.worker + i;
    if (pWorker->thread) {
      taosQsetThreadResume(pWorker->qset);
    }
  }

  for (int32_t i = 0; i < tsVWriteWP.max; ++i) {
    SWriteWorker *pWorker = tsVWriteWP.worker + i;
    if (pWorker->thread) {
      pthread_join(pWorker->thread, NULL);
      taosFreeQall(pWorker->qall);
      taosCloseQset(pWorker->qset);
    }
  }

  pthread_mutex_destroy(&tsVWriteWP.mutex);
  tfree(tsVWriteWP.worker);
  dInfo("dnode vwrite is closed");
}

void dnodeDispatchToVWriteQueue(SRpcMsg *pMsg) {
  char *pCont = pMsg->pCont;

  if (pMsg->msgType == TSDB_MSG_TYPE_SUBMIT) {
    SMsgDesc *pDesc = (SMsgDesc *)pCont;
    pDesc->numOfVnodes = htonl(pDesc->numOfVnodes);
    pCont += sizeof(SMsgDesc);
  }

  SMsgHead *pHead = (SMsgHead *) pCont;
  pHead->vgId     = htonl(pHead->vgId);
  pHead->contLen  = htonl(pHead->contLen);

  taos_queue queue = vnodeAcquireWqueue(pHead->vgId);
  if (queue) {
    // put message into queue
    SWriteMsg *pWrite = taosAllocateQitem(sizeof(SWriteMsg));
    pWrite->rpcMsg    = *pMsg;
    pWrite->pCont     = pCont;
    pWrite->contLen   = pHead->contLen;

    taosWriteQitem(queue, TAOS_QTYPE_RPC, pWrite);
  } else {
    SRpcMsg rpcRsp = {
      .handle  = pMsg->handle,
      .pCont   = NULL,
      .contLen = 0,
      .code    = TSDB_CODE_VND_INVALID_VGROUP_ID,
      .msgType = 0
    };
    rpcSendResponse(&rpcRsp);
    rpcFreeCont(pMsg->pCont);
  }
}

void *dnodeAllocVWriteQueue(void *pVnode) {
  pthread_mutex_lock(&tsVWriteWP.mutex);
  SWriteWorker *pWorker = tsVWriteWP.worker + tsVWriteWP.nextId;
  void *queue = taosOpenQueue();
  if (queue == NULL) {
    pthread_mutex_unlock(&tsVWriteWP.mutex);
    return NULL;
  }

  if (pWorker->qset == NULL) {
    pWorker->qset = taosOpenQset();
    if (pWorker->qset == NULL) {
      taosCloseQueue(queue);
      pthread_mutex_unlock(&tsVWriteWP.mutex);
      return NULL;
    }

    taosAddIntoQset(pWorker->qset, queue, pVnode);
    pWorker->qall = taosAllocateQall();
    if (pWorker->qall == NULL) {
      taosCloseQset(pWorker->qset);
      taosCloseQueue(queue);
      pthread_mutex_unlock(&tsVWriteWP.mutex);
      return NULL;
    }
    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, dnodeProcessWriteQueue, pWorker) != 0) {
      dError("failed to create thread to process vwrite queue since %s", strerror(errno));
      taosFreeQall(pWorker->qall);
      taosCloseQset(pWorker->qset);
      taosCloseQueue(queue);
      queue = NULL;
    } else {
      dDebug("dnode vwrite worker:%d is launched", pWorker->workerId);
      tsVWriteWP.nextId = (tsVWriteWP.nextId + 1) % tsVWriteWP.max;
    }

    pthread_attr_destroy(&thAttr);
  } else {
    taosAddIntoQset(pWorker->qset, queue, pVnode);
    tsVWriteWP.nextId = (tsVWriteWP.nextId + 1) % tsVWriteWP.max;
  }

  pthread_mutex_unlock(&tsVWriteWP.mutex);
  dDebug("pVnode:%p, dnode vwrite queue:%p is allocated", pVnode, queue);

  return queue;
}

void dnodeFreeVWriteQueue(void *wqueue) {
  taosCloseQueue(wqueue);
}

void dnodeSendRpcVWriteRsp(void *pVnode, void *param, int32_t code) {
  if (param == NULL) return;
  SWriteMsg *pWrite = param;

  if (code < 0) pWrite->code = code;
  int32_t count = atomic_add_fetch_32(&pWrite->processedCount, 1);

  if (count <= 1) return;

  SRpcMsg rpcRsp = {
    .handle  = pWrite->rpcMsg.handle,
    .pCont   = pWrite->rspRet.rsp,
    .contLen = pWrite->rspRet.len,
    .code    = pWrite->code,
  };

  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pWrite->rpcMsg.pCont);
  taosFreeQitem(pWrite);

  vnodeRelease(pVnode);
}

static void *dnodeProcessWriteQueue(void *param) {
  SWriteWorker *pWorker = (SWriteWorker *)param;
  SWriteMsg *   pWrite;
  SWalHead *    pHead;
  SRspRet *     pRspRet;
  void *        pVnode;
  void *        pItem;
  int32_t       numOfMsgs;
  int32_t       qtype;

  dDebug("dnode vwrite worker:%d is running", pWorker->workerId);

  while (1) {
    numOfMsgs = taosReadAllQitemsFromQset(pWorker->qset, pWorker->qall, &pVnode);
    if (numOfMsgs == 0) {
      dDebug("qset:%p, dnode vwrite got no message from qset, exiting", pWorker->qset);
      break;
    }

    for (int32_t i = 0; i < numOfMsgs; ++i) {
      pWrite = NULL;
      pRspRet = NULL;
      taosGetQitem(pWorker->qall, &qtype, &pItem);
      if (qtype == TAOS_QTYPE_RPC) {
        pWrite = pItem;
        pRspRet = &pWrite->rspRet;
        pHead = (SWalHead *)((char *)pWrite->pCont - sizeof(SWalHead));
        pHead->msgType = pWrite->rpcMsg.msgType;
        pHead->version = 0;
        pHead->len = pWrite->contLen;
        dDebug("%p, rpc msg:%s will be processed in vwrite queue", pWrite->rpcMsg.ahandle,
               taosMsg[pWrite->rpcMsg.msgType]);
      } else if (qtype == TAOS_QTYPE_CQ) {
        pHead = (SWalHead *)((char *)pItem + sizeof(SSyncHead));
        dTrace("%p, CQ wal msg:%s will be processed in vwrite queue, version:%" PRIu64, pHead, taosMsg[pHead->msgType],
               pHead->version);
      } else {
        pHead = pItem;
        dTrace("%p, wal msg:%s will be processed in vwrite queue, version:%" PRIu64, pHead, taosMsg[pHead->msgType],
               pHead->version);
      }

      int32_t code = vnodeProcessWrite(pVnode, qtype, pHead, pRspRet);
      dTrace("%p, msg:%s is processed in vwrite queue, version:%" PRIu64 ", result:%s", pHead, taosMsg[pHead->msgType],
             pHead->version, tstrerror(code));

      if (pWrite) {
        pWrite->rpcMsg.code = code;
        if (code <= 0) pWrite->processedCount = 1;
      }
    }

    walFsync(vnodeGetWal(pVnode));

    // browse all items, and process them one by one
    taosResetQitems(pWorker->qall);
    for (int32_t i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(pWorker->qall, &qtype, &pItem);
      if (qtype == TAOS_QTYPE_RPC) {
        pWrite = pItem;
        dnodeSendRpcVWriteRsp(pVnode, pItem, pWrite->rpcMsg.code);
      } else if (qtype == TAOS_QTYPE_FWD) {
        pHead = pItem;
        vnodeConfirmForward(pVnode, pHead->version, 0);
        taosFreeQitem(pItem);
        vnodeRelease(pVnode);
      } else {
        taosFreeQitem(pItem);
        vnodeRelease(pVnode);
      }
    }
  }

  return NULL;
}
