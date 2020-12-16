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
#include "tqueue.h"
#include "dnodeVWrite.h"

typedef struct {
  taos_qall qall;
  taos_qset qset;      // queue set
  int32_t   workerId;  // worker ID
  pthread_t thread;    // thread
} SVWriteWorker;

typedef struct {
  int32_t max;     // max number of workers
  int32_t nextId;  // from 0 to max-1, cyclic
  SVWriteWorker * worker;
  pthread_mutex_t mutex;
} SVWriteWorkerPool;

static SVWriteWorkerPool tsVWriteWP;
static void *dnodeProcessVWriteQueue(void *pWorker);

int32_t dnodeInitVWrite() {
  tsVWriteWP.max = tsNumOfCores;
  tsVWriteWP.worker = tcalloc(sizeof(SVWriteWorker), tsVWriteWP.max);
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
    SVWriteWorker *pWorker = tsVWriteWP.worker + i;
    if (pWorker->thread) {
      taosQsetThreadResume(pWorker->qset);
    }
  }

  for (int32_t i = 0; i < tsVWriteWP.max; ++i) {
    SVWriteWorker *pWorker = tsVWriteWP.worker + i;
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

void dnodeDispatchToVWriteQueue(SRpcMsg *pRpcMsg) {
  int32_t code;
  char *pCont = pRpcMsg->pCont;

  if (pRpcMsg->msgType == TSDB_MSG_TYPE_SUBMIT) {
    SMsgDesc *pDesc = (SMsgDesc *)pCont;
    pDesc->numOfVnodes = htonl(pDesc->numOfVnodes);
    pCont += sizeof(SMsgDesc);
  }

  SMsgHead *pMsg = (SMsgHead *)pCont;
  pMsg->vgId = htonl(pMsg->vgId);
  pMsg->contLen = htonl(pMsg->contLen);

  void *pVnode = vnodeAcquire(pMsg->vgId);
  if (pVnode == NULL) {
    code = TSDB_CODE_VND_INVALID_VGROUP_ID;
  } else {
    SWalHead *pHead = (SWalHead *)(pCont - sizeof(SWalHead));
    pHead->msgType = pRpcMsg->msgType;
    pHead->version = 0;
    pHead->len = pMsg->contLen;
    code = vnodeWriteToWQueue(pVnode, pHead, TAOS_QTYPE_RPC, pRpcMsg);
  }

  if (code != TSDB_CODE_SUCCESS) {
    SRpcMsg rpcRsp = {.handle = pRpcMsg->handle, .code = code};
    rpcSendResponse(&rpcRsp);
  }

  vnodeRelease(pVnode);
}

void *dnodeAllocVWriteQueue(void *pVnode) {
  pthread_mutex_lock(&tsVWriteWP.mutex);
  SVWriteWorker *pWorker = tsVWriteWP.worker + tsVWriteWP.nextId;
  taos_queue *queue = taosOpenQueue();
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

    if (pthread_create(&pWorker->thread, &thAttr, dnodeProcessVWriteQueue, pWorker) != 0) {
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

void dnodeFreeVWriteQueue(void *pWqueue) {
  taosCloseQueue(pWqueue);
}

void dnodeSendRpcVWriteRsp(void *pVnode, void *wparam, int32_t code) {
  if (wparam == NULL) return;
  SVWriteMsg *pWrite = wparam;

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
  vnodeFreeFromWQueue(pVnode, pWrite);
}

static void *dnodeProcessVWriteQueue(void *wparam) {
  SVWriteWorker *pWorker = wparam;
  SVWriteMsg *   pWrite;
  void *         pVnode;
  int32_t        numOfMsgs;
  int32_t        qtype;

  dDebug("dnode vwrite worker:%d is running", pWorker->workerId);

  while (1) {
    numOfMsgs = taosReadAllQitemsFromQset(pWorker->qset, pWorker->qall, &pVnode);
    if (numOfMsgs == 0) {
      dDebug("qset:%p, dnode vwrite got no message from qset, exiting", pWorker->qset);
      break;
    }

    bool forceFsync = false;
    for (int32_t i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(pWorker->qall, &qtype, (void **)&pWrite);
      dTrace("msg:%p, app:%p type:%s will be processed in vwrite queue, qtype:%s hver:%" PRIu64, pWrite,
             pWrite->rpcMsg.ahandle, taosMsg[pWrite->pHead->msgType], qtypeStr[qtype], pWrite->pHead->version);

      pWrite->code = vnodeProcessWrite(pVnode, pWrite->pHead, qtype, &pWrite->rspRet);
      if (pWrite->code <= 0) pWrite->processedCount = 1;
      if (pWrite->code == 0 && pWrite->pHead->msgType != TSDB_MSG_TYPE_SUBMIT) forceFsync = true;

      dTrace("msg:%p is processed in vwrite queue, result:%s", pWrite, tstrerror(pWrite->code));
    }

    walFsync(vnodeGetWal(pVnode), forceFsync);

    // browse all items, and process them one by one
    taosResetQitems(pWorker->qall);
    for (int32_t i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(pWorker->qall, &qtype, (void **)&pWrite);
      if (qtype == TAOS_QTYPE_RPC) {
        dnodeSendRpcVWriteRsp(pVnode, pWrite, pWrite->code);
      } else {
        if (qtype == TAOS_QTYPE_FWD) {
          vnodeConfirmForward(pVnode, pWrite->pHead->version, 0);
        }
        if (pWrite->rspRet.rsp) {
          rpcFreeCont(pWrite->rspRet.rsp);
        }
        vnodeFreeFromWQueue(pVnode, pWrite);
      }
    }
  }

  return NULL;
}
