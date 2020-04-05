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
#include "taosmsg.h"
#include "taoserror.h"
#include "tlog.h"
#include "tqueue.h"
#include "trpc.h"
#include "tsdb.h"
#include "twal.h"
#include "dataformat.h"
#include "dnodeWrite.h"
#include "dnodeMgmt.h"
#include "vnode.h"

typedef struct {
  taos_qset  qset;      // queue set
  pthread_t  thread;    // thread 
  int32_t    workerId;  // worker ID
} SWriteWorker;  

typedef struct {
  SRspRet  rspRet;
  void    *pCont;
  int32_t  contLen;
  SRpcMsg  rpcMsg;
} SWriteMsg;

typedef struct _thread_obj {
  int32_t        max;        // max number of workers
  int32_t        nextId;     // from 0 to max-1, cyclic
  SWriteWorker  *writeWorker;
} SWriteWorkerPool;

static void *dnodeProcessWriteQueue(void *param);
static void  dnodeHandleIdleWorker(SWriteWorker *pWorker);

SWriteWorkerPool wWorkerPool;

int32_t dnodeInitWrite() {

  wWorkerPool.max = tsNumOfCores;
  wWorkerPool.writeWorker = (SWriteWorker *)calloc(sizeof(SWriteWorker), wWorkerPool.max);
  if (wWorkerPool.writeWorker == NULL) return -1;

  for (int32_t i = 0; i < wWorkerPool.max; ++i) {
    wWorkerPool.writeWorker[i].workerId = i;
  }

  dPrint("dnode write is opened");
  return 0;
}

void dnodeCleanupWrite() {
  free(wWorkerPool.writeWorker);
  dPrint("dnode write is closed");
}

void dnodeWrite(SRpcMsg *pMsg) {
  char        *pCont       = (char *) pMsg->pCont;

  if (pMsg->msgType == TSDB_MSG_TYPE_SUBMIT || pMsg->msgType == TSDB_MSG_TYPE_MD_DROP_STABLE) {
    SMsgDesc *pDesc = (SMsgDesc *)pCont;
    pDesc->numOfVnodes = htonl(pDesc->numOfVnodes);
    pCont += sizeof(SMsgDesc);
  }

  SMsgHead *pHead = (SMsgHead *) pCont;
  pHead->vgId    = htonl(pHead->vgId);
  pHead->contLen = htonl(pHead->contLen);

  taos_queue queue = vnodeGetWqueue(pHead->vgId);
  if (queue) {
    // put message into queue
    SWriteMsg *pWrite = (SWriteMsg *)taosAllocateQitem(sizeof(SWriteMsg));
    pWrite->rpcMsg      = *pMsg;
    pWrite->pCont       = pCont;
    pWrite->contLen     = pHead->contLen;

    taosWriteQitem(queue, TAOS_QTYPE_RPC, pWrite);
  } else {
    SRpcMsg rpcRsp = {
      .handle  = pMsg->handle,
      .pCont   = NULL,
      .contLen = 0,
      .code    = TSDB_CODE_INVALID_VGROUP_ID,
      .msgType = 0
    };
    rpcSendResponse(&rpcRsp);
  }
}

void *dnodeAllocateWqueue(void *pVnode) {
  SWriteWorker *pWorker = wWorkerPool.writeWorker + wWorkerPool.nextId;
  taos_queue *queue = taosOpenQueue();
  if (queue == NULL) return NULL;

  if (pWorker->qset == NULL) {
    pWorker->qset = taosOpenQset();
    if (pWorker->qset == NULL) return NULL;

    taosAddIntoQset(pWorker->qset, queue, pVnode);
    wWorkerPool.nextId = (wWorkerPool.nextId + 1) % wWorkerPool.max;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, dnodeProcessWriteQueue, pWorker) != 0) {
      dError("failed to create thread to process read queue, reason:%s", strerror(errno));
      taosCloseQset(pWorker->qset);
    }
  } else {
    taosAddIntoQset(pWorker->qset, queue, pVnode);
    wWorkerPool.nextId = (wWorkerPool.nextId + 1) % wWorkerPool.max;
  }

  dTrace("queue:%p is allocated for pVnode:%p", queue, pVnode);

  return queue;
}

void dnodeFreeWqueue(void *wqueue) {
  taosCloseQueue(wqueue);

  // dynamically adjust the number of threads
}

void dnodeSendRpcWriteRsp(void *pVnode, void *param, int32_t code) {
  SWriteMsg *pWrite = (SWriteMsg *)param;

  if (code > 0) return;

  SRpcMsg rpcRsp = {
    .handle  = pWrite->rpcMsg.handle,
    .pCont   = pWrite->rspRet.rsp,
    .contLen = pWrite->rspRet.len,
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pWrite->rpcMsg.pCont);
  taosFreeQitem(pWrite);

  vnodeRelease(pVnode);
}

static void *dnodeProcessWriteQueue(void *param) {
  SWriteWorker *pWorker = (SWriteWorker *)param;
  taos_qall     qall;
  SWriteMsg    *pWrite;
  SWalHead     *pHead;
  int32_t       numOfMsgs;
  int           type;
  void         *pVnode, *item;

  qall = taosAllocateQall();

  while (1) {
    numOfMsgs = taosReadAllQitemsFromQset(pWorker->qset, qall, &pVnode);
    if (numOfMsgs <=0) { 
      dnodeHandleIdleWorker(pWorker);  // thread exit if no queues anymore
      continue;
    }

    for (int32_t i = 0; i < numOfMsgs; ++i) {
      pWrite = NULL;
      taosGetQitem(qall, &type, &item);
      if (type == TAOS_QTYPE_RPC) {
        pWrite = (SWriteMsg *)item;
        pHead = (SWalHead *)(pWrite->pCont - sizeof(SWalHead));
        pHead->msgType = pWrite->rpcMsg.msgType;
        pHead->version = 0;
        pHead->len = pWrite->contLen;
      } else {
        pHead = (SWalHead *)item;
      }

      int32_t code = vnodeProcessWrite(pVnode, type, pHead, item);
      if (pWrite) pWrite->rpcMsg.code = code;
    }

    walFsync(vnodeGetWal(pVnode));

    // browse all items, and process them one by one
    taosResetQitems(qall);
    for (int32_t i = 0; i < numOfMsgs; ++i) {
      taosGetQitem(qall, &type, &item);
      if (type == TAOS_QTYPE_RPC) {
        pWrite = (SWriteMsg *)item;
        dnodeSendRpcWriteRsp(pVnode, item, pWrite->rpcMsg.code); 
      } else {
        taosFreeQitem(item);
        vnodeRelease(pVnode);
      }
    }
  }

  taosFreeQall(qall);

  return NULL;
}

static void dnodeHandleIdleWorker(SWriteWorker *pWorker) {
  int32_t num = taosGetQueueNumber(pWorker->qset);

  if (num > 0) {
     usleep(100);
     sched_yield(); 
  } else {
     taosCloseQset(pWorker->qset);
     pWorker->qset = NULL;
     pthread_exit(NULL);
  }
}

