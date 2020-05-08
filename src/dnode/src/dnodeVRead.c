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
#include "taoserror.h"
#include "taosmsg.h"
#include "tqueue.h"
#include "trpc.h"
#include "twal.h"
#include "tglobal.h"
#include "dnodeInt.h"
#include "dnodeMgmt.h"
#include "dnodeVRead.h"
#include "vnode.h"

typedef struct {
  SRspRet  rspRet;
  void    *pCont;
  int32_t  contLen;
  SRpcMsg  rpcMsg;
} SReadMsg;

typedef struct {
  pthread_t  thread;    // thread 
  int32_t    workerId;  // worker ID
} SReadWorker;

typedef struct {
  int32_t    max;       // max number of workers
  int32_t    min;       // min number of workers
  int32_t    num;       // current number of workers
  SReadWorker *readWorker;
} SReadWorkerPool;

static void *dnodeProcessReadQueue(void *param);
static void  dnodeHandleIdleReadWorker(SReadWorker *);

// module global variable
static SReadWorkerPool readPool;
static taos_qset       readQset;

int32_t dnodeInitRead() {
  readQset = taosOpenQset();

  readPool.min = 2;
  readPool.max = tsNumOfCores * tsNumOfThreadsPerCore;
  if (readPool.max <= readPool.min * 2) readPool.max = 2 * readPool.min;
  readPool.readWorker = (SReadWorker *) calloc(sizeof(SReadWorker), readPool.max);

  if (readPool.readWorker == NULL) return -1;
  for (int i=0; i < readPool.max; ++i) {
    SReadWorker *pWorker = readPool.readWorker + i;
    pWorker->workerId = i;
  }

  dPrint("dnode read is opened");
  return 0;
}

void dnodeCleanupRead() {

  for (int i=0; i < readPool.max; ++i) {
    SReadWorker *pWorker = readPool.readWorker + i;
    if (pWorker->thread) {
      pthread_cancel(pWorker->thread);
      pthread_join(pWorker->thread, NULL);
    }
  }

  taosCloseQset(readQset);
  free(readPool.readWorker);

  dPrint("dnode read is closed");
}

void dnodeDispatchToVnodeReadQueue(SRpcMsg *pMsg) {
  int32_t     queuedMsgNum = 0;
  int32_t     leftLen      = pMsg->contLen;
  char        *pCont       = (char *) pMsg->pCont;
  void        *pVnode;  

  dTrace("dnode %s msg incoming, thandle:%p", taosMsg[pMsg->msgType], pMsg->handle);

  while (leftLen > 0) {
    SMsgHead *pHead = (SMsgHead *) pCont;
    pHead->vgId    = htonl(pHead->vgId);
    pHead->contLen = htonl(pHead->contLen);

    if (pMsg->msgType == TSDB_MSG_TYPE_FETCH) {
      pVnode = vnodeGetVnode(pHead->vgId);
    } else {
      pVnode = vnodeAccquireVnode(pHead->vgId);
    }

    if (pVnode == NULL) {
      leftLen -= pHead->contLen;
      pCont -= pHead->contLen;
      continue;
    }

    // put message into queue
    taos_queue queue = vnodeGetRqueue(pVnode);
    SReadMsg *pRead = (SReadMsg *)taosAllocateQitem(sizeof(SReadMsg));
    pRead->rpcMsg      = *pMsg;
    pRead->pCont       = pCont;
    pRead->contLen     = pHead->contLen;

    // next vnode
    leftLen -= pHead->contLen;
    pCont -= pHead->contLen;
    queuedMsgNum++;

    taosWriteQitem(queue, TAOS_QTYPE_RPC, pRead);
  }

  if (queuedMsgNum == 0) {
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

void *dnodeAllocateRqueue(void *pVnode) {
  taos_queue queue = taosOpenQueue();
  if (queue == NULL) return NULL;

  taosAddIntoQset(readQset, queue, pVnode);

  // spawn a thread to process queue
  if (readPool.num < readPool.max) {
    do {
      SReadWorker *pWorker = readPool.readWorker + readPool.num;

      pthread_attr_t thAttr;
      pthread_attr_init(&thAttr);
      pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

      if (pthread_create(&pWorker->thread, &thAttr, dnodeProcessReadQueue, pWorker) != 0) {
        dError("failed to create thread to process read queue, reason:%s", strerror(errno));
      }

      pthread_attr_destroy(&thAttr);
      readPool.num++;
      dTrace("read worker:%d is launched, total:%d", pWorker->workerId, readPool.num);
    } while (readPool.num < readPool.min);
  }

  dTrace("pVnode:%p, read queue:%p is allocated", pVnode, queue); 

  return queue;
}

void dnodeFreeRqueue(void *rqueue) {
  taosCloseQueue(rqueue);

  // dynamically adjust the number of threads
}

static void dnodeContinueExecuteQuery(void* pVnode, void* qhandle, SReadMsg *pMsg) {  
  SReadMsg *pRead = (SReadMsg *)taosAllocateQitem(sizeof(SReadMsg));
  pRead->rpcMsg      = pMsg->rpcMsg;
  pRead->pCont       = qhandle;
  pRead->contLen     = 0;
  pRead->rpcMsg.msgType = TSDB_MSG_TYPE_QUERY;
  
  taos_queue queue = vnodeGetRqueue(pVnode);
  taosWriteQitem(queue, TAOS_QTYPE_RPC, pRead);
}

void dnodeSendRpcReadRsp(void *pVnode, SReadMsg *pRead, int32_t code) {
  if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;
  if (code == TSDB_CODE_ACTION_NEED_REPROCESSED) {
    dnodeContinueExecuteQuery(pVnode, pRead->rspRet.qhandle, pRead);
  }

  SRpcMsg rpcRsp = {
    .handle  = pRead->rpcMsg.handle,
    .pCont   = pRead->rspRet.rsp,
    .contLen = pRead->rspRet.len,
    .code    = pRead->rspRet.code,
  };

  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pRead->rpcMsg.pCont);
}

static void *dnodeProcessReadQueue(void *param) {
  SReadWorker *pWorker = param;
  SReadMsg    *pReadMsg;
  int          type;
  void        *pVnode;

  while (1) {
    if (taosReadQitemFromQset(readQset, &type, (void **)&pReadMsg, &pVnode) == 0) {
      dnodeHandleIdleReadWorker(pWorker);
      continue;
    }

    int32_t code = vnodeProcessRead(pVnode, pReadMsg->rpcMsg.msgType, pReadMsg->pCont, pReadMsg->contLen, &pReadMsg->rspRet);
    dnodeSendRpcReadRsp(pVnode, pReadMsg, code);
    taosFreeQitem(pReadMsg);
  }

  return NULL;
}

static void dnodeHandleIdleReadWorker(SReadWorker *pWorker) {
  int32_t num = taosGetQueueNumber(readQset);

  if (num == 0 || (num <= readPool.min && readPool.num > readPool.min)) {
    readPool.num--;
    dTrace("read worker:%d is released, total:%d", pWorker->workerId, readPool.num);
    pthread_exit(NULL);
  } else {
    usleep(30000);
    sched_yield();
  }
}

