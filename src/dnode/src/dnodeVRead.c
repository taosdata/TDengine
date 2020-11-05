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
#include "tutil.h"
#include "tqueue.h"
#include "twal.h"
#include "tglobal.h"
#include "dnodeInt.h"
#include "dnodeMgmt.h"
#include "dnodeVRead.h"
#include "vnode.h"

typedef struct {
  pthread_t  thread;    // thread
  int32_t    workerId;  // worker ID
} SReadWorker;

typedef struct {
  int32_t    max;       // max number of workers
  int32_t    min;       // min number of workers
  int32_t    num;       // current number of workers
  SReadWorker *readWorker;
  pthread_mutex_t mutex;
} SReadWorkerPool;

static void *dnodeProcessReadQueue(void *param);
static void  dnodeHandleIdleReadWorker(SReadWorker *);

// module global variable
static SReadWorkerPool readPool;
static taos_qset       readQset;

int32_t dnodeInitVnodeRead() {
  readQset = taosOpenQset();

  readPool.min = tsNumOfCores;
  readPool.max = tsNumOfCores * tsNumOfThreadsPerCore;
  if (readPool.max <= readPool.min * 2) readPool.max = 2 * readPool.min;
  readPool.readWorker = (SReadWorker *)calloc(sizeof(SReadWorker), readPool.max);
  pthread_mutex_init(&readPool.mutex, NULL);

  if (readPool.readWorker == NULL) return -1;
  for (int i = 0; i < readPool.max; ++i) {
    SReadWorker *pWorker = readPool.readWorker + i;
    pWorker->workerId = i;
  }

  dInfo("dnode read is initialized, min worker:%d max worker:%d", readPool.min, readPool.max);
  return 0;
}

void dnodeCleanupVnodeRead() {
  for (int i = 0; i < readPool.max; ++i) {
    SReadWorker *pWorker = readPool.readWorker + i;
    if (pWorker->thread) {
      taosQsetThreadResume(readQset);
    }
  }

  for (int i = 0; i < readPool.max; ++i) {
    SReadWorker *pWorker = readPool.readWorker + i;
    if (pWorker->thread) {
      pthread_join(pWorker->thread, NULL);
    }
  }

  free(readPool.readWorker);
  taosCloseQset(readQset);
  pthread_mutex_destroy(&readPool.mutex);

  dInfo("dnode read is closed");
}

void dnodeDispatchToVnodeReadQueue(SRpcMsg *pMsg) {
  int32_t     queuedMsgNum = 0;
  int32_t     leftLen      = pMsg->contLen;
  char        *pCont       = (char *) pMsg->pCont;

  while (leftLen > 0) {
    SMsgHead *pHead = (SMsgHead *) pCont;
    pHead->vgId    = htonl(pHead->vgId);
    pHead->contLen = htonl(pHead->contLen);

    taos_queue queue = vnodeAcquireRqueue(pHead->vgId);

    if (queue == NULL) {
      leftLen -= pHead->contLen;
      pCont -= pHead->contLen;
      continue;
    }

    // put message into queue
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
        .code    = TSDB_CODE_VND_INVALID_VGROUP_ID,
        .msgType = 0
    };
    rpcSendResponse(&rpcRsp);
    rpcFreeCont(pMsg->pCont);
  }
}

void *dnodeAllocVReadQueue(void *pVnode) {
  pthread_mutex_lock(&readPool.mutex);
  taos_queue queue = taosOpenQueue();
  if (queue == NULL) {
    pthread_mutex_unlock(&readPool.mutex);
    return NULL;
  }

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
      dDebug("read worker:%d is launched, total:%d", pWorker->workerId, readPool.num);
    } while (readPool.num < readPool.min);
  }

  pthread_mutex_unlock(&readPool.mutex);
  dDebug("pVnode:%p, read queue:%p is allocated", pVnode, queue);

  return queue;
}

void dnodeFreeVReadQueue(void *rqueue) {
  taosCloseQueue(rqueue);

  // dynamically adjust the number of threads
}

void dnodeSendRpcReadRsp(void *pVnode, SReadMsg *pRead, int32_t code) {
  SRpcMsg rpcRsp = {
    .handle  = pRead->rpcMsg.handle,
    .pCont   = pRead->rspRet.rsp,
    .contLen = pRead->rspRet.len,
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pRead->rpcMsg.pCont);
  vnodeRelease(pVnode);
}

void dnodeDispatchNonRspMsg(void *pVnode, SReadMsg *pRead, int32_t code) {
  rpcFreeCont(pRead->rpcMsg.pCont);
  vnodeRelease(pVnode);
}

static void *dnodeProcessReadQueue(void *param) {
  SReadMsg    *pReadMsg;
  int          type;
  void        *pVnode;

  while (1) {
    if (taosReadQitemFromQset(readQset, &type, (void **)&pReadMsg, &pVnode) == 0) {
      dDebug("qset:%p dnode read got no message from qset, exiting", readQset);
      break;
    }

    dDebug("%p, msg:%s will be processed in vread queue, qtype:%d, msg:%p", pReadMsg->rpcMsg.ahandle,
           taosMsg[pReadMsg->rpcMsg.msgType], type, pReadMsg);

    int32_t code = vnodeProcessRead(pVnode, pReadMsg);

    if (type == TAOS_QTYPE_RPC && code != TSDB_CODE_QRY_NOT_READY) {
      dnodeSendRpcReadRsp(pVnode, pReadMsg, code);
    } else {
      if (code == TSDB_CODE_QRY_HAS_RSP) {
        dnodeSendRpcReadRsp(pVnode, pReadMsg, pReadMsg->rpcMsg.code);
      } else { // code == TSDB_CODE_QRY_NOT_READY, do not return msg to client
        assert(pReadMsg->rpcMsg.handle == NULL || (pReadMsg->rpcMsg.handle != NULL && pReadMsg->rpcMsg.msgType == 5));
        dnodeDispatchNonRspMsg(pVnode, pReadMsg, code);
      }
    }

    taosFreeQitem(pReadMsg);
  }

  return NULL;
}


UNUSED_FUNC
static void dnodeHandleIdleReadWorker(SReadWorker *pWorker) {
  int32_t num = taosGetQueueNumber(readQset);

  if (num == 0 || (num <= readPool.min && readPool.num > readPool.min)) {
    readPool.num--;
    dDebug("read worker:%d is released, total:%d", pWorker->workerId, readPool.num);
    pthread_exit(NULL);
  } else {
    usleep(30000);
    sched_yield();
  }
}

