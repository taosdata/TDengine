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
#include "tglobal.h"
#include "tqueue.h"
#include "vnode.h"
#include "dnodeInt.h"

typedef struct {
  pthread_t thread;    // thread
  int32_t   workerId;  // worker ID
} SVReadWorker;

typedef struct {
  int32_t max;  // max number of workers
  int32_t min;  // min number of workers
  int32_t num;  // current number of workers
  SVReadWorker *  worker;
  pthread_mutex_t mutex;
} SVReadWorkerPool;

static void *dnodeProcessReadQueue(void *pWorker);

// module global variable
static SVReadWorkerPool tsVReadWP;
static taos_qset        tsVReadQset;

int32_t dnodeInitVRead() {
  tsVReadQset = taosOpenQset();

  tsVReadWP.min = tsNumOfCores;
  tsVReadWP.max = tsNumOfCores * tsNumOfThreadsPerCore;
  if (tsVReadWP.max <= tsVReadWP.min * 2) tsVReadWP.max = 2 * tsVReadWP.min;
  tsVReadWP.worker = calloc(sizeof(SVReadWorker), tsVReadWP.max);
  pthread_mutex_init(&tsVReadWP.mutex, NULL);

  if (tsVReadWP.worker == NULL) return -1;
  for (int i = 0; i < tsVReadWP.max; ++i) {
    SVReadWorker *pWorker = tsVReadWP.worker + i;
    pWorker->workerId = i;
  }

  dInfo("dnode vread is initialized, min worker:%d max worker:%d", tsVReadWP.min, tsVReadWP.max);
  return 0;
}

void dnodeCleanupVRead() {
  for (int i = 0; i < tsVReadWP.max; ++i) {
    SVReadWorker *pWorker = tsVReadWP.worker + i;
    if (pWorker->thread) {
      taosQsetThreadResume(tsVReadQset);
    }
  }

  for (int i = 0; i < tsVReadWP.max; ++i) {
    SVReadWorker *pWorker = tsVReadWP.worker + i;
    if (pWorker->thread) {
      pthread_join(pWorker->thread, NULL);
    }
  }

  free(tsVReadWP.worker);
  taosCloseQset(tsVReadQset);
  pthread_mutex_destroy(&tsVReadWP.mutex);

  dInfo("dnode vread is closed");
}

void dnodeDispatchToVReadQueue(SRpcMsg *pMsg) {
  int32_t queuedMsgNum = 0;
  int32_t leftLen = pMsg->contLen;
  char *  pCont = pMsg->pCont;

  while (leftLen > 0) {
    SMsgHead *pHead = (SMsgHead *)pCont;
    pHead->vgId = htonl(pHead->vgId);
    pHead->contLen = htonl(pHead->contLen);

    void *pVnode = vnodeAcquire(pHead->vgId);
    if (pVnode != NULL) {
      int32_t code = vnodeWriteToRQueue(pVnode, pCont, pHead->contLen, TAOS_QTYPE_RPC, pMsg);
      if (code == TSDB_CODE_SUCCESS) queuedMsgNum++;
      vnodeRelease(pVnode);
    }

    leftLen -= pHead->contLen;
    pCont -= pHead->contLen;
  }

  if (queuedMsgNum == 0) {
    SRpcMsg rpcRsp = {.handle = pMsg->handle, .code = TSDB_CODE_VND_INVALID_VGROUP_ID};
    rpcSendResponse(&rpcRsp);
  }

  rpcFreeCont(pMsg->pCont);
}

void *dnodeAllocVReadQueue(void *pVnode) {
  pthread_mutex_lock(&tsVReadWP.mutex);
  taos_queue queue = taosOpenQueue();
  if (queue == NULL) {
    pthread_mutex_unlock(&tsVReadWP.mutex);
    return NULL;
  }

  taosAddIntoQset(tsVReadQset, queue, pVnode);

  // spawn a thread to process queue
  if (tsVReadWP.num < tsVReadWP.max) {
    do {
      SVReadWorker *pWorker = tsVReadWP.worker + tsVReadWP.num;

      pthread_attr_t thAttr;
      pthread_attr_init(&thAttr);
      pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

      if (pthread_create(&pWorker->thread, &thAttr, dnodeProcessReadQueue, pWorker) != 0) {
        dError("failed to create thread to process vread vqueue since %s", strerror(errno));
      }

      pthread_attr_destroy(&thAttr);
      tsVReadWP.num++;
      dDebug("dnode vread worker:%d is launched, total:%d", pWorker->workerId, tsVReadWP.num);
    } while (tsVReadWP.num < tsVReadWP.min);
  }

  pthread_mutex_unlock(&tsVReadWP.mutex);
  dDebug("pVnode:%p, dnode vread queue:%p is allocated", pVnode, queue);

  return queue;
}

void dnodeFreeVReadQueue(void *pRqueue) {
  taosCloseQueue(pRqueue);
}

void dnodeSendRpcVReadRsp(void *pVnode, SVReadMsg *pRead, int32_t code) {
  SRpcMsg rpcRsp = {
    .handle  = pRead->rpcHandle,
    .pCont   = pRead->rspRet.rsp,
    .contLen = pRead->rspRet.len,
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);
}

void dnodeDispatchNonRspMsg(void *pVnode, SVReadMsg *pRead, int32_t code) {
}

static void *dnodeProcessReadQueue(void *pWorker) {
  SVReadMsg *pRead;
  int32_t    qtype;
  void *     pVnode;

  while (1) {
    if (taosReadQitemFromQset(tsVReadQset, &qtype, (void **)&pRead, &pVnode) == 0) {
      dDebug("qset:%p dnode vread got no message from qset, exiting", tsVReadQset);
      break;
    }

    dTrace("msg:%p, app:%p type:%s will be processed in vread queue, qtype:%d", pRead, pRead->rpcAhandle,
           taosMsg[pRead->msgType], qtype);

    int32_t code = vnodeProcessRead(pVnode, pRead);

    if (qtype == TAOS_QTYPE_RPC && code != TSDB_CODE_QRY_NOT_READY) {
      dnodeSendRpcVReadRsp(pVnode, pRead, code);
    } else {
      if (code == TSDB_CODE_QRY_HAS_RSP) {
        dnodeSendRpcVReadRsp(pVnode, pRead, pRead->code);
      } else {  // code == TSDB_CODE_QRY_NOT_READY, do not return msg to client
        assert(pRead->rpcHandle == NULL || (pRead->rpcHandle != NULL && pRead->msgType == 5));
        dnodeDispatchNonRspMsg(pVnode, pRead, code);
      }
    }

    vnodeFreeFromRQueue(pVnode, pRead);
  }

  return NULL;
}
