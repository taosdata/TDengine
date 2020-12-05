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
#include "dnode.h"
#include "tsdb.h"
#include "vnodeCancel.h"

typedef struct {
  pthread_t thread;
  int32_t   workerId;
} SVCWorker;

typedef struct {
  int32_t    curNum;
  int32_t    maxNum;
  SVCWorker *worker;
} SVCWorkerPool;

static SVCWorkerPool tsVCWorkerPool;
static taos_qset     tsVCWorkerQset;
static taos_queue    tsVCWorkerQueue;

static void *vnodeCWorkerFunc(void *param);

static int32_t vnodeStartCWorker() {
  tsVCWorkerQueue = taosOpenQueue();
  if (tsVCWorkerQueue == NULL) return TSDB_CODE_DND_OUT_OF_MEMORY;

  taosAddIntoQset(tsVCWorkerQset, tsVCWorkerQueue, NULL);

  for (int32_t i = tsVCWorkerPool.curNum; i < tsVCWorkerPool.maxNum; ++i) {
    SVCWorker *pWorker = tsVCWorkerPool.worker + i;
    pWorker->workerId = i;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, vnodeCWorkerFunc, pWorker) != 0) {
      vError("failed to create thread to process vcworker queue, reason:%s", strerror(errno));
    }

    pthread_attr_destroy(&thAttr);

    tsVCWorkerPool.curNum = i + 1;
    vDebug("vcworker:%d is launched, total:%d", pWorker->workerId, tsVCWorkerPool.maxNum);
  }

  vDebug("vcworker queue:%p is allocated", tsVCWorkerQueue);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeInitCWorker() {
  tsVCWorkerQset = taosOpenQset();

  tsVCWorkerPool.maxNum = 1;
  tsVCWorkerPool.curNum = 0;
  tsVCWorkerPool.worker = calloc(sizeof(SVCWorker), tsVCWorkerPool.maxNum);

  if (tsVCWorkerPool.worker == NULL) return -1;
  for (int32_t i = 0; i < tsVCWorkerPool.maxNum; ++i) {
    SVCWorker *pWorker = tsVCWorkerPool.worker + i;
    pWorker->workerId = i;
    vDebug("vcworker:%d is created", i);
  }

  vDebug("vcworker is initialized, num:%d qset:%p", tsVCWorkerPool.maxNum, tsVCWorkerQset);

  return vnodeStartCWorker();
}

static void vnodeStopCWorker() {
  vDebug("vcworker queue:%p is freed", tsVCWorkerQueue);
  taosCloseQueue(tsVCWorkerQueue);
  tsVCWorkerQueue = NULL;
}

void vnodeCleanupCWorker() {
  for (int32_t i = 0; i < tsVCWorkerPool.maxNum; ++i) {
    SVCWorker *pWorker = tsVCWorkerPool.worker + i;
    if (pWorker->thread) {
      taosQsetThreadResume(tsVCWorkerQset);
    }
    vDebug("vcworker:%d is closed", i);
  }

  for (int32_t i = 0; i < tsVCWorkerPool.maxNum; ++i) {
    SVCWorker *pWorker = tsVCWorkerPool.worker + i;
    vDebug("vcworker:%d start to join", i);
    if (pWorker->thread) {
      pthread_join(pWorker->thread, NULL);
    }
    vDebug("vcworker:%d join success", i);
  }

  vDebug("vcworker is closed, qset:%p", tsVCWorkerQset);

  taosCloseQset(tsVCWorkerQset);
  tsVCWorkerQset = NULL;
  tfree(tsVCWorkerPool.worker);

  vnodeStopCWorker();
}

int32_t vnodeWriteIntoCQueue(SVReadMsg *pRead) {
  vTrace("msg:%p, write into vcqueue", pRead);
  return taosWriteQitem(tsVCWorkerQueue, pRead->qtype, pRead);
}

static void vnodeFreeFromCQueue(SVReadMsg *pRead) {
  vTrace("msg:%p, free from vcqueue", pRead);
  taosFreeQitem(pRead);
}

static void vnodeSendVCancelRpcRsp(SVReadMsg *pRead, int32_t code) {
  SRpcMsg rpcRsp = {
    .handle  = pRead->rpcHandle,
    .pCont   = pRead->rspRet.rsp,
    .contLen = pRead->rspRet.len,
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);
  vnodeFreeFromCQueue(pRead);
}

static void *vnodeCWorkerFunc(void *param) {
  int32_t    qtype;
  SVReadMsg *pRead;
  SVnodeObj *pVnode;

  while (1) {
    if (taosReadQitemFromQset(tsVCWorkerQset, &qtype, (void **)&pRead, (void **)&pVnode) == 0) {
      vDebug("qset:%p, vcworker got no message from qset, exiting", tsVCWorkerQset);
      break;
    }

    vTrace("msg:%p will be processed in vcworker queue", pRead);
    
    assert(qtype == TAOS_QTYPE_RPC);
    assert(pVnode == NULL);
    
    int32_t code = vnodeProcessRead(NULL, pRead);
    vnodeSendVCancelRpcRsp(pRead, code);
  }

  return NULL;
}
