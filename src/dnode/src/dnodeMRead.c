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
#include "mnode.h"
#include "dnode.h"
#include "dnodeInt.h"
#include "dnodeMgmt.h"
#include "dnodeMRead.h"

typedef struct {
  pthread_t thread;
  int32_t   workerId;
} SMReadWorker;

typedef struct {
  int32_t curNum;
  int32_t maxNum;
  SMReadWorker *readWorker;
} SMReadWorkerPool;

static SMReadWorkerPool tsMReadPool;
static taos_qset        tsMReadQset;
static taos_queue       tsMReadQueue;

static void *dnodeProcessMnodeReadQueue(void *param);

int32_t dnodeInitMnodeRead() {
  tsMReadQset = taosOpenQset();

  tsMReadPool.maxNum = tsNumOfCores * tsNumOfThreadsPerCore / 2;
  tsMReadPool.maxNum = MAX(2, tsMReadPool.maxNum);
  tsMReadPool.maxNum = MIN(4, tsMReadPool.maxNum);
  tsMReadPool.curNum = 0;
  tsMReadPool.readWorker = (SMReadWorker *)calloc(sizeof(SMReadWorker), tsMReadPool.maxNum);

  if (tsMReadPool.readWorker == NULL) return -1;
  for (int32_t i = 0; i < tsMReadPool.maxNum; ++i) {
    SMReadWorker *pWorker = tsMReadPool.readWorker + i;
    pWorker->workerId = i;
    dDebug("dnode mread worker:%d is created", i);
  }

  dDebug("dnode mread is initialized, workers:%d qset:%p", tsMReadPool.maxNum, tsMReadQset);
  return 0;
}

void dnodeCleanupMnodeRead() {
  for (int32_t i = 0; i < tsMReadPool.maxNum; ++i) {
    SMReadWorker *pWorker = tsMReadPool.readWorker + i;
    if (pWorker->thread) {
      taosQsetThreadResume(tsMReadQset);
    }
    dDebug("dnode mread worker:%d is closed", i);
  }

  for (int32_t i = 0; i < tsMReadPool.maxNum; ++i) {
    SMReadWorker *pWorker = tsMReadPool.readWorker + i;
    dDebug("dnode mread worker:%d start to join", i);
    if (pWorker->thread) {
      pthread_join(pWorker->thread, NULL);
    }
    dDebug("dnode mread worker:%d start to join", i);
  }

  dDebug("dnode mread is closed, qset:%p", tsMReadQset);

  taosCloseQset(tsMReadQset);
  tsMReadQset = NULL;
  free(tsMReadPool.readWorker);
}

int32_t dnodeAllocateMnodeRqueue() {
  tsMReadQueue = taosOpenQueue();
  if (tsMReadQueue == NULL) return TSDB_CODE_DND_OUT_OF_MEMORY;

  taosAddIntoQset(tsMReadQset, tsMReadQueue, NULL);

  for (int32_t i = tsMReadPool.curNum; i < tsMReadPool.maxNum; ++i) {
    SMReadWorker *pWorker = tsMReadPool.readWorker + i;
    pWorker->workerId = i;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, dnodeProcessMnodeReadQueue, pWorker) != 0) {
      dError("failed to create thread to process mread queue, reason:%s", strerror(errno));
    }

    pthread_attr_destroy(&thAttr);
    tsMReadPool.curNum = i + 1;
    dDebug("dnode mread worker:%d is launched, total:%d", pWorker->workerId, tsMReadPool.maxNum);
  }

  dDebug("dnode mread queue:%p is allocated", tsMReadQueue);
  return TSDB_CODE_SUCCESS;
}

void dnodeFreeMnodeRqueue() {
  dDebug("dnode mread queue:%p is freed", tsMReadQueue);
  taosCloseQueue(tsMReadQueue);
  tsMReadQueue = NULL;
}

void dnodeDispatchToMnodeReadQueue(SRpcMsg *pMsg) {
  if (!mnodeIsRunning() || tsMReadQueue == NULL) {
    dnodeSendRedirectMsg(pMsg, true);
    rpcFreeCont(pMsg->pCont);
    return;
  }

  SMnodeMsg *pRead = (SMnodeMsg *)taosAllocateQitem(sizeof(SMnodeMsg));
  mnodeCreateMsg(pRead, pMsg);
  taosWriteQitem(tsMReadQueue, TAOS_QTYPE_RPC, pRead);
}

static void dnodeFreeMnodeReadMsg(SMnodeMsg *pRead) {
  mnodeCleanupMsg(pRead);
  taosFreeQitem(pRead);
}

static void dnodeSendRpcMnodeReadRsp(SMnodeMsg *pRead, int32_t code) {
  if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) return;
  if (code == TSDB_CODE_MND_ACTION_NEED_REPROCESSED) {
    // may be a auto create req, should put into write queue
    dnodeReprocessMnodeWriteMsg(pRead);
    return;
  }

  SRpcMsg rpcRsp = {
    .handle  = pRead->rpcMsg.handle,
    .pCont   = pRead->rpcRsp.rsp,
    .contLen = pRead->rpcRsp.len,
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);
  dnodeFreeMnodeReadMsg(pRead);
}

static void *dnodeProcessMnodeReadQueue(void *param) {
  SMnodeMsg *pReadMsg;
  int32_t    type;
  void *     unUsed;
  
  while (1) {
    if (taosReadQitemFromQset(tsMReadQset, &type, (void **)&pReadMsg, &unUsed) == 0) {
      dDebug("qset:%p, mnode read got no message from qset, exiting", tsMReadQset);
      break;
    }

    dDebug("%p, msg:%s will be processed in mread queue", pReadMsg->rpcMsg.ahandle, taosMsg[pReadMsg->rpcMsg.msgType]);    
    int32_t code = mnodeProcessRead(pReadMsg);    
    dnodeSendRpcMnodeReadRsp(pReadMsg, code);   
  }

  return NULL;
}
