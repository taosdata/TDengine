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
#include "ttimer.h"
#include "tqueue.h"
#include "twal.h"
#include "tglobal.h"
#include "mnode.h"
#include "dnode.h"
#include "dnodeInt.h"
#include "dnodeMgmt.h"
#include "dnodeMWrite.h"

typedef struct {
  pthread_t thread;
  int32_t   workerId;
} SMWriteWorker;

typedef struct {
  int32_t        num;
  SMWriteWorker *writeWorker;
} SMWriteWorkerPool;

static SMWriteWorkerPool tsMWritePool;
static taos_qset         tsMWriteQset;
static taos_queue        tsMWriteQueue;
extern void *            tsDnodeTmr;

static void *dnodeProcessMnodeWriteQueue(void *param);

int32_t dnodeInitMnodeWrite() {
  tsMWriteQset = taosOpenQset();
  
  tsMWritePool.num = 1;
  tsMWritePool.writeWorker = (SMWriteWorker *)calloc(sizeof(SMWriteWorker), tsMWritePool.num);

  if (tsMWritePool.writeWorker == NULL) return -1;
  for (int32_t i = 0; i < tsMWritePool.num; ++i) {
    SMWriteWorker *pWorker = tsMWritePool.writeWorker + i;
    pWorker->workerId = i;
  }

  dInfo("dnode mwrite is opened");
  return 0;
}

void dnodeCleanupMnodeWrite() {
  for (int32_t i = 0; i < tsMWritePool.num; ++i) {
    SMWriteWorker *pWorker = tsMWritePool.writeWorker + i;
    if (pWorker->thread) {
      taosQsetThreadResume(tsMWriteQset);
    }
  }

  for (int32_t i = 0; i < tsMWritePool.num; ++i) {
    SMWriteWorker *pWorker = tsMWritePool.writeWorker + i;
    if (pWorker->thread) {
      pthread_join(pWorker->thread, NULL);
    }
  }

  taosCloseQset(tsMWriteQset);
  taosTFree(tsMWritePool.writeWorker);
  dInfo("dnode mwrite is closed");
}

int32_t dnodeAllocateMnodeWqueue() {
  tsMWriteQueue = taosOpenQueue();
  if (tsMWriteQueue == NULL) return TSDB_CODE_DND_OUT_OF_MEMORY;

  taosAddIntoQset(tsMWriteQset, tsMWriteQueue, NULL);

  for (int32_t i = 0; i < tsMWritePool.num; ++i) {
    SMWriteWorker *pWorker = tsMWritePool.writeWorker + i;
    pWorker->workerId = i;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, dnodeProcessMnodeWriteQueue, pWorker) != 0) {
      dError("failed to create thread to process mwrite queue, reason:%s", strerror(errno));
    }

    pthread_attr_destroy(&thAttr);
    dDebug("dnode mwrite worker:%d is launched, total:%d", pWorker->workerId, tsMWritePool.num);
  }

  dDebug("dnode mwrite queue:%p is allocated", tsMWriteQueue);
  return TSDB_CODE_SUCCESS;
}

void dnodeFreeMnodeWqueue() {
  taosCloseQueue(tsMWriteQueue);
  tsMWriteQueue = NULL;
}

void dnodeDispatchToMnodeWriteQueue(SRpcMsg *pMsg) {
  if (!mnodeIsRunning() || tsMWriteQueue == NULL) {
    dnodeSendRedirectMsg(pMsg, true);
    rpcFreeCont(pMsg->pCont);
    return;
  }

  SMnodeMsg *pWrite = (SMnodeMsg *)taosAllocateQitem(sizeof(SMnodeMsg));
  mnodeCreateMsg(pWrite, pMsg);

  dDebug("app:%p:%p, msg:%s is put into mwrite queue", pWrite->rpcMsg.ahandle, pWrite, taosMsg[pWrite->rpcMsg.msgType]);
  taosWriteQitem(tsMWriteQueue, TAOS_QTYPE_RPC, pWrite);
}

static void dnodeFreeMnodeWriteMsg(SMnodeMsg *pWrite) {
  mnodeCleanupMsg(pWrite);
  taosFreeQitem(pWrite);
}

void dnodeSendRpcMnodeWriteRsp(void *pMsg, int32_t code) {
  SMnodeMsg *pWrite = pMsg;
  if (pWrite == NULL) return;
  if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) return;
  if (code == TSDB_CODE_MND_ACTION_NEED_REPROCESSED) {
    dnodeReprocessMnodeWriteMsg(pWrite);
    return;
  }

  SRpcMsg rpcRsp = {
    .handle  = pWrite->rpcMsg.handle,
    .pCont   = pWrite->rpcRsp.rsp,
    .contLen = pWrite->rpcRsp.len,
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);
  dnodeFreeMnodeWriteMsg(pWrite);
}

static void *dnodeProcessMnodeWriteQueue(void *param) {
  SMnodeMsg *pWrite;
  int32_t    type;
  void *     unUsed;
  
  while (1) {
    if (taosReadQitemFromQset(tsMWriteQset, &type, (void **)&pWrite, &unUsed) == 0) {
      dDebug("dnodeProcessMnodeWriteQueue: got no message from qset, exiting...");
      break;
    }

    dDebug("app:%p:%p, msg:%s will be processed in mwrite queue", pWrite->rpcMsg.ahandle, pWrite,
           taosMsg[pWrite->rpcMsg.msgType]);

    int32_t code = mnodeProcessWrite(pWrite);
    dnodeSendRpcMnodeWriteRsp(pWrite, code);
  }

  return NULL;
}

void dnodeReprocessMnodeWriteMsg(void *pMsg) {
  SMnodeMsg *pWrite = pMsg;

  if (!mnodeIsRunning() || tsMWriteQueue == NULL) {
    dDebug("app:%p:%p, msg:%s is redirected for mnode not running, retry times:%d", pWrite->rpcMsg.ahandle, pWrite,
           taosMsg[pWrite->rpcMsg.msgType], pWrite->retry);

    dnodeSendRedirectMsg(pMsg, true);
    dnodeFreeMnodeWriteMsg(pWrite);
  } else {
    dDebug("app:%p:%p, msg:%s is reput into mwrite queue, retry times:%d", pWrite->rpcMsg.ahandle, pWrite,
           taosMsg[pWrite->rpcMsg.msgType], pWrite->retry);

    taosWriteQitem(tsMWriteQueue, TAOS_QTYPE_RPC, pWrite);
  }
}

static void dnodeDoDelayReprocessMnodeWriteMsg(void *param, void *tmrId) {
  dnodeReprocessMnodeWriteMsg(param);
}

void dnodeDelayReprocessMnodeWriteMsg(void *pMsg) {
  SMnodeMsg *mnodeMsg = pMsg;
  void *unUsed = NULL;
  taosTmrReset(dnodeDoDelayReprocessMnodeWriteMsg, 300, mnodeMsg, tsDnodeTmr, &unUsed);
}
