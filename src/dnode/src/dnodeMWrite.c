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
#include "ttimer.h"
#include "tqueue.h"
#include "mnode.h"
#include "dnodeVMgmt.h"
#include "dnodeMInfos.h"
#include "dnodeMWrite.h"

typedef struct {
  pthread_t thread;
  int32_t   workerId;
} SMWriteWorker;

typedef struct {
  int32_t curNum;
  int32_t maxNum;
  SMWriteWorker *worker;
} SMWriteWorkerPool;

static SMWriteWorkerPool tsMWriteWP;
static taos_qset         tsMWriteQset;
static taos_queue        tsMWriteQueue;
extern void *            tsDnodeTmr;

static void *dnodeProcessMWriteQueue(void *param);

int32_t dnodeInitMWrite() {
  tsMWriteQset = taosOpenQset();

  tsMWriteWP.maxNum = 1;
  tsMWriteWP.curNum = 0;
  tsMWriteWP.worker = (SMWriteWorker *)calloc(sizeof(SMWriteWorker), tsMWriteWP.maxNum);

  if (tsMWriteWP.worker == NULL) return -1;
  for (int32_t i = 0; i < tsMWriteWP.maxNum; ++i) {
    SMWriteWorker *pWorker = tsMWriteWP.worker + i;
    pWorker->workerId = i;
    dDebug("dnode mwrite worker:%d is created", i);
  }

  dDebug("dnode mwrite is initialized, workers:%d qset:%p", tsMWriteWP.maxNum, tsMWriteQset);
  return 0;
}

void dnodeCleanupMWrite() {
  for (int32_t i = 0; i < tsMWriteWP.maxNum; ++i) {
    SMWriteWorker *pWorker = tsMWriteWP.worker + i;
    if (pWorker->thread) {
      taosQsetThreadResume(tsMWriteQset);
    }
    dDebug("dnode mwrite worker:%d is closed", i);
  }

  for (int32_t i = 0; i < tsMWriteWP.maxNum; ++i) {
    SMWriteWorker *pWorker = tsMWriteWP.worker + i;
    dDebug("dnode mwrite worker:%d start to join", i);
    if (pWorker->thread) {
      pthread_join(pWorker->thread, NULL);
    }
    dDebug("dnode mwrite worker:%d join success", i);
  }

  dDebug("dnode mwrite is closed, qset:%p", tsMWriteQset);

  taosCloseQset(tsMWriteQset);
  tsMWriteQset = NULL;
  tfree(tsMWriteWP.worker);
}

int32_t dnodeAllocMWritequeue() {
  tsMWriteQueue = taosOpenQueue();
  if (tsMWriteQueue == NULL) return TSDB_CODE_DND_OUT_OF_MEMORY;

  taosAddIntoQset(tsMWriteQset, tsMWriteQueue, NULL);

  for (int32_t i = tsMWriteWP.curNum; i < tsMWriteWP.maxNum; ++i) {
    SMWriteWorker *pWorker = tsMWriteWP.worker + i;
    pWorker->workerId = i;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, dnodeProcessMWriteQueue, pWorker) != 0) {
      dError("failed to create thread to process mwrite queue, reason:%s", strerror(errno));
    }

    pthread_attr_destroy(&thAttr);
    tsMWriteWP.curNum = i + 1;
    dDebug("dnode mwrite worker:%d is launched, total:%d", pWorker->workerId, tsMWriteWP.maxNum);
  }

  dDebug("dnode mwrite queue:%p is allocated", tsMWriteQueue);
  return TSDB_CODE_SUCCESS;
}

void dnodeFreeMWritequeue() {
  dDebug("dnode mwrite queue:%p is freed", tsMWriteQueue);
  taosCloseQueue(tsMWriteQueue);
  tsMWriteQueue = NULL;
}

void dnodeDispatchToMWriteQueue(SRpcMsg *pMsg) {
  if (!mnodeIsRunning() || tsMWriteQueue == NULL) {
    dnodeSendRedirectMsg(pMsg, true);
  } else {
    SMnodeMsg *pWrite = mnodeCreateMsg(pMsg);
    dTrace("msg:%p, app:%p type:%s is put into mwrite queue:%p", pWrite, pWrite->rpcMsg.ahandle,
           taosMsg[pWrite->rpcMsg.msgType], tsMWriteQueue);
    taosWriteQitem(tsMWriteQueue, TAOS_QTYPE_RPC, pWrite);
  }
}

static void dnodeFreeMWriteMsg(SMnodeMsg *pWrite) {
  dTrace("msg:%p, app:%p type:%s is freed from mwrite queue:%p", pWrite, pWrite->rpcMsg.ahandle,
         taosMsg[pWrite->rpcMsg.msgType], tsMWriteQueue);

  mnodeCleanupMsg(pWrite);
  taosFreeQitem(pWrite);
}

void dnodeSendRpcMWriteRsp(void *pMsg, int32_t code) {
  SMnodeMsg *pWrite = pMsg;
  if (pWrite == NULL) return;
  if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) return;
  if (code == TSDB_CODE_MND_ACTION_NEED_REPROCESSED) {
    dnodeReprocessMWriteMsg(pWrite);
    return;
  }

  SRpcMsg rpcRsp = {
    .handle  = pWrite->rpcMsg.handle,
    .pCont   = pWrite->rpcRsp.rsp,
    .contLen = pWrite->rpcRsp.len,
    .code    = code,
  };

  rpcSendResponse(&rpcRsp);
  dnodeFreeMWriteMsg(pWrite);
}

static void *dnodeProcessMWriteQueue(void *param) {
  SMnodeMsg *pWrite;
  int32_t    type;
  void *     unUsed;
  
  while (1) {
    if (taosReadQitemFromQset(tsMWriteQset, &type, (void **)&pWrite, &unUsed) == 0) {
      dDebug("qset:%p, mnode write got no message from qset, exiting", tsMWriteQset);
      break;
    }

    dTrace("msg:%p, app:%p type:%s will be processed in mwrite queue", pWrite, pWrite->rpcMsg.ahandle,
           taosMsg[pWrite->rpcMsg.msgType]);

    int32_t code = mnodeProcessWrite(pWrite);
    dnodeSendRpcMWriteRsp(pWrite, code);
  }

  return NULL;
}

void dnodeReprocessMWriteMsg(void *pMsg) {
  SMnodeMsg *pWrite = pMsg;

  if (!mnodeIsRunning() || tsMWriteQueue == NULL) {
    dDebug("msg:%p, app:%p type:%s is redirected for mnode not running, retry times:%d", pWrite, pWrite->rpcMsg.ahandle,
           taosMsg[pWrite->rpcMsg.msgType], pWrite->retry);

    dnodeSendRedirectMsg(pMsg, true);
    dnodeFreeMWriteMsg(pWrite);
  } else {
    dDebug("msg:%p, app:%p type:%s is reput into mwrite queue:%p, retry times:%d", pWrite, pWrite->rpcMsg.ahandle,
           taosMsg[pWrite->rpcMsg.msgType], tsMWriteQueue, pWrite->retry);

    taosWriteQitem(tsMWriteQueue, TAOS_QTYPE_RPC, pWrite);
  }
}

static void dnodeDoDelayReprocessMWriteMsg(void *param, void *tmrId) {
  dnodeReprocessMWriteMsg(param);
}

void dnodeDelayReprocessMWriteMsg(void *pMsg) {
  SMnodeMsg *mnodeMsg = pMsg;
  void *unUsed = NULL;
  taosTmrReset(dnodeDoDelayReprocessMWriteMsg, 300, mnodeMsg, tsDnodeTmr, &unUsed);
}
