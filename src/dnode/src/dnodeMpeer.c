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
#include "trpc.h"
#include "twal.h"
#include "tglobal.h"
#include "mnode.h"
#include "dnode.h"
#include "dnodeInt.h"
#include "dnodeVMgmt.h"
#include "dnodeMWrite.h"

typedef struct {
  pthread_t thread;
  int32_t   workerId;
} SMMgmtWorker;

typedef struct {
  int32_t       num;
  SMMgmtWorker *mgmtWorker;
} SMMgmtWorkerPool;

static SMMgmtWorkerPool tsMMgmtPool;
static taos_qset        tsMMgmtQset;
static taos_queue       tsMMgmtQueue;

static void *dnodeProcessMnodeMgmtQueue(void *param);

int32_t dnodeInitMnodeMgmt() {
  tsMMgmtQset = taosOpenQset();
  
  tsMMgmtPool.num = 1;
  tsMMgmtPool.mgmtWorker = (SMMgmtWorker *)calloc(sizeof(SMMgmtWorker), tsMMgmtPool.num);

  if (tsMMgmtPool.mgmtWorker == NULL) return -1;
  for (int32_t i = 0; i < tsMMgmtPool.num; ++i) {
    SMMgmtWorker *pWorker = tsMMgmtPool.mgmtWorker + i;
    pWorker->workerId = i;
  }

  dPrint("dnode mmgmt is opened");
  return 0;
}

void dnodeCleanupMnodeMgmt() {
  for (int32_t i = 0; i < tsMMgmtPool.num; ++i) {
    SMMgmtWorker *pWorker = tsMMgmtPool.mgmtWorker + i;
    if (pWorker->thread) {
      taosQsetThreadResume(tsMMgmtQset);
    }
  }

  for (int32_t i = 0; i < tsMMgmtPool.num; ++i) {
    SMMgmtWorker *pWorker = tsMMgmtPool.mgmtWorker + i;
    if (pWorker->thread) {
      pthread_join(pWorker->thread, NULL);
    }
  }

  dPrint("dnode mmgmt is closed");
}

int32_t dnodeAllocateMnodeMqueue() {
  tsMMgmtQueue = taosOpenQueue();
  if (tsMMgmtQueue == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

  taosAddIntoQset(tsMMgmtQset, tsMMgmtQueue, NULL);

  for (int32_t i = 0; i < tsMMgmtPool.num; ++i) {
    SMMgmtWorker *pWorker = tsMMgmtPool.mgmtWorker + i;
    pWorker->workerId = i;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, dnodeProcessMnodeMgmtQueue, pWorker) != 0) {
      dError("failed to create thread to process mmgmt queue, reason:%s", strerror(errno));
    }

    pthread_attr_destroy(&thAttr);
    dTrace("dnode mmgmt worker:%d is launched, total:%d", pWorker->workerId, tsMMgmtPool.num);
  }

  dTrace("dnode mmgmt queue:%p is allocated", tsMMgmtQueue);
  return TSDB_CODE_SUCCESS;
}

void dnodeFreeMnodeRqueue() {
  taosCloseQueue(tsMMgmtQueue);
  tsMMgmtQueue = NULL;
}

void dnodeDispatchToMnodeMgmtQueue(SRpcMsg *pMsg) {
  if (!mnodeIsRunning() || tsMMgmtQueue == NULL) {
    dnodeSendRediretMsg(pMsg);
    return;
  }

  SMnodeMsg *pMgmt = (SMnodeMsg *)taosAllocateQitem(sizeof(SMnodeMsg));
  pMgmt->rpcMsg = *pMsg;
  taosWriteQitem(tsMMgmtQueue, TAOS_QTYPE_RPC, pMgmt);
}

static void dnodeSendRpcMnodeMgmtRsp(SMnodeMsg *pMgmt, int32_t code) {
  if (code == TSDB_CODE_ACTION_IN_PROGRESS) return;

  SRpcMsg rpcRsp = {
    .handle  = pMgmt->rpcMsg.handle,
    .pCont   = pMgmt->rspRet.rsp,
    .contLen = pMgmt->rspRet.len,
    .code    = pMgmt->rspRet.code,
  };

  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pMgmt->rpcMsg.pCont);
}

static void *dnodeProcessMnodeMgmtQueue(void *param) {
  SMnodeMsg *pMgmtMsg;
  int32_t    type;
  void *     unUsed;
  
  while (1) {
    if (taosReadQitemFromQset(tsMMgmtQset, &type, (void **)&pMgmtMsg, &unUsed) == 0) {
      dTrace("dnodeProcessMnodeMgmtQueue: got no message from qset, exiting...");
      break;
    }

    dTrace("%p, msg:%s will be processed", pMgmtMsg->rpcMsg.ahandle, taosMsg[pMgmtMsg->rpcMsg.msgType]);    
    int32_t code = mnodeProcessMgmt(pMgmtMsg);    
    dnodeSendRpcMnodeMgmtRsp(pMgmtMsg, code);    
    taosFreeQitem(pMgmtMsg);
  }

  return NULL;
}
