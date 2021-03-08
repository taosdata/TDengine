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
#include "tglobal.h"
#include "vnodeWorker.h"
#include "vnodeMain.h"

typedef enum {
  VNODE_WORKER_ACTION_CLEANUP,
  VNODE_WORKER_ACTION_DESTROUY
} EVMWorkerAction;

typedef struct {
  int32_t vgId;
  int32_t code;
  void *  rpcHandle;
  SVnodeObj *pVnode;
  EVMWorkerAction action;
} SVMWorkerMsg;

typedef struct {
  pthread_t thread;
  int32_t   workerId;
} SVMWorker;

typedef struct {
  int32_t    curNum;
  int32_t    maxNum;
  SVMWorker *worker;
} SVMWorkerPool;

static SVMWorkerPool tsVMWorkerPool;
static taos_qset     tsVMWorkerQset;
static taos_queue    tsVMWorkerQueue;

static void *vnodeMWorkerFunc(void *param);

static int32_t vnodeStartMWorker() {
  tsVMWorkerQueue = taosOpenQueue();
  if (tsVMWorkerQueue == NULL) return TSDB_CODE_DND_OUT_OF_MEMORY;

  taosAddIntoQset(tsVMWorkerQset, tsVMWorkerQueue, NULL);

  for (int32_t i = tsVMWorkerPool.curNum; i < tsVMWorkerPool.maxNum; ++i) {
    SVMWorker *pWorker = tsVMWorkerPool.worker + i;
    pWorker->workerId = i;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, vnodeMWorkerFunc, pWorker) != 0) {
      vError("failed to create thread to process vmworker queue, reason:%s", strerror(errno));
    }

    pthread_attr_destroy(&thAttr);

    tsVMWorkerPool.curNum = i + 1;
    vDebug("vmworker:%d is launched, total:%d", pWorker->workerId, tsVMWorkerPool.maxNum);
  }

  vDebug("vmworker queue:%p is allocated", tsVMWorkerQueue);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeInitMWorker() {
  tsVMWorkerQset = taosOpenQset();

  tsVMWorkerPool.maxNum = 1;
  tsVMWorkerPool.curNum = 0;
  tsVMWorkerPool.worker = calloc(sizeof(SVMWorker), tsVMWorkerPool.maxNum);

  if (tsVMWorkerPool.worker == NULL) return -1;
  for (int32_t i = 0; i < tsVMWorkerPool.maxNum; ++i) {
    SVMWorker *pWorker = tsVMWorkerPool.worker + i;
    pWorker->workerId = i;
    vDebug("vmworker:%d is created", i);
  }

  vDebug("vmworker is initialized, num:%d qset:%p", tsVMWorkerPool.maxNum, tsVMWorkerQset);

  return vnodeStartMWorker();
}

static void vnodeStopMWorker() {
  vDebug("vmworker queue:%p is freed", tsVMWorkerQueue);
  taosCloseQueue(tsVMWorkerQueue);
  tsVMWorkerQueue = NULL;
}

void vnodeCleanupMWorker() {
  for (int32_t i = 0; i < tsVMWorkerPool.maxNum; ++i) {
    SVMWorker *pWorker = tsVMWorkerPool.worker + i;
    if (taosCheckPthreadValid(pWorker->thread)) {
      taosQsetThreadResume(tsVMWorkerQset);
    }
    vDebug("vmworker:%d is closed", i);
  }

  for (int32_t i = 0; i < tsVMWorkerPool.maxNum; ++i) {
    SVMWorker *pWorker = tsVMWorkerPool.worker + i;
    vDebug("vmworker:%d start to join", i);
    if (taosCheckPthreadValid(pWorker->thread)) {
      pthread_join(pWorker->thread, NULL);
    }
    vDebug("vmworker:%d join success", i);
  }

  vDebug("vmworker is closed, qset:%p", tsVMWorkerQset);

  taosCloseQset(tsVMWorkerQset);
  tsVMWorkerQset = NULL;
  tfree(tsVMWorkerPool.worker);

  vnodeStopMWorker();
}

static int32_t vnodeWriteIntoMWorker(SVnodeObj *pVnode, EVMWorkerAction action, void *rpcHandle) {
  SVMWorkerMsg *pMsg = taosAllocateQitem(sizeof(SVMWorkerMsg));
  if (pMsg == NULL) return TSDB_CODE_VND_OUT_OF_MEMORY;

  pMsg->vgId = pVnode->vgId;
  pMsg->pVnode = pVnode;
  pMsg->rpcHandle = rpcHandle;
  pMsg->action = action;

  int32_t code = taosWriteQitem(tsVMWorkerQueue, TAOS_QTYPE_RPC, pMsg);
  if (code == 0) code = TSDB_CODE_DND_ACTION_IN_PROGRESS;

  return code;
}

int32_t vnodeCleanupInMWorker(SVnodeObj *pVnode) {
  vTrace("vgId:%d, will cleanup in vmworker", pVnode->vgId);
  return vnodeWriteIntoMWorker(pVnode, VNODE_WORKER_ACTION_CLEANUP, NULL);
}

int32_t vnodeDestroyInMWorker(SVnodeObj *pVnode) {
  vTrace("vgId:%d, will destroy in vmworker", pVnode->vgId);
  return vnodeWriteIntoMWorker(pVnode, VNODE_WORKER_ACTION_DESTROUY, NULL);
}

static void vnodeFreeMWorkerMsg(SVMWorkerMsg *pMsg) {
  vTrace("vgId:%d, disposed in vmworker", pMsg->vgId);
  taosFreeQitem(pMsg);
}

static void vnodeSendVMWorkerRpcRsp(SVMWorkerMsg *pMsg) {
  if (pMsg->rpcHandle != NULL) {
    SRpcMsg rpcRsp = {.handle = pMsg->rpcHandle, .code = pMsg->code};
    rpcSendResponse(&rpcRsp);
  }

  vnodeFreeMWorkerMsg(pMsg);
}

static void vnodeProcessMWorkerMsg(SVMWorkerMsg *pMsg) {
  pMsg->code = 0;

  switch (pMsg->action) {
    case VNODE_WORKER_ACTION_CLEANUP:
      vnodeCleanUp(pMsg->pVnode);
      break;
    case VNODE_WORKER_ACTION_DESTROUY:
      vnodeDestroy(pMsg->pVnode);
      break;
    default:
      break;
  }
}

static void *vnodeMWorkerFunc(void *param) {
  while (1) {
    SVMWorkerMsg *pMsg = NULL;
    if (taosReadQitemFromQset(tsVMWorkerQset, NULL, (void **)&pMsg, NULL) == 0) {
      vDebug("qset:%p, vmworker got no message from qset, exiting", tsVMWorkerQset);
      break;
    }

    vTrace("vgId:%d, action:%d will be processed in vmworker queue", pMsg->vgId, pMsg->action);
    vnodeProcessMWorkerMsg(pMsg);
    vnodeSendVMWorkerRpcRsp(pMsg);
  }

  return NULL;
}
