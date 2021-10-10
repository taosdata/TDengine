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
#include "vnodeMain.h"
#include "vnodeWorker.h"

enum { CLEANUP_TASK = 0, DESTROY_TASK = 1, BACKUP_TASK = 2 };

typedef struct {
  int32_t vgId;
  int32_t code;
  int32_t type;
  void *  rpcHandle;
  SVnode *pVnode;
} SVnTask;

static struct {
  SWorkerPool pool;
  taos_queue  pQueue;
} tsVworker = {0};

static void vnodeProcessTaskStart(void *unused, SVnTask *pTask, int32_t qtype) {
  pTask->code = 0;

  switch (pTask->type) {
    case CLEANUP_TASK:
      vnodeCleanUp(pTask->pVnode);
      break;
    case DESTROY_TASK:
      vnodeDestroy(pTask->pVnode);
      break;
    case BACKUP_TASK:
      vnodeBackup(pTask->vgId);
      break;
    default:
      break;
  }
}

static void vnodeProcessTaskEnd(void *unused, SVnTask *pTask, int32_t qtype, int32_t code) {
  if (pTask->rpcHandle != NULL) {
    SRpcMsg rpcRsp = {.handle = pTask->rpcHandle, .code = pTask->code};
    rpcSendResponse(&rpcRsp);
  }

  taosFreeQitem(pTask);
}

static int32_t vnodeWriteIntoTaskQueue(SVnode *pVnode, int32_t type, void *rpcHandle) {
  SVnTask *pTask = taosAllocateQitem(sizeof(SVnTask));
  if (pTask == NULL) return TSDB_CODE_VND_OUT_OF_MEMORY;

  pTask->vgId = pVnode->vgId;
  pTask->pVnode = pVnode;
  pTask->rpcHandle = rpcHandle;
  pTask->type = type;


  return taosWriteQitem(tsVworker.pQueue, TAOS_QTYPE_RPC, pTask);
}

void vnodeProcessCleanupTask(SVnode *pVnode) {
  vnodeWriteIntoTaskQueue(pVnode, CLEANUP_TASK, NULL);
}

void vnodeProcessDestroyTask(SVnode *pVnode) {
  vnodeWriteIntoTaskQueue(pVnode, DESTROY_TASK, NULL);
}

void vnodeProcessBackupTask(SVnode *pVnode) {
  vnodeWriteIntoTaskQueue(pVnode, BACKUP_TASK, NULL);
}

int32_t vnodeInitWorker() {
  SWorkerPool *pPool = &tsVworker.pool;
  pPool->name = "vworker";
  pPool->startFp = (ProcessStartFp)vnodeProcessTaskStart;
  pPool->endFp = (ProcessEndFp)vnodeProcessTaskEnd;
  pPool->min = 0;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  tsVworker.pQueue = tWorkerAllocQueue(pPool, NULL);

  vInfo("vworker is initialized, max worker %d", pPool->max);
  return TSDB_CODE_SUCCESS;
}

void vnodeCleanupWorker() {
  tWorkerFreeQueue(&tsVworker.pool, tsVworker.pQueue);
  tWorkerCleanup(&tsVworker.pool);
  tsVworker.pQueue = NULL;
  vInfo("vworker is closed");
}
