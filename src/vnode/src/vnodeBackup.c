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
#include "tfs.h"
#include "vnodeBackup.h"
#include "vnodeMain.h"

typedef struct {
  int32_t vgId;
} SVBackupMsg;

typedef struct {
  pthread_t thread;
  int32_t   workerId;
} SVBackupWorker;

typedef struct {
  int32_t    num;
  SVBackupWorker *worker;
} SVBackupWorkerPool;

static SVBackupWorkerPool tsVBackupPool;
static taos_qset     tsVBackupQset;
static taos_queue    tsVBackupQueue;

static void vnodeProcessBackupMsg(SVBackupMsg *pMsg) {
  int32_t vgId = pMsg->vgId;
  char newDir[TSDB_FILENAME_LEN] = {0};
  char stagingDir[TSDB_FILENAME_LEN] = {0};

  sprintf(newDir, "%s/vnode%d", "vnode_bak", vgId);
  sprintf(stagingDir, "%s/.staging/vnode%d", "vnode_bak", vgId);

  if (tsEnableVnodeBak) {
    tfsRmdir(newDir);
    tfsRename(stagingDir, newDir);
  } else {
    vInfo("vgId:%d, vnode backup not enabled", vgId);

    tfsRmdir(stagingDir);
  }
}

static void *vnodeBackupFunc(void *param) {
  while (1) {
    SVBackupMsg *pMsg = NULL;
    if (taosReadQitemFromQset(tsVBackupQset, NULL, (void **)&pMsg, NULL) == 0) {
      vDebug("qset:%p, vbackup got no message from qset, exiting", tsVBackupQset);
      break;
    }

    vTrace("vgId:%d, will be processed in vbackup queue", pMsg->vgId);
    vnodeProcessBackupMsg(pMsg);

    vTrace("vgId:%d, disposed in vbackup worker", pMsg->vgId);
    taosFreeQitem(pMsg);
  }

  return NULL;
}

static int32_t vnodeStartBackup() {
  tsVBackupQueue = taosOpenQueue();
  if (tsVBackupQueue == NULL) return TSDB_CODE_DND_OUT_OF_MEMORY;

  taosAddIntoQset(tsVBackupQset, tsVBackupQueue, NULL);

  for (int32_t i = 0; i < tsVBackupPool.num; ++i) {
    SVBackupWorker *pWorker = tsVBackupPool.worker + i;
    pWorker->workerId = i;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

    if (pthread_create(&pWorker->thread, &thAttr, vnodeBackupFunc, pWorker) != 0) {
      vError("failed to create thread to process vbackup queue, reason:%s", strerror(errno));
    }

    pthread_attr_destroy(&thAttr);

    vDebug("vbackup:%d is launched, total:%d", pWorker->workerId, tsVBackupPool.num);
  }

  vDebug("vbackup queue:%p is allocated", tsVBackupQueue);

  return TSDB_CODE_SUCCESS;
}

static int32_t vnodeWriteIntoBackupWorker(int32_t vgId) {
  SVBackupMsg *pMsg = taosAllocateQitem(sizeof(SVBackupMsg));
  if (pMsg == NULL) return TSDB_CODE_VND_OUT_OF_MEMORY;

  pMsg->vgId = vgId;

  int32_t code = taosWriteQitem(tsVBackupQueue, TAOS_QTYPE_RPC, pMsg);
  if (code == 0) code = TSDB_CODE_DND_ACTION_IN_PROGRESS;

  return code;
}

int32_t vnodeBackup(int32_t vgId) {
  vTrace("vgId:%d, will backup", vgId);
  return vnodeWriteIntoBackupWorker(vgId);
}

int32_t vnodeInitBackup() {
  tsVBackupQset = taosOpenQset();

  tsVBackupPool.num = 1;
  tsVBackupPool.worker = calloc(sizeof(SVBackupWorker), tsVBackupPool.num);

  if (tsVBackupPool.worker == NULL) return -1;
  for (int32_t i = 0; i < tsVBackupPool.num; ++i) {
    SVBackupWorker *pWorker = tsVBackupPool.worker + i;
    pWorker->workerId = i;
    vDebug("vbackup:%d is created", i);
  }

  vDebug("vbackup is initialized, num:%d qset:%p", tsVBackupPool.num, tsVBackupQset);

  return vnodeStartBackup();
}

void vnodeCleanupBackup() {
  for (int32_t i = 0; i < tsVBackupPool.num; ++i) {
    SVBackupWorker *pWorker = tsVBackupPool.worker + i;
    if (taosCheckPthreadValid(pWorker->thread)) {
      taosQsetThreadResume(tsVBackupQset);
    }
    vDebug("vbackup:%d is closed", i);
  }

  for (int32_t i = 0; i < tsVBackupPool.num; ++i) {
    SVBackupWorker *pWorker = tsVBackupPool.worker + i;
    vDebug("vbackup:%d start to join", i);
    if (taosCheckPthreadValid(pWorker->thread)) {
      pthread_join(pWorker->thread, NULL);
    }
    vDebug("vbackup:%d join success", i);
  }

  vDebug("vbackup is closed, qset:%p", tsVBackupQset);

  taosCloseQset(tsVBackupQset);
  tsVBackupQset = NULL;

  tfree(tsVBackupPool.worker);

  vDebug("vbackup queue:%p is freed", tsVBackupQueue);
  taosCloseQueue(tsVBackupQueue);
  tsVBackupQueue = NULL;
}
