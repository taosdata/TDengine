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

#include "executor.h"
#include "streamInt.h"
#include "tmisce.h"
#include "tref.h"
#include "stream.h"
#include "ttimer.h"
#include "dataSink.h"

SStreamMgmtInfo gStreamMgmt = {0};

void streamSetSnodeEnabled(  SMsgCb* msgCb) {
  gStreamMgmt.snodeEnabled = true;
  gStreamMgmt.msgCb = *msgCb;
  stInfo("snode %d enabled", (*gStreamMgmt.getDnode)(gStreamMgmt.dnode));
}

void streamSetSnodeDisabled(bool cleanup) {
  stInfo("snode disabled");
  gStreamMgmt.snodeEnabled = false;
  smUndeploySnodeTasks(cleanup);
}

void streamMgmtCleanup() {
  taosArrayDestroy(gStreamMgmt.vgLeaders);
  taosHashCleanup(gStreamMgmt.taskMap);
  taosHashCleanup(gStreamMgmt.vgroupMap);
}

void streamCleanup(void) {
  stTriggerTaskEnvCleanup();
  streamTimerCleanUp();
  smUndeployAllTasks();
  destroyDataSinkMgr();
  streamMgmtCleanup();
  destroyInserterGrpInfo();
}

int32_t streamInit(void* pDnode, getDnodeId_f getDnode, getMnodeEpset_f getMnode, getSynEpset_f getSynEpset) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  gStreamMgmt.dnode = pDnode;
  gStreamMgmt.getMnode = getMnode;
  gStreamMgmt.getDnode = getDnode;
  gStreamMgmt.getSynEpset = getSynEpset;

  gStreamMgmt.vgLeaders = taosArrayInit(20, sizeof(int32_t));
  TSDB_CHECK_NULL(gStreamMgmt.vgLeaders, code, lino, _exit, terrno);

  gStreamMgmt.taskMap = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  TSDB_CHECK_NULL(gStreamMgmt.taskMap, code, lino, _exit, terrno);

  gStreamMgmt.vgroupMap = taosHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false, HASH_ENTRY_LOCK);
  TSDB_CHECK_NULL(gStreamMgmt.vgroupMap, code, lino, _exit, terrno);

//  gStreamMgmt.snodeTasks = taosArrayInit(20, POINTER_BYTES);
//  TSDB_CHECK_NULL(gStreamMgmt.snodeTasks, code, lino, _exit, terrno);
  
  TAOS_CHECK_EXIT(streamTimerInit(&gStreamMgmt.timer));

  TAOS_CHECK_EXIT(streamHbInit(&gStreamMgmt.hb));

  TAOS_CHECK_EXIT(stTriggerTaskEnvInit());

  TAOS_CHECK_EXIT(initInserterGrpInfo());

  TAOS_CHECK_EXIT(initStreamDataSink());

_exit:

  if (code) {
    terrno = code;
    stError("%s failed at line %d, error:%s", __func__, lino, tstrerror(code));
  }

  return code;
}

int32_t streamVgIdSort(void const *lp, void const *rp) {
  int32_t* pVg1 = (int32_t*)lp;
  int32_t* pVg2 = (int32_t*)rp;

  if (*pVg1 < *pVg2) {
    return -1;
  } else if (*pVg1 > *pVg2) {
    return 1;
  }

  return 0;
}


void streamRemoveVnodeLeader(int32_t vgId) {
  taosWLockLatch(&gStreamMgmt.vgLeadersLock);
  int32_t idx = taosArraySearchIdx(gStreamMgmt.vgLeaders, &vgId, streamVgIdSort, TD_EQ);
  if (idx >= 0) {
    taosArrayRemove(gStreamMgmt.vgLeaders, idx);
  }
  taosWUnLockLatch(&gStreamMgmt.vgLeadersLock);
  
  if (idx >= 0) {
    stInfo("remove vgroup %d from vgroupLeaders succeed", vgId);
  } else {
    stWarn("remove vgroup %d from vgroupLeaders failed since not exists", vgId);
  }

  smUndeployVgTasks(vgId);
}

void streamAddVnodeLeader(int32_t vgId) {
  int32_t code = TSDB_CODE_SUCCESS;
  taosWLockLatch(&gStreamMgmt.vgLeadersLock);
  void* p = taosArrayPush(gStreamMgmt.vgLeaders, &vgId);
  if (p) {
    taosArraySort(gStreamMgmt.vgLeaders, streamVgIdSort);
  } else {
    code = terrno;
  }
  taosWUnLockLatch(&gStreamMgmt.vgLeadersLock);
  
  if (p) {
    stInfo("add vgroup %d to vgroupLeaders succeed", vgId);
  } else {
    stError("add vgroup %d to vgroupLeaders failed, error:%s", vgId, tstrerror(code));
  }
}

int32_t streamAcquireTask(int64_t streamId, int64_t taskId, SStreamTask** ppTask, void** ppAddr) {
  int64_t key[2] = {streamId, taskId};

  SStreamTask** task = taosHashAcquire(gStreamMgmt.taskMap, key, sizeof(key));
  if (NULL == task) {
    stsError("task %" PRIx64 " not exists in taskMap", taskId);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  SStreamTask* pTask = *task;
  if (taosRTryLockLatch(&pTask->entryLock)) {
    ST_TASK_DLOG("task entry lock failed since task dropping, entryLock:%x", pTask->entryLock);
    taosHashRelease(gStreamMgmt.taskMap, task);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  *ppTask = pTask;
  *ppAddr = (void*)task;

  return TSDB_CODE_SUCCESS;
}

void streamReleaseTask(void* taskAddr) {
  if (NULL == taskAddr) {
    return;
  }
  
  SStreamTask* pTask = *(SStreamTask**)taskAddr;
  SRWLatch lock = taosRUnLockLatch_r(&pTask->entryLock);
  if (taosIsOnlyWLocked(&lock)) {
    switch (pTask->type) {
      case STREAM_READER_TASK:
        stReaderTaskUndeploy((SStreamReaderTask**)taskAddr, true);
        break;
      case STREAM_TRIGGER_TASK:
        stTriggerTaskUndeploy((SStreamTriggerTask**)taskAddr, true);
        break;
      case STREAM_RUNNER_TASK:
        stRunnerTaskUndeploy((SStreamRunnerTask**)taskAddr, true);
        break;
      default:
        break;
    }
  }
  
  taosHashRelease(gStreamMgmt.taskMap, taskAddr);
}

int32_t streamAcquireTriggerTask(int64_t streamId, SStreamTask** ppTask, void** ppAddr) {
  int32_t gid = STREAM_GID(streamId);
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;
  SHashObj* pGrp = gStreamMgmt.stmGrp[gid];

  SStreamInfo* pStream = taosHashAcquire(pGrp, &streamId, sizeof(streamId));
  if (NULL == pStream) {
    stError("stream %" PRIx64 " not exists", streamId);
    return TSDB_CODE_MND_STREAM_NOT_EXIST;
  }

  TAOS_CHECK_EXIT(streamAcquireTask(streamId, pStream->triggerTaskId, ppTask, ppAddr));

_exit:

  taosHashRelease(pGrp, pStream);

  if (code) {
    stsError("%s failed at line %d, error:%s", __FUNCTION__, lino, tstrerror(code));
  }

  return code;
}


void streamHandleTaskError(int64_t streamId, int64_t taskId, int32_t errCode) {
  int64_t key[2] = {streamId, taskId};

  SStreamTask** task = taosHashGet(gStreamMgmt.taskMap, key, sizeof(key));
  if (NULL == task) {
    stError("stream %" PRIx64 " task %" PRIx64 " not exists in taskMap", streamId, taskId);
    return;
  }

  atomic_store_32(&(*task)->errorCode, errCode);
  atomic_store_32((int32_t*)&(*task)->status, STREAM_STATUS_FAILED);
}

