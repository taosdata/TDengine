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

void streamSetSnodeEnabled(void) {
  gStreamMgmt.snodeId = gStreamMgmt.dnodeId;
}

void streamSetSnodeDisabled(void) {
  gStreamMgmt.snodeId = INT32_MIN;
}

void streamCleanup(void) {
  //STREAMTODO
  streamTriggerEnvCleanup();
  destroyDataSinkMgr();
  destroyInserterGrpInfo();
}

int32_t streamInit(void* pDnode, getDnodeId_f getDnode, getMnodeEpset_f getMnode) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t lino = 0;

  gStreamMgmt.dnode = pDnode;
  gStreamMgmt.getMnode = getMnode;
  gStreamMgmt.getDnode = getDnode;

  gStreamMgmt.vgLeaders = taosArrayInit(20, sizeof(int32_t));
  TSDB_CHECK_NULL(gStreamMgmt.vgLeaders, code, lino, _exit, terrno);

  gStreamMgmt.taskMap = taosHashInit(100, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  TSDB_CHECK_NULL(gStreamMgmt.taskMap, code, lino, _exit, terrno);

  gStreamMgmt.vgroupMap = taosHashInit(20, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  TSDB_CHECK_NULL(gStreamMgmt.vgroupMap, code, lino, _exit, terrno);

  gStreamMgmt.snodeTasks = taosArrayInit(20, POINTER_BYTES);
  TSDB_CHECK_NULL(gStreamMgmt.snodeTasks, code, lino, _exit, terrno);
  
  TAOS_CHECK_EXIT(streamTimerInit(&gStreamMgmt.timer));

  TAOS_CHECK_EXIT(streamHbInit(&gStreamMgmt.hb));

  TAOS_CHECK_EXIT(streamTriggerEnvInit());

  TAOS_CHECK_EXIT(initInserterGrpInfo());

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

int32_t streamGetTask(int64_t streamId, int64_t taskId, SStreamTask** ppTask) {
  int64_t key[2] = {streamId, taskId};

  SStreamTask** task = taosHashGet(gStreamMgmt.taskMap, key, sizeof(key));
  if (NULL == task) {
    stError("stream %" PRIx64 " task %" PRIx64 " not exists in taskMap", streamId, taskId);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  *ppTask = *task;

  return TSDB_CODE_SUCCESS;
}


