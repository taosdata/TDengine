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
  
  stDebug("remove vgroup %d from vgroupLeader %s", vgId, (idx < 0) ? "failed" : "succeed");
}

void streamAddVnodeLeader(int32_t vgId) {
  taosWLockLatch(&gStreamMgmt.vgLeadersLock);
  void* p = taosArrayPush(gStreamMgmt.vgLeaders, &vgId);
  if (p) {
    taosArraySort(gStreamMgmt.vgLeaders, streamVgIdSort);
  }
  taosWUnLockLatch(&gStreamMgmt.vgLeadersLock);
  stInfo("add vgroup %d to vgroupLeader %s, error:%s", vgId, p ? "succeed" : "failed", p ? "NULL" : tstrerror(terrno));
}


