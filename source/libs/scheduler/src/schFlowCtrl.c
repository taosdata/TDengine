/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include "catalog.h"
#include "query.h"
#include "schInt.h"
#include "tmsg.h"
#include "tref.h"

static SSchJob* schFlowCtrlOwner(SSchJob* pJob) {
  SSchJob* pParent = SCH_PARENT_JOB(pJob);
  return pParent->concurrentSubJobs ? pParent : pJob;
}

void schFreeFlowCtrl(SSchJob* pJob) {
  if (schFlowCtrlOwner(pJob) != pJob) {
    pJob->flowCtrl = NULL;
    return;
  }
  void* pIter = taosHashIterate(pJob->flowCtrl, NULL);
  while (pIter) {
    taosArrayDestroy(((SSchFlowControl*)pIter)->taskList);
    pIter = taosHashIterate(pJob->flowCtrl, pIter);
  }
  taosHashCleanup(pJob->flowCtrl);
  pJob->flowCtrl = NULL;
}

static int64_t schJobScanTableNum(SSchJob* pJob) {
  int64_t sum = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pJob->dataSrcTasks); ++i) {
    SSchTask* pTask = taosArrayGetP(pJob->dataSrcTasks, i);
    sum += schTaskGetTableNum(pTask);
  }
  return sum;
}

int32_t schInitJobFlowCtrl(SSchJob* pJob) {
  int64_t sum = schJobScanTableNum(pJob);
  for (int32_t i = 0; i < taosArrayGetSize(pJob->subJobs); ++i) {
    sum += schJobScanTableNum(taosArrayGetP(pJob->subJobs, i));
  }
  if (schMgmt.cfg.maxNodeTableNum <= 0 || sum < schMgmt.cfg.maxNodeTableNum) {
    return TSDB_CODE_SUCCESS;
  }
  pJob->flowCtrl = taosHashInit(SCH_GET_TASK_CAPACITY(pJob->taskNum),
      taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pJob->flowCtrl) return terrno;
  SCH_SET_JOB_NEED_FLOW_CTRL(pJob);
  for (int32_t i = 0; i < taosArrayGetSize(pJob->subJobs); ++i) {
    SSchJob* pSub = taosArrayGetP(pJob->subJobs, i);
    pSub->flowCtrl = pJob->flowCtrl;
    SCH_SET_JOB_NEED_FLOW_CTRL(pSub);
  }
  SCH_JOB_DLOG("shared task flow control, totalTableNum:%" PRId64, sum);
  return TSDB_CODE_SUCCESS;
}

int32_t schChkJobNeedFlowCtrl(SSchJob* pJob, SSchLevel* pLevel) {
  if (SCH_PARENT_JOB(pJob)->concurrentSubJobs || pJob->flowCtrl || !SCH_IS_QUERY_JOB(pJob)) {
    return TSDB_CODE_SUCCESS;
  }
  int64_t sum = schJobScanTableNum(pJob);
  if (schMgmt.cfg.maxNodeTableNum <= 0 || sum < schMgmt.cfg.maxNodeTableNum) {
    return TSDB_CODE_SUCCESS;
  }
  pJob->flowCtrl = taosHashInit(SCH_GET_TASK_CAPACITY(pJob->taskNum),
      taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pJob->flowCtrl) return terrno;
  SCH_SET_JOB_NEED_FLOW_CTRL(pJob);
  return TSDB_CODE_SUCCESS;
}

static void schReserveTaskFlowQuota(SSchFlowControl* ctrl, SSchTask* pTask, const SEp* ep) {
  pTask->flowEp = *ep;
  pTask->flowTableNum = schTaskGetTableNum(pTask);
  atomic_store_8(&pTask->flowQueued, 0);
  atomic_store_8(&pTask->flowQuotaHeld, 1);
  ctrl->tableNumSum += pTask->flowTableNum;
  ++ctrl->execTaskNum;
}

int32_t schDecTaskFlowQuota(SSchJob* pJob, SSchTask* pTask) {
  SSchJob* pOwner = schFlowCtrlOwner(pJob);
  SCH_LOCK(SCH_WRITE, &pOwner->flowLock);
  atomic_store_8(&pTask->flowQueued, 0);
  if (atomic_load_8(&pTask->flowQuotaHeld)) {
    SSchFlowControl* ctrl = taosHashGet(pOwner->flowCtrl, &pTask->flowEp, sizeof(SEp));
    if (NULL == ctrl || 0 == ctrl->execTaskNum) {
      SCH_UNLOCK(SCH_WRITE, &pOwner->flowLock);
      return TSDB_CODE_SCH_INTERNAL_ERROR;
    }
    ctrl->tableNumSum -= pTask->flowTableNum;
    --ctrl->execTaskNum;
    atomic_store_8(&pTask->flowQuotaHeld, 0);
  }
  SCH_UNLOCK(SCH_WRITE, &pOwner->flowLock);
  return TSDB_CODE_SUCCESS;
}

int32_t schCheckIncTaskFlowQuota(SSchJob* pJob, SSchTask* pTask, bool* enough) {
  SSchJob* pOwner = schFlowCtrlOwner(pJob);
  SQueryNodeAddr* pAddr = NULL;
  SCH_ERR_RET(schSetTaskCandidateAddrs(pJob, pTask));
  SCH_ERR_RET(schGetTaskCurrentNodeAddr(pTask, pJob, &pAddr));
  const SEp* ep = SCH_GET_CUR_EP(pAddr);
  int32_t code = TSDB_CODE_SUCCESS;
  *enough = false;
  SCH_LOCK(SCH_WRITE, &pOwner->flowLock);
  SSchFlowControl* ctrl = taosHashGet(pOwner->flowCtrl, ep, sizeof(SEp));
  if (NULL == ctrl) {
    SSchFlowControl newCtrl = {0};
    code = taosHashPut(pOwner->flowCtrl, ep, sizeof(SEp), &newCtrl, sizeof(newCtrl));
    if (TSDB_CODE_SUCCESS != code) goto _return;
    ctrl = taosHashGet(pOwner->flowCtrl, ep, sizeof(SEp));
    if (NULL == ctrl) {
      code = TSDB_CODE_SCH_INTERNAL_ERROR;
      goto _return;
    }
  }
  if (atomic_load_8(&pTask->flowQuotaHeld)) {
    *enough = true;
    goto _return;
  }
  if (atomic_load_8(&pTask->flowQueued)) goto _return;
  if (0 == ctrl->execTaskNum ||
      ctrl->tableNumSum + schTaskGetTableNum(pTask) <= schMgmt.cfg.maxNodeTableNum) {
    schReserveTaskFlowQuota(ctrl, pTask, ep);
    *enough = true;
    goto _return;
  }
  if (NULL == ctrl->taskList) {
    ctrl->taskList = taosArrayInit(4, sizeof(SSchFlowTask));
    if (NULL == ctrl->taskList) {
      code = terrno;
      goto _return;
    }
  }
  SSchFlowTask entry = {.subJobId = pJob->subJobId, .pTask = pTask,
      .seriesId = pTask->seriesId, .execId = pTask->execId,
      .launchSequence = atomic_load_64(&pTask->launchSequence)};
  if (NULL == taosArrayPush(ctrl->taskList, &entry)) {
    code = terrno;
    goto _return;
  }
  pTask->flowEp = *ep;
  atomic_store_8(&pTask->flowQueued, 1);
  ctrl->sorted = false;

_return:
  SCH_UNLOCK(SCH_WRITE, &pOwner->flowLock);
  return code;
}

static bool schFlowTaskCurrent(const SSchFlowTask* entry) {
  SSchTask* pTask = entry->pTask;
  return entry->seriesId == atomic_load_64(&pTask->seriesId) &&
      entry->execId == atomic_load_32(&pTask->execId) &&
      entry->launchSequence == atomic_load_64(&pTask->launchSequence);
}

static int32_t schTaskTableNumCompare(const void* left, const void* right) {
  const SSchTask* pLeft = ((const SSchFlowTask*)left)->pTask;
  const SSchTask* pRight = ((const SSchFlowTask*)right)->pTask;
  int32_t leftTables = schTaskGetTableNum(pLeft);
  int32_t rightTables = schTaskGetTableNum(pRight);
  return leftTables < rightTables ? 1 : (leftTables > rightTables ? -1 : 0);
}

int32_t schLaunchTasksInFlowCtrlListImpl(SSchJob* pJob, SSchFlowControl* ctrl) {
  SSchJob* pOwner = schFlowCtrlOwner(pJob);
  while (!schJobNeedToStop(pOwner, NULL)) {
    SSchFlowTask selected = {0};
    SSchJob* pSelectedJob = NULL;
    SCH_LOCK(SCH_WRITE, &pOwner->flowLock);
    if (!ctrl->sorted && taosArrayGetSize(ctrl->taskList) > 1) {
      taosArraySort(ctrl->taskList, schTaskTableNumCompare);
      ctrl->sorted = true;
    }
    for (int32_t i = 0; i < taosArrayGetSize(ctrl->taskList);) {
      SSchFlowTask* entry = taosArrayGet(ctrl->taskList, i);
      SSchTask* pTask = entry->pTask;
      if (!schFlowTaskCurrent(entry) || !atomic_load_8(&pTask->flowQueued)) {
        taosArrayRemove(ctrl->taskList, i);
        continue;
      }
      if (ctrl->execTaskNum > 0 &&
          ctrl->tableNumSum + schTaskGetTableNum(pTask) > schMgmt.cfg.maxNodeTableNum) {
        ++i;
        continue;
      }
      selected = *entry;
      pSelectedJob = pJob;
      if (SCH_PARENT_JOB(pJob)->concurrentSubJobs) {
        pSelectedJob = entry->subJobId < 0 ? pOwner : taosArrayGetP(pOwner->subJobs, entry->subJobId);
      }
      schReserveTaskFlowQuota(ctrl, pTask, &pTask->flowEp);
      taosArrayRemove(ctrl->taskList, i);
      break;
    }
    SCH_UNLOCK(SCH_WRITE, &pOwner->flowLock);
    if (NULL == selected.pTask) return TSDB_CODE_SUCCESS;

    // Dispatch this attempt after releasing the flow lock.
    int32_t code = schAsyncLaunchFlowTask(pSelectedJob, &selected);
    if (TSDB_CODE_SUCCESS != code) {
      return schHandleJobFailure(pSelectedJob, code);
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t schLaunchTasksInFlowCtrlList(SSchJob* pJob, SSchTask* pTask) {
  if (!pJob->flowCtrl) return TSDB_CODE_SUCCESS;
  SSchJob* pOwner = schFlowCtrlOwner(pJob);
  SEp ep = pTask->flowEp;
  SCH_ERR_RET(schDecTaskFlowQuota(pJob, pTask));
  SSchFlowControl* ctrl = taosHashGet(pOwner->flowCtrl, &ep, sizeof(SEp));
  if (NULL == ctrl) return TSDB_CODE_SUCCESS;
  return schLaunchTasksInFlowCtrlListImpl(pJob, ctrl);
}
