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

#include "catalog.h"
#include "query.h"
#include "schInt.h"
#include "tmsg.h"
#include "tref.h"

void schFreeFlowCtrl(SSchJob *pJob) {
  if (NULL == pJob->flowCtrl) {
    return;
  }

  SSchFlowControl *ctrl = NULL;
  void            *pIter = taosHashIterate(pJob->flowCtrl, NULL);
  while (pIter) {
    ctrl = (SSchFlowControl *)pIter;

    if (ctrl->taskList) {
      taosArrayDestroy(ctrl->taskList);
    }

    pIter = taosHashIterate(pJob->flowCtrl, pIter);
  }

  taosHashCleanup(pJob->flowCtrl);
  pJob->flowCtrl = NULL;
}

int32_t schChkJobNeedFlowCtrl(SSchJob *pJob, SSchLevel *pLevel) {
  if (!SCH_IS_QUERY_JOB(pJob)) {
    SCH_JOB_DLOG("job no need flow ctrl, queryJob:%d", SCH_IS_QUERY_JOB(pJob));
    return TSDB_CODE_SUCCESS;
  }

  int64_t sum = 0;
  int32_t taskNum = taosArrayGetSize(pJob->dataSrcTasks);
  for (int32_t i = 0; i < taskNum; ++i) {
    SSchTask *pTask = *(SSchTask **)taosArrayGet(pJob->dataSrcTasks, i);

    sum += pTask->plan->execNodeStat.tableNum;
  }

  if (schMgmt.cfg.maxNodeTableNum <= 0 || sum < schMgmt.cfg.maxNodeTableNum) {
    SCH_JOB_DLOG("job no need flow ctrl, totalTableNum:%" PRId64, sum);
    return TSDB_CODE_SUCCESS;
  }

  pJob->flowCtrl =
      taosHashInit(pJob->taskNum, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), false, HASH_ENTRY_LOCK);
  if (NULL == pJob->flowCtrl) {
    SCH_JOB_ELOG("taosHashInit %d flowCtrl failed", pJob->taskNum);
    SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
  }

  SCH_SET_JOB_NEED_FLOW_CTRL(pJob);

  SCH_JOB_DLOG("job NEED flow ctrl, totalTableNum:%" PRId64, sum);

  return TSDB_CODE_SUCCESS;
}

int32_t schDecTaskFlowQuota(SSchJob *pJob, SSchTask *pTask) {
  SSchLevel       *pLevel = pTask->level;
  SSchFlowControl *ctrl = NULL;
  int32_t          code = 0;
  SEp             *ep = SCH_GET_CUR_EP(&pTask->plan->execNode);

  ctrl = (SSchFlowControl *)taosHashGet(pJob->flowCtrl, ep, sizeof(SEp));
  if (NULL == ctrl) {
    SCH_TASK_ELOG("taosHashGet node from flowCtrl failed, fqdn:%s, port:%d", ep->fqdn, ep->port);
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  SCH_LOCK(SCH_WRITE, &ctrl->lock);
  if (ctrl->execTaskNum <= 0) {
    SCH_TASK_ELOG("taosHashGet node from flowCtrl failed, fqdn:%s, port:%d", ep->fqdn, ep->port);
    SCH_ERR_JRET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  --ctrl->execTaskNum;
  ctrl->tableNumSum -= pTask->plan->execNodeStat.tableNum;

  SCH_TASK_DLOG("task quota removed, fqdn:%s, port:%d, tableNum:%d, remainNum:%" PRId64 ", remainExecTaskNum:%d", ep->fqdn,
                ep->port, pTask->plan->execNodeStat.tableNum, ctrl->tableNumSum, ctrl->execTaskNum);

_return:

  SCH_UNLOCK(SCH_WRITE, &ctrl->lock);

  SCH_RET(code);
}

int32_t schCheckIncTaskFlowQuota(SSchJob *pJob, SSchTask *pTask, bool *enough) {
  SSchLevel       *pLevel = pTask->level;
  int32_t          code = 0;
  SSchFlowControl *ctrl = NULL;
  SEp             *ep = SCH_GET_CUR_EP(&pTask->plan->execNode);

  do {
    ctrl = (SSchFlowControl *)taosHashGet(pJob->flowCtrl, ep, sizeof(SEp));
    if (NULL == ctrl) {
      SSchFlowControl nctrl = {.tableNumSum = pTask->plan->execNodeStat.tableNum, .execTaskNum = 1};

      code = taosHashPut(pJob->flowCtrl, ep, sizeof(SEp), &nctrl, sizeof(nctrl));
      if (code) {
        if (HASH_NODE_EXIST(code)) {
          continue;
        }

        SCH_TASK_ELOG("taosHashPut flowCtrl failed, size:%d", (int32_t)sizeof(nctrl));
        SCH_ERR_RET(TSDB_CODE_OUT_OF_MEMORY);
      }

      SCH_TASK_DLOG("task quota added, fqdn:%s, port:%d, tableNum:%d, remainNum:%" PRId64 ", remainExecTaskNum:%d", ep->fqdn,
                    ep->port, pTask->plan->execNodeStat.tableNum, nctrl.tableNumSum, nctrl.execTaskNum);

      *enough = true;
      return TSDB_CODE_SUCCESS;
    }

    SCH_LOCK(SCH_WRITE, &ctrl->lock);

    if (0 == ctrl->execTaskNum) {
      ctrl->tableNumSum = pTask->plan->execNodeStat.tableNum;
      ++ctrl->execTaskNum;

      *enough = true;
      break;
    }

    int64_t sum = pTask->plan->execNodeStat.tableNum + ctrl->tableNumSum;

    if (sum <= schMgmt.cfg.maxNodeTableNum) {
      ctrl->tableNumSum = sum;
      ++ctrl->execTaskNum;

      *enough = true;
      break;
    }

    if (NULL == ctrl->taskList) {
      ctrl->taskList = taosArrayInit(pLevel->taskNum, POINTER_BYTES);
      if (NULL == ctrl->taskList) {
        SCH_TASK_ELOG("taosArrayInit taskList failed, size:%d", (int32_t)pLevel->taskNum);
        SCH_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
      }
    }

    if (NULL == taosArrayPush(ctrl->taskList, &pTask)) {
      SCH_TASK_ELOG("taosArrayPush to taskList failed, size:%d", (int32_t)taosArrayGetSize(ctrl->taskList));
      SCH_ERR_JRET(TSDB_CODE_OUT_OF_MEMORY);
    }

    *enough = false;
    ctrl->sorted = false;

    break;
  } while (true);

_return:

  SCH_TASK_DLOG("task quota %s added, fqdn:%s, port:%d, tableNum:%d, remainNum:%" PRId64 ", remainExecTaskNum:%d",
                ((*enough) ? "" : "NOT"), ep->fqdn, ep->port, pTask->plan->execNodeStat.tableNum, ctrl->tableNumSum,
                ctrl->execTaskNum);

  SCH_UNLOCK(SCH_WRITE, &ctrl->lock);

  SCH_RET(code);
}

int32_t schTaskTableNumCompare(const void *key1, const void *key2) {
  SSchTask *pTask1 = *(SSchTask **)key1;
  SSchTask *pTask2 = *(SSchTask **)key2;

  if (pTask1->plan->execNodeStat.tableNum < pTask2->plan->execNodeStat.tableNum) {
    return 1;
  } else if (pTask1->plan->execNodeStat.tableNum > pTask2->plan->execNodeStat.tableNum) {
    return -1;
  } else {
    return 0;
  }
}

int32_t schLaunchTasksInFlowCtrlListImpl(SSchJob *pJob, SSchFlowControl *ctrl) {
  SCH_LOCK(SCH_WRITE, &ctrl->lock);

  if (NULL == ctrl->taskList || taosArrayGetSize(ctrl->taskList) <= 0) {
    SCH_UNLOCK(SCH_WRITE, &ctrl->lock);
    return TSDB_CODE_SUCCESS;
  }

  int64_t   remainNum = schMgmt.cfg.maxNodeTableNum - ctrl->tableNumSum;
  int32_t   taskNum = taosArrayGetSize(ctrl->taskList);
  int32_t   code = 0;
  SSchTask *pTask = NULL;

  if (taskNum > 1 && !ctrl->sorted) {
    taosArraySort(ctrl->taskList, schTaskTableNumCompare);  // desc order
  }

  for (int32_t i = 0; i < taskNum; ++i) {
    pTask = *(SSchTask **)taosArrayGet(ctrl->taskList, i);
    SEp *ep = SCH_GET_CUR_EP(&pTask->plan->execNode);

    if (pTask->plan->execNodeStat.tableNum > remainNum && ctrl->execTaskNum > 0) {
      SCH_TASK_DLOG("task NOT to launch, fqdn:%s, port:%d, tableNum:%d, remainNum:%" PRId64 ", remainExecTaskNum:%d", ep->fqdn,
                    ep->port, pTask->plan->execNodeStat.tableNum, ctrl->tableNumSum, ctrl->execTaskNum);

      continue;
    }

    ctrl->tableNumSum += pTask->plan->execNodeStat.tableNum;
    ++ctrl->execTaskNum;

    taosArrayRemove(ctrl->taskList, i);

    SCH_TASK_DLOG("task to launch, fqdn:%s, port:%d, tableNum:%d, remainNum:%" PRId64 ", remainExecTaskNum:%d", ep->fqdn,
                  ep->port, pTask->plan->execNodeStat.tableNum, ctrl->tableNumSum, ctrl->execTaskNum);

    SCH_ERR_JRET(schAsyncLaunchTaskImpl(pJob, pTask));

    remainNum -= pTask->plan->execNodeStat.tableNum;
    if (remainNum <= 0) {
      SCH_TASK_DLOG("no more task to launch, fqdn:%s, port:%d, remainNum:%" PRId64 ", remainExecTaskNum:%d", ep->fqdn, ep->port,
                    ctrl->tableNumSum, ctrl->execTaskNum);

      break;
    }

    if (i < (taskNum - 1)) {
      SSchTask *pLastTask = *(SSchTask **)taosArrayGetLast(ctrl->taskList);
      if (remainNum < pLastTask->plan->execNodeStat.tableNum) {
        SCH_TASK_DLOG("no more task to launch, fqdn:%s, port:%d, remainNum:%" PRId64 ", remainExecTaskNum:%d, smallestInList:%d",
                      ep->fqdn, ep->port, ctrl->tableNumSum, ctrl->execTaskNum, pLastTask->plan->execNodeStat.tableNum);

        break;
      }
    }

    --i;
    --taskNum;
  }

_return:

  SCH_UNLOCK(SCH_WRITE, &ctrl->lock);

  if (code) {
    code = schProcessOnTaskFailure(pJob, pTask, code);
  }

  SCH_RET(code);
}

int32_t schLaunchTasksInFlowCtrlList(SSchJob *pJob, SSchTask *pTask) {
  if (!SCH_TASK_NEED_FLOW_CTRL(pJob, pTask)) {
    return TSDB_CODE_SUCCESS;
  }

  SCH_ERR_RET(schDecTaskFlowQuota(pJob, pTask));

  SEp *ep = SCH_GET_CUR_EP(&pTask->plan->execNode);

  SSchFlowControl *ctrl = (SSchFlowControl *)taosHashGet(pJob->flowCtrl, ep, sizeof(SEp));
  if (NULL == ctrl) {
    SCH_TASK_ELOG("taosHashGet node from flowCtrl failed, fqdn:%s, port:%d", ep->fqdn, ep->port);
    SCH_ERR_RET(TSDB_CODE_SCH_INTERNAL_ERROR);
  }

  int32_t code = schLaunchTasksInFlowCtrlListImpl(pJob, ctrl);
  SCH_ERR_RET(code);

  return code;  // to avoid compiler error
}
