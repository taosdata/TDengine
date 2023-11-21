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
#include "command.h"
#include "query.h"
#include "schInt.h"
#include "tmsg.h"
#include "tref.h"
#include "trpc.h"

int32_t schSwitchJobStatus(SSchJob* pJob, int32_t status, void* param) {
  int32_t code = 0;
  SCH_ERR_JRET(schUpdateJobStatus(pJob, status));

  switch (status) {
    case JOB_TASK_STATUS_INIT:
      break;
    case JOB_TASK_STATUS_EXEC:
      SCH_ERR_JRET(schExecJob(pJob, (SSchedulerReq*)param));
      break;
    case JOB_TASK_STATUS_PART_SUCC:
      SCH_ERR_JRET(schProcessOnJobPartialSuccess(pJob));
      break;
    case JOB_TASK_STATUS_FETCH:
      SCH_ERR_JRET(schJobFetchRows(pJob));
      break;
    case JOB_TASK_STATUS_SUCC:
      break;
    case JOB_TASK_STATUS_FAIL:
      SCH_RET(schProcessOnJobFailure(pJob, (param ? *(int32_t*)param : 0)));
      break;
    case JOB_TASK_STATUS_DROP:
      schProcessOnJobDropped(pJob, *(int32_t*)param);

      if (taosRemoveRef(schMgmt.jobRef, pJob->refId)) {
        SCH_JOB_ELOG("remove job from job list failed, refId:0x%" PRIx64, pJob->refId);
      } else {
        SCH_JOB_DLOG("job removed from jobRef list, refId:0x%" PRIx64, pJob->refId);
      }
      break;
    default: {
      SCH_JOB_ELOG("unknown job status %d", status);
      SCH_RET(TSDB_CODE_SCH_STATUS_ERROR);
    }
  }

  return TSDB_CODE_SUCCESS;

_return:

  SCH_RET(schProcessOnJobFailure(pJob, code));
}

int32_t schHandleOpBeginEvent(int64_t jobId, SSchJob** job, SCH_OP_TYPE type, SSchedulerReq* pReq) {
  SSchJob* pJob = schAcquireJob(jobId);
  if (NULL == pJob) {
    qDebug("Acquire sch job failed, may be dropped, jobId:0x%" PRIx64, jobId);
    SCH_ERR_RET(TSDB_CODE_SCH_JOB_NOT_EXISTS);
  }

  *job = pJob;

  SCH_RET(schProcessOnOpBegin(pJob, type, pReq));
}

int32_t schHandleOpEndEvent(SSchJob* pJob, SCH_OP_TYPE type, SSchedulerReq* pReq, int32_t errCode) {
  int32_t code = errCode;

  if (NULL == pJob) {
    schDirectPostJobRes(pReq, errCode);
    SCH_RET(code);
  }

  schProcessOnOpEnd(pJob, type, pReq, errCode);

  if (TSDB_CODE_SCH_IGNORE_ERROR == errCode) {
    code = pJob->errCode;
  }

  schReleaseJob(pJob->refId);

  return code;
}
