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

SSchStatusFps gSchJobFps[JOB_TASK_STATUS_MAX] = {
  {JOB_TASK_STATUS_NULL,      schJobStNullEnter, schJobStNullLeave, schJobStNullEvent},
  {JOB_TASK_STATUS_INIT,      schJobStNullEnter, schJobStNullLeave, schJobStNullEvent},
  {JOB_TASK_STATUS_EXEC,      schJobStNullEnter, schJobStNullLeave, schJobStNullEvent},
  {JOB_TASK_STATUS_PART_SUCC, schJobStNullEnter, schJobStNullLeave, schJobStNullEvent},
  {JOB_TASK_STATUS_SUCC,      schJobStNullEnter, schJobStNullLeave, schJobStNullEvent},
  {JOB_TASK_STATUS_FAIL,      schJobStNullEnter, schJobStNullLeave, schJobStNullEvent},
  {JOB_TASK_STATUS_DROP,      schJobStNullEnter, schJobStNullLeave, schJobStNullEvent},
};

SSchStatusFps gSchTaskFps[JOB_TASK_STATUS_MAX] = {
  {JOB_TASK_STATUS_NULL,      schTaskStatusNullEnter, schTaskStatusNullLeave, schTaskStatusNullEvent},
  {JOB_TASK_STATUS_INIT,      schTaskStatusNullEnter, schTaskStatusNullLeave, schTaskStatusNullEvent},
  {JOB_TASK_STATUS_EXEC,      schTaskStatusNullEnter, schTaskStatusNullLeave, schTaskStatusNullEvent},
  {JOB_TASK_STATUS_PART_SUCC, schTaskStatusNullEnter, schTaskStatusNullLeave, schTaskStatusNullEvent},
  {JOB_TASK_STATUS_SUCC,      schTaskStatusNullEnter, schTaskStatusNullLeave, schTaskStatusNullEvent},
  {JOB_TASK_STATUS_FAIL,      schTaskStatusNullEnter, schTaskStatusNullLeave, schTaskStatusNullEvent},
  {JOB_TASK_STATUS_DROP,      schTaskStatusNullEnter, schTaskStatusNullLeave, schTaskStatusNullEvent},
};

int32_t schSwitchJobStatus(int32_t status, SSchJob* pJob, void* pParam) {
  schJobStatusEnter(pJob, status, pParam);
}




