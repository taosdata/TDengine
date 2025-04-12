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

#ifndef TDENGINE_STREAM_RUNNER_H
#define TDENGINE_STREAM_RUNNER_H

#include <stdint.h>
#include "stream.h"
#include "tlist.h"
#ifdef __cplusplus
extern "C" {
#endif

struct SStreamRunnerTaskExecution;
struct SStreamRunnerTask;
typedef int32_t (*StreamBuildTaskExecFn)(const struct SStreamRunnerTask*    pTask,
                                         struct SStreamRunnerTaskExecution* pTaskExec);

typedef struct SStreamRunnerTaskExecution {
  const char* pPlan;
  void*       pExecutor;
} SStreamRunnerTaskExecution;

typedef struct SStreamRunnerTaskOutput {
} SStreamRunnerTaskOutput;

typedef struct SStreamRunnerTaskExecMgr {
  SList*        pFreeExecs;
  SList*        pRunningExecs;
  TdThreadMutex lock;
} SStreamRunnerTaskExecMgr;

typedef struct SStreamRunnerTask {
  SStreamTask              streamTask;
  StreamBuildTaskExecFn    buildTaskFn;
  SStreamRunnerTaskExecMgr pExecMgr;
  SStreamRunnerTaskOutput  output;
  bool                     forceWindowClose;
  const char*              pPlan;
  int32_t                  parallelExecutionNun;
} SStreamRunnerTask;

typedef struct SStreamRunnerDeployMsg {
  SStreamTask           task;
  const char*           pPlan;
  StreamBuildTaskExecFn buildTaskFn;
  bool                  forceWindowClose;
} SStreamRunnerDeployMsg;
struct SStreamRunnerUndeployMsg;
struct SStreamRunnerTaskStatus;

typedef struct SStreamRunaaaanerUndeployMsg SStreamRunnerUndeployMsg;
typedef struct SStreamRunnerTaskStatus      SStreamRunnerTaskStatus;

int32_t stRunnerTaskDeploy(SStreamRunnerTask** pTask, const SStreamRunnerDeployMsg* pMsg);
int32_t stRunnerTaskUndeploy(SStreamRunnerTask* pTask, const SStreamRunnerUndeployMsg* pMsg);
int32_t stRunnerTaskExecute(SStreamRunnerTask* pTask, const char* pMsg, int32_t msgLen);
int32_t stRunnerTaskRetrieveStatus(SStreamRunnerTask* pTask, SStreamRunnerTaskStatus* pStatus);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_RUNNER_H
