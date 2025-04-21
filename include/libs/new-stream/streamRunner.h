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
#include "executor.h"
#include "stream.h"
#include "tlist.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SStreamRunnerTaskExecution {
  const char* pPlan;
  void*       pExecutor;
  void*       notifyEventSup;
} SStreamRunnerTaskExecution; // TODO wjm move back into Runner.c

typedef struct SStreamRunnerTaskOutput {
  struct SSDataBlock* pBlock;
} SStreamRunnerTaskOutput;

typedef struct SStreamRunnerTaskNotification {
} SStreamRunnerTaskNotification;

typedef struct SStreamRunnerTaskExecMgr {
  SList*        pFreeExecs;
  SList*        pRunningExecs;
  TdThreadMutex lock;
  bool          exit;
} SStreamRunnerTaskExecMgr;

typedef struct SStreamRunnerTask {
  SStreamTask                   task;
  SStreamRunnerTaskExecMgr      pExecMgr;
  SStreamRunnerTaskOutput       output;
  SStreamRunnerTaskNotification notification;
  bool                          forceWindowClose;
  const char*                   pPlan;
  int32_t                       parallelExecutionNun;
  SReadHandle                   handle;
} SStreamRunnerTask;

typedef struct SStreamRunnerDeployMsg {
  SStreamTask           task;
  const char*           pPlan;
  SReadHandle           handle;
  bool                  forceOutput;
  const char*           pSubTableExpr;
} SStreamRunnerDeployMsg;
struct SStreamRunnerUndeployMsg;
struct SStreamRunnerTaskStatus;

typedef struct SStreamRunnerTaskStatus  SStreamRunnerTaskStatus;

int32_t stRunnerTaskDeploy(SStreamRunnerTask* pTask, const SStreamRunnerDeployMsg* pMsg);
int32_t stRunnerTaskUndeploy(SStreamRunnerTask* pTask, const SStreamUndeployTaskMsg* pMsg);
int32_t stRunnerTaskExecute(SStreamRunnerTask* pTask, const char* pMsg, int32_t msgLen);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_RUNNER_H
