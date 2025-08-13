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

struct SStreamRunnerUndeployMsg;
struct SStreamRunnerTaskStatus;

typedef struct SStreamRunnerTaskStatus  SStreamRunnerTaskStatus;

int32_t stRunnerTaskDeploy(SStreamRunnerTask* pTask, SStreamRunnerDeployMsg* pMsg);
int32_t stRunnerTaskUndeploy(SStreamRunnerTask** ppTask, bool force);
int32_t stRunnerTaskExecute(SStreamRunnerTask* pTask, SSTriggerCalcRequest* pReq);
int32_t stRunnerFetchDataFromCache(SStreamCacheReadInfo* pInfo, bool* finished);
int32_t stRunnerTaskDropTable(SStreamRunnerTask* pTask, SSTriggerDropRequest* pReq);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_RUNNER_H
