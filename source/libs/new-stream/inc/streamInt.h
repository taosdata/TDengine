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
#ifdef USE_STREAM
#ifndef TDENGINE_STREAM_INT_H
#define TDENGINE_STREAM_INT_H

#include "executor.h"
#include "query.h"
#include "trpc.h"
#include "stream.h"
#include "tref.h"
#include "ttimer.h"
#include "streamRunner.h"

#ifdef __cplusplus
extern "C" {
#endif

#define STREAM_GRP_STREAM_NUM             20
#define STREAM_HB_ERR_HANDLE_MAX_DELAY    300000

typedef void (*taskUndeplyCallback)(void*);

typedef struct SStreamHbInfo {
  int32_t      lastErrCode;
  int64_t      lastErrTs;
  tmr_h        hbTmr;
  SStreamHbMsg hbMsg;
} SStreamHbInfo;

typedef struct SStreamTasksInfo {
  int32_t             taskNum;
  SArray*             readerList;        // SArray<SStreamReaderTask>
  SStreamTriggerTask* triggerTask;
  SArray*             runnerList;        // SArray<SStreamRunnerTask>
} SStreamTasksInfo;

typedef struct SStreamVgReaderTasks {
  SRWLatch lock;
  int64_t  streamVer;
  SArray*  taskList;       // SArray<SStreamTask*>
} SStreamVgReaderTasks;


typedef struct SStreamMgmtInfo {
  void*                  timer;
  void*                  dnode;
  int32_t                dnodeId;
  int32_t                snodeId;
  
//  SStorageAPI*           api;
  getMnodeEpset_f         getMnode;
  getDnodeId_f            getDnode;
  
  SStreamHbInfo          hb;

  bool                   hbReported;
  
  SRWLatch               vgLeadersLock;
  SArray*                vgLeaders;

  int8_t                 stmGrpIdx;
  SHashObj*              stmGrp[STREAM_MAX_GROUP_NUM];    // streamId => SStreamTasksInfo
  SHashObj*              taskMap;                         // streamId + taskId => SStreamTask*
  SHashObj*              vgroupMap;                       // vgId => SStreamVgReaderTasks

  SRWLatch               snodeLock;
  SArray*                snodeTasks;                      // SArray<SStreamTask*>
} SStreamMgmtInfo;

extern SStreamMgmtInfo gStreamMgmt;

int32_t streamTimerInit(void** ppTimer);
int32_t streamHbInit(SStreamHbInfo* pHb);
int32_t smDeployTasks(SStreamDeployActions* actions);
int32_t smUndeployTasks(SStreamUndeployActions* actions);
int32_t smStartTasks(SStreamStartActions* actions);
void smUndeployAllTasks(void);
void streamTmrStart(TAOS_TMR_CALLBACK fp, int32_t mseconds, void* pParam, void* pHandle, tmr_h* pTmrId, const char* pMsg);
int32_t stmBuildStreamsStatus(SArray** ppStatus, int32_t gid);
int32_t stmAddFetchStreamGid(void);
// int32_t stReaderTaskDeploy(SStreamReaderTask* pTask, const SStreamReaderDeployMsg* pMsg);
int32_t stReaderTaskUndeploy(SStreamReaderTask* pTask, const SStreamUndeployTaskMsg* pMsg, taskUndeplyCallback cb);
int32_t stReaderTaskExecute(SStreamReaderTask* pTask, SStreamMsg* pMsg);

#ifdef __cplusplus
}
#endif

#endif /* ifndef TDENGINE_STREAM_INT_H */
#endif /* USE_STREAM */
