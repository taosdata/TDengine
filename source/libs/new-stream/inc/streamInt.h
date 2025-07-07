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
#ifndef TDENGINE_STREAM_INT_H
#define TDENGINE_STREAM_INT_H


#include "executor.h"
#include "query.h"
#include "trpc.h"
#include "stream.h"
#include "tref.h"
#include "ttimer.h"
#include "streamRunner.h"
#include "streamTriggerTask.h"

#ifdef __cplusplus
extern "C" {
#endif

#define STREAM_GRP_STREAM_NUM             20
#define STREAM_HB_ERR_HANDLE_MAX_DELAY    300000

typedef struct SStreamHbInfo {
  int32_t      lastErrCode;
  int64_t      lastErrTs;
  tmr_h        hbTmr;
  SStreamHbMsg hbMsg;
} SStreamHbInfo;

typedef struct SStreamInfo {
  SRWLatch            lock;
  int32_t             taskNum;
  
  SList*              readerList;        // SStreamReaderTask
  int64_t             triggerTaskId;
  SStreamTriggerTask* triggerTask;
  SList*              runnerList;        // SArray<SStreamRunnerTask>

  SRWLatch            undeployLock;

  SArray*             undeployReaders;        // SArray<taskId>
  int64_t             undeployTriggerId;
  SArray*             undeployRunners;        // SArray<taskId>
} SStreamInfo;

typedef struct SStreamVgReaderTasks {
  SRWLatch lock;
  int64_t  streamVer;
  SArray*  taskList;       // SArray<SStreamTask*>
} SStreamVgReaderTasks;


typedef struct SStreamMgmtInfo {
  void*                  timer;
  void*                  dnode;
  bool                   snodeEnabled;
  SRWLatch               snodeLock;
  SNodeEpSet             snodeLeaders[2];
  SNodeEpSet             snodeReplica;
  
//  SStorageAPI*           api;
  getMnodeEpset_f         getMnode;
  getDnodeId_f            getDnode;
  getSynEpset_f           getSynEpset;
  SStreamHbInfo           hb;

  bool                   hbReported;
  
  SRWLatch               vgLeadersLock;
  SArray*                vgLeaders;

  int8_t                 stmGrpIdx;
  SHashObj*              stmGrp[STREAM_MAX_GROUP_NUM];    // streamId => SStreamInfo
  SHashObj*              taskMap;                         // streamId + taskId => SStreamTask*
  SHashObj*              vgroupMap;                       // vgId => SStreamVgReaderTasks

  SArray*                snodeTasks;                      // SArray<SStreamTask*>
} SStreamMgmtInfo;

extern SStreamMgmtInfo gStreamMgmt;

int32_t streamTimerInit(void** ppTimer);
int32_t streamHbInit(SStreamHbInfo* pHb);
int32_t smDeployTasks(SStmStreamDeploy* pDeploy);
int32_t smUndeployTasks(SStreamUndeployActions* actions);
int32_t smHandleMgmtRsp(SStreamMgmtRsps* rsps);
int32_t smStartTasks(SStreamStartActions* actions);
void smUndeployAllTasks(void);
void streamTmrStart(TAOS_TMR_CALLBACK fp, int32_t mseconds, void* pParam, void* pHandle, tmr_h* pTmrId, const char* pMsg);
int32_t stmBuildHbStreamsStatusReq(SStreamHbMsg* pMsg);
int32_t stmAddFetchStreamGid(void);

// initialize global request limit of stream triggers
int32_t streamTriggerEnvInit();
void    streamTriggerEnvStop(); // todo(kjq): call it when stop dnode
void    streamTriggerEnvCleanup();

int32_t stReaderTaskDeploy(SStreamReaderTask* pTask, const SStreamReaderDeployMsg* pMsg);
int32_t stReaderTaskUndeploy(SStreamReaderTask** ppTask, bool force);
int32_t stReaderTaskExecute(SStreamReaderTask* pTask, SStreamMsg* pMsg);

void smHandleRemovedTask(SStreamInfo* pStream, int64_t streamId, int32_t gid, bool isReader);
void smUndeployVgTasks(int32_t vgId);
int32_t smDeployStreams(SStreamDeployActions* actions);
void stmDestroySStreamInfo(void* param);
void stmDestroySStreamMgmtReq(SStreamMgmtReq* pReq);
int32_t streamBuildStateNotifyContent(ESTriggerEventType eventType, int16_t dataType, const char* pFromState,
                                      const char* pToState, char** ppContent);
int32_t streamBuildEventNotifyContent(const SSDataBlock* pInputBlock, const SNodeList* pCondCols, int32_t rowIdx,
                                      char** ppContent);
int32_t streamBuildBlockResultNotifyContent(const SSDataBlock* pBlock, char** ppContent, const SArray* pFields);
int32_t streamSendNotifyContent(SStreamTask* pTask, const char* streamName, int32_t triggerType, int64_t groupId,
                                const SArray* pNotifyAddrUrls, int32_t errorHandle, const SSTriggerCalcParam* pParams,
                                int32_t nParam);

int32_t readStreamDataCache(int64_t streamId, int64_t taskId, int64_t sessionId, int64_t groupId, TSKEY start,
                            TSKEY end, void*** pppIter);
void streamTimerCleanUp();
void smRemoveTaskPostCheck(int64_t streamId, SStreamInfo* pStream, bool* isLastTask);
void streamTmrStop(tmr_h tmrId);

#ifdef __cplusplus
}
#endif
#endif
