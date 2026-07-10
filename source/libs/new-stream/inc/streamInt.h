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
  int8_t              destroyed;
  
  SList*              readerList;        // SStreamReaderTask
  int64_t             triggerTaskId;
  SList*              triggerList;       // SStreamTriggerTask
  SList*              runnerList;        // SStreamRunnerTask

  SRWLatch            undeployLock;

  SArray*             undeployReaders;        // SArray<taskId+seriousId>
  SArray*             undeployTriggers;       // SArray<taskId+seriousId>
  SArray*             undeployRunners;        // SArray<taskId+seriousId>
} SStreamInfo;

typedef struct SStreamVgReaderTasks {
  SRWLatch lock;
  int8_t   inactive;
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
  SMsgCb                 msgCb;
  
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

  // Cleanup gating for stream timer callbacks.
  //   tmrStopped:  set to 1 by streamCleanup before tearing down shared state.
  //                streamTmrStart() checks this flag and refuses to (re-)arm
  //                any stream timer once cleanup has been signaled, so timer
  //                callbacks can no longer be scheduled.
  //   tmrInflight: bumped on entry of every stream timer callback (via the
  //                STREAM_TMR_CB_ENTER / STREAM_TMR_CB_LEAVE macros below)
  //                and decremented at exit; streamTmrWaitAllCallbacks()
  //                spins until it observes zero so that streamCleanup never
  //                frees state a callback is still dereferencing.  The
  //                increment MUST happen before the tmrStopped load
  //                (publish-before-check) to close the TOCTOU window with
  //                the cleaner; the macros below encode that order, do not
  //                hand-roll it at the call site.
  volatile int8_t        tmrStopped;
  volatile int32_t       tmrInflight;
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

int32_t stTriggerTaskEnvInit();
void    stTriggerTaskEnvCleanup();

int32_t stReaderTaskDeploy(SStreamReaderTask* pTask, const SStreamReaderDeployMsg* pMsg);
int32_t stReaderTaskUndeploy(SStreamReaderTask** ppTask, bool force);
int32_t stReaderTaskExecute(SStreamReaderTask* pTask, SStreamMsg* pMsg);

void smHandleRemovedTask(SStreamInfo* pStream, int64_t streamId, int32_t gid, EStreamTaskType type, SArray* pUndeployList, SList* pTaskList);
void smUndeployVgTasks(int32_t vgId, bool cleanup);
int32_t smDeployStreams(SStreamDeployActions* actions);
void stmDestroySStreamInfo(void* param);
int32_t streamBuildMultiStateNotifyContent(ESTriggerEventType eventType, const SArray* pStateCols,
                                           const SArray* pFromStates, const bool* pFromDefined,
                                           const SArray* pToStates, const bool* pToDefined,
                                           char** ppContent);
int32_t streamBuildIdleNotifyContent(ESTriggerEventType eventType, int64_t idleDurationMs, char** ppContent);
int32_t streamBuildEventNotifyContent(const SSDataBlock* pInputBlock, const SNodeList* pCondCols, int32_t rowIdx,
                                      int32_t condIdx, int32_t winIdx, int64_t groupId, int64_t windowStart,
                                      int64_t parentWindowStart, char** ppContent);
int32_t streamBuildBlockResultNotifyContent(const SStreamRunnerTask* pTask, const SSDataBlock* pBlock, char** ppContent,
                                            const SArray* pFields, const int32_t startRow, const int32_t endRow,
                                            bool* pHasNotifyRows);
int32_t streamSendNotifyContent(SStreamTask* pTask, const char* streamName, const char* tableName, int32_t triggerType,
                                int64_t groupId, const SArray* pNotifyAddrUrls, int32_t addOptions,
                                const SSTriggerCalcParam* pParams, int32_t nParam);

int32_t readStreamDataCache(int64_t streamId, int64_t taskId, int64_t sessionId, int64_t groupId, TSKEY start,
                            TSKEY end, void*** pppIter);
void streamTimerCleanUp();
void streamTmrWaitAllCallbacks(void);
void smRemoveTaskPostCheck(int64_t streamId, SStreamInfo* pStream, bool* isLastTask);
void streamTmrStop(tmr_h tmrId);
void smEnableVgDeploy(int32_t vgId);
void smUndeployStreamTriggerTasks(SStreamInfo* pStream, int64_t streamId);

// Stream timer callback entry/leave protocol.
// MUST be used by every callback installed via streamTmrStart so that
// streamCleanup can drain in-flight callbacks before freeing shared state.
// See SStreamMgmtInfo.tmrStopped / tmrInflight for the protocol overview.
//
// Ordering is publish-before-check (inflight++ then load tmrStopped) to
// close the TOCTOU window with streamTmrWaitAllCallbacks().  Do NOT hand
// roll this protocol at the call site; adding a new stream timer callback
// just means wrapping its body in the two macros below.
//
// Usage:
//   static void myCallback(void *param, void *tmrId) {
//     STREAM_TMR_CB_ENTER("my-cb");   // returns early if cleanup started
//     ... callback body, may self re-arm via streamTmrStart ...
//     STREAM_TMR_CB_LEAVE();           // MUST be the last statement
//   }
#define STREAM_TMR_CB_ENTER(_name)                                  \
  do {                                                              \
    atomic_add_fetch_32(&gStreamMgmt.tmrInflight, 1);               \
    if (atomic_load_8(&gStreamMgmt.tmrStopped)) {                   \
      atomic_sub_fetch_32(&gStreamMgmt.tmrInflight, 1);             \
      stTrace("%s skipped: timer stopped", (_name));                \
      return;                                                       \
    }                                                               \
  } while (0)

#define STREAM_TMR_CB_LEAVE() \
  (void)atomic_sub_fetch_32(&gStreamMgmt.tmrInflight, 1)

#ifdef __cplusplus
}
#endif
#endif
