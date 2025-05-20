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

#ifndef TDENGINE_STREAM_H
#define TDENGINE_STREAM_H

#ifdef __cplusplus
extern "C" {
#endif

#include "streamMsg.h"
#include "tlog.h"
#include "executor.h"

#define STREAM_HB_INTERVAL_MS 600

#define STREAM_MAX_GROUP_NUM  5
#define STREAM_MAX_THREAD_NUM 5
#define STREAM_RETURN_ROWS_NUM 10

#define STREAM_ACT_MIN_DELAY_MSEC (STREAM_MAX_GROUP_NUM * STREAM_HB_INTERVAL_MS)

typedef struct SStreamReaderTask {
  SStreamTask task;
  int8_t      triggerReader;
  void*       info;
} SStreamReaderTask;

typedef struct SStreamRunnerTaskExecution {
  const char        *pPlan;
  void              *pExecutor;
  void              *notifyEventSup;
  void              *pQueryPlan;
  SStreamRuntimeInfo runtimeInfo;
  char               tbname[TSDB_TABLE_NAME_LEN];
  void              *pSinkHandle;
} SStreamRunnerTaskExecution;

typedef struct SStreamRunnerTaskOutput {
  struct SSDataBlock *pBlock;
  char                outDbFName[TSDB_DB_FNAME_LEN];
  char                outSTbName[TSDB_TABLE_NAME_LEN];
  int8_t              outTblType;
  SArray             *outCols;  // array of SFieldWithOptions
  SArray             *outTags;  // array of SFieldWithOptions
  uint64_t            outStbUid;
  int32_t             outStbVersion;
} SStreamRunnerTaskOutput;

typedef struct SStreamRunnerTaskNotification {
  int8_t calcNotifyOnly;
  // notify options
  SArray* pNotifyAddrUrls;
  int32_t notifyErrorHandle;
} SStreamRunnerTaskNotification;

typedef struct SStreamRunnerTaskExecMgr {
  SList*        pFreeExecs;
  SList*        pRunningExecs;
  TdThreadMutex lock;
  bool          exit;
} SStreamRunnerTaskExecMgr;

typedef struct SStreamRunnerTask {
  SStreamTask        task;
  SStreamRunnerTaskExecMgr      execMgr;
  SStreamRunnerTaskOutput       output;
  SStreamRunnerTaskNotification notification;
  const char*                   pPlan;
  int32_t                       parallelExecutionNun;
  void*                         pMsgCb;
  void*                         pSubTableExpr;
  SArray*                       forceOutCols;  // array of SStreamOutCol, only available when forceOutput is true
  bool                          topTask;
  taskUndeplyCallback           undeployCb;
  void*                         undeployParam;
} SStreamRunnerTask;

typedef struct SStreamCacheReadInfo {
  SStreamTask  taskInfo;
  int64_t      gid;
  TSKEY        start;
  TSKEY        end;
  SSDataBlock *pBlock;
} SStreamCacheReadInfo;

#define STREAM_GID(_streamId) ((uint64_t)(_streamId) % STREAM_MAX_GROUP_NUM)

// clang-format off
#define stFatal(...) do { if (stDebugFlag & DEBUG_FATAL) { taosPrintLog("STM FATAL ", DEBUG_FATAL, 255,         __VA_ARGS__); }} while(0)
#define stError(...) do { if (stDebugFlag & DEBUG_ERROR) { taosPrintLog("STM ERROR ", DEBUG_ERROR, 255,         __VA_ARGS__); }} while(0)
#define stWarn(...)  do { if (stDebugFlag & DEBUG_WARN)  { taosPrintLog("STM WARN  ", DEBUG_WARN,  255,         __VA_ARGS__); }} while(0)
#define stInfo(...)  do { if (stDebugFlag & DEBUG_INFO)  { taosPrintLog("STM INFO  ", DEBUG_INFO,  255,         __VA_ARGS__); }} while(0)
#define stDebug(...) do { if (stDebugFlag & DEBUG_DEBUG) { taosPrintLog("STM DEBUG ", DEBUG_DEBUG, stDebugFlag, __VA_ARGS__); }} while(0)
#define stDebugL(...) do { if (stDebugFlag & DEBUG_DEBUG) { taosPrintLongString("STM DEBUG ", DEBUG_DEBUG, stDebugFlag, __VA_ARGS__); }} while(0)
#define stTrace(...) do { if (stDebugFlag & DEBUG_TRACE) { taosPrintLog("STM TRACE ", DEBUG_TRACE, stDebugFlag, __VA_ARGS__); }} while(0)
// clang-format on

#define ST_TASK_FLOG(param, ...)                                                                               \
  stFatal("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRIx64 " " param,               \
          gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
          ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
          __VA_ARGS__)
#define ST_TASK_ELOG(param, ...)                                                                               \
  stError("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRIx64 " " param,               \
          gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
          ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
          __VA_ARGS__)
#define ST_TASK_WLOG(param, ...)                                                                              \
  stWarn("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRIx64 " " param,               \
         gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
         ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
         __VA_ARGS__)
#define ST_TASK_ILOG(param, ...)                                                                              \
  stInfo("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRIx64 " " param,               \
         gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
         ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
         __VA_ARGS__)
#define ST_TASK_DLOG(param, ...)                                                                               \
  stDebug("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRIx64 " " param,               \
          gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
          ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
          __VA_ARGS__)
#define ST_TASK_DLOGL(param, ...)                                                                               \
    stDebugL("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRIx64 " " param,               \
            gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
            ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
            __VA_ARGS__)
#define ST_TASK_TLOG(param, ...)                                                                               \
  stTrace("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRIx64 " " param,               \
          gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
          ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
          __VA_ARGS__)

#define mstFatal(param, ...) stFatal("STREAM:%" PRIx64 " " param, streamId, __VA_ARGS__)
#define mstError(param, ...) stError("STREAM:%" PRIx64 " " param, streamId, __VA_ARGS__)
#define mstWarn(param, ...)  stWarn("STREAM:%" PRIx64 " " param, streamId, __VA_ARGS__)
#define mstInfo(param, ...)  stInfo("STREAM:%" PRIx64 " " param, streamId, __VA_ARGS__)
#define mstDebug(param, ...) stDebug("STREAM:%" PRIx64 " " param, streamId, __VA_ARGS__)
#define mstDebugL(param, ...) stDebugL("STREAM:%" PRIx64 " " param, streamId, __VA_ARGS__)
#define mstTrace(param, ...) stTrace("STREAM:%" PRIx64 " " param, streamId, __VA_ARGS__)

int32_t streamGetThreadIdx(int32_t threadNum, int64_t streamGId);
void    streamRemoveVnodeLeader(int32_t vgId);
void    streamAddVnodeLeader(int32_t vgId);
void    streamSetSnodeEnabled(void);
void    streamSetSnodeDisabled(void);
int32_t streamHbProcessRspMsg(SMStreamHbRspMsg *pRsp);
int32_t streamHbHandleRspErr(int32_t errCode, int64_t currTs);
int32_t streamInit(void *pDnode, getDnodeId_f getDnode, getMnodeEpset_f getMnode);
void    streamCleanup(void);
int32_t streamGetTask(int64_t streamId, int64_t taskId, SStreamTask** ppTask);
int32_t streamTriggerKickCalc();
int32_t streamTriggerProcessRsp(SStreamTask *pTask, SRpcMsg *pRsp);

#define STREAM_TRIGGER_MAX_WIN_NUM_PER_REQUEST 4096

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_H
