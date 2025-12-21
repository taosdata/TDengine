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
#define STREAM_RETURN_ROWS_NUM  4096
#define STREAM_RETURN_BLOCK_NUM 4096
// #define STREAM_RETURN_ROWS_NUM_NEW 1000000

#define STREAM_ACT_MIN_DELAY_MSEC (STREAM_MAX_GROUP_NUM * STREAM_HB_INTERVAL_MS)

#define STREAM_FLAG_TRIGGER_READER  (1 << 0)
#define STREAM_FLAG_TOP_RUNNER      (1 << 1)
#define STREAM_FLAG_REDEPLOY_RUNNER (1 << 2)

#define STREAM_IS_TRIGGER_READER(_flags) ((_flags) & STREAM_FLAG_TRIGGER_READER)
#define STREAM_IS_TOP_RUNNER(_flags) ((_flags) & STREAM_FLAG_TOP_RUNNER)
#define STREAM_IS_REDEPLOY_RUNNER(_flags) ((_flags) & STREAM_FLAG_REDEPLOY_RUNNER)

#define STREAM_CLR_FLAG(st, f) (st) &= (~f)

#define STREAM_CALC_REQ_MAX_WIN_NUM 40960

typedef struct SStreamReaderTask {
  SStreamTask task;
  int8_t      triggerReader;
  void*       info;
} SStreamReaderTask;


typedef struct SSTriggerAHandle {
  int64_t streamId;
  int64_t taskId;
  int64_t sessionId;
  void*   param;
} SSTriggerAHandle;


typedef struct SStreamRunnerTaskExecution {
  const char        *pPlan;
  void              *pExecutor;
  void              *notifyEventSup;
  void              *pQueryPlan;
  SStreamRuntimeInfo runtimeInfo;
  char               tbname[TSDB_TABLE_NAME_LEN];
  void              *pSinkHandle;
  SSDataBlock       *pOutBlock;
} SStreamRunnerTaskExecution;

typedef struct SStreamRunnerTaskOutput {
  char                outDbFName[TSDB_DB_FNAME_LEN];
  char                outSTbName[TSDB_TABLE_NAME_LEN];
  int8_t              outTblType;
  SArray             *outCols;  // array of SFieldWithOptions
  SArray             *outTags;  // array of SFieldWithOptions
  uint64_t            outStbUid;
  int32_t             outStbVersion;
  SNodeList          *pTagValExprs;
} SStreamRunnerTaskOutput;

typedef struct SStreamRunnerTaskNotification {
  int8_t calcNotifyOnly;
  // notify options
  SArray* pNotifyAddrUrls;
} SStreamRunnerTaskNotification;

typedef struct SStreamRunnerTaskExecMgr {
  SList*        pFreeExecs;
  SList*        pRunningExecs;
  TdThreadMutex lock;
  bool          lockInited;
  int32_t       execBuildNum;
} SStreamRunnerTaskExecMgr;

typedef struct SStreamTagInfo {
  SStreamGroupValue val;
  char tagName[TSDB_COL_NAME_LEN];
} SStreamTagInfo;

typedef struct SStreamRunnerTask {
  SStreamTask                   task;
  SStreamRunnerTaskExecMgr      execMgr;
  SStreamRunnerTaskOutput       output;
  SStreamRunnerTaskNotification notification;
  const char                   *pPlan;
  int32_t                       parallelExecutionNun;
  SMsgCb                        msgCb;
  void                         *pMsgCb;
  void                         *pWorkerCb;
  void                         *pSubTableExpr;
  SArray                       *forceOutCols;  // array of SStreamOutCol, only available when forceOutput is true
  bool                          topTask;
  bool                          lowLatencyCalc;
  int8_t                        vtableDeployGot;
  char                         *streamName;
  int32_t                       addOptions;
  int64_t                       mgmtReqId;
} SStreamRunnerTask;

typedef struct SStreamCacheReadInfo {
  SStreamTask  taskInfo;
  int64_t      gid;
  TSKEY        start;
  TSKEY        end;
  SSDataBlock *pBlock;
} SStreamCacheReadInfo;

#define STRIGGER_RECALC_RANGE_MAX_HOURS 24

#define STM_CHK_SET_ERROR(CMD)        \
    do {                              \
      code = (CMD);                   \
      if (code < TSDB_CODE_SUCCESS) { \
        atomic_store_32(&((SStreamTask *)pTask)->errorCode, code);  \
        atomic_store_32((int32_t*)&((SStreamTask *)pTask)->status, STREAM_STATUS_FAILED);         \
        ST_TASK_ELOG("task failed in %s at line %d", __FUNCTION__, __LINE__);       \
      }                               \
    } while (0)


#define STM_CHK_SET_ERROR_EXIT(CMD)        \
    do {                              \
      code = (CMD);                   \
      if (code < TSDB_CODE_SUCCESS) { \
        lino = __LINE__;              \
        atomic_store_32(&((SStreamTask *)pTask)->errorCode, code);  \
        atomic_store_32((int32_t*)&((SStreamTask *)pTask)->status, STREAM_STATUS_FAILED);         \
        ST_TASK_ELOG("task failed in %s at line %d", __FUNCTION__, __LINE__);       \
        goto _exit;                   \
      }                               \
    } while (0)

#define STREAM_GID(_streamId) ((uint64_t)(_streamId) % STREAM_MAX_GROUP_NUM)

#define STREAM_CHECK_RET_GOTO(CMD) \
  code = (CMD);                    \
  if (code != TSDB_CODE_SUCCESS) { \
    lino = __LINE__;               \
    goto end;                      \
  }

#define STREAM_CHECK_NULL_GOTO(CMD, ret) \
  if ((CMD) == NULL) {                   \
    code = ret;                          \
    lino = __LINE__;                     \
    goto end;                            \
  }

#define STREAM_CHECK_CONDITION_GOTO(CMD, ret) \
  if (CMD) {                                  \
    code = ret;                               \
    lino = __LINE__;                          \
    goto end;                                 \
  }

#define STREAM_PRINT_LOG_END(code, lino)                                       \
  if (code != 0) {                                                             \
    stError("%s failed at line %d since %s", __func__, lino, tstrerror(code)); \
  } else {                                                                     \
    stDebug("%s done success", __func__);                                      \
  }

#define STREAM_PRINT_LOG_END_WITHID(code, lino)                                     \
  if (code != 0) {                                                                  \
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code)); \
  } else {                                                                          \
    ST_TASK_DLOG("%s done success", __func__);                                      \
  }

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
  stFatal("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRId64 " " param,               \
          gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
          ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
          __VA_ARGS__)
#define ST_TASK_ELOG(param, ...)                                                                               \
  stError("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRId64 " " param,               \
          gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
          ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
          __VA_ARGS__)
#define ST_TASK_WLOG(param, ...)                                                                              \
  stWarn("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRId64 " " param,               \
         gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
         ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
         __VA_ARGS__)
#define ST_TASK_ILOG(param, ...)                                                                              \
  stInfo("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRId64 " " param,               \
         gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
         ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
         __VA_ARGS__)
#define ST_TASK_DLOG(param, ...)                                                                               \
  stDebug("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRId64 " " param,               \
          gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
          ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
          __VA_ARGS__)
#define ST_TASK_DLOGL(param, ...)                                                                               \
    stDebugL("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRId64 " " param,               \
            gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
            ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
            __VA_ARGS__)
#define ST_TASK_TLOG(param, ...)                                                                               \
  stTrace("TYPE: %s, NODE:%d, STREAM:%" PRIx64 ", TASK:%" PRIx64 ",SID:%" PRId64 ", SESSION:%" PRId64 " " param,               \
          gStreamTaskTypeStr[((SStreamTask *)pTask)->type], ((SStreamTask *)pTask)->nodeId,                    \
          ((SStreamTask *)pTask)->streamId, ((SStreamTask *)pTask)->taskId, ((SStreamTask *)pTask)->seriousId, ((SStreamTask *)pTask)->sessionId, \
          __VA_ARGS__)

#define stsFatal(param, ...) stFatal("%" PRIx64 " " param, streamId, __VA_ARGS__)
#define stsError(param, ...) stError("%" PRIx64 " " param, streamId, __VA_ARGS__)
#define stsWarn(param, ...)  stWarn("%" PRIx64 " " param, streamId, __VA_ARGS__)
#define stsInfo(param, ...)  stInfo("%" PRIx64 " " param, streamId, __VA_ARGS__)
#define stsDebug(param, ...) stDebug("%" PRIx64 " " param, streamId, __VA_ARGS__)
#define stsDebugL(param, ...) stDebugL("%" PRIx64 " " param, streamId, __VA_ARGS__)
#define stsTrace(param, ...) stTrace("%" PRIx64 " " param, streamId, __VA_ARGS__)

int32_t streamGetThreadIdx(int32_t threadNum, int64_t streamGId);
void    streamRemoveVnodeLeader(int32_t vgId);
void    streamAddVnodeLeader(int32_t vgId);
void    streamSetSnodeEnabled(  SMsgCb* msgCb);
void    streamSetSnodeDisabled(bool cleanup);
int32_t streamHbProcessRspMsg(SMStreamHbRspMsg *pRsp);
int32_t streamHbHandleRspErr(int32_t errCode, int64_t currTs);
int32_t streamInit(void *pDnode, getDnodeId_f getDnode, getMnodeEpset_f getMnode, getSynEpset_f getSynEpset);
void    streamCleanup(void);
int32_t streamAcquireTask(int64_t streamId, int64_t taskId, SStreamTask** ppTask, void** ppAddr);
void    streamReleaseTask(void* taskAddr);
int32_t streamAcquireTriggerTask(int64_t streamId, SStreamTask** ppTask, void** ppAddr);
void    streamHandleTaskError(int64_t streamId, int64_t taskId, int32_t errCode);
void smUndeploySnodeTasks(bool cleanup);
int32_t streamWriteCheckPoint(int64_t streamId, void* data, int64_t dataLen);
int32_t streamReadCheckPoint(int64_t streamId, void** data, int64_t* dataLen);
void    streamDeleteCheckPoint(int64_t streamId);
int32_t streamSyncWriteCheckpoint(int64_t streamId, SEpSet* epSet, void* data, int64_t dataLen);
int32_t streamSyncDeleteCheckpoint(int64_t streamId, SEpSet* epSet);
int32_t streamCheckpointSetReady(int64_t streamId);
// int32_t streamCheckpointSetNotReady(int64_t streamId);
// bool    streamCheckpointIsReady(int64_t streamId);
int32_t streamSyncAllCheckpoints(SEpSet* epSet);
void    streamDeleteAllCheckpoints();
void    smUndeploySnodeTasks(bool cleanup);
int32_t stTriggerTaskProcessRsp(SStreamTask *pTask, SRpcMsg *pRsp, int64_t *pErrTaskId);
int32_t stTriggerTaskGetStatus(SStreamTask *pTask, SSTriggerRuntimeStatus *pStatus);
int32_t stTriggerTaskGetDelay(SStreamTask *pTask, int64_t *pDelay, bool *pFillHisFinished);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STREAM_H
