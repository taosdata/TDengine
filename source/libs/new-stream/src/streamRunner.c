#include "streamRunner.h"
#include "cmdnodes.h"
#include "dataSink.h"
#include "dataSinkMgt.h"
#include "executor.h"
#include "osMemory.h"
#include "scalar.h"
#include "stream.h"
#include "streamInt.h"
#include "streamTaskStats.h"
#include "taoserror.h"
#include "tarray.h"
#include "tcommon.h"
#include "tdatablock.h"
#include "ttime.h"

static void stRunnerRecordInput(void* param, uint64_t rows, uint64_t blocks) {
  stTaskStatsRecordRunnerInput(param, rows, blocks, streamTaskGetMonotonicUs());
}

typedef struct {
  int64_t  startMonoUs;
  uint64_t durationUs;
  bool     active;
} SRunnerBlockingStats;

static void stRunnerRecordBlocking(void* param, bool blocking) {
  SRunnerBlockingStats* pStats = param;
  int64_t               nowMonoUs = streamTaskGetMonotonicUs();
  if (blocking) {
    if (!pStats->active) {
      pStats->startMonoUs = nowMonoUs;
      pStats->active = true;
    }
    return;
  }

  if (pStats->active) {
    if (nowMonoUs >= pStats->startMonoUs) {
      uint64_t durationUs = (uint64_t)(nowMonoUs - pStats->startMonoUs);
      pStats->durationUs = UINT64_MAX - pStats->durationUs < durationUs ? UINT64_MAX : pStats->durationUs + durationUs;
    }
    pStats->active = false;
  }
}

#define STREAM_RUNNER_PERIOD_LOG_BUFFER_SIZE 4096
#define STREAM_RUNNER_PERIOD_LOG_FORMAT                                                                               \
  "record=task_period task_type=runner stream_id=%" PRId64 " task_id=%" PRId64 " serious_id=%" PRId64                 \
  " node_id=%d status=%s stats_start_at=%" PRId64 " uptime_ms=%" PRId64 " stats_window_ms=%" PRId64                   \
  " calc_request_count=%" PRIu64 " logical_window_count=%" PRIu64 " input_rows=%" PRIu64 " input_blocks=%" PRIu64     \
  " output_rows=%" PRIu64 " output_blocks=%" PRIu64 " no_result_window_count=%" PRIu64 " calc_failure_count=%" PRIu64 \
  " sink_failure_count=%" PRIu64 " notify_failure_count=%" PRIu64                                                     \
  " input_rows_per_sec=%.3f input_blocks_per_sec=%.3f"                                                                \
  " output_rows_per_sec=%.3f output_blocks_per_sec=%.3f calc_duration_samples=%" PRIu64                               \
  " calc_duration_avg_ms=%s calc_duration_max_ms=%s calc_duration_lifetime_max_ms=%s"                                 \
  " calc_duration_lifetime_max_at=%s result_latency_samples=%" PRIu64                                                 \
  " result_latency_avg_ms=%s result_latency_max_ms=%s result_latency_lifetime_max_ms=%s"                              \
  " result_latency_lifetime_max_at=%s free_exec_count=%s running_exec_count=%s parallel_execution_limit=%s"           \
  " last_calc_at=%s last_result_at=%s last_output_at=%s stats_overflow=%s"

static int32_t stRunnerFormatOptionalI64(char* pBuffer, int32_t bufferSize, bool valid, int64_t value) {
  int32_t len = valid ? snprintf(pBuffer, bufferSize, "%" PRId64, value) : snprintf(pBuffer, bufferSize, "NA");
  return len < 0 || len >= bufferSize ? TSDB_CODE_OUT_OF_BUFFER : TSDB_CODE_SUCCESS;
}

static int32_t stRunnerFormatOptionalMs(char* pBuffer, int32_t bufferSize, bool valid, double valueMs) {
  int32_t len = valid ? snprintf(pBuffer, bufferSize, "%.3f", valueMs) : snprintf(pBuffer, bufferSize, "NA");
  return len < 0 || len >= bufferSize ? TSDB_CODE_OUT_OF_BUFFER : TSDB_CODE_SUCCESS;
}

int32_t stRunnerTaskLogStats(SStreamRunnerTask* pTask, const SStreamTaskPeriodSnapshot* pSnapshot) {
  if (pTask == NULL || pSnapshot == NULL || pTask->task.type != STREAM_RUNNER_TASK ||
      pSnapshot->taskType != STREAM_RUNNER_TASK || pSnapshot->statsWindowMs <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }

  bool         execSnapshotValid = false;
  int32_t      freeExecCount = 0;
  int32_t      runningExecCount = 0;
  int32_t      parallelExecutionLimit = 0;
  SStreamTask* pOwner = NULL;
  void*        taskAddr = NULL;
  // Heartbeat holds pStream->lock, which keeps the list node and base identity alive. The task-map handle protects
  // execMgr through formatting and logging because its release may complete a pending undeploy.
  if (gStreamMgmt.taskMap != NULL &&
      streamAcquireTask(pTask->task.streamId, pTask->task.taskId, &pOwner, &taskAddr) == TSDB_CODE_SUCCESS &&
      pOwner == &pTask->task) {
    if (pTask->execMgr.lockInited && taosThreadMutexLock(&pTask->execMgr.lock) == TSDB_CODE_SUCCESS) {
      if (pTask->execMgr.pFreeExecs != NULL && pTask->execMgr.pRunningExecs != NULL) {
        freeExecCount = listNEles(pTask->execMgr.pFreeExecs);
        runningExecCount = listNEles(pTask->execMgr.pRunningExecs);
        parallelExecutionLimit = pTask->parallelExecutionNun;
        execSnapshotValid = true;
      }
      if (taosThreadMutexUnlock(&pTask->execMgr.lock) != TSDB_CODE_SUCCESS) execSnapshotValid = false;
    }
  } else if (taskAddr != NULL) {
    streamReleaseTask(taskAddr);
    taskAddr = NULL;
  }

  const SStreamRunnerPeriodStats*   pPeriod = &pSnapshot->period.runner;
  const SStreamRunnerPeriodStats*   pCumulative = &pSnapshot->cumulative.runner;
  const SStreamRunnerGaugeSnapshot* pGauges = &pSnapshot->runnerGauges;
  char                              calcAvgMs[32] = {0};
  char                              calcMaxMs[32] = {0};
  char                              calcLifetimeMaxMs[32] = {0};
  char                              calcLifetimeMaxAt[32] = {0};
  char                              resultAvgMs[32] = {0};
  char                              resultMaxMs[32] = {0};
  char                              resultLifetimeMaxMs[32] = {0};
  char                              resultLifetimeMaxAt[32] = {0};
  char                              lastCalcAt[32] = {0};
  char                              lastResultAt[32] = {0};
  char                              lastOutputAt[32] = {0};
  char                              freeExecCountValue[32] = {0};
  char                              runningExecCountValue[32] = {0};
  char                              parallelExecutionLimitValue[32] = {0};

  int32_t code = stRunnerFormatOptionalMs(
      calcAvgMs, sizeof(calcAvgMs), pPeriod->calcDuration.samples > 0,
      pPeriod->calcDuration.samples > 0
          ? (double)pPeriod->calcDuration.totalUs / (double)pPeriod->calcDuration.samples / 1000.0
          : 0.0);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stRunnerFormatOptionalMs(calcMaxMs, sizeof(calcMaxMs), pPeriod->calcDuration.samples > 0,
                                  (double)pPeriod->calcDuration.maxUs / 1000.0);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stRunnerFormatOptionalMs(calcLifetimeMaxMs, sizeof(calcLifetimeMaxMs), pCumulative->calcDuration.samples > 0,
                                  (double)pCumulative->calcDuration.maxUs / 1000.0);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stRunnerFormatOptionalI64(calcLifetimeMaxAt, sizeof(calcLifetimeMaxAt), pCumulative->calcDuration.samples > 0,
                                   pCumulative->calcDuration.maxAtMs);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stRunnerFormatOptionalMs(
      resultAvgMs, sizeof(resultAvgMs), pPeriod->resultLatency.samples > 0,
      pPeriod->resultLatency.samples > 0
          ? (double)pPeriod->resultLatency.totalUs / (double)pPeriod->resultLatency.samples / 1000.0
          : 0.0);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stRunnerFormatOptionalMs(resultMaxMs, sizeof(resultMaxMs), pPeriod->resultLatency.samples > 0,
                                  (double)pPeriod->resultLatency.maxUs / 1000.0);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code =
      stRunnerFormatOptionalMs(resultLifetimeMaxMs, sizeof(resultLifetimeMaxMs), pCumulative->resultLatency.samples > 0,
                               (double)pCumulative->resultLatency.maxUs / 1000.0);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stRunnerFormatOptionalI64(resultLifetimeMaxAt, sizeof(resultLifetimeMaxAt),
                                   pCumulative->resultLatency.samples > 0, pCumulative->resultLatency.maxAtMs);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stRunnerFormatOptionalI64(lastCalcAt, sizeof(lastCalcAt), pGauges->lastCalcAtMs > 0, pGauges->lastCalcAtMs);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stRunnerFormatOptionalI64(lastResultAt, sizeof(lastResultAt), pGauges->lastResultAtMs > 0,
                                   pGauges->lastResultAtMs);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stRunnerFormatOptionalI64(lastOutputAt, sizeof(lastOutputAt), pGauges->lastOutputAtMs > 0,
                                   pGauges->lastOutputAtMs);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stRunnerFormatOptionalI64(freeExecCountValue, sizeof(freeExecCountValue), execSnapshotValid, freeExecCount);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stRunnerFormatOptionalI64(runningExecCountValue, sizeof(runningExecCountValue), execSnapshotValid,
                                   runningExecCount);
  if (code != TSDB_CODE_SUCCESS) goto _exit;
  code = stRunnerFormatOptionalI64(parallelExecutionLimitValue, sizeof(parallelExecutionLimitValue), execSnapshotValid,
                                   parallelExecutionLimit);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  const double seconds = (double)pSnapshot->statsWindowMs / 1000.0;
  const char*  pStatus = pTask->task.status >= STREAM_STATUS_UNDEPLOYED && pTask->task.status <= STREAM_STATUS_DROPPING
                             ? gStreamStatusStr[pTask->task.status]
                             : "Unknown";
  char         line[STREAM_RUNNER_PERIOD_LOG_BUFFER_SIZE] = {0};
  int32_t      len = snprintf(
      line, sizeof(line), STREAM_RUNNER_PERIOD_LOG_FORMAT, pTask->task.streamId, pTask->task.taskId,
      pTask->task.seriousId, pTask->task.nodeId, pStatus, pSnapshot->statsStartAtMs, pSnapshot->uptimeMs,
      pSnapshot->statsWindowMs, pPeriod->calcRequestCount, pPeriod->logicalWindowCount, pPeriod->inputRows,
      pPeriod->inputBlocks, pPeriod->outputRows, pPeriod->outputBlocks, pPeriod->noResultWindowCount,
      pPeriod->calcFailureCount, pPeriod->sinkFailureCount, pPeriod->notifyFailureCount,
      (double)pPeriod->inputRows / seconds, (double)pPeriod->inputBlocks / seconds,
      (double)pPeriod->outputRows / seconds, (double)pPeriod->outputBlocks / seconds, pPeriod->calcDuration.samples,
      calcAvgMs, calcMaxMs, calcLifetimeMaxMs, calcLifetimeMaxAt, pPeriod->resultLatency.samples, resultAvgMs,
      resultMaxMs, resultLifetimeMaxMs, resultLifetimeMaxAt, freeExecCountValue, runningExecCountValue,
      parallelExecutionLimitValue, lastCalcAt, lastResultAt, lastOutputAt, pSnapshot->statsOverflow ? "true" : "false");
  if (len < 0 || len >= sizeof(line)) {
    code = TSDB_CODE_OUT_OF_BUFFER;
    goto _exit;
  }

  ST_TASK_DLOG("%s", line);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (taskAddr != NULL) streamReleaseTask(taskAddr);
  return code;
}

static int32_t stRunnerInitTaskExecMgr(SStreamRunnerTask* pTask, const SStreamRunnerDeployMsg* pMsg) {
  SStreamRunnerTaskExecMgr*  pMgr = &pTask->execMgr;
  SStreamRunnerTaskExecution exec = {.pExecutor = NULL, .pPlan = pTask->pPlan};
  // decode plan into queryPlan
  int32_t code = 0, lino = 0;
  code = taosThreadMutexInit(&pMgr->lock, 0);
  if (code != 0) {
    ST_TASK_ELOG("failed to init stream runner task mgr mutex, code:%s", tstrerror(code));
    return code;
  }

  pMgr->lockInited = true;
  
  code = taosThreadMutexLock(&pMgr->lock);
  if(code != 0) {
    ST_TASK_ELOG("failed to lock stream runner task mgr mutex, code:%s", tstrerror(code));
    return code;
  }
  pMgr->pFreeExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  TSDB_CHECK_NULL(pMgr->pFreeExecs, code, lino, _exit, terrno);

  exec.runtimeInfo.vtableDeployGot = &pTask->vtableDeployGot;
  exec.runtimeInfo.pInputStatsParam = pTask->pStats;
  exec.runtimeInfo.inputStatsFp = stRunnerRecordInput;

  for (int32_t i = 0; i < pTask->parallelExecutionNun && code == 0; ++i) {
    exec.runtimeInfo.execId = i + pTask->task.deployId * pTask->parallelExecutionNun;
    if (pMsg->outTblType == TSDB_NORMAL_TABLE) {
      tstrncpy(exec.tbname, pMsg->outTblName, sizeof(exec.tbname));
    }
    ST_TASK_DLOG("init task exec mgr with execId:%d, topTask:%d, deployId: %d", exec.runtimeInfo.execId, pTask->topTask,
		    pTask->task.deployId);
    code = tdListAppend(pMgr->pFreeExecs, &exec);
    if (code != 0) {
      ST_TASK_ELOG("failed to append task exec mgr:%s", tstrerror(code));
      TAOS_CHECK_EXIT(code);
    }
  }

  pMgr->pRunningExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  if (!pMgr->pRunningExecs) return terrno;

_exit:

  taosThreadMutexUnlock(&pMgr->lock);
  
  return code;
}

static void stRunnerDestroyRuntimeInfo(SStreamRuntimeInfo* pRuntime) {
  tDestroyStRtFuncInfo(&pRuntime->funcInfo);
}

static void stRunnerDestroyTaskExecution(void* pExec) {
  SStreamRunnerTaskExecution* pExecution = pExec;
  pExecution->pPlan = NULL;
  streamDestroyExecTask(pExecution->pExecutor);  
  dsDestroyDataSinker(pExecution->pSinkHandle);
  stRunnerDestroyRuntimeInfo(&pExecution->runtimeInfo);
  blockDataDestroy(pExecution->pOutBlock);
}

static int32_t stRunnerTaskAcquireExec(SStreamRunnerTask* pTask, int32_t execId, bool markRunning, SStreamRunnerTaskExecution** ppExec) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  int32_t                   code = 0;
  code = taosThreadMutexLock(&pMgr->lock);
  if (code != 0) {
    ST_TASK_ELOG("failed to lock stream runner task exec mgr mutex, code:%s", tstrerror(code));
    return code;
  }
  ST_TASK_DLOG("get task exec with execId:%d markRunning:%d", execId, markRunning);
  if (execId == -1) {
    if (pMgr->pFreeExecs->dl_neles_ > 0) {
      SListNode* pNode = NULL;
      if (markRunning) {
        pNode = tdListPopHead(pMgr->pFreeExecs);
        tdListAppendNode(pTask->execMgr.pRunningExecs, pNode);
      } else {
        pNode = tdListGetHead(pMgr->pFreeExecs);
      }
      *ppExec = (SStreamRunnerTaskExecution*)pNode->data;
    } else {
      code = TSDB_CODE_STREAM_TASK_IVLD_STATUS;
      ST_TASK_ELOG("too many exec tasks scheduled: %s", tstrerror(code));
    }
  } else {
    SListNode* pNode = tdListGetHead(pMgr->pFreeExecs);
    while (pNode) {
      SStreamRunnerTaskExecution* pExec = (SStreamRunnerTaskExecution*)pNode->data;
      if (pExec->runtimeInfo.execId == execId) {
        if (markRunning) {
          pNode = tdListPopNode(pMgr->pFreeExecs, pNode);
          tdListAppendNode(pMgr->pRunningExecs, pNode);
        }
        *ppExec = pExec;
        goto _exit;
      }
      pNode = pNode->dl_next_;
    }
    
    if (!markRunning) {
      SListNode* pNode = tdListGetHead(pMgr->pRunningExecs);
      while (pNode) {
        SStreamRunnerTaskExecution* pExec = (SStreamRunnerTaskExecution*)pNode->data;
        if (pExec->runtimeInfo.execId == execId) {
          *ppExec = pExec;
          goto _exit;
        }
        pNode = pNode->dl_next_;
      }
    }
    
    code = TSDB_CODE_STREAM_TASK_IVLD_STATUS;
    ST_TASK_ELOG("failed to get task exec, invalid execId:%d", execId);
  }

_exit:
  
  TAOS_UNUSED(taosThreadMutexUnlock(&pMgr->lock));
  if (*ppExec) ST_TASK_DLOG("get exec task with execId: %d", (*ppExec)->runtimeInfo.execId);
  return code;
}

static void stRunnerTaskReleaseExec(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  int32_t code = (taosThreadMutexLock(&pMgr->lock));
  if (code != 0) {
    ST_TASK_ELOG("failed to lock stream runner task exec mgr mutex, code:%s", tstrerror(code));
    return;
  }
  SListNode* pNode = listNode(pExec);
  pNode = tdListPopNode(pMgr->pRunningExecs, pNode);
  tdListPrependNode(pMgr->pFreeExecs, pNode);
  TAOS_UNUSED(taosThreadMutexUnlock(&pMgr->lock));
}

static void stSetRunnerOutputInfo(SStreamRunnerTask* pTask, SStreamRunnerDeployMsg* pMsg) {
  if (pMsg->outDBFName) {
    tstrncpy(pTask->output.outDbFName, pMsg->outDBFName, sizeof(pTask->output.outDbFName));
  } else {
    pTask->output.outDbFName[0] = '\0';
  }
  TSWAP(pTask->output.outCols, pMsg->outCols);
  pTask->output.outTblType = pMsg->outTblType;
  pTask->output.outStbUid = pMsg->outStbUid;
  TSWAP(pTask->output.outTags, pMsg->outTags);
  TSWAP(pTask->output.colCids, pMsg->colCids);
  TSWAP(pTask->output.tagCids, pMsg->tagCids);
  if (pMsg->outTblType == TSDB_SUPER_TABLE)
    tstrncpy(pTask->output.outSTbName, pMsg->outTblName, sizeof(pTask->output.outSTbName));
}

int32_t stRunnerTaskDeploy(SStreamRunnerTask* pTask, SStreamRunnerDeployMsg* pMsg) {
  int32_t code = 0;

  ST_TASK_DLOGL("deploy runner task for %s.%s, runner plan:%s", pMsg->outDBFName, pMsg->outTblName,
                (char*)(pMsg->pPlan));

  pTask->topTask = pMsg->topPlan;
  code = streamTaskStatsInit(&pTask->task, &pTask->pStats);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("failed to create task statistics: %s", tstrerror(code));
    pTask->task.status = STREAM_STATUS_FAILED;
    return code;
  }

  TSWAP(pTask->pPlan, pMsg->pPlan);
  TSWAP(pTask->notification.pNotifyAddrUrls, pMsg->pNotifyAddrUrls);
  TSWAP(pTask->forceOutCols, pMsg->forceOutCols);
  pTask->parallelExecutionNun = pMsg->execReplica;
  pTask->output.outStbVersion = pMsg->outStbSversion;
  pTask->lowLatencyCalc = pMsg->lowLatencyCalc;
  pTask->notification.calcNotifyOnly = pMsg->calcNotifyOnly;
  pTask->addOptions = pMsg->addOptions;
  pTask->streamName = taosStrdup(pMsg->streamName);

  code = stRunnerInitTaskExecMgr(pTask, pMsg);
  if (code != 0) {
    ST_TASK_ELOG("failed to init task exec mgr code:%s", tstrerror(code));
    streamTaskStatsHandleLifecycle(&pTask->pStats, STREAM_TASK_STATS_DEPLOY_FAILED);
    pTask->task.status = STREAM_STATUS_FAILED;
    return code;
  }


  code = nodesStringToList(pMsg->tagValueExpr, &pTask->output.pTagValExprs);
  ST_TASK_DLOG("pTagValExprs: %s", (char*)pMsg->tagValueExpr);
  if (code != 0) {
    ST_TASK_ELOG("failed to convert tag value expr to node err: %s expr: %s", strerror(code),
                 (char*)pMsg->tagValueExpr);
    streamTaskStatsHandleLifecycle(&pTask->pStats, STREAM_TASK_STATS_DEPLOY_FAILED);
    pTask->task.status = STREAM_STATUS_FAILED;
    return code;
  }
  stSetRunnerOutputInfo(pTask, pMsg);
  ST_TASK_DLOG("subTblNameExpr: %s", (char*)pMsg->subTblNameExpr);
  code = nodesStringToNode(pMsg->subTblNameExpr, (SNode**)&pTask->pSubTableExpr);
  if (code != 0) {
    ST_TASK_ELOG("failed to deserialize sub table expr: %s", tstrerror(code));
    streamTaskStatsHandleLifecycle(&pTask->pStats, STREAM_TASK_STATS_DEPLOY_FAILED);
    pTask->task.status = STREAM_STATUS_FAILED;
    return code;
  }

  pTask->task.status = STREAM_STATUS_INIT;
  return 0;
}

int32_t stRunnerTaskUndeployImpl(SStreamRunnerTask** ppTask, const SStreamUndeployTaskMsg* pMsg, taskUndeplyCallback cb) {
  SStreamRunnerTask* pTask = *ppTask;
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  tdListFreeP(pMgr->pRunningExecs, stRunnerDestroyTaskExecution);
  tdListFreeP(pMgr->pFreeExecs, stRunnerDestroyTaskExecution);
  TAOS_UNUSED(taosThreadMutexDestroy(&pMgr->lock));
  NODES_DESTORY_NODE(pTask->pSubTableExpr);
  NODES_DESTORY_LIST(pTask->output.pTagValExprs);
  taosArrayDestroy(pTask->output.outCols);
  taosArrayDestroy(pTask->output.outTags);
  taosMemoryFreeClear(pTask->pPlan);
  taosArrayDestroyEx(pTask->forceOutCols, destroySStreamOutCols);
  taosArrayDestroyP(pTask->notification.pNotifyAddrUrls, taosMemFree);
  taosMemoryFreeClear(pTask->streamName);
  taosArrayDestroy(pTask->output.colCids);
  taosArrayDestroy(pTask->output.tagCids);
  streamTaskStatsHandleLifecycle(&pTask->pStats, STREAM_TASK_STATS_UNDEPLOYED);

  cb(ppTask);
  
  return 0;
}

void stRunnerKillAllExecs(SStreamRunnerTask *pTask) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  int32_t                   code = 0;

  if (!pMgr->lockInited) {
    return;
  }

  code = taosThreadMutexLock(&pMgr->lock);
  if (code != 0) {
    ST_TASK_ELOG("failed to lock stream runner task exec mgr mutex, code:%s", tstrerror(code));
    return;
  }
  if (NULL == pMgr->pRunningExecs) {
    TAOS_UNUSED(taosThreadMutexUnlock(&pMgr->lock));
    return;
  }
  ST_TASK_DLOG("start to kill running execs, num:%d", listNEles(pMgr->pRunningExecs));
  SListNode* pNode = tdListGetHead(pMgr->pRunningExecs);
  while (pNode) {
    SStreamRunnerTaskExecution* pExec = (SStreamRunnerTaskExecution*)pNode->data;
    TAOS_UNUSED(qAsyncKillTask(pExec->pExecutor, TSDB_CODE_STREAM_EXEC_CANCELLED));
    pNode = pNode->dl_next_;
  }
  ST_TASK_DLOG("all runner execs killed, num: %d", listNEles(pMgr->pRunningExecs));
  TAOS_UNUSED(taosThreadMutexUnlock(&pMgr->lock));
}

int32_t stRunnerTaskUndeploy(SStreamRunnerTask** ppTask, bool force) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SStreamRunnerTask *pTask = *ppTask;
  int64_t            streamId = pTask->task.streamId;
  int64_t            taskId = pTask->task.taskId;

  stRunnerKillAllExecs(pTask);
  
  if (!force && taosWTryForceLockLatch(&pTask->task.entryLock)) {
    stsDebug("ignore undeploy runner task %" PRIx64 " since working", taskId);
    return code;
  }

  ST_TASK_DLOG("runner task start undeploy impl, entryLock:%x", pTask->task.entryLock);

  return stRunnerTaskUndeployImpl(ppTask, &pTask->task.undeployMsg, pTask->task.undeployCb);
}

static bool stRunnerTaskWaitQuit(SStreamRunnerTask* pTask) { return taosHasRWWFlag(&pTask->task.entryLock); }

static int32_t stRunnerResetTaskExec(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec, bool ignoreTbName) {
  int32_t code = 0;
  if (!ignoreTbName) pExec->tbname[0] = '\0';
  ST_TASK_DLOG("streamResetTaskExec:%p, execId:%d exec finished, ignoreTbName:%d tbname: %s", pExec, pExec->runtimeInfo.execId, ignoreTbName, pExec->tbname);
  code = streamClearStatesForOperators(pExec->pExecutor);
  return code;
}

static int32_t stMakeSValueFromColInfoData(SStreamRunnerTask* pTask, SStreamGroupValue* pVal,
                                           const SColumnInfoData* pCol) {
  int32_t code = 0;
  pVal->data.type = pCol->info.type;
  char* p = colDataGetData(pCol, 0);
  pVal->isNull = colDataIsNull(pCol, 1, 0, NULL);
  if (!pVal->isNull) {
    size_t len = 0;
    if (IS_VAR_DATA_TYPE(pVal->data.type)) {
      len = varDataLen(p);
      pVal->data.pData = taosMemoryCalloc(1, len);
      if (!pVal->data.pData) {
        code = terrno;
        ST_TASK_ELOG("failed to make svalue from col info data: %s", strerror(code));
        return code;
      }
      memcpy(pVal->data.pData, varDataVal(p), len);
      pVal->data.nData = len;
    } else if (pVal->data.type == TSDB_DATA_TYPE_DECIMAL) {
      pVal->data.pData = taosMemoryCalloc(1, tDataTypes[TSDB_DATA_TYPE_DECIMAL].bytes);
      if (!pVal->data.pData) {
        code = terrno;
        ST_TASK_ELOG("failed to make svalue from col info data: %s", strerror(code));
        return code;
      }
      memcpy(pVal->data.pData, p, pCol->info.bytes);
      pVal->data.nData = pCol->info.bytes;
    } else {
      valueSetDatum(&pVal->data, pVal->data.type, p, pCol->info.bytes);
    }
  }
  return code;
}

static void stRunnerFreeTagInfo(void* p) {
  SStreamTagInfo* pTagInfo = p;
  if (pTagInfo->val.data.type == TSDB_DATA_TYPE_DECIMAL || IS_VAR_DATA_TYPE(pTagInfo->val.data.type))
    taosMemoryFreeClear(pTagInfo->val.data.pData);
}

static int32_t stRunnerCalcSubTbTagVal(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                       SArray** ppTagVals) {
  int32_t code = 0;
  int32_t lino = 0;
  SNode*  pNode = NULL;
  *ppTagVals = NULL;
  int32_t tagIdx = 0;
  FOREACH(pNode, pTask->output.pTagValExprs) {
    SScalarParam dst = {0};
    if (!*ppTagVals) *ppTagVals = taosArrayInit(1, sizeof(SStreamTagInfo));
    if (!*ppTagVals) {
      ST_TASK_ELOG("failed to init  stream tag info array: %s", strerror(code));
      code = terrno;
      break;
    }
    const SFieldWithOptions* pTagField = taosArrayGet(pTask->output.outTags, tagIdx);
    tagIdx++;
    SColumnInfoData* pCol = taosMemoryCalloc(1, sizeof(SColumnInfoData));
    if (!pCol) {
      code = terrno;
      break;
    }
    SDataType pType = ((SExprNode*)pNode)->resType;
    pCol->info.type = pType.type;
    pCol->info.bytes = pType.bytes;
    pCol->info.precision = pType.precision;
    pCol->info.scale = pType.scale;
    if (pTask->output.tagCids) {
      pCol->info.colId = *(col_id_t*)taosArrayGet(pTask->output.tagCids, tagIdx - 1);
    }
    code = colInfoDataEnsureCapacity(pCol, 1, true);
    if (code != 0) {
      ST_TASK_ELOG("failed to ensure capacity for col info data: %s", strerror(code));
      taosMemoryFreeClear(pCol);
      break;
    }

    dst.colAlloced = true;
    dst.numOfRows = 1;
    dst.columnData = pCol;
    if (pNode->type == QUERY_NODE_VALUE) {
      void* p = nodesGetValueFromNode((SValueNode*)pNode);
      code = colDataSetVal(pCol, 0, p, ((SValueNode*)pNode)->isNull);
    } else {
      code = streamCalcOneScalarExpr(pNode, &dst, &pExec->runtimeInfo.funcInfo);
    }
    if (code != 0) {
      sclFreeParam(&dst);
      break;
    }
    SStreamTagInfo tagInfo = {0};
    tstrncpy(tagInfo.tagName, pTagField->name, TSDB_COL_NAME_LEN);
    code = stMakeSValueFromColInfoData(pTask, &tagInfo.val, dst.columnData);
    sclFreeParam(&dst);
    if (NULL == taosArrayPush(*ppTagVals, &tagInfo)) {
      if (IS_VAR_DATA_TYPE(tagInfo.val.data.type) || tagInfo.val.data.type == TSDB_DATA_TYPE_DECIMAL)
        taosMemoryFreeClear(tagInfo.val.data.pData);
      code = terrno;
      break;
    }
    if (code != 0) break;
  }

  return code;
}

static int32_t stRunnerInitTbTagVal(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec, SArray** ppTagVals) {
  int32_t code = 0;
  if (pTask->output.outTblType == TSDB_SUPER_TABLE) {
    int32_t nextIdx = pExec->runtimeInfo.funcInfo.curIdx;
    pExec->runtimeInfo.funcInfo.curIdx = 0;  // always use the first window to calc tag vals
    code = stRunnerCalcSubTbTagVal(pTask, pExec, ppTagVals);
    pExec->runtimeInfo.funcInfo.curIdx = nextIdx;
  }
  return code;
}

static int32_t stRunnerGetNotifyTbName(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                       const char** ppTbName) {
  if (pExec->tbname[0] == '\0') {
    if (pTask->notification.calcNotifyOnly && pTask->pSubTableExpr == NULL) {
      const char* pStreamName = pTask->streamName;
      const char* pos = strstr(pStreamName, TS_PATH_DELIMITER);
      *ppTbName = pos ? pos + 1 : pStreamName;
      return TSDB_CODE_SUCCESS;
    } else {
      int32_t code = streamCalcOutputTbName(pTask->pSubTableExpr, pExec->tbname, &pExec->runtimeInfo.funcInfo);
      if (code != 0) {
        ST_TASK_ELOG("%s failed to calc output tbname for notification: %s", __FUNCTION__, tstrerror(code));
        return code;
      }
      ST_TASK_ILOG("%s table name is blank, so calc output table name, get %s.", __FUNCTION__, pExec->tbname);
    }
  }

  *ppTbName = pExec->tbname;
  return TSDB_CODE_SUCCESS;
}

static void stRunnerRecordFailure(SStreamRunnerTask* pTask, EStreamRunnerFailure failure, bool* pClassified) {
  stTaskStatsRecordRunnerFailure(pTask->pStats, failure, streamTaskGetMonotonicUs());
  if (pClassified != NULL) {
    *pClassified = true;
  }
}

static void stRunnerRecordOutput(SStreamRunnerTask* pTask, uint64_t rows, uint64_t blocks) {
  stTaskStatsRecordRunnerOutput(pTask->pStats, rows, blocks, streamTaskGetMonotonicUs(), taosGetTimestampMs());
}

static void stRunnerRecordReadyWindow(SStreamRunnerTask* pTask, int64_t requestStartMonoUs, bool hasResult) {
  int64_t  nowMonoUs = streamTaskGetMonotonicUs();
  uint64_t latencyUs = nowMonoUs >= requestStartMonoUs ? (uint64_t)(nowMonoUs - requestStartMonoUs) : 0;
  int64_t  nowWallMs = taosGetTimestampMs();
  stTaskStatsRecordRunnerWindow(pTask->pStats, hasResult, latencyUs, nowMonoUs, nowWallMs);
}

static uint64_t stRunnerCountNotificationRows(const SStreamRunnerTask* pTask, const SSDataBlock* pBlock,
                                              int32_t startRow, int32_t endRow) {
  if (pBlock == NULL || pBlock->info.rows <= 0 || startRow >= pBlock->info.rows || endRow < startRow) return 0;

  if (endRow >= pBlock->info.rows) {
    endRow = (int32_t)pBlock->info.rows - 1;
  }
  if ((pTask->addOptions & NOTIFY_HAS_FILTER) == 0) {
    return (uint64_t)(endRow - startRow + 1);
  }

  int32_t          filterIndex = (int32_t)taosArrayGetSize(pBlock->pDataBlock) - 1;
  SColumnInfoData* pFilterCol = taosArrayGet(pBlock->pDataBlock, filterIndex);
  if (pFilterCol == NULL || pFilterCol->info.type != TSDB_DATA_TYPE_BOOL) return 0;

  uint64_t rows = 0;
  for (int32_t row = startRow; row <= endRow; ++row) {
    if (!colDataIsNull_s(pFilterCol, row) && *(bool*)colDataGetData(pFilterCol, row)) {
      ++rows;
    }
  }
  return rows;
}

static int32_t stRunnerOutputBlock(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec, SSDataBlock* pBlock,
                                   bool* createTb, bool* pFailureClassified) {
  int32_t code = 0;
  if (stRunnerTaskWaitQuit(pTask)) {
    ST_TASK_ILOG("[runner calc]quit, skip output. status:%d", pTask->task.status);
    return TSDB_CODE_SUCCESS;
  }
  if (pTask->notification.calcNotifyOnly) return 0;
  bool needCalcTbName = pExec->tbname[0] == '\0';

  if (*createTb && needCalcTbName) {
    code = streamCalcOutputTbName(pTask->pSubTableExpr, pExec->tbname, &pExec->runtimeInfo.funcInfo);
    stDebug("stRunnerOutputBlock tbname: %s", pExec->tbname);

  }
  if (code != 0) {
    ST_TASK_ELOG("failed to calc output tbname: %s", tstrerror(code));
  } else {
    SArray* pTagVals = NULL;
    if (*createTb) code = stRunnerInitTbTagVal(pTask, pExec, &pTagVals);
    if (code == 0) {
      SStreamDataInserterInfo d = {.tbName = pExec->tbname,
                                    .streamId = pTask->task.streamId,
                                    .groupId = pExec->runtimeInfo.funcInfo.groupId,
                                    .isAutoCreateTable = *createTb,
                                    .pTagVals = pTagVals};
      SInputData              input = {.pData = pBlock, .pStreamDataInserterInfo = &d, .pTask = pTask};
      bool                    cont = false;
      code = dsPutDataBlock(pExec->pSinkHandle, &input, &cont);
      if (!*createTb && code == TSDB_CODE_STREAM_INSERT_TBINFO_NOT_FOUND &&
          pTask->output.outTblType == TSDB_NORMAL_TABLE) {
        d.isAutoCreateTable = true;
        code = dsPutDataBlock(pExec->pSinkHandle, &input, &cont);
      }
      ST_TASK_DLOG("runner output block to sink code:0x%0x, rows: %" PRId64 ", tbname: %s, createTb: %d, gid: %" PRId64,
                    code, (pBlock != NULL ? pBlock->info.rows : 0), pExec->tbname, *createTb, pExec->runtimeInfo.funcInfo.groupId);
      printDataBlock(pBlock, "output block to sink", "runner", pTask->task.streamId);
      if (code == TSDB_CODE_SUCCESS) {
        *createTb = false;  // if output block success, then no need to create table
        if (pBlock != NULL && pBlock->info.rows > 0) {
          stRunnerRecordOutput(pTask, pBlock->info.rows, 1);
        }
      } else {
        stRunnerRecordFailure(pTask, STREAM_RUNNER_FAILURE_SINK, pFailureClassified);
      }
    } else {
      ST_TASK_ELOG("failed to init tag vals for output block: %s", tstrerror(code));
    }
    taosArrayDestroyEx(pTagVals, stRunnerFreeTagInfo);
  }

  return code;
}

static int32_t stRunnerMergeOutputBlock(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                        SSDataBlock* pBlock, bool finished, bool* createTb, bool* pFailureClassified) {
  int32_t code = 0, lino = 0;
  SSDataBlock* pOutput = pExec->pOutBlock;
  if (stRunnerTaskWaitQuit(pTask)) {
    ST_TASK_ILOG("[runner calc]quit, skip merge block. status:%d", pTask->task.status);
    return TSDB_CODE_SUCCESS;
  }
  
  if (pTask->notification.calcNotifyOnly) return code;

  bool lowLatencyCalc = pTask->lowLatencyCalc || (tsStreamBatchRequestWaitMs < 1000);
  
  if (pBlock && pBlock->info.rows > 0) {
    if (pBlock->info.rows >= 4096 || lowLatencyCalc) {
      pOutput = pBlock;
    } else if (NULL == pExec->pOutBlock) {
      TAOS_CHECK_EXIT(createOneDataBlock(pBlock, true, &pExec->pOutBlock));
      pOutput = pExec->pOutBlock;
    } else {
      TAOS_CHECK_EXIT(blockDataMerge(pExec->pOutBlock, pBlock));
    }
  } else {
    TAOS_CHECK_EXIT(stRunnerOutputBlock(pTask, pExec, NULL, createTb, pFailureClassified));
  }

  if (pOutput && pOutput->info.rows > 0) {
    int32_t winNum = taosArrayGetSize(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals);
    if (lowLatencyCalc || (pExec->runtimeInfo.funcInfo.curOutIdx) >= winNum || pOutput->info.rows >= 4096) {
      TAOS_CHECK_EXIT(stRunnerOutputBlock(pTask, pExec, pOutput, createTb, pFailureClassified));
      blockDataCleanup(pOutput);
    }
  }

_exit:

  if (code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __FUNCTION__, lino, tstrerror(code));
  }
  
  return code;
}

static int32_t streamPrepareNotification(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                         const SSDataBlock* pBlock, const int32_t curWinIdx, const int32_t startRow,
                                         const int32_t endRow, uint64_t* pNotifyRows, bool* pFailureClassified) {
  int32_t code = 0;
  bool    empty = (!pBlock || pBlock->info.rows <= 0);
  if (pTask->notification.pNotifyAddrUrls == NULL || pTask->notification.pNotifyAddrUrls->size == 0) {
    return code;
  }
  char* pContent = NULL;
  bool  hasNotifyRows = false;
  code = streamBuildBlockResultNotifyContent(pTask, pBlock, &pContent, pTask->output.outCols, startRow, endRow,
                                             &hasNotifyRows);
  if (code == 0 && hasNotifyRows) {
    ST_TASK_DLOG("prepare notify:%s", pContent);
    SSTriggerCalcParam* pTriggerCalcParams = taosArrayGet(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, curWinIdx);
    if (pTriggerCalcParams == NULL) {
      ST_TASK_ELOG("%s failed to get trigger calc params for win index:%d, size:%d", __FUNCTION__, curWinIdx,
                   (int32_t)pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals->size);
      taosMemoryFreeClear(pContent);
      code = TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
      goto _exit;
    }
    pTriggerCalcParams->resultNotifyContent = pContent;
    if (pNotifyRows != NULL) {
      uint64_t rows = stRunnerCountNotificationRows(pTask, pBlock, startRow, endRow);
      *pNotifyRows = UINT64_MAX - *pNotifyRows < rows ? UINT64_MAX : *pNotifyRows + rows;
    }
  } else if (code == 0) {
    taosMemoryFreeClear(pContent);
  }
_exit:
  if (code != 0) {
    bool* pClassified = (pTask->addOptions & NOTIFY_ON_FAILURE_PAUSE) != 0 ? pFailureClassified : NULL;
    stRunnerRecordFailure(pTask, STREAM_RUNNER_FAILURE_NOTIFY, pClassified);
    ST_TASK_ELOG("failed to prepare notification for task:%" PRIx64 ", code:%s", pTask->task.streamId, tstrerror(code));
    if (pContent) taosMemoryFreeClear(pContent);
    if ((pTask->addOptions & NOTIFY_ON_FAILURE_PAUSE) ==  0) {
      code = TSDB_CODE_SUCCESS;
    }
  }
  return code;
}

static void clearNotifyContent(SStreamRunnerTaskExecution* pExec, int32_t startWinIdx, int32_t endWinIdx) {
  for (int i = startWinIdx; i < endWinIdx; ++i) {
    SSTriggerCalcParam* pTriggerCalcParams = taosArrayGet(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, i);
    if (pTriggerCalcParams != NULL && pTriggerCalcParams->resultNotifyContent != NULL) {
      taosMemoryFreeClear(pTriggerCalcParams->resultNotifyContent);
    }
  }
}

static bool stRunnerNotifyUsesNestedLeafId(int32_t notifyType) {
  return notifyType == STRIGGER_EVENT_WINDOW_OPEN || notifyType == STRIGGER_EVENT_WINDOW_CLOSE ||
         notifyType == STRIGGER_EVENT_ON_TIME;
}

static int32_t stRunnerBuildResultTriggerId(const SStreamRunnerTask* pTask, const SStreamRunnerTaskExecution* pExec,
                                            int32_t paramIndex, char triggerId[STREAM_NESTED_TRIGGER_ID_LEN]) {
  if (pTask == NULL || pExec == NULL || triggerId == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  triggerId[0] = '\0';
  if (!BIT_FLAG_TEST_MASK(pTask->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN)) {
    return TSDB_CODE_SUCCESS;
  }

  const SArray* pParams = pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals;
  if (paramIndex < 0 || pParams == NULL || pParams->elemSize != sizeof(SSTriggerCalcParam) ||
      paramIndex >= taosArrayGetSize(pParams)) {
    return TSDB_CODE_INVALID_PARA;
  }
  const SSTriggerCalcParam* pCalcParam = taosArrayGet(pParams, paramIndex);
  if (pCalcParam == NULL || !stRunnerNotifyUsesNestedLeafId(pCalcParam->notifyType)) {
    return TSDB_CODE_INVALID_PARA;
  }

  const SStreamAncestorContext* pAncestorContext = pExec->runtimeInfo.funcInfo.pAncestorContext;
  if (pAncestorContext == NULL || pAncestorContext->pParamContexts == NULL ||
      pAncestorContext->pParamContexts->elemSize != sizeof(SStreamAncestorParamContext)) {
    return TSDB_CODE_INVALID_PARA;
  }

  const int64_t                      gid = pExec->runtimeInfo.funcInfo.groupId;
  const SStreamAncestorParamContext* pMatch = NULL;
  int32_t                            matchCount = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pAncestorContext->pParamContexts); ++i) {
    const SStreamAncestorParamContext* pParam = taosArrayGet(pAncestorContext->pParamContexts, i);
    if (pParam != NULL && pParam->leafIdentity.gid == gid && pParam->paramIndex == paramIndex) {
      pMatch = pParam;
      ++matchCount;
    }
  }
  if (matchCount != 1) {
    return TSDB_CODE_INVALID_PARA;
  }

  int32_t windowIndex = -1;
  int32_t code = stResolveNestedLeafWindowIndex(pExec->runtimeInfo.funcInfo.triggerType, &pMatch->leafIdentity,
                                                pCalcParam->notifyType, pCalcParam->extraNotifyContent, &windowIndex);
  if (code != TSDB_CODE_SUCCESS) return code;
  return stBuildNestedTriggerId(pMatch->leafIdentity.gid, &pMatch->leafIdentity.lineage, pMatch->leafIdentity.openingTs,
                                windowIndex, triggerId);
}

static int32_t streamDoNotificationCurrentWins(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                               const char* tbname, uint64_t notifyRows, bool* pFailureClassified) {
  int32_t             code = 0;
  int32_t             lino = 0;
  int32_t             winSize = 0;
  int32_t             nParam = 0;
  SSTriggerCalcParam* params = NULL;
  bool                attempted = false;
  bool                delivered = false;
  char*               pTriggerIdStorage = NULL;
  const char**        pTriggerIds = NULL;
  const bool          nested = BIT_FLAG_TEST_MASK(pTask->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN);
  if (pTask->notification.pNotifyAddrUrls == NULL || pTask->notification.pNotifyAddrUrls->size == 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (tbname[0] == '\0') {
    TAOS_CHECK_EXIT(stRunnerGetNotifyTbName(pTask, pExec, &tbname));
  }

  winSize = pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals->size;
  params = taosMemoryCalloc(winSize, sizeof(SSTriggerCalcParam));
  if (!params) {
    ST_TASK_ELOG("failed to init stream pesudo func vals array, size:%d", winSize);
    TAOS_CHECK_EXIT(terrno);
  }
  if (nested) {
    pTriggerIdStorage = taosMemoryCalloc(winSize, STREAM_NESTED_TRIGGER_ID_LEN);
    if (pTriggerIdStorage == NULL) {
      ST_TASK_ELOG("failed to init nested trigger id storage, size:%d", winSize);
      TAOS_CHECK_EXIT(terrno);
    }
    pTriggerIds = taosMemoryCalloc(winSize, sizeof(*pTriggerIds));
    if (pTriggerIds == NULL) {
      ST_TASK_ELOG("failed to init nested trigger id array, size:%d", winSize);
      TAOS_CHECK_EXIT(terrno);
    }
  }

  for (int i = 0; i < winSize; ++i) {
    SSTriggerCalcParam* pTriggerCalcParams = taosArrayGet(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, i);
    if (pTriggerCalcParams == NULL) {
      continue;
    }
    if (pTriggerCalcParams->resultNotifyContent == NULL) {
      ST_TASK_DLOG("%s no notify content for index:%d", __FUNCTION__, i);
      continue;
    }
    if (nested && stRunnerNotifyUsesNestedLeafId(pTriggerCalcParams->notifyType)) {
      char* pTriggerId = pTriggerIdStorage + (size_t)nParam * STREAM_NESTED_TRIGGER_ID_LEN;
      TAOS_CHECK_EXIT(stRunnerBuildResultTriggerId(pTask, pExec, i, pTriggerId));
      pTriggerIds[nParam] = pTriggerId;
    }
    params[nParam] = *pTriggerCalcParams;
    ++nParam;
  }

  if (nested) {
    attempted = nParam > 0;
    code = streamSendNestedResultNotifyContent(&pTask->task, pTask->streamName, tbname,
                                               pExec->runtimeInfo.funcInfo.triggerType,
                                               pExec->runtimeInfo.funcInfo.groupId, pTask->notification.pNotifyAddrUrls,
                                               pTask->addOptions, params, pTriggerIds, nParam);
    delivered = attempted && code == TSDB_CODE_SUCCESS;
  } else {
    code = streamSendNotifyContentWithResult(
        &pTask->task, pTask->streamName, tbname, pExec->runtimeInfo.funcInfo.triggerType,
        pExec->runtimeInfo.funcInfo.groupId, pTask->notification.pNotifyAddrUrls, pTask->addOptions, params, nParam,
        &attempted, &delivered);
  }
  TAOS_CHECK_EXIT(code);

_exit:
  if (code != TSDB_CODE_SUCCESS || (attempted && !delivered)) {
    bool* pClassified =
        code != TSDB_CODE_SUCCESS && (pTask->addOptions & NOTIFY_ON_FAILURE_PAUSE) != 0 ? pFailureClassified : NULL;
    stRunnerRecordFailure(pTask, STREAM_RUNNER_FAILURE_NOTIFY, pClassified);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("failed to send notification for task:%" PRIx64 ", code:%s lino:%d", pTask->task.streamId,
                   tstrerror(code), lino);
    } else {
      ST_TASK_ELOG("notification delivery failed for task:%" PRIx64, pTask->task.streamId);
    }
  } else {
    ST_TASK_DLOG("send notification for task:%" PRIx64 ", win count:%d", pTask->task.streamId, nParam);
    if (pTask->notification.calcNotifyOnly && delivered && notifyRows > 0) {
      stRunnerRecordOutput(pTask, notifyRows, 1);
    }
  }
  if (!nested && (pTask->addOptions & NOTIFY_ON_FAILURE_PAUSE) == 0) {
    code = TSDB_CODE_SUCCESS;  // if notify error handle is 0, then ignore the error
  }
  clearNotifyContent(pExec, 0, winSize);
  taosMemoryFreeClear(pTriggerIds);
  taosMemoryFreeClear(pTriggerIdStorage);
  taosMemoryFreeClear(params);
  return code;
}

static int32_t streamDoNotification(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec, int32_t startWinIdx,
                                    int32_t endWinIdx, const char* tbname, uint64_t notifyRows,
                                    bool* pFailureClassified) {
  int32_t             code = 0;
  int32_t             lino = 0;
  int32_t             nParam = endWinIdx - startWinIdx;
  SSTriggerCalcParam* params = NULL;
  bool                attempted = false;
  bool                delivered = false;
  char*               pTriggerIdStorage = NULL;
  const char**        pTriggerIds = NULL;
  const bool          nested = BIT_FLAG_TEST_MASK(pTask->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN);
  if (pTask->notification.pNotifyAddrUrls == NULL || pTask->notification.pNotifyAddrUrls->size == 0) {
    return TSDB_CODE_SUCCESS;
  }

  if (tbname[0] == '\0') {
    TAOS_CHECK_EXIT(stRunnerGetNotifyTbName(pTask, pExec, &tbname));
  }

  params = taosMemoryCalloc(nParam, sizeof(SSTriggerCalcParam));
  if (!params) {
    ST_TASK_ELOG("failed to init stream pesudo func vals array, size:%d", nParam);
    TAOS_CHECK_EXIT(terrno);
  }
  if (nested) {
    pTriggerIdStorage = taosMemoryCalloc(nParam, STREAM_NESTED_TRIGGER_ID_LEN);
    if (pTriggerIdStorage == NULL) {
      ST_TASK_ELOG("failed to init nested trigger id storage, size:%d", nParam);
      TAOS_CHECK_EXIT(terrno);
    }
    pTriggerIds = taosMemoryCalloc(nParam, sizeof(*pTriggerIds));
    if (pTriggerIds == NULL) {
      ST_TASK_ELOG("failed to init nested trigger id array, size:%d", nParam);
      TAOS_CHECK_EXIT(terrno);
    }
  }

  nParam = 0;
  for (int i = startWinIdx; i < endWinIdx; ++i) {
    SSTriggerCalcParam* pTriggerCalcParams = taosArrayGet(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, i);
    if (pTriggerCalcParams == NULL) {
      ST_TASK_ELOG("%s failed to get trigger calc params for index:%d, size:%d", __FUNCTION__, i,
                   (int32_t)pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals->size);
      TAOS_CHECK_EXIT(TSDB_CODE_MND_STREAM_INTERNAL_ERROR);
    }
    if (pTriggerCalcParams->resultNotifyContent == NULL) {
      ST_TASK_DLOG("%s no notify content for index:%d", __FUNCTION__, i);
      continue;
    }
    if (nested && stRunnerNotifyUsesNestedLeafId(pTriggerCalcParams->notifyType)) {
      char* pTriggerId = pTriggerIdStorage + (size_t)nParam * STREAM_NESTED_TRIGGER_ID_LEN;
      TAOS_CHECK_EXIT(stRunnerBuildResultTriggerId(pTask, pExec, i, pTriggerId));
      pTriggerIds[nParam] = pTriggerId;
    }
    params[nParam] = *pTriggerCalcParams;
    ++nParam;
  }

  if (nested) {
    attempted = nParam > 0;
    code = streamSendNestedResultNotifyContent(&pTask->task, pTask->streamName, tbname,
                                               pExec->runtimeInfo.funcInfo.triggerType,
                                               pExec->runtimeInfo.funcInfo.groupId, pTask->notification.pNotifyAddrUrls,
                                               pTask->addOptions, params, pTriggerIds, nParam);
    delivered = attempted && code == TSDB_CODE_SUCCESS;
  } else {
    code = streamSendNotifyContentWithResult(
        &pTask->task, pTask->streamName, tbname, pExec->runtimeInfo.funcInfo.triggerType,
        pExec->runtimeInfo.funcInfo.groupId, pTask->notification.pNotifyAddrUrls, pTask->addOptions, params, nParam,
        &attempted, &delivered);
  }

_exit:
  if (code != TSDB_CODE_SUCCESS || (attempted && !delivered)) {
    bool* pClassified =
        code != TSDB_CODE_SUCCESS && (pTask->addOptions & NOTIFY_ON_FAILURE_PAUSE) != 0 ? pFailureClassified : NULL;
    stRunnerRecordFailure(pTask, STREAM_RUNNER_FAILURE_NOTIFY, pClassified);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("failed to send notification for task:%" PRIx64 ", code:%s", pTask->task.streamId, tstrerror(code));
    } else {
      ST_TASK_ELOG("notification delivery failed for task:%" PRIx64, pTask->task.streamId);
    }
  } else {
    ST_TASK_DLOG("send notification for task:%" PRIx64 ", win count:%d", pTask->task.streamId, nParam);
    if (pTask->notification.calcNotifyOnly && delivered && notifyRows > 0) {
      stRunnerRecordOutput(pTask, notifyRows, 1);
    }
  }
  if (!nested && (pTask->addOptions & NOTIFY_ON_FAILURE_PAUSE) == 0) {
    code = TSDB_CODE_SUCCESS;  // if notify error handle is 0, then ignore the error
  }
  clearNotifyContent(pExec, startWinIdx, endWinIdx);
  taosMemoryFreeClear(pTriggerIds);
  taosMemoryFreeClear(pTriggerIdStorage);
  taosMemoryFreeClear(params);
  return code;
}

static int32_t streamDoNotification1For1(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                         const SSDataBlock* pBlock, const char* tbname, bool* pFailureClassified) {
  int32_t    code = 0;
  int32_t    lino = 0;
  const bool nested = BIT_FLAG_TEST_MASK(pTask->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN);

  if (tbname[0] == '\0') {
    TAOS_CHECK_GOTO(stRunnerGetNotifyTbName(pTask, pExec, &tbname), &lino, _exit);
  }
  bool  empty = (!pBlock || pBlock->info.rows <= 0);
  char* pContent = NULL;
  bool  hasNotifyRows = false;
  bool  attempted = false;
  bool  delivered = false;
  code = streamBuildBlockResultNotifyContent(pTask, pBlock, &pContent, pTask->output.outCols, 0,
                                             empty ? 0 : pBlock->info.rows - 1, &hasNotifyRows);
  if (code == 0 && hasNotifyRows) {
    ST_TASK_DLOG("start to send notify:%s", pContent);
    int32_t             index = pExec->runtimeInfo.funcInfo.curOutIdx;
    SSTriggerCalcParam* pTriggerCalcParams = taosArrayGet(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, index);
    if (pTriggerCalcParams == NULL) {
      ST_TASK_ELOG("%s failed to get trigger calc params for index:%d, size:%d", __FUNCTION__, index,
                   (int32_t)pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals->size);
      taosMemoryFreeClear(pContent);
      code = TSDB_CODE_MND_STREAM_INTERNAL_ERROR;
      lino = __LINE__;
      goto _exit;
    }
    pTriggerCalcParams->resultNotifyContent = pContent;

    if (nested) {
      char        triggerId[STREAM_NESTED_TRIGGER_ID_LEN] = {0};
      const char* pTriggerIds[1] = {NULL};
      if (stRunnerNotifyUsesNestedLeafId(pTriggerCalcParams->notifyType)) {
        code = stRunnerBuildResultTriggerId(pTask, pExec, index, triggerId);
        pTriggerIds[0] = triggerId;
      }
      if (code == TSDB_CODE_SUCCESS) {
        attempted = pTriggerCalcParams->notifyType != STRIGGER_EVENT_WINDOW_NONE;
        code = streamSendNestedResultNotifyContent(
            &pTask->task, pTask->streamName, tbname, pExec->runtimeInfo.funcInfo.triggerType,
            pExec->runtimeInfo.funcInfo.groupId, pTask->notification.pNotifyAddrUrls, pTask->addOptions,
            pTriggerCalcParams, pTriggerIds, 1);
        delivered = attempted && code == TSDB_CODE_SUCCESS;
      }
    } else {
      code = streamSendNotifyContentWithResult(
          &pTask->task, pTask->streamName, tbname, pExec->runtimeInfo.funcInfo.triggerType,
          pExec->runtimeInfo.funcInfo.groupId, pTask->notification.pNotifyAddrUrls, pTask->addOptions,
          pTriggerCalcParams, 1, &attempted, &delivered);
    }
    taosMemoryFreeClear(pTriggerCalcParams->resultNotifyContent);
  } else if (code == 0) {
    taosMemoryFreeClear(pContent);
  }
_exit:
  if (code != TSDB_CODE_SUCCESS || (attempted && !delivered)) {
    bool* pClassified =
        code != TSDB_CODE_SUCCESS && (pTask->addOptions & NOTIFY_ON_FAILURE_PAUSE) != 0 ? pFailureClassified : NULL;
    stRunnerRecordFailure(pTask, STREAM_RUNNER_FAILURE_NOTIFY, pClassified);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("%s failed at line %d since %s", __FUNCTION__, lino, tstrerror(code));
    } else {
      ST_TASK_ELOG("notification delivery failed for task:%" PRIx64, pTask->task.streamId);
    }
  } else if (pTask->notification.calcNotifyOnly && delivered && hasNotifyRows) {
    uint64_t rows = stRunnerCountNotificationRows(pTask, pBlock, 0, empty ? 0 : (int32_t)pBlock->info.rows - 1);
    if (rows > 0) {
      stRunnerRecordOutput(pTask, rows, 1);
    }
  }
  return code;
}

static int32_t stRunnerHandleSingleWinResultBlock(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                                  SSDataBlock* pBlock, bool* pCreateTb, bool* pFailureClassified) {
  int32_t code = stRunnerMergeOutputBlock(pTask, pExec, pBlock, false, pCreateTb, pFailureClassified);
  if (code == TSDB_CODE_SUCCESS && pTask->notification.pNotifyAddrUrls &&
      pTask->notification.pNotifyAddrUrls->size > 0) {
    code = streamDoNotification1For1(pTask, pExec, pBlock, pExec->tbname, pFailureClassified);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("failed to send notification for block, code:%s", tstrerror(code));
    }
    if (!BIT_FLAG_TEST_MASK(pTask->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN) &&
        (pTask->addOptions & NOTIFY_ON_FAILURE_PAUSE) == 0) {
      code = TSDB_CODE_SUCCESS;  // ignore the notify error
    }
  }
  return code;
}

static int32_t stRunnerMergeBlockHandleOverflow(const SSDataBlock* pSrc, SSDataBlock* pDst, int32_t start,
                                                int32_t rowsToCopy, SSDataBlock** ppExtraBlock) {
  *ppExtraBlock = NULL;
  int32_t code = 0;
  if (pDst->info.rows + rowsToCopy > 4096) {
    int32_t rowsToCopy2 = 4096 - pDst->info.rows;
    if (rowsToCopy2 > 0) {
      code = blockDataMergeNRows(pDst, pSrc, start, rowsToCopy2);
      if (code != 0) return code;
      start += rowsToCopy2;
      rowsToCopy -= rowsToCopy2;
    }
  }
  if (rowsToCopy > 0) {
    code = createOneDataBlock(pSrc, false, ppExtraBlock);
    if (code == 0) {
      code = blockDataMergeNRows(*ppExtraBlock, pSrc, start, rowsToCopy);
      if (code != 0) {
        blockDataDestroy(*ppExtraBlock);
        *ppExtraBlock = NULL;
      }
    }
  }
  return code;
}

static void printOutputProjBlock(SStreamRunnerTask* pTask, const SSDataBlock* pBlock, const SArray* pWinIdxArr) {
  if (stDebugFlag & DEBUG_DEBUG) {
    if (pBlock == NULL || pBlock->info.rows == 0) {
      stDebugL("output projection block is null or has no rows");
      return;
    }

#define tsBufferMax 8192
    char    tsString[tsBufferMax] = {0};
    char    tempBuffer[32] = {0};
    int32_t tsLen = 0;

    SColumnInfoData* pTsCol = taosArrayGet(pBlock->pDataBlock, 0);
    for (int32_t i = 0; i < pBlock->info.rows; ++i) {
      if (tsLen > 0) {
        tsString[tsLen] = ',';
        tsLen++;
      }
      if (colDataIsNull_s(pTsCol, i)) {
        TAOS_UNUSED(snprintf(tsString + tsLen, tsBufferMax - tsLen, "null"));
        tsLen += 4;
      } else {
        int64_t* pTsData = (int64_t*)colDataGetNumData(pTsCol, i);
        TAOS_UNUSED(snprintf(tempBuffer, sizeof(tempBuffer), "%"PRId64, *pTsData));
        TAOS_UNUSED(snprintf(tsString + tsLen, tsBufferMax - tsLen, "%s", tempBuffer));
        tsLen += strlen(tempBuffer);

      }
      if (tsLen >= tsBufferMax - 32) {
        ST_TASK_DLOG("output projection block ts:%s ...", tsString);
        tsLen = 0;
        tsString[0] = '\0';
      }
    }
    if (tsLen > 0) {
      ST_TASK_DLOG("output projection block ts:%s", tsString);
    }
    tsLen = 0;
    tsString[0] = '\0';
    if (pWinIdxArr && pWinIdxArr->size > 0) {
      for (int i = 0; i < pWinIdxArr->size; ++i) {
        int64_t idx = *(int64_t*)taosArrayGet(pWinIdxArr, i);
        snprintf(tempBuffer, sizeof(tempBuffer), "%"PRId64, idx);
        char* p = strncat(tsString, tempBuffer, tsBufferMax - tsLen - 1);
        tsLen += strlen(tempBuffer);
        if (tsLen >= tsBufferMax - 12) {
          ST_TASK_DLOG("output projection block win idx:%s ...", tsString);
          tsLen = 0;
          tsString[0] = '\0';
        }
      }
    }
    if (tsLen > 0) {
      ST_TASK_DLOG("output projection block win idx:%s", tsString);
    }
  }
}

static int32_t stRunnerMergeExternalWinOutput(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                              SSDataBlock** ppForceOutBlock, bool finished, bool* createTable,
                                              bool* pFailureClassified) {
  if (finished || ((*ppForceOutBlock) && (*ppForceOutBlock)->info.rows > 0)) {
    int32_t code = stRunnerMergeOutputBlock(pTask, pExec, *ppForceOutBlock, false, createTable, pFailureClassified);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t stRunnerTopTaskHandleExternalWinOutputBlock(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec,
                                                           SSDataBlock* pBlock, SSDataBlock** ppForceOutBlock,
                                                           int32_t* pNextOutIdx, bool finished, bool* createTable,
                                                           int64_t requestStartMonoUs, bool* pFailureClassified) {
  int32_t code = 0;
  int     lino = 0;
  int32_t startWinIdx = *pNextOutIdx;
  int32_t endWinIdx = 0;
  uint64_t notifyRows = 0;
  if (*ppForceOutBlock) blockDataCleanup(*ppForceOutBlock);

  if ((pTask->notification.pNotifyAddrUrls != NULL && pTask->notification.pNotifyAddrUrls->size > 0) ||
      (taosArrayGetSize(pExec->runtimeInfo.pForceOutputCols) > 0)) {
    if (pBlock == NULL || pBlock->info.rows == 0) {
      // no data in current block, force output all windows between last output window and current window
      while (*pNextOutIdx < pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals->size) {
        TAOS_CHECK_GOTO(streamForceOutput(pExec->pExecutor, ppForceOutBlock, *pNextOutIdx), &lino, _exit);
        stRunnerRecordReadyWindow(pTask, requestStartMonoUs,
                                  *ppForceOutBlock != NULL && (*ppForceOutBlock)->info.rows > 0);
        TAOS_CHECK_GOTO(streamPrepareNotification(pTask, pExec, *ppForceOutBlock, *pNextOutIdx, 0, 0, &notifyRows,
                                                  pFailureClassified),
                        &lino, _exit);
        // won't overflow, total rows should smaller than 4096
        (*pNextOutIdx)++;
      }
      if (startWinIdx < *pNextOutIdx) {
        TAOS_CHECK_GOTO(
            stRunnerMergeOutputBlock(pTask, pExec, *ppForceOutBlock, false, createTable, pFailureClassified), &lino,
            _exit);
        endWinIdx = *pNextOutIdx;
        TAOS_CHECK_GOTO(
            streamDoNotification(pTask, pExec, startWinIdx, endWinIdx, pExec->tbname, notifyRows, pFailureClassified),
            &lino, _exit);
      }
      return TSDB_CODE_SUCCESS;
    }

    // printOutputProjBlock(pTask, pBlock, pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx);
    SArray* pBlkWinIdx = pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx;
    if (pBlkWinIdx == NULL || taosArrayGetSize(pBlkWinIdx) == 0) {
      int32_t totalWins = (int32_t)taosArrayGetSize(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals);
      int32_t remainingWins = totalWins - *pNextOutIdx;
      if (remainingWins != 1) {
        code = TSDB_CODE_QRY_EXECUTOR_INTERNAL_ERROR;
        lino = __LINE__;
        ST_TASK_ELOG("missing external-window row index for output block, remainingWins:%d, rows:%" PRId64,
                     remainingWins, pBlock->info.rows);
        goto _exit;
      }

      stRunnerRecordReadyWindow(pTask, requestStartMonoUs, pBlock->info.rows > 0);
      TAOS_CHECK_GOTO(streamPrepareNotification(pTask, pExec, pBlock, *pNextOutIdx, 0, pBlock->info.rows - 1,
                                                &notifyRows, pFailureClassified),
                      &lino, _exit);
      (*pNextOutIdx)++;
      TAOS_CHECK_GOTO(
          stRunnerMergeExternalWinOutput(pTask, pExec, ppForceOutBlock, finished, createTable, pFailureClassified),
          &lino, _exit);
    } else {
      for (int32_t i = 0; i < taosArrayGetSize(pBlkWinIdx); ++i) {
        int64_t idx = *(int64_t*)taosArrayGet(pBlkWinIdx, i);
        int32_t winOutIdx = idx & 0xFFFFFFFF;
        int32_t rowStartIdx = idx >> 32;
        int32_t rowEndIdx = i + 1 < taosArrayGetSize(pBlkWinIdx)
                                ? (*(int64_t*)taosArrayGet(pBlkWinIdx, i + 1) >> 32) - 1
                                : (int32_t)pBlock->info.rows - 1;
        startWinIdx = TMIN(startWinIdx, winOutIdx);
        while (*pNextOutIdx < winOutIdx) {
          TAOS_CHECK_GOTO(streamForceOutput(pExec->pExecutor, ppForceOutBlock, *pNextOutIdx), &lino, _exit);
          stRunnerRecordReadyWindow(pTask, requestStartMonoUs,
                                    *ppForceOutBlock != NULL && (*ppForceOutBlock)->info.rows > 0);
          TAOS_CHECK_GOTO(streamPrepareNotification(pTask, pExec, *ppForceOutBlock, *pNextOutIdx, 0, 0, &notifyRows,
                                                    pFailureClassified),
                          &lino, _exit);
          (*pNextOutIdx)++;
        }
        if (*pNextOutIdx == winOutIdx) {
          stRunnerRecordReadyWindow(pTask, requestStartMonoUs, rowEndIdx >= rowStartIdx);
          (*pNextOutIdx)++;
        }
        TAOS_CHECK_GOTO(streamPrepareNotification(pTask, pExec, pBlock, winOutIdx, rowStartIdx, rowEndIdx, &notifyRows,
                                                  pFailureClassified),
                        &lino, _exit);
      }
      TAOS_CHECK_GOTO(
          stRunnerMergeExternalWinOutput(pTask, pExec, ppForceOutBlock, finished, createTable, pFailureClassified),
          &lino, _exit);
    }
  } else if (pBlock == NULL || pBlock->info.rows == 0) {
    while (*pNextOutIdx < pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals->size) {
      stRunnerRecordReadyWindow(pTask, requestStartMonoUs, false);
      (*pNextOutIdx)++;
    }
  } else {
    SArray* pBlkWinIdx = pExec->runtimeInfo.funcInfo.pStreamBlkWinIdx;
    if (pBlkWinIdx == NULL || taosArrayGetSize(pBlkWinIdx) == 0) {
      stRunnerRecordReadyWindow(pTask, requestStartMonoUs, true);
      (*pNextOutIdx)++;
    } else {
      for (int32_t i = 0; i < taosArrayGetSize(pBlkWinIdx); ++i) {
        int64_t idx = *(int64_t*)taosArrayGet(pBlkWinIdx, i);
        int32_t winOutIdx = idx & 0xFFFFFFFF;
        while (*pNextOutIdx < winOutIdx) {
          stRunnerRecordReadyWindow(pTask, requestStartMonoUs, false);
          (*pNextOutIdx)++;
        }
        if (*pNextOutIdx == winOutIdx) {
          stRunnerRecordReadyWindow(pTask, requestStartMonoUs, true);
          (*pNextOutIdx)++;
        }
      }
    }
  }

  if (pBlock) {  // && *pNextOutIdx < taosArrayGetSize(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals)
    TAOS_CHECK_GOTO(stRunnerMergeOutputBlock(pTask, pExec, pBlock, finished, createTable, pFailureClassified), &lino,
                    _exit);
  }
  endWinIdx = *pNextOutIdx;
  if (endWinIdx > startWinIdx) {
    TAOS_CHECK_GOTO(
        streamDoNotification(pTask, pExec, startWinIdx, endWinIdx, pExec->tbname, notifyRows, pFailureClassified),
        &lino, _exit);
  }
_exit:
  if (code != 0) {
    ST_TASK_ELOG("failed to handle output block, code:%s, lino:%d", tstrerror(code), lino);
  }
  return code;
}

static int32_t stRunnerBuildTask(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec) {
  int32_t vgId = pTask->task.nodeId;
  int64_t st = taosGetTimestampMs();
  int64_t streamId = pTask->task.streamId;
  int32_t taskId = pTask->task.taskId;
  int32_t code = 0;

  ST_TASK_DLOG("vgId:%d start to build stream task", vgId);
  SReadHandle handle = {0};
  handle.streamRtInfo = &pExec->runtimeInfo;
  handle.pMsgCb = &pTask->msgCb;
  //handle.pMsgCb = pTask->pMsgCb;
  handle.pWorkerCb = pTask->pWorkerCb;
  if (pTask->topTask && !pTask->notification.calcNotifyOnly) {
    SStreamInserterParam params = {.dbFName = pTask->output.outDbFName,
                                   .tbname = pExec->tbname,
                                   .pFields = pTask->output.outCols,
                                   .pTagFields = pTask->output.outTags,
                                   .suid = pTask->output.outStbUid,
                                   .tbType = pTask->output.outTblType,
                                   .sver = pTask->output.outStbVersion,
                                   .stbname = pTask->output.outSTbName,
                                   .pSinkHandle = NULL,
                                   .colCids = pTask->output.colCids,
                                   .tagCids = pTask->output.tagCids};
    code = qCreateStreamExecTaskInfo(&pExec->pExecutor, (void*)pExec->pPlan, &handle, &params, vgId, taskId);
    pExec->pSinkHandle = params.pSinkHandle;
  } else {
    code = qCreateStreamExecTaskInfo(&pExec->pExecutor, (void*)pExec->pPlan, &handle, NULL, vgId, taskId);
  }
  if (code) {
    ST_TASK_ELOG("failed to build task, code:%s", tstrerror(code));
    return code;
  }

  code = qSetTaskId(pExec->pExecutor, taskId, streamId);
  if (code) {
    return code;
  }

  if (code) {
    ST_TASK_ELOG("failed to set stream notify info, code:%s", tstrerror(code));
    return code;
  }

  double el = (taosGetTimestampMs() - st) / 1000.0;
  ST_TASK_DLOG("expand stream task completed, elapsed time:%.2fsec", el);

  return code;
}

static uint64_t stRunnerCountRequestWindows(const SSTriggerCalcRequest* pReq) {
  if (!pReq->isMultiGroupCalc) return taosArrayGetSize(pReq->params);
  if (pReq->pGroupCalcInfos == NULL) return 0;

  uint64_t                windows = 0;
  int32_t                 iter = 0;
  SSTriggerGroupCalcInfo* pGroup = tSimpleHashIterate(pReq->pGroupCalcInfos, NULL, &iter);
  while (pGroup != NULL) {
    uint64_t groupWindows = taosArrayGetSize(pGroup->pParams);
    windows = UINT64_MAX - windows < groupWindows ? UINT64_MAX : windows + groupWindows;
    pGroup = tSimpleHashIterate(pReq->pGroupCalcInfos, pGroup, &iter);
  }
  return windows;
}

static int32_t stRunnerValidateContextPolicy(const SStreamRunnerTask* pTask, const SSTriggerCalcRequest* pReq) {
  const bool nested = BIT_FLAG_TEST_MASK(pTask->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN);
  return tValidateSTriggerCalcRequestAncestorContext(pReq, nested);
}

int32_t stRunnerTaskExecute(SStreamRunnerTask* pTask, SSTriggerCalcRequest* pReq, int64_t requestStartMonoUs) {
  int32_t                     code = 0;
  int32_t                     lino = 0;
  bool                        createTable = false;
  bool                        failureClassified = false;
  uint64_t                    calcDurationUs = 0;
  int64_t                     statsNowMonoUs = 0;
  SSDataBlock*                pForceOutBlock = NULL;
  SStreamRunnerTaskExecution* pExec = NULL;
  ST_TASK_DLOG("[runner calc]start, gid:%" PRId64 ", topTask: %d, brandNew:%d", pReq->gid, pTask->topTask, pReq->brandNew);

  code = stRunnerValidateContextPolicy(pTask, pReq);
  if (code != TSDB_CODE_SUCCESS) return code;

  stTaskStatsRecordRunnerRequest(pTask->pStats, stRunnerCountRequestWindows(pReq), requestStartMonoUs,
                                 taosGetTimestampMs());

  // Always markRunning=true: slot goes into running list during execution
  // and is unconditionally released back to free list at end.
  bool markRunning = true;
  code = stRunnerTaskAcquireExec(pTask, pReq->execId, markRunning, &pExec);
  if (code != 0) {
    ST_TASK_ELOG("failed to get task exec for stream code:%s", tstrerror(code));
    stRunnerRecordFailure(pTask, STREAM_RUNNER_FAILURE_CALC, NULL);
    stTaskStatsRecordRunnerCalcDuration(pTask->pStats, 0, streamTaskGetMonotonicUs(), taosGetTimestampMs());
    return code;
  }
  pExec->runtimeInfo.pInputStatsParam = pTask->pStats;
  pExec->runtimeInfo.inputStatsFp = stRunnerRecordInput;
  pTask->task.status = STREAM_STATUS_RUNNING;
  pTask->task.sessionId = pReq->sessionId;
  tDestroyStreamContextPolicy(&pExec->runtimeInfo.funcInfo.pContextPolicy);
  tDestroyStreamAncestorContext(&pExec->runtimeInfo.funcInfo.pAncestorContext);
  TSWAP(pExec->runtimeInfo.funcInfo.pContextPolicy, pReq->pContextPolicy);
  TSWAP(pExec->runtimeInfo.funcInfo.pAncestorContext, pReq->pAncestorContext);
  // Empty arrays decode as NULL. A new request must still replace state left by a reused exec.
  if (pReq->groupColVals || pReq->brandNew) {
    TSWAP(pExec->runtimeInfo.funcInfo.pStreamPartColVals, pReq->groupColVals);
  }
  if (pReq->params || pReq->brandNew) {
    TSWAP(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals, pReq->params);
  }
  pExec->runtimeInfo.funcInfo.groupId = pReq->gid;
  pExec->runtimeInfo.pForceOutputCols = pTask->forceOutCols;
  pExec->runtimeInfo.funcInfo.sessionId = pReq->sessionId;
  pExec->runtimeInfo.funcInfo.triggerType = pReq->triggerType;
  pExec->runtimeInfo.funcInfo.isWindowTrigger = pReq->isWindowTrigger;
  pExec->runtimeInfo.funcInfo.precision = pReq->precision;
  pExec->runtimeInfo.funcInfo.rollupTbCount = pReq->rollupTbCount;
  pExec->runtimeInfo.funcInfo.addOptions = pTask->addOptions;

  int32_t winNum = taosArrayGetSize(pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals);
  STREAM_CHECK_CONDITION_GOTO(winNum > STREAM_CALC_REQ_MAX_WIN_NUM, TSDB_CODE_STREAM_TASK_IVLD_STATUS);

  if (!pExec->pExecutor) {
    STREAM_CHECK_RET_GOTO(stRunnerBuildTask(pTask, pExec));
  } else if (pReq->brandNew) {
    STREAM_CHECK_RET_GOTO(stRunnerResetTaskExec(pTask, pExec, pTask->output.outTblType == TSDB_NORMAL_TABLE));
  }

  pExec->runtimeInfo.funcInfo.curIdx = pReq->curWinIdx;
  pExec->runtimeInfo.funcInfo.curOutIdx = pReq->curWinIdx;
  createTable = (pReq->createTable != 0);
  int32_t nextOutIdx = pExec->runtimeInfo.funcInfo.curOutIdx;

  ST_TASK_DLOG("[runner calc]start to loop, winNum:%d, extWindow:%d, nextOutIdx:%d, gid:%" PRId64, winNum,
               pExec->runtimeInfo.funcInfo.withExternalWindow, nextOutIdx, pReq->gid);

  // A zero-window request only initializes the output table; it must not calculate or notify.
  if (winNum == 0) {
    if (pTask->topTask && createTable) {
      code = stRunnerOutputBlock(pTask, pExec, NULL, &createTable, &failureClassified);
      if (code != TSDB_CODE_SUCCESS) {
        lino = __LINE__;
      } else if (createTable) {
        code = TSDB_CODE_MND_STREAM_TABLE_NOT_CREATE;
        lino = __LINE__;
      }
    }
    goto _cleanup;
  }

  while (pExec->runtimeInfo.funcInfo.curOutIdx < winNum && code == 0) {
    if (stRunnerTaskWaitQuit(pTask)) {
      ST_TASK_ILOG("[runner calc]quit, skip calc. gid:%" PRId64 ", status:%d", pReq->gid, pTask->task.status);
      break;
    }
    bool         finished = false;
    SSDataBlock* pBlock = NULL;
    SRunnerBlockingStats blockingStats = {0};
    pExec->runtimeInfo.pBlockingStatsParam = &blockingStats;
    pExec->runtimeInfo.blockingStatsFp = stRunnerRecordBlocking;
    int64_t calcStartMonoUs = streamTaskGetMonotonicUs();
    code = streamExecuteTask(pExec->pExecutor, &pBlock, &finished);
    pExec->runtimeInfo.pBlockingStatsParam = NULL;
    pExec->runtimeInfo.blockingStatsFp = NULL;
    int64_t calcEndMonoUs = streamTaskGetMonotonicUs();
    if (calcEndMonoUs >= calcStartMonoUs) {
      uint64_t durationUs = (uint64_t)(calcEndMonoUs - calcStartMonoUs);
      durationUs -= TMIN(durationUs, blockingStats.durationUs);
      calcDurationUs = UINT64_MAX - calcDurationUs < durationUs ? UINT64_MAX : calcDurationUs + durationUs;
    }
    if (code != TSDB_CODE_SUCCESS) {
      lino = __LINE__;
      goto end;
    }
    printDataBlock(pBlock, __func__, "streamExecuteTask block", pTask->task.streamId);
    if (pTask->topTask) {
      if (pExec->runtimeInfo.funcInfo.withExternalWindow) {
        ST_TASK_DLOG("[runner calc] external window: %d, curIdx: %d, curOutIdx: %d, nextOutIdx: %d, gid:%" PRId64,
                     pExec->runtimeInfo.funcInfo.withExternalWindow, pExec->runtimeInfo.funcInfo.curIdx,
                     pExec->runtimeInfo.funcInfo.curOutIdx, nextOutIdx, pReq->gid);
        STREAM_CHECK_RET_GOTO(stRunnerTopTaskHandleExternalWinOutputBlock(pTask, pExec, pBlock, &pForceOutBlock,
                                                                          &nextOutIdx, finished, &createTable,
                                                                          requestStartMonoUs, &failureClassified));
      } else {
        // no external window, only one window to calc, force output and output block
        if (!pBlock || pBlock->info.rows == 0) {
          if (nextOutIdx <= pExec->runtimeInfo.funcInfo.curOutIdx) {
            if (pForceOutBlock) blockDataCleanup(pForceOutBlock);
            code = streamForceOutput(pExec->pExecutor, &pForceOutBlock, nextOutIdx);
            if (code == 0) {
              stRunnerRecordReadyWindow(pTask, requestStartMonoUs,
                                        pForceOutBlock != NULL && pForceOutBlock->info.rows > 0);
              code = stRunnerHandleSingleWinResultBlock(pTask, pExec, pForceOutBlock, &createTable, &failureClassified);
            }
            ++nextOutIdx;
          }
          ST_TASK_DLOG("[runner calc]gid:%" PRId64 " result has no data, status:%d", pReq->gid, pTask->task.status);
        } else {
          ST_TASK_DLOG("[runner calc]gid:%" PRId64
                       " non external window, %d, curIdx: %d, curOutIdx: %d, nextOutIdx: %d",
                       pReq->gid, pExec->runtimeInfo.funcInfo.withExternalWindow, pExec->runtimeInfo.funcInfo.curIdx,
                       pExec->runtimeInfo.funcInfo.curOutIdx, nextOutIdx);
          if (nextOutIdx <= pExec->runtimeInfo.funcInfo.curOutIdx) {
            stRunnerRecordReadyWindow(pTask, requestStartMonoUs, true);
          }
          code = stRunnerHandleSingleWinResultBlock(pTask, pExec, pBlock, &createTable, &failureClassified);
          nextOutIdx = pExec->runtimeInfo.funcInfo.curOutIdx + 1;
        }
        if (finished && code == TSDB_CODE_SUCCESS) {
          ++pExec->runtimeInfo.funcInfo.curIdx;
          ++pExec->runtimeInfo.funcInfo.curOutIdx;
          ST_TASK_DLOG("[runner calc]gid:%" PRId64 " finished, %d, curIdx: %d, curOutIdx: %d, nextOutIdx: %d",
                       pReq->gid, pExec->runtimeInfo.funcInfo.withExternalWindow, pExec->runtimeInfo.funcInfo.curIdx,
                       pExec->runtimeInfo.funcInfo.curOutIdx, nextOutIdx);
        }
      }
    } else {
      // Scalar subquery (non-topTask): return one block per call, then release immediately.
      // The scalar subQ result is small enough to fit in one response (e.g. 50 groupids).
      if (pBlock && pBlock->info.rows > 0) {
        STREAM_CHECK_RET_GOTO(createOneDataBlock(pBlock, true, (SSDataBlock**)&pReq->pOutBlock));
        stRunnerRecordReadyWindow(pTask, requestStartMonoUs, true);
        stRunnerRecordOutput(pTask, pBlock->info.rows, 1);
      } else {
        stRunnerRecordReadyWindow(pTask, requestStartMonoUs, false);
      }
      pReq->execId = pExec->runtimeInfo.execId;
      break;
    }
    if (finished && code == TSDB_CODE_SUCCESS) {
      code = stRunnerResetTaskExec(pTask, pExec, true);
      if (code != 0) {
        ST_TASK_ELOG("failed to reset task exec, code:%s", tstrerror(code));
        break;
      }
      if (pExec->runtimeInfo.funcInfo.withExternalWindow) break;
    }
  }

end:

  if (TSDB_CODE_SUCCESS == code && ((pExec->pOutBlock && pExec->pOutBlock->info.rows > 0) || createTable)) {
    if (!pExec->pOutBlock || pExec->pOutBlock->info.rows == 0) {
      ST_TASK_DLOG(
          "output block is empty but createTable is true, do stRunnerOutputBlock to initTableInfo, gid:%" PRId64,
          pReq->gid);
    }
    code = stRunnerOutputBlock(pTask, pExec, pExec->pOutBlock, &createTable, &failureClassified);
    TAOS_CHECK_GOTO(code, &lino, end);
    code = streamDoNotificationCurrentWins(
        pTask, pExec, pExec->tbname,
        pExec->pOutBlock != NULL && pExec->pOutBlock->info.rows > 0 ? (uint64_t)pExec->pOutBlock->info.rows : 0,
        &failureClassified);
    TAOS_CHECK_GOTO(code, &lino, end);
    if (pExec->pOutBlock) {
      blockDataCleanup(pExec->pOutBlock);
    }
  }

_cleanup:

  statsNowMonoUs = streamTaskGetMonotonicUs();
  stTaskStatsRecordRunnerCalcDuration(pTask->pStats, calcDurationUs, statsNowMonoUs, taosGetTimestampMs());
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_STREAM_TABLE_NOT_CREATE && !failureClassified) {
    stRunnerRecordFailure(pTask, STREAM_RUNNER_FAILURE_CALC, &failureClassified);
  }

  ST_TASK_DLOG("execId %d stop to run, gid:%" PRId64, pExec->runtimeInfo.execId, pReq->gid);

  stRunnerTaskReleaseExec(pTask, pExec);

  if (pForceOutBlock != NULL) blockDataDestroy(pForceOutBlock);
  if (code) {
    ST_TASK_ELOG("[runner calc]faild gid:%" PRId64 ", lino:%d code:%s", pReq->gid, lino, tstrerror(code));
    if (code == TSDB_CODE_STREAM_VTABLE_NEED_REDEPLOY) {
      return TSDB_CODE_STREAM_VTABLE_NEED_REDEPLOY;
    }
    if (code != TSDB_CODE_MND_STREAM_TABLE_NOT_CREATE) {
      pTask->task.status = STREAM_STATUS_FAILED;
    }
  } else {
    ST_TASK_DLOG("[runner calc]success, gid:%" PRId64 ",, status:%d", pReq->gid, pTask->task.status);
  }
  return code;
}

static int32_t streamBuildTask(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec) {
  int32_t vgId = pTask->task.nodeId;
  int64_t st = taosGetTimestampMs();
  int64_t streamId = pTask->task.streamId;
  int32_t taskId = pTask->task.taskId;
  int32_t code = 0;

  ST_TASK_DLOG("vgId:%d start to build stream task", vgId);
  SReadHandle handle = {0};
  handle.streamRtInfo = &pExec->runtimeInfo;
  handle.pMsgCb = &pTask->msgCb;
  //handle.pMsgCb = pTask->pMsgCb;
  handle.pWorkerCb = pTask->pWorkerCb;
  if (pTask->topTask && !pTask->notification.calcNotifyOnly) {
    SStreamInserterParam params = {.dbFName = pTask->output.outDbFName,
                                   .tbname = pExec->tbname,
                                   .pFields = pTask->output.outCols,
                                   .pTagFields = pTask->output.outTags,
                                   .suid = pTask->output.outStbUid,
                                   .tbType = pTask->output.outTblType,
                                   .sver = pTask->output.outStbVersion,
                                   .stbname = pTask->output.outSTbName,
                                   .pSinkHandle = NULL,
                                   .colCids = pTask->output.colCids,
                                   .tagCids = pTask->output.tagCids};
    code = qCreateStreamExecTaskInfo(&pExec->pExecutor, (void*)pExec->pPlan, &handle, &params, vgId, taskId);
    pExec->pSinkHandle = params.pSinkHandle;
  } else {
    code = qCreateStreamExecTaskInfo(&pExec->pExecutor, (void*)pExec->pPlan, &handle, NULL, vgId, taskId);
  }
  if (code) {
    ST_TASK_ELOG("failed to build task, code:%s", tstrerror(code));
    return code;
  }

  code = qSetTaskId(pExec->pExecutor, taskId, streamId);
  if (code) {
    return code;
  }

  if (code) {
    ST_TASK_ELOG("failed to set stream notify info, code:%s", tstrerror(code));
    return code;
  }

  double el = (taosGetTimestampMs() - st) / 1000.0;
  ST_TASK_ILOG("The %dth runner exec built completed, elapsed time:%.2fsec", atomic_fetch_add_32(&pTask->execMgr.execBuildNum, 1), el);

  return code;
}

void stClearStreamCacheReadScope(SStreamCacheReadInfo* pReadInfo) {
  if (pReadInfo == NULL) {
    return;
  }
  taosArrayDestroy(pReadInfo->cacheScope.lineage.pScopes);
  pReadInfo->cacheScope = (SStreamCacheScope){0};
  pReadInfo->readInfoIndex = -1;
  pReadInfo->hasCacheScope = false;
  pReadInfo->pRuntime = NULL;
}

static int32_t stCopyStreamCacheReadScope(const SStreamCacheScope* pSource, int32_t readInfoIndex,
                                          SStreamCacheReadInfo* pReadInfo) {
  SArray* pScopes = NULL;
  if (taosArrayGetSize(pSource->lineage.pScopes) > 0) {
    pScopes = taosArrayDup(pSource->lineage.pScopes, NULL);
    if (pScopes == NULL) {
      return terrno;
    }
  }
  pReadInfo->cacheScope.gid = pSource->gid;
  pReadInfo->cacheScope.lineage.pScopes = pScopes;
  pReadInfo->readInfoIndex = readInfoIndex;
  pReadInfo->hasCacheScope = true;
  return TSDB_CODE_SUCCESS;
}

int32_t stBindStreamCacheReadScopeForTask(const SStreamRuntimeFuncInfo* pRuntime, bool nested, int32_t expectedNodeId,
                                          SStreamCacheReadInfo* pReadInfo) {
  if (pRuntime == NULL || pReadInfo == NULL || pReadInfo->hasCacheScope) {
    return TSDB_CODE_INVALID_PARA;
  }
  if (!nested) {
    SStreamCacheScope legacyScope = {.gid = pRuntime->groupId};
    return stCopyStreamCacheReadScope(&legacyScope, -1, pReadInfo);
  }
  if (pRuntime->pAncestorContext == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  if (pRuntime->isMultiGroupCalc) {
    if (taosArrayGetSize(pRuntime->pAncestorContext->pReadScopeBindings) != 1) {
      return TSDB_CODE_INVALID_PARA;
    }
    const SStreamReadScopeBinding* pBinding = taosArrayGet(pRuntime->pAncestorContext->pReadScopeBindings, 0);
    if (pBinding == NULL || pBinding->vgId != expectedNodeId || pBinding->scope.gid != pRuntime->groupId) {
      return TSDB_CODE_INVALID_PARA;
    }
    return stCopyStreamCacheReadScope(&pBinding->scope, pBinding->readInfoIndex, pReadInfo);
  }

  const SStreamAncestorParamContext* pMatch = NULL;
  int32_t                            count = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pRuntime->pAncestorContext->pParamContexts); ++i) {
    const SStreamAncestorParamContext* pParam = taosArrayGet(pRuntime->pAncestorContext->pParamContexts, i);
    if (pParam != NULL && pParam->leafIdentity.gid == pRuntime->groupId && pParam->paramIndex == pRuntime->curIdx) {
      pMatch = pParam;
      ++count;
    }
  }
  if (count != 1) {
    return TSDB_CODE_INVALID_PARA;
  }
  SStreamCacheScope scope = {.gid = pMatch->leafIdentity.gid, .lineage = pMatch->leafIdentity.lineage};
  return stCopyStreamCacheReadScope(&scope, -1, pReadInfo);
}

int32_t stBindStreamCacheReadScope(const SStreamRuntimeFuncInfo* pRuntime, SStreamCacheReadInfo* pReadInfo) {
  if (pRuntime == NULL || pReadInfo == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  const bool nested = BIT_FLAG_TEST_MASK(pRuntime->addOptions, STREAM_OPTION_NESTED_WINDOW_PLAN);
  if (nested) {
    int32_t code = tAdmitStreamContext(pRuntime->pContextPolicy, pRuntime->pAncestorContext, true);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
  }
  return stBindStreamCacheReadScopeForTask(pRuntime, nested, pRuntime->curNodeId, pReadInfo);
}

int32_t stRunnerFetchDataFromCache(SStreamCacheReadInfo* pInfo, bool* finished) {
  int64_t streamId = pInfo == NULL ? 0 : pInfo->taskInfo.streamId;
  int32_t code = readStreamDataCache(pInfo, finished);
  if (code) {
    stsError("%s failed, error:%s", __FUNCTION__, tstrerror(code));
  }
  return code;
}

int32_t stRunnerTaskDropTable(SStreamRunnerTask* pTask, SSTriggerDropRequest* pReq) {

  int32_t  code = dropStreamTable(&pTask->msgCb, (void*)&pTask->output, pReq);
  if(code == TSDB_CODE_STREAM_INSERT_TBINFO_NOT_FOUND) {
      char    tbname[TSDB_TABLE_NAME_LEN];
      SStreamRuntimeFuncInfo pStreamRuntimeInfo = {.pStreamPartColVals = pReq->groupColVals};
      code = streamCalcOutputTbName(pTask->pSubTableExpr, tbname, &pStreamRuntimeInfo);
      if(code == TSDB_CODE_SUCCESS) {
          code = dropStreamTableByTbName(&pTask->msgCb, pTask->output.outDbFName, pReq, tbname);
      }
  }
  return code;
}

int32_t stReaderAppendMgmtReq(SStreamRunnerTask* pTask, SArray** ppRes, int32_t execId, int64_t uid, SArray* pReq) {
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;
  if (NULL == *ppRes) {
    *ppRes = taosArrayInit(4, sizeof(SStreamOReaderDeployReq));
    TSDB_CHECK_NULL(*ppRes, code, lino, _exit, terrno);
  }

  SStreamOReaderDeployReq req = {.execId = execId, .vgIds = pReq, .uid = uid};
  TSDB_CHECK_NULL(taosArrayPush(*ppRes, &req), code, lino, _exit, terrno);

_exit:

  if (code) {
    ST_TASK_ELOG("%s failed at lino %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}

int32_t stRunnerBuildTaskMgmtReq(SStreamRunnerTask* pTask) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->execMgr;
  int32_t                   code = 0, lino = 0;
  code = taosThreadMutexLock(&pMgr->lock);
  if (code != 0) {
    ST_TASK_ELOG("%s failed to lock stream runner task exec mgr mutex, code:%s", __func__, tstrerror(code));
    return code;
  }

  SArray* pMgmgReq = NULL;
  SStreamVtableDeployInfo* pDeploy = NULL;
  SListNode* pNode = tdListGetHead(pMgr->pFreeExecs);
  while (pNode) {
    SStreamRunnerTaskExecution* pExec = (SStreamRunnerTaskExecution*)pNode->data;
    pDeploy = &pExec->runtimeInfo.vtableDeployInfo;
    SArray* pReq = atomic_load_ptr(&pDeploy->addVgIds);
    if (pReq && pReq == atomic_val_compare_exchange_ptr(&pDeploy->addVgIds, pReq, NULL)) {
      TAOS_CHECK_EXIT(stReaderAppendMgmtReq(pTask, &pMgmgReq, pExec->runtimeInfo.execId, pDeploy->uid, pReq));
    }
    pNode = pNode->dl_next_;
  }

  pNode = tdListGetHead(pMgr->pRunningExecs);
  while (pNode) {
    SStreamRunnerTaskExecution* pExec = (SStreamRunnerTaskExecution*)pNode->data;
    pDeploy = &pExec->runtimeInfo.vtableDeployInfo;
    SArray* pReq = atomic_load_ptr(&pDeploy->addVgIds);
    if (pReq && pReq == atomic_val_compare_exchange_ptr(&pDeploy->addVgIds, pReq, NULL)) {
      TAOS_CHECK_EXIT(stReaderAppendMgmtReq(pTask, &pMgmgReq, pExec->runtimeInfo.execId, pDeploy->uid, pReq));
    }
    pNode = pNode->dl_next_;
  }

  if (pMgmgReq && taosArrayGetSize(pMgmgReq) > 0) {
    SStreamMgmtReq *pReq = taosMemoryCalloc(1, sizeof(SStreamMgmtReq));
    QUERY_CHECK_NULL(pReq, code, lino, _exit, terrno);
    pReq->reqId = atomic_fetch_add_64(&pTask->mgmtReqId, 1);
    pReq->type = STREAM_MGMT_REQ_RUNNER_ORIGTBL_READER;
    pReq->cont.pReqs = pMgmgReq;

    ST_TASK_DLOG("task mgmtReq built with %d exec reqs", (int32_t)taosArrayGetSize(pMgmgReq));
    atomic_store_ptr(&pTask->task.pMgmtReq, pReq);
  }

  TAOS_UNUSED(taosThreadMutexUnlock(&pMgr->lock));

  return code;

_exit:

  TAOS_UNUSED(taosThreadMutexUnlock(&pMgr->lock));

  if (code) {
    taosArrayDestroyEx(pMgmgReq, tFreeRunnerOReaderDeployReq);
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  
  return code;
}

int32_t stRunnerSetMgmtRsp(SStreamRunnerTask* pTask, SArray* pRspList) {
  int32_t code = TSDB_CODE_SUCCESS, lino = 0;
  int32_t rspNum = taosArrayGetSize(pRspList);
  SStreamRunnerTaskExecution* pExec = NULL;
  for (int32_t i = 0; i < rspNum; ++i) {
    SStreamOReaderDeployRsp* pRsp = taosArrayGet(pRspList, i);
    TAOS_CHECK_EXIT(stRunnerTaskAcquireExec(pTask, pRsp->execId, false, &pExec));
    TSWAP(pExec->runtimeInfo.vtableDeployInfo.addedVgInfo, pRsp->vgList);
  }

_exit:

  if (code) {
    ST_TASK_ELOG("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  return code;
}
