#include "streamRunner.h"
#include "executor.h"
#include "plannodes.h"

static const int32_t taskConcurrentExecutionNum = 4;  // TODO wjm make it configurable

static int32_t streamBuildTask(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pTaskExec);

static int32_t stRunnerInitTaskExecMgr(SStreamRunnerTask* pTask) {
  SStreamRunnerTaskExecMgr*  pMgr = &pTask->pExecMgr;
  SStreamRunnerTaskExecution exec = {.pExecutor = NULL, .pPlan = pTask->pPlan};
  // decode plan into queryPlan
  int32_t                    code = 0;
  code = taosThreadMutexInit(&pMgr->lock, 0);
  if (code != 0) {
    ST_TASK_ELOG("failed to init stream runner task mgr mutex, code:%s", tstrerror(code));
    return code;
  }
  pMgr->pFreeExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  if (!pMgr->pFreeExecs) return terrno;

  for (int32_t i = 0; i < taskConcurrentExecutionNum && code == 0; ++i) {
    exec.runtimeInfo.execId = i;
    code = tdListAppend(pMgr->pFreeExecs, &exec);
    if (code != 0) {
      ST_TASK_ELOG("failed to append task exec mgr:%s", tstrerror(code));
    }
  }
  if (code != 0) return code;

  pMgr->pRunningExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  if (!pMgr->pRunningExecs) return terrno;
  return 0;
}

static void stRunnerDestroyTaskExecution(void* pExec) {
  SStreamRunnerTaskExecution* pExecution = pExec;
  pExecution->pPlan = 0;
  streamDestroyExecTask(pExecution->pExecutor);
}

static void stRunnerDestroyTaskExecMgr(SStreamRunnerTask* pTask) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->pExecMgr;
  pMgr->exit = true;
  taosThreadMutexLock(&pMgr->lock);
  if (pMgr->pFreeExecs->dl_neles_ > 0) {
    tdListFreeP(pMgr->pFreeExecs, stRunnerDestroyTaskExecution);
  }
  taosThreadMutexUnlock(&pMgr->lock);
}

static int32_t stRunnerTaskExecMgrAcquireExec(SStreamRunnerTask* pTask, int32_t execId, SStreamRunnerTaskExecution** ppExec) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->pExecMgr;
  int32_t                   code = 0;
  taosThreadMutexLock(&pMgr->lock);
  if (pMgr->exit) {
    code = TSDB_CODE_STREAM_TASK_NOT_EXIST;
    ST_TASK_WLOG("task has been undeployed: %s", tstrerror(code));
  } else {
    if (execId == -1) {
      if (pMgr->pFreeExecs->dl_neles_ > 0) {
        SListNode* pNode = tdListPopHead(pMgr->pFreeExecs);
        tdListAppendNode(pTask->pExecMgr.pRunningExecs, pNode);
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
          tdListPopNode(pMgr->pFreeExecs, pNode);
          tdListAppendNode(pMgr->pRunningExecs, pNode);
          *ppExec = pExec;
          break;
        }
        pNode = pNode->dl_next_;
      }
      if (!*ppExec) {
        code = TSDB_CODE_STREAM_TASK_IVLD_STATUS;
        ST_TASK_ELOG("failed to get task exec, invalid execId:%d", execId);
      }
    }
  }
  taosThreadMutexUnlock(&pMgr->lock);
  return code;
}

static void stRunnerTaskExecMgrReleaseExec(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution* pExec) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->pExecMgr;
  taosThreadMutexLock(&pMgr->lock);
  if (pMgr->exit) {
    if (pMgr->pRunningExecs->dl_neles_ == 1) {
      tdListFreeP(pMgr->pRunningExecs, stRunnerDestroyTaskExecution);
    }
  } else {
    SListNode* pNode = listNode(pExec);
    pNode = tdListPopNode(pMgr->pRunningExecs, pNode);
    tdListAppendNode(pMgr->pFreeExecs, pNode);
  }
  taosThreadMutexUnlock(&pMgr->lock);
}

int32_t stRunnerTaskDeploy(SStreamRunnerTask* pTask, const SStreamRunnerDeployMsg* pMsg) {
  ST_TASK_ILOG("deploy runner task for %s.%s", pMsg->outDBFName, pMsg->outTblName);
  pTask->pPlan = pMsg->pPlan;  // TODO wjm do we need to deep copy this char*
  pTask->forceOutCols = pMsg->forceOutCols;
  int32_t code = stRunnerInitTaskExecMgr(pTask);
  if (code != 0) {
    ST_TASK_ELOG("failed to init task exec mgr code:%s", tstrerror(code));
    pTask->task.status = STREAM_STATUS_FAILED;
    return code;
  }
  code = nodesStringToNode(pMsg->subTblNameExpr, (SNode**)&pTask->pSubTableExpr);
  if (code != 0) {
    ST_TASK_ELOG("failed to deserialize sub table expr: %s", tstrerror(code));
    pTask->task.status = STREAM_STATUS_FAILED;
    return code;
  }

  pTask->task.status = STREAM_STATUS_INIT;

  return 0;
}

int32_t stRunnerTaskUndeploy(SStreamRunnerTask* pTask, const SStreamUndeployTaskMsg* pMsg, taskUndeplyCallback cb) {
  nodesDestroyNode(pTask->pSubTableExpr);
  stRunnerDestroyTaskExecMgr(pTask);

  (*cb)(pTask);

  return 0;
}

static int32_t streamResetTaskExec(SStreamRunnerTaskExecution* pExec) {
  int32_t code = 0;
  pExec->tbname[0] = '\0';
  code = streamClearStatesForOperators(pExec->pExecutor);
  return code;
}

int32_t stRunnerTaskExecute(SStreamRunnerTask* pTask, SSTriggerCalcRequest* pReq) {
  SStreamRunnerTaskExecution* pExec = NULL;

  int32_t code = stRunnerTaskExecMgrAcquireExec(pTask, pReq->execId, &pExec);
  if (code != 0) {
    ST_TASK_ELOG("failed to get task exec for stream code:%s", tstrerror(code));
    return code;
  }

  pTask->task.sessionId = pReq->sessionId;
  pExec->runtimeInfo.funcInfo.pStreamPesudoFuncVals = pReq->groupColVals;
  pExec->runtimeInfo.funcInfo.pStreamPartColVals = pReq->params;
  pExec->runtimeInfo.funcInfo.groupId = pReq->gid;
  pExec->runtimeInfo.pForceOutputCols = pTask->forceOutCols;
  if (!pExec->pExecutor) {
    code = streamBuildTask(pTask, pExec->pExecutor);
  } else {
    if (1) // TODO wjm
      streamResetTaskExec(pExec);
  }

  streamSetTaskRuntimeInfo(pExec->pExecutor, &pExec->runtimeInfo);

  SSDataBlock* pBlock = NULL;
  uint64_t     ts = 0;
  if (code == 0) {
    code = streamExecuteTask(pExec->pExecutor, &pBlock, &ts);
  }
  if (code != 0) {
    ST_TASK_ELOG("failed to exec task code: %s", tstrerror(code));
  } else {
    if (pBlock && pBlock->info.rows > 0) {
      if (pExec->tbname[0] == '\0')
        code = streamCalcOutputTbName(pTask->pSubTableExpr, pExec->tbname, &pExec->runtimeInfo.funcInfo);
      if (code != 0) {
        ST_TASK_ELOG("failed to calc output tbname: %s", tstrerror(code));
      } else {
        // TODO wjm dump blocks to DataInserter or return to caller
      }
    }
  }
  // free the block data?
  stRunnerTaskExecMgrReleaseExec(pTask, pExec);
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
  code = qCreateStreamExecTaskInfo(&pExec->pExecutor, (void*)pExec->pPlan, &handle, vgId, taskId);
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
