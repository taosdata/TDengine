#include "streamRunner.h"
#include "executor.h"

static const int32_t taskConcurrentExecutionNum = 4;  // TODO wjm make it configurable

static int32_t streamBuildTask(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution *pTaskExec);

static int32_t stRunnerInitTaskExecMgr(SStreamRunnerTask* pTask) {
  SStreamRunnerTaskExecMgr*  pMgr = &pTask->pExecMgr;
  SStreamRunnerTaskExecution exec = {.pExecutor = NULL, .pPlan = pTask->pPlan};
  int32_t                    code = 0;
  code = taosThreadMutexInit(&pMgr->lock, 0);
  if (code != 0) {
    stError("failed to init stream runner task mgr mutex(%" PRId64 ", %" PRId64 "), code:%s",
            pTask->streamTask.streamId, pTask->streamTask.taskId, tstrerror(code));
    return code;
  }
  pMgr->pFreeExecs = tdListNew(sizeof(SStreamRunnerTaskExecution));
  if (!pMgr->pFreeExecs) return terrno;

  for (int32_t i = 0; i < taskConcurrentExecutionNum && code == 0; ++i) {
    code = tdListAppend(pMgr->pFreeExecs, &exec);
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
  pMgr->exit= true;
  taosThreadMutexLock(&pMgr->lock);
  if (pMgr->pFreeExecs->dl_neles_ > 0) {
    tdListFreeP(pMgr->pFreeExecs, stRunnerDestroyTaskExecution);
  }
  taosThreadMutexUnlock(&pMgr->lock);
}

static int32_t stRunnerTaskExecMgrAcquireExec(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution** ppExec) {
  SStreamRunnerTaskExecMgr* pMgr = &pTask->pExecMgr;
  int32_t                   code = 0;
  taosThreadMutexLock(&pMgr->lock);
  if (pMgr->exit) {
    stWarn("task has been undeployed:(%)" PRId64 ",%" PRId64 ")", pTask->streamTask.streamId, pTask->streamTask.taskId);
    code = TSDB_CODE_STREAM_TASK_NOT_EXIST;
  } else {
    if (pMgr->pFreeExecs->dl_neles_ > 0) {
      SListNode* pNode = tdListPopHead(pMgr->pFreeExecs);
      tdListAppendNode(pTask->pExecMgr.pRunningExecs, pNode);
      *ppExec = (SStreamRunnerTaskExecution*)pNode->data;
    } else {
      stError("too many exec tasks scheduled (%" PRId64 ",%" PRId64 ")", pTask->streamTask.streamId,
          pTask->streamTask.taskId);
      code = TSDB_CODE_STREAM_TASK_IVLD_STATUS;
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

int32_t stRunnerTaskDeploy(SStreamRunnerTask** ppTask, const SStreamRunnerDeployMsg* pMsg) {
  SStreamRunnerTask* pTask = taosMemoryCalloc(1, sizeof(SStreamRunnerTask));
  if (!pTask) {
    stError("failed to allocate memory for stream task (%" PRId64 ", %" PRId64 "), code:%s", pMsg->task.streamId,
            pMsg->task.taskId, tstrerror(terrno));
    return terrno;
  }
  pTask->streamTask = pMsg->task;
  pTask->pPlan = pMsg->pPlan;  // TODO wjm do we need to deep copy this char*
  pTask->handle = pMsg->handle;
  int32_t code = stRunnerInitTaskExecMgr(pTask);
  if (code != 0) {
    stError("failed to init task exec mgr %" PRId64 ", %" PRId64 "), code:%s", pTask->streamTask.streamId,
            pTask->streamTask.taskId, tstrerror(code));
    taosMemoryFree(pTask);
    return code;
  }

  *ppTask = pTask;
  return 0;
}

int32_t stRunnerTaskUndeploy(SStreamRunnerTask* pTask, const SStreamRunnerUndeployMsg* pMsg) {
  taosMemoryFree(pTask);
  stRunnerDestroyTaskExecMgr(pTask);
  return 0;
}

int32_t stRunnerTaskExecute(SStreamRunnerTask* pTask, const char* pMsg, int32_t msgLen) {
  SStreamRunnerTaskExecution* pExec = NULL;
  int32_t                     code = stRunnerTaskExecMgrAcquireExec(pTask, &pExec);
  if (code != 0) {
    stError("failed to get task exec for stream: (%" PRId64 ", %" PRId64 "), code:%s", pTask->streamTask.streamId,
            pTask->streamTask.taskId, tstrerror(code));
    return code;
  }

  if (!pExec->pExecutor) {
    code = streamBuildTask(pTask, pExec->pExecutor);
  } else {
    code = streamClearStatesForOperators(pExec->pExecutor);
  }

  SSDataBlock* pBlock = NULL;
  uint64_t     ts = 0;
  if (code == 0) {
    code = streamExecuteTask(pExec->pExecutor, &pBlock, &ts); // TODO wjm impl it
  }
  if (code != 0) {
    stError("failed to exec task (%" PRId64 ", %" PRId64 ") code: %s", pTask->streamTask.streamId,
            pTask->streamTask.taskId, tstrerror(code));
  } else {

    if (pBlock && pBlock->info.rows > 0) {
      // dump blocks to DataInserter or notify someone
    }
  }
  stRunnerTaskExecMgrReleaseExec(pTask, pExec);
  return code;
}

static int32_t streamBuildTask(SStreamRunnerTask* pTask, SStreamRunnerTaskExecution *pExec) {
  int32_t      vgId = pTask->streamTask.vgId;
  int64_t      st = taosGetTimestampMs();
  int64_t      streamId = pTask->streamTask.streamId;
  int32_t      taskId = pTask->streamTask.taskId;
  int32_t      code = 0;

  stDebug("s-task:(%" PRId64 ",%" PRId64 ") vgId:%d start to expand stream task", streamId, taskId, vgId);

  code = qCreateStreamExecTaskInfo(&pExec->pExecutor, (void*)pExec->pPlan, &pTask->handle, vgId, taskId);
  if (code) {
    stError("s-task:(%"PRId64 ",%"PRId64") failed to expand task, code:%s", streamId, taskId, tstrerror(code));
    return code;
  }

  code = qSetTaskId(pExec->pExecutor, taskId, streamId);
  if (code) {
    return code;
  }

  //code = qSetStreamNotifyInfo(pTask->exec.pExecutor, pTask->notifyInfo.notifyEventTypes, pTask->notifyInfo.pSchemaWrapper, pTask->notifyInfo.stbFullName, IS_NEW_SUBTB_RULE(pTask), &pTask->notifyEventStat);
  if (code) {
    stError("s-task:(%" PRId64 ",%" PRId64 ") failed to set stream notify info, code:%s", streamId, taskId,
            tstrerror(code));
    return code;
  }

  double el = (taosGetTimestampMs() - st) / 1000.0;
  stDebug("s-task:(%" PRId64 ",%" PRId64 ") vgId:%d expand stream task completed, elapsed time:%.2fsec", streamId,
          taskId, vgId, el);

  return code;
}
