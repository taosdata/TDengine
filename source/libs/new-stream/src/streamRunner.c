#include "streamRunner.h"
#include "executor.h"


typedef struct SStreamTaskExecutionInfo {
  int32_t        parallelExecutionNun;
  TdThreadRwlock lock;
  SArray*        pExecTaskInfos;
} SStreamTaskExecInfo;

typedef struct SStreamRunnerMgr {
  void*          pNode;
  TdThreadRwlock lock;
  int32_t        vgId;
  SHashObj*      pTaskMap;
  int32_t        (*initTask)(SStreamTask* pTask);
  int32_t        (*buildTask)(SStreamTask* pTask);
} SStreamRunnerMgr;

typedef struct SStreamTaskId {
  int64_t streamId;
  int64_t taskId;
} SStreamTaskId;

static int32_t stRunnerDoExecuteTask(SStreamRunnerMgr* pMgr, SStreamTask* pTask);

static int32_t stRunnerBuildTask(SStreamRunnerMgr* pMgr, SStreamTask* pTask) {
  int32_t code = 0;
  SReadHandle pReader = {};
  qCreateStreamExecTaskInfo(0, 0, 0, 0, 0);
  return 0;
}

static int32_t stRunnerDoExecuteTask(SStreamRunnerMgr* pMgr, SStreamTask* pTask) {
  int32_t              code = 0;
  static const int32_t taskConcurrentExecutionNum = 4; // TODO wjm make it configurable
  SStreamTaskExecInfo* pTaskExecInfo = NULL;
  //pTaskExecInfo = (SStreamTaskExecInfo*)pTask->pTaskInfo;

  int32_t curRunningNum = pTaskExecInfo->parallelExecutionNun;
  while (curRunningNum < taskConcurrentExecutionNum) {
    int32_t newVal = atomic_val_compare_exchange_32(&pTaskExecInfo->parallelExecutionNun, curRunningNum, curRunningNum + 1);
    if (newVal == curRunningNum + 1) {
      break;
    } else {
      curRunningNum = newVal;
    }
  }
  if (curRunningNum >= taskConcurrentExecutionNum) {
    stError("vgId:%d failed to exec task: (%" PRId64 ", %" PRId64 "), already have %d running", pMgr->vgId,
            pTask->streamId, pTask->taskId, curRunningNum);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;  // TODO wjm define the error code
  }
  // try build this task and execute it
  code = pMgr->buildTask(pTask);
  if (code != 0) {
    stError("vgId:%d failed to build task (%" PRId64 ", %" PRId64 "), code:%s", pMgr->vgId, pTask->streamId,
            pTask->taskId, tstrerror(code));
    atomic_fetch_sub_32(&pTaskExecInfo->parallelExecutionNun, 1);
    return code;
  }
  void* pTaskInfo = taosArrayGet(pTaskExecInfo->pExecTaskInfos, curRunningNum);
  if (!pTaskInfo) {
    stError("vgId:%d failed to get task info for task (%" PRId64 ", %" PRId64 "), code:%s", pMgr->vgId, pTask->streamId,
            pTask->taskId, tstrerror(code));
    return TSDB_CODE_STREAM_INTERNAL_ERROR;  // TODO wjm define the error code
  }
  return 0;
}

int32_t stRunnerTaskDeploy(SStreamRunnerTask** ppTask, const SStreamRunnerDeployMsg* pMsg) {
  SStreamRunnerTask* pTask = taosMemoryCalloc(1, sizeof(SStreamRunnerTask));
  if (!pTask) {
    stError("failed to allocate memory for stream task (%" PRId64 ", %" PRId64 "), code:%s", pMsg->task.streamId,
            pMsg->task.taskId, tstrerror(terrno));
    return terrno;
  }
  pTask->streamTask = pMsg->task;
  pTask->buildTaskFn = pMsg->buildTaskFn;
  *ppTask = pTask;
  return 0;
}

int32_t stRunnerTaskUndeploy(SStreamRunnerTask* pTask, const SStreamRunnerUndeployMsg* pMsg) {
  taosMemoryFree(pTask);
  // free executor
  return 0;
}

int32_t stRunnerTaskExecute(SStreamRunnerTask* pTask, const char* pMsg, int32_t msgLen) {
  int32_t code = pTask->buildTaskFn(pTask);
  if (code != 0) {
    stError("failed to build task (%" PRId64 ", %" PRId64 "), code:%s", pTask->streamTask.streamId,
            pTask->streamTask.taskId, tstrerror(code));
    return code;
  }
  return code;
}

int32_t stRunnerTaskRetrieveStatus(SStreamRunnerTask* pTask, SStreamRunnerTaskStatus* pStatus);
