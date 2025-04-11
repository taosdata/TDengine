#include "streamRunner.h"
#include "stream.h"
#include "taos.h"
#include "tencode.h"
#include "tref.h"

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

static void stRunnerMgrWLock(SStreamRunnerMgr* pMgr) {
  int32_t code = taosThreadRwlockWrlock(&pMgr->lock);
  if (code) {
    stError("vgId:%d failed to acquire write lock, code:%s", pMgr->vgId, tstrerror(code));
  }
}

static void stRunnerMgrWUnLock(SStreamRunnerMgr* pMgr) {
  int32_t code = taosThreadRwlockUnlock(&pMgr->lock);
  if (code) {
    stError("vgId:%d failed to release write lock, code:%s", pMgr->vgId, tstrerror(code));
  }
}

static void stRunnerMgrRLock(SStreamRunnerMgr* pMgr) {
  int32_t code = taosThreadRwlockRdlock(&pMgr->lock);
  if (code) {
    stError("vgId:%d failed to acquire read lock, code:%s", pMgr->vgId, tstrerror(code));
  }
}

static void stRunnerMgrRUnLock(SStreamRunnerMgr* pMgr) {
  int32_t code = taosThreadRwlockUnlock(&pMgr->lock);
  if (code) {
    stError("vgId:%d failed to release read lock, code:%s", pMgr->vgId, tstrerror(code));
  }
}

static int32_t stRunnerMgrInit(SStreamRunnerMgr* pMgr, int32_t vgId);
static int32_t stRunnerMgrRegisterTask(SStreamRunnerMgr* pMgr, SStreamTask* pTask);
static int32_t stRunnerMgrUnregisterTask(SStreamRunnerMgr* pMgr, SStreamTask* pTask);
static int32_t stRunnerAcquireTask(SStreamRunnerMgr* pMgr, const SStreamTaskId* pId, SStreamTask** ppTask);
static void    stRunnerReleaseTask(SStreamRunnerMgr* pMgr, SStreamTask* pTask);
static int32_t stRunnerDoExecuteTask(SStreamRunnerMgr* pMgr, SStreamTask* pTask);

int32_t decodeStreamTask(SStreamTask* pTask, const char* pMsg, int32_t msgLen) {
  int32_t  code = 0;
  SDecoder decoder;
  tDecoderInit(&decoder, (uint8_t*)pMsg, msgLen);
  // code = tDecodeStreamTask(&decoder, pTask);
  tDecoderClear(&decoder);
  return code;
}

static void freeStreamTask(SStreamTask* pTask) {
  if (pTask) {
    taosMemoryFree(pTask);
  }
}

int32_t stRunnerDeployTask(void* pNode, const char* pMsg, int32_t msgLen) {
  int32_t           code = 0;
  SStreamRunnerMgr* pMgr = (SStreamRunnerMgr*)pNode;
  SStreamTask*      pTask = taosMemoryCalloc(1, sizeof(SStreamTask));
  if (!pTask) {
    stError("vgId:%d failed to allocate memory for stream task", pMgr->vgId);
    return terrno;
  }
  code = decodeStreamTask(pTask, pMsg, msgLen);
  if (code != 0) {
    stError("vgId:%d failed to decode stream task, code:%s", pMgr->vgId, tstrerror(code));
    freeStreamTask(pTask);
    return code;
  }

  code = stRunnerMgrRegisterTask(pMgr, pTask);
  if (code != 0) {
    stError("vgId:%d failed to deploy stream task(%d,%d), code:%s", pMgr->vgId, pTask->streamId, pTask->taskId,
            tstrerror(code));
    freeStreamTask(pTask);
    return code;
  }
  stDebug("vgId:%d stream task (%d,%d) deployed successfully", pMgr->vgId, pTask->streamId, pTask->taskId);
  return 0;
}

int32_t stRunnerUndeplyTask(void* pNode, const char* pMsg, int32_t msgLen) { return 0; }

static int32_t stRunnerMgrInit(SStreamRunnerMgr* pMgr, int32_t vgId) {
  int32_t code = 0;
  int32_t lino = 0;
  pMgr->pTaskMap = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_NO_LOCK);
  TSDB_CHECK_NULL(pMgr->pTaskMap, code, lino, _err, terrno)
  return 0;
_err:
  stError("vgId:%d failed to init stream runner manager, code:%s", vgId, tstrerror(code));
  return code;
}

static int32_t stRunnerMgrRegisterTask(SStreamRunnerMgr* pMgr, SStreamTask* pTask) {
  int32_t       code = 0;
  SStreamTaskId taskId = {pTask->streamId, pTask->taskId};
  stRunnerMgrWLock(pMgr);
  void* p = taosHashGet(pMgr->pTaskMap, &taskId, sizeof(taskId));
  if (p) {
    stInfo("vgId:%d stream task (%d,%d) already exists", pMgr->vgId, pTask->streamId, pTask->taskId);
    goto _end;
  }
  pTask->refId = taosAddRef(0, pTask);  // TODO wjm add global ref pool
  code = taosHashPut(pMgr->pTaskMap, &taskId, sizeof(taskId), &pTask->refId, sizeof(pTask->refId));
  if (code) {
    stError("vgId:%d failed to register stream task (%d,%d), code:%s", pMgr->vgId, pTask->streamId, pTask->taskId,
            tstrerror(code));
    goto _end;
  }
  pTask = NULL;
  stRunnerMgrWUnLock(pMgr);
  return 0;
_end:
  freeStreamTask(pTask);
  stRunnerMgrWUnLock(pMgr);
  return code;
}

static int32_t stRunnerMgrUnregisterTask(SStreamRunnerMgr* pMgr, SStreamTask* pTask) { return 0; }

int32_t stRunnerExecuteTask(void* pNode, const char* pMsg, int32_t msgLen) {
  SStreamRunnerMgr* pMgr = (SStreamRunnerMgr*)pNode;
  int64_t           streamId = 0;
  int64_t           taskId = 0;
  SStreamTask*      pTask = NULL;
  int32_t           code = 0;
  // TODO decode streamId and taskId.

  SStreamTaskId id = {streamId, taskId};
  code = stRunnerAcquireTask(pMgr, &id, &pTask);
  if (code != 0) {
    stError("vgId:%d failed to acquire stream task (%d,%d), code:%s", pMgr->vgId, streamId, taskId, tstrerror(code));
    return code;
  }

  // TODO wjm execute this task

  stDebug("vgId:%d stream task (%d,%d) run successfully", pMgr->vgId, streamId, taskId);
  stRunnerReleaseTask(pMgr, pTask);
  return 0;
}

static int32_t stRunnerAcquireTask(SStreamRunnerMgr* pMgr, const SStreamTaskId* pId, SStreamTask** ppTask) {
  SStreamTask* pTask = NULL;
  int32_t code = 0;
  stRunnerMgrRLock(pMgr);
  int64_t*     pRef = taosHashGet(pMgr->pTaskMap, pId, sizeof(*pId));
  if (!pRef) {
    stError("vgId:%d stream task (%d,%d) not found", pMgr->vgId, pId->streamId, pId->taskId);
    code = TSDB_CODE_STREAM_TASK_NOT_EXIST;
    goto _end;
  }
  pTask = taosAcquireRef(0, *pRef);  // TODO wjm
  if (!pTask) {
    stError("vgId:%d failed to acquire stream task (%d,%d), code:%s, undeployed maybe", pMgr->vgId, pId->streamId,
            pId->taskId, tstrerror(terrno));
    code = TSDB_CODE_STREAM_TASK_NOT_EXIST;
    goto _end;
  }
  *ppTask = pTask;
  return code;
_end:
  stRunnerMgrRUnLock(pMgr);
  return code;
}

static void stRunnerReleaseTask(SStreamRunnerMgr* pMgr, SStreamTask* pTask) {
  if (pTask) {
    int32_t code = taosReleaseRef(0, pTask->refId);  // TODO wjm
    if (code) {
      stError("vgId:%d failed to release stream task (%d,%d), code:%s", pMgr->vgId, pTask->streamId, pTask->taskId,
              tstrerror(code));
    }
  }
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