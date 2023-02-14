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

#include "vnd.h"
#include "vnodeInt.h"

// ----------------------------------------------------------------------------
// declaration

static int32_t vndScheduleCommitTask(SVnode* pVnode, int (*exec)(void*), void* arg);
static int32_t vndScheduleCompactTask(SVnode* pVnode, int (*exec)(void*), void* arg);
static int32_t vndScheduleMergeTask(SVnode* pVnode, int (*exec)(void*), void* arg);
static int32_t vndScheduleMigrateTask(SVnode* pVnode, int (*exec)(void*), void* arg);
static int32_t vndPostScheduleCommit(SVnode* pVnode, void* arg);
static int32_t vndPostScheduleCompact(SVnode* pVnode, void* arg);
static int32_t vndPostScheduleMerge(SVnode* pVnode, void* arg);
static int32_t vndPostScheduleMigrate(SVnode* pVnode, void* arg);
// static int32_t vndPrepareCommit(SVnode* pVnode, void* arg, void** ppInfo);
static int32_t vndPrepareCompact(SVnode* pVnode, void* arg, void** ppInfo);
static int32_t vndPrepareMerge(SVnode* pVnode, void* arg, void** ppInfo);
static int32_t vndPrepareMigrate(SVnode* pVnode, void* arg, void** ppInfo);

// ----------------------------------------------------------------------------
// framework
// ----------------------------------------------------------------------------
// framework - static func

static FORCE_INLINE int32_t vndBatchAddRef(SVBatchHandle* pHandle) { return atomic_add_fetch_32(&pHandle->nRef, 1); }

static FORCE_INLINE int32_t vndBatchSubRef(SVBatchHandle* pHandle) { return atomic_sub_fetch_32(&pHandle->nRef, 1); }

static FORCE_INLINE int32_t vndBatchRLock(SVBatchHandle* pHandle) {
  taosRLockLatch(&pHandle->lock);
  return 0;
}

static FORCE_INLINE int32_t vndBatchRUnLock(SVBatchHandle* pHandle) {
  taosRUnLockLatch(&pHandle->lock);
  return 0;
}

static FORCE_INLINE int32_t vndBatchWLock(SVBatchHandle* pHandle) {
  taosWLockLatch(&pHandle->lock);
  return 0;
}

static FORCE_INLINE int32_t vndBatchWUnLock(SVBatchHandle* pHandle) {
  taosWUnLockLatch(&pHandle->lock);
  return 0;
}

static int32_t vndScheduleCommitTask(SVnode* pVnode, int (*exec)(void*), void* arg) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SCommitInfo*   pInfo = (SCommitInfo*)arg;
  SVBatchHandle* pHandle = &pVnode->batchHandle;

  // lock
  vndBatchWLock(pHandle);
  if (pInfo->allowCommit) {
    ++pHandle->nCommitTask;
    if ((code = vnodeScheduleTask(exec, arg))) {
      --pHandle->nCommitTask;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    vndBatchAddRef(pHandle);
  } else {
    ASSERTS(pHandle->commitStash == NULL, "commitStash not NULL, %p", pHandle->commitStash);
    pHandle->commitStash = pInfo;

    if ((pHandle->nMergeTask == 0) && (pHandle->mergeStash == NULL)) {
      SMergeInfo* pMergeInfo = NULL;
      vndScheduleMergeTask(pVnode, vnodeBatchTask, pMergeInfo);
    }
  }
_exit:
  // unlock
  vndBatchWUnLock(pHandle);
  if (code) {
    taosMemoryFree(arg);
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t vndScheduleMergeTask(SVnode* pVnode, int (*exec)(void*), void* arg) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SMergeInfo*    pInfo = (SMergeInfo*)arg;
  SVBatchHandle* pHandle = &pVnode->batchHandle;

  ASSERTS(arg, "arg is NULL");

  // no need to lock since it triggered by commit task which with lock already
  ASSERTS(pHandle->nMergeTask == 0, "nMergeTask is %" PRIi8, pHandle->nMergeTask);
  ASSERTS(pHandle->mergeStash == NULL, "mergeStash not NULL, %p", pHandle->mergeStash);
  ASSERTS(pHandle->nCommitTask == 1, "nCommitTask is %" PRIi8, pHandle->nCommitTask);

  if (pHandle->nTrimTask > 0) {
    pHandle->mergeStash = arg;
  } else {
    ++pHandle->nMergeTask;
    if ((code = vnodeScheduleTask(exec, arg))) {
      --pHandle->nMergeTask;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    vndBatchAddRef(pHandle);
  }

_exit:
  ASSERTS(pHandle->nCommitTask == 1, "nCommitTask is %" PRIi8, pHandle->nCommitTask);
  if (code) {
    taosMemoryFree(arg);
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t vndScheduleMigrateTask(SVnode* pVnode, int (*exec)(void*), void* arg) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SMigrateInfo*  pInfo = (SMigrateInfo*)arg;
  SVBatchHandle* pHandle = &pVnode->batchHandle;

  // lock
  vndBatchWLock(pHandle);
  if (pHandle->inClose) {
    code = TSDB_CODE_VND_IS_CLOSING;
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (pHandle->migrateStash) {
    ASSERTS(pHandle->nTrimTask > 0, "nTrimTask is %" PRIi8, pHandle->nTrimTask);
    code = TSDB_CODE_VND_TASK_ALREADY_EXIST;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pHandle->nMergeTask || pHandle->nTrimTask) {
    pHandle->migrateStash = arg;
  } else {
    ++pHandle->nTrimTask;
    if ((code = vnodeScheduleTask(exec, arg))) {
      --pHandle->nTrimTask;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    vndBatchAddRef(pHandle);
  }

_exit:
  // unlock
  vndBatchWUnLock(pHandle);
  if (code) {
    taosMemoryFree(arg);
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t vndScheduleCompactTask(SVnode* pVnode, int (*exec)(void*), void* arg) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SCompactInfo*  pInfo = (SCompactInfo*)arg;
  SVBatchHandle* pHandle = &pVnode->batchHandle;

  // lock
  vndBatchWLock(pHandle);
  if (pHandle->inClose) {
    code = TSDB_CODE_VND_IS_CLOSING;
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (pHandle->compactStash) {
    code = TSDB_CODE_VND_TASK_ALREADY_EXIST;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pHandle->nMergeTask || pHandle->nTrimTask) {
    pHandle->compactStash = arg;
  } else {
    ++pHandle->nTrimTask;
    if ((code = vnodeScheduleTask(exec, arg))) {
      --pHandle->nTrimTask;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    vndBatchAddRef(pHandle);
  }

_exit:
  // unlock
  vndBatchWUnLock(pHandle);
  if (code) {
    taosMemoryFree(arg);
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t vndPostScheduleCommit(SVnode* pVnode, void* arg) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SCommitInfo*   pInfo = (SCommitInfo*)arg;
  SVBatchHandle* pHandle = &pVnode->batchHandle;

  vndBatchWLock(pHandle);
  if (pInfo->nMaxSttF >= pVnode->config.sttTrigger && !pHandle->mergeStash && pHandle->nMergeTask == 0) {
    void* pMergeInfo = NULL;
    code = vndPrepareMerge(pVnode, NULL, &pMergeInfo);
    TSDB_CHECK_CODE(code, lino, _exit);
    vndScheduleMergeTask(pVnode, vnodeBatchTask, pMergeInfo);
  }
  ASSERTS(pInfo->nMaxSttF >= 0 && pInfo->nMaxSttF <= TSDB_MAX_STT_TRIGGER, "nMaxSttF is %" PRIu8, pInfo->nMaxSttF);

_exit:
  --pHandle->nCommitTask;
  ASSERTS(pHandle->nCommitTask == 0, "nCommitTask is %" PRIi8, pHandle->nCommitTask);
  vndBatchSubRef(&pVnode->batchHandle);
  vndBatchWUnLock(pHandle);
  if (code) {
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t vndPostScheduleMerge(SVnode* pVnode, void* arg) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SMergeInfo*    pInfo = (SMergeInfo*)arg;
  SVBatchHandle* pHandle = &pVnode->batchHandle;

  vndBatchWLock(pHandle);
  ASSERTS(pHandle->nCommitTask == 0 || pHandle->nCommitTask == 1, "nCommitTask is %" PRIi8, pHandle->nCommitTask);
  if (pHandle->commitStash) {
    SCommitInfo* pCommitInfo = (SCommitInfo*)pHandle->commitStash;
    ASSERTS(pCommitInfo->allowCommit == 0, "allow commit is %" PRIi8, pCommitInfo->allowCommit);
    pCommitInfo->allowCommit = 1;
    ++pHandle->nCommitTask;
    if ((code = vnodeScheduleTask(vnodeCommitTask, pCommitInfo))) {
      --pHandle->nCommitTask;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    pHandle->commitStash = NULL;
    vndBatchAddRef(pHandle);
  }

  ASSERTS(pHandle->nTrimTask == 0, "nTrimTask is %" PRIi8, pHandle->nTrimTask);
  if (pHandle->compactStash) {
    ++pHandle->nTrimTask;
    if ((code = vnodeScheduleTask(vnodeBatchTask, pHandle->compactStash))) {
      --pHandle->nTrimTask;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    pHandle->compactStash = NULL;
    vndBatchAddRef(pHandle);
  } else if (pHandle->migrateStash) {
    ++pHandle->nTrimTask;
    if ((code = vnodeScheduleTask(vnodeBatchTask, pHandle->migrateStash))) {
      --pHandle->nTrimTask;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    pHandle->migrateStash = NULL;
    vndBatchAddRef(pHandle);
  }

_exit:
  --pHandle->nMergeTask;
  vndBatchWUnLock(pHandle);

  if (code) {
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t vndPostScheduleMigrate(SVnode* pVnode, void* arg) {
  int32_t        code = 0;
  int32_t        lino = 0;
  SVBatchHandle* pHandle = &pVnode->batchHandle;

  vndBatchWLock(pHandle);
  ASSERTS(pHandle->nTrimTask == 1, "nTrimTask is %" PRIi8, pHandle->nTrimTask);
  ASSERTS(pHandle->nMergeTask == 0, "nMergeTask is %" PRIi8, pHandle->nMergeTask);
  if (pHandle->mergeStash) {
    ++pHandle->nMergeTask;
    if ((code = vnodeScheduleTask(vnodeBatchTask, pHandle->mergeStash))) {
      --pHandle->nMergeTask;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    vndBatchAddRef(pHandle);

    pHandle->mergeStash = NULL;
  } else if (pHandle->compactStash) {
    ++pHandle->nTrimTask;
    if ((code = vnodeScheduleTask(vnodeBatchTask, pHandle->compactStash))) {
      --pHandle->nTrimTask;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    vndBatchAddRef(pHandle);
    pHandle->compactStash = NULL;
  } else if (pHandle->migrateStash) {
    ++pHandle->nTrimTask;
    if ((code = vnodeScheduleTask(vnodeBatchTask, pHandle->migrateStash))) {
      --pHandle->nTrimTask;
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    vndBatchAddRef(pHandle);
    pHandle->migrateStash = NULL;
  }

_exit:
  --pHandle->nTrimTask;
  vndBatchWUnLock(pHandle);

  if (code) {
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }

  return code;
}

static int32_t vndPostScheduleCompact(SVnode* pVnode, void* arg) { return vndPostScheduleMigrate(pVnode, arg); }

int32_t vnodeBatchPostSchedule(SVnode* pVnode, void* arg) {
  int32_t code = 0;
  switch (((SVTaskInfo*)arg)->type) {
    case VND_TASK_COMMIT:
      code = vndPostScheduleCommit(pVnode, arg);
      break;
    case VND_TASK_COMPACT:
      code = vndPostScheduleCompact(pVnode, arg);
      break;
    case VND_TASK_MERGE:
      code = vndPostScheduleMerge(pVnode, arg);
      break;
    case VND_TASK_MIGRATE:
      code = vndPostScheduleMigrate(pVnode, arg);
      break;
    default:
      code = TSDB_CODE_APP_ERROR;
      break;
  }
  return code;
}

// framework - public func

int32_t vnodeBatchPutSchedule(SVnode* pVnode, int (*exec)(void*), void* arg) {
  int32_t code = 0;
  ASSERTS(arg, "arg is NULL");
  switch (((SVTaskInfo*)arg)->type) {
    case VND_TASK_COMMIT:
      code = vndScheduleCommitTask(pVnode, exec, arg);
      break;
    case VND_TASK_COMPACT:
      code = vndScheduleCompactTask(pVnode, exec, arg);
      break;
    case VND_TASK_MERGE:
      code = vndScheduleMergeTask(pVnode, exec, arg);
      break;
    case VND_TASK_MIGRATE:
      code = vndScheduleMigrateTask(pVnode, exec, arg);
      break;
    default:
      code = TSDB_CODE_APP_ERROR;
      break;
  }
  return code;
}

// ----------------------------------------------------------------------------
// batch tasks
// ----------------------------------------------------------------------------
// batch tasks - static func

static int32_t vndPrepareCommit(SVnode* pVnode, void* arg, void** ppInfo) {
  int32_t code = 0;
  int32_t lino = 0;
_exit:
  return code;
}
static int32_t vndPrepareCompact(SVnode* pVnode, void* arg, void** ppInfo) {
  int32_t code = 0;
  int32_t lino = 0;
_exit:
  return code;
}

static int32_t vndPrepareMerge(SVnode* pVnode, void* arg, void** ppInfo) {
  int32_t     code = 0;
  int32_t     lino = 0;
  SMergeInfo* pInfo = NULL;

  *ppInfo = (SMergeInfo*)taosMemoryCalloc(1, sizeof(SMergeInfo));
  if (NULL == *ppInfo) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  pInfo = *ppInfo;
  pInfo->taskInfo.type = VND_TASK_MERGE;
  pInfo->taskInfo.pVnode = pVnode;
  pInfo->flag = 0;
  pInfo->commitID = atomic_add_fetch_64(&pVnode->state.commitID, 1);

_exit:
  if (code) {
    vError("vgId:%d %s failed at line %d since %s, commit ID:%" PRId64, TD_VID(pVnode), __func__, lino, tstrerror(code),
           pVnode->state.commitID);
  } else {
    vDebug("vgId:%d %s done, commit ID:%" PRId64, TD_VID(pVnode), __func__, pVnode->state.commitID);
  }
  return code;
}

static int32_t vndPrepareMigrate(SVnode* pVnode, void* arg, void** ppInfo) {
  int32_t       code = 0;
  int32_t       lino = 0;
  SMigrateInfo* pInfo = NULL;

  *ppInfo = (SMigrateInfo*)taosMemoryCalloc(1, sizeof(SMigrateInfo));
  if (NULL == *ppInfo) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  pInfo = *ppInfo;
  pInfo->taskInfo.type = VND_TASK_MIGRATE;
  pInfo->taskInfo.pVnode = pVnode;
  pInfo->req = *(SVTrimDbReq*)arg;
  pInfo->index = INT32_MAX;
  pInfo->maxIndex = INT32_MIN;
  pInfo->fRange.minFid = INT32_MAX;
  pInfo->fRange.maxFid = INT32_MIN;

_exit:
  return code;
}

static int32_t vndPrepareTask(SVnode* pVnode, int8_t type, void* arg, void** ppInfo) {
  switch (type) {
    case VND_TASK_COMMIT:
      return vndPrepareCommit(pVnode, arg, ppInfo);
    case VND_TASK_COMPACT:
      return vndPrepareCompact(pVnode, arg, ppInfo);
    case VND_TASK_MERGE:
      return vndPrepareMerge(pVnode, arg, ppInfo);
    case VND_TASK_MIGRATE:
      return vndPrepareMigrate(pVnode, arg, ppInfo);
    default:
      break;
  }
  return TSDB_CODE_APP_ERROR;
}

static int32_t vnodeBatchImpl(SVTaskInfo* pInfo) {
  int32_t code = 0;
  int32_t lino = 0;
  SVnode* pVnode = pInfo->pVnode;

  code = tsdbBatchExec(pVnode->pTsdb, pInfo, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = smaBatchExec(pVnode->pSma, pInfo, 0);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  return code;
}

int32_t vnodeBatchTask(void* arg) {
  int32_t     code = 0;
  SVTaskInfo* pInfo = (SVTaskInfo*)arg;
  SVnode*     pVnode = pInfo->pVnode;

  code = vnodeBatchImpl(pInfo);
  if (code) goto _exit;

  vInfo("vgId:%d, vnode batch task(%s) done", TD_VID(pVnode), vTaskName[pInfo->type]);

_exit:
  if (code) {
    vError("vgId:%d, vnode batch task(%s) failed since %s", TD_VID(pVnode), vTaskName[pInfo->type], tstrerror(code));
  }
  vnodeBatchPostSchedule(pVnode, pInfo);
  if (pInfo->type != VND_TASK_COMMIT) {
    vndBatchSubRef(&pVnode->batchHandle);
    taosMemoryFree(pInfo);
  }

  return code;
}

// batch tasks - public func
int32_t vnodeBatchSchedule(SVnode* pVnode, int8_t type, void* arg) {
  int32_t code = 0;
  int32_t lino = 0;
  void*   pInfo = NULL;

  code = vndPrepareTask(pVnode, type, arg, &pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

  code = vnodeBatchPutSchedule(pVnode, vnodeBatchTask, pInfo);
  TSDB_CHECK_CODE(code, lino, _exit);

_exit:
  if (code) {
    vError("vgId:%d, %s:%" PRIi8 " failed at line %d since %s", TD_VID(pVnode), __func__, type, lino, tstrerror(code));
  } else {
    vInfo("vgId:%d, vnode batch schedule task(%s) done", TD_VID(pVnode), vTaskName[type]);
  }
  return code;
}

int32_t vnodeBatchPreClose(SVnode* pVnode) {
  SVBatchHandle* pHandle = &pVnode->batchHandle;
  vndBatchWLock(pHandle);
  pHandle->inClose = true;
  // clear migrate/compact tasks; while wait commit/merge tasks to finish
  taosMemoryFreeClear(pHandle->compactStash);
  taosMemoryFreeClear(pHandle->migrateStash);
  vndBatchWUnLock(pHandle);
  return 0;
}

int32_t vnodeBatchClose(SVnode* pVnode) {
  int32_t        nLoops = 0;
  SVBatchHandle* pHandle = &pVnode->batchHandle;
  while (1) {
    if (atomic_load_32(&pHandle->nRef) == 0) {
      vDebug("vgId:%d, vnode batch close, all tasks finished", TD_VID(pVnode));
      break;
    }
    ++nLoops;
    if ((nLoops & 1023) == 0) {
      sched_yield();
      if (nLoops > ((int32_t)1 << 30)) {
        nLoops = 0;
        vInfo("vgId:%d, vnode batch close, task not finished yet", TD_VID(pVnode));
      }
    }
  }
  return 0;
}