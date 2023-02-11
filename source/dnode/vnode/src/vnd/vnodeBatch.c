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
// static int32_t vndPrepareCommit(SVnode* pVnode, void* arg, void** ppInfo);
static int32_t vndPrepareCompact(SVnode* pVnode, void* arg, void** ppInfo);
static int32_t vndPrepareMerge(SVnode* pVnode, void* arg, void** ppInfo);
static int32_t vndPrepareMigrate(SVnode* pVnode, void* arg, void** ppInfo);

static int32_t vndBatchTask(void* arg);

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
  if (pInfo->canCommit) {
    code = vnodeScheduleTask(exec, arg);
    TSDB_CHECK_CODE(code, lino, _exit);
    vndBatchAddRef(pHandle);
  } else {
    if (!pHandle->commitStash) {
      pHandle->commitStash == pInfo;
    } else {
      ASSERT(pHandle->commitStash == pInfo);
    }
    if ((pHandle->nMergeTask == 0) && (pHandle->mergeStash == NULL)) {
      SMergeInfo* pMergeInfo = NULL;
      vndScheduleMergeTask(pVnode, vndBatchTask, pMergeInfo);
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

  // lock
  vndBatchWLock(pHandle);
  ASSERT(pHandle->nMergeTask == 0);
  ASSERT(pHandle->mergeStash == NULL);

  if (pHandle->nTrimTask > 0) {
    pHandle->mergeStash = arg;
  } else {
    if ((code = vnodeScheduleTask(exec, arg))) {
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    vndBatchAddRef(pHandle);
    ++pHandle->nMergeTask;
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
    ASSERT(pHandle->nTrimTask > 0);
    code = TSDB_CODE_VND_TASK_ALREADY_EXIST;
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  if (pHandle->nMergeTask || pHandle->nTrimTask) {
    pHandle->migrateStash = arg;
  } else {
    if ((code = vnodeScheduleTask(exec, arg))) {
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    vndBatchAddRef(pHandle);
    ++pHandle->nTrimTask;
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
    if ((code = vnodeScheduleTask(exec, arg))) {
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    vndBatchAddRef(pHandle);
    ++pHandle->nTrimTask;
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
  if (!pInfo->canCommit && !pHandle->mergeStash && pHandle->nMergeTask == 0) {
    SMergeInfo* pMergeInfo = NULL;
    vndScheduleMergeTask(pVnode, vndBatchTask, pMergeInfo);
  }

_exit:
  --pHandle->nCommitTask;
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
  ASSERT(pHandle->nCommitTask == 0);
  if (pHandle->commitStash) {
    SCommitInfo* pCommitInfo = (SCommitInfo*)pHandle->commitStash;
    ASSERT(pCommitInfo->canCommit == 0);
    pCommitInfo->canCommit = 1;
    if ((code = vnodeScheduleTask(vnodeCommitTask, pCommitInfo))) {
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    pHandle->commitStash = NULL;
    vndBatchAddRef(pHandle);
    ++pHandle->nCommitTask;
  }

  ASSERT(pHandle->nTrimTask == 0);
  if (pHandle->compactStash) {
    if ((code = vnodeScheduleTask(vndBatchTask, pHandle->compactStash))) {
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    pHandle->compactStash = NULL;
    vndBatchAddRef(pHandle);
    ++pHandle->nTrimTask;
  } else if (pHandle->migrateStash) {
    if ((code = vnodeScheduleTask(vndBatchTask, pHandle->migrateStash))) {
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    pHandle->migrateStash = NULL;
    vndBatchAddRef(pHandle);
    ++pHandle->nTrimTask;
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
  ASSERT(pHandle->nTrimTask == 1);
  ASSERT(pHandle->nMergeTask == 0);
  if (pHandle->mergeStash) {
    if ((code = vnodeScheduleTask(vndBatchTask, pHandle->mergeStash))) {
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    vndBatchAddRef(pHandle);
    pHandle->mergeStash = NULL;
    ++pHandle->nMergeTask;
  } else if (pHandle->compactStash) {
    if ((code = vnodeScheduleTask(vndBatchTask, pHandle->compactStash))) {
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    vndBatchAddRef(pHandle);
    pHandle->compactStash = NULL;
    ++pHandle->nTrimTask;
  } else if (pHandle->migrateStash) {
    if ((code = vnodeScheduleTask(vndBatchTask, pHandle->migrateStash))) {
      TSDB_CHECK_CODE(code, lino, _exit);
    }
    vndBatchAddRef(pHandle);
    pHandle->migrateStash = NULL;
    ++pHandle->nTrimTask;
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

int32_t vnodeBatchPostSchedule(SVnode* pVnode, void* arg, int8_t type) {
  int32_t code = 0;
  switch (type) {
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

int32_t vnodeBatchPutSchedule(SVnode* pVnode, int (*exec)(void*), void* arg, int8_t type) {
  int32_t code = 0;
  switch (type) {
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
  int32_t code = 0;
  int32_t lino = 0;
_exit:
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

static int32_t vndBatchTask(void* arg) {
  int32_t     code = 0;
  SVTaskInfo* pInfo = (SVTaskInfo*)arg;
  SVnode*     pVnode = pInfo->pVnode;

  code = vnodeBatchImpl(pInfo);
  if (code) goto _exit;

  vInfo("vgId:%d, vnode batch task(%s) done", TD_VID(pVnode), vTaskName[pInfo->type]);

_exit:
  if (code) {
    vError("vgId:%d, vnode batch task failed since %s", TD_VID(pVnode), tstrerror(code));
  } else {
    vnodeBatchPostSchedule(pVnode, pInfo, pInfo->type);
  }

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

  code = vnodeBatchPutSchedule(pVnode, vndBatchTask, pInfo, type);
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