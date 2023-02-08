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
static int32_t vnodeCompactImpl(SCompactInfo* pInfo);
static int32_t vnodeMergeImpl(SMergeInfo* pInfo);
static int32_t vnodeMigrateImpl(SMigrateInfo* pInfo);
static int32_t vnodeCompactTask(void* arg);
static int32_t vnodeMergeTask(void* arg);
static int32_t vnodeMigrateTask(void* arg);

// ----------------------------------------------------------------------------
// framework
// ----------------------------------------------------------------------------
// framework - static func

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
  int32_t code = 0;
  int32_t lino = 0;

  SCommitInfo* pInfo = (SCommitInfo*)arg;
  if (pInfo->canCommit) {
    code = vnodeScheduleTask(exec, arg);
    TSDB_CHECK_CODE(code, lino, _exit);
  } else {
    SVBatchHandle* pHandle = &pVnode->batchHandle;
    // lock
    vndBatchWLock(pHandle);
    if (atomic_load_32(&pHandle->nMergeTask) == 0) {
      if ((code = vnodeScheduleTask(exec, arg))) {
        vndBatchWUnLock(pHandle);
        TSDB_CHECK_CODE(code, lino, _exit);
      }
    } else {
      ASSERT(pHandle->commitStash == NULL);
      pHandlecommitStash = pInfo;
    }
    // unlock
    vndBatchWUnLock(pHandle);
  }
_exit:
  if (code) {
    vError("vgId:%d, %s failed at line %d since %s", TD_VID(pVnode), __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t vndScheduleCompactTask(SVnode* pVnode, int (*exec)(void*), void* arg) {
  int32_t code = 0;
  code = vnodeScheduleTask(exec, arg);
  return code;
}

static int32_t vndScheduleMergeTask(SVnode* pVnode, int (*exec)(void*), void* arg) {
  int32_t code = 0;
  code = vnodeScheduleTask(exec, arg);
  return code;
}

static int32_t vndScheduleMigrateTask(SVnode* pVnode, int (*exec)(void*), void* arg) {
  int32_t code = 0;
  code = vnodeScheduleTask(exec, arg);
  return code;
}

static int32_t vndPostSchedule(SVnode* pVnode) {
  int32_t        code = 0;
  SVBatchHandle* pHandle = &pVnode->batchHandle;

  vndBatchWLock(pHandle);
  
  vndBatchWUnLock(pHandle);

_exit:
  return code;
}

// framework - public func

int32_t vnodePutScheduleTask(SVnode* pVnode, int (*exec)(void*), void* arg, int8_t type) {
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

// migrate
static int32_t vnodeMigrateImpl(SMigrateInfo* pInfo) {
  int32_t      code = 0;
  SVnode*      pVnode = pInfo->pVnode;
  SVTrimDbReq* pReq = &pInfo->req;

  code = tsdbDoRetention(pVnode->pTsdb, pReq->timestamp);
  if (code) goto _exit;

  code = smaDoRetention(pVnode->pSma, pReq->timestamp);
  if (code) goto _exit;

  for (int32_t i = 0; i < 15; ++i) {
    taosSsleep(1);
    vInfo("%s:%d sleep %d", __func__, __LINE__, i);
  }

_exit:
  return code;
}

static int32_t vnodeMigrateTask(void* arg) {
  int32_t       code = 0;
  SMigrateInfo* pInfo = (SMigrateInfo*)arg;
  SVnode*       pVnode = pInfo->pVnode;

  code = vnodeMigrateImpl(pInfo);
  if (code) goto _exit;

_exit:
  vInfo("vgId:%d, vnode migrate done, time:%" PRIu32 " index:%d maxIndex:%d minFid:%d maxFid:%d", TD_VID(pVnode),
        pInfo->req.timestamp, pInfo->index, pInfo->maxIndex, pInfo->minFid, pInfo->maxFid);
  taosMemoryFree(pInfo);
  tsem_post(&pVnode->canTrim);
  return code;
}

// merge
static int32_t vnodeMergeImpl(SMergeInfo* pInfo) {
  int32_t code = 0;
  SVnode* pVnode = pInfo->pVnode;

  // TODO

  for (int32_t i = 0; i < 10; ++i) {
    taosSsleep(1);
    vInfo("%s:%d sleep %d", __func__, __LINE__, i);
  }

_exit:

  
  return code;
}

static int32_t vnodeMergeTask(void* arg) {
  int32_t     code = 0;
  SMergeInfo* pInfo = (SMergeInfo*)arg;
  SVnode*     pVnode = pInfo->pVnode;

  code = vnodeMergeImpl(pInfo);
  if (code) goto _exit;

_exit:
  vInfo("vgId:%d, vnode merge done, fid:%d", TD_VID(pVnode), pInfo->fid);
  taosMemoryFree(pInfo);
  tsem_post(&pVnode->canTrim);
  return code;
}

// compact
static int32_t vnodeCompactImpl(SCompactInfo* pInfo) {
  int32_t code = 0;
  SVnode* pVnode = pInfo->pVnode;

  // TODO

  for (int32_t i = 0; i < 10; ++i) {
    taosSsleep(1);
    vInfo("%s:%d sleep %d", __func__, __LINE__, i);
  }

_exit:
  return code;
}

static int32_t vnodeCompactTask(void* arg) {
  int32_t       code = 0;
  SCompactInfo* pInfo = (SCompactInfo*)arg;
  SVnode*       pVnode = pInfo->pVnode;

  code = vnodeCompactImpl(pInfo);
  if (code) goto _exit;

_exit:
  vInfo("vgId:%d, vnode compact done, index:%d maxIndex:%d minFid:%d maxFid:%d", TD_VID(pVnode), pInfo->index,
        pInfo->maxIndex, pInfo->minFid, pInfo->maxFid);
  taosMemoryFree(pInfo);
  tsem_post(&pVnode->canTrim);
  return code;
}

// batch tasks - public func

// migrate
int32_t vnodeMigrate(SVnode* pVnode, SVTrimDbReq* pReq) {
  int32_t code = 0;

  SMigrateInfo* pInfo = (SMigrateInfo*)taosMemoryCalloc(1, sizeof(*pInfo));
  if (NULL == pInfo) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  pInfo->pVnode = pVnode;
  pInfo->req = *pReq;

  tsem_wait(&pVnode->canTrim);

  // schedule the task
  code = vnodePutScheduleTask(pVnode, vnodeMigrateTask, pInfo, VND_TASK_MIGRATE);

_exit:
  if (code) {
    taosMemoryFreeClear(pInfo);
    tsem_post(&pVnode->canTrim);
    vError("vgId:%d, %s failed since %s, commit id:%" PRId64, TD_VID(pVnode), __func__, tstrerror(code),
           pVnode->state.commitID);
  } else {
    vInfo("vgId:%d, vnode async commit done, commitId:%" PRId64 " term:%" PRId64 " applied:%" PRId64, TD_VID(pVnode),
          pVnode->state.commitID, pVnode->state.applyTerm, pVnode->state.applied);
  }
  return code;
}

// merge
int32_t vnodeMerge(SVnode* pVnode) {
  int32_t code = 0;

  SMergeInfo* pInfo = (SMergeInfo*)taosMemoryCalloc(1, sizeof(*pInfo));
  if (NULL == pInfo) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  pInfo->pVnode = pVnode;

  tsem_wait(&pVnode->canTrim);

  // schedule the task
  code = vnodePutScheduleTask(pVnode, vnodeMergeTask, pInfo, VND_TASK_MERGE);

_exit:
  if (code) {
    taosMemoryFreeClear(pInfo);
    tsem_post(&pVnode->canTrim);
    vError("vgId:%d, %s failed since %s, commit id:%" PRId64, TD_VID(pVnode), __func__, tstrerror(code),
           pVnode->state.commitID);
  } else {
    vInfo("vgId:%d, vnode async commit done, commitId:%" PRId64 " term:%" PRId64 " applied:%" PRId64, TD_VID(pVnode),
          pVnode->state.commitID, pVnode->state.applyTerm, pVnode->state.applied);
  }
  return code;
}

// compact
int32_t vnodeCompact(SVnode* pVnode) {
  int32_t code = 0;

  SCompactInfo* pInfo = (SCompactInfo*)taosMemoryCalloc(1, sizeof(*pInfo));
  if (NULL == pInfo) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  pInfo->pVnode = pVnode;

  tsem_wait(&pVnode->canTrim);

  // schedule the task
  code = vnodePutScheduleTask(pVnode, vnodeCompactTask, pInfo, VND_TASK_COMPACT);

_exit:
  if (code) {
    taosMemoryFreeClear(pInfo);
    tsem_post(&pVnode->canTrim);
    vError("vgId:%d, %s failed since %s, commit id:%" PRId64, TD_VID(pVnode), __func__, tstrerror(code),
           pVnode->state.commitID);
  } else {
    vInfo("vgId:%d, vnode async commit done, commitId:%" PRId64 " term:%" PRId64 " applied:%" PRId64, TD_VID(pVnode),
          pVnode->state.commitID, pVnode->state.applyTerm, pVnode->state.applied);
  }
  return code;
}