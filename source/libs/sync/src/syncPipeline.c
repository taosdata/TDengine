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

#define _DEFAULT_SOURCE

#include "syncPipeline.h"
#include "syncCommit.h"
#include "syncIndexMgr.h"
#include "syncInt.h"
#include "syncRaftCfg.h"
#include "syncRaftEntry.h"
#include "syncRaftStore.h"
#include "syncReplication.h"
#include "syncRespMgr.h"
#include "syncSnapshot.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"

static bool syncIsMsgBlock(tmsg_t type) {
  return (type == TDMT_VND_CREATE_TABLE) || (type == TDMT_VND_ALTER_TABLE) || (type == TDMT_VND_DROP_TABLE) ||
         (type == TDMT_VND_UPDATE_TAG_VAL) || (type == TDMT_VND_ALTER_CONFIRM);
}

FORCE_INLINE static int64_t syncGetRetryMaxWaitMs() {
  return SYNC_LOG_REPL_RETRY_WAIT_MS * (1 << SYNC_MAX_RETRY_BACKOFF);
}

int64_t syncLogBufferGetEndIndex(SSyncLogBuffer* pBuf) {
  (void)taosThreadMutexLock(&pBuf->mutex);
  int64_t index = pBuf->endIndex;
  (void)taosThreadMutexUnlock(&pBuf->mutex);
  return index;
}

int32_t syncLogBufferAppend(SSyncLogBuffer* pBuf, SSyncNode* pNode, SSyncRaftEntry* pEntry) {
  int32_t code = 0;
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  (void)taosThreadMutexLock(&pBuf->mutex);
  SyncIndex index = pEntry->index;

  if (index - pBuf->startIndex >= pBuf->size) {
    code = TSDB_CODE_SYN_BUFFER_FULL;
    sError("vgId:%d, failed to append since %s. index:%" PRId64 "", pNode->vgId, tstrerror(code), index);
    goto _err;
  }

  if (pNode->restoreFinish && index - pBuf->commitIndex >= TSDB_SYNC_NEGOTIATION_WIN) {
    code = TSDB_CODE_SYN_NEGOTIATION_WIN_FULL;
    sError("vgId:%d, failed to append since %s, index:%" PRId64 ", commit-index:%" PRId64, pNode->vgId, tstrerror(code),
           index, pBuf->commitIndex);
    goto _err;
  }

  SyncIndex appliedIndex = pNode->pFsm->FpAppliedIndexCb(pNode->pFsm);
  if (pNode->restoreFinish && pBuf->commitIndex - appliedIndex >= TSDB_SYNC_APPLYQ_SIZE_LIMIT) {
    code = TSDB_CODE_SYN_WRITE_STALL;
    sError("vgId:%d, failed to append since %s. index:%" PRId64 ", commit-index:%" PRId64 ", applied-index:%" PRId64,
           pNode->vgId, tstrerror(code), index, pBuf->commitIndex, appliedIndex);
    goto _err;
  }

  if (index != pBuf->endIndex) {
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _err;
  };

  SSyncRaftEntry* pExist = pBuf->entries[index % pBuf->size].pItem;
  if (pExist != NULL) {
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _err;
  }

  // initial log buffer with at least one item, e.g. commitIndex
  SSyncRaftEntry* pMatch = pBuf->entries[(index - 1 + pBuf->size) % pBuf->size].pItem;
  if (pMatch == NULL) {
    sError("vgId:%d, no matched log entry", pNode->vgId);
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _err;
  }
  if (pMatch->index + 1 != index) {
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _err;
  }
  if (!(pMatch->term <= pEntry->term)) {
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _err;
  }

  SSyncLogBufEntry tmp = {.pItem = pEntry, .prevLogIndex = pMatch->index, .prevLogTerm = pMatch->term};
  pBuf->entries[index % pBuf->size] = tmp;
  pBuf->endIndex = index + 1;

  (void)taosThreadMutexUnlock(&pBuf->mutex);
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  return 0;

_err:
  (void)taosThreadMutexUnlock(&pBuf->mutex);
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  taosMsleep(1);
  TAOS_RETURN(code);
}

int32_t syncLogReplGetPrevLogTerm(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index, SyncTerm* pSyncTerm) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  SSyncRaftEntry* pEntry = NULL;
  SyncIndex       prevIndex = index - 1;
  SyncTerm        prevLogTerm = -1;
  int32_t         code = 0;

  if (prevIndex == -1 && pNode->pLogStore->syncLogBeginIndex(pNode->pLogStore) == 0) {
    *pSyncTerm = 0;
    return 0;
  }

  if (prevIndex > pBuf->matchIndex) {
    *pSyncTerm = -1;
    TAOS_RETURN(TSDB_CODE_WAL_LOG_NOT_EXIST);
  }

  if (index - 1 != prevIndex) return TSDB_CODE_SYN_INTERNAL_ERROR;

  if (prevIndex >= pBuf->startIndex) {
    pEntry = pBuf->entries[(prevIndex + pBuf->size) % pBuf->size].pItem;
    if (pEntry == NULL) {
      sError("vgId:%d, failed to get pre log term since no log entry found", pNode->vgId);
      *pSyncTerm = -1;
      TAOS_RETURN(TSDB_CODE_SYN_INTERNAL_ERROR);
    }
    prevLogTerm = pEntry->term;
    *pSyncTerm = prevLogTerm;
    return 0;
  }

  if (pMgr && pMgr->startIndex <= prevIndex && prevIndex < pMgr->endIndex) {
    int64_t timeMs = pMgr->states[(prevIndex + pMgr->size) % pMgr->size].timeMs;
    if (timeMs == 0) {
      sError("vgId:%d, failed to get pre log term since timeMs is 0", pNode->vgId);
      *pSyncTerm = -1;
      TAOS_RETURN(TSDB_CODE_SYN_INTERNAL_ERROR);
    }
    prevLogTerm = pMgr->states[(prevIndex + pMgr->size) % pMgr->size].term;
    if (!(prevIndex == 0 || prevLogTerm != 0)) {
      sError("vgId:%d, failed to get pre log term prevIndex:%" PRId64 ", prevLogTerm:%" PRId64, pNode->vgId, prevIndex,
             prevLogTerm);
      *pSyncTerm = -1;
      TAOS_RETURN(TSDB_CODE_SYN_INTERNAL_ERROR);
    }
    *pSyncTerm = prevLogTerm;
    return 0;
  }

  SSnapshot snapshot = {0};
  (void)pNode->pFsm->FpGetSnapshotInfo(pNode->pFsm, &snapshot);
  if (prevIndex == snapshot.lastApplyIndex) {
    *pSyncTerm = snapshot.lastApplyTerm;
    return 0;
  }

  if ((code = pNode->pLogStore->syncLogGetEntry(pNode->pLogStore, prevIndex, &pEntry)) == 0) {
    prevLogTerm = pEntry->term;
    syncEntryDestroy(pEntry);
    pEntry = NULL;
    *pSyncTerm = prevLogTerm;
    return 0;
  }

  *pSyncTerm = -1;
  sInfo("vgId:%d, failed to get log term since %s. index:%" PRId64, pNode->vgId, tstrerror(code), prevIndex);
  TAOS_RETURN(code);
}

SSyncRaftEntry* syncEntryBuildDummy(SyncTerm term, SyncIndex index, int32_t vgId) {
  return syncEntryBuildNoop(term, index, vgId);
}

int32_t syncLogValidateAlignmentOfCommit(SSyncNode* pNode, SyncIndex commitIndex) {
  SyncIndex firstVer = pNode->pLogStore->syncLogBeginIndex(pNode->pLogStore);
  if (firstVer > commitIndex + 1) {
    sError("vgId:%d, firstVer of WAL log greater than tsdb commit version + 1. firstVer:%" PRId64
           ", tsdb commit version:%" PRId64 "",
           pNode->vgId, firstVer, commitIndex);
    return TSDB_CODE_WAL_LOG_INCOMPLETE;
  }

  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  if (lastVer < commitIndex) {
    sError("vgId:%d, lastVer of WAL log less than tsdb commit version. lastVer:%" PRId64
           ", tsdb commit version:%" PRId64 "",
           pNode->vgId, lastVer, commitIndex);
    return TSDB_CODE_WAL_LOG_INCOMPLETE;
  }

  return 0;
}

int32_t syncLogBufferInitWithoutLock(SSyncLogBuffer* pBuf, SSyncNode* pNode) {
  if (pNode->pLogStore == NULL) {
    sError("log store not created");
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  if (pNode->pFsm == NULL) {
    sError("pFsm not registered");
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  if (pNode->pFsm->FpGetSnapshotInfo == NULL) {
    sError("FpGetSnapshotInfo not registered");
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }

  int32_t   code = 0, lino = 0;
  SSnapshot snapshot = {0};
  TAOS_CHECK_EXIT(pNode->pFsm->FpGetSnapshotInfo(pNode->pFsm, &snapshot));

  SyncIndex commitIndex = snapshot.lastApplyIndex;
  SyncTerm  commitTerm = TMAX(snapshot.lastApplyTerm, 0);
  TAOS_CHECK_EXIT(syncLogValidateAlignmentOfCommit(pNode, commitIndex));

  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  if (lastVer < commitIndex) return TSDB_CODE_SYN_INTERNAL_ERROR;
  ;
  SyncIndex toIndex = lastVer;
  // update match index
  pBuf->commitIndex = commitIndex;
  pBuf->matchIndex = toIndex;
  pBuf->endIndex = toIndex + 1;

  // load log entries in reverse order
  SSyncLogStore*  pLogStore = pNode->pLogStore;
  SyncIndex       index = toIndex;
  SSyncRaftEntry* pEntry = NULL;
  bool            takeDummy = false;
  int             emptySize = (TSDB_SYNC_LOG_BUFFER_SIZE >> 1);

  while (true) {
    if (index <= pBuf->commitIndex) {
      takeDummy = true;
      break;
    }

    if (pLogStore->syncLogGetEntry(pLogStore, index, &pEntry) < 0) {
      sWarn("vgId:%d, failed to get log entry since %s. index:%" PRId64 "", pNode->vgId, tstrerror(code), index);
      break;
    }

    bool taken = false;
    if (toIndex - index + 1 <= pBuf->size - emptySize) {
      SSyncLogBufEntry tmp = {.pItem = pEntry, .prevLogIndex = -1, .prevLogTerm = -1};
      pBuf->entries[index % pBuf->size] = tmp;
      taken = true;
    }

    if (index < toIndex) {
      pBuf->entries[(index + 1) % pBuf->size].prevLogIndex = pEntry->index;
      pBuf->entries[(index + 1) % pBuf->size].prevLogTerm = pEntry->term;
    }

    if (!taken) {
      syncEntryDestroy(pEntry);
      pEntry = NULL;
      break;
    }

    index--;
  }

  // put a dummy record at commitIndex if present in log buffer
  if (takeDummy) {
    if (index != pBuf->commitIndex) return TSDB_CODE_SYN_INTERNAL_ERROR;

    SSyncRaftEntry* pDummy = syncEntryBuildDummy(commitTerm, commitIndex, pNode->vgId);
    if (pDummy == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    SSyncLogBufEntry tmp = {.pItem = pDummy, .prevLogIndex = commitIndex - 1, .prevLogTerm = commitTerm};
    pBuf->entries[(commitIndex + pBuf->size) % pBuf->size] = tmp;

    if (index < toIndex) {
      pBuf->entries[(index + 1) % pBuf->size].prevLogIndex = commitIndex;
      pBuf->entries[(index + 1) % pBuf->size].prevLogTerm = commitTerm;
    }
  }

  // update startIndex
  pBuf->startIndex = takeDummy ? index : index + 1;

  pBuf->isCatchup = false;

  sInfo("vgId:%d, init sync log buffer. buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")", pNode->vgId,
        pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);

  // validate
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  return 0;

_exit:
  if (code != 0) {
    sError("vgId:%d, failed to initialize sync log buffer at line %d since %s.", pNode->vgId, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t syncLogBufferInit(SSyncLogBuffer* pBuf, SSyncNode* pNode) {
  (void)taosThreadMutexLock(&pBuf->mutex);
  int32_t ret = syncLogBufferInitWithoutLock(pBuf, pNode);
  (void)taosThreadMutexUnlock(&pBuf->mutex);
  return ret;
}

int32_t syncLogBufferReInit(SSyncLogBuffer* pBuf, SSyncNode* pNode) {
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  (void)taosThreadMutexLock(&pBuf->mutex);
  for (SyncIndex index = pBuf->startIndex; index < pBuf->endIndex; index++) {
    SSyncRaftEntry* pEntry = pBuf->entries[(index + pBuf->size) % pBuf->size].pItem;
    if (pEntry == NULL) continue;
    syncEntryDestroy(pEntry);
    pEntry = NULL;
    (void)memset(&pBuf->entries[(index + pBuf->size) % pBuf->size], 0, sizeof(pBuf->entries[0]));
  }
  pBuf->startIndex = pBuf->commitIndex = pBuf->matchIndex = pBuf->endIndex = 0;
  int32_t code = syncLogBufferInitWithoutLock(pBuf, pNode);
  if (code < 0) {
    sError("vgId:%d, failed to re-initialize sync log buffer since %s.", pNode->vgId, tstrerror(code));
  }
  (void)taosThreadMutexUnlock(&pBuf->mutex);
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  return code;
}

FORCE_INLINE SyncTerm syncLogBufferGetLastMatchTermWithoutLock(SSyncLogBuffer* pBuf) {
  SyncIndex       index = pBuf->matchIndex;
  SSyncRaftEntry* pEntry = pBuf->entries[(index + pBuf->size) % pBuf->size].pItem;
  if (pEntry == NULL) {
    sError("failed to get last match term since entry is null");
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }
  return pEntry->term;
}

SyncTerm syncLogBufferGetLastMatchTerm(SSyncLogBuffer* pBuf) {
  (void)taosThreadMutexLock(&pBuf->mutex);
  SyncTerm term = syncLogBufferGetLastMatchTermWithoutLock(pBuf);
  (void)taosThreadMutexUnlock(&pBuf->mutex);
  return term;
}

bool syncLogBufferIsEmpty(SSyncLogBuffer* pBuf) {
  (void)taosThreadMutexLock(&pBuf->mutex);
  bool empty = (pBuf->endIndex <= pBuf->startIndex);
  (void)taosThreadMutexUnlock(&pBuf->mutex);
  return empty;
}

int32_t syncLogBufferAccept(SSyncLogBuffer* pBuf, SSyncNode* pNode, SSyncRaftEntry* pEntry, SyncTerm prevTerm) {
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  (void)taosThreadMutexLock(&pBuf->mutex);
  int32_t         code = 0;
  SyncIndex       index = pEntry->index;
  SyncIndex       prevIndex = pEntry->index - 1;
  SyncTerm        lastMatchTerm = syncLogBufferGetLastMatchTermWithoutLock(pBuf);
  SSyncRaftEntry* pExist = NULL;
  bool            inBuf = true;

  if (lastMatchTerm < 0) {
    sError("vgId:%d, failed to accept, lastMatchTerm:%" PRId64, pNode->vgId, lastMatchTerm);
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _out;
  }

  if (index <= pBuf->commitIndex) {
    sTrace("vgId:%d, already committed. index:%" PRId64 ", term:%" PRId64 ". log buffer: [%" PRId64 " %" PRId64
           " %" PRId64 ", %" PRId64 ")",
           pNode->vgId, pEntry->index, pEntry->term, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex,
           pBuf->endIndex);
    SyncTerm term = -1;
    code = syncLogReplGetPrevLogTerm(NULL, pNode, index + 1, &term);
    if (pEntry->term < 0) {
      sError("vgId:%d, failed to accept, pEntry->term:%" PRId64, pNode->vgId, pEntry->term);
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
      goto _out;
    }
    if (term == pEntry->term) {
      code = 0;
    }
    goto _out;
  }

  if (pNode->raftCfg.cfg.nodeInfo[pNode->raftCfg.cfg.myIndex].nodeRole == TAOS_SYNC_ROLE_LEARNER && index > 0 &&
      index > pBuf->totalIndex) {
    pBuf->totalIndex = index;
    sTrace("vgId:%d, update learner progress. index:%" PRId64 ", term:%" PRId64 ": prevterm:%" PRId64
           " != lastmatch:%" PRId64 ". log buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
           pNode->vgId, pEntry->index, pEntry->term, prevTerm, lastMatchTerm, pBuf->startIndex, pBuf->commitIndex,
           pBuf->matchIndex, pBuf->endIndex);
  }

  if (index - pBuf->startIndex >= pBuf->size) {
    sWarn("vgId:%d, out of buffer range. index:%" PRId64 ", term:%" PRId64 ". log buffer: [%" PRId64 " %" PRId64
          " %" PRId64 ", %" PRId64 ")",
          pNode->vgId, pEntry->index, pEntry->term, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex,
          pBuf->endIndex);
    code = TSDB_CODE_OUT_OF_RANGE;
    goto _out;
  }

  if (index > pBuf->matchIndex && lastMatchTerm != prevTerm) {
    sWarn("vgId:%d, not ready to accept. index:%" PRId64 ", term:%" PRId64 ": prevterm:%" PRId64
          " != lastmatch:%" PRId64 ". log buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
          pNode->vgId, pEntry->index, pEntry->term, prevTerm, lastMatchTerm, pBuf->startIndex, pBuf->commitIndex,
          pBuf->matchIndex, pBuf->endIndex);
    code = TSDB_CODE_ACTION_IN_PROGRESS;
    goto _out;
  }

  // check current in buffer
  code = syncLogBufferGetOneEntry(pBuf, pNode, index, &inBuf, &pExist);
  if (pExist != NULL) {
    if (pEntry->index != pExist->index) {
      sError("vgId:%d, failed to accept, pEntry->index:%" PRId64 ", pExist->index:%" PRId64, pNode->vgId, pEntry->index,
             pExist->index);
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
      goto _out;
    }
    if (pEntry->term != pExist->term) {
      (void)syncLogBufferRollback(pBuf, pNode, index);
    } else {
      sTrace("vgId:%d, duplicate log entry received. index:%" PRId64 ", term:%" PRId64 ". log buffer: [%" PRId64
             " %" PRId64 " %" PRId64 ", %" PRId64 ")",
             pNode->vgId, pEntry->index, pEntry->term, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex,
             pBuf->endIndex);
      SyncTerm existPrevTerm = -1;
      (void)syncLogReplGetPrevLogTerm(NULL, pNode, index, &existPrevTerm);
      if (!(pEntry->term == pExist->term && (pEntry->index > pBuf->matchIndex || prevTerm == existPrevTerm))) {
        sError("vgId:%d, failed to accept, pEntry->term:%" PRId64 ", pExist->indexpExist->term:%" PRId64
               ", pEntry->index:%" PRId64 ", pBuf->matchIndex:%" PRId64 ", prevTerm:%" PRId64
               ", existPrevTerm:%" PRId64,
               pNode->vgId, pEntry->term, pExist->term, pEntry->index, pBuf->matchIndex, prevTerm, existPrevTerm);
        code = TSDB_CODE_SYN_INTERNAL_ERROR;
        goto _out;
      }
      code = 0;
      goto _out;
    }
  }

  // update
  if (!(pBuf->startIndex < index)) {
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _out;
  };
  if (!(index - pBuf->startIndex < pBuf->size)) {
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _out;
  }
  if (pBuf->entries[index % pBuf->size].pItem != NULL) {
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _out;
  }
  SSyncLogBufEntry tmp = {.pItem = pEntry, .prevLogIndex = prevIndex, .prevLogTerm = prevTerm};
  pEntry = NULL;
  pBuf->entries[index % pBuf->size] = tmp;

  // update end index
  pBuf->endIndex = TMAX(index + 1, pBuf->endIndex);

  // success
  code = 0;

_out:
  syncEntryDestroy(pEntry);
  if (!inBuf) {
    syncEntryDestroy(pExist);
    pExist = NULL;
  }
  (void)taosThreadMutexUnlock(&pBuf->mutex);
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  TAOS_RETURN(code);
}

static inline bool syncLogStoreNeedFlush(SSyncRaftEntry* pEntry, int32_t replicaNum) {
  return (replicaNum > 1) && (pEntry->originalRpcType == TDMT_VND_COMMIT);
}

int32_t syncLogStorePersist(SSyncLogStore* pLogStore, SSyncNode* pNode, SSyncRaftEntry* pEntry) {
  int32_t code = 0;
  if (pEntry->index < 0) return TSDB_CODE_SYN_INTERNAL_ERROR;
  SyncIndex lastVer = pLogStore->syncLogLastIndex(pLogStore);
  if (lastVer >= pEntry->index && (code = pLogStore->syncLogTruncate(pLogStore, pEntry->index)) < 0) {
    sError("failed to truncate log store since %s. from index:%" PRId64 "", tstrerror(code), pEntry->index);
    TAOS_RETURN(code);
  }
  lastVer = pLogStore->syncLogLastIndex(pLogStore);
  if (pEntry->index != lastVer + 1) return TSDB_CODE_SYN_INTERNAL_ERROR;

  bool doFsync = syncLogStoreNeedFlush(pEntry, pNode->replicaNum);
  if ((code = pLogStore->syncLogAppendEntry(pLogStore, pEntry, doFsync)) < 0) {
    sError("failed to append sync log entry since %s. index:%" PRId64 ", term:%" PRId64 "", tstrerror(code),
           pEntry->index, pEntry->term);
    TAOS_RETURN(code);
  }

  lastVer = pLogStore->syncLogLastIndex(pLogStore);
  if (pEntry->index != lastVer) return TSDB_CODE_SYN_INTERNAL_ERROR;
  return 0;
}

int64_t syncLogBufferProceed(SSyncLogBuffer* pBuf, SSyncNode* pNode, SyncTerm* pMatchTerm, char* str) {
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  (void)taosThreadMutexLock(&pBuf->mutex);

  SSyncLogStore* pLogStore = pNode->pLogStore;
  int64_t        matchIndex = pBuf->matchIndex;
  int32_t        code = 0;

  while (pBuf->matchIndex + 1 < pBuf->endIndex) {
    int64_t index = pBuf->matchIndex + 1;
    if (index < 0) {
      sError("vgId:%d, failed to proceed index:%" PRId64, pNode->vgId, index);
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
      goto _out;
    }

    // try to proceed
    SSyncLogBufEntry* pBufEntry = &pBuf->entries[index % pBuf->size];
    SyncIndex         prevLogIndex = pBufEntry->prevLogIndex;
    SyncTerm          prevLogTerm = pBufEntry->prevLogTerm;
    SSyncRaftEntry*   pEntry = pBufEntry->pItem;
    if (pEntry == NULL) {
      sTrace("vgId:%d, cannot proceed match index in log buffer. no raft entry at next pos of matchIndex:%" PRId64,
             pNode->vgId, pBuf->matchIndex);
      goto _out;
    }

    if (index != pEntry->index) {
      sError("vgId:%d, failed to proceed index:%" PRId64 ", pEntry->index:%" PRId64, pNode->vgId, index, pEntry->index);
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
      goto _out;
    }

    // match
    SSyncRaftEntry* pMatch = pBuf->entries[(pBuf->matchIndex + pBuf->size) % pBuf->size].pItem;
    if (pMatch == NULL) {
      sError("vgId:%d, failed to proceed since pMatch is null", pNode->vgId);
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
      goto _out;
    }
    if (pMatch->index != pBuf->matchIndex) {
      sError("vgId:%d, failed to proceed, pMatch->index:%" PRId64 ", pBuf->matchIndex:%" PRId64, pNode->vgId,
             pMatch->index, pBuf->matchIndex);
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
      goto _out;
    }
    if (pMatch->index + 1 != pEntry->index) {
      sError("vgId:%d, failed to proceed, pMatch->index:%" PRId64 ", pEntry->index:%" PRId64, pNode->vgId,
             pMatch->index, pEntry->index);
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
      goto _out;
    }
    if (prevLogIndex != pMatch->index) {
      sError("vgId:%d, failed to proceed, prevLogIndex:%" PRId64 ", pMatch->index:%" PRId64, pNode->vgId, prevLogIndex,
             pMatch->index);
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
      goto _out;
    }

    if (pMatch->term != prevLogTerm) {
      sInfo(
          "vgId:%d, mismatching sync log entries encountered. "
          "{ index:%" PRId64 ", term:%" PRId64
          " } "
          "{ index:%" PRId64 ", term:%" PRId64 ", prevLogIndex:%" PRId64 ", prevLogTerm:%" PRId64 " } ",
          pNode->vgId, pMatch->index, pMatch->term, pEntry->index, pEntry->term, prevLogIndex, prevLogTerm);
      goto _out;
    }

    // increase match index
    pBuf->matchIndex = index;

    sTrace("vgId:%d, log buffer proceed. start index:%" PRId64 ", match index:%" PRId64 ", end index:%" PRId64,
           pNode->vgId, pBuf->startIndex, pBuf->matchIndex, pBuf->endIndex);

    // persist
    if ((code = syncLogStorePersist(pLogStore, pNode, pEntry)) < 0) {
      sError("vgId:%d, failed to persist sync log entry from buffer since %s. index:%" PRId64, pNode->vgId,
             tstrerror(code), pEntry->index);
      taosMsleep(1);
      goto _out;
    }

    if (pEntry->originalRpcType == TDMT_SYNC_CONFIG_CHANGE) {
      if (pNode->pLogBuf->commitIndex == pEntry->index - 1) {
        sInfo(
            "vgId:%d, to change config at %s. "
            "current entry, index:%" PRId64 ", term:%" PRId64
            ", "
            "node, restore:%d, commitIndex:%" PRId64
            ", "
            "cond: (pre entry index:%" PRId64 "== buf commit index:%" PRId64 ")",
            pNode->vgId, str, pEntry->index, pEntry->term, pNode->restoreFinish, pNode->commitIndex, pEntry->index - 1,
            pNode->pLogBuf->commitIndex);
        if ((code = syncNodeChangeConfig(pNode, pEntry, str)) != 0) {
          sError("vgId:%d, failed to change config from Append since %s. index:%" PRId64, pNode->vgId, tstrerror(code),
                 pEntry->index);
          goto _out;
        }
      } else {
        sInfo(
            "vgId:%d, delay change config from Node %s. "
            "curent entry, index:%" PRId64 ", term:%" PRId64
            ", "
            "node, commitIndex:%" PRId64 ",  pBuf: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64
            "), "
            "cond:( pre entry index:%" PRId64 " != buf commit index:%" PRId64 ")",
            pNode->vgId, str, pEntry->index, pEntry->term, pNode->commitIndex, pNode->pLogBuf->startIndex,
            pNode->pLogBuf->commitIndex, pNode->pLogBuf->matchIndex, pNode->pLogBuf->endIndex, pEntry->index - 1,
            pNode->pLogBuf->commitIndex);
      }
    }

    // replicate on demand
    (void)syncNodeReplicateWithoutLock(pNode);

    if (pEntry->index != pBuf->matchIndex) {
      sError("vgId:%d, failed to proceed, pEntry->index:%" PRId64 ", pBuf->matchIndex:%" PRId64, pNode->vgId,
             pEntry->index, pBuf->matchIndex);
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
      goto _out;
    }

    // update my match index
    matchIndex = pBuf->matchIndex;
    syncIndexMgrSetIndex(pNode->pMatchIndex, &pNode->myRaftId, pBuf->matchIndex);
  }  // end of while

_out:
  pBuf->matchIndex = matchIndex;
  if (pMatchTerm) {
    *pMatchTerm = pBuf->entries[(matchIndex + pBuf->size) % pBuf->size].pItem->term;
  }
  (void)taosThreadMutexUnlock(&pBuf->mutex);
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  return matchIndex;
}

int32_t syncFsmExecute(SSyncNode* pNode, SSyncFSM* pFsm, ESyncState role, SyncTerm term, SSyncRaftEntry* pEntry,
                       int32_t applyCode, bool force) {
  // learner need to execute fsm when it catch up entry log
  // if force is true, keep all contition check to execute fsm
  if (pNode->replicaNum == 1 && pNode->restoreFinish && pNode->vgId != 1 &&
      pNode->raftCfg.cfg.nodeInfo[pNode->raftCfg.cfg.myIndex].nodeRole != TAOS_SYNC_ROLE_LEARNER && force == false) {
    sDebug("vgId:%d, not to execute fsm, index:%" PRId64 ", term:%" PRId64
           ", type:%s code:0x%x, replicaNum:%d,"
           "role:%d, restoreFinish:%d",
           pNode->vgId, pEntry->index, pEntry->term, TMSG_INFO(pEntry->originalRpcType), applyCode, pNode->replicaNum,
           pNode->raftCfg.cfg.nodeInfo[pNode->raftCfg.cfg.myIndex].nodeRole, pNode->restoreFinish);
    return 0;
  }

  if (pNode->vgId != 1 && syncIsMsgBlock(pEntry->originalRpcType)) {
    sTrace("vgId:%d, blocking msg ready to execute, index:%" PRId64 ", term:%" PRId64 ", type:%s code:0x%x",
           pNode->vgId, pEntry->index, pEntry->term, TMSG_INFO(pEntry->originalRpcType), applyCode);
  }

  if (pEntry->originalRpcType == TDMT_VND_COMMIT) {
    sInfo("vgId:%d, fsm execute vnode commit. index:%" PRId64 ", term:%" PRId64 "", pNode->vgId, pEntry->index,
          pEntry->term);
  }

  int32_t code = 0, lino = 0;
  bool    retry = false;
  do {
    SFsmCbMeta cbMeta = {0};
    cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(pNode, pEntry->index);
    if (cbMeta.lastConfigIndex < -1) {
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
      if (terrno != 0) code = terrno;
      return code;
    }

    SRpcMsg rpcMsg = {.code = applyCode};
    TAOS_CHECK_EXIT(syncEntry2OriginalRpc(pEntry, &rpcMsg));

    cbMeta.index = pEntry->index;
    cbMeta.isWeak = pEntry->isWeak;
    cbMeta.code = applyCode;
    cbMeta.state = role;
    cbMeta.seqNum = pEntry->seqNum;
    cbMeta.term = pEntry->term;
    cbMeta.currentTerm = term;
    cbMeta.flag = -1;

    (void)syncRespMgrGetAndDel(pNode->pSyncRespMgr, cbMeta.seqNum, &rpcMsg.info);
    code = pFsm->FpCommitCb(pFsm, &rpcMsg, &cbMeta);
    retry = (code != 0) && (terrno == TSDB_CODE_OUT_OF_RPC_MEMORY_QUEUE);
    if (retry) {
      taosMsleep(10);
      sError("vgId:%d, retry on fsm commit since %s. index:%" PRId64, pNode->vgId, tstrerror(code), pEntry->index);
    }
  } while (retry);

_exit:
  if (code < 0) {
    sError("vgId:%d, failed to execute fsm at line %d since %s. index:%" PRId64 ", term:%" PRId64 ", type:%s",
           pNode->vgId, lino, tstrerror(code), pEntry->index, pEntry->term, TMSG_INFO(pEntry->originalRpcType));
  }
  return code;
}

int32_t syncLogBufferValidate(SSyncLogBuffer* pBuf) {
  if (pBuf->startIndex > pBuf->matchIndex) {
    sError("failed to validate, pBuf->startIndex:%" PRId64 ", pBuf->matchIndex:%" PRId64, pBuf->startIndex,
           pBuf->matchIndex);
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  if (pBuf->commitIndex > pBuf->matchIndex) {
    sError("failed to validate, pBuf->commitIndex:%" PRId64 ", pBuf->matchIndex:%" PRId64, pBuf->commitIndex,
           pBuf->matchIndex);
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  if (pBuf->matchIndex >= pBuf->endIndex) {
    sError("failed to validate, pBuf->matchIndex:%" PRId64 ", pBuf->endIndex:%" PRId64, pBuf->matchIndex,
           pBuf->endIndex);
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  if (pBuf->endIndex - pBuf->startIndex > pBuf->size) {
    sError("failed to validate, pBuf->endIndex:%" PRId64 ", pBuf->startIndex:%" PRId64 ", pBuf->size:%" PRId64,
           pBuf->endIndex, pBuf->startIndex, pBuf->size);
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  if (pBuf->entries[(pBuf->matchIndex + pBuf->size) % pBuf->size].pItem == NULL) {
    sError("failed to validate since pItem is null");
    return TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  return 0;
}

int32_t syncLogBufferCommit(SSyncLogBuffer* pBuf, SSyncNode* pNode, int64_t commitIndex) {
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  (void)taosThreadMutexLock(&pBuf->mutex);

  SSyncLogStore*  pLogStore = pNode->pLogStore;
  SSyncFSM*       pFsm = pNode->pFsm;
  ESyncState      role = pNode->state;
  SyncTerm        currentTerm = raftStoreGetTerm(pNode);
  SyncGroupId     vgId = pNode->vgId;
  int32_t         code = 0;
  int64_t         upperIndex = TMIN(commitIndex, pBuf->matchIndex);
  SSyncRaftEntry* pEntry = NULL;
  bool            inBuf = false;
  SSyncRaftEntry* pNextEntry = NULL;
  bool            nextInBuf = false;

  if (commitIndex <= pBuf->commitIndex) {
    sDebug("vgId:%d, stale commit index. current:%" PRId64 ", notified:%" PRId64 "", vgId, pBuf->commitIndex,
           commitIndex);
    goto _out;
  }

  sTrace("vgId:%d, commit. log buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 "), role:%d, term:%" PRId64,
         pNode->vgId, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex, role, currentTerm);

  // execute in fsm
  for (int64_t index = pBuf->commitIndex + 1; index <= upperIndex; index++) {
    // get a log entry
    code = syncLogBufferGetOneEntry(pBuf, pNode, index, &inBuf, &pEntry);
    if (pEntry == NULL) {
      goto _out;
    }

    // execute it
    if (!syncUtilUserCommit(pEntry->originalRpcType)) {
      sInfo("vgId:%d, commit sync barrier. index:%" PRId64 ", term:%" PRId64 ", type:%s", vgId, pEntry->index,
            pEntry->term, TMSG_INFO(pEntry->originalRpcType));
    }

    if ((code = syncFsmExecute(pNode, pFsm, role, currentTerm, pEntry, 0, false)) != 0) {
      sError("vgId:%d, failed to execute sync log entry. index:%" PRId64 ", term:%" PRId64
             ", role:%d, current term:%" PRId64,
             vgId, pEntry->index, pEntry->term, role, currentTerm);
      goto _out;
    }
    pBuf->commitIndex = index;

    sTrace("vgId:%d, committed index:%" PRId64 ", term:%" PRId64 ", role:%d, current term:%" PRId64 "", pNode->vgId,
           pEntry->index, pEntry->term, role, currentTerm);

    code = syncLogBufferGetOneEntry(pBuf, pNode, index + 1, &nextInBuf, &pNextEntry);
    if (pNextEntry != NULL) {
      if (pNextEntry->originalRpcType == TDMT_SYNC_CONFIG_CHANGE) {
        sInfo(
            "vgId:%d, to change config at Commit. "
            "current entry, index:%" PRId64 ", term:%" PRId64
            ", "
            "node, role:%d, current term:%" PRId64
            ", restore:%d, "
            "cond, next entry index:%" PRId64 ", msgType:%s",
            vgId, pEntry->index, pEntry->term, role, currentTerm, pNode->restoreFinish, pNextEntry->index,
            TMSG_INFO(pNextEntry->originalRpcType));

        if ((code = syncNodeChangeConfig(pNode, pNextEntry, "Commit")) != 0) {
          sError("vgId:%d, failed to change config from Commit. index:%" PRId64 ", term:%" PRId64
                 ", role:%d, current term:%" PRId64,
                 vgId, pNextEntry->index, pNextEntry->term, role, currentTerm);
          goto _out;
        }

        // for 2->1, need to apply config change entry in sync thread,
        if (pNode->replicaNum == 1) {
          if ((code = syncFsmExecute(pNode, pFsm, role, currentTerm, pNextEntry, 0, true)) != 0) {
            sError("vgId:%d, failed to execute sync log entry. index:%" PRId64 ", term:%" PRId64
                   ", role:%d, current term:%" PRId64,
                   vgId, pNextEntry->index, pNextEntry->term, role, currentTerm);
            goto _out;
          }

          index++;
          pBuf->commitIndex = index;

          sTrace("vgId:%d, committed index:%" PRId64 ", term:%" PRId64 ", role:%d, current term:%" PRId64 "",
                 pNode->vgId, pNextEntry->index, pNextEntry->term, role, currentTerm);
        }
      }
      if (!nextInBuf) {
        syncEntryDestroy(pNextEntry);
        pNextEntry = NULL;
      }
    }

    if (!inBuf) {
      syncEntryDestroy(pEntry);
      pEntry = NULL;
    }
  }

  // recycle
  SyncIndex until = pBuf->commitIndex - TSDB_SYNC_LOG_BUFFER_RETENTION;
  for (SyncIndex index = pBuf->startIndex; index < until; index++) {
    SSyncRaftEntry* pEntry = pBuf->entries[(index + pBuf->size) % pBuf->size].pItem;
    if (pEntry == NULL) return TSDB_CODE_SYN_INTERNAL_ERROR;
    syncEntryDestroy(pEntry);
    (void)memset(&pBuf->entries[(index + pBuf->size) % pBuf->size], 0, sizeof(pBuf->entries[0]));
    pBuf->startIndex = index + 1;
  }

  code = 0;
_out:
  // mark as restored if needed
  if (!pNode->restoreFinish && pBuf->commitIndex >= pNode->commitIndex && pEntry != NULL &&
      currentTerm <= pEntry->term) {
    pNode->pFsm->FpRestoreFinishCb(pNode->pFsm, pBuf->commitIndex);
    pNode->restoreFinish = true;
    sInfo("vgId:%d, restore finished. term:%" PRId64 ", log buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
          pNode->vgId, currentTerm, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
  }

  if (!pNode->restoreFinish && pBuf->commitIndex == -1) {
    pNode->pFsm->FpRestoreFinishCb(pNode->pFsm, pBuf->commitIndex);
    pNode->restoreFinish = true;
    sInfo("vgId:%d, restore finished. term:%" PRId64 ", log buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
          pNode->vgId, currentTerm, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
  }

  if (!inBuf) {
    syncEntryDestroy(pEntry);
    pEntry = NULL;
  }
  if (!nextInBuf) {
    syncEntryDestroy(pNextEntry);
    pNextEntry = NULL;
  }
  (void)taosThreadMutexUnlock(&pBuf->mutex);
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  TAOS_RETURN(code);
}

void syncLogReplReset(SSyncLogReplMgr* pMgr) {
  if (pMgr == NULL) return;

  if (pMgr->startIndex < 0) {
    sError("failed to reset, pMgr->startIndex:%" PRId64, pMgr->startIndex);
    return;
  }
  for (SyncIndex index = pMgr->startIndex; index < pMgr->endIndex; index++) {
    (void)memset(&pMgr->states[index % pMgr->size], 0, sizeof(pMgr->states[0]));
  }
  pMgr->startIndex = 0;
  pMgr->matchIndex = 0;
  pMgr->endIndex = 0;
  pMgr->restored = false;
  pMgr->retryBackoff = 0;
}

int32_t syncLogReplRetryOnNeed(SSyncLogReplMgr* pMgr, SSyncNode* pNode) {
  if (pMgr->endIndex <= pMgr->startIndex) {
    return 0;
  }

  SRaftId* pDestId = &pNode->replicasId[pMgr->peerId];
  if (pMgr->retryBackoff == SYNC_MAX_RETRY_BACKOFF) {
    syncLogReplReset(pMgr);
    sWarn("vgId:%d, reset sync log repl since retry backoff exceeding limit. peer:%" PRIx64, pNode->vgId,
          pDestId->addr);
    return TSDB_CODE_OUT_OF_RANGE;
  }

  int32_t  code = 0;
  bool     retried = false;
  int64_t  retryWaitMs = syncLogReplGetRetryBackoffTimeMs(pMgr);
  int64_t  nowMs = taosGetMonoTimestampMs();
  int      count = 0;
  int64_t  firstIndex = -1;
  SyncTerm term = -1;
  int64_t  batchSize = TMAX(1, pMgr->size >> (4 + pMgr->retryBackoff));

  for (SyncIndex index = pMgr->startIndex; index < pMgr->endIndex; index++) {
    int64_t pos = index % pMgr->size;
    if (!(!pMgr->states[pos].barrier || (index == pMgr->startIndex || index + 1 == pMgr->endIndex))) {
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
      goto _out;
    }

    if (nowMs < pMgr->states[pos].timeMs + retryWaitMs) {
      break;
    }

    if (pMgr->states[pos].acked) {
      if (pMgr->matchIndex < index && pMgr->states[pos].timeMs + (syncGetRetryMaxWaitMs() << 3) < nowMs) {
        syncLogReplReset(pMgr);
        sWarn("vgId:%d, reset sync log repl since stagnation. index:%" PRId64 ", peer:%" PRIx64, pNode->vgId, index,
              pDestId->addr);
        code = TSDB_CODE_ACTION_IN_PROGRESS;
        goto _out;
      }
      continue;
    }

    bool barrier = false;
    if ((code = syncLogReplSendTo(pMgr, pNode, index, &term, pDestId, &barrier)) < 0) {
      sError("vgId:%d, failed to replicate sync log entry since %s. index:%" PRId64 ", dest:%" PRIx64 "", pNode->vgId,
             tstrerror(code), index, pDestId->addr);
      goto _out;
    }
    if (barrier != pMgr->states[pos].barrier) {
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
      goto _out;
    }
    pMgr->states[pos].timeMs = nowMs;
    pMgr->states[pos].term = term;
    pMgr->states[pos].acked = false;

    retried = true;
    if (firstIndex == -1) firstIndex = index;

    if (batchSize <= count++) {
      break;
    }
  }

_out:
  if (retried) {
    pMgr->retryBackoff = syncLogReplGetNextRetryBackoff(pMgr);
    SSyncLogBuffer* pBuf = pNode->pLogBuf;
    sInfo("vgId:%d, resend %d sync log entries. dest:%" PRIx64 ", indexes:%" PRId64 " ..., terms: ... %" PRId64
          ", retryWaitMs:%" PRId64 ", repl-mgr:[%" PRId64 " %" PRId64 ", %" PRId64 "), buffer: [%" PRId64 " %" PRId64
          " %" PRId64 ", %" PRId64 ")",
          pNode->vgId, count, pDestId->addr, firstIndex, term, retryWaitMs, pMgr->startIndex, pMgr->matchIndex,
          pMgr->endIndex, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
  }
  TAOS_RETURN(code);
}

int32_t syncLogReplRecover(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  SRaftId         destId = pMsg->srcId;
  int32_t         code = 0;
  if (pMgr->restored != false) return TSDB_CODE_SYN_INTERNAL_ERROR;

  if (pMgr->endIndex == 0) {
    if (pMgr->startIndex != 0) return TSDB_CODE_SYN_INTERNAL_ERROR;
    if (pMgr->matchIndex != 0) return TSDB_CODE_SYN_INTERNAL_ERROR;
    if (pMsg->matchIndex < 0) {
      pMgr->restored = true;
      sInfo("vgId:%d, sync log repl restored. peer: dnode:%d (%" PRIx64 "), repl-mgr:[%" PRId64 " %" PRId64 ", %" PRId64
            "), buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
            pNode->vgId, DID(&destId), destId.addr, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex,
            pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
      return 0;
    }
  } else {
    if (pMsg->lastSendIndex < pMgr->startIndex || pMsg->lastSendIndex >= pMgr->endIndex) {
      (void)syncLogReplRetryOnNeed(pMgr, pNode);
      return 0;
    }

    pMgr->states[pMsg->lastSendIndex % pMgr->size].acked = true;

    if (pMsg->success && pMsg->matchIndex == pMsg->lastSendIndex) {
      pMgr->matchIndex = pMsg->matchIndex;
      pMgr->restored = true;
      sInfo("vgId:%d, sync log repl restored. peer: dnode:%d (%" PRIx64 "), repl-mgr:[%" PRId64 " %" PRId64 ", %" PRId64
            "), buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
            pNode->vgId, DID(&destId), destId.addr, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex,
            pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
      return 0;
    }

    if (pMsg->fsmState == SYNC_FSM_STATE_INCOMPLETE || (!pMsg->success && pMsg->matchIndex >= pMsg->lastSendIndex)) {
      char* msg1 = " rollback match index failure";
      char* msg2 = " incomplete fsm state";
      sInfo("vgId:%d, snapshot replication to dnode:%d. reason:%s, match index:%" PRId64 ", last sent:%" PRId64,
            pNode->vgId, DID(&destId), (pMsg->fsmState == SYNC_FSM_STATE_INCOMPLETE ? msg2 : msg1), pMsg->matchIndex,
            pMsg->lastSendIndex);
      if ((code = syncNodeStartSnapshot(pNode, &destId)) < 0) {
        sError("vgId:%d, failed to start snapshot for peer dnode:%d", pNode->vgId, DID(&destId));
        TAOS_RETURN(code);
      }
      return 0;
    }
  }

  // check last match term
  SyncTerm  term = -1;
  SyncIndex firstVer = pNode->pLogStore->syncLogBeginIndex(pNode->pLogStore);
  SyncIndex index = TMIN(pMsg->matchIndex, pNode->pLogBuf->matchIndex);
  errno = 0;

  if (pMsg->matchIndex < pNode->pLogBuf->matchIndex) {
    code = syncLogReplGetPrevLogTerm(pMgr, pNode, index + 1, &term);
    if (term < 0 && (errno == ENFILE || errno == EMFILE || errno == ENOENT)) {
      sError("vgId:%d, failed to get prev log term since %s. index:%" PRId64, pNode->vgId, tstrerror(code), index + 1);
      TAOS_RETURN(code);
    }

    if (pMsg->matchIndex == -1) {
      // first time to restore
      sInfo("vgId:%d, first time to restore sync log repl. peer: dnode:%d (%" PRIx64 "), repl-mgr:[%" PRId64 " %" PRId64
            ", %" PRId64 "), buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 "), index:%" PRId64
            ", firstVer:%" PRId64 ", term:%" PRId64 ", lastMatchTerm:%" PRId64,
            pNode->vgId, DID(&destId), destId.addr, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex,
            pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex, index, firstVer, term,
            pMsg->lastMatchTerm);
    }

    if ((index + 1 < firstVer) || (term < 0) ||
        (term != pMsg->lastMatchTerm && (index + 1 == firstVer || index == firstVer))) {
      if (!(term >= 0 || terrno == TSDB_CODE_WAL_LOG_NOT_EXIST)) return TSDB_CODE_SYN_INTERNAL_ERROR;
      if ((code = syncNodeStartSnapshot(pNode, &destId)) < 0) {
        sError("vgId:%d, failed to start snapshot for peer dnode:%d", pNode->vgId, DID(&destId));
        TAOS_RETURN(code);
      }
      sInfo("vgId:%d, snapshot replication to peer dnode:%d", pNode->vgId, DID(&destId));
      return 0;
    }

    if (!(index + 1 >= firstVer)) return TSDB_CODE_SYN_INTERNAL_ERROR;

    if (term == pMsg->lastMatchTerm) {
      index = index + 1;
      if (!(index <= pNode->pLogBuf->matchIndex)) return TSDB_CODE_SYN_INTERNAL_ERROR;
    } else {
      if (!(index > firstVer)) return TSDB_CODE_SYN_INTERNAL_ERROR;
    }
  }

  // attempt to replicate the raft log at index
  (void)syncLogReplReset(pMgr);
  return syncLogReplProbe(pMgr, pNode, index);
}

int32_t syncLogReplProcessHeartbeatReply(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncHeartbeatReply* pMsg) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  (void)taosThreadMutexLock(&pBuf->mutex);
  if (pMsg->startTime != 0 && pMsg->startTime != pMgr->peerStartTime) {
    sInfo("vgId:%d, reset sync log repl in heartbeat. peer:%" PRIx64 ", start time:%" PRId64 ", old:%" PRId64 "",
          pNode->vgId, pMsg->srcId.addr, pMsg->startTime, pMgr->peerStartTime);
    syncLogReplReset(pMgr);
    pMgr->peerStartTime = pMsg->startTime;
  }
  (void)taosThreadMutexUnlock(&pBuf->mutex);
  return 0;
}

int32_t syncLogReplProcessReply(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  (void)taosThreadMutexLock(&pBuf->mutex);
  if (pMsg->startTime != pMgr->peerStartTime) {
    sInfo("vgId:%d, reset sync log repl in appendlog reply. peer:%" PRIx64 ", start time:%" PRId64 ", old:%" PRId64,
          pNode->vgId, pMsg->srcId.addr, pMsg->startTime, pMgr->peerStartTime);
    syncLogReplReset(pMgr);
    pMgr->peerStartTime = pMsg->startTime;
  }

  if (pMgr->restored) {
    (void)syncLogReplContinue(pMgr, pNode, pMsg);
  } else {
    (void)syncLogReplRecover(pMgr, pNode, pMsg);
  }
  (void)taosThreadMutexUnlock(&pBuf->mutex);
  return 0;
}

int32_t syncLogReplStart(SSyncLogReplMgr* pMgr, SSyncNode* pNode) {
  if (pMgr->restored) {
    (void)syncLogReplAttempt(pMgr, pNode);
  } else {
    (void)syncLogReplProbe(pMgr, pNode, pNode->pLogBuf->matchIndex);
  }
  return 0;
}

int32_t syncLogReplProbe(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index) {
  if (pMgr->restored) return TSDB_CODE_SYN_INTERNAL_ERROR;
  if (!(pMgr->startIndex >= 0)) return TSDB_CODE_SYN_INTERNAL_ERROR;
  int64_t retryMaxWaitMs = syncGetRetryMaxWaitMs();
  int64_t nowMs = taosGetMonoTimestampMs();
  int32_t code = 0;

  if (pMgr->endIndex > pMgr->startIndex &&
      nowMs < pMgr->states[pMgr->startIndex % pMgr->size].timeMs + retryMaxWaitMs) {
    return 0;
  }
  (void)syncLogReplReset(pMgr);

  SRaftId* pDestId = &pNode->replicasId[pMgr->peerId];
  bool     barrier = false;
  SyncTerm term = -1;
  if ((code = syncLogReplSendTo(pMgr, pNode, index, &term, pDestId, &barrier)) < 0) {
    sError("vgId:%d, failed to replicate log entry since %s. index:%" PRId64 ", dest: 0x%016" PRIx64 "", pNode->vgId,
           tstrerror(code), index, pDestId->addr);
    TAOS_RETURN(code);
  }

  if (!(index >= 0)) return TSDB_CODE_SYN_INTERNAL_ERROR;
  pMgr->states[index % pMgr->size].barrier = barrier;
  pMgr->states[index % pMgr->size].timeMs = nowMs;
  pMgr->states[index % pMgr->size].term = term;
  pMgr->states[index % pMgr->size].acked = false;

  pMgr->startIndex = index;
  pMgr->endIndex = index + 1;

  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  sTrace("vgId:%d, probe peer:%" PRIx64 " with msg of index:%" PRId64 " term:%" PRId64 ". repl-mgr:[%" PRId64
         " %" PRId64 ", %" PRId64 "), buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
         pNode->vgId, pDestId->addr, index, term, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex, pBuf->startIndex,
         pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
  return 0;
}

int32_t syncLogReplAttempt(SSyncLogReplMgr* pMgr, SSyncNode* pNode) {
  if (!pMgr->restored) return TSDB_CODE_SYN_INTERNAL_ERROR;

  SRaftId*  pDestId = &pNode->replicasId[pMgr->peerId];
  int32_t   batchSize = TMAX(1, pMgr->size >> (4 + pMgr->retryBackoff));
  int32_t   code = 0;
  int32_t   count = 0;
  int64_t   nowMs = taosGetMonoTimestampMs();
  int64_t   limit = pMgr->size >> 1;
  SyncTerm  term = -1;
  SyncIndex firstIndex = -1;

  for (SyncIndex index = pMgr->endIndex; index <= pNode->pLogBuf->matchIndex; index++) {
    if (batchSize < count || limit <= index - pMgr->startIndex) {
      break;
    }
    if (pMgr->startIndex + 1 < index && pMgr->states[(index - 1) % pMgr->size].barrier) {
      break;
    }
    int64_t  pos = index % pMgr->size;
    SRaftId* pDestId = &pNode->replicasId[pMgr->peerId];
    bool     barrier = false;
    SyncTerm term = -1;
    if ((code = syncLogReplSendTo(pMgr, pNode, index, &term, pDestId, &barrier)) < 0) {
      sError("vgId:%d, failed to replicate log entry since %s. index:%" PRId64 ", dest: 0x%016" PRIx64 "", pNode->vgId,
             tstrerror(code), index, pDestId->addr);
      TAOS_RETURN(code);
    }
    pMgr->states[pos].barrier = barrier;
    pMgr->states[pos].timeMs = nowMs;
    pMgr->states[pos].term = term;
    pMgr->states[pos].acked = false;

    if (firstIndex == -1) firstIndex = index;
    count++;

    pMgr->endIndex = index + 1;
    if (barrier) {
      sInfo("vgId:%d, replicated sync barrier to dnode:%d. index:%" PRId64 ", term:%" PRId64 ", repl-mgr:[%" PRId64
            " %" PRId64 ", %" PRId64 ")",
            pNode->vgId, DID(pDestId), index, term, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex);
      break;
    }
  }

  (void)syncLogReplRetryOnNeed(pMgr, pNode);

  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  sTrace("vgId:%d, replicated %d msgs to peer:%" PRIx64 ". indexes:%" PRId64 "..., terms: ...%" PRId64
         ", repl-mgr:[%" PRId64 " %" PRId64 ", %" PRId64 "), buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64
         ")",
         pNode->vgId, count, pDestId->addr, firstIndex, term, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex,
         pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
  return 0;
}

int32_t syncLogReplContinue(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg) {
  if (pMgr->restored != true) return TSDB_CODE_SYN_INTERNAL_ERROR;
  if (pMgr->startIndex <= pMsg->lastSendIndex && pMsg->lastSendIndex < pMgr->endIndex) {
    if (pMgr->startIndex < pMgr->matchIndex && pMgr->retryBackoff > 0) {
      int64_t firstMs = pMgr->states[pMgr->startIndex % pMgr->size].timeMs;
      int64_t lastMs = pMgr->states[(pMgr->endIndex - 1) % pMgr->size].timeMs;
      int64_t diffMs = lastMs - firstMs;
      if (diffMs > 0 && diffMs < ((int64_t)SYNC_LOG_REPL_RETRY_WAIT_MS << (pMgr->retryBackoff - 1))) {
        pMgr->retryBackoff -= 1;
      }
    }
    pMgr->states[pMsg->lastSendIndex % pMgr->size].acked = true;
    pMgr->matchIndex = TMAX(pMgr->matchIndex, pMsg->matchIndex);
    for (SyncIndex index = pMgr->startIndex; index < pMgr->matchIndex; index++) {
      (void)memset(&pMgr->states[index % pMgr->size], 0, sizeof(pMgr->states[0]));
    }
    pMgr->startIndex = pMgr->matchIndex;
  }

  return syncLogReplAttempt(pMgr, pNode);
}

SSyncLogReplMgr* syncLogReplCreate() {
  SSyncLogReplMgr* pMgr = taosMemoryCalloc(1, sizeof(SSyncLogReplMgr));
  if (pMgr == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pMgr->size = sizeof(pMgr->states) / sizeof(pMgr->states[0]);

  if (pMgr->size != TSDB_SYNC_LOG_BUFFER_SIZE) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return NULL;
  }

  return pMgr;
}

void syncLogReplDestroy(SSyncLogReplMgr* pMgr) {
  if (pMgr == NULL) {
    return;
  }
  (void)taosMemoryFree(pMgr);
  return;
}

int32_t syncNodeLogReplInit(SSyncNode* pNode) {
  for (int i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; i++) {
    if (pNode->logReplMgrs[i] != NULL) return TSDB_CODE_SYN_INTERNAL_ERROR;
    pNode->logReplMgrs[i] = syncLogReplCreate();
    if (pNode->logReplMgrs[i] == NULL) {
      TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
    }
    pNode->logReplMgrs[i]->peerId = i;
  }
  return 0;
}

void syncNodeLogReplDestroy(SSyncNode* pNode) {
  for (int i = 0; i < TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA; i++) {
    syncLogReplDestroy(pNode->logReplMgrs[i]);
    pNode->logReplMgrs[i] = NULL;
  }
}

int32_t syncLogBufferCreate(SSyncLogBuffer** ppBuf) {
  int32_t         code = 0;
  SSyncLogBuffer* pBuf = taosMemoryCalloc(1, sizeof(SSyncLogBuffer));
  if (pBuf == NULL) {
    TAOS_CHECK_GOTO(TSDB_CODE_OUT_OF_MEMORY, NULL, _exit);
  }

  pBuf->size = sizeof(pBuf->entries) / sizeof(pBuf->entries[0]);

  if (pBuf->size != TSDB_SYNC_LOG_BUFFER_SIZE) {
    code = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _exit;
  }

  if (taosThreadMutexAttrInit(&pBuf->attr) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    sError("failed to init log buffer mutexattr due to %s", tstrerror(code));
    goto _exit;
  }

  if (taosThreadMutexAttrSetType(&pBuf->attr, PTHREAD_MUTEX_RECURSIVE) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    sError("failed to set log buffer mutexattr type due to %s", tstrerror(code));
    goto _exit;
  }

  if (taosThreadMutexInit(&pBuf->mutex, &pBuf->attr) < 0) {
    code = TAOS_SYSTEM_ERROR(errno);
    sError("failed to init log buffer mutex due to %s", tstrerror(code));
    goto _exit;
  }
_exit:
  if (code != 0) {
    taosMemoryFreeClear(pBuf);
  }
  *ppBuf = pBuf;
  TAOS_RETURN(code);
}

void syncLogBufferClear(SSyncLogBuffer* pBuf) {
  (void)taosThreadMutexLock(&pBuf->mutex);
  for (SyncIndex index = pBuf->startIndex; index < pBuf->endIndex; index++) {
    SSyncRaftEntry* pEntry = pBuf->entries[(index + pBuf->size) % pBuf->size].pItem;
    if (pEntry == NULL) continue;
    syncEntryDestroy(pEntry);
    pEntry = NULL;
    (void)memset(&pBuf->entries[(index + pBuf->size) % pBuf->size], 0, sizeof(pBuf->entries[0]));
  }
  pBuf->startIndex = pBuf->commitIndex = pBuf->matchIndex = pBuf->endIndex = 0;
  (void)taosThreadMutexUnlock(&pBuf->mutex);
}

void syncLogBufferDestroy(SSyncLogBuffer* pBuf) {
  if (pBuf == NULL) {
    return;
  }
  syncLogBufferClear(pBuf);
  (void)taosThreadMutexDestroy(&pBuf->mutex);
  (void)taosThreadMutexAttrDestroy(&pBuf->attr);
  (void)taosMemoryFree(pBuf);
  return;
}

int32_t syncLogBufferRollback(SSyncLogBuffer* pBuf, SSyncNode* pNode, SyncIndex toIndex) {
  int32_t code = 0;
  if (!(pBuf->commitIndex < toIndex && toIndex <= pBuf->endIndex)) return TSDB_CODE_SYN_INTERNAL_ERROR;

  if (toIndex == pBuf->endIndex) {
    return 0;
  }

  sInfo("vgId:%d, rollback sync log buffer. toindex:%" PRId64 ", buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64
        ")",
        pNode->vgId, toIndex, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);

  // trunc buffer
  SyncIndex index = pBuf->endIndex - 1;
  while (index >= toIndex) {
    SSyncRaftEntry* pEntry = pBuf->entries[index % pBuf->size].pItem;
    if (pEntry != NULL) {
      (void)syncEntryDestroy(pEntry);
      pEntry = NULL;
      (void)memset(&pBuf->entries[index % pBuf->size], 0, sizeof(pBuf->entries[0]));
    }
    index--;
  }
  pBuf->endIndex = toIndex;
  pBuf->matchIndex = TMIN(pBuf->matchIndex, index);
  if (index + 1 != toIndex) return TSDB_CODE_SYN_INTERNAL_ERROR;

  // trunc wal
  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  if (lastVer >= toIndex && (code = pNode->pLogStore->syncLogTruncate(pNode->pLogStore, toIndex)) < 0) {
    sError("vgId:%d, failed to truncate log store since %s. from index:%" PRId64 "", pNode->vgId, tstrerror(code),
           toIndex);
    TAOS_RETURN(code);
  }
  lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  if (toIndex != lastVer + 1) return TSDB_CODE_SYN_INTERNAL_ERROR;

  // refill buffer on need
  if (toIndex <= pBuf->startIndex) {
    if ((code = syncLogBufferInitWithoutLock(pBuf, pNode)) < 0) {
      sError("vgId:%d, failed to refill sync log buffer since %s", pNode->vgId, tstrerror(code));
      TAOS_RETURN(code);
    }
  }

  if (pBuf->endIndex != toIndex) return TSDB_CODE_SYN_INTERNAL_ERROR;
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  return 0;
}

int32_t syncLogBufferReset(SSyncLogBuffer* pBuf, SSyncNode* pNode) {
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  (void)taosThreadMutexLock(&pBuf->mutex);
  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  if (lastVer != pBuf->matchIndex) return TSDB_CODE_SYN_INTERNAL_ERROR;
  SyncIndex index = pBuf->endIndex - 1;

  (void)syncLogBufferRollback(pBuf, pNode, pBuf->matchIndex + 1);

  sInfo("vgId:%d, reset sync log buffer. buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")", pNode->vgId,
        pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);

  pBuf->endIndex = pBuf->matchIndex + 1;

  // reset repl mgr
  for (int i = 0; i < pNode->totalReplicaNum; i++) {
    SSyncLogReplMgr* pMgr = pNode->logReplMgrs[i];
    syncLogReplReset(pMgr);
  }
  (void)taosThreadMutexUnlock(&pBuf->mutex);
  TAOS_CHECK_RETURN(syncLogBufferValidate(pBuf));
  return 0;
}

int32_t syncLogBufferGetOneEntry(SSyncLogBuffer* pBuf, SSyncNode* pNode, SyncIndex index, bool* pInBuf,
                                 SSyncRaftEntry** ppEntry) {
  int32_t code = 0;

  *ppEntry = NULL;

  if (index >= pBuf->endIndex) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  if (index > pBuf->startIndex) {  // startIndex might be dummy
    *pInBuf = true;
    *ppEntry = pBuf->entries[index % pBuf->size].pItem;
  } else {
    *pInBuf = false;

    if ((code = pNode->pLogStore->syncLogGetEntry(pNode->pLogStore, index, ppEntry)) < 0) {
      sWarn("vgId:%d, failed to get log entry since %s. index:%" PRId64 "", pNode->vgId, tstrerror(code), index);
    }
  }
  TAOS_RETURN(code);
}

int32_t syncLogReplSendTo(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index, SyncTerm* pTerm, SRaftId* pDestId,
                          bool* pBarrier) {
  SSyncRaftEntry* pEntry = NULL;
  SRpcMsg         msgOut = {0};
  bool            inBuf = false;
  SyncTerm        prevLogTerm = -1;
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  int32_t         code = 0;

  code = syncLogBufferGetOneEntry(pBuf, pNode, index, &inBuf, &pEntry);
  if (pEntry == NULL) {
    sWarn("vgId:%d, failed to get raft entry for index:%" PRId64 "", pNode->vgId, index);
    if (code == TSDB_CODE_WAL_LOG_NOT_EXIST) {
      SSyncLogReplMgr* pMgr = syncNodeGetLogReplMgr(pNode, pDestId);
      if (pMgr) {
        sInfo("vgId:%d, reset sync log repl of peer:%" PRIx64 " since %s. index:%" PRId64, pNode->vgId, pDestId->addr,
              tstrerror(code), index);
        (void)syncLogReplReset(pMgr);
      }
    }
    goto _err;
  }
  *pBarrier = syncLogReplBarrier(pEntry);

  code = syncLogReplGetPrevLogTerm(pMgr, pNode, index, &prevLogTerm);
  if (prevLogTerm < 0) {
    sError("vgId:%d, failed to get prev log term since %s. index:%" PRId64 "", pNode->vgId, tstrerror(code), index);
    goto _err;
  }
  if (pTerm) *pTerm = pEntry->term;

  code = syncBuildAppendEntriesFromRaftEntry(pNode, pEntry, prevLogTerm, &msgOut);
  if (code < 0) {
    sError("vgId:%d, failed to get append entries for index:%" PRId64 "", pNode->vgId, index);
    goto _err;
  }

  (void)syncNodeSendAppendEntries(pNode, pDestId, &msgOut);

  sTrace("vgId:%d, replicate one msg index:%" PRId64 " term:%" PRId64 " prevterm:%" PRId64 " to dest: 0x%016" PRIx64,
         pNode->vgId, pEntry->index, pEntry->term, prevLogTerm, pDestId->addr);

  if (!inBuf) {
    syncEntryDestroy(pEntry);
    pEntry = NULL;
  }
  return 0;

_err:
  rpcFreeCont(msgOut.pCont);
  msgOut.pCont = NULL;
  if (!inBuf) {
    syncEntryDestroy(pEntry);
    pEntry = NULL;
  }
  TAOS_RETURN(code);
}
