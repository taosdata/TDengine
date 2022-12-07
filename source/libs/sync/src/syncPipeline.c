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
#include "syncRaftEntry.h"
#include "syncRaftStore.h"
#include "syncReplication.h"
#include "syncRespMgr.h"
#include "syncSnapshot.h"
#include "syncUtil.h"

int64_t syncLogBufferGetEndIndex(SSyncLogBuffer* pBuf) {
  taosThreadMutexLock(&pBuf->mutex);
  int64_t index = pBuf->endIndex;
  taosThreadMutexUnlock(&pBuf->mutex);
  return index;
}

int32_t syncLogBufferAppend(SSyncLogBuffer* pBuf, SSyncNode* pNode, SSyncRaftEntry* pEntry) {
  taosThreadMutexLock(&pBuf->mutex);
  syncLogBufferValidate(pBuf);
  SyncIndex index = pEntry->index;

  if (index - pBuf->startIndex >= pBuf->size) {
    sError("vgId:%d, failed to append due to sync log buffer full. index:%" PRId64 "", pNode->vgId, index);
    goto _out;
  }

  tAssert(index == pBuf->endIndex);

  SSyncRaftEntry* pExist = pBuf->entries[index % pBuf->size].pItem;
  tAssert(pExist == NULL);

  // initial log buffer with at least one item, e.g. commitIndex
  SSyncRaftEntry* pMatch = pBuf->entries[(index - 1 + pBuf->size) % pBuf->size].pItem;
  tAssert(pMatch != NULL && "no matched log entry");
  tAssert(pMatch->index + 1 == index);

  SSyncLogBufEntry tmp = {.pItem = pEntry, .prevLogIndex = pMatch->index, .prevLogTerm = pMatch->term};
  pBuf->entries[index % pBuf->size] = tmp;
  pBuf->endIndex = index + 1;

  syncLogBufferValidate(pBuf);
  taosThreadMutexUnlock(&pBuf->mutex);
  return 0;

_out:
  syncLogBufferValidate(pBuf);
  syncEntryDestroy(pEntry);
  taosThreadMutexUnlock(&pBuf->mutex);
  return -1;
}

SyncTerm syncLogReplMgrGetPrevLogTerm(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  SSyncRaftEntry* pEntry = NULL;
  SyncIndex       prevIndex = index - 1;
  SyncTerm        prevLogTerm = -1;
  terrno = TSDB_CODE_SUCCESS;

  if (prevIndex == -1 && pNode->pLogStore->syncLogBeginIndex(pNode->pLogStore) == 0) return 0;

  if (prevIndex > pBuf->matchIndex) {
    terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
    return -1;
  }

  tAssert(index - 1 == prevIndex);

  if (prevIndex >= pBuf->startIndex) {
    pEntry = pBuf->entries[(prevIndex + pBuf->size) % pBuf->size].pItem;
    tAssert(pEntry != NULL && "no log entry found");
    prevLogTerm = pEntry->term;
    return prevLogTerm;
  }

  if (pMgr && pMgr->startIndex <= prevIndex && prevIndex < pMgr->endIndex) {
    int64_t timeMs = pMgr->states[(prevIndex + pMgr->size) % pMgr->size].timeMs;
    tAssert(timeMs != 0 && "no log entry found");
    prevLogTerm = pMgr->states[(prevIndex + pMgr->size) % pMgr->size].term;
    tAssert(prevIndex == 0 || prevLogTerm != 0);
    return prevLogTerm;
  }

  SSnapshot snapshot;
  if (pNode->pFsm->FpGetSnapshotInfo(pNode->pFsm, &snapshot) == 0 && prevIndex == snapshot.lastApplyIndex) {
    return snapshot.lastApplyTerm;
  }

  if (pNode->pLogStore->syncLogGetEntry(pNode->pLogStore, prevIndex, &pEntry) == 0) {
    prevLogTerm = pEntry->term;
    syncEntryDestroy(pEntry);
    pEntry = NULL;
    return prevLogTerm;
  }

  sError("vgId:%d, failed to get log term since %s. index: %" PRId64 "", pNode->vgId, terrstr(), prevIndex);
  terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
  return -1;
}

SSyncRaftEntry* syncEntryBuildDummy(SyncTerm term, SyncIndex index, int32_t vgId) {
  return syncEntryBuildNoop(term, index, vgId);
}

int32_t syncLogValidateAlignmentOfCommit(SSyncNode* pNode, SyncIndex commitIndex) {
  SyncIndex firstVer = pNode->pLogStore->syncLogBeginIndex(pNode->pLogStore);
  if (firstVer > commitIndex + 1) {
    sError("vgId:%d, firstVer of WAL log greater than tsdb commit version + 1. firstVer: %" PRId64
           ", tsdb commit version: %" PRId64 "",
           pNode->vgId, firstVer, commitIndex);
    return -1;
  }

  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  if (lastVer < commitIndex) {
    sError("vgId:%d, lastVer of WAL log less than tsdb commit version. lastVer: %" PRId64
           ", tsdb commit version: %" PRId64 "",
           pNode->vgId, lastVer, commitIndex);
    return -1;
  }

  return 0;
}

int32_t syncLogBufferInitWithoutLock(SSyncLogBuffer* pBuf, SSyncNode* pNode) {
  tAssert(pNode->pLogStore != NULL && "log store not created");
  tAssert(pNode->pFsm != NULL && "pFsm not registered");
  tAssert(pNode->pFsm->FpGetSnapshotInfo != NULL && "FpGetSnapshotInfo not registered");

  SSnapshot snapshot;
  if (pNode->pFsm->FpGetSnapshotInfo(pNode->pFsm, &snapshot) < 0) {
    sError("vgId:%d, failed to get snapshot info since %s", pNode->vgId, terrstr());
    goto _err;
  }
  SyncIndex commitIndex = snapshot.lastApplyIndex;
  SyncTerm  commitTerm = TMAX(snapshot.lastApplyTerm, 0);
  if (syncLogValidateAlignmentOfCommit(pNode, commitIndex)) {
    terrno = TSDB_CODE_WAL_LOG_INCOMPLETE;
    goto _err;
  }

  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  tAssert(lastVer >= commitIndex);
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

  while (true) {
    if (index <= pBuf->commitIndex) {
      takeDummy = true;
      break;
    }

    if (pLogStore->syncLogGetEntry(pLogStore, index, &pEntry) < 0) {
      sError("vgId:%d, failed to get log entry since %s. index:%" PRId64 "", pNode->vgId, terrstr(), index);
      break;
    }

    bool taken = false;
    int  emptySize = 5;
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
    tAssert(index == pBuf->commitIndex);

    SSyncRaftEntry* pDummy = syncEntryBuildDummy(commitTerm, commitIndex, pNode->vgId);
    if (pDummy == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _err;
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

  sInfo("vgId:%d, init sync log buffer. buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")", pNode->vgId,
        pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);

  // validate
  syncLogBufferValidate(pBuf);
  return 0;

_err:
  return -1;
}

int32_t syncLogBufferInit(SSyncLogBuffer* pBuf, SSyncNode* pNode) {
  taosThreadMutexLock(&pBuf->mutex);
  int32_t ret = syncLogBufferInitWithoutLock(pBuf, pNode);
  taosThreadMutexUnlock(&pBuf->mutex);
  return ret;
}

int32_t syncLogBufferReInit(SSyncLogBuffer* pBuf, SSyncNode* pNode) {
  taosThreadMutexLock(&pBuf->mutex);
  for (SyncIndex index = pBuf->startIndex; index < pBuf->endIndex; index++) {
    SSyncRaftEntry* pEntry = pBuf->entries[(index + pBuf->size) % pBuf->size].pItem;
    if (pEntry == NULL) continue;
    syncEntryDestroy(pEntry);
    pEntry = NULL;
    memset(&pBuf->entries[(index + pBuf->size) % pBuf->size], 0, sizeof(pBuf->entries[0]));
  }
  pBuf->startIndex = pBuf->commitIndex = pBuf->matchIndex = pBuf->endIndex = 0;
  int32_t ret = syncLogBufferInitWithoutLock(pBuf, pNode);
  if (ret < 0) {
    sError("vgId:%d, failed to re-initialize sync log buffer since %s.", pNode->vgId, terrstr());
  }
  taosThreadMutexUnlock(&pBuf->mutex);
  return ret;
}

FORCE_INLINE SyncTerm syncLogBufferGetLastMatchTerm(SSyncLogBuffer* pBuf) {
  SyncIndex       index = pBuf->matchIndex;
  SSyncRaftEntry* pEntry = pBuf->entries[(index + pBuf->size) % pBuf->size].pItem;
  tAssert(pEntry != NULL);
  return pEntry->term;
}

int32_t syncLogBufferAccept(SSyncLogBuffer* pBuf, SSyncNode* pNode, SSyncRaftEntry* pEntry, SyncTerm prevTerm) {
  taosThreadMutexLock(&pBuf->mutex);
  syncLogBufferValidate(pBuf);
  int32_t   ret = -1;
  SyncIndex index = pEntry->index;
  SyncIndex prevIndex = pEntry->index - 1;
  SyncTerm  lastMatchTerm = syncLogBufferGetLastMatchTerm(pBuf);

  if (index <= pBuf->commitIndex) {
    sTrace("vgId:%d, already committed. index: %" PRId64 ", term: %" PRId64 ". log buffer: [%" PRId64 " %" PRId64
           " %" PRId64 ", %" PRId64 ")",
           pNode->vgId, pEntry->index, pEntry->term, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex,
           pBuf->endIndex);
    SyncTerm term = syncLogReplMgrGetPrevLogTerm(NULL, pNode, index + 1);
    tAssert(pEntry->term >= 0);
    if (term == pEntry->term) {
      ret = 0;
    }
    goto _out;
  }

  if (index - pBuf->startIndex >= pBuf->size) {
    sWarn("vgId:%d, out of buffer range. index: %" PRId64 ", term: %" PRId64 ". log buffer: [%" PRId64 " %" PRId64
          " %" PRId64 ", %" PRId64 ")",
          pNode->vgId, pEntry->index, pEntry->term, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex,
          pBuf->endIndex);
    goto _out;
  }

  if (index > pBuf->matchIndex && lastMatchTerm != prevTerm) {
    sWarn("vgId:%d, not ready to accept. index: %" PRId64 ", term: %" PRId64 ": prevterm: %" PRId64
          " != lastmatch: %" PRId64 ". log buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
          pNode->vgId, pEntry->index, pEntry->term, prevTerm, lastMatchTerm, pBuf->startIndex, pBuf->commitIndex,
          pBuf->matchIndex, pBuf->endIndex);
    goto _out;
  }

  // check current in buffer
  SSyncRaftEntry* pExist = pBuf->entries[index % pBuf->size].pItem;
  if (pExist != NULL) {
    tAssert(pEntry->index == pExist->index);

    if (pEntry->term != pExist->term) {
      (void)syncLogBufferRollback(pBuf, pNode, index);
    } else {
      sTrace("vgId:%d, duplicate log entry received. index: %" PRId64 ", term: %" PRId64 ". log buffer: [%" PRId64
             " %" PRId64 " %" PRId64 ", %" PRId64 ")",
             pNode->vgId, pEntry->index, pEntry->term, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex,
             pBuf->endIndex);
      SyncTerm existPrevTerm = pBuf->entries[index % pBuf->size].prevLogTerm;
      tAssert(pEntry->term == pExist->term && prevTerm == existPrevTerm);
      ret = 0;
      goto _out;
    }
  }

  // update
  SSyncLogBufEntry tmp = {.pItem = pEntry, .prevLogIndex = prevIndex, .prevLogTerm = prevTerm};
  pEntry = NULL;
  pBuf->entries[index % pBuf->size] = tmp;

  // update end index
  pBuf->endIndex = TMAX(index + 1, pBuf->endIndex);

  // success
  ret = 0;

_out:
  syncEntryDestroy(pEntry);
  syncLogBufferValidate(pBuf);
  taosThreadMutexUnlock(&pBuf->mutex);
  return ret;
}

int32_t syncLogStorePersist(SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry) {
  tAssert(pEntry->index >= 0);
  SyncIndex lastVer = pLogStore->syncLogLastIndex(pLogStore);
  if (lastVer >= pEntry->index && pLogStore->syncLogTruncate(pLogStore, pEntry->index) < 0) {
    sError("failed to truncate log store since %s. from index:%" PRId64 "", terrstr(), pEntry->index);
    return -1;
  }
  lastVer = pLogStore->syncLogLastIndex(pLogStore);
  tAssert(pEntry->index == lastVer + 1);

  if (pLogStore->syncLogAppendEntry(pLogStore, pEntry) < 0) {
    sError("failed to append sync log entry since %s. index:%" PRId64 ", term:%" PRId64 "", terrstr(), pEntry->index,
           pEntry->term);
    return -1;
  }

  lastVer = pLogStore->syncLogLastIndex(pLogStore);
  tAssert(pEntry->index == lastVer);
  return 0;
}

int64_t syncLogBufferProceed(SSyncLogBuffer* pBuf, SSyncNode* pNode, SyncTerm* pMatchTerm) {
  taosThreadMutexLock(&pBuf->mutex);
  syncLogBufferValidate(pBuf);

  SSyncLogStore* pLogStore = pNode->pLogStore;
  int64_t        matchIndex = pBuf->matchIndex;

  while (pBuf->matchIndex + 1 < pBuf->endIndex) {
    int64_t index = pBuf->matchIndex + 1;
    tAssert(index >= 0);

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

    tAssert(index == pEntry->index);

    // match
    SSyncRaftEntry* pMatch = pBuf->entries[(pBuf->matchIndex + pBuf->size) % pBuf->size].pItem;
    tAssert(pMatch != NULL);
    tAssert(pMatch->index == pBuf->matchIndex);
    tAssert(pMatch->index + 1 == pEntry->index);
    tAssert(prevLogIndex == pMatch->index);

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

    sTrace("vgId:%d, log buffer proceed. start index: %" PRId64 ", match index: %" PRId64 ", end index: %" PRId64,
           pNode->vgId, pBuf->startIndex, pBuf->matchIndex, pBuf->endIndex);

    // replicate on demand
    (void)syncNodeReplicateWithoutLock(pNode);

    // persist
    if (syncLogStorePersist(pLogStore, pEntry) < 0) {
      sError("vgId:%d, failed to persist sync log entry from buffer since %s. index:%" PRId64, pNode->vgId, terrstr(),
             pEntry->index);
      goto _out;
    }
    tAssert(pEntry->index == pBuf->matchIndex);

    // update my match index
    matchIndex = pBuf->matchIndex;
    syncIndexMgrSetIndex(pNode->pMatchIndex, &pNode->myRaftId, pBuf->matchIndex);
  }  // end of while

_out:
  pBuf->matchIndex = matchIndex;
  if (pMatchTerm) {
    *pMatchTerm = pBuf->entries[(matchIndex + pBuf->size) % pBuf->size].pItem->term;
  }
  syncLogBufferValidate(pBuf);
  taosThreadMutexUnlock(&pBuf->mutex);
  return matchIndex;
}

int32_t syncLogFsmExecute(SSyncNode* pNode, SSyncFSM* pFsm, ESyncState role, SyncTerm term, SSyncRaftEntry* pEntry) {
  tAssert(pFsm->FpCommitCb != NULL && "No commit cb registered for the FSM");

  if ((pNode->replicaNum == 1) && pNode->restoreFinish && pNode->vgId != 1) {
    return 0;
  }

  if (pNode->vgId != 1 && vnodeIsMsgBlock(pEntry->originalRpcType)) {
    sTrace("vgId:%d, blocking msg ready to execute. index:%" PRId64 ", term: %" PRId64 ", type: %s", pNode->vgId,
           pEntry->index, pEntry->term, TMSG_INFO(pEntry->originalRpcType));
  }

  SRpcMsg rpcMsg = {0};
  syncEntry2OriginalRpc(pEntry, &rpcMsg);

  SFsmCbMeta cbMeta = {0};
  cbMeta.index = pEntry->index;
  cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(pNode, pEntry->index);
  cbMeta.isWeak = pEntry->isWeak;
  cbMeta.code = 0;
  cbMeta.state = role;
  cbMeta.seqNum = pEntry->seqNum;
  cbMeta.term = pEntry->term;
  cbMeta.currentTerm = term;
  cbMeta.flag = -1;

  (void)syncRespMgrGetAndDel(pNode->pSyncRespMgr, cbMeta.seqNum, &rpcMsg.info);
  int32_t code = pFsm->FpCommitCb(pFsm, &rpcMsg, &cbMeta);
  tAssert(rpcMsg.pCont == NULL);
  return code;
}

int32_t syncLogBufferValidate(SSyncLogBuffer* pBuf) {
  tAssert(pBuf->startIndex <= pBuf->matchIndex);
  tAssert(pBuf->commitIndex <= pBuf->matchIndex);
  tAssert(pBuf->matchIndex < pBuf->endIndex);
  tAssert(pBuf->endIndex - pBuf->startIndex <= pBuf->size);
  tAssert(pBuf->entries[(pBuf->matchIndex + pBuf->size) % pBuf->size].pItem);
  return 0;
}

int32_t syncLogBufferCommit(SSyncLogBuffer* pBuf, SSyncNode* pNode, int64_t commitIndex) {
  taosThreadMutexLock(&pBuf->mutex);
  syncLogBufferValidate(pBuf);

  SSyncLogStore*  pLogStore = pNode->pLogStore;
  SSyncFSM*       pFsm = pNode->pFsm;
  ESyncState      role = pNode->state;
  SyncTerm        term = pNode->pRaftStore->currentTerm;
  SyncGroupId     vgId = pNode->vgId;
  int32_t         ret = -1;
  int64_t         upperIndex = TMIN(commitIndex, pBuf->matchIndex);
  SSyncRaftEntry* pEntry = NULL;
  bool            inBuf = false;

  if (commitIndex <= pBuf->commitIndex) {
    sDebug("vgId:%d, stale commit index. current:%" PRId64 ", notified:%" PRId64 "", vgId, pBuf->commitIndex,
           commitIndex);
    ret = 0;
    goto _out;
  }

  sTrace("vgId:%d, commit. log buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 "), role: %d, term: %" PRId64,
         pNode->vgId, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex, role, term);

  // execute in fsm
  for (int64_t index = pBuf->commitIndex + 1; index <= upperIndex; index++) {
    // get a log entry
    pEntry = syncLogBufferGetOneEntry(pBuf, pNode, index, &inBuf);
    if (pEntry == NULL) {
      goto _out;
    }

    // execute it
    if (!syncUtilUserCommit(pEntry->originalRpcType)) {
      sInfo("vgId:%d, commit sync barrier. index: %" PRId64 ", term:%" PRId64 ", type: %s", vgId, pEntry->index,
            pEntry->term, TMSG_INFO(pEntry->originalRpcType));
      pBuf->commitIndex = index;
      if (!inBuf) {
        syncEntryDestroy(pEntry);
        pEntry = NULL;
      }
      continue;
    }
    if (syncLogFsmExecute(pNode, pFsm, role, term, pEntry) != 0) {
      sError("vgId:%d, failed to execute sync log entry. index:%" PRId64 ", term:%" PRId64
             ", role: %d, current term: %" PRId64,
             vgId, pEntry->index, pEntry->term, role, term);
      goto _out;
    }
    pBuf->commitIndex = index;

    sTrace("vgId:%d, committed index: %" PRId64 ", term: %" PRId64 ", role: %d, current term: %" PRId64 "", pNode->vgId,
           pEntry->index, pEntry->term, role, term);

    if (!inBuf) {
      syncEntryDestroy(pEntry);
      pEntry = NULL;
    }
  }

  // recycle
  SyncIndex until = pBuf->commitIndex - (pBuf->size >> 4);
  for (SyncIndex index = pBuf->startIndex; index < until; index++) {
    SSyncRaftEntry* pEntry = pBuf->entries[(index + pBuf->size) % pBuf->size].pItem;
    tAssert(pEntry != NULL);
    syncEntryDestroy(pEntry);
    memset(&pBuf->entries[(index + pBuf->size) % pBuf->size], 0, sizeof(pBuf->entries[0]));
    pBuf->startIndex = index + 1;
  }

  ret = 0;
_out:
  // mark as restored if needed
  if (!pNode->restoreFinish && pBuf->commitIndex >= pNode->commitIndex) {
    pNode->pFsm->FpRestoreFinishCb(pNode->pFsm);
    pNode->restoreFinish = true;
    sInfo("vgId:%d, restore finished. log buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")", pNode->vgId,
          pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
  }

  if (!inBuf) {
    syncEntryDestroy(pEntry);
    pEntry = NULL;
  }
  syncLogBufferValidate(pBuf);
  taosThreadMutexUnlock(&pBuf->mutex);
  return ret;
}

int32_t syncLogReplMgrReset(SSyncLogReplMgr* pMgr) {
  tAssert(pMgr->startIndex >= 0);
  for (SyncIndex index = pMgr->startIndex; index < pMgr->endIndex; index++) {
    memset(&pMgr->states[index % pMgr->size], 0, sizeof(pMgr->states[0]));
  }
  pMgr->startIndex = 0;
  pMgr->matchIndex = 0;
  pMgr->endIndex = 0;
  pMgr->restored = false;
  pMgr->retryBackoff = 0;
  return 0;
}

int32_t syncLogReplMgrRetryOnNeed(SSyncLogReplMgr* pMgr, SSyncNode* pNode) {
  if (pMgr->endIndex <= pMgr->startIndex) {
    return 0;
  }

  SRaftId* pDestId = &pNode->replicasId[pMgr->peerId];
  if (pMgr->retryBackoff == SYNC_MAX_RETRY_BACKOFF) {
    syncLogReplMgrReset(pMgr);
    sWarn("vgId:%d, reset sync log repl mgr since retry backoff exceeding limit. peer: %" PRIx64, pNode->vgId,
          pDestId->addr);
    return -1;
  }

  int32_t ret = -1;
  bool    retried = false;
  int64_t retryWaitMs = syncLogGetRetryBackoffTimeMs(pMgr);
  int64_t  nowMs = taosGetMonoTimestampMs();
  int      count = 0;
  int64_t  firstIndex = -1;
  SyncTerm term = -1;

  for (SyncIndex index = pMgr->startIndex; index < pMgr->endIndex; index++) {
    int64_t pos = index % pMgr->size;
    tAssert(!pMgr->states[pos].barrier || (index == pMgr->startIndex || index + 1 == pMgr->endIndex));

    if (nowMs < pMgr->states[pos].timeMs + retryWaitMs) {
      break;
    }

    if (pMgr->states[pos].acked) {
      continue;
    }

    bool barrier = false;
    if (syncLogBufferReplicateOneTo(pMgr, pNode, index, &term, pDestId, &barrier) < 0) {
      sError("vgId:%d, failed to replicate sync log entry since %s. index: %" PRId64 ", dest: %" PRIx64 "", pNode->vgId,
             terrstr(), index, pDestId->addr);
      goto _out;
    }
    tAssert(barrier == pMgr->states[pos].barrier);
    pMgr->states[pos].timeMs = nowMs;
    pMgr->states[pos].term = term;
    pMgr->states[pos].acked = false;

    retried = true;
    if (firstIndex == -1) firstIndex = index;
    count++;
  }

  ret = 0;
_out:
  if (retried) {
    pMgr->retryBackoff = syncLogGetNextRetryBackoff(pMgr);
    sInfo("vgId:%d, resent %d sync log entries. dest: %" PRIx64 ", indexes: %" PRId64 " ..., terms: ... %" PRId64
          ", retryWaitMs: %" PRId64 ", repl mgr: [%" PRId64 " %" PRId64 ", %" PRId64 ")",
          pNode->vgId, count, pDestId->addr, firstIndex, term, retryWaitMs, pMgr->startIndex, pMgr->matchIndex,
          pMgr->endIndex);
  }
  return ret;
}

int32_t syncLogReplMgrProcessReplyInRecoveryMode(SSyncLogReplMgr* pMgr, SSyncNode* pNode,
                                                 SyncAppendEntriesReply* pMsg) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  SRaftId         destId = pMsg->srcId;
  tAssert(pMgr->restored == false);
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(destId.addr, host, sizeof(host), &port);

  if (pMgr->endIndex == 0) {
    tAssert(pMgr->startIndex == 0);
    tAssert(pMgr->matchIndex == 0);
    if (pMsg->matchIndex < 0) {
      pMgr->restored = true;
      sInfo("vgId:%d, sync log repl mgr restored. peer: %s:%d (%" PRIx64 "), mgr: rs(%d) [%" PRId64 " %" PRId64
            ", %" PRId64 "), buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
            pNode->vgId, host, port, destId.addr, pMgr->restored, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex,
            pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
      return 0;
    }
  } else {
    if (pMsg->lastSendIndex < pMgr->startIndex || pMsg->lastSendIndex >= pMgr->endIndex) {
      syncLogReplMgrRetryOnNeed(pMgr, pNode);
      return 0;
    }

    pMgr->states[pMsg->lastSendIndex % pMgr->size].acked = true;

    if (pMsg->success && pMsg->matchIndex == pMsg->lastSendIndex) {
      pMgr->matchIndex = pMsg->matchIndex;
      pMgr->restored = true;
      sInfo("vgId:%d, sync log repl mgr restored. peer: %s:%d (%" PRIx64 "), mgr: rs(%d) [%" PRId64 " %" PRId64
            ", %" PRId64 "), buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
            pNode->vgId, host, port, destId.addr, pMgr->restored, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex,
            pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
      return 0;
    }

    if (pMsg->success == false && pMsg->matchIndex >= pMsg->lastSendIndex) {
      sWarn("vgId:%d, failed to rollback match index. peer: %s:%d, match index: %" PRId64 ", last sent: %" PRId64,
            pNode->vgId, host, port, pMsg->matchIndex, pMsg->lastSendIndex);
      if (syncNodeStartSnapshot(pNode, &destId) < 0) {
        sError("vgId:%d, failed to start snapshot for peer %s:%d", pNode->vgId, host, port);
        return -1;
      }
      sInfo("vgId:%d, snapshot replication to peer %s:%d", pNode->vgId, host, port);
      return 0;
    }
  }

  // check last match term
  SyncTerm  term = -1;
  SyncIndex firstVer = pNode->pLogStore->syncLogBeginIndex(pNode->pLogStore);
  SyncIndex index = TMIN(pMsg->matchIndex, pNode->pLogBuf->matchIndex);

  if (pMsg->matchIndex < pNode->pLogBuf->matchIndex) {
    term = syncLogReplMgrGetPrevLogTerm(pMgr, pNode, index + 1);
    if (term < 0 || (term != pMsg->lastMatchTerm && (index + 1 == firstVer || index == firstVer))) {
      tAssert(term >= 0 || terrno == TSDB_CODE_WAL_LOG_NOT_EXIST);
      if (syncNodeStartSnapshot(pNode, &destId) < 0) {
        sError("vgId:%d, failed to start snapshot for peer %s:%d", pNode->vgId, host, port);
        return -1;
      }
      sInfo("vgId:%d, snapshot replication to peer %s:%d", pNode->vgId, host, port);
      return 0;
    }

    tAssert(index + 1 >= firstVer);

    if (term == pMsg->lastMatchTerm) {
      index = index + 1;
      tAssert(index <= pNode->pLogBuf->matchIndex);
    } else {
      tAssert(index > firstVer);
    }
  }

  // attempt to replicate the raft log at index
  (void)syncLogReplMgrReset(pMgr);
  return syncLogReplMgrReplicateProbeOnce(pMgr, pNode, index);
}

int32_t syncLogReplMgrProcessHeartbeatReply(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncHeartbeatReply* pMsg) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  taosThreadMutexLock(&pBuf->mutex);
  if (pMsg->startTime != 0 && pMsg->startTime != pMgr->peerStartTime) {
    sInfo("vgId:%d, reset sync log repl mgr in heartbeat. peer: %" PRIx64 ", start time:%" PRId64 ", old:%" PRId64 "",
          pNode->vgId, pMsg->srcId.addr, pMsg->startTime, pMgr->peerStartTime);
    syncLogReplMgrReset(pMgr);
    pMgr->peerStartTime = pMsg->startTime;
  }
  taosThreadMutexUnlock(&pBuf->mutex);
  return 0;
}

int32_t syncLogReplMgrProcessReply(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  taosThreadMutexLock(&pBuf->mutex);
  if (pMsg->startTime != pMgr->peerStartTime) {
    sInfo("vgId:%d, reset sync log repl mgr in appendlog reply. peer: %" PRIx64 ", start time:%" PRId64
          ", old:%" PRId64,
          pNode->vgId, pMsg->srcId.addr, pMsg->startTime, pMgr->peerStartTime);
    syncLogReplMgrReset(pMgr);
    pMgr->peerStartTime = pMsg->startTime;
  }

  if (pMgr->restored) {
    (void)syncLogReplMgrProcessReplyInNormalMode(pMgr, pNode, pMsg);
  } else {
    (void)syncLogReplMgrProcessReplyInRecoveryMode(pMgr, pNode, pMsg);
  }
  taosThreadMutexUnlock(&pBuf->mutex);
  return 0;
}

int32_t syncLogReplMgrReplicateOnce(SSyncLogReplMgr* pMgr, SSyncNode* pNode) {
  if (pMgr->restored) {
    (void)syncLogReplMgrReplicateAttemptedOnce(pMgr, pNode);
  } else {
    (void)syncLogReplMgrReplicateProbeOnce(pMgr, pNode, pNode->pLogBuf->matchIndex);
  }
  return 0;
}

int32_t syncLogReplMgrReplicateProbeOnce(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index) {
  tAssert(!pMgr->restored);
  tAssert(pMgr->startIndex >= 0);
  int64_t retryMaxWaitMs = SYNC_LOG_REPL_RETRY_WAIT_MS * (1 << SYNC_MAX_RETRY_BACKOFF);
  int64_t nowMs = taosGetMonoTimestampMs();

  if (pMgr->endIndex > pMgr->startIndex &&
      nowMs < pMgr->states[pMgr->startIndex % pMgr->size].timeMs + retryMaxWaitMs) {
    return 0;
  }
  (void)syncLogReplMgrReset(pMgr);

  SRaftId*  pDestId = &pNode->replicasId[pMgr->peerId];
  bool      barrier = false;
  SyncTerm  term = -1;
  if (syncLogBufferReplicateOneTo(pMgr, pNode, index, &term, pDestId, &barrier) < 0) {
    sError("vgId:%d, failed to replicate log entry since %s. index: %" PRId64 ", dest: 0x%016" PRIx64 "", pNode->vgId,
           terrstr(), index, pDestId->addr);
    return -1;
  }

  tAssert(index >= 0);
  pMgr->states[index % pMgr->size].barrier = barrier;
  pMgr->states[index % pMgr->size].timeMs = nowMs;
  pMgr->states[index % pMgr->size].term = term;
  pMgr->states[index % pMgr->size].acked = false;

  pMgr->startIndex = index;
  pMgr->endIndex = index + 1;

  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  sTrace("vgId:%d, attempted to probe the %d'th peer with msg of index:%" PRId64 " term: %" PRId64
         ". pMgr(rs:%d): [%" PRId64 " %" PRId64 ", %" PRId64 "), pBuf: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64
         ")",
         pNode->vgId, pMgr->peerId, index, term, pMgr->restored, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex,
         pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
  return 0;
}

int32_t syncLogReplMgrReplicateAttemptedOnce(SSyncLogReplMgr* pMgr, SSyncNode* pNode) {
  tAssert(pMgr->restored);
  SRaftId* pDestId = &pNode->replicasId[pMgr->peerId];
  int32_t  batchSize = TMAX(1, pMgr->size / 20);
  int32_t  count = 0;
  int64_t  nowMs = taosGetMonoTimestampMs();
  int64_t  limit = pMgr->size >> 1;

  for (SyncIndex index = pMgr->endIndex; index <= pNode->pLogBuf->matchIndex; index++) {
    if (batchSize < count++ || limit <= index - pMgr->startIndex) {
      break;
    }
    if (pMgr->startIndex + 1 < index && pMgr->states[(index - 1) % pMgr->size].barrier) {
      break;
    }
    int64_t  pos = index % pMgr->size;
    SRaftId* pDestId = &pNode->replicasId[pMgr->peerId];
    bool     barrier = false;
    SyncTerm term = -1;
    if (syncLogBufferReplicateOneTo(pMgr, pNode, index, &term, pDestId, &barrier) < 0) {
      sError("vgId:%d, failed to replicate log entry since %s. index: %" PRId64 ", dest: 0x%016" PRIx64 "", pNode->vgId,
             terrstr(), index, pDestId->addr);
      return -1;
    }
    pMgr->states[pos].barrier = barrier;
    pMgr->states[pos].timeMs = nowMs;
    pMgr->states[pos].term = term;
    pMgr->states[pos].acked = false;

    pMgr->endIndex = index + 1;
    if (barrier) {
      sInfo("vgId:%d, replicated sync barrier to dest: %" PRIx64 ". index: %" PRId64 ", term: %" PRId64
            ", repl mgr: rs(%d) [%" PRId64 " %" PRId64 ", %" PRId64 ")",
            pNode->vgId, pDestId->addr, index, term, pMgr->restored, pMgr->startIndex, pMgr->matchIndex,
            pMgr->endIndex);
      break;
    }
  }

  syncLogReplMgrRetryOnNeed(pMgr, pNode);

  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  sTrace("vgId:%d, attempted to replicate %d msgs to the %d'th peer. pMgr(rs:%d): [%" PRId64 " %" PRId64 ", %" PRId64
         "), pBuf: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
         pNode->vgId, count, pMgr->peerId, pMgr->restored, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex,
         pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
  return 0;
}

int32_t syncLogReplMgrProcessReplyInNormalMode(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg) {
  tAssert(pMgr->restored == true);
  if (pMgr->startIndex <= pMsg->lastSendIndex && pMsg->lastSendIndex < pMgr->endIndex) {
     if (pMgr->startIndex < pMgr->matchIndex && pMgr->retryBackoff > 0) {
        int64_t firstSentMs = pMgr->states[pMgr->startIndex % pMgr->size].timeMs;
        int64_t lastSentMs = pMgr->states[(pMgr->endIndex - 1) % pMgr->size].timeMs;
        int64_t timeDiffMs = lastSentMs - firstSentMs;
        if (timeDiffMs > 0 && timeDiffMs < (SYNC_LOG_REPL_RETRY_WAIT_MS << (pMgr->retryBackoff - 1))) {
            pMgr->retryBackoff -= 1;
        }
    }
    pMgr->states[pMsg->lastSendIndex % pMgr->size].acked = true;
    pMgr->matchIndex = TMAX(pMgr->matchIndex, pMsg->matchIndex);
    for (SyncIndex index = pMgr->startIndex; index < pMgr->matchIndex; index++) {
      memset(&pMgr->states[index % pMgr->size], 0, sizeof(pMgr->states[0]));
    }
    pMgr->startIndex = pMgr->matchIndex;
  }

  return syncLogReplMgrReplicateAttemptedOnce(pMgr, pNode);
}

SSyncLogReplMgr* syncLogReplMgrCreate() {
  SSyncLogReplMgr* pMgr = taosMemoryCalloc(1, sizeof(SSyncLogReplMgr));
  if (pMgr == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pMgr->size = sizeof(pMgr->states) / sizeof(pMgr->states[0]);

  tAssert(pMgr->size == TSDB_SYNC_LOG_BUFFER_SIZE);

  return pMgr;

_err:
  taosMemoryFree(pMgr);
  return NULL;
}

void syncLogReplMgrDestroy(SSyncLogReplMgr* pMgr) {
  if (pMgr == NULL) {
    return;
  }
  (void)taosMemoryFree(pMgr);
  return;
}

int32_t syncNodeLogReplMgrInit(SSyncNode* pNode) {
  for (int i = 0; i < TSDB_MAX_REPLICA; i++) {
    tAssert(pNode->logReplMgrs[i] == NULL);
    pNode->logReplMgrs[i] = syncLogReplMgrCreate();
    pNode->logReplMgrs[i]->peerId = i;
    tAssert(pNode->logReplMgrs[i] != NULL && "Out of memory.");
  }
  return 0;
}

void syncNodeLogReplMgrDestroy(SSyncNode* pNode) {
  for (int i = 0; i < TSDB_MAX_REPLICA; i++) {
    syncLogReplMgrDestroy(pNode->logReplMgrs[i]);
    pNode->logReplMgrs[i] = NULL;
  }
}

SSyncLogBuffer* syncLogBufferCreate() {
  SSyncLogBuffer* pBuf = taosMemoryCalloc(1, sizeof(SSyncLogBuffer));
  if (pBuf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pBuf->size = sizeof(pBuf->entries) / sizeof(pBuf->entries[0]);

  tAssert(pBuf->size == TSDB_SYNC_LOG_BUFFER_SIZE);

  if (taosThreadMutexAttrInit(&pBuf->attr) < 0) {
    sError("failed to init log buffer mutexattr due to %s", strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosThreadMutexAttrSetType(&pBuf->attr, PTHREAD_MUTEX_RECURSIVE) < 0) {
    sError("failed to set log buffer mutexattr type due to %s", strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  if (taosThreadMutexInit(&pBuf->mutex, &pBuf->attr) < 0) {
    sError("failed to init log buffer mutex due to %s", strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }

  return pBuf;

_err:
  taosMemoryFree(pBuf);
  return NULL;
}

void syncLogBufferClear(SSyncLogBuffer* pBuf) {
  taosThreadMutexLock(&pBuf->mutex);
  for (SyncIndex index = pBuf->startIndex; index < pBuf->endIndex; index++) {
    SSyncRaftEntry* pEntry = pBuf->entries[(index + pBuf->size) % pBuf->size].pItem;
    if (pEntry == NULL) continue;
    syncEntryDestroy(pEntry);
    pEntry = NULL;
    memset(&pBuf->entries[(index + pBuf->size) % pBuf->size], 0, sizeof(pBuf->entries[0]));
  }
  pBuf->startIndex = pBuf->commitIndex = pBuf->matchIndex = pBuf->endIndex = 0;
  taosThreadMutexUnlock(&pBuf->mutex);
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
  tAssert(pBuf->commitIndex < toIndex && toIndex <= pBuf->endIndex);

  sInfo("vgId:%d, rollback sync log buffer. toindex: %" PRId64 ", buffer: [%" PRId64 " %" PRId64 " %" PRId64
        ", %" PRId64 ")",
        pNode->vgId, toIndex, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);

  // trunc buffer
  SyncIndex index = pBuf->endIndex - 1;
  while (index >= toIndex) {
    SSyncRaftEntry* pEntry = pBuf->entries[index % pBuf->size].pItem;
    if (pEntry != NULL) {
      (void)syncEntryDestroy(pEntry);
      pEntry = NULL;
      memset(&pBuf->entries[index % pBuf->size], 0, sizeof(pBuf->entries[0]));
    }
    index--;
  }
  pBuf->endIndex = toIndex;
  pBuf->matchIndex = TMIN(pBuf->matchIndex, index);
  tAssert(index + 1 == toIndex);

  // trunc wal
  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  if (lastVer >= toIndex && pNode->pLogStore->syncLogTruncate(pNode->pLogStore, toIndex) < 0) {
    sError("vgId:%d, failed to truncate log store since %s. from index:%" PRId64 "", pNode->vgId, terrstr(), toIndex);
    return -1;
  }
  lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  tAssert(toIndex == lastVer + 1);

  syncLogBufferValidate(pBuf);
  return 0;
}

int32_t syncLogBufferReset(SSyncLogBuffer* pBuf, SSyncNode* pNode) {
  taosThreadMutexLock(&pBuf->mutex);
  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  tAssert(lastVer == pBuf->matchIndex);
  SyncIndex index = pBuf->endIndex - 1;

  (void)syncLogBufferRollback(pBuf, pNode, pBuf->matchIndex + 1);

  sInfo("vgId:%d, reset sync log buffer. buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")", pNode->vgId,
        pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);

  pBuf->endIndex = pBuf->matchIndex + 1;

  // reset repl mgr
  for (int i = 0; i < pNode->replicaNum; i++) {
    SSyncLogReplMgr* pMgr = pNode->logReplMgrs[i];
    syncLogReplMgrReset(pMgr);
  }
  taosThreadMutexUnlock(&pBuf->mutex);
  return 0;
}

SSyncRaftEntry* syncLogBufferGetOneEntry(SSyncLogBuffer* pBuf, SSyncNode* pNode, SyncIndex index, bool* pInBuf) {
  SSyncRaftEntry* pEntry = NULL;
  if (index >= pBuf->endIndex) {
    return NULL;
  }
  if (index > pBuf->startIndex) {  // startIndex might be dummy
    *pInBuf = true;
    pEntry = pBuf->entries[index % pBuf->size].pItem;
  } else {
    *pInBuf = false;
    if (pNode->pLogStore->syncLogGetEntry(pNode->pLogStore, index, &pEntry) < 0) {
      sError("vgId:%d, failed to get log entry since %s. index:%" PRId64 "", pNode->vgId, terrstr(), index);
    }
  }
  return pEntry;
}

int32_t syncLogBufferReplicateOneTo(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index, SyncTerm* pTerm,
                                    SRaftId* pDestId, bool* pBarrier) {
  SSyncRaftEntry* pEntry = NULL;
  SRpcMsg         msgOut = {0};
  bool            inBuf = false;
  int32_t         ret = -1;
  SyncTerm        prevLogTerm = -1;
  SSyncLogBuffer* pBuf = pNode->pLogBuf;

  pEntry = syncLogBufferGetOneEntry(pBuf, pNode, index, &inBuf);
  if (pEntry == NULL) {
    sError("vgId:%d, failed to get raft entry for index: %" PRId64 "", pNode->vgId, index);
    if (terrno == TSDB_CODE_WAL_LOG_NOT_EXIST) {
      SSyncLogReplMgr* pMgr = syncNodeGetLogReplMgr(pNode, pDestId);
      if (pMgr) {
        sInfo("vgId:%d, reset sync log repl mgr of peer: %" PRIx64 " since %s. index: %" PRId64, pNode->vgId,
              pDestId->addr, terrstr(), index);
        (void)syncLogReplMgrReset(pMgr);
      }
    }
    goto _err;
  }
  *pBarrier = syncLogIsReplicationBarrier(pEntry);

  prevLogTerm = syncLogReplMgrGetPrevLogTerm(pMgr, pNode, index);
  if (prevLogTerm < 0) {
    sError("vgId:%d, failed to get prev log term since %s. index: %" PRId64 "", pNode->vgId, terrstr(), index);
    goto _err;
  }
  if (pTerm) *pTerm = pEntry->term;

  int32_t code = syncBuildAppendEntriesFromRaftLog(pNode, pEntry, prevLogTerm, &msgOut);
  if (code < 0) {
    sError("vgId:%d, failed to get append entries for index:%" PRId64 "", pNode->vgId, index);
    goto _err;
  }

  (void)syncNodeSendAppendEntries(pNode, pDestId, &msgOut);
  ret = 0;

  sTrace("vgId:%d, replicate one msg index: %" PRId64 " term: %" PRId64 " prevterm: %" PRId64 " to dest: 0x%016" PRIx64,
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
  return -1;
}
