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
    sError("vgId:%d, failed to append due to log buffer full. index:%" PRId64 "", pNode->vgId, index);
    goto _out;
  }

  ASSERT(index == pBuf->endIndex);

  SSyncRaftEntry* pExist = pBuf->entries[index % pBuf->size].pItem;
  ASSERT(pExist == NULL);

  // initial log buffer with at least one item, e.g. commitIndex
  SSyncRaftEntry* pMatch = pBuf->entries[(index - 1 + pBuf->size) % pBuf->size].pItem;
  ASSERT(pMatch != NULL && "no matched raft log entry");
  ASSERT(pMatch->index + 1 == index);

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

  if (prevIndex == -1) return 0;

  if (index - 1 > pBuf->matchIndex) {
    terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
    return -1;
  }

  ASSERT(index - 1 == prevIndex);

  if (index - 1 >= pBuf->startIndex) {
    pEntry = pBuf->entries[(index + pBuf->size) % pBuf->size].pItem;
    ASSERT(pEntry != NULL && "no log entry found");
    prevLogTerm = pBuf->entries[(index + pBuf->size) % pBuf->size].prevLogTerm;
    return prevLogTerm;
  }

  if (pMgr->startIndex <= prevIndex && prevIndex < pMgr->endIndex) {
    int64_t timeMs = pMgr->states[(prevIndex + pMgr->size) % pMgr->size].timeMs;
    ASSERT(timeMs != 0 && "no log entry found");
    prevLogTerm = pMgr->states[(prevIndex + pMgr->size) % pMgr->size].term;
    ASSERT(prevIndex == 0 || prevLogTerm != 0);
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

int32_t syncLogBufferInit(SSyncLogBuffer* pBuf, SSyncNode* pNode) {
  taosThreadMutexLock(&pBuf->mutex);
  ASSERT(pNode->pLogStore != NULL && "log store not created");
  ASSERT(pNode->pFsm != NULL && "pFsm not registered");
  ASSERT(pNode->pFsm->FpGetSnapshotInfo != NULL && "FpGetSnapshotInfo not registered");

  SSnapshot snapshot;
  if (pNode->pFsm->FpGetSnapshotInfo(pNode->pFsm, &snapshot) < 0) {
    sError("vgId:%d, failed to get snapshot info since %s", pNode->vgId, terrstr());
    goto _err;
  }
  SyncIndex commitIndex = snapshot.lastApplyIndex;
  SyncTerm  commitTerm = snapshot.lastApplyTerm;

  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  if (lastVer < commitIndex) {
    sError("vgId:%d, lastVer of WAL log less than tsdb commit version. lastVer: %" PRId64
           ", tsdb commit version: %" PRId64 "",
           pNode->vgId, lastVer, commitIndex);
    terrno = TSDB_CODE_WAL_LOG_INCOMPLETE;
    goto _err;
  }

  ASSERT(lastVer >= commitIndex);
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
    if (toIndex <= index + pBuf->size - 1) {
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
    ASSERT(index == pBuf->commitIndex);

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

  // validate
  syncLogBufferValidate(pBuf);
  taosThreadMutexUnlock(&pBuf->mutex);
  return 0;

_err:
  taosThreadMutexUnlock(&pBuf->mutex);
  return -1;
}

FORCE_INLINE SyncTerm syncLogBufferGetLastMatchTerm(SSyncLogBuffer* pBuf) {
  SyncIndex       index = pBuf->matchIndex;
  SSyncRaftEntry* pEntry = pBuf->entries[(index + pBuf->size) % pBuf->size].pItem;
  ASSERT(pEntry != NULL);
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
    sInfo("vgId:%d, raft entry already committed. index: %" PRId64 ", term: %" PRId64 ". log buffer: [%" PRId64
          " %" PRId64 " %" PRId64 ", %" PRId64 ")",
          pNode->vgId, pEntry->index, pEntry->term, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex,
          pBuf->endIndex);
    ret = 0;
    goto _out;
  }

  if (index - pBuf->startIndex >= pBuf->size) {
    sInfo("vgId:%d, raft entry out of buffer capacity. index: %" PRId64 ", term: %" PRId64 ". log buffer: [%" PRId64
          " %" PRId64 " %" PRId64 ", %" PRId64 ")",
          pNode->vgId, pEntry->index, pEntry->term, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex,
          pBuf->endIndex);
    goto _out;
  }

  if (index > pBuf->matchIndex && lastMatchTerm != prevTerm) {
    sInfo("vgId:%d, not ready to accept raft entry. index: %" PRId64 ", term: %" PRId64 ": prevterm: %" PRId64
          " != lastmatch: %" PRId64 ". log buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
          pNode->vgId, pEntry->index, pEntry->term, prevTerm, lastMatchTerm, pBuf->startIndex, pBuf->commitIndex,
          pBuf->matchIndex, pBuf->endIndex);
    goto _out;
  }

  // check current in buffer
  SSyncRaftEntry* pExist = pBuf->entries[index % pBuf->size].pItem;
  if (pExist != NULL) {
    ASSERT(pEntry->index == pExist->index);

    if (pEntry->term != pExist->term) {
      (void)syncLogBufferRollback(pBuf, index);
    } else {
      sDebug("vgId:%d, duplicate raft entry received. index: %" PRId64 ", term: %" PRId64 ". log buffer: [%" PRId64
             " %" PRId64 " %" PRId64 ", %" PRId64 ")",
             pNode->vgId, pEntry->index, pEntry->term, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex,
             pBuf->endIndex);
      SyncTerm existPrevTerm = pBuf->entries[index % pBuf->size].prevLogTerm;
      ASSERT(pEntry->term == pExist->term && prevTerm == existPrevTerm);
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
  ASSERT(pEntry->index >= 0);
  SyncIndex lastVer = pLogStore->syncLogLastIndex(pLogStore);
  if (lastVer >= pEntry->index && pLogStore->syncLogTruncate(pLogStore, pEntry->index) < 0) {
    sError("failed to truncate log store since %s. from index:%" PRId64 "", terrstr(), pEntry->index);
    return -1;
  }
  lastVer = pLogStore->syncLogLastIndex(pLogStore);
  ASSERT(pEntry->index == lastVer + 1);

  if (pLogStore->syncLogAppendEntry(pLogStore, pEntry) < 0) {
    sError("failed to append raft log entry since %s. index:%" PRId64 ", term:%" PRId64 "", terrstr(), pEntry->index,
           pEntry->term);
    return -1;
  }

  lastVer = pLogStore->syncLogLastIndex(pLogStore);
  ASSERT(pEntry->index == lastVer);
  return 0;
}

int64_t syncLogBufferProceed(SSyncLogBuffer* pBuf, SSyncNode* pNode) {
  taosThreadMutexLock(&pBuf->mutex);
  syncLogBufferValidate(pBuf);

  SSyncLogStore* pLogStore = pNode->pLogStore;
  int64_t        matchIndex = pBuf->matchIndex;

  while (pBuf->matchIndex + 1 < pBuf->endIndex) {
    int64_t index = pBuf->matchIndex + 1;
    ASSERT(index >= 0);

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

    ASSERT(index == pEntry->index);

    // match
    SSyncRaftEntry* pMatch = pBuf->entries[(pBuf->matchIndex + pBuf->size) % pBuf->size].pItem;
    ASSERT(pMatch != NULL);
    ASSERT(pMatch->index == pBuf->matchIndex);
    ASSERT(pMatch->index + 1 == pEntry->index);
    ASSERT(prevLogIndex == pMatch->index);

    if (pMatch->term != prevLogTerm) {
      sInfo(
          "vgId:%d, mismatching raft log entries encountered. "
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
    (void)syncNodeReplicate(pNode);

    // persist
    if (syncLogStorePersist(pLogStore, pEntry) < 0) {
      sError("vgId:%d, failed to persist raft log entry from log buffer since %s. index:%" PRId64, pNode->vgId,
             terrstr(), pEntry->index);
      goto _out;
    }
    ASSERT(pEntry->index == pBuf->matchIndex);

    // update my match index
    matchIndex = pBuf->matchIndex;
    syncIndexMgrSetIndex(pNode->pMatchIndex, &pNode->myRaftId, pBuf->matchIndex);
  }  // end of while

_out:
  pBuf->matchIndex = matchIndex;
  syncLogBufferValidate(pBuf);
  taosThreadMutexUnlock(&pBuf->mutex);
  return matchIndex;
}

int32_t syncLogFsmExecute(SSyncNode* pNode, SSyncFSM* pFsm, ESyncState role, SyncTerm term, SSyncRaftEntry* pEntry) {
  ASSERT(pFsm->FpCommitCb != NULL && "No commit cb registered for the FSM");

  if ((pNode->replicaNum == 1) && pNode->restoreFinish && pNode->vgId != 1) {
    return 0;
  }

  SRpcMsg rpcMsg;
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
  pFsm->FpCommitCb(pFsm, &rpcMsg, &cbMeta);
  return 0;
}

int32_t syncLogBufferValidate(SSyncLogBuffer* pBuf) {
  ASSERT(pBuf->startIndex <= pBuf->matchIndex);
  ASSERT(pBuf->commitIndex <= pBuf->matchIndex);
  ASSERT(pBuf->matchIndex < pBuf->endIndex);
  ASSERT(pBuf->endIndex - pBuf->startIndex <= pBuf->size);
  ASSERT(pBuf->entries[(pBuf->matchIndex + pBuf->size) % pBuf->size].pItem);
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
    sDebug("vgId:%d, stale commit update. current:%" PRId64 ", notified:%" PRId64 "", vgId, pBuf->commitIndex,
           commitIndex);
    ret = 0;
    goto _out;
  }

  sTrace("vgId:%d, log buffer info. role: %d, term: %" PRId64 ". start index:%" PRId64 ", commit index:%" PRId64
         ", match index: %" PRId64 ", end index:%" PRId64 "",
         pNode->vgId, role, term, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);

  // execute in fsm
  for (int64_t index = pBuf->commitIndex + 1; index <= upperIndex; index++) {
    // get a log entry
    pEntry = syncLogBufferGetOneEntry(pBuf, pNode, index, &inBuf);
    if (pEntry == NULL) {
      goto _out;
    }

    // execute it
    if (!syncUtilUserCommit(pEntry->originalRpcType)) {
      sInfo("vgId:%d, non-user msg in raft log entry. index: %" PRId64 ", term:%" PRId64 "", vgId, pEntry->index,
            pEntry->term);
      pBuf->commitIndex = index;
      if (!inBuf) {
        syncEntryDestroy(pEntry);
        pEntry = NULL;
      }
      continue;
    }

    if (syncLogFsmExecute(pNode, pFsm, role, term, pEntry) != 0) {
      sError("vgId:%d, failed to execute raft entry in FSM. log index:%" PRId64 ", term:%" PRId64 "", vgId,
             pEntry->index, pEntry->term);
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
    ASSERT(pEntry != NULL);
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
    sInfo("vgId:%d, restore finished. pBuf: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")", pNode->vgId,
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
  ASSERT(pMgr->startIndex >= 0);
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

_Atomic int64_t tsRetryCnt = 0;

int32_t syncLogReplMgrRetryOnNeed(SSyncLogReplMgr* pMgr, SSyncNode* pNode) {
  if (pMgr->endIndex <= pMgr->startIndex) {
    return 0;
  }

  int32_t ret = -1;
  bool    retried = false;
  int64_t retryWaitMs = syncLogGetRetryBackoffTimeMs(pMgr);

  for (SyncIndex index = pMgr->startIndex; index < pMgr->endIndex; index++) {
    int64_t pos = index % pMgr->size;
    ASSERT(!pMgr->states[pos].barrier || (index == pMgr->startIndex || index + 1 == pMgr->endIndex));
    if (pMgr->states[pos].acked) {
      continue;
    }
    int64_t nowMs = taosGetMonoTimestampMs();
    if (nowMs < pMgr->states[pos].timeMs + retryWaitMs) {
      break;
    }

    SRaftId* pDestId = &pNode->replicasId[pMgr->peerId];
    bool     barrier = false;
    SyncTerm term = -1;
    if (syncLogBufferReplicateOneTo(pMgr, pNode, index, &term, pDestId, &barrier) < 0) {
      sError("vgId:%d, failed to replicate log entry since %s. index: %" PRId64 ", dest: 0x%016" PRIx64 "", pNode->vgId,
             terrstr(), index, pDestId->addr);
      goto _out;
    }
    ASSERT(barrier == pMgr->states[pos].barrier);
    pMgr->states[pos].timeMs = nowMs;
    pMgr->states[pos].term = term;
    pMgr->states[pos].acked = false;
    retried = true;
    tsRetryCnt++;
  }

  ret = 0;
_out:
  if (retried) {
    pMgr->retryBackoff = syncLogGetNextRetryBackoff(pMgr);
  }
  return ret;
}

int32_t syncLogReplMgrProcessReplyInRecoveryMode(SSyncLogReplMgr* pMgr, SSyncNode* pNode,
                                                 SyncAppendEntriesReply* pMsg) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  SRaftId         destId = pMsg->srcId;
  ASSERT(pMgr->restored == false);
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);

  if (pMgr->endIndex == 0) {
    ASSERT(pMgr->startIndex == 0);
    ASSERT(pMgr->matchIndex == 0);
    if (pMsg->matchIndex < 0) {
      pMgr->restored = true;
      sInfo("vgId:%d, sync log repl mgr of peer %s:%d restored. pMgr(rs:%d): [%" PRId64 " %" PRId64 ", %" PRId64
            "), pBuf: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
            pNode->vgId, host, port, pMgr->restored, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex,
            pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
      return 0;
    }
  } else {
    if (pMsg->lastSendIndex < pMgr->startIndex || pMsg->lastSendIndex >= pMgr->endIndex) {
      syncLogReplMgrRetryOnNeed(pMgr, pNode);
      return 0;
    }

    pMgr->states[pMsg->lastSendIndex % pMgr->size].acked = true;

    if (pMsg->matchIndex == pMsg->lastSendIndex) {
      pMgr->restored = true;
      sInfo("vgId:%d, sync log repl mgr of peer %s:%d restored. pMgr(rs:%d): [%" PRId64 " %" PRId64 ", %" PRId64
            "), pBuf: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
            pNode->vgId, host, port, pMgr->restored, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex,
            pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
      return 0;
    }

    (void)syncLogReplMgrReset(pMgr);
  }

  // send match index
  SyncIndex index = TMIN(pMsg->matchIndex, pNode->pLogBuf->matchIndex);
  bool      barrier = false;
  SyncTerm  term = -1;
  ASSERT(index >= 0);
  if (syncLogBufferReplicateOneTo(pMgr, pNode, index, &term, &destId, &barrier) < 0) {
    sError("vgId:%d, failed to replicate log entry since %s. index: %" PRId64 ", dest: 0x%016" PRIx64 "", pNode->vgId,
           terrstr(), index, destId.addr);
    return -1;
  }

  int64_t nowMs = taosGetMonoTimestampMs();
  pMgr->states[index % pMgr->size].barrier = barrier;
  pMgr->states[index % pMgr->size].timeMs = nowMs;
  pMgr->states[index % pMgr->size].term = term;
  pMgr->states[index % pMgr->size].acked = false;

  pMgr->matchIndex = index;
  pMgr->startIndex = index;
  pMgr->endIndex = index + 1;
  return 0;
}

int32_t syncLogReplMgrProcessHeartbeatReply(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncHeartbeatReply* pMsg) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  taosThreadMutexLock(&pBuf->mutex);
  if (pMsg->startTime != 0 && pMsg->startTime != pMgr->peerStartTime) {
    sInfo("vgId:%d, reset sync log repl mgr in heartbeat. start time:%" PRId64 ", old start time:%" PRId64 "",
          pNode->vgId, pMsg->startTime, pMgr->peerStartTime);
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
    sInfo("vgId:%d, reset sync log repl mgr in append entries reply. start time:%" PRId64 ", old start time:%" PRId64
          "",
          pNode->vgId, pMsg->startTime, pMgr->peerStartTime);
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

int32_t syncLogBufferReplicateOnce(SSyncLogReplMgr* pMgr, SSyncNode* pNode) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  if (pMgr->restored) {
    (void)syncLogReplMgrReplicateAttemptedOnce(pMgr, pNode);
  } else {
    (void)syncLogReplMgrReplicateProbeOnce(pMgr, pNode);
  }
  return 0;
}

int32_t syncLogReplMgrReplicateProbeOnce(SSyncLogReplMgr* pMgr, SSyncNode* pNode) {
  ASSERT(!pMgr->restored);
  SyncIndex index = pNode->pLogBuf->matchIndex;
  SRaftId*  pDestId = &pNode->replicasId[pMgr->peerId];
  bool      barrier = false;
  SyncTerm  term = -1;
  if (syncLogBufferReplicateOneTo(pMgr, pNode, index, &term, pDestId, &barrier) < 0) {
    sError("vgId:%d, failed to replicate log entry since %s. index: %" PRId64 ", dest: 0x%016" PRIx64 "", pNode->vgId,
           terrstr(), index, pDestId->addr);
    return -1;
  }

  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  sTrace("vgId:%d, attempted to probe the %d'th peer with msg of index:%" PRId64 " term: %" PRId64
         ". pMgr(rs:%d): [%" PRId64 " %" PRId64 ", %" PRId64 "), pBuf: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64
         ")",
         pNode->vgId, pMgr->peerId, index, term, pMgr->restored, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex,
         pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
  return 0;
}

_Atomic int64_t tsSendCnt = 0;

int32_t syncLogReplMgrReplicateAttemptedOnce(SSyncLogReplMgr* pMgr, SSyncNode* pNode) {
  ASSERT(pMgr->restored);
  SRaftId* pDestId = &pNode->replicasId[pMgr->peerId];
  int32_t  batchSize = TMAX(1, pMgr->size / 20);
  int32_t  count = 0;
  int64_t  nowMs = taosGetMonoTimestampMs();

  for (SyncIndex index = pMgr->endIndex; index <= pNode->pLogBuf->matchIndex; index++) {
    if (batchSize < count++ || pMgr->startIndex + pMgr->size <= index) {
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
    tsSendCnt++;
    if (barrier) {
      break;
    }
  }

  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  sTrace("vgId:%d, attempted to replicate %d msgs to the %d'th peer. pMgr(rs:%d): [%" PRId64 " %" PRId64 ", %" PRId64
         "), pBuf: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
         pNode->vgId, count, pMgr->peerId, pMgr->restored, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex,
         pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
  syncLogReplMgrRetryOnNeed(pMgr, pNode);
  return 0;
}

int32_t syncLogReplMgrProcessReplyInNormalMode(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg) {
  ASSERT(pMgr->restored == true);
  if (pMgr->startIndex <= pMsg->lastSendIndex && pMsg->lastSendIndex < pMgr->endIndex) {
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

  ASSERT(pMgr->size == TSDB_SYNC_LOG_BUFFER_SIZE);

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
    ASSERT(pNode->logReplMgrs[i] == NULL);
    pNode->logReplMgrs[i] = syncLogReplMgrCreate();
    pNode->logReplMgrs[i]->peerId = i;
    ASSERT(pNode->logReplMgrs[i] != NULL && "Out of memory.");
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

  ASSERT(pBuf->size == TSDB_SYNC_LOG_BUFFER_SIZE);

  if (taosThreadMutexInit(&pBuf->mutex, NULL) < 0) {
    sError("failed to init log buffer mutex due to %s", strerror(errno));
    terrno = TAOS_SYSTEM_ERROR(errno);
    goto _err;
  }
  return pBuf;

_err:
  taosMemoryFree(pBuf);
  return NULL;
}

void syncLogBufferDestroy(SSyncLogBuffer* pBuf) {
  if (pBuf == NULL) {
    return;
  }
  (void)taosThreadMutexDestroy(&pBuf->mutex);
  (void)taosMemoryFree(pBuf);
  return;
}

int32_t syncLogBufferRollback(SSyncLogBuffer* pBuf, SyncIndex toIndex) {
  ASSERT(pBuf->commitIndex < toIndex && toIndex <= pBuf->endIndex);

  SyncIndex index = pBuf->endIndex - 1;
  while (index >= toIndex) {
    SSyncRaftEntry* pEntry = pBuf->entries[index % pBuf->size].pItem;
    if (pEntry != NULL) {
      syncEntryDestroy(pEntry);
      pEntry = NULL;
      memset(&pBuf->entries[index % pBuf->size], 0, sizeof(pBuf->entries[0]));
    }
    index--;
  }
  pBuf->endIndex = toIndex;
  pBuf->matchIndex = TMIN(pBuf->matchIndex, index);
  ASSERT(index + 1 == toIndex);
  return 0;
}

int32_t syncLogBufferReset(SSyncLogBuffer* pBuf, SSyncNode* pNode) {
  taosThreadMutexLock(&pBuf->mutex);
  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  ASSERT(lastVer == pBuf->matchIndex);
  SyncIndex index = pBuf->endIndex - 1;

  (void)syncLogBufferRollback(pBuf, pBuf->matchIndex + 1);

  sInfo("vgId:%d, reset log buffer. pBuf: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")", pNode->vgId,
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
