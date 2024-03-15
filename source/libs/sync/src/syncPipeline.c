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
#include "syncRaftCfg.h"
#include "syncVoteMgr.h"

static bool syncIsMsgBlock(tmsg_t type) {
  return (type == TDMT_VND_CREATE_TABLE) || (type == TDMT_VND_ALTER_TABLE) || (type == TDMT_VND_DROP_TABLE) ||
         (type == TDMT_VND_UPDATE_TAG_VAL) || (type == TDMT_VND_ALTER_CONFIRM);
}

FORCE_INLINE static int64_t syncGetRetryMaxWaitMs() {
  return SYNC_LOG_REPL_RETRY_WAIT_MS * (1 << SYNC_MAX_RETRY_BACKOFF);
}

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
    terrno = TSDB_CODE_SYN_BUFFER_FULL;
    sError("vgId:%d, failed to append since %s. index:%" PRId64 "", pNode->vgId, terrstr(), index);
    goto _err;
  }

  if (pNode->restoreFinish && index - pBuf->commitIndex >= TSDB_SYNC_NEGOTIATION_WIN) {
    terrno = TSDB_CODE_SYN_NEGOTIATION_WIN_FULL;
    sError("vgId:%d, failed to append since %s, index:%" PRId64 ", commit-index:%" PRId64, pNode->vgId, terrstr(),
           index, pBuf->commitIndex);
    goto _err;
  }

  SyncIndex appliedIndex = pNode->pFsm->FpAppliedIndexCb(pNode->pFsm);
  if (pNode->restoreFinish && pBuf->commitIndex - appliedIndex >= TSDB_SYNC_APPLYQ_SIZE_LIMIT) {
    terrno = TSDB_CODE_SYN_WRITE_STALL;
    sError("vgId:%d, failed to append since %s. index:%" PRId64 ", commit-index:%" PRId64 ", applied-index:%" PRId64,
           pNode->vgId, terrstr(), index, pBuf->commitIndex, appliedIndex);
    goto _err;
  }

  ASSERT(index == pBuf->endIndex);

  SSyncRaftEntry* pExist = pBuf->entries[index % pBuf->size].pItem;
  ASSERT(pExist == NULL);

  // initial log buffer with at least one item, e.g. commitIndex
  SSyncRaftEntry* pMatch = pBuf->entries[(index - 1 + pBuf->size) % pBuf->size].pItem;
  ASSERTS(pMatch != NULL, "no matched log entry");
  ASSERT(pMatch->index + 1 == index);
  ASSERT(pMatch->term <= pEntry->term);

  SSyncLogBufEntry tmp = {.pItem = pEntry, .prevLogIndex = pMatch->index, .prevLogTerm = pMatch->term};
  pBuf->entries[index % pBuf->size] = tmp;
  pBuf->endIndex = index + 1;

  syncLogBufferValidate(pBuf);
  taosThreadMutexUnlock(&pBuf->mutex);
  return 0;

_err:
  syncLogBufferValidate(pBuf);
  taosThreadMutexUnlock(&pBuf->mutex);
  taosMsleep(1);
  return -1;
}

SyncTerm syncLogReplGetPrevLogTerm(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index) {
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

  ASSERT(index - 1 == prevIndex);

  if (prevIndex >= pBuf->startIndex) {
    pEntry = pBuf->entries[(prevIndex + pBuf->size) % pBuf->size].pItem;
    ASSERTS(pEntry != NULL, "no log entry found");
    prevLogTerm = pEntry->term;
    return prevLogTerm;
  }

  if (pMgr && pMgr->startIndex <= prevIndex && prevIndex < pMgr->endIndex) {
    int64_t timeMs = pMgr->states[(prevIndex + pMgr->size) % pMgr->size].timeMs;
    ASSERTS(timeMs != 0, "no log entry found");
    prevLogTerm = pMgr->states[(prevIndex + pMgr->size) % pMgr->size].term;
    ASSERT(prevIndex == 0 || prevLogTerm != 0);
    return prevLogTerm;
  }

  SSnapshot snapshot = {0};
  pNode->pFsm->FpGetSnapshotInfo(pNode->pFsm, &snapshot);
  if (prevIndex == snapshot.lastApplyIndex) {
    return snapshot.lastApplyTerm;
  }

  if (pNode->pLogStore->syncLogGetEntry(pNode->pLogStore, prevIndex, &pEntry) == 0) {
    prevLogTerm = pEntry->term;
    syncEntryDestroy(pEntry);
    pEntry = NULL;
    return prevLogTerm;
  }

  sInfo("vgId:%d, failed to get log term since %s. index:%" PRId64, pNode->vgId, terrstr(), prevIndex);
  terrno = TSDB_CODE_WAL_LOG_NOT_EXIST;
  return -1;
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
    return -1;
  }

  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  if (lastVer < commitIndex) {
    sError("vgId:%d, lastVer of WAL log less than tsdb commit version. lastVer:%" PRId64
           ", tsdb commit version:%" PRId64 "",
           pNode->vgId, lastVer, commitIndex);
    return -1;
  }

  return 0;
}

int32_t syncLogBufferInitWithoutLock(SSyncLogBuffer* pBuf, SSyncNode* pNode) {
  ASSERTS(pNode->pLogStore != NULL, "log store not created");
  ASSERTS(pNode->pFsm != NULL, "pFsm not registered");
  ASSERTS(pNode->pFsm->FpGetSnapshotInfo != NULL, "FpGetSnapshotInfo not registered");

  SSnapshot snapshot = {0};
  pNode->pFsm->FpGetSnapshotInfo(pNode->pFsm, &snapshot);

  SyncIndex commitIndex = snapshot.lastApplyIndex;
  SyncTerm  commitTerm = TMAX(snapshot.lastApplyTerm, 0);
  if (syncLogValidateAlignmentOfCommit(pNode, commitIndex)) {
    terrno = TSDB_CODE_WAL_LOG_INCOMPLETE;
    goto _err;
  }

  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
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
  int             emptySize = (TSDB_SYNC_LOG_BUFFER_SIZE >> 1);

  while (true) {
    if (index <= pBuf->commitIndex) {
      takeDummy = true;
      break;
    }

    if (pLogStore->syncLogGetEntry(pLogStore, index, &pEntry) < 0) {
      sWarn("vgId:%d, failed to get log entry since %s. index:%" PRId64 "", pNode->vgId, terrstr(), index);
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

  pBuf->isCatchup = false;

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
  syncLogBufferValidate(pBuf);
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
  syncLogBufferValidate(pBuf);
  taosThreadMutexUnlock(&pBuf->mutex);
  return ret;
}

FORCE_INLINE SyncTerm syncLogBufferGetLastMatchTermWithoutLock(SSyncLogBuffer* pBuf) {
  SyncIndex       index = pBuf->matchIndex;
  SSyncRaftEntry* pEntry = pBuf->entries[(index + pBuf->size) % pBuf->size].pItem;
  ASSERT(pEntry != NULL);
  return pEntry->term;
}

SyncTerm syncLogBufferGetLastMatchTerm(SSyncLogBuffer* pBuf) {
  taosThreadMutexLock(&pBuf->mutex);
  SyncTerm term = syncLogBufferGetLastMatchTermWithoutLock(pBuf);
  taosThreadMutexUnlock(&pBuf->mutex);
  return term;
}

bool syncLogBufferIsEmpty(SSyncLogBuffer* pBuf) {
  taosThreadMutexLock(&pBuf->mutex);
  bool empty = (pBuf->endIndex <= pBuf->startIndex);
  taosThreadMutexUnlock(&pBuf->mutex);
  return empty;
}

int32_t syncLogBufferAccept(SSyncLogBuffer* pBuf, SSyncNode* pNode, SSyncRaftEntry* pEntry, SyncTerm prevTerm) {
  taosThreadMutexLock(&pBuf->mutex);
  syncLogBufferValidate(pBuf);
  int32_t         ret = -1;
  SyncIndex       index = pEntry->index;
  SyncIndex       prevIndex = pEntry->index - 1;
  SyncTerm        lastMatchTerm = syncLogBufferGetLastMatchTermWithoutLock(pBuf);
  SSyncRaftEntry* pExist = NULL;
  bool            inBuf = true;

  if (index <= pBuf->commitIndex) {
    sTrace("vgId:%d, already committed. index:%" PRId64 ", term:%" PRId64 ". log buffer: [%" PRId64 " %" PRId64
           " %" PRId64 ", %" PRId64 ")",
           pNode->vgId, pEntry->index, pEntry->term, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex,
           pBuf->endIndex);
    SyncTerm term = syncLogReplGetPrevLogTerm(NULL, pNode, index + 1);
    ASSERT(pEntry->term >= 0);
    if (term == pEntry->term) {
      ret = 0;
    }
    goto _out;
  }

  if(pNode->raftCfg.cfg.nodeInfo[pNode->raftCfg.cfg.myIndex].nodeRole == TAOS_SYNC_ROLE_LEARNER &&
      index > 0 && index > pBuf->totalIndex){
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
    goto _out;
  }

  if (index > pBuf->matchIndex && lastMatchTerm != prevTerm) {
    sWarn("vgId:%d, not ready to accept. index:%" PRId64 ", term:%" PRId64 ": prevterm:%" PRId64
          " != lastmatch:%" PRId64 ". log buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 ")",
          pNode->vgId, pEntry->index, pEntry->term, prevTerm, lastMatchTerm, pBuf->startIndex, pBuf->commitIndex,
          pBuf->matchIndex, pBuf->endIndex);
    goto _out;
  }

  // check current in buffer
  pExist = syncLogBufferGetOneEntry(pBuf, pNode, index, &inBuf);
  if (pExist != NULL) {
    ASSERT(pEntry->index == pExist->index);
    if (pEntry->term != pExist->term) {
      (void)syncLogBufferRollback(pBuf, pNode, index);
    } else {
      sTrace("vgId:%d, duplicate log entry received. index:%" PRId64 ", term:%" PRId64 ". log buffer: [%" PRId64
             " %" PRId64 " %" PRId64 ", %" PRId64 ")",
             pNode->vgId, pEntry->index, pEntry->term, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex,
             pBuf->endIndex);
      SyncTerm existPrevTerm = syncLogReplGetPrevLogTerm(NULL, pNode, index);
      ASSERT(pEntry->term == pExist->term && (pEntry->index > pBuf->matchIndex || prevTerm == existPrevTerm));
      ret = 0;
      goto _out;
    }
  }

  // update
  ASSERT(pBuf->startIndex < index);
  ASSERT(index - pBuf->startIndex < pBuf->size);
  ASSERT(pBuf->entries[index % pBuf->size].pItem == NULL);
  SSyncLogBufEntry tmp = {.pItem = pEntry, .prevLogIndex = prevIndex, .prevLogTerm = prevTerm};
  pEntry = NULL;
  pBuf->entries[index % pBuf->size] = tmp;

  // update end index
  pBuf->endIndex = TMAX(index + 1, pBuf->endIndex);

  // success
  ret = 0;

_out:
  syncEntryDestroy(pEntry);
  if (!inBuf) {
    syncEntryDestroy(pExist);
    pExist = NULL;
  }
  syncLogBufferValidate(pBuf);
  taosThreadMutexUnlock(&pBuf->mutex);
  return ret;
}

static inline bool syncLogStoreNeedFlush(SSyncRaftEntry* pEntry, int32_t replicaNum) {
  return (replicaNum > 1) && (pEntry->originalRpcType == TDMT_VND_COMMIT);
}

int32_t syncLogStorePersist(SSyncLogStore* pLogStore, SSyncNode* pNode, SSyncRaftEntry* pEntry) {
  ASSERT(pEntry->index >= 0);
  SyncIndex lastVer = pLogStore->syncLogLastIndex(pLogStore);
  if (lastVer >= pEntry->index && pLogStore->syncLogTruncate(pLogStore, pEntry->index) < 0) {
    sError("failed to truncate log store since %s. from index:%" PRId64 "", terrstr(), pEntry->index);
    return -1;
  }
  lastVer = pLogStore->syncLogLastIndex(pLogStore);
  ASSERT(pEntry->index == lastVer + 1);

  bool doFsync = syncLogStoreNeedFlush(pEntry, pNode->replicaNum);
  if (pLogStore->syncLogAppendEntry(pLogStore, pEntry, doFsync) < 0) {
    sError("failed to append sync log entry since %s. index:%" PRId64 ", term:%" PRId64 "", terrstr(), pEntry->index,
           pEntry->term);
    return -1;
  }

  lastVer = pLogStore->syncLogLastIndex(pLogStore);
  ASSERT(pEntry->index == lastVer);
  return 0;
}

int64_t syncLogBufferProceed(SSyncLogBuffer* pBuf, SSyncNode* pNode, SyncTerm* pMatchTerm, char *str) {
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
    if (syncLogStorePersist(pLogStore, pNode, pEntry) < 0) {
      sError("vgId:%d, failed to persist sync log entry from buffer since %s. index:%" PRId64, pNode->vgId, terrstr(),
             pEntry->index);
      taosMsleep(1);
      goto _out;
    }
    
    if(pEntry->originalRpcType == TDMT_SYNC_CONFIG_CHANGE){
      if(pNode->pLogBuf->commitIndex == pEntry->index -1){
        sInfo("vgId:%d, to change config at %s. "
              "current entry, index:%" PRId64 ", term:%" PRId64", "
              "node, restore:%d, commitIndex:%" PRId64 ", "
              "cond: (pre entry index:%" PRId64 "== buf commit index:%" PRId64 ")",
              pNode->vgId, str,
              pEntry->index, pEntry->term, 
              pNode->restoreFinish, pNode->commitIndex,
              pEntry->index - 1, pNode->pLogBuf->commitIndex);
        if(syncNodeChangeConfig(pNode, pEntry, str) != 0){
          sError("vgId:%d, failed to change config from Append since %s. index:%" PRId64, pNode->vgId, terrstr(),
             pEntry->index);
          goto _out;
        }
      }
      else{
        sInfo("vgId:%d, delay change config from Node %s. "
              "curent entry, index:%" PRId64 ", term:%" PRId64 ", "
              "node, commitIndex:%" PRId64 ",  pBuf: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 "), "
              "cond:( pre entry index:%" PRId64" != buf commit index:%" PRId64 ")",
              pNode->vgId, str,
              pEntry->index, pEntry->term, 
              pNode->commitIndex, pNode->pLogBuf->startIndex, pNode->pLogBuf->commitIndex,
              pNode->pLogBuf->matchIndex, pNode->pLogBuf->endIndex, 
              pEntry->index - 1, pNode->pLogBuf->commitIndex);
      }
    }

    // replicate on demand
    (void)syncNodeReplicateWithoutLock(pNode);

    ASSERT(pEntry->index == pBuf->matchIndex);

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

int32_t syncFsmExecute(SSyncNode* pNode, SSyncFSM* pFsm, ESyncState role, SyncTerm term, SSyncRaftEntry* pEntry,
                          int32_t applyCode, bool force) {
  //learner need to execute fsm when it catch up entry log
  //if force is true, keep all contition check to execute fsm
  if (pNode->replicaNum == 1 && pNode->restoreFinish && pNode->vgId != 1
      && pNode->raftCfg.cfg.nodeInfo[pNode->raftCfg.cfg.myIndex].nodeRole != TAOS_SYNC_ROLE_LEARNER
      && force == false) {
    sDebug("vgId:%d, not to execute fsm, index:%" PRId64 ", term:%" PRId64 ", type:%s code:0x%x, replicaNum:%d,"
          "role:%d, restoreFinish:%d",
           pNode->vgId, pEntry->index, pEntry->term, TMSG_INFO(pEntry->originalRpcType), applyCode,
           pNode->replicaNum, pNode->raftCfg.cfg.nodeInfo[pNode->raftCfg.cfg.myIndex].nodeRole, pNode->restoreFinish);
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

  int32_t code = 0;
  bool    retry = false;
  do {
    SRpcMsg rpcMsg = {.code = applyCode};
    syncEntry2OriginalRpc(pEntry, &rpcMsg);

    SFsmCbMeta cbMeta = {0};
    cbMeta.index = pEntry->index;
    cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(pNode, pEntry->index);
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
      sError("vgId:%d, retry on fsm commit since %s. index:%" PRId64, pNode->vgId, terrstr(), pEntry->index);
    }
  } while (retry);
  return code;
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
  SyncTerm        currentTerm = raftStoreGetTerm(pNode);
  SyncGroupId     vgId = pNode->vgId;
  int32_t         ret = -1;
  int64_t         upperIndex = TMIN(commitIndex, pBuf->matchIndex);
  SSyncRaftEntry* pEntry = NULL;
  bool            inBuf = false;
  SSyncRaftEntry* pNextEntry = NULL;
  bool            nextInBuf = false;

  if (commitIndex <= pBuf->commitIndex) {
    sDebug("vgId:%d, stale commit index. current:%" PRId64 ", notified:%" PRId64 "", vgId, pBuf->commitIndex,
           commitIndex);
    ret = 0;
    goto _out;
  }

  sTrace("vgId:%d, commit. log buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64 "), role:%d, term:%" PRId64,
         pNode->vgId, pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex, role, currentTerm);

  // execute in fsm
  for (int64_t index = pBuf->commitIndex + 1; index <= upperIndex; index++) {
    // get a log entry
    pEntry = syncLogBufferGetOneEntry(pBuf, pNode, index, &inBuf);
    if (pEntry == NULL) {
      goto _out;
    }

    // execute it
    if (!syncUtilUserCommit(pEntry->originalRpcType)) {
      sInfo("vgId:%d, commit sync barrier. index:%" PRId64 ", term:%" PRId64 ", type:%s", vgId, pEntry->index,
            pEntry->term, TMSG_INFO(pEntry->originalRpcType));
    }

    if (syncFsmExecute(pNode, pFsm, role, currentTerm, pEntry, 0, false) != 0) {
      sError("vgId:%d, failed to execute sync log entry. index:%" PRId64 ", term:%" PRId64
             ", role:%d, current term:%" PRId64,
             vgId, pEntry->index, pEntry->term, role, currentTerm);
      goto _out;
    }
    pBuf->commitIndex = index;

    sTrace("vgId:%d, committed index:%" PRId64 ", term:%" PRId64 ", role:%d, current term:%" PRId64 "", pNode->vgId,
           pEntry->index, pEntry->term, role, currentTerm);

    pNextEntry = syncLogBufferGetOneEntry(pBuf, pNode, index + 1, &nextInBuf);
    if (pNextEntry != NULL) {
      if(pNextEntry->originalRpcType == TDMT_SYNC_CONFIG_CHANGE){
        sInfo("vgId:%d, to change config at Commit. "
              "current entry, index:%" PRId64 ", term:%" PRId64", "
              "node, role:%d, current term:%" PRId64 ", restore:%d, "
              "cond, next entry index:%" PRId64 ", msgType:%s",
              vgId, 
              pEntry->index, pEntry->term, 
              role, currentTerm, pNode->restoreFinish,
              pNextEntry->index, TMSG_INFO(pNextEntry->originalRpcType));

        if(syncNodeChangeConfig(pNode, pNextEntry, "Commit") != 0){
          sError("vgId:%d, failed to change config from Commit. index:%" PRId64 ", term:%" PRId64
                ", role:%d, current term:%" PRId64,
                vgId, pNextEntry->index, pNextEntry->term, role, currentTerm);
            goto _out;
        }

        //for 2->1, need to apply config change entry in sync thread,
        if(pNode->replicaNum == 1){
          if (syncFsmExecute(pNode, pFsm, role, currentTerm, pNextEntry, 0, true) != 0) {
            sError("vgId:%d, failed to execute sync log entry. index:%" PRId64 ", term:%" PRId64
                ", role:%d, current term:%" PRId64,
                vgId, pNextEntry->index, pNextEntry->term, role, currentTerm);
            goto _out;
          }

          index++;
          pBuf->commitIndex = index;

          sTrace("vgId:%d, committed index:%" PRId64 ", term:%" PRId64 ", role:%d, current term:%" PRId64 "", pNode->vgId,
                pNextEntry->index, pNextEntry->term, role, currentTerm);
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
    ASSERT(pEntry != NULL);
    syncEntryDestroy(pEntry);
    memset(&pBuf->entries[(index + pBuf->size) % pBuf->size], 0, sizeof(pBuf->entries[0]));
    pBuf->startIndex = index + 1;
  }

  ret = 0;
_out:
  // mark as restored if needed
  if (!pNode->restoreFinish && pBuf->commitIndex >= pNode->commitIndex && pEntry != NULL &&
      currentTerm <= pEntry->term) {
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
  syncLogBufferValidate(pBuf);
  taosThreadMutexUnlock(&pBuf->mutex);
  return ret;
}

void syncLogReplReset(SSyncLogReplMgr* pMgr) {
  if (pMgr == NULL) return;

  ASSERT(pMgr->startIndex >= 0);
  for (SyncIndex index = pMgr->startIndex; index < pMgr->endIndex; index++) {
    memset(&pMgr->states[index % pMgr->size], 0, sizeof(pMgr->states[0]));
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
    return -1;
  }

  int32_t  ret = -1;
  bool     retried = false;
  int64_t  retryWaitMs = syncLogReplGetRetryBackoffTimeMs(pMgr);
  int64_t  nowMs = taosGetMonoTimestampMs();
  int      count = 0;
  int64_t  firstIndex = -1;
  SyncTerm term = -1;
  int64_t  batchSize = TMAX(1, pMgr->size >> (4 + pMgr->retryBackoff));

  for (SyncIndex index = pMgr->startIndex; index < pMgr->endIndex; index++) {
    int64_t pos = index % pMgr->size;
    ASSERT(!pMgr->states[pos].barrier || (index == pMgr->startIndex || index + 1 == pMgr->endIndex));

    if (nowMs < pMgr->states[pos].timeMs + retryWaitMs) {
      break;
    }

    if (pMgr->states[pos].acked) {
      if (pMgr->matchIndex < index && pMgr->states[pos].timeMs + (syncGetRetryMaxWaitMs() << 3) < nowMs) {
        syncLogReplReset(pMgr);
        sWarn("vgId:%d, reset sync log repl since stagnation. index:%" PRId64 ", peer:%" PRIx64, pNode->vgId, index,
              pDestId->addr);
        goto _out;
      }
      continue;
    }

    bool barrier = false;
    if (syncLogReplSendTo(pMgr, pNode, index, &term, pDestId, &barrier) < 0) {
      sError("vgId:%d, failed to replicate sync log entry since %s. index:%" PRId64 ", dest:%" PRIx64 "", pNode->vgId,
             terrstr(), index, pDestId->addr);
      goto _out;
    }
    ASSERT(barrier == pMgr->states[pos].barrier);
    pMgr->states[pos].timeMs = nowMs;
    pMgr->states[pos].term = term;
    pMgr->states[pos].acked = false;

    retried = true;
    if (firstIndex == -1) firstIndex = index;

    if (batchSize <= count++) {
      break;
    }
  }

  ret = 0;
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
  return ret;
}

int32_t syncLogReplRecover(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  SRaftId         destId = pMsg->srcId;
  ASSERT(pMgr->restored == false);

  if (pMgr->endIndex == 0) {
    ASSERT(pMgr->startIndex == 0);
    ASSERT(pMgr->matchIndex == 0);
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
      syncLogReplRetryOnNeed(pMgr, pNode);
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
      if (syncNodeStartSnapshot(pNode, &destId) < 0) {
        sError("vgId:%d, failed to start snapshot for peer dnode:%d", pNode->vgId, DID(&destId));
        return -1;
      }
      return 0;
    }
  }

  // check last match term
  SyncTerm  term = -1;
  SyncIndex firstVer = pNode->pLogStore->syncLogBeginIndex(pNode->pLogStore);
  SyncIndex index = TMIN(pMsg->matchIndex, pNode->pLogBuf->matchIndex);

  if (pMsg->matchIndex < pNode->pLogBuf->matchIndex) {
    term = syncLogReplGetPrevLogTerm(pMgr, pNode, index + 1);
    if ((index + 1 < firstVer) || (term < 0) ||
        (term != pMsg->lastMatchTerm && (index + 1 == firstVer || index == firstVer))) {
      ASSERT(term >= 0 || terrno == TSDB_CODE_WAL_LOG_NOT_EXIST);
      if (syncNodeStartSnapshot(pNode, &destId) < 0) {
        sError("vgId:%d, failed to start snapshot for peer dnode:%d", pNode->vgId, DID(&destId));
        return -1;
      }
      sInfo("vgId:%d, snapshot replication to peer dnode:%d", pNode->vgId, DID(&destId));
      return 0;
    }

    ASSERT(index + 1 >= firstVer);

    if (term == pMsg->lastMatchTerm) {
      index = index + 1;
      ASSERT(index <= pNode->pLogBuf->matchIndex);
    } else {
      ASSERT(index > firstVer);
    }
  }

  // attempt to replicate the raft log at index
  (void)syncLogReplReset(pMgr);
  return syncLogReplProbe(pMgr, pNode, index);
}

int32_t syncLogReplProcessHeartbeatReply(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncHeartbeatReply* pMsg) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  taosThreadMutexLock(&pBuf->mutex);
  if (pMsg->startTime != 0 && pMsg->startTime != pMgr->peerStartTime) {
    sInfo("vgId:%d, reset sync log repl in heartbeat. peer:%" PRIx64 ", start time:%" PRId64 ", old:%" PRId64 "",
          pNode->vgId, pMsg->srcId.addr, pMsg->startTime, pMgr->peerStartTime);
    syncLogReplReset(pMgr);
    pMgr->peerStartTime = pMsg->startTime;
  }
  taosThreadMutexUnlock(&pBuf->mutex);
  return 0;
}

int32_t syncLogReplProcessReply(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg) {
  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  taosThreadMutexLock(&pBuf->mutex);
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
  taosThreadMutexUnlock(&pBuf->mutex);
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
  ASSERT(!pMgr->restored);
  ASSERT(pMgr->startIndex >= 0);
  int64_t retryMaxWaitMs = syncGetRetryMaxWaitMs();
  int64_t nowMs = taosGetMonoTimestampMs();

  if (pMgr->endIndex > pMgr->startIndex &&
      nowMs < pMgr->states[pMgr->startIndex % pMgr->size].timeMs + retryMaxWaitMs) {
    return 0;
  }
  (void)syncLogReplReset(pMgr);

  SRaftId* pDestId = &pNode->replicasId[pMgr->peerId];
  bool     barrier = false;
  SyncTerm term = -1;
  if (syncLogReplSendTo(pMgr, pNode, index, &term, pDestId, &barrier) < 0) {
    sError("vgId:%d, failed to replicate log entry since %s. index:%" PRId64 ", dest: 0x%016" PRIx64 "", pNode->vgId,
           terrstr(), index, pDestId->addr);
    return -1;
  }

  ASSERT(index >= 0);
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
  ASSERT(pMgr->restored);

  SRaftId*  pDestId = &pNode->replicasId[pMgr->peerId];
  int32_t   batchSize = TMAX(1, pMgr->size >> (4 + pMgr->retryBackoff));
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
    if (syncLogReplSendTo(pMgr, pNode, index, &term, pDestId, &barrier) < 0) {
      sError("vgId:%d, failed to replicate log entry since %s. index:%" PRId64 ", dest: 0x%016" PRIx64 "", pNode->vgId,
             terrstr(), index, pDestId->addr);
      return -1;
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

  syncLogReplRetryOnNeed(pMgr, pNode);

  SSyncLogBuffer* pBuf = pNode->pLogBuf;
  sTrace("vgId:%d, replicated %d msgs to peer:%" PRIx64 ". indexes:%" PRId64 "..., terms: ...%" PRId64
         ", repl-mgr:[%" PRId64 " %" PRId64 ", %" PRId64 "), buffer: [%" PRId64 " %" PRId64 " %" PRId64 ", %" PRId64
         ")",
         pNode->vgId, count, pDestId->addr, firstIndex, term, pMgr->startIndex, pMgr->matchIndex, pMgr->endIndex,
         pBuf->startIndex, pBuf->commitIndex, pBuf->matchIndex, pBuf->endIndex);
  return 0;
}

int32_t syncLogReplContinue(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg) {
  ASSERT(pMgr->restored == true);
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
      memset(&pMgr->states[index % pMgr->size], 0, sizeof(pMgr->states[0]));
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

  ASSERT(pMgr->size == TSDB_SYNC_LOG_BUFFER_SIZE);

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
    ASSERT(pNode->logReplMgrs[i] == NULL);
    pNode->logReplMgrs[i] = syncLogReplCreate();
    if (pNode->logReplMgrs[i] == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
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

SSyncLogBuffer* syncLogBufferCreate() {
  SSyncLogBuffer* pBuf = taosMemoryCalloc(1, sizeof(SSyncLogBuffer));
  if (pBuf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pBuf->size = sizeof(pBuf->entries) / sizeof(pBuf->entries[0]);

  ASSERT(pBuf->size == TSDB_SYNC_LOG_BUFFER_SIZE);

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
  ASSERT(pBuf->commitIndex < toIndex && toIndex <= pBuf->endIndex);

  if (toIndex == pBuf->endIndex) {
    return 0;
  }

  sInfo("vgId:%d, rollback sync log buffer. toindex:%" PRId64 ", buffer: [%" PRId64 " %" PRId64 " %" PRId64
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
  ASSERT(index + 1 == toIndex);

  // trunc wal
  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  if (lastVer >= toIndex && pNode->pLogStore->syncLogTruncate(pNode->pLogStore, toIndex) < 0) {
    sError("vgId:%d, failed to truncate log store since %s. from index:%" PRId64 "", pNode->vgId, terrstr(), toIndex);
    return -1;
  }
  lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  ASSERT(toIndex == lastVer + 1);

  // refill buffer on need
  if (toIndex <= pBuf->startIndex) {
    int32_t ret = syncLogBufferInitWithoutLock(pBuf, pNode);
    if (ret < 0) {
      sError("vgId:%d, failed to refill sync log buffer since %s", pNode->vgId, terrstr());
      return -1;
    }
  }

  ASSERT(pBuf->endIndex == toIndex);
  syncLogBufferValidate(pBuf);
  return 0;
}

int32_t syncLogBufferReset(SSyncLogBuffer* pBuf, SSyncNode* pNode) {
  taosThreadMutexLock(&pBuf->mutex);
  syncLogBufferValidate(pBuf);
  SyncIndex lastVer = pNode->pLogStore->syncLogLastIndex(pNode->pLogStore);
  ASSERT(lastVer == pBuf->matchIndex);
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
  syncLogBufferValidate(pBuf);
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
      sWarn("vgId:%d, failed to get log entry since %s. index:%" PRId64 "", pNode->vgId, terrstr(), index);
    }
  }
  return pEntry;
}

int32_t syncLogReplSendTo(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index, SyncTerm* pTerm, SRaftId* pDestId,
                          bool* pBarrier) {
  SSyncRaftEntry* pEntry = NULL;
  SRpcMsg         msgOut = {0};
  bool            inBuf = false;
  SyncTerm        prevLogTerm = -1;
  SSyncLogBuffer* pBuf = pNode->pLogBuf;

  pEntry = syncLogBufferGetOneEntry(pBuf, pNode, index, &inBuf);
  if (pEntry == NULL) {
    sWarn("vgId:%d, failed to get raft entry for index:%" PRId64 "", pNode->vgId, index);
    if (terrno == TSDB_CODE_WAL_LOG_NOT_EXIST) {
      SSyncLogReplMgr* pMgr = syncNodeGetLogReplMgr(pNode, pDestId);
      if (pMgr) {
        sInfo("vgId:%d, reset sync log repl of peer:%" PRIx64 " since %s. index:%" PRId64, pNode->vgId, pDestId->addr,
              terrstr(), index);
        (void)syncLogReplReset(pMgr);
      }
    }
    goto _err;
  }
  *pBarrier = syncLogReplBarrier(pEntry);

  prevLogTerm = syncLogReplGetPrevLogTerm(pMgr, pNode, index);
  if (prevLogTerm < 0) {
    sError("vgId:%d, failed to get prev log term since %s. index:%" PRId64 "", pNode->vgId, terrstr(), index);
    goto _err;
  }
  if (pTerm) *pTerm = pEntry->term;

  int32_t code = syncBuildAppendEntriesFromRaftEntry(pNode, pEntry, prevLogTerm, &msgOut);
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
  return -1;
}
