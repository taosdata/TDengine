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
#include "syncAppendEntries.h"
#include "syncMessage.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncReplication.h"
#include "syncUtil.h"

// TLA+ Spec
// HandleAppendEntriesRequest(i, j, m) ==
//    LET logOk == \/ m.mprevLogIndex = 0
//                 \/ /\ m.mprevLogIndex > 0
//                    /\ m.mprevLogIndex <= Len(log[i])
//                    /\ m.mprevLogTerm = log[i][m.mprevLogIndex].term
//    IN /\ m.mterm <= currentTerm[i]
//       /\ \/ /\ \* reject request
//                \/ m.mterm < currentTerm[i]
//                \/ /\ m.mterm = currentTerm[i]
//                   /\ state[i] = Follower
//                   /\ \lnot logOk
//             /\ Reply([mtype           |-> AppendEntriesResponse,
//                       mterm           |-> currentTerm[i],
//                       msuccess        |-> FALSE,
//                       mmatchIndex     |-> 0,
//                       msource         |-> i,
//                       mdest           |-> j],
//                       m)
//             /\ UNCHANGED <<serverVars, logVars>>
//          \/ \* return to follower state
//             /\ m.mterm = currentTerm[i]
//             /\ state[i] = Candidate
//             /\ state' = [state EXCEPT ![i] = Follower]
//             /\ UNCHANGED <<currentTerm, votedFor, logVars, messages>>
//          \/ \* accept request
//             /\ m.mterm = currentTerm[i]
//             /\ state[i] = Follower
//             /\ logOk
//             /\ LET index == m.mprevLogIndex + 1
//                IN \/ \* already done with request
//                       /\ \/ m.mentries = << >>
//                          \/ /\ m.mentries /= << >>
//                             /\ Len(log[i]) >= index
//                             /\ log[i][index].term = m.mentries[1].term
//                          \* This could make our commitIndex decrease (for
//                          \* example if we process an old, duplicated request),
//                          \* but that doesn't really affect anything.
//                       /\ commitIndex' = [commitIndex EXCEPT ![i] =
//                                              m.mcommitIndex]
//                       /\ Reply([mtype           |-> AppendEntriesResponse,
//                                 mterm           |-> currentTerm[i],
//                                 msuccess        |-> TRUE,
//                                 mmatchIndex     |-> m.mprevLogIndex +
//                                                     Len(m.mentries),
//                                 msource         |-> i,
//                                 mdest           |-> j],
//                                 m)
//                       /\ UNCHANGED <<serverVars, log>>
//                   \/ \* conflict: remove 1 entry
//                       /\ m.mentries /= << >>
//                       /\ Len(log[i]) >= index
//                       /\ log[i][index].term /= m.mentries[1].term
//                       /\ LET new == [index2 \in 1..(Len(log[i]) - 1) |->
//                                          log[i][index2]]
//                          IN log' = [log EXCEPT ![i] = new]
//                       /\ UNCHANGED <<serverVars, commitIndex, messages>>
//                   \/ \* no conflict: append entry
//                       /\ m.mentries /= << >>
//                       /\ Len(log[i]) = m.mprevLogIndex
//                       /\ log' = [log EXCEPT ![i] =
//                                      Append(log[i], m.mentries[1])]
//                       /\ UNCHANGED <<serverVars, commitIndex, messages>>
//       /\ UNCHANGED <<candidateVars, leaderVars>>
//

int32_t syncNodeFollowerCommit(SSyncNode* ths, SyncIndex newCommitIndex) {
  if (ths->state != TAOS_SYNC_STATE_FOLLOWER) {
    sNTrace(ths, "can not do follower commit");
    return -1;
  }

  // maybe update commit index, leader notice me
  if (newCommitIndex > ths->commitIndex) {
    // has commit entry in local
    if (newCommitIndex <= ths->pLogStore->syncLogLastIndex(ths->pLogStore)) {
      // advance commit index to sanpshot first
      SSnapshot snapshot;
      ths->pFsm->FpGetSnapshotInfo(ths->pFsm, &snapshot);
      if (snapshot.lastApplyIndex >= 0 && snapshot.lastApplyIndex > ths->commitIndex) {
        SyncIndex commitBegin = ths->commitIndex;
        SyncIndex commitEnd = snapshot.lastApplyIndex;
        ths->commitIndex = snapshot.lastApplyIndex;
        sNTrace(ths, "commit by snapshot from index:%" PRId64 " to index:%" PRId64, commitBegin, commitEnd);
      }

      SyncIndex beginIndex = ths->commitIndex + 1;
      SyncIndex endIndex = newCommitIndex;

      // update commit index
      ths->commitIndex = newCommitIndex;

      // call back Wal
      int32_t code = ths->pLogStore->syncLogUpdateCommitIndex(ths->pLogStore, ths->commitIndex);
      ASSERT(code == 0);

      code = syncNodeDoCommit(ths, beginIndex, endIndex, ths->state);
      ASSERT(code == 0);
    }
  }

  return 0;
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
      ASSERT(0);
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

SSyncRaftEntry* syncLogAppendEntriesToRaftEntry(const SyncAppendEntries* pMsg) {
  SSyncRaftEntry* pEntry = taosMemoryMalloc(pMsg->dataLen);
  if (pEntry == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  (void)memcpy(pEntry, pMsg->data, pMsg->dataLen);
  ASSERT(pEntry->bytes == pMsg->dataLen);
  return pEntry;
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
  int64_t matchIndex = pBuf->matchIndex;

  while (pBuf->matchIndex + 1 < pBuf->endIndex) {
    int64_t index = pBuf->matchIndex + 1;
    ASSERT(index >= 0);

    // try to proceed
    SSyncLogBufEntry* pBufEntry = &pBuf->entries[index % pBuf->size];
    SyncIndex         prevLogIndex = pBufEntry->prevLogIndex;
    SyncTerm          prevLogTerm = pBufEntry->prevLogTerm;
    SSyncRaftEntry*   pEntry = pBufEntry->pItem;
    if (pEntry == NULL) {
      sDebug("vgId:%d, cannot proceed match index in log buffer. no raft entry at next pos of matchIndex:%" PRId64,
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

    sInfo("vgId:%d, log buffer proceed. start index: %" PRId64 ", match index: %" PRId64 ", end index: %" PRId64,
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

int32_t syncLogFsmExecute(SSyncFSM* pFsm, ESyncState role, SyncTerm term, SSyncRaftEntry* pEntry) {
  ASSERT(pFsm->FpCommitCb != NULL && "No commit cb registered for the FSM");

  SRpcMsg rpcMsg;
  syncEntry2OriginalRpc(pEntry, &rpcMsg);

  SFsmCbMeta cbMeta = {0};
  cbMeta.index = pEntry->index;
  cbMeta.lastConfigIndex = -1;
  cbMeta.isWeak = pEntry->isWeak;
  cbMeta.code = 0;
  cbMeta.state = role;
  cbMeta.seqNum = pEntry->seqNum;
  cbMeta.term = pEntry->term;
  cbMeta.currentTerm = term;
  cbMeta.flag = -1;

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

  SSyncLogStore* pLogStore = pNode->pLogStore;
  SSyncFSM*      pFsm = pNode->pFsm;
  ESyncState     role = pNode->state;
  SyncTerm       term = pNode->pRaftStore->currentTerm;
  SyncGroupId    vgId = pNode->vgId;
  int32_t        ret = 0;
  int64_t         upperIndex = TMIN(commitIndex, pBuf->matchIndex);
  SSyncRaftEntry* pEntry = NULL;
  bool            inBuf = false;

  if (commitIndex <= pBuf->commitIndex) {
    sDebug("vgId:%d, stale commit update. current:%" PRId64 ", notified:%" PRId64 "", vgId, pBuf->commitIndex,
           commitIndex);
    ret = 0;
    goto _out;
  }

  sDebug("vgId:%d, log buffer info. role: %d, term: %" PRId64 ". start index:%" PRId64 ", commit index:%" PRId64
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

    if (syncLogFsmExecute(pFsm, role, term, pEntry) != 0) {
      sError("vgId:%d, failed to execute raft entry in FSM. log index:%" PRId64 ", term:%" PRId64 "", vgId,
             pEntry->index, pEntry->term);
      ret = -1;
      goto _out;
    }
    pBuf->commitIndex = index;

    sDebug("vgId:%d, committed index: %" PRId64 ", term: %" PRId64 ", role: %d, current term: %" PRId64 "", pNode->vgId,
           pEntry->index, pEntry->term, role, term);

    if (!inBuf) {
      syncEntryDestroy(pEntry);
      pEntry = NULL;
    }
  }

  // recycle
  SyncIndex used = pBuf->endIndex - pBuf->startIndex;
  SyncIndex until = pBuf->commitIndex - (pBuf->size - used) / 2;
  for (SyncIndex index = pBuf->startIndex; index < until; index++) {
    SSyncRaftEntry* pEntry = pBuf->entries[(index + pBuf->size) % pBuf->size].pItem;
    ASSERT(pEntry != NULL);
    syncEntryDestroy(pEntry);
    memset(&pBuf->entries[(index + pBuf->size) % pBuf->size], 0, sizeof(pBuf->entries[0]));
    pBuf->startIndex = index + 1;
  }

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

int32_t syncNodeOnAppendEntries(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  SyncAppendEntries* pMsg = pRpcMsg->pCont;
  SRpcMsg            rpcRsp = {0};
  bool               accepted = false;
  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId))) {
    syncLogRecvAppendEntries(ths, pMsg, "not in my config");
    goto _IGNORE;
  }

  int32_t code = syncBuildAppendEntriesReply(&rpcRsp, ths->vgId);
  if (code != 0) {
    syncLogRecvAppendEntries(ths, pMsg, "build rsp error");
    goto _IGNORE;
  }

  SyncAppendEntriesReply* pReply = rpcRsp.pCont;
  // prepare response msg
  pReply->srcId = ths->myRaftId;
  pReply->destId = pMsg->srcId;
  pReply->term = ths->pRaftStore->currentTerm;
  pReply->success = false;
  pReply->matchIndex = SYNC_INDEX_INVALID;
  pReply->lastSendIndex = pMsg->prevLogIndex + 1;
  pReply->startTime = ths->startTime;

  if (pMsg->term < ths->pRaftStore->currentTerm) {
    goto _SEND_RESPONSE;
  }

  if (pMsg->term > ths->pRaftStore->currentTerm) {
    pReply->term = pMsg->term;
  }

  syncNodeStepDown(ths, pMsg->term);
  syncNodeResetElectTimer(ths);

  if (pMsg->dataLen < (int32_t)sizeof(SSyncRaftEntry)) {
    sError("vgId:%d, incomplete append entries received. prev index:%" PRId64 ", term:%" PRId64 ", datalen:%d",
           ths->vgId, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->dataLen);
    goto _IGNORE;
  }

  SSyncRaftEntry* pEntry = syncLogAppendEntriesToRaftEntry(pMsg);

  if (pEntry == NULL) {
    sError("vgId:%d, failed to get raft entry from append entries since %s", ths->vgId, terrstr());
    goto _IGNORE;
  }

  if (pMsg->prevLogIndex + 1 != pEntry->index) {
    sError("vgId:%d, invalid previous log index in msg. index:%" PRId64 ",  term:%" PRId64 ", prevLogIndex:%" PRId64
           ", prevLogTerm:%" PRId64,
           ths->vgId, pEntry->index, pEntry->term, pMsg->prevLogIndex, pMsg->prevLogTerm);
    goto _IGNORE;
  }

  sInfo("vgId:%d, recv append entries msg. index:%" PRId64 ", term:%" PRId64 ", preLogIndex:%" PRId64
        ", prevLogTerm:%" PRId64 " commitIndex:%" PRId64 "",
        pMsg->vgId, pMsg->prevLogIndex + 1, pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->commitIndex);

  // accept
  if (syncLogBufferAccept(ths->pLogBuf, ths, pEntry, pMsg->prevLogTerm) < 0) {
    goto _SEND_RESPONSE;
  }
  accepted = true;

_SEND_RESPONSE:
  pReply->matchIndex = syncLogBufferProceed(ths->pLogBuf, ths);
  bool matched = (pReply->matchIndex >= pReply->lastSendIndex);
  if (accepted && matched) {
    pReply->success = true;
    // update commit index only after matching
    (void)syncNodeUpdateCommitIndex(ths, pMsg->commitIndex);
  }

  // ack, i.e. send response
  (void)syncNodeSendMsgById(&pReply->destId, ths, &rpcRsp);

  // commit index, i.e. leader notice me
  if (syncLogBufferCommit(ths->pLogBuf, ths, ths->commitIndex) < 0) {
    sError("vgId:%d, failed to commit raft fsm log since %s.", ths->vgId, terrstr());
    goto _out;
  }

_out:
  return 0;

_IGNORE:
  rpcFreeCont(rpcRsp.pCont);
  return 0;
}

int32_t syncNodeOnAppendEntriesOld(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  SyncAppendEntries* pMsg = pRpcMsg->pCont;
  SRpcMsg            rpcRsp = {0};

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId))) {
    syncLogRecvAppendEntries(ths, pMsg, "not in my config");
    goto _IGNORE;
  }

  // prepare response msg
  int32_t code = syncBuildAppendEntriesReply(&rpcRsp, ths->vgId);
  if (code != 0) {
    syncLogRecvAppendEntries(ths, pMsg, "build rsp error");
    goto _IGNORE;
  }

  SyncAppendEntriesReply* pReply = rpcRsp.pCont;
  pReply->srcId = ths->myRaftId;
  pReply->destId = pMsg->srcId;
  pReply->term = ths->pRaftStore->currentTerm;
  pReply->success = false;
  // pReply->matchIndex = ths->pLogStore->syncLogLastIndex(ths->pLogStore);
  pReply->matchIndex = SYNC_INDEX_INVALID;
  pReply->lastSendIndex = pMsg->prevLogIndex + 1;
  pReply->startTime = ths->startTime;

  if (pMsg->term < ths->pRaftStore->currentTerm) {
    syncLogRecvAppendEntries(ths, pMsg, "reject, small term");
    goto _SEND_RESPONSE;
  }

  if (pMsg->term > ths->pRaftStore->currentTerm) {
    pReply->term = pMsg->term;
  }

  syncNodeStepDown(ths, pMsg->term);
  syncNodeResetElectTimer(ths);

  SyncIndex startIndex = ths->pLogStore->syncLogBeginIndex(ths->pLogStore);
  SyncIndex lastIndex = ths->pLogStore->syncLogLastIndex(ths->pLogStore);

  if (pMsg->prevLogIndex > lastIndex) {
    syncLogRecvAppendEntries(ths, pMsg, "reject, index not match");
    goto _SEND_RESPONSE;
  }

  if (pMsg->prevLogIndex >= startIndex) {
    SyncTerm myPreLogTerm = syncNodeGetPreTerm(ths, pMsg->prevLogIndex + 1);
    // ASSERT(myPreLogTerm != SYNC_TERM_INVALID);
    if (myPreLogTerm == SYNC_TERM_INVALID) {
      syncLogRecvAppendEntries(ths, pMsg, "reject, pre-term invalid");
      goto _SEND_RESPONSE;
    }

    if (myPreLogTerm != pMsg->prevLogTerm) {
      syncLogRecvAppendEntries(ths, pMsg, "reject, pre-term not match");
      goto _SEND_RESPONSE;
    }
  }

  // accept
  pReply->success = true;
  bool hasAppendEntries = pMsg->dataLen > 0;
  if (hasAppendEntries) {
    SSyncRaftEntry* pAppendEntry = syncEntryBuildFromAppendEntries(pMsg);
    ASSERT(pAppendEntry != NULL);

    SyncIndex appendIndex = pMsg->prevLogIndex + 1;

    LRUHandle* hLocal = NULL;
    LRUHandle* hAppend = NULL;

    int32_t         code = 0;
    SSyncRaftEntry* pLocalEntry = NULL;
    SLRUCache*      pCache = ths->pLogStore->pCache;
    hLocal = taosLRUCacheLookup(pCache, &appendIndex, sizeof(appendIndex));
    if (hLocal) {
      pLocalEntry = (SSyncRaftEntry*)taosLRUCacheValue(pCache, hLocal);
      code = 0;

      sNTrace(ths, "hit cache index:%" PRId64 ", bytes:%u, %p", appendIndex, pLocalEntry->bytes, pLocalEntry);

    } else {
      sNTrace(ths, "miss cache index:%" PRId64, appendIndex);

      code = ths->pLogStore->syncLogGetEntry(ths->pLogStore, appendIndex, &pLocalEntry);
    }

    if (code == 0) {
      // get local entry success

      if (pLocalEntry->term == pAppendEntry->term) {
        // do nothing
        sNTrace(ths, "log match, do nothing, index:%" PRId64, appendIndex);

      } else {
        // truncate
        code = ths->pLogStore->syncLogTruncate(ths->pLogStore, appendIndex);
        if (code != 0) {
          char logBuf[128];
          snprintf(logBuf, sizeof(logBuf), "ignore, truncate error, append-index:%" PRId64, appendIndex);
          syncLogRecvAppendEntries(ths, pMsg, logBuf);

          if (hLocal) {
            taosLRUCacheRelease(ths->pLogStore->pCache, hLocal, false);
          } else {
            syncEntryDestroy(pLocalEntry);
          }

          if (hAppend) {
            taosLRUCacheRelease(ths->pLogStore->pCache, hAppend, false);
          } else {
            syncEntryDestroy(pAppendEntry);
          }

          goto _IGNORE;
        }

        ASSERT(pAppendEntry->index == appendIndex);

        // append
        code = ths->pLogStore->syncLogAppendEntry(ths->pLogStore, pAppendEntry);
        if (code != 0) {
          char logBuf[128];
          snprintf(logBuf, sizeof(logBuf), "ignore, append error, append-index:%" PRId64, appendIndex);
          syncLogRecvAppendEntries(ths, pMsg, logBuf);

          if (hLocal) {
            taosLRUCacheRelease(ths->pLogStore->pCache, hLocal, false);
          } else {
            syncEntryDestroy(pLocalEntry);
          }

          if (hAppend) {
            taosLRUCacheRelease(ths->pLogStore->pCache, hAppend, false);
          } else {
            syncEntryDestroy(pAppendEntry);
          }

          goto _IGNORE;
        }

        syncCacheEntry(ths->pLogStore, pAppendEntry, &hAppend);
      }

    } else {
      if (terrno == TSDB_CODE_WAL_LOG_NOT_EXIST) {
        // log not exist

        // truncate
        code = ths->pLogStore->syncLogTruncate(ths->pLogStore, appendIndex);
        if (code != 0) {
          char logBuf[128];
          snprintf(logBuf, sizeof(logBuf), "ignore, log not exist, truncate error, append-index:%" PRId64, appendIndex);
          syncLogRecvAppendEntries(ths, pMsg, logBuf);

          syncEntryDestroy(pLocalEntry);
          syncEntryDestroy(pAppendEntry);
          goto _IGNORE;
        }

        // append
        code = ths->pLogStore->syncLogAppendEntry(ths->pLogStore, pAppendEntry);
        if (code != 0) {
          char logBuf[128];
          snprintf(logBuf, sizeof(logBuf), "ignore, log not exist, append error, append-index:%" PRId64, appendIndex);
          syncLogRecvAppendEntries(ths, pMsg, logBuf);

          if (hLocal) {
            taosLRUCacheRelease(ths->pLogStore->pCache, hLocal, false);
          } else {
            syncEntryDestroy(pLocalEntry);
          }

          if (hAppend) {
            taosLRUCacheRelease(ths->pLogStore->pCache, hAppend, false);
          } else {
            syncEntryDestroy(pAppendEntry);
          }

          goto _IGNORE;
        }

        syncCacheEntry(ths->pLogStore, pAppendEntry, &hAppend);

      } else {
        // get local entry success
        char logBuf[128];
        snprintf(logBuf, sizeof(logBuf), "ignore, get local entry error, append-index:%" PRId64 " err:%d", appendIndex,
                 terrno);
        syncLogRecvAppendEntries(ths, pMsg, logBuf);

        if (hLocal) {
          taosLRUCacheRelease(ths->pLogStore->pCache, hLocal, false);
        } else {
          syncEntryDestroy(pLocalEntry);
        }

        if (hAppend) {
          taosLRUCacheRelease(ths->pLogStore->pCache, hAppend, false);
        } else {
          syncEntryDestroy(pAppendEntry);
        }

        goto _IGNORE;
      }
    }

    // update match index
    pReply->matchIndex = pAppendEntry->index;

    if (hLocal) {
      taosLRUCacheRelease(ths->pLogStore->pCache, hLocal, false);
    } else {
      syncEntryDestroy(pLocalEntry);
    }

    if (hAppend) {
      taosLRUCacheRelease(ths->pLogStore->pCache, hAppend, false);
    } else {
      syncEntryDestroy(pAppendEntry);
    }

  } else {
    // no append entries, do nothing
    // maybe has extra entries, no harm

    // update match index
    pReply->matchIndex = pMsg->prevLogIndex;
  }

  // maybe update commit index, leader notice me
  syncNodeFollowerCommit(ths, pMsg->commitIndex);

  syncLogRecvAppendEntries(ths, pMsg, "accept");
  goto _SEND_RESPONSE;

_IGNORE:
  rpcFreeCont(rpcRsp.pCont);
  return 0;

_SEND_RESPONSE:
  // msg event log
  syncLogSendAppendEntriesReply(ths, pReply, "");

  // send response
  syncNodeSendMsgById(&pReply->destId, ths, &rpcRsp);
  return 0;
}
