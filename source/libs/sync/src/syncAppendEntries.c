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
#include "syncPipeline.h"
#include "syncMessage.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncReplication.h"
#include "syncUtil.h"
#include "syncCommit.h"

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

  if (pMsg->prevLogIndex + 1 != pEntry->index || pEntry->term < 0) {
    sError("vgId:%d, invalid previous log index in msg. index:%" PRId64 ",  term:%" PRId64 ", prevLogIndex:%" PRId64
           ", prevLogTerm:%" PRId64,
           ths->vgId, pEntry->index, pEntry->term, pMsg->prevLogIndex, pMsg->prevLogTerm);
    goto _IGNORE;
  }

  sTrace("vgId:%d, recv append entries msg. index:%" PRId64 ", term:%" PRId64 ", preLogIndex:%" PRId64
         ", prevLogTerm:%" PRId64 " commitIndex:%" PRId64 "",
         pMsg->vgId, pMsg->prevLogIndex + 1, pMsg->term, pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->commitIndex);

  // accept
  if (syncLogBufferAccept(ths->pLogBuf, ths, pEntry, pMsg->prevLogTerm) < 0) {
    goto _SEND_RESPONSE;
  }
  accepted = true;

_SEND_RESPONSE:
  pReply->matchIndex = syncLogBufferProceed(ths->pLogBuf, ths, &pReply->lastMatchTerm);
  bool matched = (pReply->matchIndex >= pReply->lastSendIndex);
  if (accepted && matched) {
    pReply->success = true;
    // update commit index only after matching
    (void)syncNodeUpdateCommitIndex(ths, TMIN(pMsg->commitIndex, pEntry->index));
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

      ths->pLogStore->cacheHit++;
      sNTrace(ths, "hit cache index:%" PRId64 ", bytes:%u, %p", appendIndex, pLocalEntry->bytes, pLocalEntry);

    } else {
      ths->pLogStore->cacheMiss++;
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
