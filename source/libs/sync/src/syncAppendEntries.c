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

#include "syncAppendEntries.h"
#include "syncInt.h"
#include "syncRaftCfg.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncSnapshot.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"
#include "wal.h"

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

int32_t syncNodeOnAppendEntriesCb(SSyncNode* ths, SyncAppendEntries* pMsg) {
  int32_t ret = 0;

  // print log
  syncAppendEntriesLog2("==syncNodeOnAppendEntriesCb==", pMsg);

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId)) && !ths->pRaftCfg->isStandBy) {
    syncNodeEventLog(ths, "recv sync-append-entries, maybe replica already dropped");
    return ret;
  }

  // maybe update term
  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncNodeUpdateTerm(ths, pMsg->term);
  }
  ASSERT(pMsg->term <= ths->pRaftStore->currentTerm);

  // reset elect timer
  if (pMsg->term == ths->pRaftStore->currentTerm) {
    ths->leaderCache = pMsg->srcId;
    syncNodeResetElectTimer(ths);
  }
  ASSERT(pMsg->dataLen >= 0);

  do {
    // return to follower state
    if (pMsg->term == ths->pRaftStore->currentTerm && ths->state == TAOS_SYNC_STATE_CANDIDATE) {
      syncNodeEventLog(ths, "recv sync-append-entries, candidate to follower");

      syncNodeBecomeFollower(ths, "from candidate by append entries");

      // ret or reply?
      return ret;
    }
  } while (0);

  SyncTerm localPreLogTerm = 0;
  if (pMsg->prevLogIndex >= SYNC_INDEX_BEGIN && pMsg->prevLogIndex <= ths->pLogStore->getLastIndex(ths->pLogStore)) {
    SSyncRaftEntry* pEntry = ths->pLogStore->getEntry(ths->pLogStore, pMsg->prevLogIndex);
    if (pEntry == NULL) {
      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf), "getEntry error, index:%ld, since %s", pMsg->prevLogIndex, terrstr());
      syncNodeErrorLog(ths, logBuf);
      return -1;
    }

    localPreLogTerm = pEntry->term;
    syncEntryDestory(pEntry);
  }

  bool logOK =
      (pMsg->prevLogIndex == SYNC_INDEX_INVALID) ||
      ((pMsg->prevLogIndex >= SYNC_INDEX_BEGIN) &&
       (pMsg->prevLogIndex <= ths->pLogStore->getLastIndex(ths->pLogStore)) && (pMsg->prevLogTerm == localPreLogTerm));

  // reject request
  if ((pMsg->term < ths->pRaftStore->currentTerm) ||
      ((pMsg->term == ths->pRaftStore->currentTerm) && (ths->state == TAOS_SYNC_STATE_FOLLOWER) && !logOK)) {
    do {
      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf), "recv sync-append-entries, reject, pre-index:%ld, pre-term:%lu, datalen:%d",
               pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->dataLen);
      syncNodeEventLog(ths, logBuf);
    } while (0);

    SyncAppendEntriesReply* pReply = syncAppendEntriesReplyBuild(ths->vgId);
    pReply->srcId = ths->myRaftId;
    pReply->destId = pMsg->srcId;
    pReply->term = ths->pRaftStore->currentTerm;
    pReply->success = false;
    pReply->matchIndex = SYNC_INDEX_INVALID;

    SRpcMsg rpcMsg;
    syncAppendEntriesReply2RpcMsg(pReply, &rpcMsg);
    syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
    syncAppendEntriesReplyDestroy(pReply);

    return ret;
  }

  // accept request
  if (pMsg->term == ths->pRaftStore->currentTerm && ths->state == TAOS_SYNC_STATE_FOLLOWER && logOK) {
    // preIndex = -1, or has preIndex entry in local log
    ASSERT(pMsg->prevLogIndex <= ths->pLogStore->getLastIndex(ths->pLogStore));

    // has extra entries (> preIndex) in local log
    bool hasExtraEntries = pMsg->prevLogIndex < ths->pLogStore->getLastIndex(ths->pLogStore);

    // has entries in SyncAppendEntries msg
    bool hasAppendEntries = pMsg->dataLen > 0;

    do {
      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf), "recv sync-append-entries, accept, pre-index:%ld, pre-term:%lu, datalen:%d",
               pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->dataLen);
      syncNodeEventLog(ths, logBuf);
    } while (0);

    if (hasExtraEntries && hasAppendEntries) {
      // not conflict by default
      bool conflict = false;

      SyncIndex       extraIndex = pMsg->prevLogIndex + 1;
      SSyncRaftEntry* pExtraEntry = ths->pLogStore->getEntry(ths->pLogStore, extraIndex);
      if (pExtraEntry == NULL) {
        char logBuf[128];
        snprintf(logBuf, sizeof(logBuf), "getEntry error2, index:%ld, since %s", extraIndex, terrstr());
        syncNodeErrorLog(ths, logBuf);
        return -1;
      }

      SSyncRaftEntry* pAppendEntry = syncEntryDeserialize(pMsg->data, pMsg->dataLen);
      if (pAppendEntry == NULL) {
        syncNodeErrorLog(ths, "syncEntryDeserialize pAppendEntry error");
        return -1;
      }

      // log not match, conflict
      ASSERT(extraIndex == pAppendEntry->index);
      if (pExtraEntry->term != pAppendEntry->term) {
        conflict = true;
      }

      if (conflict) {
        // roll back
        SyncIndex delBegin = ths->pLogStore->getLastIndex(ths->pLogStore);
        SyncIndex delEnd = extraIndex;

        sTrace("syncNodeOnAppendEntriesCb --> conflict:%d, delBegin:%ld, delEnd:%ld", conflict, delBegin, delEnd);

        // notice! reverse roll back!
        for (SyncIndex index = delEnd; index >= delBegin; --index) {
          if (ths->pFsm->FpRollBackCb != NULL) {
            SSyncRaftEntry* pRollBackEntry = ths->pLogStore->getEntry(ths->pLogStore, index);
            if (pRollBackEntry == NULL) {
              char logBuf[128];
              snprintf(logBuf, sizeof(logBuf), "getEntry error3, index:%ld, since %s", index, terrstr());
              syncNodeErrorLog(ths, logBuf);
              return -1;
            }

            // if (pRollBackEntry->msgType != TDMT_SYNC_NOOP) {
            if (syncUtilUserRollback(pRollBackEntry->msgType)) {
              SRpcMsg rpcMsg;
              syncEntry2OriginalRpc(pRollBackEntry, &rpcMsg);

              SFsmCbMeta cbMeta = {0};
              cbMeta.index = pRollBackEntry->index;
              cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
              cbMeta.isWeak = pRollBackEntry->isWeak;
              cbMeta.code = 0;
              cbMeta.state = ths->state;
              cbMeta.seqNum = pRollBackEntry->seqNum;
              ths->pFsm->FpRollBackCb(ths->pFsm, &rpcMsg, cbMeta);
              rpcFreeCont(rpcMsg.pCont);
            }

            syncEntryDestory(pRollBackEntry);
          }
        }

        // delete confict entries
        ths->pLogStore->truncate(ths->pLogStore, extraIndex);

        // append new entries
        ths->pLogStore->appendEntry(ths->pLogStore, pAppendEntry);

        // pre commit
        SRpcMsg rpcMsg;
        syncEntry2OriginalRpc(pAppendEntry, &rpcMsg);
        if (ths->pFsm != NULL) {
          // if (ths->pFsm->FpPreCommitCb != NULL && pAppendEntry->originalRpcType != TDMT_SYNC_NOOP) {
          if (ths->pFsm->FpPreCommitCb != NULL && syncUtilUserPreCommit(pAppendEntry->originalRpcType)) {
            SFsmCbMeta cbMeta = {0};
            cbMeta.index = pAppendEntry->index;
            cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
            cbMeta.isWeak = pAppendEntry->isWeak;
            cbMeta.code = 2;
            cbMeta.state = ths->state;
            cbMeta.seqNum = pAppendEntry->seqNum;
            ths->pFsm->FpPreCommitCb(ths->pFsm, &rpcMsg, cbMeta);
          }
        }
        rpcFreeCont(rpcMsg.pCont);
      }

      // free memory
      syncEntryDestory(pExtraEntry);
      syncEntryDestory(pAppendEntry);

    } else if (hasExtraEntries && !hasAppendEntries) {
      // do nothing

    } else if (!hasExtraEntries && hasAppendEntries) {
      SSyncRaftEntry* pAppendEntry = syncEntryDeserialize(pMsg->data, pMsg->dataLen);
      if (pAppendEntry == NULL) {
        syncNodeErrorLog(ths, "syncEntryDeserialize pAppendEntry2 error");
        return -1;
      }

      // append new entries
      ths->pLogStore->appendEntry(ths->pLogStore, pAppendEntry);

      // pre commit
      SRpcMsg rpcMsg;
      syncEntry2OriginalRpc(pAppendEntry, &rpcMsg);
      if (ths->pFsm != NULL) {
        // if (ths->pFsm->FpPreCommitCb != NULL && pAppendEntry->originalRpcType != TDMT_SYNC_NOOP) {
        if (ths->pFsm->FpPreCommitCb != NULL && syncUtilUserPreCommit(pAppendEntry->originalRpcType)) {
          SFsmCbMeta cbMeta = {0};
          cbMeta.index = pAppendEntry->index;
          cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
          cbMeta.isWeak = pAppendEntry->isWeak;
          cbMeta.code = 3;
          cbMeta.state = ths->state;
          cbMeta.seqNum = pAppendEntry->seqNum;
          ths->pFsm->FpPreCommitCb(ths->pFsm, &rpcMsg, cbMeta);
        }
      }
      rpcFreeCont(rpcMsg.pCont);

      // free memory
      syncEntryDestory(pAppendEntry);

    } else if (!hasExtraEntries && !hasAppendEntries) {
      // do nothing

    } else {
      syncNodeLog3("", ths);
      ASSERT(0);
    }

    SyncAppendEntriesReply* pReply = syncAppendEntriesReplyBuild(ths->vgId);
    pReply->srcId = ths->myRaftId;
    pReply->destId = pMsg->srcId;
    pReply->term = ths->pRaftStore->currentTerm;
    pReply->success = true;

    if (hasAppendEntries) {
      pReply->matchIndex = pMsg->prevLogIndex + 1;
    } else {
      pReply->matchIndex = pMsg->prevLogIndex;
    }

    SRpcMsg rpcMsg;
    syncAppendEntriesReply2RpcMsg(pReply, &rpcMsg);
    syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
    syncAppendEntriesReplyDestroy(pReply);

    // maybe update commit index from leader
    if (pMsg->commitIndex > ths->commitIndex) {
      // has commit entry in local
      if (pMsg->commitIndex <= ths->pLogStore->getLastIndex(ths->pLogStore)) {
        SyncIndex beginIndex = ths->commitIndex + 1;
        SyncIndex endIndex = pMsg->commitIndex;

        // update commit index
        ths->commitIndex = pMsg->commitIndex;

        // call back Wal
        ths->pLogStore->updateCommitIndex(ths->pLogStore, ths->commitIndex);

        int32_t code = syncNodeCommit(ths, beginIndex, endIndex, ths->state);
        ASSERT(code == 0);
      }
    }
  }

  return ret;
}

#if 0

int32_t syncNodeOnAppendEntriesCb(SSyncNode* ths, SyncAppendEntries* pMsg) {
  int32_t ret = 0;

  char logBuf[128] = {0};
  snprintf(logBuf, sizeof(logBuf), "==syncNodeOnAppendEntriesCb== term:%lu", ths->pRaftStore->currentTerm);
  syncAppendEntriesLog2(logBuf, pMsg);

  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncNodeUpdateTerm(ths, pMsg->term);
  }
  ASSERT(pMsg->term <= ths->pRaftStore->currentTerm);

  // reset elect timer
  if (pMsg->term == ths->pRaftStore->currentTerm) {
    ths->leaderCache = pMsg->srcId;
    syncNodeResetElectTimer(ths);
  }
  ASSERT(pMsg->dataLen >= 0);

  SyncTerm localPreLogTerm = 0;
  if (pMsg->prevLogIndex >= SYNC_INDEX_BEGIN && pMsg->prevLogIndex <= ths->pLogStore->getLastIndex(ths->pLogStore)) {
    SSyncRaftEntry* pEntry = ths->pLogStore->getEntry(ths->pLogStore, pMsg->prevLogIndex);
    if (pEntry == NULL) {
      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf), "getEntry error, index:%ld, since %s", pMsg->prevLogIndex, terrstr());
      syncNodeErrorLog(ths, logBuf);
      return -1;
    }

    localPreLogTerm = pEntry->term;
    syncEntryDestory(pEntry);
  }

  bool logOK =
      (pMsg->prevLogIndex == SYNC_INDEX_INVALID) ||
      ((pMsg->prevLogIndex >= SYNC_INDEX_BEGIN) &&
       (pMsg->prevLogIndex <= ths->pLogStore->getLastIndex(ths->pLogStore)) && (pMsg->prevLogTerm == localPreLogTerm));

  // reject request
  if ((pMsg->term < ths->pRaftStore->currentTerm) ||
      ((pMsg->term == ths->pRaftStore->currentTerm) && (ths->state == TAOS_SYNC_STATE_FOLLOWER) && !logOK)) {
    sTrace(
        "syncNodeOnAppendEntriesCb --> reject, pMsg->term:%lu, ths->pRaftStore->currentTerm:%lu, ths->state:%d, "
        "logOK:%d",
        pMsg->term, ths->pRaftStore->currentTerm, ths->state, logOK);

    SyncAppendEntriesReply* pReply = syncAppendEntriesReplyBuild(ths->vgId);
    pReply->srcId = ths->myRaftId;
    pReply->destId = pMsg->srcId;
    pReply->term = ths->pRaftStore->currentTerm;
    pReply->success = false;
    pReply->matchIndex = SYNC_INDEX_INVALID;

    SRpcMsg rpcMsg;
    syncAppendEntriesReply2RpcMsg(pReply, &rpcMsg);
    syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
    syncAppendEntriesReplyDestroy(pReply);

    return ret;
  }

  // return to follower state
  if (pMsg->term == ths->pRaftStore->currentTerm && ths->state == TAOS_SYNC_STATE_CANDIDATE) {
    sTrace(
        "syncNodeOnAppendEntriesCb --> return to follower, pMsg->term:%lu, ths->pRaftStore->currentTerm:%lu, "
        "ths->state:%d, logOK:%d",
        pMsg->term, ths->pRaftStore->currentTerm, ths->state, logOK);

    syncNodeBecomeFollower(ths, "from candidate by append entries");

    // ret or reply?
    return ret;
  }

  // accept request
  if (pMsg->term == ths->pRaftStore->currentTerm && ths->state == TAOS_SYNC_STATE_FOLLOWER && logOK) {
    // preIndex = -1, or has preIndex entry in local log
    ASSERT(pMsg->prevLogIndex <= ths->pLogStore->getLastIndex(ths->pLogStore));

    // has extra entries (> preIndex) in local log
    bool hasExtraEntries = pMsg->prevLogIndex < ths->pLogStore->getLastIndex(ths->pLogStore);

    // has entries in SyncAppendEntries msg
    bool hasAppendEntries = pMsg->dataLen > 0;

    sTrace(
        "syncNodeOnAppendEntriesCb --> accept, pMsg->term:%lu, ths->pRaftStore->currentTerm:%lu, ths->state:%d, "
        "logOK:%d, hasExtraEntries:%d, hasAppendEntries:%d",
        pMsg->term, ths->pRaftStore->currentTerm, ths->state, logOK, hasExtraEntries, hasAppendEntries);

    if (hasExtraEntries && hasAppendEntries) {
      // not conflict by default
      bool conflict = false;

      SyncIndex       extraIndex = pMsg->prevLogIndex + 1;
      SSyncRaftEntry* pExtraEntry = ths->pLogStore->getEntry(ths->pLogStore, extraIndex);
      if (pExtraEntry == NULL) {
        char logBuf[128];
        snprintf(logBuf, sizeof(logBuf), "getEntry error2, index:%ld, since %s", extraIndex, terrstr());
        syncNodeErrorLog(ths, logBuf);
        return -1;
      }

      SSyncRaftEntry* pAppendEntry = syncEntryDeserialize(pMsg->data, pMsg->dataLen);
      if (pAppendEntry == NULL) {
        syncNodeErrorLog(ths, "syncEntryDeserialize pAppendEntry error");
        return -1;
      }

      // log not match, conflict
      ASSERT(extraIndex == pAppendEntry->index);
      if (pExtraEntry->term != pAppendEntry->term) {
        conflict = true;
      }

      if (conflict) {
        // roll back
        SyncIndex delBegin = ths->pLogStore->getLastIndex(ths->pLogStore);
        SyncIndex delEnd = extraIndex;

        sTrace("syncNodeOnAppendEntriesCb --> conflict:%d, delBegin:%ld, delEnd:%ld", conflict, delBegin, delEnd);

        // notice! reverse roll back!
        for (SyncIndex index = delEnd; index >= delBegin; --index) {
          if (ths->pFsm->FpRollBackCb != NULL) {
            SSyncRaftEntry* pRollBackEntry = ths->pLogStore->getEntry(ths->pLogStore, index);
            if (pRollBackEntry == NULL) {
              char logBuf[128];
              snprintf(logBuf, sizeof(logBuf), "getEntry error3, index:%ld, since %s", index, terrstr());
              syncNodeErrorLog(ths, logBuf);
              return -1;
            }

            // if (pRollBackEntry->msgType != TDMT_SYNC_NOOP) {
            if (syncUtilUserRollback(pRollBackEntry->msgType)) {
              SRpcMsg rpcMsg;
              syncEntry2OriginalRpc(pRollBackEntry, &rpcMsg);

              SFsmCbMeta cbMeta = {0};
              cbMeta.index = pRollBackEntry->index;
              cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
              cbMeta.isWeak = pRollBackEntry->isWeak;
              cbMeta.code = 0;
              cbMeta.state = ths->state;
              cbMeta.seqNum = pRollBackEntry->seqNum;
              ths->pFsm->FpRollBackCb(ths->pFsm, &rpcMsg, cbMeta);
              rpcFreeCont(rpcMsg.pCont);
            }

            syncEntryDestory(pRollBackEntry);
          }
        }

        // delete confict entries
        ths->pLogStore->truncate(ths->pLogStore, extraIndex);

        // append new entries
        ths->pLogStore->appendEntry(ths->pLogStore, pAppendEntry);

        // pre commit
        SRpcMsg rpcMsg;
        syncEntry2OriginalRpc(pAppendEntry, &rpcMsg);
        if (ths->pFsm != NULL) {
          // if (ths->pFsm->FpPreCommitCb != NULL && pAppendEntry->originalRpcType != TDMT_SYNC_NOOP) {
          if (ths->pFsm->FpPreCommitCb != NULL && syncUtilUserPreCommit(pAppendEntry->originalRpcType)) {
            SFsmCbMeta cbMeta = {0};
            cbMeta.index = pAppendEntry->index;
            cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
            cbMeta.isWeak = pAppendEntry->isWeak;
            cbMeta.code = 2;
            cbMeta.state = ths->state;
            cbMeta.seqNum = pAppendEntry->seqNum;
            ths->pFsm->FpPreCommitCb(ths->pFsm, &rpcMsg, cbMeta);
          }
        }
        rpcFreeCont(rpcMsg.pCont);
      }

      // free memory
      syncEntryDestory(pExtraEntry);
      syncEntryDestory(pAppendEntry);

    } else if (hasExtraEntries && !hasAppendEntries) {
      // do nothing

    } else if (!hasExtraEntries && hasAppendEntries) {
      SSyncRaftEntry* pAppendEntry = syncEntryDeserialize(pMsg->data, pMsg->dataLen);
      if (pAppendEntry == NULL) {
        syncNodeErrorLog(ths, "syncEntryDeserialize pAppendEntry2 error");
        return -1;
      }

      // append new entries
      ths->pLogStore->appendEntry(ths->pLogStore, pAppendEntry);

      // pre commit
      SRpcMsg rpcMsg;
      syncEntry2OriginalRpc(pAppendEntry, &rpcMsg);
      if (ths->pFsm != NULL) {
        // if (ths->pFsm->FpPreCommitCb != NULL && pAppendEntry->originalRpcType != TDMT_SYNC_NOOP) {
        if (ths->pFsm->FpPreCommitCb != NULL && syncUtilUserPreCommit(pAppendEntry->originalRpcType)) {
          SFsmCbMeta cbMeta = {0};
          cbMeta.index = pAppendEntry->index;
          cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
          cbMeta.isWeak = pAppendEntry->isWeak;
          cbMeta.code = 3;
          cbMeta.state = ths->state;
          cbMeta.seqNum = pAppendEntry->seqNum;
          ths->pFsm->FpPreCommitCb(ths->pFsm, &rpcMsg, cbMeta);
        }
      }
      rpcFreeCont(rpcMsg.pCont);

      // free memory
      syncEntryDestory(pAppendEntry);

    } else if (!hasExtraEntries && !hasAppendEntries) {
      // do nothing

    } else {
      syncNodeLog3("", ths);
      ASSERT(0);
    }

    SyncAppendEntriesReply* pReply = syncAppendEntriesReplyBuild(ths->vgId);
    pReply->srcId = ths->myRaftId;
    pReply->destId = pMsg->srcId;
    pReply->term = ths->pRaftStore->currentTerm;
    pReply->success = true;

    if (hasAppendEntries) {
      pReply->matchIndex = pMsg->prevLogIndex + 1;
    } else {
      pReply->matchIndex = pMsg->prevLogIndex;
    }

    SRpcMsg rpcMsg;
    syncAppendEntriesReply2RpcMsg(pReply, &rpcMsg);
    syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
    syncAppendEntriesReplyDestroy(pReply);

    // maybe update commit index from leader
    if (pMsg->commitIndex > ths->commitIndex) {
      // has commit entry in local
      if (pMsg->commitIndex <= ths->pLogStore->getLastIndex(ths->pLogStore)) {
        SyncIndex beginIndex = ths->commitIndex + 1;
        SyncIndex endIndex = pMsg->commitIndex;

        // update commit index
        ths->commitIndex = pMsg->commitIndex;

        // call back Wal
        ths->pLogStore->updateCommitIndex(ths->pLogStore, ths->commitIndex);

        int32_t code = syncNodeCommit(ths, beginIndex, endIndex, ths->state);
        ASSERT(code == 0);
      }
    }
  }

  return ret;
}

#endif

static int32_t syncNodeMakeLogSame(SSyncNode* ths, SyncAppendEntries* pMsg) {
  int32_t code;

  SyncIndex delBegin = pMsg->prevLogIndex + 1;
  SyncIndex delEnd = ths->pLogStore->syncLogLastIndex(ths->pLogStore);

  // invert roll back!
  for (SyncIndex index = delEnd; index >= delBegin; --index) {
    if (ths->pFsm->FpRollBackCb != NULL) {
      SSyncRaftEntry* pRollBackEntry;
      code = ths->pLogStore->syncLogGetEntry(ths->pLogStore, index, &pRollBackEntry);
      ASSERT(code == 0);
      ASSERT(pRollBackEntry != NULL);

      if (syncUtilUserRollback(pRollBackEntry->msgType)) {
        SRpcMsg rpcMsg;
        syncEntry2OriginalRpc(pRollBackEntry, &rpcMsg);

        SFsmCbMeta cbMeta = {0};
        cbMeta.index = pRollBackEntry->index;
        cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
        cbMeta.isWeak = pRollBackEntry->isWeak;
        cbMeta.code = 0;
        cbMeta.state = ths->state;
        cbMeta.seqNum = pRollBackEntry->seqNum;
        ths->pFsm->FpRollBackCb(ths->pFsm, &rpcMsg, cbMeta);
        rpcFreeCont(rpcMsg.pCont);
      }

      syncEntryDestory(pRollBackEntry);
    }
  }

  // delete confict entries
  code = ths->pLogStore->syncLogTruncate(ths->pLogStore, delBegin);
  ASSERT(code == 0);

  char eventLog[128];
  snprintf(eventLog, sizeof(eventLog), "log truncate, from %ld to %ld", delBegin, delEnd);
  syncNodeEventLog(ths, eventLog);
  logStoreSimpleLog2("after syncNodeMakeLogSame", ths->pLogStore);

  return code;
}

static int32_t syncNodePreCommit(SSyncNode* ths, SSyncRaftEntry* pEntry) {
  SRpcMsg rpcMsg;
  syncEntry2OriginalRpc(pEntry, &rpcMsg);
  if (ths->pFsm != NULL) {
    if (ths->pFsm->FpPreCommitCb != NULL && syncUtilUserPreCommit(pEntry->originalRpcType)) {
      SFsmCbMeta cbMeta = {0};
      cbMeta.index = pEntry->index;
      cbMeta.lastConfigIndex = syncNodeGetSnapshotConfigIndex(ths, cbMeta.index);
      cbMeta.isWeak = pEntry->isWeak;
      cbMeta.code = 2;
      cbMeta.state = ths->state;
      cbMeta.seqNum = pEntry->seqNum;
      ths->pFsm->FpPreCommitCb(ths->pFsm, &rpcMsg, cbMeta);
    }
  }
  rpcFreeCont(rpcMsg.pCont);
  return 0;
}

// really pre log match
// prevLogIndex == -1
static bool syncNodeOnAppendEntriesLogOK(SSyncNode* pSyncNode, SyncAppendEntries* pMsg) {
  if (pMsg->prevLogIndex == SYNC_INDEX_INVALID) {
    return true;
  }

  SyncIndex myLastIndex = syncNodeGetLastIndex(pSyncNode);
  if (pMsg->prevLogIndex > myLastIndex) {
    sDebug("vgId:%d sync log not ok, preindex:%ld", pSyncNode->vgId, pMsg->prevLogIndex);
    return false;
  }

  SyncTerm myPreLogTerm = syncNodeGetPreTerm(pSyncNode, pMsg->prevLogIndex + 1);
  if (myPreLogTerm == SYNC_TERM_INVALID) {
    sDebug("vgId:%d sync log not ok2, preindex:%ld", pSyncNode->vgId, pMsg->prevLogIndex);
    return false;
  }

  if (pMsg->prevLogIndex <= myLastIndex && pMsg->prevLogTerm == myPreLogTerm) {
    return true;
  }

  sDebug("vgId:%d sync log not ok3, preindex:%ld", pSyncNode->vgId, pMsg->prevLogIndex);
  return false;
}

int32_t syncNodeOnAppendEntriesSnapshot2Cb(SSyncNode* ths, SyncAppendEntriesBatch* pMsg) { return 0; }

int32_t syncNodeOnAppendEntriesSnapshotCb(SSyncNode* ths, SyncAppendEntries* pMsg) {
  int32_t ret = 0;
  int32_t code = 0;

  // print log
  syncAppendEntriesLog2("==syncNodeOnAppendEntriesSnapshotCb==", pMsg);

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId)) && !ths->pRaftCfg->isStandBy) {
    syncNodeEventLog(ths, "recv sync-append-entries, maybe replica already dropped");
    return ret;
  }

  // maybe update term
  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncNodeUpdateTerm(ths, pMsg->term);
  }
  ASSERT(pMsg->term <= ths->pRaftStore->currentTerm);

  // reset elect timer
  if (pMsg->term == ths->pRaftStore->currentTerm) {
    ths->leaderCache = pMsg->srcId;
    syncNodeResetElectTimer(ths);
  }
  ASSERT(pMsg->dataLen >= 0);

  // candidate to follower
  //
  // operation:
  // to follower
  do {
    bool condition = pMsg->term == ths->pRaftStore->currentTerm && ths->state == TAOS_SYNC_STATE_CANDIDATE;
    if (condition) {
      syncNodeEventLog(ths, "recv sync-append-entries, candidate to follower");

      syncNodeBecomeFollower(ths, "from candidate by append entries");
      // do not reply?
      return ret;
    }
  } while (0);

#if 0
  // fake match
  //
  // condition1:
  // I have snapshot, no log, preIndex > myLastIndex
  //
  // condition2:
  // I have snapshot, have log, log <= snapshot, preIndex > myLastIndex
  //
  // condition3:
  // I have snapshot, preIndex < snapshot.lastApplyIndex
  //
  // condition4:
  // I have snapshot, preIndex == snapshot.lastApplyIndex, no data
  //
  // operation:
  // match snapshot.lastApplyIndex - 1;
  // no operation on log
  do {
    SyncIndex myLastIndex = syncNodeGetLastIndex(ths);
    SSnapshot snapshot;
    ths->pFsm->FpGetSnapshotInfo(ths->pFsm, &snapshot);

    bool condition0 = (pMsg->term == ths->pRaftStore->currentTerm) && (ths->state == TAOS_SYNC_STATE_FOLLOWER) &&
                      syncNodeHasSnapshot(ths);
    bool condition1 =
        condition0 && (ths->pLogStore->syncLogEntryCount(ths->pLogStore) == 0) && (pMsg->prevLogIndex > myLastIndex);   // donot use syncLogEntryCount!!! use isEmpty
    bool condition2 = condition0 && (ths->pLogStore->syncLogLastIndex(ths->pLogStore) <= snapshot.lastApplyIndex) &&
                      (pMsg->prevLogIndex > myLastIndex);
    bool condition3 = condition0 && (pMsg->prevLogIndex < snapshot.lastApplyIndex);
    bool condition4 = condition0 && (pMsg->prevLogIndex == snapshot.lastApplyIndex) && (pMsg->dataLen == 0);
    bool condition = condition1 || condition2 || condition3 || condition4;

    if (condition) {
      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf), "recv sync-append-entries, fake match, pre-index:%ld, pre-term:%lu",
               pMsg->prevLogIndex, pMsg->prevLogTerm);
      syncNodeEventLog(ths, logBuf);

      // prepare response msg
      SyncAppendEntriesReply* pReply = syncAppendEntriesReplyBuild(ths->vgId);
      pReply->srcId = ths->myRaftId;
      pReply->destId = pMsg->srcId;
      pReply->term = ths->pRaftStore->currentTerm;
      pReply->privateTerm = ths->pNewNodeReceiver->privateTerm;
      pReply->success = true;
      pReply->matchIndex = snapshot.lastApplyIndex;

      // send response
      SRpcMsg rpcMsg;
      syncAppendEntriesReply2RpcMsg(pReply, &rpcMsg);
      syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
      syncAppendEntriesReplyDestroy(pReply);

      return ret;
    }
  } while (0);
#endif

  // fake match2
  //
  // condition1:
  // preIndex <= my commit index
  //
  // operation:
  // if hasAppendEntries && pMsg->prevLogIndex == ths->commitIndex, append entry
  // match my-commit-index or my-commit-index + 1
  // no operation on log
  do {
    bool condition = (pMsg->term == ths->pRaftStore->currentTerm) && (ths->state == TAOS_SYNC_STATE_FOLLOWER) &&
                     (pMsg->prevLogIndex <= ths->commitIndex);
    if (condition) {
      do {
        char logBuf[128];
        snprintf(logBuf, sizeof(logBuf),
                 "recv sync-append-entries, fake match2, pre-index:%ld, pre-term:%lu, datalen:%d", pMsg->prevLogIndex,
                 pMsg->prevLogTerm, pMsg->dataLen);
        syncNodeEventLog(ths, logBuf);
      } while (0);

      SyncIndex matchIndex = ths->commitIndex;
      bool      hasAppendEntries = pMsg->dataLen > 0;
      if (hasAppendEntries && pMsg->prevLogIndex == ths->commitIndex) {
        // append entry
        SSyncRaftEntry* pAppendEntry = syncEntryDeserialize(pMsg->data, pMsg->dataLen);
        ASSERT(pAppendEntry != NULL);

        {
          // has extra entries (> preIndex) in local log
          SyncIndex logLastIndex = ths->pLogStore->syncLogLastIndex(ths->pLogStore);
          bool      hasExtraEntries = logLastIndex > pMsg->prevLogIndex;

          if (hasExtraEntries) {
            // make log same, rollback deleted entries
            code = syncNodeMakeLogSame(ths, pMsg);
            ASSERT(code == 0);
          }
        }

        code = ths->pLogStore->syncLogAppendEntry(ths->pLogStore, pAppendEntry);
        if (code != 0) {
          return -1;
        }

        // pre commit
        code = syncNodePreCommit(ths, pAppendEntry);
        ASSERT(code == 0);

        // update match index
        matchIndex = pMsg->prevLogIndex + 1;

        syncEntryDestory(pAppendEntry);
      }

      // prepare response msg
      SyncAppendEntriesReply* pReply = syncAppendEntriesReplyBuild(ths->vgId);
      pReply->srcId = ths->myRaftId;
      pReply->destId = pMsg->srcId;
      pReply->term = ths->pRaftStore->currentTerm;
      pReply->privateTerm = ths->pNewNodeReceiver->privateTerm;
      pReply->success = true;
      pReply->matchIndex = matchIndex;

      // send response
      SRpcMsg rpcMsg;
      syncAppendEntriesReply2RpcMsg(pReply, &rpcMsg);
      syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
      syncAppendEntriesReplyDestroy(pReply);

      return ret;
    }
  } while (0);

  // calculate logOK here, before will coredump, due to fake match
  bool logOK = syncNodeOnAppendEntriesLogOK(ths, pMsg);

  // not match
  //
  // condition1:
  // term < myTerm
  //
  // condition2:
  // !logOK
  //
  // operation:
  // not match
  // no operation on log
  do {
    bool condition1 = pMsg->term < ths->pRaftStore->currentTerm;
    bool condition2 =
        (pMsg->term == ths->pRaftStore->currentTerm) && (ths->state == TAOS_SYNC_STATE_FOLLOWER) && !logOK;
    bool condition = condition1 || condition2;

    if (condition) {
      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf), "recv sync-append-entries, not match, pre-index:%ld, pre-term:%lu, datalen:%d",
               pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->dataLen);
      syncNodeEventLog(ths, logBuf);

      // prepare response msg
      SyncAppendEntriesReply* pReply = syncAppendEntriesReplyBuild(ths->vgId);
      pReply->srcId = ths->myRaftId;
      pReply->destId = pMsg->srcId;
      pReply->term = ths->pRaftStore->currentTerm;
      pReply->privateTerm = ths->pNewNodeReceiver->privateTerm;
      pReply->success = false;
      pReply->matchIndex = SYNC_INDEX_INVALID;

      // send response
      SRpcMsg rpcMsg;
      syncAppendEntriesReply2RpcMsg(pReply, &rpcMsg);
      syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
      syncAppendEntriesReplyDestroy(pReply);

      return ret;
    }
  } while (0);

  // really match
  //
  // condition:
  // logOK
  //
  // operation:
  // match
  // make log same
  do {
    bool condition = (pMsg->term == ths->pRaftStore->currentTerm) && (ths->state == TAOS_SYNC_STATE_FOLLOWER) && logOK;
    if (condition) {
      // has extra entries (> preIndex) in local log
      SyncIndex myLastIndex = syncNodeGetLastIndex(ths);
      bool      hasExtraEntries = myLastIndex > pMsg->prevLogIndex;

      // has entries in SyncAppendEntries msg
      bool hasAppendEntries = pMsg->dataLen > 0;

      char logBuf[128];
      snprintf(logBuf, sizeof(logBuf), "recv sync-append-entries, match, pre-index:%ld, pre-term:%lu, datalen:%d",
               pMsg->prevLogIndex, pMsg->prevLogTerm, pMsg->dataLen);
      syncNodeEventLog(ths, logBuf);

      if (hasExtraEntries) {
        // make log same, rollback deleted entries
        code = syncNodeMakeLogSame(ths, pMsg);
        ASSERT(code == 0);
      }

      if (hasAppendEntries) {
        // append entry
        SSyncRaftEntry* pAppendEntry = syncEntryDeserialize(pMsg->data, pMsg->dataLen);
        ASSERT(pAppendEntry != NULL);

        code = ths->pLogStore->syncLogAppendEntry(ths->pLogStore, pAppendEntry);
        if (code != 0) {
          return -1;
        }

        // pre commit
        code = syncNodePreCommit(ths, pAppendEntry);
        ASSERT(code == 0);

        syncEntryDestory(pAppendEntry);
      }

      // prepare response msg
      SyncAppendEntriesReply* pReply = syncAppendEntriesReplyBuild(ths->vgId);
      pReply->srcId = ths->myRaftId;
      pReply->destId = pMsg->srcId;
      pReply->term = ths->pRaftStore->currentTerm;
      pReply->privateTerm = ths->pNewNodeReceiver->privateTerm;
      pReply->success = true;
      pReply->matchIndex = hasAppendEntries ? pMsg->prevLogIndex + 1 : pMsg->prevLogIndex;

      // send response
      SRpcMsg rpcMsg;
      syncAppendEntriesReply2RpcMsg(pReply, &rpcMsg);
      syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
      syncAppendEntriesReplyDestroy(pReply);

      // maybe update commit index, leader notice me
      if (pMsg->commitIndex > ths->commitIndex) {
        // has commit entry in local
        if (pMsg->commitIndex <= ths->pLogStore->syncLogLastIndex(ths->pLogStore)) {
          // advance commit index to sanpshot first
          SSnapshot snapshot;
          ths->pFsm->FpGetSnapshotInfo(ths->pFsm, &snapshot);
          if (snapshot.lastApplyIndex >= 0 && snapshot.lastApplyIndex > ths->commitIndex) {
            SyncIndex commitBegin = ths->commitIndex;
            SyncIndex commitEnd = snapshot.lastApplyIndex;
            ths->commitIndex = snapshot.lastApplyIndex;

            char eventLog[128];
            snprintf(eventLog, sizeof(eventLog), "commit by snapshot from index:%ld to index:%ld", commitBegin,
                     commitEnd);
            syncNodeEventLog(ths, eventLog);
          }

          SyncIndex beginIndex = ths->commitIndex + 1;
          SyncIndex endIndex = pMsg->commitIndex;

          // update commit index
          ths->commitIndex = pMsg->commitIndex;

          // call back Wal
          code = ths->pLogStore->updateCommitIndex(ths->pLogStore, ths->commitIndex);
          ASSERT(code == 0);

          code = syncNodeCommit(ths, beginIndex, endIndex, ths->state);
          ASSERT(code == 0);
        }
      }
      return ret;
    }
  } while (0);

  return ret;
}
