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

  char logBuf[128] = {0};
  snprintf(logBuf, sizeof(logBuf), "==syncNodeOnAppendEntriesCb== term:%lu", ths->pRaftStore->currentTerm);
  syncAppendEntriesLog2(logBuf, pMsg);

  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncNodeUpdateTerm(ths, pMsg->term);
  }
  assert(pMsg->term <= ths->pRaftStore->currentTerm);

  // reset elect timer
  if (pMsg->term == ths->pRaftStore->currentTerm) {
    ths->leaderCache = pMsg->srcId;
    syncNodeResetElectTimer(ths);
  }
  assert(pMsg->dataLen >= 0);

  SyncTerm localPreLogTerm = 0;
  if (pMsg->prevLogIndex >= SYNC_INDEX_BEGIN && pMsg->prevLogIndex <= ths->pLogStore->getLastIndex(ths->pLogStore)) {
    SSyncRaftEntry* pEntry = ths->pLogStore->getEntry(ths->pLogStore, pMsg->prevLogIndex);
    assert(pEntry != NULL);
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
    assert(pMsg->prevLogIndex <= ths->pLogStore->getLastIndex(ths->pLogStore));

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
      assert(pExtraEntry != NULL);

      SSyncRaftEntry* pAppendEntry = syncEntryDeserialize(pMsg->data, pMsg->dataLen);
      assert(pAppendEntry != NULL);

      // log not match, conflict
      assert(extraIndex == pAppendEntry->index);
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
            assert(pRollBackEntry != NULL);

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
      assert(pAppendEntry != NULL);

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
      assert(0);
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
  assert(pMsg->term <= ths->pRaftStore->currentTerm);

  // reset elect timer
  if (pMsg->term == ths->pRaftStore->currentTerm) {
    ths->leaderCache = pMsg->srcId;
    syncNodeResetElectTimer(ths);
  }
  assert(pMsg->dataLen >= 0);

  SyncTerm localPreLogTerm = 0;
  if (pMsg->prevLogIndex >= SYNC_INDEX_BEGIN && pMsg->prevLogIndex <= ths->pLogStore->getLastIndex(ths->pLogStore)) {
    SSyncRaftEntry* pEntry = ths->pLogStore->getEntry(ths->pLogStore, pMsg->prevLogIndex);
    assert(pEntry != NULL);
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
    assert(pMsg->prevLogIndex <= ths->pLogStore->getLastIndex(ths->pLogStore));

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
      assert(pExtraEntry != NULL);

      SSyncRaftEntry* pAppendEntry = syncEntryDeserialize(pMsg->data, pMsg->dataLen);
      assert(pAppendEntry != NULL);

      // log not match, conflict
      assert(extraIndex == pAppendEntry->index);
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
            assert(pRollBackEntry != NULL);

            // if (pRollBackEntry->msgType != TDMT_SYNC_NOOP) {
            if (syncUtilUserRollback(pRollBackEntry->msgType)) {
              SRpcMsg rpcMsg;
              syncEntry2OriginalRpc(pRollBackEntry, &rpcMsg);

              SFsmCbMeta cbMeta;
              cbMeta.index = pRollBackEntry->index;
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
            SFsmCbMeta cbMeta;
            cbMeta.index = pAppendEntry->index;
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
      assert(pAppendEntry != NULL);

      // append new entries
      ths->pLogStore->appendEntry(ths->pLogStore, pAppendEntry);

      // pre commit
      SRpcMsg rpcMsg;
      syncEntry2OriginalRpc(pAppendEntry, &rpcMsg);
      if (ths->pFsm != NULL) {
        // if (ths->pFsm->FpPreCommitCb != NULL && pAppendEntry->originalRpcType != TDMT_SYNC_NOOP) {
        if (ths->pFsm->FpPreCommitCb != NULL && syncUtilUserPreCommit(pAppendEntry->originalRpcType)) {
          SFsmCbMeta cbMeta;
          cbMeta.index = pAppendEntry->index;
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
      assert(0);
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

        // execute fsm
        if (ths->pFsm != NULL) {
          for (SyncIndex i = beginIndex; i <= endIndex; ++i) {
            if (i != SYNC_INDEX_INVALID) {
              SSyncRaftEntry* pEntry = ths->pLogStore->getEntry(ths->pLogStore, i);
              assert(pEntry != NULL);

              SRpcMsg rpcMsg;
              syncEntry2OriginalRpc(pEntry, &rpcMsg);

              if (ths->pFsm->FpCommitCb != NULL && syncUtilUserCommit(pEntry->originalRpcType)) {
                SFsmCbMeta cbMeta;
                cbMeta.index = pEntry->index;
                cbMeta.isWeak = pEntry->isWeak;
                cbMeta.code = 0;
                cbMeta.state = ths->state;
                cbMeta.seqNum = pEntry->seqNum;
                cbMeta.term = pEntry->term;
                cbMeta.currentTerm = ths->pRaftStore->currentTerm;
                cbMeta.flag = 0x11;

                SSnapshot snapshot;
                ASSERT(ths->pFsm->FpGetSnapshot != NULL);
                ths->pFsm->FpGetSnapshot(ths->pFsm, &snapshot);

                bool needExecute = true;
                if (cbMeta.index <= snapshot.lastApplyIndex) {
                  needExecute = false;
                }

                if (needExecute) {
                  ths->pFsm->FpCommitCb(ths->pFsm, &rpcMsg, cbMeta);
                }
              }

              // config change
              if (pEntry->originalRpcType == TDMT_SYNC_CONFIG_CHANGE) {
                SSyncCfg oldSyncCfg = ths->pRaftCfg->cfg;

                SSyncCfg newSyncCfg;
                int32_t  ret = syncCfgFromStr(rpcMsg.pCont, &newSyncCfg);
                ASSERT(ret == 0);

                // update new config myIndex
                bool hit = false;
                for (int i = 0; i < newSyncCfg.replicaNum; ++i) {
                  if (strcmp(ths->myNodeInfo.nodeFqdn, (newSyncCfg.nodeInfo)[i].nodeFqdn) == 0 &&
                      ths->myNodeInfo.nodePort == (newSyncCfg.nodeInfo)[i].nodePort) {
                    newSyncCfg.myIndex = i;
                    hit = true;
                    break;
                  }
                }

                SReConfigCbMeta cbMeta = {0};
                bool            isDrop;

                // I am in newConfig
                if (hit) {
                  syncNodeUpdateConfig(ths, &newSyncCfg, pEntry->index, &isDrop);

                  // change isStandBy to normal
                  if (!isDrop) {
                    if (ths->state == TAOS_SYNC_STATE_LEADER) {
                      syncNodeBecomeLeader(ths, "config change");
                    } else {
                      syncNodeBecomeFollower(ths, "config change");
                    }
                  }

                  if (gRaftDetailLog) {
                    char* sOld = syncCfg2Str(&oldSyncCfg);
                    char* sNew = syncCfg2Str(&newSyncCfg);
                    sInfo("==config change== 0x11 old:%s new:%s isDrop:%d \n", sOld, sNew, isDrop);
                    taosMemoryFree(sOld);
                    taosMemoryFree(sNew);
                  }
                }

                // always call FpReConfigCb
                if (ths->pFsm->FpReConfigCb != NULL) {
                  cbMeta.code = 0;
                  cbMeta.currentTerm = ths->pRaftStore->currentTerm;
                  cbMeta.index = pEntry->index;
                  cbMeta.term = pEntry->term;
                  cbMeta.newCfg = newSyncCfg;
                  cbMeta.oldCfg = oldSyncCfg;
                  cbMeta.seqNum = pEntry->seqNum;
                  cbMeta.flag = 0x11;
                  cbMeta.isDrop = isDrop;
                  ths->pFsm->FpReConfigCb(ths->pFsm, &rpcMsg, cbMeta);
                }
              }

              // restore finish
              if (pEntry->index == ths->pLogStore->getLastIndex(ths->pLogStore)) {
                if (ths->restoreFinish == false) {
                  if (ths->pFsm->FpRestoreFinishCb != NULL) {
                    ths->pFsm->FpRestoreFinishCb(ths->pFsm);
                  }
                  ths->restoreFinish = true;
                  sInfo("==syncNodeOnAppendEntriesCb== restoreFinish set true %p vgId:%d", ths, ths->vgId);

                  /*
                  tsem_post(&ths->restoreSem);
                  sInfo("==syncNodeOnAppendEntriesCb== RestoreFinish tsem_post %p", ths);
                  */
                }
              }

              rpcFreeCont(rpcMsg.pCont);
              syncEntryDestory(pEntry);
            }
          }
        }
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
  sDebug("vgId:%d sync event %s commitIndex:%ld currentTerm:%lu log truncate, from %ld to %ld", ths->vgId,
         syncUtilState2String(ths->state), ths->commitIndex, ths->pRaftStore->currentTerm, delBegin, delEnd);
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
    if (gRaftDetailLog) {
      sTrace("syncNodeOnAppendEntriesLogOK true, pMsg->prevLogIndex:%ld", pMsg->prevLogIndex);
    }
    return true;
  }

  SyncIndex myLastIndex = syncNodeGetLastIndex(pSyncNode);
  if (pMsg->prevLogIndex > myLastIndex) {
    if (gRaftDetailLog) {
      sTrace("syncNodeOnAppendEntriesLogOK false, pMsg->prevLogIndex:%ld, myLastIndex:%ld", pMsg->prevLogIndex,
             myLastIndex);
    }
    return false;
  }

  SyncTerm myPreLogTerm = syncNodeGetPreTerm(pSyncNode, pMsg->prevLogIndex + 1);
  if (pMsg->prevLogIndex <= myLastIndex && pMsg->prevLogTerm == myPreLogTerm) {
    if (gRaftDetailLog) {
      sTrace(
          "syncNodeOnAppendEntriesLogOK true, pMsg->prevLogIndex:%ld, myLastIndex:%ld, pMsg->prevLogTerm:%lu, "
          "myPreLogTerm:%lu",
          pMsg->prevLogIndex, myLastIndex, pMsg->prevLogTerm, myPreLogTerm);
    }
    return true;
  }

  if (gRaftDetailLog) {
    sTrace(
        "syncNodeOnAppendEntriesLogOK false, pMsg->prevLogIndex:%ld, myLastIndex:%ld, pMsg->prevLogTerm:%lu, "
        "myPreLogTerm:%lu",
        pMsg->prevLogIndex, myLastIndex, pMsg->prevLogTerm, myPreLogTerm);
  }

  return false;
}

int32_t syncNodeOnAppendEntriesSnapshotCb(SSyncNode* ths, SyncAppendEntries* pMsg) {
  int32_t ret = 0;
  int32_t code = 0;

  // print log
  char logBuf[128] = {0};
  snprintf(logBuf, sizeof(logBuf), "recv SyncAppendEntries, vgId:%d, term:%lu", ths->vgId,
           ths->pRaftStore->currentTerm);
  syncAppendEntriesLog2(logBuf, pMsg);

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId)) && !ths->pRaftCfg->isStandBy) {
    sInfo("recv SyncAppendEntries maybe replica already dropped");
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
      sTrace("recv SyncAppendEntries, candidate to follower");

      syncNodeBecomeFollower(ths, "from candidate by append entries");
      // do not reply?
      return ret;
    }
  } while (0);

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
    ths->pFsm->FpGetSnapshot(ths->pFsm, &snapshot);

    bool condition0 = (pMsg->term == ths->pRaftStore->currentTerm) && (ths->state == TAOS_SYNC_STATE_FOLLOWER) &&
                      syncNodeHasSnapshot(ths);
    bool condition1 =
        condition0 && (ths->pLogStore->syncLogEntryCount(ths->pLogStore) == 0) && (pMsg->prevLogIndex > myLastIndex);
    bool condition2 = condition0 && (ths->pLogStore->syncLogLastIndex(ths->pLogStore) <= snapshot.lastApplyIndex) &&
                      (pMsg->prevLogIndex > myLastIndex);
    bool condition3 = condition0 && (pMsg->prevLogIndex < snapshot.lastApplyIndex);
    bool condition4 = condition0 && (pMsg->prevLogIndex == snapshot.lastApplyIndex) && (pMsg->dataLen == 0);
    bool condition = condition1 || condition2 || condition3 || condition4;

    if (condition) {
      sTrace(
          "recv SyncAppendEntries, fake match, myLastIndex:%ld, syncLogBeginIndex:%ld, syncLogEndIndex:%ld, "
          "condition1:%d, condition2:%d, condition3:%d, condition4:%d",
          myLastIndex, ths->pLogStore->syncLogBeginIndex(ths->pLogStore),
          ths->pLogStore->syncLogEndIndex(ths->pLogStore), condition1, condition2, condition3, condition4);

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
      sTrace("recv SyncAppendEntries, fake match2, msg-prevLogIndex:%ld, my-commitIndex:%ld", pMsg->prevLogIndex,
             ths->commitIndex);

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
        ASSERT(code == 0);

        // pre commit
        code = syncNodePreCommit(ths, pAppendEntry);
        ASSERT(code == 0);

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
      sTrace(
          "recv SyncAppendEntries, not match, syncLogBeginIndex:%ld, syncLogEndIndex:%ld, condition1:%d, "
          "condition2:%d, logOK:%d",
          ths->pLogStore->syncLogBeginIndex(ths->pLogStore), ths->pLogStore->syncLogEndIndex(ths->pLogStore),
          condition1, condition2, logOK);

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

      sTrace("recv SyncAppendEntries, match, myLastIndex:%ld, hasExtraEntries:%d, hasAppendEntries:%d", myLastIndex,
             hasExtraEntries, hasAppendEntries);

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
        ASSERT(code == 0);

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
          ths->pFsm->FpGetSnapshot(ths->pFsm, &snapshot);
          if (snapshot.lastApplyIndex >= 0 && snapshot.lastApplyIndex > ths->commitIndex) {
            SyncIndex commitBegin = ths->commitIndex;
            SyncIndex commitEnd = snapshot.lastApplyIndex;
            ths->commitIndex = snapshot.lastApplyIndex;

            sDebug(
                "vgId:%d sync event %s commitIndex:%ld currentTerm:%lu commit by snapshot from index:%ld to index:%ld",
                ths->vgId, syncUtilState2String(ths->state), ths->commitIndex, ths->pRaftStore->currentTerm,
                commitBegin, commitEnd);
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
