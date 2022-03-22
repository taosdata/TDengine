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
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"

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

  char logBuf[128];
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
    SSyncRaftEntry* pEntry = logStoreGetEntry(ths->pLogStore, pMsg->prevLogIndex);
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
    SyncAppendEntriesReply* pReply = syncAppendEntriesReplyBuild();
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
    syncNodeBecomeFollower(ths);

    // ret or reply?
    return ret;
  }

  // accept request
  if (pMsg->term == ths->pRaftStore->currentTerm && ths->state == TAOS_SYNC_STATE_FOLLOWER && logOK) {
    bool preMatch = false;
    if (pMsg->prevLogIndex == SYNC_INDEX_INVALID &&
        ths->pLogStore->getLastIndex(ths->pLogStore) == SYNC_INDEX_INVALID) {
      preMatch = true;
    }
    if (pMsg->prevLogIndex >= SYNC_INDEX_BEGIN && pMsg->prevLogIndex <= ths->pLogStore->getLastIndex(ths->pLogStore)) {
      SSyncRaftEntry* pPreEntry = logStoreGetEntry(ths->pLogStore, pMsg->prevLogIndex);
      assert(pPreEntry != NULL);
      if (pMsg->prevLogTerm == pPreEntry->term) {
        preMatch = true;
      }
      syncEntryDestory(pPreEntry);
    }

    if (preMatch) {
      // must has preIndex in local log
      assert(pMsg->prevLogIndex <= ths->pLogStore->getLastIndex(ths->pLogStore));

      bool hasExtraEntries = pMsg->prevLogIndex < ths->pLogStore->getLastIndex(ths->pLogStore);
      bool hasAppendEntries = pMsg->dataLen > 0;

      if (hasExtraEntries && hasAppendEntries) {
        // conflict
        bool conflict = false;

        SyncIndex       extraIndex = pMsg->prevLogIndex + 1;
        SSyncRaftEntry* pExtraEntry = logStoreGetEntry(ths->pLogStore, extraIndex);
        assert(pExtraEntry != NULL);

        SSyncRaftEntry* pAppendEntry = syncEntryDeserialize(pMsg->data, pMsg->dataLen);
        assert(pAppendEntry != NULL);

        assert(extraIndex == pAppendEntry->index);
        if (pExtraEntry->term == pAppendEntry->term) {
          conflict = true;
        }

        if (conflict) {
          // roll back
          SyncIndex delBegin = ths->pLogStore->getLastIndex(ths->pLogStore);
          SyncIndex delEnd = extraIndex;

          // notice! reverse roll back!
          for (SyncIndex index = delEnd; index >= delBegin; --index) {
            if (ths->pFsm->FpRollBackCb != NULL) {
              SSyncRaftEntry* pRollBackEntry = logStoreGetEntry(ths->pLogStore, index);
              assert(pRollBackEntry != NULL);

              SRpcMsg rpcMsg;
              syncEntry2OriginalRpc(pRollBackEntry, &rpcMsg);
              ths->pFsm->FpRollBackCb(ths->pFsm, &rpcMsg, pRollBackEntry->index, pRollBackEntry->isWeak, 0);
              rpcFreeCont(rpcMsg.pCont);
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
            if (ths->pFsm->FpPreCommitCb != NULL) {
              ths->pFsm->FpPreCommitCb(ths->pFsm, &rpcMsg, pAppendEntry->index, pAppendEntry->isWeak, 0);
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
          if (ths->pFsm->FpPreCommitCb != NULL) {
            ths->pFsm->FpPreCommitCb(ths->pFsm, &rpcMsg, pAppendEntry->index, pAppendEntry->isWeak, 0);
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

      SyncAppendEntriesReply* pReply = syncAppendEntriesReplyBuild();
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

    } else {
      SyncAppendEntriesReply* pReply = syncAppendEntriesReplyBuild();
      pReply->srcId = ths->myRaftId;
      pReply->destId = pMsg->srcId;
      pReply->term = ths->pRaftStore->currentTerm;
      pReply->success = false;
      pReply->matchIndex = SYNC_INDEX_INVALID;

      SRpcMsg rpcMsg;
      syncAppendEntriesReply2RpcMsg(pReply, &rpcMsg);
      syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
      syncAppendEntriesReplyDestroy(pReply);
    }

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

              if (ths->pFsm->FpCommitCb != NULL) {
                ths->pFsm->FpCommitCb(ths->pFsm, &rpcMsg, pEntry->index, pEntry->isWeak, 0);
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
