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
  syncAppendEntriesLog2("==syncNodeOnAppendEntriesCb==", pMsg);

  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncNodeUpdateTerm(ths, pMsg->term);
  }
  assert(pMsg->term <= ths->pRaftStore->currentTerm);

  if (pMsg->term == ths->pRaftStore->currentTerm) {
    ths->leaderCache = pMsg->srcId;
    syncNodeResetElectTimer(ths);
  }
  assert(pMsg->dataLen >= 0);

  SyncTerm localPreLogTerm = 0;
  if (pMsg->prevLogTerm >= SYNC_INDEX_BEGIN && pMsg->prevLogTerm <= ths->pLogStore->getLastIndex(ths->pLogStore)) {
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

    // need ret?
    return ret;
  }

  // accept request
  if (pMsg->term == ths->pRaftStore->currentTerm && ths->state == TAOS_SYNC_STATE_FOLLOWER && logOK) {
    bool matchSuccess = false;
    if (pMsg->prevLogIndex == SYNC_INDEX_INVALID &&
        ths->pLogStore->getLastIndex(ths->pLogStore) == SYNC_INDEX_INVALID) {
      matchSuccess = true;
    }
    if (pMsg->prevLogIndex >= SYNC_INDEX_BEGIN && pMsg->prevLogIndex <= ths->pLogStore->getLastIndex(ths->pLogStore)) {
      SSyncRaftEntry* pPreEntry = logStoreGetEntry(ths->pLogStore, pMsg->prevLogIndex);
      assert(pPreEntry != NULL);
      if (pMsg->prevLogTerm == pPreEntry->term) {
        matchSuccess = true;
      }
      syncEntryDestory(pPreEntry);
    }

    if (matchSuccess) {
      // delete conflict entries
      if (pMsg->prevLogIndex < ths->pLogStore->getLastIndex(ths->pLogStore)) {
        SyncIndex fromIndex = pMsg->prevLogIndex + 1;
        ths->pLogStore->truncate(ths->pLogStore, fromIndex);
      }

      // append one entry
      if (pMsg->dataLen > 0) {
        SSyncRaftEntry* pEntry = syncEntryDeserialize(pMsg->data, pMsg->dataLen);
        ths->pLogStore->appendEntry(ths->pLogStore, pEntry);
        syncEntryDestory(pEntry);
      }

      SyncAppendEntriesReply* pReply = syncAppendEntriesReplyBuild();
      pReply->srcId = ths->myRaftId;
      pReply->destId = pMsg->srcId;
      pReply->term = ths->pRaftStore->currentTerm;
      pReply->success = true;
      pReply->matchIndex = pMsg->prevLogIndex + 1;

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

    if (pMsg->commitIndex > ths->commitIndex) {
      if (pMsg->commitIndex <= ths->pLogStore->getLastIndex(ths->pLogStore)) {
        // commit
        ths->commitIndex = pMsg->commitIndex;
        ths->pLogStore->updateCommitIndex(ths->pLogStore, ths->commitIndex);
      }
    }
  }

  return ret;
}
