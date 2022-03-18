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

#include "syncRequestVote.h"
#include "syncInt.h"
#include "syncRaftStore.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"

// TLA+ Spec
// HandleRequestVoteRequest(i, j, m) ==
//    LET logOk == \/ m.mlastLogTerm > LastTerm(log[i])
//                 \/ /\ m.mlastLogTerm = LastTerm(log[i])
//                    /\ m.mlastLogIndex >= Len(log[i])
//        grant == /\ m.mterm = currentTerm[i]
//                 /\ logOk
//                 /\ votedFor[i] \in {Nil, j}
//    IN /\ m.mterm <= currentTerm[i]
//       /\ \/ grant  /\ votedFor' = [votedFor EXCEPT ![i] = j]
//          \/ ~grant /\ UNCHANGED votedFor
//       /\ Reply([mtype        |-> RequestVoteResponse,
//                 mterm        |-> currentTerm[i],
//                 mvoteGranted |-> grant,
//                 \* mlog is used just for the `elections' history variable for
//                 \* the proof. It would not exist in a real implementation.
//                 mlog         |-> log[i],
//                 msource      |-> i,
//                 mdest        |-> j],
//                 m)
//       /\ UNCHANGED <<state, currentTerm, candidateVars, leaderVars, logVars>>
//
int32_t syncNodeOnRequestVoteCb(SSyncNode* ths, SyncRequestVote* pMsg) {
  int32_t ret = 0;
  syncRequestVoteLog2("==syncNodeOnRequestVoteCb==", pMsg);

  if (pMsg->term > ths->pRaftStore->currentTerm) {
    syncNodeUpdateTerm(ths, pMsg->term);
  }
  assert(pMsg->term <= ths->pRaftStore->currentTerm);

  bool logOK = (pMsg->lastLogTerm > ths->pLogStore->getLastTerm(ths->pLogStore)) ||
               ((pMsg->lastLogTerm == ths->pLogStore->getLastTerm(ths->pLogStore)) &&
                (pMsg->lastLogIndex >= ths->pLogStore->getLastIndex(ths->pLogStore)));
  bool grant = (pMsg->term == ths->pRaftStore->currentTerm) && logOK &&
               ((!raftStoreHasVoted(ths->pRaftStore)) || (syncUtilSameId(&(ths->pRaftStore->voteFor), &(pMsg->srcId))));
  if (grant) {
    // maybe has already voted for pMsg->srcId
    // vote again, no harm
    raftStoreVote(ths->pRaftStore, &(pMsg->srcId));
  }

  SyncRequestVoteReply* pReply = syncRequestVoteReplyBuild();
  pReply->srcId = ths->myRaftId;
  pReply->destId = pMsg->srcId;
  pReply->term = ths->pRaftStore->currentTerm;
  pReply->voteGranted = grant;

  SRpcMsg rpcMsg;
  syncRequestVoteReply2RpcMsg(pReply, &rpcMsg);
  syncNodeSendMsgById(&pReply->destId, ths, &rpcMsg);
  syncRequestVoteReplyDestroy(pReply);

  return ret;
}
