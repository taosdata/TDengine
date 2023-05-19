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
#include "syncRequestVoteReply.h"
#include "syncMessage.h"
#include "syncRaftStore.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"

// TLA+ Spec
// HandleRequestVoteResponse(i, j, m) ==
//    \* This tallies votes even when the current state is not Candidate, but
//    \* they won't be looked at, so it doesn't matter.
//    /\ m.mterm = currentTerm[i]
//    /\ votesResponded' = [votesResponded EXCEPT ![i] =
//                              votesResponded[i] \cup {j}]
//    /\ \/ /\ m.mvoteGranted
//          /\ votesGranted' = [votesGranted EXCEPT ![i] =
//                                  votesGranted[i] \cup {j}]
//          /\ voterLog' = [voterLog EXCEPT ![i] =
//                              voterLog[i] @@ (j :> m.mlog)]
//       \/ /\ ~m.mvoteGranted
//          /\ UNCHANGED <<votesGranted, voterLog>>
//    /\ Discard(m)
//    /\ UNCHANGED <<serverVars, votedFor, leaderVars, logVars>>
//

int32_t syncNodeOnRequestVoteReply(SSyncNode* ths, const SRpcMsg* pRpcMsg) {
  int32_t               ret = 0;
  SyncRequestVoteReply* pMsg = pRpcMsg->pCont;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(ths, &(pMsg->srcId))) {
    syncLogRecvRequestVoteReply(ths, pMsg, "not in my config");
    return -1;
  }
  SyncTerm currentTerm = raftStoreGetTerm(ths);
  // drop stale response
  if (pMsg->term < currentTerm) {
    syncLogRecvRequestVoteReply(ths, pMsg, "drop stale response");
    return -1;
  }

  if (pMsg->term > currentTerm) {
    syncLogRecvRequestVoteReply(ths, pMsg, "error term");
    syncNodeStepDown(ths, pMsg->term);
    return -1;
  }

  syncLogRecvRequestVoteReply(ths, pMsg, "");
  ASSERT(pMsg->term == currentTerm);

  // This tallies votes even when the current state is not Candidate,
  // but they won't be looked at, so it doesn't matter.
  if (ths->state == TAOS_SYNC_STATE_CANDIDATE) {
    if (ths->pVotesRespond->term != pMsg->term) {
      sNError(ths, "vote respond error vote-respond-mgr term:%" PRIu64 ", msg term:%" PRIu64 "",
              ths->pVotesRespond->term, pMsg->term);
      return -1;
    }

    votesRespondAdd(ths->pVotesRespond, pMsg);
    if (pMsg->voteGranted) {
      // add vote
      voteGrantedVote(ths->pVotesGranted, pMsg);

      // maybe to leader
      if (voteGrantedMajority(ths->pVotesGranted)) {
        if (!ths->pVotesGranted->toLeader) {
          syncNodeCandidate2Leader(ths);

          // prevent to leader again!
          ths->pVotesGranted->toLeader = true;
        }
      }
    } else {
      ;
      // do nothing
      // UNCHANGED <<votesGranted, voterLog>>
    }
  }

  return 0;
}
