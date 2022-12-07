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
#include "syncElection.h"
#include "syncMessage.h"
#include "syncRaftCfg.h"
#include "syncRaftStore.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"

// TLA+ Spec
// RequestVote(i, j) ==
//    /\ state[i] = Candidate
//    /\ j \notin votesResponded[i]
//    /\ Send([mtype         |-> RequestVoteRequest,
//             mterm         |-> currentTerm[i],
//             mlastLogTerm  |-> LastTerm(log[i]),
//             mlastLogIndex |-> Len(log[i]),
//             msource       |-> i,
//             mdest         |-> j])
//    /\ UNCHANGED <<serverVars, candidateVars, leaderVars, logVars>>

static int32_t syncNodeRequestVotePeers(SSyncNode* pNode) {
  if (pNode->state != TAOS_SYNC_STATE_CANDIDATE) {
    sNTrace(pNode, "not candidate, stop elect");
    return 0;
  }

  int32_t ret = 0;
  for (int i = 0; i < pNode->peersNum; ++i) {
    SRpcMsg rpcMsg = {0};
    ret = syncBuildRequestVote(&rpcMsg, pNode->vgId);
    tAssert(ret == 0);

    SyncRequestVote* pMsg = rpcMsg.pCont;
    pMsg->srcId = pNode->myRaftId;
    pMsg->destId = pNode->peersId[i];
    pMsg->term = pNode->pRaftStore->currentTerm;

    ret = syncNodeGetLastIndexTerm(pNode, &pMsg->lastLogIndex, &pMsg->lastLogTerm);
    tAssert(ret == 0);

    ret = syncNodeSendMsgById(&pNode->peersId[i], pNode, &rpcMsg);
    tAssert(ret == 0);
  }

  return ret;
}

int32_t syncNodeElect(SSyncNode* pSyncNode) {
  sNInfo(pSyncNode, "begin election");
  pSyncNode->electNum++;

  int32_t ret = 0;
  if (pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER) {
    syncNodeFollower2Candidate(pSyncNode);
  }

  if (pSyncNode->state != TAOS_SYNC_STATE_CANDIDATE) {
    sNError(pSyncNode, "not candidate, can not elect");
    return -1;
  }

  // start election
  raftStoreNextTerm(pSyncNode->pRaftStore);
  raftStoreClearVote(pSyncNode->pRaftStore);
  voteGrantedReset(pSyncNode->pVotesGranted, pSyncNode->pRaftStore->currentTerm);
  votesRespondReset(pSyncNode->pVotesRespond, pSyncNode->pRaftStore->currentTerm);

  syncNodeVoteForSelf(pSyncNode);
  if (voteGrantedMajority(pSyncNode->pVotesGranted)) {
    // only myself, to leader
    tAssert(!pSyncNode->pVotesGranted->toLeader);
    syncNodeCandidate2Leader(pSyncNode);
    pSyncNode->pVotesGranted->toLeader = true;
    return ret;
  }

  if (pSyncNode->replicaNum == 1) {
    // only myself, to leader
    voteGrantedUpdate(pSyncNode->pVotesGranted, pSyncNode);
    votesRespondUpdate(pSyncNode->pVotesRespond, pSyncNode);

    pSyncNode->quorum = syncUtilQuorum(pSyncNode->pRaftCfg->cfg.replicaNum);

    syncNodeCandidate2Leader(pSyncNode);
    pSyncNode->pVotesGranted->toLeader = true;
    return ret;
  }

  ret = syncNodeRequestVotePeers(pSyncNode);
  tAssert(ret == 0);

  syncNodeResetElectTimer(pSyncNode);

  return ret;
}
