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
    if(pNode->peersNodeInfo[i].nodeRole == TAOS_SYNC_ROLE_LEARNER) continue;
    
    SRpcMsg rpcMsg = {0};
    ret = syncBuildRequestVote(&rpcMsg, pNode->vgId);
    if (ret < 0) {
      sError("vgId:%d, failed to build request-vote msg since %s", pNode->vgId, terrstr());
      continue;
    }

    SyncRequestVote* pMsg = rpcMsg.pCont;
    pMsg->srcId = pNode->myRaftId;
    pMsg->destId = pNode->peersId[i];
    pMsg->term = raftStoreGetTerm(pNode);

    ret = syncNodeGetLastIndexTerm(pNode, &pMsg->lastLogIndex, &pMsg->lastLogTerm);
    if (ret < 0) {
      sError("vgId:%d, failed to get index and term of last log since %s", pNode->vgId, terrstr());
      continue;
    }

    ret = syncNodeSendMsgById(&pNode->peersId[i], pNode, &rpcMsg);
    if (ret < 0) {
      sError("vgId:%d, failed to send msg to peerId:%" PRId64, pNode->vgId, pNode->peersId[i].addr);
      continue;
    }
  }
  return 0;
}

int32_t syncNodeElect(SSyncNode* pSyncNode) {
  if (pSyncNode->fsmState == SYNC_FSM_STATE_INCOMPLETE) {
    sNError(pSyncNode, "skip leader election due to incomplete fsm state");
    return -1;
  }

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
  raftStoreNextTerm(pSyncNode);
  raftStoreClearVote(pSyncNode);

  SyncTerm currentTerm = raftStoreGetTerm(pSyncNode);
  voteGrantedReset(pSyncNode->pVotesGranted, currentTerm);
  votesRespondReset(pSyncNode->pVotesRespond, currentTerm);
  syncNodeVoteForSelf(pSyncNode, currentTerm);

  if (voteGrantedMajority(pSyncNode->pVotesGranted)) {
    // only myself, to leader
    ASSERT(!pSyncNode->pVotesGranted->toLeader);
    syncNodeCandidate2Leader(pSyncNode);
    pSyncNode->pVotesGranted->toLeader = true;
    return ret;
  }

  if (pSyncNode->replicaNum == 1) {
    // only myself, to leader
    voteGrantedUpdate(pSyncNode->pVotesGranted, pSyncNode);
    votesRespondUpdate(pSyncNode->pVotesRespond, pSyncNode);

    pSyncNode->quorum = syncUtilQuorum(pSyncNode->raftCfg.cfg.replicaNum);

    syncNodeCandidate2Leader(pSyncNode);
    pSyncNode->pVotesGranted->toLeader = true;
    return ret;
  }

  ret = syncNodeRequestVotePeers(pSyncNode);
  ASSERT(ret == 0);

  syncNodeResetElectTimer(pSyncNode);
  return ret;
}
