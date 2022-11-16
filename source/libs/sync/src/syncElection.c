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

#include "syncElection.h"
#include "syncMessage.h"
#include "syncRaftCfg.h"
#include "syncRaftStore.h"
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

int32_t syncNodeElect(SSyncNode* pSyncNode) {
  syncNodeEventLog(pSyncNode, "begin election");

  int64_t nowMs = taosGetMonoTimestampMs();
  if (nowMs < pSyncNode->lastMsgRecvTime + pSyncNode->electTimerMS) {
    sError("vgId:%d, election timer triggered ahead of time for %" PRId64 "ms", pSyncNode->vgId,
           pSyncNode->lastMsgRecvTime + pSyncNode->electTimerMS - nowMs);
    return -1;
  }

  int32_t ret = 0;
  if (pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER) {
    syncNodeFollower2Candidate(pSyncNode);
  }

  if (pSyncNode->state != TAOS_SYNC_STATE_CANDIDATE) {
    syncNodeErrorLog(pSyncNode, "not candidate, can not elect");
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
    ASSERT(!pSyncNode->pVotesGranted->toLeader);
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
  ASSERT(ret == 0);

  syncNodeResetElectTimer(pSyncNode);

  return ret;
}

int32_t syncNodeRequestVotePeers(SSyncNode* pSyncNode) {
  if (pSyncNode->state != TAOS_SYNC_STATE_CANDIDATE) {
    syncNodeEventLog(pSyncNode, "not candidate, stop elect");
    return 0;
  }

  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    SyncRequestVote* pMsg = syncRequestVoteBuild(pSyncNode->vgId);
    pMsg->srcId = pSyncNode->myRaftId;
    pMsg->destId = pSyncNode->peersId[i];
    pMsg->term = pSyncNode->pRaftStore->currentTerm;

    ret = syncNodeGetLastIndexTerm(pSyncNode, &(pMsg->lastLogIndex), &(pMsg->lastLogTerm));
    ASSERT(ret == 0);

    ret = syncNodeSendRequestVote(pSyncNode, &pSyncNode->peersId[i], pMsg);
    ASSERT(ret == 0);
    syncRequestVoteDestroy(pMsg);
  }
  return ret;
}

int32_t syncNodeSendRequestVote(SSyncNode* pSyncNode, const SRaftId* destRaftId, const SyncRequestVote* pMsg) {
  int32_t ret = 0;
  // syncLogSendRequestVote(pSyncNode, pMsg, "");
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pMsg->destId.addr, host, sizeof(host), &port);
  sInfo("vgId:%d, send request vote of term: %" PRId64 " to %s:%d", pSyncNode->vgId, pMsg->term, host, port);

  SRpcMsg rpcMsg;
  syncRequestVote2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(destRaftId, pSyncNode, &rpcMsg);
  return ret;
}
