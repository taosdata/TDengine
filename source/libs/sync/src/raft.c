/*
 * Copyright (c) 2019 TAOS Data, Inc. <cli@taosdata.com>
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

#include "raft.h"
#include "raft_configuration.h"
#include "syncInt.h"

#define RAFT_READ_LOG_MAX_NUM 100

static bool preHandleMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static bool preHandleNewTermMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static bool preHandleOldTermMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);

static int convertClear(SSyncRaft* pRaft);
static int stepFollower(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static int stepCandidate(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static int stepLeader(SSyncRaft* pRaft, const SSyncMessage* pMsg);

static void tickElection(SSyncRaft* pRaft);
static void tickHeartbeat(SSyncRaft* pRaft);

static void abortLeaderTransfer(SSyncRaft* pRaft);

static void resetRaft(SSyncRaft* pRaft, SyncTerm term);

int32_t syncRaftStart(SSyncRaft* pRaft, const SSyncInfo* pInfo) {
  SSyncNode* pNode = pRaft->pNode;
  SSyncServerState serverState;
  SStateManager* stateManager;
  SSyncLogStore* logStore;
  SSyncFSM* fsm;
  SyncIndex initIndex = pInfo->snapshotIndex;
  SSyncBuffer buffer[RAFT_READ_LOG_MAX_NUM];
  int nBuf, limit, i;
  
  memset(pRaft, 0, sizeof(SSyncRaft));

  memcpy(&pRaft->fsm, &pInfo->fsm, sizeof(SSyncFSM));
  memcpy(&pRaft->logStore, &pInfo->logStore, sizeof(SSyncLogStore));
  memcpy(&pRaft->stateManager, &pInfo->stateManager, sizeof(SStateManager));

  stateManager = &(pRaft->stateManager);
  logStore = &(pRaft->logStore);
  fsm = &(pRaft->fsm);

  // open raft log
  if ((pRaft->log = syncRaftLogOpen()) == NULL) {
    return -1;
  }
  // read server state
  if (stateManager->readServerState(stateManager, &serverState) != 0) {
    syncError("readServerState for vgid %d fail", pInfo->vgId);
    return -1;
  }
  assert(initIndex <= serverState.commitIndex);

  // restore fsm state from snapshot index + 1 until commitIndex
  ++initIndex;
  while (initIndex <= serverState.commitIndex) {
    limit = MIN(RAFT_READ_LOG_MAX_NUM, serverState.commitIndex - initIndex + 1);

    if (logStore->logRead(logStore, initIndex, limit, buffer, &nBuf) != 0) {
      return -1;
    }
    assert(limit == nBuf);
  
    for (i = 0; i < limit; ++i) {
      fsm->applyLog(fsm, initIndex + i, &(buffer[i]), NULL);
      free(buffer[i].data);
    }
    initIndex += nBuf;
  }
  assert(initIndex == serverState.commitIndex);

  pRaft->heartbeatTimeoutTick = 1;

  syncRaftBecomeFollower(pRaft, pRaft->term, SYNC_NON_NODE_ID);

  syncInfo("[%d:%d] restore vgid %d state: snapshot index success", 
    pRaft->selfGroupId, pRaft->selfId, pInfo->vgId);
  return 0;
}

int32_t syncRaftStep(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  syncDebug("from %d, to %d, type:%d, term:%" PRId64 ", state:%d",
    pMsg->from, pMsg->to, pMsg->msgType, pMsg->term, pRaft->state);

  if (preHandleMessage(pRaft, pMsg)) {
    syncFreeMessage(pMsg);
    return 0;
  }

  RaftMessageType msgType = pMsg->msgType;
  if (msgType == RAFT_MSG_INTERNAL_ELECTION) {
    syncRaftHandleElectionMessage(pRaft, pMsg);
  } else if (msgType == RAFT_MSG_VOTE) {

  } else {
    pRaft->stepFp(pRaft, pMsg);
  }

  syncFreeMessage(pMsg);
  return 0;
}

int32_t syncRaftTick(SSyncRaft* pRaft) {
  return 0;
}

void syncRaftBecomeFollower(SSyncRaft* pRaft, SyncTerm term, SyncNodeId leaderId) {
  pRaft->stepFp = stepFollower;
  resetRaft(pRaft, term);
  pRaft->tickFp = tickElection;
  pRaft->leaderId = leaderId;
  pRaft->state = TAOS_SYNC_ROLE_FOLLOWER;
}

void syncRaftBecomePreCandidate(SSyncRaft* pRaft) {
	/**
   * Becoming a pre-candidate changes our step functions and state,
	 * but doesn't change anything else. In particular it does not increase
	 * r.Term or change r.Vote.
   **/
  pRaft->stepFp = stepCandidate;
  pRaft->tickFp = tickElection;
  pRaft->state  = TAOS_SYNC_ROLE_CANDIDATE;
  pRaft->candidateState.inPreVote = true;
  syncInfo("[%d:%d] became pre-candidate at term %d" PRId64 "", pRaft->selfGroupId, pRaft->selfId, pRaft->term);
}

void syncRaftBecomeCandidate(SSyncRaft* pRaft) {
  pRaft->candidateState.inPreVote = false;
  pRaft->stepFp = stepCandidate;
  // become candidate make term+1
  resetRaft(pRaft, pRaft->term + 1);
  pRaft->tickFp = tickElection;
  pRaft->voteFor = pRaft->selfId;
  pRaft->state  = TAOS_SYNC_ROLE_CANDIDATE;
  syncInfo("[%d:%d] became candidate at term %d" PRId64 "", pRaft->selfGroupId, pRaft->selfId, pRaft->term);
}

void syncRaftBecomeLeader(SSyncRaft* pRaft) {
  assert(pRaft->state != TAOS_SYNC_ROLE_FOLLOWER);

  pRaft->stepFp = stepLeader;
  resetRaft(pRaft, pRaft->term);
  pRaft->leaderId = pRaft->leaderId;
  pRaft->state  = TAOS_SYNC_ROLE_LEADER;
  // TODO: check if there is pending config log

  syncInfo("[%d:%d] became leader at term %d" PRId64 "", pRaft->selfGroupId, pRaft->selfId, pRaft->term);
}

void syncRaftRandomizedElectionTimeout(SSyncRaft* pRaft) {
  // electionTimeoutTick in [3,6] tick
  pRaft->electionTimeoutTick = taosRand() % 4 + 3;
}

bool syncRaftIsPromotable(SSyncRaft* pRaft) {
  return pRaft->selfId != SYNC_NON_NODE_ID;
}

bool syncRaftIsPastElectionTimeout(SSyncRaft* pRaft) {
  return pRaft->electionElapsed >= pRaft->electionTimeoutTick;
}

int syncRaftQuorum(SSyncRaft* pRaft) {
  return pRaft->cluster.replica / 2 + 1;
}

int syncRaftNumOfGranted(SSyncRaft* pRaft, SyncNodeId id, bool preVote, bool accept) {
  if (accept) {
    syncInfo("[%d:%d] received (pre-vote %d) from %d at term %" PRId64 "", 
      pRaft->selfGroupId, pRaft->selfId, preVote, id, pRaft->term);
  } else {
    syncInfo("[%d:%d] received rejection from %d at term %" PRId64 "", 
      pRaft->selfGroupId, pRaft->selfId, id, pRaft->term);
  }

  int voteIndex = syncRaftConfigurationIndexOfVoter(pRaft, id);
  assert(voteIndex < pRaft->cluster.replica && voteIndex >= 0);

  pRaft->candidateState.votes[voteIndex] = accept;
  int granted = 0;
  int i;
  for (i = 0; i < pRaft->cluster.replica; ++i) {
    if (pRaft->candidateState.votes[i]) granted++;
  }

  return granted;
}

/**
 * pre-handle message, return true is no need to continue
 * Handle the message term, which may result in our stepping down to a follower.
 **/
static bool preHandleMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  // local message?
  if (pMsg->term == 0) {
    return false;
  }

  if (pMsg->term > pRaft->term) {
    return preHandleNewTermMessage(pRaft, pMsg);
  }

  return preHandleOldTermMessage(pRaft, pMsg);;
}

static bool preHandleNewTermMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  SyncNodeId leaderId = pMsg->from;
  RaftMessageType msgType = pMsg->msgType;

  if (msgType == RAFT_MSG_VOTE) {
    leaderId = SYNC_NON_NODE_ID;
  }

  if (syncIsPreVoteMsg(pMsg)) {
    // Never change our term in response to a PreVote
  } else if (syncIsPreVoteRespMsg(pMsg) && !pMsg->voteResp.reject) {
		/**
     * We send pre-vote requests with a term in our future. If the
		 * pre-vote is granted, we will increment our term when we get a
		 * quorum. If it is not, the term comes from the node that
		 * rejected our vote so we should become a follower at the new
		 * term.
     **/
  } else {
    syncInfo("[%d:%d] [term:%" PRId64 "] received a %d message with higher term from %d [term:%" PRId64 "]",
      pRaft->selfGroupId, pRaft->selfId, pRaft->term, msgType, pMsg->from, pMsg->term);
    syncRaftBecomeFollower(pRaft, pMsg->term, leaderId);
  }

  return false;
}

static bool preHandleOldTermMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {

  // if receive old term message, no need to continue
  return true;
}

static int convertClear(SSyncRaft* pRaft) {

}

static int stepFollower(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  convertClear(pRaft);
  return 0;
}

static int stepCandidate(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  convertClear(pRaft);
  memset(pRaft->candidateState.votes, 0, sizeof(bool) * TSDB_MAX_REPLICA);
  return 0;
}

static int stepLeader(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  convertClear(pRaft);
  return 0;
}

/**
 *  tickElection is run by followers and candidates per tick.
 **/
static void tickElection(SSyncRaft* pRaft) {
  pRaft->electionElapsed += 1;

  if (!syncRaftIsPromotable(pRaft)) {
    return;
  }

  if (!syncRaftIsPastElectionTimeout(pRaft)) {
    return;
  }

  // election timeout
  pRaft->electionElapsed = 0;
  SSyncMessage msg;
  syncRaftStep(pRaft, syncInitElectionMsg(&msg, pRaft->selfId));
}

static void tickHeartbeat(SSyncRaft* pRaft) {

}

static void abortLeaderTransfer(SSyncRaft* pRaft) {
  pRaft->leadTransferee = SYNC_NON_NODE_ID;
}

static void resetRaft(SSyncRaft* pRaft, SyncTerm term) {
  if (pRaft->term != term) {
    pRaft->term = term;
    pRaft->voteFor = SYNC_NON_NODE_ID;
  }

  pRaft->leaderId = SYNC_NON_NODE_ID;

  pRaft->electionElapsed = 0;
  pRaft->heartbeatElapsed = 0;

  syncRaftRandomizedElectionTimeout(pRaft);

  abortLeaderTransfer(pRaft);

  pRaft->pendingConf = false;
}