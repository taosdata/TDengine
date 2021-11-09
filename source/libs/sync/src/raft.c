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
#include "raft_log.h"
#include "raft_replication.h"
#include "sync_raft_progress_tracker.h"
#include "syncInt.h"

#define RAFT_READ_LOG_MAX_NUM 100

static bool preHandleMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static bool preHandleNewTermMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static bool preHandleOldTermMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);

static int convertClear(SSyncRaft* pRaft);
static int stepFollower(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static int stepCandidate(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static int stepLeader(SSyncRaft* pRaft, const SSyncMessage* pMsg);

static int triggerAll(SSyncRaft* pRaft);

static void tickElection(SSyncRaft* pRaft);
static void tickHeartbeat(SSyncRaft* pRaft);

static void appendEntries(SSyncRaft* pRaft, SSyncRaftEntry* entries, int n);
static bool maybeCommit(SSyncRaft* pRaft);

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

  // init progress tracker
  pRaft->tracker = syncRaftOpenProgressTracker();
  if (pRaft->tracker == NULL) {
    return -1;
  }

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

  //pRaft->heartbeatTimeoutTick = 1;

  syncRaftBecomeFollower(pRaft, pRaft->term, SYNC_NON_NODE_ID);

  pRaft->selfIndex = pRaft->cluster.selfIndex;

  syncInfo("[%d:%d] restore vgid %d state: snapshot index success", 
    pRaft->selfGroupId, pRaft->selfId, pInfo->vgId);
  return 0;
}

int32_t syncRaftStep(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  syncDebug("from %d, type:%d, term:%" PRId64 ", state:%d",
    pMsg->from, pMsg->msgType, pMsg->term, pRaft->state);

  if (preHandleMessage(pRaft, pMsg)) {
    syncFreeMessage(pMsg);
    return 0;
  }

  RaftMessageType msgType = pMsg->msgType;
  if (msgType == RAFT_MSG_INTERNAL_ELECTION) {
    syncRaftHandleElectionMessage(pRaft, pMsg);
  } else if (msgType == RAFT_MSG_VOTE) {
    syncRaftHandleVoteMessage(pRaft, pMsg);
  } else {
    pRaft->stepFp(pRaft, pMsg);
  }

  syncFreeMessage(pMsg);
  return 0;
}

int32_t syncRaftTick(SSyncRaft* pRaft) {
  pRaft->currentTick += 1;
  return 0;
}

void syncRaftBecomeFollower(SSyncRaft* pRaft, SyncTerm term, SyncNodeId leaderId) {
  convertClear(pRaft);

  pRaft->stepFp = stepFollower;
  resetRaft(pRaft, term);
  pRaft->tickFp = tickElection;
  pRaft->leaderId = leaderId;
  pRaft->state = TAOS_SYNC_ROLE_FOLLOWER;
  syncInfo("[%d:%d] became followe at term %" PRId64 "", pRaft->selfGroupId, pRaft->selfId, pRaft->term);
}

void syncRaftBecomePreCandidate(SSyncRaft* pRaft) {
  convertClear(pRaft);

	/**
   * Becoming a pre-candidate changes our step functions and state,
	 * but doesn't change anything else. In particular it does not increase
	 * r.Term or change r.Vote.
   **/
  pRaft->stepFp = stepCandidate;
  pRaft->tickFp = tickElection;
  pRaft->state  = TAOS_SYNC_ROLE_CANDIDATE;
  pRaft->candidateState.inPreVote = true;
  syncInfo("[%d:%d] became pre-candidate at term %" PRId64 "", pRaft->selfGroupId, pRaft->selfId, pRaft->term);
}

void syncRaftBecomeCandidate(SSyncRaft* pRaft) {
  convertClear(pRaft);

  pRaft->candidateState.inPreVote = false;
  pRaft->stepFp = stepCandidate;
  // become candidate make term+1
  resetRaft(pRaft, pRaft->term + 1);
  pRaft->tickFp = tickElection;
  pRaft->voteFor = pRaft->selfId;
  pRaft->state  = TAOS_SYNC_ROLE_CANDIDATE;
  syncInfo("[%d:%d] became candidate at term %" PRId64 "", pRaft->selfGroupId, pRaft->selfId, pRaft->term);
}

void syncRaftBecomeLeader(SSyncRaft* pRaft) {
  assert(pRaft->state != TAOS_SYNC_ROLE_FOLLOWER);

  pRaft->stepFp = stepLeader;
  resetRaft(pRaft, pRaft->term);
  pRaft->leaderId = pRaft->leaderId;
  pRaft->state  = TAOS_SYNC_ROLE_LEADER;
  // TODO: check if there is pending config log
  int nPendingConf = syncRaftLogNumOfPendingConf(pRaft->log);
  if (nPendingConf > 1) {
    syncFatal("unexpected multiple uncommitted config entry");
  }

  syncInfo("[%d:%d] became leader at term %" PRId64 "", pRaft->selfGroupId, pRaft->selfId, pRaft->term);

  // after become leader, send a no-op log
  SSyncRaftEntry* entry = (SSyncRaftEntry*)malloc(sizeof(SSyncRaftEntry));
  if (entry == NULL) {
    return;
  }
  *entry = (SSyncRaftEntry) {
    .buffer = (SSyncBuffer) {
      .data = NULL,
      .len = 0,
    }
  };
  appendEntries(pRaft, entry, 1);
  //syncRaftTriggerHeartbeat(pRaft);
}

void syncRaftTriggerHeartbeat(SSyncRaft* pRaft) {
  triggerAll(pRaft);
}

void syncRaftRandomizedElectionTimeout(SSyncRaft* pRaft) {
  // electionTimeoutTick in [3,6] tick
  pRaft->randomizedElectionTimeout = taosRand() % 4 + 3;
}

bool syncRaftIsPromotable(SSyncRaft* pRaft) {
  return pRaft->selfId != SYNC_NON_NODE_ID;
}

bool syncRaftIsPastElectionTimeout(SSyncRaft* pRaft) {
  return pRaft->electionElapsed >= pRaft->randomizedElectionTimeout;
}

int syncRaftQuorum(SSyncRaft* pRaft) {
  return pRaft->cluster.replica / 2 + 1;
}

int syncRaftNumOfGranted(SSyncRaft* pRaft, SyncNodeId id, bool preVote, bool accept, int* rejectNum) {
/*
  if (accept) {
    syncInfo("[%d:%d] received (pre-vote %d) from %d at term %" PRId64 "", 
      pRaft->selfGroupId, pRaft->selfId, preVote, id, pRaft->term);
  } else {
    syncInfo("[%d:%d] received rejection from %d at term %" PRId64 "", 
      pRaft->selfGroupId, pRaft->selfId, id, pRaft->term);
  }
  
  int voteIndex = syncRaftConfigurationIndexOfNode(pRaft, id);
  assert(voteIndex < pRaft->cluster.replica && voteIndex >= 0);
  assert(pRaft->candidateState.votes[voteIndex] == SYNC_RAFT_VOTE_RESP_UNKNOWN);

  pRaft->candidateState.votes[voteIndex] = accept ? SYNC_RAFT_VOTE_RESP_GRANT : SYNC_RAFT_VOTE_RESP_REJECT;
  int granted = 0, rejected = 0;
  int i;
  for (i = 0; i < pRaft->cluster.replica; ++i) {
    if (pRaft->candidateState.votes[i] == SYNC_RAFT_VOTE_RESP_GRANT) granted++;
    else if (pRaft->candidateState.votes[i] == SYNC_RAFT_VOTE_RESP_REJECT) rejected++;
  }

  if (rejectNum) *rejectNum = rejected;
  return granted;
*/
  return 0;
}

/**
 * pre-handle message, return true means no need to continue
 * Handle the message term, which may result in our stepping down to a follower.
 **/
static bool preHandleMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  // local message?
  if (pMsg->term == 0) {
    return false;
  }

  if (pMsg->term > pRaft->term) {
    return preHandleNewTermMessage(pRaft, pMsg);
  } else if (pMsg->term < pRaft->term) {
    return preHandleOldTermMessage(pRaft, pMsg);
  }

  return false;
}

static bool preHandleNewTermMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  SyncNodeId leaderId = pMsg->from;
  RaftMessageType msgType = pMsg->msgType;

  if (msgType == RAFT_MSG_VOTE) {
    // TODO
    leaderId = SYNC_NON_NODE_ID;
  }

  if (syncIsPreVoteMsg(pMsg)) {
    // Never change our term in response to a PreVote
  } else if (syncIsPreVoteRespMsg(pMsg) && !pMsg->voteResp.rejected) {
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
  if (pRaft->checkQuorum && pMsg->msgType == RAFT_MSG_APPEND) {
		/**
     * We have received messages from a leader at a lower term. It is possible
		 * that these messages were simply delayed in the network, but this could
		 * also mean that this node has advanced its term number during a network
		 * partition, and it is now unable to either win an election or to rejoin
	   * the majority on the old term. If checkQuorum is false, this will be
		 * handled by incrementing term numbers in response to MsgVote with a
		 * higher term, but if checkQuorum is true we may not advance the term on
		 * MsgVote and must generate other messages to advance the term. The net
		 * result of these two features is to minimize the disruption caused by
		 * nodes that have been removed from the cluster's configuration: a
		 * removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
		 * but it will not receive MsgApp or MsgHeartbeat, so it will not create
		 * disruptive term increases
    **/
    int peerIndex = syncRaftConfigurationIndexOfNode(pRaft, pMsg->from);
    if (peerIndex < 0) {
      return true;
    }
    SSyncMessage* msg = syncNewEmptyAppendRespMsg(pRaft->selfGroupId, pRaft->selfId, pRaft->term);
    if (msg == NULL) {
      return true;
    }

    pRaft->io.send(msg, &(pRaft->cluster.nodeInfo[peerIndex]));
  } else {
    // ignore other cases
    syncInfo("[%d:%d] [term:%" PRId64 "] ignored a %d message with lower term from %d [term:%" PRId64 "]",
      pRaft->selfGroupId, pRaft->selfId, pRaft->term, pMsg->msgType, pMsg->from, pMsg->term);    
  }

  return true;
}

static int convertClear(SSyncRaft* pRaft) {

}

static int stepFollower(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  
  return 0;
}

static int stepCandidate(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  /**
   * Only handle vote responses corresponding to our candidacy (while in
	 * StateCandidate, we may get stale MsgPreVoteResp messages in this term from
	 * our pre-candidate state).
   **/
  RaftMessageType msgType = pMsg->msgType;

  if (msgType == RAFT_MSG_INTERNAL_PROP) {
    return 0;
  }

  if (msgType == RAFT_MSG_VOTE_RESP) {
    syncRaftHandleVoteRespMessage(pRaft, pMsg);
    return 0;
  } else if (msgType == RAFT_MSG_APPEND) {
    syncRaftBecomeFollower(pRaft, pRaft->term, pMsg->from);
    syncRaftHandleAppendEntriesMessage(pRaft, pMsg);
  }
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

static void appendEntries(SSyncRaft* pRaft, SSyncRaftEntry* entries, int n) {
  SyncIndex lastIndex = syncRaftLogLastIndex(pRaft->log);
  SyncTerm term = pRaft->term;
  int i;

  for (i = 0; i < n; ++i) {
    entries[i].term = term;
    entries[i].index = lastIndex + 1 + i;
  }

  syncRaftLogAppend(pRaft->log, entries, n);

  SSyncRaftProgress* progress = &(pRaft->tracker->progressMap[pRaft->cluster.selfIndex]);
  syncRaftProgressMaybeUpdate(progress, lastIndex);
  // Regardless of maybeCommit's return, our caller will call bcastAppend.
  maybeCommit(pRaft);
}

/**
 * maybeCommit attempts to advance the commit index. Returns true if
 * the commit index changed (in which case the caller should call
 * r.bcastAppend).
 **/
static bool maybeCommit(SSyncRaft* pRaft) {
  
  return true;
}

/**
 * trigger I/O requests for newly appended log entries or heartbeats.
 **/
static int triggerAll(SSyncRaft* pRaft) {
  assert(pRaft->state == TAOS_SYNC_ROLE_LEADER);
  int i;

  for (i = 0; i < pRaft->cluster.replica; ++i) {
    if (i == pRaft->cluster.selfIndex) {
      continue;
    }

    syncRaftReplicate(pRaft, i);
  }
}

static void abortLeaderTransfer(SSyncRaft* pRaft) {
  pRaft->leadTransferee = SYNC_NON_NODE_ID;
}

static void initProgress(int i, SSyncRaftProgress* progress, void* arg) {
  syncRaftInitProgress(i, (SSyncRaft*)arg, progress);
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

  syncRaftResetVotes(pRaft->tracker);
  syncRaftProgressVisit(pRaft->tracker, initProgress, pRaft);

  pRaft->pendingConfigIndex = 0;
  pRaft->uncommittedSize = 0;
}
