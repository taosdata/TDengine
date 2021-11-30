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
#include "sync_raft_impl.h"
#include "raft_log.h"
#include "raft_replication.h"
#include "sync_raft_progress_tracker.h"
#include "syncInt.h"

static int convertClear(SSyncRaft* pRaft);
static int stepFollower(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static int stepCandidate(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static int stepLeader(SSyncRaft* pRaft, const SSyncMessage* pMsg);

static bool increaseUncommittedSize(SSyncRaft* pRaft, SSyncRaftEntry* entries, int n);

static int triggerAll(SSyncRaft* pRaft);

static void tickElection(SSyncRaft* pRaft);
static void tickHeartbeat(SSyncRaft* pRaft);

static void appendEntries(SSyncRaft* pRaft, SSyncRaftEntry* entries, int n);

static void abortLeaderTransfer(SSyncRaft* pRaft);

static void resetRaft(SSyncRaft* pRaft, SyncTerm term);

void syncRaftBecomeFollower(SSyncRaft* pRaft, SyncTerm term, SyncNodeId leaderId) {
  convertClear(pRaft);

  pRaft->stepFp = stepFollower;
  resetRaft(pRaft, term);
  pRaft->tickFp = tickElection;
  pRaft->leaderId = leaderId;
  pRaft->state = TAOS_SYNC_STATE_FOLLOWER;
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
  pRaft->state  = TAOS_SYNC_STATE_CANDIDATE;
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
  pRaft->state  = TAOS_SYNC_STATE_CANDIDATE;
  syncInfo("[%d:%d] became candidate at term %" PRId64 "", pRaft->selfGroupId, pRaft->selfId, pRaft->term);
}

void syncRaftBecomeLeader(SSyncRaft* pRaft) {
  assert(pRaft->state != TAOS_SYNC_STATE_FOLLOWER);

  pRaft->stepFp = stepLeader;
  resetRaft(pRaft, pRaft->term);
  pRaft->leaderId = pRaft->leaderId;
  pRaft->state  = TAOS_SYNC_STATE_LEADER;

  SSyncRaftProgress* progress = syncRaftFindProgressByNodeId(&pRaft->tracker->progressMap, pRaft->selfId);
  assert(progress != NULL);
	// Followers enter replicate mode when they've been successfully probed
	// (perhaps after having received a snapshot as a result). The leader is
	// trivially in this state. Note that r.reset() has initialized this
	// progress with the last index already.  
  syncRaftProgressBecomeReplicate(progress);

	// Conservatively set the pendingConfIndex to the last index in the
	// log. There may or may not be a pending config change, but it's
	// safe to delay any future proposals until we commit all our
	// pending log entries, and scanning the entire tail of the log
	// could be expensive.
  SyncIndex lastIndex = syncRaftLogLastIndex(pRaft->log);
  pRaft->pendingConfigIndex = lastIndex;

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
  syncInfo("[%d:%d] became leader at term %" PRId64 "", pRaft->selfGroupId, pRaft->selfId, pRaft->term);
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
  return 0;
  //return pRaft->cluster.replica / 2 + 1;
}

ESyncRaftVoteResult  syncRaftPollVote(SSyncRaft* pRaft, SyncNodeId id, 
                                      bool preVote, bool grant, 
                                      int* rejected, int *granted) {
  SNodeInfo* pNode = syncRaftGetNodeById(pRaft, id);
  if (pNode == NULL) {
    return true;
  }

  if (grant) {
    syncInfo("[%d:%d] received grant (pre-vote %d) from %d at term %" PRId64 "", 
      pRaft->selfGroupId, pRaft->selfId, preVote, id, pRaft->term);
  } else {
    syncInfo("[%d:%d] received rejection (pre-vote %d) from %d at term %" PRId64 "", 
      pRaft->selfGroupId, pRaft->selfId, preVote, id, pRaft->term);
  }

  syncRaftRecordVote(pRaft->tracker, pNode->nodeId, grant);
  return syncRaftTallyVotes(pRaft->tracker, rejected, granted);
}
/*
  if (accept) {
    syncInfo("[%d:%d] received (pre-vote %d) from %d at term %" PRId64 "", 
      pRaft->selfGroupId, pRaft->selfId, preVote, id, pRaft->term);
  } else {
    syncInfo("[%d:%d] received rejection from %d at term %" PRId64 "", 
      pRaft->selfGroupId, pRaft->selfId, id, pRaft->term);
  }
  
  int voteIndex = syncRaftGetNodeById(pRaft, id);
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

void syncRaftLoadState(SSyncRaft* pRaft, const SSyncServerState* serverState) {
  SyncIndex commitIndex = serverState->commitIndex;
  SyncIndex lastIndex = syncRaftLogLastIndex(pRaft->log);

  if (commitIndex < pRaft->log->commitIndex || commitIndex > lastIndex) {
    syncFatal("[%d:%d] state.commit %"PRId64" is out of range [%" PRId64 ",%" PRId64 "",
      pRaft->selfGroupId, pRaft->selfId, commitIndex, pRaft->log->commitIndex, lastIndex);
    return;
  }

  pRaft->log->commitIndex = commitIndex;
  pRaft->term = serverState->term;
  pRaft->voteFor = serverState->voteFor;
}

static void visitProgressSendAppend(SSyncRaftProgress* progress, void* arg) {
  SSyncRaft* pRaft = (SSyncRaft*)arg;
  if (pRaft->selfId == progress->id) {
    return;
  }

  syncRaftMaybeSendAppend(arg, progress, true);
}

// bcastAppend sends RPC, with entries to all peers that are not up-to-date
// according to the progress recorded in r.prs.
void syncRaftBroadcastAppend(SSyncRaft* pRaft) {
  syncRaftProgressVisit(pRaft->tracker, visitProgressSendAppend, pRaft);
}

SNodeInfo* syncRaftGetNodeById(SSyncRaft *pRaft, SyncNodeId id) {
  SNodeInfo **ppNode = taosHashGet(pRaft->nodeInfoMap, &id, sizeof(SyncNodeId*));
  if (ppNode != NULL) {
    return *ppNode;
  }

  return NULL;
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
  ESyncRaftMessageType msgType = pMsg->msgType;

  if (msgType == RAFT_MSG_INTERNAL_PROP) {
    return 0;
  }

  if (msgType == RAFT_MSG_VOTE_RESP) {
    syncRaftHandleVoteRespMessage(pRaft, pMsg);
    return 0;
  } else if (msgType == RAFT_MSG_APPEND) {
    syncRaftBecomeFollower(pRaft, pMsg->term, pMsg->from);
    syncRaftHandleAppendEntriesMessage(pRaft, pMsg);
  }
  return 0;
}

static int stepLeader(SSyncRaft* pRaft, const SSyncMessage* pMsg) {
  convertClear(pRaft);
  return 0;
}

// tickElection is run by followers and candidates after r.electionTimeout.
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

// tickHeartbeat is run by leaders to send a MsgBeat after r.heartbeatTimeout.
static void tickHeartbeat(SSyncRaft* pRaft) {

}

// TODO
static bool increaseUncommittedSize(SSyncRaft* pRaft, SSyncRaftEntry* entries, int n) {
  return false;
}

static void appendEntries(SSyncRaft* pRaft, SSyncRaftEntry* entries, int n) {
  SyncIndex lastIndex = syncRaftLogLastIndex(pRaft->log);
  SyncTerm term = pRaft->term;
  int i;

  for (i = 0; i < n; ++i) {
    entries[i].term = term;
    entries[i].index = lastIndex + 1 + i;
  }

  // Track the size of this uncommitted proposal.
  if (!increaseUncommittedSize(pRaft, entries, n)) {
    // Drop the proposal.
    return;    
  }

  syncRaftLogAppend(pRaft->log, entries, n);

  SSyncRaftProgress* progress = syncRaftFindProgressByNodeId(&pRaft->tracker->progressMap, pRaft->selfId);
  assert(progress != NULL);
  syncRaftProgressMaybeUpdate(progress, lastIndex);
  // Regardless of syncRaftMaybeCommit's return, our caller will call bcastAppend.
  syncRaftMaybeCommit(pRaft);
}

// syncRaftMaybeCommit attempts to advance the commit index. Returns true if
// the commit index changed (in which case the caller should call
// r.bcastAppend).
bool syncRaftMaybeCommit(SSyncRaft* pRaft) {
  
  return true;
}

/**
 * trigger I/O requests for newly appended log entries or heartbeats.
 **/
static int triggerAll(SSyncRaft* pRaft) {
  #if 0
  assert(pRaft->state == TAOS_SYNC_STATE_LEADER);
  int i;

  for (i = 0; i < pRaft->cluster.replica; ++i) {
    if (i == pRaft->cluster.selfIndex) {
      continue;
    }

    syncRaftMaybeSendAppend(pRaft, pRaft->tracker->progressMap.progress[i], true);
  }
  #endif
  return 0;
}

static void abortLeaderTransfer(SSyncRaft* pRaft) {
  pRaft->leadTransferee = SYNC_NON_NODE_ID;
}

static void resetProgress(SSyncRaftProgress* progress, void* arg) {
  syncRaftResetProgress((SSyncRaft*)arg, progress);
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
  syncRaftProgressVisit(pRaft->tracker, resetProgress, pRaft);

  pRaft->pendingConfigIndex = 0;
  pRaft->uncommittedSize = 0;
}