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
#include "sync_raft_restore.h"
#include "raft_replication.h"
#include "sync_raft_config_change.h"
#include "sync_raft_progress_tracker.h"
#include "syncInt.h"

#define RAFT_READ_LOG_MAX_NUM 100

static int deserializeServerStateFromBuffer(SSyncServerState* server, const char* buffer, int n);
static int deserializeClusterStateFromBuffer(SSyncConfigState* cluster, const char* buffer, int n);

static void switchToConfig(SSyncRaft* pRaft, const SSyncRaftProgressTrackerConfig* config, 
                          const SSyncRaftProgressMap* progressMap, SSyncConfigState* cs);

static void abortLeaderTransfer(SSyncRaft* pRaft);

static bool preHandleMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static bool preHandleNewTermMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static bool preHandleOldTermMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);

int32_t syncRaftStart(SSyncRaft* pRaft, const SSyncInfo* pInfo) {
  SSyncNode* pNode = pRaft->pNode;
  SSyncServerState serverState;
  SSyncConfigState confState;
  SStateManager* stateManager;
  SSyncLogStore* logStore;
  SSyncFSM* fsm;
  SSyncBuffer buffer[RAFT_READ_LOG_MAX_NUM];
  int nBuf, limit, i;
  char* buf;
  int n;
  SSyncRaftChanger changer;

  memset(pRaft, 0, sizeof(SSyncRaft));

  memcpy(&pRaft->fsm, &pInfo->fsm, sizeof(SSyncFSM));
  memcpy(&pRaft->logStore, &pInfo->logStore, sizeof(SSyncLogStore));
  memcpy(&pRaft->stateManager, &pInfo->stateManager, sizeof(SStateManager));

  stateManager = &(pRaft->stateManager);
  logStore = &(pRaft->logStore);
  fsm = &(pRaft->fsm);

  pRaft->nodeInfoMap = taosHashInit(TSDB_MAX_REPLICA, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  if (pRaft->nodeInfoMap == NULL) {
    return -1;
  }

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
  if (stateManager->readServerState(stateManager, &buf, &n) != 0) {
    syncError("readServerState for vgid %d fail", pInfo->vgId);
    return -1;
  }
  if (deserializeServerStateFromBuffer(&serverState, buf, n) != 0) {
    syncError("deserializeServerStateFromBuffer for vgid %d fail", pInfo->vgId);
    return -1;
  }
  free(buf);
  //assert(initIndex <= serverState.commitIndex);

  // read config state
  if (stateManager->readClusterState(stateManager, &buf, &n) != 0) {
    syncError("readClusterState for vgid %d fail", pInfo->vgId);
    return -1;
  }
  if (deserializeClusterStateFromBuffer(&confState, buf, n) != 0) {
    syncError("deserializeClusterStateFromBuffer for vgid %d fail", pInfo->vgId);
    return -1;
  }
  free(buf);

  changer = (SSyncRaftChanger) {
    .tracker = pRaft->tracker,
    .lastIndex = syncRaftLogLastIndex(pRaft->log),
  };
  if (syncRaftRestoreConfig(&changer, &confState) < 0) {
    syncError("syncRaftRestoreConfig for vgid %d fail", pInfo->vgId);
    return -1;
  }

  if (!syncRaftIsEmptyServerState(&serverState)) {
    syncRaftLoadState(pRaft, &serverState);
  }

  if (pInfo->appliedIndex > 0) {
    syncRaftLogAppliedTo(pRaft->log, pInfo->appliedIndex);
  }

  syncRaftBecomeFollower(pRaft, pRaft->term, SYNC_NON_NODE_ID);

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

  ESyncRaftMessageType msgType = pMsg->msgType;
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
  pRaft->tickFp(pRaft);
  return 0;
}

static int deserializeServerStateFromBuffer(SSyncServerState* server, const char* buffer, int n) {
  return 0;
}

static int deserializeClusterStateFromBuffer(SSyncConfigState* cluster, const char* buffer, int n) {
  return 0;
}

static void visitProgressMaybeSendAppend(int i, SSyncRaftProgress* progress, void* arg) {
  syncRaftReplicate(arg, progress, false);
}

// switchToConfig reconfigures this node to use the provided configuration. It
// updates the in-memory state and, when necessary, carries out additional
// actions such as reacting to the removal of nodes or changed quorum
// requirements.
//
// The inputs usually result from restoring a ConfState or applying a ConfChange.
static void switchToConfig(SSyncRaft* pRaft, const SSyncRaftProgressTrackerConfig* config, 
                          const SSyncRaftProgressMap* progressMap, SSyncConfigState* cs) {
  SyncNodeId selfId = pRaft->selfId;
  int i;
  bool exist;
  SSyncRaftProgress* progress = NULL;

  syncRaftConfigState(pRaft->tracker, cs);
  i = syncRaftFindProgressIndexByNodeId(&pRaft->tracker->progressMap, selfId);
  exist = (i != -1);

	// Update whether the node itself is a learner, resetting to false when the
	// node is removed.
  if (exist) {
    progress = &pRaft->tracker->progressMap.progress[i];
    pRaft->isLearner = progress->isLearner;
  } else {
    pRaft->isLearner = false;
  }

  if ((!exist || pRaft->isLearner) && pRaft->state == TAOS_SYNC_STATE_LEADER) {
		// This node is leader and was removed or demoted. We prevent demotions
		// at the time writing but hypothetically we handle them the same way as
		// removing the leader: stepping down into the next Term.
		//
		// TODO(tbg): step down (for sanity) and ask follower with largest Match
		// to TimeoutNow (to avoid interruption). This might still drop some
		// proposals but it's better than nothing.
		//
		// TODO(tbg): test this branch. It is untested at the time of writing.
    return;
  }

	// The remaining steps only make sense if this node is the leader and there
	// are other nodes.
  if (pRaft->state != TAOS_SYNC_STATE_LEADER || cs->voters.replica == 0) {
    return;
  }

  if (syncRaftMaybeCommit(pRaft)) {
		// If the configuration change means that more entries are committed now,
		// broadcast/append to everyone in the updated config.
    syncRaftBroadcastAppend(pRaft);
  } else {
		// Otherwise, still probe the newly added replicas; there's no reason to
		// let them wait out a heartbeat interval (or the next incoming
		// proposal).
    syncRaftProgressVisit(pRaft->tracker, visitProgressMaybeSendAppend, pRaft);

    // If the the leadTransferee was removed or demoted, abort the leadership transfer.
    SyncNodeId leadTransferee = pRaft->leadTransferee;
    if (leadTransferee != SYNC_NON_NODE_ID) {
      if (!syncRaftIsInNodeMap(&pRaft->tracker->config.voters.incoming, leadTransferee) &&
          !syncRaftIsInNodeMap(&pRaft->tracker->config.voters.outgoing, leadTransferee)) {
        abortLeaderTransfer(pRaft);
      }      
    }
  }
}

static void abortLeaderTransfer(SSyncRaft* pRaft) {
  pRaft->leadTransferee = SYNC_NON_NODE_ID;
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
  ESyncRaftMessageType msgType = pMsg->msgType;

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
    SNodeInfo* pNode = syncRaftGetNodeById(pRaft, pMsg->from);
    if (pNode == NULL) {
      return true;
    }
    SSyncMessage* msg = syncNewEmptyAppendRespMsg(pRaft->selfGroupId, pRaft->selfId, pRaft->term);
    if (msg == NULL) {
      return true;
    }

    pRaft->io.send(msg, pNode);
  } else {
    // ignore other cases
    syncInfo("[%d:%d] [term:%" PRId64 "] ignored a %d message with lower term from %d [term:%" PRId64 "]",
      pRaft->selfGroupId, pRaft->selfId, pRaft->term, pMsg->msgType, pMsg->from, pMsg->term);    
  }

  return true;
}