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

static int deserializeServerStateFromBuffer(SSyncServerState* server, const char* buffer, int n);
static int deserializeClusterConfigFromBuffer(SSyncClusterConfig* cluster, const char* buffer, int n);

static bool preHandleMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static bool preHandleNewTermMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);
static bool preHandleOldTermMessage(SSyncRaft* pRaft, const SSyncMessage* pMsg);

int32_t syncRaftStart(SSyncRaft* pRaft, const SSyncInfo* pInfo) {
  SSyncNode* pNode = pRaft->pNode;
  SSyncServerState serverState;
  SStateManager* stateManager;
  SSyncLogStore* logStore;
  SSyncFSM* fsm;
  SyncIndex initIndex = pInfo->snapshotIndex;
  SSyncBuffer buffer[RAFT_READ_LOG_MAX_NUM];
  int nBuf, limit, i;
  char* buf;
  int n;

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
  if (stateManager->readServerState(stateManager, &buf, &n) != 0) {
    syncError("readServerState for vgid %d fail", pInfo->vgId);
    return -1;
  }
  if (deserializeServerStateFromBuffer(&serverState, buf, n) != 0) {
    syncError("deserializeServerStateFromBuffer for vgid %d fail", pInfo->vgId);
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
  return 0;
}

static int deserializeServerStateFromBuffer(SSyncServerState* server, const char* buffer, int n) {
  return 0;
}

static int deserializeClusterConfigFromBuffer(SSyncClusterConfig* cluster, const char* buffer, int n) {
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