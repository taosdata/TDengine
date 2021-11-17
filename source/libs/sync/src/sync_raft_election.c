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

#include "syncInt.h"
#include "raft.h"
#include "raft_log.h"
#include "raft_message.h"
#include "sync_raft_progress_tracker.h"

static void campaign(SSyncRaft* pRaft, ESyncRaftElectionType cType);

void syncRaftStartElection(SSyncRaft* pRaft, ESyncRaftElectionType cType) {
  if (pRaft->state == TAOS_SYNC_STATE_LEADER) {
    syncDebug("[%d:%d] ignoring RAFT_MSG_INTERNAL_ELECTION because already leader", pRaft->selfGroupId, pRaft->selfId);
    return;
  }

  if (!syncRaftIsPromotable(pRaft)) {
    syncWarn("[%d:%d] is unpromotable and can not campaign", pRaft->selfGroupId, pRaft->selfId);
    return;
  }

  // if there is pending uncommitted config,cannot start election
  if (syncRaftLogNumOfPendingConf(pRaft->log) > 0 && syncRaftHasUnappliedLog(pRaft->log)) {
    syncWarn("[%d:%d] cannot syncRaftStartElection at term %" PRId64 " since there are still pending configuration changes to apply",
      pRaft->selfGroupId, pRaft->selfId, pRaft->term);
    return;
  }

  syncInfo("[%d:%d] is starting a new election at term %" PRId64 "", pRaft->selfGroupId, pRaft->selfId, pRaft->term);

  campaign(pRaft, cType);
}

// campaign transitions the raft instance to candidate state. This must only be
// called after verifying that this is a legitimate transition.
static void campaign(SSyncRaft* pRaft, ESyncRaftElectionType cType) {
  bool preVote;
  SyncTerm term;

  if (syncRaftIsPromotable(pRaft)) {
    syncDebug("[%d:%d] is unpromotable; campaign() should have been called", pRaft->selfGroupId, pRaft->selfId);
    return;
  }

  if (cType == SYNC_RAFT_CAMPAIGN_PRE_ELECTION) {
    syncRaftBecomePreCandidate(pRaft);
    preVote = true;
    // PreVote RPCs are sent for the next term before we've incremented r.Term.
    term = pRaft->term + 1;
  } else {
    syncRaftBecomeCandidate(pRaft);
    term = pRaft->term;
    preVote = false;
  }

  int quorum = syncRaftQuorum(pRaft);
  ESyncRaftVoteResult result = syncRaftPollVote(pRaft, pRaft->selfId, preVote, true, NULL, NULL);
  if (result == SYNC_RAFT_VOTE_WON) {
		// We won the election after voting for ourselves (which must mean that
		// this is a single-node cluster). Advance to the next state.
    if (cType == SYNC_RAFT_CAMPAIGN_PRE_ELECTION) {
      syncRaftStartElection(pRaft, SYNC_RAFT_CAMPAIGN_ELECTION);
    } else {
      syncRaftBecomeLeader(pRaft);
    }
    return;
  }

  // broadcast vote message to other peers
  int i;
  SyncIndex lastIndex = syncRaftLogLastIndex(pRaft->log);
  SyncTerm lastTerm = syncRaftLogLastTerm(pRaft->log);
  SSyncRaftNodeMap nodeMap;
  syncRaftJointConfigIDS(&pRaft->tracker->config.voters, &nodeMap);
  for (i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SyncNodeId nodeId = nodeMap.nodeId[i];
    if (nodeId == SYNC_NON_NODE_ID) {
      continue;
    }

    if (nodeId == pRaft->selfId) {
      continue;
    }

    SSyncMessage* pMsg = syncNewVoteMsg(pRaft->selfGroupId, pRaft->selfId, 
                                        term, cType, lastIndex, lastTerm);
    if (pMsg == NULL) {
      continue;
    }

    syncInfo("[%d:%d] [logterm: %" PRId64 ", index: %" PRId64 "] sent vote request to %d at term %" PRId64 "", 
      pRaft->selfGroupId, pRaft->selfId, lastTerm, 
      lastIndex, nodeId, pRaft->term);

    //pRaft->io.send(pMsg, &(pRaft->cluster.nodeInfo[i]));
  }
}