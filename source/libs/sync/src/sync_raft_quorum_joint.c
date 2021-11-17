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

#include "sync_raft_node_map.h"
#include "sync_raft_quorum_majority.h"
#include "sync_raft_quorum_joint.h"
#include "sync_raft_quorum.h"

/**
 * syncRaftVoteResult takes a mapping of voters to yes/no (true/false) votes and returns
 * a result indicating whether the vote is pending, lost, or won. A joint quorum
 * requires both majority quorums to vote in favor.
 **/
ESyncRaftVoteType syncRaftVoteResult(SSyncRaftQuorumJointConfig* config, SHashObj* votesMap) {
  ESyncRaftVoteResult r1 = syncRaftMajorityVoteResult(&(config->incoming), votesMap);
  ESyncRaftVoteResult r2 = syncRaftMajorityVoteResult(&(config->outgoing), votesMap);

  if (r1 == r2) {
    // If they agree, return the agreed state.
    return r1;
  }

  if (r1 == SYNC_RAFT_VOTE_LOST || r2 == SYNC_RAFT_VOTE_LOST) {
    // If either config has lost, loss is the only possible outcome.
    return SYNC_RAFT_VOTE_LOST;
  }

  // One side won, the other one is pending, so the whole outcome is.
  return SYNC_RAFT_VOTE_PENDING;
}

void syncRaftJointConfigAddToIncoming(SSyncRaftQuorumJointConfig* config, SyncNodeId id) {
  int i, min;

  for (i = 0, min = -1; i < TSDB_MAX_REPLICA; ++i) {
    if (config->incoming.nodeId[i] == id) {
      return;
    }
    if (min == -1 && config->incoming.nodeId[i] == SYNC_NON_NODE_ID) {
      min = i;
    }
  }

  assert(min != -1);
  config->incoming.nodeId[min] = id;
  config->incoming.replica += 1;
}

void syncRaftJointConfigRemoveFromIncoming(SSyncRaftQuorumJointConfig* config, SyncNodeId id) {
  int i;

  for (i = 0; i < TSDB_MAX_REPLICA; ++i) {
    if (config->incoming.nodeId[i] == id) {
      config->incoming.replica  -= 1;
      config->incoming.nodeId[i] = SYNC_NON_NODE_ID;
      break;
    }
  }

  assert(config->incoming.replica >= 0);
}

void syncRaftJointConfigIDS(const SSyncRaftQuorumJointConfig* config, SSyncRaftNodeMap* nodeMap) {
  int i, j, m;

  syncRaftCopyNodeMap(&config->incoming, nodeMap);

  syncRaftUnionNodeMap(&config->outgoing, nodeMap);
}