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

#ifndef _TD_LIBS_SYNC_RAFT_QUORUM_JOINT_H
#define _TD_LIBS_SYNC_RAFT_QUORUM_JOINT_H

#include "taosdef.h"
#include "sync.h"
#include "sync_type.h"
#include "sync_raft_node_map.h"
#include "thash.h"

/**
 * SSyncRaftQuorumJointConfig is a configuration of two groups of (possibly overlapping)
 * majority configurations. Decisions require the support of both majorities.
 **/
typedef struct SSyncRaftQuorumJointConfig {
  SSyncRaftNodeMap outgoing;
  SSyncRaftNodeMap incoming;
} SSyncRaftQuorumJointConfig;

/**
 * syncRaftVoteResult takes a mapping of voters to yes/no (true/false) votes and returns
 * a result indicating whether the vote is pending, lost, or won. A joint quorum
 * requires both majority quorums to vote in favor.
 **/
ESyncRaftVoteType syncRaftVoteResult(SSyncRaftQuorumJointConfig* config, SHashObj* votesMap);

void syncRaftInitQuorumJointConfig(SSyncRaftQuorumJointConfig* config);

static FORCE_INLINE bool syncRaftJointConfigInOutgoing(const SSyncRaftQuorumJointConfig* config, SyncNodeId id) {
  return syncRaftIsInNodeMap(&config->outgoing, id);
}

static FORCE_INLINE bool syncRaftJointConfigInIncoming(const SSyncRaftQuorumJointConfig* config, SyncNodeId id) {
  return syncRaftIsInNodeMap(&config->incoming, id);
}

void syncRaftJointConfigAddToIncoming(SSyncRaftQuorumJointConfig* config, SyncNodeId id);

void syncRaftJointConfigRemoveFromIncoming(SSyncRaftQuorumJointConfig* config, SyncNodeId id);

static FORCE_INLINE const SSyncRaftNodeMap* syncRaftJointConfigIncoming(const SSyncRaftQuorumJointConfig* config) {
  return &config->incoming;
}

static FORCE_INLINE const SSyncRaftNodeMap* syncRaftJointConfigOutgoing(const SSyncRaftQuorumJointConfig* config) {
  return &config->outgoing;
}

static FORCE_INLINE void syncRaftJointConfigClearOutgoing(SSyncRaftQuorumJointConfig* config) {
  syncRaftClearNodeMap(&config->outgoing);
}

static FORCE_INLINE bool syncRaftJointConfigIsIncomingEmpty(const SSyncRaftQuorumJointConfig* config) {
  return syncRaftNodeMapSize(&config->incoming) == 0;
}

static FORCE_INLINE bool syncRaftJointConfigIsOutgoingEmpty(const SSyncRaftQuorumJointConfig* config) {
  return syncRaftNodeMapSize(&config->outgoing) == 0;
}

static FORCE_INLINE bool syncRaftJointConfigIsInOutgoing(const SSyncRaftQuorumJointConfig* config, SyncNodeId id) {
  return syncRaftIsInNodeMap(&config->outgoing, id);
}

void syncRaftJointConfigIDS(const SSyncRaftQuorumJointConfig* config, SSyncRaftNodeMap* nodeMap);

#endif /* _TD_LIBS_SYNC_RAFT_QUORUM_JOINT_H */
