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

/**
 * SSyncRaftQuorumJointConfig is a configuration of two groups of (possibly overlapping)
 * majority configurations. Decisions require the support of both majorities.
 **/
typedef struct SSyncRaftQuorumJointConfig {
  SSyncCluster majorityConfig[2];
}SSyncRaftQuorumJointConfig;

/**
 * syncRaftVoteResult takes a mapping of voters to yes/no (true/false) votes and returns
 * a result indicating whether the vote is pending, lost, or won. A joint quorum
 * requires both majority quorums to vote in favor.
 **/
SyncRaftVoteResult syncRaftVoteResult(SSyncRaftQuorumJointConfig* config, const SyncRaftVoteResult* votes);

#endif /* _TD_LIBS_SYNC_RAFT_QUORUM_JOINT_H */
