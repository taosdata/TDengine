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

#ifndef _TD_LIBS_SYNC_RAFT_QUORUM_MAJORITY_H
#define _TD_LIBS_SYNC_RAFT_QUORUM_MAJORITY_H

#include "sync.h"
#include "sync_type.h"

/**
 * syncRaftMajorityVoteResult takes a mapping of voters to yes/no (true/false) votes and returns
 * a result indicating whether the vote is pending (i.e. neither a quorum of
 * yes/no has been reached), won (a quorum of yes has been reached), or lost (a
 * quorum of no has been reached).
 **/
SyncRaftVoteResult syncRaftMajorityVoteResult(SSyncCluster* config, const SyncRaftVoteResult* votes);

#endif /* _TD_LIBS_SYNC_RAFT_QUORUM_MAJORITY_H */
