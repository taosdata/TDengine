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

#include "sync_raft_quorum.h"
#include "sync_raft_quorum_majority.h"
#include "sync_raft_node_map.h"

/**
 * syncRaftMajorityVoteResult takes a mapping of voters to yes/no (true/false) votes and returns
 * a result indicating whether the vote is pending (i.e. neither a quorum of
 * yes/no has been reached), won (a quorum of yes has been reached), or lost (a
 * quorum of no has been reached).
 **/
ESyncRaftVoteResult syncRaftMajorityVoteResult(SSyncRaftNodeMap* config, SHashObj* votesMap) {
  if (config->replica == 0) {
    return SYNC_RAFT_VOTE_WON;
  }

  int i, g, r, missing;
  for (i = g = r = missing = 0; i < TSDB_MAX_REPLICA; ++i) {
    if (config->nodeId[i] == SYNC_NON_NODE_ID) {
      continue;
    }

    const ESyncRaftVoteType* pType = taosHashGet(votesMap, &config->nodeId[i], sizeof(SyncNodeId*));
    if (pType == NULL) {
      missing += 1;
      continue;
    }

    if (*pType == SYNC_RAFT_VOTE_RESP_GRANT) {
      g +=1;
    } else {
      r += 1;
    }
  }

  int quorum = config->replica / 2 + 1;
  if (g >= quorum) {
    return SYNC_RAFT_VOTE_WON;
  }
  if (r + missing >= quorum) {
    return SYNC_RAFT_VOTE_PENDING;
  }

  return SYNC_RAFT_VOTE_LOST;
}