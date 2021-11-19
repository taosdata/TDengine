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

#include "sync_const.h"
#include "sync_raft_quorum.h"
#include "sync_raft_quorum_majority.h"
#include "sync_raft_node_map.h"

// VoteResult takes a mapping of voters to yes/no (true/false) votes and returns
// a result indicating whether the vote is pending (i.e. neither a quorum of
// yes/no has been reached), won (a quorum of yes has been reached), or lost (a
// quorum of no has been reached).
ESyncRaftVoteResult syncRaftMajorityVoteResult(SSyncRaftNodeMap* config, SHashObj* votesMap) {
  int n = syncRaftNodeMapSize(config);
  if (n == 0) {
		// By convention, the elections on an empty config win. This comes in
		// handy with joint quorums because it'll make a half-populated joint
		// quorum behave like a majority quorum.    
    return SYNC_RAFT_VOTE_WON;
  }

  int i, g, r, missing;
  i = g = r = missing = 0;
  SyncNodeId* pId = NULL;
  while (!syncRaftIterateNodeMap(config, pId)) {
    const bool* v = (const bool*)taosHashGet(votesMap, pId, sizeof(SyncNodeId*));
    if (v == NULL) {
      missing += 1;
      continue;
    }

    if (*v) {
      g +=1;
    } else {
      r += 1;
    }
  }

  int quorum = n / 2 + 1;
  if (g >= quorum) {
    return SYNC_RAFT_VOTE_WON;
  }
  if (g + missing >= quorum) {
    return SYNC_RAFT_VOTE_PENDING;
  }

  return SYNC_RAFT_VOTE_LOST;
}

int compSyncIndex(const void * elem1, const void * elem2) {
  SyncIndex index1 = *((SyncIndex*)elem1);
  SyncIndex index2 = *((SyncIndex*)elem1);
  if (index1 > index2) return  1;
  if (index1 < index2) return -1;
  return 0;
}

SyncIndex syncRaftMajorityConfigCommittedIndex(const SSyncRaftNodeMap* config, matchAckIndexerFp indexer, void* arg) {
  int n = syncRaftNodeMapSize(config);
  if (n == 0) {
		// This plays well with joint quorums which, when one half is the zero
		// MajorityConfig, should behave like the other half.    
    return kMaxCommitIndex;
  }

	// Use an on-stack slice to collect the committed indexes when n <= 7
	// (otherwise we alloc). The alternative is to stash a slice on
	// MajorityConfig, but this impairs usability (as is, MajorityConfig is just
	// a map, and that's nice). The assumption is that running with a
	// replication factor of >7 is rare, and in cases in which it happens
	// performance is a lesser concern (additionally the performance
	// implications of an allocation here are far from drastic).
  SyncIndex* srt = NULL;
  SyncIndex srk[TSDB_MAX_REPLICA];
  if (n > TSDB_MAX_REPLICA) {
    srt = (SyncIndex*)malloc(sizeof(SyncIndex) * n);
    if (srt == NULL) {
      return kMaxCommitIndex;
    }
  } else {
    srt = &srk[0];
  }

	// Fill the slice with the indexes observed. Any unused slots will be
	// left as zero; these correspond to voters that may report in, but
	// haven't yet. We fill from the right (since the zeroes will end up on
	// the left after sorting below anyway).
  SyncNodeId *pId = NULL;
  int i = 0;
  SyncIndex index;
  while (!syncRaftIterateNodeMap(config, pId)) {
    indexer(*pId, arg, &index);    
    srt[i++] = index;
  }

	// Sort by index. Use a bespoke algorithm (copied from the stdlib's sort
	// package) to keep srt on the stack.
  qsort(srt, n, sizeof(SyncIndex), compSyncIndex);

	// The smallest index into the array for which the value is acked by a
	// quorum. In other words, from the end of the slice, move n/2+1 to the
	// left (accounting for zero-indexing).
  index = srt[n - (n/2 + 1)];
  if (srt != &srk[0]) {
    free(srt);
  }

  return index;
}