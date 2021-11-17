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

#include "sync_raft_progress_tracker.h"
#include "sync_raft_proto.h"

SSyncRaftProgressTracker* syncRaftOpenProgressTracker() {
  SSyncRaftProgressTracker* tracker = (SSyncRaftProgressTracker*)malloc(sizeof(SSyncRaftProgressTracker));
  if (tracker == NULL) {
    return NULL;
  }

  syncRaftInitNodeMap(&tracker->config.learners);
  syncRaftInitNodeMap(&tracker->config.learnersNext);

  return tracker;
}

void syncRaftResetVotes(SSyncRaftProgressTracker* tracker) {
  taosHashClear(tracker->votesMap);
}

void syncRaftProgressVisit(SSyncRaftProgressTracker* tracker, visitProgressFp visit, void* arg) {
  int i;
  for (i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SSyncRaftProgress* progress = &(tracker->progressMap.progress[i]);
    visit(i, progress, arg);
  }
}

void syncRaftRecordVote(SSyncRaftProgressTracker* tracker, SyncNodeId id, bool grant) {
  ESyncRaftVoteType* pType = taosHashGet(tracker->votesMap, &id, sizeof(SyncNodeId*));
  if (pType != NULL) {
    return;
  }

  ESyncRaftVoteType type = grant ? SYNC_RAFT_VOTE_RESP_GRANT : SYNC_RAFT_VOTE_RESP_REJECT;
  taosHashPut(tracker->votesMap, &id, sizeof(SyncNodeId), &type, sizeof(ESyncRaftVoteType*));
}

void syncRaftCloneTrackerConfig(const SSyncRaftProgressTrackerConfig* from, SSyncRaftProgressTrackerConfig* to) {

}

int syncRaftCheckProgress(const SSyncRaftProgressTrackerConfig* config, SSyncRaftProgressMap* progressMap) {
  return 0;
}

/** 
 * syncRaftTallyVotes returns the number of granted and rejected Votes, and whether the
 * election outcome is known.
 **/
ESyncRaftVoteResult syncRaftTallyVotes(SSyncRaftProgressTracker* tracker, int* rejected, int *granted) {
  int i;
  SSyncRaftProgress* progress;
  int r, g;

  for (i = 0, r = 0, g = 0; i < TSDB_MAX_REPLICA; ++i) {
    progress = &(tracker->progressMap.progress[i]);
    if (progress->id == SYNC_NON_NODE_ID) {
      continue;
    }

    ESyncRaftVoteType* pType = taosHashGet(tracker->votesMap, &progress->id, sizeof(SyncNodeId*));
    if (pType == NULL) {
      continue;
    }

    if (*pType == SYNC_RAFT_VOTE_RESP_GRANT) {
      g++;
    } else {
      r++;
    }
  }

  if (rejected) *rejected = r;
  if (granted) *granted = g;
  return syncRaftVoteResult(&(tracker->config.voters), tracker->votesMap);
}

void syncRaftConfigState(const SSyncRaftProgressTracker* tracker, SSyncConfigState* cs) {
  syncRaftCopyNodeMap(&cs->voters, &tracker->config.voters.incoming);
  syncRaftCopyNodeMap(&cs->votersOutgoing, &tracker->config.voters.outgoing);
  syncRaftCopyNodeMap(&cs->learners, &tracker->config.learners);
  syncRaftCopyNodeMap(&cs->learnersNext, &tracker->config.learnersNext);
}