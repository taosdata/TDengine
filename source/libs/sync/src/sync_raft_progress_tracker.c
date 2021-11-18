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

SSyncRaftProgressTracker* syncRaftOpenProgressTracker(SSyncRaft* pRaft) {
  SSyncRaftProgressTracker* tracker = (SSyncRaftProgressTracker*)malloc(sizeof(SSyncRaftProgressTracker));
  if (tracker == NULL) {
    return NULL;
  }

  syncRaftInitTrackConfig(&tracker->config);
  syncRaftInitNodeMap(&tracker->config.learnersNext);
  tracker->pRaft = pRaft;

  return tracker;
}

void syncRaftInitTrackConfig(SSyncRaftProgressTrackerConfig* config) {
  syncRaftInitNodeMap(&config->learners);
  syncRaftInitNodeMap(&config->learnersNext);
  syncRaftInitQuorumJointConfig(&config->voters);
  config->autoLeave = false;
}

void syncRaftFreeTrackConfig(SSyncRaftProgressTrackerConfig* config) {
  syncRaftFreeNodeMap(&config->learners);
  syncRaftFreeNodeMap(&config->learnersNext);
  syncRaftFreeQuorumJointConfig(&config->voters);
}

void syncRaftResetVotes(SSyncRaftProgressTracker* tracker) {
  taosHashClear(tracker->votesMap);
}

void syncRaftProgressVisit(SSyncRaftProgressTracker* tracker, visitProgressFp visit, void* arg) {
  syncRaftVisitProgressMap(&tracker->progressMap, visit, arg);  
}

void syncRaftRecordVote(SSyncRaftProgressTracker* tracker, SyncNodeId id, bool grant) {
  ESyncRaftVoteType* pType = taosHashGet(tracker->votesMap, &id, sizeof(SyncNodeId*));
  if (pType != NULL) {
    return;
  }

  ESyncRaftVoteType type = grant ? SYNC_RAFT_VOTE_RESP_GRANT : SYNC_RAFT_VOTE_RESP_REJECT;
  taosHashPut(tracker->votesMap, &id, sizeof(SyncNodeId), &type, sizeof(ESyncRaftVoteType*));
}

void syncRaftCopyTrackerConfig(const SSyncRaftProgressTrackerConfig* from, SSyncRaftProgressTrackerConfig* to) {
  memcpy(to, from, sizeof(SSyncRaftProgressTrackerConfig));
}

int syncRaftCheckTrackerConfigInProgress(SSyncRaftProgressTrackerConfig* config, SSyncRaftProgressMap* progressMap) {
	// NB: intentionally allow the empty config. In production we'll never see a
	// non-empty config (we prevent it from being created) but we will need to
	// be able to *create* an initial config, for example during bootstrap (or
	// during tests). Instead of having to hand-code this, we allow
	// transitioning from an empty config into any other legal and non-empty
	// config.  
  if (!syncRaftIsAllNodeInProgressMap(&config->voters.incoming, progressMap)) return -1;
  if (!syncRaftIsAllNodeInProgressMap(&config->voters.outgoing, progressMap)) return -1;
  if (!syncRaftIsAllNodeInProgressMap(&config->learners, progressMap)) return -1;
  if (!syncRaftIsAllNodeInProgressMap(&config->learnersNext, progressMap)) return -1;
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

  while (!syncRaftIterateProgressMap(&tracker->progressMap, progress)) {
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
  syncRaftCopyNodeMap(&tracker->config.voters.incoming, &cs->voters);
  syncRaftCopyNodeMap(&tracker->config.voters.outgoing, &cs->votersOutgoing);
  syncRaftCopyNodeMap(&tracker->config.learners, &cs->learners);
  syncRaftCopyNodeMap(&tracker->config.learnersNext, &cs->learnersNext);
}