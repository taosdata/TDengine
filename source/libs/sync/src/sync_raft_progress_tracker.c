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
#include "sync_const.h"
#include "sync_raft_progress_tracker.h"
#include "sync_raft_proto.h"

SSyncRaftProgressTracker* syncRaftOpenProgressTracker(SSyncRaft* pRaft) {
  SSyncRaftProgressTracker* tracker = (SSyncRaftProgressTracker*)malloc(sizeof(SSyncRaftProgressTracker));
  if (tracker == NULL) {
    return NULL;
  }

  tracker->votesMap = taosHashInit(TSDB_MAX_REPLICA, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);

  syncRaftInitTrackConfig(&tracker->config);
  tracker->pRaft = pRaft;
  tracker->maxInflightMsgs = kSyncRaftMaxInflghtMsgs;

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
  syncRaftFreeNodeMap(&config->voters.incoming);
  syncRaftFreeNodeMap(&config->voters.outgoing);
}

// ResetVotes prepares for a new round of vote counting via recordVote.
void syncRaftResetVotes(SSyncRaftProgressTracker* tracker) {
  taosHashClear(tracker->votesMap);
}

void syncRaftProgressVisit(SSyncRaftProgressTracker* tracker, visitProgressFp visit, void* arg) {
  syncRaftVisitProgressMap(&tracker->progressMap, visit, arg);  
}

// RecordVote records that the node with the given id voted for this Raft
// instance if v == true (and declined it otherwise).
void syncRaftRecordVote(SSyncRaftProgressTracker* tracker, SyncNodeId id, bool grant) {
  ESyncRaftVoteType* pType = taosHashGet(tracker->votesMap, &id, sizeof(SyncNodeId*));
  if (pType != NULL) {
    return;
  }

  taosHashPut(tracker->votesMap, &id, sizeof(SyncNodeId), &grant, sizeof(bool*));
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

// TallyVotes returns the number of granted and rejected Votes, and whether the
// election outcome is known.
ESyncRaftVoteResult syncRaftTallyVotes(SSyncRaftProgressTracker* tracker, int* rejected, int *granted) {
  SSyncRaftProgress* progress = NULL;
  int r, g;

	// Make sure to populate granted/rejected correctly even if the Votes slice
	// contains members no longer part of the configuration. This doesn't really
	// matter in the way the numbers are used (they're informational), but might
	// as well get it right.
  while (!syncRaftIterateProgressMap(&tracker->progressMap, progress)) {
    if (progress->id == SYNC_NON_NODE_ID) {
      continue;
    }

    bool* v = taosHashGet(tracker->votesMap, &progress->id, sizeof(SyncNodeId*));
    if (v == NULL) {
      continue;
    }

    if (*v) {
      g++;
    } else {
      r++;
    }
  }

  if (rejected) *rejected = r;
  if (granted) *granted = g;
  return syncRaftVoteResult(&(tracker->config.voters), tracker->votesMap);
}

void syncRaftConfigState(SSyncRaftProgressTracker* tracker, SSyncConfigState* cs) {
  syncRaftCopyNodeMap(&tracker->config.voters.incoming, &cs->voters);
  syncRaftCopyNodeMap(&tracker->config.voters.outgoing, &cs->votersOutgoing);
  syncRaftCopyNodeMap(&tracker->config.learners, &cs->learners);
  syncRaftCopyNodeMap(&tracker->config.learnersNext, &cs->learnersNext);
  cs->autoLeave = tracker->config.autoLeave;
}

static void matchAckIndexer(SyncNodeId id, void* arg, SyncIndex* index) {
  SSyncRaftProgressTracker* tracker = (SSyncRaftProgressTracker*)arg;
  SSyncRaftProgress* progress = syncRaftFindProgressByNodeId(&tracker->progressMap, id);
  if (progress == NULL) {
    *index = 0;
    return;
  }
  *index = progress->matchIndex;
}

// Committed returns the largest log index known to be committed based on what
// the voting members of the group have acknowledged.
SyncIndex syncRaftCommittedIndex(SSyncRaftProgressTracker* tracker) {
  return syncRaftJointConfigCommittedIndex(&tracker->config.voters, matchAckIndexer, tracker);
}

static void visitProgressActive(SSyncRaftProgress* progress, void* arg) {
  SHashObj* votesMap = (SHashObj*)arg;
  taosHashPut(votesMap, &progress->id, sizeof(SyncNodeId), &progress->recentActive, sizeof(bool));
}

// QuorumActive returns true if the quorum is active from the view of the local
// raft state machine. Otherwise, it returns false.
bool syncRaftQuorumActive(SSyncRaftProgressTracker* tracker) {
  SHashObj* votesMap = taosHashInit(TSDB_MAX_REPLICA, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
  syncRaftVisitProgressMap(&tracker->progressMap, visitProgressActive, votesMap);

  return syncRaftVoteResult(&tracker->config.voters, votesMap) == SYNC_RAFT_VOTE_WON;
}