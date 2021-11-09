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

SSyncRaftProgressTracker* syncRaftOpenProgressTracker() {
  SSyncRaftProgressTracker* tracker = (SSyncRaftProgressTracker*)malloc(sizeof(SSyncRaftProgressTracker));
  if (tracker == NULL) {
    return NULL;
  }

  return tracker;
}

void syncRaftResetVotes(SSyncRaftProgressTracker* tracker) {
  memset(tracker->votes, SYNC_RAFT_VOTE_RESP_UNKNOWN, sizeof(SyncRaftVoteRespType) * TSDB_MAX_REPLICA);
}

void syncRaftProgressVisit(SSyncRaftProgressTracker* tracker, visitProgressFp visit, void* arg) {
  int i;
  for (i = 0; i < TSDB_MAX_REPLICA; ++i) {
    SSyncRaftProgress* progress = &(tracker->progressMap[i]);
    visit(i, progress, arg);
  }
}