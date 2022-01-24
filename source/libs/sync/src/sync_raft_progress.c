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
#include "raft_log.h"
#include "sync_raft_progress.h"
#include "sync_raft_progress_tracker.h"
#include "sync.h"
#include "syncInt.h"

static void copyProgress(SSyncRaftProgress* progress, void* arg);

static void refProgress(SSyncRaftProgress* progress);
static void unrefProgress(SSyncRaftProgress* progress, void*);

static void resetProgressState(SSyncRaftProgress* progress, ESyncRaftProgressState state);
static void probeAcked(SSyncRaftProgress* progress);

static void resumeProgress(SSyncRaftProgress* progress);

void syncRaftResetProgress(SSyncRaft* pRaft, SSyncRaftProgress* progress) {
  if (progress->inflights) {
    syncRaftCloseInflights(progress->inflights);
  }
  SSyncRaftInflights* inflights = syncRaftOpenInflights(pRaft->tracker->maxInflightMsgs);
  if (inflights == NULL) {
    return;
  }
  *progress = (SSyncRaftProgress) {
    .matchIndex = progress->id == pRaft->selfId ? syncRaftLogLastIndex(pRaft->log) : 0,
    .nextIndex  = syncRaftLogLastIndex(pRaft->log) + 1,
    .inflights  = inflights,
    .isLearner  = false,
    .state      = PROGRESS_STATE_PROBE,
  };
}

// MaybeUpdate is called when an MsgAppResp arrives from the follower, with the
// index acked by it. The method returns false if the given n index comes from
// an outdated message. Otherwise it updates the progress and returns true.
bool syncRaftProgressMaybeUpdate(SSyncRaftProgress* progress, SyncIndex lastIndex) {
  bool updated = false;

  if (progress->matchIndex < lastIndex) {
    progress->matchIndex = lastIndex;
    updated = true;
    probeAcked(progress);
  }

  progress->nextIndex = TMAX(progress->nextIndex, lastIndex + 1);

  return updated;
}

// MaybeDecrTo adjusts the Progress to the receipt of a MsgApp rejection. The
// arguments are the index of the append message rejected by the follower, and
// the hint that we want to decrease to.
//
// Rejections can happen spuriously as messages are sent out of order or
// duplicated. In such cases, the rejection pertains to an index that the
// Progress already knows were previously acknowledged, and false is returned
// without changing the Progress.
//
// If the rejection is genuine, Next is lowered sensibly, and the Progress is
// cleared for sending log entries.
bool syncRaftProgressMaybeDecrTo(SSyncRaftProgress* progress,
                                SyncIndex rejected, SyncIndex matchHint) {
  if (progress->state == PROGRESS_STATE_REPLICATE) {
		// The rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
    if (rejected <= progress->matchIndex) {
      syncDebug("match index is up to date,ignore");
      return false;
    }

		// Directly decrease next to match + 1.
		//
		// TODO(tbg): why not use matchHint if it's larger?
    progress->nextIndex = progress->matchIndex + 1;
    return true;
  }

	// The rejection must be stale if "rejected" does not match next - 1. This
	// is because non-replicating followers are probed one entry at a time.
  if (rejected != progress->nextIndex - 1) {
    syncDebug("rejected index %" PRId64 " different from next index %" PRId64 " -> ignore"
      , rejected, progress->nextIndex);
    return false;
  }

  progress->nextIndex = TMAX(TMIN(rejected, matchHint + 1), 1);

  progress->probeSent = false;
  return true;
}

// IsPaused returns whether sending log entries to this node has been throttled.
// This is done when a node has rejected recent MsgApps, is currently waiting
// for a snapshot, or has reached the MaxInflightMsgs limit. In normal
// operation, this is false. A throttled node will be contacted less frequently
// until it has reached a state in which it's able to accept a steady stream of
// log entries again.
bool syncRaftProgressIsPaused(SSyncRaftProgress* progress) {
  switch (progress->state) {
    case PROGRESS_STATE_PROBE:
      return progress->probeSent;
    case PROGRESS_STATE_REPLICATE:
      return syncRaftInflightFull(progress->inflights);
    case PROGRESS_STATE_SNAPSHOT:
      return true;
    default:
      syncFatal("error sync state:%d", progress->state);
  }
}

SSyncRaftProgress* syncRaftFindProgressByNodeId(const SSyncRaftProgressMap* progressMap, SyncNodeId id) {
  SSyncRaftProgress** ppProgress = (SSyncRaftProgress**)taosHashGet(progressMap->progressMap, &id, sizeof(SyncNodeId*));
  if (ppProgress == NULL) {
    return NULL;
  }

  return *ppProgress;
}

int syncRaftAddToProgressMap(SSyncRaftProgressMap* progressMap, SSyncRaftProgress* progress) {
  refProgress(progress);
  taosHashPut(progressMap->progressMap, &progress->id, sizeof(SyncNodeId*), &progress, sizeof(SSyncRaftProgress*));
}

void syncRaftRemoveFromProgressMap(SSyncRaftProgressMap* progressMap, SyncNodeId id) {
  SSyncRaftProgress** ppProgress = (SSyncRaftProgress**)taosHashGet(progressMap->progressMap, &id, sizeof(SyncNodeId*));
  if (ppProgress == NULL) {
    return;
  }
  unrefProgress(*ppProgress, NULL);

  taosHashRemove(progressMap->progressMap, &id, sizeof(SyncNodeId*));
}

bool syncRaftIsInProgressMap(SSyncRaftProgressMap* progressMap, SyncNodeId id) {
  return taosHashGet(progressMap->progressMap, &id, sizeof(SyncNodeId*)) != NULL;
}

bool syncRaftProgressIsUptodate(SSyncRaft* pRaft, SSyncRaftProgress* progress) {
  return syncRaftLogLastIndex(pRaft->log) + 1 == progress->nextIndex;
}

// BecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
// optionally and if larger, the index of the pending snapshot.
void syncRaftProgressBecomeProbe(SSyncRaftProgress* progress) {
	// If the original state is StateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
  if (progress->state == PROGRESS_STATE_SNAPSHOT) {
    SyncIndex pendingSnapshotIndex = progress->pendingSnapshotIndex;
    resetProgressState(progress, PROGRESS_STATE_PROBE);
    progress->nextIndex = TMAX(progress->matchIndex + 1, pendingSnapshotIndex + 1);
  } else {
    resetProgressState(progress, PROGRESS_STATE_PROBE);
    progress->nextIndex = progress->matchIndex + 1;
  }
}

// BecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
void syncRaftProgressBecomeReplicate(SSyncRaftProgress* progress) {
  resetProgressState(progress, PROGRESS_STATE_REPLICATE);
  progress->nextIndex = progress->matchIndex + 1;
}

// BecomeSnapshot moves the Progress to StateSnapshot with the specified pending
// snapshot index.
void syncRaftProgressBecomeSnapshot(SSyncRaftProgress* progress, SyncIndex snapshotIndex) {
  resetProgressState(progress, PROGRESS_STATE_SNAPSHOT);
  progress->pendingSnapshotIndex = snapshotIndex;
}

void syncRaftCopyProgress(const SSyncRaftProgress* progress, SSyncRaftProgress* out) {
  memcpy(out, progress, sizeof(SSyncRaftProgress));
}

void syncRaftInitProgressMap(SSyncRaftProgressMap* progressMap) {
  progressMap->progressMap = taosHashInit(TSDB_MAX_REPLICA, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
}

void syncRaftFreeProgressMap(SSyncRaftProgressMap* progressMap) {
  syncRaftVisitProgressMap(progressMap, unrefProgress, NULL);
  taosHashCleanup(progressMap->progressMap);
}

void syncRaftClearProgressMap(SSyncRaftProgressMap* progressMap) {
  taosHashClear(progressMap->progressMap);
}

void syncRaftCopyProgressMap(SSyncRaftProgressMap* from, SSyncRaftProgressMap* to) {
  syncRaftVisitProgressMap(from, copyProgress, to);
}

bool syncRaftIterateProgressMap(const SSyncRaftProgressMap* progressMap, SSyncRaftProgress *pProgress) {
  SSyncRaftProgress **ppProgress = taosHashIterate(progressMap->progressMap, pProgress);
  if (ppProgress == NULL) {
    return true;
  }

  *pProgress = *(*ppProgress);
  return false;
}

bool syncRaftVisitProgressMap(SSyncRaftProgressMap* progressMap, visitProgressFp fp, void* arg) {
  SSyncRaftProgress *pProgress;
  while (!syncRaftIterateProgressMap(progressMap, pProgress)) {
    fp(pProgress, arg);
  }
}

static void copyProgress(SSyncRaftProgress* progress, void* arg) {
  assert(progress->refCount > 0);
  SSyncRaftProgressMap* to = (SSyncRaftProgressMap*)arg;
  syncRaftAddToProgressMap(to, progress);
}

static void refProgress(SSyncRaftProgress* progress) {
  progress->refCount += 1;
}

static void unrefProgress(SSyncRaftProgress* progress, void* arg) {
  (void)arg;
  progress->refCount -= 1;
  assert(progress->refCount >= 0);
  if (progress->refCount == 0) {
    free(progress);
  }
}
 
// ResetState moves the Progress into the specified State, resetting ProbeSent,
// PendingSnapshot, and Inflights.
static void resetProgressState(SSyncRaftProgress* progress, ESyncRaftProgressState state) {
  progress->probeSent = false;
  progress->pendingSnapshotIndex = 0;
  progress->state = state;
  syncRaftInflightReset(progress->inflights);
}

// ProbeAcked is called when this peer has accepted an append. It resets
// ProbeSent to signal that additional append messages should be sent without
// further delay.
static void probeAcked(SSyncRaftProgress* progress) {
  progress->probeSent = false;
}
