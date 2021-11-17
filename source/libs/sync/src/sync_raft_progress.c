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

static void resetProgressState(SSyncRaftProgress* progress, ESyncRaftProgressState state);
static void probeAcked(SSyncRaftProgress* progress);

static void resumeProgress(SSyncRaftProgress* progress);

void syncRaftInitProgress(int i, SSyncRaft* pRaft, SSyncRaftProgress* progress) {
  SSyncRaftInflights* inflights = syncRaftOpenInflights(pRaft->tracker->maxInflightMsgs);
  if (inflights == NULL) {
    return;
  }
  *progress = (SSyncRaftProgress) {
    .matchIndex = i == pRaft->selfIndex ? syncRaftLogLastIndex(pRaft->log) : 0,
    .nextIndex  = syncRaftLogLastIndex(pRaft->log) + 1,
    .inflights  = inflights,
    .isLearner  = false,
    .state      = PROGRESS_STATE_PROBE,
  };
}

/**
 * syncRaftProgressMaybeUpdate is called when an MsgAppResp arrives from the follower, with the
 * index acked by it. The method returns false if the given n index comes from
 * an outdated message. Otherwise it updates the progress and returns true.
 **/
bool syncRaftProgressMaybeUpdate(SSyncRaftProgress* progress, SyncIndex lastIndex) {
  bool updated = false;

  if (progress->matchIndex < lastIndex) {
    progress->matchIndex = lastIndex;
    updated = true;
    probeAcked(progress);
  }

  progress->nextIndex = MAX(progress->nextIndex, lastIndex + 1);

  return updated;
}

bool syncRaftProgressMaybeDecrTo(SSyncRaftProgress* progress,
                                SyncIndex rejected, SyncIndex matchHint) {
  if (progress->state == PROGRESS_STATE_REPLICATE) {
		/** 
     * the rejection must be stale if the progress has matched and "rejected"
		 * is smaller than "match".
     **/
    if (rejected <= progress->matchIndex) {
      syncDebug("match index is up to date,ignore");
      return false;
    }

    /* directly decrease next to match + 1 */
    progress->nextIndex = progress->matchIndex + 1;
    return true;
  }

	/**
   * The rejection must be stale if "rejected" does not match next - 1. This
	 * is because non-replicating followers are probed one entry at a time.
   **/
  if (rejected != progress->nextIndex - 1) {
    syncDebug("rejected index %" PRId64 " different from next index %" PRId64 " -> ignore"
      , rejected, progress->nextIndex);
    return false;
  }

  progress->nextIndex = MAX(MIN(rejected, matchHint + 1), 1);

  progress->probeSent = false;
  return true;
}

/**
 * syncRaftProgressIsPaused returns whether sending log entries to this node has been throttled.
 * This is done when a node has rejected recent MsgApps, is currently waiting
 * for a snapshot, or has reached the MaxInflightMsgs limit. In normal
 * operation, this is false. A throttled node will be contacted less frequently
 * until it has reached a state in which it's able to accept a steady stream of
 * log entries again.
 **/
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
  SSyncRaftProgress** ppProgress = (SSyncRaftProgress**)taosHashGet(progressMap, &id, sizeof(SyncNodeId*));
  if (ppProgress == NULL) {
    return NULL;
  }

  return *ppProgress;
}

int syncRaftAddToProgressMap(SSyncRaftProgressMap* progressMap, SSyncRaftProgress* progress) {
  taosHashPut(progressMap->progressMap, &progress->id, sizeof(SyncNodeId*), &progress, sizeof(SSyncRaftProgress*));
}

void syncRaftRemoveFromProgressMap(SSyncRaftProgressMap* progressMap, SyncNodeId id) {
  taosHashRemove(progressMap->progressMap, &id, sizeof(SyncNodeId*));
}

bool syncRaftProgressIsUptodate(SSyncRaft* pRaft, SSyncRaftProgress* progress) {
  return syncRaftLogLastIndex(pRaft->log) + 1 == progress->nextIndex;
}

/**
 * syncRaftProgressBecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
 * optionally and if larger, the index of the pending snapshot.
 **/
void syncRaftProgressBecomeProbe(SSyncRaftProgress* progress) {
  /**
   * If the original state is ProgressStateSnapshot, progress knows that
	 * the pending snapshot has been sent to this peer successfully, then
	 * probes from pendingSnapshot + 1.
   **/
  if (progress->state == PROGRESS_STATE_SNAPSHOT) {
    SyncIndex pendingSnapshotIndex = progress->pendingSnapshotIndex;
    resetProgressState(progress, PROGRESS_STATE_PROBE);
    progress->nextIndex = MAX(progress->matchIndex + 1, pendingSnapshotIndex + 1);
  } else {
    resetProgressState(progress, PROGRESS_STATE_PROBE);
    progress->nextIndex = progress->matchIndex + 1;
  }
}

/**
 * syncRaftProgressBecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
 **/
void syncRaftProgressBecomeReplicate(SSyncRaftProgress* progress) {
  resetProgressState(progress, PROGRESS_STATE_REPLICATE);
  progress->nextIndex = progress->matchIndex + 1;
}

void syncRaftProgressBecomeSnapshot(SSyncRaftProgress* progress, SyncIndex snapshotIndex) {
  resetProgressState(progress, PROGRESS_STATE_SNAPSHOT);
  progress->pendingSnapshotIndex = snapshotIndex;
}

void syncRaftCopyProgress(const SSyncRaftProgress* progress, SSyncRaftProgress* out) {
  memcpy(out, progress, sizeof(SSyncRaftProgress));
}

bool syncRaftIterateProgressMap(const SSyncRaftNodeMap* nodeMap, SSyncRaftProgress *pProgress) {
  SSyncRaftProgress **ppProgress = taosHashIterate(nodeMap->nodeIdMap, pProgress);
  if (ppProgress == NULL) {
    return true;
  }

  *pProgress = *(*ppProgress);
  return false;
}

/**
 * ResetState moves the Progress into the specified State, resetting ProbeSent,
 * PendingSnapshot, and Inflights.
 **/
static void resetProgressState(SSyncRaftProgress* progress, ESyncRaftProgressState state) {
  progress->probeSent = false;
  progress->pendingSnapshotIndex = 0;
  progress->state = state;
  syncRaftInflightReset(progress->inflights);
}

/**
 * probeAcked is called when this peer has accepted an append. It resets
 * ProbeSent to signal that additional append messages should be sent without
 * further delay.
 **/
static void probeAcked(SSyncRaftProgress* progress) {
  progress->probeSent = false;
}

#if 0

SyncIndex syncRaftProgressNextIndex(SSyncRaft* pRaft, int i) {
  return pRaft->leaderState.progress[i].nextIndex;
}

SyncIndex syncRaftProgressMatchIndex(SSyncRaft* pRaft, int i) {
  return pRaft->leaderState.progress[i].matchIndex;
}

void syncRaftProgressUpdateLastSend(SSyncRaft* pRaft, int i) {
  pRaft->leaderState.progress[i].lastSend = pRaft->io.time(pRaft);
}

void syncRaftProgressUpdateSnapshotLastSend(SSyncRaft* pRaft, int i) {
  pRaft->leaderState.progress[i].lastSendSnapshot = pRaft->io.time(pRaft);
}

bool syncRaftProgressResetRecentRecv(SSyncRaft* pRaft, int i) {
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);
  bool prev = progress->recentRecv;
  progress->recentRecv = false;
  return prev;
}

void syncRaftProgressMarkRecentRecv(SSyncRaft* pRaft, int i) {
  pRaft->leaderState.progress[i].recentRecv = true;
}

bool syncRaftProgressGetRecentRecv(SSyncRaft* pRaft, int i) {
  return pRaft->leaderState.progress[i].recentRecv;
}

void syncRaftProgressBecomeSnapshot(SSyncRaft* pRaft, int i) {
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);
  resetProgressState(progress, PROGRESS_STATE_SNAPSHOT);
  progress->pendingSnapshotIndex = raftLogSnapshotIndex(pRaft->log);
}

void syncRaftProgressBecomeProbe(SSyncRaft* pRaft, int i) {
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);

  if (progress->state == PROGRESS_STATE_SNAPSHOT) {
    assert(progress->pendingSnapshotIndex > 0);
    SyncIndex pendingSnapshotIndex = progress->pendingSnapshotIndex;
    resetProgressState(progress, PROGRESS_STATE_PROBE);
    progress->nextIndex = max(progress->matchIndex + 1, pendingSnapshotIndex);
  } else {
    resetProgressState(progress, PROGRESS_STATE_PROBE);
    progress->nextIndex = progress->matchIndex + 1;
  }
}

void syncRaftProgressBecomeReplicate(SSyncRaft* pRaft, int i) {
  resetProgressState(pRaft->leaderState.progress, PROGRESS_STATE_REPLICATE);
  pRaft->leaderState.progress->nextIndex = pRaft->leaderState.progress->matchIndex + 1;
}

void syncRaftProgressAbortSnapshot(SSyncRaft* pRaft, int i) {
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);
  progress->pendingSnapshotIndex = 0;
  progress->state = PROGRESS_STATE_PROBE;
}

ESyncRaftProgressState syncRaftProgressState(SSyncRaft* pRaft, int i) {
  return pRaft->leaderState.progress[i].state;
}



#endif