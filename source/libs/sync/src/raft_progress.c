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
#include "raft_unstable_log.h"
#include "raft_progress.h"
#include "sync.h"
#include "syncInt.h"

static void resetProgressState(SSyncRaftProgress* progress, RaftProgressState state);

static void resumeProgress(SSyncRaftProgress* progress);
static void pauseProgress(SSyncRaftProgress* progress);

int syncRaftProgressCreate(SSyncRaft* pRaft) {

/*
  inflights->buffer = (SyncIndex*)malloc(sizeof(SyncIndex) * pRaft->maxInflightMsgs);
  if (inflights->buffer == NULL) {
    return RAFT_OOM;
  }
  inflights->size  = pRaft->maxInflightMsgs;
*/
}

/*
int syncRaftProgressRecreate(SSyncRaft* pRaft, const RaftConfiguration* configuration) {

}
*/

bool syncRaftProgressMaybeUpdate(SSyncRaft* pRaft, int i, SyncIndex lastIndex) {
  assert(i >= 0 && i < pRaft->leaderState.nProgress);
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);
  bool updated = false;

  if (progress->matchIndex < lastIndex) {
    progress->matchIndex = lastIndex;
    updated = true;
    resumeProgress(progress);
  }
  if (progress->nextIndex < lastIndex + 1) { 
    progress->nextIndex = lastIndex + 1;
  }

  return updated;
}

void syncRaftProgressOptimisticNextIndex(SSyncRaft* pRaft, int i, SyncIndex nextIndex) {
  assert(i >= 0 && i < pRaft->leaderState.nProgress);
  pRaft->leaderState.progress[i].nextIndex = nextIndex + 1;
}

bool syncRaftProgressMaybeDecrTo(SSyncRaft* pRaft, int i,
                                SyncIndex rejected, SyncIndex lastIndex) {
  assert(i >= 0 && i < pRaft->leaderState.nProgress);
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);

  if (progress->state == PROGRESS_REPLICATE) {
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
    //syncRaftProgressBecomeProbe(raft, i);
    return true;
  }

  if (rejected != progress->nextIndex - 1) {
    syncDebug("rejected index %" PRId64 " different from next index %" PRId64 " -> ignore"
      , rejected, progress->nextIndex);
    return false;
  }

  progress->nextIndex = MIN(rejected, lastIndex + 1);
  if (progress->nextIndex < 1) {
    progress->nextIndex = 1;
  }

  resumeProgress(progress);
  return true;
}

static void resumeProgress(SSyncRaftProgress* progress) {
  progress->paused = false;
}

static void pauseProgress(SSyncRaftProgress* progress) {
  progress->paused = true;
}

bool syncRaftProgressIsPaused(SSyncRaft* pRaft, int i) {
  assert(i >= 0 && i < pRaft->leaderState.nProgress);

  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);

  switch (progress->state) {
    case PROGRESS_PROBE:
      return progress->paused;
    case PROGRESS_REPLICATE:
      return syncRaftInflightFull(&progress->inflights);
    case PROGRESS_SNAPSHOT:
      return true;
    default:
      syncFatal("error sync state:%d", progress->state);
  }
}

void syncRaftProgressFailure(SSyncRaft* pRaft, int i) {
  assert(i >= 0 && i < pRaft->leaderState.nProgress);

  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);

  progress->pendingSnapshotIndex = 0;
}

bool syncRaftProgressNeedAbortSnapshot(SSyncRaft* pRaft, int i) {
  assert(i >= 0 && i < pRaft->leaderState.nProgress);
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);

  return progress->state == PROGRESS_SNAPSHOT && progress->matchIndex >= progress->pendingSnapshotIndex;
}

bool syncRaftProgressIsUptodate(SSyncRaft* pRaft, int i) {
  assert(i >= 0 && i < pRaft->leaderState.nProgress);
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);
  return syncRaftLogLastIndex(pRaft->log) + 1 == progress->nextIndex;
}

void syncRaftProgressBecomeProbe(SSyncRaft* pRaft, int i) {
  assert(i >= 0 && i < pRaft->leaderState.nProgress);
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);
  /**
   * If the original state is ProgressStateSnapshot, progress knows that
	 * the pending snapshot has been sent to this peer successfully, then
	 * probes from pendingSnapshot + 1.
   **/
  if (progress->state == PROGRESS_SNAPSHOT) {
    SyncIndex pendingSnapshotIndex = progress->pendingSnapshotIndex;
    resetProgressState(progress, PROGRESS_PROBE);
    progress->nextIndex = MAX(progress->matchIndex + 1, pendingSnapshotIndex + 1);
  } else {
    resetProgressState(progress, PROGRESS_PROBE);
    progress->nextIndex = progress->matchIndex + 1;
  }
}

void syncRaftProgressBecomeReplicate(SSyncRaft* pRaft, int i) {
  assert(i >= 0 && i < pRaft->leaderState.nProgress);
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);
  resetProgressState(progress, PROGRESS_REPLICATE);
  progress->nextIndex = progress->matchIndex + 1;
}

void syncRaftProgressBecomeSnapshot(SSyncRaft* pRaft, int i, SyncIndex snapshotIndex) {
  assert(i >= 0 && i < pRaft->leaderState.nProgress);
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);
  resetProgressState(progress, PROGRESS_SNAPSHOT);
  progress->pendingSnapshotIndex = snapshotIndex;
}

int syncRaftInflightReset(SSyncRaftInflights* inflights) {  
  inflights->count = 0;
  inflights->start = 0;
  
  return 0;
}

bool syncRaftInflightFull(SSyncRaftInflights* inflights) {
  return inflights->count == inflights->size;
}

void syncRaftInflightAdd(SSyncRaftInflights* inflights, SyncIndex inflightIndex) {
  assert(!syncRaftInflightFull(inflights));

  int next = inflights->start + inflights->count;
  int size = inflights->size;
  /* is next wrapped around buffer? */
  if (next >= size) {
    next -= size;
  }

  inflights->buffer[next] = inflightIndex;
  inflights->count++;
}

void syncRaftInflightFreeTo(SSyncRaftInflights* inflights, SyncIndex toIndex) {
  if (inflights->count == 0 || toIndex < inflights->buffer[inflights->start]) {
    return;
  }

  int i, idx;
  for (i = 0, idx = inflights->start; i < inflights->count; i++) {
    if (toIndex < inflights->buffer[idx]) {
      break;
    }

    int size = inflights->size;
    idx++;
    if (idx >= size) {
      idx -= size;
    }
  }

  inflights->count -= i;
  inflights->start  = idx;
  assert(inflights->count >= 0);
  if (inflights->count == 0) {
    inflights->start = 0;
  }
}

void syncRaftInflightFreeFirstOne(SSyncRaftInflights* inflights) {
  syncRaftInflightFreeTo(inflights, inflights->buffer[inflights->start]);
}

static void resetProgressState(SSyncRaftProgress* progress, RaftProgressState state) {
  progress->paused = false;
  progress->pendingSnapshotIndex = 0;
  progress->state = state;
  syncRaftInflightReset(&(progress->inflights));
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
  resetProgressState(progress, PROGRESS_SNAPSHOT);
  progress->pendingSnapshotIndex = raftLogSnapshotIndex(pRaft->log);
}

void syncRaftProgressBecomeProbe(SSyncRaft* pRaft, int i) {
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);

  if (progress->state == PROGRESS_SNAPSHOT) {
    assert(progress->pendingSnapshotIndex > 0);
    SyncIndex pendingSnapshotIndex = progress->pendingSnapshotIndex;
    resetProgressState(progress, PROGRESS_PROBE);
    progress->nextIndex = max(progress->matchIndex + 1, pendingSnapshotIndex);
  } else {
    resetProgressState(progress, PROGRESS_PROBE);
    progress->nextIndex = progress->matchIndex + 1;
  }
}

void syncRaftProgressBecomeReplicate(SSyncRaft* pRaft, int i) {
  resetProgressState(pRaft->leaderState.progress, PROGRESS_REPLICATE);
  pRaft->leaderState.progress->nextIndex = pRaft->leaderState.progress->matchIndex + 1;
}

void syncRaftProgressAbortSnapshot(SSyncRaft* pRaft, int i) {
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);
  progress->pendingSnapshotIndex = 0;
  progress->state = PROGRESS_PROBE;
}

RaftProgressState syncRaftProgressState(SSyncRaft* pRaft, int i) {
  return pRaft->leaderState.progress[i].state;
}



#endif