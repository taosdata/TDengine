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
 * along with this program. If not, see <http: *www.gnu.org/licenses/>.
 */

#ifndef TD_SYNC_RAFT_PROGRESS_H
#define TD_SYNC_RAFT_PROGRESS_H

#include "sync_type.h"
#include "sync_raft_inflights.h"
#include "thash.h"

/** 
 * State defines how the leader should interact with the follower.
 *
 * When in PROGRESS_STATE_PROBE, leader sends at most one replication message
 * per heartbeat interval. It also probes actual progress of the follower.
 * 
 * When in PROGRESS_STATE_REPLICATE, leader optimistically increases next
 * to the latest entry sent after sending replication message. This is
 * an optimized state for fast replicating log entries to the follower.
 *
 * When in PROGRESS_STATE_SNAPSHOT, leader should have sent out snapshot
 * before and stops sending any replication message.
 * 
 * PROGRESS_STATE_PROBE is the initial state.
 **/
typedef enum ESyncRaftProgressState {
  /**
	* StateProbe indicates a follower whose last index isn't known. Such a
	* follower is "probed" (i.e. an append sent periodically) to narrow down
	* its last index. In the ideal (and common) case, only one round of probing
	* is necessary as the follower will react with a hint. Followers that are
	* probed over extended periods of time are often offline.
  **/
  PROGRESS_STATE_PROBE = 0,

  /**
	* StateReplicate is the state steady in which a follower eagerly receives
	* log entries to append to its log.
  **/
  PROGRESS_STATE_REPLICATE,

  /**
	* StateSnapshot indicates a follower that needs log entries not available
	* from the leader's Raft log. Such a follower needs a full snapshot to
	* return to StateReplicate.
  **/
  PROGRESS_STATE_SNAPSHOT,
} ESyncRaftProgressState;

static const char* kProgressStateString[] = {
	"Probe",
	"Replicate",
	"Snapshot",
};

/**
 * Progress represents a follower’s progress in the view of the leader. Leader maintains
 * progresses of all followers, and sends entries to the follower based on its progress.
 **/
struct SSyncRaftProgress {
	SyncGroupId groupId;

	SyncNodeId id;

  SyncIndex nextIndex;

  SyncIndex matchIndex;

  /**
	* State defines how the leader should interact with the follower.
	*
	* When in StateProbe, leader sends at most one replication message
	* per heartbeat interval. It also probes actual progress of the follower.
	*
	* When in StateReplicate, leader optimistically increases next
	* to the latest entry sent after sending replication message. This is
	* an optimized state for fast replicating log entries to the follower.
	*
	* When in StateSnapshot, leader should have sent out snapshot
	* before and stops sending any replication message.
  **/
  ESyncRaftProgressState state;

	/** 
   * pendingSnapshotIndex is used in PROGRESS_STATE_SNAPSHOT.
	 * If there is a pending snapshot, the pendingSnapshotIndex will be set to the
	 * index of the snapshot. If pendingSnapshotIndex is set, the replication process of
	 * this Progress will be paused. raft will not resend snapshot until the pending one
	 * is reported to be failed.
   **/
  SyncIndex pendingSnapshotIndex;

	/** 
   * recentActive is true if the progress is recently active. Receiving any messages
   * from the corresponding follower indicates the progress is active.
	 * RecentActive can be reset to false after an election timeout.
   **/
  bool recentActive;

  /**
	* probeSent is used while this follower is in StateProbe. When probeSent is
	* true, raft should pause sending replication message to this peer until
	* probeSent is reset. See ProbeAcked() and IsPaused().
  **/
	bool probeSent;

  /**
	 * inflights is a sliding window for the inflight messages.
	 * Each inflight message contains one or more log entries.
	 * The max number of entries per message is defined in raft config as MaxSizePerMsg.
	 * Thus inflight effectively limits both the number of inflight messages
	 * and the bandwidth each Progress can use.
	 * When inflights is Full, no more message should be sent.
	 * When a leader sends out a message, the index of the last
	 * entry should be added to inflights. The index MUST be added
	 * into inflights in order.
	 * When a leader receives a reply, the previous inflights should
	 * be freed by calling inflights.FreeLE with the index of the last
	 * received entry.
  **/
  SSyncRaftInflights* inflights;

  /**
   *  IsLearner is true if this progress is tracked for a learner.
   **/
  bool isLearner;
};

struct SSyncRaftProgressMap {
	// map nodeId -> SSyncRaftProgress*
	SHashObj* progressMap;
};

static FORCE_INLINE const char* syncRaftProgressStateString(const SSyncRaftProgress* progress) {
	return kProgressStateString[progress->state];
}

void syncRaftInitProgress(int i, SSyncRaft* pRaft, SSyncRaftProgress* progress);

/**
 * syncRaftProgressBecomeProbe transitions into StateProbe. Next is reset to Match+1 or,
 * optionally and if larger, the index of the pending snapshot.
 **/
void syncRaftProgressBecomeProbe(SSyncRaftProgress* progress);

/**
 * syncRaftProgressBecomeReplicate transitions into StateReplicate, resetting Next to Match+1.
 **/ 
void syncRaftProgressBecomeReplicate(SSyncRaftProgress* progress);

/**
 * syncRaftProgressMaybeUpdate is called when an MsgAppResp arrives from the follower, with the
 * index acked by it. The method returns false if the given n index comes from
 * an outdated message. Otherwise it updates the progress and returns true.
 **/
bool syncRaftProgressMaybeUpdate(SSyncRaftProgress* progress, SyncIndex lastIndex);

/**
 * syncRaftProgressOptimisticNextIndex signals that appends all the way up to and including index n
 * are in-flight. As a result, Next is increased to n+1.
 **/
static FORCE_INLINE void syncRaftProgressOptimisticNextIndex(SSyncRaftProgress* progress, SyncIndex nextIndex) {
  progress->nextIndex = nextIndex + 1;
}

/**
 * syncRaftProgressMaybeDecrTo adjusts the Progress to the receipt of a MsgApp rejection. The
 * arguments are the index of the append message rejected by the follower, and
 * the hint that we want to decrease to.
 *
 * Rejections can happen spuriously as messages are sent out of order or
 * duplicated. In such cases, the rejection pertains to an index that the
 * Progress already knows were previously acknowledged, and false is returned
 * without changing the Progress.
 *
 * If the rejection is genuine, Next is lowered sensibly, and the Progress is
 * cleared for sending log entries.
**/
bool syncRaftProgressMaybeDecrTo(SSyncRaftProgress* progress,
                                SyncIndex rejected, SyncIndex matchHint);

/**
 * syncRaftProgressIsPaused returns whether sending log entries to this node has been throttled.
 * This is done when a node has rejected recent MsgApps, is currently waiting
 * for a snapshot, or has reached the MaxInflightMsgs limit. In normal
 * operation, this is false. A throttled node will be contacted less frequently
 * until it has reached a state in which it's able to accept a steady stream of
 * log entries again.
 **/
bool syncRaftProgressIsPaused(SSyncRaftProgress* progress);

static FORCE_INLINE SyncIndex syncRaftProgressNextIndex(SSyncRaftProgress* progress) {
  return progress->nextIndex;
}

static FORCE_INLINE ESyncRaftProgressState syncRaftProgressInReplicate(SSyncRaftProgress* progress) {
  return progress->state == PROGRESS_STATE_REPLICATE;
}

static FORCE_INLINE ESyncRaftProgressState syncRaftProgressInSnapshot(SSyncRaftProgress* progress) {
  return progress->state == PROGRESS_STATE_SNAPSHOT;
}

static FORCE_INLINE ESyncRaftProgressState syncRaftProgressInProbe(SSyncRaftProgress* progress) {
  return progress->state == PROGRESS_STATE_PROBE;
}

static FORCE_INLINE bool syncRaftProgressRecentActive(SSyncRaftProgress* progress) {
  return progress->recentActive;
}

SSyncRaftProgress* syncRaftFindProgressByNodeId(const SSyncRaftProgressMap* progressMap, SyncNodeId id);

int syncRaftAddToProgressMap(SSyncRaftProgressMap* progressMap, SSyncRaftProgress* progress);

void syncRaftRemoveFromProgressMap(SSyncRaftProgressMap* progressMap, SyncNodeId id);

/** 
 * return true if progress's log is up-todate
 **/
bool syncRaftProgressIsUptodate(SSyncRaft* pRaft, SSyncRaftProgress* progress);

void syncRaftProgressBecomeSnapshot(SSyncRaftProgress* progress, SyncIndex snapshotIndex);

void syncRaftCopyProgress(const SSyncRaftProgress* from, SSyncRaftProgress* to);

// return true if reach the end
bool syncRaftIterateProgressMap(const SSyncRaftNodeMap* nodeMap, SSyncRaftProgress *pProgress);

#if 0

void syncRaftProgressAbortSnapshot(SSyncRaft* pRaft, int i);



SyncIndex syncRaftProgressMatchIndex(SSyncRaft* pRaft, int i);

void syncRaftProgressUpdateLastSend(SSyncRaft* pRaft, int i);

void syncRaftProgressUpdateSnapshotLastSend(SSyncRaft* pRaft, int i);

bool syncRaftProgressResetRecentRecv(SSyncRaft* pRaft, int i);

void syncRaftProgressMarkRecentRecv(SSyncRaft* pRaft, int i);



void syncRaftProgressAbortSnapshot(SSyncRaft* pRaft, int i);

#endif

#endif /* TD_SYNC_RAFT_PROGRESS_H */