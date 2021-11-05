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
#include "raft_progress.h"
#include "raft_replication.h"

static int sendSnapshot(SSyncRaft* pRaft, int i);
static int sendAppendEntries(SSyncRaft* pRaft, int i, SyncIndex index, SyncTerm term);

int syncRaftReplicate(SSyncRaft* pRaft, int i) {
  assert(pRaft->state == TAOS_SYNC_ROLE_LEADER);
  assert(i >= 0 && i < pRaft->leaderState.nProgress);

  SyncNodeId nodeId = pRaft->cluster.nodeInfo[i].nodeId;
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);
  if (syncRaftProgressIsPaused(progress)) {
    syncInfo("node %d paused", nodeId);
    return 0;
  }

  SyncIndex nextIndex = syncRaftProgressNextIndex(progress);
  SyncIndex snapshotIndex = syncRaftLogSnapshotIndex(pRaft->log);
  bool inSnapshot = syncRaftProgressInSnapshot(progress);
  SyncIndex prevIndex;
  SyncTerm prevTerm;

  /**
   * From Section 3.5:
   *
   *   When sending an AppendEntries RPC, the leader includes the index and
   *   term of the entry in its log that immediately precedes the new
   *   entries. If the follower does not find an entry in its log with the
   *   same index and term, then it refuses the new entries. The consistency
   *   check acts as an induction step: the initial empty state of the logs
   *   satisfies the Log Matching Property, and the consistency check
   *   preserves the Log Matching Property whenever logs are extended. As a
   *   result, whenever AppendEntries returns successfully, the leader knows
   *   that the follower's log is identical to its own log up through the new
   *   entries (Log Matching Property in Figure 3.2).
   **/
  if (nextIndex == 1) {
    /**
     * We're including the very first entry, so prevIndex and prevTerm are
     * null. If the first entry is not available anymore, send the last
     * snapshot if we're not already sending one. 
     **/
    if (snapshotIndex > 0 && !inSnapshot) {
      goto send_snapshot;
    }

    // otherwise send append entries from start
    prevIndex = 0;
    prevTerm = 0;    
  } else {
    /**
     * Set prevIndex and prevTerm to the index and term of the entry at
     * nextIndex - 1. 
     **/ 
    prevIndex = nextIndex - 1;
    prevTerm = syncRaftLogTermOf(pRaft->log, prevIndex);
    /**
     * If the entry is not anymore in our log, send the last snapshot if we're
     * not doing so already.
     **/
    if (prevTerm == SYNC_NON_TERM && !inSnapshot) {
      goto send_snapshot;
    }
  }

  /* Send empty AppendEntries RPC when installing a snaphot */
  if (inSnapshot) {
    prevIndex = syncRaftLogLastIndex(pRaft->log);
    prevTerm = syncRaftLogLastTerm(pRaft->log);
  }

  return sendAppendEntries(pRaft, i, prevIndex, prevTerm);

send_snapshot:
  if (syncRaftProgressRecentActive(progress)) {
    /* Only send a snapshot when we have heard from the server */
    return sendSnapshot(pRaft, i);
  } else {
    /* Send empty AppendEntries RPC when we haven't heard from the server */
    prevIndex = syncRaftLogLastIndex(pRaft->log);
    prevTerm  = syncRaftLogLastTerm(pRaft->log);
    return sendAppendEntries(pRaft, i, prevIndex, prevTerm);
  }
}

static int sendSnapshot(SSyncRaft* pRaft, int i) {
  return 0;
}

static int sendAppendEntries(SSyncRaft* pRaft, int i, SyncIndex prevIndex, SyncTerm prevTerm) {
  SyncIndex nextIndex = prevIndex + 1;
  SSyncRaftEntry *entries;
  int nEntry;
  SNodeInfo* pNode = &(pRaft->cluster.nodeInfo[i]);
  SSyncRaftProgress* progress = &(pRaft->leaderState.progress[i]);
  syncRaftLogAcquire(pRaft->log, nextIndex, pRaft->maxMsgSize, &entries, &nEntry);

  SSyncMessage* msg = syncNewAppendMsg(pRaft->selfGroupId, pRaft->selfId, pRaft->term,
                                      prevIndex, prevTerm, pRaft->log->commitIndex,
                                      nEntry, entries);

  if (msg == NULL) {
    goto err_release_log;
  }

  pRaft->io.send(msg, pNode);

  if (syncRaftProgressInReplicate(progress)) {
    SyncIndex lastIndex = nextIndex + nEntry;
    syncRaftProgressOptimisticNextIndex(progress, lastIndex);
    syncRaftInflightAdd(&progress->inflights, lastIndex);
  } else if (syncRaftProgressInProbe(progress)) {
    syncRaftProgressPause(progress);
  } else {

  }

  syncRaftProgressUpdateSendTick(progress, pRaft->currentTick);

  return 0;

err_release_log:
  syncRaftLogRelease(pRaft->log, nextIndex, entries, nEntry);
  return 0;
}