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
  SyncIndex prevIndex = nextIndex - 1;
  SyncTerm prevTerm = syncRaftLogTermOf(pRaft->log, prevIndex);

  if (prevTerm == SYNC_NON_TERM && !syncRaftProgressInSnapshot(progress)) {
    goto send_snapshot;
  }

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
    return 0;
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
}