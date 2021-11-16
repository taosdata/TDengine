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
#include "syncInt.h"
#include "raft_replication.h"

static bool sendSnapshot(SSyncRaft* pRaft, SSyncRaftProgress* progress);
static bool sendAppendEntries(SSyncRaft* pRaft, SSyncRaftProgress* progress,
                              SyncIndex prevIndex, SyncTerm prevTerm,
                              const SSyncRaftEntry *entries, int nEntry);

// syncRaftReplicate sends an append RPC with new entries to the given peer,
// if necessary. Returns true if a message was sent. The sendIfEmpty
// argument controls whether messages with no entries will be sent
// ("empty" messages are useful to convey updated Commit indexes, but
// are undesirable when we're sending multiple messages in a batch).
bool syncRaftReplicate(SSyncRaft* pRaft, SSyncRaftProgress* progress, bool sendIfEmpty) {
  assert(pRaft->state == TAOS_SYNC_STATE_LEADER);
  SyncNodeId nodeId = progress->id;

  if (syncRaftProgressIsPaused(progress)) {
    syncInfo("node [%d:%d] paused", pRaft->selfGroupId, nodeId);
    return false;
  }

  SyncIndex nextIndex = syncRaftProgressNextIndex(progress);
  SSyncRaftEntry *entries;
  int nEntry;   
  SyncIndex prevIndex;
  SyncTerm prevTerm;

  prevIndex = nextIndex - 1;
  prevTerm = syncRaftLogTermOf(pRaft->log, prevIndex);
  int ret = syncRaftLogAcquire(pRaft->log, nextIndex, pRaft->maxMsgSize, &entries, &nEntry);

  if (nEntry == 0 && !sendIfEmpty) {
    return false;
  }

  if (ret != 0 || prevTerm == SYNC_NON_TERM) {
    return sendSnapshot(pRaft, progress);
  }

  return sendAppendEntries(pRaft, progress, prevIndex, prevTerm, entries, nEntry);
}

static bool sendSnapshot(SSyncRaft* pRaft, SSyncRaftProgress* progress) {
  if (!syncRaftProgressRecentActive(progress)) {
    return false;
  }
  return true;
}

static bool sendAppendEntries(SSyncRaft* pRaft, SSyncRaftProgress* progress,
                              SyncIndex prevIndex, SyncTerm prevTerm,
                              const SSyncRaftEntry *entries, int nEntry) {
  SyncIndex lastIndex;
  SyncTerm logTerm = prevTerm;
  SNodeInfo* pNode = &(pRaft->cluster.nodeInfo[progress->selfIndex]);

  SSyncMessage* msg = syncNewAppendMsg(pRaft->selfGroupId, pRaft->selfId, pRaft->term,
                                      prevIndex, prevTerm, pRaft->log->commitIndex,
                                      nEntry, entries);

  if (msg == NULL) {
    goto err_release_log;
  }

  if (nEntry != 0) {
    switch (progress->state) {
    // optimistically increase the next when in StateReplicate
    case PROGRESS_STATE_REPLICATE:
      lastIndex = entries[nEntry - 1].index;
      syncRaftProgressOptimisticNextIndex(progress, lastIndex);
      syncRaftInflightAdd(&progress->inflights, lastIndex);
      break;
    case PROGRESS_STATE_PROBE:
      progress->probeSent = true;
      break;
    default:
      syncFatal("[%d:%d] is sending append in unhandled state %s", 
                pRaft->selfGroupId, pRaft->selfId, syncRaftProgressStateString(progress));
      break;
    }
  }
  pRaft->io.send(msg, pNode);
  return true;

err_release_log:
  syncRaftLogRelease(pRaft->log, prevIndex + 1, entries, nEntry);
  return false;
}
