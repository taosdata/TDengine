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

#ifndef _TD_LIBS_SYNC_RAFT_LOG_H
#define _TD_LIBS_SYNC_RAFT_LOG_H

#include "sync.h"
#include "sync_type.h"

typedef enum ESyncRaftEntryType {
  SYNC_ENTRY_TYPE_LOG = 1,
} ESyncRaftEntryType;

struct SSyncRaftEntry {
  SyncTerm term;

  SyncIndex index;

  ESyncRaftEntryType type;

  SSyncBuffer buffer;
};

struct SSyncRaftLog {
  SyncIndex uncommittedConfigIndex;

  SyncIndex commitIndex;

  SyncIndex appliedIndex;
};

SSyncRaftLog* syncRaftLogOpen();

SyncIndex syncRaftLogLastIndex(SSyncRaftLog* pLog);

SyncIndex syncRaftLogSnapshotIndex(SSyncRaftLog* pLog);

SyncTerm syncRaftLogLastTerm(SSyncRaftLog* pLog);

void syncRaftLogAppliedTo(SSyncRaftLog* pLog, SyncIndex appliedIndex);

bool syncRaftLogIsUptodate(SSyncRaftLog* pLog, SyncIndex index, SyncTerm term);

int syncRaftLogNumOfPendingConf(SSyncRaftLog* pLog);

bool syncRaftHasUnappliedLog(SSyncRaftLog* pLog);

SyncTerm syncRaftLogTermOf(SSyncRaftLog* pLog, SyncIndex index);

int syncRaftLogAppend(SSyncRaftLog* pLog, SSyncRaftEntry *pEntries, int n);

int syncRaftLogAcquire(SSyncRaftLog* pLog, SyncIndex index, int maxMsgSize,
                      SSyncRaftEntry **ppEntries, int *n);

void syncRaftLogRelease(SSyncRaftLog* pLog, SyncIndex index,
                      SSyncRaftEntry *pEntries, int n);

bool syncRaftLogMatchTerm();

static FORCE_INLINE bool syncRaftLogIsCommitted(SSyncRaftLog* pLog, SyncIndex index) {
  return pLog->commitIndex > index;
}

#endif  /* _TD_LIBS_SYNC_RAFT_LOG_H */
