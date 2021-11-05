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

#include "raft_log.h"

SSyncRaftLog* syncRaftLogOpen() {
  return NULL;
}

SyncIndex syncRaftLogLastIndex(SSyncRaftLog* pLog) {
  return 0;
}

SyncTerm syncRaftLogLastTerm(SSyncRaftLog* pLog) {
  return 0;
}

bool syncRaftLogIsUptodate(SSyncRaftLog* pLog, SyncIndex index, SyncTerm term) {
  return true;
}

int syncRaftLogNumOfPendingConf(SSyncRaftLog* pLog) {
  return 0;
}

bool syncRaftHasUnappliedLog(SSyncRaftLog* pLog) {
  return pLog->commitIndex > pLog->appliedIndex;
}

SyncTerm syncRaftLogTermOf(SSyncRaftLog* pLog, SyncIndex index) {
  return SYNC_NON_TERM;
}

int syncRaftLogAcquire(SSyncRaftLog* pLog, SyncIndex index, int maxMsgSize,
                      SSyncRaftEntry **ppEntries, int *n) {
  return 0;
}