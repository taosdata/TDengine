/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "syncInt.h"
#include "taosdef.h"

int32_t raftLogAppendEntry(struct SSyncLogStore* pLogStore, SSyncBuffer* pBuf);

// get one log entry, user need to free pBuf->data
int32_t raftLogGetEntry(struct SSyncLogStore* pLogStore, SyncIndex index, SSyncBuffer* pBuf);

// update log store commit index with "index"
int32_t raftLogUpdateCommitIndex(struct SSyncLogStore* pLogStore, SyncIndex index);

// truncate log with index, entries after the given index (>index) will be deleted
int32_t raftLogTruncate(struct SSyncLogStore* pLogStore, SyncIndex index);

// return commit index of log
SyncIndex raftLogGetCommitIndex(struct SSyncLogStore* pLogStore);

// return index of last entry
SyncIndex raftLogGetLastIndex(struct SSyncLogStore* pLogStore);

// return term of last entry
SyncTerm raftLogGetLastTerm(struct SSyncLogStore* pLogStore);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_LOG_H*/
