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
#include "syncRaftEntry.h"
#include "taosdef.h"

typedef struct SSyncLogStoreData {
  SSyncNode* pSyncNode;
  SWal*      pWal;
} SSyncLogStoreData;

SSyncLogStore* logStoreCreate(SSyncNode* pSyncNode);

void logStoreDestory(SSyncLogStore* pLogStore);

// append one log entry
int32_t logStoreAppendEntry(SSyncLogStore* pLogStore, SRpcMsg* pEntry);

// get one log entry, user need to free pEntry->pCont
int32_t logStoreGetEntry(SSyncLogStore* pLogStore, SyncIndex index, SRpcMsg* pEntry);

// truncate log with index, entries after the given index (>=index) will be deleted
int32_t logStoreTruncate(SSyncLogStore* pLogStore, SyncIndex fromIndex);

// return index of last entry
SyncIndex logStoreLastIndex(SSyncLogStore* pLogStore);

// return term of last entry
SyncTerm logStoreLastTerm(SSyncLogStore* pLogStore);

// update log store commit index with "index"
int32_t logStoreUpdateCommitIndex(SSyncLogStore* pLogStore, SyncIndex index);

// return commit index of log
SyncIndex logStoreGetCommitIndex(SSyncLogStore* pLogStore);

cJSON* logStore2Json(SSyncLogStore* pLogStore);

char* logStore2Str(SSyncLogStore* pLogStore);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_LOG_H*/
