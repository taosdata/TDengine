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
#include "wal.h"

typedef struct SSyncLogStoreData {
  SSyncNode* pSyncNode;
  SWal*      pWal;

  TdThreadMutex mutex;
  SWalReader*   pWalHandle;

  // SyncIndex       beginIndex;  // valid begin index, default 0, may be set beginIndex > 0
} SSyncLogStoreData;

SSyncLogStore* logStoreCreate(SSyncNode* pSyncNode);
void           logStoreDestory(SSyncLogStore* pLogStore);
cJSON*         logStore2Json(SSyncLogStore* pLogStore);
char*          logStore2Str(SSyncLogStore* pLogStore);
cJSON*         logStoreSimple2Json(SSyncLogStore* pLogStore);
char*          logStoreSimple2Str(SSyncLogStore* pLogStore);

SyncIndex logStoreFirstIndex(SSyncLogStore* pLogStore);

SyncIndex logStoreWalCommitVer(SSyncLogStore* pLogStore);

// for debug
void logStorePrint(SSyncLogStore* pLogStore);
void logStorePrint2(char* s, SSyncLogStore* pLogStore);
void logStoreLog(SSyncLogStore* pLogStore);
void logStoreLog2(char* s, SSyncLogStore* pLogStore);

void logStoreSimplePrint(SSyncLogStore* pLogStore);
void logStoreSimplePrint2(char* s, SSyncLogStore* pLogStore);
void logStoreSimpleLog(SSyncLogStore* pLogStore);
void logStoreSimpleLog2(char* s, SSyncLogStore* pLogStore);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_LOG_H*/
