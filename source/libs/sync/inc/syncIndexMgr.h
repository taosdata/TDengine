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

#ifndef _TD_LIBS_SYNC_INDEX_MGR_H
#define _TD_LIBS_SYNC_INDEX_MGR_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

// SIndexMgr -----------------------------
typedef struct SSyncIndexMgr {
  SRaftId (*replicas)[TSDB_MAX_REPLICA];
  SyncIndex index[TSDB_MAX_REPLICA];
  SyncTerm  privateTerm[TSDB_MAX_REPLICA];  // for advanced function

  int64_t startTimeArr[TSDB_MAX_REPLICA];
  int64_t recvTimeArr[TSDB_MAX_REPLICA];

  int32_t    replicaNum;
  SSyncNode *pSyncNode;
} SSyncIndexMgr;

SSyncIndexMgr *syncIndexMgrCreate(SSyncNode *pSyncNode);
void           syncIndexMgrUpdate(SSyncIndexMgr *pSyncIndexMgr, SSyncNode *pSyncNode);
void           syncIndexMgrDestroy(SSyncIndexMgr *pSyncIndexMgr);
void           syncIndexMgrClear(SSyncIndexMgr *pSyncIndexMgr);
void           syncIndexMgrSetIndex(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId, SyncIndex index);
SyncIndex      syncIndexMgrGetIndex(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId);

void     syncIndexMgrSetStartTime(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId, int64_t startTime);
int64_t  syncIndexMgrGetStartTime(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId);
void     syncIndexMgrSetRecvTime(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId, int64_t recvTime);
int64_t  syncIndexMgrGetRecvTime(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId);
void     syncIndexMgrSetTerm(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId, SyncTerm term);
SyncTerm syncIndexMgrGetTerm(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_INDEX_MGR_H*/
