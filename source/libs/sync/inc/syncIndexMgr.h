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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "syncInt.h"
#include "taosdef.h"

// SIndexMgr -----------------------------
typedef struct SSyncIndexMgr {
  SRaftId (*replicas)[TSDB_MAX_REPLICA];
  SyncIndex  index[TSDB_MAX_REPLICA];
  SyncTerm   privateTerm[TSDB_MAX_REPLICA];  // for advanced function
  int32_t    replicaNum;
  SSyncNode *pSyncNode;
} SSyncIndexMgr;

SSyncIndexMgr *syncIndexMgrCreate(SSyncNode *pSyncNode);
void           syncIndexMgrUpdate(SSyncIndexMgr *pSyncIndexMgr, SSyncNode *pSyncNode);
void           syncIndexMgrDestroy(SSyncIndexMgr *pSyncIndexMgr);
void           syncIndexMgrClear(SSyncIndexMgr *pSyncIndexMgr);
void           syncIndexMgrSetIndex(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId, SyncIndex index);
SyncIndex      syncIndexMgrGetIndex(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId);
cJSON *        syncIndexMgr2Json(SSyncIndexMgr *pSyncIndexMgr);
char *         syncIndexMgr2Str(SSyncIndexMgr *pSyncIndexMgr);

// void     syncIndexMgrSetTerm(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId, SyncTerm term);
// SyncTerm syncIndexMgrGetTerm(SSyncIndexMgr *pSyncIndexMgr, const SRaftId *pRaftId);

// for debug -------------------
void syncIndexMgrPrint(SSyncIndexMgr *pObj);
void syncIndexMgrPrint2(char *s, SSyncIndexMgr *pObj);
void syncIndexMgrLog(SSyncIndexMgr *pObj);
void syncIndexMgrLog2(char *s, SSyncIndexMgr *pObj);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_INDEX_MGR_H*/
