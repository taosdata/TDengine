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

#ifndef _TD_LIBS_SYNC_RAFT_CFG_H
#define _TD_LIBS_SYNC_RAFT_CFG_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "cJSON.h"
#include "syncInt.h"
#include "taosdef.h"

#define CONFIG_FILE_LEN 2048

#define MAX_CONFIG_INDEX_COUNT 512

typedef struct SRaftCfg {
  SSyncCfg  cfg;
  TdFilePtr pFile;
  char      path[TSDB_FILENAME_LEN * 2];
  int8_t    isStandBy;
  int32_t   batchSize;
  int8_t    snapshotStrategy;
  SyncIndex lastConfigIndex;

  SyncIndex configIndexArr[MAX_CONFIG_INDEX_COUNT];
  int32_t   configIndexCount;

} SRaftCfg;

SRaftCfg *raftCfgOpen(const char *path);
int32_t   raftCfgClose(SRaftCfg *pRaftCfg);
int32_t   raftCfgPersist(SRaftCfg *pRaftCfg);
int32_t   raftCfgAddConfigIndex(SRaftCfg *pRaftCfg, SyncIndex configIndex);

cJSON * syncCfg2Json(SSyncCfg *pSyncCfg);
char *  syncCfg2Str(SSyncCfg *pSyncCfg);
char *  syncCfg2SimpleStr(SSyncCfg *pSyncCfg);
int32_t syncCfgFromJson(const cJSON *pRoot, SSyncCfg *pSyncCfg);
int32_t syncCfgFromStr(const char *s, SSyncCfg *pSyncCfg);

cJSON * raftCfg2Json(SRaftCfg *pRaftCfg);
char *  raftCfg2Str(SRaftCfg *pRaftCfg);
int32_t raftCfgFromJson(const cJSON *pRoot, SRaftCfg *pRaftCfg);
int32_t raftCfgFromStr(const char *s, SRaftCfg *pRaftCfg);

typedef struct SRaftCfgMeta {
  int8_t    isStandBy;
  int32_t   batchSize;
  int8_t    snapshotStrategy;
  SyncIndex lastConfigIndex;
} SRaftCfgMeta;

int32_t raftCfgCreateFile(SSyncCfg *pCfg, SRaftCfgMeta meta, const char *path);

// for debug ----------------------
void syncCfgPrint(SSyncCfg *pCfg);
void syncCfgPrint2(char *s, SSyncCfg *pCfg);
void syncCfgLog(SSyncCfg *pCfg);
void syncCfgLog2(char *s, SSyncCfg *pCfg);
void syncCfgLog3(char *s, SSyncCfg *pCfg);

void raftCfgPrint(SRaftCfg *pCfg);
void raftCfgPrint2(char *s, SRaftCfg *pCfg);
void raftCfgLog(SRaftCfg *pCfg);
void raftCfgLog2(char *s, SRaftCfg *pCfg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_CFG_H*/
