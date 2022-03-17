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

#ifndef _TD_LIBS_SYNC_RAFT_STORE_H
#define _TD_LIBS_SYNC_RAFT_STORE_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "cJSON.h"
#include "syncInt.h"
#include "taosdef.h"

#define RAFT_STORE_BLOCK_SIZE 512
#define RAFT_STORE_PATH_LEN 128

typedef struct SRaftStore {
  SyncTerm  currentTerm;
  SRaftId   voteFor;
  TdFilePtr pFile;
  char      path[RAFT_STORE_PATH_LEN];
} SRaftStore;

SRaftStore *raftStoreOpen(const char *path);
int32_t     raftStoreClose(SRaftStore *pRaftStore);
int32_t     raftStorePersist(SRaftStore *pRaftStore);
int32_t     raftStoreSerialize(SRaftStore *pRaftStore, char *buf, size_t len);
int32_t     raftStoreDeserialize(SRaftStore *pRaftStore, char *buf, size_t len);

bool    raftStoreHasVoted(SRaftStore *pRaftStore);
void    raftStoreVote(SRaftStore *pRaftStore, SRaftId *pRaftId);
void    raftStoreClearVote(SRaftStore *pRaftStore);
void    raftStoreNextTerm(SRaftStore *pRaftStore);
void    raftStoreSetTerm(SRaftStore *pRaftStore, SyncTerm term);
int32_t raftStoreFromJson(SRaftStore *pRaftStore, cJSON *pJson);
cJSON * raftStore2Json(SRaftStore *pRaftStore);
char *  raftStore2Str(SRaftStore *pRaftStore);

// for debug -------------------
void raftStorePrint(SRaftStore *pObj);
void raftStorePrint2(char *s, SRaftStore *pObj);
void raftStoreLog(SRaftStore *pObj);
void raftStoreLog2(char *s, SRaftStore *pObj);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_STORE_H*/
