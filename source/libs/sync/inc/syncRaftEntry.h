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

#ifndef _TD_LIBS_SYNC_RAFT_ENTRY_H
#define _TD_LIBS_SYNC_RAFT_ENTRY_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "syncInt.h"
#include "syncMessage.h"
#include "taosdef.h"
#include "tskiplist.h"

typedef struct SSyncRaftEntry {
  uint32_t  bytes;
  uint32_t  msgType;          // TDMT_SYNC_CLIENT_REQUEST
  uint32_t  originalRpcType;  // origin RpcMsg msgType
  uint64_t  seqNum;
  bool      isWeak;
  SyncTerm  term;
  SyncIndex index;
  uint32_t  dataLen;  // origin RpcMsg.contLen
  char      data[];   // origin RpcMsg.pCont
} SSyncRaftEntry;

SSyncRaftEntry* syncEntryBuild(uint32_t dataLen);
SSyncRaftEntry* syncEntryBuild2(SyncClientRequest* pMsg, SyncTerm term, SyncIndex index);  // step 4
SSyncRaftEntry* syncEntryBuild3(SyncClientRequest* pMsg, SyncTerm term, SyncIndex index);
SSyncRaftEntry* syncEntryBuild4(SRpcMsg* pOriginalMsg, SyncTerm term, SyncIndex index);
SSyncRaftEntry* syncEntryBuildNoop(SyncTerm term, SyncIndex index, int32_t vgId);
void            syncEntryDestory(SSyncRaftEntry* pEntry);
char*           syncEntrySerialize(const SSyncRaftEntry* pEntry, uint32_t* len);  // step 5
SSyncRaftEntry* syncEntryDeserialize(const char* buf, uint32_t len);              // step 6
cJSON*          syncEntry2Json(const SSyncRaftEntry* pEntry);
char*           syncEntry2Str(const SSyncRaftEntry* pEntry);
void            syncEntry2OriginalRpc(const SSyncRaftEntry* pEntry, SRpcMsg* pRpcMsg);  // step 7

// for debug ----------------------
void syncEntryPrint(const SSyncRaftEntry* pObj);
void syncEntryPrint2(char* s, const SSyncRaftEntry* pObj);
void syncEntryLog(const SSyncRaftEntry* pObj);
void syncEntryLog2(char* s, const SSyncRaftEntry* pObj);

//-----------------------------------
typedef struct SRaftEntryHashCache {
  SHashObj*     pEntryHash;
  int32_t       maxCount;
  int32_t       currentCount;
  TdThreadMutex mutex;
  SSyncNode*    pSyncNode;
} SRaftEntryHashCache;

SRaftEntryHashCache* raftCacheCreate(SSyncNode* pSyncNode, int32_t maxCount);
void                 raftCacheDestroy(SRaftEntryHashCache* pCache);
int32_t              raftCachePutEntry(struct SRaftEntryHashCache* pCache, SSyncRaftEntry* pEntry);
int32_t              raftCacheGetEntry(struct SRaftEntryHashCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry);
int32_t              raftCacheGetEntryP(struct SRaftEntryHashCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry);
int32_t              raftCacheDelEntry(struct SRaftEntryHashCache* pCache, SyncIndex index);
int32_t              raftCacheGetAndDel(struct SRaftEntryHashCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry);
int32_t              raftCacheClear(struct SRaftEntryHashCache* pCache);

cJSON* raftCache2Json(SRaftEntryHashCache* pObj);
char*  raftCache2Str(SRaftEntryHashCache* pObj);
void   raftCachePrint(SRaftEntryHashCache* pObj);
void   raftCachePrint2(char* s, SRaftEntryHashCache* pObj);
void   raftCacheLog(SRaftEntryHashCache* pObj);
void   raftCacheLog2(char* s, SRaftEntryHashCache* pObj);

//-----------------------------------
typedef struct SRaftEntryCache {
  SSkipList*    pSkipList;
  int32_t       maxCount;
  int32_t       currentCount;
  TdThreadMutex mutex;
  SSyncNode*    pSyncNode;
} SRaftEntryCache;

SRaftEntryCache* raftEntryCacheCreate(SSyncNode* pSyncNode, int32_t maxCount);
void             raftEntryCacheDestroy(SRaftEntryCache* pCache);
int32_t          raftEntryCachePutEntry(struct SRaftEntryCache* pCache, SSyncRaftEntry* pEntry);
int32_t          raftEntryCacheGetEntry(struct SRaftEntryCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry);
int32_t          raftEntryCacheGetEntryP(struct SRaftEntryCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry);
int32_t          raftEntryCacheClear(struct SRaftEntryCache* pCache, int32_t count);

cJSON* raftEntryCache2Json(SRaftEntryCache* pObj);
char*  raftEntryCache2Str(SRaftEntryCache* pObj);
void   raftEntryCachePrint(SRaftEntryCache* pObj);
void   raftEntryCachePrint2(char* s, SRaftEntryCache* pObj);
void   raftEntryCacheLog(SRaftEntryCache* pObj);
void   raftEntryCacheLog2(char* s, SRaftEntryCache* pObj);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_ENTRY_H*/
