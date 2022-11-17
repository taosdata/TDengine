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

#include "syncInt.h"
#include "syncMessage.h"
#include "tskiplist.h"

typedef struct SSyncRaftEntry {
  uint32_t  bytes;
  uint32_t  msgType;          // TDMT_SYNC_CLIENT_REQUEST
  uint32_t  originalRpcType;  // origin RpcMsg msgType
  uint64_t  seqNum;
  bool      isWeak;
  SyncTerm  term;
  SyncIndex index;
  int64_t   rid;
  uint32_t  dataLen;  // origin RpcMsg.contLen
  char      data[];   // origin RpcMsg.pCont
} SSyncRaftEntry;

SSyncRaftEntry* syncEntryBuild(int32_t dataLen);
SSyncRaftEntry* syncEntryBuildFromClientRequest(const SyncClientRequest* pMsg, SyncTerm term, SyncIndex index);
SSyncRaftEntry* syncEntryBuildFromRpcMsg(const SRpcMsg* pMsg, SyncTerm term, SyncIndex index);
SSyncRaftEntry* syncEntryBuildFromAppendEntries(const SyncAppendEntries* pMsg);
SSyncRaftEntry* syncEntryBuildNoop(SyncTerm term, SyncIndex index, int32_t vgId);
void            syncEntryDestroy(SSyncRaftEntry* pEntry);
void            syncEntry2OriginalRpc(const SSyncRaftEntry* pEntry, SRpcMsg* pRpcMsg);  // step 7

static FORCE_INLINE bool syncLogIsReplicationBarrier(SSyncRaftEntry* pEntry) {
  return pEntry->originalRpcType == TDMT_SYNC_NOOP;
}

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

typedef struct SRaftEntryCache {
  SSkipList*    pSkipList;
  int32_t       maxCount;
  int32_t       currentCount;
  int32_t       refMgr;
  TdThreadMutex mutex;
  SSyncNode*    pSyncNode;
} SRaftEntryCache;

SRaftEntryCache* raftEntryCacheCreate(SSyncNode* pSyncNode, int32_t maxCount);
void             raftEntryCacheDestroy(SRaftEntryCache* pCache);
int32_t          raftEntryCachePutEntry(struct SRaftEntryCache* pCache, SSyncRaftEntry* pEntry);
int32_t          raftEntryCacheGetEntry(struct SRaftEntryCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry);
int32_t          raftEntryCacheGetEntryP(struct SRaftEntryCache* pCache, SyncIndex index, SSyncRaftEntry** ppEntry);
int32_t          raftEntryCacheClear(struct SRaftEntryCache* pCache, int32_t count);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_ENTRY_H*/
