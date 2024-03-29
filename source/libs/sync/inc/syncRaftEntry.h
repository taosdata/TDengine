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

static FORCE_INLINE bool syncLogReplBarrier(SSyncRaftEntry* pEntry) {
  return pEntry->originalRpcType == TDMT_SYNC_NOOP || pEntry->originalRpcType == TDMT_SYNC_CONFIG_CHANGE;
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_ENTRY_H*/
