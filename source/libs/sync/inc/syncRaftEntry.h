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

SyncRaftEntry* syncEntryBuild(int32_t dataLen);
SyncRaftEntry* syncEntryBuildFromClientRequest(const SyncClientRequest* pMsg, SyncTerm term, SyncIndex index);
SyncRaftEntry* syncEntryBuildFromRpcMsg(const SRpcMsg* pMsg, SyncTerm term, SyncIndex index);
SyncRaftEntry* syncEntryBuildFromAppendEntries(const SyncAppendEntries* pMsg);
SyncRaftEntry* syncEntryBuildNoop(SyncTerm term, SyncIndex index, int32_t vgId);
void           syncEntryDestroy(SyncRaftEntry* pEntry);
void           syncEntry2OriginalRpc(const SyncRaftEntry* pEntry, SRpcMsg* pRpcMsg);  // step 7

static FORCE_INLINE bool syncLogReplBarrier(SyncRaftEntry* pEntry) {
  return pEntry->originalRpcType == TDMT_SYNC_NOOP || pEntry->originalRpcType == TDMT_SYNC_CONFIG_CHANGE;
}

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_RAFT_ENTRY_H*/
