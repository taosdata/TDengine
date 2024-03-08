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

#define _DEFAULT_SOURCE
#include "syncRaftEntry.h"
#include "syncUtil.h"
#include "tref.h"

SyncRaftEntry* syncEntryBuild(int32_t dataLen) {
  int32_t        bytes = sizeof(SyncRaftEntry) + dataLen;
  SyncRaftEntry* pEntry = taosMemoryCalloc(1, bytes);
  if (pEntry == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pEntry->bytes = bytes;
  pEntry->dataLen = dataLen;
  pEntry->rid = -1;

  return pEntry;
}

SyncRaftEntry* syncEntryBuildFromClientRequest(const SyncClientRequest* pMsg, SyncTerm term, SyncIndex index) {
  SyncRaftEntry* pEntry = syncEntryBuild(pMsg->dataLen);
  if (pEntry == NULL) return NULL;

  pEntry->msgType = pMsg->msgType;
  pEntry->originalRpcType = pMsg->originalRpcType;
  pEntry->seqNum = pMsg->seqNum;
  pEntry->isWeak = pMsg->isWeak;
  pEntry->term = term;
  pEntry->index = index;
  memcpy(pEntry->data, pMsg->data, pMsg->dataLen);

  return pEntry;
}

SyncRaftEntry* syncEntryBuildFromRpcMsg(const SRpcMsg* pMsg, SyncTerm term, SyncIndex index) {
  SyncRaftEntry* pEntry = syncEntryBuild(pMsg->contLen);
  if (pEntry == NULL) return NULL;

  pEntry->msgType = TDMT_SYNC_CLIENT_REQUEST;
  pEntry->originalRpcType = pMsg->msgType;
  pEntry->seqNum = 0;
  pEntry->isWeak = 0;
  pEntry->term = term;
  pEntry->index = index;
  memcpy(pEntry->data, pMsg->pCont, pMsg->contLen);

  return pEntry;
}

SyncRaftEntry* syncEntryBuildFromAppendEntries(const SyncAppendEntries* pMsg) {
  SyncRaftEntry* pEntry = taosMemoryMalloc(pMsg->dataLen);
  if (pEntry == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  memcpy(pEntry, pMsg->data, pMsg->dataLen);
  ASSERT(pEntry->bytes == pMsg->dataLen);
  return pEntry;
}

SyncRaftEntry* syncEntryBuildNoop(SyncTerm term, SyncIndex index, int32_t vgId) {
  SyncRaftEntry* pEntry = syncEntryBuild(sizeof(SMsgHead));
  if (pEntry == NULL) return NULL;

  pEntry->msgType = TDMT_SYNC_CLIENT_REQUEST;
  pEntry->originalRpcType = TDMT_SYNC_NOOP;
  pEntry->seqNum = 0;
  pEntry->isWeak = 0;
  pEntry->term = term;
  pEntry->index = index;

  SMsgHead* pHead = (SMsgHead*)pEntry->data;
  pHead->vgId = vgId;
  pHead->contLen = sizeof(SMsgHead);

  return pEntry;
}

void syncEntryDestroy(SyncRaftEntry* pEntry) {
  if (pEntry != NULL) {
    sTrace("free entry:%p", pEntry);
    taosMemoryFree(pEntry);
  }
}

void syncEntry2OriginalRpc(const SyncRaftEntry* pEntry, SRpcMsg* pRpcMsg) {
  pRpcMsg->msgType = pEntry->originalRpcType;
  pRpcMsg->contLen = (int32_t)(pEntry->dataLen);
  pRpcMsg->pCont = rpcMallocCont(pRpcMsg->contLen);
  memcpy(pRpcMsg->pCont, pEntry->data, pRpcMsg->contLen);
}
