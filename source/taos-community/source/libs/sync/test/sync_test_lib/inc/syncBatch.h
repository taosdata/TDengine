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

#ifndef _TD_LIBS_SYNC_BATCH_H
#define _TD_LIBS_SYNC_BATCH_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

// ---------------------------------------------
typedef struct SRaftMeta {
  uint64_t seqNum;
  bool     isWeak;
} SRaftMeta;

// block1:
// block2: SRaftMeta array
// block3: rpc msg array (with pCont pointer)

typedef struct SyncClientRequestBatch {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;  // TDMT_SYNC_CLIENT_REQUEST_BATCH
  uint32_t dataCount;
  uint32_t dataLen;
  char     data[];  // block2, block3
} SyncClientRequestBatch;

SyncClientRequestBatch* syncClientRequestBatchBuild(SRpcMsg** rpcMsgPArr, SRaftMeta* raftArr, int32_t arrSize,
                                                    int32_t vgId);
void                    syncClientRequestBatch2RpcMsg(const SyncClientRequestBatch* pSyncMsg, SRpcMsg* pRpcMsg);
void                    syncClientRequestBatchDestroy(SyncClientRequestBatch* pMsg);
void                    syncClientRequestBatchDestroyDeep(SyncClientRequestBatch* pMsg);
SRaftMeta*              syncClientRequestBatchMetaArr(const SyncClientRequestBatch* pSyncMsg);
SRpcMsg*                syncClientRequestBatchRpcMsgArr(const SyncClientRequestBatch* pSyncMsg);
SyncClientRequestBatch* syncClientRequestBatchFromRpcMsg(const SRpcMsg* pRpcMsg);
cJSON*                  syncClientRequestBatch2Json(const SyncClientRequestBatch* pMsg);
char*                   syncClientRequestBatch2Str(const SyncClientRequestBatch* pMsg);

// for debug ----------------------
void syncClientRequestBatchPrint(const SyncClientRequestBatch* pMsg);
void syncClientRequestBatchPrint2(char* s, const SyncClientRequestBatch* pMsg);
void syncClientRequestBatchLog(const SyncClientRequestBatch* pMsg);
void syncClientRequestBatchLog2(char* s, const SyncClientRequestBatch* pMsg);

typedef struct SOffsetAndContLen {
  int32_t offset;
  int32_t contLen;
} SOffsetAndContLen;

// data:
// block1: SOffsetAndContLen Array
// block2: entry Array

typedef struct SyncAppendEntriesBatch {
  uint32_t bytes;
  int32_t  vgId;
  uint32_t msgType;
  SRaftId  srcId;
  SRaftId  destId;

  // private data
  SyncTerm  term;
  SyncIndex prevLogIndex;
  SyncTerm  prevLogTerm;
  SyncIndex commitIndex;
  SyncTerm  privateTerm;
  int32_t   dataCount;
  uint32_t  dataLen;
  char      data[];  // block1, block2
} SyncAppendEntriesBatch;

SyncAppendEntriesBatch* syncAppendEntriesBatchBuild(SSyncRaftEntry** entryPArr, int32_t arrSize, int32_t vgId);
SOffsetAndContLen*      syncAppendEntriesBatchMetaTableArray(SyncAppendEntriesBatch* pMsg);
void                    syncAppendEntriesBatchDestroy(SyncAppendEntriesBatch* pMsg);
void                    syncAppendEntriesBatchSerialize(const SyncAppendEntriesBatch* pMsg, char* buf, uint32_t bufLen);
void                    syncAppendEntriesBatchDeserialize(const char* buf, uint32_t len, SyncAppendEntriesBatch* pMsg);
char*                   syncAppendEntriesBatchSerialize2(const SyncAppendEntriesBatch* pMsg, uint32_t* len);
SyncAppendEntriesBatch* syncAppendEntriesBatchDeserialize2(const char* buf, uint32_t len);
void                    syncAppendEntriesBatch2RpcMsg(const SyncAppendEntriesBatch* pMsg, SRpcMsg* pRpcMsg);
void                    syncAppendEntriesBatchFromRpcMsg(const SRpcMsg* pRpcMsg, SyncAppendEntriesBatch* pMsg);
SyncAppendEntriesBatch* syncAppendEntriesBatchFromRpcMsg2(const SRpcMsg* pRpcMsg);

// ---------------------------------------------
void syncLogSendAppendEntriesBatch(SSyncNode* pSyncNode, const SyncAppendEntriesBatch* pMsg, const char* s);
void syncLogRecvAppendEntriesBatch(SSyncNode* pSyncNode, const SyncAppendEntriesBatch* pMsg, const char* s);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_INT_H*/
