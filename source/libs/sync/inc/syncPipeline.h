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

#ifndef _TD_LIBS_SYNC_PIPELINE_H
#define _TD_LIBS_SYNC_PIPELINE_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

typedef struct SSyncReplInfo {
  bool    barrier;
  bool    acked;
  int64_t timeMs;
  int64_t term;
} SSyncReplInfo;

typedef struct SSyncLogReplMgr {
  SSyncReplInfo states[TSDB_SYNC_LOG_BUFFER_SIZE];
  int64_t       startIndex;
  int64_t       matchIndex;
  int64_t       endIndex;
  int64_t       size;
  bool          restored;
  int64_t       peerStartTime;
  int32_t       retryBackoff;
  int32_t       peerId;
} SSyncLogReplMgr;

typedef struct SSyncLogBufEntry {
  SSyncRaftEntry* pItem;
  SyncIndex       prevLogIndex;
  SyncTerm        prevLogTerm;
} SSyncLogBufEntry;

typedef struct SSyncLogBuffer {
  SSyncLogBufEntry entries[TSDB_SYNC_LOG_BUFFER_SIZE];
  int64_t          startIndex;
  int64_t          commitIndex;
  int64_t          matchIndex;
  int64_t          endIndex;
  int64_t          size;
  TdThreadMutex    mutex;
  TdThreadMutexAttr attr;
} SSyncLogBuffer;

// SSyncLogRepMgr
SSyncLogReplMgr* syncLogReplMgrCreate();
void             syncLogReplMgrDestroy(SSyncLogReplMgr* pMgr);
void             syncLogReplMgrReset(SSyncLogReplMgr* pMgr);

int32_t          syncNodeLogReplMgrInit(SSyncNode* pNode);
void             syncNodeLogReplMgrDestroy(SSyncNode* pNode);

// access
static FORCE_INLINE int64_t syncLogGetRetryBackoffTimeMs(SSyncLogReplMgr* pMgr) {
  return (1 << pMgr->retryBackoff) * SYNC_LOG_REPL_RETRY_WAIT_MS;
}

static FORCE_INLINE int32_t syncLogGetNextRetryBackoff(SSyncLogReplMgr* pMgr) {
  return TMIN(pMgr->retryBackoff + 1, SYNC_MAX_RETRY_BACKOFF);
}

SyncTerm syncLogReplMgrGetPrevLogTerm(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index);

int32_t  syncLogReplMgrReplicateOnce(SSyncLogReplMgr* pMgr, SSyncNode* pNode);
int32_t  syncLogReplMgrReplicateOneTo(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index, SyncTerm* pTerm,
                                      SRaftId* pDestId, bool* pBarrier);
int32_t  syncLogReplMgrReplicateAttempt(SSyncLogReplMgr* pMgr, SSyncNode* pNode);
int32_t  syncLogReplMgrReplicateProbe(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index);

int32_t  syncLogReplMgrProcessReply(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg);
int32_t  syncLogReplMgrProcessReplyAsRecovery(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg);
int32_t  syncLogReplMgrProcessReplyAsNormal(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg);

int32_t syncLogReplMgrProcessHeartbeatReply(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncHeartbeatReply* pMsg);
int32_t  syncLogReplMgrRetryOnNeed(SSyncLogReplMgr* pMgr, SSyncNode* pNode);

// SSyncLogBuffer
SSyncLogBuffer* syncLogBufferCreate();
void            syncLogBufferDestroy(SSyncLogBuffer* pBuf);
int32_t         syncLogBufferInit(SSyncLogBuffer* pBuf, SSyncNode* pNode);
int32_t         syncLogBufferReInit(SSyncLogBuffer* pBuf, SSyncNode* pNode);

// access
int64_t syncLogBufferGetEndIndex(SSyncLogBuffer* pBuf);
SyncTerm syncLogBufferGetLastMatchTerm(SSyncLogBuffer* pBuf);
int32_t syncLogBufferAppend(SSyncLogBuffer* pBuf, SSyncNode* pNode, SSyncRaftEntry* pEntry);
int32_t syncLogBufferAccept(SSyncLogBuffer* pBuf, SSyncNode* pNode, SSyncRaftEntry* pEntry, SyncTerm prevTerm);
int64_t syncLogBufferProceed(SSyncLogBuffer* pBuf, SSyncNode* pNode, SyncTerm* pMatchTerm);
int32_t syncLogBufferCommit(SSyncLogBuffer* pBuf, SSyncNode* pNode, int64_t commitIndex);
int32_t syncLogBufferReset(SSyncLogBuffer* pBuf, SSyncNode* pNode);

// private
SSyncRaftEntry* syncLogBufferGetOneEntry(SSyncLogBuffer* pBuf, SSyncNode* pNode, SyncIndex index, bool* pInBuf);
int32_t         syncLogBufferValidate(SSyncLogBuffer* pBuf);
int32_t         syncLogBufferRollback(SSyncLogBuffer* pBuf, SSyncNode* pNode, SyncIndex toIndex);

int32_t syncLogFsmExecute(SSyncNode* pNode, SSyncFSM* pFsm, ESyncState role, SyncTerm term, SSyncRaftEntry* pEntry,
                          int32_t applyCode);
#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_PIPELINE_H*/
