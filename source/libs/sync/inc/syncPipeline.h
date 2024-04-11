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
} SyncLogReplMgr;

typedef struct SSyncLogBufEntry {
  SyncRaftEntry*  pItem;
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
  int64_t          totalIndex;
  bool             isCatchup;
} SyncLogBuffer;

// SSyncLogRepMgr
SyncLogReplMgr* syncLogReplCreate();
void            syncLogReplDestroy(SyncLogReplMgr* pMgr);
void            syncLogReplReset(SyncLogReplMgr* pMgr);

int32_t syncNodeLogReplInit(SyncNode* pNode);
void    syncNodeLogReplDestroy(SyncNode* pNode);

// access
static FORCE_INLINE int64_t syncLogReplGetRetryBackoffTimeMs(SyncLogReplMgr* pMgr) {
  return ((int64_t)1 << pMgr->retryBackoff) * SYNC_LOG_REPL_RETRY_WAIT_MS;
}

static FORCE_INLINE int32_t syncLogReplGetNextRetryBackoff(SyncLogReplMgr* pMgr) {
  return TMIN(pMgr->retryBackoff + 1, SYNC_MAX_RETRY_BACKOFF);
}

SyncTerm syncLogReplGetPrevLogTerm(SyncLogReplMgr* pMgr, SyncNode* pNode, SyncIndex index);

int32_t syncLogReplStart(SyncLogReplMgr* pMgr, SyncNode* pNode);
int32_t syncLogReplAttempt(SyncLogReplMgr* pMgr, SyncNode* pNode);
int32_t syncLogReplProbe(SyncLogReplMgr* pMgr, SyncNode* pNode, SyncIndex index);
int32_t syncLogReplRetryOnNeed(SyncLogReplMgr* pMgr, SyncNode* pNode);
int32_t syncLogReplSendTo(SyncLogReplMgr* pMgr, SyncNode* pNode, SyncIndex index, SyncTerm* pTerm, SRaftId* pDestId,
                          bool* pBarrier);

int32_t syncLogReplProcessReply(SyncLogReplMgr* pMgr, SyncNode* pNode, SyncAppendEntriesReply* pMsg);
int32_t syncLogReplRecover(SyncLogReplMgr* pMgr, SyncNode* pNode, SyncAppendEntriesReply* pMsg);
int32_t syncLogReplContinue(SyncLogReplMgr* pMgr, SyncNode* pNode, SyncAppendEntriesReply* pMsg);

int32_t syncLogReplProcessHeartbeatReply(SyncLogReplMgr* pMgr, SyncNode* pNode, SyncHeartbeatReply* pMsg);

// SyncLogBuffer
SyncLogBuffer* syncLogBufferCreate();
void           syncLogBufferDestroy(SyncLogBuffer* pBuf);
int32_t        syncLogBufferInit(SyncLogBuffer* pBuf, SyncNode* pNode);
int32_t        syncLogBufferReInit(SyncLogBuffer* pBuf, SyncNode* pNode);

// access
int64_t  syncLogBufferGetEndIndex(SyncLogBuffer* pBuf);
SyncTerm syncLogBufferGetLastMatchTerm(SyncLogBuffer* pBuf);
bool     syncLogBufferIsEmpty(SyncLogBuffer* pBuf);

int32_t syncLogBufferAppend(SyncLogBuffer* pBuf, SyncNode* pNode, SyncRaftEntry* pEntry);
int32_t syncLogBufferAccept(SyncLogBuffer* pBuf, SyncNode* pNode, SyncRaftEntry* pEntry, SyncTerm prevTerm);
int64_t syncLogBufferProceed(SyncLogBuffer* pBuf, SyncNode* pNode, SyncTerm* pMatchTerm, char* str);
int32_t syncLogBufferCommit(SyncLogBuffer* pBuf, SyncNode* pNode, int64_t commitIndex);
int32_t syncLogBufferReset(SyncLogBuffer* pBuf, SyncNode* pNode);

// private
SyncRaftEntry* syncLogBufferGetOneEntry(SyncLogBuffer* pBuf, SyncNode* pNode, SyncIndex index, bool* pInBuf);
int32_t        syncLogBufferValidate(SyncLogBuffer* pBuf);
int32_t        syncLogBufferRollback(SyncLogBuffer* pBuf, SyncNode* pNode, SyncIndex toIndex);

int32_t syncFsmExecute(SyncNode* pNode, SSyncFSM* pFsm, ESyncState role, SyncTerm term, SyncRaftEntry* pEntry,
                       int32_t applyCode);
#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_PIPELINE_H*/
