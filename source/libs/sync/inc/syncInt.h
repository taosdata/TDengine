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

#ifndef _TD_LIBS_SYNC_INT_H
#define _TD_LIBS_SYNC_INT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "sync.h"
#include "taosdef.h"
#include "trpc.h"
#include "ttimer.h"

typedef struct SyncTimeout            SyncTimeout;
typedef struct SyncClientRequest      SyncClientRequest;
typedef struct SyncRequestVote        SyncRequestVote;
typedef struct SyncRequestVoteReply   SyncRequestVoteReply;
typedef struct SyncAppendEntries      SyncAppendEntries;
typedef struct SyncAppendEntriesReply SyncAppendEntriesReply;
typedef struct SSyncEnv               SSyncEnv;
typedef struct SRaftStore             SRaftStore;
typedef struct SVotesGranted          SVotesGranted;
typedef struct SVotesRespond          SVotesRespond;
typedef struct SSyncIndexMgr          SSyncIndexMgr;
typedef struct SRaftCfg               SRaftCfg;
typedef struct SSyncRespMgr           SSyncRespMgr;
typedef struct SSyncSnapshotSender    SSyncSnapshotSender;
typedef struct SSyncSnapshotReceiver  SSyncSnapshotReceiver;
typedef struct SSyncTimer             SSyncTimer;
typedef struct SSyncHbTimerData       SSyncHbTimerData;
typedef struct SyncSnapshotSend       SyncSnapshotSend;
typedef struct SyncSnapshotRsp        SyncSnapshotRsp;
typedef struct SyncLocalCmd           SyncLocalCmd;
typedef struct SyncAppendEntriesBatch SyncAppendEntriesBatch;
typedef struct SyncPreSnapshotReply   SyncPreSnapshotReply;
typedef struct SyncHeartbeatReply     SyncHeartbeatReply;
typedef struct SyncHeartbeat          SyncHeartbeat;
typedef struct SyncPreSnapshot        SyncPreSnapshot;

typedef struct SRaftId {
  SyncNodeId  addr;
  SyncGroupId vgId;
} SRaftId;

typedef struct SSyncHbTimerData {
  SSyncNode*  pSyncNode;
  SSyncTimer* pTimer;
  SRaftId     destId;
  uint64_t    logicClock;
} SSyncHbTimerData;

typedef struct SSyncTimer {
  void*             pTimer;
  TAOS_TMR_CALLBACK timerCb;
  uint64_t          logicClock;
  uint64_t          counter;
  int32_t           timerMS;
  SRaftId           destId;
  SSyncHbTimerData  hbData;
} SSyncTimer;

typedef struct SElectTimerParam {
  uint64_t   logicClock;
  SSyncNode* pSyncNode;
  int64_t    executeTime;
  void*      pData;
} SElectTimerParam;

typedef struct SPeerState {
  SyncIndex lastSendIndex;
  int64_t   lastSendTime;
} SPeerState;

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

SSyncLogReplMgr* syncLogReplMgrCreate();
void             syncLogReplMgrDestroy(SSyncLogReplMgr* pMgr);

// access
static FORCE_INLINE int64_t syncLogGetRetryBackoffTimeMs(SSyncLogReplMgr* pMgr) {
  return (1 << pMgr->retryBackoff) * SYNC_LOG_REPL_RETRY_WAIT_MS;
}

static FORCE_INLINE int32_t syncLogGetNextRetryBackoff(SSyncLogReplMgr* pMgr) {
  return TMIN(pMgr->retryBackoff + 1, SYNC_MAX_RETRY_BACKOFF);
}

static FORCE_INLINE int32_t syncLogReplMgrUpdateTerm(SSyncLogReplMgr* pMgr, SyncIndex index, SyncTerm term) {
  if (pMgr->endIndex == 0) return -1;
  ASSERT(pMgr->startIndex <= index && index < pMgr->endIndex);
  pMgr->states[(index + pMgr->size) % pMgr->size].term = term;
  return 0;
}

SyncTerm syncLogReplMgrGetPrevLogTerm(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index);
int32_t  syncLogBufferReplicateOneTo(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncIndex index, SyncTerm* pTerm,
                                     SRaftId* pDestId, bool* pBarrier);
int32_t  syncLogReplMgrProcessReply(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg);
int32_t  syncLogBufferReplicateOnce(SSyncLogReplMgr* pMgr, SSyncNode* pNode);
int32_t  syncLogReplMgrReplicateAttemptedOnce(SSyncLogReplMgr* pMgr, SSyncNode* pNode);
int32_t  syncLogReplMgrReplicateProbeOnce(SSyncLogReplMgr* pMgr, SSyncNode* pNode);
int32_t  syncLogResetLogReplMgr(SSyncLogReplMgr* pMgr);
int32_t syncLogReplMgrProcessReplyInRecoveryMode(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg);
int32_t syncLogReplMgrProcessReplyInNormalMode(SSyncLogReplMgr* pMgr, SSyncNode* pNode, SyncAppendEntriesReply* pMsg);
int32_t syncLogReplMgrRetryOnNeed(SSyncLogReplMgr* pMgr, SSyncNode* pNode);

int32_t syncNodeSendAppendEntries(SSyncNode* pNode, const SRaftId* destRaftId, SRpcMsg* pRpcMsg);
int32_t syncNodeMaybeSendAppendEntries(SSyncNode* pNode, const SRaftId* destRaftId, SRpcMsg* pRpcMsg);
void    syncLogDestroyAppendEntries(SRpcMsg* pRpcMsg);

// others
bool syncLogReplMgrValidate(SSyncLogReplMgr* pMgr);

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
} SSyncLogBuffer;

SSyncLogBuffer* syncLogBufferCreate();
void            syncLogBufferDestroy(SSyncLogBuffer* pBuf);
int32_t         syncLogBufferInit(SSyncLogBuffer* pBuf, SSyncNode* pNode);

// access
int64_t syncLogBufferGetEndIndex(SSyncLogBuffer* pBuf);
int32_t syncLogBufferAppend(SSyncLogBuffer* pBuf, SSyncNode* pNode, SSyncRaftEntry* pEntry);
int32_t syncLogBufferAccept(SSyncLogBuffer* pBuf, SSyncNode* pNode, SSyncRaftEntry* pEntry, SyncTerm prevTerm);
int64_t syncLogBufferProceed(SSyncLogBuffer* pBuf, SSyncNode* pNode);
int32_t syncLogBufferCommit(SSyncLogBuffer* pBuf, SSyncNode* pNode, int64_t commitIndex);

int64_t            syncNodeUpdateCommitIndex(SSyncNode* ths, SyncIndex commtIndex);
int32_t syncLogToAppendEntries(SSyncNode* pNode, SSyncRaftEntry* pEntry, SyncTerm prevLogTerm, SRpcMsg* pRpcMsg);

// private
SSyncRaftEntry* syncLogBufferGetOneEntry(SSyncLogBuffer* pBuf, SSyncNode* pNode, SyncIndex index, bool* pInBuf);
int32_t syncLogBufferValidate(SSyncLogBuffer* pBuf);
int32_t syncLogBufferRollback(SSyncLogBuffer* pBuf, SyncIndex toIndex);
int32_t syncLogBufferReplicate(SSyncLogBuffer* pBuf, SSyncNode* pNode, SSyncRaftEntry* pEntry, SyncTerm prevLogTerm);
bool syncNodeAgreedUpon(SSyncNode* pNode, SyncIndex index);

void syncIndexMgrSetIndex(SSyncIndexMgr* pSyncIndexMgr, const SRaftId* pRaftId, SyncIndex index);

typedef struct SSyncNode {
  // init by SSyncInfo
  SyncGroupId vgId;
  SRaftCfg*   pRaftCfg;
  char        path[TSDB_FILENAME_LEN];
  char        raftStorePath[TSDB_FILENAME_LEN * 2];
  char        configPath[TSDB_FILENAME_LEN * 2];

  // sync io
  SSyncLogBuffer* pLogBuf;
  SWal*         pWal;
  const SMsgCb* msgcb;
  int32_t (*syncSendMSg)(const SEpSet* pEpSet, SRpcMsg* pMsg);
  int32_t (*syncEqMsg)(const SMsgCb* msgcb, SRpcMsg* pMsg);
  int32_t (*syncEqCtrlMsg)(const SMsgCb* msgcb, SRpcMsg* pMsg);

  // init internal
  SNodeInfo myNodeInfo;
  SRaftId   myRaftId;

  int32_t   peersNum;
  SNodeInfo peersNodeInfo[TSDB_MAX_REPLICA];
  SRaftId   peersId[TSDB_MAX_REPLICA];

  int32_t replicaNum;
  SRaftId replicasId[TSDB_MAX_REPLICA];

  // raft algorithm
  SSyncFSM* pFsm;
  int32_t   quorum;
  SRaftId   leaderCache;

  // life cycle
  int64_t rid;

  // tla+ server vars
  ESyncState  state;
  SRaftStore* pRaftStore;

  // tla+ candidate vars
  SVotesGranted* pVotesGranted;
  SVotesRespond* pVotesRespond;

  // tla+ leader vars
  SSyncIndexMgr* pNextIndex;
  SSyncIndexMgr* pMatchIndex;

  // tla+ log vars
  SSyncLogStore* pLogStore;
  SyncIndex      commitIndex;

  // timer ms init
  int32_t pingBaseLine;
  int32_t electBaseLine;
  int32_t hbBaseLine;

  // ping timer
  tmr_h             pPingTimer;
  int32_t           pingTimerMS;
  uint64_t          pingTimerLogicClock;
  uint64_t          pingTimerLogicClockUser;
  TAOS_TMR_CALLBACK FpPingTimerCB;  // Timer Fp
  uint64_t          pingTimerCounter;

  // elect timer
  tmr_h             pElectTimer;
  int32_t           electTimerMS;
  uint64_t          electTimerLogicClock;
  TAOS_TMR_CALLBACK FpElectTimerCB;  // Timer Fp
  uint64_t          electTimerCounter;
  SElectTimerParam  electTimerParam;

  // heartbeat timer
  tmr_h             pHeartbeatTimer;
  int32_t           heartbeatTimerMS;
  uint64_t          heartbeatTimerLogicClock;
  uint64_t          heartbeatTimerLogicClockUser;
  TAOS_TMR_CALLBACK FpHeartbeatTimerCB;  // Timer Fp
  uint64_t          heartbeatTimerCounter;

  // peer heartbeat timer
  SSyncTimer peerHeartbeatTimerArr[TSDB_MAX_REPLICA];

  // tools
  SSyncRespMgr* pSyncRespMgr;

  // restore state
  bool restoreFinish;
  // SSnapshot*             pSnapshot;
  SSyncSnapshotSender*   senders[TSDB_MAX_REPLICA];
  SSyncSnapshotReceiver* pNewNodeReceiver;

  // log replication mgr
  SSyncLogReplMgr* logReplMgrs[TSDB_MAX_REPLICA];

  SPeerState peerStates[TSDB_MAX_REPLICA];

  // is config changing
  bool changing;

  int64_t snapshottingIndex;
  int64_t snapshottingTime;
  int64_t minMatchIndex;

  int64_t startTime;
  int64_t leaderTime;
  int64_t lastReplicateTime;

} SSyncNode;

// open/close --------------
SSyncNode* syncNodeOpen(SSyncInfo* pSyncInfo);
int32_t    syncNodeStart(SSyncNode* pSyncNode);
int32_t    syncNodeStartStandBy(SSyncNode* pSyncNode);
void       syncNodeClose(SSyncNode* pSyncNode);
void       syncNodePreClose(SSyncNode* pSyncNode);
int32_t    syncNodePropose(SSyncNode* pSyncNode, SRpcMsg* pMsg, bool isWeak);
int32_t    syncNodeRestore(SSyncNode* pSyncNode);

// on message ---------------------
int32_t syncNodeOnTimeout(SSyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnClientRequest(SSyncNode* ths, SRpcMsg* pMsg, SyncIndex* pRetIndex);
int32_t syncNodeOnRequestVote(SSyncNode* pNode, const SRpcMsg* pMsg);
int32_t syncNodeOnRequestVoteReply(SSyncNode* pNode, const SRpcMsg* pMsg);
int32_t syncNodeOnAppendEntries(SSyncNode* pNode, const SRpcMsg* pMsg);
int32_t syncNodeOnAppendEntriesReply(SSyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnSnapshot(SSyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnSnapshotReply(SSyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnHeartbeat(SSyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnHeartbeatReply(SSyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnLocalCmd(SSyncNode* ths, const SRpcMsg* pMsg);

// timer control --------------
int32_t syncNodeStartPingTimer(SSyncNode* pSyncNode);
int32_t syncNodeStopPingTimer(SSyncNode* pSyncNode);
int32_t syncNodeStartElectTimer(SSyncNode* pSyncNode, int32_t ms);
int32_t syncNodeStopElectTimer(SSyncNode* pSyncNode);
int32_t syncNodeRestartElectTimer(SSyncNode* pSyncNode, int32_t ms);
int32_t syncNodeResetElectTimer(SSyncNode* pSyncNode);
int32_t syncNodeStartHeartbeatTimer(SSyncNode* pSyncNode);
int32_t syncNodeStopHeartbeatTimer(SSyncNode* pSyncNode);
int32_t syncNodeRestartHeartbeatTimer(SSyncNode* pSyncNode);

// utils --------------
int32_t   syncNodeSendMsgById(const SRaftId* destRaftId, SSyncNode* pSyncNode, SRpcMsg* pMsg);
int32_t   syncNodeSendMsgByInfo(const SNodeInfo* nodeInfo, SSyncNode* pSyncNode, SRpcMsg* pMsg);
SyncIndex syncMinMatchIndex(SSyncNode* pSyncNode);
int32_t   syncCacheEntry(SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry, LRUHandle** h);

// raft state change --------------
void syncNodeUpdateTerm(SSyncNode* pSyncNode, SyncTerm term);
void syncNodeUpdateTermWithoutStepDown(SSyncNode* pSyncNode, SyncTerm term);
void syncNodeStepDown(SSyncNode* pSyncNode, SyncTerm newTerm);
void syncNodeBecomeFollower(SSyncNode* pSyncNode, const char* debugStr);
void syncNodeBecomeLeader(SSyncNode* pSyncNode, const char* debugStr);
void syncNodeCandidate2Leader(SSyncNode* pSyncNode);
void syncNodeFollower2Candidate(SSyncNode* pSyncNode);
void syncNodeLeader2Follower(SSyncNode* pSyncNode);
void syncNodeCandidate2Follower(SSyncNode* pSyncNode);

// raft vote --------------
void syncNodeVoteForTerm(SSyncNode* pSyncNode, SyncTerm term, SRaftId* pRaftId);
void syncNodeVoteForSelf(SSyncNode* pSyncNode);

// log replication
SSyncLogReplMgr* syncNodeGetLogReplMgr(SSyncNode* pNode, SRaftId* pDestId);

// snapshot --------------
bool    syncNodeHasSnapshot(SSyncNode* pSyncNode);
void    syncNodeMaybeUpdateCommitBySnapshot(SSyncNode* pSyncNode);
int32_t syncNodeStartSnapshot(SSyncNode* pSyncNode, SRaftId* pDestId);

SyncIndex syncNodeGetLastIndex(const SSyncNode* pSyncNode);
SyncTerm  syncNodeGetLastTerm(SSyncNode* pSyncNode);
int32_t   syncNodeGetLastIndexTerm(SSyncNode* pSyncNode, SyncIndex* pLastIndex, SyncTerm* pLastTerm);
SyncIndex syncNodeSyncStartIndex(SSyncNode* pSyncNode);
SyncIndex syncNodeGetPreIndex(SSyncNode* pSyncNode, SyncIndex index);
SyncTerm  syncNodeGetPreTerm(SSyncNode* pSyncNode, SyncIndex index);
int32_t   syncNodeGetPreIndexTerm(SSyncNode* pSyncNode, SyncIndex index, SyncIndex* pPreIndex, SyncTerm* pPreTerm);

int32_t syncNodeDoCommit(SSyncNode* ths, SyncIndex beginIndex, SyncIndex endIndex, uint64_t flag);
int32_t syncNodeFollowerCommit(SSyncNode* ths, SyncIndex newCommitIndex);
int32_t syncNodePreCommit(SSyncNode* ths, SSyncRaftEntry* pEntry, int32_t code);

bool                 syncNodeInRaftGroup(SSyncNode* ths, SRaftId* pRaftId);
SSyncSnapshotSender* syncNodeGetSnapshotSender(SSyncNode* ths, SRaftId* pDestId);
SSyncTimer*          syncNodeGetHbTimer(SSyncNode* ths, SRaftId* pDestId);
SPeerState*          syncNodeGetPeerState(SSyncNode* ths, const SRaftId* pDestId);
bool syncNodeNeedSendAppendEntries(SSyncNode* ths, const SRaftId* pDestId, const SyncAppendEntries* pMsg);

int32_t syncGetSnapshotMeta(int64_t rid, struct SSnapshotMeta* sMeta);
int32_t syncGetSnapshotMetaByIndex(int64_t rid, SyncIndex snapshotIndex, struct SSnapshotMeta* sMeta);

int32_t syncNodeDynamicQuorum(const SSyncNode* pSyncNode);
bool    syncNodeIsMnode(SSyncNode* pSyncNode);
int32_t syncNodePeerStateInit(SSyncNode* pSyncNode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_INT_H*/
