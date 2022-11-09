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
#include "syncTools.h"
#include "taosdef.h"
#include "tlog.h"
#include "trpc.h"
#include "ttimer.h"

// clang-format off

#define sFatal(...) if (sDebugFlag & DEBUG_FATAL) { taosPrintLog("SYN FATAL ", DEBUG_FATAL, 255,        __VA_ARGS__); }
#define sError(...) if (sDebugFlag & DEBUG_ERROR) { taosPrintLog("SYN ERROR ", DEBUG_ERROR, 255,        __VA_ARGS__); }
#define sWarn(...)  if (sDebugFlag & DEBUG_WARN)  { taosPrintLog("SYN WARN ",  DEBUG_WARN,  255,        __VA_ARGS__); }
#define sInfo(...)  if (sDebugFlag & DEBUG_INFO)  { taosPrintLog("SYN ",       DEBUG_INFO,  255,        __VA_ARGS__); }
#define sDebug(...) if (sDebugFlag & DEBUG_DEBUG) { taosPrintLog("SYN ",       DEBUG_DEBUG, sDebugFlag, __VA_ARGS__); }
#define sTrace(...) if (sDebugFlag & DEBUG_TRACE) { taosPrintLog("SYN ",       DEBUG_TRACE, sDebugFlag, __VA_ARGS__); }

#define sLFatal(...) if (sDebugFlag & DEBUG_FATAL) { taosPrintLongString("SYN FATAL ", DEBUG_FATAL, 255,        __VA_ARGS__); }
#define sLError(...) if (sDebugFlag & DEBUG_ERROR) { taosPrintLongString("SYN ERROR ", DEBUG_ERROR, 255,        __VA_ARGS__); }
#define sLWarn(...)  if (sDebugFlag & DEBUG_WARN)  { taosPrintLongString("SYN WARN ",  DEBUG_WARN,  255,        __VA_ARGS__); }
#define sLInfo(...)  if (sDebugFlag & DEBUG_INFO)  { taosPrintLongString("SYN ",       DEBUG_INFO,  255,        __VA_ARGS__); }
#define sLDebug(...) if (sDebugFlag & DEBUG_DEBUG) { taosPrintLongString("SYN ",       DEBUG_DEBUG, sDebugFlag, __VA_ARGS__); }
#define sLTrace(...) if (sDebugFlag & DEBUG_TRACE) { taosPrintLongString("SYN ",       DEBUG_TRACE, sDebugFlag, __VA_ARGS__); }

#define sNFatal(pNode, ...)  if (sDebugFlag & DEBUG_FATAL) { syncPrintNodeLog("SYN FATAL ", DEBUG_FATAL, 255,        pNode, __VA_ARGS__); }
#define sNError(pNode, ...)  if (sDebugFlag & DEBUG_ERROR) { syncPrintNodeLog("SYN ERROR ", DEBUG_ERROR, 255,        pNode, __VA_ARGS__); }
#define sNWarn(pNode, ...)   if (sDebugFlag & DEBUG_WARN)  { syncPrintNodeLog("SYN WARN ",  DEBUG_WARN,  255,        pNode, __VA_ARGS__); }
#define sNInfo(pNode, ...)   if (sDebugFlag & DEBUG_INFO)  { syncPrintNodeLog("SYN ",       DEBUG_INFO,  255,        pNode, __VA_ARGS__); }
#define sNDebug(pNode, ...)  if (sDebugFlag & DEBUG_DEBUG) { syncPrintNodeLog("SYN ",       DEBUG_DEBUG, sDebugFlag, pNode, __VA_ARGS__); }
#define sNTrace(pNode, ...)  if (sDebugFlag & DEBUG_TRACE) { syncPrintNodeLog("SYN ",       DEBUG_TRACE, sDebugFlag, pNode, __VA_ARGS__); }

#define sSFatal(pSender, ...)  if (sDebugFlag & DEBUG_FATAL) { syncPrintSnapshotSenderLog("SYN FATAL ", DEBUG_FATAL, 255,        pSender, __VA_ARGS__); }
#define sSError(pSender, ...)  if (sDebugFlag & DEBUG_ERROR) { syncPrintSnapshotSenderLog("SYN ERROR ", DEBUG_ERROR, 255,        pSender, __VA_ARGS__); }
#define sSWarn(pSender, ...)   if (sDebugFlag & DEBUG_WARN)  { syncPrintSnapshotSenderLog("SYN WARN ",  DEBUG_WARN,  255,        pSender, __VA_ARGS__); }
#define sSInfo(pSender, ...)   if (sDebugFlag & DEBUG_INFO)  { syncPrintSnapshotSenderLog("SYN ",       DEBUG_INFO,  255,        pSender, __VA_ARGS__); }
#define sSDebug(pSender, ...)  if (sDebugFlag & DEBUG_DEBUG) { syncPrintSnapshotSenderLog("SYN ",       DEBUG_DEBUG, sDebugFlag, pSender, __VA_ARGS__); }
#define sSTrace(pSender, ...)  if (sDebugFlag & DEBUG_TRACE) { syncPrintSnapshotSenderLog("SYN ",       DEBUG_TRACE, sDebugFlag, pSender, __VA_ARGS__); }

#define sRFatal(pReceiver, ...)  if (sDebugFlag & DEBUG_FATAL) { syncPrintSnapshotReceiverLog("SYN FATAL ", DEBUG_FATAL, 255,        pReceiver, __VA_ARGS__); }
#define sRError(pReceiver, ...)  if (sDebugFlag & DEBUG_ERROR) { syncPrintSnapshotReceiverLog("SYN ERROR ", DEBUG_ERROR, 255,        pReceiver, __VA_ARGS__); }
#define sRWarn(pReceiver, ...)   if (sDebugFlag & DEBUG_WARN)  { syncPrintSnapshotReceiverLog("SYN WARN ",  DEBUG_WARN,  255,        pReceiver, __VA_ARGS__); }
#define sRInfo(pReceiver, ...)   if (sDebugFlag & DEBUG_INFO)  { syncPrintSnapshotReceiverLog("SYN ",       DEBUG_INFO,  255,        pReceiver, __VA_ARGS__); }
#define sRDebug(pReceiver, ...)  if (sDebugFlag & DEBUG_DEBUG) { syncPrintSnapshotReceiverLog("SYN ",       DEBUG_DEBUG, sDebugFlag, pReceiver, __VA_ARGS__); }
#define sRTrace(pReceiver, ...)  if (sDebugFlag & DEBUG_TRACE) { syncPrintSnapshotReceiverLog("SYN ",       DEBUG_TRACE, sDebugFlag, pReceiver, __VA_ARGS__); }

// clang-format on

typedef struct SyncTimeout            SyncTimeout;
typedef struct SyncClientRequest      SyncClientRequest;
typedef struct SyncPing               SyncPing;
typedef struct SyncPingReply          SyncPingReply;
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

extern bool gRaftDetailLog;

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
  void*             pData;
} SSyncTimer;

typedef struct SElectTimer {
  uint64_t   logicClock;
  SSyncNode* pSyncNode;
  void*      pData;
} SElectTimer;

typedef struct SPeerState {
  SyncIndex lastSendIndex;
  int64_t   lastSendTime;
} SPeerState;

typedef struct SSyncNode {
  // init by SSyncInfo
  SyncGroupId vgId;
  SRaftCfg*   pRaftCfg;
  char        path[TSDB_FILENAME_LEN];
  char        raftStorePath[TSDB_FILENAME_LEN * 2];
  char        configPath[TSDB_FILENAME_LEN * 2];

  // sync io
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

  // heartbeat timer
  tmr_h             pHeartbeatTimer;
  int32_t           heartbeatTimerMS;
  uint64_t          heartbeatTimerLogicClock;
  uint64_t          heartbeatTimerLogicClockUser;
  TAOS_TMR_CALLBACK FpHeartbeatTimerCB;  // Timer Fp
  uint64_t          heartbeatTimerCounter;

  // peer heartbeat timer
  SSyncTimer peerHeartbeatTimerArr[TSDB_MAX_REPLICA];

  // callback
  FpOnPingCb               FpOnPing;
  FpOnPingReplyCb          FpOnPingReply;
  FpOnClientRequestCb      FpOnClientRequest;
  FpOnTimeoutCb            FpOnTimeout;
  FpOnRequestVoteCb        FpOnRequestVote;
  FpOnRequestVoteReplyCb   FpOnRequestVoteReply;
  FpOnAppendEntriesCb      FpOnAppendEntries;
  FpOnAppendEntriesReplyCb FpOnAppendEntriesReply;
  FpOnSnapshotCb           FpOnSnapshot;
  FpOnSnapshotReplyCb      FpOnSnapshotReply;

  // tools
  SSyncRespMgr* pSyncRespMgr;

  // restore state
  bool restoreFinish;
  // SSnapshot*             pSnapshot;
  SSyncSnapshotSender*   senders[TSDB_MAX_REPLICA];
  SSyncSnapshotReceiver* pNewNodeReceiver;

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
void       syncNodeStart(SSyncNode* pSyncNode);
void       syncNodeStartStandBy(SSyncNode* pSyncNode);
void       syncNodeClose(SSyncNode* pSyncNode);
void       syncNodePreClose(SSyncNode* pSyncNode);
int32_t    syncNodePropose(SSyncNode* pSyncNode, SRpcMsg* pMsg, bool isWeak);

// option
bool          syncNodeSnapshotEnable(SSyncNode* pSyncNode);
ESyncStrategy syncNodeStrategy(SSyncNode* pSyncNode);
SyncIndex     syncNodeGetSnapshotConfigIndex(SSyncNode* pSyncNode, SyncIndex snapshotLastApplyIndex);

// ping --------------
int32_t syncNodePing(SSyncNode* pSyncNode, const SRaftId* destRaftId, SyncPing* pMsg);
int32_t syncNodePingSelf(SSyncNode* pSyncNode);
int32_t syncNodePingPeers(SSyncNode* pSyncNode);
int32_t syncNodePingAll(SSyncNode* pSyncNode);

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
cJSON*    syncNode2Json(const SSyncNode* pSyncNode);
char*     syncNode2Str(const SSyncNode* pSyncNode);
char*     syncNode2SimpleStr(const SSyncNode* pSyncNode);
bool      syncNodeInConfig(SSyncNode* pSyncNode, const SSyncCfg* config);
void      syncNodeDoConfigChange(SSyncNode* pSyncNode, SSyncCfg* newConfig, SyncIndex lastConfigChangeIndex);
SyncIndex syncMinMatchIndex(SSyncNode* pSyncNode);

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

// snapshot --------------
bool syncNodeHasSnapshot(SSyncNode* pSyncNode);
void syncNodeMaybeUpdateCommitBySnapshot(SSyncNode* pSyncNode);

SyncIndex syncNodeGetLastIndex(const SSyncNode* pSyncNode);
SyncTerm  syncNodeGetLastTerm(SSyncNode* pSyncNode);
int32_t   syncNodeGetLastIndexTerm(SSyncNode* pSyncNode, SyncIndex* pLastIndex, SyncTerm* pLastTerm);
SyncIndex syncNodeSyncStartIndex(SSyncNode* pSyncNode);
SyncIndex syncNodeGetPreIndex(SSyncNode* pSyncNode, SyncIndex index);
SyncTerm  syncNodeGetPreTerm(SSyncNode* pSyncNode, SyncIndex index);
int32_t   syncNodeGetPreIndexTerm(SSyncNode* pSyncNode, SyncIndex index, SyncIndex* pPreIndex, SyncTerm* pPreTerm);

bool    syncNodeIsOptimizedOneReplica(SSyncNode* ths, SRpcMsg* pMsg);
int32_t syncNodeDoCommit(SSyncNode* ths, SyncIndex beginIndex, SyncIndex endIndex, uint64_t flag);
int32_t syncNodeFollowerCommit(SSyncNode* ths, SyncIndex newCommitIndex);
int32_t syncNodePreCommit(SSyncNode* ths, SSyncRaftEntry* pEntry, int32_t code);

int32_t syncNodeUpdateNewConfigIndex(SSyncNode* ths, SSyncCfg* pNewCfg);

bool                 syncNodeInRaftGroup(SSyncNode* ths, SRaftId* pRaftId);
SSyncSnapshotSender* syncNodeGetSnapshotSender(SSyncNode* ths, SRaftId* pDestId);
SSyncTimer*          syncNodeGetHbTimer(SSyncNode* ths, SRaftId* pDestId);
SPeerState*          syncNodeGetPeerState(SSyncNode* ths, const SRaftId* pDestId);
bool syncNodeNeedSendAppendEntries(SSyncNode* ths, const SRaftId* pDestId, const SyncAppendEntries* pMsg);

int32_t syncGetSnapshotMeta(int64_t rid, struct SSnapshotMeta* sMeta);
int32_t syncGetSnapshotMetaByIndex(int64_t rid, SyncIndex snapshotIndex, struct SSnapshotMeta* sMeta);

bool syncNodeCanChange(SSyncNode* pSyncNode);

int32_t syncNodeLeaderTransfer(SSyncNode* pSyncNode);
int32_t syncNodeLeaderTransferTo(SSyncNode* pSyncNode, SNodeInfo newLeader);
int32_t syncDoLeaderTransfer(SSyncNode* ths, SRpcMsg* pRpcMsg, SSyncRaftEntry* pEntry);

int32_t syncNodeDynamicQuorum(const SSyncNode* pSyncNode);

bool    syncNodeIsMnode(SSyncNode* pSyncNode);
int32_t syncNodePeerStateInit(SSyncNode* pSyncNode);

// trace log
void syncLogRecvTimer(SSyncNode* pSyncNode, const SyncTimeout* pMsg, const char* s);
void syncLogRecvLocalCmd(SSyncNode* pSyncNode, const SyncLocalCmd* pMsg, const char* s);

void syncLogSendRequestVote(SSyncNode* pSyncNode, const SyncRequestVote* pMsg, const char* s);
void syncLogRecvRequestVote(SSyncNode* pSyncNode, const SyncRequestVote* pMsg, const char* s);

void syncLogSendRequestVoteReply(SSyncNode* pSyncNode, const SyncRequestVoteReply* pMsg, const char* s);
void syncLogRecvRequestVoteReply(SSyncNode* pSyncNode, const SyncRequestVoteReply* pMsg, const char* s);

void syncLogSendAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s);
void syncLogRecvAppendEntries(SSyncNode* pSyncNode, const SyncAppendEntries* pMsg, const char* s);

void syncLogSendAppendEntriesBatch(SSyncNode* pSyncNode, const SyncAppendEntriesBatch* pMsg, const char* s);
void syncLogRecvAppendEntriesBatch(SSyncNode* pSyncNode, const SyncAppendEntriesBatch* pMsg, const char* s);

void syncLogSendAppendEntriesReply(SSyncNode* pSyncNode, const SyncAppendEntriesReply* pMsg, const char* s);
void syncLogRecvAppendEntriesReply(SSyncNode* pSyncNode, const SyncAppendEntriesReply* pMsg, const char* s);

void syncLogSendHeartbeat(SSyncNode* pSyncNode, const SyncHeartbeat* pMsg, const char* s);
void syncLogRecvHeartbeat(SSyncNode* pSyncNode, const SyncHeartbeat* pMsg, const char* s);

void syncLogSendHeartbeatReply(SSyncNode* pSyncNode, const SyncHeartbeatReply* pMsg, const char* s);
void syncLogRecvHeartbeatReply(SSyncNode* pSyncNode, const SyncHeartbeatReply* pMsg, const char* s);

void syncLogSendSyncPreSnapshot(SSyncNode* pSyncNode, const SyncPreSnapshot* pMsg, const char* s);
void syncLogRecvSyncPreSnapshot(SSyncNode* pSyncNode, const SyncPreSnapshot* pMsg, const char* s);

void syncLogSendSyncPreSnapshotReply(SSyncNode* pSyncNode, const SyncPreSnapshotReply* pMsg, const char* s);
void syncLogRecvSyncPreSnapshotReply(SSyncNode* pSyncNode, const SyncPreSnapshotReply* pMsg, const char* s);

void syncLogSendSyncSnapshotSend(SSyncNode* pSyncNode, const SyncSnapshotSend* pMsg, const char* s);
void syncLogRecvSyncSnapshotSend(SSyncNode* pSyncNode, const SyncSnapshotSend* pMsg, const char* s);

void syncLogSendSyncSnapshotRsp(SSyncNode* pSyncNode, const SyncSnapshotRsp* pMsg, const char* s);
void syncLogRecvSyncSnapshotRsp(SSyncNode* pSyncNode, const SyncSnapshotRsp* pMsg, const char* s);

// syncUtil.h
void syncPrintNodeLog(const char* flags, ELogLevel level, int32_t dflag, SSyncNode* pNode, const char* format, ...);
void syncPrintSnapshotSenderLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotSender* pSender,
                                const char* format, ...);
void syncPrintSnapshotReceiverLog(const char* flags, ELogLevel level, int32_t dflag, SSyncSnapshotReceiver* pReceiver,
                                  const char* format, ...);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_INT_H*/
