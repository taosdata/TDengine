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
typedef struct SVotesGranted          SVotesGranted;
typedef struct SVotesRespond          SVotesRespond;
typedef struct SSyncIndexMgr          SSyncIndexMgr;
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
typedef struct SSyncLogBuffer         SSyncLogBuffer;
typedef struct SSyncLogReplMgr        SSyncLogReplMgr;

#define MAX_CONFIG_INDEX_COUNT 256

typedef struct SRaftCfg {
  SSyncCfg          cfg;
  int32_t           batchSize;
  int8_t            isStandBy;
  int8_t            snapshotStrategy;
  SyncIndex         lastConfigIndex;
  int32_t           configIndexCount;
  SyncIndex         configIndexArr[MAX_CONFIG_INDEX_COUNT];
} SRaftCfg;

typedef struct SRaftId {
  SyncNodeId  addr;
  SyncGroupId vgId;
} SRaftId;

typedef struct SRaftStore {
  SyncTerm currentTerm;
  SRaftId  voteFor;
  TdThreadMutex mutex;
} SRaftStore;

typedef struct SSyncHbTimerData {
  int64_t     syncNodeRid;
  SSyncTimer* pTimer;
  SRaftId     destId;
  uint64_t    logicClock;
  int64_t     execTime;
  int64_t     rid;
} SSyncHbTimerData;

typedef struct SSyncTimer {
  void*             pTimer;
  TAOS_TMR_CALLBACK timerCb;
  uint64_t          logicClock;
  uint64_t          counter;
  int32_t           timerMS;
  int64_t           timeStamp;
  SRaftId           destId;
  int64_t           hbDataRid;
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

typedef struct SSyncNode {
  // init by SSyncInfo
  SyncGroupId vgId;
  SRaftCfg    raftCfg;
  char        path[TSDB_FILENAME_LEN];
  char        raftStorePath[TSDB_FILENAME_LEN * 2];
  char        configPath[TSDB_FILENAME_LEN * 2];

  // sync io
  SSyncLogBuffer* pLogBuf;
  SWal*           pWal;
  const SMsgCb*   msgcb;
  int32_t (*syncSendMSg)(const SEpSet* pEpSet, SRpcMsg* pMsg);
  int32_t (*syncEqMsg)(const SMsgCb* msgcb, SRpcMsg* pMsg);
  int32_t (*syncEqCtrlMsg)(const SMsgCb* msgcb, SRpcMsg* pMsg);

  // init internal
  SNodeInfo myNodeInfo;
  SRaftId   myRaftId;

  int32_t   peersNum;
  SNodeInfo peersNodeInfo[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
  SEpSet    peersEpset[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
  SRaftId   peersId[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];

  int32_t replicaNum;
  int32_t totalReplicaNum;
  SRaftId replicasId[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];

  // raft algorithm
  SSyncFSM* pFsm;
  int32_t   quorum;
  SRaftId   leaderCache;
  ESyncFsmState fsmState;

  // life cycle
  int64_t rid;

  // tla+ server vars
  ESyncState state;
  SRaftStore raftStore;

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
  SSyncTimer peerHeartbeatTimerArr[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];

  // tools
  SSyncRespMgr* pSyncRespMgr;

  // restore state
  bool restoreFinish;
  // SSnapshot*             pSnapshot;
  SSyncSnapshotSender*   senders[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
  SSyncSnapshotReceiver* pNewNodeReceiver;

  // log replication mgr
  SSyncLogReplMgr* logReplMgrs[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];

  SPeerState peerStates[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];

  // is config changing
  bool changing;

  int64_t snapshottingIndex;
  int64_t snapshottingTime;
  int64_t minMatchIndex;

  int64_t startTime;
  int64_t roleTimeMs;
  int64_t lastReplicateTime;

  int32_t electNum;
  int32_t becomeLeaderNum;
  int32_t configChangeNum;
  int32_t hbSlowNum;
  int32_t hbrSlowNum;
  int32_t tmrRoutineNum;

  bool isStart;

} SSyncNode;

// open/close --------------
SSyncNode* syncNodeOpen(SSyncInfo* pSyncInfo, int32_t vnodeVersion);
int32_t    syncNodeStart(SSyncNode* pSyncNode);
int32_t    syncNodeStartStandBy(SSyncNode* pSyncNode);
void       syncNodeClose(SSyncNode* pSyncNode);
void       syncNodePreClose(SSyncNode* pSyncNode);
void       syncNodePostClose(SSyncNode* pSyncNode);
int32_t    syncNodePropose(SSyncNode* pSyncNode, SRpcMsg* pMsg, bool isWeak, int64_t* seq);
int32_t    syncNodeRestore(SSyncNode* pSyncNode);
void       syncHbTimerDataFree(SSyncHbTimerData* pData);

// config
int32_t syncNodeChangeConfig(SSyncNode* ths, SSyncRaftEntry* pEntry, char* str);

// on message ---------------------
int32_t syncNodeOnTimeout(SSyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnClientRequest(SSyncNode* ths, SRpcMsg* pMsg, SyncIndex* pRetIndex);
int32_t syncNodeOnRequestVote(SSyncNode* pNode, const SRpcMsg* pMsg);
int32_t syncNodeOnRequestVoteReply(SSyncNode* pNode, const SRpcMsg* pMsg);
int32_t syncNodeOnAppendEntries(SSyncNode* pNode, const SRpcMsg* pMsg);
int32_t syncNodeOnAppendEntriesReply(SSyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnSnapshot(SSyncNode* ths, SRpcMsg* pMsg);
int32_t syncNodeOnSnapshotRsp(SSyncNode* ths, SRpcMsg* pMsg);
int32_t syncNodeOnHeartbeat(SSyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnHeartbeatReply(SSyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnLocalCmd(SSyncNode* ths, const SRpcMsg* pMsg);

// timer control --------------
int32_t syncNodeStartPingTimer(SSyncNode* pSyncNode);
int32_t syncNodeStopPingTimer(SSyncNode* pSyncNode);
int32_t syncNodeStartElectTimer(SSyncNode* pSyncNode, int32_t ms);
int32_t syncNodeStopElectTimer(SSyncNode* pSyncNode);
int32_t syncNodeRestartElectTimer(SSyncNode* pSyncNode, int32_t ms);
void    syncNodeResetElectTimer(SSyncNode* pSyncNode);
int32_t syncNodeStartHeartbeatTimer(SSyncNode* pSyncNode);
int32_t syncNodeStopHeartbeatTimer(SSyncNode* pSyncNode);
int32_t syncNodeRestartHeartbeatTimer(SSyncNode* pSyncNode);

// utils --------------
int32_t   syncNodeSendMsgById(const SRaftId* destRaftId, SSyncNode* pSyncNode, SRpcMsg* pMsg);
SyncIndex syncMinMatchIndex(SSyncNode* pSyncNode);
int32_t   syncCacheEntry(SSyncLogStore* pLogStore, SSyncRaftEntry* pEntry, LRUHandle** h);
bool      syncNodeHeartbeatReplyTimeout(SSyncNode* pSyncNode);
bool      syncNodeSnapshotSending(SSyncNode* pSyncNode);
bool      syncNodeSnapshotRecving(SSyncNode* pSyncNode);
bool      syncNodeIsReadyForRead(SSyncNode* pSyncNode);

// raft state change --------------
void syncNodeUpdateTerm(SSyncNode* pSyncNode, SyncTerm term);
void syncNodeUpdateTermWithoutStepDown(SSyncNode* pSyncNode, SyncTerm term);
void syncNodeStepDown(SSyncNode* pSyncNode, SyncTerm newTerm);
void syncNodeBecomeFollower(SSyncNode* pSyncNode, const char* debugStr);
void syncNodeBecomeLearner(SSyncNode* pSyncNode, const char* debugStr);
void syncNodeBecomeLeader(SSyncNode* pSyncNode, const char* debugStr);
void syncNodeCandidate2Leader(SSyncNode* pSyncNode);
void syncNodeFollower2Candidate(SSyncNode* pSyncNode);
void syncNodeLeader2Follower(SSyncNode* pSyncNode);
void syncNodeCandidate2Follower(SSyncNode* pSyncNode);

// raft vote --------------
void syncNodeVoteForTerm(SSyncNode* pSyncNode, SyncTerm term, SRaftId* pRaftId);
void syncNodeVoteForSelf(SSyncNode* pSyncNode, SyncTerm term);

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
