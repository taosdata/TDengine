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

typedef struct SSyncTimeout            SyncTimeout;
typedef struct SSyncClientRequest      SyncClientRequest;
typedef struct SSyncRequestVote        SyncRequestVote;
typedef struct SSyncRequestVoteReply   SyncRequestVoteReply;
typedef struct SSyncAppendEntries      SyncAppendEntries;
typedef struct SSyncAppendEntriesReply SyncAppendEntriesReply;
typedef struct SSyncEnv                SyncEnv;
typedef struct SSVotesGranted          SVotesGranted;
typedef struct SSVotesRespond          SVotesRespond;
typedef struct SSyncIndexMgr           SyncIndexMgr;
typedef struct SSyncRespMgr            SyncRespMgr;
typedef struct SSyncSnapshotSender     SyncSnapshotSender;
typedef struct SSyncSnapshotReceiver   SyncSnapshotReceiver;
typedef struct SSyncTimer              SyncTimer;
typedef struct SSyncHbTimerData        SyncHbTimerData;
typedef struct SSyncSnapshotSend       SyncSnapshotSend;
typedef struct SSyncSnapshotRsp        SyncSnapshotRsp;
typedef struct SSyncLocalCmd           SyncLocalCmd;
typedef struct SSyncAppendEntriesBatch SyncAppendEntriesBatch;
typedef struct SSyncPreSnapshotReply   SyncPreSnapshotReply;
typedef struct SSyncHeartbeatReply     SyncHeartbeatReply;
typedef struct SSyncHeartbeat          SyncHeartbeat;
typedef struct SSyncPreSnapshot        SyncPreSnapshot;
typedef struct SSyncLogBuffer          SyncLogBuffer;
typedef struct SSyncLogReplMgr         SyncLogReplMgr;

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
  SyncTimer*  pTimer;
  SRaftId     destId;
  uint64_t    logicClock;
  int64_t     execTime;
  int64_t     rid;
} SyncHbTimerData;

typedef struct SSyncTimer {
  void*             pTimer;
  TAOS_TMR_CALLBACK timerCb;
  uint64_t          logicClock;
  uint64_t          counter;
  int32_t           timerMS;
  int64_t           timeStamp;
  SRaftId           destId;
  int64_t           hbDataRid;
} SyncTimer;

typedef struct SElectTimerParam {
  uint64_t   logicClock;
  SyncNode*  pSyncNode;
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
  SyncLogBuffer*  pLogBuf;
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
  SyncIndexMgr* pNextIndex;
  SyncIndexMgr* pMatchIndex;

  // tla+ log vars
  SSyncLogStore* pLogStore;
  SyncIndex      commitIndex;

  // assigned leader log vars
  SyncIndex assignedCommitIndex;

  SyncTerm      arbTerm;
  TdThreadMutex arbTokenMutex;
  char          arbToken[TSDB_ARB_TOKEN_SIZE];

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
  SyncTimer peerHeartbeatTimerArr[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];

  // tools
  SyncRespMgr* pSyncRespMgr;

  // restore state
  bool restoreFinish;
  // SSnapshot*             pSnapshot;
  SyncSnapshotSender*   senders[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];
  SyncSnapshotReceiver* pNewNodeReceiver;

  // log replication mgr
  SyncLogReplMgr* logReplMgrs[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];

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
  int32_t becomeAssignedLeaderNum;
  int32_t configChangeNum;
  int32_t hbSlowNum;
  int32_t hbrSlowNum;
  int32_t tmrRoutineNum;

  bool isStart;

} SyncNode;

// open/close --------------
SyncNode* syncNodeOpen(SSyncInfo* pSyncInfo, int32_t vnodeVersion);
int32_t   syncNodeStart(SyncNode* pSyncNode);
int32_t   syncNodeStartStandBy(SyncNode* pSyncNode);
void      syncNodeClose(SyncNode* pSyncNode);
void      syncNodePreClose(SyncNode* pSyncNode);
void      syncNodePostClose(SyncNode* pSyncNode);
int32_t   syncNodePropose(SyncNode* pSyncNode, SRpcMsg* pMsg, bool isWeak, int64_t* seq);
int32_t   syncNodeRestore(SyncNode* pSyncNode);
void      syncHbTimerDataFree(SyncHbTimerData* pData);

// config
int32_t syncNodeChangeConfig(SyncNode* ths, SyncRaftEntry* pEntry, char* str);

// on message ---------------------
int32_t syncNodeOnTimeout(SyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnClientRequest(SyncNode* ths, SRpcMsg* pMsg, SyncIndex* pRetIndex);
int32_t syncNodeOnRequestVote(SyncNode* pNode, const SRpcMsg* pMsg);
int32_t syncNodeOnRequestVoteReply(SyncNode* pNode, const SRpcMsg* pMsg);
int32_t syncNodeOnAppendEntries(SyncNode* pNode, SRpcMsg* pMsg);
int32_t syncNodeOnAppendEntriesReply(SyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnSnapshot(SyncNode* ths, SRpcMsg* pMsg);
int32_t syncNodeOnSnapshotRsp(SyncNode* ths, SRpcMsg* pMsg);
int32_t syncNodeOnHeartbeat(SyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnHeartbeatReply(SyncNode* ths, const SRpcMsg* pMsg);
int32_t syncNodeOnLocalCmd(SyncNode* ths, const SRpcMsg* pMsg);

// timer control --------------
int32_t syncNodeStartPingTimer(SyncNode* pSyncNode);
int32_t syncNodeStopPingTimer(SyncNode* pSyncNode);
int32_t syncNodeStartElectTimer(SyncNode* pSyncNode, int32_t ms);
int32_t syncNodeStopElectTimer(SyncNode* pSyncNode);
int32_t syncNodeRestartElectTimer(SyncNode* pSyncNode, int32_t ms);
void    syncNodeResetElectTimer(SyncNode* pSyncNode);
int32_t syncNodeStartHeartbeatTimer(SyncNode* pSyncNode);
int32_t syncNodeStopHeartbeatTimer(SyncNode* pSyncNode);
int32_t syncNodeRestartHeartbeatTimer(SyncNode* pSyncNode);

// utils --------------
int32_t   syncNodeSendMsgById(const SRaftId* destRaftId, SyncNode* pSyncNode, SRpcMsg* pMsg);
SyncIndex syncMinMatchIndex(SyncNode* pSyncNode);
int32_t   syncCacheEntry(SSyncLogStore* pLogStore, SyncRaftEntry* pEntry, LRUHandle** h);
bool      syncNodeHeartbeatReplyTimeout(SyncNode* pSyncNode);
bool      syncNodeSnapshotSending(SyncNode* pSyncNode);
bool      syncNodeSnapshotRecving(SyncNode* pSyncNode);
bool      syncNodeIsReadyForRead(SyncNode* pSyncNode);

// raft state change --------------
void syncNodeUpdateTerm(SyncNode* pSyncNode, SyncTerm term);
void syncNodeUpdateTermWithoutStepDown(SyncNode* pSyncNode, SyncTerm term);
void syncNodeStepDown(SyncNode* pSyncNode, SyncTerm newTerm);
void syncNodeBecomeFollower(SyncNode* pSyncNode, const char* debugStr);
void syncNodeBecomeLearner(SyncNode* pSyncNode, const char* debugStr);
void syncNodeBecomeLeader(SyncNode* pSyncNode, const char* debugStr);
void syncNodeBecomeAssignedLeader(SyncNode* pSyncNode);
void syncNodeCandidate2Leader(SyncNode* pSyncNode);
void syncNodeFollower2Candidate(SyncNode* pSyncNode);
void syncNodeLeader2Follower(SyncNode* pSyncNode);
void syncNodeCandidate2Follower(SyncNode* pSyncNode);
int32_t syncNodeAssignedLeader2Leader(SyncNode* pSyncNode);

// raft vote --------------
void syncNodeVoteForTerm(SyncNode* pSyncNode, SyncTerm term, SRaftId* pRaftId);
void syncNodeVoteForSelf(SyncNode* pSyncNode, SyncTerm term);

// log replication
SyncLogReplMgr* syncNodeGetLogReplMgr(SyncNode* pNode, SRaftId* pDestId);

// snapshot --------------
bool    syncNodeHasSnapshot(SyncNode* pSyncNode);
void    syncNodeMaybeUpdateCommitBySnapshot(SyncNode* pSyncNode);
int32_t syncNodeStartSnapshot(SyncNode* pSyncNode, SRaftId* pDestId);

SyncIndex syncNodeGetLastIndex(const SyncNode* pSyncNode);
SyncTerm  syncNodeGetLastTerm(SyncNode* pSyncNode);
int32_t   syncNodeGetLastIndexTerm(SyncNode* pSyncNode, SyncIndex* pLastIndex, SyncTerm* pLastTerm);
SyncIndex syncNodeSyncStartIndex(SyncNode* pSyncNode);
SyncIndex syncNodeGetPreIndex(SyncNode* pSyncNode, SyncIndex index);
SyncTerm  syncNodeGetPreTerm(SyncNode* pSyncNode, SyncIndex index);
int32_t   syncNodeGetPreIndexTerm(SyncNode* pSyncNode, SyncIndex index, SyncIndex* pPreIndex, SyncTerm* pPreTerm);

int32_t syncNodeDoCommit(SyncNode* ths, SyncIndex beginIndex, SyncIndex endIndex, uint64_t flag);
int32_t syncNodeFollowerCommit(SyncNode* ths, SyncIndex newCommitIndex);
int32_t syncNodePreCommit(SyncNode* ths, SyncRaftEntry* pEntry, int32_t code);

bool                syncNodeInRaftGroup(SyncNode* ths, SRaftId* pRaftId);
SyncSnapshotSender* syncNodeGetSnapshotSender(SyncNode* ths, SRaftId* pDestId);
SyncTimer*          syncNodeGetHbTimer(SyncNode* ths, SRaftId* pDestId);
SPeerState*         syncNodeGetPeerState(SyncNode* ths, const SRaftId* pDestId);
bool                syncNodeNeedSendAppendEntries(SyncNode* ths, const SRaftId* pDestId, const SyncAppendEntries* pMsg);

int32_t syncGetSnapshotMeta(int64_t rid, struct SSnapshotMeta* sMeta);
int32_t syncGetSnapshotMetaByIndex(int64_t rid, SyncIndex snapshotIndex, struct SSnapshotMeta* sMeta);

int32_t syncNodeDynamicQuorum(const SyncNode* pSyncNode);
bool    syncNodeIsMnode(SyncNode* pSyncNode);
int32_t syncNodePeerStateInit(SyncNode* pSyncNode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_INT_H*/
