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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "cJSON.h"
#include "sync.h"
#include "taosdef.h"
#include "tglobal.h"
#include "tlog.h"
#include "ttimer.h"

#define sFatal(...)                                              \
  {                                                              \
    if (sDebugFlag & DEBUG_FATAL) {                              \
      taosPrintLog("SYN FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); \
    }                                                            \
  }
#define sError(...)                                              \
  {                                                              \
    if (sDebugFlag & DEBUG_ERROR) {                              \
      taosPrintLog("SYN ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); \
    }                                                            \
  }
#define sWarn(...)                                             \
  {                                                            \
    if (sDebugFlag & DEBUG_WARN) {                             \
      taosPrintLog("SYN WARN ", DEBUG_WARN, 255, __VA_ARGS__); \
    }                                                          \
  }
#define sInfo(...)                                             \
  {                                                            \
    if (sDebugFlag & DEBUG_INFO) {                             \
      taosPrintLog("SYN INFO ", DEBUG_INFO, 255, __VA_ARGS__); \
    }                                                          \
  }
#define sDebug(...)                                                     \
  {                                                                     \
    if (sDebugFlag & DEBUG_DEBUG) {                                     \
      taosPrintLog("SYN DEBUG ", DEBUG_DEBUG, sDebugFlag, __VA_ARGS__); \
    }                                                                   \
  }
#define sTrace(...)                                                     \
  {                                                                     \
    if (sDebugFlag & DEBUG_TRACE) {                                     \
      taosPrintLog("SYN TRACE ", DEBUG_TRACE, sDebugFlag, __VA_ARGS__); \
    }                                                                   \
  }

struct SyncTimeout;
typedef struct SyncTimeout SyncTimeout;

struct SyncClientRequest;
typedef struct SyncClientRequest SyncClientRequest;

struct SyncPing;
typedef struct SyncPing SyncPing;

struct SyncPingReply;
typedef struct SyncPingReply SyncPingReply;

struct SyncRequestVote;
typedef struct SyncRequestVote SyncRequestVote;

struct SyncRequestVoteReply;
typedef struct SyncRequestVoteReply SyncRequestVoteReply;

struct SyncAppendEntries;
typedef struct SyncAppendEntries SyncAppendEntries;

struct SyncAppendEntriesReply;
typedef struct SyncAppendEntriesReply SyncAppendEntriesReply;

struct SSyncEnv;
typedef struct SSyncEnv SSyncEnv;

struct SRaftStore;
typedef struct SRaftStore SRaftStore;

struct SVotesGranted;
typedef struct SVotesGranted SVotesGranted;

struct SVotesRespond;
typedef struct SVotesRespond SVotesRespond;

struct SSyncIndexMgr;
typedef struct SSyncIndexMgr SSyncIndexMgr;

typedef struct SRaftId {
  SyncNodeId  addr;  // typedef uint64_t SyncNodeId;
  SyncGroupId vgId;  // typedef int32_t  SyncGroupId;
} SRaftId;

typedef struct SSyncNode {
  // init by SSyncInfo
  SyncGroupId vgId;
  SSyncCfg    syncCfg;
  char        path[TSDB_FILENAME_LEN];
  char        raftStorePath[TSDB_FILENAME_LEN * 2];

  // sync io
  SWal* pWal;
  void* rpcClient;
  int32_t (*FpSendMsg)(void* rpcClient, const SEpSet* pEpSet, SRpcMsg* pMsg);
  void* queue;
  int32_t (*FpEqMsg)(void* queue, SRpcMsg* pMsg);

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
  uint64_t          electTimerLogicClockUser;
  TAOS_TMR_CALLBACK FpElectTimerCB;  // Timer Fp
  uint64_t          electTimerCounter;

  // heartbeat timer
  tmr_h             pHeartbeatTimer;
  int32_t           heartbeatTimerMS;
  uint64_t          heartbeatTimerLogicClock;
  uint64_t          heartbeatTimerLogicClockUser;
  TAOS_TMR_CALLBACK FpHeartbeatTimerCB;  // Timer Fp
  uint64_t          heartbeatTimerCounter;

  // callback
  int32_t (*FpOnPing)(SSyncNode* ths, SyncPing* pMsg);
  int32_t (*FpOnPingReply)(SSyncNode* ths, SyncPingReply* pMsg);
  int32_t (*FpOnClientRequest)(SSyncNode* ths, SyncClientRequest* pMsg);
  int32_t (*FpOnRequestVote)(SSyncNode* ths, SyncRequestVote* pMsg);
  int32_t (*FpOnRequestVoteReply)(SSyncNode* ths, SyncRequestVoteReply* pMsg);
  int32_t (*FpOnAppendEntries)(SSyncNode* ths, SyncAppendEntries* pMsg);
  int32_t (*FpOnAppendEntriesReply)(SSyncNode* ths, SyncAppendEntriesReply* pMsg);
  int32_t (*FpOnTimeout)(SSyncNode* pSyncNode, SyncTimeout* pMsg);

} SSyncNode;

// open/close --------------
SSyncNode* syncNodeOpen(const SSyncInfo* pSyncInfo);
void       syncNodeClose(SSyncNode* pSyncNode);

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

// utils --------------
int32_t    syncNodeSendMsgById(const SRaftId* destRaftId, SSyncNode* pSyncNode, SRpcMsg* pMsg);
int32_t    syncNodeSendMsgByInfo(const SNodeInfo* nodeInfo, SSyncNode* pSyncNode, SRpcMsg* pMsg);
cJSON*     syncNode2Json(const SSyncNode* pSyncNode);
char*      syncNode2Str(const SSyncNode* pSyncNode);
SSyncNode* syncNodeAcquire(int64_t rid);
void       syncNodeRelease(SSyncNode* pNode);

// raft state change --------------
void syncNodeUpdateTerm(SSyncNode* pSyncNode, SyncTerm term);
void syncNodeBecomeFollower(SSyncNode* pSyncNode);
void syncNodeBecomeLeader(SSyncNode* pSyncNode);

void syncNodeCandidate2Leader(SSyncNode* pSyncNode);
void syncNodeFollower2Candidate(SSyncNode* pSyncNode);
void syncNodeLeader2Follower(SSyncNode* pSyncNode);
void syncNodeCandidate2Follower(SSyncNode* pSyncNode);

// raft vote --------------
void syncNodeVoteForTerm(SSyncNode* pSyncNode, SyncTerm term, SRaftId* pRaftId);
void syncNodeVoteForSelf(SSyncNode* pSyncNode);

// for debug --------------
void syncNodePrint(SSyncNode* pObj);
void syncNodePrint2(char* s, SSyncNode* pObj);
void syncNodeLog(SSyncNode* pObj);
void syncNodeLog2(char* s, SSyncNode* pObj);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_INT_H*/
