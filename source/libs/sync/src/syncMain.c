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

#include <stdint.h>
#include "sync.h"
#include "syncAppendEntries.h"
#include "syncAppendEntriesReply.h"
#include "syncEnv.h"
#include "syncIndexMgr.h"
#include "syncInt.h"
#include "syncRaft.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncRequestVote.h"
#include "syncRequestVoteReply.h"
#include "syncTimeout.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"

static int32_t tsNodeRefId = -1;

// ------ local funciton ---------
static void syncNodeEqPingTimer(void* param, void* tmrId);
static void syncNodeEqElectTimer(void* param, void* tmrId);
static void syncNodeEqHeartbeatTimer(void* param, void* tmrId);

static int32_t syncNodeOnPingCb(SSyncNode* ths, SyncPing* pMsg);
static int32_t syncNodeOnPingReplyCb(SSyncNode* ths, SyncPingReply* pMsg);

static void UpdateTerm(SSyncNode* pSyncNode, SyncTerm term);
static void syncNodeBecomeFollower(SSyncNode* pSyncNode);
static void syncNodeBecomeLeader(SSyncNode* pSyncNode);
static void syncNodeFollower2Candidate(SSyncNode* pSyncNode);
static void syncNodeCandidate2Leader(SSyncNode* pSyncNode);
static void syncNodeLeader2Follower(SSyncNode* pSyncNode);
static void syncNodeCandidate2Follower(SSyncNode* pSyncNode);
// ---------------------------------

int32_t syncInit() {
  sTrace("syncInit ok");
  return 0;
}

void syncCleanUp() { sTrace("syncCleanUp ok"); }

int64_t syncStart(const SSyncInfo* pSyncInfo) {
  SSyncNode* pSyncNode = syncNodeOpen(pSyncInfo);
  assert(pSyncNode != NULL);
  return 0;
}

void syncStop(int64_t rid) {}

int32_t syncReconfig(int64_t rid, const SSyncCfg* pSyncCfg) { return 0; }

int32_t syncForwardToPeer(int64_t rid, const SRpcMsg* pBuf, bool isWeak) { return 0; }

ESyncState syncGetMyRole(int64_t rid) { return TAOS_SYNC_STATE_LEADER; }

void syncGetNodesRole(int64_t rid, SNodesRole* pNodeRole) {}

SSyncNode* syncNodeOpen(const SSyncInfo* pSyncInfo) {
  SSyncNode* pSyncNode = (SSyncNode*)malloc(sizeof(SSyncNode));
  assert(pSyncNode != NULL);
  memset(pSyncNode, 0, sizeof(SSyncNode));

  // init by SSyncInfo
  pSyncNode->vgId = pSyncInfo->vgId;
  pSyncNode->syncCfg = pSyncInfo->syncCfg;
  memcpy(pSyncNode->path, pSyncInfo->path, sizeof(pSyncNode->path));
  pSyncNode->pWal = pSyncInfo->pWal;
  pSyncNode->rpcClient = pSyncInfo->rpcClient;
  pSyncNode->FpSendMsg = pSyncInfo->FpSendMsg;
  pSyncNode->queue = pSyncInfo->queue;
  pSyncNode->FpEqMsg = pSyncInfo->FpEqMsg;

  // init internal
  pSyncNode->myNodeInfo = pSyncInfo->syncCfg.nodeInfo[pSyncInfo->syncCfg.myIndex];
  syncUtilnodeInfo2raftId(&pSyncNode->myNodeInfo, pSyncInfo->vgId, &pSyncNode->myRaftId);

  // init peersNum, peers, peersId
  pSyncNode->peersNum = pSyncInfo->syncCfg.replicaNum - 1;
  int j = 0;
  for (int i = 0; i < pSyncInfo->syncCfg.replicaNum; ++i) {
    if (i != pSyncInfo->syncCfg.myIndex) {
      pSyncNode->peersNodeInfo[j] = pSyncInfo->syncCfg.nodeInfo[i];
      j++;
    }
  }
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    syncUtilnodeInfo2raftId(&pSyncNode->peersNodeInfo[i], pSyncInfo->vgId, &pSyncNode->peersId[i]);
  }

  // init replicaNum, replicasId
  pSyncNode->replicaNum = pSyncInfo->syncCfg.replicaNum;
  for (int i = 0; i < pSyncInfo->syncCfg.replicaNum; ++i) {
    syncUtilnodeInfo2raftId(&pSyncInfo->syncCfg.nodeInfo[i], pSyncInfo->vgId, &pSyncNode->replicasId[i]);
  }

  // init raft algorithm
  pSyncNode->pFsm = pSyncInfo->pFsm;
  pSyncNode->quorum = syncUtilQuorum(pSyncInfo->syncCfg.replicaNum);
  pSyncNode->leaderCache = EMPTY_RAFT_ID;

  // init life cycle

  // init TLA+ server vars
  pSyncNode->state = TAOS_SYNC_STATE_FOLLOWER;
  assert(pSyncNode->pRaftStore != NULL);

  // init TLA+ candidate vars
  pSyncNode->pVotesGranted = voteGrantedCreate(pSyncNode);
  assert(pSyncNode->pVotesGranted != NULL);
  pSyncNode->pVotesRespond = votesRespondCreate(pSyncNode);
  assert(pSyncNode->pVotesRespond != NULL);

  // init TLA+ leader vars
  pSyncNode->pNextIndex = syncIndexMgrCreate(pSyncNode);
  assert(pSyncNode->pNextIndex != NULL);
  pSyncNode->pMatchIndex = syncIndexMgrCreate(pSyncNode);
  assert(pSyncNode->pMatchIndex != NULL);

  // init TLA+ log vars
  pSyncNode->pLogStore = logStoreCreate(pSyncNode);
  assert(pSyncNode->pLogStore != NULL);
  pSyncNode->commitIndex = 0;

  // init ping timer
  pSyncNode->pPingTimer = NULL;
  pSyncNode->pingTimerMS = PING_TIMER_MS;
  atomic_store_64(&pSyncNode->pingTimerLogicClock, 0);
  atomic_store_64(&pSyncNode->pingTimerLogicClockUser, 0);
  pSyncNode->FpPingTimer = syncNodeEqPingTimer;
  pSyncNode->pingTimerCounter = 0;

  // init elect timer
  pSyncNode->pElectTimer = NULL;
  pSyncNode->electTimerMS = syncUtilElectRandomMS();
  atomic_store_64(&pSyncNode->electTimerLogicClock, 0);
  atomic_store_64(&pSyncNode->electTimerLogicClockUser, 0);
  pSyncNode->FpElectTimer = syncNodeEqElectTimer;
  pSyncNode->electTimerCounter = 0;

  // init heartbeat timer
  pSyncNode->pHeartbeatTimer = NULL;
  pSyncNode->heartbeatTimerMS = HEARTBEAT_TIMER_MS;
  atomic_store_64(&pSyncNode->heartbeatTimerLogicClock, 0);
  atomic_store_64(&pSyncNode->heartbeatTimerLogicClockUser, 0);
  pSyncNode->FpHeartbeatTimer = syncNodeEqHeartbeatTimer;
  pSyncNode->heartbeatTimerCounter = 0;

  // init callback
  pSyncNode->FpOnPing = syncNodeOnPingCb;
  pSyncNode->FpOnPingReply = syncNodeOnPingReplyCb;
  pSyncNode->FpOnRequestVote = syncNodeOnRequestVoteCb;
  pSyncNode->FpOnRequestVoteReply = syncNodeOnRequestVoteReplyCb;
  pSyncNode->FpOnAppendEntries = syncNodeOnAppendEntriesCb;
  pSyncNode->FpOnAppendEntriesReply = syncNodeOnAppendEntriesReplyCb;
  pSyncNode->FpOnTimeout = syncNodeOnTimeoutCb;

  return pSyncNode;
}

void syncNodeClose(SSyncNode* pSyncNode) {
  assert(pSyncNode != NULL);
  free(pSyncNode);
}

cJSON* syncNode2Json(const SSyncNode* pSyncNode) {
  char   u64buf[128];
  cJSON* pRoot = cJSON_CreateObject();

  // init by SSyncInfo
  cJSON_AddNumberToObject(pRoot, "vgId", pSyncNode->vgId);
  cJSON_AddStringToObject(pRoot, "path", pSyncNode->path);
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pWal);
  cJSON_AddStringToObject(pRoot, "pWal", u64buf);

  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->rpcClient);
  cJSON_AddStringToObject(pRoot, "rpcClient", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpSendMsg);
  cJSON_AddStringToObject(pRoot, "FpSendMsg", u64buf);

  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->queue);
  cJSON_AddStringToObject(pRoot, "queue", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpEqMsg);
  cJSON_AddStringToObject(pRoot, "FpEqMsg", u64buf);

  // init internal
  cJSON* pMe = syncUtilNodeInfo2Json(&pSyncNode->myNodeInfo);
  cJSON_AddItemToObject(pRoot, "myNodeInfo", pMe);
  cJSON* pRaftId = syncUtilRaftId2Json(&pSyncNode->myRaftId);
  cJSON_AddItemToObject(pRoot, "myRaftId", pRaftId);

  cJSON_AddNumberToObject(pRoot, "peersNum", pSyncNode->peersNum);
  cJSON* pPeers = cJSON_CreateArray();
  cJSON_AddItemToObject(pRoot, "peersNodeInfo", pPeers);
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    cJSON_AddItemToArray(pPeers, syncUtilNodeInfo2Json(&pSyncNode->peersNodeInfo[i]));
  }
  cJSON* pPeersId = cJSON_CreateArray();
  cJSON_AddItemToObject(pRoot, "peersId", pPeersId);
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    cJSON_AddItemToArray(pPeersId, syncUtilRaftId2Json(&pSyncNode->peersId[i]));
  }

  cJSON_AddNumberToObject(pRoot, "replicaNum", pSyncNode->replicaNum);
  cJSON* pReplicasId = cJSON_CreateArray();
  cJSON_AddItemToObject(pRoot, "replicasId", pReplicasId);
  for (int i = 0; i < pSyncNode->replicaNum; ++i) {
    cJSON_AddItemToArray(pReplicasId, syncUtilRaftId2Json(&pSyncNode->replicasId[i]));
  }

  // raft algorithm
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pFsm);
  cJSON_AddStringToObject(pRoot, "pFsm", u64buf);
  cJSON_AddNumberToObject(pRoot, "quorum", pSyncNode->quorum);
  cJSON* pLaderCache = syncUtilRaftId2Json(&pSyncNode->leaderCache);
  cJSON_AddItemToObject(pRoot, "leaderCache", pLaderCache);

  // tla+ server vars
  cJSON_AddNumberToObject(pRoot, "state", pSyncNode->state);
  cJSON_AddStringToObject(pRoot, "state_str", syncUtilState2String(pSyncNode->state));

  // tla+ candidate vars

  // tla+ leader vars

  // tla+ log vars

  // ping timer
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pPingTimer);
  cJSON_AddStringToObject(pRoot, "pPingTimer", u64buf);
  cJSON_AddNumberToObject(pRoot, "pingTimerMS", pSyncNode->pingTimerMS);
  snprintf(u64buf, sizeof(u64buf), "%lu", pSyncNode->pingTimerLogicClock);
  cJSON_AddStringToObject(pRoot, "pingTimerLogicClock", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%lu", pSyncNode->pingTimerLogicClockUser);
  cJSON_AddStringToObject(pRoot, "pingTimerLogicClockUser", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpPingTimer);
  cJSON_AddStringToObject(pRoot, "FpPingTimer", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%lu", pSyncNode->pingTimerCounter);
  cJSON_AddStringToObject(pRoot, "pingTimerCounter", u64buf);

  // elect timer
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pElectTimer);
  cJSON_AddStringToObject(pRoot, "pElectTimer", u64buf);
  cJSON_AddNumberToObject(pRoot, "electTimerMS", pSyncNode->electTimerMS);
  snprintf(u64buf, sizeof(u64buf), "%lu", pSyncNode->electTimerLogicClock);
  cJSON_AddStringToObject(pRoot, "electTimerLogicClock", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%lu", pSyncNode->electTimerLogicClockUser);
  cJSON_AddStringToObject(pRoot, "electTimerLogicClockUser", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpElectTimer);
  cJSON_AddStringToObject(pRoot, "FpElectTimer", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%lu", pSyncNode->electTimerCounter);
  cJSON_AddStringToObject(pRoot, "electTimerCounter", u64buf);

  // heartbeat timer
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pHeartbeatTimer);
  cJSON_AddStringToObject(pRoot, "pHeartbeatTimer", u64buf);
  cJSON_AddNumberToObject(pRoot, "heartbeatTimerMS", pSyncNode->heartbeatTimerMS);
  snprintf(u64buf, sizeof(u64buf), "%lu", pSyncNode->heartbeatTimerLogicClock);
  cJSON_AddStringToObject(pRoot, "heartbeatTimerLogicClock", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%lu", pSyncNode->heartbeatTimerLogicClockUser);
  cJSON_AddStringToObject(pRoot, "heartbeatTimerLogicClockUser", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpHeartbeatTimer);
  cJSON_AddStringToObject(pRoot, "FpHeartbeatTimer", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%lu", pSyncNode->heartbeatTimerCounter);
  cJSON_AddStringToObject(pRoot, "heartbeatTimerCounter", u64buf);

  // callback
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnPing);
  cJSON_AddStringToObject(pRoot, "FpOnPing", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnPingReply);
  cJSON_AddStringToObject(pRoot, "FpOnPingReply", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnRequestVote);
  cJSON_AddStringToObject(pRoot, "FpOnRequestVote", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnRequestVoteReply);
  cJSON_AddStringToObject(pRoot, "FpOnRequestVoteReply", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnAppendEntries);
  cJSON_AddStringToObject(pRoot, "FpOnAppendEntries", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnAppendEntriesReply);
  cJSON_AddStringToObject(pRoot, "FpOnAppendEntriesReply", u64buf);
  snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnTimeout);
  cJSON_AddStringToObject(pRoot, "FpOnTimeout", u64buf);

  cJSON* pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncNode", pRoot);
  return pJson;
}

char* syncNode2Str(const SSyncNode* pSyncNode) {
  cJSON* pJson = syncNode2Json(pSyncNode);
  char*  serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

int32_t syncNodeSendMsgById(const SRaftId* destRaftId, SSyncNode* pSyncNode, SRpcMsg* pMsg) {
  SEpSet epSet;
  syncUtilraftId2EpSet(destRaftId, &epSet);
  pSyncNode->FpSendMsg(pSyncNode->rpcClient, &epSet, pMsg);
  return 0;
}

int32_t syncNodeSendMsgByInfo(const SNodeInfo* nodeInfo, SSyncNode* pSyncNode, SRpcMsg* pMsg) {
  SEpSet epSet;
  syncUtilnodeInfo2EpSet(nodeInfo, &epSet);
  pSyncNode->FpSendMsg(pSyncNode->rpcClient, &epSet, pMsg);
  return 0;
}

int32_t syncNodePing(SSyncNode* pSyncNode, const SRaftId* destRaftId, SyncPing* pMsg) {
  sTrace("syncNodePing pSyncNode:%p ", pSyncNode);
  int32_t ret = 0;

  SRpcMsg rpcMsg;
  syncPing2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(destRaftId, pSyncNode, &rpcMsg);

  {
    cJSON* pJson = syncPing2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    sTrace("syncNodePing pMsg:%s ", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  {
    SyncPing* pMsg2 = rpcMsg.pCont;
    cJSON*    pJson = syncPing2Json(pMsg2);
    char*     serialized = cJSON_Print(pJson);
    sTrace("syncNodePing rpcMsg.pCont:%s ", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  return ret;
}

int32_t syncNodePingAll(SSyncNode* pSyncNode) {
  sTrace("syncNodePingAll pSyncNode:%p ", pSyncNode);
  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->syncCfg.replicaNum; ++i) {
    SRaftId destId;
    syncUtilnodeInfo2raftId(&pSyncNode->syncCfg.nodeInfo[i], pSyncNode->vgId, &destId);
    SyncPing* pMsg = syncPingBuild3(&pSyncNode->myRaftId, &destId);
    ret = syncNodePing(pSyncNode, &destId, pMsg);
    assert(ret == 0);
    syncPingDestroy(pMsg);
  }
}

int32_t syncNodePingPeers(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    SRaftId destId;
    syncUtilnodeInfo2raftId(&pSyncNode->peersNodeInfo[i], pSyncNode->vgId, &destId);
    SyncPing* pMsg = syncPingBuild3(&pSyncNode->myRaftId, &destId);
    ret = syncNodePing(pSyncNode, &destId, pMsg);
    assert(ret == 0);
    syncPingDestroy(pMsg);
  }
}

int32_t syncNodePingSelf(SSyncNode* pSyncNode) {
  int32_t   ret;
  SyncPing* pMsg = syncPingBuild3(&pSyncNode->myRaftId, &pSyncNode->myRaftId);
  ret = syncNodePing(pSyncNode, &pMsg->destId, pMsg);
  assert(ret == 0);
  syncPingDestroy(pMsg);
}

int32_t syncNodeStartPingTimer(SSyncNode* pSyncNode) {
  atomic_store_64(&pSyncNode->pingTimerLogicClock, pSyncNode->pingTimerLogicClockUser);
  pSyncNode->pingTimerMS = PING_TIMER_MS;
  if (pSyncNode->pPingTimer == NULL) {
    pSyncNode->pPingTimer =
        taosTmrStart(pSyncNode->FpPingTimer, pSyncNode->pingTimerMS, pSyncNode, gSyncEnv->pTimerManager);
  } else {
    taosTmrReset(pSyncNode->FpPingTimer, pSyncNode->pingTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pPingTimer);
  }
  return 0;
}

int32_t syncNodeStopPingTimer(SSyncNode* pSyncNode) {
  atomic_add_fetch_64(&pSyncNode->pingTimerLogicClockUser, 1);
  pSyncNode->pingTimerMS = TIMER_MAX_MS;
  return 0;
}

int32_t syncNodeStartElectTimer(SSyncNode* pSyncNode, int32_t ms) {
  pSyncNode->electTimerMS = ms;
  atomic_store_64(&pSyncNode->electTimerLogicClock, pSyncNode->electTimerLogicClockUser);
  if (pSyncNode->pElectTimer == NULL) {
    pSyncNode->pElectTimer =
        taosTmrStart(pSyncNode->FpElectTimer, pSyncNode->electTimerMS, pSyncNode, gSyncEnv->pTimerManager);
  } else {
    taosTmrReset(pSyncNode->FpElectTimer, pSyncNode->electTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pElectTimer);
  }
  return 0;
}

int32_t syncNodeStopElectTimer(SSyncNode* pSyncNode) {
  atomic_add_fetch_64(&pSyncNode->electTimerLogicClockUser, 1);
  pSyncNode->electTimerMS = TIMER_MAX_MS;
  return 0;
}

int32_t syncNodeRestartElectTimer(SSyncNode* pSyncNode, int32_t ms) {
  syncNodeStopElectTimer(pSyncNode);
  syncNodeStartElectTimer(pSyncNode, ms);
  return 0;
}

int32_t syncNodeStartHeartbeatTimer(SSyncNode* pSyncNode) {
  atomic_store_64(&pSyncNode->heartbeatTimerLogicClock, pSyncNode->heartbeatTimerLogicClockUser);
  if (pSyncNode->pHeartbeatTimer == NULL) {
    pSyncNode->pHeartbeatTimer =
        taosTmrStart(pSyncNode->FpHeartbeatTimer, pSyncNode->heartbeatTimerMS, pSyncNode, gSyncEnv->pTimerManager);
  } else {
    taosTmrReset(pSyncNode->FpHeartbeatTimer, pSyncNode->heartbeatTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pHeartbeatTimer);
  }
  return 0;
}

int32_t syncNodeStopHeartbeatTimer(SSyncNode* pSyncNode) {
  atomic_add_fetch_64(&pSyncNode->heartbeatTimerLogicClockUser, 1);
  pSyncNode->heartbeatTimerMS = TIMER_MAX_MS;
  return 0;
}

// ------ local funciton ---------
static int32_t syncNodeOnPingCb(SSyncNode* ths, SyncPing* pMsg) {
  int32_t ret = 0;
  sTrace("<-- syncNodeOnPingCb -->");

  {
    cJSON* pJson = syncPing2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    sTrace("process syncMessage recv: syncNodeOnPingCb pMsg:%s ", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  SyncPingReply* pMsgReply = syncPingReplyBuild3(&ths->myRaftId, &pMsg->srcId);
  SRpcMsg        rpcMsg;
  syncPingReply2RpcMsg(pMsgReply, &rpcMsg);
  syncNodeSendMsgById(&pMsgReply->destId, ths, &rpcMsg);

  return ret;
}

static int32_t syncNodeOnPingReplyCb(SSyncNode* ths, SyncPingReply* pMsg) {
  int32_t ret = 0;
  sTrace("<-- syncNodeOnPingReplyCb -->");

  {
    cJSON* pJson = syncPingReply2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    sTrace("process syncMessage recv: syncNodeOnPingReplyCb pMsg:%s ", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  return ret;
}

static void syncNodeEqPingTimer(void* param, void* tmrId) {
  SSyncNode* pSyncNode = (SSyncNode*)param;
  if (atomic_load_64(&pSyncNode->pingTimerLogicClockUser) <= atomic_load_64(&pSyncNode->pingTimerLogicClock)) {
    SyncTimeout* pSyncMsg = syncTimeoutBuild2(SYNC_TIMEOUT_PING, atomic_load_64(&pSyncNode->pingTimerLogicClock),
                                              pSyncNode->pingTimerMS, pSyncNode);
    SRpcMsg      rpcMsg;
    syncTimeout2RpcMsg(pSyncMsg, &rpcMsg);
    pSyncNode->FpEqMsg(pSyncNode->queue, &rpcMsg);
    syncTimeoutDestroy(pSyncMsg);

    // reset timer ms
    // pSyncNode->pingTimerMS += 100;

    taosTmrReset(syncNodeEqPingTimer, pSyncNode->pingTimerMS, pSyncNode, &gSyncEnv->pTimerManager,
                 &pSyncNode->pPingTimer);
  } else {
    sTrace("syncNodeEqPingTimer: pingTimerLogicClock:%lu, pingTimerLogicClockUser:%lu", pSyncNode->pingTimerLogicClock,
           pSyncNode->pingTimerLogicClockUser);
  }
}

static void syncNodeEqElectTimer(void* param, void* tmrId) {
  SSyncNode* pSyncNode = (SSyncNode*)param;
  if (atomic_load_64(&pSyncNode->electTimerLogicClockUser) <= atomic_load_64(&pSyncNode->electTimerLogicClock)) {
    SyncTimeout* pSyncMsg = syncTimeoutBuild2(SYNC_TIMEOUT_ELECTION, atomic_load_64(&pSyncNode->electTimerLogicClock),
                                              pSyncNode->electTimerMS, pSyncNode);

    SRpcMsg rpcMsg;
    syncTimeout2RpcMsg(pSyncMsg, &rpcMsg);
    pSyncNode->FpEqMsg(pSyncNode->queue, &rpcMsg);
    syncTimeoutDestroy(pSyncMsg);

    // reset timer ms
    pSyncNode->electTimerMS = syncUtilElectRandomMS();

    taosTmrReset(syncNodeEqPingTimer, pSyncNode->pingTimerMS, pSyncNode, &gSyncEnv->pTimerManager,
                 &pSyncNode->pPingTimer);
  } else {
    sTrace("syncNodeEqElectTimer: electTimerLogicClock:%lu, electTimerLogicClockUser:%lu",
           pSyncNode->electTimerLogicClock, pSyncNode->electTimerLogicClockUser);
  }
}

static void syncNodeEqHeartbeatTimer(void* param, void* tmrId) {
  SSyncNode* pSyncNode = (SSyncNode*)param;
  if (atomic_load_64(&pSyncNode->heartbeatTimerLogicClockUser) <=
      atomic_load_64(&pSyncNode->heartbeatTimerLogicClock)) {
    SyncTimeout* pSyncMsg =
        syncTimeoutBuild2(SYNC_TIMEOUT_HEARTBEAT, atomic_load_64(&pSyncNode->heartbeatTimerLogicClock),
                          pSyncNode->heartbeatTimerMS, pSyncNode);

    SRpcMsg rpcMsg;
    syncTimeout2RpcMsg(pSyncMsg, &rpcMsg);
    pSyncNode->FpEqMsg(pSyncNode->queue, &rpcMsg);
    syncTimeoutDestroy(pSyncMsg);

    // reset timer ms
    // pSyncNode->heartbeatTimerMS += 100;

    taosTmrReset(syncNodeEqHeartbeatTimer, pSyncNode->heartbeatTimerMS, pSyncNode, &gSyncEnv->pTimerManager,
                 &pSyncNode->pHeartbeatTimer);
  } else {
    sTrace("syncNodeEqHeartbeatTimer: heartbeatTimerLogicClock:%lu, heartbeatTimerLogicClockUser:%lu",
           pSyncNode->heartbeatTimerLogicClock, pSyncNode->heartbeatTimerLogicClockUser);
  }
}

static void UpdateTerm(SSyncNode* pSyncNode, SyncTerm term) {
  if (term > pSyncNode->pRaftStore->currentTerm) {
    pSyncNode->pRaftStore->currentTerm = term;
    pSyncNode->pRaftStore->voteFor = EMPTY_RAFT_ID;
    raftStorePersist(pSyncNode->pRaftStore);
    syncNodeBecomeFollower(pSyncNode);
  }
}

static void syncNodeBecomeFollower(SSyncNode* pSyncNode) {
  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    pSyncNode->leaderCache = EMPTY_RAFT_ID;
  }

  syncNodeStopHeartbeatTimer(pSyncNode);
  int32_t electMS = syncUtilElectRandomMS();
  syncNodeStartElectTimer(pSyncNode, electMS);
}

// TLA+ Spec
// \* Candidate i transitions to leader.
// BecomeLeader(i) ==
//     /\ state[i] = Candidate
//     /\ votesGranted[i] \in Quorum
//     /\ state'      = [state EXCEPT ![i] = Leader]
//     /\ nextIndex'  = [nextIndex EXCEPT ![i] =
//                          [j \in Server |-> Len(log[i]) + 1]]
//     /\ matchIndex' = [matchIndex EXCEPT ![i] =
//                          [j \in Server |-> 0]]
//     /\ elections'  = elections \cup
//                          {[eterm     |-> currentTerm[i],
//                            eleader   |-> i,
//                            elog      |-> log[i],
//                            evotes    |-> votesGranted[i],
//                            evoterLog |-> voterLog[i]]}
//     /\ UNCHANGED <<messages, currentTerm, votedFor, candidateVars, logVars>>
//
static void syncNodeBecomeLeader(SSyncNode* pSyncNode) {
  pSyncNode->state = TAOS_SYNC_STATE_LEADER;
  pSyncNode->leaderCache = pSyncNode->myRaftId;

  // next Index +=1
  // match Index = 0;

  syncNodeStopElectTimer(pSyncNode);
  syncNodeStartHeartbeatTimer(pSyncNode);

  // appendEntries;
}

static void syncNodeFollower2Candidate(SSyncNode* pSyncNode) {
  assert(pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER);
  pSyncNode->state = TAOS_SYNC_STATE_CANDIDATE;
}

static void syncNodeCandidate2Leader(SSyncNode* pSyncNode) {}

static void syncNodeLeader2Follower(SSyncNode* pSyncNode) {}

static void syncNodeCandidate2Follower(SSyncNode* pSyncNode) {}
