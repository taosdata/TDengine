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
#include "syncCommit.h"
#include "syncElection.h"
#include "syncEnv.h"
#include "syncIndexMgr.h"
#include "syncInt.h"
#include "syncMessage.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncReplication.h"
#include "syncRequestVote.h"
#include "syncRequestVoteReply.h"
#include "syncTimeout.h"
#include "syncUtil.h"
#include "syncVoteMgr.h"
#include "tref.h"

static int32_t tsNodeRefId = -1;

// ------ local funciton ---------
// enqueue message ----
static void syncNodeEqPingTimer(void* param, void* tmrId);
static void syncNodeEqElectTimer(void* param, void* tmrId);
static void syncNodeEqHeartbeatTimer(void* param, void* tmrId);

// on message ----
static int32_t syncNodeOnPingCb(SSyncNode* ths, SyncPing* pMsg);
static int32_t syncNodeOnPingReplyCb(SSyncNode* ths, SyncPingReply* pMsg);
static int32_t syncNodeOnClientRequestCb(SSyncNode* ths, SyncClientRequest* pMsg);

// life cycle
static void syncFreeNode(void* param);
// ---------------------------------

int32_t syncInit() {
  int32_t ret;
  tsNodeRefId = taosOpenRef(200, syncFreeNode);
  if (tsNodeRefId < 0) {
    sError("failed to init node ref");
    syncCleanUp();
    ret = -1;
  } else {
    ret = syncEnvStart();
  }

  return ret;
}

void syncCleanUp() {
  int32_t ret = syncEnvStop();
  assert(ret == 0);

  if (tsNodeRefId != -1) {
    taosCloseRef(tsNodeRefId);
    tsNodeRefId = -1;
  }
}

int64_t syncStart(const SSyncInfo* pSyncInfo) {
  SSyncNode* pSyncNode = syncNodeOpen(pSyncInfo);
  assert(pSyncNode != NULL);

  pSyncNode->rid = taosAddRef(tsNodeRefId, pSyncNode);
  if (pSyncNode->rid < 0) {
    syncFreeNode(pSyncNode);
    return -1;
  }

  return pSyncNode->rid;
}

void syncStop(int64_t rid) {
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return;
  }
  syncNodeClose(pSyncNode);

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  taosRemoveRef(tsNodeRefId, rid);
}

int32_t syncReconfig(int64_t rid, const SSyncCfg* pSyncCfg) {
  int32_t ret = 0;
  return ret;
}

int32_t syncPropose(int64_t rid, const SRpcMsg* pMsg, bool isWeak) {
  int32_t ret = 0;

  // todo : get pointer from rid
  SSyncNode* pSyncNode = (SSyncNode*)taosAcquireRef(tsNodeRefId, rid);
  if (pSyncNode == NULL) {
    return -1;
  }
  assert(rid == pSyncNode->rid);

  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    SyncClientRequest* pSyncMsg = syncClientRequestBuild2(pMsg, 0, isWeak);
    SRpcMsg            rpcMsg;
    syncClientRequest2RpcMsg(pSyncMsg, &rpcMsg);
    pSyncNode->FpEqMsg(pSyncNode->queue, &rpcMsg);
    syncClientRequestDestroy(pSyncMsg);
    ret = 0;

  } else {
    sTrace("syncForwardToPeer not leader, %s", syncUtilState2String(pSyncNode->state));
    ret = -1;  // todo : need define err code !!
  }

  taosReleaseRef(tsNodeRefId, pSyncNode->rid);
  return ret;
}

int32_t syncForwardToPeer(int64_t rid, const SRpcMsg* pMsg, bool isWeak) {
  int32_t ret = syncPropose(rid, pMsg, isWeak);
  return ret;
}

ESyncState syncGetMyRole(int64_t rid) {
  // todo : get pointer from rid
  SSyncNode* pSyncNode = NULL;
  return pSyncNode->state;
}

void syncGetNodesRole(int64_t rid, SNodesRole* pNodeRole) {}

// open/close --------------
SSyncNode* syncNodeOpen(const SSyncInfo* pSyncInfo) {
  SSyncNode* pSyncNode = (SSyncNode*)malloc(sizeof(SSyncNode));
  assert(pSyncNode != NULL);
  memset(pSyncNode, 0, sizeof(SSyncNode));

  if (taosMkDir(pSyncInfo->path) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    sError("failed to create dir:%s since %s", pSyncInfo->path, terrstr());
    return NULL;
  }

  // init by SSyncInfo
  pSyncNode->vgId = pSyncInfo->vgId;
  pSyncNode->syncCfg = pSyncInfo->syncCfg;
  memcpy(pSyncNode->path, pSyncInfo->path, sizeof(pSyncNode->path));
  snprintf(pSyncNode->raftStorePath, sizeof(pSyncNode->raftStorePath), "%s/raft_store.json", pSyncInfo->path);
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

  // init life cycle outside

  // TLA+ Spec
  // InitHistoryVars == /\ elections = {}
  //                    /\ allLogs   = {}
  //                    /\ voterLog  = [i \in Server |-> [j \in {} |-> <<>>]]
  // InitServerVars == /\ currentTerm = [i \in Server |-> 1]
  //                   /\ state       = [i \in Server |-> Follower]
  //                   /\ votedFor    = [i \in Server |-> Nil]
  // InitCandidateVars == /\ votesResponded = [i \in Server |-> {}]
  //                      /\ votesGranted   = [i \in Server |-> {}]
  // \* The values nextIndex[i][i] and matchIndex[i][i] are never read, since the
  // \* leader does not send itself messages. It's still easier to include these
  // \* in the functions.
  // InitLeaderVars == /\ nextIndex  = [i \in Server |-> [j \in Server |-> 1]]
  //                   /\ matchIndex = [i \in Server |-> [j \in Server |-> 0]]
  // InitLogVars == /\ log          = [i \in Server |-> << >>]
  //                /\ commitIndex  = [i \in Server |-> 0]
  // Init == /\ messages = [m \in {} |-> 0]
  //         /\ InitHistoryVars
  //         /\ InitServerVars
  //         /\ InitCandidateVars
  //         /\ InitLeaderVars
  //         /\ InitLogVars
  //

  // init TLA+ server vars
  pSyncNode->state = TAOS_SYNC_STATE_FOLLOWER;
  pSyncNode->pRaftStore = raftStoreOpen(pSyncNode->raftStorePath);
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
  pSyncNode->commitIndex = SYNC_INDEX_INVALID;

  // timer ms init
  pSyncNode->pingBaseLine = PING_TIMER_MS;
  pSyncNode->electBaseLine = ELECT_TIMER_MS_MIN;
  pSyncNode->hbBaseLine = HEARTBEAT_TIMER_MS;

  // init ping timer
  pSyncNode->pPingTimer = NULL;
  pSyncNode->pingTimerMS = pSyncNode->pingBaseLine;
  atomic_store_64(&pSyncNode->pingTimerLogicClock, 0);
  atomic_store_64(&pSyncNode->pingTimerLogicClockUser, 0);
  pSyncNode->FpPingTimerCB = syncNodeEqPingTimer;
  pSyncNode->pingTimerCounter = 0;

  // init elect timer
  pSyncNode->pElectTimer = NULL;
  pSyncNode->electTimerMS = syncUtilElectRandomMS(pSyncNode->electBaseLine, 2 * pSyncNode->electBaseLine);
  atomic_store_64(&pSyncNode->electTimerLogicClock, 0);
  atomic_store_64(&pSyncNode->electTimerLogicClockUser, 0);
  pSyncNode->FpElectTimerCB = syncNodeEqElectTimer;
  pSyncNode->electTimerCounter = 0;

  // init heartbeat timer
  pSyncNode->pHeartbeatTimer = NULL;
  pSyncNode->heartbeatTimerMS = pSyncNode->hbBaseLine;
  atomic_store_64(&pSyncNode->heartbeatTimerLogicClock, 0);
  atomic_store_64(&pSyncNode->heartbeatTimerLogicClockUser, 0);
  pSyncNode->FpHeartbeatTimerCB = syncNodeEqHeartbeatTimer;
  pSyncNode->heartbeatTimerCounter = 0;

  // init callback
  pSyncNode->FpOnPing = syncNodeOnPingCb;
  pSyncNode->FpOnPingReply = syncNodeOnPingReplyCb;
  pSyncNode->FpOnClientRequest = syncNodeOnClientRequestCb;
  pSyncNode->FpOnRequestVote = syncNodeOnRequestVoteCb;
  pSyncNode->FpOnRequestVoteReply = syncNodeOnRequestVoteReplyCb;
  pSyncNode->FpOnAppendEntries = syncNodeOnAppendEntriesCb;
  pSyncNode->FpOnAppendEntriesReply = syncNodeOnAppendEntriesReplyCb;
  pSyncNode->FpOnTimeout = syncNodeOnTimeoutCb;

  // start raft
  syncNodeBecomeFollower(pSyncNode);

  return pSyncNode;
}

void syncNodeClose(SSyncNode* pSyncNode) {
  int32_t ret;
  assert(pSyncNode != NULL);

  ret = raftStoreClose(pSyncNode->pRaftStore);
  assert(ret == 0);

  voteGrantedDestroy(pSyncNode->pVotesGranted);
  votesRespondDestory(pSyncNode->pVotesRespond);
  syncIndexMgrDestroy(pSyncNode->pNextIndex);
  syncIndexMgrDestroy(pSyncNode->pMatchIndex);
  logStoreDestory(pSyncNode->pLogStore);

  syncNodeStopPingTimer(pSyncNode);
  syncNodeStopElectTimer(pSyncNode);
  syncNodeStopHeartbeatTimer(pSyncNode);

  free(pSyncNode);
}

// ping --------------
int32_t syncNodePing(SSyncNode* pSyncNode, const SRaftId* destRaftId, SyncPing* pMsg) {
  syncPingLog2((char*)"==syncNodePing==", pMsg);
  int32_t ret = 0;

  SRpcMsg rpcMsg;
  syncPing2RpcMsg(pMsg, &rpcMsg);
  syncRpcMsgLog2((char*)"==syncNodePing==", &rpcMsg);

  ret = syncNodeSendMsgById(destRaftId, pSyncNode, &rpcMsg);
  return ret;
}

int32_t syncNodePingSelf(SSyncNode* pSyncNode) {
  int32_t   ret = 0;
  SyncPing* pMsg = syncPingBuild3(&pSyncNode->myRaftId, &pSyncNode->myRaftId);
  ret = syncNodePing(pSyncNode, &pMsg->destId, pMsg);
  assert(ret == 0);

  syncPingDestroy(pMsg);
  return ret;
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
  return ret;
}

int32_t syncNodePingAll(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->syncCfg.replicaNum; ++i) {
    SRaftId destId;
    syncUtilnodeInfo2raftId(&pSyncNode->syncCfg.nodeInfo[i], pSyncNode->vgId, &destId);
    SyncPing* pMsg = syncPingBuild3(&pSyncNode->myRaftId, &destId);
    ret = syncNodePing(pSyncNode, &destId, pMsg);
    assert(ret == 0);
    syncPingDestroy(pMsg);
  }
  return ret;
}

// timer control --------------
int32_t syncNodeStartPingTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  taosTmrReset(pSyncNode->FpPingTimerCB, pSyncNode->pingTimerMS, pSyncNode, gSyncEnv->pTimerManager,
               &pSyncNode->pPingTimer);
  atomic_store_64(&pSyncNode->pingTimerLogicClock, pSyncNode->pingTimerLogicClockUser);
  return ret;
}

int32_t syncNodeStopPingTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  atomic_add_fetch_64(&pSyncNode->pingTimerLogicClockUser, 1);
  taosTmrStop(pSyncNode->pPingTimer);
  pSyncNode->pPingTimer = NULL;
  return ret;
}

int32_t syncNodeStartElectTimer(SSyncNode* pSyncNode, int32_t ms) {
  int32_t ret = 0;
  pSyncNode->electTimerMS = ms;
  taosTmrReset(pSyncNode->FpElectTimerCB, pSyncNode->electTimerMS, pSyncNode, gSyncEnv->pTimerManager,
               &pSyncNode->pElectTimer);
  atomic_store_64(&pSyncNode->electTimerLogicClock, pSyncNode->electTimerLogicClockUser);
  return ret;
}

int32_t syncNodeStopElectTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  atomic_add_fetch_64(&pSyncNode->electTimerLogicClockUser, 1);
  taosTmrStop(pSyncNode->pElectTimer);
  pSyncNode->pElectTimer = NULL;
  return ret;
}

int32_t syncNodeRestartElectTimer(SSyncNode* pSyncNode, int32_t ms) {
  int32_t ret = 0;
  syncNodeStopElectTimer(pSyncNode);
  syncNodeStartElectTimer(pSyncNode, ms);
  return ret;
}

int32_t syncNodeResetElectTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  int32_t electMS = syncUtilElectRandomMS(pSyncNode->electBaseLine, 2 * pSyncNode->electBaseLine);
  ret = syncNodeRestartElectTimer(pSyncNode, electMS);
  return ret;
}

int32_t syncNodeStartHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  taosTmrReset(pSyncNode->FpHeartbeatTimerCB, pSyncNode->heartbeatTimerMS, pSyncNode, gSyncEnv->pTimerManager,
               &pSyncNode->pHeartbeatTimer);
  atomic_store_64(&pSyncNode->heartbeatTimerLogicClock, pSyncNode->heartbeatTimerLogicClockUser);
  return ret;
}

int32_t syncNodeStopHeartbeatTimer(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  atomic_add_fetch_64(&pSyncNode->heartbeatTimerLogicClockUser, 1);
  taosTmrStop(pSyncNode->pHeartbeatTimer);
  pSyncNode->pHeartbeatTimer = NULL;
  return ret;
}

// utils --------------
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

cJSON* syncNode2Json(const SSyncNode* pSyncNode) {
  char   u64buf[128];
  cJSON* pRoot = cJSON_CreateObject();

  if (pSyncNode != NULL) {
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

    // life cycle
    snprintf(u64buf, sizeof(u64buf), "%ld", pSyncNode->rid);
    cJSON_AddStringToObject(pRoot, "rid", u64buf);

    // tla+ server vars
    cJSON_AddNumberToObject(pRoot, "state", pSyncNode->state);
    cJSON_AddStringToObject(pRoot, "state_str", syncUtilState2String(pSyncNode->state));
    char tmpBuf[RAFT_STORE_BLOCK_SIZE];
    raftStoreSerialize(pSyncNode->pRaftStore, tmpBuf, sizeof(tmpBuf));
    cJSON_AddStringToObject(pRoot, "pRaftStore", tmpBuf);

    // tla+ candidate vars
    cJSON_AddItemToObject(pRoot, "pVotesGranted", voteGranted2Json(pSyncNode->pVotesGranted));
    cJSON_AddItemToObject(pRoot, "pVotesRespond", votesRespond2Json(pSyncNode->pVotesRespond));

    // tla+ leader vars
    cJSON_AddItemToObject(pRoot, "pNextIndex", syncIndexMgr2Json(pSyncNode->pNextIndex));
    cJSON_AddItemToObject(pRoot, "pMatchIndex", syncIndexMgr2Json(pSyncNode->pMatchIndex));

    // tla+ log vars
    cJSON_AddItemToObject(pRoot, "pLogStore", logStore2Json(pSyncNode->pLogStore));
    snprintf(u64buf, sizeof(u64buf), "%" PRId64 "", pSyncNode->commitIndex);
    cJSON_AddStringToObject(pRoot, "commitIndex", u64buf);

    // ping timer
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pPingTimer);
    cJSON_AddStringToObject(pRoot, "pPingTimer", u64buf);
    cJSON_AddNumberToObject(pRoot, "pingTimerMS", pSyncNode->pingTimerMS);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->pingTimerLogicClock);
    cJSON_AddStringToObject(pRoot, "pingTimerLogicClock", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->pingTimerLogicClockUser);
    cJSON_AddStringToObject(pRoot, "pingTimerLogicClockUser", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpPingTimerCB);
    cJSON_AddStringToObject(pRoot, "FpPingTimerCB", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->pingTimerCounter);
    cJSON_AddStringToObject(pRoot, "pingTimerCounter", u64buf);

    // elect timer
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pElectTimer);
    cJSON_AddStringToObject(pRoot, "pElectTimer", u64buf);
    cJSON_AddNumberToObject(pRoot, "electTimerMS", pSyncNode->electTimerMS);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->electTimerLogicClock);
    cJSON_AddStringToObject(pRoot, "electTimerLogicClock", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->electTimerLogicClockUser);
    cJSON_AddStringToObject(pRoot, "electTimerLogicClockUser", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpElectTimerCB);
    cJSON_AddStringToObject(pRoot, "FpElectTimerCB", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->electTimerCounter);
    cJSON_AddStringToObject(pRoot, "electTimerCounter", u64buf);

    // heartbeat timer
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pHeartbeatTimer);
    cJSON_AddStringToObject(pRoot, "pHeartbeatTimer", u64buf);
    cJSON_AddNumberToObject(pRoot, "heartbeatTimerMS", pSyncNode->heartbeatTimerMS);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->heartbeatTimerLogicClock);
    cJSON_AddStringToObject(pRoot, "heartbeatTimerLogicClock", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->heartbeatTimerLogicClockUser);
    cJSON_AddStringToObject(pRoot, "heartbeatTimerLogicClockUser", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpHeartbeatTimerCB);
    cJSON_AddStringToObject(pRoot, "FpHeartbeatTimerCB", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64 "", pSyncNode->heartbeatTimerCounter);
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
  }

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

SSyncNode* syncNodeAcquire(int64_t rid) {
  SSyncNode* pNode = taosAcquireRef(tsNodeRefId, rid);
  if (pNode == NULL) {
    sTrace("failed to acquire node from refId:%" PRId64, rid);
  }

  return pNode;
}

void syncNodeRelease(SSyncNode* pNode) { taosReleaseRef(tsNodeRefId, pNode->rid); }

// raft state change --------------
void syncNodeUpdateTerm(SSyncNode* pSyncNode, SyncTerm term) {
  if (term > pSyncNode->pRaftStore->currentTerm) {
    raftStoreSetTerm(pSyncNode->pRaftStore, term);
    syncNodeBecomeFollower(pSyncNode);
    raftStoreClearVote(pSyncNode->pRaftStore);
  }
}

void syncNodeBecomeFollower(SSyncNode* pSyncNode) {
  // maybe clear leader cache
  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    pSyncNode->leaderCache = EMPTY_RAFT_ID;
  }

  // state change
  pSyncNode->state = TAOS_SYNC_STATE_FOLLOWER;
  syncNodeStopHeartbeatTimer(pSyncNode);

  // reset elect timer
  syncNodeResetElectTimer(pSyncNode);
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
void syncNodeBecomeLeader(SSyncNode* pSyncNode) {
  // state change
  pSyncNode->state = TAOS_SYNC_STATE_LEADER;

  // set leader cache
  pSyncNode->leaderCache = pSyncNode->myRaftId;

  for (int i = 0; i < pSyncNode->pNextIndex->replicaNum; ++i) {
    // maybe overwrite myself, no harm
    // just do it!
    pSyncNode->pNextIndex->index[i] = pSyncNode->pLogStore->getLastIndex(pSyncNode->pLogStore) + 1;
  }

  for (int i = 0; i < pSyncNode->pMatchIndex->replicaNum; ++i) {
    // maybe overwrite myself, no harm
    // just do it!
    pSyncNode->pMatchIndex->index[i] = SYNC_INDEX_INVALID;
  }

  // stop elect timer
  syncNodeStopElectTimer(pSyncNode);

  // start replicate right now!
  syncNodeReplicate(pSyncNode);

  // start heartbeat timer
  syncNodeStartHeartbeatTimer(pSyncNode);
}

void syncNodeCandidate2Leader(SSyncNode* pSyncNode) {
  assert(pSyncNode->state == TAOS_SYNC_STATE_CANDIDATE);
  assert(voteGrantedMajority(pSyncNode->pVotesGranted));
  syncNodeBecomeLeader(pSyncNode);
}

void syncNodeFollower2Candidate(SSyncNode* pSyncNode) {
  assert(pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER);
  pSyncNode->state = TAOS_SYNC_STATE_CANDIDATE;
}

void syncNodeLeader2Follower(SSyncNode* pSyncNode) {
  assert(pSyncNode->state == TAOS_SYNC_STATE_LEADER);
  syncNodeBecomeFollower(pSyncNode);
}

void syncNodeCandidate2Follower(SSyncNode* pSyncNode) {
  assert(pSyncNode->state == TAOS_SYNC_STATE_CANDIDATE);
  syncNodeBecomeFollower(pSyncNode);
}

// raft vote --------------

// just called by syncNodeVoteForSelf
// need assert
void syncNodeVoteForTerm(SSyncNode* pSyncNode, SyncTerm term, SRaftId* pRaftId) {
  assert(term == pSyncNode->pRaftStore->currentTerm);
  assert(!raftStoreHasVoted(pSyncNode->pRaftStore));

  raftStoreVote(pSyncNode->pRaftStore, pRaftId);
}

// simulate get vote from outside
void syncNodeVoteForSelf(SSyncNode* pSyncNode) {
  syncNodeVoteForTerm(pSyncNode, pSyncNode->pRaftStore->currentTerm, &(pSyncNode->myRaftId));

  SyncRequestVoteReply* pMsg = syncRequestVoteReplyBuild();
  pMsg->srcId = pSyncNode->myRaftId;
  pMsg->destId = pSyncNode->myRaftId;
  pMsg->term = pSyncNode->pRaftStore->currentTerm;
  pMsg->voteGranted = true;

  voteGrantedVote(pSyncNode->pVotesGranted, pMsg);
  votesRespondAdd(pSyncNode->pVotesRespond, pMsg);
  syncRequestVoteReplyDestroy(pMsg);
}

// for debug --------------
void syncNodePrint(SSyncNode* pObj) {
  char* serialized = syncNode2Str(pObj);
  printf("syncNodePrint | len:%lu | %s \n", strlen(serialized), serialized);
  fflush(NULL);
  free(serialized);
}

void syncNodePrint2(char* s, SSyncNode* pObj) {
  char* serialized = syncNode2Str(pObj);
  printf("syncNodePrint2 | len:%lu | %s | %s \n", strlen(serialized), s, serialized);
  fflush(NULL);
  free(serialized);
}

void syncNodeLog(SSyncNode* pObj) {
  char* serialized = syncNode2Str(pObj);
  sTrace("syncNodeLog | len:%lu | %s", strlen(serialized), serialized);
  free(serialized);
}

void syncNodeLog2(char* s, SSyncNode* pObj) {
  char* serialized = syncNode2Str(pObj);
  sTrace("syncNodeLog2 | len:%lu | %s | %s", strlen(serialized), s, serialized);
  free(serialized);
}

// ------ local funciton ---------
// enqueue message ----
static void syncNodeEqPingTimer(void* param, void* tmrId) {
  SSyncNode* pSyncNode = (SSyncNode*)param;
  if (atomic_load_64(&pSyncNode->pingTimerLogicClockUser) <= atomic_load_64(&pSyncNode->pingTimerLogicClock)) {
    SyncTimeout* pSyncMsg = syncTimeoutBuild2(SYNC_TIMEOUT_PING, atomic_load_64(&pSyncNode->pingTimerLogicClock),
                                              pSyncNode->pingTimerMS, pSyncNode);
    SRpcMsg      rpcMsg;
    syncTimeout2RpcMsg(pSyncMsg, &rpcMsg);
    syncRpcMsgLog2((char*)"==syncNodeEqPingTimer==", &rpcMsg);
    pSyncNode->FpEqMsg(pSyncNode->queue, &rpcMsg);
    syncTimeoutDestroy(pSyncMsg);

    taosTmrReset(syncNodeEqPingTimer, pSyncNode->pingTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pPingTimer);
  } else {
    sTrace("==syncNodeEqPingTimer== pingTimerLogicClock:%" PRIu64 ", pingTimerLogicClockUser:%" PRIu64 "",
           pSyncNode->pingTimerLogicClock, pSyncNode->pingTimerLogicClockUser);
  }
}

static void syncNodeEqElectTimer(void* param, void* tmrId) {
  SSyncNode* pSyncNode = (SSyncNode*)param;
  if (atomic_load_64(&pSyncNode->electTimerLogicClockUser) <= atomic_load_64(&pSyncNode->electTimerLogicClock)) {
    SyncTimeout* pSyncMsg = syncTimeoutBuild2(SYNC_TIMEOUT_ELECTION, atomic_load_64(&pSyncNode->electTimerLogicClock),
                                              pSyncNode->electTimerMS, pSyncNode);
    SRpcMsg      rpcMsg;
    syncTimeout2RpcMsg(pSyncMsg, &rpcMsg);
    syncRpcMsgLog2((char*)"==syncNodeEqElectTimer==", &rpcMsg);
    pSyncNode->FpEqMsg(pSyncNode->queue, &rpcMsg);
    syncTimeoutDestroy(pSyncMsg);

    // reset timer ms
    pSyncNode->electTimerMS = syncUtilElectRandomMS(pSyncNode->electBaseLine, 2 * pSyncNode->electBaseLine);
    taosTmrReset(syncNodeEqPingTimer, pSyncNode->pingTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pPingTimer);
  } else {
    sTrace("==syncNodeEqElectTimer== electTimerLogicClock:%" PRIu64 ", electTimerLogicClockUser:%" PRIu64 "",
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
    syncRpcMsgLog2((char*)"==syncNodeEqHeartbeatTimer==", &rpcMsg);
    pSyncNode->FpEqMsg(pSyncNode->queue, &rpcMsg);
    syncTimeoutDestroy(pSyncMsg);

    taosTmrReset(syncNodeEqHeartbeatTimer, pSyncNode->heartbeatTimerMS, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pHeartbeatTimer);
  } else {
    sTrace("==syncNodeEqHeartbeatTimer== heartbeatTimerLogicClock:%" PRIu64 ", heartbeatTimerLogicClockUser:%" PRIu64
           "",
           pSyncNode->heartbeatTimerLogicClock, pSyncNode->heartbeatTimerLogicClockUser);
  }
}

// on message ----
static int32_t syncNodeOnPingCb(SSyncNode* ths, SyncPing* pMsg) {
  int32_t ret = 0;
  syncPingLog2("==syncNodeOnPingCb==", pMsg);
  SyncPingReply* pMsgReply = syncPingReplyBuild3(&ths->myRaftId, &pMsg->srcId);
  SRpcMsg        rpcMsg;
  syncPingReply2RpcMsg(pMsgReply, &rpcMsg);
  syncNodeSendMsgById(&pMsgReply->destId, ths, &rpcMsg);

  return ret;
}

static int32_t syncNodeOnPingReplyCb(SSyncNode* ths, SyncPingReply* pMsg) {
  int32_t ret = 0;
  syncPingReplyLog2("==syncNodeOnPingReplyCb==", pMsg);
  return ret;
}

// TLA+ Spec
// ClientRequest(i, v) ==
//     /\ state[i] = Leader
//     /\ LET entry == [term  |-> currentTerm[i],
//                      value |-> v]
//            newLog == Append(log[i], entry)
//        IN  log' = [log EXCEPT ![i] = newLog]
//     /\ UNCHANGED <<messages, serverVars, candidateVars,
//                    leaderVars, commitIndex>>
//
static int32_t syncNodeOnClientRequestCb(SSyncNode* ths, SyncClientRequest* pMsg) {
  int32_t ret = 0;
  syncClientRequestLog2("==syncNodeOnClientRequestCb==", pMsg);

  SyncIndex       index = ths->pLogStore->getLastIndex(ths->pLogStore) + 1;
  SyncTerm        term = ths->pRaftStore->currentTerm;
  SSyncRaftEntry* pEntry = syncEntryBuild2((SyncClientRequest*)pMsg, term, index);
  assert(pEntry != NULL);

  if (ths->state == TAOS_SYNC_STATE_LEADER) {
    ths->pLogStore->appendEntry(ths->pLogStore, pEntry);

    // start replicate right now!
    syncNodeReplicate(ths);

    // pre commit
    SRpcMsg rpcMsg;
    syncEntry2OriginalRpc(pEntry, &rpcMsg);

    if (ths->pFsm != NULL) {
      if (ths->pFsm->FpPreCommitCb != NULL) {
        ths->pFsm->FpPreCommitCb(ths->pFsm, &rpcMsg, pEntry->index, pEntry->isWeak, 0);
      }
    }
    rpcFreeCont(rpcMsg.pCont);

    // only myself, maybe commit
    syncMaybeAdvanceCommitIndex(ths);

  } else {
    // pre commit
    SRpcMsg rpcMsg;
    syncEntry2OriginalRpc(pEntry, &rpcMsg);

    if (ths->pFsm != NULL) {
      if (ths->pFsm->FpPreCommitCb != NULL) {
        ths->pFsm->FpPreCommitCb(ths->pFsm, &rpcMsg, pEntry->index, pEntry->isWeak, -2);
      }
    }
    rpcFreeCont(rpcMsg.pCont);
  }

  syncEntryDestory(pEntry);
  return ret;
}

static void syncFreeNode(void* param) {
  SSyncNode* pNode = param;
  syncNodePrint2((char*)"==syncFreeNode==", pNode);

  free(pNode);
}