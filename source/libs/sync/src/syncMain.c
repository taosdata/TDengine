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
#include "syncEnv.h"
#include "syncInt.h"
#include "syncRaft.h"
#include "syncUtil.h"

static int32_t tsNodeRefId = -1;

// ------ local funciton ---------
static int32_t syncNodeSendMsgById(const SRaftId* destRaftId, SSyncNode* pSyncNode, SRpcMsg* pMsg);
static int32_t syncNodeSendMsgByInfo(const SNodeInfo* nodeInfo, SSyncNode* pSyncNode, SRpcMsg* pMsg);
static void    syncNodePingTimerCb(void* param, void* tmrId);

static int32_t syncNodePing(SSyncNode* pSyncNode, const SRaftId* destRaftId, SyncPing* pMsg);
static int32_t syncNodeRequestVote(SSyncNode* ths, const SyncRequestVote* pMsg);
static int32_t syncNodeAppendEntries(SSyncNode* ths, const SyncAppendEntries* pMsg);

static int32_t syncNodeOnPingCb(SSyncNode* ths, SyncPing* pMsg);
static int32_t syncNodeOnPingReplyCb(SSyncNode* ths, SyncPingReply* pMsg);
static int32_t syncNodeOnRequestVoteCb(SSyncNode* ths, SyncRequestVote* pMsg);
static int32_t syncNodeOnRequestVoteReplyCb(SSyncNode* ths, SyncRequestVoteReply* pMsg);
static int32_t syncNodeOnAppendEntriesCb(SSyncNode* ths, SyncAppendEntries* pMsg);
static int32_t syncNodeOnAppendEntriesReplyCb(SSyncNode* ths, SyncAppendEntriesReply* pMsg);
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

// int32_t syncForwardToPeer(int64_t rid, const SSyncBuffer* pBuf, bool isWeak) { return 0; }

int32_t syncForwardToPeer(int64_t rid, const SRpcMsg* pBuf, bool isWeak) { return 0; }

ESyncState syncGetMyRole(int64_t rid) { return TAOS_SYNC_STATE_LEADER; }

void syncGetNodesRole(int64_t rid, SNodesRole* pNodeRole) {}

SSyncNode* syncNodeOpen(const SSyncInfo* pSyncInfo) {
  SSyncNode* pSyncNode = (SSyncNode*)malloc(sizeof(SSyncNode));
  assert(pSyncNode != NULL);
  memset(pSyncNode, 0, sizeof(SSyncNode));

  pSyncNode->vgId = pSyncInfo->vgId;
  pSyncNode->syncCfg = pSyncInfo->syncCfg;
  memcpy(pSyncNode->path, pSyncInfo->path, sizeof(pSyncNode->path));
  pSyncNode->pFsm = pSyncInfo->pFsm;

  pSyncNode->rpcClient = pSyncInfo->rpcClient;
  pSyncNode->FpSendMsg = pSyncInfo->FpSendMsg;

  pSyncNode->me = pSyncInfo->syncCfg.nodeInfo[pSyncInfo->syncCfg.myIndex];
  pSyncNode->peersNum = pSyncInfo->syncCfg.replicaNum - 1;

  int j = 0;
  for (int i = 0; i < pSyncInfo->syncCfg.replicaNum; ++i) {
    if (i != pSyncInfo->syncCfg.myIndex) {
      pSyncNode->peers[j] = pSyncInfo->syncCfg.nodeInfo[i];
      j++;
    }
  }

  pSyncNode->role = TAOS_SYNC_STATE_FOLLOWER;
  syncUtilnodeInfo2raftId(&pSyncNode->me, pSyncNode->vgId, &pSyncNode->raftId);

  pSyncNode->pPingTimer = NULL;
  pSyncNode->pingTimerMS = 1000;
  atomic_store_8(&pSyncNode->pingTimerStart, 0);
  pSyncNode->FpPingTimer = syncNodePingTimerCb;
  pSyncNode->pingTimerCounter = 0;

  pSyncNode->FpOnPing = syncNodeOnPingCb;
  pSyncNode->FpOnPingReply = syncNodeOnPingReplyCb;
  pSyncNode->FpOnRequestVote = syncNodeOnRequestVoteCb;
  pSyncNode->FpOnRequestVoteReply = syncNodeOnRequestVoteReplyCb;
  pSyncNode->FpOnAppendEntries = syncNodeOnAppendEntriesCb;
  pSyncNode->FpOnAppendEntriesReply = syncNodeOnAppendEntriesReplyCb;

  return pSyncNode;
}

void syncNodeClose(SSyncNode* pSyncNode) {
  assert(pSyncNode != NULL);
  free(pSyncNode);
}

void syncNodePingAll(SSyncNode* pSyncNode) {
  sTrace("syncNodePingAll pSyncNode:%p ", pSyncNode);
  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->syncCfg.replicaNum; ++i) {
    SRaftId destId;
    syncUtilnodeInfo2raftId(&pSyncNode->syncCfg.nodeInfo[i], pSyncNode->vgId, &destId);
    SyncPing* pMsg = syncPingBuild3(&pSyncNode->raftId, &destId);
    ret = syncNodePing(pSyncNode, &destId, pMsg);
    assert(ret == 0);
    syncPingDestroy(pMsg);
  }
}

void syncNodePingPeers(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  for (int i = 0; i < pSyncNode->peersNum; ++i) {
    SRaftId destId;
    syncUtilnodeInfo2raftId(&pSyncNode->peers[i], pSyncNode->vgId, &destId);
    SyncPing* pMsg = syncPingBuild3(&pSyncNode->raftId, &destId);
    ret = syncNodePing(pSyncNode, &destId, pMsg);
    assert(ret == 0);
    syncPingDestroy(pMsg);
  }
}

void syncNodePingSelf(SSyncNode* pSyncNode) {
  int32_t   ret;
  SyncPing* pMsg = syncPingBuild3(&pSyncNode->raftId, &pSyncNode->raftId);
  ret = syncNodePing(pSyncNode, &pMsg->destId, pMsg);
  assert(ret == 0);
  syncPingDestroy(pMsg);
}

int32_t syncNodeStartPingTimer(SSyncNode* pSyncNode) {
  if (pSyncNode->pPingTimer == NULL) {
    pSyncNode->pPingTimer =
        taosTmrStart(pSyncNode->FpPingTimer, pSyncNode->pingTimerCounter, pSyncNode, gSyncEnv->pTimerManager);
  } else {
    taosTmrReset(pSyncNode->FpPingTimer, pSyncNode->pingTimerCounter, pSyncNode, gSyncEnv->pTimerManager,
                 &pSyncNode->pPingTimer);
  }

  atomic_store_8(&pSyncNode->pingTimerStart, 1);
  return 0;
}

int32_t syncNodeStopPingTimer(SSyncNode* pSyncNode) {
  atomic_store_8(&pSyncNode->pingTimerStart, 0);
  pSyncNode->pingTimerCounter = TIMER_MAX_MS;
  return 0;
}

// ------ local funciton ---------
static int32_t syncNodePing(SSyncNode* pSyncNode, const SRaftId* destRaftId, SyncPing* pMsg) {
  sTrace("syncNodePing pSyncNode:%p ", pSyncNode);
  int32_t ret = 0;

  SRpcMsg rpcMsg;
  syncPing2RpcMsg(pMsg, &rpcMsg);

  /*
    SRpcMsg rpcMsg;
    rpcMsg.contLen = 64;
    rpcMsg.pCont = rpcMallocCont(rpcMsg.contLen);
    snprintf((char*)rpcMsg.pCont, rpcMsg.contLen, "%s", "xxxxxxxxxxxxxx");
    rpcMsg.handle = NULL;
    rpcMsg.msgType = 1;
  */

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

static int32_t syncNodeRequestVote(SSyncNode* ths, const SyncRequestVote* pMsg) {
  int32_t ret = 0;
  return ret;
}

static int32_t syncNodeAppendEntries(SSyncNode* ths, const SyncAppendEntries* pMsg) {
  int32_t ret = 0;
  return ret;
}

static int32_t syncNodeSendMsgById(const SRaftId* destRaftId, SSyncNode* pSyncNode, SRpcMsg* pMsg) {
  sTrace("syncNodeSendMsgById pSyncNode:%p ", pSyncNode);
  SEpSet epSet;
  syncUtilraftId2EpSet(destRaftId, &epSet);
  pSyncNode->FpSendMsg(pSyncNode->rpcClient, &epSet, pMsg);
  return 0;
}

static int32_t syncNodeSendMsgByInfo(const SNodeInfo* nodeInfo, SSyncNode* pSyncNode, SRpcMsg* pMsg) {
  SEpSet epSet;
  syncUtilnodeInfo2EpSet(nodeInfo, &epSet);
  pSyncNode->FpSendMsg(pSyncNode->rpcClient, &epSet, pMsg);
  return 0;
}

static int32_t syncNodeOnPingCb(SSyncNode* ths, SyncPing* pMsg) {
  int32_t ret = 0;
  sTrace("<-- syncNodeOnPingCb -->");

  {
    cJSON* pJson = syncPing2Json(pMsg);
    char*  serialized = cJSON_Print(pJson);
    sTrace("syncNodeOnPingCb syncNodePing pMsg:%s ", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  SyncPingReply* pMsgReply = syncPingReplyBuild3(&ths->raftId, &pMsg->srcId);
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
    sTrace("syncNodeOnPingReplyCb syncNodePing pMsg:%s ", serialized);
    free(serialized);
    cJSON_Delete(pJson);
  }

  return ret;
}

static int32_t syncNodeOnRequestVoteCb(SSyncNode* ths, SyncRequestVote* pMsg) {
  int32_t ret = 0;
  return ret;
}

static int32_t syncNodeOnRequestVoteReplyCb(SSyncNode* ths, SyncRequestVoteReply* pMsg) {
  int32_t ret = 0;
  return ret;
}

static int32_t syncNodeOnAppendEntriesCb(SSyncNode* ths, SyncAppendEntries* pMsg) {
  int32_t ret = 0;
  return ret;
}

static int32_t syncNodeOnAppendEntriesReplyCb(SSyncNode* ths, SyncAppendEntriesReply* pMsg) {
  int32_t ret = 0;
  return ret;
}

static void syncNodePingTimerCb(void* param, void* tmrId) {
  SSyncNode* pSyncNode = (SSyncNode*)param;
  if (atomic_load_8(&pSyncNode->pingTimerStart)) {
    ++(pSyncNode->pingTimerCounter);
    // pSyncNode->pingTimerMS += 100;

    sTrace(
        "syncNodePingTimerCb: pSyncNode->pingTimerCounter:%lu, pSyncNode->pingTimerMS:%d, pSyncNode->pPingTimer:%p, "
        "tmrId:%p ",
        pSyncNode->pingTimerCounter, pSyncNode->pingTimerMS, pSyncNode->pPingTimer, tmrId);

    syncNodePingAll(pSyncNode);

    taosTmrReset(syncNodePingTimerCb, pSyncNode->pingTimerMS, pSyncNode, &gSyncEnv->pTimerManager,
                 &pSyncNode->pPingTimer);
  } else {
    sTrace("syncNodePingTimerCb: pingTimerStart:%u ", pSyncNode->pingTimerStart);
  }
}