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
static int32_t doSyncNodeSendMsgById(SRaftId* destRaftId, struct SSyncNode* pSyncNode, SRpcMsg* pMsg);
static int32_t doSyncNodeSendMsgByInfo(SNodeInfo* nodeInfo, struct SSyncNode* pSyncNode, SRpcMsg* pMsg);

static int32_t doSyncNodePing(struct SSyncNode* ths, const SyncPing* pMsg);
static int32_t onSyncNodePing(struct SSyncNode* ths, SyncPing* pMsg);
static int32_t onSyncNodePingReply(struct SSyncNode* ths, SyncPingReply* pMsg);
static int32_t doSyncNodeRequestVote(struct SSyncNode* ths, const SyncRequestVote* pMsg);
static int32_t onSyncNodeRequestVote(struct SSyncNode* ths, SyncRequestVote* pMsg);
static int32_t onSyncNodeRequestVoteReply(struct SSyncNode* ths, SyncRequestVoteReply* pMsg);
static int32_t doSyncNodeAppendEntries(struct SSyncNode* ths, const SyncAppendEntries* pMsg);
static int32_t onSyncNodeAppendEntries(struct SSyncNode* ths, SyncAppendEntries* pMsg);
static int32_t onSyncNodeAppendEntriesReply(struct SSyncNode* ths, SyncAppendEntriesReply* pMsg);
static void    syncNodePingTimerCb(void* param, void* tmrId);
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

int32_t syncForwardToPeer(int64_t rid, const SSyncBuffer* pBuf, bool isWeak) { return 0; }

ESyncState syncGetMyRole(int64_t rid) { return TAOS_SYNC_STATE_LEADER; }

void syncGetNodesRole(int64_t rid, SNodesRole* pNodeRole) {}

SSyncNode* syncNodeOpen(const SSyncInfo* pSyncInfo) {
  SSyncNode* pSyncNode = (SSyncNode*)malloc(sizeof(SSyncNode));
  assert(pSyncNode != NULL);
  memset(pSyncNode, 0, sizeof(SSyncNode));

  pSyncNode->pPingTimer = NULL;
  pSyncNode->pingTimerMS = 1000;
  atomic_store_8(&pSyncNode->pingTimerStart, 0);
  pSyncNode->FpPingTimer = syncNodePingTimerCb;
  pSyncNode->pingTimerCounter = 0;

  pSyncNode->rpcClient = pSyncInfo->rpcClient;
  pSyncNode->FpSendMsg = pSyncInfo->FpSendMsg;

  pSyncNode->FpOnPing = onSyncNodePing;
  pSyncNode->FpOnPingReply = onSyncNodePingReply;
  pSyncNode->FpOnRequestVote = onSyncNodeRequestVote;
  pSyncNode->FpOnRequestVoteReply = onSyncNodeRequestVoteReply;
  pSyncNode->FpOnAppendEntries = onSyncNodeAppendEntries;
  pSyncNode->FpOnAppendEntriesReply = onSyncNodeAppendEntriesReply;

  return pSyncNode;
}

void syncNodeClose(SSyncNode* pSyncNode) {
  assert(pSyncNode != NULL);
  free(pSyncNode);
}

void syncNodePingAll(SSyncNode* pSyncNode) {
  sTrace("syncNodePingAll %p ", pSyncNode);
  SyncPing msg;
  doSyncNodePing(pSyncNode, &msg);
}

void syncNodePingPeers(SSyncNode* pSyncNode) {}

void syncNodePingSelf(SSyncNode* pSyncNode) {}

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

static int32_t doSyncNodeSendMsgById(SRaftId* destRaftId, struct SSyncNode* pSyncNode, SRpcMsg* pMsg) {
  SEpSet epSet;
  raftId2EpSet(destRaftId, &epSet);
  pSyncNode->FpSendMsg(pSyncNode->rpcClient, &epSet, pMsg);
  return 0;
}

static int32_t doSyncNodeSendMsgByInfo(SNodeInfo* nodeInfo, struct SSyncNode* pSyncNode, SRpcMsg* pMsg) {
  SEpSet epSet;
  nodeInfo2EpSet(nodeInfo, &epSet);

  pSyncNode->FpSendMsg(pSyncNode->rpcClient, &epSet, pMsg);
  return 0;
}

static int32_t doSyncNodePing(struct SSyncNode* ths, const SyncPing* pMsg) {
  int32_t ret;
  for (int i = 0; i < ths->syncCfg.replicaNum; ++i) {
    SRpcMsg* rpcMsg;
    syncPing2RpcMsg(pMsg, rpcMsg);
    doSyncNodeSendMsgByInfo(&ths->syncCfg.nodeInfo[i], ths, rpcMsg);
  }

  return ret;
}

static int32_t onSyncNodePing(struct SSyncNode* ths, SyncPing* pMsg) {
  int32_t ret = 0;
  return ret;
}

static int32_t onSyncNodePingReply(struct SSyncNode* ths, SyncPingReply* pMsg) {
  int32_t ret = 0;
  return ret;
}

static int32_t doSyncNodeRequestVote(struct SSyncNode* ths, const SyncRequestVote* pMsg) {
  int32_t ret = 0;
  return ret;
}

static int32_t onSyncNodeRequestVote(struct SSyncNode* ths, SyncRequestVote* pMsg) {
  int32_t ret = 0;
  return ret;
}

static int32_t onSyncNodeRequestVoteReply(struct SSyncNode* ths, SyncRequestVoteReply* pMsg) {
  int32_t ret = 0;
  return ret;
}

static int32_t doSyncNodeAppendEntries(struct SSyncNode* ths, const SyncAppendEntries* pMsg) {
  int32_t ret = 0;
  return ret;
}

static int32_t onSyncNodeAppendEntries(struct SSyncNode* ths, SyncAppendEntries* pMsg) {
  int32_t ret = 0;
  return ret;
}

static int32_t onSyncNodeAppendEntriesReply(struct SSyncNode* ths, SyncAppendEntriesReply* pMsg) {
  int32_t ret = 0;
  return ret;
}

static void syncNodePingTimerCb(void* param, void* tmrId) {
  SSyncNode* pSyncNode = (SSyncNode*)param;
  if (atomic_load_8(&pSyncNode->pingTimerStart)) {
    ++(pSyncNode->pingTimerCounter);
    // pSyncNode->pingTimerMS += 100;

    sTrace("pSyncNode->pingTimerCounter:%lu, pSyncNode->pingTimerMS:%d, pSyncNode->pPingTimer:%p, tmrId:%p ",
           pSyncNode->pingTimerCounter, pSyncNode->pingTimerMS, pSyncNode->pPingTimer, tmrId);

    taosTmrReset(syncNodePingTimerCb, pSyncNode->pingTimerMS, pSyncNode, &gSyncEnv->pTimerManager,
                 &pSyncNode->pPingTimer);

    syncNodePingAll(pSyncNode);
  }
}