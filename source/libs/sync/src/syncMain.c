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
#include "syncInt.h"
#include "syncRaft.h"

int32_t syncInit() { return 0; }

void syncCleanUp() {}

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

  pSyncNode->FpSendMsg = pSyncInfo->FpSendMsg;

  pSyncNode->FpPing = doSyncNodePing;
  pSyncNode->FpOnPing = onSyncNodePing;
  pSyncNode->FpOnPingReply = onSyncNodePingReply;
  pSyncNode->FpRequestVote = doSyncNodeRequestVote;
  pSyncNode->FpOnRequestVote = onSyncNodeRequestVote;
  pSyncNode->FpOnRequestVoteReply = onSyncNodeRequestVoteReply;
  pSyncNode->FpAppendEntries = doSyncNodeAppendEntries;
  pSyncNode->FpOnAppendEntries = onSyncNodeAppendEntries;
  pSyncNode->FpOnAppendEntriesReply = onSyncNodeAppendEntriesReply;

  return pSyncNode;
}

void syncNodeClose(SSyncNode* pSyncNode) {
  assert(pSyncNode != NULL);
  raftClose(pSyncNode->pRaft);
  free(pSyncNode);
}

static int32_t doSyncNodePing(struct SSyncNode* ths, const SyncPing* pMsg) {
  int32_t ret = ths->pRaft->FpPing(ths->pRaft, pMsg);
  return ret;
}

static int32_t onSyncNodePing(struct SSyncNode* ths, SyncPing* pMsg) {
  int32_t ret = ths->pRaft->FpOnPing(ths->pRaft, pMsg);
  return ret;
}

static int32_t onSyncNodePingReply(struct SSyncNode* ths, SyncPingReply* pMsg) {
  int32_t ret = ths->pRaft->FpOnPingReply(ths->pRaft, pMsg);
  return ret;
}

static int32_t doSyncNodeRequestVote(struct SSyncNode* ths, const SyncRequestVote* pMsg) {
  int32_t ret = ths->pRaft->FpRequestVote(ths->pRaft, pMsg);
  return ret;
}

static int32_t onSyncNodeRequestVote(struct SSyncNode* ths, SyncRequestVote* pMsg) {
  int32_t ret = ths->pRaft->FpOnRequestVote(ths->pRaft, pMsg);
  return ret;
}

static int32_t onSyncNodeRequestVoteReply(struct SSyncNode* ths, SyncRequestVoteReply* pMsg) {
  int32_t ret = ths->pRaft->FpOnRequestVoteReply(ths->pRaft, pMsg);
  return ret;
}

static int32_t doSyncNodeAppendEntries(struct SSyncNode* ths, const SyncAppendEntries* pMsg) {
  int32_t ret = ths->pRaft->FpAppendEntries(ths->pRaft, pMsg);
  return ret;
}

static int32_t onSyncNodeAppendEntries(struct SSyncNode* ths, SyncAppendEntries* pMsg) {
  int32_t ret = ths->pRaft->FpOnAppendEntries(ths->pRaft, pMsg);
  return ret;
}

static int32_t onSyncNodeAppendEntriesReply(struct SSyncNode* ths, SyncAppendEntriesReply* pMsg) {
  int32_t ret = ths->pRaft->FpOnAppendEntriesReply(ths->pRaft, pMsg);
  return ret;
}