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

#define _DEFAULT_SOURCE
#include "syncTest.h"

cJSON* syncNode2Json(const SSyncNode* pSyncNode) {
  char   u64buf[128] = {0};
  cJSON* pRoot = cJSON_CreateObject();

  if (pSyncNode != NULL) {
    // init by SSyncInfo
    cJSON_AddNumberToObject(pRoot, "vgId", pSyncNode->vgId);
    // cJSON_AddItemToObject(pRoot, "SRaftCfg", raftCfg2Json(pSyncNode->pRaftCfg));
    cJSON_AddStringToObject(pRoot, "path", pSyncNode->path);
    cJSON_AddStringToObject(pRoot, "raftStorePath", pSyncNode->raftStorePath);
    cJSON_AddStringToObject(pRoot, "configPath", pSyncNode->configPath);

    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pWal);
    cJSON_AddStringToObject(pRoot, "pWal", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->msgcb);
    cJSON_AddStringToObject(pRoot, "rpcClient", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->syncSendMSg);
    cJSON_AddStringToObject(pRoot, "syncSendMSg", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->msgcb);
    cJSON_AddStringToObject(pRoot, "queue", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->syncEqMsg);
    cJSON_AddStringToObject(pRoot, "syncEqMsg", u64buf);

    // init internal
    cJSON* pMe = syncUtilNodeInfo2Json(&pSyncNode->myNodeInfo);
    cJSON_AddItemToObject(pRoot, "myNodeInfo", pMe);
    // cJSON* pRaftId = syncUtilRaftId2Json(&pSyncNode->myRaftId);
    // cJSON_AddItemToObject(pRoot, "myRaftId", pRaftId);

    cJSON_AddNumberToObject(pRoot, "peersNum", pSyncNode->peersNum);
    cJSON* pPeers = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "peersNodeInfo", pPeers);
    for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
      cJSON_AddItemToArray(pPeers, syncUtilNodeInfo2Json(&pSyncNode->peersNodeInfo[i]));
    }
    cJSON* pPeersId = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "peersId", pPeersId);
    for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
      // cJSON_AddItemToArray(pPeersId, syncUtilRaftId2Json(&pSyncNode->peersId[i]));
    }

    cJSON_AddNumberToObject(pRoot, "replicaNum", pSyncNode->replicaNum);
    cJSON* pReplicasId = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "replicasId", pReplicasId);
    for (int32_t i = 0; i < pSyncNode->replicaNum; ++i) {
      // cJSON_AddItemToArray(pReplicasId, syncUtilRaftId2Json(&pSyncNode->replicasId[i]));
    }

    // raft algorithm
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pFsm);
    cJSON_AddStringToObject(pRoot, "pFsm", u64buf);
    cJSON_AddNumberToObject(pRoot, "quorum", pSyncNode->quorum);
    // cJSON* pLaderCache = syncUtilRaftId2Json(&pSyncNode->leaderCache);
    // cJSON_AddItemToObject(pRoot, "leaderCache", pLaderCache);

    // life cycle
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pSyncNode->rid);
    cJSON_AddStringToObject(pRoot, "rid", u64buf);

    // tla+ server vars
    cJSON_AddNumberToObject(pRoot, "state", pSyncNode->state);
    cJSON_AddStringToObject(pRoot, "state_str", syncStr(pSyncNode->state));
    // cJSON_AddItemToObject(pRoot, "pRaftStore", raftStore2Json(&pSyncNode.raftStore));

    // tla+ candidate vars
    cJSON_AddItemToObject(pRoot, "pVotesGranted", voteGranted2Json(pSyncNode->pVotesGranted));
    cJSON_AddItemToObject(pRoot, "pVotesRespond", votesRespond2Json(pSyncNode->pVotesRespond));

    // tla+ leader vars
    cJSON_AddItemToObject(pRoot, "pNextIndex", syncIndexMgr2Json(pSyncNode->pNextIndex));
    cJSON_AddItemToObject(pRoot, "pMatchIndex", syncIndexMgr2Json(pSyncNode->pMatchIndex));

    // tla+ log vars
    cJSON_AddItemToObject(pRoot, "pLogStore", logStore2Json(pSyncNode->pLogStore));
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pSyncNode->commitIndex);
    cJSON_AddStringToObject(pRoot, "commitIndex", u64buf);

    // timer ms init
    cJSON_AddNumberToObject(pRoot, "pingBaseLine", pSyncNode->pingBaseLine);
    cJSON_AddNumberToObject(pRoot, "electBaseLine", pSyncNode->electBaseLine);
    cJSON_AddNumberToObject(pRoot, "hbBaseLine", pSyncNode->hbBaseLine);

    // ping timer
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pPingTimer);
    cJSON_AddStringToObject(pRoot, "pPingTimer", u64buf);
    cJSON_AddNumberToObject(pRoot, "pingTimerMS", pSyncNode->pingTimerMS);
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pSyncNode->pingTimerLogicClock);
    cJSON_AddStringToObject(pRoot, "pingTimerLogicClock", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pSyncNode->pingTimerLogicClockUser);
    cJSON_AddStringToObject(pRoot, "pingTimerLogicClockUser", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpPingTimerCB);
    cJSON_AddStringToObject(pRoot, "FpPingTimerCB", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pSyncNode->pingTimerCounter);
    cJSON_AddStringToObject(pRoot, "pingTimerCounter", u64buf);

    // elect timer
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pElectTimer);
    cJSON_AddStringToObject(pRoot, "pElectTimer", u64buf);
    cJSON_AddNumberToObject(pRoot, "electTimerMS", pSyncNode->electTimerMS);
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pSyncNode->electTimerLogicClock);
    cJSON_AddStringToObject(pRoot, "electTimerLogicClock", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpElectTimerCB);
    cJSON_AddStringToObject(pRoot, "FpElectTimerCB", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pSyncNode->electTimerCounter);
    cJSON_AddStringToObject(pRoot, "electTimerCounter", u64buf);

    // heartbeat timer
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->pHeartbeatTimer);
    cJSON_AddStringToObject(pRoot, "pHeartbeatTimer", u64buf);
    cJSON_AddNumberToObject(pRoot, "heartbeatTimerMS", pSyncNode->heartbeatTimerMS);
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pSyncNode->heartbeatTimerLogicClock);
    cJSON_AddStringToObject(pRoot, "heartbeatTimerLogicClock", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pSyncNode->heartbeatTimerLogicClockUser);
    cJSON_AddStringToObject(pRoot, "heartbeatTimerLogicClockUser", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpHeartbeatTimerCB);
    cJSON_AddStringToObject(pRoot, "FpHeartbeatTimerCB", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pSyncNode->heartbeatTimerCounter);
    cJSON_AddStringToObject(pRoot, "heartbeatTimerCounter", u64buf);

    // callback
    // snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnPing);
    // cJSON_AddStringToObject(pRoot, "FpOnPing", u64buf);
    // snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnPingReply);
    // cJSON_AddStringToObject(pRoot, "FpOnPingReply", u64buf);
    // snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnRequestVote);
    // cJSON_AddStringToObject(pRoot, "FpOnRequestVote", u64buf);
    // snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnRequestVoteReply);
    // cJSON_AddStringToObject(pRoot, "FpOnRequestVoteReply", u64buf);
    // snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnAppendEntries);
    // cJSON_AddStringToObject(pRoot, "FpOnAppendEntries", u64buf);
    // snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnAppendEntriesReply);
    // cJSON_AddStringToObject(pRoot, "FpOnAppendEntriesReply", u64buf);
    // snprintf(u64buf, sizeof(u64buf), "%p", pSyncNode->FpOnTimeout);
    // cJSON_AddStringToObject(pRoot, "FpOnTimeout", u64buf);

    // restoreFinish
    cJSON_AddNumberToObject(pRoot, "restoreFinish", pSyncNode->restoreFinish);

    // snapshot senders
    cJSON* pSenders = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "senders", pSenders);
    for (int32_t i = 0; i < TSDB_MAX_REPLICA; ++i) {
      cJSON_AddItemToArray(pSenders, snapshotSender2Json((pSyncNode->senders)[i]));
    }

    // snapshot receivers
    cJSON* pReceivers = cJSON_CreateArray();
    cJSON_AddItemToObject(pRoot, "receiver", snapshotReceiver2Json(pSyncNode->pNewNodeReceiver));

    // changing
    cJSON_AddNumberToObject(pRoot, "changing", pSyncNode->changing);
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

inline char* syncNode2SimpleStr(const SSyncNode* pSyncNode) {
  int32_t len = 256;
  char*   s = (char*)taosMemoryMalloc(len);

  SSnapshot snapshot = {.data = NULL, .lastApplyIndex = -1, .lastApplyTerm = 0};
  if (pSyncNode->pFsm->FpGetSnapshotInfo != NULL) {
    pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);
  }
  SyncIndex logLastIndex = pSyncNode->pLogStore->syncLogLastIndex(pSyncNode->pLogStore);
  SyncIndex logBeginIndex = pSyncNode->pLogStore->syncLogBeginIndex(pSyncNode->pLogStore);

  snprintf(s, len,
           "vgId:%d, sync %s, tm:%" PRId64 ", cmt:%" PRId64 ", fst:%" PRId64 ", lst:%" PRId64 ", snap:%" PRId64
           ", sby:%d, "
           "r-num:%d, "
           "lcfg:%" PRId64 ", chging:%d, rsto:%d",
           pSyncNode->vgId, syncStr(pSyncNode->state), raftStoreGetTerm(pSyncNode), pSyncNode->commitIndex,
           logBeginIndex, logLastIndex, snapshot.lastApplyIndex, pSyncNode->raftCfg.isStandBy, pSyncNode->replicaNum,
           pSyncNode->raftCfg.lastConfigIndex, pSyncNode->changing, pSyncNode->restoreFinish);

  return s;
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
  SyncPing* pMsg = syncPingBuild3(&pSyncNode->myRaftId, &pSyncNode->myRaftId, pSyncNode->vgId);
  ret = syncNodePing(pSyncNode, &pMsg->destId, pMsg);
  ASSERT(ret == 0);

  syncPingDestroy(pMsg);
  return ret;
}

int32_t syncNodePingPeers(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  for (int32_t i = 0; i < pSyncNode->peersNum; ++i) {
    SRaftId*  destId = &(pSyncNode->peersId[i]);
    SyncPing* pMsg = syncPingBuild3(&pSyncNode->myRaftId, destId, pSyncNode->vgId);
    ret = syncNodePing(pSyncNode, destId, pMsg);
    ASSERT(ret == 0);
    syncPingDestroy(pMsg);
  }
  return ret;
}

int32_t syncNodePingAll(SSyncNode* pSyncNode) {
  int32_t ret = 0;
  for (int32_t i = 0; i < pSyncNode->raftCfg.cfg.replicaNum; ++i) {
    SRaftId*  destId = &(pSyncNode->replicasId[i]);
    SyncPing* pMsg = syncPingBuild3(&pSyncNode->myRaftId, destId, pSyncNode->vgId);
    ret = syncNodePing(pSyncNode, destId, pMsg);
    ASSERT(ret == 0);
    syncPingDestroy(pMsg);
  }
  return ret;
}

// on message ----
int32_t syncNodeOnPing(SSyncNode* ths, SyncPing* pMsg) {
  sTrace("vgId:%d, recv sync-ping", ths->vgId);

  SyncPingReply* pMsgReply = syncPingReplyBuild3(&ths->myRaftId, &pMsg->srcId, ths->vgId);
  SRpcMsg        rpcMsg;
  syncPingReply2RpcMsg(pMsgReply, &rpcMsg);

  /*
    // htonl
    SMsgHead* pHead = rpcMsg.pCont;
    pHead->contLen = htonl(pHead->contLen);
    pHead->vgId = htonl(pHead->vgId);
  */

  syncNodeSendMsgById(&pMsgReply->destId, ths, &rpcMsg);
  syncPingReplyDestroy(pMsgReply);

  return 0;
}

int32_t syncNodeOnPingReply(SSyncNode* ths, SyncPingReply* pMsg) {
  int32_t ret = 0;
  sTrace("vgId:%d, recv sync-ping-reply", ths->vgId);
  return ret;
}
