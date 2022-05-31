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

#include "syncSnapshot.h"
#include "syncRaftStore.h"
#include "syncUtil.h"

static void snapshotSenderDoStart(SSyncSnapshotSender *pSender);

SSyncSnapshotSender *snapshotSenderCreate(SSyncNode *pSyncNode, int32_t replicaIndex) {
  ASSERT(pSyncNode->pFsm->FpSnapshotStartRead != NULL);
  ASSERT(pSyncNode->pFsm->FpSnapshotStopRead != NULL);
  ASSERT(pSyncNode->pFsm->FpSnapshotDoRead != NULL);

  SSyncSnapshotSender *pSender = taosMemoryMalloc(sizeof(SSyncSnapshotSender));
  ASSERT(pSender != NULL);
  memset(pSender, 0, sizeof(*pSender));

  pSender->start = false;
  pSender->seq = SYNC_SNAPSHOT_SEQ_INVALID;
  pSender->ack = SYNC_SNAPSHOT_SEQ_INVALID;
  pSender->pReader = NULL;
  pSender->pCurrentBlock = NULL;
  pSender->blockLen = 0;
  pSender->sendingMS = 5000;
  pSender->pSyncNode = pSyncNode;
  pSender->replicaIndex = replicaIndex;
  pSender->term = pSyncNode->pRaftStore->currentTerm;

  return pSender;
}

void snapshotSenderDestroy(SSyncSnapshotSender *pSender) {
  if (pSender != NULL) {
    taosMemoryFree(pSender);
  }
}

static void snapshotSenderDoStart(SSyncSnapshotSender *pSender) {
  pSender->term = pSender->pSyncNode->pRaftStore->currentTerm;
  pSender->seq = SYNC_SNAPSHOT_SEQ_BEGIN;
  pSender->ack = SYNC_SNAPSHOT_SEQ_INVALID;

  int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotStartRead(pSender->pSyncNode->pFsm, &(pSender->pReader));
  ASSERT(ret == 0);

  pSender->pSyncNode->pFsm->FpGetSnapshot(pSender->pSyncNode->pFsm, &(pSender->snapshot));

  SyncSnapshotSend *pMsg = syncSnapshotSendBuild(0, pSender->pSyncNode->vgId);
  pMsg->srcId = pSender->pSyncNode->myRaftId;
  pMsg->destId = (pSender->pSyncNode->replicasId)[pSender->replicaIndex];
  pMsg->term = pSender->pSyncNode->pRaftStore->currentTerm;
  pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
  pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
  pMsg->seq = pSender->seq;

  SRpcMsg rpcMsg;
  syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(&(pMsg->destId), pSender->pSyncNode, &rpcMsg);
  syncSnapshotSendDestroy(pMsg);
}

void snapshotSenderStart(SSyncSnapshotSender *pSender) {
  if (!(pSender->start)) {
    snapshotSenderDoStart(pSender);
    pSender->start = true;
  } else {
    ASSERT(pSender->pSyncNode->pRaftStore->currentTerm >= pSender->term);

    // leader change
    if (pSender->pSyncNode->pRaftStore->currentTerm > pSender->term) {
      // force peer rollback
      SyncSnapshotSend *pMsg = syncSnapshotSendBuild(0, pSender->pSyncNode->vgId);
      pMsg->srcId = pSender->pSyncNode->myRaftId;
      pMsg->destId = (pSender->pSyncNode->replicasId)[pSender->replicaIndex];
      pMsg->term = pSender->pSyncNode->pRaftStore->currentTerm;
      pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
      pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
      pMsg->seq = SYNC_SNAPSHOT_SEQ_FORCE_CLOSE;

      SRpcMsg rpcMsg;
      syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
      syncNodeSendMsgById(&(pMsg->destId), pSender->pSyncNode, &rpcMsg);
      syncSnapshotSendDestroy(pMsg);

      // close reader
      int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotStopRead(pSender->pSyncNode->pFsm, pSender->pReader);
      ASSERT(ret == 0);

      // start again
      snapshotSenderDoStart(pSender);
    } else {
      // do nothing
    }
  }
}

void snapshotSenderStop(SSyncSnapshotSender *pSender) {
  int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotStopRead(pSender->pSyncNode->pFsm, pSender->pReader);
  ASSERT(ret == 0);

  if (pSender->pCurrentBlock != NULL) {
    taosMemoryFree(pSender->pCurrentBlock);
    pSender->blockLen = 0;
  }
}

// send msg from seq, seq is already updated
int32_t snapshotSend(SSyncSnapshotSender *pSender) {
  // free memory last time (seq - 1)
  if (pSender->pCurrentBlock != NULL) {
    taosMemoryFree(pSender->pCurrentBlock);
    pSender->blockLen = 0;
  }

  // read data
  int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotDoRead(pSender->pSyncNode->pFsm, pSender->pReader,
                                                           &(pSender->pCurrentBlock), &(pSender->blockLen));
  ASSERT(ret == 0);

  SyncSnapshotSend *pMsg = syncSnapshotSendBuild(pSender->blockLen, pSender->pSyncNode->vgId);
  pMsg->srcId = pSender->pSyncNode->myRaftId;
  pMsg->destId = (pSender->pSyncNode->replicasId)[pSender->replicaIndex];
  pMsg->term = pSender->pSyncNode->pRaftStore->currentTerm;
  pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
  pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
  pMsg->seq = pSender->seq;
  memcpy(pMsg->data, pSender->pCurrentBlock, pSender->blockLen);

  SRpcMsg rpcMsg;
  syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(&(pMsg->destId), pSender->pSyncNode, &rpcMsg);
  syncSnapshotSendDestroy(pMsg);

  return 0;
}

cJSON *snapshotSender2Json(SSyncSnapshotSender *pSender) {
  char   u64buf[128];
  cJSON *pRoot = cJSON_CreateObject();

  if (pSender != NULL) {
    cJSON_AddNumberToObject(pRoot, "start", pSender->start);
    cJSON_AddNumberToObject(pRoot, "seq", pSender->seq);
    cJSON_AddNumberToObject(pRoot, "ack", pSender->ack);

    snprintf(u64buf, sizeof(u64buf), "%p", pSender->pReader);
    cJSON_AddStringToObject(pRoot, "pReader", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%p", pSender->pCurrentBlock);
    cJSON_AddStringToObject(pRoot, "pCurrentBlock", u64buf);
    cJSON_AddNumberToObject(pRoot, "blockLen", pSender->blockLen);

    if (pSender->pCurrentBlock != NULL) {
      char *s;
      s = syncUtilprintBin((char *)(pSender->pCurrentBlock), pSender->blockLen);
      cJSON_AddStringToObject(pRoot, "pCurrentBlock", s);
      taosMemoryFree(s);
      s = syncUtilprintBin2((char *)(pSender->pCurrentBlock), pSender->blockLen);
      cJSON_AddStringToObject(pRoot, "pCurrentBlock2", s);
      taosMemoryFree(s);
    }

    cJSON *pSnapshot = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%lu", pSender->snapshot.lastApplyIndex);
    cJSON_AddStringToObject(pRoot, "lastApplyIndex", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%lu", pSender->snapshot.lastApplyTerm);
    cJSON_AddStringToObject(pRoot, "lastApplyTerm", u64buf);
    cJSON_AddItemToObject(pRoot, "snapshot", pSnapshot);

    snprintf(u64buf, sizeof(u64buf), "%lu", pSender->sendingMS);
    cJSON_AddStringToObject(pRoot, "sendingMS", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSender->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
    cJSON_AddNumberToObject(pRoot, "replicaIndex", pSender->replicaIndex);
    snprintf(u64buf, sizeof(u64buf), "%lu", pSender->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
  }

  cJSON *pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncSnapshotSender", pRoot);
  return pJson;
}

char *snapshotSender2Str(SSyncSnapshotSender *pSender) {
  cJSON *pJson = snapshotSender2Json(pSender);
  char  *serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// -------------------------------------
SSyncSnapshotReceiver *snapshotReceiverCreate(SSyncNode *pSyncNode, int32_t replicaIndex) {
  ASSERT(pSyncNode->pFsm->FpSnapshotStartWrite != NULL);
  ASSERT(pSyncNode->pFsm->FpSnapshotStopWrite != NULL);
  ASSERT(pSyncNode->pFsm->FpSnapshotDoWrite != NULL);

  SSyncSnapshotReceiver *pReceiver = taosMemoryMalloc(sizeof(SSyncSnapshotReceiver));
  ASSERT(pReceiver != NULL);
  memset(pReceiver, 0, sizeof(*pReceiver));

  pReceiver->start = false;
  pReceiver->ack = SYNC_SNAPSHOT_SEQ_BEGIN;
  pReceiver->pWriter = NULL;
  pReceiver->pCurrentBlock = NULL;
  pReceiver->blockLen = 0;
  pReceiver->pSyncNode = pSyncNode;
  pReceiver->replicaIndex = replicaIndex;
  pReceiver->term = pSyncNode->pRaftStore->currentTerm;

  return pReceiver;
}

void snapshotReceiverDestroy(SSyncSnapshotReceiver *pReceiver) {
  if (pReceiver != NULL) {
    taosMemoryFree(pReceiver);
  }
}

void snapshotReceiverStart(SSyncSnapshotReceiver *pReceiver) {
  if (!(pReceiver->start)) {
    pReceiver->ack = SYNC_SNAPSHOT_SEQ_BEGIN;
    int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStartWrite(pReceiver->pSyncNode->pFsm, &(pReceiver->pWriter));
    ASSERT(ret == 0);
    if (pReceiver->pCurrentBlock != NULL) {
      taosMemoryFree(pReceiver->pCurrentBlock);
      pReceiver->pCurrentBlock = NULL;
      pReceiver->blockLen = 0;
    }
    pReceiver->term = pReceiver->pSyncNode->pRaftStore->currentTerm;
  } else {
    ASSERT(0);
  }
}

void snapshotReceiverStop(SSyncSnapshotReceiver *pReceiver) {
  int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, true);
  ASSERT(ret == 0);

  if (pReceiver->pCurrentBlock != NULL) {
    taosMemoryFree(pReceiver->pCurrentBlock);
    pReceiver->blockLen = 0;
  }
}

cJSON *snapshotReceiver2Json(SSyncSnapshotReceiver *pReceiver) {
  char   u64buf[128];
  cJSON *pRoot = cJSON_CreateObject();

  if (pReceiver != NULL) {
    cJSON_AddNumberToObject(pRoot, "start", pReceiver->start);
    cJSON_AddNumberToObject(pRoot, "ack", pReceiver->ack);

    snprintf(u64buf, sizeof(u64buf), "%p", pReceiver->pWriter);
    cJSON_AddStringToObject(pRoot, "pWriter", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%p", pReceiver->pCurrentBlock);
    cJSON_AddStringToObject(pRoot, "pCurrentBlock", u64buf);
    cJSON_AddNumberToObject(pRoot, "blockLen", pReceiver->blockLen);

    if (pReceiver->pCurrentBlock != NULL) {
      char *s;
      s = syncUtilprintBin((char *)(pReceiver->pCurrentBlock), pReceiver->blockLen);
      cJSON_AddStringToObject(pRoot, "pCurrentBlock", s);
      taosMemoryFree(s);
      s = syncUtilprintBin2((char *)(pReceiver->pCurrentBlock), pReceiver->blockLen);
      cJSON_AddStringToObject(pRoot, "pCurrentBlock2", s);
      taosMemoryFree(s);
    }

    snprintf(u64buf, sizeof(u64buf), "%p", pReceiver->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
    cJSON_AddNumberToObject(pRoot, "replicaIndex", pReceiver->replicaIndex);
    snprintf(u64buf, sizeof(u64buf), "%lu", pReceiver->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
  }

  cJSON *pJson = cJSON_CreateObject();
  cJSON_AddItemToObject(pJson, "SSyncSnapshotReceiver", pRoot);
  return pJson;
}

char *snapshotReceiver2Str(SSyncSnapshotReceiver *pReceiver) {
  cJSON *pJson = snapshotReceiver2Json(pReceiver);
  char  *serialized = cJSON_Print(pJson);
  cJSON_Delete(pJson);
  return serialized;
}

// receiver do something
int32_t syncNodeOnSnapshotSendCb(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  SSyncSnapshotReceiver *pReceiver = NULL;
  for (int i = 0; i < pSyncNode->replicaNum; ++i) {
    if (syncUtilSameId(&(pMsg->srcId), &((pSyncNode->replicasId)[i]))) {
      pReceiver = (pSyncNode->receivers)[i];
    }
  }
  ASSERT(pReceiver != NULL);

  // state, term, seq/ack
  if (pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER) {
    SyncSnapshotRsp *pRspMsg = syncSnapshotRspBuild(pSyncNode->vgId);
    pRspMsg->srcId = pSyncNode->myRaftId;
    pRspMsg->destId = pMsg->srcId;
    pRspMsg->term = pSyncNode->pRaftStore->currentTerm;
    pRspMsg->lastIndex = pMsg->lastIndex;
    pRspMsg->lastTerm = pMsg->lastTerm;
    pRspMsg->ack = pMsg->seq;

    if (pMsg->seq == SYNC_SNAPSHOT_SEQ_BEGIN) {
      // begin
      snapshotReceiverStart(pReceiver);

    } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_END) {
      // end
      pSyncNode->pFsm->FpSnapshotDoWrite(pSyncNode->pFsm, pReceiver->pWriter, pMsg->data, pMsg->dataLen);
      pSyncNode->pFsm->FpSnapshotStopWrite(pSyncNode->pFsm, pReceiver->pWriter, true);
      snapshotReceiverStop(pReceiver);

    } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_FORCE_CLOSE) {
      pSyncNode->pFsm->FpSnapshotStopWrite(pSyncNode->pFsm, pReceiver->pWriter, false);
      snapshotReceiverStop(pReceiver);

    } else {
      // transfering
      if (pMsg->seq == pReceiver->ack + 1) {
        pSyncNode->pFsm->FpSnapshotDoWrite(pSyncNode->pFsm, pReceiver->pWriter, pMsg->data, pMsg->dataLen);
      }
    }

    SRpcMsg rpcMsg;
    syncSnapshotRsp2RpcMsg(pRspMsg, &rpcMsg);
    syncNodeSendMsgById(&(pRspMsg->destId), pSyncNode, &rpcMsg);
  }
  return 0;
}

// sender do something
int32_t syncNodeOnSnapshotRspCb(SSyncNode *pSyncNode, SyncSnapshotRsp *pMsg) {
  SSyncSnapshotSender *pSender = NULL;
  for (int i = 0; i < pSyncNode->replicaNum; ++i) {
    if (syncUtilSameId(&(pMsg->srcId), &((pSyncNode->replicasId)[i]))) {
      pSender = (pSyncNode->senders)[i];
    }
  }
  ASSERT(pSender != NULL);

  // state, term, seq/ack
  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    if (pMsg->term == pSyncNode->pRaftStore->currentTerm) {
      if (pMsg->ack == pSender->seq) {
        pSender->ack = pMsg->ack;
        snapshotSend(pSender);
        (pSender->seq)++;
      }
    }
  }

  return 0;
}