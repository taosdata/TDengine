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
#include "syncIndexMgr.h"
#include "syncRaftCfg.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncUtil.h"
#include "wal.h"

static void snapshotReceiverDoStart(SSyncSnapshotReceiver *pReceiver, SyncTerm privateTerm, SRaftId fromId);

//----------------------------------
SSyncSnapshotSender *snapshotSenderCreate(SSyncNode *pSyncNode, int32_t replicaIndex) {
  bool condition = (pSyncNode->pFsm->FpSnapshotStartRead != NULL) && (pSyncNode->pFsm->FpSnapshotStopRead != NULL) &&
                   (pSyncNode->pFsm->FpSnapshotDoRead != NULL);

  SSyncSnapshotSender *pSender = NULL;
  if (condition) {
    pSender = taosMemoryMalloc(sizeof(SSyncSnapshotSender));
    ASSERT(pSender != NULL);
    memset(pSender, 0, sizeof(*pSender));

    pSender->start = false;
    pSender->seq = SYNC_SNAPSHOT_SEQ_INVALID;
    pSender->ack = SYNC_SNAPSHOT_SEQ_INVALID;
    pSender->pReader = NULL;
    pSender->pCurrentBlock = NULL;
    pSender->blockLen = 0;
    pSender->sendingMS = SYNC_SNAPSHOT_RETRY_MS;
    pSender->pSyncNode = pSyncNode;
    pSender->replicaIndex = replicaIndex;
    pSender->term = pSyncNode->pRaftStore->currentTerm;
    pSender->privateTerm = taosGetTimestampMs() + 100;
    pSender->pSyncNode->pFsm->FpGetSnapshot(pSender->pSyncNode->pFsm, &(pSender->snapshot));
    pSender->finish = false;
  } else {
    sError("snapshotSenderCreate cannot create sender");
  }
  return pSender;
}

void snapshotSenderDestroy(SSyncSnapshotSender *pSender) {
  if (pSender != NULL) {
    if (pSender->pCurrentBlock != NULL) {
      taosMemoryFree(pSender->pCurrentBlock);
    }
    taosMemoryFree(pSender);
  }
}

bool snapshotSenderIsStart(SSyncSnapshotSender *pSender) { return pSender->start; }

// begin send snapshot (current term, seq begin)
void snapshotSenderStart(SSyncSnapshotSender *pSender) {
  ASSERT(!snapshotSenderIsStart(pSender));

  pSender->seq = SYNC_SNAPSHOT_SEQ_BEGIN;
  pSender->ack = SYNC_SNAPSHOT_SEQ_INVALID;

  // open snapshot reader
  ASSERT(pSender->pReader == NULL);
  int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotStartRead(pSender->pSyncNode->pFsm, &(pSender->pReader));
  ASSERT(ret == 0);

  if (pSender->pCurrentBlock != NULL) {
    taosMemoryFree(pSender->pCurrentBlock);
  }

  pSender->blockLen = 0;

  // get current snapshot info
  pSender->pSyncNode->pFsm->FpGetSnapshot(pSender->pSyncNode->pFsm, &(pSender->snapshot));
  if (pSender->snapshot.lastConfigIndex != SYNC_INDEX_INVALID) {
    /*
    SSyncRaftEntry *pEntry = NULL;
    int32_t code = pSender->pSyncNode->pLogStore->syncLogGetEntry(pSender->pSyncNode->pLogStore,
                                                                  pSender->snapshot.lastConfigIndex, &pEntry);
    ASSERT(code == 0);
    ASSERT(pEntry != NULL);
    */

    SSyncRaftEntry *pEntry =
        pSender->pSyncNode->pLogStore->getEntry(pSender->pSyncNode->pLogStore, pSender->snapshot.lastConfigIndex);
    ASSERT(pEntry != NULL);

    SRpcMsg rpcMsg;
    syncEntry2OriginalRpc(pEntry, &rpcMsg);
    SSyncCfg lastConfig;
    int32_t  ret = syncCfgFromStr(rpcMsg.pCont, &lastConfig);
    ASSERT(ret == 0);
    pSender->lastConfig = lastConfig;

    rpcFreeCont(rpcMsg.pCont);
    syncEntryDestory(pEntry);

  } else {
    memset(&(pSender->lastConfig), 0, sizeof(SSyncCfg));
  }

  pSender->sendingMS = SYNC_SNAPSHOT_RETRY_MS;
  pSender->term = pSender->pSyncNode->pRaftStore->currentTerm;
  ++(pSender->privateTerm);
  pSender->finish = false;
  pSender->start = true;

  // build begin msg
  SyncSnapshotSend *pMsg = syncSnapshotSendBuild(0, pSender->pSyncNode->vgId);
  pMsg->srcId = pSender->pSyncNode->myRaftId;
  pMsg->destId = (pSender->pSyncNode->replicasId)[pSender->replicaIndex];
  pMsg->term = pSender->pSyncNode->pRaftStore->currentTerm;
  pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
  pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
  pMsg->lastConfigIndex = pSender->snapshot.lastConfigIndex;
  pMsg->lastConfig = pSender->lastConfig;
  pMsg->seq = pSender->seq;  // SYNC_SNAPSHOT_SEQ_BEGIN
  pMsg->privateTerm = pSender->privateTerm;

  // send msg
  SRpcMsg rpcMsg;
  syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(&(pMsg->destId), pSender->pSyncNode, &rpcMsg);

  char     host[128];
  uint16_t port;
  syncUtilU642Addr(pSender->pSyncNode->replicasId[pSender->replicaIndex].addr, host, sizeof(host), &port);

  if (gRaftDetailLog) {
    char *msgStr = syncSnapshotSend2Str(pMsg);
    sDebug(
        "vgId:%d sync event %s currentTerm:%lu snapshot send to %s:%d begin seq:%d ack:%d lastApplyIndex:%ld "
        "lastApplyTerm:%lu "
        "lastConfigIndex:%ld privateTerm:%lu send "
        "msg:%s",
        pSender->pSyncNode->vgId, syncUtilState2String(pSender->pSyncNode->state),
        pSender->pSyncNode->pRaftStore->currentTerm, host, port, pSender->seq, pSender->ack,
        pSender->snapshot.lastApplyIndex, pSender->snapshot.lastApplyTerm, pSender->snapshot.lastConfigIndex,
        pSender->privateTerm, msgStr);
    taosMemoryFree(msgStr);
  } else {
    sDebug(
        "vgId:%d sync event %s currentTerm:%lu snapshot send to %s:%d begin seq:%d ack:%d lastApplyIndex:%ld "
        "lastApplyTerm:%lu "
        "lastConfigIndex:%ld privateTerm:%lu",
        pSender->pSyncNode->vgId, syncUtilState2String(pSender->pSyncNode->state),
        pSender->pSyncNode->pRaftStore->currentTerm, host, port, pSender->seq, pSender->ack,
        pSender->snapshot.lastApplyIndex, pSender->snapshot.lastApplyTerm, pSender->snapshot.lastConfigIndex,
        pSender->privateTerm);
  }

  syncSnapshotSendDestroy(pMsg);
}

#if 0
// when entry in snapshot, start sender
void snapshotSenderStart(SSyncSnapshotSender *pSender) {
  if (!(pSender->start)) {
    // start
    snapshotSenderDoStart(pSender);
    pSender->start = true;
  } else {
    // already start
    ASSERT(pSender->pSyncNode->pRaftStore->currentTerm >= pSender->term);

    // if current term is higher, need start again
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

      char *msgStr = syncSnapshotSend2Str(pMsg);
      sTrace("snapshot send force close seq:%d ack:%d send msg:%s", pSender->seq, pSender->ack, msgStr);
      taosMemoryFree(msgStr);

      syncSnapshotSendDestroy(pMsg);

      // close reader
      int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotStopRead(pSender->pSyncNode->pFsm, pSender->pReader);
      ASSERT(ret == 0);
      pSender->pReader = NULL;

      // start again
      snapshotSenderDoStart(pSender);
      pSender->start = true;
    } else {
      // current term, do nothing
      ASSERT(pSender->pSyncNode->pRaftStore->currentTerm == pSender->term);
    }
  }

  char *s = snapshotSender2Str(pSender);
  sInfo("snapshotSenderStart %s", s);
  taosMemoryFree(s);
}
#endif

void snapshotSenderStop(SSyncSnapshotSender *pSender) {
  if (pSender->pReader != NULL) {
    int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotStopRead(pSender->pSyncNode->pFsm, pSender->pReader);
    ASSERT(ret == 0);
    pSender->pReader = NULL;
  }

  if (pSender->pCurrentBlock != NULL) {
    taosMemoryFree(pSender->pCurrentBlock);
    pSender->pCurrentBlock = NULL;
    pSender->blockLen = 0;
  }

  pSender->start = false;

  if (gRaftDetailLog) {
    char *s = snapshotSender2Str(pSender);
    sInfo("snapshotSenderStop %s", s);
    taosMemoryFree(s);
  }
}

// when sender receiver ack, call this function to send msg from seq
// seq = ack + 1, already updated
int32_t snapshotSend(SSyncSnapshotSender *pSender) {
  // free memory last time (seq - 1)
  if (pSender->pCurrentBlock != NULL) {
    taosMemoryFree(pSender->pCurrentBlock);
    pSender->pCurrentBlock = NULL;
    pSender->blockLen = 0;
  }

  // read data
  int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotDoRead(pSender->pSyncNode->pFsm, pSender->pReader,
                                                           &(pSender->pCurrentBlock), &(pSender->blockLen));
  ASSERT(ret == 0);
  if (pSender->blockLen > 0) {
    // has read data
  } else {
    // read finish
    pSender->seq = SYNC_SNAPSHOT_SEQ_END;
  }

  // build msg
  SyncSnapshotSend *pMsg = syncSnapshotSendBuild(pSender->blockLen, pSender->pSyncNode->vgId);
  pMsg->srcId = pSender->pSyncNode->myRaftId;
  pMsg->destId = (pSender->pSyncNode->replicasId)[pSender->replicaIndex];
  pMsg->term = pSender->pSyncNode->pRaftStore->currentTerm;
  pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
  pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
  pMsg->lastConfigIndex = pSender->snapshot.lastConfigIndex;
  pMsg->lastConfig = pSender->lastConfig;
  pMsg->seq = pSender->seq;
  pMsg->privateTerm = pSender->privateTerm;
  memcpy(pMsg->data, pSender->pCurrentBlock, pSender->blockLen);

  // send msg
  SRpcMsg rpcMsg;
  syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(&(pMsg->destId), pSender->pSyncNode, &rpcMsg);

  char     host[128];
  uint16_t port;
  syncUtilU642Addr(pSender->pSyncNode->replicasId[pSender->replicaIndex].addr, host, sizeof(host), &port);

  if (pSender->seq == SYNC_SNAPSHOT_SEQ_END) {
    if (gRaftDetailLog) {
      char *msgStr = syncSnapshotSend2Str(pMsg);
      sDebug(
          "vgId:%d sync event %s currentTerm:%lu snapshot send to %s:%d finish seq:%d ack:%d lastApplyIndex:%ld "
          "lastApplyTerm:%lu "
          "lastConfigIndex:%ld privateTerm:%lu send "
          "msg:%s",
          pSender->pSyncNode->vgId, syncUtilState2String(pSender->pSyncNode->state),
          pSender->pSyncNode->pRaftStore->currentTerm, host, port, pSender->seq, pSender->ack,
          pSender->snapshot.lastApplyIndex, pSender->snapshot.lastApplyTerm, pSender->snapshot.lastConfigIndex,
          pSender->privateTerm, msgStr);
      taosMemoryFree(msgStr);
    } else {
      sDebug(
          "vgId:%d sync event %s currentTerm:%lu snapshot send to %s:%d finish seq:%d ack:%d lastApplyIndex:%ld "
          "lastApplyTerm:%lu "
          "lastConfigIndex:%ld privateTerm:%lu",
          pSender->pSyncNode->vgId, syncUtilState2String(pSender->pSyncNode->state),
          pSender->pSyncNode->pRaftStore->currentTerm, host, port, pSender->seq, pSender->ack,
          pSender->snapshot.lastApplyIndex, pSender->snapshot.lastApplyTerm, pSender->snapshot.lastConfigIndex,
          pSender->privateTerm);
    }
  } else {
    sDebug(
        "vgId:%d sync event %s currentTerm:%lu snapshot send to %s:%d sending seq:%d ack:%d lastApplyIndex:%ld "
        "lastApplyTerm:%lu "
        "lastConfigIndex:%ld privateTerm:%lu",
        pSender->pSyncNode->vgId, syncUtilState2String(pSender->pSyncNode->state),
        pSender->pSyncNode->pRaftStore->currentTerm, host, port, pSender->seq, pSender->ack,
        pSender->snapshot.lastApplyIndex, pSender->snapshot.lastApplyTerm, pSender->snapshot.lastConfigIndex,
        pSender->privateTerm);
  }

  syncSnapshotSendDestroy(pMsg);
  return 0;
}

// send snapshot data from cache
int32_t snapshotReSend(SSyncSnapshotSender *pSender) {
  if (pSender->pCurrentBlock != NULL) {
    SyncSnapshotSend *pMsg = syncSnapshotSendBuild(pSender->blockLen, pSender->pSyncNode->vgId);
    pMsg->srcId = pSender->pSyncNode->myRaftId;
    pMsg->destId = (pSender->pSyncNode->replicasId)[pSender->replicaIndex];
    pMsg->term = pSender->pSyncNode->pRaftStore->currentTerm;
    pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
    pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
    pMsg->lastConfigIndex = pSender->snapshot.lastConfigIndex;
    pMsg->lastConfig = pSender->lastConfig;
    pMsg->seq = pSender->seq;
    memcpy(pMsg->data, pSender->pCurrentBlock, pSender->blockLen);

    SRpcMsg rpcMsg;
    syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
    syncNodeSendMsgById(&(pMsg->destId), pSender->pSyncNode, &rpcMsg);

    char     host[128];
    uint16_t port;
    syncUtilU642Addr(pSender->pSyncNode->replicasId[pSender->replicaIndex].addr, host, sizeof(host), &port);

    if (gRaftDetailLog) {
      char *msgStr = syncSnapshotSend2Str(pMsg);
      sDebug(
          "vgId:%d sync event %s currentTerm:%lu snapshot send to %s:%d resend seq:%d ack:%d privateTerm:%lu send "
          "msg:%s",
          pSender->pSyncNode->vgId, syncUtilState2String(pSender->pSyncNode->state),
          pSender->pSyncNode->pRaftStore->currentTerm, host, port, pSender->seq, pSender->ack, pSender->privateTerm,
          msgStr);
      taosMemoryFree(msgStr);
    } else {
      sDebug("vgId:%d sync event %s currentTerm:%lu snapshot send to %s:%d resend seq:%d ack:%d privateTerm:%lu",
             pSender->pSyncNode->vgId, syncUtilState2String(pSender->pSyncNode->state),
             pSender->pSyncNode->pRaftStore->currentTerm, host, port, pSender->seq, pSender->ack, pSender->privateTerm);
    }

    syncSnapshotSendDestroy(pMsg);
  }
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
    cJSON_AddStringToObject(pSnapshot, "lastApplyIndex", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%lu", pSender->snapshot.lastApplyTerm);
    cJSON_AddStringToObject(pSnapshot, "lastApplyTerm", u64buf);
    cJSON_AddItemToObject(pRoot, "snapshot", pSnapshot);

    snprintf(u64buf, sizeof(u64buf), "%lu", pSender->sendingMS);
    cJSON_AddStringToObject(pRoot, "sendingMS", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSender->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
    cJSON_AddNumberToObject(pRoot, "replicaIndex", pSender->replicaIndex);
    snprintf(u64buf, sizeof(u64buf), "%lu", pSender->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%lu", pSender->privateTerm);
    cJSON_AddStringToObject(pRoot, "privateTerm", u64buf);
    cJSON_AddNumberToObject(pRoot, "finish", pSender->finish);
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
SSyncSnapshotReceiver *snapshotReceiverCreate(SSyncNode *pSyncNode, SRaftId fromId) {
  bool condition = (pSyncNode->pFsm->FpSnapshotStartWrite != NULL) && (pSyncNode->pFsm->FpSnapshotStopWrite != NULL) &&
                   (pSyncNode->pFsm->FpSnapshotDoWrite != NULL);

  SSyncSnapshotReceiver *pReceiver = NULL;
  if (condition) {
    pReceiver = taosMemoryMalloc(sizeof(SSyncSnapshotReceiver));
    ASSERT(pReceiver != NULL);
    memset(pReceiver, 0, sizeof(*pReceiver));

    pReceiver->start = false;
    pReceiver->ack = SYNC_SNAPSHOT_SEQ_BEGIN;
    pReceiver->pWriter = NULL;
    pReceiver->pSyncNode = pSyncNode;
    pReceiver->fromId = fromId;
    pReceiver->term = pSyncNode->pRaftStore->currentTerm;
    pReceiver->privateTerm = 0;

  } else {
    sInfo("snapshotReceiverCreate cannot create receiver");
  }

  return pReceiver;
}

void snapshotReceiverDestroy(SSyncSnapshotReceiver *pReceiver) {
  if (pReceiver != NULL) {
    taosMemoryFree(pReceiver);
  }
}

bool snapshotReceiverIsStart(SSyncSnapshotReceiver *pReceiver) { return pReceiver->start; }

// begin receive snapshot msg (current term, seq begin)
static void snapshotReceiverDoStart(SSyncSnapshotReceiver *pReceiver, SyncTerm privateTerm, SRaftId fromId) {
  pReceiver->term = pReceiver->pSyncNode->pRaftStore->currentTerm;
  pReceiver->privateTerm = privateTerm;
  pReceiver->ack = SYNC_SNAPSHOT_SEQ_BEGIN;
  pReceiver->fromId = fromId;

  ASSERT(pReceiver->pWriter == NULL);
  int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStartWrite(pReceiver->pSyncNode->pFsm, &(pReceiver->pWriter));
  ASSERT(ret == 0);
}

// if receiver receive msg from seq = SYNC_SNAPSHOT_SEQ_BEGIN, start receiver
// if already start, force close, start again
void snapshotReceiverStart(SSyncSnapshotReceiver *pReceiver, SyncTerm privateTerm, SRaftId fromId) {
  if (!snapshotReceiverIsStart(pReceiver)) {
    // start
    snapshotReceiverDoStart(pReceiver, privateTerm, fromId);
    pReceiver->start = true;

  } else {
    // already start
    sInfo("snapshot recv, receiver already start");

    // force close, abandon incomplete data
    int32_t ret =
        pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, false);
    ASSERT(ret == 0);
    pReceiver->pWriter = NULL;

    // start again
    snapshotReceiverDoStart(pReceiver, privateTerm, fromId);
    pReceiver->start = true;
  }

  if (gRaftDetailLog) {
    char *s = snapshotReceiver2Str(pReceiver);
    sInfo("snapshotReceiverStart %s", s);
    taosMemoryFree(s);
  }
}

void snapshotReceiverStop(SSyncSnapshotReceiver *pReceiver, bool apply) {
  if (pReceiver->pWriter != NULL) {
    int32_t ret =
        pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, false);
    ASSERT(ret == 0);
    pReceiver->pWriter = NULL;
  }

  pReceiver->start = false;

  if (apply) {
    //    ++(pReceiver->privateTerm);
  }

  if (gRaftDetailLog) {
    char *s = snapshotReceiver2Str(pReceiver);
    sInfo("snapshotReceiverStop %s", s);
    taosMemoryFree(s);
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

    snprintf(u64buf, sizeof(u64buf), "%p", pReceiver->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);

    cJSON *pFromId = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%lu", pReceiver->fromId.addr);
    cJSON_AddStringToObject(pFromId, "addr", u64buf);
    {
      uint64_t u64 = pReceiver->fromId.addr;
      cJSON   *pTmp = pFromId;
      char     host[128] = {0};
      uint16_t port;
      syncUtilU642Addr(u64, host, sizeof(host), &port);
      cJSON_AddStringToObject(pTmp, "addr_host", host);
      cJSON_AddNumberToObject(pTmp, "addr_port", port);
    }
    cJSON_AddNumberToObject(pFromId, "vgId", pReceiver->fromId.vgId);
    cJSON_AddItemToObject(pRoot, "fromId", pFromId);

    snprintf(u64buf, sizeof(u64buf), "%lu", pReceiver->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%lu", pReceiver->privateTerm);
    cJSON_AddStringToObject(pRoot, "privateTerm", u64buf);
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
  // get receiver
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;
  bool                   needRsp = false;
  int32_t                writeCode = 0;

  // state, term, seq/ack
  if (pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER) {
    if (pMsg->term == pSyncNode->pRaftStore->currentTerm) {
      if (pMsg->seq == SYNC_SNAPSHOT_SEQ_BEGIN) {
        // begin
        snapshotReceiverStart(pReceiver, pMsg->privateTerm, pMsg->srcId);
        pReceiver->ack = pMsg->seq;
        needRsp = true;

        char     host[128];
        uint16_t port;
        syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);

        if (gRaftDetailLog) {
          char *msgStr = syncSnapshotSend2Str(pMsg);
          sDebug(
              "vgId:%d sync event %s currentTerm:%lu snapshot recv from %s:%d begin ack:%d, lastIndex:%ld, "
              "lastTerm:%lu, "
              "lastConfigIndex:%ld, privateTerm:%lu, recv msg:%s",
              pSyncNode->vgId, syncUtilState2String(pSyncNode->state), pSyncNode->pRaftStore->currentTerm, host, port,
              pReceiver->ack, pMsg->lastIndex, pMsg->lastTerm, pMsg->lastConfigIndex, pReceiver->privateTerm, msgStr);
          taosMemoryFree(msgStr);
        } else {
          sDebug(
              "vgId:%d sync event %s currentTerm:%lu snapshot recv from %s:%d begin ack:%d, lastIndex:%ld, "
              "lastTerm:%lu, "
              "lastConfigIndex:%ld privateTerm:%lu",
              pSyncNode->vgId, syncUtilState2String(pSyncNode->state), pSyncNode->pRaftStore->currentTerm, host, port,
              pReceiver->ack, pMsg->lastIndex, pMsg->lastTerm, pMsg->lastConfigIndex, pReceiver->privateTerm);
        }

      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_END) {
        // end, finish FSM
        writeCode = pSyncNode->pFsm->FpSnapshotDoWrite(pSyncNode->pFsm, pReceiver->pWriter, pMsg->data, pMsg->dataLen);
        ASSERT(writeCode == 0);

        pSyncNode->pFsm->FpSnapshotStopWrite(pSyncNode->pFsm, pReceiver->pWriter, true);
        pSyncNode->pLogStore->syncLogSetBeginIndex(pSyncNode->pLogStore, pMsg->lastIndex + 1);

        // maybe update lastconfig
        if (pMsg->lastConfigIndex >= SYNC_INDEX_BEGIN) {
          // int32_t  oldReplicaNum = pSyncNode->replicaNum;
          SSyncCfg oldSyncCfg = pSyncNode->pRaftCfg->cfg;

          // update new config myIndex
          SSyncCfg newSyncCfg = pMsg->lastConfig;
          syncNodeUpdateNewConfigIndex(pSyncNode, &newSyncCfg);
          bool IamInNew = syncNodeInConfig(pSyncNode, &newSyncCfg);

#if 0
          // update new config myIndex
          bool     IamInNew = false;
          SSyncCfg newSyncCfg = pMsg->lastConfig;
          for (int i = 0; i < newSyncCfg.replicaNum; ++i) {
            if (strcmp(pSyncNode->myNodeInfo.nodeFqdn, (newSyncCfg.nodeInfo)[i].nodeFqdn) == 0 &&
                pSyncNode->myNodeInfo.nodePort == (newSyncCfg.nodeInfo)[i].nodePort) {
              newSyncCfg.myIndex = i;
              IamInNew = true;
              break;
            }
          }
#endif

          bool isDrop;
          if (IamInNew) {
            sDebug(
                "vgId:%d sync event %s currentTerm:%lu update config by snapshot, lastIndex:%ld, lastTerm:%lu, "
                "lastConfigIndex:%ld ",
                pSyncNode->vgId, syncUtilState2String(pSyncNode->state), pSyncNode->pRaftStore->currentTerm,
                pMsg->lastIndex, pMsg->lastTerm, pMsg->lastConfigIndex);
            syncNodeUpdateConfig(pSyncNode, &newSyncCfg, pMsg->lastConfigIndex, &isDrop);
          } else {
            sDebug(
                "vgId:%d sync event %s currentTerm:%lu do not update config by snapshot, I am not in newCfg, "
                "lastIndex:%ld, lastTerm:%lu, "
                "lastConfigIndex:%ld ",
                pSyncNode->vgId, syncUtilState2String(pSyncNode->state), pSyncNode->pRaftStore->currentTerm,
                pMsg->lastIndex, pMsg->lastTerm, pMsg->lastConfigIndex);
          }

          // change isStandBy to normal
          if (!isDrop) {
            char  tmpbuf[512];
            char *oldStr = syncCfg2Str(&oldSyncCfg);
            char *newStr = syncCfg2Str(&newSyncCfg);
            syncUtilJson2Line(oldStr);
            syncUtilJson2Line(newStr);
            snprintf(tmpbuf, sizeof(tmpbuf), "config change3 from %d to %d, %s  -->  %s", oldSyncCfg.replicaNum,
                     newSyncCfg.replicaNum, oldStr, newStr);
            taosMemoryFree(oldStr);
            taosMemoryFree(newStr);

            if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
              syncNodeBecomeLeader(pSyncNode, tmpbuf);
            } else {
              syncNodeBecomeFollower(pSyncNode, tmpbuf);
            }
          }
        }

        SSnapshot snapshot;
        pSyncNode->pFsm->FpGetSnapshot(pSyncNode->pFsm, &snapshot);

        char     host[128];
        uint16_t port;
        syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);

        if (gRaftDetailLog) {
          char *logSimpleStr = logStoreSimple2Str(pSyncNode->pLogStore);
          sDebug(
              "vgId:%d sync event %s currentTerm:%lu snapshot recv from %s:%d finish, update log begin index:%ld, "
              "snapshot.lastApplyIndex:%ld, "
              "snapshot.lastApplyTerm:%lu, snapshot.lastConfigIndex:%ld, privateTerm:%lu, raft log:%s",
              pSyncNode->vgId, syncUtilState2String(pSyncNode->state), pSyncNode->pRaftStore->currentTerm, host, port,
              pMsg->lastIndex + 1, snapshot.lastApplyIndex, snapshot.lastApplyTerm, snapshot.lastConfigIndex,
              pReceiver->privateTerm, logSimpleStr);
          taosMemoryFree(logSimpleStr);
        } else {
          sDebug(
              "vgId:%d sync event %s currentTerm:%lu snapshot recv from %s:%d finish, update log begin index:%ld, "
              "snapshot.lastApplyIndex:%ld, "
              "snapshot.lastApplyTerm:%lu, snapshot.lastConfigIndex:%ld, privateTerm:%lu",
              pSyncNode->vgId, syncUtilState2String(pSyncNode->state), pSyncNode->pRaftStore->currentTerm, host, port,
              pMsg->lastIndex + 1, snapshot.lastApplyIndex, snapshot.lastApplyTerm, snapshot.lastConfigIndex,
              pReceiver->privateTerm);
        }

        pReceiver->pWriter = NULL;
        snapshotReceiverStop(pReceiver, true);
        pReceiver->ack = pMsg->seq;
        needRsp = true;

        if (gRaftDetailLog) {
          char *msgStr = syncSnapshotSend2Str(pMsg);
          sDebug(
              "vgId:%d sync event %s currentTerm:%lu snapshot recv from %s:%d end ack:%d, lastIndex:%ld, lastTerm:%lu, "
              "lastConfigIndex:%ld, privateTerm:%lu, recv msg:%s",
              pReceiver->pSyncNode->vgId, syncUtilState2String(pSyncNode->state),
              pReceiver->pSyncNode->pRaftStore->currentTerm, host, port, pReceiver->ack, pMsg->lastIndex,
              pMsg->lastTerm, pMsg->lastConfigIndex, pReceiver->privateTerm, msgStr);
          taosMemoryFree(msgStr);
        } else {
          sDebug(
              "vgId:%d sync event %s currentTerm:%lu snapshot recv from %s:%d end ack:%d, lastIndex:%ld, lastTerm:%lu, "
              "lastConfigIndex:%ld, privateTerm:%lu",
              pReceiver->pSyncNode->vgId, syncUtilState2String(pSyncNode->state),
              pReceiver->pSyncNode->pRaftStore->currentTerm, host, port, pReceiver->ack, pMsg->lastIndex,
              pMsg->lastTerm, pMsg->lastConfigIndex, pReceiver->privateTerm);
        }

      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_FORCE_CLOSE) {
        pSyncNode->pFsm->FpSnapshotStopWrite(pSyncNode->pFsm, pReceiver->pWriter, false);
        snapshotReceiverStop(pReceiver, false);
        needRsp = false;

        char     host[128];
        uint16_t port;
        syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);

        if (gRaftDetailLog) {
          char *msgStr = syncSnapshotSend2Str(pMsg);
          sDebug(
              "vgId:%d sync event %s currentTerm:%lu snapshot recv from %s:%d force close ack:%d, lastIndex:%ld, "
              "lastTerm:%lu, "
              "lastConfigIndex:%ld, privateTerm:%lu, recv "
              "msg:%s",
              pReceiver->pSyncNode->vgId, syncUtilState2String(pSyncNode->state),
              pReceiver->pSyncNode->pRaftStore->currentTerm, host, port, pReceiver->ack, pMsg->lastIndex,
              pMsg->lastTerm, pMsg->lastConfigIndex, pReceiver->privateTerm, msgStr);
          taosMemoryFree(msgStr);
        } else {
          sDebug(
              "vgId:%d sync event %s currentTerm:%lu snapshot recv from %s:%d force close ack:%d, lastIndex:%ld, "
              "lastTerm:%lu, "
              "lastConfigIndex:%ld, privateTerm:%lu",
              pReceiver->pSyncNode->vgId, syncUtilState2String(pSyncNode->state),
              pReceiver->pSyncNode->pRaftStore->currentTerm, host, port, pReceiver->ack, pMsg->lastIndex,
              pMsg->lastTerm, pMsg->lastConfigIndex, pReceiver->privateTerm);
        }

      } else if (pMsg->seq > SYNC_SNAPSHOT_SEQ_BEGIN && pMsg->seq < SYNC_SNAPSHOT_SEQ_END) {
        // transfering
        if (pMsg->seq == pReceiver->ack + 1) {
          writeCode =
              pSyncNode->pFsm->FpSnapshotDoWrite(pSyncNode->pFsm, pReceiver->pWriter, pMsg->data, pMsg->dataLen);
          ASSERT(writeCode == 0);
          pReceiver->ack = pMsg->seq;
        }
        needRsp = true;

        char     host[128];
        uint16_t port;
        syncUtilU642Addr(pMsg->srcId.addr, host, sizeof(host), &port);

        if (gRaftDetailLog) {
          char *msgStr = syncSnapshotSend2Str(pMsg);
          sDebug(
              "vgId:%d sync event %s currentTerm:%lu snapshot recv from %s:%d receiving ack:%d, lastIndex:%ld, "
              "lastTerm:%lu, "
              "lastConfigIndex:%ld, privateTerm:%lu, recv msg:%s",
              pSyncNode->vgId, syncUtilState2String(pSyncNode->state), pSyncNode->pRaftStore->currentTerm, host, port,
              pReceiver->ack, pMsg->lastIndex, pMsg->lastTerm, pMsg->lastConfigIndex, pReceiver->privateTerm, msgStr);
          taosMemoryFree(msgStr);
        } else {
          sDebug(
              "vgId:%d sync event %s currentTerm:%lu snapshot recv from %s:%d receiving ack:%d, lastIndex:%ld, "
              "lastTerm:%lu, "
              "lastConfigIndex:%ld, privateTerm:%lu",
              pSyncNode->vgId, syncUtilState2String(pSyncNode->state), pSyncNode->pRaftStore->currentTerm, host, port,
              pReceiver->ack, pMsg->lastIndex, pMsg->lastTerm, pMsg->lastConfigIndex, pReceiver->privateTerm);
        }

      } else {
        ASSERT(0);
      }

      if (needRsp) {
        SyncSnapshotRsp *pRspMsg = syncSnapshotRspBuild(pSyncNode->vgId);
        pRspMsg->srcId = pSyncNode->myRaftId;
        pRspMsg->destId = pMsg->srcId;
        pRspMsg->term = pSyncNode->pRaftStore->currentTerm;
        pRspMsg->lastIndex = pMsg->lastIndex;
        pRspMsg->lastTerm = pMsg->lastTerm;
        pRspMsg->ack = pReceiver->ack;
        pRspMsg->code = writeCode;
        pRspMsg->privateTerm = pReceiver->privateTerm;

        SRpcMsg rpcMsg;
        syncSnapshotRsp2RpcMsg(pRspMsg, &rpcMsg);
        syncNodeSendMsgById(&(pRspMsg->destId), pSyncNode, &rpcMsg);

        syncSnapshotRspDestroy(pRspMsg);
      }
    }
  } else {
    syncNodeLog2("syncNodeOnSnapshotSendCb not follower", pSyncNode);
  }

  return 0;
}

// sender receives ack, set seq = ack + 1, send msg from seq
// if ack == SYNC_SNAPSHOT_SEQ_END, stop sender
int32_t syncNodeOnSnapshotRspCb(SSyncNode *pSyncNode, SyncSnapshotRsp *pMsg) {
  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(pSyncNode, &(pMsg->srcId)) && pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    sInfo("recv SyncSnapshotRsp maybe replica already dropped");
    return 0;
  }

  // get sender
  SSyncSnapshotSender *pSender = syncNodeGetSnapshotSender(pSyncNode, &(pMsg->srcId));
  ASSERT(pSender != NULL);

  // state, term, seq/ack
  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    if (pMsg->term == pSyncNode->pRaftStore->currentTerm) {
      // receiver ack is finish, close sender
      if (pMsg->ack == SYNC_SNAPSHOT_SEQ_END) {
        pSender->finish = true;
        snapshotSenderStop(pSender);
        return 0;
      }

      // send next msg
      if (pMsg->ack == pSender->seq) {
        // update sender ack
        pSender->ack = pMsg->ack;
        (pSender->seq)++;
        snapshotSend(pSender);

      } else if (pMsg->ack == pSender->seq - 1) {
        snapshotReSend(pSender);

      } else {
        ASSERT(0);
      }
    }
  } else {
    syncNodeLog2("syncNodeOnSnapshotRspCb not leader", pSyncNode);
  }

  return 0;
}
