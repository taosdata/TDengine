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

//----------------------------------
static void    snapshotSenderUpdateProgress(SSyncSnapshotSender *pSender, SyncSnapshotRsp *pMsg);
static void    snapshotReceiverDoStart(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pBeginMsg);
static void    snapshotReceiverGotData(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pMsg);
static int32_t snapshotReceiverFinish(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pMsg);

//----------------------------------
SSyncSnapshotSender *snapshotSenderCreate(SSyncNode *pSyncNode, int32_t replicaIndex) {
  bool condition = (pSyncNode->pFsm->FpSnapshotStartRead != NULL) && (pSyncNode->pFsm->FpSnapshotStopRead != NULL) &&
                   (pSyncNode->pFsm->FpSnapshotDoRead != NULL);

  SSyncSnapshotSender *pSender = NULL;
  if (condition) {
    pSender = taosMemoryMalloc(sizeof(SSyncSnapshotSender));
    if (pSender == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }
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
    pSender->startTime = 0;
    pSender->pSyncNode->pFsm->FpGetSnapshotInfo(pSender->pSyncNode->pFsm, &(pSender->snapshot));
    pSender->finish = false;
  } else {
    sError("vgId:%d, cannot create snapshot sender", pSyncNode->vgId);
  }

  return pSender;
}

void snapshotSenderDestroy(SSyncSnapshotSender *pSender) {
  if (pSender != NULL) {
    // free current block
    if (pSender->pCurrentBlock != NULL) {
      taosMemoryFree(pSender->pCurrentBlock);
      pSender->pCurrentBlock = NULL;
    }

    // close reader
    if (pSender->pReader != NULL) {
      int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotStopRead(pSender->pSyncNode->pFsm, pSender->pReader);
      if (ret != 0) {
        sNError(pSender->pSyncNode, "stop reader error");
      }
      pSender->pReader = NULL;
    }

    // free sender
    taosMemoryFree(pSender);
  }
}

bool snapshotSenderIsStart(SSyncSnapshotSender *pSender) { return pSender->start; }

int32_t snapshotSenderStart(SSyncSnapshotSender *pSender) {
  ASSERT(!snapshotSenderIsStart(pSender));

  pSender->start = true;
  pSender->seq = SYNC_SNAPSHOT_SEQ_BEGIN;
  pSender->ack = SYNC_SNAPSHOT_SEQ_INVALID;
  pSender->pReader = NULL;
  pSender->pCurrentBlock = NULL;
  pSender->blockLen = 0;

  pSender->snapshotParam.start = SYNC_INDEX_INVALID;
  pSender->snapshotParam.end = SYNC_INDEX_INVALID;

  pSender->snapshot.data = NULL;
  pSender->snapshotParam.end = SYNC_INDEX_INVALID;
  pSender->snapshot.lastApplyIndex = SYNC_INDEX_INVALID;
  pSender->snapshot.lastApplyTerm = SYNC_TERM_INVALID;
  pSender->snapshot.lastConfigIndex = SYNC_INDEX_INVALID;

  memset(&(pSender->lastConfig), 0, sizeof(pSender->lastConfig));
  pSender->sendingMS = 0;
  pSender->term = pSender->pSyncNode->pRaftStore->currentTerm;
  pSender->startTime = taosGetTimestampMs();
  pSender->finish = false;

  // build begin msg
  SyncSnapshotSend *pMsg = syncSnapshotSendBuild(0, pSender->pSyncNode->vgId);
  pMsg->srcId = pSender->pSyncNode->myRaftId;
  pMsg->destId = (pSender->pSyncNode->replicasId)[pSender->replicaIndex];
  pMsg->term = pSender->pSyncNode->pRaftStore->currentTerm;
  pMsg->beginIndex = pSender->snapshotParam.start;
  pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
  pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
  pMsg->lastConfigIndex = pSender->snapshot.lastConfigIndex;
  pMsg->lastConfig = pSender->lastConfig;
  pMsg->startTime = pSender->startTime;
  pMsg->seq = SYNC_SNAPSHOT_SEQ_PRE_SNAPSHOT;

  // send msg
  SRpcMsg rpcMsg;
  syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(&(pMsg->destId), pSender->pSyncNode, &rpcMsg);
  syncSnapshotSendDestroy(pMsg);

  // event log
  sSTrace(pSender, "snapshot sender start");
  return 0;
}

int32_t snapshotSenderStop(SSyncSnapshotSender *pSender, bool finish) {
  // update flag
  pSender->start = false;
  pSender->finish = finish;

  // close reader
  if (pSender->pReader != NULL) {
    int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotStopRead(pSender->pSyncNode->pFsm, pSender->pReader);
    ASSERT(ret == 0);
    pSender->pReader = NULL;
  }

  // free current block
  if (pSender->pCurrentBlock != NULL) {
    taosMemoryFree(pSender->pCurrentBlock);
    pSender->pCurrentBlock = NULL;
    pSender->blockLen = 0;
  }

  // event log
  sSTrace(pSender, "snapshot sender stop");
  return 0;
}

// when sender receive ack, call this function to send msg from seq
// seq = ack + 1, already updated
int32_t snapshotSend(SSyncSnapshotSender *pSender) {
  // free memory last time (current seq - 1)
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
    // read finish, update seq to end
    pSender->seq = SYNC_SNAPSHOT_SEQ_END;
  }

  // build msg
  SyncSnapshotSend *pMsg = syncSnapshotSendBuild(pSender->blockLen, pSender->pSyncNode->vgId);
  pMsg->srcId = pSender->pSyncNode->myRaftId;
  pMsg->destId = (pSender->pSyncNode->replicasId)[pSender->replicaIndex];
  pMsg->term = pSender->pSyncNode->pRaftStore->currentTerm;
  pMsg->beginIndex = pSender->snapshotParam.start;
  pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
  pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
  pMsg->lastConfigIndex = pSender->snapshot.lastConfigIndex;
  pMsg->lastConfig = pSender->lastConfig;
  pMsg->seq = pSender->seq;

  // pMsg->privateTerm = pSender->privateTerm;

  memcpy(pMsg->data, pSender->pCurrentBlock, pSender->blockLen);

  // send msg
  SRpcMsg rpcMsg;
  syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(&(pMsg->destId), pSender->pSyncNode, &rpcMsg);
  syncSnapshotSendDestroy(pMsg);

  // event log
  if (pSender->seq == SYNC_SNAPSHOT_SEQ_END) {
    sSTrace(pSender, "snapshot sender finish");
  } else {
    sSTrace(pSender, "snapshot sender sending");
  }
  return 0;
}

// send snapshot data from cache
int32_t snapshotReSend(SSyncSnapshotSender *pSender) {
  // send current block data
  if (pSender->pCurrentBlock != NULL && pSender->blockLen > 0) {
    // build msg
    SyncSnapshotSend *pMsg = syncSnapshotSendBuild(pSender->blockLen, pSender->pSyncNode->vgId);
    pMsg->srcId = pSender->pSyncNode->myRaftId;
    pMsg->destId = (pSender->pSyncNode->replicasId)[pSender->replicaIndex];
    pMsg->term = pSender->pSyncNode->pRaftStore->currentTerm;
    pMsg->beginIndex = pSender->snapshotParam.start;
    pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
    pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
    pMsg->lastConfigIndex = pSender->snapshot.lastConfigIndex;
    pMsg->lastConfig = pSender->lastConfig;
    pMsg->seq = pSender->seq;

    //  pMsg->privateTerm = pSender->privateTerm;

    memcpy(pMsg->data, pSender->pCurrentBlock, pSender->blockLen);

    // send msg
    SRpcMsg rpcMsg;
    syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
    syncNodeSendMsgById(&(pMsg->destId), pSender->pSyncNode, &rpcMsg);
    syncSnapshotSendDestroy(pMsg);

    // event log
    sSTrace(pSender, "snapshot sender resend");
  }

  return 0;
}

static void snapshotSenderUpdateProgress(SSyncSnapshotSender *pSender, SyncSnapshotRsp *pMsg) {
  ASSERT(pMsg->ack == pSender->seq);
  pSender->ack = pMsg->ack;
  ++(pSender->seq);
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
      s = syncUtilPrintBin((char *)(pSender->pCurrentBlock), pSender->blockLen);
      cJSON_AddStringToObject(pRoot, "pCurrentBlock", s);
      taosMemoryFree(s);
      s = syncUtilPrintBin2((char *)(pSender->pCurrentBlock), pSender->blockLen);
      cJSON_AddStringToObject(pRoot, "pCurrentBlock2", s);
      taosMemoryFree(s);
    }

    cJSON *pSnapshot = cJSON_CreateObject();
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSender->snapshot.lastApplyIndex);
    cJSON_AddStringToObject(pSnapshot, "lastApplyIndex", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSender->snapshot.lastApplyTerm);
    cJSON_AddStringToObject(pSnapshot, "lastApplyTerm", u64buf);
    cJSON_AddItemToObject(pRoot, "snapshot", pSnapshot);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSender->sendingMS);
    cJSON_AddStringToObject(pRoot, "sendingMS", u64buf);
    snprintf(u64buf, sizeof(u64buf), "%p", pSender->pSyncNode);
    cJSON_AddStringToObject(pRoot, "pSyncNode", u64buf);
    cJSON_AddNumberToObject(pRoot, "replicaIndex", pSender->replicaIndex);
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSender->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);

    //  snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pSender->privateTerm);
    //  cJSON_AddStringToObject(pRoot, "privateTerm", u64buf);

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

int32_t syncNodeStartSnapshot(SSyncNode *pSyncNode, SRaftId *pDestId) {
  sNTrace(pSyncNode, "starting snapshot ...");

  SSyncSnapshotSender *pSender = syncNodeGetSnapshotSender(pSyncNode, pDestId);
  if (pSender == NULL) {
    sNError(pSyncNode, "start snapshot error, sender is null");
    return -1;
  }

  int32_t code = 0;

  if (snapshotSenderIsStart(pSender)) {
    code = snapshotSenderStop(pSender, false);
    if (code != 0) {
      sNError(pSyncNode, "snapshot sender stop error");
      return -1;
    }
  }

  code = snapshotSenderStart(pSender);
  if (code != 0) {
    sNError(pSyncNode, "snapshot sender start error");
    return -1;
  }

  return 0;
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
    pReceiver->snapshot.data = NULL;
    pReceiver->snapshot.lastApplyIndex = SYNC_INDEX_INVALID;
    pReceiver->snapshot.lastApplyTerm = 0;
    pReceiver->snapshot.lastConfigIndex = SYNC_INDEX_INVALID;

  } else {
    sError("vgId:%d, cannot create snapshot receiver", pSyncNode->vgId);
  }

  return pReceiver;
}

void snapshotReceiverDestroy(SSyncSnapshotReceiver *pReceiver) {
  if (pReceiver != NULL) {
    // close writer
    if (pReceiver->pWriter != NULL) {
      int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter,
                                                                    false, &(pReceiver->snapshot));
      ASSERT(ret == 0);
      pReceiver->pWriter = NULL;
    }

    // free receiver
    taosMemoryFree(pReceiver);
  }
}

bool snapshotReceiverIsStart(SSyncSnapshotReceiver *pReceiver) { return pReceiver->start; }

// static do start by privateTerm, pBeginMsg
// receive first snapshot data
// write first block data
static void snapshotReceiverDoStart(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pBeginMsg) {
  pReceiver->start = true;
  pReceiver->ack = SYNC_SNAPSHOT_SEQ_BEGIN;

  // start writer
  ASSERT(pReceiver->pWriter == NULL);
  int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStartWrite(pReceiver->pSyncNode->pFsm,
                                                                 &(pReceiver->snapshotParam), &(pReceiver->pWriter));
  ASSERT(ret == 0);

  pReceiver->term = pReceiver->pSyncNode->pRaftStore->currentTerm;
  pReceiver->snapshotParam.start = pBeginMsg->beginIndex;
  pReceiver->snapshotParam.end = pBeginMsg->lastIndex;

  pReceiver->fromId = pBeginMsg->srcId;

  // update snapshot
  pReceiver->snapshot.lastApplyIndex = pBeginMsg->lastIndex;
  pReceiver->snapshot.lastApplyTerm = pBeginMsg->lastTerm;
  pReceiver->snapshot.lastConfigIndex = pBeginMsg->lastConfigIndex;

  pReceiver->startTime = pBeginMsg->startTime;

  // event log
  sRTrace(pReceiver, "snapshot receiver start");
}

// force stop
void snapshotReceiverForceStop(SSyncSnapshotReceiver *pReceiver) {
  // force close, abandon incomplete data
  if (pReceiver->pWriter != NULL) {
    int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, false,
                                                                  &(pReceiver->snapshot));
    ASSERT(ret == 0);
    pReceiver->pWriter = NULL;
  }

  pReceiver->start = false;

  // event log
  sRTrace(pReceiver, "snapshot receiver force stop");
}

// if receiver receive msg from seq = SYNC_SNAPSHOT_SEQ_BEGIN, start receiver
int32_t snapshotReceiverStart(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pBeginMsg) {
  ASSERT(!snapshotReceiverIsStart(pReceiver));
  snapshotReceiverDoStart(pReceiver, pBeginMsg);

  return 0;
}

// just set start = false
// FpSnapshotStopWrite should not be called, assert writer == NULL
int32_t snapshotReceiverStop(SSyncSnapshotReceiver *pReceiver) {
  if (pReceiver->pWriter != NULL) {
    int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, false,
                                                                  &(pReceiver->snapshot));
    ASSERT(ret == 0);
    pReceiver->pWriter = NULL;
  }

  pReceiver->start = false;

  // event log
  sRTrace(pReceiver, "snapshot receiver stop");
  return 0;
}

// when recv last snapshot block, apply data into snapshot
static int32_t snapshotReceiverFinish(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pMsg) {
  ASSERT(pMsg->seq == SYNC_SNAPSHOT_SEQ_END);

  int32_t code = 0;
  if (pReceiver->pWriter != NULL) {
    // write data
    if (pMsg->dataLen > 0) {
      code = pReceiver->pSyncNode->pFsm->FpSnapshotDoWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, pMsg->data,
                                                           pMsg->dataLen);
      if (code != 0) {
        sNError(pReceiver->pSyncNode, "snapshot write error");
        return -1;
      }
    }

    // reset wal
    code =
        pReceiver->pSyncNode->pLogStore->syncLogRestoreFromSnapshot(pReceiver->pSyncNode->pLogStore, pMsg->lastIndex);
    if (code != 0) {
      sNError(pReceiver->pSyncNode, "wal restore from snapshot error");
      return -1;
    }

    // update commit index
    if (pReceiver->snapshot.lastApplyIndex > pReceiver->pSyncNode->commitIndex) {
      pReceiver->pSyncNode->commitIndex = pReceiver->snapshot.lastApplyIndex;
    }

    // maybe update term
    if (pReceiver->snapshot.lastApplyTerm > pReceiver->pSyncNode->pRaftStore->currentTerm) {
      pReceiver->pSyncNode->pRaftStore->currentTerm = pReceiver->snapshot.lastApplyTerm;
      raftStorePersist(pReceiver->pSyncNode->pRaftStore);
    }

    // stop writer, apply data
    code = pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, true,
                                                           &(pReceiver->snapshot));
    if (code != 0) {
      sNError(pReceiver->pSyncNode, "snapshot stop writer true error");
      return -1;
    }
    pReceiver->pWriter = NULL;

    // update progress
    pReceiver->ack = SYNC_SNAPSHOT_SEQ_END;

  } else {
    sNError(pReceiver->pSyncNode, "snapshot stop writer true error");
    return -1;
  }

  // event log
  sRTrace(pReceiver, "snapshot receiver got last data, finish, apply snapshot");
  return 0;
}

// apply data block
// update progress
static void snapshotReceiverGotData(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pMsg) {
  ASSERT(pMsg->seq == pReceiver->ack + 1);

  if (pReceiver->pWriter != NULL) {
    if (pMsg->dataLen > 0) {
      // apply data block
      int32_t code = pReceiver->pSyncNode->pFsm->FpSnapshotDoWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter,
                                                                   pMsg->data, pMsg->dataLen);
      ASSERT(code == 0);
    }

    // update progress
    pReceiver->ack = pMsg->seq;

    // event log
    sRTrace(pReceiver, "snapshot receiver receiving");
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
    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pReceiver->fromId.addr);
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

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pReceiver->snapshot.lastApplyIndex);
    cJSON_AddStringToObject(pRoot, "snapshot.lastApplyIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pReceiver->snapshot.lastApplyTerm);
    cJSON_AddStringToObject(pRoot, "snapshot.lastApplyTerm", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pReceiver->snapshot.lastConfigIndex);
    cJSON_AddStringToObject(pRoot, "snapshot.lastConfigIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRIu64, pReceiver->term);
    cJSON_AddStringToObject(pRoot, "term", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%" PRId64, pReceiver->startTime);
    cJSON_AddStringToObject(pRoot, "startTime", u64buf);
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

SyncIndex syncNodeGetSnapBeginIndex(SSyncNode *ths) {
  SyncIndex snapStart = SYNC_INDEX_INVALID;

  if (syncNodeIsMnode(ths)) {
    snapStart = SYNC_INDEX_BEGIN;

  } else {
    SSyncLogStoreData *pData = ths->pLogStore->data;
    SWal              *pWal = pData->pWal;

    bool    isEmpty = ths->pLogStore->syncLogIsEmpty(ths->pLogStore);
    int64_t walCommitVer = walGetCommittedVer(pWal);

    if (!isEmpty && ths->commitIndex != walCommitVer) {
      sNError(ths, "commit not same, wal-commit:%" PRId64 ", commit:%" PRId64 ", ignore", walCommitVer,
              ths->commitIndex);
      snapStart = walCommitVer + 1;
    } else {
      snapStart = ths->commitIndex + 1;
    }
  }

  return snapStart;
}

static int32_t syncNodeOnSnapshotPre(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;

  if (snapshotReceiverIsStart(pReceiver)) {
    // already start

    if (pMsg->startTime > pReceiver->startTime) {
      goto _START_RECEIVER;

    } else if (pMsg->startTime == pReceiver->startTime) {
      goto _SEND_REPLY;

    } else {
      // ignore
      return 0;
    }

  } else {
    // start new
    goto _START_RECEIVER;
  }

_START_RECEIVER:
  if (taosGetTimestampMs() - pMsg->startTime > SNAPSHOT_MAX_CLOCK_SKEW_MS) {
    sNError(pSyncNode, "snapshot receiver time skew too much");
    return -1;
  } else {
    // waiting for clock match
    while (taosGetTimestampMs() > pMsg->startTime) {
      taosMsleep(10);
    }

    snapshotReceiverStart(pReceiver, pMsg);  // set start-time same with sender
  }

_SEND_REPLY:
    // build msg
    ;  // make complier happy
  SyncSnapshotRsp *pRspMsg = syncSnapshotRspBuild(pSyncNode->vgId);
  pRspMsg->srcId = pSyncNode->myRaftId;
  pRspMsg->destId = pMsg->srcId;
  pRspMsg->term = pSyncNode->pRaftStore->currentTerm;
  pRspMsg->lastIndex = pMsg->lastIndex;
  pRspMsg->lastTerm = pMsg->lastTerm;
  pRspMsg->startTime = pReceiver->startTime;
  pRspMsg->ack = pMsg->seq;  // receiver maybe already closed
  pRspMsg->code = 0;
  pRspMsg->snapBeginIndex = syncNodeGetSnapBeginIndex(pSyncNode);

  // send msg
  SRpcMsg rpcMsg;
  syncSnapshotRsp2RpcMsg(pRspMsg, &rpcMsg);
  syncNodeSendMsgById(&(pRspMsg->destId), pSyncNode, &rpcMsg);
  syncSnapshotRspDestroy(pRspMsg);

  return 0;
}

static int32_t syncNodeOnSnapshotBegin(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  // condition 1
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;

  if (snapshotReceiverIsStart(pReceiver)) {
    if (pMsg->startTime > pReceiver->startTime) {
      snapshotReceiverStop(pReceiver);

    } else if (pMsg->startTime == pReceiver->startTime) {
      return 0;
    } else {
      // ignore
      sNTrace(pSyncNode, "msg ignore");
      return 0;
    }
  }

_START_RECEIVER:
  if (taosGetTimestampMs() - pMsg->startTime > SNAPSHOT_MAX_CLOCK_SKEW_MS) {
    sNError(pSyncNode, "snapshot receiver time skew too much");
    return -1;
  } else {
    // waiting for clock match
    while (taosGetTimestampMs() > pMsg->startTime) {
      taosMsleep(10);
    }

    snapshotReceiverStart(pReceiver, pMsg);

    // build msg
    SyncSnapshotRsp *pRspMsg = syncSnapshotRspBuild(pSyncNode->vgId);
    pRspMsg->srcId = pSyncNode->myRaftId;
    pRspMsg->destId = pMsg->srcId;
    pRspMsg->term = pSyncNode->pRaftStore->currentTerm;
    pRspMsg->lastIndex = pMsg->lastIndex;
    pRspMsg->lastTerm = pMsg->lastTerm;
    pRspMsg->ack = pReceiver->ack;  // receiver maybe already closed
    pRspMsg->code = 0;

    // send msg
    SRpcMsg rpcMsg;
    syncSnapshotRsp2RpcMsg(pRspMsg, &rpcMsg);
    syncNodeSendMsgById(&(pRspMsg->destId), pSyncNode, &rpcMsg);
    syncSnapshotRspDestroy(pRspMsg);
  }

  return 0;
}

static int32_t syncNodeOnSnapshotTransfer(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) { return 0; }

static int32_t syncNodeOnSnapshotEnd(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) { return 0; }

// receiver on message
//
// condition 1, recv SYNC_SNAPSHOT_SEQ_PRE_SNAPSHOT
//              if receiver already start
//                    if sender.start-time > receiver.start-time, restart receiver(reply snapshot start)
//                    if sender.start-time = receiver.start-time, maybe duplicate msg
//                    if sender.start-time < receiver.start-time, ignore
//              else
//                    waiting for clock match
//                    start receiver(reply snapshot start)
//
// condition 2, recv SYNC_SNAPSHOT_SEQ_BEGIN
//              a. create writer with <begin, end>
//
// condition 3, recv SYNC_SNAPSHOT_SEQ_END, finish receiver(apply snapshot data, update commit index, maybe reconfig)
//
// condition 4, recv SYNC_SNAPSHOT_SEQ_FORCE_CLOSE, force close
//
// condition 5, got data, update ack
//
int32_t syncNodeOnSnapshot(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(pSyncNode, &(pMsg->srcId))) {
    syncLogRecvSyncSnapshotSend(pSyncNode, pMsg, "not in my config");
    return 0;
  }

  if (pMsg->term < pSyncNode->pRaftStore->currentTerm) {
    syncLogRecvSyncSnapshotSend(pSyncNode, pMsg, "reject, small term");
    return 0;
  }

  if (pMsg->term > pSyncNode->pRaftStore->currentTerm) {
    syncNodeStepDown(pSyncNode, pMsg->term);
  }
  syncNodeResetElectTimer(pSyncNode);

  int32_t                code = 0;
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;

  // state, term, seq/ack
  if (pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER) {
    if (pMsg->term == pSyncNode->pRaftStore->currentTerm) {
      if (pMsg->seq == SYNC_SNAPSHOT_SEQ_PRE_SNAPSHOT) {
        syncNodeOnSnapshotPre(pSyncNode, pMsg);

      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_BEGIN) {
        syncNodeOnSnapshotBegin(pSyncNode, pMsg);

      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_END) {
        // condition 2
        // end, finish FSM
        code = snapshotReceiverFinish(pReceiver, pMsg);
        if (code == 0) {
          snapshotReceiverStop(pReceiver);
        }
        bool needRsp = true;

        // maybe update lastconfig
        if (pMsg->lastConfigIndex >= SYNC_INDEX_BEGIN) {
          SSyncCfg oldSyncCfg = pSyncNode->pRaftCfg->cfg;

          // update new config myIndex
          SSyncCfg newSyncCfg = pMsg->lastConfig;
          syncNodeUpdateNewConfigIndex(pSyncNode, &newSyncCfg);

          // do config change
          syncNodeDoConfigChange(pSyncNode, &newSyncCfg, pMsg->lastConfigIndex);
        }

      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_FORCE_CLOSE) {
        // condition 3
        // force close
        snapshotReceiverForceStop(pReceiver);
        bool needRsp = false;

      } else if (pMsg->seq > SYNC_SNAPSHOT_SEQ_BEGIN && pMsg->seq < SYNC_SNAPSHOT_SEQ_END) {
        // condition 4
        // transfering
        if (pMsg->seq == pReceiver->ack + 1) {
          snapshotReceiverGotData(pReceiver, pMsg);
        }
        bool needRsp = true;

      } else {
        // error log
        sRTrace(pReceiver, "snapshot receiver recv error seq:%d, my ack:%d", pMsg->seq, pReceiver->ack);
        return -1;
      }

    } else {
      // error log
      sRTrace(pReceiver, "snapshot receiver term not equal");
      return -1;
    }
  } else {
    // error log
    sRTrace(pReceiver, "snapshot receiver not follower");
    return -1;
  }

  return 0;
}

int32_t syncNodeOnSnapshotReplyPre(SSyncNode *pSyncNode, SyncSnapshotRsp *pMsg) {
  // get sender
  SSyncSnapshotSender *pSender = syncNodeGetSnapshotSender(pSyncNode, &(pMsg->srcId));
  ASSERT(pSender != NULL);

  SSnapshot snapshot;
  pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);

  // prepare <begin, end>
  pSender->snapshotParam.start = pMsg->snapBeginIndex;
  pSender->snapshotParam.end = snapshot.lastApplyIndex;

  if (pMsg->snapBeginIndex > snapshot.lastApplyIndex) {
    sNError(pSyncNode, "snapshot last index too small");
    return -1;
  }

  // start reader
  int32_t code = pSyncNode->pFsm->FpSnapshotStartRead(pSyncNode->pFsm, &(pSender->snapshotParam), &(pSender->pReader));
  if (code != 0) {
    sNError(pSyncNode, "create snapshot reader error");
    return -1;
  }

  // build begin msg
  SyncSnapshotSend *pSendMsg = syncSnapshotSendBuild(0, pSender->pSyncNode->vgId);
  pSendMsg->srcId = pSender->pSyncNode->myRaftId;
  pSendMsg->destId = (pSender->pSyncNode->replicasId)[pSender->replicaIndex];
  pSendMsg->term = pSender->pSyncNode->pRaftStore->currentTerm;
  pSendMsg->beginIndex = pSender->snapshotParam.start;
  pSendMsg->lastIndex = pSender->snapshot.lastApplyIndex;
  pSendMsg->lastTerm = pSender->snapshot.lastApplyTerm;
  pSendMsg->lastConfigIndex = pSender->snapshot.lastConfigIndex;
  pSendMsg->lastConfig = pSender->lastConfig;
  pSendMsg->startTime = pSender->startTime;
  pSendMsg->seq = SYNC_SNAPSHOT_SEQ_BEGIN;

  // send msg
  SRpcMsg rpcMsg;
  syncSnapshotSend2RpcMsg(pSendMsg, &rpcMsg);
  syncNodeSendMsgById(&(pSendMsg->destId), pSender->pSyncNode, &rpcMsg);
  syncSnapshotSendDestroy(pSendMsg);

  return 0;
}

// sender on message
//
// condition 1 sender receives SYNC_SNAPSHOT_SEQ_END, close sender
// condition 2 sender receives ack, set seq = ack + 1, send msg from seq
// condition 3 sender receives error msg, just print error log
//
int32_t syncNodeOnSnapshotReply(SSyncNode *pSyncNode, SyncSnapshotRsp *pMsg) {
  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(pSyncNode, &(pMsg->srcId))) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "maybe replica already dropped");
    return -1;
  }

  // get sender
  SSyncSnapshotSender *pSender = syncNodeGetSnapshotSender(pSyncNode, &(pMsg->srcId));
  ASSERT(pSender != NULL);

  if (pMsg->startTime != pSender->startTime) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "sender/receiver start time not match");
    return -1;
  }

  // state, term, seq/ack
  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    if (pMsg->term == pSyncNode->pRaftStore->currentTerm) {
      // prepare <begin, end>, send begin msg
      if (pMsg->ack == SYNC_SNAPSHOT_SEQ_PRE_SNAPSHOT) {
        syncNodeOnSnapshotReplyPre(pSyncNode, pMsg);
        return 0;
      }

      // receive ack is finish, close sender
      if (pMsg->ack == SYNC_SNAPSHOT_SEQ_END) {
        snapshotSenderStop(pSender, true);
        return 0;
      }

      // send next msg
      if (pMsg->ack == pSender->seq) {
        // update sender ack
        snapshotSenderUpdateProgress(pSender, pMsg);
        snapshotSend(pSender);

      } else if (pMsg->ack == pSender->seq - 1) {
        // maybe resend
        snapshotReSend(pSender);

      } else {
        // error log
        sSError(pSender, "snapshot sender recv error ack:%d, my seq:%d", pMsg->ack, pSender->seq);
        return -1;
      }
    } else {
      // error log
      sSError(pSender, "snapshot sender term not equal");
      return -1;
    }
  } else {
    // error log
    sSError(pSender, "snapshot sender not leader");
    return -1;
  }

  return 0;
}

int32_t syncNodeOnPreSnapshot(SSyncNode *ths, SyncPreSnapshot *pMsg) {
  syncLogRecvSyncPreSnapshot(ths, pMsg, "");

  SyncPreSnapshotReply *pMsgReply = syncPreSnapshotReplyBuild(ths->vgId);
  pMsgReply->srcId = ths->myRaftId;
  pMsgReply->destId = pMsg->srcId;
  pMsgReply->term = ths->pRaftStore->currentTerm;

  SSyncLogStoreData *pData = ths->pLogStore->data;
  SWal              *pWal = pData->pWal;

  if (syncNodeIsMnode(ths)) {
    pMsgReply->snapStart = SYNC_INDEX_BEGIN;

  } else {
    bool    isEmpty = ths->pLogStore->syncLogIsEmpty(ths->pLogStore);
    int64_t walCommitVer = walGetCommittedVer(pWal);

    if (!isEmpty && ths->commitIndex != walCommitVer) {
      sNError(ths, "commit not same, wal-commit:%" PRId64 ", commit:%" PRId64 ", ignore", walCommitVer,
              ths->commitIndex);
      goto _IGNORE;
    }

    pMsgReply->snapStart = ths->commitIndex + 1;

    // make local log clean
    int32_t code = ths->pLogStore->syncLogTruncate(ths->pLogStore, pMsgReply->snapStart);
    if (code != 0) {
      sNError(ths, "truncate wal error");
      goto _IGNORE;
    }
  }

  // can not write behind _RESPONSE
  SRpcMsg rpcMsg;

_RESPONSE:
  syncPreSnapshotReply2RpcMsg(pMsgReply, &rpcMsg);
  syncNodeSendMsgById(&pMsgReply->destId, ths, &rpcMsg);

  syncPreSnapshotReplyDestroy(pMsgReply);
  return 0;

_IGNORE:
  syncPreSnapshotReplyDestroy(pMsgReply);
  return 0;
}

int32_t syncNodeOnPreSnapshotReply(SSyncNode *ths, SyncPreSnapshotReply *pMsg) {
  syncLogRecvSyncPreSnapshotReply(ths, pMsg, "");

  // start snapshot

  return 0;
}