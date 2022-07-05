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
static void    snapshotReceiverForceStop(SSyncSnapshotReceiver *pReceiver);
static void    snapshotReceiverGotData(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pMsg);
static int32_t snapshotReceiverFinish(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pMsg);

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
        syncNodeErrorLog(pSender->pSyncNode, "stop reader error");
      }
      pSender->pReader = NULL;
    }

    // free sender
    taosMemoryFree(pSender);
  }
}

bool snapshotSenderIsStart(SSyncSnapshotSender *pSender) { return pSender->start; }

// begin send snapshot by param, snapshot, pReader
//
// action:
// 1. assert reader not start
// 2. update state
// 3. send first snapshot block
int32_t snapshotSenderStart(SSyncSnapshotSender *pSender, SSnapshotParam snapshotParam, SSnapshot snapshot,
                            void *pReader) {
  ASSERT(!snapshotSenderIsStart(pSender));

  // init snapshot, parm, reader
  ASSERT(pSender->pReader == NULL);
  pSender->pReader = pReader;
  pSender->snapshot = snapshot;
  pSender->snapshotParam = snapshotParam;

  // init current block
  if (pSender->pCurrentBlock != NULL) {
    taosMemoryFree(pSender->pCurrentBlock);
  }
  pSender->blockLen = 0;

  // update term
  pSender->term = pSender->pSyncNode->pRaftStore->currentTerm;
  ++(pSender->privateTerm);  // increase private term

  // update state
  pSender->finish = false;
  pSender->start = true;
  pSender->seq = SYNC_SNAPSHOT_SEQ_BEGIN;
  pSender->ack = SYNC_SNAPSHOT_SEQ_INVALID;

  // init last config
  if (pSender->snapshot.lastConfigIndex != SYNC_INDEX_INVALID) {
    int32_t         code = 0;
    SSyncRaftEntry *pEntry = NULL;
    bool            getLastConfig = false;

    code = pSender->pSyncNode->pLogStore->syncLogGetEntry(pSender->pSyncNode->pLogStore,
                                                          pSender->snapshot.lastConfigIndex, &pEntry);
    if (code == 0 && pEntry != NULL) {
      SRpcMsg rpcMsg;
      syncEntry2OriginalRpc(pEntry, &rpcMsg);

      SSyncCfg lastConfig;
      int32_t  ret = syncCfgFromStr(rpcMsg.pCont, &lastConfig);
      ASSERT(ret == 0);
      pSender->lastConfig = lastConfig;
      getLastConfig = true;

      rpcFreeCont(rpcMsg.pCont);
      syncEntryDestory(pEntry);
    } else {
      if (pSender->snapshot.lastConfigIndex == pSender->pSyncNode->pRaftCfg->lastConfigIndex) {
        sTrace("vgId:%d, sync sender get cfg from local", pSender->pSyncNode->vgId);
        pSender->lastConfig = pSender->pSyncNode->pRaftCfg->cfg;
        getLastConfig = true;
      }
    }

    // last config not found in wal, update to -1
    if (!getLastConfig) {
      SyncIndex oldLastConfigIndex = pSender->snapshot.lastConfigIndex;
      SyncIndex newLastConfigIndex = SYNC_INDEX_INVALID;
      pSender->snapshot.lastConfigIndex = SYNC_INDEX_INVALID;
      memset(&(pSender->lastConfig), 0, sizeof(SSyncCfg));

      // event log
      do {
        char logBuf[128];
        snprintf(logBuf, sizeof(logBuf), "snapshot sender update lcindex from %ld to %ld", oldLastConfigIndex,
                 newLastConfigIndex);
        char *eventLog = snapshotSender2SimpleStr(pSender, logBuf);
        syncNodeEventLog(pSender->pSyncNode, eventLog);
        taosMemoryFree(eventLog);
      } while (0);
    }

  } else {
    // no last config
    memset(&(pSender->lastConfig), 0, sizeof(SSyncCfg));
  }

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
  pMsg->seq = pSender->seq;  // SYNC_SNAPSHOT_SEQ_BEGIN
  pMsg->privateTerm = pSender->privateTerm;

  // send msg
  SRpcMsg rpcMsg;
  syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(&(pMsg->destId), pSender->pSyncNode, &rpcMsg);
  syncSnapshotSendDestroy(pMsg);

  // event log
  do {
    char *eventLog = snapshotSender2SimpleStr(pSender, "snapshot sender start");
    syncNodeEventLog(pSender->pSyncNode, eventLog);
    taosMemoryFree(eventLog);
  } while (0);

  return 0;
}

int32_t snapshotSenderStop(SSyncSnapshotSender *pSender, bool finish) {
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

  // update flag
  pSender->start = false;
  pSender->finish = finish;

  // do not update term, maybe print

  // event log
  do {
    char *eventLog = snapshotSender2SimpleStr(pSender, "snapshot sender stop");
    syncNodeEventLog(pSender->pSyncNode, eventLog);
    taosMemoryFree(eventLog);
  } while (0);

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
  pMsg->privateTerm = pSender->privateTerm;
  memcpy(pMsg->data, pSender->pCurrentBlock, pSender->blockLen);

  // send msg
  SRpcMsg rpcMsg;
  syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
  syncNodeSendMsgById(&(pMsg->destId), pSender->pSyncNode, &rpcMsg);
  syncSnapshotSendDestroy(pMsg);

  // event log
  do {
    char *eventLog = NULL;
    if (pSender->seq == SYNC_SNAPSHOT_SEQ_END) {
      eventLog = snapshotSender2SimpleStr(pSender, "snapshot sender finish");
    } else {
      eventLog = snapshotSender2SimpleStr(pSender, "snapshot sender sending");
    }
    syncNodeEventLog(pSender->pSyncNode, eventLog);
    taosMemoryFree(eventLog);
  } while (0);

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
    pMsg->privateTerm = pSender->privateTerm;
    memcpy(pMsg->data, pSender->pCurrentBlock, pSender->blockLen);

    // send msg
    SRpcMsg rpcMsg;
    syncSnapshotSend2RpcMsg(pMsg, &rpcMsg);
    syncNodeSendMsgById(&(pMsg->destId), pSender->pSyncNode, &rpcMsg);
    syncSnapshotSendDestroy(pMsg);

    // event log
    do {
      char *eventLog = snapshotSender2SimpleStr(pSender, "snapshot sender resend");
      syncNodeEventLog(pSender->pSyncNode, eventLog);
      taosMemoryFree(eventLog);
    } while (0);
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

char *snapshotSender2SimpleStr(SSyncSnapshotSender *pSender, char *event) {
  int32_t len = 256;
  char   *s = taosMemoryMalloc(len);

  SRaftId  destId = pSender->pSyncNode->replicasId[pSender->replicaIndex];
  char     host[64];
  uint16_t port;
  syncUtilU642Addr(destId.addr, host, sizeof(host), &port);

  snprintf(s, len,
           "%s {%p s-param:%ld e-param:%ld laindex:%ld laterm:%lu lcindex:%ld seq:%d ack:%d finish:%d pterm:%lu "
           "replica-index:%d %s:%d}",
           event, pSender, pSender->snapshotParam.start, pSender->snapshotParam.end, pSender->snapshot.lastApplyIndex,
           pSender->snapshot.lastApplyTerm, pSender->snapshot.lastConfigIndex, pSender->seq, pSender->ack,
           pSender->finish, pSender->privateTerm, pSender->replicaIndex, host, port);

  return s;
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
      int32_t ret =
          pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, false);
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
  // update state
  pReceiver->term = pReceiver->pSyncNode->pRaftStore->currentTerm;
  pReceiver->privateTerm = pBeginMsg->privateTerm;
  pReceiver->ack = SYNC_SNAPSHOT_SEQ_BEGIN;
  pReceiver->fromId = pBeginMsg->srcId;
  pReceiver->start = true;

  // update snapshot
  pReceiver->snapshot.lastApplyIndex = pBeginMsg->lastIndex;
  pReceiver->snapshot.lastApplyTerm = pBeginMsg->lastTerm;
  pReceiver->snapshot.lastConfigIndex = pBeginMsg->lastConfigIndex;
  pReceiver->snapshotParam.start = pBeginMsg->beginIndex;
  pReceiver->snapshotParam.end = pBeginMsg->lastIndex;

  // start writer
  ASSERT(pReceiver->pWriter == NULL);
  int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStartWrite(pReceiver->pSyncNode->pFsm,
                                                                 &(pReceiver->snapshotParam), &(pReceiver->pWriter));
  ASSERT(ret == 0);

  // event log
  do {
    char *eventLog = snapshotReceiver2SimpleStr(pReceiver, "snapshot receiver start");
    syncNodeEventLog(pReceiver->pSyncNode, eventLog);
    taosMemoryFree(eventLog);
  } while (0);
}

// force stop
static void snapshotReceiverForceStop(SSyncSnapshotReceiver *pReceiver) {
  // force close, abandon incomplete data
  if (pReceiver->pWriter != NULL) {
    int32_t ret =
        pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, false);
    ASSERT(ret == 0);
    pReceiver->pWriter = NULL;
  }

  pReceiver->start = false;

  // event log
  do {
    char *eventLog = snapshotReceiver2SimpleStr(pReceiver, "snapshot receiver force stop");
    syncNodeEventLog(pReceiver->pSyncNode, eventLog);
    taosMemoryFree(eventLog);
  } while (0);
}

// if receiver receive msg from seq = SYNC_SNAPSHOT_SEQ_BEGIN, start receiver
// if already start, force close, start again
int32_t snapshotReceiverStart(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pBeginMsg) {
  if (!snapshotReceiverIsStart(pReceiver)) {
    // first start
    snapshotReceiverDoStart(pReceiver, pBeginMsg);

  } else {
    // already start
    sInfo("vgId:%d, snapshot recv, receiver already start", pReceiver->pSyncNode->vgId);

    // force close, abandon incomplete data
    snapshotReceiverForceStop(pReceiver);

    // start again
    snapshotReceiverDoStart(pReceiver, pBeginMsg);
  }

  return 0;
}

// just set start = false
// FpSnapshotStopWrite should not be called, assert writer == NULL
int32_t snapshotReceiverStop(SSyncSnapshotReceiver *pReceiver) {
  if (pReceiver->pWriter != NULL) {
    int32_t ret =
        pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, false);
    ASSERT(ret == 0);
    pReceiver->pWriter = NULL;
  }

  pReceiver->start = false;

  // event log
  do {
    SSnapshot snapshot;
    pReceiver->pSyncNode->pFsm->FpGetSnapshotInfo(pReceiver->pSyncNode->pFsm, &snapshot);
    char *eventLog = snapshotReceiver2SimpleStr(pReceiver, "snapshot receiver stop");
    syncNodeEventLog(pReceiver->pSyncNode, eventLog);
    taosMemoryFree(eventLog);
  } while (0);

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
        syncNodeErrorLog(pReceiver->pSyncNode, "snapshot write error");
        return -1;
      }
    }

    // reset wal
    code =
        pReceiver->pSyncNode->pLogStore->syncLogRestoreFromSnapshot(pReceiver->pSyncNode->pLogStore, pMsg->lastIndex);
    if (code != 0) {
      syncNodeErrorLog(pReceiver->pSyncNode, "wal restore from snapshot error");
      return -1;
    }

    // update commit index
    if (pReceiver->snapshot.lastApplyIndex > pReceiver->pSyncNode->commitIndex) {
      pReceiver->pSyncNode->commitIndex = pReceiver->snapshot.lastApplyIndex;
    }

    // stop writer, apply data
    code = pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, true);
    if (code != 0) {
      syncNodeErrorLog(pReceiver->pSyncNode, "snapshot stop writer true error");
      ASSERT(0);
      return -1;
    }
    pReceiver->pWriter = NULL;

    // update progress
    pReceiver->ack = SYNC_SNAPSHOT_SEQ_END;

  } else {
    syncNodeErrorLog(pReceiver->pSyncNode, "snapshot stop writer true error");
    return -1;
  }

  // event log
  do {
    SSnapshot snapshot;
    pReceiver->pSyncNode->pFsm->FpGetSnapshotInfo(pReceiver->pSyncNode->pFsm, &snapshot);
    char *eventLog = snapshotReceiver2SimpleStr(pReceiver, "snapshot receiver got last data, finish, apply snapshot");
    syncNodeEventLog(pReceiver->pSyncNode, eventLog);
    taosMemoryFree(eventLog);
  } while (0);

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
    do {
      char *eventLog = snapshotReceiver2SimpleStr(pReceiver, "snapshot receiver receiving");
      syncNodeEventLog(pReceiver->pSyncNode, eventLog);
      taosMemoryFree(eventLog);
    } while (0);
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

    snprintf(u64buf, sizeof(u64buf), "%lu", pReceiver->snapshot.lastApplyIndex);
    cJSON_AddStringToObject(pRoot, "snapshot.lastApplyIndex", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%lu", pReceiver->snapshot.lastApplyTerm);
    cJSON_AddStringToObject(pRoot, "snapshot.lastApplyTerm", u64buf);

    snprintf(u64buf, sizeof(u64buf), "%lu", pReceiver->snapshot.lastConfigIndex);
    cJSON_AddStringToObject(pRoot, "snapshot.lastConfigIndex", u64buf);

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

char *snapshotReceiver2SimpleStr(SSyncSnapshotReceiver *pReceiver, char *event) {
  int32_t len = 256;
  char   *s = taosMemoryMalloc(len);

  SRaftId  fromId = pReceiver->fromId;
  char     host[128];
  uint16_t port;
  syncUtilU642Addr(fromId.addr, host, sizeof(host), &port);

  snprintf(s, len,
           "%s {%p start:%d ack:%d term:%lu pterm:%lu from:%s:%d s-param:%ld e-param:%ld laindex:%ld laterm:%lu "
           "lcindex:%ld}",
           event, pReceiver, pReceiver->start, pReceiver->ack, pReceiver->term, pReceiver->privateTerm, host, port,
           pReceiver->snapshotParam.start, pReceiver->snapshotParam.end, pReceiver->snapshot.lastApplyIndex,
           pReceiver->snapshot.lastApplyTerm, pReceiver->snapshot.lastConfigIndex);

  return s;
}

// receiver on message
//
// condition 1, recv SYNC_SNAPSHOT_SEQ_BEGIN, start receiver, update privateTerm
// condition 2, recv SYNC_SNAPSHOT_SEQ_END, finish receiver(apply snapshot data, update commit index, maybe reconfig)
// condition 3, recv SYNC_SNAPSHOT_SEQ_FORCE_CLOSE, force close
// condition 4, got data, update ack
//
int32_t syncNodeOnSnapshotSendCb(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  // get receiver
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;
  bool                   needRsp = false;
  int32_t                code = 0;

  // state, term, seq/ack
  if (pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER) {
    if (pMsg->term == pSyncNode->pRaftStore->currentTerm) {
      if (pMsg->seq == SYNC_SNAPSHOT_SEQ_BEGIN) {
        // condition 1
        // begin, no data
        snapshotReceiverStart(pReceiver, pMsg);
        needRsp = true;

      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_END) {
        // condition 2
        // end, finish FSM
        code = snapshotReceiverFinish(pReceiver, pMsg);
        if (code == 0) {
          snapshotReceiverStop(pReceiver);
        }
        needRsp = true;

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
        needRsp = false;

      } else if (pMsg->seq > SYNC_SNAPSHOT_SEQ_BEGIN && pMsg->seq < SYNC_SNAPSHOT_SEQ_END) {
        // condition 4
        // transfering
        if (pMsg->seq == pReceiver->ack + 1) {
          snapshotReceiverGotData(pReceiver, pMsg);
        }
        needRsp = true;

      } else {
        // error log
        do {
          char logBuf[96];
          snprintf(logBuf, sizeof(logBuf), "snapshot receiver recv error seq:%d, my ack:%d", pMsg->seq, pReceiver->ack);
          char *eventLog = snapshotReceiver2SimpleStr(pReceiver, logBuf);
          syncNodeErrorLog(pSyncNode, eventLog);
          taosMemoryFree(eventLog);
        } while (0);

        return -1;
      }

      // send ack
      if (needRsp) {
        // build msg
        SyncSnapshotRsp *pRspMsg = syncSnapshotRspBuild(pSyncNode->vgId);
        pRspMsg->srcId = pSyncNode->myRaftId;
        pRspMsg->destId = pMsg->srcId;
        pRspMsg->term = pSyncNode->pRaftStore->currentTerm;
        pRspMsg->lastIndex = pMsg->lastIndex;
        pRspMsg->lastTerm = pMsg->lastTerm;
        pRspMsg->ack = pReceiver->ack;  // receiver maybe already closed
        pRspMsg->code = 0;
        pRspMsg->privateTerm = pReceiver->privateTerm;  // receiver maybe already closed

        // send msg
        SRpcMsg rpcMsg;
        syncSnapshotRsp2RpcMsg(pRspMsg, &rpcMsg);
        syncNodeSendMsgById(&(pRspMsg->destId), pSyncNode, &rpcMsg);
        syncSnapshotRspDestroy(pRspMsg);
      }

    } else {
      // error log
      do {
        char *eventLog = snapshotReceiver2SimpleStr(pReceiver, "snapshot receiver term not equal");
        syncNodeErrorLog(pSyncNode, eventLog);
        taosMemoryFree(eventLog);
      } while (0);

      return -1;
    }
  } else {
    // error log
    do {
      char *eventLog = snapshotReceiver2SimpleStr(pReceiver, "snapshot receiver not follower");
      syncNodeErrorLog(pSyncNode, eventLog);
      taosMemoryFree(eventLog);
    } while (0);

    return -1;
  }

  return 0;
}

// sender on message
//
// condition 1 sender receives SYNC_SNAPSHOT_SEQ_END, close sender
// condition 2 sender receives ack, set seq = ack + 1, send msg from seq
// condition 3 sender receives error msg, just print error log
//
int32_t syncNodeOnSnapshotRspCb(SSyncNode *pSyncNode, SyncSnapshotRsp *pMsg) {
  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(pSyncNode, &(pMsg->srcId)) && pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    sError("vgId:%d, recv sync-snapshot-rsp, maybe replica already dropped", pSyncNode->vgId);
    return -1;
  }

  // get sender
  SSyncSnapshotSender *pSender = syncNodeGetSnapshotSender(pSyncNode, &(pMsg->srcId));
  ASSERT(pSender != NULL);

  // state, term, seq/ack
  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    if (pMsg->term == pSyncNode->pRaftStore->currentTerm) {
      // condition 1
      // receive ack is finish, close sender
      if (pMsg->ack == SYNC_SNAPSHOT_SEQ_END) {
        snapshotSenderStop(pSender, true);
        return 0;
      }

      // condition 2
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
        do {
          char logBuf[96];
          snprintf(logBuf, sizeof(logBuf), "snapshot sender recv error ack:%d, my seq:%d", pMsg->ack, pSender->seq);
          char *eventLog = snapshotSender2SimpleStr(pSender, logBuf);
          syncNodeErrorLog(pSyncNode, eventLog);
          taosMemoryFree(eventLog);
        } while (0);

        return -1;
      }
    } else {
      // error log
      do {
        char *eventLog = snapshotSender2SimpleStr(pSender, "snapshot sender term not equal");
        syncNodeErrorLog(pSyncNode, eventLog);
        taosMemoryFree(eventLog);
      } while (0);

      return -1;
    }
  } else {
    // error log
    do {
      char *eventLog = snapshotSender2SimpleStr(pSender, "snapshot sender not leader");
      syncNodeErrorLog(pSyncNode, eventLog);
      taosMemoryFree(eventLog);
    } while (0);

    return -1;
  }

  return 0;
}
