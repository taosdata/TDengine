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
#include "syncSnapshot.h"
#include "syncIndexMgr.h"
#include "syncPipeline.h"
#include "syncRaftCfg.h"
#include "syncRaftLog.h"
#include "syncRaftStore.h"
#include "syncReplication.h"
#include "syncUtil.h"

SSyncSnapshotSender *snapshotSenderCreate(SSyncNode *pSyncNode, int32_t replicaIndex) {
  bool condition = (pSyncNode->pFsm->FpSnapshotStartRead != NULL) && (pSyncNode->pFsm->FpSnapshotStopRead != NULL) &&
                   (pSyncNode->pFsm->FpSnapshotDoRead != NULL);

  SSyncSnapshotSender *pSender = NULL;
  if (condition) {
    pSender = taosMemoryCalloc(1, sizeof(SSyncSnapshotSender));
    if (pSender == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }

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
    pSender->endTime = 0;
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
  tAssert(!snapshotSenderIsStart(pSender));

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
  pSender->lastSendTime = pSender->startTime;
  pSender->finish = false;

  // build begin msg
  SRpcMsg rpcMsg = {0};
  (void)syncBuildSnapshotSend(&rpcMsg, 0, pSender->pSyncNode->vgId);

  SyncSnapshotSend *pMsg = rpcMsg.pCont;
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
  syncNodeSendMsgById(&pMsg->destId, pSender->pSyncNode, &rpcMsg);
  syncLogSendSyncSnapshotSend(pSender->pSyncNode, pMsg, "");

  // event log
  sSTrace(pSender, "snapshot sender start");
  return 0;
}

int32_t snapshotSenderStop(SSyncSnapshotSender *pSender, bool finish) {
  // update flag
  pSender->start = false;
  pSender->finish = finish;
  pSender->endTime = taosGetTimestampMs();

  // close reader
  if (pSender->pReader != NULL) {
    int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotStopRead(pSender->pSyncNode->pFsm, pSender->pReader);
    tAssert(ret == 0);
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
  tAssert(ret == 0);
  if (pSender->blockLen > 0) {
    // has read data
  } else {
    // read finish, update seq to end
    pSender->seq = SYNC_SNAPSHOT_SEQ_END;
  }

  // build msg
  SRpcMsg rpcMsg = {0};
  (void)syncBuildSnapshotSend(&rpcMsg, pSender->blockLen, pSender->pSyncNode->vgId);

  SyncSnapshotSend *pMsg = rpcMsg.pCont;
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

  if (pSender->pCurrentBlock != NULL) {
    memcpy(pMsg->data, pSender->pCurrentBlock, pSender->blockLen);
  }

  // send msg
  syncNodeSendMsgById(&pMsg->destId, pSender->pSyncNode, &rpcMsg);
  syncLogSendSyncSnapshotSend(pSender->pSyncNode, pMsg, "");

  pSender->lastSendTime = taosGetTimestampMs();

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

  // build msg
  SRpcMsg rpcMsg = {0};
  (void)syncBuildSnapshotSend(&rpcMsg, pSender->blockLen, pSender->pSyncNode->vgId);

  SyncSnapshotSend *pMsg = rpcMsg.pCont;
  pMsg->srcId = pSender->pSyncNode->myRaftId;
  pMsg->destId = (pSender->pSyncNode->replicasId)[pSender->replicaIndex];
  pMsg->term = pSender->pSyncNode->pRaftStore->currentTerm;
  pMsg->beginIndex = pSender->snapshotParam.start;
  pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
  pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
  pMsg->lastConfigIndex = pSender->snapshot.lastConfigIndex;
  pMsg->lastConfig = pSender->lastConfig;
  pMsg->seq = pSender->seq;

  if (pSender->pCurrentBlock != NULL && pSender->blockLen > 0) {
    //  pMsg->privateTerm = pSender->privateTerm;
    memcpy(pMsg->data, pSender->pCurrentBlock, pSender->blockLen);
  }

  // send msg
  syncNodeSendMsgById(&pMsg->destId, pSender->pSyncNode, &rpcMsg);
  syncLogSendSyncSnapshotSend(pSender->pSyncNode, pMsg, "");

  pSender->lastSendTime = taosGetTimestampMs();

  // event log
  sSTrace(pSender, "snapshot sender resend");

  return 0;
}

static void snapshotSenderUpdateProgress(SSyncSnapshotSender *pSender, SyncSnapshotRsp *pMsg) {
  tAssert(pMsg->ack == pSender->seq);
  pSender->ack = pMsg->ack;
  ++(pSender->seq);
}

// return 0, start ok
// return 1, last snapshot finish ok
// return -1, error
int32_t syncNodeStartSnapshot(SSyncNode *pSyncNode, SRaftId *pDestId) {
  sNTrace(pSyncNode, "starting snapshot ...");

  SSyncSnapshotSender *pSender = syncNodeGetSnapshotSender(pSyncNode, pDestId);
  if (pSender == NULL) {
    sNError(pSyncNode, "start snapshot error, sender is null");
    return -1;
  }

  int32_t code = 0;

  if (snapshotSenderIsStart(pSender)) {
    sNTrace(pSyncNode, "snapshot sender already start, ignore");
    return 0;
  }

  if (!snapshotSenderIsStart(pSender) && pSender->finish &&
      taosGetTimestampMs() - pSender->endTime < SNAPSHOT_WAIT_MS) {
    sNTrace(pSyncNode, "snapshot sender too frequently, ignore");
    return 1;
  }

  char     host[64];
  uint16_t port;
  syncUtilU642Addr(pDestId->addr, host, sizeof(host), &port);
  sInfo("vgId:%d, start snapshot for peer: %s:%d", pSyncNode->vgId, host, port);

  code = snapshotSenderStart(pSender);
  if (code != 0) {
    sNError(pSyncNode, "snapshot sender start error");
    return -1;
  }

  return 0;
}

SSyncSnapshotReceiver *snapshotReceiverCreate(SSyncNode *pSyncNode, SRaftId fromId) {
  bool condition = (pSyncNode->pFsm->FpSnapshotStartWrite != NULL) && (pSyncNode->pFsm->FpSnapshotStopWrite != NULL) &&
                   (pSyncNode->pFsm->FpSnapshotDoWrite != NULL);

  SSyncSnapshotReceiver *pReceiver = NULL;
  if (condition) {
    pReceiver = taosMemoryCalloc(1, sizeof(SSyncSnapshotReceiver));
    if (pReceiver == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return NULL;
    }

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
      tAssert(ret == 0);
      pReceiver->pWriter = NULL;
    }

    // free receiver
    taosMemoryFree(pReceiver);
  }
}

bool snapshotReceiverIsStart(SSyncSnapshotReceiver *pReceiver) { return pReceiver->start; }

// force stop
void snapshotReceiverForceStop(SSyncSnapshotReceiver *pReceiver) {
  // force close, abandon incomplete data
  if (pReceiver->pWriter != NULL) {
    int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, false,
                                                                  &(pReceiver->snapshot));
    tAssert(ret == 0);
    pReceiver->pWriter = NULL;
  }

  pReceiver->start = false;

  // event log
  sRTrace(pReceiver, "snapshot receiver force stop");
}

int32_t snapshotReceiverStartWriter(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pBeginMsg) {
  tAssert(snapshotReceiverIsStart(pReceiver));

  // update ack
  pReceiver->ack = SYNC_SNAPSHOT_SEQ_BEGIN;

  // update snapshot
  pReceiver->snapshot.lastApplyIndex = pBeginMsg->lastIndex;
  pReceiver->snapshot.lastApplyTerm = pBeginMsg->lastTerm;
  pReceiver->snapshot.lastConfigIndex = pBeginMsg->lastConfigIndex;

  pReceiver->snapshotParam.start = pBeginMsg->beginIndex;
  pReceiver->snapshotParam.end = pBeginMsg->lastIndex;

  // start writer
  tAssert(pReceiver->pWriter == NULL);
  int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStartWrite(pReceiver->pSyncNode->pFsm,
                                                                 &(pReceiver->snapshotParam), &(pReceiver->pWriter));
  tAssert(ret == 0);

  // event log
  sRTrace(pReceiver, "snapshot receiver start writer");

  return 0;
}

int32_t snapshotReceiverStart(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pPreMsg) {
  if (snapshotReceiverIsStart(pReceiver)) {
    sWarn("vgId:%d, snapshot receiver has started.", pReceiver->pSyncNode->vgId);
    return 0;
  }

  pReceiver->start = true;
  pReceiver->ack = SYNC_SNAPSHOT_SEQ_PRE_SNAPSHOT;
  pReceiver->term = pReceiver->pSyncNode->pRaftStore->currentTerm;
  pReceiver->fromId = pPreMsg->srcId;
  pReceiver->startTime = pPreMsg->startTime;

  // event log
  sRTrace(pReceiver, "snapshot receiver start");

  return 0;
}

// just set start = false
// FpSnapshotStopWrite should not be called, assert writer == NULL
int32_t snapshotReceiverStop(SSyncSnapshotReceiver *pReceiver) {
  if (pReceiver->pWriter != NULL) {
    int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, false,
                                                                  &(pReceiver->snapshot));
    tAssert(ret == 0);
    pReceiver->pWriter = NULL;
  }

  pReceiver->start = false;

  // event log
  sRTrace(pReceiver, "snapshot receiver stop");
  return 0;
}

// when recv last snapshot block, apply data into snapshot
static int32_t snapshotReceiverFinish(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pMsg) {
  tAssert(pMsg->seq == SYNC_SNAPSHOT_SEQ_END);

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
  tAssert(pMsg->seq == pReceiver->ack + 1);

  if (pReceiver->pWriter != NULL) {
    if (pMsg->dataLen > 0) {
      // apply data block
      int32_t code = pReceiver->pSyncNode->pFsm->FpSnapshotDoWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter,
                                                                   pMsg->data, pMsg->dataLen);
      tAssert(code == 0);
    }

    // update progress
    pReceiver->ack = pMsg->seq;

    // event log
    sRTrace(pReceiver, "snapshot receiver receiving");
  }
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
    int64_t timeNow = taosGetTimestampMs();
    while (timeNow < pMsg->startTime) {
      sNTrace(pSyncNode, "snapshot receiver pre waitting for true time, now:%" PRId64 ", stime:%" PRId64, timeNow,
              pMsg->startTime);
      taosMsleep(10);
      timeNow = taosGetTimestampMs();
    }

    if (snapshotReceiverIsStart(pReceiver)) {
      snapshotReceiverForceStop(pReceiver);
    }

    snapshotReceiverStart(pReceiver, pMsg);  // set start-time same with sender
  }

_SEND_REPLY:
    // build msg
    ;  // make complier happy

  SRpcMsg rpcMsg = {0};
  (void)syncBuildSnapshotSendRsp(&rpcMsg, pSyncNode->vgId);

  SyncSnapshotRsp *pRspMsg = rpcMsg.pCont;
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
  syncNodeSendMsgById(&pRspMsg->destId, pSyncNode, &rpcMsg);
  syncLogSendSyncSnapshotRsp(pSyncNode, pRspMsg, "");
  return 0;
}

static int32_t syncNodeOnSnapshotBegin(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  // condition 1
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;

  if (!snapshotReceiverIsStart(pReceiver)) {
    sNError(pSyncNode, "snapshot receiver not start");
    return -1;
  }

  if (pReceiver->startTime != pMsg->startTime) {
    sNError(pSyncNode, "snapshot receiver time not equal");
    return -1;
  }

  // start writer
  snapshotReceiverStartWriter(pReceiver, pMsg);

  // build msg
  SRpcMsg rpcMsg = {0};
  (void)syncBuildSnapshotSendRsp(&rpcMsg, pSyncNode->vgId);

  SyncSnapshotRsp *pRspMsg = rpcMsg.pCont;
  pRspMsg->srcId = pSyncNode->myRaftId;
  pRspMsg->destId = pMsg->srcId;
  pRspMsg->term = pSyncNode->pRaftStore->currentTerm;
  pRspMsg->lastIndex = pMsg->lastIndex;
  pRspMsg->lastTerm = pMsg->lastTerm;
  pRspMsg->startTime = pReceiver->startTime;
  pRspMsg->ack = pReceiver->ack;  // receiver maybe already closed
  pRspMsg->code = 0;
  pRspMsg->snapBeginIndex = pReceiver->snapshotParam.start;

  // send msg
  syncNodeSendMsgById(&pRspMsg->destId, pSyncNode, &rpcMsg);
  syncLogSendSyncSnapshotRsp(pSyncNode, pRspMsg, "");
  return 0;
}

static int32_t syncNodeOnSnapshotTransfering(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  // condition 4
  // transfering
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;

  // waiting for clock match
  int64_t timeNow = taosGetTimestampMs();
  while (timeNow < pMsg->startTime) {
    sNTrace(pSyncNode, "snapshot receiver transfering waitting for true time, now:%" PRId64 ", stime:%" PRId64, timeNow,
            pMsg->startTime);
    taosMsleep(10);
  }

  if (pMsg->seq == pReceiver->ack + 1) {
    snapshotReceiverGotData(pReceiver, pMsg);
  }

  // build msg
  SRpcMsg rpcMsg = {0};
  (void)syncBuildSnapshotSendRsp(&rpcMsg, pSyncNode->vgId);

  SyncSnapshotRsp *pRspMsg = rpcMsg.pCont;
  pRspMsg->srcId = pSyncNode->myRaftId;
  pRspMsg->destId = pMsg->srcId;
  pRspMsg->term = pSyncNode->pRaftStore->currentTerm;
  pRspMsg->lastIndex = pMsg->lastIndex;
  pRspMsg->lastTerm = pMsg->lastTerm;
  pRspMsg->startTime = pReceiver->startTime;
  pRspMsg->ack = pReceiver->ack;  // receiver maybe already closed
  pRspMsg->code = 0;
  pRspMsg->snapBeginIndex = pReceiver->snapshotParam.start;

  // send msg
  syncNodeSendMsgById(&pRspMsg->destId, pSyncNode, &rpcMsg);
  syncLogSendSyncSnapshotRsp(pSyncNode, pRspMsg, "");
  return 0;
}

static int32_t syncNodeOnSnapshotEnd(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  // condition 2
  // end, finish FSM
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;

  // waiting for clock match
  int64_t timeNow = taosGetTimestampMs();
  while (timeNow < pMsg->startTime) {
    sNTrace(pSyncNode, "snapshot receiver finish waitting for true time, now:%" PRId64 ", stime:%" PRId64, timeNow,
            pMsg->startTime);
    taosMsleep(10);
  }

  int32_t code = snapshotReceiverFinish(pReceiver, pMsg);
  if (code == 0) {
    snapshotReceiverStop(pReceiver);
  }

  // build msg
  SRpcMsg rpcMsg = {0};
  (void)syncBuildSnapshotSendRsp(&rpcMsg, pSyncNode->vgId);

  SyncSnapshotRsp *pRspMsg = rpcMsg.pCont;
  pRspMsg->srcId = pSyncNode->myRaftId;
  pRspMsg->destId = pMsg->srcId;
  pRspMsg->term = pSyncNode->pRaftStore->currentTerm;
  pRspMsg->lastIndex = pMsg->lastIndex;
  pRspMsg->lastTerm = pMsg->lastTerm;
  pRspMsg->startTime = pReceiver->startTime;
  pRspMsg->ack = pReceiver->ack;  // receiver maybe already closed
  pRspMsg->code = 0;
  pRspMsg->snapBeginIndex = pReceiver->snapshotParam.start;

  // send msg
  syncNodeSendMsgById(&pRspMsg->destId, pSyncNode, &rpcMsg);
  syncLogSendSyncSnapshotRsp(pSyncNode, pRspMsg, "");
  return 0;
}

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
int32_t syncNodeOnSnapshot(SSyncNode *pSyncNode, const SRpcMsg *pRpcMsg) {
  SyncSnapshotSend *pMsg = pRpcMsg->pCont;

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

  syncLogRecvSyncSnapshotSend(pSyncNode, pMsg, "");

  // state, term, seq/ack
  if (pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER) {
    if (pMsg->term == pSyncNode->pRaftStore->currentTerm) {
      if (pMsg->seq == SYNC_SNAPSHOT_SEQ_PRE_SNAPSHOT) {
        syncNodeOnSnapshotPre(pSyncNode, pMsg);

      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_BEGIN) {
        syncNodeOnSnapshotBegin(pSyncNode, pMsg);

      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_END) {
        syncNodeOnSnapshotEnd(pSyncNode, pMsg);
        (void)syncLogBufferReInit(pSyncNode->pLogBuf, pSyncNode);

      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_FORCE_CLOSE) {
        // force close, no response
        snapshotReceiverForceStop(pReceiver);

      } else if (pMsg->seq > SYNC_SNAPSHOT_SEQ_BEGIN && pMsg->seq < SYNC_SNAPSHOT_SEQ_END) {
        syncNodeOnSnapshotTransfering(pSyncNode, pMsg);

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
  tAssert(pSender != NULL);

  SSnapshot snapshot;
  pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);

  // prepare <begin, end>
  pSender->snapshotParam.start = pMsg->snapBeginIndex;
  pSender->snapshotParam.end = snapshot.lastApplyIndex;

  sNTrace(pSyncNode, "prepare snapshot, recv-begin:%" PRId64 ", snapshot.last:%" PRId64 ", snapshot.term:%" PRId64,
          pMsg->snapBeginIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm);

  if (pMsg->snapBeginIndex > snapshot.lastApplyIndex) {
    sNError(pSyncNode, "snapshot last index too small");
    return -1;
  }

  // update sender
  pSender->snapshot = snapshot;

  // start reader
  int32_t code = pSyncNode->pFsm->FpSnapshotStartRead(pSyncNode->pFsm, &(pSender->snapshotParam), &(pSender->pReader));
  if (code != 0) {
    sNError(pSyncNode, "create snapshot reader error");
    return -1;
  }

  // update next index
  syncIndexMgrSetIndex(pSyncNode->pNextIndex, &pMsg->srcId, snapshot.lastApplyIndex + 1);

  // update seq
  pSender->seq = SYNC_SNAPSHOT_SEQ_BEGIN;

  // build begin msg
  SRpcMsg rpcMsg = {0};
  (void)syncBuildSnapshotSend(&rpcMsg, 0, pSender->pSyncNode->vgId);

  SyncSnapshotSend *pSendMsg = rpcMsg.pCont;
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
  syncNodeSendMsgById(&pSendMsg->destId, pSender->pSyncNode, &rpcMsg);
  syncLogSendSyncSnapshotSend(pSyncNode, pSendMsg, "");

  return 0;
}

// sender on message
//
// condition 1 sender receives SYNC_SNAPSHOT_SEQ_END, close sender
// condition 2 sender receives ack, set seq = ack + 1, send msg from seq
// condition 3 sender receives error msg, just print error log
//
int32_t syncNodeOnSnapshotReply(SSyncNode *pSyncNode, const SRpcMsg *pRpcMsg) {
  SyncSnapshotRsp *pMsg = pRpcMsg->pCont;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(pSyncNode, &(pMsg->srcId))) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "maybe replica already dropped");
    return -1;
  }

  // get sender
  SSyncSnapshotSender *pSender = syncNodeGetSnapshotSender(pSyncNode, &(pMsg->srcId));
  tAssert(pSender != NULL);

  if (pMsg->startTime != pSender->startTime) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "sender/receiver start time not match");
    return -1;
  }

  syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "");

  // state, term, seq/ack
  if (pSyncNode->state == TAOS_SYNC_STATE_LEADER) {
    if (pMsg->term == pSyncNode->pRaftStore->currentTerm) {
      // prepare <begin, end>, send begin msg
      if (pMsg->ack == SYNC_SNAPSHOT_SEQ_PRE_SNAPSHOT) {
        syncNodeOnSnapshotReplyPre(pSyncNode, pMsg);
        return 0;
      }

      if (pMsg->ack == SYNC_SNAPSHOT_SEQ_BEGIN) {
        snapshotSenderUpdateProgress(pSender, pMsg);
        snapshotSend(pSender);
        return 0;
      }

      // receive ack is finish, close sender
      if (pMsg->ack == SYNC_SNAPSHOT_SEQ_END) {
        snapshotSenderStop(pSender, true);
        SSyncLogReplMgr *pMgr = syncNodeGetLogReplMgr(pSyncNode, &pMsg->srcId);
        if (pMgr) {
          syncLogReplMgrReset(pMgr);
        }
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
