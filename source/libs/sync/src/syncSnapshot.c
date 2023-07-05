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
  if (!condition) return NULL;

  SSyncSnapshotSender *pSender = taosMemoryCalloc(1, sizeof(SSyncSnapshotSender));
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
  pSender->term = raftStoreGetTerm(pSyncNode);
  pSender->startTime = 0;
  pSender->endTime = 0;
  pSender->pSyncNode->pFsm->FpGetSnapshotInfo(pSender->pSyncNode->pFsm, &pSender->snapshot);
  pSender->finish = false;

  return pSender;
}

void snapshotSenderDestroy(SSyncSnapshotSender *pSender) {
  if (pSender == NULL) return;

  // free current block
  if (pSender->pCurrentBlock != NULL) {
    taosMemoryFree(pSender->pCurrentBlock);
    pSender->pCurrentBlock = NULL;
  }

  // close reader
  if (pSender->pReader != NULL) {
    pSender->pSyncNode->pFsm->FpSnapshotStopRead(pSender->pSyncNode->pFsm, pSender->pReader);
    pSender->pReader = NULL;
  }

  // free sender
  taosMemoryFree(pSender);
}

bool snapshotSenderIsStart(SSyncSnapshotSender *pSender) { return pSender->start; }

int32_t snapshotSenderStart(SSyncSnapshotSender *pSender) {
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

  memset(&pSender->lastConfig, 0, sizeof(pSender->lastConfig));
  pSender->sendingMS = 0;
  pSender->term = raftStoreGetTerm(pSender->pSyncNode);
  pSender->startTime = taosGetTimestampMs();
  pSender->lastSendTime = pSender->startTime;
  pSender->finish = false;

  // build begin msg
  SRpcMsg rpcMsg = {0};
  if (syncBuildSnapshotSend(&rpcMsg, 0, pSender->pSyncNode->vgId) != 0) {
    sSError(pSender, "snapshot sender build msg failed since %s", terrstr());
    return -1;
  }

  SyncSnapshotSend *pMsg = rpcMsg.pCont;
  pMsg->srcId = pSender->pSyncNode->myRaftId;
  pMsg->destId = pSender->pSyncNode->replicasId[pSender->replicaIndex];
  pMsg->term = raftStoreGetTerm(pSender->pSyncNode);
  pMsg->beginIndex = pSender->snapshotParam.start;
  pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
  pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
  pMsg->lastConfigIndex = pSender->snapshot.lastConfigIndex;
  pMsg->lastConfig = pSender->lastConfig;
  pMsg->startTime = pSender->startTime;
  pMsg->seq = SYNC_SNAPSHOT_SEQ_PREP_SNAPSHOT;

  // event log
  syncLogSendSyncSnapshotSend(pSender->pSyncNode, pMsg, "snapshot sender start");

  // send msg
  if (syncNodeSendMsgById(&pMsg->destId, pSender->pSyncNode, &rpcMsg) != 0) {
    sSError(pSender, "snapshot sender send msg failed since %s", terrstr());
    return -1;
  }

  return 0;
}

void snapshotSenderStop(SSyncSnapshotSender *pSender, bool finish) {
  sSDebug(pSender, "snapshot sender stop, finish:%d reader:%p", finish, pSender->pReader);

  // update flag
  pSender->start = false;
  pSender->finish = finish;
  pSender->endTime = taosGetTimestampMs();

  // close reader
  if (pSender->pReader != NULL) {
    pSender->pSyncNode->pFsm->FpSnapshotStopRead(pSender->pSyncNode->pFsm, pSender->pReader);
    pSender->pReader = NULL;
  }

  // free current block
  if (pSender->pCurrentBlock != NULL) {
    taosMemoryFree(pSender->pCurrentBlock);
    pSender->pCurrentBlock = NULL;
    pSender->blockLen = 0;
  }
}

// when sender receive ack, call this function to send msg from seq
// seq = ack + 1, already updated
static int32_t snapshotSend(SSyncSnapshotSender *pSender) {
  // free memory last time (current seq - 1)
  if (pSender->pCurrentBlock != NULL) {
    taosMemoryFree(pSender->pCurrentBlock);
    pSender->pCurrentBlock = NULL;
    pSender->blockLen = 0;
  }

  // read data
  int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotDoRead(pSender->pSyncNode->pFsm, pSender->pReader,
                                                           &pSender->pCurrentBlock, &pSender->blockLen);
  if (ret != 0) {
    sSError(pSender, "snapshot sender read failed since %s", terrstr());
    return -1;
  }

  if (pSender->blockLen > 0) {
    // has read data
    sSDebug(pSender, "vgId:%d, snapshot sender continue to read, blockLen:%d seq:%d", pSender->pSyncNode->vgId,
            pSender->blockLen, pSender->seq);
  } else {
    // read finish, update seq to end
    pSender->seq = SYNC_SNAPSHOT_SEQ_END;
    sSInfo(pSender, "vgId:%d, snapshot sender read to the end, blockLen:%d seq:%d", pSender->pSyncNode->vgId,
           pSender->blockLen, pSender->seq);
  }

  // build msg
  SRpcMsg rpcMsg = {0};
  if (syncBuildSnapshotSend(&rpcMsg, pSender->blockLen, pSender->pSyncNode->vgId) != 0) {
    sSError(pSender, "vgId:%d, snapshot sender build msg failed since %s", pSender->pSyncNode->vgId, terrstr());
    return -1;
  }

  SyncSnapshotSend *pMsg = rpcMsg.pCont;
  pMsg->srcId = pSender->pSyncNode->myRaftId;
  pMsg->destId = pSender->pSyncNode->replicasId[pSender->replicaIndex];
  pMsg->term = raftStoreGetTerm(pSender->pSyncNode);
  pMsg->beginIndex = pSender->snapshotParam.start;
  pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
  pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
  pMsg->lastConfigIndex = pSender->snapshot.lastConfigIndex;
  pMsg->lastConfig = pSender->lastConfig;
  pMsg->seq = pSender->seq;

  if (pSender->pCurrentBlock != NULL) {
    memcpy(pMsg->data, pSender->pCurrentBlock, pSender->blockLen);
  }

  // event log
  if (pSender->seq == SYNC_SNAPSHOT_SEQ_END) {
    syncLogSendSyncSnapshotSend(pSender->pSyncNode, pMsg, "snapshot sender finish");
  } else {
    syncLogSendSyncSnapshotSend(pSender->pSyncNode, pMsg, "snapshot sender sending");
  }

  // send msg
  if (syncNodeSendMsgById(&pMsg->destId, pSender->pSyncNode, &rpcMsg) != 0) {
    sSError(pSender, "snapshot sender send msg failed since %s", terrstr());
    return -1;
  }

  pSender->lastSendTime = taosGetTimestampMs();
  return 0;
}

// send snapshot data from cache
int32_t snapshotReSend(SSyncSnapshotSender *pSender) {
  // build msg
  SRpcMsg rpcMsg = {0};
  if (syncBuildSnapshotSend(&rpcMsg, pSender->blockLen, pSender->pSyncNode->vgId) != 0) {
    sSError(pSender, "snapshot sender build msg failed since %s", terrstr());
    return -1;
  }

  SyncSnapshotSend *pMsg = rpcMsg.pCont;
  pMsg->srcId = pSender->pSyncNode->myRaftId;
  pMsg->destId = pSender->pSyncNode->replicasId[pSender->replicaIndex];
  pMsg->term = raftStoreGetTerm(pSender->pSyncNode);
  pMsg->beginIndex = pSender->snapshotParam.start;
  pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
  pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
  pMsg->lastConfigIndex = pSender->snapshot.lastConfigIndex;
  pMsg->lastConfig = pSender->lastConfig;
  pMsg->seq = pSender->seq;

  if (pSender->pCurrentBlock != NULL && pSender->blockLen > 0) {
    memcpy(pMsg->data, pSender->pCurrentBlock, pSender->blockLen);
  }

  // event log
  syncLogSendSyncSnapshotSend(pSender->pSyncNode, pMsg, "snapshot sender resend");

  // send msg
  if (syncNodeSendMsgById(&pMsg->destId, pSender->pSyncNode, &rpcMsg) != 0) {
    sSError(pSender, "snapshot sender resend msg failed since %s", terrstr());
    return -1;
  }

  pSender->lastSendTime = taosGetTimestampMs();
  return 0;
}

static int32_t snapshotSenderUpdateProgress(SSyncSnapshotSender *pSender, SyncSnapshotRsp *pMsg) {
  if (pMsg->ack != pSender->seq) {
    sSError(pSender, "snapshot sender update seq failed, ack:%d seq:%d", pMsg->ack, pSender->seq);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  pSender->ack = pMsg->ack;
  pSender->seq++;

  sSDebug(pSender, "snapshot sender update seq:%d", pSender->seq);
  return 0;
}

// return 0, start ok
// return 1, last snapshot finish ok
// return -1, error
int32_t syncNodeStartSnapshot(SSyncNode *pSyncNode, SRaftId *pDestId) {
  sNInfo(pSyncNode, "snapshot sender starting ...");

  SSyncSnapshotSender *pSender = syncNodeGetSnapshotSender(pSyncNode, pDestId);
  if (pSender == NULL) {
    sNError(pSyncNode, "snapshot sender start error since get failed");
    return -1;
  }

  if (snapshotSenderIsStart(pSender)) {
    sSInfo(pSender, "snapshot sender already start, ignore");
    return 0;
  }

  if (pSender->finish && taosGetTimestampMs() - pSender->endTime < SNAPSHOT_WAIT_MS) {
    sSInfo(pSender, "snapshot sender start too frequently, ignore");
    return 0;
  }

  sSInfo(pSender, "snapshot sender start");

  int32_t code = snapshotSenderStart(pSender);
  if (code != 0) {
    sSError(pSender, "snapshot sender start error since %s", terrstr());
    return -1;
  }

  return 0;
}

SSyncSnapshotReceiver *snapshotReceiverCreate(SSyncNode *pSyncNode, SRaftId fromId) {
  bool condition = (pSyncNode->pFsm->FpSnapshotStartWrite != NULL) && (pSyncNode->pFsm->FpSnapshotStopWrite != NULL) &&
                   (pSyncNode->pFsm->FpSnapshotDoWrite != NULL);
  if (!condition) return NULL;

  SSyncSnapshotReceiver *pReceiver = taosMemoryCalloc(1, sizeof(SSyncSnapshotReceiver));
  if (pReceiver == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pReceiver->start = false;
  pReceiver->ack = SYNC_SNAPSHOT_SEQ_BEGIN;
  pReceiver->pWriter = NULL;
  pReceiver->pSyncNode = pSyncNode;
  pReceiver->fromId = fromId;
  pReceiver->term = raftStoreGetTerm(pSyncNode);
  pReceiver->snapshot.data = NULL;
  pReceiver->snapshot.lastApplyIndex = SYNC_INDEX_INVALID;
  pReceiver->snapshot.lastApplyTerm = 0;
  pReceiver->snapshot.lastConfigIndex = SYNC_INDEX_INVALID;

  return pReceiver;
}

void snapshotReceiverDestroy(SSyncSnapshotReceiver *pReceiver) {
  if (pReceiver == NULL) return;

  // close writer
  if (pReceiver->pWriter != NULL) {
    int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, false,
                                                                  &pReceiver->snapshot);
    if (ret != 0) {
      sError("vgId:%d, snapshot receiver stop failed while destroy since %s", pReceiver->pSyncNode->vgId, terrstr());
    }
    pReceiver->pWriter = NULL;
  }

  // free receiver
  taosMemoryFree(pReceiver);
}

bool snapshotReceiverIsStart(SSyncSnapshotReceiver *pReceiver) {
  return (pReceiver != NULL ? pReceiver->start : false);
}

static int32_t snapshotReceiverStartWriter(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pBeginMsg) {
  if (pReceiver->pWriter != NULL) {
    sRError(pReceiver, "vgId:%d, snapshot receiver writer is not null", pReceiver->pSyncNode->vgId);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  // update ack
  pReceiver->ack = SYNC_SNAPSHOT_SEQ_BEGIN;

  // update snapshot
  pReceiver->snapshot.lastApplyIndex = pBeginMsg->lastIndex;
  pReceiver->snapshot.lastApplyTerm = pBeginMsg->lastTerm;
  pReceiver->snapshot.lastConfigIndex = pBeginMsg->lastConfigIndex;
  pReceiver->snapshotParam.start = pBeginMsg->beginIndex;
  pReceiver->snapshotParam.end = pBeginMsg->lastIndex;

  // start writer
  int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStartWrite(pReceiver->pSyncNode->pFsm, &pReceiver->snapshotParam,
                                                                 &pReceiver->pWriter);
  if (ret != 0) {
    sRError(pReceiver, "snapshot receiver start write failed since %s", terrstr());
    return -1;
  }

  // event log
  sRInfo(pReceiver, "snapshot receiver start write");
  return 0;
}

void snapshotReceiverStart(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pPreMsg) {
  if (snapshotReceiverIsStart(pReceiver)) {
    sRInfo(pReceiver, "snapshot receiver has started");
    return;
  }

  pReceiver->start = true;
  pReceiver->ack = SYNC_SNAPSHOT_SEQ_PREP_SNAPSHOT;
  pReceiver->term = raftStoreGetTerm(pReceiver->pSyncNode);
  pReceiver->fromId = pPreMsg->srcId;
  pReceiver->startTime = pPreMsg->startTime;

  // event log
  sRInfo(pReceiver, "snapshot receiver is start");
}

// just set start = false
// FpSnapshotStopWrite should not be called
void snapshotReceiverStop(SSyncSnapshotReceiver *pReceiver) {
  sRInfo(pReceiver, "snapshot receiver stop, not apply, writer:%p", pReceiver->pWriter);

  if (pReceiver->pWriter != NULL) {
    int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, false,
                                                                  &pReceiver->snapshot);
    if (ret != 0) {
      sRError(pReceiver, "snapshot receiver stop write failed since %s", terrstr());
    }
    pReceiver->pWriter = NULL;
  } else {
    sRInfo(pReceiver, "snapshot receiver stop, writer is null");
  }

  pReceiver->start = false;
}

// when recv last snapshot block, apply data into snapshot
static int32_t snapshotReceiverFinish(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pMsg) {
  int32_t code = 0;
  if (pReceiver->pWriter != NULL) {
    // write data
    sRInfo(pReceiver, "snapshot receiver write finish, blockLen:%d seq:%d", pMsg->dataLen, pMsg->seq);
    if (pMsg->dataLen > 0) {
      code = pReceiver->pSyncNode->pFsm->FpSnapshotDoWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, pMsg->data,
                                                           pMsg->dataLen);
      if (code != 0) {
        sRError(pReceiver, "failed to finish snapshot receiver write since %s", terrstr());
        return -1;
      }
    }

    // reset wal
    sRInfo(pReceiver, "snapshot receiver log restore");
    code =
        pReceiver->pSyncNode->pLogStore->syncLogRestoreFromSnapshot(pReceiver->pSyncNode->pLogStore, pMsg->lastIndex);
    if (code != 0) {
      sRError(pReceiver, "failed to snapshot receiver log restore since %s", terrstr());
      return -1;
    }

    // update commit index
    if (pReceiver->snapshot.lastApplyIndex > pReceiver->pSyncNode->commitIndex) {
      pReceiver->pSyncNode->commitIndex = pReceiver->snapshot.lastApplyIndex;
    }

    // maybe update term
    if (pReceiver->snapshot.lastApplyTerm > raftStoreGetTerm(pReceiver->pSyncNode)) {
      raftStoreSetTerm(pReceiver->pSyncNode, pReceiver->snapshot.lastApplyTerm);
    }

    // stop writer, apply data
    sRInfo(pReceiver, "snapshot receiver apply write");
    code = pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, true,
                                                           &pReceiver->snapshot);
    if (code != 0) {
      sRError(pReceiver, "snapshot receiver apply failed  since %s", terrstr());
      return -1;
    }
    pReceiver->pWriter = NULL;

    // update progress
    pReceiver->ack = SYNC_SNAPSHOT_SEQ_END;

  } else {
    sRError(pReceiver, "snapshot receiver finish error since writer is null");
    return -1;
  }

  // event log
  sRInfo(pReceiver, "snapshot receiver got last data and apply snapshot finished");
  return 0;
}

// apply data block
// update progress
static int32_t snapshotReceiverGotData(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pMsg) {
  if (pMsg->seq != pReceiver->ack + 1) {
    sRError(pReceiver, "snapshot receiver invalid seq, ack:%d seq:%d", pReceiver->ack, pMsg->seq);
    terrno = TSDB_CODE_SYN_INVALID_SNAPSHOT_MSG;
    return -1;
  }

  if (pReceiver->pWriter == NULL) {
    sRError(pReceiver, "snapshot receiver failed to write data since writer is null");
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  sRDebug(pReceiver, "snapshot receiver continue to write, blockLen:%d seq:%d", pMsg->dataLen, pMsg->seq);

  if (pMsg->dataLen > 0) {
    // apply data block
    int32_t code = pReceiver->pSyncNode->pFsm->FpSnapshotDoWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter,
                                                                 pMsg->data, pMsg->dataLen);
    if (code != 0) {
      sRError(pReceiver, "snapshot receiver continue write failed since %s", terrstr());
      return -1;
    }
  }

  // update progress
  pReceiver->ack = pMsg->seq;

  // event log
  sRDebug(pReceiver, "snapshot receiver continue to write finish");
  return 0;
}

SyncIndex syncNodeGetSnapBeginIndex(SSyncNode *ths) {
  SyncIndex snapStart = SYNC_INDEX_INVALID;

  if (syncNodeIsMnode(ths)) {
    snapStart = SYNC_INDEX_BEGIN;
    sNInfo(ths, "snapshot begin index is %" PRId64 " since its mnode", snapStart);
  } else {
    SSyncLogStoreData *pData = ths->pLogStore->data;
    SWal              *pWal = pData->pWal;

    int64_t walCommitVer = walGetCommittedVer(pWal);
    snapStart = TMAX(ths->commitIndex, walCommitVer) + 1;

    sNInfo(ths, "snapshot begin index is %" PRId64, snapStart);
  }

  return snapStart;
}

static int32_t syncNodeOnSnapshotPrep(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;
  int64_t                timeNow = taosGetTimestampMs();
  int32_t                code = 0;

  if (snapshotReceiverIsStart(pReceiver)) {
    // already start
    if (pMsg->startTime > pReceiver->startTime) {
      sRInfo(pReceiver, "snapshot receiver startTime:%" PRId64 " > msg startTime:%" PRId64 " start receiver",
             pReceiver->startTime, pMsg->startTime);
      goto _START_RECEIVER;
    } else if (pMsg->startTime == pReceiver->startTime) {
      sRInfo(pReceiver, "snapshot receiver startTime:%" PRId64 " == msg startTime:%" PRId64 " send reply",
             pReceiver->startTime, pMsg->startTime);
      goto _SEND_REPLY;
    } else {
      // ignore
      sRError(pReceiver, "snapshot receiver startTime:%" PRId64 " < msg startTime:%" PRId64 " ignore",
              pReceiver->startTime, pMsg->startTime);
      terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
      code = terrno;
      goto _SEND_REPLY;
    }
  } else {
    // start new
    sRInfo(pReceiver, "snapshot receiver not start yet so start new one");
    goto _START_RECEIVER;
  }

_START_RECEIVER:
  if (timeNow - pMsg->startTime > SNAPSHOT_MAX_CLOCK_SKEW_MS) {
    sRError(pReceiver, "snapshot receiver time skew too much, now:%" PRId64 " msg startTime:%" PRId64, timeNow,
            pMsg->startTime);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    code = terrno;
  } else {
    // waiting for clock match
    while (timeNow < pMsg->startTime) {
      sRInfo(pReceiver, "snapshot receiver pre waitting for true time, now:%" PRId64 ", startTime:%" PRId64, timeNow,
             pMsg->startTime);
      taosMsleep(10);
      timeNow = taosGetTimestampMs();
    }

    if (snapshotReceiverIsStart(pReceiver)) {
      sRInfo(pReceiver, "snapshot receiver already start and force stop pre one");
      snapshotReceiverStop(pReceiver);
    }

    snapshotReceiverStart(pReceiver, pMsg);  // set start-time same with sender
  }

_SEND_REPLY:
    // build msg
    ;  // make complier happy

  SRpcMsg rpcMsg = {0};
  if (syncBuildSnapshotSendRsp(&rpcMsg, pSyncNode->vgId) != 0) {
    sRError(pReceiver, "snapshot receiver failed to build resp since %s", terrstr());
    return -1;
  }

  SyncSnapshotRsp *pRspMsg = rpcMsg.pCont;
  pRspMsg->srcId = pSyncNode->myRaftId;
  pRspMsg->destId = pMsg->srcId;
  pRspMsg->term = raftStoreGetTerm(pSyncNode);
  pRspMsg->lastIndex = pMsg->lastIndex;
  pRspMsg->lastTerm = pMsg->lastTerm;
  pRspMsg->startTime = pReceiver->startTime;
  pRspMsg->ack = pMsg->seq;  // receiver maybe already closed
  pRspMsg->code = code;
  pRspMsg->snapBeginIndex = syncNodeGetSnapBeginIndex(pSyncNode);

  // send msg
  syncLogSendSyncSnapshotRsp(pSyncNode, pRspMsg, "snapshot receiver pre-snapshot");
  if (syncNodeSendMsgById(&pRspMsg->destId, pSyncNode, &rpcMsg) != 0) {
    sRError(pReceiver, "snapshot receiver failed to build resp since %s", terrstr());
    return -1;
  }

  return code;
}

static int32_t syncNodeOnSnapshotBegin(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  // condition 1
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;
  int32_t                code = TSDB_CODE_SYN_INTERNAL_ERROR;

  if (!snapshotReceiverIsStart(pReceiver)) {
    sRError(pReceiver, "snapshot receiver begin failed since not start");
    goto _SEND_REPLY;
  }

  if (pReceiver->startTime != pMsg->startTime) {
    sRError(pReceiver, "snapshot receiver begin failed since startTime:%" PRId64 " not equal to msg startTime:%" PRId64,
            pReceiver->startTime, pMsg->startTime);
    goto _SEND_REPLY;
  }

  // start writer
  if (snapshotReceiverStartWriter(pReceiver, pMsg) != 0) {
    sRError(pReceiver, "snapshot receiver begin failed since start writer failed");
    goto _SEND_REPLY;
  }

  code = 0;
_SEND_REPLY:
  if (code != 0 && terrno != 0) {
    code = terrno;
  }

  // build msg
  SRpcMsg rpcMsg = {0};
  if (syncBuildSnapshotSendRsp(&rpcMsg, pSyncNode->vgId) != 0) {
    sRError(pReceiver, "snapshot receiver build resp failed since %s", terrstr());
    return -1;
  }

  SyncSnapshotRsp *pRspMsg = rpcMsg.pCont;
  pRspMsg->srcId = pSyncNode->myRaftId;
  pRspMsg->destId = pMsg->srcId;
  pRspMsg->term = raftStoreGetTerm(pSyncNode);
  pRspMsg->lastIndex = pMsg->lastIndex;
  pRspMsg->lastTerm = pMsg->lastTerm;
  pRspMsg->startTime = pReceiver->startTime;
  pRspMsg->ack = pReceiver->ack;  // receiver maybe already closed
  pRspMsg->code = code;
  pRspMsg->snapBeginIndex = pReceiver->snapshotParam.start;

  // send msg
  syncLogSendSyncSnapshotRsp(pSyncNode, pRspMsg, "snapshot receiver begin");
  if (syncNodeSendMsgById(&pRspMsg->destId, pSyncNode, &rpcMsg) != 0) {
    sRError(pReceiver, "snapshot receiver send resp failed since %s", terrstr());
    return -1;
  }

  return code;
}

static int32_t syncNodeOnSnapshotReceive(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  // condition 4
  // transfering
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;

  // waiting for clock match
  int64_t timeNow = taosGetTimestampMs();
  while (timeNow < pMsg->startTime) {
    sRInfo(pReceiver, "snapshot receiver receiving waitting for true time, now:%" PRId64 ", stime:%" PRId64, timeNow,
           pMsg->startTime);
    taosMsleep(10);
    timeNow = taosGetTimestampMs();
  }

  int32_t code = 0;
  if (snapshotReceiverGotData(pReceiver, pMsg) != 0) {
    code = terrno;
    if (code >= SYNC_SNAPSHOT_SEQ_INVALID) {
      code = TSDB_CODE_SYN_INTERNAL_ERROR;
    }
  }

  // build msg
  SRpcMsg rpcMsg = {0};
  if (syncBuildSnapshotSendRsp(&rpcMsg, pSyncNode->vgId)) {
    sRError(pReceiver, "snapshot receiver build resp failed since %s", terrstr());
    return -1;
  }

  SyncSnapshotRsp *pRspMsg = rpcMsg.pCont;
  pRspMsg->srcId = pSyncNode->myRaftId;
  pRspMsg->destId = pMsg->srcId;
  pRspMsg->term = raftStoreGetTerm(pSyncNode);
  pRspMsg->lastIndex = pMsg->lastIndex;
  pRspMsg->lastTerm = pMsg->lastTerm;
  pRspMsg->startTime = pReceiver->startTime;
  pRspMsg->ack = pReceiver->ack;  // receiver maybe already closed
  pRspMsg->code = code;
  pRspMsg->snapBeginIndex = pReceiver->snapshotParam.start;

  // send msg
  syncLogSendSyncSnapshotRsp(pSyncNode, pRspMsg, "snapshot receiver received");
  if (syncNodeSendMsgById(&pRspMsg->destId, pSyncNode, &rpcMsg) != 0) {
    sRError(pReceiver, "snapshot receiver send resp failed since %s", terrstr());
    return -1;
  }

  return code;
}

static int32_t syncNodeOnSnapshotEnd(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  // condition 2
  // end, finish FSM
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;

  // waiting for clock match
  int64_t timeNow = taosGetTimestampMs();
  while (timeNow < pMsg->startTime) {
    sRInfo(pReceiver, "snapshot receiver finish waitting for true time, now:%" PRId64 ", stime:%" PRId64, timeNow,
           pMsg->startTime);
    taosMsleep(10);
    timeNow = taosGetTimestampMs();
  }

  int32_t code = snapshotReceiverFinish(pReceiver, pMsg);
  if (code == 0) {
    snapshotReceiverStop(pReceiver);
  }

  // build msg
  SRpcMsg rpcMsg = {0};
  if (syncBuildSnapshotSendRsp(&rpcMsg, pSyncNode->vgId) != 0) {
    sRError(pReceiver, "snapshot receiver build rsp failed since %s", terrstr());
    return -1;
  }

  SyncSnapshotRsp *pRspMsg = rpcMsg.pCont;
  pRspMsg->srcId = pSyncNode->myRaftId;
  pRspMsg->destId = pMsg->srcId;
  pRspMsg->term = raftStoreGetTerm(pSyncNode);
  pRspMsg->lastIndex = pMsg->lastIndex;
  pRspMsg->lastTerm = pMsg->lastTerm;
  pRspMsg->startTime = pReceiver->startTime;
  pRspMsg->ack = pReceiver->ack;  // receiver maybe already closed
  pRspMsg->code = code;
  pRspMsg->snapBeginIndex = pReceiver->snapshotParam.start;

  // send msg
  syncLogSendSyncSnapshotRsp(pSyncNode, pRspMsg, "snapshot receiver end");
  if (syncNodeSendMsgById(&pRspMsg->destId, pSyncNode, &rpcMsg) != 0) {
    sRError(pReceiver, "snapshot receiver send rsp failed since %s", terrstr());
    return -1;
  }

  return code;
}

// receiver on message
//
// condition 1, recv SYNC_SNAPSHOT_SEQ_PREP_SNAPSHOT
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
  SyncSnapshotSend      *pMsg = pRpcMsg->pCont;
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(pSyncNode, &pMsg->srcId)) {
    syncLogRecvSyncSnapshotSend(pSyncNode, pMsg, "not in my config");
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  if (pMsg->term < raftStoreGetTerm(pSyncNode)) {
    syncLogRecvSyncSnapshotSend(pSyncNode, pMsg, "reject since small term");
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  if(pSyncNode->raftCfg.cfg.nodeInfo[pSyncNode->raftCfg.cfg.myIndex].nodeRole != TAOS_SYNC_ROLE_LEARNER){
    if (pMsg->term > raftStoreGetTerm(pSyncNode)) {
      syncNodeStepDown(pSyncNode, pMsg->term);
    }
  }
  else{
    syncNodeUpdateTermWithoutStepDown(pSyncNode, pMsg->term);
  }

  // state, term, seq/ack
  int32_t code = 0;
  if (pSyncNode->state == TAOS_SYNC_STATE_FOLLOWER || pSyncNode->state == TAOS_SYNC_STATE_LEARNER) {
    if (pMsg->term == raftStoreGetTerm(pSyncNode)) {
      if (pMsg->seq == SYNC_SNAPSHOT_SEQ_PREP_SNAPSHOT) {
        syncLogRecvSyncSnapshotSend(pSyncNode, pMsg, "process seq pre-snapshot");
        code = syncNodeOnSnapshotPrep(pSyncNode, pMsg);
      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_BEGIN) {
        syncLogRecvSyncSnapshotSend(pSyncNode, pMsg, "process seq begin");
        code = syncNodeOnSnapshotBegin(pSyncNode, pMsg);
      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_END) {
        syncLogRecvSyncSnapshotSend(pSyncNode, pMsg, "process seq end");
        code = syncNodeOnSnapshotEnd(pSyncNode, pMsg);
        if (syncLogBufferReInit(pSyncNode->pLogBuf, pSyncNode) != 0) {
          sRError(pReceiver, "failed to reinit log buffer since %s", terrstr());
          code = -1;
        }
      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_FORCE_CLOSE) {
        // force close, no response
        syncLogRecvSyncSnapshotSend(pSyncNode, pMsg, "process force stop");
        snapshotReceiverStop(pReceiver);
      } else if (pMsg->seq > SYNC_SNAPSHOT_SEQ_BEGIN && pMsg->seq < SYNC_SNAPSHOT_SEQ_END) {
        syncLogRecvSyncSnapshotSend(pSyncNode, pMsg, "process seq data");
        code = syncNodeOnSnapshotReceive(pSyncNode, pMsg);
      } else {
        // error log
        sRError(pReceiver, "snapshot receiver recv error seq:%d, my ack:%d", pMsg->seq, pReceiver->ack);
        code = -1;
      }
    } else {
      // error log
      sRError(pReceiver, "snapshot receiver term not equal");
      code = -1;
    }
  } else {
    // error log
    sRError(pReceiver, "snapshot receiver not follower");
    code = -1;
  }

  syncNodeResetElectTimer(pSyncNode);
  return code;
}

static int32_t syncNodeOnSnapshotPrepRsp(SSyncNode *pSyncNode, SSyncSnapshotSender *pSender, SyncSnapshotRsp *pMsg) {
  SSnapshot snapshot = {0};
  pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);

  // prepare <begin, end>
  pSender->snapshotParam.start = pMsg->snapBeginIndex;
  pSender->snapshotParam.end = snapshot.lastApplyIndex;

  sSInfo(pSender, "prepare snapshot, recv-begin:%" PRId64 ", snapshot.last:%" PRId64 ", snapshot.term:%" PRId64,
         pMsg->snapBeginIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm);

  if (pMsg->snapBeginIndex > snapshot.lastApplyIndex) {
    sSError(pSender, "prepare snapshot failed since beginIndex:%" PRId64 " larger than applyIndex:%" PRId64,
            pMsg->snapBeginIndex, snapshot.lastApplyIndex);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  // update sender
  pSender->snapshot = snapshot;

  // start reader
  int32_t code = pSyncNode->pFsm->FpSnapshotStartRead(pSyncNode->pFsm, &pSender->snapshotParam, &pSender->pReader);
  if (code != 0) {
    sSError(pSender, "prepare snapshot failed since %s", terrstr());
    return -1;
  }

  // update next index
  syncIndexMgrSetIndex(pSyncNode->pNextIndex, &pMsg->srcId, snapshot.lastApplyIndex + 1);

  // update seq
  pSender->seq = SYNC_SNAPSHOT_SEQ_BEGIN;

  // build begin msg
  SRpcMsg rpcMsg = {0};
  if (syncBuildSnapshotSend(&rpcMsg, 0, pSender->pSyncNode->vgId) != 0) {
    sSError(pSender, "prepare snapshot failed since build msg error");
    return -1;
  }

  SyncSnapshotSend *pSendMsg = rpcMsg.pCont;
  pSendMsg->srcId = pSender->pSyncNode->myRaftId;
  pSendMsg->destId = pSender->pSyncNode->replicasId[pSender->replicaIndex];
  pSendMsg->term = raftStoreGetTerm(pSender->pSyncNode);
  pSendMsg->beginIndex = pSender->snapshotParam.start;
  pSendMsg->lastIndex = pSender->snapshot.lastApplyIndex;
  pSendMsg->lastTerm = pSender->snapshot.lastApplyTerm;
  pSendMsg->lastConfigIndex = pSender->snapshot.lastConfigIndex;
  pSendMsg->lastConfig = pSender->lastConfig;
  pSendMsg->startTime = pSender->startTime;
  pSendMsg->seq = SYNC_SNAPSHOT_SEQ_BEGIN;

  // send msg
  syncLogSendSyncSnapshotSend(pSyncNode, pSendMsg, "snapshot sender reply pre");
  if (syncNodeSendMsgById(&pSendMsg->destId, pSender->pSyncNode, &rpcMsg) != 0) {
    sSError(pSender, "prepare snapshot failed since send msg error");
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
int32_t syncNodeOnSnapshotRsp(SSyncNode *pSyncNode, const SRpcMsg *pRpcMsg) {
  SyncSnapshotRsp *pMsg = pRpcMsg->pCont;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(pSyncNode, &pMsg->srcId)) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "maybe replica already dropped");
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  // get sender
  SSyncSnapshotSender *pSender = syncNodeGetSnapshotSender(pSyncNode, &pMsg->srcId);
  if (pSender == NULL) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "sender is null");
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  // state, term, seq/ack
  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "snapshot sender not leader");
    sSError(pSender, "snapshot sender not leader");
    terrno = TSDB_CODE_SYN_NOT_LEADER;
    goto _ERROR;
  }

  if (pMsg->startTime != pSender->startTime) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "snapshot sender and receiver time not match");
    sSError(pSender, "sender:%" PRId64 " receiver:%" PRId64 " time not match, error:%s 0x%x", pMsg->startTime,
            pSender->startTime, tstrerror(pMsg->code), pMsg->code);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _ERROR;
  }

  SyncTerm currentTerm = raftStoreGetTerm(pSyncNode);
  if (pMsg->term != currentTerm) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "snapshot sender and receiver term not match");
    sSError(pSender, "snapshot sender term not equal, msg term:%" PRId64 " currentTerm:%" PRId64, pMsg->term,
            currentTerm);
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _ERROR;
  }

  if (pMsg->code != 0) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "receive error code");
    sSError(pSender, "snapshot sender receive error:%s 0x%x and stop sender", tstrerror(pMsg->code), pMsg->code);
    terrno = pMsg->code;
    goto _ERROR;
  }

  // prepare <begin, end>, send begin msg
  if (pMsg->ack == SYNC_SNAPSHOT_SEQ_PREP_SNAPSHOT) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "process seq pre-snapshot");
    return syncNodeOnSnapshotPrepRsp(pSyncNode, pSender, pMsg);
  }

  if (pSender->pReader == NULL || pSender->finish) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "snapshot sender invalid");
    sSError(pSender, "snapshot sender invalid error:%s 0x%x, pReader:%p finish:%d", tstrerror(pMsg->code), pMsg->code,
            pSender->pReader, pSender->finish);
    terrno = pMsg->code;
    goto _ERROR;
  }

  if (pMsg->ack == SYNC_SNAPSHOT_SEQ_BEGIN) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "process seq begin");
    if (snapshotSenderUpdateProgress(pSender, pMsg) != 0) {
      return -1;
    }

    if (snapshotSend(pSender) != 0) {
      return -1;
    }
    return 0;
  }

  // receive ack is finish, close sender
  if (pMsg->ack == SYNC_SNAPSHOT_SEQ_END) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "process seq end");
    snapshotSenderStop(pSender, true);
    syncNodeReplicateReset(pSyncNode, &pMsg->srcId);
    return 0;
  }

  // send next msg
  if (pMsg->ack == pSender->seq) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "process seq data");
    // update sender ack
    if (snapshotSenderUpdateProgress(pSender, pMsg) != 0) {
      return -1;
    }
    if (snapshotSend(pSender) != 0) {
      return -1;
    }
  } else if (pMsg->ack == pSender->seq - 1) {
    // maybe resend
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "process seq and resend");
    if (snapshotReSend(pSender) != 0) {
      return -1;
    }
  } else {
    // error log
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "receive error ack");
    sSError(pSender, "snapshot sender receive error ack:%d, my seq:%d", pMsg->ack, pSender->seq);
    snapshotSenderStop(pSender, true);
    syncNodeReplicateReset(pSyncNode, &pMsg->srcId);
    return -1;
  }

  return 0;

_ERROR:
  snapshotSenderStop(pSender, true);
  syncNodeReplicateReset(pSyncNode, &pMsg->srcId);
  return -1;
}
