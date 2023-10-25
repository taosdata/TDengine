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

static void syncSnapBufferReset(SSyncSnapBuffer *pBuf) {
  taosThreadMutexLock(&pBuf->mutex);
  for (int64_t i = pBuf->start; i < pBuf->end; ++i) {
    rpcFreeCont(pBuf->entries[i % pBuf->size]);
    pBuf->entries[i % pBuf->size] = NULL;
  }
  pBuf->start = 1;
  pBuf->end = 1;
  pBuf->cursor = 0;
  taosThreadMutexUnlock(&pBuf->mutex);
}

static void syncSnapBufferDestroy(SSyncSnapBuffer **ppBuf) {
  if (ppBuf == NULL || ppBuf[0] == NULL) return;
  SSyncSnapBuffer *pBuf = ppBuf[0];

  syncSnapBufferReset(pBuf);

  taosThreadMutexDestroy(&pBuf->mutex);
  taosMemoryFree(ppBuf[0]);
  ppBuf[0] = NULL;
  return;
}

static SSyncSnapBuffer *syncSnapBufferCreate() {
  SSyncSnapBuffer *pBuf = taosMemoryCalloc(1, sizeof(SSyncSnapBuffer));
  if (pBuf == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }
  pBuf->size = sizeof(pBuf->entries) / sizeof(void *);
  ASSERT(pBuf->size == TSDB_SYNC_SNAP_BUFFER_SIZE);
  taosThreadMutexInit(&pBuf->mutex, NULL);
  return pBuf;
}

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
  pSender->startTime = -1;
  pSender->waitTime = -1;
  pSender->pSyncNode->pFsm->FpGetSnapshotInfo(pSender->pSyncNode->pFsm, &pSender->snapshot);
  pSender->finish = false;

  pSender->pSndBuf = syncSnapBufferCreate();
  if (pSender->pSndBuf == NULL) {
    taosMemoryFree(pSender);
    pSender = NULL;
    return NULL;
  }
  syncSnapBufferReset(pSender->pSndBuf);

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

  // free snap buffer
  if (pSender->pSndBuf) {
    syncSnapBufferDestroy(&pSender->pSndBuf);
  }
  // free sender
  taosMemoryFree(pSender);
}

bool snapshotSenderIsStart(SSyncSnapshotSender *pSender) { return pSender->start; }

int32_t snapshotSenderStart(SSyncSnapshotSender *pSender) {
  int32_t code = -1;

  int8_t started = atomic_val_compare_exchange_8(&pSender->start, false, true);
  if (started) return 0;

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
  pSender->startTime = taosGetMonoTimestampMs();
  pSender->lastSendTime = taosGetTimestampMs();
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
  pMsg->term = pSender->term;
  pMsg->beginIndex = pSender->snapshotParam.start;
  pMsg->lastIndex = pSender->snapshot.lastApplyIndex;
  pMsg->lastTerm = pSender->snapshot.lastApplyTerm;
  pMsg->lastConfigIndex = pSender->snapshot.lastConfigIndex;
  pMsg->lastConfig = pSender->lastConfig;
  pMsg->startTime = pSender->startTime;
  pMsg->seq = SYNC_SNAPSHOT_SEQ_PREP_SNAPSHOT;

  // send msg
  if (syncNodeSendMsgById(&pMsg->destId, pSender->pSyncNode, &rpcMsg) != 0) {
    sSError(pSender, "snapshot sender send msg failed since %s", terrstr());
    return -1;
  }

  sSInfo(pSender, "snapshot sender start, to dnode:%d.", DID(&pMsg->destId));
  return 0;
}

void snapshotSenderStop(SSyncSnapshotSender *pSender, bool finish) {
  sSDebug(pSender, "snapshot sender stop, finish:%d reader:%p", finish, pSender->pReader);

  // update flag
  int8_t stopped = !atomic_val_compare_exchange_8(&pSender->start, true, false);
  if (stopped) return;

  pSender->finish = finish;
  pSender->waitTime = -1;

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

  syncSnapBufferReset(pSender->pSndBuf);

  SRaftId destId = pSender->pSyncNode->replicasId[pSender->replicaIndex];
  sSInfo(pSender, "snapshot sender stop, to dnode:%d, finish:%d", DID(&destId), finish);
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

  if (pSender->seq != SYNC_SNAPSHOT_SEQ_END) {
    pSender->seq++;

    // read data
    int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotDoRead(pSender->pSyncNode->pFsm, pSender->pReader,
                                                             &pSender->pCurrentBlock, &pSender->blockLen);
    if (ret != 0) {
      sSError(pSender, "snapshot sender read failed since %s", terrstr());
      return -1;
    }

    if (pSender->blockLen > 0) {
      // has read data
      sSDebug(pSender, "snapshot sender continue to read, blockLen:%d seq:%d", pSender->blockLen, pSender->seq);
    } else {
      // read finish, update seq to end
      pSender->seq = SYNC_SNAPSHOT_SEQ_END;
      sSInfo(pSender, "snapshot sender read to the end, blockLen:%d seq:%d", pSender->blockLen, pSender->seq);
      return 0;
    }
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
  pMsg->startTime = pSender->startTime;
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
  pMsg->startTime = pSender->startTime;
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

// return 0, start ok
// return 1, last snapshot finish ok
// return -1, error
int32_t syncNodeStartSnapshot(SSyncNode *pSyncNode, SRaftId *pDestId) {
  SSyncSnapshotSender *pSender = syncNodeGetSnapshotSender(pSyncNode, pDestId);
  if (pSender == NULL) {
    sNError(pSyncNode, "snapshot sender start error since get failed");
    return -1;
  }

  if (snapshotSenderIsStart(pSender)) {
    sSDebug(pSender, "snapshot sender already start, ignore");
    return 0;
  }

  int64_t timeNow = taosGetTimestampMs();
  if (pSender->waitTime <= 0) {
    pSender->waitTime = timeNow + SNAPSHOT_WAIT_MS;
  }
  if (timeNow < pSender->waitTime) {
    sSDebug(pSender, "snapshot sender waitTime not expired yet, ignore");
    return 0;
  }

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
  pReceiver->startTime = 0;
  pReceiver->ack = SYNC_SNAPSHOT_SEQ_BEGIN;
  pReceiver->pWriter = NULL;
  pReceiver->pSyncNode = pSyncNode;
  pReceiver->fromId = fromId;
  pReceiver->term = raftStoreGetTerm(pSyncNode);
  pReceiver->snapshot.data = NULL;
  pReceiver->snapshot.lastApplyIndex = SYNC_INDEX_INVALID;
  pReceiver->snapshot.lastApplyTerm = 0;
  pReceiver->snapshot.lastConfigIndex = SYNC_INDEX_INVALID;

  pReceiver->pRcvBuf = syncSnapBufferCreate();
  if (pReceiver->pRcvBuf == NULL) {
    taosMemoryFree(pReceiver);
    pReceiver = NULL;
    return NULL;
  }

  syncSnapBufferReset(pReceiver->pRcvBuf);
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

  // free snap buf
  if (pReceiver->pRcvBuf) {
    syncSnapBufferDestroy(&pReceiver->pRcvBuf);
  }

  // free receiver
  taosMemoryFree(pReceiver);
}

bool snapshotReceiverIsStart(SSyncSnapshotReceiver *pReceiver) {
  return (pReceiver != NULL ? atomic_load_8(&pReceiver->start) : false);
}

static int32_t snapshotReceiverSignatureCmp(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pMsg) {
  if (pReceiver->term < pMsg->term) return -1;
  if (pReceiver->term > pMsg->term) return 1;
  if (pReceiver->startTime < pMsg->startTime) return -1;
  if (pReceiver->startTime > pMsg->startTime) return 1;
  return 0;
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

  int8_t started = atomic_val_compare_exchange_8(&pReceiver->start, false, true);
  if (started) return;

  pReceiver->ack = SYNC_SNAPSHOT_SEQ_PREP_SNAPSHOT;
  pReceiver->term = pPreMsg->term;
  pReceiver->fromId = pPreMsg->srcId;
  pReceiver->startTime = pPreMsg->startTime;

  sRInfo(pReceiver, "snapshot receiver start, from dnode:%d.", DID(&pReceiver->fromId));
}

// just set start = false
// FpSnapshotStopWrite should not be called
void snapshotReceiverStop(SSyncSnapshotReceiver *pReceiver) {
  sRDebug(pReceiver, "snapshot receiver stop, not apply, writer:%p", pReceiver->pWriter);

  int8_t stopped = !atomic_val_compare_exchange_8(&pReceiver->start, true, false);
  if (stopped) return;

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

  syncSnapBufferReset(pReceiver->pRcvBuf);
}

// when recv last snapshot block, apply data into snapshot
static int32_t snapshotReceiverFinish(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pMsg) {
  int32_t code = 0;
  if (pReceiver->pWriter != NULL) {
    // write data
    sRInfo(pReceiver, "snapshot receiver write about to finish, blockLen:%d seq:%d", pMsg->dataLen, pMsg->seq);
    if (pMsg->dataLen > 0) {
      code = pReceiver->pSyncNode->pFsm->FpSnapshotDoWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, pMsg->data,
                                                           pMsg->dataLen);
      if (code != 0) {
        sRError(pReceiver, "failed to finish snapshot receiver write since %s", terrstr());
        return -1;
      }
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
    code = pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter, true,
                                                           &pReceiver->snapshot);
    if (code != 0) {
      sRError(pReceiver, "snapshot receiver apply failed  since %s", terrstr());
      return -1;
    }
    pReceiver->pWriter = NULL;
    sRInfo(pReceiver, "snapshot receiver write stopped");

    // update progress
    pReceiver->ack = SYNC_SNAPSHOT_SEQ_END;

    // reset wal
    code =
        pReceiver->pSyncNode->pLogStore->syncLogRestoreFromSnapshot(pReceiver->pSyncNode->pLogStore, pMsg->lastIndex);
    if (code != 0) {
      sRError(pReceiver, "failed to snapshot receiver log restore since %s", terrstr());
      return -1;
    }
    sRInfo(pReceiver, "wal log restored from snapshot");
  } else {
    sRError(pReceiver, "snapshot receiver finish error since writer is null");
    return -1;
  }

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
    int32_t order = 0;
    if ((order = snapshotReceiverSignatureCmp(pReceiver, pMsg)) < 0) {
      sRInfo(pReceiver,
             "received a new snapshot preparation. restart receiver."
             " msg signature:(%" PRId64 ", %" PRId64 ")",
             pMsg->term, pMsg->startTime);
      goto _START_RECEIVER;
    } else if (order == 0) {
      sRInfo(pReceiver,
             "received a duplicate snapshot preparation. send reply."
             " msg signature:(%" PRId64 ", %" PRId64 ")",
             pMsg->term, pMsg->startTime);
      goto _SEND_REPLY;
    } else {
      // ignore
      sRError(pReceiver,
              "received a stale snapshot preparation. ignore."
              " msg signature:(%" PRId64 ", %" PRId64 ")",
              pMsg->term, pMsg->startTime);
      terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
      code = terrno;
      goto _SEND_REPLY;
    }
  } else {
    // start new
    sRInfo(pReceiver, "snapshot receiver not start yet so start new one");
    goto _START_RECEIVER;
  }

_START_RECEIVER:;
  if (snapshotReceiverIsStart(pReceiver)) {
    sRInfo(pReceiver, "snapshot receiver already start and force stop pre one");
    snapshotReceiverStop(pReceiver);
  }

  snapshotReceiverStart(pReceiver, pMsg);  // set start-time same with sender

_SEND_REPLY:;

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
  pRspMsg->startTime = pMsg->startTime;
  pRspMsg->ack = pMsg->seq;  // receiver maybe already closed
  pRspMsg->code = code;
  pRspMsg->snapBeginIndex = syncNodeGetSnapBeginIndex(pSyncNode);

  // send msg
  if (syncNodeSendMsgById(&pRspMsg->destId, pSyncNode, &rpcMsg) != 0) {
    sRError(pReceiver, "failed to send resp since %s", terrstr());
    return -1;
  }

  return code;
}

static int32_t syncNodeOnSnapshotBegin(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  // condition 1
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;
  int32_t                code = TSDB_CODE_SYN_INTERNAL_ERROR;

  if (!snapshotReceiverIsStart(pReceiver)) {
    sRError(pReceiver, "failed to begin snapshot receiver since not started");
    goto _SEND_REPLY;
  }

  if (snapshotReceiverSignatureCmp(pReceiver, pMsg) != 0) {
    terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
    sRError(pReceiver, "failed to begin snapshot receiver since %s", terrstr());
    goto _SEND_REPLY;
  }

  // start writer
  if (snapshotReceiverStartWriter(pReceiver, pMsg) != 0) {
    sRError(pReceiver, "failed to start snapshot writer since %s", terrstr());
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
    sRError(pReceiver, "failed to build snapshot receiver resp since %s", terrstr());
    return -1;
  }

  SyncSnapshotRsp *pRspMsg = rpcMsg.pCont;
  pRspMsg->srcId = pSyncNode->myRaftId;
  pRspMsg->destId = pMsg->srcId;
  pRspMsg->term = raftStoreGetTerm(pSyncNode);
  pRspMsg->lastIndex = pMsg->lastIndex;
  pRspMsg->lastTerm = pMsg->lastTerm;
  pRspMsg->startTime = pMsg->startTime;
  pRspMsg->ack = pReceiver->ack;  // receiver maybe already closed
  pRspMsg->code = code;
  pRspMsg->snapBeginIndex = pReceiver->snapshotParam.start;

  // send msg
  if (syncNodeSendMsgById(&pRspMsg->destId, pSyncNode, &rpcMsg) != 0) {
    sRError(pReceiver, "failed to send snapshot receiver resp since %s", terrstr());
    return -1;
  }

  return code;
}

static int32_t syncSnapSendRsp(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pMsg, int32_t code) {
  SSyncNode *pSyncNode = pReceiver->pSyncNode;
  // build msg
  SRpcMsg rpcMsg = {0};
  if (syncBuildSnapshotSendRsp(&rpcMsg, pSyncNode->vgId)) {
    sRError(pReceiver, "failed to build snapshot receiver resp since %s", terrstr());
    return -1;
  }

  SyncSnapshotRsp *pRspMsg = rpcMsg.pCont;
  pRspMsg->srcId = pSyncNode->myRaftId;
  pRspMsg->destId = pMsg->srcId;
  pRspMsg->term = raftStoreGetTerm(pSyncNode);
  pRspMsg->lastIndex = pMsg->lastIndex;
  pRspMsg->lastTerm = pMsg->lastTerm;
  pRspMsg->startTime = pMsg->startTime;
  pRspMsg->ack = pReceiver->ack;  // receiver maybe already closed
  pRspMsg->code = code;
  pRspMsg->snapBeginIndex = pReceiver->snapshotParam.start;

  // send msg
  if (syncNodeSendMsgById(&pRspMsg->destId, pSyncNode, &rpcMsg) != 0) {
    sRError(pReceiver, "failed to send snapshot receiver resp since %s", terrstr());
    return -1;
  }
  return 0;
}

static int32_t syncSnapBufferRecv(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend **ppMsg) {
  int32_t           code = 0;
  SSyncSnapBuffer  *pRcvBuf = pReceiver->pRcvBuf;
  SyncSnapshotSend *pMsg = ppMsg[0];
  terrno = TSDB_CODE_SUCCESS;

  taosThreadMutexLock(&pRcvBuf->mutex);

  if (pMsg->seq - pRcvBuf->start >= pRcvBuf->size) {
    terrno = TSDB_CODE_SYN_BUFFER_FULL;
    code = terrno;
    goto _out;
  }

  ASSERT(pRcvBuf->start <= pRcvBuf->cursor + 1 && pRcvBuf->cursor < pRcvBuf->end);

  if (pMsg->seq > pRcvBuf->cursor) {
    pRcvBuf->entries[pMsg->seq % pRcvBuf->size] = pMsg;
    ppMsg[0] = NULL;
    pRcvBuf->end = TMAX(pMsg->seq + 1, pRcvBuf->end);
  }

  for (int64_t seq = pRcvBuf->cursor + 1; seq < pRcvBuf->end; ++seq) {
    if (pRcvBuf->entries[seq]) {
      pRcvBuf->cursor = seq;
    } else {
      break;
    }
  }

  for (int64_t seq = pRcvBuf->start; seq <= pRcvBuf->cursor; ++seq) {
    if (snapshotReceiverGotData(pReceiver, pRcvBuf->entries[seq % pRcvBuf->size]) != 0) {
      code = terrno;
      if (code >= SYNC_SNAPSHOT_SEQ_INVALID) {
        code = TSDB_CODE_SYN_INTERNAL_ERROR;
      }
    }
    pRcvBuf->start = seq + 1;
    syncSnapSendRsp(pReceiver, pRcvBuf->entries[seq % pRcvBuf->size], code);
    rpcFreeCont(pRcvBuf->entries[seq % pRcvBuf->size]);
    pRcvBuf->entries[seq % pRcvBuf->size] = NULL;
    if (code) goto _out;
  }

_out:
  taosThreadMutexUnlock(&pRcvBuf->mutex);
  return code;
}

static int32_t syncNodeOnSnapshotReceive(SSyncNode *pSyncNode, SyncSnapshotSend **ppMsg) {
  // condition 4
  // transfering
  SyncSnapshotSend *pMsg = ppMsg[0];
  ASSERT(pMsg);
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;
  int64_t                timeNow = taosGetTimestampMs();
  int32_t                code = 0;

  if (snapshotReceiverSignatureCmp(pReceiver, pMsg) != 0) {
    terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
    sRError(pReceiver, "failed to receive snapshot data since %s.", terrstr());
    return syncSnapSendRsp(pReceiver, pMsg, terrno);
  }

  return syncSnapBufferRecv(pReceiver, ppMsg);
}

static int32_t syncNodeOnSnapshotEnd(SSyncNode *pSyncNode, SyncSnapshotSend *pMsg) {
  // condition 2
  // end, finish FSM
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;
  int64_t timeNow = taosGetTimestampMs();
  int32_t                code = 0;

  if (snapshotReceiverSignatureCmp(pReceiver, pMsg) != 0) {
    sRError(pReceiver, "snapshot end failed since startTime:%" PRId64 " not equal to msg startTime:%" PRId64,
            pReceiver->startTime, pMsg->startTime);
    terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
    code = terrno;
    goto _SEND_REPLY;
  }

  code = snapshotReceiverFinish(pReceiver, pMsg);
  if (code == 0) {
    snapshotReceiverStop(pReceiver);
  }

_SEND_REPLY:;
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
  pRspMsg->startTime = pMsg->startTime;
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
int32_t syncNodeOnSnapshot(SSyncNode *pSyncNode, SRpcMsg *pRpcMsg) {
  SyncSnapshotSend **ppMsg = (SyncSnapshotSend **)&pRpcMsg->pCont;
  SyncSnapshotSend      *pMsg = ppMsg[0];
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(pSyncNode, &pMsg->srcId)) {
    syncLogRecvSyncSnapshotSend(pSyncNode, pMsg, "not in my config");
    terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
    return -1;
  }

  if (pMsg->term < raftStoreGetTerm(pSyncNode)) {
    syncLogRecvSyncSnapshotSend(pSyncNode, pMsg, "reject since small term");
    terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
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
        sInfo("vgId:%d, receive prepare msg of snap replication. msg signature:(%" PRId64 ", %" PRId64 ")",
              pSyncNode->vgId, pMsg->term, pMsg->startTime);
        code = syncNodeOnSnapshotPrep(pSyncNode, pMsg);
      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_BEGIN) {
        sInfo("vgId:%d, receive begin msg of snap replication. msg signature:(%" PRId64 ", %" PRId64 ")",
              pSyncNode->vgId, pMsg->term, pMsg->startTime);
        code = syncNodeOnSnapshotBegin(pSyncNode, pMsg);
      } else if (pMsg->seq == SYNC_SNAPSHOT_SEQ_END) {
        sInfo("vgId:%d, receive end msg of snap replication. msg signature:(%" PRId64 ", %" PRId64 ")", pSyncNode->vgId,
              pMsg->term, pMsg->startTime);
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
        code = syncNodeOnSnapshotReceive(pSyncNode, ppMsg);
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
    sSError(pSender, "failed to prepare snapshot since beginIndex:%" PRId64 " larger than applyIndex:%" PRId64,
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

  sSInfo(pSender, "begin snapshot replication to dnode %d.", DID(&pSendMsg->destId));

  // send msg
  syncLogSendSyncSnapshotSend(pSyncNode, pSendMsg, "snapshot sender reply pre");
  if (syncNodeSendMsgById(&pSendMsg->destId, pSender->pSyncNode, &rpcMsg) != 0) {
    sSError(pSender, "prepare snapshot failed since send msg error");
    return -1;
  }

  return 0;
}

static int32_t snapshotSenderSignatureCmp(SSyncSnapshotSender *pSender, SyncSnapshotRsp *pMsg) {
  if (pSender->term < pMsg->term) return -1;
  if (pSender->term > pMsg->term) return 1;
  if (pSender->startTime < pMsg->startTime) return -1;
  if (pSender->startTime > pMsg->startTime) return 1;
  return 0;
}

static int32_t syncSnapBufferSend(SSyncSnapshotSender *pSender, SyncSnapshotRsp **ppMsg) {
  int32_t          code = 0;
  SSyncSnapBuffer *pSndBuf = pSender->pSndBuf;
  SyncSnapshotRsp *pMsg = ppMsg[0];

  taosThreadMutexLock(&pSndBuf->mutex);

  if (pMsg->ack - pSndBuf->start >= pSndBuf->size) {
    terrno = TSDB_CODE_SYN_BUFFER_FULL;
    code = terrno;
    goto _out;
  }

  ASSERT(pSndBuf->start <= pSndBuf->cursor + 1 && pSndBuf->cursor < pSndBuf->end);

  if (pMsg->ack > pSndBuf->cursor && pSndBuf->entries[pMsg->ack % pSndBuf->size] == NULL) {
    pSndBuf->entries[pMsg->ack % pSndBuf->size] = pMsg;
    ppMsg[0] = NULL;
    pSndBuf->end = TMAX(pMsg->ack + 1, pSndBuf->end);
  }

  for (int64_t ack = pSndBuf->cursor + 1; ack < pSndBuf->end; ++ack) {
    if (pSndBuf->entries[ack % pSndBuf->size]) {
      pSndBuf->cursor = ack;
    } else {
      break;
    }
  }

  for (int64_t ack = pSndBuf->start; ack <= pSndBuf->cursor; ++ack) {
    rpcFreeCont(pSndBuf->entries[ack % pSndBuf->size]);
    pSndBuf->entries[ack % pSndBuf->size] = NULL;
    pSndBuf->start = ack + 1;
  }

  pSender->ack = pSndBuf->start - 1;

  while (pSender->seq != SYNC_SNAPSHOT_SEQ_END && pSender->seq - pSndBuf->start < (pSndBuf->size >> 2)) {
    if (snapshotSend(pSender) != 0) {
      code = terrno;
      goto _out;
    }
    if (pSender->seq != SYNC_SNAPSHOT_SEQ_END) {
      pSndBuf->end = TMAX(pSender->seq + 1, pSndBuf->end);
    }
  }

  if (pSender->seq == SYNC_SNAPSHOT_SEQ_END && pSndBuf->end <= pSndBuf->start) {
    if (snapshotSend(pSender) != 0) {
      code = terrno;
      goto _out;
    }
  }
_out:
  taosThreadMutexUnlock(&pSndBuf->mutex);
  return code;
}

// sender on message
//
// condition 1 sender receives SYNC_SNAPSHOT_SEQ_END, close sender
// condition 2 sender receives ack, set seq = ack + 1, send msg from seq
// condition 3 sender receives error msg, just print error log
//
int32_t syncNodeOnSnapshotRsp(SSyncNode *pSyncNode, SRpcMsg *pRpcMsg) {
  SyncSnapshotRsp **ppMsg = (SyncSnapshotRsp **)&pRpcMsg->pCont;
  SyncSnapshotRsp  *pMsg = ppMsg[0];

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(pSyncNode, &pMsg->srcId)) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "maybe replica already dropped");
    terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
    return -1;
  }

  // get sender
  SSyncSnapshotSender *pSender = syncNodeGetSnapshotSender(pSyncNode, &pMsg->srcId);
  if (pSender == NULL) {
    syncLogRecvSyncSnapshotRsp(pSyncNode, pMsg, "sender is null");
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  if (!snapshotSenderIsStart(pSender)) {
    sSError(pSender, "snapshot sender not started yet. sender startTime:%" PRId64 ", msg startTime:%" PRId64,
            pSender->startTime, pMsg->startTime);
    return -1;
  }

  // check signature
  int32_t order = 0;
  if ((order = snapshotSenderSignatureCmp(pSender, pMsg)) > 0) {
    sSError(pSender, "received a stale snapshot rsp, msg signature:(%" PRId64 ", %" PRId64 "), ignore it.", pMsg->term,
            pMsg->startTime);
    terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
    return -1;
  } else if (order < 0) {
    sSError(pSender, "snapshot sender is stale. stop");
    terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
    goto _ERROR;
  }

  // state, term, seq/ack
  if (pSyncNode->state != TAOS_SYNC_STATE_LEADER) {
    sSError(pSender, "snapshot sender not leader");
    terrno = TSDB_CODE_SYN_NOT_LEADER;
    goto _ERROR;
  }

  SyncTerm currentTerm = raftStoreGetTerm(pSyncNode);
  if (pMsg->term != currentTerm) {
    sSError(pSender, "snapshot sender term mismatch, msg term:%" PRId64 " currentTerm:%" PRId64, pMsg->term,
            currentTerm);
    terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
    goto _ERROR;
  }

  if (pMsg->code != 0) {
    sSError(pSender, "snapshot sender receive error:%s 0x%x and stop sender", tstrerror(pMsg->code), pMsg->code);
    terrno = pMsg->code;
    goto _ERROR;
  }

  // prepare <begin, end>, send begin msg
  if (pMsg->ack == SYNC_SNAPSHOT_SEQ_PREP_SNAPSHOT) {
    return syncNodeOnSnapshotPrepRsp(pSyncNode, pSender, pMsg);
  }

  if (pSender->pReader == NULL || pSender->finish) {
    sSError(pSender, "snapshot sender invalid error:%s 0x%x, pReader:%p finish:%d", tstrerror(pMsg->code), pMsg->code,
            pSender->pReader, pSender->finish);
    terrno = pMsg->code;
    goto _ERROR;
  }

  if (pMsg->ack == SYNC_SNAPSHOT_SEQ_BEGIN) {
    sSInfo(pSender, "process seq begin");
    if (snapshotSend(pSender) != 0) {
      goto _ERROR;
    }
    return 0;
  }

  // receive ack is finish, close sender
  if (pMsg->ack == SYNC_SNAPSHOT_SEQ_END) {
    sSInfo(pSender, "process seq end");
    snapshotSenderStop(pSender, true);
    syncNodeReplicateReset(pSyncNode, &pMsg->srcId);
    return 0;
  }

  // send next msg
  if (syncSnapBufferSend(pSender, ppMsg) != 0) {
    sSError(pSender, "failed to send snapshot msg since %s. seq:%d", terrstr(), pSender->seq);
    goto _ERROR;
  }
  return 0;

_ERROR:
  snapshotSenderStop(pSender, false);
  syncNodeReplicateReset(pSyncNode, &pMsg->srcId);
  return -1;
}
