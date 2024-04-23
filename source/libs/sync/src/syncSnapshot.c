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
#include "tglobal.h"

static SyncIndex syncNodeGetSnapBeginIndex(SSyncNode *ths);

static void syncSnapBufferReset(SSyncSnapBuffer *pBuf) {
  for (int64_t i = pBuf->start; i < pBuf->end; ++i) {
    if (pBuf->entryDeleteCb) {
      pBuf->entryDeleteCb(pBuf->entries[i % pBuf->size]);
    }
    pBuf->entries[i % pBuf->size] = NULL;
  }
  pBuf->start = SYNC_SNAPSHOT_SEQ_BEGIN + 1;
  pBuf->end = pBuf->start;
  pBuf->cursor = pBuf->start - 1;
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
  pSender->sendingMS = SYNC_SNAPSHOT_RETRY_MS;
  pSender->pSyncNode = pSyncNode;
  pSender->replicaIndex = replicaIndex;
  pSender->term = raftStoreGetTerm(pSyncNode);
  pSender->startTime = -1;
  pSender->pSyncNode->pFsm->FpGetSnapshotInfo(pSender->pSyncNode->pFsm, &pSender->snapshot);
  pSender->finish = false;

  SSyncSnapBuffer *pSndBuf = syncSnapBufferCreate();
  if (pSndBuf == NULL) {
    taosMemoryFree(pSender);
    pSender = NULL;
    return NULL;
  }
  pSndBuf->entryDeleteCb = syncSnapBlockDestroy;
  pSender->pSndBuf = pSndBuf;

  syncSnapBufferReset(pSender->pSndBuf);
  return pSender;
}

void syncSnapBlockDestroy(void *ptr) {
  SyncSnapBlock *pBlk = ptr;
  if (pBlk->pBlock != NULL) {
    taosMemoryFree(pBlk->pBlock);
    pBlk->pBlock = NULL;
    pBlk->blockLen = 0;
  }
  taosMemoryFree(pBlk);
}

static int32_t snapshotSenderClearInfoData(SSyncSnapshotSender *pSender) {
  if (pSender->snapshotParam.data) {
    taosMemoryFree(pSender->snapshotParam.data);
    pSender->snapshotParam.data = NULL;
  }

  if (pSender->snapshot.data) {
    taosMemoryFree(pSender->snapshot.data);
    pSender->snapshot.data = NULL;
  }
  return 0;
}

void snapshotSenderDestroy(SSyncSnapshotSender *pSender) {
  if (pSender == NULL) return;

  // close reader
  if (pSender->pReader != NULL) {
    pSender->pSyncNode->pFsm->FpSnapshotStopRead(pSender->pSyncNode->pFsm, pSender->pReader);
    pSender->pReader = NULL;
  }

  // free snap buffer
  if (pSender->pSndBuf) {
    syncSnapBufferDestroy(&pSender->pSndBuf);
  }

  snapshotSenderClearInfoData(pSender);

  // free sender
  taosMemoryFree(pSender);
}

bool snapshotSenderIsStart(SSyncSnapshotSender *pSender) { return atomic_load_8(&pSender->start); }

int32_t snapshotSenderStart(SSyncSnapshotSender *pSender) {
  int32_t code = -1;

  int8_t started = atomic_val_compare_exchange_8(&pSender->start, false, true);
  if (started) return 0;

  pSender->seq = SYNC_SNAPSHOT_SEQ_PREP;
  pSender->ack = SYNC_SNAPSHOT_SEQ_INVALID;
  pSender->pReader = NULL;
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

  // Get snapshot info
  SSyncNode *pSyncNode = pSender->pSyncNode;
  SSnapshot  snapInfo = {.type = TDMT_SYNC_PREP_SNAPSHOT};
  if (pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapInfo) != 0) {
    sSError(pSender, "snapshot get info failure since %s", terrstr());
    goto _out;
  }

  void   *pData = snapInfo.data;
  int32_t type = (pData) ? snapInfo.type : 0;
  int32_t dataLen = 0;
  if (pData) {
    SSyncTLV *datHead = pData;
    if (datHead->typ != TDMT_SYNC_PREP_SNAPSHOT) {
      sSError(pSender, "unexpected data typ in data of snapshot info. typ: %d", datHead->typ);
      terrno = TSDB_CODE_INVALID_DATA_FMT;
      goto _out;
    }
    dataLen = sizeof(SSyncTLV) + datHead->len;
  }

  if (syncSnapSendMsg(pSender, pSender->seq, pData, dataLen, type) != 0) {
    goto _out;
  }

  SRaftId destId = pSender->pSyncNode->replicasId[pSender->replicaIndex];
  sSInfo(pSender, "snapshot sender start, to dnode:%d.", DID(&destId));
  code = 0;
_out:
  if (snapInfo.data) {
    taosMemoryFree(snapInfo.data);
    snapInfo.data = NULL;
  }
  return code;
}

void snapshotSenderStop(SSyncSnapshotSender *pSender, bool finish) {
  sSDebug(pSender, "snapshot sender stop, finish:%d reader:%p", finish, pSender->pReader);

  // update flag
  int8_t stopped = !atomic_val_compare_exchange_8(&pSender->start, true, false);
  if (stopped) return;
  taosThreadMutexLock(&pSender->pSndBuf->mutex);
  {
    pSender->finish = finish;

    // close reader
    if (pSender->pReader != NULL) {
      pSender->pSyncNode->pFsm->FpSnapshotStopRead(pSender->pSyncNode->pFsm, pSender->pReader);
      pSender->pReader = NULL;
    }

    syncSnapBufferReset(pSender->pSndBuf);

    snapshotSenderClearInfoData(pSender);

    SRaftId destId = pSender->pSyncNode->replicasId[pSender->replicaIndex];
    sSInfo(pSender, "snapshot sender stop, to dnode:%d, finish:%d", DID(&destId), finish);
  }
  taosThreadMutexUnlock(&pSender->pSndBuf->mutex);
}

int32_t syncSnapSendMsg(SSyncSnapshotSender *pSender, int32_t seq, void *pBlock, int32_t blockLen, int32_t typ) {
  int32_t code = -1;
  SRpcMsg rpcMsg = {0};

  if (syncBuildSnapshotSend(&rpcMsg, blockLen, pSender->pSyncNode->vgId) != 0) {
    sSError(pSender, "failed to build snap replication msg since %s", terrstr());
    goto _OUT;
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
  pMsg->seq = seq;

  if (pBlock != NULL && blockLen > 0) {
    memcpy(pMsg->data, pBlock, blockLen);
  }
  pMsg->payloadType = typ;

  // send msg
  if (syncNodeSendMsgById(&pMsg->destId, pSender->pSyncNode, &rpcMsg) != 0) {
    sSError(pSender, "failed to send snap replication msg since %s. seq:%d", terrstr(), seq);
    goto _OUT;
  }

  code = 0;
_OUT:
  return code;
}

// when sender receive ack, call this function to send msg from seq
// seq = ack + 1, already updated
static int32_t snapshotSend(SSyncSnapshotSender *pSender) {
  int32_t        code = -1;
  SyncSnapBlock *pBlk = NULL;

  if (pSender->seq < SYNC_SNAPSHOT_SEQ_END) {
    pSender->seq++;

    if (pSender->seq > SYNC_SNAPSHOT_SEQ_BEGIN) {
      pBlk = taosMemoryCalloc(1, sizeof(SyncSnapBlock));
      if (pBlk == NULL) {
        terrno = TSDB_CODE_OUT_OF_MEMORY;
        goto _OUT;
      }

      pBlk->seq = pSender->seq;

      // read data
      int32_t ret = pSender->pSyncNode->pFsm->FpSnapshotDoRead(pSender->pSyncNode->pFsm, pSender->pReader,
                                                               &pBlk->pBlock, &pBlk->blockLen);
      if (ret != 0) {
        sSError(pSender, "snapshot sender read failed since %s", terrstr());
        goto _OUT;
      }

      if (pBlk->blockLen > 0) {
        // has read data
        sSDebug(pSender, "snapshot sender continue to read, blockLen:%d seq:%d", pBlk->blockLen, pBlk->seq);
      } else {
        // read finish, update seq to end
        pSender->seq = SYNC_SNAPSHOT_SEQ_END;
        sSInfo(pSender, "snapshot sender read to the end");
        code = 0;
        goto _OUT;
      }
    }
  }

  ASSERT(pSender->seq >= SYNC_SNAPSHOT_SEQ_BEGIN && pSender->seq <= SYNC_SNAPSHOT_SEQ_END);

  // send msg
  int32_t blockLen = (pBlk) ? pBlk->blockLen : 0;
  void   *pBlock = (pBlk) ? pBlk->pBlock : NULL;
  if (syncSnapSendMsg(pSender, pSender->seq, pBlock, blockLen, 0) != 0) {
    goto _OUT;
  }

  // put in buffer
  int64_t nowMs = taosGetTimestampMs();
  if (pBlk) {
    ASSERT(pBlk->seq > SYNC_SNAPSHOT_SEQ_BEGIN && pBlk->seq < SYNC_SNAPSHOT_SEQ_END);
    pBlk->sendTimeMs = nowMs;
    pSender->pSndBuf->entries[pSender->seq % pSender->pSndBuf->size] = pBlk;
    pBlk = NULL;
    pSender->pSndBuf->end = TMAX(pSender->seq + 1, pSender->pSndBuf->end);
  }
  pSender->lastSendTime = nowMs;
  code = 0;

_OUT:;
  if (pBlk != NULL) {
    syncSnapBlockDestroy(pBlk);
    pBlk = NULL;
  }
  return code;
}

// send snapshot data from cache
int32_t snapshotReSend(SSyncSnapshotSender *pSender) {
  SSyncSnapBuffer *pSndBuf = pSender->pSndBuf;
  int32_t          code = -1;
  taosThreadMutexLock(&pSndBuf->mutex);
  if (pSender->pReader == NULL || pSender->finish || !snapshotSenderIsStart(pSender)) {
    goto _out;
  }

  for (int32_t seq = pSndBuf->cursor + 1; seq < pSndBuf->end; ++seq) {
    SyncSnapBlock *pBlk = pSndBuf->entries[seq % pSndBuf->size];
    ASSERT(pBlk);
    int64_t nowMs = taosGetTimestampMs();
    if (pBlk->acked || nowMs < pBlk->sendTimeMs + SYNC_SNAP_RESEND_MS) {
      continue;
    }
    if (syncSnapSendMsg(pSender, pBlk->seq, pBlk->pBlock, pBlk->blockLen, 0) != 0) {
      goto _out;
    }
    pBlk->sendTimeMs = nowMs;
  }

  if (pSender->seq != SYNC_SNAPSHOT_SEQ_END && pSndBuf->end <= pSndBuf->start) {
    if (snapshotSend(pSender) != 0) {
      goto _out;
    }
  }

  if (pSender->seq == SYNC_SNAPSHOT_SEQ_END && pSndBuf->end <= pSndBuf->start) {
    if (syncSnapSendMsg(pSender, pSender->seq, NULL, 0, 0) != 0) {
      goto _out;
    }
  }
  code = 0;
_out:;
  taosThreadMutexUnlock(&pSndBuf->mutex);
  return code;
}

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

  taosMsleep(1);

  int32_t code = snapshotSenderStart(pSender);
  if (code != 0) {
    sSError(pSender, "snapshot sender start error since %s", terrstr());
    return -1;
  }

  return 0;
}

// receiver
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

  SSyncSnapBuffer *pRcvBuf = syncSnapBufferCreate();
  if (pRcvBuf == NULL) {
    taosMemoryFree(pReceiver);
    pReceiver = NULL;
    return NULL;
  }
  pRcvBuf->entryDeleteCb = rpcFreeCont;
  pReceiver->pRcvBuf = pRcvBuf;

  syncSnapBufferReset(pReceiver->pRcvBuf);
  return pReceiver;
}

static int32_t snapshotReceiverClearInfoData(SSyncSnapshotReceiver *pReceiver) {
  if (pReceiver->snapshotParam.data) {
    taosMemoryFree(pReceiver->snapshotParam.data);
    pReceiver->snapshotParam.data = NULL;
  }

  if (pReceiver->snapshot.data) {
    taosMemoryFree(pReceiver->snapshot.data);
    pReceiver->snapshot.data = NULL;
  }
  return 0;
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

  snapshotReceiverClearInfoData(pReceiver);

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

  pReceiver->ack = SYNC_SNAPSHOT_SEQ_PREP;
  pReceiver->term = pPreMsg->term;
  pReceiver->fromId = pPreMsg->srcId;
  pReceiver->startTime = pPreMsg->startTime;

  pReceiver->snapshotParam.start = syncNodeGetSnapBeginIndex(pReceiver->pSyncNode);
  pReceiver->snapshotParam.end = -1;

  sRInfo(pReceiver, "snapshot receiver start, from dnode:%d.", DID(&pReceiver->fromId));
}

void snapshotReceiverStop(SSyncSnapshotReceiver *pReceiver) {
  sRDebug(pReceiver, "snapshot receiver stop, not apply, writer:%p", pReceiver->pWriter);

  int8_t stopped = !atomic_val_compare_exchange_8(&pReceiver->start, true, false);
  if (stopped) return;
  taosThreadMutexLock(&pReceiver->pRcvBuf->mutex);
  {
    if (pReceiver->pWriter != NULL) {
      int32_t ret = pReceiver->pSyncNode->pFsm->FpSnapshotStopWrite(pReceiver->pSyncNode->pFsm, pReceiver->pWriter,
                                                                    false, &pReceiver->snapshot);
      if (ret != 0) {
        sRError(pReceiver, "snapshot receiver stop write failed since %s", terrstr());
      }
      pReceiver->pWriter = NULL;
    } else {
      sRInfo(pReceiver, "snapshot receiver stop, writer is null");
    }

    syncSnapBufferReset(pReceiver->pRcvBuf);

    snapshotReceiverClearInfoData(pReceiver);
  }
  taosThreadMutexUnlock(&pReceiver->pRcvBuf->mutex);
}

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

    // get fsmState
    SSnapshot snapshot = {0};
    pReceiver->pSyncNode->pFsm->FpGetSnapshotInfo(pReceiver->pSyncNode->pFsm, &snapshot);
    pReceiver->pSyncNode->fsmState = snapshot.state;

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

static int32_t syncSnapReceiverExchgSnapInfo(SSyncNode *pSyncNode, SSyncSnapshotReceiver *pReceiver,
                                             SyncSnapshotSend *pMsg, SSnapshot *pInfo) {
  ASSERT(pMsg->payloadType == TDMT_SYNC_PREP_SNAPSHOT);
  int32_t code = 0;

  // copy snap info from leader
  void *data = taosMemoryCalloc(1, pMsg->dataLen);
  if (data == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = terrno;
    goto _out;
  }
  pInfo->data = data;
  data = NULL;
  memcpy(pInfo->data, pMsg->data, pMsg->dataLen);

  // exchange snap info
  if (pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, pInfo) != 0) {
    sRError(pReceiver, "failed to get snapshot info. type: %d", pMsg->payloadType);
    goto _out;
  }
  SSyncTLV *datHead = pInfo->data;
  if (datHead->typ != TDMT_SYNC_PREP_SNAPSHOT_REPLY) {
    sRError(pReceiver, "unexpected data typ in data of snapshot info. typ: %d", datHead->typ);
    code = TSDB_CODE_INVALID_DATA_FMT;
    goto _out;
  }
  int32_t dataLen = sizeof(SSyncTLV) + datHead->len;

  // save exchanged snap info
  SSnapshotParam *pParam = &pReceiver->snapshotParam;
  data = taosMemoryRealloc(pParam->data, dataLen);
  if (data == NULL) {
    sError("vgId:%d, failed to realloc memory for snapshot prep due to %s. dataLen:%d", pSyncNode->vgId,
           strerror(errno), dataLen);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    code = terrno;
    goto _out;
  }
  pParam->data = data;
  data = NULL;
  memcpy(pParam->data, pInfo->data, dataLen);

_out:
  return code;
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

_START_RECEIVER:
  if (snapshotReceiverIsStart(pReceiver)) {
    sRInfo(pReceiver, "snapshot receiver already start and force stop pre one");
    snapshotReceiverStop(pReceiver);
  }

  snapshotReceiverStart(pReceiver, pMsg);

_SEND_REPLY:;

  SSnapshot snapInfo = {.type = TDMT_SYNC_PREP_SNAPSHOT_REPLY};
  int32_t   dataLen = 0;
  if (pMsg->payloadType == TDMT_SYNC_PREP_SNAPSHOT) {
    if (syncSnapReceiverExchgSnapInfo(pSyncNode, pReceiver, pMsg, &snapInfo) != 0) {
      goto _out;
    }
    SSyncTLV *datHead = snapInfo.data;
    dataLen = sizeof(SSyncTLV) + datHead->len;
  }

  // send response
  int32_t type = (snapInfo.data) ? snapInfo.type : 0;
  if (syncSnapSendRsp(pReceiver, pMsg, snapInfo.data, dataLen, type, code) != 0) {
    code = terrno;
    goto _out;
  }

_out:
  if (snapInfo.data) {
    taosMemoryFree(snapInfo.data);
    snapInfo.data = NULL;
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

  SyncIndex beginIndex = syncNodeGetSnapBeginIndex(pSyncNode);
  if (pReceiver->snapshotParam.start != beginIndex) {
    sRError(pReceiver, "snapshot begin index is changed unexpectedly. sver:%" PRId64 ", beginIndex:%" PRId64,
            pReceiver->snapshotParam.start, beginIndex);
    goto _SEND_REPLY;
  }

  code = 0;
_SEND_REPLY:
  if (code != 0 && terrno != 0) {
    code = terrno;
  }

  // send response
  if (syncSnapSendRsp(pReceiver, pMsg, NULL, 0, 0, code) != 0) {
    return -1;
  }

  return code;
}

int32_t syncSnapSendRsp(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pMsg, void *pBlock, int32_t blockLen,
                        int32_t type, int32_t code) {
  SSyncNode *pSyncNode = pReceiver->pSyncNode;
  // build msg
  SRpcMsg rpcMsg = {0};
  if (syncBuildSnapshotSendRsp(&rpcMsg, blockLen, pSyncNode->vgId)) {
    sRError(pReceiver, "failed to build snapshot receiver resp since %s", terrstr());
    return -1;
  }

  SyncSnapshotRsp *pRspMsg = rpcMsg.pCont;
  pRspMsg->srcId = pSyncNode->myRaftId;
  pRspMsg->destId = pMsg->srcId;
  pRspMsg->term = pMsg->term;
  pRspMsg->lastIndex = pMsg->lastIndex;
  pRspMsg->lastTerm = pMsg->lastTerm;
  pRspMsg->startTime = pMsg->startTime;
  pRspMsg->ack = pMsg->seq;
  pRspMsg->code = code;
  pRspMsg->snapBeginIndex = pReceiver->snapshotParam.start;
  pRspMsg->payloadType = type;

  if (pBlock != NULL && blockLen > 0) {
    memcpy(pRspMsg->data, pBlock, blockLen);
  }

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
    if (pRcvBuf->entries[pMsg->seq % pRcvBuf->size]) {
      pRcvBuf->entryDeleteCb(pRcvBuf->entries[pMsg->seq % pRcvBuf->size]);
    }
    pRcvBuf->entries[pMsg->seq % pRcvBuf->size] = pMsg;
    ppMsg[0] = NULL;
    pRcvBuf->end = TMAX(pMsg->seq + 1, pRcvBuf->end);
  } else if (pMsg->seq < pRcvBuf->start) {
    syncSnapSendRsp(pReceiver, pMsg, NULL, 0, 0, code);
    goto _out;
  }

  for (int64_t seq = pRcvBuf->cursor + 1; seq < pRcvBuf->end; ++seq) {
    if (pRcvBuf->entries[seq % pRcvBuf->size]) {
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
    syncSnapSendRsp(pReceiver, pRcvBuf->entries[seq % pRcvBuf->size], NULL, 0, 0, code);
    pRcvBuf->entryDeleteCb(pRcvBuf->entries[seq % pRcvBuf->size]);
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
    return syncSnapSendRsp(pReceiver, pMsg, NULL, 0, 0, terrno);
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
  if (syncBuildSnapshotSendRsp(&rpcMsg, 0, pSyncNode->vgId) != 0) {
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

int32_t syncNodeOnSnapshot(SSyncNode *pSyncNode, SRpcMsg *pRpcMsg) {
  SyncSnapshotSend **ppMsg = (SyncSnapshotSend **)&pRpcMsg->pCont;
  SyncSnapshotSend      *pMsg = ppMsg[0];
  SSyncSnapshotReceiver *pReceiver = pSyncNode->pNewNodeReceiver;
  int32_t                code = 0;

  // if already drop replica, do not process
  if (!syncNodeInRaftGroup(pSyncNode, &pMsg->srcId)) {
    syncLogRecvSyncSnapshotSend(pSyncNode, pMsg, "not in my config");
    terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
    return -1;
  }

  if (pMsg->term < raftStoreGetTerm(pSyncNode)) {
    sRError(pReceiver, "reject snap replication with smaller term. msg term:%" PRId64 ", seq:%d", pMsg->term,
            pMsg->seq);
    terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
    syncSnapSendRsp(pReceiver, pMsg, NULL, 0, 0, terrno);
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

  if (pSyncNode->state != TAOS_SYNC_STATE_FOLLOWER && pSyncNode->state != TAOS_SYNC_STATE_LEARNER) {
    sRError(pReceiver, "snapshot receiver not a follower or learner");
    return -1;
  }

  if (pMsg->seq < SYNC_SNAPSHOT_SEQ_PREP || pMsg->seq > SYNC_SNAPSHOT_SEQ_END) {
    sRError(pReceiver, "snap replication msg with invalid seq:%d", pMsg->seq);
    return -1;
  }

  // prepare
  if (pMsg->seq == SYNC_SNAPSHOT_SEQ_PREP) {
    sInfo("vgId:%d, prepare snap replication. msg signature:(%" PRId64 ", %" PRId64 ")", pSyncNode->vgId, pMsg->term,
          pMsg->startTime);
    code = syncNodeOnSnapshotPrep(pSyncNode, pMsg);
    goto _out;
  }

  // begin
  if (pMsg->seq == SYNC_SNAPSHOT_SEQ_BEGIN) {
    sInfo("vgId:%d, begin snap replication. msg signature:(%" PRId64 ", %" PRId64 ")", pSyncNode->vgId, pMsg->term,
          pMsg->startTime);
    code = syncNodeOnSnapshotBegin(pSyncNode, pMsg);
    goto _out;
  }

  // data
  if (pMsg->seq > SYNC_SNAPSHOT_SEQ_BEGIN && pMsg->seq < SYNC_SNAPSHOT_SEQ_END) {
    code = syncNodeOnSnapshotReceive(pSyncNode, ppMsg);
    goto _out;
  }

  // end
  if (pMsg->seq == SYNC_SNAPSHOT_SEQ_END) {
    sInfo("vgId:%d, end snap replication. msg signature:(%" PRId64 ", %" PRId64 ")", pSyncNode->vgId, pMsg->term,
          pMsg->startTime);
    code = syncNodeOnSnapshotEnd(pSyncNode, pMsg);
    if (code != 0) {
      sRError(pReceiver, "failed to end snapshot.");
      goto _out;
    }

    code = syncLogBufferReInit(pSyncNode->pLogBuf, pSyncNode);
    if (code != 0) {
      sRError(pReceiver, "failed to reinit log buffer since %s", terrstr());
    }
    goto _out;
  }

_out:;
  syncNodeResetElectTimer(pSyncNode);
  return code;
}

static int32_t syncSnapSenderExchgSnapInfo(SSyncNode *pSyncNode, SSyncSnapshotSender *pSender, SyncSnapshotRsp *pMsg) {
  ASSERT(pMsg->payloadType == TDMT_SYNC_PREP_SNAPSHOT_REPLY);

  SSyncTLV *datHead = (void *)pMsg->data;
  if (datHead->typ != pMsg->payloadType) {
    sSError(pSender, "unexpected data type in data of SyncSnapshotRsp. typ: %d", datHead->typ);
    terrno = TSDB_CODE_INVALID_DATA_FMT;
    return -1;
  }
  int32_t dataLen = sizeof(SSyncTLV) + datHead->len;

  SSnapshotParam *pParam = &pSender->snapshotParam;
  void           *data = taosMemoryRealloc(pParam->data, dataLen);
  if (data == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  memcpy(data, pMsg->data, dataLen);

  pParam->data = data;
  data = NULL;
  sSInfo(pSender, "data of snapshot param. len: %d", datHead->len);
  return 0;
}

// sender
static int32_t syncNodeOnSnapshotPrepRsp(SSyncNode *pSyncNode, SSyncSnapshotSender *pSender, SyncSnapshotRsp *pMsg) {
  int32_t   code = -1;
  SSnapshot snapshot = {0};

  taosThreadMutexLock(&pSender->pSndBuf->mutex);
  pSyncNode->pFsm->FpGetSnapshotInfo(pSyncNode->pFsm, &snapshot);

  // prepare <begin, end>
  pSender->snapshotParam.start = pMsg->snapBeginIndex;
  pSender->snapshotParam.end = snapshot.lastApplyIndex;

  sSInfo(pSender, "prepare snapshot, recv-begin:%" PRId64 ", snapshot.last:%" PRId64 ", snapshot.term:%" PRId64,
         pMsg->snapBeginIndex, snapshot.lastApplyIndex, snapshot.lastApplyTerm);

  // update sender
  pSender->snapshot = snapshot;

  // start reader
  if (pMsg->payloadType == TDMT_SYNC_PREP_SNAPSHOT_REPLY) {
    if (syncSnapSenderExchgSnapInfo(pSyncNode, pSender, pMsg) != 0) {
      goto _out;
    }
  }

  code = pSyncNode->pFsm->FpSnapshotStartRead(pSyncNode->pFsm, &pSender->snapshotParam, &pSender->pReader);
  if (code != 0) {
    sSError(pSender, "prepare snapshot failed since %s", terrstr());
    goto _out;
  }

  // update next index
  syncIndexMgrSetIndex(pSyncNode->pNextIndex, &pMsg->srcId, snapshot.lastApplyIndex + 1);

  code = snapshotSend(pSender);

_out:
  taosThreadMutexUnlock(&pSender->pSndBuf->mutex);
  return code;
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
  if (snapshotSenderSignatureCmp(pSender, pMsg) != 0) {
    code = terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
    goto _out;
  }

  if (pSender->pReader == NULL || pSender->finish || !snapshotSenderIsStart(pSender)) {
    code = terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    goto _out;
  }

  if (pMsg->ack - pSndBuf->start >= pSndBuf->size) {
    code = terrno = TSDB_CODE_SYN_BUFFER_FULL;
    goto _out;
  }

  ASSERT(pSndBuf->start <= pSndBuf->cursor + 1 && pSndBuf->cursor < pSndBuf->end);

  if (pMsg->ack > pSndBuf->cursor && pMsg->ack < pSndBuf->end) {
    SyncSnapBlock *pBlk = pSndBuf->entries[pMsg->ack % pSndBuf->size];
    ASSERT(pBlk);
    pBlk->acked = 1;
  }

  for (int64_t ack = pSndBuf->cursor + 1; ack < pSndBuf->end; ++ack) {
    SyncSnapBlock *pBlk = pSndBuf->entries[ack % pSndBuf->size];
    if (pBlk->acked) {
      pSndBuf->cursor = ack;
    } else {
      break;
    }
  }

  for (int64_t ack = pSndBuf->start; ack <= pSndBuf->cursor; ++ack) {
    pSndBuf->entryDeleteCb(pSndBuf->entries[ack % pSndBuf->size]);
    pSndBuf->entries[ack % pSndBuf->size] = NULL;
    pSndBuf->start = ack + 1;
  }

  while (pSender->seq != SYNC_SNAPSHOT_SEQ_END && pSender->seq - pSndBuf->start < tsSnapReplMaxWaitN) {
    if (snapshotSend(pSender) != 0) {
      code = terrno;
      goto _out;
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
    sSWarn(pSender, "ignore a stale snap rsp, msg signature:(%" PRId64 ", %" PRId64 ").", pMsg->term, pMsg->startTime);
    terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
    return -1;
  } else if (order < 0) {
    sSError(pSender, "snapshot sender is stale. stop");
    terrno = TSDB_CODE_SYN_MISMATCHED_SIGNATURE;
    goto _ERROR;
  }

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

  // send begin
  if (pMsg->ack == SYNC_SNAPSHOT_SEQ_PREP) {
    sSInfo(pSender, "process prepare rsp");
    if (syncNodeOnSnapshotPrepRsp(pSyncNode, pSender, pMsg) != 0) {
      goto _ERROR;
    }
  }

  // send msg of data or end
  if (pMsg->ack >= SYNC_SNAPSHOT_SEQ_BEGIN && pMsg->ack < SYNC_SNAPSHOT_SEQ_END) {
    if (syncSnapBufferSend(pSender, ppMsg) != 0) {
      sSError(pSender, "failed to replicate snap since %s. seq:%d, pReader:%p, finish:%d", terrstr(), pSender->seq,
              pSender->pReader, pSender->finish);
      goto _ERROR;
    }
  }

  // end
  if (pMsg->ack == SYNC_SNAPSHOT_SEQ_END) {
    sSInfo(pSender, "process end rsp");
    snapshotSenderStop(pSender, true);
    syncNodeReplicateReset(pSyncNode, &pMsg->srcId);
  }

  return 0;

_ERROR:
  snapshotSenderStop(pSender, false);
  syncNodeReplicateReset(pSyncNode, &pMsg->srcId);
  return -1;
}
