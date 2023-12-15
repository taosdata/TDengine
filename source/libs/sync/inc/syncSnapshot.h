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

#ifndef _TD_LIBS_SYNC_SNAPSHOT_H
#define _TD_LIBS_SYNC_SNAPSHOT_H

#ifdef __cplusplus
extern "C" {
#endif

#include "syncInt.h"

#define SYNC_SNAPSHOT_SEQ_FORCE_CLOSE  -3
#define SYNC_SNAPSHOT_SEQ_INVALID      -2
#define SYNC_SNAPSHOT_SEQ_PREP         -1
#define SYNC_SNAPSHOT_SEQ_BEGIN        0
#define SYNC_SNAPSHOT_SEQ_END          0x7FFFFFFF

#define SYNC_SNAPSHOT_RETRY_MS 5000

typedef struct SSyncSnapBuffer {
  void         *entries[TSDB_SYNC_SNAP_BUFFER_SIZE];
  int64_t       start;
  int64_t       cursor;
  int64_t       end;
  int64_t       size;
  TdThreadMutex mutex;
  void (*entryDeleteCb)(void *ptr);
} SSyncSnapBuffer;

typedef struct SyncSnapBlock {
  int32_t seq;
  int8_t  acked;
  int64_t sendTimeMs;

  int16_t blockType;
  void   *pBlock;
  int32_t blockLen;
} SyncSnapBlock;

void syncSnapBlockDestroy(void *ptr);

typedef struct SSyncSnapshotSender {
  int8_t         start;
  int32_t        seq;
  int32_t        ack;
  void          *pReader;
  SSnapshotParam snapshotParam;
  SSnapshot      snapshot;
  SSyncCfg       lastConfig;
  int64_t        sendingMS;
  SyncTerm       term;
  int64_t        startTime;
  int64_t        lastSendTime;
  bool           finish;

  // ring buffer for ack
  SSyncSnapBuffer *pSndBuf;

  // init when create
  SSyncNode *pSyncNode;
  int32_t    replicaIndex;
} SSyncSnapshotSender;

SSyncSnapshotSender *snapshotSenderCreate(SSyncNode *pSyncNode, int32_t replicaIndex);
void                 snapshotSenderDestroy(SSyncSnapshotSender *pSender);
bool                 snapshotSenderIsStart(SSyncSnapshotSender *pSender);
int32_t              snapshotSenderStart(SSyncSnapshotSender *pSender);
void                 snapshotSenderStop(SSyncSnapshotSender *pSender, bool finish);
int32_t              snapshotReSend(SSyncSnapshotSender *pSender);

typedef struct SSyncSnapshotReceiver {
  // update when prep snapshot
  int8_t   start;
  int32_t  ack;
  SyncTerm term;
  SRaftId  fromId;
  int64_t  startTime;

  // update when begin
  void          *pWriter;
  SSnapshotParam snapshotParam;
  SSnapshot      snapshot;

  // buffer
  SSyncSnapBuffer *pRcvBuf;

  // init when create
  SSyncNode *pSyncNode;
} SSyncSnapshotReceiver;

SSyncSnapshotReceiver *snapshotReceiverCreate(SSyncNode *pSyncNode, SRaftId fromId);
void                   snapshotReceiverDestroy(SSyncSnapshotReceiver *pReceiver);
void                   snapshotReceiverStart(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pBeginMsg);
void                   snapshotReceiverStop(SSyncSnapshotReceiver *pReceiver);
bool                   snapshotReceiverIsStart(SSyncSnapshotReceiver *pReceiver);

// on message
// int32_t syncNodeOnSnapshot(SSyncNode *ths, const SRpcMsg *pMsg);
// int32_t syncNodeOnSnapshotRsp(SSyncNode *ths, const SRpcMsg *pMsg);

SyncIndex syncNodeGetSnapshotConfigIndex(SSyncNode *pSyncNode, SyncIndex snapshotLastApplyIndex);

// start

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_SNAPSHOT_H*/
