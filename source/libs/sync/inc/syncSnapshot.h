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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "cJSON.h"
#include "syncInt.h"
#include "syncMessage.h"
#include "taosdef.h"

#define SYNC_SNAPSHOT_SEQ_INVALID -1
#define SYNC_SNAPSHOT_SEQ_FORCE_CLOSE -2
#define SYNC_SNAPSHOT_SEQ_BEGIN 0
#define SYNC_SNAPSHOT_SEQ_END 0x7FFFFFFF

#define SYNC_SNAPSHOT_RETRY_MS 5000

//---------------------------------------------------
typedef struct SSyncSnapshotSender {
  bool           start;
  int32_t        seq;
  int32_t        ack;
  void *         pReader;
  void *         pCurrentBlock;
  int32_t        blockLen;
  SSnapshotParam snapshotParam;
  SSnapshot      snapshot;
  SSyncCfg       lastConfig;
  int64_t        sendingMS;
  SSyncNode *    pSyncNode;
  int32_t        replicaIndex;
  SyncTerm       term;
  SyncTerm       privateTerm;
  bool           finish;
} SSyncSnapshotSender;

SSyncSnapshotSender *snapshotSenderCreate(SSyncNode *pSyncNode, int32_t replicaIndex);
void                 snapshotSenderDestroy(SSyncSnapshotSender *pSender);
bool                 snapshotSenderIsStart(SSyncSnapshotSender *pSender);
int32_t              snapshotSenderStart(SSyncSnapshotSender *pSender, SSnapshotParam snapshotParam, SSnapshot snapshot,
                                         void *pReader);
int32_t              snapshotSenderStop(SSyncSnapshotSender *pSender, bool finish);
int32_t              snapshotSend(SSyncSnapshotSender *pSender);
int32_t              snapshotReSend(SSyncSnapshotSender *pSender);

cJSON *snapshotSender2Json(SSyncSnapshotSender *pSender);
char * snapshotSender2Str(SSyncSnapshotSender *pSender);
char * snapshotSender2SimpleStr(SSyncSnapshotSender *pSender, char *event);

//---------------------------------------------------
typedef struct SSyncSnapshotReceiver {
  bool           start;
  int32_t        ack;
  void *         pWriter;
  SyncTerm       term;
  SyncTerm       privateTerm;
  SSnapshotParam snapshotParam;
  SSnapshot      snapshot;
  SRaftId        fromId;
  SSyncNode *    pSyncNode;

} SSyncSnapshotReceiver;

SSyncSnapshotReceiver *snapshotReceiverCreate(SSyncNode *pSyncNode, SRaftId fromId);
void                   snapshotReceiverDestroy(SSyncSnapshotReceiver *pReceiver);
int32_t                snapshotReceiverStart(SSyncSnapshotReceiver *pReceiver, SyncSnapshotSend *pBeginMsg);
int32_t                snapshotReceiverStop(SSyncSnapshotReceiver *pReceiver);
bool                   snapshotReceiverIsStart(SSyncSnapshotReceiver *pReceiver);

cJSON *snapshotReceiver2Json(SSyncSnapshotReceiver *pReceiver);
char * snapshotReceiver2Str(SSyncSnapshotReceiver *pReceiver);
char * snapshotReceiver2SimpleStr(SSyncSnapshotReceiver *pReceiver, char *event);

//---------------------------------------------------
// on message
int32_t syncNodeOnSnapshotSendCb(SSyncNode *ths, SyncSnapshotSend *pMsg);
int32_t syncNodeOnSnapshotRspCb(SSyncNode *ths, SyncSnapshotRsp *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_SNAPSHOT_H*/
