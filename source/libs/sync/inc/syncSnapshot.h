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
#include "taosdef.h"

typedef struct SSyncSnapshotSender {
  bool       isStart;
  int32_t    progressIndex;
  void *     pCurrentBlock;
  int32_t    len;
  SSnapshot *pSnapshot;
  SSyncNode *pSyncNode;
} SSyncSnapshotSender;

SSyncSnapshotSender *snapshotSenderCreate(SSyncNode *pSyncNode);
void                 snapshotSenderDestroy(SSyncSnapshotSender *pSender);
int32_t              snapshotSend(SSyncSnapshotSender *pSender);
cJSON *              snapshotSender2Json(SSyncSnapshotSender *pSender);
char *               snapshotSender2Str(SSyncSnapshotSender *pSender);

typedef struct SSyncSnapshotReceiver {
  bool       isStart;
  int32_t    progressIndex;
  void *     pCurrentBlock;
  int32_t    len;
  SSnapshot *pSnapshot;
  SSyncNode *pSyncNode;
} SSyncSnapshotReceiver;

SSyncSnapshotReceiver *snapshotReceiverCreate(SSyncNode *pSyncNode);
void                   snapshotReceiverDestroy(SSyncSnapshotReceiver *pReceiver);
int32_t                snapshotReceive(SSyncSnapshotReceiver *pReceiver);
cJSON *                snapshotReceiver2Json(SSyncSnapshotReceiver *pReceiver);
char *                 snapshotReceiver2Str(SSyncSnapshotReceiver *pReceiver);

#ifdef __cplusplus
}
#endif

#endif /*_TD_LIBS_SYNC_SNAPSHOT_H*/
