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

SSyncSnapshotSender *snapshotSenderCreate(SSyncNode *pSyncNode) { return NULL; }

void snapshotSenderDestroy(SSyncSnapshotSender *pSender) {}

int32_t snapshotSend(SSyncSnapshotSender *pSender) { return 0; }

cJSON *snapshotSender2Json(SSyncSnapshotSender *pSender) { return NULL; }

char *snapshotSender2Str(SSyncSnapshotSender *pSender) { return NULL; }

SSyncSnapshotReceiver *snapshotReceiverCreate(SSyncNode *pSyncNode) { return NULL; }

void snapshotReceiverDestroy(SSyncSnapshotReceiver *pReceiver) {}

int32_t snapshotReceive(SSyncSnapshotReceiver *pReceiver) { return 0; }

cJSON *snapshotReceiver2Json(SSyncSnapshotReceiver *pReceiver) { return NULL; }

char *snapshotReceiver2Str(SSyncSnapshotReceiver *pReceiver) { return NULL; }
