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

int32_t snapshotSenderStart(SSyncSnapshotSender *pSender) { return 0; }

int32_t snapshotSenderStop(SSyncSnapshotSender *pSender) { return 0; }

int32_t snapshotSend(SSyncSnapshotSender *pSender) { return 0; }

int32_t snapshotReceiverStart(SSyncSnapshotReceiver *pReceiver) { return 0; }

int32_t snapshotReceiverStop(SSyncSnapshotReceiver *pReceiver) { return 0; }

int32_t snapshotReceive(SSyncSnapshotReceiver *pReceiver) { return 0; }
