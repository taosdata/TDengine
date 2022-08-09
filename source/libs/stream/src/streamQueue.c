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

#include "tstream.h"

SStreamQueue* streamQueueOpen() {
  SStreamQueue* pQueue = taosMemoryCalloc(1, sizeof(SStreamQueue));
  if (pQueue == NULL) return NULL;
  pQueue->queue = taosOpenQueue();
  pQueue->qall = taosAllocateQall();
  if (pQueue->queue == NULL || pQueue->qall == NULL) {
    goto FAIL;
  }
  pQueue->status = STREAM_QUEUE__SUCESS;
  return pQueue;
FAIL:
  if (pQueue->queue) taosCloseQueue(pQueue->queue);
  if (pQueue->qall) taosFreeQall(pQueue->qall);
  taosMemoryFree(pQueue);
  return NULL;
}

void streamQueueClose(SStreamQueue* queue) {
  while (1) {
    void* qItem = streamQueueNextItem(queue);
    if (qItem) {
      taosFreeQitem(qItem);
    } else {
      return;
    }
  }
}
