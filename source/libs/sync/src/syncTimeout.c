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

#include "syncTimeout.h"
#include "syncElection.h"
#include "syncReplication.h"

int32_t syncNodeOnTimeoutCb(SSyncNode* ths, SyncTimeout* pMsg) {
  int32_t ret = 0;
  syncTimeoutLog2("==syncNodeOnTimeoutCb==", pMsg);

  if (pMsg->timeoutType == SYNC_TIMEOUT_PING) {
    if (atomic_load_64(&ths->pingTimerLogicClockUser) <= pMsg->logicClock) {
      ++(ths->pingTimerCounter);
      syncNodePingAll(ths);
    }

  } else if (pMsg->timeoutType == SYNC_TIMEOUT_ELECTION) {
    if (atomic_load_64(&ths->electTimerLogicClockUser) <= pMsg->logicClock) {
      ++(ths->electTimerCounter);
      syncNodeElect(ths);
    }

  } else if (pMsg->timeoutType == SYNC_TIMEOUT_HEARTBEAT) {
    if (atomic_load_64(&ths->heartbeatTimerLogicClockUser) <= pMsg->logicClock) {
      ++(ths->heartbeatTimerCounter);
      syncNodeReplicate(ths);
    }
  } else {
    sTrace("unknown timeoutType:%d", pMsg->timeoutType);
  }

  return ret;
}