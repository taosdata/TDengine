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
#include "syncRespMgr.h"

int32_t syncNodeTimerRoutine(SSyncNode* ths) {
  syncNodeEventLog(ths, "timer routines");

#if 0
  if (ths->vgId != 1) {
    syncRespClean(ths->pSyncRespMgr);
  }
#endif

  return 0;
}

int32_t syncNodeOnTimeoutCb(SSyncNode* ths, SyncTimeout* pMsg) {
  int32_t ret = 0;
  syncTimeoutLog2("==syncNodeOnTimeoutCb==", pMsg);

  if (pMsg->timeoutType == SYNC_TIMEOUT_PING) {
    if (atomic_load_64(&ths->pingTimerLogicClockUser) <= pMsg->logicClock) {
      ++(ths->pingTimerCounter);

      // syncNodePingAll(ths);
      // syncNodePingPeers(ths);

      sTrace("vgId:%d, sync timeout, type:ping count:%d", ths->vgId, ths->pingTimerCounter);
      syncNodeTimerRoutine(ths);
    }

  } else if (pMsg->timeoutType == SYNC_TIMEOUT_ELECTION) {
    if (atomic_load_64(&ths->electTimerLogicClockUser) <= pMsg->logicClock) {
      ++(ths->electTimerCounter);
      sInfo("vgId:%d, sync timeout, type:election count:%d, electTimerLogicClockUser:%ld", ths->vgId,
            ths->electTimerCounter, ths->electTimerLogicClockUser);
      syncNodeElect(ths);
    }

  } else if (pMsg->timeoutType == SYNC_TIMEOUT_HEARTBEAT) {
    if (atomic_load_64(&ths->heartbeatTimerLogicClockUser) <= pMsg->logicClock) {
      ++(ths->heartbeatTimerCounter);
      sInfo("vgId:%d, sync timeout, type:replicate count:%d, heartbeatTimerLogicClockUser:%ld", ths->vgId,
            ths->heartbeatTimerCounter, ths->heartbeatTimerLogicClockUser);
      syncNodeReplicate(ths);
    }
  } else {
    sError("vgId:%d, unknown timeout-type:%d", ths->vgId, pMsg->timeoutType);
  }

  return ret;
}