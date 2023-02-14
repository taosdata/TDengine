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
#include "syncTimeout.h"
#include "syncElection.h"
#include "syncRaftCfg.h"
#include "syncRaftLog.h"
#include "syncReplication.h"
#include "syncRespMgr.h"
#include "syncSnapshot.h"
#include "syncUtil.h"

static void syncNodeCleanConfigIndex(SSyncNode* ths) {
#if 0
  int32_t   newArrIndex = 0;
  SyncIndex newConfigIndexArr[MAX_CONFIG_INDEX_COUNT] = {0};
  SSnapshot snapshot = {0};

  ths->pFsm->FpGetSnapshotInfo(ths->pFsm, &snapshot);
  if (snapshot.lastApplyIndex != SYNC_INDEX_INVALID) {
    for (int32_t i = 0; i < ths->raftCfg.configIndexCount; ++i) {
      if (ths->raftCfg.configIndexArr[i] < snapshot.lastConfigIndex) {
        // pass
      } else {
        // save
        newConfigIndexArr[newArrIndex] = ths->raftCfg.configIndexArr[i];
        ++newArrIndex;
      }
    }

    int32_t oldCnt = ths->raftCfg.configIndexCount;
    ths->raftCfg.configIndexCount = newArrIndex;
    memcpy(ths->raftCfg.configIndexArr, newConfigIndexArr, sizeof(newConfigIndexArr));

    int32_t code = syncWriteCfgFile(ths);
    if (code != 0) {
      sNFatal(ths, "failed to persist cfg");
    } else {
      sNTrace(ths, "clean config index arr, old-cnt:%d, new-cnt:%d", oldCnt, ths->raftCfg.configIndexCount);
    }
  }
#endif
}

static int32_t syncNodeTimerRoutine(SSyncNode* ths) {
  ths->tmrRoutineNum++;

  if (ths->tmrRoutineNum % 60 == 0 && ths->replicaNum > 1) {
    sNInfo(ths, "timer routines");
  } else {
    sNTrace(ths, "timer routines");
  }

  // timer replicate
  syncNodeReplicate(ths);

  // clean mnode index
  if (syncNodeIsMnode(ths)) {
    syncNodeCleanConfigIndex(ths);
  }

  int64_t timeNow = taosGetTimestampMs();

  for (int i = 0; i < ths->peersNum; ++i) {
    SSyncSnapshotSender* pSender = syncNodeGetSnapshotSender(ths, &(ths->peersId[i]));
    if (pSender != NULL) {
      if (ths->isStart && ths->state == TAOS_SYNC_STATE_LEADER && pSender->start &&
          timeNow - pSender->lastSendTime > SYNC_SNAP_RESEND_MS) {
        snapshotReSend(pSender);
      } else {
        sTrace("vgId:%d, do not resend: nstart%d, now:%" PRId64 ", lstsend:%" PRId64 ", diff:%" PRId64, ths->vgId,
               ths->isStart, timeNow, pSender->lastSendTime, timeNow - pSender->lastSendTime);
      }
    }
  }

  if (atomic_load_64(&ths->snapshottingIndex) != SYNC_INDEX_INVALID) {
    // end timeout wal snapshot
    if (timeNow - ths->snapshottingTime > SYNC_DEL_WAL_MS &&
        atomic_load_64(&ths->snapshottingIndex) != SYNC_INDEX_INVALID) {
      SSyncLogStoreData* pData = ths->pLogStore->data;
      int32_t            code = walEndSnapshot(pData->pWal);
      if (code != 0) {
        sNError(ths, "timer wal snapshot end error since:%s", terrstr());
        return -1;
      } else {
        sNTrace(ths, "wal snapshot end, index:%" PRId64, atomic_load_64(&ths->snapshottingIndex));
        atomic_store_64(&ths->snapshottingIndex, SYNC_INDEX_INVALID);
      }
    }
  }

  if (!syncNodeIsMnode(ths)) {
    syncRespClean(ths->pSyncRespMgr);
  }

  return 0;
}

int32_t syncNodeOnTimeout(SSyncNode* ths, const SRpcMsg* pRpc) {
  int32_t      ret = 0;
  SyncTimeout* pMsg = pRpc->pCont;

  syncLogRecvTimer(ths, pMsg, "");

  if (pMsg->timeoutType == SYNC_TIMEOUT_PING) {
    if (atomic_load_64(&ths->pingTimerLogicClockUser) <= pMsg->logicClock) {
      ++(ths->pingTimerCounter);

      // syncNodePingAll(ths);
      // syncNodePingPeers(ths);

      syncNodeTimerRoutine(ths);
    }

  } else if (pMsg->timeoutType == SYNC_TIMEOUT_ELECTION) {
    if (atomic_load_64(&ths->electTimerLogicClock) <= pMsg->logicClock) {
      ++(ths->electTimerCounter);

      syncNodeElect(ths);
    }

  } else if (pMsg->timeoutType == SYNC_TIMEOUT_HEARTBEAT) {
    if (atomic_load_64(&ths->heartbeatTimerLogicClockUser) <= pMsg->logicClock) {
      ++(ths->heartbeatTimerCounter);
      sTrace("vgId:%d, sync timer, type:replicate count:%" PRIu64 ", lc-user:%" PRIu64, ths->vgId,
             ths->heartbeatTimerCounter, ths->heartbeatTimerLogicClockUser);

      // syncNodeReplicate(ths, true);
    }

  } else {
    sError("vgId:%d, recv unknown timer-type:%d", ths->vgId, pMsg->timeoutType);
  }

  return ret;
}
