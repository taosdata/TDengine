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
#include "mndAcct.h"
#include "mndCluster.h"
#include "mndCompact.h"
#include "mndCompactDetail.h"
#include "mndConsumer.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndFunc.h"
#include "mndGrant.h"
#include "mndIndex.h"
#include "mndInfoSchema.h"
#include "mndMnode.h"
#include "mndPerfSchema.h"
#include "mndPrivilege.h"
#include "mndProfile.h"
#include "mndQnode.h"
#include "mndQuery.h"
#include "mndShow.h"
#include "mndSma.h"
#include "mndSnode.h"
#include "mndStb.h"
#include "mndStream.h"
#include "mndSubscribe.h"
#include "mndSync.h"
#include "mndTelem.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "mndView.h"

static inline int32_t mndAcquireRpc(SMnode *pMnode) {
  int32_t code = 0;
  taosThreadRwlockRdlock(&pMnode->lock);
  if (pMnode->stopped) {
    terrno = TSDB_CODE_APP_IS_STOPPING;
    code = -1;
  } else if (!mndIsLeader(pMnode)) {
    code = -1;
  } else {
#if 1
    atomic_add_fetch_32(&pMnode->rpcRef, 1);
#else
    int32_t ref = atomic_add_fetch_32(&pMnode->rpcRef, 1);
    mTrace("mnode rpc is acquired, ref:%d", ref);
#endif
  }
  taosThreadRwlockUnlock(&pMnode->lock);
  return code;
}

static inline void mndReleaseRpc(SMnode *pMnode) {
  taosThreadRwlockRdlock(&pMnode->lock);
#if 1
  atomic_sub_fetch_32(&pMnode->rpcRef, 1);
#else
  int32_t ref = atomic_sub_fetch_32(&pMnode->rpcRef, 1);
  mTrace("mnode rpc is released, ref:%d", ref);
#endif
  taosThreadRwlockUnlock(&pMnode->lock);
}

static void *mndBuildTimerMsg(int32_t *pContLen) {
  SMTimerReq timerReq = {0};

  int32_t contLen = tSerializeSMTimerMsg(NULL, 0, &timerReq);
  if (contLen <= 0) return NULL;
  void *pReq = rpcMallocCont(contLen);
  if (pReq == NULL) return NULL;

  tSerializeSMTimerMsg(pReq, contLen, &timerReq);
  *pContLen = contLen;
  return pReq;
}

static void *mndBuildCheckpointTickMsg(int32_t *pContLen, int64_t sec) {
  SMStreamTickReq timerReq = {
      .tick = sec,
  };

  int32_t contLen = tSerializeSMStreamTickMsg(NULL, 0, &timerReq);
  if (contLen <= 0) return NULL;
  void *pReq = rpcMallocCont(contLen);
  if (pReq == NULL) return NULL;

  tSerializeSMStreamTickMsg(pReq, contLen, &timerReq);
  *pContLen = contLen;
  return pReq;
}

static void mndPullupTrans(SMnode *pMnode) {
  mTrace("pullup trans msg");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_TRANS_TIMER, .pCont = pReq, .contLen = contLen};
    tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  }
}

static void mndPullupCompacts(SMnode *pMnode) {
  mTrace("pullup compact timer msg");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_COMPACT_TIMER, .pCont = pReq, .contLen = contLen};
    tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  }
}

static void mndPullupTtl(SMnode *pMnode) {
  mTrace("pullup ttl");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  SRpcMsg rpcMsg = {.msgType = TDMT_MND_TTL_TIMER, .pCont = pReq, .contLen = contLen};
  tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
}

static void mndPullupTrimDb(SMnode *pMnode) {
  mTrace("pullup trim");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  SRpcMsg rpcMsg = {.msgType = TDMT_MND_TRIM_DB_TIMER, .pCont = pReq, .contLen = contLen};
  tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
}

static void mndCalMqRebalance(SMnode *pMnode) {
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_TMQ_TIMER, .pCont = pReq, .contLen = contLen};
    tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  }
}

static void mndStreamCheckpointTick(SMnode *pMnode, int64_t sec) {
  int32_t contLen = 0;
  void   *pReq = mndBuildCheckpointTickMsg(&contLen, sec);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_STREAM_CHECKPOINT_TIMER, .pCont = pReq, .contLen = contLen};
    tmsgPutToQueue(&pMnode->msgCb, READ_QUEUE, &rpcMsg);
  }
}

static void mndStreamCheckpointRemain(SMnode *pMnode) {
  int32_t contLen = 0;
  void   *pReq = mndBuildCheckpointTickMsg(&contLen, 0);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_STREAM_CHECKPOINT_CANDIDITATE, .pCont = pReq, .contLen = contLen};
    tmsgPutToQueue(&pMnode->msgCb, READ_QUEUE, &rpcMsg);
  }
}

static void mndStreamCheckNode(SMnode *pMnode) {
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_NODECHECK_TIMER, .pCont = pReq, .contLen = contLen};
    tmsgPutToQueue(&pMnode->msgCb, READ_QUEUE, &rpcMsg);
  }
}

static void mndPullupTelem(SMnode *pMnode) {
  mTrace("pullup telem msg");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_TELEM_TIMER, .pCont = pReq, .contLen = contLen};
    tmsgPutToQueue(&pMnode->msgCb, READ_QUEUE, &rpcMsg);
  }
}

static void mndPullupGrant(SMnode *pMnode) {
  mTrace("pullup grant msg");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {
        .msgType = TDMT_MND_GRANT_HB_TIMER, .pCont = pReq, .contLen = contLen, .info.ahandle = (void *)0x9527};
    tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  }
}

static void mndIncreaseUpTime(SMnode *pMnode) {
  mTrace("increate uptime");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {
        .msgType = TDMT_MND_UPTIME_TIMER, .pCont = pReq, .contLen = contLen, .info.ahandle = (void *)0x9528};
    tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  }
}

static void mndSetVgroupOffline(SMnode *pMnode, int32_t dnodeId, int64_t curMs) {
  SSdb *pSdb = pMnode->pSdb;

  void *pIter = NULL;
  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    bool stateChanged = false;
    for (int32_t vg = 0; vg < pVgroup->replica; ++vg) {
      SVnodeGid *pGid = &pVgroup->vnodeGid[vg];
      if (pGid->dnodeId == dnodeId) {
        if (pGid->syncState != TAOS_SYNC_STATE_OFFLINE) {
          mInfo(
              "vgId:%d, state changed by offline check, old state:%s restored:%d canRead:%d new state:error restored:0 "
              "canRead:0",
              pVgroup->vgId, syncStr(pGid->syncState), pGid->syncRestore, pGid->syncCanRead);
          pGid->syncState = TAOS_SYNC_STATE_OFFLINE;
          pGid->syncRestore = 0;
          pGid->syncCanRead = 0;
          pGid->startTimeMs = 0;
          stateChanged = true;
        }
        break;
      }
    }

    if (stateChanged) {
      SDbObj *pDb = mndAcquireDb(pMnode, pVgroup->dbName);
      if (pDb != NULL && pDb->stateTs != curMs) {
        mInfo("db:%s, stateTs changed by offline check, old newTs:%" PRId64 " newTs:%" PRId64, pDb->name, pDb->stateTs,
              curMs);
        pDb->stateTs = curMs;
      }
      mndReleaseDb(pMnode, pDb);
    }

    sdbRelease(pSdb, pVgroup);
  }
}

static void mndCheckDnodeOffline(SMnode *pMnode) {
  mTrace("check dnode offline");
  if (mndAcquireRpc(pMnode) != 0) return;

  SSdb   *pSdb = pMnode->pSdb;
  int64_t curMs = taosGetTimestampMs();

  void *pIter = NULL;
  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;

    bool online = mndIsDnodeOnline(pDnode, curMs);
    if (!online) {
      mInfo("dnode:%d, in offline state", pDnode->id);
      mndSetVgroupOffline(pMnode, pDnode->id, curMs);
    }

    sdbRelease(pSdb, pDnode);
  }

  mndReleaseRpc(pMnode);
}

static bool mnodeIsNotLeader(SMnode *pMnode) {
  terrno = 0;
  taosThreadRwlockRdlock(&pMnode->lock);
  SSyncState state = syncGetState(pMnode->syncMgmt.sync);
  if (terrno != 0) {
    taosThreadRwlockUnlock(&pMnode->lock);
    return true;
  }

  if (state.state != TAOS_SYNC_STATE_LEADER) {
    taosThreadRwlockUnlock(&pMnode->lock);
    terrno = TSDB_CODE_SYN_NOT_LEADER;
    return true;
  }
  if (!state.restored || !pMnode->restored) {
    taosThreadRwlockUnlock(&pMnode->lock);
    terrno = TSDB_CODE_SYN_RESTORING;
    return true;
  }
  taosThreadRwlockUnlock(&pMnode->lock);
  return false;
}

static int32_t minCronTime() {
  int32_t min = INT32_MAX;
  min = TMIN(min, tsTtlPushIntervalSec);
  min = TMIN(min, tsTrimVDbIntervalSec);
  min = TMIN(min, tsTransPullupInterval);
  min = TMIN(min, tsCompactPullupInterval);
  min = TMIN(min, tsMqRebalanceInterval);
  min = TMIN(min, tsStreamCheckpointInterval);
  min = TMIN(min, 6);  // checkpointRemain
  min = TMIN(min, tsStreamNodeCheckInterval);

  int64_t telemInt = TMIN(60, (tsTelemInterval - 1));
  min = TMIN(min, telemInt);
  min = TMIN(min, tsGrantHBInterval);
  min = TMIN(min, tsUptimeInterval);

  return min <= 1 ? 2 : min;
}
void mndDoTimerPullupTask(SMnode *pMnode, int64_t sec) {
  if (sec % tsTtlPushIntervalSec == 0) {
    mndPullupTtl(pMnode);
  }

  if (sec % tsTrimVDbIntervalSec == 0) {
    mndPullupTrimDb(pMnode);
  }

  if (sec % tsTransPullupInterval == 0) {
    mndPullupTrans(pMnode);
  }

  if (sec % tsCompactPullupInterval == 0) {
    mndPullupCompacts(pMnode);
  }

  if (sec % tsMqRebalanceInterval == 0) {
    mndCalMqRebalance(pMnode);
  }

  if (sec % tsStreamCheckpointInterval == 0) {
    mndStreamCheckpointTick(pMnode, sec);
  }

  if (sec % 5 == 0) {
    mndStreamCheckpointRemain(pMnode);
  }

  if (sec % tsStreamNodeCheckInterval == 0) {
    mndStreamCheckNode(pMnode);
  }

  if (sec % tsTelemInterval == (TMIN(60, (tsTelemInterval - 1)))) {
    mndPullupTelem(pMnode);
  }

  if (sec % tsGrantHBInterval == 0) {
    mndPullupGrant(pMnode);
  }

  if (sec % tsUptimeInterval == 0) {
    mndIncreaseUpTime(pMnode);
  }
}
void mndDoTimerCheckTask(SMnode *pMnode, int64_t sec) {
  if (sec % (tsStatusInterval * 5) == 0) {
    mndCheckDnodeOffline(pMnode);
  }
  if (sec % (MNODE_TIMEOUT_SEC / 2) == 0) {
    mndSyncCheckTimeout(pMnode);
  }
}
static void *mndThreadFp(void *param) {
  SMnode *pMnode = param;
  int64_t lastTime = 0;
  setThreadName("mnode-timer");

  while (1) {
    lastTime++;
    taosMsleep(100);
    if (mndGetStop(pMnode)) break;
    if (lastTime % 10 != 0) continue;

    int64_t sec = lastTime / 10;
    mndDoTimerCheckTask(pMnode, sec);

    int64_t minCron = minCronTime();
    if (sec % minCron == 0 && mnodeIsNotLeader(pMnode)) {
      // not leader, do nothing
      mTrace("timer not process since mnode is not leader, reason: %s", tstrerror(terrno));
      terrno = 0;
      continue;
    }
    mndDoTimerPullupTask(pMnode, sec);
  }

  return NULL;
}

static int32_t mndInitTimer(SMnode *pMnode) {
  TdThreadAttr thAttr;
  taosThreadAttrInit(&thAttr);
  taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if (taosThreadCreate(&pMnode->thread, &thAttr, mndThreadFp, pMnode) != 0) {
    mError("failed to create timer thread since %s", strerror(errno));
    return -1;
  }

  taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("mnode-timer", "initialized");
  return 0;
}

static void mndCleanupTimer(SMnode *pMnode) {
  if (taosCheckPthreadValid(pMnode->thread)) {
    taosThreadJoin(pMnode->thread, NULL);
    taosThreadClear(&pMnode->thread);
  }
}

static int32_t mndCreateDir(SMnode *pMnode, const char *path) {
  pMnode->path = taosStrdup(path);
  if (pMnode->path == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (taosMkDir(pMnode->path) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

static int32_t mndInitWal(SMnode *pMnode) {
  char path[PATH_MAX + 20] = {0};
  snprintf(path, sizeof(path), "%s%swal", pMnode->path, TD_DIRSEP);
  SWalCfg cfg = {
      .vgId = 1,
      .fsyncPeriod = 0,
      .rollPeriod = -1,
      .segSize = -1,
      .retentionPeriod = 0,
      .retentionSize = 0,
      .level = TAOS_WAL_FSYNC,
  };

  pMnode->pWal = walOpen(path, &cfg);
  if (pMnode->pWal == NULL) {
    mError("failed to open wal since %s. wal:%s", terrstr(), path);
    return -1;
  }

  return 0;
}

static void mndCloseWal(SMnode *pMnode) {
  if (pMnode->pWal != NULL) {
    walClose(pMnode->pWal);
    pMnode->pWal = NULL;
  }
}

static int32_t mndInitSdb(SMnode *pMnode) {
  SSdbOpt opt = {0};
  opt.path = pMnode->path;
  opt.pMnode = pMnode;
  opt.pWal = pMnode->pWal;

  pMnode->pSdb = sdbInit(&opt);
  if (pMnode->pSdb == NULL) {
    return -1;
  }

  return 0;
}

static int32_t mndOpenSdb(SMnode *pMnode) {
  int32_t code = 0;
  if (!pMnode->deploy) {
    code = sdbReadFile(pMnode->pSdb);
  }

  atomic_store_64(&pMnode->applied, pMnode->pSdb->commitIndex);
  return code;
}

static void mndCleanupSdb(SMnode *pMnode) {
  if (pMnode->pSdb) {
    sdbCleanup(pMnode->pSdb);
    pMnode->pSdb = NULL;
  }
}

static int32_t mndAllocStep(SMnode *pMnode, char *name, MndInitFp initFp, MndCleanupFp cleanupFp) {
  SMnodeStep step = {0};
  step.name = name;
  step.initFp = initFp;
  step.cleanupFp = cleanupFp;
  if (taosArrayPush(pMnode->pSteps, &step) == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static int32_t mndInitSteps(SMnode *pMnode) {
  if (mndAllocStep(pMnode, "mnode-wal", mndInitWal, mndCloseWal) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-sdb", mndInitSdb, mndCleanupSdb) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-trans", mndInitTrans, mndCleanupTrans) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-cluster", mndInitCluster, mndCleanupCluster) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-mnode", mndInitMnode, mndCleanupMnode) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-qnode", mndInitQnode, mndCleanupQnode) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-snode", mndInitSnode, mndCleanupSnode) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-dnode", mndInitDnode, mndCleanupDnode) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-user", mndInitUser, mndCleanupUser) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-grant", mndInitGrant, mndCleanupGrant) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-privilege", mndInitPrivilege, mndCleanupPrivilege) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-acct", mndInitAcct, mndCleanupAcct) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-stream", mndInitStream, mndCleanupStream) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-topic", mndInitTopic, mndCleanupTopic) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-consumer", mndInitConsumer, mndCleanupConsumer) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-subscribe", mndInitSubscribe, mndCleanupSubscribe) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-vgroup", mndInitVgroup, mndCleanupVgroup) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-stb", mndInitStb, mndCleanupStb) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-sma", mndInitSma, mndCleanupSma) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-idx", mndInitIdx, mndCleanupIdx) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-infos", mndInitInfos, mndCleanupInfos) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-perfs", mndInitPerfs, mndCleanupPerfs) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-db", mndInitDb, mndCleanupDb) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-func", mndInitFunc, mndCleanupFunc) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-view", mndInitView, mndCleanupView) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-compact", mndInitCompact, mndCleanupCompact) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-compact-detail", mndInitCompactDetail, mndCleanupCompactDetail) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-sdb", mndOpenSdb, NULL) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-profile", mndInitProfile, mndCleanupProfile) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-show", mndInitShow, mndCleanupShow) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-query", mndInitQuery, mndCleanupQuery) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-sync", mndInitSync, mndCleanupSync) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-telem", mndInitTelem, mndCleanupTelem) != 0) return -1;

  return 0;
}

static void mndCleanupSteps(SMnode *pMnode, int32_t pos) {
  if (pMnode->pSteps == NULL) return;

  if (pos == -1) {
    pos = taosArrayGetSize(pMnode->pSteps) - 1;
  }

  for (int32_t s = pos; s >= 0; s--) {
    SMnodeStep *pStep = taosArrayGet(pMnode->pSteps, s);
    mInfo("%s will cleanup", pStep->name);
    if (pStep->cleanupFp != NULL) {
      (*pStep->cleanupFp)(pMnode);
    }
  }

  taosArrayClear(pMnode->pSteps);
  taosArrayDestroy(pMnode->pSteps);
  pMnode->pSteps = NULL;
}

static int32_t mndExecSteps(SMnode *pMnode) {
  int32_t size = taosArrayGetSize(pMnode->pSteps);
  for (int32_t pos = 0; pos < size; pos++) {
    SMnodeStep *pStep = taosArrayGet(pMnode->pSteps, pos);
    if (pStep->initFp == NULL) continue;

    if ((*pStep->initFp)(pMnode) != 0) {
      int32_t code = terrno;
      mError("%s exec failed since %s, start to cleanup", pStep->name, terrstr());
      mndCleanupSteps(pMnode, pos);
      terrno = code;
      return -1;
    } else {
      mInfo("%s is initialized", pStep->name);
      tmsgReportStartup(pStep->name, "initialized");
    }
  }

  pMnode->clusterId = mndGetClusterId(pMnode);
  return 0;
}

static void mndSetOptions(SMnode *pMnode, const SMnodeOpt *pOption) {
  pMnode->msgCb = pOption->msgCb;
  pMnode->selfDnodeId = pOption->dnodeId;
  pMnode->syncMgmt.selfIndex = pOption->selfIndex;
  pMnode->syncMgmt.numOfReplicas = pOption->numOfReplicas;
  pMnode->syncMgmt.numOfTotalReplicas = pOption->numOfTotalReplicas;
  pMnode->syncMgmt.lastIndex = pOption->lastIndex;
  memcpy(pMnode->syncMgmt.replicas, pOption->replicas, sizeof(pOption->replicas));
  memcpy(pMnode->syncMgmt.nodeRoles, pOption->nodeRoles, sizeof(pOption->nodeRoles));
}

SMnode *mndOpen(const char *path, const SMnodeOpt *pOption) {
  mInfo("start to open mnode in %s", path);

  SMnode *pMnode = taosMemoryCalloc(1, sizeof(SMnode));
  if (pMnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to open mnode since %s", terrstr());
    return NULL;
  }
  memset(pMnode, 0, sizeof(SMnode));

  char timestr[24] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &pMnode->checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  mndSetOptions(pMnode, pOption);

  pMnode->deploy = pOption->deploy;
  pMnode->pSteps = taosArrayInit(24, sizeof(SMnodeStep));
  if (pMnode->pSteps == NULL) {
    taosMemoryFree(pMnode);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to open mnode since %s", terrstr());
    return NULL;
  }

  int32_t code = mndCreateDir(pMnode, path);
  if (code != 0) {
    code = terrno;
    mError("failed to open mnode since %s", terrstr());
    mndClose(pMnode);
    terrno = code;
    return NULL;
  }

  code = mndInitSteps(pMnode);
  if (code != 0) {
    code = terrno;
    mError("failed to open mnode since %s", terrstr());
    mndClose(pMnode);
    terrno = code;
    return NULL;
  }

  code = mndExecSteps(pMnode);
  if (code != 0) {
    code = terrno;
    mError("failed to open mnode since %s", terrstr());
    mndClose(pMnode);
    terrno = code;
    return NULL;
  }

  mInfo("mnode open successfully");
  return pMnode;
}

void mndPreClose(SMnode *pMnode) {
  if (pMnode != NULL) {
    syncLeaderTransfer(pMnode->syncMgmt.sync);
    syncPreStop(pMnode->syncMgmt.sync);
    sdbWriteFile(pMnode->pSdb, 0);
  }
}

void mndClose(SMnode *pMnode) {
  if (pMnode != NULL) {
    mInfo("start to close mnode");
    mndCleanupSteps(pMnode, -1);
    taosMemoryFreeClear(pMnode->path);
    taosMemoryFreeClear(pMnode);
    mInfo("mnode is closed");
  }
}

int32_t mndStart(SMnode *pMnode) {
  mndSyncStart(pMnode);
  if (pMnode->deploy) {
    if (sdbDeploy(pMnode->pSdb) != 0) {
      mError("failed to deploy sdb while start mnode");
      return -1;
    }
    mndSetRestored(pMnode, true);
  }

  grantReset(pMnode, TSDB_GRANT_ALL, 0);

  return mndInitTimer(pMnode);
}

int32_t mndIsCatchUp(SMnode *pMnode) {
  int64_t rid = pMnode->syncMgmt.sync;
  return syncIsCatchUp(rid);
}

ESyncRole mndGetRole(SMnode *pMnode) {
  int64_t rid = pMnode->syncMgmt.sync;
  return syncGetRole(rid);
}

void mndStop(SMnode *pMnode) {
  mndSetStop(pMnode);
  mndSyncStop(pMnode);
  mndCleanupTimer(pMnode);
}

int32_t mndProcessSyncMsg(SRpcMsg *pMsg) {
  SMnode    *pMnode = pMsg->info.node;
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;

  const STraceId *trace = &pMsg->info.traceId;
  mGTrace("vgId:1, sync msg:%p will be processed, type:%s", pMsg, TMSG_INFO(pMsg->msgType));

  int32_t code = syncProcessMsg(pMgmt->sync, pMsg);
  if (code != 0) {
    mGError("vgId:1, failed to process sync msg:%p type:%s since %s", pMsg, TMSG_INFO(pMsg->msgType), terrstr());
  }

  return code;
}

static int32_t mndCheckMnodeState(SRpcMsg *pMsg) {
  if (!IsReq(pMsg)) return 0;
  if (pMsg->msgType == TDMT_SCH_QUERY || pMsg->msgType == TDMT_SCH_MERGE_QUERY ||
      pMsg->msgType == TDMT_SCH_QUERY_CONTINUE || pMsg->msgType == TDMT_SCH_QUERY_HEARTBEAT ||
      pMsg->msgType == TDMT_SCH_FETCH || pMsg->msgType == TDMT_SCH_MERGE_FETCH || pMsg->msgType == TDMT_SCH_DROP_TASK ||
      pMsg->msgType == TDMT_SCH_TASK_NOTIFY) {
    return 0;
  }

  SMnode *pMnode = pMsg->info.node;
  taosThreadRwlockRdlock(&pMnode->lock);
  if (pMnode->stopped) {
    taosThreadRwlockUnlock(&pMnode->lock);
    terrno = TSDB_CODE_APP_IS_STOPPING;
    return -1;
  }

  terrno = 0;
  SSyncState state = syncGetState(pMnode->syncMgmt.sync);
  if (terrno != 0) {
    taosThreadRwlockUnlock(&pMnode->lock);
    return -1;
  }

  if (state.state != TAOS_SYNC_STATE_LEADER) {
    taosThreadRwlockUnlock(&pMnode->lock);
    terrno = TSDB_CODE_SYN_NOT_LEADER;
    goto _OVER;
  }

  if (!state.restored || !pMnode->restored) {
    taosThreadRwlockUnlock(&pMnode->lock);
    terrno = TSDB_CODE_SYN_RESTORING;
    goto _OVER;
  }

#if 1
  atomic_add_fetch_32(&pMnode->rpcRef, 1);
#else
  int32_t ref = atomic_add_fetch_32(&pMnode->rpcRef, 1);
  mTrace("mnode rpc is acquired, ref:%d", ref);
#endif

  taosThreadRwlockUnlock(&pMnode->lock);
  return 0;

_OVER:
  if (pMsg->msgType == TDMT_MND_TMQ_TIMER || pMsg->msgType == TDMT_MND_TELEM_TIMER ||
      pMsg->msgType == TDMT_MND_TRANS_TIMER || pMsg->msgType == TDMT_MND_TTL_TIMER ||
      pMsg->msgType == TDMT_MND_TRIM_DB_TIMER || pMsg->msgType == TDMT_MND_UPTIME_TIMER ||
      pMsg->msgType == TDMT_MND_COMPACT_TIMER || pMsg->msgType == TDMT_MND_NODECHECK_TIMER ||
      pMsg->msgType == TDMT_MND_GRANT_HB_TIMER || pMsg->msgType == TDMT_MND_STREAM_CHECKPOINT_CANDIDITATE ||
      pMsg->msgType == TDMT_MND_STREAM_CHECKPOINT_TIMER || pMsg->msgType == TDMT_MND_STREAM_REQ_CHKPT) {
    mTrace("timer not process since mnode restored:%d stopped:%d, sync restored:%d role:%s ", pMnode->restored,
           pMnode->stopped, state.restored, syncStr(state.state));
    return -1;
  }

  const STraceId *trace = &pMsg->info.traceId;
  SEpSet          epSet = {0};
  int32_t         tmpCode = terrno;
  mndGetMnodeEpSet(pMnode, &epSet);
  terrno = tmpCode;

  mGDebug(
      "msg:%p, type:%s failed to process since %s, mnode restored:%d stopped:%d, sync restored:%d "
      "role:%s, redirect numOfEps:%d inUse:%d, type:%s",
      pMsg, TMSG_INFO(pMsg->msgType), terrstr(), pMnode->restored, pMnode->stopped, state.restored,
      syncStr(state.restored), epSet.numOfEps, epSet.inUse, TMSG_INFO(pMsg->msgType));

  if (epSet.numOfEps <= 0) return -1;

  for (int32_t i = 0; i < epSet.numOfEps; ++i) {
    mDebug("mnode index:%d, ep:%s:%u", i, epSet.eps[i].fqdn, epSet.eps[i].port);
  }

  int32_t contLen = tSerializeSEpSet(NULL, 0, &epSet);
  pMsg->info.rsp = rpcMallocCont(contLen);
  if (pMsg->info.rsp != NULL) {
    tSerializeSEpSet(pMsg->info.rsp, contLen, &epSet);
    pMsg->info.hasEpSet = 1;
    pMsg->info.rspLen = contLen;
  }

  return -1;
}

int32_t mndProcessRpcMsg(SRpcMsg *pMsg) {
  SMnode         *pMnode = pMsg->info.node;
  const STraceId *trace = &pMsg->info.traceId;

  MndMsgFp fp = pMnode->msgFp[TMSG_INDEX(pMsg->msgType)];
  if (fp == NULL) {
    mGError("msg:%p, failed to get msg handle, app:%p type:%s", pMsg, pMsg->info.ahandle, TMSG_INFO(pMsg->msgType));
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    return -1;
  }

  if (mndCheckMnodeState(pMsg) != 0) return -1;

  mGTrace("msg:%p, start to process in mnode, app:%p type:%s", pMsg, pMsg->info.ahandle, TMSG_INFO(pMsg->msgType));
  int32_t code = (*fp)(pMsg);
  mndReleaseRpc(pMnode);

  if (code == TSDB_CODE_ACTION_IN_PROGRESS) {
    mGTrace("msg:%p, won't response immediately since in progress", pMsg);
  } else if (code == 0) {
    mGTrace("msg:%p, successfully processed", pMsg);
  } else {
    if (code == -1) {
      code = terrno;
    }
    mGError("msg:%p, failed to process since %s, app:%p type:%s", pMsg, tstrerror(code), pMsg->info.ahandle,
            TMSG_INFO(pMsg->msgType));
  }

  return code;
}

void mndSetMsgHandle(SMnode *pMnode, tmsg_t msgType, MndMsgFp fp) {
  tmsg_t type = TMSG_INDEX(msgType);
  if (type < TDMT_MAX) {
    pMnode->msgFp[type] = fp;
  }
}

// Note: uid 0 is reserved
int64_t mndGenerateUid(const char *name, int32_t len) {
  int32_t hashval = MurmurHash3_32(name, len);
  do {
    int64_t us = taosGetTimestampUs();
    int64_t x = (us & 0x000000FFFFFFFFFF) << 24;
    int64_t uuid = x + ((hashval & ((1ul << 16) - 1ul)) << 8) + (taosRand() & ((1ul << 8) - 1ul));
    if (uuid) {
      return llabs(uuid);
    }
  } while (true);
}

int32_t mndGetMonitorInfo(SMnode *pMnode, SMonClusterInfo *pClusterInfo, SMonVgroupInfo *pVgroupInfo,
                          SMonStbInfo *pStbInfo, SMonGrantInfo *pGrantInfo) {
  if (mndAcquireRpc(pMnode) != 0) return -1;

  SSdb   *pSdb = pMnode->pSdb;
  int64_t ms = taosGetTimestampMs();

  pClusterInfo->dnodes = taosArrayInit(sdbGetSize(pSdb, SDB_DNODE), sizeof(SMonDnodeDesc));
  pClusterInfo->mnodes = taosArrayInit(sdbGetSize(pSdb, SDB_MNODE), sizeof(SMonMnodeDesc));
  pVgroupInfo->vgroups = taosArrayInit(sdbGetSize(pSdb, SDB_VGROUP), sizeof(SMonVgroupDesc));
  pStbInfo->stbs = taosArrayInit(sdbGetSize(pSdb, SDB_STB), sizeof(SMonStbDesc));
  if (pClusterInfo->dnodes == NULL || pClusterInfo->mnodes == NULL || pVgroupInfo->vgroups == NULL ||
      pStbInfo->stbs == NULL) {
    mndReleaseRpc(pMnode);
    return -1;
  }

  // cluster info
  tstrncpy(pClusterInfo->version, version, sizeof(pClusterInfo->version));
  pClusterInfo->monitor_interval = tsMonitorInterval;
  pClusterInfo->connections_total = mndGetNumOfConnections(pMnode);
  pClusterInfo->dbs_total = sdbGetSize(pSdb, SDB_DB);
  pClusterInfo->stbs_total = sdbGetSize(pSdb, SDB_STB);
  pClusterInfo->topics_toal = sdbGetSize(pSdb, SDB_TOPIC);
  pClusterInfo->streams_total = sdbGetSize(pSdb, SDB_STREAM);

  void *pIter = NULL;
  while (1) {
    SDnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SMonDnodeDesc desc = {0};
    desc.dnode_id = pObj->id;
    tstrncpy(desc.dnode_ep, pObj->ep, sizeof(desc.dnode_ep));
    if (mndIsDnodeOnline(pObj, ms)) {
      tstrncpy(desc.status, "ready", sizeof(desc.status));
    } else {
      tstrncpy(desc.status, "offline", sizeof(desc.status));
    }
    taosArrayPush(pClusterInfo->dnodes, &desc);
    sdbRelease(pSdb, pObj);
  }

  pIter = NULL;
  while (1) {
    SMnodeObj *pObj = NULL;
    pIter = sdbFetch(pSdb, SDB_MNODE, pIter, (void **)&pObj);
    if (pIter == NULL) break;

    SMonMnodeDesc desc = {0};
    desc.mnode_id = pObj->id;
    tstrncpy(desc.mnode_ep, pObj->pDnode->ep, sizeof(desc.mnode_ep));

    if (pObj->id == pMnode->selfDnodeId) {
      pClusterInfo->first_ep_dnode_id = pObj->id;
      tstrncpy(pClusterInfo->first_ep, pObj->pDnode->ep, sizeof(pClusterInfo->first_ep));
      //pClusterInfo->master_uptime = (float)mndGetClusterUpTime(pMnode) / 86400.0f;
      pClusterInfo->master_uptime = mndGetClusterUpTime(pMnode);
      // pClusterInfo->master_uptime = (ms - pObj->stateStartTime) / (86400000.0f);
      tstrncpy(desc.role, syncStr(TAOS_SYNC_STATE_LEADER), sizeof(desc.role));
      desc.syncState = TAOS_SYNC_STATE_LEADER;
    } else {
      tstrncpy(desc.role, syncStr(pObj->syncState), sizeof(desc.role));
      desc.syncState = pObj->syncState;
    }
    taosArrayPush(pClusterInfo->mnodes, &desc);
    sdbRelease(pSdb, pObj);
  }

  // vgroup info
  pIter = NULL;
  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    pClusterInfo->vgroups_total++;
    pClusterInfo->tbs_total += pVgroup->numOfTables;

    SMonVgroupDesc desc = {0};
    desc.vgroup_id = pVgroup->vgId;

    SName name = {0};
    tNameFromString(&name, pVgroup->dbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    tNameGetDbName(&name, desc.database_name);

    desc.tables_num = pVgroup->numOfTables;
    pGrantInfo->timeseries_used += pVgroup->numOfTimeSeries;
    tstrncpy(desc.status, "unsynced", sizeof(desc.status));
    for (int32_t i = 0; i < pVgroup->replica; ++i) {
      SVnodeGid     *pVgid = &pVgroup->vnodeGid[i];
      SMonVnodeDesc *pVnDesc = &desc.vnodes[i];
      pVnDesc->dnode_id = pVgid->dnodeId;
      tstrncpy(pVnDesc->vnode_role, syncStr(pVgid->syncState), sizeof(pVnDesc->vnode_role));
      pVnDesc->syncState = pVgid->syncState;
      if (pVgid->syncState == TAOS_SYNC_STATE_LEADER) {
        tstrncpy(desc.status, "ready", sizeof(desc.status));
        pClusterInfo->vgroups_alive++;
      }
      if (pVgid->syncState != TAOS_SYNC_STATE_ERROR && pVgid->syncState != TAOS_SYNC_STATE_OFFLINE) {
        pClusterInfo->vnodes_alive++;
      }
      pClusterInfo->vnodes_total++;
    }

    taosArrayPush(pVgroupInfo->vgroups, &desc);
    sdbRelease(pSdb, pVgroup);
  }

  // stb info
  pIter = NULL;
  while (1) {
    SStbObj *pStb = NULL;
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
    if (pIter == NULL) break;

    SMonStbDesc desc = {0};

    SName name1 = {0};
    tNameFromString(&name1, pStb->db, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    tNameGetDbName(&name1, desc.database_name);

    SName name2 = {0};
    tNameFromString(&name2, pStb->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    tstrncpy(desc.stb_name, tNameGetTableName(&name2), TSDB_TABLE_NAME_LEN);

    taosArrayPush(pStbInfo->stbs, &desc);
    sdbRelease(pSdb, pStb);
  }

  // grant info
  pGrantInfo->expire_time = (pMnode->grant.expireTimeMS - ms) / 1000;
  pGrantInfo->timeseries_total = pMnode->grant.timeseriesAllowed;
  if (pMnode->grant.expireTimeMS == 0) {
    pGrantInfo->expire_time = 0;
    pGrantInfo->timeseries_total = 0;
  }

  mndReleaseRpc(pMnode);
  return 0;
}

int32_t mndGetLoad(SMnode *pMnode, SMnodeLoad *pLoad) {
  SSyncState state = syncGetState(pMnode->syncMgmt.sync);
  pLoad->syncState = state.state;
  pLoad->syncRestore = state.restored;
  pLoad->syncTerm = state.term;
  pLoad->roleTimeMs = state.roleTimeMs;
  mTrace("mnode current syncState is %s, syncRestore:%d, syncTerm:%" PRId64 " ,roleTimeMs:%" PRId64,
         syncStr(pLoad->syncState), pLoad->syncRestore, pLoad->syncTerm, pLoad->roleTimeMs);
  return 0;
}

void mndSetRestored(SMnode *pMnode, bool restored) {
  if (restored) {
    taosThreadRwlockWrlock(&pMnode->lock);
    pMnode->restored = true;
    taosThreadRwlockUnlock(&pMnode->lock);
    mInfo("mnode set restored:%d", restored);
  } else {
    taosThreadRwlockWrlock(&pMnode->lock);
    pMnode->restored = false;
    taosThreadRwlockUnlock(&pMnode->lock);
    mInfo("mnode set restored:%d", restored);
    while (1) {
      if (pMnode->rpcRef <= 0) break;
      taosMsleep(3);
    }
  }
}

bool mndGetRestored(SMnode *pMnode) { return pMnode->restored; }

void mndSetStop(SMnode *pMnode) {
  taosThreadRwlockWrlock(&pMnode->lock);
  pMnode->stopped = true;
  taosThreadRwlockUnlock(&pMnode->lock);
  mInfo("mnode set stopped");
}

bool mndGetStop(SMnode *pMnode) { return pMnode->stopped; }
