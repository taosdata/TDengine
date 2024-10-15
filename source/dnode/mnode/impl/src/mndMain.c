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
#include "mndArbGroup.h"
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
  (void)taosThreadRwlockRdlock(&pMnode->lock);
  if (pMnode->stopped) {
    code = TSDB_CODE_APP_IS_STOPPING;
  } else if (!mndIsLeader(pMnode)) {
    code = -1;
  } else {
#if 1
    (void)atomic_add_fetch_32(&pMnode->rpcRef, 1);
#else
    int32_t ref = atomic_add_fetch_32(&pMnode->rpcRef, 1);
    mTrace("mnode rpc is acquired, ref:%d", ref);
#endif
  }
  (void)taosThreadRwlockUnlock(&pMnode->lock);
  TAOS_RETURN(code);
}

static inline void mndReleaseRpc(SMnode *pMnode) {
  (void)taosThreadRwlockRdlock(&pMnode->lock);
#if 1
  (void)atomic_sub_fetch_32(&pMnode->rpcRef, 1);
#else
  int32_t ref = atomic_sub_fetch_32(&pMnode->rpcRef, 1);
  mTrace("mnode rpc is released, ref:%d", ref);
#endif
  (void)taosThreadRwlockUnlock(&pMnode->lock);
}

static void *mndBuildTimerMsg(int32_t *pContLen) {
  terrno = 0;
  SMTimerReq timerReq = {0};

  int32_t contLen = tSerializeSMTimerMsg(NULL, 0, &timerReq);
  if (contLen <= 0) return NULL;
  void *pReq = rpcMallocCont(contLen);
  if (pReq == NULL) return NULL;

  if (tSerializeSMTimerMsg(pReq, contLen, &timerReq) < 0) {
    mError("failed to serialize timer msg since %s", terrstr());
  }
  *pContLen = contLen;
  return pReq;
}

static void mndPullupTrans(SMnode *pMnode) {
  mTrace("pullup trans msg");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_TRANS_TIMER, .pCont = pReq, .contLen = contLen};
    // TODO check return value
    if (tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg) < 0) {
      mError("failed to put into write-queue since %s, line:%d", terrstr(), __LINE__);
    }
  }
}

static void mndPullupCompacts(SMnode *pMnode) {
  mTrace("pullup compact timer msg");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_COMPACT_TIMER, .pCont = pReq, .contLen = contLen};
    // TODO check return value
    if (tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg) < 0) {
      mError("failed to put into write-queue since %s, line:%d", terrstr(), __LINE__);
    }
  }
}

static void mndPullupTtl(SMnode *pMnode) {
  mTrace("pullup ttl");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  SRpcMsg rpcMsg = {.msgType = TDMT_MND_TTL_TIMER, .pCont = pReq, .contLen = contLen};
  // TODO check return value
  if (tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg) < 0) {
    mError("failed to put into write-queue since %s, line:%d", terrstr(), __LINE__);
  }
}

static void mndPullupTrimDb(SMnode *pMnode) {
  mTrace("pullup s3migrate");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  SRpcMsg rpcMsg = {.msgType = TDMT_MND_TRIM_DB_TIMER, .pCont = pReq, .contLen = contLen};
  // TODO check return value
  if (tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg) < 0) {
    mError("failed to put into write-queue since %s, line:%d", terrstr(), __LINE__);
  }
}

static void mndPullupS3MigrateDb(SMnode *pMnode) {
  mTrace("pullup trim");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  // TODO check return value
  SRpcMsg rpcMsg = {.msgType = TDMT_MND_S3MIGRATE_DB_TIMER, .pCont = pReq, .contLen = contLen};
  if (tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg) < 0) {
    mError("failed to put into write-queue since %s, line:%d", terrstr(), __LINE__);
  }
}

static int32_t mndPullupArbHeartbeat(SMnode *pMnode) {
  mTrace("pullup arb hb");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  SRpcMsg rpcMsg = {.msgType = TDMT_MND_ARB_HEARTBEAT_TIMER, .pCont = pReq, .contLen = contLen, .info.noResp = 1};
  return tmsgPutToQueue(&pMnode->msgCb, ARB_QUEUE, &rpcMsg);
}

static int32_t mndPullupArbCheckSync(SMnode *pMnode) {
  mTrace("pullup arb sync");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  SRpcMsg rpcMsg = {.msgType = TDMT_MND_ARB_CHECK_SYNC_TIMER, .pCont = pReq, .contLen = contLen, .info.noResp = 1};
  return tmsgPutToQueue(&pMnode->msgCb, ARB_QUEUE, &rpcMsg);
}

static void mndCalMqRebalance(SMnode *pMnode) {
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_TMQ_TIMER, .pCont = pReq, .contLen = contLen};
    if (tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg) < 0) {
      mError("failed to put into write-queue since %s, line:%d", terrstr(), __LINE__);
    }
  }
}

static void mndStreamCheckpointTimer(SMnode *pMnode) {
  SMStreamDoCheckpointMsg *pMsg = rpcMallocCont(sizeof(SMStreamDoCheckpointMsg));
  if (pMsg != NULL) {
    int32_t size = sizeof(SMStreamDoCheckpointMsg);
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_STREAM_BEGIN_CHECKPOINT, .pCont = pMsg, .contLen = size};
    // TODO check return value
    if (tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg) < 0) {
      mError("failed to put into write-queue since %s, line:%d", terrstr(), __LINE__);
    }
  }
}

static void mndStreamCheckNode(SMnode *pMnode) {
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_NODECHECK_TIMER, .pCont = pReq, .contLen = contLen};
    // TODO check return value
    if (tmsgPutToQueue(&pMnode->msgCb, READ_QUEUE, &rpcMsg) < 0) {
      mError("failed to put into read-queue since %s, line:%d", terrstr(), __LINE__);
    }
  }
}

static void mndStreamConsensusChkpt(SMnode *pMnode) {
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_STREAM_CONSEN_TIMER, .pCont = pReq, .contLen = contLen};
    // TODO check return value
    if (tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg) < 0) {
      mError("failed to put into write-queue since %s, line:%d", terrstr(), __LINE__);
    }
  }
}

static void mndPullupTelem(SMnode *pMnode) {
  mTrace("pullup telem msg");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_TELEM_TIMER, .pCont = pReq, .contLen = contLen};
    // TODO check return value
    if (tmsgPutToQueue(&pMnode->msgCb, READ_QUEUE, &rpcMsg) < 0) {
      mError("failed to put into read-queue since %s, line:%d", terrstr(), __LINE__);
    }
  }
}

static void mndPullupGrant(SMnode *pMnode) {
  mTrace("pullup grant msg");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_GRANT_HB_TIMER,
                      .pCont = pReq,
                      .contLen = contLen,
                      .info.notFreeAhandle = 1,
                      .info.ahandle = 0};
    // TODO check return value
    if (tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg) < 0) {
      mError("failed to put into write-queue since %s, line:%d", terrstr(), __LINE__);
    }
  }
}

static void mndIncreaseUpTime(SMnode *pMnode) {
  mTrace("increate uptime");
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_UPTIME_TIMER,
                      .pCont = pReq,
                      .contLen = contLen,
                      .info.notFreeAhandle = 1,
                      .info.ahandle = (void *)0x9527};
    // TODO check return value
    if (tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg) < 0) {
      mError("failed to put into write-queue since %s, line:%d", terrstr(), __LINE__);
    }
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
  (void)taosThreadRwlockRdlock(&pMnode->lock);
  SSyncState state = syncGetState(pMnode->syncMgmt.sync);
  if (terrno != 0) {
    (void)taosThreadRwlockUnlock(&pMnode->lock);
    return true;
  }

  if (state.state != TAOS_SYNC_STATE_LEADER) {
    (void)taosThreadRwlockUnlock(&pMnode->lock);
    terrno = TSDB_CODE_SYN_NOT_LEADER;
    return true;
  }
  if (!state.restored || !pMnode->restored) {
    (void)taosThreadRwlockUnlock(&pMnode->lock);
    terrno = TSDB_CODE_SYN_RESTORING;
    return true;
  }
  (void)taosThreadRwlockUnlock(&pMnode->lock);
  return false;
}

static int32_t minCronTime() {
  int32_t min = INT32_MAX;
  min = TMIN(min, tsTtlPushIntervalSec);
  min = TMIN(min, tsTrimVDbIntervalSec);
  min = TMIN(min, tsS3MigrateIntervalSec);
  min = TMIN(min, tsTransPullupInterval);
  min = TMIN(min, tsCompactPullupInterval);
  min = TMIN(min, tsMqRebalanceInterval);
  min = TMIN(min, tsStreamCheckpointInterval);
  min = TMIN(min, tsStreamNodeCheckInterval);
  min = TMIN(min, tsArbHeartBeatIntervalSec);
  min = TMIN(min, tsArbCheckSyncIntervalSec);

  int64_t telemInt = TMIN(60, (tsTelemInterval - 1));
  min = TMIN(min, telemInt);
  min = TMIN(min, tsGrantHBInterval);
  min = TMIN(min, tsUptimeInterval);

  return min <= 1 ? 2 : min;
}
void mndDoTimerPullupTask(SMnode *pMnode, int64_t sec) {
  int32_t code = 0;
  if (sec % tsTtlPushIntervalSec == 0) {
    mndPullupTtl(pMnode);
  }

  if (sec % tsTrimVDbIntervalSec == 0) {
    mndPullupTrimDb(pMnode);
  }

  if (tsS3MigrateEnabled && sec % tsS3MigrateIntervalSec == 0) {
    mndPullupS3MigrateDb(pMnode);
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

  if (sec % 30 == 0) {  // send the checkpoint info every 30 sec
    mndStreamCheckpointTimer(pMnode);
  }

  if (sec % tsStreamNodeCheckInterval == 0) {
    mndStreamCheckNode(pMnode);
  }

  if (sec % 5 == 0) {
    mndStreamConsensusChkpt(pMnode);
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

  if (sec % (tsArbHeartBeatIntervalSec) == 0) {
    if ((code = mndPullupArbHeartbeat(pMnode)) != 0) {
      mError("failed to pullup arb heartbeat, since:%s", tstrerror(code));
    }
  }

  if (sec % (tsArbCheckSyncIntervalSec) == 0) {
    if ((code = mndPullupArbCheckSync(pMnode)) != 0) {
      mError("failed to pullup arb check sync, since:%s", tstrerror(code));
    }
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
  int32_t      code = 0;
  TdThreadAttr thAttr;
  (void)taosThreadAttrInit(&thAttr);
  (void)taosThreadAttrSetDetachState(&thAttr, PTHREAD_CREATE_JOINABLE);
  if ((code = taosThreadCreate(&pMnode->thread, &thAttr, mndThreadFp, pMnode)) != 0) {
    mError("failed to create timer thread since %s", tstrerror(code));
    TAOS_RETURN(code);
  }

  (void)taosThreadAttrDestroy(&thAttr);
  tmsgReportStartup("mnode-timer", "initialized");
  TAOS_RETURN(code);
}

static void mndCleanupTimer(SMnode *pMnode) {
  if (taosCheckPthreadValid(pMnode->thread)) {
    (void)taosThreadJoin(pMnode->thread, NULL);
    taosThreadClear(&pMnode->thread);
  }
}

static int32_t mndCreateDir(SMnode *pMnode, const char *path) {
  int32_t code = 0;
  pMnode->path = taosStrdup(path);
  if (pMnode->path == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }

  if (taosMkDir(pMnode->path) != 0) {
    code = terrno;
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static int32_t mndInitWal(SMnode *pMnode) {
  int32_t code = 0;
  char    path[PATH_MAX + 20] = {0};
  (void)snprintf(path, sizeof(path), "%s%swal", pMnode->path, TD_DIRSEP);
  SWalCfg cfg = {.vgId = 1,
                 .fsyncPeriod = 0,
                 .rollPeriod = -1,
                 .segSize = -1,
                 .retentionPeriod = 0,
                 .retentionSize = 0,
                 .level = TAOS_WAL_FSYNC,
                 .encryptAlgorithm = 0,
                 .encryptKey = {0}};

#if defined(TD_ENTERPRISE)
  if (tsiEncryptAlgorithm == DND_CA_SM4 && (tsiEncryptScope & DND_CS_MNODE_WAL) == DND_CS_MNODE_WAL) {
    cfg.encryptAlgorithm = (tsiEncryptScope & DND_CS_MNODE_WAL) ? tsiEncryptAlgorithm : 0;
    if (tsEncryptKey[0] == '\0') {
      code = TSDB_CODE_DNODE_INVALID_ENCRYPTKEY;
      TAOS_RETURN(code);
    } else {
      (void)strncpy(cfg.encryptKey, tsEncryptKey, ENCRYPT_KEY_LEN);
    }
  }
#endif

  pMnode->pWal = walOpen(path, &cfg);
  if (pMnode->pWal == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    mError("failed to open wal since %s. wal:%s", tstrerror(code), path);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static void mndCloseWal(SMnode *pMnode) {
  if (pMnode->pWal != NULL) {
    walClose(pMnode->pWal);
    pMnode->pWal = NULL;
  }
}

static int32_t mndInitSdb(SMnode *pMnode) {
  int32_t code = 0;
  SSdbOpt opt = {0};
  opt.path = pMnode->path;
  opt.pMnode = pMnode;
  opt.pWal = pMnode->pWal;

  pMnode->pSdb = sdbInit(&opt);
  if (pMnode->pSdb == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  TAOS_RETURN(code);
}

static int32_t mndOpenSdb(SMnode *pMnode) {
  int32_t code = 0;
  if (!pMnode->deploy) {
    code = sdbReadFile(pMnode->pSdb);
  }

  mInfo("vgId:1, mnode sdb is opened, with applied index:%" PRId64, pMnode->pSdb->commitIndex);

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
    TAOS_RETURN(terrno);
  }

  TAOS_RETURN(0);
}

static int32_t mndInitSteps(SMnode *pMnode) {
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-wal", mndInitWal, mndCloseWal));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-sdb", mndInitSdb, mndCleanupSdb));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-trans", mndInitTrans, mndCleanupTrans));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-cluster", mndInitCluster, mndCleanupCluster));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-mnode", mndInitMnode, mndCleanupMnode));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-qnode", mndInitQnode, mndCleanupQnode));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-snode", mndInitSnode, mndCleanupSnode));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-arbgroup", mndInitArbGroup, mndCleanupArbGroup));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-dnode", mndInitDnode, mndCleanupDnode));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-user", mndInitUser, mndCleanupUser));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-grant", mndInitGrant, mndCleanupGrant));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-privilege", mndInitPrivilege, mndCleanupPrivilege));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-acct", mndInitAcct, mndCleanupAcct));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-stream", mndInitStream, mndCleanupStream));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-topic", mndInitTopic, mndCleanupTopic));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-consumer", mndInitConsumer, mndCleanupConsumer));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-subscribe", mndInitSubscribe, mndCleanupSubscribe));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-vgroup", mndInitVgroup, mndCleanupVgroup));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-stb", mndInitStb, mndCleanupStb));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-sma", mndInitSma, mndCleanupSma));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-idx", mndInitIdx, mndCleanupIdx));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-infos", mndInitInfos, mndCleanupInfos));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-perfs", mndInitPerfs, mndCleanupPerfs));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-db", mndInitDb, mndCleanupDb));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-func", mndInitFunc, mndCleanupFunc));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-view", mndInitView, mndCleanupView));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-compact", mndInitCompact, mndCleanupCompact));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-compact-detail", mndInitCompactDetail, mndCleanupCompactDetail));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-sdb", mndOpenSdb, NULL));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-profile", mndInitProfile, mndCleanupProfile));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-show", mndInitShow, mndCleanupShow));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-query", mndInitQuery, mndCleanupQuery));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-sync", mndInitSync, mndCleanupSync));
  TAOS_CHECK_RETURN(mndAllocStep(pMnode, "mnode-telem", mndInitTelem, mndCleanupTelem));

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
  int32_t code = 0;
  int32_t size = taosArrayGetSize(pMnode->pSteps);
  for (int32_t pos = 0; pos < size; pos++) {
    SMnodeStep *pStep = taosArrayGet(pMnode->pSteps, pos);
    if (pStep->initFp == NULL) continue;

    if ((code = (*pStep->initFp)(pMnode)) != 0) {
      mError("%s exec failed since %s, start to cleanup", pStep->name, tstrerror(code));
      mndCleanupSteps(pMnode, pos);
      TAOS_RETURN(code);
    } else {
      mInfo("%s is initialized", pStep->name);
      tmsgReportStartup(pStep->name, "initialized");
    }
  }

  pMnode->clusterId = mndGetClusterId(pMnode);
  TAOS_RETURN(0);
}

static void mndSetOptions(SMnode *pMnode, const SMnodeOpt *pOption) {
  pMnode->msgCb = pOption->msgCb;
  pMnode->selfDnodeId = pOption->dnodeId;
  pMnode->syncMgmt.selfIndex = pOption->selfIndex;
  pMnode->syncMgmt.numOfReplicas = pOption->numOfReplicas;
  pMnode->syncMgmt.numOfTotalReplicas = pOption->numOfTotalReplicas;
  pMnode->syncMgmt.lastIndex = pOption->lastIndex;
  (void)memcpy(pMnode->syncMgmt.replicas, pOption->replicas, sizeof(pOption->replicas));
  (void)memcpy(pMnode->syncMgmt.nodeRoles, pOption->nodeRoles, sizeof(pOption->nodeRoles));
}

SMnode *mndOpen(const char *path, const SMnodeOpt *pOption) {
  terrno = 0;
  mInfo("start to open mnode in %s", path);

  SMnode *pMnode = taosMemoryCalloc(1, sizeof(SMnode));
  if (pMnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to open mnode since %s", terrstr());
    return NULL;
  }
  (void)memset(pMnode, 0, sizeof(SMnode));

  int32_t code = taosThreadRwlockInit(&pMnode->lock, NULL);
  if (code != 0) {
    taosMemoryFree(pMnode);
    mError("failed to open mnode lock since %s", tstrerror(code));
    return NULL;
  }

  char timestr[24] = "1970-01-01 00:00:00.00";
  code = taosParseTime(timestr, &pMnode->checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  if (code < 0) {
    mError("failed to parse time since %s", tstrerror(code));
    (void)taosThreadRwlockDestroy(&pMnode->lock);
    taosMemoryFree(pMnode);
    return NULL;
  }
  mndSetOptions(pMnode, pOption);

  pMnode->deploy = pOption->deploy;
  pMnode->pSteps = taosArrayInit(24, sizeof(SMnodeStep));
  if (pMnode->pSteps == NULL) {
    taosMemoryFree(pMnode);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to open mnode since %s", terrstr());
    return NULL;
  }

  code = mndCreateDir(pMnode, path);
  if (code != 0) {
    code = terrno;
    mError("failed to open mnode since %s", tstrerror(code));
    mndClose(pMnode);
    terrno = code;
    return NULL;
  }

  code = mndInitSteps(pMnode);
  if (code != 0) {
    code = terrno;
    mError("failed to open mnode since %s", tstrerror(code));
    mndClose(pMnode);
    terrno = code;
    return NULL;
  }

  code = mndExecSteps(pMnode);
  if (code != 0) {
    code = terrno;
    mError("failed to open mnode since %s", tstrerror(code));
    mndClose(pMnode);
    terrno = code;
    return NULL;
  }

  mInfo("mnode open successfully");
  return pMnode;
}

void mndPreClose(SMnode *pMnode) {
  if (pMnode != NULL) {
    int32_t code = 0;
    // TODO check return value
    code = syncLeaderTransfer(pMnode->syncMgmt.sync);
    if (code < 0) {
      mError("failed to transfer leader since %s", tstrerror(code));
    }
    syncPreStop(pMnode->syncMgmt.sync);
    code = sdbWriteFile(pMnode->pSdb, 0);
    if (code < 0) {
      mError("failed to write sdb since %s", tstrerror(code));
    }
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

int64_t mndGetTerm(SMnode *pMnode) {
  int64_t rid = pMnode->syncMgmt.sync;
  return syncGetTerm(rid);
}

int32_t mndGetArbToken(SMnode *pMnode, char *outToken) { return syncGetArbToken(pMnode->syncMgmt.sync, outToken); }

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
    mGError("vgId:1, failed to process sync msg:%p type:%s, reason: %s, code:0x%x", pMsg, TMSG_INFO(pMsg->msgType),
            tstrerror(code), code);
  }

  return code;
}

static int32_t mndCheckMnodeState(SRpcMsg *pMsg) {
  int32_t code = 0;
  if (!IsReq(pMsg)) TAOS_RETURN(code);
  if (pMsg->msgType == TDMT_SCH_QUERY || pMsg->msgType == TDMT_SCH_MERGE_QUERY ||
      pMsg->msgType == TDMT_SCH_QUERY_CONTINUE || pMsg->msgType == TDMT_SCH_QUERY_HEARTBEAT ||
      pMsg->msgType == TDMT_SCH_FETCH || pMsg->msgType == TDMT_SCH_MERGE_FETCH || pMsg->msgType == TDMT_SCH_DROP_TASK ||
      pMsg->msgType == TDMT_SCH_TASK_NOTIFY) {
    TAOS_RETURN(code);
  }

  SMnode *pMnode = pMsg->info.node;
  (void)taosThreadRwlockRdlock(&pMnode->lock);
  if (pMnode->stopped) {
    (void)taosThreadRwlockUnlock(&pMnode->lock);
    code = TSDB_CODE_APP_IS_STOPPING;
    TAOS_RETURN(code);
  }

  terrno = 0;
  SSyncState state = syncGetState(pMnode->syncMgmt.sync);
  if (terrno != 0) {
    (void)taosThreadRwlockUnlock(&pMnode->lock);
    code = terrno;
    TAOS_RETURN(code);
  }

  if (state.state != TAOS_SYNC_STATE_LEADER) {
    (void)taosThreadRwlockUnlock(&pMnode->lock);
    code = TSDB_CODE_SYN_NOT_LEADER;
    goto _OVER;
  }

  if (!state.restored || !pMnode->restored) {
    (void)taosThreadRwlockUnlock(&pMnode->lock);
    code = TSDB_CODE_SYN_RESTORING;
    goto _OVER;
  }

#if 1
  (void)atomic_add_fetch_32(&pMnode->rpcRef, 1);
#else
  int32_t ref = atomic_add_fetch_32(&pMnode->rpcRef, 1);
  mTrace("mnode rpc is acquired, ref:%d", ref);
#endif

  (void)taosThreadRwlockUnlock(&pMnode->lock);
  TAOS_RETURN(code);

_OVER:
  if (pMsg->msgType == TDMT_MND_TMQ_TIMER || pMsg->msgType == TDMT_MND_TELEM_TIMER ||
      pMsg->msgType == TDMT_MND_TRANS_TIMER || pMsg->msgType == TDMT_MND_TTL_TIMER ||
      pMsg->msgType == TDMT_MND_TRIM_DB_TIMER || pMsg->msgType == TDMT_MND_UPTIME_TIMER ||
      pMsg->msgType == TDMT_MND_COMPACT_TIMER || pMsg->msgType == TDMT_MND_NODECHECK_TIMER ||
      pMsg->msgType == TDMT_MND_GRANT_HB_TIMER || pMsg->msgType == TDMT_MND_STREAM_REQ_CHKPT ||
      pMsg->msgType == TDMT_MND_S3MIGRATE_DB_TIMER || pMsg->msgType == TDMT_MND_ARB_HEARTBEAT_TIMER ||
      pMsg->msgType == TDMT_MND_ARB_CHECK_SYNC_TIMER) {
    mTrace("timer not process since mnode restored:%d stopped:%d, sync restored:%d role:%s ", pMnode->restored,
           pMnode->stopped, state.restored, syncStr(state.state));
    TAOS_RETURN(code);
  }

  const STraceId *trace = &pMsg->info.traceId;
  SEpSet          epSet = {0};
  mndGetMnodeEpSet(pMnode, &epSet);

  mGDebug(
      "msg:%p, type:%s failed to process since %s, mnode restored:%d stopped:%d, sync restored:%d "
      "role:%s, redirect numOfEps:%d inUse:%d, type:%s",
      pMsg, TMSG_INFO(pMsg->msgType), tstrerror(code), pMnode->restored, pMnode->stopped, state.restored,
      syncStr(state.state), epSet.numOfEps, epSet.inUse, TMSG_INFO(pMsg->msgType));

  if (epSet.numOfEps <= 0) return -1;

  for (int32_t i = 0; i < epSet.numOfEps; ++i) {
    mDebug("mnode index:%d, ep:%s:%u", i, epSet.eps[i].fqdn, epSet.eps[i].port);
  }

  int32_t contLen = tSerializeSEpSet(NULL, 0, &epSet);
  pMsg->info.rsp = rpcMallocCont(contLen);
  if (pMsg->info.rsp != NULL) {
    if (tSerializeSEpSet(pMsg->info.rsp, contLen, &epSet) < 0) {
      mError("failed to serialize ep set");
    }
    pMsg->info.hasEpSet = 1;
    pMsg->info.rspLen = contLen;
  }

  TAOS_RETURN(code);
}

int32_t mndProcessRpcMsg(SRpcMsg *pMsg, SQueueInfo *pQueueInfo) {
  SMnode         *pMnode = pMsg->info.node;
  const STraceId *trace = &pMsg->info.traceId;
  int32_t         code = TSDB_CODE_SUCCESS;

  MndMsgFp    fp = pMnode->msgFp[TMSG_INDEX(pMsg->msgType)];
  MndMsgFpExt fpExt = NULL;
  if (fp == NULL) {
    fpExt = pMnode->msgFpExt[TMSG_INDEX(pMsg->msgType)];
    if (fpExt == NULL) {
      mGError("msg:%p, failed to get msg handle, app:%p type:%s", pMsg, pMsg->info.ahandle, TMSG_INFO(pMsg->msgType));
      code = TSDB_CODE_MSG_NOT_PROCESSED;
      TAOS_RETURN(code);
    }
  }

  TAOS_CHECK_RETURN(mndCheckMnodeState(pMsg));

  mGTrace("msg:%p, start to process in mnode, app:%p type:%s", pMsg, pMsg->info.ahandle, TMSG_INFO(pMsg->msgType));
  if (fp)
    code = (*fp)(pMsg);
  else
    code = (*fpExt)(pMsg, pQueueInfo);
  mndReleaseRpc(pMnode);

  if (code == TSDB_CODE_ACTION_IN_PROGRESS) {
    mGTrace("msg:%p, won't response immediately since in progress", pMsg);
  } else if (code == 0) {
    mGTrace("msg:%p, successfully processed", pMsg);
  } else {
    // TODO removve this wrong set code
    if (code == -1) {
      code = terrno;
    }
    mGError("msg:%p, failed to process since %s, app:%p type:%s", pMsg, tstrerror(code), pMsg->info.ahandle,
            TMSG_INFO(pMsg->msgType));
  }

  TAOS_RETURN(code);
}

void mndSetMsgHandle(SMnode *pMnode, tmsg_t msgType, MndMsgFp fp) {
  tmsg_t type = TMSG_INDEX(msgType);
  if (type < TDMT_MAX) {
    pMnode->msgFp[type] = fp;
  }
}

void mndSetMsgHandleExt(SMnode *pMnode, tmsg_t msgType, MndMsgFpExt fp) {
  tmsg_t type = TMSG_INDEX(msgType);
  if (type < TDMT_MAX) {
    pMnode->msgFpExt[type] = fp;
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
  int32_t code = 0;
  TAOS_CHECK_RETURN(mndAcquireRpc(pMnode));

  SSdb   *pSdb = pMnode->pSdb;
  int64_t ms = taosGetTimestampMs();

  pClusterInfo->dnodes = taosArrayInit(sdbGetSize(pSdb, SDB_DNODE), sizeof(SMonDnodeDesc));
  pClusterInfo->mnodes = taosArrayInit(sdbGetSize(pSdb, SDB_MNODE), sizeof(SMonMnodeDesc));
  pVgroupInfo->vgroups = taosArrayInit(sdbGetSize(pSdb, SDB_VGROUP), sizeof(SMonVgroupDesc));
  pStbInfo->stbs = taosArrayInit(sdbGetSize(pSdb, SDB_STB), sizeof(SMonStbDesc));
  if (pClusterInfo->dnodes == NULL || pClusterInfo->mnodes == NULL || pVgroupInfo->vgroups == NULL ||
      pStbInfo->stbs == NULL) {
    mndReleaseRpc(pMnode);
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
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
    if (taosArrayPush(pClusterInfo->dnodes, &desc) == NULL) {
      mError("failed put dnode into array, but continue at this monitor report")
    }
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
      // pClusterInfo->master_uptime = (float)mndGetClusterUpTime(pMnode) / 86400.0f;
      pClusterInfo->master_uptime = mndGetClusterUpTime(pMnode);
      // pClusterInfo->master_uptime = (ms - pObj->stateStartTime) / (86400000.0f);
      tstrncpy(desc.role, syncStr(TAOS_SYNC_STATE_LEADER), sizeof(desc.role));
      desc.syncState = TAOS_SYNC_STATE_LEADER;
    } else {
      tstrncpy(desc.role, syncStr(pObj->syncState), sizeof(desc.role));
      desc.syncState = pObj->syncState;
    }
    if (taosArrayPush(pClusterInfo->mnodes, &desc) == NULL) {
      mError("failed to put mnode into array, but continue at this monitor report");
    }
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
    code = tNameFromString(&name, pVgroup->dbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    if (code < 0) {
      mError("failed to get db name since %s", tstrerror(code));
      sdbRelease(pSdb, pVgroup);
      TAOS_RETURN(code);
    }
    (void)tNameGetDbName(&name, desc.database_name);

    desc.tables_num = pVgroup->numOfTables;
    pGrantInfo->timeseries_used += pVgroup->numOfTimeSeries;
    tstrncpy(desc.status, "unsynced", sizeof(desc.status));
    for (int32_t i = 0; i < pVgroup->replica; ++i) {
      SVnodeGid     *pVgid = &pVgroup->vnodeGid[i];
      SMonVnodeDesc *pVnDesc = &desc.vnodes[i];
      pVnDesc->dnode_id = pVgid->dnodeId;
      tstrncpy(pVnDesc->vnode_role, syncStr(pVgid->syncState), sizeof(pVnDesc->vnode_role));
      pVnDesc->syncState = pVgid->syncState;
      if (pVgid->syncState == TAOS_SYNC_STATE_LEADER || pVgid->syncState == TAOS_SYNC_STATE_ASSIGNED_LEADER) {
        tstrncpy(desc.status, "ready", sizeof(desc.status));
        pClusterInfo->vgroups_alive++;
      }
      if (pVgid->syncState != TAOS_SYNC_STATE_ERROR && pVgid->syncState != TAOS_SYNC_STATE_OFFLINE) {
        pClusterInfo->vnodes_alive++;
      }
      pClusterInfo->vnodes_total++;
    }

    if (taosArrayPush(pVgroupInfo->vgroups, &desc) == NULL) {
      mError("failed to put vgroup into array, but continue at this monitor report")
    }
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
    code = tNameFromString(&name1, pStb->db, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    if (code < 0) {
      mError("failed to get db name since %s", tstrerror(code));
      sdbRelease(pSdb, pStb);
      TAOS_RETURN(code);
    }
    (void)tNameGetDbName(&name1, desc.database_name);

    SName name2 = {0};
    code = tNameFromString(&name2, pStb->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    if (code < 0) {
      mError("failed to get table name since %s", tstrerror(code));
      sdbRelease(pSdb, pStb);
      TAOS_RETURN(code);
    }
    tstrncpy(desc.stb_name, tNameGetTableName(&name2), TSDB_TABLE_NAME_LEN);

    if (taosArrayPush(pStbInfo->stbs, &desc) == NULL) {
      mError("failed to put stb into array, but continue at this monitor report");
    }
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
  TAOS_RETURN(code);
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

int64_t mndGetRoleTimeMs(SMnode *pMnode) {
  SSyncState state = syncGetState(pMnode->syncMgmt.sync);
  return state.roleTimeMs;
}

void mndSetRestored(SMnode *pMnode, bool restored) {
  if (restored) {
    (void)taosThreadRwlockWrlock(&pMnode->lock);
    pMnode->restored = true;
    (void)taosThreadRwlockUnlock(&pMnode->lock);
    mInfo("mnode set restored:%d", restored);
  } else {
    (void)taosThreadRwlockWrlock(&pMnode->lock);
    pMnode->restored = false;
    (void)taosThreadRwlockUnlock(&pMnode->lock);
    mInfo("mnode set restored:%d", restored);
    while (1) {
      if (pMnode->rpcRef <= 0) break;
      taosMsleep(3);
    }
  }
}

bool mndGetRestored(SMnode *pMnode) { return pMnode->restored; }

void mndSetStop(SMnode *pMnode) {
  (void)taosThreadRwlockWrlock(&pMnode->lock);
  pMnode->stopped = true;
  (void)taosThreadRwlockUnlock(&pMnode->lock);
  mInfo("mnode set stopped");
}

bool mndGetStop(SMnode *pMnode) { return pMnode->stopped; }
