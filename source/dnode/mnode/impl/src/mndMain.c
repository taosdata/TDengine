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
#include "mndBnode.h"
#include "mndCluster.h"
#include "mndConsumer.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndFunc.h"
#include "mndGrant.h"
#include "mndInfoSchema.h"
#include "mndMnode.h"
#include "mndOffset.h"
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

static void mndPullupTrans(SMnode *pMnode) {
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_TRANS_TIMER, .pCont = pReq, .contLen = contLen};
    tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
  }
}

static void mndTtlTimer(SMnode *pMnode) {
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  SRpcMsg rpcMsg = {.msgType = TDMT_MND_TTL_TIMER, .pCont = pReq, .contLen = contLen};
  tmsgPutToQueue(&pMnode->msgCb, WRITE_QUEUE, &rpcMsg);
}

static void mndCalMqRebalance(SMnode *pMnode) {
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_MQ_TIMER, .pCont = pReq, .contLen = contLen};
    tmsgPutToQueue(&pMnode->msgCb, READ_QUEUE, &rpcMsg);
  }
}

static void mndPullupTelem(SMnode *pMnode) {
  int32_t contLen = 0;
  void   *pReq = mndBuildTimerMsg(&contLen);
  if (pReq != NULL) {
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_TELEM_TIMER, .pCont = pReq, .contLen = contLen};
    tmsgPutToQueue(&pMnode->msgCb, READ_QUEUE, &rpcMsg);
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

    if (lastTime % (tsTtlPushInterval * 10) == 1) {
      mndTtlTimer(pMnode);
    }

    if (lastTime % (tsTransPullupInterval * 10) == 0) {
      mndPullupTrans(pMnode);
    }

    if (lastTime % (tsMqRebalanceInterval * 10) == 0) {
      mndCalMqRebalance(pMnode);
    }

    if (lastTime % (tsTelemInterval * 10) == 0) {
      mndPullupTelem(pMnode);
    }
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
  pMnode->path = strdup(path);
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
      .retentionPeriod = -1,
      .retentionSize = -1,
      .level = TAOS_WAL_FSYNC,
  };

  pMnode->pWal = walOpen(path, &cfg);
  if (pMnode->pWal == NULL) {
    mError("failed to open wal since %s", terrstr());
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
  if (!pMnode->deploy) {
    return sdbReadFile(pMnode->pSdb);
  } else {
    return 0;
  }
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
  if (mndAllocStep(pMnode, "mnode-bnode", mndInitBnode, mndCleanupBnode) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-dnode", mndInitDnode, mndCleanupDnode) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-user", mndInitUser, mndCleanupUser) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-grant", mndInitGrant, mndCleanupGrant) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-privilege", mndInitPrivilege, mndCleanupPrivilege) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-acct", mndInitAcct, mndCleanupAcct) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-stream", mndInitStream, mndCleanupStream) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-topic", mndInitTopic, mndCleanupTopic) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-consumer", mndInitConsumer, mndCleanupConsumer) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-subscribe", mndInitSubscribe, mndCleanupSubscribe) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-offset", mndInitOffset, mndCleanupOffset) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-vgroup", mndInitVgroup, mndCleanupVgroup) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-stb", mndInitStb, mndCleanupStb) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-sma", mndInitSma, mndCleanupSma) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-infos", mndInitInfos, mndCleanupInfos) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-perfs", mndInitPerfs, mndCleanupPerfs) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-db", mndInitDb, mndCleanupDb) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-func", mndInitFunc, mndCleanupFunc) != 0) return -1;
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
    mDebug("%s will cleanup", pStep->name);
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
      mDebug("%s is initialized", pStep->name);
      tmsgReportStartup(pStep->name, "initialized");
    }
  }

  pMnode->clusterId = mndGetClusterId(pMnode);
  return 0;
}

static void mndSetOptions(SMnode *pMnode, const SMnodeOpt *pOption) {
  pMnode->msgCb = pOption->msgCb;
  pMnode->selfDnodeId = pOption->dnodeId;
  pMnode->syncMgmt.replica = pOption->replica;
  pMnode->syncMgmt.standby = pOption->standby;
}

SMnode *mndOpen(const char *path, const SMnodeOpt *pOption) {
  mDebug("start to open mnode in %s", path);

  SMnode *pMnode = taosMemoryCalloc(1, sizeof(SMnode));
  if (pMnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to open mnode since %s", terrstr());
    return NULL;
  }

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

  mDebug("mnode open successfully ");
  return pMnode;
}

void mndPreClose(SMnode *pMnode) {
  if (pMnode != NULL) {
    syncLeaderTransfer(pMnode->syncMgmt.sync);
  }
}

void mndClose(SMnode *pMnode) {
  if (pMnode != NULL) {
    mDebug("start to close mnode");
    mndCleanupSteps(pMnode, -1);
    taosMemoryFreeClear(pMnode->path);
    taosMemoryFreeClear(pMnode);
    mDebug("mnode is closed");
  }
}

int32_t mndStart(SMnode *pMnode) {
  mndSyncStart(pMnode);
  if (pMnode->deploy) {
    if (sdbDeploy(pMnode->pSdb) != 0) {
      mError("failed to deploy sdb while start mnode");
      return -1;
    }
    mndSetRestore(pMnode, true);
  }
  return mndInitTimer(pMnode);
}

void mndStop(SMnode *pMnode) {
  mndSetStop(pMnode);
  mndSyncStop(pMnode);
  mndCleanupTimer(pMnode);
}

int32_t mndProcessSyncMsg(SRpcMsg *pMsg) {
  SMnode    *pMnode = pMsg->info.node;
  SSyncMgmt *pMgmt = &pMnode->syncMgmt;
  int32_t    code = 0;

  if (!syncEnvIsStart()) {
    mError("failed to process sync msg:%p type:%s since syncEnv stop", pMsg, TMSG_INFO(pMsg->msgType));
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  SSyncNode *pSyncNode = syncNodeAcquire(pMgmt->sync);
  if (pSyncNode == NULL) {
    mError("failed to process sync msg:%p type:%s since syncNode is null", pMsg, TMSG_INFO(pMsg->msgType));
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
    return -1;
  }

  do {
    char          *syncNodeStr = sync2SimpleStr(pMgmt->sync);
    static int64_t mndTick = 0;
    if (++mndTick % 10 == 1) {
      mTrace("vgId:%d, sync trace msg:%s, %s", syncGetVgId(pMgmt->sync), TMSG_INFO(pMsg->msgType), syncNodeStr);
    }
    if (gRaftDetailLog) {
      char logBuf[512] = {0};
      snprintf(logBuf, sizeof(logBuf), "==mndProcessSyncMsg== msgType:%d, syncNode: %s", pMsg->msgType, syncNodeStr);
      syncRpcMsgLog2(logBuf, pMsg);
    }
    taosMemoryFree(syncNodeStr);
  } while (0);

  // ToDo: ugly! use function pointer
  if (syncNodeStrategy(pSyncNode) == SYNC_STRATEGY_STANDARD_SNAPSHOT) {
    if (pMsg->msgType == TDMT_SYNC_TIMEOUT) {
      SyncTimeout *pSyncMsg = syncTimeoutFromRpcMsg2(pMsg);
      code = syncNodeOnTimeoutCb(pSyncNode, pSyncMsg);
      syncTimeoutDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_PING) {
      SyncPing *pSyncMsg = syncPingFromRpcMsg2(pMsg);
      code = syncNodeOnPingCb(pSyncNode, pSyncMsg);
      syncPingDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_PING_REPLY) {
      SyncPingReply *pSyncMsg = syncPingReplyFromRpcMsg2(pMsg);
      code = syncNodeOnPingReplyCb(pSyncNode, pSyncMsg);
      syncPingReplyDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_CLIENT_REQUEST) {
      SyncClientRequest *pSyncMsg = syncClientRequestFromRpcMsg2(pMsg);
      code = syncNodeOnClientRequestCb(pSyncNode, pSyncMsg, NULL);
      syncClientRequestDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_REQUEST_VOTE) {
      SyncRequestVote *pSyncMsg = syncRequestVoteFromRpcMsg2(pMsg);
      code = syncNodeOnRequestVoteSnapshotCb(pSyncNode, pSyncMsg);
      syncRequestVoteDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_REQUEST_VOTE_REPLY) {
      SyncRequestVoteReply *pSyncMsg = syncRequestVoteReplyFromRpcMsg2(pMsg);
      code = syncNodeOnRequestVoteReplySnapshotCb(pSyncNode, pSyncMsg);
      syncRequestVoteReplyDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_APPEND_ENTRIES) {
      SyncAppendEntries *pSyncMsg = syncAppendEntriesFromRpcMsg2(pMsg);
      code = syncNodeOnAppendEntriesSnapshotCb(pSyncNode, pSyncMsg);
      syncAppendEntriesDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_APPEND_ENTRIES_REPLY) {
      SyncAppendEntriesReply *pSyncMsg = syncAppendEntriesReplyFromRpcMsg2(pMsg);
      code = syncNodeOnAppendEntriesReplySnapshotCb(pSyncNode, pSyncMsg);
      syncAppendEntriesReplyDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_SNAPSHOT_SEND) {
      SyncSnapshotSend *pSyncMsg = syncSnapshotSendFromRpcMsg2(pMsg);
      code = syncNodeOnSnapshotSendCb(pSyncNode, pSyncMsg);
      syncSnapshotSendDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_SNAPSHOT_RSP) {
      SyncSnapshotRsp *pSyncMsg = syncSnapshotRspFromRpcMsg2(pMsg);
      code = syncNodeOnSnapshotRspCb(pSyncNode, pSyncMsg);
      syncSnapshotRspDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_SET_MNODE_STANDBY) {
      code = syncSetStandby(pMgmt->sync);
      SRpcMsg rsp = {.code = code, .info = pMsg->info};
      tmsgSendRsp(&rsp);
    } else {
      mError("failed to process msg:%p since invalid type:%s", pMsg, TMSG_INFO(pMsg->msgType));
      code = -1;
    }
  } else {
    if (pMsg->msgType == TDMT_SYNC_TIMEOUT) {
      SyncTimeout *pSyncMsg = syncTimeoutFromRpcMsg2(pMsg);
      code = syncNodeOnTimeoutCb(pSyncNode, pSyncMsg);
      syncTimeoutDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_PING) {
      SyncPing *pSyncMsg = syncPingFromRpcMsg2(pMsg);
      code = syncNodeOnPingCb(pSyncNode, pSyncMsg);
      syncPingDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_PING_REPLY) {
      SyncPingReply *pSyncMsg = syncPingReplyFromRpcMsg2(pMsg);
      code = syncNodeOnPingReplyCb(pSyncNode, pSyncMsg);
      syncPingReplyDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_CLIENT_REQUEST) {
      SyncClientRequest *pSyncMsg = syncClientRequestFromRpcMsg2(pMsg);
      code = syncNodeOnClientRequestCb(pSyncNode, pSyncMsg, NULL);
      syncClientRequestDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_REQUEST_VOTE) {
      SyncRequestVote *pSyncMsg = syncRequestVoteFromRpcMsg2(pMsg);
      code = syncNodeOnRequestVoteCb(pSyncNode, pSyncMsg);
      syncRequestVoteDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_REQUEST_VOTE_REPLY) {
      SyncRequestVoteReply *pSyncMsg = syncRequestVoteReplyFromRpcMsg2(pMsg);
      code = syncNodeOnRequestVoteReplyCb(pSyncNode, pSyncMsg);
      syncRequestVoteReplyDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_APPEND_ENTRIES) {
      SyncAppendEntries *pSyncMsg = syncAppendEntriesFromRpcMsg2(pMsg);
      code = syncNodeOnAppendEntriesCb(pSyncNode, pSyncMsg);
      syncAppendEntriesDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_APPEND_ENTRIES_REPLY) {
      SyncAppendEntriesReply *pSyncMsg = syncAppendEntriesReplyFromRpcMsg2(pMsg);
      code = syncNodeOnAppendEntriesReplyCb(pSyncNode, pSyncMsg);
      syncAppendEntriesReplyDestroy(pSyncMsg);
    } else if (pMsg->msgType == TDMT_SYNC_SET_MNODE_STANDBY) {
      code = syncSetStandby(pMgmt->sync);
      SRpcMsg rsp = {.code = code, .info = pMsg->info};
      tmsgSendRsp(&rsp);
    } else {
      mError("failed to process msg:%p since invalid type:%s", pMsg, TMSG_INFO(pMsg->msgType));
      code = -1;
    }
  }

  syncNodeRelease(pSyncNode);

  if (code != 0) {
    terrno = TSDB_CODE_SYN_INTERNAL_ERROR;
  }
  return code;
}

static int32_t mndCheckMnodeState(SRpcMsg *pMsg) {
  if (!IsReq(pMsg)) return 0;
  if (pMsg->msgType == TDMT_SCH_QUERY || pMsg->msgType == TDMT_SCH_MERGE_QUERY ||
      pMsg->msgType == TDMT_SCH_QUERY_CONTINUE || pMsg->msgType == TDMT_SCH_QUERY_HEARTBEAT ||
      pMsg->msgType == TDMT_SCH_FETCH || pMsg->msgType == TDMT_SCH_MERGE_FETCH || pMsg->msgType == TDMT_SCH_DROP_TASK) {
    return 0;
  }
  if (mndAcquireRpcRef(pMsg->info.node) == 0) return 0;
  if (pMsg->msgType == TDMT_MND_MQ_TIMER || pMsg->msgType == TDMT_MND_TELEM_TIMER ||
      pMsg->msgType == TDMT_MND_TRANS_TIMER || pMsg->msgType == TDMT_MND_TTL_TIMER) {
    return -1;
  }

  SEpSet epSet = {0};
  mndGetMnodeEpSet(pMsg->info.node, &epSet);

  const STraceId *trace = &pMsg->info.traceId;
  mError("msg:%p, failed to check mnode state since %s, type:%s, numOfMnodes:%d inUse:%d", pMsg, terrstr(),
         TMSG_INFO(pMsg->msgType), epSet.numOfEps, epSet.inUse);

  if (epSet.numOfEps > 0) {
    for (int32_t i = 0; i < epSet.numOfEps; ++i) {
      mInfo("mnode index:%d, ep:%s:%u", i, epSet.eps[i].fqdn, epSet.eps[i].port);
    }

    int32_t contLen = tSerializeSEpSet(NULL, 0, &epSet);
    pMsg->info.rsp = rpcMallocCont(contLen);
    pMsg->info.hasEpSet = 1;
    if (pMsg->info.rsp != NULL) {
      tSerializeSEpSet(pMsg->info.rsp, contLen, &epSet);
      pMsg->info.rspLen = contLen;
      terrno = TSDB_CODE_RPC_REDIRECT;
    } else {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
    }
  } else {
    terrno = TSDB_CODE_APP_NOT_READY;
  }

  return -1;
}

static int32_t mndCheckMsgContent(SRpcMsg *pMsg) {
  if (!IsReq(pMsg)) return 0;
  if (pMsg->contLen != 0 && pMsg->pCont != NULL) return 0;

  const STraceId *trace = &pMsg->info.traceId;
  mGError("msg:%p, failed to check msg, cont:%p contLen:%d, app:%p type:%s", pMsg, pMsg->pCont, pMsg->contLen,
          pMsg->info.ahandle, TMSG_INFO(pMsg->msgType));
  terrno = TSDB_CODE_INVALID_MSG_LEN;
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

  if (mndCheckMsgContent(pMsg) != 0) return -1;
  if (mndCheckMnodeState(pMsg) != 0) return -1;

  mGTrace("msg:%p, start to process in mnode, app:%p type:%s", pMsg, pMsg->info.ahandle, TMSG_INFO(pMsg->msgType));
  int32_t code = (*fp)(pMsg);
  mndReleaseRpcRef(pMnode);

  if (code == TSDB_CODE_ACTION_IN_PROGRESS) {
    mGTrace("msg:%p, won't response immediately since in progress", pMsg);
  } else if (code == 0) {
    mGTrace("msg:%p, successfully processed", pMsg);
  } else {
    mGError("msg:%p, failed to process since %s, app:%p type:%s", pMsg, terrstr(), pMsg->info.ahandle,
            TMSG_INFO(pMsg->msgType));
  }

  return code;
}

void mndSetMsgHandle(SMnode *pMnode, tmsg_t msgType, MndMsgFp fp) {
  tmsg_t type = TMSG_INDEX(msgType);
  if (type >= 0 && type < TDMT_MAX) {
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
  if (mndAcquireRpcRef(pMnode) != 0) return -1;

  SSdb   *pSdb = pMnode->pSdb;
  int64_t ms = taosGetTimestampMs();

  pClusterInfo->dnodes = taosArrayInit(sdbGetSize(pSdb, SDB_DNODE), sizeof(SMonDnodeDesc));
  pClusterInfo->mnodes = taosArrayInit(sdbGetSize(pSdb, SDB_MNODE), sizeof(SMonMnodeDesc));
  pVgroupInfo->vgroups = taosArrayInit(sdbGetSize(pSdb, SDB_VGROUP), sizeof(SMonVgroupDesc));
  pStbInfo->stbs = taosArrayInit(sdbGetSize(pSdb, SDB_STB), sizeof(SMonStbDesc));
  if (pClusterInfo->dnodes == NULL || pClusterInfo->mnodes == NULL || pVgroupInfo->vgroups == NULL ||
      pStbInfo->stbs == NULL) {
    mndReleaseRpcRef(pMnode);
    return -1;
  }

  // cluster info
  tstrncpy(pClusterInfo->version, version, sizeof(pClusterInfo->version));
  pClusterInfo->monitor_interval = tsMonitorInterval;
  pClusterInfo->connections_total = mndGetNumOfConnections(pMnode);
  pClusterInfo->dbs_total = sdbGetSize(pSdb, SDB_DB);
  pClusterInfo->stbs_total = sdbGetSize(pSdb, SDB_STB);

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
      pClusterInfo->master_uptime = (ms - pObj->stateStartTime) / (86400000.0f);
      tstrncpy(desc.role, syncStr(TAOS_SYNC_STATE_LEADER), sizeof(desc.role));
    } else {
      tstrncpy(desc.role, syncStr(pObj->state), sizeof(desc.role));
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
      tstrncpy(pVnDesc->vnode_role, syncStr(pVgid->role), sizeof(pVnDesc->vnode_role));
      if (pVgid->role == TAOS_SYNC_STATE_LEADER) {
        tstrncpy(desc.status, "ready", sizeof(desc.status));
        pClusterInfo->vgroups_alive++;
      }
      if (pVgid->role != TAOS_SYNC_STATE_ERROR) {
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
  pGrantInfo->expire_time = (pMnode->grant.expireTimeMS - ms) / 86400000.0f;
  pGrantInfo->timeseries_total = pMnode->grant.timeseriesAllowed;
  if (pMnode->grant.expireTimeMS == 0) {
    pGrantInfo->expire_time = INT32_MAX;
    pGrantInfo->timeseries_total = INT32_MAX;
  }

  mndReleaseRpcRef(pMnode);
  return 0;
}

int32_t mndGetLoad(SMnode *pMnode, SMnodeLoad *pLoad) {
  pLoad->syncState = syncGetMyRole(pMnode->syncMgmt.sync);
  mTrace("mnode current syncstate is %s", syncStr(pLoad->syncState));
  return 0;
}

int32_t mndAcquireRpcRef(SMnode *pMnode) {
  int32_t code = 0;
  taosThreadRwlockRdlock(&pMnode->lock);
  if (pMnode->stopped) {
    terrno = TSDB_CODE_APP_NOT_READY;
    code = -1;
  } else if (!mndIsMaster(pMnode)) {
    code = -1;
  } else {
    int32_t ref = atomic_add_fetch_32(&pMnode->rpcRef, 1);
    // mTrace("mnode rpc is acquired, ref:%d", ref);
  }
  taosThreadRwlockUnlock(&pMnode->lock);
  return code;
}

void mndReleaseRpcRef(SMnode *pMnode) {
  taosThreadRwlockRdlock(&pMnode->lock);
  int32_t ref = atomic_sub_fetch_32(&pMnode->rpcRef, 1);
  // mTrace("mnode rpc is released, ref:%d", ref);
  taosThreadRwlockUnlock(&pMnode->lock);
}

void mndSetRestore(SMnode *pMnode, bool restored) {
  if (restored) {
    taosThreadRwlockWrlock(&pMnode->lock);
    pMnode->restored = true;
    taosThreadRwlockUnlock(&pMnode->lock);
    mTrace("mnode set restored:%d", restored);
  } else {
    taosThreadRwlockWrlock(&pMnode->lock);
    pMnode->restored = false;
    taosThreadRwlockUnlock(&pMnode->lock);
    mTrace("mnode set restored:%d", restored);
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
  mTrace("mnode set stopped");
}

bool mndGetStop(SMnode *pMnode) { return pMnode->stopped; }
