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
#include "mndAuth.h"
#include "mndBnode.h"
#include "mndCluster.h"
#include "mndConsumer.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndFunc.h"
#include "mndMnode.h"
#include "mndProfile.h"
#include "mndQnode.h"
#include "mndShow.h"
#include "mndSnode.h"
#include "mndStb.h"
#include "mndSubscribe.h"
#include "mndSync.h"
#include "mndTelem.h"
#include "mndTopic.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"

int32_t mndSendReqToDnode(SMnode *pMnode, SEpSet *pEpSet, SRpcMsg *pMsg) {
  if (pMnode == NULL || pMnode->sendReqToDnodeFp == NULL) {
    terrno = TSDB_CODE_MND_NOT_READY;
    return -1;
  }

  return (*pMnode->sendReqToDnodeFp)(pMnode->pDnode, pEpSet, pMsg);
}

int32_t mndSendReqToMnode(SMnode *pMnode, SRpcMsg *pMsg) {
  if (pMnode == NULL || pMnode->sendReqToDnodeFp == NULL) {
    terrno = TSDB_CODE_MND_NOT_READY;
    return -1;
  }

  return (*pMnode->sendReqToMnodeFp)(pMnode->pDnode, pMsg);
}

void mndSendRedirectRsp(SMnode *pMnode, SRpcMsg *pMsg) {
  if (pMnode != NULL && pMnode->sendRedirectRspFp != NULL) {
    (*pMnode->sendRedirectRspFp)(pMnode->pDnode, pMsg);
  }
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

static void mndTransReExecute(void *param, void *tmrId) {
  SMnode *pMnode = param;
  if (mndIsMaster(pMnode)) {
    int32_t contLen = 0;
    void *  pReq = mndBuildTimerMsg(&contLen);
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_TRANS, .pCont = pReq, .contLen = contLen};
    pMnode->putReqToMWriteQFp(pMnode->pDnode, &rpcMsg);
  }

  taosTmrReset(mndTransReExecute, 3000, pMnode, pMnode->timer, &pMnode->transTimer);
}

static void mndCalMqRebalance(void *param, void *tmrId) {
  SMnode *pMnode = param;
  if (mndIsMaster(pMnode)) {
    int32_t contLen = 0;
    void *  pReq = mndBuildTimerMsg(&contLen);
    SRpcMsg rpcMsg = {.msgType = TDMT_MND_MQ_TIMER, .pCont = pReq, .contLen = contLen};
    pMnode->putReqToMReadQFp(pMnode->pDnode, &rpcMsg);
  }

  taosTmrReset(mndCalMqRebalance, 3000, pMnode, pMnode->timer, &pMnode->mqTimer);
}

static int32_t mndInitTimer(SMnode *pMnode) {
  if (pMnode->timer == NULL) {
    pMnode->timer = taosTmrInit(5000, 200, 3600000, "MND");
  }

  if (pMnode->timer == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (taosTmrReset(mndTransReExecute, 6000, pMnode, pMnode->timer, &pMnode->transTimer)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (taosTmrReset(mndCalMqRebalance, 3000, pMnode, pMnode->timer, &pMnode->mqTimer)) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void mndCleanupTimer(SMnode *pMnode) {
  if (pMnode->timer != NULL) {
    taosTmrStop(pMnode->transTimer);
    pMnode->transTimer = NULL;
    taosTmrStop(pMnode->mqTimer);
    pMnode->mqTimer = NULL;
    taosTmrCleanUp(pMnode->timer);
    pMnode->timer = NULL;
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

static int32_t mndInitSdb(SMnode *pMnode) {
  SSdbOpt opt = {0};
  opt.path = pMnode->path;
  opt.pMnode = pMnode;

  pMnode->pSdb = sdbInit(&opt);
  if (pMnode->pSdb == NULL) {
    return -1;
  }

  return 0;
}

static int32_t mndDeploySdb(SMnode *pMnode) { return sdbDeploy(pMnode->pSdb); }
static int32_t mndReadSdb(SMnode *pMnode) { return sdbReadFile(pMnode->pSdb); }

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
  if (mndAllocStep(pMnode, "mnode-sdb", mndInitSdb, mndCleanupSdb) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-trans", mndInitTrans, mndCleanupTrans) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-cluster", mndInitCluster, mndCleanupCluster) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-mnode", mndInitMnode, mndCleanupMnode) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-qnode", mndInitQnode, mndCleanupQnode) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-qnode", mndInitSnode, mndCleanupSnode) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-qnode", mndInitBnode, mndCleanupBnode) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-dnode", mndInitDnode, mndCleanupDnode) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-user", mndInitUser, mndCleanupUser) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-auth", mndInitAuth, mndCleanupAuth) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-acct", mndInitAcct, mndCleanupAcct) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-topic", mndInitTopic, mndCleanupTopic) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-consumer", mndInitConsumer, mndCleanupConsumer) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-subscribe", mndInitSubscribe, mndCleanupSubscribe) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-vgroup", mndInitVgroup, mndCleanupVgroup) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-stb", mndInitStb, mndCleanupStb) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-db", mndInitDb, mndCleanupDb) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-func", mndInitFunc, mndCleanupFunc) != 0) return -1;
  if (pMnode->clusterId <= 0) {
    if (mndAllocStep(pMnode, "mnode-sdb-deploy", mndDeploySdb, NULL) != 0) return -1;
  } else {
    if (mndAllocStep(pMnode, "mnode-sdb-read", mndReadSdb, NULL) != 0) return -1;
  }
  if (mndAllocStep(pMnode, "mnode-timer", mndInitTimer, NULL) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-profile", mndInitProfile, mndCleanupProfile) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-show", mndInitShow, mndCleanupShow) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-sync", mndInitSync, mndCleanupSync) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-telem", mndInitTelem, mndCleanupTelem) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-timer", NULL, mndCleanupTimer) != 0) return -1;

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
    }
  }

  return 0;
}

static int32_t mndSetOptions(SMnode *pMnode, const SMnodeOpt *pOption) {
  pMnode->dnodeId = pOption->dnodeId;
  pMnode->clusterId = pOption->clusterId;
  pMnode->replica = pOption->replica;
  pMnode->selfIndex = pOption->selfIndex;
  memcpy(&pMnode->replicas, pOption->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
  pMnode->pDnode = pOption->pDnode;
  pMnode->putReqToMWriteQFp = pOption->putReqToMWriteQFp;
  pMnode->putReqToMReadQFp = pOption->putReqToMReadQFp;
  pMnode->sendReqToDnodeFp = pOption->sendReqToDnodeFp;
  pMnode->sendReqToMnodeFp = pOption->sendReqToMnodeFp;
  pMnode->sendRedirectRspFp = pOption->sendRedirectRspFp;
  pMnode->cfg.sver = pOption->cfg.sver;
  pMnode->cfg.enableTelem = pOption->cfg.enableTelem;
  pMnode->cfg.statusInterval = pOption->cfg.statusInterval;
  pMnode->cfg.shellActivityTimer = pOption->cfg.shellActivityTimer;
  pMnode->cfg.timezone = strdup(pOption->cfg.timezone);
  pMnode->cfg.locale = strdup(pOption->cfg.locale);
  pMnode->cfg.charset = strdup(pOption->cfg.charset);
  pMnode->cfg.gitinfo = strdup(pOption->cfg.gitinfo);
  pMnode->cfg.buildinfo = strdup(pOption->cfg.buildinfo);

  if (pMnode->sendReqToDnodeFp == NULL || pMnode->sendReqToMnodeFp == NULL || pMnode->sendRedirectRspFp == NULL ||
      pMnode->putReqToMWriteQFp == NULL || pMnode->dnodeId < 0 || pMnode->clusterId < 0 ||
      pMnode->cfg.statusInterval < 1) {
    terrno = TSDB_CODE_MND_INVALID_OPTIONS;
    return -1;
  }

  if (pMnode->cfg.timezone == NULL || pMnode->cfg.locale == NULL || pMnode->cfg.charset == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

SMnode *mndOpen(const char *path, const SMnodeOpt *pOption) {
  mDebug("start to open mnode in %s", path);

  SMnode *pMnode = calloc(1, sizeof(SMnode));
  if (pMnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to open mnode since %s", terrstr());
    return NULL;
  }

  char timestr[24] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &pMnode->checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);

  pMnode->pSteps = taosArrayInit(24, sizeof(SMnodeStep));
  if (pMnode->pSteps == NULL) {
    free(pMnode);
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

  code = mndSetOptions(pMnode, pOption);
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

void mndClose(SMnode *pMnode) {
  if (pMnode != NULL) {
    mDebug("start to close mnode");
    mndCleanupSteps(pMnode, -1);
    tfree(pMnode->path);
    tfree(pMnode->cfg.charset);
    tfree(pMnode->cfg.locale);
    tfree(pMnode->cfg.timezone);
    tfree(pMnode->cfg.gitinfo);
    tfree(pMnode->cfg.buildinfo);
    tfree(pMnode);
    mDebug("mnode is closed");
  }
}

int32_t mndAlter(SMnode *pMnode, const SMnodeOpt *pOption) {
  mDebug("start to alter mnode");
  mDebug("mnode is altered");
  return 0;
}

void mndDestroy(const char *path) {
  mDebug("start to destroy mnode at %s", path);
  taosRemoveDir(path);
  mDebug("mnode is destroyed");
}

int32_t mndGetLoad(SMnode *pMnode, SMnodeLoad *pLoad) {
  pLoad->numOfDnode = 0;
  pLoad->numOfMnode = 0;
  pLoad->numOfVgroup = 0;
  pLoad->numOfDatabase = 0;
  pLoad->numOfSuperTable = 0;
  pLoad->numOfChildTable = 0;
  pLoad->numOfColumn = 0;
  pLoad->totalPoints = 0;
  pLoad->totalStorage = 0;
  pLoad->compStorage = 0;

  return 0;
}

SMnodeMsg *mndInitMsg(SMnode *pMnode, SRpcMsg *pRpcMsg) {
  SMnodeMsg *pMsg = taosAllocateQitem(sizeof(SMnodeMsg));
  if (pMsg == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to create msg since %s, app:%p RPC:%p", terrstr(), pRpcMsg->ahandle, pRpcMsg->handle);
    return NULL;
  }

  if (pRpcMsg->msgType != TDMT_MND_TRANS && pRpcMsg->msgType != TDMT_MND_MQ_TIMER &&
      pRpcMsg->msgType != TDMT_MND_MQ_DO_REBALANCE) {
    SRpcConnInfo connInfo = {0};
    if ((pRpcMsg->msgType & 1U) && rpcGetConnInfo(pRpcMsg->handle, &connInfo) != 0) {
      taosFreeQitem(pMsg);
      terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
      mError("failed to create msg since %s, app:%p RPC:%p", terrstr(), pRpcMsg->ahandle, pRpcMsg->handle);
      return NULL;
    }
    memcpy(pMsg->user, connInfo.user, TSDB_USER_LEN);
  }

  pMsg->pMnode = pMnode;
  pMsg->rpcMsg = *pRpcMsg;
  pMsg->createdTime = taosGetTimestampSec();

  mTrace("msg:%p, is created, app:%p RPC:%p user:%s", pMsg, pRpcMsg->ahandle, pRpcMsg->handle, pMsg->user);
  return pMsg;
}

void mndCleanupMsg(SMnodeMsg *pMsg) {
  mTrace("msg:%p, is destroyed, app:%p RPC:%p", pMsg, pMsg->rpcMsg.ahandle, pMsg->rpcMsg.handle);
  rpcFreeCont(pMsg->rpcMsg.pCont);
  pMsg->rpcMsg.pCont = NULL;
  taosFreeQitem(pMsg);
}

void mndSendRsp(SMnodeMsg *pMsg, int32_t code) {
  SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .code = code};
  rpcSendResponse(&rpcRsp);
}

void mndProcessMsg(SMnodeMsg *pMsg) {
  SMnode *pMnode = pMsg->pMnode;
  int32_t code = 0;
  tmsg_t  msgType = pMsg->rpcMsg.msgType;
  void *  ahandle = pMsg->rpcMsg.ahandle;
  bool    isReq = (msgType & 1U);

  mTrace("msg:%p, type:%s will be processed, app:%p", pMsg, TMSG_INFO(msgType), ahandle);

  if (isReq && !mndIsMaster(pMnode)) {
    code = TSDB_CODE_APP_NOT_READY;
    mDebug("msg:%p, failed to process since %s, app:%p", pMsg, terrstr(), ahandle);
    goto PROCESS_RPC_END;
  }

  if (isReq && pMsg->rpcMsg.pCont == NULL) {
    code = TSDB_CODE_MND_INVALID_MSG_LEN;
    mError("msg:%p, failed to process since %s, app:%p", pMsg, terrstr(), ahandle);
    goto PROCESS_RPC_END;
  }

  MndMsgFp fp = pMnode->msgFp[TMSG_INDEX(msgType)];
  if (fp == NULL) {
    code = TSDB_CODE_MSG_NOT_PROCESSED;
    mError("msg:%p, failed to process since no msg handle, app:%p", pMsg, ahandle);
    goto PROCESS_RPC_END;
  }

  code = (*fp)(pMsg);
  if (code == TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mTrace("msg:%p, in progress, app:%p", pMsg, ahandle);
    return;
  } else if (code != 0) {
    code = terrno;
    mError("msg:%p, failed to process since %s, app:%p", pMsg, terrstr(), ahandle);
    goto PROCESS_RPC_END;
  } else {
    mTrace("msg:%p, is processed, app:%p", pMsg, ahandle);
  }

PROCESS_RPC_END:
  if (isReq) {
    if (pMsg->rpcMsg.handle == NULL) return;

    if (code == TSDB_CODE_APP_NOT_READY) {
      mndSendRedirectRsp(pMnode, &pMsg->rpcMsg);
    } else if (code != 0) {
      SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .code = code};
      rpcSendResponse(&rpcRsp);
    } else {
      SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .contLen = pMsg->contLen, .pCont = pMsg->pCont};
      rpcSendResponse(&rpcRsp);
    }
  }
}

void mndSetMsgHandle(SMnode *pMnode, tmsg_t msgType, MndMsgFp fp) {
  tmsg_t type = TMSG_INDEX(msgType);
  if (type >= 0 && type < TDMT_MAX) {
    pMnode->msgFp[type] = fp;
  }
}

uint64_t mndGenerateUid(char *name, int32_t len) {
  int64_t  us = taosGetTimestampUs();
  int32_t  hashval = MurmurHash3_32(name, len);
  uint64_t x = (us & 0x000000FFFFFFFFFF) << 24;
  return x + ((hashval & ((1ul << 16) - 1ul)) << 8) + (taosRand() & ((1ul << 8) - 1ul));
}
