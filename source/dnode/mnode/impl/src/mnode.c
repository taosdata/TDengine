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
#include "mndBalance.h"
#include "mndCluster.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndFunc.h"
#include "mndMnode.h"
#include "mndOper.h"
#include "mndProfile.h"
#include "mndShow.h"
#include "mndStable.h"
#include "mndSync.h"
#include "mndTelem.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"

void mndSendMsgToDnode(SMnode *pMnode, SEpSet *pEpSet, SRpcMsg *pMsg) {
  if (pMnode != NULL && pMnode->sendMsgToDnodeFp != NULL) {
    (*pMnode->sendMsgToDnodeFp)(pMnode->pDnode, pEpSet, pMsg);
  }
}

void mndSendMsgToMnode(SMnode *pMnode, SRpcMsg *pMsg) {
  if (pMnode != NULL && pMnode->sendMsgToMnodeFp != NULL) {
    (*pMnode->sendMsgToMnodeFp)(pMnode->pDnode, pMsg);
  }
}

void mndSendRedirectMsg(SMnode *pMnode, SRpcMsg *pMsg) {
  if (pMnode != NULL && pMnode->sendRedirectMsgFp != NULL) {
    (*pMnode->sendRedirectMsgFp)(pMnode->pDnode, pMsg);
  }
}

static int32_t mndInitTimer(SMnode *pMnode) {
  if (pMnode->timer == NULL) {
    pMnode->timer = taosTmrInit(5000, 200, 3600000, "MND");
  }

  if (pMnode->timer == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  return 0;
}

static void mndCleanupTimer(SMnode *pMnode) {
  if (pMnode->timer != NULL) {
    taosTmrCleanUp(pMnode->timer);
    pMnode->timer = NULL;
  }
}

static int32_t mnodeCreateDir(SMnode *pMnode, const char *path) {
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
  if (mndAllocStep(pMnode, "mnode-dnode", mndInitDnode, mndCleanupDnode) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-mnode", mndInitMnode, mndCleanupMnode) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-acct", mndInitAcct, mndCleanupAcct) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-auth", mndInitAuth, mndCleanupAuth) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-user", mndInitUser, mndCleanupUser) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-db", mndInitDb, mndCleanupDb) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-vgroup", mndInitVgroup, mndCleanupVgroup) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-stable", mndInitStable, mndCleanupStable) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-func", mndInitFunc, mndCleanupFunc) != 0) return -1;
  if (pMnode->clusterId <= 0) {
    if (mndAllocStep(pMnode, "mnode-sdb-deploy", mndDeploySdb, NULL) != 0) return -1;
  } else {
    if (mndAllocStep(pMnode, "mnode-sdb-read", mndReadSdb, NULL) != 0) return -1;
  }
  if (mndAllocStep(pMnode, "mnode-timer", mndInitTimer, NULL) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-balance", mndInitBalance, mndCleanupBalance) != 0) return -1;
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
    mDebug("step:%s will cleanup", pStep->name);
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

    // (*pMnode->reportProgress)(pStep->name, "start initialize");

    if ((*pStep->initFp)(pMnode) != 0) {
      mError("step:%s exec failed since %s, start to cleanup", pStep->name, terrstr());
      mndCleanupSteps(pMnode, pos);
      return -1;
    } else {
      mDebug("step:%s is initialized", pStep->name);
    }

    // (*pMnode->reportProgress)(pStep->name, "initialize completed");
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
  pMnode->putMsgToApplyMsgFp = pOption->putMsgToApplyMsgFp;
  pMnode->sendMsgToDnodeFp = pOption->sendMsgToDnodeFp;
  pMnode->sendMsgToMnodeFp = pOption->sendMsgToMnodeFp;
  pMnode->sendRedirectMsgFp = pOption->sendRedirectMsgFp;
  pMnode->sver = pOption->sver;
  pMnode->statusInterval = pOption->statusInterval;
  pMnode->mnodeEqualVnodeNum = pOption->mnodeEqualVnodeNum;
  pMnode->timezone = strdup(pOption->timezone);
  pMnode->locale = strdup(pOption->locale);
  pMnode->charset = strdup(pOption->charset);

  if (pMnode->sendMsgToDnodeFp == NULL || pMnode->sendMsgToMnodeFp == NULL || pMnode->sendRedirectMsgFp == NULL ||
      pMnode->putMsgToApplyMsgFp == NULL || pMnode->dnodeId < 0 || pMnode->clusterId < 0 ||
      pMnode->statusInterval < 1 || pOption->mnodeEqualVnodeNum < 0) {
    terrno = TSDB_CODE_MND_INVALID_OPTIONS;
    return -1;
  }

  if (pMnode->timezone == NULL || pMnode->locale == NULL || pMnode->charset == NULL) {
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

  pMnode->pSteps = taosArrayInit(24, sizeof(SMnodeStep));
  if (pMnode->pSteps == NULL) {
    free(pMnode);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to open mnode since %s", terrstr());
    return NULL;
  }

  int32_t code = mnodeCreateDir(pMnode, path);
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
    tfree(pMnode->charset);
    tfree(pMnode->locale);
    tfree(pMnode->timezone);
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
    mError("failed to create msg since %s", terrstr());
    return NULL;
  }

  SRpcConnInfo connInfo = {0};
  if (rpcGetConnInfo(pRpcMsg->handle, &connInfo) != 0) {
    mndCleanupMsg(pMsg);
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    mError("failed to create msg since %s", terrstr());
    return NULL;
  }
  memcpy(pMsg->user, connInfo.user, TSDB_USER_LEN);

  pMsg->pMnode = pMnode;
  pMsg->rpcMsg = *pRpcMsg;
  pMsg->createdTime = taosGetTimestampSec();

  mTrace("msg:%p, is created", pMsg);
  return pMsg;
}

void mndCleanupMsg(SMnodeMsg *pMsg) {
  taosFreeQitem(pMsg);
  mTrace("msg:%p, is destroyed", pMsg);
}

void mndSendRsp(SMnodeMsg *pMsg, int32_t code) {
  SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .code = code};
  rpcSendResponse(&rpcRsp);
}

static void mndProcessRpcMsg(SMnodeMsg *pMsg) {
  SMnode *pMnode = pMsg->pMnode;
  int32_t code = 0;
  int32_t msgType = pMsg->rpcMsg.msgType;
  void   *ahandle = pMsg->rpcMsg.ahandle;
  bool    isReq = (msgType % 2 == 1);

  mTrace("msg:%p, app:%p will be processed", pMsg, ahandle);

  if (isReq && !mndIsMaster(pMnode)) {
    code = TSDB_CODE_APP_NOT_READY;
    mDebug("msg:%p, app:%p failed to process since %s", pMsg, ahandle, terrstr());
    goto PROCESS_RPC_END;
  }

  if (isReq && pMsg->rpcMsg.pCont == NULL) {
    code = TSDB_CODE_MND_INVALID_MSG_LEN;
    mError("msg:%p, app:%p failed to process since %s", pMsg, ahandle, terrstr());
    goto PROCESS_RPC_END;
  }

  MndMsgFp fp = pMnode->msgFp[msgType];
  if (fp == NULL) {
    code = TSDB_CODE_MSG_NOT_PROCESSED;
    mError("msg:%p, app:%p failed to process since not handle", pMsg, ahandle);
    goto PROCESS_RPC_END;
  }

  code = (*fp)(pMnode, pMsg);
  if (code != 0) {
    code = terrno;
    mError("msg:%p, app:%p failed to process since %s", pMsg, ahandle, terrstr());
    goto PROCESS_RPC_END;
  } else {
    mTrace("msg:%p, app:%p is processed", pMsg, ahandle);
  }

PROCESS_RPC_END:
  if (isReq) {
    if (code == TSDB_CODE_APP_NOT_READY) {
      mndSendRedirectMsg(pMnode, &pMsg->rpcMsg);
    } else if (code != 0) {
      SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .code = code};
      rpcSendResponse(&rpcRsp);
    } else {
      SRpcMsg rpcRsp = {.handle = pMsg->rpcMsg.handle, .contLen = pMsg->contLen, .pCont = pMsg->pCont};
      rpcSendResponse(&rpcRsp);
    }
  }
}

void mndSetMsgHandle(SMnode *pMnode, int32_t msgType, MndMsgFp fp) {
  if (msgType >= 0 && msgType < TSDB_MSG_TYPE_MAX) {
    pMnode->msgFp[msgType] = fp;
  }
}

void mndProcessReadMsg(SMnodeMsg *pMsg) { mndProcessRpcMsg(pMsg); }

void mndProcessWriteMsg(SMnodeMsg *pMsg) { mndProcessRpcMsg(pMsg); }

void mndProcessSyncMsg(SMnodeMsg *pMsg) { mndProcessRpcMsg(pMsg); }

void mndProcessApplyMsg(SMnodeMsg *pMsg) {}
