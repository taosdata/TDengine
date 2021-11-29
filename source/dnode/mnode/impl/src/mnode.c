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

int32_t mndGetDnodeId(SMnode *pMnode) {
  if (pMnode != NULL) {
    return pMnode->dnodeId;
  }
  return -1;
}

int64_t mndGetClusterId(SMnode *pMnode) {
  if (pMnode != NULL) {
    return pMnode->clusterId;
  }
  return -1;
}

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

tmr_h mndGetTimer(SMnode *pMnode) {
  if (pMnode != NULL) {
    return pMnode->timer;
  }
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

  if (pMnode->sendMsgToDnodeFp == NULL || pMnode->sendMsgToMnodeFp == NULL || pMnode->sendRedirectMsgFp == NULL ||
      pMnode->putMsgToApplyMsgFp == NULL || pMnode->dnodeId < 0 || pMnode->clusterId < 0) {
    terrno = TSDB_CODE_MND_APP_ERROR;
    return -1;
  }

  return 0;
}

static int32_t mndAllocStep(SMnode *pMnode, char *name, MndInitFp initFp, MndCleanupFp cleanupFp) {
  SMnodeStep step = {0};
  step.name = name;
  step.initFp = initFp;
  step.cleanupFp = cleanupFp;
  if (taosArrayPush(&pMnode->steps, &step) != NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("failed to alloc step:%s since %s", name, terrstr());
    return -1;
  }

  return 0;
}

static int32_t mndInitSteps(SMnode *pMnode) {
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
  if (mndAllocStep(pMnode, "mnode-sdb", sdbInit, sdbCleanup) != 0) return -1;

  if (pMnode->replica == 1) {
    if (mndAllocStep(pMnode, "mnode-deploy-sdb", sdbDeploy, sdbClose) != 0) return -1;
  } else {
    if (mndAllocStep(pMnode, "mnode-open-sdb", sdbOpen, sdbClose) != 0) return -1;
  }

  if (mndAllocStep(pMnode, "mnode-timer", mndInitTimer, NULL) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-sdb-file", sdbOpen, sdbClose) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-balance", mndInitBalance, mndCleanupBalance) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-profile", mndInitProfile, mndCleanupProfile) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-show", mndInitShow, mndCleanupShow) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-sync", mndInitSync, mndCleanupSync) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-telem", mndInitTelem, mndCleanupTelem) != 0) return -1;
  if (mndAllocStep(pMnode, "mnode-timer", NULL, mndCleanupTimer) != 0) return -1;

  return 0;
}

static void mndCleanupSteps(SMnode *pMnode, int32_t pos) {
  if (pos == -1) {
    pos = taosArrayGetSize(&pMnode->steps);
  }

  for (int32_t s = pos; s >= 0; s--) {
    SMnodeStep *pStep = taosArrayGet(&pMnode->steps, pos);
    mDebug("step:%s will cleanup", pStep->name);
    if (pStep->cleanupFp != NULL) {
      (*pStep->cleanupFp)(pMnode);
    }
  }

  taosArrayClear(&pMnode->steps);
}

static int32_t mndExecSteps(SMnode *pMnode) {
  int32_t size = taosArrayGetSize(&pMnode->steps);
  for (int32_t pos = 0; pos < size; pos++) {
    SMnodeStep *pStep = taosArrayGet(&pMnode->steps, pos);
    if (pStep->initFp == NULL) continue;

    // (*pMnode->reportProgress)(pStep->name, "start initialize");

    int32_t code = (*pStep->initFp)(pMnode);
    if (code != 0) {
      mError("step:%s exec failed since %s, start to cleanup", pStep->name, tstrerror(code));
      mndCleanupSteps(pMnode, pos);
      terrno = code;
      return code;
    } else {
      mDebug("step:%s is initialized", pStep->name);
    }

    // (*pMnode->reportProgress)(pStep->name, "initialize completed");
  }
}

SMnode *mndOpen(const char *path, const SMnodeOpt *pOption) {
  SMnode *pMnode = calloc(1, sizeof(SMnode));

  int32_t code = mndSetOptions(pMnode, pOption);
  if (code != 0) {
    mndClose(pMnode);
    terrno = code;
    mError("failed to set mnode options since %s", terrstr());
    return NULL;
  }

  code = mndInitSteps(pMnode);
  if (code != 0) {
    mndClose(pMnode);
    terrno = code;
    mError("failed to int steps since %s", terrstr());
    return NULL;
  }

  code = mndExecSteps(pMnode);
  if (code != 0) {
    mndClose(pMnode);
    terrno = code;
    mError("failed to execute steps since %s", terrstr());
    return NULL;
  }

  mDebug("mnode:%p object is created", pMnode);
  return pMnode;
}

void mndClose(SMnode *pMnode) {
  mndCleanupSteps(pMnode, -1);
  free(pMnode);
  mDebug("mnode:%p object is cleaned up", pMnode);
}

int32_t mndAlter(SMnode *pMnode, const SMnodeOpt *pOption) {
  assert(1);
  return 0;
}

void mndDestroy(const char *path) {
  mDebug("mnode in %s will be destroyed", path);
  sdbUnDeploy();
}

int32_t mndGetLoad(SMnode *pMnode, SMnodeLoad *pLoad) {
  assert(1);
  return 0;
}

SMnodeMsg *mndInitMsg(SMnode *pMnode, SRpcMsg *pRpcMsg) {
  SMnodeMsg *pMsg = taosAllocateQitem(sizeof(SMnodeMsg));
  if (pMsg == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (rpcGetConnInfo(pRpcMsg->handle, &pMsg->conn) != 0) {
    mndCleanupMsg(pMsg);
    mError("can not get user from conn:%p", pMsg->rpcMsg.handle);
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    return NULL;
  }

  pMsg->rpcMsg = *pRpcMsg;
  pMsg->createdTime = taosGetTimestampSec();

  return pMsg;
}

void mndCleanupMsg(SMnodeMsg *pMsg) {
  if (pMsg->pUser != NULL) {
    sdbRelease(pMsg->pUser);
  }

  taosFreeQitem(pMsg);
}

void mndSendRsp(SMnodeMsg *pMsg, int32_t code) {}

static void mndProcessRpcMsg(SMnodeMsg *pMsg) {
  SMnode *pMnode = pMsg->pMnode;

  if (!mnodeIsMaster(pMnode)) {
    mndSendRedirectMsg(pMnode, &pMsg->rpcMsg);
    mndCleanupMsg(pMsg);
    return;
  }

  int32_t  msgType = pMsg->rpcMsg.msgType;
  MndMsgFp fp = pMnode->msgFp[msgType];
  if (fp == NULL) {
    mError("RPC %p, req:%s is not processed", pMsg->rpcMsg.handle, taosMsg[msgType]);
    SRpcMsg rspMsg = {.handle = pMsg->rpcMsg.handle, .code = TSDB_CODE_MSG_NOT_PROCESSED};
    rpcSendResponse(&rspMsg);
    mndCleanupMsg(pMsg);
    return;
  }

  int32_t code = (*fp)(pMnode, pMsg);
  if (code != 0) {
    mError("RPC %p, req:%s processed error since %s", pMsg->rpcMsg.handle, taosMsg[msgType], tstrerror(code));
    SRpcMsg rspMsg = {.handle = pMsg->rpcMsg.handle, .code = TSDB_CODE_MSG_NOT_PROCESSED};
    rpcSendResponse(&rspMsg);
  }

  mndCleanupMsg(pMsg);
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

#if 0

static void mnodeProcessWriteReq(SMnodeMsg *pMsg, void *unused) {
  int32_t msgType = pMsg->rpcMsg.msgType;
  void   *ahandle = pMsg->rpcMsg.ahandle;
  int32_t code = 0;

  if (pMsg->rpcMsg.pCont == NULL) {
    mError("msg:%p, app:%p type:%s content is null", pMsg, ahandle, taosMsg[msgType]);
    code = TSDB_CODE_MND_INVALID_MSG_LEN;
    goto PROCESS_WRITE_REQ_END;
  }

  if (!mnodeIsMaster()) {
    SMnodeRsp *rpcRsp = &pMsg->rpcRsp;
    SEpSet *epSet = rpcMallocCont(sizeof(SEpSet));
    mnodeGetMnodeEpSetForShell(epSet, true);
    rpcRsp->rsp = epSet;
    rpcRsp->len = sizeof(SEpSet);

    mDebug("msg:%p, app:%p type:%s in write queue, is redirected, numOfEps:%d inUse:%d", pMsg, ahandle,
           taosMsg[msgType], epSet->numOfEps, epSet->inUse);

    code = TSDB_CODE_RPC_REDIRECT;
    goto PROCESS_WRITE_REQ_END;
  }

  if (tsMworker.writeMsgFp[msgType] == NULL) {
    mError("msg:%p, app:%p type:%s not processed", pMsg, ahandle, taosMsg[msgType]);
    code = TSDB_CODE_MND_MSG_NOT_PROCESSED;
    goto PROCESS_WRITE_REQ_END;
  }

  code = (*tsMworker.writeMsgFp[msgType])(pMsg);

PROCESS_WRITE_REQ_END:
  mndSendRsp(pMsg, code);
}

static void mnodeProcessReadReq(SMnodeMsg *pMsg, void *unused) {
  int32_t msgType = pMsg->rpcMsg.msgType;
  void   *ahandle = pMsg->rpcMsg.ahandle;
  int32_t code = 0;

  if (pMsg->rpcMsg.pCont == NULL) {
    mError("msg:%p, app:%p type:%s in mread queue, content is null", pMsg, ahandle, taosMsg[msgType]);
    code = TSDB_CODE_MND_INVALID_MSG_LEN;
    goto PROCESS_READ_REQ_END;
  }

  if (!mnodeIsMaster()) {
    SMnodeRsp *rpcRsp = &pMsg->rpcRsp;
    SEpSet *epSet = rpcMallocCont(sizeof(SEpSet));
    if (!epSet) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto PROCESS_READ_REQ_END;
    }
    mnodeGetMnodeEpSetForShell(epSet, true);
    rpcRsp->rsp = epSet;
    rpcRsp->len = sizeof(SEpSet);

    mDebug("msg:%p, app:%p type:%s in mread queue is redirected, numOfEps:%d inUse:%d", pMsg, ahandle, taosMsg[msgType],
           epSet->numOfEps, epSet->inUse);
    code = TSDB_CODE_RPC_REDIRECT;
    goto PROCESS_READ_REQ_END;
  }

  if (tsMworker.readMsgFp[msgType] == NULL) {
    mError("msg:%p, app:%p type:%s in mread queue, not processed", pMsg, ahandle, taosMsg[msgType]);
    code = TSDB_CODE_MND_MSG_NOT_PROCESSED;
    goto PROCESS_READ_REQ_END;
  }

  mTrace("msg:%p, app:%p type:%s will be processed in mread queue", pMsg, ahandle, taosMsg[msgType]);
  code = (*tsMworker.readMsgFp[msgType])(pMsg);

PROCESS_READ_REQ_END:
  mndSendRsp(pMsg, code);
}

static void mnodeProcessPeerReq(SMnodeMsg *pMsg, void *unused) {
  int32_t msgType = pMsg->rpcMsg.msgType;
  void   *ahandle = pMsg->rpcMsg.ahandle;
  int32_t code = 0;

  if (pMsg->rpcMsg.pCont == NULL) {
    mError("msg:%p, ahandle:%p type:%s in mpeer queue, content is null", pMsg, ahandle, taosMsg[msgType]);
    code = TSDB_CODE_MND_INVALID_MSG_LEN;
    goto PROCESS_PEER_REQ_END;
  }

  if (!mnodeIsMaster()) {
    SMnodeRsp *rpcRsp = &pMsg->rpcRsp;
    SEpSet *epSet = rpcMallocCont(sizeof(SEpSet));
    mnodeGetMnodeEpSetForPeer(epSet, true);
    rpcRsp->rsp = epSet;
    rpcRsp->len = sizeof(SEpSet);

    mDebug("msg:%p, ahandle:%p type:%s in mpeer queue is redirected, numOfEps:%d inUse:%d", pMsg, ahandle,
           taosMsg[msgType], epSet->numOfEps, epSet->inUse);

    code = TSDB_CODE_RPC_REDIRECT;
    goto PROCESS_PEER_REQ_END;
  }

  if (tsMworker.peerReqFp[msgType] == NULL) {
    mError("msg:%p, ahandle:%p type:%s in mpeer queue, not processed", pMsg, ahandle, taosMsg[msgType]);
    code = TSDB_CODE_MND_MSG_NOT_PROCESSED;
    goto PROCESS_PEER_REQ_END;
  }

  code = (*tsMworker.peerReqFp[msgType])(pMsg);

PROCESS_PEER_REQ_END:
  mndSendRsp(pMsg, code);
}

static void mnodeProcessPeerRsp(SMnodeMsg *pMsg, void *unused) {
  int32_t  msgType = pMsg->rpcMsg.msgType;
  SRpcMsg *pRpcMsg = &pMsg->rpcMsg;

  if (!mnodeIsMaster()) {
    mError("msg:%p, ahandle:%p type:%s not processed for not master", pRpcMsg, pRpcMsg->ahandle, taosMsg[msgType]);
    mndCleanupMsg2(pMsg);
  }

  if (tsMworker.peerRspFp[msgType]) {
    (*tsMworker.peerRspFp[msgType])(pRpcMsg);
  } else {
    mError("msg:%p, ahandle:%p type:%s is not processed", pRpcMsg, pRpcMsg->ahandle, taosMsg[msgType]);
  }

  mndCleanupMsg2(pMsg);
}
#endif