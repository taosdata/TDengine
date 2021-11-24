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
#include "os.h"
#include "tglobal.h"
#include "tqueue.h"
#include "mnodeAcct.h"
#include "mnodeAuth.h"
#include "mnodeBalance.h"
#include "mnodeCluster.h"
#include "mnodeDb.h"
#include "mnodeDnode.h"
#include "mnodeFunc.h"
#include "mnodeMnode.h"
#include "mnodeOper.h"
#include "mnodeProfile.h"
#include "mnodeShow.h"
#include "mnodeStable.h"
#include "mnodeSync.h"
#include "mnodeTelem.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"
#include "mnodeTrans.h"

SMnodeBak tsMint = {0};

int32_t mnodeGetDnodeId() { return tsMint.para.dnodeId; }

int64_t mnodeGetClusterId() { return tsMint.para.clusterId; }

void mnodeSendMsgToDnode(SMnode *pMnode, struct SEpSet *epSet, struct SRpcMsg *rpcMsg) {
  assert(pMnode);
  (*pMnode->sendMsgToDnodeFp)(pMnode->pServer, epSet, rpcMsg);
}

void mnodeSendMsgToMnode(SMnode *pMnode, struct SRpcMsg *rpcMsg) {
  assert(pMnode);
  (*pMnode->sendMsgToMnodeFp)(pMnode->pServer, rpcMsg);
}

void mnodeSendRedirectMsg(SMnode *pMnode, struct SRpcMsg *rpcMsg, bool forShell) {
  assert(pMnode);
  (*pMnode->sendRedirectMsgFp)(pMnode->pServer, rpcMsg, forShell);
}

static int32_t mnodeInitTimer() {
  if (tsMint.timer == NULL) {
    tsMint.timer = taosTmrInit(tsMaxShellConns, 200, 3600000, "MND");
  }

  if (tsMint.timer == NULL) {
    return -1;
  }

  return 0;
}

static void mnodeCleanupTimer() {
  if (tsMint.timer != NULL) {
    taosTmrCleanUp(tsMint.timer);
    tsMint.timer = NULL;
  }
}

tmr_h mnodeGetTimer() { return tsMint.timer; }

static int32_t mnodeSetOptions(SMnode *pMnode, const SMnodeOptions *pOptions) {
  pMnode->dnodeId = pOptions->dnodeId;
  pMnode->clusterId = pOptions->clusterId;
  pMnode->replica = pOptions->replica;
  pMnode->selfIndex = pOptions->selfIndex;
  memcpy(&pMnode->replicas, pOptions->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
  pMnode->pServer = pOptions->pDnode;
  pMnode->putMsgToApplyMsgFp = pOptions->putMsgToApplyMsgFp;
  pMnode->sendMsgToDnodeFp = pOptions->sendMsgToDnodeFp;
  pMnode->sendMsgToMnodeFp = pOptions->sendMsgToMnodeFp;
  pMnode->sendRedirectMsgFp = pOptions->sendRedirectMsgFp;

  if (pMnode->sendMsgToDnodeFp == NULL || pMnode->sendMsgToMnodeFp == NULL || pMnode->sendRedirectMsgFp == NULL ||
      pMnode->putMsgToApplyMsgFp == NULL || pMnode->dnodeId < 0 || pMnode->clusterId < 0) {
    terrno = TSDB_CODE_MND_APP_ERROR;
    return -1;
  }

  return 0;
}

static int32_t mnodeAllocInitSteps() {
  struct SSteps *steps = taosStepInit(16, NULL);
  if (steps == NULL) return -1;

  if (taosStepAdd(steps, "mnode-trans", mnodeInitTrans, mnodeCleanupTrans) != 0) return -1;
  if (taosStepAdd(steps, "mnode-cluster", mnodeInitCluster, mnodeCleanupCluster) != 0) return -1;
  if (taosStepAdd(steps, "mnode-dnode", mnodeInitDnode, mnodeCleanupDnode) != 0) return -1;
  if (taosStepAdd(steps, "mnode-mnode", mnodeInitMnode, mnodeCleanupMnode) != 0) return -1;
  if (taosStepAdd(steps, "mnode-acct", mnodeInitAcct, mnodeCleanupAcct) != 0) return -1;
  if (taosStepAdd(steps, "mnode-auth", mnodeInitAuth, mnodeCleanupAuth) != 0) return -1;
  if (taosStepAdd(steps, "mnode-user", mnodeInitUser, mnodeCleanupUser) != 0) return -1;
  if (taosStepAdd(steps, "mnode-db", mnodeInitDb, mnodeCleanupDb) != 0) return -1;
  if (taosStepAdd(steps, "mnode-vgroup", mnodeInitVgroup, mnodeCleanupVgroup) != 0) return -1;
  if (taosStepAdd(steps, "mnode-stable", mnodeInitStable, mnodeCleanupStable) != 0) return -1;
  if (taosStepAdd(steps, "mnode-func", mnodeInitFunc, mnodeCleanupFunc) != 0) return -1;
  if (taosStepAdd(steps, "mnode-sdb", sdbInit, sdbCleanup) != 0) return -1;

  tsMint.pInitSteps = steps;
  return 0;
}

static int32_t mnodeAllocStartSteps() {
  struct SSteps *steps = taosStepInit(8, NULL);
  if (steps == NULL) return -1;

  taosStepAdd(steps, "mnode-timer", mnodeInitTimer, NULL);
  taosStepAdd(steps, "mnode-sdb-file", sdbOpen, sdbClose);
  taosStepAdd(steps, "mnode-balance", mnodeInitBalance, mnodeCleanupBalance);
  taosStepAdd(steps, "mnode-profile", mnodeInitProfile, mnodeCleanupProfile);
  taosStepAdd(steps, "mnode-show", mnodeInitShow, mnodeCleanUpShow);
  taosStepAdd(steps, "mnode-sync", mnodeInitSync, mnodeCleanUpSync);
  taosStepAdd(steps, "mnode-telem", mnodeInitTelem, mnodeCleanupTelem);
  taosStepAdd(steps, "mnode-timer", NULL, mnodeCleanupTimer);

  tsMint.pStartSteps = steps;
  return 0;
}

SMnode *mnodeOpen(const char *path, const SMnodeOptions *pOptions) {
  SMnode *pMnode = calloc(1, sizeof(SMnode));

  if (mnodeSetOptions(pMnode, pOptions) != 0) {
    free(pMnode);
    mError("failed to init mnode options since %s", terrstr());
    return NULL;
  }

  if (mnodeAllocInitSteps() != 0) {
    mError("failed to alloc init steps since %s", terrstr());
    return NULL;
  }

  if (mnodeAllocStartSteps() != 0) {
    mError("failed to alloc start steps since %s", terrstr());
    return NULL;
  }

  taosStepExec(tsMint.pInitSteps);

  if (tsMint.para.dnodeId <= 0 && tsMint.para.clusterId <= 0) {
    if (sdbDeploy() != 0) {
      mError("failed to deploy sdb since %s", terrstr());
      return NULL;
    } else {
      mInfo("mnode is deployed");
    }
  }

  taosStepExec(tsMint.pStartSteps);

  return pMnode;
}

void mnodeClose(SMnode *pMnode) { free(pMnode); }

int32_t mnodeAlter(SMnode *pMnode, const SMnodeOptions *pOptions) { return 0; }

void mnodeDestroy(const char *path) { sdbUnDeploy(); }

int32_t mnodeGetLoad(SMnode *pMnode, SMnodeLoad *pLoad) { return 0; }

SMnodeMsg *mnodeInitMsg(SMnode *pMnode, SRpcMsg *pRpcMsg) {
  SMnodeMsg *pMsg = taosAllocateQitem(sizeof(SMnodeMsg));
  if (pMsg == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  if (rpcGetConnInfo(pRpcMsg->handle, &pMsg->conn) != 0) {
    mnodeCleanupMsg(pMsg);
    mError("can not get user from conn:%p", pMsg->rpcMsg.handle);
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    return NULL;
  }

  pMsg->rpcMsg = *pRpcMsg;
  pMsg->createdTime = taosGetTimestampSec();

  return pMsg;
}

void mnodeCleanupMsg(SMnodeMsg *pMsg) {
  if (pMsg->pUser != NULL) {
    sdbRelease(pMsg->pUser);
  }

  taosFreeQitem(pMsg);
}

void mnodeSendRsp(SMnodeMsg *pMsg, int32_t code) {}

static void mnodeProcessRpcMsg(SMnodeMsg *pMsg) {
  if (!mnodeIsMaster()) {
    mnodeSendRedirectMsg(NULL, &pMsg->rpcMsg, true);
    mnodeCleanupMsg(pMsg);
    return;
  }

  int32_t msgType = pMsg->rpcMsg.msgType;

  MnodeRpcFp fp = tsMint.msgFp[msgType];
  if (fp == NULL) {
  }

  int32_t code = (fp)(pMsg);
  if (code != 0) {
    assert(code);
  }
}

void mnodeSetMsgFp(int32_t msgType, MnodeRpcFp fp) {
  if (msgType > 0 || msgType < TSDB_MSG_TYPE_MAX) {
    tsMint.msgFp[msgType] = fp;
  }
}

void mnodeProcessReadMsg(SMnode *pMnode, SMnodeMsg *pMsg) { mnodeProcessRpcMsg(pMsg); }

void mnodeProcessWriteMsg(SMnode *pMnode, SMnodeMsg *pMsg) { mnodeProcessRpcMsg(pMsg); }

void mnodeProcessSyncMsg(SMnode *pMnode, SMnodeMsg *pMsg) { mnodeProcessRpcMsg(pMsg); }

void mnodeProcessApplyMsg(SMnode *pMnode, SMnodeMsg *pMsg) {}

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
    SMnRsp *rpcRsp = &pMsg->rpcRsp;
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
  mnodeSendRsp(pMsg, code);
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
    SMnRsp *rpcRsp = &pMsg->rpcRsp;
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
  mnodeSendRsp(pMsg, code);
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
    SMnRsp *rpcRsp = &pMsg->rpcRsp;
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
  mnodeSendRsp(pMsg, code);
}

static void mnodeProcessPeerRsp(SMnodeMsg *pMsg, void *unused) {
  int32_t  msgType = pMsg->rpcMsg.msgType;
  SRpcMsg *pRpcMsg = &pMsg->rpcMsg;

  if (!mnodeIsMaster()) {
    mError("msg:%p, ahandle:%p type:%s not processed for not master", pRpcMsg, pRpcMsg->ahandle, taosMsg[msgType]);
    mnodeCleanupMsg2(pMsg);
  }

  if (tsMworker.peerRspFp[msgType]) {
    (*tsMworker.peerRspFp[msgType])(pRpcMsg);
  } else {
    mError("msg:%p, ahandle:%p type:%s is not processed", pRpcMsg, pRpcMsg->ahandle, taosMsg[msgType]);
  }

  mnodeCleanupMsg2(pMsg);
}
#endif