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
#include "tstep.h"
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

static struct {
  int32_t    dnodeId;
  int64_t    clusterId;
  tmr_h      timer;
  SSteps    *pInitSteps;
  SSteps    *pStartSteps;
  SMnodePara para;
  MnodeRpcFp msgFp[TSDB_MSG_TYPE_MAX];
} tsMint;

int32_t mnodeGetDnodeId() { return tsMint.para.dnodeId; }

int64_t mnodeGetClusterId() { return tsMint.para.clusterId; }

void mnodeSendMsgToDnode(struct SEpSet *epSet, struct SRpcMsg *rpcMsg) { (*tsMint.para.SendMsgToDnode)(epSet, rpcMsg); }

void mnodeSendMsgToMnode(struct SRpcMsg *rpcMsg) { return (*tsMint.para.SendMsgToMnode)(rpcMsg); }

void mnodeSendRedirectMsg(struct SRpcMsg *rpcMsg, bool forShell) { (*tsMint.para.SendRedirectMsg)(rpcMsg, forShell); }

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

static int32_t mnodeSetPara(SMnodePara para) {
  tsMint.para = para;

  if (tsMint.para.SendMsgToDnode == NULL) {
    terrno = TSDB_CODE_MND_APP_ERROR;
    return -1;
  }

  if (tsMint.para.SendMsgToMnode == NULL) {
    terrno = TSDB_CODE_MND_APP_ERROR;
    return -1;
  }

  if (tsMint.para.SendRedirectMsg == NULL) {
    terrno = TSDB_CODE_MND_APP_ERROR;
    return -1;
  }

  if (tsMint.para.PutMsgIntoApplyQueue == NULL) {
    terrno = TSDB_CODE_MND_APP_ERROR;
    return -1;
  }

  if (tsMint.para.dnodeId < 0) {
    terrno = TSDB_CODE_MND_APP_ERROR;
    return -1;
  }

  if (tsMint.para.clusterId < 0) {
    terrno = TSDB_CODE_MND_APP_ERROR;
    return -1;
  }

  return 0;
}

static int32_t mnodeAllocInitSteps() {
  struct SSteps *steps = taosStepInit(16, NULL);
  if (steps == NULL) return -1;

  if (taosStepAdd(steps, "mnode-trans", trnInit, trnCleanup) != 0) return -1;
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
  taosStepAdd(steps, "mnode-sdb-file", sdbRead, (CleanupFp)sdbCommit);
  taosStepAdd(steps, "mnode-balance", mnodeInitBalance, mnodeCleanupBalance);
  taosStepAdd(steps, "mnode-profile", mnodeInitProfile, mnodeCleanupProfile);
  taosStepAdd(steps, "mnode-show", mnodeInitShow, mnodeCleanUpShow);
  taosStepAdd(steps, "mnode-sync", mnodeInitSync, mnodeCleanUpSync);
  taosStepAdd(steps, "mnode-telem", mnodeInitTelem, mnodeCleanupTelem);
  taosStepAdd(steps, "mnode-timer", NULL, mnodeCleanupTimer);

  tsMint.pStartSteps = steps;
  return 0;
}

int32_t mnodeInit(SMnodePara para) {
  if (mnodeSetPara(para) != 0) {
    mError("failed to init mnode para since %s", terrstr());
    return -1;
  }

  if (mnodeAllocInitSteps() != 0) {
    mError("failed to alloc init steps since %s", terrstr());
    return -1;
  }

  if (mnodeAllocStartSteps() != 0) {
    mError("failed to alloc start steps since %s", terrstr());
    return -1;
  }

  return taosStepExec(tsMint.pInitSteps);
}

void mnodeCleanup() { taosStepCleanup(tsMint.pInitSteps); }

int32_t mnodeDeploy(SMnodeCfg *pCfg) {
  if (tsMint.para.dnodeId <= 0 && tsMint.para.clusterId <= 0) {
    if (sdbDeploy() != 0) {
      mError("failed to deploy sdb since %s", terrstr());
      return -1;
    }
  }

  mDebug("mnode is deployed");
  return 0;
}

void mnodeUnDeploy() { sdbUnDeploy(); }

int32_t mnodeStart(SMnodeCfg *pCfg) { return taosStepExec(tsMint.pStartSteps); }

int32_t mnodeAlter(SMnodeCfg *pCfg) { return 0; }

void mnodeStop() { taosStepCleanup(tsMint.pStartSteps); }

int32_t mnodeGetLoad(SMnodeLoad *pLoad) { return 0; }

SMnodeMsg *mnodeInitMsg(SRpcMsg *pRpcMsg) {
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

static void mnodeProcessRpcMsg(SMnodeMsg *pMsg) {
  int32_t msgType = pMsg->rpcMsg.msgType;

  if (tsMint.msgFp[msgType] == NULL) {
  }

  (*tsMint.msgFp[msgType])(pMsg);
}

void mnodeSetMsgFp(int32_t msgType, MnodeRpcFp fp) {
  if (msgType > 0 || msgType < TSDB_MSG_TYPE_MAX) {
    tsMint.msgFp[msgType] = fp;
  }
}

void mnodeProcessMsg(SMnodeMsg *pMsg, EMnMsgType msgType) {
  if (!mnodeIsMaster()) {
    mnodeSendRedirectMsg(&pMsg->rpcMsg, true);
    mnodeCleanupMsg(pMsg);
    return;
  }

  switch (msgType) {
    case MN_MSG_TYPE_READ:
    case MN_MSG_TYPE_WRITE:
    case MN_MSG_TYPE_SYNC:
      mnodeProcessRpcMsg(pMsg);
      break;
    case MN_MSG_TYPE_APPLY:
      break;
    default:
      break;
  }
}

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