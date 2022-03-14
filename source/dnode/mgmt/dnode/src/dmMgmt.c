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
#include "dmMgmt.h"
#include "dmWorker.h"
// #include "dmMgmt.h"
#include "dmFile.h"
#include "dmHandle.h"
#include "dndMonitor.h"
// #include "dndBnode.h"
// #include "mm.h"
// #include "dndQnode.h"
// #include "dndSnode.h"
#include "dndTransport.h"
// #include "dndVnodes.h"
#include "dndWorker.h"
// #include "monitor.h"

#if 0
static void dndProcessMgmtQueue(SDnode *pDnode, SRpcMsg *pMsg);


static int32_t dndProcessConfigDnodeReq(SDnode *pDnode, SRpcMsg *pReq);
static void    dndProcessStatusRsp(SDnode *pDnode, SRpcMsg *pRsp);
static void    dndProcessAuthRsp(SDnode *pDnode, SRpcMsg *pRsp);
static void    dndProcessGrantRsp(SDnode *pDnode, SRpcMsg *pRsp);

int32_t dndGetDnodeId(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosRLockLatch(&pMgmt->latch);
  int32_t dnodeId = pMgmt->dnodeId;
  taosRUnLockLatch(&pMgmt->latch);
  return dnodeId;
}

int64_t dndGetClusterId(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosRLockLatch(&pMgmt->latch);
  int64_t clusterId = pMgmt->clusterId;
  taosRUnLockLatch(&pMgmt->latch);
  return clusterId;
}

void dndGetDnodeEp(SDnode *pDnode, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosRLockLatch(&pMgmt->latch);

  SDnodeEp *pDnodeEp = taosHashGet(pMgmt->dnodeHash, &dnodeId, sizeof(int32_t));
  if (pDnodeEp != NULL) {
    if (pPort != NULL) {
      *pPort = pDnodeEp->ep.port;
    }
    if (pFqdn != NULL) {
      tstrncpy(pFqdn, pDnodeEp->ep.fqdn, TSDB_FQDN_LEN);
    }
    if (pEp != NULL) {
      snprintf(pEp, TSDB_EP_LEN, "%s:%u", pDnodeEp->ep.fqdn, pDnodeEp->ep.port);
    }
  }

  taosRUnLockLatch(&pMgmt->latch);
}

void dndGetMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosRLockLatch(&pMgmt->latch);
  *pEpSet = pMgmt->mnodeEpSet;
  taosRUnLockLatch(&pMgmt->latch);
}

void dndSendRedirectRsp(SDnode *pDnode, SRpcMsg *pReq) {
  tmsg_t msgType = pReq->msgType;

  SEpSet epSet = {0};
  dndGetMnodeEpSet(pDnode, &epSet);

  dDebug("RPC %p, req:%s is redirected, num:%d use:%d", pReq->handle, TMSG_INFO(msgType), epSet.numOfEps, epSet.inUse);
  for (int32_t i = 0; i < epSet.numOfEps; ++i) {
    dDebug("mnode index:%d %s:%u", i, epSet.eps[i].fqdn, epSet.eps[i].port);
    if (strcmp(epSet.eps[i].fqdn, pDnode->cfg.localFqdn) == 0 && epSet.eps[i].port == pDnode->cfg.serverPort) {
      epSet.inUse = (i + 1) % epSet.numOfEps;
    }

    epSet.eps[i].port = htons(epSet.eps[i].port);
  }

  rpcSendRedirectRsp(pReq->handle, &epSet);
}

static void dndUpdateMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet) {
  dInfo("mnode is changed, num:%d use:%d", pEpSet->numOfEps, pEpSet->inUse);

  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosWLockLatch(&pMgmt->latch);

  pMgmt->mnodeEpSet = *pEpSet;
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    dInfo("mnode index:%d %s:%u", i, pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
  }

  taosWUnLockLatch(&pMgmt->latch);
}


void dndSendStatusReq(SDnode *pDnode) {
  SStatusReq req = {0};

  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosRLockLatch(&pMgmt->latch);
  req.sver = tsVersion;
  req.dver = pMgmt->dver;
  req.dnodeId = pMgmt->dnodeId;
  req.clusterId = pMgmt->clusterId;
  req.rebootTime = pMgmt->rebootTime;
  req.updateTime = pMgmt->updateTime;
  req.numOfCores = tsNumOfCores;
  req.numOfSupportVnodes = pDnode->cfg.numOfSupportVnodes;
  memcpy(req.dnodeEp, pDnode->cfg.localEp, TSDB_EP_LEN);

  req.clusterCfg.statusInterval = tsStatusInterval;
  req.clusterCfg.checkTime = 0;
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &req.clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  memcpy(req.clusterCfg.timezone, tsTimezone, TD_TIMEZONE_LEN);
  memcpy(req.clusterCfg.locale, tsLocale, TD_LOCALE_LEN);
  memcpy(req.clusterCfg.charset, tsCharset, TD_LOCALE_LEN);
  taosRUnLockLatch(&pMgmt->latch);

#if 0
  req.pVloads = taosArrayInit(TSDB_MAX_VNODES, sizeof(SVnodeLoad));
  dndGetVnodeLoads(pDnode, req.pVloads);
#endif

  int32_t contLen = tSerializeSStatusReq(NULL, 0, &req);
  void   *pHead = rpcMallocCont(contLen);
  tSerializeSStatusReq(pHead, contLen, &req);
  taosArrayDestroy(req.pVloads);

  SRpcMsg rpcMsg = {.pCont = pHead, .contLen = contLen, .msgType = TDMT_MND_STATUS, .ahandle = (void *)9527};
  pMgmt->statusSent = 1;

  dTrace("pDnode:%p, send status req to mnode", pDnode);
  dndSendReqToMnode(pDnode, &rpcMsg);
}

static void dndUpdateDnodeCfg(SDnode *pDnode, SDnodeCfg *pCfg) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  if (pMgmt->dnodeId == 0) {
    dInfo("set dnodeId:%d clusterId:%" PRId64, pCfg->dnodeId, pCfg->clusterId);
    taosWLockLatch(&pMgmt->latch);
    pMgmt->dnodeId = pCfg->dnodeId;
    pMgmt->clusterId = pCfg->clusterId;
    dmWriteFile(pDnode);
    taosWUnLockLatch(&pMgmt->latch);
  }
}

static void dndProcessStatusRsp(SDnode *pDnode, SRpcMsg *pRsp) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  if (pRsp->code != TSDB_CODE_SUCCESS) {
    if (pRsp->code == TSDB_CODE_MND_DNODE_NOT_EXIST && !pMgmt->dropped && pMgmt->dnodeId > 0) {
      dInfo("dnode:%d, set to dropped since not exist in mnode", pMgmt->dnodeId);
      pMgmt->dropped = 1;
      dmWriteFile(pDnode);
    }
  } else {
    SStatusRsp statusRsp = {0};
    if (pRsp->pCont != NULL && pRsp->contLen != 0 &&
        tDeserializeSStatusRsp(pRsp->pCont, pRsp->contLen, &statusRsp) == 0) {
      pMgmt->dver = statusRsp.dver;
      dndUpdateDnodeCfg(pDnode, &statusRsp.dnodeCfg);
      dndUpdateDnodeEps(pDnode, statusRsp.pDnodeEps);
    }
    taosArrayDestroy(statusRsp.pDnodeEps);
  }

  pMgmt->statusSent = 0;
}

static void dndProcessAuthRsp(SDnode *pDnode, SRpcMsg *pReq) { dError("auth rsp is received, but not supported yet"); }

static void dndProcessGrantRsp(SDnode *pDnode, SRpcMsg *pReq) {
  dError("grant rsp is received, but not supported yet");
}

static int32_t dndProcessConfigDnodeReq(SDnode *pDnode, SRpcMsg *pReq) {
  dError("config req is received, but not supported yet");
  SDCfgDnodeReq *pCfg = pReq->pCont;
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

void dndProcessStartupReq(SDnode *pDnode, SRpcMsg *pReq) {
  dDebug("startup req is received");

  SStartupReq *pStartup = rpcMallocCont(sizeof(SStartupReq));
  dndGetStartup(pDnode, pStartup);

  dDebug("startup req is sent, step:%s desc:%s finished:%d", pStartup->name, pStartup->desc, pStartup->finished);

  SRpcMsg rpcRsp = {.handle = pReq->handle, .pCont = pStartup, .contLen = sizeof(SStartupReq)};
  rpcSendResponse(&rpcRsp);
}

void dndStopMgmt(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  dndCleanupWorker(&pMgmt->mgmtWorker);
  dndCleanupWorker(&pMgmt->statusWorker);

  if (pMgmt->threadId != NULL) {
    taosDestoryThread(pMgmt->threadId);
    pMgmt->threadId = NULL;
  }
}

void dndCleanupMgmt(SDnode *pDnode) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;
  taosWLockLatch(&pMgmt->latch);

  if (pMgmt->pDnodeEps != NULL) {
    taosArrayDestroy(pMgmt->pDnodeEps);
    pMgmt->pDnodeEps = NULL;
  }

  if (pMgmt->dnodeHash != NULL) {
    taosHashCleanup(pMgmt->dnodeHash);
    pMgmt->dnodeHash = NULL;
  }

  if (pMgmt->file != NULL) {
    free(pMgmt->file);
    pMgmt->file = NULL;
  }

  taosWUnLockLatch(&pMgmt->latch);
  dInfo("dnode-mgmt is cleaned up");
}

void dndProcessMgmtMsg(SDnode *pDnode, SRpcMsg *pMsg, SEpSet *pEpSet) {
  SDnodeMgmt *pMgmt = &pDnode->dmgmt;

  if (pEpSet && pEpSet->numOfEps > 0 && pMsg->msgType == TDMT_MND_STATUS_RSP) {
    dndUpdateMnodeEpSet(pDnode, pEpSet);
  }

  SDnodeWorker *pWorker = &pMgmt->mgmtWorker;
  if (pMsg->msgType == TDMT_MND_STATUS_RSP) {
    pWorker = &pMgmt->statusWorker;
  }

  if (dndWriteMsgToWorker(pWorker, pMsg, sizeof(SRpcMsg)) != 0) {
    if (pMsg->msgType & 1u) {
      SRpcMsg rsp = {.handle = pMsg->handle, .code = TSDB_CODE_OUT_OF_MEMORY};
      rpcSendResponse(&rsp);
    }
    rpcFreeCont(pMsg->pCont);
    taosFreeQitem(pMsg);
  }
}

static void dndProcessMgmtQueue(SDnode *pDnode, SRpcMsg *pMsg) {
  int32_t code = 0;

#if 0
  switch (pMsg->msgType) {
    case TDMT_DND_CREATE_MNODE:
      code = mmProcessCreateMnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_ALTER_MNODE:
      code = mmProcessAlterMnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_MNODE:
      code = mmProcessDropMnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_QNODE:
      code = dndProcessCreateQnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_QNODE:
      code = dndProcessDropQnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_SNODE:
      code = dndProcessCreateSnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_SNODE:
      code = dndProcessDropSnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_BNODE:
      code = dndProcessCreateBnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_BNODE:
      code = dndProcessDropBnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_CONFIG_DNODE:
      code = dndProcessConfigDnodeReq(pDnode, pMsg);
      break;
    case TDMT_MND_STATUS_RSP:
      dndProcessStatusRsp(pDnode, pMsg);
      break;
    case TDMT_MND_AUTH_RSP:
      dndProcessAuthRsp(pDnode, pMsg);
      break;
    case TDMT_MND_GRANT_RSP:
      dndProcessGrantRsp(pDnode, pMsg);
      break;
    case TDMT_DND_CREATE_VNODE:
      code = dndProcessCreateVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_ALTER_VNODE:
      code = dndProcessAlterVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_DROP_VNODE:
      code = dndProcessDropVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_SYNC_VNODE:
      code = dndProcessSyncVnodeReq(pDnode, pMsg);
      break;
    case TDMT_DND_COMPACT_VNODE:
      code = dndProcessCompactVnodeReq(pDnode, pMsg);
      break;
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      code = -1;
      dError("RPC %p, dnode msg:%s not processed", pMsg->handle, TMSG_INFO(pMsg->msgType));
      break;
  }
#endif
  if (pMsg->msgType & 1u) {
    if (code != 0) code = terrno;
    SRpcMsg rsp = {.code = code, .handle = pMsg->handle, .ahandle = pMsg->ahandle};
    rpcSendResponse(&rsp);
  }

  rpcFreeCont(pMsg->pCont);
  pMsg->pCont = NULL;
  taosFreeQitem(pMsg);
}

#endif



void dndStopMgmt(SDnode *pDnode) {}

void dndCleanupMgmt(SDnode *pDnode){}


void dndSendStatusReq(SDnode *pDnode){}


void dndGetMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet) {}


void dndProcessStartupReq(SDnode *pDnode, SRpcMsg *pReq){}
void dndProcessMgmtMsg(SDnode *pDnode, SMgmtWrapper *pWrapper, SNodeMsg *pMsg){}

static int32_t dmInit(SMgmtWrapper *pWrapper) {
  SDnodeMgmt *pMgmt = calloc(1, sizeof(SDnodeMgmt));

  pMgmt->dnodeId = 0;
  pMgmt->rebootTime = taosGetTimestampMs();
  pMgmt->dropped = 0;
  pMgmt->clusterId = 0;
  taosInitRWLatch(&pMgmt->latch);

  pMgmt->dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pMgmt->dnodeHash == NULL) {
    dError("node:%s, failed to init dnode hash", pWrapper->name);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dmReadFile(pWrapper->pDnode) != 0) {
    dError("node:%s, failed to read file since %s", pWrapper->name, terrstr());
    return -1;
  }

  if (pMgmt->dropped) {
    dError("node:%s, will not start since its already dropped", pWrapper->name);
    return -1;
  }

  if (dmStartWorker(pMgmt) != 0) {
     dError("node:%s, failed to start worker since %s", pWrapper->name, terrstr());
    return -1;
  }

  dInfo("dnode-mgmt is initialized");
  return 0;

  // dndSetStatus(pDnode, DND_STAT_RUNNING);
  // dndSendStatusReq(pDnode);
  // dndReportStartup(pDnode, "TDengine", "initialized successfully");

#if 0
  if (dndInitTrans(pDnode) != 0) {
    dError("failed to init transport since %s", terrstr());
    return -1;
  }

    SDiskCfg dCfg = {0};
  tstrncpy(dCfg.dir, pDnode->cfg.dataDir, TSDB_FILENAME_LEN);
  dCfg.level = 0;
  dCfg.primary = 1;
  SDiskCfg *pDisks = pDnode->cfg.pDisks;
  int32_t   numOfDisks = pDnode->cfg.numOfDisks;
  if (numOfDisks <= 0 || pDisks == NULL) {
    pDisks = &dCfg;
    numOfDisks = 1;
  }

  pDnode->pTfs = tfsOpen(pDisks, numOfDisks);
  if (pDnode->pTfs == NULL) {
    dError("failed to init tfs since %s", terrstr());
    return -1;
  }
#endif
}

static void dmCleanup(SDnode *pDnode, SMgmtWrapper *pWrapper){

}

static bool dmRequire(SMgmtWrapper *pWrapper) { return true; }

SMgmtFp dmGetMgmtFp() {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = dmInit;
  mgmtFp.closeFp = dmCleanup;
  mgmtFp.requiredFp = dmRequire;
  mgmtFp.getMsgHandleFp = dmGetMsgHandle;
  return mgmtFp;
}
