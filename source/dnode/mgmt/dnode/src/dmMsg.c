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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "dmMsg.h"
#include "dmFile.h"
#include "dmWorker.h"
#include "vmInt.h"

void dmSendStatusReq(SDnodeMgmt *pMgmt) {
  SDnode    *pDnode = pMgmt->pDnode;
  SStatusReq req = {0};

  taosRLockLatch(&pMgmt->latch);
  req.sver = tsVersion;
  req.dver = pMgmt->dver;
  req.dnodeId = pMgmt->dnodeId;
  req.clusterId = pMgmt->clusterId;
  req.rebootTime = pDnode->rebootTime;
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

  req.pVloads = taosArrayInit(TSDB_MAX_VNODES, sizeof(SVnodeLoad));
  vmGetVnodeLoads(pDnode, req.pVloads);

  int32_t contLen = tSerializeSStatusReq(NULL, 0, &req);
  void   *pHead = rpcMallocCont(contLen);
  tSerializeSStatusReq(pHead, contLen, &req);
  taosArrayDestroy(req.pVloads);

  SRpcMsg rpcMsg = {.pCont = pHead, .contLen = contLen, .msgType = TDMT_MND_STATUS, .ahandle = (void *)9527};
  pMgmt->statusSent = 1;

  dTrace("pDnode:%p, send status req to mnode", pDnode);
  dndSendReqToMnode(pMgmt->pDnode, &rpcMsg);
}

static void dndUpdateDnodeCfg(SDnodeMgmt *pMgmt, SDnodeCfg *pCfg) {
  if (pMgmt->dnodeId == 0) {
    dInfo("set dnodeId:%d clusterId:%" PRId64, pCfg->dnodeId, pCfg->clusterId);
    taosWLockLatch(&pMgmt->latch);
    pMgmt->dnodeId = pCfg->dnodeId;
    pMgmt->clusterId = pCfg->clusterId;
    dmWriteFile(pMgmt);
    taosWUnLockLatch(&pMgmt->latch);
  }
}

void dmProcessStatusRsp(SDnode *pDnode, SRpcMsg *pRsp) {
  SDnodeMgmt *pMgmt = dndGetWrapper(pDnode, MNODE)->pMgmt;

  if (pRsp->code != TSDB_CODE_SUCCESS) {
    if (pRsp->code == TSDB_CODE_MND_DNODE_NOT_EXIST && !pMgmt->dropped && pMgmt->dnodeId > 0) {
      dInfo("dnode:%d, set to dropped since not exist in mnode", pMgmt->dnodeId);
      pMgmt->dropped = 1;
      dmWriteFile(pMgmt);
    }
  } else {
    SStatusRsp statusRsp = {0};
    if (pRsp->pCont != NULL && pRsp->contLen != 0 &&
        tDeserializeSStatusRsp(pRsp->pCont, pRsp->contLen, &statusRsp) == 0) {
      pMgmt->dver = statusRsp.dver;
      dndUpdateDnodeCfg(pMgmt, &statusRsp.dnodeCfg);
      dmUpdateDnodeEps(pMgmt, statusRsp.pDnodeEps);
    }
    taosArrayDestroy(statusRsp.pDnodeEps);
  }

  pMgmt->statusSent = 0;
}

void dmProcessAuthRsp(SDnode *pDnode, SRpcMsg *pReq) { dError("auth rsp is received, but not supported yet"); }

void dmProcessGrantRsp(SDnode *pDnode, SRpcMsg *pReq) { dError("grant rsp is received, but not supported yet"); }

int32_t dmProcessConfigReq(SDnode *pDnode, SRpcMsg *pReq) {
  dError("config req is received, but not supported yet");
  SDCfgDnodeReq *pCfg = pReq->pCont;
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

void dmProcessStartupReq(SDnode *pDnode, SRpcMsg *pReq) {
  dDebug("startup req is received");

  SStartupReq *pStartup = rpcMallocCont(sizeof(SStartupReq));
  dndGetStartup(pDnode, pStartup);

  dDebug("startup req is sent, step:%s desc:%s finished:%d", pStartup->name, pStartup->desc, pStartup->finished);

  SRpcMsg rpcRsp = {.handle = pReq->handle, .pCont = pStartup, .contLen = sizeof(SStartupReq)};
  rpcSendResponse(&rpcRsp);
}

void dmInitMsgHandles(SMgmtWrapper *pWrapper) {
  // Requests handled by DNODE
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_MNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_ALTER_MNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_MNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_QNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_QNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_SNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_SNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_BNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_BNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_VNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_ALTER_VNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_VNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_SYNC_VNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_COMPACT_VNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_CONFIG_DNODE, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_DND_NETWORK_TEST, dmProcessMgmtMsg);

  // Requests handled by MNODE
  dndSetMsgHandle(pWrapper, TDMT_MND_STATUS_RSP, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_GRANT_RSP, dmProcessMgmtMsg);
  dndSetMsgHandle(pWrapper, TDMT_MND_AUTH_RSP, dmProcessMgmtMsg);
}
