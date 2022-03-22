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
#include "dmInt.h"
#include "vm.h"

void dmSendStatusReq(SDnodeMgmt *pMgmt) {
  SDnode    *pDnode = pMgmt->pDnode;
  SStatusReq req = {0};

  taosRLockLatch(&pMgmt->latch);
  req.sver = tsVersion;
  req.dver = pMgmt->dver;
  req.dnodeId = pDnode->dnodeId;
  req.clusterId = pDnode->clusterId;
  req.rebootTime = pDnode->rebootTime;
  req.updateTime = pMgmt->updateTime;
  req.numOfCores = tsNumOfCores;
  req.numOfSupportVnodes = pDnode->numOfSupportVnodes;
  tstrncpy(req.dnodeEp, pDnode->localEp, TSDB_EP_LEN);

  req.clusterCfg.statusInterval = tsStatusInterval;
  req.clusterCfg.checkTime = 0;
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &req.clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  memcpy(req.clusterCfg.timezone, tsTimezone, TD_TIMEZONE_LEN);
  memcpy(req.clusterCfg.locale, tsLocale, TD_LOCALE_LEN);
  memcpy(req.clusterCfg.charset, tsCharset, TD_LOCALE_LEN);
  taosRUnLockLatch(&pMgmt->latch);

  req.pVloads = taosArrayInit(TSDB_MAX_VNODES, sizeof(SVnodeLoad));
  vmMonitorVnodeLoads(dndAcquireWrapper(pDnode, VNODES), req.pVloads);

  int32_t contLen = tSerializeSStatusReq(NULL, 0, &req);
  void   *pHead = rpcMallocCont(contLen);
  tSerializeSStatusReq(pHead, contLen, &req);
  taosArrayDestroy(req.pVloads);

  SRpcMsg rpcMsg = {.pCont = pHead, .contLen = contLen, .msgType = TDMT_MND_STATUS, .ahandle = (void *)9527};
  pMgmt->statusSent = 1;

  dTrace("send req:%s to mnode, app:%p", TMSG_INFO(rpcMsg.msgType), rpcMsg.ahandle);
  dndSendReqToMnode(pMgmt->pWrapper, &rpcMsg);
}

static void dmUpdateDnodeCfg(SDnodeMgmt *pMgmt, SDnodeCfg *pCfg) {
  SDnode *pDnode = pMgmt->pDnode;

  if (pDnode->dnodeId == 0) {
    dInfo("set dnodeId:%d clusterId:%" PRId64, pCfg->dnodeId, pCfg->clusterId);
    taosWLockLatch(&pMgmt->latch);
    pDnode->dnodeId = pCfg->dnodeId;
    pDnode->clusterId = pCfg->clusterId;
    dmWriteFile(pMgmt);
    taosWUnLockLatch(&pMgmt->latch);
  }
}

int32_t dmProcessStatusRsp(SDnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  SDnode  *pDnode = pMgmt->pDnode;
  SRpcMsg *pRsp = &pMsg->rpcMsg;

  if (pRsp->code != TSDB_CODE_SUCCESS) {
    if (pRsp->code == TSDB_CODE_MND_DNODE_NOT_EXIST && !pDnode->dropped && pDnode->dnodeId > 0) {
      dInfo("dnode:%d, set to dropped since not exist in mnode", pDnode->dnodeId);
      pDnode->dropped = 1;
      dmWriteFile(pMgmt);
    }
  } else {
    SStatusRsp statusRsp = {0};
    if (pRsp->pCont != NULL && pRsp->contLen != 0 &&
        tDeserializeSStatusRsp(pRsp->pCont, pRsp->contLen, &statusRsp) == 0) {
      pMgmt->dver = statusRsp.dver;
      dmUpdateDnodeCfg(pMgmt, &statusRsp.dnodeCfg);
      dmUpdateDnodeEps(pMgmt, statusRsp.pDnodeEps);
    }
    tFreeSStatusRsp(&statusRsp);
  }

  pMgmt->statusSent = 0;
}

int32_t dmProcessAuthRsp(SDnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  SRpcMsg *pRsp = &pMsg->rpcMsg;
  dError("auth rsp is received, but not supported yet");
  return 0;
}

int32_t dmProcessGrantRsp(SDnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  SRpcMsg *pRsp = &pMsg->rpcMsg;
  dError("grant rsp is received, but not supported yet");
  return 0;
}

int32_t dmProcessConfigReq(SDnodeMgmt *pMgmt, SNodeMsg *pMsg) {
  SRpcMsg       *pReq = &pMsg->rpcMsg;
  SDCfgDnodeReq *pCfg = pReq->pCont;
  dError("config req is received, but not supported yet");
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

void dmInitMsgHandles(SMgmtWrapper *pWrapper) {
  // Requests handled by DNODE
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_MNODE, (NodeMsgFp)dmProcessMgmtMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_MNODE, (NodeMsgFp)dmProcessMgmtMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_QNODE, (NodeMsgFp)dmProcessMgmtMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_QNODE, (NodeMsgFp)dmProcessMgmtMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_SNODE, (NodeMsgFp)dmProcessMgmtMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_SNODE, (NodeMsgFp)dmProcessMgmtMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_BNODE, (NodeMsgFp)dmProcessMgmtMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_BNODE, (NodeMsgFp)dmProcessMgmtMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_CONFIG_DNODE, (NodeMsgFp)dmProcessMgmtMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_DND_NETWORK_TEST, (NodeMsgFp)dmProcessMgmtMsg, 0);

  // Requests handled by MNODE
  dndSetMsgHandle(pWrapper, TDMT_MND_STATUS_RSP, (NodeMsgFp)dmProcessMgmtMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_GRANT_RSP, (NodeMsgFp)dmProcessMgmtMsg, 0);
  dndSetMsgHandle(pWrapper, TDMT_MND_AUTH_RSP, (NodeMsgFp)dmProcessMgmtMsg, 0);
}
