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
#include "dndImp.h"

static int32_t dmProcessStatusRsp(SDnode *pDnode, SRpcMsg *pRsp) {
  SDnode *pDnode = pMgmt->pDnode;

  if (pRsp->code != TSDB_CODE_SUCCESS) {
    if (pRsp->code == TSDB_CODE_MND_DNODE_NOT_EXIST && !pDnode->data.dropped && pDnode->data.dnodeId > 0) {
      dInfo("dnode:%d, set to dropped since not exist in mnode", pDnode->data.dnodeId);
      pDnode->data.dropped = 1;
      dmWriteFile(pMgmt);
    }
  } else {
    SStatusRsp statusRsp = {0};
    if (pRsp->pCont != NULL && pRsp->contLen != 0 &&
        tDeserializeSStatusRsp(pRsp->pCont, pRsp->contLen, &statusRsp) == 0) {
      pMgmt->dnodeVer = statusRsp.dnodeVer;
      dmUpdateDnodeCfg(pMgmt, &statusRsp.dnodeCfg);
      dmUpdateDnodeEps(pMgmt, statusRsp.pDnodeEps);
    }
    tFreeSStatusRsp(&statusRsp);
  }

  return TSDB_CODE_SUCCESS;
}

void dmSendStatusReq(SDnode *pDnode) {
  SStatusReq req = {0};

  taosRLockLatch(&pDnode->data.latch);
  req.sver = tsVersion;
  req.dnodeVer = pMgmt->dnodeVer;
  req.dnodeId = pDnode->data.dnodeId;
  req.clusterId = pDnode->data.clusterId;
  req.rebootTime = pDnode->data.rebootTime;
  req.updateTime = pMgmt->updateTime;
  req.numOfCores = tsNumOfCores;
  req.numOfSupportVnodes = pDnode->data.supportVnodes;
  tstrncpy(req.dnodeEp, pDnode->data.localEp, TSDB_EP_LEN);

  req.clusterCfg.statusInterval = tsStatusInterval;
  req.clusterCfg.checkTime = 0;
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &req.clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  memcpy(req.clusterCfg.timezone, tsTimezoneStr, TD_TIMEZONE_LEN);
  memcpy(req.clusterCfg.locale, tsLocale, TD_LOCALE_LEN);
  memcpy(req.clusterCfg.charset, tsCharset, TD_LOCALE_LEN);
  taosRUnLockLatch(&pDnode->data.latch);

  SMgmtWrapper *pWrapper = dndAcquireWrapper(pDnode, VNODE);
  if (pWrapper != NULL) {
    SMonVloadInfo info = {0};
    dmGetVnodeLoads(pWrapper, &info);
    req.pVloads = info.pVloads;
    dndReleaseWrapper(pWrapper);
  }

  int32_t contLen = tSerializeSStatusReq(NULL, 0, &req);
  void   *pHead = rpcMallocCont(contLen);
  tSerializeSStatusReq(pHead, contLen, &req);
  taosArrayDestroy(req.pVloads);

  SRpcMsg rpcMsg = {.pCont = pHead, .contLen = contLen, .msgType = TDMT_MND_STATUS, .ahandle = (void *)0x9527};
  SRpcMsg rspMsg = {0};
  
  dTrace("send req:%s to mnode, app:%p", TMSG_INFO(rpcMsg.msgType), rpcMsg.ahandle);
  dmSendToMnodeRecv(pDnode, rpcMsg, &rpcRsp);
  dmProcessStatusRsp(pDnode, &rpcRsp);
}

static void dmUpdateDnodeCfg(SDnodeData *pMgmt, SDnodeCfg *pCfg) {
  SDnode *pDnode = pMgmt->pDnode;

  if (pDnode->data.dnodeId == 0) {
    dInfo("set dnodeId:%d clusterId:%" PRId64, pCfg->dnodeId, pCfg->clusterId);
    taosWLockLatch(&pDnode->data.latch);
    pDnode->data.dnodeId = pCfg->dnodeId;
    pDnode->data.clusterId = pCfg->clusterId;
    dmWriteFile(pMgmt);
    taosWUnLockLatch(&pDnode->data.latch);
  }
}

int32_t dmProcessAuthRsp(SDnodeData *pMgmt, SNodeMsg *pMsg) {
  SRpcMsg *pRsp = &pMsg->rpcMsg;
  dError("auth rsp is received, but not supported yet");
  return 0;
}

int32_t dmProcessGrantRsp(SDnodeData *pMgmt, SNodeMsg *pMsg) {
  SRpcMsg *pRsp = &pMsg->rpcMsg;
  dError("grant rsp is received, but not supported yet");
  return 0;
}

int32_t dmProcessConfigReq(SDnodeData *pMgmt, SNodeMsg *pMsg) {
  SRpcMsg       *pReq = &pMsg->rpcMsg;
  SDCfgDnodeReq *pCfg = pReq->pCont;
  dError("config req is received, but not supported yet");
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

static int32_t dmProcessCreateNodeMsg(SDnode *pDnode, EDndNodeType ntype, SNodeMsg *pMsg) {
  SMgmtWrapper *pWrapper = dndAcquireWrapper(pDnode, ntype);
  if (pWrapper != NULL) {
    dndReleaseWrapper(pWrapper);
    terrno = TSDB_CODE_NODE_ALREADY_DEPLOYED;
    dError("failed to create node since %s", terrstr());
    return -1;
  }

  pWrapper = &pDnode->wrappers[ntype];

  if (taosMkDir(pWrapper->path) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to create dir:%s since %s", pWrapper->path, terrstr());
    return -1;
  }

  int32_t code = (*pWrapper->fp.createFp)(pWrapper, pMsg);
  if (code != 0) {
    dError("node:%s, failed to open since %s", pWrapper->name, terrstr());
  } else {
    dDebug("node:%s, has been opened", pWrapper->name);
    pWrapper->deployed = true;
  }

  return code;
}

static int32_t dmProcessDropNodeMsg(SDnode *pDnode, EDndNodeType ntype, SNodeMsg *pMsg) {
  SMgmtWrapper *pWrapper = dndAcquireWrapper(pDnode, ntype);
  if (pWrapper == NULL) {
    terrno = TSDB_CODE_NODE_NOT_DEPLOYED;
    dError("failed to drop node since %s", terrstr());
    return -1;
  }

  taosWLockLatch(&pWrapper->latch);
  pWrapper->deployed = false;

  int32_t code = (*pWrapper->fp.dropFp)(pWrapper, pMsg);
  if (code != 0) {
    pWrapper->deployed = true;
    dError("node:%s, failed to drop since %s", pWrapper->name, terrstr());
  } else {
    pWrapper->deployed = false;
    dDebug("node:%s, has been dropped", pWrapper->name);
  }

  taosWUnLockLatch(&pWrapper->latch);
  dndReleaseWrapper(pWrapper);
  return code;
}

int32_t dmProcessCDnodeReq(SDnode *pDnode, SNodeMsg *pMsg) {
  switch (pMsg->rpcMsg.msgType) {
    case TDMT_DND_CREATE_MNODE:
      return dmProcessCreateNodeMsg(pDnode, MNODE, pMsg);
    case TDMT_DND_DROP_MNODE:
      return dmProcessDropNodeMsg(pDnode, MNODE, pMsg);
    case TDMT_DND_CREATE_QNODE:
      return dmProcessCreateNodeMsg(pDnode, QNODE, pMsg);
    case TDMT_DND_DROP_QNODE:
      return dmProcessDropNodeMsg(pDnode, QNODE, pMsg);
    case TDMT_DND_CREATE_SNODE:
      return dmProcessCreateNodeMsg(pDnode, SNODE, pMsg);
    case TDMT_DND_DROP_SNODE:
      return dmProcessDropNodeMsg(pDnode, SNODE, pMsg);
    case TDMT_DND_CREATE_BNODE:
      return dmProcessCreateNodeMsg(pDnode, BNODE, pMsg);
    case TDMT_DND_DROP_BNODE:
      return dmProcessDropNodeMsg(pDnode, BNODE, pMsg);
    default:
      terrno = TSDB_CODE_MSG_NOT_PROCESSED;
      return -1;
  }
}

static void dmSetMsgHandle(SMgmtWrapper *pWrapper) {
  // Requests handled by DNODE
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_MNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_MNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_QNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_QNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_SNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_SNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_CREATE_BNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_DROP_BNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_DND_CONFIG_DNODE, dmProcessMgmtMsg, DEFAULT_HANDLE);

  // Requests handled by MNODE
  dndSetMsgHandle(pWrapper, TDMT_MND_GRANT_RSP, dmProcessMgmtMsg, DEFAULT_HANDLE);
  dndSetMsgHandle(pWrapper, TDMT_MND_AUTH_RSP, dmProcessMgmtMsg, DEFAULT_HANDLE);
}

static int32_t dmStart(SMgmtWrapper *pWrapper) { return dmStartStatusThread(pWrapper->pDnode); }

static void dmStop(SMgmtWrapper *pWrapper) { dmStopThread(pWrapper->pDnode); }

static int32_t dmInit(SMgmtWrapper *pWrapper) {
  dInfo("dnode-data start to init");
  SDnode *pDnode = pWrapper->pDnode;

  pDnode->data.dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pDnode->data.dnodeHash == NULL) {
    dError("failed to init dnode hash");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dmReadFile(pDnode) != 0) {
    dError("failed to read file since %s", terrstr());
    return -1;
  }

  if (pDnode->data.dropped) {
    dError("dnode will not start since its already dropped");
    return -1;
  }

  if (dmStartWorker(pDnode) != 0) {
    return -1;
  }

  if (dmInitTrans(pDnode) != 0) {
    dError("failed to init transport since %s", terrstr());
    return -1;
  }

  dInfo("dnode-data is initialized");
  return 0;
}

static void dmCleanup(SMgmtWrapper *pWrapper) {
  dInfo("dnode-data start to clean up");
  SDnode *pDnode = pWrapper->pDnode;
  dmStopWorker(pDnode);

  taosWLockLatch(&pDnode->data.latch);
  if (pMgmt->dnodeEps != NULL) {
    taosArrayDestroy(pMgmt->dnodeEps);
    pMgmt->dnodeEps = NULL;
  }
  if (pMgmt->dnodeHash != NULL) {
    taosHashCleanup(pMgmt->dnodeHash);
    pMgmt->dnodeHash = NULL;
  }
  taosWUnLockLatch(&pDnode->data.latch);

  dndCleanupTrans(pDnode);
  dInfo("dnode-data is cleaned up");
}

static int32_t dmRequire(SMgmtWrapper *pWrapper, bool *required) {
  *required = true;
  return 0;
}

void dmSetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = dmInit;
  mgmtFp.closeFp = dmCleanup;
  mgmtFp.startFp = dmStart;
  mgmtFp.stopFp = dmStop;
  mgmtFp.requiredFp = dmRequire;

  dmSetMsgHandle(pWrapper);
  pWrapper->name = "dnode";
  pWrapper->fp = mgmtFp;
}
