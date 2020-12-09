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
#include "tqueue.h"
#include "dnodeVMgmt.h"

typedef struct {
  SRpcMsg rpcMsg;
  char    pCont[];
} SMgmtMsg;

static taos_qset  tsMgmtQset = NULL;
static taos_queue tsMgmtQueue = NULL;
static pthread_t  tsQthread;

static void *  dnodeProcessMgmtQueue(void *param);
static int32_t dnodeProcessCreateVnodeMsg(SRpcMsg *pMsg);
static int32_t dnodeProcessAlterVnodeMsg(SRpcMsg *pMsg);
static int32_t dnodeProcessDropVnodeMsg(SRpcMsg *pMsg);
static int32_t dnodeProcessAlterStreamMsg(SRpcMsg *pMsg);
static int32_t dnodeProcessConfigDnodeMsg(SRpcMsg *pMsg);
static int32_t dnodeProcessCreateMnodeMsg(SRpcMsg *pMsg);
static int32_t (*dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *pMsg);

int32_t dnodeInitVMgmt() {
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = dnodeProcessCreateVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE]  = dnodeProcessAlterVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE]   = dnodeProcessDropVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = dnodeProcessAlterStreamMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE] = dnodeProcessConfigDnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CREATE_MNODE] = dnodeProcessCreateMnodeMsg;

  int32_t code = vnodeInitMgmt();
  if (code != TSDB_CODE_SUCCESS) return -1;

  tsMgmtQset = taosOpenQset();
  if (tsMgmtQset == NULL) {
    dError("failed to create the vmgmt queue set");
    return -1;
  }

  tsMgmtQueue = taosOpenQueue();
  if (tsMgmtQueue == NULL) {
    dError("failed to create the vmgmt queue");
    return -1;
  }

  taosAddIntoQset(tsMgmtQset, tsMgmtQueue, NULL);

  pthread_attr_t thAttr;
  pthread_attr_init(&thAttr);
  pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

  code = pthread_create(&tsQthread, &thAttr, dnodeProcessMgmtQueue, NULL);
  pthread_attr_destroy(&thAttr);
  if (code != 0) {
    dError("failed to create thread to process vmgmt queue, reason:%s", strerror(errno));
    return -1;
  }

  dInfo("dnode vmgmt is initialized");
  return TSDB_CODE_SUCCESS;
}

void dnodeCleanupVMgmt() {
  if (tsMgmtQset) taosQsetThreadResume(tsMgmtQset);
  if (tsQthread) pthread_join(tsQthread, NULL);

  if (tsMgmtQueue) taosCloseQueue(tsMgmtQueue);
  if (tsMgmtQset) taosCloseQset(tsMgmtQset);

  tsMgmtQset = NULL;
  tsMgmtQueue = NULL;

  vnodeCleanupMgmt();
}

static int32_t dnodeWriteToMgmtQueue(SRpcMsg *pMsg) {
  int32_t   size = sizeof(SMgmtMsg) + pMsg->contLen;
  SMgmtMsg *pMgmt = taosAllocateQitem(size);
  if (pMgmt == NULL) return TSDB_CODE_DND_OUT_OF_MEMORY;

  pMgmt->rpcMsg = *pMsg;
  pMgmt->rpcMsg.pCont = pMgmt->pCont;
  memcpy(pMgmt->pCont, pMsg->pCont, pMsg->contLen);
  taosWriteQitem(tsMgmtQueue, TAOS_QTYPE_RPC, pMgmt);

  return TSDB_CODE_SUCCESS;
}

void dnodeDispatchToVMgmtQueue(SRpcMsg *pMsg) {
  int32_t code = dnodeWriteToMgmtQueue(pMsg);
  if (code != TSDB_CODE_SUCCESS) {
    SRpcMsg rsp = {.handle = pMsg->handle, .code = code};
    rpcSendResponse(&rsp);
  }

  rpcFreeCont(pMsg->pCont);
}

static void *dnodeProcessMgmtQueue(void *param) {
  SMgmtMsg *pMgmt;
  SRpcMsg * pMsg;
  SRpcMsg   rsp = {0};
  int32_t   qtype;
  void *    handle;

  while (1) {
    if (taosReadQitemFromQset(tsMgmtQset, &qtype, (void **)&pMgmt, &handle) == 0) {
      dDebug("qset:%p, dnode mgmt got no message from qset, exit", tsMgmtQset);
      break;
    }

    pMsg = &pMgmt->rpcMsg;
    dTrace("msg:%p, ahandle:%p type:%s will be processed", pMgmt, pMsg->ahandle, taosMsg[pMsg->msgType]);
    if (dnodeProcessMgmtMsgFp[pMsg->msgType]) {
      rsp.code = (*dnodeProcessMgmtMsgFp[pMsg->msgType])(pMsg);
    } else {
      rsp.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    }

    dTrace("msg:%p, is processed, code:0x%x", pMgmt, rsp.code);
    if (rsp.code != TSDB_CODE_DND_ACTION_IN_PROGRESS) {
      rsp.handle = pMsg->handle;
      rsp.pCont = NULL;
      rpcSendResponse(&rsp);
    }

    taosFreeQitem(pMsg);
  }

  return NULL;
}

static SCreateVnodeMsg* dnodeParseVnodeMsg(SRpcMsg *rpcMsg) {
  SCreateVnodeMsg *pCreate = rpcMsg->pCont;
  pCreate->cfg.vgId                = htonl(pCreate->cfg.vgId);
  pCreate->cfg.cfgVersion          = htonl(pCreate->cfg.cfgVersion);
  pCreate->cfg.maxTables           = htonl(pCreate->cfg.maxTables);
  pCreate->cfg.cacheBlockSize      = htonl(pCreate->cfg.cacheBlockSize);
  pCreate->cfg.totalBlocks         = htonl(pCreate->cfg.totalBlocks);
  pCreate->cfg.daysPerFile         = htonl(pCreate->cfg.daysPerFile);
  pCreate->cfg.daysToKeep1         = htonl(pCreate->cfg.daysToKeep1);
  pCreate->cfg.daysToKeep2         = htonl(pCreate->cfg.daysToKeep2);
  pCreate->cfg.daysToKeep          = htonl(pCreate->cfg.daysToKeep);
  pCreate->cfg.minRowsPerFileBlock = htonl(pCreate->cfg.minRowsPerFileBlock);
  pCreate->cfg.maxRowsPerFileBlock = htonl(pCreate->cfg.maxRowsPerFileBlock);
  pCreate->cfg.fsyncPeriod         = htonl(pCreate->cfg.fsyncPeriod);
  pCreate->cfg.commitTime          = htonl(pCreate->cfg.commitTime);

  for (int32_t j = 0; j < pCreate->cfg.replications; ++j) {
    pCreate->nodes[j].nodeId = htonl(pCreate->nodes[j].nodeId);
  }

  return pCreate;
}

static int32_t dnodeProcessCreateVnodeMsg(SRpcMsg *rpcMsg) {
  SCreateVnodeMsg *pCreate = dnodeParseVnodeMsg(rpcMsg);

  void *pVnode = vnodeAcquire(pCreate->cfg.vgId);
  if (pVnode != NULL) {
    dDebug("vgId:%d, already exist, return success", pCreate->cfg.vgId);
    vnodeRelease(pVnode);
    return TSDB_CODE_SUCCESS;
  } else {
    dDebug("vgId:%d, create vnode msg is received", pCreate->cfg.vgId);
    return vnodeCreate(pCreate);
  }
}

static int32_t dnodeProcessAlterVnodeMsg(SRpcMsg *rpcMsg) {
  SAlterVnodeMsg *pAlter = dnodeParseVnodeMsg(rpcMsg);

  void *pVnode = vnodeAcquire(pAlter->cfg.vgId);
  if (pVnode != NULL) {
    dDebug("vgId:%d, alter vnode msg is received", pAlter->cfg.vgId);
    int32_t code = vnodeAlter(pVnode, pAlter);
    vnodeRelease(pVnode);
    return code;
  } else {
    dError("vgId:%d, vnode not exist, can't alter it", pAlter->cfg.vgId);
    return TSDB_CODE_VND_INVALID_VGROUP_ID;
  }
}

static int32_t dnodeProcessDropVnodeMsg(SRpcMsg *rpcMsg) {
  SDropVnodeMsg *pDrop = rpcMsg->pCont;
  pDrop->vgId = htonl(pDrop->vgId);

  return vnodeDrop(pDrop->vgId);
}

static int32_t dnodeProcessAlterStreamMsg(SRpcMsg *pMsg) {
  return 0;
}

static int32_t dnodeProcessConfigDnodeMsg(SRpcMsg *pMsg) {
  SCfgDnodeMsg *pCfg = pMsg->pCont;
  return taosCfgDynamicOptions(pCfg->config);
}

static int32_t dnodeProcessCreateMnodeMsg(SRpcMsg *pMsg) {
  SCreateMnodeMsg *pCfg = pMsg->pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  if (pCfg->dnodeId != dnodeGetDnodeId()) {
    dDebug("dnodeId:%d, in create mnode msg is not equal with saved dnodeId:%d", pCfg->dnodeId, dnodeGetDnodeId());
    return TSDB_CODE_MND_DNODE_ID_NOT_CONFIGURED;
  }

  if (strcmp(pCfg->dnodeEp, tsLocalEp) != 0) {
    dDebug("dnodeEp:%s, in create mnode msg is not equal with saved dnodeEp:%s", pCfg->dnodeEp, tsLocalEp);
    return TSDB_CODE_MND_DNODE_EP_NOT_CONFIGURED;
  }

  dDebug("dnodeId:%d, create mnode msg is received from mnodes, numOfMnodes:%d", pCfg->dnodeId, pCfg->mnodes.mnodeNum);
  for (int i = 0; i < pCfg->mnodes.mnodeNum; ++i) {
    pCfg->mnodes.mnodeInfos[i].mnodeId = htonl(pCfg->mnodes.mnodeInfos[i].mnodeId);
    dDebug("mnode index:%d, mnode:%d:%s", i, pCfg->mnodes.mnodeInfos[i].mnodeId, pCfg->mnodes.mnodeInfos[i].mnodeEp);
  }

  dnodeStartMnode(&pCfg->mnodes);

  return TSDB_CODE_SUCCESS;
}