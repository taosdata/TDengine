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
#include "vnodeMain.h"
#include "vnodeMgmtMsg.h"

static SCreateVnodeMsg* vnodeParseVnodeMsg(SRpcMsg *rpcMsg) {
  SCreateVnodeMsg *pCreate = rpcMsg->pCont;
  pCreate->cfg.vgId                = htonl(pCreate->cfg.vgId);
  pCreate->cfg.dbCfgVersion        = htonl(pCreate->cfg.dbCfgVersion);
  pCreate->cfg.vgCfgVersion        = htonl(pCreate->cfg.vgCfgVersion);
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

  for (int32_t j = 0; j < pCreate->cfg.vgReplica; ++j) {
    pCreate->nodes[j].nodeId = htonl(pCreate->nodes[j].nodeId);
  }

  return pCreate;
}

int32_t vnodeProcessCreateVnodeMsg(SRpcMsg *rpcMsg) {
  SCreateVnodeMsg *pCreate = vnodeParseVnodeMsg(rpcMsg);
  SVnode *pVnode = vnodeAcquire(pCreate->cfg.vgId);
  if (pVnode != NULL) {
    vDebug("vgId:%d, already exist, return success", pCreate->cfg.vgId);
    vnodeRelease(pVnode);
    return TSDB_CODE_SUCCESS;
  } else {
    vDebug("vgId:%d, create vnode msg is received", pCreate->cfg.vgId);
    return vnodeCreate(pCreate);
  }
}

int32_t vnodeProcessAlterVnodeMsg(SRpcMsg *rpcMsg) {
  SAlterVnodeMsg *pAlter = vnodeParseVnodeMsg(rpcMsg);

  void *pVnode = vnodeAcquireNotClose(pAlter->cfg.vgId);
  if (pVnode != NULL) {
    vDebug("vgId:%d, alter vnode msg is received", pAlter->cfg.vgId);
    int32_t code = vnodeAlter(pVnode, pAlter);
    vnodeRelease(pVnode);
    return code;
  } else {
    vInfo("vgId:%d, vnode not exist, can't alter it", pAlter->cfg.vgId);
    return TSDB_CODE_VND_INVALID_VGROUP_ID;
  }
}

int32_t vnodeProcessSyncVnodeMsg(SRpcMsg *rpcMsg) {
  SSyncVnodeMsg *pSyncVnode = rpcMsg->pCont;
  pSyncVnode->vgId = htonl(pSyncVnode->vgId);

  return vnodeSync(pSyncVnode->vgId);
}

int32_t vnodeProcessCompactVnodeMsg(SRpcMsg *rpcMsg) {
  SCompactVnodeMsg *pCompactVnode = rpcMsg->pCont;
  pCompactVnode->vgId = htonl(pCompactVnode->vgId);
  return vnodeCompact(pCompactVnode->vgId);
}

int32_t vnodeProcessDropVnodeMsg(SRpcMsg *rpcMsg) {
  SDropVnodeMsg *pDrop = rpcMsg->pCont;
  pDrop->vgId = htonl(pDrop->vgId);

  return vnodeDrop(pDrop->vgId);
}

int32_t vnodeProcessAlterStreamReq(SRpcMsg *pMsg) { return 0; }
