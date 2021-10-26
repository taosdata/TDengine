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
#include "vnodeMgmt.h"

static struct {
  SWorkerPool createPool;
  taos_queue  createQueue;
  SWorkerPool workerPool;
  taos_queue  workerQueue;
  int32_t (*msgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);
} tsVmgmt = {0};

static int32_t vnodeParseCreateVnodeReq(SRpcMsg *rpcMsg, int32_t *vgId, SVnodeCfg *pCfg) {
  SCreateVnodeMsg *pCreate = rpcMsg->pCont;

  *vgId = htonl(pCreate->vgId);

  pCfg->dropped = 0;
  tstrncpy(pCfg->db, pCreate->db, sizeof(pCfg->db));

  pCfg->tsdb.cacheBlockSize = htonl(pCreate->cacheBlockSize);
  pCfg->tsdb.totalBlocks = htonl(pCreate->totalBlocks);
  pCfg->tsdb.daysPerFile = htonl(pCreate->daysPerFile);
  pCfg->tsdb.daysToKeep1 = htonl(pCreate->daysToKeep1);
  pCfg->tsdb.daysToKeep2 = htonl(pCreate->daysToKeep2);
  pCfg->tsdb.daysToKeep0 = htonl(pCreate->daysToKeep0);
  pCfg->tsdb.minRowsPerFileBlock = htonl(pCreate->minRowsPerFileBlock);
  pCfg->tsdb.maxRowsPerFileBlock = htonl(pCreate->maxRowsPerFileBlock);
  pCfg->tsdb.precision = pCreate->precision;
  pCfg->tsdb.compression = pCreate->compression;
  pCfg->tsdb.cacheLastRow = pCreate->cacheLastRow;
  pCfg->tsdb.update = pCreate->update;

  pCfg->wal.fsyncPeriod = htonl(pCreate->fsyncPeriod);
  pCfg->wal.walLevel = pCreate->walLevel;

  pCfg->sync.replica = pCreate->replica;
  pCfg->sync.quorum = pCreate->quorum;

  for (int32_t j = 0; j < pCreate->replica; ++j) {
    pCfg->sync.nodes[j].nodePort = htons(pCreate->nodes[j].port);
    tstrncpy(pCfg->sync.nodes[j].nodeFqdn, pCreate->nodes[j].fqdn, TSDB_FQDN_LEN);
  }

  return 0;
}

static int32_t vnodeProcessCreateVnodeReq(SRpcMsg *rpcMsg) {
  SVnodeCfg vnodeCfg = {0};
  int32_t   vgId = 0;

  int32_t code = vnodeParseCreateVnodeReq(rpcMsg, &vgId, &vnodeCfg);
  if (code != 0) {
    vError("failed to parse create vnode msg since %s", tstrerror(code));
  }

  vDebug("vgId:%d, create vnode req is received", vgId);

  SVnode *pVnode = vnodeAcquireInAllState(vgId);
  if (pVnode != NULL) {
    vDebug("vgId:%d, already exist, return success", vgId);
    vnodeRelease(pVnode);
    return code;
  }

  code = vnodeCreateVnode(vgId, &vnodeCfg);
  if (code != 0) {
    vError("vgId:%d, failed to create vnode since %s", vgId, tstrerror(code));
  }

  return code;
}

static int32_t vnodeProcessAlterVnodeReq(SRpcMsg *rpcMsg) {
  SVnodeCfg vnodeCfg = {0};
  int32_t   vgId = 0;

  int32_t code = vnodeParseCreateVnodeReq(rpcMsg, &vgId, &vnodeCfg);
  if (code != 0) {
    vError("failed to parse create vnode msg since %s", tstrerror(code));
  }

  vDebug("vgId:%d, alter vnode req is received", vgId);

  SVnode *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    code = terrno;
    vDebug("vgId:%d, failed to alter vnode since %s", vgId, tstrerror(code));
    return code;
  }

  code = vnodeAlterVnode(pVnode, &vnodeCfg);
  if (code != 0) {
    vError("vgId:%d, failed to alter vnode since %s", vgId, tstrerror(code));
  }

  vnodeRelease(pVnode);
  return code;
}

static SDropVnodeMsg *vnodeParseDropVnodeReq(SRpcMsg *rpcMsg) {
  SDropVnodeMsg *pDrop = rpcMsg->pCont;
  pDrop->vgId = htonl(pDrop->vgId);
  return pDrop;
}

static int32_t vnodeProcessSyncVnodeReq(SRpcMsg *rpcMsg) {
  SSyncVnodeMsg *pSync = (SSyncVnodeMsg *)vnodeParseDropVnodeReq(rpcMsg);

  int32_t code = 0;
  int32_t vgId = pSync->vgId;
  vDebug("vgId:%d, sync vnode req is received", vgId);

  SVnode *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    code = terrno;
    vDebug("vgId:%d, failed to sync since %s", vgId, tstrerror(code));
    return code;
  }

  code = vnodeSyncVnode(pVnode);
  if (code != 0) {
    vError("vgId:%d, failed to compact vnode since %s", vgId, tstrerror(code));
  }

  vnodeRelease(pVnode);
  return code;
}

static int32_t vnodeProcessCompactVnodeReq(SRpcMsg *rpcMsg) {
  SCompactVnodeMsg *pCompact = (SCompactVnodeMsg *)vnodeParseDropVnodeReq(rpcMsg);

  int32_t code = 0;
  int32_t vgId = pCompact->vgId;
  vDebug("vgId:%d, compact vnode req is received", vgId);

  SVnode *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    code = terrno;
    vDebug("vgId:%d, failed to compact since %s", vgId, tstrerror(code));
    return code;
  }

  code = vnodeCompactVnode(pVnode);
  if (code != 0) {
    vError("vgId:%d, failed to compact vnode since %s", vgId, tstrerror(code));
  }

  vnodeRelease(pVnode);
  return code;
}

static int32_t vnodeProcessDropVnodeReq(SRpcMsg *rpcMsg) {
  SDropVnodeMsg *pDrop = vnodeParseDropVnodeReq(rpcMsg);

  int32_t code = 0;
  int32_t vgId = pDrop->vgId;
  vDebug("vgId:%d, drop vnode req is received", vgId);

  SVnode *pVnode = vnodeAcquire(vgId);
  if (pVnode == NULL) {
    code = terrno;
    vDebug("vgId:%d, failed to drop since %s", vgId, tstrerror(code));
    return code;
  }

  code = vnodeDropVnode(pVnode);
  if (code != 0) {
    vError("vgId:%d, failed to drop vnode since %s", vgId, tstrerror(code));
  }

  vnodeRelease(pVnode);
  return code;
}

static int32_t vnodeProcessAlterStreamReq(SRpcMsg *pMsg) {
  vError("alter stream msg not processed");
  return TSDB_CODE_VND_MSG_NOT_PROCESSED;
}

static int32_t vnodeProcessMgmtStart(void *unused, SVnMgmtMsg *pMgmt, int32_t qtype) {
  SRpcMsg *pMsg = &pMgmt->rpcMsg;
  int32_t  msgType = pMsg->msgType;

  if (tsVmgmt.msgFp[msgType]) {
    vTrace("msg:%p, ahandle:%p type:%s will be processed", pMgmt, pMsg->ahandle, taosMsg[msgType]);
    return (*tsVmgmt.msgFp[msgType])(pMsg);
  } else {
    vError("msg:%p, ahandle:%p type:%s not processed since no handle", pMgmt, pMsg->ahandle, taosMsg[msgType]);
    return TSDB_CODE_DND_MSG_NOT_PROCESSED;
  }
}

static void vnodeProcessMgmtEnd(void *unused, SVnMgmtMsg *pMgmt, int32_t qtype, int32_t code) {
  SRpcMsg *pMsg = &pMgmt->rpcMsg;
  vTrace("msg:%p, is processed, result:%s", pMgmt, tstrerror(code));

  SRpcMsg rsp = {.code = code, .handle = pMsg->handle};
  rpcSendResponse(&rsp);
  taosFreeQitem(pMgmt);
}

static void vnodeInitMgmtReqFp() {
  tsVmgmt.msgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = vnodeProcessCreateVnodeReq;
  tsVmgmt.msgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE] = vnodeProcessAlterVnodeReq;
  tsVmgmt.msgFp[TSDB_MSG_TYPE_MD_SYNC_VNODE] = vnodeProcessSyncVnodeReq;
  tsVmgmt.msgFp[TSDB_MSG_TYPE_MD_COMPACT_VNODE] = vnodeProcessCompactVnodeReq;
  tsVmgmt.msgFp[TSDB_MSG_TYPE_MD_DROP_VNODE] = vnodeProcessDropVnodeReq;
  tsVmgmt.msgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = vnodeProcessAlterStreamReq;
}

static int32_t vnodeWriteToMgmtQueue(SRpcMsg *pMsg) {
  int32_t     size = sizeof(SVnMgmtMsg) + pMsg->contLen;
  SVnMgmtMsg *pMgmt = taosAllocateQitem(size);
  if (pMgmt == NULL) return TSDB_CODE_DND_OUT_OF_MEMORY;

  pMgmt->rpcMsg = *pMsg;
  pMgmt->rpcMsg.pCont = pMgmt->pCont;
  memcpy(pMgmt->pCont, pMsg->pCont, pMsg->contLen);

  if (pMsg->msgType == TSDB_MSG_TYPE_MD_CREATE_VNODE) {
    return taosWriteQitem(tsVmgmt.createQueue, TAOS_QTYPE_RPC, pMgmt);
  } else {
    return taosWriteQitem(tsVmgmt.workerQueue, TAOS_QTYPE_RPC, pMgmt);
  }
}

void vnodeProcessMgmtMsg(SRpcMsg *pMsg) {
  int32_t code = vnodeWriteToMgmtQueue(pMsg);
  if (code != TSDB_CODE_SUCCESS) {
    vError("msg, ahandle:%p type:%s not processed since %s", pMsg->ahandle, taosMsg[pMsg->msgType], tstrerror(code));
    SRpcMsg rsp = {.handle = pMsg->handle, .code = code};
    rpcSendResponse(&rsp);
  }

  rpcFreeCont(pMsg->pCont);
}

int32_t vnodeInitMgmt() {
  vnodeInitMgmtReqFp();

  SWorkerPool *pPool = &tsVmgmt.createPool;
  pPool->name = "vnode-mgmt-create";
  pPool->startFp = (ProcessStartFp)vnodeProcessMgmtStart;
  pPool->endFp = (ProcessEndFp)vnodeProcessMgmtEnd;
  pPool->min = 1;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  tsVmgmt.createQueue = tWorkerAllocQueue(pPool, NULL);

  pPool = &tsVmgmt.workerPool;
  pPool->name = "vnode-mgmt-worker";
  pPool->startFp = (ProcessStartFp)vnodeProcessMgmtStart;
  pPool->endFp = (ProcessEndFp)vnodeProcessMgmtEnd;
  pPool->min = 1;
  pPool->max = 1;
  if (tWorkerInit(pPool) != 0) {
    return TSDB_CODE_VND_OUT_OF_MEMORY;
  }

  tsVmgmt.workerQueue = tWorkerAllocQueue(pPool, NULL);

  vInfo("vmgmt is initialized");
  return TSDB_CODE_SUCCESS;
}

void vnodeCleanupMgmt() {
  tWorkerFreeQueue(&tsVmgmt.createPool, tsVmgmt.createQueue);
  tWorkerCleanup(&tsVmgmt.createPool);
  tsVmgmt.createQueue = NULL;

  tWorkerFreeQueue(&tsVmgmt.workerPool, tsVmgmt.workerQueue);
  tWorkerCleanup(&tsVmgmt.workerPool);
  tsVmgmt.createQueue = NULL;
  vInfo("vmgmt is closed");
}
