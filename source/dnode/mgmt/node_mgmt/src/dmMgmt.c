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
#include "dmNodes.h"
#include "index.h"
#include "qworker.h"
#include "tstream.h"
#ifdef TD_TSZ
#include "tcompression.h"
#include "tglobal.h"
#include "tgrant.h"
#endif

static bool dmRequireNode(SDnode *pDnode, SMgmtWrapper *pWrapper) {
  SMgmtInputOpt input = dmBuildMgmtInputOpt(pWrapper);

  bool    required = false;
  int32_t code = (*pWrapper->func.requiredFp)(&input, &required);
  if (!required) {
    dDebug("node:%s, does not require startup", pWrapper->name);
  } else {
    dDebug("node:%s, required to startup", pWrapper->name);
  }

  return required;
}

int32_t dmInitDnode(SDnode *pDnode) {
  dDebug("start to create dnode");
  int32_t code = -1;
  char    path[PATH_MAX + 100] = {0};

  if (dmInitVars(pDnode) != 0) {
    goto _OVER;
  }

#ifdef TD_TSZ
  // compress module init
  tsCompressInit(tsLossyColumns, tsFPrecision, tsDPrecision, tsMaxRange, tsCurRange, (int)tsIfAdtFse, tsCompressor);
#endif

  pDnode->wrappers[DNODE].func = dmGetMgmtFunc();
  pDnode->wrappers[MNODE].func = mmGetMgmtFunc();
  pDnode->wrappers[VNODE].func = vmGetMgmtFunc();
  pDnode->wrappers[QNODE].func = qmGetMgmtFunc();
  pDnode->wrappers[SNODE].func = smGetMgmtFunc();

  for (EDndNodeType ntype = DNODE; ntype < NODE_END; ++ntype) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
    pWrapper->pDnode = pDnode;
    pWrapper->name = dmNodeName(ntype);
    pWrapper->ntype = ntype;
    taosThreadRwlockInit(&pWrapper->lock, NULL);

    snprintf(path, sizeof(path), "%s%s%s", tsDataDir, TD_DIRSEP, pWrapper->name);
    pWrapper->path = taosStrdup(path);
    if (pWrapper->path == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    pWrapper->required = dmRequireNode(pDnode, pWrapper);
  }

  pDnode->lockfile = dmCheckRunning(tsDataDir);
  if (pDnode->lockfile == NULL) {
    goto _OVER;
  }
  if (dmInitModule(pDnode) != 0) {
    goto _OVER;
  }

  indexInit(tsNumOfCommitThreads);
  streamMetaInit();

  dmInitStatusClient(pDnode);
  dmInitSyncClient(pDnode);  

  dmReportStartup("dnode-transport", "initialized");
  dDebug("dnode is created, ptr:%p", pDnode);
  code = 0;

_OVER:
  if (code != 0 && pDnode != NULL) {
    dmClearVars(pDnode);
    pDnode = NULL;
    dError("failed to create dnode since %s", terrstr());
  }

  return code;
}

void dmCleanupDnode(SDnode *pDnode) {
  if (pDnode == NULL) {
    return;
  }

  dmCleanupClient(pDnode);
  dmCleanupStatusClient(pDnode);
  dmCleanupSyncClient(pDnode);
  dmCleanupServer(pDnode);

  dmClearVars(pDnode);
  rpcCleanup();
  streamMetaCleanup();
  indexCleanup();
  taosConvDestroy();

#ifdef TD_TSZ
  // compress destroy
  tsCompressExit();
#endif

  dDebug("dnode is closed, ptr:%p", pDnode);
}

int32_t dmInitVars(SDnode *pDnode) {
  SDnodeData *pData = &pDnode->data;
  pData->dnodeId = 0;
  pData->clusterId = 0;
  pData->dnodeVer = 0;
  pData->engineVer = 0;
  pData->updateTime = 0;
  pData->rebootTime = taosGetTimestampMs();
  pData->dropped = 0;
  pData->stopped = 0;
  char *machineId = tGetMachineId();
  if (machineId) {
    tstrncpy(pData->machineId, machineId, TSDB_MACHINE_ID_LEN + 1);
    taosMemoryFreeClear(machineId);
  } else {
#if defined(TD_ENTERPRISE) && !defined(GRANTS_CFG)
    terrno = TSDB_CODE_DNODE_NO_MACHINE_CODE;
    return -1;
#endif
  }

  pData->dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pData->dnodeHash == NULL) {
    dError("failed to init dnode hash");
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dmReadEps(pData) != 0) {
    dError("failed to read file since %s", terrstr());
    return -1;
  }

  if (pData->dropped) {
    dError("dnode will not start since its already dropped");
    return -1;
  }

  taosThreadRwlockInit(&pData->lock, NULL);
  taosThreadMutexInit(&pDnode->mutex, NULL);
  return 0;
}

void dmClearVars(SDnode *pDnode) {
  for (EDndNodeType ntype = DNODE; ntype < NODE_END; ++ntype) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
    taosMemoryFreeClear(pWrapper->path);
    taosThreadRwlockDestroy(&pWrapper->lock);
  }
  if (pDnode->lockfile != NULL) {
    taosUnLockFile(pDnode->lockfile);
    taosCloseFile(&pDnode->lockfile);
    pDnode->lockfile = NULL;
  }

  SDnodeData *pData = &pDnode->data;
  taosThreadRwlockWrlock(&pData->lock);
  if (pData->oldDnodeEps != NULL) {
    if (dmWriteEps(pData) == 0) {
      dmRemoveDnodePairs(pData);
    }
    taosArrayDestroy(pData->oldDnodeEps);
    pData->oldDnodeEps = NULL;
  }
  if (pData->dnodeEps != NULL) {
    taosArrayDestroy(pData->dnodeEps);
    pData->dnodeEps = NULL;
  }
  if (pData->dnodeHash != NULL) {
    taosHashCleanup(pData->dnodeHash);
    pData->dnodeHash = NULL;
  }
  taosThreadRwlockUnlock(&pData->lock);

  taosThreadRwlockDestroy(&pData->lock);
  taosThreadMutexDestroy(&pDnode->mutex);
  memset(&pDnode->mutex, 0, sizeof(pDnode->mutex));
}

void dmSetStatus(SDnode *pDnode, EDndRunStatus status) {
  if (pDnode->status != status) {
    dDebug("dnode status set from %s to %s", dmStatStr(pDnode->status), dmStatStr(status));
    pDnode->status = status;
  }
}

SMgmtWrapper *dmAcquireWrapper(SDnode *pDnode, EDndNodeType ntype) {
  SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
  SMgmtWrapper *pRetWrapper = pWrapper;

  taosThreadRwlockRdlock(&pWrapper->lock);
  if (pWrapper->deployed) {
    int32_t refCount = atomic_add_fetch_32(&pWrapper->refCount, 1);
    // dTrace("node:%s, is acquired, ref:%d", pWrapper->name, refCount);
  } else {
    pRetWrapper = NULL;
  }
  taosThreadRwlockUnlock(&pWrapper->lock);

  return pRetWrapper;
}

int32_t dmMarkWrapper(SMgmtWrapper *pWrapper) {
  int32_t code = 0;

  taosThreadRwlockRdlock(&pWrapper->lock);
  if (pWrapper->deployed) {
    int32_t refCount = atomic_add_fetch_32(&pWrapper->refCount, 1);
    // dTrace("node:%s, is marked, ref:%d", pWrapper->name, refCount);
  } else {
    switch (pWrapper->ntype) {
      case MNODE:
        terrno = TSDB_CODE_MNODE_NOT_FOUND;
        break;
      case QNODE:
        terrno = TSDB_CODE_QNODE_NOT_FOUND;
        break;
      case SNODE:
        terrno = TSDB_CODE_SNODE_NOT_FOUND;
        break;
      case VNODE:
        terrno = TSDB_CODE_VND_STOPPED;
        break;
      default:
        terrno = TSDB_CODE_APP_IS_STOPPING;
        break;
    }
    code = -1;
  }
  taosThreadRwlockUnlock(&pWrapper->lock);

  return code;
}

void dmReleaseWrapper(SMgmtWrapper *pWrapper) {
  if (pWrapper == NULL) return;

  taosThreadRwlockRdlock(&pWrapper->lock);
  int32_t refCount = atomic_sub_fetch_32(&pWrapper->refCount, 1);
  taosThreadRwlockUnlock(&pWrapper->lock);
  // dTrace("node:%s, is released, ref:%d", pWrapper->name, refCount);
}

static void dmGetServerStartupStatus(SDnode *pDnode, SServerStatusRsp *pStatus) {
  SDnodeMgmt *pMgmt = pDnode->wrappers[DNODE].pMgmt;
  pStatus->details[0] = 0;

  if (pDnode->status == DND_STAT_INIT) {
    pStatus->statusCode = TSDB_SRV_STATUS_NETWORK_OK;
    snprintf(pStatus->details, sizeof(pStatus->details), "%s: %s", pDnode->startup.name, pDnode->startup.desc);
  } else if (pDnode->status == DND_STAT_STOPPED) {
    pStatus->statusCode = TSDB_SRV_STATUS_EXTING;
  } else {
    pStatus->statusCode = TSDB_SRV_STATUS_SERVICE_OK;
  }
}

void dmProcessNetTestReq(SDnode *pDnode, SRpcMsg *pMsg) {
  dDebug("msg:%p, net test req will be processed", pMsg);

  SRpcMsg rsp = {.info = pMsg->info};
  rsp.pCont = rpcMallocCont(pMsg->contLen);
  if (rsp.pCont == NULL) {
    rsp.code = TSDB_CODE_OUT_OF_MEMORY;
  } else {
    rsp.contLen = pMsg->contLen;
  }

  rpcSendResponse(&rsp);
  rpcFreeCont(pMsg->pCont);
}

void dmProcessServerStartupStatus(SDnode *pDnode, SRpcMsg *pMsg) {
  dDebug("msg:%p, server startup status req will be processed", pMsg);

  SServerStatusRsp statusRsp = {0};
  dmGetServerStartupStatus(pDnode, &statusRsp);

  SRpcMsg rsp = {.info = pMsg->info};
  int32_t contLen = tSerializeSServerStatusRsp(NULL, 0, &statusRsp);
  if (contLen < 0) {
    rsp.code = TSDB_CODE_OUT_OF_MEMORY;
  } else {
    rsp.pCont = rpcMallocCont(contLen);
    if (rsp.pCont != NULL) {
      tSerializeSServerStatusRsp(rsp.pCont, contLen, &statusRsp);
      rsp.contLen = contLen;
    }
  }

  rpcSendResponse(&rsp);
  rpcFreeCont(pMsg->pCont);
}
