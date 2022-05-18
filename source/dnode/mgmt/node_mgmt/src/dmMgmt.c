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

static bool dmRequireNode(SDnode *pDnode, SMgmtWrapper *pWrapper) {
  SMgmtInputOpt input = dmBuildMgmtInputOpt(pWrapper);

  bool    required = false;
  int32_t code = (*pWrapper->func.requiredFp)(&input, &required);
  if (!required) {
    dDebug("node:%s, does not require startup", pWrapper->name);
  }

  if (pWrapper->ntype == DNODE) {
    if (pDnode->rtype != DNODE && pDnode->rtype != NODE_END) {
      required = false;
      dDebug("node:%s, does not require startup in child process", pWrapper->name);
    }
  } else {
    if (OnlyInChildProc(pWrapper)) {
      if (pWrapper->ntype != pDnode->rtype) {
        dDebug("node:%s, does not require startup in child process", pWrapper->name);
        required = false;
      }
    }
  }

  if (required) {
    dDebug("node:%s, required to startup", pWrapper->name);
  }

  return required;
}

static int32_t dmInitVars(SDnode *pDnode, EDndNodeType rtype) {
  pDnode->rtype = rtype;

  if (tsMultiProcess == 0) {
    pDnode->ptype = DND_PROC_SINGLE;
    dInfo("dnode will run in single-process mode");
  } else if (tsMultiProcess > 1) {
    pDnode->ptype = DND_PROC_TEST;
    dInfo("dnode will run in multi-process test mode");
  } else if (pDnode->rtype == DNODE || pDnode->rtype == NODE_END) {
    pDnode->ptype = DND_PROC_PARENT;
    dInfo("dnode will run in parent-process mode");
  } else {
    pDnode->ptype = DND_PROC_CHILD;
    SMgmtWrapper *pWrapper = &pDnode->wrappers[pDnode->rtype];
    dInfo("dnode will run in child-process mode, node:%s", dmNodeName(pDnode->rtype));
  }

  SDnodeData *pData = &pDnode->data;
  pData->dnodeId = 0;
  pData->clusterId = 0;
  pData->dnodeVer = 0;
  pData->updateTime = 0;
  pData->rebootTime = taosGetTimestampMs();
  pData->dropped = 0;
  pData->stopped = 0;

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

  taosInitRWLatch(&pData->latch);
  taosThreadMutexInit(&pDnode->mutex, NULL);
  return 0;
}

static void dmClearVars(SDnode *pDnode) {
  for (EDndNodeType ntype = DNODE; ntype < NODE_END; ++ntype) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
    taosMemoryFreeClear(pWrapper->path);
  }
  if (pDnode->lockfile != NULL) {
    taosUnLockFile(pDnode->lockfile);
    taosCloseFile(&pDnode->lockfile);
    pDnode->lockfile = NULL;
  }

  SDnodeData *pData = &pDnode->data;
  taosWLockLatch(&pData->latch);
  if (pData->dnodeEps != NULL) {
    taosArrayDestroy(pData->dnodeEps);
    pData->dnodeEps = NULL;
  }
  if (pData->dnodeHash != NULL) {
    taosHashCleanup(pData->dnodeHash);
    pData->dnodeHash = NULL;
  }
  taosWUnLockLatch(&pData->latch);

  taosThreadMutexDestroy(&pDnode->mutex);
  memset(&pDnode->mutex, 0, sizeof(pDnode->mutex));
}

int32_t dmInitDnode(SDnode *pDnode, EDndNodeType rtype) {
  dInfo("start to create dnode");
  int32_t code = -1;
  char    path[PATH_MAX + 100] = {0};

  if (dmInitVars(pDnode, rtype) != 0) {
    goto _OVER;
  }

  pDnode->wrappers[DNODE].func = dmGetMgmtFunc();
  pDnode->wrappers[MNODE].func = mmGetMgmtFunc();
  pDnode->wrappers[VNODE].func = vmGetMgmtFunc();
  pDnode->wrappers[QNODE].func = qmGetMgmtFunc();
  pDnode->wrappers[SNODE].func = smGetMgmtFunc();
  pDnode->wrappers[BNODE].func = bmGetMgmtFunc();

  for (EDndNodeType ntype = DNODE; ntype < NODE_END; ++ntype) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
    pWrapper->pDnode = pDnode;
    pWrapper->name = dmNodeName(ntype);
    pWrapper->ntype = ntype;
    pWrapper->proc.wrapper = pWrapper;
    pWrapper->proc.shm.id = -1;
    pWrapper->proc.pid = -1;
    pWrapper->proc.ptype = pDnode->ptype;
    if (ntype == DNODE) {
      pWrapper->proc.ptype = DND_PROC_SINGLE;
    }
    taosInitRWLatch(&pWrapper->latch);

    snprintf(path, sizeof(path), "%s%s%s", tsDataDir, TD_DIRSEP, pWrapper->name);
    pWrapper->path = strdup(path);
    if (pWrapper->path == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    pWrapper->required = dmRequireNode(pDnode, pWrapper);

    if (ntype != DNODE && dmReadShmFile(pWrapper->path, pWrapper->name, pDnode->rtype, &pWrapper->proc.shm) != 0) {
      dError("node:%s, failed to read shm file since %s", pWrapper->name, terrstr());
      goto _OVER;
    }
  }

  if (dmInitMsgHandle(pDnode) != 0) {
    dError("failed to init msg handles since %s", terrstr());
    goto _OVER;
  }

  if (pDnode->ptype == SINGLE_PROC || (pDnode->ptype & PARENT_PROC)) {
    pDnode->lockfile = dmCheckRunning(tsDataDir);
    if (pDnode->lockfile == NULL) {
      goto _OVER;
    }

    if (dmInitServer(pDnode) != 0) {
      dError("failed to init transport since %s", terrstr());
      goto _OVER;
    }
  }

  if (dmInitClient(pDnode) != 0) {
    goto _OVER;
  }

  dmReportStartup("dnode-transport", "initialized");
  dInfo("dnode is created, ptr:%p", pDnode);
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
  if (pDnode == NULL) return;

  dmCleanupClient(pDnode);
  dmCleanupServer(pDnode);
  dmClearVars(pDnode);
  dInfo("dnode is closed, ptr:%p", pDnode);
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

  taosRLockLatch(&pWrapper->latch);
  if (pWrapper->deployed) {
    int32_t refCount = atomic_add_fetch_32(&pWrapper->refCount, 1);
    dTrace("node:%s, is acquired, ref:%d", pWrapper->name, refCount);
  } else {
    terrno = TSDB_CODE_NODE_NOT_DEPLOYED;
    pRetWrapper = NULL;
  }
  taosRUnLockLatch(&pWrapper->latch);

  return pRetWrapper;
}

int32_t dmMarkWrapper(SMgmtWrapper *pWrapper) {
  int32_t code = 0;

  taosRLockLatch(&pWrapper->latch);
  if (pWrapper->deployed || (InParentProc(pWrapper) && pWrapper->required)) {
    int32_t refCount = atomic_add_fetch_32(&pWrapper->refCount, 1);
    dTrace("node:%s, is marked, ref:%d", pWrapper->name, refCount);
  } else {
    terrno = TSDB_CODE_NODE_NOT_DEPLOYED;
    code = -1;
  }
  taosRUnLockLatch(&pWrapper->latch);

  return code;
}

void dmReleaseWrapper(SMgmtWrapper *pWrapper) {
  if (pWrapper == NULL) return;

  taosRLockLatch(&pWrapper->latch);
  int32_t refCount = atomic_sub_fetch_32(&pWrapper->refCount, 1);
  taosRUnLockLatch(&pWrapper->latch);
  dTrace("node:%s, is released, ref:%d", pWrapper->name, refCount);
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
  SRpcMsg rsp = {.code = 0, .info = pMsg->info};
  rsp.pCont = rpcMallocCont(pMsg->contLen);
  if (rsp.pCont == NULL) {
    rsp.code = TSDB_CODE_OUT_OF_MEMORY;
  } else {
    rsp.contLen = pMsg->contLen;
  }
  rpcSendResponse(&rsp);
}

void dmProcessServerStartupStatus(SDnode *pDnode, SRpcMsg *pMsg) {
  dDebug("msg:%p, server startup status req will be processed", pMsg);
  SServerStatusRsp statusRsp = {0};
  dmGetServerStartupStatus(pDnode, &statusRsp);

  SRpcMsg rspMsg = {.info = pMsg->info};
  int32_t rspLen = tSerializeSServerStatusRsp(NULL, 0, &statusRsp);
  if (rspLen < 0) {
    rspMsg.code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    rspMsg.code = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  tSerializeSServerStatusRsp(pRsp, rspLen, &statusRsp);
  rspMsg.pCont = pRsp;
  rspMsg.contLen = rspLen;

_OVER:
  rpcSendResponse(&rspMsg);
}
