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

static bool dmIsNodeRequired(SDnode *pDnode, EDndNodeType ntype) { return pDnode->wrappers[ntype].required; }

static bool dmRequireNode(SMgmtWrapper *pWrapper) {
  SMgmtInputOpt *pInput = &pWrapper->pDnode->input;
  pInput->name = pWrapper->name;
  pInput->path = pWrapper->path;

  bool    required = false;
  int32_t code = (*pWrapper->func.requiredFp)(pInput, &required);
  if (!required) {
    dDebug("node:%s, does not require startup", pWrapper->name);
  }

  if (pWrapper->ntype == DNODE && pWrapper->pDnode->rtype != DNODE && pWrapper->pDnode->rtype != NODE_END) {
    required = false;
    dDebug("node:%s, does not require startup in child process", pWrapper->name);
  }

  return required;
}

static int32_t dmInitVars(SDnode *pDnode, const SDnodeOpt *pOption) {
  pDnode->rtype = pOption->ntype;

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
    dInfo("dnode will run in child-process mode, node:%s", pWrapper->name);
  }

  pDnode->input.dnodeId = 0;
  pDnode->input.clusterId = 0;
  pDnode->input.localEp = strdup(pOption->localEp);
  pDnode->input.localFqdn = strdup(pOption->localFqdn);
  pDnode->input.firstEp = strdup(pOption->firstEp);
  pDnode->input.secondEp = strdup(pOption->secondEp);
  pDnode->input.serverPort = pOption->serverPort;
  pDnode->input.supportVnodes = pOption->numOfSupportVnodes;
  pDnode->input.numOfDisks = pOption->numOfDisks;
  pDnode->input.disks = pOption->disks;
  pDnode->input.dataDir = strdup(pOption->dataDir);
  pDnode->input.pDnode = pDnode;
  pDnode->input.processCreateNodeFp = dmProcessCreateNodeReq;
  pDnode->input.processDropNodeFp = dmProcessDropNodeReq;
  pDnode->input.isNodeRequiredFp = dmIsNodeRequired;

  if (pDnode->input.dataDir == NULL || pDnode->input.localEp == NULL || pDnode->input.localFqdn == NULL ||
      pDnode->input.firstEp == NULL || pDnode->input.secondEp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

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

  taosMemoryFreeClear(pDnode->input.localEp);
  taosMemoryFreeClear(pDnode->input.localFqdn);
  taosMemoryFreeClear(pDnode->input.firstEp);
  taosMemoryFreeClear(pDnode->input.secondEp);
  taosMemoryFreeClear(pDnode->input.dataDir);

  taosThreadMutexDestroy(&pDnode->mutex);
  memset(&pDnode->mutex, 0, sizeof(pDnode->mutex));
  taosMemoryFree(pDnode);
  dDebug("dnode memory is cleared, data:%p", pDnode);
}

SDnode *dmCreate(const SDnodeOpt *pOption) {
  dInfo("start to create dnode");
  int32_t code = -1;
  char    path[PATH_MAX + 100] = {0};
  SDnode *pDnode = NULL;

  pDnode = taosMemoryCalloc(1, sizeof(SDnode));
  if (pDnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (dmInitVars(pDnode, pOption) != 0) {
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

    snprintf(path, sizeof(path), "%s%s%s", pOption->dataDir, TD_DIRSEP, pWrapper->name);
    pWrapper->path = strdup(path);
    if (pWrapper->path == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    if (ntype != DNODE && dmReadShmFile(pWrapper->path, pWrapper->name, pDnode->rtype, &pWrapper->proc.shm) != 0) {
      dError("node:%s, failed to read shm file since %s", pWrapper->name, terrstr());
      goto _OVER;
    }

    pWrapper->required = dmRequireNode(pWrapper);
  }

  if (dmInitMsgHandle(pDnode) != 0) {
    dError("failed to init msg handles since %s", terrstr());
    goto _OVER;
  }

  if (dmInitClient(pDnode) != 0) {
    goto _OVER;
  }

  if (OnlyInSingleProc(pDnode->ptype) && InParentProc(pDnode->ptype)) {
    pDnode->lockfile = dmCheckRunning(pOption->dataDir);
    if (pDnode->lockfile == NULL) {
      goto _OVER;
    }

    if (dmInitServer(pDnode) != 0) {
      dError("failed to init transport since %s", terrstr());
      goto _OVER;
    }
  }

  dmReportStartup(pDnode, "dnode-transport", "initialized");
  dInfo("dnode is created, data:%p", pDnode);
  code = 0;

_OVER:
  if (code != 0 && pDnode != NULL) {
    dmClearVars(pDnode);
    pDnode = NULL;
    dError("failed to create dnode since %s", terrstr());
  }

  return pDnode;
}

void dmClose(SDnode *pDnode) {
  if (pDnode == NULL) return;

  dmCleanupClient(pDnode);
  dmCleanupServer(pDnode);

  dmClearVars(pDnode);
  dInfo("dnode is closed, data:%p", pDnode);
}

void dmSetStatus(SDnode *pDnode, EDndRunStatus status) {
  if (pDnode->status != status) {
    dDebug("dnode status set from %s to %s", dmStatStr(pDnode->status), dmStatStr(status));
    pDnode->status = status;
  }
}

void dmSetEvent(SDnode *pDnode, EDndEvent event) {
  if (event == DND_EVENT_STOP) {
    pDnode->event = event;
  }
}

SMgmtWrapper *dmAcquireWrapper(SDnode *pDnode, EDndNodeType ntype) {
  SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
  SMgmtWrapper *pRetWrapper = pWrapper;

  taosRLockLatch(&pWrapper->latch);
  if (pWrapper->deployed) {
    int32_t refCount = atomic_add_fetch_32(&pWrapper->refCount, 1);
    dTrace("node:%s, is acquired, refCount:%d", pWrapper->name, refCount);
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
  if (pWrapper->deployed || (InParentProc(pWrapper->proc.ptype) && pWrapper->required)) {
    int32_t refCount = atomic_add_fetch_32(&pWrapper->refCount, 1);
    dTrace("node:%s, is marked, refCount:%d", pWrapper->name, refCount);
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
  dTrace("node:%s, is released, refCount:%d", pWrapper->name, refCount);
}

void dmReportStartup(SDnode *pDnode, const char *pName, const char *pDesc) {
  SStartupInfo *pStartup = &pDnode->startup;
  tstrncpy(pStartup->name, pName, TSDB_STEP_NAME_LEN);
  tstrncpy(pStartup->desc, pDesc, TSDB_STEP_DESC_LEN);
  dInfo("step:%s, %s", pStartup->name, pStartup->desc);
}

void dmReportStartupByWrapper(SMgmtWrapper *pWrapper, const char *pName, const char *pDesc) {
  dmReportStartup(pWrapper->pDnode, pName, pDesc);
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

void dmProcessNetTestReq(SDnode *pDnode, SRpcMsg *pReq) {
  dDebug("net test req is received");
  SRpcMsg rsp = {.handle = pReq->handle, .refId = pReq->refId, .ahandle = pReq->ahandle, .code = 0};
  rsp.pCont = rpcMallocCont(pReq->contLen);
  if (rsp.pCont == NULL) {
    rsp.code = TSDB_CODE_OUT_OF_MEMORY;
  } else {
    rsp.contLen = pReq->contLen;
  }
  rpcSendResponse(&rsp);
  rpcFreeCont(pReq->pCont);
}

void dmProcessServerStartupStatus(SDnode *pDnode, SRpcMsg *pReq) {
  dDebug("server startup status req is received");

  SServerStatusRsp statusRsp = {0};
  dmGetServerStartupStatus(pDnode, &statusRsp);

  SRpcMsg rspMsg = {.handle = pReq->handle, .ahandle = pReq->ahandle, .refId = pReq->refId};
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
  rpcFreeCont(pReq->pCont);
}

int32_t dmProcessCreateNodeReq(SDnode *pDnode, EDndNodeType ntype, SNodeMsg *pMsg) {
  SMgmtWrapper *pWrapper = dmAcquireWrapper(pDnode, ntype);
  if (pWrapper != NULL) {
    dmReleaseWrapper(pWrapper);
    terrno = TSDB_CODE_NODE_ALREADY_DEPLOYED;
    dError("failed to create node since %s", terrstr());
    return -1;
  }

  taosThreadMutexLock(&pDnode->mutex);
  pWrapper = &pDnode->wrappers[ntype];

  if (taosMkDir(pWrapper->path) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to create dir:%s since %s", pWrapper->path, terrstr());
    return -1;
  }

  SMgmtInputOpt *pInput = &pWrapper->pDnode->input;
  pInput->name = pWrapper->name;
  pInput->path = pWrapper->path;
  pInput->msgCb = dmGetMsgcb(pWrapper);

  int32_t code = (*pWrapper->func.createFp)(pInput, pMsg);
  if (code != 0) {
    dError("node:%s, failed to create since %s", pWrapper->name, terrstr());
  } else {
    dDebug("node:%s, has been created", pWrapper->name);
    (void)dmOpenNode(pWrapper);
    pWrapper->required = true;
    pWrapper->deployed = true;
    pWrapper->proc.ptype = pDnode->ptype;
  }

  taosThreadMutexUnlock(&pDnode->mutex);
  return code;
}

int32_t dmProcessDropNodeReq(SDnode *pDnode, EDndNodeType ntype, SNodeMsg *pMsg) {
  SMgmtWrapper *pWrapper = dmAcquireWrapper(pDnode, ntype);
  if (pWrapper == NULL) {
    terrno = TSDB_CODE_NODE_NOT_DEPLOYED;
    dError("failed to drop node since %s", terrstr());
    return -1;
  }

  taosThreadMutexLock(&pDnode->mutex);

  int32_t code = (*pWrapper->func.dropFp)(pWrapper->pMgmt, pMsg);
  if (code != 0) {
    dError("node:%s, failed to drop since %s", pWrapper->name, terrstr());
  } else {
    dDebug("node:%s, has been dropped", pWrapper->name);
    pWrapper->required = false;
    pWrapper->deployed = false;
  }

  dmReleaseWrapper(pWrapper);

  if (code == 0) {
    dmCloseNode(pWrapper);
    taosRemoveDir(pWrapper->path);
  }
  taosThreadMutexUnlock(&pDnode->mutex);
  return code;
}