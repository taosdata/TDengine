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
#include "dmFile.h"
#include "dmMsg.h"
#include "dmWorker.h"

int32_t dmGetDnodeId(SDnode *pDnode) {
  SMgmtWrapper *pWrapper = dndGetWrapper(pDnode, DNODE);
  SDnodeMgmt   *pMgmt = pWrapper->pMgmt;

  taosRLockLatch(&pMgmt->latch);
  int32_t dnodeId = pMgmt->dnodeId;
  taosRUnLockLatch(&pMgmt->latch);
  return dnodeId;
}

int64_t dmGetClusterId(SDnode *pDnode) {
  SMgmtWrapper *pWrapper = dndGetWrapper(pDnode, DNODE);
  SDnodeMgmt   *pMgmt = pWrapper->pMgmt;

  taosRLockLatch(&pMgmt->latch);
  int64_t clusterId = pMgmt->clusterId;
  taosRUnLockLatch(&pMgmt->latch);
  return clusterId;
}

void dmGetMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet) {
  SMgmtWrapper *pWrapper = dndGetWrapper(pDnode, DNODE);
  SDnodeMgmt   *pMgmt = pWrapper->pMgmt;

  taosRLockLatch(&pMgmt->latch);
  *pEpSet = pMgmt->mnodeEpSet;
  taosRUnLockLatch(&pMgmt->latch);
}

void dmUpdateMnodeEpSet(SDnode *pDnode, SEpSet *pEpSet) {
  dInfo("mnode is changed, num:%d use:%d", pEpSet->numOfEps, pEpSet->inUse);

  SMgmtWrapper *pWrapper = dndGetWrapper(pDnode, DNODE);
  SDnodeMgmt   *pMgmt = pWrapper->pMgmt;

  taosWLockLatch(&pMgmt->latch);

  pMgmt->mnodeEpSet = *pEpSet;
  for (int32_t i = 0; i < pEpSet->numOfEps; ++i) {
    dInfo("mnode index:%d %s:%u", i, pEpSet->eps[i].fqdn, pEpSet->eps[i].port);
  }

  taosWUnLockLatch(&pMgmt->latch);
}

void dmGetDnodeEp(SDnode *pDnode, int32_t dnodeId, char *pEp, char *pFqdn, uint16_t *pPort) {
  SMgmtWrapper *pWrapper = dndGetWrapper(pDnode, DNODE);
  SDnodeMgmt   *pMgmt = pWrapper->pMgmt;

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

void dmSendRedirectRsp(SDnode *pDnode, SRpcMsg *pReq) {
  tmsg_t msgType = pReq->msgType;

  SEpSet epSet = {0};
  dmGetMnodeEpSet(pDnode, &epSet);

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

int32_t dmInit(SMgmtWrapper *pWrapper) {
  SDnode     *pDnode = pWrapper->pDnode;
  SDnodeMgmt *pMgmt = calloc(1, sizeof(SDnodeMgmt));

  pMgmt->dnodeId = 0;
  pMgmt->dropped = 0;
  pMgmt->clusterId = 0;
  pMgmt->path = pWrapper->path;
  pMgmt->pDnode = pDnode;
  memcpy(pMgmt->localEp, pDnode->cfg.localEp, TSDB_EP_LEN);
  memcpy(pMgmt->firstEp, pDnode->cfg.firstEp, TSDB_EP_LEN);
  taosInitRWLatch(&pMgmt->latch);

  pMgmt->dnodeHash = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pMgmt->dnodeHash == NULL) {
    dError("node:%s, failed to init dnode hash", pWrapper->name);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  if (dmReadFile(pMgmt) != 0) {
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

  pWrapper->pMgmt = pMgmt;
  dndSetStatus(pDnode, DND_STAT_RUNNING);
  dmSendStatusReq(pMgmt);
  dndReportStartup(pDnode, "TDengine", "initialized successfully");

  dInfo("dnode-mgmt is initialized");
  return 0;
}

void dmCleanup(SMgmtWrapper *pWrapper) {
  SDnodeMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return;

  dmStopWorker(pMgmt);

  taosWLockLatch(&pMgmt->latch);

  if (pMgmt->pDnodeEps != NULL) {
    taosArrayDestroy(pMgmt->pDnodeEps);
    pMgmt->pDnodeEps = NULL;
  }

  if (pMgmt->dnodeHash != NULL) {
    taosHashCleanup(pMgmt->dnodeHash);
    pMgmt->dnodeHash = NULL;
  }

  taosWUnLockLatch(&pMgmt->latch);
  dInfo("dnode-mgmt is cleaned up");
}

bool dmRequire(SMgmtWrapper *pWrapper) { return true; }

void dmGetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = dmInit;
  mgmtFp.closeFp = dmCleanup;
  mgmtFp.requiredFp = dmRequire;

  dmInitMsgHandles(pWrapper);
  pWrapper->name = "dnode";
  pWrapper->fp = mgmtFp;
}
