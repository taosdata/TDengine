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
#include "dndInt.h"

static int32_t dndInitMemory(SDnode *pDnode, const SDnodeOpt *pOption) {
  pDnode->numOfSupportVnodes = pOption->numOfSupportVnodes;
  pDnode->serverPort = pOption->serverPort;
  pDnode->dataDir = strdup(pOption->dataDir);
  pDnode->localEp = strdup(pOption->localEp);
  pDnode->localFqdn = strdup(pOption->localFqdn);
  pDnode->firstEp = strdup(pOption->firstEp);
  pDnode->secondEp = strdup(pOption->secondEp);
  pDnode->pDisks = pOption->pDisks;
  pDnode->numOfDisks = pOption->numOfDisks;
  pDnode->rebootTime = taosGetTimestampMs();

  if (pDnode->dataDir == NULL || pDnode->localEp == NULL || pDnode->localFqdn == NULL || pDnode->firstEp == NULL ||
      pDnode->secondEp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  return 0;
}

static void dndClearMemory(SDnode *pDnode) {
  for (ENodeType n = 0; n < NODE_MAX; ++n) {
    SMgmtWrapper *pMgmt = &pDnode->wrappers[n];
    taosMemoryFreeClear(pMgmt->path);
  }
  if (pDnode->pLockFile != NULL) {
    taosUnLockFile(pDnode->pLockFile);
    taosCloseFile(&pDnode->pLockFile);
    pDnode->pLockFile = NULL;
  }
  taosMemoryFreeClear(pDnode->localEp);
  taosMemoryFreeClear(pDnode->localFqdn);
  taosMemoryFreeClear(pDnode->firstEp);
  taosMemoryFreeClear(pDnode->secondEp);
  taosMemoryFreeClear(pDnode->dataDir);
  taosMemoryFree(pDnode);
  dDebug("dnode object memory is cleared, data:%p", pDnode);
}

SDnode *dndCreate(const SDnodeOpt *pOption) {
  dInfo("start to create dnode object");
  int32_t code = -1;
  char    path[PATH_MAX] = {0};
  SDnode *pDnode = NULL;

  pDnode = taosMemoryCalloc(1, sizeof(SDnode));
  if (pDnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  if (dndInitMemory(pDnode, pOption) != 0) {
    goto _OVER;
  }

  dndSetStatus(pDnode, DND_STAT_INIT);
  pDnode->pLockFile = dndCheckRunning(pDnode->dataDir);
  if (pDnode->pLockFile == NULL) {
    goto _OVER;
  }

  if (dndInitServer(pDnode) != 0) {
    dError("failed to init trans server since %s", terrstr());
    goto _OVER;
  }

  if (dndInitClient(pDnode) != 0) {
    dError("failed to init trans client since %s", terrstr());
    goto _OVER;
  }

  dmGetMgmtFp(&pDnode->wrappers[DNODE]);
  mmGetMgmtFp(&pDnode->wrappers[MNODE]);
  vmGetMgmtFp(&pDnode->wrappers[VNODES]);
  qmGetMgmtFp(&pDnode->wrappers[QNODE]);
  smGetMgmtFp(&pDnode->wrappers[SNODE]);
  bmGetMgmtFp(&pDnode->wrappers[BNODE]);

  if (dndInitMsgHandle(pDnode) != 0) {
    goto _OVER;
  }

  for (ENodeType n = 0; n < NODE_MAX; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    snprintf(path, sizeof(path), "%s%s%s", pDnode->dataDir, TD_DIRSEP, pWrapper->name);
    pWrapper->path = strdup(path);
    pWrapper->pDnode = pDnode;
    if (pWrapper->path == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    pWrapper->procType = PROC_SINGLE;
    taosInitRWLatch(&pWrapper->latch);
  }

  code = 0;

_OVER:
  if (code != 0 && pDnode) {
    dndClearMemory(pDnode);
    pDnode = NULL;
    dError("failed to create dnode object since %s", terrstr());
  } else {
    dInfo("dnode object is created, data:%p", pDnode);
  }

  return pDnode;
}

void dndClose(SDnode *pDnode) {
  if (pDnode == NULL) return;

  if (dndGetStatus(pDnode) == DND_STAT_STOPPED) {
    dError("dnode is shutting down, data:%p", pDnode);
    return;
  }

  dInfo("start to close dnode, data:%p", pDnode);
  dndSetStatus(pDnode, DND_STAT_STOPPED);

  dndCleanupServer(pDnode);
  dndCleanupClient(pDnode);

  for (ENodeType n = 0; n < NODE_MAX; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    dndCloseNode(pWrapper);
  }

  dndClearMemory(pDnode);
  dInfo("dnode object is closed, data:%p", pDnode);
}

void dndHandleEvent(SDnode *pDnode, EDndEvent event) {
  dInfo("dnode object receive event %d, data:%p", event, pDnode);
  pDnode->event = event;
}

SMgmtWrapper *dndAcquireWrapper(SDnode *pDnode, ENodeType ntype) {
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

int32_t dndMarkWrapper(SMgmtWrapper *pWrapper) {
  int32_t code = 0;

  taosRLockLatch(&pWrapper->latch);
  if (pWrapper->deployed) {
    int32_t refCount = atomic_add_fetch_32(&pWrapper->refCount, 1);
    dTrace("node:%s, is marked, refCount:%d", pWrapper->name, refCount);
  } else {
    terrno = TSDB_CODE_NODE_NOT_DEPLOYED;
    code = -1;
  }
  taosRUnLockLatch(&pWrapper->latch);

  return code;
}

void dndReleaseWrapper(SMgmtWrapper *pWrapper) {
  if (pWrapper == NULL) return;

  taosRLockLatch(&pWrapper->latch);
  int32_t refCount = atomic_sub_fetch_32(&pWrapper->refCount, 1);
  taosRUnLockLatch(&pWrapper->latch);
  dTrace("node:%s, is released, refCount:%d", pWrapper->name, refCount);
}