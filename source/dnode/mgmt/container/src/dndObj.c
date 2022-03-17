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
    tfree(pMgmt->path);
  }
  if (pDnode->pLockFile != NULL) {
    taosUnLockFile(pDnode->pLockFile);
    taosCloseFile(&pDnode->pLockFile);
    pDnode->pLockFile = NULL;
  }
  tfree(pDnode->localEp);
  tfree(pDnode->localFqdn);
  tfree(pDnode->firstEp);
  tfree(pDnode->secondEp);
  tfree(pDnode->dataDir);
  free(pDnode);
  dDebug("dnode object memory is cleared, data:%p", pDnode);
}

SDnode *dndCreate(const SDnodeOpt *pOption) {
  dInfo("start to create dnode object");
  int32_t code = -1;
  char    path[PATH_MAX];
  SDnode *pDnode = NULL;

  pDnode = calloc(1, sizeof(SDnode));
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

SMgmtWrapper *dndAcquireWrapper(SDnode *pDnode, ENodeType nodeType) {
  SMgmtWrapper *pWrapper = &pDnode->wrappers[nodeType];
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

void dndReleaseWrapper(SMgmtWrapper *pWrapper) {
  if (pWrapper == NULL) return;

  taosRLockLatch(&pWrapper->latch);
  int32_t refCount = atomic_sub_fetch_32(&pWrapper->refCount, 1);
  taosRUnLockLatch(&pWrapper->latch);
  dTrace("node:%s, is released, refCount:%d", pWrapper->name, refCount);
}