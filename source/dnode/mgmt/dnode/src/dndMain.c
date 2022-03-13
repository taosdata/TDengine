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
#include "dndMain.h"
// #include "dndBnode.h"
#include "dndMgmt.h"
// #include "mm.h"
// #include "dndQnode.h"
// #include "dndSnode.h"
#include "dndTransport.h"
// #include "dndVnodes.h"
// #include "monitor.h"
// #include "sync.h"
// #include "tfs.h"
// #include "wal.h"

static int8_t once = DND_ENV_INIT;

SMgmtFp mmGetNodeFp() {
  SMgmtFp nullFp = {0};
  return nullFp;
}

SMgmtFp vndGetNodeFp() {
  SMgmtFp nullFp = {0};
  return nullFp;
}

SMgmtFp qndGetNodeFp() {
  SMgmtFp nullFp = {0};
  return nullFp;
}

SMgmtFp sndGetNodeFp() {
  SMgmtFp nullFp = {0};
  return nullFp;
}

SMgmtFp bndGetNodeFp() {
  SMgmtFp nullFp = {0};
  return nullFp;
}

static void dndResetLog(SMgmtWrapper *pMgmt) {
  char logname[24] = {0};
  snprintf(logname, sizeof(logname), "%slog", pMgmt->name);

  dInfo("node:%s, reset log to %s", pMgmt->name, logname);
  taosCloseLog();
  taosInitLog(logname, 1);
}

static bool dndRequireOpenNode(SMgmtWrapper *pMgmt) {
  bool required = (*pMgmt->fp.requiredFp)(pMgmt);
  if (!required) {
    dDebug("node:%s, no need to start on this dnode", pMgmt->name);
  } else {
    dDebug("node:%s, need to start on this dnode", pMgmt->name);
  }
  return required;
}

static void dndClearDnodeMem(SDnode *pDnode) {
  for (ENodeType n = 0; n < NODE_MAX; ++n) {
    SMgmtWrapper *pMgmt = &pDnode->mgmts[n];
    tfree(pMgmt->path);
  }
  if (pDnode->pLockFile != NULL) {
    taosUnLockFile(pDnode->pLockFile);
    taosCloseFile(&pDnode->pLockFile);
  }
  tfree(pDnode->path);
  dDebug("dnode object memory is cleared, data:%p", pDnode);
}

static int32_t dndInitDnodeResource(SDnode *pDnode) {
  SDiskCfg dCfg = {0};
  tstrncpy(dCfg.dir, pDnode->cfg.dataDir, TSDB_FILENAME_LEN);
  dCfg.level = 0;
  dCfg.primary = 1;
  SDiskCfg *pDisks = pDnode->cfg.pDisks;
  int32_t   numOfDisks = pDnode->cfg.numOfDisks;
  if (numOfDisks <= 0 || pDisks == NULL) {
    pDisks = &dCfg;
    numOfDisks = 1;
  }

  pDnode->pTfs = tfsOpen(pDisks, numOfDisks);
  if (pDnode->pTfs == NULL) {
    dError("failed to init tfs since %s", terrstr());
    return -1;
  }

  if (dndInitMgmt(pDnode) != 0) {
    dError("failed to init mgmt since %s", terrstr());
    return -1;
  }

  if (dndInitTrans(pDnode) != 0) {
    dError("failed to init transport since %s", terrstr());
    return -1;
  }

  dndSetStatus(pDnode, DND_STAT_RUNNING);
  dndSendStatusReq(pDnode);
  dndReportStartup(pDnode, "TDengine", "initialized successfully");
  return 0;
}

static void dndClearDnodeResource(SDnode *pDnode) {
  dndCleanupTrans(pDnode);
  dndStopMgmt(pDnode);
  dndCleanupMgmt(pDnode);
  tfsClose(pDnode->pTfs);
  dDebug("dnode object resource is cleared, data:%p", pDnode);
}

SDnode *dndCreate(SDndCfg *pCfg) {
  dInfo("start to create dnode object");
  int32_t code = -1;
  char    path[PATH_MAX + 100];
  SDnode *pDnode = NULL;

  pDnode = calloc(1, sizeof(SDnode));
  if (pDnode == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _OVER;
  }

  dndSetStatus(pDnode, DND_STAT_INIT);

  snprintf(path, sizeof(path), "%s%sdnode", pCfg->dataDir, TD_DIRSEP);
  pDnode->path = strdup(path);
  if (taosMkDir(path) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("failed to create dir:%s since %s", path, terrstr());
    goto _OVER;
  }

  pDnode->mgmts[MNODE].fp = mmGetNodeFp();
  pDnode->mgmts[VNODES].fp = vndGetNodeFp();
  pDnode->mgmts[QNODE].fp = qndGetNodeFp();
  pDnode->mgmts[SNODE].fp = sndGetNodeFp();
  pDnode->mgmts[BNODE].fp = bndGetNodeFp();
  pDnode->mgmts[MNODE].name = "mnode";
  pDnode->mgmts[VNODES].name = "vnodes";
  pDnode->mgmts[QNODE].name = "qnode";
  pDnode->mgmts[SNODE].name = "snode";
  pDnode->mgmts[BNODE].name = "bnode";
  memcpy(&pDnode->cfg, pCfg, sizeof(SDndCfg));

  for (ENodeType n = 0; n < NODE_MAX; ++n) {
    SMgmtWrapper *pMgmt = &pDnode->mgmts[n];
    snprintf(path, sizeof(path), "%s%s%s", pCfg->dataDir, TD_DIRSEP, pDnode->mgmts[n].name);
    pMgmt->path = strdup(path);
    if (pDnode->mgmts[n].path == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    pMgmt->procType = PROC_SINGLE;
    pMgmt->required = dndRequireOpenNode(pMgmt);
    if (pMgmt->required) {
      if (taosMkDir(pMgmt->path) != 0) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        dError("failed to create dir:%s since %s", pMgmt->path, terrstr());
        goto _OVER;
      }
    }
  }

  pDnode->pLockFile = dndCheckRunning(pCfg->dataDir);
  if (pDnode->pLockFile == NULL) {
    goto _OVER;
  }

_OVER:
  if (code != 0 && pDnode) {
    dndClearDnodeMem(pDnode);
    tfree(pDnode);
    dError("failed to create dnode object since %s", terrstr());
  } else {
    dInfo("dnode object is created, data:%p", pDnode);
  }

  return pDnode;
}

#if 0






  if (dndInitVnodes(pDnode) != 0) {
    dError("failed to init vnodes since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }

  if (dndInitQnode(pDnode) != 0) {
    dError("failed to init qnode since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }

  if (dndInitSnode(pDnode) != 0) {
    dError("failed to init snode since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }

  if (dndInitBnode(pDnode) != 0) {
    dError("failed to init bnode since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }

  if (mmInit(pDnode) != 0) {
    dError("failed to init mnode since %s", terrstr());
    dndClose(pDnode);
    return NULL;
  }


// mmCleanup(pDnode);
  // dndCleanupBnode(pDnode);
  // dndCleanupSnode(pDnode);
  // dndCleanupQnode(pDnode);
  // dndCleanupVnodes(pDnode);
    

  return pDnode;
}
#endif

void dndClose(SDnode *pDnode) {
  if (pDnode == NULL) return;

  if (dndGetStatus(pDnode) == DND_STAT_STOPPED) {
    dError("dnode is shutting down, data:%p", pDnode);
    return;
  }

  dInfo("start to close dnode, data:%p", pDnode);
  dndSetStatus(pDnode, DND_STAT_STOPPED);

  dndClearDnodeResource(pDnode);
  dndClearDnodeMem(pDnode);
  tfree(pDnode);
  dInfo("dnode object is closed, data:%p", pDnode);
}

int32_t dndInit() {
  if (atomic_val_compare_exchange_8(&once, DND_ENV_INIT, DND_ENV_READY) != DND_ENV_INIT) {
    terrno = TSDB_CODE_REPEAT_INIT;
    dError("failed to init dnode env since %s", terrstr());
    return -1;
  }

  taosIgnSIGPIPE();
  taosBlockSIGPIPE();
  taosResolveCRC();

  if (rpcInit() != 0) {
    dError("failed to init rpc since %s", terrstr());
    dndCleanup();
    return -1;
  }

  if (walInit() != 0) {
    dError("failed to init wal since %s", terrstr());
    dndCleanup();
    return -1;
  }


  // SVnodeOpt vnodeOpt = {
  //     .nthreads = tsNumOfCommitThreads, .putReqToVQueryQFp = dndPutReqToVQueryQ, .sendReqToDnodeFp = dndSendReqToDnode};

  // if (vnodeInit(&vnodeOpt) != 0) {
  //   dError("failed to init vnode since %s", terrstr());
  //   dndCleanup();
  //   return -1;
  // }

  SMonCfg monCfg = {.maxLogs = tsMonitorMaxLogs, .port = tsMonitorPort, .server = tsMonitorFqdn, .comp = tsMonitorComp};
  if (monInit(&monCfg) != 0) {
    dError("failed to init monitor since %s", terrstr());
    dndCleanup();
    return -1;
  }

  dInfo("dnode env is initialized");
  return 0;
}

void dndCleanup() {
  if (atomic_val_compare_exchange_8(&once, DND_ENV_READY, DND_ENV_CLEANUP) != DND_ENV_READY) {
    dError("dnode env is already cleaned up");
    return;
  }

  walCleanUp();
  // vnodeCleanup();
  rpcCleanup();
  monCleanup();

  taosStopCacheRefreshWorker();
  dInfo("dnode env is cleaned up");
}

void dndRun(SDnode *pDnode) {
  while (pDnode->event != DND_EVENT_STOP) {
    taosMsleep(100);
  }
}
