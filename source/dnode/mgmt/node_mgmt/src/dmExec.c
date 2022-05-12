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
#include "dmImp.h"

static int32_t dmInitParentProc(SMgmtWrapper *pWrapper) {
  int32_t shmsize = tsMnodeShmSize;
  if (pWrapper->nodeType == VNODE) {
    shmsize = tsVnodeShmSize;
  } else if (pWrapper->nodeType == QNODE) {
    shmsize = tsQnodeShmSize;
  } else if (pWrapper->nodeType == SNODE) {
    shmsize = tsSnodeShmSize;
  } else if (pWrapper->nodeType == MNODE) {
    shmsize = tsMnodeShmSize;
  } else if (pWrapper->nodeType == BNODE) {
    shmsize = tsBnodeShmSize;
  } else {
    return -1;
  }

  if (taosCreateShm(&pWrapper->procShm, pWrapper->nodeType, shmsize) != 0) {
    terrno = TAOS_SYSTEM_ERROR(terrno);
    dError("node:%s, failed to create shm size:%d since %s", pWrapper->name, shmsize, terrstr());
    return -1;
  }
  dInfo("node:%s, shm:%d is created, size:%d", pWrapper->name, pWrapper->procShm.id, shmsize);

  SProcCfg cfg = dmGenProcCfg(pWrapper);
  cfg.isChild = false;
  pWrapper->procType = DND_PROC_PARENT;
  pWrapper->procObj = taosProcInit(&cfg);
  if (pWrapper->procObj == NULL) {
    dError("node:%s, failed to create proc since %s", pWrapper->name, terrstr());
    return -1;
  }

  return 0;
}

static int32_t dmNewNodeProc(SMgmtWrapper *pWrapper, EDndNodeType n) {
  char  tstr[8] = {0};
  char *args[6] = {0};
  snprintf(tstr, sizeof(tstr), "%d", n);
  args[1] = "-c";
  args[2] = configDir;
  args[3] = "-n";
  args[4] = tstr;
  args[5] = NULL;

  int32_t pid = taosNewProc(args);
  if (pid <= 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("node:%s, failed to exec in new process since %s", pWrapper->name, terrstr());
    return -1;
  }

  pWrapper->procId = pid;
  dInfo("node:%s, continue running in new process:%d", pWrapper->name, pid);
  return 0;
}

static int32_t dmRunParentProc(SMgmtWrapper *pWrapper) {
  if (pWrapper->pDnode->ntype == NODE_END) {
    dInfo("node:%s, should be started manually in child process", pWrapper->name);
  } else {
    if (dmNewNodeProc(pWrapper, pWrapper->nodeType) != 0) {
      return -1;
    }
  }
  if (taosProcRun(pWrapper->procObj) != 0) {
    dError("node:%s, failed to run proc since %s", pWrapper->name, terrstr());
    return -1;
  }
  return 0;
}

static int32_t dmInitChildProc(SMgmtWrapper *pWrapper) {
  SProcCfg cfg = dmGenProcCfg(pWrapper);
  cfg.isChild = true;
  pWrapper->procObj = taosProcInit(&cfg);
  if (pWrapper->procObj == NULL) {
    dError("node:%s, failed to create proc since %s", pWrapper->name, terrstr());
    return -1;
  }
  return 0;
}

static int32_t dmRunChildProc(SMgmtWrapper *pWrapper) {
  if (taosProcRun(pWrapper->procObj) != 0) {
    dError("node:%s, failed to run proc since %s", pWrapper->name, terrstr());
    return -1;
  }
  return 0;
}

int32_t dmOpenNode(SMgmtWrapper *pWrapper) {
  if (taosMkDir(pWrapper->path) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("node:%s, failed to create dir:%s since %s", pWrapper->name, pWrapper->path, terrstr());
    return -1;
  }

  SMgmtOutputOpt output = {0};
  SMgmtInputOpt *pInput = &pWrapper->pDnode->input;
  pInput->name = pWrapper->name;
  pInput->path = pWrapper->path;
  pInput->msgCb = dmGetMsgcb(pWrapper);
  if (pWrapper->nodeType == DNODE) {
    tmsgSetDefaultMsgCb(&pInput->msgCb);
  }

  if (pWrapper->procType == DND_PROC_SINGLE || pWrapper->procType == DND_PROC_CHILD) {
    if ((*pWrapper->func.openFp)(pInput, &output) != 0) {
      dError("node:%s, failed to open since %s", pWrapper->name, terrstr());
      return -1;
    }
    if (pWrapper->procType == DND_PROC_CHILD) {
      if (dmInitChildProc(pWrapper) != 0) return -1;
      if (dmRunChildProc(pWrapper) != 0) return -1;
    }
    dDebug("node:%s, has been opened", pWrapper->name);
    pWrapper->deployed = true;
  } else {
    if (dmInitParentProc(pWrapper) != 0) return -1;
    if (dmWriteShmFile(pWrapper->path, pWrapper->name, &pWrapper->procShm) != 0) return -1;
    if (dmRunParentProc(pWrapper) != 0) return -1;
  }

  if (output.dnodeId != 0) {
    pInput->dnodeId = output.dnodeId;
  }
  if (output.pMgmt != NULL) {
    pWrapper->pMgmt = output.pMgmt;
  }
  if (output.mnodeEps.numOfEps != 0) {
    pWrapper->pDnode->mnodeEps = output.mnodeEps;
  }

  dmReportStartup(pWrapper->pDnode, pWrapper->name, "openned");
  return 0;
}

int32_t dmStartNode(SMgmtWrapper *pWrapper) {
  if (!pWrapper->required) return 0;

  if (pWrapper->procType == DND_PROC_PARENT) {
    dInfo("node:%s, not start in parent process", pWrapper->name);
  } else if (pWrapper->procType == DND_PROC_CHILD) {
    dInfo("node:%s, start in child process", pWrapper->name);
    if (pWrapper->nodeType != DNODE) {
      if (pWrapper->func.startFp != NULL && (*pWrapper->func.startFp)(pWrapper->pMgmt) != 0) {
        dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
        return -1;
      }
    }
  } else {
    if (pWrapper->func.startFp != NULL && (*pWrapper->func.startFp)(pWrapper->pMgmt) != 0) {
      dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
      return -1;
    }
  }

  dmReportStartup(pWrapper->pDnode, pWrapper->name, "started");
  return 0;
}

void dmStopNode(SMgmtWrapper *pWrapper) {
  if (pWrapper->func.stopFp != NULL && pWrapper->pMgmt != NULL) {
    (*pWrapper->func.stopFp)(pWrapper->pMgmt);
  }
}

void dmCloseNode(SMgmtWrapper *pWrapper) {
  dInfo("node:%s, start to close", pWrapper->name);
  pWrapper->deployed = false;

  while (pWrapper->refCount > 0) {
    taosMsleep(10);
  }

  if (pWrapper->procType == DND_PROC_PARENT) {
    if (pWrapper->procId > 0 && taosProcExist(pWrapper->procId)) {
      dInfo("node:%s, send kill signal to the child process:%d", pWrapper->name, pWrapper->procId);
      taosKillProc(pWrapper->procId);
      dInfo("node:%s, wait for child process:%d to stop", pWrapper->name, pWrapper->procId);
      taosWaitProc(pWrapper->procId);
      dInfo("node:%s, child process:%d is stopped", pWrapper->name, pWrapper->procId);
    }
  }

  taosWLockLatch(&pWrapper->latch);
  if (pWrapper->pMgmt != NULL) {
    (*pWrapper->func.closeFp)(pWrapper->pMgmt);
    pWrapper->pMgmt = NULL;
  }
  taosWUnLockLatch(&pWrapper->latch);

  if (pWrapper->procObj) {
    taosProcCleanup(pWrapper->procObj);
    pWrapper->procObj = NULL;
  }

  dInfo("node:%s, has been closed", pWrapper->name);
}

static int32_t dmOpenNodes(SDnode *pDnode) {
  if (pDnode->ptype == DND_PROC_CHILD) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[pDnode->ntype];
    pWrapper->procType = DND_PROC_CHILD;
    return dmOpenNode(pWrapper);
  } else {
    for (EDndNodeType n = DNODE; n < NODE_END; ++n) {
      SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
      if (!pWrapper->required) continue;
      if (n == DNODE) {
        pWrapper->procType = DND_PROC_SINGLE;
      } else {
        pWrapper->procType = pDnode->ptype;
      }
      if (dmOpenNode(pWrapper) != 0) {
        return -1;
      }
    }
  }

  dmSetStatus(pDnode, DND_STAT_RUNNING);
  return 0;
}

static int32_t dmStartNodes(SDnode *pDnode) {
  for (EDndNodeType n = DNODE; n < NODE_END; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    if (dmStartNode(pWrapper) != 0) {
      dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
      return -1;
    }
  }

  dInfo("TDengine initialized successfully");
  dmReportStartup(pDnode, "TDengine", "initialized successfully");
  return 0;
}

static void dmStopNodes(SDnode *pDnode) {
  for (EDndNodeType n = DNODE; n < NODE_END; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    dmStopNode(pWrapper);
  }
}

static void dmCloseNodes(SDnode *pDnode) {
  for (EDndNodeType n = DNODE; n < NODE_END; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    dmCloseNode(pWrapper);
  }
}

static void dmProcessProcHandle(void *handle) {
  dWarn("handle:%p, the child process dies and send an offline rsp", handle);
  SRpcMsg rpcMsg = {.handle = handle, .code = TSDB_CODE_NODE_OFFLINE};
  rpcSendResponse(&rpcMsg);
}

static void dmWatchNodes(SDnode *pDnode) {
  if (pDnode->ptype != DND_PROC_PARENT) return;
  if (pDnode->ntype == NODE_END) return;

  taosThreadMutexLock(&pDnode->mutex);
  for (EDndNodeType n = DNODE + 1; n < NODE_END; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    if (!pWrapper->required) continue;
    if (pWrapper->procType != DND_PROC_PARENT) continue;

    if (pWrapper->procId <= 0 || !taosProcExist(pWrapper->procId)) {
      dWarn("node:%s, process:%d is killed and needs to be restarted", pWrapper->name, pWrapper->procId);
      if (pWrapper->procObj) {
        taosProcCloseHandles(pWrapper->procObj, dmProcessProcHandle);
      }
      dmNewNodeProc(pWrapper, n);
    }
  }
  taosThreadMutexUnlock(&pDnode->mutex);
}

int32_t dmRun(SDnode *pDnode) {
  if (tsMultiProcess == 0) {
    pDnode->ptype = DND_PROC_SINGLE;
    dInfo("dnode run in single process mode");
  } else if (tsMultiProcess == 2) {
    pDnode->ptype = DND_PROC_TEST;
    dInfo("dnode run in multi-process test mode");
  } else if (pDnode->ntype == DNODE || pDnode->ntype == NODE_END) {
    pDnode->ptype = DND_PROC_PARENT;
    dInfo("dnode run in parent process mode");
  } else {
    pDnode->ptype = DND_PROC_CHILD;
    SMgmtWrapper *pWrapper = &pDnode->wrappers[pDnode->ntype];
    dInfo("%s run in child process mode", pWrapper->name);
  }

  if (pDnode->ptype != DND_PROC_CHILD) {
    if (dmInitServer(pDnode) != 0) {
      dError("failed to init transport since %s", terrstr());
      return -1;
    }
    dmReportStartup(pDnode, "dnode-transport", "initialized");
  }

  if (dmOpenNodes(pDnode) != 0) {
    dError("failed to open nodes since %s", terrstr());
    return -1;
  }

  if (dmStartNodes(pDnode) != 0) {
    dError("failed to start nodes since %s", terrstr());
    return -1;
  }

  while (1) {
    taosMsleep(100);
    if (pDnode->event & DND_EVENT_STOP) {
      dInfo("dnode is about to stop");
      dmSetStatus(pDnode, DND_STAT_STOPPED);
      dmStopNodes(pDnode);
      dmCloseNodes(pDnode);
      return 0;
    } else {
      dmWatchNodes(pDnode);
    }
  }
}
