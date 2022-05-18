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

static int32_t dmCreateShm(SMgmtWrapper *pWrapper) {
  int32_t shmsize = tsMnodeShmSize;
  if (pWrapper->ntype == VNODE) {
    shmsize = tsVnodeShmSize;
  } else if (pWrapper->ntype == QNODE) {
    shmsize = tsQnodeShmSize;
  } else if (pWrapper->ntype == SNODE) {
    shmsize = tsSnodeShmSize;
  } else if (pWrapper->ntype == MNODE) {
    shmsize = tsMnodeShmSize;
  } else if (pWrapper->ntype == BNODE) {
    shmsize = tsBnodeShmSize;
  } else {
    return -1;
  }

  if (taosCreateShm(&pWrapper->proc.shm, pWrapper->ntype, shmsize) != 0) {
    terrno = TAOS_SYSTEM_ERROR(terrno);
    dError("node:%s, failed to create shm size:%d since %s", pWrapper->name, shmsize, terrstr());
    return -1;
  }

  dInfo("node:%s, shm:%d is created, size:%d", pWrapper->name, pWrapper->proc.shm.id, shmsize);
  return 0;
}

static int32_t dmNewProc(SMgmtWrapper *pWrapper, EDndNodeType ntype) {
  char  tstr[8] = {0};
  char *args[6] = {0};
  snprintf(tstr, sizeof(tstr), "%d", ntype);
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

  taosIgnSignal(SIGCHLD);
  pWrapper->proc.pid = pid;
  dInfo("node:%s, continue running in new pid:%d", pWrapper->name, pid);
  return 0;
}

int32_t dmOpenNode(SMgmtWrapper *pWrapper) {
  SDnode *pDnode = pWrapper->pDnode;

  if (taosMkDir(pWrapper->path) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("node:%s, failed to create dir:%s since %s", pWrapper->name, pWrapper->path, terrstr());
    return -1;
  }

  SMgmtOutputOpt output = {0};
  SMgmtInputOpt  input = dmBuildMgmtInputOpt(pWrapper);

  if (pWrapper->ntype == DNODE || InChildProc(pWrapper)) {
    tmsgSetDefault(&input.msgCb);
  }

  if (OnlyInSingleProc(pWrapper)) {
    dInfo("node:%s, start to open", pWrapper->name);
    if ((*pWrapper->func.openFp)(&input, &output) != 0) {
      dError("node:%s, failed to open since %s", pWrapper->name, terrstr());
      return -1;
    }
    dInfo("node:%s, has been opened", pWrapper->name);
    pWrapper->deployed = true;
  }

  if (InParentProc(pWrapper)) {
    dDebug("node:%s, start to open", pWrapper->name);
    if (dmCreateShm(pWrapper) != 0) {
      return -1;
    }
    if (dmWriteShmFile(pWrapper->path, pWrapper->name, &pWrapper->proc.shm) != 0) {
      return -1;
    }

    if (OnlyInParentProc(pWrapper)) {
      if (dmInitProc(pWrapper) != 0) {
        dError("node:%s, failed to init proc since %s", pWrapper->name, terrstr());
        return -1;
      }
      if (pDnode->rtype == NODE_END) {
        dInfo("node:%s, should be started manually in child process", pWrapper->name);
      } else {
        if (dmNewProc(pWrapper, pWrapper->ntype) != 0) {
          return -1;
        }
      }
      if (dmRunProc(&pWrapper->proc) != 0) {
        dError("node:%s, failed to run proc since %s", pWrapper->name, terrstr());
        return -1;
      }
    }
    dDebug("node:%s, has been opened in parent process", pWrapper->name);
  }

  if (InChildProc(pWrapper)) {
    dDebug("node:%s, start to open", pWrapper->name);
    if ((*pWrapper->func.openFp)(&input, &output) != 0) {
      dError("node:%s, failed to open since %s", pWrapper->name, terrstr());
      return -1;
    }
    if (dmInitProc(pWrapper) != 0) {
      return -1;
    }
    if (dmRunProc(&pWrapper->proc) != 0) {
      return -1;
    }
    dDebug("node:%s, has been opened in child process", pWrapper->name);
    pWrapper->deployed = true;
  }

  if (output.pMgmt != NULL) {
    pWrapper->pMgmt = output.pMgmt;
  }

  dmReportStartup(pWrapper->name, "openned");
  return 0;
}

int32_t dmStartNode(SMgmtWrapper *pWrapper) {
  if (OnlyInParentProc(pWrapper)) return 0;
  if (pWrapper->func.startFp != NULL) {
    dDebug("node:%s, start to start", pWrapper->name);
    if ((*pWrapper->func.startFp)(pWrapper->pMgmt) != 0) {
      dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
      return -1;
    }
    dDebug("node:%s, has been started", pWrapper->name);
  }

  dmReportStartup(pWrapper->name, "started");
  return 0;
}

void dmStopNode(SMgmtWrapper *pWrapper) {
  if (pWrapper->func.stopFp != NULL && pWrapper->pMgmt != NULL) {
    dDebug("node:%s, start to stop", pWrapper->name);
    (*pWrapper->func.stopFp)(pWrapper->pMgmt);
    dDebug("node:%s, has been stopped", pWrapper->name);
  }
}

void dmCloseNode(SMgmtWrapper *pWrapper) {
  dInfo("node:%s, start to close", pWrapper->name);
  pWrapper->deployed = false;

  while (pWrapper->refCount > 0) {
    taosMsleep(10);
  }

  if (OnlyInParentProc(pWrapper)) {
    int32_t pid = pWrapper->proc.pid;
    if (pid > 0 && taosProcExist(pid)) {
      dInfo("node:%s, send kill signal to the child pid:%d", pWrapper->name, pid);
      taosKillProc(pid);
      dInfo("node:%s, wait for child pid:%d to stop", pWrapper->name, pid);
      taosWaitProc(pid);
      dInfo("node:%s, child pid:%d is stopped", pWrapper->name, pid);
    }
  }

  taosWLockLatch(&pWrapper->latch);
  if (pWrapper->pMgmt != NULL) {
    (*pWrapper->func.closeFp)(pWrapper->pMgmt);
    pWrapper->pMgmt = NULL;
  }
  taosWUnLockLatch(&pWrapper->latch);

  if (!OnlyInSingleProc(pWrapper)) {
    dmCleanupProc(pWrapper);
  }

  dInfo("node:%s, has been closed", pWrapper->name);
}

static int32_t dmOpenNodes(SDnode *pDnode) {
  for (EDndNodeType ntype = DNODE; ntype < NODE_END; ++ntype) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
    if (!pWrapper->required) continue;
    if (dmOpenNode(pWrapper) != 0) {
      dError("node:%s, failed to open since %s", pWrapper->name, terrstr());
      return -1;
    }
  }

  dmSetStatus(pDnode, DND_STAT_RUNNING);
  return 0;
}

static int32_t dmStartNodes(SDnode *pDnode) {
  for (EDndNodeType ntype = DNODE; ntype < NODE_END; ++ntype) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
    if (!pWrapper->required) continue;
    if (dmStartNode(pWrapper) != 0) {
      dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
      return -1;
    }
  }

  dInfo("TDengine initialized successfully");
  dmReportStartup("TDengine", "initialized successfully");
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

static void dmWatchNodes(SDnode *pDnode) {
  if (pDnode->ptype != PARENT_PROC) return;
  if (pDnode->rtype == NODE_END) return;

  taosThreadMutexLock(&pDnode->mutex);
  for (EDndNodeType ntype = DNODE + 1; ntype < NODE_END; ++ntype) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[ntype];
    SProc        *proc = &pWrapper->proc;

    if (!pWrapper->required) continue;
    if (!OnlyInParentProc(pWrapper)) continue;

    if (proc->pid <= 0 || !taosProcExist(proc->pid)) {
      dError("node:%s, pid:%d is killed and needs to restart", pWrapper->name, proc->pid);
      dmCloseProcRpcHandles(&pWrapper->proc);
      dmNewProc(pWrapper, ntype);
    }
  }
  taosThreadMutexUnlock(&pDnode->mutex);
}

int32_t dmRunDnode(SDnode *pDnode) {
  if (dmOpenNodes(pDnode) != 0) {
    dError("failed to open nodes since %s", terrstr());
    return -1;
  }

  if (dmStartNodes(pDnode) != 0) {
    dError("failed to start nodes since %s", terrstr());
    return -1;
  }

  while (1) {
    if (pDnode->stop) {
      dInfo("dnode is about to stop");
      dmSetStatus(pDnode, DND_STAT_STOPPED);
      dmStopNodes(pDnode);
      dmCloseNodes(pDnode);
      return 0;
    }

    dmWatchNodes(pDnode);
    taosMsleep(100);
  }
}
