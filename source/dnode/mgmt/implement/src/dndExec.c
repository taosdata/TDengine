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
#include "dndNode.h"

static bool dndRequireNode(SMgmtWrapper *pWrapper) {
  bool    required = false;
  int32_t code = (*pWrapper->fp.requiredFp)(pWrapper, &required);
  if (!required) {
    dDebug("node:%s, does not require startup", pWrapper->name);
  } else {
    dDebug("node:%s, needs to be started", pWrapper->name);
  }
  return required;
}

static int32_t dndInitNodeProc(SMgmtWrapper *pWrapper) {
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

  if (taosCreateShm(&pWrapper->procShm, pWrapper->ntype, shmsize) != 0) {
    terrno = TAOS_SYSTEM_ERROR(terrno);
    dError("node:%s, failed to create shm size:%d since %s", pWrapper->name, shmsize, terrstr());
    return -1;
  }
  dInfo("node:%s, shm:%d is created, size:%d", pWrapper->name, pWrapper->procShm.id, shmsize);

  SProcCfg cfg = dndGenProcCfg(pWrapper);
  cfg.isChild = false;
  pWrapper->procType = DND_PROC_PARENT;
  pWrapper->procObj = taosProcInit(&cfg);
  if (pWrapper->procObj == NULL) {
    dError("node:%s, failed to create proc since %s", pWrapper->name, terrstr());
    return -1;
  }

  return 0;
}

static int32_t dndNewNodeProc(SMgmtWrapper *pWrapper, EDndNodeType n) {
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

static int32_t dndRunNodeProc(SMgmtWrapper *pWrapper) {
  if (pWrapper->pDnode->ntype == NODE_END) {
    dInfo("node:%s, should be started manually", pWrapper->name);
  } else {
    if (dndNewNodeProc(pWrapper, pWrapper->ntype) != 0) {
      return -1;
    }
  }

  if (taosProcRun(pWrapper->procObj) != 0) {
    dError("node:%s, failed to run proc since %s", pWrapper->name, terrstr());
    return -1;
  }

  return 0;
}

static int32_t dndOpenNodeImp(SMgmtWrapper *pWrapper) {
  if (taosMkDir(pWrapper->path) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("node:%s, failed to create dir:%s since %s", pWrapper->name, pWrapper->path, terrstr());
    return -1;
  }

  if ((*pWrapper->fp.openFp)(pWrapper) != 0) {
    dError("node:%s, failed to open since %s", pWrapper->name, terrstr());
    return -1;
  }

  dDebug("node:%s, has been opened", pWrapper->name);
  pWrapper->deployed = true;
  return 0;
}

int32_t dndOpenNode(SMgmtWrapper *pWrapper) {
  SDnode *pDnode = pWrapper->pDnode;
  if (pDnode->ptype == DND_PROC_SINGLE) {
    return dndOpenNodeImp(pWrapper);
  } else if (pDnode->ptype == DND_PROC_PARENT) {
    if (dndInitNodeProc(pWrapper) != 0) return -1;
    if (dndWriteShmFile(pDnode) != 0) return -1;
    if (dndRunNodeProc(pWrapper) != 0) return -1;
  }
  return 0;
}

static void dndCloseNodeImp(SMgmtWrapper *pWrapper) {
  dDebug("node:%s, mgmt start to close", pWrapper->name);
  pWrapper->required = false;
  taosWLockLatch(&pWrapper->latch);
  if (pWrapper->deployed) {
    (*pWrapper->fp.closeFp)(pWrapper);
    pWrapper->deployed = false;
  }
  taosWUnLockLatch(&pWrapper->latch);

  while (pWrapper->refCount > 0) {
    taosMsleep(10);
  }

  if (pWrapper->procObj) {
    taosProcCleanup(pWrapper->procObj);
    pWrapper->procObj = NULL;
  }
  dDebug("node:%s, mgmt has been closed", pWrapper->name);
}

void dndCloseNode(SMgmtWrapper *pWrapper) {
  if (pWrapper->pDnode->ptype == DND_PROC_PARENT) {
    if (pWrapper->procId > 0 && taosProcExist(pWrapper->procId)) {
      dInfo("node:%s, send kill signal to the child process:%d", pWrapper->name, pWrapper->procId);
      taosKillProc(pWrapper->procId);
      dInfo("node:%s, wait for child process:%d to stop", pWrapper->name, pWrapper->procId);
      taosWaitProc(pWrapper->procId);
      dInfo("node:%s, child process:%d is stopped", pWrapper->name, pWrapper->procId);
    }
  }
  dndCloseNodeImp(pWrapper);
}

static void dndProcessProcHandle(void *handle) {
  dWarn("handle:%p, the child process dies and send an offline rsp", handle);
  SRpcMsg rpcMsg = {.handle = handle, .code = TSDB_CODE_NODE_OFFLINE};
  rpcSendResponse(&rpcMsg);
}

static int32_t dndRunInSingleProcess(SDnode *pDnode) {
  dInfo("dnode run in single process");
  pDnode->ptype = DND_PROC_SINGLE;

  for (EDndNodeType n = NODE_BEGIN; n < NODE_END; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    pWrapper->required = dndRequireNode(pWrapper);
    if (!pWrapper->required) continue;

    if (dndOpenNodeImp(pWrapper) != 0) {
      dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
      return -1;
    }
  }

  dndSetStatus(pDnode, DND_STAT_RUNNING);

  for (EDndNodeType n = 0; n < NODE_END; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    if (!pWrapper->required) continue;
    if (pWrapper->fp.startFp == NULL) continue;
    if ((*pWrapper->fp.startFp)(pWrapper) != 0) {
      dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
      return -1;
    }
  }

  dInfo("TDengine initialized successfully");
  dndReportStartup(pDnode, "TDengine", "initialized successfully");
  while (1) {
    if (pDnode->event == DND_EVENT_STOP) {
      dInfo("dnode is about to stop");
      dndSetStatus(pDnode, DND_STAT_STOPPED);
      break;
    }
    taosMsleep(100);
  }

  return 0;
}

static int32_t dndRunInParentProcess(SDnode *pDnode) {
  dInfo("dnode run in parent process");
  pDnode->ptype = DND_PROC_PARENT;

  SMgmtWrapper *pDWrapper = &pDnode->wrappers[NODE_BEGIN];
  if (dndOpenNodeImp(pDWrapper) != 0) {
    dError("node:%s, failed to start since %s", pDWrapper->name, terrstr());
    return -1;
  }

  for (EDndNodeType n = NODE_BEGIN + 1; n < NODE_END; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    pWrapper->required = dndRequireNode(pWrapper);
    if (!pWrapper->required) continue;
    if (dndInitNodeProc(pWrapper) != 0) return -1;
  }

  if (dndWriteShmFile(pDnode) != 0) {
    dError("failed to write runtime file since %s", terrstr());
    return -1;
  }

  for (EDndNodeType n = NODE_BEGIN + 1; n < NODE_END; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    if (!pWrapper->required) continue;
    if (dndRunNodeProc(pWrapper) != 0) return -1;
  }

  dndSetStatus(pDnode, DND_STAT_RUNNING);

  if ((*pDWrapper->fp.startFp)(pDWrapper) != 0) {
    dError("node:%s, failed to start since %s", pDWrapper->name, terrstr());
    return -1;
  }

  dInfo("TDengine initialized successfully");
  dndReportStartup(pDnode, "TDengine", "initialized successfully");

  while (1) {
    if (pDnode->event == DND_EVENT_STOP) {
      dInfo("dnode is about to stop");
      dndSetStatus(pDnode, DND_STAT_STOPPED);

      for (EDndNodeType n = NODE_BEGIN + 1; n < NODE_END; ++n) {
        SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
        if (!pWrapper->required) continue;
        if (pDnode->ntype == NODE_END) continue;

        if (pWrapper->procId > 0 && taosProcExist(pWrapper->procId)) {
          dInfo("node:%s, send kill signal to the child process:%d", pWrapper->name, pWrapper->procId);
          taosKillProc(pWrapper->procId);
          dInfo("node:%s, wait for child process:%d to stop", pWrapper->name, pWrapper->procId);
          taosWaitProc(pWrapper->procId);
          dInfo("node:%s, child process:%d is stopped", pWrapper->name, pWrapper->procId);
        }
      }
      break;
    } else {
      for (EDndNodeType n = NODE_BEGIN + 1; n < NODE_END; ++n) {
        SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
        if (!pWrapper->required) continue;
        if (pDnode->ntype == NODE_END) continue;

        if (pWrapper->procId <= 0 || !taosProcExist(pWrapper->procId)) {
          dWarn("node:%s, process:%d is killed and needs to be restarted", pWrapper->name, pWrapper->procId);
          taosProcCloseHandles(pWrapper->procObj, dndProcessProcHandle);
          dndNewNodeProc(pWrapper, n);
        }
      }
    }

    taosMsleep(100);
  }

  return 0;
}

static int32_t dndRunInChildProcess(SDnode *pDnode) {
  SMgmtWrapper *pWrapper = &pDnode->wrappers[pDnode->ntype];
  dInfo("%s run in child process", pWrapper->name);
  pDnode->ptype = DND_PROC_CHILD;

  pWrapper->required = dndRequireNode(pWrapper);
  if (!pWrapper->required) {
    dError("%s does not require startup", pWrapper->name);
    return -1;
  }

  SMsgCb msgCb = dndCreateMsgcb(pWrapper);
  tmsgSetDefaultMsgCb(&msgCb);
  pWrapper->procType = DND_PROC_CHILD;

  if (dndOpenNodeImp(pWrapper) != 0) {
    dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
    return -1;
  }

  SProcCfg cfg = dndGenProcCfg(pWrapper);
  cfg.isChild = true;
  pWrapper->procObj = taosProcInit(&cfg);
  if (pWrapper->procObj == NULL) {
    dError("node:%s, failed to create proc since %s", pWrapper->name, terrstr());
    return -1;
  }

  if (pWrapper->fp.startFp != NULL) {
    if ((*pWrapper->fp.startFp)(pWrapper) != 0) {
      dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
      return -1;
    }
  }

  dndSetStatus(pDnode, DND_STAT_RUNNING);

  if (taosProcRun(pWrapper->procObj) != 0) {
    dError("node:%s, failed to run proc since %s", pWrapper->name, terrstr());
    return -1;
  }

  dInfo("TDengine initialized successfully");
  dndReportStartup(pDnode, "TDengine", "initialized successfully");
  while (1) {
    if (pDnode->event == DND_EVENT_STOP) {
      dInfo("%s is about to stop", pWrapper->name);
      dndSetStatus(pDnode, DND_STAT_STOPPED);
      break;
    }
    taosMsleep(100);
  }

  return 0;
}

int32_t dndRun(SDnode *pDnode) {
  if (!tsMultiProcess) {
    return dndRunInSingleProcess(pDnode);
  } else if (pDnode->ntype == NODE_BEGIN || pDnode->ntype == NODE_END) {
    return dndRunInParentProcess(pDnode);
  } else {
    return dndRunInChildProcess(pDnode);
  }

  return 0;
}
