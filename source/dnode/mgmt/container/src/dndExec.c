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

static void dndResetLog(SMgmtWrapper *pMgmt) {
  char logname[24] = {0};
  snprintf(logname, sizeof(logname), "%slog", pMgmt->name);

  dInfo("node:%s, reset log to %s", pMgmt->name, logname);
  taosCloseLog();
  taosInitLog(logname, 1);
}

static bool dndRequireNode(SMgmtWrapper *pWrapper) {
  bool required = false;
  int32_t code =(*pWrapper->fp.requiredFp)(pWrapper, &required);
  if (!required) {
    dDebug("node:%s, no need to start", pWrapper->name);
  } else {
    dDebug("node:%s, need to start", pWrapper->name);
  }
  return required;
}

int32_t dndOpenNode(SMgmtWrapper *pWrapper) {
  int32_t code = (*pWrapper->fp.openFp)(pWrapper);
  if (code != 0) {
    dError("node:%s, failed to open since %s", pWrapper->name, terrstr());
    return -1;
  } else {
    dDebug("node:%s, has been opened", pWrapper->name);
  }

  pWrapper->deployed = true;
  return 0;
}

void dndCloseNode(SMgmtWrapper *pWrapper) {
  dDebug("node:%s, start to close", pWrapper->name);
  taosWLockLatch(&pWrapper->latch);
  if (pWrapper->deployed) {
    (*pWrapper->fp.closeFp)(pWrapper);
    pWrapper->deployed = false;
  }
  taosWUnLockLatch(&pWrapper->latch);

  while (pWrapper->refCount > 0) {
    taosMsleep(10);
  }

  if (pWrapper->pProc) {
    taosProcCleanup(pWrapper->pProc);
    pWrapper->pProc = NULL;
  }
  dDebug("node:%s, has been closed", pWrapper->name);
}

static int32_t dndRunInSingleProcess(SDnode *pDnode) {
  dInfo("dnode run in single process mode");

  for (ENodeType n = 0; n < NODE_MAX; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    pWrapper->required = dndRequireNode(pWrapper);
    if (!pWrapper->required) continue;

    if (taosMkDir(pWrapper->path) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      dError("failed to create dir:%s since %s", pWrapper->path, terrstr());
      return -1;
    }

    dInfo("node:%s, will start in single process", pWrapper->name);
    pWrapper->procType = PROC_SINGLE;
    if (dndOpenNode(pWrapper) != 0) {
      dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
      return -1;
    }
  }

  dndSetStatus(pDnode, DND_STAT_RUNNING);

  for (ENodeType n = 0; n < NODE_MAX; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    if (!pWrapper->required) continue;
    if (pWrapper->fp.startFp == NULL) continue;
    if ((*pWrapper->fp.startFp)(pWrapper) != 0) {
      dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
      return -1;
    }
  }

  return 0;
}

static void dndClearNodesExecpt(SDnode *pDnode, ENodeType except) {
  dndCleanupServer(pDnode);
  for (ENodeType n = 0; n < NODE_MAX; ++n) {
    if (except == n) continue;
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    dndCloseNode(pWrapper);
  }
}

static void dndConsumeChildQueue(SMgmtWrapper *pWrapper, SNodeMsg *pMsg, int32_t msgLen, void *pCont, int32_t contLen) {
  dTrace("msg:%p, get from child queue", pMsg);
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  pRpc->pCont = pCont;

  NodeMsgFp msgFp = pWrapper->msgFps[TMSG_INDEX(pRpc->msgType)];
  int32_t   code = (*msgFp)(pWrapper, pMsg);

  if (code != 0) {
    if (pRpc->msgType & 1U) {
      SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = terrno};
      dndSendRsp(pWrapper, &rsp);
    }

    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pCont);
  }
}

static void dndConsumeParentQueue(SMgmtWrapper *pWrapper, SRpcMsg *pRsp, int32_t msgLen, void *pCont, int32_t contLen) {
  dTrace("msg:%p, get from parent queue", pRsp);
  pRsp->pCont = pCont;
  dndSendRpcRsp(pWrapper, pRsp);
  taosMemoryFree(pRsp);
}

static int32_t dndRunInMultiProcess(SDnode *pDnode) {
  dInfo("dnode run in multi process mode");

  for (ENodeType n = 0; n < NODE_MAX; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    pWrapper->required = dndRequireNode(pWrapper);
    if (!pWrapper->required) continue;

    if (taosMkDir(pWrapper->path) != 0) {
      terrno = TAOS_SYSTEM_ERROR(errno);
      dError("failed to create dir:%s since %s", pWrapper->path, terrstr());
      return -1;
    }

    if (n == DNODE) {
      dInfo("node:%s, will start in parent process", pWrapper->name);
      pWrapper->procType = PROC_SINGLE;
      if (dndOpenNode(pWrapper) != 0) {
        dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
        return -1;
      }
      continue;
    }

    SProcCfg  cfg = {.childQueueSize = 1024 * 1024 * 2,  // size will be a configuration item
                     .childConsumeFp = (ProcConsumeFp)dndConsumeChildQueue,
                     .childMallocHeadFp = (ProcMallocFp)taosAllocateQitem,
                     .childFreeHeadFp = (ProcFreeFp)taosFreeQitem,
                     .childMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                     .childFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                     .parentQueueSize = 1024 * 1024 * 2,  // size will be a configuration item
                     .parentConsumeFp = (ProcConsumeFp)dndConsumeParentQueue,
                     .parentdMallocHeadFp = (ProcMallocFp)taosMemoryMalloc,
                     .parentFreeHeadFp = (ProcFreeFp)taosMemoryFree,
                     .parentMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                     .parentFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                     .testFlag = 0,
                     .pParent = pWrapper,
                     .name = pWrapper->name};
    SProcObj *pProc = taosProcInit(&cfg);
    if (pProc == NULL) {
      dError("node:%s, failed to fork since %s", pWrapper->name, terrstr());
      return -1;
    }

    pWrapper->pProc = pProc;

    if (taosProcIsChild(pProc)) {
      dInfo("node:%s, will start in child process", pWrapper->name);
      pWrapper->procType = PROC_CHILD;
      dndResetLog(pWrapper);

      dInfo("node:%s, clean up resources inherited from parent", pWrapper->name);
      dndClearNodesExecpt(pDnode, n);

      dInfo("node:%s, will be initialized in child process", pWrapper->name);
      dndOpenNode(pWrapper);
    } else {
      dInfo("node:%s, will not start in parent process", pWrapper->name);
      pWrapper->procType = PROC_PARENT;
    }

    if (taosProcRun(pProc) != 0) {
      dError("node:%s, failed to run proc since %s", pWrapper->name, terrstr());
      return -1;
    }
  }

#if 0
  SMgmtWrapper *pWrapper = dndAcquireWrapper(pDnode, DNODE);
  if (pWrapper->procType == PROC_PARENT && dmStart(pWrapper->pMgmt) != 0) {
    dndReleaseWrapper(pWrapper);
    dError("failed to start dnode worker since %s", terrstr());
    return -1;
  }

  dndReleaseWrapper(pWrapper);
#endif
  return 0;
}

int32_t dndRun(SDnode *pDnode) {
  if (tsMultiProcess == 0) {
    if (dndRunInSingleProcess(pDnode) != 0) {
      dError("failed to run dnode in single process mode since %s", terrstr());
      return -1;
    }
  } else {
    if (dndRunInMultiProcess(pDnode) != 0) {
      dError("failed to run dnode in multi process mode since %s", terrstr());
      return -1;
    }
  }

  dndReportStartup(pDnode, "TDengine", "initialized successfully");

  while (1) {
    if (pDnode->event == DND_EVENT_STOP) {
      dInfo("dnode is about to stop");
      break;
    }
    taosMsleep(100);
  }

  return 0;
}
