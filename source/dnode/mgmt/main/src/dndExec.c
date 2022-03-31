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

static bool dndRequireNode(SMgmtWrapper *pWrapper) {
  bool    required = false;
  int32_t code = (*pWrapper->fp.requiredFp)(pWrapper, &required);
  if (!required) {
    dDebug("node:%s, no need to start", pWrapper->name);
  } else {
    dDebug("node:%s, need to start", pWrapper->name);
  }
  return required;
}

int32_t dndOpenNode(SMgmtWrapper *pWrapper) {
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

void dndCloseNode(SMgmtWrapper *pWrapper) {
  dDebug("node:%s, start to close", pWrapper->name);
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

  if (pWrapper->pProc) {
    taosProcCleanup(pWrapper->pProc);
    pWrapper->pProc = NULL;
  }
  dDebug("node:%s, has been closed", pWrapper->name);
}

static void dndConsumeChildQueue(SMgmtWrapper *pWrapper, SNodeMsg *pMsg, int16_t msgLen, void *pCont, int32_t contLen,
                                 ProcFuncType ftype) {
  SRpcMsg *pRpc = &pMsg->rpcMsg;
  pRpc->pCont = pCont;
  dTrace("msg:%p, get from child queue, handle:%p app:%p", pMsg, pRpc->handle, pRpc->ahandle);

  NodeMsgFp msgFp = pWrapper->msgFps[TMSG_INDEX(pRpc->msgType)];
  int32_t   code = (*msgFp)(pWrapper, pMsg);

  if (code != 0) {
    dError("msg:%p, failed to process since code:0x%04x:%s", pMsg, code & 0XFFFF, tstrerror(code));
    if (pRpc->msgType & 1U) {
      SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = terrno};
      dndSendRsp(pWrapper, &rsp);
    }

    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pCont);
  }
}

static void dndConsumeParentQueue(SMgmtWrapper *pWrapper, SRpcMsg *pMsg, int16_t msgLen, void *pCont, int32_t contLen,
                                  ProcFuncType ftype) {
  pMsg->pCont = pCont;
  dTrace("msg:%p, get from parent queue, handle:%p app:%p", pMsg, pMsg->handle, pMsg->ahandle);

  switch (ftype) {
    case PROC_REG:
      rpcRegisterBrokenLinkArg(pMsg);
      break;
    case PROC_RELEASE:
      rpcReleaseHandle(pMsg->handle, (int8_t)pMsg->code);
      rpcFreeCont(pCont);
      break;
    case PROC_REQ:
      // todo send to dnode
      dndSendReqToMnode(pWrapper, pMsg);
    default:
      dndSendRpcRsp(pWrapper, pMsg);
      break;
  }
  taosMemoryFree(pMsg);
}

static int32_t dndNewProc(SMgmtWrapper *pWrapper, ENodeType n) {
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
  dInfo("node:%s, run in new process, pid:%d", pWrapper->name, pid);
  return 0;
}

static SProcCfg dndGenProcCfg(SMgmtWrapper *pWrapper) {
  SProcCfg cfg = {.childConsumeFp = (ProcConsumeFp)dndConsumeChildQueue,
                  .childMallocHeadFp = (ProcMallocFp)taosAllocateQitem,
                  .childFreeHeadFp = (ProcFreeFp)taosFreeQitem,
                  .childMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                  .childFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                  .parentConsumeFp = (ProcConsumeFp)dndConsumeParentQueue,
                  .parentMallocHeadFp = (ProcMallocFp)taosMemoryMalloc,
                  .parentFreeHeadFp = (ProcFreeFp)taosMemoryFree,
                  .parentMallocBodyFp = (ProcMallocFp)rpcMallocCont,
                  .parentFreeBodyFp = (ProcFreeFp)rpcFreeCont,
                  .shm = pWrapper->shm,
                  .pParent = pWrapper,
                  .name = pWrapper->name};
  return cfg;
}

static int32_t dndRunInSingleProcess(SDnode *pDnode) {
  dInfo("dnode start to run in single process");

  for (ENodeType n = DNODE; n < NODE_MAX; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    pWrapper->required = dndRequireNode(pWrapper);
    if (!pWrapper->required) continue;

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

  dInfo("TDengine initialized successfully");
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

static int32_t dndRunInParentProcess(SDnode *pDnode) {
  dInfo("dnode start to run in parent process");
  SMgmtWrapper *pDWrapper = &pDnode->wrappers[DNODE];
  if (dndOpenNode(pDWrapper) != 0) {
    dError("node:%s, failed to start since %s", pDWrapper->name, terrstr());
    return -1;
  }

  for (ENodeType n = DNODE + 1; n < NODE_MAX; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    pWrapper->required = dndRequireNode(pWrapper);
    if (!pWrapper->required) continue;

    int64_t shmsize = 1024 * 1024 * 2;  // size will be a configuration item
    if (taosCreateShm(&pWrapper->shm, shmsize) != 0) {
      terrno = TAOS_SYSTEM_ERROR(terrno);
      dError("node:%s, failed to create shm size:%" PRId64 " since %s", pWrapper->name, shmsize, terrstr());
      return -1;
    }

    SProcCfg cfg = dndGenProcCfg(pWrapper);
    cfg.isChild = false;
    pWrapper->procType = PROC_PARENT;
    pWrapper->pProc = taosProcInit(&cfg);
    if (pWrapper->pProc == NULL) {
      dError("node:%s, failed to create proc since %s", pWrapper->name, terrstr());
      return -1;
    }
  }

  if (dndWriteShmFile(pDnode) != 0) {
    dError("failed to write runtime file since %s", terrstr());
    return -1;
  }

  for (ENodeType n = DNODE + 1; n < NODE_MAX; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    if (!pWrapper->required) continue;

    if (pDnode->ntype == NODE_MAX) {
      dInfo("node:%s, should be started manually", pWrapper->name);
    } else {
      if (dndNewProc(pWrapper, n) != 0) {
        return -1;
      }
    }

    if (taosProcRun(pWrapper->pProc) != 0) {
      dError("node:%s, failed to run proc since %s", pWrapper->name, terrstr());
      return -1;
    }
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
      break;
    }

    for (ENodeType n = DNODE + 1; n < NODE_MAX; ++n) {
      SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
      if (!pWrapper->required) continue;
      if (pDnode->ntype == NODE_MAX) continue;

      if (pWrapper->procId != 0 && !taosProcExists(pWrapper->procId)) {
        dInfo("node:%s, process not exist, pid:%d", pWrapper->name, pWrapper->procId);
        dndNewProc(pWrapper, n);
      }

      taosMsleep(100);
    }
  }

  return 0;
}

static int32_t dndRunInChildProcess(SDnode *pDnode) {
  dInfo("dnode start to run in child process");
  SMgmtWrapper *pWrapper = &pDnode->wrappers[pDnode->ntype];

  SMsgCb msgCb = dndCreateMsgcb(pWrapper);
  tmsgSetDefaultMsgCb(&msgCb);
  pWrapper->procType = PROC_CHILD;

  if (dndOpenNode(pWrapper) != 0) {
    dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
    return -1;
  }

  SProcCfg cfg = dndGenProcCfg(pWrapper);
  cfg.isChild = true;
  pWrapper->pProc = taosProcInit(&cfg);
  if (pWrapper->pProc == NULL) {
    dError("node:%s, failed to create proc since %s", pWrapper->name, terrstr());
    return -1;
  }

  if (pWrapper->fp.startFp != NULL) {
    if ((*pWrapper->fp.startFp)(pWrapper) != 0) {
      dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
      return -1;
    }
  }

  if (taosProcRun(pWrapper->pProc) != 0) {
    dError("node:%s, failed to run proc since %s", pWrapper->name, terrstr());
    return -1;
  }

  dInfo("TDengine initialized successfully");
  dndReportStartup(pDnode, "TDengine", "initialized successfully");
  while (1) {
    if (pDnode->event == DND_EVENT_STOP) {
      dInfo("dnode is about to stop");
      break;
    }
    taosMsleep(100);
  }
}

int32_t dndRun(SDnode *pDnode) {
  if (!tsMultiProcess) {
    return dndRunInSingleProcess(pDnode);
  } else if (pDnode->ntype == DNODE || pDnode->ntype == NODE_MAX) {
    return dndRunInParentProcess(pDnode);
  } else {
    return dndRunInChildProcess(pDnode);
  }

  return 0;
}
