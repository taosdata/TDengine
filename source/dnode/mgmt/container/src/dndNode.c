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
#include "dndTransport.h"

#include "bmInt.h"
#include "dmInt.h"
#include "mmInt.h"
#include "qmInt.h"
#include "smInt.h"
#include "vmInt.h"

static void dndResetLog(SMgmtWrapper *pMgmt) {
  char logname[24] = {0};
  snprintf(logname, sizeof(logname), "%slog", pMgmt->name);

  dInfo("node:%s, reset log to %s", pMgmt->name, logname);
  taosCloseLog();
  taosInitLog(logname, 1);
}

static bool dndRequireNode(SMgmtWrapper *pMgmt) {
  bool required = (*pMgmt->fp.requiredFp)(pMgmt);
  if (!required) {
    dDebug("node:%s, no need to start", pMgmt->name);
  } else {
    dDebug("node:%s, need to start", pMgmt->name);
  }
  return required;
}

static int32_t dndOpenNode(SMgmtWrapper *pWrapper) { return (*pWrapper->fp.openFp)(pWrapper); }

static void dndCloseNode(SMgmtWrapper *pWrapper) {
  if (pWrapper->required) {
    (*pWrapper->fp.closeFp)(pWrapper);
  }
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
  tfree(pDnode);
  dDebug("dnode object memory is cleared, data:%p", pDnode);
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

  memcpy(&pDnode->cfg, pCfg, sizeof(SDndCfg));
  dndSetStatus(pDnode, DND_STAT_INIT);
  pDnode->rebootTime = taosGetTimestampMs();
  pDnode->pLockFile = dndCheckRunning(pCfg->dataDir);
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
    snprintf(path, sizeof(path), "%s%s%s", pCfg->dataDir, TD_DIRSEP, pDnode->wrappers[n].name);
    pWrapper->path = strdup(path);
    pWrapper->pDnode = pDnode;
    if (pDnode->wrappers[n].path == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      goto _OVER;
    }

    pWrapper->procType = PROC_SINGLE;
    pWrapper->required = dndRequireNode(pWrapper);
    if (pWrapper->required) {
      if (taosMkDir(pWrapper->path) != 0) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        dError("failed to create dir:%s since %s", pWrapper->path, terrstr());
        goto _OVER;
      }
    }
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

static int32_t dndRunInSingleProcess(SDnode *pDnode) {
  dInfo("dnode run in single process mode");

  for (ENodeType n = 0; n < NODE_MAX; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    if (!pWrapper->required) continue;

    dInfo("node:%s, will start in single process", pWrapper->name);
    if (dndOpenNode(pWrapper) != 0) {
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

static int32_t dndRunInMultiProcess(SDnode *pDnode) {
  for (ENodeType n = 0; n < NODE_MAX; ++n) {
    SMgmtWrapper *pWrapper = &pDnode->wrappers[n];
    if (!pWrapper->required) continue;

    if (n == DNODE) {
      dInfo("node:%s, will start in parent process", pWrapper->name);
      pWrapper->procType = PROC_PARENT;
      if (dndOpenNode(pWrapper) != 0) {
        dError("node:%s, failed to start since %s", pWrapper->name, terrstr());
        return -1;
      }
      continue;
    }

    SProcCfg  cfg = {0};
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
  }

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

  while (1) {
    if (pDnode->event == DND_EVENT_STOP) {
      dInfo("dnode is about to stop");
      break;
    }
    taosMsleep(100);
  }

  return 0;
}

void dndeHandleEvent(SDnode *pDnode, EDndEvent event) {
  dInfo("dnode object receive event %d, data:%p", event, pDnode);
  pDnode->event = event;
}

static int32_t dndBuildMsg(SNodeMsg *pMsg, SRpcMsg *pRpc, SEpSet *pEpSet) {
  SRpcConnInfo connInfo = {0};
  if ((pRpc->msgType & 1U) && rpcGetConnInfo(pRpc->handle, &connInfo) != 0) {
    terrno = TSDB_CODE_MND_NO_USER_FROM_CONN;
    dError("failed to build msg since %s, app:%p RPC:%p", terrstr(), pRpc->ahandle, pRpc->handle);
    return -1;
  }

  memcpy(pMsg->user, connInfo.user, TSDB_USER_LEN);
  pMsg->rpcMsg = *pRpc;

  return 0;
}

static void dndSendRpcRsp(SDnode *pDnode, SRpcMsg *pRpc) {
  if (pRpc->code == TSDB_CODE_APP_NOT_READY) {
    dmSendRedirectRsp(pDnode, pRpc);
  } else {
    rpcSendResponse(pRpc);
  }
}

void dndProcessRpcMsg(SMgmtWrapper *pWrapper, SRpcMsg *pRpc, SEpSet *pEpSet) {
  if (pEpSet && pEpSet->numOfEps > 0 && pRpc->msgType == TDMT_MND_STATUS_RSP) {
    dmUpdateMnodeEpSet(pWrapper->pDnode, pEpSet);
  }

  int32_t   code = -1;
  SNodeMsg *pMsg = NULL;

  NodeMsgFp msgFp = pWrapper->msgFps[TMSG_INDEX(pRpc->msgType)];
  if (msgFp == NULL) {
    terrno = TSDB_CODE_MSG_NOT_PROCESSED;
    goto _OVER;
  }

  pMsg = taosAllocateQitem(sizeof(SNodeMsg));
  if (pMsg == NULL) {
    goto _OVER;
  }

  if (dndBuildMsg(pMsg, pRpc, pEpSet) != 0) {
    goto _OVER;
  }

  dTrace("msg:%p, is created, app:%p user:%s", pMsg, pRpc->ahandle, pMsg->user);
  code = (*msgFp)(pWrapper, pMsg);

_OVER:

  if (code != 0) {
    dError("msg:%p, failed to process since %s", pMsg, terrstr());
    bool isReq = (pRpc->msgType & 1U);
    if (isReq) {
      SRpcMsg rsp = {.handle = pRpc->handle, .ahandle = pRpc->ahandle, .code = terrno};
      dndSendRpcRsp(pWrapper->pDnode, &rsp);
    }
    dTrace("msg:%p, is freed", pMsg);
    taosFreeQitem(pMsg);
    rpcFreeCont(pRpc->pCont);
  }
}