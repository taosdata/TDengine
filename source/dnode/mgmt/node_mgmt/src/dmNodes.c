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

int32_t dmOpenNode(SMgmtWrapper *pWrapper) {
  SDnode *pDnode = pWrapper->pDnode;

  if (taosMkDir(pWrapper->path) != 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    dError("node:%s, failed to create dir:%s since %s", pWrapper->name, pWrapper->path, terrstr());
    return -1;
  }

  SMgmtOutputOpt output = {0};
  SMgmtInputOpt  input = dmBuildMgmtInputOpt(pWrapper);

  dInfo("node:%s, start to open", pWrapper->name);
  tmsgSetDefault(&input.msgCb);
  if ((*pWrapper->func.openFp)(&input, &output) != 0) {
    dError("node:%s, failed to open since %s", pWrapper->name, terrstr());
    return -1;
  }
  dInfo("node:%s, has been opened", pWrapper->name);
  pWrapper->deployed = true;

  if (output.pMgmt != NULL) {
    pWrapper->pMgmt = output.pMgmt;
  }

  dmReportStartup(pWrapper->name, "opened");
  return 0;
}

int32_t dmStartNode(SMgmtWrapper *pWrapper) {
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

  taosThreadRwlockWrlock(&pWrapper->lock);
  if (pWrapper->pMgmt != NULL) {
    (*pWrapper->func.closeFp)(pWrapper->pMgmt);
    pWrapper->pMgmt = NULL;
  }
  taosThreadRwlockUnlock(&pWrapper->lock);

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

  dInfo("The daemon initialized successfully");
  dmReportStartup("The daemon", "initialized successfully");
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

int32_t dmRunDnode(SDnode *pDnode) {
  int32_t count = 0;
  if (dmOpenNodes(pDnode) != 0) {
    dError("failed to open nodes since %s", terrstr());
    dmCloseNodes(pDnode);
    return -1;
  }

  if (dmStartNodes(pDnode) != 0) {
    dError("failed to start nodes since %s", terrstr());
    dmSetStatus(pDnode, DND_STAT_STOPPED);
    dmStopNodes(pDnode);
    dmCloseNodes(pDnode);
    return -1;
  }

  while (1) {
    if (pDnode->stop) {
      dInfo("The daemon is about to stop");
      dmSetStatus(pDnode, DND_STAT_STOPPED);
      dmStopNodes(pDnode);
      dmCloseNodes(pDnode);
      return 0;
    }

    if (count == 10) {
      osUpdate();
      count = 0;
    } else {
      count++;
    }

    taosMsleep(100);
  }
}
