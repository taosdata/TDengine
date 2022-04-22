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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "mmInt.h"
#include "wal.h"

static bool mmDeployRequired(SDnode *pDnode) {
  if (pDnode->data.dnodeId > 0) return false;
  if (pDnode->data.clusterId > 0) return false;
  if (strcmp(pDnode->data.localEp, pDnode->data.firstEp) != 0) return false;
  return true;
}

static int32_t mmRequire(SMgmtWrapper *pWrapper, bool *required) {
  SMnodeMgmt mgmt = {0};
  mgmt.path = pWrapper->path;
  if (mmReadFile(&mgmt, required) != 0) {
    return -1;
  }

  if (!(*required)) {
    *required = mmDeployRequired(pWrapper->pDnode);
  }

  return 0;
}

static void mmInitOption(SMnodeMgmt *pMgmt, SMnodeOpt *pOption) {
  SMsgCb msgCb = pMgmt->pDnode->data.msgCb;
  msgCb.pWrapper = pMgmt->pWrapper;
  msgCb.queueFps[QUERY_QUEUE] = mmPutMsgToQueryQueue;
  msgCb.queueFps[READ_QUEUE] = mmPutMsgToReadQueue;
  msgCb.queueFps[WRITE_QUEUE] = mmPutMsgToWriteQueue;
  msgCb.queueFps[SYNC_QUEUE] = mmPutMsgToWriteQueue;
  pOption->msgCb = msgCb;
}

static void mmBuildOptionForDeploy(SMnodeMgmt *pMgmt, SMnodeOpt *pOption) {
  mmInitOption(pMgmt, pOption);
  pOption->replica = 1;
  pOption->selfIndex = 0;
  SReplica *pReplica = &pOption->replicas[0];
  pReplica->id = 1;
  pReplica->port = pMgmt->pDnode->data.serverPort;
  tstrncpy(pReplica->fqdn, pMgmt->pDnode->data.localFqdn, TSDB_FQDN_LEN);
  pOption->deploy = true;

  pMgmt->selfIndex = pOption->selfIndex;
  pMgmt->replica = pOption->replica;
  memcpy(&pMgmt->replicas, pOption->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
}

static void mmBuildOptionForOpen(SMnodeMgmt *pMgmt, SMnodeOpt *pOption) {
  mmInitOption(pMgmt, pOption);
  pOption->selfIndex = pMgmt->selfIndex;
  pOption->replica = pMgmt->replica;
  memcpy(&pOption->replicas, pMgmt->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
  pOption->deploy = false;
}

static int32_t mmBuildOptionFromReq(SMnodeMgmt *pMgmt, SMnodeOpt *pOption, SDCreateMnodeReq *pCreate) {
  mmInitOption(pMgmt, pOption);

  pOption->replica = pCreate->replica;
  pOption->selfIndex = -1;
  for (int32_t i = 0; i < pCreate->replica; ++i) {
    SReplica *pReplica = &pOption->replicas[i];
    pReplica->id = pCreate->replicas[i].id;
    pReplica->port = pCreate->replicas[i].port;
    memcpy(pReplica->fqdn, pCreate->replicas[i].fqdn, TSDB_FQDN_LEN);
    if (pReplica->id == pMgmt->pDnode->data.dnodeId) {
      pOption->selfIndex = i;
    }
  }

  if (pOption->selfIndex == -1) {
    dError("failed to build mnode options since %s", terrstr());
    return -1;
  }
  pOption->deploy = true;

  pMgmt->selfIndex = pOption->selfIndex;
  pMgmt->replica = pOption->replica;
  memcpy(&pMgmt->replicas, pOption->replicas, sizeof(SReplica) * TSDB_MAX_REPLICA);
  return 0;
}

int32_t mmAlter(SMnodeMgmt *pMgmt, SDAlterMnodeReq *pReq) {
  SMnodeOpt option = {0};
  if (mmBuildOptionFromReq(pMgmt, &option, pReq) != 0) {
    return -1;
  }

  if (mndAlter(pMgmt->pMnode, &option) != 0) {
    return -1;
  }

  bool deployed = true;
  if (mmWriteFile(pMgmt->pWrapper, pReq, deployed) != 0) {
    dError("failed to write mnode file since %s", terrstr());
    return -1;
  }

  return 0;
}

static void mmClose(SMgmtWrapper *pWrapper) {
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt == NULL) return;

  dInfo("mnode-mgmt start to cleanup");
  if (pMgmt->pMnode != NULL) {
    mmStopWorker(pMgmt);
    mndClose(pMgmt->pMnode);
    pMgmt->pMnode = NULL;
  }

  pWrapper->pMgmt = NULL;
  taosMemoryFree(pMgmt);
  dInfo("mnode-mgmt is cleaned up");
}

static int32_t mmOpen(SMgmtWrapper *pWrapper) {
  dInfo("mnode-mgmt start to init");
  if (walInit() != 0) {
    dError("failed to init wal since %s", terrstr());
    return -1;
  }

  SMnodeMgmt *pMgmt = taosMemoryCalloc(1, sizeof(SMnodeMgmt));
  if (pMgmt == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  pMgmt->path = pWrapper->path;
  pMgmt->pDnode = pWrapper->pDnode;
  pMgmt->pWrapper = pWrapper;
  pWrapper->pMgmt = pMgmt;

  bool deployed = false;
  if (mmReadFile(pMgmt, &deployed) != 0) {
    dError("failed to read file since %s", terrstr());
    mmClose(pWrapper);
    return -1;
  }

  SMnodeOpt option = {0};
  if (!deployed) {
    dInfo("mnode start to deploy");
    if (pWrapper->procType == DND_PROC_CHILD) {
      pWrapper->pDnode->data.dnodeId = 1;
    }
    mmBuildOptionForDeploy(pMgmt, &option);
  } else {
    dInfo("mnode start to open");
    mmBuildOptionForOpen(pMgmt, &option);
  }

  pMgmt->pMnode = mndOpen(pMgmt->path, &option);
  if (pMgmt->pMnode == NULL) {
    dError("failed to open mnode since %s", terrstr());
    mmClose(pWrapper);
    return -1;
  }
  dmReportStartup(pWrapper->pDnode, "mnode-impl", "initialized");

  if (mmStartWorker(pMgmt) != 0) {
    dError("failed to start mnode worker since %s", terrstr());
    mmClose(pWrapper);
    return -1;
  }
  dmReportStartup(pWrapper->pDnode, "mnode-worker", "initialized");

  if (!deployed) {
    deployed = true;
    if (mmWriteFile(pWrapper, NULL, deployed) != 0) {
      dError("failed to write mnode file since %s", terrstr());
      return -1;
    }
  }

  dInfo("mnode-mgmt is initialized");
  return 0;
}

static int32_t mmStart(SMgmtWrapper *pWrapper) {
  dDebug("mnode-mgmt start to run");
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  return mndStart(pMgmt->pMnode);
}

static void mmStop(SMgmtWrapper *pWrapper) {
  dDebug("mnode-mgmt start to stop");
  SMnodeMgmt *pMgmt = pWrapper->pMgmt;
  if (pMgmt != NULL) {
    mndStop(pMgmt->pMnode);
  }
}

void mmSetMgmtFp(SMgmtWrapper *pWrapper) {
  SMgmtFp mgmtFp = {0};
  mgmtFp.openFp = mmOpen;
  mgmtFp.closeFp = mmClose;
  mgmtFp.startFp = mmStart;
  mgmtFp.stopFp = mmStop;
  mgmtFp.createFp = mmProcessCreateReq;
  mgmtFp.dropFp = mmProcessDropReq;
  mgmtFp.requiredFp = mmRequire;

  mmInitMsgHandle(pWrapper);
  pWrapper->name = "mnode";
  pWrapper->fp = mgmtFp;
}
