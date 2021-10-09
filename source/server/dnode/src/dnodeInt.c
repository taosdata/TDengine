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
#include "os.h"
#if 0
#include "qScript.h"
#include "tfile.h"
#include "tsync.h"
#include "twal.h"
#endif
#include "tstep.h"
#include "dnodeCfg.h"
#include "dnodeCheck.h"
#include "dnodeEps.h"
#include "dnodeMain.h"
#include "dnodeMnodeEps.h"
#include "dnodeStatus.h"
#include "dnodeTelem.h"
#include "dnodeTrans.h"
#include "mnode.h"
#include "vnode.h"

SDnode *dnodeInst() {
  static SDnode inst = {0};
  return &inst;
}

static int32_t dnodeInitVnodeModule(void **unused) {
  SVnodePara para;
  para.fp.GetDnodeEp = dnodeGetDnodeEp;
  para.fp.SendMsgToDnode = dnodeSendMsgToDnode;
  para.fp.SendMsgToMnode = dnodeSendMsgToMnode;

  return vnodeInit(para);
}

static int32_t dnodeInitMnodeModule(void **unused) {
  SDnode *dnode = dnodeInst();

  SMnodePara para;
  para.fp.GetDnodeEp = dnodeGetDnodeEp;
  para.fp.SendMsgToDnode = dnodeSendMsgToDnode;
  para.fp.SendMsgToMnode = dnodeSendMsgToMnode;
  para.fp.SendRedirectMsg = dnodeSendRedirectMsg;
  para.dnodeId = dnode->cfg->dnodeId;
  strncpy(para.clusterId, dnode->cfg->clusterId, sizeof(para.clusterId));

  return mnodeInit(para);
}

int32_t dnodeInit() {
  struct SSteps *steps = taosStepInit(24, dnodeReportStartup);
  if (steps == NULL) return -1;

  SDnode *dnode = dnodeInst();

  taosStepAdd(steps, "dnode-main", (void **)&dnode->main, (InitFp)dnodeInitMain, (CleanupFp)dnodeCleanupMain);
  taosStepAdd(steps, "dnode-storage", NULL, (InitFp)dnodeInitStorage, (CleanupFp)dnodeCleanupStorage);
  //taosStepAdd(steps, "dnode-tfs", NULL, (InitFp)tfInit, (CleanupFp)tfCleanup);
  taosStepAdd(steps, "dnode-rpc", NULL, (InitFp)rpcInit, (CleanupFp)rpcCleanup);
  taosStepAdd(steps, "dnode-check", (void **)&dnode->check, (InitFp)dnodeInitCheck, (CleanupFp)dnodeCleanupCheck);
  taosStepAdd(steps, "dnode-cfg", (void **)&dnode->cfg, (InitFp)dnodeInitCfg, (CleanupFp)dnodeCleanupCfg);
  taosStepAdd(steps, "dnode-deps", (void **)&dnode->eps, (InitFp)dnodeInitEps, (CleanupFp)dnodeCleanupEps);
  taosStepAdd(steps, "dnode-meps", (void **)&dnode->meps, (InitFp)dnodeInitMnodeEps, (CleanupFp)dnodeCleanupMnodeEps);
  //taosStepAdd(steps, "dnode-wal", NULL, (InitFp)walInit, (CleanupFp)walCleanUp);
  //taosStepAdd(steps, "dnode-sync", NULL, (InitFp)syncInit, (CleanupFp)syncCleanUp);
  taosStepAdd(steps, "dnode-vnode", NULL, (InitFp)dnodeInitVnodeModule, (CleanupFp)vnodeCleanup);
  taosStepAdd(steps, "dnode-mnode", NULL, (InitFp)dnodeInitMnodeModule, (CleanupFp)mnodeCleanup);
  taosStepAdd(steps, "dnode-trans", (void **)&dnode->trans, (InitFp)dnodeInitTrans, (CleanupFp)dnodeCleanupTrans);
  taosStepAdd(steps, "dnode-status", (void **)&dnode->status, (InitFp)dnodeInitStatus, (CleanupFp)dnodeCleanupStatus);
  taosStepAdd(steps, "dnode-telem", (void **)&dnode->telem, (InitFp)dnodeInitTelem, (CleanupFp)dnodeCleanupTelem);
  //taosStepAdd(steps, "dnode-script", NULL, (InitFp)scriptEnvPoolInit, (CleanupFp)scriptEnvPoolCleanup);

  dnode->steps = steps;
  taosStepExec(dnode->steps);

  if (dnode->main) {
    dnode->main->runStatus = TD_RUN_STAT_RUNNING;
    dnodeReportStartupFinished("TDengine", "initialized successfully");
    dInfo("TDengine is initialized successfully");
  }

  return 0;
}

void dnodeCleanup() {
  SDnode *dnode = dnodeInst();
  if (dnode->main->runStatus != TD_RUN_STAT_STOPPED) {
    dnode->main->runStatus = TD_RUN_STAT_STOPPED;
    taosStepCleanup(dnode->steps);
  }
}
