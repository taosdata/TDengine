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
#include "dnodeTelemetry.h"
#include "dnodeTrans.h"
#include "mnode.h"
#include "vnode.h"

static int32_t dnodeInitRpcEnv(Dnode *dnode, void **unUsed) { return rpcInit(); }
static void    dnodeCleanupRpcEnv(void **unUsed) { rpcCleanup(); }
#if 0
static int32_t dnodeInitTfsEnv(Dnode *dnode, void **unUsed) { return tfInit(); }
static void    dnodeCleanupTfsEnv(void **unUsed) { tfCleanup(); }
static int32_t dnodeInitScriptEnv(Dnode *dnode, void **unUsed) { return scriptEnvPoolInit(); }
static void    dnodeCleanupScriptEnv(void **unUsed) { scriptEnvPoolCleanup(); }
static int32_t dnodeInitWalEnv(Dnode *dnode, void **unUsed) { return walInit(); }
static void    dnodeCleanupWalEnv(void **unUsed) { walCleanUp(); }
static int32_t dnodeInitSyncEnv(Dnode *dnode, void **unUsed) { return syncInit(); }
static void    dnodeCleanupSyncEnv(void **unUsed) { syncCleanUp(); }
#endif

static int32_t dnodeInitVnodeModule(Dnode *dnode, struct Vnode** out) {
  SVnodePara para;
  para.fp.GetDnodeEp = dnodeGetDnodeEp;
  para.fp.SendMsgToDnode = dnodeSendMsgToDnode;
  para.fp.SendMsgToMnode = dnodeSendMsgToMnode;
  para.dnode = dnode;

  struct Vnode *vnode = vnodeCreateInstance(para);
  if (vnode == NULL) {
    return -1;
  }

  *out = vnode;
  return 0;
}

static void dnodeCleanupVnodeModule(Dnode *dnode, struct Vnode **out) {
  struct Vnode *vnode = *out;
  *out = NULL;
  vnodeDropInstance(vnode);
}

static int32_t dnodeInitMnodeModule(Dnode *dnode, struct Mnode **out) {
  SMnodePara para;
  para.fp.GetDnodeEp = dnodeGetDnodeEp;
  para.fp.SendMsgToDnode = dnodeSendMsgToDnode;
  para.fp.SendMsgToMnode = dnodeSendMsgToMnode;
  para.fp.SendRedirectMsg = dnodeSendRedirectMsg;
  para.dnode = dnode;
  para.dnodeId = dnode->cfg->dnodeId;
  strncpy(para.clusterId, dnode->cfg->clusterId, sizeof(para.clusterId));

  struct Mnode *mnode = mnodeCreateInstance(para);
  if (mnode == NULL) {
    return -1;
  }

  *out = mnode;
  return 0;
}

static void dnodeCleanupMnodeModule(Dnode *dnode, struct Mnode **out) {
  struct Mnode *mnode = *out;
  *out = NULL;
  mnodeDropInstance(mnode);
}

Dnode *dnodeCreateInstance() {
  Dnode *dnode = calloc(1, sizeof(Dnode));
  if (dnode == NULL) {
    return NULL;
  }

  SSteps *steps = taosStepInit(24);
  if (steps == NULL) {
    return NULL;
  }

  SStepObj step = {0};
  step.parent = dnode;
  
  step.name = "dnode-main";
  step.self = (void **)&dnode->main;
  step.initFp = (FnInitObj)dnodeInitMain;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupMain;
  step.reportFp = NULL;
  taosStepAdd(steps, &step);

  step.name = "dnode-storage";
  step.self = NULL;
  step.initFp = (FnInitObj)dnodeInitStorage;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupStorage;
  step.reportFp = (FnReportProgress)dnodeReportStartup;
  taosStepAdd(steps, &step);

#if 0
  step.name = "dnode-tfs-env";
  step.self = NULL;
  step.initFp = (FnInitObj)dnodeInitTfsEnv;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupTfsEnv;
  taosStepAdd(steps, &step);
#endif  

  step.name = "dnode-rpc-env";
  step.self = NULL;
  step.initFp = (FnInitObj)dnodeInitRpcEnv;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupRpcEnv;
  taosStepAdd(steps, &step);

  step.name = "dnode-check";
  step.self = (void **)&dnode->check;
  step.initFp = (FnInitObj)dnodeInitCheck;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupCheck;
  taosStepAdd(steps, &step);

  step.name = "dnode-cfg";
  step.self = (void **)&dnode->cfg;
  step.initFp = (FnInitObj)dnodeInitCfg;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupCfg;
  taosStepAdd(steps, &step);

  step.name = "dnode-deps";
  step.self = (void **)&dnode->eps;
  step.initFp = (FnInitObj)dnodeInitEps;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupEps;
  taosStepAdd(steps, &step);

  step.name = "dnode-meps";
  step.self = (void **)&dnode->meps;
  step.initFp = (FnInitObj)dnodeInitMnodeEps;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupMnodeEps;
  taosStepAdd(steps, &step);

#if 0
  step.name = "dnode-wal";
  step.self = NULL;
  step.initFp = (FnInitObj)dnodeInitWalEnv;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupWalEnv;
  taosStepAdd(steps, &step);

  step.name = "dnode-sync";
  step.self = NULL;
  step.initFp = (FnInitObj)dnodeInitSyncEnv;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupSyncEnv;
  taosStepAdd(steps, &step);
#endif  

  step.name = "dnode-vnode";
  step.self = (void **)&dnode->vnode;
  step.initFp = (FnInitObj)dnodeInitVnodeModule;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupVnodeModule;
  taosStepAdd(steps, &step);

  step.name = "dnode-mnode";
  step.self = (void **)&dnode->mnode;
  step.initFp = (FnInitObj)dnodeInitMnodeModule;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupMnodeModule;
  taosStepAdd(steps, &step);

  step.name = "dnode-trans";
  step.self = (void **)&dnode->trans;
  step.initFp = (FnInitObj)dnodeInitTrans;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupTrans;
  taosStepAdd(steps, &step);

  step.name = "dnode-status";
  step.self = (void **)&dnode->status;
  step.initFp = (FnInitObj)dnodeInitStatus;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupStatus;
  taosStepAdd(steps, &step);

  step.name = "dnode-telem";
  step.self = (void **)&dnode->telem;
  step.initFp = (FnInitObj)dnodeInitTelemetry;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupTelemetry;
  taosStepAdd(steps, &step);

#if 0
  step.name = "dnode-script";
  step.self = NULL;
  step.initFp = (FnInitObj)dnodeInitScriptEnv;
  step.cleanupFp = (FnCleanupObj)dnodeCleanupScriptEnv;
  taosStepAdd(steps, &step);
#endif

  dnode->steps = steps;
  taosStepExec(dnode->steps);

  if (dnode->main) {
    dnode->main->runStatus = TD_RUN_STAT_RUNNING;
    dnodeReportStartupFinished(dnode, "TDengine", "initialized successfully");
    dInfo("TDengine is initialized successfully");
  }

  return dnode;
}

void dnodeDropInstance(Dnode *dnode) {
  if (dnode->main->runStatus != TD_RUN_STAT_STOPPED) {
    dnode->main->runStatus = TD_RUN_STAT_STOPPED;
    taosStepCleanup(dnode->steps);
  }
}
