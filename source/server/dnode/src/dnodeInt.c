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
#include "dnodeTrans.h"
#include "mnode.h"
#include "vnode.h"

static struct SSteps *tsSteps;

static int32_t dnodeInitVnodeModule(void **unused) {
  SVnodePara para;
  para.fp.GetDnodeEp = dnodeGetDnodeEp;
  para.fp.SendMsgToDnode = dnodeSendMsgToDnode;
  para.fp.SendMsgToMnode = dnodeSendMsgToMnode;

  return vnodeInit(para);
}

static int32_t dnodeInitMnodeModule(void **unused) {
  SMnodePara para;
  para.fp.GetDnodeEp = dnodeGetDnodeEp;
  para.fp.SendMsgToDnode = dnodeSendMsgToDnode;
  para.fp.SendMsgToMnode = dnodeSendMsgToMnode;
  para.fp.SendRedirectMsg = dnodeSendRedirectMsg;
  dnodeGetCfg(&para.dnodeId, para.clusterId);

  return mnodeInit(para);
}

int32_t dnodeInit() {
  tsSteps = taosStepInit(24, dnodeReportStartup);
  if (tsSteps == NULL) return -1;

  taosStepAdd(tsSteps, "dnode-main", dnodeInitMain, dnodeCleanupMain);
  taosStepAdd(tsSteps, "dnode-storage", dnodeInitStorage, dnodeCleanupStorage);
  //taosStepAdd(tsSteps, "dnode-tfs", tfInit, tfCleanup);
  taosStepAdd(tsSteps, "dnode-rpc", rpcInit, rpcCleanup);
  taosStepAdd(tsSteps, "dnode-check", dnodeInitCheck, dnodeCleanupCheck);
  taosStepAdd(tsSteps, "dnode-cfg", dnodeInitCfg, dnodeCleanupCfg);
  taosStepAdd(tsSteps, "dnode-deps", dnodeInitEps, dnodeCleanupEps);
  taosStepAdd(tsSteps, "dnode-meps", dnodeInitMnodeEps, dnodeCleanupMnodeEps);
  //taosStepAdd(tsSteps, "dnode-wal", walInit, walCleanUp);
  //taosStepAdd(tsSteps, "dnode-sync", syncInit, syncCleanUp);
  taosStepAdd(tsSteps, "dnode-vnode", dnodeInitVnodeModule, vnodeCleanup);
  taosStepAdd(tsSteps, "dnode-mnode", dnodeInitMnodeModule, mnodeCleanup);
  taosStepAdd(tsSteps, "dnode-trans", dnodeInitTrans, dnodeCleanupTrans);
  taosStepAdd(tsSteps, "dnode-status", dnodeInitStatus, dnodeCleanupStatus);
  //taosStepAdd(tsSteps, "dnode-script",scriptEnvPoolInit, scriptEnvPoolCleanup);

  taosStepExec(tsSteps);

  dnodeSetRunStat(DN_RUN_STAT_RUNNING);
  dnodeReportStartupFinished("TDengine", "initialized successfully");
  dInfo("TDengine is initialized successfully");

  return 0;
}

void dnodeCleanup() {
  if (dnodeGetRunStat() != DN_RUN_STAT_STOPPED) {
    dnodeSetRunStat(DN_RUN_STAT_STOPPED);
    taosStepCleanup(tsSteps);
    tsSteps = NULL;
  }
}
