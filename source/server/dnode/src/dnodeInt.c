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
#include "dnodeCheck.h"
#include "dnodeEps.h"
#include "dnodeMsg.h"
#include "dnodeTrans.h"
#include "mnode.h"
#include "sync.h"
#include "tcache.h"
#include "tconfig.h"
#include "tnote.h"
#include "tstep.h"
#include "vnode.h"
#include "wal.h"

static struct {
  EDnStat      runStatus;
  SStartupStep startup;
  SSteps      *steps;
} tsDnode;

EDnStat dnodeGetRunStat() { return tsDnode.runStatus; }

void dnodeSetRunStat(EDnStat stat) { tsDnode.runStatus = stat; }

void dnodeReportStartup(char *name, char *desc) {
  SStartupStep *startup = &tsDnode.startup;
  tstrncpy(startup->name, name, strlen(startup->name));
  tstrncpy(startup->desc, desc, strlen(startup->desc));
  startup->finished = 0;
}

static void dnodeReportStartupFinished(char *name, char *desc) {
  SStartupStep *startup = &tsDnode.startup;
  tstrncpy(startup->name, name, strlen(startup->name));
  tstrncpy(startup->desc, desc, strlen(startup->desc));
  startup->finished = 1;
}

void dnodeGetStartup(SStartupStep *pStep) { memcpy(pStep, &tsDnode.startup, sizeof(SStartupStep)); }

static int32_t dnodeInitVnode() {
  SVnodePara para;
  para.fp.GetDnodeEp = dnodeGetEp;
  para.fp.SendMsgToDnode = dnodeSendMsgToDnode;
  para.fp.SendMsgToMnode = dnodeSendMsgToMnode;
  para.fp.ReportStartup = dnodeReportStartup;

  return vnodeInit(para);
}

static int32_t dnodeInitMnode() {
  SMnodePara para;
  para.fp.GetDnodeEp = dnodeGetEp;
  para.fp.SendMsgToDnode = dnodeSendMsgToDnode;
  para.fp.SendMsgToMnode = dnodeSendMsgToMnode;
  para.fp.SendRedirectMsg = dnodeSendRedirectMsg;
  para.dnodeId = dnodeGetDnodeId();
  para.clusterId = dnodeGetClusterId();

  return mnodeInit(para);
}

static int32_t dnodeInitTfs() {}

static int32_t dnodeInitMain() {
  tsDnode.runStatus = DN_RUN_STAT_STOPPED;
  tscEmbedded = 1;
  taosIgnSIGPIPE();
  taosBlockSIGPIPE();
  taosResolveCRC();
  taosInitGlobalCfg();
  taosReadGlobalLogCfg();
  taosSetCoreDump(tsEnableCoreFile);

  if (!taosMkDir(tsLogDir)) {
    printf("failed to create dir: %s, reason: %s\n", tsLogDir, strerror(errno));
    return -1;
  }

  char temp[TSDB_FILENAME_LEN];
  sprintf(temp, "%s/taosdlog", tsLogDir);
  if (taosInitLog(temp, tsNumOfLogLines, 1) < 0) {
    printf("failed to init log file\n");
  }

  if (!taosReadGlobalCfg()) {
    taosPrintGlobalCfg();
    dError("TDengine read global config failed");
    return -1;
  }

  dInfo("start to initialize TDengine");

  taosInitNotes();

  return taosCheckGlobalCfg();
}

static void dnodeCleanupMain() {
  taos_cleanup();
  taosCloseLog();
  taosStopCacheRefreshWorker();
}

static int32_t dnodeCheckRunning(char *dir) {
  char filepath[256] = {0};
  snprintf(filepath, sizeof(filepath), "%s/.running", dir);

  FileFd fd = taosOpenFileCreateWriteTrunc(filepath);
  if (fd < 0) {
    dError("failed to open lock file:%s since %s, quit", filepath, strerror(errno));
    return -1;
  }

  int32_t ret = taosLockFile(fd);
  if (ret != 0) {
    dError("failed to lock file:%s since %s, quit", filepath, strerror(errno));
    taosCloseFile(fd);
    return -1;
  }

  return 0;
}

static int32_t dnodeInitDir() {
  sprintf(tsMnodeDir, "%s/mnode", tsDataDir);
  sprintf(tsVnodeDir, "%s/vnode", tsDataDir);
  sprintf(tsDnodeDir, "%s/dnode", tsDataDir);

  if (!taosMkDir(tsDnodeDir)) {
    dError("failed to create dir:%s since %s", tsDnodeDir, strerror(errno));
    return -1;
  }

  if (!taosMkDir(tsMnodeDir)) {
    dError("failed to create dir:%s since %s", tsMnodeDir, strerror(errno));
    return -1;
  }

  if (!taosMkDir(tsVnodeDir)) {
    dError("failed to create dir:%s since %s", tsVnodeDir, strerror(errno));
    return -1;
  }

  if (dnodeCheckRunning(tsDnodeDir) != 0) {
    return -1;
  }

  return 0;
}

static void dnodeCleanupDir() {}

int32_t dnodeInit() {
  SSteps *steps = taosStepInit(24, dnodeReportStartup);
  if (steps == NULL) return -1;

  taosStepAdd(steps, "dnode-main", dnodeInitMain, dnodeCleanupMain);
  taosStepAdd(steps, "dnode-dir", dnodeInitDir, dnodeCleanupDir);
  taosStepAdd(steps, "dnode-check", dnodeInitCheck, dnodeCleanupCheck);
  taosStepAdd(steps, "dnode-rpc", rpcInit, rpcCleanup);
  taosStepAdd(steps, "dnode-tfs", dnodeInitTfs, NULL);
  taosStepAdd(steps, "dnode-wal", walInit, walCleanUp);
  taosStepAdd(steps, "dnode-sync", syncInit, syncCleanUp);
  taosStepAdd(steps, "dnode-eps", dnodeInitEps, dnodeCleanupEps);
  taosStepAdd(steps, "dnode-vnode", dnodeInitVnode, vnodeCleanup);
  taosStepAdd(steps, "dnode-mnode", dnodeInitMnode, mnodeCleanup);
  taosStepAdd(steps, "dnode-trans", dnodeInitTrans, dnodeCleanupTrans);
  taosStepAdd(steps, "dnode-msg", dnodeInitMsg, dnodeCleanupMsg);

  tsDnode.steps = steps;
  taosStepExec(tsDnode.steps);

  dnodeSetRunStat(DN_RUN_STAT_RUNNING);
  dnodeReportStartupFinished("TDengine", "initialized successfully");
  dInfo("TDengine is initialized successfully");

  return 0;
}

void dnodeCleanup() {
  if (dnodeGetRunStat() != DN_RUN_STAT_STOPPED) {
    dnodeSetRunStat(DN_RUN_STAT_STOPPED);
    taosStepCleanup(tsDnode.steps);
    tsDnode.steps = NULL;
  }
}
