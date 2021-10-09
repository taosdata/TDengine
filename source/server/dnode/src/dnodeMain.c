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
#include "tcache.h"
#include "tconfig.h"
#include "tglobal.h"
#if 0
#include "tfs.h"
#endif
#include "tnote.h"
#include "tcompression.h"
#include "ttimer.h"
#include "dnodeCfg.h"
#include "dnodeMain.h"
#include "mnode.h"

static struct {
  RunStat      runStatus;
  void *       dnodeTimer;
  SStartupStep startup;
} tsDmain;

static void dnodeCheckDataDirOpenned(char *dir) {
#if 0
  char filepath[256] = {0};
  snprintf(filepath, sizeof(filepath), "%s/.running", dir);

  int32_t fd = open(filepath, O_WRONLY | O_CREAT | O_TRUNC, S_IRWXU | S_IRWXG | S_IRWXO);
  if (fd < 0) {
    dError("failed to open lock file:%s, reason: %s, quit", filepath, strerror(errno));
    exit(0);
  }

  int32_t ret = flock(fd, LOCK_EX | LOCK_NB);
  if (ret != 0) {
    dError("failed to lock file:%s ret:%d since %s, database may be running, quit", filepath, ret, strerror(errno));
    close(fd);
    exit(0);
  }
#endif
}

int32_t dnodeInitMain() {
  tsDmain.runStatus = TD_RUN_STAT_STOPPED;
  tsDmain.dnodeTimer = taosTmrInit(100, 200, 60000, "DND-TMR");
  if (tsDmain.dnodeTimer == NULL) {
    dError("failed to init dnode timer");
    return -1;
  }

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

void dnodeCleanupMain() {
  if (tsDmain.dnodeTimer != NULL) {
    taosTmrCleanUp(tsDmain.dnodeTimer);
    tsDmain.dnodeTimer = NULL;
  }

#if 0
  taos_cleanup();
#endif
  taosCloseLog();
  taosStopCacheRefreshWorker();
}

int32_t dnodeInitStorage() {
#ifdef TD_TSZ
  // compress module init
  tsCompressInit();
#endif

  // storage module init
  if (tsDiskCfgNum == 1 && !taosMkDir(tsDataDir)) {
    dError("failed to create dir:%s since %s", tsDataDir, strerror(errno));
    return -1;
  }

#if 0
  if (tfsInit(tsDiskCfg, tsDiskCfgNum) < 0) {
    dError("failed to init TFS since %s", tstrerror(terrno));
    return -1;
  }

  strncpy(tsDataDir, TFS_PRIMARY_PATH(), TSDB_FILENAME_LEN);
#endif
  sprintf(tsMnodeDir, "%s/mnode", tsDataDir);
  sprintf(tsVnodeDir, "%s/vnode", tsDataDir);
  sprintf(tsDnodeDir, "%s/dnode", tsDataDir);

  if (!taosMkDir(tsMnodeDir)) {
    dError("failed to create dir:%s since %s", tsMnodeDir, strerror(errno));
    return -1;
  }

  if (!taosMkDir(tsDnodeDir)) {
    dError("failed to create dir:%s since %s", tsDnodeDir, strerror(errno));
    return -1;
  }

#if 0
  if (tfsMkdir("vnode") < 0) {
    dError("failed to create vnode dir since %s", tstrerror(terrno));
    return -1;
  }

  if (tfsMkdir("vnode_bak") < 0) {
    dError("failed to create vnode_bak dir since %s", tstrerror(terrno));
    return -1;
  }

  TDIR *tdir = tfsOpendir("vnode_bak/.staging");
  bool  stagingNotEmpty = tfsReaddir(tdir) != NULL;
  tfsClosedir(tdir);

  if (stagingNotEmpty) {
    dError("vnode_bak/.staging dir not empty, fix it first.");
    return -1;
  }

  if (tfsMkdir("vnode_bak/.staging") < 0) {
    dError("failed to create vnode_bak/.staging dir since %s", tstrerror(terrno));
    return -1;
  }

  dnodeCheckDataDirOpenned(tsDnodeDir);

  taosGetDisk();
  dnodePrintDiskInfo();
#endif  

  dInfo("dnode storage is initialized at %s", tsDnodeDir);
  return 0;
}

void dnodeCleanupStorage() {
#if 0
  // storage destroy
  tfsDestroy();

 #ifdef TD_TSZ
  // compress destroy
  tsCompressExit();
 #endif
#endif
}

void dnodeReportStartup(char *name, char *desc) {
  SStartupStep *startup = &tsDmain.startup;
  tstrncpy(startup->name, name, strlen(startup->name));
  tstrncpy(startup->desc, desc, strlen(startup->desc));
  startup->finished = 0;
}

void dnodeReportStartupFinished(char *name, char *desc) {
  SStartupStep *startup = &tsDmain.startup;
  tstrncpy(startup->name, name, strlen(startup->name));
  tstrncpy(startup->desc, desc, strlen(startup->desc));
  startup->finished = 1;
}

void dnodeProcessStartupReq(SRpcMsg *pMsg) {
  dInfo("startup msg is received, cont:%s", (char *)pMsg->pCont);

  SStartupStep *pStep = rpcMallocCont(sizeof(SStartupStep));
  memcpy(pStep, &tsDmain.startup, sizeof(SStartupStep));

  dDebug("startup msg is sent, step:%s desc:%s finished:%d", pStep->name, pStep->desc, pStep->finished);

  SRpcMsg rpcRsp = {.handle = pMsg->handle, .pCont = pStep, .contLen = sizeof(SStartupStep)};
  rpcSendResponse(&rpcRsp);
  rpcFreeCont(pMsg->pCont);
}

static int32_t dnodeStartMnode(SRpcMsg *pMsg) {
  SCreateMnodeMsg *pCfg = pMsg->pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  if (pCfg->dnodeId != dnodeGetDnodeId()) {
    dDebug("dnode:%d, in create meps msg is not equal with saved dnodeId:%d", pCfg->dnodeId,
           dnodeGetDnodeId());
    return TSDB_CODE_MND_DNODE_ID_NOT_CONFIGURED;
  }

  if (strcmp(pCfg->dnodeEp, tsLocalEp) != 0) {
    dDebug("dnodeEp:%s, in create meps msg is not equal with saved dnodeEp:%s", pCfg->dnodeEp, tsLocalEp);
    return TSDB_CODE_MND_DNODE_EP_NOT_CONFIGURED;
  }

  dDebug("dnode:%d, create meps msg is received from mnodes, numOfMnodes:%d", pCfg->dnodeId, pCfg->mnodes.mnodeNum);
  for (int32_t i = 0; i < pCfg->mnodes.mnodeNum; ++i) {
    pCfg->mnodes.mnodeInfos[i].mnodeId = htonl(pCfg->mnodes.mnodeInfos[i].mnodeId);
    dDebug("meps index:%d, meps:%d:%s", i, pCfg->mnodes.mnodeInfos[i].mnodeId, pCfg->mnodes.mnodeInfos[i].mnodeEp);
  }

  if (mnodeIsServing()) return 0;

  return mnodeDeploy(&pCfg->mnodes);
}

void dnodeProcessCreateMnodeReq(SRpcMsg *pMsg) {
  int32_t code = dnodeStartMnode(pMsg);

  SRpcMsg rspMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0, .code = code};

  rpcSendResponse(&rspMsg);
  rpcFreeCont(pMsg->pCont);
}

void dnodeProcessConfigDnodeReq(SRpcMsg *pMsg) {
  SCfgDnodeMsg *pCfg = pMsg->pCont;

  int32_t code = taosCfgDynamicOptions(pCfg->config);

  SRpcMsg rspMsg = {.handle = pMsg->handle, .pCont = NULL, .contLen = 0, .code = code};

  rpcSendResponse(&rspMsg);
  rpcFreeCont(pMsg->pCont);
}

RunStat dnodeGetRunStat() { return tsDmain.runStatus; }

void dnodeSetRunStat(RunStat stat) { tsDmain.runStatus = stat; }

void* dnodeGetTimer() { return tsDmain.dnodeTimer; }