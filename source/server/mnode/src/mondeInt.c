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
#include "tglobal.h"
#include "tstep.h"
#include "mnodeInt.h"

#include "mnodeCluster.h"
#include "mnodeDb.h"
#include "mnodeDnode.h"
#include "mnodeFunc.h"
#include "mnodeMnode.h"
#include "mnodeProfile.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeTable.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"
#include "mnodeWorker.h"
#include "mnodeDb.h"
#include "mnodeRaft.h"

static struct {
  int32_t  state;
  int32_t  dnodeId;
  char     clusterId[TSDB_CLUSTER_ID_LEN];
  tmr_h    timer;
  SMnodeFp fp;
  SSteps * steps;
} tsMint;

tmr_h mnodeGetTimer() { return tsMint.timer; }

int32_t mnodeGetDnodeId() { return tsMint.dnodeId; }

char *mnodeGetClusterId() { return tsMint.clusterId; }

bool mnodeIsServing() { return tsMint.state == TAOS_MN_STATUS_READY; }

void mnodeSendMsgToDnode(struct SRpcEpSet *epSet, struct SRpcMsg *rpcMsg) {
  (*tsMint.fp.SendMsgToDnode)(epSet, rpcMsg);
}

void mnodeSendMsgToMnode(struct SRpcMsg *rpcMsg) { return (*tsMint.fp.SendMsgToMnode)(rpcMsg); }

void mnodeSendRedirectMsg(struct SRpcMsg *rpcMsg, bool forShell) { (*tsMint.fp.SendRedirectMsg)(rpcMsg, forShell); }

int32_t mnodeGetStatistics(SMnodeStat *stat) { return 0; }

static int32_t mnodeSetPara(SMnodePara para) {
  tsMint.fp = para.fp;
  tsMint.dnodeId = para.dnodeId;
  strncpy(tsMint.clusterId, para.clusterId, TSDB_CLUSTER_ID_LEN);

  if (tsMint.fp.GetDnodeEp == NULL) return -1;
  if (tsMint.fp.SendMsgToDnode == NULL) return -1;
  if (tsMint.fp.SendMsgToMnode == NULL) return -1;
  if (tsMint.fp.SendRedirectMsg == NULL) return -1;
  if (tsMint.dnodeId < 0) return -1;

  return 0;
}

static bool mnodeNeedDeploy() {
  if (tsMint.dnodeId > 0) return false;
  if (tsMint.clusterId[0] != 0) return false;
  if (strcmp(tsFirst, tsLocalEp) != 0) return false;
  return true;
}

static bool mnodeIsDeployed() {
  struct stat dirstat;
  char        filename[PATH_MAX + 20] = {0};
  snprintf(filename, sizeof(filename), "%s/wal/wal0", tsMnodeDir);

  return (stat(filename, &dirstat) == 0);
}

int32_t mnodeDeploy(struct SMInfos *pMinfos) {
  if (pMinfos == NULL) {  // first deploy
    tsMint.dnodeId = 1;
    bool getuid = taosGetSystemUid(tsMint.clusterId);
    if (!getuid) {
      strcpy(tsMint.clusterId, "tdengine3.0");
      mError("deploy new mnode but failed to get uid, set to default val %s", tsMint.clusterId);
    } else {
      mDebug("deploy new mnode and uid is %s", tsMint.clusterId);
    }
  } else {  // todo
  }

  if (mkdir(tsMnodeDir, 0755) != 0 && errno != EEXIST) {
    mError("failed to init mnode dir:%s, reason:%s", tsMnodeDir, strerror(errno));
    return -1;
  }

  return 0;
}

void mnodeUnDeploy() {
  if (remove(tsMnodeDir) != 0) {
    mInfo("failed to remove mnode file, reason:%s", strerror(errno));
  } else {
    mInfo("mnode file is removed");
  }
}

static int32_t mnodeInitTimer() {
  if (tsMint.timer == NULL) {
    tsMint.timer = taosTmrInit(tsMaxShellConns, 200, 3600000, "MND");
  }

  return 0;
}

static void mnodeCleanupTimer() {
  if (tsMint.timer != NULL) {
    taosTmrCleanUp(tsMint.timer);
    tsMint.timer = NULL;
  }
}

static int32_t mnodeInitSteps() {
  struct SSteps *steps = taosStepInit(20, NULL);
  if (steps == NULL) return -1;

  taosStepAdd(steps, "mnode-timer",   mnodeInitTimer,   NULL);
  taosStepAdd(steps, "mnode-worker",  mnodeInitWorker, NULL);
  taosStepAdd(steps, "mnode-sdbref",  sdbInitRef,       sdbCleanUpRef);
  taosStepAdd(steps, "mnode-profile", mnodeInitProfile, mnodeCleanupProfile);
  taosStepAdd(steps, "mnode-funcs",   mnodeInitFuncs,   mnodeCleanupFuncs);
  taosStepAdd(steps, "mnode-dnodes",  mnodeInitDnodes,  mnodeCleanupDnodes);
  taosStepAdd(steps, "mnode-dbs",     mnodeInitDbs,     mnodeCleanupDbs);
  taosStepAdd(steps, "mnode-vgroups", mnodeInitVgroups, mnodeCleanupVgroups);
  taosStepAdd(steps, "mnode-tables",  mnodeInitTables,  mnodeCleanupTables);  
  taosStepAdd(steps, "mnode-mnodes",  mnodeInitMnodes,  mnodeCleanupMnodes);
  taosStepAdd(steps, "mnode-sdb",     sdbInit,          sdbCleanUp);
  taosStepAdd(steps, "mnode-balance", bnInit,           bnCleanUp);
  taosStepAdd(steps, "mnode-grant",   grantInit,        grantCleanUp);
  taosStepAdd(steps, "mnode-show",    mnodeInitShow,    mnodeCleanUpShow);
  taosStepAdd(steps, "mnode-kv",      mnodeInitKv,      mnodeCleanupKv);
  taosStepAdd(steps, "mnode-raft",    mnodeInitRaft,    mnodeCleanupRaft);
  taosStepAdd(steps, "mnode-cluster", mnodeInitCluster, mnodeCleanupCluster);
  taosStepAdd(steps, "mnode-users",   mnodeInitUsers,   mnodeCleanupUsers);
  taosStepAdd(steps, "mnode-worker",  NULL,             mnodeCleanupWorker);
  taosStepAdd(steps, "mnode-timer",   NULL,             mnodeCleanupTimer);

  tsMint.steps = steps;
  return taosStepExec(tsMint.steps);
}

static void mnodeCleanupSteps() { taosStepCleanup(tsMint.steps); }


int32_t mnodeInit(SMnodePara para) {
  int32_t code = 0;
  if (tsMint.state != TAOS_MN_STATUS_UNINIT) {
    return 0;
  } else {
    tsMint.state = TAOS_MN_STATUS_INIT;
  }

  code = mnodeSetPara(para);
  if (code != 0) {
    tsMint.state = TAOS_MN_STATUS_UNINIT;
    return code;
  }

  

  bool needDeploy = mnodeNeedDeploy();
  bool deployed = mnodeIsDeployed();

  if (!deployed) {
    if (needDeploy) {
      code = mnodeDeploy(NULL);
      if (code != 0) {
        tsMint.state = TAOS_MN_STATUS_UNINIT;
        return code;
      }
    } else {
      tsMint.state = TAOS_MN_STATUS_UNINIT;
      return 0;
    }
  }

  mInfo("starting to initialize mnode ...");

  code = mnodeInitSteps();
  if (code != 0) {
    tsMint.state = TAOS_MN_STATUS_UNINIT;
    return -1;
  }

  // todo
  grantReset(TSDB_GRANT_ALL, 0);
  sdbUpdateSync(NULL);

  tsMint.state = TAOS_MN_STATUS_READY;
  mInfo("mnode is initialized successfully");
  return 0;
}

void mnodeCleanup() {
  if (tsMint.state != TAOS_MN_STATUS_UNINIT && tsMint.state != TAOS_MN_STATUS_CLOSING) {
    mInfo("starting to clean up mnode");
    tsMint.state = TAOS_MN_STATUS_CLOSING;
    
    mnodeCleanupSteps();

    tsMint.state = TAOS_MN_STATUS_UNINIT;
    mInfo("mnode is cleaned up");
  }
}

int32_t mnodeRetriveAuth(char *user, char *spi, char *encrypt, char *secret, char *ckey) { return 0; }

void mnodeProcessMsg(SRpcMsg *rpcMsg) {}