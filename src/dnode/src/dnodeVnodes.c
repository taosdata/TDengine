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
#include "ttimer.h"
#include "dnodeEps.h"
#include "dnodeCfg.h"
#include "dnodeMInfos.h"
#include "dnodeVnodes.h"

typedef struct {
  pthread_t thread;
  int32_t   threadIndex;
  int32_t   failed;
  int32_t   opened;
  int32_t   vnodeNum;
  int32_t * vnodeList;
} SOpenVnodeThread;

extern void *   tsDnodeTmr;
static void *   tsStatusTimer = NULL;
static uint32_t tsRebootTime = 0;
static int32_t  tsOpenVnodes = 0;
static int32_t  tsTotalVnodes = 0;

static void dnodeSendStatusMsg(void *handle, void *tmrId);
static void dnodeProcessStatusRsp(SRpcMsg *pMsg);

int32_t dnodeInitStatusTimer() {
  dnodeAddClientRspHandle(TSDB_MSG_TYPE_DM_STATUS_RSP, dnodeProcessStatusRsp);

  tsRebootTime = taosGetTimestampSec();
  taosTmrReset(dnodeSendStatusMsg, 500, NULL, tsDnodeTmr, &tsStatusTimer);

  dInfo("dnode status timer is initialized");
  return TSDB_CODE_SUCCESS;
}

void dnodeCleanupStatusTimer() {
  if (tsStatusTimer != NULL) {
    taosTmrStopA(&tsStatusTimer);
    tsStatusTimer = NULL;
  }
}

static int32_t dnodeGetVnodeList(int32_t vnodeList[], int32_t *numOfVnodes) {
  DIR *dir = opendir(tsVnodeDir);
  if (dir == NULL) return TSDB_CODE_DND_NO_WRITE_ACCESS;

  *numOfVnodes = 0;
  struct dirent *de = NULL;
  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
    if (de->d_type & DT_DIR) {
      if (strncmp("vnode", de->d_name, 5) != 0) continue;
      int32_t vnode = atoi(de->d_name + 5);
      if (vnode == 0) continue;

      (*numOfVnodes)++;

      if (*numOfVnodes >= TSDB_MAX_VNODES) {
        dError("vgId:%d, too many vnode directory in disk, exist:%d max:%d", vnode, *numOfVnodes, TSDB_MAX_VNODES);
        closedir(dir);
        return TSDB_CODE_DND_TOO_MANY_VNODES;
      } else {
        vnodeList[*numOfVnodes - 1] = vnode;
      }
    }
  }
  closedir(dir);

  return TSDB_CODE_SUCCESS;
}

static void *dnodeOpenVnode(void *param) {
  SOpenVnodeThread *pThread = param;
  char stepDesc[TSDB_STEP_DESC_LEN] = {0};

  dDebug("thread:%d, start to open %d vnodes", pThread->threadIndex, pThread->vnodeNum);

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    int32_t vgId = pThread->vnodeList[v];
    snprintf(stepDesc, TSDB_STEP_DESC_LEN, "vgId:%d, start to restore, %d of %d have been opened", vgId, tsOpenVnodes, tsTotalVnodes);
    dnodeReportStep("open-vnodes", stepDesc, 0);

    if (vnodeOpen(vgId) < 0) {
      dError("vgId:%d, failed to open vnode by thread:%d", vgId, pThread->threadIndex);
      pThread->failed++;
    } else {
      dDebug("vgId:%d, is opened by thread:%d", vgId, pThread->threadIndex);
      pThread->opened++;
    }

    atomic_add_fetch_32(&tsOpenVnodes, 1);
  }

  dDebug("thread:%d, total vnodes:%d, opened:%d failed:%d", pThread->threadIndex, pThread->vnodeNum, pThread->opened,
         pThread->failed);
  return NULL;
}

int32_t dnodeInitVnodes() {
  int32_t vnodeList[TSDB_MAX_VNODES] = {0};
  int32_t numOfVnodes = 0;
  int32_t status = dnodeGetVnodeList(vnodeList, &numOfVnodes);
  tsTotalVnodes = numOfVnodes;

  if (status != TSDB_CODE_SUCCESS) {
    dInfo("get dnode list failed");
    return status;
  }

  int32_t threadNum = tsNumOfCores;
  int32_t vnodesPerThread = numOfVnodes / threadNum + 1;
  SOpenVnodeThread *threads = calloc(threadNum, sizeof(SOpenVnodeThread));
  for (int32_t t = 0; t < threadNum; ++t) {
    threads[t].threadIndex = t;
    threads[t].vnodeList = calloc(vnodesPerThread, sizeof(int32_t));
  }

  for (int32_t v = 0; v < numOfVnodes; ++v) {
    int32_t t = v % threadNum;
    SOpenVnodeThread *pThread = &threads[t];
    pThread->vnodeList[pThread->vnodeNum++] = vnodeList[v];
  }

  dInfo("start %d threads to open %d vnodes", threadNum, numOfVnodes);

  for (int32_t t = 0; t < threadNum; ++t) {
    SOpenVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum == 0) continue;

    pthread_attr_t thAttr;
    pthread_attr_init(&thAttr);
    pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);
    if (pthread_create(&pThread->thread, &thAttr, dnodeOpenVnode, pThread) != 0) {
      dError("thread:%d, failed to create thread to open vnode, reason:%s", pThread->threadIndex, strerror(errno));
    }

    pthread_attr_destroy(&thAttr);
  }

  int32_t openVnodes = 0;
  int32_t failedVnodes = 0;
  for (int32_t t = 0; t < threadNum; ++t) {
    SOpenVnodeThread *pThread = &threads[t];
    if (pThread->vnodeNum > 0 && taosCheckPthreadValid(pThread->thread)) {
      pthread_join(pThread->thread, NULL);
    }
    openVnodes += pThread->opened;
    failedVnodes += pThread->failed;
    free(pThread->vnodeList);
  }

  free(threads);
  dInfo("there are total vnodes:%d, opened:%d", numOfVnodes, openVnodes);

  if (failedVnodes != 0) {
    dError("there are total vnodes:%d, failed:%d", numOfVnodes, failedVnodes);
    return -1;
  }

  return TSDB_CODE_SUCCESS;
}

void dnodeCleanupVnodes() {
  int32_t vnodeList[TSDB_MAX_VNODES]= {0};
  int32_t numOfVnodes = 0;
  int32_t status;

  status = vnodeGetVnodeList(vnodeList, &numOfVnodes);

  if (status != TSDB_CODE_SUCCESS) {
    dInfo("get dnode list failed");
    return;
  }

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    vnodeClose(vnodeList[i]);
  }

  dInfo("total vnodes:%d are all closed", numOfVnodes);
}

static void dnodeProcessStatusRsp(SRpcMsg *pMsg) {
  if (pMsg->code != TSDB_CODE_SUCCESS) {
    dError("status rsp is received, error:%s", tstrerror(pMsg->code));
    if (pMsg->code == TSDB_CODE_MND_DNODE_NOT_EXIST) {
      char clusterId[TSDB_CLUSTER_ID_LEN];
      dnodeGetClusterId(clusterId);
      if (clusterId[0] != '\0') {
	dError("exit zombie dropped dnode");
	exit(EXIT_FAILURE);
      }
    }
    taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsDnodeTmr, &tsStatusTimer);
    return;
  }

  SStatusRsp *pStatusRsp = pMsg->pCont;
  SMInfos *minfos = &pStatusRsp->mnodes;
  dnodeUpdateMInfos(minfos);

  SDnodeCfg *pCfg = &pStatusRsp->dnodeCfg;
  pCfg->numOfVnodes = htonl(pCfg->numOfVnodes);
  pCfg->moduleStatus = htonl(pCfg->moduleStatus);
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  dnodeUpdateCfg(pCfg);

  vnodeSetAccess(pStatusRsp->vgAccess, pCfg->numOfVnodes);

  SDnodeEps *pEps = (SDnodeEps *)((char *)pStatusRsp->vgAccess + pCfg->numOfVnodes * sizeof(SVgroupAccess));
  dnodeUpdateEps(pEps);

  taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsDnodeTmr, &tsStatusTimer);
}

static void dnodeSendStatusMsg(void *handle, void *tmrId) {
  if (tsDnodeTmr == NULL) {
    dError("dnode timer is already released");
    return;
  }

  if (tsStatusTimer == NULL) {
    taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsDnodeTmr, &tsStatusTimer);
    dError("failed to start status timer");
    return;
  }

  int32_t contLen = sizeof(SStatusMsg) + TSDB_MAX_VNODES * sizeof(SVnodeLoad);
  SStatusMsg *pStatus = rpcMallocCont(contLen);
  if (pStatus == NULL) {
    taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsDnodeTmr, &tsStatusTimer);
    dError("failed to malloc status message");
    return;
  }

  dnodeGetCfg(&pStatus->dnodeId, pStatus->clusterId);
  pStatus->dnodeId          = htonl(dnodeGetDnodeId());
  pStatus->version          = htonl(tsVersion);
  pStatus->lastReboot       = htonl(tsRebootTime);
  pStatus->numOfCores       = htons((uint16_t) tsNumOfCores);
  pStatus->diskAvailable    = tsAvailDataDirGB;
  pStatus->alternativeRole  = tsAlternativeRole;
  tstrncpy(pStatus->dnodeEp, tsLocalEp, TSDB_EP_LEN);

  // fill cluster cfg parameters
  pStatus->clusterCfg.numOfMnodes        = htonl(tsNumOfMnodes);
  pStatus->clusterCfg.mnodeEqualVnodeNum = htonl(tsMnodeEqualVnodeNum);
  pStatus->clusterCfg.offlineThreshold   = htonl(tsOfflineThreshold);
  pStatus->clusterCfg.statusInterval     = htonl(tsStatusInterval);
  pStatus->clusterCfg.maxtablesPerVnode  = htonl(tsMaxTablePerVnode);
  pStatus->clusterCfg.maxVgroupsPerDb    = htonl(tsMaxVgroupsPerDb);
  tstrncpy(pStatus->clusterCfg.arbitrator, tsArbitrator, TSDB_EP_LEN);
  tstrncpy(pStatus->clusterCfg.timezone, tsTimezone, 64);
  pStatus->clusterCfg.checkTime = 0;
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &pStatus->clusterCfg.checkTime, (int32_t)strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  tstrncpy(pStatus->clusterCfg.locale, tsLocale, TSDB_LOCALE_LEN);
  tstrncpy(pStatus->clusterCfg.charset, tsCharset, TSDB_LOCALE_LEN);

  pStatus->clusterCfg.enableBalance = tsEnableBalance;
  pStatus->clusterCfg.flowCtrl = tsEnableFlowCtrl;
  pStatus->clusterCfg.slaveQuery = tsEnableSlaveQuery;
  pStatus->clusterCfg.adjustMaster = tsEnableAdjustMaster;

  vnodeBuildStatusMsg(pStatus);
  contLen = sizeof(SStatusMsg) + pStatus->openVnodes * sizeof(SVnodeLoad);
  pStatus->openVnodes = htons(pStatus->openVnodes);

  SRpcMsg rpcMsg = {
    .pCont   = pStatus,
    .contLen = contLen,
    .msgType = TSDB_MSG_TYPE_DM_STATUS
  };

  SRpcEpSet epSet;
  dnodeGetEpSetForPeer(&epSet);
  dnodeSendMsgToDnode(&epSet, &rpcMsg);
}

void dnodeSendStatusMsgToMnode() {
  if (tsDnodeTmr != NULL && tsStatusTimer != NULL) {
    dInfo("force send status msg to mnode");
    taosTmrReset(dnodeSendStatusMsg, 3, NULL, tsDnodeTmr, &tsStatusTimer);
  }
}
