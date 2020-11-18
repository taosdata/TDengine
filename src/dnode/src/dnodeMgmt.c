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
#include "cJSON.h"
#include "taoserror.h"
#include "taosmsg.h"
#include "ttimer.h"
#include "tsdb.h"
#include "twal.h"
#include "tqueue.h"
#include "tsync.h"
#include "ttimer.h"
#include "tbalance.h"
#include "tglobal.h"
#include "dnode.h"
#include "vnode.h"
#include "mnode.h"
#include "dnodeInt.h"
#include "dnodeMgmt.h"
#include "dnodeEps.h"
#include "dnodeCfg.h"
#include "dnodeMInfos.h"
#include "dnodeVRead.h"
#include "dnodeVWrite.h"
#include "dnodeModule.h"

typedef struct {
  pthread_t thread;
  int32_t   threadIndex;
  int32_t   failed;
  int32_t   opened;
  int32_t   vnodeNum;
  int32_t * vnodeList;
} SOpenVnodeThread;

typedef struct {
  SRpcMsg rpcMsg;
  char    pCont[];
} SMgmtMsg;

void *            tsDnodeTmr = NULL;
static void *     tsStatusTimer = NULL;
static uint32_t   tsRebootTime;
static taos_qset  tsMgmtQset = NULL;
static taos_queue tsMgmtQueue = NULL;
static pthread_t  tsQthread;

static void   dnodeProcessStatusRsp(SRpcMsg *pMsg);
static void   dnodeSendStatusMsg(void *handle, void *tmrId);
static void  *dnodeProcessMgmtQueue(void *param);

static int32_t  dnodeOpenVnodes();
static void     dnodeCloseVnodes();
static int32_t  dnodeProcessCreateVnodeMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessAlterVnodeMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessDropVnodeMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessAlterStreamMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessConfigDnodeMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessCreateMnodeMsg(SRpcMsg *pMsg);
static int32_t (*dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *pMsg);

int32_t dnodeInitMgmt() {
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = dnodeProcessCreateVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE]  = dnodeProcessAlterVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE]   = dnodeProcessDropVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = dnodeProcessAlterStreamMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE] = dnodeProcessConfigDnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CREATE_MNODE] = dnodeProcessCreateMnodeMsg;

  dnodeAddClientRspHandle(TSDB_MSG_TYPE_DM_STATUS_RSP,  dnodeProcessStatusRsp);
  tsRebootTime = taosGetTimestampSec();

  int32_t code = vnodeInitResources();
  if (code != TSDB_CODE_SUCCESS) {
    dnodeCleanupMgmt();
    return -1;
  }

  // create the queue and thread to handle the message 
  tsMgmtQset = taosOpenQset();
  if (tsMgmtQset == NULL) {
    dError("failed to create the mgmt queue set");
    dnodeCleanupMgmt();
    return -1;
  }

  tsMgmtQueue = taosOpenQueue();
  if (tsMgmtQueue == NULL) {
    dError("failed to create the mgmt queue");
    dnodeCleanupMgmt();
    return -1;
  }

  taosAddIntoQset(tsMgmtQset, tsMgmtQueue, NULL);

  pthread_attr_t thAttr;
  pthread_attr_init(&thAttr);
  pthread_attr_setdetachstate(&thAttr, PTHREAD_CREATE_JOINABLE);

  code = pthread_create(&tsQthread, &thAttr, dnodeProcessMgmtQueue, NULL);
  pthread_attr_destroy(&thAttr);
  if (code != 0) {
    dError("failed to create thread to process mgmt queue, reason:%s", strerror(errno));
    dnodeCleanupMgmt();
    return -1; 
  }

  code = dnodeOpenVnodes();
  if (code != TSDB_CODE_SUCCESS) {
    dnodeCleanupMgmt();
    return -1;
  }

  dInfo("dnode mgmt is initialized");
 
  return TSDB_CODE_SUCCESS;
}

int32_t dnodeInitMgmtTimer() {
  tsDnodeTmr = taosTmrInit(100, 200, 60000, "DND-DM");
  if (tsDnodeTmr == NULL) {
    dError("failed to init dnode timer");
    dnodeCleanupMgmt();
    return -1;
  }

  taosTmrReset(dnodeSendStatusMsg, 500, NULL, tsDnodeTmr, &tsStatusTimer);
  dInfo("dnode mgmt timer is initialized");
  return TSDB_CODE_SUCCESS;
}

void dnodeSendStatusMsgToMnode() {
  if (tsDnodeTmr != NULL && tsStatusTimer != NULL) {
    dInfo("force send status msg to mnode");
    taosTmrReset(dnodeSendStatusMsg, 3, NULL, tsDnodeTmr, &tsStatusTimer);
  }
}

void dnodeCleanupMgmtTimer() {
  if (tsStatusTimer != NULL) {
    taosTmrStopA(&tsStatusTimer);
    tsStatusTimer = NULL;
  }

  if (tsDnodeTmr != NULL) {
    taosTmrCleanUp(tsDnodeTmr);
    tsDnodeTmr = NULL;
  }
}

void dnodeCleanupMgmt() {
  dnodeCleanupMgmtTimer();
  dnodeCloseVnodes();

  if (tsMgmtQset) taosQsetThreadResume(tsMgmtQset);
  if (tsQthread) pthread_join(tsQthread, NULL);

  if (tsMgmtQueue) taosCloseQueue(tsMgmtQueue);
  if (tsMgmtQset) taosCloseQset(tsMgmtQset);
  tsMgmtQset = NULL;
  tsMgmtQueue = NULL;

  vnodeCleanupResources();
}

static int32_t dnodeWriteToMgmtQueue(SRpcMsg *pMsg) {
  int32_t   size = sizeof(SMgmtMsg) + pMsg->contLen;
  SMgmtMsg *pMgmt = taosAllocateQitem(size);
  if (pMgmt == NULL) {
    return TSDB_CODE_DND_OUT_OF_MEMORY;
  }

  pMgmt->rpcMsg = *pMsg;
  pMgmt->rpcMsg.pCont = pMgmt->pCont;
  memcpy(pMgmt->pCont, pMsg->pCont, pMsg->contLen);
  taosWriteQitem(tsMgmtQueue, TAOS_QTYPE_RPC, pMgmt);

  return TSDB_CODE_SUCCESS;
}

void dnodeDispatchToMgmtQueue(SRpcMsg *pMsg) {
  int32_t code = dnodeWriteToMgmtQueue(pMsg);
  if (code != TSDB_CODE_SUCCESS) {
    SRpcMsg rsp = {.handle = pMsg->handle, .code = code};
    rpcSendResponse(&rsp);
  }

  rpcFreeCont(pMsg->pCont);
}

static void *dnodeProcessMgmtQueue(void *param) {
  SMgmtMsg *pMgmt;
  SRpcMsg * pMsg;
  SRpcMsg   rsp = {0};
  int32_t   qtype;
  void *    handle;

  while (1) {
    if (taosReadQitemFromQset(tsMgmtQset, &qtype, (void **)&pMgmt, &handle) == 0) {
      dDebug("qset:%p, dnode mgmt got no message from qset, exit", tsMgmtQset);
      break;
    }

    pMsg = &pMgmt->rpcMsg;
    dDebug("%p, msg:%p:%s will be processed", pMsg->ahandle, pMgmt, taosMsg[pMsg->msgType]);
    if (dnodeProcessMgmtMsgFp[pMsg->msgType]) {
      rsp.code = (*dnodeProcessMgmtMsgFp[pMsg->msgType])(pMsg);
    } else {
      rsp.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    }

    rsp.handle = pMsg->handle;
    rsp.pCont = NULL;
    rpcSendResponse(&rsp);

    taosFreeQitem(pMsg);
  }

  return NULL;
}

static int32_t dnodeGetVnodeList(int32_t vnodeList[], int32_t *numOfVnodes) {
  DIR *dir = opendir(tsVnodeDir);
  if (dir == NULL) {
    return TSDB_CODE_DND_NO_WRITE_ACCESS;
  }

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
        continue;
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
  char vnodeDir[TSDB_FILENAME_LEN * 3];

  dDebug("thread:%d, start to open %d vnodes", pThread->threadIndex, pThread->vnodeNum);

  for (int32_t v = 0; v < pThread->vnodeNum; ++v) {
    int32_t vgId = pThread->vnodeList[v];
    snprintf(vnodeDir, TSDB_FILENAME_LEN * 3, "%s/vnode%d", tsVnodeDir, vgId);
    if (vnodeOpen(vgId, vnodeDir) < 0) {
      dError("vgId:%d, failed to open vnode by thread:%d", vgId, pThread->threadIndex);
      pThread->failed++;
    } else {
      dDebug("vgId:%d, is openned by thread:%d", vgId, pThread->threadIndex);
      pThread->opened++;
    }
  }

  dDebug("thread:%d, total vnodes:%d, openned:%d failed:%d", pThread->threadIndex, pThread->vnodeNum, pThread->opened,
         pThread->failed);
  return NULL;
}

static int32_t dnodeOpenVnodes() {
  int32_t vnodeList[TSDB_MAX_VNODES] = {0};
  int32_t numOfVnodes = 0;
  int32_t status = dnodeGetVnodeList(vnodeList, &numOfVnodes);

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

  dDebug("start %d threads to open %d vnodes", threadNum, numOfVnodes);

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
    if (pThread->vnodeNum > 0 && pThread->thread) {
      pthread_join(pThread->thread, NULL);
    }
    openVnodes += pThread->opened;
    failedVnodes += pThread->failed;
    free(pThread->vnodeList);
  }

  free(threads);
  dInfo("there are total vnodes:%d, openned:%d failed:%d", numOfVnodes, openVnodes, failedVnodes);

  return TSDB_CODE_SUCCESS;
}

static void dnodeCloseVnodes() {
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

static void* dnodeParseVnodeMsg(SRpcMsg *rpcMsg) {
  SCreateVnodeMsg *pCreate = rpcMsg->pCont;
  pCreate->cfg.vgId                = htonl(pCreate->cfg.vgId);
  pCreate->cfg.cfgVersion          = htonl(pCreate->cfg.cfgVersion);
  pCreate->cfg.maxTables           = htonl(pCreate->cfg.maxTables);
  pCreate->cfg.cacheBlockSize      = htonl(pCreate->cfg.cacheBlockSize);
  pCreate->cfg.totalBlocks         = htonl(pCreate->cfg.totalBlocks);
  pCreate->cfg.daysPerFile         = htonl(pCreate->cfg.daysPerFile);
  pCreate->cfg.daysToKeep1         = htonl(pCreate->cfg.daysToKeep1);
  pCreate->cfg.daysToKeep2         = htonl(pCreate->cfg.daysToKeep2);
  pCreate->cfg.daysToKeep          = htonl(pCreate->cfg.daysToKeep);
  pCreate->cfg.minRowsPerFileBlock = htonl(pCreate->cfg.minRowsPerFileBlock);
  pCreate->cfg.maxRowsPerFileBlock = htonl(pCreate->cfg.maxRowsPerFileBlock);
  pCreate->cfg.fsyncPeriod         = htonl(pCreate->cfg.fsyncPeriod);
  pCreate->cfg.commitTime          = htonl(pCreate->cfg.commitTime);

  for (int32_t j = 0; j < pCreate->cfg.replications; ++j) {
    pCreate->nodes[j].nodeId = htonl(pCreate->nodes[j].nodeId);
  }

  return pCreate;
}

static int32_t dnodeProcessCreateVnodeMsg(SRpcMsg *rpcMsg) {
  SCreateVnodeMsg *pCreate = dnodeParseVnodeMsg(rpcMsg);

  void *pVnode = vnodeAcquire(pCreate->cfg.vgId);
  if (pVnode != NULL) {
    dDebug("vgId:%d, already exist, return success", pCreate->cfg.vgId);
    vnodeRelease(pVnode);
    return TSDB_CODE_SUCCESS;
  } else {
    dDebug("vgId:%d, create vnode msg is received", pCreate->cfg.vgId);
    return vnodeCreate(pCreate);
  }
}

static int32_t dnodeProcessAlterVnodeMsg(SRpcMsg *rpcMsg) {
  SAlterVnodeMsg *pAlter = dnodeParseVnodeMsg(rpcMsg);

  void *pVnode = vnodeAcquire(pAlter->cfg.vgId);
  if (pVnode != NULL) {
    dDebug("vgId:%d, alter vnode msg is received", pAlter->cfg.vgId);
    int32_t code = vnodeAlter(pVnode, pAlter);
    vnodeRelease(pVnode);
    return code;
  } else {
    dError("vgId:%d, vnode not exist, can't alter it", pAlter->cfg.vgId);
    return TSDB_CODE_VND_INVALID_VGROUP_ID;
  }
}

static int32_t dnodeProcessDropVnodeMsg(SRpcMsg *rpcMsg) {
  SDropVnodeMsg *pDrop = rpcMsg->pCont;
  pDrop->vgId = htonl(pDrop->vgId);

  return vnodeDrop(pDrop->vgId);
}

static int32_t dnodeProcessAlterStreamMsg(SRpcMsg *pMsg) {
//  SAlterStreamMsg *pStream = pCont;
//  pStream->uid    = htobe64(pStream->uid);
//  pStream->stime  = htobe64(pStream->stime);
//  pStream->vnode  = htonl(pStream->vnode);
//  pStream->sid    = htonl(pStream->sid);
//  pStream->status = htonl(pStream->status);
//
//  int32_t code = dnodeCreateStream(pStream);

  return 0;
}

static int32_t dnodeProcessConfigDnodeMsg(SRpcMsg *pMsg) {
  SCfgDnodeMsg *pCfg = pMsg->pCont;
  return taosCfgDynamicOptions(pCfg->config);
}

static int32_t dnodeProcessCreateMnodeMsg(SRpcMsg *pMsg) {
  SCreateMnodeMsg *pCfg = pMsg->pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  if (pCfg->dnodeId != dnodeGetDnodeId()) {
    dError("dnodeId:%d, in create mnode msg is not equal with saved dnodeId:%d", pCfg->dnodeId, dnodeGetDnodeId());
    return TSDB_CODE_MND_DNODE_ID_NOT_CONFIGURED;
  }

  if (strcmp(pCfg->dnodeEp, tsLocalEp) != 0) {
    dError("dnodeEp:%s, in create mnode msg is not equal with saved dnodeEp:%s", pCfg->dnodeEp, tsLocalEp);
    return TSDB_CODE_MND_DNODE_EP_NOT_CONFIGURED;
  }

  dDebug("dnodeId:%d, create mnode msg is received from mnodes, numOfMnodes:%d", pCfg->dnodeId, pCfg->mnodes.mnodeNum);
  for (int i = 0; i < pCfg->mnodes.mnodeNum; ++i) {
    pCfg->mnodes.mnodeInfos[i].mnodeId = htonl(pCfg->mnodes.mnodeInfos[i].mnodeId);
    dDebug("mnode index:%d, mnode:%d:%s", i, pCfg->mnodes.mnodeInfos[i].mnodeId, pCfg->mnodes.mnodeInfos[i].mnodeEp);
  }

  dnodeStartMnode(&pCfg->mnodes);

  return TSDB_CODE_SUCCESS;
}

static void dnodeProcessStatusRsp(SRpcMsg *pMsg) {
  if (pMsg->code != TSDB_CODE_SUCCESS) {
    dError("status rsp is received, error:%s", tstrerror(pMsg->code));
    taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsDnodeTmr, &tsStatusTimer);
    return;
  }

  SStatusRsp *pStatusRsp = pMsg->pCont;
  SMnodeInfos *minfos = &pStatusRsp->mnodes;
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
  pStatus->alternativeRole  = (uint8_t) tsAlternativeRole;
  tstrncpy(pStatus->dnodeEp, tsLocalEp, TSDB_EP_LEN);

  // fill cluster cfg parameters
  pStatus->clusterCfg.numOfMnodes        = htonl(tsNumOfMnodes);
  pStatus->clusterCfg.enableBalance      = htonl(tsEnableBalance);
  pStatus->clusterCfg.mnodeEqualVnodeNum = htonl(tsMnodeEqualVnodeNum);
  pStatus->clusterCfg.offlineThreshold   = htonl(tsOfflineThreshold);
  pStatus->clusterCfg.statusInterval     = htonl(tsStatusInterval);
  pStatus->clusterCfg.maxtablesPerVnode  = htonl(tsMaxTablePerVnode);
  pStatus->clusterCfg.maxVgroupsPerDb    = htonl(tsMaxVgroupsPerDb);
  tstrncpy(pStatus->clusterCfg.arbitrator, tsArbitrator, TSDB_EP_LEN);
  tstrncpy(pStatus->clusterCfg.timezone, tsTimezone, 64);
  pStatus->clusterCfg.checkTime = 0;
  char timestr[32] = "1970-01-01 00:00:00.00";
  (void)taosParseTime(timestr, &pStatus->clusterCfg.checkTime, strlen(timestr), TSDB_TIME_PRECISION_MILLI, 0);
  tstrncpy(pStatus->clusterCfg.locale, tsLocale, TSDB_LOCALE_LEN);
  tstrncpy(pStatus->clusterCfg.charset, tsCharset, TSDB_LOCALE_LEN);  
  
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

void dnodeSendRedirectMsg(SRpcMsg *rpcMsg, bool forShell) {
  SRpcConnInfo connInfo = {0};
  rpcGetConnInfo(rpcMsg->handle, &connInfo);

  SRpcEpSet epSet = {0};
  if (forShell) {
    dnodeGetEpSetForShell(&epSet);
  } else {
    dnodeGetEpSetForPeer(&epSet);
  }
  
  dDebug("msg:%s will be redirected, dnodeIp:%s user:%s, numOfEps:%d inUse:%d", taosMsg[rpcMsg->msgType],
         taosIpStr(connInfo.clientIp), connInfo.user, epSet.numOfEps, epSet.inUse);

  for (int i = 0; i < epSet.numOfEps; ++i) {
    dDebug("mnode index:%d %s:%d", i, epSet.fqdn[i], epSet.port[i]);
    epSet.port[i] = htons(epSet.port[i]);
  }

  rpcSendRedirectRsp(rpcMsg->handle, &epSet);
}
