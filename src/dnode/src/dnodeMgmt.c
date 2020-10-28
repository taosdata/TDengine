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
#include "dnodeVRead.h"
#include "dnodeVWrite.h"
#include "dnodeModule.h"

#define MPEER_CONTENT_LEN 2000

typedef struct {
  pthread_t thread;
  int32_t   threadIndex;
  int32_t   failed;
  int32_t   opened;
  int32_t   vnodeNum;
  int32_t * vnodeList;
} SOpenVnodeThread;

void *          tsDnodeTmr = NULL;
static void *   tsStatusTimer = NULL;
static uint32_t tsRebootTime;

static SRpcEpSet     tsDMnodeEpSet = {0};
static SDMMnodeInfos tsDMnodeInfos = {0};
static SDMDnodeCfg   tsDnodeCfg = {0};
static taos_qset     tsMgmtQset = NULL;
static taos_queue    tsMgmtQueue = NULL;
static pthread_t     tsQthread;

static void   dnodeUpdateMnodeInfos(SDMMnodeInfos *pMnodes);
static bool   dnodeReadMnodeInfos();
static void   dnodeSaveMnodeInfos();
static void   dnodeUpdateDnodeCfg(SDMDnodeCfg *pCfg);
static bool   dnodeReadDnodeCfg();
static void   dnodeSaveDnodeCfg();
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
static int32_t dnodeProcessCreateMnodeMsg(SRpcMsg *pMsg);
static int32_t (*dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *pMsg);

int32_t dnodeInitMgmt() {
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = dnodeProcessCreateVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_VNODE]  = dnodeProcessAlterVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE]   = dnodeProcessDropVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = dnodeProcessAlterStreamMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE] = dnodeProcessConfigDnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CREATE_MNODE] = dnodeProcessCreateMnodeMsg;

  dnodeAddClientRspHandle(TSDB_MSG_TYPE_DM_STATUS_RSP,  dnodeProcessStatusRsp);
  dnodeReadDnodeCfg();
  tsRebootTime = taosGetTimestampSec();

  if (!dnodeReadMnodeInfos()) {
    memset(&tsDMnodeEpSet, 0, sizeof(SRpcEpSet));
    memset(&tsDMnodeInfos, 0, sizeof(SDMMnodeInfos));

    tsDMnodeEpSet.numOfEps = 1;
    taosGetFqdnPortFromEp(tsFirst, tsDMnodeEpSet.fqdn[0], &tsDMnodeEpSet.port[0]);
    
    if (strcmp(tsSecond, tsFirst) != 0) {
      tsDMnodeEpSet.numOfEps = 2;
      taosGetFqdnPortFromEp(tsSecond, tsDMnodeEpSet.fqdn[1], &tsDMnodeEpSet.port[1]);
    }
  } else {
    tsDMnodeEpSet.inUse = tsDMnodeInfos.inUse;
    tsDMnodeEpSet.numOfEps = tsDMnodeInfos.nodeNum;
    for (int32_t i = 0; i < tsDMnodeInfos.nodeNum; i++) {
      taosGetFqdnPortFromEp(tsDMnodeInfos.nodeInfos[i].nodeEp, tsDMnodeEpSet.fqdn[i], &tsDMnodeEpSet.port[i]);
    }
  }

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

void dnodeDispatchToMgmtQueue(SRpcMsg *pMsg) {
  void *item;

  item = taosAllocateQitem(sizeof(SRpcMsg));
  if (item) {
    memcpy(item, pMsg, sizeof(SRpcMsg));
    taosWriteQitem(tsMgmtQueue, 1, item);
  } else {
    SRpcMsg rsp = {
      .handle = pMsg->handle,
      .pCont  = NULL,
      .code   = TSDB_CODE_DND_OUT_OF_MEMORY
    };
    
    rpcSendResponse(&rsp);
    rpcFreeCont(pMsg->pCont);
  }
}

static void *dnodeProcessMgmtQueue(void *param) {
  SRpcMsg *pMsg;
  SRpcMsg  rsp = {0};
  int      type;
  void *   handle;

  while (1) {
    if (taosReadQitemFromQset(tsMgmtQset, &type, (void **) &pMsg, &handle) == 0) {
      dDebug("qset:%p, dnode mgmt got no message from qset, exit", tsMgmtQset);
      break;
    }

    dDebug("%p, msg:%s will be processed", pMsg->ahandle, taosMsg[pMsg->msgType]);    
    if (dnodeProcessMgmtMsgFp[pMsg->msgType]) {
      rsp.code = (*dnodeProcessMgmtMsgFp[pMsg->msgType])(pMsg);
    } else {
      rsp.code = TSDB_CODE_DND_MSG_NOT_PROCESSED;
    }

    rsp.handle = pMsg->handle;
    rsp.pCont  = NULL;
    rpcSendResponse(&rsp);

    rpcFreeCont(pMsg->pCont);
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
  SMDCreateVnodeMsg *pCreate = rpcMsg->pCont;
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
  SMDCreateVnodeMsg *pCreate = dnodeParseVnodeMsg(rpcMsg);

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
  SMDAlterVnodeMsg *pAlter = dnodeParseVnodeMsg(rpcMsg);

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
  SMDDropVnodeMsg *pDrop = rpcMsg->pCont;
  pDrop->vgId = htonl(pDrop->vgId);

  return vnodeDrop(pDrop->vgId);
}

static int32_t dnodeProcessAlterStreamMsg(SRpcMsg *pMsg) {
//  SMDAlterStreamMsg *pStream = pCont;
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
  SMDCfgDnodeMsg *pCfg = pMsg->pCont;
  return taosCfgDynamicOptions(pCfg->config);
}

static int32_t dnodeProcessCreateMnodeMsg(SRpcMsg *pMsg) {
  SMDCreateMnodeMsg *pCfg = pMsg->pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  if (pCfg->dnodeId != dnodeGetDnodeId()) {
    dError("dnodeId:%d, in create mnode msg is not equal with saved dnodeId:%d", pCfg->dnodeId, dnodeGetDnodeId());
    return TSDB_CODE_MND_DNODE_ID_NOT_CONFIGURED;
  }

  if (strcmp(pCfg->dnodeEp, tsLocalEp) != 0) {
    dError("dnodeEp:%s, in create mnode msg is not equal with saved dnodeEp:%s", pCfg->dnodeEp, tsLocalEp);
    return TSDB_CODE_MND_DNODE_EP_NOT_CONFIGURED;
  }

  dDebug("dnodeId:%d, create mnode msg is received from mnodes, numOfMnodes:%d", pCfg->dnodeId, pCfg->mnodes.nodeNum);
  for (int i = 0; i < pCfg->mnodes.nodeNum; ++i) {
    pCfg->mnodes.nodeInfos[i].nodeId = htonl(pCfg->mnodes.nodeInfos[i].nodeId);
    dDebug("mnode index:%d, mnode:%d:%s", i, pCfg->mnodes.nodeInfos[i].nodeId, pCfg->mnodes.nodeInfos[i].nodeEp);
  }

  dnodeStartMnode(&pCfg->mnodes);

  return TSDB_CODE_SUCCESS;
}

void dnodeUpdateMnodeEpSetForPeer(SRpcEpSet *pEpSet) {
  if (pEpSet->numOfEps <= 0) {
    dError("mnode EP list for peer is changed, but content is invalid, discard it");
    return;
  }

  dInfo("mnode EP list for peer is changed, numOfEps:%d inUse:%d", pEpSet->numOfEps, pEpSet->inUse);
  for (int i = 0; i < pEpSet->numOfEps; ++i) {
    pEpSet->port[i] -= TSDB_PORT_DNODEDNODE;
    dInfo("mnode index:%d %s:%u", i, pEpSet->fqdn[i], pEpSet->port[i]);
  }

  tsDMnodeEpSet = *pEpSet;
}

void dnodeGetMnodeEpSetForPeer(void *epSetRaw) {
  SRpcEpSet *epSet = epSetRaw;
  *epSet = tsDMnodeEpSet;

  for (int i=0; i<epSet->numOfEps; ++i) 
    epSet->port[i] += TSDB_PORT_DNODEDNODE;
}

void dnodeGetMnodeEpSetForShell(void *epSetRaw) {
  SRpcEpSet *epSet = epSetRaw;
  *epSet = tsDMnodeEpSet;
}

static void dnodeProcessStatusRsp(SRpcMsg *pMsg) {
  if (pMsg->code != TSDB_CODE_SUCCESS) {
    dError("status rsp is received, error:%s", tstrerror(pMsg->code));
    taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsDnodeTmr, &tsStatusTimer);
    return;
  }

  SDMStatusRsp *pStatusRsp = pMsg->pCont;
  SDMMnodeInfos *pMnodes = &pStatusRsp->mnodes;
  if (pMnodes->nodeNum <= 0) {
    dError("status msg is invalid, num of ips is %d", pMnodes->nodeNum);
    taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsDnodeTmr, &tsStatusTimer);
    return;
  }

  SDMDnodeCfg *pCfg = &pStatusRsp->dnodeCfg;
  pCfg->numOfVnodes  = htonl(pCfg->numOfVnodes);
  pCfg->moduleStatus = htonl(pCfg->moduleStatus);
  pCfg->dnodeId = htonl(pCfg->dnodeId);

  for (int32_t i = 0; i < pMnodes->nodeNum; ++i) {
    SDMMnodeInfo *pMnodeInfo = &pMnodes->nodeInfos[i];
    pMnodeInfo->nodeId   = htonl(pMnodeInfo->nodeId);
  }

  vnodeSetAccess(pStatusRsp->vgAccess, pCfg->numOfVnodes);

  // will not set mnode in status msg
  // dnodeProcessModuleStatus(pCfg->moduleStatus);
  dnodeUpdateDnodeCfg(pCfg);

  dnodeUpdateMnodeInfos(pMnodes);
  taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsDnodeTmr, &tsStatusTimer);
}

static bool dnodeCheckMnodeInfos(SDMMnodeInfos *pMnodes) {
  if (pMnodes->nodeNum <= 0 || pMnodes->nodeNum > 3) {
    dError("invalid mnode infos, num:%d", pMnodes->nodeNum);
    return false;
  }

  for (int32_t i = 0; i < pMnodes->nodeNum; ++i) {
    SDMMnodeInfo *pMnodeInfo = &pMnodes->nodeInfos[i];
    if (pMnodeInfo->nodeId <= 0 || strlen(pMnodeInfo->nodeEp) <= 5) {
      dError("invalid mnode info:%d, nodeId:%d nodeEp:%s", i, pMnodeInfo->nodeId, pMnodeInfo->nodeEp);
      return false;
    }
  }

  return true;
}

static void dnodeUpdateMnodeInfos(SDMMnodeInfos *pMnodes) {
  bool mnodesChanged = (memcmp(&tsDMnodeInfos, pMnodes, sizeof(SDMMnodeInfos)) != 0);
  bool mnodesNotInit = (tsDMnodeInfos.nodeNum == 0);
  if (!(mnodesChanged || mnodesNotInit)) return;

  if (!dnodeCheckMnodeInfos(pMnodes)) return;

  memcpy(&tsDMnodeInfos, pMnodes, sizeof(SDMMnodeInfos));
  dInfo("mnode infos is changed, nodeNum:%d inUse:%d", tsDMnodeInfos.nodeNum, tsDMnodeInfos.inUse);
  for (int32_t i = 0; i < tsDMnodeInfos.nodeNum; i++) {
    dInfo("mnode index:%d, %s", tsDMnodeInfos.nodeInfos[i].nodeId, tsDMnodeInfos.nodeInfos[i].nodeEp);
  }

  tsDMnodeEpSet.inUse = tsDMnodeInfos.inUse;
  tsDMnodeEpSet.numOfEps = tsDMnodeInfos.nodeNum;
  for (int32_t i = 0; i < tsDMnodeInfos.nodeNum; i++) {
    taosGetFqdnPortFromEp(tsDMnodeInfos.nodeInfos[i].nodeEp, tsDMnodeEpSet.fqdn[i], &tsDMnodeEpSet.port[i]);
  }

  dnodeSaveMnodeInfos();
  sdbUpdateAsync();
}

static bool dnodeReadMnodeInfos() {
  char ipFile[TSDB_FILENAME_LEN*2] = {0};
  
  sprintf(ipFile, "%s/mnodeEpSet.json", tsDnodeDir);
  FILE *fp = fopen(ipFile, "r");
  if (!fp) {
    dDebug("failed to read mnodeEpSet.json, file not exist");
    return false;
  }

  bool  ret = false;
  int   maxLen = 2000;
  char *content = calloc(1, maxLen + 1);
  int   len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    free(content);
    fclose(fp);
    dError("failed to read mnodeEpSet.json, content is null");
    return false;
  }

  content[len] = 0;
  cJSON* root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read mnodeEpSet.json, invalid json format");
    goto PARSE_OVER;
  }

  cJSON* inUse = cJSON_GetObjectItem(root, "inUse");
  if (!inUse || inUse->type != cJSON_Number) {
    dError("failed to read mnodeEpSet.json, inUse not found");
    goto PARSE_OVER;
  }
  tsDMnodeInfos.inUse = inUse->valueint;

  cJSON* nodeNum = cJSON_GetObjectItem(root, "nodeNum");
  if (!nodeNum || nodeNum->type != cJSON_Number) {
    dError("failed to read mnodeEpSet.json, nodeNum not found");
    goto PARSE_OVER;
  }
  tsDMnodeInfos.nodeNum = nodeNum->valueint;

  cJSON* nodeInfos = cJSON_GetObjectItem(root, "nodeInfos");
  if (!nodeInfos || nodeInfos->type != cJSON_Array) {
    dError("failed to read mnodeEpSet.json, nodeInfos not found");
    goto PARSE_OVER;
  }

  int size = cJSON_GetArraySize(nodeInfos);
  if (size != tsDMnodeInfos.nodeNum) {
    dError("failed to read mnodeEpSet.json, nodeInfos size not matched");
    goto PARSE_OVER;
  }

  for (int i = 0; i < size; ++i) {
    cJSON *nodeInfo = cJSON_GetArrayItem(nodeInfos, i);
    if (nodeInfo == NULL) continue;

    cJSON *nodeId = cJSON_GetObjectItem(nodeInfo, "nodeId");
    if (!nodeId || nodeId->type != cJSON_Number) {
      dError("failed to read mnodeEpSet.json, nodeId not found");
      goto PARSE_OVER;
    }
    tsDMnodeInfos.nodeInfos[i].nodeId = nodeId->valueint;

    cJSON *nodeEp = cJSON_GetObjectItem(nodeInfo, "nodeEp");
    if (!nodeEp || nodeEp->type != cJSON_String || nodeEp->valuestring == NULL) {
      dError("failed to read mnodeEpSet.json, nodeName not found");
      goto PARSE_OVER;
    }
    strncpy(tsDMnodeInfos.nodeInfos[i].nodeEp, nodeEp->valuestring, TSDB_EP_LEN);
  }

  ret = true;

  dInfo("read mnode epSet successed, numOfEps:%d inUse:%d", tsDMnodeInfos.nodeNum, tsDMnodeInfos.inUse);
  for (int32_t i = 0; i < tsDMnodeInfos.nodeNum; i++) {
    dInfo("mnode:%d, %s", tsDMnodeInfos.nodeInfos[i].nodeId, tsDMnodeInfos.nodeInfos[i].nodeEp);
  }

PARSE_OVER:
  free(content);
  cJSON_Delete(root);
  fclose(fp);
  return ret;
}

static void dnodeSaveMnodeInfos() {
  char ipFile[TSDB_FILENAME_LEN] = {0};
  sprintf(ipFile, "%s/mnodeEpSet.json", tsDnodeDir);
  FILE *fp = fopen(ipFile, "w");
  if (!fp) return;

  int32_t len = 0;
  int32_t maxLen = 2000;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"inUse\": %d,\n", tsDMnodeInfos.inUse);
  len += snprintf(content + len, maxLen - len, "  \"nodeNum\": %d,\n", tsDMnodeInfos.nodeNum);
  len += snprintf(content + len, maxLen - len, "  \"nodeInfos\": [{\n");
  for (int32_t i = 0; i < tsDMnodeInfos.nodeNum; i++) {
    len += snprintf(content + len, maxLen - len, "    \"nodeId\": %d,\n", tsDMnodeInfos.nodeInfos[i].nodeId);
    len += snprintf(content + len, maxLen - len, "    \"nodeEp\": \"%s\"\n", tsDMnodeInfos.nodeInfos[i].nodeEp);
    if (i < tsDMnodeInfos.nodeNum -1) {
      len += snprintf(content + len, maxLen - len, "  },{\n");  
    } else {
      len += snprintf(content + len, maxLen - len, "  }]\n");  
    }
  }
  len += snprintf(content + len, maxLen - len, "}\n"); 

  fwrite(content, 1, len, fp);
  fflush(fp);
  fclose(fp);
  free(content);
  
  dInfo("save mnode epSet successed");
}

char *dnodeGetMnodeMasterEp() {
  return tsDMnodeInfos.nodeInfos[tsDMnodeEpSet.inUse].nodeEp;
}

void* dnodeGetMnodeInfos() {
  return &tsDMnodeInfos;
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

  int32_t contLen = sizeof(SDMStatusMsg) + TSDB_MAX_VNODES * sizeof(SVnodeLoad);
  SDMStatusMsg *pStatus = rpcMallocCont(contLen);
  if (pStatus == NULL) {
    taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsDnodeTmr, &tsStatusTimer);
    dError("failed to malloc status message");
    return;
  }

  //strcpy(pStatus->dnodeName, tsDnodeName);
  pStatus->version          = htonl(tsVersion);
  pStatus->dnodeId          = htonl(tsDnodeCfg.dnodeId);
  pStatus->lastReboot       = htonl(tsRebootTime);
  pStatus->numOfCores       = htons((uint16_t) tsNumOfCores);
  pStatus->diskAvailable    = tsAvailDataDirGB;
  pStatus->alternativeRole  = (uint8_t) tsAlternativeRole;
  tstrncpy(pStatus->clusterId, tsDnodeCfg.clusterId, TSDB_CLUSTER_ID_LEN);
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
  contLen = sizeof(SDMStatusMsg) + pStatus->openVnodes * sizeof(SVnodeLoad);
  pStatus->openVnodes = htons(pStatus->openVnodes);
  
  SRpcMsg rpcMsg = {
    .pCont   = pStatus,
    .contLen = contLen,
    .msgType = TSDB_MSG_TYPE_DM_STATUS
  };

  SRpcEpSet epSet;
  dnodeGetMnodeEpSetForPeer(&epSet);
  dnodeSendMsgToDnode(&epSet, &rpcMsg);
}

static bool dnodeReadDnodeCfg() {
  char dnodeCfgFile[TSDB_FILENAME_LEN*2] = {0};
  
  sprintf(dnodeCfgFile, "%s/dnodeCfg.json", tsDnodeDir);

  FILE *fp = fopen(dnodeCfgFile, "r");
  if (!fp) {
    dDebug("failed to read dnodeCfg.json, file not exist");
    return false;
  }

  bool  ret = false;
  int   maxLen = 100;
  char *content = calloc(1, maxLen + 1);
  int   len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    free(content);
    fclose(fp);
    dError("failed to read dnodeCfg.json, content is null");
    return false;
  }

  content[len] = 0;
  cJSON* root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read dnodeCfg.json, invalid json format");
    goto PARSE_CFG_OVER;
  }

  cJSON* dnodeId = cJSON_GetObjectItem(root, "dnodeId");
  if (!dnodeId || dnodeId->type != cJSON_Number) {
    dError("failed to read dnodeCfg.json, dnodeId not found");
    goto PARSE_CFG_OVER;
  }
  tsDnodeCfg.dnodeId = dnodeId->valueint;

  cJSON* clusterId = cJSON_GetObjectItem(root, "clusterId");
  if (!clusterId || clusterId->type != cJSON_String) {
    dError("failed to read dnodeCfg.json, clusterId not found");
    goto PARSE_CFG_OVER;
  }
  tstrncpy(tsDnodeCfg.clusterId, clusterId->valuestring, TSDB_CLUSTER_ID_LEN);

  ret = true;

  dInfo("read numOfVnodes successed, dnodeId:%d", tsDnodeCfg.dnodeId);

PARSE_CFG_OVER:
  free(content);
  cJSON_Delete(root);
  fclose(fp);
  return ret;
}

static void dnodeSaveDnodeCfg() {
  char dnodeCfgFile[TSDB_FILENAME_LEN] = {0};
  sprintf(dnodeCfgFile, "%s/dnodeCfg.json", tsDnodeDir);

  FILE *fp = fopen(dnodeCfgFile, "w");
  if (!fp) return;

  int32_t len = 0;
  int32_t maxLen = 200;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeId\": %d,\n", tsDnodeCfg.dnodeId);
  len += snprintf(content + len, maxLen - len, "  \"clusterId\": \"%s\"\n", tsDnodeCfg.clusterId);
  len += snprintf(content + len, maxLen - len, "}\n");

  fwrite(content, 1, len, fp);
  fflush(fp);
  fclose(fp);
  free(content);
  
  dInfo("save dnodeId successed");
}

void dnodeUpdateDnodeCfg(SDMDnodeCfg *pCfg) {
  if (tsDnodeCfg.dnodeId == 0) {
    dInfo("dnodeId is set to %d, clusterId is set to %s", pCfg->dnodeId, pCfg->clusterId);  
    tsDnodeCfg.dnodeId = pCfg->dnodeId;
    tstrncpy(tsDnodeCfg.clusterId, pCfg->clusterId, TSDB_CLUSTER_ID_LEN);
    dnodeSaveDnodeCfg();
  }
}

int32_t dnodeGetDnodeId() {
  return tsDnodeCfg.dnodeId;
}

void dnodeSendRedirectMsg(SRpcMsg *rpcMsg, bool forShell) {
  SRpcConnInfo connInfo = {0};
  rpcGetConnInfo(rpcMsg->handle, &connInfo);

  SRpcEpSet epSet = {0};
  if (forShell) {
    dnodeGetMnodeEpSetForShell(&epSet);
  } else {
    dnodeGetMnodeEpSetForPeer(&epSet);
  }
  
  dDebug("msg:%s will be redirected, dnodeIp:%s user:%s, numOfEps:%d inUse:%d", taosMsg[rpcMsg->msgType],
         taosIpStr(connInfo.clientIp), connInfo.user, epSet.numOfEps, epSet.inUse);

  for (int i = 0; i < epSet.numOfEps; ++i) {
    dDebug("mnode index:%d %s:%d", i, epSet.fqdn[i], epSet.port[i]);
    epSet.port[i] = htons(epSet.port[i]);
  }

  rpcSendRedirectRsp(rpcMsg->handle, &epSet);
}
