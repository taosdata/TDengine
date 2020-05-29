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
#include "ttime.h"
#include "ttimer.h"
#include "tsdb.h"
#include "twal.h"
#include "tqueue.h"
#include "tsync.h"
#include "ttime.h"
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

void *          tsDnodeTmr = NULL;
static void *   tsStatusTimer = NULL;
static uint32_t tsRebootTime;

static SRpcIpSet     tsDMnodeIpSet = {0};
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
static int32_t  dnodeProcessDropVnodeMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessAlterStreamMsg(SRpcMsg *pMsg);
static int32_t  dnodeProcessConfigDnodeMsg(SRpcMsg *pMsg);
static int32_t (*dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *pMsg);

int32_t dnodeInitMgmt() {
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CREATE_VNODE] = dnodeProcessCreateVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_DROP_VNODE]   = dnodeProcessDropVnodeMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_ALTER_STREAM] = dnodeProcessAlterStreamMsg;
  dnodeProcessMgmtMsgFp[TSDB_MSG_TYPE_MD_CONFIG_DNODE] = dnodeProcessConfigDnodeMsg;

  dnodeAddClientRspHandle(TSDB_MSG_TYPE_DM_STATUS_RSP,  dnodeProcessStatusRsp);
  dnodeReadDnodeCfg();
  tsRebootTime = taosGetTimestampSec();

  if (!dnodeReadMnodeInfos()) {
    memset(&tsDMnodeIpSet, 0, sizeof(SRpcIpSet));
    memset(&tsDMnodeInfos, 0, sizeof(SDMMnodeInfos));

    tsDMnodeIpSet.numOfIps = 1;
    taosGetFqdnPortFromEp(tsFirst, tsDMnodeIpSet.fqdn[0], &tsDMnodeIpSet.port[0]);
    
    if (strcmp(tsSecond, tsFirst) != 0) {
      tsDMnodeIpSet.numOfIps = 2;
      taosGetFqdnPortFromEp(tsSecond, tsDMnodeIpSet.fqdn[1], &tsDMnodeIpSet.port[1]);
    }
  } else {
    tsDMnodeIpSet.inUse = tsDMnodeInfos.inUse;
    tsDMnodeIpSet.numOfIps = tsDMnodeInfos.nodeNum;
    for (int32_t i = 0; i < tsDMnodeInfos.nodeNum; i++) {
      taosGetFqdnPortFromEp(tsDMnodeInfos.nodeInfos[i].nodeEp, tsDMnodeIpSet.fqdn[i], &tsDMnodeIpSet.port[i]);
    }
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

  int32_t code = pthread_create(&tsQthread, &thAttr, dnodeProcessMgmtQueue, NULL);
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

  tsDnodeTmr = taosTmrInit(100, 200, 60000, "DND-DM");
  if (tsDnodeTmr == NULL) {
    dError("failed to init dnode timer");
    dnodeCleanupMgmt();
    return -1;
  }

  taosTmrReset(dnodeSendStatusMsg, 500, NULL, tsDnodeTmr, &tsStatusTimer);
  
  dPrint("dnode mgmt is initialized");
 
  return TSDB_CODE_SUCCESS;
}

void dnodeCleanupMgmt() {
  if (tsStatusTimer != NULL) {
    taosTmrStopA(&tsStatusTimer);
    tsStatusTimer = NULL;
  }

  if (tsDnodeTmr != NULL) {
    taosTmrCleanUp(tsDnodeTmr);
    tsDnodeTmr = NULL;
  }

  dnodeCloseVnodes();

  if (tsMgmtQset) taosQsetThreadResume(tsMgmtQset);
  if (tsQthread) pthread_join(tsQthread, NULL);

  if (tsMgmtQueue) taosCloseQueue(tsMgmtQueue);
  if (tsMgmtQset) taosCloseQset(tsMgmtQset);
  tsMgmtQset = NULL;
  tsMgmtQueue = NULL;

}

void dnodeDispatchToMgmtQueue(SRpcMsg *pMsg) {
  void *item;

  item = taosAllocateQitem(sizeof(SRpcMsg));
  if (item) {
    memcpy(item, pMsg, sizeof(SRpcMsg));
    taosWriteQitem(tsMgmtQueue, 1, item);
  } else {
    SRpcMsg  rsp;
    rsp.handle = pMsg->handle;
    rsp.pCont  = NULL;
    rsp.code = TSDB_CODE_SERV_OUT_OF_MEMORY;
    rpcSendResponse(&rsp);
    rpcFreeCont(pMsg->pCont);
  }
}

static void *dnodeProcessMgmtQueue(void *param) {
  SRpcMsg *pMsg;
  SRpcMsg  rsp;
  int      type;
  void    *handle;

  while (1) {
    if (taosReadQitemFromQset(tsMgmtQset, &type, (void **) &pMsg, &handle) == 0) {
      dTrace("dnode mgmt got no message from qset, exit ...");
      break;
    }

    dTrace("%p, msg:%s will be processed", pMsg->ahandle, taosMsg[pMsg->msgType]);    
    if (dnodeProcessMgmtMsgFp[pMsg->msgType]) {
      rsp.code = (*dnodeProcessMgmtMsgFp[pMsg->msgType])(pMsg);
    } else {
      rsp.code = TSDB_CODE_MSG_NOT_PROCESSED;
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
    return TSDB_CODE_NO_WRITE_ACCESS;
  }

  *numOfVnodes = 0;
  struct dirent *de = NULL;
  while ((de = readdir(dir)) != NULL) {
    if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0) continue;
    if (de->d_type & DT_DIR) {
      if (strncmp("vnode", de->d_name, 5) != 0) continue;
      int32_t vnode = atoi(de->d_name + 5);
      if (vnode == 0) continue;

      vnodeList[*numOfVnodes] = vnode;
      (*numOfVnodes)++;
    }
  }
  closedir(dir);

  return TSDB_CODE_SUCCESS;
}

static int32_t dnodeOpenVnodes() {
  char vnodeDir[TSDB_FILENAME_LEN * 3];
  int32_t failed = 0;
  int32_t *vnodeList = (int32_t *)malloc(sizeof(int32_t) * TSDB_MAX_VNODES);
  int32_t numOfVnodes;
  int32_t status;

  status = dnodeGetVnodeList(vnodeList, &numOfVnodes);

  if (status != TSDB_CODE_SUCCESS) {
    dPrint("Get dnode list failed");
    return status;
  }

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    snprintf(vnodeDir, TSDB_FILENAME_LEN * 3, "%s/vnode%d", tsVnodeDir, vnodeList[i]);
    if (vnodeOpen(vnodeList[i], vnodeDir) < 0) failed++;
  }

  free(vnodeList);
  dPrint("there are total vnodes:%d, openned:%d failed:%d", numOfVnodes, numOfVnodes-failed, failed);
  return TSDB_CODE_SUCCESS;
}

void dnodeStartStream() {
  int32_t vnodeList[TSDB_MAX_VNODES];
  int32_t numOfVnodes = 0;
  int32_t status = dnodeGetVnodeList(vnodeList, &numOfVnodes);

  if (status != TSDB_CODE_SUCCESS) {
    dPrint("Get dnode list failed");
    return;
  }

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    vnodeStartStream(vnodeList[i]);
  }

  dPrint("streams started");
}

static void dnodeCloseVnodes() {
  int32_t *vnodeList = (int32_t *)malloc(sizeof(int32_t) * TSDB_MAX_VNODES);
  int32_t numOfVnodes;
  int32_t status;

  status = dnodeGetVnodeList(vnodeList, &numOfVnodes);

  if (status != TSDB_CODE_SUCCESS) {
    dPrint("Get dnode list failed");
    return;
  }

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    vnodeClose(vnodeList[i]);
  }

  free(vnodeList);
  dPrint("total vnodes:%d are all closed", numOfVnodes);
}

static int32_t dnodeProcessCreateVnodeMsg(SRpcMsg *rpcMsg) {
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
  pCreate->cfg.commitTime          = htonl(pCreate->cfg.commitTime);

  for (int32_t j = 0; j < pCreate->cfg.replications; ++j) {
    pCreate->nodes[j].nodeId = htonl(pCreate->nodes[j].nodeId);
  }

  void *pVnode = vnodeAccquireVnode(pCreate->cfg.vgId);
  if (pVnode != NULL) {
    int32_t code = vnodeAlter(pVnode, pCreate);
    vnodeRelease(pVnode);
    return code;
  } else {
    return vnodeCreate(pCreate);
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
  SMDCfgDnodeMsg *pCfg = (SMDCfgDnodeMsg *)pMsg->pCont;
  return taosCfgDynamicOptions(pCfg->config);
}

void dnodeUpdateMnodeIpSetForPeer(SRpcIpSet *pIpSet) {
  dPrint("mnode IP list for is changed, numOfIps:%d inUse:%d", pIpSet->numOfIps, pIpSet->inUse);
  for (int i = 0; i < pIpSet->numOfIps; ++i) {
    pIpSet->port[i] -= TSDB_PORT_DNODEDNODE;
    dPrint("mnode index:%d %s:%u", i, pIpSet->fqdn[i], pIpSet->port[i])
  }

  tsDMnodeIpSet = *pIpSet;
}

void dnodeGetMnodeIpSetForPeer(void *ipSetRaw) {
  SRpcIpSet *ipSet = ipSetRaw;
  *ipSet = tsDMnodeIpSet;

  for (int i=0; i<ipSet->numOfIps; ++i) 
    ipSet->port[i] += TSDB_PORT_DNODEDNODE;
}

void dnodeGetMnodeIpSetForShell(void *ipSetRaw) {
  SRpcIpSet *ipSet = ipSetRaw;
  *ipSet = tsDMnodeIpSet;
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
  pCfg->dnodeId      = htonl(pCfg->dnodeId);

  for (int32_t i = 0; i < pMnodes->nodeNum; ++i) {
    SDMMnodeInfo *pMnodeInfo = &pMnodes->nodeInfos[i];
    pMnodeInfo->nodeId   = htonl(pMnodeInfo->nodeId);
  }

  SDMVgroupAccess *pVgAcccess = pStatusRsp->vgAccess;
  for (int32_t i = 0; i < pCfg->numOfVnodes; ++i) {
    pVgAcccess[i].vgId = htonl(pVgAcccess[i].vgId);
  }
  
  dnodeProcessModuleStatus(pCfg->moduleStatus);
  dnodeUpdateDnodeCfg(pCfg);
  dnodeUpdateMnodeInfos(pMnodes);
  taosTmrReset(dnodeSendStatusMsg, tsStatusInterval * 1000, NULL, tsDnodeTmr, &tsStatusTimer);
}

static void dnodeUpdateMnodeInfos(SDMMnodeInfos *pMnodes) {
  bool mnodesChanged = (memcmp(&tsDMnodeInfos, pMnodes, sizeof(SDMMnodeInfos)) != 0);
  bool mnodesNotInit = (tsDMnodeInfos.nodeNum == 0);
  if (!(mnodesChanged || mnodesNotInit)) return;

  memcpy(&tsDMnodeInfos, pMnodes, sizeof(SDMMnodeInfos));
  dPrint("mnode infos is changed, nodeNum:%d inUse:%d", tsDMnodeInfos.nodeNum, tsDMnodeInfos.inUse);
  for (int32_t i = 0; i < tsDMnodeInfos.nodeNum; i++) {
    dPrint("mnode index:%d, %s", tsDMnodeInfos.nodeInfos[i].nodeId, tsDMnodeInfos.nodeInfos[i].nodeEp);
  }

  tsDMnodeIpSet.inUse = tsDMnodeInfos.inUse;
  tsDMnodeIpSet.numOfIps = tsDMnodeInfos.nodeNum;
  for (int32_t i = 0; i < tsDMnodeInfos.nodeNum; i++) {
    taosGetFqdnPortFromEp(tsDMnodeInfos.nodeInfos[i].nodeEp, tsDMnodeIpSet.fqdn[i], &tsDMnodeIpSet.port[i]);
  }

  dnodeSaveMnodeInfos();
  sdbUpdateSync();
}

static bool dnodeReadMnodeInfos() {
  char ipFile[TSDB_FILENAME_LEN*2] = {0};
  
  sprintf(ipFile, "%s/mnodeIpList.json", tsDnodeDir);
  FILE *fp = fopen(ipFile, "r");
  if (!fp) {
    dTrace("failed to read mnodeIpList.json, file not exist");
    return false;
  }

  bool  ret = false;
  int   maxLen = 2000;
  char *content = calloc(1, maxLen + 1);
  int   len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    free(content);
    fclose(fp);
    dError("failed to read mnodeIpList.json, content is null");
    return false;
  }

  cJSON* root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read mnodeIpList.json, invalid json format");
    goto PARSE_OVER;
  }

  cJSON* inUse = cJSON_GetObjectItem(root, "inUse");
  if (!inUse || inUse->type != cJSON_Number) {
    dError("failed to read mnodeIpList.json, inUse not found");
    goto PARSE_OVER;
  }
  tsDMnodeInfos.inUse = inUse->valueint;

  cJSON* nodeNum = cJSON_GetObjectItem(root, "nodeNum");
  if (!nodeNum || nodeNum->type != cJSON_Number) {
    dError("failed to read mnodeIpList.json, nodeNum not found");
    goto PARSE_OVER;
  }
  tsDMnodeInfos.nodeNum = nodeNum->valueint;

  cJSON* nodeInfos = cJSON_GetObjectItem(root, "nodeInfos");
  if (!nodeInfos || nodeInfos->type != cJSON_Array) {
    dError("failed to read mnodeIpList.json, nodeInfos not found");
    goto PARSE_OVER;
  }

  int size = cJSON_GetArraySize(nodeInfos);
  if (size != tsDMnodeInfos.nodeNum) {
    dError("failed to read mnodeIpList.json, nodeInfos size not matched");
    goto PARSE_OVER;
  }

  for (int i = 0; i < size; ++i) {
    cJSON* nodeInfo = cJSON_GetArrayItem(nodeInfos, i);
    if (nodeInfo == NULL) continue;

    cJSON *nodeId = cJSON_GetObjectItem(nodeInfo, "nodeId");
    if (!nodeId || nodeId->type != cJSON_Number) {
      dError("failed to read mnodeIpList.json, nodeId not found");
      goto PARSE_OVER;
    }
    tsDMnodeInfos.nodeInfos[i].nodeId = nodeId->valueint;

    cJSON *nodeEp = cJSON_GetObjectItem(nodeInfo, "nodeEp");
    if (!nodeEp || nodeEp->type != cJSON_String || nodeEp->valuestring == NULL) {
      dError("failed to read mnodeIpList.json, nodeName not found");
      goto PARSE_OVER;
    }
    strncpy(tsDMnodeInfos.nodeInfos[i].nodeEp, nodeEp->valuestring, TSDB_EP_LEN);
 }

  ret = true;

  dPrint("read mnode iplist successed, numOfIps:%d inUse:%d", tsDMnodeInfos.nodeNum, tsDMnodeInfos.inUse);
  for (int32_t i = 0; i < tsDMnodeInfos.nodeNum; i++) {
    dPrint("mnode:%d, %s", tsDMnodeInfos.nodeInfos[i].nodeId, tsDMnodeInfos.nodeInfos[i].nodeEp);
  }

PARSE_OVER:
  free(content);
  cJSON_Delete(root);
  fclose(fp);
  return ret;
}

static void dnodeSaveMnodeInfos() {
  char ipFile[TSDB_FILENAME_LEN] = {0};
  sprintf(ipFile, "%s/mnodeIpList.json", tsDnodeDir);
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
  fclose(fp);
  free(content);
  
  dPrint("save mnode iplist successed");
}

char *dnodeGetMnodeMasterEp() {
  return tsDMnodeInfos.nodeInfos[tsDMnodeIpSet.inUse].nodeEp;
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
  strcpy(pStatus->dnodeEp, tsLocalEp);
  pStatus->lastReboot       = htonl(tsRebootTime);
  pStatus->numOfTotalVnodes = htons((uint16_t) tsNumOfTotalVnodes);
  pStatus->numOfCores       = htons((uint16_t) tsNumOfCores);
  pStatus->diskAvailable    = tsAvailDataDirGB;
  pStatus->alternativeRole  = (uint8_t) tsAlternativeRole;
  
  vnodeBuildStatusMsg(pStatus);
  contLen = sizeof(SDMStatusMsg) + pStatus->openVnodes * sizeof(SVnodeLoad);
  pStatus->openVnodes = htons(pStatus->openVnodes);
  
  SRpcMsg rpcMsg = {
    .pCont   = pStatus,
    .contLen = contLen,
    .msgType = TSDB_MSG_TYPE_DM_STATUS
  };

  SRpcIpSet ipSet;
  dnodeGetMnodeIpSetForPeer(&ipSet);
  dnodeSendMsgToDnode(&ipSet, &rpcMsg);
}

static bool dnodeReadDnodeCfg() {
  char dnodeCfgFile[TSDB_FILENAME_LEN*2] = {0};
  
  sprintf(dnodeCfgFile, "%s/dnodeCfg.json", tsDnodeDir);

  FILE *fp = fopen(dnodeCfgFile, "r");
  if (!fp) {
    dTrace("failed to read dnodeCfg.json, file not exist");
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

  ret = true;

  dPrint("read numOfVnodes successed, dnodeId:%d", tsDnodeCfg.dnodeId);

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
  int32_t maxLen = 100;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"dnodeId\": %d\n", tsDnodeCfg.dnodeId);
  len += snprintf(content + len, maxLen - len, "}\n"); 

  fwrite(content, 1, len, fp);
  fclose(fp);
  free(content);
  
  dPrint("save dnodeId successed");
}

void dnodeUpdateDnodeCfg(SDMDnodeCfg *pCfg) {
  if (tsDnodeCfg.dnodeId == 0) {
    dPrint("dnodeId is set to %d", pCfg->dnodeId);  
    tsDnodeCfg.dnodeId = pCfg->dnodeId;
    dnodeSaveDnodeCfg();
  }
}

int32_t dnodeGetDnodeId() {
  return tsDnodeCfg.dnodeId;
}

void dnodeSendRedirectMsg(SRpcMsg *rpcMsg, bool forShell) {
  SRpcConnInfo connInfo;
  rpcGetConnInfo(rpcMsg->handle, &connInfo);

  SRpcIpSet ipSet = {0};
  if (forShell) {
    dnodeGetMnodeIpSetForShell(&ipSet);
  } else {
    dnodeGetMnodeIpSetForPeer(&ipSet);
  }
  
  dTrace("msg:%s will be redirected, dnodeIp:%s user:%s, numOfIps:%d inUse:%d", taosMsg[rpcMsg->msgType],
         taosIpStr(connInfo.clientIp), connInfo.user, ipSet.numOfIps, ipSet.inUse);

  for (int i = 0; i < ipSet.numOfIps; ++i) {
    dTrace("mnode index:%d %s:%d", i, ipSet.fqdn[i], ipSet.port[i]);
    ipSet.port[i] = htons(ipSet.port[i]);
  }

  rpcSendRedirectRsp(rpcMsg->handle, &ipSet);
}
