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
#include "ihash.h"
#include "taoserror.h"
#include "taosmsg.h"
#include "ttime.h"
#include "ttimer.h"
#include "trpc.h"
#include "tsdb.h"
#include "twal.h"
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

static void   dnodeUpdateMnodeInfos(SDMMnodeInfos *pMnodes);
static bool   dnodeReadMnodeInfos();
static void   dnodeSaveMnodeInfos();
static void   dnodeUpdateDnodeCfg(SDMDnodeCfg *pCfg);
static bool   dnodeReadDnodeCfg();
static void   dnodeSaveDnodeCfg();
static void   dnodeProcessStatusRsp(SRpcMsg *pMsg);
static void   dnodeSendStatusMsg(void *handle, void *tmrId);

static void    *tsDnodeTmr = NULL;
static void    *tsStatusTimer = NULL;
static uint32_t tsRebootTime;

static SRpcIpSet     tsMnodeIpSet  = {0};
static SDMMnodeInfos tsMnodeInfos = {0};
static SDMDnodeCfg   tsDnodeCfg = {0};

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

  tsDnodeTmr = taosTmrInit(100, 200, 60000, "DND-DM");
  if (tsDnodeTmr == NULL) {
    dError("failed to init dnode timer");
    return -1;
  }
  
  if (!dnodeReadMnodeInfos()) {
    memset(&tsMnodeIpSet, 0, sizeof(SRpcIpSet));
    memset(&tsMnodeInfos, 0, sizeof(SDMMnodeInfos));
    tsMnodeIpSet.numOfIps = 1;
    taosGetFqdnPortFromEp(tsFirst, tsMnodeIpSet.fqdn[0], &tsMnodeIpSet.port[0]);
    tsMnodeIpSet.port[0] += TSDB_PORT_DNODEDNODE;
    if (strcmp(tsSecond, tsFirst) != 0) {
      tsMnodeIpSet.numOfIps = 2;
      taosGetFqdnPortFromEp(tsSecond, tsMnodeIpSet.fqdn[1], &tsMnodeIpSet.port[1]);
      tsMnodeIpSet.port[1] += TSDB_PORT_DNODEDNODE;
    }
  } else {
    tsMnodeIpSet.inUse = tsMnodeInfos.inUse;
    tsMnodeIpSet.numOfIps = tsMnodeInfos.nodeNum;
    for (int32_t i = 0; i < tsMnodeInfos.nodeNum; i++) {
      taosGetFqdnPortFromEp(tsMnodeInfos.nodeInfos[i].nodeEp, tsMnodeIpSet.fqdn[i], &tsMnodeIpSet.port[i]);
      tsMnodeIpSet.port[i] += TSDB_PORT_DNODEDNODE;
    }
  }

  int32_t code = dnodeOpenVnodes();
  if (code != TSDB_CODE_SUCCESS) {
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
}

void dnodeDispatchToDnodeMgmt(SRpcMsg *pMsg) {
  SRpcMsg rsp;

  if (dnodeProcessMgmtMsgFp[pMsg->msgType]) {
    rsp.code = (*dnodeProcessMgmtMsgFp[pMsg->msgType])(pMsg);
  } else {
    rsp.code = TSDB_CODE_MSG_NOT_PROCESSED;
  }

  rsp.handle = pMsg->handle;
  rsp.pCont  = NULL;
  rpcSendResponse(&rsp);

  rpcFreeCont(pMsg->pCont);
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


void dnodeUpdateIpSet(void *ahandle, SRpcIpSet *pIpSet) {
  dTrace("mgmt IP list is changed for ufp is called");
  tsMnodeIpSet = *pIpSet;
}

void dnodeGetMnodeDnodeIpSet(void *ipSetRaw) {
  SRpcIpSet *ipSet = ipSetRaw;
  ipSet->numOfIps = tsMnodeInfos.nodeNum;
  ipSet->inUse = tsMnodeInfos.inUse;
  for (int32_t i = 0; i < tsMnodeInfos.nodeNum; ++i) {
    taosGetFqdnPortFromEp(tsMnodeInfos.nodeInfos[i].nodeEp, ipSet->fqdn[i], &ipSet->port[i]);
    ipSet->port[i] += TSDB_PORT_DNODEDNODE;
  }
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
  bool mnodesChanged = (memcmp(&tsMnodeInfos, pMnodes, sizeof(SDMMnodeInfos)) != 0);
  bool mnodesNotInit = (tsMnodeInfos.nodeNum == 0);
  if (!(mnodesChanged || mnodesNotInit)) return;

  memcpy(&tsMnodeInfos, pMnodes, sizeof(SDMMnodeInfos));

  tsMnodeIpSet.inUse = tsMnodeInfos.inUse;
  tsMnodeIpSet.numOfIps = tsMnodeInfos.nodeNum;
  for (int32_t i = 0; i < tsMnodeInfos.nodeNum; i++) {
    taosGetFqdnPortFromEp(tsMnodeInfos.nodeInfos[i].nodeEp, tsMnodeIpSet.fqdn[i], &tsMnodeIpSet.port[i]);
    tsMnodeIpSet.port[i] += TSDB_PORT_DNODEDNODE;
  }

  dPrint("mnodes is changed, nodeNum:%d inUse:%d", tsMnodeInfos.nodeNum, tsMnodeInfos.inUse);
  for (int32_t i = 0; i < tsMnodeInfos.nodeNum; i++) {
    dPrint("mnode:%d, %s", tsMnodeInfos.nodeInfos[i].nodeId, tsMnodeInfos.nodeInfos[i].nodeEp);
  }

  dnodeSaveMnodeInfos();
  sdbUpdateSync();
}

static bool dnodeReadMnodeInfos() {
  char ipFile[TSDB_FILENAME_LEN] = {0};
  sprintf(ipFile, "%s/mgmtIpList.json", tsDnodeDir);
  FILE *fp = fopen(ipFile, "r");
  if (!fp) {
    dTrace("failed to read mnode mgmtIpList.json, file not exist");
    return false;
  }

  bool  ret = false;
  int   maxLen = 2000;
  char *content = calloc(1, maxLen + 1);
  int   len = fread(content, 1, maxLen, fp);
  if (len <= 0) {
    free(content);
    fclose(fp);
    dError("failed to read mnode mgmtIpList.json, content is null");
    return false;
  }

  cJSON* root = cJSON_Parse(content);
  if (root == NULL) {
    dError("failed to read mnode mgmtIpList.json, invalid json format");
    goto PARSE_OVER;
  }

  cJSON* inUse = cJSON_GetObjectItem(root, "inUse");
  if (!inUse || inUse->type != cJSON_Number) {
    dError("failed to read mnode mgmtIpList.json, inUse not found");
    goto PARSE_OVER;
  }
  tsMnodeInfos.inUse = inUse->valueint;

  cJSON* nodeNum = cJSON_GetObjectItem(root, "nodeNum");
  if (!nodeNum || nodeNum->type != cJSON_Number) {
    dError("failed to read mnode mgmtIpList.json, nodeNum not found");
    goto PARSE_OVER;
  }
  tsMnodeInfos.nodeNum = nodeNum->valueint;

  cJSON* nodeInfos = cJSON_GetObjectItem(root, "nodeInfos");
  if (!nodeInfos || nodeInfos->type != cJSON_Array) {
    dError("failed to read mnode mgmtIpList.json, nodeInfos not found");
    goto PARSE_OVER;
  }

  int size = cJSON_GetArraySize(nodeInfos);
  if (size != tsMnodeInfos.nodeNum) {
    dError("failed to read mnode mgmtIpList.json, nodeInfos size not matched");
    goto PARSE_OVER;
  }

  for (int i = 0; i < size; ++i) {
    cJSON* nodeInfo = cJSON_GetArrayItem(nodeInfos, i);
    if (nodeInfo == NULL) continue;

    cJSON *nodeId = cJSON_GetObjectItem(nodeInfo, "nodeId");
    if (!nodeId || nodeId->type != cJSON_Number) {
      dError("failed to read mnode mgmtIpList.json, nodeId not found");
      goto PARSE_OVER;
    }
    tsMnodeInfos.nodeInfos[i].nodeId = nodeId->valueint;

    cJSON *nodeEp = cJSON_GetObjectItem(nodeInfo, "nodeEp");
    if (!nodeEp || nodeEp->type != cJSON_String || nodeEp->valuestring == NULL) {
      dError("failed to read mnode mgmtIpList.json, nodeName not found");
      goto PARSE_OVER;
    }
    strncpy(tsMnodeInfos.nodeInfos[i].nodeEp, nodeEp->valuestring, TSDB_FQDN_LEN);
 }

  ret = true;

  dPrint("read mnode iplist successed, numOfIps:%d inUse:%d", tsMnodeInfos.nodeNum, tsMnodeInfos.inUse);
  for (int32_t i = 0; i < tsMnodeInfos.nodeNum; i++) {
    dPrint("mnode:%d, %s", tsMnodeInfos.nodeInfos[i].nodeId, tsMnodeInfos.nodeInfos[i].nodeEp);
  }

PARSE_OVER:
  free(content);
  cJSON_Delete(root);
  fclose(fp);
  return ret;
}

static void dnodeSaveMnodeInfos() {
  char ipFile[TSDB_FILENAME_LEN] = {0};
  sprintf(ipFile, "%s/mgmtIpList.json", tsDnodeDir);
  FILE *fp = fopen(ipFile, "w");
  if (!fp) return;

  int32_t len = 0;
  int32_t maxLen = 2000;
  char *  content = calloc(1, maxLen + 1);

  len += snprintf(content + len, maxLen - len, "{\n");
  len += snprintf(content + len, maxLen - len, "  \"inUse\": %d,\n", tsMnodeInfos.inUse);
  len += snprintf(content + len, maxLen - len, "  \"nodeNum\": %d,\n", tsMnodeInfos.nodeNum);
  len += snprintf(content + len, maxLen - len, "  \"nodeInfos\": [{\n");
  for (int32_t i = 0; i < tsMnodeInfos.nodeNum; i++) {
    len += snprintf(content + len, maxLen - len, "    \"nodeId\": %d,\n", tsMnodeInfos.nodeInfos[i].nodeId);
    len += snprintf(content + len, maxLen - len, "    \"nodeEp\": \"%s\"\n", tsMnodeInfos.nodeInfos[i].nodeEp);
    if (i < tsMnodeInfos.nodeNum -1) {
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
  return tsMnodeInfos.nodeInfos[tsMnodeIpSet.inUse].nodeEp;
}

void* dnodeGetMnodeInfos() {
  return &tsMnodeInfos;
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

  dnodeSendMsgToDnode(&tsMnodeIpSet, &rpcMsg);
}

static bool dnodeReadDnodeCfg() {
  char dnodeCfgFile[TSDB_FILENAME_LEN] = {0};
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

