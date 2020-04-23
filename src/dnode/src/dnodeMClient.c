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
#include "taosmsg.h"
#include "trpc.h"
#include "tutil.h"
#include "tsync.h"
#include "ttime.h"
#include "ttimer.h"
#include "tbalance.h"
#include "tglobal.h"
#include "vnode.h"
#include "mnode.h"
#include "dnode.h"
#include "dnodeLog.h"
#include "dnodeMClient.h"
#include "dnodeModule.h"
#include "dnodeMgmt.h"

#define MPEER_CONTENT_LEN 2000

static void   dnodeUpdateMnodeInfos(SDMMnodeInfos *pMnodes);
static bool   dnodeReadMnodeInfos();
static void   dnodeSaveMnodeInfos();
static void   dnodeUpdateDnodeCfg(SDMDnodeCfg *pCfg);
static bool   dnodeReadDnodeCfg();
static void   dnodeSaveDnodeCfg();
static void   dnodeProcessRspFromMnode(SRpcMsg *pMsg);
static void   dnodeProcessStatusRsp(SRpcMsg *pMsg);
static void   dnodeSendStatusMsg(void *handle, void *tmrId);
static void (*tsDnodeProcessMgmtRspFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);

static void    *tsDnodeMClientRpc = NULL;
static void    *tsDnodeTmr = NULL;
static void    *tsStatusTimer = NULL;
static uint32_t tsRebootTime;

static SRpcIpSet     tsMnodeIpSet  = {0};
static SDMMnodeInfos tsMnodeInfos = {0};
static SDMDnodeCfg   tsDnodeCfg = {0};

void dnodeUpdateIpSet(void *ahandle, SRpcIpSet *pIpSet) {
  dTrace("mgmt IP list is changed for ufp is called");
  tsMnodeIpSet = *pIpSet;
}

int32_t dnodeInitMClient() {
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
    tsMnodeIpSet.port = tsMnodeDnodePort;
    tsMnodeIpSet.numOfIps = 1;
    tsMnodeIpSet.ip[0] = inet_addr(tsMasterIp);
    if (strcmp(tsSecondIp, tsMasterIp) != 0) {
      tsMnodeIpSet.numOfIps = 2;
      tsMnodeIpSet.ip[1] = inet_addr(tsSecondIp);
    }
  } else {
    tsMnodeIpSet.inUse = tsMnodeInfos.inUse;
    tsMnodeIpSet.numOfIps = tsMnodeInfos.nodeNum;
    tsMnodeIpSet.port = tsMnodeInfos.nodeInfos[0].nodePort;
    for (int32_t i = 0; i < tsMnodeInfos.nodeNum; i++) {
      tsMnodeIpSet.ip[i] = tsMnodeInfos.nodeInfos[i].nodeIp;
    }
  }

  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp      = tsAnyIp ? "0.0.0.0" : tsPrivateIp;
  rpcInit.localPort    = 0;
  rpcInit.label        = "DND-MC";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = dnodeProcessRspFromMnode;
  rpcInit.ufp          = dnodeUpdateIpSet;
  rpcInit.sessions     = 100;
  rpcInit.connType     = TAOS_CONN_CLIENT;
  rpcInit.idleTime     = tsShellActivityTimer * 2000;
  rpcInit.user         = "t";
  rpcInit.ckey         = "key";
  rpcInit.secret       = "secret";

  tsDnodeMClientRpc = rpcOpen(&rpcInit);
  if (tsDnodeMClientRpc == NULL) {
    dError("failed to init mnode rpc client");
    return -1;
  }

  tsDnodeProcessMgmtRspFp[TSDB_MSG_TYPE_DM_STATUS_RSP] = dnodeProcessStatusRsp;
  taosTmrReset(dnodeSendStatusMsg, 500, NULL, tsDnodeTmr, &tsStatusTimer);
  
  dPrint("mnode rpc client is opened");
  return 0;
}

void dnodeCleanupMClient() {
  if (tsStatusTimer != NULL) {
    taosTmrStopA(&tsStatusTimer);
    tsStatusTimer = NULL;
  }

  if (tsDnodeTmr != NULL) {
    taosTmrCleanUp(tsDnodeTmr);
    tsDnodeTmr = NULL;
  }

  if (tsDnodeMClientRpc) {
    rpcClose(tsDnodeMClientRpc);
    tsDnodeMClientRpc = NULL;
    dPrint("mnode rpc client is closed");
  }
}

static void dnodeProcessRspFromMnode(SRpcMsg *pMsg) {
  if (tsDnodeProcessMgmtRspFp[pMsg->msgType]) {
    (*tsDnodeProcessMgmtRspFp[pMsg->msgType])(pMsg);
  } else {
    dError("%s is not processed in mnode rpc client", taosMsg[pMsg->msgType]);
  }

  rpcFreeCont(pMsg->pCont);
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
    pMnodeInfo->nodeIp   = htonl(pMnodeInfo->nodeIp);
    pMnodeInfo->nodePort = htons(pMnodeInfo->nodePort);
    pMnodeInfo->syncPort = htons(pMnodeInfo->syncPort);
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
  tsMnodeIpSet.port = tsMnodeInfos.nodeInfos[0].nodePort;
  for (int32_t i = 0; i < tsMnodeInfos.nodeNum; i++) {
    tsMnodeIpSet.ip[i] = tsMnodeInfos.nodeInfos[i].nodeIp;
  }

  dPrint("mnodes is changed, nodeNum:%d inUse:%d", tsMnodeInfos.nodeNum, tsMnodeInfos.inUse);
  for (int32_t i = 0; i < tsMnodeInfos.nodeNum; i++) {
    dPrint("mnode:%d, ip:%s:%u name:%s", tsMnodeInfos.nodeInfos[i].nodeId, taosIpStr(tsMnodeInfos.nodeInfos[i].nodeIp),
           tsMnodeInfos.nodeInfos[i].nodePort, tsMnodeInfos.nodeInfos[i].nodeName);
  }

  dnodeSaveMnodeInfos();
  sdbUpdateSync();
}

void dnodeSendMsgToMnode(SRpcMsg *rpcMsg) {
  if (tsDnodeMClientRpc) {
    rpcSendRequest(tsDnodeMClientRpc, &tsMnodeIpSet, rpcMsg);
  }
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

    cJSON *nodeIp = cJSON_GetObjectItem(nodeInfo, "nodeIp");
    if (!nodeIp || nodeIp->type != cJSON_String || nodeIp->valuestring == NULL) {
      dError("failed to read mnode mgmtIpList.json, nodeIp not found");
      goto PARSE_OVER;
    }
    tsMnodeInfos.nodeInfos[i].nodeIp = inet_addr(nodeIp->valuestring);

    cJSON *nodePort = cJSON_GetObjectItem(nodeInfo, "nodePort");
    if (!nodePort || nodePort->type != cJSON_Number) {
      dError("failed to read mnode mgmtIpList.json, nodePort not found");
      goto PARSE_OVER;
    }
    tsMnodeInfos.nodeInfos[i].nodePort = (uint16_t)nodePort->valueint;

    cJSON *syncPort = cJSON_GetObjectItem(nodeInfo, "syncPort");
    if (!syncPort || syncPort->type != cJSON_Number) {
      dError("failed to read mnode mgmtIpList.json, syncPort not found");
      goto PARSE_OVER;
    }
    tsMnodeInfos.nodeInfos[i].syncPort = (uint16_t)syncPort->valueint;

    cJSON *nodeName = cJSON_GetObjectItem(nodeInfo, "nodeName");
    if (!nodeName || nodeName->type != cJSON_String || nodeName->valuestring == NULL) {
      dError("failed to read mnode mgmtIpList.json, nodeName not found");
      goto PARSE_OVER;
    }
    strncpy(tsMnodeInfos.nodeInfos[i].nodeName, nodeName->valuestring, TSDB_NODE_NAME_LEN);
  }

  ret = true;

  dPrint("read mnode iplist successed, numOfIps:%d inUse:%d", tsMnodeInfos.nodeNum, tsMnodeInfos.inUse);
  for (int32_t i = 0; i < tsMnodeInfos.nodeNum; i++) {
    dPrint("mnode:%d, ip:%s:%u name:%s", tsMnodeInfos.nodeInfos[i].nodeId,
            taosIpStr(tsMnodeInfos.nodeInfos[i].nodeIp), tsMnodeInfos.nodeInfos[i].nodePort,
            tsMnodeInfos.nodeInfos[i].nodeName);
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
    len += snprintf(content + len, maxLen - len, "    \"nodeIp\": \"%s\",\n", taosIpStr(tsMnodeInfos.nodeInfos[i].nodeIp));
    len += snprintf(content + len, maxLen - len, "    \"nodePort\": %u,\n", tsMnodeInfos.nodeInfos[i].nodePort);
    len += snprintf(content + len, maxLen - len, "    \"syncPort\": %u,\n", tsMnodeInfos.nodeInfos[i].syncPort);
    len += snprintf(content + len, maxLen - len, "    \"nodeName\": \"%s\"\n",  tsMnodeInfos.nodeInfos[i].nodeName);
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

uint32_t dnodeGetMnodeMasteIp() {
  return tsMnodeIpSet.ip[tsMnodeIpSet.inUse];
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
  pStatus->privateIp        = htonl(inet_addr(tsPrivateIp));
  pStatus->publicIp         = htonl(inet_addr(tsPublicIp));
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

  dnodeSendMsgToMnode(&rpcMsg);
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