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
#include "tlog.h"
#include "trpc.h"
#include "tutil.h"
#include "tsync.h"
#include "dnode.h"
#include "dnodeMClient.h"
#include "dnodeModule.h"
#include "dnodeMgmt.h"

#define MPEER_CONTENT_LEN 2000

static bool   dnodeReadMnodeIpList();
static void   dnodeSaveMnodeIpList();
static void   dnodeProcessRspFromMnode(SRpcMsg *pMsg);
static void   dnodeProcessStatusRsp(SRpcMsg *pMsg);
static void (*tsDnodeProcessMgmtRspFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);
static void  *tsDnodeMClientRpc = NULL;
static SRpcIpSet    tsMnodeIpList  = {0};
static SDMNodeInfos tsMnodeInfos = {0};

int32_t dnodeInitMClient() {
  if (!dnodeReadMnodeIpList()) {
    memset(&tsMnodeIpList, 0, sizeof(SRpcIpSet));
    memset(&tsMnodeInfos, 0, sizeof(SDMNodeInfos));
    tsMnodeIpList.port = tsMnodeDnodePort;
    tsMnodeIpList.numOfIps = 1;
    tsMnodeIpList.ip[0] = inet_addr(tsMasterIp);
    if (tsSecondIp[0]) {
      tsMnodeIpList.numOfIps = 2;
      tsMnodeIpList.ip[1] = inet_addr(tsSecondIp);
    }
  } else {
    tsMnodeIpList.inUse = tsMnodeInfos.inUse;
    tsMnodeIpList.numOfIps = tsMnodeInfos.nodeNum;
    tsMnodeIpList.port = tsMnodeInfos.nodeInfos[0].nodePort;
    for (int32_t i = 0; i < tsMnodeInfos.nodeNum; i++) {
      tsMnodeIpList.ip[i] = tsMnodeInfos.nodeInfos[i].nodeIp;
    }
  }

  tsDnodeProcessMgmtRspFp[TSDB_MSG_TYPE_DM_STATUS_RSP] = dnodeProcessStatusRsp;
  
  SRpcInit rpcInit;
  memset(&rpcInit, 0, sizeof(rpcInit));
  rpcInit.localIp      = tsAnyIp ? "0.0.0.0" : tsPrivateIp;
  rpcInit.localPort    = 0;
  rpcInit.label        = "DND-MC";
  rpcInit.numOfThreads = 1;
  rpcInit.cfp          = dnodeProcessRspFromMnode;
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

  dPrint("mnode rpc client is opened");
  return 0;
}

void dnodeCleanupMClient() {
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
    return;
  }

  SDMStatusRsp *pStatusRsp = pMsg->pCont;
  SDMNodeInfos *mpeers = &pStatusRsp->mpeers;
  if (mpeers->nodeNum <= 0) {
    dError("status msg is invalid, num of ips is %d", mpeers->nodeNum);
    return;
  }

  SRpcIpSet mgmtIpSet = {0};
  mgmtIpSet.inUse = mpeers->inUse;
  mgmtIpSet.numOfIps = mpeers->nodeNum;
  mgmtIpSet.port = htons(mpeers->nodeInfos[0].nodePort);
  for (int32_t i = 0; i < mpeers->nodeNum; i++) {
    mgmtIpSet.ip[i] = htonl(mpeers->nodeInfos[i].nodeIp);
  }

  if (memcmp(&mgmtIpSet, &tsMnodeIpList, sizeof(SRpcIpSet)) != 0) {
    memcpy(&tsMnodeIpList, &mgmtIpSet, sizeof(SRpcIpSet));  
    memcpy(&tsMnodeInfos, mpeers, sizeof(SDMNodeInfos));
    dPrint("mnode ip list is changed, numOfIps:%d inUse:%d", tsMnodeInfos.nodeNum, tsMnodeInfos.inUse);
    for (int32_t i = 0; i < mpeers->nodeNum; i++) {
      tsMnodeInfos.nodeInfos[i].nodeId = htonl(mpeers->nodeInfos[i].nodeId);
      tsMnodeInfos.nodeInfos[i].nodeIp = htonl(mpeers->nodeInfos[i].nodeIp);
      tsMnodeInfos.nodeInfos[i].nodePort = htons(mpeers->nodeInfos[i].nodePort);
      dPrint("mnode:%d, ip:%s:%u name:%s", tsMnodeInfos.nodeInfos[i].nodeId,
             taosIpStr(tsMnodeInfos.nodeInfos[i].nodeId), tsMnodeInfos.nodeInfos[i].nodePort,
             tsMnodeInfos.nodeInfos[i].nodeName);
    }
    dnodeSaveMnodeIpList();
  }

  SDnodeState *pState  = &pStatusRsp->dnodeState;
  pState->numOfVnodes  = htonl(pState->numOfVnodes);
  pState->moduleStatus = htonl(pState->moduleStatus);
  pState->createdTime  = htonl(pState->createdTime);
  pState->dnodeId      = htonl(pState->dnodeId);
  
  dnodeProcessModuleStatus(pState->moduleStatus);
  dnodeUpdateDnodeId(pState->dnodeId);
}

void dnodeSendMsgToMnode(SRpcMsg *rpcMsg) {
  if (tsDnodeMClientRpc) {
    rpcSendRequest(tsDnodeMClientRpc, &tsMnodeIpList, rpcMsg);
  }
}

static bool dnodeReadMnodeIpList() {
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

    cJSON *nodeName = cJSON_GetObjectItem(nodeInfo, "nodeName");
    if (!nodeIp || nodeName->type != cJSON_String || nodeName->valuestring == NULL) {
      dError("failed to read mnode mgmtIpList.json, nodeName not found");
      goto PARSE_OVER;
    }
    strncpy(tsMnodeInfos.nodeInfos[i].nodeName, nodeName->valuestring, TSDB_NODE_NAME_LEN);
  }

  ret = true;

  dPrint("read mnode iplist successed, numOfIps:%d inUse:%d", tsMnodeInfos.nodeNum, tsMnodeInfos.inUse);
  for (int32_t i = 0; i < tsMnodeInfos.nodeNum; i++) {
    dPrint("mnode:%d, ip:%s:%u name:%s", tsMnodeInfos.nodeInfos[i].nodeId,
            taosIpStr(tsMnodeInfos.nodeInfos[i].nodeId), tsMnodeInfos.nodeInfos[i].nodePort,
            tsMnodeInfos.nodeInfos[i].nodeName);
  }

PARSE_OVER:
  free(content);
  cJSON_Delete(root);
  fclose(fp);
  return ret;
}

static void dnodeSaveMnodeIpList() {
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
  return tsMnodeIpList.ip[tsMnodeIpList.inUse];
}

void* dnodeGetMpeerInfos() {
  return &tsMnodeInfos;
}
