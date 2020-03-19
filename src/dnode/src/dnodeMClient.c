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
#include "taosmsg.h"
#include "tlog.h"
#include "trpc.h"
#include "tutil.h"
#include "dnode.h"
#include "dnodeMClient.h"
#include "dnodeModule.h"
#include "dnodeMgmt.h"

static bool   dnodeReadMnodeIpList();
static void   dnodeSaveMnodeIpList();
static void   dnodeProcessRspFromMnode(SRpcMsg *pMsg);
static void   dnodeProcessStatusRsp(SRpcMsg *pMsg);
static void (*tsDnodeProcessMgmtRspFp[TSDB_MSG_TYPE_MAX])(SRpcMsg *);
static void  *tsDnodeMClientRpc = NULL;
static SRpcIpSet tsDnodeMnodeIpList  = {0};

int32_t dnodeInitMClient() {
  if (!dnodeReadMnodeIpList()) {
    dTrace("failed to read mnode iplist, set it from cfg file");
    memset(&tsDnodeMnodeIpList, 0, sizeof(SRpcIpSet));
    tsDnodeMnodeIpList.port = tsMnodeDnodePort;
    tsDnodeMnodeIpList.numOfIps = 1;
    tsDnodeMnodeIpList.ip[0] = inet_addr(tsMasterIp);
    if (tsSecondIp[0]) {
      tsDnodeMnodeIpList.numOfIps = 2;
      tsDnodeMnodeIpList.ip[1] = inet_addr(tsSecondIp);
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
  if (pStatusRsp->ipList.numOfIps <= 0) {
    dError("status msg is invalid, num of ips is %d", pStatusRsp->ipList.numOfIps);
    return;
  }

  pStatusRsp->ipList.port = htons(pStatusRsp->ipList.port);
  for (int32_t i = 0; i < pStatusRsp->ipList.numOfIps; ++i) {
    pStatusRsp->ipList.ip[i] = htonl(pStatusRsp->ipList.ip[i]);
  }

  //dTrace("status msg is received, result:%s", tstrerror(pMsg->code));

  if (memcmp(&(pStatusRsp->ipList), &tsDnodeMnodeIpList, sizeof(SRpcIpSet)) != 0) {
    dPrint("mnode ip list is changed, numOfIps:%d inUse:%d", pStatusRsp->ipList.numOfIps, pStatusRsp->ipList.inUse);
    memcpy(&tsDnodeMnodeIpList, &pStatusRsp->ipList, sizeof(SRpcIpSet));
    for (int32_t i = 0; i < tsDnodeMnodeIpList.numOfIps; ++i) {
      dPrint("mnode index:%d ip:%s", i, taosIpStr(tsDnodeMnodeIpList.ip[i]));
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
  rpcSendRequest(tsDnodeMClientRpc, &tsDnodeMnodeIpList, rpcMsg);
}

static bool dnodeReadMnodeIpList() {
  char ipFile[TSDB_FILENAME_LEN] = {0};
  sprintf(ipFile, "%s/iplist", tsDnodeDir);

  FILE *fp = fopen(ipFile, "r");
  if (!fp) return false;
  
  char option[32] = {0};
  int32_t value = 0;
  int32_t num = 0;
  
  num = fscanf(fp, "%s %d", option, &value);
  if (num != 2) return false;
  if (strcmp(option, "inUse") != 0) return false;
  tsDnodeMnodeIpList.inUse = (int8_t)value;;

  num = fscanf(fp, "%s %d", option, &value);
  if (num != 2) return false;
  if (strcmp(option, "numOfIps") != 0) return false;
  tsDnodeMnodeIpList.numOfIps = (int8_t)value;

  num = fscanf(fp, "%s %d", option, &value);
  if (num != 2) return false;
  if (strcmp(option, "port") != 0) return false;
  tsDnodeMnodeIpList.port = (uint16_t)value;

  for (int32_t i = 0; i < tsDnodeMnodeIpList.numOfIps; i++) {
    num = fscanf(fp, "%s %d", option, &value);
    if (num != 2) return false;
    if (strncmp(option, "ip", 2) != 0) return false;
    tsDnodeMnodeIpList.ip[i] = (uint32_t)value;
  }

  fclose(fp);
  dPrint("read mnode iplist successed");
  for (int32_t i = 0; i < tsDnodeMnodeIpList.numOfIps; i++) {
    dPrint("mnode index:%d ip:%s", i, taosIpStr(tsDnodeMnodeIpList.ip[i]));
  } 

  return true;
}

static void dnodeSaveMnodeIpList() {
  char ipFile[TSDB_FILENAME_LEN] = {0};
  sprintf(ipFile, "%s/iplist", tsDnodeDir);

  FILE *fp = fopen(ipFile, "w");
  if (!fp) return;

  fprintf(fp, "inUse %d\n", tsDnodeMnodeIpList.inUse);
  fprintf(fp, "numOfIps %d\n", tsDnodeMnodeIpList.numOfIps);
  fprintf(fp, "port %u\n", tsDnodeMnodeIpList.port);
  for (int32_t i = 0; i < tsDnodeMnodeIpList.numOfIps; i++) {
    fprintf(fp, "ip%d %u\n", i, tsDnodeMnodeIpList.ip[i]);
  }
  
  fclose(fp);
  dPrint("save mnode iplist successed");
}

uint32_t dnodeGetMnodeMasteIp() {
  return tsDnodeMnodeIpList.ip[0];
}