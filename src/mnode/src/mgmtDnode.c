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
#include "tmodule.h"
#include "tbalance.h"
#include "tcluster.h"
#include "mnode.h"
#include "mgmtDClient.h"
#include "mgmtMnode.h"
#include "mgmtShell.h"
#include "mgmtDServer.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"

static void    clusterProcessCfgDnodeMsg(SQueuedMsg *pMsg);
static void    clusterProcessCfgDnodeMsgRsp(SRpcMsg *rpcMsg) ;
static void    clusterProcessDnodeStatusMsg(SRpcMsg *rpcMsg);
static int32_t clusterGetModuleMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t clusterRetrieveModules(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t clusterGetConfigMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t clusterRetrieveConfigs(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t clusterGetVnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t clusterRetrieveVnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t clusterGetDnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t clusterRetrieveDnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);

#ifndef _CLUSTER

static SDnodeObj  tsDnodeObj = {0};

int32_t clusterInitDnodes() {
  tsDnodeObj.dnodeId          = 1;
  tsDnodeObj.privateIp        = inet_addr(tsPrivateIp);
  tsDnodeObj.publicIp         = inet_addr(tsPublicIp);
  tsDnodeObj.createdTime      = taosGetTimestampMs();
  tsDnodeObj.numOfTotalVnodes = tsNumOfTotalVnodes;
  tsDnodeObj.status           = TAOS_DN_STATUS_OFFLINE;
  tsDnodeObj.lastReboot       = taosGetTimestampSec();
  sprintf(tsDnodeObj.dnodeName, "%d", tsDnodeObj.dnodeId);

  tsDnodeObj.moduleStatus |= (1 << TSDB_MOD_MGMT);
  if (tsEnableHttpModule) {
    tsDnodeObj.moduleStatus |= (1 << TSDB_MOD_HTTP);
  }
  if (tsEnableMonitorModule) {
    tsDnodeObj.moduleStatus |= (1 << TSDB_MOD_MONITOR);
  }
  return 0;
}

void *clusterGetNextDnode(void *pNode, SDnodeObj **pDnode) {
  if (*pDnode == NULL) {
    *pDnode = &tsDnodeObj;
  } else {
    *pDnode = NULL;
  }
  return *pDnode;
}

void    clusterCleanupDnodes() {}
int32_t clusterGetDnodesNum() { return 1; }
void *  clusterGetDnode(int32_t dnodeId) { return dnodeId == 1 ? &tsDnodeObj : NULL; }
void *  clusterGetDnodeByIp(uint32_t ip) { return &tsDnodeObj; }
void    clusterReleaseDnode(struct _dnode_obj *pDnode) {}
void    clusterUpdateDnode(struct _dnode_obj *pDnode) {}

#endif

int32_t clusterInit() {
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_CONFIG_DNODE, clusterProcessCfgDnodeMsg);
  mgmtAddDClientRspHandle(TSDB_MSG_TYPE_MD_CONFIG_DNODE_RSP, clusterProcessCfgDnodeMsgRsp);
  mgmtAddDServerMsgHandle(TSDB_MSG_TYPE_DM_STATUS, clusterProcessDnodeStatusMsg);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_MODULE, clusterGetModuleMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_MODULE, clusterRetrieveModules);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_CONFIGS, clusterGetConfigMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_CONFIGS, clusterRetrieveConfigs);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_VNODES, clusterGetVnodeMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_VNODES, clusterRetrieveVnodes);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_DNODE, clusterGetDnodeMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_DNODE, clusterRetrieveDnodes);
 
  return clusterInitDnodes();
}

void clusterCleanUp() {
  clusterCleanupDnodes();
}

void clusterProcessCfgDnodeMsg(SQueuedMsg *pMsg) {
  SRpcMsg rpcRsp = {.handle = pMsg->thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  
  SCMCfgDnodeMsg *pCmCfgDnode = pMsg->pCont;
  if (pCmCfgDnode->ip[0] == 0) {
    strcpy(pCmCfgDnode->ip, tsPrivateIp);
  } else {
    strcpy(pCmCfgDnode->ip, pCmCfgDnode->ip);
  }
  uint32_t dnodeIp = inet_addr(pCmCfgDnode->ip);

  if (strcmp(pMsg->pUser->pAcct->user, "root") != 0) {
    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
  } else {
    SRpcIpSet ipSet = mgmtGetIpSetFromIp(dnodeIp);
    SMDCfgDnodeMsg *pMdCfgDnode = rpcMallocCont(sizeof(SMDCfgDnodeMsg));
    strcpy(pMdCfgDnode->ip, pCmCfgDnode->ip);
    strcpy(pMdCfgDnode->config, pCmCfgDnode->config);
    SRpcMsg rpcMdCfgDnodeMsg = {
        .handle = 0,
        .code = 0,
        .msgType = TSDB_MSG_TYPE_MD_CONFIG_DNODE,
        .pCont = pMdCfgDnode,
        .contLen = sizeof(SMDCfgDnodeMsg)
    };
    mgmtSendMsgToDnode(&ipSet, &rpcMdCfgDnodeMsg);
    rpcRsp.code = TSDB_CODE_SUCCESS;
  }

  if (rpcRsp.code == TSDB_CODE_SUCCESS) {
    mPrint("dnode:%s, is configured by %s", pCmCfgDnode->ip, pMsg->pUser->user);
  }

  rpcSendResponse(&rpcRsp);
}

static void clusterProcessCfgDnodeMsgRsp(SRpcMsg *rpcMsg) {
  mPrint("cfg vnode rsp is received, result:%s", tstrerror(rpcMsg->code));
}

void clusterProcessDnodeStatusMsg(SRpcMsg *rpcMsg) {
  if (mgmtCheckRedirect(rpcMsg->handle)) return;

  SDMStatusMsg *pStatus = rpcMsg->pCont;
  pStatus->dnodeId = htonl(pStatus->dnodeId);
  pStatus->privateIp = htonl(pStatus->privateIp);
  pStatus->publicIp = htonl(pStatus->publicIp);
  pStatus->lastReboot = htonl(pStatus->lastReboot);
  pStatus->numOfCores = htons(pStatus->numOfCores);
  pStatus->numOfTotalVnodes = htons(pStatus->numOfTotalVnodes);

  uint32_t version = htonl(pStatus->version);
  if (version != tsVersion) {
    mError("status msg version:%d not equal with mnode:%d", version, tsVersion);
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_INVALID_MSG_VERSION);
    return ;
  }

  SDnodeObj *pDnode = NULL;
  if (pStatus->dnodeId == 0) {
    pDnode = clusterGetDnodeByIp(pStatus->privateIp);
    if (pDnode == NULL) {
      mTrace("dnode not created, privateIp:%s", taosIpStr(pStatus->privateIp));
      mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_DNODE_NOT_EXIST);
      return;
    }
  } else {
    pDnode = clusterGetDnode(pStatus->dnodeId);
    if (pDnode == NULL) {
      mError("dnode:%d, not exist, privateIp:%s", pStatus->dnodeId, taosIpStr(pStatus->privateIp));
      mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_DNODE_NOT_EXIST);
      return;
    }
  }
  
  pDnode->privateIp        = pStatus->privateIp;
  pDnode->publicIp         = pStatus->publicIp;
  pDnode->lastReboot       = pStatus->lastReboot;
  pDnode->numOfCores       = pStatus->numOfCores;
  pDnode->diskAvailable    = pStatus->diskAvailable;
  pDnode->alternativeRole  = pStatus->alternativeRole;
  pDnode->numOfTotalVnodes = pStatus->numOfTotalVnodes; 
  
  if (pStatus->dnodeId == 0) {
    mTrace("dnode:%d, first access, privateIp:%s, name:%s", pDnode->dnodeId, taosIpStr(pDnode->privateIp), pDnode->dnodeName);
  }
 
  int32_t openVnodes = htons(pStatus->openVnodes);
  for (int32_t j = 0; j < openVnodes; ++j) {
    pDnode->vload[j].vgId          = htonl(pStatus->load[j].vgId);
    pDnode->vload[j].totalStorage  = htobe64(pStatus->load[j].totalStorage);
    pDnode->vload[j].compStorage   = htobe64(pStatus->load[j].compStorage);
    pDnode->vload[j].pointsWritten = htobe64(pStatus->load[j].pointsWritten);
    
    SVgObj *pVgroup = mgmtGetVgroup(pDnode->vload[j].vgId);
    if (pVgroup == NULL) {
      SRpcIpSet ipSet = mgmtGetIpSetFromIp(pDnode->privateIp);
      mPrint("dnode:%d, vgroup:%d not exist in mnode, drop it", pDnode->dnodeId, pDnode->vload[j].vgId);
      mgmtSendDropVnodeMsg(pDnode->vload[j].vgId, &ipSet, NULL);
    }
    mgmtReleaseVgroup(pVgroup);
  }

  if (pDnode->status == TAOS_DN_STATUS_OFFLINE) {
    mTrace("dnode:%d, from offline to online", pDnode->dnodeId);
    pDnode->status = TAOS_DN_STATUS_READY;
    balanceNotify();
  }

  clusterReleaseDnode(pDnode);

  int32_t contLen = sizeof(SDMStatusRsp) + TSDB_MAX_VNODES * sizeof(SVnodeAccess);
  SDMStatusRsp *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  mgmtGetMnodePrivateIpList(&pRsp->ipList);

  pRsp->dnodeState.dnodeId = htonl(pDnode->dnodeId);
  pRsp->dnodeState.moduleStatus = htonl(pDnode->moduleStatus);
  pRsp->dnodeState.createdTime  = htonl(pDnode->createdTime / 1000);
  pRsp->dnodeState.numOfVnodes = 0;
  
  contLen = sizeof(SDMStatusRsp);

  //TODO: set vnode access
  
  SRpcMsg rpcRsp = {
    .handle  = rpcMsg->handle,
    .code    = TSDB_CODE_SUCCESS,
    .pCont   = pRsp,
    .contLen = contLen
  };

  rpcSendResponse(&rpcRsp);
}

static int32_t clusterGetDnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mgmtGetUserFromConn(pConn, NULL);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->pAcct->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "private ip");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "public ip");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "open vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "total vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

#ifdef _VPEER
  pShow->bytes[cols] = 18;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "balance");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;
#endif  

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = clusterGetDnodesNum();
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  mgmtReleaseUser(pUser);

  return 0;
}

static int32_t clusterRetrieveDnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  int32_t    cols      = 0;
  SDnodeObj *pDnode   = NULL;
  char      *pWrite;
  char       ipstr[32];

  while (numOfRows < rows) {
    clusterReleaseDnode(pDnode);
    pShow->pNode = clusterGetNextDnode(pShow->pNode, (SDnodeObj **)&pDnode);
    if (pDnode == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->dnodeId;
    cols++;

    tinet_ntoa(ipstr, pDnode->privateIp);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, ipstr);
    cols++;

    tinet_ntoa(ipstr, pDnode->publicIp);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, ipstr);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pDnode->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, clusterGetDnodeStatusStr(pDnode->status));
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->openVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->numOfTotalVnodes;
    cols++;

#ifdef _VPEER
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, clusterGetDnodeStatusStr(pDnode->status));
    cols++;
#endif    

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static bool clusterCheckModuleInDnode(SDnodeObj *pDnode, int32_t moduleType) {
  uint32_t status = pDnode->moduleStatus & (1 << moduleType);
  return status > 0;
}

static int32_t clusterGetModuleMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SUserObj *pUser = mgmtGetUserFromConn(pConn, NULL);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "IP");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "module type");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "module status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = 0;
  SDnodeObj *pDnode = NULL;
  while (1) {
    pShow->pNode = clusterGetNextDnode(pShow->pNode, (SDnodeObj **)&pDnode);
    if (pDnode == NULL) break;
    for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
      if (clusterCheckModuleInDnode(pDnode, moduleType)) {
        pShow->numOfRows++;
      }
    }
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;
  mgmtReleaseUser(pUser);

  return 0;
}

int32_t clusterRetrieveModules(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  SDnodeObj *pDnode = NULL;
  char *     pWrite;
  int32_t    cols = 0;
  char       ipstr[20];

  while (numOfRows < rows) {
    clusterReleaseDnode(pDnode);
    pShow->pNode = clusterGetNextDnode(pShow->pNode, (SDnodeObj **)&pDnode);
    if (pDnode == NULL) break;

    for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
      if (!clusterCheckModuleInDnode(pDnode, moduleType)) {
        continue;
      }

      cols = 0;

      tinet_ntoa(ipstr, pDnode->privateIp);
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      strcpy(pWrite, ipstr);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      strcpy(pWrite, tsModule[moduleType].name);
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      strcpy(pWrite, clusterGetDnodeStatusStr(pDnode->status));
      cols++;

      numOfRows++;
    }
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static bool clusterCheckConfigShow(SGlobalConfig *cfg) {
  if (!(cfg->cfgType & TSDB_CFG_CTYPE_B_SHOW))
    return false;
  return true;
}

static int32_t clusterGetConfigMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SUserObj *pUser = mgmtGetUserFromConn(pConn, NULL);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_CFG_OPTION_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "config name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_CFG_VALUE_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "config value");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = 0;
  for (int32_t i = tsGlobalConfigNum - 1; i >= 0; --i) {
    SGlobalConfig *cfg = tsGlobalConfig + i;
    if (!clusterCheckConfigShow(cfg)) continue;
    pShow->numOfRows++;
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;
  mgmtReleaseUser(pUser);

  return 0;
}

static int32_t clusterRetrieveConfigs(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;

  for (int32_t i = tsGlobalConfigNum - 1; i >= 0 && numOfRows < rows; --i) {
    SGlobalConfig *cfg = tsGlobalConfig + i;
    if (!clusterCheckConfigShow(cfg)) continue;

    char *pWrite;
    int32_t   cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    snprintf(pWrite, TSDB_CFG_OPTION_LEN, "%s", cfg->option);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    switch (cfg->valType) {
      case TSDB_CFG_VTYPE_SHORT:
        snprintf(pWrite, TSDB_CFG_VALUE_LEN, "%d", *((int16_t *)cfg->ptr));
        numOfRows++;
        break;
      case TSDB_CFG_VTYPE_INT:
        snprintf(pWrite, TSDB_CFG_VALUE_LEN, "%d", *((int32_t *)cfg->ptr));
        numOfRows++;
        break;
      case TSDB_CFG_VTYPE_UINT:
        snprintf(pWrite, TSDB_CFG_VALUE_LEN, "%d", *((uint32_t *)cfg->ptr));
        numOfRows++;
        break;
      case TSDB_CFG_VTYPE_FLOAT:
        snprintf(pWrite, TSDB_CFG_VALUE_LEN, "%f", *((float *)cfg->ptr));
        numOfRows++;
        break;
      case TSDB_CFG_VTYPE_STRING:
      case TSDB_CFG_VTYPE_IPSTR:
      case TSDB_CFG_VTYPE_DIRECTORY:
        snprintf(pWrite, TSDB_CFG_VALUE_LEN, "%s", (char *)cfg->ptr);
        numOfRows++;
        break;
      default:
        break;
    }
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static int32_t clusterGetVnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;
  SUserObj *pUser = mgmtGetUserFromConn(pConn, NULL);
  if (pUser == NULL) return 0;
  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vnode");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 12;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  SDnodeObj *pDnode = NULL;
  if (pShow->payloadLen > 0 ) {
    uint32_t ip = ip2uint(pShow->payload);
    pDnode = clusterGetDnodeByIp(ip);
    if (NULL == pDnode) {
      return TSDB_CODE_NODE_OFFLINE;
    }

    SVnodeLoad* pVnode;
    pShow->numOfRows = 0;
    for (int32_t i = 0 ; i < TSDB_MAX_VNODES; i++) {
      pVnode = &pDnode->vload[i];
      if (0 != pVnode->vgId) {
        pShow->numOfRows++;
      }
    }
    
    pShow->pNode = pDnode;
  } else {
    while (true) {
      pShow->pNode = clusterGetNextDnode(pShow->pNode, (SDnodeObj **)&pDnode);
      if (pDnode == NULL) break;
      pShow->numOfRows += pDnode->openVnodes;

      if (0 == pShow->numOfRows) return TSDB_CODE_NODE_OFFLINE;      
    }

    pShow->pNode = NULL;
  } 

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  clusterReleaseDnode(pDnode);
  mgmtReleaseUser(pUser);

  return 0;
}

static int32_t clusterRetrieveVnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  SDnodeObj *pDnode = NULL;
  char *     pWrite;
  int32_t    cols = 0;

  if (0 == rows) return 0;

  if (pShow->payloadLen) {
    // output the vnodes info of the designated dnode. And output all vnodes of this dnode, instead of rows (max 100)
    pDnode = (SDnodeObj *)(pShow->pNode);
    if (pDnode != NULL) {
      SVnodeLoad* pVnode;
      for (int32_t i = 0 ; i < TSDB_MAX_VNODES; i++) {
        pVnode = &pDnode->vload[i];
        if (0 == pVnode->vgId) {
          continue;
        }
        
        cols = 0;
        
        pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
        *(uint32_t *)pWrite = pVnode->vgId;
        cols++;
        
        pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
        strcpy(pWrite, pVnode->status ? "ready" : "offline");
        cols++;
        
        numOfRows++;
      }
    }
  } else {
    // TODO: output all vnodes of all dnodes
    numOfRows = 0;
  }
  
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

char* clusterGetDnodeStatusStr(int32_t dnodeStatus) {
  switch (dnodeStatus) {
    case TAOS_DN_STATUS_OFFLINE:   return "offline";
    case TAOS_DN_STATUS_DROPPING:  return "dropping";
    case TAOS_DN_STATUS_BALANCING: return "balancing";
    case TAOS_DN_STATUS_READY:     return "ready";
    default:                       return "undefined";
  }
}
