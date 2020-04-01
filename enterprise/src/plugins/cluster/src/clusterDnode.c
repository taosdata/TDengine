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
#include "tglobalcfg.h"
#include "tmodule.h"
#include "tstatus.h"
#include "taosdef.h"
#include "taosmsg.h"
#include "tlog.h"
#include "mnode.h"
#include "mgmtDnode.h"
#include "mgmtGrant.h"
#include "mgmtMnode.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"
#include "dnodeMClient.h"

void *tsDnodeSdb = NULL;
extern void *  tsVgroupSdb;
static int32_t tsDnodeUpdateSize = 0;
static int32_t clusterCreateDnode(uint32_t ip);
static int32_t clusterDropDnode(SDnodeObj *pDnode);
static void    clusterProcessCreateDnodeMsg(SQueuedMsg *pMsg);
static void    clusterProcessDropDnodeMsg(SQueuedMsg *pMsg);
static int32_t clusterGetModuleMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t clusterRetrieveModules(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t clusterGetConfigMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t clusterRetrieveConfigs(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t clusterGetVnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t clusterRetrieveVnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t clusterGetDnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t clusterRetrieveDnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static int32_t clusterDnodeActionDestroy(SSdbOperDesc *pOper) {
  tfree(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t clusterDnodeActionInsert(SSdbOperDesc *pOper) {
  return TSDB_CODE_SUCCESS;
}

static int32_t clusterDnodeActionDelete(SSdbOperDesc *pOper) {
  SDnodeObj *pDnode = pOper->pObj;
  void *     pNode = NULL;
  void *     pLastNode = NULL;
  SVgObj *   pVgroup = NULL;
  int32_t    numOfVgroups = 0;

  while (1) {
    pLastNode = pNode;
    pNode = sdbFetchRow(tsVgroupSdb, pNode, (void **)&pVgroup);
    if (pVgroup == NULL) break;

    if (pVgroup->vnodeGid[0].dnodeId == pDnode->dnodeId) {
      SSdbOperDesc oper = {
        .type = SDB_OPER_TYPE_LOCAL,
        .table = tsVgroupSdb,
        .pObj = pVgroup,
      };
      sdbDeleteRow(&oper);
      pNode = pLastNode;
      numOfVgroups++;
      continue;
    }
  }

  mTrace("dnode:%d, all vgroups:%d is dropped from sdb", pDnode->dnodeId, numOfVgroups);
  return TSDB_CODE_SUCCESS;
}

static int32_t clusterDnodeActionUpdate(SSdbOperDesc *pOper) {
  return TSDB_CODE_SUCCESS;
}

static int32_t clusterDnodeActionEncode(SSdbOperDesc *pOper) {
  SDnodeObj *pDnode = pOper->pObj;

  if (pOper->maxRowSize < tsDnodeUpdateSize) {
    return -1;
  } else {
    memcpy(pOper->rowData, pDnode, tsDnodeUpdateSize);
    pOper->rowSize = tsDnodeUpdateSize;
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t clusterDnodeActionDecode(SSdbOperDesc *pOper) {
  SDnodeObj *pDnode = (SDnodeObj *) calloc(1, sizeof(SDnodeObj));
  if (pDnode == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

  memcpy(pDnode, pOper->rowData, tsDnodeUpdateSize);
  pOper->pObj = pDnode;
  return TSDB_CODE_SUCCESS;
}

int32_t clusterInit() {
  SDnodeObj tObj;
  tsDnodeUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableName    = "dnodes",
    .hashSessions = TSDB_MAX_DNODES,
    .maxRowSize   = tsDnodeUpdateSize,
    .keyType      = SDB_KEY_TYPE_AUTO,
    .insertFp     = clusterDnodeActionInsert,
    .deleteFp     = clusterDnodeActionDelete,
    .updateFp     = clusterDnodeActionUpdate,
    .encodeFp     = clusterDnodeActionEncode,
    .decodeFp     = clusterDnodeActionDecode,
    .destroyFp    = clusterDnodeActionDestroy,
  };

  tsDnodeSdb = sdbOpenTable(&tableDesc);
  if (tsDnodeSdb == NULL) {
    mError("failed to init dnodes data");
    return -1;
  }

  int32_t numOfRows = sdbGetNumOfRows(tsDnodeSdb);
  if (numOfRows <= 0) {
    if (strcmp(tsMasterIp, tsPrivateIp) == 0) {
      clusterCreateDnode(inet_addr(tsPrivateIp));
    }
  }

  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_CREATE_DNODE, clusterProcessCreateDnodeMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_DROP_DNODE, clusterProcessDropDnodeMsg);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_MODULE, clusterGetModuleMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_MODULE, clusterRetrieveModules);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_CONFIGS, clusterGetConfigMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_CONFIGS, clusterRetrieveConfigs);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_VNODES, clusterGetVnodeMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_VNODES, clusterRetrieveVnodes);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_DNODE, clusterGetDnodeMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_DNODE, clusterRetrieveDnodes);
  
  mTrace("dnodes is initialized");
  return 0;
}

void clusterCleanUp() {
  sdbCloseTable(tsDnodeSdb);
}

int32_t clusterGetDnodesNum() {
  return sdbGetNumOfRows(tsDnodeSdb);
}

SDnodeObj *clusterGetDnode(int32_t dnodeId) {
  return (SDnodeObj *)sdbGetRow(tsDnodeSdb, &dnodeId);
}

SDnodeObj *clusterGetDnodeByIp(uint32_t ip) {
  SDnodeObj *pDnode = NULL;
  void *     pNode = NULL;

  while (1) {
    pNode = sdbFetchRow(tsDnodeSdb, pNode, (void**)&pDnode);
    if (pDnode == NULL) break;
    if (ip == pDnode->privateIp) {
      return pDnode;
    }
  }

  return NULL;
}

static int32_t clusterCreateDnode(uint32_t ip) {
  int32_t grantCode = grantCheck(TSDB_GRANT_DNODE);
  if (grantCode != TSDB_CODE_SUCCESS) {
    return grantCode;
  }

  SDnodeObj *pDnode = mgmtGetDnodeByIp(ip);
  if (pDnode != NULL) {
    mError("dnode:%d is alredy exist, ip:%s", pDnode->dnodeId, taosIpStr(pDnode->privateIp));
    return TSDB_CODE_DNODE_ALREADY_EXIST;
  }

  pDnode = (SDnodeObj *) calloc(1, sizeof(SDnodeObj));
  pDnode->privateIp = ip;
  pDnode->publicIp = ip;
  pDnode->createdTime = taosGetTimestampMs();
  pDnode->status = TSDB_DN_STATUS_OFFLINE; 
  pDnode->numOfTotalVnodes = TSDB_INVALID_VNODE_NUM; 

  if (pDnode->privateIp == inet_addr(tsMasterIp)) {
    pDnode->moduleStatus |= (1 << TSDB_MOD_MGMT);
  }
  
  SSdbOperDesc oper = {
    .type = SDB_OPER_TYPE_GLOBAL,
    .table = tsDnodeSdb,
    .pObj = pDnode,
    .rowSize = sizeof(SDnodeObj)
  };

  int32_t code = sdbInsertRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(pDnode);
    code = TSDB_CODE_SDB_ERROR;
  }

  mPrint("dnode:%d is created, result:%s", pDnode->dnodeId, tstrerror(code));
  return code;
}

static int32_t clusterDropDnode(SDnodeObj *pDnode) {
  SSdbOperDesc oper = {
    .type = SDB_OPER_TYPE_GLOBAL,
    .table = tsDnodeSdb,
    .pObj = pDnode
  };

  int32_t code = sdbDeleteRow(&oper); 
  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_SDB_ERROR;
  }

  mLPrint("dnode:%d is dropped from cluster, result:%s", pDnode->dnodeId, tstrerror(code));
  return code;
}

static int32_t clusterDropDnodeByIp(uint32_t ip) {
  SDnodeObj *pDnode = clusterGetDnodeByIp(ip);
  if (pDnode == NULL) {
    mError("dnode:%s, is not exist", taosIpStr(ip));
    return TSDB_CODE_INVALID_VALUE;
  }

  if (pDnode->privateIp == dnodeGetMnodeMasteIp()) {
    mError("dnode:%d, can't drop dnode which is master", pDnode->dnodeId);
    return TSDB_CODE_NO_REMOVE_MASTER;
  }

  return clusterDropDnode(pDnode);
}

static int32_t clusterGetDnodeMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mgmtGetUserFromConn(pConn, NULL);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->pAcct->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
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

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 18;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "balance");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = clusterGetDnodesNum();
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  return 0;
}

static int32_t clusterRetrieveDnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  int32_t   cols      = 0;
  SDnodeObj *pDnode   = NULL;
  char      *pWrite;
  char      ipstr[20];

  while (numOfRows < rows) {
    pShow->pNode = sdbFetchRow(tsDnodeSdb, pShow->pNode, (void**)&pDnode);
    if (pDnode == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int8_t *)pWrite = (int8_t)pDnode->dnodeId;
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
    *(int16_t *)pWrite = pDnode->openVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->numOfTotalVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, taosGetDnodeStatusStr(pDnode->status) );
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, taosGetDnodeLbStatusStr(pDnode->lbStatus));
    cols++;

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
    pShow->pNode = sdbFetchRow(tsDnodeSdb, pShow->pNode, (void**)&pDnode);
    if (pDnode == NULL) break;
    for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
      if (clusterCheckModuleInDnode(pDnode, moduleType)) {
        pShow->numOfRows++;
      }
    }
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  return 0;
}

int32_t clusterRetrieveModules(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  SDnodeObj *pDnode = NULL;
  char *     pWrite;
  int32_t    cols = 0;
  char       ipstr[20];

  while (numOfRows < rows) {
    pShow->pNode = sdbFetchRow(tsDnodeSdb, pShow->pNode, (void**)&pDnode);
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
      strcpy(pWrite, taosGetDnodeStatusStr(pDnode->status) );
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

  pShow->bytes[cols] = 12;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "sync_status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  SDnodeObj *pDnode = NULL;
  if (pShow->payloadLen > 0 ) {
    uint32_t ip = ip2uint(pShow->payload);
    pDnode = mgmtGetDnodeByIp(ip);
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
      pShow->pNode = sdbFetchRow(tsDnodeSdb, pShow->pNode, (void**)&pDnode);
      if (pDnode == NULL) break;
      pShow->numOfRows += pDnode->openVnodes;

      if (0 == pShow->numOfRows) return TSDB_CODE_NODE_OFFLINE;      
    }

    pShow->pNode = NULL;
  } 

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

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
        strcpy(pWrite, taosGetVnodeStatusStr(pVnode->status));
        cols++;
        
        pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
        strcpy(pWrite, taosGetVnodeSyncStatusStr(pVnode->syncStatus));
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

static void clusterProcessCreateDnodeMsg(SQueuedMsg *pMsg) {
  SRpcMsg rpcRsp = {.handle = pMsg->thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  if (mgmtCheckRedirect(pMsg->thandle)) return;

  SCMCreateDnodeMsg *pCreate = pMsg->pCont;

  if (strcmp(pMsg->pUser->pAcct->user, "root") != 0) {
    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
  } else {
    uint32_t ip = inet_addr(pCreate->ip);
    rpcRsp.code = clusterCreateDnode(ip);
    if (rpcRsp.code == TSDB_CODE_SUCCESS) {
      SDnodeObj *pDnode = mgmtGetDnodeByIp(ip);
      mLPrint("dnode:%d, ip:%s is created by %s", pDnode->dnodeId, pCreate->ip, pMsg->pUser->user);
    } else {
      mError("failed to create dnode:%s, reason:%s", pCreate->ip, tstrerror(rpcRsp.code));
    }
  }
  rpcSendResponse(&rpcRsp);
}

static void clusterProcessDropDnodeMsg(SQueuedMsg *pMsg) {
  SRpcMsg rpcRsp = {.handle = pMsg->thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  if (mgmtCheckRedirect(pMsg->thandle)) return;

  SCMDropDnodeMsg *pDrop = pMsg->pCont;
  if (strcmp(pMsg->pUser->pAcct->user, "root") != 0) {
    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
  } else {
    uint32_t ip = inet_addr(pDrop->ip);
    rpcRsp.code = clusterDropDnodeByIp(ip);
    if (rpcRsp.code == TSDB_CODE_SUCCESS) {
      mLPrint("dnode:%s is dropped by %s", pDrop->ip, pMsg->pUser->user);
    } else {
      mError("failed to drop dnode:%s, reason:%s", pDrop->ip, tstrerror(rpcRsp.code));
    }
  }

  rpcSendResponse(&rpcRsp);
}

