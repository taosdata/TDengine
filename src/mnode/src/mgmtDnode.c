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
#include "tschemautil.h"
#include "tstatus.h"
#include "mnode.h"
#include "mgmtDnode.h"
#include "mgmtBalance.h"
#include "mgmtUser.h"

SDnodeObj tsDnodeObj;

void mgmtSetDnodeMaxVnodes(SDnodeObj *pDnode) {
  int32_t maxVnodes = pDnode->numOfCores * tsNumOfVnodesPerCore;
  maxVnodes = maxVnodes > TSDB_MAX_VNODES ? TSDB_MAX_VNODES : maxVnodes;
  maxVnodes = maxVnodes < TSDB_MIN_VNODES ? TSDB_MIN_VNODES : maxVnodes;
  if (pDnode->numOfTotalVnodes != 0) {
    maxVnodes = pDnode->numOfTotalVnodes;
  }
  if (pDnode->alternativeRole == TSDB_DNODE_ROLE_MGMT) {
    maxVnodes = 0;
  }

  pDnode->numOfVnodes = maxVnodes;
  pDnode->numOfFreeVnodes = maxVnodes;
  pDnode->openVnodes = 0;
  pDnode->status = TSDB_DN_STATUS_OFFLINE;
}

void mgmtCalcNumOfFreeVnodes(SDnodeObj *pDnode) {
  int32_t totalVnodes = 0;

  mTrace("dnode:%s, begin calc free vnodes", taosIpStr(pDnode->privateIp));
  for (int32_t i = 0; i < pDnode->numOfVnodes; ++i) {
    SVnodeLoad *pVload = pDnode->vload + i;
    if (pVload->vgId != 0) {
      mTrace("%d-dnode:%s, calc free vnodes, exist vnode:%d, vgroup:%d, state:%d %s, dropstate:%d %s, syncstatus:%d %s",
             totalVnodes, taosIpStr(pDnode->privateIp), i, pVload->vgId,
             pVload->status, taosGetVnodeStatusStr(pVload->status),
             pVload->dropStatus, taosGetVnodeDropStatusStr(pVload->dropStatus),
             pVload->syncStatus, taosGetVnodeSyncStatusStr(pVload->syncStatus));
      totalVnodes++;
    }
  }

  pDnode->numOfFreeVnodes = pDnode->numOfVnodes - totalVnodes;
  mTrace("dnode:%s, numOfVnodes:%d, numOfFreeVnodes:%d, totalVnodes:%d",
          taosIpStr(pDnode->privateIp), pDnode->numOfVnodes, pDnode->numOfFreeVnodes, totalVnodes);
}

void mgmtSetDnodeVgid(SVnodeGid vnodeGid[], int32_t numOfVnodes, int32_t vgId) {
  SDnodeObj *pDnode;

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    pDnode = mgmtGetDnode(vnodeGid[i].ip);
    if (pDnode) {
      SVnodeLoad *pVload = pDnode->vload + vnodeGid[i].vnode;
      memset(pVload, 0, sizeof(SVnodeLoad));
      pVload->vnode = vnodeGid[i].vnode;
      pVload->vgId = vgId;
      mTrace("dnode:%s, vnode:%d add to vgroup:%d", taosIpStr(vnodeGid[i].ip), vnodeGid[i].vnode, pVload->vgId);
      mgmtCalcNumOfFreeVnodes(pDnode);
    } else {
      mError("dnode:%s, not in dnode DB!!!", taosIpStr(vnodeGid[i].ip));
    }
  }
}

void mgmtUnSetDnodeVgid(SVnodeGid vnodeGid[], int32_t numOfVnodes) {
  SDnodeObj *pDnode;

  for (int32_t i = 0; i < numOfVnodes; ++i) {
    pDnode = mgmtGetDnode(vnodeGid[i].ip);
    if (pDnode) {
      SVnodeLoad *pVload = pDnode->vload + vnodeGid[i].vnode;
      mTrace("dnode:%s, vnode:%d remove from vgroup:%d", taosIpStr(vnodeGid[i].ip), vnodeGid[i].vnode, pVload->vgId);
      memset(pVload, 0, sizeof(SVnodeLoad));
      mgmtCalcNumOfFreeVnodes(pDnode);
    } else {
      mError("dnode:%s not in dnode DB!!!", taosIpStr(vnodeGid[i].ip));
    }
  }
}

int32_t mgmtGetDnodeMeta(SMeterMeta *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SUserObj *pUser = mgmtGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  SCMSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "IP");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "created time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "open vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "free vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 18;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "balance state");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "public ip");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = mgmtGetDnodesNum();
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  return 0;
}

int32_t mgmtRetrieveDnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  SDnodeObj *pDnode   = NULL;
  char      *pWrite;
  int32_t   cols      = 0;
  char      ipstr[20];

  while (numOfRows < rows) {
    pShow->pNode = mgmtGetNextDnode(pShow, (SDnodeObj **)&pDnode);
    if (pDnode == NULL) break;

    cols = 0;

    tinet_ntoa(ipstr, pDnode->privateIp);
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
    *(int16_t *)pWrite = pDnode->numOfFreeVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, taosGetDnodeStatusStr(pDnode->status) );
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, taosGetDnodeLbStatusStr(pDnode->lbStatus));
    cols++;

    tinet_ntoa(ipstr, pDnode->publicIp);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, ipstr);
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

int32_t mgmtGetModuleMeta(SMeterMeta *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SUserObj *pUser = mgmtGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  SCMSchema *pSchema = tsGetSchema(pMeta);

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
    pShow->pNode = mgmtGetNextDnode(pShow, (SDnodeObj **)&pDnode);
    if (pDnode == NULL) break;
    for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
      if (mgmtCheckModuleInDnode(pDnode, moduleType)) {
        pShow->numOfRows++;
      }
    }
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  return 0;
}

int32_t mgmtRetrieveModules(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  SDnodeObj *pDnode = NULL;
  char *     pWrite;
  int32_t    cols = 0;
  char       ipstr[20];

  while (numOfRows < rows) {
    pShow->pNode = mgmtGetNextDnode(pShow, (SDnodeObj **)&pDnode);
    if (pDnode == NULL) break;

    for (int32_t moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
      if (!mgmtCheckModuleInDnode(pDnode, moduleType)) {
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

int32_t mgmtGetConfigMeta(SMeterMeta *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SUserObj *pUser = mgmtGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  SCMSchema *pSchema = tsGetSchema(pMeta);

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
    if (!mgmtCheckConfigShow(cfg)) continue;
    pShow->numOfRows++;
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  return 0;
}

int32_t mgmtRetrieveConfigs(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;

  for (int32_t i = tsGlobalConfigNum - 1; i >= 0 && numOfRows < rows; --i) {
    SGlobalConfig *cfg = tsGlobalConfig + i;
    if (!mgmtCheckConfigShow(cfg)) continue;

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

int32_t mgmtGetVnodeMeta(SMeterMeta *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;
  SUserObj *pUser = mgmtGetUserFromConn(pConn);
  if (pUser == NULL) return 0;
  if (strcmp(pUser->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  SCMSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vnode");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vgid");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 12;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 12;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "sync status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  // TODO: if other thread drop dnode ????
  SDnodeObj *pDnode = NULL;
  if (pShow->payloadLen > 0 ) {
    uint32_t ip = ip2uint(pShow->payload);
    pDnode = mgmtGetDnode(ip);
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
      pShow->pNode = mgmtGetNextDnode(pShow, (SDnodeObj **)&pDnode);
      if (pDnode == NULL) break;
      pShow->numOfRows += pDnode->openVnodes;

      if (0 == pShow->numOfRows) return TSDB_CODE_NODE_OFFLINE;      
    }

    pShow->pNode = NULL;
  } 

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

int32_t mgmtRetrieveVnodes(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t        numOfRows = 0;
  SDnodeObj *pDnode = NULL;
  char *     pWrite;
  int32_t        cols = 0;

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
        *(uint32_t *)pWrite = pVnode->vnode;
        cols++;
        
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

SDnodeObj *mgmtGetDnodeImp(uint32_t ip) {
  return &tsDnodeObj;
}

SDnodeObj *(*mgmtGetDnode)(uint32_t ip) = mgmtGetDnodeImp;

int32_t mgmtUpdateDnodeImp(SDnodeObj *pDnode) {
  return 0;
}

int32_t (*mgmtUpdateDnode)(SDnodeObj *pDnode) = mgmtUpdateDnodeImp;

void mgmtCleanUpDnodesImp() {
}

void (*mgmtCleanUpDnodes)() = mgmtCleanUpDnodesImp;

int32_t mgmtInitDnodesImp() {
  tsDnodeObj.privateIp        = inet_addr(tsPrivateIp);;
  tsDnodeObj.createdTime      = taosGetTimestampMs();
  tsDnodeObj.lastReboot       = taosGetTimestampSec();
  tsDnodeObj.numOfCores       = (uint16_t) tsNumOfCores;
  tsDnodeObj.status           = TSDB_DN_STATUS_READY;
  tsDnodeObj.alternativeRole  = TSDB_DNODE_ROLE_ANY;
  tsDnodeObj.numOfTotalVnodes = tsNumOfTotalVnodes;
  tsDnodeObj.thandle          = (void *) (1);  //hack way
  if (tsDnodeObj.numOfVnodes == TSDB_INVALID_VNODE_NUM) {
    mgmtSetDnodeMaxVnodes(&tsDnodeObj);
    mPrint("dnode first access, set total vnodes:%d", tsDnodeObj.numOfVnodes);
  }

  tsDnodeObj.status = TSDB_DN_STATUS_READY;
  return 0;
}

int32_t (*mgmtInitDnodes)() = mgmtInitDnodesImp;

int32_t mgmtGetDnodesNumImp() {
  return 1;
}

int32_t (*mgmtGetDnodesNum)() = mgmtGetDnodesNumImp;

void *mgmtGetNextDnodeImp(SShowObj *pShow, SDnodeObj **pDnode) {
  if (*pDnode == NULL) {
    *pDnode = &tsDnodeObj;
  } else {
    *pDnode = NULL;
  }

  return *pDnode;
}

void *(*mgmtGetNextDnode)(SShowObj *pShow, SDnodeObj **pDnode) = mgmtGetNextDnodeImp;

int32_t mgmtGetScoresMetaImp(SMeterMeta *pMeta, SShowObj *pShow, void *pConn) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

int32_t (*mgmtGetScoresMeta)(SMeterMeta *pMeta, SShowObj *pShow, void *pConn) = mgmtGetScoresMetaImp;

int32_t mgmtRetrieveScoresImp(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  return 0;
}

int32_t (*mgmtRetrieveScores)(SShowObj *pShow, char *data, int32_t rows, void *pConn) = mgmtRetrieveScoresImp;

void mgmtSetDnodeUnRemoveImp(SDnodeObj *pDnode) {
}

void (*mgmtSetDnodeUnRemove)(SDnodeObj *pDnode) = mgmtSetDnodeUnRemoveImp;

bool mgmtCheckConfigShowImp(SGlobalConfig *cfg) {
  if (cfg->cfgType & TSDB_CFG_CTYPE_B_CLUSTER)
    return false;
  if (cfg->cfgType & TSDB_CFG_CTYPE_B_NOT_PRINT)
    return false;
  return true;
}

bool (*mgmtCheckConfigShow)(SGlobalConfig *cfg) = mgmtCheckConfigShowImp;