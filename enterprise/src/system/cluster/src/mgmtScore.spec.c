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
#include <arpa/inet.h>
#include <netinet/in.h>
#include "mgmtBalance.h"
#include "tschemautil.h"
#include "ttime.h"

int         mgmtOrderedDnodesSize = 0;
int         mgmtOrderedDnodesMallocSize = 0;
SDnodeObj **mgmtOrderedDnodes = NULL;

int mgmtCalcCpuScore(SDnodeObj *pDnode) {
  if (pDnode->cpuAvgUsage < 80)
    return 0;
  else if (pDnode->cpuAvgUsage < 90)
    return 10;
  else
    return 50;
}

int mgmtCalcMemoryScore(SDnodeObj *pDnode) {
  if (pDnode->memoryAvgUsage < 80)
    return 0;
  else if (pDnode->memoryAvgUsage < 90)
    return 10;
  else
    return 50;
}

int mgmtCalcDiskScore(SDnodeObj *pDnode) {
  if (pDnode->diskAvgUsage < 80)
    return 0;
  else if (pDnode->diskAvgUsage < 90)
    return 10;
  else
    return 50;
}

int mgmtCalcBandwidthScore(SDnodeObj *pDnode) {
  if (pDnode->bandwidthUsage < 30)
    return 0;
  else if (pDnode->bandwidthUsage < 80)
    return 10;
  else
    return 50;
}

float mgmtCalcModuleScore(SDnodeObj *pDnode) {
  if (pDnode->numOfVnodes <= 1) return 0;
  if (mgmtCheckModuleInDnode(pDnode, TSDB_MOD_MGMT)) {
    return (float)tsModule[TSDB_MOD_MGMT].equalVnodeNum / pDnode->numOfVnodes * 100;
  }
  return 0;
  // float equalVnodes = 0;
  // for (int moduleType = 0; moduleType < TSDB_MOD_MAX; ++moduleType) {
  //  if (mgmtCheckModuleInDnode(pDnode, moduleType)) {
  //    equalVnodes += tsModule[moduleType].equalVnodeNum;
  //  }
  //}
}

float mgmtCalcVnodeScore(SDnodeObj *pDnode, int extra) {
  if (pDnode->numOfVnodes <= 1) return 0;
  return (float)(pDnode->numOfVnodes - pDnode->numOfFreeVnodes + extra) / pDnode->numOfVnodes * 100;
}

/**
 * calc singe score, such as cpu/memory/disk/bandwitdh/vnode
 * 1. get the score config
 * 2. if the value is out of range, use border data
 * 3. otherwise use interpolation method
 **/
void mgmtCalcDnodeScore(SDnodeObj *pDnode) {
  pDnode->lbScore = mgmtCalcCpuScore(pDnode) + mgmtCalcMemoryScore(pDnode) + mgmtCalcDiskScore(pDnode) +
                    mgmtCalcBandwidthScore(pDnode) + mgmtCalcModuleScore(pDnode) + mgmtCalcVnodeScore(pDnode, 0) +
                    pDnode->customScore;
}

float mgmtTryCalcDnodeScore(SDnodeObj *pDnode, int extra) {
  return mgmtCalcCpuScore(pDnode) + mgmtCalcMemoryScore(pDnode) + mgmtCalcDiskScore(pDnode) +
         mgmtCalcBandwidthScore(pDnode) + mgmtCalcModuleScore(pDnode) + mgmtCalcVnodeScore(pDnode, extra) +
         pDnode->customScore;
}

void mgmtCreateDnodeOrderList() {
  if (mgmtOrderedDnodes != NULL) {
    free(mgmtOrderedDnodes);
    mgmtOrderedDnodes = NULL;
  }

  if (mgmtOrderedDnodesMallocSize <= 0) mgmtOrderedDnodesMallocSize = 4;
  mgmtOrderedDnodes = (SDnodeObj **)malloc(mgmtOrderedDnodesMallocSize * sizeof(SDnodeObj *));
  memset(mgmtOrderedDnodes, 0, mgmtOrderedDnodesMallocSize * sizeof(SDnodeObj *));
}

void mgmtCalcSystemScore() {
  if (!tsEnableMonitorModule) return;
  if (!tsBalancePolicy) return;

  static uint32_t lastTime = 0;
  if (lastTime == 0) {
    lastTime = taosGetTimestampSec();
    return;
  }

  uint32_t ts = taosGetTimestampSec();
  if (ts - lastTime > 86400) {
    lastTime = ts;
    // fetch system paramete from sys.cpu and so on
  }
}

void mgmtReleaseDnodeOrderList() {
  if (mgmtOrderedDnodes != NULL) {
    free(mgmtOrderedDnodes);
    mgmtOrderedDnodes = NULL;
  }
}

void mgmtAllocDnodeOrderList() {
  mgmtOrderedDnodesSize = sdbGetNumOfRows(dnodeSdb);

  if (mgmtOrderedDnodesMallocSize <= mgmtOrderedDnodesSize) {
    mgmtOrderedDnodesMallocSize = mgmtOrderedDnodesSize * 2;
    if (mgmtOrderedDnodesMallocSize <= 0) mgmtOrderedDnodesMallocSize = 4;
    mgmtReleaseDnodeOrderList();
    mgmtOrderedDnodes = (SDnodeObj **)malloc(mgmtOrderedDnodesMallocSize * sizeof(SDnodeObj *));
    memset(mgmtOrderedDnodes, 0, mgmtOrderedDnodesMallocSize * sizeof(SDnodeObj *));
  }
}

/**
 * create a dnode list based on the balance score in asscending order
 * the balance score is calculate here
 * for every operation may change the score
 **/
void mgmtMakeDnodeOrderList() {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;

  mgmtAllocDnodeOrderList();
  // fill and order
  int dnodeIndex = 0;
  while (dnodeIndex < mgmtOrderedDnodesSize) {
    pNode = sdbFetchRow(dnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;
    mgmtCalcDnodeScore(pDnode);

    int orderIndex;
    for (orderIndex = dnodeIndex; orderIndex > 0; --orderIndex) {
      if (pDnode->lbScore > mgmtOrderedDnodes[orderIndex - 1]->lbScore) {
        break;
      }
      mgmtOrderedDnodes[orderIndex] = mgmtOrderedDnodes[orderIndex - 1];
    }
    mgmtOrderedDnodes[orderIndex] = pDnode;
    dnodeIndex++;
  }
}

int mgmtGetScoresMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int cols = 0;

  if (strcmp(pConn->pAcct->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  SSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = 16;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "IP");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "system scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "custom scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "module scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vnode scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "total scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "open vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "total vnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 18;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "balance state");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = sdbGetNumOfRows(dnodeSdb);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  return 0;
}

int mgmtRetrieveScores(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int        numOfRows = 0;
  SDnodeObj *pDnode = NULL;
  char *     pWrite;
  int        cols = 0;
  char       ipstr[20];

  while (numOfRows < rows) {
    pShow->pNode = sdbFetchRow(dnodeSdb, pShow->pNode, (void **)&pDnode);
    if (pDnode == NULL) break;

    int systemScore = mgmtCalcCpuScore(pDnode) + mgmtCalcMemoryScore(pDnode) + mgmtCalcDiskScore(pDnode) +
                      mgmtCalcBandwidthScore(pDnode);
    float moduleScore = mgmtCalcModuleScore(pDnode);
    float vnodeScore = mgmtCalcVnodeScore(pDnode, 0);

    cols = 0;

    tinet_ntoa(ipstr, pDnode->privateIp);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, ipstr);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = systemScore;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDnode->customScore;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = (int32_t)moduleScore;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = (int32_t)vnodeScore;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = (int32_t)(vnodeScore + moduleScore + pDnode->customScore + systemScore);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDnode->openVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDnode->numOfVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, taosGetDnodeBalanceStateStr(pDnode->lbState));
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}
