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
#include "tschemautil.h"
#include "tmodule.h"
#include "tstatus.h"
#include "ttime.h"
#include "mgmtDnode.h"
#include "balanceScore.h"

extern void *tsVgroupSdb;
extern void *tsDnodeSdb;
extern int32_t tsVgUpdateSize;
extern int32_t tsMnodeUpdateSize;
extern int32_t tsDnodeUpdateSize;


int32_t     tsBalanceDnodeListSize = 0;
SDnodeObj **tsBalanceDnodeList     = NULL;

static int32_t tsBalanceDnodesListMallocSize = 0;

static int32_t balanceCalcCpuScore(SDnodeObj *pDnode) {
  if (pDnode->cpuAvgUsage < 80)
    return 0;
  else if (pDnode->cpuAvgUsage < 90)
    return 10;
  else
    return 50;
}

static int32_t balanceCalcMemoryScore(SDnodeObj *pDnode) {
  if (pDnode->memoryAvgUsage < 80)
    return 0;
  else if (pDnode->memoryAvgUsage < 90)
    return 10;
  else
    return 50;
}

static int32_t balanceCalcDiskScore(SDnodeObj *pDnode) {
  if (pDnode->diskAvgUsage < 80)
    return 0;
  else if (pDnode->diskAvgUsage < 90)
    return 10;
  else
    return 50;
}

static int32_t balanceCalcBandwidthScore(SDnodeObj *pDnode) {
  if (pDnode->bandwidthUsage < 30)
    return 0;
  else if (pDnode->bandwidthUsage < 80)
    return 10;
  else
    return 50;
}

static float balanceCalcModuleScore(SDnodeObj *pDnode) {
  if (pDnode->numOfVnodes <= 1) return 0;
  if (mgmtCheckModuleInDnode(pDnode, TSDB_MOD_MGMT)) {
    return (float)tsModule[TSDB_MOD_MGMT].equalVnodeNum / pDnode->numOfVnodes * 100;
  }
  return 0;
}

static float balanceCalcVnodeScore(SDnodeObj *pDnode, int32_t extra) {
  if (pDnode->numOfVnodes <= 1) return 0;
  return (float)(pDnode->numOfVnodes - pDnode->numOfFreeVnodes + extra) / pDnode->numOfVnodes * 100;
}

/**
 * calc singe score, such as cpu/memory/disk/bandwitdh/vnode
 * 1. get the score config
 * 2. if the value is out of range, use border data
 * 3. otherwise use interpolation method
 **/
void balanceCalcDnodeScore(SDnodeObj *pDnode) {
  pDnode->lbScore = balanceCalcCpuScore(pDnode) + balanceCalcMemoryScore(pDnode) + balanceCalcDiskScore(pDnode) +
                    balanceCalcBandwidthScore(pDnode) + balanceCalcModuleScore(pDnode) + balanceCalcVnodeScore(pDnode, 0) +
                    pDnode->customScore;
}

float balanceTryCalcDnodeScore(SDnodeObj *pDnode, int32_t extra) {
  return balanceCalcCpuScore(pDnode) + balanceCalcMemoryScore(pDnode) + balanceCalcDiskScore(pDnode) +
         balanceCalcBandwidthScore(pDnode) + balanceCalcModuleScore(pDnode) + balanceCalcVnodeScore(pDnode, extra) +
         pDnode->customScore;
}

void balanceInitDnodeList() {
  if (tsBalanceDnodeList != NULL) {
    free(tsBalanceDnodeList);
    tsBalanceDnodeList = NULL;
  }

  if (tsBalanceDnodesListMallocSize <= 0) tsBalanceDnodesListMallocSize = 4;
  tsBalanceDnodeList = (SDnodeObj **)malloc(tsBalanceDnodesListMallocSize * sizeof(SDnodeObj *));
  memset(tsBalanceDnodeList, 0, tsBalanceDnodesListMallocSize * sizeof(SDnodeObj *));
}

void balanceCalcSystemScore() {
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

void balanceReleaseDnodeList() {
  if (tsBalanceDnodeList != NULL) {
    free(tsBalanceDnodeList);
    tsBalanceDnodeList = NULL;
  }
}

static void balanceAllocDnodeOrderList() {
  tsBalanceDnodeListSize = sdbGetNumOfRows(tsDnodeSdb);

  if (tsBalanceDnodesListMallocSize <= tsBalanceDnodeListSize) {
    tsBalanceDnodesListMallocSize = tsBalanceDnodeListSize * 2;
    if (tsBalanceDnodesListMallocSize <= 0) tsBalanceDnodesListMallocSize = 4;
    balanceReleaseDnodeList();
    tsBalanceDnodeList = (SDnodeObj **)malloc(tsBalanceDnodesListMallocSize * sizeof(SDnodeObj *));
    memset(tsBalanceDnodeList, 0, tsBalanceDnodesListMallocSize * sizeof(SDnodeObj *));
  }
}

/**
 * create a dnode list based on the balance score in asscending order
 * the balance score is calculate here
 * for every operation may change the score
 **/
void balanceMakeDnodeList() {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;

  balanceAllocDnodeOrderList();

  // fill and order
  int32_t dnodeIndex = 0;
  while (dnodeIndex < tsBalanceDnodeListSize) {
    pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;
    balanceCalcDnodeScore(pDnode);

    int32_t orderIndex;
    for (orderIndex = dnodeIndex; orderIndex > 0; --orderIndex) {
      if (pDnode->lbScore > tsBalanceDnodeList[orderIndex - 1]->lbScore) {
        break;
      }
      tsBalanceDnodeList[orderIndex] = tsBalanceDnodeList[orderIndex - 1];
    }
    tsBalanceDnodeList[orderIndex] = pDnode;
    dnodeIndex++;
  }
}

int32_t balanceGetScoresMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

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
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = sdbGetNumOfRows(tsDnodeSdb);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  return 0;
}

int32_t balanceRetrieveScores(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t        numOfRows = 0;
  SDnodeObj *pDnode = NULL;
  char *     pWrite;
  int32_t        cols = 0;
  char       ipstr[20];

  while (numOfRows < rows) {
    pShow->pNode = sdbFetchRow(tsDnodeSdb, pShow->pNode, (void **)&pDnode);
    if (pDnode == NULL) break;

    int32_t systemScore = balanceCalcCpuScore(pDnode) + balanceCalcMemoryScore(pDnode) + balanceCalcDiskScore(pDnode) +
                      balanceCalcBandwidthScore(pDnode);
    float moduleScore = balanceCalcModuleScore(pDnode);
    float vnodeScore = balanceCalcVnodeScore(pDnode, 0);

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
    strcpy(pWrite, taosGetDnodeLbStatusStr(pDnode->lbStatus));
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}
