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
#include "tglobal.h"
#include "mnodeShow.h"
#include "mnodeUser.h"
#include "bnScore.h"

SBnDnodes tsBnDnodes;

static int32_t bnGetScoresMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t bnRetrieveScores(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static int32_t bnCalcCpuScore(SDnodeObj *pDnode) {
  if (pDnode->cpuAvgUsage < 80)
    return 0;
  else if (pDnode->cpuAvgUsage < 90)
    return 10;
  else
    return 50;
}

static int32_t bnCalcMemoryScore(SDnodeObj *pDnode) {
  if (pDnode->memoryAvgUsage < 80)
    return 0;
  else if (pDnode->memoryAvgUsage < 90)
    return 10;
  else
    return 50;
}

static int32_t bnCalcDiskScore(SDnodeObj *pDnode) {
  if (pDnode->diskAvgUsage < 80)
    return 0;
  else if (pDnode->diskAvgUsage < 90)
    return 10;
  else
    return 50;
}

static int32_t bnCalcBandScore(SDnodeObj *pDnode) {
  if (pDnode->bandwidthUsage < 30)
    return 0;
  else if (pDnode->bandwidthUsage < 80)
    return 10;
  else
    return 50;
}

static float bnCalcModuleScore(SDnodeObj *pDnode) {
  if (pDnode->numOfCores <= 0) return 0;
  if (pDnode->isMgmt) {
    return (float)tsMnodeEqualVnodeNum / pDnode->numOfCores;
  }
  return 0;
}

static float bnCalcVnodeScore(SDnodeObj *pDnode, int32_t extra) {
  if (pDnode->status == TAOS_DN_STATUS_DROPPING || pDnode->status == TAOS_DN_STATUS_OFFLINE) return 100000000;
  if (pDnode->numOfCores <= 0) return 0;
  return (float)(pDnode->openVnodes + extra) / pDnode->numOfCores;
}

/**
 * calc singe score, such as cpu/memory/disk/bandwitdh/vnode
 * 1. get the score config
 * 2. if the value is out of range, use border data
 * 3. otherwise use interpolation method
 **/
static void bnCalcDnodeScore(SDnodeObj *pDnode) {
  pDnode->score = bnCalcCpuScore(pDnode) + bnCalcMemoryScore(pDnode) + bnCalcDiskScore(pDnode) +
                  bnCalcBandScore(pDnode) + bnCalcModuleScore(pDnode) + bnCalcVnodeScore(pDnode, 0) +
                  pDnode->customScore;
}

float bnTryCalcDnodeScore(SDnodeObj *pDnode, int32_t extra) {
  int32_t systemScore = bnCalcCpuScore(pDnode) + bnCalcMemoryScore(pDnode) + bnCalcDiskScore(pDnode) +
                        bnCalcBandScore(pDnode);
  float moduleScore = bnCalcModuleScore(pDnode);
  float vnodeScore = bnCalcVnodeScore(pDnode, extra);

  float score = systemScore + moduleScore + vnodeScore +  pDnode->customScore;
  return score;
}

void bnInitDnodes() {
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_SCORES, bnGetScoresMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_SCORES, bnRetrieveScores);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_SCORES, mnodeCancelGetNextDnode);

  memset(&tsBnDnodes, 0, sizeof(SBnDnodes));
  tsBnDnodes.maxSize = 16;
  tsBnDnodes.list = calloc(tsBnDnodes.maxSize, sizeof(SDnodeObj *));
}

void bnCleanupDnodes() {
  if (tsBnDnodes.list != NULL) {
    free(tsBnDnodes.list);
    tsBnDnodes.list = NULL;
  }
}

static void bnCheckDnodesSize(int32_t dnodesNum) {
  if (tsBnDnodes.maxSize <= dnodesNum) {
    tsBnDnodes.maxSize = dnodesNum * 2;
    tsBnDnodes.list = realloc(tsBnDnodes.list, tsBnDnodes.maxSize * sizeof(SDnodeObj *));
  }
}

void bnAccquireDnodes() {
  int32_t dnodesNum = mnodeGetDnodesNum();
  bnCheckDnodesSize(dnodesNum);

  void *     pIter = NULL;
  SDnodeObj *pDnode = NULL;
  int32_t    dnodeIndex = 0;

  while (1) {
    if (dnodeIndex >= dnodesNum) {
      mnodeCancelGetNextDnode(pIter);
      break;
    }

    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    if (pDnode->status == TAOS_DN_STATUS_OFFLINE) {
      mnodeDecDnodeRef(pDnode);
      continue;
    }

    bnCalcDnodeScore(pDnode);
    
    int32_t orderIndex = dnodeIndex;
    for (; orderIndex > 0; --orderIndex) {
      if (pDnode->score > tsBnDnodes.list[orderIndex - 1]->score) {
        break;
      }
      tsBnDnodes.list[orderIndex] = tsBnDnodes.list[orderIndex - 1];
    }
    tsBnDnodes.list[orderIndex] = pDnode;
    dnodeIndex++;
  }

  tsBnDnodes.size = dnodeIndex;
}

void bnReleaseDnodes() {
  for (int32_t i = 0; i < tsBnDnodes.size; ++i) {
    SDnodeObj *pDnode = tsBnDnodes.list[i];
    if (pDnode != NULL) {
      mnodeDecDnodeRef(pDnode);
    }
  }
}

static int32_t bnGetScoresMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->pAcct->user, "root") != 0) {
    mnodeDecUserRef(pUser);
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_FLOAT;
  strcpy(pSchema[cols].name, "system scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_FLOAT;
  strcpy(pSchema[cols].name, "custom scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_FLOAT;
  strcpy(pSchema[cols].name, "module scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_FLOAT;
  strcpy(pSchema[cols].name, "vnode scores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_FLOAT;
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
  strcpy(pSchema[cols].name, "cpu cores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 18 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "balance state");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = mnodeGetDnodesNum();
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pIter = NULL;

  mnodeDecUserRef(pUser);

  return 0;
}

static int32_t bnRetrieveScores(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t    numOfRows = 0;
  SDnodeObj *pDnode = NULL;
  char *     pWrite;
  int32_t    cols = 0;

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextDnode(pShow->pIter, &pDnode);
    if (pDnode == NULL) break;

    int32_t systemScore = bnCalcCpuScore(pDnode) + bnCalcMemoryScore(pDnode) + bnCalcDiskScore(pDnode) + bnCalcBandScore(pDnode);
    float moduleScore = bnCalcModuleScore(pDnode);
    float vnodeScore = bnCalcVnodeScore(pDnode, 0);

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDnode->dnodeId;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(float *)pWrite = systemScore;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(float *)pWrite = pDnode->customScore;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(float *)pWrite = (int32_t)moduleScore;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(float *)pWrite = (int32_t)vnodeScore;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(float *)pWrite = (int32_t)(vnodeScore + moduleScore + pDnode->customScore + systemScore);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDnode->openVnodes;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDnode->numOfCores;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, dnodeStatus[pDnode->status]);
    cols++;

    numOfRows++;
    mnodeDecDnodeRef(pDnode);
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}
