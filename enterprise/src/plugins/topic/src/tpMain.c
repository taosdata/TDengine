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
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeDb.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeUser.h"
#include "mnodeRead.h"
#include "mnodeWrite.h"

extern void *  tsDbSdb;
extern char *  mnodeGetDbStr(char *src);
static int32_t mnodeGetTpMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveTps(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mnodeProcessCreateTpMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessAlterTpMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessDropTpMsg(SMnodeMsg *pMsg);
static void    mnodeCancelGetNextTp(void *pIter);

int32_t tpInit() {
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_CREATE_TP, mnodeProcessCreateTpMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_ALTER_TP, mnodeProcessAlterTpMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_DROP_TP, mnodeProcessDropTpMsg);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_TP, mnodeGetTpMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_TP, mnodeRetrieveTps);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_TP, mnodeCancelGetNextTp);

  return 0;
}

void tpCleanUp() {}



static int32_t mnodeProcessCreateTpMsg(SMnodeMsg *pMsg) {  
  
  return 0;
}

static int32_t mnodeProcessAlterTpMsg(SMnodeMsg *pMsg) {
  return 0;
}

static int32_t mnodeProcessDropTpMsg(SMnodeMsg *pMsg) {
  return 0;
}

static int32_t mnodeGetTpMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SSchema * pSchema = pMeta->schema;
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  pShow->bytes[cols] = (TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "created_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "partitions");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "replica");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "quorum");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "days");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 24 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "keep0,keep1,keep(D)");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "cache(MB)");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "blocks");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "minrows");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "maxrows");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "wallevel");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "fsync");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "comp");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->numOfRows = pUser->pAcct->acctInfo.numOfDbs;

  mnodeDecUserRef(pUser);
  return 0;
}

static int32_t mnodeRetrieveTps(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  SDbObj *  pDb = NULL;
  char *    pWrite;
  int32_t   cols = 0;
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextDb(pShow->pIter, &pDb);

    if (pDb == NULL) break;
    if (pDb->pAcct != pUser->pAcct || pDb->status != TSDB_DB_STATUS_READY || pDb->cfg.dbType != TSDB_DB_TYPE_TOPIC) {
      mnodeDecDbRef(pDb);
      continue;
    }

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    char *name = mnodeGetDbStr(pDb->name);
    if (name != NULL) {
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, name, pShow->bytes[cols]);
    } else {
      STR_TO_VARSTR(pWrite, "NULL");
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pDb->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->cfg.partitions;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDb->cfg.replications;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDb->cfg.quorum;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pDb->cfg.daysPerFile;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;

    char tmp[128] = {0};
    sprintf(tmp, "%d,%d,%d", pDb->cfg.daysToKeep1, pDb->cfg.daysToKeep2, pDb->cfg.daysToKeep);
    STR_WITH_SIZE_TO_VARSTR(pWrite, tmp, strlen(tmp));
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->cfg.cacheBlockSize;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->cfg.totalBlocks;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->cfg.minRowsPerFileBlock;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->cfg.maxRowsPerFileBlock;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int8_t *)pWrite = pDb->cfg.walLevel;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->cfg.fsyncPeriod;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int8_t *)pWrite = pDb->cfg.compression;
    cols++;

    numOfRows++;
    mnodeDecDbRef(pDb);
  }

  pShow->numOfReads += numOfRows;
  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);

  mnodeDecUserRef(pUser);
  return numOfRows;
}

void mnodeCancelGetNextTp(void *pIter) {
  sdbFreeIter(tsDbSdb, pIter);
}