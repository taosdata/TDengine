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
#include "taoserror.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeCluster.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "tglobal.h"

int64_t        tsClusterRid = -1;
static void *  tsClusterSdb = NULL;
static int32_t tsClusterUpdateSize;
static char    tsClusterId[TSDB_CLUSTER_ID_LEN];
static int32_t mnodeCreateCluster();

static int32_t mnodeGetClusterMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveClusters(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static void    mnodeCancelGetNextCluster(void *pIter);

static int32_t mnodeClusterActionDestroy(SSdbRow *pRow) {
  tfree(pRow->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeClusterActionInsert(SSdbRow *pRow) {
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeClusterActionDelete(SSdbRow *pRow) {
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeClusterActionUpdate(SSdbRow *pRow) {
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeClusterActionEncode(SSdbRow *pRow) {
  SClusterObj *pCluster = pRow->pObj;
  memcpy(pRow->rowData, pCluster, tsClusterUpdateSize);
  pRow->rowSize = tsClusterUpdateSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeClusterActionDecode(SSdbRow *pRow) {
  SClusterObj *pCluster = (SClusterObj *) calloc(1, sizeof(SClusterObj));
  if (pCluster == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

  memcpy(pCluster, pRow->rowData, tsClusterUpdateSize);
  pRow->pObj = pCluster;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeClusterActionRestored() {
  int64_t numOfRows = sdbGetNumOfRows(tsClusterSdb);
  if (numOfRows <= 0 && dnodeIsFirstDeploy()) {
    mInfo("dnode first deploy, create cluster");
    int32_t code = mnodeCreateCluster();
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
      mError("failed to create cluster, reason:%s", tstrerror(code));
      return code;
    }
  }

  mnodeUpdateClusterId();
  return TSDB_CODE_SUCCESS;
}

int32_t mnodeInitCluster() {
  SClusterObj tObj;
  tsClusterUpdateSize = (int32_t)((int8_t *)tObj.updateEnd - (int8_t *)&tObj);

  SSdbTableDesc desc = {
    .id           = SDB_TABLE_CLUSTER,
    .name         = "cluster",
    .hashSessions = TSDB_DEFAULT_CLUSTER_HASH_SIZE,
    .maxRowSize   = tsClusterUpdateSize,
    .refCountPos  = (int32_t)((int8_t *)(&tObj.refCount) - (int8_t *)&tObj),
    .keyType      = SDB_KEY_STRING,
    .fpInsert     = mnodeClusterActionInsert,
    .fpDelete     = mnodeClusterActionDelete,
    .fpUpdate     = mnodeClusterActionUpdate,
    .fpEncode     = mnodeClusterActionEncode,
    .fpDecode     = mnodeClusterActionDecode,
    .fpDestroy    = mnodeClusterActionDestroy,
    .fpRestored   = mnodeClusterActionRestored
  };

  tsClusterRid = sdbOpenTable(&desc);
  tsClusterSdb = sdbGetTableByRid(tsClusterRid);
  if (tsClusterSdb == NULL) {
    mError("table:%s, rid:%" PRId64 ", failed to create hash", desc.name, tsClusterRid);
    return -1;
  }

  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_CLUSTER, mnodeGetClusterMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_CLUSTER, mnodeRetrieveClusters);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_CLUSTER, mnodeCancelGetNextCluster);

  mDebug("table:%s, hash is created", desc.name);
  return TSDB_CODE_SUCCESS;
}

void mnodeCleanupCluster() {
  sdbCloseTable(tsClusterRid);
  tsClusterSdb = NULL;
}

void *mnodeGetNextCluster(void *pIter, SClusterObj **pCluster) {
  return sdbFetchRow(tsClusterSdb, pIter, (void **)pCluster); 
}

void mnodeCancelGetNextCluster(void *pIter) {
  sdbFreeIter(tsClusterSdb, pIter);
}

void mnodeIncClusterRef(SClusterObj *pCluster) {
  sdbIncRef(tsClusterSdb, pCluster);
}

void mnodeDecClusterRef(SClusterObj *pCluster) {
  sdbDecRef(tsClusterSdb, pCluster);
}

static int32_t mnodeCreateCluster() {
  int64_t numOfClusters = sdbGetNumOfRows(tsClusterSdb);
  if (numOfClusters != 0) return TSDB_CODE_SUCCESS;

  SClusterObj *pCluster = malloc(sizeof(SClusterObj));
  memset(pCluster, 0, sizeof(SClusterObj));
  pCluster->createdTime = taosGetTimestampMs();
  bool getuid = taosGetSystemUid(pCluster->uid);
  if (!getuid) {
    strcpy(pCluster->uid, "tdengine2.0");
    mError("failed to get uid from system, set to default val %s", pCluster->uid);
  } else {
    mDebug("uid is %s", pCluster->uid);
  }

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsClusterSdb,
    .pObj   = pCluster,
  };

  return sdbInsertRow(&row);
}

const char* mnodeGetClusterId() {
  return tsClusterId;
}

void mnodeUpdateClusterId() {
  SClusterObj *pCluster = NULL;
  void *pIter = mnodeGetNextCluster(NULL, &pCluster);
  if (pCluster != NULL) {
    tstrncpy(tsClusterId, pCluster->uid, TSDB_CLUSTER_ID_LEN);
    mDebug("cluster id is set to %s", tsClusterId);
  }

  mnodeDecClusterRef(pCluster);
  mnodeCancelGetNextCluster(pIter);
}

static int32_t mnodeGetClusterMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_CLUSTER_ID_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "clusterId");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  strcpy(pMeta->tableFname, "show cluster");
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = 1;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

static int32_t mnodeRetrieveClusters(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;
  int32_t cols = 0;
  char *  pWrite;
  SClusterObj *pCluster = NULL;

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextCluster(pShow->pIter, &pCluster);
    if (pCluster == NULL) break;
    
    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pCluster->uid, TSDB_CLUSTER_ID_LEN);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *) pWrite = pCluster->createdTime;
    cols++;

    mnodeDecClusterRef(pCluster);
    numOfRows++;
  }

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}
