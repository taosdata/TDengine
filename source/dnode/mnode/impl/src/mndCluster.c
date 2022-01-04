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
#include "mndCluster.h"
#include "mndShow.h"
#include "mndTrans.h"

#define TSDB_CLUSTER_VER_NUMBE 1
#define TSDB_CLUSTER_RESERVE_SIZE 64

static SSdbRaw *mndClusterActionEncode(SClusterObj *pCluster);
static SSdbRow *mndClusterActionDecode(SSdbRaw *pRaw);
static int32_t  mndClusterActionInsert(SSdb *pSdb, SClusterObj *pCluster);
static int32_t  mndClusterActionDelete(SSdb *pSdb, SClusterObj *pCluster);
static int32_t  mndClusterActionUpdate(SSdb *pSdb, SClusterObj *pOldCluster, SClusterObj *pNewCluster);
static int32_t  mndCreateDefaultCluster(SMnode *pMnode);
static int32_t  mndGetClusterMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t  mndRetrieveClusters(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextCluster(SMnode *pMnode, void *pIter);

int32_t mndInitCluster(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_CLUSTER,
                     .keyType = SDB_KEY_INT32,
                     .deployFp = (SdbDeployFp)mndCreateDefaultCluster,
                     .encodeFp = (SdbEncodeFp)mndClusterActionEncode,
                     .decodeFp = (SdbDecodeFp)mndClusterActionDecode,
                     .insertFp = (SdbInsertFp)mndClusterActionInsert,
                     .updateFp = (SdbUpdateFp)mndClusterActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndClusterActionDelete};

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_CLUSTER, mndGetClusterMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CLUSTER, mndRetrieveClusters);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_CLUSTER, mndCancelGetNextCluster);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupCluster(SMnode *pMnode) {}

int32_t mndGetClusterName(SMnode *pMnode, char *clusterName, int32_t len) {
  SSdb *pSdb = pMnode->pSdb;

  SClusterObj *pCluster = sdbAcquire(pSdb, SDB_CLUSTER, &pMnode->clusterId);
  if (pCluster = NULL) {
    return -1;
  }

  tstrncpy(clusterName, pCluster->name, len);
  sdbRelease(pSdb, pCluster);
  return 0;
}

static SSdbRaw *mndClusterActionEncode(SClusterObj *pCluster) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_CLUSTER, TSDB_CLUSTER_VER_NUMBE, sizeof(SClusterObj) + TSDB_CLUSTER_RESERVE_SIZE);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_INT64(pRaw, dataPos, pCluster->id);
  SDB_SET_INT64(pRaw, dataPos, pCluster->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pCluster->updateTime)
  SDB_SET_BINARY(pRaw, dataPos, pCluster->name, TSDB_CLUSTER_ID_LEN)
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_CLUSTER_RESERVE_SIZE)

  return pRaw;
}

static SSdbRow *mndClusterActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != TSDB_CLUSTER_VER_NUMBE) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("failed to decode cluster since %s", terrstr());
    return NULL;
  }

  SSdbRow     *pRow = sdbAllocRow(sizeof(SClusterObj));
  SClusterObj *pCluster = sdbGetRowObj(pRow);
  if (pCluster == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_INT64(pRaw, pRow, dataPos, &pCluster->id)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pCluster->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pCluster->updateTime)
  SDB_GET_BINARY(pRaw, pRow, dataPos, pCluster->name, TSDB_CLUSTER_ID_LEN)
  SDB_GET_RESERVE(pRaw, pRow, dataPos, TSDB_CLUSTER_RESERVE_SIZE)

  return pRow;
}

static int32_t mndClusterActionInsert(SSdb *pSdb, SClusterObj *pCluster) {
  mTrace("cluster:%" PRId64 ", perform insert action", pCluster->id);
  return 0;
}

static int32_t mndClusterActionDelete(SSdb *pSdb, SClusterObj *pCluster) {
  mTrace("cluster:%" PRId64 ", perform delete action", pCluster->id);
  return 0;
}

static int32_t mndClusterActionUpdate(SSdb *pSdb, SClusterObj *pOldCluster, SClusterObj *pNewCluster) {
  mTrace("cluster:%" PRId64 ", perform update action", pOldCluster->id);
  return 0;
}

static int32_t mndCreateDefaultCluster(SMnode *pMnode) {
  SClusterObj clusterObj = {0};
  clusterObj.createdTime = taosGetTimestampMs();
  clusterObj.updateTime = clusterObj.createdTime;

  int32_t code = taosGetSystemUid(clusterObj.name, TSDB_CLUSTER_ID_LEN);
  if (code != 0) {
    strcpy(clusterObj.name, "tdengine2.0");
    mError("failed to get name from system, set to default val %s", clusterObj.name);
  } else {
    mDebug("cluster:%" PRId64 ", name is %s", clusterObj.id, clusterObj.name);
  }
  clusterObj.id = MurmurHash3_32(clusterObj.name, TSDB_CLUSTER_ID_LEN);
  clusterObj.id = (clusterObj.id >= 0 ? clusterObj.id : -clusterObj.id);
  pMnode->clusterId = clusterObj.id;

  SSdbRaw *pRaw = mndClusterActionEncode(&clusterObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mDebug("cluster:%" PRId64 ", will be created while deploy sdb", clusterObj.id);
  return sdbWrite(pMnode->pSdb, pRaw);
}

static int32_t mndGetClusterMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchema;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_BIGINT;
  strcpy(pSchema[cols].name, "id");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_CLUSTER_ID_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = htonl(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htonl(cols);
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = 1;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

static int32_t mndRetrieveClusters(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode      *pMnode = pMsg->pMnode;
  SSdb        *pSdb = pMnode->pSdb;
  int32_t      numOfRows = 0;
  int32_t      cols = 0;
  char        *pWrite;
  SClusterObj *pCluster = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_CLUSTER, pShow->pIter, (void **)&pCluster);
    if (pShow->pIter == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pCluster->id;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pCluster->name, TSDB_CLUSTER_ID_LEN);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pCluster->createdTime;
    cols++;

    sdbRelease(pSdb, pCluster);
    numOfRows++;
  }

  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextCluster(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
