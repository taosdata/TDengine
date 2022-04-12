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

#define TSDB_CLUSTER_VER_NUMBE    1
#define TSDB_CLUSTER_RESERVE_SIZE 64

static SSdbRaw *mndClusterActionEncode(SClusterObj *pCluster);
static SSdbRow *mndClusterActionDecode(SSdbRaw *pRaw);
static int32_t  mndClusterActionInsert(SSdb *pSdb, SClusterObj *pCluster);
static int32_t  mndClusterActionDelete(SSdb *pSdb, SClusterObj *pCluster);
static int32_t  mndClusterActionUpdate(SSdb *pSdb, SClusterObj *pOldCluster, SClusterObj *pNewCluster);
static int32_t  mndCreateDefaultCluster(SMnode *pMnode);
static int32_t  mndRetrieveClusters(SNodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextCluster(SMnode *pMnode, void *pIter);

int32_t mndInitCluster(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_CLUSTER,
                     .keyType = SDB_KEY_INT64,
                     .deployFp = (SdbDeployFp)mndCreateDefaultCluster,
                     .encodeFp = (SdbEncodeFp)mndClusterActionEncode,
                     .decodeFp = (SdbDecodeFp)mndClusterActionDecode,
                     .insertFp = (SdbInsertFp)mndClusterActionInsert,
                     .updateFp = (SdbUpdateFp)mndClusterActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndClusterActionDelete};

  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CLUSTER, mndRetrieveClusters);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_CLUSTER, mndCancelGetNextCluster);
  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupCluster(SMnode *pMnode) {}

int32_t mndGetClusterName(SMnode *pMnode, char *clusterName, int32_t len) {
  SSdb *pSdb = pMnode->pSdb;

  SClusterObj *pCluster = sdbAcquire(pSdb, SDB_CLUSTER, &pMnode->clusterId);
  if (pCluster == NULL) {
    return -1;
  }

  tstrncpy(clusterName, pCluster->name, len);
  sdbRelease(pSdb, pCluster);
  return 0;
}

int64_t mndGetClusterId(SMnode *pMnode) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  int64_t clusterId = -1;

  while (1) {
    SClusterObj *pCluster = NULL;
    pIter = sdbFetch(pSdb, SDB_CLUSTER, pIter, (void **)&pCluster);
    if (pIter == NULL) break;

    clusterId = pCluster->id;
    sdbRelease(pSdb, pCluster);
  }

  return clusterId;
}

static SSdbRaw *mndClusterActionEncode(SClusterObj *pCluster) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_CLUSTER, TSDB_CLUSTER_VER_NUMBE, sizeof(SClusterObj) + TSDB_CLUSTER_RESERVE_SIZE);
  if (pRaw == NULL) goto CLUSTER_ENCODE_OVER;

  int32_t dataPos = 0;
  SDB_SET_INT64(pRaw, dataPos, pCluster->id, CLUSTER_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pCluster->createdTime, CLUSTER_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pCluster->updateTime, CLUSTER_ENCODE_OVER)
  SDB_SET_BINARY(pRaw, dataPos, pCluster->name, TSDB_CLUSTER_ID_LEN, CLUSTER_ENCODE_OVER)
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_CLUSTER_RESERVE_SIZE, CLUSTER_ENCODE_OVER)

  terrno = 0;

CLUSTER_ENCODE_OVER:
  if (terrno != 0) {
    mError("cluster:%" PRId64 ", failed to encode to raw:%p since %s", pCluster->id, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("cluster:%" PRId64 ", encode to raw:%p, row:%p", pCluster->id, pRaw, pCluster);
  return pRaw;
}

static SSdbRow *mndClusterActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto CLUSTER_DECODE_OVER;

  if (sver != TSDB_CLUSTER_VER_NUMBE) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto CLUSTER_DECODE_OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SClusterObj));
  if (pRow == NULL) goto CLUSTER_DECODE_OVER;

  SClusterObj *pCluster = sdbGetRowObj(pRow);
  if (pCluster == NULL) goto CLUSTER_DECODE_OVER;

  int32_t dataPos = 0;
  SDB_GET_INT64(pRaw, dataPos, &pCluster->id, CLUSTER_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pCluster->createdTime, CLUSTER_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pCluster->updateTime, CLUSTER_DECODE_OVER)
  SDB_GET_BINARY(pRaw, dataPos, pCluster->name, TSDB_CLUSTER_ID_LEN, CLUSTER_DECODE_OVER)
  SDB_GET_RESERVE(pRaw, dataPos, TSDB_CLUSTER_RESERVE_SIZE, CLUSTER_DECODE_OVER)

  terrno = 0;

CLUSTER_DECODE_OVER:
  if (terrno != 0) {
    mError("cluster:%" PRId64 ", failed to decode from raw:%p since %s", pCluster->id, pRaw, terrstr());
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("cluster:%" PRId64 ", decode from raw:%p, row:%p", pCluster->id, pRaw, pCluster);
  return pRow;
}

static int32_t mndClusterActionInsert(SSdb *pSdb, SClusterObj *pCluster) {
  mTrace("cluster:%" PRId64 ", perform insert action, row:%p", pCluster->id, pCluster);
  return 0;
}

static int32_t mndClusterActionDelete(SSdb *pSdb, SClusterObj *pCluster) {
  mTrace("cluster:%" PRId64 ", perform delete action, row:%p", pCluster->id, pCluster);
  return 0;
}

static int32_t mndClusterActionUpdate(SSdb *pSdb, SClusterObj *pOld, SClusterObj *pNew) {
  mTrace("cluster:%" PRId64 ", perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  return 0;
}

static int32_t mndCreateDefaultCluster(SMnode *pMnode) {
  SClusterObj clusterObj = {0};
  clusterObj.createdTime = taosGetTimestampMs();
  clusterObj.updateTime = clusterObj.createdTime;

  int32_t code = taosGetSystemUUID(clusterObj.name, TSDB_CLUSTER_ID_LEN);
  if (code != 0) {
    strcpy(clusterObj.name, "tdengine2.0");
    mError("failed to get name from system, set to default val %s", clusterObj.name);
  }

  clusterObj.id = mndGenerateUid(clusterObj.name, TSDB_CLUSTER_ID_LEN);
  clusterObj.id = (clusterObj.id >= 0 ? clusterObj.id : -clusterObj.id);
  pMnode->clusterId = clusterObj.id;
  mDebug("cluster:%" PRId64 ", name is %s", clusterObj.id, clusterObj.name);

  SSdbRaw *pRaw = mndClusterActionEncode(&clusterObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mDebug("cluster:%" PRId64 ", will be created while deploy sdb, raw:%p", clusterObj.id, pRaw);
  return sdbWrite(pMnode->pSdb, pRaw);
}

static int32_t mndRetrieveClusters(SNodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode      *pMnode = pMsg->pNode;
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
