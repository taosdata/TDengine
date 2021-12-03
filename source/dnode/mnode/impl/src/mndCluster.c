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
#include "mndTrans.h"

#define SDB_CLUSTER_VER 1

static SSdbRaw *mndClusterActionEncode(SClusterObj *pCluster) {
  SSdbRaw *pRaw = sdbAllocRaw(SDB_CLUSTER, SDB_CLUSTER_VER, sizeof(SClusterObj));
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, pCluster->id);
  SDB_SET_INT64(pRaw, dataPos, pCluster->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pCluster->updateTime)
  SDB_SET_BINARY(pRaw, dataPos, pCluster->uid, TSDB_CLUSTER_ID_LEN)

  return pRaw;
}

static SSdbRow *mndClusterActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != SDB_CLUSTER_VER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("failed to decode cluster since %s", terrstr());
    return NULL;
  }

  SSdbRow     *pRow = sdbAllocRow(sizeof(SClusterObj));
  SClusterObj *pCluster = sdbGetRowObj(pRow);
  if (pCluster == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, pRow, dataPos, &pCluster->id)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pCluster->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pCluster->updateTime)
  SDB_GET_BINARY(pRaw, pRow, dataPos, pCluster->uid, TSDB_CLUSTER_ID_LEN)

  return pRow;
}

static int32_t mndClusterActionInsert(SSdb *pSdb, SClusterObj *pCluster) {
  mTrace("cluster:%d, perform insert action", pCluster->id);
  return 0;
}

static int32_t mndClusterActionDelete(SSdb *pSdb, SClusterObj *pCluster) {
  mTrace("cluster:%d, perform delete action", pCluster->id);
  return 0;
}

static int32_t mndClusterActionUpdate(SSdb *pSdb, SClusterObj *pSrcCluster, SClusterObj *pDstCluster) {
  mTrace("cluster:%d, perform update action", pSrcCluster->id);
  return 0;
}

static int32_t mndCreateDefaultCluster(SMnode *pMnode) {
  SClusterObj clusterObj = {0};
  clusterObj.createdTime = taosGetTimestampMs();
  clusterObj.updateTime = clusterObj.createdTime;

  int32_t code = taosGetSystemUid(clusterObj.uid, TSDB_CLUSTER_ID_LEN);
  if (code != 0) {
    strcpy(clusterObj.uid, "tdengine2.0");
    mError("failed to get uid from system, set to default val %s", clusterObj.uid);
  } else {
    mDebug("cluster:%d, uid is %s", clusterObj.id, clusterObj.uid);
  }
  clusterObj.id = MurmurHash3_32(clusterObj.uid, TSDB_CLUSTER_ID_LEN);
  clusterObj.id = abs(clusterObj.id);
  pMnode->clusterId = clusterObj.id;

  SSdbRaw *pRaw = mndClusterActionEncode(&clusterObj);
  if (pRaw == NULL) return -1;
  sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mTrace("cluster:%d, will be created while deploy sdb", clusterObj.id);
  return sdbWrite(pMnode->pSdb, pRaw);
}

int32_t mndInitCluster(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_CLUSTER,
                     .keyType = SDB_KEY_INT32,
                     .deployFp = (SdbDeployFp)mndCreateDefaultCluster,
                     .encodeFp = (SdbEncodeFp)mndClusterActionEncode,
                     .decodeFp = (SdbDecodeFp)mndClusterActionDecode,
                     .insertFp = (SdbInsertFp)mndClusterActionInsert,
                     .updateFp = (SdbUpdateFp)mndClusterActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndClusterActionDelete};

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupCluster(SMnode *pMnode) {}
