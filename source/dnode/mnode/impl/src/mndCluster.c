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

#define CLUSTER_VER_NUMBE    2
#define CLUSTER_RESERVE_SIZE 60
int64_t tsExpireTime = 0;

static SSdbRaw *mndClusterActionEncode(SClusterObj *pCluster);
static SSdbRow *mndClusterActionDecode(SSdbRaw *pRaw);
static int32_t  mndClusterActionInsert(SSdb *pSdb, SClusterObj *pCluster);
static int32_t  mndClusterActionDelete(SSdb *pSdb, SClusterObj *pCluster);
static int32_t  mndClusterActionUpdate(SSdb *pSdb, SClusterObj *pOldCluster, SClusterObj *pNewCluster);
static int32_t  mndCreateDefaultCluster(SMnode *pMnode);
static int32_t  mndRetrieveClusters(SRpcMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextCluster(SMnode *pMnode, void *pIter);
static int32_t  mndProcessUptimeTimer(SRpcMsg *pReq);

int32_t mndInitCluster(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_CLUSTER,
      .keyType = SDB_KEY_INT64,
      .deployFp = (SdbDeployFp)mndCreateDefaultCluster,
      .encodeFp = (SdbEncodeFp)mndClusterActionEncode,
      .decodeFp = (SdbDecodeFp)mndClusterActionDecode,
      .insertFp = (SdbInsertFp)mndClusterActionInsert,
      .updateFp = (SdbUpdateFp)mndClusterActionUpdate,
      .deleteFp = (SdbDeleteFp)mndClusterActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_UPTIME_TIMER, mndProcessUptimeTimer);
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

static SClusterObj *mndAcquireCluster(SMnode *pMnode, void **ppIter) {
  SSdb *pSdb = pMnode->pSdb;
  void *pIter = NULL;

  while (1) {
    SClusterObj *pCluster = NULL;
    pIter = sdbFetch(pSdb, SDB_CLUSTER, pIter, (void **)&pCluster);
    if (pIter == NULL) break;

    *ppIter = pIter;
    return pCluster;
  }

  return NULL;
}

static void mndReleaseCluster(SMnode *pMnode, SClusterObj *pCluster, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
  sdbRelease(pSdb, pCluster);
}

int64_t mndGetClusterId(SMnode *pMnode) {
  int64_t      clusterId = 0;
  void        *pIter = NULL;
  SClusterObj *pCluster = mndAcquireCluster(pMnode, &pIter);
  if (pCluster != NULL) {
    clusterId = pCluster->id;
    mndReleaseCluster(pMnode, pCluster, pIter);
  }

  return clusterId;
}

int64_t mndGetClusterCreateTime(SMnode *pMnode) {
  int64_t      createTime = 0;
  void        *pIter = NULL;
  SClusterObj *pCluster = mndAcquireCluster(pMnode, &pIter);
  if (pCluster != NULL) {
    createTime = pCluster->createdTime;
    mndReleaseCluster(pMnode, pCluster, pIter);
  }

  return createTime;
}

static int32_t mndGetClusterUpTimeImp(SClusterObj *pCluster) {
#if 0
  int32_t upTime = taosGetTimestampSec() - pCluster->updateTime / 1000;
  upTime = upTime + pCluster->upTime;
  return upTime;
#else
  return pCluster->upTime;
#endif
}

int64_t mndGetClusterUpTime(SMnode *pMnode) {
  int64_t      upTime = 0;
  void        *pIter = NULL;
  SClusterObj *pCluster = mndAcquireCluster(pMnode, &pIter);
  if (pCluster != NULL) {
    upTime = mndGetClusterUpTimeImp(pCluster);
    mndReleaseCluster(pMnode, pCluster, pIter);
  }

  return upTime;
}

static SSdbRaw *mndClusterActionEncode(SClusterObj *pCluster) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t  nMachineIds = taosArrayGetSize(pCluster->pMachineIds);
  int32_t  machineSize = sizeof(int16_t) + nMachineIds * sizeof(SMachineId);
  SSdbRaw *pRaw = sdbAllocRaw(SDB_CLUSTER, CLUSTER_VER_NUMBE, sizeof(SClusterObj) + machineSize + CLUSTER_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT64(pRaw, dataPos, pCluster->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pCluster->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pCluster->updateTime, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pCluster->name, TSDB_CLUSTER_ID_LEN, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pCluster->upTime, _OVER)
  SDB_SET_INT16(pRaw, dataPos, nMachineIds, _OVER)
  for (int32_t i = 0; i < nMachineIds; ++i) {
    SDB_SET_BINARY(pRaw, dataPos, ((SMachineId*)TARRAY_GET_ELEM(pCluster->pMachineIds, i))->id, TSDB_MACHINE_ID_LEN, _OVER)
  }
  SDB_SET_RESERVE(pRaw, dataPos, CLUSTER_RESERVE_SIZE, _OVER)
  SDB_SET_DATALEN(pRaw, dataPos, _OVER);

  terrno = 0;

_OVER:
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
  SClusterObj *pCluster = NULL;
  SSdbRow     *pRow = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver < 0 || sver > CLUSTER_VER_NUMBE) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto _OVER;
  }

  pRow = sdbAllocRow(sizeof(SClusterObj));
  if (pRow == NULL) goto _OVER;

  pCluster = sdbGetRowObj(pRow);
  if (pCluster == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_GET_INT64(pRaw, dataPos, &pCluster->id, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pCluster->createdTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pCluster->updateTime, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pCluster->name, TSDB_CLUSTER_ID_LEN, _OVER)
  SDB_GET_INT32(pRaw, dataPos, &pCluster->upTime, _OVER)

  if (sver > 1) {
    int16_t nMachineIds = 0;
    SDB_GET_INT16(pRaw, dataPos, &nMachineIds, _OVER)
    if (nMachineIds > 0) {
      pCluster->pMachineIds = taosArrayInit(nMachineIds, sizeof(SMachineId));
      if (!pCluster->pMachineIds) goto _OVER;
      for (int16_t i = 0; i < nMachineIds; ++i) {
        SDB_GET_BINARY(pRaw, dataPos, ((SMachineId *)TARRAY_GET_ELEM(pCluster->pMachineIds, i))->id,
                       TSDB_MACHINE_ID_LEN, _OVER)
      }
    }
  }
  SDB_GET_RESERVE(pRaw, dataPos, CLUSTER_RESERVE_SIZE, _OVER)

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("cluster:%" PRId64 ", failed to decode from raw:%p since %s", pCluster == NULL ? 0 : pCluster->id, pRaw,
           terrstr());
    mndFreeClusterObj(pCluster);
    taosMemoryFreeClear(pRow);
    return NULL;
  }

  mTrace("cluster:%" PRId64 ", decode from raw:%p, row:%p", pCluster->id, pRaw, pCluster);
  return pRow;
}

static int32_t mndClusterActionInsert(SSdb *pSdb, SClusterObj *pCluster) {
  mTrace("cluster:%" PRId64 ", perform insert action, row:%p", pCluster->id, pCluster);
  pSdb->pMnode->clusterId = pCluster->id;
  pCluster->updateTime = taosGetTimestampMs();
  return 0;
}

static int32_t mndClusterActionDelete(SSdb *pSdb, SClusterObj *pCluster) {
  mTrace("cluster:%" PRId64 ", perform delete action, row:%p", pCluster->id, pCluster);
  return 0;
}

static int32_t mndClusterActionUpdate(SSdb *pSdb, SClusterObj *pOld, SClusterObj *pNew) {
  mTrace("cluster:%" PRId64 ", perform update action, old row:%p new row:%p, uptime from %d to %d", pOld->id, pOld,
         pNew, pOld->upTime, pNew->upTime);
  pOld->upTime = pNew->upTime;
  pOld->updateTime = taosGetTimestampMs();
  return 0;
}

static int32_t mndCreateDefaultCluster(SMnode *pMnode) {
  SClusterObj clusterObj = {0};
  clusterObj.createdTime = taosGetTimestampMs();
  clusterObj.updateTime = clusterObj.createdTime;

  int32_t code = taosGetSystemUUID(clusterObj.name, TSDB_CLUSTER_ID_LEN);
  if (code != 0) {
    strcpy(clusterObj.name, "tdengine3.0");
    mError("failed to get name from system, set to default val %s", clusterObj.name);
  }

  clusterObj.id = mndGenerateUid(clusterObj.name, TSDB_CLUSTER_ID_LEN);
  clusterObj.id = (clusterObj.id >= 0 ? clusterObj.id : -clusterObj.id);
  pMnode->clusterId = clusterObj.id;
  clusterObj.pMachineIds = taosArrayInit(1, sizeof(SMachineId));
  if(!clusterObj.pMachineIds) {
    return -1;
  }
  char *machineId = grantGetMachineId();
  if (machineId) {
    taosArrayPush(clusterObj.pMachineIds, machineId);
    taosMemoryFree(machineId);
  }

  mInfo("cluster:%" PRId64 ", name is %s", clusterObj.id, clusterObj.name);

  SSdbRaw *pRaw = mndClusterActionEncode(&clusterObj);
  if (pRaw == NULL) {
    mndFreeClusterObj(&clusterObj);
    return -1;
  }
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  mInfo("cluster:%" PRId64 ", will be created when deploying, raw:%p", clusterObj.id, pRaw);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "create-cluster");
  if (pTrans == NULL) {
    sdbFreeRaw(pRaw);
    mndFreeClusterObj(&clusterObj);
    mError("cluster:%" PRId64 ", failed to create since %s", clusterObj.id, terrstr());
    return -1;
  }
  mInfo("trans:%d, used to create cluster:%" PRId64, pTrans->id, clusterObj.id);

  if (mndTransAppendCommitlog(pTrans, pRaw) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    mndFreeClusterObj(&clusterObj);
    return -1;
  }
  (void)sdbSetRawStatus(pRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    mndFreeClusterObj(&clusterObj);
    return -1;
  }

  mndTransDrop(pTrans);
  mndFreeClusterObj(&clusterObj);
  return 0;
}

static int32_t mndRetrieveClusters(SRpcMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode      *pMnode = pMsg->info.node;
  SSdb        *pSdb = pMnode->pSdb;
  int32_t      numOfRows = 0;
  int32_t      cols = 0;
  SClusterObj *pCluster = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_CLUSTER, pShow->pIter, (void **)&pCluster);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pCluster->id, false);

    char buf[tListLen(pCluster->name) + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(buf, pCluster->name, pShow->pMeta->pSchemas[cols].bytes);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, buf, false);

    int32_t upTime = mndGetClusterUpTimeImp(pCluster);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&upTime, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)&pCluster->createdTime, false);

    char ver[12] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(ver, tsVersionName, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    colDataSetVal(pColInfo, numOfRows, (const char *)ver, false);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (tsExpireTime <= 0) {
      colDataSetNULL(pColInfo, numOfRows);
    } else {
      colDataSetVal(pColInfo, numOfRows, (const char *)&tsExpireTime, false);
    }

    sdbRelease(pSdb, pCluster);
    numOfRows++;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextCluster(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static int32_t mndProcessUptimeTimer(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  SClusterObj  clusterObj = {0};
  void        *pIter = NULL;
  SClusterObj *pCluster = mndAcquireCluster(pMnode, &pIter);
  if (pCluster != NULL) {
    if(mndDupClusterObj(pCluster, &clusterObj) != 0){
      mndReleaseCluster(pMnode, pCluster, pIter);
      mndFreeClusterObj(&clusterObj);
      return -1;
    }
    clusterObj.upTime += tsUptimeInterval;
    mndReleaseCluster(pMnode, pCluster, pIter);
  }

  if (clusterObj.id <= 0) {
    mError("can't get cluster info while update uptime");
    mndFreeClusterObj(&clusterObj);
    return 0;
  }

  mInfo("update cluster uptime to %d", clusterObj.upTime);
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, pReq, "update-uptime");
  if (pTrans == NULL) {
    mndFreeClusterObj(&clusterObj);
    return -1;
  }

  SSdbRaw *pCommitRaw = mndClusterActionEncode(&clusterObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    mndFreeClusterObj(&clusterObj);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    mndFreeClusterObj(&clusterObj);
    return -1;
  }

  mndTransDrop(pTrans);
  mndFreeClusterObj(&clusterObj);
  return 0;
}

int32_t mndDupClusterObj(SClusterObj *pOld, SClusterObj *pNew) {
  memcpy(pNew, pOld, sizeof(SClusterObj));
  pNew->updateTime = taosGetTimestampMs();
  pNew->pMachineIds = taosArrayInit(taosArrayGetSize(pOld->pMachineIds), sizeof(SMachineId));
  if (!pNew->pMachineIds) return -1;
  taosArrayAddAll(pNew->pMachineIds, pOld->pMachineIds);
  return 0;
}

void mndFreeClusterObj(SClusterObj *pCluster) {
  if (pCluster) {
    pCluster->pMachineIds = taosArrayDestroy(pCluster->pMachineIds);
  }
}

int32_t mndProcessClusterMachineIds(SMnode *pMnode, SMachineId *pIds, int32_t nIds) {
  SClusterObj  clusterObj = {0};
  void        *pIter = NULL;
  SClusterObj *pCluster = mndAcquireCluster(pMnode, &pIter);
  if (pCluster != NULL) {
    int32_t nDups = 0;
    int32_t size = taosArrayGetSize(pCluster->pMachineIds);
    for (int32_t n = 0; n < nIds; ++n) {
      bool exist = false;
      for (int32_t i = 0; i < size; ++i) {
        SMachineId *pId = TARRAY_GET_ELEM(pCluster->pMachineIds, i);
        if (0 == strncmp(pId->id, (pIds + nIds)->id, TSDB_MACHINE_ID_LEN + 1)) {
          exist = true;
          ++nDups;
          break;
        }
      }
      if (!exist) {
        if (!clusterObj.pMachineIds) {
          if(mndDupClusterObj(pCluster, &clusterObj) != 0){
            mndReleaseCluster(pMnode, pCluster, pIter);
            mndFreeClusterObj(&clusterObj);
            return -1;
          }
        }
        if (!taosArrayPush(clusterObj.pMachineIds, pIds + n)) {
          mndReleaseCluster(pMnode, pCluster, pIter);
          mndFreeClusterObj(&clusterObj);
          terrno = TSDB_CODE_OUT_OF_MEMORY;
          return -1;
        }
      }
    }

    if (nDups == nIds) {
      mndReleaseCluster(pMnode, pCluster, pIter);
      mndFreeClusterObj(&clusterObj);
      return 0;
    }

    mndReleaseCluster(pMnode, pCluster, pIter);
  }

  if (clusterObj.id <= 0) {
    mError("can't get cluster info while process machine-id");
    mndFreeClusterObj(&clusterObj);
    return -1;
  }

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_NOTHING, NULL, "machine-id");
  if (pTrans == NULL) {
    mndFreeClusterObj(&clusterObj);
    return -1;
  }

  SSdbRaw *pCommitRaw = mndClusterActionEncode(&clusterObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    mndFreeClusterObj(&clusterObj);
    return -1;
  }
  (void)sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    mndFreeClusterObj(&clusterObj);
    return -1;
  }

  mndTransDrop(pTrans);
  mndFreeClusterObj(&clusterObj);
  return 0;
}
