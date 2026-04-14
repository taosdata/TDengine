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
#include "audit.h"
#include "mndCluster.h"
#include "mndGrant.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"

#define CLUSTER_VER_NUMBE    1
#define CLUSTER_RESERVE_SIZE 19
int64_t tsExpireTime = 0;

static SSdbRaw *mndClusterActionEncode(SClusterObj *pCluster);
static SSdbRow *mndClusterActionDecode(SSdbRaw *pRaw);
static int32_t  mndClusterActionInsert(SSdb *pSdb, SClusterObj *pCluster);
static int32_t  mndClusterActionDelete(SSdb *pSdb, SClusterObj *pCluster);
static int32_t  mndClusterActionUpdate(SSdb *pSdb, SClusterObj *pOldCluster, SClusterObj *pNewCluster);
static int32_t  mndCreateDefaultCluster(SMnode *pMnode);
static int32_t  mndRetrieveClusters(SRpcMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextCluster(SMnode *pMnode, void *pIter);
static int32_t  mndRetrieveSecurityPolicies(SRpcMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void     mndCancelGetNextSecurityPolicy(SMnode *pMnode, void *pIter);
static int32_t  mndProcessUptimeTimer(SRpcMsg *pReq);
static int32_t  mndProcessConfigClusterReq(SRpcMsg *pReq);
static int32_t  mndProcessConfigClusterRsp(SRpcMsg *pReq);
static int32_t  mndProcessEnforceSodImpl(SMnode *pMnode);

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
  mndSetMsgHandle(pMnode, TDMT_MND_CONFIG_CLUSTER, mndProcessConfigClusterReq);
  mndSetMsgHandle(pMnode, TDMT_MND_CONFIG_CLUSTER_RSP, mndProcessConfigClusterRsp);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_CLUSTER, mndRetrieveClusters);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_CLUSTER, mndCancelGetNextCluster);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_SECURITY_POLICIES, mndRetrieveSecurityPolicies);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_SECURITY_POLICIES, mndCancelGetNextSecurityPolicy);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupCluster(SMnode *pMnode) {}

int32_t mndGetClusterName(SMnode *pMnode, char *clusterName, int32_t len) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  SClusterObj *pCluster = sdbAcquire(pSdb, SDB_CLUSTER, &pMnode->clusterId);
  if (pCluster == NULL) {
    code = terrno;
    TAOS_RETURN(code);
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
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  SSdbRaw *pRaw = sdbAllocRaw(SDB_CLUSTER, CLUSTER_VER_NUMBE, sizeof(SClusterObj) + CLUSTER_RESERVE_SIZE);
  if (pRaw == NULL) goto _OVER;

  int32_t dataPos = 0;
  SDB_SET_INT64(pRaw, dataPos, pCluster->id, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pCluster->createdTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pCluster->updateTime, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pCluster->name, TSDB_CLUSTER_ID_LEN, _OVER)
  SDB_SET_INT32(pRaw, dataPos, pCluster->upTime, _OVER)
  SDB_SET_UINT8(pRaw, dataPos, pCluster->flag, _OVER)
  SDB_SET_BINARY(pRaw, dataPos, pCluster->sodActivator, TSDB_USER_LEN, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pCluster->sodActivateTime, _OVER)
  SDB_SET_INT64(pRaw, dataPos, pCluster->macActivateTime, _OVER)
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
  int32_t code = 0;
  int32_t lino = 0;
  terrno = TSDB_CODE_OUT_OF_MEMORY;
  SClusterObj *pCluster = NULL;
  SSdbRow     *pRow = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto _OVER;

  if (sver != CLUSTER_VER_NUMBE) {
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
  SDB_GET_UINT8(pRaw, dataPos, &pCluster->flag, _OVER)
  SDB_GET_BINARY(pRaw, dataPos, pCluster->sodActivator, TSDB_USER_LEN, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pCluster->sodActivateTime, _OVER)
  SDB_GET_INT64(pRaw, dataPos, &pCluster->macActivateTime, _OVER)
  SDB_GET_RESERVE(pRaw, dataPos, CLUSTER_RESERVE_SIZE, _OVER)

  // for backward compatibility
  if (pCluster->sodActivateTime == 0 && pCluster->macActivateTime == 0 && pCluster->createdTime != 0) {
    pCluster->sodActivateTime = taosGetTimestampMs();
    pCluster->macActivateTime = pCluster->sodActivateTime;
  }

  terrno = 0;

_OVER:
  if (terrno != 0) {
    mError("cluster:%" PRId64 ", failed to decode from raw:%p since %s", pCluster == NULL ? 0 : pCluster->id, pRaw,
           terrstr());
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
  pOld->flag = pNew->flag;
  tstrncpy(pOld->sodActivator, pNew->sodActivator, sizeof(pOld->sodActivator));
  pOld->sodActivateTime = pNew->sodActivateTime;
  pOld->macActivateTime = pNew->macActivateTime;
  return 0;
}

static int32_t mndCreateDefaultCluster(SMnode *pMnode) {
  SClusterObj clusterObj = {0};
  clusterObj.createdTime = taosGetTimestampMs();
  clusterObj.updateTime = clusterObj.createdTime;
  clusterObj.macActivateTime = clusterObj.createdTime;
  clusterObj.sodActivateTime = clusterObj.createdTime;


  int32_t code = taosGetSystemUUIDLen(clusterObj.name, TSDB_CLUSTER_ID_LEN);
  if (code != 0) {
    tstrncpy(clusterObj.name, "tdengine3.0", sizeof(clusterObj.name));
    mError("failed to get name from system, set to default val %s", clusterObj.name);
  }

  clusterObj.id = mndGenerateUid(clusterObj.name, TSDB_CLUSTER_ID_LEN);
  clusterObj.id = (clusterObj.id >= 0 ? clusterObj.id : -clusterObj.id);
  pMnode->clusterId = clusterObj.id;
  mInfo("cluster:%" PRId64 ", name is %s", clusterObj.id, clusterObj.name);

  SSdbRaw *pRaw = mndClusterActionEncode(&clusterObj);
  if (pRaw == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }
  code = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  if (code != 0) {
    sdbFreeRaw(pRaw);
    TAOS_RETURN(code);
  }

  mInfo("cluster:%" PRId64 ", will be created when deploying, raw:%p", clusterObj.id, pRaw);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, NULL, "create-cluster");
  if (pTrans == NULL) {
    sdbFreeRaw(pRaw);
    mError("cluster:%" PRId64 ", failed to create since %s", clusterObj.id, terrstr());
    code = terrno;
    TAOS_RETURN(code);
  }
  mInfo("trans:%d, used to create cluster:%" PRId64, pTrans->id, clusterObj.id);

  if ((code = mndTransAppendCommitlog(pTrans, pRaw)) != 0) {
    mError("trans:%d, failed to commit redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }
  code = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
  if (code != 0) {
    sdbFreeRaw(pRaw);
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndRetrieveClusters(SRpcMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode      *pMnode = pMsg->info.node;
  SSdb        *pSdb = pMnode->pSdb;
  int32_t      code = 0;
  int32_t      lino = 0;
  int32_t      numOfRows = 0;
  int32_t      cols = 0;
  SClusterObj *pCluster = NULL;

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_CLUSTER, pShow->pIter, (void **)&pCluster);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pCluster->id, false, pCluster, pShow->pIter, _OVER);

    char buf[tListLen(pCluster->name) + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(buf, pCluster->name, pShow->pMeta->pSchemas[cols].bytes);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pCluster, pShow->pIter, _OVER);

    int32_t upTime = mndGetClusterUpTimeImp(pCluster);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&upTime, false, pCluster, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pCluster->createdTime, false, pCluster, pShow->pIter, _OVER);

    char ver[12] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(ver, tsVersionName, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)ver, false, pCluster, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    if (tsExpireTime <= 0) {
      colDataSetNULL(pColInfo, numOfRows);
    } else {
      COL_DATA_SET_VAL_GOTO((const char *)&tsExpireTime, false, pCluster, pShow->pIter, _OVER);
    }

    sdbRelease(pSdb, pCluster);
    numOfRows++;
  }

  pShow->numOfRows += numOfRows;

_OVER:
  if (code != 0) {
    mError("failed to retrieve cluster info at line %d since %s", lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextCluster(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_CLUSTER);
}

static const char *_SoDMandatoryInfo[3][2] = {
    {"mandatory", "system is operational, root disabled permanently"},
    {"mandatory(initial)", "Initial phase: mandatory roles missing, only account setup operations are allowed"},
    {"mandatory(enforcing)", "Enforce phase: transitioning mode, account destructive operations are blocked"},
};

static int32_t mndRetrieveSecurityPolicies(SRpcMsg *pMsg, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode      *pMnode = pMsg->info.node;
  SSdb        *pSdb = pMnode->pSdb;
  int32_t      code = 0, lino = 0;
  int32_t      numOfRows = 0;
  int32_t      cols = 0;
  SClusterObj *pCluster = NULL;
  char         buf[128 + VARSTR_HEADER_SIZE] = {0};

  while ((numOfRows + 1) < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_CLUSTER, pShow->pIter, (void **)&pCluster);
    if (pShow->pIter == NULL) break;

    cols = 0;
    STR_WITH_MAXSIZE_TO_VARSTR(buf, "SoD", pShow->pMeta->pSchemas[cols].bytes);
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pCluster, pShow->pIter, _OVER);

    int32_t sodMode = pCluster->sodMode;
    int32_t sodPhase = mndGetSoDPhase(pMnode);
    bool    sodEnabled = (sodMode == SOD_MODE_ENABLED && sodPhase == TSDB_SOD_PHASE_STABLE);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, sodEnabled ? "enabled" : _SoDMandatoryInfo[sodPhase][0],
                               pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pCluster, pShow->pIter, _OVER);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, sodEnabled ? "SYSTEM" : pCluster->sodActivator, pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pCluster, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pCluster->sodActivateTime, false, pCluster, pShow->pIter, _OVER);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, sodEnabled ? "non-mandatory, root not disabled" : _SoDMandatoryInfo[sodPhase][1],
                               pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pCluster, pShow->pIter, _OVER);

    ++numOfRows;

    cols = 0;
    STR_WITH_MAXSIZE_TO_VARSTR(buf, "MAC", pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pCluster, pShow->pIter, _OVER);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, "mandatory", pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pCluster, pShow->pIter, _OVER);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, "SYSTEM", pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pCluster, pShow->pIter, _OVER);

    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO((const char *)&pCluster->macActivateTime, false, pCluster, pShow->pIter, _OVER);

    STR_WITH_MAXSIZE_TO_VARSTR(buf, "security levels 0-4, non-configurable", pShow->pMeta->pSchemas[cols].bytes);
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    COL_DATA_SET_VAL_GOTO(buf, false, pCluster, pShow->pIter, _OVER);

    sdbRelease(pSdb, pCluster);
    ++numOfRows;
  }

  pShow->numOfRows += numOfRows;

_OVER:
  if (code != 0) {
    mError("failed to retrieve security policies at line %d since %s", lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextSecurityPolicy(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_CLUSTER);
}

static int32_t mndProcessUptimeTimer(SRpcMsg *pReq) {
  SMnode      *pMnode = pReq->info.node;
  SClusterObj  clusterObj = {0};
  void        *pIter = NULL;
  SClusterObj *pCluster = mndAcquireCluster(pMnode, &pIter);
  if (pCluster != NULL) {
    (void)memcpy(&clusterObj, pCluster, sizeof(SClusterObj));
    clusterObj.upTime += tsUptimeInterval;
    mndReleaseCluster(pMnode, pCluster, pIter);
  }

  if (clusterObj.id <= 0) {
    mError("can't get cluster info while update uptime");
    return 0;
  }

  int32_t code = 0;
  mInfo("update cluster uptime to %d", clusterObj.upTime);
  // SoD mode also exists in cluster table, so use TRN_CONFLICT_ROLE aligned with SoD transaction.
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ROLE, pReq, "update-uptime");
  if (pTrans == NULL) {
    code = terrno;
    TAOS_RETURN(code);
  }

  SSdbRaw *pCommitRaw = mndClusterActionEncode(&clusterObj);
  if (pCommitRaw == NULL) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    code = terrno;
    TAOS_RETURN(code);
  }

  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    sdbFreeRaw(pCommitRaw);
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }
  code = sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);
  if (code != 0) {
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    TAOS_RETURN(code);
  }

  mndTransDrop(pTrans);
  return 0;
}

/**
 * @brief Separation of Duties(SoD) is a security principle that restricts the permissions of users to prevent them from
 * having excessive privileges that could lead to security breaches.
 *
 * @param pMnode
 * @param pReq
 * @param pCfg 
 * @return int32_t
 */
// #ifdef TD_ENTERPRISE
static int32_t mndProcessConfigSoDReq(SMnode *pMnode, SRpcMsg *pReq, SMCfgClusterReq *pCfg) {
  int32_t     code = 0, lino = 0;
  SClusterObj clusterObj = {0};
  STrans     *pTrans = NULL;
  SUserObj   *pRootUser = NULL;
  SUserObj    newRootUser = {0};

  TAOS_CHECK_EXIT(mndCheckOperPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_CONFIG_SOD));

  // Only support to set SoD mode to mandatory, which means SoD is enforced and root user is disabled permanently.
  if (taosStrncasecmp(pCfg->value, "mandatory", 10) != 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_CFG_VALUE);
  }

  void        *pIter = NULL;
  SClusterObj *pCluster = mndAcquireCluster(pMnode, &pIter);
  if (!pCluster || pCluster->id <= 0) {
    if (pCluster) mndReleaseCluster(pMnode, pCluster, pIter);
    TAOS_CHECK_EXIT(TSDB_CODE_APP_IS_STARTING);
  }

  if (pCluster->sodMode == SOD_MODE_MANDATORY) {
    mndReleaseCluster(pMnode, pCluster, pIter);
    TAOS_RETURN(0);
  }

  if ((code = mndCheckManagementRoleStatus(pMnode, NULL, 0))) {
    mndReleaseCluster(pMnode, pCluster, pIter);
    TAOS_CHECK_EXIT(code);
  }

  mInfo("update cluster SoD mode to mandatory by %s", RPC_MSG_USER(pReq));
  (void)memcpy(&clusterObj, pCluster, sizeof(SClusterObj));
  clusterObj.sodMode = SOD_MODE_MANDATORY;
  tstrncpy(clusterObj.sodActivator, RPC_MSG_USER(pReq), sizeof(clusterObj.sodActivator));
  clusterObj.sodActivateTime = taosGetTimestampMs();
  mndReleaseCluster(pMnode, pCluster, pIter);

  
  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, "root", &pRootUser));
  TAOS_CHECK_EXIT(mndUserDupObj(pRootUser, &newRootUser));
  newRootUser.enable = 0;

  // When SoD mode is updated to mandatory, root user will be disabled permanently and 3 common users with
  // SYSDBA、SYSSEC and SYSAUDIT should exist.
  pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_CONFLICT_ROLE, pReq, "update-sod-mode");
  if (pTrans == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }
  SSdbRaw *pCommitRaw = mndClusterActionEncode(&clusterObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    if (pCommitRaw) sdbFreeRaw(pCommitRaw);
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
  // disable root user
  SSdbRaw *pCommitRawRoot = mndUserActionEncode(&newRootUser);
  if (pCommitRawRoot == NULL || mndTransAppendCommitlog(pTrans, pCommitRawRoot) != 0) {
    if (pCommitRawRoot) sdbFreeRaw(pCommitRawRoot);
    TAOS_CHECK_EXIT(terrno);
  }
  TAOS_CHECK_EXIT(sdbSetRawStatus(pCommitRawRoot, SDB_STATUS_READY));
  mndSetSoDPhase(pMnode, TSDB_SOD_PHASE_ENFORCE);
  mndTransSetCb(pTrans, 0, TRANS_STOP_FUNC_SOD, NULL, 0);
  if ((code = mndTransPrepare(pMnode, pTrans)) != 0) {
    mndSetSoDPhase(pMnode, TSDB_SOD_PHASE_STABLE);  // restore SoD status to stable since transition won't happen
    TAOS_CHECK_EXIT(code);
  }
_exit:
  if(pRootUser) mndReleaseUser(pMnode, pRootUser);
  mndUserFreeObj(&newRootUser);
  mndTransDrop(pTrans);
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}
// #endif

int32_t mndProcessConfigClusterReq(SRpcMsg *pReq) {
  int32_t         code = 0;
  SMnode         *pMnode = pReq->info.node;
  SMCfgClusterReq cfgReq = {0};
  int64_t         tss = taosGetTimestampMs();
  if (tDeserializeSMCfgClusterReq(pReq->pCont, pReq->contLen, &cfgReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    TAOS_RETURN(code);
  }

  mInfo("cluster: start to config, option:%s, value:%s", cfgReq.config, cfgReq.value);
  if ((code = mndCheckOperPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_CONFIG_CLUSTER)) != 0) {
    goto _exit;
  }

  SClusterObj  clusterObj = {0};
  void        *pIter = NULL;
  SClusterObj *pCluster = mndAcquireCluster(pMnode, &pIter);
  if (!pCluster || pCluster->id <= 0) {
    code = TSDB_CODE_APP_IS_STARTING;
    if (pCluster) mndReleaseCluster(pMnode, pCluster, pIter);
    goto _exit;
  }
  (void)memcpy(&clusterObj, pCluster, sizeof(SClusterObj));
  mndReleaseCluster(pMnode, pCluster, pIter);

  if (strncmp(cfgReq.config, GRANT_ACTIVE_CODE, TSDB_DNODE_CONFIG_LEN) == 0) {
#ifdef TD_ENTERPRISE
    if (0 != (code = mndProcessConfigGrantReq(pMnode, pReq, &cfgReq))) {
      goto _exit;
    }
#else
    code = TSDB_CODE_OPS_NOT_SUPPORT;
    goto _exit;
#endif
  } else if (taosStrncasecmp(cfgReq.config, "SoD", 4) == 0 ||
             taosStrncasecmp(cfgReq.config, "separation_of_duties", 22) == 0) {
#ifdef TD_ENTERPRISE
    if (0 != (code = mndProcessConfigSoDReq(pMnode, pReq, &cfgReq))) {
      goto _exit;
    }
#else
    code = TSDB_CODE_OPS_NOT_SUPPORT;
    goto _exit;
#endif
  } else {
    code = TSDB_CODE_OPS_NOT_SUPPORT;
    goto _exit;
  }

  if (tsAuditLevel >= AUDIT_LEVEL_CLUSTER) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    {  // audit
      auditRecord(pReq, pMnode->clusterId, "alterCluster", "", "", cfgReq.sql,
                  TMIN(cfgReq.sqlLen, GRANT_ACTIVE_HEAD_LEN << 1), duration, 0);
    }
  }

_exit:
  tFreeSMCfgClusterReq(&cfgReq);
  if (code != 0) {
    mError("cluster: failed to config:%s %s since %s", cfgReq.config, cfgReq.value, tstrerror(code));
  } else {
    mInfo("cluster: success to config:%s %s", cfgReq.config, cfgReq.value);
  }
  TAOS_RETURN(code);
}

int32_t mndProcessConfigClusterRsp(SRpcMsg *pRsp) {
  mInfo("config rsp from cluster");
  return 0;
}

int32_t mndGetClusterSoDMode(SMnode *pMnode) {
  int32_t      sodMode = SOD_MODE_ENABLED;
  void        *pIter = NULL;
  SClusterObj *pCluster = mndAcquireCluster(pMnode, &pIter);
  if (pCluster != NULL) {
    sodMode = pCluster->sodMode;
    mndReleaseCluster(pMnode, pCluster, pIter);
  }

  return sodMode;
}

void mndSodTransStop(SMnode *pMnode, void *param, int32_t paramLen) {
#ifdef TD_ENTERPRISE
  mInfo("SoD trans stop, set SoD phase to %d", TSDB_SOD_PHASE_STABLE);
  mndSetSoDPhase(pMnode, TSDB_SOD_PHASE_STABLE);
#endif
}

void mndSodGrantRoleStop(SMnode *pMnode, void *param, int32_t paramLen) {
#ifdef TD_ENTERPRISE
  if (mndGetSoDPhase(pMnode) != TSDB_SOD_PHASE_INITIAL) {
    return;
  }

  if (mndCheckManagementRoleStatus(pMnode, NULL, 0) == 0) {
    mInfo("SoD role check completed, all mandatory roles satisfied, trigger enforce SoD");
    (void)mndProcessEnforceSodImpl(pMnode);
  }
#endif
}
#ifdef TD_ENTERPRISE
static int32_t mndProcessEnforceSodImpl(SMnode *pMnode) {
  int32_t code = 0, lino = 0;
  int32_t contLen = 0;
  void   *pCont = NULL;

  SMCfgClusterReq cfgReq = {0};
  tsnprintf(cfgReq.config, sizeof(cfgReq.config), "SoD");
  tsnprintf(cfgReq.value, sizeof(cfgReq.value), "mandatory");
  contLen = tSerializeSMCfgClusterReq(NULL, 0, &cfgReq);
  TAOS_CHECK_EXIT(contLen);
  if (!(pCont = rpcMallocCont(contLen))) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  if ((code = tSerializeSMCfgClusterReq(pCont, contLen, &cfgReq)) != contLen) {
    rpcFreeCont(pCont);
    TAOS_CHECK_EXIT(code);
  }

  SRpcMsg rpcMsg = {.pCont = pCont,
                    .contLen = contLen,
                    .msgType = TDMT_MND_CONFIG_CLUSTER,
                    .info.ahandle = 0,
                    .info.notFreeAhandle = 1};
  SEpSet epSet = {0};
  mndGetMnodeEpSet(pMnode, &epSet);
  TAOS_CHECK_EXIT(tmsgSendReq(&epSet, &rpcMsg));
_exit:
  if (code < 0) {
    mError("failed at line %d to enforce SoD since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

int32_t mndProcessEnforceSod(SMnode *pMnode) {
  int32_t code = 0, lino = 0;
  void   *pIter = NULL;

  SClusterObj *pCluster = mndAcquireCluster(pMnode, &pIter);
  if (pCluster == NULL) {
    TAOS_CHECK_EXIT(terrno);
  }

  if (pCluster->sodMode == SOD_MODE_MANDATORY) {
    mInfo("cluster is already in SoD mandatory mode");
    mndReleaseCluster(pMnode, pCluster, pIter);
    TAOS_RETURN(0);
  }

  mndSetSoDPhase(pMnode, TSDB_SOD_PHASE_INITIAL);
  mInfo("start to enforce SoD from initial phase, cluster:%" PRId64, pCluster->id);
  if ((code = mndCheckManagementRoleStatus(pMnode, NULL, 0))) {
    mndReleaseCluster(pMnode, pCluster, pIter);
    TAOS_CHECK_EXIT(code);
  }

  TAOS_CHECK_EXIT(mndProcessEnforceSodImpl(pMnode));
  code = TSDB_CODE_ACTION_IN_PROGRESS;

_exit:
  if (code < 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("failed to enforce SoD at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}
#endif
