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
#include "dnode.h"
#include "machine.h"
#include "mndAcct.h"
#include "mndCluster.h"
#include "mndDb.h"
#include "mndDef.h"
#include "mndDnode.h"
#include "mndGrant.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndSync.h"
#include "mndUser.h"
#include "mnode.h"
#include "os.h"
#include "sdb.h"
#include "tdataformat.h"
#include "tglobal.h"
#include "tlog.h"
#include "trpc.h"
#include "ttimer.h"
#include "tutil.h"

#if defined(CUS_NAME) || defined(CUS_PROMPT) || defined(CUS_EMAIL)
#include "cus_name.h"
#endif

#define COMPARE_SET_VAL(a, b, _comp_sign_) \
  do {                                     \
    if ((a)_comp_sign_(b)) {               \
      (a) = (b);                           \
    }                                      \
  } while (0)

#define GRANT_ITEM_SET_VAL(v1, v2, _max_val_) \
  do {                                        \
    if ((v1) != (_max_val_)) {                \
      if ((v2) == (_max_val_)) {              \
        (v1) = (_max_val_);                   \
      } else if ((v1) < (v2)) {               \
        (v1) = (v2);                          \
      }                                       \
    }                                         \
  } while (0)

#define GRANT_ITEM_COMPARE(v1, v2, _max_val_) \
  do {                                        \
    if ((v1) == (_max_val_)) {                \
      if ((v2) != (_max_val_)) {              \
        return 1;                             \
      }                                       \
    } else if ((v2) == (_max_val_)) {         \
      result = -1;                            \
    } else if ((v1) > (v2)) {                 \
      return 1;                               \
    } else if ((v1) < (v2)) {                 \
      result = -1;                            \
    }                                         \
  } while (0)

#define GRANT_ITEMS_INIT(pItems)                     \
  do {                                               \
    pItems[0].number = GRANT_CONN_NUM_UNDEF;         \
    pItems[0].speed = GRANT_CONN_SPEED_UNDEF;        \
    pItems[0].expire = GRANT_CONN_EXPIRE_UNDEF;      \
    for (int32_t i = 1; i < CONN_TYPE_MAX_V1; ++i) { \
      *(pItems + i) = *(pItems + 0);                 \
    }                                                \
  } while (0)

#define GRANT_EXPIRE_SHOW(expireSec)                   \
  do {                                                 \
    ++cols;                                            \
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols); \
    if ((expireSec) != GRANT_UNIQ_MAX_EXPIRE_SECOND) { \
      grantSecondsToString((expireSec), ts);           \
      src = ts;                                        \
    } else {                                           \
      src = "unlimited";                               \
    }                                                  \
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));    \
    colDataSetVal(pColInfo, numOfRows, tmp, false);    \
  } while (0)

#define GRANT_ITEM_SHOW(cur, limit, unit)                         \
  do {                                                            \
    ++cols;                                                       \
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);            \
    if ((limit) != GRANT_UNIQ_UNLIMITED) {                        \
      if ((unit) <= 32) {                                         \
        sprintf(tmp1, "%d/%d", (int32_t)(cur), (int32_t)(limit)); \
      } else {                                                    \
        sprintf(tmp1, "%" PRIi64 "/%" PRIi64, (cur), (limit));    \
      }                                                           \
      src = tmp1;                                                 \
    } else {                                                      \
      src = "unlimited";                                          \
    }                                                             \
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));               \
    colDataSetVal(pColInfo, numOfRows, tmp, false);               \
  } while (0)

#define GRANT_DATA_IN_SHOW(appType, appStr)                                                                            \
  do {                                                                                                                 \
    ++cols;                                                                                                            \
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);                                                                 \
    pDataIn = GRANT_DATA_IN(&gStatus, (appType));                                                                      \
    grantSecondsToString((int64_t)pDataIn->expire * 86400, ts);                                                        \
    sprintf(tmp1,                                                                                                      \
            "{\"type\":\"%s\",\"number\":%d,\"speed\":%" PRIi16 ",\"expire\":\"%" PRIu16 "\", \"expireTime\":\"%s\"}", \
            (appStr), pDataIn->number, pDataIn->speed, pDataIn->expire, ts);                                           \
    STR_WITH_SIZE_TO_VARSTR(tmp, tmp1, strlen(tmp1));                                                                  \
    colDataSetVal(pColInfo, numOfRows, tmp, false);                                                                    \
  } while (0)

#ifndef GRANTS_CFG
#define GRANT_VERSION (gStatus.officialVersion ? "official" : "trial")
#define GRANT_EXPIRE (gStatus.basicExpireSec)
#define GRANT_EXPIRED(exp) (exp) ? TSDB_CODE_GRANT_EXPIRED : TSDB_CODE_SUCCESS
#define GRANT_EXPIRE_VAL (gStatus.basicExpired | (tsDiskCfgNum > 1 ? (gStatus.multiTierExpired << 1) : 0))
#else
#define GRANT_VERSION ("official")
#define GRANT_EXPIRE (GRANT_UNIQ_UNLIMITED)
#endif
#define GRANT_CONN_MAJOR_VER 1
#define GRANT_CONN_MINOR_VER 1
#define GRANT_FLAG_TDENGINE ((int8_t)0x01)
#define GRANT_FLAG_CONNECTORS ((int8_t)0x02)
#define GRANT_CONN_ITEMS(s) ((s)->connectors.items)
#define GRANT_CONN_ITEM(s, i) ((s)->connectors.items + i)
#define GRANT_CONN_OFFICIAL(s) ((s)->connectors.officialVersion)
#define SET_GRANT_TDENGINE(s) ((s)->flag |= GRANT_FLAG_TDENGINE)
#define SET_GRANT_CONNECTORS(s) ((s)->flag |= GRANT_FLAG_CONNECTORS)
#define SET_GRANT_CONNECTORS_OFFICIAL(s) (GRANT_CONN_OFFICIAL(s) = 1)
#define SET_GRANT_CONNECTORS_TRIAL(s) (GRANT_CONN_OFFICIAL(s) = 0)
#define IS_GRANT_TDENGINE(s) (((s)->flag & 0x01) == GRANT_FLAG_TDENGINE)
#define IS_GRANT_CONNECTORS(s) (((s)->flag & 0x02) == GRANT_FLAG_CONNECTORS)
#define IS_GRANT_CONNECTORS_OFFICIAL(s) GRANT_CONN_OFFICIAL(s)
#define GRANT_GET_DIST(p, idx) (((SGrantDistInfo *)TARRAY_GET_ELEM((p), (idx)))->dist)
#define GRANT_CONN_DIST(p, idx) (((SGrantDistInfo *)TARRAY_GET_ELEM((p), (idx)))->connDist)
// uniq grant
#define GRANT_DATA_IN(s, i) ((s)->ins + i)

#define GRANT_DIST_TOLERENCE 86400 // seconds

#define GRANT_TS_SEC_LEN 20

#ifndef min
#define min(x, y) (x) < (y) ? (x) : (y)
#endif
#if 1
extern void *tsMnodeTmr;
#endif

#ifdef GRANTS_CFG
#include "tgrantCfg.h"

typedef struct {
  bool          updateForced;
  int8_t        flag;  // version 2 since 3.0.5.0
  int32_t       dnodeId;
  uint64_t      limitTimeSeries;
  uint32_t      limitDbs;
  uint32_t      limitSTables;
  uint32_t      limitTables;
  uint32_t      distribute;                          // version 3 since 3.1.0.0
  char          active[GRANT_ACTIVE_KEY_LEN + 1];    // version 3 since 3.1.0.0
  char          machine[GRANT_MACHINE_KEY_LEN + 1];  // version 4 since 3.1.1.7
  SGrantConnMsg connectors;                          // version 2 since 3.0.5.0
} SCloudGrantMsg;

typedef struct {
  uint64_t      curTimeSeries;
  uint64_t      limitTimeSeries;
  int8_t        flag;       // version 2 since 3.0.5.0
  uint32_t      lastCheck;  // version 2 since 3.0.5.0
  uint32_t      curDbs;
  uint32_t      limitDbs;
  uint32_t      curSTables;
  uint32_t      limitSTables;
  uint32_t      curTables;
  uint32_t      limitTables;
  SGrantConnMsg connectors;  // version 2 since 3.0.5.0
} SCloudGrantStatus;

SCloudGrantStatus cloudGrantStatus = {.curTimeSeries = 0,
                                      .limitTimeSeries = GRANT_TIME_SERIES_LIMITS,
                                      .flag = 0,
                                      .lastCheck = 0,
                                      .curDbs = 0,
                                      .limitDbs = GRANT_DATABASE_LIMITS,
                                      .curSTables = 0,
                                      .limitSTables = GRANT_STABLE_LIMITS,
                                      .curTables = 0,
                                      .limitTables = GRANT_TABLE_LIMITS,
                                      .connectors.majorVer = GRANT_CONN_MAJOR_VER,
                                      .connectors.minorVer = GRANT_CONN_MINOR_VER,
                                      .connectors.officialVersion = 0};

GRANT_CFG_EXTERN;
typedef SCloudGrantStatus GrantStatus;
typedef SCloudGrantMsg    GrantMsg;
#else
SGrantStatus     grantStatus = {0};
typedef SGrantUniqStatus GrantStatus;
typedef SGrantMsg    GrantMsg;
#endif

SGrantUniqStatus grantUniqStatus = {
    .basicExpireSec = GRANT_UNIQ_UNLIMITED,
    .limitDnodes = GRANT_UNIQ_UNLIMITED,
    .limitTimeSeries = GRANT_UNIQ_UNLIMITED,
    .limitCpuCores = GRANT_UNIQ_UNLIMITED,
    .limitStreams = GRANT_UNIQ_UNLIMITED,
    .limitTopics = GRANT_UNIQ_UNLIMITED,
    .streamExpireSec = GRANT_UNIQ_UNLIMITED,
    .topicExpireSec = GRANT_UNIQ_UNLIMITED,
    .multiTierExpireSec = GRANT_UNIQ_UNLIMITED,
    .auditExpireSec = GRANT_UNIQ_UNLIMITED,
    .bakRstExpireSec = GRANT_UNIQ_UNLIMITED,
    .replicaExpireSec = GRANT_UNIQ_UNLIMITED,
};

typedef SGrantNotify GrantNotify;

extern SGrantUniqObj grantObj;
extern int32_t       grantMachineVer;
extern char          tsVersionName[16];
extern int64_t       tsExpireTime;

static int32_t  grantSecondsToString(int64_t seconds, char *ts);
static void     dmRefreshGrantCfg();
static void     grantRetrieveGrantInfo(SMnode *pMnode);
static void     grantResetMaster(SMnode *pMnode);
static void     grantSetClusterInfo(SMnode *pMnode);
static void     grantConnStatusCheck(SMnode *pMnode, uint32_t curTime, SDnodeInfo *pDnodeInfo);
static int64_t  grantGetClusterCreateTime(SMnode *pMnode);
static int32_t  mndProcessGrantHB(SRpcMsg *pReq);
static int32_t  mndProcessGrantRsp(SRpcMsg *pRsp);
static int32_t  dmGenerateGrantMsg(GrantMsg *pGrant, GrantStatus *pGrantStatus, SDnodeInfo *pInfo, int64_t clusterTime);
static int32_t  mndSetActiveCodeFromCfg(SDnodeInfo *pDnodeInfo, GrantMsg *pMsg);
static int32_t  mndProcessDnodeSGrantMsg(SMnode *pMnode, SDnodeInfo *pDnodeInfo, GrantMsg *pGrantMsg,
                                         GrantStatus *pGrantStatus);
static int32_t  tSerializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, SDnodeInfo *pInfo,
                                      int64_t clusterTime);
static int32_t  tDeserializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, SDnodeInfo *pInfo,
                                        int64_t *clusterTime);
static int32_t  tSerializeGrantMsg(void *buf, int32_t bufLen, GrantMsg *pMsg);
static int32_t  tDeserializeGrantMsg(void *buf, int32_t bufLen, GrantMsg *pMsg);
static int32_t  tSerializeGrantNotify(void *buf, int32_t bufLen, GrantNotify *pNotify);
static int32_t  tDeserializeGrantNotify(void *buf, int32_t bufLen, GrantNotify *pNotify);
static int64_t  grantGetClusterCurTimeSeries(SMnode *pMnode);
static void     grantStatusCheck(SMnode *pMnode, uint32_t curTime, SDnodeInfo *pDnodeInfo);

static int32_t mndRetrieveGrant(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextGrant(SMnode *pMnode, void *pIter);

// connectors
static void    tGrantConnItemsInit(SGrantConnItem *pItems, int32_t nItem);
static int32_t tGrantConnItemsNum(int8_t version);
static int32_t tSerializeGrantConnMsg(SEncoder *encoder, SGrantConnMsg *pMsg);
static int32_t tDeserializeGrantConnMsg(SDecoder *decoder, SGrantConnMsg *pMsg);

typedef struct {
  uint32_t dist;
  uint32_t connDist;
  int32_t  dnodeId;
} SGrantDistInfo;

typedef struct {
  SHashObj    *pOfficials;
  SHashObj    *pMachines;
  SArray      *pDistInfo;
  SArray      *pDnodeInfo;
  SMnode      *pMnode;
  SGrantedInfo grantedInfo;
  int32_t      nGrantReq;
  int32_t      nGrantRsp;
  int64_t      lastCheck;
  int8_t       nGrantNone;
} SGrantHandle;

static bool    recheckClusterTime = true;
static int8_t  grantHbLock = 0;
static int64_t grantNotifyCnt = 0;
static int64_t grantNotifyTimeSeries = INT64_MAX;
static int64_t grantClusterEpoch = 0;
int32_t        grantFlag = 0;
SGrantHandle   grantHandle = {0};

// extern SSysTableMeta infosMeta[];
#ifdef GRANTS_CFG
#define gStatus cloudGrantStatus
#else
#define gStatus grantUniqStatus
#endif

int32_t mndInitGrant(SMnode *pMnode) {
  terrno = 0;
  tsGrantHBInterval = GRANT_HEART_BEAT_MIN;
#ifdef GRANTS_CFG
  grantFlag |= (int32_t)GRANT_EDITION_CLOUD;
#endif

  mndSetMsgHandle(pMnode, TDMT_MND_GRANT_HB_TIMER, mndProcessGrantHB);
  mndSetMsgHandle(pMnode, TDMT_MND_GRANT_RSP, mndProcessGrantRsp);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndRetrieveGrant);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndCancelGetNextGrant);
  grantSetClusterInfo(pMnode);
  if (!(grantHandle.pOfficials = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UINT), true, true))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  if (!(grantHandle.pMachines = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UINT), true, true))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  if (!(grantHandle.pDistInfo = taosArrayInit(0, sizeof(SGrantDistInfo)))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  if (!(grantHandle.pDnodeInfo = taosArrayInit(0, sizeof(SDnodeInfo)))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  grantHandle.pMnode = pMnode;

_exit:
  if (terrno != 0) {
    uError("grant data initialize failed since %s", tstrerror(terrno));
    mndCleanupGrant();
  } else {
    uDebug("grant data is initialized");
  }

  return terrno;
}

void mndCleanupGrant() {
  taosHashCleanup(grantHandle.pOfficials);
  taosHashCleanup(grantHandle.pMachines);
  taosArrayDestroy(grantHandle.pDistInfo);
  taosArrayDestroy(grantHandle.pDnodeInfo);
  grantHandle.pOfficials = NULL;
  grantHandle.pMachines = NULL;
  grantHandle.pDistInfo = NULL;
  grantHandle.pDnodeInfo = NULL;
  grantHandle.pMnode = NULL;
}

static int64_t grantGetExpireSec(int64_t expireSec) {
  if (expireSec >= 0) {
    return expireSec;
  }
  if (expireSec == GRANT_UNIQ_UNLIMITED) {
    return GRANT_UNIQ_MAX_EXPIRE_SECOND;
  }
  if (expireSec == GRANT_UNIQ_UNDEFINED) {
    return expireSec = grantClusterEpoch + GRANT_DEFAULT;
  }
  ASSERTS(0, "invalid expireSec:%" PRIi64, expireSec);
  return expireSec = grantClusterEpoch + GRANT_DEFAULT;
}

static void grantSetClusterInfo(SMnode *pMnode) {
  if (strncmp(tsVersionName, GRANT_VERSION, 16) != 0) {
    strncpy(tsVersionName, GRANT_VERSION, 16);
  }
  int64_t expireSec = grantGetExpireSec(GRANT_EXPIRE);
  COMPARE_SET_VAL(tsExpireTime, expireSec * 1000, !=);
  COMPARE_SET_VAL(pMnode->grant.expireTimeMS, tsExpireTime, !=);
  COMPARE_SET_VAL(pMnode->grant.timeseriesAllowed, (int64_t)gStatus.limitTimeSeries, !=);
}

static FORCE_INLINE void grantSetClusterIdEx(int64_t clusterId) {
  if (grantObj.clusterId[0] == 0) {
    if (clusterId > 0) {
      snprintf(grantObj.clusterId, GRANT_CLUSTER_ID_LEN + 1, "%" PRIi64, clusterId);
    }
  }
}

static FORCE_INLINE void grantSetClusterId(SMnode *pMnode) {
  if (grantObj.clusterId[0] == 0) {
    int64_t clusterId = mndGetClusterId(pMnode);
    if (clusterId > 0) {
      snprintf(grantObj.clusterId, GRANT_CLUSTER_ID_LEN + 1, "%" PRIi64, clusterId);
    }
  }
}

static void grantSetActiveCodes(SDnodeInfo *pInfo, SGrantObj *pObj, SGrantConnObj *pConnObj) {
  if (0 != pInfo->active[0] && pObj) {
    tstrncpy(pObj->active, pInfo->active, GRANT_ACTIVE_KEY_LEN + 1);
  }
  if (0 != pInfo->connActive[0] && pConnObj) {
    tstrncpy(pConnObj->active, pInfo->connActive, GRANT_CONN_ACTIVE_KEY_LEN + 1);
  }
}

int32_t dmProcessGrantNotify(void *pInfo, SRpcMsg *pMsg) {
  terrno = 0;
  if (!pMsg->pCont || (pMsg->contLen <= 0)) {
    terrno = TSDB_CODE_INVALID_MSG;
    uWarn("failed to process grant notify in dnode since msg is empty");
    goto _err;
  }
  // step 1: process grant status from mnode
  SGrantNotify grantNotify = {0};
  if (tDeserializeGrantNotify(pMsg->pCont, pMsg->contLen, &grantNotify) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    uWarn("failed to process grant notify in dnode since %s", terrstr());
    goto _err;
  }

  gStatus.curTimeSeries = grantNotify.curTimeSeries;


  return TSDB_CODE_SUCCESS;
_err:
  pMsg->code = terrno;
  pMsg->info.rsp = NULL;
  pMsg->info.rspLen = 0;

  uWarn("failed to process grant notify and send rsp in dnode since %s", tstrerror(terrno));

  return TSDB_CODE_FAILED;
}

/**
 * @brief process grant status msg in dnode and respond with grant msg
 *
 * @param pInfo
 * @param pMsg
 * @return int32_t
 */
int32_t dmProcessGrantReq(void *pInfo, SRpcMsg *pMsg) {
  terrno = 0;
  if (!pMsg->pCont || (pMsg->contLen <= 0)) {
    terrno = TSDB_CODE_INVALID_MSG;
    uWarn("failed to process grant req in dnode since msg is empty");
    goto _err;
  }
  // step 1: process grant status from mnode
  GrantStatus grantStatusReq = {0};
  SDnodeInfo  dnodeInfo = {0};
  int64_t     clusterTime = 0;
  if (tDeserializeGrantStatus(pMsg->pCont, pMsg->contLen, &grantStatusReq, &dnodeInfo, &clusterTime) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    uWarn("failed to process grant req in dnode since %s", terrstr());
    goto _err;
  }

  // step 2: set local dnode grant status
#ifdef GRANTS_CFG
  cloudGrantStatus.curTimeSeries = grantStatusReq.curTimeSeries;
  cloudGrantStatus.curDbs = grantStatusReq.curDbs;
  cloudGrantStatus.curSTables = grantStatusReq.curSTables;
  cloudGrantStatus.curTables = grantStatusReq.curTables;
  // connectors
  cloudGrantStatus.connectors = grantStatusReq.connectors;
#else
  gStatus = grantStatusReq;  // assign directly
  int8_t grantExpireVal = GRANT_EXPIRE_VAL;
  if (grantExpireVal == 0) {
    atomic_val_compare_exchange_8(&tsGrant, 0, 1);
  } else {
    atomic_store_8(&tsGrant, 0);
  }
#endif

  // step 3: respond with grant msg
  grantSetClusterIdEx(*(int64_t *)pInfo);
  GrantMsg grantMsg = {.connectors.majorVer = GRANT_CONN_MAJOR_VER, .connectors.minorVer = GRANT_CONN_MINOR_VER};
  dmGenerateGrantMsg(&grantMsg, &grantStatusReq, &dnodeInfo, clusterTime);
  int32_t contLen = tSerializeGrantMsg(NULL, 0, &grantMsg);
  void   *pCont = rpcMallocCont(contLen);
  if (!pCont) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  tSerializeGrantMsg(pCont, contLen, &grantMsg);

  pMsg->code = 0;
  pMsg->info.rsp = pCont;
  pMsg->info.rspLen = contLen;

  uDebug("succeed to process grant req and send rsp in dnode");

  return TSDB_CODE_SUCCESS;
_err:
  pMsg->code = terrno;
  pMsg->info.rsp = NULL;
  pMsg->info.rspLen = 0;

  uWarn("failed to process grant req and send rsp in dnode since %s", tstrerror(terrno));

  return TSDB_CODE_FAILED;
}

static void dmRefreshGrantCfg(SGrantObj *pObj, SGrantConnObj *pConnObj) {
  char cfgFile[PATH_MAX] = {0};
#ifdef CUS_PROMPT
  sprintf(cfgFile, "%s/%s.cfg", configDir, CUS_PROMPT);
#else
  sprintf(cfgFile, "%s/taos.cfg", configDir);
#endif
  grantActiveSystem(cfgFile, pObj, pConnObj);
}

static int32_t dmGenerateGrantMsg(GrantMsg *pGrantMsg, GrantStatus *pGrantStatus, SDnodeInfo *pInfo,
                                  int64_t clusterTime) {
  SGrantObj     grantObj = {0};
  SGrantConnObj grantConnObj = {0};
  grantSetActiveCodes(pInfo, &grantObj, &grantConnObj);
  // refresh
  dmRefreshGrantCfg(&grantObj, &grantConnObj);
#ifdef GRANTS_CFG
  pGrantMsg->updateForced = tsGrantUpdateForced;
  tsGrantUpdateForced = false;
  if (pGrantMsg->updateForced) {
    cloudGrantStatus.limitTimeSeries = tsGrantLimitTimeSeries;
    cloudGrantStatus.limitDbs = tsGrantLimitDbs;
    cloudGrantStatus.limitSTables = tsGrantLimitSTables;
    cloudGrantStatus.limitTables = tsGrantLimitTables;
  } else {
    if (cloudGrantStatus.limitTimeSeries == GRANT_TIME_SERIES_LIMITS) {
      cloudGrantStatus.limitTimeSeries = pGrantStatus->limitTimeSeries;
    } else {
      COMPARE_SET_VAL(cloudGrantStatus.limitTimeSeries, pGrantStatus->limitTimeSeries, <);
    }
    if (cloudGrantStatus.limitDbs == GRANT_DATABASE_LIMITS) {
      cloudGrantStatus.limitDbs = pGrantStatus->limitDbs;
    } else {
      COMPARE_SET_VAL(cloudGrantStatus.limitDbs, pGrantStatus->limitDbs, <);
    }
    if (cloudGrantStatus.limitSTables == GRANT_STABLE_LIMITS) {
      cloudGrantStatus.limitSTables = pGrantStatus->limitSTables;
    } else {
      COMPARE_SET_VAL(cloudGrantStatus.limitSTables, pGrantStatus->limitSTables, <);
    }
    if (cloudGrantStatus.limitTables == GRANT_TABLE_LIMITS) {
      cloudGrantStatus.limitTables = pGrantStatus->limitTables;
    } else {
      COMPARE_SET_VAL(cloudGrantStatus.limitTables, pGrantStatus->limitTables, <);
    }

    tsGrantLimitTimeSeries = cloudGrantStatus.limitTimeSeries;
    tsGrantLimitDbs = cloudGrantStatus.limitDbs;
    tsGrantLimitSTables = cloudGrantStatus.limitSTables;
    tsGrantLimitTables = cloudGrantStatus.limitTables;
  }

  uInfo("dnode send grant message,timeseries:%" PRIu64 ", database:%u, stable:%u, table:%u, set to grant state",
        cloudGrantStatus.limitTimeSeries, cloudGrantStatus.limitDbs, cloudGrantStatus.limitSTables,
        cloudGrantStatus.limitTables);
  pGrantMsg->limitTimeSeries = cloudGrantStatus.limitTimeSeries;
  pGrantMsg->limitDbs = cloudGrantStatus.limitDbs;
  pGrantMsg->limitSTables = cloudGrantStatus.limitSTables;
  pGrantMsg->limitTables = cloudGrantStatus.limitTables;
  SET_GRANT_TDENGINE(pGrantMsg);
#else

  if (grantObj.granted) {
    int64_t tolerence = taosGetTimestampMs() / 1000 + GRANT_CHK_TOLERENCE;
    if (clusterTime > tolerence) {
      grantObj.granted = false;
      uWarn("failed to grant since time out of sync: cluster %" PRIi64 " > %" PRIi64, clusterTime, tolerence);
    } else {
      int64_t grantCurrent = GRANT_CUR_TIME;
      if (grantCurrent > tolerence) {
        grantObj.granted = false;
        uWarn("failed to grant since time out of sync: grant %" PRIi64 " > %" PRIi64, grantCurrent, tolerence);
      } else {
        uDebug("continue to grant since time in sync: cluster,grant %" PRIi64 ",%" PRIi64 "  < %" PRIi64, clusterTime,
              grantCurrent, tolerence);
      }
    }
  } else {
    uDebug("failed to grant since active granted is false");
  }

  if (grantObj.granted) {
    SET_GRANT_TDENGINE(pGrantMsg);
    pGrantMsg->usbDongle = grantObj.usbDongle;
    pGrantMsg->updateForced = grantObj.updateForced;
    pGrantMsg->officialVersion = grantObj.officialVersion;
    pGrantMsg->expireTimeSec = grantObj.expireTimeSec;
    pGrantMsg->limitStorage = grantObj.limitStorage;  // GB
    pGrantMsg->limitSpeed = grantObj.limitSpeed;
    pGrantMsg->limitTimeSeries = grantObj.limitTimeSeries;
    pGrantMsg->limitQueryTime = grantObj.limitQueryTime;
    pGrantMsg->limitDbs = grantObj.limitDbs;
    pGrantMsg->limitUsers = grantObj.limitUsers;
    pGrantMsg->limitConns = grantObj.limitConns;
    pGrantMsg->limitStreams = grantObj.limitStreams;
    pGrantMsg->limitAccts = grantObj.limitAccts;
    pGrantMsg->limitDnodes = grantObj.limitDnodes;
    pGrantMsg->limitCpuCores = grantObj.limitCpuCores;
    pGrantMsg->reserveKey1 = grantObj.reserveKey1;
    pGrantMsg->reserveKey2 = grantObj.reserveKey2;
  }
#endif
  if (grantConnObj.granted) {
    SET_GRANT_CONNECTORS(pGrantMsg);
    SGrantConnMsg *pConn = &pGrantMsg->connectors;
    pConn->officialVersion = grantConnObj.officialVersion;
    memcpy(pConn->items, grantConnObj.items, sizeof(SGrantConnItem) * CONN_TYPE_MAX_V1);
    pGrantMsg->connectors.distribute = grantConnObj.distribute;
  }

  // fetch the activeCodes in taos.cfg if not set in sdb/dnode
#ifndef GRANTS_CFG
  if (pInfo->active[0] == 0 && grantObj.active[0] != 0) {
    strncpy(pGrantMsg->active, grantObj.active, GRANT_ACTIVE_KEY_LEN + 1);
  }
#endif

  if (pInfo->connActive[0] == 0 && grantConnObj.active[0] != 0) {
    strncpy(pGrantMsg->connectors.active, grantConnObj.active, GRANT_CONN_ACTIVE_KEY_LEN + 1);
  }

  // assign machine for activeCode checking in mnode leader
  strncpy(pGrantMsg->machine, grantObj.machine, GRANT_MACHINE_KEY_LEN + 1);

  pGrantMsg->dnodeId = pInfo->id;

  return TSDB_CODE_SUCCESS;
}

static void grantConnActiveFillUndef(SMnode *pMnode, SGrantConnItem *pItems) {
  if (grantClusterEpoch <= 0) {
    grantClusterEpoch = grantGetClusterCreateTime(pMnode);
  }

  SGrantConnItem defaultItem = {.number = GRANT_CONN_NUM_DEFAULT,
                                .speed = GRANT_CONN_SPEED_DEFAULT,
                                .expire = ceil((double)grantClusterEpoch / 86400) + GRANT_CONN_EXPIRE_DEFAULT};

  for (int32_t i = 0; i < CONN_TYPE_MAX_V1; ++i) {
    SGrantConnItem *pItem = pItems + i;
    if (GRANT_CONN_ITEM_UNDEF(pItem)) {
      *pItem = defaultItem;
    }
  }
}

/**
 * @brief 1) send grant status to dnode in async mode
 * @param pMnode
 * @param pDnodeInfo
 * @param clusterTime
 * @return int32_t
 */
static int32_t mndSendGrantStatusToDnode(SMnode *pMnode, SDnodeInfo *pDnodeInfo, int64_t clusterTime) {
  // step 1: send grant status to dnode
  int32_t contLen = tSerializeGrantStatus(NULL, 0, &gStatus, pDnodeInfo, clusterTime);
  void   *pCont = rpcMallocCont(contLen);
  if (!pCont) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uWarn("failed to generate grant status msg since %s", terrstr());
    return TSDB_CODE_FAILED;
  }

  if (tSerializeGrantStatus(pCont, contLen, &gStatus, pDnodeInfo, clusterTime) < 0) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uWarn("failed to generate grant status msg when serialize since %s", terrstr());
    return TSDB_CODE_FAILED;
  }

  SRpcMsg rpcMsg = {.pCont = pCont, .contLen = contLen, .msgType = TDMT_MND_GRANT, .info.ahandle = (void *)0x818};

  uDebug("send grant status msg to dnode:%d %s:%" PRIu16, pDnodeInfo->id, pDnodeInfo->ep.fqdn, pDnodeInfo->ep.port);

  SEpSet epSet = {.numOfEps = 1};
  strncpy(epSet.eps[0].fqdn, pDnodeInfo->ep.fqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = pDnodeInfo->ep.port;

  if((terrno = tmsgSendReq(&epSet, &rpcMsg)) != 0){
    uWarn("failed to send grant status msg since %s", terrstr());
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
_err:
  return TSDB_CODE_FAILED;
}

static void mndProcessGrantStatusCheck() {
#ifdef GRANTS_CFG
  grantConnStatusCheck(grantHandle.pMnode, taosGetTimestampMs() / 1000, NULL);
#else
  grantStatusCheck(grantHandle.pMnode, taosGetTimestampMs() / 1000, NULL);
#endif

  bool minHbInterval = false;
  int8_t grantExpireVal = GRANT_EXPIRE_VAL;
  if (grantExpireVal == 0) {
    if (0 == atomic_val_compare_exchange_8(&tsGrant, 0, 1)) {
      minHbInterval = true;
    }
  } else if (0 != atomic_load_8(&tsGrant)) {
    minHbInterval = true;
  }

  if (atomic_val_compare_exchange_8(&grantHbLock, 2, 0) == 2) {
    minHbInterval = true;
  } else {
    atomic_store_8(&grantHbLock, 0);
  }

  if (minHbInterval) tsGrantHBInterval = GRANT_HEART_BEAT_MIN;
}

static int32_t dnodeInfoCmprFn(const void *p1, const void *p2) {
  SDnodeInfo *pInfo1 = (SDnodeInfo *)p1;
  SDnodeInfo *pInfo2 = (SDnodeInfo *)p2;

  if (pInfo1->id < pInfo2->id) {
    return -1;
  }
  return pInfo1->id != pInfo2->id ? 1 : 0;
}

/**
 * @brief 1) process response (grant msg) from dnode in async mode
 * @param pRsp
 * @return int32_t
 */
static int32_t mndProcessGrantRsp(SRpcMsg *pRsp) {
  int32_t code = 0;

  ++grantHandle.nGrantRsp;

  if (!pRsp->pCont || pRsp->contLen <= 0 || pRsp->code != 0) {
    code = pRsp->code != 0 ? pRsp->code : TSDB_CODE_INVALID_MSG_LEN;
    goto _exit;
  }

  GrantMsg grantMsgRsp = {0};
  if (tDeserializeGrantMsg(pRsp->pCont, pRsp->contLen, &grantMsgRsp) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  SDnodeInfo  dnodeInfo = {.id = grantMsgRsp.dnodeId};
  SDnodeInfo *pDnodeInfo = taosArraySearch(grantHandle.pDnodeInfo, &dnodeInfo, dnodeInfoCmprFn, TD_EQ);

  ASSERTS(pDnodeInfo, "pDnodeInfo is NULL for %d", grantMsgRsp.dnodeId);

  if (pDnodeInfo) {
    uDebug("succeed to receive grant msg from dnode:%d, %s:%" PRIu16 ", nReq:%d, nRsp:%d", grantMsgRsp.dnodeId,
           pDnodeInfo->ep.fqdn, pDnodeInfo->ep.port, grantHandle.nGrantReq, grantHandle.nGrantRsp);
    mndProcessDnodeSGrantMsg(grantHandle.pMnode, pDnodeInfo, &grantMsgRsp, &gStatus);
    mndSetActiveCodeFromCfg(pDnodeInfo, &grantMsgRsp);
  }

_exit:
  if (grantHandle.nGrantRsp >= grantHandle.nGrantReq) {
    mndProcessGrantStatusCheck();
  }
  return code;
}

static void grantCheckClusterInfo(SMnode *pMnode) {
  if (recheckClusterTime) {
    int64_t clusterCreateTime = grantGetClusterCreateTime(pMnode);
    if (clusterCreateTime > 0) {
      recheckClusterTime = false;
      COMPARE_SET_VAL(grantClusterEpoch, clusterCreateTime, !=);
    }
  }

  if (recheckClusterTime) {
    tsGrantHBInterval = GRANT_HEART_BEAT_MIN;
  } else if (tsGrantHBInterval != GRANT_HEART_BEAT_MSG) {
    tsGrantHBInterval = GRANT_HEART_BEAT_MSG;
  }
}

/**
 * @brief process grant heartbeat msg from mnode
 *
 * @param pReq
 * @return int32_t
 */
static int32_t mndProcessGrantHB(SRpcMsg *pReq) {
  if (0 != atomic_val_compare_exchange_8(&grantHbLock, 0, 1)) {
    uWarn("previous grant task not finished yet");
    atomic_val_compare_exchange_8(&grantHbLock, 1, 2);
    // in case not enough grant responses are received for a long time
    if (taosGetTimestampMs() - grantHandle.lastCheck > 15000) {
      mndProcessGrantStatusCheck();
    }
    return 0;
  }

  SMnode *pMnode = pReq->info.node;

  grantCheckClusterInfo(pMnode);

  grantRetrieveGrantInfo(pMnode);

  char active[GRANT_UNIQ_ACTIVE_KEY_LEN + 1] = "\0";
  mndGetClusterActive(pMnode, active);
  if (active[0] != 0) {
    grantUniqParseActiveCode(&grantObj, NULL);
  }

  grantSetClusterInfo(pMnode);

  // reset grantHandle
  taosHashClear(grantHandle.pOfficials);
  taosArrayClear(grantHandle.pDistInfo);
  taosArrayClear(grantHandle.pDnodeInfo);
  grantHandle.nGrantReq = 0;
  grantHandle.nGrantRsp = 0;

  mndGetDnodeData(pMnode, grantHandle.pDnodeInfo);

  int32_t dnodeSize = taosArrayGetSize(grantHandle.pDnodeInfo);

  if (dnodeSize > 1) {
    taosArraySort(grantHandle.pDnodeInfo, dnodeInfoCmprFn);
  }

  int64_t clusterTime = grantGetClusterCreateTime(pMnode) + mndGetClusterUpTime(pMnode);
  for (int32_t i = 0; i < dnodeSize; ++i) {
    SDnodeInfo *info = (SDnodeInfo *)TARRAY_GET_ELEM(grantHandle.pDnodeInfo, i);
    if (info->offlineReason == DND_REASON_STATUS_MSG_TIMEOUT || info->offlineReason == DND_REASON_STATUS_NOT_RECEIVED) {
      uDebug("not send grant status to dnode:%d since offline state:%d", info->id, info->offlineReason);
      continue;
    }
    if (0 == mndSendGrantStatusToDnode(pMnode, info, clusterTime)) {
      ++grantHandle.nGrantReq;
    }
  }

  grantHandle.lastCheck = taosGetTimestampMs();

  // tolerence for exception
  if (grantHandle.nGrantReq <= 0) {
    if (++grantHandle.nGrantNone > 5) {  
      grantHandle.nGrantNone = 0;
      mndProcessGrantStatusCheck();
    } else {
      atomic_store_8(&grantHbLock, 0);
    }
  }

  return 0;
}

/**
 * @brief process grant heartbeat msg from mnode
 *
 * @param pReq
 * @return int32_t
 */
static int32_t mndProcessGrantHBOld(SRpcMsg *pReq) {
  if (0 != atomic_val_compare_exchange_8(&grantHbLock, 0, 1)) {
    uWarn("previous grant task not finished yet");
    atomic_val_compare_exchange_8(&grantHbLock, 1, 2);
    return 0;
  }

  SMnode *pMnode = pReq->info.node;

  grantCheckClusterInfo(pMnode);

  grantRetrieveGrantInfo(pMnode);

  // reset grantHandle
  taosHashClear(grantHandle.pOfficials);
  taosArrayClear(grantHandle.pDistInfo);
  taosArrayClear(grantHandle.pDnodeInfo);
  grantHandle.nGrantReq = 0;
  grantHandle.nGrantRsp = 0;

  mndGetDnodeData(pMnode, grantHandle.pDnodeInfo);

  int32_t dnodeSize = taosArrayGetSize(grantHandle.pDnodeInfo);

  if (dnodeSize > 1) {
    taosArraySort(grantHandle.pDnodeInfo, dnodeInfoCmprFn);
  }

  int64_t clusterTime = grantGetClusterCreateTime(pMnode) + mndGetClusterUpTime(pMnode);
  for (int32_t i = 0; i < dnodeSize; ++i) {
    SDnodeInfo *info = (SDnodeInfo *)TARRAY_GET_ELEM(grantHandle.pDnodeInfo, i);
    if (info->offlineReason == DND_REASON_STATUS_MSG_TIMEOUT || info->offlineReason == DND_REASON_STATUS_NOT_RECEIVED) {
      uDebug("not send grant status to dnode:%d since offline state:%d", info->id, info->offlineReason);
      continue;
    }
    if (0 == mndSendGrantStatusToDnode(pMnode, info, clusterTime)) {
      ++grantHandle.nGrantReq;
    }
  }

  // tolerence for exception
  if (grantHandle.nGrantReq <= 0) {
    if (++grantHandle.nGrantNone > 5) {  
      grantHandle.nGrantNone = 0;
      mndProcessGrantStatusCheck();
    } else {
      atomic_store_8(&grantHbLock, 0);
    }
  }

  return 0;
}

void grantParseParameter() {
#ifdef _TD_MIPS
  fprintf(stderr, "the MIPS platform does not support machine code currently!\n");
#else
  char *key = grantGetMachineSerials();
  if (key != NULL) {
    fprintf(stdout, "machine code: %s \n", key);
  } else {
    fprintf(stderr, "should generate machine code under root authority!\n");
  }
#endif
  exit(EXIT_SUCCESS);
}

void tGetMachineId() {
#ifdef _TD_MIPS
  uWarn(the MIPS platform does not support machine code currently!)
#else
  char *key = grantGetMachineSerials();
  if (key != NULL) {
    fprintf(stdout, "machine code: %s \n", key);
  } else {
    fprintf(stderr, "should generate machine code under root authority!\n");
  }
#endif
  exit(EXIT_SUCCESS);
}

static int32_t grantSecondsToString(int64_t seconds, char *ts) {
  time_t    sec = seconds;
  struct tm ptm;
  if (taosLocalTime(&sec, &ptm, ts) != NULL) {
    strftime(ts, GRANT_TS_SEC_LEN, "%Y-%m-%d %H:%M:%S", &ptm);
    return 0;
  }
  return -1;
}

static int64_t grantGetClusterCreateTime(SMnode *pMnode) {
  int64_t createTime = mndGetClusterCreateTime(pMnode);
  return createTime / 1000;
}

static uint32_t grantGetClusterCurSpeed() { return 0; }

/**
 * @brief  numOfColumns: stable + ctable + ntable in all master vnodes, not including Primary TS Key column, not
 * including tsma dstVg
 *
 * @return int64_t
 */
static int64_t grantGetClusterCurTimeSeries(SMnode *pMnode) {
  int64_t numOfPoints = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;

  while ((pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup))) {
    if (!pVgroup->isTsma) {
      numOfPoints += pVgroup->numOfTimeSeries;
    }
    sdbRelease(pSdb, pVgroup);
  }

  return numOfPoints;
}

/**
 * @brief not including tsma storage
 *
 * @param pMnode
 * @return uint64_t
 */
static uint64_t grantGetClusterCurStorage(SMnode *pMnode) {
  uint64_t storage = 0;
  SSdb    *pSdb = pMnode->pSdb;
  SVgObj  *pVgroup = NULL;
  void    *pIter = NULL;

  while ((pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup))) {
    if (!pVgroup->isTsma) {
      storage += pVgroup->compStorage;
    }
    sdbRelease(pSdb, pVgroup);
  }

  return storage;
}

static uint32_t grantGetClusterCurQueryTime() { return 0; }

static uint32_t grantGetClusterCurDbs(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  // 2 built-in system DB not included
  return (uint32_t)(sdbGetSize(pSdb, SDB_DB));
}

/**
 * @brief Not including the built-in user root
 *
 * @param pMnode
 * @return uint32_t
 */
static uint32_t grantGetClusterCurUsers(SMnode *pMnode) {
  SSdb     *pSdb = pMnode->pSdb;
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  uint32_t  numOfUsers = 0;

  while ((pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser))) {
    if (strcmp(pUser->user, "root") != 0) {
      ++numOfUsers;
    }
    sdbRelease(pSdb, pUser);
  }

  return numOfUsers;
}

static uint32_t grantGetClusterCurAccts(SMnode *pMnode) {
  SSdb     *pSdb = pMnode->pSdb;
  void     *pIter = NULL;
  SAcctObj *pAcct = NULL;
  uint32_t  numOfAccts = 0;
  while ((pIter = sdbFetch(pSdb, SDB_ACCT, pIter, (void **)&pAcct))) {
    if (strcmp(pAcct->acct, "root") != 0) {
      ++numOfAccts;
    }
    sdbRelease(pSdb, pAcct);
  }

  return numOfAccts;
}

static int32_t grantGetClusterCurDnodes(SMnode *pMnode) { return mndGetDnodeSize(pMnode); }

static uint32_t grantGetClusterCurSTables(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  return (uint32_t)sdbGetSize(pSdb, SDB_STB);
}

static uint32_t grantGetClusterCurTables(SMnode *pMnode) {
  uint64_t numOfPoints = 0;
  SSdb    *pSdb = pMnode->pSdb;
  SVgObj  *pVgroup = NULL;
  void    *pIter = NULL;

  while ((pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup))) {
    if (!pVgroup->isTsma) {
      numOfPoints += pVgroup->numOfTables;
    }
    sdbRelease(pSdb, pVgroup);
  }

  return numOfPoints;
}

static int32_t grantGetClusterCurCores(SMnode *pMnode) {
  SSdb      *pSdb = pMnode->pSdb;
  SDnodeObj *pDnode = NULL;
  void      *pIter = NULL;
  int32_t   numOfCores = 0;

  while ((pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode))) {
    numOfCores += (int32_t)pDnode->numOfCores;
    sdbRelease(pSdb, pDnode);
  }

  return numOfCores;
}

static int16_t grantGetClusterCurStreams(SMnode *pMnode) {
  SSdb       *pSdb = pMnode->pSdb;
  SStreamObj *pStream = NULL;
  void       *pIter = NULL;
  int16_t     numOfStreams = 0;

  while ((pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream))) {
    ++numOfStreams;
    sdbRelease(pSdb, pStream);
  }

  return numOfStreams;
}

static int16_t grantGetClusterCurTopics(SMnode *pMnode) {
  SSdb        *pSdb = pMnode->pSdb;
  SMqTopicObj *pTopic = NULL;
  void        *pIter = NULL;
  int16_t      numOfTopics = 0;

  while ((pIter = sdbFetch(pSdb, SDB_TOPIC, pIter, (void **)&pTopic))) {
    ++numOfTopics;
    sdbRelease(pSdb, pTopic);
  }

  return numOfTopics;
}

/**
 * @brief retrieve the statis info
 *
 * @param pMnode
 */
static void grantRetrieveGrantInfo(SMnode *pMnode) {
#ifdef GRANTS_CFG
  cloudGrantStatus.curTimeSeries = grantGetClusterCurTimeSeries(pMnode);
  cloudGrantStatus.curDbs = grantGetClusterCurDbs(pMnode);
  cloudGrantStatus.curSTables = grantGetClusterCurSTables(pMnode);
  cloudGrantStatus.curTables = grantGetClusterCurTables(pMnode);
#else
  gStatus.curTimeSeries = grantGetClusterCurTimeSeries(pMnode);
  gStatus.curDnodes = grantGetClusterCurDnodes(pMnode);
  gStatus.curCpuCores = grantGetClusterCurCores(pMnode);
  gStatus.curStreams = grantGetClusterCurStreams(pMnode);
  gStatus.curTopics = grantGetClusterCurTopics(pMnode);
#endif
}

static int32_t tSerializeGrantNotify(void *buf, int32_t bufLen, GrantNotify *pNotify) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  if (tEncodeU64(&encoder, pNotify->curTimeSeries) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

static int32_t mndSendGrantNotifyToDnode(SMnode *pMnode, SDnodeInfo *pDnodeInfo, SGrantNotify *pNotify) {
  int32_t contLen = tSerializeGrantNotify(NULL, 0, pNotify);
  void   *pCont = rpcMallocCont(contLen);
  if (!pCont) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uWarn("failed to generate grant notify msg since %s", terrstr());
    return TSDB_CODE_FAILED;
  }

  tSerializeGrantNotify(pCont, contLen, pNotify);

  SRpcMsg rpcMsg = {.pCont = pCont, .contLen = contLen, .msgType = TDMT_MND_GRANT_NOTIFY, .info.noResp = 1};

  uDebug("send grant notify msg to dnode:%d %s:%" PRIu16, pDnodeInfo->id, pDnodeInfo->ep.fqdn, pDnodeInfo->ep.port);

  SEpSet epSet = {.numOfEps = 1};
  strncpy(epSet.eps[0].fqdn, pDnodeInfo->ep.fqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = pDnodeInfo->ep.port;
  tmsgSendReq(&epSet, &rpcMsg);

  // rpcSendRequest(pMnode->msgCb.clientRpc, &epSet, &rpcMsg, NULL);

  return TSDB_CODE_SUCCESS;
}

static int32_t mndProcessGrantNotify(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  int32_t dnodeSize = mndGetDnodeSize(pMnode);
  SArray *pDnodeInfo = taosArrayInit(dnodeSize, sizeof(SDnodeInfo));
  if (!pDnodeInfo) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uWarn("failed to process grant notify msg since %s", terrstr());
    return -1;
  }

  mndGetDnodeData(pMnode, pDnodeInfo);

  int64_t notifyTimeSeries = atomic_load_64(&gStatus.curTimeSeries);
  atomic_store_64(&grantNotifyTimeSeries, notifyTimeSeries);

  SGrantNotify notify = {.curTimeSeries = notifyTimeSeries};
  for (int32_t i = 0; i < taosArrayGetSize(pDnodeInfo); ++i) {
    SDnodeInfo *info = (SDnodeInfo *)taosArrayGet(pDnodeInfo, i);
    mndSendGrantNotifyToDnode(pMnode, info, &notify);
  }

  taosArrayDestroy(pDnodeInfo);
  return 0;
}

int32_t mndUpdClusterInfo(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;

  gStatus.curTimeSeries = grantGetClusterCurTimeSeries(pMnode);

#ifndef GRANTS_CFG
  if (gStatus.curTimeSeries > gStatus.limitTimeSeries) {
    if ((atomic_fetch_add_64(&grantNotifyCnt, 1) & 127) < 3) {
      mndProcessGrantNotify(pReq);
    }
    if (grantNotifyCnt >= INT32_MAX) {
      atomic_store_64(&grantNotifyCnt, grantNotifyCnt & 127);
    }
  } else {
    if (atomic_load_64(&gStatus.curTimeSeries) < atomic_load_64(&grantNotifyTimeSeries)) {
      mndProcessGrantNotify(pReq);
    }
    if (grantNotifyCnt != 0) atomic_store_64(&grantNotifyCnt, 0);
  }
#endif

  return 0;
}

/**
 * @brief init the grant status after mnode startup
 *
 * @param pMnode
 */
static void grantResetMaster(SMnode *pMnode) {
  grantRetrieveGrantInfo(pMnode);
#ifndef GRANTS_CFG
  int64_t curTime = taosGetTimestampMs() / 1000;
  int64_t grantCurTime = TMAX(curTime, GRANT_CUR_TIME);
  int64_t clusterCreateTime = grantGetClusterCreateTime(pMnode);

  if (clusterCreateTime > 0) {
    COMPARE_SET_VAL(grantClusterEpoch, clusterCreateTime, !=);
    gStatus.basicExpireSec =
        clusterCreateTime <= grantCurTime ? (ceil((double)clusterCreateTime / 86400) * 86400 + GRANT_DEFAULT) : 0;
    gStatus.basicExpired = gStatus.basicExpireSec > grantCurTime ? false : true;
    
    gStatus.multiTierExpireSec = gStatus.basicExpireSec;
    gStatus.multiTierExpired = gStatus.basicExpired;
    gStatus.streamExpireSec = gStatus.basicExpireSec;
    gStatus.streamExpired = gStatus.basicExpired;
    gStatus.topicExpireSec = gStatus.basicExpireSec;
    gStatus.topicExpired = gStatus.basicExpired;
    gStatus.auditExpireSec = gStatus.basicExpireSec;
    gStatus.auditExpired = gStatus.basicExpired;

    gStatus.bakRstExpireSec = gStatus.basicExpireSec;
    gStatus.replicaExpireSec = gStatus.basicExpireSec;

    char ts[GRANT_TS_SEC_LEN] = {0};
    grantSecondsToString(gStatus.basicExpireSec, ts);
    uInfo("grant expire time reset to %s %u, current timeseries %" PRIu64, ts, gStatus.basicExpireSec,
          gStatus.curTimeSeries);
  }
#endif
  SGrantDataIns in = {.number = GRANT_UNIQ_DFT_DATAIN_NUM,
                      .speed = GRANT_UNIQ_DFT_DATAIN_SPEED,
                      .expire = ceil((double)clusterCreateTime / 86400) + GRANT_UNIQ_DFT_DATAIN_EXPIRE};
  for (int32_t i = 0; i < CONN_TYPE_MAX; ++i) {
    gStatus.ins[i] = in;
  }
}

void grantReset(SMnode *pMnode, EGrantType grant, uint64_t value) {
  switch (grant) {
    case TSDB_GRANT_ALL:
      grantResetMaster(pMnode);
      break;
    case TSDB_GRANT_STORAGE:
#ifdef GRANTS_RESERVE
      gStatus.curStorage = value;
#endif
      break;
    default:
      break;
  }
}

void grantAdd(EGrantType grant, uint64_t value) {
#if 0
  switch (grant) {
    case TSDB_GRANT_TIMESERIES:
      grantAddTimeSeries(value);
      break;
    case TSDB_GRANT_STORAGE:
      grantStatus.curStorage = value;
      break;
    default:
      break;
  }
#endif
}


void grantRestore(EGrantType grant, uint64_t value) {
#if 0
  switch (grant) {
    case TSDB_GRANT_TIMESERIES:
      grantRestoreTimeSeries(value);
      break;
    case TSDB_GRANT_STORAGE:
      grantStatus.curStorage = value;
      break;
    default:
      break;
  }
#endif
}

#ifdef GRANTS_CFG
static int32_t cloudGrantCheckTimeSeries() {
  if (cloudGrantStatus.limitTimeSeries == GRANT_TIME_SERIES_LIMITS ||
      cloudGrantStatus.curTimeSeries < cloudGrantStatus.limitTimeSeries) {
    return TSDB_CODE_SUCCESS;
  } else {
    uError("grant failed to create table, exist:%" PRIu64 ", reason:grant timeseries limited",
           cloudGrantStatus.curTimeSeries);
    return TSDB_CODE_GRANT_TIMESERIES_LIMITED;
  }
}
static int32_t cloudGrantCheckDatabases() {
  if (cloudGrantStatus.limitDbs == GRANT_DATABASE_LIMITS || cloudGrantStatus.curDbs < cloudGrantStatus.limitDbs) {
    return TSDB_CODE_SUCCESS;
  } else {
    uError("grant failed to create db, exist:%" PRIu32 ", reason:grant database limited", cloudGrantStatus.curDbs);
    return TSDB_CODE_GRANT_DB_LIMITED;
  }
}
static int32_t cloudGrantCheckSTables() {
  if (cloudGrantStatus.limitSTables == GRANT_STABLE_LIMITS ||
      cloudGrantStatus.curSTables < cloudGrantStatus.limitSTables) {
    return TSDB_CODE_SUCCESS;
  } else {
    uError("grant failed to create stable, exist:%" PRIu32 ", reason:grant stable limited",
           cloudGrantStatus.curSTables);
    return TSDB_CODE_GRANT_STABLE_LIMITED;
  }
}
static int32_t cloudGrantCheckTables() {
  if (cloudGrantStatus.limitTables == GRANT_TABLE_LIMITS || cloudGrantStatus.curTables < cloudGrantStatus.limitTables) {
    return TSDB_CODE_SUCCESS;
  } else {
    uError("grant failed to create table, exist:%" PRIu32 ", reason:grant table limited", cloudGrantStatus.curTables);
    return TSDB_CODE_GRANT_TABLE_LIMITED;
  }
}

#else

static int32_t grantCheckUsers() { return 0; }

static int32_t grantCheckDatabases() { return 0; }

static int32_t grantCheckTimeSeries() {
  ASSERTS(gStatus.limitTimeSeries != GRANT_UNIQ_UNDEFINED, "limitTimeSeries is %" PRIi64, GRANT_UNIQ_UNDEFINED);
  if (gStatus.limitTimeSeries == GRANT_UNIQ_UNLIMITED || gStatus.curTimeSeries < gStatus.limitTimeSeries) {
    return 0;
  }

  uError("grant failed to create table, exist:%" PRIu64 ", reason:grant timeseries limited", gStatus.curTimeSeries);
  return TSDB_CODE_GRANT_TIMESERIES_LIMITED;
}

static int32_t grantCheckAccts() { return 0; }

static int32_t grantCheckDnodes() {
  ASSERTS(gStatus.limitDnodes != GRANT_UNIQ_UNDEFINED, "limitDnodes is %" PRIi64, GRANT_UNIQ_UNDEFINED);
  if (gStatus.limitDnodes == GRANT_UNIQ_UNLIMITED) {
    return 0;
  }
  gStatus.curDnodes = grantGetClusterCurDnodes(grantHandle.pMnode);
  if (gStatus.curDnodes < gStatus.limitDnodes) {
    return 0;
  }
  uError("grant failed to create dnode, exist:%" PRIu32 ", reason:grant dnode limited", gStatus.curDnodes);
  return TSDB_CODE_GRANT_DNODE_LIMITED;
}

static int32_t grantCheckStorage() { return 0; }

static int32_t grantCheckGrantSpeed() { return TSDB_CODE_SUCCESS; }
static int32_t grantCheckQueryTime() { return TSDB_CODE_SUCCESS; }
static int32_t grantCheckConns() { return TSDB_CODE_SUCCESS; }
static int32_t grantCheckStreams() {
  ASSERTS(gStatus.limitStreams != GRANT_UNIQ_UNDEFINED, "limitStreams is %d", GRANT_UNIQ_UNDEFINED);
  if (!gStatus.streamExpired &&
      (gStatus.limitStreams == GRANT_UNIQ_UNLIMITED || gStatus.curStreams < gStatus.limitStreams)) {
    return 0;
  }
  uError("grant failed to check stream, expire:%" PRIi64 ", num:%d, reason:stream limited",
         (int64_t)gStatus.streamExpireSec, (int32_t)gStatus.curStreams);
  return TSDB_CODE_GRANT_CPU_LIMITED;
}
static int32_t grantCheckTopics() {
  ASSERTS(gStatus.limitTopics != GRANT_UNIQ_UNDEFINED, "limitTopics is %d", GRANT_UNIQ_UNDEFINED);
  if (!gStatus.topicExpired &&
      (gStatus.limitTopics == GRANT_UNIQ_UNLIMITED || gStatus.curTopics < gStatus.limitTopics)) {
    return 0;
  }
  uError("grant failed to check topic, expire:%" PRIi64 ", num:%d, reason:topic limited",
         (int64_t)gStatus.topicExpireSec, (int32_t)gStatus.curTopics);
  return TSDB_CODE_GRANT_CPU_LIMITED;
}

static int32_t grantCheckStreamExpired() { return gStatus.streamExpired ? TSDB_CODE_GRANT_EXPIRED : TSDB_CODE_SUCCESS; }

static int32_t grantCheckCpuCores() {
  if (gStatus.limitCpuCores == GRANT_UNIQ_UNLIMITED) {
    return 0;
  }
  gStatus.curCpuCores = grantGetClusterCurCores(grantHandle.pMnode);
  if (gStatus.curCpuCores < gStatus.limitCpuCores) {
    return 0;
  }
  uError("grant failed to create dnode, exist:%" PRIu32 ", reason:grant cpu cores limited", gStatus.curCpuCores);
  return TSDB_CODE_GRANT_CPU_LIMITED;
}

#endif

int32_t grantCheck(EGrantType grant) {
#ifdef GRANTS_CFG
  switch (grant) {
    case TSDB_GRANT_DB:
      return cloudGrantCheckDatabases();
    case TSDB_GRANT_TIMESERIES:
      return cloudGrantCheckTimeSeries();
    case TSDB_GRANT_STABLE:
      return cloudGrantCheckSTables();
    case TSDB_GRANT_TABLE:
      return cloudGrantCheckTables();
    default:
      break;
  }
#else
  switch (grant) {
    case TSDB_GRANT_TIME:
      return GRANT_EXPIRED(gStatus.basicExpired);
    case TSDB_GRANT_USER:
      return grantCheckUsers();
    case TSDB_GRANT_DB:
      return grantCheckDatabases();
    case TSDB_GRANT_TIMESERIES:
      return grantCheckTimeSeries();
    case TSDB_GRANT_DNODE:
      return grantCheckDnodes();
    case TSDB_GRANT_ACCT:
      return grantCheckAccts();
    case TSDB_GRANT_STORAGE:
      return grantCheckStorage();
    case TSDB_GRANT_SPEED:
      return grantCheckGrantSpeed();
    case TSDB_GRANT_QUERY_TIME:
      return grantCheckQueryTime();
    case TSDB_GRANT_CONNS:
      return grantCheckConns();
    case TSDB_GRANT_STREAM:
      return grantCheckStreams();
    case TSDB_GRANT_CPU_CORES:
      return grantCheckCpuCores();
    case TSDB_GRANT_TOPIC:
      return grantCheckTopics();
    case TSDB_GRANT_STREAM_EXPIRE:
      return GRANT_EXPIRED(gStatus.streamExpired);
    case TSDB_GRANT_TOPIC_EXPIRE:
      return GRANT_EXPIRED(gStatus.topicExpired);
    case TSDB_GRANT_AUDIT_EXPIRE:
      return GRANT_EXPIRED(gStatus.auditExpired);
    case TSDB_GRANT_MULTI_TIER_EXPIRE:
      return GRANT_EXPIRED(gStatus.multiTierExpired);
    default:
      break;
  }
#endif
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE bool grantIsValid(GrantMsg *pStatus) {
  return pStatus->limitTimeSeries || IS_GRANT_TDENGINE(pStatus);
}
static FORCE_INLINE bool grantConnIsValid(GrantMsg *pStatus) { return IS_GRANT_CONNECTORS(pStatus); }

#ifndef GRANTS_CFG
static void grantStatusAssignLimits(SGrantStatus *p1, SGrantStatus *p2, bool isCombine) {
  if (isCombine) {
    // use larger value
    if (p2->usbDongle) p1->usbDongle = p2->usbDongle;
    if (p2->officialVersion) p1->officialVersion = p2->officialVersion;
    GRANT_ITEM_SET_VAL(p1->expireTimeSec, p2->expireTimeSec, GRANT_EXPIRE_TIME);
    GRANT_ITEM_SET_VAL(p1->limitStorage, p2->limitStorage, GRANT_STORAGE_LIMITS);
    GRANT_ITEM_SET_VAL(p1->limitSpeed, p2->limitSpeed, GRANT_WRITING_SPEED_LIMITS);
    GRANT_ITEM_SET_VAL(p1->limitTimeSeries, p2->limitTimeSeries, GRANT_TIME_SERIES_LIMITS);
    GRANT_ITEM_SET_VAL(p1->limitQueryTime, p2->limitQueryTime, GRANT_QUERY_TIME_LIMITS);
    GRANT_ITEM_SET_VAL(p1->limitDbs, p2->limitDbs, GRANT_DATABASE_LIMITS);
    GRANT_ITEM_SET_VAL(p1->limitUsers, p2->limitUsers, GRANT_USER_LIMITS);
    GRANT_ITEM_SET_VAL(p1->limitConns, p2->limitConns, GRANT_CONNECTION_LIMITS);
    GRANT_ITEM_SET_VAL(p1->limitStreams, p2->limitStreams, GRANT_STREAM_LIMITS);
    GRANT_ITEM_SET_VAL(p1->limitAccts, p2->limitAccts, GRANT_ACCT_LIMITS);
    GRANT_ITEM_SET_VAL(p1->limitDnodes, p2->limitDnodes, GRANT_DNODE_LIMITS);
    GRANT_ITEM_SET_VAL(p1->limitCpuCores, p2->limitCpuCores, GRANT_CPU_LIMITS);
  } else {
    p1->usbDongle = p2->usbDongle;
    p1->officialVersion = p2->officialVersion;
    p1->expireTimeSec = p2->expireTimeSec;
    p1->limitStorage = p2->limitStorage;
    p1->limitSpeed = p2->limitSpeed;
    p1->limitTimeSeries = p2->limitTimeSeries;
    p1->limitQueryTime = p2->limitQueryTime;
    p1->limitDbs = p2->limitDbs;
    p1->limitUsers = p2->limitUsers;
    p1->limitConns = p2->limitConns;
    p1->limitStreams = p2->limitStreams;
    p1->limitAccts = p2->limitAccts;
    p1->limitDnodes = p2->limitDnodes;
    p1->limitCpuCores = p2->limitCpuCores;
  }
}
#endif

static void grantConnStatusAssignLimits(void *g1, void *g2, bool isCombine) {
#ifndef GRANTS_CFG
  SGrantStatus *p1 = g1;
  SGrantStatus *p2 = g2;
#else
  GrantStatus *p1 = g1;
  GrantStatus *p2 = g2;
#endif
  if (isCombine) {
    // use larger value
    if (IS_GRANT_CONNECTORS_OFFICIAL(p2)) {
      SET_GRANT_CONNECTORS_OFFICIAL(p1);
    }
    for (int32_t i = 0; i < GRANT_CONN_NUM; ++i) {
      SGrantConnItem *pItem = GRANT_CONN_ITEM(p1, i);
      SGrantConnItem *qItem = GRANT_CONN_ITEM(p2, i);
      if (!GRANT_CONN_ITEM_UNDEF(qItem)) {
        if (GRANT_CONN_ITEM_UNDEF(pItem)) {
          *pItem = *qItem;
        } else {
          GRANT_ITEM_SET_VAL(pItem->number, qItem->number, GRANT_CONN_LIMITS);
          GRANT_ITEM_SET_VAL(pItem->speed, qItem->speed, GRANT_CONN_LIMITS);
          GRANT_ITEM_SET_VAL(pItem->expire, qItem->expire, GRANT_CONN_EXPIRE_LIMITS);
        }
      }
    }
  } else {
    GRANT_CONN_OFFICIAL(p1) = GRANT_CONN_OFFICIAL(p2);
    memcpy(GRANT_CONN_ITEMS(p1), GRANT_CONN_ITEMS(p2), sizeof(SGrantConnItem) * GRANT_CONN_NUM);
  }
}

static int grantConnDistCompare(const void *l, const void *r) {
  if (((SGrantDistInfo *)l)->connDist == ((SGrantDistInfo *)r)->connDist) return 0;
  return ((SGrantDistInfo *)l)->connDist > ((SGrantDistInfo *)r)->connDist ? 1 : -1;
}

static int grantDistCompare(const void *l, const void *r) {
  if (((SGrantDistInfo *)l)->dist == ((SGrantDistInfo *)r)->dist) return 0;
  return ((SGrantDistInfo *)l)->dist > ((SGrantDistInfo *)r)->dist ? 1 : -1;
}

static void grantConnStatusCheckImpl(SMnode *pMnode) {
  int32_t   nGrant = 0;
  SHashObj *pGrants = grantHandle.pOfficials;
  SArray   *pDists = grantHandle.pDistInfo;
  int32_t   distSize = taosArrayGetSize(pDists);

  if (distSize <= 0) goto _exit;

  uint32_t leastDist = 0;
  if (distSize > 1) {
    taosArraySort(pDists, grantConnDistCompare);
    uint32_t lastDist = GRANT_CONN_DIST(pDists, distSize - 1);
    uint32_t last2Dist = GRANT_CONN_DIST(pDists, distSize - 2);
    leastDist = lastDist == last2Dist ? lastDist : lastDist - GRANT_DIST_TOLERENCE;
  } else {
    leastDist = GRANT_CONN_DIST(pDists, distSize - 1);
  }

#ifndef GRANTS_CFG
  SGrantStatus status = {0};
#else
  GrantStatus status = {0};
#endif

  SGrantConnItem *pItems = status.connectors.items;
  GRANT_ITEMS_INIT(pItems);

  for (int32_t i = distSize; i > 0;) {
    SGrantDistInfo *pInfo = TARRAY_GET_ELEM(pDists, --i);
    if (pInfo->connDist < leastDist) continue;
#ifndef GRANTS_CFG
    SGrantStatus *pStatus = taosHashGet(pGrants, &pInfo->dnodeId, sizeof(pInfo->dnodeId));
#else
    GrantStatus *pStatus = taosHashGet(pGrants, &pInfo->dnodeId, sizeof(pInfo->dnodeId));
#endif
    if (pStatus && IS_GRANT_CONNECTORS(pStatus)) {
      grantConnStatusAssignLimits(&status, pStatus, true);
      ++nGrant;
    }
  }

  // fill conn grant items from undef to default
  grantConnActiveFillUndef(pMnode, pItems);

  if (nGrant > 0) grantConnStatusAssignLimits(&gStatus, &status, false);

_exit:
  if (nGrant == 0) {
    // grantConnResetMaster(pMnode);
  }
}

#ifndef GRANTS_CFG
static void grantStatusCheckImpl(SMnode *pMnode) {
  int32_t   nGrant = 0;
  SHashObj *pGrants = grantHandle.pOfficials;
  SArray   *pDists = grantHandle.pDistInfo;
  int32_t   distSize = taosArrayGetSize(pDists);

  if (distSize <= 0) goto _exit;

  uint32_t leastDist = 0;
  if (distSize > 1) {
    taosArraySort(pDists, grantDistCompare);
    uint32_t lastDist = GRANT_GET_DIST(pDists, distSize - 1);
    uint32_t last2Dist = GRANT_GET_DIST(pDists, distSize - 2);
    leastDist = lastDist == last2Dist ? lastDist : lastDist - GRANT_DIST_TOLERENCE;
  } else {
    leastDist = GRANT_GET_DIST(pDists, distSize - 1);
  }

  SGrantStatus status = {0};

  for (int32_t i = distSize; i > 0;) {
    SGrantDistInfo *pInfo = TARRAY_GET_ELEM(pDists, --i);
    if (pInfo->dist < leastDist) continue;
    SGrantStatus *pStatus = taosHashGet(pGrants, &pInfo->dnodeId, sizeof(pInfo->dnodeId));
    if (pStatus && IS_GRANT_TDENGINE(pStatus)) {
      grantStatusAssignLimits(&status, pStatus, true);
      ++nGrant;
    }
  }

  if (nGrant > 0) grantStatusAssignLimits(&grantStatus, &status, false);

_exit:
  if (nGrant == 0) {
    uWarn("grant reset because official grants not received");
    grantResetMaster(pMnode);
  }
}
#endif

#ifdef GRANTS_CFG
static void grantConnStatusCheck(SMnode *pMnode, uint32_t curTime, SDnodeInfo *pDnodeInfo) {
  // for connectors
  grantConnStatusCheckImpl(pMnode);

  *grantHandle.lastCheck = curTime;

  uDebug("grant message received from dnode:%d, timeseries:%" PRIu64
        ", database:%u, stable:%u, table:%u, set to grant state",
        pDnodeInfo ? pDnodeInfo->id : -1, gStatus.limitTimeSeries, gStatus.limitDbs, gStatus.limitSTables,
        gStatus.limitTables);
}
#endif

#ifndef GRANTS_CFG
static void grantStatusCheck(SMnode *pMnode, uint32_t curTime, SDnodeInfo *pDnodeInfo) {
  // for TDengine
  grantStatusCheckImpl(pMnode);
  // GrantStatus *pGrantStatus = &gStatus;
  // char         ts[GRANT_TS_SEC_LEN] = {0};
  // grantSecondsToString(pGrantStatus->expireTimeSec, ts);
  // uint32_t     grantCurTime = TMAX(curTime, GRANT_CUR_TIME);
  // if (pGrantStatus->expireTimeSec > grantCurTime) {
  //   if (pGrantStatus->expired) {
  //     pGrantStatus->expired = false;
  //     uDebug("grant message received from dnode:%d, storage:%uGB, timeseries:%" PRIu64
  //           ", database:%u, user:%u, expire:%s %u, curtime:%u, set to grant state",
  //           pDnodeInfo ? pDnodeInfo->id : -1, (uint32_t)(pGrantStatus->limitStorage / (int64_t)1073741824),
  //           pGrantStatus->limitTimeSeries, pGrantStatus->limitDbs, pGrantStatus->limitUsers, ts,
  //           pGrantStatus->expireTimeSec, grantCurTime);
  //   } else {
  //     uTrace("grant message received from dnode:%d, storage:%uGB, timeseries:%" PRIu64
  //            ", database:%u, user:%u, expire:%s %u, curtime:%u, already in grant state",
  //            pDnodeInfo ? pDnodeInfo->id : -1, (uint32_t)(pGrantStatus->limitStorage / (int64_t)1073741824),
  //            pGrantStatus->limitTimeSeries, pGrantStatus->limitDbs, pGrantStatus->limitUsers, ts,
  //            pGrantStatus->expireTimeSec, grantCurTime);
  //   }
  // } else {
  //   pGrantStatus->expired = true;
  //   uError("grant cluster expired at %s %u, curtime: %u, set to un-grant state", ts, pGrantStatus->expireTimeSec,
  //          grantCurTime);
  // }

  // for connectors
  grantConnStatusCheckImpl(pMnode);

}

static FORCE_INLINE bool grantIsOfficial(GrantStatus *pStatus) { return pStatus->officialVersion; }

#endif

static int32_t mndProcessDnodeSGrantMsg(SMnode *pMnode, SDnodeInfo *pDnodeInfo, GrantMsg *pGrantMsg,
                                        GrantStatus *pGrantStatus) {
  uint32_t curTime = taosGetTimestampMs() / 1000;
  if (pGrantMsg->machine[0] != 0) {
    const char *val = taosHashGet(grantHandle.pMachines, &pDnodeInfo->id, sizeof(pDnodeInfo->id));
    if (!val || 0 != strncmp(val, pGrantMsg->machine, GRANT_MACHINE_KEY_LEN + 1)) {
      taosHashPut(grantHandle.pMachines, &pDnodeInfo->id, sizeof(pDnodeInfo->id), &pGrantMsg->machine,
                  strlen(pGrantMsg->machine));
    }
  }
#ifdef GRANTS_CFG
  if (pGrantMsg->updateForced) {
    pGrantStatus->limitTimeSeries = pGrantMsg->limitTimeSeries;
    pGrantStatus->limitDbs = pGrantMsg->limitDbs;
    pGrantStatus->limitSTables = pGrantMsg->limitSTables;
    pGrantStatus->limitTables = pGrantMsg->limitTables;
  } else {
    COMPARE_SET_VAL(pGrantStatus->limitTimeSeries, pGrantMsg->limitTimeSeries, <);
    COMPARE_SET_VAL(pGrantStatus->limitDbs, pGrantMsg->limitDbs, <);
    COMPARE_SET_VAL(pGrantStatus->limitSTables, pGrantMsg->limitSTables, <);
    COMPARE_SET_VAL(pGrantStatus->limitTables, pGrantMsg->limitTables, <);
  }

  if (grantConnIsValid(pGrantMsg)) {
    GrantStatus status = {0};
    SET_GRANT_CONNECTORS(&status);
    GRANT_CONN_OFFICIAL(&status) = GRANT_CONN_OFFICIAL(pGrantMsg);
    memcpy(GRANT_CONN_ITEMS(&status), GRANT_CONN_ITEMS(pGrantMsg), sizeof(SGrantConnItem) * GRANT_CONN_NUM);

    taosHashPut(grantHandle.pOfficials, &pDnodeInfo->id, sizeof(TSDB_DATA_TYPE_INT), &status, sizeof(GrantStatus));
    SGrantDistInfo distInfo = {.connDist = pGrantMsg->distribute, .dnodeId = pDnodeInfo->id};
    taosArrayPush(grantHandle.pDistInfo, &distInfo);
  }

#else
  // process grant status from mnode
  if (grantIsValid(pGrantMsg) || grantConnIsValid(pGrantMsg)) {
    SGrantStatus status = {0};
    if (grantIsValid(pGrantMsg)) {
      SET_GRANT_TDENGINE(&status);
      status.usbDongle = pGrantMsg->usbDongle;
      status.officialVersion = pGrantMsg->officialVersion;
      status.expireTimeSec = pGrantMsg->expireTimeSec;
      status.limitStorage = (uint64_t)(pGrantMsg->limitStorage) * (uint64_t)1073741824;
      status.limitSpeed = pGrantMsg->limitSpeed;
      status.limitTimeSeries = pGrantMsg->limitTimeSeries;
      status.limitQueryTime = pGrantMsg->limitQueryTime;
      status.limitDbs = pGrantMsg->limitDbs;
      status.limitUsers = pGrantMsg->limitUsers;
      status.limitConns = pGrantMsg->limitConns;
      status.limitStreams = pGrantMsg->limitStreams;
      status.limitAccts = pGrantMsg->limitAccts;
      status.limitDnodes = pGrantMsg->limitDnodes;
      status.limitCpuCores = pGrantMsg->limitCpuCores;
    }

    // assign the connectors
    if (grantConnIsValid(pGrantMsg)) {
      SET_GRANT_CONNECTORS(&status);
      GRANT_CONN_OFFICIAL(&status) = GRANT_CONN_OFFICIAL(pGrantMsg);
      memcpy(GRANT_CONN_ITEMS(&status), GRANT_CONN_ITEMS(pGrantMsg), sizeof(SGrantConnItem) * GRANT_CONN_NUM);
    }

    taosHashPut(grantHandle.pOfficials, &pDnodeInfo->id, sizeof(TSDB_DATA_TYPE_INT), &status, sizeof(GrantStatus));
    SGrantDistInfo distInfo = {
        .dist = pGrantMsg->distribute,
        .connDist = pGrantMsg->connectors.distribute,
        .dnodeId = pDnodeInfo->id,
    };
    taosArrayPush(grantHandle.pDistInfo, &distInfo);
  }

#endif
  return TSDB_CODE_SUCCESS;
}

static int32_t mndCfgDnodeReq(SDnodeInfo *pDnodeInfo, const char *cfg, const char *val) {
  SMCfgDnodeReq req = {0};
  req.dnodeId = pDnodeInfo->id;
  strncpy(req.config, cfg, TSDB_DNODE_CONFIG_LEN);
  strncpy(req.value, val, TSDB_DNODE_VALUE_LEN);

  int32_t contLen = tSerializeSMCfgDnodeReq(NULL, 0, &req);
  void   *pCont = rpcMallocCont(contLen);
  if (!pCont) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uWarn("failed to generate dnodeCfg msg for grant since %s", terrstr());
    return TSDB_CODE_FAILED;
  }

  tSerializeSMCfgDnodeReq(pCont, contLen, &req);

  SRpcMsg rpcMsg = {
      .pCont = pCont,
      .contLen = contLen,
      .msgType = TDMT_MND_CONFIG_DNODE,
  };

  uInfo("send cfg dnode req for grant to dnode:%d %s:%" PRIu16, pDnodeInfo->id, pDnodeInfo->ep.fqdn,
        pDnodeInfo->ep.port);

  SEpSet epSet = {.numOfEps = 1};
  strncpy(epSet.eps[0].fqdn, tsLocalFqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = tsServerPort;

  tmsgSendReq(&epSet, &rpcMsg);

  return TSDB_CODE_SUCCESS;
}

static int32_t mndSetActiveCodeFromCfg(SDnodeInfo *pDnodeInfo, GrantMsg *pMsg) {
#ifndef GRANTS_CFG
  if (pDnodeInfo->active[0] == 0 && pMsg->active[0] != 0) {
    mndCfgDnodeReq(pDnodeInfo, GRANT_ACTIVE_CODE, pMsg->active);
  }
#endif
  if (pDnodeInfo->connActive[0] == 0 && pMsg->connectors.active[0] != 0) {
    mndCfgDnodeReq(pDnodeInfo, GRANT_C_ACTIVE_CODE, pMsg->connectors.active);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mndRetrieveGrant(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  int32_t numOfRows = 0;
  int32_t cols = 0;
  char   *pWrite = NULL;
  char    tmp[192] = {0};
  char    tmp1[192] = {0};
  char    ts[GRANT_TS_SEC_LEN] = {0};

  if (pShow->numOfRows < 1) {
    SGrantDataIns *pDataIn = NULL;
#ifdef GRANTS_CFG
    const char      *src;
    SColumnInfoData *pColInfo;

    cols = 0;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "cloud";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (cloudGrantStatus.limitTimeSeries != GRANT_TIME_SERIES_LIMITS) {
      sprintf(tmp1, "%" PRIu64 "/%" PRIu64, cloudGrantStatus.curTimeSeries, cloudGrantStatus.limitTimeSeries);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (cloudGrantStatus.limitDbs != GRANT_DATABASE_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurDbs(pMnode), cloudGrantStatus.limitDbs);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (cloudGrantStatus.limitSTables != GRANT_STABLE_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurSTables(pMnode), cloudGrantStatus.limitSTables);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (cloudGrantStatus.limitTables != GRANT_TABLE_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurTables(pMnode), cloudGrantStatus.limitTables);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);
#else
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    const char      *src = GRANT_VERSION;
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    GRANT_EXPIRE_SHOW(gStatus.basicExpireSec);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = gStatus.basicExpired || (gStatus.multiTierExpired && tsDiskCfgNum > 1) ? "true" : "false";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    GRANT_ITEM_SHOW(gStatus.curTimeSeries, gStatus.limitTimeSeries, 64);
    GRANT_ITEM_SHOW(gStatus.curDnodes, gStatus.limitDnodes, 16);
    GRANT_ITEM_SHOW(gStatus.curStreams, gStatus.limitStreams, 16);
    GRANT_ITEM_SHOW(gStatus.curTopics, gStatus.limitTopics, 16);
    GRANT_ITEM_SHOW(gStatus.curCpuCores, gStatus.limitCpuCores, 32);
    GRANT_EXPIRE_SHOW(gStatus.multiTierExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.streamExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.topicExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.auditExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.bakRstExpireSec);
    GRANT_EXPIRE_SHOW(gStatus.replicaExpireSec);

#endif
    // connectors
    GRANT_DATA_IN_SHOW(CONN_TYPE_OPC_DA, "OPC_DA");
    GRANT_DATA_IN_SHOW(CONN_TYPE_OPC_UA, "OPC_UA");
    GRANT_DATA_IN_SHOW(CONN_TYPE_PI, "Pi");
    GRANT_DATA_IN_SHOW(CONN_TYPE_KAFKA, "Kafka");
    GRANT_DATA_IN_SHOW(CONN_TYPE_INFLUXDB, "InfluxDB");
    GRANT_DATA_IN_SHOW(CONN_TYPE_MQTT, "MQTT");
    GRANT_DATA_IN_SHOW(CONN_TYPE_OpenTSDB, "OpenTSDB");
    GRANT_DATA_IN_SHOW(CONN_TYPE_TDengine_2_6, "TDengine2.6");
    GRANT_DATA_IN_SHOW(CONN_TYPE_TDengine_3_0, "TDengine3.0");

    numOfRows++;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextGrant(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static int32_t tDeserializeGrantNotify(void *buf, int32_t bufLen, GrantNotify *pNotify) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  if (tDecodeU64(&decoder, &pNotify->curTimeSeries) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

static int32_t tSerializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, SDnodeInfo *pInfo, int64_t clusterTime) {
#if 0
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

#ifdef GRANTS_CFG
  // grant status
  if (tEncodeU64(&encoder, pStatus->limitTimeSeries) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitDbs) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitSTables) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitTables) < 0) return -1;
  // current value
  if (tEncodeU64(&encoder, pStatus->curTimeSeries) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->curDbs) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->curSTables) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->curTables) < 0) return -1;
#else
  // grant status
  if (tEncodeI8(&encoder, pStatus->usbDongle ? 1 : 0) < 0) return -1;
  if (tEncodeI8(&encoder, pStatus->officialVersion ? 1 : 0) < 0) return -1;
  if (tEncodeI8(&encoder, pStatus->expired ? 1 : 0) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->expireTimeSec) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->lastCheck) < 0) return -1;
  if (tEncodeU64(&encoder, pStatus->limitStorage) < 0) return -1;
  if (tEncodeU64(&encoder, pStatus->limitTimeSeries) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitSpeed) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitQueryTime) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitDbs) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitUsers) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitConns) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitStreams) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitAccts) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitDnodes) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitCpuCores) < 0) return -1;
  // current value
  if (tEncodeU64(&encoder, pStatus->curStorage) < 0) return -1;
  if (tEncodeU64(&encoder, pStatus->curTimeSeries) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->curSpeed) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->curQueryTime) < 0) return -1;
  // additional values
  if (tEncodeU32v(&encoder, pStatus->curDbs) < 0) return -1;
  if (tEncodeU32v(&encoder, pStatus->curUsers) < 0) return -1;
  if (tEncodeU32v(&encoder, pStatus->curAccts) < 0) return -1;
  if (tEncodeU32v(&encoder, pStatus->curDnodes) < 0) return -1;
  // version 2: since 3.0.5.0
  if (tEncodeU32v(&encoder, pStatus->curCpuCores) < 0) return -1;
  if (tEncodeI8(&encoder, pStatus->flag) < 0) return -1;
#endif
  // version 2: support activeCode/connectors activeCode since 3.0.5.0
  if (tSerializeGrantConnMsg(&encoder, &pStatus->connectors) < 0) return -1;
  if (tEncodeBinary(&encoder, pInfo->active, TSDB_ACTIVE_KEY_LEN) < 0) return -1;
  if (tEncodeBinary(&encoder, pInfo->connActive, TSDB_CONN_ACTIVE_KEY_LEN) < 0) return -1;
  // end of version 2

  // version 3: since 3.1.0.0
  if (tEncodeI64v(&encoder, clusterTime) < 0) return -1;

  // version 4: since 3.1.2.0/3.2.1.0
  if (tEncodeI32v(&encoder, pInfo->id) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);

  return tlen;
#endif
  return 0;
}

int32_t tDeserializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, SDnodeInfo *pInfo,
                                int64_t *clusterTime) {
#if 0
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

#ifdef GRANTS_CFG
  // grant status
  if (tDecodeU64(&decoder, &pStatus->limitTimeSeries) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitDbs) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitSTables) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitTables) < 0) return -1;
  // current value
  if (tDecodeU64(&decoder, &pStatus->curTimeSeries) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->curDbs) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->curSTables) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->curTables) < 0) return -1;
#else
  // grant status
  if (tDecodeI8(&decoder, (int8_t *)&pStatus->usbDongle) < 0) return -1;
  if (tDecodeI8(&decoder, (int8_t *)&pStatus->officialVersion) < 0) return -1;
  if (tDecodeI8(&decoder, (int8_t *)&pStatus->expired) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->expireTimeSec) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->lastCheck) < 0) return -1;
  if (tDecodeU64(&decoder, &pStatus->limitStorage) < 0) return -1;
  if (tDecodeU64(&decoder, &pStatus->limitTimeSeries) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitSpeed) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitQueryTime) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitDbs) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitUsers) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitConns) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitStreams) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitAccts) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitDnodes) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitCpuCores) < 0) return -1;
  // current value
  if (tDecodeU64(&decoder, &pStatus->curStorage) < 0) return -1;
  if (tDecodeU64(&decoder, &pStatus->curTimeSeries) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->curSpeed) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->curQueryTime) < 0) return -1;
  // additional values
  if (tDecodeU32v(&decoder, &pStatus->curDbs) < 0) return -1;
  if (tDecodeU32v(&decoder, &pStatus->curUsers) < 0) return -1;
  if (tDecodeU32v(&decoder, &pStatus->curAccts) < 0) return -1;
  if (tDecodeU32v(&decoder, &pStatus->curDnodes) < 0) return -1;
  // version 2: support curCurCores since 3.0.5.0
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeU32v(&decoder, &pStatus->curCpuCores) < 0) return -1;
    if (tDecodeI8(&decoder, (int8_t *)&pStatus->flag) < 0) return -1;
  }
#endif

  // version 2: support activeCode/connectors activeCode since 3.0.5.0
  if (!tDecodeIsEnd(&decoder)) {
    if (tDeserializeGrantConnMsg(&decoder, &pStatus->connectors) < 0) return -1;
    char *data = NULL;
    if (tDecodeBinary(&decoder, (uint8_t **)&data, NULL) < 0) return -1;
    if (data) tstrncpy(pInfo->active, data, TSDB_ACTIVE_KEY_LEN);
    data = NULL;
    if (tDecodeBinary(&decoder, (uint8_t **)&data, NULL) < 0) return -1;
    if (data) tstrncpy(pInfo->connActive, data, TSDB_CONN_ACTIVE_KEY_LEN);
  }

  // version 3: since 3.1.0.0
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI64v(&decoder, clusterTime) < 0) return -1;
  }

  // version 4: since 3.1.2.0/3.2.1.0
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32v(&decoder, &pInfo->id) < 0) return -1;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
#endif
  return 0;
}

int32_t tSerializeGrantMsg(void *buf, int32_t bufLen, GrantMsg *pMsg) {
#if 0
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

#ifdef GRANTS_CFG
  // grant msg
  if (tEncodeI8(&encoder, pMsg->updateForced ? 1 : 0) < 0) return -1;
  if (tEncodeU64(&encoder, pMsg->limitTimeSeries) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitDbs) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitSTables) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitTables) < 0) return -1;
#else
  // grant msg
  if (tEncodeI8(&encoder, pMsg->updateForced ? 1 : 0) < 0) return -1;
  if (tEncodeI8(&encoder, pMsg->usbDongle ? 1 : 0) < 0) return -1;
  if (tEncodeI8(&encoder, pMsg->officialVersion ? 1 : 0) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->expireTimeSec) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitStorage) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitSpeed) < 0) return -1;
  if (tEncodeU64(&encoder, pMsg->limitTimeSeries) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitQueryTime) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitDbs) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitUsers) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitConns) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitStreams) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitAccts) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitDnodes) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitCpuCores) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->reserveKey1) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->reserveKey2) < 0) return -1;
#endif

  if (tEncodeI8(&encoder, pMsg->flag) < 0) return -1;                      // version 2 since 3.0.5.0
  if (tSerializeGrantConnMsg(&encoder, &pMsg->connectors) < 0) return -1;  // version 2 since 3.0.5.0

  // since 3.1.0.0
  if (tEncodeU32(&encoder, pMsg->connectors.distribute) < 0) return -1;
  int16_t len = strlen(pMsg->active);
  if (tEncodeI16v(&encoder, len) < 0) return -1;
  if (len > 0 && tEncodeBinary(&encoder, pMsg->active, len) < 0) return -1;
  len = strlen(pMsg->connectors.active);
  if (tEncodeI16v(&encoder, len) < 0) return -1;
  if (len > 0 && tEncodeBinary(&encoder, pMsg->connectors.active, len) < 0) return -1;

  // since 3.1.1.7
  len = strlen(pMsg->machine);
  if (tEncodeI16v(&encoder, len) < 0) return -1;
  if (len > 0 && tEncodeBinary(&encoder, pMsg->machine, len) < 0) return -1;

  // since 3.1.2.0/3.2.1.0
  if (tEncodeI32v(&encoder, pMsg->dnodeId) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
#endif
  return 0;
}

int32_t tDeserializeGrantMsg(void *buf, int32_t bufLen, GrantMsg *pMsg) {
#if 0
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

#ifdef GRANTS_CFG
  // grant msg
  if (tDecodeI8(&decoder, (int8_t *)&pMsg->updateForced) < 0) return -1;
  if (tDecodeU64(&decoder, &pMsg->limitTimeSeries) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitDbs) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitSTables) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitTables) < 0) return -1;
#else
  // grant msg
  if (tDecodeI8(&decoder, (int8_t *)&pMsg->updateForced) < 0) return -1;
  if (tDecodeI8(&decoder, (int8_t *)&pMsg->usbDongle) < 0) return -1;
  if (tDecodeI8(&decoder, (int8_t *)&pMsg->officialVersion) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->expireTimeSec) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitStorage) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitSpeed) < 0) return -1;
  if (tDecodeU64(&decoder, &pMsg->limitTimeSeries) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitQueryTime) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitDbs) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitUsers) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitConns) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitStreams) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitAccts) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitDnodes) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitCpuCores) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->reserveKey1) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->reserveKey2) < 0) return -1;
#endif

  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI8(&decoder, &pMsg->flag) < 0) return -1;                       // version 2 since 3.0.5.0
    if (tDeserializeGrantConnMsg(&decoder, &pMsg->connectors) < 0) return -1;  // version 2 since 3.0.5.0
  }

  // since 3.1.0.0
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeU32(&decoder, &pMsg->connectors.distribute) < 0) return -1;
    int16_t len = 0;
    if (tDecodeI16v(&decoder, &len) < 0) return -1;
    if (len > 0) {
      char *data = NULL;
      if (tDecodeBinary(&decoder, (uint8_t **)&data, NULL) < 0) return -1;
      if (data) strncpy(pMsg->active, data, len);
    }
    if (tDecodeI16v(&decoder, &len) < 0) return -1;
    if (len > 0) {
      char *data = NULL;
      if (tDecodeBinary(&decoder, (uint8_t **)&data, NULL) < 0) return -1;
      if (data) strncpy(pMsg->connectors.active, data, len);
    }
  }
  // since 3.1.1.7
  if (!tDecodeIsEnd(&decoder)) {
    int16_t len = 0;
    if (tDecodeI16v(&decoder, &len) < 0) return -1;
    if (len > 0) {
      char *data = NULL;
      if (tDecodeBinary(&decoder, (uint8_t **)&data, NULL) < 0) return -1;
      if (data) strncpy(pMsg->machine, data, len);
    }
  }

  // since 3.1.2.0/3.2.1.0
  if (!tDecodeIsEnd(&decoder)) {
    if (tDecodeI32v(&decoder, &pMsg->dnodeId) < 0) return -1;
  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
#endif
  return 0;
}

static void tGrantConnItemsInit(SGrantConnItem *pItems, int32_t nItem) {
  if (pItems && (nItem > 0)) {
    memset(pItems, 0, nItem * sizeof(SGrantConnItem));
  }
}

static int32_t tGrantConnItemsNum(int8_t version) {
  switch (version) {
    case 1:
      return GRANT_CONN_NUM_V1;
    default:
      return GRANT_CONN_NUM;
  }
}

static int32_t tSerializeGrantConnMsg(SEncoder *encoder, SGrantConnMsg *pMsg) {
  if (tEncodeI8(encoder, pMsg->majorVer) < 0) return -1;
  if (tEncodeI8(encoder, pMsg->minorVer) < 0) return -1;
  if (tEncodeU8(encoder, pMsg->officialVersion) < 0) return -1;
  if (tEncodeU8(encoder, CONN_TYPE_MAX_V1) < 0) return -1;
  for (int32_t i = 0; i < CONN_TYPE_MAX_V1; ++i) {
    SGrantConnItem *pItem = pMsg->items + i;
    if (tEncodeI32v(encoder, pItem->number) < 0) return -1;
    if (tEncodeI16v(encoder, pItem->speed) < 0) return -1;
    if (tEncodeU16v(encoder, pItem->expire) < 0) return -1;
  }
  return 0;
}

static int32_t tDeserializeGrantConnMsg(SDecoder *decoder, SGrantConnMsg *pMsg) {
  uint8_t nItems = 0;
  int32_t maxItems = tGrantConnItemsNum(-1);

  if (tDecodeI8(decoder, &pMsg->majorVer) < 0) return -1;
  if (tDecodeI8(decoder, &pMsg->minorVer) < 0) return -1;
  if (pMsg->majorVer == GRANT_CONN_MAJOR_VER) {
    if (tDecodeU8(decoder, (uint8_t *)&pMsg->officialVersion) < 0) return -1;
    if (tDecodeU8(decoder, &nItems) < 0) return -1;
    if (nItems > maxItems) {
      nItems = maxItems;
    }
    for (int32_t i = 0; i < nItems; ++i) {
      SGrantConnItem *pItem = pMsg->items + i;
      if (tDecodeI32v(decoder, &pItem->number) < 0) return -1;
      if (tDecodeI16v(decoder, &pItem->speed) < 0) return -1;
      if (tDecodeU16v(decoder, &pItem->expire) < 0) return -1;
    }
  } else {
    tGrantConnItemsInit(pMsg->items + nItems, maxItems - nItems);
    return -1;
  }

  return 0;
}
