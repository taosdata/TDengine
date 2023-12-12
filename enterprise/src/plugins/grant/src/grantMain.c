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

#define GRANT_ITEMS_INIT(pItems)                  \
  do {                                            \
    pItems[0].number = GRANT_CONN_NUM_UNDEF;      \
    pItems[0].speed = GRANT_CONN_SPEED_UNDEF;     \
    pItems[0].expire = GRANT_CONN_EXPIRE_UNDEF;   \
    for (int32_t i = 1; i < CONN_TYPE_MAX; ++i) { \
      *(pItems + i) = *(pItems + 0);              \
    }                                             \
  } while (0)

#define GRANT_LIMIT_TD_TO_UNIQ(td, uniq, max) \
  do {                                        \
    if ((td) == GRANT_LEGACY_LIMITS) {        \
      (uniq) = GRANT_UNIQ_UNLIMITED;          \
    } else {                                  \
      (uniq) = (td) > (max) ? (max) : (td);   \
    }                                         \
  } while (0)

#define GRANT_ITEM_TO_DATAIN(inField, iField, iLimits, iUndef) \
  do {                                                         \
    if ((iField) == (iLimits)) {                               \
      (inField) = GRANT_UNIQ_UNLIMITED;                        \
    } else if ((iField) == (iUndef)) {                         \
      (inField) = GRANT_UNIQ_UNDEFINED;                        \
    } else {                                                   \
      (inField) = (iField);                                    \
    }                                                          \
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

#define GRANT_VERSION (gStatus.officialVersion ? "official" : "trial")
#define GRANT_EXPIRE (gStatus.basicExpireSec)
#define GRANT_EXPIRED(exp) (exp) ? TSDB_CODE_GRANT_EXPIRED : TSDB_CODE_SUCCESS
#define GRANT_EXPIRE_VAL (gStatus.basicExpired | (tsDiskCfgNum > 1 ? (gStatus.multiTierExpired << 1) : 0))
#define GRANT_CONN_MAJOR_VER 1
#define GRANT_CONN_MINOR_VER 1
#define GRANT_FLAG_TDENGINE ((int8_t)0x01)
#define GRANT_FLAG_CONNECTORS ((int8_t)0x02)
#define GRANT_CONN_ITEMS(s) ((s)->connectors.items)
#define GRANT_CONN_ITEM(s, i) ((s)->connectors.items + i)
#define GRANT_CONN_OFFICIAL(s) ((s)->connectors.officialVersion)
#define SET_GRANT_LEGACY(s) ((s)->flag |= 0x01)
#define SET_GRANT_TDENGINE(s) ((s)->flag |= GRANT_FLAG_TDENGINE)
#define SET_GRANT_CONNECTORS(s) ((s)->flag |= GRANT_FLAG_CONNECTORS)
#define SET_GRANT_CONNECTORS_OFFICIAL(s) (GRANT_CONN_OFFICIAL(s) = 1)
#define SET_GRANT_CONNECTORS_TRIAL(s) (GRANT_CONN_OFFICIAL(s) = 0)
#define IS_GRANT_LEGACY(s) (((s)->flag & 0x01))
#define IS_GRANT_TDENGINE(s) (((s)->flag & 0x01) == GRANT_FLAG_TDENGINE)
#define IS_GRANT_CONNECTORS(s) (((s)->flag & 0x02) == GRANT_FLAG_CONNECTORS)
#define IS_GRANT_CONNECTORS_OFFICIAL(s) GRANT_CONN_OFFICIAL(s)
#define GRANT_GET_DIST(p, idx) (((SGrantDistInfo *)TARRAY_GET_ELEM((p), (idx)))->dist)
#define GRANT_CONN_DIST(p, idx) (((SGrantDistInfo *)TARRAY_GET_ELEM((p), (idx)))->connDist)
// uniq grant
#define GRANT_DATA_IN(s, i) ((s)->ins + i)

#define GRANT_DIST_TOLERENCE 86400  // seconds
#define GRANT_TS_SEC_LEN 20

SGrantStatus     grantStatus = {0};
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

typedef SGrantNotify     GrantNotify;
typedef SGrantUniqStatus GrantStatus;
typedef SGrantMsg        GrantMsg;

extern SGrantUniqObj grantObj;
extern char          tsVersionName[16];
extern int64_t       tsExpireTime;

static int32_t  grantSecondsToString(int64_t seconds, char *ts);
static void     dmRefreshGrantCfg();
static void     grantRetrieveGrantInfo(SMnode *pMnode);
static void     grantResetMaster(SMnode *pMnode);
static void     grantSetClusterInfo(SMnode *pMnode);
static int64_t  grantGetClusterCreateTime(SMnode *pMnode);
static int32_t  mndProcessGrantHB(SRpcMsg *pReq);
static int32_t  mndProcessGrantRsp(SRpcMsg *pRsp);
static int32_t  dmGenerateGrantMsg(SGrantUniqMsg *pGrant, GrantStatus *pGrantStatus, SDnodeInfo *pInfo, int64_t clusterTime);
static int32_t  mndProcessDnodeSGrantMsg(SMnode *pMnode, SDnodeInfo *pDnodeInfo, SGrantUniqMsg *pGrantMsg,
                                         GrantStatus *pGrantStatus);
static int32_t  tSerializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, SDnodeInfo *pInfo,
                                      int64_t clusterTime);
static int32_t  tDeserializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, SDnodeInfo *pInfo,
                                        int64_t *clusterTime);
static int32_t  tSerializeGrantMsg(void *buf, int32_t bufLen, SGrantUniqMsg *pMsg);
static int32_t  tDeserializeGrantMsg(void *buf, int32_t bufLen, SGrantUniqMsg *pMsg);
static int32_t  tSerializeGrantNotify(void *buf, int32_t bufLen, GrantNotify *pNotify);
static int32_t  tDeserializeGrantNotify(void *buf, int32_t bufLen, GrantNotify *pNotify);
static int64_t  grantGetClusterCurTimeSeries(SMnode *pMnode);
static void     grantStatusCheck(SMnode *pMnode, uint32_t curTime, SDnodeInfo *pDnodeInfo);

static int32_t mndRetrieveGrant(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextGrant(SMnode *pMnode, void *pIter);

// connectors
static int32_t tGrantConnItemsNum(int8_t version);
static int32_t tSerializeGrantConnMsg(SEncoder *encoder, SGrantConnMsg *pMsg);
static int32_t tDeserializeGrantConnMsg(SDecoder *decoder, SGrantConnMsg *pMsg);
static void    grantDataInsSetDefault(SGrantDataIns *pIns, int32_t num);
static int32_t tSerializeGrantDataIns(SEncoder *encoder, SGrantDataIns *pIns);
static int32_t tDeserializeGrantDataIns(SDecoder *decoder, SGrantDataIns *pIns);

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
  int64_t      lastCheck;
  int16_t      nGrantReq;
  int16_t      nGrantRsp;
  int16_t      nTaosdGranted;
  int16_t      nConnGranted;
  int8_t       nGrantNone;
} SGrantHandle;

static bool         recheckClusterTime = true;
static int8_t       grantHbLock = 0;
static int64_t      grantNotifyCnt = 0;
static int64_t      grantNotifyTimeSeries = INT64_MAX;
static int64_t      grantClusterEpoch = 0;
static SGrantHandle grantHandle = {0};
SGrantedInfo        grantedInfo = {0};

#define gStatus grantUniqStatus

int32_t mndInitGrant(SMnode *pMnode) {
  terrno = 0;
  tsGrantHBInterval = GRANT_HEART_BEAT_MIN;

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
  if (grantObj.clusterId[0] == 0 && clusterId > 0) {
    snprintf(grantObj.clusterId, GRANT_CLUSTER_ID_LEN + 1, "%" PRIi64, clusterId);
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
  gStatus = grantStatusReq;  // assign directly
#ifndef GRANTS_CFG
  int8_t grantExpireVal = GRANT_EXPIRE_VAL;
  if (grantExpireVal == 0) {
    atomic_val_compare_exchange_8(&tsGrant, 0, 1);
  } else {
    atomic_store_8(&tsGrant, 0);
  }
#endif

  // step 3: respond with grant msg
  grantSetClusterIdEx(*(int64_t *)pInfo);
  SGrantUniqMsg grantMsg = {0};
  if (0 != (terrno = dmGenerateGrantMsg(&grantMsg, &grantStatusReq, &dnodeInfo, clusterTime))) {
    goto _err;
  }
  int32_t contLen = tSerializeGrantMsg(NULL, 0, &grantMsg);
  void   *pCont = rpcMallocCont(contLen);
  if (!pCont) {
    taosMemoryFreeClear(grantMsg.pLegacy);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _err;
  }

  tSerializeGrantMsg(pCont, contLen, &grantMsg);
  taosMemoryFreeClear(grantMsg.pLegacy);

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

static int32_t dmGenerateGrantMsg(SGrantUniqMsg *pGrant, GrantStatus *pGrantStatus, SDnodeInfo *pInfo,
                                  int64_t clusterTime) {
  int32_t code = 0;

  // uniq grant
  pGrant->dnodeId = pInfo->id;
  pGrant->diskCfgNum = tsDiskCfgNum;
  char *machineId = grantGetMachineId();
  if (machineId) {
    memcpy(pGrant->machine, machineId, TSDB_MACHINE_ID_LEN);
    taosMemoryFreeClear(machineId);
  }
  if (pGrantStatus->uniqActive) goto _exit;

    // legacy grant
#ifdef GRANTS_CFG
  pGrant->pLegacy = taosMemoryCalloc(1, sizeof(SGrantMsg));
  if (!pGrant->pLegacy) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  SET_GRANT_LEGACY(pGrant);

  SGrantMsg *pLegacy = pGrant->pLegacy;
  SET_GRANT_TDENGINE(pLegacy);
  pLegacy->officialVersion = 1;
  pLegacy->expireTimeSec = GRANT_LEGACY_LIMITS;
  pLegacy->limitStorage = GRANT_LEGACY_LIMITS;
  pLegacy->limitSpeed = GRANT_LEGACY_LIMITS;
  pLegacy->limitTimeSeries = GRANT_LEGACY_LIMITS;
  pLegacy->limitQueryTime = GRANT_LEGACY_LIMITS;
  pLegacy->limitDbs = GRANT_LEGACY_LIMITS;
  pLegacy->limitUsers = GRANT_LEGACY_LIMITS;
  pLegacy->limitConns = GRANT_LEGACY_LIMITS;
  pLegacy->limitStreams = GRANT_LEGACY_LIMITS;
  pLegacy->limitAccts = GRANT_LEGACY_LIMITS;
  pLegacy->limitDnodes = GRANT_LEGACY_LIMITS;
  pLegacy->limitCpuCores = GRANT_LEGACY_LIMITS;
  pLegacy->reserveKey1 = GRANT_DIST_MIN + pInfo->id;
  pLegacy->reserveKey2 = 0;

  SET_GRANT_CONNECTORS(pLegacy);
  SGrantConnMsg *pConn = &pLegacy->connectors;
  pConn->officialVersion = 1;
  pConn->distribute = GRANT_DIST_MIN + pInfo->id;
  SGrantConnItem item = {.number = GRANT_CONN_LIMITS, .speed = GRANT_CONN_LIMITS, .expire = GRANT_CONN_EXPIRE_LIMITS};
  for (int32_t i = 1; i < CONN_TYPE_MAX; ++i) {
    SGrantConnItem *pItem = GRANT_CONN_ITEM(pLegacy, i);
    *pItem = item;
  }
#else
  SGrantObj     grantObj = {0};
  SGrantConnObj grantConnObj = {0};
  grantSetActiveCodes(pInfo, &grantObj, &grantConnObj);
  dmRefreshGrantCfg(&grantObj, &grantConnObj);

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

  if (!grantObj.granted && !grantConnObj.granted) goto _exit;

  pGrant->pLegacy = taosMemoryCalloc(1, sizeof(SGrantMsg));
  if (!pGrant->pLegacy) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  
  SET_GRANT_LEGACY(pGrant);

  SGrantMsg *pLegacy = pGrant->pLegacy;
  if (grantObj.granted) {
    SET_GRANT_TDENGINE(pLegacy);
    pLegacy->officialVersion = grantObj.officialVersion;
    pLegacy->expireTimeSec = grantObj.expireTimeSec;
    pLegacy->limitStorage = grantObj.limitStorage;  // GB
    pLegacy->limitSpeed = grantObj.limitSpeed;
    pLegacy->limitTimeSeries = grantObj.limitTimeSeries;
    pLegacy->limitQueryTime = grantObj.limitQueryTime;
    pLegacy->limitDbs = grantObj.limitDbs;
    pLegacy->limitUsers = grantObj.limitUsers;
    pLegacy->limitConns = grantObj.limitConns;
    pLegacy->limitStreams = grantObj.limitStreams;
    pLegacy->limitAccts = grantObj.limitAccts;
    pLegacy->limitDnodes = grantObj.limitDnodes;
    pLegacy->limitCpuCores = grantObj.limitCpuCores;
    pLegacy->reserveKey1 = grantObj.reserveKey1;
    pLegacy->reserveKey2 = grantObj.reserveKey2;
  }

  if (grantConnObj.granted) {
    SET_GRANT_CONNECTORS(pLegacy);
    SGrantConnMsg *pConn = &pLegacy->connectors;
    pConn->officialVersion = grantConnObj.officialVersion;
    memcpy(pConn->items, grantConnObj.items, sizeof(SGrantConnItem) * CONN_TYPE_MAX_V1);
    pLegacy->connectors.distribute = grantConnObj.distribute;
  }
#endif

_exit:
  return code;
}

static void grantConnActiveFillUndef(SMnode *pMnode, SGrantConnItem *pItems) {
  if (grantClusterEpoch <= 0) {
    grantClusterEpoch = grantGetClusterCreateTime(pMnode);
  }

  SGrantConnItem defaultItem = {.number = GRANT_CONN_NUM_DEFAULT,
                                .speed = GRANT_CONN_SPEED_DEFAULT,
                                .expire = ceil((double)grantClusterEpoch / 86400) + GRANT_CONN_EXPIRE_DEFAULT};

  for (int32_t i = 0; i < CONN_TYPE_MAX; ++i) {
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

static int32_t genUniqActiveFromLegacy(SGrantUniqObj *pObj, SGrantStatus *pStatus) {
  pObj->version = 0;
  pObj->granted = 0;
  pObj->distribute = GRANT_DIST_MIN;

  ASSERTS(IS_GRANT_TDENGINE(pStatus) || IS_GRANT_CONNECTORS(pStatus), "Invalid status flag:%" PRIi8, pStatus->flag);

  if (IS_GRANT_TDENGINE(pStatus)) {
    pObj->basicExpireDay = ceil((double)pStatus->expireTimeSec / 86400);
    pObj->multiTierExpireDay = pObj->basicExpireDay;
    pObj->streamExpireDay = pObj->basicExpireDay;
    pObj->topicExpireDay = pObj->basicExpireDay;
    pObj->bakRstExpireDay = pObj->basicExpireDay;
    pObj->replicaExpireDay = pObj->basicExpireDay;
    pObj->auditExpireDay = pObj->basicExpireDay;
    GRANT_LIMIT_TD_TO_UNIQ(pStatus->limitCpuCores, pObj->limitCpuCores, INT32_MAX);
    GRANT_LIMIT_TD_TO_UNIQ(pStatus->limitDnodes, pObj->limitDnodes, INT16_MAX);
    GRANT_LIMIT_TD_TO_UNIQ(pStatus->limitStreams, pObj->limitStreams, INT16_MAX);
    GRANT_LIMIT_TD_TO_UNIQ(pStatus->limitCpuCores, pObj->limitTopics, INT16_MAX);
    GRANT_LIMIT_TD_TO_UNIQ(pStatus->limitTimeSeries, pObj->limitTimeSeries, INT64_MAX);
    pObj->limitTopics = GRANT_UNIQ_UNLIMITED;
  } else {
    pObj->basicExpireDay = GRANT_UNIQ_UNDEFINED;
    pObj->multiTierExpireDay = GRANT_UNIQ_UNDEFINED;
    pObj->streamExpireDay = GRANT_UNIQ_UNDEFINED;
    pObj->topicExpireDay = GRANT_UNIQ_UNDEFINED;
    pObj->bakRstExpireDay = GRANT_UNIQ_UNDEFINED;
    pObj->replicaExpireDay = GRANT_UNIQ_UNDEFINED;
    pObj->auditExpireDay = GRANT_UNIQ_UNDEFINED;
    pObj->limitCpuCores = GRANT_UNIQ_UNDEFINED;
    pObj->limitDnodes = GRANT_UNIQ_UNDEFINED;
    pObj->limitStreams = GRANT_UNIQ_UNDEFINED;
    pObj->limitTopics = GRANT_UNIQ_UNDEFINED;
    pObj->limitTimeSeries = GRANT_UNIQ_UNDEFINED;
  }

  int32_t i = 0;
  if (IS_GRANT_CONNECTORS(pStatus)) {
    for (i = 0; i < CONN_TYPE_MAX_V1; ++i) {
      SGrantDataIns  *pIn = pObj->ins + i;
      SGrantConnItem *pItem = GRANT_CONN_ITEM(pStatus, i);
      GRANT_ITEM_TO_DATAIN(pIn->number, pItem->number, GRANT_CONN_LIMITS, GRANT_CONN_NUM_UNDEF);
      GRANT_ITEM_TO_DATAIN(pIn->speed, pItem->speed, GRANT_CONN_LIMITS, GRANT_CONN_SPEED_UNDEF);
      GRANT_ITEM_TO_DATAIN(pIn->expire, pItem->expire, GRANT_CONN_EXPIRE_LIMITS, GRANT_CONN_EXPIRE_UNDEF);
    }
  }
  for (int32_t j = i; j < CONN_TYPE_MAX; ++j) {
    SGrantDataIns *pIn = pObj->ins + j;
    pIn->number = GRANT_UNIQ_UNDEFINED;
    pIn->speed = GRANT_UNIQ_UNDEFINED;
    pIn->expire = GRANT_UNIQ_UNDEFINED;
  }

  if(grantUniqGenActiveCode(pObj)){
    // mndCfgDnodeReq(0, 0, 0);
  }

  return 0;
}

static void mndProcessGrantStatusCheck() {
  grantStatusCheck(grantHandle.pMnode, taosGetTimestampMs() / 1000, NULL);
  if(grantHandle.nTaosdGranted || grantHandle.nConnGranted){
    SGrantUniqObj uniqObj = {0};
    memcpy(uniqObj.clusterId, grantObj.clusterId, GRANT_CLUSTER_ID_LEN);
    genUniqActiveFromLegacy(&uniqObj, &grantStatus);
  }

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

  SGrantUniqMsg grantMsgRsp = {0};
  if (tDeserializeGrantMsg(pRsp->pCont, pRsp->contLen, &grantMsgRsp) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  SDnodeInfo  dnodeInfo = {.id = grantMsgRsp.dnodeId};
  SDnodeInfo *pDnodeInfo = taosArraySearch(grantHandle.pDnodeInfo, &dnodeInfo, dnodeInfoCmprFn, TD_EQ);

  ASSERTS(pDnodeInfo, "pDnodeInfo is NULL for %d", grantMsgRsp.dnodeId);

  if (pDnodeInfo) {
    uDebug("succeed to receive grant msg from dnode:%d, %s:%" PRIu16 ", nReq:%" PRIi16 ", nRsp:%" PRIi16,
           grantMsgRsp.dnodeId, pDnodeInfo->ep.fqdn, pDnodeInfo->ep.port, grantHandle.nGrantReq, grantHandle.nGrantRsp);
    mndProcessDnodeSGrantMsg(grantHandle.pMnode, pDnodeInfo, &grantMsgRsp, &gStatus);
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
    // in case some grant responses are not received for a long time
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
    if (!grantUniqParseActiveCode(&grantObj, NULL)) {
      grantResetMaster(pMnode);
    }
    gStatus.uniqActive = 1;
  } else {
    gStatus.uniqActive = 0;
  }

  // set cluster info after parse uniq active
  grantSetClusterInfo(pMnode);

  // reset grantHandle
  taosHashClear(grantHandle.pOfficials);
  taosArrayClear(grantHandle.pDistInfo);
  taosArrayClear(grantHandle.pDnodeInfo);
  taosHashClear(grantHandle.pMachines);
  grantHandle.nGrantReq = 0;
  grantHandle.nGrantRsp = 0;
  grantHandle.nTaosdGranted = 0;
  grantHandle.nConnGranted = 0;

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
  gStatus.curTimeSeries = grantGetClusterCurTimeSeries(pMnode);
  gStatus.curDnodes = grantGetClusterCurDnodes(pMnode);
  gStatus.curCpuCores = grantGetClusterCurCores(pMnode);
  gStatus.curStreams = grantGetClusterCurStreams(pMnode);
  gStatus.curTopics = grantGetClusterCurTopics(pMnode);
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
  grantDataInsSetDefault(gStatus.ins, CONN_TYPE_MAX);
}

static void grantDataInsSetDefault(SGrantDataIns *pIns, int32_t num) {
  if (grantClusterEpoch <= 0) grantClusterEpoch = grantGetClusterCreateTime(grantHandle.pMnode);
  SGrantDataIns in = {.number = GRANT_UNIQ_DFT_DATAIN_NUM,
                      .speed = GRANT_UNIQ_DFT_DATAIN_SPEED,
                      .expire = ceil((double)grantClusterEpoch / 86400) + GRANT_UNIQ_DFT_DATAIN_EXPIRE};

  for (int32_t i = 0; i < num; ++i) {
    *(pIns + i) = in;
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
  if (grantHandle.pMnode) gStatus.curDnodes = grantGetClusterCurDnodes(grantHandle.pMnode);
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
  if (grantHandle.pMnode) gStatus.curCpuCores = grantGetClusterCurCores(grantHandle.pMnode);
  if (gStatus.curCpuCores < gStatus.limitCpuCores) {
    return 0;
  }
  uError("grant failed to create dnode, exist:%" PRIu32 ", reason:grant cpu cores limited", gStatus.curCpuCores);
  return TSDB_CODE_GRANT_CPU_LIMITED;
}

int32_t grantCheck(EGrantType grant) {
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
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE bool grantIsValid(SGrantMsg *pStatus) {
  return pStatus->limitTimeSeries || IS_GRANT_TDENGINE(pStatus);
}
static FORCE_INLINE bool grantConnIsValid(SGrantMsg *pStatus) { return IS_GRANT_CONNECTORS(pStatus); }

static void grantStatusAssignLimits(SGrantStatus *p1, SGrantStatus *p2, bool isCombine) {
  if (isCombine) {
    // use larger value
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

static void grantConnStatusAssignLimits(void *g1, void *g2, bool isCombine) {
  SGrantStatus *p1 = g1;
  SGrantStatus *p2 = g2;
  if (isCombine) {
    // use larger value
    if (IS_GRANT_CONNECTORS_OFFICIAL(p2)) {
      SET_GRANT_CONNECTORS_OFFICIAL(p1);
    }
    for (int32_t i = 0; i < CONN_TYPE_MAX; ++i) {
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
    memcpy(GRANT_CONN_ITEMS(p1), GRANT_CONN_ITEMS(p2), sizeof(SGrantConnItem) * CONN_TYPE_MAX);
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
  SHashObj *pGrants = grantHandle.pOfficials;
  SArray   *pDists = grantHandle.pDistInfo;
  int32_t   distSize = taosArrayGetSize(pDists);

  if (distSize <= 0) return;

  if (distSize > 1) taosArraySort(pDists, grantConnDistCompare);
  uint32_t leastDist = GRANT_CONN_DIST(pDists, distSize - 1);

  SGrantStatus    status = {0};
  SGrantConnItem *pItems = GRANT_CONN_ITEMS(&status);
  GRANT_ITEMS_INIT(pItems);

  for (int32_t i = distSize; i > 0;) {
    SGrantDistInfo *pInfo = TARRAY_GET_ELEM(pDists, --i);
    if (pInfo->connDist < leastDist) break;
    SGrantStatus *pStatus = taosHashGet(pGrants, &pInfo->dnodeId, sizeof(pInfo->dnodeId));
    if (pStatus && IS_GRANT_CONNECTORS(pStatus)) {
      grantConnStatusAssignLimits(&status, pStatus, true);
      ++grantHandle.nConnGranted;
    }
  }

  // fill conn grant items from undef to default
  grantConnActiveFillUndef(pMnode, pItems);

  if (grantHandle.nConnGranted > 0) {
    SET_GRANT_CONNECTORS(&grantStatus);
    grantConnStatusAssignLimits(&gStatus, &status, false);
  }
}

static void grantStatusCheckImpl(SMnode *pMnode) {
  SHashObj *pGrants = grantHandle.pOfficials;
  SArray   *pDists = grantHandle.pDistInfo;
  int32_t   distSize = taosArrayGetSize(pDists);

  if (distSize <= 0) return;

  if (distSize > 1) taosArraySort(pDists, grantDistCompare);
  uint32_t leastDist = GRANT_GET_DIST(pDists, distSize - 1);

  SGrantStatus status = {0};
  for (int32_t i = distSize; i > 0;) {
    SGrantDistInfo *pInfo = TARRAY_GET_ELEM(pDists, --i);
    if (pInfo->dist < leastDist) break;
    SGrantStatus *pStatus = taosHashGet(pGrants, &pInfo->dnodeId, sizeof(pInfo->dnodeId));
    if (pStatus && IS_GRANT_TDENGINE(pStatus)) {
      grantStatusAssignLimits(&status, pStatus, true);
      ++grantHandle.nTaosdGranted;
    }
  }

  if (grantHandle.nTaosdGranted > 0) {
    SET_GRANT_TDENGINE(&grantStatus);
    grantStatusAssignLimits(&grantStatus, &status, false);
  }
}

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

static int32_t mndProcessDnodeSGrantMsg(SMnode *pMnode, SDnodeInfo *pDnodeInfo, SGrantUniqMsg *pGrantMsg,
                                        GrantStatus *pGrantStatus) {
  uint32_t curTime = taosGetTimestampMs() / 1000;
  if (pGrantMsg->machine[0] != 0) {
    const char *val = taosHashGet(grantHandle.pMachines, &pDnodeInfo->id, sizeof(pDnodeInfo->id));
    if (!val || 0 != strncmp(val, pGrantMsg->machine, GRANT_MACHINE_KEY_LEN + 1)) {
      taosHashPut(grantHandle.pMachines, &pDnodeInfo->id, sizeof(pDnodeInfo->id), &pGrantMsg->machine,
                  strlen(pGrantMsg->machine));
    }
  }

  if (pGrantMsg->pLegacy) {
    SGrantMsg *pLegacy = pGrantMsg->pLegacy;
    if (grantIsValid(pLegacy) || grantConnIsValid(pLegacy)) {
      SGrantStatus status = {0};
      if (grantIsValid(pLegacy)) {
        SET_GRANT_TDENGINE(&status);
        status.officialVersion = pLegacy->officialVersion;
        status.expireTimeSec = pLegacy->expireTimeSec;
        status.limitStorage = (uint64_t)(pLegacy->limitStorage) * (uint64_t)1073741824;
        status.limitSpeed = pLegacy->limitSpeed;
        status.limitTimeSeries = pLegacy->limitTimeSeries;
        status.limitQueryTime = pLegacy->limitQueryTime;
        status.limitDbs = pLegacy->limitDbs;
        status.limitUsers = pLegacy->limitUsers;
        status.limitConns = pLegacy->limitConns;
        status.limitStreams = pLegacy->limitStreams;
        status.limitAccts = pLegacy->limitAccts;
        status.limitDnodes = pLegacy->limitDnodes;
        status.limitCpuCores = pLegacy->limitCpuCores;
      }

      // assign the connectors
      if (grantConnIsValid(pLegacy)) {
        SET_GRANT_CONNECTORS(&status);
        GRANT_CONN_OFFICIAL(&status) = GRANT_CONN_OFFICIAL(pLegacy);
        memcpy(GRANT_CONN_ITEMS(&status), GRANT_CONN_ITEMS(pLegacy), sizeof(SGrantConnItem) * CONN_TYPE_MAX_V1);
        for (int32_t i = CONN_TYPE_MAX_V1; i < CONN_TYPE_MAX; ++i) {
          SGrantConnItem *pItem = GRANT_CONN_ITEM(&status, i);
          GRANT_CONN_ITEM_SET_UNDEF(pItem);
        }
      }

      taosHashPut(grantHandle.pOfficials, &pDnodeInfo->id, sizeof(TSDB_DATA_TYPE_INT), &status, sizeof(status));
      SGrantDistInfo distInfo = {
          .dist = pLegacy->distribute,
          .connDist = pLegacy->connectors.distribute,
          .dnodeId = pDnodeInfo->id,
      };
      taosArrayPush(grantHandle.pDistInfo, &distInfo);
    }
  }

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
  int32_t  code = TSDB_CODE_OUT_OF_MEMORY;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) goto _exit;

  if (tDecodeU64(&decoder, &pNotify->curTimeSeries) < 0) goto _exit;
  code = 0;
_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return code;
}

static int32_t tSerializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, SDnodeInfo *pInfo,
                                     int64_t clusterTime) {
  int32_t  code = TSDB_CODE_OUT_OF_MEMORY;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) goto _exit;

  // grant status
  if (tEncodeI64v(&encoder, pStatus->p1) < 0) goto _exit;
  if (tEncodeI64v(&encoder, pStatus->p2) < 0) goto _exit;
  if (tEncodeI64v(&encoder, pStatus->p3) < 0) goto _exit;
  if (tEncodeI64v(&encoder, pStatus->p4) < 0) goto _exit;
  if (tEncodeI64v(&encoder, pStatus->p5) < 0) goto _exit;
  if (tEncodeI64v(&encoder, pStatus->p6) < 0) goto _exit;
  if (tEncodeI64v(&encoder, pStatus->p7) < 0) goto _exit;

  if (tEncodeI64v(&encoder, pStatus->limitTimeSeries) < 0) goto _exit;
  if (tEncodeI64v(&encoder, pStatus->curTimeSeries) < 0) goto _exit;
  if (tEncodeI32v(&encoder, pStatus->limitCpuCores) < 0) goto _exit;
  if (tEncodeI32v(&encoder, pStatus->curCpuCores) < 0) goto _exit;

  if (tSerializeGrantDataIns(&encoder, pStatus->ins) < 0) goto _exit;  // optional

  if (tEncodeI64v(&encoder, clusterTime) < 0) goto _exit;
  if (tEncodeI32v(&encoder, pInfo->id) < 0) goto _exit;
  if (!pStatus->uniqActive) {
    if (tEncodeBinary(&encoder, pInfo->active, strlen(pInfo->active)) < 0) goto _exit;
    if (tEncodeBinary(&encoder, pInfo->connActive, strlen(pInfo->connActive)) < 0) goto _exit;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  code = 0;
_exit:
  tEncoderClear(&encoder);

  return code == 0 ? tlen : code;
}

int32_t tDeserializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, SDnodeInfo *pInfo,
                                int64_t *clusterTime) {
  int32_t  code = TSDB_CODE_OUT_OF_MEMORY;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) goto _exit;

  // grant status
  if (tDecodeI64v(&decoder, &pStatus->p1) < 0) goto _exit;
  if (tDecodeI64v(&decoder, &pStatus->p2) < 0) goto _exit;
  if (tDecodeI64v(&decoder, &pStatus->p3) < 0) goto _exit;
  if (tDecodeI64v(&decoder, &pStatus->p4) < 0) goto _exit;
  if (tDecodeI64v(&decoder, &pStatus->p5) < 0) goto _exit;
  if (tDecodeI64v(&decoder, &pStatus->p6) < 0) goto _exit;
  if (tDecodeI64v(&decoder, &pStatus->p7) < 0) goto _exit;

  if (tDecodeI64v(&decoder, &pStatus->limitTimeSeries) < 0) goto _exit;
  if (tDecodeI64v(&decoder, &pStatus->curTimeSeries) < 0) goto _exit;
  if (tDecodeI32v(&decoder, &pStatus->limitCpuCores) < 0) goto _exit;
  if (tDecodeI32v(&decoder, &pStatus->curCpuCores) < 0) goto _exit;

  if (tDeserializeGrantDataIns(&decoder, pStatus->ins) < 0) goto _exit; // optional

  if (tDecodeI64v(&decoder, clusterTime) < 0) goto _exit;
  if (tDecodeI32v(&decoder, &pInfo->id) < 0) goto _exit;
  if (!pStatus->uniqActive) {
    char   *data = NULL;
    int32_t dataLen = 0;
    if (tDecodeBinary(&decoder, (uint8_t **)&data, &dataLen) < 0) goto _exit;
    if (data && dataLen) tstrncpy(pInfo->active, data, TSDB_ACTIVE_KEY_LEN);
    data = NULL;
    dataLen = 0;
    if (tDecodeBinary(&decoder, (uint8_t **)&data, &dataLen) < 0) goto _exit;
    if (data && dataLen) tstrncpy(pInfo->connActive, data, TSDB_CONN_ACTIVE_KEY_LEN);
  }
  code = 0;
_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return code;
}

int32_t tSerializeGrantMsg(void *buf, int32_t bufLen, SGrantUniqMsg *pMsg) {
  int32_t  code = TSDB_CODE_OUT_OF_MEMORY;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) goto _exit;

  if (tEncodeI8(&encoder, pMsg->flag) < 0) goto _exit;
  if (tEncodeI32(&encoder, pMsg->dnodeId) < 0) goto _exit;
  if (tEncodeI32(&encoder, pMsg->diskCfgNum) < 0) goto _exit;
  int32_t len = strlen(pMsg->machine);
  if (tEncodeI32v(&encoder, len) < 0) goto _exit;
  if (len > 0 && tEncodeBinary(&encoder, pMsg->machine, len) < 0) goto _exit;

  if (pMsg->pLegacy) {
    SGrantMsg *pLegacy = pMsg->pLegacy;
    if (tEncodeI8(&encoder, pLegacy->flag) < 0) goto _exit;
    if (tEncodeI8(&encoder, pLegacy->officialVersion ? 1 : 0) < 0) goto _exit;
    if (tEncodeU32(&encoder, pLegacy->expireTimeSec) < 0) goto _exit;
    if (tEncodeU32(&encoder, pLegacy->limitStorage) < 0) goto _exit;
    if (tEncodeU32(&encoder, pLegacy->limitSpeed) < 0) goto _exit;
    if (tEncodeU64(&encoder, pLegacy->limitTimeSeries) < 0) goto _exit;
    if (tEncodeU32(&encoder, pLegacy->limitQueryTime) < 0) goto _exit;
    if (tEncodeU32(&encoder, pLegacy->limitDbs) < 0) goto _exit;
    if (tEncodeU32(&encoder, pLegacy->limitUsers) < 0) goto _exit;
    if (tEncodeU32(&encoder, pLegacy->limitConns) < 0) goto _exit;
    if (tEncodeU32(&encoder, pLegacy->limitStreams) < 0) goto _exit;
    if (tEncodeU32(&encoder, pLegacy->limitAccts) < 0) goto _exit;
    if (tEncodeU32(&encoder, pLegacy->limitDnodes) < 0) goto _exit;
    if (tEncodeU32(&encoder, pLegacy->limitCpuCores) < 0) goto _exit;
    if (tEncodeU32(&encoder, pLegacy->reserveKey1) < 0) goto _exit;
    if (tEncodeU32(&encoder, pLegacy->reserveKey2) < 0) goto _exit;
    if (tSerializeGrantConnMsg(&encoder, &pLegacy->connectors) < 0) goto _exit;
  }

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  code = 0;
_exit:
  tEncoderClear(&encoder);
  return code == 0 ? tlen : code;
}

int32_t tDeserializeGrantMsg(void *buf, int32_t bufLen, SGrantUniqMsg *pMsg) {
  int32_t  code = TSDB_CODE_OUT_OF_MEMORY;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) goto _exit;

  if (tDecodeI8(&decoder, &pMsg->flag) < 0) goto _exit;
  if (tDecodeI32(&decoder, &pMsg->dnodeId) < 0) goto _exit;
  if (tDecodeI32(&decoder, &pMsg->diskCfgNum) < 0) goto _exit;
  int32_t len = 0;
  if (tDecodeI32v(&decoder, &len) < 0) goto _exit;
  if (len > 0) {
    char *data = NULL;
    if (tDecodeBinary(&decoder, (uint8_t **)&data, NULL) < 0) goto _exit;
    if (data) strncpy(pMsg->machine, data, len);
  }

  if (IS_GRANT_LEGACY(pMsg)) {
    if(!(pMsg->pLegacy = taosMemoryCalloc(1, sizeof(SGrantMsg)))) goto _exit;
    SGrantMsg *pLegacy = pMsg->pLegacy;

    if (tDecodeI8(&decoder, (int8_t *)&pLegacy->flag) < 0) goto _exit;
    if (tDecodeI8(&decoder, (int8_t *)&pLegacy->officialVersion) < 0) goto _exit;
    if (tDecodeU32(&decoder, &pLegacy->expireTimeSec) < 0) goto _exit;
    if (tDecodeU32(&decoder, &pLegacy->limitStorage) < 0) goto _exit;
    if (tDecodeU32(&decoder, &pLegacy->limitSpeed) < 0) goto _exit;
    if (tDecodeU64(&decoder, &pLegacy->limitTimeSeries) < 0) goto _exit;
    if (tDecodeU32(&decoder, &pLegacy->limitQueryTime) < 0) goto _exit;
    if (tDecodeU32(&decoder, &pLegacy->limitDbs) < 0) goto _exit;
    if (tDecodeU32(&decoder, &pLegacy->limitUsers) < 0) goto _exit;
    if (tDecodeU32(&decoder, &pLegacy->limitConns) < 0) goto _exit;
    if (tDecodeU32(&decoder, &pLegacy->limitStreams) < 0) goto _exit;
    if (tDecodeU32(&decoder, &pLegacy->limitAccts) < 0) goto _exit;
    if (tDecodeU32(&decoder, &pLegacy->limitDnodes) < 0) goto _exit;
    if (tDecodeU32(&decoder, &pLegacy->limitCpuCores) < 0) goto _exit;
    if (tDecodeU32(&decoder, &pLegacy->reserveKey1) < 0) goto _exit;
    if (tDecodeU32(&decoder, &pLegacy->reserveKey2) < 0) goto _exit;
    if (tDeserializeGrantConnMsg(&decoder, &pLegacy->connectors) < 0) goto _exit;
  }
  code = 0;
_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return code;
}

static int32_t tSerializeGrantDataIns(SEncoder *encoder, SGrantDataIns *pIns) {
  if (tEncodeU8(encoder, CONN_TYPE_MAX) < 0) return -1;
  for (int32_t i = 0; i < CONN_TYPE_MAX; ++i) {
    SGrantDataIns *pIn = pIns + i;
    if (tEncodeI32v(encoder, pIn->number) < 0) return -1;
    if (tEncodeI32v(encoder, pIn->speed) < 0) return -1;
    if (tEncodeI32v(encoder, pIn->expire) < 0) return -1;
  }
  return 0;
}

static int32_t tDeserializeGrantDataIns(SDecoder *decoder, SGrantDataIns *pIns) {
  uint8_t       nIns = 0;
  SGrantDataIns in;
  if (tDecodeU8(decoder, &nIns) < 0) return -1;
  for (int32_t i = 0; i < nIns; ++i) {
    SGrantDataIns *pIn = pIns + i;
    if (i >= CONN_TYPE_MAX) {
      pIn = &in;
    }
    if (tDecodeI32v(decoder, &pIn->number) < 0) return -1;
    if (tDecodeI32v(decoder, &pIn->speed) < 0) return -1;
    if (tDecodeI32v(decoder, &pIn->expire) < 0) return -1;
  }
  if (nIns < CONN_TYPE_MAX) {
    grantDataInsSetDefault(pIns + nIns, CONN_TYPE_MAX - nIns);
  }
  return 0;
}

static int32_t tSerializeGrantConnMsg(SEncoder *encoder, SGrantConnMsg *pMsg) {
  if (tEncodeU8(encoder, pMsg->officialVersion) < 0) return -1;
  if (tEncodeU32(encoder, pMsg->distribute) < 0) return -1;
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
  uint8_t        nItems = 0;
  SGrantConnItem item;

  if (tDecodeU8(decoder, (uint8_t *)&pMsg->officialVersion) < 0) return -1;
  if (tDecodeU32(decoder, &pMsg->distribute) < 0) return -1;
  if (tDecodeU8(decoder, &nItems) < 0) return -1;
  for (int32_t i = 0; i < nItems; ++i) {
    SGrantConnItem *pItem = pMsg->items + i;
    if (nItems >= CONN_TYPE_MAX_V1) {
      pItem = &item;
    }
    if (tDecodeI32v(decoder, &pItem->number) < 0) return -1;
    if (tDecodeI16v(decoder, &pItem->speed) < 0) return -1;
    if (tDecodeU16v(decoder, &pItem->expire) < 0) return -1;
  }

  return 0;
}
