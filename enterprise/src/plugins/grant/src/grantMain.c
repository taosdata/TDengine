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
#include "tchecksum.h"
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
    if ((expireSec) != GRANT_UNIQ_UNLIMITED) {         \
      grantSecondsToString((expireSec), ts);           \
      src = ts;                                        \
    } else {                                           \
      src = GRANT_UNIQ_UNLIMITED_S;                    \
    }                                                  \
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));    \
    colDataSetVal(pColInfo, numOfRows, tmp, false);    \
  } while (0)

#define GRANT_ITEM_SHOW(cur, limit, unit)                                        \
  do {                                                                           \
    ++cols;                                                                      \
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);                           \
    if ((limit) != GRANT_UNIQ_UNLIMITED) {                                       \
      if ((unit) <= 32) {                                                        \
        sprintf(tmp1, "%d/%d", (int32_t)(cur), (int32_t)(limit));                \
      } else {                                                                   \
        sprintf(tmp1, "%" PRIi64 "/%" PRIi64, (int64_t)(cur), (int64_t)(limit)); \
      }                                                                          \
      src = tmp1;                                                                \
    } else {                                                                     \
      src = "unlimited";                                                         \
    }                                                                            \
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));                              \
    colDataSetVal(pColInfo, numOfRows, tmp, false);                              \
  } while (0)

// #define GRANT_DATA_IN_SHOW(appType)                                                                                    \
//   do {                                                                                                                 \
//     ++cols;                                                                                                            \
//     pColInfo = taosArrayGet(pBlock->pDataBlock, cols);                                                                 \
//     pDataIn = GRANT_DATA_IN(&gStatus, (appType));                                                                      \
//     grantSecondsToString((int64_t)pDataIn->expire * 86400, ts);                                                        \
//     sprintf(tmp1,                                                                                                      \
//             "{\"type\":\"%s\",\"number\":%d,\"speed\":%" PRIi16 ",\"expire\":\"%" PRIu16 "\", \"expireTime\":\"%s\"}", \
//             gConnName[(appType)], pDataIn->number, pDataIn->speed, pDataIn->expire, ts);                               \
//     STR_WITH_SIZE_TO_VARSTR(tmp, tmp1, strlen(tmp1));                                                                  \
//     colDataSetVal(pColInfo, numOfRows, tmp, false);                                                                    \
//   } while (0)

#define GRANT_VALUE_CONVERT(from, to, factor, dft) \
  do {                                             \
    if ((from) == GRANT_UNIQ_UNDEFINED) {          \
      (to) = (dft) * (factor);                     \
    } else if ((from) == GRANT_UNIQ_UNLIMITED) {   \
      (to) = (from);                               \
    } else {                                       \
      (to) = (from) * (factor);                    \
    }                                              \
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
// uniq grant
// #define GRANT_DATA_IN(s, i) ((s)->ins + i)
#define GRANT_IS_IVLD_MACHINE() ((grantHandle.info & 0x01) != 0)
#define GRANT_SET_IVLD_MACHINE() (grantHandle.info |= 0x01)
#define GRANT_SET_VLD_MACHINE() (grantHandle.info |= 0x0)

#define GRANT_DIST_TOLERENCE 86400  // seconds
#define GRANT_TS_SEC_LEN 20

static const char gConnName[CONN_TYPE_DYN_MAX][GRANT_ITEM_NAME_LEN] = {
    "opc_da", "opc_ua", "pi", "kafka", "influxdb", "mqtt", "avevahistorian", "opentsdb", "td2.6", "td3.0"};

static const char *gConnDisplay[CONN_TYPE_DYN_MAX] = {
    "OPC_DA", "OPC_UA", "Pi", "Kafka", "InfluxDB", "MQTT", "avevaHistorian", "OpenTSDB", "TDengine2.6", "TDengine3.0"};

static const char gGrantName[GRANT_OPT_DYN_MAX][GRANT_ITEM_NAME_LEN] = {
    "basic", "service", "stream", "subscription", "audit", "csv", "view", "storage", "backup_restore"};

static const char *gGrantDisplay[GRANT_OPT_DYN_MAX] = {
    "basic", "service", "stream", "subscription", "audit", "csv", "view", "multi_tier_storage", "backup_restore"};

static const char *gGrantState[GRANT_STATE_MAX] = {"ungranted", "ungranted", "granted", "expired",
                                                   "revoked"};  // keep 0/1 ungranted

static const char *tGetConnDisplay(const char *name) {
  for (int32_t i = CONN_TYPE_MAX; i < CONN_TYPE_DYN_MAX; ++i) {
    if (strncasecmp(gConnName[i], name, GRANT_ITEM_NAME_LEN) == 0) {
      return gConnDisplay[i];
    }
  }
  return "";
}

static const char *tGetGrantDisplay(const char *name) {
  for (int32_t i = GRANT_OPT_MAX; i < GRANT_OPT_DYN_MAX; ++i) {
    if (strncasecmp(gGrantName[i], name, GRANT_ITEM_NAME_LEN) == 0) {
      return gGrantDisplay[i];
    }
  }
  return "";
}

SGrantStatus gStatus = {
    .limitDnodes = GRANT_UNIQ_UNLIMITED,
    .limitTimeSeries = GRANT_UNIQ_UNLIMITED,
    .limitCpuCores = GRANT_UNIQ_UNLIMITED,
    .limitStreams = GRANT_UNIQ_UNLIMITED,
    .limitSubscriptions = GRANT_UNIQ_UNLIMITED,
    .limitViews = GRANT_UNIQ_UNLIMITED,

    .basicExpireSec = GRANT_UNIQ_UNLIMITED,
    .streamExpireSec = GRANT_UNIQ_UNLIMITED,
    .subscriptionExpireSec = GRANT_UNIQ_UNLIMITED,
    .multiTierExpireSec = GRANT_UNIQ_UNLIMITED,
    .auditExpireSec = GRANT_UNIQ_UNLIMITED,
    .csvExpireSec = GRANT_UNIQ_UNLIMITED,
    .bakRstExpireSec = GRANT_UNIQ_UNLIMITED,
    .viewExpireSec = GRANT_UNIQ_UNLIMITED,
    .revokedExpireSec = GRANT_UNIQ_UNLIMITED,
};

typedef SGrantNotify GrantNotify;
typedef SGrantStatus GrantStatus;

extern SGrantUniqObj grantObj;
extern char          tsVersionName[16];
extern int64_t       tsExpireTime;

static int32_t grantSecondsToString(int64_t seconds, char *ts);
static void    dmRefreshGrantCfg();
static void    grantRetrieveGrantInfo(SMnode *pMnode);
static void    grantResetMaster(SMnode *pMnode);
static void    grantSetClusterInfo(SMnode *pMnode);
static void    grantObjInit(SGrantUniqObj *pObj, bool official);
static void    grantStatusInit(SGrantStatus *pStatus);
static int64_t grantGetClusterCreateTime(SMnode *pMnode);
static int32_t mndProcessGrantHB(SRpcMsg *pReq);
static int32_t tSerializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, int64_t clusterTime);
static int32_t tDeserializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, int64_t *clusterTime);
static int32_t tSerializeGrantNotify(void *buf, int32_t bufLen, GrantNotify *pNotify);
static int32_t tDeserializeGrantNotify(void *buf, int32_t bufLen, GrantNotify *pNotify);
static int64_t grantGetClusterCurTimeSeries(SMnode *pMnode);
static void    grantStatusCheck(SMnode *pMnode, uint32_t curTime, SDnodeInfo *pDnodeInfo);

static int32_t mndRetrieveGrant(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextGrant(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveGrantFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextGrantFull(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveGrantLog(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextGrantLog(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveMachines(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextMachines(SMnode *pMnode, void *pIter);

static int32_t tGrantConnItemsNum(int8_t version);
static void    grantDataInsSetDefault(int32_t *pDataIns, int32_t num);
static int32_t tSerializeGrantDataIns(SEncoder *encoder, int32_t *pIns);
static int32_t tDeserializeGrantDataIns(SDecoder *decoder, int32_t *pIns);
static int32_t tSerializeGrantDynDataIns(SEncoder *encoder, SArray *pIns);
static int32_t tDeserializeGrantDynDataIns(SDecoder *decoder, SArray *pIns);

typedef struct {
  SSHashObj     *pMachines;
  SArray        *pDnodeInfo;
  SMnode        *pMnode;
  int64_t        lastCheck;
  int16_t        nServer;
  uint8_t        info;
  TdThreadRwlock rwLock;
} SGrantHandle;

static bool         recheckClusterTime = true;
static int64_t      grantNotifyCnt = 0;
static int64_t      grantNotifyTimeSeries = INT64_MAX;
static int64_t      grantClusterEpoch = 0;
static SGrantHandle grantHandle = {.lastCheck = INT64_MIN};

int32_t mndInitGrant(SMnode *pMnode) {
  terrno = 0;
  tsGrantHBInterval = 1;
  grantHandle.pMnode = pMnode;

  mndSetMsgHandle(pMnode, TDMT_MND_GRANT_HB_TIMER, mndProcessGrantHB);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndRetrieveGrant);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndCancelGetNextGrant);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS_FULL, mndRetrieveGrantFull);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_GRANTS_FULL, mndCancelGetNextGrantFull);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS_LOG, mndRetrieveGrantLog);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_GRANTS_LOG, mndCancelGetNextGrantLog);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_MACHINES, mndRetrieveMachines);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_MACHINES, mndCancelGetNextMachines);

  SSdbTable table = {
      .sdbType = SDB_GRANT,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndGrantActionEncode,
      .decodeFp = (SdbDecodeFp)mndGrantActionDecode,
      .insertFp = (SdbInsertFp)mndGrantActionInsert,
      .updateFp = (SdbUpdateFp)mndGrantActionUpdate,
      .deleteFp = (SdbDeleteFp)mndGrantActionDelete,
  };

  if (sdbSetTable(pMnode->pSdb, table) != 0) {
    goto _exit;
  }

  grantSetClusterInfo(pMnode);

  if (!(grantHandle.pMachines = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY)))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  if (!(grantHandle.pDnodeInfo = taosArrayInit(0, sizeof(SDnodeInfo)))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  taosThreadRwlockInit(&grantHandle.rwLock, NULL);

_exit:
  if (terrno != 0) {
    uError("grant data initialize failed since %s", tstrerror(terrno));
    mndCleanupGrant(pMnode);
  } else {
    uDebug("grant data is initialized");
  }

  return terrno;
}

void tResetGrantUniqObj(SGrantUniqObj *pObj) {
  if (grantObj.active) grantObj.active[0] = 0;
  if (grantObj.historicalActive) grantObj.historicalActive[0] = 0;
  taosArrayClear(grantObj.pMachines);
  taosArrayClear(grantObj.pDataIns);
  taosArrayClear(grantObj.pItem32);
  taosArrayClear(grantObj.pItem64);
}

static void tDestroyGrantStatus(SGrantStatus *pStatus) {
  if (pStatus) {
    taosArrayDestroy(pStatus->pDataIns);
    taosArrayDestroy(pStatus->pItem32);
    taosArrayDestroy(pStatus->pItem64);
  }
}

void mndCleanupGrant(SMnode *pMnode) {
  tSimpleHashCleanup(grantHandle.pMachines);
  taosArrayDestroy(grantHandle.pDnodeInfo);
  taosThreadRwlockDestroy(&grantHandle.rwLock);
  grantHandle.pMachines = NULL;
  grantHandle.pDnodeInfo = NULL;
  grantHandle.pMnode = NULL;

  tDestroyGrantUniqObj(&grantObj);
  tDestroyGrantStatus(&gStatus);
}

static void grantObjInit(SGrantUniqObj *pObj, bool official) {
  pObj->flags = 0;
  for (int32_t i = 0; i < GRANT_UNIQ_TOKEN_NUM; ++i) {
    pObj->token[i] = 0;
  }
  pObj->distribute = 0;
  pObj->granted = 0;
  pObj->officialVersion = official ? 1 : 0;
  pObj->validDays = GRANT_UNIQ_UNDEFINED;
  pObj->version = GRANT_UNIQ_ACTIVE_VER;
  pObj->limitTimeSeries = GRANT_UNIQ_UNDEFINED;
  pObj->limitCpuCores = GRANT_UNIQ_UNDEFINED;
  pObj->limitDnodes = GRANT_UNIQ_UNDEFINED;
  pObj->limitStreams = GRANT_UNIQ_UNDEFINED;
  pObj->limitSubscriptions = GRANT_UNIQ_UNDEFINED;
  pObj->reserve = 0;
  pObj->limitViews = GRANT_UNIQ_UNDEFINED;
  for (int32_t i = GRANT_OPT_BASIC; i < GRANT_OPT_MAX; ++i) {
    pObj->expireDays[i] = GRANT_UNIQ_UNDEFINED;
  }
  for (int32_t i = 0; i < GRANT_UNIQ_KNOWN_DATAIN_VALS; ++i) {
    pObj->dataIns[i] = GRANT_UNIQ_UNDEFINED;
  }
  taosArrayClear(pObj->pDataIns);
  taosArrayClear(pObj->pItem32);
  taosArrayClear(pObj->pItem64);
  taosArrayClear(pObj->pMachines);
}

static int64_t grantGetExpireSec(int64_t expireSec) {
  if (expireSec >= GRANT_UNIQ_UNLIMITED) {
    return expireSec;
  }

  if (expireSec == GRANT_UNIQ_UNDEFINED) {
    return expireSec = grantClusterEpoch + GRANT_DEFAULT;
  }
  ASSERTS(0, "invalid expireSec:%" PRIi64, expireSec);
  return expireSec = grantClusterEpoch + GRANT_DEFAULT;
}

static void grantSetClusterInfo(SMnode *pMnode) {
  if (strncmp(tsVersionName, GRANT_VERSION, tListLen(tsVersionName)) != 0) {
    tstrncpy(tsVersionName, GRANT_VERSION, tListLen(tsVersionName));
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

static FORCE_INLINE void grantSetClusterId(SMnode *pMnode, char *pClusterId) {
  if ((*pClusterId == 0) && pMnode) {
    int64_t clusterId = mndGetClusterId(pMnode);
    if (clusterId > 0) {
      snprintf(pClusterId, GRANT_CLUSTER_ID_LEN + 1, "%" PRIi64, clusterId);
    }
  }
}

static void grantSetActiveCodes(SDnodeInfo *pInfo, SGrantBasicObj *pObj, SGrantConnObj *pConnObj) {
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
  char tbuf[40] = {0};
  TRACE_TO_STR(&pMsg->info.traceId, tbuf);

  terrno = 0;

  if (!pMsg->pCont || (pMsg->contLen <= 0)) {
    terrno = TSDB_CODE_INVALID_MSG;
    uWarn("failed to process grant req in dnode since msg is empty, gtid:%s", tbuf);
    goto _err;
  }
  // step 1: process grant status from mnode
  GrantStatus grantStatusReq = {0};
  int64_t     clusterTime = 0;
  if (tDeserializeGrantStatus(pMsg->pCont, pMsg->contLen, &grantStatusReq, &clusterTime) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto _err;
  }

  // step 2: set local dnode grant status
  SArray *pDataIns = gStatus.pDataIns;
  SArray *pItem32 = gStatus.pItem32;
  SArray *pItem64 = gStatus.pItem64;

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

  uDebug("succeed to process grant req in dnode, gtid:%s", tbuf);

  return TSDB_CODE_SUCCESS;
_err:
  pMsg->code = terrno;
  pMsg->info.rsp = NULL;
  pMsg->info.rspLen = 0;

  uWarn("failed to process grant req in dnode since %s, gtid:%s", tstrerror(terrno), tbuf);

  return TSDB_CODE_FAILED;
}

static void dmRefreshGrantCfg(SGrantBasicObj *pObj, SGrantConnObj *pConnObj) {
  char cfgFile[PATH_MAX] = {0};
#ifdef CUS_PROMPT
  sprintf(cfgFile, "%s/%s.cfg", configDir, CUS_PROMPT);
#else
  sprintf(cfgFile, "%s/taos.cfg", configDir);
#endif
  grantActiveSystem(cfgFile, pObj, pConnObj);
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

static int32_t mndSendGrantStatusToDnode(SMnode *pMnode, SDnodeInfo *pDnodeInfo, int32_t contLen, void *pCont) {
  // send grant status to dnode
  SRpcMsg rpcMsg = {
      .pCont = pCont, .contLen = contLen, .msgType = TDMT_MND_GRANT, .info.ahandle = (void *)0x818, .info.noResp = 1};

  SEpSet epSet = {.numOfEps = 1};
  tstrncpy(epSet.eps[0].fqdn, pDnodeInfo->ep.fqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = pDnodeInfo->ep.port;

  if ((terrno = tmsgSendReq(&epSet, &rpcMsg)) != 0) {
    uWarn("failed to send grant status msg since %s", terrstr());
    return TSDB_CODE_FAILED;
  }

  return TSDB_CODE_SUCCESS;
_err:
  return TSDB_CODE_FAILED;
}

static void mndProcessGrantStatusCheck() {
  grantStatusCheck(grantHandle.pMnode, taosGetTimestampMs() / 1000, NULL);
  if (grantHandle.nServer > 0) {
    GRANT_SET_IVLD_MACHINE();
  } else {
    GRANT_SET_VLD_MACHINE();
  }

  bool   minHbInterval = false;
  int8_t grantExpireVal = GRANT_EXPIRE_VAL;
  if (grantExpireVal == 0) {
    if (0 == atomic_val_compare_exchange_8(&tsGrant, 0, 1)) {
      minHbInterval = true;
    }
  } else if (0 != atomic_load_8(&tsGrant)) {
    minHbInterval = true;
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

static int32_t grantCheckClusterInfo(SMnode *pMnode) {
  int32_t code = 0;
  if (recheckClusterTime) {
    int64_t clusterCreateTime = grantGetClusterCreateTime(pMnode);
    if (clusterCreateTime > 0) {
      COMPARE_SET_VAL(grantClusterEpoch, clusterCreateTime, !=);
      recheckClusterTime = false;
    } else {
      code = TSDB_CODE_APP_IS_STARTING;
    }
  }

  if (recheckClusterTime) {
    COMPARE_SET_VAL(tsGrantHBInterval, GRANT_HEART_BEAT_MIN, !=);
  } else {
    COMPARE_SET_VAL(tsGrantHBInterval, GRANT_HEART_BEAT_MSG, !=);
  }

  if (grantObj.clusterId[0] == 0) {
    grantSetClusterId(pMnode, grantObj.clusterId);
    if (grantObj.clusterId[0] == 0) {
      code = TSDB_CODE_APP_IS_STARTING;
    }
  }
_exit:
  if (code != 0) {
    recheckClusterTime = true;
  }
  return code;
}

static int32_t grantGetClusterMachines(SMnode *pMnode, SSHashObj *pRes) {
  SSdb      *pSdb = pMnode->pSdb;
  SDnodeObj *pDnode = NULL;
  void      *pIter = NULL;

  while ((pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode))) {
    int32_t klen = strlen(pDnode->machineId);
    if (klen == TSDB_MACHINE_ID_LEN) {
      tSimpleHashPut(pRes, pDnode->machineId, klen, &pDnode->id, sizeof(pDnode->id));
    }
    sdbRelease(pSdb, pDnode);
  }

  return 0;
}

static int32_t fillGrantStatusFromObj(SGrantStatus *pStatus, SGrantUniqObj *pObj, bool revoked) {
  int32_t clusterEpochDay = ceil((double)grantClusterEpoch / 86400);
  int32_t dftExpireDay = clusterEpochDay + GRANT_UNIQ_DFT_BASIC_EXPIRE;

  gStatus.officialVersion = grantObj.officialVersion;
  GRANT_VALUE_CONVERT(grantObj.expireDays[GRANT_OPT_BASIC], gStatus.basicExpireSec, 86400, dftExpireDay);
  GRANT_VALUE_CONVERT(grantObj.expireDays[GRANT_OPT_SERVICE], gStatus.serviceExpireSec, 86400, clusterEpochDay);
  GRANT_VALUE_CONVERT(grantObj.limitTimeSeries, gStatus.limitTimeSeries, 1, GRANT_UNIQ_DFT_BASIC_TIMESERIES);
  GRANT_VALUE_CONVERT(grantObj.limitDnodes, gStatus.limitDnodes, 1, GRANT_UNIQ_DFT_BASIC_DNODES);
  GRANT_VALUE_CONVERT(grantObj.limitCpuCores, gStatus.limitCpuCores, 1, GRANT_UNIQ_DFT_BASIC_CPU);
  GRANT_VALUE_CONVERT(grantObj.expireDays[GRANT_OPT_STREAM], gStatus.streamExpireSec, 86400, dftExpireDay);
  GRANT_VALUE_CONVERT(grantObj.limitStreams, gStatus.limitStreams, 1, GRANT_UNIQ_DFT_STREAM_NUM);
  GRANT_VALUE_CONVERT(grantObj.expireDays[GRANT_OPT_SUBSCRIPTION], gStatus.subscriptionExpireSec, 86400, dftExpireDay);
  GRANT_VALUE_CONVERT(grantObj.limitSubscriptions, gStatus.limitSubscriptions, 1, GRANT_UNIQ_DFT_SUBSCRIPTION_NUM);
  GRANT_VALUE_CONVERT(grantObj.limitViews, gStatus.limitViews, 1, GRANT_UNIQ_DFT_VIEW_NUM);
  GRANT_VALUE_CONVERT(grantObj.expireDays[GRANT_OPT_STORAGE], gStatus.multiTierExpireSec, 86400, dftExpireDay);
  GRANT_VALUE_CONVERT(grantObj.expireDays[GRANT_OPT_AUDIT], gStatus.auditExpireSec, 86400, dftExpireDay);
  GRANT_VALUE_CONVERT(grantObj.expireDays[GRANT_OPT_CSV], gStatus.csvExpireSec, 86400, dftExpireDay);
  GRANT_VALUE_CONVERT(grantObj.expireDays[GRANT_OPT_VIEW], gStatus.viewExpireSec, 86400, dftExpireDay);
  GRANT_VALUE_CONVERT(grantObj.expireDays[GRANT_OPT_DATA_BAK_RST], gStatus.bakRstExpireSec, 86400, dftExpireDay);
  
  for (int32_t i = 0; i < GRANT_UNIQ_KNOWN_DATAIN_VALS; i += 3) {
    GRANT_VALUE_CONVERT(grantObj.dataIns[i], gStatus.dataIns[i], 1, dftExpireDay);                         // expire
    GRANT_VALUE_CONVERT(grantObj.dataIns[i + 1], gStatus.dataIns[i + 1], 1, GRANT_UNIQ_DFT_DATAIN_SPEED);  // speed
    GRANT_VALUE_CONVERT(grantObj.dataIns[i + 2], gStatus.dataIns[i + 2], 1, GRANT_UNIQ_DFT_DATAIN_NUM);    // number
  }

  int64_t curTime = taosGetTimestampMs() / 1000;
  char    ts[GRANT_TS_SEC_LEN] = {0};
  int64_t grantCurTime = TMAX(curTime, GRANT_CUR_TIME);
  int64_t expireSec = revoked ? gStatus.revokedExpireSec : gStatus.basicExpireSec;
  if (expireSec > grantCurTime) {
    if (gStatus.expired) {
      gStatus.expired = 0;
    }
  } else {
    gStatus.expired = 1;
    grantSecondsToString(gStatus.basicExpireSec, ts);
    uWarn("grant cluster expired at %s %" PRIi64 ", curtime: %" PRIi64 ", set to %s state", ts, (int64_t)expireSec,
          grantCurTime, gGrantState[gStatus.grantState]);
  }

  // add rwlock since retrieve would access simultaneously
  taosThreadRwlockWrlock(&grantHandle.rwLock);
  int32_t nDataIn = taosArrayGetSize(pObj->pDataIns);
  if (nDataIn > 0) {
    void *tmp = pStatus->pDataIns;
    pStatus->pDataIns = pObj->pDataIns;
    pObj->pDataIns = tmp;
  } else {
    taosArrayClear(pStatus->pDataIns);
  }

  int32_t nItem32 = taosArrayGetSize(pObj->pItem32);
  if (nItem32 > 0) {
    void *tmp = pStatus->pItem32;
    pStatus->pItem32 = pObj->pItem32;
    pObj->pItem32 = tmp;
  } else {
    taosArrayClear(pStatus->pItem32);
  }

  int32_t nItem64 = taosArrayGetSize(pObj->pItem64);
  if (nItem64 > 0) {
    void *tmp = pStatus->pItem64;
    pStatus->pItem64 = pObj->pItem64;
    pObj->pItem64 = tmp;
  } else {
    taosArrayClear(pStatus->pItem64);
  }
  taosThreadRwlockUnlock(&grantHandle.rwLock);

  return 0;
}

static int32_t grantMachineCmprFn(const void *p1, const void *p2) {
  const void *m2 = &((SGrantMachine *)p2)->machine[0];
  return memcmp(p1, m2, TSDB_MACHINE_ID_LEN);
}

static int32_t grantCheckMachines(SGrantObj *pGrant, SArray **pGrantMachines, bool *toRevoked) {
  int32_t nDnodeLimit = gStatus.limitDnodes >= 0 ? gStatus.limitDnodes : INT32_MAX;
  int32_t nMachines = taosArrayGetSize(pGrant->pMachines);
  void   *pe = NULL;
  int32_t iter = 0;
  if (nMachines > 1 && pGrant->pMachines) taosArraySort(pGrant->pMachines, grantMachineCmprFn);
  if (nMachines < nDnodeLimit) {
    // append if not exist in SGrantObj, transfer to revoked state if exceeded
    int32_t idx = 0;
    void   *machines[128];
    int32_t dnodeIds[128];
    while ((pe = tSimpleHashIterate(grantHandle.pMachines, pe, &iter)) != NULL) {
      void *key = tSimpleHashGetKey(pe, NULL);
      if (!pGrant->pMachines || !taosArraySearch(pGrant->pMachines, key, grantMachineCmprFn, TD_EQ)) {
        machines[idx] = key;
        dnodeIds[idx] = *(int32_t *)pe;
        if (++idx >= 128) break;
      }
    }
    int32_t num = idx;
    if (nMachines + idx > nDnodeLimit) {
      if (toRevoked) *toRevoked = true;  // exceeded
      num = nDnodeLimit - nMachines;
    }
    if (num > 0) {
      *pGrantMachines = taosArrayInit(num, sizeof(SGrantMachine));
      int64_t curTime = taosGetTimestampMs() / 1000;
      for (int32_t i = 0; i < num; ++i) {
        taosArrayPush(*pGrantMachines, &(SGrantMachine){.id = dnodeIds[i], .ts = curTime});
        SGrantMachine *pLastMachine = taosArrayGetLast(*pGrantMachines);
        tstrncpy(pLastMachine->machine, machines[i], TSDB_MACHINE_ID_LEN + 1);
      }
    }

  } else if (nMachines == nDnodeLimit) {
    // if dnode machines all exist in cluster, it's ok; otherwise transfer to revoked state
    while ((pe = tSimpleHashIterate(grantHandle.pMachines, pe, &iter)) != NULL) {
      void *key = tSimpleHashGetKey(pe, NULL);
      if (!pGrant->pMachines || !taosArraySearch(pGrant->pMachines, key, grantMachineCmprFn, TD_EQ)) {
        if (toRevoked) *toRevoked = true;  // mismatch
        break;
      }
    }
  } else {
    // transfer to revoked if exceeded
    if (toRevoked) *toRevoked = true;
  }

  return 0;
}

static int32_t mndProcessGrantHBSyncInfo(SMnode *pMnode, int8_t type) {
  int32_t    code = 0;
  int32_t    lino = 0;
  int64_t    curTime = taosGetTimestampMs() / 1000;
  bool       toRevoked = false;
  bool       granted = false;
  bool       stated = true;
  void      *pIter = NULL;
  SGrantObj *pGrant = NULL;
  SArray    *pGrantMachines = NULL;

  code = grantCheckClusterInfo(pMnode);
  TSDB_CHECK_CODE(code, lino, _exit);

  grantRetrieveGrantInfo(pMnode);

  grantGetClusterMachines(pMnode, grantHandle.pMachines);

  pGrant = mndAcquireGrant(pMnode, &pIter);
  if (!pGrant) {
    mndProcessUpdGrantLog(pMnode, NULL, pGrantMachines,
                          &(SGrantState){.state = GRANT_STATE_UNGRANTED, .reason = GRANT_STATE_REASON_INIT});
    tsGrantHBInterval = GRANT_HEART_BEAT_MIN;  // don't check machines for revoked state since
    goto _exit;
  }

  SGrantState *pLastState = pGrant->nStates > 0 ? &pGrant->states[pGrant->nStates - 1] : NULL;
  if (!pLastState) {
    stated = false;
    gStatus.grantState = GRANT_STATE_UNGRANTED;
  } else {
    gStatus.grantState = pLastState->state;
  }

  if (gStatus.grantState == GRANT_STATE_REVOKED) {
    gStatus.revokedExpireSec = pLastState->ts + GRANT_CHK_TOLERENCE;

    char ts[GRANT_TS_SEC_LEN] = {0};
    grantSecondsToString(gStatus.revokedExpireSec, ts);

    int64_t grantCurTime = TMAX(curTime, GRANT_CUR_TIME);
    if (gStatus.revokedExpireSec > grantCurTime) {
      if (gStatus.expired) {
        gStatus.expired = 0;
      }
    } else {
      gStatus.expired = 1;
      uWarn("grant cluster expired at %s %" PRIi64 ", curtime: %" PRIi64 ", set to %s state", ts,
            gStatus.revokedExpireSec, grantCurTime, gGrantState[gStatus.grantState]);
    }
    mndReleaseGrant(pMnode, pGrant, pIter);
    goto _exit;
  }

  grantObjInit(&grantObj, false);

  int16_t activeLen = pGrant->active ? strlen(pGrant->active) : 0;
  if (!grantObj.active) {
    char *tmp = taosMemoryRealloc(grantObj.active, activeLen + 1);
    if (!tmp) {
      mndReleaseGrant(pMnode, pGrant, pIter);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    grantObj.active = tmp;
    grantObj.activeBufLen = activeLen + 1;
  } else if (grantObj.activeBufLen < activeLen + 1) {
    char *tmp = taosMemoryRealloc(grantObj.active, activeLen + 1);
    if (!tmp) {
      mndReleaseGrant(pMnode, pGrant, pIter);
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    grantObj.active = tmp;
    grantObj.activeBufLen = activeLen + 1;
  }

  if (activeLen > 0) {
    if (0 != strncmp(grantObj.active, pGrant->active, activeLen + 1)) {
      tstrncpy(grantObj.active, pGrant->active, activeLen + 1);
    }
  } else {
    grantObj.active[0] = 0;
  }

  if (grantObj.active && grantObj.active[0] != 0) {
    if (0 != grantUniqParseActiveCode(&grantObj, NULL)) {
      grantResetMaster(pMnode);
    } else {
      granted = true;
      code = fillGrantStatusFromObj(&gStatus, &grantObj, toRevoked);
      TSDB_CHECK_CODE(code, lino, _exit);
    }
  }

  // check machines
#ifdef GRANTS_CFG
  if (!granted || (grantObj.flags & 0x02)) {
    grantCheckMachines(pGrant, &pGrantMachines, &toRevoked);
  }
#endif

  mndReleaseGrant(pMnode, pGrant, pIter);

  SGrantState state = {0};
  if (toRevoked) {
    state.state = GRANT_STATE_REVOKED;
    state.reason = GRANT_STATE_REASON_MISMATCH;
    code = mndProcessUpdGrantLog(pMnode, NULL, pGrantMachines, &state);
    TSDB_CHECK_CODE(code, lino, _exit);
    int64_t grantCurTime = TMAX(curTime, GRANT_CUR_TIME);
    int64_t expireSec = gStatus.revokedExpireSec;
    if (expireSec > grantCurTime) {
      if (gStatus.expired) {
        gStatus.expired = 0;
      }
    } else {
      gStatus.expired = 1;
      char ts[GRANT_TS_SEC_LEN] = {0};
      grantSecondsToString(expireSec, ts);
      uWarn("grant cluster expired at %s %" PRIi64 ", curtime: %" PRIi64 ", set to %s state", ts, (int64_t)expireSec,
            grantCurTime, gGrantState[gStatus.grantState]);
    }
  } else {
    int8_t oldState = gStatus.grantState;
    bool   appendState = false;
    if (oldState == GRANT_STATE_UNGRANTED) {
      if (granted) {
        if (gStatus.expired) {
          state.state = GRANT_STATE_EXPIRED;
          state.reason = GRANT_STATE_REASON_EXPIRE;
          appendState = true;
        } else {
          state.state = GRANT_STATE_GRANTED;
          state.reason = GRANT_STATE_REASON_ALTER;
          appendState = true;
        }
      } else if (stated = false) {
        state.state = GRANT_STATE_UNGRANTED;
        state.reason = GRANT_STATE_REASON_INIT;
        appendState = true;
      }
    } else if (oldState == GRANT_STATE_GRANTED) {
      if (gStatus.expired) {
        state.state = GRANT_STATE_EXPIRED;
        state.reason = GRANT_STATE_REASON_EXPIRE;
        appendState = true;
      }
    } else if (oldState == GRANT_STATE_EXPIRED) {
      if (gStatus.expired == false) {
        state.state = GRANT_STATE_GRANTED;
        state.reason = GRANT_STATE_REASON_ALTER;
        appendState = true;
      }
    }
    code = mndProcessUpdGrantLog(pMnode, NULL, pGrantMachines, appendState ? &state : NULL);
    TSDB_CHECK_CODE(code, lino, _exit);
  }

  // set cluster info after parse uniq active
  grantSetClusterInfo(pMnode);
_exit:
  taosArrayDestroy(pGrantMachines);
  if (code != 0) {
    uError("grant hb failed since %s", tstrerror(code));
  }
  return code;
}

static int32_t mndProcessGrantHBImpl(SMnode *pMnode, int8_t type) {
  if (!pMnode) {
    terrno = TSDB_CODE_INVALID_PTR;
    return -1;
  }

  mndProcessGrantHBSyncInfo(pMnode, type);

  // reset grantHandle and send gStatus to all dnodes, no resp needed
  taosArrayClear(grantHandle.pDnodeInfo);
  tSimpleHashClear(grantHandle.pMachines);
  grantHandle.nServer = 0;

  mndGetDnodeData(pMnode, grantHandle.pDnodeInfo);

  int32_t dnodeSize = taosArrayGetSize(grantHandle.pDnodeInfo);
  int64_t clusterTime = grantGetClusterCreateTime(pMnode) + mndGetClusterUpTime(pMnode);
  int32_t contLen = 0;
  void   *pCont = NULL;
  if (dnodeSize > 1) {
    // taosArraySort(grantHandle.pDnodeInfo, dnodeInfoCmprFn);
    contLen = tSerializeGrantStatus(NULL, 0, &gStatus, clusterTime);
    pCont = rpcMallocCont(contLen);
    if (!pCont) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      uWarn("failed to generate grant status msg since %s", terrstr());
      return TSDB_CODE_FAILED;
    }

    if (tSerializeGrantStatus(pCont, contLen, &gStatus, clusterTime) < 0) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      uWarn("failed to generate grant status msg when serialize since %s", terrstr());
      return TSDB_CODE_FAILED;
    }

    bool sent = false;
    for (int32_t i = 0; i < dnodeSize; ++i) {
      SDnodeInfo *info = (SDnodeInfo *)TARRAY_GET_ELEM(grantHandle.pDnodeInfo, i);
      if (info->offlineReason == DND_REASON_STATUS_MSG_TIMEOUT ||
          info->offlineReason == DND_REASON_STATUS_NOT_RECEIVED) {
        uDebug("not send grant status to dnode:%d since offline state:%d", info->id, info->offlineReason);
        continue;
      }

      if (tsServerPort == info->ep.port && 0 == strncmp(tsLocalFqdn, info->ep.fqdn, TSDB_FQDN_LEN)) {
        uDebug("not send grant status to dnode:%d since duplicated node", info->id);
        continue;
      }

      if (sent == false) {
        sent = true;
        mndSendGrantStatusToDnode(pMnode, info, contLen, pCont);
      } else {
        void *qCont = rpcMallocCont(contLen);
        if (!qCont) return TSDB_CODE_FAILED;
        memcpy(qCont, pCont, contLen);
        mndSendGrantStatusToDnode(pMnode, info, contLen, qCont);
      }
    }
    if (sent == false) {
      rpcFreeCont(pCont);
    }
  }

  grantHandle.lastCheck = taosGetTimestampMs();

  return 0;
}

/**
 * @brief process grant heartbeat msg from mnode
 *
 * @param pReq
 * @return int32_t
 */
static int32_t mndProcessGrantHB(SRpcMsg *pReq) {
  SMnode *pMnode = pReq ? pReq->info.node : grantHandle.pMnode;
  return mndProcessGrantHBImpl(pMnode, 0);
}

void grantParseParameter() {
#ifdef _TD_MIPS
  fprintf(stderr, "the MIPS platform does not support machine code currently!\n");
#else
  char *key = tGetMachineId();  //  grantGetMachineSerials();
  if (key != NULL) {
    fprintf(stdout, "machine code: %s \n", key);
    taosMemoryFree(key);
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
  int32_t    numOfCores = 0;

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

static int16_t grantGetClusterCurSubscriptions(SMnode *pMnode) {
  SSdb            *pSdb = pMnode->pSdb;
  SMqSubscribeObj *pSubscribe = NULL;
  void            *pIter = NULL;
  int16_t          numOfSubscriptions = 0;

  while ((pIter = sdbFetch(pSdb, SDB_SUBSCRIBE, pIter, (void **)&pSubscribe))) {
    ++numOfSubscriptions;
    sdbRelease(pSdb, pSubscribe);
  }

  return numOfSubscriptions;
}

static int32_t grantGetClusterCurViews(SMnode *pMnode) {
  SSdb     *pSdb = pMnode->pSdb;
  SViewObj *pView = NULL;
  void     *pIter = NULL;
  int32_t   numOfViews = 0;

  while ((pIter = sdbFetch(pSdb, SDB_VIEW, pIter, (void **)&pView))) {
    ++numOfViews;
    sdbRelease(pSdb, pView);
  }

  return numOfViews;
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
  gStatus.curSubscriptions = grantGetClusterCurSubscriptions(pMnode);
  gStatus.curViews = grantGetClusterCurViews(pMnode);
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
  tstrncpy(epSet.eps[0].fqdn, pDnodeInfo->ep.fqdn, TSDB_FQDN_LEN);
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
    gStatus.subscriptionExpireSec = gStatus.basicExpireSec;
    gStatus.subscriptionExpired = gStatus.basicExpired;
    gStatus.auditExpireSec = gStatus.basicExpireSec;
    gStatus.auditExpired = gStatus.basicExpired;
    gStatus.csvExpireSec = gStatus.basicExpireSec;
    gStatus.csvExpired = gStatus.basicExpired;
    gStatus.bakRstExpireSec = gStatus.basicExpireSec;
    gStatus.viewExpireSec = gStatus.basicExpireSec;
    gStatus.viewExpired = gStatus.basicExpired;

    char ts[GRANT_TS_SEC_LEN] = {0};
    grantSecondsToString(gStatus.basicExpireSec, ts);
    uInfo("grant expire time reset to %s %" PRIi64 ", current timeseries %" PRIi64, ts, (int64_t)gStatus.basicExpireSec,
          gStatus.curTimeSeries);
  }
#endif
  grantDataInsSetDefault(&gStatus.dataIns[0], GRANT_UNIQ_KNOWN_DATAIN_VALS);
}

static void grantDataInsSetDefault(int32_t *pDataIns, int32_t num) {
  if (grantClusterEpoch <= 0) grantClusterEpoch = grantGetClusterCreateTime(grantHandle.pMnode);
  // SGrantDataIns in = {.number = GRANT_UNIQ_DFT_DATAIN_NUM,
  //                     .speed = GRANT_UNIQ_DFT_DATAIN_SPEED,
  int32_t expire = ceil((double)grantClusterEpoch / 86400) + GRANT_UNIQ_DFT_DATAIN_EXPIRE;

  for (int32_t i = 0; i < num; i += 3) {
    *(pDataIns + i) = expire;                           // expire
    *(pDataIns + i + 1) = GRANT_UNIQ_DFT_DATAIN_SPEED;  // speed
    *(pDataIns + i + 2) = GRANT_UNIQ_DFT_DATAIN_NUM;    // number
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
  return TSDB_CODE_GRANT_STREAM_LIMITED;
}
static int32_t grantCheckSubscriptions() {
  ASSERTS(gStatus.limitSubscriptions != GRANT_UNIQ_UNDEFINED, "limitSubscription is %d", GRANT_UNIQ_UNDEFINED);
  if (!gStatus.subscriptionExpired &&
      (gStatus.limitSubscriptions == GRANT_UNIQ_UNLIMITED || gStatus.curSubscriptions < gStatus.limitSubscriptions)) {
    return 0;
  }
  uError("grant failed to check topic, expire:%" PRIi64 ", num:%d, reason:topic limited",
         (int64_t)gStatus.subscriptionExpireSec, (int32_t)gStatus.curSubscriptions);
  return TSDB_CODE_GRANT_SUBSCRIPTION_LIMITED;  // TODO
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
    case TSDB_GRANT_STREAMS:
      return grantCheckStreams();
    case TSDB_GRANT_CPU_CORES:
      return grantCheckCpuCores();
    case TSDB_GRANT_SUBSCRIPTION:
      return grantCheckSubscriptions();
    case TSDB_GRANT_STREAM_EXPIRE:
      return GRANT_EXPIRED(gStatus.streamExpired);
    case TSDB_GRANT_SUBSCRIPTION_EXPIRE:
      return GRANT_EXPIRED(gStatus.subscriptionExpired);
    case TSDB_GRANT_AUDIT_EXPIRE:
      return GRANT_EXPIRED(gStatus.auditExpired);
    case TSDB_GRANT_CSV_EXPIRE:
      return GRANT_EXPIRED(gStatus.csvExpired);
    case TSDB_GRANT_MULTI_TIER_EXPIRE:
      return GRANT_EXPIRED(gStatus.multiTierExpired);
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

static void grantStatusAssignLimits(SGrantStatus *p1, SGrantStatus *p2, bool isCombine) {
  // if (isCombine) {
  //   // use larger value
  //   if (p2->officialVersion) p1->officialVersion = p2->officialVersion;
  //   GRANT_ITEM_SET_VAL(p1->expireTimeSec, p2->expireTimeSec, GRANT_EXPIRE_TIME);
  //   GRANT_ITEM_SET_VAL(p1->limitStorage, p2->limitStorage, GRANT_STORAGE_LIMITS);
  //   GRANT_ITEM_SET_VAL(p1->limitSpeed, p2->limitSpeed, GRANT_WRITING_SPEED_LIMITS);
  //   GRANT_ITEM_SET_VAL(p1->limitTimeSeries, p2->limitTimeSeries, GRANT_TIME_SERIES_LIMITS);
  //   GRANT_ITEM_SET_VAL(p1->limitQueryTime, p2->limitQueryTime, GRANT_QUERY_TIME_LIMITS);
  //   GRANT_ITEM_SET_VAL(p1->limitDbs, p2->limitDbs, GRANT_DATABASE_LIMITS);
  //   GRANT_ITEM_SET_VAL(p1->limitUsers, p2->limitUsers, GRANT_USER_LIMITS);
  //   GRANT_ITEM_SET_VAL(p1->limitConns, p2->limitConns, GRANT_CONNECTION_LIMITS);
  //   GRANT_ITEM_SET_VAL(p1->limitStreams, p2->limitStreams, GRANT_STREAM_LIMITS);
  //   GRANT_ITEM_SET_VAL(p1->limitAccts, p2->limitAccts, GRANT_ACCT_LIMITS);
  //   GRANT_ITEM_SET_VAL(p1->limitDnodes, p2->limitDnodes, GRANT_DNODE_LIMITS);
  //   GRANT_ITEM_SET_VAL(p1->limitCpuCores, p2->limitCpuCores, GRANT_CPU_LIMITS);
  // } else {
  //   p1->officialVersion = p2->officialVersion;
  //   p1->expireTimeSec = p2->expireTimeSec;
  //   p1->limitStorage = p2->limitStorage;
  //   p1->limitSpeed = p2->limitSpeed;
  //   p1->limitTimeSeries = p2->limitTimeSeries;
  //   p1->limitQueryTime = p2->limitQueryTime;
  //   p1->limitDbs = p2->limitDbs;
  //   p1->limitUsers = p2->limitUsers;
  //   p1->limitConns = p2->limitConns;
  //   p1->limitStreams = p2->limitStreams;
  //   p1->limitAccts = p2->limitAccts;
  //   p1->limitDnodes = p2->limitDnodes;
  //   p1->limitCpuCores = p2->limitCpuCores;
  // }
}

static void grantStatusCheck(SMnode *pMnode, uint32_t curTime, SDnodeInfo *pDnodeInfo) {
  // for TDengine
  // grantStatusCheckImpl(pMnode);
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
  // grantConnStatusCheckImpl(pMnode);
}

static int32_t mndCfgDnodeReq(SDnodeInfo *pDnodeInfo, const char *cfg, const char *val) {
  SMCfgDnodeReq req = {0};
  req.dnodeId = pDnodeInfo->id;
  tstrncpy(req.config, cfg, TSDB_DNODE_CONFIG_LEN);
  tstrncpy(req.value, val, TSDB_DNODE_VALUE_LEN);

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
  tstrncpy(epSet.eps[0].fqdn, tsLocalFqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = tsServerPort;

  tmsgSendReq(&epSet, &rpcMsg);

  return TSDB_CODE_SUCCESS;
}

static int32_t machineCmprFn(const void *p1, const void *p2) { return memcmp(p1, p2, TSDB_MACHINE_ID_LEN); }

// mnode-write thread
int32_t grantAlterActiveCode(SMnode *pMnode, SGrantObj *pObj, const char *oldActive, const char *newActive,
                             char **mergeActive) {
  int32_t       code = 0;
  SGrantUniqObj newObj = {0};
  SGrantUniqObj oldObj = {0};
  SGrantUniqObj mergeObj = {0};
  SSHashObj    *pMachineHash = NULL;
  SArray       *pMachines = NULL;
  bool          revoked = false;

  // step 1: basic judgement and init
  if (!newActive || newActive[0] == 0) {
    code = TSDB_CODE_INVALID_PTR;
    goto _exit;
  }

  if (grantObj.clusterId[0] == 0) {
    grantSetClusterId(pMnode, grantObj.clusterId);
    if (grantObj.clusterId[0] == 0) {
      code = TSDB_CODE_APP_IS_STARTING;
      goto _exit;
    }
  }

  SGrantState lastState = {0};
  if (0 != (code = mndGrantGetLastState(pMnode, &lastState))) {
    if (code != TSDB_CODE_GRANT_OBJ_NOT_EXIST) {
      goto _exit;
    }
  } else if (lastState.state == GRANT_STATE_REVOKED) {
    revoked = true;
  }

  // duplication check
  for (int32_t i = 0; i < pObj->nActives; ++i) {
    if (0 == memcmp(&pObj->actives[i].active[0], newActive, GRANT_UNIQ_HEAD_LEN)) {
      code = TSDB_CODE_GRANT_DUPLICATED_ACTIVE;
      goto _exit;
    }
  }

  if (!(pMachineHash = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY)))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  grantGetClusterMachines(pMnode, pMachineHash);

  // step 2: parse new
  memcpy(newObj.clusterId, grantObj.clusterId, GRANT_CLUSTER_ID_LEN);
  grantObjInit(&newObj, 0);
  int32_t newActiveLen = strlen(newActive);
  if (!(newObj.active = taosMemoryMalloc(newActiveLen + 1))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  newObj.activeBufLen = newActiveLen + 1;
  tstrncpy(newObj.active, newActive, newActiveLen + 1);

  code = grantUniqParseActiveCode(&newObj, NULL);
  if (code != 0 || !newObj.granted) {
    code = code != 0 ? code : TSDB_CODE_GRANT_PAR_IVLD_ACTIVE;
    goto _exit;
  } else {
    int64_t curTime = taosGetTimestampMs() / 1000;
    if (newObj.validDays > 0) {
      if (curTime - newObj.distribute > (int64_t)newObj.validDays * 86400) {
        code = TSDB_CODE_GRANT_PAR_IVLD_DIST;
        goto _exit;
      }
    }

    int64_t basicExpire = newObj.expireDays[GRANT_OPT_BASIC];
    if (basicExpire != GRANT_UNIQ_UNDEFINED && basicExpire != GRANT_UNIQ_UNLIMITED) {
      int64_t grantCurTime = TMAX(curTime, GRANT_CUR_TIME);
      if (basicExpire * 86400 <= grantCurTime) {
        code = TSDB_CODE_GRANT_EXPIRED;
        goto _exit;
      }
    }
  }

  if (newObj.token[0] > 0) {  // check last active
    bool   found = false;
    int8_t nActive = pObj->nActives;
    while (--nActive >= 0) {
      TSCKSUM chksum = taosCalcChecksum(0, pObj->actives[nActive].active, GRANT_ACTIVE_HEAD_LEN);
      if (chksum == newObj.token[0]) {
        found = true;
        break;
      }
    }
    if (!found) {
      code = TSDB_CODE_GRANT_LAST_ACTIVE_NOT_FOUND;
      goto _exit;
    }
  }

  if (newObj.token[1] > 0) {  // check machines
    if (!(pMachines = taosArrayInit(tSimpleHashGetSize(pMachineHash), TSDB_MACHINE_ID_LEN))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    void   *pe = NULL;
    int32_t iter = 0;
    while ((pe = tSimpleHashIterate(pMachineHash, pe, &iter)) != NULL) {
      void *key = tSimpleHashGetKey(pe, NULL);
      taosArrayPush(pMachines, key);
    }
    int32_t nFinalMachine = taosArrayGetSize(pMachines);
    if (nFinalMachine > 1) taosArraySort(pMachines, machineCmprFn);

    TSCKSUM machineChksum = 0;
    if (nFinalMachine > 0) {
      machineChksum = taosCalcChecksum(0, TARRAY_GET_ELEM(pMachines, 0), nFinalMachine * TSDB_MACHINE_ID_LEN);
    }
    if (machineChksum != newObj.token[1]) {
      code = TSDB_CODE_GRANT_MACHINES_MISMATCH;
      goto _exit;
    }
    // cleanup pGrant->pMachines in revoked state
    if (revoked) taosArrayClear(pObj->pMachines);
  } else if (revoked) {
    code = TSDB_CODE_GRANT_UNLICENSED_CLUSTER;
    goto _exit;
  }

  grantRetrieveGrantInfo(pMnode);
  //  check grantItems of basic function
  if ((newObj.limitTimeSeries > GRANT_UNIQ_UNLIMITED) && (gStatus.curTimeSeries > newObj.limitTimeSeries)) {
    code = TSDB_CODE_GRANT_TIMESERIES_LIMITED;
    goto _exit;
  }
  if ((newObj.limitDnodes > GRANT_UNIQ_UNLIMITED) && (gStatus.curDnodes > newObj.limitDnodes)) {
    code = TSDB_CODE_GRANT_DNODE_LIMITED;
    goto _exit;
  }
  if ((newObj.limitCpuCores > GRANT_UNIQ_UNLIMITED) && (gStatus.curCpuCores > newObj.limitCpuCores)) {
    code = TSDB_CODE_GRANT_CPU_LIMITED;
    goto _exit;
  }

  // basicExpireDay = newObj.expireDays[GRANT_OPT_BASIC];

  // step 3: parse old
  memcpy(oldObj.clusterId, grantObj.clusterId, GRANT_CLUSTER_ID_LEN);
  grantObjInit(&oldObj, 0);
  if (oldActive) {
    int32_t oldActiveLen = strlen(oldActive);
    if (!(oldObj.active = taosMemoryMalloc(oldActiveLen + 1))) {
      code = TSDB_CODE_OUT_OF_MEMORY;
      goto _exit;
    }
    oldObj.activeBufLen = oldActiveLen + 1;
    tstrncpy(oldObj.active, oldActive, oldActiveLen + 1);
    code = grantUniqParseActiveCode(&oldObj, NULL);
    if (code != 0 || !oldObj.granted) {
      code = code != 0 ? code : TSDB_CODE_GRANT_PAR_IVLD_ACTIVE;
      if ((newObj.flags & 0x40)) {  // skip if old active parse failed
        uInfo("old active parse failed since %s, continue to alter as new flags:0x%x", tstrerror(code), oldObj.flags);
        code = 0;
      } else {
        code = code != 0 ? code : TSDB_CODE_GRANT_PAR_IVLD_ACTIVE;
        goto _exit;
      }
    }
  }

  if (oldObj.granted == 0 || lastState.state == GRANT_STATE_REVOKED || lastState.state == GRANT_STATE_EXPIRED) {
    if (newObj.expireDays[GRANT_OPT_BASIC] == GRANT_UNIQ_UNDEFINED || newObj.limitTimeSeries == GRANT_UNIQ_UNDEFINED ||
        newObj.limitDnodes == GRANT_UNIQ_UNDEFINED || newObj.limitCpuCores == GRANT_UNIQ_UNDEFINED) {
      code = TSDB_CODE_GRANT_LACK_OF_BASIC;
      goto _exit;
    }
  }

  // step 4: merge active code

  if (0 != (code = grantUniqMergeActiveCode(&oldObj, &newObj, &mergeObj, 1))) {
    goto _exit;
  }

  SGrantUniqObj *fromObj = mergeObj.granted ? &mergeObj : &newObj;
  if (0 != (code = fillGrantStatusFromObj(&gStatus, fromObj, false))) {
    goto _exit;
  }

  if (mergeObj.granted) {
    *mergeActive = mergeObj.active;
    mergeObj.active = NULL;
  }

  uInfo("succeed to alter grant active");

_exit:
  taosArrayDestroy(pMachines);
  tSimpleHashCleanup(pMachineHash);
  tDestroyGrantUniqObj(&mergeObj);
  tDestroyGrantUniqObj(&newObj);
  tDestroyGrantUniqObj(&oldObj);
  if (code != 0) {
    uError("failed to alter grant active:%s since %s", newActive, tstrerror(code));
  }
  return code;
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
    // SGrantDataIns *pDataIn = NULL;
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    const char      *src = GRANT_VERSION;
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    if (gStatus.grantState == GRANT_STATE_REVOKED) {
      GRANT_EXPIRE_SHOW(gStatus.revokedExpireSec);
    } else {
      GRANT_EXPIRE_SHOW(gStatus.basicExpireSec);
    }

    GRANT_EXPIRE_SHOW(gStatus.serviceExpireSec);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = gStatus.basicExpired || (gStatus.multiTierExpired && tsDiskCfgNum > 1) ? "true" : "false";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (gStatus.grantState < 0 || gStatus.grantState > GRANT_STATE_MAX) {
      src = "unknown";
    } else {
      src = gGrantState[gStatus.grantState];
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    GRANT_ITEM_SHOW(gStatus.curTimeSeries, gStatus.limitTimeSeries, 64);
    GRANT_ITEM_SHOW(gStatus.curDnodes, gStatus.limitDnodes, 16);
    GRANT_ITEM_SHOW(gStatus.curCpuCores, gStatus.limitCpuCores, 32);

    ++numOfRows;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextGrant(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

static int32_t mndRetrieveGrantFullItem(SSDataBlock *pBlock, int32_t *numOfRows, const char *name, const char *display,
                                        int64_t expire, int64_t curVal, int64_t limit, bool isDataIn) {
  int32_t cols = 0;
  char    tmp[192];
  char   *pBuf = &tmp[0];
  char   *qBuf = NULL;
  char    ts[GRANT_TS_SEC_LEN] = {0};

  SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
  qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
  snprintf(qBuf, 192, "%s", name);
  varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
  colDataSetVal(pColInfo, *numOfRows, pBuf, false);

  ++cols;
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
  qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
  snprintf(qBuf, 192, "%s", display);
  varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
  colDataSetVal(pColInfo, *numOfRows, pBuf, false);

  ++cols;
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
  qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
  if (expire == GRANT_UNIQ_UNLIMITED) {
    snprintf(qBuf, 192, GRANT_UNIQ_UNLIMITED_S);
  } else {
    grantSecondsToString(isDataIn ? expire * 86400 : expire, ts);
    snprintf(qBuf, 192, "%s", ts);
  }
  varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
  colDataSetVal(pColInfo, *numOfRows, pBuf, false);

  ++cols;
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
  qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
  if (isDataIn) {
    grantSecondsToString(expire * 86400, ts);
    snprintf(qBuf, 192, "{\"number\":%" PRIi64 ", speed:%" PRIi64 ", expire:\"%" PRIi64 "\", expireTime:\"%s\"}",
             curVal, limit, expire, ts);
  } else if (limit == GRANT_UNIQ_UNLIMITED) {
    snprintf(qBuf, 192, GRANT_UNIQ_UNLIMITED_S);
  } else if (limit != GRANT_UNIQ_UNUTILIZED) {
    snprintf(qBuf, 192, "%" PRIi64 "/%" PRIi64, curVal, limit);
  } else {
    qBuf[0] = 0;
  }
  varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
  colDataSetVal(pColInfo, *numOfRows, pBuf, false);

  ++(*numOfRows);
  return 0;
}

static int32_t mndRetrieveGrantFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       numOfRows = 0;
  SGrantStatus *pStatus = &gStatus;

  if (pShow->numOfRows < 1) {
    mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_STREAM], gGrantDisplay[GRANT_OPT_STREAM],
                             pStatus->streamExpireSec, pStatus->curStreams, pStatus->limitStreams, false);
    mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_SUBSCRIPTION],
                             gGrantDisplay[GRANT_OPT_SUBSCRIPTION], pStatus->subscriptionExpireSec,
                             pStatus->curSubscriptions, pStatus->limitSubscriptions, false);
    mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_AUDIT], gGrantDisplay[GRANT_OPT_AUDIT],
                             pStatus->auditExpireSec, 0, GRANT_UNIQ_UNUTILIZED, false);
    mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_CSV], gGrantDisplay[GRANT_OPT_CSV],
                             pStatus->csvExpireSec, 0, GRANT_UNIQ_UNUTILIZED, false);
    mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_VIEW], gGrantDisplay[GRANT_OPT_VIEW],
                             pStatus->streamExpireSec, pStatus->curViews, pStatus->limitViews, false);
    mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_STORAGE], gGrantDisplay[GRANT_OPT_STORAGE],
                             pStatus->streamExpireSec, 0, GRANT_UNIQ_UNUTILIZED, false);
    mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_DATA_BAK_RST],
                             gGrantDisplay[GRANT_OPT_DATA_BAK_RST], pStatus->streamExpireSec, 0, GRANT_UNIQ_UNUTILIZED,
                             false);

    taosThreadRwlockRdlock(&grantHandle.rwLock);

    int32_t nDynamic = taosArrayGetSize(pStatus->pItem32);
    for (int32_t i = 0; i < nDynamic; ++i) {
      SGrantItem32 *pItem = TARRAY_GET_ELEM(pStatus->pItem32, i);
      mndRetrieveGrantFullItem(pBlock, &numOfRows, pItem->name, tGetGrantDisplay(pItem->name),
                               (int64_t)pItem->expire * 86400, 0, pItem->number, false);
    }
    nDynamic = taosArrayGetSize(pStatus->pItem64);
    for (int32_t i = 0; i < nDynamic; ++i) {
      SGrantItem64 *pItem = TARRAY_GET_ELEM(pStatus->pItem64, i);
      mndRetrieveGrantFullItem(pBlock, &numOfRows, pItem->name, tGetGrantDisplay(pItem->name),
                               (int64_t)pItem->expire * 86400, 0, pItem->number, false);
    }

    for (int32_t i = 0; i < CONN_TYPE_MAX; ++i) {
      mndRetrieveGrantFullItem(pBlock, &numOfRows, gConnName[i], gConnDisplay[i], pStatus->dataIns[3 * i],
                               pStatus->dataIns[3 * i + 1], pStatus->dataIns[3 * i + 2], true);
    }
    nDynamic = taosArrayGetSize(pStatus->pDataIns);
    for (int32_t i = 0; i < nDynamic; ++i) {
      SGrantDataIns *pDataIn = TARRAY_GET_ELEM(pStatus->pDataIns, i);
      mndRetrieveGrantFullItem(pBlock, &numOfRows, pDataIn->name, tGetConnDisplay(pDataIn->name), pDataIn->expire,
                               pDataIn->number, pDataIn->speed, true);
    }

    taosThreadRwlockUnlock(&grantHandle.rwLock);
  }

  pShow->numOfRows += numOfRows;

  return numOfRows;
}

static void mndCancelGetNextGrantFull(SMnode *pMnode, void *pIter) {
  printf("%s:%d executed\n\n\n\n\n", __func__, __LINE__);
}
static int32_t mndRetrieveGrantLog(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  int32_t numOfRows = 0;
  int32_t cols = 0;
  char   *pBuf = NULL;
  char   *qBuf = NULL;
  char    tmp[50];
  int32_t tmpLen = 0;
  int32_t bufLen = 0;
  int32_t nMachines = 0;
  void   *pIter = NULL;

  SGrantObj *pGrant = mndAcquireGrant(pMnode, &pIter);
  if (!pGrant) {
    return 0;
  }
  nMachines = taosArrayGetSize(pGrant->pMachines);
  bufLen = nMachines * 37;         // max len of machine
  if (bufLen < 840) bufLen = 840;  // max len of state: 28*30=840

  bufLen += VARSTR_HEADER_SIZE;

  if (!(pBuf = taosMemoryCalloc(1, bufLen))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return 0;
  }

  if (pShow->numOfRows < 1) {
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
    for (int32_t i = 0; i < pGrant->nStates; ++i) {
      SGrantState *pState = &pGrant->states[i];
      if (i == 0) {
        snprintf(tmp, 50, "%" PRIi64 ",%d,%d,%d", (int64_t)pState->ts, pState->reason, pState->lastState,
                 pState->state);
      } else {
        snprintf(tmp, 50, ";%" PRIi64 ",%d,%d,%d", (int64_t)pState->ts, pState->reason, pState->lastState,
                 pState->state);
      }
      tmpLen = strlen(tmp);
      memcpy(qBuf, tmp, tmpLen);
      qBuf += tmpLen;
    }
    qBuf[0] = 0;
    varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
    colDataSetVal(pColInfo, numOfRows, pBuf, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
    for (int32_t i = 0; i < pGrant->nActives; ++i) {
      SGrantActive *pActive = &pGrant->actives[i];
      if (i == 0) {
        snprintf(tmp, 50, "%" PRIi64 ",%s", (int64_t)pActive->ts, pActive->active);
      } else {
        snprintf(tmp, 50, ";%" PRIi64 ",%s", (int64_t)pActive->ts, pActive->active);
      }
      tmpLen = strlen(tmp);
      memcpy(qBuf, tmp, tmpLen);
      qBuf += tmpLen;
    }
    qBuf[0] = 0;
    varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
    colDataSetVal(pColInfo, numOfRows, pBuf, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
    for (int32_t i = 0; i < nMachines; ++i) {
      SGrantMachine *pMachine = TARRAY_GET_ELEM(pGrant->pMachines, i);
      if (i == 0) {
        snprintf(tmp, 50, "%" PRIi64 ",%s", (int64_t)pMachine->ts, pMachine->machine);
      } else {
        snprintf(tmp, 50, ";%" PRIi64 ",%s", (int64_t)pMachine->ts, pMachine->machine);
      }
      tmpLen = strlen(tmp);
      memcpy(qBuf, tmp, tmpLen);
      qBuf += tmpLen;
    }
    qBuf[0] = 0;
    varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
    colDataSetVal(pColInfo, numOfRows, pBuf, false);

    ++numOfRows;
  }
  mndReleaseGrant(pMnode, pGrant, pIter);

  pShow->numOfRows += numOfRows;

  taosMemoryFree(pBuf);
  return numOfRows;
}

static void mndCancelGetNextGrantLog(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
static int32_t mndRetrieveMachines(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  int32_t numOfRows = 0;
  int32_t cols = 0;
  char   *pBuf = NULL;
  char   *qBuf = NULL;
  char    tmp[50];
  int32_t tmpLen = 0;
  int32_t bufLen = 0;
  int32_t nMachines = mndGetDnodeSize(pMnode);
  void   *pIter = NULL;
  SSdb   *pSdb = pMnode->pSdb;

  bufLen = VARSTR_HEADER_SIZE + TSDB_CLUSTER_ID_LEN + 1 + nMachines * (TSDB_MACHINE_ID_LEN + 1);
  if (!(pBuf = taosMemoryCalloc(1, bufLen))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return 0;
  }

  if (pShow->numOfRows < 1) {
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
    snprintf(qBuf, TSDB_CLUSTER_ID_LEN + 1, "%s", grantObj.clusterId);
    varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
    colDataSetVal(pColInfo, numOfRows, pBuf, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
    snprintf(qBuf, TSDB_CLUSTER_ID_LEN + 2, "%s;", grantObj.clusterId);
    qBuf += strlen(qBuf);

    SDnodeObj *pDnode = NULL;
    bool       first = true;
    while ((pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode))) {
      if (pDnode->machineId[0] == 0) continue;
      if (first) {
        snprintf(qBuf, TSDB_MACHINE_ID_LEN + 1, "%s", pDnode->machineId);
        first = false;
        qBuf += TSDB_MACHINE_ID_LEN;
      } else {
        snprintf(qBuf, TSDB_MACHINE_ID_LEN + 2, ",%s", pDnode->machineId);
        qBuf += (TSDB_MACHINE_ID_LEN + 1);
      }
      sdbRelease(pSdb, pDnode);
    }
    varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
    colDataSetVal(pColInfo, numOfRows, pBuf, false);

    ++numOfRows;
  }

  pShow->numOfRows += numOfRows;

  taosMemoryFree(pBuf);
  return numOfRows;
}

static void mndCancelGetNextMachines(SMnode *pMnode, void *pIter) {
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

static int32_t tSerializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, int64_t clusterTime) {
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

  if (tSerializeGrantDataIns(&encoder, pStatus->dataIns) < 0) goto _exit;

  if (tEncodeI64v(&encoder, clusterTime) < 0) goto _exit;

  if (tSerializeGrantDynDataIns(&encoder, pStatus->pDataIns) < 0) goto _exit;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  code = 0;
_exit:
  tEncoderClear(&encoder);

  return code == 0 ? tlen : code;
}

int32_t tDeserializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, int64_t *clusterTime) {
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

  if (tDecodeI64v(&decoder, clusterTime) < 0) goto _exit;

  if (tDeserializeGrantDynDataIns(&decoder, pStatus->pDataIns) < 0) goto _exit;

  code = 0;
_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return code;
}

static int32_t tSerializeGrantDataIns(SEncoder *encoder, int32_t *dataIn) {
  for (int32_t i = 0; i < GRANT_UNIQ_KNOWN_DATAIN_VALS; ++i) {
    if (tEncodeI32v(encoder, dataIn[i]) < 0) return -1;
  }
  return 0;
}

static int32_t tDeserializeGrantDataIns(SDecoder *decoder, int32_t *dataIn) {
  for (int32_t i = 0; i < GRANT_UNIQ_KNOWN_DATAIN_VALS; ++i) {
    if (tDecodeI32v(decoder, &dataIn[i]) < 0) return -1;
  }
  return 0;
}

static int32_t tSerializeGrantDynDataIns(SEncoder *encoder, SArray *pIns) {
  int16_t nDataIns = taosArrayGetSize(pIns);
  if (tEncodeI16v(encoder, nDataIns) < 0) return -1;
  for (int32_t i = 0; i < nDataIns; ++i) {
    SGrantDataIns *pIn = TARRAY_GET_ELEM(pIns, i);
    if (tEncodeCStr(encoder, pIn->name) < 0) return -1;
    if (tEncodeI32v(encoder, pIn->number) < 0) return -1;
    if (tEncodeI32v(encoder, pIn->speed) < 0) return -1;
    if (tEncodeI32v(encoder, pIn->expire) < 0) return -1;
  }
  return 0;
}

static int32_t tDeserializeGrantDynDataIns(SDecoder *decoder, SArray *pIns) {
  int16_t nIns = 0;
  if (tDecodeI16v(decoder, &nIns) < 0) return -1;
  if (nIns <= 0) return 0;
  if (!pIns || !(pIns = taosArrayInit(nIns, sizeof(SGrantDataIns)))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < nIns; ++i) {
    SGrantDataIns *pIn = TARRAY_GET_ELEM(pIns, i);
    if (tDecodeCStrTo(decoder, &pIn->name[0]) < 0) return -1;
    if (tDecodeI32v(decoder, &pIn->number) < 0) return -1;
    if (tDecodeI32v(decoder, &pIn->speed) < 0) return -1;
    if (tDecodeI32v(decoder, &pIn->expire) < 0) return -1;
  }
  return 0;
}