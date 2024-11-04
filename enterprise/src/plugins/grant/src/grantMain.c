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
#include "tbase64.h"
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

#define GRANT_OPT_EXPIRE_CHECK(expire, name)                                                                       \
  do {                                                                                                             \
    if ((expire) == GRANT_UNIQ_UNDEFINED) {                                                                        \
      if (basicLtDefault && (0 != strcmp((name), "service"))) {                                                    \
        code = TSDB_CODE_GRANT_OPT_EXPIRE_TOO_LARGE;                                                               \
        uError("grant optional items check failed since %s, basic:%" PRIi64 " < default %s:%" PRIi64 "(second)",   \
               tstrerror(code), basicExpireSec, (name), defaultExpireSec);                                         \
        TSDB_CHECK_CODE(code, lino, _exit);                                                                        \
      }                                                                                                            \
    } else if ((expire) == GRANT_UNIQ_UNLIMITED || (expire) > basicExpireDay) {                                    \
      code = TSDB_CODE_GRANT_OPT_EXPIRE_TOO_LARGE;                                                                 \
      uError("grant optional items check failed since %s, basic:%d < %s:%d(day)", tstrerror(code), basicExpireDay, \
             (name), (expire));                                                                                    \
      TSDB_CHECK_CODE(code, lino, _exit);                                                                          \
    }                                                                                                              \
  } while (0)

#define GRANT_ITEM_EXPIRE_CHECK(val, now, expired)            \
  do {                                                        \
    if (((val) == GRANT_UNIQ_UNLIMITED) || ((val) > (now))) { \
      if ((expired)) (expired) = 0;                           \
    } else {                                                  \
      if (!(expired)) (expired) = 1;                          \
    }                                                         \
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

#define GRANT_EXPIRE_SHOW(expireSec)                      \
  do {                                                    \
    ++cols;                                               \
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);    \
    if ((expireSec) != GRANT_UNIQ_UNLIMITED) {            \
      TAOS_UNUSED(grantSecondsToString((expireSec), ts)); \
      src = ts;                                           \
    } else {                                              \
      src = GRANT_UNIQ_UNLIMITED_S;                       \
    }                                                     \
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));       \
    COL_DATA_SET_VAL_GOTO(tmp, false, NULL, _exit);       \
  } while (0)

#define GRANT_ITEM_SHOW(cur, limit, unit)                                            \
  do {                                                                               \
    ++cols;                                                                          \
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);                               \
    if ((limit) != GRANT_UNIQ_UNLIMITED) {                                           \
      (void)sprintf(tmp1, "%" PRIi64 "/%" PRIi64, (int64_t)(cur), (int64_t)(limit)); \
    } else {                                                                         \
      (void)sprintf(tmp1, "%" PRIi64 "/%s", (int64_t)(cur), GRANT_UNIQ_UNLIMITED_S); \
    }                                                                                \
    src = tmp1;                                                                      \
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));                                  \
    COL_DATA_SET_VAL_GOTO(tmp, false, NULL, _exit);                                  \
  } while (0)

#define GRANT_VALUE_CONVERT(from, to, factor, dft) \
  do {                                             \
    if ((from) == GRANT_UNIQ_UNDEFINED) {          \
      (to) = (dft);                                \
    } else if ((from) == GRANT_UNIQ_UNLIMITED) {   \
      (to) = (from);                               \
    } else {                                       \
      (to) = (int64_t)(from) * (factor);           \
    }                                              \
  } while (0)

#define GRANT_EXPIRE_CONVERT(from, to, factor, dft, show)               \
  do {                                                                  \
    if ((from) == GRANT_UNIQ_UNDEFINED) {                               \
      if ((show)) {                                                     \
        (to) = revoked ? TMIN((dft), gStatus.revokedExpireSec) : (dft); \
      } else {                                                          \
        (to) = (from);                                                  \
      }                                                                 \
    } else if ((from) == GRANT_UNIQ_UNLIMITED) {                        \
      (to) = revoked ? gStatus.revokedExpireSec : (from);               \
    } else {                                                            \
      int64_t tmp = (int64_t)(from) * (factor);                         \
      (to) = revoked ? TMIN(tmp, gStatus.revokedExpireSec) : tmp;       \
    }                                                                   \
  } while (0)

#define GRANT_CHECK_ERROR_LOG(item, cv, lv)                            \
  do {                                                                 \
    uError("failed to grant check since current number of %s %" PRIi64 \
           " is larger than the licensed upper limit %" PRIi64,        \
           (item), (int64_t)(cv), (int64_t)(lv));                      \
  } while (0)

#define GRANT_OPT_EXPIRE_INIT(ev, ed, idx) \
  do {                                     \
    if (grantHandle.showOpts[(idx)]) {     \
      (ev) = GRANT_UNIQ_UNLIMITED;         \
    } else {                               \
      (ev) = GRANT_UNIQ_UNDEFINED;         \
      (ed) = 1;                            \
    }                                      \
  } while (0)

#define GRANT_OPT_LIMITS_INIT(lv, idx) \
  do {                                 \
    if (grantHandle.showOpts[(idx)]) { \
      (lv) = GRANT_UNIQ_UNLIMITED;     \
    }                                  \
  } while (0)

#define GRANT_OPT_EXPIRE_ASSIGN(es, esv, ed, edv, idx) \
  do {                                                 \
    if (grantHandle.showOpts[(idx)]) {                 \
      (es) = (esv);                                    \
      (ed) = (edv);                                    \
    }                                                  \
  } while (0)

// make sure the expire_sec is not GRANT_UNIQ_UNDEFINED
#define GRANT_EXPIRE_TUNE_INDUSTRY(expire_sec)  \
  do {                                          \
    if ((expire_sec) == GRANT_UNIQ_UNDEFINED) { \
      --(expire_sec);                           \
    }                                           \
  } while (0)

#ifdef GRANTS_CFG
#define GRANT_VERSION ("cloud")
#else
#define GRANT_VERSION (gStatus.officialVersion ? "official" : "trial")
#endif
#define GRANT_EXPIRE (gStatus.basicExpireSec)
#define GRANT_EXPIRED(exp) ((exp) ? TSDB_CODE_GRANT_BASIC_EXPIRED : TSDB_CODE_SUCCESS)
#define GRANT_EXPIRED_OPT(expbasic, expopt, erropt) \
  ((expbasic) ? TSDB_CODE_GRANT_BASIC_EXPIRED : ((expopt) ? (erropt) : TSDB_CODE_SUCCESS))
#define GRANT_EXPIRE_VAL (gStatus.expired | (gStatus.multiTierExpired ? gStatus.nDiskCfg > 1 : 0))
#define GRANT_TS_SEC_LEN 20
#define GRANT_LOG_MAX_MACHINE 300

static const char gConnName[CONN_TYPE_DYN_MAX][GRANT_ITEM_NAME_LEN] = {
    "opc_da", "opc_ua", "pi",    "kafka",    "influxdb", "mqtt",  "avevahistorian", "opentsdb",
    "td2.6",  "td3.0",  "mysql", "postgres", "oracle",   "mssql", "mongodb",        "csv"};

static const char *gConnDisplay[CONN_TYPE_DYN_MAX] = {
    "OPC_DA",      "OPC_UA",      "Pi",    "Kafka",      "InfluxDB", "MQTT",      "avevaHistorian", "OpenTSDB",
    "TDengine2.6", "TDengine3.0", "MySQL", "PostgreSQL", "Oracle",   "SqlServer", "MongoDB",        "CSV"};

static const char gGrantName[GRANT_OPT_DYN_MAX][GRANT_ITEM_NAME_LEN] = {
    "basic",   "service",        "stream",         "subscription",  "audit",        "csv",           "view",
    "storage", "backup_restore", "object_storage", "active_active", "dual_replica", "db_encryption", "data_sync"};

static const char *gGrantDisplay[GRANT_OPT_DYN_MAX] = {"Basic",
                                                       "Service Time",
                                                       "Stream",
                                                       "Subscription",
                                                       "Audit",
                                                       "CSV",
                                                       "View",
                                                       "Multi-Tier Storage",
                                                       "Data Backup & Restore",
                                                       "Object Storage",
                                                       "Active-Active",
                                                       "Dual-Replica HA",
                                                       "Database Encryption",
                                                       "Data Synchronization"};

static const char *gGrantState[GRANT_STATE_MAX] = {"ungranted", "ungranted", "granted", "expired",
                                                   "revoked"};  // keep 0/1 ungranted

static const char *gGrantReason[GRANT_STATE_REASON_MAX] = {"init", "alter", "mismatch", "expire"};

static int32_t tGetConnIndex(const char *name) {
  for (int32_t i = CONN_TYPE_MAX; i < CONN_TYPE_DYN_MAX; ++i) {
    if (strncasecmp(gConnName[i], name, GRANT_ITEM_NAME_LEN) == 0) {
      return i;
    }
  }
  return -1;
}

static const char *tGetConnDisplay(const char *name) {
  for (int32_t i = CONN_TYPE_MAX; i < CONN_TYPE_DYN_MAX; ++i) {
    if (strncasecmp(gConnName[i], name, GRANT_ITEM_NAME_LEN) == 0) {
      return gConnDisplay[i];
    }
  }
  return name;
}

static int32_t tGetGrantIndex(const char *name) {
  for (int32_t i = GRANT_OPT_MAX; i < GRANT_OPT_DYN_MAX; ++i) {
    if (strncasecmp(gGrantName[i], name, GRANT_ITEM_NAME_LEN) == 0) {
      return i;
    }
  }
  return -1;
}

static const char *tGetGrantDisplay(const char *name) {
  for (int32_t i = GRANT_OPT_MAX; i < GRANT_OPT_DYN_MAX; ++i) {
    if (strncasecmp(gGrantName[i], name, GRANT_ITEM_NAME_LEN) == 0) {
      return gGrantDisplay[i];
    }
  }
  return name;
}

SGrantStatus gStatus = {
    .limitDnodes = GRANT_UNIQ_UNLIMITED,
    .limitTimeSeries = GRANT_UNIQ_UNLIMITED,
    .limitCpuCores = GRANT_UNIQ_UNLIMITED,
};

typedef SGrantNotify GrantNotify;
typedef SGrantStatus GrantStatus;

extern SGrantUniqObj grantObj;
extern char          tsVersionName[16];
extern int64_t       tsExpireTime;

static int32_t grantSecondsToString(int64_t seconds, char *ts);
static void    grantRetrieveGrantInfo(SMnode *pMnode);
static void    grantResetMaster(SMnode *pMnode, int64_t upgradeSec);
static void    grantSetClusterInfo(SMnode *pMnode);
static void    grantObjInit(SGrantUniqObj *pObj, bool official);
static void    grantStatusInit(SGrantStatus *pStatus);
static void    grantDataInsSetDefault(SGrantDataIn *pDataIns, int32_t num, int64_t expireSec);
static int32_t grantCheckViews(bool allowEqual, int8_t traceLevel);
static int64_t grantGetClusterCreateTime(SMnode *pMnode);
static int32_t mndProcessGrantHB(SRpcMsg *pReq);
static int32_t tSerializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, int64_t clusterTime,
                                     uint32_t *pLen);
static int32_t tDeserializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, int64_t *clusterTime);
static int32_t tSerializeGrantNotify(void *buf, int32_t bufLen, GrantNotify *pNotify, uint32_t *pLen);
static int32_t tDeserializeGrantNotify(void *buf, int32_t bufLen, GrantNotify *pNotify);
static int64_t grantGetClusterCurTimeSeries(SMnode *pMnode);

static int32_t mndRetrieveGrant(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextGrant(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveGrantFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextGrantFull(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveGrantLogs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextGrantLogs(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveMachines(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextMachines(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveEncryptions(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextEncryptions(SMnode *pMnode, void *pIter);

static int32_t tSerializeGrantDataIns(SEncoder *encoder, SGrantDataIn *pIns);
static int32_t tDeserializeGrantDataIns(SDecoder *decoder, SGrantDataIn *pIns);
static int32_t tSerializeGrantDynDataIns(SEncoder *encoder, SArray *pIns);
static int32_t tDeserializeGrantDynDataIns(SDecoder *decoder, SArray *pIns);

typedef struct {
  SSHashObj *pMachineHash;
  SArray    *pDnodeInfo;
  SMnode    *pMnode;
  int32_t    nDiskCfg;
  SRWLatch   rwLock;
  int8_t     showOpts[GRANT_OPT_DYN_MAX];
  int8_t     showDataIns[CONN_TYPE_DYN_MAX];
} SGrantHandle;

static bool         recheckClusterTime = true;
static int64_t      grantNotifyTimestamp = 0;
static int64_t      grantNotifyTimeSeries = INT64_MAX;
static int64_t      grantClusterEpoch = 0;
static int64_t      grantClusterTime = 0;
static SGrantHandle grantHandle = {0};

int32_t mndInitGrant(SMnode *pMnode) {
  int32_t code = 0;
  int32_t lino = 0;

  grantStatusInit(&gStatus);
  grantHandle.pMnode = pMnode;
  tsGrantHBInterval = 1;

  mndSetMsgHandle(pMnode, TDMT_MND_GRANT_HB_TIMER, mndProcessGrantHB);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndRetrieveGrant);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndCancelGetNextGrant);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS_FULL, mndRetrieveGrantFull);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_GRANTS_FULL, mndCancelGetNextGrantFull);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS_LOGS, mndRetrieveGrantLogs);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_GRANTS_LOGS, mndCancelGetNextGrantLogs);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_MACHINES, mndRetrieveMachines);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_MACHINES, mndCancelGetNextMachines);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_ENCRYPTIONS, mndRetrieveEncryptions);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_ENCRYPTIONS, mndCancelGetNextEncryptions);

  SSdbTable table = {
      .sdbType = SDB_GRANT,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndGrantActionEncode,
      .decodeFp = (SdbDecodeFp)mndGrantActionDecode,
      .insertFp = (SdbInsertFp)mndGrantActionInsert,
      .updateFp = (SdbUpdateFp)mndGrantActionUpdate,
      .deleteFp = (SdbDeleteFp)mndGrantActionDelete,
  };

  TAOS_CHECK_EXIT(sdbSetTable(pMnode->pSdb, table));

  grantSetClusterInfo(pMnode);

  if (!(grantHandle.pMachineHash = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY)))) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }
  if (!(grantHandle.pDnodeInfo = taosArrayInit(0, sizeof(SDnodeInfo)))) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

_exit:
  if (code < 0) {
    uError("grant data initialize failed at line %d since %s", lino, tstrerror(code));
    mndCleanupGrant();
  } else {
    uDebug("grant data is initialized");
  }

  TAOS_RETURN(code);
}

void tResetGrantUniqObj(SGrantUniqObj *pObj) {
  if (grantObj.active) grantObj.active[0] = 0;
  if (grantObj.historicalActive) grantObj.historicalActive[0] = 0;
  taosArrayClear(grantObj.pMachines);
  taosArrayClear(grantObj.pDataIns);
  taosArrayClear(grantObj.pItem64);
  taosArrayClear(grantObj.pItemI64);
  taosArrayClear(grantObj.pItemN64);
}

static void grantInitShowFlags() {
  grantHandle.showOpts[GRANT_OPT_BASIC] = 1;
  grantHandle.showOpts[GRANT_OPT_SERVICE] = 1;

#if !defined(TD_INDUSTRY) || defined(TD_FUNC_STREAM)
  grantHandle.showOpts[GRANT_OPT_STREAM] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_FUNC_SUBSCRIPTION)
  grantHandle.showOpts[GRANT_OPT_SUBSCRIPTION] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_FUNC_AUDIT)
  grantHandle.showOpts[GRANT_OPT_AUDIT] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_FUNC_CSV)
  grantHandle.showOpts[GRANT_OPT_CSV] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_FUNC_VIEW)
  grantHandle.showOpts[GRANT_OPT_VIEW] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_FUNC_MULTI_TIER_STORAGE)
  grantHandle.showOpts[GRANT_OPT_STORAGE] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_FUNC_DATA_BAK_RESTORE)
  grantHandle.showOpts[GRANT_OPT_DATA_BAK_RST] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_FUNC_OBJECT_STORAGE)
  grantHandle.showOpts[GRANT_OPT_OBJECT_STORAGE] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_FUNC_ACTIVE_ACTIVE)
  grantHandle.showOpts[GRANT_OPT_ACTIVE_ACTIVE] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_FUNC_DUAL_REPLICA_HA)
  grantHandle.showOpts[GRANT_OPT_DUAL_REPLICA_HA] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_FUNC_DB_ENCRYPTION)
  grantHandle.showOpts[GRANT_OPT_DB_ENCRYPTION] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_FUNC_DATA_SYNC)
  grantHandle.showOpts[GRANT_OPT_DATA_SYNC] = 1;
#endif

// DataIns
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_OPC_DA)
  grantHandle.showDataIns[CONN_TYPE_OPC_DA] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_OPC_UA)
  grantHandle.showDataIns[CONN_TYPE_OPC_UA] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_PI)
  grantHandle.showDataIns[CONN_TYPE_PI] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_KAFKA)
  grantHandle.showDataIns[CONN_TYPE_KAFKA] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_INFLUXDB)
  grantHandle.showDataIns[CONN_TYPE_INFLUXDB] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_MQTT)
  grantHandle.showDataIns[CONN_TYPE_MQTT] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_AVEVAHISTORIAN)
  grantHandle.showDataIns[CONN_TYPE_AVEVAHISTORIAN] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_OPENTSDB)
  grantHandle.showDataIns[CONN_TYPE_OPENTSDB] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_TDENGINE_2_6)
  grantHandle.showDataIns[CONN_TYPE_TDENGINE_2_6] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_TDENGINE_3_0)
  grantHandle.showDataIns[CONN_TYPE_TDENGINE_3_0] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_MYSQL)
  grantHandle.showDataIns[CONN_TYPE_MYSQL] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_POSTGRES)
  grantHandle.showDataIns[CONN_TYPE_POSTGRES] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_ORACLE)
  grantHandle.showDataIns[CONN_TYPE_ORACLE] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_MSSQL)
  grantHandle.showDataIns[CONN_TYPE_MSSQL] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_MONGODB)
  grantHandle.showDataIns[CONN_TYPE_MONGODB] = 1;
#endif
#if !defined(TD_INDUSTRY) || defined(TD_DATAIN_CSV)
  grantHandle.showDataIns[CONN_TYPE_CSV] = 1;
#endif

  // add future datains here ...
}

static void grantStatusInit(SGrantStatus *pStatus) {
  grantInitShowFlags();

  GRANT_OPT_EXPIRE_INIT(pStatus->basicExpireSec, pStatus->expired, GRANT_OPT_BASIC);
  GRANT_OPT_EXPIRE_INIT(pStatus->streamExpireSec, pStatus->streamExpired, GRANT_OPT_STREAM);
  GRANT_OPT_LIMITS_INIT(pStatus->limitStreams, GRANT_OPT_STREAM);
  GRANT_OPT_EXPIRE_INIT(pStatus->subscriptionExpireSec, pStatus->subscriptionExpired, GRANT_OPT_SUBSCRIPTION);
  GRANT_OPT_LIMITS_INIT(pStatus->limitSubscriptions, GRANT_OPT_SUBSCRIPTION);
  GRANT_OPT_EXPIRE_INIT(pStatus->auditExpireSec, pStatus->auditExpired, GRANT_OPT_AUDIT);
  GRANT_OPT_EXPIRE_INIT(pStatus->csvExpireSec, pStatus->csvExpired, GRANT_OPT_CSV);
  GRANT_OPT_EXPIRE_INIT(pStatus->viewExpireSec, pStatus->viewExpired, GRANT_OPT_VIEW);
  GRANT_OPT_LIMITS_INIT(pStatus->limitViews, GRANT_OPT_VIEW);
  GRANT_OPT_EXPIRE_INIT(pStatus->multiTierExpireSec, pStatus->multiTierExpired, GRANT_OPT_STORAGE);
  GRANT_OPT_EXPIRE_INIT(pStatus->bakRstExpireSec, pStatus->placeHolder, GRANT_OPT_DATA_BAK_RST);
  GRANT_OPT_EXPIRE_INIT(pStatus->objectStorageExpireSec, pStatus->objectStorageExpired, GRANT_OPT_OBJECT_STORAGE);
  GRANT_OPT_EXPIRE_INIT(pStatus->activeActiveExpireSec, pStatus->placeHolder, GRANT_OPT_ACTIVE_ACTIVE);
  GRANT_OPT_EXPIRE_INIT(pStatus->dualReplicaHAExpireSec, pStatus->dualReplicaHAExpired, GRANT_OPT_DUAL_REPLICA_HA);
  GRANT_OPT_EXPIRE_INIT(pStatus->dbEncryptionExpireSec, pStatus->dbEncryptionExpired, GRANT_OPT_DB_ENCRYPTION);
  GRANT_OPT_EXPIRE_INIT(pStatus->dataSyncExpireSec, pStatus->placeHolder, GRANT_OPT_DATA_SYNC);

  grantDataInsSetDefault(pStatus->dataIns, CONN_TYPE_DYN_MAX, GRANT_UNIQ_UNLIMITED);
}

static void tDestroyGrantStatus(SGrantStatus *pStatus) {
  if (pStatus) {
    taosArrayDestroy(pStatus->pDataIns);
    taosArrayDestroy(pStatus->pItemN64);
  }
}

void mndCleanupGrant() {
  tSimpleHashCleanup(grantHandle.pMachineHash);
  taosArrayDestroy(grantHandle.pDnodeInfo);
  grantHandle.pMachineHash = NULL;
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
  taosArrayClear(pObj->pItem64);
  taosArrayClear(pObj->pItemI64);
  taosArrayClear(pObj->pItemN64);
  taosArrayClear(pObj->pMachines);
}

static int64_t grantGetCurTime(int64_t curSec, bool checkUptime) {
  if (!checkUptime) return curSec;
  int64_t dndCurSec = (tsDndStart + tsDndUpTime) / 1000;
  int64_t result = TMAX(curSec, dndCurSec);
  result = TMAX(result, grantClusterTime);
  return result;
}

static int64_t grantGetExpireSec(int64_t expireSec) {
  if (gStatus.grantState == GRANT_STATE_REVOKED) {
    return gStatus.revokedExpireSec;
  }

  if (expireSec > GRANT_UNIQ_UNLIMITED) {
    return expireSec;
  }

  if (expireSec == GRANT_UNIQ_UNLIMITED) {
    return expireSec = GRANT_UNIQ_MAX_EXPIRE_SECOND;
  }

  if (expireSec == GRANT_UNIQ_UNDEFINED) {
    return grantClusterEpoch + GRANT_DEFAULT;
  }

  return grantClusterEpoch + GRANT_DEFAULT;
}

static void grantSetClusterInfo(SMnode *pMnode) {
  if (strncmp(tsVersionName, GRANT_VERSION, tListLen(tsVersionName)) != 0) {
    tstrncpy(tsVersionName, GRANT_VERSION, tListLen(tsVersionName));
  }
  int64_t expireSec = grantGetExpireSec(GRANT_EXPIRE);
  COMPARE_SET_VAL(tsExpireTime, expireSec * 1000, !=);
  COMPARE_SET_VAL(pMnode->grant.expireTimeMS, tsExpireTime, !=);
  if (gStatus.limitTimeSeries == GRANT_UNIQ_UNLIMITED) {
    COMPARE_SET_VAL(pMnode->grant.timeseriesAllowed, INT64_MAX, !=);
  } else {
    COMPARE_SET_VAL(pMnode->grant.timeseriesAllowed, (int64_t)gStatus.limitTimeSeries, !=);
  }
}

static FORCE_INLINE void grantSetClusterIdEx(int64_t clusterId) {
  if (grantObj.clusterId[0] == 0 && clusterId > 0) {
    (void)snprintf(grantObj.clusterId, GRANT_CLUSTER_ID_LEN + 1, "%" PRIi64, clusterId);
  }
}

static FORCE_INLINE void grantSetClusterId(SMnode *pMnode, char *pClusterId) {
  if ((*pClusterId == 0) && pMnode) {
    int64_t clusterId = mndGetClusterId(pMnode);
    if (clusterId > 0) {
      (void)snprintf(pClusterId, GRANT_CLUSTER_ID_LEN + 1, "%" PRIi64, clusterId);
    }
  }
}

int32_t dmProcessGrantNotify(void *pInfo, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  if (!pMsg->pCont || (pMsg->contLen <= 0)) {
    uWarn("failed to process grant notify in dnode since msg is empty");
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_MSG);
  }
  // step 1: process grant status from mnode
  SGrantNotify grantNotify = {0};
  TAOS_CHECK_EXIT(tDeserializeGrantNotify(pMsg->pCont, pMsg->contLen, &grantNotify));

  gStatus.curTimeSeries = grantNotify.curTimeSeries;

  return TSDB_CODE_SUCCESS;
_exit:
  pMsg->code = code;
  pMsg->info.rsp = NULL;
  pMsg->info.rspLen = 0;

  uWarn("failed to process grant notify and send rsp in dnode at line %d since %s", lino, tstrerror(code));

  TAOS_RETURN(code);
}

/**
 * @brief process grant status msg in dnode and respond with grant msg
 *
 * @param pInfo
 * @param pMsg
 * @return int32_t
 */
int32_t dmProcessGrantReq(void *pInfo, SRpcMsg *pMsg) {
  int32_t code = 0;
  int32_t lino = 0;
  char    tbuf[40] = {0};
  TRACE_TO_STR(&pMsg->info.traceId, tbuf);

  if (!pMsg->pCont || (pMsg->contLen <= 0)) {
    code = TSDB_CODE_INVALID_MSG;
    uWarn("failed to process grant req in dnode since msg is empty, gtid:%s", tbuf);
    TAOS_CHECK_EXIT(code);
  }
  // step 1: process grant status from mnode
  GrantStatus grantStatusReq = {0};
  int64_t     clusterTime = 0;
  TAOS_CHECK_EXIT(tDeserializeGrantStatus(pMsg->pCont, pMsg->contLen, &grantStatusReq, &clusterTime));

  // step 2: set local dnode grant status
  taosWLockLatch(&grantHandle.rwLock);
  SArray *pDataIns = gStatus.pDataIns;
  gStatus = grantStatusReq;  // assign directly
  taosArrayDestroy(pDataIns);
  taosWUnLockLatch(&grantHandle.rwLock);

  int8_t grantExpireVal = GRANT_EXPIRE_VAL;
  int8_t tsGrantVal = 0;
  if (grantExpireVal == 0) tsGrantVal |= GRANT_FLAG_ALL;
  if (grantCheck(TSDB_GRANT_AUDIT) == 0) tsGrantVal |= GRANT_FLAG_AUDIT;
  if (grantCheckViews(false, DEBUG_DEBUG) == 0) tsGrantVal |= GRANT_FLAG_VIEW;

  if (atomic_load_8(&tsGrant) != tsGrantVal) {
    atomic_store_8(&tsGrant, tsGrantVal);
  }

  // step 3: respond with grant msg
  grantSetClusterIdEx(*(int64_t *)pInfo);

  uDebug("succeed to process grant req in dnode, gtid:%s", tbuf);

  return TSDB_CODE_SUCCESS;
_exit:
  pMsg->code = code;
  pMsg->info.rsp = NULL;
  pMsg->info.rspLen = 0;

  uWarn("failed to process grant req in dnode at line %d since %s, gtid:%s", lino, tstrerror(code), tbuf);

  TAOS_RETURN(code);
}

static int32_t mndSendGrantStatusToDnode(SMnode *pMnode, SDnodeInfo *pDnodeInfo, int32_t contLen, void *pCont) {
  // send grant status to dnode
  SRpcMsg rpcMsg = {
      .pCont = pCont, .contLen = contLen, .msgType = TDMT_MND_GRANT, .info.ahandle = (void *)0x818, .info.noResp = 1};

  SEpSet epSet = {.numOfEps = 1};
  tstrncpy(epSet.eps[0].fqdn, pDnodeInfo->ep.fqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = pDnodeInfo->ep.port;

  int32_t code = 0;
  if ((code = tmsgSendReq(&epSet, &rpcMsg)) != 0) {
    uWarn("failed to send grant status msg since %s", tstrerror(code));
    TAOS_RETURN(code);
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

static void mndProcessGrantStatusCheck() {
  int64_t curTime = taosGetTimestampMs() / 1000;
  int64_t grantCurTime = grantGetCurTime(curTime, grantObj.flags & GRANT_ACTIVE_FLG_CHECK_UPTIME);
  int64_t expireSec = gStatus.grantState == GRANT_STATE_REVOKED ? gStatus.revokedExpireSec : gStatus.basicExpireSec;
  if (expireSec == GRANT_UNIQ_UNLIMITED || expireSec > grantCurTime) {
    if (gStatus.expired) {
      gStatus.expired = 0;
    }
  } else {
    gStatus.expired = 1;
    char ts[GRANT_TS_SEC_LEN] = {0};
    TAOS_UNUSED(grantSecondsToString(expireSec, ts));
    uWarn("grant cluster expired at %s %" PRIi64 ", curtime: %" PRIi64 ", set to %s state", ts, (int64_t)expireSec,
          grantCurTime, gGrantState[gStatus.grantState]);
  }

  int8_t grantExpireVal = GRANT_EXPIRE_VAL;
  int8_t tsGrantVal = 0;
  if (grantExpireVal == 0) tsGrantVal |= GRANT_FLAG_ALL;
  if (grantCheck(TSDB_GRANT_AUDIT) == 0) tsGrantVal |= GRANT_FLAG_AUDIT;
  if (grantCheckViews(false, DEBUG_DEBUG) == 0) tsGrantVal |= GRANT_FLAG_VIEW;

  if (atomic_load_8(&tsGrant) != tsGrantVal) {
    atomic_store_8(&tsGrant, tsGrantVal);
  }
}

static int32_t grantCheckClusterInfo(SMnode *pMnode) {
  int32_t code = 0;
  if (recheckClusterTime) {
    int64_t clusterCreateTime = grantGetClusterCreateTime(pMnode);
    if (clusterCreateTime != 0) {
      COMPARE_SET_VAL(grantClusterEpoch, clusterCreateTime, !=);
      recheckClusterTime = false;
    } else {
      code = TSDB_CODE_APP_IS_STARTING;
    }
  }

  if (grantObj.clusterId[0] == 0) {
    grantSetClusterId(pMnode, grantObj.clusterId);
    if (grantObj.clusterId[0] == 0) {
      code = TSDB_CODE_APP_IS_STARTING;
    }
  }
_exit:
  if (code < 0) {
    recheckClusterTime = true;
  }
  if (recheckClusterTime) {
    COMPARE_SET_VAL(tsGrantHBInterval, GRANT_HEART_BEAT_MIN, !=);
  } else {
    COMPARE_SET_VAL(tsGrantHBInterval, GRANT_HEART_BEAT_MSG, !=);
  }
  TAOS_RETURN(code);
}

static int32_t grantGetDnodesMiscInfo(SMnode *pMnode, SSHashObj *pMachineHash) {
  SSdb      *pSdb = pMnode->pSdb;
  SDnodeObj *pDnode = NULL;
  void      *pIter = NULL;
  int32_t    code = 0;

  while ((pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode))) {
    // machineCode
    int32_t klen = strlen(pDnode->machineId);
    if (klen == TSDB_MACHINE_ID_LEN) {
      if (tSimpleHashPut(pMachineHash, pDnode->machineId, klen + 1, &pDnode->id, sizeof(pDnode->id)) != 0) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        sdbRelease(pSdb, pDnode);
        uError("failed to put machine hash since %s", tstrerror(code));
        TAOS_RETURN(code);
      }
    }
    // nDiskCfg
    if (pDnode->numOfDiskCfg > grantHandle.nDiskCfg) {
      grantHandle.nDiskCfg = pDnode->numOfDiskCfg;
    }

    sdbRelease(pSdb, pDnode);
  }

  TAOS_RETURN(code);
}

static void grantUniqAdjustSubscribeByDataSync(SGrantUniqObj *pObj) {
  SGrantItemI64 *pDataSync = NULL;
  int32_t        size = taosArrayGetSize(pObj->pItemI64);
  for (int32_t i = 0; i < size; ++i) {
    SGrantItemI64 *pItem = TARRAY_GET_ELEM(pObj->pItemI64, i);
    if (pItem->index == GRANT_OPT_DATA_SYNC) {
      pDataSync = pItem;
      break;
    }
  }
  if (pDataSync) {
    /**
     * 需求：TS-5301
     *  数据同步功能需要授权，数据同步指从本集群将数据同步到另一集群
     *  数据同步功能依赖数据订阅功能，如果授权了数据同步功能则自动授权数据订阅功能，在生成授权码时解决这一依赖，以如下几种组合：
     *  1）只授权数据订阅功能，行为不受影响
     *  2）同时授权了数据订阅和数据同步，如果数据订阅过期时间早于数据同步过期时间，将数据订阅过期时间与数据同步过期时间保持一致，其他情况互不干扰。
     *  3）只授权了数据同步，自动授权数据订阅（数据订阅过期时间与数据同步保持一致，其他授权项都使用默认值）
     *
     * 逻辑：
     * 1）sync 功能已过期: 修改 subscribe 不受影响
     * 2）sync 功能未过期: subscribe 过期时间不能小于 sync 过期时间，否则以 sync 为准，不报错; subscribe
     *    数量不受限制，因为已创建的订阅不受影响。
     */
    if ((pDataSync->expire == GRANT_UNIQ_UNLIMITED) ||
        (((int64_t)pDataSync->expire * 86400LL) > (taosGetTimestampMs() / 1000LL))) {
      int32_t subExpire = pObj->expireDays[GRANT_OPT_SUBSCRIPTION];
      if (subExpire == GRANT_UNIQ_UNDEFINED) {
        pObj->expireDays[GRANT_OPT_SUBSCRIPTION] = pDataSync->expire;
        pObj->limitSubscriptions = GRANT_UNIQ_DFT_SUBSCRIPTION_NUM;
        uDebug("adjust grant of subscribe by data_sync expire:%d and default limits:%" PRIi16, pDataSync->expire,
               pObj->limitSubscriptions);
      } else if ((subExpire != GRANT_UNIQ_UNLIMITED) &&
                 (pDataSync->expire == GRANT_UNIQ_UNLIMITED || subExpire < pDataSync->expire)) {
        pObj->expireDays[GRANT_OPT_SUBSCRIPTION] = pDataSync->expire;
        uDebug("adjust grant of subscribe by data_sync expire:%d", pDataSync->expire);
      }
    }
  }
}

static int32_t fillGrantStatusFromObj(SGrantStatus *pStatus, SGrantUniqObj *pObj, int8_t state) {
  bool revoked = state == GRANT_STATE_REVOKED;
#ifndef GRANTS_CFG
  int64_t dftExpireSec = grantClusterEpoch + GRANT_DEFAULT;
  GRANT_EXPIRE_TUNE_INDUSTRY(dftExpireSec);
#else
  int64_t dftExpireSec = GRANT_UNIQ_UNLIMITED;
#endif

  grantUniqAdjustSubscribeByDataSync(&grantObj);

  gStatus.officialVersion = grantObj.officialVersion;
  gStatus.checkUpTime = (grantObj.flags & GRANT_ACTIVE_FLG_CHECK_UPTIME) ? 1 : 0;
  gStatus.checkMachineCode = (grantObj.flags & GRANT_ACTIVE_FLG_CHECK_MACHINE) ? 1 : 0;
  gStatus.skipOldActiveIfParseFail = (grantObj.flags & GRANT_ACTIVE_FLG_SKIP_FAIL_OLD) ? 1 : 0;
  gStatus.checkHistoricalActive = grantObj.token[0] ? 1 : 0;
  GRANT_VALUE_CONVERT(grantObj.expireDays[GRANT_OPT_BASIC], gStatus.basicExpireSec, 86400, dftExpireSec);
  GRANT_EXPIRE_CONVERT(grantObj.expireDays[GRANT_OPT_SERVICE], gStatus.serviceExpireSec, 86400, grantClusterEpoch,
                       true);
  GRANT_VALUE_CONVERT(grantObj.limitTimeSeries, gStatus.limitTimeSeries, 1, GRANT_UNIQ_DFT_BASIC_TIMESERIES);
  GRANT_VALUE_CONVERT(grantObj.limitDnodes, gStatus.limitDnodes, 1, GRANT_UNIQ_DFT_BASIC_DNODES);
  GRANT_VALUE_CONVERT(grantObj.limitCpuCores, gStatus.limitCpuCores, 1, GRANT_UNIQ_DFT_BASIC_CPU);
  GRANT_EXPIRE_CONVERT(grantObj.expireDays[GRANT_OPT_STREAM], gStatus.streamExpireSec, 86400, dftExpireSec,
                       grantHandle.showOpts[GRANT_OPT_STREAM]);
  GRANT_VALUE_CONVERT(grantObj.limitStreams, gStatus.limitStreams, 1, GRANT_UNIQ_DFT_STREAM_NUM);
  GRANT_EXPIRE_CONVERT(grantObj.expireDays[GRANT_OPT_SUBSCRIPTION], gStatus.subscriptionExpireSec, 86400, dftExpireSec,
                       grantHandle.showOpts[GRANT_OPT_SUBSCRIPTION]);
  GRANT_VALUE_CONVERT(grantObj.limitSubscriptions, gStatus.limitSubscriptions, 1, GRANT_UNIQ_DFT_SUBSCRIPTION_NUM);
  GRANT_VALUE_CONVERT(grantObj.limitViews, gStatus.limitViews, 1, GRANT_UNIQ_DFT_VIEW_NUM);
  GRANT_EXPIRE_CONVERT(grantObj.expireDays[GRANT_OPT_STORAGE], gStatus.multiTierExpireSec, 86400, dftExpireSec,
                       grantHandle.showOpts[GRANT_OPT_STORAGE]);
  GRANT_EXPIRE_CONVERT(grantObj.expireDays[GRANT_OPT_AUDIT], gStatus.auditExpireSec, 86400, dftExpireSec,
                       grantHandle.showOpts[GRANT_OPT_AUDIT]);
  GRANT_EXPIRE_CONVERT(grantObj.expireDays[GRANT_OPT_CSV], gStatus.csvExpireSec, 86400, dftExpireSec,
                       grantHandle.showOpts[GRANT_OPT_CSV]);
  GRANT_EXPIRE_CONVERT(grantObj.expireDays[GRANT_OPT_VIEW], gStatus.viewExpireSec, 86400, dftExpireSec,
                       grantHandle.showOpts[GRANT_OPT_VIEW]);
  GRANT_EXPIRE_CONVERT(grantObj.expireDays[GRANT_OPT_DATA_BAK_RST], gStatus.bakRstExpireSec, 86400, dftExpireSec,
                       grantHandle.showOpts[GRANT_OPT_DATA_BAK_RST]);

  int32_t nVariantGrantItems = taosArrayGetSize(pObj->pItemI64);
  if (nVariantGrantItems > 0) {
#if defined(ASSERT_NOT_CORE) && !defined(GRANTS_CFG)  // release version
    int64_t dftExpireEpoch = grantClusterEpoch;
    GRANT_EXPIRE_TUNE_INDUSTRY(dftExpireEpoch);
#endif
    for (int32_t i = 0; i < nVariantGrantItems; ++i) {
      SGrantItemI64 *pItemI64 = TARRAY_GET_ELEM(pObj->pItemI64, i);
      switch (pItemI64->index) {
        case GRANT_OPT_OBJECT_STORAGE: {
          GRANT_EXPIRE_CONVERT(pItemI64->expire, gStatus.objectStorageExpireSec, 86400, dftExpireSec,
                               grantHandle.showOpts[GRANT_OPT_OBJECT_STORAGE]);
        } break;
        case GRANT_OPT_ACTIVE_ACTIVE: {
          GRANT_EXPIRE_CONVERT(pItemI64->expire, gStatus.activeActiveExpireSec, 86400, dftExpireSec,
                               grantHandle.showOpts[GRANT_OPT_ACTIVE_ACTIVE]);
        } break;
        case GRANT_OPT_DUAL_REPLICA_HA: {
#if defined(ASSERT_NOT_CORE) && !defined(GRANTS_CFG)  // release version
          GRANT_EXPIRE_CONVERT(pItemI64->expire, gStatus.dualReplicaHAExpireSec, 86400, dftExpireEpoch,
                               grantHandle.showOpts[GRANT_OPT_DUAL_REPLICA_HA]);
#else
          GRANT_EXPIRE_CONVERT(pItemI64->expire, gStatus.dualReplicaHAExpireSec, 86400, dftExpireSec,
                               grantHandle.showOpts[GRANT_OPT_DUAL_REPLICA_HA]);
#endif
        } break;
        case GRANT_OPT_DB_ENCRYPTION: {
#if defined(ASSERT_NOT_CORE) && !defined(GRANTS_CFG)  // release version
          GRANT_EXPIRE_CONVERT(pItemI64->expire, gStatus.dbEncryptionExpireSec, 86400, dftExpireEpoch,
                               grantHandle.showOpts[GRANT_OPT_DB_ENCRYPTION]);
#else
          GRANT_EXPIRE_CONVERT(pItemI64->expire, gStatus.dbEncryptionExpireSec, 86400, dftExpireSec,
                               grantHandle.showOpts[GRANT_OPT_DB_ENCRYPTION]);
#endif
        } break;
        case GRANT_OPT_DATA_SYNC: {
          GRANT_EXPIRE_CONVERT(pItemI64->expire, gStatus.dataSyncExpireSec, 86400, dftExpireSec,
                               grantHandle.showOpts[GRANT_OPT_DATA_SYNC]);
        } break;
        default:
          break;
      }
    }
  }

  for (int32_t i = 0; i < CONN_TYPE_MAX; ++i) {
    int32_t j = i * 3;
    GRANT_EXPIRE_CONVERT(grantObj.dataIns[j], gStatus.dataIns[i].expireSec, 86400, dftExpireSec,
                         grantHandle.showDataIns[i]);                                                        // expire
    GRANT_VALUE_CONVERT(grantObj.dataIns[j + 1], gStatus.dataIns[i].speed, 1, GRANT_UNIQ_DFT_DATAIN_SPEED);  // speed
    GRANT_VALUE_CONVERT(grantObj.dataIns[j + 2], gStatus.dataIns[i].number, 1, GRANT_UNIQ_DFT_DATAIN_NUM);   // number
  }

  int64_t curTime = taosGetTimestampMs() / 1000;
  char    ts[GRANT_TS_SEC_LEN] = {0};
  int64_t grantCurTime = grantGetCurTime(curTime, grantObj.flags & GRANT_ACTIVE_FLG_CHECK_UPTIME);
  int64_t expireSec = revoked ? gStatus.revokedExpireSec : gStatus.basicExpireSec;
  if (expireSec == GRANT_UNIQ_UNLIMITED || expireSec > grantCurTime) {
    COMPARE_SET_VAL(gStatus.expired, 0, !=);
  } else {
    COMPARE_SET_VAL(gStatus.expired, 1, !=);
    TAOS_UNUSED(grantSecondsToString(expireSec, ts));
    uWarn("grant cluster expired at %s %" PRIi64 ", curtime: %" PRIi64, ts, (int64_t)expireSec, grantCurTime);
  }

  GRANT_ITEM_EXPIRE_CHECK(gStatus.auditExpireSec, grantCurTime, gStatus.auditExpired);
  GRANT_ITEM_EXPIRE_CHECK(gStatus.csvExpireSec, grantCurTime, gStatus.csvExpired);
  GRANT_ITEM_EXPIRE_CHECK(gStatus.streamExpireSec, grantCurTime, gStatus.streamExpired);
  GRANT_ITEM_EXPIRE_CHECK(gStatus.subscriptionExpireSec, grantCurTime, gStatus.subscriptionExpired);
  GRANT_ITEM_EXPIRE_CHECK(gStatus.viewExpireSec, grantCurTime, gStatus.viewExpired);
  GRANT_ITEM_EXPIRE_CHECK(gStatus.multiTierExpireSec, grantCurTime, gStatus.multiTierExpired);
  GRANT_ITEM_EXPIRE_CHECK(gStatus.objectStorageExpireSec, grantCurTime, gStatus.objectStorageExpired);
  GRANT_ITEM_EXPIRE_CHECK(gStatus.dualReplicaHAExpireSec, grantCurTime, gStatus.dualReplicaHAExpired);
  GRANT_ITEM_EXPIRE_CHECK(gStatus.dbEncryptionExpireSec, grantCurTime, gStatus.dbEncryptionExpired);

  // extract known dataIns from grantObj to grantStatus
  int8_t  knownDataInAssigned[CONN_TYPE_DYN_MAX] = {0};
  int32_t nDataIn = taosArrayGetSize(pObj->pDataIns);
  if (nDataIn > 0) {
    for (int32_t i = 0; i < TARRAY_SIZE(pObj->pDataIns); ++i) {
      SGrantDataIns *pDataIns = TARRAY_GET_ELEM(pObj->pDataIns, i);
      int32_t        j = tGetConnIndex(pDataIns->name);
      if (j >= CONN_TYPE_MAX && j < CONN_TYPE_DYN_MAX) {
        GRANT_EXPIRE_CONVERT(pDataIns->expire, gStatus.dataIns[j].expireSec, 86400, dftExpireSec,
                             grantHandle.showDataIns[j]);
        GRANT_VALUE_CONVERT(pDataIns->speed, gStatus.dataIns[j].speed, 1, GRANT_UNIQ_DFT_DATAIN_SPEED);
        GRANT_VALUE_CONVERT(pDataIns->number, gStatus.dataIns[j].number, 1,
                            j == CONN_TYPE_CSV ? GRANT_UNIQ_UNLIMITED : GRANT_UNIQ_DFT_DATAIN_NUM);

        knownDataInAssigned[j] = 1;
        taosArrayRemove(pObj->pDataIns, i--);
      }
    }
  }
  for (int32_t j = CONN_TYPE_MAX; j < CONN_TYPE_DYN_MAX; ++j) {
    if (knownDataInAssigned[j] == 0) {
      GRANT_EXPIRE_CONVERT(GRANT_UNIQ_UNDEFINED, gStatus.dataIns[j].expireSec, 86400, dftExpireSec,
                           grantHandle.showDataIns[j]);
      GRANT_VALUE_CONVERT(GRANT_UNIQ_UNDEFINED, gStatus.dataIns[j].speed, 1, GRANT_UNIQ_DFT_DATAIN_SPEED);
      GRANT_VALUE_CONVERT(GRANT_UNIQ_UNDEFINED, gStatus.dataIns[j].number, 1,
                          j == CONN_TYPE_CSV ? GRANT_UNIQ_UNLIMITED : GRANT_UNIQ_DFT_DATAIN_NUM);
    }
  }

  // add rwlock since retrieve would access simultaneously
  taosWLockLatch(&grantHandle.rwLock);
  nDataIn = taosArrayGetSize(pObj->pDataIns);
  if (nDataIn > 0) {
    void *tmp = pStatus->pDataIns;
    pStatus->pDataIns = pObj->pDataIns;
    pObj->pDataIns = tmp;
  } else {
    taosArrayClear(pStatus->pDataIns);
  }

  int32_t nItem64 = taosArrayGetSize(pObj->pItemN64);
  if (nItem64 > 0) {
    void *tmp = pStatus->pItemN64;
    pStatus->pItemN64 = pObj->pItemN64;
    pObj->pItemN64 = tmp;
  } else {
    taosArrayClear(pStatus->pItemN64);
  }

  taosWUnLockLatch(&grantHandle.rwLock);

  TAOS_RETURN(0);
}

static int32_t grantMachineCmprFn(const void *p1, const void *p2) {
  const void *m1 = &((SGrantMachine *)p1)->machine[0];
  const void *m2 = &((SGrantMachine *)p2)->machine[0];
  return memcmp(m1, m2, TSDB_MACHINE_ID_LEN);
}

static int32_t grantMachineKeyCmprFn(const void *p1, const void *p2) {
  const void *m2 = &((SGrantMachine *)p2)->machine[0];
  return memcmp(p1, m2, TSDB_MACHINE_ID_LEN);
}

static int32_t grantCheckMachines(SGrantLogObj *pGrant, SArray **pGrantMachines, bool *toRevoked) {
  int32_t nDnodeLimit = gStatus.limitDnodes >= 0 ? gStatus.limitDnodes : INT32_MAX;
  int32_t nMachines = taosArrayGetSize(pGrant->pMachines);
  void   *pe = NULL;
  int32_t iter = 0;
  if (nMachines > 1 && pGrant->pMachines) taosArraySort(pGrant->pMachines, grantMachineCmprFn);
  if (nMachines < nDnodeLimit) {
    // append if not exist in SGrantLogObj, transfer to revoked state if exceeded
    int32_t idx = 0;
    void   *machines[GRANT_LOG_MAX_MACHINE];
    int32_t dnodeIds[GRANT_LOG_MAX_MACHINE];
    while ((pe = tSimpleHashIterate(grantHandle.pMachineHash, pe, &iter)) != NULL) {
      void *key = tSimpleHashGetKey(pe, NULL);
      if (!pGrant->pMachines || !taosArraySearch(pGrant->pMachines, key, grantMachineKeyCmprFn, TD_EQ)) {
        machines[idx] = key;
        dnodeIds[idx] = *(int32_t *)pe;
        if (++idx >= GRANT_LOG_MAX_MACHINE) break;
      }
    }
    int32_t num = idx;
    if (nMachines + idx > nDnodeLimit) {
      if (toRevoked) *toRevoked = true;  // exceeded
      uWarn("grant check machines, convert to revoked state since number of dnodes:%d,%d exceed the limit:%d",
            nMachines, idx, nDnodeLimit);
      num = nDnodeLimit - nMachines;
    }
    if (num > 0) {
      *pGrantMachines = taosArrayInit_s(sizeof(SGrantMachine), num);
      if (NULL == *pGrantMachines) {
        TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
      }
      int64_t curTime = taosGetTimestampMs() / 1000;
      for (int32_t i = 0; i < num; ++i) {
        SGrantMachine *pMachine = TARRAY_GET_ELEM(*pGrantMachines, i);
        pMachine->id = dnodeIds[i];
        pMachine->ts = curTime;
        (void)memcpy(&pMachine->machine[0], machines[i], TSDB_MACHINE_ID_LEN);
      }
    }
  } else if (nMachines == nDnodeLimit) {
    // if dnode machines all exist in cluster, it's ok; otherwise transfer to revoked state
    while ((pe = tSimpleHashIterate(grantHandle.pMachineHash, pe, &iter)) != NULL) {
      void *key = tSimpleHashGetKey(pe, NULL);
      if (!pGrant->pMachines || !taosArraySearch(pGrant->pMachines, key, grantMachineKeyCmprFn, TD_EQ)) {
        if (toRevoked) *toRevoked = true;  // mismatch
        uWarn("grant check machines, convert to revoked state since dnode:%d, %s mismatch, limit:%d", *(int32_t *)pe,
              (char *)key, nDnodeLimit);
        char *buf = taosMemoryMalloc(nMachines * 50);
        if (buf) {  // print debug info
          char *pBuf = buf;
          for (int32_t i = 0; i < nMachines; ++i) {
            SGrantMachine *pMachine = TARRAY_GET_ELEM(pGrant->pMachines, i);
            (void)snprintf(pBuf, 50, "%" PRIi64 ",%d,%s;", (int64_t)pMachine->ts, (int32_t)pMachine->id,
                           pMachine->machine);
            pBuf += strlen(pBuf);
          }
          if (pBuf != buf) --pBuf;
          pBuf[0] = 0;
          uWarn("grant check machines, %s", buf);
          taosMemoryFree(buf);
        }
        break;
      }
    }
  } else {
    // transfer to revoked if exceeded
    if (toRevoked) *toRevoked = true;
    uWarn("grant check machines, convert to revoked state since number of dnodes:%d exceed the limit:%d", nMachines,
          nDnodeLimit);
  }

  TAOS_RETURN(0);
}

static int32_t mndProcessGrantHBSyncInfo(SMnode *pMnode, int8_t type) {
  int32_t       code = 0;
  int32_t       lino = 0;
  int64_t       curTime = taosGetTimestampMs() / 1000;
  bool          toRevoked = false;
  bool          stated = true;
  bool          legacy = false;
  void         *pIter = NULL;
  SGrantLogObj *pGrant = NULL;
  SArray       *pGrantMachines = NULL;

  pGrant = mndAcquireGrant(pMnode, &pIter);
  if (!pGrant) {
    code = mndProcessUpdGrantLog(pMnode, NULL, pGrantMachines,
                                 &(SGrantState){.state = GRANT_STATE_UNGRANTED, .reason = GRANT_STATE_REASON_INIT});
    tsGrantHBInterval = GRANT_HEART_BEAT_MIN;
    TAOS_CHECK_EXIT(code);
    TAOS_RETURN(code);
  }

  SGrantState *pLastState = pGrant->nStates > 0 ? &pGrant->states[pGrant->nStates - 1] : NULL;
  if (!pLastState) {
    stated = false;
    gStatus.grantState = GRANT_STATE_UNGRANTED;
  } else {
    gStatus.grantState = pLastState->state;
    if (gStatus.grantState == GRANT_STATE_REVOKED) {
      int64_t revokedExpireSec = pLastState->ts + GRANT_CHK_TOLERENCE;
      GRANT_EXPIRE_TUNE_INDUSTRY(revokedExpireSec);
      gStatus.revokedExpireSec = revokedExpireSec;
    }
  }

  grantRetrieveGrantInfo(pMnode);

  grantObjInit(&grantObj, false);

  int16_t activeLen = pGrant->active ? strlen(pGrant->active) : 0;
  if (!grantObj.active) {
    char *tmp = taosMemoryRealloc(grantObj.active, activeLen + 1);
    if (!tmp) {
      mndReleaseGrant(pMnode, pGrant, pIter);
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    grantObj.active = tmp;
    grantObj.activeBufLen = activeLen + 1;
  } else if (grantObj.activeBufLen < activeLen + 1) {
    char *tmp = taosMemoryRealloc(grantObj.active, activeLen + 1);
    if (!tmp) {
      mndReleaseGrant(pMnode, pGrant, pIter);
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
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

  TAOS_CHECK_EXIT(grantGetDnodesMiscInfo(pMnode, grantHandle.pMachineHash));

  if (grantObj.active && grantObj.active[0] != 0) {
    if (0 != (code = grantUniqParseActiveCode(&grantObj, NULL))) {
      grantResetMaster(pMnode, 0);
    } else {
      TAOS_CHECK_EXIT(fillGrantStatusFromObj(&gStatus, &grantObj, gStatus.grantState));
    }
  } else {
    if (pGrant->upgradeTime == 0) {
      pGrant->upgradeTime = curTime != 0 ? curTime : 1;
    }
    grantResetMaster(pMnode, pGrant->upgradeTime);
  }

  if (pLastState->state == GRANT_STATE_REVOKED) {
    mndReleaseGrant(pMnode, pGrant, pIter);
  } else {
    // check machines
#ifndef GRANTS_CFG
    if (!grantObj.granted || (grantObj.flags & GRANT_ACTIVE_FLG_CHECK_MACHINE)) {
      if ((code = grantCheckMachines(pGrant, &pGrantMachines, &toRevoked)) != 0) {
        mndReleaseGrant(pMnode, pGrant, pIter);
        TAOS_CHECK_EXIT(code);
      }
    }
#endif

    mndReleaseGrant(pMnode, pGrant, pIter);

    SGrantState state = {0};
    if (toRevoked) {
      state.state = GRANT_STATE_REVOKED;
      state.reason = GRANT_STATE_REASON_MISMATCH;
      // The revoked state is only set in grantLog, gStatus.grantState is not updated in current HB loop.
      TAOS_CHECK_EXIT(mndProcessUpdGrantLog(pMnode, NULL, pGrantMachines, &state));
      // Since gStatus.grantState is only set according to grantLog.lastState(to ensure the state is persisted in
      // grantLog), the next HB is triggered immediately to update the expired state according to
      // gStatus.revokedExpireSec.
      tsGrantHBInterval = GRANT_HEART_BEAT_MIN;
    } else {
      int8_t oldState = pLastState->state;
      bool   appendState = false;
      if (oldState == GRANT_STATE_UNGRANTED) {
        if (grantObj.granted) {
          if (gStatus.expired) {
            state.state = GRANT_STATE_EXPIRED;
            state.reason = GRANT_STATE_REASON_EXPIRE;
            appendState = true;
          } else {
            state.state = GRANT_STATE_GRANTED;
            state.reason = GRANT_STATE_REASON_ALTER;
            appendState = true;
          }
        } else if (false == stated) {
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
        if (0 == gStatus.expired) {
          state.state = GRANT_STATE_GRANTED;
          state.reason = GRANT_STATE_REASON_ALTER;
          appendState = true;
        }
      }
      TAOS_CHECK_EXIT(mndProcessUpdGrantLog(pMnode, NULL, pGrantMachines, appendState ? &state : NULL));
    }
  }
  // set cluster info after parse uniq active
  grantSetClusterInfo(pMnode);
  mndProcessGrantStatusCheck();
_exit:
  taosArrayDestroy(pGrantMachines);
  if (code < 0) {
    uError("grant hb failed at line %d since %s", lino, tstrerror(code));
  }
  return code;
}

static int32_t mndProcessGrantHBImpl(SMnode *pMnode, int8_t type) {
  int32_t code = 0;
  int32_t lino = 0;

  if (!pMnode) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PTR);
  }

  TAOS_CHECK_EXIT(grantCheckClusterInfo(pMnode));

  grantClusterTime = grantClusterEpoch + mndGetClusterUpTime(pMnode);
  TAOS_CHECK_EXIT(mndProcessGrantHBSyncInfo(pMnode, type));
  COMPARE_SET_VAL(gStatus.nDiskCfg, grantHandle.nDiskCfg, !=);

  // reset grantHandle and send gStatus to all dnodes, no resp needed
  taosArrayClear(grantHandle.pDnodeInfo);
  tSimpleHashClear(grantHandle.pMachineHash);
  grantHandle.nDiskCfg = 0;

  TAOS_CHECK_EXIT(mndGetDnodeData(pMnode, grantHandle.pDnodeInfo));

  int32_t dnodeSize = taosArrayGetSize(grantHandle.pDnodeInfo);
  int32_t contLen = 0;
  if (dnodeSize > 1) {
    void *pCont = NULL;
    void *qCont = NULL;
    TAOS_CHECK_EXIT(tSerializeGrantStatus(NULL, 0, &gStatus, grantClusterTime, &contLen));
    pCont = rpcMallocCont(contLen);
    if (!pCont) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }

    if ((code = tSerializeGrantStatus(pCont, contLen, &gStatus, grantClusterTime, NULL)) < 0) {
      rpcFreeCont(pCont);
      TAOS_CHECK_EXIT(code);
    }

    for (int32_t i = 0; i < dnodeSize; ++i) {
      SDnodeInfo *info = (SDnodeInfo *)TARRAY_GET_ELEM(grantHandle.pDnodeInfo, i);
      if (info->offlineReason != DND_REASON_ONLINE) {
        uDebug("not send grant status to dnode:%d since offline state:%d", info->id, info->offlineReason);
        continue;
      }

      if (tsServerPort == info->ep.port && 0 == strncmp(tsLocalFqdn, info->ep.fqdn, TSDB_FQDN_LEN)) {
        uDebug("not send grant status to dnode:%d since duplicated node", info->id);
        continue;
      }

      if (i < dnodeSize - 1) {
        qCont = rpcMallocCont(contLen);
        if (!qCont) {
          rpcFreeCont(pCont);
          TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
        }
        (void)memcpy(qCont, pCont, contLen);
        TAOS_UNUSED(mndSendGrantStatusToDnode(pMnode, info, contLen, qCont));
      } else {
        TAOS_UNUSED(mndSendGrantStatusToDnode(pMnode, info, contLen, pCont));
        pCont = NULL;
      }
    }

    rpcFreeCont(pCont);
  }

_exit:
  if (code < 0) {
    uError("failed to process grant hb at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
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

static uint8_t grantGetMachineFlag(const char *machineCode) {
  uint8_t  flag = 0;
  int32_t  outlen = 0;
  uint8_t *machine = NULL;
  TAOS_UNUSED(base64_decode(machineCode, TSDB_MACHINE_ID_LEN, &outlen, &machine));
  if (machine) {
    flag = machine[0];
    taosMemoryFree(machine);
  }
  return flag;
}

void grantParseParameter() {
  char   *key = NULL;
  int32_t code = tGetMachineId(&key);  //  grantGetMachineSerials();
  if (key != NULL) {
    uint8_t flag = grantGetMachineFlag(key);
    if (flag >= 1 && flag <= 5) {
      fprintf(stdout, "machine code(%" PRIu8 "): %s\n", flag, key);
    } else {
      fprintf(stdout,
              "failed to generate machine code since invalid flag:%" PRIu8 ", please contact TAOS Data for support\n",
              flag);
    }
    taosMemoryFree(key);
  } else {
    fprintf(stderr, "failed to generate machine code since %s, please contact TAOS Data for support\n",
            tstrerror(code));
  }
  exit(EXIT_SUCCESS);
}

static int32_t grantSecondsToString(int64_t seconds, char *ts) {
  time_t    sec = seconds;
  struct tm ptm;
  if (taosLocalTime(&sec, &ptm, ts, GRANT_TS_SEC_LEN) != NULL) {
    if (strftime(ts, GRANT_TS_SEC_LEN, "%Y-%m-%d %H:%M:%S", &ptm)) {
      return 0;
    }
  }
  ts[0] = 0;
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
  gStatus.curSubscriptions = grantGetClusterCurTopics(pMnode);
  gStatus.curViews = grantGetClusterCurViews(pMnode);
}

static int32_t tSerializeGrantNotify(void *buf, int32_t bufLen, GrantNotify *pNotify, uint32_t *pLen) {
  int32_t  code = 0;
  int32_t  lino = 0;
  uint32_t tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeU64(&encoder, pNotify->curTimeSeries));

  tEndEncode(&encoder);

  tlen = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  if (pLen) *pLen = tlen;
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int32_t mndSendGrantNotifyToDnode(SMnode *pMnode, SDnodeInfo *pDnodeInfo, SGrantNotify *pNotify) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t contLen = 0;

  TAOS_CHECK_EXIT(tSerializeGrantNotify(NULL, 0, pNotify, &contLen));
  void *pCont = rpcMallocCont(contLen);
  if (!pCont) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  if ((code = tSerializeGrantNotify(pCont, contLen, pNotify, NULL)) < 0) {
    rpcFreeCont(pCont);
    TAOS_CHECK_EXIT(code);
  }

  SRpcMsg rpcMsg = {.pCont = pCont, .contLen = contLen, .msgType = TDMT_MND_GRANT_NOTIFY, .info.noResp = 1};

  uDebug("send grant notify msg to dnode:%d %s:%" PRIu16, pDnodeInfo->id, pDnodeInfo->ep.fqdn, pDnodeInfo->ep.port);

  SEpSet epSet = {.numOfEps = 1};
  tstrncpy(epSet.eps[0].fqdn, pDnodeInfo->ep.fqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = pDnodeInfo->ep.port;
  TAOS_CHECK_EXIT(tmsgSendReq(&epSet, &rpcMsg));

_exit:
  if (code < 0) {
    uError("failed to send grant notify to dnode %d at line %d since %s", pDnodeInfo->id, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t mndProcessGrantNotify(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  int32_t code = 0;
  int32_t lino = 0;
  int32_t dnodeSize = mndGetDnodeSize(pMnode);
  int64_t notifyTimeSeries = atomic_load_64(&gStatus.curTimeSeries);
  SArray *pDnodeInfo = NULL;

  if (dnodeSize <= 1) {
    atomic_store_64(&grantNotifyTimeSeries, notifyTimeSeries);
    return 0;
  }

  pDnodeInfo = taosArrayInit(dnodeSize, sizeof(SDnodeInfo));
  if (!pDnodeInfo) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  TAOS_CHECK_EXIT(mndGetDnodeData(pMnode, pDnodeInfo));

  int32_t dInfoSize = taosArrayGetSize(pDnodeInfo);
  atomic_store_64(&grantNotifyTimeSeries, notifyTimeSeries);
  SGrantNotify notify = {.curTimeSeries = notifyTimeSeries};
  for (int32_t i = 0; i < dInfoSize; ++i) {
    SDnodeInfo *info = (SDnodeInfo *)TARRAY_GET_ELEM(pDnodeInfo, i);
    if (info->offlineReason != DND_REASON_ONLINE) {
      uDebug("not send grant notify to dnode:%d since offline state:%d", info->id, info->offlineReason);
      continue;
    }

    if (tsServerPort == info->ep.port && 0 == strncmp(tsLocalFqdn, info->ep.fqdn, TSDB_FQDN_LEN)) {
      uDebug("not send grant notify to dnode:%d since duplicated node", info->id);
      continue;
    }
    TAOS_UNUSED(mndSendGrantNotifyToDnode(pMnode, info, &notify));
  }

  grantNotifyTimestamp = taosGetTimestampMs();

_exit:
  taosArrayDestroy(pDnodeInfo);
  if (code < 0) {
    uError("failed to process grant notify at line %d since %s", lino, tstrerror(code));
  }
  return 0;
}

int32_t mndUpdClusterInfo(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  int64_t lastTimeSeries = atomic_load_64(&gStatus.curTimeSeries);

  gStatus.curTimeSeries = grantGetClusterCurTimeSeries(pMnode);

#ifndef GRANTS_CFG
  if ((gStatus.curTimeSeries > gStatus.limitTimeSeries) ||
      ((gStatus.curTimeSeries > lastTimeSeries) && (taosGetTimestampMs() - grantNotifyTimestamp > 500))) {
    TAOS_UNUSED(mndProcessGrantNotify(pReq));
  } else {
    if (atomic_load_64(&gStatus.curTimeSeries) < atomic_load_64(&grantNotifyTimeSeries)) {
      TAOS_UNUSED(mndProcessGrantNotify(pReq));
    }
  }
#endif

  return 0;
}

static void grantDataInsSetDefault(SGrantDataIn *pDataIns, int32_t num, int64_t expireSec) {
  for (int32_t i = 0; i < num; ++i) {
    (pDataIns + i)->expireSec = grantHandle.showDataIns[i] ? expireSec : GRANT_UNIQ_UNDEFINED;
    (pDataIns + i)->speed = GRANT_UNIQ_DFT_DATAIN_SPEED;
    (pDataIns + i)->number = (i == CONN_TYPE_CSV ? GRANT_UNIQ_UNLIMITED : GRANT_UNIQ_DFT_DATAIN_NUM);
  }
}

/**
 * @brief init the grant status after mnode startup
 *
 * @param pMnode
 */
static void grantResetMaster(SMnode *pMnode, int64_t upgradeSec) {
#ifndef GRANTS_CFG
  grantRetrieveGrantInfo(pMnode);
  int64_t curTime = taosGetTimestampMs() / 1000;
  int64_t grantCurTime = grantGetCurTime(curTime, true);
  int64_t baseSeconds = upgradeSec;
  int64_t clusterCreateTime = 0;
  bool    revoked = gStatus.grantState == GRANT_STATE_REVOKED;

  if (baseSeconds == 0) {
    if (grantClusterEpoch == 0) {
      clusterCreateTime = grantGetClusterCreateTime(pMnode);
      if (clusterCreateTime != 0) COMPARE_SET_VAL(grantClusterEpoch, clusterCreateTime, !=);
    }
    baseSeconds = grantClusterEpoch;
  }

  if (baseSeconds != 0) {
    gStatus.basicExpireSec = baseSeconds + GRANT_DEFAULT;

    // basic item
    int64_t expireSec = revoked ? gStatus.revokedExpireSec : gStatus.basicExpireSec;
    gStatus.expired = expireSec > grantCurTime ? 0 : 1;
    if (gStatus.expired) {
      char ts[GRANT_TS_SEC_LEN] = {0};
      TAOS_UNUSED(grantSecondsToString(expireSec, ts));
      uWarn("grant cluster expired at %s %" PRIi64 ", curtime: %" PRIi64, ts, (int64_t)expireSec, grantCurTime);
    }
    gStatus.serviceExpireSec = grantClusterEpoch;

    // optional items
    int64_t optExpireSec =
        revoked ? TMIN(gStatus.revokedExpireSec, (int64_t)gStatus.basicExpireSec) : gStatus.basicExpireSec;
    GRANT_EXPIRE_TUNE_INDUSTRY(optExpireSec);
    int8_t optExpired = optExpireSec > grantCurTime ? 0 : 1;

    GRANT_OPT_EXPIRE_ASSIGN(gStatus.multiTierExpireSec, optExpireSec, gStatus.multiTierExpired, optExpired,
                            GRANT_OPT_STORAGE);
    GRANT_OPT_EXPIRE_ASSIGN(gStatus.streamExpireSec, optExpireSec, gStatus.streamExpired, optExpired, GRANT_OPT_STREAM);
    GRANT_OPT_EXPIRE_ASSIGN(gStatus.subscriptionExpireSec, optExpireSec, gStatus.subscriptionExpired, optExpired,
                            GRANT_OPT_SUBSCRIPTION);
    GRANT_OPT_EXPIRE_ASSIGN(gStatus.auditExpireSec, optExpireSec, gStatus.auditExpired, optExpired, GRANT_OPT_AUDIT);
    GRANT_OPT_EXPIRE_ASSIGN(gStatus.csvExpireSec, optExpireSec, gStatus.csvExpired, optExpired, GRANT_OPT_CSV);
    GRANT_OPT_EXPIRE_ASSIGN(gStatus.bakRstExpireSec, optExpireSec, gStatus.placeHolder, 0, GRANT_OPT_DATA_BAK_RST);
    GRANT_OPT_EXPIRE_ASSIGN(gStatus.viewExpireSec, optExpireSec, gStatus.viewExpired, optExpired, GRANT_OPT_VIEW);
    GRANT_OPT_EXPIRE_ASSIGN(gStatus.objectStorageExpireSec, optExpireSec, gStatus.objectStorageExpired, optExpired,
                            GRANT_OPT_OBJECT_STORAGE);
    GRANT_OPT_EXPIRE_ASSIGN(gStatus.activeActiveExpireSec, optExpireSec, gStatus.placeHolder, 0,
                            GRANT_OPT_ACTIVE_ACTIVE);

#ifndef ASSERT_NOT_CORE
    GRANT_OPT_EXPIRE_ASSIGN(gStatus.dualReplicaHAExpireSec, optExpireSec, gStatus.dualReplicaHAExpired, optExpired,
                            GRANT_OPT_DUAL_REPLICA_HA);
    GRANT_OPT_EXPIRE_ASSIGN(gStatus.dbEncryptionExpireSec, optExpireSec, gStatus.dbEncryptionExpired, optExpired,
                            GRANT_OPT_DB_ENCRYPTION);
#else  // release version
    int64_t optExpireEpoch = grantClusterEpoch;
    GRANT_EXPIRE_TUNE_INDUSTRY(optExpireEpoch);
    GRANT_OPT_EXPIRE_ASSIGN(gStatus.dualReplicaHAExpireSec, optExpireEpoch, gStatus.dualReplicaHAExpired, true,
                            GRANT_OPT_DUAL_REPLICA_HA);
    GRANT_OPT_EXPIRE_ASSIGN(gStatus.dbEncryptionExpireSec, optExpireEpoch, gStatus.dbEncryptionExpired, true,
                            GRANT_OPT_DB_ENCRYPTION);
#endif
    GRANT_OPT_EXPIRE_ASSIGN(gStatus.dataSyncExpireSec, optExpireSec, gStatus.placeHolder, 0, GRANT_OPT_DATA_SYNC);

    // fixed dataIns
    grantDataInsSetDefault(gStatus.dataIns, CONN_TYPE_DYN_MAX, optExpireSec);
  }
#else
  gStatus.serviceExpireSec = GRANT_UNIQ_UNLIMITED;
  grantDataInsSetDefault(gStatus.dataIns, CONN_TYPE_DYN_MAX, GRANT_UNIQ_DFT_DATAIN_EXPIRE);
#endif
}

void grantReset(SMnode *pMnode, EGrantType grant, uint64_t value) {
  switch (grant) {
    case TSDB_GRANT_ALL: {
      grantResetMaster(pMnode, 0);
      grantSetClusterInfo(pMnode);
    } break;
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
  if (gStatus.limitTimeSeries == GRANT_UNIQ_UNLIMITED || gStatus.curTimeSeries < gStatus.limitTimeSeries) {
    return 0;
  }

  uError("grant failed to create table/add column, exist:%" PRIi64 ", reason:grant timeseries limited",
         gStatus.curTimeSeries);
  return TSDB_CODE_GRANT_TIMESERIES_LIMITED;
}

static int32_t grantCheckAccts() { return 0; }

static int32_t grantCheckDnodes() {
  if (gStatus.limitDnodes == GRANT_UNIQ_UNLIMITED) {
    return 0;
  }
  if (grantHandle.pMnode) gStatus.curDnodes = grantGetClusterCurDnodes(grantHandle.pMnode);
  if (gStatus.curDnodes < gStatus.limitDnodes) {
    return 0;
  }
  uError("grant failed to create dnode, exist:%d, reason:grant dnode limited", (int32_t)gStatus.curDnodes);
  return TSDB_CODE_GRANT_DNODE_LIMITED;
}

static int32_t grantCheckGrantSpeed() { return TSDB_CODE_SUCCESS; }
static int32_t grantCheckQueryTime() { return TSDB_CODE_SUCCESS; }
static int32_t grantCheckConns() { return TSDB_CODE_SUCCESS; }

static int32_t grantCheckStreams(bool checkNum) {
  int32_t code = 0;
  if (gStatus.expired || gStatus.streamExpired) {
    code = gStatus.expired ? TSDB_CODE_GRANT_BASIC_EXPIRED : TSDB_CODE_GRANT_STREAM_EXPIRED;
  } else if (checkNum && gStatus.limitStreams != GRANT_UNIQ_UNLIMITED) {
    if (grantHandle.pMnode) gStatus.curStreams = grantGetClusterCurStreams(grantHandle.pMnode);
    if (gStatus.curStreams >= gStatus.limitStreams) code = TSDB_CODE_GRANT_STREAM_LIMITED;
  }

  if (code < 0) {
    uError("grant failed to check stream, expire:%" PRIi64 ", num:%d, reason:stream limited",
           (int64_t)gStatus.streamExpireSec, (int32_t)gStatus.curStreams);
  }

  return code;
}

static int32_t grantCheckSubscriptions(bool checkNum) {
  int32_t code = 0;
  if (gStatus.expired || gStatus.subscriptionExpired) {
    code = gStatus.expired ? TSDB_CODE_GRANT_BASIC_EXPIRED : TSDB_CODE_GRANT_SUBSCRIPTION_EXPIRED;
  } else if (checkNum && gStatus.limitSubscriptions != GRANT_UNIQ_UNLIMITED) {
    if (grantHandle.pMnode) gStatus.curSubscriptions = grantGetClusterCurTopics(grantHandle.pMnode);
    if (gStatus.curSubscriptions >= gStatus.limitSubscriptions) code = TSDB_CODE_GRANT_SUBSCRIPTION_LIMITED;
  }

  if (code < 0) {
    uError("grant failed to check subscription, expire:%" PRIi64 ", num:%d, reason:subscription limited",
           (int64_t)gStatus.subscriptionExpireSec, (int32_t)gStatus.curSubscriptions);
  }

  return code;
}

static int32_t grantCheckViews(bool checkNum, int8_t traceLevel) {
  int32_t code = 0;
  if (gStatus.expired || gStatus.viewExpired) {
    code = gStatus.expired ? TSDB_CODE_GRANT_BASIC_EXPIRED : TSDB_CODE_GRANT_VIEW_EXPIRED;
  } else if (checkNum && gStatus.limitViews != GRANT_UNIQ_UNLIMITED) {
    if (grantHandle.pMnode) gStatus.curViews = grantGetClusterCurViews(grantHandle.pMnode);
    if (gStatus.curViews >= gStatus.limitViews) code = TSDB_CODE_GRANT_VIEW_LIMITED;
  }

  if (code < 0) {
    if (DEBUG_ERROR == traceLevel) {
      uError("grant failed to check view, expire:%" PRIi64 ", num:%d, reason:view limited",
             (int64_t)gStatus.viewExpireSec, (int32_t)gStatus.curViews);
    } else {
      uDebug("grant failed to check view, expire:%" PRIi64 ", num:%d, reason:view limited",
             (int64_t)gStatus.viewExpireSec, (int32_t)gStatus.curViews);
    }
  }

  return code;
}

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

int32_t grantCheckExpire(EGrantType grant) {
  switch (grant) {
    case TSDB_GRANT_STREAMS:
      return grantCheckStreams(false);
    case TSDB_GRANT_SUBSCRIPTION:
      return grantCheckSubscriptions(false);
    case TSDB_GRANT_VIEW:
      return grantCheckViews(false, DEBUG_ERROR);
    default:
      uError("undefined grant check expire type:%d", grant);
      break;
  }
  return TSDB_CODE_SUCCESS;
}

int64_t grantRemain(EGrantType grant) {
  switch (grant) {
    case TSDB_GRANT_TIMESERIES:
      return gStatus.limitTimeSeries == GRANT_UNIQ_UNLIMITED ? INT64_MAX
                                                             : gStatus.limitTimeSeries - gStatus.curTimeSeries;
    default:
      break;
  }
  return 0;
}

int32_t grantCheck(EGrantType grant) {
  switch (grant) {
    case TSDB_GRANT_TIME:
      return GRANT_EXPIRED(gStatus.expired);
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
      return TSDB_CODE_SUCCESS;
    case TSDB_GRANT_SPEED:
      return grantCheckGrantSpeed();
    case TSDB_GRANT_QUERY_TIME:
      return grantCheckQueryTime();
    case TSDB_GRANT_CONNS:
      return grantCheckConns();
    case TSDB_GRANT_STREAMS:
      return grantCheckStreams(true);
    case TSDB_GRANT_CPU_CORES:
      return grantCheckCpuCores();
    case TSDB_GRANT_SUBSCRIPTION:
      return grantCheckSubscriptions(true);
    case TSDB_GRANT_VIEW:
      return grantCheckViews(true, DEBUG_ERROR);
    case TSDB_GRANT_AUDIT:
      return GRANT_EXPIRED_OPT(gStatus.expired, gStatus.auditExpired, TSDB_CODE_GRANT_AUDIT_EXPIRED);
    case TSDB_GRANT_CSV:
      return GRANT_EXPIRED_OPT(gStatus.expired, gStatus.csvExpired, TSDB_CODE_GRANT_CSV_EXPIRED);
    case TSDB_GRANT_MULTI_TIER:
      return GRANT_EXPIRED_OPT(gStatus.expired, gStatus.multiTierExpired, TSDB_CODE_GRANT_MULTI_STORAGE_EXPIRED);
    case TSDB_GRANT_OBJECT_STORAGE:
      return GRANT_EXPIRED_OPT(gStatus.expired, gStatus.objectStorageExpired, TSDB_CODE_GRANT_OBJECT_STROAGE_EXPIRED);
    case TSDB_GRANT_DUAL_REPLICA_HA:
      return GRANT_EXPIRED_OPT(gStatus.expired, gStatus.dualReplicaHAExpired, TSDB_CODE_GRANT_DUAL_REPLICA_HA_EXPIRED);
    case TSDB_GRANT_DB_ENCRYPTION:
      return GRANT_EXPIRED_OPT(gStatus.expired, gStatus.dbEncryptionExpired, TSDB_CODE_GRANT_DB_ENCRYPTION_EXPIRED);
    default:
      break;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t mndCfgDnodeReq(SDnodeInfo *pDnodeInfo, const char *cfg, const char *val) {
  int32_t       code = 0;
  int32_t       lino = 0;
  SMCfgDnodeReq req = {0};
  req.dnodeId = pDnodeInfo->id;
  tstrncpy(req.config, cfg, TSDB_DNODE_CONFIG_LEN);
  tstrncpy(req.value, val, TSDB_DNODE_VALUE_LEN);

  int32_t contLen = tSerializeSMCfgDnodeReq(NULL, 0, &req);
  if (contLen < 0) {
    TAOS_CHECK_EXIT(contLen);
  }
  void *pCont = rpcMallocCont(contLen);
  if (!pCont) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  contLen = tSerializeSMCfgDnodeReq(pCont, contLen, &req);
  if (contLen < 0) {
    rpcFreeCont(pCont);
    TAOS_CHECK_EXIT(contLen);
  }

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

  TAOS_CHECK_EXIT(tmsgSendReq(&epSet, &rpcMsg));

_exit:
  if (code != 0) {
    uError("failed to send cfg dnode req for grant to dnode:%d at line %d since %s", pDnodeInfo->id, lino,
           tstrerror(code));
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t grantOptExpireDaysCheck(SMnode *pMnode, SGrantUniqObj *pObj, int64_t upgradeTime) {
  int32_t code = 0;
  int32_t lino = 0;
  int32_t basicExpireDay = pObj->expireDays[GRANT_OPT_BASIC];
  bool    basicLtDefault = false;

  if (pObj->granted == 0) goto _exit;

  if (basicExpireDay == GRANT_UNIQ_UNDEFINED) {
    code = TSDB_CODE_GRANT_LACK_OF_BASIC;
    TSDB_CHECK_CODE(code, lino, _exit);
  } else if (basicExpireDay == GRANT_UNIQ_UNLIMITED) {
    goto _exit;
  }

  int64_t basicExpireSec = (int64_t)basicExpireDay * 86400;
  int64_t defaultExpireSec = 0;
  if (upgradeTime > 0) {
    defaultExpireSec = upgradeTime + GRANT_DEFAULT;
  } else {
    if (grantClusterEpoch == 0) {
      int64_t clusterCreateTime = grantGetClusterCreateTime(pMnode);
      if (clusterCreateTime > 0) COMPARE_SET_VAL(grantClusterEpoch, clusterCreateTime, !=);
    }
    defaultExpireSec = grantClusterEpoch + GRANT_DEFAULT;
  }
  if (basicExpireSec < defaultExpireSec) basicLtDefault = true;

  for (int32_t i = 1; i < GRANT_OPT_MAX; ++i) {
    GRANT_OPT_EXPIRE_CHECK(pObj->expireDays[i], gGrantName[i]);
  }

  for (int32_t i = 0; i < GRANT_UNIQ_KNOWN_DATAIN_VALS; i += 3) {
    GRANT_OPT_EXPIRE_CHECK(pObj->dataIns[i], gConnName[i / 3]);
  }

  int32_t size = taosArrayGetSize(pObj->pDataIns);
  for (int32_t i = 0; i < size; ++i) {
    SGrantDataIns *pItem = TARRAY_GET_ELEM(pObj->pDataIns, i);
    GRANT_OPT_EXPIRE_CHECK(pItem->expire, pItem->name);
  }

  size = taosArrayGetSize(pObj->pItem64);
  for (int32_t i = 0; i < size; ++i) {
    SGrantItem64 *pItem = TARRAY_GET_ELEM(pObj->pItem64, i);
    GRANT_OPT_EXPIRE_CHECK(pItem->expire, pItem->name);
  }

  size = taosArrayGetSize(pObj->pItemI64);
  for (int32_t i = 0; i < size; ++i) {
    SGrantItemI64 *pItem = TARRAY_GET_ELEM(pObj->pItemI64, i);
    GRANT_OPT_EXPIRE_CHECK(pItem->expire, gGrantName[pItem->index]);
  }

  size = taosArrayGetSize(pObj->pItemN64);
  for (int32_t i = 0; i < size; ++i) {
    SGrantItem64 *pItem = TARRAY_GET_ELEM(pObj->pItemN64, i);
    GRANT_OPT_EXPIRE_CHECK(pItem->expire, pItem->name);
  }

_exit:
  if (code < 0) {
    uError("grant optional items check failed at line %d since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t grantCheckGrantItems(SMnode *pMnode, SGrantUniqObj *pObj) {
  // basic
  if ((pObj->limitTimeSeries > GRANT_UNIQ_UNLIMITED) &&
      ((gStatus.curTimeSeries = grantGetClusterCurTimeSeries(pMnode)) > pObj->limitTimeSeries)) {
    GRANT_CHECK_ERROR_LOG("time series", gStatus.curTimeSeries, pObj->limitTimeSeries);
    return TSDB_CODE_GRANT_TIMESERIES_LIMITED;
  }
  if ((pObj->limitDnodes > GRANT_UNIQ_UNLIMITED) &&
      ((gStatus.curDnodes = grantGetClusterCurDnodes(pMnode)) > pObj->limitDnodes)) {
    GRANT_CHECK_ERROR_LOG("dnodes", gStatus.curDnodes, pObj->limitDnodes);
    return TSDB_CODE_GRANT_DNODE_LIMITED;
  }
  if ((pObj->limitCpuCores > GRANT_UNIQ_UNLIMITED) &&
      ((gStatus.curCpuCores = grantGetClusterCurCores(pMnode)) > pObj->limitCpuCores)) {
    GRANT_CHECK_ERROR_LOG("cpu cores", gStatus.curCpuCores, pObj->limitCpuCores);
    return TSDB_CODE_GRANT_CPU_LIMITED;
  }

  // optional
  if ((pObj->limitStreams > GRANT_UNIQ_UNLIMITED) &&
      ((gStatus.curStreams = grantGetClusterCurStreams(pMnode)) > pObj->limitStreams)) {
    GRANT_CHECK_ERROR_LOG("streams", gStatus.curStreams, pObj->limitStreams);
    return TSDB_CODE_GRANT_STREAM_LIMITED;
  }
  if ((pObj->limitSubscriptions > GRANT_UNIQ_UNLIMITED) &&
      ((gStatus.curSubscriptions = grantGetClusterCurTopics(pMnode)) > pObj->limitSubscriptions)) {
    GRANT_CHECK_ERROR_LOG("topics", gStatus.curSubscriptions, pObj->limitSubscriptions);
    return TSDB_CODE_GRANT_SUBSCRIPTION_LIMITED;
  }
  if ((pObj->limitViews > GRANT_UNIQ_UNLIMITED) &&
      ((gStatus.curViews = grantGetClusterCurViews(pMnode)) > pObj->limitViews)) {
    GRANT_CHECK_ERROR_LOG("views", gStatus.curViews, pObj->limitViews);
    return TSDB_CODE_GRANT_VIEW_LIMITED;
  }

  return 0;
}

static int32_t machineCmprFn(const void *p1, const void *p2) { return memcmp(p1, p2, TSDB_MACHINE_ID_LEN); }

// mnode-write thread
int32_t grantAlterActiveCode(SMnode *pMnode, SGrantLogObj *pObj, const char *oldActive, const char *newActive,
                             char **mergeActive) {
  int32_t       code = 0;
  int32_t       lino = 0;
  SGrantUniqObj newObj = {0};
  SGrantUniqObj oldObj = {0};
  SGrantUniqObj mergeObj = {0};
  SSHashObj    *pMachineHash = NULL;
  SArray       *pMachines = NULL;
  bool          revoked = false;

  // step 1: basic judgement and init
  if (!newActive || newActive[0] == 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_INVALID_PTR);
  }

  if (grantObj.clusterId[0] == 0) {
    grantSetClusterId(pMnode, grantObj.clusterId);
    if (grantObj.clusterId[0] == 0) {
      TAOS_CHECK_EXIT(TSDB_CODE_APP_IS_STARTING);
    }
  }

  SGrantState lastState = {0};
  if (0 != (code = mndGrantGetLastState(pMnode, &lastState))) {
    if (code != TSDB_CODE_GRANT_OBJ_NOT_EXIST) {
      TAOS_CHECK_EXIT(code);
    }
  } else if (lastState.state == GRANT_STATE_REVOKED) {
    revoked = true;
  }

  // check duplicated active
  for (int32_t i = 0; i < pObj->nActives; ++i) {
    if (0 == memcmp(&pObj->actives[i].active[0], newActive, GRANT_ACTIVE_HEAD_LEN)) {
      TAOS_CHECK_EXIT(TSDB_CODE_GRANT_DUPLICATED_ACTIVE);
    }
  }

  if (!(pMachineHash = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY)))) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }
  TAOS_CHECK_EXIT(grantGetDnodesMiscInfo(pMnode, pMachineHash));

  // step 2: parse new
  (void)memcpy(newObj.clusterId, grantObj.clusterId, GRANT_CLUSTER_ID_LEN);
  grantObjInit(&newObj, 0);
  int32_t newActiveLen = strlen(newActive);
  if (!(newObj.active = taosMemoryMalloc(newActiveLen + 1))) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }
  newObj.activeBufLen = newActiveLen + 1;
  tstrncpy(newObj.active, newActive, newActiveLen + 1);

  code = grantUniqParseActiveCode(&newObj, NULL);
  if (code < 0 || !newObj.granted) {
    code = code != 0 ? code : TSDB_CODE_GRANT_PAR_IVLD_ACTIVE;
    TAOS_CHECK_EXIT(code);
  } else {
    int64_t curTime = taosGetTimestampMs() / 1000;
    if (newObj.validDays > 0) {  // check valid days
      if (curTime - (int64_t)newObj.distribute > (int64_t)newObj.validDays * 86400) {
        uWarn("now:%" PRIi64 " minus distribute time:%" PRIi64 " larger than valid time:%" PRIi64, curTime,
              (int64_t)newObj.distribute, (int64_t)newObj.validDays * 86400);
        TAOS_CHECK_EXIT(TSDB_CODE_GRANT_PAR_IVLD_DIST);
      }
    }

    // check expire
    int64_t basicExpire = newObj.expireDays[GRANT_OPT_BASIC];
    if (basicExpire != GRANT_UNIQ_UNDEFINED && basicExpire != GRANT_UNIQ_UNLIMITED) {
      int64_t grantCurTime = grantGetCurTime(curTime, newObj.flags & GRANT_ACTIVE_FLG_CHECK_UPTIME);
      if (basicExpire * 86400 <= grantCurTime) {
        TAOS_CHECK_EXIT(TSDB_CODE_GRANT_BASIC_EXPIRED);
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
      TAOS_CHECK_EXIT(TSDB_CODE_GRANT_LAST_ACTIVE_NOT_FOUND);
    }
  }

#ifndef GRANTS_CFG
  if (newObj.token[1] > 0) {  // check machines
    if (!(pMachines = taosArrayInit(tSimpleHashGetSize(pMachineHash), TSDB_MACHINE_ID_LEN))) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    void   *pe = NULL;
    int32_t iter = 0;
    while ((pe = tSimpleHashIterate(pMachineHash, pe, &iter)) != NULL) {
      void *key = tSimpleHashGetKey(pe, NULL);
      if (taosArrayPush(pMachines, key) == NULL) {
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
    }
    int32_t nFinalMachine = taosArrayGetSize(pMachines);
    if (nFinalMachine > 1) taosArraySort(pMachines, machineCmprFn);

    TSCKSUM machineChksum = 0;
    if (nFinalMachine > 0) {
      machineChksum = taosCalcChecksum(0, TARRAY_GET_ELEM(pMachines, 0), nFinalMachine * TSDB_MACHINE_ID_LEN);
    }
    if (machineChksum != newObj.token[1]) {
      TAOS_CHECK_EXIT(TSDB_CODE_GRANT_MACHINES_MISMATCH);
    }
    // cleanup pGrant->pMachines in revoked state
    if (revoked) taosArrayClear(pObj->pMachines);
  } else if (revoked) {
    TAOS_CHECK_EXIT(TSDB_CODE_GRANT_UNLICENSED_CLUSTER);
  }
#endif

  TAOS_CHECK_EXIT(grantCheckGrantItems(pMnode, &newObj));

  // step 3: parse old
  (void)memcpy(oldObj.clusterId, grantObj.clusterId, GRANT_CLUSTER_ID_LEN);
  grantObjInit(&oldObj, 0);
  if (oldActive) {
    int32_t oldActiveLen = strlen(oldActive);
    if (!(oldObj.active = taosMemoryMalloc(oldActiveLen + 1))) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    oldObj.activeBufLen = oldActiveLen + 1;
    tstrncpy(oldObj.active, oldActive, oldActiveLen + 1);
    code = grantUniqParseActiveCode(&oldObj, NULL);
    if (code < 0 || !oldObj.granted) {
      code = code != 0 ? code : TSDB_CODE_GRANT_PAR_IVLD_ACTIVE;
      if ((newObj.flags & GRANT_ACTIVE_FLG_SKIP_FAIL_OLD)) {  // skip if old active parse failed
        uInfo("old active parse failed since %s, continue to alter as new flags is:0x%x", tstrerror(code),
              newObj.flags);
        code = 0;
      } else {
        code = code != 0 ? code : TSDB_CODE_GRANT_PAR_IVLD_ACTIVE;
        uError("old active parse failed since %s, active:%s", tstrerror(code), oldActive);
        TAOS_CHECK_EXIT(code);
      }
    }
  }

  // check basic functions: 1) first activeCode;  2) in revoked state; 3) in expired state
  if (oldObj.granted == 0 || lastState.state == GRANT_STATE_REVOKED || lastState.state == GRANT_STATE_EXPIRED) {
    if (newObj.expireDays[GRANT_OPT_BASIC] == GRANT_UNIQ_UNDEFINED || newObj.limitTimeSeries == GRANT_UNIQ_UNDEFINED ||
        newObj.limitDnodes == GRANT_UNIQ_UNDEFINED || newObj.limitCpuCores == GRANT_UNIQ_UNDEFINED) {
      TAOS_CHECK_EXIT(TSDB_CODE_GRANT_LACK_OF_BASIC);
    }
  }

  // step 4: merge active code
  TAOS_CHECK_EXIT(grantUniqMergeActiveCode(&oldObj, &newObj, &mergeObj));

  TAOS_CHECK_EXIT(grantOptExpireDaysCheck(pMnode, mergeObj.granted ? &mergeObj : &newObj, pObj->upgradeTime));

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
  if (code < 0) {
    uError("failed to alter grant active:%s at line %d since %s", newActive, lino, tstrerror(code));
  }
  return code;
}

static int32_t mndRetrieveGrant(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t code = 0;
  int32_t lino = 0;
  int32_t numOfRows = 0;
  int32_t cols = 0;
  char   *pWrite = NULL;
  char    tmp[GRANTS_COL_MAX_LEN] = {0};
  char    tmp1[GRANTS_COL_MAX_LEN] = {0};
  char    ts[GRANT_TS_SEC_LEN] = {0};

  if (pShow->numOfRows < 1) {
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    const char      *src = GRANT_VERSION;
    (void)snprintf(tmp1, GRANTS_COL_MAX_LEN, "%s %s", TD_PRODUCT_NAME, src);
    STR_WITH_SIZE_TO_VARSTR(tmp, tmp1, strlen(tmp1));
    COL_DATA_SET_VAL_GOTO(tmp, false, NULL, _exit);

    if (gStatus.grantState == GRANT_STATE_REVOKED) {
      GRANT_EXPIRE_SHOW(gStatus.revokedExpireSec);
    } else {
      GRANT_EXPIRE_SHOW(gStatus.basicExpireSec);
    }

    GRANT_EXPIRE_SHOW(gStatus.serviceExpireSec);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = (gStatus.expired || (gStatus.multiTierExpired && gStatus.nDiskCfg > 1)) ? "true" : "false";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    COL_DATA_SET_VAL_GOTO(tmp, false, NULL, _exit);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (gStatus.grantState < 0 || gStatus.grantState > GRANT_STATE_MAX) {
      src = "unknown";
    } else {
      src = gGrantState[gStatus.grantState];
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    COL_DATA_SET_VAL_GOTO(tmp, false, NULL, _exit);

    GRANT_ITEM_SHOW(gStatus.curTimeSeries, gStatus.limitTimeSeries, 64);
    GRANT_ITEM_SHOW(gStatus.curDnodes, gStatus.limitDnodes, 16);
    GRANT_ITEM_SHOW(gStatus.curCpuCores, gStatus.limitCpuCores, 32);

    ++numOfRows;
  }

  pShow->numOfRows += numOfRows;
_exit:
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextGrant(SMnode *pMnode, void *pIter) {}

static int32_t mndRetrieveGrantFullItem(SSDataBlock *pBlock, int32_t *numOfRows, const char *name, const char *display,
                                        int64_t expire, int64_t curVal, int64_t limit, bool isDataIn, bool optional) {
  int32_t cols = 0;
  int32_t colLen = GRANTS_COL_MAX_LEN - VARSTR_HEADER_SIZE;
  char    tmp[GRANTS_COL_MAX_LEN];
  char   *pBuf = &tmp[0];
  char   *qBuf = NULL;
  char    ts[GRANT_TS_SEC_LEN] = {0};

#ifdef TD_INDUSTRY
  if (optional && (expire == GRANT_UNIQ_UNDEFINED)) {
    TAOS_RETURN(0);
  }
#endif

  SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
  qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
  (void)snprintf(qBuf, colLen, "%s", name);
  varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
  TAOS_CHECK_RETURN(colDataSetVal(pColInfo, *numOfRows, pBuf, false));

  ++cols;
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
  qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
  (void)snprintf(qBuf, colLen, "%s", display);
  varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
  TAOS_CHECK_RETURN(colDataSetVal(pColInfo, *numOfRows, pBuf, false));

  ++cols;
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
  qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
  if (expire == GRANT_UNIQ_UNLIMITED) {
    (void)snprintf(qBuf, colLen, GRANT_UNIQ_UNLIMITED_S);
  } else {
    TAOS_UNUSED(grantSecondsToString(expire, ts));
    (void)snprintf(qBuf, colLen, "%s", ts);
  }
  varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
  TAOS_CHECK_RETURN(colDataSetVal(pColInfo, *numOfRows, pBuf, false));

  ++cols;
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
  qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
  if (isDataIn) {
    if (expire != GRANT_UNIQ_UNLIMITED) TAOS_UNUSED(grantSecondsToString(expire, ts));
    (void)snprintf(qBuf, colLen,
                   "{\"number\":%" PRIi64 ", \"speed\":%" PRIi64 ", \"expire\":\"%" PRIi64 "\", \"expireTime\":\"%s\"}",
                   curVal, limit, expire, expire != GRANT_UNIQ_UNLIMITED ? ts : GRANT_UNIQ_UNLIMITED_S);
  } else if (limit == GRANT_UNIQ_UNLIMITED) {
    (void)snprintf(qBuf, colLen, "%" PRIi64 "/%s", curVal, GRANT_UNIQ_UNLIMITED_S);
  } else if (limit != GRANT_UNIQ_UNUTILIZED) {
    (void)snprintf(qBuf, colLen, "%" PRIi64 "/%" PRIi64, curVal, limit);
  } else {
    qBuf[0] = 0;
  }
  varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
  TAOS_CHECK_RETURN(colDataSetVal(pColInfo, *numOfRows, pBuf, false));

  ++(*numOfRows);
  TAOS_RETURN(0);
}

static int32_t mndRetrieveGrantFull(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = 0;
  int32_t       lino = 0;
  int32_t       numOfRows = 0;
  SGrantStatus *pStatus = &gStatus;

  if (pShow->numOfRows < 1) {
    // sevice
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_SERVICE],
                                             gGrantDisplay[GRANT_OPT_SERVICE], pStatus->serviceExpireSec, 0,
                                             GRANT_UNIQ_UNUTILIZED, false, false));
    // with expire and limits
    int64_t basicExpireSec =
        pStatus->grantState == GRANT_STATE_REVOKED ? pStatus->revokedExpireSec : pStatus->basicExpireSec;
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, "timeseries", "Timeseries", basicExpireSec,
                                             pStatus->curTimeSeries, pStatus->limitTimeSeries, false, false));
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, "dnodes", "Dnodes", basicExpireSec, pStatus->curDnodes,
                                             pStatus->limitDnodes, false, false));
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, "cpu_cores", "CPU Cores", basicExpireSec,
                                             pStatus->curCpuCores, pStatus->limitCpuCores, false, false));

    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_STREAM],
                                             gGrantDisplay[GRANT_OPT_STREAM], pStatus->streamExpireSec,
                                             pStatus->curStreams, pStatus->limitStreams, false, true));
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_SUBSCRIPTION],
                                             gGrantDisplay[GRANT_OPT_SUBSCRIPTION], pStatus->subscriptionExpireSec,
                                             pStatus->curSubscriptions, pStatus->limitSubscriptions, false, true));
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_VIEW],
                                             gGrantDisplay[GRANT_OPT_VIEW], pStatus->viewExpireSec, pStatus->curViews,
                                             pStatus->limitViews, false, true));
    // with expire and no limits
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_AUDIT],
                                             gGrantDisplay[GRANT_OPT_AUDIT], pStatus->auditExpireSec, 0,
                                             GRANT_UNIQ_UNUTILIZED, false, true));
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_STORAGE],
                                             gGrantDisplay[GRANT_OPT_STORAGE], pStatus->multiTierExpireSec, 0,
                                             GRANT_UNIQ_UNUTILIZED, false, true));
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_DATA_SYNC],
                                             gGrantDisplay[GRANT_OPT_DATA_SYNC], pStatus->dataSyncExpireSec, 0,
                                             GRANT_UNIQ_UNUTILIZED, false, true));
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_DATA_BAK_RST],
                                             gGrantDisplay[GRANT_OPT_DATA_BAK_RST], pStatus->bakRstExpireSec, 0,
                                             GRANT_UNIQ_UNUTILIZED, false, true));
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_OBJECT_STORAGE],
                                             gGrantDisplay[GRANT_OPT_OBJECT_STORAGE], pStatus->objectStorageExpireSec,
                                             0, GRANT_UNIQ_UNUTILIZED, false, true));
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_ACTIVE_ACTIVE],
                                             gGrantDisplay[GRANT_OPT_ACTIVE_ACTIVE], pStatus->activeActiveExpireSec, 0,
                                             GRANT_UNIQ_UNUTILIZED, false, true));
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_DUAL_REPLICA_HA],
                                             gGrantDisplay[GRANT_OPT_DUAL_REPLICA_HA], pStatus->dualReplicaHAExpireSec,
                                             0, GRANT_UNIQ_UNUTILIZED, false, true));
    TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(pBlock, &numOfRows, gGrantName[GRANT_OPT_DB_ENCRYPTION],
                                             gGrantDisplay[GRANT_OPT_DB_ENCRYPTION], pStatus->dbEncryptionExpireSec, 0,
                                             GRANT_UNIQ_UNUTILIZED, false, true));

    taosRLockLatch(&grantHandle.rwLock);

    // dynamic grantItem64
    int32_t nDynamic = taosArrayGetSize(pStatus->pItemN64);
    for (int32_t i = 0; i < nDynamic; ++i) {
      SGrantItem64 *pItem = TARRAY_GET_ELEM(pStatus->pItemN64, i);
      TAOS_CHECK_EXIT(mndRetrieveGrantFullItem(
          pBlock, &numOfRows, pItem->name, tGetGrantDisplay(pItem->name),
          pItem->expire == GRANT_UNIQ_UNLIMITED ? pItem->expire : (int64_t)pItem->expire * 86400, 0, pItem->number,
          false, true));
    }
    // known dataIns
    for (int32_t i = 0; i < CONN_TYPE_DYN_MAX; ++i) {
      code = mndRetrieveGrantFullItem(pBlock, &numOfRows, gConnName[i], gConnDisplay[i], pStatus->dataIns[i].expireSec,
                                      pStatus->dataIns[i].number, pStatus->dataIns[i].speed, true, true);
      if (code < 0) {
        taosRUnLockLatch(&grantHandle.rwLock);
        TAOS_CHECK_EXIT(code);
      }
    }
    // dynamic dataIns
    nDynamic = taosArrayGetSize(pStatus->pDataIns);
    for (int32_t i = 0; i < nDynamic; ++i) {
      SGrantDataIns *pDataIn = TARRAY_GET_ELEM(pStatus->pDataIns, i);
      code = mndRetrieveGrantFullItem(
          pBlock, &numOfRows, pDataIn->name, tGetConnDisplay(pDataIn->name),
          pDataIn->expire == GRANT_UNIQ_UNLIMITED ? pDataIn->expire : (int64_t)pDataIn->expire * 86400, pDataIn->number,
          pDataIn->speed, true, true);
      if (code < 0) {
        taosRUnLockLatch(&grantHandle.rwLock);
        TAOS_CHECK_EXIT(code);
      }
    }

    taosRUnLockLatch(&grantHandle.rwLock);
  }

  pShow->numOfRows += numOfRows;

_exit:
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextGrantFull(SMnode *pMnode, void *pIter) {}

static int32_t mndRetrieveGrantLogs(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  SSdb   *pSdb = pMnode->pSdb;
  int32_t code = 0;
  int32_t lino = 0;
  int32_t numOfRows = 0;
  int32_t cols = 0;
  char   *pBuf = NULL;
  char   *qBuf = NULL;
  char    ts[GRANT_TS_SEC_LEN];
  int32_t tmpLen = 0;
  int32_t bufLen = 0;
  int32_t nMachines = 0;
  void   *pIter = NULL;

  SGrantLogObj *pGrant = mndAcquireGrant(pMnode, &pIter);
  if (!pGrant) {
    TAOS_RETURN(0);  // no grant logs, normal case, don't
  }

  nMachines = taosArrayGetSize(pGrant->pMachines);
  bufLen = nMachines * 52;  // max len of machine(19+1+4+1+24+2+1 = 52)
  if (bufLen < 1470) {
    bufLen = 1470;  // max len of state: (19+1+8+1+9+1+9+1) 49*30=1470
  } else if (bufLen > TSDB_GRANT_LOG_COL_LEN) {
    code = TSDB_CODE_APP_ERROR;
    uError("machine col len of grant logs overflow(%d > %d) since %s", bufLen, TSDB_GRANT_LOG_COL_LEN, tstrerror(code));
    TAOS_CHECK_EXIT(code);
  }

  if (!(pBuf = taosMemoryCalloc(1, bufLen + VARSTR_HEADER_SIZE))) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (pShow->numOfRows < 1) {
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
    for (int32_t i = 0; i < pGrant->nStates; ++i) {
      SGrantState *pState = &pGrant->states[i];
      TAOS_UNUSED(grantSecondsToString(pState->ts, ts));
      if (i == 0) {
        tmpLen = tsnprintf(qBuf, bufLen - POINTER_DISTANCE(qBuf, pBuf), "%s,%s,%s,%s", ts, gGrantReason[pState->reason],
                           gGrantState[pState->lastState], gGrantState[pState->state]);
      } else {
        tmpLen = tsnprintf(qBuf, bufLen - POINTER_DISTANCE(qBuf, pBuf), ";%s,%s,%s,%s", ts,
                           gGrantReason[pState->reason], gGrantState[pState->lastState], gGrantState[pState->state]);
      }
      qBuf += tmpLen;
    }
    qBuf[0] = 0;
    varDataSetLen(pBuf, POINTER_DISTANCE(qBuf, pBuf) - VARSTR_HEADER_SIZE);
    COL_DATA_SET_VAL_GOTO(pBuf, false, NULL, _exit);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
    for (int32_t i = 0; i < pGrant->nActives; ++i) {
      SGrantActive *pActive = &pGrant->actives[i];
      TAOS_UNUSED(grantSecondsToString(pActive->ts, ts));
      if (i == 0) {
        tmpLen = tsnprintf(qBuf, bufLen - POINTER_DISTANCE(qBuf, pBuf), "%s,%s", ts, pActive->active);
      } else {
        tmpLen = tsnprintf(qBuf, bufLen - POINTER_DISTANCE(qBuf, pBuf), ";%s,%s", ts, pActive->active);
      }
      qBuf += tmpLen;
    }
    qBuf[0] = 0;
    varDataSetLen(pBuf, POINTER_DISTANCE(qBuf, pBuf) - VARSTR_HEADER_SIZE);
    COL_DATA_SET_VAL_GOTO(pBuf, false, NULL, _exit);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
    for (int32_t i = 0; i < nMachines; ++i) {
      SGrantMachine *pMachine = TARRAY_GET_ELEM(pGrant->pMachines, i);
      TAOS_UNUSED(grantSecondsToString(pMachine->ts, ts));
      if (i == 0) {
        tmpLen = tsnprintf(qBuf, bufLen - POINTER_DISTANCE(qBuf, pBuf), "%s,%d,%s,%" PRIu8, ts, pMachine->id,
                           pMachine->machine, grantGetMachineFlag(pMachine->machine));
      } else {
        tmpLen = tsnprintf(qBuf, bufLen - POINTER_DISTANCE(qBuf, pBuf), ";%s,%d,%s,%" PRIu8, ts, pMachine->id,
                           pMachine->machine, grantGetMachineFlag(pMachine->machine));
      }
      qBuf += tmpLen;
    }
    qBuf[0] = 0;
    varDataSetLen(pBuf, POINTER_DISTANCE(qBuf, pBuf) - VARSTR_HEADER_SIZE);
    COL_DATA_SET_VAL_GOTO(pBuf, false, NULL, _exit);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (pColInfo) {
      qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
      if (gStatus.grantState == GRANT_STATE_INIT || gStatus.grantState == GRANT_STATE_UNGRANTED) {
        tmpLen = 0;
      } else {
        tmpLen = tsnprintf(qBuf, bufLen,
                           "checkUpTime:%" PRIi8 ",checkMachineCode:%" PRIi8 ",checkHistoricalActive:%" PRIi8
                           ",skipOldActiveIfParseFail:%" PRIi8,
                           gStatus.checkUpTime ? 1 : 0, gStatus.checkMachineCode ? 1 : 0,
                           gStatus.checkHistoricalActive ? 1 : 0, gStatus.skipOldActiveIfParseFail ? 1 : 0);
      }
      qBuf += tmpLen;
      qBuf[0] = 0;
      varDataSetLen(pBuf, POINTER_DISTANCE(qBuf, pBuf) - VARSTR_HEADER_SIZE);
      COL_DATA_SET_VAL_GOTO(pBuf, false, NULL, _exit);
    }

    ++numOfRows;
  }

  pShow->numOfRows += numOfRows;

_exit:
  mndReleaseGrant(pMnode, pGrant, pIter);
  taosMemoryFree(pBuf);
  if (code < 0) {
    mError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextGrantLogs(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_GRANT);
}
static int32_t mndRetrieveMachines(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  int32_t code = 0;
  int32_t lino = 0;
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
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (grantObj.clusterId[0] == 0) {
    grantSetClusterId(pMnode, grantObj.clusterId);
    if (grantObj.clusterId[0] == 0) {
      TAOS_CHECK_EXIT(TSDB_CODE_APP_IS_STARTING);
    }
  }

  if (pShow->numOfRows < 1) {
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
    (void)snprintf(qBuf, TSDB_CLUSTER_ID_LEN + 1, "%s", grantObj.clusterId);
    varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
    COL_DATA_SET_VAL_GOTO(pBuf, false, NULL, _exit);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);

    SDnodeObj *pDnode = NULL;
    int32_t    index = 0;
    while ((pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode))) {
      if (pDnode->machineId[0] == 0) continue;
      if (index == 0) {
        (void)snprintf(qBuf, TSDB_MACHINE_ID_LEN + 1, "%s", pDnode->machineId);
        qBuf += TSDB_MACHINE_ID_LEN;
      } else {
        (void)snprintf(qBuf, TSDB_MACHINE_ID_LEN + 2, ",%s", pDnode->machineId);
        qBuf += (TSDB_MACHINE_ID_LEN + 1);
      }
      ++index;
      sdbRelease(pSdb, pDnode);
    }
    COL_DATA_SET_VAL_GOTO((const char *)&index, false, NULL, _exit);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
    COL_DATA_SET_VAL_GOTO(pBuf, false, NULL, _exit);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (pColInfo) { // for compatibility of old version
      qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
      snprintf(qBuf, TSDB_VERSION_LEN, "%s", version);
      varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
      COL_DATA_SET_VAL_GOTO(pBuf, false, NULL, _exit);
    }

    ++numOfRows;
  }

  pShow->numOfRows += numOfRows;

_exit:
  taosMemoryFree(pBuf);
  if (code < 0) {
    mError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextMachines(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_DNODE);
}

static int32_t tDeserializeGrantNotify(void *buf, int32_t bufLen, GrantNotify *pNotify) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeU64(&decoder, &pNotify->curTimeSeries));
_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    uError("failed to deserialize grant notify at line %d since %s", lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int32_t tSerializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, int64_t clusterTime,
                                     uint32_t *pLen) {
  int32_t  code = 0;
  int32_t  lino = 0;
  uint32_t tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  // grant status
  // since 3.2.3.0
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p1));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p2));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p3));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p4));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p5));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p6));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p7));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p8));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p9));

  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->limitTimeSeries));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->curTimeSeries));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pStatus->limitCpuCores));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pStatus->curCpuCores));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pStatus->limitViews));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pStatus->curViews));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->revokedExpireSec));

  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, clusterTime));

  TAOS_CHECK_EXIT(tSerializeGrantDataIns(&encoder, pStatus->dataIns));
  TAOS_CHECK_EXIT(tSerializeGrantDynDataIns(&encoder, pStatus->pDataIns));

  // since 3.3.0.0
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p10));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p11));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p12));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p13));
  // since 3.3.2.9
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pStatus->p14));

  // for future grantItems

  tEndEncode(&encoder);

  tlen = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  if (pLen) *pLen = tlen;
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

int32_t tDeserializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, int64_t *clusterTime) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  // grant status
  // since 3.2.3.0
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p1));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p2));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p3));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p4));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p5));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p6));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p7));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p8));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p9));

  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->limitTimeSeries));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->curTimeSeries));
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pStatus->limitCpuCores));
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pStatus->curCpuCores));
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pStatus->limitViews));
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pStatus->curViews));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->revokedExpireSec));

  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, clusterTime));
  TAOS_CHECK_EXIT(tDeserializeGrantDataIns(&decoder, pStatus->dataIns));
  TAOS_CHECK_EXIT(tDeserializeGrantDynDataIns(&decoder, pStatus->pDataIns));

  // since 3.3.0.0
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p10));
    TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p11));
    TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p12));
    TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p13));
  }
  // since 3.3.2.9
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pStatus->p14));
  }

  // for future grantItems
  // ...
  // if(!tDecodeIsEnd(&decoder) ...

_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  TAOS_RETURN(code);
}

static int32_t tSerializeGrantDataIns(SEncoder *encoder, SGrantDataIn *dataIn) {
  TAOS_CHECK_RETURN(tEncodeI16v(encoder, CONN_TYPE_DYN_MAX));
  for (int32_t i = 0; i < CONN_TYPE_DYN_MAX; ++i) {
    TAOS_CHECK_RETURN(tEncodeI32v(encoder, dataIn[i].number));
    TAOS_CHECK_RETURN(tEncodeI32v(encoder, dataIn[i].speed));
    TAOS_CHECK_RETURN(tEncodeI64v(encoder, dataIn[i].expireSec));
  }
  TAOS_RETURN(0);
}

static int32_t tDeserializeGrantDataIns(SDecoder *decoder, SGrantDataIn *dataIn) {
  int16_t nIns = 0;
  TAOS_CHECK_RETURN(tDecodeI16v(decoder, &nIns));
  for (int32_t i = 0; i < nIns; ++i) {
    if (i >= CONN_TYPE_DYN_MAX) {
      TAOS_CHECK_RETURN(tDecodeI32v(decoder, NULL));
      TAOS_CHECK_RETURN(tDecodeI32v(decoder, NULL));
      TAOS_CHECK_RETURN(tDecodeI64v(decoder, NULL));
    } else {
      TAOS_CHECK_RETURN(tDecodeI32v(decoder, &dataIn[i].number));
      TAOS_CHECK_RETURN(tDecodeI32v(decoder, &dataIn[i].speed));
      TAOS_CHECK_RETURN(tDecodeI64v(decoder, &dataIn[i].expireSec));
    }
  }
  TAOS_RETURN(0);
}

static int32_t tSerializeGrantDynDataIns(SEncoder *encoder, SArray *pIns) {
  int16_t nDataIns = taosArrayGetSize(pIns);
  TAOS_CHECK_RETURN(tEncodeI16v(encoder, nDataIns));
  for (int32_t i = 0; i < nDataIns; ++i) {
    SGrantDataIns *pIn = TARRAY_GET_ELEM(pIns, i);
    TAOS_CHECK_RETURN(tEncodeCStr(encoder, pIn->name));
    TAOS_CHECK_RETURN(tEncodeI32v(encoder, pIn->number));
    TAOS_CHECK_RETURN(tEncodeI32v(encoder, pIn->speed));
    TAOS_CHECK_RETURN(tEncodeI32v(encoder, pIn->expire));
  }
  TAOS_RETURN(0);
}

static int32_t tDeserializeGrantDynDataIns(SDecoder *decoder, SArray *pIns) {
  int16_t nIns = 0;
  TAOS_CHECK_RETURN(tDecodeI16v(decoder, &nIns));
  if (nIns <= 0) TAOS_RETURN(0);
  if (!pIns && !(pIns = taosArrayInit_s(sizeof(SGrantDataIns), nIns))) {
    TAOS_RETURN(TSDB_CODE_OUT_OF_MEMORY);
  }

  for (int32_t i = 0; i < nIns; ++i) {
    SGrantDataIns *pIn = TARRAY_GET_ELEM(pIns, i);
    TAOS_CHECK_RETURN(tDecodeCStrTo(decoder, &pIn->name[0]));
    TAOS_CHECK_RETURN(tDecodeI32v(decoder, &pIn->number));
    TAOS_CHECK_RETURN(tDecodeI32v(decoder, &pIn->speed));
    TAOS_CHECK_RETURN(tDecodeI32v(decoder, &pIn->expire));
  }
  TAOS_RETURN(0);
}

static const char *getEncryptKeyStatStr(int8_t encryptKeyStat) {
  switch (encryptKeyStat) {
    case ENCRYPT_KEY_STAT_UNKNOWN:
      return "unknown";
    case ENCRYPT_KEY_STAT_UNSET:
      return "unset";
    case ENCRYPT_KEY_STAT_SET:
      return "set";
    case ENCRYPT_KEY_STAT_LOADED:
      return "loaded";
    default:
      break;
  }
  return "unknown";
}

static int32_t mndRetrieveEncryptions(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode    *pMnode = pReq->info.node;
  SSdb      *pSdb = pMnode->pSdb;
  int32_t    code = 0;
  int32_t    lino = 0;
  int32_t    numOfRows = 0;
  int32_t    cols = 0;
  bool       online = true;
  ESdbStatus objStatus = 0;
  SDnodeObj *pDnode = NULL;
  int64_t    curMs = taosGetTimestampMs();
  char       buf[16];

  while (numOfRows < rows) {
    pShow->pIter = sdbFetchAll(pSdb, SDB_DNODE, pShow->pIter, (void **)&pDnode, &objStatus, true);
    if (pShow->pIter == NULL) break;

    online = mndIsDnodeOnline(pDnode, curMs);
    cols = 0;

    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pDnode->id, false, pDnode, _exit);
    ++cols;

    const char *keyStr = getEncryptKeyStatStr(online ? pDnode->encryptionKeyStat : ENCRYPT_KEY_STAT_UNKNOWN);
    STR_TO_VARSTR(buf, keyStr)
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO(buf, false, pDnode, _exit);

    ++numOfRows;
    sdbRelease(pSdb, pDnode);
  }

  pShow->numOfRows += numOfRows;
_exit:
  if (code < 0) {
    mError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelGetNextEncryptions(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_DNODE);
}