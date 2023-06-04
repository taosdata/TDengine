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

#define GRANT_VERSION (grantStatus.officialVersion ? "official" : "trial")
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
  uint64_t      limitTimeSeries;
  uint32_t      limitDbs;
  uint32_t      limitSTables;
  uint32_t      limitTables;
  SGrantConnMsg connectors;  // version 2 since 3.0.5.0
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

SCloudGrantStatus cloudGrantStatus = {0,
                                      GRANT_TIME_SERIES_LIMITS,
                                      0,
                                      0,
                                      0,
                                      GRANT_DATABASE_LIMITS,
                                      0,
                                      GRANT_STABLE_LIMITS,
                                      0,
                                      GRANT_TABLE_LIMITS,
                                      .connectors.majorVer = GRANT_CONN_MAJOR_VER,
                                      .connectors.minorVer = GRANT_CONN_MINOR_VER,
                                      .connectors.officialVersion = 0};

GRANT_CFG_EXTERN;
typedef SCloudGrantStatus GrantStatus;
typedef SCloudGrantMsg    GrantMsg;
#else
typedef SGrantStatus GrantStatus;
typedef SGrantMsg    GrantMsg;
#endif

extern SGrantObj grantObj;
extern char      tsVersionName[16];
extern int64_t   tsExpireTime;

// for compatibility: grantMain.c could work with machine.o before 3.0.5.0
SGrantConnObj grantConnObj = {.machine = grantObj.machine, .clusterId = grantObj.clusterId};

static char    *grantSecondsToString(uint32_t seconds);
static void     dmRefreshGrantCfg();
static void     grantRetrieveGrantInfo(SMnode *pMnode);
static void     grantResetMaster(SMnode *pMnode);
static void     grantConnResetMaster(SMnode *pMnode);
static void     grantSetClusterInfo(SMnode *pMnode);
static int32_t  mndProcessGrantHB(SRpcMsg *pReq);
static int32_t  dmGenerateGrantMsg(GrantMsg *pGrant, GrantStatus *pGrantStatus, SDnodeInfo *pInfo);
static int32_t  mndProcessDnodeSGrantMsg(SMnode *pMnode, SDnodeInfo *pDnodeInfo, GrantMsg *pGrantMsg,
                                         GrantStatus *pGrantStatus);
static int32_t  tSerializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, SDnodeInfo *pInfo);
static int32_t  tDeserializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, SDnodeInfo *pInfo);
static int32_t  tSerializeGrantMsg(void *buf, int32_t bufLen, GrantMsg *pMsg);
static int32_t  tDeserializeGrantMsg(void *buf, int32_t bufLen, GrantMsg *pMsg);
static uint64_t grantGetClusterCurTimeSeries(SMnode *pMnode);

static int32_t mndRetrieveGrant(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextGrant(SMnode *pMnode, void *pIter);

// connectors
static void    tGrantConnItemsInit(SGrantConnItem *pItems, int32_t nItem);
static int32_t tGrantConnItemsNum(int8_t version);
static int32_t tSerializeGrantConnMsg(SEncoder *encoder, SGrantConnMsg *pMsg);
static int32_t tDeserializeGrantConnMsg(SDecoder *decoder, SGrantConnMsg *pMsg);

typedef struct {
  uint32_t *lastCheck;
  SHashObj *pOfficials;
} SGrantHandle;

static bool  recheckClusterTime = true;
static void *grantCheckTimer = NULL;
static void *grantSendTimer = NULL;
int32_t      grantFlag = 0;
SGrantHandle grantHandle = {0};
SGrantStatus grantStatus = {false,
                            false,
                            false,
                            0,
                            GRANT_EXPIRE_TIME,
                            0,
                            (int64_t)(GRANT_STORAGE_LIMITS)*1073741824L,
                            0,
                            GRANT_TIME_SERIES_LIMITS,
                            GRANT_EXPIRE_TIME,
                            0,
                            GRANT_WRITING_SPEED_LIMITS,
                            0,
                            GRANT_QUERY_TIME_LIMITS,
                            0,
                            GRANT_DATABASE_LIMITS,
                            0,
                            GRANT_USER_LIMITS,
                            GRANT_CONNECTION_LIMITS,
                            GRANT_STREAM_LIMITS,
                            0,
                            GRANT_ACCT_LIMITS,
                            0,
                            GRANT_DNODE_LIMITS,
                            GRANT_CPU_LIMITS,
                            0,
                            .connectors.majorVer = GRANT_CONN_MAJOR_VER,
                            .connectors.minorVer = GRANT_CONN_MINOR_VER,
                            .connectors.officialVersion = 0};

// extern SSysTableMeta infosMeta[];
#ifdef GRANTS_CFG
#define gStatus cloudGrantStatus
#else
#define gStatus grantStatus
#endif

int32_t mndInitGrant(SMnode *pMnode) {
  terrno = 0;
  tsGrantHBInterval = 5;
#ifdef GRANTS_CFG
  grantFlag |= (int32_t)GRANT_EDITION_CLOUD;
#endif
  gStatus.lastCheck = (uint32_t)(taosGetTimestampMs() / 1000);
  grantHandle.lastCheck = &gStatus.lastCheck;

  mndSetMsgHandle(pMnode, TDMT_MND_GRANT_HB_TIMER, mndProcessGrantHB);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndRetrieveGrant);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndCancelGetNextGrant);
  grantSetClusterInfo(pMnode);
  if (!(grantHandle.pOfficials = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UINT), true, true))) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

_exit:
  if (terrno != 0) {
    uError("grant data initialize failed since %s", tstrerror(terrno));
  } else {
    uDebug("grant data is initialized");
  }

  return terrno;
}

void mndCleanupGrant() {
  taosTmrStopA(&grantCheckTimer);
  taosTmrStopA(&grantSendTimer);
  taosHashCleanup(grantHandle.pOfficials);
}

static void grantSetClusterInfo(SMnode *pMnode) {
  if (strncmp(tsVersionName, GRANT_VERSION, 16) != 0) {
    strncpy(tsVersionName, GRANT_VERSION, 16);
  }
  COMPARE_SET_VAL(tsExpireTime, (int64_t)grantStatus.expireTimeSec * 1000, !=);
  COMPARE_SET_VAL(pMnode->grant.expireTimeMS, tsExpireTime, !=);
  COMPARE_SET_VAL(pMnode->grant.timeseriesAllowed, (int64_t)grantStatus.limitTimeSeries, !=);
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

static void grantSetActiveCodes(SDnodeInfo *pInfo) {
  if (0 != pInfo->active[0] && 0 != strncmp(grantObj.active, pInfo->active, GRANT_ACTIVE_KEY_LEN + 1)) {
    tstrncpy(grantObj.active, pInfo->active, GRANT_ACTIVE_KEY_LEN + 1);
  }
  if (0 != pInfo->connActive[0] &&
      0 != strncmp(grantConnObj.active, pInfo->connActive, GRANT_CONN_ACTIVE_KEY_LEN + 1)) {
    tstrncpy(grantConnObj.active, pInfo->connActive, GRANT_CONN_ACTIVE_KEY_LEN + 1);
  }
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
  if (tDeserializeGrantStatus(pMsg->pCont, pMsg->contLen, &grantStatusReq, &dnodeInfo) != 0) {
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
  grantStatus = grantStatusReq;  // assign directly
#endif

  // step 3: respond with grant msg
  grantSetClusterIdEx(*(int64_t *)pInfo);
  GrantMsg grantMsg = {.connectors.majorVer = GRANT_CONN_MAJOR_VER, .connectors.minorVer = GRANT_CONN_MINOR_VER};
  dmGenerateGrantMsg(&grantMsg, &grantStatusReq, &dnodeInfo);
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

static void dmRefreshGrantCfg() {
  char cfgFile[PATH_MAX] = {0};
#ifdef CUS_PROMPT
  sprintf(cfgFile, "%s/%s.cfg", configDir, CUS_PROMPT);
#else
  sprintf(cfgFile, "%s/taos.cfg", configDir);
#endif
  grantActiveSystem(cfgFile);
}

static int32_t dmGenerateGrantMsg(GrantMsg *pGrantMsg, GrantStatus *pGrantStatus, SDnodeInfo *pInfo) {
  grantSetActiveCodes(pInfo);
  // refresh
  dmRefreshGrantCfg();
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
    memcpy(pConn->items, grantConnObj.items, sizeof(SGrantConnItem) * CONN_TYPE_MAX);
  }

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief 1) send grant status to dnode
 *        2) process response (grant msg) from dnode
 * @param pMnode
 * @param pDnodeInfo
 * @return int32_t
 */
static int32_t mndSendGrantStatusToDnode(SMnode *pMnode, SDnodeInfo *pDnodeInfo) {
  // step 1: send grant status to dnode
  int32_t contLen = tSerializeGrantStatus(NULL, 0, &gStatus, pDnodeInfo);
  void   *pCont = rpcMallocCont(contLen);
  if (!pCont) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uWarn("failed to generate grant status msg since %s", terrstr());
    return TSDB_CODE_FAILED;
  }

  tSerializeGrantStatus(pCont, contLen, &gStatus, pDnodeInfo);

  SRpcMsg rpcMsg = {.pCont = pCont, .contLen = contLen, .msgType = TDMT_MND_GRANT};
  SRpcMsg rpcRsp = {0};

  uDebug("send grant status msg to dnode:%d %s:%" PRIu16, pDnodeInfo->id, pDnodeInfo->ep.fqdn, pDnodeInfo->ep.port);

  SEpSet epSet = {.numOfEps = 1};
  strncpy(epSet.eps[0].fqdn, pDnodeInfo->ep.fqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = pDnodeInfo->ep.port;

  // TODO: use async mode instead of sync mode
  rpcSendRecv(pMnode->msgCb.clientRpc, &epSet, &rpcMsg, &rpcRsp);

  // step 2: process response from dnode
  if (!rpcRsp.pCont || rpcRsp.contLen <= 0 || rpcRsp.code != 0) {
    uError("failed to process the grant rsp from dnode:%d %s:%" PRIu16 " since empty content: %" PRIi32, pDnodeInfo->id,
           pDnodeInfo->ep.fqdn, pDnodeInfo->ep.port, rpcRsp.code);
    goto _err;
  }

  GrantMsg grantMsgRsp = {0};
  if (tDeserializeGrantMsg(rpcRsp.pCont, rpcRsp.contLen, &grantMsgRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    uWarn("failed to process the grant rsp from dnode:%d %s:%" PRIu16 " since %s", pDnodeInfo->id, pDnodeInfo->ep.fqdn,
          pDnodeInfo->ep.port, terrstr());
    goto _err;
  }

  uDebug("succeed to receive grant msg from dnode:%d %s:%" PRIu16, pDnodeInfo->id, pDnodeInfo->ep.fqdn,
         pDnodeInfo->ep.port);
  mndProcessDnodeSGrantMsg(pMnode, pDnodeInfo, &grantMsgRsp, &gStatus);

  rpcFreeCont(rpcRsp.pCont);
  return TSDB_CODE_SUCCESS;
_err:
  rpcFreeCont(rpcRsp.pCont);
  return TSDB_CODE_FAILED;
}

/**
 * @brief process grant heartbeat msg from mnode
 *
 * @param pReq
 * @return int32_t
 */
static int32_t mndProcessGrantHB(SRpcMsg *pReq) {
  if (tsGrantHBInterval != GRANT_HEART_BEAT_MSG) tsGrantHBInterval = GRANT_HEART_BEAT_MSG;
  SMnode *pMnode = pReq->info.node;
  int32_t dnodeSize = mndGetDnodeSize(pMnode);

  SArray *pDnodeInfo = taosArrayInit(dnodeSize, sizeof(SDnodeInfo));
  if (!pDnodeInfo) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uWarn("failed to process grant hb msg since %s", terrstr());
    return -1;
  }

  if (recheckClusterTime) {
    grantResetMaster(pMnode);
    grantConnResetMaster(pMnode);
  }
  grantRetrieveGrantInfo(pMnode);

  grantSetClusterInfo(pMnode);

  mndGetDnodeData(pMnode, pDnodeInfo);

  for (int32_t i = 0; i < taosArrayGetSize(pDnodeInfo); ++i) {
    SDnodeInfo *info = (SDnodeInfo *)taosArrayGet(pDnodeInfo, i);
    mndSendGrantStatusToDnode(pMnode, info);
  }

  taosArrayDestroy(pDnodeInfo);

  if (grantCheck(TSDB_GRANT_TIME) == TSDB_CODE_SUCCESS) {
    atomic_store_8(&tsExpired, 0);
  } else {
    atomic_store_8(&tsExpired, 1);
  }

  return 0;
}

void grantParseParameter() {
  char *key = grantGetMachineSerials();
  if (key != NULL) {
    fprintf(stdout, "machine code: %s \n", key);
  } else {
    fprintf(stderr, "should generate machine code under root authority!\n");
  }
  exit(EXIT_SUCCESS);
}

static char *grantSecondsToString(uint32_t seconds) {
  char     *ts = taosMemoryCalloc(64, 1);
  time_t    sec = seconds;
  struct tm ptm;
  if (taosLocalTime(&sec, &ptm, ts) != NULL) {
    strftime(ts, 64, "%Y-%m-%d %H:%M:%S", &ptm);
  }
  return ts;
}

static uint32_t grantGetClusterCreateTime(SMnode *pMnode) {
  int64_t createTime = (int64_t)taosGetTimestampMs();
  int64_t clusterTime = mndGetClusterCreateTime(pMnode);

  if (clusterTime < createTime) {
    createTime = clusterTime;
  }

  return (uint32_t)(createTime / 1000);
}

static uint32_t grantGetClusterCurSpeed() { return 0; }

/**
 * @brief  numOfColumns: stable + ctable + ntable in all master vnodes, not including Primary TS Key column, not
 * including tsma dstVg
 *
 * @return uint64_t
 */
static uint64_t grantGetClusterCurTimeSeries(SMnode *pMnode) {
  uint64_t numOfPoints = 0;
  SSdb    *pSdb = pMnode->pSdb;
  SVgObj  *pVgroup = NULL;
  void    *pIter = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

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

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

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

  while (1) {
    pIter = sdbFetch(pSdb, SDB_USER, pIter, (void **)&pUser);
    if (pUser == NULL) break;
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
  while (1) {
    pIter = sdbFetch(pSdb, SDB_ACCT, pIter, (void **)&pAcct);
    if (pAcct == NULL) break;
    if (strcmp(pAcct->acct, "root") != 0) {
      ++numOfAccts;
    }
    sdbRelease(pSdb, pAcct);
  }

  return numOfAccts;
}

static uint32_t grantGetClusterCurDnodes(SMnode *pMnode) { return (uint32_t)mndGetDnodeSize(pMnode); }

static uint32_t grantGetClusterCurSTables(SMnode *pMnode) {
  SSdb *pSdb = pMnode->pSdb;
  return (uint32_t)sdbGetSize(pSdb, SDB_STB);
}

static uint32_t grantGetClusterCurTables(SMnode *pMnode) {
  uint64_t numOfPoints = 0;
  SSdb    *pSdb = pMnode->pSdb;
  SVgObj  *pVgroup = NULL;
  void    *pIter = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;

    if (!pVgroup->isTsma) {
      numOfPoints += pVgroup->numOfTables;
    }

    sdbRelease(pSdb, pVgroup);
  }

  return numOfPoints;
}

static uint32_t grantGetClusterCurCores(SMnode *pMnode) {
  SSdb      *pSdb = pMnode->pSdb;
  SDnodeObj *pDnode = NULL;
  void      *pIter = NULL;
  uint32_t   numOfCores = 0;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;

    numOfCores += (uint32_t)pDnode->numOfCores;

    sdbRelease(pSdb, pDnode);
  }

  return numOfCores;
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
  grantStatus.curStorage = grantGetClusterCurStorage(pMnode);
  grantStatus.curSpeed = grantGetClusterCurSpeed();
  grantStatus.curTimeSeries = grantGetClusterCurTimeSeries(pMnode);
  grantStatus.curQueryTime = grantGetClusterCurQueryTime();
  grantStatus.curUsers = grantGetClusterCurUsers(pMnode);
  grantStatus.curAccts = grantGetClusterCurAccts(pMnode);
  grantStatus.curDnodes = grantGetClusterCurDnodes(pMnode);
  grantStatus.curDbs = grantGetClusterCurDbs(pMnode);
  grantStatus.curCpuCores = grantGetClusterCurCores(pMnode);
#endif
}

static void grantConnResetMaster(SMnode *pMnode) {
  uint32_t clusterCreateTime = grantGetClusterCreateTime(pMnode);
  if (clusterCreateTime > 0) {
    recheckClusterTime = false;
    SGrantConnItem item = {.number = GRANT_CONN_NUM_DEFAULT,
                           .speed = GRANT_CONN_SPEED_DEFAULT,
                           .expire = ceil((double)clusterCreateTime / 86400) + GRANT_CONN_EXPIRE_DEFAULT};
    for (int32_t i = 0; i < GRANT_CONN_NUM; ++i) {
      *(gStatus.connectors.items + i) = item;
    }
  }
}

/**
 * @brief init the grant status after mnode startup
 *
 * @param pMnode
 */
static void grantResetMaster(SMnode *pMnode) {
  grantRetrieveGrantInfo(pMnode);
#ifndef GRANTS_CFG
  uint32_t curTime = taosGetTimestampMs() / 1000;
  uint32_t clusterCreateTime = grantGetClusterCreateTime(pMnode);
  if (clusterCreateTime > 0) {
    recheckClusterTime = false;
    grantStatus.expireTimeSec = clusterCreateTime + GRANT_DEFAULT;
    grantStatus.expireTimeSec += GRANT_TOLERENCE;
    grantStatus.expired = false;

    char *ts = grantSecondsToString(grantStatus.expireTimeSec);
    uInfo("grant expire time reset to %s %u, current timeseries %" PRIu64, ts, grantStatus.expireTimeSec,
          grantStatus.curTimeSeries);
    taosMemoryFree(ts);
  }
#endif
}

void grantReset(SMnode *pMnode, EGrantType grant, uint64_t value) {
  switch (grant) {
    case TSDB_GRANT_ALL:
      grantResetMaster(pMnode);
      grantConnResetMaster(pMnode);
      break;
    case TSDB_GRANT_STORAGE:
      grantStatus.curStorage = value;
      break;
    default:
      break;
  }
}

static void grantAddTimeSeries(uint64_t timeSeriesNum) {
  atomic_add_fetch_64(&grantStatus.curTimeSeries, timeSeriesNum);
}

static void grantRestoreTimeSeries(uint64_t timeSeriesNum) {
  if (grantStatus.curTimeSeries < timeSeriesNum) {
    grantStatus.curTimeSeries = 0;
  } else {
    atomic_sub_fetch_64(&grantStatus.curTimeSeries, timeSeriesNum);
  }
}

void grantAdd(EGrantType grant, uint64_t value) {
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
}

void grantRestore(EGrantType grant, uint64_t value) {
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

static int32_t grantCheckExpired() {
  if (grantStatus.expired) {
    return TSDB_CODE_GRANT_EXPIRED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t grantCheckUsers() {
  if (grantStatus.limitUsers == GRANT_USER_LIMITS || grantStatus.curUsers < grantStatus.limitUsers) {
    return 0;
  } else {
    uError("grant failed to create user, exist:%" PRIu32 ", reason:grant user limited", grantStatus.curUsers);
    return TSDB_CODE_GRANT_USER_LIMITED;
  }
}

static int32_t grantCheckDatabases() {
  if (grantStatus.limitDbs == GRANT_DATABASE_LIMITS || grantStatus.curDbs < grantStatus.limitDbs) {
    return 0;
  } else {
    uError("grant failed to create db, exist:%" PRIu32 ", reason:grant database limited", grantStatus.curDbs);
    return TSDB_CODE_GRANT_DB_LIMITED;
  }
}

static int32_t grantCheckTimeSeries() {
  if (grantStatus.limitTimeSeries == GRANT_TIME_SERIES_LIMITS ||
      grantStatus.curTimeSeries <= grantStatus.limitTimeSeries) {
    return 0;
  } else {
    uError("grant failed to create table, exist:%" PRIu64 ", reason:grant timeseries limited",
           grantStatus.curTimeSeries);
    return TSDB_CODE_GRANT_TIMESERIES_LIMITED;
  }
}

static int32_t grantCheckAccts() {
  int32_t code = grantCheckUsers();
  if (code != 0) {
    return code;
  }

  if (grantStatus.limitAccts == GRANT_ACCT_LIMITS || grantStatus.curAccts < grantStatus.limitAccts) {
    return 0;
  } else {
    uError("grant failed to create account, exist:%" PRIu32 ", reason:grant account limited", grantStatus.curAccts);
    return TSDB_CODE_GRANT_ACCT_LIMITED;
  }
}

static int32_t grantCheckDnodes() {
  if (grantStatus.limitDnodes == GRANT_DNODE_LIMITS || grantStatus.curDnodes < grantStatus.limitDnodes) {
    return 0;
  } else {
    uError("grant failed to create dnode, exist:%" PRIu32 ", reason:grant dnode limited", grantStatus.curDnodes);
    return TSDB_CODE_GRANT_DNODE_LIMITED;
  }
}

static int32_t grantCheckStorage() {
  if (grantStatus.limitStorage == GRANT_STORAGE_LIMITS || grantStatus.curStorage <= grantStatus.limitStorage) {
    return 0;
  } else {
    uError("grant storage in-available, used:%" PRIu64 ", grant:%" PRIu64 ", reason:grant storage limited",
           grantStatus.curStorage, grantStatus.limitStorage);
    return TSDB_CODE_GRANT_STORAGE_LIMITED;
  }
}

static int32_t grantCheckGrantSpeed() { return TSDB_CODE_SUCCESS; }
static int32_t grantCheckQueryTime() { return TSDB_CODE_SUCCESS; }
static int32_t grantCheckConns() { return TSDB_CODE_SUCCESS; }
static int32_t grantCheckStreams() { return TSDB_CODE_SUCCESS; }
static int32_t grantCheckCpuCores() {
  if (grantStatus.limitCpuCores == GRANT_CPU_LIMITS || grantStatus.curCpuCores < grantStatus.limitCpuCores) {
    return 0;
  }
  uError("grant failed to create dnode, exist:%" PRIu32 ", reason:grant cpu cores limited", grantStatus.curCpuCores);
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
      return grantCheckExpired();
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
    default:
      break;
  }
#endif
  return TSDB_CODE_SUCCESS;
}

static FORCE_INLINE bool grantIsOfficial(SGrantStatus *pStatus) { return pStatus->officialVersion; }

static FORCE_INLINE bool grantIsValid(GrantMsg *pStatus) {
  return pStatus->limitTimeSeries || IS_GRANT_TDENGINE(pStatus);
}
static FORCE_INLINE bool grantConnIsValid(GrantMsg *pStatus) { return IS_GRANT_CONNECTORS(pStatus); }

static FORCE_INLINE bool grantShouldCheck(int64_t curTime) {
  if (curTime - *grantHandle.lastCheck < GRANT_CHECK_INTERVAL) {
    return false;
  }
  return true;
}

#ifndef GRANTS_CFG
static void grantStatusAssignLimits(GrantStatus *p1, GrantStatus *p2, bool isCombine) {
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

static void grantConnStatusAssignLimits(GrantStatus *p1, GrantStatus *p2, bool isCombine) {
  if (isCombine) {
    // use larger value
    if (IS_GRANT_CONNECTORS_OFFICIAL(p2)) {
      SET_GRANT_CONNECTORS_OFFICIAL(p1);
    }
    for (int32_t i = 0; i < GRANT_CONN_NUM; ++i) {
      SGrantConnItem *pItem = GRANT_CONN_ITEM(p1, i);
      SGrantConnItem *qItem = GRANT_CONN_ITEM(p2, i);
      GRANT_ITEM_SET_VAL(pItem->number, qItem->number, GRANT_CONN_LIMITS);
      GRANT_ITEM_SET_VAL(pItem->speed, qItem->speed, GRANT_CONN_LIMITS);
      GRANT_ITEM_SET_VAL(pItem->expire, qItem->expire, GRANT_CONN_EXPIRE_LIMITS);
    }
  } else {
    GRANT_CONN_OFFICIAL(p1) = GRANT_CONN_OFFICIAL(p2);
    memcpy(GRANT_CONN_ITEMS(p1), GRANT_CONN_ITEMS(p2), sizeof(SGrantConnItem) * GRANT_CONN_NUM);
  }
}

#ifdef GRANTS_CFG
static void grantConnStatusCheck(SMnode *pMnode, uint32_t curTime) {
  int32_t   nGrantConn = 0;
  SHashObj *pGrants = grantHandle.pOfficials;
  if (taosHashGetSize(pGrants) > 0) {
    GrantStatus  status = {0};
    GrantStatus *iter = taosHashIterate(pGrants, NULL);

    while (iter) {
      if (IS_GRANT_CONNECTORS(iter)) {
        grantConnStatusAssignLimits(&status, iter, true);
        ++nGrantConn;
      }
      iter = taosHashIterate(pGrants, iter);
    }
    if (nGrantConn > 0) grantConnStatusAssignLimits(&gStatus, &status, false);

    taosHashClear(pGrants);
  }

  if (nGrantConn == 0) {
    grantConnResetMaster(pMnode);
  }

  *grantHandle.lastCheck = curTime;
}
#endif

#ifndef GRANTS_CFG
static void grantStatusCheck(SMnode *pMnode, uint32_t curTime) {
  int32_t   nGrant = 0;
  int32_t   nGrantConn = 0;
  SHashObj *pGrants = grantHandle.pOfficials;
  if (taosHashGetSize(pGrants) > 0) {
    GrantStatus  status = {0};
    GrantStatus *iter = taosHashIterate(pGrants, NULL);

    while (iter) {
      if (IS_GRANT_TDENGINE(iter)) {
        grantStatusAssignLimits(&status, iter, true);
        ++nGrant;
      }
      if (IS_GRANT_CONNECTORS(iter)) {
        grantConnStatusAssignLimits(&status, iter, true);
        ++nGrantConn;
      }
      iter = taosHashIterate(pGrants, iter);
    }

    if (nGrant > 0) grantStatusAssignLimits(&gStatus, &status, false);

    if (nGrantConn > 0) grantConnStatusAssignLimits(&gStatus, &status, false);

    taosHashClear(pGrants);

    uDebug("grant reset. usbDongle:%d, official:%d, expired:%d, expireTime:%" PRIu32 ", limitTimeSeries:%" PRIu64,
           gStatus.usbDongle, gStatus.officialVersion, gStatus.expired, gStatus.expireTimeSec, gStatus.limitTimeSeries);
  }

  if (nGrant == 0) {
    char *ts = grantSecondsToString(*grantHandle.lastCheck);
    uWarn("grant reset because official grants not received since %s", ts);
    taosMemoryFree(ts);
    grantResetMaster(pMnode);
  }

  if (nGrantConn == 0) {
    grantConnResetMaster(pMnode);
  }

  *grantHandle.lastCheck = curTime;
}

static int32_t grantStatusCompare(SGrantStatus *p1, SGrantStatus *p2) {
  int32_t result = 0;

  bool    offical1 = grantIsOfficial(p1);
  bool    offical2 = grantIsOfficial(p2);

  if (offical1 < offical2) {
    result = -1;
  } else if (offical1 > offical2) {
    return 1;
  }
  // compare neccessary grant items, adjust the check if needed
  GRANT_ITEM_COMPARE(p1->expireTimeSec, p2->expireTimeSec, GRANT_EXPIRE_TIME);
  GRANT_ITEM_COMPARE(p1->limitTimeSeries, p2->limitTimeSeries, GRANT_TIME_SERIES_LIMITS);
  GRANT_ITEM_COMPARE(p1->limitStorage, p2->limitStorage, GRANT_STORAGE_LIMITS);
  GRANT_ITEM_COMPARE(p1->limitDbs, p2->limitDbs, GRANT_DATABASE_LIMITS);
  GRANT_ITEM_COMPARE(p1->limitDnodes, p2->limitDnodes, GRANT_DNODE_LIMITS);
  GRANT_ITEM_COMPARE(p1->limitCpuCores, p2->limitCpuCores, GRANT_CPU_LIMITS);
  return result;
}
#endif

static int32_t grantConnStatusCompare(GrantStatus *p1, GrantStatus *p2) {
  int32_t result = 0;
  bool    official1 = IS_GRANT_CONNECTORS_OFFICIAL(p1);
  bool    official2 = IS_GRANT_CONNECTORS_OFFICIAL(p2);
  if (official1 < official2) {
    result = -1;
  } else if (official1 > official2) {
    return 1;
  }
  for (int32_t i = 0; i < GRANT_CONN_NUM; ++i) {
    SGrantConnItem *pItem = GRANT_CONN_ITEM(p1, i);
    SGrantConnItem *qItem = GRANT_CONN_ITEM(p2, i);
    GRANT_ITEM_COMPARE(pItem->number, qItem->number, GRANT_CONN_LIMITS);
    GRANT_ITEM_COMPARE(pItem->speed, qItem->speed, GRANT_CONN_LIMITS);
    GRANT_ITEM_COMPARE(pItem->expire, qItem->expire, GRANT_CONN_EXPIRE_LIMITS);
  }
  return result;
}

static int32_t mndProcessDnodeSGrantMsg(SMnode *pMnode, SDnodeInfo *pDnodeInfo, GrantMsg *pGrantMsg,
                                        GrantStatus *pGrantStatus) {
  uint32_t curTime = taosGetTimestampMs() / 1000;
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
    // take effect right now when grants upgrade
    int32_t grantCompare = grantConnStatusCompare(&status, &gStatus);
    if (grantCompare > 0) {
      if (GRANT_CONN_OFFICIAL(&gStatus) == GRANT_CONN_OFFICIAL(&status)) {
        // use larger value
        grantConnStatusAssignLimits(&gStatus, &status, true);
      } else {
        // from trial to official, assign the value directly
        grantConnStatusAssignLimits(&gStatus, &status, false);
      }
    }

    taosHashPut(grantHandle.pOfficials, &pDnodeInfo->id, sizeof(TSDB_DATA_TYPE_UINT), &status, sizeof(GrantStatus));
  }

  bool shouldCheck = grantShouldCheck(curTime);
  uTrace("grant message received from dnode:%" PRIu32 ", should check: %s, curTime:%" PRIu32
         ", grantLastCheck:%" PRIu32,
         pDnodeInfo->id, shouldCheck ? "true" : "false", curTime, *grantHandle.lastCheck);
  if (shouldCheck) grantConnStatusCheck(pMnode, curTime);

  uInfo("grant message received from dnode, timeseries:%" PRIu64
        ", database:%u, stable:%u, table:%u, set to grant state",
        pGrantStatus->limitTimeSeries, pGrantStatus->limitDbs, pGrantStatus->limitSTables, pGrantStatus->limitTables);
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

      // take effect right now when grants upgrade
      int32_t grantCompare = grantStatusCompare(&status, &grantStatus);
      if (grantCompare > 0) {
        if (grantStatus.officialVersion == status.officialVersion) {
          // use larger value
          grantStatusAssignLimits(&grantStatus, &status, true);
        } else {
          // from trial to official, assign the value directly
          grantStatusAssignLimits(&grantStatus, &status, false);
        }
      }
    }

    // assign the connectors
    if (grantConnIsValid(pGrantMsg)) {
      SET_GRANT_CONNECTORS(&status);
      GRANT_CONN_OFFICIAL(&status) = GRANT_CONN_OFFICIAL(pGrantMsg);
      memcpy(GRANT_CONN_ITEMS(&status), GRANT_CONN_ITEMS(pGrantMsg), sizeof(SGrantConnItem) * GRANT_CONN_NUM);
      // take effect right now when grants upgrade
      int32_t grantCompare = grantConnStatusCompare(&status, &grantStatus);
      if (grantCompare > 0) {
        if (GRANT_CONN_OFFICIAL(&grantStatus) == GRANT_CONN_OFFICIAL(&status)) {
          // use larger value
          grantConnStatusAssignLimits(&grantStatus, &status, true);
        } else {
          // from trial to official, assign the value directly
          grantConnStatusAssignLimits(&grantStatus, &status, false);
        }
      }
    }

    taosHashPut(grantHandle.pOfficials, &pDnodeInfo->id, sizeof(TSDB_DATA_TYPE_UINT), &status, sizeof(SGrantStatus));
  }

  bool shouldCheck = grantShouldCheck(curTime);
  uTrace("grant message received from dnode:%" PRIu32 ", should check: %s, curTime:%" PRIu32
         ", grantLastCheck:%" PRIu32,
         pDnodeInfo->id, shouldCheck ? "true" : "false", curTime, *grantHandle.lastCheck);
  if (shouldCheck) grantStatusCheck(pMnode, curTime);

  char *ts = grantSecondsToString(pGrantStatus->expireTimeSec);
  if (pGrantStatus->expireTimeSec > curTime) {
    if (pGrantStatus->expired) {
      pGrantStatus->expired = false;
      uInfo("grant message received from dnode:%" PRIu32 ", storage:%uGB, timeseries:%" PRIu64
            ", database:%u, user:%u, expire:%s %u, curtime:%u, set to grant state",
            pDnodeInfo->id, (uint32_t)(pGrantStatus->limitStorage / (int64_t)1073741824), pGrantStatus->limitTimeSeries,
            pGrantStatus->limitDbs, pGrantStatus->limitUsers, ts, pGrantStatus->expireTimeSec, curTime);
    } else {
      uTrace("grant message received from dnode:%" PRIu32 ", storage:%uGB, timeseries:%" PRIu64
             ", database:%u, user:%u, expire:%s %u, curtime:%u, already in grant state",
             pDnodeInfo->id, (uint32_t)(pGrantStatus->limitStorage / (int64_t)1073741824),
             pGrantStatus->limitTimeSeries, pGrantStatus->limitDbs, pGrantStatus->limitUsers, ts,
             pGrantStatus->expireTimeSec, curTime);
    }
  } else {
    uError("grant cluster expired at %s %u, curtime: %u, set to un-grant state", ts, pGrantStatus->expireTimeSec,
           curTime);
    pGrantStatus->expired = true;
  }

  taosMemoryFree(ts);
#endif
  return TSDB_CODE_SUCCESS;
}

static int32_t mndRetrieveGrant(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode *pMnode = pReq->info.node;
  int32_t numOfRows = 0;
  int32_t cols = 0;
  char   *pWrite = NULL;
  char    tmp[192] = {0};
  char    tmp1[192] = {0};

  if (pShow->numOfRows < 1) {
    SGrantConnItem *pItem = NULL;
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

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char      expire[22] = {0};
    time_t    tt = grantStatus.expireTimeSec;
    struct tm ptm;
    if (taosLocalTime(&tt, &ptm, expire) != NULL) {
      strftime(expire, 21, "%Y-%m-%d %H:%M:%S", &ptm);
    }
    src = grantStatus.expireTimeSec != GRANT_EXPIRE_TIME ? expire : "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = grantStatus.expired ? "true" : "false";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if ((uint32_t)(grantStatus.limitStorage / (int64_t)1073741824) != GRANT_STORAGE_LIMITS) {
      sprintf(tmp1, "%" PRIu32 "/%" PRIu32, (uint32_t)(grantStatus.curStorage / (int64_t)1073741824),
              (uint32_t)(grantStatus.limitStorage / (int64_t)1073741824));
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitTimeSeries != GRANT_TIME_SERIES_LIMITS) {
      sprintf(tmp1, "%" PRIu64 "/%" PRIu64, grantStatus.curTimeSeries, grantStatus.limitTimeSeries);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitDbs != GRANT_DATABASE_LIMITS) {
      sprintf(tmp1, "%u/%u", grantStatus.curDbs, grantStatus.limitDbs);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitUsers != GRANT_USER_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurUsers(pMnode), grantStatus.limitUsers);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitAccts != GRANT_ACCT_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurAccts(pMnode), grantStatus.limitAccts);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitDnodes != GRANT_DNODE_LIMITS) {
      sprintf(tmp1, "%u/%u", grantStatus.curDnodes, grantStatus.limitDnodes);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);  // connections

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);  // streams

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitCpuCores != GRANT_ACCT_LIMITS) {
      sprintf(tmp1, "%u/%u", grantStatus.curCpuCores, grantStatus.limitCpuCores);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);  // cpu cores

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);  // speed

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataSetVal(pColInfo, numOfRows, tmp, false);  // query time
#endif
    // connectors
    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    pItem = GRANT_CONN_ITEM(&gStatus, CONN_TYPE_OPC_DA);
    sprintf(tmp1, "{\"type\":\"OPC_DA\",\"number\":%d,\"speed\":%" PRIi16 ",\"expire\":\"%" PRIu16 "\"}", pItem->number,
            pItem->speed, pItem->expire);

    STR_WITH_SIZE_TO_VARSTR(tmp, tmp1, strlen(tmp1));
    colDataAppend(pColInfo, numOfRows, tmp, false);  // opc_da

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    pItem = GRANT_CONN_ITEM(&gStatus, CONN_TYPE_OPC_UA);
    sprintf(tmp1, "{\"type\":\"OPC_UA\",\"number\":%d,\"speed\":%" PRIi16 ",\"expire\":\"%" PRIu16 "\"}", pItem->number,
            pItem->speed, pItem->expire);
    STR_WITH_SIZE_TO_VARSTR(tmp, tmp1, strlen(tmp1));
    colDataAppend(pColInfo, numOfRows, tmp, false);  // opc_ua

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    pItem = GRANT_CONN_ITEM(&gStatus, CONN_TYPE_PI);
    sprintf(tmp1, "{\"type\":\"Pi\",\"number\":%d,\"speed\":%" PRIi16 ",\"expire\":\"%" PRIu16 "\"}", pItem->number,
            pItem->speed, pItem->expire);
    STR_WITH_SIZE_TO_VARSTR(tmp, tmp1, strlen(tmp1));
    colDataAppend(pColInfo, numOfRows, tmp, false);  // pi

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    pItem = GRANT_CONN_ITEM(&gStatus, CONN_TYPE_KAFKA);
    sprintf(tmp1, "{\"type\":\"Kafka\",\"number\":%d,\"speed\":%" PRIi16 ",\"expire\":\"%" PRIu16 "\"}", pItem->number,
            pItem->speed, pItem->expire);
    STR_WITH_SIZE_TO_VARSTR(tmp, tmp1, strlen(tmp1));
    colDataAppend(pColInfo, numOfRows, tmp, false);  // kafka

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    pItem = GRANT_CONN_ITEM(&gStatus, CONN_TYPE_INFLUXDB);
    sprintf(tmp1, "{\"type\":\"InfluxDB\",\"number\":%d,\"speed\":%" PRIi16 ",\"expire\":\"%" PRIu16 "\"}",
            pItem->number, pItem->speed, pItem->expire);
    STR_WITH_SIZE_TO_VARSTR(tmp, tmp1, strlen(tmp1));
    colDataAppend(pColInfo, numOfRows, tmp, false);  // influxdb

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    pItem = GRANT_CONN_ITEM(&gStatus, CONN_TYPE_MQTT);
    sprintf(tmp1, "{\"type\":\"MQTT\",\"number\":%d,\"speed\":%" PRIi16 ",\"expire\":\"%" PRIu16 "\"}", pItem->number,
            pItem->speed, pItem->expire);
    STR_WITH_SIZE_TO_VARSTR(tmp, tmp1, strlen(tmp1));
    colDataAppend(pColInfo, numOfRows, tmp, false);  // mqtt

    numOfRows++;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextGrant(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

int32_t tSerializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, SDnodeInfo *pInfo) {
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

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus, SDnodeInfo *pInfo) {
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
    tstrncpy(pInfo->active, data, TSDB_ACTIVE_KEY_LEN);
    if (tDecodeBinary(&decoder, (uint8_t **)&data, NULL) < 0) return -1;
    tstrncpy(pInfo->connActive, data, TSDB_CONN_ACTIVE_KEY_LEN);

  }

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeGrantMsg(void *buf, int32_t bufLen, GrantMsg *pMsg) {
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

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeGrantMsg(void *buf, int32_t bufLen, GrantMsg *pMsg) {
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

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
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
  if (tEncodeU8(encoder, CONN_TYPE_MAX) < 0) return -1;
  for (int32_t i = 0; i < CONN_TYPE_MAX; ++i) {
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