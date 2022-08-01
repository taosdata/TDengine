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
#include "mndCluster.h"
#include "mndDb.h"
#include "mndDef.h"
#include "mndDnode.h"
#include "mndGrant.h"
#include "mnode.h"
#include "os.h"
#include "tdataformat.h"
#include "tglobal.h"
#include "tlog.h"
#include "trpc.h"
#include "ttimer.h"
#include "tutil.h"
#include "mndAcct.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndUser.h"
#include "sdb.h"
#include "mndSync.h"
#include "tgrantCfg.h"

#define COMPARE_SET_VAL(a, b, _comp_sign_) \
  do {                                     \
    if ((a)_comp_sign_(b)) {               \
      (a) = (b);                           \
    }                                      \
  } while (0)

#ifndef min
#define min(x, y) (x) < (y) ? (x) : (y)
#endif
#if 1
extern void *tsMnodeTmr;
#endif

#ifdef CFG_GRANTS
typedef struct {
  bool     updateForced;
  uint64_t limitTimeSeries;
  uint32_t limitDbs;
  uint32_t limitSTables;
  uint32_t limitTables;
} SCloudGrantMsg;

typedef struct {
  uint64_t curTimeSeries;
  uint64_t limitTimeSeries;
  uint32_t curDbs;
  uint32_t limitDbs;
  uint32_t curSTables;
  uint32_t limitSTables;
  uint32_t curTables;
  uint32_t limitTables;
} SCloudGrantStatus;

SCloudGrantStatus cloudGrantStatus = {0,
                                      GRANT_TIME_SERIES_LIMITS,
                                      0,
                                      GRANT_DATABASE_LIMITS,
                                      0,
                                      GRANT_STABLE_LIMITS,
                                      0,
                                      GRANT_TABLE_LIMITS};

GRANT_CFG_EXTERN;
typedef SCloudGrantStatus GrantStatus;
typedef SCloudGrantMsg    GrantMsg;
#else
typedef SGrantStatus GrantStatus;
typedef SGrantMsg    GrantMsg;
#endif

extern SGrantObj grantObj;

static char    *grantSecondsToString(uint32_t seconds);
static void     dmRefreshGrantCfg();
static void     grantRetrieveGrantInfo(SMnode *pMnode);
static void     grantResetMaster(SMnode *pMnode);
static int32_t  mndProcessGrantHB(SRpcMsg *pReq);
static int32_t  dmGenerateGrantMsg(GrantMsg *pGrant, GrantStatus *pGrantStatus);
static int32_t  mndProcessDnodeSGrantMsg(SMnode *pMnode, GrantMsg *pGrantMsg, GrantStatus *pGrantStatus);
static int32_t  tSerializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus);
static int32_t  tDeserializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus);
static int32_t  tSerializeGrantMsg(void *buf, int32_t bufLen, GrantMsg *pMsg);
static int32_t  tDeserializeGrantMsg(void *buf, int32_t bufLen, GrantMsg *pMsg);
static uint64_t grantGetClusterCurTimeSeries(SMnode *pMnode);

static int32_t mndRetrieveGrant(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextGrant(SMnode *pMnode, void *pIter);

static void *grantCheckTimer = NULL;
static void *grantSendTimer = NULL;
SGrantStatus grantStatus = {false,
                            false,
                            false,
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
                            GRANT_CPU_LIMITS};

// extern SSysTableMeta infosMeta[];
#ifdef CFG_GRANTS
#define status cloudGrantStatus
#else
#define status grantStatus
#endif
int32_t mndInitGrant(SMnode *pMnode) {
  tsGrantHBInterval = GRANT_CHECK_INTERVAL;
  // fprintf(stdout,"%s(%d) %s %08" PRId64 " sizeof(infosMeta)=%d infosMeta=%p\n", __FILE__, __LINE__,__func__,taosGetSelfPthreadId(),sizeof(infosMeta),infosMeta);fflush(stdout);
  mndSetMsgHandle(pMnode, TDMT_MND_GRANT_HB_TIMER, mndProcessGrantHB);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndRetrieveGrant);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndCancelGetNextGrant);
  

  uInfo("grant data is initialized");
  return TSDB_CODE_SUCCESS;
}

void mndCleanupGrant() {
  taosTmrStopA(&grantCheckTimer);
  taosTmrStopA(&grantSendTimer);
}

/**
 * @brief process grant status msg in dnode and respond with grant msg
 *
 * @param pMsg
 * @return int32_t
 */
int32_t dmProcessGrantReq(SRpcMsg *pMsg) {
  if (!pMsg->pCont || (pMsg->contLen <= 0)) {
    terrno = TSDB_CODE_INVALID_MSG;
    uWarn("failed to process grant req in dnode since msg is empty");
    goto _err;
  }
  // step 1: process grant status from mnode
  GrantStatus grantStatusReq = {0};
  if (tDeserializeGrantStatus(pMsg->pCont, pMsg->contLen, &grantStatusReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    uWarn("failed to process grant req in dnode since %s", terrstr());
    goto _err;
  }

  // step 2: set local dnode grant status
#ifdef CFG_GRANTS
  cloudGrantStatus.curTimeSeries = grantStatusReq.curTimeSeries;
  cloudGrantStatus.curDbs = grantStatusReq.curDbs;
  cloudGrantStatus.curSTables = grantStatusReq.curSTables;
  cloudGrantStatus.curTables = grantStatusReq.curTables;
#else
  grantStatus.curTimeSeries = grantStatusReq.curTimeSeries;
  grantStatus.curStorage = grantStatusReq.curStorage;
  grantStatus.curSpeed = grantStatusReq.curSpeed;
  grantStatus.curQueryTime = grantStatusReq.curQueryTime;
#endif

  // step 3: respond with grant msg
  GrantMsg grantMsg = {0};
  dmGenerateGrantMsg(&grantMsg, &grantStatusReq);
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

  uInfo("succeed to process grant req and send rsp in dnode");

  return TSDB_CODE_SUCCESS;
_err:
  pMsg->code = terrno;
  pMsg->info.rsp = NULL;
  pMsg->info.rspLen = 0;

  return TSDB_CODE_FAILED;
}

static void dmRefreshGrantCfg() {
  char cfgFile[PATH_MAX] = {0};
  sprintf(cfgFile, "%s/taos.cfg", configDir);
  grantActiveSystem(cfgFile);
}

static int32_t dmGenerateGrantMsg(GrantMsg *pGrantMsg, GrantStatus *pGrantStatus) {
#ifdef CFG_GRANTS
    pGrantMsg->updateForced = false;
    if (cloudGrantStatus.limitTimeSeries != tsGrantLimitTimeSeries) pGrantMsg->updateForced = true;
    if (cloudGrantStatus.limitDbs != tsGrantLimitDbs) pGrantMsg->updateForced = true;
    if (cloudGrantStatus.limitSTables != tsGrantLimitSTables) pGrantMsg->updateForced = true;
    if (cloudGrantStatus.limitTables != tsGrantLimitTables) pGrantMsg->updateForced = true;
    if (pGrantMsg->updateForced) {
      cloudGrantStatus.limitTimeSeries = tsGrantLimitTimeSeries;
      cloudGrantStatus.limitDbs = tsGrantLimitDbs;
      cloudGrantStatus.limitSTables = tsGrantLimitSTables;
      cloudGrantStatus.limitTables = tsGrantLimitTables;
    } else {
      COMPARE_SET_VAL(cloudGrantStatus.limitTimeSeries, pGrantStatus->limitTimeSeries, <);
      COMPARE_SET_VAL(cloudGrantStatus.limitDbs, pGrantStatus->limitDbs, <);
      COMPARE_SET_VAL(cloudGrantStatus.limitSTables, pGrantStatus->limitSTables, <);
      COMPARE_SET_VAL(cloudGrantStatus.limitTables, pGrantStatus->limitTables, <);
    }
    
    uInfo("dnode send grant message,timeseries:%" PRIu64 ", database:%u, stable:%u, table:%u, set to grant state",
          cloudGrantStatus.limitTimeSeries, cloudGrantStatus.limitDbs, cloudGrantStatus.limitSTables, cloudGrantStatus.limitTables);
    pGrantMsg->limitTimeSeries = cloudGrantStatus.limitTimeSeries;
    pGrantMsg->limitDbs = cloudGrantStatus.limitDbs;
    pGrantMsg->limitSTables = cloudGrantStatus.limitSTables;
    pGrantMsg->limitTables = cloudGrantStatus.limitTables;
#else
  // refresh
  dmRefreshGrantCfg();

  uint32_t curTime = taosGetTimestampSec();
  if (grantObj.updateForced) {
    grantStatus.usbDongle = grantObj.usbDongle > 0 ? true : false;
    grantStatus.officialVersion = grantObj.officialVersion > 0 ? true : false;
    grantStatus.lastReceived = curTime;
    if (grantObj.granted) {
      grantStatus.expireTimeSec = grantObj.expireTimeSec;
    } else {
      grantStatus.expireTimeSec = 0;
    }
    grantStatus.limitStorage = (int64_t)(grantObj.limitStorage * (int64_t)1073741824);
    grantStatus.limitSpeed = grantObj.limitSpeed;
    grantStatus.limitTimeSeries = grantObj.limitTimeSeries;
    grantStatus.limitQueryTime = grantObj.limitQueryTime;
    grantStatus.limitDbs = grantObj.limitDbs;
    grantStatus.limitUsers = grantObj.limitUsers;
    grantStatus.limitConns = grantObj.limitConns;
    grantStatus.limitStreams = grantObj.limitStreams;
    grantStatus.limitAccts = grantObj.limitAccts;
    grantStatus.limitDnodes = grantObj.limitDnodes;
    grantStatus.limitCpuCores = grantObj.limitCpuCores;
  } else if (grantObj.granted) {
    if (pGrantStatus->usbDongle) {
      grantStatus.usbDongle = pGrantStatus->usbDongle;
    }
    grantStatus.officialVersion = pGrantStatus->officialVersion;
    grantStatus.lastReceived = curTime;

    COMPARE_SET_VAL(grantStatus.expireTimeSec, pGrantStatus->expireTimeSec, <);
    COMPARE_SET_VAL(grantStatus.limitStorage, pGrantStatus->limitStorage, <);
    COMPARE_SET_VAL(grantStatus.limitSpeed, pGrantStatus->limitSpeed, <);
    COMPARE_SET_VAL(grantStatus.limitTimeSeries, pGrantStatus->limitTimeSeries, <);
    COMPARE_SET_VAL(grantStatus.limitQueryTime, pGrantStatus->limitQueryTime, <);
    COMPARE_SET_VAL(grantStatus.limitDbs, pGrantStatus->limitDbs, <);
    COMPARE_SET_VAL(grantStatus.limitUsers, pGrantStatus->limitUsers, <);
    COMPARE_SET_VAL(grantStatus.limitConns, pGrantStatus->limitConns, <);
    COMPARE_SET_VAL(grantStatus.limitStreams, pGrantStatus->limitStreams, <);
    COMPARE_SET_VAL(grantStatus.limitAccts, pGrantStatus->limitAccts, <);
    COMPARE_SET_VAL(grantStatus.limitDnodes, pGrantStatus->limitDnodes, <);
    COMPARE_SET_VAL(grantStatus.limitCpuCores, pGrantStatus->limitCpuCores, <);
  } else {
    grantStatus.usbDongle = pGrantStatus->usbDongle;
    grantStatus.officialVersion = pGrantStatus->officialVersion;
    grantStatus.lastReceived = curTime;
    grantStatus.expireTimeSec = pGrantStatus->expireTimeSec;
    grantStatus.limitStorage = pGrantStatus->limitStorage;
    grantStatus.limitSpeed = pGrantStatus->limitSpeed;
    grantStatus.limitTimeSeries = pGrantStatus->limitTimeSeries;
    grantStatus.limitQueryTime = pGrantStatus->limitQueryTime;
    grantStatus.limitDbs = pGrantStatus->limitDbs;
    grantStatus.limitUsers = pGrantStatus->limitUsers;
    grantStatus.limitConns = pGrantStatus->limitConns;
    grantStatus.limitStreams = pGrantStatus->limitStreams;
    grantStatus.limitAccts = pGrantStatus->limitAccts;
    grantStatus.limitDnodes = pGrantStatus->limitDnodes;
    grantStatus.limitCpuCores = pGrantStatus->limitCpuCores;

  }

  char *ts = grantSecondsToString(grantStatus.expireTimeSec);
  if (grantStatus.expireTimeSec > curTime) {
    uInfo("dnode send grant message, storage:%uGB, timeseries:%" PRIu64
          ", database:%u, user:%u, expire:%s %u, curtime:%u, set to grant state",
          (uint32_t)(grantStatus.limitStorage / (int64_t)1073741824), grantStatus.limitTimeSeries, grantStatus.limitDbs,
          grantStatus.limitUsers, ts, grantStatus.expireTimeSec, curTime);
    grantStatus.expired = false;
  } else {
    uError("grant cluster expired at %s %u, curtime: %u, set to un-grant state", ts, grantStatus.expireTimeSec,
           curTime);
    grantStatus.expired = true;
  }
  taosMemoryFree(ts);

  pGrantMsg->usbDongle = grantStatus.usbDongle;
  pGrantMsg->updateForced = grantObj.updateForced;
  pGrantMsg->officialVersion = grantStatus.officialVersion;
  pGrantMsg->expireTimeSec = grantStatus.expireTimeSec;
  pGrantMsg->limitStorage = (uint32_t)(grantStatus.limitStorage / (int64_t)1073741824);
  pGrantMsg->limitSpeed = grantStatus.limitSpeed;
  pGrantMsg->limitTimeSeries = grantStatus.limitTimeSeries;
  pGrantMsg->limitQueryTime = grantStatus.limitQueryTime;
  pGrantMsg->limitDbs = grantStatus.limitDbs;
  pGrantMsg->limitUsers = grantStatus.limitUsers;
  pGrantMsg->limitConns = grantStatus.limitConns;
  pGrantMsg->limitStreams = grantStatus.limitStreams;
  pGrantMsg->limitAccts = grantStatus.limitAccts;
  pGrantMsg->limitDnodes = grantStatus.limitDnodes;
  pGrantMsg->limitCpuCores = grantStatus.limitCpuCores;
  pGrantMsg->reserveKey1 = grantObj.reserveKey1;
  pGrantMsg->reserveKey2 = grantObj.reserveKey2;
#endif

  return TSDB_CODE_SUCCESS;
}

/**
 * @brief 1) send grant status to dnode
 *        2) process response (grant msg) from dnode
 * @param pMnode
 * @param pDnodeEp
 * @return int32_t
 */
static int32_t mndSendGrantStatusToDnode(SMnode *pMnode, SDnodeEp *pDnodeEp) {
  // step 1: send grant status to dnode
  int32_t contLen = tSerializeGrantStatus(NULL, 0, &status);
  void   *pCont = rpcMallocCont(contLen);
  if (!pCont) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uWarn("failed to generate grant status msg since %s", terrstr());
    return TSDB_CODE_FAILED;
  }

  tSerializeGrantStatus(pCont, contLen, &status);

  SRpcMsg rpcMsg = {.pCont = pCont, .contLen = contLen, .msgType = TDMT_MND_GRANT};
  SRpcMsg rpcRsp = {0};

  uInfo("send grant status msg to dnode:%d %s:%" PRIu16, pDnodeEp->id, pDnodeEp->ep.fqdn, pDnodeEp->ep.port);

  SEpSet epSet = {.numOfEps = 1};
  strncpy(epSet.eps[0].fqdn, pDnodeEp->ep.fqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = pDnodeEp->ep.port;

  // TODO: use async mode instead of sync mode
  rpcSendRecv(pMnode->msgCb.clientRpc, &epSet, &rpcMsg, &rpcRsp);

  // step 2: process response from dnode
  if (!rpcRsp.pCont || rpcRsp.contLen <= 0 || rpcRsp.code != 0) {
    uError("failed to process the grant rsp from dnode since empty content: %" PRIi32, rpcRsp.code);
    goto _err;
  }

  GrantMsg grantMsgRsp = {0};
  if (tDeserializeGrantMsg(rpcRsp.pCont, rpcRsp.contLen, &grantMsgRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    uWarn("failed to process the grant rsp from dnode since %s", terrstr());
    goto _err;
  }

  uInfo("succeed to receive grant msg from dnode");
  mndProcessDnodeSGrantMsg(pMnode, &grantMsgRsp, &status);

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
  SMnode *pMnode = pReq->info.node;
  int32_t dnodeSize = mndGetDnodeSize(pMnode);

  SArray *pDnodeEps = taosArrayInit(dnodeSize, sizeof(SDnodeEp));
  if (!pDnodeEps) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uWarn("failed to process grant hb msg since %s", terrstr());
    return -1;
  }

  grantRetrieveGrantInfo(pMnode);

  mndGetDnodeData(pMnode, pDnodeEps);

  for (int32_t i = 0; i < taosArrayGetSize(pDnodeEps); ++i) {
    SDnodeEp *pDnodeEp = (SDnodeEp *)taosArrayGet(pDnodeEps, i);
    mndSendGrantStatusToDnode(pMnode, pDnodeEp);
  }

  taosArrayDestroy(pDnodeEps);

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
  char      *ts = taosMemoryCalloc(64, 1);
  time_t     sec = seconds;
  struct tm  ptm;
  taosLocalTime(&sec, &ptm);
  strftime(ts, 64, "%Y-%m-%d %H:%M:%S", &ptm);
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

static uint32_t grantGetClusterCurSTables(SMnode *pMnode) { return 0; }

static uint32_t grantGetClusterCurTables(SMnode *pMnode) { return 0; }

/**
 * @brief retrieve the statis info
 *
 * @param pMnode
 */
static void grantRetrieveGrantInfo(SMnode *pMnode) {
#ifdef CFG_GRANTS
  cloudGrantStatus.curTimeSeries = grantGetClusterCurTimeSeries(pMnode);
  cloudGrantStatus.curDbs = grantGetClusterCurDbs(pMnode);
  cloudGrantStatus.curSTables = grantGetClusterCurSTables(pMnode);
  cloudGrantStatus.curTables = grantGetClusterCurSTables(pMnode);
#else
  grantStatus.curStorage = grantGetClusterCurStorage(pMnode);
  grantStatus.curSpeed = grantGetClusterCurSpeed();
  grantStatus.curTimeSeries = grantGetClusterCurTimeSeries(pMnode);
  grantStatus.curQueryTime = grantGetClusterCurQueryTime();
  grantStatus.curUsers = grantGetClusterCurUsers(pMnode);
  grantStatus.curAccts = grantGetClusterCurAccts(pMnode);
  grantStatus.curDnodes = grantGetClusterCurDnodes(pMnode);
  grantStatus.curDbs = grantGetClusterCurDbs(pMnode);
#endif
}

/**
 * @brief init the grant status after mnode startup
 *
 * @param pMnode
 */
static void grantResetMaster(SMnode *pMnode) {
  grantRetrieveGrantInfo(pMnode);
#ifndef CFG_GRANTS
  uint32_t clusterCreateTime = grantGetClusterCreateTime(pMnode);

  grantStatus.expireTimeSec = clusterCreateTime + GRANT_DEFAULT;
  // grantStatus.expireTimeSec = grantStatus.expireTimeSec; // TODO: Why this logic changes from 2.0?
  grantStatus.expireTimeSec += GRANT_TOLERENCE;
  grantStatus.lastReceived = grantStatus.expireTimeSec;
  grantStatus.expired = false;

  char *ts = grantSecondsToString(grantStatus.expireTimeSec);
  uInfo("grant expire time reset to %s %u, current timeseries %" PRIu64, ts, grantStatus.expireTimeSec,
        grantStatus.curTimeSeries);
  taosMemoryFree(ts);
#endif
}

void grantReset(SMnode *pMnode, EGrantType grant, uint64_t value) {
  switch (grant) {
    case TSDB_GRANT_ALL:
      grantResetMaster(pMnode);
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

#ifdef CFG_GRANTS
static int32_t cloudGrantCheckTimeSeries() {
  if (cloudGrantStatus.limitTimeSeries == GRANT_TIME_SERIES_LIMITS || cloudGrantStatus.curTimeSeries < cloudGrantStatus.limitTimeSeries) {
    return TSDB_CODE_SUCCESS;
  } else {
    uError("grant failed to create table, exist:%" PRIu64 ", reason:grant timeseries limited", cloudGrantStatus.curTimeSeries);
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
  if (cloudGrantStatus.limitSTables == GRANT_STABLE_LIMITS || cloudGrantStatus.curSTables < cloudGrantStatus.limitSTables) {
    return TSDB_CODE_SUCCESS;
  } else {
    uError("grant failed to create stable, exist:%" PRIu32 ", reason:grant stable limited", cloudGrantStatus.curSTables);
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
static int32_t grantCheckCpuCores() { return TSDB_CODE_SUCCESS; }

#endif

int32_t grantCheck(EGrantType grant) {
#ifdef CFG_GRANTS
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

static int32_t mndProcessDnodeSGrantMsg(SMnode *pMnode, GrantMsg *pGrantMsg, GrantStatus *pGrantStatus) {
#ifdef CFG_GRANTS
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

  uInfo("grant message received from dnode, timeseries:%" PRIu64 ", database:%u, stable:%u, table:%u, set to grant state",
        pGrantStatus->limitTimeSeries, pGrantStatus->limitDbs, pGrantStatus->limitSTables,pGrantStatus->limitTables);
#else
  uint32_t curTime = taosGetTimestampSec();
  // TODO: process grant status from mnode
  if (pGrantMsg->updateForced) {
    pGrantStatus->usbDongle = pGrantMsg->usbDongle;
    pGrantStatus->officialVersion = pGrantMsg->officialVersion;
    pGrantStatus->lastReceived = curTime;

    if (pGrantMsg->expireTimeSec == 0) {
      grantResetMaster(pMnode);
    } else {
      pGrantStatus->expireTimeSec = pGrantMsg->expireTimeSec;
    }
    pGrantStatus->limitStorage = (int64_t)(pGrantMsg->limitStorage * (int64_t)1073741824);
    pGrantStatus->limitSpeed = pGrantMsg->limitSpeed;
    pGrantStatus->limitTimeSeries = pGrantMsg->limitTimeSeries;
    pGrantStatus->limitQueryTime = pGrantMsg->limitQueryTime;
    pGrantStatus->limitDbs = pGrantMsg->limitDbs;
    pGrantStatus->limitUsers = pGrantMsg->limitUsers;
    pGrantStatus->limitConns = pGrantMsg->limitConns;
    pGrantStatus->limitStreams = pGrantMsg->limitStreams;
    pGrantStatus->limitAccts = pGrantMsg->limitAccts;
    pGrantStatus->limitDnodes = pGrantMsg->limitDnodes;
    pGrantStatus->limitCpuCores = pGrantMsg->limitCpuCores;
  } else {
    if (pGrantMsg->usbDongle) {
      pGrantStatus->usbDongle = pGrantMsg->usbDongle;
    }
    pGrantStatus->officialVersion = pGrantMsg->officialVersion;
    pGrantStatus->lastReceived = curTime;

    COMPARE_SET_VAL(pGrantStatus->expireTimeSec, pGrantMsg->expireTimeSec, <);
    COMPARE_SET_VAL(pGrantStatus->limitStorage, (int64_t)(pGrantMsg->limitStorage * (int64_t)1073741824), <);
    COMPARE_SET_VAL(pGrantStatus->limitSpeed, pGrantMsg->limitSpeed, <);
    COMPARE_SET_VAL(pGrantStatus->limitTimeSeries, pGrantMsg->limitTimeSeries, <);
    COMPARE_SET_VAL(pGrantStatus->limitQueryTime, pGrantMsg->limitQueryTime, <);
    COMPARE_SET_VAL(pGrantStatus->limitDbs, pGrantMsg->limitDbs, <);
    COMPARE_SET_VAL(pGrantStatus->limitUsers, pGrantMsg->limitUsers, <);
    COMPARE_SET_VAL(pGrantStatus->limitConns, pGrantMsg->limitConns, <);
    COMPARE_SET_VAL(pGrantStatus->limitStreams, pGrantMsg->limitStreams, <);
    COMPARE_SET_VAL(pGrantStatus->limitAccts, pGrantMsg->limitAccts, <);
    COMPARE_SET_VAL(pGrantStatus->limitDnodes, pGrantMsg->limitDnodes, <);
    COMPARE_SET_VAL(pGrantStatus->limitCpuCores, pGrantMsg->limitCpuCores, <);
  }

  char *ts = grantSecondsToString(pGrantStatus->expireTimeSec);
  if (pGrantStatus->expireTimeSec > curTime) {
    uInfo("grant message received from dnode, storage:%uGB, timeseries:%" PRIu64
          ", database:%u, user:%u, expire:%s %u, "
          "curtime:%u, set to grant state",
          (uint32_t)(pGrantStatus->limitStorage / (int64_t)1073741824), pGrantStatus->limitTimeSeries,
          pGrantStatus->limitDbs, pGrantStatus->limitUsers, ts, pGrantStatus->expireTimeSec, curTime);
    pGrantStatus->expired = false;
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
  char    tmp[42] = {0};
  char    tmp1[42] = {0};

  if (pShow->numOfRows < 1) {
  #ifdef CFG_GRANTS
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    const char      *src;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (cloudGrantStatus.limitTimeSeries != GRANT_TIME_SERIES_LIMITS) {
      sprintf(tmp1, "%" PRIu64 "/%" PRIu64, cloudGrantStatus.curTimeSeries, cloudGrantStatus.limitTimeSeries);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (cloudGrantStatus.limitDbs != GRANT_DATABASE_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurDbs(pMnode), cloudGrantStatus.limitDbs);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (cloudGrantStatus.limitSTables != GRANT_STABLE_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurSTables(pMnode), cloudGrantStatus.limitSTables);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (cloudGrantStatus.limitTables != GRANT_TABLE_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurTables(pMnode), cloudGrantStatus.limitTables);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);
  #else
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    const char      *src = grantStatus.officialVersion ? "official" : "trial";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char       expire[22] = {0};
    time_t     tt = grantStatus.expireTimeSec;
    struct tm  ptm;
    taosLocalTime(&tt, &ptm);
    strftime(expire, 21, "%Y-%m-%d %H:%M:%S", &ptm);
    src = grantStatus.expireTimeSec != GRANT_EXPIRE_TIME ? expire : "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = grantStatus.expired ? " true" : " false";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

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
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitTimeSeries != GRANT_TIME_SERIES_LIMITS) {
      sprintf(tmp1, "%" PRIu64 "/%" PRIu64, grantStatus.curTimeSeries, grantStatus.limitTimeSeries);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitDbs != GRANT_DATABASE_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurDbs(pMnode), grantStatus.limitDbs);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitUsers != GRANT_USER_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurUsers(pMnode), grantStatus.limitUsers);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitAccts != GRANT_ACCT_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurAccts(pMnode), grantStatus.limitAccts);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitDnodes != GRANT_DNODE_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurDnodes(pMnode), grantStatus.limitDnodes);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    ++cols;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);
  #endif
    numOfRows++;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextGrant(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

int32_t tSerializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

#ifdef CFG_GRANTS
  // grant status
  if (tEncodeU64(&encoder, pStatus->limitTimeSeries) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitDbs) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitSTables) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitTables) < 0) return -1;
  // current value
  if (tEncodeU64(&encoder, pStatus->curTimeSeries) < 0) return -1;
  if (tEncodeU64(&encoder, pStatus->curDbs) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->curSTables) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->curTables) < 0) return -1;
#else
  // grant status
  if (tEncodeI8(&encoder, pStatus->usbDongle ? 1 : 0) < 0) return -1;
  if (tEncodeI8(&encoder, pStatus->officialVersion ? 1 : 0) < 0) return -1;
  if (tEncodeI8(&encoder, pStatus->expired ? 1 : 0) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->expireTimeSec) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->lastReceived) < 0) return -1;
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
#endif

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeGrantStatus(void *buf, int32_t bufLen, GrantStatus *pStatus) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

#ifdef CFG_GRANTS
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
  if (tDecodeU32(&decoder, &pStatus->lastReceived) < 0) return -1;
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
  #endif

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeGrantMsg(void *buf, int32_t bufLen, GrantMsg *pMsg) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

#ifdef CFG_GRANTS
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

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeGrantMsg(void *buf, int32_t bufLen, GrantMsg *pMsg) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

#ifdef CFG_GRANTS
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

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}
