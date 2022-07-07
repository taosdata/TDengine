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
// #include "mnodeTable.h"
#include "mndAcct.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndUser.h"
#include "sdb.h"
// #include "mnodePeer.h"
#include "mndSync.h"

#ifndef min
#define min(x, y) (x) < (y) ? (x) : (y)
#endif
#if 1
extern void *tsMnodeTmr;
#endif
extern SGrantObj grantObj;

static char   *grantSecondsToString(uint32_t seconds);
static void    grantCheckGrantInfo(void *, void *);
static void    grantSendMsgToMgmt(void *, void *);
static int32_t grantProcessMsgInMgmt(SRpcMsg *pMsg);
static void    grantProcessRspInDnode(SRpcMsg *rpcMsg);
static void    mndRefreshGrantCfg();
static int32_t dmGenerateGrantMsg(SGrantMsg *pGrant);
static int32_t tSerializeGrantStatus(void *buf, int32_t bufLen, SGrantStatus *pStatus);
static int32_t tDeserializeGrantStatus(void *buf, int32_t bufLen, SGrantStatus *pStatus);
static int32_t tSerializeGrantMsg(void *buf, int32_t bufLen, SGrantMsg *pMsg);
static int32_t tDeserializeGrantMsg(void *buf, int32_t bufLen, SGrantMsg *pMsg);

#if 0
// static int32_t grantGetMetaData(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
#endif
static int32_t mndRetrieveGrant(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelGetNextGrant(SMnode *pMnode, void *pIter);

static void *grantCheckTimer = NULL;
static void *grantSendTimer = NULL;
SGrantStatus grantStatus = {false,
                            false,
                            GRANT_EXPIRE_TIME,
                            GRANT_EXPIRE_TIME,
                            0,
                            (int64_t)(GRANT_STORAGE_LIMITS)*1073741824L,
                            0,
                            GRANT_WRITING_SPEED_LIMITS,
                            0,
                            GRANT_TIME_SERIES_LIMITS,
                            0,
                            GRANT_QUERY_TIME_LIMITS,
                            GRANT_DATABASE_LIMITS,
                            GRANT_USER_LIMITS,
                            GRANT_CONNECTION_LIMITS,
                            GRANT_STREAM_LIMITS,
                            GRANT_ACCT_LIMITS,
                            GRANT_DNODE_LIMITS,
                            GRANT_CPU_LIMITS};

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
  SGrantStatus grantStatusReq = {0};
  if (tDeserializeGrantStatus(pMsg->pCont, pMsg->contLen, &grantStatusReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    uWarn("failed to process grant req in dnode since %s", terrstr());
    goto _err;
  }

  // TODO: process grant status from mnode
  // ...

  // step 2: respond with grant msg
  SGrantMsg grantMsg = {0};
  dmGenerateGrantMsg(&grantMsg);
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

static void mndRefreshGrantCfg() {
  char cfgFile[PATH_MAX] = {0};
  sprintf(cfgFile, "%s/taos.cfg", configDir);
  grantActiveSystem(cfgFile);
}

static int32_t dmGenerateGrantMsg(SGrantMsg *pGrant) {
  // refresh
  mndRefreshGrantCfg();

  pGrant->usbDongle = grantObj.usbDongle;
  pGrant->updateForced = grantObj.updateForced;
  pGrant->officialVersion = grantObj.officialVersion;
  pGrant->expireTimeSec = grantObj.expireTimeSec;
  pGrant->limitStorage = grantObj.limitStorage;
  pGrant->limitSpeed = grantObj.limitSpeed;
  pGrant->limitTimeSeries = grantObj.limitTimeSeries;
  pGrant->limitQueryTime = grantObj.limitQueryTime;
  pGrant->limitDbs = grantObj.limitDbs;
  pGrant->limitUsers = grantObj.limitUsers;
  pGrant->limitConns = grantObj.limitConns;
  pGrant->limitStreams = grantObj.limitStreams;
  pGrant->limitAccts = grantObj.limitAccts;
  pGrant->limitDnodes = grantObj.limitDnodes;
  pGrant->limitCpuCores = grantObj.limitCpuCores;
  pGrant->reserveKey1 = grantObj.reserveKey1;
  pGrant->reserveKey2 = grantObj.reserveKey2;

  char *ts = grantSecondsToString(grantObj.expireTimeSec);
  uDebug("generate grant message: storage:%uGB, timeseries:%u, database:%u, users:%u, expire:%s %u",
         grantObj.limitStorage, grantObj.limitTimeSeries, grantObj.limitDbs, grantObj.limitUsers, ts,
         grantObj.expireTimeSec);
  taosMemoryFreeClear(ts);

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
#if 0
  if (pDnodeEp->isMnode) {
    // no need to send grant status, but should check the local grantObj
    // TODO: check the local grantObj and update the grantStatus
    // ...
    uInfo("dnode id:%d, %s:%" PRIu16 " is mnode", pDnodeEp->id, pDnodeEp->ep.fqdn, pDnodeEp->ep.port);
    return TSDB_CODE_SUCCESS;
  }
#endif

  // step 1: send grant status to dnode
  int32_t contLen = tSerializeGrantStatus(NULL, 0, &grantStatus);
  void   *pCont = rpcMallocCont(contLen);
  if (!pCont) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    uWarn("failed to generate grant status msg since %s", terrstr());
    return TSDB_CODE_FAILED;
  }

  tSerializeGrantStatus(pCont, contLen, &grantStatus);

  SRpcMsg rpcMsg = {.pCont = pCont, .contLen = contLen, .msgType = TDMT_MND_GRANT};
  SRpcMsg rpcRsp = {0};

  uInfo("send grant status msg to dnode:%d %s:%" PRIu16, pDnodeEp->id, pDnodeEp->ep.fqdn, pDnodeEp->ep.port);

  SEpSet epSet = {.numOfEps = 1};
  strncpy(epSet.eps[0].fqdn, pDnodeEp->ep.fqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = pDnodeEp->ep.port;

  rpcSendRecv(pMnode->msgCb.clientRpc, &epSet, &rpcMsg, &rpcRsp);

  // step 2: process response from dnode
  if (!rpcRsp.pCont || rpcRsp.contLen <= 0 || rpcRsp.code != 0) {
    uError("failed to process the grant rsp from dnode since empty content");
    goto _err;
  }

  SGrantMsg grantMsgRsp = {0};
  if (tDeserializeGrantMsg(rpcRsp.pCont, rpcRsp.contLen, &grantMsgRsp) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    uWarn("failed to process the grant rsp from dnode since %s", terrstr());
    goto _err;
  }

  uInfo("succeed to receive grant msg from dnode");
  // TODO: process the grant rsp from dnode
  // ...

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

  mndGetDnodeData(pMnode, pDnodeEps);

  for (int32_t i = 0; i < taosArrayGetSize(pDnodeEps); ++i) {
    SDnodeEp *pDnodeEp = (SDnodeEp *)taosArrayGet(pDnodeEps, i);
    uInfo("dnode id:%d, is mnode:%" PRIi8 ", %s:%" PRIu16, pDnodeEp->id, pDnodeEp->isMnode, pDnodeEp->ep.fqdn,
          pDnodeEp->ep.port);

    mndSendGrantStatusToDnode(pMnode, pDnodeEp);
  }

  taosArrayDestroy(pDnodeEps);
  return 0;
}

int32_t mndInitGrant(SMnode *pMnode) {
  mndRefreshGrantCfg();

  mndSetMsgHandle(pMnode, TDMT_MND_GRANT_HB_TIMER, mndProcessGrantHB);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndRetrieveGrant);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_GRANTS, mndCancelGetNextGrant);
#if 0
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_GRANTS, grantGetMetaData);
  mnodeAddPeerMsgHandle(TSDB_MSG_TYPE_DM_GRANT, grantProcessMsgInMgmt);
  dnodeAddClientRspHandle(TSDB_MSG_TYPE_DM_GRANT_RSP, grantProcessRspInDnode);
  taosTmrReset(grantSendMsgToMgmt, 500, NULL, tsMnodeTmr, &grantSendTimer);
#endif

  uDebug("grant data is initialized");
  return TSDB_CODE_SUCCESS;
}

void mndCleanupGrant() {
  taosTmrStopA(&grantCheckTimer);
  taosTmrStopA(&grantSendTimer);
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
  struct tm *ptm = taosLocalTime(&sec, NULL);
  strftime(ts, 64, "%Y-%m-%d %H:%M:%S", ptm);
  return ts;
}

static uint32_t grantGetClusterCreateTime() {
  void      *pIter = NULL;
  SDnodeObj *pDnode = NULL;
  SAcctObj  *pAcct = NULL;
  SUserObj  *pUser = NULL;
  SDbObj    *pDb = NULL;

  int64_t createTime = (int64_t)taosGetTimestampMs();
#if 0
  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    createTime = createTime < pDnode->createdTime ? createTime : pDnode->createdTime;
    mnodeDecDnodeRef(pDnode);
  }
  pIter = NULL;

  while (1) {
    pIter = mnodeGetNextAcct(pIter, &pAcct);
    if (pAcct == NULL) break;
    createTime = createTime < pAcct->createdTime ? createTime : pAcct->createdTime;
    mnodeDecAcctRef(pAcct);
  }
  pIter = NULL;

  while (1) {
    pIter = mnodeGetNextUser(pIter, &pUser);
    if (pUser == NULL) break;
    createTime = createTime < pUser->createdTime ? createTime : pUser->createdTime;
    mnodeDecUserRef(pUser);
  }
  pIter = NULL;

  while (1) {
    pIter = mnodeGetNextDb(pIter, &pDb);
    if (pDb == NULL) break;
    createTime = createTime < pDb->createdTime ? createTime : pDb->createdTime;
    mnodeDecDbRef(pDb);
  }
#endif
  return (uint32_t)(createTime / 1000);
}

static uint32_t grantGetClusterCurSpeed() { return 0; }

uint32_t grantGetClusterCurTimeSeries() {
  void    *pIter = NULL;
  uint32_t numOfPoints = 0;
#if 0
  SCTableObj *pTable = NULL;

  while (1) {
    pIter = mnodeGetNextChildTable(pIter, &pTable);
    if (pTable == NULL) break;
    if (pTable->superTable != NULL) {
      numOfPoints += (pTable->superTable->numOfColumns - 1);
    } else {
      numOfPoints += (pTable->numOfColumns - 1);
    }
    mnodeDecTableRef(pTable);
  }
#endif
  return numOfPoints;
}

static uint32_t grantGetClusterCurQueryTime() { return 0; }

static uint32_t grantGetClusterCurDbs() {
  void    *pIter = NULL;
  SDbObj  *pDb = NULL;
  uint32_t numOfDbs = 0;
#if 0
  while (1) {
    pIter = mnodeGetNextDb(pIter, &pDb);
    if (pDb == NULL) break;
    if (strcmp(pDb->name, tsMonitorDbName) != 0) numOfDbs++;
    mnodeDecDbRef(pDb);
  }
#endif
  return numOfDbs;
}

static uint32_t grantGetClusterCurUsers() {
  void     *pIter = NULL;
  SUserObj *pUser = NULL;
  uint32_t  numOfUsers = 0;
#if 0
  while (1) {
    pIter = mnodeGetNextUser(pIter, &pUser);
    if (pUser == NULL) break;
    if (strcmp(pUser->user, "monitor") == 0) continue;
    if (pUser->user[0] == '_') continue;
    numOfUsers++;
    mnodeDecUserRef(pUser);
  }
#endif
  return numOfUsers;
}

UNUSED_FUNC
static uint32_t grantGetClusterCurConns() { return 0; }

UNUSED_FUNC
static uint32_t grantGetClusterCurStreams() { return 0; }

static uint32_t grantGetClusterCurAccts() {
  void     *pIter = NULL;
  SAcctObj *pAcct = NULL;
  uint32_t  numOfAccts = 0;
#if 0
  while (1) {
    pIter = mnodeGetNextAcct(pIter, &pAcct);
    if (pAcct == NULL) break;
    numOfAccts++;
    mnodeDecAcctRef(pAcct);
  }
#endif
  return numOfAccts;
}

static uint32_t grantGetClusterCurDnodes() {
  void      *pIter = NULL;
  SDnodeObj *pDnode = NULL;
  int32_t    numOfDnodes = 0;
#if 0
  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    numOfDnodes++;
    mnodeDecDnodeRef(pDnode);
  }
#endif
  return numOfDnodes;
}

UNUSED_FUNC
static uint32_t grantGetClusterCurCpuCores() {
  void      *pIter = NULL;
  SDnodeObj *pDnode = NULL;
  uint32_t   numOfCpuCores = 0;
#if 0
  while (1) {
    pIter = mnodeGetNextDnode(pIter, &pDnode);
    if (pDnode == NULL) break;
    numOfCpuCores += pDnode->numOfCores;
    mnodeDecDnodeRef(pDnode);
  }
#endif
  return numOfCpuCores;
}

static void grantResetMaster() {
  uint32_t curTime = taosGetTimestampSec();
  uint32_t clusterCreateTime = grantGetClusterCreateTime();

  grantStatus.expireTimeSec = clusterCreateTime + GRANT_DEFAULT;
  grantStatus.expireTimeSec = grantStatus.expireTimeSec > curTime ? grantStatus.expireTimeSec : curTime;
  grantStatus.expireTimeSec += GRANT_TOLERENCE;
  grantStatus.lastReceived = grantStatus.expireTimeSec;
  grantStatus.expired = false;

  grantStatus.curSpeed = grantGetClusterCurSpeed();
#if 0
  grantStatus.curTimeSeries = grantGetClusterCurTimeSeries();
#endif
  grantStatus.curQueryTime = grantGetClusterCurQueryTime();

  char *ts = grantSecondsToString(grantStatus.expireTimeSec);
  uDebug("grant expire time reset to %s %u, current timeseries %u", ts, grantStatus.expireTimeSec,
         grantStatus.curTimeSeries);
  taosMemoryFree(ts);

#if 0
  taosTmrReset(grantCheckGrantInfo, GRANT_CHECK_INTERVAL * 1000, NULL, tsMnodeTmr, &grantCheckTimer);
#endif
}

void grantReset(EGrantType grant, uint64_t value) {
  switch (grant) {
    case TSDB_GRANT_ALL:
      grantResetMaster();
      break;
    case TSDB_GRANT_STORAGE:
      grantStatus.curStorage = value;
      break;
    default:
      break;
  }
}

static void grantAddTimeSeries(uint32_t timeSeriesNum) {
  atomic_add_fetch_32(&grantStatus.curTimeSeries, timeSeriesNum);
}

static void grantRestoreTimeSeries(uint32_t timeSeriesNum) {
  if (grantStatus.curTimeSeries < timeSeriesNum) {
    grantStatus.curTimeSeries = 0;
  } else {
    atomic_sub_fetch_32(&grantStatus.curTimeSeries, timeSeriesNum);
  }
}

void grantAdd(EGrantType grant, uint64_t value) {
  switch (grant) {
    case TSDB_GRANT_TIMESERIES:
      grantAddTimeSeries((uint32_t)value);
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
      grantRestoreTimeSeries((uint32_t)value);
      break;
    case TSDB_GRANT_STORAGE:
      grantStatus.curStorage = value;
      break;
    default:
      break;
  }
}

static int32_t grantCheckExpired() {
  if (grantStatus.expired) {
    return TSDB_CODE_GRANT_EXPIRED;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t grantCheckUsers() {
  if (grantCheckExpired()) {
    uError("grant failed to create user, reason:grant expired");
    return TSDB_CODE_GRANT_EXPIRED;
  }

  uint32_t numOfTotalUsers = grantGetClusterCurUsers();
  if (grantStatus.limitUsers == GRANT_USER_LIMITS || numOfTotalUsers < grantStatus.limitUsers) {
    return 0;
  } else {
    uError("grant failed to create user, exist:%d, reason:grant user limited", numOfTotalUsers);
    return TSDB_CODE_GRANT_USER_LIMITED;
  }
}

static int32_t grantCheckDatabases() {
  if (grantCheckExpired()) {
    uError("grant failed to create db, reason:grant expired");
    return TSDB_CODE_GRANT_EXPIRED;
  }

  uint32_t numOfTotalDbs = grantGetClusterCurDbs();
  if (grantStatus.limitDbs == GRANT_DATABASE_LIMITS || numOfTotalDbs < grantStatus.limitDbs) {
    return 0;
  } else {
    uError("grant failed to create db, exist:%d, reason:grant database limited", numOfTotalDbs);
    return TSDB_CODE_GRANT_DB_LIMITED;
  }
}

static int32_t grantCheckTimeSeries() {
  if (grantCheckExpired()) {
    uError("grant failed to create table, reason:grant expired");
    return TSDB_CODE_GRANT_EXPIRED;
  }

  if (grantStatus.limitTimeSeries == GRANT_TIME_SERIES_LIMITS ||
      grantStatus.curTimeSeries <= grantStatus.limitTimeSeries) {
    return 0;
  } else {
    uError("grant failed to create table, exist:%d, reason:grant timeseries limited", grantStatus.curTimeSeries);
    return TSDB_CODE_GRANT_TIMESERIES_LIMITED;
  }
}

static int32_t grantCheckAccts() {
  int32_t code = grantCheckUsers();
  if (code != 0) {
    return code;
  }

  uint32_t numOfTotalAccts = grantGetClusterCurAccts();
  if (grantStatus.limitAccts == GRANT_ACCT_LIMITS || numOfTotalAccts < grantStatus.limitAccts) {
    return 0;
  } else {
    uError("grant failed to create account, exist:%d, reason:grant account limited", numOfTotalAccts);
    return TSDB_CODE_GRANT_ACCT_LIMITED;
  }
}

static int32_t grantCheckDnodes() {
  if (grantCheckExpired()) {
    uError("grant failed to create account, reason:grant expired");
    return TSDB_CODE_GRANT_EXPIRED;
  }

  uint32_t numOfTotalDnodes = grantGetClusterCurDnodes();
  if (grantStatus.limitDnodes == GRANT_DNODE_LIMITS || numOfTotalDnodes < grantStatus.limitDnodes) {
    return 0;
  } else {
    uError("grant failed to create dnode, exist:%d, reason:grant dnode limited", numOfTotalDnodes);
    return TSDB_CODE_GRANT_DNODE_LIMITED;
  }
}

static int32_t grantCheckStorage() {
  if (grantCheckExpired()) {
    uError("failed to write data, reason:grant expired");
    return TSDB_CODE_GRANT_EXPIRED;
  }

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

int32_t grantCheck(EGrantType grant) {
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

  return TSDB_CODE_SUCCESS;
}

static void grantProcessRspInDnode(SRpcMsg *rpcMsg) {
  uDebug("grant rsp received from mnode, result:%s", tstrerror(rpcMsg->code));
#if 0
  if (rpcMsg->code != TSDB_CODE_SUCCESS && tsMnodeTmr != NULL) {
    taosTmrReset(grantSendMsgToMgmt, 3000, NULL, tsMnodeTmr, &grantSendTimer);
  }
#endif
}

static void grantSendMsgToMgmt(void *p1, void *p2) {
#if 0
  taosTmrReset(grantSendMsgToMgmt, GRANT_HEART_BEAT_MSG * 1000, NULL, tsMnodeTmr, &grantSendTimer);
#endif

  if (!grantObj.granted) return;

  SGrantMsg *pGrant = rpcMallocCont(sizeof(SGrantMsg));

  pGrant->officialVersion = htonl(grantObj.officialVersion);
  pGrant->expireTimeSec = htonl(grantObj.expireTimeSec);
  pGrant->limitStorage = htonl(grantObj.limitStorage);
  pGrant->limitSpeed = htonl(grantObj.limitSpeed);
  pGrant->limitTimeSeries = htonl(grantObj.limitTimeSeries);
  pGrant->limitQueryTime = htonl(grantObj.limitQueryTime);
  pGrant->limitDbs = htonl(grantObj.limitDbs);
  pGrant->limitUsers = htonl(grantObj.limitUsers);
  pGrant->limitConns = htonl(grantObj.limitConns);
  pGrant->limitStreams = htonl(grantObj.limitStreams);
  pGrant->limitAccts = htonl(grantObj.limitAccts);
  pGrant->limitDnodes = htonl(grantObj.limitDnodes);
  pGrant->limitCpuCores = htonl(grantObj.limitCpuCores);
  pGrant->reserveKey1 = htonl(grantObj.reserveKey1);
  pGrant->reserveKey2 = htonl(grantObj.reserveKey2);

  char *ts = grantSecondsToString(grantObj.expireTimeSec);
  uDebug("grant send message to mnode, storage:%uGB, timeseries:%u, database:%u, users:%u, expire:%s %u",
         grantObj.limitStorage, grantObj.limitTimeSeries, grantObj.limitDbs, grantObj.limitUsers, ts,
         grantObj.expireTimeSec);
  taosMemoryFree(ts);

  SRpcMsg rpcMsg = {.pCont = pGrant, .contLen = sizeof(SGrantMsg), .msgType = TDMT_MND_GRANT};

  // SRpcEpSet epSet = {0};
  // dnodeGetEpSetForPeer(&epSet);
  // dnodeSendMsgToDnode(&epSet, &rpcMsg);
}

static int32_t grantProcessMsgInMgmt(SRpcMsg *pMsg) {
  SGrantMsg *pGrant = pMsg->pCont;

#ifndef GRANT_MIRROR_VERSION
  grantStatus.officialVersion = htonl(pGrant->officialVersion);

  uint32_t curTime = taosGetTimestampSec();
  grantStatus.lastReceived = curTime;
  grantStatus.expireTimeSec = htonl(pGrant->expireTimeSec);
  grantStatus.limitStorage = (int64_t)(htonl(pGrant->limitStorage) * (int64_t)1073741824);
  grantStatus.limitSpeed = htonl(pGrant->limitSpeed);
  grantStatus.limitTimeSeries = htonl(pGrant->limitTimeSeries);
  grantStatus.limitQueryTime = htonl(pGrant->limitQueryTime);
  grantStatus.limitDbs = htonl(pGrant->limitDbs);
  grantStatus.limitUsers = htonl(pGrant->limitUsers);
  grantStatus.limitConns = htonl(pGrant->limitConns);
  grantStatus.limitStreams = htonl(pGrant->limitStreams);
  grantStatus.limitAccts = htonl(pGrant->limitAccts);
  grantStatus.limitDnodes = htonl(pGrant->limitDnodes);
  grantStatus.limitCpuCores = htonl(pGrant->limitCpuCores);
#else
  grantStatus.officialVersion = htonl(pGrant->officialVersion);

  uint32_t curTime = taosGetTimestampSec();
  grantStatus.lastReceived = curTime;
  grantStatus.expireTimeSec = min(grantStatus.expireTimeSec, htonl(pGrant->expireTimeSec));
  grantStatus.limitStorage =
      min(grantStatus.limitStorage, (int64_t)(htonl(pGrant->limitStorage) * (int64_t)1073741824));
  grantStatus.limitSpeed = min(grantStatus.limitSpeed, htonl(pGrant->limitSpeed));
  grantStatus.limitTimeSeries = min(grantStatus.limitTimeSeries, htonl(pGrant->limitTimeSeries));
  grantStatus.limitQueryTime = min(grantStatus.limitQueryTime, htonl(pGrant->limitQueryTime));
  grantStatus.limitDbs = min(grantStatus.limitDbs, htonl(pGrant->limitDbs));
  grantStatus.limitUsers = min(grantStatus.limitUsers, htonl(pGrant->limitUsers));
  grantStatus.limitConns = min(grantStatus.limitConns, htonl(pGrant->limitConns));
  grantStatus.limitStreams = min(grantStatus.limitStreams, htonl(pGrant->limitStreams));
  grantStatus.limitAccts = min(grantStatus.limitAccts, htonl(pGrant->limitAccts));
  grantStatus.limitDnodes = min(grantStatus.limitDnodes, htonl(pGrant->limitDnodes));
  grantStatus.limitCpuCores = min(grantStatus.limitCpuCores, htonl(pGrant->limitCpuCores));
#endif

  char *ts = grantSecondsToString(grantStatus.expireTimeSec);

  if (grantStatus.expireTimeSec > curTime) {
    uDebug(
        "grant message received from dnode, storage:%uGB, timeseries:%u, database:%u, user:%u, expire:%s %u, "
        "curtime:%u, set to grant state",
        htonl(pGrant->limitStorage), grantStatus.limitTimeSeries, grantStatus.limitDbs, grantStatus.limitUsers, ts,
        grantStatus.expireTimeSec, curTime);
    grantStatus.expired = false;
  } else {
    uError("grant cluster expired at %s %u, curtime: %u, set to un-grant state", ts, grantStatus.expireTimeSec,
           curTime);
    grantStatus.expired = true;
  }

  taosMemoryFree(ts);

  return TSDB_CODE_SUCCESS;
}

static void grantCheckGrantInfo(void *p1, void *p2) {
#if 0
  taosTmrReset(grantCheckGrantInfo, GRANT_CHECK_INTERVAL * 1000, NULL, tsMnodeTmr, &grantCheckTimer);
  grantStatus.expired = false;
  if (mndIsMaster()) {

    /*
     * When all nodes are online, the grant time is judged
     */
    void *     pIter = NULL;
    SDnodeObj *pDnode = NULL;
    while (1) {
      pIter = mnodeGetNextDnode(pIter, &pDnode);
      if (pDnode == NULL) break;

      if (pDnode->status == 0) {  // TSDB_DN_STATUS_OFFLINE
        mnodeDecDnodeRef(pDnode);
        mnodeCancelGetNextDnode(pIter);
        return;
      }

      mnodeDecDnodeRef(pDnode);
    }

    uint32_t curTime = taosGetTimestampSec();
    if (curTime > grantStatus.lastReceived && curTime - grantStatus.lastReceived > GRANT_TOLERENCE) {
      char *ts1 = grantSecondsToString(grantStatus.expireTimeSec);
      char *ts2 = grantSecondsToString(grantStatus.lastReceived);
      uError("grant message not received beyond %d seconds, set to un-grant state, expire at %s, last received at %s, ", GRANT_TOLERENCE, ts1, ts2);
      taosMemoryFree(ts1);
      taosMemoryFree(ts2);
      grantStatus.expired = true;
    }
  }
#endif
  /*
   * For debug usage
   */
  if (0) {
    uint32_t curTime = taosGetTimestampSec();
    char    *ts1 = grantSecondsToString(grantStatus.expireTimeSec);
    char    *ts2 = grantSecondsToString(grantStatus.lastReceived);
    uDebug("grant expire at %s, last received at %s, expired %d seconds, tolerance %d seconds", ts1, ts2,
           curTime - grantStatus.lastReceived, GRANT_TOLERENCE);
    taosMemoryFree(ts1);
    taosMemoryFree(ts2);
  }
}

static int32_t mndRetrieveGrant(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  int32_t numOfRows = 0;
  char   *pWrite;
  int32_t cols = 0;
  char    tmp[32];
  char    tmp1[32];

  if (pShow->numOfRows < 1) {
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    const char      *src = grantStatus.officialVersion ? "official" : "trial";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    char       expire[22] = {0};
    time_t     tt = grantStatus.expireTimeSec;
    struct tm *ptm = taosLocalTime(&tt, NULL);
    strftime(expire, 21, "%Y-%m-%d %H:%M:%S", ptm);
    src = grantStatus.expireTimeSec != GRANT_EXPIRE_TIME ? expire : "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = grantStatus.expired ? "true" : "false";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if ((uint32_t)(grantStatus.limitStorage / (int64_t)1073741824) != GRANT_STORAGE_LIMITS) {
      sprintf(tmp1, "%u/%u", (uint32_t)(grantStatus.curStorage / (int64_t)1073741824),
              (uint32_t)(grantStatus.limitStorage / (int64_t)1073741824));
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitTimeSeries != GRANT_TIME_SERIES_LIMITS) {
      sprintf(tmp1, "%u/%u", grantStatus.curTimeSeries, grantStatus.limitTimeSeries);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitDbs != GRANT_DATABASE_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurDbs(), grantStatus.limitDbs);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitUsers != GRANT_USER_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurUsers(), grantStatus.limitUsers);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitAccts != GRANT_ACCT_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurAccts(), grantStatus.limitAccts);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    if (grantStatus.limitDnodes != GRANT_DNODE_LIMITS) {
      sprintf(tmp1, "%u/%u", grantGetClusterCurDnodes(), grantStatus.limitDnodes);
      src = tmp1;
    } else {
      src = "unlimited";
    }
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    cols++;
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    src = "unlimited";
    STR_WITH_SIZE_TO_VARSTR(tmp, src, strlen(src));
    colDataAppend(pColInfo, numOfRows, tmp, false);

    numOfRows++;
  }

  pShow->numOfRows += numOfRows;
  return numOfRows;
}

static void mndCancelGetNextGrant(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}

int32_t tSerializeGrantStatus(void *buf, int32_t bufLen, SGrantStatus *pStatus) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  // grant status
  if (tEncodeI8(&encoder, pStatus->usbDongle ? 1 : 0) < 0) return -1;
  if (tEncodeI8(&encoder, pStatus->officialVersion ? 1 : 0) < 0) return -1;
  if (tEncodeI8(&encoder, pStatus->expired ? 1 : 0) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->expireTimeSec) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->lastReceived) < 0) return -1;
  if (tEncodeU64(&encoder, pStatus->curStorage) < 0) return -1;
  if (tEncodeU64(&encoder, pStatus->limitStorage) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->curSpeed) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitSpeed) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->curTimeSeries) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitTimeSeries) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->curQueryTime) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitQueryTime) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitDbs) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitUsers) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitConns) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitStreams) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitAccts) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitDnodes) < 0) return -1;
  if (tEncodeU32(&encoder, pStatus->limitCpuCores) < 0) return -1;

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeGrantStatus(void *buf, int32_t bufLen, SGrantStatus *pStatus) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  // grant status
  if (tDecodeI8(&decoder, (int8_t *)&pStatus->usbDongle) < 0) return -1;
  if (tDecodeI8(&decoder, (int8_t *)&pStatus->officialVersion) < 0) return -1;
  if (tDecodeI8(&decoder, (int8_t *)&pStatus->expired) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->expireTimeSec) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->lastReceived) < 0) return -1;
  if (tDecodeU64(&decoder, &pStatus->curStorage) < 0) return -1;
  if (tDecodeU64(&decoder, &pStatus->limitStorage) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->curSpeed) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitSpeed) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->curTimeSeries) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitTimeSeries) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->curQueryTime) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitQueryTime) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitDbs) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitUsers) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitConns) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitStreams) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitAccts) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitDnodes) < 0) return -1;
  if (tDecodeU32(&decoder, &pStatus->limitCpuCores) < 0) return -1;

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}

int32_t tSerializeGrantMsg(void *buf, int32_t bufLen, SGrantMsg *pMsg) {
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  if (tStartEncode(&encoder) < 0) return -1;

  // grant msg
  if (tEncodeI8(&encoder, pMsg->updateForced ? 1 : 0) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->usbDongle) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->officialVersion) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->expireTimeSec) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitStorage) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitSpeed) < 0) return -1;
  if (tEncodeU32(&encoder, pMsg->limitTimeSeries) < 0) return -1;
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

  tEndEncode(&encoder);

  int32_t tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

int32_t tDeserializeGrantMsg(void *buf, int32_t bufLen, SGrantMsg *pMsg) {
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  if (tStartDecode(&decoder) < 0) return -1;

  // grant msg
  if (tDecodeI8(&decoder, (int8_t *)&pMsg->updateForced) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->usbDongle) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->officialVersion) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->expireTimeSec) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitStorage) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitSpeed) < 0) return -1;
  if (tDecodeU32(&decoder, &pMsg->limitTimeSeries) < 0) return -1;
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

  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  return 0;
}
