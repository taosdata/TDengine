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
#include "os.h"
#include "tulog.h"
#include "ttime.h"
#include "ttimer.h"
#include "trpc.h"
#include "tutil.h"
#include "tgrant.h"
#include "tglobal.h"
#include "machine.h"
#include "mnode.h"
#include "mgmtDef.h"
#include "mgmtDServer.h"
#include "mgmtMnode.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "dnodeMClient.h"

#define min(x, y) (x)<(y)?(x):(y)

extern void *tsMgmtTmr;
extern void *tsDnodeSdb;
extern void *tsUserSdb;
extern void *tsAcctSdb;
extern void *tsDbSdb;
extern void *tsChildTableSdb;
extern SGrantObj grantObj;

static char   *grantSecondsToString(uint32_t seconds);
static void    grantCheckGrantInfo();
static void    grantSendMsgToMgmt();
static void    grantProcessMsgInMgmt(SRpcMsg *pMsg);
static int32_t grantGetMetaData(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t grantRetrieveData(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static void *grantCheckTimer = NULL;
static void *grantSendTimer = NULL;
static SGrantStatus grantStatus = {
  false,
  false,
  GRANT_EXPIRE_TIME,
  GRANT_EXPIRE_TIME,
  0,
  (int64_t)(GRANT_STORAGE_LIMITS)* 1073741824L,
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
  GRANT_CPU_LIMITS
};

int32_t grantInit() {
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_GRANTS, grantGetMetaData);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_GRANTS, grantRetrieveData);
  mgmtAddDServerMsgHandle(TSDB_MSG_TYPE_DM_GRANT, grantProcessMsgInMgmt);
  taosTmrReset(grantSendMsgToMgmt, GRANT_CHECK_INTERVAL * 1000, NULL, tsMgmtTmr, &grantSendTimer);

  uTrace("grant data is initialized");
  return TSDB_CODE_SUCCESS;
}

void grantCleanUp() {
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
  char *ts = calloc(64, 1);
  time_t sec = seconds;
  struct tm * ptm = localtime(&sec);
  strftime(ts, 64, "%Y-%m-%d %H:%M:%S", ptm);
  return ts;
}
static uint32_t grantGetCulsterCreateTime() {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;
  SAcctObj * pAcct = NULL;
  SUserObj * pUser = NULL;
  SDbObj *   pDb = NULL;

  int64_t createTime = (int64_t)taosGetTimestampMs();
  while (1) {
    pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;
    createTime = createTime < pDnode->createdTime ? createTime : pDnode->createdTime;
  }

  while (1) {
    pNode = sdbFetchRow(tsAcctSdb, pNode, (void **)&pAcct);
    if (pAcct == NULL) break;
    createTime = createTime < pAcct->createdTime ? createTime : pAcct->createdTime;
  }

  while (1) {
    pNode = sdbFetchRow(tsUserSdb, pNode, (void **)&pUser);
    if (pUser == NULL) break;
    createTime = createTime < pUser->createdTime ? createTime : pUser->createdTime;
  }

  while (1) {
    pNode = sdbFetchRow(tsDbSdb, pNode, (void **)&pDb);
    if (pDb == NULL) break;
    createTime = createTime < pDb->createdTime ? createTime : pDb->createdTime;
  }

  return (uint32_t)(createTime / 1000);
}

static uint32_t grantGetCulsterCurSpeed() { return 0; }

static uint32_t grantGetCulsterCurTimeSeries() {
  void *          pNode = NULL;
  SChildTableObj *pTable = NULL;
  uint32_t        numOfPoints = 0;

  while (1) {
    pNode = sdbFetchRow(tsChildTableSdb, pNode, (void **)&pTable);
    if (pTable == NULL) break;
    if (pTable->superTable != NULL) {
      numOfPoints += (pTable->superTable->numOfColumns - 1);
    } else {
      numOfPoints += (pTable->numOfColumns - 1);
    }
  }

  return numOfPoints;
}

static uint32_t grantGetCulsterCurQueryTime() { return 0; }

static uint32_t grantGetCulsterCurDbs() {
  void *   pNode = NULL;
  SDbObj * pDb = NULL;
  uint32_t numOfDbs = 0;

  while (1) {
    pNode = sdbFetchRow(tsDbSdb, pNode, (void **)&pDb);
    if (pDb == NULL) break;
    if (strcmp(pDb->name, tsMonitorDbName) != 0) numOfDbs++;
  }

  return numOfDbs;
}

static uint32_t grantGetCulsterCurUsers() {
  void *    pNode = NULL;
  SUserObj *pUser = NULL;
  uint32_t  numOfUsers = 0;

  while (1) {
    pNode = sdbFetchRow(tsUserSdb, pNode, (void **)&pUser);
    if (pUser == NULL) break;
    if (strcmp(pUser->user, "monitor") == 0) continue;
    if (pUser->user[0] == '_') continue;
    numOfUsers++;
  }

  return numOfUsers;
}

UNUSED_FUNC
static uint32_t grantGetCulsterCurConns() { return 0; }

UNUSED_FUNC
static uint32_t grantGetCulsterCurStreams() { return 0; }

static uint32_t grantGetCulsterCurAccts() {
  void *    pNode = NULL;
  SAcctObj *pAcct = NULL;
  uint32_t  numOfAccts = 0;

  while (1) {
    pNode = sdbFetchRow(tsAcctSdb, pNode, (void **)&pAcct);
    if (pAcct == NULL) break;
    numOfAccts++;
  }

  return numOfAccts;
}

static uint32_t grantGetCulsterCurDnodes() {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;
  int32_t    numOfDnodes = 0;

  while (1) {
    pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;
    numOfDnodes++;
  }

  return numOfDnodes;
}

UNUSED_FUNC
static uint32_t grantGetCulsterCurCpuCores() {
  void *     pNode = NULL;
  SDnodeObj *pDnode = NULL;
  uint32_t   numOfCpuCores = 0;

  while (1) {
    pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **)&pDnode);
    if (pDnode == NULL) break;
    numOfCpuCores += pDnode->numOfCores;
  }

  return numOfCpuCores;
}

static void grantResetMaster() {
  uint32_t curTime = taosGetTimestampSec();
  uint32_t clusterCreateTime = grantGetCulsterCreateTime();

  grantStatus.expireTimeSec = clusterCreateTime + GRANT_DEFAULT;
  grantStatus.expireTimeSec = grantStatus.expireTimeSec > curTime ? grantStatus.expireTimeSec : curTime;
  grantStatus.lastReceived = grantStatus.expireTimeSec;
  grantStatus.expired = false;

  grantStatus.curSpeed = grantGetCulsterCurSpeed();
  grantStatus.curTimeSeries = grantGetCulsterCurTimeSeries();
  grantStatus.curQueryTime = grantGetCulsterCurQueryTime();

  char *ts = grantSecondsToString(grantStatus.expireTimeSec);
  uPrint("grant expire time reset to %s %u, current timeseries %u", ts, grantStatus.expireTimeSec,
         grantStatus.curTimeSeries);
  free(ts);

  taosTmrReset(grantCheckGrantInfo, GRANT_CHECK_INTERVAL * 1000, NULL, tsMgmtTmr, &grantCheckTimer);
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
  __sync_add_and_fetch(&grantStatus.curTimeSeries, timeSeriesNum);
}

static void grantRestoreTimeSeries(uint32_t timeSeriesNum) {
  if (grantStatus.curTimeSeries < timeSeriesNum) {
    grantStatus.curTimeSeries = 0;
  } else {
    __sync_add_and_fetch(&grantStatus.curTimeSeries, -timeSeriesNum);
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

  uint32_t numOfTotalUsers = grantGetCulsterCurUsers();
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

  uint32_t numOfTotalDbs = grantGetCulsterCurDbs();
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

  uint32_t numOfTotalAccts = grantGetCulsterCurAccts();
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

  uint32_t numOfTotalDnodes = grantGetCulsterCurDnodes();
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
  }
  else {
    uError("grant storage in-available, used:%lld, grant:%lld, reason:grant storage limited", grantStatus.curStorage, grantStatus.limitStorage);
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

static void grantSendMsgToMgmt() {
  taosTmrReset(grantSendMsgToMgmt, GRANT_HEART_BEAT_MSG * 1000, NULL, tsMgmtTmr, &grantSendTimer);

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
  uTrace(
      "grant send message to mgmt, storage limits:%uGB, timeseries limits:%u, database limits:%u, user limits:%u, "
      "expire: %s %u",
      grantObj.limitStorage, grantObj.limitTimeSeries, grantObj.limitDbs, grantObj.limitUsers, ts,
      grantObj.expireTimeSec);
  free(ts);

  SRpcMsg rpcMsg = {
    .pCont   = pGrant,
    .contLen = sizeof(SGrantMsg),
    .msgType = TSDB_MSG_TYPE_DM_GRANT
  };
  dnodeSendMsgToMnode(&rpcMsg);
}

static void grantProcessMsgInMgmt(SRpcMsg *pMsg)
{  
  SGrantMsg  *pGrant = pMsg->pCont;

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
  grantStatus.limitStorage = min(grantStatus.limitStorage, (int64_t)(htonl(pGrant->limitStorage) * (int64_t)1073741824));
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
    uTrace("grant message received, storage limits:%uGB, timeseries limits:%u, database limits:%u, user limits:%u, expire: %s %u, curtime: %u, set to grant state"
      , htonl(pGrant->limitStorage), grantStatus.limitTimeSeries, grantStatus.limitDbs, grantStatus.limitUsers, ts, grantStatus.expireTimeSec, curTime);
    grantStatus.expired = false;
  }
  else {
    uError("grant cluster expired at %s %u, curtime: %u, set to un-grant state", ts, grantStatus.expireTimeSec, curTime);
    grantStatus.expired = true;
  }

  free(ts);

  SRpcMsg rpcMsg = {0};
  rpcMsg.code = TSDB_CODE_SUCCESS;
  rpcMsg.handle = pMsg->handle;
  rpcSendResponse(&rpcMsg);
}

static void grantCheckGrantInfo() {
  taosTmrReset(grantCheckGrantInfo, GRANT_CHECK_INTERVAL * 1000, NULL, tsMgmtTmr, &grantCheckTimer);
  grantStatus.expired = false;

  if (sdbIsMaster()) {

    /*
     * When all nodes are online, the grant time is judged
     */
    void *     pNode = NULL;
    SDnodeObj *pDnode = NULL;
    while (1) {
      pNode = sdbFetchRow(tsDnodeSdb, pNode, (void **)&pDnode);
      if (pDnode == NULL) break;

      if (pDnode->status == 0) { //TSDB_DN_STATUS_OFFLINE
        return;
      };
    }

    uint32_t curTime = taosGetTimestampSec();
    if (curTime > grantStatus.lastReceived && curTime - grantStatus.lastReceived > GRANT_TOLERENCE) {
      char *ts1 = grantSecondsToString(grantStatus.expireTimeSec);
      char *ts2 = grantSecondsToString(grantStatus.lastReceived);
      uError("grant message not received beyond %d seconds, set to un-grant state, expire at %s, last received at %s, ", GRANT_TOLERENCE, ts1, ts2);
      free(ts1);
      free(ts2);
      grantStatus.expired = true;
    }
  }

  /*
   * For debug usage
   */
  if (0) {
    uint32_t curTime = taosGetTimestampSec();
    char *ts1 = grantSecondsToString(grantStatus.expireTimeSec);
    char *ts2 = grantSecondsToString(grantStatus.lastReceived);
    uTrace("grant expire at %s, last received at %s, expired %d seconds, tolerance %d seconds", ts1, ts2, curTime - grantStatus.lastReceived, GRANT_TOLERENCE);
    free(ts1);
    free(ts2);
  }
}

static int32_t grantGetMetaData(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  grantActiveSystem(NULL);
  grantSendMsgToMgmt();
  usleep(10000);

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "version");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 19;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "expire time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 5;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "expired");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 21;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "storage(GB)");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 21;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "timeseries");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "databases");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "users");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "accounts");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "dnodes");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 11;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "connections");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 9;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "streams");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 9;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "cpu cores");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 9;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "speed(PPS)");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 9;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "querytime");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = 1;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->pNode = NULL;

  return 0;
}

int32_t grantRetrieveData(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  char *pWrite;
  int32_t   cols = 0;

  if (pShow->numOfReads < 1) {
    cols = 0;

    char       expire[20] = {0};
    time_t     tt = grantStatus.expireTimeSec;
    struct tm *ptm = localtime(&tt);
    strftime(expire, 21, "%Y-%m-%d %H:%M:%S", ptm);

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (grantStatus.officialVersion) {
      strcpy(pWrite, "official");
    } else {
      strcpy(pWrite, "trial");
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (grantStatus.expireTimeSec != GRANT_EXPIRE_TIME) {  // 2100-01-01
      strncpy(pWrite, expire, 21);
    } else {
      strcpy(pWrite, "unlimited");
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (grantStatus.expired) {  // 2100-01-01
      strcpy(pWrite, "true");
    } else {
      strcpy(pWrite, "false");
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if ((uint32_t)(grantStatus.limitStorage / (int64_t)1073741824) != GRANT_STORAGE_LIMITS) {
      sprintf(pWrite, "%u/%u", (uint32_t)(grantStatus.curStorage / (int64_t)1073741824),
              (uint32_t)(grantStatus.limitStorage / (int64_t)1073741824));
    } else {
      strcpy(pWrite, "unlimited");
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (grantStatus.limitTimeSeries != GRANT_TIME_SERIES_LIMITS) {
      sprintf(pWrite, "%u/%u", grantStatus.curTimeSeries, grantStatus.limitTimeSeries);
    } else {
      strcpy(pWrite, "unlimited");
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (grantStatus.limitDbs != GRANT_DATABASE_LIMITS) {
      sprintf(pWrite, "%u/%u", grantGetCulsterCurDbs(), grantStatus.limitDbs);
    } else {
      strcpy(pWrite, "unlimited");
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (grantStatus.limitUsers != GRANT_USER_LIMITS) {
      sprintf(pWrite, "%u/%u", grantGetCulsterCurUsers(), grantStatus.limitUsers);
    } else {
      strcpy(pWrite, "unlimited");
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (grantStatus.limitAccts != GRANT_ACCT_LIMITS) {
      sprintf(pWrite, "%u/%u", grantGetCulsterCurAccts(), grantStatus.limitAccts);
    } else {
      strcpy(pWrite, "unlimited");
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (grantStatus.limitDnodes != GRANT_DNODE_LIMITS) {
      sprintf(pWrite, "%u/%u", grantGetCulsterCurDnodes(), grantStatus.limitDnodes);
    } else {
      strcpy(pWrite, "unlimited");
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    // if (grantStatus.limitConns != GRANT_CONNECTION_LIMITS) {
    //  sprintf(pWrite, "%u/%u", grantGetCulsterCurConns(), grantStatus.limitConns);
    //}
    // else {
    strcpy(pWrite, "unlimited");
    //}
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    // if (grantStatus.limitStreams != GRANT_STREAM_LIMITS) {
    //  sprintf(pWrite, "%u/%u", grantGetCulsterCurStreams(), grantStatus.limitStreams);
    //}
    // else {
    strcpy(pWrite, "unlimited");
    //}
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    // if (grantStatus.limitCpuCores != GRANT_CPU_LIMITS) {
    //  sprintf(pWrite, "%u/%u", grantGetCulsterCurCpuCores(), grantStatus.limitCpuCores);
    //}
    // else {
    strcpy(pWrite, "unlimited");
    //}
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    // if (grantStatus.limitSpeed != GRANT_WRITING_SPEED_LIMITS) {
    //  sprintf(pWrite, "%u/%u", grantStatus.curSpeed, grantStatus.limitSpeed);
    //}
    // else {
    strcpy(pWrite, "unlimited");
    //}
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    // if (grantStatus.limitQueryTime != GRANT_QUERY_TIME_LIMITS) {
    //  sprintf(pWrite, "%u/%u", grantStatus.curQueryTime, grantStatus.limitQueryTime);
    //}
    // else {
    strcpy(pWrite, "unlimited");
    //}
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}