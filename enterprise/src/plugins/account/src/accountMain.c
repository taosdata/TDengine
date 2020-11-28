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
#include "taosdef.h"
#include "taoserror.h"
#include "ttimer.h"
#include "tutil.h"
#include "tgrant.h"
#include "tglobal.h"
#include "tdataformat.h"
#include "monitor.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeAcct.h"
#include "mnodeDnode.h"
#include "mnodeDb.h"
#include "mnodeMnode.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"
#include "mnodeRead.h"
#include "mnodeWrite.h"

#define TSDB_MIN_USERS_PER_ACCT       2
#define TSDB_MAX_USERS_PER_ACCT       10
#define TSDB_MIN_DBS_PER_ACCT         1
#define TSDB_MAX_DBS_PER_ACCT         64
#define TSDB_MIN_TIMESERIES_PER_ACCT  10
#define TSDB_MAX_TIMESERIES_PER_ACCT  INT32_MAX
#define TSDB_MIN_CONNECTIONS_PER_ACCT 10
#define TSDB_MAX_CONNECTIONS_PER_ACCT 1024
#define TSDB_MIN_STREAMS_PER_ACCT     10
#define TSDB_MAX_STREAMS_PER_ACCT     1000
#define TSDB_MIN_SPOINTS_PER_ACCT     5000
#define TSDB_MAX_SPOINTS_PER_ACCT     10000000
#define TSDB_MIN_STORAGE_PER_ACCT     0  // 1G
#define TSDB_MAX_STORAGE_PER_ACCT     INT64_MAX
#define TSDB_MIN_QUERYTIME_PER_ACCT   3600  // 1 hour
#define TSDB_MAX_QUERYTIME_PER_ACCT   INT64_MAX

extern void *  tsAcctSdb;
extern void *  tsMnodeTmr;
static void *  tsMgmtStatisTimer = NULL;

static int64_t acctGetStatistic(SAcctObj *pAcct);
static int32_t acctProcessCreateAcctMsg(SMnodeMsg *pMsg);
static int32_t acctProcessDropAcctMsg(SMnodeMsg *pMsg);
static int32_t acctProcessAlterAcctMsg(SMnodeMsg *pMsg);
static int32_t acctGetAcctMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t acctRetrieveData(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static void acctDoStatistic(void *handle, void *tmrId) {  
  if (tsAcctSdb != NULL) {
    SAcctObj *pAcct = NULL;
    void *    pIter = NULL;
    int64_t   totalStorage = 0;

    while (1) {
      pIter = mnodeGetNextAcct(pIter, &pAcct);
      if (pAcct == NULL) break;
      totalStorage += acctGetStatistic(pAcct);
      mnodeDecAcctRef(pAcct);
    }

    grantReset(TSDB_GRANT_STORAGE, (uint64_t)totalStorage);
  }

  taosTmrReset(acctDoStatistic, tsMonitorInterval * 1000, NULL, tsMnodeTmr, &tsMgmtStatisTimer);
}

int32_t acctInit() {
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_CREATE_ACCT, acctProcessCreateAcctMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_DROP_ACCT, acctProcessDropAcctMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_ALTER_ACCT, acctProcessAlterAcctMsg);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_ACCT, acctGetAcctMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_ACCT, acctRetrieveData);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_ACCT, mnodeCancelGetNextAcct);

  taosTmrReset(acctDoStatistic, tsStatusInterval * 1000, NULL, tsMnodeTmr, &tsMgmtStatisTimer);
  
  mDebug("table:accounts, is initialized");
  return 0;
}

static int32_t acctCheckAcctParams(SAcctCfg *pCfg) {
  if (pCfg == NULL) return 0;
  if (pCfg->maxUsers <= 0) pCfg->maxUsers = TSDB_MAX_USERS_PER_ACCT;
  if (pCfg->maxUsers < TSDB_MIN_USERS_PER_ACCT) pCfg->maxUsers = TSDB_MIN_USERS_PER_ACCT;

  if (pCfg->maxDbs <= 0) pCfg->maxDbs = TSDB_MAX_DBS_PER_ACCT;
  if (pCfg->maxDbs < TSDB_MIN_DBS_PER_ACCT) pCfg->maxDbs = TSDB_MIN_DBS_PER_ACCT;

  if (pCfg->maxTimeSeries <= 0) pCfg->maxTimeSeries = TSDB_MAX_TIMESERIES_PER_ACCT;
  if (pCfg->maxTimeSeries < TSDB_MIN_TIMESERIES_PER_ACCT) pCfg->maxTimeSeries = TSDB_MIN_TIMESERIES_PER_ACCT;

  if (pCfg->maxConnections <= 0) pCfg->maxConnections = TSDB_MAX_CONNECTIONS_PER_ACCT;
  if (pCfg->maxConnections < TSDB_MIN_CONNECTIONS_PER_ACCT) pCfg->maxConnections = TSDB_MIN_CONNECTIONS_PER_ACCT;

  if (pCfg->maxStreams <= 0) pCfg->maxStreams = TSDB_MAX_STREAMS_PER_ACCT;
  if (pCfg->maxStreams < TSDB_MIN_STREAMS_PER_ACCT) pCfg->maxStreams = TSDB_MIN_STREAMS_PER_ACCT;

  if (pCfg->maxPointsPerSecond <= 0) pCfg->maxPointsPerSecond = TSDB_MAX_SPOINTS_PER_ACCT;
  if (pCfg->maxPointsPerSecond < TSDB_MIN_SPOINTS_PER_ACCT) pCfg->maxPointsPerSecond = TSDB_MIN_SPOINTS_PER_ACCT;

  if (pCfg->maxStorage <= 0) pCfg->maxStorage = TSDB_MAX_STORAGE_PER_ACCT;
  if (pCfg->maxStorage < TSDB_MIN_STORAGE_PER_ACCT) pCfg->maxStorage = TSDB_MIN_STORAGE_PER_ACCT;

  if (pCfg->maxQueryTime <= 0) pCfg->maxQueryTime = TSDB_MAX_QUERYTIME_PER_ACCT;
  if (pCfg->maxQueryTime < TSDB_MIN_QUERYTIME_PER_ACCT) pCfg->maxQueryTime = TSDB_MIN_QUERYTIME_PER_ACCT;

  if (pCfg->accessState < 0) pCfg->accessState = TSDB_VN_ALL_ACCCESS;

  if (pCfg->maxUsers < TSDB_MIN_USERS_PER_ACCT || pCfg->maxUsers > TSDB_MAX_USERS_PER_ACCT) {
    mWarn("Invalid acct parameter maxUsers: %d, range: %d--%d", pCfg->maxUsers, TSDB_MIN_USERS_PER_ACCT,
          TSDB_MAX_USERS_PER_ACCT);
    return -1;
  }

  if (pCfg->maxDbs < TSDB_MIN_DBS_PER_ACCT || pCfg->maxDbs > TSDB_MAX_DBS_PER_ACCT) {
    mWarn("Invalid acct parameter maxDbs: %d, range: %d--%d", pCfg->maxUsers, TSDB_MIN_DBS_PER_ACCT,
          TSDB_MAX_DBS_PER_ACCT);
    return -1;
  }

  if ((pCfg->maxTimeSeries < TSDB_MIN_TIMESERIES_PER_ACCT) || (pCfg->maxTimeSeries > TSDB_MAX_TIMESERIES_PER_ACCT)) {
    mWarn("Invalid acct parameter maxTimeSeries: %d, range: %d--%d", pCfg->maxTimeSeries, TSDB_MIN_TIMESERIES_PER_ACCT,
          TSDB_MAX_TIMESERIES_PER_ACCT);
    return -1;
  }

  if (pCfg->maxConnections < TSDB_MIN_CONNECTIONS_PER_ACCT || pCfg->maxConnections > TSDB_MAX_CONNECTIONS_PER_ACCT) {
    mWarn("Invalid acct parameter maxConnections: %d, range: %d--%d", pCfg->maxConnections,
          TSDB_MIN_CONNECTIONS_PER_ACCT, TSDB_MAX_CONNECTIONS_PER_ACCT);
    return -1;
  }

  if (pCfg->maxStreams < TSDB_MIN_STREAMS_PER_ACCT || pCfg->maxStreams > TSDB_MAX_STREAMS_PER_ACCT) {
    mWarn("Invalid acct parameter maxStreams: %d, range: %d--%d", pCfg->maxStreams, TSDB_MIN_STREAMS_PER_ACCT,
          TSDB_MAX_STREAMS_PER_ACCT);
    return -1;
  }

  if (pCfg->maxPointsPerSecond < TSDB_MIN_SPOINTS_PER_ACCT || pCfg->maxPointsPerSecond > TSDB_MAX_SPOINTS_PER_ACCT) {
    mWarn("Invalid acct parameter maxPointsPerSecond: %d, range: %d--%d", pCfg->maxPointsPerSecond,
          TSDB_MIN_SPOINTS_PER_ACCT, TSDB_MAX_SPOINTS_PER_ACCT);
    return -1;
  }

  if (pCfg->maxStorage < TSDB_MIN_STORAGE_PER_ACCT || pCfg->maxStorage > TSDB_MAX_STORAGE_PER_ACCT) {
    mWarn("Invalid acct parameter maxStorage: %" PRId64 ", range: %d--%" PRId64, pCfg->maxStorage,
          TSDB_MIN_STORAGE_PER_ACCT, TSDB_MAX_STORAGE_PER_ACCT);
    return -1;
  }

  if (pCfg->maxQueryTime < TSDB_MIN_QUERYTIME_PER_ACCT || pCfg->maxQueryTime > TSDB_MAX_QUERYTIME_PER_ACCT) {
    mWarn("Invalid acct parameter maxQueryTime: %" PRId64 ", range: %d--%" PRId64, pCfg->maxQueryTime,
          TSDB_MIN_QUERYTIME_PER_ACCT, TSDB_MAX_QUERYTIME_PER_ACCT);
    return -1;
  }

  if ((pCfg->accessState != TSDB_VN_ALL_ACCCESS) && (pCfg->accessState != TSDB_VN_WRITE_ACCCESS) &&
      (pCfg->accessState != TSDB_VN_READ_ACCCESS) && (pCfg->accessState != 0)) {
    mWarn("Invalid acct parameter accessState: %d", pCfg->accessState);
    return -1;
  }

  return 0;
}

static int32_t acctCreateAcct(char *name, char *pass, SAcctCfg *pCfg, void *pMsg) {
  SAcctObj *pAcct = mnodeGetAcct(name);
  if (pAcct != NULL) {
    mWarn("acct:%s, is already there", name);
    mnodeDecAcctRef(pAcct);
    return TSDB_CODE_MND_ACCT_ALREADY_EXIST;
  }

  SUserObj *pUser = mnodeGetUser(name);
  if (pUser != NULL) {
    mWarn("user:%s, is already there", name);
    mnodeDecUserRef(pUser);
    return TSDB_CODE_MND_USER_ALREADY_EXIST;
  }

  if (acctCheckAcctParams(pCfg) < 0) {
    return TSDB_CODE_MND_INVALID_ACCT_OPTION;
  }

  pAcct = malloc(sizeof(SAcctObj));
  memset(pAcct, 0, sizeof(SAcctObj));
  strcpy(pAcct->user, name);
  taosEncryptPass((uint8_t*) pass, strlen(pass), pAcct->pass);
  if (pCfg != NULL) {
    pAcct->cfg = *pCfg;
  } else {
    // TODO : set default
    pAcct->cfg = (SAcctCfg){.maxUsers = TSDB_MAX_USERS_PER_ACCT,
                            .maxDbs = TSDB_MAX_DBS_PER_ACCT,
                            .maxTimeSeries = TSDB_MAX_TIMESERIES_PER_ACCT,
                            .maxConnections = TSDB_MAX_CONNECTIONS_PER_ACCT,
                            .maxStreams = TSDB_MAX_STREAMS_PER_ACCT,
                            .maxPointsPerSecond = TSDB_MAX_SPOINTS_PER_ACCT,
                            .maxStorage = TSDB_MAX_STORAGE_PER_ACCT,
                            .maxQueryTime = TSDB_MAX_QUERYTIME_PER_ACCT,
                            .maxInbound = 0,
                            .maxOutbound = 0,
                            .accessState = TSDB_VN_ALL_ACCCESS};
  }
  pAcct->acctId = sdbGetId(tsAcctSdb);
  pAcct->createdTime = taosGetTimestampMs();

  int32_t grantCode = grantCheck(TSDB_GRANT_ACCT);
  if (grantCode != TSDB_CODE_SUCCESS) return grantCode;

   SSdbRow row = {
    .type     = SDB_OPER_GLOBAL,
    .pTable   = tsAcctSdb,
    .pObj     = pAcct,
    .rowSize  = sizeof(SAcctObj),
    .pMsg     = pMsg
  };
  
  int32_t code = sdbInsertRow(&row);

  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("acct:%s, failed to create by %s, reason:%s", pAcct->user, mnodeGetUserFromMsg(pMsg), tstrerror(code));
    tfree(pAcct);
  } else {
    mLInfo("acct:%s, is created by %s", pAcct->user, mnodeGetUserFromMsg(pMsg));

    // create a user in the same name and pass
    char suser[64] = {0};
    sprintf(suser, "_%s", name);
    mnodeCreateUser(pAcct, name, pass, NULL);
    mnodeCreateUser(pAcct, suser, tsInternalPass, NULL);  // create stream user
  }

  return code;
}

static int32_t acctDropAcct(char *name, void *pMsg) {
  SAcctObj *pAcct = mnodeGetAcct(name);
  if (pAcct == NULL) {
    mWarn("acct:%s, is not there", name);
    return TSDB_CODE_MND_INVALID_ACCT;
  }

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsAcctSdb,
    .pObj   = pAcct,
    .pMsg   = pMsg
  };

  int32_t code = sdbDeleteRow(&row);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("acct:%s, failed to drop by %s, reason:%s", pAcct->user, mnodeGetUserFromMsg(pMsg), tstrerror(code));
  } else {
    mLInfo("acct:%s, is dropped by %s", pAcct->user, mnodeGetUserFromMsg(pMsg));
  }

  mnodeDecAcctRef(pAcct);
  return code;
}

void acctCleanUp() {
  if (tsMgmtStatisTimer != NULL) {
    taosTmrStopA(&tsMgmtStatisTimer);
    tsMgmtStatisTimer = NULL;
  }
}

static int32_t acctGetAcctMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->pAcct->user, "root") != 0) {
    mnodeDecUserRef(pUser);
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = (TSDB_USER_LEN - 1) + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 14 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "Users/TUsers");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "Dbs/TDbs");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 18 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "Series/TSeries");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 18 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "Streams/TStreams");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 22 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "Storage(G)/TStorage(G)");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 6 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "state");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_BIGINT;
  strcpy(pSchema[cols].name, "UDisk");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = sdbGetNumOfRows(tsAcctSdb);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  mnodeDecUserRef(pUser);
  return 0;
}

char *mnodeGetAcctStateStr(int32_t accessState) {
  if (accessState == 0) {
    return "no";
  } else if (accessState == TSDB_VN_ALL_ACCCESS) {
    return "all";
  } else if (accessState == TSDB_VN_WRITE_ACCCESS) {
    return "write";
  } else if (accessState == TSDB_VN_READ_ACCCESS) {
    return "read";
  }

  return "null";
}

static int32_t acctRetrieveData(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t   numOfRows = 0;
  SAcctObj *pAcct = NULL;
  char *    pWrite;
  int32_t   cols = 0;
  char      tmp[24];

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextAcct(pShow->pIter, &pAcct);
    if (pAcct == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pAcct->user, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pAcct->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    sprintf(tmp, "%d/%d", pAcct->acctInfo.numOfUsers, pAcct->cfg.maxUsers);
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tmp, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    sprintf(tmp, "%d/%d", pAcct->acctInfo.numOfDbs, pAcct->cfg.maxDbs);
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tmp, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    sprintf(tmp, "%d/%d", pAcct->acctInfo.numOfTimeSeries, pAcct->cfg.maxTimeSeries);
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tmp, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    sprintf(tmp, "%d/%d", pAcct->acctInfo.numOfStreams, pAcct->cfg.maxStreams);
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tmp, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (pAcct->cfg.maxStorage == INT64_MAX) {
      sprintf(tmp, "%.3f/unlimited", pAcct->acctInfo.totalStorage / (1024. * 1024. * 1024.));
    } else {
      sprintf(tmp, "%.3f/%.3f", pAcct->acctInfo.totalStorage / (1024. * 1024. * 1024),
              pAcct->cfg.maxStorage / (1024. * 1024. * 1024));
    }
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tmp, pShow->bytes[cols]);
    cols++;
  
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    char *role = mnodeGetAcctStateStr(pAcct->cfg.accessState);
    STR_TO_VARSTR(pWrite, role);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pAcct->acctInfo.compStorage;
    cols++;

    numOfRows++;

    mnodeDecAcctRef(pAcct);
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

static int32_t acctCheckAlterAcctParams(SAcctObj *pAcct, SAcctCfg *pCfg) {
  if (pCfg->maxUsers >= 0 && (pCfg->maxUsers < TSDB_MIN_USERS_PER_ACCT || pCfg->maxUsers > TSDB_MAX_USERS_PER_ACCT)) {
    mWarn("Invalid acct parameter maxUsers: %d, range: %d--%d", pCfg->maxUsers, TSDB_MIN_USERS_PER_ACCT,
          TSDB_MAX_USERS_PER_ACCT);
    return -1;
  }

  if (pCfg->maxDbs >= 0 && (pCfg->maxDbs < TSDB_MIN_DBS_PER_ACCT || pCfg->maxDbs > TSDB_MAX_DBS_PER_ACCT)) {
    mWarn("Invalid acct parameter maxDbs: %d, range: %d--%d", pCfg->maxUsers, TSDB_MIN_DBS_PER_ACCT,
          TSDB_MAX_DBS_PER_ACCT);
    return -1;
  }

  if (pCfg->maxTimeSeries >= 0 &&
      (pCfg->maxTimeSeries < TSDB_MIN_TIMESERIES_PER_ACCT || pCfg->maxTimeSeries > TSDB_MAX_TIMESERIES_PER_ACCT)) {
    mWarn("Invalid acct parameter maxTimeSeries: %d, range: %d--%d", pCfg->maxTimeSeries, TSDB_MIN_TIMESERIES_PER_ACCT,
          TSDB_MAX_TIMESERIES_PER_ACCT);
    return -1;
  }

  if (pCfg->maxConnections >= 0 &&
      (pCfg->maxConnections < TSDB_MIN_CONNECTIONS_PER_ACCT || pCfg->maxConnections > TSDB_MAX_CONNECTIONS_PER_ACCT)) {
    mWarn("Invalid acct parameter maxConnections: %d, range: %d--%d", pCfg->maxConnections,
          TSDB_MIN_CONNECTIONS_PER_ACCT, TSDB_MAX_CONNECTIONS_PER_ACCT);
    return -1;
  }

  if (pCfg->maxStreams >= 0 &&
      (pCfg->maxStreams < TSDB_MIN_STREAMS_PER_ACCT || pCfg->maxStreams > TSDB_MAX_STREAMS_PER_ACCT)) {
    mWarn("Invalid acct parameter maxStreams: %d, range: %d--%d", pCfg->maxStreams, TSDB_MIN_STREAMS_PER_ACCT,
          TSDB_MAX_STREAMS_PER_ACCT);
    return -1;
  }

  if (pCfg->maxPointsPerSecond >= 0 &&
      (pCfg->maxPointsPerSecond < TSDB_MIN_SPOINTS_PER_ACCT || pCfg->maxPointsPerSecond > TSDB_MAX_SPOINTS_PER_ACCT)) {
    mWarn("Invalid acct parameter maxPointsPerSecond: %d, range: %d--%d", pCfg->maxPointsPerSecond,
          TSDB_MIN_SPOINTS_PER_ACCT, TSDB_MAX_SPOINTS_PER_ACCT);
    return -1;
  }

  if (pCfg->maxStorage >= 0 &&
      (pCfg->maxStorage < TSDB_MIN_STORAGE_PER_ACCT || pCfg->maxStorage > TSDB_MAX_STORAGE_PER_ACCT)) {
    mWarn("Invalid acct parameter maxStorage: %" PRId64 ", range: %d--%" PRId64, pCfg->maxStorage,
          TSDB_MIN_STORAGE_PER_ACCT, TSDB_MAX_STORAGE_PER_ACCT);
    return -1;
  }

  if (pCfg->maxQueryTime >= 0 &&
      (pCfg->maxQueryTime < TSDB_MIN_QUERYTIME_PER_ACCT || pCfg->maxQueryTime > TSDB_MAX_QUERYTIME_PER_ACCT)) {
    mWarn("Invalid acct parameter maxQueryTime: %" PRId64 ", range: %d--%" PRId64, pCfg->maxQueryTime,
          TSDB_MIN_QUERYTIME_PER_ACCT, TSDB_MAX_QUERYTIME_PER_ACCT);
    return -1;
  }

  if ((pCfg->accessState >= 0) && (pCfg->accessState != TSDB_VN_ALL_ACCCESS) &&
      (pCfg->accessState != TSDB_VN_WRITE_ACCCESS) && (pCfg->accessState != TSDB_VN_READ_ACCCESS) &&
      (pCfg->accessState != 0)) {
    mWarn("Invalid acct parameter accessState: %d", pCfg->accessState);
    return -1;
  }

  return 0;
}

static int32_t acctAlterAcct(char *name, char *pass, SAcctCfg *pCfg, void *pMsg) {
  SAcctObj *pAcct = NULL;

  pAcct = mnodeGetAcct(name);
  if (pAcct == NULL) {
    mDebug("account: %s not exists", name);
    return TSDB_CODE_MND_INVALID_ACCT;
  }

  if (acctCheckAlterAcctParams(pAcct, pCfg) < 0) return TSDB_CODE_MND_INVALID_ACCT_OPTION;

  if (pCfg->maxUsers > 0) {
    mDebug("account: %s maxUsers is modified from %d to %d", name, pAcct->cfg.maxUsers, pCfg->maxUsers);
    pAcct->cfg.maxUsers = pCfg->maxUsers;
  }

  if (pCfg->maxDbs > 0) {
    mDebug("account: %s maxDbs is modified from %d to %d", name, pAcct->cfg.maxDbs, pCfg->maxDbs);
    pAcct->cfg.maxDbs = pCfg->maxDbs;
  }

  if (pCfg->maxTimeSeries > 0) {
    mDebug("account: %s maxTimeSeries is modified from %d to %d", name, pAcct->cfg.maxTimeSeries, pCfg->maxTimeSeries);
    pAcct->cfg.maxTimeSeries = pCfg->maxTimeSeries;
  }

  if (pCfg->maxStreams > 0) {
    mDebug("account: %s maxStreams is modified from %d to %d", name, pAcct->cfg.maxStreams, pCfg->maxStreams);
    pAcct->cfg.maxStreams = pCfg->maxStreams;
  }

  if (pCfg->maxPointsPerSecond > 0) {
    mDebug("account: %s maxPointsPerSecond is modified from %d to %d", name, pAcct->cfg.maxPointsPerSecond,
           pCfg->maxPointsPerSecond);
    pAcct->cfg.maxPointsPerSecond = pCfg->maxPointsPerSecond;
  }

  if (pCfg->maxStorage > 0) {
    mDebug("account: %s maxStorage is modified from %" PRId64 " to %" PRId64, name, pAcct->cfg.maxStorage,
           pCfg->maxStorage);
    pAcct->cfg.maxStorage = pCfg->maxStorage;
    // TODO : Reactive account vnodes
  }

  if (pCfg->maxQueryTime > 0) {
    mDebug("account: %s maxQueryTime is modified from %" PRId64 " to %" PRId64, name, pAcct->cfg.maxQueryTime,
           pCfg->maxQueryTime);
    pAcct->cfg.maxQueryTime = pCfg->maxQueryTime;
    // TODO : Reactive account vnodes
  }

  if (pCfg->accessState >= 0) {
    mDebug("account: %s accessState is modified from %d to %d", name, pAcct->cfg.accessState, pCfg->accessState);
    pAcct->cfg.accessState = pCfg->accessState;
  }

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsAcctSdb,
    .pObj   = pAcct,
    .pMsg   = pMsg
  };

  int32_t code = sdbUpdateRow(&row);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("acct:%s, failed to drop by %s, reason:%s", pAcct->user, mnodeGetUserFromMsg(pMsg), tstrerror(code));
    tfree(pAcct);
  } else {
    mLInfo("acct:%s, is dropped by %s", pAcct->user, mnodeGetUserFromMsg(pMsg));
  }

  mnodeDecAcctRef(pAcct);
  return code;
}

static int64_t acctGetStatistic(SAcctObj *pAcct) {
  if (pAcct == NULL) return 0;
  
  void   *pIter = NULL;
  SVgObj *pVgroup;
  int64_t totalStorage = 0;
  int64_t pointsWritten = 0;
  TSKEY   sKey = taosGetTimestampMs();

  while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;
    if (pVgroup->pDb != NULL && pVgroup->pDb->pAcct == pAcct) {
      totalStorage += pVgroup->totalStorage;
      pointsWritten += pVgroup->pointsWritten;
      pVgroup->accessState = pAcct->acctInfo.accessState;
    }
    mnodeDecVgroupRef(pVgroup);
  }
  
  pAcct->acctInfo.totalStorage = totalStorage;
  pAcct->acctInfo.numOfPointsPerSecond =
      (int32_t)((pointsWritten - pAcct->acctInfo.totalPoints) * 1000 / (sKey - pAcct->acctInfo.sKey));
  pAcct->acctInfo.sKey = sKey;
  pAcct->acctInfo.totalPoints = pointsWritten;

  // set vnode access
  char accessState = TSDB_VN_ALL_ACCCESS;
  if (pAcct->acctInfo.totalStorage > pAcct->cfg.maxStorage) {
    accessState &= (~TSDB_VN_WRITE_ACCCESS);
    mDebug("acct:%s, set state to no write access, totalStorage:%" PRId64 " maxStorage:%" PRId64, pAcct->user,
           pAcct->acctInfo.totalStorage, pAcct->cfg.maxStorage);
  }

  if (grantCheck(TSDB_GRANT_STORAGE) != 0) {
    accessState &= (~TSDB_VN_WRITE_ACCCESS);
    mDebug("acct:%s, set state to no write access, totalStorage:%" PRId64 " larger than grant value", pAcct->user,
           pAcct->acctInfo.totalStorage);
  }

  if (pAcct->acctInfo.queryTime > pAcct->cfg.maxQueryTime) {
    accessState &= (~TSDB_VN_READ_ACCCESS);
  }

  accessState &= pAcct->cfg.accessState;
  pAcct->acctInfo.accessState = accessState;

  // record monitor info
  SAcctMonitorObj monObj = {0};
  monObj.acctId                 = pAcct->user;
  monObj.currentPointsPerSecond = pAcct->acctInfo.numOfPointsPerSecond;
  monObj.maxPointsPerSecond     = pAcct->cfg.maxPointsPerSecond;
  monObj.totalTimeSeries        = pAcct->acctInfo.numOfTimeSeries;
  monObj.maxTimeSeries          = pAcct->cfg.maxTimeSeries;
  monObj.totalStorage           = pAcct->acctInfo.totalStorage;
  monObj.maxStorage             = pAcct->cfg.maxStorage;
  monObj.totalQueryTime         = pAcct->acctInfo.queryTime;
  monObj.maxQueryTime           = pAcct->cfg.maxQueryTime;
  monObj.totalInbound           = pAcct->acctInfo.inblound;
  monObj.maxInbound             = pAcct->cfg.maxInbound;
  monObj.totalOutbound          = pAcct->acctInfo.outbound;
  monObj.maxOutbound            = pAcct->cfg.maxOutbound;
  monObj.totalDbs               = pAcct->acctInfo.numOfDbs;
  monObj.maxDbs                 = pAcct->cfg.maxDbs;
  monObj.totalUsers             = pAcct->acctInfo.numOfUsers;
  monObj.maxUsers               = pAcct->cfg.maxUsers;
  monObj.totalStreams           = pAcct->acctInfo.numOfStreams;
  monObj.maxStreams             = pAcct->cfg.maxStreams;
  monObj.totalConns             = pAcct->acctInfo.numOfConns;
  monObj.maxConns               = pAcct->cfg.maxConnections;
  monObj.accessState            = pAcct->acctInfo.accessState;

  monitorSaveAcctLog(&monObj);

  return totalStorage;
}

static int32_t acctProcessCreateAcctMsg(SMnodeMsg *pMsg) {
  SCreateAcctMsg *pCreate = pMsg->rpcMsg.pCont;
  SAcctObj *pAcct = mnodeGetAcct(pCreate->user);
  if (pAcct != NULL) {
    mInfo("acct:%s, already exist, update it", pCreate->user);
    mnodeDecAcctRef(pAcct);
    return acctProcessAlterAcctMsg(pMsg);
  }
  
  pCreate->cfg.maxUsers           = htonl(pCreate->cfg.maxUsers);
  pCreate->cfg.maxDbs             = htonl(pCreate->cfg.maxDbs);
  pCreate->cfg.maxTimeSeries      = htonl(pCreate->cfg.maxTimeSeries);
  pCreate->cfg.maxConnections     = htonl(pCreate->cfg.maxConnections);
  pCreate->cfg.maxStreams         = htonl(pCreate->cfg.maxStreams);
  pCreate->cfg.maxPointsPerSecond = htonl(pCreate->cfg.maxPointsPerSecond);
  pCreate->cfg.maxStorage         = htobe64(pCreate->cfg.maxStorage);
  pCreate->cfg.maxQueryTime       = htobe64(pCreate->cfg.maxQueryTime);
  pCreate->cfg.maxInbound         = htobe64(pCreate->cfg.maxInbound);
  pCreate->cfg.maxOutbound        = htobe64(pCreate->cfg.maxOutbound);

  SUserObj *pUser = pMsg->pUser;
  if (strcmp(pUser->user, "root") != 0) {
    mError("acct:%s, failed to create account, no rights", pCreate->user);
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  return acctCreateAcct(pCreate->user, pCreate->pass, &(pCreate->cfg), pMsg);
}

static int32_t acctProcessDropAcctMsg(SMnodeMsg *pMsg) {
  SDropAcctMsg *pDrop = pMsg->rpcMsg.pCont;

  SUserObj *pUser = pMsg->pUser;
  if (strcmp(pUser->user, "root") != 0) {
    mError("acct:%s, failed to drop account, invalid user", pDrop->user);
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  return acctDropAcct(pDrop->user, pMsg);
}

static int32_t acctProcessAlterAcctMsg(SMnodeMsg *pMsg) {
  SAlterAcctMsg *pAlter = pMsg->rpcMsg.pCont;
  pAlter->cfg.maxUsers           = htonl(pAlter->cfg.maxUsers);
  pAlter->cfg.maxDbs             = htonl(pAlter->cfg.maxDbs);
  pAlter->cfg.maxTimeSeries      = htonl(pAlter->cfg.maxTimeSeries);
  pAlter->cfg.maxConnections     = htonl(pAlter->cfg.maxConnections);
  pAlter->cfg.maxStreams         = htonl(pAlter->cfg.maxStreams);
  pAlter->cfg.maxPointsPerSecond = htonl(pAlter->cfg.maxPointsPerSecond);
  pAlter->cfg.maxStorage         = htobe64(pAlter->cfg.maxStorage);
  pAlter->cfg.maxQueryTime       = htobe64(pAlter->cfg.maxQueryTime);
  pAlter->cfg.maxInbound         = htobe64(pAlter->cfg.maxInbound);
  pAlter->cfg.maxOutbound        = htobe64(pAlter->cfg.maxOutbound);

  SUserObj *pUser = pMsg->pUser;
  if (strcmp(pUser->user, "root") != 0) {
    mError("acct:%s, failed to alter account, no rights", pAlter->user);
    return TSDB_CODE_MND_NO_RIGHTS;
  }

  return acctAlterAcct(pAlter->user, pAlter->pass, &(pAlter->cfg), pMsg);
}

static int32_t acctCheckUserLimit(SAcctObj *pAcct) {
  if (pAcct->cfg.maxUsers != 0 && pAcct->acctInfo.numOfUsers >= pAcct->cfg.maxUsers) {
    mError("acct:%s, users:%d exceed limit:%d", pAcct->user, pAcct->acctInfo.numOfUsers, pAcct->cfg.maxUsers);
    return TSDB_CODE_MND_TOO_MANY_USERS;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t acctrCheckDbLimit(SAcctObj *pAcct) {
  if (pAcct->cfg.maxDbs != 0 && pAcct->acctInfo.numOfDbs >= pAcct->cfg.maxDbs) {
    mError("acct:%s, dbs:%d exceed limit:%d", pAcct->user, pAcct->acctInfo.numOfDbs, pAcct->cfg.maxDbs);
    return TSDB_CODE_MND_TOO_MANY_DATABASES;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t acctCheckTableLimit(SAcctObj *pAcct) {
  if (pAcct->cfg.maxTimeSeries != 0 && pAcct->acctInfo.numOfTimeSeries >= pAcct->cfg.maxTimeSeries) {
    mError("acct:%s, timeSeries:%d exceed limit:%d", pAcct->user, pAcct->acctInfo.numOfTimeSeries,
           pAcct->cfg.maxTimeSeries);
    return TSDB_CODE_MND_TOO_MANY_TIMESERIES;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t acctCheck(void *param, EAcctGrantType type) {
  SAcctObj *pAcct = param;
  switch (type) {
    case ACCT_GRANT_USER:
      return acctCheckUserLimit(pAcct);
    case ACCT_GRANT_DB:
      return acctrCheckDbLimit(pAcct);
    case ACCT_GRANT_TABLE:
      return acctCheckTableLimit(pAcct);
  }

  return TSDB_CODE_SUCCESS;
}
