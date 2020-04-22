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
#include "trpc.h"
#include "ttime.h"
#include "ttimer.h"
#include "tutil.h"
#include "tgrant.h"
#include "tglobal.h"
#include "mnode.h"
#include "monitorSystem.h"
#include "mgmtDef.h"
#include "mgmtLog.h"
#include "mgmtAcct.h"
#include "mgmtDnode.h"
#include "mgmtDb.h"
#include "mgmtMnode.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"

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

extern void *  tsMgmtTmr;
extern void *  tsAcctSdb;
extern int32_t tsAcctUpdateSize;
extern void   *tsDnodeSdb;
static void   *tsMgmtStatisTimer = NULL;

static int64_t acctGetStatistic(SAcctObj *pAcct);
static void    acctProcessCreateAcctMsg(SQueuedMsg *pMsg);
static void    acctProcessDropAcctMsg(SQueuedMsg *pMsg);
static void    acctProcessAlterAcctMsg(SQueuedMsg *pMsg);
static int32_t acctGetAcctMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t acctRetrieveData(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static void acctDoStatistic(void *handle, void *tmrId) {
  SAcctObj *pAcct = NULL;
  void *    pNode = NULL;

  if (tsAcctSdb != NULL) {
    int64_t totalStorage = 0;
    while (1) {
      pNode = sdbFetchRow(tsAcctSdb, pNode, (void **)&pAcct);
      if (pAcct == NULL) break;
      totalStorage += acctGetStatistic(pAcct);
      mgmtDecAcctRef(pAcct);
    }

    grantReset(TSDB_GRANT_STORAGE, (uint64_t)totalStorage);
  }

  taosTmrReset(acctDoStatistic, tsStatusInterval * 30000, NULL, tsMgmtTmr, &tsMgmtStatisTimer);
}

int32_t acctInit() {
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_CREATE_ACCT, acctProcessCreateAcctMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_DROP_ACCT, acctProcessDropAcctMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_ALTER_ACCT, acctProcessAlterAcctMsg);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_ACCT, acctGetAcctMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_ACCT, acctRetrieveData);

  taosTmrReset(acctDoStatistic, tsStatusInterval * 1000, NULL, tsMgmtTmr, &tsMgmtStatisTimer);
  
  mTrace("table:accounts is initialized");
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
    mWarn("Invalid acct parameter maxStorage: %" PRId64 ", range: %" PRId64 "--%" PRId64, pCfg->maxStorage, TSDB_MIN_STORAGE_PER_ACCT,
          TSDB_MAX_STORAGE_PER_ACCT);
    return -1;
  }

  if (pCfg->maxQueryTime < TSDB_MIN_QUERYTIME_PER_ACCT || pCfg->maxQueryTime > TSDB_MAX_QUERYTIME_PER_ACCT) {
    mWarn("Invalid acct parameter maxQueryTime: %" PRId64 ", range: %" PRId64 "--%" PRId64, pCfg->maxQueryTime, TSDB_MIN_QUERYTIME_PER_ACCT,
          TSDB_MAX_QUERYTIME_PER_ACCT);
    return -1;
  }

  if ((pCfg->accessState != TSDB_VN_ALL_ACCCESS) && (pCfg->accessState != TSDB_VN_WRITE_ACCCESS) &&
      (pCfg->accessState != TSDB_VN_READ_ACCCESS) && (pCfg->accessState != 0)) {
    mWarn("Invalid acct parameter accessState: %d", pCfg->accessState);
    return -1;
  }

  return 0;
}

static int32_t acctCreateAcct(char *name, char *pass, SAcctCfg *pCfg) {
  SAcctObj *pAcct = (SAcctObj *)sdbGetRow(tsAcctSdb, name);
  if (pAcct != NULL) {
    return TSDB_CODE_ACCT_ALREADY_EXIST;
  }

  SUserObj *pUser = mgmtGetUser(name);
  if (pUser != NULL) {
    mWarn("user:%s is already there", name);
    return TSDB_CODE_USER_ALREADY_EXIST;
  }

  if (acctCheckAcctParams(pCfg) < 0) {
    return TSDB_CODE_INVALID_ACCT_PARAMETER;
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

   SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsAcctSdb,
    .pObj = pAcct,
    .rowSize = sizeof(SAcctObj)
  };
  int32_t code = sdbInsertRow(&oper);

  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_SDB_ERROR;
    tfree(pAcct);
  } else {
    // create a user in the same name and pass
    char suser[64] = {0};
    sprintf(suser, "_%s", name);
    mgmtCreateUser(pAcct, name, pass);
    mgmtCreateUser(pAcct, suser, tsInternalPass);  // create stream user
    pthread_mutex_init(&pAcct->mutex, NULL);
  }

  return code;
}

int32_t acctDropAcct(char *name) {
  SAcctObj *pAcct = (SAcctObj *)sdbGetRow(tsAcctSdb, name);
  if (pAcct == NULL) {
    mWarn("account:%s is not there", name);
    return TSDB_CODE_INVALID_ACCT;
  }

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsAcctSdb,
    .pObj = pAcct
  };

  int32_t code = sdbDeleteRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_SDB_ERROR;
  }

  return 0;
}

void acctCleanUp() {
  if (tsMgmtStatisTimer != NULL) {
    taosTmrStopA(&tsMgmtStatisTimer);
    tsMgmtStatisTimer = NULL;
  }

}

static int32_t acctGetAcctMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mgmtGetUserFromConn(pConn, NULL);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->pAcct->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_USER_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 14;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "Users/TUsers");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "Dbs/TDbs");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 18;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "Series/TSeries");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 18;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "Streams/TStreams");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 22;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "Storage(G)/TStorage(G)");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10;
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

  return 0;
}

static int32_t acctRetrieveData(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t       numOfRows = 0;
  SAcctObj *pAcct = NULL;
  char *    pWrite;
  int32_t       cols = 0;

  while (numOfRows < rows) {
    pShow->pNode = sdbFetchRow(tsAcctSdb, pShow->pNode, (void **)&pAcct);
    if (pAcct == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strcpy(pWrite, pAcct->user);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pAcct->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    sprintf(pWrite, "%d/%d", pAcct->acctInfo.numOfUsers, pAcct->cfg.maxUsers);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    sprintf(pWrite, "%d/%d", pAcct->acctInfo.numOfDbs, pAcct->cfg.maxDbs);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    sprintf(pWrite, "%d/%d", pAcct->acctInfo.numOfTimeSeries, pAcct->cfg.maxTimeSeries);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    sprintf(pWrite, "%d/%d", pAcct->acctInfo.numOfStreams, pAcct->cfg.maxStreams);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (pAcct->cfg.maxStorage == INT64_MAX) {
      sprintf(pWrite, "%.3f/unlimited", pAcct->acctInfo.totalStorage / (1024. * 1024. * 1024.));
    } else {
      sprintf(pWrite, "%.3f/%.3f", pAcct->acctInfo.totalStorage / (1024. * 1024. * 1024),
              pAcct->cfg.maxStorage / (1024. * 1024. * 1024));
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pAcct->acctInfo.compStorage;
    cols++;

    numOfRows++;
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
    mWarn("Invalid acct parameter maxStorage: %" PRId64 ", range: %" PRId64 "--%" PRId64, pCfg->maxStorage, TSDB_MIN_STORAGE_PER_ACCT,
          TSDB_MAX_STORAGE_PER_ACCT);
    return -1;
  }

  if (pCfg->maxQueryTime >= 0 &&
      (pCfg->maxQueryTime < TSDB_MIN_QUERYTIME_PER_ACCT || pCfg->maxQueryTime > TSDB_MAX_QUERYTIME_PER_ACCT)) {
    mWarn("Invalid acct parameter maxQueryTime: %" PRId64 ", range: %" PRId64 "--%" PRId64, pCfg->maxQueryTime, TSDB_MIN_QUERYTIME_PER_ACCT,
          TSDB_MAX_QUERYTIME_PER_ACCT);
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

static int32_t acctUpdateAcct(SAcctObj *pAcct) {
  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsAcctSdb,
    .pObj = pAcct,
    .rowSize = tsAcctUpdateSize
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(pAcct);
    code = TSDB_CODE_SDB_ERROR;
  }

  return code;
}

static int32_t acctAlterAcct(char *name, char *pass, SAcctCfg *pCfg) {
  SAcctObj *pAcct = NULL;

  pAcct = mgmtGetAcct(name);
  if (pAcct == NULL) {
    mTrace("account: %s not exists", name);
    return TSDB_CODE_INVALID_ACCT;
  }

  if (acctCheckAlterAcctParams(pAcct, pCfg) < 0) return TSDB_CODE_INVALID_OPTION;

  if (pCfg->maxUsers > 0) {
    mTrace("account: %s maxUsers is modified from %d to %d", name, pAcct->cfg.maxUsers, pCfg->maxUsers);
    pAcct->cfg.maxUsers = pCfg->maxUsers;
  }

  if (pCfg->maxDbs > 0) {
    mTrace("account: %s maxDbs is modified from %d to %d", name, pAcct->cfg.maxDbs, pCfg->maxDbs);
    pAcct->cfg.maxDbs = pCfg->maxDbs;
  }

  if (pCfg->maxTimeSeries > 0) {
    mTrace("account: %s maxTimeSeries is modified from %d to %d", name, pAcct->cfg.maxTimeSeries, pCfg->maxTimeSeries);
    pAcct->cfg.maxTimeSeries = pCfg->maxTimeSeries;
  }

  if (pCfg->maxStreams > 0) {
    mTrace("account: %s maxStreams is modified from %d to %d", name, pAcct->cfg.maxStreams, pCfg->maxStreams);
    pAcct->cfg.maxStreams = pCfg->maxStreams;
  }

  if (pCfg->maxPointsPerSecond > 0) {
    mTrace("account: %s maxPointsPerSecond is modified from %d to %d", name, pAcct->cfg.maxPointsPerSecond,
           pCfg->maxPointsPerSecond);
    pAcct->cfg.maxPointsPerSecond = pCfg->maxPointsPerSecond;
  }

  if (pCfg->maxStorage > 0) {
    mTrace("account: %s maxStorage is modified from %d to %d", name, pAcct->cfg.maxStorage, pCfg->maxStorage);
    pAcct->cfg.maxStorage = pCfg->maxStorage;
    // TODO : Reactive account vnodes
  }

  if (pCfg->maxQueryTime > 0) {
    mTrace("account: %s maxQueryTime is modified from %d to %d", name, pAcct->cfg.maxQueryTime, pCfg->maxQueryTime);
    pAcct->cfg.maxQueryTime = pCfg->maxQueryTime;
    // TODO : Reactive account vnodes
  }

  if (pCfg->accessState >= 0) {
    mTrace("account: %s accessState is modified from %d to %d", name, pAcct->cfg.accessState, pCfg->accessState);
    pAcct->cfg.accessState = pCfg->accessState;
  }

  acctUpdateAcct(pAcct);

  return TSDB_CODE_SUCCESS;
}

static int64_t acctGetStatistic(SAcctObj *pAcct) {
  if (pAcct == NULL) return 0;
  
  void   *pNode = NULL;
  SVgObj *pVgroup;
  int64_t totalStorage = 0;
  int64_t pointsWritten = 0;
  TSKEY   sKey = taosGetTimestampMs();

  while (1) {
    pNode = mgmtGetNextVgroup(pNode, &pVgroup);
    if (pVgroup == NULL) break;
    if (pVgroup->pDb != NULL && pVgroup->pDb->pAcct == pAcct) {
      totalStorage += pVgroup->totalStorage;
      pointsWritten += pVgroup->pointsWritten;
    }
    mgmtDecVgroupRef(pVgroup);
  }

  pAcct->acctInfo.totalStorage = totalStorage;
  pAcct->acctInfo.numOfPointsPerSecond =
      (int32_t)((pointsWritten - pAcct->acctInfo.totalPoints) * 1000 / (sKey - pAcct->acctInfo.sKey));
  pAcct->acctInfo.sKey = sKey;
  pAcct->acctInfo.totalPoints = pointsWritten;

  monitorSaveAcctLog(pAcct->user, pAcct->acctInfo.numOfPointsPerSecond, pAcct->cfg.maxPointsPerSecond,
                 pAcct->acctInfo.numOfTimeSeries, pAcct->cfg.maxTimeSeries, pAcct->acctInfo.totalStorage,
                 pAcct->cfg.maxStorage, pAcct->acctInfo.queryTime, pAcct->cfg.maxQueryTime, pAcct->acctInfo.inblound,
                 pAcct->cfg.maxInbound, pAcct->acctInfo.outbound, pAcct->cfg.maxOutbound, pAcct->acctInfo.numOfDbs,
                 pAcct->cfg.maxDbs, pAcct->acctInfo.numOfUsers, pAcct->cfg.maxUsers, pAcct->acctInfo.numOfStreams,
                 pAcct->cfg.maxStreams, pAcct->acctInfo.numOfConns, pAcct->cfg.maxConnections,
                 pAcct->acctInfo.accessState);

  return totalStorage;
}

static void acctProcessCreateAcctMsg(SQueuedMsg *pMsg) {
  SCMCreateAcctMsg *pCreate = pMsg->pCont;
  SAcctObj *pAcct = (SAcctObj *)sdbGetRow(tsAcctSdb, pCreate->user);
  if (pAcct != NULL) {
    mError("account:%s, already exist, update it", pCreate->user);
    acctProcessAlterAcctMsg(pMsg);
    return;
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
    mError("account:%s, failed to create account, invalid user", pCreate->user);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_RIGHTS);
    return;
  }

  int32_t code = acctCreateAcct(pCreate->user, pCreate->pass, &(pCreate->cfg));
  if (code == TSDB_CODE_SUCCESS) {
    mLPrint("account:%s is created by %s", pCreate->user, pUser->user);
  } else {
    mError("account:%s, failed to create account, reason:%s", pCreate->user, tstrerror(code));
  }

  mgmtSendSimpleResp(pMsg->thandle, code);
}

static void acctProcessDropAcctMsg(SQueuedMsg *pMsg) {
  SCMDropAcctMsg *pDrop = pMsg->pCont;

  SUserObj *pUser = pMsg->pUser;
  if (strcmp(pUser->user, "root") != 0) {
    mError("account:%s, failed to drop account, invalid user", pDrop->user);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_RIGHTS);
    return;
  }

  int32_t code = acctDropAcct(pDrop->user);
  if (code == TSDB_CODE_SUCCESS) {
    mLPrint("account:%s is dropped by %s", pDrop->user, pUser->user);
  } else {
    mError("account:%s, failed to drop account, reason:%s", pDrop->user, tstrerror(code));
  }

  mgmtSendSimpleResp(pMsg->thandle, code);
}

static void acctProcessAlterAcctMsg(SQueuedMsg *pMsg) {
  SCMAlterAcctMsg *pAlter = pMsg->pCont;
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
    mError("account:%s, failed to alter account, no rights", pAlter->user);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_RIGHTS);
    return;
  }

  int32_t code = acctAlterAcct(pAlter->user, pAlter->pass, &(pAlter->cfg));;
  if (code == TSDB_CODE_SUCCESS) {
    mLPrint("account:%s is altered by %s", pAlter->user, pUser->user);
  } else {
    mError("account:%s, failed to alter account, reason:%s", pAlter->user, tstrerror(code));
  }

  mgmtSendSimpleResp(pMsg->thandle, code);
}

static int32_t clusterCheckUserLimit(SAcctObj *pAcct) {
  if (pAcct->cfg.maxUsers != 0 && pAcct->acctInfo.numOfUsers >= pAcct->cfg.maxUsers) {
    mError("account:%s, users:%d exceed limit:%d", pAcct->acctId, pAcct->acctInfo.numOfUsers, pAcct->cfg.maxUsers);
    return TSDB_CODE_TOO_MANY_USERS;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t clusterCheckDbLimit(SAcctObj *pAcct) {
  if (pAcct->cfg.maxDbs != 0 && pAcct->acctInfo.numOfDbs >= pAcct->cfg.maxDbs) {
    mError("account:%s, dbs:%d exceed limit:%d", pAcct->acctId, pAcct->acctInfo.numOfDbs, pAcct->cfg.maxDbs);
    return TSDB_CODE_TOO_MANY_DATABASES;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t clusterCheckTableLimit(SAcctObj *pAcct) {
  if (pAcct->cfg.maxTimeSeries != 0 && pAcct->acctInfo.numOfTimeSeries >= pAcct->cfg.maxTimeSeries) {
    mError("account:%s, timeSeries:%d exceed limit:%d", pAcct->acctId, pAcct->acctInfo.numOfTimeSeries, pAcct->cfg.maxTimeSeries);
    return TSDB_CODE_TOO_MANY_TIME_SERIES;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t acctCheck(void *param, EAcctGrantType type) {
  SAcctObj *pAcct = param;
  switch (type) {
    case ACCT_GRANT_USER:
      return clusterCheckUserLimit(pAcct);
    case ACCT_GRANT_DB:
      return clusterCheckDbLimit(pAcct);
    case ACCT_GRANT_TABLE:
      return clusterCheckTableLimit(pAcct);
  }

  return TSDB_CODE_SUCCESS;
}
