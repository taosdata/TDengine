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
#include "tutil.h"
#include "account.h"
#include "mgmtAcct.h"
#include "mgmtDb.h"
#include "mgmtDnode.h"
#include "mgmtGrant.h"
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

static void   *tsMgmtStatisTimer = NULL;
static void   *tsAcctSdb         = NULL;
static int32_t tsAcctUpdateSize;

static void    mgmtCreateRootAcct();
static int64_t mgmtGetStatistic(SAcctObj *pAcct);
static void    mgmtProcessCreateAcctMsg(SQueuedMsg *pMsg);
static void    mgmtProcessDropAcctMsg(SQueuedMsg *pMsg);
static void    mgmtProcessAlterAcctMsg(SQueuedMsg *pMsg);
static int32_t mgmtGetAcctMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mgmtRetrieveAccts(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static int32_t mgmtAcctActionDestroy(SSdbOperDesc *pOper) {
   tfree(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtAcctActionInsert(SSdbOperDesc *pOper) {
  SAcctObj *pAcct = pOper->pObj;
  pAcct->pHead = NULL;
  pAcct->pUser = NULL;
  pAcct->acctInfo.numOfUsers = 0;
  pAcct->acctInfo.numOfDbs = 0;
  pAcct->acctInfo.numOfTimeSeries = 0;
  pAcct->acctInfo.numOfPointsPerSecond = 0;
  pAcct->acctInfo.numOfConns = 0;
  pAcct->acctInfo.numOfQueries = 0;
  pAcct->acctInfo.numOfStreams = 0;
  pAcct->acctInfo.totalStorage = 0;
  pAcct->acctInfo.compStorage = 0;
  pAcct->acctInfo.queryTime = 0;
  pthread_mutex_init(&pAcct->mutex, NULL);

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtAcctActionDelete(SSdbOperDesc *pOper) {
  SAcctObj *pAcct = pOper->pObj;
  mgmtDropAllUsers(pAcct);
  mgmtDropAllDbs(pAcct);
  pthread_mutex_destroy(&pAcct->mutex);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtAcctActionUpdate(SSdbOperDesc *pOper) {
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtAcctActionEncode(SSdbOperDesc *pOper) {
  SAcctObj *pAcct = pOper->pObj;

  if (pOper->maxRowSize < tsAcctUpdateSize) {
    return -1;
  } else {
    memcpy(pOper->rowData, pAcct, tsAcctUpdateSize);
    pOper->rowSize = tsAcctUpdateSize;
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t mgmtAcctActionDecode(SSdbOperDesc *pOper) {
  SAcctObj *pAcct = (SAcctObj *) calloc(1, sizeof(SAcctObj));
  if (pAcct == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

  memcpy(pAcct, pOper->rowData, tsAcctUpdateSize);
  pOper->pObj = pAcct;
  return TSDB_CODE_SUCCESS;
}

static void mgmtDoAcctStatistic(void *handle, void *tmrId) {
  SAcctObj *pAcct = NULL;
  void *    pNode = NULL;

  if (tsAcctSdb != NULL) {
    int64_t totalStorage = 0;
    while (1) {
      pNode = sdbFetchRow(tsAcctSdb, pNode, (void **)&pAcct);
      if (pAcct == NULL) break;
      totalStorage += mgmtGetStatistic(pAcct);
    }

    mgmtSetCurStorage(totalStorage);
  }

  mgmtSendGrantMsgToMaster();
  taosTmrReset(mgmtDoAcctStatistic, tsStatusInterval * 30000, NULL, tsMgmtTmr, &tsMgmtStatisTimer);
}

int32_t mgmtInitAccts() {
  SAcctObj tObj;
  tsAcctUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableName    = "accounts",
    .hashSessions = TSDB_MAX_USERS,
    .maxRowSize   = tsAcctUpdateSize,
    .keyType      = SDB_KEY_TYPE_STRING,
    .insertFp     = mgmtAcctActionInsert,
    .deleteFp     = mgmtAcctActionDelete,
    .updateFp     = mgmtAcctActionUpdate,
    .encodeFp     = mgmtAcctActionEncode,
    .decodeFp     = mgmtAcctActionDecode,
    .destroyFp    = mgmtAcctActionDestroy,
  };

  tsAcctSdb = sdbOpenTable(&tableDesc);
  if (tsAcctSdb == NULL) {
    mError("failed to init acct data");
    return -1;
  }

  mgmtCreateRootAcct();
  taosTmrReset(mgmtDoAcctStatistic, tsStatusInterval * 30000, NULL, tsMgmtTmr, &tsMgmtStatisTimer);

  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_CREATE_ACCT, mgmtProcessCreateAcctMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_DROP_ACCT, mgmtProcessDropAcctMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_ALTER_ACCT, mgmtProcessAlterAcctMsg);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_ACCT, mgmtGetAcctMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_ACCT, mgmtRetrieveAccts);

  mTrace("account is initialized");
  return 0;
}

SAcctObj *mgmtGetAcct(char *name) {
  return (SAcctObj *)sdbGetRow(tsAcctSdb, name);
}

static int32_t mgmtCheckAcctParams(SAcctCfg *pCfg) {
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

static int32_t mgmtCreateAcct(char *name, char *pass, SAcctCfg *pCfg) {
  SAcctObj *
  pAcct = (SAcctObj *)sdbGetRow(tsAcctSdb, name);
  if (pAcct != NULL) {
    return TSDB_CODE_ACCT_ALREADY_EXIST;
  }

  SUserObj *pUser = mgmtGetUser(name);
  if (pUser != NULL) {
    mWarn("user:%s is already there", name);
    return TSDB_CODE_USER_ALREADY_EXIST;
  }

  if (mgmtCheckAcctParams(pCfg) < 0) {
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

  int32_t grantCode = mgmtCheckAccts();
  if (grantCode != 0) return grantCode;

   SSdbOperDesc oper = {
    .type = SDB_OPER_TYPE_GLOBAL,
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

int32_t mgmtDropAcct(char *name) {
  SAcctObj *pAcct = (SAcctObj *)sdbGetRow(tsAcctSdb, name);
  if (pAcct == NULL) {
    mWarn("account:%s is not there", name);
    return TSDB_CODE_INVALID_ACCT;
  }

  SSdbOperDesc oper = {
    .type = SDB_OPER_TYPE_GLOBAL,
    .table = tsAcctSdb,
    .pObj = pAcct
  };

  int32_t code = sdbDeleteRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_SDB_ERROR;
  }

  return 0;
}

void mgmtCleanUpAccts() {
  if (tsMgmtStatisTimer != NULL) {
    taosTmrStopA(&tsMgmtStatisTimer);
    tsMgmtStatisTimer = NULL;
  }

  sdbCloseTable(tsAcctSdb);
}

static int32_t mgmtGetAcctMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SUserObj *pUser = mgmtGetUserFromConn(pConn, NULL);
  if (pUser == NULL) return 0;

  if (strcmp(pUser->pAcct->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "created time");
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

static int32_t mgmtRetrieveAccts(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
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

static int32_t mgmtCheckAlterAcctParams(SAcctObj *pAcct, SAcctCfg *pCfg) {
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

static int32_t mgmtUpdateAcct(SAcctObj *pAcct) {
  SSdbOperDesc oper = {
    .type = SDB_OPER_TYPE_GLOBAL,
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

static int32_t mgmtAlterAcct(char *name, char *pass, SAcctCfg *pCfg) {
  SAcctObj *pAcct = NULL;

  pAcct = mgmtGetAcct(name);
  if (pAcct == NULL) {
    mTrace("account: %s not exists", name);
    return TSDB_CODE_INVALID_ACCT;
  }

  if (mgmtCheckAlterAcctParams(pAcct, pCfg) < 0) return TSDB_CODE_INVALID_OPTION;

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

  mgmtUpdateAcct(pAcct);

  return TSDB_CODE_SUCCESS;
}

static int64_t mgmtGetStatistic(SAcctObj *pAcct) {
  if (pAcct == NULL) return -1;
  
  SShowObj   pShow;
  SDnodeObj *pDnode;
  int64_t totalStorage = 0;
  int64_t pointsWritten = 0;
  TSKEY   sKey = taosGetTimestampMs();

  while (1) {
    pShow.pNode = mgmtGetNextDnode(&pShow, (SDnodeObj **)&pDnode);
    if (pDnode == NULL) break;
    for (int i = 0; i < pDnode->openVnodes; ++i) {
      SVgObj *pVgroup = mgmtGetVgroup(pDnode->vload[i].vgId);
      if (pVgroup == NULL) continue;
      if (pVgroup->pDb->pAcct == pAcct) {
        totalStorage += pDnode->vload[i].totalStorage;
        pointsWritten += pDnode->vload[i].pointsWritten;
      }
    }
  }

  pAcct->acctInfo.totalStorage = totalStorage;
  pAcct->acctInfo.numOfPointsPerSecond =
      (int32_t)((pointsWritten - pAcct->acctInfo.totalPoints) * 1000 / (sKey - pAcct->acctInfo.sKey));
  pAcct->acctInfo.sKey = sKey;
  pAcct->acctInfo.totalPoints = pointsWritten;

  if (taosLogAcctFp)
    taosLogAcctFp(pAcct->user, pAcct->acctInfo.numOfPointsPerSecond, pAcct->cfg.maxPointsPerSecond,
                  pAcct->acctInfo.numOfTimeSeries, pAcct->cfg.maxTimeSeries, pAcct->acctInfo.totalStorage,
                  pAcct->cfg.maxStorage, pAcct->acctInfo.queryTime, pAcct->cfg.maxQueryTime, pAcct->acctInfo.inblound,
                  pAcct->cfg.maxInbound, pAcct->acctInfo.outbound, pAcct->cfg.maxOutbound, pAcct->acctInfo.numOfDbs,
                  pAcct->cfg.maxDbs, pAcct->acctInfo.numOfUsers, pAcct->cfg.maxUsers, pAcct->acctInfo.numOfStreams,
                  pAcct->cfg.maxStreams, pAcct->acctInfo.numOfConns, pAcct->cfg.maxConnections,
                  pAcct->acctInfo.accessState);

  return totalStorage;
}

static void mgmtCreateRootAcct() {
  int32_t numOfAccts = sdbGetNumOfRows(tsAcctSdb);
  if (numOfAccts == 0) {
    SAcctObj *pAcct = malloc(sizeof(SAcctObj));
    memset(pAcct, 0, sizeof(SAcctObj));
    strcpy(pAcct->user, "root");
    taosEncryptPass((uint8_t*)"taosdata", strlen("taosdata"), pAcct->pass);
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
    pAcct->acctId = sdbGetId(tsAcctSdb);
    pAcct->createdTime = taosGetTimestampMs();

    SSdbOperDesc oper = {
      .type = SDB_OPER_TYPE_GLOBAL,
      .table = tsAcctSdb,
      .pObj = pAcct,
      .rowSize = sizeof(SAcctObj)
    };
    sdbInsertRow(&oper);
  }
}

static void mgmtProcessCreateAcctMsg(SQueuedMsg *pMsg) {
  SRpcMsg *rpcMsg = pMsg->pCont;
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  
  SCMCreateAcctMsg *pCreate = rpcMsg->pCont;
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

  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
    mError("account:%s, failed to create account, need redirect message", pCreate->user);
    return;
  }

  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle, NULL);
  if (pUser == NULL) {
    mError("account:%s, failed to create account, invalid user", pCreate->user);
    rpcRsp.code = TSDB_CODE_INVALID_USER;
    rpcSendResponse(&rpcRsp);
    return;
  }

  if (strcmp(pUser->user, "root") != 0) {
    mError("account:%s, failed to create account, no rights", pCreate->user);
    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
    rpcSendResponse(&rpcRsp);
    return;
  }

  int32_t code = mgmtCreateAcct(pCreate->user, pCreate->pass, &(pCreate->cfg));
  if (code == TSDB_CODE_SUCCESS) {
    mLPrint("account:%s is created by %s", pCreate->user, pUser->user);
  } else {
    mError("account:%s, failed to create account, reason:%s", pCreate->user, tstrerror(code));
  }

  rpcRsp.code = code;
  rpcSendResponse(&rpcRsp);
}

static void mgmtProcessDropAcctMsg(SQueuedMsg *pMsg) {
  SRpcMsg *rpcMsg = pMsg->pCont;
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  
  SCMDropAcctMsg *pDrop = rpcMsg->pCont;
  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
    mError("account:%s, failed to drop account, need redirect message", pDrop->user);
    return;
  }

  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle, NULL);
  if (pUser == NULL) {
    mError("account:%s, failed to drop account, invalid user", pDrop->user);
    rpcRsp.code = TSDB_CODE_INVALID_USER;
    rpcSendResponse(&rpcRsp);
    return;
  }

  if (strcmp(pUser->user, "root") != 0) {
    mError("account:%s, failed to drop account, no rights", pDrop->user);
    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
    rpcSendResponse(&rpcRsp);
    return;
  }

  int32_t code = mgmtDropAcct(pDrop->user);
  if (code == TSDB_CODE_SUCCESS) {
    mLPrint("account:%s is dropped by %s", pDrop->user, pUser->user);
  } else {
    mError("account:%s, failed to drop account, reason:%s", pDrop->user, tstrerror(code));
  }

  rpcRsp.code = code;
  rpcSendResponse(&rpcRsp);
}

static void mgmtProcessAlterAcctMsg(SQueuedMsg *pMsg) {
  SRpcMsg *rpcMsg = pMsg->pCont;
  SRpcMsg rpcRsp = {.handle = rpcMsg->handle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  
  SCMAlterAcctMsg *pAlter = rpcMsg->pCont;
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

  if (mgmtCheckRedirect(rpcMsg->handle) != TSDB_CODE_SUCCESS) {
    mError("account:%s, failed to alter account, need redirect message", pAlter->user);
    return;
  }

  SUserObj *pUser = mgmtGetUserFromConn(rpcMsg->handle, NULL);
  if (pUser == NULL) {
    mError("account:%s, failed to alter account, invalid user", pAlter->user);
    rpcRsp.code = TSDB_CODE_INVALID_USER;
    rpcSendResponse(&rpcRsp);
    return;
  }

  if (strcmp(pUser->user, "root") != 0) {
    mError("account:%s, failed to alter account, no rights", pAlter->user);
    rpcRsp.code = TSDB_CODE_NO_RIGHTS;
    rpcSendResponse(&rpcRsp);
    return;
  }

  int32_t code = mgmtAlterAcct(pAlter->user, pAlter->pass, &(pAlter->cfg));;
  if (code == TSDB_CODE_SUCCESS) {
    mLPrint("account:%s is altered by %s", pAlter->user, pUser->user);
  } else {
    mError("account:%s, failed to alter account, reason:%s", pAlter->user, tstrerror(code));
  }

  rpcRsp.code = code;
  rpcSendResponse(&rpcRsp);
}

int32_t mgmtCheckUserLimit(SAcctObj *pAcct) {
  if (pAcct->cfg.maxUsers != 0 && pAcct->acctInfo.numOfUsers >= pAcct->cfg.maxUsers) {
    mError("account:%s, users:%d exceed limit:%d", pAcct->acctId, pAcct->acctInfo.numOfUsers, pAcct->cfg.maxUsers);
    return TSDB_CODE_TOO_MANY_USERS;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtCheckDbLimit(SAcctObj *pAcct) {
  if (pAcct->cfg.maxDbs != 0 && pAcct->acctInfo.numOfDbs >= pAcct->cfg.maxDbs) {
    mError("account:%s, dbs:%d exceed limit:%d", pAcct->acctId, pAcct->acctInfo.numOfDbs, pAcct->cfg.maxDbs);
    return TSDB_CODE_TOO_MANY_DATABASES;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtCheckTableLimit(SAcctObj *pAcct) {
  if (pAcct->cfg.maxTimeSeries != 0 && pAcct->acctInfo.numOfTimeSeries >= pAcct->cfg.maxTimeSeries) {
    mError("account:%s, timeSeries:%d exceed limit:%d", pAcct->acctId, pAcct->acctInfo.numOfTimeSeries, pAcct->cfg.maxTimeSeries);
    return TSDB_CODE_NOT_ENOUGH_TIME_SERIES;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtAddDbIntoAcct(SAcctObj *pAcct, SDbObj *pDb) {
  pthread_mutex_lock(&pAcct->mutex);
  pDb->next = pAcct->pHead;
  pDb->prev = NULL;
  pDb->pAcct = pAcct;

  if (pAcct->pHead) {
    pAcct->pHead->prev = pDb;
  }

  pAcct->pHead = pDb;
  pAcct->acctInfo.numOfDbs++;
  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

int32_t mgmtRemoveDbFromAcct(SAcctObj *pAcct, SDbObj *pDb) {
  pthread_mutex_lock(&pAcct->mutex);
  if (pDb->prev) {
    pDb->prev->next = pDb->next;
  }

  if (pDb->next) {
    pDb->next->prev = pDb->prev;
  }

  if (pDb->prev == NULL) {
    pAcct->pHead = pDb->next;
  }

  pAcct->acctInfo.numOfDbs--;
  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

int32_t mgmtAddUserIntoAcct(SAcctObj *pAcct, SUserObj *pUser) {
  pthread_mutex_lock(&pAcct->mutex);
  pUser->next = pAcct->pUser;
  pUser->prev = NULL;

  if (pAcct->pUser) {
    pAcct->pUser->prev = pUser;
  }

  pAcct->pUser = pUser;
  pAcct->acctInfo.numOfUsers++;
  pUser->pAcct = pAcct;
  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}

int32_t mgmtRemoveUserFromAcct(SAcctObj *pAcct, SUserObj *pUser) {
  pthread_mutex_lock(&pAcct->mutex);
  if (pUser->prev) {
    pUser->prev->next = pUser->next;
  }

  if (pUser->next) {
    pUser->next->prev = pUser->prev;
  }

  if (pUser->prev == NULL) {
    pAcct->pUser = pUser->next;
  }

  pAcct->acctInfo.numOfUsers--;
  pthread_mutex_unlock(&pAcct->mutex);

  return 0;
}