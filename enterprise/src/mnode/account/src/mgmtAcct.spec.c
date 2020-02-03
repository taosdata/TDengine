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
#include <arpa/inet.h>

#include "mgmt.h"
#include "tschemautil.h"

#define TSDB_MIN_USERS_PER_ACCT 2
#define TSDB_MAX_USERS_PER_ACCT 10
#define TSDB_MIN_DBS_PER_ACCT 1
#define TSDB_MAX_DBS_PER_ACCT 64
#define TSDB_MIN_TIMESERIES_PER_ACCT 10
#define TSDB_MAX_TIMESERIES_PER_ACCT INT32_MAX
#define TSDB_MIN_CONNECTIONS_PER_ACCT 10
#define TSDB_MAX_CONNECTIONS_PER_ACCT 1024
#define TSDB_MIN_STREAMS_PER_ACCT 10
#define TSDB_MAX_STREAMS_PER_ACCT 1000
#define TSDB_MIN_SPOINTS_PER_ACCT 5000
#define TSDB_MAX_SPOINTS_PER_ACCT 10000000
#define TSDB_MIN_STORAGE_PER_ACCT 0  // 1G
#define TSDB_MAX_STORAGE_PER_ACCT INT64_MAX
#define TSDB_MIN_QUERYTIME_PER_ACCT 3600  // 1 hour
#define TSDB_MAX_QUERYTIME_PER_ACCT INT64_MAX

void *       acctSdb = NULL;
extern void *userSdb;
extern void *dbSdb;
int          tsAcctUpdateSize;

void *(*mgmtAcctActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int size, int *ssize);
void *mgmtAcctActionInsert(void *row, char *str, int size, int *ssize);
void *mgmtAcctActionDelete(void *row, char *str, int size, int *ssize);
void *mgmtAcctActionUpdate(void *row, char *str, int size, int *ssize);
void *mgmtAcctActionEncode(void *row, char *str, int size, int *ssize);
void *mgmtAcctActionDecode(void *row, char *str, int size, int *ssize);
void *mgmtAcctActionBeforeBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtAcctActionBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtAcctActionAfterBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtAcctActionReset(void *row, char *str, int size, int *ssize);
void *mgmtAcctActionDestroy(void *row, char *str, int size, int *ssize);

void mgmtAcctActionInit() {
  mgmtAcctActionFp[SDB_TYPE_INSERT] = mgmtAcctActionInsert;
  mgmtAcctActionFp[SDB_TYPE_DELETE] = mgmtAcctActionDelete;
  mgmtAcctActionFp[SDB_TYPE_UPDATE] = mgmtAcctActionUpdate;
  mgmtAcctActionFp[SDB_TYPE_ENCODE] = mgmtAcctActionEncode;
  mgmtAcctActionFp[SDB_TYPE_DECODE] = mgmtAcctActionDecode;
  mgmtAcctActionFp[SDB_TYPE_BEFORE_BATCH_UPDATE] = mgmtAcctActionBeforeBatchUpdate;
  mgmtAcctActionFp[SDB_TYPE_BATCH_UPDATE] = mgmtAcctActionBatchUpdate;
  mgmtAcctActionFp[SDB_TYPE_AFTER_BATCH_UPDATE] = mgmtAcctActionAfterBatchUpdate;
  mgmtAcctActionFp[SDB_TYPE_RESET] = mgmtAcctActionReset;
  mgmtAcctActionFp[SDB_TYPE_DESTROY] = mgmtAcctActionDestroy;
}

void *mgmtAcctAction(char action, void *row, char *str, int size, int *ssize) {
  if (mgmtAcctActionFp[(uint8_t)action] != NULL) {
    return (*(mgmtAcctActionFp[(uint8_t)action]))(row, str, size, ssize);
  }
  return NULL;
}

int mgmtInitAccts() {
  void *    pNode = NULL;
  SAcctObj *pAcct = NULL;
  int       numOfAccts = 0;

  mgmtAcctActionInit();

  acctSdb = sdbOpenTable(tsMaxAccounts, sizeof(SAcctObj), "account", SDB_KEYTYPE_STRING, mgmtDirectory, mgmtAcctAction);
  if (acctSdb == NULL) {
    mError("failed to init account data");
    return -1;
  }

  while (1) {
    pNode = sdbFetchRow(acctSdb, pNode, (void **)&pAcct);
    if (pAcct == NULL) break;

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
    numOfAccts++;
  }

  SAcctObj tObj;
  tsAcctUpdateSize = tObj.updateEnd - (char *)&tObj;

  mTrace("account is initialized");
  return 0;
}

SAcctObj *mgmtGetAcct(char *name) { return (SAcctObj *)sdbGetRow(acctSdb, name); }

int mgmtCheckUserLimit(SAcctObj *pAcct) {
  int numOfUsers = sdbGetNumOfRows(userSdb);
  if (numOfUsers >= tsMaxUsers || pAcct->acctInfo.numOfUsers >= pAcct->cfg.maxUsers) {
    mWarn("numOfUsers:%d, exceed tsMaxUsers:%d or account numOfUsers: %d, exceed account maxUsers: %d",
          numOfUsers, tsMaxUsers, pAcct->acctInfo.numOfUsers, pAcct->cfg.maxUsers);
    return TSDB_CODE_TOO_MANY_USERS;
  }
  return 0;
}

int mgmtCheckDbLimit(SAcctObj *pAcct) {
  int numOfDbs = sdbGetNumOfRows(dbSdb);
  if (numOfDbs >= tsMaxDbs || pAcct->acctInfo.numOfDbs >= pAcct->cfg.maxDbs) {
    mWarn("numOfDbs:%d, exceed tsMaxDbs:%d or account numOfDbs: %d, exceed account maxDbs:%d",
          numOfDbs, tsMaxDbs, pAcct->acctInfo.numOfDbs, pAcct->cfg.maxDbs);
    return TSDB_CODE_TOO_MANY_DATABSES;
  }
  return 0;
}

int mgmtCheckMeterLimit(SAcctObj *pAcct, SCreateTableMsg *pCreate) {
  if (pAcct->acctInfo.numOfTimeSeries + pCreate->numOfColumns - 1 > pAcct->cfg.maxTimeSeries) {
    mWarn("Time series is not enough, account numOfTimeSeries: %d, account maxTimeSeries: %d, required time series: %d",
          pAcct->acctInfo.numOfTimeSeries, pAcct->cfg.maxTimeSeries, pCreate->numOfColumns);
    return TSDB_CODE_NOT_ENOUGH_TIME_SERIES;
  }
  return 0;
}

int mgmtCheckUserGrant() {
  return grantCheckUsers();
}

int mgmtCheckDbGrant() {
  return grantCheckDatabases();
}

int mgmtCheckMeterGrant(SCreateTableMsg *pCreate, STabObj * pMeter) {
  if (grantCheckExpired()) {
    mError("failed to create meter:%s, reason:grant expired", pMeter->meterId);
    return TSDB_CODE_GRANT_EXPIRED;
  }

  if (pCreate->numOfTags == 0) {
    int grantCode = grantCheckTimeSeries(pMeter->numOfColumns);
    if (grantCode != 0) return grantCode;
  }

  return 0;
}

int mgmtCheckAcctParams(SAcctCfg *pCfg) {
  // TODO
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

int mgmtCreateAcct(char *name, char *pass, SAcctCfg *pCfg) {
  SAcctObj *pAcct;

  int numOfAccts = sdbGetNumOfRows(acctSdb);
  if (numOfAccts >= tsMaxAccounts) {
    mWarn("numOfAccts:%d, exceed tsMaxAccounts:%d", numOfAccts, tsMaxAccounts);
    return TSDB_CODE_TOO_MANY_ACCTS;
  }

  pAcct = (SAcctObj *)sdbGetRow(acctSdb, name);
  if (pAcct != NULL) {
    return TSDB_CODE_ACCT_ALREADY_EXIST;
  }

  int numOfUsers = sdbGetNumOfRows(userSdb);
  if (numOfUsers >= tsMaxUsers) {
    mWarn("numOfUsers:%d, exceed tsMaxUsers:%d", numOfUsers, tsMaxUsers);
    return TSDB_CODE_TOO_MANY_USERS;
  }

  SUserObj *pUser = (SUserObj *)sdbGetRow(userSdb, name);
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
  pAcct->acctId = sdbGetId(acctSdb);
  pAcct->createdTime = taosGetTimestampMs();

  int grantCode = grantCheckAccts();
  if (grantCode != 0) return grantCode;

  int code = TSDB_CODE_SUCCESS;
  if (sdbInsertRow(acctSdb, pAcct, 0) < 0) {
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

int mgmtDropAcct(char *name) {
  SAcctObj *pAcct;

  pAcct = (SAcctObj *)sdbGetRow(acctSdb, name);
  if (pAcct == NULL) {
    mWarn("account:%s is not there", name);
    return TSDB_CODE_INVALID_ACCT;
  }

  while (pAcct->pHead) {
    if (mgmtDropDb(pAcct->pHead) != TSDB_CODE_SUCCESS) return TSDB_CODE_ACTION_IN_PROGRESS;
  }

  while (pAcct->pUser) mgmtDropUser(pAcct, pAcct->pUser->user);

  pthread_mutex_destroy(&pAcct->mutex);
  sdbDeleteRow(acctSdb, pAcct);

  return 0;
}

void mgmtCheckAcct() {
  int numOfRows = 0;

  numOfRows = sdbGetNumOfRows(acctSdb);

  if (numOfRows == 0) {
    mTrace("no any accounts, create the root acct");
    mgmtCreateAcct("root", "taosdata", NULL);

    SAcctObj *pAcct = mgmtGetAcct("root");
    mgmtCreateUser(pAcct, "monitor", tsInternalPass);
    mgmtCreateUser(pAcct, "_root", tsInternalPass);
  }
}

void mgmtCleanUpAccts() { sdbCloseTable(acctSdb); }

int mgmtGetAcctMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int cols = 0;

  if (strcmp(pConn->pAcct->user, "root") != 0) return TSDB_CODE_NO_RIGHTS;

  pShow->bytes[cols] = TSDB_METER_NAME_LEN;
  SSchema *pSchema = tsGetSchema(pMeta);

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
  for (int i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = sdbGetNumOfRows(acctSdb);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

int mgmtRetrieveAccts(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int       numOfRows = 0;
  SAcctObj *pAcct = NULL;
  char *    pWrite;
  int       cols = 0;

  while (numOfRows < rows) {
    pShow->pNode = sdbFetchRow(acctSdb, pShow->pNode, (void **)&pAcct);
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

void *mgmtAcctActionInsert(void *row, char *str, int size, int *ssize) {
  SAcctObj *pAcct = (SAcctObj *)row;
  pAcct->pHead = NULL;
  pAcct->pUser = NULL;
  pAcct->acctInfo.numOfUsers = 0;
  pAcct->acctInfo.numOfDbs = 0;

  return NULL;
}
void *mgmtAcctActionDelete(void *row, char *str, int size, int *ssize) { return NULL; }
void *mgmtAcctActionUpdate(void *row, char *str, int size, int *ssize) {
  return mgmtAcctActionReset(row, str, size, ssize);
}
void *mgmtAcctActionEncode(void *row, char *str, int size, int *ssize) {
  SAcctObj *pAcct = (SAcctObj *)row;
  int       tsize = pAcct->updateEnd - (char *)pAcct;
  if (size < tsize) {
    *ssize = -1;
  } else {
    memcpy(str, pAcct, tsize);
    *ssize = tsize;
  }

  return NULL;
}
void *mgmtAcctActionDecode(void *row, char *str, int size, int *ssize) {
  SAcctObj *pAcct = (SAcctObj *)malloc(sizeof(SAcctObj));
  if (pAcct == NULL) return NULL;
  memset(pAcct, 0, sizeof(SAcctObj));

  int tsize = pAcct->updateEnd - (char *)pAcct;
  memcpy(pAcct, str, tsize);
  return (void *)pAcct;
}
void *mgmtAcctActionBeforeBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }
void *mgmtAcctActionBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }
void *mgmtAcctActionAfterBatchUpdate(void *row, char *str, int size, int *ssize) { return NULL; }
void *mgmtAcctActionReset(void *row, char *str, int size, int *ssize) {
  SAcctObj *pAcct = (SAcctObj *)row;
  int       tsize = pAcct->updateEnd - (char *)pAcct;
  memcpy(pAcct, str, tsize);
  return NULL;
}
void *mgmtAcctActionDestroy(void *row, char *str, int size, int *ssize) {
  tfree(row);
  return NULL;
}

int mgmtCheckAlterAcctParams(SAcctObj *pAcct, SAcctCfg *pCfg) {
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

int mgmtUpdateAcct(SAcctObj *pAcct) { return sdbUpdateRow(acctSdb, pAcct, 0, 1); }

int mgmtAlterAcct(char *name, char *pass, SAcctCfg *pCfg) {
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

  // TODO : Update account on disk
  mgmtUpdateAcct(pAcct);

  return TSDB_CODE_SUCCESS;
}

int64_t mgmtGetAcctStatistic(SAcctObj *pAcct) {
  TSKEY   sKey;
  int64_t totalStorage = 0;
  int64_t pointsWritten = 0;

  SDbObj *pDb = NULL;
  SVgObj *pVgroup = NULL;

  if (pAcct == NULL) return -1;
  pDb = pAcct->pHead;

  sKey = taosGetTimestampMs();

  while (pDb != NULL) {
    pVgroup = pDb->pHead;
    while (pVgroup != NULL) {
      // TODO:
      for (int i = 0; i < pVgroup->numOfVnodes; i++) {
        SDnodeObj *pDnode = mgmtGetDnode(pVgroup->vnodeGid[i].ip);
        if (pDnode == NULL) continue;
        totalStorage += pDnode->vload[pVgroup->vnodeGid[i].vnode].totalStorage;
        pointsWritten += pDnode->vload[pVgroup->vnodeGid[i].vnode].pointsWritten;
        if (pDnode != NULL) continue;
      }
      pVgroup = pVgroup->next;
    }
    pDb = pDb->next;
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

void mgmtCreateRootAcct() {
  SAcctObj *pAcct;

  int numOfAccts = sdbGetNumOfRows(acctSdb);
  int numOfUsers = sdbGetNumOfRows(userSdb);

  if (numOfAccts == 0 && numOfUsers > 0) {
    pAcct = malloc(sizeof(SAcctObj));
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
    pAcct->acctId = sdbGetId(acctSdb);
    pAcct->createdTime = taosGetTimestampMs();
    sdbInsertRow(acctSdb, pAcct, 0);
  }
}
