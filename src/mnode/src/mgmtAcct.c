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
#include "taoserror.h"
#include "ttime.h"
#include "tutil.h"
#include "dnode.h"
#include "mgmtDef.h"
#include "mgmtInt.h"
#include "mgmtAcct.h"
#include "mgmtDb.h"
#include "mgmtSdb.h"
#include "mgmtUser.h"

void *  tsAcctSdb = NULL;
static int32_t tsAcctUpdateSize;
static void    mgmtCreateRootAcct();

static int32_t mgmtActionAcctDestroy(SSdbOper *pOper) {
  SAcctObj *pAcct = pOper->pObj;
  pthread_mutex_destroy(&pAcct->mutex);
  tfree(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtAcctActionInsert(SSdbOper *pOper) {
  SAcctObj *pAcct = pOper->pObj;
  memset(&pAcct->acctInfo, 0, sizeof(SAcctInfo));
  pthread_mutex_init(&pAcct->mutex, NULL);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtActionAcctDelete(SSdbOper *pOper) {
  SAcctObj *pAcct = pOper->pObj;
  mgmtDropAllUsers(pAcct);
  mgmtDropAllDbs(pAcct);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtActionAcctUpdate(SSdbOper *pOper) {
  SAcctObj *pAcct = pOper->pObj;
  SAcctObj *pSaved = mgmtGetAcct(pAcct->user);
  if (pAcct != pSaved) {
    memcpy(pSaved, pAcct, tsAcctUpdateSize);
    free(pAcct);
  }
  mgmtDecAcctRef(pSaved);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtActionActionEncode(SSdbOper *pOper) {
  SAcctObj *pAcct = pOper->pObj;
  memcpy(pOper->rowData, pAcct, tsAcctUpdateSize);
  pOper->rowSize = tsAcctUpdateSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtActionAcctDecode(SSdbOper *pOper) {
  SAcctObj *pAcct = (SAcctObj *) calloc(1, sizeof(SAcctObj));
  if (pAcct == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

  memcpy(pAcct, pOper->rowData, tsAcctUpdateSize);
  pOper->pObj = pAcct;
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtActionAcctRestored() {
  if (dnodeIsFirstDeploy()) {
    mgmtCreateRootAcct();
  }

  acctInit();
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtInitAccts() {
  SAcctObj tObj;
  tsAcctUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableId      = SDB_TABLE_ACCOUNT,
    .tableName    = "accounts",
    .hashSessions = TSDB_DEFAULT_ACCOUNTS_HASH_SIZE,
    .maxRowSize   = tsAcctUpdateSize,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_STRING,
    .insertFp     = mgmtAcctActionInsert,
    .deleteFp     = mgmtActionAcctDelete,
    .updateFp     = mgmtActionAcctUpdate,
    .encodeFp     = mgmtActionActionEncode,
    .decodeFp     = mgmtActionAcctDecode,
    .destroyFp    = mgmtActionAcctDestroy,
    .restoredFp   = mgmtActionAcctRestored
  };

  tsAcctSdb = sdbOpenTable(&tableDesc);
  if (tsAcctSdb == NULL) {
    mError("table:%s, failed to create hash", tableDesc.tableName);
    return -1;
  }

  mTrace("table:%s, hash is created", tableDesc.tableName);
  return TSDB_CODE_SUCCESS;
}

void mgmtCleanUpAccts() {
  sdbCloseTable(tsAcctSdb);
  acctCleanUp();
}

void *mgmtGetAcct(char *name) {
  return sdbGetRow(tsAcctSdb, name);
}

void *mgmtGetNextAcct(void *pIter, SAcctObj **pAcct) {
  return sdbFetchRow(tsAcctSdb, pIter, (void **)pAcct); 
}

void mgmtIncAcctRef(SAcctObj *pAcct) {
  sdbIncRef(tsAcctSdb, pAcct);
}

void mgmtDecAcctRef(SAcctObj *pAcct) {
  sdbDecRef(tsAcctSdb, pAcct);
}

void mgmtAddDbToAcct(SAcctObj *pAcct, SDbObj *pDb) {
  atomic_add_fetch_32(&pAcct->acctInfo.numOfDbs, 1);
  pDb->pAcct = pAcct;
  mgmtIncAcctRef(pAcct);
}

void mgmtDropDbFromAcct(SAcctObj *pAcct, SDbObj *pDb) {
  atomic_sub_fetch_32(&pAcct->acctInfo.numOfDbs, 1);
  pDb->pAcct = NULL;
  mgmtDecAcctRef(pAcct);
}

void mgmtAddUserToAcct(SAcctObj *pAcct, SUserObj *pUser) {
  atomic_add_fetch_32(&pAcct->acctInfo.numOfUsers, 1);
  pUser->pAcct = pAcct;
  mgmtIncAcctRef(pAcct);
}

void mgmtDropUserFromAcct(SAcctObj *pAcct, SUserObj *pUser) {
  atomic_sub_fetch_32(&pAcct->acctInfo.numOfUsers, 1);
  pUser->pAcct = NULL;
  mgmtDecAcctRef(pAcct);
}

static void mgmtCreateRootAcct() {
  int32_t numOfAccts = sdbGetNumOfRows(tsAcctSdb);
  if (numOfAccts != 0) return;

  SAcctObj *pAcct = malloc(sizeof(SAcctObj));
  memset(pAcct, 0, sizeof(SAcctObj));
  strcpy(pAcct->user, "root");
  taosEncryptPass((uint8_t*)"taosdata", strlen("taosdata"), pAcct->pass);
  pAcct->cfg = (SAcctCfg){
    .maxUsers           = 10,
    .maxDbs             = 64,
    .maxTimeSeries      = INT32_MAX,
    .maxConnections     = 1024,
    .maxStreams         = 1000,
    .maxPointsPerSecond = 10000000,
    .maxStorage         = INT64_MAX,
    .maxQueryTime       = INT64_MAX,
    .maxInbound         = 0,
    .maxOutbound        = 0,
    .accessState        = TSDB_VN_ALL_ACCCESS
  };
  pAcct->acctId = sdbGetId(tsAcctSdb);
  pAcct->createdTime = taosGetTimestampMs();

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsAcctSdb,
    .pObj = pAcct,
  };
  sdbInsertRow(&oper);
}

#ifndef _ACCT

int32_t acctInit() { return TSDB_CODE_SUCCESS; }
void    acctCleanUp() {}
int32_t acctCheck(void *pAcct, EAcctGrantType type) { return TSDB_CODE_SUCCESS; }

#endif