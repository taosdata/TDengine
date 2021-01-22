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
#include "tglobal.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeAcct.h"
#include "mnodeDb.h"
#include "mnodeSdb.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"

int64_t tsAcctRid = -1;
void *  tsAcctSdb = NULL;
static int32_t tsAcctUpdateSize;
static int32_t mnodeCreateRootAcct();

static int32_t mnodeAcctActionDestroy(SSdbRow *pRow) {
  SAcctObj *pAcct = pRow->pObj;
  pthread_mutex_destroy(&pAcct->mutex);
  tfree(pRow->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeAcctActionInsert(SSdbRow *pRow) {
  SAcctObj *pAcct = pRow->pObj;
  memset(&pAcct->acctInfo, 0, sizeof(SAcctInfo));
  pAcct->acctInfo.accessState = TSDB_VN_ALL_ACCCESS;
  pthread_mutex_init(&pAcct->mutex, NULL);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeAcctActionDelete(SSdbRow *pRow) {
  SAcctObj *pAcct = pRow->pObj;
  mnodeDropAllUsers(pAcct);
  mnodeDropAllDbs(pAcct);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeAcctActionUpdate(SSdbRow *pRow) {
  SAcctObj *pAcct = pRow->pObj;
  SAcctObj *pSaved = mnodeGetAcct(pAcct->user);
  if (pAcct != pSaved) {
    memcpy(pSaved, pAcct, tsAcctUpdateSize);
    free(pAcct);
  }
  mnodeDecAcctRef(pSaved);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeAcctActionEncode(SSdbRow *pRow) {
  SAcctObj *pAcct = pRow->pObj;
  memcpy(pRow->rowData, pAcct, tsAcctUpdateSize);
  pRow->rowSize = tsAcctUpdateSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeAcctActionDecode(SSdbRow *pRow) {
  SAcctObj *pAcct = (SAcctObj *) calloc(1, sizeof(SAcctObj));
  if (pAcct == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

  memcpy(pAcct, pRow->rowData, tsAcctUpdateSize);
  pRow->pObj = pAcct;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeAcctActionRestored() {
  int64_t numOfRows = sdbGetNumOfRows(tsAcctSdb);
  if (numOfRows <= 0 && dnodeIsFirstDeploy()) {
    mInfo("dnode first deploy, create root acct");
    int32_t code = mnodeCreateRootAcct();
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
      mError("failed to create root account, reason:%s", tstrerror(code));
      return code;
    }
  }

  acctInit();
  return TSDB_CODE_SUCCESS;
}

int32_t mnodeInitAccts() {
  SAcctObj tObj;
  tsAcctUpdateSize = (int32_t)((int8_t *)tObj.updateEnd - (int8_t *)&tObj);

  SSdbTableDesc desc = {
    .id           = SDB_TABLE_ACCOUNT,
    .name         = "accounts",
    .hashSessions = TSDB_DEFAULT_ACCOUNTS_HASH_SIZE,
    .maxRowSize   = tsAcctUpdateSize,
    .refCountPos  = (int32_t)((int8_t *)(&tObj.refCount) - (int8_t *)&tObj),
    .keyType      = SDB_KEY_STRING,
    .fpInsert     = mnodeAcctActionInsert,
    .fpDelete     = mnodeAcctActionDelete,
    .fpUpdate     = mnodeAcctActionUpdate,
    .fpEncode     = mnodeAcctActionEncode,
    .fpDecode     = mnodeAcctActionDecode,
    .fpDestroy    = mnodeAcctActionDestroy,
    .fpRestored   = mnodeAcctActionRestored
  };

  tsAcctRid = sdbOpenTable(&desc);
  tsAcctSdb = sdbGetTableByRid(tsAcctRid);
  if (tsAcctSdb == NULL) {
    mError("table:%s, failed to create hash", desc.name);
    return -1;
  }

  mDebug("table:%s, hash is created", desc.name);
  return TSDB_CODE_SUCCESS;
}

void mnodeCleanupAccts() {
  acctCleanUp();
  sdbCloseTable(tsAcctRid);
  tsAcctSdb = NULL;
}

void mnodeGetStatOfAllAcct(SAcctInfo* pAcctInfo) {
  memset(pAcctInfo, 0, sizeof(*pAcctInfo));

  void   *pIter = NULL;
  SAcctObj *pAcct = NULL;
  while (1) {
    pIter = mnodeGetNextAcct(pIter, &pAcct);
    if (pAcct == NULL) {
      break;
    }
    pAcctInfo->numOfDbs += pAcct->acctInfo.numOfDbs;
    pAcctInfo->numOfTimeSeries += pAcct->acctInfo.numOfTimeSeries;
    mnodeDecAcctRef(pAcct);
  }

  SVgObj *pVgroup = NULL;
  pIter = NULL;
  while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) {
       break;
    }
    pAcctInfo->totalStorage += pVgroup->totalStorage;
    pAcctInfo->compStorage += pVgroup->compStorage;
    pAcctInfo->totalPoints += pVgroup->pointsWritten;
    mnodeDecVgroupRef(pVgroup);
  }
}

void *mnodeGetAcct(char *name) {
  return sdbGetRow(tsAcctSdb, name);
}

void *mnodeGetNextAcct(void *pIter, SAcctObj **pAcct) {
  return sdbFetchRow(tsAcctSdb, pIter, (void **)pAcct); 
}

void mnodeCancelGetNextAcct(void *pIter) {
  sdbFreeIter(tsAcctSdb, pIter);
}

void mnodeIncAcctRef(SAcctObj *pAcct) {
  sdbIncRef(tsAcctSdb, pAcct);
}

void mnodeDecAcctRef(SAcctObj *pAcct) {
  sdbDecRef(tsAcctSdb, pAcct);
}

void mnodeAddDbToAcct(SAcctObj *pAcct, SDbObj *pDb) {
  atomic_add_fetch_32(&pAcct->acctInfo.numOfDbs, 1);
  pDb->pAcct = pAcct;
  mnodeIncAcctRef(pAcct);
}

void mnodeDropDbFromAcct(SAcctObj *pAcct, SDbObj *pDb) {
  atomic_sub_fetch_32(&pAcct->acctInfo.numOfDbs, 1);
  pDb->pAcct = NULL;
  mnodeDecAcctRef(pAcct);
}

void mnodeAddUserToAcct(SAcctObj *pAcct, SUserObj *pUser) {
  atomic_add_fetch_32(&pAcct->acctInfo.numOfUsers, 1);
  pUser->pAcct = pAcct;
  mnodeIncAcctRef(pAcct);
}

void mnodeDropUserFromAcct(SAcctObj *pAcct, SUserObj *pUser) {
  atomic_sub_fetch_32(&pAcct->acctInfo.numOfUsers, 1);
  pUser->pAcct = NULL;
  mnodeDecAcctRef(pAcct);
}

static int32_t mnodeCreateRootAcct() {
  int64_t numOfAccts = sdbGetNumOfRows(tsAcctSdb);
  if (numOfAccts != 0) return TSDB_CODE_SUCCESS;

  SAcctObj *pAcct = malloc(sizeof(SAcctObj));
  memset(pAcct, 0, sizeof(SAcctObj));
  strcpy(pAcct->user, TSDB_DEFAULT_USER);
  taosEncryptPass((uint8_t *)TSDB_DEFAULT_PASS, strlen(TSDB_DEFAULT_PASS), pAcct->pass);
  pAcct->cfg = (SAcctCfg){
    .maxUsers           = 128,
    .maxDbs             = 128,
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

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsAcctSdb,
    .pObj   = pAcct,
  };

  return sdbInsertRow(&row);
}

#ifndef _ACCT

int32_t acctInit() { return TSDB_CODE_SUCCESS; }
void    acctCleanUp() {}
int32_t acctCheck(void *pAcct, EAcctGrantType type) { return TSDB_CODE_SUCCESS; }

#endif
