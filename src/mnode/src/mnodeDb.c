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
#include "tutil.h"
#include "tgrant.h"
#include "tglobal.h"
#include "tname.h"
#include "tbn.h"
#include "tdataformat.h"
#include "mnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeAcct.h"
#include "mnodeDb.h"
#include "mnodeDnode.h"
#include "mnodeMnode.h"
#include "mnodeProfile.h"
#include "mnodeWrite.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeTable.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"

#define VG_LIST_SIZE 8
int64_t        tsDbRid = -1;
static void *  tsDbSdb = NULL;
static int32_t tsDbUpdateSize;

static int32_t mnodeCreateDb(SAcctObj *pAcct, SCreateDbMsg *pCreate, SMnodeMsg *pMsg);
static int32_t mnodeDropDb(SMnodeMsg *newMsg);
static int32_t mnodeSetDbDropping(SDbObj *pDb);
static int32_t mnodeGetDbMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveDbs(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mnodeProcessCreateDbMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessAlterDbMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessDropDbMsg(SMnodeMsg *pMsg);

static void mnodeDestroyDb(SDbObj *pDb) {
  pthread_mutex_destroy(&pDb->mutex);
  tfree(pDb->vgList);
  tfree(pDb);
}

static int32_t mnodeDbActionDestroy(SSdbRow *pRow) {
  mnodeDestroyDb(pRow->pObj);
  return TSDB_CODE_SUCCESS;
}

int64_t mnodeGetDbNum() {
  return sdbGetNumOfRows(tsDbSdb);
}

static int32_t mnodeDbActionInsert(SSdbRow *pRow) {
  SDbObj *pDb = pRow->pObj;
  SAcctObj *pAcct = mnodeGetAcct(pDb->acct);

  pthread_mutex_init(&pDb->mutex, NULL);
  pthread_mutex_lock(&pDb->mutex);
  pDb->vgListSize = VG_LIST_SIZE;
  pDb->vgList = calloc(pDb->vgListSize, sizeof(SVgObj *));
  pDb->numOfVgroups = 0;
  pthread_mutex_unlock(&pDb->mutex);

  pDb->numOfTables = 0;
  pDb->numOfSuperTables = 0;

  if (pAcct != NULL) {
    mnodeAddDbToAcct(pAcct, pDb);
    mnodeDecAcctRef(pAcct);
  }
  else {
    mError("db:%s, acct:%s info not exist in sdb", pDb->name, pDb->acct);
    return TSDB_CODE_MND_INVALID_ACCT;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDbActionDelete(SSdbRow *pRow) {
  SDbObj *pDb = pRow->pObj;
  SAcctObj *pAcct = mnodeGetAcct(pDb->acct);

  mnodeDropAllChildTables(pDb);
  mnodeDropAllSuperTables(pDb);
  mnodeDropAllDbVgroups(pDb);

  if (pAcct) {
    mnodeDropDbFromAcct(pAcct, pDb);
    mnodeDecAcctRef(pAcct);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDbActionUpdate(SSdbRow *pRow) {
  SDbObj *pNew = pRow->pObj;
  SDbObj *pDb = mnodeGetDb(pNew->name);
  if (pDb != NULL && pNew != pDb) {
    memcpy(pDb, pNew, pRow->rowSize);
    free(pNew->vgList);
    free(pNew);
  }
  //mnodeUpdateAllDbVgroups(pDb);
  mnodeDecDbRef(pDb);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDbActionEncode(SSdbRow *pRow) {
  SDbObj *pDb = pRow->pObj;
  memcpy(pRow->rowData, pDb, tsDbUpdateSize);
  pRow->rowSize = tsDbUpdateSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDbActionDecode(SSdbRow *pRow) {
  SDbObj *pDb = (SDbObj *) calloc(1, sizeof(SDbObj));
  if (pDb == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;
  
  memcpy(pDb, pRow->rowData, tsDbUpdateSize);
  pRow->pObj = pDb;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDbActionRestored() {
  return 0;
}

int32_t mnodeInitDbs() {
  SDbObj tObj;
  tsDbUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc desc = {
    .id           = SDB_TABLE_DB,
    .name         = "dbs",
    .hashSessions = TSDB_DEFAULT_DBS_HASH_SIZE,
    .maxRowSize   = tsDbUpdateSize,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_STRING,
    .fpInsert     = mnodeDbActionInsert,
    .fpDelete     = mnodeDbActionDelete,
    .fpUpdate     = mnodeDbActionUpdate,
    .fpEncode     = mnodeDbActionEncode,
    .fpDecode     = mnodeDbActionDecode,
    .fpDestroy    = mnodeDbActionDestroy,
    .fpRestored   = mnodeDbActionRestored
  };

  tsDbRid = sdbOpenTable(&desc);
  tsDbSdb = sdbGetTableByRid(tsDbRid);
  if (tsDbSdb == NULL) {
    mError("failed to init db data");
    return -1;
  }

  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_CREATE_DB, mnodeProcessCreateDbMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_ALTER_DB, mnodeProcessAlterDbMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_DROP_DB, mnodeProcessDropDbMsg);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_DB, mnodeGetDbMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_DB, mnodeRetrieveDbs);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_DB, mnodeCancelGetNextDb);
  
  mDebug("table:dbs table is created");
  return 0;
}

void *mnodeGetNextDb(void *pIter, SDbObj **pDb) {
  return sdbFetchRow(tsDbSdb, pIter, (void **)pDb);
}

void mnodeCancelGetNextDb(void *pIter) {
  sdbFreeIter(tsDbSdb, pIter);
}

SDbObj *mnodeGetDb(char *db) {
  return (SDbObj *)sdbGetRow(tsDbSdb, db);
}

void mnodeIncDbRef(SDbObj *pDb) {
  return sdbIncRef(tsDbSdb, pDb); 
}

void mnodeDecDbRef(SDbObj *pDb) { 
  return sdbDecRef(tsDbSdb, pDb); 
}

SDbObj *mnodeGetDbByTableName(char *tableName) {
  SName name = {0};
  tNameFromString(&name, tableName, T_NAME_ACCT|T_NAME_DB|T_NAME_TABLE);

  // validate the tableName?
  char db[TSDB_TABLE_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, db);

  return mnodeGetDb(db);
}

static int32_t mnodeCheckDbCfg(SDbCfg *pCfg) {
  if (pCfg->cacheBlockSize < TSDB_MIN_CACHE_BLOCK_SIZE || pCfg->cacheBlockSize > TSDB_MAX_CACHE_BLOCK_SIZE) {
    mError("invalid db option cacheBlockSize:%d valid range: [%d, %d]", pCfg->cacheBlockSize, TSDB_MIN_CACHE_BLOCK_SIZE,
           TSDB_MAX_CACHE_BLOCK_SIZE);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->totalBlocks < TSDB_MIN_TOTAL_BLOCKS || pCfg->totalBlocks > TSDB_MAX_TOTAL_BLOCKS) {
    mError("invalid db option totalBlocks:%d valid range: [%d, %d]", pCfg->totalBlocks, TSDB_MIN_TOTAL_BLOCKS,
           TSDB_MAX_TOTAL_BLOCKS);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->maxTables < TSDB_MIN_TABLES || pCfg->maxTables > TSDB_MAX_TABLES) {
    mError("invalid db option maxTables:%d valid range: [%d, %d]", pCfg->maxTables, TSDB_MIN_TABLES, TSDB_MAX_TABLES);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->daysPerFile < TSDB_MIN_DAYS_PER_FILE || pCfg->daysPerFile > TSDB_MAX_DAYS_PER_FILE) {
    mError("invalid db option daysPerFile:%d valid range: [%d, %d]", pCfg->daysPerFile, TSDB_MIN_DAYS_PER_FILE,
           TSDB_MAX_DAYS_PER_FILE);
    return TSDB_CODE_MND_INVALID_DB_OPTION_DAYS;
  }

  if (pCfg->daysToKeep < TSDB_MIN_KEEP || pCfg->daysToKeep > TSDB_MAX_KEEP) {
    mError("invalid db option daysToKeep:%d valid range: [%d, %d]", pCfg->daysToKeep, TSDB_MIN_KEEP, TSDB_MAX_KEEP);
    return TSDB_CODE_MND_INVALID_DB_OPTION_KEEP;
  }

  if (pCfg->daysToKeep < pCfg->daysPerFile) {
    mError("invalid db option daysToKeep:%d should larger than daysPerFile:%d", pCfg->daysToKeep, pCfg->daysPerFile);
    return TSDB_CODE_MND_INVALID_DB_OPTION_KEEP;
  }

  if (pCfg->daysToKeep2 < TSDB_MIN_KEEP || pCfg->daysToKeep2 > pCfg->daysToKeep) {
    mError("invalid db option daysToKeep2:%d valid range: [%d, %d]", pCfg->daysToKeep2, TSDB_MIN_KEEP, pCfg->daysToKeep);
    return TSDB_CODE_MND_INVALID_DB_OPTION_KEEP;
  }

  if (pCfg->daysToKeep1 < TSDB_MIN_KEEP || pCfg->daysToKeep1 > pCfg->daysToKeep2) {
    mError("invalid db option daysToKeep1:%d valid range: [%d, %d]", pCfg->daysToKeep1, TSDB_MIN_KEEP, pCfg->daysToKeep2);
    return TSDB_CODE_MND_INVALID_DB_OPTION_KEEP;
  }

  if (pCfg->maxRowsPerFileBlock < TSDB_MIN_MAX_ROW_FBLOCK || pCfg->maxRowsPerFileBlock > TSDB_MAX_MAX_ROW_FBLOCK) {
    mError("invalid db option maxRowsPerFileBlock:%d valid range: [%d, %d]", pCfg->maxRowsPerFileBlock,
           TSDB_MIN_MAX_ROW_FBLOCK, TSDB_MAX_MAX_ROW_FBLOCK);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->minRowsPerFileBlock < TSDB_MIN_MIN_ROW_FBLOCK || pCfg->minRowsPerFileBlock > TSDB_MAX_MIN_ROW_FBLOCK) {
    mError("invalid db option minRowsPerFileBlock:%d valid range: [%d, %d]", pCfg->minRowsPerFileBlock,
           TSDB_MIN_MIN_ROW_FBLOCK, TSDB_MAX_MIN_ROW_FBLOCK);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->minRowsPerFileBlock > pCfg->maxRowsPerFileBlock) {
    mError("invalid db option minRowsPerFileBlock:%d should smaller than maxRowsPerFileBlock:%d",
           pCfg->minRowsPerFileBlock, pCfg->maxRowsPerFileBlock);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->commitTime < TSDB_MIN_COMMIT_TIME || pCfg->commitTime > TSDB_MAX_COMMIT_TIME) {
    mError("invalid db option commitTime:%d valid range: [%d, %d]", pCfg->commitTime, TSDB_MIN_COMMIT_TIME,
           TSDB_MAX_COMMIT_TIME);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->precision < TSDB_MIN_PRECISION && pCfg->precision > TSDB_MAX_PRECISION) {
    mError("invalid db option timePrecision:%d valid value: [%d, %d]", pCfg->precision, TSDB_MIN_PRECISION,
           TSDB_MAX_PRECISION);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->compression < TSDB_MIN_COMP_LEVEL || pCfg->compression > TSDB_MAX_COMP_LEVEL) {
    mError("invalid db option compression:%d valid range: [%d, %d]", pCfg->compression, TSDB_MIN_COMP_LEVEL,
           TSDB_MAX_COMP_LEVEL);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->walLevel < TSDB_MIN_WAL_LEVEL || pCfg->walLevel > TSDB_MAX_WAL_LEVEL) {
    mError("invalid db option walLevel:%d, valid range: [%d, %d]", pCfg->walLevel, TSDB_MIN_WAL_LEVEL, TSDB_MAX_WAL_LEVEL);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->fsyncPeriod < TSDB_MIN_FSYNC_PERIOD || pCfg->fsyncPeriod > TSDB_MAX_FSYNC_PERIOD) {
    mError("invalid db option fsyncPeriod:%d, valid range: [%d, %d]", pCfg->fsyncPeriod, TSDB_MIN_FSYNC_PERIOD, TSDB_MAX_FSYNC_PERIOD);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->replications < TSDB_MIN_DB_REPLICA_OPTION || pCfg->replications > TSDB_MAX_DB_REPLICA_OPTION) {
    mError("invalid db option replications:%d valid range: [%d, %d]", pCfg->replications, TSDB_MIN_DB_REPLICA_OPTION,
           TSDB_MAX_DB_REPLICA_OPTION);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->replications > mnodeGetDnodesNum()) {
    mError("no enough dnode to config replica: %d, #dnodes: %d", pCfg->replications, mnodeGetDnodesNum());
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->quorum > pCfg->replications) {
    mError("invalid db option quorum:%d larger than replica:%d", pCfg->quorum, pCfg->replications);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->quorum < TSDB_MIN_DB_QUORUM_OPTION || pCfg->quorum > TSDB_MAX_DB_QUORUM_OPTION) {
    mError("invalid db option quorum:%d valid range: [%d, %d]", pCfg->quorum, TSDB_MIN_DB_QUORUM_OPTION,
           TSDB_MAX_DB_QUORUM_OPTION);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->update < TSDB_MIN_DB_UPDATE || pCfg->update > TSDB_MAX_DB_UPDATE) {
    mError("invalid db option update:%d valid range: [%d, %d]", pCfg->update, TSDB_MIN_DB_UPDATE, TSDB_MAX_DB_UPDATE);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (pCfg->cacheLastRow < TSDB_MIN_DB_CACHE_LAST_ROW || pCfg->cacheLastRow > TSDB_MAX_DB_CACHE_LAST_ROW) {
    mError("invalid db option cacheLastRow:%d valid range: [%d, %d]", pCfg->cacheLastRow, TSDB_MIN_DB_CACHE_LAST_ROW, TSDB_MAX_DB_CACHE_LAST_ROW);
    return TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  return TSDB_CODE_SUCCESS;
}

static void mnodeSetDefaultDbCfg(SDbCfg *pCfg) {
  if (pCfg->cacheBlockSize < 0) pCfg->cacheBlockSize = tsCacheBlockSize;
  if (pCfg->totalBlocks < 0) pCfg->totalBlocks = tsBlocksPerVnode;
  if (pCfg->maxTables < 0) pCfg->maxTables = tsMaxTablePerVnode;
  if (pCfg->daysPerFile < 0) pCfg->daysPerFile = tsDaysPerFile;
  if (pCfg->daysToKeep < 0) pCfg->daysToKeep = tsDaysToKeep;
  if (pCfg->daysToKeep1 < 0) pCfg->daysToKeep1 = pCfg->daysToKeep;
  if (pCfg->daysToKeep2 < 0) pCfg->daysToKeep2 = pCfg->daysToKeep;
  if (pCfg->minRowsPerFileBlock < 0) pCfg->minRowsPerFileBlock = tsMinRowsInFileBlock;
  if (pCfg->maxRowsPerFileBlock < 0) pCfg->maxRowsPerFileBlock = tsMaxRowsInFileBlock;
  if (pCfg->fsyncPeriod <0) pCfg->fsyncPeriod = tsFsyncPeriod;
  if (pCfg->commitTime < 0) pCfg->commitTime = tsCommitTime;
  if (pCfg->precision < 0) pCfg->precision = tsTimePrecision;
  if (pCfg->compression < 0) pCfg->compression = tsCompression;
  if (pCfg->walLevel < 0) pCfg->walLevel = tsWAL;
  if (pCfg->replications < 0) pCfg->replications = tsReplications;
  if (pCfg->quorum < 0) pCfg->quorum = tsQuorum;
  if (pCfg->update < 0) pCfg->update = tsUpdate;
  if (pCfg->cacheLastRow < 0) pCfg->cacheLastRow = tsCacheLastRow;
}

static int32_t mnodeCreateDbCb(SMnodeMsg *pMsg, int32_t code) {
  SDbObj *pDb = pMsg->pDb;
  if (code == TSDB_CODE_SUCCESS) {
    mLInfo("db:%s, is created by %s", pDb->name, mnodeGetUserFromMsg(pMsg));
  } else {
    mError("db:%s, failed to create by %s, reason:%s", pDb->name, mnodeGetUserFromMsg(pMsg), tstrerror(code));
  }

  return code;
}

static int32_t mnodeCreateDb(SAcctObj *pAcct, SCreateDbMsg *pCreate, SMnodeMsg *pMsg) {
  int32_t code = acctCheck(pAcct, ACCT_GRANT_DB);
  if (code != 0) return code;

  SDbObj *pDb = mnodeGetDb(pCreate->db);
  if (pDb != NULL) {
    mnodeDecDbRef(pDb); 
    if (pCreate->ignoreExist) {
      mDebug("db:%s, already exist, ignore exist is set", pCreate->db);
      return TSDB_CODE_SUCCESS;
    } else {
      mError("db:%s, already exist, ignore exist not set", pCreate->db);
      return TSDB_CODE_MND_DB_ALREADY_EXIST;
    }
  }

  code = grantCheck(TSDB_GRANT_DB);
  if (code != 0) return code;

  pDb = calloc(1, sizeof(SDbObj));
  tstrncpy(pDb->name, pCreate->db, sizeof(pDb->name));
  tstrncpy(pDb->acct, pAcct->user, sizeof(pDb->acct)); 
  pDb->createdTime = taosGetTimestampMs(); 
  pDb->cfg = (SDbCfg) {
    .cacheBlockSize      = pCreate->cacheBlockSize,
    .totalBlocks         = pCreate->totalBlocks,
    .maxTables           = pCreate->maxTables,
    .daysPerFile         = pCreate->daysPerFile,
    .daysToKeep          = pCreate->daysToKeep,
    .daysToKeep1         = pCreate->daysToKeep1,
    .daysToKeep2         = pCreate->daysToKeep2,
    .minRowsPerFileBlock = pCreate->minRowsPerFileBlock,
    .maxRowsPerFileBlock = pCreate->maxRowsPerFileBlock,
    .fsyncPeriod         = pCreate->fsyncPeriod,
    .commitTime          = pCreate->commitTime,
    .precision           = pCreate->precision,
    .compression         = pCreate->compression,
    .walLevel            = pCreate->walLevel,
    .replications        = pCreate->replications,
    .quorum              = pCreate->quorum,
    .update              = pCreate->update,
    .cacheLastRow        = pCreate->cacheLastRow
  };

  mnodeSetDefaultDbCfg(&pDb->cfg);

  code = mnodeCheckDbCfg(&pDb->cfg);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(pDb);
    return code;
  }

  pMsg->pDb = pDb;
  mnodeIncDbRef(pDb);

  SSdbRow row = {
    .type     = SDB_OPER_GLOBAL,
    .pTable   = tsDbSdb,
    .pObj     = pDb,
    .rowSize  = sizeof(SDbObj),
    .pMsg     = pMsg,
    .fpRsp    = mnodeCreateDbCb
  };

  code = sdbInsertRow(&row);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to create, reason:%s", pDb->name, tstrerror(code));
    pMsg->pDb = NULL;
    mnodeDestroyDb(pDb);
  }

  return code;
}

bool mnodeCheckIsMonitorDB(char *db, char *monitordb) {
  char dbName[TSDB_DB_NAME_LEN] = {0};
  extractDBName(db, dbName);

  size_t len = strlen(dbName);
  return (strncasecmp(dbName, monitordb, len) == 0 && len == strlen(monitordb));
}

#if 0
void mnodePrintVgroups(SDbObj *pDb, char *row) {
  mInfo("db:%s, vgroup link from head, row:%s", pDb->name, row);  
  SVgObj *pVgroup = pDb->pHead;
  while (pVgroup != NULL) {
    mInfo("vgId:%d", pVgroup->vgId);
    pVgroup = pVgroup->next;
  }

  mInfo("db:%s, vgroup link from tail", pDb->name, pDb->numOfVgroups);
  pVgroup = pDb->pTail;
  while (pVgroup != NULL) {
    mInfo("vgId:%d", pVgroup->vgId);
    pVgroup = pVgroup->prev;
  }
}
#endif

void mnodeAddVgroupIntoDb(SVgObj *pVgroup) {
  SDbObj *pDb = pVgroup->pDb;

  pthread_mutex_lock(&pDb->mutex);
  int32_t vgPos = pDb->numOfVgroups++;
  if (vgPos >= pDb->vgListSize) {
    pDb->vgList = realloc(pDb->vgList, pDb->vgListSize * 2 * sizeof(SVgObj *));
    memset(pDb->vgList + pDb->vgListSize, 0, pDb->vgListSize * sizeof(SVgObj *));
    pDb->vgListSize *= 2;
  }

  pDb->vgList[vgPos] = pVgroup;
  pthread_mutex_unlock(&pDb->mutex);
}

void mnodeRemoveVgroupFromDb(SVgObj *pVgroup) {
  SDbObj *pDb = pVgroup->pDb;

  pthread_mutex_lock(&pDb->mutex);
  for (int32_t v1 = 0; v1 < pDb->numOfVgroups; ++v1) {
    if (pDb->vgList[v1] == pVgroup) {
      for (int32_t v2 = v1; v2 < pDb->numOfVgroups - 1; ++v2) {
        pDb->vgList[v2] = pDb->vgList[v2 + 1];
      }
      pDb->numOfVgroups--;
      pDb->vgList[pDb->numOfVgroups] = NULL;
      break;
    }
  }

  pthread_mutex_unlock(&pDb->mutex);
}

void mnodeCleanupDbs() {
  sdbCloseTable(tsDbRid);
  tsDbSdb = NULL;
}

static int32_t mnodeGetDbMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SSchema *pSchema = pMeta->schema;
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  pShow->bytes[cols] = (TSDB_DB_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "created_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "ntables");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

#ifndef __CLOUD_VERSION__
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
#endif
    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema[cols].name, "vgroups");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;
#ifndef __CLOUD_VERSION__
  }
#endif

#ifndef __CLOUD_VERSION__
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
#endif
    pShow->bytes[cols] = 2;
    pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
    strcpy(pSchema[cols].name, "replica");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 2;
    pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
    strcpy(pSchema[cols].name, "quorum");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 2;
    pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
    strcpy(pSchema[cols].name, "days");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;
#ifndef __CLOUD_VERSION__
  }
#endif

  pShow->bytes[cols] = 24 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "keep1,keep2,keep(D)");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

#ifndef __CLOUD_VERSION__
  if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
#endif

    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema[cols].name, "cache(MB)");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;
    
    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema[cols].name, "blocks");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;
    
    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema[cols].name, "minrows");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema[cols].name, "maxrows");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 1;
    pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
    strcpy(pSchema[cols].name, "wallevel");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema[cols].name, "fsync");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 1;
    pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
    strcpy(pSchema[cols].name, "comp");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 1;
    pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
    strcpy(pSchema[cols].name, "cachelast");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;
#ifndef __CLOUD_VERSION__
  }
#endif

  pShow->bytes[cols] = 3 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "precision");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 1;
  pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
  strcpy(pSchema[cols].name, "update");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 10 + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "status");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  pShow->numOfRows = pUser->pAcct->acctInfo.numOfDbs;

  mnodeDecUserRef(pUser);
  return 0;
}

static char *mnodeGetDbStr(char *src) {
  char *pos = strstr(src, TS_PATH_DELIMITER);
  if (pos != NULL) ++pos;

  return pos;
}

static int32_t mnodeRetrieveDbs(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;
  SDbObj *pDb = NULL;
  char *  pWrite;
  int32_t cols = 0;
  SUserObj *pUser = mnodeGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextDb(pShow->pIter, &pDb);

    if (pDb == NULL) break;
    if (pDb->pAcct != pUser->pAcct) {
      mnodeDecDbRef(pDb);
      continue;
    }

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;  
    char* name = mnodeGetDbStr(pDb->name);
    if (name != NULL) {
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, name, pShow->bytes[cols]);
    } else {
      STR_TO_VARSTR(pWrite, "NULL");
    }
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pDb->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->numOfTables;
    cols++;

#ifndef __CLOUD_VERSION__
    if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
#endif
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = pDb->numOfVgroups;
      cols++;
#ifndef __CLOUD_VERSION__
    }
#endif

#ifndef __CLOUD_VERSION__
    if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
#endif
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *)pWrite = pDb->cfg.replications;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *)pWrite = pDb->cfg.quorum;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *)pWrite = pDb->cfg.daysPerFile;
      cols++;
#ifndef __CLOUD_VERSION__
    }
#endif

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    
    char tmp[128] = {0};
    sprintf(tmp, "%d,%d,%d", pDb->cfg.daysToKeep1, pDb->cfg.daysToKeep2, pDb->cfg.daysToKeep);
    STR_WITH_SIZE_TO_VARSTR(pWrite, tmp, strlen(tmp));
    cols++;

#ifndef __CLOUD_VERSION__
    if (strcmp(pUser->user, TSDB_DEFAULT_USER) == 0) {
#endif
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = pDb->cfg.cacheBlockSize;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = pDb->cfg.totalBlocks;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = pDb->cfg.minRowsPerFileBlock;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = pDb->cfg.maxRowsPerFileBlock;
      cols++;
      
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int8_t *)pWrite = pDb->cfg.walLevel;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = pDb->cfg.fsyncPeriod;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int8_t *)pWrite = pDb->cfg.compression;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int8_t *)pWrite = pDb->cfg.cacheLastRow;
      cols++;
#ifndef __CLOUD_VERSION__
    }
#endif

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    char *prec = (pDb->cfg.precision == TSDB_TIME_PRECISION_MILLI) ? TSDB_TIME_PRECISION_MILLI_STR
                                                                   : TSDB_TIME_PRECISION_MICRO_STR;
    STR_WITH_SIZE_TO_VARSTR(pWrite, prec, 2);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int8_t *)pWrite = pDb->cfg.update;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (pDb->status == TSDB_DB_STATUS_READY) {
      const char *src = "ready";
      STR_WITH_SIZE_TO_VARSTR(pWrite, src, strlen(src));
    } else {
      const char *src = "dropping";
      STR_WITH_SIZE_TO_VARSTR(pWrite, src, strlen(src));
    }
    cols++;

    numOfRows++;
    mnodeDecDbRef(pDb);
  }

  pShow->numOfReads += numOfRows;
  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);

  mnodeDecUserRef(pUser);
  return numOfRows;
}

void mnodeAddSuperTableIntoDb(SDbObj *pDb) {
  atomic_add_fetch_32(&pDb->numOfSuperTables, 1);
}

void mnodeRemoveSuperTableFromDb(SDbObj *pDb) {
  atomic_add_fetch_32(&pDb->numOfSuperTables, -1);
}

void mnodeAddTableIntoDb(SDbObj *pDb) {
  atomic_add_fetch_32(&pDb->numOfTables, 1);
}

void mnodeRemoveTableFromDb(SDbObj *pDb) {
  atomic_add_fetch_32(&pDb->numOfTables, -1);
}

static int32_t mnodeSetDbDropping(SDbObj *pDb) {
  if (pDb->status) return TSDB_CODE_SUCCESS;

  pDb->status = true;
  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsDbSdb,
    .pObj   = pDb
  };

  int32_t code = sdbUpdateRow(&row);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to set dropping state, reason:%s", pDb->name, tstrerror(code));
  }

  return code;
}

static int32_t mnodeProcessCreateDbMsg(SMnodeMsg *pMsg) {
  SCreateDbMsg *pCreate    = pMsg->rpcMsg.pCont;  
  pCreate->maxTables       = htonl(pCreate->maxTables);
  pCreate->cacheBlockSize  = htonl(pCreate->cacheBlockSize);
  pCreate->totalBlocks     = htonl(pCreate->totalBlocks);
  pCreate->daysPerFile     = htonl(pCreate->daysPerFile);
  pCreate->daysToKeep      = htonl(pCreate->daysToKeep);
  pCreate->daysToKeep1     = htonl(pCreate->daysToKeep1);
  pCreate->daysToKeep2     = htonl(pCreate->daysToKeep2);
  pCreate->commitTime      = htonl(pCreate->commitTime);
  pCreate->fsyncPeriod     = htonl(pCreate->fsyncPeriod);
  pCreate->minRowsPerFileBlock = htonl(pCreate->minRowsPerFileBlock);
  pCreate->maxRowsPerFileBlock = htonl(pCreate->maxRowsPerFileBlock);
  
  int32_t code;
  if (grantCheck(TSDB_GRANT_TIME) != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_GRANT_EXPIRED;
  } else if (!pMsg->pUser->writeAuth) {
    code = TSDB_CODE_MND_NO_RIGHTS;
  } else {
    code = mnodeCreateDb(pMsg->pUser->pAcct, pCreate, pMsg);
  }

  return code;
}

static SDbCfg mnodeGetAlterDbOption(SDbObj *pDb, SAlterDbMsg *pAlter) {
  SDbCfg  newCfg = pDb->cfg;
  int32_t maxTables      = htonl(pAlter->maxTables);
  int32_t cacheBlockSize = htonl(pAlter->cacheBlockSize);
  int32_t totalBlocks    = htonl(pAlter->totalBlocks);
  int32_t daysPerFile    = htonl(pAlter->daysPerFile);
  int32_t daysToKeep     = htonl(pAlter->daysToKeep);
  int32_t daysToKeep1    = htonl(pAlter->daysToKeep1);
  int32_t daysToKeep2    = htonl(pAlter->daysToKeep2);
  int32_t minRows        = htonl(pAlter->minRowsPerFileBlock);
  int32_t maxRows        = htonl(pAlter->maxRowsPerFileBlock);
  int32_t commitTime     = htonl(pAlter->commitTime);
  int32_t fsyncPeriod    = htonl(pAlter->fsyncPeriod);
  int8_t  compression    = pAlter->compression;
  int8_t  walLevel       = pAlter->walLevel;
  int8_t  replications   = pAlter->replications;
  int8_t  quorum         = pAlter->quorum;
  int8_t  precision      = pAlter->precision;
  int8_t  update         = pAlter->update;
  int8_t  cacheLastRow   = pAlter->cacheLastRow;
  
  terrno = TSDB_CODE_SUCCESS;

  if (cacheBlockSize > 0 && cacheBlockSize != pDb->cfg.cacheBlockSize) {
    mError("db:%s, can't alter cache option", pDb->name);
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (totalBlocks > 0 && totalBlocks != pDb->cfg.totalBlocks) {
    mInfo("db:%s, blocks:%d change to %d", pDb->name, pDb->cfg.totalBlocks, totalBlocks);
    newCfg.totalBlocks = totalBlocks;
  }

  if (maxTables > 0) {
    mInfo("db:%s, maxTables:%d change to %d", pDb->name, pDb->cfg.maxTables, maxTables);
    newCfg.maxTables = maxTables;
    if (newCfg.maxTables < pDb->cfg.maxTables) {
      mError("db:%s, tables:%d should larger than origin:%d", pDb->name, newCfg.maxTables, pDb->cfg.maxTables);
      terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    }
  }

  if (daysPerFile > 0 && daysPerFile != pDb->cfg.daysPerFile) {
    mError("db:%s, can't alter days option", pDb->name);
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (daysToKeep > 0 && daysToKeep != pDb->cfg.daysToKeep) {
    mDebug("db:%s, daysToKeep:%d change to %d", pDb->name, pDb->cfg.daysToKeep, daysToKeep);
    newCfg.daysToKeep = daysToKeep;
  }

  if (daysToKeep1 > 0 && daysToKeep1 != pDb->cfg.daysToKeep1) {
    mDebug("db:%s, daysToKeep1:%d change to %d", pDb->name, pDb->cfg.daysToKeep1, daysToKeep1);
    newCfg.daysToKeep1 = daysToKeep1;
  }

  if (daysToKeep2 > 0 && daysToKeep2 != pDb->cfg.daysToKeep2) {
    mDebug("db:%s, daysToKeep2:%d change to %d", pDb->name, pDb->cfg.daysToKeep2, daysToKeep2);
    newCfg.daysToKeep2 = daysToKeep2;
  }

  if (minRows > 0 && minRows != pDb->cfg.minRowsPerFileBlock) {
    mError("db:%s, can't alter minRows option", pDb->name);
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (maxRows > 0 && maxRows != pDb->cfg.maxRowsPerFileBlock) {
    mError("db:%s, can't alter maxRows option", pDb->name);
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (commitTime > 0 && commitTime != pDb->cfg.commitTime) {
    mError("db:%s, can't alter commitTime option", pDb->name);
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (precision > 0 && precision != pDb->cfg.precision) {
    mError("db:%s, can't alter precision option", pDb->name);
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
  }

  if (compression >= 0 && compression != pDb->cfg.compression) {
    mDebug("db:%s, compression:%d change to %d", pDb->name, pDb->cfg.compression, compression);
    newCfg.compression = compression;
  }

  if (walLevel > 0 && walLevel != pDb->cfg.walLevel) {
    mDebug("db:%s, walLevel:%d change to %d", pDb->name, pDb->cfg.walLevel, walLevel);
    newCfg.walLevel = walLevel;
  }

  if (fsyncPeriod >= 0 && fsyncPeriod != pDb->cfg.fsyncPeriod) {
    mDebug("db:%s, fsyncPeriod:%d change to %d", pDb->name, pDb->cfg.fsyncPeriod, fsyncPeriod);
    newCfg.fsyncPeriod = fsyncPeriod;
  }

  if (replications > 0 && replications != pDb->cfg.replications) {
    mDebug("db:%s, replications:%d change to %d", pDb->name, pDb->cfg.replications, replications);
    newCfg.replications = replications;

    if (pDb->cfg.walLevel < TSDB_MIN_WAL_LEVEL) {
      mError("db:%s, walLevel:%d must be greater than 0", pDb->name, pDb->cfg.walLevel);
      terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    }

    if (replications > mnodeGetDnodesNum()) {
      mError("db:%s, no enough dnode to change replica:%d", pDb->name, replications);
      terrno = TSDB_CODE_MND_NO_ENOUGH_DNODES;
    }

    if (pDb->cfg.replications - replications >= 2) {
      mError("db:%s, replica number can't change from %d to %d", pDb->name, pDb->cfg.replications, replications);
      terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
    }
  }

  if (quorum >= 0 && quorum != pDb->cfg.quorum) {
    mDebug("db:%s, quorum:%d change to %d", pDb->name, pDb->cfg.quorum, quorum);
    newCfg.quorum = quorum;
  }

  if (update >= 0 && update != pDb->cfg.update) {
#if 0
    mDebug("db:%s, update:%d change to %d", pDb->name, pDb->cfg.update, update);
    newCfg.update = update;
#else
    mError("db:%s, can't alter update option", pDb->name);
    terrno = TSDB_CODE_MND_INVALID_DB_OPTION;
#endif
  }

  if (cacheLastRow >= 0 && cacheLastRow != pDb->cfg.cacheLastRow) {
    mDebug("db:%s, cacheLastRow:%d change to %d", pDb->name, pDb->cfg.cacheLastRow, cacheLastRow);
    newCfg.cacheLastRow = cacheLastRow;
  }

  return newCfg;
}

static int32_t mnodeAlterDbCb(SMnodeMsg *pMsg, int32_t code) {
  if (code != TSDB_CODE_SUCCESS) return code;
  SDbObj *pDb = pMsg->pDb;

  void *pIter = NULL;
  SVgObj *pVgroup = NULL;
    while (1) {
    pIter = mnodeGetNextVgroup(pIter, &pVgroup);
    if (pVgroup == NULL) break;
    if (pVgroup->pDb == pDb) {
      mnodeSendAlterVgroupMsg(pVgroup);
    }
    mnodeDecVgroupRef(pVgroup);
  }

  mDebug("db:%s, all vgroups is altered", pDb->name);
  mLInfo("db:%s, is alterd by %s", pDb->name, mnodeGetUserFromMsg(pMsg));

  bnNotify();

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeAlterDb(SDbObj *pDb, SAlterDbMsg *pAlter, void *pMsg) {
  SDbCfg newCfg = mnodeGetAlterDbOption(pDb, pAlter);
  if (terrno != TSDB_CODE_SUCCESS) {
    return terrno;
  }

  int32_t code = mnodeCheckDbCfg(&newCfg);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (memcmp(&newCfg, &pDb->cfg, sizeof(SDbCfg)) != 0) {
    pDb->cfg = newCfg;
    pDb->dbCfgVersion++;
    SSdbRow row = {
      .type    = SDB_OPER_GLOBAL,
      .pTable  = tsDbSdb,
      .pObj    = pDb,
      .pMsg    = pMsg,
      .fpRsp   = mnodeAlterDbCb
    };

    code = sdbUpdateRow(&row);
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
      mError("db:%s, failed to alter, reason:%s", pDb->name, tstrerror(code));
    }
  }

  return code;
}

static int32_t mnodeProcessAlterDbMsg(SMnodeMsg *pMsg) {
  SAlterDbMsg *pAlter = pMsg->rpcMsg.pCont;
  mDebug("db:%s, alter db msg is received from thandle:%p", pAlter->db, pMsg->rpcMsg.handle);

  if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDb(pAlter->db);
  if (pMsg->pDb == NULL) {
    mError("db:%s, failed to alter, invalid db", pAlter->db);
    return TSDB_CODE_MND_INVALID_DB;
  }
  
  if (pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pAlter->db, pMsg->pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  return mnodeAlterDb(pMsg->pDb, pAlter, pMsg);
}

static int32_t mnodeDropDbCb(SMnodeMsg *pMsg, int32_t code) {
  SDbObj *pDb = pMsg->pDb;
  if (code != TSDB_CODE_SUCCESS) {
    mError("db:%s, failed to drop from sdb, reason:%s", pDb->name, tstrerror(code));
  } else {
    mLInfo("db:%s, is dropped by %s", pDb->name, mnodeGetUserFromMsg(pMsg));
  }

  return code;
}

static int32_t mnodeDropDb(SMnodeMsg *pMsg) {
  if (pMsg == NULL) return TSDB_CODE_MND_APP_ERROR;
  
  SDbObj *pDb = pMsg->pDb;
  mInfo("db:%s, drop db from sdb", pDb->name);

  SSdbRow row = {
    .type    = SDB_OPER_GLOBAL,
    .pTable  = tsDbSdb,
    .pObj    = pDb,
    .pMsg    = pMsg,
    .fpRsp   = mnodeDropDbCb
  };

  int32_t code = sdbDeleteRow(&row);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to drop, reason:%s", pDb->name, tstrerror(code));
  }

  return code;
}

static int32_t mnodeProcessDropDbMsg(SMnodeMsg *pMsg) {
  SDropDbMsg *pDrop = pMsg->rpcMsg.pCont;
  mDebug("db:%s, drop db msg is received from thandle:%p", pDrop->db, pMsg->rpcMsg.handle);

  if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDb(pDrop->db);
  if (pMsg->pDb == NULL) {
    if (pDrop->ignoreNotExists) {
      mDebug("db:%s, db is not exist, treat as success", pDrop->db);
      return TSDB_CODE_SUCCESS;
    } else {
      mError("db:%s, failed to drop, invalid db", pDrop->db);
      return TSDB_CODE_MND_INVALID_DB;
    }
  }

#if 0
  if (mnodeCheckIsMonitorDB(pMsg->pDb->name, tsMonitorDbName)) {
    mError("db:%s, can't drop monitor database", pDrop->db);
    return TSDB_CODE_MND_MONITOR_DB_FORBIDDEN;
  }
#endif   

  int32_t code = mnodeSetDbDropping(pMsg->pDb);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("db:%s, failed to drop, reason:%s", pDrop->db, tstrerror(code));
    return code;
  }

  mnodeSendDropAllDbVgroupsMsg(pMsg->pDb);

  mDebug("db:%s, all vgroups is dropped", pMsg->pDb->name);
  return mnodeDropDb(pMsg);
}

void  mnodeDropAllDbs(SAcctObj *pAcct)  {
  int32_t numOfDbs = 0;
  SDbObj *pDb = NULL;
  void *  pIter = NULL;

  mInfo("acct:%s, all dbs will be dropped from sdb", pAcct->user);

  while (1) {
    pIter = mnodeGetNextDb(pIter, &pDb);
    if (pDb == NULL) break;

    if (pDb->pAcct == pAcct) {
      mInfo("db:%s, drop db from sdb for acct:%s is dropped", pDb->name, pAcct->user);
      SSdbRow row = {
        .type   = SDB_OPER_LOCAL,
        .pTable = tsDbSdb,
        .pObj   = pDb
      };
      
      sdbDeleteRow(&row);
      numOfDbs++;
    }
    mnodeDecDbRef(pDb);
  }

  mInfo("acct:%s, all dbs:%d is dropped from sdb", pAcct->user, numOfDbs);
}
