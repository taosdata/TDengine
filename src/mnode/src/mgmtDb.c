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
#include "ttime.h"
#include "tname.h"
#include "tbalance.h"
#include "tdataformat.h"
#include "mgmtDef.h"
#include "mgmtLog.h"
#include "mgmtAcct.h"
#include "mgmtDb.h"
#include "mgmtDnode.h"
#include "mgmtMnode.h"
#include "mgmtShell.h"
#include "mgmtProfile.h"
#include "mgmtSdb.h"
#include "mgmtTable.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"

static void *  tsDbSdb = NULL;
static int32_t tsDbUpdateSize;

static int32_t mgmtCreateDb(SAcctObj *pAcct, SCMCreateDbMsg *pCreate);
static void    mgmtDropDb(SQueuedMsg *newMsg);
static int32_t mgmtSetDbDropping(SDbObj *pDb);
static int32_t mgmtGetDbMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mgmtRetrieveDbs(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static void    mgmtProcessCreateDbMsg(SQueuedMsg *pMsg);
static void    mgmtProcessAlterDbMsg(SQueuedMsg *pMsg);
static void    mgmtProcessDropDbMsg(SQueuedMsg *pMsg);

static int32_t mgmtDbActionDestroy(SSdbOper *pOper) {
  tfree(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtDbActionInsert(SSdbOper *pOper) {
  SDbObj *pDb = pOper->pObj;
  SAcctObj *pAcct = mgmtGetAcct(pDb->acct);

  pDb->pHead = NULL;
  pDb->pTail = NULL;
  pDb->numOfVgroups = 0;
  pDb->numOfTables = 0;
  pDb->numOfSuperTables = 0;

  if (pAcct != NULL) {
    mgmtAddDbToAcct(pAcct, pDb);
    mgmtDecAcctRef(pAcct);
  }
  else {
    mError("db:%s, acct:%s info not exist in sdb", pDb->name, pDb->acct);
    return TSDB_CODE_INVALID_ACCT;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtDbActionDelete(SSdbOper *pOper) {
  SDbObj *pDb = pOper->pObj;
  SAcctObj *pAcct = mgmtGetAcct(pDb->acct);

  mgmtDropDbFromAcct(pAcct, pDb);
  mgmtDropAllChildTables(pDb);
  mgmtDropAllSuperTables(pDb);
  mgmtDropAllDbVgroups(pDb, false);
  mgmtDecAcctRef(pAcct);
  
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtDbActionUpdate(SSdbOper *pOper) {
  SDbObj *pDb = pOper->pObj;
  SDbObj *pSaved = mgmtGetDb(pDb->name);
  if (pDb != pSaved) {
    memcpy(pSaved, pDb, pOper->rowSize);
    free(pDb);
  }
  mgmtDecDbRef(pSaved);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtDbActionEncode(SSdbOper *pOper) {
  SDbObj *pDb = pOper->pObj;
  memcpy(pOper->rowData, pDb, tsDbUpdateSize);
  pOper->rowSize = tsDbUpdateSize;
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtDbActionDecode(SSdbOper *pOper) {
  SDbObj *pDb = (SDbObj *) calloc(1, sizeof(SDbObj));
  if (pDb == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;
  
  memcpy(pDb, pOper->rowData, tsDbUpdateSize);
  pOper->pObj = pDb;
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtDbActionRestored() {
  return 0;
}

int32_t mgmtInitDbs() {
  SDbObj tObj;
  tsDbUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableId      = SDB_TABLE_DB,
    .tableName    = "dbs",
    .hashSessions = TSDB_MAX_DBS,
    .maxRowSize   = tsDbUpdateSize,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_STRING,
    .insertFp     = mgmtDbActionInsert,
    .deleteFp     = mgmtDbActionDelete,
    .updateFp     = mgmtDbActionUpdate,
    .encodeFp     = mgmtDbActionEncode,
    .decodeFp     = mgmtDbActionDecode,
    .destroyFp    = mgmtDbActionDestroy,
    .restoredFp   = mgmtDbActionRestored
  };

  tsDbSdb = sdbOpenTable(&tableDesc);
  if (tsDbSdb == NULL) {
    mError("failed to init db data");
    return -1;
  }

  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_CREATE_DB, mgmtProcessCreateDbMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_ALTER_DB, mgmtProcessAlterDbMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_DROP_DB, mgmtProcessDropDbMsg);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_DB, mgmtGetDbMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_DB, mgmtRetrieveDbs);
  
  mTrace("table:dbs table is created");
  return 0;
}

void *mgmtGetNextDb(void *pNode, SDbObj **pDb) {
  return sdbFetchRow(tsDbSdb, pNode, (void **)pDb);
}

SDbObj *mgmtGetDb(char *db) {
  return (SDbObj *)sdbGetRow(tsDbSdb, db);
}

void mgmtIncDbRef(SDbObj *pDb) {
  return sdbIncRef(tsDbSdb, pDb); 
}

void mgmtDecDbRef(SDbObj *pDb) { 
  return sdbDecRef(tsDbSdb, pDb); 
}

SDbObj *mgmtGetDbByTableId(char *tableId) {
  char db[TSDB_TABLE_ID_LEN], *pos;

  pos = strstr(tableId, TS_PATH_DELIMITER);
  pos = strstr(pos + 1, TS_PATH_DELIMITER);
  memset(db, 0, sizeof(db));
  strncpy(db, tableId, pos - tableId);

  return mgmtGetDb(db);
}

static int32_t mgmtCheckDbCfg(SDbCfg *pCfg) {
  if (pCfg->cacheBlockSize < TSDB_MIN_CACHE_BLOCK_SIZE || pCfg->cacheBlockSize > TSDB_MAX_CACHE_BLOCK_SIZE) {
    mError("invalid db option cacheBlockSize:%d valid range: [%d, %d]", pCfg->cacheBlockSize, TSDB_MIN_CACHE_BLOCK_SIZE,
           TSDB_MAX_CACHE_BLOCK_SIZE);
  }

  if (pCfg->totalBlocks < TSDB_MIN_TOTAL_BLOCKS || pCfg->totalBlocks > TSDB_MAX_TOTAL_BLOCKS) {
    mError("invalid db option totalBlocks:%d valid range: [%d, %d]", pCfg->totalBlocks, TSDB_MIN_TOTAL_BLOCKS,
           TSDB_MAX_TOTAL_BLOCKS);
  }

  if (pCfg->maxTables < TSDB_MIN_TABLES || pCfg->maxTables > TSDB_MAX_TABLES) {
    mError("invalid db option maxTables:%d valid range: [%d, %d]", pCfg->maxTables, TSDB_MIN_TABLES, TSDB_MAX_TABLES);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCfg->daysPerFile < TSDB_MIN_DAYS_PER_FILE || pCfg->daysPerFile > TSDB_MAX_DAYS_PER_FILE) {
    mError("invalid db option daysPerFile:%d valid range: [%d, %d]", pCfg->daysPerFile, TSDB_MIN_DAYS_PER_FILE,
           TSDB_MAX_DAYS_PER_FILE);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCfg->daysToKeep < TSDB_MIN_KEEP || pCfg->daysToKeep > TSDB_MAX_KEEP) {
    mError("invalid db option daysToKeep:%d", pCfg->daysToKeep);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCfg->daysToKeep < pCfg->daysPerFile) {
    mError("invalid db option daysToKeep:%d daysPerFile:%d", pCfg->daysToKeep, pCfg->daysPerFile);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCfg->minRowsPerFileBlock < TSDB_MIN_MIN_ROW_FBLOCK || pCfg->minRowsPerFileBlock > TSDB_MAX_MIN_ROW_FBLOCK) {
    mError("invalid db option minRowsPerFileBlock:%d valid range: [%d, %d]", pCfg->minRowsPerFileBlock,
           TSDB_MIN_MIN_ROW_FBLOCK, TSDB_MAX_MIN_ROW_FBLOCK);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCfg->maxRowsPerFileBlock < TSDB_MIN_MAX_ROW_FBLOCK || pCfg->maxRowsPerFileBlock > TSDB_MAX_MAX_ROW_FBLOCK) {
    mError("invalid db option maxRowsPerFileBlock:%d valid range: [%d, %d]", pCfg->maxRowsPerFileBlock,
           TSDB_MIN_MAX_ROW_FBLOCK, TSDB_MAX_MAX_ROW_FBLOCK);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCfg->minRowsPerFileBlock > pCfg->maxRowsPerFileBlock) {
    mError("invalid db option minRowsPerFileBlock:%d maxRowsPerFileBlock:%d", pCfg->minRowsPerFileBlock,
           pCfg->maxRowsPerFileBlock);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCfg->commitTime < TSDB_MIN_COMMIT_TIME || pCfg->commitTime > TSDB_MAX_COMMIT_TIME) {
    mError("invalid db option commitTime:%d valid range: [%d, %d]", pCfg->commitTime, TSDB_MIN_COMMIT_TIME,
           TSDB_MAX_COMMIT_TIME);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCfg->precision < TSDB_MIN_PRECISION && pCfg->precision > TSDB_MAX_PRECISION) {
    mError("invalid db option timePrecision:%d valid value: [%d, %d]", pCfg->precision, TSDB_MIN_PRECISION,
           TSDB_MAX_PRECISION);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCfg->compression < TSDB_MIN_COMP_LEVEL || pCfg->compression > TSDB_MAX_COMP_LEVEL) {
    mError("invalid db option compression:%d valid range: [%d, %d]", pCfg->compression, TSDB_MIN_COMP_LEVEL,
           TSDB_MAX_COMP_LEVEL);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCfg->commitLog < TSDB_MIN_CLOG_LEVEL || pCfg->commitLog > TSDB_MAX_CLOG_LEVEL) {
    mError("invalid db option commitLog:%d, only 0-2 allowed", pCfg->commitLog);
    return TSDB_CODE_INVALID_OPTION;
  }

  if (pCfg->replications < TSDB_MIN_REPLICA_NUM || pCfg->replications > TSDB_MAX_REPLICA_NUM) {
    mError("invalid db option replications:%d valid range: [%d, %d]", pCfg->replications, TSDB_MIN_REPLICA_NUM,
           TSDB_MAX_REPLICA_NUM);
    return TSDB_CODE_INVALID_OPTION;
  }

#ifndef _SYNC
  if (pCfg->replications != 1) {
    mError("invalid db option replications:%d can only be 1 in this version", pCfg->replications);
    return TSDB_CODE_INVALID_OPTION;
  }
#endif

  return TSDB_CODE_SUCCESS;
}

static void mgmtSetDefaultDbCfg(SDbCfg *pCfg) {
  if (pCfg->cacheBlockSize < 0) pCfg->cacheBlockSize = tsCacheBlockSize;
  if (pCfg->totalBlocks < 0) pCfg->totalBlocks = tsTotalBlocks;
  if (pCfg->maxTables < 0) pCfg->maxTables = tsTablesPerVnode;
  if (pCfg->daysPerFile < 0) pCfg->daysPerFile = tsDaysPerFile;
  if (pCfg->daysToKeep < 0) pCfg->daysToKeep = tsDaysToKeep;
  if (pCfg->daysToKeep1 < 0) pCfg->daysToKeep1 = pCfg->daysToKeep;
  if (pCfg->daysToKeep2 < 0) pCfg->daysToKeep2 = pCfg->daysToKeep;
  if (pCfg->minRowsPerFileBlock < 0) pCfg->minRowsPerFileBlock = tsMinRowsInFileBlock;
  if (pCfg->maxRowsPerFileBlock < 0) pCfg->maxRowsPerFileBlock = tsMaxRowsInFileBlock;
  if (pCfg->commitTime < 0) pCfg->commitTime = tsCommitTime;
  if (pCfg->precision < 0) pCfg->precision = tsTimePrecision;
  if (pCfg->compression < 0) pCfg->compression = tsCompression;
  if (pCfg->commitLog < 0) pCfg->commitLog = tsCommitLog;
  if (pCfg->replications < 0) pCfg->replications = tsReplications;
}

static int32_t mgmtCreateDb(SAcctObj *pAcct, SCMCreateDbMsg *pCreate) {
  int32_t code = acctCheck(pAcct, ACCT_GRANT_DB);
  if (code != 0) return code;

  SDbObj *pDb = mgmtGetDb(pCreate->db);
  if (pDb != NULL) {
    mgmtDecDbRef(pDb); 
    if (pCreate->ignoreExist) {
      return TSDB_CODE_SUCCESS;
    } else {
      return TSDB_CODE_DB_ALREADY_EXIST;
    }
  }

  code = grantCheck(TSDB_GRANT_DB);
  if (code != 0) return code;

  pDb = calloc(1, sizeof(SDbObj));
  strncpy(pDb->name, pCreate->db, TSDB_DB_NAME_LEN);
  strncpy(pDb->acct, pAcct->user, TSDB_USER_LEN); 
  pDb->createdTime = taosGetTimestampMs(); 
  pDb->cfg = (SDbCfg) {
    .cacheBlockSize      = pCreate->cacheBlockSize,
    .totalBlocks         = pCreate->totalBlocks,
    .maxTables           = pCreate->maxSessions,
    .daysPerFile         = pCreate->daysPerFile,
    .daysToKeep          = pCreate->daysToKeep,
    .daysToKeep1         = pCreate->daysToKeep1,
    .daysToKeep2         = pCreate->daysToKeep2,
    .minRowsPerFileBlock = pCreate->maxRowsPerFileBlock,
    .maxRowsPerFileBlock = pCreate->maxRowsPerFileBlock,
    .commitTime          = pCreate->commitTime,
    .precision           = pCreate->precision,
    .compression         = pCreate->compression,
    .commitLog           = pCreate->commitLog,
    .replications        = pCreate->replications
  };

  mgmtSetDefaultDbCfg(&pDb->cfg);

  code = mgmtCheckDbCfg(&pDb->cfg);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(pDb);
    return code;
  }

  SSdbOper oper = {
    .type    = SDB_OPER_GLOBAL,
    .table   = tsDbSdb,
    .pObj    = pDb,
    .rowSize = sizeof(SDbObj),
  };

  code = sdbInsertRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    tfree(pDb);
    code = TSDB_CODE_SDB_ERROR;
  }

  return code;
}

bool mgmtCheckIsMonitorDB(char *db, char *monitordb) {
  char dbName[TSDB_DB_NAME_LEN + 1] = {0};
  extractDBName(db, dbName);

  size_t len = strlen(dbName);
  return (strncasecmp(dbName, monitordb, len) == 0 && len == strlen(monitordb));
}

#if 0
void mgmtPrintVgroups(SDbObj *pDb, char *oper) {
  mPrint("db:%s, vgroup link from head, oper:%s", pDb->name, oper);  
  SVgObj *pVgroup = pDb->pHead;
  while (pVgroup != NULL) {
    mPrint("vgId:%d", pVgroup->vgId);
    pVgroup = pVgroup->next;
  }

  mPrint("db:%s, vgroup link from tail", pDb->name, pDb->numOfVgroups);
  pVgroup = pDb->pTail;
  while (pVgroup != NULL) {
    mPrint("vgId:%d", pVgroup->vgId);
    pVgroup = pVgroup->prev;
  }
}
#endif

void mgmtAddVgroupIntoDb(SVgObj *pVgroup) {
  SDbObj *pDb = pVgroup->pDb;

  pVgroup->next = pDb->pHead;
  pVgroup->prev = NULL;

  if (pDb->pHead) pDb->pHead->prev = pVgroup;
  if (pDb->pTail == NULL) pDb->pTail = pVgroup;

  pDb->pHead = pVgroup;
  pDb->numOfVgroups++;
}

void mgmtAddVgroupIntoDbTail(SVgObj *pVgroup) {
  SDbObj *pDb = pVgroup->pDb;
  pVgroup->next = NULL;
  pVgroup->prev = pDb->pTail;

  if (pDb->pTail) pDb->pTail->next = pVgroup;
  if (pDb->pHead == NULL) pDb->pHead = pVgroup;

  pDb->pTail = pVgroup;
  pDb->numOfVgroups++;
}

void mgmtRemoveVgroupFromDb(SVgObj *pVgroup) {
  SDbObj *pDb = pVgroup->pDb;
  if (pVgroup->prev) pVgroup->prev->next = pVgroup->next;
  if (pVgroup->next) pVgroup->next->prev = pVgroup->prev;
  if (pVgroup->prev == NULL) pDb->pHead = pVgroup->next;
  if (pVgroup->next == NULL) pDb->pTail = pVgroup->prev;
  pDb->numOfVgroups--;
}

void mgmtMoveVgroupToTail(SVgObj *pVgroup) {
  mgmtRemoveVgroupFromDb(pVgroup);
  mgmtAddVgroupIntoDbTail(pVgroup);
}

void mgmtMoveVgroupToHead(SVgObj *pVgroup) {
  mgmtRemoveVgroupFromDb(pVgroup);
  mgmtAddVgroupIntoDb(pVgroup);
}

void mgmtCleanUpDbs() {
  sdbCloseTable(tsDbSdb);
}

static int32_t mgmtGetDbMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  int32_t cols = 0;

  SSchema *pSchema = pMeta->schema;
  SUserObj *pUser = mgmtGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  pShow->bytes[cols] = TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE;
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
  if (strcmp(pUser->user, "root") == 0) {
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
  if (strcmp(pUser->user, "root") == 0) {
#endif
    pShow->bytes[cols] = 2;
    pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
    strcpy(pSchema[cols].name, "replica");
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
  if (strcmp(pUser->user, "root") == 0) {
#endif
    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema[cols].name, "tables");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

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

    pShow->bytes[cols] = 4;
    pSchema[cols].type = TSDB_DATA_TYPE_INT;
    strcpy(pSchema[cols].name, "ctime(s)");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 1;
    pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
    strcpy(pSchema[cols].name, "clog");
    pSchema[cols].bytes = htons(pShow->bytes[cols]);
    cols++;

    pShow->bytes[cols] = 1;
    pSchema[cols].type = TSDB_DATA_TYPE_TINYINT;
    strcpy(pSchema[cols].name, "comp");
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

  mgmtDecUserRef(pUser);
  return 0;
}

static char *mgmtGetDbStr(char *src) {
  char *pos = strstr(src, TS_PATH_DELIMITER);
  return ++pos;
}

static int32_t mgmtRetrieveDbs(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t numOfRows = 0;
  SDbObj *pDb = NULL;
  char *  pWrite;
  int32_t cols = 0;
  SUserObj *pUser = mgmtGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  while (numOfRows < rows) {
    pShow->pNode = mgmtGetNextDb(pShow->pNode, &pDb);
    if (pDb == NULL) break;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    
    char* name = mgmtGetDbStr(pDb->name);
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, name, TSDB_DB_NAME_LEN);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pDb->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pDb->numOfTables;
    cols++;

#ifndef __CLOUD_VERSION__
    if (strcmp(pUser->user, "root") == 0) {
#endif
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = pDb->numOfVgroups;
      cols++;
#ifndef __CLOUD_VERSION__
    }
#endif

#ifndef __CLOUD_VERSION__
    if (strcmp(pUser->user, "root") == 0) {
#endif
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *)pWrite = pDb->cfg.replications;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int16_t *)pWrite = pDb->cfg.daysPerFile;
      cols++;
#ifndef __CLOUD_VERSION__
    }
#endif

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    
    char tmp[128] = {0};
    size_t n = sprintf(tmp, "%d,%d,%d", pDb->cfg.daysToKeep1, pDb->cfg.daysToKeep2, pDb->cfg.daysToKeep);
    STR_WITH_SIZE_TO_VARSTR(pWrite, tmp, n);
    cols++;

#ifndef __CLOUD_VERSION__
    if (strcmp(pUser->user, "root") == 0) {
#endif
      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int32_t *)pWrite = pDb->cfg.maxTables;  // table num can be created should minus 1
      cols++;

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
      *(int32_t *)pWrite = pDb->cfg.commitTime;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int8_t *)pWrite = pDb->cfg.commitLog;
      cols++;

      pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
      *(int8_t *)pWrite = pDb->cfg.compression;
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
    if (pDb->status == TSDB_DB_STATUS_READY) {
      STR_WITH_SIZE_TO_VARSTR(pWrite, "ready", 5);
    } else {
      STR_WITH_SIZE_TO_VARSTR(pWrite, "dropping", 8);
    }
    cols++;

    numOfRows++;
    mgmtDecDbRef(pDb);
  }

  pShow->numOfReads += numOfRows;
  mgmtDecUserRef(pUser);
  return numOfRows;
}

void mgmtAddSuperTableIntoDb(SDbObj *pDb) {
  atomic_add_fetch_32(&pDb->numOfSuperTables, 1);
}

void mgmtRemoveSuperTableFromDb(SDbObj *pDb) {
  atomic_add_fetch_32(&pDb->numOfSuperTables, -1);
}

void mgmtAddTableIntoDb(SDbObj *pDb) {
  atomic_add_fetch_32(&pDb->numOfTables, 1);
}

void mgmtRemoveTableFromDb(SDbObj *pDb) {
  atomic_add_fetch_32(&pDb->numOfTables, -1);
}

static int32_t mgmtSetDbDropping(SDbObj *pDb) {
  if (pDb->status) return TSDB_CODE_SUCCESS;

  pDb->status = true;
  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsDbSdb,
    .pObj = pDb
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_SDB_ERROR;
  }

  return code;
}

static void mgmtProcessCreateDbMsg(SQueuedMsg *pMsg) {
  SCMCreateDbMsg *pCreate = pMsg->pCont;
  pCreate->maxSessions     = htonl(pCreate->maxSessions);
  pCreate->cacheBlockSize  = htonl(pCreate->cacheBlockSize);
  pCreate->totalBlocks     = htonl(pCreate->totalBlocks);
  pCreate->daysPerFile     = htonl(pCreate->daysPerFile);
  pCreate->daysToKeep      = htonl(pCreate->daysToKeep);
  pCreate->daysToKeep1     = htonl(pCreate->daysToKeep1);
  pCreate->daysToKeep2     = htonl(pCreate->daysToKeep2);
  pCreate->commitTime      = htonl(pCreate->commitTime);
  pCreate->minRowsPerFileBlock = htonl(pCreate->minRowsPerFileBlock);
  pCreate->maxRowsPerFileBlock = htonl(pCreate->maxRowsPerFileBlock);
  
  int32_t code;
  if (grantCheck(TSDB_GRANT_TIME) != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_GRANT_EXPIRED;
  } else if (!pMsg->pUser->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    code = mgmtCreateDb(pMsg->pUser->pAcct, pCreate);
    if (code == TSDB_CODE_SUCCESS) {
      mLPrint("db:%s, is created by %s", pCreate->db, pMsg->pUser->user);
    }
  }

  mgmtSendSimpleResp(pMsg->thandle, code);
}

static SDbCfg mgmtGetAlterDbOption(SDbObj *pDb, SCMAlterDbMsg *pAlter) {
  SDbCfg  newCfg = pDb->cfg;
  int32_t cacheBlockSize = htonl(pAlter->daysToKeep);
  int32_t totalBlocks = htonl(pAlter->totalBlocks);
  int32_t maxTables = htonl(pAlter->maxSessions);
  int32_t daysToKeep = htonl(pAlter->daysToKeep);
  int32_t daysToKeep1 = htonl(pAlter->daysToKeep1);
  int32_t daysToKeep2 = htonl(pAlter->daysToKeep2);
  int8_t  compression = pAlter->compression;
  int8_t  replications = pAlter->replications;

  terrno = TSDB_CODE_SUCCESS;

  if (cacheBlockSize > 0 && cacheBlockSize != pDb->cfg.cacheBlockSize) {
    mTrace("db:%s, cache:%d change to %d", pDb->name, pDb->cfg.cacheBlockSize, cacheBlockSize);
    newCfg.cacheBlockSize = cacheBlockSize;
  }

  if (totalBlocks > 0 && totalBlocks != pDb->cfg.totalBlocks) {
    mTrace("db:%s, blocks:%d change to %d", pDb->name, pDb->cfg.totalBlocks, totalBlocks);
    newCfg.totalBlocks = totalBlocks;
  }

  if (maxTables > 0 && maxTables != pDb->cfg.maxTables) {
    mTrace("db:%s, tables:%d change to %d", pDb->name, pDb->cfg.maxTables, maxTables);
    newCfg.maxTables = maxTables;
    if (newCfg.maxTables < pDb->cfg.maxTables) {
      mTrace("db:%s, tables:%d should larger than origin:%d", pDb->name, newCfg.maxTables, pDb->cfg.maxTables);
      terrno = TSDB_CODE_INVALID_OPTION;
    }
  }

  if (daysToKeep > 0 && daysToKeep != pDb->cfg.daysToKeep) {
    mTrace("db:%s, daysToKeep:%d change to %d", pDb->name, pDb->cfg.daysToKeep, daysToKeep);
    newCfg.daysToKeep = daysToKeep;
  }

  if (daysToKeep1 > 0 && daysToKeep1 != pDb->cfg.daysToKeep1) {
    mTrace("db:%s, daysToKeep1:%d change to %d", pDb->name, pDb->cfg.daysToKeep1, daysToKeep1);
    newCfg.daysToKeep1 = daysToKeep1;
  }

  if (daysToKeep2 > 0 && daysToKeep2 != pDb->cfg.daysToKeep2) {
    mTrace("db:%s, daysToKeep2:%d change to %d", pDb->name, pDb->cfg.daysToKeep2, daysToKeep2);
    newCfg.daysToKeep2 = daysToKeep2;
  }

  if (compression > 0 && compression != pDb->cfg.compression) {
    mTrace("db:%s, compression:%d change to %d", pDb->name, pDb->cfg.compression, compression);
    newCfg.compression = compression;
  }

  if (replications > 0 && replications != pDb->cfg.replications) {
    mTrace("db:%s, replications:%d change to %d", pDb->name, pDb->cfg.replications, replications);
    newCfg.replications = replications;
  } 

  if (replications > mgmtGetDnodesNum()) {
    mError("db:%s, no enough dnode to change replica:%d", pDb->name, replications);
    terrno = TSDB_CODE_NO_ENOUGH_DNODES;
  }

  if (pDb->cfg.replications - replications >= 2) {
    mError("db:%s, replica number can't change from 3 to 1", pDb->name, replications);
    terrno = TSDB_CODE_INVALID_OPTION;
  }

  return newCfg;
}

static int32_t mgmtAlterDb(SDbObj *pDb, SCMAlterDbMsg *pAlter) {
  SDbCfg newCfg = mgmtGetAlterDbOption(pDb, pAlter);
  if (terrno != TSDB_CODE_SUCCESS) {
    return terrno;
  }

  int32_t code = mgmtCheckDbCfg(&newCfg);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  int32_t oldReplica = pDb->cfg.replications;

  if (memcmp(&newCfg, &pDb->cfg, sizeof(SDbCfg)) != 0) {
    pDb->cfg = newCfg;
    pDb->cfgVersion++;
    SSdbOper oper = {
      .type = SDB_OPER_GLOBAL,
      .table = tsDbSdb,
      .pObj = pDb
    };

    int32_t code = sdbUpdateRow(&oper);
    if (code != TSDB_CODE_SUCCESS) {
      return TSDB_CODE_SDB_ERROR;
    }
  }

  void *pNode = NULL;
  while (1) {
    SVgObj *pVgroup = NULL;
    pNode = mgmtGetNextVgroup(pNode, &pVgroup);
    if (pVgroup == NULL) break;   
    mgmtSendCreateVgroupMsg(pVgroup, NULL);
    mgmtDecVgroupRef(pVgroup);
  }

  if (oldReplica != pDb->cfg.replications) {
    balanceNotify();
  }

  return TSDB_CODE_SUCCESS;
}

static void mgmtProcessAlterDbMsg(SQueuedMsg *pMsg) {
  SCMAlterDbMsg *pAlter = pMsg->pCont;
  mTrace("db:%s, alter db msg is received from thandle:%p", pAlter->db, pMsg->thandle);

  if (grantCheck(TSDB_GRANT_TIME) != TSDB_CODE_SUCCESS) {
    mError("db:%s, failed to alter, grant expired", pAlter->db);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_GRANT_EXPIRED);
    return;
  }

  if (pMsg->pDb == NULL) pMsg->pDb = mgmtGetDb(pAlter->db);
  if (pMsg->pDb == NULL) {
    mError("db:%s, failed to alter, invalid db", pAlter->db);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_DB);
    return;
  }

  int32_t code = mgmtAlterDb(pMsg->pDb, pAlter);
  if (code != TSDB_CODE_SUCCESS) {
    mError("db:%s, failed to alter, invalid db option", pAlter->db);
    mgmtSendSimpleResp(pMsg->thandle, code);
    return;
  }

  mTrace("db:%s, all vgroups is altered", pMsg->pDb->name);
  mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SUCCESS);
}

static void mgmtDropDb(SQueuedMsg *pMsg) {
  SDbObj *pDb = pMsg->pDb;
  mPrint("db:%s, drop db from sdb", pDb->name);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsDbSdb,
    .pObj = pDb
  };
  int32_t code = sdbDeleteRow(&oper);
  if (code != 0) {
    code = TSDB_CODE_SDB_ERROR;
  }

  mgmtSendSimpleResp(pMsg->thandle, code);
}

static void mgmtProcessDropDbMsg(SQueuedMsg *pMsg) {
  SCMDropDbMsg *pDrop = pMsg->pCont;
  mTrace("db:%s, drop db msg is received from thandle:%p", pDrop->db, pMsg->thandle);

  if (grantCheck(TSDB_GRANT_TIME) != TSDB_CODE_SUCCESS) {
    mError("db:%s, failed to drop, grant expired", pDrop->db);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_GRANT_EXPIRED);
    return;
  }

  if (pMsg->pDb == NULL) pMsg->pDb = mgmtGetDb(pDrop->db);
  if (pMsg->pDb == NULL) {
    if (pDrop->ignoreNotExists) {
      mTrace("db:%s, db is not exist, think drop success", pDrop->db);
      mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SUCCESS);
      return;
    } else {
      mError("db:%s, failed to drop, invalid db", pDrop->db);
      mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_DB);
      return;
    }
  }

  if (mgmtCheckIsMonitorDB(pMsg->pDb->name, tsMonitorDbName)) {
    mError("db:%s, can't drop monitor database", pDrop->db);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_MONITOR_DB_FORBIDDEN);
    return;
  }

  int32_t code = mgmtSetDbDropping(pMsg->pDb);
  if (code != TSDB_CODE_SUCCESS) {
    mError("db:%s, failed to drop, reason:%s", pDrop->db, tstrerror(code));
    mgmtSendSimpleResp(pMsg->thandle, code);
    return;
  }

#if 1
  mgmtDropAllDbVgroups(pMsg->pDb, true);
#else
  SVgObj *pVgroup = pMsg->pDb->pHead;
  if (pVgroup != NULL) {
    mPrint("vgId:%d, will be dropped", pVgroup->vgId);
    SQueuedMsg *newMsg = mgmtCloneQueuedMsg(pMsg);
    newMsg->ahandle = pVgroup;
    newMsg->expected = pVgroup->numOfVnodes;
    mgmtDropVgroup(pVgroup, newMsg);
    return;
  }
#endif  

  mTrace("db:%s, all vgroups is dropped", pMsg->pDb->name);
  mgmtDropDb(pMsg);
}

void  mgmtDropAllDbs(SAcctObj *pAcct)  {
  int32_t numOfDbs = 0;
  SDbObj *pDb = NULL;
  void *  pNode = NULL;

  mPrint("acct:%s, all dbs will be dropped from sdb", pAcct->user);

  while (1) {
    pNode = mgmtGetNextDb(pNode, &pDb);
    if (pDb == NULL) break;

    if (pDb->pAcct == pAcct) {
      mPrint("db:%s, drop db from sdb for acct:%s is dropped", pDb->name, pAcct->user);
      SSdbOper oper = {
        .type = SDB_OPER_LOCAL,
        .table = tsDbSdb,
        .pObj = pDb
      };
      
      sdbDeleteRow(&oper);
      numOfDbs++;
    }
    mgmtDecDbRef(pDb);
  }

  mPrint("acct:%s, all dbs:%d is dropped from sdb", pAcct->user, numOfDbs);
}
