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
#include "taosmsg.h"
#include "tschemautil.h"
#include "tscompression.h"
#include "tskiplist.h"
#include "tsqlfunction.h"
#include "ttime.h"
#include "tstatus.h"
#include "tutil.h"
#include "mnode.h"
#include "mgmtAcct.h"
#include "mgmtChildTable.h"
#include "mgmtDb.h"
#include "mgmtDnode.h"
#include "mgmtGrant.h"
#include "mgmtShell.h"
#include "mgmtSuperTable.h"
#include "mgmtTable.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"

static void *tsSuperTableSdb;
static int32_t tsSuperTableUpdateSize;

static void *(*mgmtSuperTableActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtSuperTableActionInsert(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtSuperTableActionDelete(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtSuperTableActionUpdate(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtSuperTableActionEncode(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtSuperTableActionDecode(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtSuperTableActionReset(void *row, char *str, int32_t size, int32_t *ssize);
static void *mgmtSuperTableActionDestroy(void *row, char *str, int32_t size, int32_t *ssize);
static int32_t mgmtRetrieveShowSuperTables(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mgmtGetShowSuperTableMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn);

static void mgmtDestroySuperTable(SSuperTableObj *pTable) {
  free(pTable->schema);
  free(pTable);
}

static void mgmtSuperTableActionInit() {
  SSuperTableObj tObj;
  tsSuperTableUpdateSize = tObj.updateEnd - (int8_t *)&tObj;

  mgmtSuperTableActionFp[SDB_TYPE_INSERT]  = mgmtSuperTableActionInsert;
  mgmtSuperTableActionFp[SDB_TYPE_DELETE]  = mgmtSuperTableActionDelete;
  mgmtSuperTableActionFp[SDB_TYPE_UPDATE]  = mgmtSuperTableActionUpdate;
  mgmtSuperTableActionFp[SDB_TYPE_ENCODE]  = mgmtSuperTableActionEncode;
  mgmtSuperTableActionFp[SDB_TYPE_DECODE]  = mgmtSuperTableActionDecode;
  mgmtSuperTableActionFp[SDB_TYPE_RESET]   = mgmtSuperTableActionReset;
  mgmtSuperTableActionFp[SDB_TYPE_DESTROY] = mgmtSuperTableActionDestroy;
}

void *mgmtSuperTableActionReset(void *row, char *str, int32_t size, int32_t *ssize) {
  SSuperTableObj *pTable = (SSuperTableObj *) row;
  memcpy(pTable, str, tsSuperTableUpdateSize);

  int32_t schemaSize = sizeof(SSchema) * (pTable->numOfColumns + pTable->numOfTags);
  pTable->schema = realloc(pTable->schema, schemaSize);
  memcpy(pTable->schema, str + tsSuperTableUpdateSize, schemaSize);

  return NULL;
}

void *mgmtSuperTableActionDestroy(void *row, char *str, int32_t size, int32_t *ssize) {
  SSuperTableObj *pTable = (SSuperTableObj *) row;
  mgmtDestroySuperTable(pTable);
  return NULL;
}

void *mgmtSuperTableActionInsert(void *row, char *str, int32_t size, int32_t *ssize) {
  STableInfo *pTable = (STableInfo *) row;
  SDbObj     *pDb    = mgmtGetDbByTableId(pTable->tableId);
  if (pDb) {
    mgmtAddSuperTableIntoDb(pDb);
  }
  return NULL;
}

void *mgmtSuperTableActionDelete(void *row, char *str, int32_t size, int32_t *ssize) {
  STableInfo *pTable = (STableInfo *) row;
  SDbObj     *pDb    = mgmtGetDbByTableId(pTable->tableId);
  if (pDb) {
    mgmtRemoveSuperTableFromDb(pDb);
  }
  return NULL;
}

void *mgmtSuperTableActionUpdate(void *row, char *str, int32_t size, int32_t *ssize) {
  return mgmtSuperTableActionReset(row, str, size, NULL);
}

void *mgmtSuperTableActionEncode(void *row, char *str, int32_t size, int32_t *ssize) {
  SSuperTableObj *pTable = (SSuperTableObj *) row;
  assert(row != NULL && str != NULL);

  int32_t schemaSize = sizeof(SSchema) * (pTable->numOfColumns + pTable->numOfTags);

  if (size < tsSuperTableUpdateSize + schemaSize + 1) {
    *ssize = -1;
    return NULL;
  }

  memcpy(str, pTable, tsSuperTableUpdateSize);
  memcpy(str + tsSuperTableUpdateSize, pTable->schema, schemaSize);
  *ssize = tsSuperTableUpdateSize + schemaSize;

  return NULL;
}

void *mgmtSuperTableActionDecode(void *row, char *str, int32_t size, int32_t *ssize) {
  assert(str != NULL);

  SSuperTableObj *pTable = (SSuperTableObj *) malloc(sizeof(SSuperTableObj));
  if (pTable == NULL) {
    return NULL;
  }
  memset(pTable, 0, sizeof(SSuperTableObj));

  if (size < tsSuperTableUpdateSize) {
    mgmtDestroySuperTable(pTable);
    return NULL;
  }
  memcpy(pTable, str, tsSuperTableUpdateSize);

  int32_t schemaSize = sizeof(SSchema) * (pTable->numOfColumns + pTable->numOfTags);
  pTable->schema = malloc(schemaSize);
  if (pTable->schema == NULL) {
    mgmtDestroySuperTable(pTable);
    return NULL;
  }

  memcpy(pTable->schema, str + tsSuperTableUpdateSize, schemaSize);
  return (void *) pTable;
}

void *mgmtSuperTableAction(char action, void *row, char *str, int32_t size, int32_t *ssize) {
  if (mgmtSuperTableActionFp[(uint8_t) action] != NULL) {
    return (*(mgmtSuperTableActionFp[(uint8_t) action]))(row, str, size, ssize);
  }
  return NULL;
}

int32_t mgmtInitSuperTables() {
  void *pNode     = NULL;
  void *pLastNode = NULL;
  SSuperTableObj *pTable = NULL;

  mgmtSuperTableActionInit();

  tsSuperTableSdb = sdbOpenTable(tsMaxTables, tsSuperTableUpdateSize + sizeof(SSchema) * TSDB_MAX_COLUMNS,
                          "stables", SDB_KEYTYPE_STRING, tsMnodeDir, mgmtSuperTableAction);
  if (tsSuperTableSdb == NULL) {
    mError("failed to init stables data");
    return -1;
  }

  pNode = NULL;
  while (1) {
    pNode = sdbFetchRow(tsSuperTableSdb, pNode, (void **) &pTable);
    if (pTable == NULL) {
      break;
    }

    SDbObj *pDb = mgmtGetDbByTableId(pTable->tableId);
    if (pDb == NULL) {
      mError("super table:%s, failed to get db, discard it", pTable->tableId);
      sdbDeleteRow(tsSuperTableSdb, pTable);
      pNode = pLastNode;
      continue;
    }

    mgmtAddSuperTableIntoDb(pDb);
  }

  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_METRIC, mgmtGetShowSuperTableMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_METRIC, mgmtRetrieveShowSuperTables);

  mTrace("stables is initialized");
  return 0;
}

void mgmtCleanUpSuperTables() {
  sdbCloseTable(tsSuperTableSdb);
}

int32_t mgmtCreateSuperTable(SDbObj *pDb, SCMCreateTableMsg *pCreate) {
  int32_t numOfTables = sdbGetNumOfRows(tsSuperTableSdb);
  if (numOfTables >= TSDB_MAX_SUPER_TABLES) {
    mError("stable:%s, numOfTables:%d exceed maxTables:%d", pCreate->tableId, numOfTables, TSDB_MAX_SUPER_TABLES);
    return TSDB_CODE_TOO_MANY_TABLES;
  }

  SSuperTableObj *pStable = (SSuperTableObj *)calloc(sizeof(SSuperTableObj), 1);
  if (pStable == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  strcpy(pStable->tableId, pCreate->tableId);
  pStable->type        = TSDB_SUPER_TABLE;
  pStable->createdTime = taosGetTimestampMs();
  pStable->vgId = 0;
  pStable->sid = 0;
  pStable->uid = (((uint64_t)pStable->createdTime) << 16) + ((uint64_t)sdbGetVersion() & ((1ul << 16) - 1ul));
  pStable->sversion = 0;
  pStable->numOfColumns = pCreate->numOfColumns;
  pStable->numOfTags = pCreate->numOfTags;

  int32_t numOfCols = pCreate->numOfColumns + pCreate->numOfTags;
  int32_t schemaSize = numOfCols * sizeof(SSchema);
  pStable->schema = (SSchema *)calloc(1, schemaSize);
  if (pStable->schema == NULL) {
    free(pStable);
    mError("stable:%s, no schema input", pCreate->tableId);
    return TSDB_CODE_INVALID_TABLE;
  }
  memcpy(pStable->schema, pCreate->schema, numOfCols * sizeof(SSchema));

  pStable->nextColId = 0;
  for (int32_t col = 0; col < pCreate->numOfColumns; col++) {
    SSchema *tschema = (SSchema *)pStable->schema;
    tschema[col].colId = pStable->nextColId++;
  }

  if (sdbInsertRow(tsSuperTableSdb, pStable, 0) < 0) {
    mError("table:%s, update sdb error", pCreate->tableId);
    return TSDB_CODE_SDB_ERROR;
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mgmtDropSuperTable(SDbObj *pDb, SSuperTableObj *pSuperTable) {
  //TODO drop all child tables

  mgmtRemoveSuperTableFromDb(pDb);
  return sdbDeleteRow(tsSuperTableSdb, pSuperTable);
}

void* mgmtGetSuperTable(char *tableId) {
  return sdbGetRow(tsSuperTableSdb, tableId);
}

void *mgmtGetSuperTableVgroup(SSuperTableObj *pStable) {
  //TODO get vgroup of dnodes
  SCMSuperTableInfoRsp *rsp = rpcMallocCont(sizeof(SCMSuperTableInfoRsp) + sizeof(uint32_t) * mgmtGetDnodesNum());
  rsp->numOfDnodes = 1;
  rsp->dnodeIps[0] = 0;
  return rsp;
}

int32_t mgmtFindSuperTableTagIndex(SSuperTableObj *pStable, const char *tagName) {
  for (int32_t i = 0; i < pStable->numOfTags; i++) {
    SSchema *schema = (SSchema *)(pStable->schema + (pStable->numOfColumns + i) * sizeof(SSchema));
    if (strcasecmp(tagName, schema->name) == 0) {
      return i;
    }
  }

  return -1;
}

int32_t mgmtAddSuperTableTag(SSuperTableObj *pStable, SSchema schema[], int32_t ntags) {
  if (pStable->numOfTags + ntags > TSDB_MAX_TAGS) {
    return TSDB_CODE_APP_ERROR;
  }

  // check if schemas have the same name
  for (int32_t i = 1; i < ntags; i++) {
    for (int32_t j = 0; j < i; j++) {
      if (strcasecmp(schema[i].name, schema[j].name) == 0) {
        return TSDB_CODE_APP_ERROR;
      }
    }
  }

  SDbObj *pDb = mgmtGetDbByTableId(pStable->tableId);
  if (pDb == NULL) {
    mError("meter: %s not belongs to any database", pStable->tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("DB: %s not belongs to andy account", pDb->name);
    return TSDB_CODE_APP_ERROR;
  }

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfTags + pStable->numOfColumns);
  pStable->schema = realloc(pStable->schema, schemaSize + sizeof(SSchema) * ntags);

  memmove(pStable->schema + sizeof(SSchema) * (pStable->numOfColumns + ntags),
          pStable->schema + sizeof(SSchema) * pStable->numOfColumns, sizeof(SSchema) * pStable->numOfTags);
  memcpy(pStable->schema + sizeof(SSchema) * pStable->numOfColumns, schema, sizeof(SSchema) * ntags);

  SSchema *tschema = (SSchema *) (pStable->schema + sizeof(SSchema) * pStable->numOfColumns);
  for (int32_t i = 0; i < ntags; i++) {
    tschema[i].colId = pStable->nextColId++;
  }

  pStable->numOfColumns += ntags;
  pStable->sversion++;

  pAcct->acctInfo.numOfTimeSeries += (ntags * pStable->numOfTables);
  sdbUpdateRow(tsSuperTableSdb, pStable, 0, 1);

  mTrace("Succeed to add tag column %s to table %s", schema[0].name, pStable->tableId);
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtDropSuperTableTag(SSuperTableObj *pStable, char *tagName) {
  int32_t col = mgmtFindSuperTableTagIndex(pStable, tagName);
  if (col <= 0 || col >= pStable->numOfTags) {
    return TSDB_CODE_APP_ERROR;
  }

  SDbObj *pDb = mgmtGetDbByTableId(pStable->tableId);
  if (pDb == NULL) {
    mError("table: %s not belongs to any database", pStable->tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("DB: %s not belongs to any account", pDb->name);
    return TSDB_CODE_APP_ERROR;
  }

  memmove(pStable->schema + sizeof(SSchema) * col, pStable->schema + sizeof(SSchema) * (col + 1),
          sizeof(SSchema) * (pStable->numOfColumns + pStable->numOfTags - col - 1));

  pStable->numOfTags--;
  pStable->sversion++;

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfTags + pStable->numOfColumns);
  pStable->schema = realloc(pStable->schema, schemaSize);

  sdbUpdateRow(tsSuperTableSdb, pStable, 0, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t mgmtModifySuperTableTagNameByName(SSuperTableObj *pStable, char *oldTagName, char *newTagName) {
  int32_t col = mgmtFindSuperTableTagIndex(pStable, oldTagName);
  if (col < 0) {
    // Tag name does not exist
    mError("Failed to modify table %s tag column, oname: %s, nname: %s", pStable->tableId, oldTagName, newTagName);
    return TSDB_CODE_INVALID_MSG_TYPE;
  }

  int32_t      rowSize = 0;
  uint32_t len     = strlen(newTagName);

  if (col >= pStable->numOfTags || len >= TSDB_COL_NAME_LEN || mgmtFindSuperTableTagIndex(pStable, newTagName) >= 0) {
    return TSDB_CODE_APP_ERROR;
  }

  // update
  SSchema *schema = (SSchema *) (pStable->schema + (pStable->numOfColumns + col) * sizeof(SSchema));
  strncpy(schema->name, newTagName, TSDB_COL_NAME_LEN);

  // Encode string
  int32_t  size = 1 + sizeof(SSuperTableObj) + TSDB_MAX_BYTES_PER_ROW;
  char *msg = (char *) malloc(size);
  if (msg == NULL) return TSDB_CODE_APP_ERROR;
  memset(msg, 0, size);

  mgmtSuperTableActionEncode(pStable, msg, size, &rowSize);

  int32_t ret = sdbUpdateRow(tsSuperTableSdb, msg, rowSize, 1);
  tfree(msg);

  if (ret < 0) {
    mError("Failed to modify table %s tag column", pStable->tableId);
    return TSDB_CODE_APP_ERROR;
  }

  mTrace("Succeed to modify table %s tag column", pStable->tableId);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtFindSuperTableColumnIndex(SSuperTableObj *pStable, char *colName) {
  SSchema *schema = (SSchema *) pStable->schema;
  for (int32_t i = 0; i < pStable->numOfColumns; i++) {
    if (strcasecmp(schema[i].name, colName) == 0) {
      return i;
    }
  }

  return -1;
}

int32_t mgmtAddSuperTableColumn(SSuperTableObj *pStable, SSchema schema[], int32_t ncols) {
  if (ncols <= 0) {
    return TSDB_CODE_APP_ERROR;
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (mgmtFindSuperTableColumnIndex(pStable, schema[i].name) > 0) {
      return TSDB_CODE_APP_ERROR;
    }
  }

  SDbObj *pDb = mgmtGetDbByTableId(pStable->tableId);
  if (pDb == NULL) {
    mError("meter: %s not belongs to any database", pStable->tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("DB: %s not belongs to andy account", pDb->name);
    return TSDB_CODE_APP_ERROR;
  }

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfTags + pStable->numOfColumns);
  pStable->schema = realloc(pStable->schema, schemaSize + sizeof(SSchema) * ncols);

  memmove(pStable->schema + sizeof(SSchema) * (pStable->numOfColumns + ncols),
          pStable->schema + sizeof(SSchema) * pStable->numOfColumns, sizeof(SSchema) * pStable->numOfTags);
  memcpy(pStable->schema + sizeof(SSchema) * pStable->numOfColumns, schema, sizeof(SSchema) * ncols);

  SSchema *tschema = (SSchema *) (pStable->schema + sizeof(SSchema) * pStable->numOfColumns);
  for (int32_t i = 0; i < ncols; i++) {
    tschema[i].colId = pStable->nextColId++;
  }

  pStable->numOfColumns += ncols;
  pStable->sversion++;

  pAcct->acctInfo.numOfTimeSeries += (ncols * pStable->numOfTables);
  sdbUpdateRow(tsSuperTableSdb, pStable, 0, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t mgmtDropSuperTableColumnByName(SSuperTableObj *pStable, char *colName) {
  int32_t col = mgmtFindSuperTableColumnIndex(pStable, colName);
  if (col < 0) {
    return TSDB_CODE_APP_ERROR;
  }

  SDbObj *pDb = mgmtGetDbByTableId(pStable->tableId);
  if (pDb == NULL) {
    mError("table: %s not belongs to any database", pStable->tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("DB: %s not belongs to any account", pDb->name);
    return TSDB_CODE_APP_ERROR;
  }

  memmove(pStable->schema + sizeof(SSchema) * col, pStable->schema + sizeof(SSchema) * (col + 1),
          sizeof(SSchema) * (pStable->numOfColumns + pStable->numOfTags - col - 1));

  pStable->numOfColumns--;
  pStable->sversion++;

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfTags + pStable->numOfColumns);
  pStable->schema = realloc(pStable->schema, schemaSize);

  pAcct->acctInfo.numOfTimeSeries -= (pStable->numOfTables);
  sdbUpdateRow(tsSuperTableSdb, pStable, 0, 1);

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtGetShowSuperTableMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mgmtGetDb(pShow->db);
  if (pDb == NULL) {
    return TSDB_CODE_DB_NOT_SELECTED;
  }

  int32_t cols = 0;
  SSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "created_time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "columns");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "tags");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "tables");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = pDb->numOfSuperTables;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

int32_t mgmtRetrieveShowSuperTables(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t         numOfRows = 0;
  char *          pWrite;
  int32_t         cols = 0;
  SSuperTableObj *pTable = NULL;
  char            prefix[20] = {0};
  int32_t         prefixLen;

  SDbObj *pDb = mgmtGetDb(pShow->db);
  if (pDb == NULL) return 0;

  SUserObj *pUser = mgmtGetUserFromConn(pConn);

  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    if (strcmp(pUser->user, "root") != 0 && strcmp(pUser->user, "_root") != 0 && strcmp(pUser->user, "monitor") != 0 ) {
      return 0;
    }
  }

  strcpy(prefix, pDb->name);
  strcat(prefix, TS_PATH_DELIMITER);
  prefixLen = strlen(prefix);

  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;
  char stableName[TSDB_TABLE_NAME_LEN] = {0};

  while (numOfRows < rows) {
    pShow->pNode = sdbFetchRow(tsSuperTableSdb, pShow->pNode, (void **) &pTable);
    if (pTable == NULL) break;
    if (strncmp(pTable->tableId, prefix, prefixLen)) {
      continue;
    }

    memset(stableName, 0, tListLen(stableName));
    extractTableName(pTable->tableId, stableName);

    if (pShow->payloadLen > 0 &&
        patternMatch(pShow->payload, stableName, TSDB_TABLE_NAME_LEN, &info) != TSDB_PATTERN_MATCH)
      continue;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strncpy(pWrite, stableName, TSDB_TABLE_NAME_LEN);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pTable->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pTable->numOfColumns;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pTable->numOfTags;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pTable->numOfTables;
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

void mgmtAddTableIntoSuperTable(SSuperTableObj *pStable) {
  pStable->numOfTables++;
}

void mgmtRemoveTableFromSuperTable(SSuperTableObj *pStable) {
  pStable->numOfTables--;
}

int32_t mgmtSetSchemaFromSuperTable(SSchema *pSchema, SSuperTableObj *pTable) {
  int32_t numOfCols = pTable->numOfColumns + pTable->numOfTags;
  for (int32_t i = 0; i < numOfCols; ++i) {
    strcpy(pSchema->name, pTable->schema[i].name);
    pSchema->type  = pTable->schema[i].type;
    pSchema->bytes = htons(pTable->schema[i].bytes);
    pSchema->colId = htons(pTable->schema[i].colId);
    pSchema++;
  }

  return (pTable->numOfColumns + pTable->numOfTags) * sizeof(SSchema);
}

int32_t mgmtGetSuperTableMeta(SDbObj *pDb, SSuperTableObj *pTable, STableMeta *pMeta, bool usePublicIp) {
  pMeta->uid          = htobe64(pTable->uid);
  pMeta->sid          = htonl(pTable->sid);
  pMeta->vgid         = htonl(pTable->vgId);
  pMeta->sversion     = htons(pTable->sversion);
  pMeta->precision    = pDb->cfg.precision;
  pMeta->numOfTags    = pTable->numOfTags;
  pMeta->numOfColumns = htons(pTable->numOfColumns);
  pMeta->tableType    = pTable->type;
  pMeta->contLen      = sizeof(STableMeta) + mgmtSetSchemaFromSuperTable(pMeta->schema, pTable);
  strcpy(pMeta->tableId, pTable->tableId);

  return TSDB_CODE_SUCCESS;
}

