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
#include "name.h"
#include "tsqlfunction.h"
#include "mgmtAcct.h"
#include "mgmtChildTable.h"
#include "mgmtDb.h"
#include "mgmtDClient.h"
#include "mgmtDnode.h"
#include "mgmtGrant.h"
#include "mgmtShell.h"
#include "mgmtSuperTable.h"
#include "mgmtSdb.h"
#include "mgmtTable.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"

static void   *tsSuperTableSdb;
static int32_t tsSuperTableUpdateSize;
static void    mgmtProcessSuperTableVgroupMsg(SQueuedMsg *queueMsg);
static void    mgmtProcessDropStableRsp(SRpcMsg *rpcMsg);
static int32_t mgmtRetrieveShowSuperTables(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mgmtGetShowSuperTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);

static void mgmtDestroySuperTable(SSuperTableObj *pStable) {
  tfree(pStable->schema);
  tfree(pStable);
}

static int32_t mgmtSuperTableActionDestroy(SSdbOperDesc *pOper) {
  mgmtDestroySuperTable(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtSuperTableActionInsert(SSdbOperDesc *pOper) {
  SSuperTableObj *pStable = pOper->pObj;
  SDbObj *pDb = mgmtGetDbByTableId(pStable->info.tableId);
  if (pDb != NULL) {
    mgmtAddSuperTableIntoDb(pDb);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtSuperTableActionDelete(SSdbOperDesc *pOper) {
  SSuperTableObj *pStable = pOper->pObj;
  SDbObj *pDb = mgmtGetDbByTableId(pStable->info.tableId);
  if (pDb != NULL) {
    mgmtRemoveSuperTableFromDb(pDb);
    mgmtDropAllChildTablesInStable((SSuperTableObj *)pStable);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtSuperTableActionUpdate(SSdbOperDesc *pOper) {
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtSuperTableActionEncode(SSdbOperDesc *pOper) {
  SSuperTableObj *pStable = pOper->pObj;
  assert(pOper->pObj != NULL && pOper->rowData != NULL);

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfColumns + pStable->numOfTags);

  if (pOper->maxRowSize < tsSuperTableUpdateSize + schemaSize) {
    return TSDB_CODE_INVALID_MSG_LEN;
  }

  memcpy(pOper->rowData, pStable, tsSuperTableUpdateSize);
  memcpy(pOper->rowData + tsSuperTableUpdateSize, pStable->schema, schemaSize);
  pOper->rowSize = tsSuperTableUpdateSize + schemaSize;

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtSuperTableActionDecode(SSdbOperDesc *pOper) {
  assert(pOper->rowData != NULL);

  SSuperTableObj *pStable = (SSuperTableObj *) calloc(1, sizeof(SSuperTableObj));
  if (pStable == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

  memcpy(pStable, pOper->rowData, tsSuperTableUpdateSize);

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfColumns + pStable->numOfTags);
  pStable->schema = malloc(schemaSize);
  if (pStable->schema == NULL) {
    mgmtDestroySuperTable(pStable);
    return -1;
  }

  memcpy(pStable->schema, pOper->rowData + tsSuperTableUpdateSize, schemaSize);
  pOper->pObj = pStable;

  return TSDB_CODE_SUCCESS;
}

int32_t mgmtInitSuperTables() {
  SSuperTableObj tObj;
  tsSuperTableUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableName    = "stables",
    .hashSessions = TSDB_MAX_SUPER_TABLES,
    .maxRowSize   = tsSuperTableUpdateSize + sizeof(SSchema) * TSDB_MAX_COLUMNS,
    .keyType      = SDB_KEY_TYPE_STRING,
    .insertFp     = mgmtSuperTableActionInsert,
    .deleteFp     = mgmtSuperTableActionDelete,
    .updateFp     = mgmtSuperTableActionUpdate,
    .encodeFp     = mgmtSuperTableActionEncode,
    .decodeFp     = mgmtSuperTableActionDecode,
    .destroyFp    = mgmtSuperTableActionDestroy,
  };

  tsSuperTableSdb = sdbOpenTable(&tableDesc);
  if (tsSuperTableSdb == NULL) {
    mError("failed to init stables data");
    return -1;
  }

  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_STABLE_VGROUP, mgmtProcessSuperTableVgroupMsg);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_METRIC, mgmtGetShowSuperTableMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_METRIC, mgmtRetrieveShowSuperTables);
  mgmtAddDClientRspHandle(TSDB_MSG_TYPE_MD_DROP_STABLE_RSP, mgmtProcessDropStableRsp);

  mTrace("stables is initialized");
  return 0;
}

void mgmtCleanUpSuperTables() {
  sdbCloseTable(tsSuperTableSdb);
}

void mgmtCreateSuperTable(SQueuedMsg *pMsg) {
  SCMCreateTableMsg *pCreate = pMsg->pCont;
  SSuperTableObj *pStable = (SSuperTableObj *)calloc(1, sizeof(SSuperTableObj));
  if (pStable == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  strcpy(pStable->info.tableId, pCreate->tableId);
  pStable->info.type    = TSDB_SUPER_TABLE;
  pStable->createdTime  = taosGetTimestampMs();
  pStable->uid          = (((uint64_t) pStable->createdTime) << 16) + (sdbGetVersion() & ((1ul << 16) - 1ul));
  pStable->sversion     = 0;
  pStable->numOfColumns = htons(pCreate->numOfColumns);
  pStable->numOfTags    = htons(pCreate->numOfTags);

  int32_t numOfCols = pCreate->numOfColumns + pCreate->numOfTags;
  int32_t schemaSize = numOfCols * sizeof(SSchema);
  pStable->schema = (SSchema *)calloc(1, schemaSize);
  if (pStable->schema == NULL) {
    free(pStable);
    mError("stable:%s, no schema input", pCreate->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_TABLE);
    return;
  }
  memcpy(pStable->schema, pCreate->schema, numOfCols * sizeof(SSchema));

  pStable->nextColId = 0;
  for (int32_t col = 0; col < numOfCols; col++) {
    SSchema *tschema = pStable->schema;
    tschema[col].colId = pStable->nextColId++;
    tschema[col].bytes = htons(tschema[col].bytes);
  }

  SSdbOperDesc oper = {
    .type = SDB_OPER_TYPE_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable,
    .rowSize = sizeof(SSuperTableObj) + schemaSize
  };

  int32_t code = sdbInsertRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    mgmtDestroySuperTable(pStable);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SDB_ERROR);
  } else {
    mLPrint("stable:%s, is created, tags:%d cols:%d", pStable->info.tableId, pStable->numOfTags, pStable->numOfColumns);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SUCCESS);
  }
}

void mgmtDropSuperTable(SQueuedMsg *pMsg, SSuperTableObj *pStable) {
  if (pStable->numOfTables != 0) {
    mError("stable:%s, numOfTables:%d not 0", pStable->info.tableId, pStable->numOfTables);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_OTHERS);
  } else {
    SSdbOperDesc oper = {
      .type = SDB_OPER_TYPE_GLOBAL,
      .table = tsSuperTableSdb,
      .pObj = pStable
    };
    int32_t code = sdbDeleteRow(&oper);
    mLPrint("stable:%s, is dropped from sdb, result:%s", pStable->info.tableId, tstrerror(code));
    mgmtSendSimpleResp(pMsg->thandle, code);
  }
}

void* mgmtGetSuperTable(char *tableId) {
  return sdbGetRow(tsSuperTableSdb, tableId);
}

static void *mgmtGetSuperTableVgroup(SSuperTableObj *pStable) {
  SCMSTableVgroupRspMsg *rsp = rpcMallocCont(sizeof(SCMSTableVgroupRspMsg) + sizeof(uint32_t) * mgmtGetDnodesNum());
  rsp->numOfDnodes = htonl(1);
  rsp->dnodeIps[0] = htonl(inet_addr(tsPrivateIp));
  return rsp;
}

static int32_t mgmtFindSuperTableTagIndex(SSuperTableObj *pStable, const char *tagName) {
  for (int32_t i = 0; i < pStable->numOfTags; i++) {
    SSchema *schema = (SSchema *)(pStable->schema + (pStable->numOfColumns + i) * sizeof(SSchema));
    if (strcasecmp(tagName, schema->name) == 0) {
      return i;
    }
  }

  return -1;
}

static int32_t mgmtAddSuperTableTag(SSuperTableObj *pStable, SSchema schema[], int32_t ntags) {
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

  SDbObj *pDb = mgmtGetDbByTableId(pStable->info.tableId);
  if (pDb == NULL) {
    mError("meter: %s not belongs to any database", pStable->info.tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = acctGetAcct(pDb->cfg.acct);
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
  // sdbUpdateRow(tsSuperTableSdb, pStable, tsSuperTableUpdateSize, SDB_OPER_GLOBAL);

  mTrace("Succeed to add tag column %s to table %s", schema[0].name, pStable->info.tableId);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtDropSuperTableTag(SSuperTableObj *pStable, char *tagName) {
  int32_t col = mgmtFindSuperTableTagIndex(pStable, tagName);
  if (col <= 0 || col >= pStable->numOfTags) {
    return TSDB_CODE_APP_ERROR;
  }

  SDbObj *pDb = mgmtGetDbByTableId(pStable->info.tableId);
  if (pDb == NULL) {
    mError("table: %s not belongs to any database", pStable->info.tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = acctGetAcct(pDb->cfg.acct);
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

  // sdbUpdateRow(tsSuperTableSdb, pStable, tsSuperTableUpdateSize, SDB_OPER_GLOBAL);

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtModifySuperTableTagNameByName(SSuperTableObj *pStable, char *oldTagName, char *newTagName) {
  int32_t col = mgmtFindSuperTableTagIndex(pStable, oldTagName);
  if (col < 0) {
    // Tag name does not exist
    mError("Failed to modify table %s tag column, oname: %s, nname: %s", pStable->info.tableId, oldTagName, newTagName);
    return TSDB_CODE_INVALID_MSG_TYPE;
  }

  // int32_t  rowSize = 0;
  uint32_t len = strlen(newTagName);

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

  // mgmtSuperTableActionEncode(pStable, msg, size, &rowSize);

  int32_t ret = 0;
  // int32_t ret = sdbUpdateRow(tsSuperTableSdb, msg, tsSuperTableUpdateSize, SDB_OPER_GLOBAL);
  tfree(msg);

  if (ret < 0) {
    mError("Failed to modify table %s tag column", pStable->info.tableId);
    return TSDB_CODE_APP_ERROR;
  }

  mTrace("Succeed to modify table %s tag column", pStable->info.tableId);
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

static int32_t mgmtAddSuperTableColumn(SSuperTableObj *pStable, SSchema schema[], int32_t ncols) {
  if (ncols <= 0) {
    return TSDB_CODE_APP_ERROR;
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (mgmtFindSuperTableColumnIndex(pStable, schema[i].name) > 0) {
      return TSDB_CODE_APP_ERROR;
    }
  }

  SDbObj *pDb = mgmtGetDbByTableId(pStable->info.tableId);
  if (pDb == NULL) {
    mError("meter: %s not belongs to any database", pStable->info.tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = acctGetAcct(pDb->cfg.acct);
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
  // sdbUpdateRow(tsSuperTableSdb, pStable, tsSuperTableUpdateSize, SDB_OPER_GLOBAL);

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtDropSuperTableColumnByName(SSuperTableObj *pStable, char *colName) {
  int32_t col = mgmtFindSuperTableColumnIndex(pStable, colName);
  if (col < 0) {
    return TSDB_CODE_APP_ERROR;
  }

  SDbObj *pDb = mgmtGetDbByTableId(pStable->info.tableId);
  if (pDb == NULL) {
    mError("table: %s not belongs to any database", pStable->info.tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = acctGetAcct(pDb->cfg.acct);
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
  // sdbUpdateRow(tsSuperTableSdb, pStable, tsSuperTableUpdateSize, SDB_OPER_GLOBAL);

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtGetShowSuperTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mgmtGetDb(pShow->db);
  if (pDb == NULL) {
    return TSDB_CODE_DB_NOT_SELECTED;
  }

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
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

  SUserObj *pUser = mgmtGetUserFromConn(pConn, NULL);

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
    if (strncmp(pTable->info.tableId, prefix, prefixLen)) {
      continue;
    }

    memset(stableName, 0, tListLen(stableName));
    mgmtExtractTableName(pTable->info.tableId, stableName);

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

void mgmtDropAllSuperTables(SDbObj *pDropDb) {
  void *pNode = NULL;
  void *pLastNode = NULL;
  int32_t numOfTables = 0;
  int32_t dbNameLen = strlen(pDropDb->name);
  SSuperTableObj *pTable = NULL;

  while (1) {
    pNode = sdbFetchRow(tsSuperTableSdb, pNode, (void **)&pTable);
    if (pTable == NULL) {
      break;
    }

    if (strncmp(pDropDb->name, pTable->info.tableId, dbNameLen) == 0) {
      SSdbOperDesc oper = {
        .type = SDB_OPER_TYPE_LOCAL,
        .table = tsSuperTableSdb,
        .pObj = pTable,
      };
      sdbDeleteRow(&oper);
      pNode = pLastNode;
      numOfTables ++;
      continue;
    }
  }

  mTrace("db:%s, all super tables:%d is dropped from sdb", pDropDb->name, numOfTables);
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

void mgmtGetSuperTableMeta(SQueuedMsg *pMsg, SSuperTableObj *pTable) {
  SDbObj *pDb = pMsg->pDb;
  
  STableMetaMsg *pMeta = rpcMallocCont(sizeof(STableMetaMsg) + sizeof(SSchema) * TSDB_MAX_COLUMNS);
  pMeta->uid          = htobe64(pTable->uid);
  pMeta->sversion     = htons(pTable->sversion);
  pMeta->precision    = pDb->cfg.precision;
  pMeta->numOfTags    = (uint8_t)pTable->numOfTags;
  pMeta->numOfColumns = htons((int16_t)pTable->numOfColumns);
  pMeta->tableType    = pTable->info.type;
  pMeta->contLen      = sizeof(STableMetaMsg) + mgmtSetSchemaFromSuperTable(pMeta->schema, pTable);
  strcpy(pMeta->tableId, pTable->info.tableId);

  SRpcMsg rpcRsp = {
    .handle = pMsg->thandle, 
    .pCont = pMeta, 
    .contLen = pMeta->contLen,
  };
  pMeta->contLen = htons(pMeta->contLen);
  rpcSendResponse(&rpcRsp);

  mTrace("stable:%%s, uid:%" PRIu64 " table meta is retrieved", pTable->info.tableId, pTable->uid);
}

static void mgmtProcessSuperTableVgroupMsg(SQueuedMsg *pMsg) {
  SCMSTableVgroupMsg *pInfo = pMsg->pCont;
  STableInfo *pTable = mgmtGetSuperTable(pInfo->tableId);
  if (pTable == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_TABLE);
    return;
  }

  SCMSTableVgroupRspMsg *pRsp = mgmtGetSuperTableVgroup((SSuperTableObj *) pTable);
  if (pRsp != NULL) {
    int32_t msgLen = sizeof(SSuperTableObj) + htonl(pRsp->numOfDnodes) * sizeof(int32_t);
    SRpcMsg rpcRsp = {0};
    rpcRsp.handle = pMsg->thandle;
    rpcRsp.pCont = pRsp;
    rpcRsp.contLen = msgLen;
    rpcSendResponse(&rpcRsp);
  } else {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_TABLE);
  }
}

void mgmtAlterSuperTable(SQueuedMsg *pMsg, SSuperTableObj *pTable) {
  int32_t code = TSDB_CODE_OPS_NOT_SUPPORT;
  SCMAlterTableMsg *pAlter = pMsg->pCont;
   
  if (pAlter->type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN) {
    code = mgmtAddSuperTableTag((SSuperTableObj *) pTable, pAlter->schema, 1);
  } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN) {
    code = mgmtDropSuperTableTag((SSuperTableObj *) pTable, pAlter->schema[0].name);
  } else if (pAlter->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN) {
    code = mgmtModifySuperTableTagNameByName((SSuperTableObj *) pTable, pAlter->schema[0].name, pAlter->schema[1].name);
  } else if (pAlter->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
    code = mgmtAddSuperTableColumn((SSuperTableObj *) pTable, pAlter->schema, 1);
  } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
    code = mgmtDropSuperTableColumnByName((SSuperTableObj *) pTable, pAlter->schema[0].name);
  } else {}

  mgmtSendSimpleResp(pMsg->thandle, code);
}

static void mgmtProcessDropStableRsp(SRpcMsg *rpcMsg) {
 mTrace("drop stable rsp received, handle:%p code:%d", rpcMsg->handle, rpcMsg->code);
}