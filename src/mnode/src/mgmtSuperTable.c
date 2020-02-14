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

#include "mnode.h"
#include "mgmtAcct.h"
#include "mgmtGrant.h"
#include "mgmtUtil.h"
#include "mgmtDb.h"
#include "mgmtDnodeInt.h"
#include "mgmtVgroup.h"
#include "mgmtSupertableQuery.h"
#include "mgmtTable.h"
#include "taosmsg.h"
#include "tast.h"
#include "textbuffer.h"
#include "tschemautil.h"
#include "tscompression.h"
#include "tskiplist.h"
#include "tsqlfunction.h"
#include "ttime.h"
#include "tstatus.h"


#include "sdb.h"
#include "mgmtSuperTable.h"
#include "mgmtChildTable.h"

#include "tutil.h"


typedef struct {
  char     meterId[TSDB_TABLE_ID_LEN + 1];
  char     type;
  uint32_t cols;
  char     data[];
} SMeterBatchUpdateMsg;

typedef struct {
  int32_t col;
  int32_t pos;
  SSchema schema;
} SchemaUnit;

typedef struct {
  char    meterId[TSDB_TABLE_ID_LEN + 1];
  char    action;
  int32_t dataSize;
  char    data[];
} SMeterUpdateMsg;

void *tsSuperTableSdb;
void *(*mgmtSuperTableActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int size, int *ssize);

void *mgmtSuperTableActionInsert(void *row, char *str, int size, int *ssize);
void *mgmtSuperTableActionDelete(void *row, char *str, int size, int *ssize);
void *mgmtSuperTableActionUpdate(void *row, char *str, int size, int *ssize);
void *mgmtSuperTableActionEncode(void *row, char *str, int size, int *ssize);
void *mgmtSuperTableActionDecode(void *row, char *str, int size, int *ssize);
void *mgmtSuperTableActionReset(void *row, char *str, int size, int *ssize);
void *mgmtSuperTableActionDestroy(void *row, char *str, int size, int *ssize);

static void mgmtDestroySuperTable(SSuperTableObj *pTable) {
  free(pTable->schema);
  free(pTable);
}

static void mgmtSuperTableActionInit() {
  mgmtSuperTableActionFp[SDB_TYPE_INSERT] = mgmtSuperTableActionInsert;
  mgmtSuperTableActionFp[SDB_TYPE_DELETE] = mgmtSuperTableActionDelete;
  mgmtSuperTableActionFp[SDB_TYPE_UPDATE] = mgmtSuperTableActionUpdate;
  mgmtSuperTableActionFp[SDB_TYPE_ENCODE] = mgmtSuperTableActionEncode;
  mgmtSuperTableActionFp[SDB_TYPE_DECODE] = mgmtSuperTableActionDecode;
  mgmtSuperTableActionFp[SDB_TYPE_RESET] = mgmtSuperTableActionReset;
  mgmtSuperTableActionFp[SDB_TYPE_DESTROY] = mgmtSuperTableActionDestroy;
}

void *mgmtSuperTableActionReset(void *row, char *str, int size, int *ssize) {
  SSuperTableObj *pTable = (SSuperTableObj *) row;
  int tsize = pTable->updateEnd - (int8_t *) pTable;
  memcpy(pTable, str, tsize);
  pTable->schema = (char *) realloc(pTable->schema, pTable->schemaSize);
  memcpy(pTable->schema, str + tsize, pTable->schemaSize);
  return NULL;
}

void *mgmtSuperTableActionDestroy(void *row, char *str, int size, int *ssize) {
  SSuperTableObj *pTable = (SSuperTableObj *) row;
  mgmtDestroySuperTable(pTable);
  return NULL;
}

void *mgmtSuperTableActionInsert(void *row, char *str, int size, int *ssize) {
  SSuperTableObj *pTable = (SSuperTableObj *) row;
  SDbObj *pDb = mgmtGetDbByMeterId(pTable->tableId);
  if (pDb) {
    mgmtAddMetricIntoDb(pDb, pTable);
  }
  return NULL;
}

void *mgmtSuperTableActionDelete(void *row, char *str, int size, int *ssize) {
  SSuperTableObj *pTable = (SSuperTableObj *) row;
  SDbObj *pDb = mgmtGetDbByMeterId(pTable->tableId);
  if (pDb) {
    mgmtRemoveMetricFromDb(pDb, pTable);
  }
  return NULL;
}

void *mgmtSuperTableActionUpdate(void *row, char *str, int size, int *ssize) {
  return mgmtSuperTableActionReset(row, str, size, NULL);
}

void *mgmtSuperTableActionEncode(void *row, char *str, int size, int *ssize) {
  SSuperTableObj *pTable = (SSuperTableObj *) row;
  assert(row != NULL && str != NULL);

  int tsize = pTable->updateEnd - (int8_t *) pTable;
  if (size < tsize + pTable->schemaSize + 1) {
    *ssize = -1;
    return NULL;
  }

  memcpy(str, pTable, tsize);
  memcpy(str + tsize, pTable->schema, pTable->schemaSize);
  *ssize = tsize + pTable->schemaSize;

  return NULL;
}

void *mgmtSuperTableActionDecode(void *row, char *str, int size, int *ssize) {
  assert(str != NULL);

  SSuperTableObj *pTable = (SSuperTableObj *)malloc(sizeof(SSuperTableObj));
  if (pTable == NULL) {
    return NULL;
  }
  memset(pTable, 0, sizeof(STabObj));

  int tsize = pTable->updateEnd - (int8_t *)pTable;
  if (size < tsize) {
    mgmtDestroySuperTable(pTable);
    return NULL;
  }
  memcpy(pTable, str, tsize);

  pTable->schema = (char *)malloc(pTable->schemaSize);
  if (pTable->schema == NULL) {
    mgmtDestroySuperTable(pTable);
    return NULL;
  }

  memcpy(pTable->schema, str + tsize, pTable->schemaSize);
  return (void *)pTable;
}

void *mgmtSuperTableAction(char action, void *row, char *str, int size, int *ssize) {
  if (mgmtSuperTableActionFp[(uint8_t)action] != NULL) {
    return (*(mgmtSuperTableActionFp[(uint8_t)action]))(row, str, size, ssize);
  }
  return NULL;
}

int32_t mgmtInitSuperTables() {
  void *    pNode = NULL;
  void *    pLastNode = NULL;
  SVgObj *  pVgroup = NULL;
  STabObj * pTable = NULL;
  STabObj * pMetric = NULL;
  SDbObj *  pDb = NULL;
  SAcctObj *pAcct = NULL;

  // TODO: Make sure this function only run once
  mgmtSuperTableActionInit();

  tsSuperTableSdb = sdbOpenTable(tsMaxTables, sizeof(STabObj) + sizeof(SSchema) * TSDB_MAX_COLUMNS + TSDB_MAX_SQL_LEN,
                          "meters", SDB_KEYTYPE_STRING, mgmtDirectory, mgmtSuperTableAction);
  if (meterSdb == NULL) {
    mError("failed to init meter data");
    return -1;
  }

  pNode = NULL;
  while (1) {
    pNode = sdbFetchRow(meterSdb, pNode, (void **)&pTable);
    if (pTable == NULL) break;
    if (mgmtIsSuperTable(pTable)) pTable->numOfMeters = 0;
  }

  pNode = NULL;
  while (1) {
    pLastNode = pNode;
    pNode = sdbFetchRow(meterSdb, pNode, (void **)&pTable);
    if (pTable == NULL) break;

    pDb = mgmtGetDbByMeterId(pTable->meterId);
    if (pDb == NULL) {
      mError("meter:%s, failed to get db, discard it", pTable->meterId, pTable->gid.vgId, pTable->gid.sid);
      pTable->gid.vgId = 0;
      sdbDeleteRow(meterSdb, pTable);
      pNode = pLastNode;
      continue;
    }

    if (mgmtIsNormalTable(pTable)) {
      pVgroup = mgmtGetVgroup(pTable->gid.vgId);

      if (pVgroup == NULL) {
        mError("meter:%s, failed to get vgroup:%d sid:%d, discard it", pTable->meterId, pTable->gid.vgId, pTable->gid.sid);
        pTable->gid.vgId = 0;
        sdbDeleteRow(meterSdb, pTable);
        pNode = pLastNode;
        continue;
      }

      if (strcmp(pVgroup->dbName, pDb->name) != 0) {
        mError("meter:%s, db:%s not match with vgroup:%d db:%s sid:%d, discard it",
               pTable->meterId, pDb->name, pTable->gid.vgId, pVgroup->dbName, pTable->gid.sid);
        pTable->gid.vgId = 0;
        sdbDeleteRow(meterSdb, pTable);
        pNode = pLastNode;
        continue;
      }

      if ( pVgroup->meterList == NULL) {
        mError("meter:%s, vgroup:%d meterlist is null", pTable->meterId, pTable->gid.vgId);
        pTable->gid.vgId = 0;
        sdbDeleteRow(meterSdb, pTable);
        pNode = pLastNode;
        continue;
      }

      pVgroup->meterList[pTable->gid.sid] = pTable;
      taosIdPoolMarkStatus(pVgroup->idPool, pTable->gid.sid, 1);

      if (pTable->tableType == TSDB_TABLE_TYPE_STREAM_TABLE) {
        pTable->pSql = (char *)pTable->schema + sizeof(SSchema) * pTable->numOfColumns;
      }

      if (mgmtTableCreateFromSuperTable(pTable)) {
        pTable->pTagData = (char *)pTable->schema;  // + sizeof(SSchema)*pTable->numOfColumns;
        pMetric = mgmtGetTable(pTable->pTagData);
        if (pMetric) mgmtAddMeterIntoMetric(pMetric, pTable);
      }

      pAcct = mgmtGetAcct(pDb->cfg.acct);
      if (pAcct) mgmtAddMeterStatisticToAcct(pTable, pAcct);
    } else {
      if (pDb) mgmtAddMetricIntoDb(pDb, pTable);
    }
  }

  mgmtSetVgroupIdPool();

  mTrace("meter is initialized");
  return 0;
}

void mgmtCleanUpSuperTables() {
}

int32_t mgmtCreateSuperTable(SDbObj *pDb, SCreateTableMsg *pCreate) {
  int numOfTables = sdbGetNumOfRows(tsSuperTableSdb);
  if (numOfTables >= TSDB_MAX_TABLES) {
    mError("super table:%s, numOfTables:%d exceed maxTables:%d", pCreate->meterId, numOfTables, TSDB_MAX_TABLES);
    return TSDB_CODE_TOO_MANY_TABLES;
  }

  SSuperTableObj *pMetric = (SSuperTableObj *)calloc(sizeof(SSuperTableObj), 1);
  if (pMetric == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  strcpy(pMetric->tableId, pCreate->meterId);
  pMetric->createdTime = taosGetTimestampMs();
  pMetric->vgId = 0;
  pMetric->sid = 0;
  pMetric->uid = (((uint64_t)pMetric->createdTime) << 16) + ((uint64_t)sdbGetVersion() & ((1ul << 16) - 1ul));
  pMetric->sversion = 0;
  pMetric->numOfColumns = pCreate->numOfColumns;
  pMetric->numOfTags = pCreate->numOfTags;
  pMetric->numOfMeters = 0;

  int numOfCols = pCreate->numOfColumns + pCreate->numOfTags;
  pMetric->schemaSize = numOfCols * sizeof(SSchema);
  pMetric->schema = (int8_t *)calloc(1, pMetric->schemaSize);
  if (pMetric->schema == NULL) {
    free(pMetric);
    mError("table:%s, no schema input", pCreate->meterId);
    return TSDB_CODE_INVALID_TABLE;
  }
  memcpy(pMetric->schema, pCreate->schema, numOfCols * sizeof(SSchema));

  pMetric->nextColId = 0;
  for (int col = 0; col < pCreate->numOfColumns; col++) {
    SSchema *tschema = (SSchema *)pMetric->schema;
    tschema[col].colId = pMetric->nextColId++;
  }

  if (sdbInsertRow(tsSuperTableSdb, pMetric, 0) < 0) {
    mError("table:%s, update sdb error", pCreate->meterId);
    return TSDB_CODE_SDB_ERROR;
  }

  return 0;
}

int32_t mgmtDropSuperTable(SDbObj *pDb, SSuperTableObj *pSuperTable) {
  SChildTableObj *pMetric;
  while ((pMetric = pSuperTable->pHead) != NULL) {
    mgmtDropChildTable(pDb, pMetric);
  }
  sdbDeleteRow(tsSuperTableSdb, pMetric);
}

SSuperTableObj* mgmtGetSuperTable(char *tableId) {
  return (SSuperTableObj *)sdbGetRow(tsSuperTableSdb, tableId);
}

int32_t mgmtFindTagCol(SSuperTableObj *pMetric, const char *tagName) {
  for (int i = 0; i < pMetric->numOfTags; i++) {
    SSchema *schema = (SSchema *)(pMetric->schema + (pMetric->numOfColumns + i) * sizeof(SSchema));
    if (strcasecmp(tagName, schema->name) == 0) {
      return i;
    }
  }

  return -1;
}

int32_t mgmtAddSuperTableTag(SSuperTableObj *pMetric, SSchema schema[], int32_t ntags) {
  if (pMetric->numOfTags + ntags > TSDB_MAX_TAGS) {
    return TSDB_CODE_APP_ERROR;
  }

  // check if schemas have the same name
  for (int i = 1; i < ntags; i++) {
    for (int j = 0; j < i; j++) {
      if (strcasecmp(schema[i].name, schema[j].name) == 0) {
        return TSDB_CODE_APP_ERROR;
      }
    }
  }

  for (int i = 0; i < ntags; i++) {
    if (mgmtFindTagCol(pMetric, schema[i].name) >= 0) {
      return TSDB_CODE_APP_ERROR;
    }
  }

  uint32_t size = sizeof(SMeterBatchUpdateMsg) + sizeof(SSchema) * ntags;
  SMeterBatchUpdateMsg *msg = (SMeterBatchUpdateMsg *) malloc(size);
  memset(msg, 0, size);

  memcpy(msg->meterId, pMetric->tableId, TSDB_TABLE_ID_LEN);
  msg->type = SDB_TYPE_INSERT;
  msg->cols = ntags;
  memcpy(msg->data, schema, sizeof(SSchema) * ntags);

  int32_t ret = sdbBatchUpdateRow(tsSuperTableSdb, msg, size);
  tfree(msg);
//
//  if (msg->type == SDB_TYPE_INSERT) {  // Insert schema
//    uint32_t total_cols = pTable->numOfColumns + pTable->numOfTags;
//    pTable->schema = realloc(pTable->schema, (total_cols + msg->cols) * sizeof(SSchema));
//    pTable->schemaSize = (total_cols + msg->cols) * sizeof(SSchema);
//    pTable->numOfTags += msg->cols;
//    memcpy(pTable->schema + total_cols * sizeof(SSchema), msg->data, msg->cols * sizeof(SSchema));
//
//  }

  if (ret < 0) {
    mError("Failed to add tag column %s to table %s", schema[0].name, pMetric->tableId);
    return TSDB_CODE_APP_ERROR;
  }

  mTrace("Succeed to add tag column %s to table %s", schema[0].name, pMetric->tableId);
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtDropSuperTableTag(SSuperTableObj *pMetric, char *tagName) {
  int col = mgmtFindTagCol(pMetric, tagName);
  if (col <= 0 || col >= pMetric->numOfTags) {
    return TSDB_CODE_APP_ERROR;
  }

  // Pack message to do batch update
  uint32_t             size = sizeof(SMeterBatchUpdateMsg) + sizeof(SchemaUnit);
  SMeterBatchUpdateMsg *msg = (SMeterBatchUpdateMsg *) malloc(size);
  memset(msg, 0, size);

  memcpy(msg->meterId, pMetric->tableId, TSDB_TABLE_ID_LEN);
  msg->type = SDB_TYPE_DELETE;
  msg->cols = 1;
//
//  // Make sure the order of tag columns
//  SchemaUnit *schemaUnit = (SchemaUnit *)(msg->data);
//  int         col = schemaUnit->col;
//  assert(col > 0 && col < pTable->numOfTags);
//  if (col < pTable->numOfTags - 1) {
//    memmove(pTable->schema + sizeof(SSchema) * (pTable->numOfColumns + col),
//            pTable->schema + sizeof(SSchema) * (pTable->numOfColumns + col + 1),
//            pTable->schemaSize - (sizeof(SSchema) * (pTable->numOfColumns + col + 1)));
//  }
//  pTable->schemaSize -= sizeof(SSchema);
//  pTable->numOfTags--;
//  pTable->schema = realloc(pTable->schema, pTable->schemaSize);
//
  ((SchemaUnit *) (msg->data))->col    = col;
  ((SchemaUnit *) (msg->data))->pos    = mgmtGetTagsLength(pMetric, col) + TSDB_TABLE_ID_LEN;
  ((SchemaUnit *) (msg->data))->schema = *(SSchema *) (pMetric->schema + sizeof(SSchema) * (pMetric->numOfColumns + col));

  int32_t ret = sdbBatchUpdateRow(tsSuperTableSdb, msg, size);
  tfree(msg);

  if (ret < 0) {
    mError("Failed to drop tag column: %d from table: %s", col, pMetric->tableId);
    return TSDB_CODE_APP_ERROR;
  }

  mTrace("Succeed to drop tag column: %d from table: %s", col, pMetric->tableId);
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtModifySuperTableTagNameByName(SSuperTableObj *pMetric, char *oldTagName, char *newTagName) {
  int col = mgmtFindTagCol(pMetric, oldTagName);
  if (col < 0) {
    // Tag name does not exist
    mError("Failed to modify table %s tag column, oname: %s, nname: %s", pMetric->tableId, oldTagName, newTagName);
    return TSDB_CODE_INVALID_MSG_TYPE;
  }

  int      rowSize = 0;
  uint32_t len     = strlen(newTagName);

  if (col >= pMetric->numOfTags || len >= TSDB_COL_NAME_LEN || mgmtFindTagCol(pMetric, newTagName) >= 0) {
    return TSDB_CODE_APP_ERROR;
  }

  // update
  SSchema *schema = (SSchema *) (pMetric->schema + (pMetric->numOfColumns + col) * sizeof(SSchema));
  strncpy(schema->name, newTagName, TSDB_COL_NAME_LEN);

  // Encode string
  int  size = 1 + sizeof(STabObj) + TSDB_MAX_BYTES_PER_ROW;
  char *msg = (char *) malloc(size);
  if (msg == NULL) return TSDB_CODE_APP_ERROR;
  memset(msg, 0, size);

  mgmtSuperTableActionEncode(pMetric, msg, size, &rowSize);

  int32_t ret = sdbUpdateRow(tsSuperTableSdb, msg, rowSize, 1);
  tfree(msg);

  if (ret < 0) {
    mError("Failed to modify table %s tag column", pMetric->tableId);
    return TSDB_CODE_APP_ERROR;
  }

  mTrace("Succeed to modify table %s tag column", pMetric->tableId);
  return TSDB_CODE_SUCCESS;
}


static int32_t mgmtFindSuperTableColumnIndex(SNormalTableObj *pMetric, char *colName) {
  SSchema *schema = (SSchema *) pMetric->schema;
  for (int32_t i = 0; i < pMetric->numOfColumns; i++) {
    if (strcasecmp(schema[i].name, colName) == 0) {
      return i;
    }
  }

  return -1;
}

int32_t mgmtAddSuperTableColumn(SSuperTableObj *pMetric, SSchema schema[], int ncols) {
  if (ncols <= 0) {
    return TSDB_CODE_APP_ERROR;
  }

  for (int i = 0; i < ncols; i++) {
    if (mgmtFindSuperTableColumnIndex(pMetric, schema[i].name) > 0) {
      return TSDB_CODE_APP_ERROR;
    }
  }

  SDbObj *pDb = mgmtGetDbByMeterId(pMetric->tableId);
  if (pDb == NULL) {
    mError("meter: %s not belongs to any database", pMetric->tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("DB: %s not belongs to andy account", pDb->name);
    return TSDB_CODE_APP_ERROR;
  }

  pMetric->schema = realloc(pMetric->schema, pMetric->schemaSize + sizeof(SSchema) * ncols);

  memmove(pMetric->schema + sizeof(SSchema) * (pMetric->numOfColumns + ncols),
          pMetric->schema + sizeof(SSchema) * pMetric->numOfColumns, sizeof(SSchema) * pMetric->numOfTags);
  memcpy(pMetric->schema + sizeof(SSchema) * pMetric->numOfColumns, schema, sizeof(SSchema) * ncols);

  SSchema *tschema = (SSchema *) (pMetric->schema + sizeof(SSchema) * pMetric->numOfColumns);
  for (int i = 0; i < ncols; i++) {
    tschema[i].colId = pMetric->nextColId++;
  }

  pMetric->schemaSize += sizeof(SSchema) * ncols;
  pMetric->numOfColumns += ncols;
  pMetric->sversion++;

  pAcct->acctInfo.numOfTimeSeries += (ncols * pMetric->numOfMeters);
  sdbUpdateRow(tsSuperTableSdb, pMetric, 0, 1);

  return TSDB_CODE_SUCCESS;
}

int32_t mgmtDropSuperTableColumnByName(SSuperTableObj *pMetric, char *colName) {
  int32_t col = mgmtFindSuperTableColumnIndex(pMetric, colName);
  if (col < 0) {
    return TSDB_CODE_APP_ERROR;
  }

  SDbObj *pDb = mgmtGetDbByMeterId(pMetric->tableId);
  if (pDb == NULL) {
    mError("table: %s not belongs to any database", pMetric->tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("DB: %s not belongs to any account", pDb->name);
    return TSDB_CODE_APP_ERROR;
  }

  memmove(pMetric->schema + sizeof(SSchema) * col, pMetric->schema + sizeof(SSchema) * (col + 1),
          sizeof(SSchema) * (pMetric->numOfColumns + pMetric->numOfTags - col - 1));

  pMetric->schemaSize -= sizeof(SSchema);
  pMetric->numOfColumns--;
  pMetric->schema = realloc(pMetric->schema, pMetric->schemaSize);
  pMetric->sversion++;

  pAcct->acctInfo.numOfTimeSeries -= (pMetric->numOfMeters);
  sdbUpdateRow(tsSuperTableSdb, pMetric, 0, 1);

  return TSDB_CODE_SUCCESS;
}


int mgmtGetSuperTableMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int cols = 0;

  SDbObj *pDb = NULL;
  if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);

  if (pDb == NULL) return TSDB_CODE_DB_NOT_SELECTED;

  SSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = TSDB_METER_NAME_LEN;
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
  for (int i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = pDb->numOfMetrics;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

int mgmtRetrieveSuperTables(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int             numOfRows = 0;
  char *          pWrite;
  int             cols = 0;
  SSuperTableObj *pTable = NULL;
  char            prefix[20] = {0};
  int32_t         prefixLen;

  SDbObj *pDb = NULL;
  if (pConn->pDb != NULL) {
    pDb = mgmtGetDb(pConn->pDb->name);
  }

  if (pDb == NULL) {
    return 0;
  }

  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    if (strcmp(pConn->pUser->user, "root") != 0 && strcmp(pConn->pUser->user, "_root") != 0 && strcmp(pConn->pUser->user, "monitor") != 0 ) {
      return 0;
    }
  }

  strcpy(prefix, pDb->name);
  strcat(prefix, TS_PATH_DELIMITER);
  prefixLen = strlen(prefix);

  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;
  char metricName[TSDB_METER_NAME_LEN] = {0};

  while (numOfRows < rows) {
    pTable = (SSuperTableObj *)pShow->pNode;
    if (pTable == NULL) break;
    pShow->pNode = (void *)pTable->next;

    if (strncmp(pTable->tableId, prefix, prefixLen)) {
      continue;
    }

    memset(metricName, 0, tListLen(metricName));
    extractTableName(pTable->tableId, metricName);

    if (pShow->payloadLen > 0 &&
        patternMatch(pShow->payload, metricName, TSDB_METER_NAME_LEN, &info) != TSDB_PATTERN_MATCH)
      continue;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    extractTableName(pTable->tableId, pWrite);
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
    *(int32_t *)pWrite = pTable->numOfMeters;
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}


int mgmtAddMeterIntoMetric(STabObj *pMetric, STabObj *pTable) {
  if (pTable == NULL || pMetric == NULL) return -1;

  pthread_rwlock_wrlock(&(pMetric->rwLock));
  // add meter into skip list
  pTable->next = pMetric->pHead;
  pTable->prev = NULL;

  if (pMetric->pHead) pMetric->pHead->prev = pTable;

  pMetric->pHead = pTable;
  pMetric->numOfMeters++;

  addMeterIntoMetricIndex(pMetric, pTable);

  pthread_rwlock_unlock(&(pMetric->rwLock));

  return 0;
}

int mgmtRemoveMeterFromMetric(STabObj *pMetric, STabObj *pTable) {
  pthread_rwlock_wrlock(&(pMetric->rwLock));

  if (pTable->prev) pTable->prev->next = pTable->next;

  if (pTable->next) pTable->next->prev = pTable->prev;

  if (pTable->prev == NULL) pMetric->pHead = pTable->next;

  pMetric->numOfMeters--;

  removeMeterFromMetricIndex(pMetric, pTable);

  pthread_rwlock_unlock(&(pMetric->rwLock));

  return 0;
}