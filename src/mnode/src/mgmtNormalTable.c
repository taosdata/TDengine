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
#include "tast.h"
#include "textbuffer.h"
#include "tschemautil.h"
#include "tscompression.h"
#include "tskiplist.h"
#include "tsqlfunction.h"
#include "ttime.h"
#include "tstatus.h"
#include "tutil.h"
#include "mnode.h"
#include "mgmtAcct.h"
#include "mgmtDb.h"
#include "mgmtDnodeInt.h"
#include "mgmtGrant.h"
#include "mgmtNormalTable.h"
#include "mgmtSuperTable.h"
#include "mgmtTable.h"
#include "mgmtVgroup.h"

void *tsNormalTableSdb;
int32_t tsNormalTableUpdateSize;
void *(*mgmtNormalTableActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int32_t size, int32_t *ssize);

void *mgmtNormalTableActionInsert(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtNormalTableActionDelete(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtNormalTableActionUpdate(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtNormalTableActionEncode(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtNormalTableActionDecode(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtNormalTableActionReset(void *row, char *str, int32_t size, int32_t *ssize);
void *mgmtNormalTableActionDestroy(void *row, char *str, int32_t size, int32_t *ssize);

static void mgmtDestroyNormalTable(SNormalTableObj *pTable) {
  free(pTable->schema);
  free(pTable->sql);
  free(pTable);
}

static void mgmtNormalTableActionInit() {
  mgmtNormalTableActionFp[SDB_TYPE_INSERT] = mgmtNormalTableActionInsert;
  mgmtNormalTableActionFp[SDB_TYPE_DELETE] = mgmtNormalTableActionDelete;
  mgmtNormalTableActionFp[SDB_TYPE_UPDATE] = mgmtNormalTableActionUpdate;
  mgmtNormalTableActionFp[SDB_TYPE_ENCODE] = mgmtNormalTableActionEncode;
  mgmtNormalTableActionFp[SDB_TYPE_DECODE] = mgmtNormalTableActionDecode;
  mgmtNormalTableActionFp[SDB_TYPE_RESET] = mgmtNormalTableActionReset;
  mgmtNormalTableActionFp[SDB_TYPE_DESTROY] = mgmtNormalTableActionDestroy;
}

void *mgmtNormalTableActionReset(void *row, char *str, int32_t size, int32_t *ssize) {
  SNormalTableObj *pTable = (SNormalTableObj *) row;
  memcpy(pTable, str, tsNormalTableUpdateSize);

  int32_t schemaSize = sizeof(SSchema) * (pTable->numOfColumns) + pTable->sqlLen;
  pTable->schema = realloc(pTable->schema, schemaSize);
  pTable->sql = (char*)pTable->schema + sizeof(SSchema) * (pTable->numOfColumns);
  memcpy(pTable->schema, str + tsNormalTableUpdateSize, schemaSize);

  return NULL;
}

void *mgmtNormalTableActionDestroy(void *row, char *str, int32_t size, int32_t *ssize) {
  SNormalTableObj *pTable = (SNormalTableObj *)row;
  mgmtDestroyNormalTable(pTable);
  return NULL;
}

void *mgmtNormalTableActionInsert(void *row, char *str, int32_t size, int32_t *ssize) {
  SNormalTableObj *pTable = (SNormalTableObj *) row;

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    mError("id:%s not in vgroup:%d", pTable->tableId, pTable->vgId);
    return NULL;
  }

  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
  if (pDb == NULL) {
    mError("vgroup:%d not in DB:%s", pVgroup->vgId, pVgroup->dbName);
    return NULL;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("account not exists");
    return NULL;
  }

  if (!sdbMaster) {
    int32_t sid = taosAllocateId(pVgroup->idPool);
    if (sid != pTable->sid) {
      mError("sid:%d is not matched from the master:%d", sid, pTable->sid);
      return NULL;
    }
  }

  mgmtAddTimeSeries(pAcct, pTable->numOfColumns - 1);
  mgmtAddTableIntoDb(pDb);
  mgmtAddTableIntoVgroup(pVgroup, (STableInfo *) pTable);

  if (pVgroup->numOfTables >= pDb->cfg.maxSessions - 1 && pDb->numOfVgroups > 1) {
    mgmtMoveVgroupToTail(pDb, pVgroup);
  }

  return NULL;
}

void *mgmtNormalTableActionDelete(void *row, char *str, int32_t size, int32_t *ssize) {
  SNormalTableObj *pTable = (SNormalTableObj *) row;
  if (pTable->vgId == 0) {
    return NULL;
  }

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    mError("id:%s not in vgroup:%d", pTable->tableId, pTable->vgId);
    return NULL;
  }

  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
  if (pDb == NULL) {
    mError("vgroup:%d not in DB:%s", pVgroup->vgId, pVgroup->dbName);
    return NULL;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("account not exists");
    return NULL;
  }

  mgmtRestoreTimeSeries(pAcct, pTable->numOfColumns - 1);
  mgmtRemoveTableFromDb(pDb);
  mgmtRemoveTableFromVgroup(pVgroup, (STableInfo *) pTable);

  if (pVgroup->numOfTables > 0) {
    mgmtMoveVgroupToHead(pDb, pVgroup);
  }

  return NULL;
}

void *mgmtNormalTableActionUpdate(void *row, char *str, int32_t size, int32_t *ssize) {
  return mgmtNormalTableActionReset(row, str, size, NULL);
}

void *mgmtNormalTableActionEncode(void *row, char *str, int32_t size, int32_t *ssize) {
  SNormalTableObj *pTable = (SNormalTableObj *) row;
  assert(row != NULL && str != NULL);

  int32_t schemaSize = pTable->numOfColumns * sizeof(SSchema);
  if (size < tsNormalTableUpdateSize + schemaSize + 1) {
    *ssize = -1;
    return NULL;
  }

  memcpy(str, pTable, tsNormalTableUpdateSize);
  memcpy(str + tsNormalTableUpdateSize, pTable->schema, schemaSize);
  memcpy(str + tsNormalTableUpdateSize + schemaSize, pTable->sql, pTable->sqlLen);
  *ssize = tsNormalTableUpdateSize + schemaSize + pTable->sqlLen;

  return NULL;
}

void *mgmtNormalTableActionDecode(void *row, char *str, int32_t size, int32_t *ssize) {
  assert(str != NULL);

  SNormalTableObj *pTable = (SNormalTableObj *)malloc(sizeof(SNormalTableObj));
  if (pTable == NULL) {
    return NULL;
  }
  memset(pTable, 0, sizeof(SNormalTableObj));

  if (size < tsNormalTableUpdateSize) {
    mgmtDestroyNormalTable(pTable);
    return NULL;
  }
  memcpy(pTable, str, tsNormalTableUpdateSize);

  int32_t schemaSize = pTable->numOfColumns * sizeof(SSchema);
  pTable->schema = (SSchema *)malloc(schemaSize);
  if (pTable->schema == NULL) {
    mgmtDestroyNormalTable(pTable);
    return NULL;
  }

  memcpy(pTable->schema, str + tsNormalTableUpdateSize, schemaSize);

  pTable->sql = (char *)malloc(pTable->sqlLen);
  if (pTable->sql == NULL) {
    mgmtDestroyNormalTable(pTable);
    return NULL;
  }
  memcpy(pTable->sql, str + tsNormalTableUpdateSize + schemaSize, pTable->sqlLen);
  return (void *)pTable;
}

void *mgmtNormalTableAction(char action, void *row, char *str, int32_t size, int32_t *ssize) {
  if (mgmtNormalTableActionFp[(uint8_t)action] != NULL) {
    return (*(mgmtNormalTableActionFp[(uint8_t)action]))(row, str, size, ssize);
  }
  return NULL;
}

int32_t mgmtInitNormalTables() {
  void *pNode = NULL;
  void *pLastNode = NULL;
  SNormalTableObj *pTable = NULL;

  mgmtNormalTableActionInit();
  SNormalTableObj tObj;
  tsNormalTableUpdateSize = tObj.updateEnd - (int8_t *)&tObj;

  tsNormalTableSdb = sdbOpenTable(tsMaxTables, sizeof(SNormalTableObj) + sizeof(SSchema) * TSDB_MAX_COLUMNS,
                                 "ntables", SDB_KEYTYPE_STRING, tsMgmtDirectory, mgmtNormalTableAction);
  if (tsNormalTableSdb == NULL) {
    mError("failed to init ntables data");
    return -1;
  }

  while (1) {
    pLastNode = pNode;
    pNode = sdbFetchRow(tsNormalTableSdb, pNode, (void **)&pTable);
    if (pTable == NULL) break;

    SDbObj *pDb = mgmtGetDbByTableId(pTable->tableId);
    if (pDb == NULL) {
      mError("ntable:%s, failed to get db, discard it", pTable->tableId);
      sdbDeleteRow(tsNormalTableSdb, pTable);
      pNode = pLastNode;
      continue;
    }

    SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
    if (pVgroup == NULL) {
      mError("ntable:%s, failed to get vgroup:%d sid:%d, discard it", pTable->tableId, pTable->vgId, pTable->sid);
      pTable->vgId = 0;
      sdbDeleteRow(tsNormalTableSdb, pTable);
      pNode = pLastNode;
      continue;
    }

    if (strcmp(pVgroup->dbName, pDb->name) != 0) {
      mError("ntable:%s, db:%s not match with vgroup:%d db:%s sid:%d, discard it",
               pTable->tableId, pDb->name, pTable->vgId, pVgroup->dbName, pTable->sid);
      pTable->vgId = 0;
      sdbDeleteRow(tsNormalTableSdb, pTable);
      pNode = pLastNode;
      continue;
    }

    if (pVgroup->tableList == NULL) {
      mError("ntable:%s, vgroup:%d tableList is null", pTable->tableId, pTable->vgId);
      pTable->vgId = 0;
      sdbDeleteRow(tsNormalTableSdb, pTable);
      pNode = pLastNode;
      continue;
    }

    mgmtAddTableIntoVgroup(pVgroup, pTable);
    //pVgroup->tableList[pTable->sid] = (STableInfo*)pTable;
    taosIdPoolMarkStatus(pVgroup->idPool, pTable->sid, 1);

    pTable->sql = (char *)pTable->schema + sizeof(SSchema) * pTable->numOfColumns;

    SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
    mgmtAddTimeSeries(pAcct, pTable->numOfColumns - 1);
  }

  mTrace("ntables is initialized");
  return 0;
}

void mgmtCleanUpNormalTables() {
  sdbCloseTable(tsNormalTableSdb);
}

int8_t *mgmtBuildCreateNormalTableMsg(SNormalTableObj *pTable) {
//  int8_t *pMsg = NULL;
//  SDCreateTableMsg *pCreateTable = (SDCreateTableMsg *) pMsg;
//  memcpy(pCreateTable->tableId, pTable->tableId, TSDB_TABLE_ID_LEN);
//  pCreateTable->vnode        = htobe32(vnode);
//  pCreateTable->sid          = htobe32(pTable->sid);
//  pCreateTable->uid          = htobe64(pTable->uid);
//  pCreateTable->createdTime  = htobe64(pTable->createdTime);
//  pCreateTable->sversion     = htobe32(pTable->sversion);
//  pCreateTable->numOfColumns = htobe16(pTable->numOfColumns);
//
//  SSchema *pSchema  = pTable->schema;
//  int32_t totalCols = pCreateTable->numOfColumns;

//  for (int32_t col = 0; col < totalCols; ++col) {
//    SMColumn *colData = &((SMColumn *) (pCreateTable->data))[col];
//    colData->type  = pSchema[col].type;
//    colData->bytes = htons(pSchema[col].bytes);
//    colData->colId = htons(pSchema[col].colId);
//  }

//  int32_t totalColsSize = sizeof(SMColumn *) * totalCols;
//  pMsg = pCreateTable->data + totalColsSize;

//  return pMsg;
  return NULL;
}

int32_t mgmtCreateNormalTable(SDbObj *pDb, SCreateTableMsg *pCreate, SVgObj *pVgroup, int32_t sid) {
  int32_t numOfTables = sdbGetNumOfRows(tsNormalTableSdb);
  if (numOfTables >= TSDB_MAX_NORMAL_TABLES) {
    mError("normal table:%s, numOfTables:%d exceed maxTables:%d", pCreate->tableId, numOfTables, TSDB_MAX_NORMAL_TABLES);
    return TSDB_CODE_TOO_MANY_TABLES;
  }

  SNormalTableObj *pTable = (SNormalTableObj *) calloc(sizeof(SNormalTableObj), 1);
  if (pTable == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  strcpy(pTable->tableId, pCreate->tableId);
  pTable->type         = TSDB_TABLE_TYPE_NORMAL_TABLE;
  pTable->createdTime  = taosGetTimestampMs();
  pTable->vgId         = pVgroup->vgId;
  pTable->sid          = sid;
  pTable->uid          = (((uint64_t) pTable->createdTime) << 16) + ((uint64_t) sdbGetVersion() & ((1ul << 16) - 1ul));
  pTable->sversion     = 0;
  pTable->numOfColumns = pCreate->numOfColumns;

  int32_t numOfCols  = pCreate->numOfColumns;
  int32_t schemaSize = numOfCols * sizeof(SSchema);
  pTable->schema     = (SSchema *) calloc(1, schemaSize);
  if (pTable->schema == NULL) {
    free(pTable);
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }
  memcpy(pTable->schema, pCreate->schema, numOfCols * sizeof(SSchema));

  pTable->nextColId = 0;
  for (int32_t col = 0; col < pCreate->numOfColumns; col++) {
    SSchema *tschema   = (SSchema *) pTable->schema;
    tschema[col].colId = pTable->nextColId++;
  }

  pTable->sqlLen = pCreate->sqlLen;
  if (pTable->sqlLen != 0) {
    pTable->type = TSDB_TABLE_TYPE_STREAM_TABLE;
    pTable->sql = calloc(1, pTable->sqlLen);
    if (pTable->sql == NULL) {
      free(pTable);
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    }
    memcpy(pTable->sql, (char *) (pCreate->schema) + numOfCols * sizeof(SSchema), pCreate->sqlLen);
    pTable->sql[pCreate->sqlLen - 1] = 0;
    mTrace("table:%s, stream sql len:%d sql:%s", pCreate->tableId, pCreate->sqlLen, pTable->sql);
  }

  if (sdbInsertRow(tsNormalTableSdb, pTable, 0) < 0) {
    mError("table:%s, update sdb error", pCreate->tableId);
    return TSDB_CODE_SDB_ERROR;
  }

  mgmtSendCreateNormalTableMsg(pTable, pVgroup);

  mTrace("table:%s, create table in vgroup, vgId:%d sid:%d vnode:%d uid:%" PRIu64 " db:%s",
         pTable->tableId, pVgroup->vgId, sid, pVgroup->vnodeGid[0].vnode, pTable->uid, pDb->name);

  return 0;
}

int32_t mgmtDropNormalTable(SDbObj *pDb, SNormalTableObj *pTable) {
  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    return TSDB_CODE_OTHERS;
  }

  mgmtSendRemoveTableMsg((STableInfo *) pTable, pVgroup);

  sdbDeleteRow(tsNormalTableSdb, pTable);

  if (pVgroup->numOfTables <= 0) {
    mgmtDropVgroup(pDb, pVgroup);
  }

  return 0;
}

void* mgmtGetNormalTable(char *tableId) {
  return sdbGetRow(tsNormalTableSdb, tableId);
}

static int32_t mgmtFindNormalTableColumnIndex(SNormalTableObj *pTable, char *colName) {
  SSchema *schema = (SSchema *) pTable->schema;
  for (int32_t i = 0; i < pTable->numOfColumns; i++) {
    if (strcasecmp(schema[i].name, colName) == 0) {
      return i;
    }
  }

  return -1;
}

int32_t mgmtAddNormalTableColumn(SNormalTableObj *pTable, SSchema schema[], int32_t ncols) {
  if (ncols <= 0) {
    return TSDB_CODE_APP_ERROR;
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (mgmtFindNormalTableColumnIndex(pTable, schema[i].name) > 0) {
      return TSDB_CODE_APP_ERROR;
    }
  }

  SDbObj *pDb = mgmtGetDbByTableId(pTable->tableId);
  if (pDb == NULL) {
    mError("table: %s not belongs to any database", pTable->tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("DB: %s not belongs to andy account", pDb->name);
    return TSDB_CODE_APP_ERROR;
  }

  int32_t schemaSize = pTable->numOfColumns * sizeof(SSchema);
  pTable->schema = realloc(pTable->schema, schemaSize + sizeof(SSchema) * ncols);

  memcpy(pTable->schema + schemaSize, schema, sizeof(SSchema) * ncols);

  SSchema *tschema = (SSchema *) (pTable->schema + sizeof(SSchema) * pTable->numOfColumns);
  for (int32_t i = 0; i < ncols; i++) {
    tschema[i].colId = pTable->nextColId++;
  }

  pTable->numOfColumns += ncols;
  pTable->sversion++;
  pAcct->acctInfo.numOfTimeSeries += ncols;

  sdbUpdateRow(tsNormalTableSdb, pTable, 0, 1);
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtDropNormalTableColumnByName(SNormalTableObj *pTable, char *colName) {
  int32_t col = mgmtFindNormalTableColumnIndex(pTable, colName);
  if (col < 0) {
    return TSDB_CODE_APP_ERROR;
  }

  SDbObj *pDb = mgmtGetDbByTableId(pTable->tableId);
  if (pDb == NULL) {
    mError("table: %s not belongs to any database", pTable->tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("DB: %s not belongs to any account", pDb->name);
    return TSDB_CODE_APP_ERROR;
  }

  memmove(pTable->schema + sizeof(SSchema) * col, pTable->schema + sizeof(SSchema) * (col + 1),
          sizeof(SSchema) * (pTable->numOfColumns - col - 1));

  pTable->numOfColumns--;
  pTable->sversion++;

  pAcct->acctInfo.numOfTimeSeries--;
  sdbUpdateRow(tsNormalTableSdb, pTable, 0, 1);

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtSetSchemaFromNormalTable(SSchema *pSchema, SNormalTableObj *pTable) {
  int32_t numOfCols = pTable->numOfColumns;
  for (int32_t i = 0; i < numOfCols; ++i) {
    strcpy(pSchema->name, pTable->schema[i].name);
    pSchema->type  = pTable->schema[i].type;
    pSchema->bytes = htons(pTable->schema[i].bytes);
    pSchema->colId = htons(pTable->schema[i].colId);
    pSchema++;
  }

  return numOfCols * sizeof(SSchema);
}

int32_t mgmtGetNormalTableMeta(SDbObj *pDb, SNormalTableObj *pTable, STableMeta *pMeta, bool usePublicIp) {
  pMeta->uid          = htobe64(pTable->uid);
  pMeta->sid          = htonl(pTable->sid);
  pMeta->vgid         = htonl(pTable->vgId);
  pMeta->sversion     = htons(pTable->sversion);
  pMeta->precision    = pDb->cfg.precision;
  pMeta->numOfTags    = 0;
  pMeta->numOfColumns = htons(pTable->numOfColumns);
  pMeta->tableType    = pTable->type;
  pMeta->contLen      = sizeof(STableMeta) + mgmtSetSchemaFromNormalTable(pMeta->schema, pTable);

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    return TSDB_CODE_INVALID_TABLE;
  }
  for (int32_t i = 0; i < TSDB_VNODES_SUPPORT; ++i) {
    if (usePublicIp) {
      pMeta->vpeerDesc[i].ip    = pVgroup->vnodeGid[i].publicIp;
      pMeta->vpeerDesc[i].vnode = htonl(pVgroup->vnodeGid[i].vnode);
    } else {
      pMeta->vpeerDesc[i].ip    = pVgroup->vnodeGid[i].ip;
      pMeta->vpeerDesc[i].vnode = htonl(pVgroup->vnodeGid[i].vnode);
    }
  }

  return TSDB_CODE_SUCCESS;
}

