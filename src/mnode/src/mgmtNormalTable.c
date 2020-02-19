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
#include "mgmtNormalTable.h"


void *tsNormalTableSdb;
void *(*mgmtNormalTableActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int size, int *ssize);

void *mgmtNormalTableActionInsert(void *row, char *str, int size, int *ssize);
void *mgmtNormalTableActionDelete(void *row, char *str, int size, int *ssize);
void *mgmtNormalTableActionUpdate(void *row, char *str, int size, int *ssize);
void *mgmtNormalTableActionEncode(void *row, char *str, int size, int *ssize);
void *mgmtNormalTableActionDecode(void *row, char *str, int size, int *ssize);
void *mgmtNormalTableActionReset(void *row, char *str, int size, int *ssize);
void *mgmtNormalTableActionDestroy(void *row, char *str, int size, int *ssize);

static void mgmtDestroyNormalTable(SNormalTableObj *pTable) {
  free(pTable->schema);
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

void *mgmtNormalTableActionReset(void *row, char *str, int size, int *ssize) {
  SNormalTableObj *pTable = (SNormalTableObj *) row;
  int tsize = pTable->updateEnd - (int8_t *) pTable;
  memcpy(pTable, str, tsize);
  return NULL;
}

void *mgmtNormalTableActionDestroy(void *row, char *str, int size, int *ssize) {
  SNormalTableObj *pTable = (SNormalTableObj *)row;
  mgmtDestroyNormalTable(pTable);
  return NULL;
}

void *mgmtNormalTableActionInsert(void *row, char *str, int size, int *ssize) {
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
    int sid = taosAllocateId(pVgroup->idPool);
    if (sid != pTable->sid) {
      mError("sid:%d is not matched from the master:%d", sid, pTable->sid);
      return NULL;
    }
  }

  pAcct->acctInfo.numOfTimeSeries += (pTable->numOfColumns - 1);
  pVgroup->numOfMeters++;
  pDb->numOfTables++;
  pVgroup->meterList[pTable->sid] = pTable;

  if (pVgroup->numOfMeters >= pDb->cfg.maxSessions - 1 && pDb->numOfVgroups > 1) {
    mgmtMoveVgroupToTail(pDb, pVgroup);
  }

  return NULL;
}

void *mgmtNormalTableActionDelete(void *row, char *str, int size, int *ssize) {
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

  pAcct->acctInfo.numOfTimeSeries -= (pTable->numOfColumns - 1);
  pVgroup->meterList[pTable->sid] = NULL;
  pVgroup->numOfMeters--;
  pDb->numOfTables--;
  taosFreeId(pVgroup->idPool, pTable->sid);

  if (pVgroup->numOfMeters > 0) {
    mgmtMoveVgroupToHead(pDb, pVgroup);
  }

  return NULL;
}

void *mgmtNormalTableActionUpdate(void *row, char *str, int size, int *ssize) {
  return mgmtNormalTableActionReset(row, str, size, NULL);
}

void *mgmtNormalTableActionEncode(void *row, char *str, int size, int *ssize) {
  SNormalTableObj *pTable = (SNormalTableObj *) row;
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

void *mgmtNormalTableActionDecode(void *row, char *str, int size, int *ssize) {
  assert(str != NULL);

  SNormalTableObj *pTable = (SNormalTableObj *)malloc(sizeof(SNormalTableObj));
  if (pTable == NULL) {
    return NULL;
  }
  memset(pTable, 0, sizeof(STabObj));

  int tsize = pTable->updateEnd - (int8_t *)pTable;
  if (size < tsize) {
    mgmtDestroyNormalTable(pTable);
    return NULL;
  }
  memcpy(pTable, str, tsize);

  pTable->schema = (char *)malloc(pTable->schemaSize);
  if (pTable->schema == NULL) {
    mgmtDestroyNormalTable(pTable);
    return NULL;
  }

  memcpy(pTable->schema, str + tsize, pTable->schemaSize);
  return (void *)pTable;
}

void *mgmtNormalTableAction(char action, void *row, char *str, int size, int *ssize) {
  if (mgmtNormalTableActionFp[(uint8_t)action] != NULL) {
    return (*(mgmtNormalTableActionFp[(uint8_t)action]))(row, str, size, ssize);
  }
  return NULL;
}

int32_t mgmtInitNormalTables() {
  return 0;
}

void mgmtCleanUpNormalTables() {
  sdbCloseTable(tsNormalTableSdb);
}

int8_t *mgmtBuildCreateNormalTableMsg(SNormalTableObj *pTable, int32_t vnode) {
  int8_t *pMsg = NULL;
  SDCreateTableMsg *pCreateTable = (SDCreateTableMsg *) pMsg;
  memcpy(pCreateTable->tableId, pTable->tableId, TSDB_TABLE_ID_LEN);
  pCreateTable->vnode        = htobe32(vnode);
  pCreateTable->sid          = htobe32(pTable->sid);
  pCreateTable->uid          = htobe64(pTable->uid);
  pCreateTable->createdTime  = htobe64(pTable->createdTime);
  pCreateTable->sversion     = htobe32(pTable->sversion);
  pCreateTable->numOfColumns = htobe16(pTable->numOfColumns);

  SSchema *pSchema  = pTable->schema;
  int32_t totalCols = pCreateTable->numOfColumns;

//  for (int32_t col = 0; col < totalCols; ++col) {
//    SMColumn *colData = &((SMColumn *) (pCreateTable->data))[col];
//    colData->type  = pSchema[col].type;
//    colData->bytes = htons(pSchema[col].bytes);
//    colData->colId = htons(pSchema[col].colId);
//  }

//  int32_t totalColsSize = sizeof(SMColumn *) * totalCols;
//  pMsg = pCreateTable->data + totalColsSize;

  return pMsg;
}

int32_t mgmtCreateNormalTable(SDbObj *pDb, SCreateTableMsg *pCreate, SVgObj *pVgroup, int32_t sid) {
  int numOfTables = sdbGetNumOfRows(tsChildTableSdb);
  if (numOfTables >= TSDB_MAX_TABLES) {
    mError("normal table:%s, numOfTables:%d exceed maxTables:%d", pCreate->meterId, numOfTables, TSDB_MAX_TABLES);
    return TSDB_CODE_TOO_MANY_TABLES;
  }

  SNormalTableObj *pTable = (SNormalTableObj *) calloc(sizeof(SNormalTableObj), 1);
  if (pTable == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  strcpy(pTable->tableId, pCreate->meterId);
  pTable->createdTime  = taosGetTimestampMs();
  pTable->vgId         = pVgroup->vgId;
  pTable->sid          = sid;
  pTable->uid          = (((uint64_t) pTable->createdTime) << 16) + ((uint64_t) sdbGetVersion() & ((1ul << 16) - 1ul));
  pTable->sversion     = 0;
  pTable->numOfColumns = pCreate->numOfColumns;

  int numOfCols = pCreate->numOfColumns + pCreate->numOfTags;
  pTable->schemaSize = numOfCols * sizeof(SSchema);
  pTable->schema     = (int8_t *) calloc(1, pTable->schemaSize);
  if (pTable->schema == NULL) {
    free(pTable);
    mError("table:%s, no schema input", pCreate->meterId);
    return TSDB_CODE_INVALID_TABLE;
  }
  memcpy(pTable->schema, pCreate->schema, numOfCols * sizeof(SSchema));

  pTable->nextColId = 0;
  for (int col = 0; col < pCreate->numOfColumns; col++) {
    SSchema *tschema   = (SSchema *) pTable->schema;
    tschema[col].colId = pTable->nextColId++;
  }

  if (sdbInsertRow(tsNormalTableSdb, pTable, 0) < 0) {
    mError("table:%s, update sdb error", pCreate->meterId);
    return TSDB_CODE_SDB_ERROR;
  }

  mgmtAddTimeSeries(pTable->numOfColumns - 1);

  mgmtSendCreateNormalTableMsg(pTable, pVgroup);

  mTrace("table:%s, create table in vgroup, vgId:%d sid:%d vnode:%d uid:%"
             PRIu64
             " db:%s",
         pTable->tableId, pVgroup->vgId, sid, pVgroup->vnodeGid[0].vnode, pTable->uid, pDb->name);

  return 0;
}

int32_t mgmtDropNormalTable(SDbObj *pDb, SNormalTableObj *pTable) {
  SVgObj *pVgroup;
  SAcctObj *pAcct;

  pAcct = mgmtGetAcct(pDb->cfg.acct);

  if (pAcct != NULL) {
    pAcct->acctInfo.numOfTimeSeries -= (pTable->numOfColumns - 1);
  }

  pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    return TSDB_CODE_OTHERS;
  }

  mgmtRestoreTimeSeries(pTable->numOfColumns - 1);

  mgmtSendRemoveMeterMsgToDnode(pTable, pVgroup);

  sdbDeleteRow(tsChildTableSdb, pTable);

  if (pVgroup->numOfMeters <= 0) {
    mgmtDropVgroup(pDb, pVgroup);
  }

  return 0;
}

SNormalTableObj* mgmtGetNormalTable(char *tableId) {
  return (SNormalTableObj *)sdbGetRow(tsNormalTableSdb, tableId);
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

int32_t mgmtAddNormalTableColumn(SNormalTableObj *pTable, SSchema schema[], int ncols) {
  if (ncols <= 0) {
    return TSDB_CODE_APP_ERROR;
  }

  for (int i = 0; i < ncols; i++) {
    if (mgmtFindNormalTableColumnIndex(pTable, schema[i].name) > 0) {
      return TSDB_CODE_APP_ERROR;
    }
  }

  SDbObj *pDb = mgmtGetDbByMeterId(pTable->tableId);
  if (pDb == NULL) {
    mError("table: %s not belongs to any database", pTable->tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("DB: %s not belongs to andy account", pDb->name);
    return TSDB_CODE_APP_ERROR;
  }

  pTable->schema = realloc(pTable->schema, pTable->schemaSize + sizeof(SSchema) * ncols);

  memcpy(pTable->schema + pTable->schemaSize, schema, sizeof(SSchema) * ncols);

  SSchema *tschema = (SSchema *) (pTable->schema + sizeof(SSchema) * pTable->numOfColumns);
  for (int i = 0; i < ncols; i++) {
    tschema[i].colId = pTable->nextColId++;
  }

  pTable->schemaSize += sizeof(SSchema) * ncols;
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

  SDbObj *pDb = mgmtGetDbByMeterId(pTable->tableId);
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


  pTable->schemaSize -= sizeof(SSchema);
  pTable->numOfColumns--;
  pTable->schema = realloc(pTable->schema, pTable->schemaSize);
  pTable->sversion++;

  pAcct->acctInfo.numOfTimeSeries--;
  sdbUpdateRow(tsNormalTableSdb, pTable, 0, 1);

  return TSDB_CODE_SUCCESS;
}
