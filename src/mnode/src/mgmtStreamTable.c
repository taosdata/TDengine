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
#include "mgmtStreamTable.h"


void *tsStreamTableSdb;
void *(*mgmtStreamTableActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int size, int *ssize);

void *mgmtStreamTableActionInsert(void *row, char *str, int size, int *ssize);
void *mgmtStreamTableActionDelete(void *row, char *str, int size, int *ssize);
void *mgmtStreamTableActionUpdate(void *row, char *str, int size, int *ssize);
void *mgmtStreamTableActionEncode(void *row, char *str, int size, int *ssize);
void *mgmtStreamTableActionDecode(void *row, char *str, int size, int *ssize);
void *mgmtStreamTableActionReset(void *row, char *str, int size, int *ssize);
void *mgmtStreamTableActionDestroy(void *row, char *str, int size, int *ssize);

static void mgmtDestroyStreamTable(SStreamTableObj *pTable) {
  free(pTable->schema);
  free(pTable->pSql);
  free(pTable);
}

static void mgmtStreamTableActionInit() {
  mgmtStreamTableActionFp[SDB_TYPE_INSERT] = mgmtStreamTableActionInsert;
  mgmtStreamTableActionFp[SDB_TYPE_DELETE] = mgmtStreamTableActionDelete;
  mgmtStreamTableActionFp[SDB_TYPE_UPDATE] = mgmtStreamTableActionUpdate;
  mgmtStreamTableActionFp[SDB_TYPE_ENCODE] = mgmtStreamTableActionEncode;
  mgmtStreamTableActionFp[SDB_TYPE_DECODE] = mgmtStreamTableActionDecode;
  mgmtStreamTableActionFp[SDB_TYPE_RESET] = mgmtStreamTableActionReset;
  mgmtStreamTableActionFp[SDB_TYPE_DESTROY] = mgmtStreamTableActionDestroy;
}

void *mgmtStreamTableActionReset(void *row, char *str, int size, int *ssize) {
  SStreamTableObj *pTable = (SStreamTableObj *) row;
  int tsize = pTable->updateEnd - (int8_t *) pTable;
  memcpy(pTable, str, tsize);
  pTable->schema = (char *) realloc(pTable->schema, pTable->schemaSize);
  memcpy(pTable->schema, str + tsize, pTable->schemaSize);
  pTable->pSql = (char *) realloc(pTable->pSql, pTable->sqlLen);
  memcpy(pTable->pSql, str + tsize + pTable->schemaSize, pTable->sqlLen);
  return NULL;
}

void *mgmtStreamTableActionDestroy(void *row, char *str, int size, int *ssize) {
  SStreamTableObj *pTable = (SStreamTableObj *)row;
  mgmtDestroyStreamTable(pTable);
  return NULL;
}

void *mgmtStreamTableActionInsert(void *row, char *str, int size, int *ssize) {
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

void *mgmtStreamTableActionDelete(void *row, char *str, int size, int *ssize) {
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

void *mgmtStreamTableActionUpdate(void *row, char *str, int size, int *ssize) {
  return mgmtStreamTableActionReset(row, str, size, NULL);
}

void *mgmtStreamTableActionEncode(void *row, char *str, int size, int *ssize) {
  SStreamTableObj *pTable = (SStreamTableObj *) row;
  assert(row != NULL && str != NULL);

  int tsize = pTable->updateEnd - (int8_t *) pTable;
  if (size < tsize + pTable->schemaSize + pTable->sqlLen + 1) {
    *ssize = -1;
    return NULL;
  }

  memcpy(str, pTable, tsize);
  memcpy(str + tsize, pTable->schema, pTable->schemaSize);
  memcpy(str + tsize + pTable->schemaSize, pTable->pSql, pTable->sqlLen);
  *ssize = tsize + pTable->schemaSize + pTable->sqlLen;

  return NULL;
}

void *mgmtStreamTableActionDecode(void *row, char *str, int size, int *ssize) {
  assert(str != NULL);

  SStreamTableObj *pTable = (SStreamTableObj *)malloc(sizeof(SNormalTableObj));
  if (pTable == NULL) {
    return NULL;
  }
  memset(pTable, 0, sizeof(STabObj));

  int tsize = pTable->updateEnd - (int8_t *)pTable;
  if (size < tsize) {
    mgmtDestroyStreamTable(pTable);
    return NULL;
  }
  memcpy(pTable, str, tsize);

  pTable->schema = (char *)malloc(pTable->schemaSize);
  if (pTable->schema == NULL) {
    mgmtDestroyStreamTable(pTable);
    return NULL;
  }
  memcpy(pTable->schema, str + tsize, pTable->schemaSize);

  pTable->pSql = (char *)malloc(pTable->sqlLen);
  if (pTable->pSql == NULL) {
    mgmtDestroyStreamTable(pTable);
    return NULL;
  }
  memcpy(pTable->pSql, str + tsize + pTable->schemaSize, pTable->sqlLen);
  return (void *)pTable;
}

void *mgmtStreamTableAction(char action, void *row, char *str, int size, int *ssize) {
  if (mgmtStreamTableActionFp[(uint8_t)action] != NULL) {
    return (*(mgmtStreamTableActionFp[(uint8_t)action]))(row, str, size, ssize);
  }
  return NULL;
}

int32_t mgmtInitStreamTables() {
  return 0;
}

void mgmtCleanUpStreamTables() {
}

int8_t *mgmtBuildCreateStreamTableMsg(SStreamTableObj *pTable, int8_t *pMsg, int32_t vnode) {
  SDCreateTableMsg *pCreateTable = (SDCreateTableMsg *) pMsg;
  memcpy(pCreateTable->tableId, pTable->tableId, TSDB_TABLE_ID_LEN);
  pCreateTable->vnode        = htonl(vnode);
  pCreateTable->sid          = htonl(pTable->sid);
  pCreateTable->uid          = pTable->uid;
  pCreateTable->createdTime  = htobe64(pTable->createdTime);
  pCreateTable->sversion     = htonl(pTable->sversion);
  pCreateTable->numOfColumns = htons(pTable->numOfColumns);
  //pCreateTable->sqlLen       = htons(pTable->sqlLen);

  SSchema *pSchema  = pTable->schema;
  int32_t totalCols = pCreateTable->numOfColumns;

//  for (int32_t col = 0; col < totalCols; ++col) {
//    SMColumn *colData = &((SMColumn *) (pCreateTable->data))[col];
//    colData->type  = pSchema[col].type;
//    colData->bytes = htons(pSchema[col].bytes);
//    colData->colId = htons(pSchema[col].colId);
//  }

//  int32_t totalColsSize = sizeof(SMColumn *) * totalCols;
//  pMsg = pCreateTable->data + totalColsSize + pTable->sqlLen;

//  char *sql = pTable->schema + pTable->schemaSize;
//  memcpy(pCreateTable->data + totalColsSize, pTable->sqlLen, sql);

  return pMsg;
}

int32_t mgmtCreateStreamTable(SDbObj *pDb, SCreateTableMsg *pCreate, SVgObj *pVgroup, int32_t sid) {
  int numOfTables = sdbGetNumOfRows(tsStreamTableSdb);
  if (numOfTables >= TSDB_MAX_TABLES) {
    mError("stream table:%s, numOfTables:%d exceed maxTables:%d", pCreate->meterId, numOfTables, TSDB_MAX_TABLES);
    return TSDB_CODE_TOO_MANY_TABLES;
  }

  SStreamTableObj *pTable = (SStreamTableObj *) calloc(sizeof(SStreamTableObj), 1);
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
  pTable->schemaSize = numOfCols * sizeof(SSchema) + pCreate->sqlLen;
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

  pTable->pSql = pTable->schema + numOfCols * sizeof(SSchema);
  memcpy(pTable->pSql, (char *) (pCreate->schema) + numOfCols * sizeof(SSchema), pCreate->sqlLen);
  pTable->pSql[pCreate->sqlLen - 1] = 0;
  mTrace("table:%s, stream sql len:%d sql:%s", pCreate->meterId, pCreate->sqlLen, pTable->pSql);

  if (sdbInsertRow(tsStreamTableSdb, pTable, 0) < 0) {
    mError("table:%s, update sdb error", pCreate->meterId);
    return TSDB_CODE_SDB_ERROR;
  }

  mgmtAddTimeSeries(pTable->numOfColumns - 1);

  mgmtSendCreateStreamTableMsg(pTable, pVgroup);

  mTrace("table:%s, create table in vgroup, vgId:%d sid:%d vnode:%d uid:%"
             PRIu64
             " db:%s",
         pTable->tableId, pVgroup->vgId, sid, pVgroup->vnodeGid[0].vnode, pTable->uid, pDb->name);

  return 0;
}

int32_t mgmtDropStreamTable(SDbObj *pDb, SStreamTableObj *pTable) {
  SVgObj *  pVgroup;
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

SStreamTableObj* mgmtGetStreamTable(char *tableId) {
  return (SStreamTableObj *)sdbGetRow(tsStreamTableSdb, tableId);
}