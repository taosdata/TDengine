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
#include "tscompression.h"
#include "tskiplist.h"
#include "ttime.h"
#include "tstatus.h"
#include "tutil.h"
#include "mnode.h"
#include "mgmtAcct.h"
#include "mgmtDb.h"
#include "mgmtDClient.h"
#include "mgmtGrant.h"
#include "mgmtMnode.h"
#include "mgmtNormalTable.h"
#include "mgmtSdb.h"
#include "mgmtSuperTable.h"
#include "mgmtTable.h"
#include "mgmtVgroup.h"

void   *tsNormalTableSdb;
int32_t tsNormalTableUpdateSize;

static void mgmtDestroyNormalTable(SNormalTableObj *pTable) {
  tfree(pTable->schema);
  tfree(pTable->sql);
  tfree(pTable);
}

static int32_t mgmtNormalTableActionDestroy(SSdbOperDesc *pOper) {
  mgmtDestroyNormalTable(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtNormalTableActionInsert(SSdbOperDesc *pOper) {
  SNormalTableObj *pTable = pOper->pObj;

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    mError("ntable:%s not in vgroup:%d", pTable->tableId, pTable->vgId);
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
  if (pDb == NULL) {
    mError("ntable:%s, vgroup:%d not in DB:%s", pTable->tableId, pVgroup->vgId, pVgroup->dbName);
    return TSDB_CODE_INVALID_DB;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("ntable:%s, account:%s not exists", pTable->tableId, pDb->cfg.acct);
    return TSDB_CODE_INVALID_ACCT;
  }

  mgmtAddTimeSeries(pAcct, pTable->numOfColumns - 1);
  mgmtAddTableIntoDb(pDb);
  mgmtAddTableIntoVgroup(pVgroup, (STableInfo *) pTable);

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtNormalTableActionDelete(SSdbOperDesc *pOper) {
  SNormalTableObj *pTable = pOper->pObj;
  if (pTable->vgId == 0) {
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
  if (pDb == NULL) {
    mError("ntable:%s, vgroup:%d not in DB:%s", pTable->tableId, pVgroup->vgId, pVgroup->dbName);
    return TSDB_CODE_INVALID_DB;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("account not exists");
    return TSDB_CODE_INVALID_ACCT;
  }

  mgmtRestoreTimeSeries(pAcct, pTable->numOfColumns - 1);
  mgmtRemoveTableFromDb(pDb);
  mgmtRemoveTableFromVgroup(pVgroup, (STableInfo *) pTable);

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtNormalTableActionUpdate(SSdbOperDesc *pOper) {
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtNormalTableActionEncode(SSdbOperDesc *pOper) {
  SNormalTableObj *pTable = pOper->pObj;
  assert(pOper->pObj != NULL && pOper->rowData != NULL);

  int32_t schemaSize = pTable->numOfColumns * sizeof(SSchema);
  if (pOper->maxRowSize < tsNormalTableUpdateSize + schemaSize) {
    return TSDB_CODE_INVALID_MSG_LEN;
  }

  memcpy(pOper->rowData, pTable, tsNormalTableUpdateSize);
  memcpy(pOper->rowData + tsNormalTableUpdateSize, pTable->schema, schemaSize);
  memcpy(pOper->rowData + tsNormalTableUpdateSize + schemaSize, pTable->sql, pTable->sqlLen);

  pOper->rowSize = tsNormalTableUpdateSize + schemaSize + pTable->sqlLen;
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtNormalTableActionDecode(SSdbOperDesc *pOper) {
  assert(pOper->rowData != NULL);

  SNormalTableObj *pTable = (SNormalTableObj *)calloc(1, sizeof(SNormalTableObj));
  if (pTable == NULL) TSDB_CODE_SERV_OUT_OF_MEMORY;

  memcpy(pTable, pOper->rowData, tsNormalTableUpdateSize);

  int32_t schemaSize = pTable->numOfColumns * sizeof(SSchema);
  pTable->schema = (SSchema *)malloc(schemaSize);
  if (pTable->schema == NULL) {
    mgmtDestroyNormalTable(pTable);
    return -1;
  }

  memcpy(pTable->schema, pOper->rowData + tsNormalTableUpdateSize, schemaSize);

  pTable->sql = (char *)malloc(pTable->sqlLen);
  if (pTable->sql == NULL) {
    mgmtDestroyNormalTable(pTable);
    return -1;
  }
  memcpy(pTable->sql, pOper->rowData + tsNormalTableUpdateSize + schemaSize, pTable->sqlLen);
  
  pOper->pObj = pTable;
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtInitNormalTables() {
  void *pNode = NULL;
  void *pLastNode = NULL;
  SNormalTableObj *pTable = NULL;

  SNormalTableObj tObj;
  tsNormalTableUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableName    = "ntables",
    .hashSessions = TSDB_MAX_NORMAL_TABLES,
    .maxRowSize   = sizeof(SNormalTableObj) + sizeof(SSchema) * TSDB_MAX_COLUMNS,
    .keyType      = SDB_KEY_TYPE_STRING,
    .insertFp     = mgmtNormalTableActionInsert,
    .deleteFp     = mgmtNormalTableActionDelete,
    .updateFp     = mgmtNormalTableActionUpdate,
    .encodeFp     = mgmtNormalTableActionEncode,
    .decodeFp     = mgmtNormalTableActionDecode,
    .destroyFp    = mgmtNormalTableActionDestroy,
  };

  tsNormalTableSdb = sdbOpenTable(&tableDesc);
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
       SSdbOperDesc desc = {0};
      desc.type = SDB_OPER_TYPE_LOCAL;
      desc.pObj = pTable;
      desc.table = tsNormalTableSdb;
      sdbDeleteRow(&desc);
      pNode = pLastNode;
      continue;
    }

    SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
    if (pVgroup == NULL) {
      mError("ntable:%s, failed to get vgroup:%d sid:%d, discard it", pTable->tableId, pTable->vgId, pTable->sid);
      pTable->vgId = 0;
      SSdbOperDesc desc = {0};
      desc.type = SDB_OPER_TYPE_LOCAL;
      desc.pObj = pTable;
      desc.table = tsNormalTableSdb;
      sdbDeleteRow(&desc);
      pNode = pLastNode;
      continue;
    }

    if (strcmp(pVgroup->dbName, pDb->name) != 0) {
      mError("ntable:%s, db:%s not match with vgroup:%d db:%s sid:%d, discard it",
               pTable->tableId, pDb->name, pTable->vgId, pVgroup->dbName, pTable->sid);
      pTable->vgId = 0;
      
      SSdbOperDesc desc = {0};
      desc.type = SDB_OPER_TYPE_LOCAL;
      desc.pObj = pTable;
      desc.table = tsNormalTableSdb;
      sdbDeleteRow(&desc);
      pNode = pLastNode;
      continue;
    }

    if (pVgroup->tableList == NULL) {
      mError("ntable:%s, vgroup:%d tableList is null", pTable->tableId, pTable->vgId);
      pTable->vgId = 0;
       SSdbOperDesc desc = {0};
      desc.type = SDB_OPER_TYPE_LOCAL;
      desc.pObj = pTable;
      desc.table = tsNormalTableSdb;
      sdbDeleteRow(&desc);
      pNode = pLastNode;
      continue;
    }
  }

  mTrace("ntables is initialized");
  return 0;
}

void mgmtCleanUpNormalTables() {
  sdbCloseTable(tsNormalTableSdb);
}

void *mgmtBuildCreateNormalTableMsg(SNormalTableObj *pTable) {
  int32_t totalCols = pTable->numOfColumns;
  int32_t contLen   = sizeof(SMDCreateTableMsg) + totalCols * sizeof(SSchema) + pTable->sqlLen;

  SMDCreateTableMsg *pCreate = rpcMallocCont(contLen);
  if (pCreate == NULL) {
    terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
    return NULL;
  }

  memcpy(pCreate->tableId, pTable->tableId, TSDB_TABLE_ID_LEN + 1);
  pCreate->contLen       = htonl(contLen);
  pCreate->vgId          = htonl(pTable->vgId);
  pCreate->tableType     = pTable->type;
  pCreate->numOfColumns  = htons(pTable->numOfColumns);
  pCreate->numOfTags     = 0;
  pCreate->sid           = htonl(pTable->sid);
  pCreate->sversion      = htonl(pTable->sversion);
  pCreate->tagDataLen    = 0;
  pCreate->sqlDataLen    = htonl(pTable->sqlLen);
  pCreate->uid           = htobe64(pTable->uid);
  pCreate->superTableUid = 0;
  pCreate->createdTime   = htobe64(pTable->createdTime);

  SSchema *pSchema = (SSchema *) pCreate->data;
  memcpy(pSchema, pTable->schema, totalCols * sizeof(SSchema));
  for (int32_t col = 0; col < totalCols; ++col) {
    pSchema->bytes = htons(pSchema->bytes);
    pSchema->colId = htons(pSchema->colId);
    pSchema++;
  }

  memcpy(pCreate + sizeof(SMDCreateTableMsg) + totalCols * sizeof(SSchema), pTable->sql, pTable->sqlLen);
  return pCreate;
}

void *mgmtCreateNormalTable(SCMCreateTableMsg *pCreate, SVgObj *pVgroup, int32_t sid) {
  SNormalTableObj *pTable = (SNormalTableObj *) calloc(sizeof(SNormalTableObj), 1);
  if (pTable == NULL) {
    mError("table:%s, failed to alloc memory", pCreate->tableId);
    terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
    return NULL;
  }

  strcpy(pTable->tableId, pCreate->tableId);
  pTable->type         = TSDB_NORMAL_TABLE;
  pTable->vgId         = pVgroup->vgId;
  pTable->createdTime  = taosGetTimestampMs();
  pTable->uid          = (((uint64_t) pTable->createdTime) << 16) + (sdbGetVersion() & ((1ul << 16) - 1ul));
  pTable->sid          = sid;
  pTable->sversion     = 0;
  pTable->numOfColumns = htons(pCreate->numOfColumns);
  pTable->sqlLen       = htons(pCreate->sqlLen);

  int32_t numOfCols  = pTable->numOfColumns;
  int32_t schemaSize = numOfCols * sizeof(SSchema);
  pTable->schema     = (SSchema *) calloc(1, schemaSize);
  if (pTable->schema == NULL) {
    free(pTable);
    terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
    return NULL;
  }
  memcpy(pTable->schema, pCreate->schema, numOfCols * sizeof(SSchema));

  pTable->nextColId = 0;
  for (int32_t col = 0; col < numOfCols; col++) {
    SSchema *tschema   = pTable->schema;
    tschema[col].colId = pTable->nextColId++;
    tschema[col].bytes = htons(tschema[col].bytes);
  }

  if (pTable->sqlLen != 0) {
    pTable->type = TSDB_STREAM_TABLE;
    pTable->sql = calloc(1, pTable->sqlLen);
    if (pTable->sql == NULL) {
      free(pTable);
      terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
      return NULL;
    }
    memcpy(pTable->sql, (char *) (pCreate->schema) + numOfCols * sizeof(SSchema), pTable->sqlLen);
    pTable->sql[pTable->sqlLen - 1] = 0;
    mTrace("table:%s, stream sql len:%d sql:%s", pTable->tableId, pTable->sqlLen, pTable->sql);
  }

  SSdbOperDesc desc = {0};
  desc.type = SDB_OPER_TYPE_GLOBAL;
  desc.pObj = pTable;
  desc.table = tsNormalTableSdb;
  if (sdbInsertRow(&desc) < 0) {
    mError("table:%s, update sdb error", pTable->tableId);
    free(pTable);
    terrno = TSDB_CODE_SDB_ERROR;
    return NULL;
  }

  mTrace("table:%s, create ntable in vgroup, uid:%" PRIu64 , pTable->tableId, pTable->uid);
  return pTable;
}

int32_t mgmtDropNormalTable(SQueuedMsg *newMsg, SNormalTableObj *pTable) {
  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    mError("table:%s, failed to drop normal table, vgroup not exist", pTable->tableId);
    return TSDB_CODE_OTHERS;
  }

  SMDDropTableMsg *pDrop = rpcMallocCont(sizeof(SMDDropTableMsg));
  if (pDrop == NULL) {
    mError("table:%s, failed to drop normal table, no enough memory", pTable->tableId);
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  strcpy(pDrop->tableId, pTable->tableId);
  pDrop->contLen = htonl(sizeof(SMDDropTableMsg));
  pDrop->vgId    = htonl(pVgroup->vgId);
  pDrop->sid     = htonl(pTable->sid);
  pDrop->uid     = htobe64(pTable->uid);

  SRpcIpSet ipSet = mgmtGetIpSetFromVgroup(pVgroup);
  mTrace("table:%s, send drop table msg", pDrop->tableId);
  SRpcMsg rpcMsg = {
      .handle  = newMsg,
      .pCont   = pDrop,
      .contLen = sizeof(SMDDropTableMsg),
      .code    = 0,
      .msgType = TSDB_MSG_TYPE_MD_DROP_TABLE
  };

  newMsg->ahandle = pTable;
  mgmtSendMsgToDnode(&ipSet, &rpcMsg);
  return TSDB_CODE_SUCCESS;
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

  SSdbOperDesc desc = {0};
  desc.type = SDB_OPER_TYPE_GLOBAL;
  desc.pObj = pTable;
  desc.table = tsNormalTableSdb;
  desc.rowData = pTable;
  desc.rowSize = tsNormalTableUpdateSize;
  sdbUpdateRow(&desc);

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

  SSdbOperDesc desc = {0};
  desc.type = SDB_OPER_TYPE_GLOBAL;
  desc.pObj = pTable;
  desc.table = tsNormalTableSdb;
  desc.rowData = pTable;
  desc.rowSize = tsNormalTableUpdateSize;
  sdbUpdateRow(&desc);

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

int32_t mgmtGetNormalTableMeta(SDbObj *pDb, SNormalTableObj *pTable, STableMetaMsg *pMeta, bool usePublicIp) {
  pMeta->uid          = htobe64(pTable->uid);
  pMeta->sid          = htonl(pTable->sid);
  pMeta->vgId         = htonl(pTable->vgId);
  pMeta->sversion     = htons(pTable->sversion);
  pMeta->precision    = pDb->cfg.precision;
  pMeta->numOfTags    = 0;
  pMeta->numOfColumns = htons(pTable->numOfColumns);
  pMeta->tableType    = pTable->type;
  pMeta->contLen      = sizeof(STableMetaMsg) + mgmtSetSchemaFromNormalTable(pMeta->schema, pTable);
  
  strncpy(pMeta->tableId, pTable->tableId, tListLen(pTable->tableId));

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    return TSDB_CODE_INVALID_TABLE;
  }
  for (int32_t i = 0; i < TSDB_VNODES_SUPPORT; ++i) {
    if (usePublicIp) {
      pMeta->vpeerDesc[i].ip    = pVgroup->vnodeGid[i].publicIp;
    } else {
      pMeta->vpeerDesc[i].ip    = pVgroup->vnodeGid[i].privateIp;
    }
    
    pMeta->vpeerDesc[i].vnode = htonl(pVgroup->vnodeGid[i].vnode);
  }
  pMeta->numOfVpeers = pVgroup->numOfVnodes;

  return TSDB_CODE_SUCCESS;
}

void mgmtDropAllNormalTables(SDbObj *pDropDb) {
  void *pNode = NULL;
  void *pLastNode = NULL;
  int32_t numOfTables = 0;
  int32_t dbNameLen = strlen(pDropDb->name);
  SNormalTableObj *pTable = NULL;

  while (1) {
    pNode = sdbFetchRow(tsNormalTableSdb, pNode, (void **)&pTable);
    if (pTable == NULL) break;

    if (strncmp(pDropDb->name, pTable->tableId, dbNameLen) == 0) {
      SSdbOperDesc oper = {
        .type = SDB_OPER_TYPE_LOCAL,
        .table = tsNormalTableSdb,
        .pObj = pTable,
      };
      sdbDeleteRow(&oper);
      pNode = pLastNode;
      numOfTables++;
      continue;
    }
  }

  mTrace("db:%s, all normal tables:%d is dropped from sdb", pDropDb->name, numOfTables);
}
