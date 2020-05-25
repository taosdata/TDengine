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
#include "ttime.h"
#include "tutil.h"
#include "taoserror.h"
#include "taosmsg.h"
#include "tscompression.h"
#include "tname.h"
#include "tidpool.h"
#include "tglobal.h"
#include "hash.h"
#include "dnode.h"
#include "mgmtDef.h"
#include "mgmtInt.h"
#include "mgmtAcct.h"
#include "mgmtDb.h"
#include "mgmtDnode.h"
#include "tgrant.h"
#include "mgmtMnode.h"
#include "mgmtProfile.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtTable.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"
#include "tcompare.h"
#include "tdataformat.h"

static void *  tsChildTableSdb;
static void *  tsSuperTableSdb;
static int32_t tsChildTableUpdateSize;
static int32_t tsSuperTableUpdateSize;
static void *  mgmtGetChildTable(char *tableId);
static void *  mgmtGetSuperTable(char *tableId);
static void *  mgmtGetSuperTableByUid(uint64_t uid);
static void    mgmtDropAllChildTablesInStable(SSuperTableObj *pStable);
static void    mgmtAddTableIntoStable(SSuperTableObj *pStable, SChildTableObj *pCtable);
static void    mgmtRemoveTableFromStable(SSuperTableObj *pStable, SChildTableObj *pCtable);

static int32_t mgmtGetShowTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mgmtRetrieveShowTables(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mgmtRetrieveShowSuperTables(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mgmtGetShowSuperTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);

static void mgmtProcessCreateTableMsg(SQueuedMsg *queueMsg);
static void mgmtProcessCreateSuperTableMsg(SQueuedMsg *pMsg);
static void mgmtProcessCreateChildTableMsg(SQueuedMsg *pMsg);
static void mgmtProcessCreateChildTableRsp(SRpcMsg *rpcMsg);

static void mgmtProcessDropTableMsg(SQueuedMsg *queueMsg);
static void mgmtProcessDropSuperTableMsg(SQueuedMsg *pMsg);
static void mgmtProcessDropSuperTableRsp(SRpcMsg *rpcMsg);
static void mgmtProcessDropChildTableMsg(SQueuedMsg *pMsg);
static void mgmtProcessDropChildTableRsp(SRpcMsg *rpcMsg);

static void mgmtProcessSuperTableVgroupMsg(SQueuedMsg *queueMsg);
static void mgmtProcessMultiTableMetaMsg(SQueuedMsg *queueMsg);
static void mgmtProcessTableCfgMsg(SRpcMsg *rpcMsg);

static void mgmtProcessTableMetaMsg(SQueuedMsg *queueMsg);
static void mgmtGetSuperTableMeta(SQueuedMsg *pMsg);
static void mgmtGetChildTableMeta(SQueuedMsg *pMsg);
static void mgmtAutoCreateChildTable(SQueuedMsg *pMsg);

static void mgmtProcessAlterTableMsg(SQueuedMsg *queueMsg);
static void mgmtProcessAlterTableRsp(SRpcMsg *rpcMsg);

static int32_t mgmtFindSuperTableColumnIndex(SSuperTableObj *pStable, char *colName);

static void mgmtDestroyChildTable(SChildTableObj *pTable) {
  tfree(pTable->info.tableId);
  tfree(pTable->schema);
  tfree(pTable->sql);
  tfree(pTable);
}

static int32_t mgmtChildTableActionDestroy(SSdbOper *pOper) {
  mgmtDestroyChildTable(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionInsert(SSdbOper *pOper) {
  SChildTableObj *pTable = pOper->pObj;

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    mError("ctable:%s, not in vgId:%d", pTable->info.tableId, pTable->vgId);
    return TSDB_CODE_INVALID_VGROUP_ID;
  }
  mgmtDecVgroupRef(pVgroup);

  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
  if (pDb == NULL) {
    mError("ctable:%s, vgId:%d not in db:%s", pTable->info.tableId, pVgroup->vgId, pVgroup->dbName);
    return TSDB_CODE_INVALID_DB;
  }
  mgmtDecDbRef(pDb);

  SAcctObj *pAcct = mgmtGetAcct(pDb->acct);
  if (pAcct == NULL) {
    mError("ctable:%s, acct:%s not exists", pTable->info.tableId, pDb->acct);
    return TSDB_CODE_INVALID_ACCT;
  }
  mgmtDecAcctRef(pAcct);

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    // add ref
    pTable->superTable = mgmtGetSuperTableByUid(pTable->suid);
    mgmtAddTableIntoStable(pTable->superTable, pTable);
    grantAdd(TSDB_GRANT_TIMESERIES, pTable->superTable->numOfColumns - 1);
    pAcct->acctInfo.numOfTimeSeries += (pTable->superTable->numOfColumns - 1);
  } else {
    grantAdd(TSDB_GRANT_TIMESERIES, pTable->numOfColumns - 1);
    pAcct->acctInfo.numOfTimeSeries += (pTable->numOfColumns - 1);
  }

  mgmtAddTableIntoDb(pDb);
  mgmtAddTableIntoVgroup(pVgroup, pTable);

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionDelete(SSdbOper *pOper) {
  SChildTableObj *pTable = pOper->pObj;
  if (pTable->vgId == 0) {
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    return TSDB_CODE_INVALID_VGROUP_ID;
  }
  mgmtDecVgroupRef(pVgroup);

  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
  if (pDb == NULL) {
    mError("ctable:%s, vgId:%d not in DB:%s", pTable->info.tableId, pVgroup->vgId, pVgroup->dbName);
    return TSDB_CODE_INVALID_DB;
  }
  mgmtDecDbRef(pDb);

  SAcctObj *pAcct = mgmtGetAcct(pDb->acct);
  if (pAcct == NULL) {
    mError("ctable:%s, acct:%s not exists", pTable->info.tableId, pDb->acct);
    return TSDB_CODE_INVALID_ACCT;
  }
  mgmtDecAcctRef(pAcct);

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    grantRestore(TSDB_GRANT_TIMESERIES, pTable->superTable->numOfColumns - 1);
    pAcct->acctInfo.numOfTimeSeries -= (pTable->superTable->numOfColumns - 1);
    mgmtRemoveTableFromStable(pTable->superTable, pTable);
    mgmtDecTableRef(pTable->superTable);
  } else {
    grantRestore(TSDB_GRANT_TIMESERIES, pTable->numOfColumns - 1);
    pAcct->acctInfo.numOfTimeSeries -= (pTable->numOfColumns - 1);
  }
  mgmtRemoveTableFromDb(pDb);
  mgmtRemoveTableFromVgroup(pVgroup, pTable);
 
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionUpdate(SSdbOper *pOper) {
  SChildTableObj *pNew = pOper->pObj;
  SChildTableObj *pTable = mgmtGetChildTable(pNew->info.tableId);
  if (pTable != pNew) {
    void *oldTableId = pTable->info.tableId;
    void *oldSql = pTable->sql;
    void *oldSchema = pTable->schema;
    memcpy(pTable, pNew, pOper->rowSize);
    pTable->sql = pNew->sql;
    pTable->schema = pNew->schema;
    free(pNew);
    free(oldSql);
    free(oldSchema);
    free(oldTableId);
  }
  mgmtDecTableRef(pTable);

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionEncode(SSdbOper *pOper) {
  SChildTableObj *pTable = pOper->pObj;
  assert(pTable != NULL && pOper->rowData != NULL);

  int32_t len = strlen(pTable->info.tableId);
  if (len > TSDB_TABLE_ID_LEN) return TSDB_CODE_INVALID_TABLE_ID;

  memcpy(pOper->rowData, pTable->info.tableId, len);
  memset(pOper->rowData + len, 0, 1);
  len++;

  memcpy(pOper->rowData + len, (char*)pTable + sizeof(char *), tsChildTableUpdateSize);
  len += tsChildTableUpdateSize;

  if (pTable->info.type != TSDB_CHILD_TABLE) {
    int32_t schemaSize = pTable->numOfColumns * sizeof(SSchema);
    memcpy(pOper->rowData + len, pTable->schema, schemaSize);
    len += schemaSize;

    if (pTable->sqlLen != 0) {
      memcpy(pOper->rowData + len, pTable->sql, pTable->sqlLen);
      len += pTable->sqlLen;
    }
  }

  pOper->rowSize = len;

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionDecode(SSdbOper *pOper) {
  assert(pOper->rowData != NULL);
  SChildTableObj *pTable = calloc(1, sizeof(SChildTableObj));
  if (pTable == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

  int32_t len = strlen(pOper->rowData);
  if (len > TSDB_TABLE_ID_LEN) return TSDB_CODE_INVALID_TABLE_ID;
  pTable->info.tableId = strdup(pOper->rowData);
  len++;

  memcpy((char*)pTable + sizeof(char *), pOper->rowData + len, tsChildTableUpdateSize);
  len += tsChildTableUpdateSize;

  if (pTable->info.type != TSDB_CHILD_TABLE) {
    int32_t schemaSize = pTable->numOfColumns * sizeof(SSchema);
    pTable->schema = (SSchema *)malloc(schemaSize);
    if (pTable->schema == NULL) {
      mgmtDestroyChildTable(pTable);
      return TSDB_CODE_INVALID_TABLE_TYPE;
    }
    memcpy(pTable->schema, pOper->rowData + len, schemaSize);
    len += schemaSize;

    if (pTable->sqlLen != 0) {
      pTable->sql = malloc(pTable->sqlLen);
      if (pTable->sql == NULL) {
        mgmtDestroyChildTable(pTable);
        return TSDB_CODE_SERV_OUT_OF_MEMORY;
      }
      memcpy(pTable->sql, pOper->rowData + len, pTable->sqlLen);
    }
  }

  pOper->pObj = pTable;
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionRestored() {
  void *pIter = NULL;
  SChildTableObj *pTable = NULL;

  while (1) {
    pIter = mgmtGetNextChildTable(pIter, &pTable);
    if (pTable == NULL) break;

    SDbObj *pDb = mgmtGetDbByTableId(pTable->info.tableId);
    if (pDb == NULL) {
      mError("ctable:%s, failed to get db, discard it", pTable->info.tableId);
      SSdbOper desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .table = tsChildTableSdb};
      sdbDeleteRow(&desc);
      mgmtDecTableRef(pTable);
      continue;
    }
    mgmtDecDbRef(pDb);

    SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
    if (pVgroup == NULL) {
      mError("ctable:%s, failed to get vgId:%d sid:%d, discard it", pTable->info.tableId, pTable->vgId, pTable->sid);
      pTable->vgId = 0;
      SSdbOper desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .table = tsChildTableSdb};
      sdbDeleteRow(&desc);
      mgmtDecTableRef(pTable);
      continue;
    }
    mgmtDecVgroupRef(pVgroup);

    if (strcmp(pVgroup->dbName, pDb->name) != 0) {
      mError("ctable:%s, db:%s not match with vgId:%d db:%s sid:%d, discard it",
             pTable->info.tableId, pDb->name, pTable->vgId, pVgroup->dbName, pTable->sid);
      pTable->vgId = 0;
      SSdbOper desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .table = tsChildTableSdb};
      sdbDeleteRow(&desc);
      mgmtDecTableRef(pTable);
      continue;
    }

    if (pVgroup->tableList == NULL) {
      mError("ctable:%s, vgId:%d tableList is null", pTable->info.tableId, pTable->vgId);
      pTable->vgId = 0;
      SSdbOper desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .table = tsChildTableSdb};
      sdbDeleteRow(&desc);
      mgmtDecTableRef(pTable);
      continue;
    }

    if (pTable->info.type == TSDB_CHILD_TABLE) {
      SSuperTableObj *pSuperTable = mgmtGetSuperTableByUid(pTable->suid);
      if (pSuperTable == NULL) {
        mError("ctable:%s, stable:%" PRIu64 " not exist", pTable->info.tableId, pTable->suid);
        pTable->vgId = 0;
        SSdbOper desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .table = tsChildTableSdb};
        sdbDeleteRow(&desc);
        mgmtDecTableRef(pTable);
        continue;
      }
      mgmtDecTableRef(pSuperTable);
    }

    mgmtDecTableRef(pTable);
  }

  sdbFreeIter(pIter);

  return 0;
}

static int32_t mgmtInitChildTables() {
  SChildTableObj tObj;
  tsChildTableUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj.info.type;

  SSdbTableDesc tableDesc = {
    .tableId      = SDB_TABLE_CTABLE,
    .tableName    = "ctables",
    .hashSessions = TSDB_DEFAULT_CTABLES_HASH_SIZE,
    .maxRowSize   = sizeof(SChildTableObj) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16) + TSDB_TABLE_ID_LEN + TSDB_CQ_SQL_SIZE,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_VAR_STRING,
    .insertFp     = mgmtChildTableActionInsert,
    .deleteFp     = mgmtChildTableActionDelete,
    .updateFp     = mgmtChildTableActionUpdate,
    .encodeFp     = mgmtChildTableActionEncode,
    .decodeFp     = mgmtChildTableActionDecode,
    .destroyFp    = mgmtChildTableActionDestroy,
    .restoredFp   = mgmtChildTableActionRestored
  };

  tsChildTableSdb = sdbOpenTable(&tableDesc);
  if (tsChildTableSdb == NULL) {
    mError("failed to init child table data");
    return -1;
  }

  mTrace("table:ctables is created");
  return 0;
}

static void mgmtCleanUpChildTables() {
  sdbCloseTable(tsChildTableSdb);
}

static void mgmtAddTableIntoStable(SSuperTableObj *pStable, SChildTableObj *pCtable) {
  pStable->numOfTables++;

  if (pStable->vgHash == NULL) {
    pStable->vgHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false);
  }

  if (pStable->vgHash != NULL) {
    taosHashPut(pStable->vgHash, (char *)&pCtable->vgId, sizeof(pCtable->vgId), &pCtable->vgId, sizeof(pCtable->vgId));
  }
}

static void mgmtRemoveTableFromStable(SSuperTableObj *pStable, SChildTableObj *pCtable) {
  pStable->numOfTables--;

  if (pStable->vgHash == NULL) return;

  SVgObj *pVgroup = mgmtGetVgroup(pCtable->vgId);
  if (pVgroup == NULL) {
    taosHashRemove(pStable->vgHash, (char *)&pCtable->vgId, sizeof(pCtable->vgId));
  }
  mgmtDecVgroupRef(pVgroup);
}

static void mgmtDestroySuperTable(SSuperTableObj *pStable) {
  if (pStable->vgHash != NULL) {
    taosHashCleanup(pStable->vgHash);
    pStable->vgHash = NULL;
  }
  tfree(pStable->info.tableId);
  tfree(pStable->schema);
  tfree(pStable);
}

static int32_t mgmtSuperTableActionDestroy(SSdbOper *pOper) {
  mgmtDestroySuperTable(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtSuperTableActionInsert(SSdbOper *pOper) {
  SSuperTableObj *pStable = pOper->pObj;
  SDbObj *pDb = mgmtGetDbByTableId(pStable->info.tableId);
  if (pDb != NULL) {
    mgmtAddSuperTableIntoDb(pDb);
  }
  mgmtDecDbRef(pDb);

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtSuperTableActionDelete(SSdbOper *pOper) {
  SSuperTableObj *pStable = pOper->pObj;
  SDbObj *pDb = mgmtGetDbByTableId(pStable->info.tableId);
  if (pDb != NULL) {
    mgmtRemoveSuperTableFromDb(pDb);
    mgmtDropAllChildTablesInStable((SSuperTableObj *)pStable);
  }
  mgmtDecDbRef(pDb);

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtSuperTableActionUpdate(SSdbOper *pOper) {
  SSuperTableObj *pNew = pOper->pObj;
  SSuperTableObj *pTable = mgmtGetSuperTable(pNew->info.tableId);
  if (pTable != pNew) {
    void *oldTableId = pTable->info.tableId;
    void *oldSchema = pTable->schema;
    memcpy(pTable, pNew, pOper->rowSize);
    pTable->schema = pNew->schema;
    free(pNew->vgHash);
    free(pNew);
    free(oldTableId);
    free(oldSchema);
  }
  mgmtDecTableRef(pTable);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtSuperTableActionEncode(SSdbOper *pOper) {
  SSuperTableObj *pStable = pOper->pObj;
  assert(pOper->pObj != NULL && pOper->rowData != NULL);

  int32_t len = strlen(pStable->info.tableId);
  if (len > TSDB_TABLE_ID_LEN) len = TSDB_CODE_INVALID_TABLE_ID;

  memcpy(pOper->rowData, pStable->info.tableId, len);
  memset(pOper->rowData + len, 0, 1);
  len++;

  memcpy(pOper->rowData + len, (char*)pStable + sizeof(char *), tsSuperTableUpdateSize);
  len += tsSuperTableUpdateSize;

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfColumns + pStable->numOfTags);
  memcpy(pOper->rowData + len, pStable->schema, schemaSize);
  len += schemaSize;

  pOper->rowSize = len;

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtSuperTableActionDecode(SSdbOper *pOper) {
  assert(pOper->rowData != NULL);
  SSuperTableObj *pStable = (SSuperTableObj *) calloc(1, sizeof(SSuperTableObj));
  if (pStable == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;

  int32_t len = strlen(pOper->rowData);
  if (len > TSDB_TABLE_ID_LEN) return TSDB_CODE_INVALID_TABLE_ID;
  pStable->info.tableId = strdup(pOper->rowData);
  len++;

  memcpy((char*)pStable + sizeof(char *), pOper->rowData + len, tsSuperTableUpdateSize);
  len += tsSuperTableUpdateSize;

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfColumns + pStable->numOfTags);
  pStable->schema = malloc(schemaSize);
  if (pStable->schema == NULL) {
    mgmtDestroySuperTable(pStable);
    return TSDB_CODE_NOT_SUPER_TABLE;
  }

  memcpy(pStable->schema, pOper->rowData + len, schemaSize);
  
  pOper->pObj = pStable;

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtSuperTableActionRestored() {
  return 0;
}

static int32_t mgmtInitSuperTables() {
  SSuperTableObj tObj;
  tsSuperTableUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj.info.type;

  SSdbTableDesc tableDesc = {
    .tableId      = SDB_TABLE_STABLE,
    .tableName    = "stables",
    .hashSessions = TSDB_DEFAULT_STABLES_HASH_SIZE,
    .maxRowSize   = sizeof(SSuperTableObj) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16) + TSDB_TABLE_ID_LEN,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_VAR_STRING,
    .insertFp     = mgmtSuperTableActionInsert,
    .deleteFp     = mgmtSuperTableActionDelete,
    .updateFp     = mgmtSuperTableActionUpdate,
    .encodeFp     = mgmtSuperTableActionEncode,
    .decodeFp     = mgmtSuperTableActionDecode,
    .destroyFp    = mgmtSuperTableActionDestroy,
    .restoredFp   = mgmtSuperTableActionRestored
  };

  tsSuperTableSdb = sdbOpenTable(&tableDesc);
  if (tsSuperTableSdb == NULL) {
    mError("failed to init stables data");
    return -1;
  }

  mTrace("table:stables is created");
  return 0;
}

static void mgmtCleanUpSuperTables() {
  sdbCloseTable(tsSuperTableSdb);
}

int32_t mgmtInitTables() {
  int32_t code = mgmtInitSuperTables();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = mgmtInitChildTables();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_TABLES_META, mgmtProcessMultiTableMetaMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_CREATE_TABLE, mgmtProcessCreateTableMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_DROP_TABLE, mgmtProcessDropTableMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_ALTER_TABLE, mgmtProcessAlterTableMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_TABLE_META, mgmtProcessTableMetaMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_STABLE_VGROUP, mgmtProcessSuperTableVgroupMsg);
  
  dnodeAddClientRspHandle(TSDB_MSG_TYPE_MD_CREATE_TABLE_RSP, mgmtProcessCreateChildTableRsp);
  dnodeAddClientRspHandle(TSDB_MSG_TYPE_MD_DROP_TABLE_RSP, mgmtProcessDropChildTableRsp);
  dnodeAddClientRspHandle(TSDB_MSG_TYPE_MD_DROP_STABLE_RSP, mgmtProcessDropSuperTableRsp);
  dnodeAddClientRspHandle(TSDB_MSG_TYPE_MD_ALTER_TABLE_RSP, mgmtProcessAlterTableRsp);

  dnodeAddServerMsgHandle(TSDB_MSG_TYPE_DM_CONFIG_TABLE, mgmtProcessTableCfgMsg);

  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_TABLE, mgmtGetShowTableMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_TABLE, mgmtRetrieveShowTables);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_METRIC, mgmtGetShowSuperTableMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_METRIC, mgmtRetrieveShowSuperTables);
  
  return TSDB_CODE_SUCCESS;
}

static void *mgmtGetChildTable(char *tableId) {
  return sdbGetRow(tsChildTableSdb, tableId);
}

static void *mgmtGetSuperTable(char *tableId) {
  return sdbGetRow(tsSuperTableSdb, tableId);
}

static void *mgmtGetSuperTableByUid(uint64_t uid) {
  SSuperTableObj *pStable = NULL;
  void *pIter = NULL;

  while (1) {
    pIter = mgmtGetNextSuperTable(pIter, &pStable);
    if (pStable == NULL) break;
    if (pStable->uid == uid) {
      sdbFreeIter(pIter);
      return pStable;
    }
    mgmtDecTableRef(pStable);
  }

  sdbFreeIter(pIter);

  return NULL;
}

void *mgmtGetTable(char *tableId) {
  void *pTable = mgmtGetSuperTable(tableId);
  if (pTable != NULL) {
    return pTable;
  }

  pTable = mgmtGetChildTable(tableId);
  if (pTable != NULL) {
    return pTable;
  }

  return NULL;
}

void *mgmtGetNextChildTable(void *pIter, SChildTableObj **pTable) {
  return sdbFetchRow(tsChildTableSdb, pIter, (void **)pTable);
}

void *mgmtGetNextSuperTable(void *pIter, SSuperTableObj **pTable) {
  return sdbFetchRow(tsSuperTableSdb, pIter, (void **)pTable);
}

void mgmtIncTableRef(void *p1) {
  STableObj *pTable = (STableObj *)p1;
  if (pTable->type == TSDB_SUPER_TABLE) {
    sdbIncRef(tsSuperTableSdb, pTable);
  } else {
    sdbIncRef(tsChildTableSdb, pTable);
  }
}

void mgmtDecTableRef(void *p1) {
  if (p1 == NULL) return;

  STableObj *pTable = (STableObj *)p1;
  if (pTable->type == TSDB_SUPER_TABLE) {
    sdbDecRef(tsSuperTableSdb, pTable);
  } else {
    sdbDecRef(tsChildTableSdb, pTable);
  }
}

void mgmtCleanUpTables() {
  mgmtCleanUpChildTables();
  mgmtCleanUpSuperTables();
}

// todo move to name.h, add length of table name
static void mgmtExtractTableName(char* tableId, char* name) {
  int pos = -1;
  int num = 0;
  for (pos = 0; tableId[pos] != 0; ++pos) {
    if (tableId[pos] == '.') num++;
    if (num == 2) break;
  }

  if (num == 2) {
    strcpy(name, tableId + pos + 1);
  }
}

static void mgmtProcessCreateTableMsg(SQueuedMsg *pMsg) {
  SCMCreateTableMsg *pCreate = pMsg->pCont;
  
  if (pMsg->pDb == NULL) pMsg->pDb = mgmtGetDb(pCreate->db);
  if (pMsg->pDb == NULL || pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("table:%s, failed to create, db not selected", pCreate->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_DB_NOT_SELECTED);
    return;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mgmtGetTable(pCreate->tableId);
  if (pMsg->pTable != NULL && pMsg->retry == 0) {
    if (pCreate->getMeta) {
      mTrace("table:%s, continue to get meta", pCreate->tableId);
      mgmtGetChildTableMeta(pMsg);
    } else if (pCreate->igExists) {
      mTrace("table:%s, is already exist", pCreate->tableId);
      mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SUCCESS);
    } else {
      mError("table:%s, failed to create, table already exist", pCreate->tableId);
      mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_TABLE_ALREADY_EXIST);
    }
    return;
  }

  if (pCreate->numOfTags != 0) {
    mTrace("table:%s, create msg is received from thandle:%p", pCreate->tableId, pMsg->thandle);
    mgmtProcessCreateSuperTableMsg(pMsg);
  } else {
    mTrace("table:%s, create msg is received from thandle:%p", pCreate->tableId, pMsg->thandle);
    mgmtProcessCreateChildTableMsg(pMsg);
  }
}

static void mgmtProcessDropTableMsg(SQueuedMsg *pMsg) {
  SCMDropTableMsg *pDrop = pMsg->pCont;
  if (pMsg->pDb == NULL) pMsg->pDb = mgmtGetDbByTableId(pDrop->tableId);
  if (pMsg->pDb == NULL || pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("table:%s, failed to drop table, db not selected", pDrop->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_DB_NOT_SELECTED);
    return;
  }

  if (mgmtCheckIsMonitorDB(pMsg->pDb->name, tsMonitorDbName)) {
    mError("table:%s, failed to drop table, in monitor database", pDrop->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_MONITOR_DB_FORBIDDEN);
    return;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mgmtGetTable(pDrop->tableId);
  if (pMsg->pTable == NULL) {
    if (pDrop->igNotExists) {
      mTrace("table:%s, table is not exist, think drop success", pDrop->tableId);
      mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SUCCESS);
      return;
    } else {
      mError("table:%s, failed to drop table, table not exist", pDrop->tableId);
      mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_TABLE);
      return;
    }
  }

  if (pMsg->pTable->type == TSDB_SUPER_TABLE) {
    mPrint("table:%s, start to drop stable", pDrop->tableId);
    mgmtProcessDropSuperTableMsg(pMsg);
  } else {
    mPrint("table:%s, start to drop ctable", pDrop->tableId);
    mgmtProcessDropChildTableMsg(pMsg);
  }
}

static void mgmtProcessTableMetaMsg(SQueuedMsg *pMsg) {
  SCMTableInfoMsg *pInfo = pMsg->pCont;
  pInfo->createFlag = htons(pInfo->createFlag);
  mTrace("table:%s, table meta msg is received from thandle:%p, createFlag:%d", pInfo->tableId, pMsg->thandle, pInfo->createFlag);

  if (pMsg->pDb == NULL) pMsg->pDb = mgmtGetDbByTableId(pInfo->tableId);
  if (pMsg->pDb == NULL || pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("table:%s, failed to get table meta, db not selected", pInfo->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_DB_NOT_SELECTED);
    return;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mgmtGetTable(pInfo->tableId);
  if (pMsg->pTable == NULL) {
    if (!pInfo->createFlag) {
      mError("table:%s, failed to get table meta, table not exist", pInfo->tableId);
      mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_TABLE);
    } else {
      mgmtAutoCreateChildTable(pMsg);
    }
  } else {
    if (pMsg->pTable->type != TSDB_SUPER_TABLE) {
      mgmtGetChildTableMeta(pMsg);
    } else {
      mgmtGetSuperTableMeta(pMsg);
    }
  }
}

static void mgmtProcessCreateSuperTableMsg(SQueuedMsg *pMsg) {
  SCMCreateTableMsg *pCreate = pMsg->pCont;
  SSuperTableObj *pStable = calloc(1, sizeof(SSuperTableObj));
  if (pStable == NULL) {
    mError("table:%s, failed to create, no enough memory", pCreate->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  pStable->info.tableId = strdup(pCreate->tableId);
  pStable->info.type    = TSDB_SUPER_TABLE;
  pStable->createdTime  = taosGetTimestampMs();
  pStable->uid          = (((uint64_t) pStable->createdTime) << 16) + (sdbGetVersion() & ((1ul << 16) - 1ul));
  pStable->sversion     = 0;
  pStable->tversion     = 0;
  pStable->numOfColumns = htons(pCreate->numOfColumns);
  pStable->numOfTags    = htons(pCreate->numOfTags);

  int32_t numOfCols = pStable->numOfColumns + pStable->numOfTags;
  int32_t schemaSize = numOfCols * sizeof(SSchema);
  pStable->schema = (SSchema *)calloc(1, schemaSize);
  if (pStable->schema == NULL) {
    free(pStable);
    mError("table:%s, failed to create, no schema input", pCreate->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_TABLE);
    return;
  }
  memcpy(pStable->schema, pCreate->schema, numOfCols * sizeof(SSchema));

  pStable->nextColId = 0;
  for (int32_t col = 0; col < numOfCols; col++) {
    SSchema *tschema = pStable->schema;
    tschema[col].colId = pStable->nextColId++;
    tschema[col].bytes = htons(tschema[col].bytes);
    
    // todo 1. check the length of each column; 2. check the total length of all columns
    assert(tschema[col].type >= TSDB_DATA_TYPE_BOOL && tschema[col].type <= TSDB_DATA_TYPE_NCHAR);
  }

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable,
    .rowSize = sizeof(SSuperTableObj) + schemaSize
  };

  int32_t code = sdbInsertRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    mgmtDestroySuperTable(pStable);
    mError("table:%s, failed to create, sdb error", pCreate->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SDB_ERROR);
  } else {
    mLPrint("table:%s, is created, tags:%d fields:%d", pStable->info.tableId, pStable->numOfTags, pStable->numOfColumns);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SUCCESS);
  }
}

static void mgmtProcessDropSuperTableMsg(SQueuedMsg *pMsg) {
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  if (pStable->numOfTables != 0) {
    SHashMutableIterator *pIter = taosHashCreateIter(pStable->vgHash);
    while (taosHashIterNext(pIter)) {
      int32_t *pVgId = taosHashIterGet(pIter);
      SVgObj *pVgroup = mgmtGetVgroup(*pVgId);
      if (pVgroup == NULL) break;

      SMDDropSTableMsg *pDrop = rpcMallocCont(sizeof(SMDDropSTableMsg));
      pDrop->contLen = htonl(sizeof(SMDDropSTableMsg));
      pDrop->vgId = htonl(pVgroup->vgId);
      pDrop->uid = htobe64(pStable->uid);
      mgmtExtractTableName(pStable->info.tableId, pDrop->tableId);
        
      mPrint("stable:%s, send drop stable msg to vgId:%d", pStable->info.tableId, pVgroup->vgId);
      SRpcIpSet ipSet = mgmtGetIpSetFromVgroup(pVgroup);
      SRpcMsg rpcMsg = {.pCont = pDrop, .contLen = sizeof(SMDDropSTableMsg), .msgType = TSDB_MSG_TYPE_MD_DROP_STABLE};
      dnodeSendMsgToDnode(&ipSet, &rpcMsg);
      mgmtDecVgroupRef(pVgroup);
    }
    taosHashDestroyIter(pIter);

    mgmtDropAllChildTablesInStable(pStable);
  } 
  
  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable
  };
  
  int32_t code = sdbDeleteRow(&oper);
  mLPrint("stable:%s, is dropped from sdb, result:%s", pStable->info.tableId, tstrerror(code));
  mgmtSendSimpleResp(pMsg->thandle, code);
}

static int32_t mgmtFindSuperTableTagIndex(SSuperTableObj *pStable, const char *tagName) {
  SSchema *schema = (SSchema *) pStable->schema;
  for (int32_t tag = 0; tag < pStable->numOfTags; tag++) {
    if (strcasecmp(schema[pStable->numOfColumns + tag].name, tagName) == 0) {
      return tag;
    }
  }

  return -1;
}

static int32_t mgmtAddSuperTableTag(SSuperTableObj *pStable, SSchema schema[], int32_t ntags) {
  if (pStable->numOfTags + ntags > TSDB_MAX_TAGS) {
    mError("stable:%s, add tag, too many tags", pStable->info.tableId);
    return TSDB_CODE_TOO_MANY_TAGS;
  }

  for (int32_t i = 0; i < ntags; i++) {
    if (mgmtFindSuperTableColumnIndex(pStable, schema[i].name) > 0) {
      mError("stable:%s, add tag, column:%s already exist", pStable->info.tableId, schema[i].name);
      return TSDB_CODE_TAG_ALREAY_EXIST;
    }

    if (mgmtFindSuperTableTagIndex(pStable, schema[i].name) > 0) {
      mError("stable:%s, add tag, tag:%s already exist", pStable->info.tableId, schema[i].name);
      return TSDB_CODE_FIELD_ALREAY_EXIST;
    }
  }

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfTags + pStable->numOfColumns);
  pStable->schema = realloc(pStable->schema, schemaSize + sizeof(SSchema) * ntags);

  memcpy(pStable->schema + pStable->numOfColumns + pStable->numOfTags, schema, sizeof(SSchema) * ntags);

  SSchema *tschema = (SSchema *)(pStable->schema + pStable->numOfColumns + pStable->numOfTags);
  for (int32_t i = 0; i < ntags; i++) {
    tschema[i].colId = pStable->nextColId++;
  }

  pStable->numOfTags += ntags;
  pStable->tversion++;

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_SDB_ERROR;
  }

  mPrint("stable %s, succeed to add tag %s", pStable->info.tableId, schema[0].name);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtDropSuperTableTag(SSuperTableObj *pStable, char *tagName) {
  int32_t col = mgmtFindSuperTableTagIndex(pStable, tagName);
  if (col < 0) {
    mError("stable:%s, drop tag, tag:%s not exist", pStable->info.tableId, tagName);
    return TSDB_CODE_TAG_NOT_EXIST;
  }

  memmove(pStable->schema + pStable->numOfColumns + col, pStable->schema + pStable->numOfColumns + col + 1,
          sizeof(SSchema) * (pStable->numOfTags - col - 1));
  pStable->numOfTags--;
  pStable->tversion++;

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_SDB_ERROR;
  }
  
  mPrint("stable %s, succeed to drop tag %s", pStable->info.tableId, tagName);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtModifySuperTableTagName(SSuperTableObj *pStable, char *oldTagName, char *newTagName) {
  int32_t col = mgmtFindSuperTableTagIndex(pStable, oldTagName);
  if (col < 0) {
    mError("stable:%s, failed to modify table tag, oldName: %s, newName: %s", pStable->info.tableId, oldTagName, newTagName);
    return TSDB_CODE_TAG_NOT_EXIST;
  }

  // int32_t  rowSize = 0;
  uint32_t len = strlen(newTagName);
  if (len >= TSDB_COL_NAME_LEN) {
    return TSDB_CODE_COL_NAME_TOO_LONG;
  }

  if (mgmtFindSuperTableTagIndex(pStable, newTagName) >= 0) {
    return TSDB_CODE_TAG_ALREAY_EXIST;
  }

  // update
  SSchema *schema = (SSchema *) (pStable->schema + pStable->numOfColumns + col);
  strncpy(schema->name, newTagName, TSDB_COL_NAME_LEN);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_SDB_ERROR;
  }
  
  mPrint("stable %s, succeed to modify tag %s to %s", pStable->info.tableId, oldTagName, newTagName);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtFindSuperTableColumnIndex(SSuperTableObj *pStable, char *colName) {
  SSchema *schema = (SSchema *) pStable->schema;
  for (int32_t col = 0; col < pStable->numOfColumns; col++) {
    if (strcasecmp(schema[col].name, colName) == 0) {
      return col;
    }
  }

  return -1;
}

static int32_t mgmtAddSuperTableColumn(SDbObj *pDb, SSuperTableObj *pStable, SSchema schema[], int32_t ncols) {
  if (ncols <= 0) {
    mError("stable:%s, add column, ncols:%d <= 0", pStable->info.tableId);
    return TSDB_CODE_APP_ERROR;
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (mgmtFindSuperTableColumnIndex(pStable, schema[i].name) > 0) {
      mError("stable:%s, add column, column:%s already exist", pStable->info.tableId, schema[i].name);
      return TSDB_CODE_FIELD_ALREAY_EXIST;
    }

    if (mgmtFindSuperTableTagIndex(pStable, schema[i].name) > 0) {
      mError("stable:%s, add column, tag:%s already exist", pStable->info.tableId, schema[i].name);
      return TSDB_CODE_TAG_ALREAY_EXIST;
    }
  }

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfTags + pStable->numOfColumns);
  pStable->schema = realloc(pStable->schema, schemaSize + sizeof(SSchema) * ncols);

  memmove(pStable->schema + pStable->numOfColumns + ncols, pStable->schema + pStable->numOfColumns,
          sizeof(SSchema) * pStable->numOfTags);
  memcpy(pStable->schema + pStable->numOfColumns, schema, sizeof(SSchema) * ncols);

  SSchema *tschema = (SSchema *) (pStable->schema + sizeof(SSchema) * pStable->numOfColumns);
  for (int32_t i = 0; i < ncols; i++) {
    tschema[i].colId = pStable->nextColId++;
  }

  pStable->numOfColumns += ncols;
  pStable->sversion++;

  SAcctObj *pAcct = mgmtGetAcct(pDb->acct);
  if (pAcct != NULL) {
    pAcct->acctInfo.numOfTimeSeries += (ncols * pStable->numOfTables);
    mgmtDecAcctRef(pAcct);
  }

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_SDB_ERROR;
  }
  
  mPrint("stable %s, succeed to add column", pStable->info.tableId);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtDropSuperTableColumn(SDbObj *pDb, SSuperTableObj *pStable, char *colName) {
  int32_t col = mgmtFindSuperTableColumnIndex(pStable, colName);
  if (col <= 0) {
    mError("stable:%s, drop column, column:%s not exist", pStable->info.tableId, colName);
    return TSDB_CODE_FIELD_NOT_EXIST;
  }

  memmove(pStable->schema + col, pStable->schema + col + 1,
          sizeof(SSchema) * (pStable->numOfColumns + pStable->numOfTags - col - 1));

  pStable->numOfColumns--;
  pStable->sversion++;

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfTags + pStable->numOfColumns);
  pStable->schema = realloc(pStable->schema, schemaSize);

  SAcctObj *pAcct = mgmtGetAcct(pDb->acct);
  if (pAcct != NULL) {
    pAcct->acctInfo.numOfTimeSeries -= pStable->numOfTables;
    mgmtDecAcctRef(pAcct);
  }

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_SDB_ERROR;
  }
  
  mPrint("stable %s, succeed to delete column", pStable->info.tableId);
  return TSDB_CODE_SUCCESS;
}

// show super tables
static int32_t mgmtGetShowSuperTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mgmtGetDb(pShow->db);
  if (pDb == NULL) return TSDB_CODE_DB_NOT_SELECTED;

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
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

  mgmtDecDbRef(pDb);
  return 0;
}

// retrieve super tables
int32_t mgmtRetrieveShowSuperTables(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t         numOfRows = 0;
  char *          pWrite;
  int32_t         cols = 0;
  SSuperTableObj *pTable = NULL;
  char            prefix[20] = {0};
  int32_t         prefixLen;

  SDbObj *pDb = mgmtGetDb(pShow->db);
  if (pDb == NULL) return 0;

  strcpy(prefix, pDb->name);
  strcat(prefix, TS_PATH_DELIMITER);
  prefixLen = strlen(prefix);

  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;
  char stableName[TSDB_TABLE_NAME_LEN + 1] = {0};

  while (numOfRows < rows) {    
    pShow->pIter = mgmtGetNextSuperTable(pShow->pIter, &pTable);
    if (pTable == NULL) break;
    if (strncmp(pTable->info.tableId, prefix, prefixLen)) {
      mgmtDecTableRef(pTable);
      continue;
    }

    memset(stableName, 0, tListLen(stableName));
    mgmtExtractTableName(pTable->info.tableId, stableName);

    if (pShow->payloadLen > 0 && patternMatch(pShow->payload, stableName, TSDB_TABLE_NAME_LEN, &info) != TSDB_PATTERN_MATCH) {
      mgmtDecTableRef(pTable);
      continue;
    }

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
  
    int16_t len = strnlen(stableName, TSDB_DB_NAME_LEN);
    *(int16_t*) pWrite = len;
    pWrite += sizeof(int16_t); // todo refactor
  
    strncpy(pWrite, stableName, len);
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
    mgmtDecTableRef(pTable);
  }

  pShow->numOfReads += numOfRows;
  mgmtDecDbRef(pDb);

  return numOfRows;
}

void mgmtDropAllSuperTables(SDbObj *pDropDb) {
  void *  pIter= NULL;
  int32_t numOfTables = 0;
  int32_t dbNameLen = strlen(pDropDb->name);
  SSuperTableObj *pTable = NULL;

  mPrint("db:%s, all super tables will be dropped from sdb", pDropDb->name);

  while (1) {
    pIter = mgmtGetNextSuperTable(pIter, &pTable);
    if (pTable == NULL) break;

    if (strncmp(pDropDb->name, pTable->info.tableId, dbNameLen) == 0) {
      SSdbOper oper = {
        .type = SDB_OPER_LOCAL,
        .table = tsSuperTableSdb,
        .pObj = pTable,
      };
      sdbDeleteRow(&oper);
      numOfTables ++;
    }

    mgmtDecTableRef(pTable);
  }

  sdbFreeIter(pIter);

  mPrint("db:%s, all super tables:%d is dropped from sdb", pDropDb->name, numOfTables);
}

static int32_t mgmtSetSchemaFromSuperTable(SSchema *pSchema, SSuperTableObj *pTable) {
  int32_t numOfCols = pTable->numOfColumns + pTable->numOfTags;
  assert(numOfCols <= TSDB_MAX_COLUMNS);
  
  for (int32_t i = 0; i < numOfCols; ++i) {
    strncpy(pSchema->name, pTable->schema[i].name, TSDB_COL_NAME_LEN);
    pSchema->type  = pTable->schema[i].type;
    pSchema->bytes = htons(pTable->schema[i].bytes);
    pSchema->colId = htons(pTable->schema[i].colId);
    pSchema++;
  }

  return (pTable->numOfColumns + pTable->numOfTags) * sizeof(SSchema);
}

static void mgmtGetSuperTableMeta(SQueuedMsg *pMsg) {
  SSuperTableObj *pTable = (SSuperTableObj *)pMsg->pTable;
  STableMetaMsg *pMeta   = rpcMallocCont(sizeof(STableMetaMsg) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16));
  pMeta->uid          = htobe64(pTable->uid);
  pMeta->sversion     = htons(pTable->sversion);
  pMeta->tversion     = htons(pTable->tversion);
  pMeta->precision    = pMsg->pDb->cfg.precision;
  pMeta->numOfTags    = (uint8_t)pTable->numOfTags;
  pMeta->numOfColumns = htons((int16_t)pTable->numOfColumns);
  pMeta->tableType    = pTable->info.type;
  pMeta->contLen      = sizeof(STableMetaMsg) + mgmtSetSchemaFromSuperTable(pMeta->schema, pTable);
  strncpy(pMeta->tableId, pTable->info.tableId, TSDB_TABLE_ID_LEN);

  SRpcMsg rpcRsp = {
    .handle = pMsg->thandle, 
    .pCont = pMeta, 
    .contLen = pMeta->contLen,
  };
  pMeta->contLen = htons(pMeta->contLen);
  rpcSendResponse(&rpcRsp);

  mTrace("stable:%s, uid:%" PRIu64 " table meta is retrieved", pTable->info.tableId, pTable->uid);
}

static void mgmtProcessSuperTableVgroupMsg(SQueuedMsg *pMsg) {
  SCMSTableVgroupMsg *pInfo = pMsg->pCont;
  int32_t numOfTable = htonl(pInfo->numOfTables);

  // reserve space
  int32_t contLen = sizeof(SCMSTableVgroupRspMsg) + 32 * sizeof(SCMVgroupInfo) + sizeof(SVgroupsInfo); 
  for (int32_t i = 0; i < numOfTable; ++i) {
    char *stableName = (char*)pInfo + sizeof(SCMSTableVgroupMsg) + (TSDB_TABLE_ID_LEN) * i;
    SSuperTableObj *pTable = mgmtGetSuperTable(stableName);
    if (pTable->vgHash != NULL) {
      contLen += (taosHashGetSize(pTable->vgHash) * sizeof(SCMVgroupInfo) + sizeof(SVgroupsInfo));
    }
    mgmtDecTableRef(pTable);
  }

  SCMSTableVgroupRspMsg *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  pRsp->numOfTables = htonl(numOfTable);
  char* msg = (char*) pRsp + sizeof(SCMSTableVgroupRspMsg);

  for (int32_t i = 0; i < numOfTable; ++i) {
    char *stableName = (char*)pInfo + sizeof(SCMSTableVgroupMsg) + (TSDB_TABLE_ID_LEN) * i;
    SSuperTableObj *pTable = mgmtGetSuperTable(stableName);
    SVgroupsInfo *pVgroupInfo = (SVgroupsInfo *)msg;

    SHashMutableIterator *pIter = taosHashCreateIter(pTable->vgHash);
    int32_t vgSize = 0;
    while (taosHashIterNext(pIter)) {
      int32_t *pVgId = taosHashIterGet(pIter);
      SVgObj * pVgroup = mgmtGetVgroup(*pVgId);
      if (pVgroup == NULL) continue;

      pVgroupInfo->vgroups[vgSize].vgId = htonl(pVgroup->vgId);
      for (int32_t vn = 0; vn < pVgroup->numOfVnodes; ++vn) {
        SDnodeObj *pDnode = pVgroup->vnodeGid[vn].pDnode;
        if (pDnode == NULL) break;

        strncpy(pVgroupInfo->vgroups[vgSize].ipAddr[vn].fqdn, pDnode->dnodeFqdn, tListLen(pDnode->dnodeFqdn));
        pVgroupInfo->vgroups[vgSize].ipAddr[vn].port = htons(pDnode->dnodePort);

        pVgroupInfo->vgroups[vgSize].numOfIps++;
      }

      vgSize++;
      mgmtDecVgroupRef(pVgroup);
    }

    taosHashDestroyIter(pIter);

    pVgroupInfo->numOfVgroups = htonl(vgSize);

    // one table is done, try the next table
    msg += sizeof(SVgroupsInfo) + vgSize * sizeof(SCMVgroupInfo);
  }

  SRpcMsg rpcRsp = {0};
  rpcRsp.handle = pMsg->thandle;
  rpcRsp.pCont = pRsp;
  rpcRsp.contLen = msg - (char*) pRsp;
  rpcSendResponse(&rpcRsp);
}

static void mgmtProcessDropSuperTableRsp(SRpcMsg *rpcMsg) {
  mPrint("drop stable rsp received, result:%s", tstrerror(rpcMsg->code));
}

static void *mgmtBuildCreateChildTableMsg(SCMCreateTableMsg *pMsg, SChildTableObj *pTable) {
  STagData *  pTagData = NULL;
  int32_t tagDataLen = 0;
  int32_t totalCols = 0;
  int32_t contLen = 0;
  if (pTable->info.type == TSDB_CHILD_TABLE && pMsg != NULL) {
    pTagData = (STagData*)pMsg->schema;
    tagDataLen = ntohl(pTagData->dataLen);
    totalCols = pTable->superTable->numOfColumns + pTable->superTable->numOfTags;
    contLen = sizeof(SMDCreateTableMsg) + totalCols * sizeof(SSchema) + tagDataLen + pTable->sqlLen;
  } else {
    totalCols = pTable->numOfColumns;
    contLen = sizeof(SMDCreateTableMsg) + totalCols * sizeof(SSchema) + pTable->sqlLen;
  }

  SMDCreateTableMsg *pCreate = rpcMallocCont(contLen);
  if (pCreate == NULL) {
    terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
    return NULL;
  }

  mgmtExtractTableName(pTable->info.tableId, pCreate->tableId);
  pCreate->contLen       = htonl(contLen);
  pCreate->vgId          = htonl(pTable->vgId);
  pCreate->tableType     = pTable->info.type;
  pCreate->createdTime   = htobe64(pTable->createdTime);
  pCreate->sid           = htonl(pTable->sid);
  pCreate->sqlDataLen    = htonl(pTable->sqlLen);
  pCreate->uid           = htobe64(pTable->uid);
  
  if (pTable->info.type == TSDB_CHILD_TABLE) {
    mgmtExtractTableName(pTable->superTable->info.tableId, pCreate->superTableId);
    pCreate->numOfColumns  = htons(pTable->superTable->numOfColumns);
    pCreate->numOfTags     = htons(pTable->superTable->numOfTags);
    pCreate->sversion      = htonl(pTable->superTable->sversion);
    pCreate->tversion      = htonl(pTable->superTable->tversion);
    pCreate->tagDataLen    = htonl(tagDataLen);
    pCreate->superTableUid = htobe64(pTable->superTable->uid);
  } else {
    pCreate->numOfColumns  = htons(pTable->numOfColumns);
    pCreate->numOfTags     = 0;
    pCreate->sversion      = htonl(pTable->sversion);
    pCreate->tversion      = 0;
    pCreate->tagDataLen    = 0;
    pCreate->superTableUid = 0;
  }
 
  SSchema *pSchema = (SSchema *) pCreate->data;
  if (pTable->info.type == TSDB_CHILD_TABLE) {
    memcpy(pSchema, pTable->superTable->schema, totalCols * sizeof(SSchema));
  } else {
    memcpy(pSchema, pTable->schema, totalCols * sizeof(SSchema));
  }
  for (int32_t col = 0; col < totalCols; ++col) {
    pSchema->bytes = htons(pSchema->bytes);
    pSchema->colId = htons(pSchema->colId);
    pSchema++;
  }

  if (pTable->info.type == TSDB_CHILD_TABLE && pMsg != NULL) {
    memcpy(pCreate->data + totalCols * sizeof(SSchema), pTagData->data, tagDataLen);
    memcpy(pCreate->data + totalCols * sizeof(SSchema) + tagDataLen, pTable->sql, pTable->sqlLen);
  }

  return pCreate;
}

static SChildTableObj* mgmtDoCreateChildTable(SCMCreateTableMsg *pCreate, SVgObj *pVgroup, int32_t tid) {
  SChildTableObj *pTable = calloc(1, sizeof(SChildTableObj));
  if (pTable == NULL) {
    mError("table:%s, failed to alloc memory", pCreate->tableId);
    terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
    return NULL;
  }

  if (pCreate->numOfColumns == 0) {
    pTable->info.type = TSDB_CHILD_TABLE;
  } else {
    pTable->info.type = TSDB_NORMAL_TABLE;
  }

  pTable->info.tableId = strdup(pCreate->tableId);  
  pTable->createdTime  = taosGetTimestampMs();
  pTable->sid          = tid;
  pTable->vgId         = pVgroup->vgId;
    
  if (pTable->info.type == TSDB_CHILD_TABLE) {
    STagData *pTagData = (STagData *) pCreate->schema;  // it is a tag key
    SSuperTableObj *pSuperTable = mgmtGetSuperTable(pTagData->name);
    if (pSuperTable == NULL) {
      mError("table:%s, corresponding super table:%s does not exist", pCreate->tableId, pTagData->name);
      free(pTable);
      terrno = TSDB_CODE_INVALID_TABLE;
      return NULL;
    }
    mgmtDecTableRef(pSuperTable);

    pTable->suid = pSuperTable->uid;
    pTable->uid  = (((uint64_t)pTable->vgId) << 40) + ((((uint64_t)pTable->sid) & ((1ul << 24) - 1ul)) << 16) +
                  (sdbGetVersion() & ((1ul << 16) - 1ul));
    pTable->superTable = pSuperTable;
  } else {
    pTable->uid          = (((uint64_t) pTable->createdTime) << 16) + (sdbGetVersion() & ((1ul << 16) - 1ul));
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
      pTable->info.type = TSDB_STREAM_TABLE;
      pTable->sql = calloc(1, pTable->sqlLen);
      if (pTable->sql == NULL) {
        free(pTable);
        terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
        return NULL;
      }
      memcpy(pTable->sql, (char *) (pCreate->schema) + numOfCols * sizeof(SSchema), pTable->sqlLen);
      pTable->sql[pTable->sqlLen - 1] = 0;
      mTrace("table:%s, stream sql len:%d sql:%s", pTable->info.tableId, pTable->sqlLen, pTable->sql);
    }
  }
  
  SSdbOper desc = {0};
  desc.type = SDB_OPER_GLOBAL;
  desc.pObj = pTable;
  desc.table = tsChildTableSdb;
  
  if (sdbInsertRow(&desc) != TSDB_CODE_SUCCESS) {
    free(pTable);
    mError("table:%s, update sdb error", pCreate->tableId);
    terrno = TSDB_CODE_SDB_ERROR;
    return NULL;
  }

  mTrace("table:%s, create table in vgroup:%d, id:%d, uid:%" PRIu64 , pTable->info.tableId, pVgroup->vgId, pTable->sid, pTable->uid);
  return pTable;
}

static void mgmtProcessCreateChildTableMsg(SQueuedMsg *pMsg) {
  SCMCreateTableMsg *pCreate = pMsg->pCont;
  int32_t code = grantCheck(TSDB_GRANT_TIMESERIES);
  if (code != TSDB_CODE_SUCCESS) {
    mError("table:%s, failed to create, grant timeseries failed", pCreate->tableId);
    mgmtSendSimpleResp(pMsg->thandle, code);
    return;
  }

  SVgObj *pVgroup = mgmtGetAvailableVgroup(pMsg->pDb);
  if (pVgroup == NULL) {
    mTrace("table:%s, start to create a new vgroup", pCreate->tableId);
    mgmtCreateVgroup(mgmtCloneQueuedMsg(pMsg), pMsg->pDb);
    return;
  }

  if (pMsg->retry == 0) {
    if (pMsg->pTable == NULL) {
      int32_t sid = taosAllocateId(pVgroup->idPool);
      if (sid <= 0) {
        mTrace("tables:%s, no enough sid in vgId:%d", pCreate->tableId, pVgroup->vgId);
        mgmtCreateVgroup(mgmtCloneQueuedMsg(pMsg), pMsg->pDb);
        return;
      }

      pMsg->pTable = (STableObj *)mgmtDoCreateChildTable(pCreate, pVgroup, sid);
      if (pMsg->pTable == NULL) {
        mgmtSendSimpleResp(pMsg->thandle, terrno);
        return;
      }

      mgmtIncTableRef(pMsg->pTable);
    }
  } else {
    if (pMsg->pTable == NULL) pMsg->pTable = mgmtGetTable(pCreate->tableId);
  }

  if (pMsg->pTable == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, terrno);
    return;
  }

  SMDCreateTableMsg *pMDCreate = mgmtBuildCreateChildTableMsg(pCreate, (SChildTableObj *) pMsg->pTable);
  if (pMDCreate == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, terrno);
    return;
  }

  SRpcIpSet ipSet = mgmtGetIpSetFromVgroup(pVgroup);
  SQueuedMsg *newMsg = mgmtCloneQueuedMsg(pMsg);
  newMsg->ahandle = pMsg->pTable;
  newMsg->maxRetry = 10;
  SRpcMsg rpcMsg = {
      .handle  = newMsg,
      .pCont   = pMDCreate,
      .contLen = htonl(pMDCreate->contLen),
      .code    = 0,
      .msgType = TSDB_MSG_TYPE_MD_CREATE_TABLE
  };

  dnodeSendMsgToDnode(&ipSet, &rpcMsg);
}

static void mgmtProcessDropChildTableMsg(SQueuedMsg *pMsg) {
  SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;
  if (pMsg->pVgroup == NULL) pMsg->pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pMsg->pVgroup == NULL) {
    mError("table:%s, failed to drop ctable, vgroup not exist", pTable->info.tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_OTHERS);
    return;
  }

  SMDDropTableMsg *pDrop = rpcMallocCont(sizeof(SMDDropTableMsg));
  if (pDrop == NULL) {
    mError("table:%s, failed to drop ctable, no enough memory", pTable->info.tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  strcpy(pDrop->tableId, pTable->info.tableId);
  pDrop->vgId    = htonl(pTable->vgId);
  pDrop->contLen = htonl(sizeof(SMDDropTableMsg));
  pDrop->sid     = htonl(pTable->sid);
  pDrop->uid     = htobe64(pTable->uid);

  SRpcIpSet ipSet = mgmtGetIpSetFromVgroup(pMsg->pVgroup);

  mPrint("table:%s, send drop ctable msg", pDrop->tableId);
  SQueuedMsg *newMsg = mgmtCloneQueuedMsg(pMsg);
  newMsg->ahandle = pMsg->pTable;
  SRpcMsg rpcMsg = {
    .handle  = newMsg,
    .pCont   = pDrop,
    .contLen = sizeof(SMDDropTableMsg),
    .code    = 0,
    .msgType = TSDB_MSG_TYPE_MD_DROP_TABLE
  };

  dnodeSendMsgToDnode(&ipSet, &rpcMsg);
}

static int32_t mgmtModifyChildTableTagValue(SChildTableObj *pTable, char *tagName, char *nContent) {
  return TSDB_CODE_OPS_NOT_SUPPORT;
}

static int32_t mgmtFindNormalTableColumnIndex(SChildTableObj *pTable, char *colName) {
  SSchema *schema = (SSchema *) pTable->schema;
  for (int32_t col = 0; col < pTable->numOfColumns; col++) {
    if (strcasecmp(schema[col].name, colName) == 0) {
      return col;
    }
  }

  return -1;
}

static int32_t mgmtAddNormalTableColumn(SDbObj *pDb, SChildTableObj *pTable, SSchema schema[], int32_t ncols) {
  if (ncols <= 0) {
    mError("table:%s, add column, ncols:%d <= 0", pTable->info.tableId);
    return TSDB_CODE_APP_ERROR;
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (mgmtFindNormalTableColumnIndex(pTable, schema[i].name) > 0) {
      mError("table:%s, add column, column:%s already exist", pTable->info.tableId, schema[i].name);
      return TSDB_CODE_FIELD_ALREAY_EXIST;
    }
  }

  int32_t schemaSize = pTable->numOfColumns * sizeof(SSchema);
  pTable->schema = realloc(pTable->schema, schemaSize + sizeof(SSchema) * ncols);

  memcpy(pTable->schema + pTable->numOfColumns, schema, sizeof(SSchema) * ncols);

  SSchema *tschema = (SSchema *) (pTable->schema + pTable->numOfColumns);
  for (int32_t i = 0; i < ncols; i++) {
    tschema[i].colId = pTable->nextColId++;
  }

  pTable->numOfColumns += ncols;
  pTable->sversion++;
  
  SAcctObj *pAcct = mgmtGetAcct(pDb->acct);
  if (pAcct != NULL) {
    pAcct->acctInfo.numOfTimeSeries += ncols;
    mgmtDecAcctRef(pAcct);
  }
 
  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsChildTableSdb,
    .pObj = pTable
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_SDB_ERROR;
  }
  
  mPrint("table %s, succeed to add column", pTable->info.tableId);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtDropNormalTableColumn(SDbObj *pDb, SChildTableObj *pTable, char *colName) {
  int32_t col = mgmtFindNormalTableColumnIndex(pTable, colName);
  if (col <= 0) {
    mError("table:%s, drop column, column:%s not exist", pTable->info.tableId, colName);
    return TSDB_CODE_FIELD_NOT_EXIST;
  }

  memmove(pTable->schema + col, pTable->schema + col + 1, sizeof(SSchema) * (pTable->numOfColumns - col - 1));
  pTable->numOfColumns--;
  pTable->sversion++;

  SAcctObj *pAcct = mgmtGetAcct(pDb->acct);
  if (pAcct != NULL) {
    pAcct->acctInfo.numOfTimeSeries--;
    mgmtDecAcctRef(pAcct);
  }
 
  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsChildTableSdb,
    .pObj = pTable
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_SDB_ERROR;
  }
  
  mPrint("table %s, succeed to drop column %s", pTable->info.tableId, colName);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtSetSchemaFromNormalTable(SSchema *pSchema, SChildTableObj *pTable) {
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

static int32_t mgmtDoGetChildTableMeta(SQueuedMsg *pMsg, STableMetaMsg *pMeta) {
  SDbObj *pDb = pMsg->pDb;
  SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;

  pMeta->uid       = htobe64(pTable->uid);
  pMeta->sid       = htonl(pTable->sid);
  pMeta->precision = pDb->cfg.precision;
  pMeta->tableType = pTable->info.type;
  strncpy(pMeta->tableId, pTable->info.tableId, strlen(pTable->info.tableId));

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    pMeta->sversion     = htons(pTable->superTable->sversion);
    pMeta->tversion     = htons(pTable->superTable->tversion);
    pMeta->numOfTags    = (int8_t)pTable->superTable->numOfTags;
    pMeta->numOfColumns = htons((int16_t)pTable->superTable->numOfColumns);
    pMeta->contLen      = sizeof(STableMetaMsg) + mgmtSetSchemaFromSuperTable(pMeta->schema, pTable->superTable);
  } else {
    pMeta->sversion     = htons(pTable->sversion);
    pMeta->tversion     = 0;
    pMeta->numOfTags    = 0;
    pMeta->numOfColumns = htons((int16_t)pTable->numOfColumns);
    pMeta->contLen      = sizeof(STableMetaMsg) + mgmtSetSchemaFromNormalTable(pMeta->schema, pTable); 
  }
  
  if (pMsg->pVgroup == NULL) pMsg->pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pMsg->pVgroup == NULL) {
    mError("table:%s, failed to get table meta, db not selected", pTable->info.tableId);
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  for (int32_t i = 0; i < pMsg->pVgroup->numOfVnodes; ++i) {
    SDnodeObj *pDnode = mgmtGetDnode(pMsg->pVgroup->vnodeGid[i].dnodeId);
    if (pDnode == NULL) break;
    strcpy(pMeta->vgroup.ipAddr[i].fqdn, pDnode->dnodeFqdn);
    pMeta->vgroup.ipAddr[i].port = htons(pDnode->dnodePort + TSDB_PORT_DNODESHELL);
    pMeta->vgroup.numOfIps++;
    mgmtDecDnodeRef(pDnode);
  }
  pMeta->vgroup.vgId = htonl(pMsg->pVgroup->vgId);

  mTrace("table:%s, uid:%" PRIu64 " table meta is retrieved", pTable->info.tableId, pTable->uid);

  return TSDB_CODE_SUCCESS;
}

static void mgmtAutoCreateChildTable(SQueuedMsg *pMsg) {
  SCMTableInfoMsg *pInfo = pMsg->pCont;
  STagData* pTag = (STagData*)pInfo->tags;

  int32_t contLen = sizeof(SCMCreateTableMsg) + offsetof(STagData, data) + ntohl(pTag->dataLen);
  SCMCreateTableMsg *pCreateMsg = rpcMallocCont(contLen);
  if (pCreateMsg == NULL) {
    mError("table:%s, failed to create table while get meta info, no enough memory", pInfo->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  strncpy(pCreateMsg->tableId, pInfo->tableId, tListLen(pInfo->tableId));
  strcpy(pCreateMsg->db, pMsg->pDb->name);
  pCreateMsg->igExists = 1;
  pCreateMsg->getMeta = 1;
  pCreateMsg->contLen = htonl(contLen);

  memcpy(pCreateMsg->schema, pInfo->tags, contLen - sizeof(SCMCreateTableMsg));

  SQueuedMsg *newMsg = mgmtCloneQueuedMsg(pMsg);
  newMsg->msgType = TSDB_MSG_TYPE_CM_CREATE_TABLE;
  newMsg->pCont = pCreateMsg;

  mTrace("table:%s, start to create on demand, stable:%s", pInfo->tableId, pInfo->tags);
  mgmtAddToShellQueue(newMsg);
}

static void mgmtGetChildTableMeta(SQueuedMsg *pMsg) {
  STableMetaMsg *pMeta = rpcMallocCont(sizeof(STableMetaMsg) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16));
  if (pMeta == NULL) {
    mError("table:%s, failed to get table meta, no enough memory", pMsg->pTable->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  mgmtDoGetChildTableMeta(pMsg, pMeta);

  SRpcMsg rpcRsp = {
    .handle = pMsg->thandle, 
    .pCont = pMeta, 
    .contLen = pMeta->contLen,
  };
  pMeta->contLen = htons(pMeta->contLen);
  rpcSendResponse(&rpcRsp);
}

void mgmtDropAllChildTablesInVgroups(SVgObj *pVgroup) {
  void *  pIter = NULL;
  int32_t numOfTables = 0;
  SChildTableObj *pTable = NULL;

  mPrint("vgId:%d, all child tables will be dropped from sdb", pVgroup->vgId);

  while (1) {
    pIter = mgmtGetNextChildTable(pIter, &pTable);
    if (pTable == NULL) break;

    if (pTable->vgId == pVgroup->vgId) {
      SSdbOper oper = {
        .type = SDB_OPER_LOCAL,
        .table = tsChildTableSdb,
        .pObj = pTable,
      };
      sdbDeleteRow(&oper);
      numOfTables++;
    }
    mgmtDecTableRef(pTable);
  }

  sdbFreeIter(pIter);

  mPrint("vgId:%d, all child tables is dropped from sdb", pVgroup->vgId);
}

void mgmtDropAllChildTables(SDbObj *pDropDb) {
  void *  pIter = NULL;
  int32_t numOfTables = 0;
  int32_t dbNameLen = strlen(pDropDb->name);
  SChildTableObj *pTable = NULL;

  mPrint("db:%s, all child tables will be dropped from sdb", pDropDb->name);

  while (1) {
    pIter = mgmtGetNextChildTable(pIter, &pTable);
    if (pTable == NULL) break;

    if (strncmp(pDropDb->name, pTable->info.tableId, dbNameLen) == 0) {
      SSdbOper oper = {
        .type = SDB_OPER_LOCAL,
        .table = tsChildTableSdb,
        .pObj = pTable,
      };
      sdbDeleteRow(&oper);
      numOfTables++;
    }
    mgmtDecTableRef(pTable);
  }

  sdbFreeIter(pIter);

  mPrint("db:%s, all child tables:%d is dropped from sdb", pDropDb->name, numOfTables);
}

static void mgmtDropAllChildTablesInStable(SSuperTableObj *pStable) {
  void *  pIter = NULL;
  int32_t numOfTables = 0;
  SChildTableObj *pTable = NULL;

  mPrint("stable:%s, all child tables will dropped from sdb", pStable->info.tableId, numOfTables);

  while (1) {
    pIter = mgmtGetNextChildTable(pIter, &pTable);
    if (pTable == NULL) break;

    if (pTable->superTable == pStable) {
      SSdbOper oper = {
        .type = SDB_OPER_LOCAL,
        .table = tsChildTableSdb,
        .pObj = pTable,
      };
      sdbDeleteRow(&oper);
      numOfTables++;
    }

    mgmtDecTableRef(pTable);
  }

  sdbFreeIter(pIter);

  mPrint("stable:%s, all child tables:%d is dropped from sdb", pStable->info.tableId, numOfTables);
}

static SChildTableObj* mgmtGetTableByPos(int32_t vnode, int32_t sid) {
  SVgObj *pVgroup = mgmtGetVgroup(vnode);
  if (pVgroup == NULL) return NULL;

  SChildTableObj *pTable = pVgroup->tableList[sid - 1];
  mgmtIncTableRef((STableObj *)pTable);

  mgmtDecVgroupRef(pVgroup);
  return pTable;
}

static void mgmtProcessTableCfgMsg(SRpcMsg *rpcMsg) {
  SDMConfigTableMsg *pCfg = (SDMConfigTableMsg *) rpcMsg->pCont;
  pCfg->dnode = htonl(pCfg->dnode);
  pCfg->vnode = htonl(pCfg->vnode);
  pCfg->sid   = htonl(pCfg->sid);
  mTrace("dnode:%s, vnode:%d, sid:%d, receive table config msg", taosIpStr(pCfg->dnode), pCfg->vnode, pCfg->sid);

  SChildTableObj *pTable = mgmtGetTableByPos(pCfg->vnode, pCfg->sid);
  if (pTable == NULL) {
    mError("dnode:%s, vnode:%d, sid:%d, table not found", taosIpStr(pCfg->dnode), pCfg->vnode, pCfg->sid);
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_NOT_ACTIVE_TABLE);
    return;
  }

  mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_SUCCESS);

  SMDCreateTableMsg *pMDCreate = NULL;
  pMDCreate = mgmtBuildCreateChildTableMsg(NULL, (SChildTableObj *) pTable);
  if (pMDCreate == NULL) {
    mgmtDecTableRef(pTable);
    return;
  }

  SDnodeObj *pDnode = mgmtGetDnode(pCfg->dnode);
  SRpcIpSet ipSet = mgmtGetIpSetFromIp(pDnode->dnodeEp);
  SRpcMsg rpcRsp = {
      .handle  = NULL,
      .pCont   = pMDCreate,
      .contLen = htonl(pMDCreate->contLen),
      .code    = 0,
      .msgType = TSDB_MSG_TYPE_MD_CREATE_TABLE
  };
  dnodeSendMsgToDnode(&ipSet, &rpcRsp);

  mgmtDecTableRef(pTable);
  mgmtDecDnodeRef(pDnode);
}

// handle drop child response
static void mgmtProcessDropChildTableRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->handle == NULL) return;

  SQueuedMsg *queueMsg = rpcMsg->handle;
  queueMsg->received++;

  SChildTableObj *pTable = queueMsg->ahandle;
  mPrint("table:%s, drop table rsp received, thandle:%p result:%s", pTable->info.tableId, queueMsg->thandle, tstrerror(rpcMsg->code));

  if (rpcMsg->code != TSDB_CODE_SUCCESS) {
    mError("table:%s, failed to drop in dnode, reason:%s", pTable->info.tableId, tstrerror(rpcMsg->code));
    mgmtSendSimpleResp(queueMsg->thandle, rpcMsg->code);
    free(queueMsg);
    mgmtDecTableRef(pTable);
    return;
  }

  if (queueMsg->pVgroup == NULL) queueMsg->pVgroup = mgmtGetVgroup(pTable->vgId);
  if (queueMsg->pVgroup == NULL) {
    mError("table:%s, failed to get vgroup", pTable->info.tableId);
    mgmtSendSimpleResp(queueMsg->thandle, TSDB_CODE_INVALID_VGROUP_ID);
    return;
  }
  
  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsChildTableSdb,
    .pObj = pTable
  };
  
  int32_t code = sdbDeleteRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    mError("table:%s, update ctables sdb error", pTable->info.tableId);
    mgmtSendSimpleResp(queueMsg->thandle, TSDB_CODE_SDB_ERROR);
    return;
  }

  if (queueMsg->pVgroup->numOfTables <= 0) {
    mPrint("vgId:%d, all tables is dropped, drop vgroup", queueMsg->pVgroup->vgId);
    mgmtDropVgroup(queueMsg->pVgroup, NULL);
  }

  mgmtSendSimpleResp(queueMsg->thandle, TSDB_CODE_SUCCESS);
  mgmtFreeQueuedMsg(queueMsg);
}

// handle create table response from dnode
// if failed, drop the table cached
static void mgmtProcessCreateChildTableRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->handle == NULL) return;

  SQueuedMsg *queueMsg = rpcMsg->handle;
  queueMsg->received++;

  SChildTableObj *pTable = queueMsg->ahandle;
  mTrace("table:%s, create table rsp received, thandle:%p result:%s", pTable->info.tableId, queueMsg->thandle,
         tstrerror(rpcMsg->code));

  if (rpcMsg->code != TSDB_CODE_SUCCESS) {
    if (queueMsg->retry++ < queueMsg->maxRetry) {
      mTrace("table:%s, create table rsp received, retry:%d thandle:%p result:%s", pTable->info.tableId,
             queueMsg->retry, queueMsg->thandle, tstrerror(rpcMsg->code));
      mgmtDealyedAddToShellQueue(queueMsg);
    } else {
      mError("table:%s, failed to create in dnode, thandle:%p result:%s", pTable->info.tableId,
             queueMsg->thandle, tstrerror(rpcMsg->code));
      
      SSdbOper oper = {
        .type = SDB_OPER_GLOBAL,
        .table = tsChildTableSdb,
        .pObj = pTable
      };
      sdbDeleteRow(&oper);
    
      mgmtSendSimpleResp(queueMsg->thandle, rpcMsg->code);
      mgmtFreeQueuedMsg(queueMsg);
    }    
  } else {
    mTrace("table:%s, created in dnode, thandle:%p result:%s", pTable->info.tableId, queueMsg->thandle,
           tstrerror(rpcMsg->code));
    SCMCreateTableMsg *pCreate = queueMsg->pCont;
    if (pCreate->getMeta) {
      mTrace("table:%s, continue to get meta", pTable->info.tableId);
      queueMsg->retry = 0;
      mgmtAddToShellQueue(queueMsg);
    } else {
      mgmtSendSimpleResp(queueMsg->thandle, rpcMsg->code);
      mgmtFreeQueuedMsg(queueMsg);
    }
  }
}

// not implemented yet
static void mgmtProcessAlterTableRsp(SRpcMsg *rpcMsg) {
  mTrace("alter table rsp received, handle:%p code:%s", rpcMsg->handle, tstrerror(rpcMsg->code));
}

static void mgmtProcessMultiTableMetaMsg(SQueuedMsg *pMsg) {
  SCMMultiTableInfoMsg *pInfo = pMsg->pCont;
  pInfo->numOfTables = htonl(pInfo->numOfTables);

  int32_t totalMallocLen = 4 * 1024 * 1024;  // first malloc 4 MB, subsequent reallocation as twice
  SMultiTableMeta *pMultiMeta = rpcMallocCont(totalMallocLen);
  if (pMultiMeta == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  pMultiMeta->contLen = sizeof(SMultiTableMeta);
  pMultiMeta->numOfTables = 0;

  for (int32_t t = 0; t < pInfo->numOfTables; ++t) {
    char * tableId = (char *)(pInfo->tableIds + t * TSDB_TABLE_ID_LEN + 1);
    SChildTableObj *pTable = mgmtGetChildTable(tableId);
    if (pTable == NULL) continue;

    if (pMsg->pDb == NULL) pMsg->pDb = mgmtGetDbByTableId(tableId);
    if (pMsg->pDb == NULL) {
      mgmtDecTableRef(pTable);
      continue;
    }

    int availLen = totalMallocLen - pMultiMeta->contLen;
    if (availLen <= sizeof(STableMetaMsg) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16)) {
      totalMallocLen *= 2;
      pMultiMeta = rpcReallocCont(pMultiMeta, totalMallocLen);
      if (pMultiMeta == NULL) {
        mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
        mgmtDecTableRef(pTable);
        return;
      } else {
        t--;
        mgmtDecTableRef(pTable);
        continue;
      }
    }

    STableMetaMsg *pMeta = (STableMetaMsg *)(pMultiMeta->metas + pMultiMeta->contLen);
    int32_t code = mgmtDoGetChildTableMeta(pMsg, pMeta);
    if (code == TSDB_CODE_SUCCESS) {
      pMultiMeta->numOfTables ++;
      pMultiMeta->contLen += pMeta->contLen;
    }

    mgmtDecTableRef(pTable);
  }

  SRpcMsg rpcRsp = {0};
  rpcRsp.handle = pMsg->thandle;
  rpcRsp.pCont = pMultiMeta;
  rpcRsp.contLen = pMultiMeta->contLen;
  rpcSendResponse(&rpcRsp);
}

static int32_t mgmtGetShowTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mgmtGetDb(pShow->db);
  if (pDb == NULL) return TSDB_CODE_DB_NOT_SELECTED;

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "table_name");
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

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "stable_name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = pDb->numOfTables;
  pShow->rowSize   = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  mgmtDecDbRef(pDb);
  return 0;
}

static void mgmtVacuumResult(char *data, int32_t numOfCols, int32_t rows, int32_t capacity, SShowObj *pShow) {
  if (rows < capacity) {
    for (int32_t i = 0; i < numOfCols; ++i) {
      memmove(data + pShow->offset[i] * rows, data + pShow->offset[i] * capacity, pShow->bytes[i] * rows);
    }
  }
}

static int32_t mgmtRetrieveShowTables(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  SDbObj *pDb = mgmtGetDb(pShow->db);
  if (pDb == NULL) return 0;

  int32_t numOfRows  = 0;
  SChildTableObj *pTable = NULL;
  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  char prefix[64] = {0};
  strcpy(prefix, pDb->name);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = strlen(prefix);

  while (numOfRows < rows) {
    pShow->pIter = mgmtGetNextChildTable(pShow->pIter, &pTable);
    if (pTable == NULL) break;

    // not belong to current db
    if (strncmp(pTable->info.tableId, prefix, prefixLen)) {
      mgmtDecTableRef(pTable);
      continue;
    }

    char tableName[TSDB_TABLE_NAME_LEN + 1] = {0};
    
    // pattern compare for table name
    mgmtExtractTableName(pTable->info.tableId, tableName);

    if (pShow->payloadLen > 0 && patternMatch(pShow->payload, tableName, TSDB_TABLE_NAME_LEN, &info) != TSDB_PATTERN_MATCH) {
      mgmtDecTableRef(pTable);
      continue;
    }

    int32_t cols = 0;

    char *pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;

    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tableName, TSDB_TABLE_NAME_LEN);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *) pWrite = pTable->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (pTable->info.type == TSDB_CHILD_TABLE) {
      *(int16_t *)pWrite = pTable->superTable->numOfColumns;
    } else {
      *(int16_t *)pWrite = pTable->numOfColumns;
    }

    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    
    memset(tableName, 0, tListLen(tableName));
    if (pTable->info.type == TSDB_CHILD_TABLE) {
      mgmtExtractTableName(pTable->superTable->info.tableId, tableName);
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tableName, TSDB_TABLE_NAME_LEN);
    }
    
    cols++;

    numOfRows++;
    mgmtDecTableRef(pTable);
  }

  pShow->numOfReads += numOfRows;
  const int32_t NUM_OF_COLUMNS = 4;

  mgmtVacuumResult(data, NUM_OF_COLUMNS, numOfRows, rows, pShow);
  mgmtDecDbRef(pDb);

  return numOfRows;
}

static void mgmtProcessAlterTableMsg(SQueuedMsg *pMsg) {
  SCMAlterTableMsg *pAlter = pMsg->pCont;
  mTrace("table:%s, alter table msg is received from thandle:%p", pAlter->tableId, pMsg->thandle);

  if (pMsg->pDb == NULL) pMsg->pDb = mgmtGetDbByTableId(pAlter->tableId);
  if (pMsg->pDb == NULL || pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("table:%s, failed to alter table, db not selected", pAlter->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_DB_NOT_SELECTED);
    return;
  }

  if (mgmtCheckIsMonitorDB(pMsg->pDb->name, tsMonitorDbName)) {
    mError("table:%s, failed to alter table, its log db", pAlter->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_MONITOR_DB_FORBIDDEN);
    return;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mgmtGetTable(pAlter->tableId);
  if (pMsg->pTable == NULL) {
    mError("table:%s, failed to alter table, table not exist", pMsg->pTable->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_TABLE);
    return;
  }

  pAlter->type = htons(pAlter->type);
  pAlter->numOfCols = htons(pAlter->numOfCols);
  pAlter->tagValLen = htonl(pAlter->tagValLen);

  if (pAlter->numOfCols > 2) {
    mError("table:%s, error numOfCols:%d in alter table", pAlter->tableId, pAlter->numOfCols);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_APP_ERROR);
    return;
  }

  for (int32_t i = 0; i < pAlter->numOfCols; ++i) {
    pAlter->schema[i].bytes = htons(pAlter->schema[i].bytes);
  }

  int32_t code = TSDB_CODE_OPS_NOT_SUPPORT;
  if (pMsg->pTable->type == TSDB_SUPER_TABLE) {
    SSuperTableObj *pTable = (SSuperTableObj *)pMsg->pTable;
    mTrace("table:%s, start to alter stable", pAlter->tableId);
    if (pAlter->type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN) {
      code = mgmtAddSuperTableTag(pTable, pAlter->schema, 1);
    } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN) {
      code = mgmtDropSuperTableTag(pTable, pAlter->schema[0].name);
    } else if (pAlter->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN) {
      code = mgmtModifySuperTableTagName(pTable, pAlter->schema[0].name, pAlter->schema[1].name);
    } else if (pAlter->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
      code = mgmtAddSuperTableColumn(pMsg->pDb, pTable, pAlter->schema, 1);
    } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
      code = mgmtDropSuperTableColumn(pMsg->pDb, pTable, pAlter->schema[0].name);
    } else {
    }
  } else {
    mTrace("table:%s, start to alter ctable", pAlter->tableId);
    SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;
    if (pAlter->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
      char *tagVal = (char*)(pAlter->schema + pAlter->numOfCols);
      code = mgmtModifyChildTableTagValue(pTable, pAlter->schema[0].name, tagVal);
    } else if (pAlter->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
      code = mgmtAddNormalTableColumn(pMsg->pDb, pTable, pAlter->schema, 1);
    } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
      code = mgmtDropNormalTableColumn(pMsg->pDb, pTable, pAlter->schema[0].name);
    } else {
    }
  }

  mgmtSendSimpleResp(pMsg->thandle, code);
}
