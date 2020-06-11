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
#include "tcompare.h"
#include "tdataformat.h"
#include "tgrant.h"
#include "hash.h"
#include "mnode.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeAcct.h"
#include "mnodeDb.h"
#include "mnodeDnode.h"
#include "mnodeMnode.h"
#include "mnodeProfile.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeTable.h"
#include "mnodeUser.h"
#include "mnodeVgroup.h"
#include "mnodeWrite.h"
#include "mnodeRead.h"
#include "mnodePeer.h"

static void *  tsChildTableSdb;
static void *  tsSuperTableSdb;
static int32_t tsChildTableUpdateSize;
static int32_t tsSuperTableUpdateSize;
static void *  mnodeGetChildTable(char *tableId);
static void *  mnodeGetSuperTable(char *tableId);
static void *  mnodeGetSuperTableByUid(uint64_t uid);
static void    mnodeDropAllChildTablesInStable(SSuperTableObj *pStable);
static void    mnodeAddTableIntoStable(SSuperTableObj *pStable, SChildTableObj *pCtable);
static void    mnodeRemoveTableFromStable(SSuperTableObj *pStable, SChildTableObj *pCtable);

static int32_t mnodeGetShowTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveShowTables(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mnodeGetShowSuperTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveShowSuperTables(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static int32_t mnodeGetStreamTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mnodeRetrieveStreamTables(SShowObj *pShow, char *data, int32_t rows, void *pConn);
 
static int32_t mnodeProcessCreateTableMsg(SMnodeMsg *mnodeMsg);
static int32_t mnodeProcessCreateSuperTableMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessCreateChildTableMsg(SMnodeMsg *pMsg);
static void    mnodeProcessCreateChildTableRsp(SRpcMsg *rpcMsg);

static int32_t mnodeProcessDropTableMsg(SMnodeMsg *mnodeMsg);
static int32_t mnodeProcessDropSuperTableMsg(SMnodeMsg *pMsg);
static void    mnodeProcessDropSuperTableRsp(SRpcMsg *rpcMsg);
static int32_t mnodeProcessDropChildTableMsg(SMnodeMsg *pMsg);
static void    mnodeProcessDropChildTableRsp(SRpcMsg *rpcMsg);

static int32_t mnodeProcessSuperTableVgroupMsg(SMnodeMsg *mnodeMsg);
static int32_t mnodeProcessMultiTableMetaMsg(SMnodeMsg *mnodeMsg);
static int32_t mnodeProcessTableCfgMsg(SMnodeMsg *mnodeMsg);

static int32_t mnodeProcessTableMetaMsg(SMnodeMsg *mnodeMsg);
static int32_t mnodeGetSuperTableMeta(SMnodeMsg *pMsg);
static int32_t mnodeGetChildTableMeta(SMnodeMsg *pMsg);
static int32_t mnodeAutoCreateChildTable(SMnodeMsg *pMsg);

static int32_t mnodeProcessAlterTableMsg(SMnodeMsg *mnodeMsg);
static void    mnodeProcessAlterTableRsp(SRpcMsg *rpcMsg);

static int32_t mnodeFindSuperTableColumnIndex(SSuperTableObj *pStable, char *colName);

static void mnodeDestroyChildTable(SChildTableObj *pTable) {
  tfree(pTable->info.tableId);
  tfree(pTable->schema);
  tfree(pTable->sql);
  tfree(pTable);
}

static int32_t mnodeChildTableActionDestroy(SSdbOper *pOper) {
  mnodeDestroyChildTable(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeChildTableActionInsert(SSdbOper *pOper) {
  SChildTableObj *pTable = pOper->pObj;

  SVgObj *pVgroup = mnodeGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    mError("ctable:%s, not in vgId:%d", pTable->info.tableId, pTable->vgId);
    return TSDB_CODE_MND_VGROUP_NOT_EXIST;
  }
  mnodeDecVgroupRef(pVgroup);

  SDbObj *pDb = mnodeGetDb(pVgroup->dbName);
  if (pDb == NULL) {
    mError("ctable:%s, vgId:%d not in db:%s", pTable->info.tableId, pVgroup->vgId, pVgroup->dbName);
    return TSDB_CODE_MND_INVALID_DB;
  }
  mnodeDecDbRef(pDb);

  SAcctObj *pAcct = mnodeGetAcct(pDb->acct);
  if (pAcct == NULL) {
    mError("ctable:%s, acct:%s not exists", pTable->info.tableId, pDb->acct);
    return TSDB_CODE_MND_INVALID_ACCT;
  }
  mnodeDecAcctRef(pAcct);

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    // add ref
    pTable->superTable = mnodeGetSuperTableByUid(pTable->suid);
    mnodeAddTableIntoStable(pTable->superTable, pTable);
    grantAdd(TSDB_GRANT_TIMESERIES, pTable->superTable->numOfColumns - 1);
    pAcct->acctInfo.numOfTimeSeries += (pTable->superTable->numOfColumns - 1);
  } else {
    grantAdd(TSDB_GRANT_TIMESERIES, pTable->numOfColumns - 1);
    pAcct->acctInfo.numOfTimeSeries += (pTable->numOfColumns - 1);
  }

  mnodeAddTableIntoDb(pDb);
  mnodeAddTableIntoVgroup(pVgroup, pTable);

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeChildTableActionDelete(SSdbOper *pOper) {
  SChildTableObj *pTable = pOper->pObj;
  if (pTable->vgId == 0) {
    return TSDB_CODE_MND_VGROUP_NOT_EXIST;
  }

  SVgObj *pVgroup = NULL;
  SDbObj *pDb = NULL;
  SAcctObj *pAcct = NULL;
  
  pVgroup = mnodeGetVgroup(pTable->vgId);
  if (pVgroup != NULL) pDb = mnodeGetDb(pVgroup->dbName);
  if (pDb != NULL) pAcct = mnodeGetAcct(pDb->acct);

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    grantRestore(TSDB_GRANT_TIMESERIES, pTable->superTable->numOfColumns - 1);
    if (pAcct != NULL) pAcct->acctInfo.numOfTimeSeries -= (pTable->superTable->numOfColumns - 1);
    mnodeRemoveTableFromStable(pTable->superTable, pTable);
    mnodeDecTableRef(pTable->superTable);
  } else {
    grantRestore(TSDB_GRANT_TIMESERIES, pTable->numOfColumns - 1);
    if (pAcct != NULL) pAcct->acctInfo.numOfTimeSeries -= (pTable->numOfColumns - 1);
  }
  
  if (pDb != NULL) mnodeRemoveTableFromDb(pDb);
  if (pVgroup != NULL) mnodeRemoveTableFromVgroup(pVgroup, pTable);

  mnodeDecVgroupRef(pVgroup);
  mnodeDecDbRef(pDb);
  mnodeDecAcctRef(pAcct);
 
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeChildTableActionUpdate(SSdbOper *pOper) {
  SChildTableObj *pNew = pOper->pObj;
  SChildTableObj *pTable = mnodeGetChildTable(pNew->info.tableId);
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
  mnodeDecTableRef(pTable);

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeChildTableActionEncode(SSdbOper *pOper) {
  SChildTableObj *pTable = pOper->pObj;
  assert(pTable != NULL && pOper->rowData != NULL);

  int32_t len = strlen(pTable->info.tableId);
  if (len > TSDB_TABLE_ID_LEN) return TSDB_CODE_MND_INVALID_TABLE_ID;

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

static int32_t mnodeChildTableActionDecode(SSdbOper *pOper) {
  assert(pOper->rowData != NULL);
  SChildTableObj *pTable = calloc(1, sizeof(SChildTableObj));
  if (pTable == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

  int32_t len = strlen(pOper->rowData);
  if (len > TSDB_TABLE_ID_LEN) {
    free(pTable);
    return TSDB_CODE_MND_INVALID_TABLE_ID;
  }
  pTable->info.tableId = strdup(pOper->rowData);
  len++;

  memcpy((char*)pTable + sizeof(char *), pOper->rowData + len, tsChildTableUpdateSize);
  len += tsChildTableUpdateSize;

  if (pTable->info.type != TSDB_CHILD_TABLE) {
    int32_t schemaSize = pTable->numOfColumns * sizeof(SSchema);
    pTable->schema = (SSchema *)malloc(schemaSize);
    if (pTable->schema == NULL) {
      mnodeDestroyChildTable(pTable);
      return TSDB_CODE_MND_INVALID_TABLE_TYPE;
    }
    memcpy(pTable->schema, pOper->rowData + len, schemaSize);
    len += schemaSize;

    if (pTable->sqlLen != 0) {
      pTable->sql = malloc(pTable->sqlLen);
      if (pTable->sql == NULL) {
        mnodeDestroyChildTable(pTable);
        return TSDB_CODE_MND_OUT_OF_MEMORY;
      }
      memcpy(pTable->sql, pOper->rowData + len, pTable->sqlLen);
    }
  }

  pOper->pObj = pTable;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeChildTableActionRestored() {
  void *pIter = NULL;
  SChildTableObj *pTable = NULL;

  while (1) {
    pIter = mnodeGetNextChildTable(pIter, &pTable);
    if (pTable == NULL) break;

    SDbObj *pDb = mnodeGetDbByTableId(pTable->info.tableId);
    if (pDb == NULL) {
      mError("ctable:%s, failed to get db, discard it", pTable->info.tableId);
      SSdbOper desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .table = tsChildTableSdb};
      sdbDeleteRow(&desc);
      mnodeDecTableRef(pTable);
      continue;
    }
    mnodeDecDbRef(pDb);

    SVgObj *pVgroup = mnodeGetVgroup(pTable->vgId);
    if (pVgroup == NULL) {
      mError("ctable:%s, failed to get vgId:%d sid:%d, discard it", pTable->info.tableId, pTable->vgId, pTable->sid);
      pTable->vgId = 0;
      SSdbOper desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .table = tsChildTableSdb};
      sdbDeleteRow(&desc);
      mnodeDecTableRef(pTable);
      continue;
    }
    mnodeDecVgroupRef(pVgroup);

    if (strcmp(pVgroup->dbName, pDb->name) != 0) {
      mError("ctable:%s, db:%s not match with vgId:%d db:%s sid:%d, discard it",
             pTable->info.tableId, pDb->name, pTable->vgId, pVgroup->dbName, pTable->sid);
      pTable->vgId = 0;
      SSdbOper desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .table = tsChildTableSdb};
      sdbDeleteRow(&desc);
      mnodeDecTableRef(pTable);
      continue;
    }

    if (pVgroup->tableList == NULL) {
      mError("ctable:%s, vgId:%d tableList is null", pTable->info.tableId, pTable->vgId);
      pTable->vgId = 0;
      SSdbOper desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .table = tsChildTableSdb};
      sdbDeleteRow(&desc);
      mnodeDecTableRef(pTable);
      continue;
    }

    if (pTable->info.type == TSDB_CHILD_TABLE) {
      SSuperTableObj *pSuperTable = mnodeGetSuperTableByUid(pTable->suid);
      if (pSuperTable == NULL) {
        mError("ctable:%s, stable:%" PRIu64 " not exist", pTable->info.tableId, pTable->suid);
        pTable->vgId = 0;
        SSdbOper desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .table = tsChildTableSdb};
        sdbDeleteRow(&desc);
        mnodeDecTableRef(pTable);
        continue;
      }
      mnodeDecTableRef(pSuperTable);
    }

    mnodeDecTableRef(pTable);
  }

  sdbFreeIter(pIter);

  return 0;
}

static int32_t mnodeInitChildTables() {
  SChildTableObj tObj;
  tsChildTableUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj.info.type;

  SSdbTableDesc tableDesc = {
    .tableId      = SDB_TABLE_CTABLE,
    .tableName    = "ctables",
    .hashSessions = TSDB_DEFAULT_CTABLES_HASH_SIZE,
    .maxRowSize   = sizeof(SChildTableObj) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16) + TSDB_TABLE_ID_LEN + TSDB_CQ_SQL_SIZE,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_VAR_STRING,
    .insertFp     = mnodeChildTableActionInsert,
    .deleteFp     = mnodeChildTableActionDelete,
    .updateFp     = mnodeChildTableActionUpdate,
    .encodeFp     = mnodeChildTableActionEncode,
    .decodeFp     = mnodeChildTableActionDecode,
    .destroyFp    = mnodeChildTableActionDestroy,
    .restoredFp   = mnodeChildTableActionRestored
  };

  tsChildTableSdb = sdbOpenTable(&tableDesc);
  if (tsChildTableSdb == NULL) {
    mError("failed to init child table data");
    return -1;
  }

  mTrace("table:ctables is created");
  return 0;
}

static void mnodeCleanupChildTables() {
  sdbCloseTable(tsChildTableSdb);
}

static void mnodeAddTableIntoStable(SSuperTableObj *pStable, SChildTableObj *pCtable) {
  pStable->numOfTables++;

  if (pStable->vgHash == NULL) {
    pStable->vgHash = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), false);
  }

  if (pStable->vgHash != NULL) {
    taosHashPut(pStable->vgHash, (char *)&pCtable->vgId, sizeof(pCtable->vgId), &pCtable->vgId, sizeof(pCtable->vgId));
  }
}

static void mnodeRemoveTableFromStable(SSuperTableObj *pStable, SChildTableObj *pCtable) {
  pStable->numOfTables--;

  if (pStable->vgHash == NULL) return;

  SVgObj *pVgroup = mnodeGetVgroup(pCtable->vgId);
  if (pVgroup == NULL) {
    taosHashRemove(pStable->vgHash, (char *)&pCtable->vgId, sizeof(pCtable->vgId));
  }
  mnodeDecVgroupRef(pVgroup);
}

static void mnodeDestroySuperTable(SSuperTableObj *pStable) {
  if (pStable->vgHash != NULL) {
    taosHashCleanup(pStable->vgHash);
    pStable->vgHash = NULL;
  }
  tfree(pStable->info.tableId);
  tfree(pStable->schema);
  tfree(pStable);
}

static int32_t mnodeSuperTableActionDestroy(SSdbOper *pOper) {
  mnodeDestroySuperTable(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeSuperTableActionInsert(SSdbOper *pOper) {
  SSuperTableObj *pStable = pOper->pObj;
  SDbObj *pDb = mnodeGetDbByTableId(pStable->info.tableId);
  if (pDb != NULL) {
    mnodeAddSuperTableIntoDb(pDb);
  }
  mnodeDecDbRef(pDb);

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeSuperTableActionDelete(SSdbOper *pOper) {
  SSuperTableObj *pStable = pOper->pObj;
  SDbObj *pDb = mnodeGetDbByTableId(pStable->info.tableId);
  if (pDb != NULL) {
    mnodeRemoveSuperTableFromDb(pDb);
    mnodeDropAllChildTablesInStable((SSuperTableObj *)pStable);
  }
  mnodeDecDbRef(pDb);

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeSuperTableActionUpdate(SSdbOper *pOper) {
  SSuperTableObj *pNew = pOper->pObj;
  SSuperTableObj *pTable = mnodeGetSuperTable(pNew->info.tableId);
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
  mnodeDecTableRef(pTable);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeSuperTableActionEncode(SSdbOper *pOper) {
  SSuperTableObj *pStable = pOper->pObj;
  assert(pOper->pObj != NULL && pOper->rowData != NULL);

  int32_t len = strlen(pStable->info.tableId);
  if (len > TSDB_TABLE_ID_LEN) len = TSDB_CODE_MND_INVALID_TABLE_ID;

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

static int32_t mnodeSuperTableActionDecode(SSdbOper *pOper) {
  assert(pOper->rowData != NULL);
  SSuperTableObj *pStable = (SSuperTableObj *) calloc(1, sizeof(SSuperTableObj));
  if (pStable == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

  int32_t len = strlen(pOper->rowData);
  if (len > TSDB_TABLE_ID_LEN){
    free(pStable);
    return TSDB_CODE_MND_INVALID_TABLE_ID;
  }
  pStable->info.tableId = strdup(pOper->rowData);
  len++;

  memcpy((char*)pStable + sizeof(char *), pOper->rowData + len, tsSuperTableUpdateSize);
  len += tsSuperTableUpdateSize;

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfColumns + pStable->numOfTags);
  pStable->schema = malloc(schemaSize);
  if (pStable->schema == NULL) {
    mnodeDestroySuperTable(pStable);
    return TSDB_CODE_MND_NOT_SUPER_TABLE;
  }

  memcpy(pStable->schema, pOper->rowData + len, schemaSize);
  
  pOper->pObj = pStable;

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeSuperTableActionRestored() {
  return 0;
}

static int32_t mnodeInitSuperTables() {
  SSuperTableObj tObj;
  tsSuperTableUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj.info.type;

  SSdbTableDesc tableDesc = {
    .tableId      = SDB_TABLE_STABLE,
    .tableName    = "stables",
    .hashSessions = TSDB_DEFAULT_STABLES_HASH_SIZE,
    .maxRowSize   = sizeof(SSuperTableObj) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16) + TSDB_TABLE_ID_LEN,
    .refCountPos  = (int8_t *)(&tObj.refCount) - (int8_t *)&tObj,
    .keyType      = SDB_KEY_VAR_STRING,
    .insertFp     = mnodeSuperTableActionInsert,
    .deleteFp     = mnodeSuperTableActionDelete,
    .updateFp     = mnodeSuperTableActionUpdate,
    .encodeFp     = mnodeSuperTableActionEncode,
    .decodeFp     = mnodeSuperTableActionDecode,
    .destroyFp    = mnodeSuperTableActionDestroy,
    .restoredFp   = mnodeSuperTableActionRestored
  };

  tsSuperTableSdb = sdbOpenTable(&tableDesc);
  if (tsSuperTableSdb == NULL) {
    mError("failed to init stables data");
    return -1;
  }

  mTrace("table:stables is created");
  return 0;
}

static void mnodeCleanupSuperTables() {
  sdbCloseTable(tsSuperTableSdb);
}

int32_t mnodeInitTables() {
  int32_t code = mnodeInitSuperTables();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = mnodeInitChildTables();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  mnodeAddReadMsgHandle(TSDB_MSG_TYPE_CM_TABLES_META, mnodeProcessMultiTableMetaMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_CREATE_TABLE, mnodeProcessCreateTableMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_DROP_TABLE, mnodeProcessDropTableMsg);
  mnodeAddWriteMsgHandle(TSDB_MSG_TYPE_CM_ALTER_TABLE, mnodeProcessAlterTableMsg);
  mnodeAddReadMsgHandle(TSDB_MSG_TYPE_CM_TABLE_META, mnodeProcessTableMetaMsg);
  mnodeAddReadMsgHandle(TSDB_MSG_TYPE_CM_STABLE_VGROUP, mnodeProcessSuperTableVgroupMsg);
  
  mnodeAddPeerRspHandle(TSDB_MSG_TYPE_MD_CREATE_TABLE_RSP, mnodeProcessCreateChildTableRsp);
  mnodeAddPeerRspHandle(TSDB_MSG_TYPE_MD_DROP_TABLE_RSP, mnodeProcessDropChildTableRsp);
  mnodeAddPeerRspHandle(TSDB_MSG_TYPE_MD_DROP_STABLE_RSP, mnodeProcessDropSuperTableRsp);
  mnodeAddPeerRspHandle(TSDB_MSG_TYPE_MD_ALTER_TABLE_RSP, mnodeProcessAlterTableRsp);

  mnodeAddPeerMsgHandle(TSDB_MSG_TYPE_DM_CONFIG_TABLE, mnodeProcessTableCfgMsg);

  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_TABLE, mnodeGetShowTableMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_TABLE, mnodeRetrieveShowTables);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_METRIC, mnodeGetShowSuperTableMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_METRIC, mnodeRetrieveShowSuperTables);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_STREAMTABLES, mnodeGetStreamTableMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_STREAMTABLES, mnodeRetrieveStreamTables);

  return TSDB_CODE_SUCCESS;
}

static void *mnodeGetChildTable(char *tableId) {
  return sdbGetRow(tsChildTableSdb, tableId);
}

static void *mnodeGetSuperTable(char *tableId) {
  return sdbGetRow(tsSuperTableSdb, tableId);
}

static void *mnodeGetSuperTableByUid(uint64_t uid) {
  SSuperTableObj *pStable = NULL;
  void *pIter = NULL;

  while (1) {
    pIter = mnodeGetNextSuperTable(pIter, &pStable);
    if (pStable == NULL) break;
    if (pStable->uid == uid) {
      sdbFreeIter(pIter);
      return pStable;
    }
    mnodeDecTableRef(pStable);
  }

  sdbFreeIter(pIter);

  return NULL;
}

void *mnodeGetTable(char *tableId) {
  void *pTable = mnodeGetSuperTable(tableId);
  if (pTable != NULL) {
    return pTable;
  }

  pTable = mnodeGetChildTable(tableId);
  if (pTable != NULL) {
    return pTable;
  }

  return NULL;
}

void *mnodeGetNextChildTable(void *pIter, SChildTableObj **pTable) {
  return sdbFetchRow(tsChildTableSdb, pIter, (void **)pTable);
}

void *mnodeGetNextSuperTable(void *pIter, SSuperTableObj **pTable) {
  return sdbFetchRow(tsSuperTableSdb, pIter, (void **)pTable);
}

void mnodeIncTableRef(void *p1) {
  STableObj *pTable = (STableObj *)p1;
  if (pTable->type == TSDB_SUPER_TABLE) {
    sdbIncRef(tsSuperTableSdb, pTable);
  } else {
    sdbIncRef(tsChildTableSdb, pTable);
  }
}

void mnodeDecTableRef(void *p1) {
  if (p1 == NULL) return;

  STableObj *pTable = (STableObj *)p1;
  if (pTable->type == TSDB_SUPER_TABLE) {
    sdbDecRef(tsSuperTableSdb, pTable);
  } else {
    sdbDecRef(tsChildTableSdb, pTable);
  }
}

void mnodeCleanupTables() {
  mnodeCleanupChildTables();
  mnodeCleanupSuperTables();
}

// todo move to name.h, add length of table name
static void mnodeExtractTableName(char* tableId, char* name) {
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

static int32_t mnodeProcessCreateTableMsg(SMnodeMsg *pMsg) {
  SCMCreateTableMsg *pCreate = pMsg->rpcMsg.pCont;
  
  if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDb(pCreate->db);
  if (pMsg->pDb == NULL || pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("table:%s, failed to create, db not selected", pCreate->tableId);
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pCreate->tableId);
  if (pMsg->pTable != NULL && pMsg->retry == 0) {
    if (pCreate->getMeta) {
      mTrace("table:%s, continue to get meta", pCreate->tableId);
      return mnodeGetChildTableMeta(pMsg);
    } else if (pCreate->igExists) {
      mTrace("table:%s, is already exist", pCreate->tableId);
      return TSDB_CODE_SUCCESS;
    } else {
      mError("table:%s, failed to create, table already exist", pCreate->tableId);
      return TSDB_CODE_MND_TABLE_ALREADY_EXIST;
    }
  }

  if (pCreate->numOfTags != 0) {
    mTrace("table:%s, create stable msg is received from thandle:%p", pCreate->tableId, pMsg->rpcMsg.handle);
    return mnodeProcessCreateSuperTableMsg(pMsg);
  } else {
    mTrace("table:%s, create ctable msg is received from thandle:%p", pCreate->tableId, pMsg->rpcMsg.handle);
    return mnodeProcessCreateChildTableMsg(pMsg);
  }
}

static int32_t mnodeProcessDropTableMsg(SMnodeMsg *pMsg) {
  SCMDropTableMsg *pDrop = pMsg->rpcMsg.pCont;
  if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDbByTableId(pDrop->tableId);
  if (pMsg->pDb == NULL || pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("table:%s, failed to drop table, db not selected", pDrop->tableId);
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }

  if (mnodeCheckIsMonitorDB(pMsg->pDb->name, tsMonitorDbName)) {
    mError("table:%s, failed to drop table, in monitor database", pDrop->tableId);
    return TSDB_CODE_MND_MONITOR_DB_FORBIDDEN;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pDrop->tableId);
  if (pMsg->pTable == NULL) {
    if (pDrop->igNotExists) {
      mTrace("table:%s, table is not exist, think drop success", pDrop->tableId);
      return TSDB_CODE_SUCCESS;
    } else {
      mError("table:%s, failed to drop table, table not exist", pDrop->tableId);
      return TSDB_CODE_MND_INVALID_TABLE_NAME;
    }
  }

  if (pMsg->pTable->type == TSDB_SUPER_TABLE) {
    mPrint("table:%s, start to drop stable", pDrop->tableId);
    return mnodeProcessDropSuperTableMsg(pMsg);
  } else {
    mPrint("table:%s, start to drop ctable", pDrop->tableId);
    return mnodeProcessDropChildTableMsg(pMsg);
  }
}

static int32_t mnodeProcessTableMetaMsg(SMnodeMsg *pMsg) {
  SCMTableInfoMsg *pInfo = pMsg->rpcMsg.pCont;
  pInfo->createFlag = htons(pInfo->createFlag);
  mTrace("table:%s, table meta msg is received from thandle:%p, createFlag:%d", pInfo->tableId, pMsg->rpcMsg.handle, pInfo->createFlag);

  if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDbByTableId(pInfo->tableId);
  if (pMsg->pDb == NULL || pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("table:%s, failed to get table meta, db not selected", pInfo->tableId);
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pInfo->tableId);
  if (pMsg->pTable == NULL) {
    if (!pInfo->createFlag) {
      mError("table:%s, failed to get table meta, table not exist", pInfo->tableId);
      return TSDB_CODE_MND_INVALID_TABLE_NAME;
    } else {
      mTrace("table:%s, failed to get table meta, start auto create table ", pInfo->tableId);
      return mnodeAutoCreateChildTable(pMsg);
    }
  } else {
    if (pMsg->pTable->type != TSDB_SUPER_TABLE) {
      return mnodeGetChildTableMeta(pMsg);
    } else {
      return mnodeGetSuperTableMeta(pMsg);
    }
  }
}

static int32_t mnodeProcessCreateSuperTableMsg(SMnodeMsg *pMsg) {
  SCMCreateTableMsg *pCreate = pMsg->rpcMsg.pCont;
  SSuperTableObj *pStable = calloc(1, sizeof(SSuperTableObj));
  if (pStable == NULL) {
    mError("table:%s, failed to create, no enough memory", pCreate->tableId);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
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
    return TSDB_CODE_MND_INVALID_TABLE_NAME;
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
    mnodeDestroySuperTable(pStable);
    mError("table:%s, failed to create, sdb error", pCreate->tableId);
    return TSDB_CODE_MND_SDB_ERROR;
  } else {
    mLPrint("table:%s, is created, tags:%d fields:%d", pStable->info.tableId, pStable->numOfTags, pStable->numOfColumns);
    return TSDB_CODE_SUCCESS;
  }
}

static int32_t mnodeProcessDropSuperTableMsg(SMnodeMsg *pMsg) {
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  if (pStable->numOfTables != 0) {
    SHashMutableIterator *pIter = taosHashCreateIter(pStable->vgHash);
    while (taosHashIterNext(pIter)) {
      int32_t *pVgId = taosHashIterGet(pIter);
      SVgObj *pVgroup = mnodeGetVgroup(*pVgId);
      if (pVgroup == NULL) break;

      SMDDropSTableMsg *pDrop = rpcMallocCont(sizeof(SMDDropSTableMsg));
      pDrop->contLen = htonl(sizeof(SMDDropSTableMsg));
      pDrop->vgId = htonl(pVgroup->vgId);
      pDrop->uid = htobe64(pStable->uid);
      mnodeExtractTableName(pStable->info.tableId, pDrop->tableId);
        
      mPrint("stable:%s, send drop stable msg to vgId:%d", pStable->info.tableId, pVgroup->vgId);
      SRpcIpSet ipSet = mnodeGetIpSetFromVgroup(pVgroup);
      SRpcMsg rpcMsg = {.pCont = pDrop, .contLen = sizeof(SMDDropSTableMsg), .msgType = TSDB_MSG_TYPE_MD_DROP_STABLE};
      dnodeSendMsgToDnode(&ipSet, &rpcMsg);
      mnodeDecVgroupRef(pVgroup);
    }
    taosHashDestroyIter(pIter);

    mnodeDropAllChildTablesInStable(pStable);
  } 
  
  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable
  };
  
  int32_t code = sdbDeleteRow(&oper);
  mLPrint("stable:%s, is dropped from sdb, result:%s", pStable->info.tableId, tstrerror(code));
  return code;
}

static int32_t mnodeFindSuperTableTagIndex(SSuperTableObj *pStable, const char *tagName) {
  SSchema *schema = (SSchema *) pStable->schema;
  for (int32_t tag = 0; tag < pStable->numOfTags; tag++) {
    if (strcasecmp(schema[pStable->numOfColumns + tag].name, tagName) == 0) {
      return tag;
    }
  }

  return -1;
}

static int32_t mnodeAddSuperTableTag(SSuperTableObj *pStable, SSchema schema[], int32_t ntags) {
  if (pStable->numOfTags + ntags > TSDB_MAX_TAGS) {
    mError("stable:%s, add tag, too many tags", pStable->info.tableId);
    return TSDB_CODE_MND_TOO_MANY_TAGS;
  }

  for (int32_t i = 0; i < ntags; i++) {
    if (mnodeFindSuperTableColumnIndex(pStable, schema[i].name) > 0) {
      mError("stable:%s, add tag, column:%s already exist", pStable->info.tableId, schema[i].name);
      return TSDB_CODE_MND_TAG_ALREAY_EXIST;
    }

    if (mnodeFindSuperTableTagIndex(pStable, schema[i].name) > 0) {
      mError("stable:%s, add tag, tag:%s already exist", pStable->info.tableId, schema[i].name);
      return TSDB_CODE_MND_FIELD_ALREAY_EXIST;
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
    return TSDB_CODE_MND_SDB_ERROR;
  }

  mPrint("stable %s, succeed to add tag %s", pStable->info.tableId, schema[0].name);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDropSuperTableTag(SSuperTableObj *pStable, char *tagName) {
  int32_t col = mnodeFindSuperTableTagIndex(pStable, tagName);
  if (col < 0) {
    mError("stable:%s, drop tag, tag:%s not exist", pStable->info.tableId, tagName);
    return TSDB_CODE_MND_TAG_NOT_EXIST;
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
    return TSDB_CODE_MND_SDB_ERROR;
  }
  
  mPrint("stable %s, succeed to drop tag %s", pStable->info.tableId, tagName);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeModifySuperTableTagName(SSuperTableObj *pStable, char *oldTagName, char *newTagName) {
  int32_t col = mnodeFindSuperTableTagIndex(pStable, oldTagName);
  if (col < 0) {
    mError("stable:%s, failed to modify table tag, oldName: %s, newName: %s", pStable->info.tableId, oldTagName, newTagName);
    return TSDB_CODE_MND_TAG_NOT_EXIST;
  }

  // int32_t  rowSize = 0;
  uint32_t len = strlen(newTagName);
  if (len >= TSDB_COL_NAME_LEN) {
    return TSDB_CODE_MND_COL_NAME_TOO_LONG;
  }

  if (mnodeFindSuperTableTagIndex(pStable, newTagName) >= 0) {
    return TSDB_CODE_MND_TAG_ALREAY_EXIST;
  }

  // update
  SSchema *schema = (SSchema *) (pStable->schema + pStable->numOfColumns + col);
  tstrncpy(schema->name, newTagName, sizeof(schema->name));

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_MND_SDB_ERROR;
  }
  
  mPrint("stable %s, succeed to modify tag %s to %s", pStable->info.tableId, oldTagName, newTagName);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeFindSuperTableColumnIndex(SSuperTableObj *pStable, char *colName) {
  SSchema *schema = (SSchema *) pStable->schema;
  for (int32_t col = 0; col < pStable->numOfColumns; col++) {
    if (strcasecmp(schema[col].name, colName) == 0) {
      return col;
    }
  }

  return -1;
}

static int32_t mnodeAddSuperTableColumn(SDbObj *pDb, SSuperTableObj *pStable, SSchema schema[], int32_t ncols) {
  if (ncols <= 0) {
    mError("stable:%s, add column, ncols:%d <= 0", pStable->info.tableId, ncols);
    return TSDB_CODE_MND_APP_ERROR;
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (mnodeFindSuperTableColumnIndex(pStable, schema[i].name) > 0) {
      mError("stable:%s, add column, column:%s already exist", pStable->info.tableId, schema[i].name);
      return TSDB_CODE_MND_FIELD_ALREAY_EXIST;
    }

    if (mnodeFindSuperTableTagIndex(pStable, schema[i].name) > 0) {
      mError("stable:%s, add column, tag:%s already exist", pStable->info.tableId, schema[i].name);
      return TSDB_CODE_MND_TAG_ALREAY_EXIST;
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

  SAcctObj *pAcct = mnodeGetAcct(pDb->acct);
  if (pAcct != NULL) {
    pAcct->acctInfo.numOfTimeSeries += (ncols * pStable->numOfTables);
    mnodeDecAcctRef(pAcct);
  }

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_MND_SDB_ERROR;
  }
  
  mPrint("stable %s, succeed to add column", pStable->info.tableId);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDropSuperTableColumn(SDbObj *pDb, SSuperTableObj *pStable, char *colName) {
  int32_t col = mnodeFindSuperTableColumnIndex(pStable, colName);
  if (col <= 0) {
    mError("stable:%s, drop column, column:%s not exist", pStable->info.tableId, colName);
    return TSDB_CODE_MND_FIELD_NOT_EXIST;
  }

  memmove(pStable->schema + col, pStable->schema + col + 1,
          sizeof(SSchema) * (pStable->numOfColumns + pStable->numOfTags - col - 1));

  pStable->numOfColumns--;
  pStable->sversion++;

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfTags + pStable->numOfColumns);
  pStable->schema = realloc(pStable->schema, schemaSize);

  SAcctObj *pAcct = mnodeGetAcct(pDb->acct);
  if (pAcct != NULL) {
    pAcct->acctInfo.numOfTimeSeries -= pStable->numOfTables;
    mnodeDecAcctRef(pAcct);
  }

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_MND_SDB_ERROR;
  }
  
  mPrint("stable %s, succeed to delete column", pStable->info.tableId);
  return TSDB_CODE_SUCCESS;
}

// show super tables
static int32_t mnodeGetShowSuperTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) return TSDB_CODE_MND_DB_NOT_SELECTED;

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = (TSDB_TABLE_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
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

  mnodeDecDbRef(pDb);
  return 0;
}

// retrieve super tables
int32_t mnodeRetrieveShowSuperTables(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  int32_t         numOfRows = 0;
  char *          pWrite;
  int32_t         cols = 0;
  SSuperTableObj *pTable = NULL;
  char            prefix[20] = {0};
  int32_t         prefixLen;

  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) return 0;

  strcpy(prefix, pDb->name);
  strcat(prefix, TS_PATH_DELIMITER);
  prefixLen = strlen(prefix);

  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;
  char stableName[TSDB_TABLE_NAME_LEN] = {0};

  while (numOfRows < rows) {    
    pShow->pIter = mnodeGetNextSuperTable(pShow->pIter, &pTable);
    if (pTable == NULL) break;
    if (strncmp(pTable->info.tableId, prefix, prefixLen)) {
      mnodeDecTableRef(pTable);
      continue;
    }

    memset(stableName, 0, tListLen(stableName));
    mnodeExtractTableName(pTable->info.tableId, stableName);

    if (pShow->payloadLen > 0 && patternMatch(pShow->payload, stableName, sizeof(stableName) - 1, &info) != TSDB_PATTERN_MATCH) {
      mnodeDecTableRef(pTable);
      continue;
    }

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
  
    int16_t len = strnlen(stableName, TSDB_DB_NAME_LEN - 1);
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
    mnodeDecTableRef(pTable);
  }

  pShow->numOfReads += numOfRows;
  mnodeDecDbRef(pDb);

  return numOfRows;
}

void mnodeDropAllSuperTables(SDbObj *pDropDb) {
  void *  pIter= NULL;
  int32_t numOfTables = 0;
  int32_t dbNameLen = strlen(pDropDb->name);
  SSuperTableObj *pTable = NULL;

  mPrint("db:%s, all super tables will be dropped from sdb", pDropDb->name);

  while (1) {
    pIter = mnodeGetNextSuperTable(pIter, &pTable);
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

    mnodeDecTableRef(pTable);
  }

  sdbFreeIter(pIter);

  mPrint("db:%s, all super tables:%d is dropped from sdb", pDropDb->name, numOfTables);
}

static int32_t mnodeSetSchemaFromSuperTable(SSchema *pSchema, SSuperTableObj *pTable) {
  int32_t numOfCols = pTable->numOfColumns + pTable->numOfTags;
  assert(numOfCols <= TSDB_MAX_COLUMNS);
  
  for (int32_t i = 0; i < numOfCols; ++i) {
    tstrncpy(pSchema->name, pTable->schema[i].name, sizeof(pSchema->name));
    pSchema->type  = pTable->schema[i].type;
    pSchema->bytes = htons(pTable->schema[i].bytes);
    pSchema->colId = htons(pTable->schema[i].colId);
    pSchema++;
  }

  return (pTable->numOfColumns + pTable->numOfTags) * sizeof(SSchema);
}

static int32_t mnodeGetSuperTableMeta(SMnodeMsg *pMsg) {
  SSuperTableObj *pTable = (SSuperTableObj *)pMsg->pTable;
  STableMetaMsg *pMeta   = rpcMallocCont(sizeof(STableMetaMsg) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16));
  pMeta->uid          = htobe64(pTable->uid);
  pMeta->sversion     = htons(pTable->sversion);
  pMeta->tversion     = htons(pTable->tversion);
  pMeta->precision    = pMsg->pDb->cfg.precision;
  pMeta->numOfTags    = (uint8_t)pTable->numOfTags;
  pMeta->numOfColumns = htons((int16_t)pTable->numOfColumns);
  pMeta->tableType    = pTable->info.type;
  pMeta->contLen      = sizeof(STableMetaMsg) + mnodeSetSchemaFromSuperTable(pMeta->schema, pTable);
  strncpy(pMeta->tableId, pTable->info.tableId, TSDB_TABLE_ID_LEN);

  pMeta->contLen = htons(pMeta->contLen);

  pMsg->rpcRsp.rsp = pMeta;
  pMsg->rpcRsp.len = pMeta->contLen;
  
  mTrace("stable:%s, uid:%" PRIu64 " table meta is retrieved", pTable->info.tableId, pTable->uid);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeProcessSuperTableVgroupMsg(SMnodeMsg *pMsg) {
  SCMSTableVgroupMsg *pInfo = pMsg->rpcMsg.pCont;
  int32_t numOfTable = htonl(pInfo->numOfTables);

  // reserve space
  int32_t contLen = sizeof(SCMSTableVgroupRspMsg) + 32 * sizeof(SCMVgroupInfo) + sizeof(SVgroupsInfo); 
  for (int32_t i = 0; i < numOfTable; ++i) {
    char *stableName = (char*)pInfo + sizeof(SCMSTableVgroupMsg) + (TSDB_TABLE_ID_LEN) * i;
    SSuperTableObj *pTable = mnodeGetSuperTable(stableName);
    if (pTable != NULL && pTable->vgHash != NULL) {
      contLen += (taosHashGetSize(pTable->vgHash) * sizeof(SCMVgroupInfo) + sizeof(SVgroupsInfo));
    } 
    mnodeDecTableRef(pTable);
  }

  SCMSTableVgroupRspMsg *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  pRsp->numOfTables = 0;
  char *msg = (char *)pRsp + sizeof(SCMSTableVgroupRspMsg);

  for (int32_t i = 0; i < numOfTable; ++i) {
    char *          stableName = (char *)pInfo + sizeof(SCMSTableVgroupMsg) + (TSDB_TABLE_ID_LEN)*i;
    SSuperTableObj *pTable = mnodeGetSuperTable(stableName);
    if (pTable == NULL) {
      mError("stable:%s, not exist while get stable vgroup info", stableName);
      mnodeDecTableRef(pTable);
      continue;
    }
    if (pTable->vgHash == NULL) {
      mError("stable:%s, not vgroup exist while get stable vgroup info", stableName);
      mnodeDecTableRef(pTable);

      // even this super table has no corresponding table, still return
      pRsp->numOfTables++;

      SVgroupsInfo *pVgroupInfo = (SVgroupsInfo *)msg;
      pVgroupInfo->numOfVgroups = 0;
      
      msg += sizeof(SVgroupsInfo);
    } else {
      SVgroupsInfo *pVgroupInfo = (SVgroupsInfo *)msg;

      SHashMutableIterator *pIter = taosHashCreateIter(pTable->vgHash);
      int32_t               vgSize = 0;
      while (taosHashIterNext(pIter)) {
        int32_t *pVgId = taosHashIterGet(pIter);
        SVgObj * pVgroup = mnodeGetVgroup(*pVgId);
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
        mnodeDecVgroupRef(pVgroup);
      }

      taosHashDestroyIter(pIter);
      mnodeDecTableRef(pTable);

      pVgroupInfo->numOfVgroups = htonl(vgSize);

      // one table is done, try the next table
      msg += sizeof(SVgroupsInfo) + vgSize * sizeof(SCMVgroupInfo);
      pRsp->numOfTables++;
    }
  }

  if (pRsp->numOfTables != numOfTable) {
    rpcFreeCont(pRsp);
    return TSDB_CODE_MND_INVALID_TABLE_NAME;
  } else {
    pRsp->numOfTables = htonl(pRsp->numOfTables);
    pMsg->rpcRsp.rsp = pRsp;
    pMsg->rpcRsp.len = msg - (char *)pRsp;

    return TSDB_CODE_SUCCESS;
  }
}

static void mnodeProcessDropSuperTableRsp(SRpcMsg *rpcMsg) {
  mPrint("drop stable rsp received, result:%s", tstrerror(rpcMsg->code));
}

static void *mnodeBuildCreateChildTableMsg(SCMCreateTableMsg *pMsg, SChildTableObj *pTable) {
  STagData *  pTagData = NULL;
  int32_t tagDataLen = 0;
  int32_t totalCols = 0;
  int32_t contLen = 0;
  if (pTable->info.type == TSDB_CHILD_TABLE) {
    totalCols = pTable->superTable->numOfColumns + pTable->superTable->numOfTags;
    contLen = sizeof(SMDCreateTableMsg) + totalCols * sizeof(SSchema) + tagDataLen + pTable->sqlLen;
    if (pMsg != NULL) {
      pTagData = (STagData *)pMsg->schema;
      tagDataLen = ntohl(pTagData->dataLen);
      contLen += tagDataLen;
    }
  } else {
    totalCols = pTable->numOfColumns;
    contLen = sizeof(SMDCreateTableMsg) + totalCols * sizeof(SSchema) + pTable->sqlLen;
  }

  SMDCreateTableMsg *pCreate = rpcMallocCont(contLen);
  if (pCreate == NULL) {
    terrno = TSDB_CODE_MND_OUT_OF_MEMORY;
    return NULL;
  }

  mnodeExtractTableName(pTable->info.tableId, pCreate->tableId);
  pCreate->contLen       = htonl(contLen);
  pCreate->vgId          = htonl(pTable->vgId);
  pCreate->tableType     = pTable->info.type;
  pCreate->createdTime   = htobe64(pTable->createdTime);
  pCreate->sid           = htonl(pTable->sid);
  pCreate->sqlDataLen    = htonl(pTable->sqlLen);
  pCreate->uid           = htobe64(pTable->uid);
  
  if (pTable->info.type == TSDB_CHILD_TABLE) {
    mnodeExtractTableName(pTable->superTable->info.tableId, pCreate->superTableId);
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
  }

  if (pTable->info.type == TSDB_STREAM_TABLE) {
    memcpy(pCreate->data + totalCols * sizeof(SSchema), pTable->sql, pTable->sqlLen);
  }

  return pCreate;
}

static SChildTableObj* mnodeDoCreateChildTable(SCMCreateTableMsg *pCreate, SVgObj *pVgroup, int32_t tid) {
  SChildTableObj *pTable = calloc(1, sizeof(SChildTableObj));
  if (pTable == NULL) {
    mError("table:%s, failed to alloc memory", pCreate->tableId);
    terrno = TSDB_CODE_MND_OUT_OF_MEMORY;
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
    SSuperTableObj *pSuperTable = mnodeGetSuperTable(pTagData->name);
    if (pSuperTable == NULL) {
      mError("table:%s, corresponding super table:%s does not exist", pCreate->tableId, pTagData->name);
      mnodeDestroyChildTable(pTable);
      terrno = TSDB_CODE_MND_INVALID_TABLE_NAME;
      return NULL;
    }
    mnodeDecTableRef(pSuperTable);

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
      terrno = TSDB_CODE_MND_OUT_OF_MEMORY;
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
        terrno = TSDB_CODE_MND_OUT_OF_MEMORY;
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
    terrno = TSDB_CODE_MND_SDB_ERROR;
    return NULL;
  }

  mTrace("table:%s, create table in vgroup:%d, id:%d, uid:%" PRIu64 , pTable->info.tableId, pVgroup->vgId, pTable->sid, pTable->uid);
  return pTable;
}

static int32_t mnodeProcessCreateChildTableMsg(SMnodeMsg *pMsg) {
  SCMCreateTableMsg *pCreate = pMsg->rpcMsg.pCont;
  int32_t code = grantCheck(TSDB_GRANT_TIMESERIES);
  if (code != TSDB_CODE_SUCCESS) {
    mError("table:%s, failed to create, grant timeseries failed", pCreate->tableId);
    return code;
  }

  SVgObj *pVgroup = mnodeGetAvailableVgroup(pMsg->pDb);
  if (pVgroup == NULL) {
    mTrace("table:%s, start to create a new vgroup", pCreate->tableId);
    return mnodeCreateVgroup(pMsg, pMsg->pDb);
  }

  if (pMsg->retry == 0) {
    if (pMsg->pTable == NULL) {
      int32_t sid = taosAllocateId(pVgroup->idPool);
      if (sid <= 0) {
        mTrace("tables:%s, no enough sid in vgId:%d", pCreate->tableId, pVgroup->vgId);
        return mnodeCreateVgroup(pMsg, pMsg->pDb);
      }

      pMsg->pTable = (STableObj *)mnodeDoCreateChildTable(pCreate, pVgroup, sid);
      if (pMsg->pTable == NULL) {
        return terrno;
      }

      mnodeIncTableRef(pMsg->pTable);
    }
  } else {
    if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pCreate->tableId);
  }

  if (pMsg->pTable == NULL) {
    return terrno;
  }

  SMDCreateTableMsg *pMDCreate = mnodeBuildCreateChildTableMsg(pCreate, (SChildTableObj *)pMsg->pTable);
  if (pMDCreate == NULL) {
    return terrno;
  }

  SRpcIpSet ipSet = mnodeGetIpSetFromVgroup(pVgroup);
  SRpcMsg rpcMsg = {
      .handle  = pMsg,
      .pCont   = pMDCreate,
      .contLen = htonl(pMDCreate->contLen),
      .code    = 0,
      .msgType = TSDB_MSG_TYPE_MD_CREATE_TABLE
  };

  dnodeSendMsgToDnode(&ipSet, &rpcMsg);

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mnodeProcessDropChildTableMsg(SMnodeMsg *pMsg) {
  SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;
  if (pMsg->pVgroup == NULL) pMsg->pVgroup = mnodeGetVgroup(pTable->vgId);
  if (pMsg->pVgroup == NULL) {
    mError("table:%s, failed to drop ctable, vgroup not exist", pTable->info.tableId);
    return TSDB_CODE_MND_APP_ERROR;
  }

  SMDDropTableMsg *pDrop = rpcMallocCont(sizeof(SMDDropTableMsg));
  if (pDrop == NULL) {
    mError("table:%s, failed to drop ctable, no enough memory", pTable->info.tableId);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  strcpy(pDrop->tableId, pTable->info.tableId);
  pDrop->vgId    = htonl(pTable->vgId);
  pDrop->contLen = htonl(sizeof(SMDDropTableMsg));
  pDrop->sid     = htonl(pTable->sid);
  pDrop->uid     = htobe64(pTable->uid);

  SRpcIpSet ipSet = mnodeGetIpSetFromVgroup(pMsg->pVgroup);

  mPrint("table:%s, send drop ctable msg", pDrop->tableId);
  SRpcMsg rpcMsg = {
    .handle  = pMsg,
    .pCont   = pDrop,
    .contLen = sizeof(SMDDropTableMsg),
    .code    = 0,
    .msgType = TSDB_MSG_TYPE_MD_DROP_TABLE
  };

  dnodeSendMsgToDnode(&ipSet, &rpcMsg);

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mnodeModifyChildTableTagValue(SChildTableObj *pTable, char *tagName, char *nContent) {
  return TSDB_CODE_COM_OPS_NOT_SUPPORT;
}

static int32_t mnodeFindNormalTableColumnIndex(SChildTableObj *pTable, char *colName) {
  SSchema *schema = (SSchema *) pTable->schema;
  for (int32_t col = 0; col < pTable->numOfColumns; col++) {
    if (strcasecmp(schema[col].name, colName) == 0) {
      return col;
    }
  }

  return -1;
}

static int32_t mnodeAddNormalTableColumn(SDbObj *pDb, SChildTableObj *pTable, SSchema schema[], int32_t ncols) {
  if (ncols <= 0) {
    mError("table:%s, add column, ncols:%d <= 0", pTable->info.tableId, ncols);
    return TSDB_CODE_MND_APP_ERROR;
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (mnodeFindNormalTableColumnIndex(pTable, schema[i].name) > 0) {
      mError("table:%s, add column, column:%s already exist", pTable->info.tableId, schema[i].name);
      return TSDB_CODE_MND_FIELD_ALREAY_EXIST;
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
  
  SAcctObj *pAcct = mnodeGetAcct(pDb->acct);
  if (pAcct != NULL) {
    pAcct->acctInfo.numOfTimeSeries += ncols;
    mnodeDecAcctRef(pAcct);
  }
 
  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsChildTableSdb,
    .pObj = pTable
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_MND_SDB_ERROR;
  }
  
  mPrint("table %s, succeed to add column", pTable->info.tableId);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeDropNormalTableColumn(SDbObj *pDb, SChildTableObj *pTable, char *colName) {
  int32_t col = mnodeFindNormalTableColumnIndex(pTable, colName);
  if (col <= 0) {
    mError("table:%s, drop column, column:%s not exist", pTable->info.tableId, colName);
    return TSDB_CODE_MND_FIELD_NOT_EXIST;
  }

  memmove(pTable->schema + col, pTable->schema + col + 1, sizeof(SSchema) * (pTable->numOfColumns - col - 1));
  pTable->numOfColumns--;
  pTable->sversion++;

  SAcctObj *pAcct = mnodeGetAcct(pDb->acct);
  if (pAcct != NULL) {
    pAcct->acctInfo.numOfTimeSeries--;
    mnodeDecAcctRef(pAcct);
  }
 
  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsChildTableSdb,
    .pObj = pTable
  };

  int32_t code = sdbUpdateRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    return TSDB_CODE_MND_SDB_ERROR;
  }
  
  mPrint("table %s, succeed to drop column %s", pTable->info.tableId, colName);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeSetSchemaFromNormalTable(SSchema *pSchema, SChildTableObj *pTable) {
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

static int32_t mnodeDoGetChildTableMeta(SMnodeMsg *pMsg, STableMetaMsg *pMeta) {
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
    pMeta->contLen      = sizeof(STableMetaMsg) + mnodeSetSchemaFromSuperTable(pMeta->schema, pTable->superTable);
  } else {
    pMeta->sversion     = htons(pTable->sversion);
    pMeta->tversion     = 0;
    pMeta->numOfTags    = 0;
    pMeta->numOfColumns = htons((int16_t)pTable->numOfColumns);
    pMeta->contLen      = sizeof(STableMetaMsg) + mnodeSetSchemaFromNormalTable(pMeta->schema, pTable); 
  }
  
  if (pMsg->pVgroup == NULL) pMsg->pVgroup = mnodeGetVgroup(pTable->vgId);
  if (pMsg->pVgroup == NULL) {
    mError("table:%s, failed to get table meta, vgroup not exist", pTable->info.tableId);
    return TSDB_CODE_MND_VGROUP_NOT_EXIST;
  }

  for (int32_t i = 0; i < pMsg->pVgroup->numOfVnodes; ++i) {
    SDnodeObj *pDnode = mnodeGetDnode(pMsg->pVgroup->vnodeGid[i].dnodeId);
    if (pDnode == NULL) break;
    strcpy(pMeta->vgroup.ipAddr[i].fqdn, pDnode->dnodeFqdn);
    pMeta->vgroup.ipAddr[i].port = htons(pDnode->dnodePort + TSDB_PORT_DNODESHELL);
    pMeta->vgroup.numOfIps++;
    mnodeDecDnodeRef(pDnode);
  }
  pMeta->vgroup.vgId = htonl(pMsg->pVgroup->vgId);

  mTrace("table:%s, uid:%" PRIu64 " table meta is retrieved", pTable->info.tableId, pTable->uid);

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeAutoCreateChildTable(SMnodeMsg *pMsg) {
  SCMTableInfoMsg *pInfo = pMsg->rpcMsg.pCont;
  STagData *pTag = (STagData *)pInfo->tags;

  int32_t contLen = sizeof(SCMCreateTableMsg) + offsetof(STagData, data) + ntohl(pTag->dataLen);
  SCMCreateTableMsg *pCreateMsg = rpcMallocCont(contLen);
  if (pCreateMsg == NULL) {
    mError("table:%s, failed to create table while get meta info, no enough memory", pInfo->tableId);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  tstrncpy(pCreateMsg->tableId, pInfo->tableId, sizeof(pInfo->tableId));
  tstrncpy(pCreateMsg->db, pMsg->pDb->name, sizeof(pCreateMsg->db));
  pCreateMsg->igExists = 1;
  pCreateMsg->getMeta = 1;
  pCreateMsg->contLen = htonl(contLen);

  memcpy(pCreateMsg->schema, pInfo->tags, contLen - sizeof(SCMCreateTableMsg));
  mTrace("table:%s, start to create on demand, stable:%s", pInfo->tableId, ((STagData *)(pCreateMsg->schema))->name);

  rpcFreeCont(pMsg->rpcMsg.pCont);
  pMsg->rpcMsg.msgType = TSDB_MSG_TYPE_CM_CREATE_TABLE;
  pMsg->rpcMsg.pCont = pCreateMsg;
  pMsg->rpcMsg.contLen = contLen;
  
  return TSDB_CODE_MND_ACTION_NEED_REPROCESSED;
}

static int32_t mnodeGetChildTableMeta(SMnodeMsg *pMsg) {
  STableMetaMsg *pMeta = rpcMallocCont(sizeof(STableMetaMsg) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16));
  if (pMeta == NULL) {
    mError("table:%s, failed to get table meta, no enough memory", pMsg->pTable->tableId);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  mnodeDoGetChildTableMeta(pMsg, pMeta);

  pMsg->rpcRsp.len = pMeta->contLen;
  pMsg->rpcRsp.rsp = pMeta;
  pMeta->contLen = htons(pMeta->contLen);

  return TSDB_CODE_SUCCESS;
}

void mnodeDropAllChildTablesInVgroups(SVgObj *pVgroup) {
  void *  pIter = NULL;
  int32_t numOfTables = 0;
  SChildTableObj *pTable = NULL;

  mPrint("vgId:%d, all child tables will be dropped from sdb", pVgroup->vgId);

  while (1) {
    pIter = mnodeGetNextChildTable(pIter, &pTable);
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
    mnodeDecTableRef(pTable);
  }

  sdbFreeIter(pIter);

  mPrint("vgId:%d, all child tables is dropped from sdb", pVgroup->vgId);
}

void mnodeDropAllChildTables(SDbObj *pDropDb) {
  void *  pIter = NULL;
  int32_t numOfTables = 0;
  int32_t dbNameLen = strlen(pDropDb->name);
  SChildTableObj *pTable = NULL;

  mPrint("db:%s, all child tables will be dropped from sdb", pDropDb->name);

  while (1) {
    pIter = mnodeGetNextChildTable(pIter, &pTable);
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
    mnodeDecTableRef(pTable);
  }

  sdbFreeIter(pIter);

  mPrint("db:%s, all child tables:%d is dropped from sdb", pDropDb->name, numOfTables);
}

static void mnodeDropAllChildTablesInStable(SSuperTableObj *pStable) {
  void *  pIter = NULL;
  int32_t numOfTables = 0;
  SChildTableObj *pTable = NULL;

  mPrint("stable:%s, all child tables will dropped from sdb", pStable->info.tableId);

  while (1) {
    pIter = mnodeGetNextChildTable(pIter, &pTable);
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

    mnodeDecTableRef(pTable);
  }

  sdbFreeIter(pIter);

  mPrint("stable:%s, all child tables:%d is dropped from sdb", pStable->info.tableId, numOfTables);
}

static SChildTableObj* mnodeGetTableByPos(int32_t vnode, int32_t sid) {
  SVgObj *pVgroup = mnodeGetVgroup(vnode);
  if (pVgroup == NULL) return NULL;

  SChildTableObj *pTable = pVgroup->tableList[sid - 1];
  mnodeIncTableRef((STableObj *)pTable);

  mnodeDecVgroupRef(pVgroup);
  return pTable;
}

static int32_t mnodeProcessTableCfgMsg(SMnodeMsg *pMsg) {
  SDMConfigTableMsg *pCfg = pMsg->rpcMsg.pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  pCfg->vgId = htonl(pCfg->vgId);
  pCfg->sid = htonl(pCfg->sid);
  mTrace("dnode:%d, vgId:%d sid:%d, receive table config msg", pCfg->dnodeId, pCfg->vgId, pCfg->sid);

  SChildTableObj *pTable = mnodeGetTableByPos(pCfg->vgId, pCfg->sid);
  if (pTable == NULL) {
    mError("dnode:%d, vgId:%d sid:%d, table not found", pCfg->dnodeId, pCfg->vgId, pCfg->sid);
    return TSDB_CODE_MND_INVALID_TABLE_ID;
  }

  SMDCreateTableMsg *pCreate = NULL;
  pCreate = mnodeBuildCreateChildTableMsg(NULL, (SChildTableObj *)pTable);
  mnodeDecTableRef(pTable);
    
  if (pCreate == NULL) return terrno;
  
  pMsg->rpcRsp.rsp = pCreate;
  pMsg->rpcRsp.len = htonl(pCreate->contLen);
  return TSDB_CODE_SUCCESS;
}

// handle drop child response
static void mnodeProcessDropChildTableRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->handle == NULL) return;

  SMnodeMsg *mnodeMsg = rpcMsg->handle;
  mnodeMsg->received++;

  SChildTableObj *pTable = (SChildTableObj *)mnodeMsg->pTable;
  assert(pTable);
  mPrint("table:%s, drop table rsp received, thandle:%p result:%s", pTable->info.tableId, mnodeMsg->rpcMsg.handle, tstrerror(rpcMsg->code));

  if (rpcMsg->code != TSDB_CODE_SUCCESS) {
    mError("table:%s, failed to drop in dnode, reason:%s", pTable->info.tableId, tstrerror(rpcMsg->code));
    dnodeSendRpcMnodeWriteRsp(mnodeMsg, rpcMsg->code);
    return;
  }

  if (mnodeMsg->pVgroup == NULL) mnodeMsg->pVgroup = mnodeGetVgroup(pTable->vgId);
  if (mnodeMsg->pVgroup == NULL) {
    mError("table:%s, failed to get vgroup", pTable->info.tableId);
    dnodeSendRpcMnodeWriteRsp(mnodeMsg, TSDB_CODE_MND_VGROUP_NOT_EXIST);
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
    dnodeSendRpcMnodeWriteRsp(mnodeMsg, TSDB_CODE_MND_SDB_ERROR);
    return;
  }

  if (mnodeMsg->pVgroup->numOfTables <= 0) {
    mPrint("vgId:%d, all tables is dropped, drop vgroup", mnodeMsg->pVgroup->vgId);
    mnodeDropVgroup(mnodeMsg->pVgroup, NULL);
  }

  dnodeSendRpcMnodeWriteRsp(mnodeMsg, TSDB_CODE_SUCCESS);
}

// handle create table response from dnode
// if failed, drop the table cached
static void mnodeProcessCreateChildTableRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->handle == NULL) return;

  SMnodeMsg *mnodeMsg = rpcMsg->handle;
  mnodeMsg->received++;

  SChildTableObj *pTable = (SChildTableObj *)mnodeMsg->pTable;
  assert(pTable);
  mTrace("table:%s, create table rsp received, thandle:%p result:%s", pTable->info.tableId, mnodeMsg->rpcMsg.handle,
         tstrerror(rpcMsg->code));

  if (rpcMsg->code != TSDB_CODE_SUCCESS) {
    if (mnodeMsg->retry++ < 10) {
      mTrace("table:%s, create table rsp received, retry:%d thandle:%p result:%s", pTable->info.tableId,
             mnodeMsg->retry, mnodeMsg->rpcMsg.handle, tstrerror(rpcMsg->code));
      dnodeDelayReprocessMnodeWriteMsg(mnodeMsg);
    } else {
      mError("table:%s, failed to create in dnode, thandle:%p result:%s", pTable->info.tableId,
             mnodeMsg->rpcMsg.handle, tstrerror(rpcMsg->code));
      
      SSdbOper oper = {
        .type = SDB_OPER_GLOBAL,
        .table = tsChildTableSdb,
        .pObj = pTable
      };
      sdbDeleteRow(&oper);
    
      dnodeSendRpcMnodeWriteRsp(mnodeMsg, rpcMsg->code);
    }    
  } else {
    mTrace("table:%s, created in dnode, thandle:%p result:%s", pTable->info.tableId, mnodeMsg->rpcMsg.handle,
           tstrerror(rpcMsg->code));
    SCMCreateTableMsg *pCreate = mnodeMsg->rpcMsg.pCont;
    if (pCreate->getMeta) {
      mTrace("table:%s, continue to get meta", pTable->info.tableId);
      mnodeMsg->retry = 0;
      dnodeReprocessMnodeWriteMsg(mnodeMsg);
    } else {
      dnodeSendRpcMnodeWriteRsp(mnodeMsg, rpcMsg->code);
    }
  }
}

// not implemented yet
static void mnodeProcessAlterTableRsp(SRpcMsg *rpcMsg) {
  mTrace("alter table rsp received, handle:%p code:%s", rpcMsg->handle, tstrerror(rpcMsg->code));
}

static int32_t mnodeProcessMultiTableMetaMsg(SMnodeMsg *pMsg) {
  SCMMultiTableInfoMsg *pInfo = pMsg->rpcMsg.pCont;
  pInfo->numOfTables = htonl(pInfo->numOfTables);

  int32_t totalMallocLen = 4 * 1024 * 1024;  // first malloc 4 MB, subsequent reallocation as twice
  SMultiTableMeta *pMultiMeta = rpcMallocCont(totalMallocLen);
  if (pMultiMeta == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  pMultiMeta->contLen = sizeof(SMultiTableMeta);
  pMultiMeta->numOfTables = 0;

  for (int32_t t = 0; t < pInfo->numOfTables; ++t) {
    char * tableId = (char *)(pInfo->tableIds + t * TSDB_TABLE_ID_LEN + 1);
    SChildTableObj *pTable = mnodeGetChildTable(tableId);
    if (pTable == NULL) continue;

    if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDbByTableId(tableId);
    if (pMsg->pDb == NULL) {
      mnodeDecTableRef(pTable);
      continue;
    }

    int availLen = totalMallocLen - pMultiMeta->contLen;
    if (availLen <= sizeof(STableMetaMsg) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16)) {
      totalMallocLen *= 2;
      pMultiMeta = rpcReallocCont(pMultiMeta, totalMallocLen);
      if (pMultiMeta == NULL) {
        mnodeDecTableRef(pTable);
        return TSDB_CODE_MND_OUT_OF_MEMORY;
      } else {
        t--;
        mnodeDecTableRef(pTable);
        continue;
      }
    }

    STableMetaMsg *pMeta = (STableMetaMsg *)(pMultiMeta->metas + pMultiMeta->contLen);
    int32_t code = mnodeDoGetChildTableMeta(pMsg, pMeta);
    if (code == TSDB_CODE_SUCCESS) {
      pMultiMeta->numOfTables ++;
      pMultiMeta->contLen += pMeta->contLen;
    }

    mnodeDecTableRef(pTable);
  }

  pMsg->rpcRsp.rsp = pMultiMeta;
  pMsg->rpcRsp.len = pMultiMeta->contLen;

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeGetShowTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) return TSDB_CODE_MND_DB_NOT_SELECTED;

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = (TSDB_TABLE_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
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

  pShow->bytes[cols] = (TSDB_TABLE_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
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

  mnodeDecDbRef(pDb);
  return 0;
}

static int32_t mnodeRetrieveShowTables(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) return 0;

  int32_t numOfRows  = 0;
  SChildTableObj *pTable = NULL;
  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  char prefix[64] = {0};
  strcpy(prefix, pDb->name);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = strlen(prefix);

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextChildTable(pShow->pIter, &pTable);
    if (pTable == NULL) break;

    // not belong to current db
    if (strncmp(pTable->info.tableId, prefix, prefixLen)) {
      mnodeDecTableRef(pTable);
      continue;
    }

    char tableName[TSDB_TABLE_NAME_LEN] = {0};
    
    // pattern compare for table name
    mnodeExtractTableName(pTable->info.tableId, tableName);

    if (pShow->payloadLen > 0 && patternMatch(pShow->payload, tableName, sizeof(tableName) - 1, &info) != TSDB_PATTERN_MATCH) {
      mnodeDecTableRef(pTable);
      continue;
    }

    int32_t cols = 0;

    char *pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;

    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tableName, sizeof(tableName) - 1);
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
    
    memset(tableName, 0, sizeof(tableName));
    if (pTable->info.type == TSDB_CHILD_TABLE) {
      mnodeExtractTableName(pTable->superTable->info.tableId, tableName);
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tableName, sizeof(tableName) - 1);
    }
    
    cols++;

    numOfRows++;
    mnodeDecTableRef(pTable);
  }

  pShow->numOfReads += numOfRows;
  const int32_t NUM_OF_COLUMNS = 4;

  mnodeVacuumResult(data, NUM_OF_COLUMNS, numOfRows, rows, pShow);
  mnodeDecDbRef(pDb);

  return numOfRows;
}

static int32_t mnodeProcessAlterTableMsg(SMnodeMsg *pMsg) {
  SCMAlterTableMsg *pAlter = pMsg->rpcMsg.pCont;
  mTrace("table:%s, alter table msg is received from thandle:%p", pAlter->tableId, pMsg->rpcMsg.handle);

  if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDbByTableId(pAlter->tableId);
  if (pMsg->pDb == NULL || pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("table:%s, failed to alter table, db not selected", pAlter->tableId);
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }

  if (mnodeCheckIsMonitorDB(pMsg->pDb->name, tsMonitorDbName)) {
    mError("table:%s, failed to alter table, its log db", pAlter->tableId);
    return TSDB_CODE_MND_MONITOR_DB_FORBIDDEN;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pAlter->tableId);
  if (pMsg->pTable == NULL) {
    mError("table:%s, failed to alter table, table not exist", pMsg->pTable->tableId);
    return TSDB_CODE_MND_INVALID_TABLE_NAME;
  }

  pAlter->type = htons(pAlter->type);
  pAlter->numOfCols = htons(pAlter->numOfCols);
  pAlter->tagValLen = htonl(pAlter->tagValLen);

  if (pAlter->numOfCols > 2) {
    mError("table:%s, error numOfCols:%d in alter table", pAlter->tableId, pAlter->numOfCols);
    return TSDB_CODE_MND_APP_ERROR;
  }

  for (int32_t i = 0; i < pAlter->numOfCols; ++i) {
    pAlter->schema[i].bytes = htons(pAlter->schema[i].bytes);
  }

  int32_t code = TSDB_CODE_COM_OPS_NOT_SUPPORT;
  if (pMsg->pTable->type == TSDB_SUPER_TABLE) {
    SSuperTableObj *pTable = (SSuperTableObj *)pMsg->pTable;
    mTrace("table:%s, start to alter stable", pAlter->tableId);
    if (pAlter->type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN) {
      code = mnodeAddSuperTableTag(pTable, pAlter->schema, 1);
    } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN) {
      code = mnodeDropSuperTableTag(pTable, pAlter->schema[0].name);
    } else if (pAlter->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN) {
      code = mnodeModifySuperTableTagName(pTable, pAlter->schema[0].name, pAlter->schema[1].name);
    } else if (pAlter->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
      code = mnodeAddSuperTableColumn(pMsg->pDb, pTable, pAlter->schema, 1);
    } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
      code = mnodeDropSuperTableColumn(pMsg->pDb, pTable, pAlter->schema[0].name);
    } else {
    }
  } else {
    mTrace("table:%s, start to alter ctable", pAlter->tableId);
    SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;
    if (pAlter->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
      char *tagVal = (char*)(pAlter->schema + pAlter->numOfCols);
      code = mnodeModifyChildTableTagValue(pTable, pAlter->schema[0].name, tagVal);
    } else if (pAlter->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
      code = mnodeAddNormalTableColumn(pMsg->pDb, pTable, pAlter->schema, 1);
    } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
      code = mnodeDropNormalTableColumn(pMsg->pDb, pTable, pAlter->schema[0].name);
    } else {
    }
  }

  return code;
}

static int32_t mnodeGetStreamTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) return TSDB_CODE_MND_DB_NOT_SELECTED;

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = (TSDB_TABLE_NAME_LEN - 1) + VARSTR_HEADER_SIZE;
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

  pShow->bytes[cols] = TSDB_MAX_SQL_SHOW_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "sql");
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

  mnodeDecDbRef(pDb);
  return 0;
}

static int32_t mnodeRetrieveStreamTables(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) return 0;

  
  int32_t numOfRows  = 0;
  SChildTableObj *pTable = NULL;
  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  char prefix[64] = {0};
  strcpy(prefix, pDb->name);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = strlen(prefix);

  while (numOfRows < rows) {
    pShow->pIter = mnodeGetNextChildTable(pShow->pIter, &pTable);
    if (pTable == NULL) break;
    
    // not belong to current db
    if (strncmp(pTable->info.tableId, prefix, prefixLen) || pTable->info.type != TSDB_STREAM_TABLE) {
      mnodeDecTableRef(pTable);
      continue;
    }

    char tableName[TSDB_TABLE_NAME_LEN] = {0};
    
    // pattern compare for table name
    mnodeExtractTableName(pTable->info.tableId, tableName);

    if (pShow->payloadLen > 0 && patternMatch(pShow->payload, tableName, sizeof(tableName) - 1, &info) != TSDB_PATTERN_MATCH) {
      mnodeDecTableRef(pTable);
      continue;
    }

    int32_t cols = 0;

    char *pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;

    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tableName, sizeof(tableName) - 1);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *) pWrite = pTable->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pTable->numOfColumns;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pTable->sql, TSDB_MAX_SQL_SHOW_LEN);    
    cols++;

    numOfRows++;
    mnodeDecTableRef(pTable);
  }

  pShow->numOfReads += numOfRows;
  const int32_t NUM_OF_COLUMNS = 4;

  mnodeVacuumResult(data, NUM_OF_COLUMNS, numOfRows, rows, pShow);
  mnodeDecDbRef(pDb);

  return numOfRows;
}
