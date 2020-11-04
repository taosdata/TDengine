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
 
static int32_t mnodeProcessCreateTableMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessCreateSuperTableMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessCreateChildTableMsg(SMnodeMsg *pMsg);
static void    mnodeProcessCreateChildTableRsp(SRpcMsg *rpcMsg);

static int32_t mnodeProcessDropTableMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessDropSuperTableMsg(SMnodeMsg *pMsg);
static void    mnodeProcessDropSuperTableRsp(SRpcMsg *rpcMsg);
static int32_t mnodeProcessDropChildTableMsg(SMnodeMsg *pMsg);
static void    mnodeProcessDropChildTableRsp(SRpcMsg *rpcMsg);

static int32_t mnodeProcessSuperTableVgroupMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessMultiTableMetaMsg(SMnodeMsg *pMsg);
static int32_t mnodeProcessTableCfgMsg(SMnodeMsg *pMsg);

static int32_t mnodeProcessTableMetaMsg(SMnodeMsg *pMsg);
static int32_t mnodeGetSuperTableMeta(SMnodeMsg *pMsg);
static int32_t mnodeGetChildTableMeta(SMnodeMsg *pMsg);
static int32_t mnodeAutoCreateChildTable(SMnodeMsg *pMsg);

static int32_t mnodeProcessAlterTableMsg(SMnodeMsg *pMsg);
static void    mnodeProcessAlterTableRsp(SRpcMsg *rpcMsg);

static int32_t mnodeFindSuperTableColumnIndex(SSuperTableObj *pStable, char *colName);

static void mnodeDestroyChildTable(SChildTableObj *pTable) {
  taosTFree(pTable->info.tableId);
  taosTFree(pTable->schema);
  taosTFree(pTable->sql);
  taosTFree(pTable);
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
  }

  SDbObj *pDb = NULL;
  if (pVgroup != NULL) {
    pDb = mnodeGetDb(pVgroup->dbName);
    if (pDb == NULL) {
      mError("ctable:%s, vgId:%d not in db:%s", pTable->info.tableId, pVgroup->vgId, pVgroup->dbName);
    }
  }

  SAcctObj *pAcct = NULL;
  if (pDb != NULL) {
    pAcct = mnodeGetAcct(pDb->acct);
    if (pAcct == NULL) {
      mError("ctable:%s, acct:%s not exists", pTable->info.tableId, pDb->acct);
    }
  }

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    // add ref
    pTable->superTable = mnodeGetSuperTableByUid(pTable->suid);
    if (pTable->superTable != NULL) {
      mnodeAddTableIntoStable(pTable->superTable, pTable);
      grantAdd(TSDB_GRANT_TIMESERIES, pTable->superTable->numOfColumns - 1);
      if (pAcct) pAcct->acctInfo.numOfTimeSeries += (pTable->superTable->numOfColumns - 1);
    } else {
      mError("table:%s:%p, correspond stable not found suid:%" PRIu64, pTable->info.tableId, pTable, pTable->suid);
    }
  } else {
    grantAdd(TSDB_GRANT_TIMESERIES, pTable->numOfColumns - 1);
    if (pAcct) pAcct->acctInfo.numOfTimeSeries += (pTable->numOfColumns - 1);
  }

  if (pDb) mnodeAddTableIntoDb(pDb);
  if (pVgroup) mnodeAddTableIntoVgroup(pVgroup, pTable);

  mnodeDecVgroupRef(pVgroup);
  mnodeDecDbRef(pDb);
  mnodeDecAcctRef(pAcct);

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
    if (pTable->superTable) {
      grantRestore(TSDB_GRANT_TIMESERIES, pTable->superTable->numOfColumns - 1);
      if (pAcct != NULL) pAcct->acctInfo.numOfTimeSeries -= (pTable->superTable->numOfColumns - 1);
      mnodeRemoveTableFromStable(pTable->superTable, pTable);
      mnodeDecTableRef(pTable->superTable);
    }
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
    void *oldSTable = pTable->superTable;
    int32_t oldRefCount = pTable->refCount;
    
    memcpy(pTable, pNew, sizeof(SChildTableObj));
    
    pTable->refCount = oldRefCount;
    pTable->sql = pNew->sql;
    pTable->schema = pNew->schema;
    pTable->superTable = oldSTable;
    
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
  if (len >= TSDB_TABLE_FNAME_LEN) return TSDB_CODE_MND_INVALID_TABLE_ID;

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
  if (len >= TSDB_TABLE_FNAME_LEN) {
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
    if (pDb == NULL || pDb->status != TSDB_DB_STATUS_READY) {
      mError("ctable:%s, failed to get db or db in dropping, discard it", pTable->info.tableId);
      SSdbOper desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .table = tsChildTableSdb};
      sdbDeleteRow(&desc);
      mnodeDecTableRef(pTable);
      mnodeDecDbRef(pDb);
      continue;
    }
    mnodeDecDbRef(pDb);

    SVgObj *pVgroup = mnodeGetVgroup(pTable->vgId);
    if (pVgroup == NULL) {
      mError("ctable:%s, failed to get vgId:%d tid:%d, discard it", pTable->info.tableId, pTable->vgId, pTable->tid);
      pTable->vgId = 0;
      SSdbOper desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .table = tsChildTableSdb};
      sdbDeleteRow(&desc);
      mnodeDecTableRef(pTable);
      continue;
    }
    mnodeDecVgroupRef(pVgroup);

    if (strcmp(pVgroup->dbName, pDb->name) != 0) {
      mError("ctable:%s, db:%s not match with vgId:%d db:%s sid:%d, discard it",
             pTable->info.tableId, pDb->name, pTable->vgId, pVgroup->dbName, pTable->tid);
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
    .maxRowSize   = sizeof(SChildTableObj) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16) + TSDB_TABLE_FNAME_LEN + TSDB_CQ_SQL_SIZE,
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

  mDebug("table:ctables is created");
  return 0;
}

static void mnodeCleanupChildTables() {
  sdbCloseTable(tsChildTableSdb);
  tsChildTableSdb = NULL;
}

int64_t mnodeGetSuperTableNum() {
  return sdbGetNumOfRows(tsSuperTableSdb);
}

int64_t mnodeGetChildTableNum() {
  return sdbGetNumOfRows(tsChildTableSdb);
}

static void mnodeAddTableIntoStable(SSuperTableObj *pStable, SChildTableObj *pCtable) {
  atomic_add_fetch_32(&pStable->numOfTables, 1);

  if (pStable->vgHash == NULL) {
    pStable->vgHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, false);
  }

  if (pStable->vgHash != NULL) {
    if (taosHashGet(pStable->vgHash, &pCtable->vgId, sizeof(pCtable->vgId)) == NULL) {
      taosHashPut(pStable->vgHash, &pCtable->vgId, sizeof(pCtable->vgId), &pCtable->vgId, sizeof(pCtable->vgId));
      mDebug("table:%s, vgId:%d is put into stable vgList, sizeOfVgList:%d", pStable->info.tableId, pCtable->vgId,
             (int32_t)taosHashGetSize(pStable->vgHash));
    }
  }
}

static void mnodeRemoveTableFromStable(SSuperTableObj *pStable, SChildTableObj *pCtable) {
  atomic_sub_fetch_32(&pStable->numOfTables, 1);

  if (pStable->vgHash == NULL) return;

  SVgObj *pVgroup = mnodeGetVgroup(pCtable->vgId);
  if (pVgroup == NULL) {
    taosHashRemove(pStable->vgHash, (char *)&pCtable->vgId, sizeof(pCtable->vgId));
    mDebug("table:%s, vgId:%d is remove from stable vgList, sizeOfVgList:%d", pStable->info.tableId, pCtable->vgId,
           (int32_t)taosHashGetSize(pStable->vgHash));
  }
  mnodeDecVgroupRef(pVgroup);
}

static void mnodeDestroySuperTable(SSuperTableObj *pStable) {
  if (pStable->vgHash != NULL) {
    taosHashCleanup(pStable->vgHash);
    pStable->vgHash = NULL;
  }
  taosTFree(pStable->info.tableId);
  taosTFree(pStable->schema);
  taosTFree(pStable);
}

static int32_t mnodeSuperTableActionDestroy(SSdbOper *pOper) {
  mnodeDestroySuperTable(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeSuperTableActionInsert(SSdbOper *pOper) {
  SSuperTableObj *pStable = pOper->pObj;
  SDbObj *pDb = mnodeGetDbByTableId(pStable->info.tableId);
  if (pDb != NULL && pDb->status == TSDB_DB_STATUS_READY) {
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
  if (pTable != NULL && pTable != pNew) {
    void *oldTableId = pTable->info.tableId;
    void *oldSchema = pTable->schema;
    void *oldVgHash = pTable->vgHash;
    int32_t oldRefCount = pTable->refCount;
    int32_t oldNumOfTables = pTable->numOfTables;

    memcpy(pTable, pNew, sizeof(SSuperTableObj));

    pTable->vgHash = oldVgHash;
    pTable->refCount = oldRefCount;
    pTable->schema = pNew->schema;
    pTable->numOfTables = oldNumOfTables;
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
  if (len >= TSDB_TABLE_FNAME_LEN) len = TSDB_CODE_MND_INVALID_TABLE_ID;

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
  if (len >= TSDB_TABLE_FNAME_LEN){
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
    .maxRowSize   = sizeof(SSuperTableObj) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16) + TSDB_TABLE_FNAME_LEN,
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

  mDebug("table:stables is created");
  return 0;
}

static void mnodeCleanupSuperTables() {
  sdbCloseTable(tsSuperTableSdb);
  tsSuperTableSdb = NULL;
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
  if (pMsg->pDb == NULL) {
    mError("app:%p:%p, table:%s, failed to create, db not selected", pMsg->rpcMsg.ahandle, pMsg, pCreate->tableId);
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }
  
  if (pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pMsg->pDb->name, pMsg->pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pCreate->tableId);
  if (pMsg->pTable != NULL && pMsg->retry == 0) {
    if (pCreate->getMeta) {
      mDebug("app:%p:%p, table:%s, continue to get meta", pMsg->rpcMsg.ahandle, pMsg, pCreate->tableId);
      return mnodeGetChildTableMeta(pMsg);
    } else if (pCreate->igExists) {
      mDebug("app:%p:%p, table:%s, is already exist", pMsg->rpcMsg.ahandle, pMsg, pCreate->tableId);
      return TSDB_CODE_SUCCESS;
    } else {
      mError("app:%p:%p, table:%s, failed to create, table already exist", pMsg->rpcMsg.ahandle, pMsg,
             pCreate->tableId);
      return TSDB_CODE_MND_TABLE_ALREADY_EXIST;
    }
  }

  if (pCreate->numOfTags != 0) {
    mDebug("app:%p:%p, table:%s, create stable msg is received from thandle:%p", pMsg->rpcMsg.ahandle, pMsg,
           pCreate->tableId, pMsg->rpcMsg.handle);
    return mnodeProcessCreateSuperTableMsg(pMsg);
  } else {
    mDebug("app:%p:%p, table:%s, create ctable msg is received from thandle:%p", pMsg->rpcMsg.ahandle, pMsg,
           pCreate->tableId, pMsg->rpcMsg.handle);
    return mnodeProcessCreateChildTableMsg(pMsg);
  }
}

static int32_t mnodeProcessDropTableMsg(SMnodeMsg *pMsg) {
  SCMDropTableMsg *pDrop = pMsg->rpcMsg.pCont;
  if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDbByTableId(pDrop->tableId);
  if (pMsg->pDb == NULL) {
    mError("app:%p:%p, table:%s, failed to drop table, db not selected or db in dropping", pMsg->rpcMsg.ahandle, pMsg, pDrop->tableId);
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }
  
  if (pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pMsg->pDb->name, pMsg->pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  if (mnodeCheckIsMonitorDB(pMsg->pDb->name, tsMonitorDbName)) {
    mError("app:%p:%p, table:%s, failed to drop table, in monitor database", pMsg->rpcMsg.ahandle, pMsg,
           pDrop->tableId);
    return TSDB_CODE_MND_MONITOR_DB_FORBIDDEN;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pDrop->tableId);
  if (pMsg->pTable == NULL) {
    if (pDrop->igNotExists) {
      mDebug("app:%p:%p, table:%s, table is not exist, treat as success", pMsg->rpcMsg.ahandle, pMsg, pDrop->tableId);
      return TSDB_CODE_SUCCESS;
    } else {
      mError("app:%p:%p, table:%s, failed to drop table, table not exist", pMsg->rpcMsg.ahandle, pMsg, pDrop->tableId);
      return TSDB_CODE_MND_INVALID_TABLE_NAME;
    }
  }

  if (pMsg->pTable->type == TSDB_SUPER_TABLE) {
    SSuperTableObj *pSTable = (SSuperTableObj *)pMsg->pTable;
    mInfo("app:%p:%p, table:%s, start to drop stable, uid:%" PRIu64 ", numOfChildTables:%d, sizeOfVgList:%d",
          pMsg->rpcMsg.ahandle, pMsg, pDrop->tableId, pSTable->uid, pSTable->numOfTables, (int32_t)taosHashGetSize(pSTable->vgHash));
    return mnodeProcessDropSuperTableMsg(pMsg);
  } else {
    SChildTableObj *pCTable = (SChildTableObj *)pMsg->pTable;
    mInfo("app:%p:%p, table:%s, start to drop ctable, vgId:%d tid:%d uid:%" PRIu64, pMsg->rpcMsg.ahandle, pMsg,
          pDrop->tableId, pCTable->vgId, pCTable->tid, pCTable->uid);
    return mnodeProcessDropChildTableMsg(pMsg);
  }
}

static int32_t mnodeProcessTableMetaMsg(SMnodeMsg *pMsg) {
  STableInfoMsg *pInfo = pMsg->rpcMsg.pCont;
  pInfo->createFlag = htons(pInfo->createFlag);
  mDebug("app:%p:%p, table:%s, table meta msg is received from thandle:%p, createFlag:%d", pMsg->rpcMsg.ahandle, pMsg,
         pInfo->tableId, pMsg->rpcMsg.handle, pInfo->createFlag);

  if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDbByTableId(pInfo->tableId);
  if (pMsg->pDb == NULL) {
    mError("app:%p:%p, table:%s, failed to get table meta, db not selected", pMsg->rpcMsg.ahandle, pMsg,
           pInfo->tableId);
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }
  
  if (pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pMsg->pDb->name, pMsg->pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pInfo->tableId);
  if (pMsg->pTable == NULL) {
    if (!pInfo->createFlag) {
      mError("app:%p:%p, table:%s, failed to get table meta, table not exist", pMsg->rpcMsg.ahandle, pMsg,
             pInfo->tableId);
      return TSDB_CODE_MND_INVALID_TABLE_NAME;
    } else {
      mDebug("app:%p:%p, table:%s, failed to get table meta, start auto create table ", pMsg->rpcMsg.ahandle, pMsg,
             pInfo->tableId);
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

static int32_t mnodeCreateSuperTableCb(SMnodeMsg *pMsg, int32_t code) {
  SSuperTableObj *pTable = (SSuperTableObj *)pMsg->pTable;
  assert(pTable);

  if (code == TSDB_CODE_SUCCESS) {
    mLInfo("stable:%s, is created in sdb, uid:%" PRIu64, pTable->info.tableId, pTable->uid);
  } else {
    mError("app:%p:%p, stable:%s, failed to create in sdb, reason:%s", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId,
           tstrerror(code));
    SSdbOper desc = {.type = SDB_OPER_GLOBAL, .pObj = pTable, .table = tsSuperTableSdb};
    sdbDeleteRow(&desc);
  }

  return code;
}

static int32_t mnodeProcessCreateSuperTableMsg(SMnodeMsg *pMsg) {
  if (pMsg == NULL) return TSDB_CODE_MND_APP_ERROR;

  SCMCreateTableMsg *pCreate = pMsg->rpcMsg.pCont;
  SSuperTableObj *   pStable = calloc(1, sizeof(SSuperTableObj));
  if (pStable == NULL) {
    mError("app:%p:%p, table:%s, failed to create, no enough memory", pMsg->rpcMsg.ahandle, pMsg, pCreate->tableId);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  int64_t us = taosGetTimestampUs();
  pStable->info.tableId = strdup(pCreate->tableId);
  pStable->info.type    = TSDB_SUPER_TABLE;
  pStable->createdTime  = taosGetTimestampMs();
  pStable->uid          = (us << 24) + ((sdbGetVersion() & ((1ul << 16) - 1ul)) << 8) + (taosRand() & ((1ul << 8) - 1ul));
  pStable->sversion     = 0;
  pStable->tversion     = 0;
  pStable->numOfColumns = htons(pCreate->numOfColumns);
  pStable->numOfTags    = htons(pCreate->numOfTags);

  int32_t numOfCols = pStable->numOfColumns + pStable->numOfTags;
  int32_t schemaSize = numOfCols * sizeof(SSchema);
  pStable->schema = (SSchema *)calloc(1, schemaSize);
  if (pStable->schema == NULL) {
    free(pStable);
    mError("app:%p:%p, table:%s, failed to create, no schema input", pMsg->rpcMsg.ahandle, pMsg, pCreate->tableId);
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

  pMsg->pTable = (STableObj *)pStable;
  mnodeIncTableRef(pMsg->pTable);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable,
    .rowSize = sizeof(SSuperTableObj) + schemaSize,
    .pMsg = pMsg,
    .writeCb = mnodeCreateSuperTableCb
  };

  int32_t code = sdbInsertRow(&oper);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mnodeDestroySuperTable(pStable);
    pMsg->pTable = NULL;
    mError("app:%p:%p, table:%s, failed to create, sdb error", pMsg->rpcMsg.ahandle, pMsg, pCreate->tableId);
  }

  return code;
}

static int32_t mnodeDropSuperTableCb(SMnodeMsg *pMsg, int32_t code) {
  SSuperTableObj *pTable = (SSuperTableObj *)pMsg->pTable;
  if (code != TSDB_CODE_SUCCESS) {
    mError("app:%p:%p, stable:%s, failed to drop, sdb error", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId);
  } else {
    mLInfo("app:%p:%p, stable:%s, is dropped from sdb", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId);
  }

  return code;
}

static int32_t mnodeProcessDropSuperTableMsg(SMnodeMsg *pMsg) {
  if (pMsg == NULL) return TSDB_CODE_MND_APP_ERROR;

  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
   if (pStable->vgHash != NULL /*pStable->numOfTables != 0*/) {
    SHashMutableIterator *pIter = taosHashCreateIter(pStable->vgHash);
    while (taosHashIterNext(pIter)) {
      int32_t *pVgId = taosHashIterGet(pIter);
      SVgObj *pVgroup = mnodeGetVgroup(*pVgId);
      if (pVgroup == NULL) break;

      SDropSTableMsg *pDrop = rpcMallocCont(sizeof(SDropSTableMsg));
      pDrop->contLen = htonl(sizeof(SDropSTableMsg));
      pDrop->vgId = htonl(pVgroup->vgId);
      pDrop->uid = htobe64(pStable->uid);
      mnodeExtractTableName(pStable->info.tableId, pDrop->tableId);

      mInfo("app:%p:%p, stable:%s, send drop stable msg to vgId:%d", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
             pVgroup->vgId);
      SRpcEpSet epSet = mnodeGetEpSetFromVgroup(pVgroup);
      SRpcMsg   rpcMsg = {.pCont = pDrop, .contLen = sizeof(SDropSTableMsg), .msgType = TSDB_MSG_TYPE_MD_DROP_STABLE};
      dnodeSendMsgToDnode(&epSet, &rpcMsg);
      mnodeDecVgroupRef(pVgroup);
    }
    taosHashDestroyIter(pIter);

    mnodeDropAllChildTablesInStable(pStable);
  } 
  
  SSdbOper oper = {
    .type  = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj  = pStable,
    .pMsg  = pMsg,
    .writeCb = mnodeDropSuperTableCb
  };

  int32_t code = sdbDeleteRow(&oper);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("app:%p:%p, table:%s, failed to drop, reason:%s", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
           tstrerror(code));
  }

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

static int32_t mnodeAddSuperTableTagCb(SMnodeMsg *pMsg, int32_t code) {
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  mLInfo("app:%p:%p, stable %s, add tag result:%s", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
          tstrerror(code));

  return code;
}

static int32_t mnodeAddSuperTableTag(SMnodeMsg *pMsg, SSchema schema[], int32_t ntags) {
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  if (pStable->numOfTags + ntags > TSDB_MAX_TAGS) {
    mError("app:%p:%p, stable:%s, add tag, too many tags", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId);
    return TSDB_CODE_MND_TOO_MANY_TAGS;
  }

  for (int32_t i = 0; i < ntags; i++) {
    if (mnodeFindSuperTableColumnIndex(pStable, schema[i].name) > 0) {
      mError("app:%p:%p, stable:%s, add tag, column:%s already exist", pMsg->rpcMsg.ahandle, pMsg,
             pStable->info.tableId, schema[i].name);
      return TSDB_CODE_MND_TAG_ALREAY_EXIST;
    }

    if (mnodeFindSuperTableTagIndex(pStable, schema[i].name) > 0) {
      mError("app:%p:%p, stable:%s, add tag, tag:%s already exist", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
             schema[i].name);
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

  mInfo("app:%p:%p, stable %s, start to add tag %s", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
         schema[0].name);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable,
    .pMsg = pMsg,
    .writeCb = mnodeAddSuperTableTagCb
  };

  return sdbUpdateRow(&oper);
}

static int32_t mnodeDropSuperTableTagCb(SMnodeMsg *pMsg, int32_t code) {
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  mLInfo("app:%p:%p, stable %s, drop tag result:%s", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
          tstrerror(code));
  return code;
}

static int32_t mnodeDropSuperTableTag(SMnodeMsg *pMsg, char *tagName) {
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  int32_t col = mnodeFindSuperTableTagIndex(pStable, tagName);
  if (col < 0) {
    mError("app:%p:%p, stable:%s, drop tag, tag:%s not exist", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
           tagName);
    return TSDB_CODE_MND_TAG_NOT_EXIST;
  }

  memmove(pStable->schema + pStable->numOfColumns + col, pStable->schema + pStable->numOfColumns + col + 1,
          sizeof(SSchema) * (pStable->numOfTags - col - 1));
  pStable->numOfTags--;
  pStable->tversion++;

  mInfo("app:%p:%p, stable %s, start to drop tag %s", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId, tagName);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable,
    .pMsg = pMsg,
    .writeCb = mnodeDropSuperTableTagCb
  };

  return sdbUpdateRow(&oper);
}

static int32_t mnodeModifySuperTableTagNameCb(SMnodeMsg *pMsg, int32_t code) {
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  mLInfo("app:%p:%p, stable %s, modify tag result:%s", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
         tstrerror(code));
  return code;
}

static int32_t mnodeModifySuperTableTagName(SMnodeMsg *pMsg, char *oldTagName, char *newTagName) {
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  int32_t col = mnodeFindSuperTableTagIndex(pStable, oldTagName);
  if (col < 0) {
    mError("app:%p:%p, stable:%s, failed to modify table tag, oldName: %s, newName: %s", pMsg->rpcMsg.ahandle, pMsg,
           pStable->info.tableId, oldTagName, newTagName);
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

  mInfo("app:%p:%p, stable %s, start to modify tag %s to %s", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
         oldTagName, newTagName);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable,
    .pMsg = pMsg,
    .writeCb = mnodeModifySuperTableTagNameCb
  };

  return sdbUpdateRow(&oper);
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

static int32_t mnodeAddSuperTableColumnCb(SMnodeMsg *pMsg, int32_t code) {
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  mLInfo("app:%p:%p, stable %s, add column result:%s", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
          tstrerror(code));
  return code;
}

static int32_t mnodeAddSuperTableColumn(SMnodeMsg *pMsg, SSchema schema[], int32_t ncols) {
  SDbObj *pDb = pMsg->pDb;
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  if (ncols <= 0) {
    mError("app:%p:%p, stable:%s, add column, ncols:%d <= 0", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId, ncols);
    return TSDB_CODE_MND_APP_ERROR;
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (mnodeFindSuperTableColumnIndex(pStable, schema[i].name) > 0) {
      mError("app:%p:%p, stable:%s, add column, column:%s already exist", pMsg->rpcMsg.ahandle, pMsg,
             pStable->info.tableId, schema[i].name);
      return TSDB_CODE_MND_FIELD_ALREAY_EXIST;
    }

    if (mnodeFindSuperTableTagIndex(pStable, schema[i].name) > 0) {
      mError("app:%p:%p, stable:%s, add column, tag:%s already exist", pMsg->rpcMsg.ahandle, pMsg,
             pStable->info.tableId, schema[i].name);
      return TSDB_CODE_MND_TAG_ALREAY_EXIST;
    }
  }

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfTags + pStable->numOfColumns);
  pStable->schema = realloc(pStable->schema, schemaSize + sizeof(SSchema) * ncols);

  memmove(pStable->schema + pStable->numOfColumns + ncols, pStable->schema + pStable->numOfColumns,
          sizeof(SSchema) * pStable->numOfTags);
  memcpy(pStable->schema + pStable->numOfColumns, schema, sizeof(SSchema) * ncols);

  SSchema *tschema = (SSchema *) (pStable->schema + pStable->numOfColumns);
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

  mInfo("app:%p:%p, stable %s, start to add column", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable,
    .pMsg = pMsg,
    .writeCb = mnodeAddSuperTableColumnCb
  };

 return sdbUpdateRow(&oper);
}

static int32_t mnodeDropSuperTableColumnCb(SMnodeMsg *pMsg, int32_t code) {
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  mLInfo("app:%p:%p, stable %s, delete column result:%s", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
         tstrerror(code));
  return code;
}

static int32_t mnodeDropSuperTableColumn(SMnodeMsg *pMsg, char *colName) {
  SDbObj *pDb = pMsg->pDb;
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  int32_t col = mnodeFindSuperTableColumnIndex(pStable, colName);
  if (col <= 0) {
    mError("app:%p:%p, stable:%s, drop column, column:%s not exist", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
           colName);
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

  mInfo("app:%p:%p, stable %s, start to delete column", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable,
    .pMsg = pMsg,
    .writeCb = mnodeDropSuperTableColumnCb
  };

  return sdbUpdateRow(&oper);
}

static int32_t mnodeChangeSuperTableColumnCb(SMnodeMsg *pMsg, int32_t code) {
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  mLInfo("app:%p:%p, stable %s, change column result:%s", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
         tstrerror(code));
  return code;
}

static int32_t mnodeChangeSuperTableColumn(SMnodeMsg *pMsg, char *oldName, char *newName) {
  SSuperTableObj *pStable = (SSuperTableObj *)pMsg->pTable;
  int32_t col = mnodeFindSuperTableColumnIndex(pStable, oldName);
  if (col < 0) {
    mError("app:%p:%p, stable:%s, change column, oldName: %s, newName: %s", pMsg->rpcMsg.ahandle, pMsg,
           pStable->info.tableId, oldName, newName);
    return TSDB_CODE_MND_FIELD_NOT_EXIST;
  }

  // int32_t  rowSize = 0;
  uint32_t len = strlen(newName);
  if (len >= TSDB_COL_NAME_LEN) {
    return TSDB_CODE_MND_COL_NAME_TOO_LONG;
  }

  if (mnodeFindSuperTableColumnIndex(pStable, newName) >= 0) {
    return TSDB_CODE_MND_FIELD_ALREAY_EXIST;
  }
  
  // update
  SSchema *schema = (SSchema *) (pStable->schema + col);
  tstrncpy(schema->name, newName, sizeof(schema->name));

  mInfo("app:%p:%p, stable %s, start to modify column %s to %s", pMsg->rpcMsg.ahandle, pMsg, pStable->info.tableId,
         oldName, newName);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsSuperTableSdb,
    .pObj = pStable,
    .pMsg = pMsg,
    .writeCb = mnodeChangeSuperTableColumnCb
  };

  return sdbUpdateRow(&oper);
}

// show super tables
static int32_t mnodeGetShowSuperTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) return TSDB_CODE_MND_DB_NOT_SELECTED;
    
  if (pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pDb->name, pDb->status);
    mnodeDecDbRef(pDb);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  SSchema tbnameSchema = tGetTableNameColumnSchema();
  pShow->bytes[cols] = tbnameSchema.bytes;
  pSchema[cols].type = tbnameSchema.type;
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
  char            prefix[64] = {0};
  int32_t         prefixLen;

  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) return 0;
  
  if (pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pDb->name, pDb->status);
    mnodeDecDbRef(pDb);
    return 0;
  }

  tstrncpy(prefix, pDb->name, 64);
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
  
    int16_t len = strnlen(stableName, TSDB_TABLE_NAME_LEN - 1);
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

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  mnodeDecDbRef(pDb);

  return numOfRows;
}

void mnodeDropAllSuperTables(SDbObj *pDropDb) {
  void *  pIter= NULL;
  int32_t numOfTables = 0;
  SSuperTableObj *pTable = NULL;

  char prefix[64] = {0};
  tstrncpy(prefix, pDropDb->name, 64);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = strlen(prefix);

  mInfo("db:%s, all super tables will be dropped from sdb", pDropDb->name);

  while (1) {
    pIter = mnodeGetNextSuperTable(pIter, &pTable);
    if (pTable == NULL) break;

    if (strncmp(prefix, pTable->info.tableId, prefixLen) == 0) {
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

  mInfo("db:%s, all super tables:%d is dropped from sdb", pDropDb->name, numOfTables);
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
  if (pMeta == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }
  pMeta->uid          = htobe64(pTable->uid);
  pMeta->sversion     = htons(pTable->sversion);
  pMeta->tversion     = htons(pTable->tversion);
  pMeta->precision    = pMsg->pDb->cfg.precision;
  pMeta->numOfTags    = (uint8_t)pTable->numOfTags;
  pMeta->numOfColumns = htons((int16_t)pTable->numOfColumns);
  pMeta->tableType    = pTable->info.type;
  pMeta->contLen      = sizeof(STableMetaMsg) + mnodeSetSchemaFromSuperTable(pMeta->schema, pTable);
  tstrncpy(pMeta->tableId, pTable->info.tableId, sizeof(pMeta->tableId));

  pMsg->rpcRsp.len = pMeta->contLen;
  pMeta->contLen = htons(pMeta->contLen);

  pMsg->rpcRsp.rsp = pMeta;

  mDebug("app:%p:%p, stable:%s, uid:%" PRIu64 " table meta is retrieved", pMsg->rpcMsg.ahandle, pMsg,
         pTable->info.tableId, pTable->uid);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeProcessSuperTableVgroupMsg(SMnodeMsg *pMsg) {
  SSTableVgroupMsg *pInfo = pMsg->rpcMsg.pCont;
  int32_t numOfTable = htonl(pInfo->numOfTables);

  // reserve space
  int32_t contLen = sizeof(SSTableVgroupRspMsg) + 32 * sizeof(SVgroupMsg) + sizeof(SVgroupsMsg);
  for (int32_t i = 0; i < numOfTable; ++i) {
    char *stableName = (char *)pInfo + sizeof(SSTableVgroupMsg) + (TSDB_TABLE_FNAME_LEN)*i;
    SSuperTableObj *pTable = mnodeGetSuperTable(stableName);
    if (pTable != NULL && pTable->vgHash != NULL) {
      contLen += (taosHashGetSize(pTable->vgHash) * sizeof(SVgroupMsg) + sizeof(SVgroupsMsg));
    }

    mnodeDecTableRef(pTable);
  }

  SSTableVgroupRspMsg *pRsp = rpcMallocCont(contLen);
  if (pRsp == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  pRsp->numOfTables = 0;
  char *msg = (char *)pRsp + sizeof(SSTableVgroupRspMsg);

  for (int32_t i = 0; i < numOfTable; ++i) {
    char *          stableName = (char *)pInfo + sizeof(SSTableVgroupMsg) + (TSDB_TABLE_FNAME_LEN)*i;
    SSuperTableObj *pTable = mnodeGetSuperTable(stableName);
    if (pTable == NULL) {
      mError("app:%p:%p, stable:%s, not exist while get stable vgroup info", pMsg->rpcMsg.ahandle, pMsg, stableName);
      mnodeDecTableRef(pTable);
      continue;
    }
    if (pTable->vgHash == NULL) {
      mDebug("app:%p:%p, stable:%s, no vgroup exist while get stable vgroup info", pMsg->rpcMsg.ahandle, pMsg,
             stableName);
      mnodeDecTableRef(pTable);

      // even this super table has no corresponding table, still return
      pRsp->numOfTables++;

      SVgroupsMsg *pVgroupMsg = (SVgroupsMsg *)msg;
      pVgroupMsg->numOfVgroups = 0;
      
      msg += sizeof(SVgroupsMsg);
    } else {
      SVgroupsMsg *pVgroupMsg = (SVgroupsMsg *)msg;

      SHashMutableIterator *pIter = taosHashCreateIter(pTable->vgHash);
      int32_t               vgSize = 0;
      while (taosHashIterNext(pIter)) {
        int32_t *pVgId = taosHashIterGet(pIter);
        SVgObj * pVgroup = mnodeGetVgroup(*pVgId);
        if (pVgroup == NULL) continue;

        pVgroupMsg->vgroups[vgSize].vgId = htonl(pVgroup->vgId);
        pVgroupMsg->vgroups[vgSize].numOfEps = 0;

        for (int32_t vn = 0; vn < pVgroup->numOfVnodes; ++vn) {
          SDnodeObj *pDnode = pVgroup->vnodeGid[vn].pDnode;
          if (pDnode == NULL) break;

          tstrncpy(pVgroupMsg->vgroups[vgSize].epAddr[vn].fqdn, pDnode->dnodeFqdn, TSDB_FQDN_LEN);
          pVgroupMsg->vgroups[vgSize].epAddr[vn].port = htons(pDnode->dnodePort);

          pVgroupMsg->vgroups[vgSize].numOfEps++;
        }

        vgSize++;
        mnodeDecVgroupRef(pVgroup);
      }

      taosHashDestroyIter(pIter);
      mnodeDecTableRef(pTable);

      pVgroupMsg->numOfVgroups = htonl(vgSize);

      // one table is done, try the next table
      msg += sizeof(SVgroupsMsg) + vgSize * sizeof(SVgroupMsg);
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
  mInfo("drop stable rsp received, result:%s", tstrerror(rpcMsg->code));
}

static void *mnodeBuildCreateChildTableMsg(SCMCreateTableMsg *pMsg, SChildTableObj *pTable) {
  STagData *  pTagData = NULL;
  int32_t tagDataLen = 0;
  int32_t totalCols = 0;
  int32_t contLen = 0;
  if (pTable->info.type == TSDB_CHILD_TABLE) {
    totalCols = pTable->superTable->numOfColumns + pTable->superTable->numOfTags;
    contLen = sizeof(SMDCreateTableMsg) + totalCols * sizeof(SSchema) + pTable->sqlLen;
    if (pMsg != NULL) {
      pTagData = (STagData *)pMsg->schema;
      tagDataLen = htonl(pTagData->dataLen);
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
  pCreate->tid           = htonl(pTable->tid);
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

static int32_t mnodeDoCreateChildTableFp(SMnodeMsg *pMsg) {
  SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;
  assert(pTable);

  mDebug("app:%p:%p, table:%s, created in mnode, vgId:%d sid:%d, uid:%" PRIu64, pMsg->rpcMsg.ahandle, pMsg,
         pTable->info.tableId, pTable->vgId, pTable->tid, pTable->uid);

  SCMCreateTableMsg *pCreate = pMsg->rpcMsg.pCont;
  SMDCreateTableMsg *pMDCreate = mnodeBuildCreateChildTableMsg(pCreate, pTable);
  if (pMDCreate == NULL) {
    return terrno;
  }

  SRpcEpSet epSet = mnodeGetEpSetFromVgroup(pMsg->pVgroup);
  SRpcMsg rpcMsg = {
      .ahandle = pMsg,
      .pCont   = pMDCreate,
      .contLen = htonl(pMDCreate->contLen),
      .code    = 0,
      .msgType = TSDB_MSG_TYPE_MD_CREATE_TABLE
  };

  dnodeSendMsgToDnode(&epSet, &rpcMsg);
  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mnodeDoCreateChildTableCb(SMnodeMsg *pMsg, int32_t code) {
  SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;
  SCMCreateTableMsg *pCreate = pMsg->rpcMsg.pCont;
  assert(pTable);

  if (code == TSDB_CODE_SUCCESS) {
    if (pCreate->getMeta) {
      mDebug("app:%p:%p, table:%s, created in dnode and continue to get meta, thandle:%p", pMsg->rpcMsg.ahandle, pMsg,
             pTable->info.tableId, pMsg->rpcMsg.handle);

      pMsg->retry = 0;
      dnodeReprocessMnodeWriteMsg(pMsg);
    } else {
      mDebug("app:%p:%p, table:%s, created in dnode, thandle:%p", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId,
             pMsg->rpcMsg.handle);

      dnodeSendRpcMnodeWriteRsp(pMsg, TSDB_CODE_SUCCESS);
    }
    return TSDB_CODE_MND_ACTION_IN_PROGRESS;
  } else {
    mError("app:%p:%p, table:%s, failed to create table sid:%d, uid:%" PRIu64 ", reason:%s", pMsg->rpcMsg.ahandle, pMsg,
           pTable->info.tableId, pTable->tid, pTable->uid, tstrerror(code));
    SSdbOper desc = {.type = SDB_OPER_GLOBAL, .pObj = pTable, .table = tsChildTableSdb};
    sdbDeleteRow(&desc);
    return code;
  }
}

static int32_t mnodeDoCreateChildTable(SMnodeMsg *pMsg, int32_t tid) {
  SVgObj *pVgroup = pMsg->pVgroup;
  SCMCreateTableMsg *pCreate = pMsg->rpcMsg.pCont;
  SChildTableObj *pTable = calloc(1, sizeof(SChildTableObj));
  if (pTable == NULL) {
    mError("app:%p:%p, table:%s, failed to alloc memory", pMsg->rpcMsg.ahandle, pMsg, pCreate->tableId);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  if (pCreate->numOfColumns == 0) {
    pTable->info.type = TSDB_CHILD_TABLE;
  } else {
    pTable->info.type = TSDB_NORMAL_TABLE;
  }

  pTable->info.tableId = strdup(pCreate->tableId);  
  pTable->createdTime  = taosGetTimestampMs();
  pTable->tid          = tid;
  pTable->vgId         = pVgroup->vgId;

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    STagData *pTagData = (STagData *)pCreate->schema;  // it is a tag key
    if (pMsg->pSTable == NULL) pMsg->pSTable = mnodeGetSuperTable(pTagData->name);
    if (pMsg->pSTable == NULL) {
      mError("app:%p:%p, table:%s, corresponding super table:%s does not exist", pMsg->rpcMsg.ahandle, pMsg,
             pCreate->tableId, pTagData->name);
      mnodeDestroyChildTable(pTable);
      return TSDB_CODE_MND_INVALID_TABLE_NAME;
    }

    pTable->suid = pMsg->pSTable->uid;
    pTable->uid = (((uint64_t)pTable->vgId) << 48) + ((((uint64_t)pTable->tid) & ((1ul << 24) - 1ul)) << 24) +
                  ((sdbGetVersion() & ((1ul << 16) - 1ul)) << 8) + (taosRand() & ((1ul << 8) - 1ul));
    pTable->superTable = pMsg->pSTable;
  } else {
    if (pTable->info.type == TSDB_SUPER_TABLE) {
      int64_t us = taosGetTimestampUs();
      pTable->uid = (us << 24) + ((sdbGetVersion() & ((1ul << 16) - 1ul)) << 8) + (taosRand() & ((1ul << 8) - 1ul));
    } else {
      pTable->uid = (((uint64_t)pTable->vgId) << 48) + ((((uint64_t)pTable->tid) & ((1ul << 24) - 1ul)) << 24) +
                    ((sdbGetVersion() & ((1ul << 16) - 1ul)) << 8) + (taosRand() & ((1ul << 8) - 1ul));
    }

    pTable->sversion     = 0;
    pTable->numOfColumns = htons(pCreate->numOfColumns);
    pTable->sqlLen       = htons(pCreate->sqlLen);

    int32_t numOfCols  = pTable->numOfColumns;
    int32_t schemaSize = numOfCols * sizeof(SSchema);
    pTable->schema     = (SSchema *) calloc(1, schemaSize);
    if (pTable->schema == NULL) {
      free(pTable);
      return TSDB_CODE_MND_OUT_OF_MEMORY;
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
        return TSDB_CODE_MND_OUT_OF_MEMORY;
      }
      memcpy(pTable->sql, (char *) (pCreate->schema) + numOfCols * sizeof(SSchema), pTable->sqlLen);
      pTable->sql[pTable->sqlLen - 1] = 0;
      mDebug("app:%p:%p, table:%s, stream sql len:%d sql:%s", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId,
             pTable->sqlLen, pTable->sql);
    }
  }

  pMsg->pTable = (STableObj *)pTable;
  mnodeIncTableRef(pMsg->pTable);

  SSdbOper desc = {
    .type  = SDB_OPER_GLOBAL,
    .pObj  = pTable,
    .table = tsChildTableSdb,
    .pMsg  = pMsg,
    .reqFp = mnodeDoCreateChildTableFp
  };
  
  int32_t code = sdbInsertRow(&desc);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mnodeDestroyChildTable(pTable);
    pMsg->pTable = NULL;
    mError("app:%p:%p, table:%s, failed to create, reason:%s", pMsg->rpcMsg.ahandle, pMsg, pCreate->tableId,
           tstrerror(code));
  } else {
    mDebug("app:%p:%p, table:%s, allocated in vgroup, vgId:%d sid:%d uid:%" PRIu64, pMsg->rpcMsg.ahandle, pMsg,
           pTable->info.tableId, pVgroup->vgId, pTable->tid, pTable->uid);
  }

  return code;
}

static int32_t mnodeProcessCreateChildTableMsg(SMnodeMsg *pMsg) {
  SCMCreateTableMsg *pCreate = pMsg->rpcMsg.pCont;
  int32_t code = grantCheck(TSDB_GRANT_TIMESERIES);
  if (code != TSDB_CODE_SUCCESS) {
    mError("app:%p:%p, table:%s, failed to create, grant timeseries failed", pMsg->rpcMsg.ahandle, pMsg,
           pCreate->tableId);
    return code;
  }

  if (pMsg->retry == 0) {
    if (pMsg->pTable == NULL) {
      SVgObj *pVgroup = NULL;
      int32_t tid = 0;
      code = mnodeGetAvailableVgroup(pMsg, &pVgroup, &tid);
      if (code != TSDB_CODE_SUCCESS) {
        mDebug("app:%p:%p, table:%s, failed to get available vgroup, reason:%s", pMsg->rpcMsg.ahandle, pMsg,
               pCreate->tableId, tstrerror(code));
        return code;
      }

      if (pMsg->pVgroup != NULL) {
        mnodeDecVgroupRef(pMsg->pVgroup);
      }

      pMsg->pVgroup = pVgroup;
      mnodeIncVgroupRef(pVgroup);

      return mnodeDoCreateChildTable(pMsg, tid);
    }
  } else {
    if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pCreate->tableId);
  }

  if (pMsg->pTable == NULL) {
    mError("app:%p:%p, table:%s, object not found, retry:%d reason:%s", pMsg->rpcMsg.ahandle, pMsg, pCreate->tableId, pMsg->retry,
           tstrerror(terrno));
    return terrno;
  } else {
    mDebug("app:%p:%p, table:%s, send create msg to vnode again", pMsg->rpcMsg.ahandle, pMsg, pCreate->tableId);
    return mnodeDoCreateChildTableFp(pMsg);
  }
}

static int32_t mnodeSendDropChildTableMsg(SMnodeMsg *pMsg, bool needReturn) {
  SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;
  mLInfo("app:%p:%p, ctable:%s, is dropped from sdb", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId);

  SMDDropTableMsg *pDrop = rpcMallocCont(sizeof(SMDDropTableMsg));
  if (pDrop == NULL) {
    mError("app:%p:%p, ctable:%s, failed to drop ctable, no enough memory", pMsg->rpcMsg.ahandle, pMsg,
           pTable->info.tableId);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  tstrncpy(pDrop->tableId, pTable->info.tableId, TSDB_TABLE_FNAME_LEN);
  pDrop->vgId    = htonl(pTable->vgId);
  pDrop->contLen = htonl(sizeof(SMDDropTableMsg));
  pDrop->tid     = htonl(pTable->tid);
  pDrop->uid     = htobe64(pTable->uid);

  SRpcEpSet epSet = mnodeGetEpSetFromVgroup(pMsg->pVgroup);

  mInfo("app:%p:%p, ctable:%s, send drop ctable msg, vgId:%d sid:%d uid:%" PRIu64, pMsg->rpcMsg.ahandle, pMsg,
        pDrop->tableId, pTable->vgId, pTable->tid, pTable->uid);

  SRpcMsg rpcMsg = {
    .ahandle = pMsg,
    .pCont   = pDrop,
    .contLen = sizeof(SMDDropTableMsg),
    .code    = 0,
    .msgType = TSDB_MSG_TYPE_MD_DROP_TABLE
  };

  if (!needReturn) rpcMsg.ahandle = NULL;

  dnodeSendMsgToDnode(&epSet, &rpcMsg);

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mnodeDropChildTableCb(SMnodeMsg *pMsg, int32_t code) {
  if (code != TSDB_CODE_SUCCESS) {
    SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;
    mError("app:%p:%p, ctable:%s, failed to drop, sdb error", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId);
    return code;
  } 

  return mnodeSendDropChildTableMsg(pMsg, true);
}

static int32_t mnodeProcessDropChildTableMsg(SMnodeMsg *pMsg) {
  SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;
  if (pMsg->pVgroup == NULL) pMsg->pVgroup = mnodeGetVgroup(pTable->vgId);
  if (pMsg->pVgroup == NULL) {
    mError("app:%p:%p, table:%s, failed to drop ctable, vgroup not exist", pMsg->rpcMsg.ahandle, pMsg,
           pTable->info.tableId);
    return TSDB_CODE_MND_APP_ERROR;
  }

  SSdbOper oper = {
    .type  = SDB_OPER_GLOBAL,
    .table = tsChildTableSdb,
    .pObj  = pTable,
    .pMsg  = pMsg,
    .writeCb = mnodeDropChildTableCb
  };

  int32_t code = sdbDeleteRow(&oper);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("app:%p:%p, ctable:%s, failed to drop, reason:%s", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId,
           tstrerror(code));
  }

  return code;
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

static int32_t mnodeAlterNormalTableColumnCb(SMnodeMsg *pMsg, int32_t code) {
  SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;
  if (code != TSDB_CODE_SUCCESS) {
    mError("app:%p:%p, ctable %s, failed to alter column, reason:%s", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId,
           tstrerror(code));
    return code;
  }

  SMDCreateTableMsg *pMDCreate = mnodeBuildCreateChildTableMsg(NULL, pTable);
  if (pMDCreate == NULL) {
    return terrno;
  }

  if (pMsg->pVgroup == NULL) {
    pMsg->pVgroup = mnodeGetVgroup(pTable->vgId);
    if (pMsg->pVgroup == NULL) {
      rpcFreeCont(pMDCreate);
      mError("app:%p:%p, ctable %s, vgId:%d not exist in mnode", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId,
             pTable->vgId);
      return TSDB_CODE_MND_VGROUP_NOT_EXIST;
    }
  }

  SRpcEpSet epSet = mnodeGetEpSetFromVgroup(pMsg->pVgroup);
  SRpcMsg rpcMsg = {
      .ahandle = pMsg,
      .pCont   = pMDCreate,
      .contLen = htonl(pMDCreate->contLen),
      .code    = 0,
      .msgType = TSDB_MSG_TYPE_MD_ALTER_TABLE
  };

  mDebug("app:%p:%p, ctable %s, send alter column msg to vgId:%d", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId,
         pMsg->pVgroup->vgId);

  dnodeSendMsgToDnode(&epSet, &rpcMsg);
  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mnodeAddNormalTableColumn(SMnodeMsg *pMsg, SSchema schema[], int32_t ncols) {
  SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;
  SDbObj *pDb = pMsg->pDb;
  if (ncols <= 0) {
    mError("app:%p:%p, ctable:%s, add column, ncols:%d <= 0", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId, ncols);
    return TSDB_CODE_MND_APP_ERROR;
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (mnodeFindNormalTableColumnIndex(pTable, schema[i].name) > 0) {
      mError("app:%p:%p, ctable:%s, add column, column:%s already exist", pMsg->rpcMsg.ahandle, pMsg,
             pTable->info.tableId, schema[i].name);
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

  mInfo("app:%p:%p, ctable %s, start to add column", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsChildTableSdb,
    .pObj = pTable,
    .pMsg = pMsg,
    .writeCb = mnodeAlterNormalTableColumnCb
  };

  return sdbUpdateRow(&oper);
}

static int32_t mnodeDropNormalTableColumn(SMnodeMsg *pMsg, char *colName) {
  SDbObj *pDb = pMsg->pDb;
  SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;
  int32_t col = mnodeFindNormalTableColumnIndex(pTable, colName);
  if (col <= 0) {
    mError("app:%p:%p, ctable:%s, drop column, column:%s not exist", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId,
           colName);
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

  mInfo("app:%p:%p, ctable %s, start to drop column %s", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId, colName);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsChildTableSdb,
    .pObj = pTable,
    .pMsg = pMsg,
    .writeCb = mnodeAlterNormalTableColumnCb
  };

  return sdbUpdateRow(&oper);
}

static int32_t mnodeChangeNormalTableColumn(SMnodeMsg *pMsg, char *oldName, char *newName) {
  SChildTableObj *pTable = (SChildTableObj *)pMsg->pTable;
  int32_t col = mnodeFindNormalTableColumnIndex(pTable, oldName);
  if (col < 0) {
    mError("app:%p:%p, ctable:%s, change column, oldName: %s, newName: %s", pMsg->rpcMsg.ahandle, pMsg,
           pTable->info.tableId, oldName, newName);
    return TSDB_CODE_MND_FIELD_NOT_EXIST;
  }

  // int32_t  rowSize = 0;
  uint32_t len = strlen(newName);
  if (len >= TSDB_COL_NAME_LEN) {
    return TSDB_CODE_MND_COL_NAME_TOO_LONG;
  }

  if (mnodeFindNormalTableColumnIndex(pTable, newName) >= 0) {
    return TSDB_CODE_MND_FIELD_ALREAY_EXIST;
  }
  
  // update
  SSchema *schema = (SSchema *) (pTable->schema + col);
  tstrncpy(schema->name, newName, sizeof(schema->name));

  mInfo("app:%p:%p, ctable %s, start to modify column %s to %s", pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId,
         oldName, newName);

  SSdbOper oper = {
    .type = SDB_OPER_GLOBAL,
    .table = tsChildTableSdb,
    .pObj = pTable,
    .pMsg = pMsg,
    .writeCb = mnodeAlterNormalTableColumnCb
  };

  return sdbUpdateRow(&oper);
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
  pMeta->tid       = htonl(pTable->tid);
  pMeta->precision = pDb->cfg.precision;
  pMeta->tableType = pTable->info.type;
  tstrncpy(pMeta->tableId, pTable->info.tableId, TSDB_TABLE_FNAME_LEN);
  if (pTable->superTable != NULL) {
    tstrncpy(pMeta->sTableId, pTable->superTable->info.tableId, TSDB_TABLE_FNAME_LEN);
  }

  if (pTable->info.type == TSDB_CHILD_TABLE && pTable->superTable != NULL) {
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
    mError("app:%p:%p, table:%s, failed to get table meta, vgroup not exist", pMsg->rpcMsg.ahandle, pMsg,
           pTable->info.tableId);
    return TSDB_CODE_MND_VGROUP_NOT_EXIST;
  }

  for (int32_t i = 0; i < pMsg->pVgroup->numOfVnodes; ++i) {
    SDnodeObj *pDnode = mnodeGetDnode(pMsg->pVgroup->vnodeGid[i].dnodeId);
    if (pDnode == NULL) break;
    strcpy(pMeta->vgroup.epAddr[i].fqdn, pDnode->dnodeFqdn);
    pMeta->vgroup.epAddr[i].port = htons(pDnode->dnodePort + TSDB_PORT_DNODESHELL);
    pMeta->vgroup.numOfEps++;
    mnodeDecDnodeRef(pDnode);
  }
  pMeta->vgroup.vgId = htonl(pMsg->pVgroup->vgId);

  mDebug("app:%p:%p, table:%s, uid:%" PRIu64 " table meta is retrieved, vgId:%d sid:%d", pMsg->rpcMsg.ahandle, pMsg,
         pTable->info.tableId, pTable->uid, pTable->vgId, pTable->tid);

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeAutoCreateChildTable(SMnodeMsg *pMsg) {
  STableInfoMsg *pInfo = pMsg->rpcMsg.pCont;
  STagData *pTags = (STagData *)pInfo->tags;
  int32_t tagLen = htonl(pTags->dataLen);
  if (pTags->name[0] == 0) {
    mError("app:%p:%p, table:%s, failed to create table on demand for stable is empty, tagLen:%d", pMsg->rpcMsg.ahandle,
           pMsg, pInfo->tableId, tagLen);
    return TSDB_CODE_MND_INVALID_STABLE_NAME; 
  }

  int32_t contLen = sizeof(SCMCreateTableMsg) + offsetof(STagData, data) + tagLen;
  SCMCreateTableMsg *pCreateMsg = rpcMallocCont(contLen);
  if (pCreateMsg == NULL) {
    mError("app:%p:%p, table:%s, failed to create table while get meta info, no enough memory", pMsg->rpcMsg.ahandle,
           pMsg, pInfo->tableId);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  size_t size = sizeof(pInfo->tableId);
  tstrncpy(pCreateMsg->tableId, pInfo->tableId, size);
  tstrncpy(pCreateMsg->db, pMsg->pDb->name, sizeof(pCreateMsg->db));
  pCreateMsg->igExists = 1;
  pCreateMsg->getMeta = 1;
  pCreateMsg->contLen = htonl(contLen);

  memcpy(pCreateMsg->schema, pTags, contLen - sizeof(SCMCreateTableMsg));
  mDebug("app:%p:%p, table:%s, start to create on demand, tagLen:%d stable:%s",
         pMsg->rpcMsg.ahandle, pMsg, pInfo->tableId, tagLen, pTags->name);

  rpcFreeCont(pMsg->rpcMsg.pCont);
  pMsg->rpcMsg.msgType = TSDB_MSG_TYPE_CM_CREATE_TABLE;
  pMsg->rpcMsg.pCont = pCreateMsg;
  pMsg->rpcMsg.contLen = contLen;
  
  return TSDB_CODE_MND_ACTION_NEED_REPROCESSED;
}

static int32_t mnodeGetChildTableMeta(SMnodeMsg *pMsg) {
  STableMetaMsg *pMeta =
      rpcMallocCont(sizeof(STableMetaMsg) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16));
  if (pMeta == NULL) {
    mError("app:%p:%p, table:%s, failed to get table meta, no enough memory", pMsg->rpcMsg.ahandle, pMsg,
           pMsg->pTable->tableId);
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

  mInfo("vgId:%d, all child tables will be dropped from sdb", pVgroup->vgId);

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

  mInfo("vgId:%d, all child tables is dropped from sdb", pVgroup->vgId);
}

void mnodeDropAllChildTables(SDbObj *pDropDb) {
  void *  pIter = NULL;
  int32_t numOfTables = 0;
  SChildTableObj *pTable = NULL;

  char prefix[64] = {0};
  tstrncpy(prefix, pDropDb->name, 64);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = strlen(prefix);

  mInfo("db:%s, all child tables will be dropped from sdb", pDropDb->name);

  while (1) {
    pIter = mnodeGetNextChildTable(pIter, &pTable);
    if (pTable == NULL) break;

    if (strncmp(prefix, pTable->info.tableId, prefixLen) == 0) {
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

  mInfo("db:%s, all child tables:%d is dropped from sdb", pDropDb->name, numOfTables);
}

static void mnodeDropAllChildTablesInStable(SSuperTableObj *pStable) {
  void *  pIter = NULL;
  int32_t numOfTables = 0;
  SChildTableObj *pTable = NULL;

  mInfo("stable:%s uid:%" PRIu64 ", all child tables:%d will be dropped from sdb", pStable->info.tableId, pStable->uid,
        pStable->numOfTables);

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

  mInfo("stable:%s, all child tables:%d is dropped from sdb", pStable->info.tableId, numOfTables);
}

#if 0
static SChildTableObj* mnodeGetTableByPos(int32_t vnode, int32_t tid) {
  SVgObj *pVgroup = mnodeGetVgroup(vnode);
  if (pVgroup == NULL) return NULL;

  SChildTableObj *pTable = pVgroup->tableList[tid - 1];
  mnodeIncTableRef((STableObj *)pTable);

  mnodeDecVgroupRef(pVgroup);
  return pTable;
}
#endif

static int32_t mnodeProcessTableCfgMsg(SMnodeMsg *pMsg) {
  return TSDB_CODE_COM_OPS_NOT_SUPPORT;
#if 0  
  SConfigTableMsg *pCfg = pMsg->rpcMsg.pCont;
  pCfg->dnodeId = htonl(pCfg->dnodeId);
  pCfg->vgId = htonl(pCfg->vgId);
  pCfg->sid = htonl(pCfg->sid);
  mDebug("app:%p:%p, dnode:%d, vgId:%d sid:%d, receive table config msg", pMsg->rpcMsg.ahandle, pMsg, pCfg->dnodeId,
         pCfg->vgId, pCfg->sid);

  SChildTableObj *pTable = mnodeGetTableByPos(pCfg->vgId, pCfg->sid);
  if (pTable == NULL) {
    mError("app:%p:%p, dnode:%d, vgId:%d sid:%d, table not found", pMsg->rpcMsg.ahandle, pMsg, pCfg->dnodeId,
           pCfg->vgId, pCfg->sid);
    return TSDB_CODE_MND_INVALID_TABLE_ID;
  }

  SMDCreateTableMsg *pCreate = NULL;
  pCreate = mnodeBuildCreateChildTableMsg(NULL, (SChildTableObj *)pTable);
  mnodeDecTableRef(pTable);
    
  if (pCreate == NULL) return terrno;
  
  pMsg->rpcRsp.rsp = pCreate;
  pMsg->rpcRsp.len = htonl(pCreate->contLen);
  return TSDB_CODE_SUCCESS;
#endif  
}

// handle drop child response
static void mnodeProcessDropChildTableRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->ahandle == NULL) return;

  SMnodeMsg *mnodeMsg = rpcMsg->ahandle;
  mnodeMsg->received++;

  SChildTableObj *pTable = (SChildTableObj *)mnodeMsg->pTable;
  assert(pTable);

  mInfo("app:%p:%p, table:%s, drop table rsp received, vgId:%d sid:%d uid:%" PRIu64 ", thandle:%p result:%s",
        mnodeMsg->rpcMsg.ahandle, mnodeMsg, pTable->info.tableId, pTable->vgId, pTable->tid, pTable->uid,
        mnodeMsg->rpcMsg.handle, tstrerror(rpcMsg->code));

  if (rpcMsg->code != TSDB_CODE_SUCCESS) {
    mError("app:%p:%p, table:%s, failed to drop in dnode, vgId:%d sid:%d uid:%" PRIu64 ", reason:%s",
           mnodeMsg->rpcMsg.ahandle, mnodeMsg, pTable->info.tableId, pTable->vgId, pTable->tid, pTable->uid,
           tstrerror(rpcMsg->code));
    dnodeSendRpcMnodeWriteRsp(mnodeMsg, rpcMsg->code);
    return;
  }

  if (mnodeMsg->pVgroup == NULL) mnodeMsg->pVgroup = mnodeGetVgroup(pTable->vgId);
  if (mnodeMsg->pVgroup == NULL) {
    mError("app:%p:%p, table:%s, failed to get vgroup", mnodeMsg->rpcMsg.ahandle, mnodeMsg, pTable->info.tableId);
    dnodeSendRpcMnodeWriteRsp(mnodeMsg, TSDB_CODE_MND_VGROUP_NOT_EXIST);
    return;
  }

  if (mnodeMsg->pVgroup->numOfTables <= 0) {
    mInfo("app:%p:%p, vgId:%d, all tables is dropped, drop vgroup", mnodeMsg->rpcMsg.ahandle, mnodeMsg,
           mnodeMsg->pVgroup->vgId);
    mnodeDropVgroup(mnodeMsg->pVgroup, NULL);
  }

  dnodeSendRpcMnodeWriteRsp(mnodeMsg, TSDB_CODE_SUCCESS);
}

/*
 * handle create table response from dnode
 *   if failed, drop the table cached
 */
static void mnodeProcessCreateChildTableRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->ahandle == NULL) return;

  SMnodeMsg *mnodeMsg = rpcMsg->ahandle;
  mnodeMsg->received++;

  SChildTableObj *pTable = (SChildTableObj *)mnodeMsg->pTable;
  assert(pTable);

  // If the table is deleted by another thread during creation, stop creating and send drop msg to vnode
  if (sdbCheckRowDeleted(tsChildTableSdb, pTable)) {
    mDebug("app:%p:%p, table:%s, create table rsp received, but a deleting opertion incoming, vgId:%d sid:%d uid:%" PRIu64,
           mnodeMsg->rpcMsg.ahandle, mnodeMsg, pTable->info.tableId, pTable->vgId, pTable->tid, pTable->uid);

    // if the vgroup is already dropped from hash, it can't be accquired by pTable->vgId
    // so the refCount of vgroup can not be decreased
    // SVgObj *pVgroup = mnodeGetVgroup(pTable->vgId);
    // if (pVgroup == NULL) {
    //   mnodeRemoveTableFromVgroup(mnodeMsg->pVgroup, pTable);
    // }
    // mnodeDecVgroupRef(pVgroup);

    mnodeSendDropChildTableMsg(mnodeMsg, false);
    rpcMsg->code = TSDB_CODE_SUCCESS;
    dnodeSendRpcMnodeWriteRsp(mnodeMsg, rpcMsg->code);
    return;
  }

  if (rpcMsg->code == TSDB_CODE_SUCCESS || rpcMsg->code == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
     SSdbOper desc = {
      .type  = SDB_OPER_GLOBAL,
      .pObj  = pTable,
      .table = tsChildTableSdb,
      .pMsg  = mnodeMsg,
      .writeCb = mnodeDoCreateChildTableCb
    };
    
    int32_t code = sdbInsertRowImp(&desc);
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
      mnodeMsg->pTable = NULL;
      mnodeDestroyChildTable(pTable);
      dnodeSendRpcMnodeWriteRsp(mnodeMsg, code);
    }
  } else {
    if (mnodeMsg->retry++ < 10) {
      mDebug("app:%p:%p, table:%s, create table rsp received, need retry, times:%d vgId:%d sid:%d uid:%" PRIu64
             " result:%s thandle:%p",
             mnodeMsg->rpcMsg.ahandle, mnodeMsg, pTable->info.tableId, mnodeMsg->retry, pTable->vgId, pTable->tid,
             pTable->uid, tstrerror(rpcMsg->code), mnodeMsg->rpcMsg.handle);

      dnodeDelayReprocessMnodeWriteMsg(mnodeMsg);
    } else {
      mError("app:%p:%p, table:%s, failed to create in dnode, vgId:%d sid:%d uid:%" PRIu64 ", result:%s thandle:%p",
             mnodeMsg->rpcMsg.ahandle, mnodeMsg, pTable->info.tableId, pTable->vgId, pTable->tid, pTable->uid,
             tstrerror(rpcMsg->code), mnodeMsg->rpcMsg.handle);

      SSdbOper oper = {.type = SDB_OPER_GLOBAL, .table = tsChildTableSdb, .pObj = pTable};
      sdbDeleteRow(&oper);

      dnodeSendRpcMnodeWriteRsp(mnodeMsg, rpcMsg->code);
    }
  }
}

static void mnodeProcessAlterTableRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->ahandle == NULL) return;

  SMnodeMsg *mnodeMsg = rpcMsg->ahandle;
  mnodeMsg->received++;

  SChildTableObj *pTable = (SChildTableObj *)mnodeMsg->pTable;
  assert(pTable);

  if (rpcMsg->code == TSDB_CODE_SUCCESS || rpcMsg->code == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
    mDebug("app:%p:%p, ctable:%s, altered in dnode, thandle:%p result:%s", mnodeMsg->rpcMsg.ahandle, mnodeMsg,
           pTable->info.tableId, mnodeMsg->rpcMsg.handle, tstrerror(rpcMsg->code));

    dnodeSendRpcMnodeWriteRsp(mnodeMsg, TSDB_CODE_SUCCESS);
  } else {
    if (mnodeMsg->retry++ < 3) {
      mDebug("app:%p:%p, table:%s, alter table rsp received, need retry, times:%d result:%s thandle:%p",
             mnodeMsg->rpcMsg.ahandle, mnodeMsg, pTable->info.tableId, mnodeMsg->retry, tstrerror(rpcMsg->code),
             mnodeMsg->rpcMsg.handle);

      dnodeDelayReprocessMnodeWriteMsg(mnodeMsg);
    } else {
      mError("app:%p:%p, table:%s, failed to alter in dnode, result:%s thandle:%p", mnodeMsg->rpcMsg.ahandle, mnodeMsg,
             pTable->info.tableId, tstrerror(rpcMsg->code), mnodeMsg->rpcMsg.handle);
      dnodeSendRpcMnodeWriteRsp(mnodeMsg, rpcMsg->code);
    }
  }
}

static int32_t mnodeProcessMultiTableMetaMsg(SMnodeMsg *pMsg) {
  SMultiTableInfoMsg *pInfo = pMsg->rpcMsg.pCont;
  pInfo->numOfTables = htonl(pInfo->numOfTables);

  int32_t totalMallocLen = 4 * 1024 * 1024;  // first malloc 4 MB, subsequent reallocation as twice
  SMultiTableMeta *pMultiMeta = rpcMallocCont(totalMallocLen);
  if (pMultiMeta == NULL) {
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  pMultiMeta->contLen = sizeof(SMultiTableMeta);
  pMultiMeta->numOfTables = 0;

  for (int32_t t = 0; t < pInfo->numOfTables; ++t) {
    char * tableId = (char *)(pInfo->tableIds + t * TSDB_TABLE_FNAME_LEN);
    SChildTableObj *pTable = mnodeGetChildTable(tableId);
    if (pTable == NULL) continue;

    if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDbByTableId(tableId);
    if (pMsg->pDb == NULL || pMsg->pDb->status != TSDB_DB_STATUS_READY) {
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
  
  if (pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pDb->name, pDb->status);
    mnodeDecDbRef(pDb);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  SSchema s = tGetTableNameColumnSchema();
  pShow->bytes[cols] = s.bytes;
  pSchema[cols].type = s.type;
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

  SSchema tbCol = tGetTableNameColumnSchema();
  pShow->bytes[cols] = tbCol.bytes + VARSTR_HEADER_SIZE;
  pSchema[cols].type = tbCol.type;
  strcpy(pSchema[cols].name, "stable_name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8; // table uid
  pSchema[cols].type = TSDB_DATA_TYPE_BIGINT;
  strcpy(pSchema[cols].name, "uid");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "tid");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "vgId");
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
  
  if (pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pDb->name, pDb->status);
    mnodeDecDbRef(pDb);
    return 0;
  }

  int32_t cols       = 0;
  int32_t numOfRows  = 0;
  SChildTableObj *pTable = NULL;
  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  char prefix[64] = {0};
  tstrncpy(prefix, pDb->name, 64);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = strlen(prefix);

  char* pattern = NULL;
  if (pShow->payloadLen > 0) {
    pattern = (char*)malloc(pShow->payloadLen + 1);
    if (pattern == NULL) {
      terrno = TSDB_CODE_QRY_OUT_OF_MEMORY;
      return 0;
    }
    memcpy(pattern, pShow->payload, pShow->payloadLen);
    pattern[pShow->payloadLen] = 0;
  }

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

    if (pattern != NULL && patternMatch(pattern, tableName, sizeof(tableName) - 1, &info) != TSDB_PATTERN_MATCH) {
      mnodeDecTableRef(pTable);
      continue;
    }

    cols = 0;
    char *pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;

    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tableName, pShow->bytes[cols]);
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
      STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tableName, pShow->bytes[cols]);
    }
    
    cols++;

    // uid
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t*) pWrite = pTable->uid;
    cols++;


    // tid
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t*) pWrite = pTable->tid;
    cols++;

    //vgid
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t*) pWrite = pTable->vgId;
    cols++;

    numOfRows++;
    mnodeDecTableRef(pTable);
  }

  pShow->numOfReads += numOfRows;

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  mnodeDecDbRef(pDb);
  free(pattern);

  return numOfRows;
}

static int32_t mnodeProcessAlterTableMsg(SMnodeMsg *pMsg) {
  SAlterTableMsg *pAlter = pMsg->rpcMsg.pCont;
  mDebug("app:%p:%p, table:%s, alter table msg is received from thandle:%p", pMsg->rpcMsg.ahandle, pMsg,
         pAlter->tableId, pMsg->rpcMsg.handle);

  if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDbByTableId(pAlter->tableId);
  if (pMsg->pDb == NULL) {
    mError("app:%p:%p, table:%s, failed to alter table, db not selected", pMsg->rpcMsg.ahandle, pMsg, pAlter->tableId);
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }
  
  if (pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pMsg->pDb->name, pMsg->pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  if (mnodeCheckIsMonitorDB(pMsg->pDb->name, tsMonitorDbName)) {
    mError("app:%p:%p, table:%s, failed to alter table, its log db", pMsg->rpcMsg.ahandle, pMsg, pAlter->tableId);
    return TSDB_CODE_MND_MONITOR_DB_FORBIDDEN;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pAlter->tableId);
  if (pMsg->pTable == NULL) {
    mError("app:%p:%p, table:%s, failed to alter table, table not exist", pMsg->rpcMsg.ahandle, pMsg, pAlter->tableId);
    return TSDB_CODE_MND_INVALID_TABLE_NAME;
  }

  pAlter->type = htons(pAlter->type);
  pAlter->numOfCols = htons(pAlter->numOfCols);
  pAlter->tagValLen = htonl(pAlter->tagValLen);

  if (pAlter->numOfCols > 2) {
    mError("app:%p:%p, table:%s, error numOfCols:%d in alter table", pMsg->rpcMsg.ahandle, pMsg, pAlter->tableId,
           pAlter->numOfCols);
    return TSDB_CODE_MND_APP_ERROR;
  }

  for (int32_t i = 0; i < pAlter->numOfCols; ++i) {
    pAlter->schema[i].bytes = htons(pAlter->schema[i].bytes);
  }

  int32_t code = TSDB_CODE_COM_OPS_NOT_SUPPORT;
  if (pMsg->pTable->type == TSDB_SUPER_TABLE) {
    mDebug("app:%p:%p, table:%s, start to alter stable", pMsg->rpcMsg.ahandle, pMsg, pAlter->tableId);
    if (pAlter->type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN) {
      code = mnodeAddSuperTableTag(pMsg, pAlter->schema, 1);
    } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN) {
      code = mnodeDropSuperTableTag(pMsg, pAlter->schema[0].name);
    } else if (pAlter->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN) {
      code = mnodeModifySuperTableTagName(pMsg, pAlter->schema[0].name, pAlter->schema[1].name);
    } else if (pAlter->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
      code = mnodeAddSuperTableColumn(pMsg, pAlter->schema, 1);
    } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
      code = mnodeDropSuperTableColumn(pMsg, pAlter->schema[0].name);
    } else if (pAlter->type == TSDB_ALTER_TABLE_CHANGE_COLUMN) {
      code = mnodeChangeSuperTableColumn(pMsg, pAlter->schema[0].name, pAlter->schema[1].name);
    } else {
    }
  } else {
    mDebug("app:%p:%p, table:%s, start to alter ctable", pMsg->rpcMsg.ahandle, pMsg, pAlter->tableId);
    if (pAlter->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
      return TSDB_CODE_COM_OPS_NOT_SUPPORT;
    } else if (pAlter->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
      code = mnodeAddNormalTableColumn(pMsg, pAlter->schema, 1);
    } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
      code = mnodeDropNormalTableColumn(pMsg, pAlter->schema[0].name);
    } else if (pAlter->type == TSDB_ALTER_TABLE_CHANGE_COLUMN) {
      code = mnodeChangeNormalTableColumn(pMsg, pAlter->schema[0].name, pAlter->schema[1].name);
    } else {
    }
  }

 return code;
}

static int32_t mnodeGetStreamTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mnodeGetDb(pShow->db);
  if (pDb == NULL) return TSDB_CODE_MND_DB_NOT_SELECTED;
  
  if (pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pDb->name, pDb->status);
    mnodeDecDbRef(pDb);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  SSchema tbnameColSchema = tGetTableNameColumnSchema();
  pShow->bytes[cols] = tbnameColSchema.bytes;
  pSchema[cols].type = tbnameColSchema.type;
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
  
  if (pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pDb->name, pDb->status);
    mnodeDecDbRef(pDb);
    return 0;
  }
  
  int32_t numOfRows  = 0;
  SChildTableObj *pTable = NULL;
  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  char prefix[64] = {0};
  tstrncpy(prefix, pDb->name, 64);
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

    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, tableName, pShow->bytes[cols]);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *) pWrite = pTable->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pTable->numOfColumns;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_WITH_MAXSIZE_TO_VARSTR(pWrite, pTable->sql, pShow->bytes[cols]);    
    cols++;

    numOfRows++;
    mnodeDecTableRef(pTable);
  }

  pShow->numOfReads += numOfRows;

  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  mnodeDecDbRef(pDb);

  return numOfRows;
}
