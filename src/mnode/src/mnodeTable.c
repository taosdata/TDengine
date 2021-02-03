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
#include "tqueue.h"
#include "hash.h"
#include "mnode.h"
#include "dnode.h"
#include "mnodeDef.h"
#include "mnodeInt.h"
#include "mnodeAcct.h"
#include "mnodeDb.h"
#include "mnodeDnode.h"
#include "mnodeSdb.h"
#include "mnodeShow.h"
#include "mnodeTable.h"
#include "mnodeVgroup.h"
#include "mnodeWrite.h"
#include "mnodeRead.h"
#include "mnodePeer.h"

#define ALTER_CTABLE_RETRY_TIMES  3
#define CREATE_CTABLE_RETRY_TIMES 10
#define CREATE_CTABLE_RETRY_SEC   14

int64_t          tsCTableRid = -1;
static void *    tsChildTableSdb;
int64_t          tsSTableRid = -1;
static void *    tsSuperTableSdb;
static SHashObj *tsSTableUidHash;
static int32_t   tsChildTableUpdateSize;
static int32_t   tsSuperTableUpdateSize;

static void *  mnodeGetChildTable(char *tableId);
static void *  mnodeGetSuperTable(char *tableId);
static void *  mnodeGetSuperTableByUid(uint64_t uid);
static void    mnodeDropAllChildTablesInStable(SSTableObj *pStable);
static void    mnodeAddTableIntoStable(SSTableObj *pStable, SCTableObj *pCtable);
static void    mnodeRemoveTableFromStable(SSTableObj *pStable, SCTableObj *pCtable);

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

static int32_t mnodeFindSuperTableColumnIndex(SSTableObj *pStable, char *colName);

static void mnodeDestroyChildTable(SCTableObj *pTable) {
  tfree(pTable->info.tableId);
  tfree(pTable->schema);
  tfree(pTable->sql);
  tfree(pTable);
}

static int32_t mnodeChildTableActionDestroy(SSdbRow *pRow) {
  mnodeDestroyChildTable(pRow->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeChildTableActionInsert(SSdbRow *pRow) {
  SCTableObj *pTable = pRow->pObj;

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

static int32_t mnodeChildTableActionDelete(SSdbRow *pRow) {
  SCTableObj *pTable = pRow->pObj;
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

static int32_t mnodeChildTableActionUpdate(SSdbRow *pRow) {
  SCTableObj *pNew = pRow->pObj;
  SCTableObj *pTable = mnodeGetChildTable(pNew->info.tableId);
  if (pTable != pNew) {
    void *oldTableId = pTable->info.tableId;
    void *oldSql = pTable->sql;
    void *oldSchema = pTable->schema;
    void *oldSTable = pTable->superTable;
    int32_t oldRefCount = pTable->refCount;

    memcpy(pTable, pNew, sizeof(SCTableObj));

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

static int32_t mnodeChildTableActionEncode(SSdbRow *pRow) {
  SCTableObj *pTable = pRow->pObj;
  assert(pTable != NULL && pRow->rowData != NULL);

  int32_t len = (int32_t)strlen(pTable->info.tableId);
  if (len >= TSDB_TABLE_FNAME_LEN) return TSDB_CODE_MND_INVALID_TABLE_ID;

  memcpy(pRow->rowData, pTable->info.tableId, len);
  memset((char *)pRow->rowData + len, 0, 1);
  len++;

  memcpy((char *)pRow->rowData + len, (char *)pTable + sizeof(char *), tsChildTableUpdateSize);
  len += tsChildTableUpdateSize;

  if (pTable->info.type != TSDB_CHILD_TABLE) {
    int32_t schemaSize = pTable->numOfColumns * sizeof(SSchema);
    memcpy((char *)pRow->rowData + len, pTable->schema, schemaSize);
    len += schemaSize;

    if (pTable->sqlLen != 0) {
      memcpy((char *)pRow->rowData + len, pTable->sql, pTable->sqlLen);
      len += pTable->sqlLen;
    }
  }

  pRow->rowSize = len;

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeChildTableActionDecode(SSdbRow *pRow) {
  assert(pRow->rowData != NULL);
  SCTableObj *pTable = calloc(1, sizeof(SCTableObj));
  if (pTable == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

  int32_t len = (int32_t)strlen(pRow->rowData);
  if (len >= TSDB_TABLE_FNAME_LEN) {
    free(pTable);
    return TSDB_CODE_MND_INVALID_TABLE_ID;
  }
  pTable->info.tableId = strdup(pRow->rowData);
  len++;

  memcpy((char *)pTable + sizeof(char *), (char *)pRow->rowData + len, tsChildTableUpdateSize);
  len += tsChildTableUpdateSize;

  if (pTable->info.type != TSDB_CHILD_TABLE) {
    int32_t schemaSize = pTable->numOfColumns * sizeof(SSchema);
    pTable->schema = (SSchema *)malloc(schemaSize);
    if (pTable->schema == NULL) {
      mnodeDestroyChildTable(pTable);
      return TSDB_CODE_MND_INVALID_TABLE_TYPE;
    }
    memcpy(pTable->schema, (char *)pRow->rowData + len, schemaSize);
    len += schemaSize;

    if (pTable->sqlLen != 0) {
      pTable->sql = malloc(pTable->sqlLen);
      if (pTable->sql == NULL) {
        mnodeDestroyChildTable(pTable);
        return TSDB_CODE_MND_OUT_OF_MEMORY;
      }
      memcpy(pTable->sql, (char *)pRow->rowData + len, pTable->sqlLen);
    }
  }

  pRow->pObj = pTable;
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeChildTableActionRestored() {
#if 0
  void *pIter = NULL;
  SCTableObj *pTable = NULL;

  while (1) {
    pIter = mnodeGetNextChildTable(pIter, &pTable);
    if (pTable == NULL) break;

    SDbObj *pDb = mnodeGetDbByTableName(pTable->info.tableId);
    if (pDb == NULL || pDb->status != TSDB_DB_STATUS_READY) {
      mError("ctable:%s, failed to get db or db in dropping, discard it", pTable->info.tableId);
      SSdbRow desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .pTable = tsChildTableSdb};
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
      SSdbRow desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .pTable = tsChildTableSdb};
      sdbDeleteRow(&desc);
      mnodeDecTableRef(pTable);
      continue;
    }
    mnodeDecVgroupRef(pVgroup);

    if (strcmp(pVgroup->dbName, pDb->name) != 0) {
      mError("ctable:%s, db:%s not match with vgId:%d db:%s sid:%d, discard it",
             pTable->info.tableId, pDb->name, pTable->vgId, pVgroup->dbName, pTable->tid);
      pTable->vgId = 0;
      SSdbRow desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .pTable = tsChildTableSdb};
      sdbDeleteRow(&desc);
      mnodeDecTableRef(pTable);
      continue;
    }

    if (pTable->info.type == TSDB_CHILD_TABLE) {
      SSTableObj *pSuperTable = mnodeGetSuperTableByUid(pTable->suid);
      if (pSuperTable == NULL) {
        mError("ctable:%s, stable:%" PRIu64 " not exist", pTable->info.tableId, pTable->suid);
        pTable->vgId = 0;
        SSdbRow desc = {.type = SDB_OPER_LOCAL, .pObj = pTable, .pTable = tsChildTableSdb};
        sdbDeleteRow(&desc);
        mnodeDecTableRef(pTable);
        continue;
      }
      mnodeDecTableRef(pSuperTable);
    }

    mnodeDecTableRef(pTable);
  }

  mnodeCancelGetNextChildTable(pIter);
#endif
  return 0;
}

static int32_t mnodeInitChildTables() {
  SCTableObj tObj;
  tsChildTableUpdateSize = (int32_t)((int8_t *)tObj.updateEnd - (int8_t *)&tObj.info.type);

  SSdbTableDesc desc = {
    .id           = SDB_TABLE_CTABLE,
    .name         = "ctables",
    .hashSessions = TSDB_DEFAULT_CTABLES_HASH_SIZE,
    .maxRowSize   = sizeof(SCTableObj) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16) + TSDB_TABLE_FNAME_LEN + TSDB_CQ_SQL_SIZE,
    .refCountPos  = (int32_t)((int8_t *)(&tObj.refCount) - (int8_t *)&tObj),
    .keyType      = SDB_KEY_VAR_STRING,
    .fpInsert     = mnodeChildTableActionInsert,
    .fpDelete     = mnodeChildTableActionDelete,
    .fpUpdate     = mnodeChildTableActionUpdate,
    .fpEncode     = mnodeChildTableActionEncode,
    .fpDecode     = mnodeChildTableActionDecode,
    .fpDestroy    = mnodeChildTableActionDestroy,
    .fpRestored   = mnodeChildTableActionRestored
  };

  tsCTableRid = sdbOpenTable(&desc);
  tsChildTableSdb = sdbGetTableByRid(tsCTableRid);
  if (tsChildTableSdb == NULL) {
    mError("failed to init child table data");
    return -1;
  }

  mDebug("table:ctables is created");
  return 0;
}

static void mnodeCleanupChildTables() {
  sdbCloseTable(tsCTableRid);
  tsChildTableSdb = NULL;
}

int64_t mnodeGetSuperTableNum() {
  return sdbGetNumOfRows(tsSuperTableSdb);
}

int64_t mnodeGetChildTableNum() {
  return sdbGetNumOfRows(tsChildTableSdb);
}

static void mnodeAddTableIntoStable(SSTableObj *pStable, SCTableObj *pCtable) {
  atomic_add_fetch_32(&pStable->numOfTables, 1);

  if (pStable->vgHash == NULL) {
    pStable->vgHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_ENTRY_LOCK);
    mDebug("table:%s, create hash:%p", pStable->info.tableId, pStable->vgHash);
  }

  if (pStable->vgHash != NULL) {
    if (taosHashGet(pStable->vgHash, &pCtable->vgId, sizeof(pCtable->vgId)) == NULL) {
      taosHashPut(pStable->vgHash, &pCtable->vgId, sizeof(pCtable->vgId), &pCtable->vgId, sizeof(pCtable->vgId));
      mDebug("table:%s, vgId:%d is put into stable hash:%p, sizeOfVgList:%d", pStable->info.tableId, pCtable->vgId,
             pStable->vgHash, taosHashGetSize(pStable->vgHash));
    }
  }
}

static void mnodeRemoveTableFromStable(SSTableObj *pStable, SCTableObj *pCtable) {
  atomic_sub_fetch_32(&pStable->numOfTables, 1);

  if (pStable->vgHash == NULL) return;

  SVgObj *pVgroup = mnodeGetVgroup(pCtable->vgId);
  if (pVgroup == NULL) {
    taosHashRemove(pStable->vgHash, &pCtable->vgId, sizeof(pCtable->vgId));
    mDebug("table:%s, vgId:%d is remove from stable hash:%p sizeOfVgList:%d", pStable->info.tableId, pCtable->vgId,
           pStable->vgHash, taosHashGetSize(pStable->vgHash));
  }
  mnodeDecVgroupRef(pVgroup);
}

static void mnodeDestroySuperTable(SSTableObj *pStable) {
  mDebug("table:%s, is destroyed, stable hash:%p", pStable->info.tableId, pStable->vgHash);
  if (pStable->vgHash != NULL) {
    taosHashCleanup(pStable->vgHash);
    pStable->vgHash = NULL;
  }
  tfree(pStable->info.tableId);
  tfree(pStable->schema);
  tfree(pStable);
}

static int32_t mnodeSuperTableActionDestroy(SSdbRow *pRow) {
  mnodeDestroySuperTable(pRow->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeSuperTableActionInsert(SSdbRow *pRow) {
  SSTableObj *pStable = pRow->pObj;
  SDbObj *pDb = mnodeGetDbByTableName(pStable->info.tableId);
  if (pDb != NULL && pDb->status == TSDB_DB_STATUS_READY) {
    mnodeAddSuperTableIntoDb(pDb);
  }
  mnodeDecDbRef(pDb);

  taosHashPut(tsSTableUidHash, &pStable->uid, sizeof(int64_t), &pStable, sizeof(int64_t));
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeSuperTableActionDelete(SSdbRow *pRow) {
  SSTableObj *pStable = pRow->pObj;
  SDbObj *pDb = mnodeGetDbByTableName(pStable->info.tableId);
  if (pDb != NULL) {
    mnodeRemoveSuperTableFromDb(pDb);
    mnodeDropAllChildTablesInStable((SSTableObj *)pStable);
  }
  mnodeDecDbRef(pDb);

  taosHashRemove(tsSTableUidHash, &pStable->uid, sizeof(int64_t));
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeSuperTableActionUpdate(SSdbRow *pRow) {
  SSTableObj *pNew = pRow->pObj;
  SSTableObj *pTable = mnodeGetSuperTable(pNew->info.tableId);
  if (pTable != NULL && pTable != pNew) {
    mDebug("table:%s, will be updated, hash:%p sizeOfVgList:%d, new hash:%p sizeOfVgList:%d", pTable->info.tableId,
           pTable->vgHash, taosHashGetSize(pTable->vgHash), pNew->vgHash, taosHashGetSize(pNew->vgHash));

    void *oldTableId = pTable->info.tableId;
    void *oldSchema = pTable->schema;
    void *oldVgHash = pTable->vgHash;
    int32_t oldRefCount = pTable->refCount;
    int32_t oldNumOfTables = pTable->numOfTables;

    memcpy(pTable, pNew, sizeof(SSTableObj));

    pTable->vgHash = oldVgHash;
    pTable->refCount = oldRefCount;
    pTable->schema = pNew->schema;
    pTable->numOfTables = oldNumOfTables;
    free(pNew);
    free(oldTableId);
    free(oldSchema);

    mDebug("table:%s, update finished, hash:%p sizeOfVgList:%d", pTable->info.tableId, pTable->vgHash,
           taosHashGetSize(pTable->vgHash));
  }

  mnodeDecTableRef(pTable);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeSuperTableActionEncode(SSdbRow *pRow) {
  SSTableObj *pStable = pRow->pObj;
  assert(pRow->pObj != NULL && pRow->rowData != NULL);

  int32_t len = (int32_t)strlen(pStable->info.tableId);
  if (len >= TSDB_TABLE_FNAME_LEN) len = TSDB_CODE_MND_INVALID_TABLE_ID;

  memcpy(pRow->rowData, pStable->info.tableId, len);
  memset((char *)pRow->rowData + len, 0, 1);
  len++;

  memcpy((char *)pRow->rowData + len, (char *)pStable + sizeof(char *), tsSuperTableUpdateSize);
  len += tsSuperTableUpdateSize;

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfColumns + pStable->numOfTags);
  memcpy((char *)pRow->rowData + len, pStable->schema, schemaSize);
  len += schemaSize;

  pRow->rowSize = len;

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeSuperTableActionDecode(SSdbRow *pRow) {
  assert(pRow->rowData != NULL);
  SSTableObj *pStable = (SSTableObj *) calloc(1, sizeof(SSTableObj));
  if (pStable == NULL) return TSDB_CODE_MND_OUT_OF_MEMORY;

  int32_t len = (int32_t)strlen(pRow->rowData);
  if (len >= TSDB_TABLE_FNAME_LEN){
    free(pStable);
    return TSDB_CODE_MND_INVALID_TABLE_ID;
  }
  pStable->info.tableId = strdup(pRow->rowData);
  len++;

  memcpy((char *)pStable + sizeof(char *), (char *)pRow->rowData + len, tsSuperTableUpdateSize);
  len += tsSuperTableUpdateSize;

  int32_t schemaSize = sizeof(SSchema) * (pStable->numOfColumns + pStable->numOfTags);
  pStable->schema = malloc(schemaSize);
  if (pStable->schema == NULL) {
    mnodeDestroySuperTable(pStable);
    return TSDB_CODE_MND_NOT_SUPER_TABLE;
  }

  memcpy(pStable->schema, (char *)pRow->rowData + len, schemaSize);
  pRow->pObj = pStable;

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeSuperTableActionRestored() {
  return 0;
}

static int32_t mnodeInitSuperTables() {
  SSTableObj tObj;
  tsSuperTableUpdateSize = (int32_t)((int8_t *)tObj.updateEnd - (int8_t *)&tObj.info.type);

  SSdbTableDesc desc = {
    .id           = SDB_TABLE_STABLE,
    .name         = "stables",
    .hashSessions = TSDB_DEFAULT_STABLES_HASH_SIZE,
    .maxRowSize   = sizeof(SSTableObj) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16) + TSDB_TABLE_FNAME_LEN,
    .refCountPos  = (int32_t)((int8_t *)(&tObj.refCount) - (int8_t *)&tObj),
    .keyType      = SDB_KEY_VAR_STRING,
    .fpInsert     = mnodeSuperTableActionInsert,
    .fpDelete     = mnodeSuperTableActionDelete,
    .fpUpdate     = mnodeSuperTableActionUpdate,
    .fpEncode     = mnodeSuperTableActionEncode,
    .fpDecode     = mnodeSuperTableActionDecode,
    .fpDestroy    = mnodeSuperTableActionDestroy,
    .fpRestored   = mnodeSuperTableActionRestored
  };

  tsSTableUidHash = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  tsSTableRid = sdbOpenTable(&desc);
  tsSuperTableSdb = sdbGetTableByRid(tsSTableRid);
  if (tsSuperTableSdb == NULL) {
    mError("failed to init stables data");
    return -1;
  }

  mDebug("table:stables is created");
  return 0;
}

static void mnodeCleanupSuperTables() {
  sdbCloseTable(tsSTableRid);
  tsSuperTableSdb = NULL;

  taosHashCleanup(tsSTableUidHash);
  tsSTableUidHash = NULL;
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
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_TABLE, mnodeCancelGetNextChildTable);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_METRIC, mnodeGetShowSuperTableMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_METRIC, mnodeRetrieveShowSuperTables);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_METRIC, mnodeCancelGetNextSuperTable);
  mnodeAddShowMetaHandle(TSDB_MGMT_TABLE_STREAMTABLES, mnodeGetStreamTableMeta);
  mnodeAddShowRetrieveHandle(TSDB_MGMT_TABLE_STREAMTABLES, mnodeRetrieveStreamTables);
  mnodeAddShowFreeIterHandle(TSDB_MGMT_TABLE_STREAMTABLES, mnodeCancelGetNextChildTable);

  return TSDB_CODE_SUCCESS;
}

static void *mnodeGetChildTable(char *tableId) {
  return sdbGetRow(tsChildTableSdb, tableId);
}

static void *mnodeGetSuperTable(char *tableId) {
  return sdbGetRow(tsSuperTableSdb, tableId);
}

static void *mnodeGetSuperTableByUid(uint64_t uid) {
  SSTableObj **ppStable = taosHashGet(tsSTableUidHash, &uid, sizeof(int64_t));
  if (ppStable == NULL || *ppStable == NULL) return NULL;

  SSTableObj *pStable = *ppStable;
  mnodeIncTableRef(pStable);
  return pStable;
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

void *mnodeGetNextChildTable(void *pIter, SCTableObj **pTable) {
  return sdbFetchRow(tsChildTableSdb, pIter, (void **)pTable);
}

void mnodeCancelGetNextChildTable(void *pIter) {
  sdbFreeIter(tsChildTableSdb, pIter);
}

void *mnodeGetNextSuperTable(void *pIter, SSTableObj **pTable) {
  return sdbFetchRow(tsSuperTableSdb, pIter, (void **)pTable);
}

void mnodeCancelGetNextSuperTable(void *pIter) {
  sdbFreeIter(tsSuperTableSdb, pIter);
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

static SMnodeMsg *mnodeCreateSubMsg(SMnodeMsg *pBatchMasterMsg, int32_t contSize) {
  SMnodeMsg *pSubMsg = taosAllocateQitem(sizeof(*pBatchMasterMsg) + contSize);
  *pSubMsg = *pBatchMasterMsg;

  //pSubMsg->pCont = (char *) pSubMsg + sizeof(SMnodeMsg);
  pSubMsg->rpcMsg.pCont = pSubMsg->pCont;
  pSubMsg->successed = 0;
  pSubMsg->expected = 0;
  SCMCreateTableMsg *pCM = pSubMsg->rpcMsg.pCont;
  pCM->numOfTables = htonl(1);
  pCM->contLen = htonl(contSize);

  return pSubMsg;
}

void mnodeDestroySubMsg(SMnodeMsg *pSubMsg) {
  if (pSubMsg) {
    // pUser is retained in batch master msg
    if (pSubMsg->pDb) mnodeDecDbRef(pSubMsg->pDb);
    if (pSubMsg->pVgroup) mnodeDecVgroupRef(pSubMsg->pVgroup);
    if (pSubMsg->pTable) mnodeDecTableRef(pSubMsg->pTable);
    if (pSubMsg->pSTable) mnodeDecTableRef(pSubMsg->pSTable);
    if (pSubMsg->pAcct) mnodeDecAcctRef(pSubMsg->pAcct);
    if (pSubMsg->pDnode) mnodeDecDnodeRef(pSubMsg->pDnode);

    taosFreeQitem(pSubMsg);
  }
}

static int32_t mnodeValidateCreateTableMsg(SCreateTableMsg *pCreateTable, SMnodeMsg *pMsg) {
  if (pMsg->pDb == NULL) {
    pMsg->pDb = mnodeGetDbByTableName(pCreateTable->tableName);
  }

  if (pMsg->pDb == NULL) {
    mError("msg:%p, app:%p table:%s, failed to create, db not selected", pMsg, pMsg->rpcMsg.ahandle, pCreateTable->tableName);
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }

  if (pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pMsg->pDb->name, pMsg->pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pCreateTable->tableName);
  if (pMsg->pTable != NULL && pMsg->retry == 0) {
    if (pCreateTable->getMeta) {
      mDebug("msg:%p, app:%p table:%s, continue to get meta", pMsg, pMsg->rpcMsg.ahandle, pCreateTable->tableName);
      return mnodeGetChildTableMeta(pMsg);
    } else if (pCreateTable->igExists) {
      mDebug("msg:%p, app:%p table:%s, is already exist", pMsg, pMsg->rpcMsg.ahandle, pCreateTable->tableName);
      return TSDB_CODE_SUCCESS;
    } else {
      mError("msg:%p, app:%p table:%s, failed to create, table already exist", pMsg, pMsg->rpcMsg.ahandle,
             pCreateTable->tableName);
      return TSDB_CODE_MND_TABLE_ALREADY_EXIST;
    }
  }

  if (pCreateTable->numOfTags != 0) {
    mDebug("msg:%p, app:%p table:%s, create stable msg is received from thandle:%p", pMsg, pMsg->rpcMsg.ahandle,
           pCreateTable->tableName, pMsg->rpcMsg.handle);
    return mnodeProcessCreateSuperTableMsg(pMsg);
  } else {
    mDebug("msg:%p, app:%p table:%s, create ctable msg is received from thandle:%p", pMsg, pMsg->rpcMsg.ahandle,
           pCreateTable->tableName, pMsg->rpcMsg.handle);
    return mnodeProcessCreateChildTableMsg(pMsg);
  }
}

static int32_t mnodeProcessBatchCreateTableMsg(SMnodeMsg *pMsg) {
  if (pMsg->pBatchMasterMsg == NULL) { // batch master first round
    pMsg->pBatchMasterMsg = pMsg;

    SCMCreateTableMsg *pCreate = pMsg->rpcMsg.pCont;
    int32_t numOfTables = htonl(pCreate->numOfTables);
    int32_t contentLen = htonl(pCreate->contLen);
    pMsg->expected = numOfTables;

    int32_t code = TSDB_CODE_SUCCESS;
    SCreateTableMsg *pCreateTable = (SCreateTableMsg*) ((char*) pCreate + sizeof(SCMCreateTableMsg));
    for (SCreateTableMsg *p = pCreateTable; p < (SCreateTableMsg *) ((char *) pCreate + contentLen); p = (SCreateTableMsg *) ((char *) p + htonl(p->len))) {
      SMnodeMsg *pSubMsg = mnodeCreateSubMsg(pMsg, sizeof(SCMCreateTableMsg) + htonl(p->len));
      memcpy(pSubMsg->pCont + sizeof(SCMCreateTableMsg), p, htonl(p->len));
      code = mnodeValidateCreateTableMsg(p, pSubMsg);

      if (code == TSDB_CODE_SUCCESS || code == TSDB_CODE_MND_TABLE_ALREADY_EXIST) {
	++pSubMsg->pBatchMasterMsg->successed;
	mnodeDestroySubMsg(pSubMsg);
	continue;
      }

      if (code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
	mnodeDestroySubMsg(pSubMsg);
	return code;
      }
    }

    if (pMsg->successed >= pMsg->expected) {
      return code;
    } else {
      return TSDB_CODE_MND_ACTION_IN_PROGRESS;
    }
  } else {
    if (pMsg->pBatchMasterMsg != pMsg) { // batch sub replay
      SCMCreateTableMsg *pCreate = pMsg->rpcMsg.pCont;
      SCreateTableMsg *pCreateTable = (SCreateTableMsg*) ((char*) pCreate + sizeof(SCMCreateTableMsg));
      int32_t code = mnodeValidateCreateTableMsg(pCreateTable, pMsg);
      if (code == TSDB_CODE_SUCCESS || code == TSDB_CODE_MND_TABLE_ALREADY_EXIST) {
        ++pMsg->pBatchMasterMsg->successed;
        mnodeDestroySubMsg(pMsg);
      } else if (code == TSDB_CODE_MND_ACTION_NEED_REPROCESSED) {
        return code;
      } else if (code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
        ++pMsg->pBatchMasterMsg->received;
        mnodeDestroySubMsg(pMsg);
      }

      if (pMsg->pBatchMasterMsg->successed + pMsg->pBatchMasterMsg->received
	  >= pMsg->pBatchMasterMsg->expected) {
        dnodeSendRpcMWriteRsp(pMsg->pBatchMasterMsg, TSDB_CODE_SUCCESS);
      }

      return TSDB_CODE_MND_ACTION_IN_PROGRESS;
    } else { // batch master replay, reprocess the whole batch
      assert(0);
      return TSDB_CODE_MND_MSG_NOT_PROCESSED;
    }
  }
}

static int32_t mnodeProcessCreateTableMsg(SMnodeMsg *pMsg) {
  SCMCreateTableMsg *pCreate = pMsg->rpcMsg.pCont;

  int32_t numOfTables = htonl(pCreate->numOfTables);
  int32_t contentLen = htonl(pCreate->contLen);
  if (numOfTables == 0 || contentLen == 0) {
    // todo return error
  }

  // batch master msg first round or reprocessing and batch sub msg reprocessing
  if (numOfTables > 1 || pMsg->pBatchMasterMsg != NULL) {
    return mnodeProcessBatchCreateTableMsg(pMsg);
  }

  SCreateTableMsg *p = (SCreateTableMsg*)((char*) pCreate + sizeof(SCMCreateTableMsg));
  if (pMsg->pDb == NULL) {
    pMsg->pDb = mnodeGetDbByTableName(p->tableName);
  }

  if (pMsg->pDb == NULL) {
    mError("msg:%p, app:%p table:%s, failed to create, db not selected", pMsg, pMsg->rpcMsg.ahandle, p->tableName);
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }

  if (pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pMsg->pDb->name, pMsg->pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(p->tableName);
  if (pMsg->pTable != NULL && pMsg->retry == 0) {
    if (p->getMeta) {
      mDebug("msg:%p, app:%p table:%s, continue to get meta", pMsg, pMsg->rpcMsg.ahandle, p->tableName);
      return mnodeGetChildTableMeta(pMsg);
    } else if (p->igExists) {
      mDebug("msg:%p, app:%p table:%s, is already exist", pMsg, pMsg->rpcMsg.ahandle, p->tableName);
      return TSDB_CODE_SUCCESS;
    } else {
      mError("msg:%p, app:%p table:%s, failed to create, table already exist", pMsg, pMsg->rpcMsg.ahandle, p->tableName);
      return TSDB_CODE_MND_TABLE_ALREADY_EXIST;
    }
  }

  if (p->numOfTags != 0) {
    mDebug("msg:%p, app:%p table:%s, create stable msg is received from thandle:%p", pMsg, pMsg->rpcMsg.ahandle,
           p->tableName, pMsg->rpcMsg.handle);
    return mnodeProcessCreateSuperTableMsg(pMsg);
  } else {
    mDebug("msg:%p, app:%p table:%s, create ctable msg is received from thandle:%p", pMsg, pMsg->rpcMsg.ahandle,
           p->tableName, pMsg->rpcMsg.handle);
    return mnodeProcessCreateChildTableMsg(pMsg);
  }
}

static int32_t mnodeProcessDropTableMsg(SMnodeMsg *pMsg) {
  SCMDropTableMsg *pDrop = pMsg->rpcMsg.pCont;
  if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDbByTableName(pDrop->name);
  if (pMsg->pDb == NULL) {
    mError("msg:%p, app:%p table:%s, failed to drop table, db not selected or db in dropping", pMsg,
           pMsg->rpcMsg.ahandle, pDrop->name);
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }

  if (pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pMsg->pDb->name, pMsg->pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  if (mnodeCheckIsMonitorDB(pMsg->pDb->name, tsMonitorDbName)) {
    mError("msg:%p, app:%p table:%s, failed to drop table, in monitor database", pMsg, pMsg->rpcMsg.ahandle,
           pDrop->name);
    return TSDB_CODE_MND_MONITOR_DB_FORBIDDEN;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pDrop->name);
  if (pMsg->pTable == NULL) {
    if (pDrop->igNotExists) {
      mDebug("msg:%p, app:%p table:%s is not exist, treat as success", pMsg, pMsg->rpcMsg.ahandle, pDrop->name);
      return TSDB_CODE_SUCCESS;
    } else {
      mError("msg:%p, app:%p table:%s, failed to drop, table not exist", pMsg, pMsg->rpcMsg.ahandle, pDrop->name);
      return TSDB_CODE_MND_INVALID_TABLE_NAME;
    }
  }

  if (pMsg->pTable->type == TSDB_SUPER_TABLE) {
    SSTableObj *pSTable = (SSTableObj *)pMsg->pTable;
    mInfo("msg:%p, app:%p table:%s, start to drop stable, uid:%" PRIu64 ", numOfChildTables:%d, sizeOfVgList:%d", pMsg,
          pMsg->rpcMsg.ahandle, pDrop->name, pSTable->uid, pSTable->numOfTables, taosHashGetSize(pSTable->vgHash));
    return mnodeProcessDropSuperTableMsg(pMsg);
  } else {
    SCTableObj *pCTable = (SCTableObj *)pMsg->pTable;
    mInfo("msg:%p, app:%p table:%s, start to drop ctable, vgId:%d tid:%d uid:%" PRIu64, pMsg, pMsg->rpcMsg.ahandle,
          pDrop->name, pCTable->vgId, pCTable->tid, pCTable->uid);
    return mnodeProcessDropChildTableMsg(pMsg);
  }
}

static int32_t mnodeProcessTableMetaMsg(SMnodeMsg *pMsg) {
  STableInfoMsg *pInfo = pMsg->rpcMsg.pCont;
  pInfo->createFlag = htons(pInfo->createFlag);
  mDebug("msg:%p, app:%p table:%s, table meta msg is received from thandle:%p, createFlag:%d", pMsg, pMsg->rpcMsg.ahandle,
         pInfo->tableFname, pMsg->rpcMsg.handle, pInfo->createFlag);

  if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDbByTableName(pInfo->tableFname);
  if (pMsg->pDb == NULL) {
    mError("msg:%p, app:%p table:%s, failed to get table meta, db not selected", pMsg, pMsg->rpcMsg.ahandle,
           pInfo->tableFname);
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }

  if (pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pMsg->pDb->name, pMsg->pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pInfo->tableFname);
  if (pMsg->pTable == NULL) {
    if (!pInfo->createFlag) {
      mError("msg:%p, app:%p table:%s, failed to get table meta, table not exist", pMsg, pMsg->rpcMsg.ahandle,
             pInfo->tableFname);
      return TSDB_CODE_MND_INVALID_TABLE_NAME;
    } else {
      mDebug("msg:%p, app:%p table:%s, failed to get table meta, start auto create table ", pMsg, pMsg->rpcMsg.ahandle,
             pInfo->tableFname);
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
  SSTableObj *pTable = (SSTableObj *)pMsg->pTable;
  assert(pTable);

  if (code == TSDB_CODE_SUCCESS) {
    mLInfo("stable:%s, is created in sdb, uid:%" PRIu64, pTable->info.tableId, pTable->uid);
  } else {
    mError("msg:%p, app:%p stable:%s, failed to create in sdb, reason:%s", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId,
           tstrerror(code));
    SSdbRow desc = {.type = SDB_OPER_GLOBAL, .pObj = pTable, .pTable = tsSuperTableSdb};
    sdbDeleteRow(&desc);
  }

  return code;
}

static int32_t mnodeProcessCreateSuperTableMsg(SMnodeMsg *pMsg) {
  if (pMsg == NULL) return TSDB_CODE_MND_APP_ERROR;

  SCMCreateTableMsg *pCreate1 = pMsg->rpcMsg.pCont;
  if (pCreate1->numOfTables == 0) {
    return TSDB_CODE_MND_INVALID_CREATE_TABLE_MSG;
  }

  SCreateTableMsg* pCreate = (SCreateTableMsg*)((char*)pCreate1 + sizeof(SCMCreateTableMsg));

  SSTableObj *   pStable = calloc(1, sizeof(SSTableObj));
  if (pStable == NULL) {
    mError("msg:%p, app:%p table:%s, failed to create, no enough memory", pMsg, pMsg->rpcMsg.ahandle, pCreate->tableName);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  int64_t us = taosGetTimestampUs();
  pStable->info.tableId = strdup(pCreate->tableName);
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
    mError("msg:%p, app:%p table:%s, failed to create, no schema input", pMsg, pMsg->rpcMsg.ahandle, pCreate->tableName);
    return TSDB_CODE_MND_INVALID_TABLE_NAME;
  }

  memcpy(pStable->schema, pCreate->schema, numOfCols * sizeof(SSchema));

  if (pStable->numOfColumns > TSDB_MAX_COLUMNS || pStable->numOfTags > TSDB_MAX_TAGS) {
    mError("msg:%p, app:%p table:%s, failed to create, too many columns", pMsg, pMsg->rpcMsg.ahandle, pCreate->tableName);
    return TSDB_CODE_MND_INVALID_TABLE_NAME;
  }

  pStable->nextColId = 0;

  for (int32_t col = 0; col < numOfCols; col++) {
    SSchema *tschema = pStable->schema;
    tschema[col].colId = pStable->nextColId++;
    tschema[col].bytes = htons(tschema[col].bytes);
  }

  if (!tIsValidSchema(pStable->schema, pStable->numOfColumns, pStable->numOfTags)) {
    mError("msg:%p, app:%p table:%s, failed to create table, invalid schema", pMsg, pMsg->rpcMsg.ahandle, pCreate->tableName);
    return TSDB_CODE_MND_INVALID_CREATE_TABLE_MSG;
  }

  pMsg->pTable = (STableObj *)pStable;
  mnodeIncTableRef(pMsg->pTable);

  SSdbRow row = {
    .type    = SDB_OPER_GLOBAL,
    .pTable  = tsSuperTableSdb,
    .pObj    = pStable,
    .rowSize = sizeof(SSTableObj) + schemaSize,
    .pMsg    = pMsg,
    .fpRsp   = mnodeCreateSuperTableCb
  };

  int32_t code = sdbInsertRow(&row);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mnodeDestroySuperTable(pStable);
    pMsg->pTable = NULL;
    mError("msg:%p, app:%p table:%s, failed to create, sdb error", pMsg, pMsg->rpcMsg.ahandle, pCreate->tableName);
  }

  return code;
}

static int32_t mnodeDropSuperTableCb(SMnodeMsg *pMsg, int32_t code) {
  SSTableObj *pTable = (SSTableObj *)pMsg->pTable;
  if (code != TSDB_CODE_SUCCESS) {
    mError("msg:%p, app:%p stable:%s, failed to drop, sdb error", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId);

    return code;
  }

  mLInfo("msg:%p, app:%p stable:%s, is dropped from sdb", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId);

  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  if (pStable->vgHash != NULL /*pStable->numOfTables != 0*/) {
    int32_t *pVgId = taosHashIterate(pStable->vgHash, NULL);
    while (pVgId) {
      SVgObj *pVgroup = mnodeGetVgroup(*pVgId);
      pVgId = taosHashIterate(pStable->vgHash, pVgId);
      if (pVgroup == NULL) break;

      SDropSTableMsg *pDrop = rpcMallocCont(sizeof(SDropSTableMsg));
      pDrop->contLen = htonl(sizeof(SDropSTableMsg));
      pDrop->vgId = htonl(pVgroup->vgId);
      pDrop->uid = htobe64(pStable->uid);
      mnodeExtractTableName(pStable->info.tableId, pDrop->tableFname);

      mInfo("msg:%p, app:%p stable:%s, send drop stable msg to vgId:%d, hash:%p sizeOfVgList:%d", pMsg,
            pMsg->rpcMsg.ahandle, pStable->info.tableId, pVgroup->vgId, pStable->vgHash,
            taosHashGetSize(pStable->vgHash));
      SRpcEpSet epSet = mnodeGetEpSetFromVgroup(pVgroup);
      SRpcMsg   rpcMsg = {.pCont = pDrop, .contLen = sizeof(SDropSTableMsg), .msgType = TSDB_MSG_TYPE_MD_DROP_STABLE};
      dnodeSendMsgToDnode(&epSet, &rpcMsg);
      mnodeDecVgroupRef(pVgroup);
    }

    taosHashCancelIterate(pStable->vgHash, pVgId);

    mnodeDropAllChildTablesInStable(pStable);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeProcessDropSuperTableMsg(SMnodeMsg *pMsg) {
  if (pMsg == NULL) return TSDB_CODE_MND_APP_ERROR;

  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  mInfo("msg:%p, app:%p stable:%s will be dropped, hash:%p sizeOfVgList:%d", pMsg, pMsg->rpcMsg.ahandle,
        pStable->info.tableId, pStable->vgHash, taosHashGetSize(pStable->vgHash));

  SSdbRow row = {
    .type    = SDB_OPER_GLOBAL,
    .pTable  = tsSuperTableSdb,
    .pObj    = pStable,
    .pMsg    = pMsg,
    .fpRsp   = mnodeDropSuperTableCb
  };

  int32_t code = sdbDeleteRow(&row);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("msg:%p, app:%p table:%s, failed to drop, reason:%s", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId,
           tstrerror(code));
  }

  return code;
}

static int32_t mnodeFindSuperTableTagIndex(SSTableObj *pStable, const char *tagName) {
  SSchema *schema = (SSchema *) pStable->schema;
  for (int32_t tag = 0; tag < pStable->numOfTags; tag++) {
    if (strcasecmp(schema[pStable->numOfColumns + tag].name, tagName) == 0) {
      return tag;
    }
  }

  return -1;
}

static int32_t mnodeAddSuperTableTagCb(SMnodeMsg *pMsg, int32_t code) {
  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  mLInfo("msg:%p, app:%p stable %s, add tag result:%s", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId,
          tstrerror(code));

  return code;
}

static int32_t mnodeAddSuperTableTag(SMnodeMsg *pMsg, SSchema schema[], int32_t ntags) {
  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  if (pStable->numOfTags + ntags > TSDB_MAX_TAGS) {
    mError("msg:%p, app:%p stable:%s, add tag, too many tags", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId);
    return TSDB_CODE_MND_TOO_MANY_TAGS;
  }

  for (int32_t i = 0; i < ntags; i++) {
    if (mnodeFindSuperTableColumnIndex(pStable, schema[i].name) > 0) {
      mError("msg:%p, app:%p stable:%s, add tag, column:%s already exist", pMsg, pMsg->rpcMsg.ahandle,
             pStable->info.tableId, schema[i].name);
      return TSDB_CODE_MND_TAG_ALREAY_EXIST;
    }

    if (mnodeFindSuperTableTagIndex(pStable, schema[i].name) > 0) {
      mError("msg:%p, app:%p stable:%s, add tag, tag:%s already exist", pMsg, pMsg->rpcMsg.ahandle,
             pStable->info.tableId, schema[i].name);
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

  mInfo("msg:%p, app:%p stable %s, start to add tag %s", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId,
         schema[0].name);

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsSuperTableSdb,
    .pObj   = pStable,
    .pMsg   = pMsg,
    .fpRsp  = mnodeAddSuperTableTagCb
  };

  return sdbUpdateRow(&row);
}

static int32_t mnodeDropSuperTableTagCb(SMnodeMsg *pMsg, int32_t code) {
  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  mLInfo("msg:%p, app:%p stable %s, drop tag result:%s", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId,
          tstrerror(code));
  return code;
}

static int32_t mnodeDropSuperTableTag(SMnodeMsg *pMsg, char *tagName) {
  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  int32_t col = mnodeFindSuperTableTagIndex(pStable, tagName);
  if (col < 0) {
    mError("msg:%p, app:%p stable:%s, drop tag, tag:%s not exist", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId,
           tagName);
    return TSDB_CODE_MND_TAG_NOT_EXIST;
  }

  memmove(pStable->schema + pStable->numOfColumns + col, pStable->schema + pStable->numOfColumns + col + 1,
          sizeof(SSchema) * (pStable->numOfTags - col - 1));
  pStable->numOfTags--;
  pStable->tversion++;

  mInfo("msg:%p, app:%p stable %s, start to drop tag %s", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId, tagName);

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsSuperTableSdb,
    .pObj   = pStable,
    .pMsg   = pMsg,
    .fpRsp  = mnodeDropSuperTableTagCb
  };

  return sdbUpdateRow(&row);
}

static int32_t mnodeModifySuperTableTagNameCb(SMnodeMsg *pMsg, int32_t code) {
  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  mLInfo("msg:%p, app:%p stable %s, modify tag result:%s", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId,
         tstrerror(code));
  return code;
}

static int32_t mnodeModifySuperTableTagName(SMnodeMsg *pMsg, char *oldTagName, char *newTagName) {
  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  int32_t col = mnodeFindSuperTableTagIndex(pStable, oldTagName);
  if (col < 0) {
    mError("msg:%p, app:%p stable:%s, failed to modify table tag, oldName: %s, newName: %s", pMsg, pMsg->rpcMsg.ahandle,
           pStable->info.tableId, oldTagName, newTagName);
    return TSDB_CODE_MND_TAG_NOT_EXIST;
  }

  // int32_t  rowSize = 0;
  uint32_t len = (int32_t)strlen(newTagName);
  if (len >= TSDB_COL_NAME_LEN) {
    return TSDB_CODE_MND_COL_NAME_TOO_LONG;
  }

  if (mnodeFindSuperTableTagIndex(pStable, newTagName) >= 0) {
    return TSDB_CODE_MND_TAG_ALREAY_EXIST;
  }

  // update
  SSchema *schema = (SSchema *) (pStable->schema + pStable->numOfColumns + col);
  tstrncpy(schema->name, newTagName, sizeof(schema->name));

  mInfo("msg:%p, app:%p stable %s, start to modify tag %s to %s", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId,
         oldTagName, newTagName);

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsSuperTableSdb,
    .pObj   = pStable,
    .pMsg   = pMsg,
    .fpRsp  = mnodeModifySuperTableTagNameCb
  };

  return sdbUpdateRow(&row);
}

static int32_t mnodeFindSuperTableColumnIndex(SSTableObj *pStable, char *colName) {
  SSchema *schema = (SSchema *) pStable->schema;
  for (int32_t col = 0; col < pStable->numOfColumns; col++) {
    if (strcasecmp(schema[col].name, colName) == 0) {
      return col;
    }
  }

  return -1;
}

static int32_t mnodeAddSuperTableColumnCb(SMnodeMsg *pMsg, int32_t code) {
  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  mLInfo("msg:%p, app:%p stable %s, add column result:%s", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId,
          tstrerror(code));
  return code;
}

static int32_t mnodeAddSuperTableColumn(SMnodeMsg *pMsg, SSchema schema[], int32_t ncols) {
  SDbObj *pDb = pMsg->pDb;
  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  if (ncols <= 0) {
    mError("msg:%p, app:%p stable:%s, add column, ncols:%d <= 0", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId, ncols);
    return TSDB_CODE_MND_APP_ERROR;
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (mnodeFindSuperTableColumnIndex(pStable, schema[i].name) > 0) {
      mError("msg:%p, app:%p stable:%s, add column, column:%s already exist", pMsg, pMsg->rpcMsg.ahandle,
             pStable->info.tableId, schema[i].name);
      return TSDB_CODE_MND_FIELD_ALREAY_EXIST;
    }

    if (mnodeFindSuperTableTagIndex(pStable, schema[i].name) > 0) {
      mError("msg:%p, app:%p stable:%s, add column, tag:%s already exist", pMsg, pMsg->rpcMsg.ahandle,
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

  mInfo("msg:%p, app:%p stable %s, start to add column", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId);

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsSuperTableSdb,
    .pObj   = pStable,
    .pMsg   = pMsg,
    .fpRsp  = mnodeAddSuperTableColumnCb
  };

 return sdbUpdateRow(&row);
}

static int32_t mnodeDropSuperTableColumnCb(SMnodeMsg *pMsg, int32_t code) {
  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  mLInfo("msg:%p, app:%p stable %s, delete column result:%s", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId,
         tstrerror(code));
  return code;
}

static int32_t mnodeDropSuperTableColumn(SMnodeMsg *pMsg, char *colName) {
  SDbObj *pDb = pMsg->pDb;
  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  int32_t col = mnodeFindSuperTableColumnIndex(pStable, colName);
  if (col <= 0) {
    mError("msg:%p, app:%p stable:%s, drop column, column:%s not exist", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId,
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

  mInfo("msg:%p, app:%p stable %s, start to delete column", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId);

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsSuperTableSdb,
    .pObj   = pStable,
    .pMsg   = pMsg,
    .fpRsp  = mnodeDropSuperTableColumnCb
  };

  return sdbUpdateRow(&row);
}

static int32_t mnodeChangeSuperTableColumnCb(SMnodeMsg *pMsg, int32_t code) {
  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  mLInfo("msg:%p, app:%p stable %s, change column result:%s", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId,
         tstrerror(code));
  return code;
}

static int32_t mnodeChangeSuperTableColumn(SMnodeMsg *pMsg, char *oldName, char *newName) {
  SSTableObj *pStable = (SSTableObj *)pMsg->pTable;
  int32_t col = mnodeFindSuperTableColumnIndex(pStable, oldName);
  if (col < 0) {
    mError("msg:%p, app:%p stable:%s, change column, oldName:%s, newName:%s", pMsg, pMsg->rpcMsg.ahandle,
           pStable->info.tableId, oldName, newName);
    return TSDB_CODE_MND_FIELD_NOT_EXIST;
  }

  // int32_t  rowSize = 0;
  uint32_t len = (uint32_t)strlen(newName);
  if (len >= TSDB_COL_NAME_LEN) {
    return TSDB_CODE_MND_COL_NAME_TOO_LONG;
  }

  if (mnodeFindSuperTableColumnIndex(pStable, newName) >= 0) {
    return TSDB_CODE_MND_FIELD_ALREAY_EXIST;
  }

  // update
  SSchema *schema = (SSchema *) (pStable->schema + col);
  tstrncpy(schema->name, newName, sizeof(schema->name));

  mInfo("msg:%p, app:%p stable %s, start to modify column %s to %s", pMsg, pMsg->rpcMsg.ahandle, pStable->info.tableId,
         oldName, newName);

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsSuperTableSdb,
    .pObj   = pStable,
    .pMsg   = pMsg,
    .fpRsp  = mnodeChangeSuperTableColumnCb
  };

  return sdbUpdateRow(&row);
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

  SSchema* tbnameSchema = tGetTbnameColumnSchema();
  pShow->bytes[cols] = tbnameSchema->bytes;
  pSchema[cols].type = tbnameSchema->type;
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
  SSTableObj *pTable = NULL;
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
  prefixLen = (int32_t)strlen(prefix);

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

    int16_t len = (int16_t)strnlen(stableName, TSDB_TABLE_NAME_LEN - 1);
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
  SSTableObj *pTable = NULL;

  char prefix[64] = {0};
  tstrncpy(prefix, pDropDb->name, 64);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = (int32_t)strlen(prefix);

  mInfo("db:%s, all super tables will be dropped from sdb", pDropDb->name);

  while (1) {
    pIter = mnodeGetNextSuperTable(pIter, &pTable);
    if (pTable == NULL) break;

    if (strncmp(prefix, pTable->info.tableId, prefixLen) == 0) {
      SSdbRow row = {
        .type   = SDB_OPER_LOCAL,
        .pTable = tsSuperTableSdb,
        .pObj   = pTable,
      };
      sdbDeleteRow(&row);
      numOfTables ++;
    }

    mnodeDecTableRef(pTable);
  }

  mInfo("db:%s, all super tables:%d is dropped from sdb", pDropDb->name, numOfTables);
}

static int32_t mnodeSetSchemaFromSuperTable(SSchema *pSchema, SSTableObj *pTable) {
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
  SSTableObj *pTable = (SSTableObj *)pMsg->pTable;
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
  tstrncpy(pMeta->tableFname, pTable->info.tableId, sizeof(pMeta->tableFname));

  pMsg->rpcRsp.len = pMeta->contLen;
  pMeta->contLen = htons(pMeta->contLen);

  pMsg->rpcRsp.rsp = pMeta;

  mDebug("msg:%p, app:%p stable:%s, uid:%" PRIu64 " table meta is retrieved, sizeOfVgList:%d numOfTables:%d", pMsg,
         pMsg->rpcMsg.ahandle, pTable->info.tableId, pTable->uid, taosHashGetSize(pTable->vgHash), pTable->numOfTables);
  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeProcessSuperTableVgroupMsg(SMnodeMsg *pMsg) {
  SSTableVgroupMsg *pInfo = pMsg->rpcMsg.pCont;
  int32_t numOfTable = htonl(pInfo->numOfTables);

  // reserve space
  int32_t contLen = sizeof(SSTableVgroupRspMsg) + 32 * sizeof(SVgroupMsg) + sizeof(SVgroupsMsg);
  for (int32_t i = 0; i < numOfTable; ++i) {
    char *stableName = (char *)pInfo + sizeof(SSTableVgroupMsg) + (TSDB_TABLE_FNAME_LEN)*i;
    SSTableObj *pTable = mnodeGetSuperTable(stableName);
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
    char *stableName = (char *)pInfo + sizeof(SSTableVgroupMsg) + (TSDB_TABLE_FNAME_LEN)*i;
    SSTableObj *pTable = mnodeGetSuperTable(stableName);
    if (pTable == NULL) {
      mError("msg:%p, app:%p stable:%s, not exist while get stable vgroup info", pMsg, pMsg->rpcMsg.ahandle, stableName);
      mnodeDecTableRef(pTable);
      continue;
    }
    if (pTable->vgHash == NULL) {
      mDebug("msg:%p, app:%p stable:%s, no vgroup exist while get stable vgroup info", pMsg, pMsg->rpcMsg.ahandle,
             stableName);
      mnodeDecTableRef(pTable);

      // even this super table has no corresponding table, still return
      pRsp->numOfTables++;

      SVgroupsMsg *pVgroupMsg = (SVgroupsMsg *)msg;
      pVgroupMsg->numOfVgroups = 0;

      msg += sizeof(SVgroupsMsg);
    } else {
      SVgroupsMsg *pVgroupMsg = (SVgroupsMsg *)msg;
      mDebug("msg:%p, app:%p stable:%s, hash:%p sizeOfVgList:%d will be returned", pMsg, pMsg->rpcMsg.ahandle,
             pTable->info.tableId, pTable->vgHash, taosHashGetSize(pTable->vgHash));

      int32_t *pVgId = taosHashIterate(pTable->vgHash, NULL);
      int32_t  vgSize = 0;
      while (pVgId) {
        SVgObj *pVgroup = mnodeGetVgroup(*pVgId);
        pVgId = taosHashIterate(pTable->vgHash, pVgId);
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

      taosHashCancelIterate(pTable->vgHash, pVgId);
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
    pRsp->numOfTables = (int32_t)htonl(pRsp->numOfTables);
    pMsg->rpcRsp.rsp = pRsp;
    pMsg->rpcRsp.len = (int32_t)((char *)msg - (char *)pRsp);

    return TSDB_CODE_SUCCESS;
  }
}

static void mnodeProcessDropSuperTableRsp(SRpcMsg *rpcMsg) {
  mInfo("drop stable rsp received, result:%s", tstrerror(rpcMsg->code));
}

static void *mnodeBuildCreateChildTableMsg(SCMCreateTableMsg *pCreateMsg, SCTableObj *pTable) {
  SCreateTableMsg* pMsg = (SCreateTableMsg*) ((char*)pCreateMsg + sizeof(SCMCreateTableMsg));

  char* tagData = NULL;

  int32_t tagDataLen = 0;
  int32_t totalCols = 0;
  int32_t contLen = 0;
  if (pTable->info.type == TSDB_CHILD_TABLE) {
    totalCols = pTable->superTable->numOfColumns + pTable->superTable->numOfTags;
    contLen = sizeof(SMDCreateTableMsg) + totalCols * sizeof(SSchema) + pTable->sqlLen;
    if (pMsg != NULL) {
      int32_t nameLen = htonl(*(int32_t*)pMsg->schema);
      char* p = pMsg->schema + nameLen + sizeof(int32_t);

      tagDataLen = htonl(*(int32_t*) p);
      contLen += tagDataLen;

      tagData = p + sizeof(int32_t);
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

  mnodeExtractTableName(pTable->info.tableId, pCreate->tableFname);
  pCreate->contLen       = htonl(contLen);
  pCreate->vgId          = htonl(pTable->vgId);
  pCreate->tableType     = pTable->info.type;
  pCreate->createdTime   = htobe64(pTable->createdTime);
  pCreate->tid           = htonl(pTable->tid);
  pCreate->sqlDataLen    = htonl(pTable->sqlLen);
  pCreate->uid           = htobe64(pTable->uid);

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    mnodeExtractTableName(pTable->superTable->info.tableId, pCreate->stableFname);
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
    memcpy(pCreate->data + totalCols * sizeof(SSchema), tagData, tagDataLen);
  }

  if (pTable->info.type == TSDB_STREAM_TABLE) {
    memcpy(pCreate->data + totalCols * sizeof(SSchema), pTable->sql, pTable->sqlLen);
  }

  return pCreate;
}

static int32_t mnodeDoCreateChildTableFp(SMnodeMsg *pMsg) {
  SCTableObj *pTable = (SCTableObj *)pMsg->pTable;
  assert(pTable);

  mDebug("msg:%p, app:%p table:%s, created in mnode, vgId:%d sid:%d, uid:%" PRIu64, pMsg, pMsg->rpcMsg.ahandle,
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
  SCTableObj *pTable = (SCTableObj *)pMsg->pTable;

  SCreateTableMsg *pCreate = (SCreateTableMsg*) ((char*)pMsg->rpcMsg.pCont + sizeof(SCMCreateTableMsg));
  assert(pTable);

  if (code == TSDB_CODE_SUCCESS) {
    if (pCreate->getMeta) {
      mDebug("msg:%p, app:%p table:%s, created in dnode and continue to get meta, thandle:%p", pMsg,
             pMsg->rpcMsg.ahandle, pTable->info.tableId, pMsg->rpcMsg.handle);

      pMsg->retry = 0;
      dnodeReprocessMWriteMsg(pMsg);
    } else {
      mDebug("msg:%p, app:%p table:%s, created in dnode, thandle:%p", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId,
             pMsg->rpcMsg.handle);

      if (pMsg->pBatchMasterMsg) {
	++pMsg->pBatchMasterMsg->successed;
	if (pMsg->pBatchMasterMsg->successed + pMsg->pBatchMasterMsg->received
	    >= pMsg->pBatchMasterMsg->expected) {
	  dnodeSendRpcMWriteRsp(pMsg->pBatchMasterMsg, code);
	}

	mnodeDestroySubMsg(pMsg);

	return TSDB_CODE_MND_ACTION_IN_PROGRESS;
      }

      dnodeSendRpcMWriteRsp(pMsg, TSDB_CODE_SUCCESS);
    }
    return TSDB_CODE_MND_ACTION_IN_PROGRESS;
  } else {
    mError("msg:%p, app:%p table:%s, failed to create table sid:%d, uid:%" PRIu64 ", reason:%s", pMsg,
           pMsg->rpcMsg.ahandle, pTable->info.tableId, pTable->tid, pTable->uid, tstrerror(code));
    SSdbRow desc = {.type = SDB_OPER_GLOBAL, .pObj = pTable, .pTable = tsChildTableSdb};
    sdbDeleteRow(&desc);
    return code;
  }
}

static int32_t mnodeDoCreateChildTable(SMnodeMsg *pMsg, int32_t tid) {
  SVgObj *pVgroup = pMsg->pVgroup;

  SCMCreateTableMsg *p1 = pMsg->rpcMsg.pCont;
  SCreateTableMsg   *pCreate = (SCreateTableMsg*)((char*)p1 + sizeof(SCMCreateTableMsg));

  SCTableObj *pTable = calloc(1, sizeof(SCTableObj));
  if (pTable == NULL) {
    mError("msg:%p, app:%p table:%s, failed to alloc memory", pMsg, pMsg->rpcMsg.ahandle, pCreate->tableName);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  pTable->info.type    = (pCreate->numOfColumns == 0)? TSDB_CHILD_TABLE:TSDB_NORMAL_TABLE;
  pTable->info.tableId = strdup(pCreate->tableName);
  pTable->createdTime  = taosGetTimestampMs();
  pTable->tid          = tid;
  pTable->vgId         = pVgroup->vgId;

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    int32_t nameLen = htonl(*(int32_t*) pCreate->schema);
    char* name = (char*)pCreate->schema + sizeof(int32_t);

    char stableName[TSDB_TABLE_FNAME_LEN] = {0};
    memcpy(stableName, name, nameLen);

    char prefix[64] = {0};
    size_t prefixLen = tableIdPrefix(pMsg->pDb->name, prefix, 64);
    if (0 != strncasecmp(prefix, stableName, prefixLen)) {
      mError("msg:%p, app:%p table:%s, corresponding super table:%s not in this db", pMsg, pMsg->rpcMsg.ahandle,
             pCreate->tableName, stableName);
      mnodeDestroyChildTable(pTable);
      return TSDB_CODE_TDB_INVALID_CREATE_TB_MSG;
    }

    if (pMsg->pSTable == NULL) pMsg->pSTable = mnodeGetSuperTable(stableName);
    if (pMsg->pSTable == NULL) {
      mError("msg:%p, app:%p table:%s, corresponding super table:%s does not exist", pMsg, pMsg->rpcMsg.ahandle,
             pCreate->tableName, stableName);
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
      mDebug("msg:%p, app:%p table:%s, stream sql len:%d sql:%s", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId,
             pTable->sqlLen, pTable->sql);
    }
  }

  pMsg->pTable = (STableObj *)pTable;
  mnodeIncTableRef(pMsg->pTable);

  SSdbRow desc = {
    .type   = SDB_OPER_GLOBAL,
    .pObj   = pTable,
    .pTable = tsChildTableSdb,
    .pMsg   = pMsg,
    .fpReq  = mnodeDoCreateChildTableFp
  };

  int32_t code = sdbInsertRow(&desc);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mnodeDestroyChildTable(pTable);
    pMsg->pTable = NULL;
    mError("msg:%p, app:%p table:%s, failed to create, reason:%s", pMsg, pMsg->rpcMsg.ahandle, pCreate->tableName,
           tstrerror(code));
  } else {
    mDebug("msg:%p, app:%p table:%s, allocated in vgroup, vgId:%d sid:%d uid:%" PRIu64, pMsg, pMsg->rpcMsg.ahandle,
           pTable->info.tableId, pVgroup->vgId, pTable->tid, pTable->uid);
  }

  return code;
}

static int32_t mnodeProcessCreateChildTableMsg(SMnodeMsg *pMsg) {
  //SCMCreateTableMsg* p1 = pMsg->rpcMsg.pCont; // there are several tables here.
  SCreateTableMsg* pCreate = (SCreateTableMsg*)((char *)pMsg->rpcMsg.pCont + sizeof(SCMCreateTableMsg));

  int32_t code = grantCheck(TSDB_GRANT_TIMESERIES);
  if (code != TSDB_CODE_SUCCESS) {
    mError("msg:%p, app:%p table:%s, failed to create, grant timeseries failed", pMsg, pMsg->rpcMsg.ahandle,
           pCreate->tableName);
    return code;
  }

  if (pMsg->retry == 0) {
    if (pMsg->pTable == NULL) {
      SVgObj *pVgroup = NULL;
      int32_t tid = 0;
      code = mnodeGetAvailableVgroup(pMsg, &pVgroup, &tid);
      if (code != TSDB_CODE_SUCCESS) {
        mDebug("msg:%p, app:%p table:%s, failed to get available vgroup, reason:%s", pMsg, pMsg->rpcMsg.ahandle,
               pCreate->tableName, tstrerror(code));
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
    if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pCreate->tableName);
  }

  if (pMsg->pTable == NULL) {
    mError("msg:%p, app:%p table:%s, object not found, retry:%d reason:%s", pMsg, pMsg->rpcMsg.ahandle, pCreate->tableName, pMsg->retry,
           tstrerror(terrno));
    return terrno;
  } else {
    mDebug("msg:%p, app:%p table:%s, send create msg to vnode again", pMsg, pMsg->rpcMsg.ahandle, pCreate->tableName);
    return mnodeDoCreateChildTableFp(pMsg);
  }
}

static int32_t mnodeSendDropChildTableMsg(SMnodeMsg *pMsg, bool needReturn) {
  SCTableObj *pTable = (SCTableObj *)pMsg->pTable;
  mLInfo("msg:%p, app:%p ctable:%s, is dropped from sdb", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId);

  SMDDropTableMsg *pDrop = rpcMallocCont(sizeof(SMDDropTableMsg));
  if (pDrop == NULL) {
    mError("msg:%p, app:%p ctable:%s, failed to drop ctable, no enough memory", pMsg, pMsg->rpcMsg.ahandle,
           pTable->info.tableId);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  tstrncpy(pDrop->tableFname, pTable->info.tableId, TSDB_TABLE_FNAME_LEN);
  pDrop->vgId    = htonl(pTable->vgId);
  pDrop->contLen = htonl(sizeof(SMDDropTableMsg));
  pDrop->tid     = htonl(pTable->tid);
  pDrop->uid     = htobe64(pTable->uid);

  SRpcEpSet epSet = mnodeGetEpSetFromVgroup(pMsg->pVgroup);

  mInfo("msg:%p, app:%p ctable:%s, send drop ctable msg, vgId:%d sid:%d uid:%" PRIu64, pMsg, pMsg->rpcMsg.ahandle,
        pDrop->tableFname, pTable->vgId, pTable->tid, pTable->uid);

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
    SCTableObj *pTable = (SCTableObj *)pMsg->pTable;
    mError("msg:%p, app:%p ctable:%s, failed to drop, sdb error", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId);
    return code;
  }

  return mnodeSendDropChildTableMsg(pMsg, true);
}

static int32_t mnodeProcessDropChildTableMsg(SMnodeMsg *pMsg) {
  SCTableObj *pTable = (SCTableObj *)pMsg->pTable;
  if (pMsg->pVgroup == NULL) pMsg->pVgroup = mnodeGetVgroup(pTable->vgId);
  if (pMsg->pVgroup == NULL) {
    mError("msg:%p, app:%p table:%s, failed to drop ctable, vgroup not exist", pMsg, pMsg->rpcMsg.ahandle,
           pTable->info.tableId);
    return TSDB_CODE_MND_APP_ERROR;
  }

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsChildTableSdb,
    .pObj   = pTable,
    .pMsg   = pMsg,
    .fpRsp  = mnodeDropChildTableCb
  };

  int32_t code = sdbDeleteRow(&row);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("msg:%p, app:%p ctable:%s, failed to drop, reason:%s", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId,
           tstrerror(code));
  }

  return code;
}

static int32_t mnodeFindNormalTableColumnIndex(SCTableObj *pTable, char *colName) {
  SSchema *schema = (SSchema *) pTable->schema;
  for (int32_t col = 0; col < pTable->numOfColumns; col++) {
    if (strcasecmp(schema[col].name, colName) == 0) {
      return col;
    }
  }

  return -1;
}

static int32_t mnodeAlterNormalTableColumnCb(SMnodeMsg *pMsg, int32_t code) {
  SCTableObj *pTable = (SCTableObj *)pMsg->pTable;
  if (code != TSDB_CODE_SUCCESS) {
    mError("msg:%p, app:%p ctable %s, failed to alter column, reason:%s", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId,
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
      mError("msg:%p, app:%p ctable %s, vgId:%d not exist in mnode", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId,
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

  mDebug("msg:%p, app:%p ctable %s, send alter column msg to vgId:%d", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId,
         pMsg->pVgroup->vgId);

  dnodeSendMsgToDnode(&epSet, &rpcMsg);
  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mnodeAddNormalTableColumn(SMnodeMsg *pMsg, SSchema schema[], int32_t ncols) {
  SCTableObj *pTable = (SCTableObj *)pMsg->pTable;
  SDbObj *pDb = pMsg->pDb;
  if (ncols <= 0) {
    mError("msg:%p, app:%p ctable:%s, add column, ncols:%d <= 0", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId, ncols);
    return TSDB_CODE_MND_APP_ERROR;
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (mnodeFindNormalTableColumnIndex(pTable, schema[i].name) > 0) {
      mError("msg:%p, app:%p ctable:%s, add column, column:%s already exist", pMsg, pMsg->rpcMsg.ahandle,
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

  mInfo("msg:%p, app:%p ctable %s, start to add column", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId);

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsChildTableSdb,
    .pObj   = pTable,
    .pMsg   = pMsg,
    .fpRsp  = mnodeAlterNormalTableColumnCb
  };

  return sdbUpdateRow(&row);
}

static int32_t mnodeDropNormalTableColumn(SMnodeMsg *pMsg, char *colName) {
  SDbObj *pDb = pMsg->pDb;
  SCTableObj *pTable = (SCTableObj *)pMsg->pTable;
  int32_t col = mnodeFindNormalTableColumnIndex(pTable, colName);
  if (col <= 0) {
    mError("msg:%p, app:%p ctable:%s, drop column, column:%s not exist", pMsg, pMsg->rpcMsg.ahandle,
           pTable->info.tableId, colName);
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

  mInfo("msg:%p, app:%p ctable %s, start to drop column %s", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId, colName);

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsChildTableSdb,
    .pObj   = pTable,
    .pMsg   = pMsg,
    .fpRsp  = mnodeAlterNormalTableColumnCb
  };

  return sdbUpdateRow(&row);
}

static int32_t mnodeChangeNormalTableColumn(SMnodeMsg *pMsg, char *oldName, char *newName) {
  SCTableObj *pTable = (SCTableObj *)pMsg->pTable;
  int32_t col = mnodeFindNormalTableColumnIndex(pTable, oldName);
  if (col < 0) {
    mError("msg:%p, app:%p ctable:%s, change column, oldName: %s, newName: %s", pMsg, pMsg->rpcMsg.ahandle,
           pTable->info.tableId, oldName, newName);
    return TSDB_CODE_MND_FIELD_NOT_EXIST;
  }

  // int32_t  rowSize = 0;
  uint32_t len = (uint32_t)strlen(newName);
  if (len >= TSDB_COL_NAME_LEN) {
    return TSDB_CODE_MND_COL_NAME_TOO_LONG;
  }

  if (mnodeFindNormalTableColumnIndex(pTable, newName) >= 0) {
    return TSDB_CODE_MND_FIELD_ALREAY_EXIST;
  }

  // update
  SSchema *schema = (SSchema *) (pTable->schema + col);
  tstrncpy(schema->name, newName, sizeof(schema->name));

  mInfo("msg:%p, app:%p ctable %s, start to modify column %s to %s", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId,
         oldName, newName);

  SSdbRow row = {
    .type   = SDB_OPER_GLOBAL,
    .pTable = tsChildTableSdb,
    .pObj   = pTable,
    .pMsg   = pMsg,
    .fpRsp  = mnodeAlterNormalTableColumnCb
  };

  return sdbUpdateRow(&row);
}

static int32_t mnodeSetSchemaFromNormalTable(SSchema *pSchema, SCTableObj *pTable) {
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
  SCTableObj *pTable = (SCTableObj *)pMsg->pTable;

  pMeta->uid       = htobe64(pTable->uid);
  pMeta->tid       = htonl(pTable->tid);
  pMeta->precision = pDb->cfg.precision;
  pMeta->tableType = pTable->info.type;
  tstrncpy(pMeta->tableFname, pTable->info.tableId, TSDB_TABLE_FNAME_LEN);

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    assert(pTable->superTable != NULL);
    tstrncpy(pMeta->sTableName, pTable->superTable->info.tableId, TSDB_TABLE_FNAME_LEN);

    pMeta->suid         = pTable->superTable->uid;
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
    mError("msg:%p, app:%p table:%s, failed to get table meta, vgroup not exist", pMsg, pMsg->rpcMsg.ahandle,
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

  mDebug("msg:%p, app:%p table:%s, uid:%" PRIu64 " table meta is retrieved, vgId:%d sid:%d", pMsg, pMsg->rpcMsg.ahandle,
         pTable->info.tableId, pTable->uid, pTable->vgId, pTable->tid);

  return TSDB_CODE_SUCCESS;
}

static int32_t mnodeAutoCreateChildTable(SMnodeMsg *pMsg) {
  STableInfoMsg *pInfo = pMsg->rpcMsg.pCont;

  if (pMsg->rpcMsg.contLen <= sizeof(*pInfo)) {
    mError("msg:%p, app:%p table:%s, failed to auto create child table, tags not exist", pMsg, pMsg->rpcMsg.ahandle,
           pInfo->tableFname);
    return TSDB_CODE_MND_TAG_NOT_EXIST;
  }

  char* p = pInfo->tags;
  int32_t nameLen = htonl(*(int32_t*) p);
  p += sizeof(int32_t);
  p += nameLen;

  int32_t tagLen = htonl(*(int32_t*) p);
  p += sizeof(int32_t);

  int32_t totalLen = nameLen + tagLen + sizeof(int32_t)*2;
  if (tagLen == 0 || nameLen == 0) {
    mError("msg:%p, app:%p table:%s, failed to create table on demand for super table is empty, tagLen:%d", pMsg,
           pMsg->rpcMsg.ahandle, pInfo->tableFname, tagLen);
    return TSDB_CODE_MND_INVALID_STABLE_NAME;
  }

  int32_t contLen = sizeof(SCMCreateTableMsg) + sizeof(SCreateTableMsg) + totalLen;
  SCMCreateTableMsg *pCreateMsg = calloc(1, contLen);
  if (pCreateMsg == NULL) {
    mError("msg:%p, app:%p table:%s, failed to create table while get meta info, no enough memory", pMsg,
           pMsg->rpcMsg.ahandle, pInfo->tableFname);
    return TSDB_CODE_MND_OUT_OF_MEMORY;
  }

  SCreateTableMsg* pCreate = (SCreateTableMsg*) ((char*) pCreateMsg + sizeof(SCMCreateTableMsg));

  size_t size = tListLen(pInfo->tableFname);
  tstrncpy(pCreate->tableName, pInfo->tableFname, size);
  pCreate->igExists = 1;
  pCreate->getMeta = 1;

  pCreateMsg->numOfTables = htonl(1);
  pCreateMsg->contLen = htonl(contLen);

  memcpy(pCreate->schema, pInfo->tags, totalLen);

  char name[TSDB_TABLE_FNAME_LEN] = {0};
  memcpy(name, pInfo->tags + sizeof(int32_t), nameLen);

  mDebug("msg:%p, app:%p table:%s, start to create on demand, tagLen:%d stable:%s", pMsg, pMsg->rpcMsg.ahandle,
         pInfo->tableFname, tagLen, name);

  if (pMsg->rpcMsg.pCont != pMsg->pCont) {
    tfree(pMsg->rpcMsg.pCont);
  }
  pMsg->rpcMsg.msgType = TSDB_MSG_TYPE_CM_CREATE_TABLE;
  pMsg->rpcMsg.pCont = pCreateMsg;
  pMsg->rpcMsg.contLen = contLen;

  return TSDB_CODE_MND_ACTION_NEED_REPROCESSED;
}

static int32_t mnodeGetChildTableMeta(SMnodeMsg *pMsg) {
  STableMetaMsg *pMeta =
      rpcMallocCont(sizeof(STableMetaMsg) + sizeof(SSchema) * (TSDB_MAX_TAGS + TSDB_MAX_COLUMNS + 16));
  if (pMeta == NULL) {
    mError("msg:%p, app:%p table:%s, failed to get table meta, no enough memory", pMsg, pMsg->rpcMsg.ahandle,
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
  SCTableObj *pTable = NULL;

  mInfo("vgId:%d, all child tables will be dropped from sdb", pVgroup->vgId);

  while (1) {
    pIter = mnodeGetNextChildTable(pIter, &pTable);
    if (pTable == NULL) break;

    if (pTable->vgId == pVgroup->vgId) {
      SSdbRow row = {
        .type   = SDB_OPER_LOCAL,
        .pTable = tsChildTableSdb,
        .pObj   = pTable,
      };
      sdbDeleteRow(&row);
      numOfTables++;
    }
    mnodeDecTableRef(pTable);
  }

  mInfo("vgId:%d, all child tables is dropped from sdb", pVgroup->vgId);
}

void mnodeDropAllChildTables(SDbObj *pDropDb) {
  void *  pIter = NULL;
  int32_t numOfTables = 0;
  SCTableObj *pTable = NULL;

  char prefix[64] = {0};
  tstrncpy(prefix, pDropDb->name, 64);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = (int32_t)strlen(prefix);

  mInfo("db:%s, all child tables will be dropped from sdb", pDropDb->name);

  while (1) {
    pIter = mnodeGetNextChildTable(pIter, &pTable);
    if (pTable == NULL) break;

    if (strncmp(prefix, pTable->info.tableId, prefixLen) == 0) {
      SSdbRow row = {
        .type   = SDB_OPER_LOCAL,
        .pTable = tsChildTableSdb,
        .pObj   = pTable,
      };
      sdbDeleteRow(&row);
      numOfTables++;
    }
    mnodeDecTableRef(pTable);
  }

  mInfo("db:%s, all child tables:%d is dropped from sdb", pDropDb->name, numOfTables);
}

static void mnodeDropAllChildTablesInStable(SSTableObj *pStable) {
  void *  pIter = NULL;
  int32_t numOfTables = 0;
  SCTableObj *pTable = NULL;

  mInfo("stable:%s uid:%" PRIu64 ", all child tables:%d will be dropped from sdb", pStable->info.tableId, pStable->uid,
        pStable->numOfTables);

  while (1) {
    pIter = mnodeGetNextChildTable(pIter, &pTable);
    if (pTable == NULL) break;

    if (pTable->superTable == pStable) {
      SSdbRow row = {
        .type   = SDB_OPER_LOCAL,
        .pTable = tsChildTableSdb,
        .pObj   = pTable,
      };
      sdbDeleteRow(&row);
      numOfTables++;
    }

    mnodeDecTableRef(pTable);
  }

  mInfo("stable:%s, all child tables:%d is dropped from sdb", pStable->info.tableId, numOfTables);
}

#if 0
static SCTableObj* mnodeGetTableByPos(int32_t vnode, int32_t tid) {
  SVgObj *pVgroup = mnodeGetVgroup(vnode);
  if (pVgroup == NULL) return NULL;

  SCTableObj *pTable = pVgroup->tableList[tid - 1];
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
  mDebug("msg:%p, app:%p dnode:%d, vgId:%d sid:%d, receive table config msg", pMsg, pMsg->rpcMsg.ahandle, pCfg->dnodeId,
         pCfg->vgId, pCfg->sid);

  SCTableObj *pTable = mnodeGetTableByPos(pCfg->vgId, pCfg->sid);
  if (pTable == NULL) {
    mError("msg:%p, app:%p dnode:%d, vgId:%d sid:%d, table not found", pMsg, pMsg->rpcMsg.ahandle, pCfg->dnodeId,
           pCfg->vgId, pCfg->sid);
    return TSDB_CODE_MND_INVALID_TABLE_ID;
  }

  SMDCreateTableMsg *pCreate = NULL;
  pCreate = mnodeBuildCreateChildTableMsg(NULL, (SCTableObj *)pTable);
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

  SMnodeMsg *pMsg = rpcMsg->ahandle;
  pMsg->received++;

  SCTableObj *pTable = (SCTableObj *)pMsg->pTable;
  assert(pTable);

  mInfo("msg:%p, app:%p table:%s, drop table rsp received, vgId:%d sid:%d uid:%" PRIu64 ", thandle:%p result:%s", pMsg,
        pMsg->rpcMsg.ahandle, pTable->info.tableId, pTable->vgId, pTable->tid, pTable->uid, pMsg->rpcMsg.handle,
        tstrerror(rpcMsg->code));

  if (rpcMsg->code != TSDB_CODE_SUCCESS) {
    mError("msg:%p, app:%p table:%s, failed to drop in dnode, vgId:%d sid:%d uid:%" PRIu64 ", reason:%s", pMsg,
           pMsg->rpcMsg.ahandle, pTable->info.tableId, pTable->vgId, pTable->tid, pTable->uid, tstrerror(rpcMsg->code));
    dnodeSendRpcMWriteRsp(pMsg, rpcMsg->code);
    return;
  }

  if (pMsg->pVgroup == NULL) pMsg->pVgroup = mnodeGetVgroup(pTable->vgId);
  if (pMsg->pVgroup == NULL) {
    mError("msg:%p, app:%p table:%s, failed to get vgroup", pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId);
    dnodeSendRpcMWriteRsp(pMsg, TSDB_CODE_MND_VGROUP_NOT_EXIST);
    return;
  }

  if (pMsg->pVgroup->numOfTables <= 0) {
    mInfo("msg:%p, app:%p vgId:%d, all tables is dropped, drop vgroup", pMsg, pMsg->rpcMsg.ahandle,
          pMsg->pVgroup->vgId);
    mnodeDropVgroup(pMsg->pVgroup, NULL);
  }

  dnodeSendRpcMWriteRsp(pMsg, TSDB_CODE_SUCCESS);
}

/*
 * handle create table response from dnode
 *   if failed, drop the table cached
 */
static void mnodeProcessCreateChildTableRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->ahandle == NULL) return;

  SMnodeMsg *pMsg = rpcMsg->ahandle;
  pMsg->received++;

  SCTableObj *pTable = (SCTableObj *)pMsg->pTable;
  assert(pTable);

  // If the table is deleted by another thread during creation, stop creating and send drop msg to vnode
  if (sdbCheckRowDeleted(tsChildTableSdb, pTable)) {
    mDebug("msg:%p, app:%p table:%s, create table rsp received, but a deleting opertion incoming, vgId:%d sid:%d uid:%" PRIu64,
           pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId, pTable->vgId, pTable->tid, pTable->uid);

    // if the vgroup is already dropped from hash, it can't be accquired by pTable->vgId
    // so the refCount of vgroup can not be decreased
    // SVgObj *pVgroup = mnodeGetVgroup(pTable->vgId);
    // if (pVgroup == NULL) {
    //   mnodeRemoveTableFromVgroup(pMsg->pVgroup, pTable);
    // }
    // mnodeDecVgroupRef(pVgroup);

    mnodeSendDropChildTableMsg(pMsg, false);
    rpcMsg->code = TSDB_CODE_SUCCESS;

    if (pMsg->pBatchMasterMsg) {
      ++pMsg->pBatchMasterMsg->successed;
      if (pMsg->pBatchMasterMsg->successed + pMsg->pBatchMasterMsg->received
	  >= pMsg->pBatchMasterMsg->expected) {
	dnodeSendRpcMWriteRsp(pMsg->pBatchMasterMsg, rpcMsg->code);
      }

      mnodeDestroySubMsg(pMsg);

      return;
    }

    dnodeSendRpcMWriteRsp(pMsg, rpcMsg->code);
    return;
  }

  if (rpcMsg->code == TSDB_CODE_SUCCESS || rpcMsg->code == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
     SSdbRow desc = {
      .type   = SDB_OPER_GLOBAL,
      .pObj   = pTable,
      .pTable = tsChildTableSdb,
      .pMsg   = pMsg,
      .fpRsp  = mnodeDoCreateChildTableCb
    };

    int32_t code = sdbInsertRowToQueue(&desc);
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
      pMsg->pTable = NULL;
      mnodeDestroyChildTable(pTable);

      if (pMsg->pBatchMasterMsg) {
	++pMsg->pBatchMasterMsg->received;
	if (pMsg->pBatchMasterMsg->successed + pMsg->pBatchMasterMsg->received
	    >= pMsg->pBatchMasterMsg->expected) {
	  dnodeSendRpcMWriteRsp(pMsg->pBatchMasterMsg, code);
	}

	mnodeDestroySubMsg(pMsg);

	return;
      }

      dnodeSendRpcMWriteRsp(pMsg, code);
    }
  } else {
    pMsg->retry++;
    int32_t sec = taosGetTimestampSec();
    if (pMsg->retry < CREATE_CTABLE_RETRY_TIMES && ABS(sec - pMsg->incomingTs) < CREATE_CTABLE_RETRY_SEC) {
      mDebug("msg:%p, app:%p table:%s, create table rsp received, need retry, times:%d vgId:%d sid:%d uid:%" PRIu64
             " result:%s thandle:%p",
             pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId, pMsg->retry, pTable->vgId, pTable->tid, pTable->uid,
             tstrerror(rpcMsg->code), pMsg->rpcMsg.handle);

      dnodeDelayReprocessMWriteMsg(pMsg);
    } else {
      mError("msg:%p, app:%p table:%s, failed to create in dnode, vgId:%d sid:%d uid:%" PRIu64
             ", result:%s thandle:%p incomingTs:%d curTs:%d retryTimes:%d",
             pMsg, pMsg->rpcMsg.ahandle, pTable->info.tableId, pTable->vgId, pTable->tid, pTable->uid,
             tstrerror(rpcMsg->code), pMsg->rpcMsg.handle, pMsg->incomingTs, sec, pMsg->retry);

      SSdbRow row = {.type = SDB_OPER_GLOBAL, .pTable = tsChildTableSdb, .pObj = pTable};
      sdbDeleteRow(&row);

      if (rpcMsg->code == TSDB_CODE_APP_NOT_READY) {
        //Avoid retry again in client
        rpcMsg->code = TSDB_CODE_MND_VGROUP_NOT_READY;
      }

      if (pMsg->pBatchMasterMsg) {
	++pMsg->pBatchMasterMsg->received;
	if (pMsg->pBatchMasterMsg->successed + pMsg->pBatchMasterMsg->received
	    >= pMsg->pBatchMasterMsg->expected) {
	  dnodeSendRpcMWriteRsp(pMsg->pBatchMasterMsg, rpcMsg->code);
	}

	mnodeDestroySubMsg(pMsg);

	return;
      }

      dnodeSendRpcMWriteRsp(pMsg, rpcMsg->code);
    }
  }
}

static void mnodeProcessAlterTableRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->ahandle == NULL) return;

  SMnodeMsg *pMsg = rpcMsg->ahandle;
  pMsg->received++;

  SCTableObj *pTable = (SCTableObj *)pMsg->pTable;
  assert(pTable);

  if (rpcMsg->code == TSDB_CODE_SUCCESS || rpcMsg->code == TSDB_CODE_TDB_TABLE_ALREADY_EXIST) {
    mDebug("msg:%p, app:%p ctable:%s, altered in dnode, thandle:%p result:%s", pMsg, pMsg->rpcMsg.ahandle,
           pTable->info.tableId, pMsg->rpcMsg.handle, tstrerror(rpcMsg->code));

    dnodeSendRpcMWriteRsp(pMsg, TSDB_CODE_SUCCESS);
  } else {
    if (pMsg->retry++ < ALTER_CTABLE_RETRY_TIMES) {
      mDebug("msg:%p, app:%p table:%s, alter table rsp received, need retry, times:%d result:%s thandle:%p",
             pMsg->rpcMsg.ahandle, pMsg, pTable->info.tableId, pMsg->retry, tstrerror(rpcMsg->code),
             pMsg->rpcMsg.handle);

      dnodeDelayReprocessMWriteMsg(pMsg);
    } else {
      mError("msg:%p, app:%p table:%s, failed to alter in dnode, result:%s thandle:%p", pMsg, pMsg->rpcMsg.ahandle,
             pTable->info.tableId, tstrerror(rpcMsg->code), pMsg->rpcMsg.handle);
      dnodeSendRpcMWriteRsp(pMsg, rpcMsg->code);
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
    SCTableObj *pTable = mnodeGetChildTable(tableId);
    if (pTable == NULL) continue;

    if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDbByTableName(tableId);
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

  SSchema* s = tGetTbnameColumnSchema();
  pShow->bytes[cols] = s->bytes;
  pSchema[cols].type = s->type;
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

  SSchema* tbCol = tGetTbnameColumnSchema();
  pShow->bytes[cols] = tbCol->bytes + VARSTR_HEADER_SIZE;
  pSchema[cols].type = tbCol->type;
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
  SCTableObj *pTable = NULL;
  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  char prefix[64] = {0};
  int32_t prefixLen = (int32_t)tableIdPrefix(pDb->name, prefix, 64);

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
  mDebug("msg:%p, app:%p table:%s, alter table msg is received from thandle:%p", pMsg, pMsg->rpcMsg.ahandle,
         pAlter->tableFname, pMsg->rpcMsg.handle);

  if (pMsg->pDb == NULL) pMsg->pDb = mnodeGetDbByTableName(pAlter->tableFname);
  if (pMsg->pDb == NULL) {
    mError("msg:%p, app:%p table:%s, failed to alter table, db not selected", pMsg, pMsg->rpcMsg.ahandle, pAlter->tableFname);
    return TSDB_CODE_MND_DB_NOT_SELECTED;
  }

  if (pMsg->pDb->status != TSDB_DB_STATUS_READY) {
    mError("db:%s, status:%d, in dropping", pMsg->pDb->name, pMsg->pDb->status);
    return TSDB_CODE_MND_DB_IN_DROPPING;
  }

  if (mnodeCheckIsMonitorDB(pMsg->pDb->name, tsMonitorDbName)) {
    mError("msg:%p, app:%p table:%s, failed to alter table, its log db", pMsg, pMsg->rpcMsg.ahandle, pAlter->tableFname);
    return TSDB_CODE_MND_MONITOR_DB_FORBIDDEN;
  }

  if (pMsg->pTable == NULL) pMsg->pTable = mnodeGetTable(pAlter->tableFname);
  if (pMsg->pTable == NULL) {
    mError("msg:%p, app:%p table:%s, failed to alter table, table not exist", pMsg, pMsg->rpcMsg.ahandle, pAlter->tableFname);
    return TSDB_CODE_MND_INVALID_TABLE_NAME;
  }

  pAlter->type = htons(pAlter->type);
  pAlter->numOfCols = htons(pAlter->numOfCols);
  pAlter->tagValLen = htonl(pAlter->tagValLen);

  if (pAlter->numOfCols > 2) {
    mError("msg:%p, app:%p table:%s, error numOfCols:%d in alter table", pMsg, pMsg->rpcMsg.ahandle, pAlter->tableFname,
           pAlter->numOfCols);
    return TSDB_CODE_MND_APP_ERROR;
  }

  for (int32_t i = 0; i < pAlter->numOfCols; ++i) {
    pAlter->schema[i].bytes = htons(pAlter->schema[i].bytes);
  }

  int32_t code = TSDB_CODE_COM_OPS_NOT_SUPPORT;
  if (pMsg->pTable->type == TSDB_SUPER_TABLE) {
    mDebug("msg:%p, app:%p table:%s, start to alter stable", pMsg, pMsg->rpcMsg.ahandle, pAlter->tableFname);
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
    mDebug("msg:%p, app:%p table:%s, start to alter ctable", pMsg, pMsg->rpcMsg.ahandle, pAlter->tableFname);
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

  SSchema* tbnameColSchema = tGetTbnameColumnSchema();
  pShow->bytes[cols] = tbnameColSchema->bytes;
  pSchema[cols].type = tbnameColSchema->type;
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
  SCTableObj *pTable = NULL;
  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  char prefix[64] = {0};
  tstrncpy(prefix, pDb->name, 64);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = (int32_t)strlen(prefix);

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
