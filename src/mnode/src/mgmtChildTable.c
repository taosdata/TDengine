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
#include "mgmtChildTable.h"
#include "mgmtDb.h"
#include "mgmtDClient.h"
#include "mgmtGrant.h"
#include "mgmtMnode.h"
#include "mgmtProfile.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtSuperTable.h"
#include "mgmtTable.h"
#include "mgmtVgroup.h"

void   *tsChildTableSdb;
int32_t tsChildTableUpdateSize;

static void mgmtDestroyChildTable(SChildTableObj *pTable) {
  tfree(pTable);
}

static int32_t mgmtChildTableActionDestroy(void *pObj) {
  SChildTableObj *pTable = (SChildTableObj *)pObj;
  mgmtDestroyChildTable(pTable);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionInsert(void *pObj) {
  SChildTableObj *pTable = (SChildTableObj *) pObj;

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    mError("ctable:%s, not in vgroup:%d", pTable->tableId, pTable->vgId);
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
  if (pDb == NULL) {
    mError("ctable:%s, vgroup:%d not in db:%s", pTable->tableId, pVgroup->vgId, pVgroup->dbName);
    return TSDB_CODE_INVALID_DB;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("ctable:%s, account:%s not exists", pTable->tableId, pDb->cfg.acct);
    return TSDB_CODE_INVALID_ACCT;
  }

  if (!mgmtIsMaster()) {
    int32_t sid = taosAllocateId(pVgroup->idPool);
    if (sid != pTable->sid) {
      mError("ctable:%s, sid:%d is not matched from the master:%d", pTable->tableId, sid, pTable->sid);
      return TSDB_CODE_INVALID_SESSION_ID;
    }
  }

  pTable->superTable = mgmtGetSuperTable(pTable->superTableId);
  mgmtAddTableIntoSuperTable(pTable->superTable);

  mgmtAddTimeSeries(pAcct, pTable->superTable->numOfColumns - 1);
  mgmtAddTableIntoDb(pDb);
  mgmtAddTableIntoVgroup(pVgroup, (STableInfo *) pTable);

  if (pVgroup->numOfTables >= pDb->cfg.maxSessions - 1 && pDb->numOfVgroups > 1) {
    mgmtMoveVgroupToTail(pDb, pVgroup);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionDelete(void *pObj) {
  SChildTableObj *pTable = (SChildTableObj *) pObj;
  if (pTable->vgId == 0) {
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
  if (pDb == NULL) {
    mError("ctable:%s, vgroup:%d not in DB:%s", pTable->tableId, pVgroup->vgId, pVgroup->dbName);
    return TSDB_CODE_INVALID_DB;
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("ctable:%s, account:%s not exists", pTable->tableId, pDb->cfg.acct);
    return TSDB_CODE_INVALID_ACCT;
  }

  mgmtRestoreTimeSeries(pAcct, pTable->superTable->numOfColumns - 1);
  mgmtRemoveTableFromDb(pDb);
  mgmtRemoveTableFromVgroup(pVgroup, (STableInfo *) pTable);

  mgmtRemoveTableFromSuperTable(pTable->superTable);

  if (pVgroup->numOfTables > 0) {
    mgmtMoveVgroupToHead(pDb, pVgroup);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionUpdate(void *pObj) {
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionEncode(void *pObj, void *pData, int32_t maxRowSize) {
  SChildTableObj *pTable = (SChildTableObj *) pObj;
  assert(pObj != NULL && pData != NULL);

  memcpy(pData, pTable, tsChildTableUpdateSize);
  return tsChildTableUpdateSize;
}

static void *mgmtChildTableActionDecode(void *pData) {
  assert(pData != NULL);

  SChildTableObj *pTable = (SChildTableObj *)calloc(sizeof(SChildTableObj), 1);
  if (pTable == NULL) return NULL;

  memcpy(pTable, pData, tsChildTableUpdateSize);

  return (void *)pTable;
}

int32_t mgmtInitChildTables() {
  void *pNode = NULL;
  void *pLastNode = NULL;
  SChildTableObj *pTable = NULL;

  SChildTableObj tObj;
  tsChildTableUpdateSize = tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableName    = "ctables",
    .hashSessions = tsMaxTables,
    .maxRowSize   = tsChildTableUpdateSize,
    .keyType      = SDB_KEYTYPE_STRING,
    .insertFp     = mgmtChildTableActionInsert,
    .deleteFp     = mgmtChildTableActionDelete,
    .updateFp     = mgmtChildTableActionUpdate,
    .encodeFp     = mgmtChildTableActionEncode,
    .decodeFp     = mgmtChildTableActionDecode,
    .destroyFp    = mgmtChildTableActionDestroy,
  };

  tsChildTableSdb = sdbOpenTable(&tableDesc);
  if (tsChildTableSdb == NULL) {
    mError("failed to init child table data");
    return -1;
  }

  pNode = NULL;
  while (1) {
    pNode = sdbFetchRow(tsChildTableSdb, pNode, (void **)&pTable);
    if (pTable == NULL) {
      break;
    }

    SDbObj *pDb = mgmtGetDbByTableId(pTable->tableId);
    if (pDb == NULL) {
      mError("ctable:%s, failed to get db, discard it", pTable->tableId);
      sdbDeleteRow(tsChildTableSdb, pTable, SDB_OPER_DISK);
      pNode = pLastNode;
      continue;
    }

    SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
    if (pVgroup == NULL) {
      mError("ctable:%s, failed to get vgroup:%d sid:%d, discard it", pTable->tableId, pTable->vgId, pTable->sid);
      pTable->vgId = 0;
      sdbDeleteRow(tsChildTableSdb, pTable, SDB_OPER_DISK);
      pNode = pLastNode;
      continue;
    }

    if (strcmp(pVgroup->dbName, pDb->name) != 0) {
      mError("ctable:%s, db:%s not match with vgroup:%d db:%s sid:%d, discard it",
             pTable->tableId, pDb->name, pTable->vgId, pVgroup->dbName, pTable->sid);
      pTable->vgId = 0;
      sdbDeleteRow(tsChildTableSdb, pTable, SDB_OPER_DISK);
      pNode = pLastNode;
      continue;
    }

    if (pVgroup->tableList == NULL) {
      mError("ctable:%s, vgroup:%d tableList is null", pTable->tableId, pTable->vgId);
      pTable->vgId = 0;
      sdbDeleteRow(tsChildTableSdb, pTable, SDB_OPER_DISK);
      pNode = pLastNode;
      continue;
    }

    pVgroup->tableList[pTable->sid] = (STableInfo*)pTable;
    taosIdPoolMarkStatus(pVgroup->idPool, pTable->sid, 1);

    SSuperTableObj *pSuperTable = mgmtGetSuperTable(pTable->superTableId);
    if (pSuperTable == NULL) {
      mError("ctable:%s, stable:%s not exist", pTable->tableId, pTable->superTableId);
      pTable->vgId = 0;
      sdbDeleteRow(tsChildTableSdb, pTable, SDB_OPER_DISK);
      pNode = pLastNode;
      continue;
    }

    pTable->superTable = pSuperTable;
    mgmtAddTableIntoSuperTable(pSuperTable);

    SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
    mgmtAddTimeSeries(pAcct, pTable->superTable->numOfColumns - 1);
  }

  mTrace("child table is initialized");
  return 0;
}

void mgmtCleanUpChildTables() {
  sdbCloseTable(tsChildTableSdb);
}

void *mgmtBuildCreateChildTableMsg(SCMCreateTableMsg *pMsg, SChildTableObj *pTable) {
  char    *pTagData  = pMsg->schema + TSDB_TABLE_ID_LEN + 1;
  int32_t tagDataLen = htonl(pMsg->contLen) - sizeof(SCMCreateTableMsg) - TSDB_TABLE_ID_LEN - 1;
  int32_t totalCols  = pTable->superTable->numOfColumns + pTable->superTable->numOfTags;
  int32_t contLen    = sizeof(SMDCreateTableMsg) + totalCols * sizeof(SSchema) + tagDataLen;

  SMDCreateTableMsg *pCreate = rpcMallocCont(contLen);
  if (pCreate == NULL) {
    terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
    return NULL;
  }

  memcpy(pCreate->tableId, pTable->tableId, TSDB_TABLE_ID_LEN + 1);
  memcpy(pCreate->superTableId, pTable->superTable->tableId, TSDB_TABLE_ID_LEN + 1);
  pCreate->contLen       = htonl(contLen);
  pCreate->vgId          = htonl(pTable->vgId);
  pCreate->tableType     = pTable->type;
  pCreate->numOfColumns  = htons(pTable->superTable->numOfColumns);
  pCreate->numOfTags     = htons(pTable->superTable->numOfTags);
  pCreate->sid           = htonl(pTable->sid);
  pCreate->sversion      = htonl(pTable->superTable->sversion);
  pCreate->tagDataLen    = htonl(tagDataLen);
  pCreate->sqlDataLen    = 0;
  pCreate->uid           = htobe64(pTable->uid);
  pCreate->superTableUid = htobe64(pTable->superTable->uid);
  pCreate->createdTime   = htobe64(pTable->createdTime);

  SSchema *pSchema = (SSchema *) pCreate->data;
  memcpy(pSchema, pTable->superTable->schema, totalCols * sizeof(SSchema));
  for (int32_t col = 0; col < totalCols; ++col) {
    pSchema->bytes = htons(pSchema->bytes);
    pSchema->colId = htons(pSchema->colId);
    pSchema++;
  }

  memcpy(pCreate->data + totalCols * sizeof(SSchema), pTagData, tagDataLen);
  return pCreate;
}

void* mgmtCreateChildTable(SCMCreateTableMsg *pCreate, SVgObj *pVgroup, int32_t tid) {
  char *pTagData = (char *) pCreate->schema;  // it is a tag key
  SSuperTableObj *pSuperTable = mgmtGetSuperTable(pTagData);
  if (pSuperTable == NULL) {
    mError("ctable:%s, corresponding super table does not exist", pCreate->tableId);
    terrno = TSDB_CODE_INVALID_TABLE;
    return NULL;
  }

  SChildTableObj *pTable = (SChildTableObj *) calloc(sizeof(SChildTableObj), 1);
  if (pTable == NULL) {
    mError("ctable:%s, failed to alloc memory", pCreate->tableId);
    terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
    return NULL;
  }

  strcpy(pTable->tableId, pCreate->tableId);
  strcpy(pTable->superTableId, pSuperTable->tableId);
  pTable->type        = TSDB_CHILD_TABLE;
  pTable->createdTime = taosGetTimestampMs();
  pTable->uid         = (((uint64_t) pTable->vgId) << 40) + ((((uint64_t) pTable->sid) & ((1ul << 24) - 1ul)) << 16) +
                        (sdbGetVersion() & ((1ul << 16) - 1ul));
  pTable->sid         = tid;
  pTable->vgId        = pVgroup->vgId;
  pTable->superTable  = pSuperTable;

  if (sdbInsertRow(tsChildTableSdb, pTable, SDB_OPER_GLOBAL) < 0) {
    free(pTable);
    mError("ctable:%s, update sdb error", pCreate->tableId);
    terrno = TSDB_CODE_SDB_ERROR;
    return NULL;
  }

  mTrace("ctable:%s, create ctable in vgroup, uid:%" PRIu64 , pTable->tableId, pTable->uid);
  return pTable;
}

int32_t mgmtDropChildTable(SQueuedMsg *newMsg, SChildTableObj *pTable) {
  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    mError("ctable:%s, failed to drop child table, vgroup not exist", pTable->tableId);
    return TSDB_CODE_OTHERS;
  }

  SMDDropTableMsg *pDrop = rpcMallocCont(sizeof(SMDDropTableMsg));
  if (pDrop == NULL) {
    mError("ctable:%s, failed to drop child table, no enough memory", pTable->tableId);
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  strcpy(pDrop->tableId, pTable->tableId);
  pDrop->vgId    = htonl(pTable->vgId);
  pDrop->contLen = htonl(sizeof(SMDDropTableMsg));
  pDrop->sid     = htonl(pTable->sid);
  pDrop->uid     = htobe64(pTable->uid);

  SRpcIpSet ipSet = mgmtGetIpSetFromVgroup(pVgroup);

  mTrace("ctable:%s, send drop table msg", pDrop->tableId);
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

void* mgmtGetChildTable(char *tableId) {
  return sdbGetRow(tsChildTableSdb, tableId);
}

int32_t mgmtModifyChildTableTagValueByName(SChildTableObj *pTable, char *tagName, char *nContent) {
// TODO: send message to dnode  
//  int32_t col = mgmtFindSuperTableTagIndex(pTable->superTable, tagName);
//  if (col < 0 || col > pTable->superTable->numOfTags) {
//    return TSDB_CODE_APP_ERROR;
//  }
//
//  //TODO send msg to dnode
//  mTrace("Succeed to modify tag column %d of table %s", col, pTable->tableId);
//  return TSDB_CODE_SUCCESS;

//  int32_t rowSize = 0;
//  SSchema *schema = (SSchema *)(pSuperTable->schema + (pSuperTable->numOfColumns + col) * sizeof(SSchema));
//
//  if (col == 0) {
//    pTable->isDirty = 1;
//    removeMeterFromMetricIndex(pSuperTable, pTable);
//  }
//  memcpy(pTable->pTagData + mgmtGetTagsLength(pMetric, col) + TSDB_TABLE_ID_LEN, nContent, schema->bytes);
//  if (col == 0) {
//    addMeterIntoMetricIndex(pMetric, pTable);
//  }
//
//  // Encode the string
//  int32_t   size = sizeof(STabObj) + TSDB_MAX_BYTES_PER_ROW + 1;
//  char *msg = (char *)malloc(size);
//  if (msg == NULL) {
//    mError("failed to allocate message memory while modify tag value");
//    return TSDB_CODE_APP_ERROR;
//  }
//  memset(msg, 0, size);
//
//  mgmtMeterActionEncode(pTable, msg, size, &rowSize);
//
//  int32_t ret = sdbUpdateRow(tsChildTableSdb, msg, rowSize, 1);  // Need callback function
//  tfree(msg);
//
//  if (pTable->isDirty) pTable->isDirty = 0;
//
//  if (ret < 0) {
//    mError("Failed to modify tag column %d of table %s", col, pTable->tableId);
//    return TSDB_CODE_APP_ERROR;
//  }
//
//  mTrace("Succeed to modify tag column %d of table %s", col, pTable->tableId);
//  return TSDB_CODE_SUCCESS;
  return 0;
}

int32_t mgmtGetChildTableMeta(SDbObj *pDb, SChildTableObj *pTable, STableMetaMsg *pMeta, bool usePublicIp) {
  pMeta->uid          = htobe64(pTable->uid);
  pMeta->sid          = htonl(pTable->sid);
  pMeta->vgid         = htonl(pTable->vgId);
  pMeta->sversion     = htons(pTable->superTable->sversion);
  pMeta->precision    = pDb->cfg.precision;
  pMeta->numOfTags    = pTable->superTable->numOfTags;
  pMeta->numOfColumns = htons(pTable->superTable->numOfColumns);
  pMeta->tableType    = pTable->type;
  pMeta->contLen      = sizeof(STableMetaMsg) + mgmtSetSchemaFromSuperTable(pMeta->schema, pTable->superTable);
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

void mgmtDropAllChildTables(SDbObj *pDropDb) {
  void *pNode = NULL;
  void *pLastNode = NULL;
  int32_t numOfTables = 0;
  int32_t dbNameLen = strlen(pDropDb->name);
  SChildTableObj *pTable = NULL;

  while (1) {
    pNode = sdbFetchRow(tsChildTableSdb, pNode, (void **)&pTable);
    if (pTable == NULL) {
      break;
    }

    if (strncmp(pDropDb->name, pTable->tableId, dbNameLen) == 0) {
      sdbDeleteRow(tsChildTableSdb, pTable, SDB_OPER_LOCAL);
      pNode = pLastNode;
      numOfTables ++;
      continue;
    }
  }

  mTrace("db:%s, all child tables:%d is dropped", pDropDb->name, numOfTables);
}