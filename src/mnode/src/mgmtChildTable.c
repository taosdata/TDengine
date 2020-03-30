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
#include "qast.h"
#include "qextbuffer.h"
#include "taoserror.h"
#include "taosmsg.h"
#include "tscompression.h"
#include "tskiplist.h"
#include "tsqlfunction.h"
#include "tstatus.h"
#include "ttime.h"
#include "name.h"
#include "mnode.h"
#include "mgmtAcct.h"
#include "mgmtChildTable.h"
#include "mgmtDb.h"
#include "mgmtDClient.h"
#include "mgmtDnode.h"
#include "mgmtDServer.h"
#include "mgmtGrant.h"
#include "mgmtMnode.h"
#include "mgmtProfile.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtSuperTable.h"
#include "mgmtTable.h"
#include "mgmtVgroup.h"
#include "mgmtUser.h"

void   *tsChildTableSdb;
static int32_t tsChildTableUpdateSize;
static void    mgmtProcessMultiTableMetaMsg(SQueuedMsg *queueMsg);
static void    mgmtProcessCreateTableRsp(SRpcMsg *rpcMsg);
static void    mgmtProcessAlterTableRsp(SRpcMsg *rpcMsg);
static void    mgmtProcessDropTableRsp(SRpcMsg *rpcMsg);
static void    mgmtProcessTableCfgMsg(SRpcMsg *rpcMsg);
static int32_t mgmtGetShowTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mgmtRetrieveShowTables(SShowObj *pShow, char *data, int32_t rows, void *pConn);

static void mgmtDestroyChildTable(SChildTableObj *pTable) {
  tfree(pTable->schema);
  tfree(pTable->sql);
  tfree(pTable);
}

static int32_t mgmtChildTableActionDestroy(SSdbOperDesc *pOper) {
  mgmtDestroyChildTable(pOper->pObj);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionInsert(SSdbOperDesc *pOper) {
  SChildTableObj *pTable = pOper->pObj;

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    mError("ctable:%s, not in vgroup:%d", pTable->info.tableId, pTable->vgId);
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
  if (pDb == NULL) {
    mError("ctable:%s, vgroup:%d not in db:%s", pTable->info.tableId, pVgroup->vgId, pVgroup->dbName);
    return TSDB_CODE_INVALID_DB;
  }

  SAcctObj *pAcct = acctGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("ctable:%s, account:%s not exists", pTable->info.tableId, pDb->cfg.acct);
    return TSDB_CODE_INVALID_ACCT;
  }

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    pTable->superTable = mgmtGetSuperTable(pTable->superTableId);
    pTable->superTable->numOfTables++;
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

static int32_t mgmtChildTableActionDelete(SSdbOperDesc *pOper) {
  SChildTableObj *pTable = pOper->pObj;
  if (pTable->vgId == 0) {
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  SDbObj *pDb = mgmtGetDb(pVgroup->dbName);
  if (pDb == NULL) {
    mError("ctable:%s, vgroup:%d not in DB:%s", pTable->info.tableId, pVgroup->vgId, pVgroup->dbName);
    return TSDB_CODE_INVALID_DB;
  }

  SAcctObj *pAcct = acctGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("ctable:%s, account:%s not exists", pTable->info.tableId, pDb->cfg.acct);
    return TSDB_CODE_INVALID_ACCT;
  }

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    grantRestore(TSDB_GRANT_TIMESERIES, pTable->superTable->numOfColumns - 1);
    pAcct->acctInfo.numOfTimeSeries -= (pTable->superTable->numOfColumns - 1);
    pTable->superTable->numOfTables--;
  } else {
    grantRestore(TSDB_GRANT_TIMESERIES, pTable->numOfColumns - 1);
    pAcct->acctInfo.numOfTimeSeries -= (pTable->numOfColumns - 1);
  }
  mgmtRemoveTableFromDb(pDb);
  mgmtRemoveTableFromVgroup(pVgroup, pTable);
 
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionUpdate(SSdbOperDesc *pOper) {
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionEncode(SSdbOperDesc *pOper) {
  SChildTableObj *pTable = pOper->pObj;
  assert(pTable != NULL && pOper->rowData != NULL);

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    memcpy(pOper->rowData, pTable, tsChildTableUpdateSize);
    pOper->rowSize = tsChildTableUpdateSize;
  } else {
    int32_t schemaSize = pTable->numOfColumns * sizeof(SSchema);
    if (pOper->maxRowSize < tsChildTableUpdateSize + schemaSize) {
      return TSDB_CODE_INVALID_MSG_LEN;
    }
    memcpy(pOper->rowData, pTable, tsChildTableUpdateSize);
    memcpy(pOper->rowData + tsChildTableUpdateSize, pTable->schema, schemaSize);
    memcpy(pOper->rowData + tsChildTableUpdateSize + schemaSize, pTable->sql, pTable->sqlLen);
    pOper->rowSize = tsChildTableUpdateSize + schemaSize + pTable->sqlLen;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtChildTableActionDecode(SSdbOperDesc *pOper) {
  assert(pOper->rowData != NULL);
  SChildTableObj *pTable = calloc(1, sizeof(SChildTableObj));
  if (pTable == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  memcpy(pTable, pOper->rowData, tsChildTableUpdateSize);

  if (pTable->info.type != TSDB_CHILD_TABLE) {
    int32_t schemaSize = pTable->numOfColumns * sizeof(SSchema);
    pTable->schema = (SSchema *)malloc(schemaSize);
    if (pTable->schema == NULL) {
      mgmtDestroyChildTable(pTable);
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    }
    memcpy(pTable->schema, pOper->rowData + tsChildTableUpdateSize, schemaSize);

    pTable->sql = (char *)malloc(pTable->sqlLen);
    if (pTable->sql == NULL) {
      mgmtDestroyChildTable(pTable);
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    }
    memcpy(pTable->sql, pOper->rowData + tsChildTableUpdateSize + schemaSize, pTable->sqlLen);
  }

  pOper->pObj = pTable;
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtInitChildTables() {
  void *pNode = NULL;
  void *pLastNode = NULL;
  SChildTableObj *pTable = NULL;

  SChildTableObj tObj;
  tsChildTableUpdateSize = (int8_t *)tObj.updateEnd - (int8_t *)&tObj;

  SSdbTableDesc tableDesc = {
    .tableName    = "ctables",
    .hashSessions = tsMaxTables,
    .maxRowSize   = sizeof(SChildTableObj) + sizeof(SSchema) * TSDB_MAX_COLUMNS,
    .keyType      = SDB_KEY_TYPE_STRING,
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
    pLastNode = pNode;
    pNode = sdbFetchRow(tsChildTableSdb, pNode, (void **)&pTable);
    if (pTable == NULL) {
      break;
    }

    SDbObj *pDb = mgmtGetDbByTableId(pTable->info.tableId);
    if (pDb == NULL) {
      mError("ctable:%s, failed to get db, discard it", pTable->info.tableId);
      SSdbOperDesc desc = {0};
      desc.type = SDB_OPER_TYPE_LOCAL;
      desc.pObj = pTable;
      desc.table = tsChildTableSdb;
      sdbDeleteRow(&desc);
      pNode = pLastNode;
      continue;
    }

    SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
    if (pVgroup == NULL) {
      mError("ctable:%s, failed to get vgroup:%d sid:%d, discard it", pTable->info.tableId, pTable->vgId, pTable->sid);
      pTable->vgId = 0;
      SSdbOperDesc desc = {0};
      desc.type = SDB_OPER_TYPE_LOCAL;
      desc.pObj = pTable;
      desc.table = tsChildTableSdb;
      sdbDeleteRow(&desc);
      pNode = pLastNode;
      continue;
    }

    if (strcmp(pVgroup->dbName, pDb->name) != 0) {
      mError("ctable:%s, db:%s not match with vgroup:%d db:%s sid:%d, discard it",
             pTable->info.tableId, pDb->name, pTable->vgId, pVgroup->dbName, pTable->sid);
      pTable->vgId = 0;
      SSdbOperDesc desc = {0};
      desc.type = SDB_OPER_TYPE_LOCAL;
      desc.pObj = pTable;
      desc.table = tsChildTableSdb;
      sdbDeleteRow(&desc);
      pNode = pLastNode;
      continue;
    }

    if (pVgroup->tableList == NULL) {
      mError("ctable:%s, vgroup:%d tableList is null", pTable->info.tableId, pTable->vgId);
      pTable->vgId = 0;
      SSdbOperDesc desc = {0};
      desc.type = SDB_OPER_TYPE_LOCAL;
      desc.pObj = pTable;
      desc.table = tsChildTableSdb;
      sdbDeleteRow(&desc);
      pNode = pLastNode;
      continue;
    }

    if (pTable->info.type == TSDB_CHILD_TABLE) {
      SSuperTableObj *pSuperTable = mgmtGetSuperTable(pTable->superTableId);
      if (pSuperTable == NULL) {
        mError("ctable:%s, stable:%s not exist", pTable->info.tableId, pTable->superTableId);
        pTable->vgId = 0;
        SSdbOperDesc desc = {0};
        desc.type = SDB_OPER_TYPE_LOCAL;
        desc.pObj = pTable;
        desc.table = tsChildTableSdb;
        sdbDeleteRow(&desc);
        pNode = pLastNode;
        continue;
      }
    }
  }

  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_TABLES_META, mgmtProcessMultiTableMetaMsg);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_TABLE, mgmtGetShowTableMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_TABLE, mgmtRetrieveShowTables);
  mgmtAddDClientRspHandle(TSDB_MSG_TYPE_MD_CREATE_TABLE_RSP, mgmtProcessCreateTableRsp);
  mgmtAddDClientRspHandle(TSDB_MSG_TYPE_MD_DROP_TABLE_RSP, mgmtProcessDropTableRsp);
  mgmtAddDClientRspHandle(TSDB_MSG_TYPE_MD_ALTER_TABLE_RSP, mgmtProcessAlterTableRsp);
  mgmtAddDServerMsgHandle(TSDB_MSG_TYPE_DM_CONFIG_TABLE, mgmtProcessTableCfgMsg);

  mTrace("child table is initialized");
  return 0;
}

void mgmtCleanUpChildTables() {
  sdbCloseTable(tsChildTableSdb);
}

static void *mgmtBuildCreateChildTableMsg(SCMCreateTableMsg *pMsg, SChildTableObj *pTable) {
  char *  pTagData = NULL;
  int32_t tagDataLen = 0;
  int32_t totalCols = 0;
  int32_t contLen = 0;
  if (pTable->info.type == TSDB_CHILD_TABLE && pMsg != NULL) {
    pTagData = pMsg->schema + TSDB_TABLE_ID_LEN + 1;
    tagDataLen = htonl(pMsg->contLen) - sizeof(SCMCreateTableMsg) - TSDB_TABLE_ID_LEN - 1;
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

  memcpy(pCreate->tableId, pTable->info.tableId, TSDB_TABLE_ID_LEN + 1);
  pCreate->contLen       = htonl(contLen);
  pCreate->vgId          = htonl(pTable->vgId);
  pCreate->tableType     = pTable->info.type;
  pCreate->createdTime   = htobe64(pTable->createdTime);
  pCreate->sid           = htonl(pTable->sid);
  pCreate->sqlDataLen    = htonl(pTable->sqlLen);
  pCreate->uid           = htobe64(pTable->uid);
  
  if (pTable->info.type == TSDB_CHILD_TABLE) {
    memcpy(pCreate->superTableId, pTable->superTable->info.tableId, TSDB_TABLE_ID_LEN + 1);
    pCreate->numOfColumns  = htons(pTable->superTable->numOfColumns);
    pCreate->numOfTags     = htons(pTable->superTable->numOfTags);
    pCreate->sversion      = htonl(pTable->superTable->sversion);
    pCreate->tagDataLen    = htonl(tagDataLen);
    pCreate->superTableUid = htobe64(pTable->superTable->uid);
  } else {
    pCreate->numOfColumns  = htons(pTable->numOfColumns);
    pCreate->numOfTags     = 0;
    pCreate->sversion      = htonl(pTable->sversion);
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
    memcpy(pCreate->data + totalCols * sizeof(SSchema), pTagData, tagDataLen);
    memcpy(pCreate->data + totalCols * sizeof(SSchema) + tagDataLen, pTable->sql, pTable->sqlLen);
  }

  return pCreate;
}

static SChildTableObj* mgmtDoCreateChildTable(SCMCreateTableMsg *pCreate, SVgObj *pVgroup, int32_t tid) {
  SChildTableObj *pTable = (SChildTableObj *) calloc(1, sizeof(SChildTableObj));
  if (pTable == NULL) {
    mError("ctable:%s, failed to alloc memory", pCreate->tableId);
    terrno = TSDB_CODE_SERV_OUT_OF_MEMORY;
    return NULL;
  }

  if (pCreate->numOfColumns == 0) {
    pTable->info.type = TSDB_CHILD_TABLE;
  } else {
    pTable->info.type = TSDB_NORMAL_TABLE;
  }

  strcpy(pTable->info.tableId, pCreate->tableId);  
  pTable->createdTime = taosGetTimestampMs();
  pTable->sid         = tid;
  pTable->vgId        = pVgroup->vgId;
    
  if (pTable->info.type == TSDB_CHILD_TABLE) {
    char *pTagData = (char *) pCreate->schema;  // it is a tag key
    SSuperTableObj *pSuperTable = mgmtGetSuperTable(pTagData);
    if (pSuperTable == NULL) {
      mError("ctable:%s, corresponding super table does not exist", pCreate->tableId);
      free(pTable);
      terrno = TSDB_CODE_INVALID_TABLE;
      return NULL;
    }

    strcpy(pTable->superTableId, pSuperTable->info.tableId);
    pTable->uid         = (((uint64_t) pTable->vgId) << 40) + ((((uint64_t) pTable->sid) & ((1ul << 24) - 1ul)) << 16) +
                          (sdbGetVersion() & ((1ul << 16) - 1ul));
    pTable->superTable  = pSuperTable;
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
  
  SSdbOperDesc desc = {0};
  desc.type = SDB_OPER_TYPE_GLOBAL;
  desc.pObj = pTable;
  desc.table = tsChildTableSdb;
  
  if (sdbInsertRow(&desc) != TSDB_CODE_SUCCESS) {
    free(pTable);
    mError("ctable:%s, update sdb error", pCreate->tableId);
    terrno = TSDB_CODE_SDB_ERROR;
    return NULL;
  }

  mTrace("ctable:%s, create ctable in vgroup, uid:%" PRIu64 , pTable->info.tableId, pTable->uid);
  return pTable;
}

void mgmtCreateChildTable(SQueuedMsg *pMsg) {
  SCMCreateTableMsg *pCreate = pMsg->pCont;

  int32_t code = grantCheck(TSDB_GRANT_TIMESERIES);
  if (code != TSDB_CODE_SUCCESS) {
    mError("table:%s, failed to create, grant not", pCreate->tableId);
    mgmtSendSimpleResp(pMsg->thandle, code);
    return;
  }

  SQueuedMsg *newMsg = malloc(sizeof(SQueuedMsg));
  memcpy(newMsg, pMsg, sizeof(SQueuedMsg));
  pMsg->pCont = NULL;

  SVgObj *pVgroup = mgmtGetAvailableVgroup(pMsg->pDb);
  if (pVgroup == NULL) {
    mTrace("table:%s, start to create a new vgroup", pCreate->tableId);
    mgmtCreateVgroup(newMsg);
    return;
  }

  int32_t sid = taosAllocateId(pVgroup->idPool);
  if (sid < 0) {
    mTrace("tables:%s, no enough sid in vgroup:%d", pVgroup->vgId);
    mgmtCreateVgroup(newMsg);
    return;
  }

  SChildTableObj *pTable = mgmtDoCreateChildTable(pCreate, pVgroup, sid);
  if (pTable == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, terrno);
    mgmtFreeQueuedMsg(newMsg);
    return;
  }

  SMDCreateTableMsg *pMDCreate = mgmtBuildCreateChildTableMsg(pCreate, (SChildTableObj *) pTable);
  if (pMDCreate == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, terrno);
    mgmtFreeQueuedMsg(newMsg);
    return;
  }

  SRpcIpSet ipSet = mgmtGetIpSetFromVgroup(pVgroup);
  SRpcMsg rpcMsg = {
      .handle  = newMsg,
      .pCont   = pMDCreate,
      .contLen = htonl(pMDCreate->contLen),
      .code    = 0,
      .msgType = TSDB_MSG_TYPE_MD_CREATE_TABLE
  };

  newMsg->ahandle = pTable;
  mgmtSendMsgToDnode(&ipSet, &rpcMsg);
}

void mgmtDropChildTable(SQueuedMsg *pMsg, SChildTableObj *pTable) {
  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    mError("ctable:%s, failed to drop child table, vgroup not exist", pTable->info.tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_OTHERS);
    return;
  }

  SMDDropTableMsg *pDrop = rpcMallocCont(sizeof(SMDDropTableMsg));
  if (pDrop == NULL) {
    mError("ctable:%s, failed to drop child table, no enough memory", pTable->info.tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  strcpy(pDrop->tableId, pTable->info.tableId);
  pDrop->vgId    = htonl(pTable->vgId);
  pDrop->contLen = htonl(sizeof(SMDDropTableMsg));
  pDrop->sid     = htonl(pTable->sid);
  pDrop->uid     = htobe64(pTable->uid);

  SRpcIpSet ipSet = mgmtGetIpSetFromVgroup(pVgroup);

  mTrace("ctable:%s, send drop table msg", pDrop->tableId);
  SRpcMsg rpcMsg = {
    .handle  = pMsg,
    .pCont   = pDrop,
    .contLen = sizeof(SMDDropTableMsg),
    .code    = 0,
    .msgType = TSDB_MSG_TYPE_MD_DROP_TABLE
  };

  pMsg->ahandle = pTable;
  mgmtSendMsgToDnode(&ipSet, &rpcMsg);
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
//  mTrace("Succeed to modify tag column %d of table %s", col, pTable->info.tableId);
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
//    mError("Failed to modify tag column %d of table %s", col, pTable->info.tableId);
//    return TSDB_CODE_APP_ERROR;
//  }
//
//  mTrace("Succeed to modify tag column %d of table %s", col, pTable->info.tableId);
//  return TSDB_CODE_SUCCESS;
  return 0;
}


static int32_t mgmtFindNormalTableColumnIndex(SChildTableObj *pTable, char *colName) {
  SSchema *schema = (SSchema *) pTable->schema;
  for (int32_t i = 0; i < pTable->numOfColumns; i++) {
    if (strcasecmp(schema[i].name, colName) == 0) {
      return i;
    }
  }

  return -1;
}

static int32_t mgmtAddNormalTableColumn(SChildTableObj *pTable, SSchema schema[], int32_t ncols) {
  if (ncols <= 0) {
    return TSDB_CODE_APP_ERROR;
  }

  for (int32_t i = 0; i < ncols; i++) {
    if (mgmtFindNormalTableColumnIndex(pTable, schema[i].name) > 0) {
      return TSDB_CODE_APP_ERROR;
    }
  }

  SDbObj *pDb = mgmtGetDbByTableId(pTable->info.tableId);
  if (pDb == NULL) {
    mError("table: %s not belongs to any database", pTable->info.tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = acctGetAcct(pDb->cfg.acct);
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
  desc.table = tsChildTableSdb;
  desc.rowData = pTable;
  desc.rowSize = tsChildTableUpdateSize;
  sdbUpdateRow(&desc);

  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtDropNormalTableColumnByName(SChildTableObj *pTable, char *colName) {
  int32_t col = mgmtFindNormalTableColumnIndex(pTable, colName);
  if (col < 0) {
    return TSDB_CODE_APP_ERROR;
  }

  SDbObj *pDb = mgmtGetDbByTableId(pTable->info.tableId);
  if (pDb == NULL) {
    mError("table: %s not belongs to any database", pTable->info.tableId);
    return TSDB_CODE_APP_ERROR;
  }

  SAcctObj *pAcct = acctGetAcct(pDb->cfg.acct);
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
  desc.table = tsChildTableSdb;
  desc.rowData = pTable;
  desc.rowSize = tsChildTableUpdateSize;
  sdbUpdateRow(&desc);

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

static int32_t mgmtDoGetChildTableMeta(SDbObj *pDb, SChildTableObj *pTable, STableMetaMsg *pMeta, bool usePublicIp) {
  pMeta->uid       = htobe64(pTable->uid);
  pMeta->sid       = htonl(pTable->sid);
  pMeta->vgId      = htonl(pTable->vgId);
  pMeta->precision = pDb->cfg.precision;
  pMeta->tableType = pTable->info.type;
  strncpy(pMeta->tableId, pTable->info.tableId, tListLen(pTable->info.tableId));

  if (pTable->info.type == TSDB_CHILD_TABLE) {
    pMeta->sversion     = htons(pTable->superTable->sversion);
    pMeta->numOfTags    = 0;
    pMeta->numOfColumns = htons((int16_t)pTable->superTable->numOfColumns);
    pMeta->contLen      = sizeof(STableMetaMsg) + mgmtSetSchemaFromSuperTable(pMeta->schema, pTable->superTable);
    strncpy(pMeta->stableId, pTable->superTable->info.tableId, tListLen(pMeta->stableId));
  } else {
    pMeta->sversion     = htons(pTable->sversion);
    pMeta->numOfTags    = 0;
    pMeta->numOfColumns = htons((int16_t)pTable->numOfColumns);
    pMeta->contLen      = sizeof(STableMetaMsg) + mgmtSetSchemaFromNormalTable(pMeta->schema, pTable); 
  }
  
  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    mError("table:%s, failed to get table meta, db not selected", pTable->info.tableId);
    return TSDB_CODE_INVALID_VGROUP_ID;
  }

  for (int32_t i = 0; i < TSDB_VNODES_SUPPORT; ++i) {
    if (usePublicIp) {
      pMeta->vpeerDesc[i].ip = pVgroup->vnodeGid[i].publicIp;
    } else {
      pMeta->vpeerDesc[i].ip = pVgroup->vnodeGid[i].privateIp;
    }
    pMeta->vpeerDesc[i].vnode = htonl(pVgroup->vnodeGid[i].vnode);
  }
  pMeta->numOfVpeers = pVgroup->numOfVnodes;

  mTrace("table:%s, uid:%" PRIu64 " table meta is retrieved", pTable->info.tableId, pTable->uid);

  return TSDB_CODE_SUCCESS;
}

void mgmtGetChildTableMeta(SQueuedMsg *pMsg, SChildTableObj *pTable) {
  SCMTableInfoMsg *pInfo = pMsg->pCont;
  SDbObj *pDb = mgmtGetDbByTableId(pInfo->tableId);
  if (pDb == NULL || pDb->dirty) {
    mError("table:%s, failed to get table meta, db not selected", pInfo->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_DB_NOT_SELECTED);
    return;
  }

  if (pTable == NULL) {
    if (htons(pInfo->createFlag) != 1) {
      mError("table:%s, failed to get table meta, table not exist", pInfo->tableId);
      mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_TABLE);
      return;
    } else {
      //TODO: on demand create table from super table if table does not exists
      int32_t contLen = sizeof(SCMCreateTableMsg) + sizeof(STagData);
      SCMCreateTableMsg *pCreateMsg = rpcMallocCont(contLen);
      if (pCreateMsg == NULL) {
        mError("table:%s, failed to create table while get meta info, no enough memory", pInfo->tableId);
        mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
        return;
      }
      memcpy(pCreateMsg->schema, pInfo->tags, sizeof(STagData));
      strcpy(pCreateMsg->tableId, pInfo->tableId);

      SQueuedMsg *newMsg = malloc(sizeof(SQueuedMsg));
      memcpy(newMsg, pMsg, sizeof(SQueuedMsg));
      pMsg->pCont = NULL;

      newMsg->ahandle = newMsg->pCont;
      newMsg->pCont = pCreateMsg;
      mTrace("table:%s, start to create in demand", pInfo->tableId);
      mgmtAddToShellQueue(newMsg);
      return;
    }
  }

  STableMetaMsg *pMeta = rpcMallocCont(sizeof(STableMetaMsg) + sizeof(SSchema) * TSDB_MAX_COLUMNS);
  if (pMeta == NULL) {
    mError("table:%s, failed to get table meta, no enough memory", pTable->info.tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  mgmtDoGetChildTableMeta(pDb, pTable, pMeta, pMsg->usePublicIp);

  SRpcMsg rpcRsp = {
    .handle = pMsg->thandle, 
    .pCont = pMeta, 
    .contLen = pMeta->contLen,
  };
  pMeta->contLen = htons(pMeta->contLen);
  rpcSendResponse(&rpcRsp);
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

    if (strncmp(pDropDb->name, pTable->info.tableId, dbNameLen) == 0) {
      SSdbOperDesc oper = {
        .type = SDB_OPER_TYPE_LOCAL,
        .table = tsChildTableSdb,
        .pObj = pTable,
      };
      sdbDeleteRow(&oper);
      pNode = pLastNode;
      numOfTables++;
      continue;
    }
  }

  mTrace("db:%s, all child tables:%d is dropped from sdb", pDropDb->name, numOfTables);
}

void mgmtDropAllChildTablesInStable(SSuperTableObj *pStable) {
  void *pNode = NULL;
  void *pLastNode = NULL;
  int32_t numOfTables = 0;
  SChildTableObj *pTable = NULL;

  while (1) {
    pNode = sdbFetchRow(tsChildTableSdb, pNode, (void **)&pTable);
    if (pTable == NULL) {
      break;
    }

    if (pTable->superTable == pStable) {
      SSdbOperDesc oper = {
        .type = SDB_OPER_TYPE_LOCAL,
        .table = tsChildTableSdb,
        .pObj = pTable,
      };
      sdbDeleteRow(&oper);
      pNode = pLastNode;
      numOfTables++;
      continue;
    }
  }

  mTrace("stable:%s, all child tables:%d is dropped from sdb", pStable->info.tableId, numOfTables);
}

static STableInfo* mgmtGetTableByPos(uint32_t dnodeId, int32_t vnode, int32_t sid) {
  SDnodeObj *pObj = mgmtGetDnode(dnodeId);
  SVgObj *pVgroup = mgmtGetVgroup(vnode);

  if (pObj == NULL || pVgroup == NULL) {
    return NULL;
  }

  return (STableInfo *)pVgroup->tableList[sid];
}

static void mgmtProcessTableCfgMsg(SRpcMsg *rpcMsg) {
  if (mgmtCheckRedirect(rpcMsg->handle)) return;

  SDMConfigTableMsg *pCfg = (SDMConfigTableMsg *) rpcMsg->pCont;
  pCfg->dnode = htonl(pCfg->dnode);
  pCfg->vnode = htonl(pCfg->vnode);
  pCfg->sid   = htonl(pCfg->sid);
  mTrace("dnode:%s, vnode:%d, sid:%d, receive table config msg", taosIpStr(pCfg->dnode), pCfg->vnode, pCfg->sid);

  STableInfo *pTable = mgmtGetTableByPos(pCfg->dnode, pCfg->vnode, pCfg->sid);
  if (pTable == NULL) {
    mError("dnode:%s, vnode:%d, sid:%d, table not found", taosIpStr(pCfg->dnode), pCfg->vnode, pCfg->sid);
    mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_NOT_ACTIVE_TABLE);
    return;
  }

  mgmtSendSimpleResp(rpcMsg->handle, TSDB_CODE_SUCCESS);

  SMDCreateTableMsg *pMDCreate = NULL;
  pMDCreate = mgmtBuildCreateChildTableMsg(NULL, (SChildTableObj *) pTable);
  if (pMDCreate == NULL) {
    return;
  }

  SRpcIpSet ipSet = mgmtGetIpSetFromIp(pCfg->dnode);
  SRpcMsg rpcRsp = {
      .handle  = NULL,
      .pCont   = pMDCreate,
      .contLen = htonl(pMDCreate->contLen),
      .code    = 0,
      .msgType = TSDB_MSG_TYPE_MD_CREATE_TABLE
  };
  mgmtSendMsgToDnode(&ipSet, &rpcRsp);
}

static void mgmtProcessDropTableRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->handle == NULL) return;

  SQueuedMsg *queueMsg = rpcMsg->handle;
  queueMsg->received++;

  SChildTableObj *pTable = queueMsg->ahandle;
  mTrace("table:%s, drop table rsp received, thandle:%p result:%s", pTable->info.tableId, queueMsg->thandle, tstrerror(rpcMsg->code));

  if (rpcMsg->code != TSDB_CODE_SUCCESS) {
    mError("table:%s, failed to drop in dnode, reason:%s", pTable->info.tableId, tstrerror(rpcMsg->code));
    mgmtSendSimpleResp(queueMsg->thandle, rpcMsg->code);
    free(queueMsg);
    return;
  }

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    mError("table:%s, failed to get vgroup", pTable->info.tableId);
    mgmtSendSimpleResp(queueMsg->thandle, TSDB_CODE_INVALID_VGROUP_ID);
    free(queueMsg);
    return;
  }
  
  SSdbOperDesc oper = {
    .type = SDB_OPER_TYPE_GLOBAL,
    .table = tsChildTableSdb,
    .pObj = pTable
  };
  int32_t code = sdbDeleteRow(&oper);
  if (code != TSDB_CODE_SUCCESS) {
    mError("table:%s, update ctables sdb error", pTable->info.tableId);
    mgmtSendSimpleResp(queueMsg->thandle, TSDB_CODE_SDB_ERROR);
    free(queueMsg);
    return;
  }

  if (pVgroup->numOfTables <= 0) {
    mPrint("vgroup:%d, all tables is dropped, drop vgroup", pVgroup->vgId);
    mgmtDropVgroup(pVgroup, NULL);
  }

  mgmtSendSimpleResp(queueMsg->thandle, TSDB_CODE_SUCCESS);
  free(queueMsg);
}

static void mgmtProcessCreateTableRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->handle == NULL) return;

  SQueuedMsg *queueMsg = rpcMsg->handle;
  queueMsg->received++;

  SChildTableObj *pTable = queueMsg->ahandle;
  mTrace("table:%s, create table rsp received, thandle:%p ahandle:%p result:%s", pTable->info.tableId, queueMsg->thandle,
         rpcMsg->handle, tstrerror(rpcMsg->code));

  if (rpcMsg->code != TSDB_CODE_SUCCESS) {
    SSdbOperDesc oper = {
      .type = SDB_OPER_TYPE_GLOBAL,
      .table = tsChildTableSdb,
      .pObj = pTable
    };
    sdbDeleteRow(&oper);
    
    mError("table:%s, failed to create in dnode, reason:%s", pTable->info.tableId, tstrerror(rpcMsg->code));
    mgmtSendSimpleResp(queueMsg->thandle, rpcMsg->code);
  } else {
    mTrace("table:%s, created in dnode", pTable->info.tableId);
    if (queueMsg->msgType != TSDB_MSG_TYPE_CM_CREATE_TABLE) {
      SQueuedMsg *newMsg = calloc(1, sizeof(SQueuedMsg));
      newMsg->msgType = queueMsg->msgType;
      newMsg->thandle = queueMsg->thandle;
      newMsg->pDb     = queueMsg->pDb;
      newMsg->pUser   = queueMsg->pUser;
      newMsg->contLen = queueMsg->contLen;
      newMsg->pCont   = rpcMallocCont(newMsg->contLen);
      memcpy(newMsg->pCont, queueMsg->pCont, newMsg->contLen);
      mTrace("table:%s, start to get meta", pTable->info.tableId);
      mgmtAddToShellQueue(newMsg);
    } else {
      mgmtSendSimpleResp(queueMsg->thandle, rpcMsg->code);
    }
  }

  free(queueMsg);
}

static void mgmtProcessAlterTableRsp(SRpcMsg *rpcMsg) {
  mTrace("alter table rsp received, handle:%p code:%d", rpcMsg->handle, rpcMsg->code);
}

static void mgmtProcessMultiTableMetaMsg(SQueuedMsg *pMsg) {
  SRpcConnInfo connInfo;
  if (rpcGetConnInfo(pMsg->thandle, &connInfo) != 0) {
    mError("conn:%p is already released while get mulit table meta", pMsg->thandle);
    return;
  }

  bool usePublicIp = (connInfo.serverIp == tsPublicIpInt);
  SUserObj *pUser = mgmtGetUser(connInfo.user);
  if (pUser == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_USER);
    return;
  }

  SCMMultiTableInfoMsg *pInfo = pMsg->pCont;
  pInfo->numOfTables = htonl(pInfo->numOfTables);

  int32_t totalMallocLen = 4*1024*1024; // first malloc 4 MB, subsequent reallocation as twice
  SMultiTableMeta *pMultiMeta = rpcMallocCont(totalMallocLen);
  if (pMultiMeta == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
    return;
  }

  pMultiMeta->contLen = sizeof(SMultiTableMeta);
  pMultiMeta->numOfTables = 0;

  for (int t = 0; t < pInfo->numOfTables; ++t) {
    char *tableId = (char*)(pInfo->tableIds + t * TSDB_TABLE_ID_LEN);
    SChildTableObj *pTable = mgmtGetChildTable(tableId);
    if (pTable == NULL) continue;

    SDbObj *pDb = mgmtGetDbByTableId(tableId);
    if (pDb == NULL) continue;

    int availLen = totalMallocLen - pMultiMeta->contLen;
    if (availLen <= sizeof(STableMetaMsg) + sizeof(SSchema) * TSDB_MAX_COLUMNS) {
      //TODO realloc
      //totalMallocLen *= 2;
      //pMultiMeta = rpcReMalloc(pMultiMeta, totalMallocLen);
      //if (pMultiMeta == NULL) {
      ///  rpcSendResponse(ahandle, TSDB_CODE_SERV_OUT_OF_MEMORY, NULL, 0);
      //  return TSDB_CODE_SERV_OUT_OF_MEMORY;
      //} else {
      //  t--;
      //  continue;
      //}
    }

    STableMetaMsg *pMeta = (STableMetaMsg *)(pMultiMeta->metas + pMultiMeta->contLen);
    int32_t code = mgmtDoGetChildTableMeta(pDb, pTable, pMeta, usePublicIp);
    if (code == TSDB_CODE_SUCCESS) {
      pMultiMeta->numOfTables ++;
      pMultiMeta->contLen += pMeta->contLen;
    }
  }

  SRpcMsg rpcRsp = {0};
  rpcRsp.handle = pMsg->thandle;
  rpcRsp.pCont = pMultiMeta;
  rpcRsp.contLen = pMultiMeta->contLen;
  rpcSendResponse(&rpcRsp);
}

static int32_t mgmtGetShowTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mgmtGetDb(pShow->db);
  if (pDb == NULL) {
    return TSDB_CODE_DB_NOT_SELECTED;
  }

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "table name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create time");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "columns");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "stable name");
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

  SUserObj *pUser = mgmtGetUserFromConn(pConn, NULL);
  if (pUser == NULL) return 0;

  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    if (strcmp(pUser->user, "root") != 0 && strcmp(pUser->user, "_root") != 0 &&
        strcmp(pUser->user, "monitor") != 0) {
      return 0;
    }
  }

  int32_t numOfRows  = 0;
  SChildTableObj *pTable = NULL;
  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  char prefix[64] = {0};
  strcpy(prefix, pDb->name);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = strlen(prefix);

  while (numOfRows < rows) {
    pShow->pNode = sdbFetchRow(tsChildTableSdb, pShow->pNode, (void **) &pTable);
    if (pTable == NULL) break;

    // not belong to current db
    if (strncmp(pTable->info.tableId, prefix, prefixLen)) {
      continue;
    }

    char tableName[TSDB_TABLE_NAME_LEN] = {0};
    memset(tableName, 0, tListLen(tableName));
    
    // pattern compare for meter name
    mgmtExtractTableName(pTable->info.tableId, tableName);

    if (pShow->payloadLen > 0 &&
        patternMatch(pShow->payload, tableName, TSDB_TABLE_NAME_LEN, &info) != TSDB_PATTERN_MATCH) {
      continue;
    }

    int32_t cols = 0;

    char *pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strncpy(pWrite, tableName, TSDB_TABLE_NAME_LEN);
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
    if (pTable->info.type == TSDB_CHILD_TABLE) {
      mgmtExtractTableName(pTable->superTableId, pWrite);
    }
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  const int32_t NUM_OF_COLUMNS = 4;

  mgmtVacuumResult(data, NUM_OF_COLUMNS, numOfRows, rows, pShow);

  return numOfRows;
}

void mgmtAlterChildTable(SQueuedMsg *pMsg, SChildTableObj *pTable) {
  int32_t code = TSDB_CODE_OPS_NOT_SUPPORT;
  SCMAlterTableMsg *pAlter = pMsg->pCont;;

  if (pAlter->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
    code = mgmtModifyChildTableTagValueByName(pTable, pAlter->schema[0].name, pAlter->tagVal);
  } else if (pAlter->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
    code = mgmtAddNormalTableColumn(pTable, pAlter->schema, 1);
  } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
    code = mgmtDropNormalTableColumnByName(pTable, pAlter->schema[0].name);
  } else {
  }

  mgmtSendSimpleResp(pMsg->thandle, code);
}