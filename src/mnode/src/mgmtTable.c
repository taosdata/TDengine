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
#include "mgmtTable.h"
//#include "../../client/inc/tsclient.h"
#include "mgmtAcct.h"
#include "mgmtChildTable.h"
#include "mgmtDClient.h"
#include "mgmtDb.h"
#include "mgmtDnode.h"
#include "mgmtGrant.h"
#include "mgmtMnode.h"
#include "mgmtNormalTable.h"
#include "mgmtProfile.h"
#include "mgmtShell.h"
#include "mgmtSuperTable.h"
#include "mgmtUser.h"
#include "mgmtVgroup.h"
#include "mnode.h"
#include "os.h"
#include "qast.h"
#include "qextbuffer.h"
#include "taoserror.h"
#include "taosmsg.h"
#include "tscompression.h"
#include "tskiplist.h"
#include "tsqlfunction.h"
#include "tstatus.h"
#include "ttime.h"

extern void *tsNormalTableSdb;
extern void *tsChildTableSdb;

static void mgmtProcessCreateTableMsg(SQueuedMsg *queueMsg);
static void mgmtProcessDropTableMsg(SQueuedMsg *queueMsg);
static void mgmtProcessAlterTableMsg(SQueuedMsg *queueMsg);
static void mgmtProcessTableMetaMsg(SQueuedMsg *queueMsg);
static void mgmtProcessMultiTableMetaMsg(SQueuedMsg *queueMsg);
static void mgmtProcessSuperTableMetaMsg(SQueuedMsg *queueMsg);
static void mgmtProcessCreateTableRsp(SRpcMsg *rpcMsg);
static void mgmtProcessDropTableRsp(SRpcMsg *rpcMsg);
static int32_t mgmtGetShowTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn);
static int32_t mgmtRetrieveShowTables(SShowObj *pShow, char *data, int32_t rows, void *pConn);
static void mgmtProcessGetTableMeta(STableInfo *pTable, void *thandle);

int32_t mgmtInitTables() {
  int32_t code = mgmtInitSuperTables();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = mgmtInitNormalTables();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = mgmtInitChildTables();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  mgmtSetVgroupIdPool();

  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_CREATE_TABLE, mgmtProcessCreateTableMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_DROP_TABLE, mgmtProcessDropTableMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_ALTER_TABLE, mgmtProcessAlterTableMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_TABLE_META, mgmtProcessTableMetaMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_TABLES_META, mgmtProcessMultiTableMetaMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_STABLE_META, mgmtProcessSuperTableMetaMsg);
  mgmtAddShellShowMetaHandle(TSDB_MGMT_TABLE_TABLE, mgmtGetShowTableMeta);
  mgmtAddShellShowRetrieveHandle(TSDB_MGMT_TABLE_TABLE, mgmtRetrieveShowTables);
  mgmtAddDClientRspHandle(TSDB_MSG_TYPE_MD_CREATE_TABLE_RSP, mgmtProcessCreateTableRsp);
  mgmtAddDClientRspHandle(TSDB_MSG_TYPE_MD_DROP_TABLE_RSP, mgmtProcessDropTableRsp);
  mgmtAddDClientRspHandle(TSDB_MSG_TYPE_MD_ALTER_TABLE_RSP, NULL);
  mgmtAddDClientRspHandle(TSDB_MSG_TYPE_MD_DROP_STABLE_RSP, NULL);

  return TSDB_CODE_SUCCESS;
}

STableInfo* mgmtGetTable(char *tableId) {
  STableInfo *tableInfo = (STableInfo *) mgmtGetSuperTable(tableId);
  if (tableInfo != NULL) {
    return tableInfo;
  }

  tableInfo = (STableInfo *) mgmtGetNormalTable(tableId);
  if (tableInfo != NULL) {
    return tableInfo;
  }

  tableInfo = (STableInfo *) mgmtGetChildTable(tableId);
  if (tableInfo != NULL) {
    return tableInfo;
  }

  return NULL;
}

STableInfo* mgmtGetTableByPos(uint32_t dnodeIp, int32_t vnode, int32_t sid) {
  SDnodeObj *pObj = mgmtGetDnode(dnodeIp);
  if (pObj != NULL && vnode >= 0 && vnode < pObj->numOfVnodes) {
    int32_t vgId = pObj->vload[vnode].vgId;
    SVgObj *pVgroup = mgmtGetVgroup(vgId);
    if (pVgroup) {
      return pVgroup->tableList[sid];
    }
  }

  return NULL;
}

int32_t mgmtGetTableMeta(SDbObj *pDb, STableInfo *pTable, STableMetaMsg *pMeta, bool usePublicIp) {
  if (pTable->type == TSDB_CHILD_TABLE) {
    mgmtGetChildTableMeta(pDb, (SChildTableObj *) pTable, pMeta, usePublicIp);
  } else if (pTable->type == TSDB_NORMAL_TABLE) {
    mgmtGetNormalTableMeta(pDb, (SNormalTableObj *) pTable, pMeta, usePublicIp);
  } else if (pTable->type == TSDB_SUPER_TABLE) {
    mgmtGetSuperTableMeta(pDb, (SSuperTableObj *) pTable, pMeta, usePublicIp);
  } else {
    mTrace("%s, uid:%" PRIu64 " table meta retrieve failed, invalid type", pTable->tableId, pTable->uid);
    return TSDB_CODE_INVALID_TABLE;
  }

  mTrace("%s, uid:%" PRIu64 " table meta is retrieved", pTable->tableId, pTable->uid);
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtAlterTable(SDbObj *pDb, SCMAlterTableMsg *pAlter) {
  STableInfo *pTable = mgmtGetTable(pAlter->tableId);
  if (pTable == NULL) {
    return TSDB_CODE_INVALID_TABLE;
  }

  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    return TSDB_CODE_MONITOR_DB_FORBIDDEN;
  }

  if (pAlter->type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN) {
    if (pTable->type == TSDB_SUPER_TABLE) {
      return mgmtAddSuperTableTag((SSuperTableObj *) pTable, pAlter->schema, 1);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN) {
    if (pTable->type == TSDB_SUPER_TABLE) {
      return mgmtDropSuperTableTag((SSuperTableObj *) pTable, pAlter->schema[0].name);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN) {
    if (pTable->type == TSDB_SUPER_TABLE) {
      return mgmtModifySuperTableTagNameByName((SSuperTableObj *) pTable, pAlter->schema[0].name, pAlter->schema[1].name);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
    if (pTable->type == TSDB_CHILD_TABLE) {
      return mgmtModifyChildTableTagValueByName((SChildTableObj *) pTable, pAlter->schema[0].name, pAlter->tagVal);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
    if (pTable->type == TSDB_NORMAL_TABLE) {
      return mgmtAddNormalTableColumn((SNormalTableObj *) pTable, pAlter->schema, 1);
    } else if (pTable->type == TSDB_SUPER_TABLE) {
      return mgmtAddSuperTableColumn((SSuperTableObj *) pTable, pAlter->schema, 1);
    } else {}
  } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
    if (pTable->type == TSDB_NORMAL_TABLE) {
      return mgmtDropNormalTableColumnByName((SNormalTableObj *) pTable, pAlter->schema[0].name);
    } else if (pTable->type == TSDB_SUPER_TABLE) {
      return mgmtDropSuperTableColumnByName((SSuperTableObj *) pTable, pAlter->schema[0].name);
    } else {}
  } else {}

  return TSDB_CODE_OPS_NOT_SUPPORT;
}

void mgmtCleanUpTables() {
  mgmtCleanUpNormalTables();
  mgmtCleanUpChildTables();
  mgmtCleanUpSuperTables();
}

int32_t mgmtGetShowTableMeta(STableMetaMsg *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mgmtGetDb(pShow->db);
  if (pDb == NULL) {
    return TSDB_CODE_DB_NOT_SELECTED;
  }

  int32_t cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN;
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

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN;
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

  return 0;
}

/*
 * remove the hole in result set
 */
static void mgmtVacuumResult(char *data, int32_t numOfCols, int32_t rows, int32_t capacity, SShowObj *pShow) {
  if (rows < capacity) {
    for (int32_t i = 0; i < numOfCols; ++i) {
      memmove(data + pShow->offset[i] * rows, data + pShow->offset[i] * capacity, pShow->bytes[i] * rows);
    }
  }
}

int32_t mgmtRetrieveShowTables(SShowObj *pShow, char *data, int32_t rows, void *pConn) {
  SDbObj *pDb = mgmtGetDb(pShow->db);
  if (pDb == NULL) return 0;

  SUserObj *pUser = mgmtGetUserFromConn(pConn);
  if (pUser == NULL) return 0;

  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    if (strcmp(pUser->user, "root") != 0 && strcmp(pUser->user, "_root") != 0 &&
        strcmp(pUser->user, "monitor") != 0) {
      return 0;
    }
  }

  int32_t numOfRows  = 0;
  int32_t numOfRead  = 0;
  int32_t cols       = 0;
  void    *pTable    = NULL;
  char    *pWrite    = NULL;
  char    prefix[20] = {0};
  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  strcpy(prefix, pDb->name);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = strlen(prefix);

  while (numOfRows < rows) {
    int16_t numOfColumns      = 0;
    int64_t createdTime       = 0;
    char    *tableId          = NULL;
    char    *superTableId     = NULL;
    void    *pNormalTableNode = sdbFetchRow(tsNormalTableSdb, pShow->pNode, (void **) &pTable);
    if (pTable != NULL) {
      SNormalTableObj *pNormalTable = (SNormalTableObj *) pTable;
      pShow->pNode = pNormalTableNode;
      tableId      = pNormalTable->tableId;
      superTableId = NULL;
      createdTime  = pNormalTable->createdTime;
      numOfColumns = pNormalTable->numOfColumns;
    } else {
      void *pChildTableNode = sdbFetchRow(tsChildTableSdb, pShow->pNode, (void **) &pTable);
      if (pTable != NULL) {
        SChildTableObj *pChildTable = (SChildTableObj *) pTable;
        pShow->pNode = pChildTableNode;
        tableId      = pChildTable->tableId;
        superTableId = pChildTable->superTableId;
        createdTime  = pChildTable->createdTime;
        numOfColumns = pChildTable->superTable->numOfColumns;
      } else {
        break;
      }
    }

    // not belong to current db
    if (strncmp(tableId, prefix, prefixLen)) {
      continue;
    }

    char tableName[TSDB_TABLE_NAME_LEN] = {0};
    memset(tableName, 0, tListLen(tableName));
    numOfRead++;

    // pattern compare for meter name
    extractTableName(tableId, tableName);

    if (pShow->payloadLen > 0 &&
        patternMatch(pShow->payload, tableName, TSDB_TABLE_NAME_LEN, &info) != TSDB_PATTERN_MATCH) {
      continue;
    }

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strncpy(pWrite, tableName, TSDB_TABLE_NAME_LEN);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *) pWrite = createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *) pWrite = numOfColumns;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (superTableId != NULL) {
      extractTableName(superTableId, pWrite);
    }
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRead;
  const int32_t NUM_OF_COLUMNS = 4;

  mgmtVacuumResult(data, NUM_OF_COLUMNS, numOfRows, rows, pShow);

  return numOfRows;
}

void mgmtProcessCreateTableMsg(SQueuedMsg *pMsg) {
  if (mgmtCheckRedirect(pMsg->thandle)) return;

  SCMCreateTableMsg *pCreate = pMsg->pCont;
  mTrace("table:%s, create msg is received from thandle:%p", pCreate->tableId, pMsg->thandle);

  if (mgmtCheckExpired()) {
    mError("table:%s, failed to create, grant expired", pCreate->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_GRANT_EXPIRED);
    return;
  }

  if (!pMsg->pUser->writeAuth) {
    mError("table:%s, failed to create, no rights", pCreate->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_RIGHTS);
    return;
  }

  SAcctObj *pAcct = pMsg->pUser->pAcct;
  int32_t code = mgmtCheckTableLimit(pAcct, htons(pCreate->numOfColumns));
  if (code != TSDB_CODE_SUCCESS) {
    mError("table:%s, failed to create, exceed the limit", pCreate->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_RIGHTS);
    return;
  }

  pMsg->pDb = mgmtGetDb(pCreate->db);
  if (pMsg->pDb == NULL) {
    mError("table:%s, failed to create, db not selected", pCreate->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_DB_NOT_SELECTED);
    return;
  }

  STableInfo *pTable = mgmtGetTable(pCreate->tableId);
  if (pTable != NULL) {
    if (pCreate->igExists) {
      mTrace("table:%s is already exist", pCreate->tableId);
      mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SUCCESS);
      return;
    } else {
      mError("table:%s, failed to create, table already exist", pCreate->tableId);
      mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_TABLE_ALREADY_EXIST);
      return;
    }
  }

  if (pCreate->numOfTags != 0) {
    mTrace("table:%s, is a super table", pCreate->tableId);
    code = mgmtCreateSuperTable(pCreate);
    mgmtSendSimpleResp(pMsg->thandle, code);
    return;
  }

  code = mgmtCheckTimeSeries(pCreate->numOfColumns);
  if (code != TSDB_CODE_SUCCESS) {
    mError("table:%s, failed to create, timeseries exceed the limit", pCreate->tableId);
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

  SMDCreateTableMsg *pMDCreate = NULL;
  if (pCreate->numOfColumns == 0) {
    mTrace("table:%s, is a child table, vgroup:%d sid:%d ahandle:%p", pCreate->tableId, pVgroup->vgId, sid, pMsg);
    pTable = mgmtCreateChildTable(pCreate, pVgroup, sid);
    if (pTable == NULL) {
      mgmtSendSimpleResp(pMsg->thandle, terrno);
      return;
    }
    pMDCreate = mgmtBuildCreateChildTableMsg(pCreate, (SChildTableObj *) pTable);
    if (pMDCreate == NULL) {
      mgmtSendSimpleResp(pMsg->thandle, terrno);
      return;
    }
  } else {
    mTrace("table:%s, is a normal table, vgroup:%d sid:%d ahandle:%p", pCreate->tableId, pVgroup->vgId, sid, pMsg);
    pTable = mgmtCreateNormalTable(pCreate, pVgroup, sid);
    if (pTable == NULL) {
      mgmtSendSimpleResp(pMsg->thandle, terrno);
      return;
    }
    pMDCreate = mgmtBuildCreateNormalTableMsg((SNormalTableObj *) pTable);
    if (pMDCreate == NULL) {
      mgmtSendSimpleResp(pMsg->thandle, terrno);
      return;
    }
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

void mgmtProcessDropTableMsg(SQueuedMsg *pMsg) {
  if (mgmtCheckRedirect(pMsg->thandle)) return;

  SCMDropTableMsg *pDrop = pMsg->pCont;
  mTrace("table:%s, drop table msg is received from thandle:%p", pDrop->tableId, pMsg->thandle);

  if (mgmtCheckExpired()) {
    mError("table:%s, failed to drop, grant expired", pDrop->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_GRANT_EXPIRED);
    return;
  }

  if (!pMsg->pUser->writeAuth) {
    mError("table:%s, failed to drop, no rights", pDrop->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_RIGHTS);
    return;
  }

  SDbObj *pDb = mgmtGetDbByTableId(pDrop->tableId);
  if (pDb == NULL) {
    mError("table:%s, failed to drop table, db not selected", pDrop->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_DB_NOT_SELECTED);
    return;
  }

  STableInfo *pTable = mgmtGetTable(pDrop->tableId);
  if (pTable == NULL) {
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

  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    mError("table:%s, failed to drop table, in monitor database", pDrop->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_MONITOR_DB_FORBIDDEN);
    return;
  }

  SQueuedMsg *newMsg = malloc(sizeof(SQueuedMsg));
  memcpy(newMsg, pMsg, sizeof(SQueuedMsg));
  pMsg->pCont = NULL;
  int32_t code;

  switch (pTable->type) {
    case TSDB_SUPER_TABLE:
      mTrace("table:%s, start to drop super table", pDrop->tableId);
      code = mgmtDropSuperTable(newMsg, pDb, (SSuperTableObj *) pTable);
      break;
    case TSDB_CHILD_TABLE:
      mTrace("table:%s, start to drop child table", pDrop->tableId);
      code = mgmtDropChildTable(newMsg, (SChildTableObj *) pTable);
      break;
    case TSDB_NORMAL_TABLE:
      mTrace("table:%s, start to drop normal table", pDrop->tableId);
      code = mgmtDropNormalTable(newMsg, (SNormalTableObj *) pTable);
      break;
    case TSDB_STREAM_TABLE:
      mTrace("table:%s, start to drop stream table", pDrop->tableId);
      code = mgmtDropNormalTable(newMsg, (SNormalTableObj *) pTable);
      break;
    default:
      code = TSDB_CODE_INVALID_TABLE_TYPE;
      mError("table:%s, invalid table type:%d", pDrop->tableId, pTable->type);
  }

  if (code != TSDB_CODE_SUCCESS) {
    free(newMsg);
    mgmtSendSimpleResp(pMsg->thandle, code);
  }
}

void mgmtProcessAlterTableMsg(SQueuedMsg *pMsg) {
  if (mgmtCheckRedirect(pMsg->thandle)) {
    return;
  }

  SUserObj *pUser = mgmtGetUserFromConn(pMsg->thandle);
  if (pUser == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_USER);
    return;
  }

  SCMAlterTableMsg *pAlter = pMsg->pCont;

  int32_t code;
  if (!pUser->writeAuth) {
    code = TSDB_CODE_NO_RIGHTS;
  } else {
    pAlter->type      = htons(pAlter->type);
    pAlter->numOfCols = htons(pAlter->numOfCols);

    if (pAlter->numOfCols > 2) {
      mError("table:%s error numOfCols:%d in alter table", pAlter->tableId, pAlter->numOfCols);
      code = TSDB_CODE_APP_ERROR;
    } else {
      SDbObj *pDb = mgmtGetDb(pAlter->db);
      if (pDb) {
        for (int32_t i = 0; i < pAlter->numOfCols; ++i) {
          pAlter->schema[i].bytes = htons(pAlter->schema[i].bytes);
        }

        code = mgmtAlterTable(pDb, pAlter);
        if (code == 0) {
          mLPrint("table:%s is altered by %s", pAlter->tableId, pUser->user);
        }
      } else {
        code = TSDB_CODE_DB_NOT_SELECTED;
      }
    }
  }

  mgmtSendSimpleResp(pMsg->thandle, code);
}

void mgmtProcessGetTableMeta(STableInfo *pTable, void *thandle) {
  SRpcMsg rpcRsp = {.handle = thandle, .pCont = NULL, .contLen = 0, .code = 0, .msgType = 0};
  SDbObj* pDb = mgmtGetDbByTableId(pTable->tableId);
  if (pDb == NULL || pDb->dropStatus != TSDB_DB_STATUS_READY) {
    mError("table:%s, failed to get table meta, db not selected", pTable->tableId);
    rpcRsp.code = TSDB_CODE_DB_NOT_SELECTED;
    rpcSendResponse(&rpcRsp);
    return;
  }

  SRpcConnInfo connInfo;
  if (rpcGetConnInfo(thandle, &connInfo) != 0) {
    mError("conn:%p is already released while get table meta", thandle);
    return;
  }

  bool usePublicIp = (connInfo.serverIp == tsPublicIpInt);
  
  STableMetaMsg *pMeta = rpcMallocCont(sizeof(STableMetaMsg) + sizeof(SSchema) * TSDB_MAX_COLUMNS);
  rpcRsp.code = mgmtGetTableMeta(pDb, pTable, pMeta, usePublicIp);

  if (rpcRsp.code != TSDB_CODE_SUCCESS) {
    rpcFreeCont(pMeta);
  } else {
    pMeta->contLen = htons(pMeta->contLen);
    rpcRsp.pCont   = pMeta;
    rpcRsp.contLen = pMeta->contLen;
  }

  rpcSendResponse(&rpcRsp);
}

void mgmtProcessTableMetaMsg(SQueuedMsg *pMsg) {
  SCMTableInfoMsg *pInfo = pMsg->pCont;
  pInfo->createFlag = htons(pInfo->createFlag);

  SUserObj *pUser = mgmtGetUserFromConn(pMsg->thandle);
  if (pUser == NULL) {
    mError("table:%s, failed to get table meta, invalid user", pInfo->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_USER);
    return;
  }

  STableInfo *pTable = mgmtGetTable(pInfo->tableId);
  if (pTable == NULL) {
    if (pInfo->createFlag != 1) {
      mError("table:%s, failed to get table meta, table not exist", pInfo->tableId);
      mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_TABLE);
      return;
    } else {
      // on demand create table from super table if table does not exists
      if (mgmtCheckRedirect(pMsg->thandle)) {
        mError("table:%s, failed to create table while get meta info, need redirect message", pInfo->tableId);
        return;
      }

      int32_t contLen = sizeof(SCMCreateTableMsg) + sizeof(STagData);
      SCMCreateTableMsg *pCreateMsg = rpcMallocCont(contLen);
      if (pCreateMsg == NULL) {
        mError("table:%s, failed to create table while get meta info, no enough memory", pInfo->tableId);
        mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_SERV_OUT_OF_MEMORY);
        return;
      }

      memcpy(pCreateMsg->schema, pInfo->tags, sizeof(STagData));
      strcpy(pCreateMsg->tableId, pInfo->tableId);

      mError("table:%s, start to create table while get meta info", pInfo->tableId);
//      mgmtCreateTable(pCreateMsg, contLen, pMsg->thandle, true);
    }
  } else {
    mgmtProcessGetTableMeta(pTable, pMsg->thandle);
  }
}

void mgmtProcessMultiTableMetaMsg(SQueuedMsg *pMsg) {
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
    STableInfo *pTable = mgmtGetTable(tableId);
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
    int32_t code = mgmtGetTableMeta(pDb, pTable, pMeta, usePublicIp);
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

void mgmtProcessSuperTableMetaMsg(SQueuedMsg *pMsg) {
  SCMSuperTableInfoMsg *pInfo = pMsg->pCont;
  STableInfo *pTable = mgmtGetSuperTable(pInfo->tableId);
  if (pTable == NULL) {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_TABLE);
    return;
  }

  SCMSuperTableInfoRsp *pRsp = mgmtGetSuperTableVgroup((SSuperTableObj *) pTable);
  if (pRsp != NULL) {
    int32_t msgLen = sizeof(SSuperTableObj) + htonl(pRsp->numOfDnodes) * sizeof(int32_t);
    SRpcMsg rpcRsp = {0};
    rpcRsp.handle = pMsg->thandle;
    rpcRsp.pCont = pRsp;
    rpcRsp.contLen = msgLen;
    rpcSendResponse(&rpcRsp);
  } else {
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_TABLE);
  }
}

static void mgmtProcessCreateTableRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->handle == NULL) return;

  SQueuedMsg *queueMsg = rpcMsg->handle;
  queueMsg->received++;

  STableInfo *pTable = queueMsg->ahandle;
  mTrace("table:%s, create table rsp received, thandle:%p ahandle:%p result:%s", pTable->tableId, queueMsg->thandle,
         rpcMsg->handle, tstrerror(rpcMsg->code));

  if (rpcMsg->code != TSDB_CODE_SUCCESS) {
    if (pTable->type == TSDB_CHILD_TABLE) {
      sdbDeleteRow(tsChildTableSdb, pTable);
    } else if (pTable->type == TSDB_NORMAL_TABLE){
      sdbDeleteRow(tsNormalTableSdb, pTable);
    } else {}
    mError("table:%s, failed to create in dnode, reason:%s", pTable->tableId, tstrerror(rpcMsg->code));
    mgmtSendSimpleResp(queueMsg->thandle, rpcMsg->code);
  } else {
    mTrace("table:%s, created in dnode", pTable->tableId);
    if (queueMsg->msgType != TSDB_MSG_TYPE_CM_CREATE_TABLE) {
      SQueuedMsg *newMsg = calloc(1, sizeof(SQueuedMsg));
      newMsg->msgType = queueMsg->msgType;
      newMsg->thandle = queueMsg->thandle;
      newMsg->pDb     = queueMsg->pDb;
      newMsg->pUser   = queueMsg->pUser;
      newMsg->contLen = queueMsg->contLen;
      newMsg->pCont   = rpcMallocCont(newMsg->contLen);
      memcpy(newMsg->pCont, queueMsg->pCont, newMsg->contLen);
      mTrace("table:%s, start to get meta", pTable->tableId);
      mgmtAddToShellQueue(newMsg);
    } else {
      mgmtSendSimpleResp(queueMsg->thandle, rpcMsg->code);
    }
  }

  free(queueMsg);
}

static void mgmtProcessDropTableRsp(SRpcMsg *rpcMsg) {
  if (rpcMsg->handle == NULL) return;

  SQueuedMsg *queueMsg = rpcMsg->handle;
  queueMsg->received++;

  STableInfo *pTable = queueMsg->ahandle;
  mTrace("table:%s, drop table rsp received, thandle:%p result:%s", pTable->tableId, queueMsg->thandle, tstrerror(rpcMsg->code));

  if (rpcMsg->code != TSDB_CODE_SUCCESS) {
    mError("table:%s, failed to drop in dnode, reason:%s", pTable->tableId, tstrerror(rpcMsg->code));
    mgmtSendSimpleResp(queueMsg->thandle, rpcMsg->code);
    free(queueMsg);
    return;
  }

  SVgObj *pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    mError("table:%s, failed to get vgroup", pTable->tableId);
    mgmtSendSimpleResp(queueMsg->thandle, TSDB_CODE_INVALID_VGROUP_ID);
    free(queueMsg);
    return;
  }

  if (pTable->type == TSDB_CHILD_TABLE) {
    if (sdbDeleteRow(tsChildTableSdb, pTable) < 0) {
      mError("table:%s, update ctables sdb error", pTable->tableId);
      mgmtSendSimpleResp(queueMsg->thandle, TSDB_CODE_SDB_ERROR);
      free(queueMsg);
      return;
    }
  } else if (pTable->type == TSDB_NORMAL_TABLE){
    if (sdbDeleteRow(tsNormalTableSdb, pTable) < 0) {
      mError("table:%s, update ntables sdb error", pTable->tableId);
      mgmtSendSimpleResp(queueMsg->thandle, TSDB_CODE_SDB_ERROR);
      free(queueMsg);
      return;
    }
  }

  if (pVgroup->numOfTables <= 0) {
    mPrint("vgroup:%d, all tables is dropped, drop vgroup", pVgroup->vgId);
    mgmtDropVgroup(pVgroup, NULL);
  }

  mgmtSendSimpleResp(queueMsg->thandle, TSDB_CODE_SUCCESS);
  free(queueMsg);
}
