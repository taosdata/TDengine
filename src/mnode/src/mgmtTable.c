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
#include "mgmtAcct.h"
#include "mgmtChildTable.h"
#include "mgmtDClient.h"
#include "mgmtDb.h"
#include "mgmtDnode.h"
#include "mgmtDServer.h"
#include "mgmtGrant.h"
#include "mgmtMnode.h"
#include "mgmtProfile.h"
#include "mgmtSdb.h"
#include "mgmtShell.h"
#include "mgmtSuperTable.h"
#include "mgmtTable.h"
#include "mgmtUser.h"

static void mgmtProcessCreateTableMsg(SQueuedMsg *queueMsg);
static void mgmtProcessDropTableMsg(SQueuedMsg *queueMsg);
static void mgmtProcessAlterTableMsg(SQueuedMsg *queueMsg);
static void mgmtProcessTableMetaMsg(SQueuedMsg *queueMsg);

int32_t mgmtInitTables() {
  int32_t code = mgmtInitSuperTables();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = mgmtInitChildTables();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_CREATE_TABLE, mgmtProcessCreateTableMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_DROP_TABLE, mgmtProcessDropTableMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_ALTER_TABLE, mgmtProcessAlterTableMsg);
  mgmtAddShellMsgHandle(TSDB_MSG_TYPE_CM_TABLE_META, mgmtProcessTableMetaMsg);

  return TSDB_CODE_SUCCESS;
}

STableInfo *mgmtGetTable(char *tableId) {
  STableInfo *tableInfo = mgmtGetSuperTable(tableId);
  if (tableInfo != NULL) {
    return tableInfo;
  }

  tableInfo = mgmtGetChildTable(tableId);
  if (tableInfo != NULL) {
    return tableInfo;
  }

  return NULL;
}

void mgmtCleanUpTables() {
  mgmtCleanUpChildTables();
  mgmtCleanUpSuperTables();
}

void mgmtExtractTableName(char* tableId, char* name) {
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
  mTrace("table:%s, create msg is received from thandle:%p", pCreate->tableId, pMsg->thandle);

  if (mgmtCheckRedirect(pMsg->thandle)) return;

  if (!pMsg->pUser->writeAuth) {
    mError("table:%s, failed to create, no rights", pCreate->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_RIGHTS);
    return;
  }

  pMsg->pDb = mgmtGetDb(pCreate->db);
  if (pMsg->pDb == NULL || pMsg->pDb->dirty) {
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
    mTrace("table:%s, is a stable", pCreate->tableId);
    mgmtCreateSuperTable(pMsg);
  } else {
    mTrace("table:%s, is a ctable", pCreate->tableId);
    mgmtCreateChildTable(pMsg);
  }
}

static void mgmtProcessDropTableMsg(SQueuedMsg *pMsg) {
  SCMDropTableMsg *pDrop = pMsg->pCont;
  mTrace("table:%s, drop table msg is received from thandle:%p", pDrop->tableId, pMsg->thandle);

  if (mgmtCheckRedirect(pMsg->thandle)) return;

  if (!pMsg->pUser->writeAuth) {
    mError("table:%s, failed to drop, no rights", pDrop->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_RIGHTS);
    return;
  }

  pMsg->pDb = mgmtGetDbByTableId(pDrop->tableId);
  if (pMsg->pDb == NULL || pMsg->pDb->dirty) {
    mError("table:%s, failed to drop table, db not selected", pDrop->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_DB_NOT_SELECTED);
    return;
  }

  if (mgmtCheckIsMonitorDB(pMsg->pDb->name, tsMonitorDbName)) {
    mError("table:%s, failed to drop table, in monitor database", pDrop->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_MONITOR_DB_FORBIDDEN);
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

  if (pTable->type == TSDB_SUPER_TABLE) {
    mTrace("table:%s, start to drop stable", pDrop->tableId);
    mgmtDropSuperTable(pMsg, (SSuperTableObj *)pTable);
  } else {
    mTrace("table:%s, start to drop ctable", pDrop->tableId);
    mgmtDropChildTable(pMsg, (SChildTableObj *)pTable);
  }
}

static void mgmtProcessAlterTableMsg(SQueuedMsg *pMsg) {
  SCMAlterTableMsg *pAlter = pMsg->pCont;
  mTrace("table:%s, alter table msg is received from thandle:%p", pAlter->tableId, pMsg->thandle);

  if (mgmtCheckRedirect(pMsg->thandle)) return;

  if (!pMsg->pUser->writeAuth) {
    mTrace("table:%s, failed to alter table, no rights", pAlter->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_NO_RIGHTS);
    return;
  }

  pMsg->pDb = mgmtGetDbByTableId(pAlter->tableId);
  if (pMsg->pDb == NULL || pMsg->pDb->dirty) {
    mError("table:%s, failed to alter table, db not selected", pAlter->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_DB_NOT_SELECTED);
    return;
  }

  if (mgmtCheckIsMonitorDB(pMsg->pDb->name, tsMonitorDbName)) {
    mError("table:%s, failed to alter table, its log db", pAlter->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_MONITOR_DB_FORBIDDEN);
    return;
  }

  STableInfo *pTable = mgmtGetTable(pAlter->tableId);
  if (pTable == NULL) {
    mError("table:%s, failed to alter table, table not exist", pTable->tableId);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_INVALID_TABLE);
    return;
  }

  pAlter->numOfCols = htons(pAlter->numOfCols);
  if (pAlter->numOfCols > 2) {
    mError("table:%s, error numOfCols:%d in alter table", pAlter->tableId, pAlter->numOfCols);
    mgmtSendSimpleResp(pMsg->thandle, TSDB_CODE_APP_ERROR);
    return;
  }

  for (int32_t i = 0; i < pAlter->numOfCols; ++i) {
    pAlter->schema[i].bytes = htons(pAlter->schema[i].bytes);
  }

  if (pTable->type == TSDB_SUPER_TABLE) {
    mTrace("table:%s, start to alter stable", pAlter->tableId);
    mgmtAlterSuperTable(pMsg, (SSuperTableObj *)pTable);
  } else {
    mTrace("table:%s, start to alter ctable", pAlter->tableId);
    mgmtAlterChildTable(pMsg, (SChildTableObj *)pTable);
  }
}

static void mgmtProcessTableMetaMsg(SQueuedMsg *pMsg) {
  SCMTableInfoMsg *pInfo = pMsg->pCont;
  mTrace("table:%s, table meta msg is received from thandle:%p", pInfo->tableId, pMsg->thandle);

  STableInfo *pTable = mgmtGetTable(pInfo->tableId);
  if (pTable == NULL || pTable->type != TSDB_SUPER_TABLE) {
    mgmtGetChildTableMeta(pMsg, (SChildTableObj *)pTable);
  } else {
    mgmtGetSuperTableMeta(pMsg, (SSuperTableObj *)pTable);
  }
}