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
#include "mgmtStreamtableQuery.h"
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

int32_t mgmtInitStreamTables() {
  return 0;
}

void mgmtCleanUpStreamTables() {
}

int32_t mgmtCreateStreamTable(SDbObj *pDb, SCreateTableMsg *pCreate, int32_t vgId, int32_t sid) {
  int numOfTables = sdbGetNumOfRows(tsStreamTableSdb);
  if (numOfTables >= TSDB_MAX_TABLES) {
    mError("stream table:%s, numOfTables:%d exceed maxTables:%d", pCreate->meterId, numOfTables, TSDB_MAX_TABLES);
    return TSDB_CODE_TOO_MANY_TABLES;
  }

  SStreamTableObj *pTable = (SStreamTableObj *)calloc(sizeof(SStreamTableObj), 1);
  if (pTable == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }

  strcpy(pTable->tableId, pCreate->meterId);
  pTable->createdTime = taosGetTimestampMs();
  pTable->vgId = vgId;
  pTable->sid = sid;
  pTable->uid = (((uint64_t)pTable->createdTime) << 16) + ((uint64_t)sdbGetVersion() & ((1ul << 16) - 1ul));
  pTable->sversion = 0;
  pTable->numOfColumns = pCreate->numOfColumns;

  int numOfCols = pCreate->numOfColumns + pCreate->numOfTags;
  pTable->schemaSize = numOfCols * sizeof(SSchema) + pCreate->sqlLen;
  pTable->schema = (int8_t *)calloc(1, pTable->schemaSize);
  if (pTable->schema == NULL) {
    free(pTable);
    mError("table:%s, no schema input", pCreate->meterId);
    return TSDB_CODE_INVALID_TABLE;
  }
  memcpy(pTable->schema, pCreate->schema, numOfCols * sizeof(SSchema));

  pTable->nextColId = 0;
  for (int col = 0; col < pCreate->numOfColumns; col++) {
    SSchema *tschema = (SSchema *)pTable->schema;
    tschema[col].colId = pTable->nextColId++;
  }

  pTable->pSql = pTable->schema + numOfCols * sizeof(SSchema);
  memcpy(pTable->pSql, (char *)(pCreate->schema) + numOfCols * sizeof(SSchema), pCreate->sqlLen);
  pTable->pSql[pCreate->sqlLen - 1] = 0;
  mTrace("table:%s, stream sql len:%d sql:%s", pCreate->meterId, pCreate->sqlLen, pTable->pSql);

  if (sdbInsertRow(tsStreamTableSdb, pTable, 0) < 0) {
    mError("table:%s, update sdb error", pCreate->meterId);
    return TSDB_CODE_SDB_ERROR;
  }

//  mTrace("table:%s, send create table msg to dnode, vgId:%d, sid:%d, vnode:%d",
//         pTable->meterId, pTable->gid.vgId, pTable->gid.sid, pVgroup->vnodeGid[0].vnode);
//
//  mgmtAddTimeSeries(pTable->numOfColumns - 1);
//  mgmtSendCreateMsgToVgroup(pTable, pVgroup);

  mTrace("table:%s, create table in vgroup, vgId:%d sid:%d vnode:%d uid:%" PRIu64 " db:%s",
         pTable->meterId, pVgroup->vgId, sid, pVgroup->vnodeGid[0].vnode, pTable->uid, pDb->name);

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

SStreamTableObj* mgmtGetStreamTable(char *tableId); {
  return (SStreamTableObj *)sdbGetRow(tsStreamTableSdb, tableId);
}