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
#include "mgmtSupertableQuery.h"
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

#include "mgmtChildTable.h"
#include "mgmtSuperTable.h"



int32_t mgmtInitChildTables() {
  return 0;
}

void mgmtCleanUpChildTables() {
}

char *mgmtBuildCreateChildTableMsg(SChildTableObj *pTable, char *pMsg, int vnode) {
  
}

int32_t mgmtCreateChildTable(SDbObj *pDb, SCreateTableMsg *pCreate, int32_t vgId, int32_t sid) {
  int numOfTables = sdbGetNumOfRows(tsChildTableSdb);
  if (numOfTables >= tsMaxTables) {
    mError("child table:%s, numOfTables:%d exceed maxTables:%d", pCreate->meterId, numOfTables, tsMaxTables);
    return TSDB_CODE_TOO_MANY_TABLES;
  }

  char *pTagData = (char *)pCreate->schema;  // it is a tag key
  SSuperTableObj *pSuperTable = mgmtGetSuperTable(pTagData);
  if (pSuperTable == NULL) {
    mError("table:%s, corresponding super table does not exist", pCreate->meterId);
    return TSDB_CODE_INVALID_TABLE;
  }

  SChildTableObj *pTable = (SChildTableObj *)calloc(sizeof(SChildTableObj), 1);
  if (pTable == NULL) {
    return TSDB_CODE_SERV_OUT_OF_MEMORY;
  }
  strcpy(pTable->tableId, pCreate->meterId);
  strcpy(pTable->superTableId, pSuperTable->tableId);
  pTable->createdTime = taosGetTimestampMs();
  pTable->superTable = pSuperTable;
  pTable->vgId = vgId;
  pTable->sid = sid;
  pTable->uid = (((uint64_t) pTable->vgId) << 40) + ((((uint64_t) pTable->sid) & ((1ul << 24) - 1ul)) << 16) +
                ((uint64_t) sdbGetVersion() & ((1ul << 16) - 1ul));

  SVariableMsg tags = {0};
  tags.size = mgmtGetTagsLength(pSuperTable, INT_MAX) + (uint32_t)TSDB_TABLE_ID_LEN;
  tags.data = (char *)calloc(1, tags.size);
  if (tags.data == NULL) {
    free(pTable);
    mError("table:%s, corresponding super table schema is null", pCreate->meterId);
    return TSDB_CODE_INVALID_TABLE;
  }
  memcpy(tags.data, pTagData, tags.size);

  if (sdbInsertRow(tsStreamTableSdb, pTable, 0) < 0) {
    mError("table:%s, update sdb error", pCreate->meterId);
    return TSDB_CODE_SDB_ERROR;
  }

  mgmtAddTimeSeries(pTable->superTable->numOfColumns - 1);
  mgmtSendCreateMsgToVgroup(pTable, pVgroup);

  mTrace("table:%s, create table in vgroup, vgId:%d sid:%d vnode:%d uid:%" PRIu64 " db:%s",
         pTable->tableId, vgId, sid, pVgroup->vnodeGid[0].vnode, pTable->uid, pDb->name);

  return 0;
}

int32_t mgmtDropChildTable(SDbObj *pDb, SChildTableObj *pTable) {
  SVgObj *pVgroup;
  SAcctObj *pAcct;

  pAcct = mgmtGetAcct(pDb->cfg.acct);

  if (pAcct != NULL) {
    pAcct->acctInfo.numOfTimeSeries -= (pTable->superTable->numOfColumns - 1);
  }

  pVgroup = mgmtGetVgroup(pTable->vgId);
  if (pVgroup == NULL) {
    return TSDB_CODE_OTHERS;
  }

  mgmtRestoreTimeSeries(pTable->superTable->numOfColumns - 1);
  mgmtSendRemoveMeterMsgToDnode(pTable, pVgroup);
  sdbDeleteRow(tsChildTableSdb, pTable);

  if (pVgroup->numOfMeters <= 0) {
    mgmtDropVgroup(pDb, pVgroup);
  }

  return 0;
}

SChildTableObj* mgmtGetChildTable(char *tableId) {
  return (SChildTableObj *)sdbGetRow(tsChildTableSdb, tableId);
}

int32_t mgmtModifyChildTableTagValueByName(SChildTableObj *pTable, char *tagName, char *nContent) {
  int col = mgmtFindTagCol(pTable->superTable, tagName);
  if (col < 0 || col > pTable->superTable->numOfTags) {
    return TSDB_CODE_APP_ERROR;
  }

  //TODO send msg to dnode
  mTrace("Succeed to modify tag column %d of table %s", col, pTable->tableId);
  return TSDB_CODE_SUCCESS;

//  int rowSize = 0;
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
//  int   size = sizeof(STabObj) + TSDB_MAX_BYTES_PER_ROW + 1;
//  char *msg = (char *)malloc(size);
//  if (msg == NULL) {
//    mError("failed to allocate message memory while modify tag value");
//    return TSDB_CODE_APP_ERROR;
//  }
//  memset(msg, 0, size);
//
//  mgmtMeterActionEncode(pTable, msg, size, &rowSize);
//
//  int32_t ret = sdbUpdateRow(meterSdb, msg, rowSize, 1);  // Need callback function
//  tfree(msg);
//
//  if (pTable->isDirty) pTable->isDirty = 0;
//
//  if (ret < 0) {
//    mError("Failed to modify tag column %d of table %s", col, pTable->meterId);
//    return TSDB_CODE_APP_ERROR;
//  }
//
//  mTrace("Succeed to modify tag column %d of table %s", col, pTable->meterId);
//  return TSDB_CODE_SUCCESS;
}

