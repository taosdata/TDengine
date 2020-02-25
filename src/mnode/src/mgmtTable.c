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
#include "taoserror.h"
#include "taosmsg.h"
#include "tast.h"
#include "textbuffer.h"
#include "tschemautil.h"
#include "tscompression.h"
#include "tskiplist.h"
#include "tsqlfunction.h"
#include "tstatus.h"
#include "ttime.h"
#include "mnode.h"
#include "mgmtAcct.h"
#include "mgmtChildTable.h"
#include "mgmtDb.h"
#include "mgmtDnode.h"
#include "mgmtDnodeInt.h"
#include "mgmtGrant.h"
#include "mgmtNormalTable.h"
#include "mgmtStreamTable.h"
#include "mgmtSuperTable.h"
#include "mgmtTable.h"
#include "mgmtVgroup.h"

int32_t mgmtInitTables() {
  int32_t code = mgmtInitSuperTables();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = mgmtInitNormalTables();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = mgmtInitStreamTables();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = mgmtInitChildTables();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

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

  tableInfo = (STableInfo *) mgmtGetStreamTable(tableId);
  if (tableInfo != NULL) {
    return tableInfo;
  }

  tableInfo = (STableInfo *) mgmtGetNormalTable(tableId);
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

int32_t mgmtGetTableMeta(SDbObj *pDb, STableInfo *pTable, STableMeta *pMeta, bool usePublicIp) {
  if (pTable->type == TSDB_TABLE_TYPE_CHILD_TABLE) {
    mgmtGetChildTableMeta(pDb, (SChildTableObj *) pTable, pMeta, usePublicIp);
  } else if (pTable->type == TSDB_TABLE_TYPE_STREAM_TABLE) {
    mgmtGetStreamTableMeta(pDb, (SStreamTableObj *) pTable, pMeta, usePublicIp);
  } else if (pTable->type == TSDB_TABLE_TYPE_NORMAL_TABLE) {
    mgmtGetNormalTableMeta(pDb, (SNormalTableObj *) pTable, pMeta, usePublicIp);
  } else if (pTable->type == TSDB_TABLE_TYPE_SUPER_TABLE) {
    mgmtGetSuperTableMeta(pDb, (SSuperTableObj *) pTable, pMeta, usePublicIp);
  } else {
    mTrace("%s, uid:%" PRIu64 " table meta retrieve failed, invalid type", pTable->tableId, pTable->uid);
    return TSDB_CODE_INVALID_TABLE;
  }

  mTrace("%s, uid:%" PRIu64 " table meta is retrieved", pTable->tableId, pTable->uid);
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtCreateTable(SDbObj *pDb, SCreateTableMsg *pCreate) {
  STableInfo *pTable = mgmtGetTable(pCreate->tableId);
  if (pTable != NULL) {
    if (pCreate->igExists) {
      return TSDB_CODE_SUCCESS;
    } else {
      return TSDB_CODE_TABLE_ALREADY_EXIST;
    }
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  assert(pAcct != NULL);
  int32_t code = mgmtCheckTableLimit(pAcct, pCreate);
  if (code != 0) {
    mError("table:%s, exceed the limit", pCreate->tableId);
    return code;
  }

  if (mgmtCheckExpired()) {
    mError("failed to create meter:%s, reason:grant expired", pCreate->tableId);
    return TSDB_CODE_GRANT_EXPIRED;
  }

  if (pCreate->numOfTags == 0) {
    int32_t grantCode = mgmtCheckTimeSeries(pCreate->numOfColumns);
    if (grantCode != 0) {
      mError("table:%s, grant expired", pCreate->tableId);
      return grantCode;
    }

    SVgObj *pVgroup = mgmtGetAvailVgroup(pDb);
    if (pVgroup == NULL) {
      return terrno;
    }

    int32_t sid = mgmtAllocateSid(pDb, pVgroup);
    if (sid < 0) {
      return terrno;
    }

    if (pCreate->numOfColumns == 0) {
      return mgmtCreateChildTable(pDb, pCreate, pVgroup, sid);
    } else if (pCreate->sqlLen > 0) {
      return mgmtCreateStreamTable(pDb, pCreate, pVgroup, sid);
    } else {
      return mgmtCreateNormalTable(pDb, pCreate, pVgroup, sid);
    }
  } else {
    return mgmtCreateSuperTable(pDb, pCreate);
  }
}

int32_t mgmtDropTable(SDbObj *pDb, char *tableId, int32_t ignore) {
  STableInfo *pTable = mgmtGetTable(tableId);
  if (pTable == NULL) {
    if (ignore) {
      return TSDB_CODE_SUCCESS;
    } else {
      return TSDB_CODE_INVALID_TABLE;
    }
  }

  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    return TSDB_CODE_MONITOR_DB_FORBIDDEN;
  }

  switch (pTable->type) {
    case TSDB_TABLE_TYPE_SUPER_TABLE:
      return mgmtDropSuperTable(pDb, (SSuperTableObj *) pTable);
    case TSDB_TABLE_TYPE_CHILD_TABLE:
      return mgmtDropChildTable(pDb, (SChildTableObj *) pTable);
    case TSDB_TABLE_TYPE_STREAM_TABLE:
      return mgmtDropStreamTable(pDb, (SStreamTableObj *) pTable);
    case TSDB_TABLE_TYPE_NORMAL_TABLE:
      return mgmtDropNormalTable(pDb, (SNormalTableObj *) pTable);
    default:
      return TSDB_CODE_INVALID_TABLE;
  }
}

int32_t mgmtAlterTable(SDbObj *pDb, SAlterTableMsg *pAlter) {
  STableInfo *pTable = mgmtGetTable(pAlter->tableId);
  if (pTable == NULL) {
    return TSDB_CODE_INVALID_TABLE;
  }

  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    return TSDB_CODE_MONITOR_DB_FORBIDDEN;
  }

  if (pAlter->type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN) {
    if (pTable->type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtAddSuperTableTag((SSuperTableObj *) pTable, pAlter->schema, 1);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN) {
    if (pTable->type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtDropSuperTableTag((SSuperTableObj *) pTable, pAlter->schema[0].name);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN) {
    if (pTable->type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtModifySuperTableTagNameByName((SSuperTableObj *) pTable, pAlter->schema[0].name, pAlter->schema[1].name);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
    if (pTable->type == TSDB_TABLE_TYPE_CHILD_TABLE) {
      return mgmtModifyChildTableTagValueByName((SChildTableObj *) pTable, pAlter->schema[0].name, pAlter->tagVal);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
    if (pTable->type == TSDB_TABLE_TYPE_NORMAL_TABLE) {
      return mgmtAddNormalTableColumn((SNormalTableObj *) pTable, pAlter->schema, 1);
    } else if (pTable->type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtAddSuperTableColumn((SSuperTableObj *) pTable, pAlter->schema, 1);
    } else {}
  } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
    if (pTable->type == TSDB_TABLE_TYPE_NORMAL_TABLE) {
      return mgmtDropNormalTableColumnByName((SNormalTableObj *) pTable, pAlter->schema[0].name);
    } else if (pTable->type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtDropSuperTableColumnByName((SSuperTableObj *) pTable, pAlter->schema[0].name);
    } else {}
  } else {}

  return TSDB_CODE_OPS_NOT_SUPPORT;
}

void mgmtCleanUpMeters() {
  mgmtCleanUpNormalTables();
  mgmtCleanUpStreamTables();
  mgmtCleanUpChildTables();
  mgmtCleanUpSuperTables();
}

int32_t mgmtGetShowTableMeta(STableMeta *pMeta, SShowObj *pShow, void *pConn) {
  SDbObj *pDb = mgmtGetDb(pShow->db);
  if (pDb == NULL) {
    return TSDB_CODE_DB_NOT_SELECTED;
  }

  int32_t cols = 0;
  SSchema *pSchema = tsGetSchema(pMeta);

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
  strcpy(pSchema[cols].name, "stable");
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
  int32_t numOfRows  = 0;
//  int32_t numOfRead  = 0;
//  int32_t cols       = 0;
//  void    *pTable    = NULL;
//  char    *pWrite    = NULL;
//
//  int16_t numOfColumns;
//  int64_t createdTime;
//  char    *tableId;
//  char    *superTableId;
//  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;
//
//  SDbObj *pDb = NULL;
//  if (pConn->pDb != NULL) {
//    pDb = mgmtGetDb(pConn->pDb->name);
//  }
//
//  if (pDb == NULL) {
//    return 0;
//  }
//
//  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
//    if (strcmp(pConn->pUser->user, "root") != 0 && strcmp(pConn->pUser->user, "_root") != 0 &&
//        strcmp(pConn->pUser->user, "monitor") != 0) {
//      return 0;
//    }
//  }
//
//  char prefix[20] = {0};
//  strcpy(prefix, pDb->name);
//  strcat(prefix, TS_PATH_DELIMITER);
//  int32_t prefixLen = strlen(prefix);
//
//  while (numOfRows < rows) {
//    void *pNormalTableNode = sdbFetchRow(tsNormalTableSdb, pShow->pNode, (void **) &pTable);
//    if (pTable != NULL) {
//      SNormalTableObj *pNormalTable = (SNormalTableObj *) pTable;
//      pShow->pNode = pNormalTableNode;
//      tableId      = pNormalTable->tableId;
//      superTableId = NULL;
//      createdTime  = pNormalTable->createdTime;
//      numOfColumns = pNormalTable->numOfColumns;
//    } else {
//      void *pStreamTableNode = sdbFetchRow(tsStreamTableSdb, pShow->pNode, (void **) &pTable);
//      if (pTable != NULL) {
//        SStreamTableObj *pChildTable = (SStreamTableObj *) pTable;
//        pShow->pNode = pStreamTableNode;
//        tableId      = pChildTable->tableId;
//        superTableId = NULL;
//        createdTime  = pChildTable->createdTime;
//        numOfColumns = pChildTable->numOfColumns;
//      } else {
//        void *pChildTableNode = sdbFetchRow(tsChildTableSdb, pShow->pNode, (void **) &pTable);
//        if (pTable != NULL) {
//          SChildTableObj *pChildTable = (SChildTableObj *) pTable;
//          pShow->pNode = pChildTableNode;
//          tableId      = pChildTable->tableId;
//          superTableId = NULL;
//          createdTime  = pChildTable->createdTime;
//          numOfColumns = pChildTable->superTable->numOfColumns;
//        } else {
//          break;
//        }
//      }
//    }
//
//    // not belong to current db
//    if (strncmp(tableId, prefix, prefixLen)) {
//      continue;
//    }
//
//    char meterName[TSDB_TABLE_NAME_LEN] = {0};
//    memset(meterName, 0, tListLen(meterName));
//    numOfRead++;
//
//    // pattern compare for meter name
//    extractTableName(tableId, meterName);
//
//    if (pShow->payloadLen > 0 &&
//        patternMatch(pShow->payload, meterName, TSDB_TABLE_NAME_LEN, &info) != TSDB_PATTERN_MATCH) {
//      continue;
//    }
//
//    cols = 0;
//
//    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
//    strncpy(pWrite, meterName, TSDB_TABLE_NAME_LEN);
//    cols++;
//
//    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
//    *(int64_t *) pWrite = createdTime;
//    cols++;
//
//    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
//    *(int16_t *) pWrite = numOfColumns;
//    cols++;
//
//    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
//    if (superTableId != NULL) {
//      extractTableName(superTableId, pWrite);
//    }
//    cols++;
//
//    numOfRows++;
//  }
//
//  pShow->numOfReads += numOfRead;
//  const int32_t NUM_OF_COLUMNS = 4;
//
//  mgmtVacuumResult(data, NUM_OF_COLUMNS, numOfRows, rows, pShow);

  return numOfRows;
}
