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

#include "mgmtSuperTable.h"
#include "mgmtChildTable.h"
#include "mgmtNormalTable.h"
#include "mgmtStreamTable.h"

#include "taoserror.h"

extern int64_t sdbVersion;

static int32_t mgmtGetReqTagsLength(STabObj *pMetric, int16_t *cols, int32_t numOfCols) {
  assert(mgmtIsSuperTable(pMetric) && numOfCols >= 0 && numOfCols <= TSDB_MAX_TAGS + 1);

  int32_t len = 0;
  for (int32_t i = 0; i < numOfCols; ++i) {
    assert(cols[i] < pMetric->numOfTags);
    if (cols[i] == -1) {
      len += TSDB_METER_NAME_LEN;
    } else {
      len += ((SSchema *)pMetric->schema)[pMetric->numOfColumns + cols[i]].bytes;
    }
  }

  return len;
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

int mgmtInitMeters() {
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
  return NULL;
}

int32_t mgmtCreateTable(SDbObj *pDb, SCreateTableMsg *pCreate) {
  STableInfo *table = mgmtGetTable(pCreate->meterId);
  if (table != NULL) {
    if (pCreate->igExists) {
      return TSDB_CODE_SUCCESS;
    } else {
      return TSDB_CODE_TABLE_ALREADY_EXIST;
    }
  }

  SAcctObj *pAcct = mgmtGetAcct(pDb->cfg.acct);
  assert(pAcct != NULL);
  int code = mgmtCheckTableLimit(pAcct, pCreate);
  if (code != 0) {
    mError("table:%s, exceed the limit", pCreate->meterId);
    return code;
  }

  if (mgmtCheckExpired()) {
    mError("failed to create meter:%s, reason:grant expired", pCreate->meterId);
    return TSDB_CODE_GRANT_EXPIRED;
  }

  if (pCreate->numOfTags == 0) {
    int grantCode = mgmtCheckTimeSeries(pCreate->numOfColumns);
    if (grantCode != 0) {
      mError("table:%s, grant expired", pCreate->meterId);
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

int mgmtDropTable(SDbObj *pDb, char *tableId, int ignore) {
  STableInfo *table = mgmtGetTable(tableId);
  if (table == NULL) {
    if (ignore) {
      return TSDB_CODE_SUCCESS;
    } else {
      return TSDB_CODE_INVALID_TABLE;
    }
  }

  // 0.log
  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    return TSDB_CODE_MONITOR_DB_FORBIDDEN;
  }

  switch (table->type) {
    case TSDB_TABLE_TYPE_SUPER_TABLE:
      return mgmtDropSuperTable(pDb, table);
    case TSDB_TABLE_TYPE_CHILD_TABLE:
      return mgmtDropChildTable(pDb, table);
    case TSDB_TABLE_TYPE_STREAM_TABLE:
      return mgmtDropStreamTable(pDb, table);
    case TSDB_TABLE_TYPE_NORMAL_TABLE:
      return mgmtDropNormalTable(pDb, table);
    default:
      return TSDB_CODE_INVALID_TABLE;
  }
}

int mgmtAlterTable(SDbObj *pDb, SAlterTableMsg *pAlter) {
  STableInfo *table = mgmtGetTable(pAlter->meterId);
  if (table == NULL) {
    return TSDB_CODE_INVALID_TABLE;
  }

  // 0.log
  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    return TSDB_CODE_MONITOR_DB_FORBIDDEN;
  }

//  if (pAlter->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
//    return mgmtUpdate
//    if (!mgmtIsNormalTable(pTable) || !mgmtTableCreateFromSuperTable(pTable)) {
//      return TSDB_CODE_OPS_NOT_SUPPORT;
//    }
//  }

  // todo add
  /* mgmtMeterAddTags */
  if (pAlter->type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN) {
    if (table->type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtAddSuperTableTag(table, pAlter->schema, 1);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN) {
    if (table->type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtDropSuperTableTag(table, pAlter->schema[0].name);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN) {
    if (table->type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtModifySuperTableTagNameByName(table, pAlter->schema[0].name, pAlter->schema[1].name);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
    if (table->type == TSDB_TABLE_TYPE_CHILD_TABLE) {
      return mgmtModifyChildTableTagValueByName(table, pAlter->schema[0].name, pAlter->tagVal);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
    if (table->type == TSDB_TABLE_TYPE_NORMAL_TABLE) {
      return mgmtAddNormalTableColumn(table, pAlter->schema, 1);
    } else if (table->type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtAddSuperTableColumn(table, pAlter->schema, 1);
    } else {}
  } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
    if (table->type == TSDB_TABLE_TYPE_NORMAL_TABLE) {
      return mgmtDropNormalTableColumnByName(table, pAlter->schema[0].name);
    } else if (table->type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtDropSuperTableColumnByName(table, pAlter->schema[0].name);
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

int32_t mgmtGetTableMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int32_t cols = 0;

  SDbObj *pDb = NULL;
  if (pConn->pDb != NULL) {
    pDb = mgmtGetDb(pConn->pDb->name);
  }

  if (pDb == NULL) {
    return TSDB_CODE_DB_NOT_SELECTED;
  }

  SSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = TSDB_METER_NAME_LEN;
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

  pShow->bytes[cols] = TSDB_METER_NAME_LEN;
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

int32_t mgmtRetrieveTables(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int32_t numOfRows  = 0;
  int32_t numOfRead  = 0;
  int32_t cols       = 0;
  void    *pTable    = NULL;
  char    *pWrite    = NULL;

  int16_t numOfColumns;
  int64_t createdTime;
  char    *tableId;
  char    *superTableId;
  SPatternCompareInfo info  = PATTERN_COMPARE_INFO_INITIALIZER;

  SDbObj *pDb = NULL;
  if (pConn->pDb != NULL) {
    pDb = mgmtGetDb(pConn->pDb->name);
  }

  if (pDb == NULL) {
    return 0;
  }

  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    if (strcmp(pConn->pUser->user, "root") != 0 && strcmp(pConn->pUser->user, "_root") != 0 &&
        strcmp(pConn->pUser->user, "monitor") != 0) {
      return 0;
    }
  }

  char prefix[20] = {0};
  strcpy(prefix, pDb->name);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = strlen(prefix);

  while (numOfRows < rows) {
    void *pNormalTableNode = sdbFetchRow(tsNormalTableSdb, pShow->pNode, (void **) &pTable);
    if (pTable != NULL) {
      SNormalTableObj *pNormalTable = (SNormalTableObj *) pTable;
      pShow->pNode = pNormalTableNode;
      tableId      = pNormalTable->tableId;
      superTableId = NULL;
      createdTime  = pNormalTable->createdTime;
      numOfColumns = pNormalTable->numOfColumns;
    } else {
      void *pStreamTableNode = sdbFetchRow(tsStreamTableSdb, pShow->pNode, (void **) &pTable);
      if (pTable != NULL) {
        SStreamTableObj *pChildTable = (SStreamTableObj *) pTable;
        pShow->pNode = pStreamTableNode;
        tableId      = pChildTable->tableId;
        superTableId = NULL;
        createdTime  = pChildTable->createdTime;
        numOfColumns = pChildTable->numOfColumns;
      } else {
        void *pChildTableNode = sdbFetchRow(tsChildTableSdb, pShow->pNode, (void **) &pTable);
        if (pTable != NULL) {
          SChildTableObj *pChildTable = (SChildTableObj *) pTable;
          pShow->pNode = pChildTableNode;
          tableId      = pChildTable->tableId;
          superTableId = NULL;
          createdTime  = pChildTable->createdTime;
          numOfColumns = pChildTable->superTable->numOfColumns;
        } else {
          break;
        }
      }
    }

    // not belong to current db
    if (strncmp(tableId, prefix, prefixLen)) {
      continue;
    }

    char meterName[TSDB_METER_NAME_LEN] = {0};
    memset(meterName, 0, tListLen(meterName));
    numOfRead++;

    // pattern compare for meter name
    extractTableName(tableId, meterName);

    if (pShow->payloadLen > 0 &&
        patternMatch(pShow->payload, meterName, TSDB_METER_NAME_LEN, &info) != TSDB_PATTERN_MATCH) {
      continue;
    }

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strncpy(pWrite, meterName, TSDB_METER_NAME_LEN);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *) pWrite = createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *) pWrite = numOfColumns;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
//    if (pTable->pTagData) {
//      extractTableName(superTableId, pWrite);
//    }
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRead;
  const int32_t NUM_OF_COLUMNS = 4;

  mgmtVacuumResult(data, NUM_OF_COLUMNS, numOfRows, rows, pShow);

  return numOfRows;
}
