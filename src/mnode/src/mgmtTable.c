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


void *meterSdb = NULL;
void *(*mgmtMeterActionFp[SDB_MAX_ACTION_TYPES])(void *row, char *str, int size, int *ssize);

// Function declaration
void *mgmtMeterActionInsert(void *row, char *str, int size, int *ssize);
void *mgmtMeterActionDelete(void *row, char *str, int size, int *ssize);
void *mgmtMeterActionUpdate(void *row, char *str, int size, int *ssize);
void *mgmtMeterActionEncode(void *row, char *str, int size, int *ssize);
void *mgmtMeterActionDecode(void *row, char *str, int size, int *ssize);
void *mgmtMeterActionBeforeBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtMeterActionBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtMeterActionAfterBatchUpdate(void *row, char *str, int size, int *ssize);
void *mgmtMeterActionReset(void *row, char *str, int size, int *ssize);
void *mgmtMeterActionDestroy(void *row, char *str, int size, int *ssize);
int32_t mgmtMeterAddTags(STabObj *pMetric, SSchema schema[], int ntags);
static void removeMeterFromMetricIndex(STabObj *pMetric, STabObj *pTable);
static void addMeterIntoMetricIndex(STabObj *pMetric, STabObj *pTable);
int32_t mgmtMeterDropTagByName(STabObj *pMetric, char *name);
int32_t mgmtMeterModifyTagNameByName(STabObj *pMetric, const char *oname, const char *nname);
int32_t mgmtMeterModifyTagValueByName(STabObj *pTable, char *tagName, char *nContent);
int32_t mgmtMeterAddColumn(STabObj *pTable, SSchema schema[], int ncols);
int32_t mgmtMeterDropColumnByName(STabObj *pTable, const char *name);
static int dropMeterImp(SDbObj *pDb, STabObj * pTable, SAcctObj *pAcct);
static void dropAllMetersOfMetric(SDbObj *pDb, STabObj * pMetric, SAcctObj *pAcct);

static void mgmtMeterActionInit() {
  mgmtMeterActionFp[SDB_TYPE_INSERT] = mgmtMeterActionInsert;
  mgmtMeterActionFp[SDB_TYPE_DELETE] = mgmtMeterActionDelete;
  mgmtMeterActionFp[SDB_TYPE_UPDATE] = mgmtMeterActionUpdate;
  mgmtMeterActionFp[SDB_TYPE_ENCODE] = mgmtMeterActionEncode;
  mgmtMeterActionFp[SDB_TYPE_DECODE] = mgmtMeterActionDecode;
  mgmtMeterActionFp[SDB_TYPE_BEFORE_BATCH_UPDATE] = mgmtMeterActionBeforeBatchUpdate;
  mgmtMeterActionFp[SDB_TYPE_BATCH_UPDATE] = mgmtMeterActionBatchUpdate;
  mgmtMeterActionFp[SDB_TYPE_AFTER_BATCH_UPDATE] = mgmtMeterActionAfterBatchUpdate;
  mgmtMeterActionFp[SDB_TYPE_RESET] = mgmtMeterActionReset;
  mgmtMeterActionFp[SDB_TYPE_DESTROY] = mgmtMeterActionDestroy;
}

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

void *mgmtMeterActionReset(void *row, char *str, int size, int *ssize) {
  STabObj *pTable = (STabObj *)row;
  int      tsize = pTable->updateEnd - (char *)pTable;
  memcpy(pTable, str, tsize);
  pTable->schema = (char *)realloc(pTable->schema, pTable->schemaSize);
  memcpy(pTable->schema, str + tsize, pTable->schemaSize);

  if (mgmtTableCreateFromSuperTable(pTable)) {
    pTable->pTagData = pTable->schema;
  }

  return NULL;
}

void *mgmtMeterActionDestroy(void *row, char *str, int size, int *ssize) {
  STabObj *pTable = (STabObj *)row;
  mgmtDestroyMeter(pTable);
  return NULL;
}

void *mgmtMeterActionInsert(void *row, char *str, int size, int *ssize) {
  STabObj * pTable = NULL;
  SVgObj *  pVgroup = NULL;
  SDbObj *  pDb = NULL;
  STabObj * pMetric = NULL;
  SAcctObj *pAcct = NULL;

  pTable = (STabObj *)row;

  if (mgmtIsNormalTable(pTable)) {
    pVgroup = mgmtGetVgroup(pTable->gid.vgId);
    if (pVgroup == NULL) {
      mError("id:%s not in vgroup:%d", pTable->meterId, pTable->gid.vgId);
      return NULL;
    }

    pDb = mgmtGetDb(pVgroup->dbName);
    if (pDb == NULL) {
      mError("vgroup:%d not in DB:%s", pVgroup->vgId, pVgroup->dbName);
      return NULL;
    }

    pAcct = mgmtGetAcct(pDb->cfg.acct);
    // TODO : check if account exists.
    if (pAcct == NULL) {
      mError("account not exists");
      return NULL;
    }
  }

  if (mgmtTableCreateFromSuperTable(pTable)) {
    pTable->pTagData = (char *)pTable->schema;
    pMetric = mgmtGetTable(pTable->pTagData);
    assert(pMetric != NULL);
  }

  if (pTable->tableType == TSDB_TABLE_TYPE_STREAM_TABLE) {
    pTable->pSql = (char *)pTable->schema + sizeof(SSchema) * pTable->numOfColumns;
  }

  if (mgmtIsNormalTable(pTable)) {
    if (pMetric) mgmtAddMeterIntoMetric(pMetric, pTable);

    if (!sdbMaster) {
      int sid = taosAllocateId(pVgroup->idPool);
      if (sid != pTable->gid.sid) {
        mError("sid:%d is not matched from the master:%d", sid, pTable->gid.sid);
        return NULL;
      }
    }

    pAcct->acctInfo.numOfTimeSeries += (pTable->numOfColumns - 1);
    pVgroup->numOfMeters++;
    pDb->numOfTables++;
    pVgroup->meterList[pTable->gid.sid] = pTable;

    if (pVgroup->numOfMeters >= pDb->cfg.maxSessions - 1 && pDb->numOfVgroups > 1) mgmtMoveVgroupToTail(pDb, pVgroup);
  } else {
    // insert a metric
    pTable->pHead = NULL;
    pTable->pSkipList = NULL;
    pDb = mgmtGetDbByMeterId(pTable->meterId);
    if (pDb) {
      mgmtAddMetricIntoDb(pDb, pTable);
    }
  }

  return NULL;
}

void *mgmtMeterActionDelete(void *row, char *str, int size, int *ssize) {
  STabObj *pTable = NULL;
  SVgObj * pVgroup = NULL;
  SDbObj * pDb = NULL;
  STabObj *pMetric = NULL;

  pTable = (STabObj *)row;

  if (mgmtIsNormalTable(pTable)) {
    if (pTable->gid.vgId == 0) {
      return NULL;
    }

    pVgroup = mgmtGetVgroup(pTable->gid.vgId);
    if (pVgroup == NULL) {
      mError("id:%s not in vgroup:%d", pTable->meterId, pTable->gid.vgId);
      return NULL;
    }

    pDb = mgmtGetDb(pVgroup->dbName);
    if (pDb == NULL) {
      mError("vgroup:%d not in DB:%s", pVgroup->vgId, pVgroup->dbName);
      return NULL;
    }
  }

  if (mgmtTableCreateFromSuperTable(pTable)) {
    pTable->pTagData = (char *)pTable->schema;
    pMetric = mgmtGetTable(pTable->pTagData);
    assert(pMetric != NULL);
  }

  if (mgmtIsNormalTable(pTable)) {
    if (pMetric) mgmtRemoveMeterFromMetric(pMetric, pTable);

    pVgroup->meterList[pTable->gid.sid] = NULL;
    pVgroup->numOfMeters--;
    pDb->numOfTables--;
    taosFreeId(pVgroup->idPool, pTable->gid.sid);

    if (pVgroup->numOfMeters > 0) mgmtMoveVgroupToHead(pDb, pVgroup);
  } else {
    // remove a metric
    // remove all the associated meters

    pDb = mgmtGetDbByMeterId(pTable->meterId);
    if (pDb) mgmtRemoveMetricFromDb(pDb, pTable);
  }

  return NULL;
}

void *mgmtMeterActionUpdate(void *row, char *str, int size, int *ssize) {
  STabObj *pTable = NULL;
  STabObj *pMetric = NULL;

  pTable = (STabObj *)row;
  STabObj *pNew = (STabObj *)str;

  if (pNew->isDirty) {
    pMetric = mgmtGetTable(pTable->pTagData);
    removeMeterFromMetricIndex(pMetric, pTable);
  }
  mgmtMeterActionReset(pTable, str, size, NULL);
  pTable->pTagData = pTable->schema;
  if (pNew->isDirty) {
    addMeterIntoMetricIndex(pMetric, pTable);
    pTable->isDirty = 0;
  }

  return NULL;
}

void *mgmtMeterActionEncode(void *row, char *str, int size, int *ssize) {
  assert(row != NULL && str != NULL);

  STabObj *pTable = (STabObj *)row;
  int      tsize = pTable->updateEnd - (char *)pTable;

  if (size < tsize + pTable->schemaSize + 1) {
    *ssize = -1;
    return NULL;
  }

  memcpy(str, pTable, tsize);
  memcpy(str + tsize, pTable->schema, pTable->schemaSize);

  *ssize = tsize + pTable->schemaSize;

  return NULL;
}

void *mgmtMeterActionDecode(void *row, char *str, int size, int *ssize) {
  assert(str != NULL);

  STabObj *pTable = (STabObj *)malloc(sizeof(STabObj));
  if (pTable == NULL) return NULL;
  memset(pTable, 0, sizeof(STabObj));

  int tsize = pTable->updateEnd - (char *)pTable;
  if (size < tsize) {
    mgmtDestroyMeter(pTable);
    return NULL;
  }
  memcpy(pTable, str, tsize);

  pTable->schema = (char *)malloc(pTable->schemaSize);
  if (pTable->schema == NULL) {
    mgmtDestroyMeter(pTable);
    return NULL;
  }

  memcpy(pTable->schema, str + tsize, pTable->schemaSize);
  return (void *)pTable;
}

void *mgmtMeterActionBeforeBatchUpdate(void *row, char *str, int size, int *ssize) {
  STabObj *pMetric = (STabObj *)row;

  pthread_rwlock_wrlock(&(pMetric->rwLock));

  return NULL;
}

void *mgmtMeterActionBatchUpdate(void *row, char *str, int size, int *ssize) {
  STabObj *             pTable = (STabObj *)row;
  SMeterBatchUpdateMsg *msg = (SMeterBatchUpdateMsg *)str;

  if (mgmtIsSuperTable(pTable)) {
    if (msg->type == SDB_TYPE_INSERT) {  // Insert schema
      uint32_t total_cols = pTable->numOfColumns + pTable->numOfTags;
      pTable->schema = realloc(pTable->schema, (total_cols + msg->cols) * sizeof(SSchema));
      pTable->schemaSize = (total_cols + msg->cols) * sizeof(SSchema);
      pTable->numOfTags += msg->cols;
      memcpy(pTable->schema + total_cols * sizeof(SSchema), msg->data, msg->cols * sizeof(SSchema));

    } else if (msg->type == SDB_TYPE_DELETE) {  // Delete schema
      // Make sure the order of tag columns
      SchemaUnit *schemaUnit = (SchemaUnit *)(msg->data);
      int         col = schemaUnit->col;
      assert(col > 0 && col < pTable->numOfTags);
      if (col < pTable->numOfTags - 1) {
        memmove(pTable->schema + sizeof(SSchema) * (pTable->numOfColumns + col),
                pTable->schema + sizeof(SSchema) * (pTable->numOfColumns + col + 1),
                pTable->schemaSize - (sizeof(SSchema) * (pTable->numOfColumns + col + 1)));
      }
      pTable->schemaSize -= sizeof(SSchema);
      pTable->numOfTags--;
      pTable->schema = realloc(pTable->schema, pTable->schemaSize);
    }

    return pTable->pHead;

  } else if (mgmtTableCreateFromSuperTable(pTable)) {
    if (msg->type == SDB_TYPE_INSERT) {
      SSchema *schemas = (SSchema *)msg->data;
      int      total_size = 0;
      for (int i = 0; i < msg->cols; i++) {
        total_size += schemas[i].bytes;
      }
      pTable->schema = realloc(pTable->schema, pTable->schemaSize + total_size);
      pTable->pTagData = pTable->schema;
      memset(pTable->schema + pTable->schemaSize, 0, total_size);
      pTable->schemaSize += total_size;
      // TODO: set the data as default value
    } else if (msg->type == SDB_TYPE_DELETE) {  // Delete values in MTABLEs
      SchemaUnit *schemaUnit = (SchemaUnit *)(msg->data);
      int32_t     pos = schemaUnit->pos;
      int32_t     bytes = schemaUnit->schema.bytes;
      assert(pos + bytes <= pTable->schemaSize);

      if (pos + bytes != pTable->schemaSize) {
        memmove(pTable->schema + pos, pTable->schema + pos + bytes, pTable->schemaSize - (pos + bytes));
      }

      pTable->schemaSize -= bytes;
      pTable->schema = realloc(pTable->schema, pTable->schemaSize);
    }

    return pTable->next;
  }

  return NULL;
}

void *mgmtMeterActionAfterBatchUpdate(void *row, char *str, int size, int *ssize) {
  STabObj *pMetric = (STabObj *)row;

  pthread_rwlock_unlock(&(pMetric->rwLock));

  return NULL;
}

void *mgmtMeterAction(char action, void *row, char *str, int size, int *ssize) {
  if (mgmtMeterActionFp[(uint8_t)action] != NULL) {
    return (*(mgmtMeterActionFp[(uint8_t)action]))(row, str, size, ssize);
  }
  return NULL;
}

void mgmtAddMeterStatisticToAcct(STabObj *pTable, SAcctObj *pAcct) {
  pAcct->acctInfo.numOfTimeSeries += (pTable->numOfColumns - 1);
}

int mgmtInitMeters() {
  void *    pNode = NULL;
  void *    pLastNode = NULL;
  SVgObj *  pVgroup = NULL;
  STabObj * pTable = NULL;
  STabObj * pMetric = NULL;
  SDbObj *  pDb = NULL;
  SAcctObj *pAcct = NULL;

  // TODO: Make sure this function only run once
  mgmtMeterActionInit();

  meterSdb = sdbOpenTable(tsMaxTables, sizeof(STabObj) + sizeof(SSchema) * TSDB_MAX_COLUMNS + TSDB_MAX_SQL_LEN,
                          "meters", SDB_KEYTYPE_STRING, mgmtDirectory, mgmtMeterAction);
  if (meterSdb == NULL) {
    mError("failed to init meter data");
    return -1;
  }

  pNode = NULL;
  while (1) {
    pNode = sdbFetchRow(meterSdb, pNode, (void **)&pTable);
    if (pTable == NULL) break;
    if (mgmtIsSuperTable(pTable)) pTable->numOfMeters = 0;
  }

  pNode = NULL;
  while (1) {
    pLastNode = pNode;
    pNode = sdbFetchRow(meterSdb, pNode, (void **)&pTable);
    if (pTable == NULL) break;

    pDb = mgmtGetDbByMeterId(pTable->meterId);
    if (pDb == NULL) {
      mError("meter:%s, failed to get db, discard it", pTable->meterId, pTable->gid.vgId, pTable->gid.sid);
      pTable->gid.vgId = 0;
      sdbDeleteRow(meterSdb, pTable);
      pNode = pLastNode;
      continue;
    }

    if (mgmtIsNormalTable(pTable)) {
      pVgroup = mgmtGetVgroup(pTable->gid.vgId);

      if (pVgroup == NULL) {
        mError("meter:%s, failed to get vgroup:%d sid:%d, discard it", pTable->meterId, pTable->gid.vgId, pTable->gid.sid);
        pTable->gid.vgId = 0;
        sdbDeleteRow(meterSdb, pTable);
        pNode = pLastNode;
        continue;
      }

      if (strcmp(pVgroup->dbName, pDb->name) != 0) {
        mError("meter:%s, db:%s not match with vgroup:%d db:%s sid:%d, discard it",
               pTable->meterId, pDb->name, pTable->gid.vgId, pVgroup->dbName, pTable->gid.sid);
        pTable->gid.vgId = 0;
        sdbDeleteRow(meterSdb, pTable);
        pNode = pLastNode;
        continue;
      }

      if ( pVgroup->meterList == NULL) {
        mError("meter:%s, vgroup:%d meterlist is null", pTable->meterId, pTable->gid.vgId);
        pTable->gid.vgId = 0;
        sdbDeleteRow(meterSdb, pTable);
        pNode = pLastNode;
        continue;
      }

      pVgroup->meterList[pTable->gid.sid] = pTable;
      taosIdPoolMarkStatus(pVgroup->idPool, pTable->gid.sid, 1);

      if (pTable->tableType == TSDB_TABLE_TYPE_STREAM_TABLE) {
        pTable->pSql = (char *)pTable->schema + sizeof(SSchema) * pTable->numOfColumns;
      }

      if (mgmtTableCreateFromSuperTable(pTable)) {
        pTable->pTagData = (char *)pTable->schema;  // + sizeof(SSchema)*pTable->numOfColumns;
        pMetric = mgmtGetTable(pTable->pTagData);
        if (pMetric) mgmtAddMeterIntoMetric(pMetric, pTable);
      }

      pAcct = mgmtGetAcct(pDb->cfg.acct);
      if (pAcct) mgmtAddMeterStatisticToAcct(pTable, pAcct);
    } else {
      if (pDb) mgmtAddMetricIntoDb(pDb, pTable);
    }
  }

  mgmtSetVgroupIdPool();

  mTrace("meter is initialized");
  return 0;
}

STableObj mgmtGetTable(char *tableId) {
  STableObj table = {.type = TSDB_TABLE_TYPE_MAX, .obj = NULL};

  table.obj = mgmtGetSuperTable(tableId);
  if (table.obj != NULL) {
    table.type = TSDB_TABLE_TYPE_SUPER_TABLE;
    return table;
  }

  table.obj = mgmtGetNormalTable(tableId);
  if (table.obj != NULL) {
    table.type = TSDB_TABLE_TYPE_NORMAL_TABLE;
    return table;
  }

  table.obj = mgmtGetStreamTable(tableId);
  if (table.obj != NULL) {
    table.type = TSDB_TABLE_TYPE_STREAM_TABLE;
    return table;
  }

  table.obj = mgmtGetNormalTable(tableId);
  if (table.obj != NULL) {
    table.type = TSDB_TABLE_TYPE_CHILD_TABLE;
    return table;
  }

  return table;
}

int32_t mgmtCreateTable(SDbObj *pDb, SCreateTableMsg *pCreate) {
  STableObj table = mgmtGetTable(pCreate->meterId);
  if (table.obj != NULL) {
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
      return mgmtCreateChildTable(pDb, pCreate, pVgroup->vgId, sid);
    } else if (pCreate->sqlLen > 0) {
      return mgmtCreateStreamTable(pDb, pCreate, pVgroup->vgId, sid);
    } else {
      return mgmtCreateNormalTable(pDb, pCreate, pVgroup->vgId, sid);
    }
  } else {
    return mgmtCreateSuperTable(pDb, pCreate);
  }
}

int mgmtDropTable(SDbObj *pDb, char *tableId, int ignore) {
  STableObj table = mgmtGetTable(tableId);
  if (table.obj == NULL) {
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

  switch (table.type) {
    case TSDB_TABLE_TYPE_SUPER_TABLE:
      return mgmtDropSuperTable(pDb, table.obj);
    case TSDB_TABLE_TYPE_CHILD_TABLE:
      return mgmtDropChildTable(pDb, table.obj);
    case TSDB_TABLE_TYPE_STREAM_TABLE:
      return mgmtDropStreamTable(pDb, table.obj);
    case TSDB_TABLE_TYPE_NORMAL_TABLE:
      return mgmtDropNormalTable(pDb, table.obj);
    default:
      return TSDB_CODE_INVALID_TABLE;
  }
}

int mgmtAlterTable(SDbObj *pDb, SAlterTableMsg *pAlter) {
  STableObj table = mgmtGetTable(tableId);
  if (table.obj == NULL) {
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
    if (table.type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtAddSuperTableTag(table.obj, pAlter->schema, 1);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN) {
    if (table.type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtDropSuperTableTag(table.obj, pAlter->schema[0].name);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN) {
    if (table.type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtModifySuperTableTagNameByName(table.obj, pAlter->schema[0].name, pAlter->schema[1].name);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
    if (table.type == TSDB_TABLE_TYPE_CHILD_TABLE) {
      return mgmtModifyChildTableTagValueByName(table.obj, pAlter->schema[0].name, pAlter->tagVal);
    }
  } else if (pAlter->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
    if (table.type == TSDB_TABLE_TYPE_NORMAL_TABLE) {
      return mgmtAddNormalTableColumn(table.obj, pAlter->schema, 1);
    } else if (table.type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtAddSuperTableColumn(table.obj, pAlter->schema, 1);
    } else {}
  } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
    if (table.type == TSDB_TABLE_TYPE_NORMAL_TABLE) {
      return mgmtDropNormalTableColumnByName(table.obj, pAlter->schema[0].name);
    } else if (table.type == TSDB_TABLE_TYPE_SUPER_TABLE) {
      return mgmtDropSuperTableColumnByName(table.obj, pAlter->schema[0].name);
    } else {}
  } else {}

  return TSDB_CODE_OPS_NOT_SUPPORT;
}

int mgmtAddMeterIntoMetric(STabObj *pMetric, STabObj *pTable) {
  if (pTable == NULL || pMetric == NULL) return -1;

  pthread_rwlock_wrlock(&(pMetric->rwLock));
  // add meter into skip list
  pTable->next = pMetric->pHead;
  pTable->prev = NULL;

  if (pMetric->pHead) pMetric->pHead->prev = pTable;

  pMetric->pHead = pTable;
  pMetric->numOfMeters++;

  addMeterIntoMetricIndex(pMetric, pTable);

  pthread_rwlock_unlock(&(pMetric->rwLock));

  return 0;
}

int mgmtRemoveMeterFromMetric(STabObj *pMetric, STabObj *pTable) {
  pthread_rwlock_wrlock(&(pMetric->rwLock));

  if (pTable->prev) pTable->prev->next = pTable->next;

  if (pTable->next) pTable->next->prev = pTable->prev;

  if (pTable->prev == NULL) pMetric->pHead = pTable->next;

  pMetric->numOfMeters--;

  removeMeterFromMetricIndex(pMetric, pTable);

  pthread_rwlock_unlock(&(pMetric->rwLock));

  return 0;
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
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

int32_t mgmtRetrieveTables(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int32_t  numOfRows = 0;
  STabObj *pTable = NULL;
  char *   pWrite;
  int32_t  cols = 0;
  int32_t  prefixLen;
  int32_t  numOfRead = 0;
  char     prefix[20] = {0};
  int16_t  numOfColumns;
  char *   tableId;
  char *   superTableId;
  int64_t  createdTime;
  void *   pNormalTableNode;
  void *   pChildTableNode;

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

  strcpy(prefix, pDb->name);
  strcat(prefix, TS_PATH_DELIMITER);
  prefixLen = strlen(prefix);

  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;
  char meterName[TSDB_METER_NAME_LEN] = {0};

  while (numOfRows < rows) {
    pNormalTableNode = sdbFetchRow(tsNormalTableSdb, pShow->pNode, (void **) &pTable);
    if (pTable != NULL) {
      pShow->pNode = pNormalTableNode;
      SNormalTableObj *pNormalTable = (SNormalTableObj *) pTable;
      tableId = pNormalTable->tableId;
      superTableId = NULL;
      createdTime = pNormalTable->createdTime;
      numOfColumns = pNormalTable->numOfColumns;
    } else {
      pChildTableNode = sdbFetchRow(tsChildTableSdb, pShow->pNode, (void **) &pTable);
      if (pTable != NULL) {
        pShow->pNode = pChildTableNode;
        SChildTableObj *pChildTable = (SChildTableObj *) pTable;
        tableId = pChildTable->tableId;
        superTableId = NULL;
        createdTime = pChildTable->createdTime;
        numOfColumns = pChildTable->superTable->numOfColumns;
      } else {
        break;
      }
    }

    // not belong to current db
    if (strncmp(tableId, prefix, prefixLen)) {
      continue;
    }

    numOfRead++;
    memset(meterName, 0, tListLen(meterName));

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
    if (pTable->pTagData) {
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

SSchema *mgmtGetTableSchema(STabObj *pTable) {
  if (pTable == NULL) {
    return NULL;
  }

  if (!mgmtTableCreateFromSuperTable(pTable)) {
    return (SSchema *)pTable->schema;
  }

  STabObj *pMetric = mgmtGetTable(pTable->pTagData);
  assert(pMetric != NULL);

  return (SSchema *)pMetric->schema;
}

