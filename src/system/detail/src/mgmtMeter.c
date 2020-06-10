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

#include "mgmt.h"
#include "mgmtUtil.h"
#include "taosmsg.h"
#include "tast.h"
#include "textbuffer.h"
#include "tschemautil.h"
#include "tscompression.h"
#include "tskiplist.h"
#include "tsqlfunction.h"
#include "ttime.h"
#include "vnodeTagMgmt.h"
#include "vnodeStatus.h"

extern int64_t sdbVersion;

#define mgmtDestroyMeter(pMeter)            \
  do {                                      \
    tfree(pMeter->schema);                  \
    pMeter->pSkipList = tSkipListDestroy((pMeter)->pSkipList); \
    tfree(pMeter);                          \
  } while (0)

enum _Meter_Update_Action {
  METER_UPDATE_TAG_NAME,
  METER_UPDATE_TAG_VALUE,
  METER_UPDATE_TAG_VALUE_COL0,
  METER_UPDATE_NULL,
  MAX_METER_UPDATE_ACTION
};

typedef struct {
  int32_t col;
  int32_t pos;
  SSchema schema;
} SchemaUnit;

typedef struct {
  char     meterId[TSDB_METER_ID_LEN + 1];
  char     type;
  uint32_t cols;
  char     data[];
} SMeterBatchUpdateMsg;

typedef struct {
  char    meterId[TSDB_METER_ID_LEN + 1];
  char    action;
  int32_t dataSize;
  char    data[];
} SMeterUpdateMsg;

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
static void removeMeterFromMetricIndex(STabObj *pMetric, STabObj *pMeter);
static void addMeterIntoMetricIndex(STabObj *pMetric, STabObj *pMeter);
int32_t mgmtMeterDropTagByName(STabObj *pMetric, char *name);
int32_t mgmtMeterModifyTagNameByName(STabObj *pMetric, const char *oname, const char *nname);
int32_t mgmtMeterModifyTagValueByName(STabObj *pMeter, char *tagName, char *nContent);
int32_t mgmtMeterAddColumn(STabObj *pMeter, SSchema schema[], int ncols);
int32_t mgmtMeterDropColumnByName(STabObj *pMeter, const char *name);
static int dropMeterImp(SDbObj *pDb, STabObj * pMeter, SAcctObj *pAcct);
static void dropAllMetersOfMetric(SDbObj *pDb, STabObj * pMetric, SAcctObj *pAcct);

int mgmtCheckMeterLimit(SAcctObj *pAcct, SCreateTableMsg *pCreate);
int mgmtCheckMeterGrant(SCreateTableMsg *pCreate, STabObj * pMeter);

void mgmtMeterActionInit() {
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
  assert(mgmtIsMetric(pMetric) && numOfCols >= 0 && numOfCols <= TSDB_MAX_TAGS + 1);

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
  STabObj *pMeter = (STabObj *)row;
  int      tsize = pMeter->updateEnd - (char *)pMeter;
  memcpy(pMeter, str, tsize);
  pMeter->schema = (char *)realloc(pMeter->schema, pMeter->schemaSize);
  memcpy(pMeter->schema, str + tsize, pMeter->schemaSize);

  if (mgmtMeterCreateFromMetric(pMeter)) {
    pMeter->pTagData = pMeter->schema;
  }

  return NULL;
}

void *mgmtMeterActionDestroy(void *row, char *str, int size, int *ssize) {
  STabObj *pMeter = (STabObj *)row;
  mgmtDestroyMeter(pMeter);
  return NULL;
}

void *mgmtMeterActionInsert(void *row, char *str, int size, int *ssize) {
  STabObj * pMeter = NULL;
  SVgObj *  pVgroup = NULL;
  SDbObj *  pDb = NULL;
  STabObj * pMetric = NULL;
  SAcctObj *pAcct = NULL;

  pMeter = (STabObj *)row;

  if (mgmtIsNormalMeter(pMeter)) {
    pVgroup = mgmtGetVgroup(pMeter->gid.vgId);
    if (pVgroup == NULL) {
      mError("id:%s not in vgroup:%d", pMeter->meterId, pMeter->gid.vgId);
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

  if (mgmtMeterCreateFromMetric(pMeter)) {
    pMeter->pTagData = (char *)pMeter->schema;
    pMetric = mgmtGetMeter(pMeter->pTagData);
    assert(pMetric != NULL);
  }

  if (pMeter->meterType == TSDB_METER_STABLE) {
    pMeter->pSql = (char *)pMeter->schema + sizeof(SSchema) * pMeter->numOfColumns;
  }

  if (mgmtIsNormalMeter(pMeter)) {
    if (pMetric) mgmtAddMeterIntoMetric(pMetric, pMeter);

    if (!sdbMaster) {
      int sid = taosAllocateId(pVgroup->idPool);
      if (sid != pMeter->gid.sid) {
        mError("sid:%d is not matched from the master:%d", sid, pMeter->gid.sid);
        return NULL;
      }
    }

    pAcct->acctInfo.numOfTimeSeries += (pMeter->numOfColumns - 1);
    pVgroup->numOfMeters++;
    pDb->numOfTables++;
    pVgroup->meterList[pMeter->gid.sid] = pMeter;

    if (pVgroup->numOfMeters >= pDb->cfg.maxSessions - 1 && pDb->numOfVgroups > 1) mgmtMoveVgroupToTail(pDb, pVgroup);
  } else {
    // insert a metric
    pMeter->pHead = NULL;
    pMeter->pSkipList = NULL;
    pDb = mgmtGetDbByMeterId(pMeter->meterId);
    if (pDb) {
      mgmtAddMetricIntoDb(pDb, pMeter);
    }
  }

  return NULL;
}

void *mgmtMeterActionDelete(void *row, char *str, int size, int *ssize) {
  STabObj *pMeter = NULL;
  SVgObj * pVgroup = NULL;
  SDbObj * pDb = NULL;
  STabObj *pMetric = NULL;

  pMeter = (STabObj *)row;

  if (mgmtIsNormalMeter(pMeter)) {
    if (pMeter->gid.vgId == 0) {
      return NULL;
    }

    pVgroup = mgmtGetVgroup(pMeter->gid.vgId);
    if (pVgroup == NULL) {
      mError("id:%s not in vgroup:%d", pMeter->meterId, pMeter->gid.vgId);
      return NULL;
    }

    pDb = mgmtGetDb(pVgroup->dbName);
    if (pDb == NULL) {
      mError("vgroup:%d not in DB:%s", pVgroup->vgId, pVgroup->dbName);
      return NULL;
    }
  }

  if (mgmtMeterCreateFromMetric(pMeter)) {
    pMeter->pTagData = (char *)pMeter->schema;
    pMetric = mgmtGetMeter(pMeter->pTagData);
    assert(pMetric != NULL);
  }

  if (mgmtIsNormalMeter(pMeter)) {
    if (pMetric) mgmtRemoveMeterFromMetric(pMetric, pMeter);

    pVgroup->meterList[pMeter->gid.sid] = NULL;
    pVgroup->numOfMeters--;
    pDb->numOfTables--;
    taosFreeId(pVgroup->idPool, pMeter->gid.sid);

    if (pVgroup->numOfMeters > 0) mgmtMoveVgroupToHead(pDb, pVgroup);
  } else {
    // remove a metric
    // remove all the associated meters

    pDb = mgmtGetDbByMeterId(pMeter->meterId);
    if (pDb) mgmtRemoveMetricFromDb(pDb, pMeter);
  }

  return NULL;
}

void *mgmtMeterActionUpdate(void *row, char *str, int size, int *ssize) {
  STabObj *pMeter = NULL;
  STabObj *pMetric = NULL;

  pMeter = (STabObj *)row;
  STabObj *pNew = (STabObj *)str;

  if (pNew->isDirty) {
    pMetric = mgmtGetMeter(pMeter->pTagData);
    removeMeterFromMetricIndex(pMetric, pMeter);
  }
  mgmtMeterActionReset(pMeter, str, size, NULL);
  pMeter->pTagData = pMeter->schema;
  if (pNew->isDirty) {
    addMeterIntoMetricIndex(pMetric, pMeter);
    pMeter->isDirty = 0;
  }

  return NULL;
}

void *mgmtMeterActionEncode(void *row, char *str, int size, int *ssize) {
  assert(row != NULL && str != NULL);

  STabObj *pMeter = (STabObj *)row;
  int      tsize = pMeter->updateEnd - (char *)pMeter;

  if (size < tsize + pMeter->schemaSize + 1) {
    *ssize = -1;
    return NULL;
  }

  memcpy(str, pMeter, tsize);
  memcpy(str + tsize, pMeter->schema, pMeter->schemaSize);

  *ssize = tsize + pMeter->schemaSize;

  return NULL;
}

void *mgmtMeterActionDecode(void *row, char *str, int size, int *ssize) {
  assert(str != NULL);

  STabObj *pMeter = (STabObj *)malloc(sizeof(STabObj));
  if (pMeter == NULL) return NULL;
  memset(pMeter, 0, sizeof(STabObj));

  int tsize = pMeter->updateEnd - (char *)pMeter;
  if (size < tsize) {
    mgmtDestroyMeter(pMeter);
    return NULL;
  }
  memcpy(pMeter, str, tsize);

  pMeter->schema = (char *)malloc(pMeter->schemaSize);
  if (pMeter->schema == NULL) {
    mgmtDestroyMeter(pMeter);
    return NULL;
  }

  memcpy(pMeter->schema, str + tsize, pMeter->schemaSize);
  return (void *)pMeter;
}

void *mgmtMeterActionBeforeBatchUpdate(void *row, char *str, int size, int *ssize) {
  STabObj *pMetric = (STabObj *)row;

  pthread_rwlock_wrlock(&(pMetric->rwLock));

  return NULL;
}

void *mgmtMeterActionBatchUpdate(void *row, char *str, int size, int *ssize) {
  STabObj *             pMeter = (STabObj *)row;
  SMeterBatchUpdateMsg *msg = (SMeterBatchUpdateMsg *)str;

  if (mgmtIsMetric(pMeter)) {
    if (msg->type == SDB_TYPE_INSERT) {  // Insert schema
      uint32_t total_cols = pMeter->numOfColumns + pMeter->numOfTags;
      pMeter->schema = realloc(pMeter->schema, (total_cols + msg->cols) * sizeof(SSchema));
      pMeter->schemaSize = (total_cols + msg->cols) * sizeof(SSchema);
      pMeter->numOfTags += msg->cols;
      memcpy(pMeter->schema + total_cols * sizeof(SSchema), msg->data, msg->cols * sizeof(SSchema));

    } else if (msg->type == SDB_TYPE_DELETE) {  // Delete schema
      // Make sure the order of tag columns
      SchemaUnit *schemaUnit = (SchemaUnit *)(msg->data);
      int         col = schemaUnit->col;
      assert(col > 0 && col < pMeter->numOfTags);
      if (col < pMeter->numOfTags - 1) {
        memmove(pMeter->schema + sizeof(SSchema) * (pMeter->numOfColumns + col),
                pMeter->schema + sizeof(SSchema) * (pMeter->numOfColumns + col + 1),
                pMeter->schemaSize - (sizeof(SSchema) * (pMeter->numOfColumns + col + 1)));
      }
      pMeter->schemaSize -= sizeof(SSchema);
      pMeter->numOfTags--;
      pMeter->schema = realloc(pMeter->schema, pMeter->schemaSize);
    }

    return pMeter->pHead;

  } else if (mgmtMeterCreateFromMetric(pMeter)) {
    if (msg->type == SDB_TYPE_INSERT) {
      SSchema *schemas = (SSchema *)msg->data;
      int      total_size = 0;
      for (int i = 0; i < msg->cols; i++) {
        total_size += schemas[i].bytes;
      }
      pMeter->schema = realloc(pMeter->schema, pMeter->schemaSize + total_size);
      pMeter->pTagData = pMeter->schema;
      memset(pMeter->schema + pMeter->schemaSize, 0, total_size);
      pMeter->schemaSize += total_size;
      // TODO: set the data as default value
    } else if (msg->type == SDB_TYPE_DELETE) {  // Delete values in MTABLEs
      SchemaUnit *schemaUnit = (SchemaUnit *)(msg->data);
      int32_t     pos = schemaUnit->pos;
      int32_t     bytes = schemaUnit->schema.bytes;
      assert(pos + bytes <= pMeter->schemaSize);

      if (pos + bytes != pMeter->schemaSize) {
        memmove(pMeter->schema + pos, pMeter->schema + pos + bytes, pMeter->schemaSize - (pos + bytes));
      }

      pMeter->schemaSize -= bytes;
      pMeter->schema = realloc(pMeter->schema, pMeter->schemaSize);
    }

    return pMeter->next;
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

void mgmtAddMeterStatisticToAcct(STabObj *pMeter, SAcctObj *pAcct) {
  pAcct->acctInfo.numOfTimeSeries += (pMeter->numOfColumns - 1);
}

int mgmtInitMeters() {
  void *    pNode = NULL;
  void *    pLastNode = NULL;
  SVgObj *  pVgroup = NULL;
  STabObj * pMeter = NULL;
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
    pNode = sdbFetchRow(meterSdb, pNode, (void **)&pMeter);
    if (pMeter == NULL) break;
    if (mgmtIsMetric(pMeter)) pMeter->numOfMeters = 0;
  }

  pNode = NULL;
  while (1) {
    pLastNode = pNode;
    pNode = sdbFetchRow(meterSdb, pNode, (void **)&pMeter);
    if (pMeter == NULL) break;

    pDb = mgmtGetDbByMeterId(pMeter->meterId);
    if (pDb == NULL) {
      mError("meter:%s, failed to get db, discard it", pMeter->meterId, pMeter->gid.vgId, pMeter->gid.sid);
      pMeter->gid.vgId = 0;
      sdbDeleteRow(meterSdb, pMeter);
      pNode = pLastNode;
      continue;
    }

    if (mgmtIsNormalMeter(pMeter)) {
      pVgroup = mgmtGetVgroup(pMeter->gid.vgId);

      if (pVgroup == NULL) {
        mError("meter:%s, failed to get vgroup:%d sid:%d, discard it", pMeter->meterId, pMeter->gid.vgId, pMeter->gid.sid);
        pMeter->gid.vgId = 0;
        sdbDeleteRow(meterSdb, pMeter);
        pNode = pLastNode;
        continue;
      }

      if (strcmp(pVgroup->dbName, pDb->name) != 0) {
        mError("meter:%s, db:%s not match with vgroup:%d db:%s sid:%d, discard it",
               pMeter->meterId, pDb->name, pMeter->gid.vgId, pVgroup->dbName, pMeter->gid.sid);
        pMeter->gid.vgId = 0;
        sdbDeleteRow(meterSdb, pMeter);
        pNode = pLastNode;
        continue;
      }

      if ( pVgroup->meterList == NULL) {
        mError("meter:%s, vgroup:%d meterlist is null", pMeter->meterId, pMeter->gid.vgId);
        pMeter->gid.vgId = 0;
        sdbDeleteRow(meterSdb, pMeter);
        pNode = pLastNode;
        continue;
      }

      pVgroup->meterList[pMeter->gid.sid] = pMeter;
      taosIdPoolMarkStatus(pVgroup->idPool, pMeter->gid.sid, 1);

      if (pMeter->meterType == TSDB_METER_STABLE) {
        pMeter->pSql = (char *)pMeter->schema + sizeof(SSchema) * pMeter->numOfColumns;
      }

      if (mgmtMeterCreateFromMetric(pMeter)) {
        pMeter->pTagData = (char *)pMeter->schema;  // + sizeof(SSchema)*pMeter->numOfColumns;
        pMetric = mgmtGetMeter(pMeter->pTagData);
        if (pMetric) mgmtAddMeterIntoMetric(pMetric, pMeter);
      }

      pAcct = mgmtGetAcct(pDb->cfg.acct);
      if (pAcct) mgmtAddMeterStatisticToAcct(pMeter, pAcct);
    } else {
      if (pDb) mgmtAddMetricIntoDb(pDb, pMeter);
    }
  }

  mgmtSetVgroupIdPool();

  mTrace("meter is initialized");
  return 0;
}

STabObj *mgmtGetMeter(char *meterId) { return (STabObj *)sdbGetRow(meterSdb, meterId); }

int mgmtCreateMeter(SDbObj *pDb, SCreateTableMsg *pCreate) {
  STabObj * pMeter = NULL;
  STabObj * pMetric = NULL;
  SVgObj *  pVgroup = NULL;
  int       size = 0;
  SAcctObj *pAcct = NULL;

  int numOfTables = sdbGetNumOfRows(meterSdb);
  if (numOfTables >= tsMaxTables) {
    mError("table:%s, numOfTables:%d exceed maxTables:%d", pCreate->meterId, numOfTables, tsMaxTables);
    return TSDB_CODE_TOO_MANY_TABLES;
  }

  pAcct = mgmtGetAcct(pDb->cfg.acct);
  assert(pAcct != NULL);
  int code = mgmtCheckMeterLimit(pAcct, pCreate);
  if (code != 0) {
    mError("table:%s, exceed the limit", pCreate->meterId);
    return code;
  }

  // does table exist?
  pMeter = mgmtGetMeter(pCreate->meterId);
  if (pMeter) {
    if (pCreate->igExists) {
      return TSDB_CODE_SUCCESS;
    } else {
      return TSDB_CODE_TABLE_ALREADY_EXIST;
    }
  }

  // Create the table object
  pMeter = (STabObj *)malloc(sizeof(STabObj));
  if (pMeter == NULL) return TSDB_CODE_SERV_OUT_OF_MEMORY;
  memset(pMeter, 0, sizeof(STabObj));

  if (pCreate->numOfColumns == 0 && pCreate->numOfTags == 0) {  // MTABLE
    pMeter->meterType = TSDB_METER_MTABLE;
    char *pTagData = (char *)pCreate->schema;  // it is a tag key
    pMetric = mgmtGetMeter(pTagData);
    if (pMetric == NULL) {
      mError("table:%s, corresponding super table does not exist", pCreate->meterId);
      free(pMeter);
      return TSDB_CODE_INVALID_TABLE;
    }

    /*
     * for meters created according to metrics, the schema of this meter isn't needed.
     * so, we don't allocate memory for it in order to save a huge amount of
     * memory when a large amount of meters are created according to this super table.
     */
    size = mgmtGetTagsLength(pMetric, INT_MAX) + (uint32_t)TSDB_METER_ID_LEN;
    pMeter->schema = (char *)malloc(size);
    if (pMeter->schema == NULL) {
      mgmtDestroyMeter(pMeter);
      mError("table:%s, corresponding super table schema is null", pCreate->meterId);
      return TSDB_CODE_INVALID_TABLE;
    }
    memset(pMeter->schema, 0, size);

    pMeter->schemaSize = size;

    pMeter->numOfColumns = pMetric->numOfColumns;
    pMeter->sversion = pMetric->sversion;
    pMeter->pTagData = pMeter->schema;
    pMeter->nextColId = pMetric->nextColId;
    memcpy(pMeter->pTagData, pTagData, size);
  } else {
    int numOfCols = pCreate->numOfColumns + pCreate->numOfTags;
    size = numOfCols * sizeof(SSchema) + pCreate->sqlLen;
    pMeter->schema = (char *)malloc(size);
    if (pMeter->schema == NULL) {
      mgmtDestroyMeter(pMeter);
      mError("table:%s, no schema input", pCreate->meterId);
      return TSDB_CODE_SERV_OUT_OF_MEMORY;
    }
    memset(pMeter->schema, 0, size);

    pMeter->numOfColumns = pCreate->numOfColumns;
    pMeter->sversion = 0;
    pMeter->numOfTags = pCreate->numOfTags;
    pMeter->schemaSize = size;
    memcpy(pMeter->schema, pCreate->schema, numOfCols * sizeof(SSchema));

    for (int k = 0; k < pCreate->numOfColumns; k++) {
      SSchema *tschema = (SSchema *)pMeter->schema;
      tschema[k].colId = pMeter->nextColId++;
    }

    if (pCreate->sqlLen > 0) {
      pMeter->meterType = TSDB_METER_STABLE;
      pMeter->pSql = pMeter->schema + numOfCols * sizeof(SSchema);
      memcpy(pMeter->pSql, (char *)(pCreate->schema) + numOfCols * sizeof(SSchema), pCreate->sqlLen);
      pMeter->pSql[pCreate->sqlLen - 1] = 0;
      mTrace("table:%s, stream sql len:%d sql:%s", pCreate->meterId, pCreate->sqlLen, pMeter->pSql);
    } else {
      if (pCreate->numOfTags > 0) {
        pMeter->meterType = TSDB_METER_METRIC;
      } else {
        pMeter->meterType = TSDB_METER_OTABLE;
      }
    }
  }

  pMeter->createdTime = taosGetTimestampMs();
  strcpy(pMeter->meterId, pCreate->meterId);
  if (pthread_rwlock_init(&pMeter->rwLock, NULL)) {
    mError("table:%s, failed to init meter lock", pCreate->meterId);
    mgmtDestroyMeter(pMeter);
    return TSDB_CODE_FAILED_TO_LOCK_RESOURCES;
  }

  code = mgmtCheckMeterGrant(pCreate, pMeter);
  if (code != 0) {
    mError("table:%s, grant expired", pCreate->meterId);
    return code;
  }

  if (pCreate->numOfTags == 0) {  // handle normal meter creation
    pVgroup = pDb->pHead;

    if (pDb->vgStatus == TSDB_VG_STATUS_IN_PROGRESS) {
      mgmtDestroyMeter(pMeter);
      //mTrace("table:%s, vgroup in creating progress", pCreate->meterId);
      return TSDB_CODE_ACTION_IN_PROGRESS;
    }

    if (pDb->vgStatus == TSDB_VG_STATUS_FULL) {
      mgmtDestroyMeter(pMeter);
      mError("table:%s, vgroup is full", pCreate->meterId);
      return TSDB_CODE_NO_ENOUGH_DNODES;
    }

    if (pDb->vgStatus == TSDB_VG_STATUS_NO_DISK_PERMISSIONS ||
        pDb->vgStatus == TSDB_VG_STATUS_SERVER_NO_PACE ||
        pDb->vgStatus == TSDB_VG_STATUS_SERV_OUT_OF_MEMORY ||
        pDb->vgStatus == TSDB_VG_STATUS_INIT_FAILED ) {
      mgmtDestroyMeter(pMeter);
      mError("table:%s, vgroup init failed, reason:%d %s", pCreate->meterId, pDb->vgStatus, taosGetVgroupStatusStr(pDb->vgStatus));
      return pDb->vgStatus;
    }

    if (pVgroup == NULL) {
      pDb->vgStatus = TSDB_VG_STATUS_IN_PROGRESS;
      mgmtCreateVgroup(pDb);
      mgmtDestroyMeter(pMeter);
      mTrace("table:%s, vgroup malloced, wait for create progress finished", pCreate->meterId);
      return TSDB_CODE_ACTION_IN_PROGRESS;
    }

    int sid = taosAllocateId(pVgroup->idPool);
    if (sid < 0) {
      mWarn("table:%s, vgroup:%d run out of ID, num:%d", pCreate->meterId, pVgroup->vgId, taosIdPoolNumOfUsed(pVgroup->idPool));
      pDb->vgStatus = TSDB_VG_STATUS_IN_PROGRESS;
      mgmtCreateVgroup(pDb);
      mgmtDestroyMeter(pMeter);
      return TSDB_CODE_ACTION_IN_PROGRESS;
    }

    pMeter->gid.sid = sid;
    pMeter->gid.vgId = pVgroup->vgId;
    pMeter->uid = (((uint64_t)pMeter->gid.vgId) << 40) + ((((uint64_t)pMeter->gid.sid) & ((1ul << 24) - 1ul)) << 16) +
                  ((uint64_t)sdbVersion & ((1ul << 16) - 1ul));

    mTrace("table:%s, create table in vgroup, vgId:%d sid:%d vnode:%d uid:%" PRIu64 " db:%s",
           pMeter->meterId, pVgroup->vgId, sid, pVgroup->vnodeGid[0].vnode, pMeter->uid, pDb->name);
  } else {
    pMeter->uid = (((uint64_t)pMeter->createdTime) << 16) + ((uint64_t)sdbVersion & ((1ul << 16) - 1ul));
  }

  if (sdbInsertRow(meterSdb, pMeter, 0) < 0) {
    mError("table:%s, update sdb error", pCreate->meterId);
    return TSDB_CODE_SDB_ERROR;
  }

  // send create message to the selected vnode servers
  if (pCreate->numOfTags == 0) {
    mTrace("table:%s, send create table msg to dnode, vgId:%d, sid:%d, vnode:%d",
           pMeter->meterId, pMeter->gid.vgId, pMeter->gid.sid, pVgroup->vnodeGid[0].vnode);

    grantAddTimeSeries(pMeter->numOfColumns - 1);
    mgmtSendCreateMsgToVgroup(pMeter, pVgroup);
  }

  return 0;
}

int mgmtDropMeter(SDbObj *pDb, char *meterId, int ignore) {
  STabObj * pMeter;
  SAcctObj *pAcct;

  pMeter = mgmtGetMeter(meterId);
  if (pMeter == NULL) {
    if (ignore) {
      return TSDB_CODE_SUCCESS;
    } else {
      return TSDB_CODE_INVALID_TABLE;
    }
  }

  pAcct = mgmtGetAcct(pDb->cfg.acct);

  // 0.log
  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    return TSDB_CODE_MONITOR_DB_FORBEIDDEN;
  }

  if (mgmtIsNormalMeter(pMeter)) {
    return dropMeterImp(pDb, pMeter, pAcct);
  } else {
    // remove a metric
    /*
    if (pMeter->numOfMeters > 0) {
      assert(pMeter->pSkipList != NULL && pMeter->pSkipList->nSize > 0);
      return TSDB_CODE_RELATED_TABLES_EXIST;
    }
    */
    // first delet all meters of metric
    dropAllMetersOfMetric(pDb, pMeter, pAcct);

    // finally delete metric
    sdbDeleteRow(meterSdb, pMeter);
  }

  return 0;
}

int mgmtAlterMeter(SDbObj *pDb, SAlterTableMsg *pAlter) {
  STabObj *pMeter;

  pMeter = mgmtGetMeter(pAlter->meterId);
  if (pMeter == NULL) {
    return TSDB_CODE_INVALID_TABLE;
  }

  // 0.log
  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) return TSDB_CODE_MONITOR_DB_FORBEIDDEN;

  if (pAlter->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
    if (!mgmtIsNormalMeter(pMeter) || !mgmtMeterCreateFromMetric(pMeter)) {
      return TSDB_CODE_OPS_NOT_SUPPORT;
    }
  }

  // todo add
  /* mgmtMeterAddTags */
  if (pAlter->type == TSDB_ALTER_TABLE_ADD_TAG_COLUMN) {
    mTrace("alter table %s to add tag column:%s, type:%d", pMeter->meterId, pAlter->schema[0].name,
           pAlter->schema[0].type);
    return mgmtMeterAddTags(pMeter, pAlter->schema, 1);
  } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_TAG_COLUMN) {
    mTrace("alter table %s to drop tag column:%s", pMeter->meterId, pAlter->schema[0].name);
    return mgmtMeterDropTagByName(pMeter, pAlter->schema[0].name);
  } else if (pAlter->type == TSDB_ALTER_TABLE_CHANGE_TAG_COLUMN) {
    mTrace("alter table %s to change tag column name, old: %s, new: %s", pMeter->meterId, pAlter->schema[0].name,
           pAlter->schema[1].name);
    return mgmtMeterModifyTagNameByName(pMeter, pAlter->schema[0].name, pAlter->schema[1].name);
  } else if (pAlter->type == TSDB_ALTER_TABLE_UPDATE_TAG_VAL) {
    mTrace("alter table %s to modify tag value, tag name:%s", pMeter->meterId, pAlter->schema[0].name);
    return mgmtMeterModifyTagValueByName(pMeter, pAlter->schema[0].name, pAlter->tagVal);
  } else if (pAlter->type == TSDB_ALTER_TABLE_ADD_COLUMN) {
    mTrace("alter table %s to add column:%s, type:%d", pMeter->meterId, pAlter->schema[0].name, pAlter->schema[0].type);
    return mgmtMeterAddColumn(pMeter, pAlter->schema, 1);
  } else if (pAlter->type == TSDB_ALTER_TABLE_DROP_COLUMN) {
    mTrace("alter table %s to drop column:%s", pMeter->meterId, pAlter->schema[0].name);
    return mgmtMeterDropColumnByName(pMeter, pAlter->schema[0].name);
  } else {
    return TSDB_CODE_INVALID_MSG_TYPE;
  }

  return TSDB_CODE_SUCCESS;
}

static int dropMeterImp(SDbObj *pDb, STabObj * pMeter, SAcctObj *pAcct) {
  SVgObj *  pVgroup;

  if (pAcct != NULL) pAcct->acctInfo.numOfTimeSeries -= (pMeter->numOfColumns - 1);
  
  pVgroup = mgmtGetVgroup(pMeter->gid.vgId);
  if (pVgroup == NULL) return TSDB_CODE_OTHERS;

  grantRestoreTimeSeries(pMeter->numOfColumns - 1);
  mgmtSendRemoveMeterMsgToDnode(pMeter, pVgroup);
  sdbDeleteRow(meterSdb, pMeter);

  if (pVgroup->numOfMeters <= 0) mgmtDropVgroup(pDb, pVgroup);

  return 0;
}

static void dropAllMetersOfMetric(SDbObj *pDb, STabObj * pMetric, SAcctObj *pAcct) {
  STabObj * pMeter = NULL;

  while ((pMeter = pMetric->pHead) != NULL) {
    (void)dropMeterImp(pDb, pMeter, pAcct);    
  }
}

/*
 * create key of each meter for skip list, which is generated from first tag
 * column
 */
static void createKeyFromTagValue(STabObj *pMetric, STabObj *pMeter, tSkipListKey *pKey) {
  SSchema *     pTagSchema = (SSchema *)(pMetric->schema + pMetric->numOfColumns * sizeof(SSchema));
  const int16_t KEY_COLUMN_OF_TAGS = 0;

  char *tagVal = pMeter->pTagData + TSDB_METER_ID_LEN;  // tag start position
  *pKey = tSkipListCreateKey(pTagSchema[KEY_COLUMN_OF_TAGS].type, tagVal, pTagSchema[KEY_COLUMN_OF_TAGS].bytes);
}

/*
 * add a meter into a metric's skip list
 */
static void addMeterIntoMetricIndex(STabObj *pMetric, STabObj *pMeter) {
  const int16_t KEY_COLUMN_OF_TAGS = 0;
  SSchema *     pTagSchema = (SSchema *)(pMetric->schema + pMetric->numOfColumns * sizeof(SSchema));

  if (pMetric->pSkipList == NULL) {
    pMetric->pSkipList = tSkipListCreate(MAX_SKIP_LIST_LEVEL, pTagSchema[KEY_COLUMN_OF_TAGS].type,
                    pTagSchema[KEY_COLUMN_OF_TAGS].bytes);
  }

  if (pMetric->pSkipList) {
    tSkipListKey key = {0};
    createKeyFromTagValue(pMetric, pMeter, &key);
    tSkipListPut(pMetric->pSkipList, pMeter, &key, 1);

    tSkipListDestroyKey(&key);
  }
}

static void removeMeterFromMetricIndex(STabObj *pMetric, STabObj *pMeter) {
  if (pMetric->pSkipList == NULL) {
    return;
  }

  tSkipListKey key = {0};
  createKeyFromTagValue(pMetric, pMeter, &key);
  tSkipListNode **pRes = NULL;

  int32_t num = tSkipListGets(pMetric->pSkipList, &key, &pRes);
  for (int32_t i = 0; i < num; ++i) {
    STabObj *pOneMeter = (STabObj *)pRes[i]->pData;
    if (pOneMeter->gid.sid == pMeter->gid.sid && pOneMeter->gid.vgId == pMeter->gid.vgId) {
      assert(pMeter == pOneMeter);
      tSkipListRemoveNode(pMetric->pSkipList, pRes[i]);
    }
  }

  tSkipListDestroyKey(&key);
  if (num != 0) {
    free(pRes);
  }
}

int mgmtAddMeterIntoMetric(STabObj *pMetric, STabObj *pMeter) {
  if (pMeter == NULL || pMetric == NULL) return -1;

  pthread_rwlock_wrlock(&(pMetric->rwLock));
  // add meter into skip list
  pMeter->next = pMetric->pHead;
  pMeter->prev = NULL;

  if (pMetric->pHead) pMetric->pHead->prev = pMeter;

  pMetric->pHead = pMeter;
  pMetric->numOfMeters++;

  addMeterIntoMetricIndex(pMetric, pMeter);

  pthread_rwlock_unlock(&(pMetric->rwLock));

  return 0;
}

int mgmtRemoveMeterFromMetric(STabObj *pMetric, STabObj *pMeter) {
  pthread_rwlock_wrlock(&(pMetric->rwLock));

  if (pMeter->prev) pMeter->prev->next = pMeter->next;

  if (pMeter->next) pMeter->next->prev = pMeter->prev;

  if (pMeter->prev == NULL) pMetric->pHead = pMeter->next;

  pMetric->numOfMeters--;

  removeMeterFromMetricIndex(pMetric, pMeter);

  pthread_rwlock_unlock(&(pMetric->rwLock));

  return 0;
}

void mgmtCleanUpMeters() { sdbCloseTable(meterSdb); }

int mgmtGetMeterMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int cols = 0;

  SDbObj *pDb = NULL;
  if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);

  if (pDb == NULL) return TSDB_CODE_DB_NOT_SELECTED;

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
  for (int i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  //  pShow->numOfRows = sdbGetNumOfRows (meterSdb);
  pShow->numOfRows = pDb->numOfTables;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

SSchema *mgmtGetMeterSchema(STabObj *pMeter) {
  if (pMeter == NULL) {
    return NULL;
  }

  if (!mgmtMeterCreateFromMetric(pMeter)) {
    return (SSchema *)pMeter->schema;
  }

  STabObj *pMetric = mgmtGetMeter(pMeter->pTagData);
  assert(pMetric != NULL);

  return (SSchema *)pMetric->schema;
}

static int32_t mgmtSerializeTagValue(char* pMsg, STabObj* pMeter, int16_t* tagsId, int32_t numOfTags) {
  int32_t offset = 0;
  
  for (int32_t j = 0; j < numOfTags; ++j) {
    if (tagsId[j] == TSDB_TBNAME_COLUMN_INDEX) {  // handle the table name tags
      char name[TSDB_METER_NAME_LEN] = {0};
      extractTableName(pMeter->meterId, name);
      
      memcpy(pMsg + offset, name, TSDB_METER_NAME_LEN);
      offset += TSDB_METER_NAME_LEN;
    } else {
      SSchema s = {0};
      char *  tag = mgmtMeterGetTag(pMeter, tagsId[j], &s);
      
      memcpy(pMsg + offset, tag, (size_t)s.bytes);
      offset += s.bytes;
    }
  }
  
  return offset;
}

/*
 * serialize SVnodeSidList to byte array
 */
static char *mgmtBuildMetricMetaMsg(SConnObj *pConn, STabObj *pMeter, int32_t *ovgId, SVnodeSidList **pList, SMetricMeta *pMeta,
                                    int32_t tagLen, int16_t numOfTags, int16_t *tagsId, int32_t maxNumOfMeters,
                                    char *pMsg) {
  if (pMeter->gid.vgId != *ovgId || ((*pList) != NULL && (*pList)->numOfSids >= maxNumOfMeters)) {
    /*
     * here we construct a new vnode group for 2 reasons
     * 1. the query msg may be larger than 64k,
     * 2. the following meters belong to different vnodes
     */
    (*pList) = (SVnodeSidList *)pMsg;
    (*pList)->numOfSids = 0;
    (*pList)->index = 0;
    pMeta->numOfVnodes++;

    SVgObj *pVgroup = mgmtGetVgroup(pMeter->gid.vgId);
    for (int i = 0; i < TSDB_VNODES_SUPPORT; ++i) {
      if (pConn->usePublicIp) {
        (*pList)->vpeerDesc[i].ip = pVgroup->vnodeGid[i].publicIp;
        (*pList)->vpeerDesc[i].vnode = pVgroup->vnodeGid[i].vnode;
      } else {
        (*pList)->vpeerDesc[i].ip = pVgroup->vnodeGid[i].ip;
        (*pList)->vpeerDesc[i].vnode = pVgroup->vnodeGid[i].vnode;
      }
    }

    pMsg += sizeof(SVnodeSidList);
    (*ovgId) = pMeter->gid.vgId;
  }
  pMeta->numOfMeters++;
  (*pList)->numOfSids++;

  SMeterSidExtInfo *pSMeterTagInfo = (SMeterSidExtInfo *)pMsg;
  pSMeterTagInfo->sid = htonl(pMeter->gid.sid);
  pSMeterTagInfo->uid = htobe64(pMeter->uid);
  
  pMsg += sizeof(SMeterSidExtInfo);

  int32_t offset = mgmtSerializeTagValue(pMsg, pMeter, tagsId, numOfTags);
  assert(offset == tagLen);
  
  pMsg += offset;
  return pMsg;
}

// get total number of vnodes in final result set
static int32_t mgmtGetNumOfVnodesInResult(tQueryResultset *pResult) {
  int32_t numOfVnodes = 0;
  int32_t prevGid = -1;

  for (int32_t i = 0; i < pResult->num; ++i) {
    STabObj *pMeter = pResult->pRes[i];
    if (prevGid == -1) {
      prevGid = pMeter->gid.vgId;
      numOfVnodes++;
    } else if (prevGid != pMeter->gid.vgId) {
      prevGid = pMeter->gid.vgId;
      numOfVnodes++;
    }
  }

  return numOfVnodes;
}

static int32_t mgmtGetMetricMetaMsgSize(tQueryResultset *pResult, int32_t tagLength, int32_t maxMetersPerQuery) {
  int32_t numOfVnodes = mgmtGetNumOfVnodesInResult(pResult);

  int32_t size = (sizeof(SMeterSidExtInfo) + tagLength) * pResult->num +
                 ((pResult->num / maxMetersPerQuery) + 1 + numOfVnodes) * sizeof(SVnodeSidList) + sizeof(SMetricMeta) +
                 1024;

  return size;
}

static SMetricMetaElemMsg *doConvertMetricMetaMsg(SMetricMetaMsg *pMetricMetaMsg, int32_t tableIndex) {
  SMetricMetaElemMsg *pElem = (SMetricMetaElemMsg *)((char *)pMetricMetaMsg + pMetricMetaMsg->metaElem[tableIndex]);

  pElem->orderIndex = htons(pElem->orderIndex);
  pElem->orderType = htons(pElem->orderType);
  pElem->numOfTags = htons(pElem->numOfTags);

  pElem->numOfGroupCols = htons(pElem->numOfGroupCols);
  pElem->condLen = htonl(pElem->condLen);
  pElem->cond = htonl(pElem->cond);

  pElem->elemLen = htons(pElem->elemLen);

  pElem->tableCond = htonl(pElem->tableCond);
  pElem->tableCondLen = htonl(pElem->tableCondLen);

  pElem->rel = htons(pElem->rel);

  for (int32_t i = 0; i < pElem->numOfTags; ++i) {
    pElem->tagCols[i] = htons(pElem->tagCols[i]);
  }

  pElem->groupbyTagColumnList = htonl(pElem->groupbyTagColumnList);

  SColIndexEx *groupColIds = (SColIndexEx*) (((char *)pMetricMetaMsg) + pElem->groupbyTagColumnList);
  for (int32_t i = 0; i < pElem->numOfGroupCols; ++i) {
    groupColIds[i].colId = htons(groupColIds[i].colId);
    groupColIds[i].colIdx = htons(groupColIds[i].colIdx);
    groupColIds[i].flag = htons(groupColIds[i].flag);
    groupColIds[i].colIdxInBuf = 0;
  }

  return pElem;
}

static int32_t mgmtBuildMetricMetaRspMsg(SConnObj *pConn, SMetricMetaMsg *pMetricMetaMsg, tQueryResultset *pResult,
                                         char **pStart, int32_t *tagLen, int32_t rspMsgSize, int32_t maxTablePerVnode,
                                         int32_t code) {
  *pStart = taosBuildRspMsgWithSize(pConn->thandle, TSDB_MSG_TYPE_METRIC_META_RSP, rspMsgSize);
  if (*pStart == NULL) {
    return 0;
  }

  char *    pMsg = (*pStart);
  STaosRsp *pRsp = (STaosRsp *)pMsg;

  pRsp->code = code;
  pMsg += sizeof(STaosRsp);
  *pMsg = TSDB_IE_TYPE_META;
  pMsg++;

  if (code != TSDB_CODE_SUCCESS) {
    return pMsg - (*pStart);  // one bit in payload
  }

  int32_t msgLen = 0;

  *(int16_t *)pMsg = htons(pMetricMetaMsg->numOfMeters);
  pMsg += sizeof(int16_t);

  for (int32_t j = 0; j < pMetricMetaMsg->numOfMeters; ++j) {
    SVnodeSidList *pList = NULL;
    int            ovgId = -1;

    SMetricMeta *pMeta = (SMetricMeta *)pMsg;

    pMeta->numOfMeters = 0;
    pMeta->numOfVnodes = 0;
    pMeta->tagLen = htons((uint16_t)tagLen[j]);

    pMsg = (char *)pMeta + sizeof(SMetricMeta);

    SMetricMetaElemMsg *pElem = (SMetricMetaElemMsg *)((char *)pMetricMetaMsg + pMetricMetaMsg->metaElem[j]);

    for (int32_t i = 0; i < pResult[j].num; ++i) {
      STabObj *pMeter = pResult[j].pRes[i];
      pMsg = mgmtBuildMetricMetaMsg(pConn, pMeter, &ovgId, &pList, pMeta, tagLen[j], pElem->numOfTags, pElem->tagCols,
                                    maxTablePerVnode, pMsg);
    }

    mTrace("metric:%s metric-meta tables:%d, vnode:%d", pElem->meterId, pMeta->numOfMeters, pMeta->numOfVnodes);

    pMeta->numOfMeters = htonl(pMeta->numOfMeters);
    pMeta->numOfVnodes = htonl(pMeta->numOfVnodes);
  }

  msgLen = pMsg - (*pStart);
  mTrace("metric-meta msg size %d", msgLen);

  return msgLen;
}

int mgmtRetrieveMetricMeta(SConnObj *pConn, char **pStart, SMetricMetaMsg *pMetricMetaMsg) {
  /*
   * naive method: Do not limit the maximum number of meters in each
   * vnode(subquery), split the result according to vnodes
   *
   * todo: split the number of vnodes to make sure each vnode has the same
   * number of tables to query, while not break the upper limit of number of vnode queries
   */
  int32_t          maxMetersPerVNodeForQuery = INT32_MAX;
  int              msgLen = 0;
  int              ret = TSDB_CODE_SUCCESS;
  tQueryResultset *result = calloc(1, pMetricMetaMsg->numOfMeters * sizeof(tQueryResultset));
  int32_t *        tagLen = calloc(1, sizeof(int32_t) * pMetricMetaMsg->numOfMeters);

  if (result == NULL || tagLen == NULL) {
    tfree(result);
    tfree(tagLen);
    return -1;
  }

  for (int32_t i = 0; i < pMetricMetaMsg->numOfMeters; ++i) {
    SMetricMetaElemMsg *pElem = doConvertMetricMetaMsg(pMetricMetaMsg, i);
    STabObj *           pMetric = mgmtGetMeter(pElem->meterId);

    if (!mgmtIsMetric(pMetric)) {
      ret = TSDB_CODE_NOT_SUPER_TABLE;
      break;
    }

    tagLen[i] = mgmtGetReqTagsLength(pMetric, (int16_t *)pElem->tagCols, pElem->numOfTags);
  }

#if 0
    //todo: opt for join process
    int64_t num = 0;
    int32_t index = 0;

    for (int32_t i = 0; i < pMetricMetaMsg->numOfMeters; ++i) {
        SMetricMetaElemMsg *pElem = (SMetricMetaElemMsg*) ((char *) pMetricMetaMsg + pMetricMetaMsg->metaElem[i]);
        STabObj *pMetric = mgmtGetMeter(pElem->meterId);

        if (pMetric->pSkipList->nSize > num) {
            index = i;
            num = pMetric->pSkipList->nSize;
        }
    }
#endif

  if (ret == TSDB_CODE_SUCCESS) {
    // todo opt performance
    for (int32_t i = 0; i < pMetricMetaMsg->numOfMeters; ++i) {
      ret = mgmtRetrieveMetersFromMetric(pMetricMetaMsg, i, &result[i]);
    }
  }

  if (ret == TSDB_CODE_SUCCESS) {
    ret = mgmtDoJoin(pMetricMetaMsg, result);
  }

  if (ret == TSDB_CODE_SUCCESS) {
    for (int32_t i = 0; i < pMetricMetaMsg->numOfMeters; ++i) {
      mgmtReorganizeMetersInMetricMeta(pMetricMetaMsg, i, &result[i]);
    }
  }

  if (ret == TSDB_CODE_SUCCESS) {
    for (int32_t i = 0; i < pMetricMetaMsg->numOfMeters; ++i) {
      msgLen += mgmtGetMetricMetaMsgSize(&result[i], tagLen[i], maxMetersPerVNodeForQuery);
    }
  } else {
    msgLen = 512;
  }

  msgLen = mgmtBuildMetricMetaRspMsg(pConn, pMetricMetaMsg, result, pStart, tagLen, msgLen, maxMetersPerVNodeForQuery, ret);

  for (int32_t i = 0; i < pMetricMetaMsg->numOfMeters; ++i) {
    tQueryResultClean(&result[i]);
  }

  free(tagLen);
  free(result);

  return msgLen;
}

int mgmtRetrieveMeters(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int      numOfRows = 0;
  STabObj *pMeter = NULL;
  char *   pWrite;
  int      cols = 0;
  int      prefixLen;
  int      numOfRead = 0;
  char     prefix[20] = {0};

  SDbObj *pDb = NULL;
  if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);

  if (pDb == NULL) return 0;
  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    if (strcmp(pConn->pUser->user, "root") != 0 && strcmp(pConn->pUser->user, "_root") != 0 && strcmp(pConn->pUser->user, "monitor") != 0 ) {
      return 0;
    }
  }

  strcpy(prefix, pDb->name);
  strcat(prefix, TS_PATH_DELIMITER);
  prefixLen = strlen(prefix);

  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;
  char                meterName[TSDB_METER_NAME_LEN] = {0};

  while (numOfRows < rows) {
    pShow->pNode = sdbFetchRow(meterSdb, pShow->pNode, (void **)&pMeter);
    if (pMeter == NULL) break;

    if (mgmtIsMetric(pMeter)) continue;

    // not belong to current db
    if (strncmp(pMeter->meterId, prefix, prefixLen)) continue;

    numOfRead++;
    memset(meterName, 0, tListLen(meterName));

    // pattern compare for meter name
    extractTableName(pMeter->meterId, meterName);

    if (pShow->payloadLen > 0 &&
        patternMatch(pShow->payload, meterName, TSDB_METER_NAME_LEN, &info) != TSDB_PATTERN_MATCH)
      continue;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    strncpy(pWrite, meterName, TSDB_METER_NAME_LEN);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pMeter->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pMeter->numOfColumns;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    if (pMeter->pTagData) {
      extractTableName(pMeter->pTagData, pWrite);
    }
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRead;
  const int32_t NUM_OF_COLUMNS = 4;

  mgmtVacuumResult(data, NUM_OF_COLUMNS, numOfRows, rows, pShow);

  return numOfRows;
}

int mgmtGetMetricMeta(SMeterMeta *pMeta, SShowObj *pShow, SConnObj *pConn) {
  int cols = 0;

  SDbObj *pDb = NULL;
  if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);

  if (pDb == NULL) return TSDB_CODE_DB_NOT_SELECTED;

  SSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = TSDB_METER_NAME_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
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
  for (int i = 1; i < cols; ++i) pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];

  pShow->numOfRows = pDb->numOfMetrics;
  pShow->pNode = pDb->pMetric;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

int mgmtRetrieveMetrics(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int      numOfRows = 0;
  STabObj *pMetric = NULL;
  char *   pWrite;
  int      cols = 0;

  SDbObj *pDb = NULL;
  if (pConn->pDb != NULL) pDb = mgmtGetDb(pConn->pDb->name);

  if (pDb == NULL) return 0;
  if (mgmtCheckIsMonitorDB(pDb->name, tsMonitorDbName)) {
    if (strcmp(pConn->pUser->user, "root") != 0 && strcmp(pConn->pUser->user, "_root") != 0 && strcmp(pConn->pUser->user, "monitor") != 0 ) {
      return 0;
    }
  }

  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  char metricName[TSDB_METER_NAME_LEN] = {0};

  while (numOfRows < rows) {
    pMetric = (STabObj *)pShow->pNode;
    if (pMetric == NULL) break;
    pShow->pNode = (void *)pMetric->next;

    memset(metricName, 0, tListLen(metricName));
    extractTableName(pMetric->meterId, metricName);

    if (pShow->payloadLen > 0 &&
        patternMatch(pShow->payload, metricName, TSDB_METER_NAME_LEN, &info) != TSDB_PATTERN_MATCH)
      continue;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    extractTableName(pMetric->meterId, pWrite);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pMetric->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pMetric->numOfColumns;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pMetric->numOfTags;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pMetric->numOfMeters;
    cols++;

    numOfRows++;
  }

  pShow->numOfReads += numOfRows;
  return numOfRows;
}

int32_t mgmtFindTagCol(STabObj *pMetric, const char *tagName) {
  if (!mgmtIsMetric(pMetric)) return -1;

  SSchema *schema = NULL;

  for (int i = 0; i < pMetric->numOfTags; i++) {
    schema = (SSchema *)(pMetric->schema + (pMetric->numOfColumns + i) * sizeof(SSchema));

    if (strcasecmp(tagName, schema->name) == 0) return i;
  }

  return -1;
}

int32_t mgmtMeterModifyTagNameByCol(STabObj *pMetric, uint32_t col, const char *nname) {
  int rowSize = 0;
  assert(col >= 0);

  uint32_t len = strlen(nname);

  if (pMetric == NULL || (!mgmtIsMetric(pMetric)) || col >= pMetric->numOfTags || len >= TSDB_COL_NAME_LEN ||
      mgmtFindTagCol(pMetric, nname) >= 0)
    return TSDB_CODE_APP_ERROR;

  // update
  SSchema *schema = (SSchema *)(pMetric->schema + (pMetric->numOfColumns + col) * sizeof(SSchema));
  strncpy(schema->name, nname, TSDB_COL_NAME_LEN);

  // Encode string
  int   size = 1 + sizeof(STabObj) + TSDB_MAX_BYTES_PER_ROW;
  char *msg = (char *)malloc(size);
  if (msg == NULL) return TSDB_CODE_APP_ERROR;
  memset(msg, 0, size);

  mgmtMeterActionEncode(pMetric, msg, size, &rowSize);

  int32_t ret = sdbUpdateRow(meterSdb, msg, rowSize, 1);
  tfree(msg);

  if (ret < 0) {
    mError("Failed to modify table %s tag column", pMetric->meterId);
    return TSDB_CODE_APP_ERROR;
  }

  mTrace("Succeed to modify table %s tag column", pMetric->meterId);
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtMeterModifyTagNameByName(STabObj *pMetric, const char *oname, const char *nname) {
  if (pMetric == NULL || (!mgmtIsMetric(pMetric))) return TSDB_CODE_APP_ERROR;

  int index = mgmtFindTagCol(pMetric, oname);
  if (index < 0) {
    // Tag name does not exist
    mError("Failed to modify table %s tag column, oname: %s, nname: %s", pMetric->meterId, oname, nname);
    return TSDB_CODE_INVALID_MSG_TYPE;
  }

  return mgmtMeterModifyTagNameByCol(pMetric, index, nname);
}

int32_t mgmtMeterModifyTagValueByCol(STabObj *pMeter, int col, const char *nContent) {
  int rowSize = 0;
  if (pMeter == NULL || nContent == NULL || (!mgmtMeterCreateFromMetric(pMeter))) return TSDB_CODE_APP_ERROR;

  STabObj *pMetric = mgmtGetMeter(pMeter->pTagData);
  assert(pMetric != NULL);

  if (col < 0 || col > pMetric->numOfTags) return TSDB_CODE_APP_ERROR;

  SSchema *schema = (SSchema *)(pMetric->schema + (pMetric->numOfColumns + col) * sizeof(SSchema));

  if (col == 0) {
    pMeter->isDirty = 1;
    removeMeterFromMetricIndex(pMetric, pMeter);
  }
  memcpy(pMeter->pTagData + mgmtGetTagsLength(pMetric, col) + TSDB_METER_ID_LEN, nContent, schema->bytes);
  if (col == 0) {
    addMeterIntoMetricIndex(pMetric, pMeter);
  }

  // Encode the string
  int   size = sizeof(STabObj) + TSDB_MAX_BYTES_PER_ROW + 1;
  char *msg = (char *)malloc(size);
  if (msg == NULL) {
    mError("failed to allocate message memory while modify tag value");
    return TSDB_CODE_APP_ERROR;
  }
  memset(msg, 0, size);

  mgmtMeterActionEncode(pMeter, msg, size, &rowSize);

  int32_t ret = sdbUpdateRow(meterSdb, msg, rowSize, 1);  // Need callback function
  tfree(msg);

  if (pMeter->isDirty) pMeter->isDirty = 0;

  if (ret < 0) {
    mError("Failed to modify tag column %d of table %s", col, pMeter->meterId);
    return TSDB_CODE_APP_ERROR;
  }

  mTrace("Succeed to modify tag column %d of table %s", col, pMeter->meterId);
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtMeterModifyTagValueByName(STabObj *pMeter, char *tagName, char *nContent) {
  if (pMeter == NULL || tagName == NULL || nContent == NULL || (!mgmtMeterCreateFromMetric(pMeter)))
    return TSDB_CODE_INVALID_MSG_TYPE;

  STabObj *pMetric = mgmtGetMeter(pMeter->pTagData);
  if (pMetric == NULL) return TSDB_CODE_APP_ERROR;

  int col = mgmtFindTagCol(pMetric, tagName);
  if (col < 0) return TSDB_CODE_APP_ERROR;

  return mgmtMeterModifyTagValueByCol(pMeter, col, nContent);
}

int32_t mgmtMeterAddTags(STabObj *pMetric, SSchema schema[], int ntags) {
  if (pMetric == NULL || (!mgmtIsMetric(pMetric))) return TSDB_CODE_INVALID_TABLE;

  if (pMetric->numOfTags + ntags > TSDB_MAX_TAGS) return TSDB_CODE_APP_ERROR;

  // check if schemas have the same name
  for (int i = 1; i < ntags; i++) {
    for (int j = 0; j < i; j++) {
      if (strcasecmp(schema[i].name, schema[j].name) == 0) {
        return TSDB_CODE_APP_ERROR;
      }
    }
  }

  for (int i = 0; i < ntags; i++) {
    if (mgmtFindTagCol(pMetric, schema[i].name) >= 0) {
      return TSDB_CODE_APP_ERROR;
    }
  }

  uint32_t              size = sizeof(SMeterBatchUpdateMsg) + sizeof(SSchema) * ntags;
  SMeterBatchUpdateMsg *msg = (SMeterBatchUpdateMsg *)malloc(size);
  memset(msg, 0, size);

  memcpy(msg->meterId, pMetric->meterId, TSDB_METER_ID_LEN);
  msg->type = SDB_TYPE_INSERT;
  msg->cols = ntags;
  memcpy(msg->data, schema, sizeof(SSchema) * ntags);

  int32_t ret = sdbBatchUpdateRow(meterSdb, msg, size);
  tfree(msg);

  if (ret < 0) {
    mError("Failed to add tag column %s to table %s", schema[0].name, pMetric->meterId);
    return TSDB_CODE_APP_ERROR;
  }

  mTrace("Succeed to add tag column %s to table %s", schema[0].name, pMetric->meterId);
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtMeterDropTagByCol(STabObj *pMetric, int col) {
  if (pMetric == NULL || (!mgmtIsMetric(pMetric)) || col <= 0 || col >= pMetric->numOfTags) return TSDB_CODE_APP_ERROR;

  // Pack message to do batch update
  uint32_t              size = sizeof(SMeterBatchUpdateMsg) + sizeof(SchemaUnit);
  SMeterBatchUpdateMsg *msg = (SMeterBatchUpdateMsg *)malloc(size);
  memset(msg, 0, size);

  memcpy(msg->meterId, pMetric->meterId, TSDB_METER_ID_LEN);
  msg->type = SDB_TYPE_DELETE;  // TODO: what should here be ?
  msg->cols = 1;

  ((SchemaUnit *)(msg->data))->col = col;
  ((SchemaUnit *)(msg->data))->pos = mgmtGetTagsLength(pMetric, col) + TSDB_METER_ID_LEN;
  ((SchemaUnit *)(msg->data))->schema = *(SSchema *)(pMetric->schema + sizeof(SSchema) * (pMetric->numOfColumns + col));

  int32_t ret = sdbBatchUpdateRow(meterSdb, msg, size);
  tfree(msg);

  if (ret < 0) {
    mError("Failed to drop tag column: %d from table: %s", col, pMetric->meterId);
    return TSDB_CODE_APP_ERROR;
  }

  mTrace("Succeed to drop tag column: %d from table: %s", col, pMetric->meterId);
  return TSDB_CODE_SUCCESS;
}

int32_t mgmtMeterDropTagByName(STabObj *pMetric, char *name) {
  if (pMetric == NULL || (!mgmtIsMetric(pMetric))) {
    mTrace("Failed to drop tag name: %s from table: %s", name, pMetric->meterId);
    return TSDB_CODE_INVALID_TABLE;
  }

  int col = mgmtFindTagCol(pMetric, name);

  return mgmtMeterDropTagByCol(pMetric, col);
}

int32_t mgmtFindColumnIndex(STabObj *pMeter, const char *colName) {
  STabObj *pMetric = NULL;
  SSchema *schema = NULL;

  if (pMeter->meterType == TSDB_METER_OTABLE || pMeter->meterType == TSDB_METER_METRIC) {
    schema = (SSchema *)pMeter->schema;
    for (int32_t i = 0; i < pMeter->numOfColumns; i++) {
      if (strcasecmp(schema[i].name, colName) == 0) {
        return i;
      }
    }

  } else if (pMeter->meterType == TSDB_METER_MTABLE) {
    pMetric = mgmtGetMeter(pMeter->pTagData);
    if (pMetric == NULL) {
      mError("MTable not belongs to any metric, meter: %s", pMeter->meterId);
      return -1;
    }
    schema = (SSchema *)pMetric->schema;
    for (int32_t i = 0; i < pMetric->numOfColumns; i++) {
      if (strcasecmp(schema[i].name, colName) == 0) {
        return i;
      }
    }
  }

  return -1;
}

int32_t mgmtMeterAddColumn(STabObj *pMeter, SSchema schema[], int ncols) {
  SAcctObj *pAcct = NULL;
  SDbObj *  pDb = NULL;

  if (pMeter == NULL || pMeter->meterType == TSDB_METER_MTABLE || pMeter->meterType == TSDB_METER_STABLE || ncols <= 0)
    return TSDB_CODE_APP_ERROR;

  // ASSUMPTION: no two tags are the same
  for (int i = 0; i < ncols; i++)
    if (mgmtFindColumnIndex(pMeter, schema[i].name) > 0) return TSDB_CODE_APP_ERROR;

  pDb = mgmtGetDbByMeterId(pMeter->meterId);
  if (pDb == NULL) {
    mError("meter: %s not belongs to any database", pMeter->meterId);
    return TSDB_CODE_APP_ERROR;
  }

  pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("DB: %s not belongs to andy account", pDb->name);
    return TSDB_CODE_APP_ERROR;
  }

  pMeter->schema = realloc(pMeter->schema, pMeter->schemaSize + sizeof(SSchema) * ncols);

  if (pMeter->meterType == TSDB_METER_OTABLE) {
    memcpy(pMeter->schema + pMeter->schemaSize, schema, sizeof(SSchema) * ncols);
  } else if (pMeter->meterType == TSDB_METER_METRIC) {
    memmove(pMeter->schema + sizeof(SSchema) * (pMeter->numOfColumns + ncols),
            pMeter->schema + sizeof(SSchema) * pMeter->numOfColumns, sizeof(SSchema) * pMeter->numOfTags);
    memcpy(pMeter->schema + sizeof(SSchema) * pMeter->numOfColumns, schema, sizeof(SSchema) * ncols);
  }

  SSchema *tschema = (SSchema *)(pMeter->schema + sizeof(SSchema) * pMeter->numOfColumns);
  for (int i = 0; i < ncols; i++) tschema[i].colId = pMeter->nextColId++;

  pMeter->schemaSize += sizeof(SSchema) * ncols;
  pMeter->numOfColumns += ncols;
  pMeter->sversion++;
  if (mgmtIsNormalMeter(pMeter))
    pAcct->acctInfo.numOfTimeSeries += ncols;
  else
    pAcct->acctInfo.numOfTimeSeries += (ncols * pMeter->numOfMeters);
  sdbUpdateRow(meterSdb, pMeter, 0, 1);

  if (pMeter->meterType == TSDB_METER_METRIC) {
    for (STabObj *pObj = pMeter->pHead; pObj != NULL; pObj = pObj->next) {
      pObj->numOfColumns++;
      pObj->nextColId = pMeter->nextColId;
      pObj->sversion = pMeter->sversion;
      sdbUpdateRow(meterSdb, pObj, 0, 1);
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t mgmtMeterDropColumnByName(STabObj *pMeter, const char *name) {
  SAcctObj *pAcct = NULL;
  SDbObj *  pDb = NULL;

  if (pMeter == NULL || pMeter->meterType == TSDB_METER_MTABLE || pMeter->meterType == TSDB_METER_STABLE)
    return TSDB_CODE_APP_ERROR;

  int32_t index = mgmtFindColumnIndex(pMeter, name);
  if (index < 0) return TSDB_CODE_APP_ERROR;

  pDb = mgmtGetDbByMeterId(pMeter->meterId);
  if (pDb == NULL) {
    mError("meter: %s not belongs to any database", pMeter->meterId);
    return TSDB_CODE_APP_ERROR;
  }

  pAcct = mgmtGetAcct(pDb->cfg.acct);
  if (pAcct == NULL) {
    mError("DB: %s not belongs to any account", pDb->name);
    return TSDB_CODE_APP_ERROR;
  }

  if (pMeter->meterType == TSDB_METER_OTABLE) {
    memmove(pMeter->schema + sizeof(SSchema) * index, pMeter->schema + sizeof(SSchema) * (index + 1),
            sizeof(SSchema) * (pMeter->numOfColumns - index - 1));
  } else if (pMeter->meterType == TSDB_METER_METRIC) {
    memmove(pMeter->schema + sizeof(SSchema) * index, pMeter->schema + sizeof(SSchema) * (index + 1),
            sizeof(SSchema) * (pMeter->numOfColumns + pMeter->numOfTags - index - 1));
  }
  pMeter->schemaSize -= sizeof(SSchema);
  pMeter->numOfColumns--;
  if (mgmtIsNormalMeter(pMeter))
    pAcct->acctInfo.numOfTimeSeries--;
  else
    pAcct->acctInfo.numOfTimeSeries -= (pMeter->numOfMeters);

  pMeter->schema = realloc(pMeter->schema, pMeter->schemaSize);
  pMeter->sversion++;
  sdbUpdateRow(meterSdb, pMeter, 0, 1);

  if (pMeter->meterType == TSDB_METER_METRIC) {
    for (STabObj *pObj = pMeter->pHead; pObj != NULL; pObj = pObj->next) {
      pObj->numOfColumns--;
      pObj->sversion = pMeter->sversion;
      sdbUpdateRow(meterSdb, pObj, 0, 1);
    }
  }

  return TSDB_CODE_SUCCESS;
}
