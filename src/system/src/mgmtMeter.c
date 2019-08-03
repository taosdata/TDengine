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

#include <arpa/inet.h>
#include <assert.h>
#include <limits.h>
#include <stdint.h>

#include "mgmt.h"
#include "taosmsg.h"
#include "tast.h"
#include "textbuffer.h"
#include "tschemautil.h"
#include "tscompression.h"
#include "tskiplist.h"
#include "tsqlfunction.h"
#include "ttime.h"
#include "vnodeTagMgmt.h"

extern int64_t sdbVersion;

#define mgmtDestroyMeter(pMeter) \
  do {                           \
    tfree(pMeter->schema);       \
    tSkipListDestroy(&(pMeter->pSkipList));\
    tfree(pMeter);               \
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

static int32_t mgmtGetTagsLength(STabObj *pMetric, int32_t col) {  // length befor column col
  assert(mgmtIsMetric(pMetric) && col >= 0);

  int32_t len = 0;
  for (int32_t i = 0; i < pMetric->numOfTags && i < col; ++i) {
    len += ((SSchema *)pMetric->schema)[pMetric->numOfColumns + i].bytes;
  }

  return len;
}

static int32_t mgmtGetReqTagsLength(STabObj *pMetric, int16_t *cols, int32_t numOfCols) {
  assert(mgmtIsMetric(pMetric) && numOfCols >= 0 && numOfCols <= TSDB_MAX_TAGS);

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

static char *mgmtMeterGetTag(STabObj *pMeter, int32_t col, SSchema *pTagColSchema) {
  if (!mgmtMeterCreateFromMetric(pMeter)) {
    return NULL;
  }

  STabObj *pMetric = mgmtGetMeter(pMeter->pTagData);
  int32_t  offset = mgmtGetTagsLength(pMetric, col) + TSDB_METER_ID_LEN;
  assert(offset > 0);

  if (pTagColSchema != NULL) {
    *pTagColSchema = ((SSchema *)pMetric->schema)[pMetric->numOfColumns + col];
  }

  return (pMeter->pTagData + offset);
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

    pAcct = &acctObj;
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
    if (pMetric) {
      mgmtAddMeterIntoMetric(pMetric, pMeter);
    }

    pAcct->acctInfo.numOfTimeSeries += (pMeter->numOfColumns - 1);
    pVgroup->numOfMeters++;
    pDb->numOfTables++;
    pVgroup->meterList[pMeter->gid.sid] = pMeter;

    if (pVgroup->numOfMeters >= pDb->cfg.maxSessions - 1 && pDb->numOfVgroups > 1) {
      mgmtMoveVgroupToTail(pDb, pVgroup);
    }
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
  if (mgmtMeterActionFp[action] != NULL) {
    return (*(mgmtMeterActionFp[action]))(row, str, size, ssize);
  }
  return NULL;
}

void mgmtAddMeterStatisticToAcct(STabObj *pMeter, SAcctObj *pAcct) {
  pAcct->acctInfo.numOfTimeSeries += (pMeter->numOfColumns - 1);
}

int mgmtInitMeters() {
  void *    pNode = NULL;
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
    pNode = sdbFetchRow(meterSdb, pNode, (void **)&pMeter);
    if (pMeter == NULL) break;

    pDb = mgmtGetDbByMeterId(pMeter->meterId);
    if (pDb == NULL) {
      mError("failed to get db: %s", pMeter->meterId);
      continue;
    }

    if (mgmtIsNormalMeter(pMeter)) {
      pVgroup = mgmtGetVgroup(pMeter->gid.vgId);
      if (pVgroup == NULL || pVgroup->meterList == NULL) {
        mError("failed to get vgroup:%i", pMeter->gid.vgId);
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
        if (pMetric) {
          mgmtAddMeterIntoMetric(pMetric, pMeter);
        }
      }

      pAcct = &acctObj;
      mgmtAddMeterStatisticToAcct(pMeter, pAcct);
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

  int numOfTables = sdbGetNumOfRows(meterSdb);
  if (numOfTables >= tsMaxTables) {
    mWarn("numOfTables:%d, exceed tsMaxTables:%d", numOfTables, tsMaxTables);
    return TSDB_CODE_TOO_MANY_TABLES;
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
      return TSDB_CODE_INVALID_TABLE;
    }

    /*
     * for meters created according to metrics, the schema of this meter isn't
     * needed.
     * so, we don't allocate memory for it in order to save a huge amount of
     * memory when a large amount of meters are created using metrics.
     */
    size = mgmtGetTagsLength(pMetric, INT_MAX) + (uint32_t)TSDB_METER_ID_LEN;
    pMeter->schema = (char *)malloc(size);
    if (pMeter->schema == NULL) {
      mgmtDestroyMeter(pMeter);
      return TSDB_CODE_INVALID_TABLE;
    }
    memset(pMeter->schema, 0, size);

    pMeter->schemaSize = size;

    pMeter->numOfColumns = pMetric->numOfColumns;
    pMeter->sversion = pMetric->sversion;
    pMeter->pTagData = pMeter->schema;  // + pMetric->numOfColumns*sizeof(SSchema);
    pMeter->nextColId = pMetric->nextColId;
    memcpy(pMeter->pTagData, pTagData, size);

  } else {
    int numOfCols = pCreate->numOfColumns + pCreate->numOfTags;
    size = numOfCols * sizeof(SSchema) + pCreate->sqlLen;
    pMeter->schema = (char *)malloc(size);
    if (pMeter->schema == NULL) {
      mgmtDestroyMeter(pMeter);
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
      mTrace("stream sql len:%d, sql:%s", pCreate->sqlLen, pMeter->pSql);
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
    mError("Failed to init meter lock");
    mgmtDestroyMeter(pMeter);
    return TSDB_CODE_OTHERS;
  }

  if (pCreate->numOfTags == 0) {  // handle normal meter creation
    pVgroup = pDb->pHead;

    if (pDb->vgStatus == TSDB_VG_STATUS_IN_PROGRESS) {
      mgmtDestroyMeter(pMeter);
      return TSDB_CODE_ACTION_IN_PROGRESS;
    }

    if (pDb->vgStatus == TSDB_VG_STATUS_FULL) {
      mgmtDestroyMeter(pMeter);
      return TSDB_CODE_NO_ENOUGH_PNODES;
    }

    if (pDb->vgStatus == TSDB_VG_STATUS_COMMITLOG_INIT_FAILED) {
      mgmtDestroyMeter(pMeter);
      return TSDB_CODE_VG_COMMITLOG_INIT_FAILED;
    }

    if (pDb->vgStatus == TSDB_VG_STATUS_INIT_FAILED) {
      mgmtDestroyMeter(pMeter);
      return TSDB_CODE_VG_INIT_FAILED;
    }

    if (pVgroup == NULL) {
      pDb->vgStatus = TSDB_VG_STATUS_IN_PROGRESS;
      mgmtCreateVgroup(pDb);
      mgmtDestroyMeter(pMeter);
      return TSDB_CODE_ACTION_IN_PROGRESS;
    }

    int sid = taosAllocateId(pVgroup->idPool);
    if (sid < 0) {
      mWarn("db:%s, vgroup:%d, run out of ID, num:%d", pDb->name, pVgroup->vgId, taosIdPoolNumOfUsed(pVgroup->idPool));
      pDb->vgStatus = TSDB_VG_STATUS_IN_PROGRESS;
      mgmtCreateVgroup(pDb);
      mgmtDestroyMeter(pMeter);
      return TSDB_CODE_ACTION_IN_PROGRESS;
    }

    pMeter->gid.sid = sid;
    pMeter->gid.vgId = pVgroup->vgId;
    pMeter->uid = (((uint64_t)pMeter->gid.vgId) << 40) + ((((uint64_t)pMeter->gid.sid) & ((1ul << 24) - 1ul)) << 16) +
                  ((uint64_t)sdbVersion & ((1ul << 16) - 1ul));
  } else {
    pMeter->uid = (((uint64_t)pMeter->createdTime) << 16) + ((uint64_t)sdbVersion & ((1ul << 16) - 1ul));
  }

  if (sdbInsertRow(meterSdb, pMeter, 0) < 0) {
    return TSDB_CODE_SDB_ERROR;
  }

  // send create message to the selected vnode servers
  if (pCreate->numOfTags == 0) {
    mgmtSendCreateMsgToVnode(pMeter, pVgroup->vnodeGid[0].vnode);
  }

  return 0;
}

int mgmtDropMeter(SDbObj *pDb, char *meterId, int ignore) {
  STabObj * pMeter;
  SVgObj *  pVgroup;
  SAcctObj *pAcct;

  pMeter = mgmtGetMeter(meterId);
  if (pMeter == NULL) {
    if (ignore) {
      return TSDB_CODE_SUCCESS;
    } else {
      return TSDB_CODE_INVALID_TABLE;
    }
  }

  pAcct = &acctObj;

  // 0.sys
  if (taosCheckDbName(pDb->name, tsMonitorDbName)) return TSDB_CODE_MONITOR_DB_FORBEIDDEN;

  if (mgmtIsNormalMeter(pMeter)) {
    if (pAcct != NULL) pAcct->acctInfo.numOfTimeSeries -= (pMeter->numOfColumns - 1);
    pVgroup = mgmtGetVgroup(pMeter->gid.vgId);
    if (pVgroup == NULL) {
      return TSDB_CODE_OTHERS;
    }

    mgmtSendRemoveMeterMsgToVnode(pMeter, pVgroup->vnodeGid[0].vnode);
    sdbDeleteRow(meterSdb, pMeter);

    if (pVgroup->numOfMeters <= 0) mgmtDropVgroup(pDb, pVgroup);
  } else {
    // remove a metric
    if (pMeter->numOfMeters > 0) {
      assert(pMeter->pSkipList != NULL && pMeter->pSkipList->nSize > 0);
      return TSDB_CODE_RELATED_TABLES_EXIST;
    }
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

  // 0.sys
  if (taosCheckDbName(pDb->name, tsMonitorDbName)) {
    return TSDB_CODE_MONITOR_DB_FORBEIDDEN;
  }

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
    tSkipListCreate(&pMetric->pSkipList, MAX_SKIP_LIST_LEVEL, pTagSchema[KEY_COLUMN_OF_TAGS].type,
                    pTagSchema[KEY_COLUMN_OF_TAGS].bytes, tSkipListDefaultCompare);
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

  if (pConn->pDb == NULL) return TSDB_CODE_DB_NOT_SELECTED;

  SSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = TSDB_METER_NAME_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "table_name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "created time");
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
  pShow->numOfRows = pConn->pDb->numOfTables;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

static int32_t tabObjVGIDComparator(const void *pLeft, const void *pRight) {
  STabObj *p1 = *(STabObj **)pLeft;
  STabObj *p2 = *(STabObj **)pRight;

  int32_t ret = p1->gid.vgId - p2->gid.vgId;
  if (ret == 0) {
    return ret;
  } else {
    return ret > 0 ? 1 : -1;
  }
}

/*
 * qsort comparator
 * sort the result to ensure meters with the same gid is grouped together
 */
static int32_t nodeVGIDComparator(const void *pLeft, const void *pRight) {
  tSkipListNode *p1 = *((tSkipListNode **)pLeft);
  tSkipListNode *p2 = *((tSkipListNode **)pRight);

  return tabObjVGIDComparator(&p1->pData, &p2->pData);
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

/*
 * serialize SVnodeSidList to byte array
 */
static char *mgmtBuildMetricMetaMsg(STabObj *pMeter, int32_t *ovgId, SVnodeSidList **pList, SMetricMeta *pMeta,
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
    (*pList)->vpeerDesc[0].vnode = pVgroup->vnodeGid[0].vnode;

    pMsg += sizeof(SVnodeSidList);
    (*ovgId) = pMeter->gid.vgId;
  }
  pMeta->numOfMeters++;
  (*pList)->numOfSids++;

  SMeterSidExtInfo *pSMeterTagInfo = (SMeterSidExtInfo *)pMsg;
  pSMeterTagInfo->sid = pMeter->gid.sid;
  pMsg += sizeof(SMeterSidExtInfo);

  int32_t offset = 0;
  for (int32_t j = 0; j < numOfTags; ++j) {
    if (tagsId[j] == -1) {
      char name[TSDB_METER_NAME_LEN] = {0};
      extractMeterName(pMeter->meterId, name);

      memcpy(pMsg + offset, name, TSDB_METER_NAME_LEN);
      offset += TSDB_METER_NAME_LEN;
    } else {
      SSchema s = {0};
      char *  tag = mgmtMeterGetTag(pMeter, tagsId[j], &s);

      memcpy(pMsg + offset, tag, (size_t)s.bytes);
      offset += s.bytes;
    }
  }

  pMsg += offset;
  assert(offset == tagLen);

  return pMsg;
}

static STabObj *mgmtGetResultPayload(tQueryResultset *pRes, int32_t index) {
  if (index < 0 || index >= pRes->num) {
    return NULL;
  }

  if (pRes->nodeType == TAST_NODE_TYPE_INDEX_ENTRY) {
    return (STabObj *)((tSkipListNode *)pRes->pRes[index])->pData;
  } else {
    return (STabObj *)pRes->pRes[index];
  }
}

// get total number of vnodes in final result set
static int32_t mgmtGetNumOfVnodesInResult(tQueryResultset *pResult) {
  int32_t numOfVnodes = 0;
  int32_t prevGid = -1;

  for (int32_t i = 0; i < pResult->num; ++i) {
    STabObj *pMeter = mgmtGetResultPayload(pResult, i);
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

static void mgmtRetrieveMetersFromIDs(tQueryResultset *pRes, char *queryStr, char *origins, STabObj *pMetric) {
  char *  sep = ",";
  char *  pToken = NULL;
  int32_t s = 4;

  pRes->pRes = malloc(sizeof(char *) * s);
  pRes->nodeType = TAST_NODE_TYPE_METER_PTR;
  pRes->num = 0;

  for (pToken = strsep(&queryStr, sep); pToken != NULL; pToken = strsep(&queryStr, sep)) {
    STabObj *pMeterObj = mgmtGetMeter(pToken);
    if (pMeterObj == NULL) {
      mWarn("metric:%s error in metric query expression:%s, invalid meter id:%s", pMetric->meterId, origins, pToken);
      continue;
    } else {
      /* double the old size */
      if (pRes->num >= s) {
        s *= 2;
        pRes->pRes = realloc(pRes->pRes, sizeof(char *) * s);
      }

      /* not a table created from metric, ignore */
      if (pMeterObj->meterType != TSDB_METER_MTABLE) {
        continue;
      }

      /* queried meter not belongs to this metric, ignore */
      if (mgmtGetMeter(pMeterObj->pTagData)->uid != pMetric->uid) {
        continue;
      }

      pRes->pRes[pRes->num++] = pMeterObj;
    }
  }
}

static int32_t tabObjResultComparator(const void *p1, const void *p2, void *param) {
  tOrderDescriptor *pOrderDesc = (tOrderDescriptor *)param;

  STabObj *pNode1 = (STabObj *)p1;
  STabObj *pNode2 = (STabObj *)p2;

  for (int32_t i = 0; i < pOrderDesc->orderIdx.numOfOrderedCols; ++i) {
    int32_t colIdx = pOrderDesc->orderIdx.pData[i];

    char *f1 = NULL;
    char *f2 = NULL;

    SSchema schema = {0};

    if (colIdx == -1) {
      f1 = pNode1->meterId;
      f2 = pNode2->meterId;
      schema.type = TSDB_DATA_TYPE_BINARY;
      schema.bytes = TSDB_METER_ID_LEN;
    } else {
      f1 = mgmtMeterGetTag(pNode1, colIdx, NULL);
      f2 = mgmtMeterGetTag(pNode2, colIdx, &schema);
      assert(schema.type == pOrderDesc->pTagSchema->pSchema[colIdx].type);
    }

    int32_t ret = doCompare(f1, f2, schema.type, schema.bytes);
    if (ret == 0) {
      continue;
    } else {
      return ret;
    }
  }

  return 0;
}

static int32_t nodeResultComparator(const void *p1, const void *p2, void *param) {
  STabObj *pNode1 = (STabObj *)((tSkipListNode *)p1)->pData;
  STabObj *pNode2 = (STabObj *)((tSkipListNode *)p2)->pData;

  return tabObjResultComparator(pNode1, pNode2, param);
}

// todo merge sort function with losertree used
static void mgmtReorganizeMetersInMetricMeta(STabObj *pMetric, SMetricMetaMsg *pInfo, SSchema *pTagSchema,
                                             tQueryResultset *pRes) {
  /* no result, no need to pagination */
  if (pRes->num <= 0) {
    return;
  }

  /*
   * To apply the group limitation and group offset, we should sort the result
   * list according to the
   * order condition
   */
  tOrderDescriptor *descriptor =
      (tOrderDescriptor *)calloc(1, sizeof(tOrderDescriptor) + sizeof(int32_t) * pInfo->numOfGroupbyCols);
  descriptor->pTagSchema = tCreateTagSchema(pTagSchema, pMetric->numOfTags);
  descriptor->orderIdx.numOfOrderedCols = pInfo->numOfGroupbyCols;

  int32_t *startPos = NULL;
  int32_t  numOfSubset = 1;

  if (pInfo->numOfGroupbyCols > 0) {
    memcpy(descriptor->orderIdx.pData, (int16_t *)pInfo->groupbyTagIds, sizeof(int16_t) * pInfo->numOfGroupbyCols);
    // sort results list
    __ext_compar_fn_t comparFn =
        (pRes->nodeType == TAST_NODE_TYPE_METER_PTR) ? tabObjResultComparator : nodeResultComparator;

    tQSortEx(pRes->pRes, POINTER_BYTES, 0, pRes->num - 1, descriptor, comparFn);
    startPos = calculateSubGroup(pRes->pRes, pRes->num, &numOfSubset, descriptor, comparFn);
  } else {
    startPos = malloc(2 * sizeof(int32_t));

    startPos[0] = 0;
    startPos[1] = (int32_t)pRes->num;
  }

  /*
   * sort the result according to vgid to ensure meters with the same vgid is
   * continuous in the result list
   */
  __compar_fn_t functor = (pRes->nodeType == TAST_NODE_TYPE_METER_PTR) ? tabObjVGIDComparator : nodeVGIDComparator;
  qsort(pRes->pRes, (size_t) pRes->num, POINTER_BYTES, functor);

  free(descriptor->pTagSchema);
  free(descriptor);
  free(startPos);
}

static char *getTagValueFromMeter(STabObj *pMeter, int32_t offset, void *param) {
  if (offset == -1) {
    extractMeterName(pMeter->meterId, param);
    return param;
  } else {
    char *tags = pMeter->pTagData + TSDB_METER_ID_LEN;  // tag start position
    return (tags + offset);
  }
}

// todo refactor
bool tSQLElemFilterCallback(tSkipListNode *pNode, void *param) {
  tQueryInfo *pCols = (tQueryInfo *)param;
  STabObj *   pMeter = (STabObj *)pNode->pData;

  char   name[TSDB_METER_NAME_LEN + 1] = {0};
  char * val = getTagValueFromMeter(pMeter, pCols->offset, name);
  int8_t type = (pCols->pSchema[pCols->colIdx].type);

  int32_t ret = 0;
  if (pCols->q.nType == TSDB_DATA_TYPE_BINARY || pCols->q.nType == TSDB_DATA_TYPE_NCHAR) {
    ret = pCols->comparator(val, pCols->q.pz);
  } else {
    tVariant v = {0};
    switch (type) {
      case TSDB_DATA_TYPE_INT:
        v.i64Key = *(int32_t *)val;
        break;
      case TSDB_DATA_TYPE_BIGINT:
        v.i64Key = *(int64_t *)val;
        break;
      case TSDB_DATA_TYPE_TINYINT:
        v.i64Key = *(int8_t *)val;
        break;
      case TSDB_DATA_TYPE_SMALLINT:
        v.i64Key = *(int16_t *)val;
        break;
      case TSDB_DATA_TYPE_DOUBLE:
        v.dKey = *(double *)val;
        break;
      case TSDB_DATA_TYPE_FLOAT:
        v.dKey = *(float *)val;
        break;
      case TSDB_DATA_TYPE_BOOL:
        v.i64Key = *(int8_t *)val;
        break;
    }
    ret = pCols->comparator(&v.i64Key, &pCols->q.i64Key);
  }

  switch (pCols->optr) {
    case TSDB_RELATION_EQUAL: {
      return ret == 0;
    }
    case TSDB_RELATION_NOT_EQUAL: {
      return ret != 0;
    }
    case TSDB_RELATION_LARGE_EQUAL: {
      return ret >= 0;
    }
    case TSDB_RELATION_LARGE: {
      return ret > 0;
    }
    case TSDB_RELATION_LESS_EQUAL: {
      return ret <= 0;
    }
    case TSDB_RELATION_LESS: {
      return ret < 0;
    }
    case TSDB_RELATION_LIKE: {
      return ret == 0;
    }

    default:
      assert(false);
  }
  return true;
}

int mgmtRetrieveMetersFromMetric(STabObj *pMetric, SMetricMetaMsg *pInfo, tQueryResultset *pRes) {
  /* no table created in accordance with this metric. */
  if (pMetric->pSkipList == NULL || pMetric->pSkipList->nSize == 0) {
    assert(pMetric->numOfMeters == 0);
    return TSDB_CODE_SUCCESS;
  }

  char *  pQueryCond = pInfo->tags;
  int32_t queryCondLength = pInfo->condLength;

  tSQLBinaryExpr *pExpr = NULL;
  SSchema *       pTagSchema = (SSchema *)(pMetric->schema + pMetric->numOfColumns * sizeof(SSchema));

  char *queryStr = calloc(1, (queryCondLength + 1) * TSDB_NCHAR_SIZE);
  if (queryCondLength > 0) {
    /* transfer the unicode string to mbs binary expression */
    taosUcs4ToMbs(pQueryCond, queryCondLength * TSDB_NCHAR_SIZE, queryStr);
    queryCondLength = strlen(queryStr) + 1;

    mTrace("metric:%s len:%d, type:%d, metric query condition:%s", pMetric->meterId, queryCondLength, pInfo->type, queryStr);
  } else {
    mTrace("metric:%s, retrieve all meter, no query condition", pMetric->meterId);
  }

  if (queryCondLength > 0) {
    if (pInfo->type == TSQL_STABLE_QTYPE_SET) {
      char *oldStr = strdup(queryStr);
      mgmtRetrieveMetersFromIDs(pRes, queryStr, oldStr, pMetric);
      tfree(oldStr);
    } else {
      tSQLBinaryExprFromString(&pExpr, pTagSchema, pMetric->numOfTags, queryStr, queryCondLength);

      /* failed to build expression, no result, return immediately */
      if (pExpr == NULL) {
        mError("metric:%s, no result returned, error in metric query expression:%s", pMetric->meterId, queryStr);
        tfree(queryStr);
        return TSDB_CODE_OPS_NOT_SUPPORT;
      } else {
        // query according to the binary expression
        tSQLBinaryExprTraverse(pExpr, pMetric->pSkipList, pTagSchema, pMetric->numOfTags, tSQLElemFilterCallback, pRes);
        tSQLBinaryExprDestroy(&pExpr);
      }
    }
  } else {
    pRes->num = tSkipListIterateList(pMetric->pSkipList, (tSkipListNode ***)&pRes->pRes, NULL, NULL);
  }

  tfree(queryStr);
  mTrace("metric:%s numOfRes:%d", pMetric->meterId, pRes->num);

  mgmtReorganizeMetersInMetricMeta(pMetric, pInfo, pTagSchema, pRes);
  return TSDB_CODE_SUCCESS;
}

static int32_t mgmtGetMetricMetaMsgSize(tQueryResultset *pResult, int32_t tagLength, int32_t maxMetersPerQuery) {
  int32_t numOfVnodes = mgmtGetNumOfVnodesInResult(pResult);

  int32_t size = (sizeof(SMeterSidExtInfo) + tagLength) * pResult->num +
                 ((pResult->num / maxMetersPerQuery) + 1 + numOfVnodes) * sizeof(SVnodeSidList) + sizeof(SMetricMeta) +
                 1024;

  return size;
}

int mgmtRetrieveMetricMeta(void *thandle, char **pStart, STabObj *pMetric, SMetricMetaMsg *pMetricMetaMsg) {
  SVnodeSidList *pList = NULL;

  int32_t tagLen = mgmtGetReqTagsLength(pMetric, (int16_t *)pMetricMetaMsg->tagCols, pMetricMetaMsg->numOfTags);

  /*
   * naive method: Do not limit the maximum number of meters in each
   * vnode(subquery),
   * split the result according to vnodes
   * todo: split the number of vnodes to make sure each vnode has the same
   * number of
   * tables to query, while not break the upper limit of number of vnode queries
   */
  int32_t maxMetersPerVNodeInQuery = INT32_MAX;

  int ovgId = -1;

  tQueryResultset result = {0};
  int             ret = mgmtRetrieveMetersFromMetric(pMetric, pMetricMetaMsg, &result);

  int rspMsgSize = 512;
  if (ret == TSDB_CODE_SUCCESS) {
    rspMsgSize = mgmtGetMetricMetaMsgSize(&result, tagLen, maxMetersPerVNodeInQuery);
  }

  *pStart = taosBuildRspMsgWithSize(thandle, TSDB_MSG_TYPE_METRIC_META_RSP, rspMsgSize);
  if (*pStart == NULL) return 0;

  char *    pMsg = (*pStart);
  STaosRsp *pRsp = (STaosRsp *)pMsg;

  pRsp->code = ret;
  pMsg += sizeof(STaosRsp);
  *pMsg = TSDB_IE_TYPE_META;
  pMsg++;

  if (ret != TSDB_CODE_SUCCESS) {
    return pMsg - (*pStart);  // one bit in payload
  }

  SMetricMeta *pMeta = (SMetricMeta *)pMsg;

  pMeta->numOfMeters = 0;
  pMeta->numOfVnodes = 0;
  pMeta->tagLen = htons((uint16_t)tagLen);

  pMsg = (char *)pMeta + sizeof(SMetricMeta);

  // char* start = pMsg;
  for (int32_t i = 0; i < result.num; ++i) {
    STabObj *pMeter = mgmtGetResultPayload(&result, i);

#ifdef _DEBUG_VIEW
    mTrace("vgid: %d, sid: %d", pMeter->gid.vgId, pMeter->gid.sid);
#endif
    pMsg = mgmtBuildMetricMetaMsg(pMeter, &ovgId, &pList, pMeta, tagLen, pMetricMetaMsg->numOfTags,
                                  pMetricMetaMsg->tagCols, maxMetersPerVNodeInQuery, pMsg);
  }

  // char* output = malloc(pMsg - (*pStart));
  //  int32_t len = tsCompressString(start, pMsg - start, 0, output, 2, NULL);

  int32_t msgLen = pMsg - (*pStart);
  mTrace("metric:%s metric-meta tables:%d, vnode:%d, msg size %d", pMetric->meterId, pMeta->numOfMeters,
         pMeta->numOfVnodes, msgLen);

  pMeta->numOfMeters = htonl(pMeta->numOfMeters);
  pMeta->numOfVnodes = htonl(pMeta->numOfVnodes);

  tfree(result.pRes);
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

  if (pConn->pDb == NULL) return 0;
  strcpy(prefix, pConn->pDb->name);
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
    extractMeterName(pMeter->meterId, meterName);

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
      extractMeterName(pMeter->pTagData, pWrite);
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

  if (pConn->pDb == NULL) return TSDB_CODE_DB_NOT_SELECTED;

  SSchema *pSchema = tsGetSchema(pMeta);

  pShow->bytes[cols] = TSDB_METER_NAME_LEN;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "created time");
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

  pShow->numOfRows = pConn->pDb->numOfMetrics;
  pShow->pNode = pConn->pDb->pMetric;
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];

  return 0;
}

int mgmtRetrieveMetrics(SShowObj *pShow, char *data, int rows, SConnObj *pConn) {
  int      numOfRows = 0;
  STabObj *pMetric = NULL;
  char *   pWrite;
  int      cols = 0;

  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  char metricName[TSDB_METER_NAME_LEN] = {0};

  while (numOfRows < rows) {
    pMetric = (STabObj *)pShow->pNode;
    if (pMetric == NULL) break;
    pShow->pNode = (void *)pMetric->next;

    memset(metricName, 0, tListLen(metricName));
    extractMeterName(pMetric->meterId, metricName);

    if (pShow->payloadLen > 0 &&
        patternMatch(pShow->payload, metricName, TSDB_METER_NAME_LEN, &info) != TSDB_PATTERN_MATCH)
      continue;

    cols = 0;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    extractMeterName(pMetric->meterId, pWrite);
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

  pAcct = &acctObj;
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

  pAcct = &acctObj;

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
