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
#include "mndStable.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndDb.h"

#define TSDB_STABLE_VER_NUM 1
#define TSDB_STABLE_RESERVE_SIZE 64

static SSdbRaw *mndStableActionEncode(SStableObj *pStable);
static SSdbRow *mndStableActionDecode(SSdbRaw *pRaw);
static int32_t  mndStableActionInsert(SSdb *pSdb, SStableObj *pStable);
static int32_t  mndStableActionDelete(SSdb *pSdb, SStableObj *pStable);
static int32_t  mndStableActionUpdate(SSdb *pSdb, SStableObj *pOldStable, SStableObj *pNewStable);
static int32_t  mndProcessCreateStableMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessAlterStableMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessDropStableMsg(SMnodeMsg *pMsg);
static int32_t  mndGetStableMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t  mndRetrieveStables(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextStable(SMnode *pMnode, void *pIter);

int32_t mndInitStable(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_STABLE,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndStableActionEncode,
                     .decodeFp = (SdbDecodeFp)mndStableActionDecode,
                     .insertFp = (SdbInsertFp)mndStableActionInsert,
                     .updateFp = (SdbUpdateFp)mndStableActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndStableActionDelete};

  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CREATE_DB, mndProcessCreateStableMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_ALTER_DB, mndProcessAlterStableMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_DROP_DB, mndProcessDropStableMsg);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_DB, mndGetStableMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_DB, mndRetrieveStables);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_DB, mndCancelGetNextStable);
}

void mndCleanupStable(SMnode *pMnode) {}

static SSdbRaw *mndStableActionEncode(SStableObj *pStable) {
  int32_t  size = sizeof(SStableObj) + (pStable->numOfFields + pStable->numOfTags) * sizeof(SSchema);
  SSdbRaw *pRaw = sdbAllocRaw(SDB_STABLE, TSDB_STABLE_VER_NUM, size);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pStable->name, TSDB_TABLE_NAME_LEN)
  SDB_SET_INT64(pRaw, dataPos, pStable->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pStable->updateTime)
  SDB_SET_INT64(pRaw, dataPos, pStable->uid)
  SDB_SET_INT64(pRaw, dataPos, pStable->version)
  SDB_SET_INT16(pRaw, dataPos, pStable->numOfFields)
  SDB_SET_INT16(pRaw, dataPos, pStable->numOfTags)

  for (int32_t i = 0; i < pStable->numOfFields; ++i) {
    SSchema *pSchema = &pStable->fieldSchema[i];
    SDB_SET_INT8(pRaw, dataPos, pSchema->type);
    SDB_SET_INT16(pRaw, dataPos, pSchema->colId);
    SDB_SET_INT16(pRaw, dataPos, pSchema->bytes);
    SDB_SET_BINARY(pRaw, dataPos, pSchema->name, TSDB_COL_NAME_LEN);
  }

  for (int32_t i = 0; i < pStable->numOfTags; ++i) {
    SSchema *pSchema = &pStable->tagSchema[i];
    SDB_SET_INT8(pRaw, dataPos, pSchema->type);
    SDB_SET_INT32(pRaw, dataPos, pSchema->colId);
    SDB_SET_INT32(pRaw, dataPos, pSchema->bytes);
    SDB_SET_BINARY(pRaw, dataPos, pSchema->name, TSDB_COL_NAME_LEN);
  }

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_STABLE_RESERVE_SIZE)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mndStableActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != TSDB_STABLE_VER_NUM) {
    mError("failed to decode stable since %s", terrstr());
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    return NULL;
  }

  int32_t     size = sizeof(SStableObj) + TSDB_MAX_COLUMNS * sizeof(SSchema);
  SSdbRow    *pRow = sdbAllocRow(size);
  SStableObj *pStable = sdbGetRowObj(pRow);
  if (pStable == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, pRow, dataPos, pStable->name, TSDB_TABLE_NAME_LEN)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pStable->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pStable->updateTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pStable->uid)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pStable->version)
  SDB_GET_INT16(pRaw, pRow, dataPos, &pStable->numOfFields)
  SDB_GET_INT16(pRaw, pRow, dataPos, &pStable->numOfTags)

  pStable->fieldSchema = (SSchema *)pStable->pCont;
  pStable->tagSchema = (SSchema *)(pStable->pCont + pStable->numOfFields * sizeof(SSchema));

  for (int32_t i = 0; i < pStable->numOfFields; ++i) {
    SSchema *pSchema = &pStable->fieldSchema[i];
    SDB_GET_INT8(pRaw, pRow, dataPos, &pSchema->type);
    SDB_GET_INT16(pRaw, pRow, dataPos, &pSchema->colId);
    SDB_GET_INT32(pRaw, pRow, dataPos, &pSchema->bytes);
    SDB_GET_BINARY(pRaw, pRow, dataPos, pSchema->name, TSDB_COL_NAME_LEN);
  }

  for (int32_t i = 0; i < pStable->numOfTags; ++i) {
    SSchema *pSchema = &pStable->tagSchema[i];
    SDB_GET_INT8(pRaw, pRow, dataPos, &pSchema->type);
    SDB_GET_INT16(pRaw, pRow, dataPos, &pSchema->colId);
    SDB_GET_INT32(pRaw, pRow, dataPos, &pSchema->bytes);
    SDB_GET_BINARY(pRaw, pRow, dataPos, pSchema->name, TSDB_COL_NAME_LEN);
  }

  SDB_GET_RESERVE(pRaw, pRow, dataPos, TSDB_STABLE_RESERVE_SIZE)

  return pRow;
}

static int32_t mndStableActionInsert(SSdb *pSdb, SStableObj *pStable) {
  mTrace("stable:%s, perform insert action", pStable->name);
  return 0;
}

static int32_t mndStableActionDelete(SSdb *pSdb, SStableObj *pStable) {
  mTrace("stable:%s, perform delete action", pStable->name);
  return 0;
}

static int32_t mndStableActionUpdate(SSdb *pSdb, SStableObj *pOldStable, SStableObj *pNewStable) {
  mTrace("stable:%s, perform update action", pOldStable->name);
  memcpy(pOldStable->name, pNewStable->name, TSDB_TABLE_NAME_LEN);
  pOldStable->createdTime = pNewStable->createdTime;
  pOldStable->updateTime = pNewStable->updateTime;
  pOldStable->uid = pNewStable->uid;
  pOldStable->version = pNewStable->version;
  pOldStable->numOfFields = pNewStable->numOfFields;
  pOldStable->numOfTags = pNewStable->numOfTags;
  pOldStable->createdTime = pNewStable->createdTime;

  memcpy(pOldStable->pCont, pNewStable, sizeof(SDbObj));
  // pStable->fieldSchema = pStable->pCont;
  // pStable->tagSchema = pStable->pCont + pStable->numOfFields * sizeof(SSchema);

  return 0;
}

static int32_t mndProcessCreateStableMsg(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessAlterStableMsg(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessDropStableMsg(SMnodeMsg *pMsg) { return 0; }

static int32_t mndGetNumOfStables(SMnode *pMnode, char *dbName, int32_t *pNumOfStables) {
  SSdb *pSdb = pMnode->pSdb;

  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    return -1;
  }

  int32_t numOfStables = 0;
  void   *pIter = NULL;
  while (1) {
    SStableObj *pStable = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pStable);
    if (pIter == NULL) break;

    if (strcmp(pStable->db, dbName) == 0) {
      numOfStables++;
    }

    sdbRelease(pSdb, pStable);
  }

  *pNumOfStables = numOfStables;
  return 0;
}

static int32_t mndGetStableMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  if (mndGetNumOfStables(pMnode, pShow->db, &pShow->numOfRows) != 0) {
    return -1;
  }

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->schema;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
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

  pShow->bytes[cols] = 2;
  pSchema[cols].type = TSDB_DATA_TYPE_SMALLINT;
  strcpy(pSchema[cols].name, "tags");
  pSchema[cols].bytes = htons(pShow->bytes[cols]);
  cols++;

  pMeta->numOfColumns = htons(cols);
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tableFname, mndShowStr(pShow->type));

  return 0;
}

static void mnodeExtractTableName(char* tableId, char* name) {
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

static int32_t mndRetrieveStables(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode     *pMnode = pMsg->pMnode;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  SStableObj *pStable = NULL;
  int32_t     cols = 0;
  char       *pWrite;
  char        prefix[64] = {0};

  tstrncpy(prefix, pShow->db, 64);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = (int32_t)strlen(prefix);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_STABLE, pShow->pIter, (void **)&pStable);
    if (pShow->pIter == NULL) break;

    if (strncmp(pStable->name, prefix, prefixLen) != 0) {
      sdbRelease(pSdb, pStable);
      continue;
    }

    cols = 0;

    char stableName[TSDB_TABLE_FNAME_LEN] = {0};
    memcpy(stableName, pStable->name + prefixLen, TSDB_TABLE_FNAME_LEN - prefixLen);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, stableName);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pStable->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pStable->numOfFields;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pStable->numOfTags;
    cols++;

    numOfRows++;
    sdbRelease(pSdb, pStable);
  }

  pShow->numOfReads += numOfRows;
  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  return numOfRows;
}

static void mndCancelGetNextStable(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}