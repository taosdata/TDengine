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
#include "tname.h"

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
static int32_t  mndProcessCreateStableInRsp(SMnodeMsg *pMsg);
static int32_t  mndProcessAlterStableInRsp(SMnodeMsg *pMsg);
static int32_t  mndProcessDropStableInRsp(SMnodeMsg *pMsg);
static int32_t  mndProcessStableMetaMsg(SMnodeMsg *pMsg);
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

  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CREATE_STABLE, mndProcessCreateStableMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_ALTER_STABLE, mndProcessAlterStableMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_DROP_STABLE, mndProcessDropStableMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CREATE_STABLE_IN_RSP, mndProcessCreateStableInRsp);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_ALTER_STABLE_IN_RSP, mndProcessAlterStableInRsp);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_DROP_STABLE_IN_RSP, mndProcessDropStableInRsp);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_TABLE_META, mndProcessStableMetaMsg);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_STABLE, mndGetStableMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STABLE, mndRetrieveStables);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STABLE, mndCancelGetNextStable);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupStable(SMnode *pMnode) {}

static SSdbRaw *mndStableActionEncode(SStableObj *pStable) {
  int32_t  size = sizeof(SStableObj) + (pStable->numOfColumns + pStable->numOfTags) * sizeof(SSchema);
  SSdbRaw *pRaw = sdbAllocRaw(SDB_STABLE, TSDB_STABLE_VER_NUM, size);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pStable->name, TSDB_TABLE_NAME_LEN)
  SDB_SET_INT64(pRaw, dataPos, pStable->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pStable->updateTime)
  SDB_SET_INT64(pRaw, dataPos, pStable->uid)
  SDB_SET_INT64(pRaw, dataPos, pStable->version)
  SDB_SET_INT32(pRaw, dataPos, pStable->numOfColumns)
  SDB_SET_INT32(pRaw, dataPos, pStable->numOfTags)

  for (int32_t i = 0; i < pStable->numOfColumns; ++i) {
    SSchema *pSchema = &pStable->columnSchema[i];
    SDB_SET_INT8(pRaw, dataPos, pSchema->type);
    SDB_SET_INT32(pRaw, dataPos, pSchema->colId);
    SDB_SET_INT32(pRaw, dataPos, pSchema->bytes);
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
  SDB_GET_INT32(pRaw, pRow, dataPos, &pStable->numOfColumns)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pStable->numOfTags)

  pStable->columnSchema = calloc(pStable->numOfColumns, sizeof(SSchema));
  pStable->tagSchema = calloc(pStable->numOfTags, sizeof(SSchema));

  for (int32_t i = 0; i < pStable->numOfColumns; ++i) {
    SSchema *pSchema = &pStable->columnSchema[i];
    SDB_GET_INT8(pRaw, pRow, dataPos, &pSchema->type);
    SDB_GET_INT32(pRaw, pRow, dataPos, &pSchema->colId);
    SDB_GET_INT32(pRaw, pRow, dataPos, &pSchema->bytes);
    SDB_GET_BINARY(pRaw, pRow, dataPos, pSchema->name, TSDB_COL_NAME_LEN);
  }

  for (int32_t i = 0; i < pStable->numOfTags; ++i) {
    SSchema *pSchema = &pStable->tagSchema[i];
    SDB_GET_INT8(pRaw, pRow, dataPos, &pSchema->type);
    SDB_GET_INT32(pRaw, pRow, dataPos, &pSchema->colId);
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
  atomic_exchange_32(&pOldStable->updateTime, pNewStable->updateTime);
  atomic_exchange_32(&pOldStable->version, pNewStable->version);

  taosWLockLatch(&pOldStable->lock);
  int32_t numOfTags = pNewStable->numOfTags;
  int32_t tagSize = numOfTags * sizeof(SSchema);
  int32_t numOfColumns = pNewStable->numOfColumns;
  int32_t columnSize = numOfColumns * sizeof(SSchema);

  if (pOldStable->numOfTags < numOfTags) {
    pOldStable->tagSchema = malloc(tagSize);
  }
  if (pOldStable->numOfColumns < numOfColumns) {
    pOldStable->columnSchema = malloc(columnSize);
  }

  memcpy(pOldStable->tagSchema, pNewStable->tagSchema, tagSize);
  memcpy(pOldStable->columnSchema, pNewStable->columnSchema, columnSize);
  taosWUnLockLatch(&pOldStable->lock);
  return 0;
}

SStableObj *mndAcquireStb(SMnode *pMnode, char *stbName) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbAcquire(pSdb, SDB_STABLE, stbName);
}

void mndReleaseStb(SMnode *pMnode, SStableObj *pStb) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pStb);
}

static int32_t mndProcessCreateStableMsg(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessCreateStableInRsp(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessAlterStableMsg(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessAlterStableInRsp(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessDropStableMsg(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessDropStableInRsp(SMnodeMsg *pMsg) { return 0; }

static SDbObj *mndGetDbByStbName(SMnode *pMnode, char *stbName) {
  SName name = {0};
  tNameFromString(&name, stbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

  char db[TSDB_TABLE_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, db);

  return mndAcquireDb(pMnode, db);
}

static int32_t mndProcessStableMetaMsg(SMnodeMsg *pMsg) {
  SMnode         *pMnode = pMsg->pMnode;
  SStableInfoMsg *pInfo = pMsg->rpcMsg.pCont;

  mDebug("stable:%s, start to retrieve meta", pInfo->name);

  SDbObj *pDb = mndGetDbByStbName(pMnode, pInfo->name);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    mError("stable:%s, failed to retrieve meta since %s", pInfo->name, terrstr());
    return -1;
  }

  SStableObj *pStb = mndAcquireStb(pMnode, pInfo->name);
  if (pStb == NULL) {
    mndReleaseDb(pMnode, pDb);
    terrno = TSDB_CODE_MND_INVALID_TABLE_NAME;
    mError("stable:%s, failed to get meta since %s", pInfo->name, terrstr());
    return -1;
  }

  int32_t        contLen = sizeof(STableMetaMsg) + (pStb->numOfColumns + pStb->numOfTags) * sizeof(SSchema);
  STableMetaMsg *pMeta = rpcMallocCont(contLen);
  if (pMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("stable:%s, failed to get meta since %s", pInfo->name, terrstr());
    return -1;
  }

  memcpy(pMeta->stableFname, pStb->name, TSDB_TABLE_FNAME_LEN);
  pMeta->numOfTags = htonl(pStb->numOfTags);
  pMeta->numOfColumns = htonl(pStb->numOfColumns);
  pMeta->precision = pDb->cfg.precision;
  pMeta->tableType = TSDB_SUPER_TABLE;
  pMeta->update = pDb->cfg.update;
  pMeta->sversion = htonl(pStb->version);
  pMeta->suid = htonl(pStb->uid);

  for (int32_t i = 0; i < pStb->numOfColumns; ++i) {
    SSchema *pSchema = &pMeta->pSchema[i];
    SSchema *pColumn = &pStb->columnSchema[i];
    memcpy(pSchema->name, pColumn->name, TSDB_COL_NAME_LEN);
    pSchema->type = pColumn->type;
    pSchema->colId = htonl(pColumn->colId);
    pSchema->bytes = htonl(pColumn->bytes);
  }

  for (int32_t i = 0; i < pStb->numOfTags; ++i) {
    SSchema *pSchema = &pMeta->pSchema[i + pStb->numOfColumns];
    SSchema *pTag = &pStb->tagSchema[i];
    memcpy(pSchema->name, pTag->name, TSDB_COL_NAME_LEN);
    pSchema->type = pTag->type;
    pSchema->colId = htons(pTag->colId);
    pSchema->bytes = htonl(pTag->bytes);
  }

  pMsg->pCont = pMeta;
  pMsg->contLen = contLen;

  mDebug("stable:%s, meta is retrieved, cols:%d tags:%d", pInfo->name, pStb->numOfColumns, pStb->numOfTags);
  return 0;
}

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
  SSchema *pSchema = pMeta->pSchema;

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
    *(int16_t *)pWrite = pStable->numOfColumns;
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