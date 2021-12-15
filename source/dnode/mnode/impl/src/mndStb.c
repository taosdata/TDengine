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
#include "mndStb.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "tname.h"

#define TSDB_STB_VER_NUM 1
#define TSDB_STB_RESERVE_SIZE 64

static SSdbRaw *mndStbActionEncode(SStbObj *pStb);
static SSdbRow *mndStbActionDecode(SSdbRaw *pRaw);
static int32_t  mndStbActionInsert(SSdb *pSdb, SStbObj *pStb);
static int32_t  mndStbActionDelete(SSdb *pSdb, SStbObj *pStb);
static int32_t  mndStbActionUpdate(SSdb *pSdb, SStbObj *pOldStb, SStbObj *pNewStb);
static int32_t  mndProcessCreateStbMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessAlterStbMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessDropStbMsg(SMnodeMsg *pMsg);
static int32_t  mndProcessCreateStbInRsp(SMnodeMsg *pMsg);
static int32_t  mndProcessAlterStbInRsp(SMnodeMsg *pMsg);
static int32_t  mndProcessDropStbInRsp(SMnodeMsg *pMsg);
static int32_t  mndProcessStbMetaMsg(SMnodeMsg *pMsg);
static int32_t  mndGetStbMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta);
static int32_t  mndRetrieveStb(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextStb(SMnode *pMnode, void *pIter);

int32_t mndInitStb(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_STB,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndStbActionEncode,
                     .decodeFp = (SdbDecodeFp)mndStbActionDecode,
                     .insertFp = (SdbInsertFp)mndStbActionInsert,
                     .updateFp = (SdbUpdateFp)mndStbActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndStbActionDelete};

  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CREATE_STB, mndProcessCreateStbMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_ALTER_STB, mndProcessAlterStbMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_DROP_STB, mndProcessDropStbMsg);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_CREATE_STB_IN_RSP, mndProcessCreateStbInRsp);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_ALTER_STB_IN_RSP, mndProcessAlterStbInRsp);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_DROP_STB_IN_RSP, mndProcessDropStbInRsp);
  mndSetMsgHandle(pMnode, TSDB_MSG_TYPE_TABLE_META, mndProcessStbMetaMsg);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_STB, mndGetStbMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STB, mndRetrieveStb);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STB, mndCancelGetNextStb);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupStb(SMnode *pMnode) {}

static SSdbRaw *mndStbActionEncode(SStbObj *pStb) {
  int32_t  size = sizeof(SStbObj) + (pStb->numOfColumns + pStb->numOfTags) * sizeof(SSchema);
  SSdbRaw *pRaw = sdbAllocRaw(SDB_STB, TSDB_STB_VER_NUM, size);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pStb->name, TSDB_TABLE_NAME_LEN)
  SDB_SET_INT64(pRaw, dataPos, pStb->createdTime)
  SDB_SET_INT64(pRaw, dataPos, pStb->updateTime)
  SDB_SET_INT64(pRaw, dataPos, pStb->uid)
  SDB_SET_INT64(pRaw, dataPos, pStb->version)
  SDB_SET_INT32(pRaw, dataPos, pStb->numOfColumns)
  SDB_SET_INT32(pRaw, dataPos, pStb->numOfTags)

  int32_t totalCols = pStb->numOfColumns + pStb->numOfTags;
  for (int32_t i = 0; i < totalCols; ++i) {
    SSchema *pSchema = &pStb->pSchema[i];
    SDB_SET_INT8(pRaw, dataPos, pSchema->type);
    SDB_SET_INT32(pRaw, dataPos, pSchema->colId);
    SDB_SET_INT32(pRaw, dataPos, pSchema->bytes);
    SDB_SET_BINARY(pRaw, dataPos, pSchema->name, TSDB_COL_NAME_LEN);
  }

  SDB_SET_RESERVE(pRaw, dataPos, TSDB_STB_RESERVE_SIZE)
  SDB_SET_DATALEN(pRaw, dataPos);

  return pRaw;
}

static SSdbRow *mndStbActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;

  if (sver != TSDB_STB_VER_NUM) {
    mError("failed to decode stable since %s", terrstr());
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    return NULL;
  }

  int32_t  size = sizeof(SStbObj) + TSDB_MAX_COLUMNS * sizeof(SSchema);
  SSdbRow *pRow = sdbAllocRow(size);
  SStbObj *pStb = sdbGetRowObj(pRow);
  if (pStb == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, pRow, dataPos, pStb->name, TSDB_TABLE_NAME_LEN)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pStb->createdTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pStb->updateTime)
  SDB_GET_INT64(pRaw, pRow, dataPos, &pStb->uid)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pStb->version)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pStb->numOfColumns)
  SDB_GET_INT32(pRaw, pRow, dataPos, &pStb->numOfTags)

  int32_t totalCols = pStb->numOfColumns + pStb->numOfTags;
  pStb->pSchema = calloc(totalCols, sizeof(SSchema));

  for (int32_t i = 0; i < totalCols; ++i) {
    SSchema *pSchema = &pStb->pSchema[i];
    SDB_GET_INT8(pRaw, pRow, dataPos, &pSchema->type);
    SDB_GET_INT32(pRaw, pRow, dataPos, &pSchema->colId);
    SDB_GET_INT32(pRaw, pRow, dataPos, &pSchema->bytes);
    SDB_GET_BINARY(pRaw, pRow, dataPos, pSchema->name, TSDB_COL_NAME_LEN);
  }

  SDB_GET_RESERVE(pRaw, pRow, dataPos, TSDB_STB_RESERVE_SIZE)

  return pRow;
}

static int32_t mndStbActionInsert(SSdb *pSdb, SStbObj *pStb) {
  mTrace("stb:%s, perform insert action", pStb->name);
  return 0;
}

static int32_t mndStbActionDelete(SSdb *pSdb, SStbObj *pStb) {
  mTrace("stb:%s, perform delete action", pStb->name);
  return 0;
}

static int32_t mndStbActionUpdate(SSdb *pSdb, SStbObj *pOldStb, SStbObj *pNewStb) {
  mTrace("stb:%s, perform update action", pOldStb->name);
  atomic_exchange_32(&pOldStb->updateTime, pNewStb->updateTime);
  atomic_exchange_32(&pOldStb->version, pNewStb->version);

  taosWLockLatch(&pOldStb->lock);
  int32_t totalCols = pNewStb->numOfTags + pNewStb->numOfColumns;
  int32_t totalSize = totalCols * sizeof(SSchema);

  if (pOldStb->numOfTags + pOldStb->numOfColumns < totalCols) {
    pOldStb->pSchema = malloc(totalSize);
  }

  memcpy(pOldStb->pSchema, pNewStb->pSchema, totalSize);
  taosWUnLockLatch(&pOldStb->lock);
  return 0;
}

SStbObj *mndAcquireStb(SMnode *pMnode, char *stbName) {
  SSdb *pSdb = pMnode->pSdb;
  return sdbAcquire(pSdb, SDB_STB, stbName);
}

void mndReleaseStb(SMnode *pMnode, SStbObj *pStb) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pStb);
}

static SDbObj *mndAcquireDbByStb(SMnode *pMnode, char *stbName) {
  SName name = {0};
  tNameFromString(&name, stbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

  char db[TSDB_TABLE_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, db);

  return mndAcquireDb(pMnode, db);
}

static int32_t mndCheckStbMsg(SCreateStbMsg *pCreate) {
  pCreate->numOfColumns = htonl(pCreate->numOfColumns);
  pCreate->numOfTags = htonl(pCreate->numOfTags);
  int32_t totalCols = pCreate->numOfColumns + pCreate->numOfTags;
  for (int32_t i = 0; i < totalCols; ++i) {
    SSchema *pSchema = &pCreate->pSchema[i];
    pSchema->colId = htonl(pSchema->colId);
    pSchema->bytes = htonl(pSchema->bytes);
  }

  if (pCreate->igExists < 0 || pCreate->igExists > 1) {
    terrno = TSDB_CODE_MND_STB_INVALID_IGEXIST;
    return -1;
  }

  if (pCreate->numOfColumns < TSDB_MIN_COLUMNS || pCreate->numOfColumns > TSDB_MAX_COLUMNS) {
    terrno = TSDB_CODE_MND_STB_INVALID_COLS_NUM;
    return -1;
  }

  if (pCreate->numOfTags <= 0 || pCreate->numOfTags > TSDB_MAX_TAGS) {
    terrno = TSDB_CODE_MND_STB_INVALID_TAGS_NUM;
    return -1;
  }

  int32_t maxColId = (TSDB_MAX_COLUMNS + TSDB_MAX_TAGS);
  for (int32_t i = 0; i < totalCols; ++i) {
    SSchema *pSchema = &pCreate->pSchema[i];
    if (pSchema->type <= 0) {
      terrno = TSDB_CODE_MND_STB_INVALID_COL_TYPE;
      return -1;
    }
    if (pSchema->colId < 0 || pSchema->colId >= maxColId) {
      terrno = TSDB_CODE_MND_STB_INVALID_COL_ID;
      return -1;
    }
    if (pSchema->bytes <= 0) {
      terrno = TSDB_CODE_MND_STB_INVALID_COL_BYTES;
      return -1;
    }
    if (pSchema->name[0] == 0) {
      terrno = TSDB_CODE_MND_STB_INVALID_COL_NAME;
      return -1;
    }
  }

  return 0;
}

static int32_t mndCreateStb(SMnode *pMnode, SMnodeMsg *pMsg, SCreateStbMsg *pCreate, SDbObj *pDb) {
  SStbObj stbObj = {0};
  tstrncpy(stbObj.name, pCreate->name, TSDB_TABLE_FNAME_LEN);
  tstrncpy(stbObj.db, pDb->name, TSDB_FULL_DB_NAME_LEN);
  stbObj.createdTime = taosGetTimestampMs();
  stbObj.updateTime = stbObj.createdTime;
  stbObj.uid = mndGenerateUid(pCreate->name, TSDB_TABLE_FNAME_LEN);
  stbObj.version = 1;
  stbObj.numOfColumns = pCreate->numOfColumns;
  stbObj.numOfTags = pCreate->numOfTags;

  int32_t totalCols = stbObj.numOfColumns + stbObj.numOfTags;
  int32_t totalSize = totalCols * sizeof(SSchema);
  stbObj.pSchema = malloc(totalSize);
  if (stbObj.pSchema == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }
  memcpy(stbObj.pSchema, pCreate->pSchema, totalSize);

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, pMsg->rpcMsg.handle);
  if (pTrans == NULL) {
    mError("stb:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to create stb:%s", pTrans->id, pCreate->name);

  SSdbRaw *pRedoRaw = mndStbActionEncode(&stbObj);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING);

  SSdbRaw *pUndoRaw = mndStbActionEncode(&stbObj);
  if (pUndoRaw == NULL || mndTransAppendUndolog(pTrans, pUndoRaw) != 0) {
    mError("trans:%d, failed to append undo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED);

  SSdbRaw *pCommitRaw = mndStbActionEncode(&stbObj);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessCreateStbMsg(SMnodeMsg *pMsg) {
  SMnode        *pMnode = pMsg->pMnode;
  SCreateStbMsg *pCreate = pMsg->rpcMsg.pCont;

  mDebug("stb:%s, start to create", pCreate->name);

  if (mndCheckStbMsg(pCreate) != 0) {
    mError("stb:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  SStbObj *pStb = mndAcquireStb(pMnode, pCreate->name);
  if (pStb != NULL) {
    sdbRelease(pMnode->pSdb, pStb);
    if (pCreate->igExists) {
      mDebug("stb:%s, already exist, ignore exist is set", pCreate->name);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_STB_ALREADY_EXIST;
      mError("db:%s, failed to create since %s", pCreate->name, terrstr());
      return -1;
    }
  }

  SDbObj *pDb = mndAcquireDbByStb(pMnode, pCreate->name);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    mError("stb:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  int32_t code = mndCreateStb(pMnode, pMsg, pCreate, pDb);
  mndReleaseDb(pMnode, pDb);

  if (code != 0) {
    terrno = code;
    mError("stb:%s, failed to create since %s", pCreate->name, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessCreateStbInRsp(SMnodeMsg *pMsg) { return 0; }

static int32_t mndCheckAlterStbMsg(SAlterStbMsg *pAlter) {
  SSchema *pSchema = &pAlter->schema;
  pSchema->colId = htonl(pSchema->colId);
  pSchema->bytes = htonl(pSchema->bytes);

  if (pSchema->type <= 0) {
    terrno = TSDB_CODE_MND_STB_INVALID_COL_TYPE;
    return -1;
  }
  if (pSchema->colId < 0 || pSchema->colId >= (TSDB_MAX_COLUMNS + TSDB_MAX_TAGS)) {
    terrno = TSDB_CODE_MND_STB_INVALID_COL_ID;
    return -1;
  }
  if (pSchema->bytes <= 0) {
    terrno = TSDB_CODE_MND_STB_INVALID_COL_BYTES;
    return -1;
  }
  if (pSchema->name[0] == 0) {
    terrno = TSDB_CODE_MND_STB_INVALID_COL_NAME;
    return -1;
  }

  return 0;
}

static int32_t mndUpdateStb(SMnode *pMnode, SMnodeMsg *pMsg, SStbObj *pOldStb, SStbObj *pNewStb) { return 0; }

static int32_t mndProcessAlterStbMsg(SMnodeMsg *pMsg) {
  SMnode       *pMnode = pMsg->pMnode;
  SAlterStbMsg *pAlter = pMsg->rpcMsg.pCont;

  mDebug("stb:%s, start to alter", pAlter->name);

  if (mndCheckAlterStbMsg(pAlter) != 0) {
    mError("stb:%s, failed to alter since %s", pAlter->name, terrstr());
    return -1;
  }

  SStbObj *pStb = mndAcquireStb(pMnode, pAlter->name);
  if (pStb == NULL) {
    terrno = TSDB_CODE_MND_STB_NOT_EXIST;
    mError("stb:%s, failed to alter since %s", pAlter->name, terrstr());
    return -1;
  }

  SStbObj stbObj = {0};
  memcpy(&stbObj, pStb, sizeof(SStbObj));

  int32_t code = mndUpdateStb(pMnode, pMsg, pStb, &stbObj);
  mndReleaseStb(pMnode, pStb);

  if (code != 0) {
    mError("stb:%s, failed to alter since %s", pAlter->name, tstrerror(code));
    return code;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessAlterStbInRsp(SMnodeMsg *pMsg) { return 0; }

static int32_t mndDropStb(SMnode *pMnode, SMnodeMsg *pMsg, SStbObj *pStb) {
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, pMsg->rpcMsg.handle);
  if (pTrans == NULL) {
    mError("stb:%s, failed to drop since %s", pStb->name, terrstr());
    return -1;
  }
  mDebug("trans:%d, used to drop stb:%s", pTrans->id, pStb->name);

  SSdbRaw *pRedoRaw = mndStbActionEncode(pStb);
  if (pRedoRaw == NULL || mndTransAppendRedolog(pTrans, pRedoRaw) != 0) {
    mError("trans:%d, failed to append redo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING);

  SSdbRaw *pUndoRaw = mndStbActionEncode(pStb);
  if (pUndoRaw == NULL || mndTransAppendUndolog(pTrans, pUndoRaw) != 0) {
    mError("trans:%d, failed to append undo log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pUndoRaw, SDB_STATUS_READY);

  SSdbRaw *pCommitRaw = mndStbActionEncode(pStb);
  if (pCommitRaw == NULL || mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) {
    mError("trans:%d, failed to append commit log since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }
  sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED);

  if (mndTransPrepare(pMnode, pTrans) != 0) {
    mError("trans:%d, failed to prepare since %s", pTrans->id, terrstr());
    mndTransDrop(pTrans);
    return -1;
  }

  mndTransDrop(pTrans);
  return 0;
}

static int32_t mndProcessDropStbMsg(SMnodeMsg *pMsg) {
  SMnode      *pMnode = pMsg->pMnode;
  SDropStbMsg *pDrop = pMsg->rpcMsg.pCont;

  mDebug("stb:%s, start to drop", pDrop->name);

  SStbObj *pStb = mndAcquireStb(pMnode, pDrop->name);
  if (pStb == NULL) {
    if (pDrop->igNotExists) {
      mDebug("stb:%s, not exist, ignore not exist is set", pDrop->name);
      return 0;
    } else {
      terrno = TSDB_CODE_MND_STB_NOT_EXIST;
      mError("stb:%s, failed to drop since %s", pDrop->name, terrstr());
      return -1;
    }
  }

  int32_t code = mndDropStb(pMnode, pMsg, pStb);
  mndReleaseStb(pMnode, pStb);

  if (code != 0) {
    terrno = code;
    mError("stb:%s, failed to drop since %s", pDrop->name, terrstr());
    return -1;
  }

  return TSDB_CODE_MND_ACTION_IN_PROGRESS;
}

static int32_t mndProcessDropStbInRsp(SMnodeMsg *pMsg) { return 0; }

static int32_t mndProcessStbMetaMsg(SMnodeMsg *pMsg) {
  SMnode      *pMnode = pMsg->pMnode;
  SStbInfoMsg *pInfo = pMsg->rpcMsg.pCont;

  mDebug("stb:%s, start to retrieve meta", pInfo->name);

  SDbObj *pDb = mndAcquireDbByStb(pMnode, pInfo->name);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    mError("stb:%s, failed to retrieve meta since %s", pInfo->name, terrstr());
    return -1;
  }

  SStbObj *pStb = mndAcquireStb(pMnode, pInfo->name);
  if (pStb == NULL) {
    mndReleaseDb(pMnode, pDb);
    terrno = TSDB_CODE_MND_INVALID_TABLE_NAME;
    mError("stb:%s, failed to get meta since %s", pInfo->name, terrstr());
    return -1;
  }

  int32_t        contLen = sizeof(STableMetaMsg) + (pStb->numOfColumns + pStb->numOfTags) * sizeof(SSchema);
  STableMetaMsg *pMeta = rpcMallocCont(contLen);
  if (pMeta == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    mError("stb:%s, failed to get meta since %s", pInfo->name, terrstr());
    return -1;
  }

  memcpy(pMeta->stbFname, pStb->name, TSDB_TABLE_FNAME_LEN);
  pMeta->numOfTags = htonl(pStb->numOfTags);
  pMeta->numOfColumns = htonl(pStb->numOfColumns);
  pMeta->precision = pDb->cfg.precision;
  pMeta->tableType = TSDB_SUPER_TABLE;
  pMeta->update = pDb->cfg.update;
  pMeta->sversion = htonl(pStb->version);
  pMeta->suid = htonl(pStb->uid);

  for (int32_t i = 0; i < pStb->numOfColumns; ++i) {
    SSchema *pSchema = &pMeta->pSchema[i];
    SSchema *pSrcSchema = &pStb->pSchema[i];
    memcpy(pSchema->name, pSrcSchema->name, TSDB_COL_NAME_LEN);
    pSchema->type = pSrcSchema->type;
    pSchema->colId = htonl(pSrcSchema->colId);
    pSchema->bytes = htonl(pSrcSchema->bytes);
  }

  pMsg->pCont = pMeta;
  pMsg->contLen = contLen;

  mDebug("stb:%s, meta is retrieved, cols:%d tags:%d", pInfo->name, pStb->numOfColumns, pStb->numOfTags);
  return 0;
}

static int32_t mndGetNumOfStbs(SMnode *pMnode, char *dbName, int32_t *pNumOfStbs) {
  SSdb *pSdb = pMnode->pSdb;

  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    return -1;
  }

  int32_t numOfStbs = 0;
  void   *pIter = NULL;
  while (1) {
    SStbObj *pStb = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pStb);
    if (pIter == NULL) break;

    if (strcmp(pStb->db, dbName) == 0) {
      numOfStbs++;
    }

    sdbRelease(pSdb, pStb);
  }

  *pNumOfStbs = numOfStbs;
  return 0;
}

static int32_t mndGetStbMeta(SMnodeMsg *pMsg, SShowObj *pShow, STableMetaMsg *pMeta) {
  SMnode *pMnode = pMsg->pMnode;
  SSdb   *pSdb = pMnode->pSdb;

  if (mndGetNumOfStbs(pMnode, pShow->db, &pShow->numOfRows) != 0) {
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
  strcpy(pMeta->tbFname, mndShowStr(pShow->type));

  return 0;
}

static void mnodeExtractTableName(char *tableId, char *name) {
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

static int32_t mndRetrieveStb(SMnodeMsg *pMsg, SShowObj *pShow, char *data, int32_t rows) {
  SMnode  *pMnode = pMsg->pMnode;
  SSdb    *pSdb = pMnode->pSdb;
  int32_t  numOfRows = 0;
  SStbObj *pStb = NULL;
  int32_t  cols = 0;
  char    *pWrite;
  char     prefix[64] = {0};

  tstrncpy(prefix, pShow->db, 64);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = (int32_t)strlen(prefix);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_STB, pShow->pIter, (void **)&pStb);
    if (pShow->pIter == NULL) break;

    if (strncmp(pStb->name, prefix, prefixLen) != 0) {
      sdbRelease(pSdb, pStb);
      continue;
    }

    cols = 0;

    char stbName[TSDB_TABLE_FNAME_LEN] = {0};
    memcpy(stbName, pStb->name + prefixLen, TSDB_TABLE_FNAME_LEN - prefixLen);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, stbName);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pStb->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pStb->numOfColumns;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int16_t *)pWrite = pStb->numOfTags;
    cols++;

    numOfRows++;
    sdbRelease(pSdb, pStb);
  }

  pShow->numOfReads += numOfRows;
  mnodeVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  return numOfRows;
}

static void mndCancelGetNextStb(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}