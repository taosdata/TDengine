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
#include "mndAuth.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndShow.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "mndInfoSchema.h"
#include "tname.h"

#define TSDB_STB_VER_NUMBER 1
#define TSDB_STB_RESERVE_SIZE 64

static SSdbRow *mndStbActionDecode(SSdbRaw *pRaw);
static int32_t  mndStbActionInsert(SSdb *pSdb, SStbObj *pStb);
static int32_t  mndStbActionDelete(SSdb *pSdb, SStbObj *pStb);
static int32_t  mndStbActionUpdate(SSdb *pSdb, SStbObj *pOld, SStbObj *pNew);
static int32_t  mndProcessMCreateStbReq(SNodeMsg *pReq);
static int32_t  mndProcessMAlterStbReq(SNodeMsg *pReq);
static int32_t  mndProcessMDropStbReq(SNodeMsg *pReq);
static int32_t  mndProcessVCreateStbRsp(SNodeMsg *pRsp);
static int32_t  mndProcessVAlterStbRsp(SNodeMsg *pRsp);
static int32_t  mndProcessVDropStbRsp(SNodeMsg *pRsp);
static int32_t  mndProcessTableMetaReq(SNodeMsg *pReq);
static int32_t  mndGetStbMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta);
static int32_t  mndRetrieveStb(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows);
static void     mndCancelGetNextStb(SMnode *pMnode, void *pIter);

int32_t mndInitStb(SMnode *pMnode) {
  SSdbTable table = {.sdbType = SDB_STB,
                     .keyType = SDB_KEY_BINARY,
                     .encodeFp = (SdbEncodeFp)mndStbActionEncode,
                     .decodeFp = (SdbDecodeFp)mndStbActionDecode,
                     .insertFp = (SdbInsertFp)mndStbActionInsert,
                     .updateFp = (SdbUpdateFp)mndStbActionUpdate,
                     .deleteFp = (SdbDeleteFp)mndStbActionDelete};

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_STB, mndProcessMCreateStbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_STB, mndProcessMAlterStbReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_STB, mndProcessMDropStbReq);
  mndSetMsgHandle(pMnode, TDMT_VND_CREATE_STB_RSP, mndProcessVCreateStbRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_STB_RSP, mndProcessVAlterStbRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_STB_RSP, mndProcessVDropStbRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_TABLE_META, mndProcessTableMetaReq);

  mndAddShowMetaHandle(pMnode, TSDB_MGMT_TABLE_STB, mndGetStbMeta);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_STB, mndRetrieveStb);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_STB, mndCancelGetNextStb);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupStb(SMnode *pMnode) {}

SSdbRaw *mndStbActionEncode(SStbObj *pStb) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int32_t  size = sizeof(SStbObj) + (pStb->numOfColumns + pStb->numOfTags) * sizeof(SSchema) + TSDB_STB_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_STB, TSDB_STB_VER_NUMBER, size);
  if (pRaw == NULL) goto STB_ENCODE_OVER;

  int32_t dataPos = 0;
  SDB_SET_BINARY(pRaw, dataPos, pStb->name, TSDB_TABLE_FNAME_LEN, STB_ENCODE_OVER)
  SDB_SET_BINARY(pRaw, dataPos, pStb->db, TSDB_DB_FNAME_LEN, STB_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pStb->createdTime, STB_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pStb->updateTime, STB_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pStb->uid, STB_ENCODE_OVER)
  SDB_SET_INT64(pRaw, dataPos, pStb->dbUid, STB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->version, STB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->nextColId, STB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->numOfColumns, STB_ENCODE_OVER)
  SDB_SET_INT32(pRaw, dataPos, pStb->numOfTags, STB_ENCODE_OVER)

  for (int32_t i = 0; i < pStb->numOfColumns; ++i) {
    SSchema *pSchema = &pStb->pColumns[i];
    SDB_SET_INT8(pRaw, dataPos, pSchema->type, STB_ENCODE_OVER)
    SDB_SET_INT32(pRaw, dataPos, pSchema->colId, STB_ENCODE_OVER)
    SDB_SET_INT32(pRaw, dataPos, pSchema->bytes, STB_ENCODE_OVER)
    SDB_SET_BINARY(pRaw, dataPos, pSchema->name, TSDB_COL_NAME_LEN, STB_ENCODE_OVER)
  }

  for (int32_t i = 0; i < pStb->numOfTags; ++i) {
    SSchema *pSchema = &pStb->pTags[i];
    SDB_SET_INT8(pRaw, dataPos, pSchema->type, STB_ENCODE_OVER)
    SDB_SET_INT32(pRaw, dataPos, pSchema->colId, STB_ENCODE_OVER)
    SDB_SET_INT32(pRaw, dataPos, pSchema->bytes, STB_ENCODE_OVER)
    SDB_SET_BINARY(pRaw, dataPos, pSchema->name, TSDB_COL_NAME_LEN, STB_ENCODE_OVER)
  }

  SDB_SET_BINARY(pRaw, dataPos, pStb->comment, TSDB_STB_COMMENT_LEN, STB_ENCODE_OVER)
  SDB_SET_RESERVE(pRaw, dataPos, TSDB_STB_RESERVE_SIZE, STB_ENCODE_OVER)
  SDB_SET_DATALEN(pRaw, dataPos, STB_ENCODE_OVER)

  terrno = 0;

STB_ENCODE_OVER:
  if (terrno != 0) {
    mError("stb:%s, failed to encode to raw:%p since %s", pStb->name, pRaw, terrstr());
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("stb:%s, encode to raw:%p, row:%p", pStb->name, pRaw, pStb);
  return pRaw;
}

static SSdbRow *mndStbActionDecode(SSdbRaw *pRaw) {
  terrno = TSDB_CODE_OUT_OF_MEMORY;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) goto STB_DECODE_OVER;

  if (sver != TSDB_STB_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    goto STB_DECODE_OVER;
  }

  SSdbRow *pRow = sdbAllocRow(sizeof(SStbObj));
  if (pRow == NULL) goto STB_DECODE_OVER;

  SStbObj *pStb = sdbGetRowObj(pRow);
  if (pStb == NULL) goto STB_DECODE_OVER;

  int32_t dataPos = 0;
  SDB_GET_BINARY(pRaw, dataPos, pStb->name, TSDB_TABLE_FNAME_LEN, STB_DECODE_OVER)
  SDB_GET_BINARY(pRaw, dataPos, pStb->db, TSDB_DB_FNAME_LEN, STB_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pStb->createdTime, STB_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pStb->updateTime, STB_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pStb->uid, STB_DECODE_OVER)
  SDB_GET_INT64(pRaw, dataPos, &pStb->dbUid, STB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->version, STB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->nextColId, STB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->numOfColumns, STB_DECODE_OVER)
  SDB_GET_INT32(pRaw, dataPos, &pStb->numOfTags, STB_DECODE_OVER)

  pStb->pColumns = calloc(pStb->numOfColumns, sizeof(SSchema));
  pStb->pTags = calloc(pStb->numOfTags, sizeof(SSchema));
  if (pStb->pColumns == NULL || pStb->pTags == NULL) {
    goto STB_DECODE_OVER;
  }

  for (int32_t i = 0; i < pStb->numOfColumns; ++i) {
    SSchema *pSchema = &pStb->pColumns[i];
    SDB_GET_INT8(pRaw, dataPos, &pSchema->type, STB_DECODE_OVER)
    SDB_GET_INT32(pRaw, dataPos, &pSchema->colId, STB_DECODE_OVER)
    SDB_GET_INT32(pRaw, dataPos, &pSchema->bytes, STB_DECODE_OVER)
    SDB_GET_BINARY(pRaw, dataPos, pSchema->name, TSDB_COL_NAME_LEN, STB_DECODE_OVER)
  }

  for (int32_t i = 0; i < pStb->numOfTags; ++i) {
    SSchema *pSchema = &pStb->pTags[i];
    SDB_GET_INT8(pRaw, dataPos, &pSchema->type, STB_DECODE_OVER)
    SDB_GET_INT32(pRaw, dataPos, &pSchema->colId, STB_DECODE_OVER)
    SDB_GET_INT32(pRaw, dataPos, &pSchema->bytes, STB_DECODE_OVER)
    SDB_GET_BINARY(pRaw, dataPos, pSchema->name, TSDB_COL_NAME_LEN, STB_DECODE_OVER)
  }

  SDB_GET_BINARY(pRaw, dataPos, pStb->comment, TSDB_STB_COMMENT_LEN, STB_DECODE_OVER)
  SDB_GET_RESERVE(pRaw, dataPos, TSDB_STB_RESERVE_SIZE, STB_DECODE_OVER)

  terrno = 0;

STB_DECODE_OVER:
  if (terrno != 0) {
    mError("stb:%s, failed to decode from raw:%p since %s", pStb->name, pRaw, terrstr());
    tfree(pStb->pColumns);
    tfree(pStb->pTags);
    tfree(pRow);
    return NULL;
  }

  mTrace("stb:%s, decode from raw:%p, row:%p", pStb->name, pRaw, pStb);
  return pRow;
}

static int32_t mndStbActionInsert(SSdb *pSdb, SStbObj *pStb) {
  mTrace("stb:%s, perform insert action, row:%p", pStb->name, pStb);
  return 0;
}

static int32_t mndStbActionDelete(SSdb *pSdb, SStbObj *pStb) {
  mTrace("stb:%s, perform delete action, row:%p", pStb->name, pStb);
  tfree(pStb->pColumns);
  tfree(pStb->pTags);
  return 0;
}

static int32_t mndStbActionUpdate(SSdb *pSdb, SStbObj *pOld, SStbObj *pNew) {
  mTrace("stb:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);

  taosWLockLatch(&pOld->lock);

  if (pOld->numOfColumns < pNew->numOfColumns) {
    void *pColumns = malloc(pNew->numOfColumns * sizeof(SSchema));
    if (pColumns != NULL) {
      free(pOld->pColumns);
      pOld->pColumns = pColumns;
    } else {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      mTrace("stb:%s, failed to perform update action since %s", pOld->name, terrstr());
      taosWUnLockLatch(&pOld->lock);
    }
  }

  if (pOld->numOfTags < pNew->numOfTags) {
    void *pTags = malloc(pNew->numOfTags * sizeof(SSchema));
    if (pTags != NULL) {
      free(pOld->pTags);
      pOld->pTags = pTags;
    } else {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      mTrace("stb:%s, failed to perform update action since %s", pOld->name, terrstr());
      taosWUnLockLatch(&pOld->lock);
    }
  }

  pOld->updateTime = pNew->updateTime;
  pOld->version = pNew->version;
  pOld->nextColId = pNew->nextColId;
  pOld->numOfColumns = pNew->numOfColumns;
  pOld->numOfTags = pNew->numOfTags;
  memcpy(pOld->pColumns, pNew->pColumns, pOld->numOfColumns * sizeof(SSchema));
  memcpy(pOld->pTags, pNew->pTags, pOld->numOfTags * sizeof(SSchema));
  memcpy(pOld->comment, pNew->comment, TSDB_STB_COMMENT_LEN);
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

SStbObj *mndAcquireStb(SMnode *pMnode, char *stbName) {
  SSdb    *pSdb = pMnode->pSdb;
  SStbObj *pStb = sdbAcquire(pSdb, SDB_STB, stbName);
  if (pStb == NULL && terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
    terrno = TSDB_CODE_MND_STB_NOT_EXIST;
  }
  return pStb;
}

void mndReleaseStb(SMnode *pMnode, SStbObj *pStb) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pStb);
}

static SDbObj *mndAcquireDbByStb(SMnode *pMnode, const char *stbName) {
  SName name = {0};
  tNameFromString(&name, stbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

  char db[TSDB_TABLE_FNAME_LEN] = {0};
  tNameGetFullDbName(&name, db);

  return mndAcquireDb(pMnode, db);
}

static void *mndBuildVCreateStbReq(SMnode *pMnode, SVgObj *pVgroup, SStbObj *pStb, int32_t *pContLen) {
  SName name = {0};
  tNameFromString(&name, pStb->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

  SVCreateTbReq req = {0};
  req.ver = 0;
  req.name = (char *)tNameGetTableName(&name);
  req.ttl = 0;
  req.keep = 0;
  req.type = TD_SUPER_TABLE;
  req.stbCfg.suid = pStb->uid;
  req.stbCfg.nCols = pStb->numOfColumns;
  req.stbCfg.pSchema = pStb->pColumns;
  req.stbCfg.nTagCols = pStb->numOfTags;
  req.stbCfg.pTagSchema = pStb->pTags;

  int32_t   contLen = tSerializeSVCreateTbReq(NULL, &req) + sizeof(SMsgHead);
  SMsgHead *pHead = malloc(contLen);
  if (pHead == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);

  void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
  tSerializeSVCreateTbReq(&pBuf, &req);

  *pContLen = contLen;
  return pHead;
}

static void *mndBuildVDropStbReq(SMnode *pMnode, SVgObj *pVgroup, SStbObj *pStb, int32_t *pContLen) {
  SName name = {0};
  tNameFromString(&name, pStb->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);

  SVDropTbReq req = {0};
  req.ver = 0;
  req.name = (char *)tNameGetTableName(&name);
  req.type = TD_SUPER_TABLE;
  req.suid = pStb->uid;

  int32_t   contLen = tSerializeSVDropTbReq(NULL, &req) + sizeof(SMsgHead);
  SMsgHead *pHead = malloc(contLen);
  if (pHead == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return NULL;
  }

  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);

  void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
  tSerializeSVDropTbReq(&pBuf, &req);

  *pContLen = contLen;
  return pHead;
}

static int32_t mndCheckCreateStbReq(SMCreateStbReq *pCreate) {
  if (pCreate->igExists < 0 || pCreate->igExists > 1) {
    terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
    return -1;
  }

  if (pCreate->numOfColumns < TSDB_MIN_COLUMNS || pCreate->numOfColumns > TSDB_MAX_COLUMNS) {
    terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
    return -1;
  }

  if (pCreate->numOfTags <= 0 || pCreate->numOfTags > TSDB_MAX_TAGS) {
    terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
    return -1;
  }

  SField *pField = taosArrayGet(pCreate->pColumns, 0);
  if (pField->type != TSDB_DATA_TYPE_TIMESTAMP) {
    terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
    return -1;
  }

  for (int32_t i = 0; i < pCreate->numOfColumns; ++i) {
    SField *pField = taosArrayGet(pCreate->pColumns, i);
    if (pField->type < 0) {
      terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
      return -1;
    }
    if (pField->bytes <= 0) {
      terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
      return -1;
    }
    if (pField->name[0] == 0) {
      terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
      return -1;
    }
  }

  for (int32_t i = 0; i < pCreate->numOfTags; ++i) {
    SField *pField = taosArrayGet(pCreate->pTags, i);
    if (pField->type < 0) {
      terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
      return -1;
    }
    if (pField->bytes <= 0) {
      terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
      return -1;
    }
    if (pField->name[0] == 0) {
      terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
      return -1;
    }
  }

  return 0;
}

static int32_t mndSetCreateStbRedoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  SSdbRaw *pRedoRaw = mndStbActionEncode(pStb);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING) != 0) return -1;

  return 0;
}

static int32_t mndSetCreateStbUndoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  SSdbRaw *pUndoRaw = mndStbActionEncode(pStb);
  if (pUndoRaw == NULL) return -1;
  if (mndTransAppendUndolog(pTrans, pUndoRaw) != 0) return -1;
  if (sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED) != 0) return -1;

  return 0;
}

static int32_t mndSetCreateStbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  SSdbRaw *pCommitRaw = mndStbActionEncode(pStb);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;

  return 0;
}

static int32_t mndSetCreateStbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;
  int32_t contLen;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pDb->uid) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    void *pReq = mndBuildVCreateStbReq(pMnode, pVgroup, pStb, &contLen);
    if (pReq == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_VND_CREATE_STB;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      free(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      return -1;
    }
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndSetCreateStbUndoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pDb->uid) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    int32_t contLen = 0;
    void   *pReq = mndBuildVDropStbReq(pMnode, pVgroup, pStb, &contLen);
    if (pReq == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_VND_DROP_STB;
    if (mndTransAppendUndoAction(pTrans, &action) != 0) {
      free(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      return -1;
    }
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndCreateStb(SMnode *pMnode, SNodeMsg *pReq, SMCreateStbReq *pCreate, SDbObj *pDb) {
  SStbObj stbObj = {0};
  memcpy(stbObj.name, pCreate->name, TSDB_TABLE_FNAME_LEN);
  memcpy(stbObj.db, pDb->name, TSDB_DB_FNAME_LEN);
  stbObj.createdTime = taosGetTimestampMs();
  stbObj.updateTime = stbObj.createdTime;
  stbObj.uid = mndGenerateUid(pCreate->name, TSDB_TABLE_FNAME_LEN);
  stbObj.dbUid = pDb->uid;
  stbObj.version = 1;
  stbObj.nextColId = 1;
  stbObj.numOfColumns = pCreate->numOfColumns;
  stbObj.numOfTags = pCreate->numOfTags;

  stbObj.pColumns = malloc(stbObj.numOfColumns * sizeof(SSchema));
  stbObj.pTags = malloc(stbObj.numOfTags * sizeof(SSchema));
  if (stbObj.pColumns == NULL || stbObj.pTags == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < stbObj.numOfColumns; ++i) {
    SField  *pField = taosArrayGet(pCreate->pColumns, i);
    SSchema *pSchema = &stbObj.pColumns[i];
    pSchema->type = pField->type;
    pSchema->bytes = pField->bytes;
    memcpy(pSchema->name, pField->name, TSDB_COL_NAME_LEN);
    pSchema->colId = stbObj.nextColId;
    stbObj.nextColId++;
  }

  for (int32_t i = 0; i < stbObj.numOfTags; ++i) {
    SField  *pField = taosArrayGet(pCreate->pTags, i);
    SSchema *pSchema = &stbObj.pTags[i];
    pSchema->type = pField->type;
    pSchema->bytes = pField->bytes;
    memcpy(pSchema->name, pField->name, TSDB_COL_NAME_LEN);
    pSchema->colId = stbObj.nextColId;
    stbObj.nextColId++;
  }

  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK, TRN_TYPE_CREATE_STB, &pReq->rpcMsg);
  if (pTrans == NULL) goto CREATE_STB_OVER;

  mDebug("trans:%d, used to create stb:%s", pTrans->id, pCreate->name);
  mndTransSetDbInfo(pTrans, pDb);

  if (mndSetCreateStbRedoLogs(pMnode, pTrans, pDb, &stbObj) != 0) goto CREATE_STB_OVER;
  if (mndSetCreateStbUndoLogs(pMnode, pTrans, pDb, &stbObj) != 0) goto CREATE_STB_OVER;
  if (mndSetCreateStbCommitLogs(pMnode, pTrans, pDb, &stbObj) != 0) goto CREATE_STB_OVER;
  if (mndSetCreateStbRedoActions(pMnode, pTrans, pDb, &stbObj) != 0) goto CREATE_STB_OVER;
  if (mndSetCreateStbUndoActions(pMnode, pTrans, pDb, &stbObj) != 0) goto CREATE_STB_OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto CREATE_STB_OVER;

  code = 0;

CREATE_STB_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessMCreateStbReq(SNodeMsg *pReq) {
  SMnode        *pMnode = pReq->pNode;
  int32_t        code = -1;
  SStbObj       *pTopicStb = NULL;
  SStbObj       *pStb = NULL;
  SDbObj        *pDb = NULL;
  SUserObj      *pUser = NULL;
  SMCreateStbReq createReq = {0};

  if (tDeserializeSMCreateStbReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto CREATE_STB_OVER;
  }

  mDebug("stb:%s, start to create", createReq.name);
  if (mndCheckCreateStbReq(&createReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto CREATE_STB_OVER;
  }

  pStb = mndAcquireStb(pMnode, createReq.name);
  if (pStb != NULL) {
    if (createReq.igExists) {
      mDebug("stb:%s, already exist, ignore exist is set", createReq.name);
      code = 0;
      goto CREATE_STB_OVER;
    } else {
      terrno = TSDB_CODE_MND_STB_ALREADY_EXIST;
      goto CREATE_STB_OVER;
    }
  } else if (terrno != TSDB_CODE_MND_STB_NOT_EXIST) {
    goto CREATE_STB_OVER;
  }

  pTopicStb = mndAcquireStb(pMnode, createReq.name);
  if (pTopicStb != NULL) {
    terrno = TSDB_CODE_MND_NAME_CONFLICT_WITH_TOPIC;
    goto CREATE_STB_OVER;
  }

  pDb = mndAcquireDbByStb(pMnode, createReq.name);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto CREATE_STB_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    goto CREATE_STB_OVER;
  }

  if (mndCheckWriteAuth(pUser, pDb) != 0) {
    goto CREATE_STB_OVER;
  }

  code = mndCreateStb(pMnode, pReq, &createReq, pDb);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

CREATE_STB_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("stb:%s, failed to create since %s", createReq.name, terrstr());
  }

  mndReleaseStb(pMnode, pStb);
  mndReleaseStb(pMnode, pTopicStb);
  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);
  tFreeSMCreateStbReq(&createReq);

  return code;
}

static int32_t mndProcessVCreateStbRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndCheckAlterStbReq(SMAltertbReq *pAlter) {
  if (pAlter->numOfFields < 1 || pAlter->numOfFields != (int32_t)taosArrayGetSize(pAlter->pFields)) {
    terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
    return -1;
  }

  for (int32_t i = 0; i < pAlter->numOfFields; ++i) {
    SField *pField = taosArrayGet(pAlter->pFields, i);

    if (pField->type <= 0) {
      terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
      return -1;
    }
    if (pField->bytes <= 0) {
      terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
      return -1;
    }
    if (pField->name[0] == 0) {
      terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
      return -1;
    }
  }

  return 0;
}

static int32_t mndFindSuperTableTagIndex(const SStbObj *pStb, const char *tagName) {
  for (int32_t tag = 0; tag < pStb->numOfTags; tag++) {
    if (strcasecmp(pStb->pTags[tag].name, tagName) == 0) {
      return tag;
    }
  }

  return -1;
}

static int32_t mndFindSuperTableColumnIndex(const SStbObj *pStb, const char *colName) {
  for (int32_t col = 0; col < pStb->numOfColumns; col++) {
    if (strcasecmp(pStb->pColumns[col].name, colName) == 0) {
      return col;
    }
  }

  return -1;
}

static int32_t mndAllocStbSchemas(const SStbObj *pOld, SStbObj *pNew) {
  pNew->pTags = calloc(pNew->numOfTags, sizeof(SSchema));
  pNew->pColumns = calloc(pNew->numOfColumns, sizeof(SSchema));
  if (pNew->pTags == NULL || pNew->pColumns == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  memcpy(pNew->pColumns, pOld->pColumns, sizeof(SSchema) * pOld->numOfColumns);
  memcpy(pNew->pTags, pOld->pTags, sizeof(SSchema) * pOld->numOfTags);
  return 0;
}

static int32_t mndAddSuperTableTag(const SStbObj *pOld, SStbObj *pNew, SArray *pFields, int32_t ntags) {
  if (pOld->numOfTags + ntags > TSDB_MAX_TAGS) {
    terrno = TSDB_CODE_MND_TOO_MANY_TAGS;
    return -1;
  }

  if (pOld->numOfColumns + ntags + pOld->numOfTags > TSDB_MAX_COLUMNS) {
    terrno = TSDB_CODE_MND_TOO_MANY_COLUMNS;
    return -1;
  }

  pNew->numOfTags = pNew->numOfTags + ntags;
  if (mndAllocStbSchemas(pOld, pNew) != 0) {
    return -1;
  }

  for (int32_t i = 0; i < ntags; i++) {
    SField *pField = taosArrayGet(pFields, i);
    if (mndFindSuperTableColumnIndex(pOld, pField->name) > 0) {
      terrno = TSDB_CODE_MND_COLUMN_ALREADY_EXIST;
      return -1;
    }

    if (mndFindSuperTableTagIndex(pOld, pField->name) > 0) {
      terrno = TSDB_CODE_MND_TAG_ALREADY_EXIST;
      return -1;
    }

    SSchema *pSchema = &pNew->pTags[pOld->numOfTags + i];
    pSchema->bytes = pField->bytes;
    pSchema->type = pField->type;
    memcpy(pSchema->name, pField->name, TSDB_COL_NAME_LEN);
    pSchema->colId = pNew->nextColId;
    pNew->nextColId++;

    mDebug("stb:%s, start to add tag %s", pNew->name, pSchema->name);
  }

  pNew->version++;
  return 0;
}

static int32_t mndDropSuperTableTag(const SStbObj *pOld, SStbObj *pNew, const char *tagName) {
  int32_t tag = mndFindSuperTableTagIndex(pOld, tagName);
  if (tag < 0) {
    terrno = TSDB_CODE_MND_TAG_NOT_EXIST;
    return -1;
  }

  if (mndAllocStbSchemas(pOld, pNew) != 0) {
    return -1;
  }

  memmove(pNew->pTags + tag, pNew->pTags + tag + 1, sizeof(SSchema) * (pNew->numOfTags - tag - 1));
  pNew->numOfTags--;

  pNew->version++;
  mDebug("stb:%s, start to drop tag %s", pNew->name, tagName);
  return 0;
}

static int32_t mndAlterStbTagName(const SStbObj *pOld, SStbObj *pNew, SArray *pFields) {
  if ((int32_t)taosArrayGetSize(pFields) != 2) {
    terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
    return -1;
  }

  SField *pField0 = taosArrayGet(pFields, 0);
  SField *pField1 = taosArrayGet(pFields, 1);

  const char *oldTagName = pField0->name;
  const char *newTagName = pField1->name;

  int32_t tag = mndFindSuperTableTagIndex(pOld, oldTagName);
  if (tag < 0) {
    terrno = TSDB_CODE_MND_TAG_NOT_EXIST;
    return -1;
  }

  if (mndFindSuperTableTagIndex(pOld, newTagName) >= 0) {
    terrno = TSDB_CODE_MND_TAG_ALREADY_EXIST;
    return -1;
  }

  if (mndFindSuperTableColumnIndex(pOld, newTagName) >= 0) {
    terrno = TSDB_CODE_MND_COLUMN_ALREADY_EXIST;
    return -1;
  }

  if (mndAllocStbSchemas(pOld, pNew) != 0) {
    return -1;
  }

  SSchema *pSchema = (SSchema *)(pNew->pTags + tag);
  memcpy(pSchema->name, newTagName, TSDB_COL_NAME_LEN);

  pNew->version++;
  mDebug("stb:%s, start to modify tag %s to %s", pNew->name, oldTagName, newTagName);
  return 0;
}

static int32_t mndAlterStbTagBytes(const SStbObj *pOld, SStbObj *pNew, const SField *pField) {
  int32_t tag = mndFindSuperTableTagIndex(pOld, pField->name);
  if (tag < 0) {
    terrno = TSDB_CODE_MND_TAG_NOT_EXIST;
    return -1;
  }

  if (mndAllocStbSchemas(pOld, pNew) != 0) {
    return -1;
  }

  SSchema *pTag = pNew->pTags + tag;

  if (!(pTag->type == TSDB_DATA_TYPE_BINARY || pTag->type == TSDB_DATA_TYPE_NCHAR)) {
    terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
    return -1;
  }

  if (pField->bytes <= pTag->bytes) {
    terrno = TSDB_CODE_MND_INVALID_ROW_BYTES;
    return -1;
  }

  pTag->bytes = pField->bytes;
  pNew->version++;

  mDebug("stb:%s, start to modify tag len %s to %d", pNew->name, pField->name, pField->bytes);
  return 0;
}

static int32_t mndAddSuperTableColumn(const SStbObj *pOld, SStbObj *pNew, SArray *pFields, int32_t ncols) {
  if (pOld->numOfColumns + ncols + pOld->numOfTags > TSDB_MAX_COLUMNS) {
    terrno = TSDB_CODE_MND_TOO_MANY_COLUMNS;
    return -1;
  }

  pNew->numOfColumns = pNew->numOfColumns + ncols;
  if (mndAllocStbSchemas(pOld, pNew) != 0) {
    return -1;
  }

  for (int32_t i = 0; i < ncols; i++) {
    SField *pField = taosArrayGet(pFields, i);
    if (mndFindSuperTableColumnIndex(pOld, pField->name) > 0) {
      terrno = TSDB_CODE_MND_COLUMN_ALREADY_EXIST;
      return -1;
    }

    if (mndFindSuperTableTagIndex(pOld, pField->name) > 0) {
      terrno = TSDB_CODE_MND_TAG_ALREADY_EXIST;
      return -1;
    }

    SSchema *pSchema = &pNew->pColumns[pOld->numOfColumns + i];
    pSchema->bytes = pField->bytes;
    pSchema->type = pField->type;
    memcpy(pSchema->name, pField->name, TSDB_COL_NAME_LEN);
    pSchema->colId = pNew->nextColId;
    pNew->nextColId++;

    mDebug("stb:%s, start to add column %s", pNew->name, pSchema->name);
  }

  pNew->version++;
  return 0;
}

static int32_t mndDropSuperTableColumn(const SStbObj *pOld, SStbObj *pNew, const char *colName) {
  int32_t col = mndFindSuperTableColumnIndex(pOld, colName);
  if (col < 0) {
    terrno = TSDB_CODE_MND_COLUMN_NOT_EXIST;
    return -1;
  }

  if (col == 0) {
    terrno = TSDB_CODE_MND_INVALID_STB_ALTER_OPTION;
    return -1;
  }

  if (pOld->numOfColumns == 2) {
    terrno = TSDB_CODE_MND_INVALID_STB_ALTER_OPTION;
    return -1;
  }

  if (mndAllocStbSchemas(pOld, pNew) != 0) {
    return -1;
  }

  memmove(pNew->pColumns + col, pNew->pColumns + col + 1, sizeof(SSchema) * (pNew->numOfColumns - col - 1));
  pNew->numOfColumns--;

  pNew->version++;
  mDebug("stb:%s, start to drop col %s", pNew->name, colName);
  return 0;
}

static int32_t mndAlterStbColumnBytes(const SStbObj *pOld, SStbObj *pNew, const SField *pField) {
  int32_t col = mndFindSuperTableColumnIndex(pOld, pField->name);
  if (col < 0) {
    terrno = TSDB_CODE_MND_COLUMN_NOT_EXIST;
    return -1;
  }

  uint32_t nLen = 0;
  for (int32_t i = 0; i < pOld->numOfColumns; ++i) {
    nLen += (pOld->pColumns[i].colId == col) ? pField->bytes : pOld->pColumns[i].bytes;
  }

  if (nLen > TSDB_MAX_BYTES_PER_ROW) {
    terrno = TSDB_CODE_MND_INVALID_ROW_BYTES;
    return -1;
  }

  if (mndAllocStbSchemas(pOld, pNew) != 0) {
    return -1;
  }

  SSchema *pCol = pNew->pColumns + col;
  if (!(pCol->type == TSDB_DATA_TYPE_BINARY || pCol->type == TSDB_DATA_TYPE_NCHAR)) {
    terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
    return -1;
  }

  if (pField->bytes <= pCol->bytes) {
    terrno = TSDB_CODE_MND_INVALID_ROW_BYTES;
    return -1;
  }

  pCol->bytes = pField->bytes;
  pNew->version++;

  mDebug("stb:%s, start to modify col len %s to %d", pNew->name, pField->name, pField->bytes);
  return 0;
}

static int32_t mndSetAlterStbRedoLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  SSdbRaw *pRedoRaw = mndStbActionEncode(pStb);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_UPDATING) != 0) return -1;

  return 0;
}

static int32_t mndSetAlterStbCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  SSdbRaw *pCommitRaw = mndStbActionEncode(pStb);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY) != 0) return -1;

  return 0;
}

static int32_t mndSetAlterStbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;
  int32_t contLen;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pDb->uid) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    void *pReq = mndBuildVCreateStbReq(pMnode, pVgroup, pStb, &contLen);
    if (pReq == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_VND_ALTER_STB;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      free(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      return -1;
    }
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndAlterStb(SMnode *pMnode, SNodeMsg *pReq, const SMAltertbReq *pAlter, SDbObj *pDb, SStbObj *pOld) {
  SStbObj stbObj = {0};
  taosRLockLatch(&pOld->lock);
  memcpy(&stbObj, pOld, sizeof(SStbObj));
  stbObj.pColumns = NULL;
  stbObj.pTags = NULL;
  stbObj.updateTime = taosGetTimestampMs();
  taosRUnLockLatch(&pOld->lock);

  int32_t code = -1;
  STrans *pTrans = NULL;
  SField *pField0 = taosArrayGet(pAlter->pFields, 0);

  switch (pAlter->alterType) {
    case TSDB_ALTER_TABLE_ADD_TAG:
      code = mndAddSuperTableTag(pOld, &stbObj, pAlter->pFields, pAlter->numOfFields);
      break;
    case TSDB_ALTER_TABLE_DROP_TAG:
      code = mndDropSuperTableTag(pOld, &stbObj, pField0->name);
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_NAME:
      code = mndAlterStbTagName(pOld, &stbObj, pAlter->pFields);
      break;
    case TSDB_ALTER_TABLE_UPDATE_TAG_BYTES:
      code = mndAlterStbTagBytes(pOld, &stbObj, pField0);
      break;
    case TSDB_ALTER_TABLE_ADD_COLUMN:
      code = mndAddSuperTableColumn(pOld, &stbObj, pAlter->pFields, pAlter->numOfFields);
      break;
    case TSDB_ALTER_TABLE_DROP_COLUMN:
      code = mndDropSuperTableColumn(pOld, &stbObj, pField0->name);
      break;
    case TSDB_ALTER_TABLE_UPDATE_COLUMN_BYTES:
      code = mndAlterStbColumnBytes(pOld, &stbObj, pField0);
      break;
    default:
      terrno = TSDB_CODE_MND_INVALID_STB_OPTION;
      break;
  }

  if (code != 0) goto ALTER_STB_OVER;

  code = -1;
  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_TYPE_ALTER_STB, &pReq->rpcMsg);
  if (pTrans == NULL) goto ALTER_STB_OVER;

  mDebug("trans:%d, used to alter stb:%s", pTrans->id, pAlter->name);
  mndTransSetDbInfo(pTrans, pDb);

  if (mndSetAlterStbRedoLogs(pMnode, pTrans, pDb, &stbObj) != 0) goto ALTER_STB_OVER;
  if (mndSetAlterStbCommitLogs(pMnode, pTrans, pDb, &stbObj) != 0) goto ALTER_STB_OVER;
  if (mndSetAlterStbRedoActions(pMnode, pTrans, pDb, &stbObj) != 0) goto ALTER_STB_OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto ALTER_STB_OVER;

  code = 0;

ALTER_STB_OVER:
  mndTransDrop(pTrans);
  tfree(stbObj.pTags);
  tfree(stbObj.pColumns);
  return code;
}

static int32_t mndProcessMAlterStbReq(SNodeMsg *pReq) {
  SMnode      *pMnode = pReq->pNode;
  int32_t      code = -1;
  SDbObj      *pDb = NULL;
  SStbObj     *pStb = NULL;
  SUserObj    *pUser = NULL;
  SMAltertbReq alterReq = {0};

  if (tDeserializeSMAlterStbReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &alterReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto ALTER_STB_OVER;
  }

  mDebug("stb:%s, start to alter", alterReq.name);
  if (mndCheckAlterStbReq(&alterReq) != 0) goto ALTER_STB_OVER;

  pDb = mndAcquireDbByStb(pMnode, alterReq.name);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_INVALID_DB;
    goto ALTER_STB_OVER;
  }

  pStb = mndAcquireStb(pMnode, alterReq.name);
  if (pStb == NULL) {
    terrno = TSDB_CODE_MND_STB_NOT_EXIST;
    goto ALTER_STB_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    goto ALTER_STB_OVER;
  }

  if (mndCheckWriteAuth(pUser, pDb) != 0) {
    goto ALTER_STB_OVER;
  }

  code = mndAlterStb(pMnode, pReq, &alterReq, pDb, pStb);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

ALTER_STB_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("stb:%s, failed to alter since %s", alterReq.name, terrstr());
  }

  mndReleaseStb(pMnode, pStb);
  mndReleaseDb(pMnode, pDb);
  mndReleaseUser(pMnode, pUser);
  taosArrayDestroy(alterReq.pFields);

  return code;
}

static int32_t mndProcessVAlterStbRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndSetDropStbRedoLogs(SMnode *pMnode, STrans *pTrans, SStbObj *pStb) {
  SSdbRaw *pRedoRaw = mndStbActionEncode(pStb);
  if (pRedoRaw == NULL) return -1;
  if (mndTransAppendRedolog(pTrans, pRedoRaw) != 0) return -1;
  if (sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING) != 0) return -1;

  return 0;
}

static int32_t mndSetDropStbCommitLogs(SMnode *pMnode, STrans *pTrans, SStbObj *pStb) {
  SSdbRaw *pCommitRaw = mndStbActionEncode(pStb);
  if (pCommitRaw == NULL) return -1;
  if (mndTransAppendCommitlog(pTrans, pCommitRaw) != 0) return -1;
  if (sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED) != 0) return -1;

  return 0;
}

static int32_t mndSetDropStbRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;
  int32_t contLen;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->dbUid != pDb->uid) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    int32_t contLen = 0;
    void   *pReq = mndBuildVDropStbReq(pMnode, pVgroup, pStb, &contLen);
    if (pReq == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }

    STransAction action = {0};
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_VND_DROP_STB;
    action.acceptableCode = TSDB_CODE_VND_TB_NOT_EXIST;
    if (mndTransAppendRedoAction(pTrans, &action) != 0) {
      free(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      return -1;
    }
    sdbRelease(pSdb, pVgroup);
  }

  return 0;
}

static int32_t mndDropStb(SMnode *pMnode, SNodeMsg *pReq, SDbObj *pDb, SStbObj *pStb) {
  int32_t code = -1;
  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_ROLLBACK,TRN_TYPE_DROP_STB, &pReq->rpcMsg);
  if (pTrans == NULL) goto DROP_STB_OVER;

  mDebug("trans:%d, used to drop stb:%s", pTrans->id, pStb->name);
  mndTransSetDbInfo(pTrans, pDb);

  if (mndSetDropStbRedoLogs(pMnode, pTrans, pStb) != 0) goto DROP_STB_OVER;
  if (mndSetDropStbCommitLogs(pMnode, pTrans, pStb) != 0) goto DROP_STB_OVER;
  if (mndSetDropStbRedoActions(pMnode, pTrans, pDb, pStb) != 0) goto DROP_STB_OVER;
  if (mndTransPrepare(pMnode, pTrans) != 0) goto DROP_STB_OVER;

  code = 0;

DROP_STB_OVER:
  mndTransDrop(pTrans);
  return code;
}

static int32_t mndProcessMDropStbReq(SNodeMsg *pReq) {
  SMnode      *pMnode = pReq->pNode;
  int32_t      code = -1;
  SUserObj    *pUser = NULL;
  SDbObj      *pDb = NULL;
  SStbObj     *pStb = NULL;
  SMDropStbReq dropReq = {0};

  if (tDeserializeSMDropStbReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &dropReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto DROP_STB_OVER;
  }

  mDebug("stb:%s, start to drop", dropReq.name);

  pStb = mndAcquireStb(pMnode, dropReq.name);
  if (pStb == NULL) {
    if (dropReq.igNotExists) {
      mDebug("stb:%s, not exist, ignore not exist is set", dropReq.name);
      code = 0;
      goto DROP_STB_OVER;
    } else {
      terrno = TSDB_CODE_MND_STB_NOT_EXIST;
      goto DROP_STB_OVER;
    }
  }

  pDb = mndAcquireDbByStb(pMnode, dropReq.name);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto DROP_STB_OVER;
  }

  pUser = mndAcquireUser(pMnode, pReq->user);
  if (pUser == NULL) {
    goto DROP_STB_OVER;
  }

  if (mndCheckWriteAuth(pUser, pDb) != 0) {
    goto DROP_STB_OVER;
  }

  code = mndDropStb(pMnode, pReq, pDb, pStb);
  if (code == 0) code = TSDB_CODE_MND_ACTION_IN_PROGRESS;

DROP_STB_OVER:
  if (code != 0 && code != TSDB_CODE_MND_ACTION_IN_PROGRESS) {
    mError("stb:%s, failed to drop since %s", dropReq.name, terrstr());
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseStb(pMnode, pStb);
  mndReleaseUser(pMnode, pUser);

  return code;
}

static int32_t mndProcessVDropStbRsp(SNodeMsg *pRsp) {
  mndTransProcessRsp(pRsp);
  return 0;
}

static int32_t mndBuildStbSchemaImp(SDbObj *pDb, SStbObj *pStb, const char *tbName, STableMetaRsp *pRsp) {
  taosRLockLatch(&pStb->lock);

  int32_t totalCols = pStb->numOfColumns + pStb->numOfTags;
  pRsp->pSchemas = calloc(totalCols, sizeof(SSchema));
  if (pRsp->pSchemas == NULL) {
    taosRUnLockLatch(&pStb->lock);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  strcpy(pRsp->dbFName, pStb->db);
  strcpy(pRsp->tbName, tbName);
  strcpy(pRsp->stbName, tbName);
  pRsp->dbId = pDb->uid;
  pRsp->numOfTags = pStb->numOfTags;
  pRsp->numOfColumns = pStb->numOfColumns;
  pRsp->precision = pDb->cfg.precision;
  pRsp->tableType = TSDB_SUPER_TABLE;
  pRsp->update = pDb->cfg.update;
  pRsp->sversion = pStb->version;
  pRsp->suid = pStb->uid;
  pRsp->tuid = pStb->uid;

  for (int32_t i = 0; i < pStb->numOfColumns; ++i) {
    SSchema *pSchema = &pRsp->pSchemas[i];
    SSchema *pSrcSchema = &pStb->pColumns[i];
    memcpy(pSchema->name, pSrcSchema->name, TSDB_COL_NAME_LEN);
    pSchema->type = pSrcSchema->type;
    pSchema->colId = pSrcSchema->colId;
    pSchema->bytes = pSrcSchema->bytes;
  }

  for (int32_t i = 0; i < pStb->numOfTags; ++i) {
    SSchema *pSchema = &pRsp->pSchemas[i + pStb->numOfColumns];
    SSchema *pSrcSchema = &pStb->pTags[i];
    memcpy(pSchema->name, pSrcSchema->name, TSDB_COL_NAME_LEN);
    pSchema->type = pSrcSchema->type;
    pSchema->colId = pSrcSchema->colId;
    pSchema->bytes = pSrcSchema->bytes;
  }

  taosRUnLockLatch(&pStb->lock);
  return 0;
}

static int32_t mndBuildStbSchema(SMnode *pMnode, const char *dbFName, const char *tbName, STableMetaRsp *pRsp) {
  char tbFName[TSDB_TABLE_FNAME_LEN] = {0};
  snprintf(tbFName, sizeof(tbFName), "%s.%s", dbFName, tbName);

  SDbObj *pDb = mndAcquireDb(pMnode, dbFName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    return -1;
  }

  SStbObj *pStb = mndAcquireStb(pMnode, tbFName);
  if (pStb == NULL) {
    mndReleaseDb(pMnode, pDb);
    terrno = TSDB_CODE_MND_INVALID_STB;
    return -1;
  }

  int32_t code = mndBuildStbSchemaImp(pDb, pStb, tbName, pRsp);
  mndReleaseDb(pMnode, pDb);
  mndReleaseStb(pMnode, pStb);
  return code;
}

static int32_t mndProcessTableMetaReq(SNodeMsg *pReq) {
  SMnode       *pMnode = pReq->pNode;
  int32_t       code = -1;
  STableInfoReq infoReq = {0};
  STableMetaRsp metaRsp = {0};

  if (tDeserializeSTableInfoReq(pReq->rpcMsg.pCont, pReq->rpcMsg.contLen, &infoReq) != 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto RETRIEVE_META_OVER;
  }

  if (0 == strcmp(infoReq.dbFName, TSDB_INFORMATION_SCHEMA_DB)) {
    mDebug("information_schema table:%s.%s, start to retrieve meta", infoReq.dbFName, infoReq.tbName);
    if (mndBuildInsTableSchema(pMnode, infoReq.dbFName, infoReq.tbName, &metaRsp) != 0) {
      goto RETRIEVE_META_OVER;
    }
  } else {
    mDebug("stb:%s.%s, start to retrieve meta", infoReq.dbFName, infoReq.tbName);
    if (mndBuildStbSchema(pMnode, infoReq.dbFName, infoReq.tbName, &metaRsp) != 0) {
      goto RETRIEVE_META_OVER;
    }
  }

  int32_t rspLen = tSerializeSTableMetaRsp(NULL, 0, &metaRsp);
  if (rspLen < 0) {
    terrno = TSDB_CODE_INVALID_MSG;
    goto RETRIEVE_META_OVER;
  }

  void *pRsp = rpcMallocCont(rspLen);
  if (pRsp == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    goto RETRIEVE_META_OVER;
  }

  tSerializeSTableMetaRsp(pRsp, rspLen, &metaRsp);
  pReq->pRsp = pRsp;
  pReq->rspLen = rspLen;
  code = 0;

  mDebug("stb:%s.%s, meta is retrieved", infoReq.dbFName, infoReq.tbName);

RETRIEVE_META_OVER:
  if (code != 0) {
    mError("stb:%s.%s, failed to retrieve meta since %s", infoReq.dbFName, infoReq.tbName, terrstr());
  }

  tFreeSTableMetaRsp(&metaRsp);
  return code;
}

int32_t mndValidateStbInfo(SMnode *pMnode, SSTableMetaVersion *pStbVersions, int32_t numOfStbs, void **ppRsp,
                           int32_t *pRspLen) {
  STableMetaBatchRsp batchMetaRsp = {0};
  batchMetaRsp.pArray = taosArrayInit(numOfStbs, sizeof(STableMetaRsp));
  if (batchMetaRsp.pArray == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int32_t i = 0; i < numOfStbs; ++i) {
    SSTableMetaVersion *pStbVersion = &pStbVersions[i];
    pStbVersion->suid = be64toh(pStbVersion->suid);
    pStbVersion->sversion = ntohs(pStbVersion->sversion);
    pStbVersion->tversion = ntohs(pStbVersion->tversion);

    STableMetaRsp metaRsp = {0};
    mDebug("stb:%s.%s, start to retrieve meta", pStbVersion->dbFName, pStbVersion->stbName);
    if (mndBuildStbSchema(pMnode, pStbVersion->dbFName, pStbVersion->stbName, &metaRsp) != 0) {
      metaRsp.numOfColumns = -1;
      metaRsp.suid = pStbVersion->suid;
    }

    if (pStbVersion->sversion != metaRsp.sversion) {
      taosArrayPush(batchMetaRsp.pArray, &metaRsp);
    }
  }

  int32_t rspLen = tSerializeSTableMetaBatchRsp(NULL, 0, &batchMetaRsp);
  if (rspLen < 0) {
    tFreeSTableMetaBatchRsp(&batchMetaRsp);
    terrno = TSDB_CODE_INVALID_MSG;
    return -1;
  }

  void *pRsp = malloc(rspLen);
  if (pRsp == NULL) {
    tFreeSTableMetaBatchRsp(&batchMetaRsp);
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  tSerializeSTableMetaBatchRsp(pRsp, rspLen, &batchMetaRsp);
  *ppRsp = pRsp;
  *pRspLen = rspLen;
  return 0;
}

static int32_t mndGetNumOfStbs(SMnode *pMnode, char *dbName, int32_t *pNumOfStbs) {
  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    terrno = TSDB_CODE_MND_DB_NOT_SELECTED;
    return -1;
  }

  int32_t numOfStbs = 0;
  void   *pIter = NULL;
  while (1) {
    SStbObj *pStb = NULL;
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
    if (pIter == NULL) break;

    if (pStb->dbUid == pDb->uid) {
      numOfStbs++;
    }

    sdbRelease(pSdb, pStb);
  }

  *pNumOfStbs = numOfStbs;
  mndReleaseDb(pMnode, pDb);
  return 0;
}

static int32_t mndGetStbMeta(SNodeMsg *pReq, SShowObj *pShow, STableMetaRsp *pMeta) {
  SMnode *pMnode = pReq->pNode;
  SSdb   *pSdb = pMnode->pSdb;

  if (mndGetNumOfStbs(pMnode, pShow->db, &pShow->numOfRows) != 0) {
    return -1;
  }

  int32_t  cols = 0;
  SSchema *pSchema = pMeta->pSchemas;

  pShow->bytes[cols] = TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE;
  pSchema[cols].type = TSDB_DATA_TYPE_BINARY;
  strcpy(pSchema[cols].name, "name");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 8;
  pSchema[cols].type = TSDB_DATA_TYPE_TIMESTAMP;
  strcpy(pSchema[cols].name, "create_time");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "columns");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pShow->bytes[cols] = 4;
  pSchema[cols].type = TSDB_DATA_TYPE_INT;
  strcpy(pSchema[cols].name, "tags");
  pSchema[cols].bytes = pShow->bytes[cols];
  cols++;

  pMeta->numOfColumns = cols;
  pShow->numOfColumns = cols;

  pShow->offset[0] = 0;
  for (int32_t i = 1; i < cols; ++i) {
    pShow->offset[i] = pShow->offset[i - 1] + pShow->bytes[i - 1];
  }

  pShow->numOfRows = sdbGetSize(pSdb, SDB_STB);
  pShow->rowSize = pShow->offset[cols - 1] + pShow->bytes[cols - 1];
  strcpy(pMeta->tbName, mndShowStr(pShow->type));

  return 0;
}

static void mndExtractTableName(char *tableId, char *name) {
  int32_t pos = -1;
  int32_t num = 0;
  for (pos = 0; tableId[pos] != 0; ++pos) {
    if (tableId[pos] == TS_PATH_DELIMITER[0]) num++;
    if (num == 2) break;
  }

  if (num == 2) {
    strcpy(name, tableId + pos + 1);
  }
}

static int32_t mndRetrieveStb(SNodeMsg *pReq, SShowObj *pShow, char *data, int32_t rows) {
  SMnode  *pMnode = pReq->pNode;
  SSdb    *pSdb = pMnode->pSdb;
  int32_t  numOfRows = 0;
  SStbObj *pStb = NULL;
  int32_t  cols = 0;
  char    *pWrite;
  char     prefix[TSDB_DB_FNAME_LEN] = {0};

  SDbObj* pDb = NULL;
  if (strlen(pShow->db) > 0) {
    pDb = mndAcquireDb(pMnode, pShow->db);
    if (pDb == NULL) return 0;
  }

  tstrncpy(prefix, pShow->db, TSDB_DB_FNAME_LEN);
  strcat(prefix, TS_PATH_DELIMITER);
  int32_t prefixLen = (int32_t)strlen(prefix);

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_STB, pShow->pIter, (void **)&pStb);
    if (pShow->pIter == NULL) break;

    if (pDb != NULL && pStb->dbUid != pDb->uid) {
      if (strncmp(pStb->db, pDb->name, prefixLen) == 0) {
        mError("Inconsistent table data, name:%s, db:%s, dbUid:%" PRIu64, pStb->name, pDb->name, pDb->uid);
      }

      sdbRelease(pSdb, pStb);
      continue;
    }

    cols = 0;

    SName name = {0};
    char stbName[TSDB_TABLE_NAME_LEN] = {0};
    mndExtractTableName(pStb->name, stbName);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, stbName);
    cols++;

    char  db[TSDB_DB_NAME_LEN] = {0};
    tNameFromString(&name, pStb->db, T_NAME_ACCT|T_NAME_DB);
    tNameGetDbName(&name, db);
    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, db);
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pStb->createdTime;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pStb->numOfColumns;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = pStb->numOfTags;
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int32_t *)pWrite = 0; // number of tables
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    *(int64_t *)pWrite = pStb->updateTime; // number of tables
    cols++;

    pWrite = data + pShow->offset[cols] * rows + pShow->bytes[cols] * numOfRows;
    STR_TO_VARSTR(pWrite, pStb->comment);
    cols++;

    numOfRows++;
    sdbRelease(pSdb, pStb);
  }

  if (pDb != NULL) {
    mndReleaseDb(pMnode, pDb);
  }

  pShow->numOfReads += numOfRows;
  mndVacuumResult(data, pShow->numOfColumns, numOfRows, rows, pShow);
  return numOfRows;
}

static void mndCancelGetNextStb(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetch(pSdb, pIter);
}
