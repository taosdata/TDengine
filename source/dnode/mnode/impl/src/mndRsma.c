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
#include "functionMgt.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndInfoSchema.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndSma.h"
#include "mndStb.h"
#include "mndTrans.h"
#include "mndUser.h"
#include "parser.h"
#include "tname.h"

#define MND_RSMA_VER_NUMBER   1
#define MND_RSMA_RESERVE_SIZE 64

static SSdbRaw *mndRsmaActionEncode(SRsmaObj *pSma);
static SSdbRow *mndRsmaActionDecode(SSdbRaw *pRaw);
static int32_t  mndRsmaActionInsert(SSdb *pSdb, SRsmaObj *pSma);
static int32_t  mndRsmaActionDelete(SSdb *pSdb, SRsmaObj *pSpSmatb);
static int32_t  mndRsmaActionUpdate(SSdb *pSdb, SRsmaObj *pOld, SRsmaObj *pNew);
static int32_t  mndProcessCreateRsmaReq(SRpcMsg *pReq);
static int32_t  mndProcessDropRsmaReq(SRpcMsg *pReq);

static int32_t mndRetrieveRsma(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelRetrieveRsma(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveRsmaTask(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelRetrieveRsmaTask(SMnode *pMnode, void *pIter);

int32_t mndInitRsma(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_RSMA,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndRsmaActionEncode,
      .decodeFp = (SdbDecodeFp)mndRsmaActionDecode,
      .insertFp = (SdbInsertFp)mndRsmaActionInsert,
      .updateFp = (SdbUpdateFp)mndRsmaActionUpdate,
      .deleteFp = (SdbDeleteFp)mndRsmaActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_RSMA, mndProcessCreateRsmaReq);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_RSMA, mndProcessDropRsmaReq);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_RSMAS, mndRetrieveRsma);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_RSMAS, mndCancelRetrieveRsma);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_RSMA_TASKS, mndRetrieveRsmaTask);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_RSMA_TASKS, mndCancelRetrieveRsmaTask);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupRsma(SMnode *pMnode) {}

void mndRsmaFreeObj(SRsmaObj *pObj) {
  if (pObj) {
    taosMemoryFreeClear(pObj->funcColIds);
    taosMemoryFreeClear(pObj->funcIds);
  }
}

static int32_t tSerializeSRsmaObj(void *buf, int32_t bufLen, const SRsmaObj *pObj) {
  int32_t  code = 0, lino = 0;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->name));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->tbname));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->db));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->createUser));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->createdTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->updateTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->uid));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->tbUid));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->dbUid));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->interval[0]));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->interval[1]));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pObj->tbType));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pObj->intervalUnit));
  TAOS_CHECK_EXIT(tEncodeI16v(&encoder, pObj->nFuncs));
  for (int16_t i = 0; i < pObj->nFuncs; ++i) {
    TAOS_CHECK_EXIT(tEncodeI16v(&encoder, pObj->funcColIds[i]));
    TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pObj->funcIds[i]));
  }

  tEndEncode(&encoder);

  tlen = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  if (code < 0) {
    mError("rsma, %s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }

  return tlen;
}

static int32_t tDeserializeSRsmaObj(void *buf, int32_t bufLen, SRsmaObj *pObj) {
  int32_t  code = 0, lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->name));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->tbname));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->db));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->createUser));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->createdTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->updateTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->uid));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->tbUid));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->dbUid));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->interval[0]));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->interval[1]));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pObj->tbType));
  TAOS_CHECK_EXIT(tDecodeI8(&decoder, &pObj->intervalUnit));
  TAOS_CHECK_EXIT(tDecodeI16v(&decoder, &pObj->nFuncs));
  if (pObj->nFuncs > 0) {
    if (!(pObj->funcColIds = taosMemoryMalloc(sizeof(col_id_t) * pObj->nFuncs))) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    if (!(pObj->funcIds = taosMemoryMalloc(sizeof(int32_t) * pObj->nFuncs))) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    for (int16_t i = 0; i < pObj->nFuncs; ++i) {
      TAOS_CHECK_EXIT(tDecodeI16v(&decoder, &pObj->funcColIds[i]));
      TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pObj->funcIds[i]));
    }
  }

_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("rsma, %s failed at line %d since %s, row:%p", __func__, lino, tstrerror(code), pObj);
  }
  TAOS_RETURN(code);
}

static SSdbRaw *mndRsmaActionEncode(SRsmaObj *pObj) {
  int32_t  code = 0, lino = 0;
  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;
  int32_t  tlen = tSerializeSRsmaObj(NULL, 0, pObj);
  if (tlen < 0) {
    TAOS_CHECK_EXIT(tlen);
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_RSMA, MND_RSMA_VER_NUMBER, size);
  if (pRaw == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  tlen = tSerializeSRsmaObj(buf, tlen, pObj);
  if (tlen < 0) {
    TAOS_CHECK_EXIT(tlen);
  }

  int32_t dataPos = 0;
  SDB_SET_INT32(pRaw, dataPos, tlen, _exit);
  SDB_SET_BINARY(pRaw, dataPos, buf, tlen, _exit);
  SDB_SET_DATALEN(pRaw, dataPos, _exit);

_exit:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    mError("rsma, failed at line %d to encode to raw:%p since %s", lino, pRaw, tstrerror(code));
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("rsma, encode to raw:%p, row:%p", pRaw, pObj);
  return pRaw;
}

SSdbRow *mndRsmaActionDecode(SSdbRaw *pRaw) {
  int32_t   code = 0, lino = 0;
  SSdbRow  *pRow = NULL;
  SRsmaObj *pObj = NULL;
  void     *buf = NULL;

  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) {
    goto _exit;
  }

  if (sver != MND_RSMA_VER_NUMBER) {
    code = TSDB_CODE_SDB_INVALID_DATA_VER;
    mError("rsma read invalid ver, data ver: %d, curr ver: %d", sver, MND_RSMA_VER_NUMBER);
    goto _exit;
  }

  if (!(pRow = sdbAllocRow(sizeof(SRsmaObj)))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  if (!(pObj = sdbGetRowObj(pRow))) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, _exit);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, _exit);

  if (tDeserializeSRsmaObj(buf, tlen, pObj) < 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  taosInitRWLatch(&pObj->lock);

_exit:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    mError("rsma, failed at line %d to decode from raw:%p since %s", lino, pRaw, tstrerror(code));
    mndRsmaFreeObj(pObj);
    taosMemoryFreeClear(pRow);
    return NULL;
  }
  mTrace("rsma, decode from raw:%p, row:%p", pRaw, pObj);
  return pRow;
}

static int32_t mndRsmaActionInsert(SSdb *pSdb, SRsmaObj *pObj) {
  mTrace("rsma:%s, perform insert action, row:%p", pObj->name, pObj);
  return 0;
}

static int32_t mndRsmaActionDelete(SSdb *pSdb, SRsmaObj *pObj) {
  mTrace("rsma:%s, perform delete action, row:%p", pObj->name, pObj);
  mndRsmaFreeObj(pObj);
  return 0;
}

static int32_t mndRsmaActionUpdate(SSdb *pSdb, SRsmaObj *pOld, SRsmaObj *pNew) {
  mTrace("rsma:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  pOld->nFuncs = pNew->nFuncs;
  TSWAP(pOld->funcColIds, pNew->funcColIds);
  TSWAP(pOld->funcIds, pNew->funcIds);
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

SRsmaObj *mndAcquireRsma(SMnode *pMnode, char *name) {
  SSdb     *pSdb = pMnode->pSdb;
  SRsmaObj *pObj = sdbAcquire(pSdb, SDB_RSMA, name);
  if (pObj == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      terrno = TSDB_CODE_MND_RSMA_NOT_EXIST;
    } else if (terrno == TSDB_CODE_SDB_OBJ_CREATING) {
      terrno = TSDB_CODE_MND_RSMA_IN_CREATING;
    } else if (terrno == TSDB_CODE_SDB_OBJ_DROPPING) {
      terrno = TSDB_CODE_MND_RSMA_IN_DROPPING;
    } else {
      terrno = TSDB_CODE_APP_ERROR;
      mFatal("rsma:%s, failed to acquire rsma since %s", name, terrstr());
    }
  }
  return pObj;
}

void mndReleaseRsma(SMnode *pMnode, SRsmaObj *pSma) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pSma);
}

static int32_t mndSetCreateRsmaRedoLogs(SMnode *pMnode, STrans *pTrans, SRsmaObj *pSma) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndRsmaActionEncode(pSma);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));

  TAOS_RETURN(code);
}

static int32_t mndSetCreateRsmaUndoLogs(SMnode *pMnode, STrans *pTrans, SRsmaObj *pSma) {
  int32_t  code = 0;
  SSdbRaw *pUndoRaw = mndRsmaActionEncode(pSma);
  if (!pUndoRaw) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pUndoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateRsmaCommitLogs(SMnode *pMnode, STrans *pTrans, SRsmaObj *pSma) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndRsmaActionEncode(pSma);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));

  TAOS_RETURN(code);
}

static int32_t mndSetDropRsmaRedoLogs(SMnode *pMnode, STrans *pTrans, SRsmaObj *pSma) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndRsmaActionEncode(pSma);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    return -1;
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));

  return 0;
}

static int32_t mndSetDropRsmaCommitLogs(SMnode *pMnode, STrans *pTrans, SRsmaObj *pSma) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndRsmaActionEncode(pSma);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    return -1;
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));

  return 0;
}

static int32_t mndDropRsma(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SRsmaObj *pSma) {
  int32_t code = -1;
#if 0
  SVgObj     *pVgroup = NULL;
  SStbObj    *pStb = NULL;
  STrans     *pTrans = NULL;
  SStreamObj *pStream = NULL;

  pVgroup = mndAcquireVgroup(pMnode, pSma->dstVgId);
  if (pVgroup == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  pStb = mndAcquireStb(pMnode, pSma->stb);
  if (pStb == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB, pReq, "drop-sma");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _OVER;
  }

  mInfo("trans:%d, used to drop sma:%s", pTrans->id, pSma->name);
  mndTransSetDbName(pTrans, pDb->name, NULL);
  TAOS_CHECK_GOTO(mndTransCheckConflict(pMnode, pTrans), NULL, _OVER);

  mndTransSetSerial(pTrans);

  char streamName[TSDB_TABLE_FNAME_LEN] = {0};
  code = mndGetStreamNameFromSmaName(streamName, pSma->name);
  if (TSDB_CODE_SUCCESS != code) {
    goto _OVER;
  }

  code = mndAcquireStream(pMnode, streamName, &pStream);
  if (pStream == NULL || pStream->pCreate->streamId != pSma->uid || code != 0) {
    sdbRelease(pMnode->pSdb, pStream);
    goto _OVER;
  } else {
    // drop stream
    if ((code = mndStreamTransAppend(pStream, pTrans, SDB_STATUS_DROPPED)) < 0) {
      mError("stream:%s, failed to drop log since %s", pStream->pCreate->name, tstrerror(code));
      sdbRelease(pMnode->pSdb, pStream);
      goto _OVER;
    }
  }
  TAOS_CHECK_GOTO(mndSetDropSmaRedoLogs(pMnode, pTrans, pSma), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetDropSmaVgroupRedoLogs(pMnode, pTrans, pVgroup), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetDropSmaCommitLogs(pMnode, pTrans, pSma), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetDropSmaVgroupCommitLogs(pMnode, pTrans, pVgroup), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetUpdateSmaStbCommitLogs(pMnode, pTrans, pStb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetDropSmaVgroupRedoActions(pMnode, pTrans, pDb, pVgroup), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pMnode, pTrans), NULL, _OVER);

  code = 0;

_OVER:
  mndTransDrop(pTrans);
  mndReleaseStream(pMnode, pStream);
  mndReleaseVgroup(pMnode, pVgroup);
  mndReleaseStb(pMnode, pStb);
#endif
  TAOS_RETURN(code);
}

static int32_t mndProcessDropRsmaReq(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  int32_t code = -1;
#if 0
  SDbObj      *pDb = NULL;
  SRsmaObj     *pSma = NULL;
  SMDropSmaReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMDropSmaReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _OVER);

  mInfo("sma:%s, start to drop", dropReq.name);

  SSIdx idx = {0};
  if ((code = mndAcquireGlobalIdx(pMnode, dropReq.name, SDB_SMA, &idx)) == 0) {
    pSma = idx.pIdx;
  } else {
    goto _OVER;
  }
  if (pSma == NULL) {
    if (dropReq.igNotExists) {
      mInfo("sma:%s, not exist, ignore not exist is set", dropReq.name);
      code = 0;
      goto _OVER;
    } else {
      code = TSDB_CODE_MND_SMA_NOT_EXIST;
      goto _OVER;
    }
  }

  SName name = {0};
  code = tNameFromString(&name, dropReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    goto _OVER;
  }
  char db[TSDB_TABLE_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(&name, db);

  pDb = mndAcquireDb(pMnode, db);
  if (pDb == NULL) {
    code = TSDB_CODE_MND_DB_NOT_SELECTED;
    goto _OVER;
  }

  TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb), NULL, _OVER);

  code = mndDropSma(pMnode, pReq, pDb, pSma);
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

_OVER:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("sma:%s, failed to drop since %s", dropReq.name, tstrerror(code));
  }

  mndReleaseSma(pMnode, pSma);
  mndReleaseDb(pMnode, pDb);
#endif
  TAOS_RETURN(code);
}

static int32_t mndRetrieveRsma(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  SRsmaObj *pSma = NULL;
  int32_t   cols = 0;
  int32_t   code = 0;
#if 0
  SDbObj *pDb = NULL;
  if (strlen(pShow->db) > 0) {
    pDb = mndAcquireDb(pMnode, pShow->db);
    if (pDb == NULL) return 0;
  }
  SSmaAndTagIter *pIter = pShow->pIter;
  while (numOfRows < rows) {
    pIter->pSmaIter = sdbFetch(pSdb, SDB_SMA, pIter->pSmaIter, (void **)&pSma);
    if (pIter->pSmaIter == NULL) break;

    if (NULL != pDb && pSma->dbUid != pDb->uid) {
      sdbRelease(pSdb, pSma);
      continue;
    }

    cols = 0;

    SName smaName = {0};
    SName stbName = {0};
    char  n2[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    char  n3[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    code = tNameFromString(&smaName, pSma->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    char n1[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    if (TSDB_CODE_SUCCESS == code) {
      STR_TO_VARSTR(n1, (char *)tNameGetTableName(&smaName));
      STR_TO_VARSTR(n2, (char *)mndGetDbStr(pSma->db));
      code = tNameFromString(&stbName, pSma->stb, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    }
    SColumnInfoData *pColInfo = NULL;
    if (TSDB_CODE_SUCCESS == code) {
      STR_TO_VARSTR(n3, (char *)tNameGetTableName(&stbName));

      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)n1, false);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)n2, false);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)n3, false);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&pSma->dstVgId, false);
    }
    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)&pSma->createdTime, false);
    }

    char col[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_TO_VARSTR(col, (char *)"");

    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)col, false);
    }

    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);

      char tag[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(tag, (char *)"sma_index");
      code = colDataSetVal(pColInfo, numOfRows, (const char *)tag, false);
    }

    numOfRows++;
    sdbRelease(pSdb, pSma);
    if (TSDB_CODE_SUCCESS != code) {
      sdbCancelFetch(pMnode->pSdb, pIter->pSmaIter);
      numOfRows = -1;
      break;
    }
  }

  mndReleaseDb(pMnode, pDb);
  pShow->numOfRows += numOfRows;
#endif
  return numOfRows;
}

static int32_t mndRetrieveRsmaTask(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;
  int32_t   numOfRows = 0;
  SRsmaObj *pSma = NULL;
  int32_t   cols = 0;
  int32_t   code = 0;
  pShow->numOfRows += numOfRows;
  return numOfRows;
}

// static void initRsmaObj(SCreateTSMACxt *pCxt) {
//   memcpy(pCxt->pSma->name, pCxt->pCreateSmaReq->name, TSDB_TABLE_FNAME_LEN);
//   memcpy(pCxt->pSma->stb, pCxt->pCreateSmaReq->stb, TSDB_TABLE_FNAME_LEN);
//   memcpy(pCxt->pSma->db, pCxt->pDb->name, TSDB_DB_FNAME_LEN);
//   if (pCxt->pBaseSma) memcpy(pCxt->pSma->baseSmaName, pCxt->pBaseSma->name, TSDB_TABLE_FNAME_LEN);
//   pCxt->pSma->createdTime = taosGetTimestampMs();
//   pCxt->pSma->uid = mndGenerateUid(pCxt->pCreateSmaReq->name, TSDB_TABLE_FNAME_LEN);

//   memcpy(pCxt->pSma->dstTbName, pCxt->targetStbFullName, TSDB_TABLE_FNAME_LEN);
//   pCxt->pSma->dstTbUid = 0;  // not used
//   pCxt->pSma->stbUid = pCxt->pSrcStb ? pCxt->pSrcStb->uid : pCxt->pCreateSmaReq->normSourceTbUid;
//   pCxt->pSma->dbUid = pCxt->pDb->uid;
//   pCxt->pSma->interval = pCxt->pCreateSmaReq->interval;
//   pCxt->pSma->intervalUnit = pCxt->pCreateSmaReq->intervalUnit;
//   //  pCxt->pSma->timezone = taosGetLocalTimezoneOffset();
//   pCxt->pSma->version = 1;

//   pCxt->pSma->exprLen = pCxt->pCreateSmaReq->exprLen;
//   pCxt->pSma->sqlLen = pCxt->pCreateSmaReq->sqlLen;
//   pCxt->pSma->astLen = pCxt->pCreateSmaReq->astLen;
//   pCxt->pSma->expr = pCxt->pCreateSmaReq->expr;
//   pCxt->pSma->sql = pCxt->pCreateSmaReq->sql;
//   pCxt->pSma->ast = pCxt->pCreateSmaReq->ast;
// }

static int32_t mndSetUpdateDbRsmaVersionPrepareLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pOld, SDbObj *pNew) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndDbActionEncode(pOld);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendPrepareLog(pTrans, pRedoRaw)) != 0) {
    sdbFreeRaw(pRedoRaw);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY));
}

static int32_t mndSetUpdateDbRsmaVersionCommitLogs(SMnode *pMnode, STrans *pTrans, SDbObj *pOld, SDbObj *pNew) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndDbActionEncode(pNew);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  if ((code = mndTransAppendCommitlog(pTrans, pCommitRaw)) != 0) {
    sdbFreeRaw(pCommitRaw);
    TAOS_RETURN(code);
  }

  TAOS_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));
}

static int32_t mndCreateRsma(SMnode *pMnode, SRpcMsg *pReq, SUserObj *pUser, SDbObj *pDb, SMCreateRsmaReq *pCreate) {
  int32_t    code = 0, lino = 0;
  SRsmaObj   obj = {0};
  int32_t    nDbs = 0, nVgs = 0, nStbs = 0;
  SDnodeObj *pDnode = NULL;
  SDbObj    *pDbs = NULL;
  SStbObj   *pStbs = NULL;
  STrans    *pTrans = NULL;
#if 1
  (void)snprintf(obj.name, TSDB_TABLE_FNAME_LEN, "%s", pCreate->name);
  (void)snprintf(obj.db, TSDB_DB_FNAME_LEN, "%s", pDb->name);
  (void)snprintf(obj.tbname, TSDB_TABLE_FNAME_LEN, "%s", pCreate->tbName);
  (void)snprintf(obj.createUser, TSDB_USER_LEN, "%s", pUser->user);
  obj.createdTime = taosGetTimestampMs();
  obj.updateTime = obj.createdTime;
  obj.uid = mndGenerateUid(obj.name, TSDB_TABLE_FNAME_LEN);
  obj.tbUid = pCreate->tbUid;
  obj.dbUid = pDb->uid;
  obj.interval[0] = pCreate->interval[0];
  obj.interval[1] = pCreate->interval[1];
  obj.version = 1;
  obj.tbType = pCreate->tbType;  // ETableType: 1 stable. Only super table supported currently.
  obj.intervalUnit = pCreate->intervalUnit;
  obj.nFuncs = pCreate->nFuncs;
  if (obj.nFuncs > 0) {
    TSDB_CHECK_NULL((obj.funcColIds = taosMemoryCalloc(obj.nFuncs, sizeof(col_id_t))), code, lino, _exit, terrno);
    TSDB_CHECK_NULL((obj.funcIds = taosMemoryCalloc(obj.nFuncs, sizeof(func_id_t))), code, lino, _exit, terrno);
  }

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "create-rsma")),
                  code, lino, _exit, terrno);
  mInfo("trans:%d, used to create rsma %s on tb %s", pTrans->id, obj.name, obj.tbname);

  mndTransSetDbName(pTrans, obj.name, NULL);
  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);
  TAOS_CHECK_EXIT(mndTransCheckConflict(pMnode, pTrans));

  mndTransSetOper(pTrans, MND_OPER_CREATE_RSMA);
  // TAOS_CHECK_EXIT(mndSetCreateRsmaPrepareActions(pMnode, pTrans, &mntObj));
  // TAOS_CHECK_EXIT(mndSetCreateDbPrepareActions(pMnode, pTrans, pDbs, nDbs));
  // TAOS_CHECK_EXIT(mndSetCreateVgPrepareActions(pMnode, pTrans, pVgs, nVgs));
  // TAOS_CHECK_EXIT(mndSetCreateRsmaRedoActions(pMnode, pTrans, &mntObj, pVgs, nVgs));
  // // TAOS_CHECK_EXIT(mndSetCreateRsmaUndoLogs(pMnode, pTrans, &mntObj));
  // TAOS_CHECK_EXIT(mndSetCreateRsmaCommitLogs(pMnode, pTrans, &mntObj));
  // TAOS_CHECK_EXIT(mndSetCreateDbCommitLogs(pMnode, pTrans, pDbs, nDbs));
  // TAOS_CHECK_EXIT(mndSetCreateVgCommitLogs(pMnode, pTrans, pVgs, nVgs));
  // TAOS_CHECK_EXIT(mndSetCreateStbCommitActions(pMnode, pTrans, pStbs, nStbs));
  // TAOS_CHECK_EXIT(mndSetCreateDbUndoActions(pMnode, pTrans, &mntObj, pVgroups));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("rsma:%s, failed at line %d to create rsma, since %s", obj.name, lino, tstrerror(code));
  }
  mndReleaseDnode(pMnode, pDnode);
  mndRsmaFreeObj(&obj);
  mndTransDrop(pTrans);
  if (pStbs) {
    for (int32_t i = 0; i < nStbs; ++i) {
      mndFreeStb(pStbs + i);
    }
    taosMemFreeClear(pStbs);
  }
#endif
  TAOS_RETURN(code);
}

static int32_t mndCheckCreateRsmaReq(SMCreateRsmaReq *pCreate) {
  int32_t code = TSDB_CODE_MND_INVALID_RSMA_OPTION;
  if (pCreate->name[0] == 0) goto _exit;
  if (pCreate->tbName[0] == 0) goto _exit;
  if (pCreate->igExists < 0 || pCreate->igExists > 1) goto _exit;
  if (pCreate->intervalUnit < 0) goto _exit;
  if (pCreate->interval[0] <= 0) goto _exit;
  if (pCreate->interval[1] < 0) goto _exit;

  SName rsmaName = {0};
  if ((code = tNameFromString(&rsmaName, pCreate->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) < 0) goto _exit;
  if (*(char *)tNameGetTableName(&rsmaName) == 0) goto _exit;
  code = 0;
_exit:
  TAOS_RETURN(code);
}

static int32_t mndCheckRsmaConflicts(SMnode *pMnode, SDbObj *pDbObj, SMCreateRsmaReq *pCreate) {
  void     *pIter = NULL;
  SSdb     *pSdb = pMnode->pSdb;
  SRsmaObj *pObj = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_RSMA, pIter, (void **)&pObj))) {
    if (pObj->tbUid == pCreate->tbUid && pObj->dbUid == pDbObj->uid) {
      sdbCancelFetch(pSdb, (pIter));
      mError("rsma:%s, conflict with existing rsma:%s on same table uid:%ld", pCreate->name, pObj->name, pObj->tbUid);
      return TSDB_CODE_QRY_DUPLICATED_OPERATION;
    }
    sdbRelease(pSdb, pObj);
  }
  return 0;
}

static int32_t mndProcessCreateRsmaReq(SRpcMsg *pReq) {
  int32_t         code = 0, lino = 0;
  SMnode         *pMnode = pReq->info.node;
  SDbObj         *pDb = NULL;
  SStbObj        *pStb = NULL;
  SRsmaObj       *pSma = NULL;
  SUserObj       *pUser = NULL;
  int64_t         mTraceId = TRACE_GET_ROOTID(&pReq->info.traceId);
  SMCreateRsmaReq createReq = {0};

  TAOS_CHECK_EXIT(tDeserializeSMCreateRsmaReq(pReq->pCont, pReq->contLen, &createReq));

  mInfo("start to create rsma: %s", createReq.name);
  TAOS_CHECK_EXIT(mndCheckCreateRsmaReq(&createReq));

  if ((pSma = mndAcquireRsma(pMnode, createReq.name))) {
    if (createReq.igExists) {
      mInfo("rsma:%s, already exist, ignore exist is set", createReq.name);
      code = 0;
      goto _exit;
    } else {
      TAOS_CHECK_EXIT(TSDB_CODE_MND_RSMA_ALREADY_EXIST);
    }
  } else {
    if ((code = terrno) == TSDB_CODE_MND_RSMA_NOT_EXIST) {
      // continue
    } else {  // TSDB_CODE_MND_RSMA_IN_CREATING | TSDB_CODE_MND_RSMA_IN_DROPPING | TSDB_CODE_APP_ERROR
      goto _exit;
    }
  }

  SName name = {0};
  TAOS_CHECK_EXIT(tNameFromString(&name, createReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));
  char db[TSDB_TABLE_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(&name, db);

  pDb = mndAcquireDb(pMnode, db);
  if (pDb == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_DB_NOT_SELECTED);
  }

  TAOS_CHECK_EXIT(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_READ_DB, pDb));
  TAOS_CHECK_EXIT(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb));

  TAOS_CHECK_EXIT(mndCheckRsmaConflicts(pMnode, pDb, &createReq));

  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, pReq->info.conn.user, &pUser));
  TAOS_CHECK_EXIT(mndCreateRsma(pMnode, pReq, pUser, pDb, &createReq));
  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("rsma:%s, failed to create since %s", createReq.name, tstrerror(code));
  }
  if (pStb) mndReleaseStb(pMnode, pStb);
  mndReleaseRsma(pMnode, pSma);
  mndReleaseDb(pMnode, pDb);
  tFreeSMCreateRsmaReq(&createReq);
  TAOS_RETURN(code);
}
#if 0
static int32_t mndDropRsma(void * pCxt) { //SCreateTSMACxt *pCxt) {
  int32_t      code = -1;
  STransAction dropStreamRedoAction = {0};
  STrans      *pTrans = mndTransCreate(pCxt->pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_TSMA, pCxt->pRpcReq, "drop-tsma");
  if (!pTrans) {
    code = terrno;
    goto _OVER;
  }
  mndTransSetDbName(pTrans, pCxt->pDb->name, NULL);
  if (mndTransCheckConflict(pCxt->pMnode, pTrans) != 0) goto _OVER;
  mndTransSetSerial(pTrans);
  mndGetMnodeEpSet(pCxt->pMnode, &dropStreamRedoAction.epSet);
  dropStreamRedoAction.acceptableCode = TSDB_CODE_MND_STREAM_NOT_EXIST;
  dropStreamRedoAction.msgType = TDMT_MND_DROP_STREAM;
  dropStreamRedoAction.contLen = pCxt->pDropSmaReq->dropStreamReqLen;
  dropStreamRedoAction.pCont = taosMemoryCalloc(1, dropStreamRedoAction.contLen);
  memcpy(dropStreamRedoAction.pCont, pCxt->pDropSmaReq->dropStreamReq, dropStreamRedoAction.contLen);

  // output stable is not dropped when dropping stream, dropping it when dropping tsma
  SMDropStbReq dropStbReq = {0};
  dropStbReq.igNotExists = false;
  tstrncpy(dropStbReq.name, pCxt->targetStbFullName, TSDB_TABLE_FNAME_LEN);
  dropStbReq.sql = "drop";
  dropStbReq.sqlLen = 5;

  STransAction dropStbRedoAction = {0};
  mndGetMnodeEpSet(pCxt->pMnode, &dropStbRedoAction.epSet);
  dropStbRedoAction.acceptableCode = TSDB_CODE_MND_STB_NOT_EXIST;
  dropStbRedoAction.msgType = TDMT_MND_STB_DROP;
  dropStbRedoAction.contLen = tSerializeSMDropStbReq(0, 0, &dropStbReq);
  dropStbRedoAction.pCont = taosMemoryCalloc(1, dropStbRedoAction.contLen);
  if (!dropStbRedoAction.pCont) {
    code = terrno;
    goto _OVER;
  }
  if (dropStbRedoAction.contLen !=
      tSerializeSMDropStbReq(dropStbRedoAction.pCont, dropStbRedoAction.contLen, &dropStbReq)) {
    mError("tsma: %s, failedto drop due to drop stb req encode failure", pCxt->pDropSmaReq->name);
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  SDbObj newDb = {0};
  memcpy(&newDb, pCxt->pDb, sizeof(SDbObj));
  newDb.tsmaVersion++;
  TAOS_CHECK_GOTO(mndSetDropSmaRedoLogs(pCxt->pMnode, pTrans, pCxt->pSma), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetDropSmaCommitLogs(pCxt->pMnode, pTrans, pCxt->pSma), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransAppendRedoAction(pTrans, &dropStreamRedoAction), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransAppendRedoAction(pTrans, &dropStbRedoAction), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetUpdateDbTsmaVersionPrepareLogs(pCxt->pMnode, pTrans, pCxt->pDb, &newDb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndSetUpdateDbTsmaVersionCommitLogs(pCxt->pMnode, pTrans, pCxt->pDb, &newDb), NULL, _OVER);
  TAOS_CHECK_GOTO(mndTransPrepare(pCxt->pMnode, pTrans), NULL, _OVER);
  code = TSDB_CODE_SUCCESS;
_OVER:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}
#endif
#if 0
static int32_t mndProcessDropRsmaReq(SRpcMsg *pReq) {
  int32_t      code = -1;

  SMDropSmaReq dropReq = {0};
  SRsmaObj     *pSma = NULL;
  SDbObj      *pDb = NULL;
  SMnode      *pMnode = pReq->info.node;
  SStbObj     *pStb = NULL;
  if (tDeserializeSMDropSmaReq(pReq->pCont, pReq->contLen, &dropReq) != TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_INVALID_MSG;
    goto _OVER;
  }

  char streamName[TSDB_TABLE_FNAME_LEN] = {0};
  char streamTargetStbFullName[TSDB_TABLE_FNAME_LEN] = {0};
  code = mndTSMAGenerateOutputName(dropReq.name, streamName, streamTargetStbFullName);
  if (TSDB_CODE_SUCCESS != code) {
    goto _OVER;
  }

  pStb = mndAcquireStb(pMnode, streamTargetStbFullName);

  pSma = mndAcquireSma(pMnode, dropReq.name);
  if (!pSma && dropReq.igNotExists) {
    code = 0;
    goto _OVER;
  }
  if (!pSma) {
    code = TSDB_CODE_MND_SMA_NOT_EXIST;
    goto _OVER;
  }
  SName name = {0};
  code = tNameFromString(&name, dropReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    goto _OVER;
  }
  char db[TSDB_TABLE_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(&name, db);

  pDb = mndAcquireDb(pMnode, db);
  if (!pDb) {
    code = TSDB_CODE_MND_DB_NOT_EXIST;
    goto _OVER;
  }

  if ((code = mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb)) != 0) {
    goto _OVER;
  }

  if (hasRecursiveTsmasBasedOnMe(pMnode, pSma)) {
    code = TSDB_CODE_MND_INVALID_DROP_TSMA;
    goto _OVER;
  }

  SCreateTSMACxt cxt = {
      .pDb = pDb,
      .pMnode = pMnode,
      .pRpcReq = pReq,
      .pSma = pSma,
      .streamName = streamName,
      .targetStbFullName = streamTargetStbFullName,
      .pDropSmaReq = &dropReq,
  };

  code = mndDropTSMA(&cxt);

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
_OVER:

  mndReleaseStb(pMnode, pStb);
  mndReleaseSma(pMnode, pSma);
  mndReleaseDb(pMnode, pDb);
  TAOS_RETURN(code);
}
static int32_t mndRetrieveRsma(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SDbObj          *pDb = NULL;
  int32_t          numOfRows = 0;
  SRsmaObj         *pSma = NULL;
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = 0;
  SColumnInfoData *pColInfo;
  if (pShow->pIter == NULL) {
    pShow->pIter = taosMemoryCalloc(1, sizeof(SSmaAndTagIter));
  }
  if (!pShow->pIter) {
    return terrno;
  }
  if (pShow->db[0]) {
    pDb = mndAcquireDb(pMnode, pShow->db);
  }
  SSmaAndTagIter *pIter = pShow->pIter;
  while (numOfRows < rows) {
    pIter->pSmaIter = sdbFetch(pMnode->pSdb, SDB_SMA, pIter->pSmaIter, (void **)&pSma);
    if (pIter->pSmaIter == NULL) break;
    SDbObj *pSrcDb = mndAcquireDb(pMnode, pSma->db);

    if ((pDb && pSma->dbUid != pDb->uid) || !pSrcDb) {
      sdbRelease(pMnode->pSdb, pSma);
      if (pSrcDb) mndReleaseDb(pMnode, pSrcDb);
      continue;
    }

    int32_t cols = 0;
    SName   n = {0};

    code = tNameFromString(&n, pSma->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    char smaName[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    if (TSDB_CODE_SUCCESS == code) {
      STR_TO_VARSTR(smaName, (char *)tNameGetTableName(&n));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)smaName, false);
    }

    char db[TSDB_DB_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    if (TSDB_CODE_SUCCESS == code) {
      STR_TO_VARSTR(db, (char *)mndGetDbStr(pSma->db));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)db, false);
    }

    if (TSDB_CODE_SUCCESS == code) {
      code = tNameFromString(&n, pSma->stb, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    }
    char srcTb[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
    if (TSDB_CODE_SUCCESS == code) {
      STR_TO_VARSTR(srcTb, (char *)tNameGetTableName(&n));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)srcTb, false);
    }

    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)db, false);
    }

    if (TSDB_CODE_SUCCESS == code) {
      code = tNameFromString(&n, pSma->dstTbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    }

    if (TSDB_CODE_SUCCESS == code) {
      char targetTb[TSDB_TABLE_FNAME_LEN + VARSTR_HEADER_SIZE] = {0};
      STR_TO_VARSTR(targetTb, (char *)tNameGetTableName(&n));
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)targetTb, false);
    }

    if (TSDB_CODE_SUCCESS == code) {
      // stream name
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)smaName, false);
    }

    if (TSDB_CODE_SUCCESS == code) {
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, (const char *)(&pSma->createdTime), false);
    }

    // interval
    char    interval[64 + VARSTR_HEADER_SIZE] = {0};
    int32_t len = 0;
    if (TSDB_CODE_SUCCESS == code) {
      if (!IS_CALENDAR_TIME_DURATION(pSma->intervalUnit)) {
        len = tsnprintf(interval + VARSTR_HEADER_SIZE, 64, "%" PRId64 "%c", pSma->interval,
                        getPrecisionUnit(pSrcDb->cfg.precision));
      } else {
        len = tsnprintf(interval + VARSTR_HEADER_SIZE, 64, "%" PRId64 "%c", pSma->interval, pSma->intervalUnit);
      }
      varDataSetLen(interval, len);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, interval, false);
    }

    char buf[TSDB_MAX_SAVED_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
    if (TSDB_CODE_SUCCESS == code) {
      // create sql
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      len = tsnprintf(buf + VARSTR_HEADER_SIZE, TSDB_MAX_SAVED_SQL_LEN, "%s", pSma->sql);
      varDataSetLen(buf, TMIN(len, TSDB_MAX_SAVED_SQL_LEN));
      code = colDataSetVal(pColInfo, numOfRows, buf, false);
    }

    // func list
    len = 0;
    SNode *pNode = NULL, *pFunc = NULL;
    if (TSDB_CODE_SUCCESS == code) {
      code = nodesStringToNode(pSma->ast, &pNode);
    }
    if (TSDB_CODE_SUCCESS == code) {
      char *start = buf + VARSTR_HEADER_SIZE;
      FOREACH(pFunc, ((SSelectStmt *)pNode)->pProjectionList) {
        if (nodeType(pFunc) == QUERY_NODE_FUNCTION) {
          SFunctionNode *pFuncNode = (SFunctionNode *)pFunc;
          if (!fmIsTSMASupportedFunc(pFuncNode->funcId)) continue;
          len += tsnprintf(start, TSDB_MAX_SAVED_SQL_LEN - len, "%s%s", start != buf + VARSTR_HEADER_SIZE ? "," : "",
                           ((SExprNode *)pFunc)->userAlias);
          if (len >= TSDB_MAX_SAVED_SQL_LEN) {
            len = TSDB_MAX_SAVED_SQL_LEN;
            break;
          }
          start = buf + VARSTR_HEADER_SIZE + len;
        }
      }
      nodesDestroyNode(pNode);
    }

    if (TSDB_CODE_SUCCESS == code) {
      varDataSetLen(buf, len);
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
      code = colDataSetVal(pColInfo, numOfRows, buf, false);
    }

    numOfRows++;
    mndReleaseSma(pMnode, pSma);
    mndReleaseDb(pMnode, pSrcDb);
    if (TSDB_CODE_SUCCESS != code) {
      sdbCancelFetch(pMnode->pSdb, pIter->pSmaIter);
      numOfRows = code;
      break;
    }
  }
  mndReleaseDb(pMnode, pDb);
  pShow->numOfRows += numOfRows;
  if (numOfRows < rows) {
    taosMemoryFree(pShow->pIter);
    pShow->pIter = NULL;
  }
  return numOfRows;
}
#endif
static void mndCancelRetrieveRsma(SMnode *pMnode, void *pIter) {
#if 0
  SSmaAndTagIter *p = pIter;
  if (p != NULL) {
    SSdb *pSdb = pMnode->pSdb;
    sdbCancelFetchByType(pSdb, p->pSmaIter, SDB_SMA);
  }
  taosMemoryFree(p);
#endif
}
static void mndCancelRetrieveRsmaTask(SMnode *pMnode, void *pIter) {
#if 0
  SSmaAndTagIter *p = pIter;
  if (p != NULL) {
    SSdb *pSdb = pMnode->pSdb;
    sdbCancelFetchByType(pSdb, p->pSmaIter, SDB_SMA);
  }
  taosMemoryFree(p);
#endif
}

int32_t dumpRsmaInfoFromSmaObj(const SRsmaObj *pSma, const SStbObj *pDestStb, STableTSMAInfo *pInfo,
                               const SRsmaObj *pBaseTsma) {
  int32_t code = 0;
#if 0
  pInfo->interval = pSma->interval;
  pInfo->unit = pSma->intervalUnit;
  pInfo->tsmaId = pSma->uid;
  pInfo->version = pSma->version;
  pInfo->tsmaId = pSma->uid;
  pInfo->destTbUid = pDestStb->uid;
  SName sName = {0};
  code = tNameFromString(&sName, pSma->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  tstrncpy(pInfo->name, sName.tname, TSDB_TABLE_NAME_LEN);
  tstrncpy(pInfo->targetDbFName, pSma->db, TSDB_DB_FNAME_LEN);
  code = tNameFromString(&sName, pSma->dstTbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  tstrncpy(pInfo->targetTb, sName.tname, TSDB_TABLE_NAME_LEN);
  tstrncpy(pInfo->dbFName, pSma->db, TSDB_DB_FNAME_LEN);
  code = tNameFromString(&sName, pSma->stb, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
  if (TSDB_CODE_SUCCESS != code) {
    return code;
  }
  tstrncpy(pInfo->tb, sName.tname, TSDB_TABLE_NAME_LEN);
  pInfo->pFuncs = taosArrayInit(8, sizeof(STableTSMAFuncInfo));
  if (!pInfo->pFuncs) return TSDB_CODE_OUT_OF_MEMORY;

  SNode *pNode, *pFunc;
  if (TSDB_CODE_SUCCESS != nodesStringToNode(pBaseTsma ? pBaseTsma->ast : pSma->ast, &pNode)) {
    taosArrayDestroy(pInfo->pFuncs);
    pInfo->pFuncs = NULL;
    return TSDB_CODE_TSMA_INVALID_STAT;
  }
  if (pNode) {
    SSelectStmt *pSelect = (SSelectStmt *)pNode;
    FOREACH(pFunc, pSelect->pProjectionList) {
      STableTSMAFuncInfo funcInfo = {0};
      SFunctionNode     *pFuncNode = (SFunctionNode *)pFunc;
      if (!fmIsTSMASupportedFunc(pFuncNode->funcId)) continue;
      funcInfo.funcId = pFuncNode->funcId;
      funcInfo.colId = ((SColumnNode *)pFuncNode->pParameterList->pHead->pNode)->colId;
      if (!taosArrayPush(pInfo->pFuncs, &funcInfo)) {
        code = terrno;
        taosArrayDestroy(pInfo->pFuncs);
        nodesDestroyNode(pNode);
        return code;
      }
    }
    nodesDestroyNode(pNode);
  }
  pInfo->ast = taosStrdup(pSma->ast);
  if (!pInfo->ast) code = terrno;

  if (code == TSDB_CODE_SUCCESS && pDestStb->numOfTags > 0) {
    pInfo->pTags = taosArrayInit(pDestStb->numOfTags, sizeof(SSchema));
    if (!pInfo->pTags) {
      code = terrno;
    } else {
      for (int32_t i = 0; i < pDestStb->numOfTags; ++i) {
        if (NULL == taosArrayPush(pInfo->pTags, &pDestStb->pTags[i])) {
          code = terrno;
          break;
        }
      }
    }
  }
  if (code == TSDB_CODE_SUCCESS) {
    pInfo->pUsedCols = taosArrayInit(pDestStb->numOfColumns - 3, sizeof(SSchema));
    if (!pInfo->pUsedCols)
      code = terrno;
    else {
      // skip _wstart, _wend, _duration
      for (int32_t i = 1; i < pDestStb->numOfColumns - 2; ++i) {
        if (NULL == taosArrayPush(pInfo->pUsedCols, &pDestStb->pColumns[i])) {
          code = terrno;
          break;
        }
      }
    }
  }
#endif
  TAOS_RETURN(code);
}

#if 0
typedef bool (*tsmaFilter)(const SRsmaObj *pSma, void *param);

static int32_t mndGetSomeRsmas(SMnode *pMnode, STableTSMAInfoRsp *pRsp, tsmaFilter filtered, void *param, bool *exist) {
  int32_t     code = 0;
  SRsmaObj    *pSma = NULL;
  SRsmaObj    *pBaseTsma = NULL;
  SSdb       *pSdb = pMnode->pSdb;
  void       *pIter = NULL;
  SStreamObj *pStream = NULL;
  SStbObj    *pStb = NULL;
  bool        shouldRetry = false;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_SMA, pIter, (void **)&pSma);
    if (pIter == NULL) break;

    if (filtered(pSma, param)) {
      sdbRelease(pSdb, pSma);
      continue;
    }

    pStb = mndAcquireStb(pMnode, pSma->dstTbName);
    if (!pStb) {
      sdbRelease(pSdb, pSma);
      shouldRetry = true;
      continue;
    }

    SName smaName;
    char  streamName[TSDB_TABLE_FNAME_LEN] = {0};
    code = tNameFromString(&smaName, pSma->name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE);
    if (TSDB_CODE_SUCCESS != code) {
      sdbRelease(pSdb, pSma);
      mndReleaseStb(pMnode, pStb);
      TAOS_RETURN(code);
    }
    snprintf(streamName, TSDB_TABLE_FNAME_LEN, "%d.%s.%s", smaName.acctId, smaName.dbname, smaName.tname);
    pStream = NULL;

    code = mndAcquireStream(pMnode, streamName, &pStream);
    if (!pStream) {
      shouldRetry = true;
      sdbRelease(pSdb, pSma);
      mndReleaseStb(pMnode, pStb);
      continue;
    }
    if (code != 0) {
      sdbRelease(pSdb, pSma);
      mndReleaseStb(pMnode, pStb);
      TAOS_RETURN(code);
    }

    int64_t streamId = pStream->pCreate->streamId;
    mndReleaseStream(pMnode, pStream);

    STableTSMAInfo *pTsma = taosMemoryCalloc(1, sizeof(STableTSMAInfo));
    if (!pTsma) {
      code = terrno;
      mndReleaseStb(pMnode, pStb);
      sdbRelease(pSdb, pSma);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }

    pTsma->streamAddr = taosMemoryCalloc(1, sizeof(SStreamTaskAddr));
    code = msmGetTriggerTaskAddr(pMnode, streamId, pTsma->streamAddr);
    if (code != 0) {
      shouldRetry = true;
      mndReleaseStb(pMnode, pStb);
      sdbRelease(pSdb, pSma);
      tFreeAndClearTableTSMAInfo(pTsma);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }
    pTsma->streamUid = streamId;

    code = mndGetDeepestBaseForTsma(pMnode, pSma, &pBaseTsma);
    if (code == 0) {
      code = dumpTSMAInfoFromSmaObj(pSma, pStb, pTsma, pBaseTsma);
    }
    mndReleaseStb(pMnode, pStb);
    sdbRelease(pSdb, pSma);
    if (pBaseTsma) mndReleaseSma(pMnode, pBaseTsma);
    if (terrno) {
      tFreeAndClearTableTSMAInfo(pTsma);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }
    if (NULL == taosArrayPush(pRsp->pTsmas, &pTsma)) {
      code = terrno;
      tFreeAndClearTableTSMAInfo(pTsma);
      sdbCancelFetch(pSdb, pIter);
      TAOS_RETURN(code);
    }
    *exist = true;
  }
  if (shouldRetry) {
    return TSDB_CODE_NEED_RETRY;
  }
  return TSDB_CODE_SUCCESS;
}

static bool tsmaTbFilter(const SRsmaObj *pSma, void *param) {
  const char *tbFName = param;
  return pSma->stb[0] != tbFName[0] || strcmp(pSma->stb, tbFName) != 0;
}

static int32_t mndGetTableTSMA(SMnode *pMnode, char *tbFName, STableTSMAInfoRsp *pRsp, bool *exist) {
  return mndGetSomeTsmas(pMnode, pRsp, tsmaTbFilter, tbFName, exist);
}

static bool tsmaDbFilter(const SRsmaObj *pSma, void *param) {
  uint64_t *dbUid = param;
  return pSma->dbUid != *dbUid;
}

int32_t mndGetDbTsmas(SMnode *pMnode, const char *dbFName, uint64_t dbUid, STableTSMAInfoRsp *pRsp, bool *exist) {
  return mndGetSomeTsmas(pMnode, pRsp, tsmaDbFilter, &dbUid, exist);
}

static int32_t mkNonExistTSMAInfo(const STSMAVersion *pTsmaVer, STableTSMAInfo **ppTsma) {
  STableTSMAInfo *pInfo = taosMemoryCalloc(1, sizeof(STableTSMAInfo));
  if (!pInfo) {
    return terrno;
  }
  pInfo->pFuncs = NULL;
  pInfo->tsmaId = pTsmaVer->tsmaId;
  tstrncpy(pInfo->dbFName, pTsmaVer->dbFName, TSDB_DB_FNAME_LEN);
  tstrncpy(pInfo->tb, pTsmaVer->tbName, TSDB_TABLE_NAME_LEN);
  tstrncpy(pInfo->name, pTsmaVer->name, TSDB_TABLE_NAME_LEN);
  pInfo->dbId = pTsmaVer->dbId;
  pInfo->ast = taosMemoryCalloc(1, 1);
  if (!pInfo->ast) {
    taosMemoryFree(pInfo);
    return terrno;
  }
  *ppTsma = pInfo;
  return TSDB_CODE_SUCCESS;
}

int32_t mndValidateTSMAInfo(SMnode *pMnode, STSMAVersion *pTsmaVersions, int32_t numOfTsmas, void **ppRsp,
                            int32_t *pRspLen) {
  int32_t         code = -1;
  STSMAHbRsp      hbRsp = {0};
  int32_t         rspLen = 0;
  void           *pRsp = NULL;
  char            tsmaFName[TSDB_TABLE_FNAME_LEN] = {0};
  STableTSMAInfo *pTsmaInfo = NULL;

  hbRsp.pTsmas = taosArrayInit(numOfTsmas, POINTER_BYTES);
  if (!hbRsp.pTsmas) {
    code = terrno;
    TAOS_RETURN(code);
  }

  for (int32_t i = 0; i < numOfTsmas; ++i) {
    STSMAVersion *pTsmaVer = &pTsmaVersions[i];
    pTsmaVer->dbId = be64toh(pTsmaVer->dbId);
    pTsmaVer->tsmaId = be64toh(pTsmaVer->tsmaId);
    pTsmaVer->version = ntohl(pTsmaVer->version);

    snprintf(tsmaFName, sizeof(tsmaFName), "%s.%s", pTsmaVer->dbFName, pTsmaVer->name);
    SRsmaObj *pSma = mndAcquireSma(pMnode, tsmaFName);
    if (!pSma) {
      code = mkNonExistTSMAInfo(pTsmaVer, &pTsmaInfo);
      if (code) goto _OVER;
      if (NULL == taosArrayPush(hbRsp.pTsmas, &pTsmaInfo)) {
        code = terrno;
        tFreeAndClearTableTSMAInfo(pTsmaInfo);
        goto _OVER;
      }
      continue;
    }

    if (pSma->uid != pTsmaVer->tsmaId) {
      mDebug("tsma: %s.%" PRIx64 " tsmaId mismatch with current %" PRIx64, tsmaFName, pTsmaVer->tsmaId, pSma->uid);
      code = mkNonExistTSMAInfo(pTsmaVer, &pTsmaInfo);
      mndReleaseSma(pMnode, pSma);
      if (code) goto _OVER;
      if (NULL == taosArrayPush(hbRsp.pTsmas, &pTsmaInfo)) {
        code = terrno;
        tFreeAndClearTableTSMAInfo(pTsmaInfo);
        goto _OVER;
      }
      continue;
    } else if (pSma->version == pTsmaVer->version) {
      mndReleaseSma(pMnode, pSma);
      continue;
    }

    SStbObj *pDestStb = mndAcquireStb(pMnode, pSma->dstTbName);
    if (!pDestStb) {
      mInfo("tsma: %s.%" PRIx64 " dest stb: %s not found, maybe dropped", tsmaFName, pTsmaVer->tsmaId, pSma->dstTbName);
      code = mkNonExistTSMAInfo(pTsmaVer, &pTsmaInfo);
      mndReleaseSma(pMnode, pSma);
      if (code) goto _OVER;
      if (NULL == taosArrayPush(hbRsp.pTsmas, &pTsmaInfo)) {
        code = terrno;
        tFreeAndClearTableTSMAInfo(pTsmaInfo);
        goto _OVER;
      }
      continue;
    }

    // dump smaObj into rsp
    STableTSMAInfo *pInfo = NULL;
    pInfo = taosMemoryCalloc(1, sizeof(STableTSMAInfo));
    if (!pInfo) {
      code = terrno;
      mndReleaseSma(pMnode, pSma);
      mndReleaseStb(pMnode, pDestStb);
      goto _OVER;
    }

    SRsmaObj *pBaseSma = NULL;
    code = mndGetDeepestBaseForTsma(pMnode, pSma, &pBaseSma);
    if (code == 0) code = dumpTSMAInfoFromSmaObj(pSma, pDestStb, pInfo, pBaseSma);

    mndReleaseStb(pMnode, pDestStb);
    mndReleaseSma(pMnode, pSma);
    if (pBaseSma) mndReleaseSma(pMnode, pBaseSma);
    if (terrno) {
      tFreeAndClearTableTSMAInfo(pInfo);
      goto _OVER;
    }

    if (NULL == taosArrayPush(hbRsp.pTsmas, pInfo)) {
      code = terrno;
      tFreeAndClearTableTSMAInfo(pInfo);
      goto _OVER;
    }
  }

  rspLen = tSerializeTSMAHbRsp(NULL, 0, &hbRsp);
  if (rspLen < 0) {
    code = terrno;
    goto _OVER;
  }

  pRsp = taosMemoryMalloc(rspLen);
  if (!pRsp) {
    code = terrno;
    rspLen = 0;
    goto _OVER;
  }

  rspLen = tSerializeTSMAHbRsp(pRsp, rspLen, &hbRsp);
  if (rspLen < 0) {
    code = terrno;
    goto _OVER;
  }
  code = 0;
_OVER:
  tFreeTSMAHbRsp(&hbRsp);
  *ppRsp = pRsp;
  *pRspLen = rspLen;
  TAOS_RETURN(code);
}
#endif
int32_t mndDropRsmasByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
#if 0
  while (1) {
    SRsmaObj *pSma = NULL;
    pIter = sdbFetch(pSdb, SDB_SMA, pIter, (void **)&pSma);
    if (pIter == NULL) break;

    if (pSma->dbUid == pDb->uid) {
      if ((code = mndSetDropSmaCommitLogs(pMnode, pTrans, pSma)) != 0) {
        sdbRelease(pSdb, pSma);
        sdbCancelFetch(pSdb, pSma);
        TAOS_RETURN(code);
      }
    }

    sdbRelease(pSdb, pSma);
  }
#endif
  TAOS_RETURN(code);
}