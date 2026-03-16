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
#include "audit.h"
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
#include "mndVgroup.h"
#include "parser.h"
#include "tname.h"

#define MND_TXN_VER_NUMBER   1
#define MND_TXN_RESERVE_SIZE 64

static SSdbRaw *mndTxnActionEncode(STxnObj *pTxn);
static SSdbRow *mndTxnActionDecode(SSdbRaw *pRaw);
static int32_t  mndTxnActionInsert(SSdb *pSdb, STxnObj *pTxn);
static int32_t  mndTxnActionDelete(SSdb *pSdb, STxnObj *pTxn);
static int32_t  mndTxnActionUpdate(SSdb *pSdb, STxnObj *pOld, STxnObj *pNew);
static int32_t  mndProcessCreateTxnReq(SRpcMsg *pReq);
static int32_t  mndProcessDropTxnReq(SRpcMsg *pReq);
static int32_t  mndProcessAlterTxnReq(SRpcMsg *pReq);
static int32_t  mndProcessGetTxnReq(SRpcMsg *pReq);

static int32_t mndRetrieveTxn(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelRetrieveTxn(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveTxnTask(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelRetrieveTxnTask(SMnode *pMnode, void *pIter);

int32_t mndInitTxn(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_TXN,
      .keyType = SDB_KEY_BINARY,
      .encodeFp = (SdbEncodeFp)mndTxnActionEncode,
      .decodeFp = (SdbDecodeFp)mndTxnActionDecode,
      .insertFp = (SdbInsertFp)mndTxnActionInsert,
      .updateFp = (SdbUpdateFp)mndTxnActionUpdate,
      .deleteFp = (SdbDeleteFp)mndTxnActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_TXN, mndProcessCreateTxnReq);
  mndSetMsgHandle(pMnode, TDMT_VND_CREATE_TXN_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_TXN, mndProcessDropTxnReq);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_TXN_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_ALTER_TXN, mndProcessAlterTxnReq);
  mndSetMsgHandle(pMnode, TDMT_VND_ALTER_TXN_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_GET_TXN, mndProcessGetTxnReq);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TXN, mndRetrieveTxn);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TXN, mndCancelRetrieveTxn);

  return sdbSetTable(pMnode->pSdb, table);
}

void mndCleanupTxn(SMnode *pMnode) {}

void mndTxnFreeObj(STxnObj *pObj) {
  if (pObj) {
    taosMemoryFreeClear(pObj->funcColIds);
    taosMemoryFreeClear(pObj->funcIds);
  }
}

static int32_t tSerializeSTxnObj(void *buf, int32_t bufLen, const STxnObj *pObj) {
  int32_t  code = 0, lino = 0;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->name));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->tbName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->dbFName));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->createUser));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->createdTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->updateTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->uid));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->tbUid));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->dbUid));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->interval[0]));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->interval[1]));
  TAOS_CHECK_EXIT(tEncodeU64v(&encoder, pObj->reserved));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pObj->version));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pObj->tbType));
  TAOS_CHECK_EXIT(tEncodeI8(&encoder, pObj->intervalUnit));
  TAOS_CHECK_EXIT(tEncodeI16v(&encoder, pObj->nFuncs));
  for (int16_t i = 0; i < pObj->nFuncs; ++i) {
    TAOS_CHECK_EXIT(tEncodeI16v(&encoder, pObj->funcColIds[i]));
    TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pObj->funcIds[i]));
  }
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->ownerId));

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

static int32_t tDeserializeSTxnObj(void *buf, int32_t bufLen, STxnObj *pObj) {
  int32_t  code = 0, lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->name));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->tbName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->dbFName));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->createUser));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->createdTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->updateTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->uid));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->tbUid));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->dbUid));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->interval[0]));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->interval[1]));
  TAOS_CHECK_EXIT(tDecodeU64v(&decoder, &pObj->reserved));
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pObj->version));
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
  if (!tDecodeIsEnd(&decoder)) {
    TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->ownerId));
  }

_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("rsma, %s failed at line %d since %s, row:%p", __func__, lino, tstrerror(code), pObj);
  }
  TAOS_RETURN(code);
}

static SSdbRaw *mndTxnActionEncode(STxnObj *pObj) {
  int32_t  code = 0, lino = 0;
  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;
  int32_t  tlen = tSerializeSTxnObj(NULL, 0, pObj);
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

  tlen = tSerializeSTxnObj(buf, tlen, pObj);
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
  TAOS_CHECK_EXIT(sdbGetRawSoftVer(pRaw, &sver));

  if (sver != MND_RSMA_VER_NUMBER) {
    mError("rsma read invalid ver, data ver: %d, curr ver: %d", sver, MND_RSMA_VER_NUMBER);
    TAOS_CHECK_EXIT(TSDB_CODE_SDB_INVALID_DATA_VER);
  }

  if (!(pRow = sdbAllocRow(sizeof(SRsmaObj)))) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  if (!(pObj = sdbGetRowObj(pRow))) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  int32_t tlen;
  int32_t dataPos = 0;
  SDB_GET_INT32(pRaw, dataPos, &tlen, _exit);
  buf = taosMemoryMalloc(tlen + 1);
  if (buf == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }
  SDB_GET_BINARY(pRaw, dataPos, buf, tlen, _exit);

  TAOS_CHECK_EXIT(tDeserializeSTxnObj(buf, tlen, pObj));

  taosInitRWLatch(&pObj->lock);

_exit:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    mError("txn, failed at line %d to decode from raw:%p since %s", lino, pRaw, tstrerror(code));
    mndTxnFreeObj(pObj);
    taosMemoryFreeClear(pRow);
    return NULL;
  }
  mTrace("txn, decode from raw:%p, row:%p", pRaw, pObj);
  return pRow;
}

static int32_t mndTxnActionInsert(SSdb *pSdb, STxnObj *pObj) {
  mTrace("txn:%s, perform insert action, row:%p", pObj->name, pObj);
  return 0;
}

static int32_t mndTxnActionDelete(SSdb *pSdb, STxnObj *pObj) {
  mTrace("txn:%s, perform delete action, row:%p", pObj->name, pObj);
  mndTxnFreeObj(pObj);
  return 0;
}

static int32_t mndTxnActionUpdate(SSdb *pSdb, STxnObj *pOld, STxnObj *pNew) {
  mTrace("txn:%s, perform update action, old row:%p new row:%p", pOld->name, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->updateTime = pNew->updateTime;
  pOld->nFuncs = pNew->nFuncs;
  pOld->ownerId = pNew->ownerId;
  TSWAP(pOld->funcColIds, pNew->funcColIds);
  TSWAP(pOld->funcIds, pNew->funcIds);
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

STxnObj *mndAcquireTxn(SMnode *pMnode, char *name) {
  SSdb     *pSdb = pMnode->pSdb;
  STxnObj *pObj = sdbAcquire(pSdb, SDB_TXN, name);
  if (pObj == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      terrno = TSDB_CODE_TXN_NOT_EXIST;
    } else if (terrno == TSDB_CODE_SDB_OBJ_CREATING) {
      terrno = TSDB_CODE_MND_TXN_IN_CREATING;
    } else if (terrno == TSDB_CODE_SDB_OBJ_DROPPING) {
      terrno = TSDB_CODE_MND_TXN_IN_DROPPING;
    } else {
      terrno = TSDB_CODE_APP_ERROR;
      mFatal("txn:%s, failed to acquire txn since %s", name, terrstr());
    }
  }
  return pObj;
}

void mndReleaseTxn(SMnode *pMnode, STxnObj *pTxn) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pTxn);
}
#ifdef TD_ENTERPRISE
static int32_t mndSetCreateTxnRedoLogs(SMnode *pMnode, STrans *pTrans, STxnObj *pTxn) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndTxnActionEncode(pTxn);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendRedolog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_CREATING));

  TAOS_RETURN(code);
}

static int32_t mndSetCreateTxnUndoLogs(SMnode *pMnode, STrans *pTrans, STxnObj *pTxn) {
  int32_t  code = 0;
  SSdbRaw *pUndoRaw = mndTxnActionEncode(pTxn);
  if (!pUndoRaw) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendUndolog(pTrans, pUndoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pUndoRaw, SDB_STATUS_DROPPED));
  TAOS_RETURN(code);
}

static int32_t mndSetCreateTxnPrepareActions(SMnode *pMnode, STrans *pTrans, STxnObj *pTxn) {
  SSdbRaw *pDbRaw = mndTxnActionEncode(pTxn);
  if (pDbRaw == NULL) return -1;

  if (mndTransAppendPrepareLog(pTrans, pDbRaw) != 0) return -1;
  if (sdbSetRawStatus(pDbRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
}

static void *mndBuildVCreateTxnReq(SMnode *pMnode, SVgObj *pVgroup, SStbObj *pStb, STxnObj *pObj,
                                    SMCreateTxnReq *pCreate, int32_t *pContLen) {
  int32_t         code = 0, lino = 0;
  SMsgHead       *pHead = NULL;
  SMCreateTxnReq req = *pCreate;

  req.uid = pObj->uid;  // use the uid generated by mnode

  int32_t contLen = tSerializeSMCreateTxnReq(NULL, 0, &req);
  TAOS_CHECK_EXIT(contLen);
  contLen += sizeof(SMsgHead);
  TSDB_CHECK_NULL((pHead = taosMemoryMalloc(contLen)), code, lino, _exit, terrno);
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);
  void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
  TAOS_CHECK_EXIT(tSerializeSMCreateTxnReq(pBuf, contLen, &req));
_exit:
  if (code < 0) {
    taosMemoryFreeClear(pHead);
    terrno = code;
    *pContLen = 0;
    return NULL;
  }
  *pContLen = contLen;
  return pHead;
}

static int32_t mndSetCreateTxnRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb, STxnObj *pObj,
                                           SMCreateTxnReq *pCreate) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;

  SName name = {0};
  if ((code = tNameFromString(&name, pCreate->tbFName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) != 0) {
    return code;
  }
  tstrncpy(pCreate->tbFName, (char *)tNameGetTableName(&name), sizeof(pCreate->tbFName));  // convert tbFName to tbName

  while ((pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup))) {
    if (!mndVgroupInDb(pVgroup, pDb->uid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    int32_t contLen = 0;
    void   *pReq = mndBuildVCreateTxnReq(pMnode, pVgroup, pStb, pObj, pCreate, &contLen);
    if (pReq == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      code = terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL;
      TAOS_RETURN(code);
    }

    STransAction action = {0};
    action.mTraceId = pTrans->mTraceId;
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_VND_CREATE_TXN;
    action.acceptableCode = TSDB_CODE_TXN_ALREADY_EXISTS;  // check whether the txn uid exist
    action.retryCode = TSDB_CODE_TDB_STB_NOT_EXIST;         // retry if relative table not exist
    if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
      taosMemoryFree(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      TAOS_RETURN(code);
    }
    sdbRelease(pSdb, pVgroup);
  }

  TAOS_RETURN(code);
}

static int32_t mndSetCreateTxnCommitLogs(SMnode *pMnode, STrans *pTrans, STxnObj *pTxn) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndTxnActionEncode(pTxn);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));

  TAOS_RETURN(code);
}

static int32_t mndSetDropTxnPrepareLogs(SMnode *pMnode, STrans *pTrans, STxnObj *pTxn) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndTxnActionEncode(pTxn);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    return -1;
  }
  TAOS_CHECK_RETURN(mndTransAppendPrepareLog(pTrans, pRedoRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pRedoRaw, SDB_STATUS_DROPPING));

  return 0;
}

static int32_t mndSetDropTxnCommitLogs(SMnode *pMnode, STrans *pTrans, STxnObj *pTxn) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndTxnActionEncode(pTxn);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    return -1;
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_DROPPED));

  return 0;
}

static void *mndBuildVDropTxnReq(SMnode *pMnode, SVgObj *pVgroup, STxnObj *pObj, int32_t *pContLen) {
  int32_t       code = 0, lino = 0;
  SMsgHead     *pHead = NULL;
  SVDropTxnReq req = {0};

  (void)snprintf(req.tbName, sizeof(req.tbName), "%s", pObj->tbName);
  (void)snprintf(req.name, sizeof(req.name), "%s", pObj->name);
  req.tbType = pObj->tbType;
  req.uid = pObj->uid;
  req.tbUid = pObj->tbUid;

  int32_t contLen = tSerializeSVDropTxnReq(NULL, 0, &req);
  TAOS_CHECK_EXIT(contLen);
  contLen += sizeof(SMsgHead);
  TSDB_CHECK_NULL((pHead = taosMemoryMalloc(contLen)), code, lino, _exit, terrno);
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);
  void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
  TAOS_CHECK_EXIT(tSerializeSVDropTxnReq(pBuf, contLen, &req));
_exit:
  if (code < 0) {
    taosMemoryFreeClear(pHead);
    terrno = code;
    *pContLen = 0;
    return NULL;
  }
  *pContLen = contLen;
  return pHead;
}

static int32_t mndSetDropTxnRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, STxnObj *pTxn) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;

  while ((pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup))) {
    if (!mndVgroupInDb(pVgroup, pDb->uid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    int32_t contLen = 0;
    void   *pReq = mndBuildVDropTxnReq(pMnode, pVgroup, pTxn, &contLen);
    if (pReq == NULL) {
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      code = terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL;
      TAOS_RETURN(code);
    }

    STransAction action = {0};
    action.mTraceId = pTrans->mTraceId;
    action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
    action.pCont = pReq;
    action.contLen = contLen;
    action.msgType = TDMT_VND_DROP_TXN;
    action.acceptableCode = TSDB_CODE_TXN_NOT_EXIST;
    if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
      taosMemoryFree(pReq);
      sdbCancelFetch(pSdb, pIter);
      sdbRelease(pSdb, pVgroup);
      TAOS_RETURN(code);
    }
    sdbRelease(pSdb, pVgroup);
  }
  TAOS_RETURN(code);
}

static int32_t mndDropTxn(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, STxnObj *pObj) {
  int32_t code = 0, lino = 0;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "drop-txn");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _exit;
  }

  mInfo("trans:%d start to drop txn:%s", pTrans->id, pObj->name);

  mndTransSetDbName(pTrans, pDb->name, pObj->name);
  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);
  TAOS_CHECK_EXIT(mndTransCheckConflict(pMnode, pTrans));

  mndTransSetOper(pTrans, MND_OPER_DROP_TXN);
  TAOS_CHECK_EXIT(mndSetDropTxnPrepareLogs(pMnode, pTrans, pObj));
  TAOS_CHECK_EXIT(mndSetDropTxnCommitLogs(pMnode, pTrans, pObj));
  TAOS_CHECK_EXIT(mndSetDropTxnRedoActions(pMnode, pTrans, pDb, pObj));

  // int32_t rspLen = 0;
  // void   *pRsp = NULL;
  // TAOS_CHECK_EXIT(mndBuildDropTxnRsp(pObj, &rspLen, &pRsp, false));
  // mndTransSetRpcRsp(pTrans, pRsp, rspLen);

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txn:%s, failed to drop at line:%d since %s", pObj->name, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}
#endif
static int32_t mndProcessDropTxnReq(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  int32_t code = 0, lino = 0;
#ifdef TD_ENTERPRISE
  SDbObj       *pDb = NULL;
  STxnObj     *pObj = NULL;
  SUserObj     *pUser = NULL;
  SMDropTxnReq dropReq = {0};
  int64_t       tss = taosGetTimestampMs();

  TAOS_CHECK_GOTO(tDeserializeSMDropTxnReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _exit);

  mInfo("txn:%s, start to drop", dropReq.name);

  pObj = mndAcquireTxn(pMnode, dropReq.name);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    if (dropReq.igNotExists) {
      code = 0;  // mndBuildDropMountRsp(pObj, &pReq->info.rspLen, &pReq->info.rsp, true);
    }
    goto _exit;
  }

  SName name = {0};
  TAOS_CHECK_EXIT(tNameFromString(&name, pObj->dbFName, T_NAME_ACCT | T_NAME_DB));
  if (!(pDb = mndAcquireDb(pMnode, pObj->dbFName))) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_DB_NOT_EXIST);
  }

  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pUser));

  // TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, RPC_MSG_USER(pReq), MND_OPER_WRITE_DB, pDb), NULL, _exit);
  TAOS_CHECK_EXIT(
      mndCheckObjPrivilegeRecF(pMnode, pUser, PRIV_CM_DROP, PRIV_OBJ_RSMA, pObj->ownerId, pObj->dbFName, pObj->name));

  code = mndDropRsma(pMnode, pReq, pDb, pObj);
  if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  if (tsAuditLevel >= AUDIT_LEVEL_DATABASE) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    auditRecord(pReq, pMnode->clusterId, "dropRsma", dropReq.name, "", "", 0, duration, 0);
  }
_exit:
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("rsma:%s, failed at line %d to drop since %s", dropReq.name, lino, tstrerror(code));
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseTxn(pMnode, pObj);
  mndReleaseUser(pMnode, pUser);
#endif
  TAOS_RETURN(code);
}
#ifdef TD_ENTERPRISE
static int32_t mndCreateTxn(SMnode *pMnode, SRpcMsg *pReq, SUserObj *pUser, SDbObj *pDb, SStbObj *pStb,
                             SMCreateTxnReq *pCreate) {
  int32_t  code = 0, lino = 0;
  STxnObj obj = {0};
  STrans  *pTrans = NULL;

  (void)snprintf(obj.name, TSDB_TABLE_NAME_LEN, "%s", pCreate->name);
  (void)snprintf(obj.dbFName, TSDB_DB_FNAME_LEN, "%s", pDb->name);

  const char *tbName = strrchr(pCreate->tbFName, '.');
  (void)snprintf(obj.tbName, TSDB_TABLE_NAME_LEN, "%s", tbName ? tbName + 1 : pCreate->tbFName);
  (void)snprintf(obj.createUser, TSDB_USER_LEN, "%s", pUser->user);
  obj.ownerId = pUser->uid;
  obj.createdTime = taosGetTimestampMs();
  obj.updateTime = obj.createdTime;
  obj.uid = mndGenerateUid(obj.name, strlen(obj.name));
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
    for (int16_t i = 0; i < obj.nFuncs; ++i) {
      obj.funcColIds[i] = pCreate->funcColIds[i];
      obj.funcIds[i] = pCreate->funcIds[i];
    }
  }

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "create-rsma")),
                  code, lino, _exit, terrno);
  mInfo("trans:%d, used to create rsma %s on tb %s.%s", pTrans->id, obj.name, obj.dbFName, obj.tbName);

  mndTransSetDbName(pTrans, obj.dbFName, obj.name);
  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);
  TAOS_CHECK_EXIT(mndTransCheckConflict(pMnode, pTrans));

  mndTransSetOper(pTrans, MND_OPER_CREATE_RSMA);
  TAOS_CHECK_EXIT(mndSetCreateRsmaPrepareActions(pMnode, pTrans, &obj));
  TAOS_CHECK_EXIT(mndSetCreateRsmaRedoActions(pMnode, pTrans, pDb, pStb, &obj, pCreate));
  TAOS_CHECK_EXIT(mndSetCreateRsmaCommitLogs(pMnode, pTrans, &obj));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("rsma:%s, failed at line %d to create rsma, since %s", obj.name, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  mndRsmaFreeObj(&obj);
  TAOS_RETURN(code);
}

static int32_t mndCheckCreateRsmaReq(SMCreateRsmaReq *pCreate) {
  int32_t code = TSDB_CODE_MND_INVALID_RSMA_OPTION;
  if (pCreate->name[0] == 0) goto _exit;
  if (pCreate->tbFName[0] == 0) goto _exit;
  if (pCreate->igExists < 0 || pCreate->igExists > 1) goto _exit;
  if (pCreate->intervalUnit < 0) goto _exit;
  if (pCreate->interval[0] < 0) goto _exit;
  if (pCreate->interval[1] < 0) goto _exit;
  if (pCreate->interval[0] == 0 && pCreate->interval[1] == 0) goto _exit;

  SName fname = {0};
  if ((code = tNameFromString(&fname, pCreate->tbFName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) < 0) goto _exit;
  if (*(char *)tNameGetTableName(&fname) == 0) goto _exit;
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
      sdbRelease(pSdb, pObj);
      mError("rsma:%s, conflict with existing rsma %s on same table %s.%s:%" PRIi64, pCreate->name, pObj->name,
             pObj->dbFName, pObj->tbName, pObj->tbUid);
      return TSDB_CODE_MND_RSMA_EXIST_IN_TABLE;
    }
    sdbRelease(pSdb, pObj);
  }
  return 0;
}
#endif
static int32_t mndProcessCreateRsmaReq(SRpcMsg *pReq) {
  int32_t code = 0, lino = 0;
#ifdef TD_ENTERPRISE
  SMnode         *pMnode = pReq->info.node;
  SDbObj         *pDb = NULL;
  SStbObj        *pStb = NULL;
  SRsmaObj       *pSma = NULL;
  SUserObj       *pUser = NULL;
  int64_t         mTraceId = TRACE_GET_ROOTID(&pReq->info.traceId);
  SMCreateRsmaReq createReq = {0};
  int64_t         tss = taosGetTimestampMs();

  TAOS_CHECK_EXIT(tDeserializeSMCreateRsmaReq(pReq->pCont, pReq->contLen, &createReq));

  mInfo("start to create rsma: %s", createReq.name);
  TAOS_CHECK_EXIT(mndCheckCreateRsmaReq(&createReq));

  if ((pSma = mndAcquireRsma(pMnode, createReq.name))) {
    if (createReq.igExists) {
      mInfo("rsma:%s, already exist, ignore exist is set", createReq.name);
      code = 0;
      goto _exit;
    } else {
      TAOS_CHECK_EXIT(TSDB_CODE_RSMA_ALREADY_EXISTS);
    }
  } else {
    if ((code = terrno) == TSDB_CODE_RSMA_NOT_EXIST) {
      // continue
    } else {  // TSDB_CODE_MND_RSMA_IN_CREATING | TSDB_CODE_MND_RSMA_IN_DROPPING | TSDB_CODE_APP_ERROR
      goto _exit;
    }
  }

  SName name = {0};
  TAOS_CHECK_EXIT(tNameFromString(&name, createReq.tbFName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));
  char db[TSDB_TABLE_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(&name, db);

  pDb = mndAcquireDb(pMnode, db);
  if (pDb == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_DB_NOT_SELECTED);
  }

  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pUser));

  // TAOS_CHECK_EXIT(mndCheckDbPrivilege(pMnode, RPC_MSG_USER(pReq), MND_OPER_READ_DB, pDb));
  // TAOS_CHECK_EXIT(mndCheckDbPrivilege(pMnode, RPC_MSG_USER(pReq), MND_OPER_WRITE_DB, pDb));

  // already check select table/insert table/create rsma privileges in parser
  TAOS_CHECK_EXIT(mndCheckDbPrivilege(pMnode, RPC_MSG_USER(pReq), RPC_MSG_TOKEN(pReq), MND_OPER_USE_DB, pDb));

  pStb = mndAcquireStb(pMnode, createReq.tbFName);
  if (pStb == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STB_NOT_EXIST);
  }

  TAOS_CHECK_EXIT(mndCheckRsmaConflicts(pMnode, pDb, &createReq));

  TAOS_CHECK_EXIT(mndCreateRsma(pMnode, pReq, pUser, pDb, pStb, &createReq));

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_DATABASE) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    auditRecord(pReq, pMnode->clusterId, "createRsma", createReq.name, createReq.tbFName, "", 0, duration, 0);
  }
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("rsma:%s, failed at line %d to create since %s", createReq.name, lino, tstrerror(code));
  }
  if (pSma) mndReleaseRsma(pMnode, pSma);
  if (pStb) mndReleaseStb(pMnode, pStb);
  if (pDb) mndReleaseDb(pMnode, pDb);
  if (pUser) mndReleaseUser(pMnode, pUser);
  tFreeSMCreateRsmaReq(&createReq);
#endif
  TAOS_RETURN(code);
}