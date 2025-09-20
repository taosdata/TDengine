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
  mndSetMsgHandle(pMnode, TDMT_VND_CREATE_RSMA_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_DROP_RSMA, mndProcessDropRsmaReq);
  mndSetMsgHandle(pMnode, TDMT_VND_DROP_RSMA_RSP, mndTransProcessRsp);
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
      terrno = TSDB_CODE_RSMA_NOT_EXIST;
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

static int32_t mndSetCreateRsmaPrepareActions(SMnode *pMnode, STrans *pTrans, SRsmaObj *pSma) {
  SSdbRaw *pDbRaw = mndRsmaActionEncode(pSma);
  if (pDbRaw == NULL) return -1;

  if (mndTransAppendPrepareLog(pTrans, pDbRaw) != 0) return -1;
  if (sdbSetRawStatus(pDbRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
}

static void *mndBuildVCreateRsmaReq(SMnode *pMnode, SVgObj *pVgroup, SStbObj *pStb, SRsmaObj *pObj,
                                    SMCreateRsmaReq *pCreate, int32_t *pContLen) {
  int32_t         code = 0, lino = 0;
  SMsgHead       *pHead = NULL;
  SVCreateRsmaReq req = *pCreate;

  req.uid = pObj->uid;  // use the uid generated by mnode

  int32_t contLen = tSerializeSVCreateRsmaReq(NULL, 0, &req);
  TAOS_CHECK_EXIT(contLen);
  contLen += sizeof(SMsgHead);
  TSDB_CHECK_NULL((pHead = taosMemoryMalloc(contLen)), code, lino, _exit, terrno);
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);
  void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
  TAOS_CHECK_EXIT(tSerializeSVCreateRsmaReq(pBuf, contLen, &req));
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

static int32_t mndSetCreateRsmaRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb, SRsmaObj *pObj,
                                           SMCreateRsmaReq *pCreate) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;
  SVgObj *pVgroup = NULL;
  void   *pIter = NULL;

  SName name = {0};
  if ((code = tNameFromString(&name, pCreate->tbName, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) != 0) {
    return code;
  }
  tstrncpy(pCreate->tbName, (char *)tNameGetTableName(&name), sizeof(pCreate->name));

  while ((pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup))) {
    if (!mndVgroupInDb(pVgroup, pDb->uid)) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }

    int32_t contLen = 0;
    void   *pReq = mndBuildVCreateRsmaReq(pMnode, pVgroup, pStb, pObj, pCreate, &contLen);
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
    action.msgType = TDMT_VND_CREATE_RSMA;
    action.acceptableCode = TSDB_CODE_RSMA_ALREADY_EXISTS;  // check whether the rsma uid exist
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

static int32_t mndSetDropRsmaPrepareLogs(SMnode *pMnode, STrans *pTrans, SRsmaObj *pSma) {
  int32_t  code = 0;
  SSdbRaw *pRedoRaw = mndRsmaActionEncode(pSma);
  if (pRedoRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    return -1;
  }
  TAOS_CHECK_RETURN(mndTransAppendPrepareLog(pTrans, pRedoRaw));
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

static void *mndBuildVDropRsmaReq(SMnode *pMnode, SVgObj *pVgroup, SRsmaObj *pObj, int32_t *pContLen) {
  int32_t       code = 0, lino = 0;
  SMsgHead     *pHead = NULL;
  SVDropRsmaReq req = {0};

  SName name = {0};
  if ((terrno = tNameFromString(&name, pObj->tbname, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE)) != 0) {
    return NULL;
  }
  (void)snprintf(req.tbName, sizeof(req.tbName), "%s", (char *)tNameGetTableName(&name));
  (void)snprintf(req.name, sizeof(req.name), "%s", pObj->name);
  req.tbType = pObj->tbType;
  req.uid = pObj->uid;
  req.tbUid = pObj->tbUid;

  int32_t contLen = tSerializeSVDropRsmaReq(NULL, 0, &req);
  TAOS_CHECK_EXIT(contLen);
  contLen += sizeof(SMsgHead);
  TSDB_CHECK_NULL((pHead = taosMemoryMalloc(contLen)), code, lino, _exit, terrno);
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(pVgroup->vgId);
  void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
  TAOS_CHECK_EXIT(tSerializeSVDropRsmaReq(pBuf, contLen, &req));
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

static int32_t mndSetDropRsmaRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SRsmaObj *pSma) {
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
    void   *pReq = mndBuildVDropRsmaReq(pMnode, pVgroup, pSma, &contLen);
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
    action.msgType = TDMT_VND_DROP_RSMA;
    action.acceptableCode = TSDB_CODE_RSMA_NOT_EXIST;
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

static int32_t mndDropRsma(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, SRsmaObj *pObj) {
  int32_t code = 0, lino = 0;

  STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "drop-rsma");
  if (pTrans == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    goto _exit;
  }

  mInfo("trans:%d start to drop rsma:%s", pTrans->id, pObj->name);

  mndTransSetDbName(pTrans, pDb->name, pObj->name);
  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);
  TAOS_CHECK_EXIT(mndTransCheckConflict(pMnode, pTrans));

  mndTransSetOper(pTrans, MND_OPER_DROP_RSMA);
  TAOS_CHECK_EXIT(mndSetDropRsmaPrepareLogs(pMnode, pTrans, pObj));
  TAOS_CHECK_EXIT(mndSetDropRsmaCommitLogs(pMnode, pTrans, pObj));
  TAOS_CHECK_EXIT(mndSetDropRsmaRedoActions(pMnode, pTrans, pDb, pObj));

  // int32_t rspLen = 0;
  // void   *pRsp = NULL;
  // TAOS_CHECK_EXIT(mndBuildDropRsmaRsp(pObj, &rspLen, &pRsp, false));
  // mndTransSetRpcRsp(pTrans, pRsp, rspLen);

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("rsma:%s, failed to drop at line:%d since %s", pObj->name, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessDropRsmaReq(SRpcMsg *pReq) {
  SMnode       *pMnode = pReq->info.node;
  int32_t       code = 0, lino = 0;
  SDbObj       *pDb = NULL;
  SRsmaObj     *pObj = NULL;
  SMDropRsmaReq dropReq = {0};

  TAOS_CHECK_GOTO(tDeserializeSMDropRsmaReq(pReq->pCont, pReq->contLen, &dropReq), NULL, _exit);

  mInfo("rsma:%s, start to drop", dropReq.name);

  pObj = mndAcquireRsma(pMnode, dropReq.name);
  if (pObj == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    if (dropReq.igNotExists) {
      code = 0;  // mndBuildDropMountRsp(pObj, &pReq->info.rspLen, &pReq->info.rsp, true);
    }
    goto _exit;
  }

  SName name = {0};
  TAOS_CHECK_EXIT(tNameFromString(&name, dropReq.name, T_NAME_ACCT | T_NAME_DB));

  char db[TSDB_TABLE_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(&name, db);
  if (!(pDb = mndAcquireDb(pMnode, db))) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_DB_NOT_SELECTED);
  }

  TAOS_CHECK_GOTO(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb), NULL, _exit);

  code = mndDropRsma(pMnode, pReq, pDb, pObj);
  if (code == TSDB_CODE_SUCCESS) {
    code = TSDB_CODE_ACTION_IN_PROGRESS;
  }

  auditRecord(pReq, pMnode->clusterId, "dropRsma", dropReq.name, "", "", 0);
_exit:
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("rsma:%s, failed at line %d to drop since %s", dropReq.name, lino, tstrerror(code));
  }

  mndReleaseDb(pMnode, pDb);
  mndReleaseRsma(pMnode, pObj);
  TAOS_RETURN(code);
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

static int32_t mndCreateRsma(SMnode *pMnode, SRpcMsg *pReq, SUserObj *pUser, SDbObj *pDb, SStbObj *pStb,
                             SMCreateRsmaReq *pCreate) {
  int32_t    code = 0, lino = 0;
  SRsmaObj   obj = {0};
  int32_t    nDbs = 0, nVgs = 0, nStbs = 0;
  SDbObj    *pDbs = NULL;
  SStbObj   *pStbs = NULL;
  STrans    *pTrans = NULL;

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
    for (int16_t i = 0; i < obj.nFuncs; ++i) {
      obj.funcColIds[i] = pCreate->funcColIds[i];
      obj.funcIds[i] = pCreate->funcIds[i];
    }
  }

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "create-rsma")),
                  code, lino, _exit, terrno);
  mInfo("trans:%d, used to create rsma %s on tb %s", pTrans->id, obj.name, obj.tbname);

  mndTransSetDbName(pTrans, obj.db, obj.name);
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
  if (pStbs) {
    for (int32_t i = 0; i < nStbs; ++i) {
      mndFreeStb(pStbs + i);
    }
    taosMemFreeClear(pStbs);
  }
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
      sdbRelease(pSdb, pObj);
      mError("rsma:%s, conflict with existing rsma %s on same table %s:%" PRIi64, pCreate->name, pObj->name,
             pObj->tbname, pObj->tbUid);
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
  TAOS_CHECK_EXIT(tNameFromString(&name, createReq.name, T_NAME_ACCT | T_NAME_DB | T_NAME_TABLE));
  char db[TSDB_TABLE_FNAME_LEN] = {0};
  (void)tNameGetFullDbName(&name, db);

  pDb = mndAcquireDb(pMnode, db);
  if (pDb == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_DB_NOT_SELECTED);
  }

  TAOS_CHECK_EXIT(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_READ_DB, pDb));
  TAOS_CHECK_EXIT(mndCheckDbPrivilege(pMnode, pReq->info.conn.user, MND_OPER_WRITE_DB, pDb));

  pStb = mndAcquireStb(pMnode, createReq.tbName);
  if (pStb == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_STB_NOT_EXIST);
  }

  TAOS_CHECK_EXIT(mndCheckRsmaConflicts(pMnode, pDb, &createReq));

  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, pReq->info.conn.user, &pUser));
  TAOS_CHECK_EXIT(mndCreateRsma(pMnode, pReq, pUser, pDb, pStb, &createReq));

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  auditRecord(pReq, pMnode->clusterId, "createRsma", createReq.name, createReq.tbName, "", 0);
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("rsma:%s, failed at line %d to create since %s", createReq.name, lino, tstrerror(code));
  }
  if (pSma) mndReleaseRsma(pMnode, pSma);
  if (pStb) mndReleaseStb(pMnode, pStb);
  if (pDb) mndReleaseDb(pMnode, pDb);
  tFreeSMCreateRsmaReq(&createReq);
  TAOS_RETURN(code);
}

static int32_t mndRetrieveRsma(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode          *pMnode = pReq->info.node;
  int32_t          code = 0, lino = 0;
  int32_t          numOfRows = 0;
  int32_t          cols = 0;
  char             tmp[512];
  int32_t          tmpLen = 0;
  int32_t          bufLen = 0;
  char            *pBuf = NULL;
  char            *qBuf = NULL;
  void            *pIter = NULL;
  SSdb            *pSdb = pMnode->pSdb;
  SColumnInfoData *pColInfo = NULL;

  pBuf = tmp;
  bufLen = sizeof(tmp) - VARSTR_HEADER_SIZE;
  if (pShow->numOfRows < 1) {
    SRsmaObj *pObj = NULL;
    int32_t   index = 0;
    while ((pIter = sdbFetch(pSdb, SDB_RSMA, pIter, (void **)&pObj))) {
      cols = 0;
      pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
      qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
      TAOS_UNUSED(snprintf(qBuf, bufLen, "%s", pObj->name));
      varDataSetLen(pBuf, strlen(pBuf + VARSTR_HEADER_SIZE));
      COL_DATA_SET_VAL_GOTO(pBuf, false, pObj, pIter, _exit);

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        COL_DATA_SET_VAL_GOTO((const char *)(&pObj->uid), false, pObj, pIter, _exit);
      }

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
        const char *db = strstr(pObj->db, ".");
        TAOS_UNUSED(snprintf(qBuf, bufLen, "%s", db ? db + 1 : pObj->db));
        varDataSetLen(pBuf, strlen(qBuf));
        COL_DATA_SET_VAL_GOTO(pBuf, false, pObj, pIter, _exit);
      }

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
        const char *tb = strrchr(pObj->tbname, '.');
        TAOS_UNUSED(snprintf(qBuf, bufLen, "%s", tb ? tb + 1 : pObj->tbname));
        varDataSetLen(pBuf, strlen(qBuf));
        COL_DATA_SET_VAL_GOTO(pBuf, false, pObj, pIter, _exit);
      }

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
        if (pObj->tbType == TSDB_SUPER_TABLE) {
          TAOS_UNUSED(snprintf(qBuf, bufLen, "SUPER_TABLE"));
        } else if (pObj->tbType == TSDB_NORMAL_TABLE) {
          TAOS_UNUSED(snprintf(qBuf, bufLen, "NORMAL_TABLE"));
        } else if (pObj->tbType == TSDB_CHILD_TABLE) {
          TAOS_UNUSED(snprintf(qBuf, bufLen, "CHILD_TABLE"));
        } else {
          TAOS_UNUSED(snprintf(qBuf, bufLen, "UNKNOWN"));
        }
        varDataSetLen(pBuf, strlen(qBuf));
        COL_DATA_SET_VAL_GOTO(pBuf, false, pObj, pIter, _exit);
      }

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        TAOS_UNUSED(snprintf(pBuf, bufLen, "%" PRIi64, pObj->createdTime));
        COL_DATA_SET_VAL_GOTO((const char *)&pObj->createdTime, false, pObj, pIter, _exit);
      }

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
        TAOS_UNUSED(snprintf(qBuf, bufLen, "%" PRIi64 "%c", pObj->interval[0], pObj->intervalUnit));
        if (pObj->interval[1] > 0) {
          tmpLen = strlen(qBuf);
          TAOS_UNUSED(
              snprintf(qBuf + tmpLen, bufLen - tmpLen, ",%" PRIi64 "%c", pObj->interval[1], pObj->intervalUnit));
        }
        varDataSetLen(pBuf, strlen(qBuf));
        COL_DATA_SET_VAL_GOTO(pBuf, false, pObj, pIter, _exit);
      }

      if ((pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols))) {
        qBuf = POINTER_SHIFT(pBuf, VARSTR_HEADER_SIZE);
        TAOS_UNUSED(snprintf(qBuf, bufLen, "%s", pObj->tbname));
        varDataSetLen(pBuf, strlen(qBuf));
        COL_DATA_SET_VAL_GOTO(pBuf, false, pObj, pIter, _exit);
      }

      sdbRelease(pSdb, pObj);
      ++numOfRows;
    }
  }

  pShow->numOfRows += numOfRows;

_exit:
  if (code < 0) {
    mError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }
  return numOfRows;
}

static void mndCancelRetrieveRsma(SMnode *pMnode, void *pIter) {
  SSdb *pSdb = pMnode->pSdb;
  sdbCancelFetchByType(pSdb, pIter, SDB_RSMA);
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


int32_t mndDropRsmasByDb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  SRsmaObj *pObj = NULL;
  void     *pIter = NULL;

  while ((pIter = sdbFetch(pSdb, SDB_RSMA, pIter, (void **)&pObj))) {
    if (pObj->dbUid == pDb->uid) {
      if ((code = mndSetDropRsmaCommitLogs(pMnode, pTrans, pObj)) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pObj);
        TAOS_RETURN(code);
      }
    }
    sdbRelease(pSdb, pObj);
  }

  TAOS_RETURN(code);
}

int32_t mndDropRsmaByStb(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, SStbObj *pStb) {
  int32_t   code = 0;
  SSdb     *pSdb = pMnode->pSdb;
  SRsmaObj *pObj = NULL;
  void     *pIter = NULL;

  while ((pIter = sdbFetch(pSdb, SDB_RSMA, pIter, (void **)&pObj))) {
    if (pObj->tbUid == pStb->uid && pObj->dbUid == pStb->dbUid) {
      if ((code = mndSetDropRsmaCommitLogs(pMnode, pTrans, pObj)) != 0) {
        sdbCancelFetch(pSdb, pIter);
        sdbRelease(pSdb, pObj);
        TAOS_RETURN(code);
      }
    }
    sdbRelease(pSdb, pObj);
  }

  TAOS_RETURN(code);
}
