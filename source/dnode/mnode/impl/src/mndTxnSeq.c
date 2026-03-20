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
#include "mndTxn.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndTrans.h"
#include "parser.h"
#include "tname.h"

#define MND_TXN_SEQ_VER_NUMBER   1

static SSdbRaw *mndTxnSeqActionEncode(STxnSeqObj *pObj);
static SSdbRow *mndTxnSeqActionDecode(SSdbRaw *pRaw);
static int32_t  mndTxnSeqActionInsert(SSdb *pSdb, STxnSeqObj *pObj);
static int32_t  mndTxnSeqActionDelete(SSdb *pSdb, STxnSeqObj *pObj);
static int32_t  mndTxnSeqActionUpdate(SSdb *pSdb, STxnSeqObj *pOld, STxnSeqObj *pNew);
static int32_t  mndProcessTxnIdAllocReq(SRpcMsg *pReq);


int32_t mndInitTxn(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_TXN_SEQ,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndTxnSeqActionEncode,
      .decodeFp = (SdbDecodeFp)mndTxnSeqActionDecode,
      .insertFp = (SdbInsertFp)mndTxnSeqActionInsert,
      .updateFp = (SdbUpdateFp)mndTxnSeqActionUpdate,
      .deleteFp = (SdbDeleteFp)mndTxnSeqActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_ALLOC_TXN_ID, mndProcessTxnIdAllocReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALLOC_TXN_ID_RSP, mndTransProcessRsp);


  return sdbSetTable(pMnode->pSdb, table);
}

static int32_t tSerializeSTxnSeqObj(void *buf, int32_t bufLen, const STxnSeqObj *pObj) {
  int32_t  code = 0, lino = 0;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pObj->id));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->maxRangeId));

  tEndEncode(&encoder);

  tlen = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  if (code < 0) {
    mError("txnSeq, %s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }

  return tlen;
}

static int32_t tDeserializeSTxnSeqObj(void *buf, int32_t bufLen, STxnSeqObj *pObj) {
  int32_t  code = 0, lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pObj->id));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->maxRangeId));

_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("txnSeq, %s failed at line %d since %s, row:%p", __func__, lino, tstrerror(code), pObj);
  }
  TAOS_RETURN(code);
}

static SSdbRaw *mndTxnSeqActionEncode(STxnSeqObj *pObj) {
  int32_t  code = 0, lino = 0;
  void    *buf = NULL;
  SSdbRaw *pRaw = NULL;
  int32_t  tlen = tSerializeSTxnSeqObj(NULL, 0, pObj);
  if (tlen < 0) {
    TAOS_CHECK_EXIT(tlen);
  }

  int32_t size = sizeof(int32_t) + tlen;
  pRaw = sdbAllocRaw(SDB_TXN_SEQ, MND_TXN_SEQ_VER_NUMBER, size);
  if (pRaw == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  buf = taosMemoryMalloc(tlen);
  if (buf == NULL) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  tlen = tSerializeSTxnSeqObj(buf, tlen, pObj);
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
    mError("txnSeq, failed at line %d to encode to raw:%p since %s", lino, pRaw, tstrerror(code));
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("txnSeq, encode to raw:%p, row:%p", pRaw, pObj);
  return pRaw;
}

SSdbRow *mndTxnSeqActionDecode(SSdbRaw *pRaw) {
  int32_t  code = 0, lino = 0;
  SSdbRow *pRow = NULL;
  STxnSeqObj *pObj = NULL;
  void    *buf = NULL;

  int8_t sver = 0;
  TAOS_CHECK_EXIT(sdbGetRawSoftVer(pRaw, &sver));

  if (sver != MND_TXN_SEQ_VER_NUMBER) {
    mError("txn read invalid ver, data ver: %d, curr ver: %d", sver, MND_TXN_SEQ_VER_NUMBER);
    TAOS_CHECK_EXIT(TSDB_CODE_SDB_INVALID_DATA_VER);
  }

  if (!(pRow = sdbAllocRow(sizeof(STxnSeqObj)))) {
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

  TAOS_CHECK_EXIT(tDeserializeSTxnSeqObj(buf, tlen, pObj));

  taosInitRWLatch(&pObj->lock);

_exit:
  taosMemoryFreeClear(buf);
  if (code != TSDB_CODE_SUCCESS) {
    terrno = code;
    mError("txnSeq, failed at line %d to decode from raw:%p since %s", lino, pRaw, tstrerror(code));
    mndTxnFreeObj(pObj);
    taosMemoryFreeClear(pRow);
    return NULL;
  }
  mTrace("txnSeq, decode from raw:%p, row:%p", pRaw, pObj);
  return pRow;
}

static int32_t mndTxnSeqActionInsert(SSdb *pSdb, STxnSeqObj *pObj) {
  mTrace("txnSeq:%" PRIu64 ", perform insert action, row:%p", pObj->id, pObj);
  return 0;
}

static int32_t mndTxnSeqActionDelete(SSdb *pSdb, STxnSeqObj *pObj) {
  mTrace("txnSeq:%" PRIu64 ", perform delete action, row:%p", pObj->id, pObj);
  mndTxnFreeObj(pObj);
  return 0;
}

static int32_t mndTxnSeqActionUpdate(SSdb *pSdb, STxnSeqObj *pOld, STxnSeqObj *pNew) {
  mTrace("txnSeq:%" PRIu64 ", perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->id = pNew->id;
  pOld->maxRangeId = pNew->maxRangeId;
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

STxnSeqObj *mndAcquireTxn(SMnode *pMnode, utxn_id_t id) {
  SSdb    *pSdb = pMnode->pSdb;
  STxnSeqObj *pObj = sdbAcquire(pSdb, SDB_TXN_SEQ, &id);
  if (pObj == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      terrno = TSDB_CODE_TXN_NOT_EXIST;
    } else if (terrno == TSDB_CODE_SDB_OBJ_CREATING) {
      terrno = TSDB_CODE_MND_TXN_IN_CREATING;
    } else if (terrno == TSDB_CODE_SDB_OBJ_DROPPING) {
      terrno = TSDB_CODE_MND_TXN_IN_DROPPING;
    } else {
      terrno = TSDB_CODE_APP_ERROR;
      mFatal("txn:%" PRIu64 ", failed to acquire txn since %s", id, terrstr());
    }
  }
  return pObj;
}

void mndReleaseTxn(SMnode *pMnode, STxnSeqObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

/**
 * @brief Non thread safe. Return unique id with format: 40(sec) + 12(nodeId) + 4(reserved) + 8(seqId)  bits.
 *
 */
int64_t mndGenTxnId(int32_t nodeId) {
  static int64_t lastSec = 0;
  static int32_t seqId = 0;

  int64_t sec = taosGetTimestampSec();

  // Make sure upper 32 bits is not all 0 to avoid conflicts with id in STrans(mndDef.h)
  if (((sec & 0xFFFFFFFFFFLL) >> 8) == 0) {
    sec += 0x100;
  }

  if (sec < lastSec) {
    sec = lastSec;
  }

  if (sec == lastSec) {
    if (seqId >= 255) {
      ++sec;
      seqId = 0;
    } else {
      ++seqId;
    }
  } else {
    seqId = 0;
  }
  lastSec = sec;

  uint64_t x = (uint64_t)(sec & 0xFFFFFFFFFFLL) << 24;
  uint64_t n = (uint64_t)(nodeId & 0xFFF) << 12;
  uint64_t s = (uint64_t)(seqId & 0xFF);

  int64_t uuid = x | n | s;
  return uuid;
}


static int32_t mndSetCreateTxnSeqCommitLogs(SMnode *pMnode, STrans *pTrans, STxnSeqObj *pObj) {
  int32_t  code = 0;
  SSdbRaw *pCommitRaw = mndTxnSeqActionEncode(pObj);
  if (pCommitRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }
  TAOS_CHECK_RETURN(mndTransAppendCommitlog(pTrans, pCommitRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pCommitRaw, SDB_STATUS_READY));

  TAOS_RETURN(code);
}

static int32_t mndAllocTxnId(SMnode *pMnode, SRpcMsg *pReq, SMTransReq *pTransReq) {
  int32_t code = 0, lino = 0;
  STxnSeqObj obj = {0};
  STrans *pTrans = NULL;

  

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "alloc-txn-id")), code,
                  lino, _exit, terrno);
  mInfo("trans:%d, used to allocate txn id %" PRIu64, pTrans->id, obj.id);

  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);

  mndTransSetOper(pTrans, MND_OPER_ALLOC_TXN_ID);
  TAOS_CHECK_EXIT(mndSetCreateTxnSeqCommitLogs(pMnode, pTrans, &obj));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txnSeq:%" PRIu64 ", failed at line %d to allocate txn id, since %s", obj.id, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessAllocTxnIdReq(SRpcMsg *pReq) {
  int32_t code = 0, lino = 0;

  SMnode    *pMnode = pReq->info.node;
  STxnSeqObj   *pObj = NULL;
  SMTransReq txnReq = {0};

  TAOS_CHECK_EXIT(tDeserializeSMTransReq(pReq->pCont, pReq->contLen, &txnReq));

  mInfo("start to allocate txn id: %" PRIu64, txnReq.txnId);

  if ((pObj = mndAcquireTxn(pMnode, txnReq.txnId))) {
    TAOS_CHECK_EXIT(TSDB_CODE_TXN_ALREADY_EXISTS);
  }

  TAOS_CHECK_EXIT(mndAllocTxnId(pMnode, pReq, &txnReq));

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txnSeq:%" PRIu64 ", failed at line %d to allocate txn id since %s", txnReq.txnId, lino, tstrerror(code));
  }
  if (pObj) mndReleaseTxn(pMnode, pObj);

  TAOS_RETURN(code);
}
