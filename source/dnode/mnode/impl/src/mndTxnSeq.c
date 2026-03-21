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
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndTrans.h"
#include "mndTxn.h"
#include "parser.h"
#include "tname.h"

#define MND_TXN_SEQ_VER_NUMBER 1

#define TXN_ID_RANGE_STEP          100
#define TXN_ID_RANGE_WATERMARK_PCT 80

static SSdbRaw *mndTxnSeqActionEncode(STxnSeqObj *pObj);
static SSdbRow *mndTxnSeqActionDecode(SSdbRaw *pRaw);
static int32_t  mndTxnSeqActionInsert(SSdb *pSdb, STxnSeqObj *pObj);
static int32_t  mndTxnSeqActionDelete(SSdb *pSdb, STxnSeqObj *pObj);
static int32_t  mndTxnSeqActionUpdate(SSdb *pSdb, STxnSeqObj *pOld, STxnSeqObj *pNew);
static int32_t  mndProcessTxnSeqAllocReq(SRpcMsg *pReq);
static int32_t  initTxnSeq(SMnode *pMnode);
static int32_t  triggerAllocateTxnSeq(SMnode *pMnode);

static utxn_id_t currentTxnId = INT64_MAX;
static int32_t   txnAllocReqNum = 0;

int32_t mndInitTxnSeq(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_TXN_SEQ,
      .keyType = SDB_KEY_INT32,
      .encodeFp = (SdbEncodeFp)mndTxnSeqActionEncode,
      .decodeFp = (SdbDecodeFp)mndTxnSeqActionDecode,
      .insertFp = (SdbInsertFp)mndTxnSeqActionInsert,
      .updateFp = (SdbUpdateFp)mndTxnSeqActionUpdate,
      .deleteFp = (SdbDeleteFp)mndTxnSeqActionDelete,
  };

  mndSetMsgHandle(pMnode, TDMT_MND_ALLOC_TXN_SEQ, mndProcessTxnSeqAllocReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ALLOC_TXN_SEQ_RSP, mndTransProcessRsp);

  TAOS_CHECK_RETURN(sdbSetTable(pMnode->pSdb, table));

  return initTxnSeq(pMnode);
}

void mndCleanupTxnSeq(SMnode *pMnode) {}

static int32_t initTxnSeq(SMnode *pMnode) {
  int32_t     code = 0, lino = 0;
  STxnSeqObj *pObj = NULL;
  int32_t     id = 0;  // fixed id for txn seq object
  if ((code = mndAcquireTxnSeq(pMnode, id, &pObj)) == 0) {
    currentTxnId = pObj->maxRangeId;
    mndReleaseTxnSeq(pMnode, pObj);
  }

  code = triggerAllocateTxnSeq(pMnode);

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    mWarn("txnSeq, failed at line %d to init txn seq since %s", lino, tstrerror(code));
  }
  return code;
}

static int32_t triggerAllocateTxnSeq(SMnode *pMnode) {
  int32_t code = 0, lino = 0;
  if (!mndIsLeader(pMnode)) {
    mWarn("txnSeq, failed at line %d to allocate txn seq since not leader", lino);
    return code;
  }

  int32_t code = 0;
  int32_t lino = 0;
  int32_t contLen = 0;

  TAOS_CHECK_EXIT(tSerializeGrantNotify(NULL, 0, pNotify, &contLen));
  void *pCont = rpcMallocCont(contLen);
  if (!pCont) {
    TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
  }

  if ((code = tSerializeGrantNotify(pCont, contLen, pNotify, NULL)) < 0) {
    rpcFreeCont(pCont);
    TAOS_CHECK_EXIT(code);
  }

  SRpcMsg rpcMsg = {.pCont = pCont, .contLen = contLen, .msgType = TDMT_MND_GRANT_NOTIFY, .info.noResp = 1};

  uDebug("send grant notify msg to dnode:%d %s:%" PRIu16, pDnodeInfo->id, pDnodeInfo->ep.fqdn, pDnodeInfo->ep.port);

  SEpSet epSet = {.numOfEps = 1};
  tstrncpy(epSet.eps[0].fqdn, pDnodeInfo->ep.fqdn, TSDB_FQDN_LEN);
  epSet.eps[0].port = pDnodeInfo->ep.port;
  TAOS_CHECK_EXIT(tmsgSendReq(&epSet, &rpcMsg));

  SRpcMsg rpcMsg = {.msgType = TDMT_MND_ALLOC_TXN_SEQ, .info.ahandle = 0, .info.notFreeAhandle = 1};
  SEpSet  epSet = {0};
  mndGetMnodeEpSet(pMnode, &epSet);
  TAOS_CHECK_EXIT(tmsgSendReq(&epSet, &rpcMsg));
_exit:
  if (code < 0) {
    mError("failed at line %d to allocate txn seq since %s", lino, tstrerror(code));
  }
  TAOS_RETURN(code);
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
  int32_t     code = 0, lino = 0;
  SSdbRow    *pRow = NULL;
  STxnSeqObj *pObj = NULL;
  void       *buf = NULL;

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

int32_t mndAcquireUser(SMnode *pMnode, const char *userName, SUserObj **ppUser) {
  int32_t code = 0;
  SSdb   *pSdb = pMnode->pSdb;

  *ppUser = sdbAcquire(pSdb, SDB_USER, userName);
  if (*ppUser == NULL) {
    if (terrno == TSDB_CODE_SDB_OBJ_NOT_THERE) {
      code = TSDB_CODE_MND_USER_NOT_EXIST;
    } else {
      code = TSDB_CODE_MND_USER_NOT_AVAILABLE;
    }
  }
  TAOS_RETURN(code);
}

int32_t mndAcquireTxnSeq(SMnode *pMnode, int32_t id, STxnSeqObj **ppObj) {
  int32_t     code = 0;
  SSdb       *pSdb = pMnode->pSdb;
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
      mFatal("txnSeq:%" PRIu64 ", failed to acquire txn seq since %s", id, terrstr());
    }
  }
  *ppObj = pObj;
  TAOS_RETURN(code);
}

void mndReleaseTxnSeq(SMnode *pMnode, STxnSeqObj *pObj) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pObj);
}

#if 0
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
#else
utxn_id_t mndGenTxnId(SMnode *pMnode) {
  int32_t     code = 0, lino = 0;
  STxnSeqObj *pObj = NULL;
  TAOS_CHECK_RETURN(mndAcquireTxnSeq(pMnode, 0, &pObj));
  utxn_id_t nextId = -1;
  bool      needAlloc = false;
  taosWLockLatch(&pObj->lock);
  if (pObj->currentId >= pObj->maxRangeId) {
    needAlloc = true;
  } else {
    nextId = ++pObj->currentId;

    utxn_id_t usedInRange = pObj->currentId - (pObj->maxRangeId - TXN_ID_RANGE_STEP);
    if (usedInRange >= (TXN_ID_RANGE_STEP * TXN_ID_RANGE_WATERMARK_PCT / 100)) {
      needAlloc = true;
    }
  }

  if (needAlloc) {
    mInfo("txnSeq, currentId:%" PRIu64 " has reached  maxRangeId:%" PRIu64 ", trigger allocation of new range",
          pObj->currentId, pObj->maxRangeId);
    TAOS_CHECK_EXIT(triggerAllocateTxnSeq(pMnode));
  }

_exit:
  taosWUnlockLatch(&pObj->lock);
  if (code != TSDB_CODE_SUCCESS) {
    mError("txnSeq, failed at line %d to generate txn id since %s", lino, tstrerror(code));
    return code;
  }
  return nextId;
}
#endif

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

static int32_t mndAllocTxnSeq(SMnode *pMnode, SRpcMsg *pReq, utxn_id_t nextTxnRangeStart) {
  int32_t    code = 0, lino = 0;
  STxnSeqObj obj = {0};
  STrans    *pTrans = NULL;

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "alloc-txn-seq")),
                  code, lino, _exit, terrno);
  mInfo("trans:%d, used to allocate txn seq %" PRIu64, pTrans->id, obj.id);

  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);

  mndTransSetOper(pTrans, MND_OPER_ALLOC_TXN_SEQ);
  TAOS_CHECK_EXIT(mndSetCreateTxnSeqCommitLogs(pMnode, pTrans, &obj));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txnSeq:%" PRIu64 ", failed at line %d to allocate txn seq, since %s", obj.id, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

typedef struct {
  utxn_id_t txnId;
} SMTxnSeqReq;

static int32_t tSerializeTxnSeq(void *buf, int32_t bufLen, SMTxnSeqReq *pReq, uint32_t *pLen) {
  int32_t  code = 0;
  int32_t  lino = 0;
  uint32_t tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pReq->txnId));

  tEndEncode(&encoder);

  tlen = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  if (pLen) *pLen = tlen;
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int32_t tDeserializeTxnSeq(void *buf, int32_t bufLen, SMTxnSeqReq *pREq) {
  int32_t  code = 0;
  int32_t  lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pREq->txnId));
_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    uError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }

  TAOS_RETURN(code);
}

static int32_t mndProcessAllocTxnSeqReq(SRpcMsg *pReq) {
  int32_t code = 0, lino = 0;

  SMnode     *pMnode = pReq->info.node;
  STxnSeqObj *pObj = NULL;
  SMTxnSeqReq txnReq = {0};

  TAOS_CHECK_EXIT(tDeserializeTxnSeq(pReq->pCont, pReq->contLen, &txnReq));

  mInfo("start to allocate txn seq: %" PRIu64, txnReq.txnId);

  TAOS_CHECK_EXIT(mndAllocTxnSeq(pMnode, pReq, &txnReq));

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txnSeq:%" PRIu64 ", failed at line %d to allocate txn seq since %s", txnReq.txnId, lino, tstrerror(code));
  }
  if (pObj) mndReleaseTxn(pMnode, pObj);

  TAOS_RETURN(code);
}
