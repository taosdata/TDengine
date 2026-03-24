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
#include "mndInt.h"
#include "mndTxnSeq.h"
#include "audit.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndSync.h"
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
static int32_t  mndProcessBeginTxnReq(SRpcMsg *pReq);
static int32_t  mndProcessCommitTxnReq(SRpcMsg *pReq);
static int32_t  mndProcessRollbackTxnReq(SRpcMsg *pReq);

static int32_t mndRetrieveTxn(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelRetrieveTxn(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveTxnTask(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelRetrieveTxnTask(SMnode *pMnode, void *pIter);

// 超时扫描函数（由外部定期调用）
static void mndTxnTimeoutScanImpl(SMnode *pMnode);

int32_t mndInitTxn(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_TXN,
      .keyType = SDB_KEY_INT64,
      .encodeFp = (SdbEncodeFp)mndTxnActionEncode,
      .decodeFp = (SdbDecodeFp)mndTxnActionDecode,
      .insertFp = (SdbInsertFp)mndTxnActionInsert,
      .updateFp = (SdbUpdateFp)mndTxnActionUpdate,
      .deleteFp = (SdbDeleteFp)mndTxnActionDelete,
  };

  // 初始化 STxnMgmt 运行时管理结构
  STxnMgmt *pMgmt = &pMnode->txnMgmt;
  pMgmt->pTxnHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  if (pMgmt->pTxnHash == NULL) {
    mError("txn, failed to init txn hash");
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosThreadRwlockInit(&pMgmt->lock, NULL);
  pMgmt->currentTxnId = -1;
  pMgmt->hTimeoutTimer = NULL;  // 定时器在 mnode 启动完成后再启动

  mndSetMsgHandle(pMnode, TDMT_MND_BEGIN_TXN, mndProcessBeginTxnReq);
  mndSetMsgHandle(pMnode, TDMT_MND_BEGIN_TXN_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_COMMIT_TXN, mndProcessCommitTxnReq);
  mndSetMsgHandle(pMnode, TDMT_MND_COMMIT_TXN_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_ROLLBACK_TXN, mndProcessRollbackTxnReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ROLLBACK_TXN_RSP, mndTransProcessRsp);

  //   mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TXN, mndRetrieveTxn);
  //   mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TXN, mndCancelRetrieveTxn);

  return sdbSetTable(pMnode->pSdb, table);
}

// 超时扫描实现（由外部定期调用）
static void mndTxnTimeoutScanImpl(SMnode *pMnode) {
  // TODO: 遍历 SDB 中所有活跃事务，检查超时并触发 ROLLBACK
  int64_t now = taosGetTimestampMs();
  mTrace("txn, timeout scan executed at %" PRId64, now);
}

// 手动触发超时扫描（供 mndMain.c 定期调用）
void mndTxnDoTimeoutScan(SMnode *pMnode) {
  mndTxnTimeoutScanImpl(pMnode);
}

// 启动超时扫描（暂时为空实现，未来可改为定时器）
int32_t mndStartTxnTimer(SMnode *pMnode) {
  // 暂时不使用定时器，超时扫描由 mndMain.c 的定期任务调用
  mInfo("txn, timeout scan ready (no timer, will be called periodically)");
  return 0;
}

void mndCleanupTxn(SMnode *pMnode) {
  STxnMgmt *pMgmt = &pMnode->txnMgmt;
  if (pMgmt->hTimeoutTimer) {
    taosTmrStop(pMgmt->hTimeoutTimer);
    pMgmt->hTimeoutTimer = NULL;
  }
  taosThreadRwlockDestroy(&pMgmt->lock);
  if (pMgmt->pTxnHash) {
    // TODO: 遍历并释放所有 SUserTxn
    taosHashCleanup(pMgmt->pTxnHash);
    pMgmt->pTxnHash = NULL;
  }
}

// MNode 侧用户事务阶段名称，用于日志输出
const char *mndUtxnStageStr(EUtxnStage stage) {
  switch (stage) {
    case UTXN_STAGE_IDLE:        return "IDLE";
    case UTXN_STAGE_ACTIVE:      return "ACTIVE";
    case UTXN_STAGE_PREPARING:   return "PREPARING";
    case UTXN_STAGE_DECIDING:    return "DECIDING";
    case UTXN_STAGE_COMMITTING:  return "COMMITTING";
    case UTXN_STAGE_ROLLINGBACK: return "ROLLINGBACK";
    case UTXN_STAGE_COMPLETED:   return "COMPLETED";
    case UTXN_STAGE_ZOMBIE:      return "ZOMBIE";
    default:                     return "UNKNOWN";
  }
}

// VNode 侧事务阶段名称，用于日志输出
const char *mndVtxnStageStr(EVtxnStage stage) {
  switch (stage) {
    case VTXN_STAGE_NONE:      return "NONE";
    case VTXN_STAGE_ACTIVE:    return "ACTIVE";
    case VTXN_STAGE_PREPARED:  return "PREPARED";
    case VTXN_STAGE_FINISHING: return "FINISHING";
    default:                   return "UNKNOWN";
  }
}

void mndTxnFreeObj(STxnObj *pObj) {
  if (pObj) {
    if (pObj->pVgList) {
      taosArrayDestroy(pObj->pVgList);
      pObj->pVgList = NULL;
    }
  }
}

static int32_t tSerializeSTxnObj(void *buf, int32_t bufLen, const STxnObj *pObj) {
  int32_t  code = 0, lino = 0;
  int32_t  tlen = 0;
  SEncoder encoder = {0};
  tEncoderInit(&encoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartEncode(&encoder));

  TAOS_CHECK_EXIT(tEncodeU64v(&encoder, pObj->id));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->createUser));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->ownerId));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->createTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->lastActiveTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->term));  // 添加 term 序列化
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pObj->timeoutSec));
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pObj->stage));

  // 序列化 pVgList（VGroup 参与者列表）
  int32_t vgNum = pObj->pVgList ? taosArrayGetSize(pObj->pVgList) : 0;
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, vgNum));
  for (int32_t i = 0; i < vgNum; ++i) {
    int32_t vgId = *(int32_t *)taosArrayGet(pObj->pVgList, i);
    TAOS_CHECK_EXIT(tEncodeI32v(&encoder, vgId));
  }

  tEndEncode(&encoder);

  tlen = encoder.pos;
_exit:
  tEncoderClear(&encoder);
  if (code < 0) {
    mError("txn, %s failed at line %d since %s", __func__, lino, tstrerror(code));
    TAOS_RETURN(code);
  }

  return tlen;
}

static int32_t tDeserializeSTxnObj(void *buf, int32_t bufLen, STxnObj *pObj) {
  int32_t  code = 0, lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeU64v(&decoder, &pObj->id));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->createUser));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->ownerId));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->createTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->lastActiveTime));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->term));  // 添加 term 反序列化
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &pObj->timeoutSec));
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, (int32_t *)&pObj->stage));

  // 反序列化 pVgList（VGroup 参与者列表）
  int32_t vgNum = 0;
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &vgNum));
  if (vgNum > 0) {
    pObj->pVgList = taosArrayInit(vgNum, sizeof(int32_t));
    if (pObj->pVgList == NULL) {
      TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
    }
    for (int32_t i = 0; i < vgNum; ++i) {
      int32_t vgId = 0;
      TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &vgId));
      if (taosArrayPush(pObj->pVgList, &vgId) == NULL) {
        TAOS_CHECK_EXIT(TSDB_CODE_OUT_OF_MEMORY);
      }
    }
  }

_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("txn, %s failed at line %d since %s, row:%p", __func__, lino, tstrerror(code), pObj);
    if (pObj->pVgList) {
      taosArrayDestroy(pObj->pVgList);
      pObj->pVgList = NULL;
    }
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
  pRaw = sdbAllocRaw(SDB_TXN, MND_TXN_VER_NUMBER, size);
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
    mError("txn, failed at line %d to encode to raw:%p since %s", lino, pRaw, tstrerror(code));
    sdbFreeRaw(pRaw);
    return NULL;
  }

  mTrace("txn, encode to raw:%p, row:%p", pRaw, pObj);
  return pRaw;
}

SSdbRow *mndTxnActionDecode(SSdbRaw *pRaw) {
  int32_t  code = 0, lino = 0;
  SSdbRow *pRow = NULL;
  STxnObj *pObj = NULL;
  void    *buf = NULL;

  int8_t sver = 0;
  TAOS_CHECK_EXIT(sdbGetRawSoftVer(pRaw, &sver));

  if (sver != MND_TXN_VER_NUMBER) {
    mError("txn read invalid ver, data ver: %d, curr ver: %d", sver, MND_TXN_VER_NUMBER);
    TAOS_CHECK_EXIT(TSDB_CODE_SDB_INVALID_DATA_VER);
  }

  if (!(pRow = sdbAllocRow(sizeof(STxnObj)))) {
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
  mTrace("txn:%" PRIu64 ", perform insert action, row:%p", pObj->id, pObj);
  return 0;
}

static int32_t mndTxnActionDelete(SSdb *pSdb, STxnObj *pObj) {
  mTrace("txn:%" PRIu64 ", perform delete action, row:%p", pObj->id, pObj);
  mndTxnFreeObj(pObj);
  return 0;
}

static int32_t mndTxnActionUpdate(SSdb *pSdb, STxnObj *pOld, STxnObj *pNew) {
  mTrace("txn:%" PRIu64 ", perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  pOld->timeoutSec = pNew->timeoutSec;
  pOld->stage = pNew->stage;
  pOld->lastActiveTime = pNew->lastActiveTime;
  taosWUnLockLatch(&pOld->lock);
  return 0;
}

STxnObj *mndAcquireTxn(SMnode *pMnode, utxn_id_t id) {
  SSdb    *pSdb = pMnode->pSdb;
  STxnObj *pObj = sdbAcquire(pSdb, SDB_TXN, &id);
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

void mndReleaseTxn(SMnode *pMnode, STxnObj *pTxn) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pTxn);
}

const char *mndTxnStr(EUtxnStage stage) { return mndUtxnStageStr(stage); }

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
#if 0
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
#endif
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
#if 0
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
//   SVDropTxnReq req = {0};

//   (void)snprintf(req.tbName, sizeof(req.tbName), "%s", pObj->tbName);
//   (void)snprintf(req.name, sizeof(req.name), "%s", pObj->name);
//   req.tbType = pObj->tbType;
//   req.uid = pObj->uid;
//   req.tbUid = pObj->tbUid;

//   int32_t contLen = tSerializeSVDropTxnReq(NULL, 0, &req);
//   TAOS_CHECK_EXIT(contLen);
//   contLen += sizeof(SMsgHead);
//   TSDB_CHECK_NULL((pHead = taosMemoryMalloc(contLen)), code, lino, _exit, terrno);
//   pHead->contLen = htonl(contLen);
//   pHead->vgId = htonl(pVgroup->vgId);
//   void *pBuf = POINTER_SHIFT(pHead, sizeof(SMsgHead));
//   TAOS_CHECK_EXIT(tSerializeSVDropTxnReq(pBuf, contLen, &req));
// _exit:
//   if (code < 0) {
//     taosMemoryFreeClear(pHead);
//     terrno = code;
//     *pContLen = 0;
//     return NULL;
//   }
//   *pContLen = contLen;
  return pHead;
}

static int32_t mndSetDropTxnRedoActions(SMnode *pMnode, STrans *pTrans, SDbObj *pDb, STxnObj *pTxn) {
  int32_t code = 0;
  // SSdb   *pSdb = pMnode->pSdb;
  // SVgObj *pVgroup = NULL;
  // void   *pIter = NULL;

  // while ((pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup))) {
  //   if (!mndVgroupInDb(pVgroup, pDb->uid)) {
  //     sdbRelease(pSdb, pVgroup);
  //     continue;
  //   }

  //   int32_t contLen = 0;
  //   void   *pReq = mndBuildVDropTxnReq(pMnode, pVgroup, pTxn, &contLen);
  //   if (pReq == NULL) {
  //     sdbCancelFetch(pSdb, pIter);
  //     sdbRelease(pSdb, pVgroup);
  //     code = terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL;
  //     TAOS_RETURN(code);
  //   }

  //   STransAction action = {0};
  //   action.mTraceId = pTrans->mTraceId;
  //   action.epSet = mndGetVgroupEpset(pMnode, pVgroup);
  //   action.pCont = pReq;
  //   action.contLen = contLen;
  //   action.msgType = TDMT_VND_DROP_TXN;
  //   action.acceptableCode = TSDB_CODE_TXN_NOT_EXIST;
  //   if ((code = mndTransAppendRedoAction(pTrans, &action)) != 0) {
  //     taosMemoryFree(pReq);
  //     sdbCancelFetch(pSdb, pIter);
  //     sdbRelease(pSdb, pVgroup);
  //     TAOS_RETURN(code);
  //   }
  //   sdbRelease(pSdb, pVgroup);
  // }
  TAOS_RETURN(code);
}

static int32_t mndDropTxn(SMnode *pMnode, SRpcMsg *pReq, SDbObj *pDb, STxnObj *pObj) {
  int32_t code = 0, lino = 0;

//   STrans *pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_DB_INSIDE, pReq, "drop-txn");
//   if (pTrans == NULL) {
//     code = TSDB_CODE_MND_RETURN_VALUE_NULL;
//     if (terrno != 0) code = terrno;
//     goto _exit;
//   }

//   mInfo("trans:%d start to drop txn:%s", pTrans->id, pObj->name);

//   mndTransSetDbName(pTrans, pDb->name, pObj->name);
//   mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);
//   TAOS_CHECK_EXIT(mndTransCheckConflict(pMnode, pTrans));

//   mndTransSetOper(pTrans, MND_OPER_DROP_TXN);
//   TAOS_CHECK_EXIT(mndSetDropTxnPrepareLogs(pMnode, pTrans, pObj));
//   TAOS_CHECK_EXIT(mndSetDropTxnCommitLogs(pMnode, pTrans, pObj));
//   TAOS_CHECK_EXIT(mndSetDropTxnRedoActions(pMnode, pTrans, pDb, pObj));

//   // int32_t rspLen = 0;
//   // void   *pRsp = NULL;
//   // TAOS_CHECK_EXIT(mndBuildDropTxnRsp(pObj, &rspLen, &pRsp, false));
//   // mndTransSetRpcRsp(pTrans, pRsp, rspLen);

//   TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
// _exit:
//   if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
//     mError("txn:%s, failed to drop at line:%d since %s", pObj->name, lino, tstrerror(code));
//   }
//   mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessDropTxnReq(SRpcMsg *pReq) {
  SMnode *pMnode = pReq->info.node;
  int32_t code = 0, lino = 0;
#if 0
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

#endif

static int32_t mndBeginTxn(SMnode *pMnode, SRpcMsg *pReq, SUserObj *pUser, SMTransReq *pTransReq) {
  int32_t code = 0, lino = 0;
  STxnObj obj = {0};
  STrans *pTrans = NULL;

  (void)snprintf(obj.createUser, TSDB_USER_LEN, "%s", pUser->user);
  obj.ownerId = pUser->uid;
  obj.id = pTransReq->txnId;
  obj.createTime = taosGetTimestampMs();
  obj.lastActiveTime = obj.createTime;
  obj.timeoutSec = 30;  // pReq->timeoutSec;
  obj.stage = UTXN_STAGE_ACTIVE;

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "begin-txn")), code,
                  lino, _exit, terrno);
  mInfo("trans:%d, used to create txn %" PRIu64, pTrans->id, obj.id);

  // mndTransSetDbName(pTrans, obj.dbFName, obj.name);
  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);
  TAOS_CHECK_EXIT(mndTransCheckConflict(pMnode, pTrans));

  mndTransSetOper(pTrans, MND_OPER_BEGIN_TXN);
  // TAOS_CHECK_EXIT(mndSetCreateRsmaPrepareActions(pMnode, pTrans, &obj));
  // TAOS_CHECK_EXIT(mndSetCreateRsmaRedoActions(pMnode, pTrans, pDb, pStb, &obj, pReq));
  TAOS_CHECK_EXIT(mndSetCreateTxnCommitLogs(pMnode, pTrans, &obj));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txn:%" PRIu64 ", failed at line %d to begin txn, since %s", obj.id, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndProcessBeginTxnReq(SRpcMsg *pReq) {
  int32_t code = 0, lino = 0;

  SMnode    *pMnode = pReq->info.node;
  STxnObj   *pTxn = NULL;
  SUserObj  *pOperUser = NULL;
  int64_t    mTraceId = TRACE_GET_ROOTID(&pReq->info.traceId);
  SMTransReq txnReq = {0};
  int64_t    tss = taosGetTimestampMs();

  TAOS_CHECK_EXIT(tDeserializeSMTransReq(pReq->pCont, pReq->contLen, &txnReq));

  if (txnReq.txnId != 0) {
    mInfo("txn:%" PRIu64 ", is already beginning, ignore begin request", txnReq.txnId);
    code = 0;
    goto _exit;
  } else {
    txnReq.txnId = mndGenTxnId(pMnode);
    if (txnReq.txnId < 0) {
      code = (int32_t)txnReq.txnId;
      goto _exit;
    }
  }
  mInfo("start to begin txn: %" PRIu64, txnReq.txnId);
  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pOperUser));
  pTxn = mndAcquireTxn(pMnode, txnReq.txnId);
  if (pTxn != NULL) {
    // 事务已存在，幂等返回成功（客户端重试场景）
    mInfo("txn:%" PRIu64 ", already exists, return success", txnReq.txnId);
    mndReleaseTxn(pMnode, pTxn);
    pTxn = NULL;
    goto _exit;
  }
  terrno = 0;  // 清除 sdbAcquire 设置的 terrno

  TAOS_CHECK_EXIT(mndBeginTxn(pMnode, pReq, pOperUser, &txnReq));

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_SYSTEM) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    // auditRecord(pReq, pMnode->clusterId, "createTxn", txnReq.name, txnReq.tbFName, "", 0, duration, 0);
  }
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txn:%" PRIu64 ", failed at line %d to begin since %s", txnReq.txnId, lino, tstrerror(code));
  }
  if (pTxn) mndReleaseTxn(pMnode, pTxn);
  mndReleaseUser(pMnode, pOperUser);

  TAOS_RETURN(code);
}

static int32_t mndProcessCommitTxnReq(SRpcMsg *pReq) {
  int32_t code = 0, lino = 0;

  SMnode    *pMnode = pReq->info.node;
  STxnObj   *pTxn = NULL;
  int64_t    mTraceId = TRACE_GET_ROOTID(&pReq->info.traceId);
  SMTransReq txnReq = {0};
  int64_t    tss = taosGetTimestampMs();

  TAOS_CHECK_EXIT(tDeserializeSMTransReq(pReq->pCont, pReq->contLen, &txnReq));

  if (txnReq.txnId == 0) {
    mInfo("txn:%" PRIu64 ", is invalid, ignore commit request", txnReq.txnId);
    goto _exit;
  }
  mInfo("start to commit txn: %" PRIu64, txnReq.txnId);
  pTxn = mndAcquireTxn(pMnode, txnReq.txnId);
  if (pTxn == NULL) {
    mError("txn:%" PRIu64 ", not found, cannot commit", txnReq.txnId);
    TAOS_CHECK_EXIT(TSDB_CODE_TXN_NOT_EXIST);
  }
  if (pTxn->stage != UTXN_STAGE_ACTIVE) {
    mError("txn:%" PRIu64 ", stage=%s, cannot commit", txnReq.txnId, mndUtxnStageStr(pTxn->stage));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_TXN_INVALID_STAGE);
  }

  // TODO: 向 pVgList 中所有 VGroup 广播 PREPARE，等待 ACK 后进入 DECIDING 阶段
  // TAOS_CHECK_EXIT(mndCommitTxn(pMnode, pReq, pTxn));

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_SYSTEM) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    // auditRecord(pReq, pMnode->clusterId, "createTxn", txnReq.name, txnReq.tbFName, "", 0, duration, 0);
  }
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txn:%" PRIu64 ", failed at line %d to commit since %s", txnReq.txnId, lino, tstrerror(code));
  }
  if (pTxn) mndReleaseTxn(pMnode, pTxn);
  // tFreeSMTransReq(&txnReq);

  TAOS_RETURN(code);
}

static int32_t mndProcessRollbackTxnReq(SRpcMsg *pReq) {
  int32_t code = 0, lino = 0;

  SMnode    *pMnode = pReq->info.node;
  STxnObj   *pTxn = NULL;
  SUserObj  *pOperUser = NULL;
  int64_t    mTraceId = TRACE_GET_ROOTID(&pReq->info.traceId);
  SMTransReq txnReq = {0};
  int64_t    tss = taosGetTimestampMs();

  TAOS_CHECK_EXIT(tDeserializeSMTransReq(pReq->pCont, pReq->contLen, &txnReq));

  if (txnReq.txnId == 0) {
    mInfo("txn:%" PRIu64 ", is invalid, ignore rollback request", txnReq.txnId);
    code = 0;
    goto _exit;
  }
  mInfo("start to rollback txn: %" PRIu64, txnReq.txnId);
  pTxn = mndAcquireTxn(pMnode, txnReq.txnId);
  if (pTxn == NULL) {
    // 事务不存在，幂等返回成功（已经回滚完成的场景）
    mInfo("txn:%" PRIu64 ", not found, treat as already rolled back", txnReq.txnId);
    terrno = 0;
    goto _exit;
  }
  if (pTxn->stage == UTXN_STAGE_COMMITTING || pTxn->stage == UTXN_STAGE_DECIDING) {
    mError("txn:%" PRIu64 ", stage=%s, cannot rollback after commit decision", txnReq.txnId,
           mndUtxnStageStr(pTxn->stage));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_TXN_INVALID_STAGE);
  }

  // TODO: 向 pVgList 中所有 VGroup 广播 ROLLBACK，等待 ACK 后删除 SDB 记录
  // TAOS_CHECK_EXIT(mndRollbackTxn(pMnode, pReq, pTxn));

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_SYSTEM) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    // auditRecord(pReq, pMnode->clusterId, "createTxn", txnReq.name, txnReq.tbFName, "", 0, duration, 0);
  }
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txn:%" PRIu64 ", failed at line %d to rollback since %s", txnReq.txnId, lino, tstrerror(code));
  }
  if (pTxn) mndReleaseTxn(pMnode, pTxn);
  mndReleaseUser(pMnode, pOperUser);

  TAOS_RETURN(code);
}



#if 0
// 伪代码：处理 VNode 的心跳反馈
void mndOnVnodeHeartbeat(STxnCtx *pCtx, int32_t vnodeId, EUtxnStatus vnodeStatus) {
    switch (pCtx->status) {
        case UTXN_STATUS_PREPARING:
            if (vnodeStatus == VNODE_PREPARED) {
                markAck(pCtx, vnodeId);
                if (isAllAcked(pCtx)) {
                    // 只有这里，才触发一次 MNode 的 WAL 写入
                    pCtx->status = UTXN_STATUS_PREPARED;
                    mndPersistTxn(pCtx); 
                }
            }
            break;

        case UTXN_STATUS_COMMITTING:
            if (vnodeStatus == VNODE_COMMITTED) {
                markAck(pCtx, vnodeId);
                if (isAllAcked(pCtx)) {
                    // 事务彻底完成，记录终态，移除 Hash
                    pCtx->status = UTXN_STATUS_COMMITTED;
                    mndFinalizeTxn(pCtx); 
                }
            }
            break;
    }
}

// 这是一个专门用来打印“谁在拖后腿”的工具函数
void mndLogTxnProgress(SUserTxn *pTxn) {
  int64_t now = taosGetTimestampMs();
  
  // 只有处于中间态（COMMITTING/ROLLBACKING）且超过 30 秒没动静才报警
  if (pTxn->status != UTXN_STATUS_COMMITTING && pTxn->status != UTXN_STATUS_ROLLBACKING) {
    return;
  }
  
  if (now - pTxn->startTime < 30000) return; // 事务还没执行满 30 秒，不急着报警

  // 控制日志频率：每 30 秒打印一次警告
  if (now - pTxn->lastWarnTime > 30000) {
    char waitingList[512] = {0};
    int32_t offset = 0;

    int32_t numVgs = taosArrayGetSize(pTxn->pVgList);
    for (int32_t i = 0; i < numVgs; ++i) {
      // 检查位图，看这个 VGroup 是否已经全员 ACK
      if (!bmIsSet(pTxn->pAckBitmap, i)) {
        int32_t vgId = *(int32_t*)taosArrayGet(pTxn->pVgList, i);
        
        // 进一步获取该 VGroup 对应的 DNode 信息（方便运维定位）
        // 假设通过现有元数据接口找到该 VGroup 的 Leader 或所有成员所在的 DNode
        char dnodeIds[64] = {0};
        mndGetDnodesByVgId(vgId, dnodeIds, sizeof(dnodeIds)); 

        offset += snprintf(waitingList + offset, sizeof(waitingList) - offset, 
                           "vgId:%d(on dnodes:%s) ", vgId, dnodeIds);
        
        if (offset >= sizeof(waitingList) - 32) break; // 缓冲区快满了
      }
    }

    uWarn("txn:0x%llx has been %s for %llds, still waiting for: [%s]",
          pTxn->txnId, 
          (pTxn->status == UTXN_STATUS_COMMITTING ? "committing" : "rollbacking"),
          (now - pTxn->startTime) / 1000,
          waitingList);

    pTxn->lastWarnTime = now;
  }
}

void mndOnVnodeAck(SUserTxn *pTxn, int32_t dnodeId, int32_t vgId, int32_t code) {
  if (code == TSDB_CODE_SUCCESS) {
    // 成功逻辑：置位，检查全员 ACK
    mndMarkBitAndCheckNextStep(pTxn, dnodeId, vgId);
  } else {
    // 失败逻辑
    if (pTxn->status == UTXN_STATUS_PREPARING) {
      uError("txn:0x%lx prepare failed on dnode:%d vgId:%d, code:%d. Rolling back...", 
             pTxn->txnId, dnodeId, vgId, code);
      
      // 1. 切换到回滚态
      pTxn->status = UTXN_STATUS_ROLLBACKING;
      // 2. 重置位图，准备收集回滚的 ACK
      bmClearAll(pTxn->pAckBitmap);
      // 3. (可选) 记录一条日志，标记事务已失败
    } else if (pTxn->status == UTXN_STATUS_COMMITTING) {
      // 决策已定，不准回滚，只能硬抗
      uFatal("txn:0x%lx commit failed on dnode:%d vgId:%d, code:%d. System Inconsistent!", 
             pTxn->txnId, dnodeId, vgId, code);
      // 继续重试，不改变状态
    }
  }
}

void mndOnVnodeAck(SUserTxn *pTxn, int32_t vgId, int32_t dnodeId) {
  // 1. 在 SArray 中定位下标 (假设下标为 idx)
  int32_t idx = mndFindParticipantIdx(pTxn, vgId, dnodeId);
  if (idx < 0) return; 

  // 2. 幂等检查：只有位图里是 0，才处理
  if (bmIsSet(pTxn->pBitMap, idx) == 0) {
    bmSet(pTxn->pBitMap, idx);  // 标记为已收到
    pTxn->nAck++;               // 只有第一次收到时，计数器才加 1
    
    // 3. 检查是否全员到齐
    if (pTxn->nAck == taosArrayGetSize(pTxn->pParticipants)) {
      mndTransitionStatus(pTxn); // 状态跃迁
    }
  }
}

void mndOnDnodeDropped(int32_t droppedDnodeId) {
  // 遍历哈希表中的所有活跃事务
  SUserTxn* pTxn = NULL;
  while ((pTxn = mndGetNextActiveTxn(&iter)) != NULL) {
    TdThreadLock(&pTxn->lock);
    
    bool changed = false;
    for (int i = 0; i < taosArrayGetSize(pTxn->pParticipants); ++i) {
      STxnParticipant* p = taosArrayGet(pTxn->pParticipants, i);
      if (p->dnodeId == droppedDnodeId && !bmIsSet(pTxn->pAckBitmap, i)) {
        bmSet(pTxn->pAckBitmap, i); // 强制置位
        changed = true;
      }
    }
    
    if (changed && bmIsAllSet(pTxn->pAckBitmap)) {
       // 触发状态跃迁逻辑
       mndTransitionTxnStatus(pTxn); 
    }
    
    TdThreadUnlock(&pTxn->lock);
  }
}

void mndHandleDnodeDropInTxn(SUserTxn *pTxn, int32_t droppedDnodeId) {
  int32_t size = taosArrayGetSize(pTxn->pParticipants);
  for (int32_t i = 0; i < size; ++i) {
    STxnParticipant *p = taosArrayGet(pTxn->pParticipants, i);
    
    // 如果这个参与者属于被删掉的节点，且还没回 ACK
    if (p->dnodeId == droppedDnodeId && bmIsSet(pTxn->pBitMap, i) == 0) {
      bmSet(pTxn->pBitMap, i);
      pTxn->nAck++;
      uInfo("txn:0x%lx, force ack for dropped dnode:%d", pTxn->txnId, droppedDnodeId);
    }
  }
  
  // 补偿完后，再次检查是否可以闭环
  if (pTxn->nAck == size) {
    mndTransitionStatus(pTxn);
  }
}


#endif