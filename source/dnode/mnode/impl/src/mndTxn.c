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
#include "audit.h"
#include "mndDb.h"
#include "mndDnode.h"
#include "mndInt.h"
#include "mndMnode.h"
#include "mndPrivilege.h"
#include "mndShow.h"
#include "mndStb.h"
#include "mndSync.h"
#include "mndTrans.h"
#include "mndTxnSeq.h"
#include "mndUser.h"
#include "mndVgroup.h"
#include "parser.h"
#include "tname.h"

#define MND_TXN_VER_NUMBER   1
#define MND_TXN_RESERVE_SIZE 64
#define MND_TXN_MAX_ACTIVE   200

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

// Forward declarations
static void    mndTxnTimeoutScanImpl(SMnode *pMnode);
static int32_t mndRollbackTxn(SMnode *pMnode, SRpcMsg *pReq, STxnObj *pTxn, int32_t reason);
static int32_t mndCommitTxn(SMnode *pMnode, SRpcMsg *pReq, STxnObj *pTxn);
static int32_t mndTxnAfterRestored(SMnode *pMnode);
static void    mndTxnRebuildShadowOpsFromSdb(SMnode *pMnode, STxnObj *pTxn);

static void mndUserTxnFreeFp(void *ptr) {
  SUserTxn *pTxn = (SUserTxn *)ptr;
  if (pTxn->pVgList) {
    taosArrayDestroy(pTxn->pVgList);
    pTxn->pVgList = NULL;
  }
  if (pTxn->pVgAckBitmap) {
    taosMemoryFree(pTxn->pVgAckBitmap);
    pTxn->pVgAckBitmap = NULL;
  }
  taosThreadRwlockDestroy(&pTxn->lock);
}

int32_t mndInitTxn(SMnode *pMnode) {
  SSdbTable table = {
      .sdbType = SDB_TXN,
      .keyType = SDB_KEY_INT64,
      .encodeFp = (SdbEncodeFp)mndTxnActionEncode,
      .decodeFp = (SdbDecodeFp)mndTxnActionDecode,
      .insertFp = (SdbInsertFp)mndTxnActionInsert,
      .updateFp = (SdbUpdateFp)mndTxnActionUpdate,
      .deleteFp = (SdbDeleteFp)mndTxnActionDelete,
      .afterRestoredFp = (SdbAfterRestoredFp)mndTxnAfterRestored,
  };

  // 初始化 STxnMgmt 运行时管理结构
  STxnMgmt *pMgmt = &pMnode->txnMgmt;
  pMgmt->pTxnHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  if (pMgmt->pTxnHash == NULL) {
    mError("txn, failed to init txn hash");
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  taosHashSetFreeFp(pMgmt->pTxnHash, mndUserTxnFreeFp);
  taosThreadRwlockInit(&pMgmt->lock, NULL);
  pMgmt->currentTxnId = -1;
  pMgmt->hTimeoutTimer = NULL;  // 定时器在 mnode 启动完成后再启动

  mndSetMsgHandle(pMnode, TDMT_MND_BEGIN_TXN, mndProcessBeginTxnReq);
  mndSetMsgHandle(pMnode, TDMT_MND_BEGIN_TXN_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_COMMIT_TXN, mndProcessCommitTxnReq);
  mndSetMsgHandle(pMnode, TDMT_MND_COMMIT_TXN_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_ROLLBACK_TXN, mndProcessRollbackTxnReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ROLLBACK_TXN_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_TXN_COMMIT_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_TXN_ROLLBACK_RSP, mndTransProcessRsp);

  //   mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TXN, mndRetrieveTxn);
  //   mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TXN, mndCancelRetrieveTxn);

  return sdbSetTable(pMnode->pSdb, table);
}

// 超时扫描实现（由外部定期调用）
static void mndTxnTimeoutScanImpl(SMnode *pMnode) {
  SSdb   *pSdb = pMnode->pSdb;
  int64_t now = taosGetTimestampMs();
  void   *pIter = NULL;

  while (1) {
    STxnObj *pTxn = NULL;
    pIter = sdbFetch(pSdb, SDB_TXN, pIter, (void **)&pTxn);
    if (pIter == NULL) break;

    int64_t elapsed = now - pTxn->lastActiveTime;
    int64_t timeout = (int64_t)pTxn->timeoutSec * 1000;

    // §35 taosX: replicated txns have no timeout (resolved by explicit COMMIT/ROLLBACK from WAL)
    if (TXN_IS_REPLICATED(pTxn->id)) {
      sdbRelease(pSdb, pTxn);
      continue;
    }

    // Clock regression protection: skip if clock moved backward (NTP correction)
    if (elapsed < 0) {
      mDebug("txn:%" PRIu64 ", clock regression detected, elapsed=%" PRId64 "ms, skip timeout check", pTxn->id,
             elapsed);
      sdbRelease(pSdb, pTxn);
      continue;
    }

    // §43 Absolute lifetime limit: rollback if total lifetime exceeds max regardless of activity
    int64_t lifetime = now - pTxn->createTime;
    if (pTxn->stage == UTXN_STAGE_ACTIVE && lifetime > (int64_t)TSDB_META_TXN_MAX_LIFETIME_SEC * 1000) {
      mWarn("txn:%" PRIu64 ", stage=%s, lifetime=%" PRId64 "ms > max=%" PRId64
            "ms, triggering ROLLBACK due to exceeded lifetime",
            pTxn->id, mndUtxnStageStr(pTxn->stage), lifetime, (int64_t)TSDB_META_TXN_MAX_LIFETIME_SEC * 1000);

      SRpcMsg synReq = {0};
      synReq.info.node = pMnode;

      int32_t code = mndRollbackTxn(pMnode, &synReq, pTxn, TSDB_CODE_TXN_EXCEEDED_LIFETIME);
      if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
        mError("txn:%" PRIu64 ", lifetime rollback failed: %s", pTxn->id, tstrerror(code));
      } else {
        mInfo("txn:%" PRIu64 ", lifetime rollback initiated", pTxn->id);
      }
      sdbRelease(pSdb, pTxn);
      continue;
    }

    if (pTxn->stage == UTXN_STAGE_ACTIVE && elapsed > timeout) {
      mWarn("txn:%" PRIu64 ", stage=%s, elapsed=%" PRId64 "ms > timeout=%" PRId64 "ms, triggering ROLLBACK", pTxn->id,
            mndUtxnStageStr(pTxn->stage), elapsed, timeout);

      // Build a synthetic SRpcMsg for the rollback Trans (no real client connection)
      SRpcMsg synReq = {0};
      synReq.info.node = pMnode;

      int32_t code = mndRollbackTxn(pMnode, &synReq, pTxn, TSDB_CODE_VND_TXN_EXPIRED);
      if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
        mError("txn:%" PRIu64 ", timeout rollback failed: %s", pTxn->id, tstrerror(code));
      } else {
        mInfo("txn:%" PRIu64 ", timeout rollback initiated", pTxn->id);
      }
    }
    sdbRelease(pSdb, pTxn);
  }
}

// 手动触发超时扫描（供 mndMain.c 定期调用）
void mndTxnDoTimeoutScan(SMnode *pMnode) {
  mndTxnTimeoutScanImpl(pMnode);
}

/**
 * Leader switchover recovery: scan all STxnObj in SDB after Raft restore,
 * and continue pushing in-flight transactions based on their stage.
 *
 * Per skill.md §6.3:
 *   ACTIVE       → refresh lastActiveTime, await further ops or timeout
 *   COMMITTING   → re-create Trans to broadcast COMMIT to VNodes
 *   ROLLINGBACK  → re-create Trans to broadcast ROLLBACK to VNodes
 *   COMPLETED/ZOMBIE → delete from SDB immediately
 */
static int32_t mndTxnAfterRestored(SMnode *pMnode) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  int32_t numRecovered = 0;

  mInfo("txn, scanning SDB for in-flight transactions after leader restore");

  while (1) {
    STxnObj *pTxn = NULL;
    pIter = sdbFetch(pSdb, SDB_TXN, pIter, (void **)&pTxn);
    if (pIter == NULL) break;

    SRpcMsg synReq = {0};
    synReq.info.node = pMnode;

    switch (pTxn->stage) {
      case UTXN_STAGE_ACTIVE: {
        // Refresh lastActiveTime; client can reconnect and resume this txn.
        // Rebuild shadow ops from SStbObj markers (txnId, txnStatus, pTxnAlterReqs).
        mInfo("txn:%" PRIu64 ", restored in ACTIVE stage, rebuilding shadow ops and resetting lastActiveTime",
              pTxn->id);
        mndTxnRebuildShadowOpsFromSdb(pMnode, pTxn);
        taosWLockLatch(&pTxn->lock);
        pTxn->lastActiveTime = taosGetTimestampMs();
        taosWUnLockLatch(&pTxn->lock);
        numRecovered++;
        break;
      }
      case UTXN_STAGE_COMMITTING: {
        // The original "commit-txn" STrans is already persisted in SDB (it was the one that
        // set stage=COMMITTING via its prepareLog). mndTransPullup will automatically retry
        // that STrans — do NOT create a duplicate via mndCommitTxn.
        mInfo("txn:%" PRIu64 ", restored in COMMITTING stage, original STrans will be retried by mndTransPullup",
              pTxn->id);
        numRecovered++;
        break;
      }
      case UTXN_STAGE_ROLLINGBACK: {
        // Same: the original "rollback-txn" STrans is in SDB. mndTransPullup retries it.
        mInfo("txn:%" PRIu64 ", restored in ROLLINGBACK stage, original STrans will be retried by mndTransPullup",
              pTxn->id);
        numRecovered++;
        break;
      }
      case UTXN_STAGE_COMPLETED:
      case UTXN_STAGE_ZOMBIE: {
        mInfo("txn:%" PRIu64 ", restored in %s stage, will be cleaned by timeout scan", pTxn->id,
              mndUtxnStageStr(pTxn->stage));
        numRecovered++;
        break;
      }
      default:
        break;
    }
    sdbRelease(pSdb, pTxn);
  }

  mInfo("txn, leader restore scan complete, recovered %d transactions", numRecovered);
  return 0;
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
    taosHashCleanup(pMgmt->pTxnHash);
    pMgmt->pTxnHash = NULL;
  }
}

// MNode 侧用户事务阶段名称，用于日志输出
const char *mndUtxnStageStr(EUtxnStage stage) {
  switch (stage) {
    case UTXN_STAGE_IDLE:        return "IDLE";
    case UTXN_STAGE_ACTIVE:      return "ACTIVE";
    case UTXN_STAGE_ABORTED:
      return "ABORTED";
    case UTXN_STAGE_PREPARING:
      return "PREPARING";
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
    if (pObj->pShadowOps) {
      int32_t sz = taosArrayGetSize(pObj->pShadowOps);
      for (int32_t i = 0; i < sz; i++) {
        SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pObj->pShadowOps, i);
        taosMemoryFreeClear(pOp->pReqData);
      }
      taosArrayDestroy(pObj->pShadowOps);
      pObj->pShadowOps = NULL;
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

  // NOTE: pShadowOps is NOT serialized — it is a runtime-only field.
  // On restart, shadow ops are reconstructed from SDB (SStbObj.txnId) by mndTxnRebuildShadowOpsFromSdb().

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

  // NOTE: pShadowOps is NOT deserialized — it is a runtime-only field.
  // Backward compat: skip over any shadow ops data that may have been written by an older build.
  // On restart, shadow ops are reconstructed from SDB by mndTxnRebuildShadowOpsFromSdb().
  pObj->pShadowOps = NULL;

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

// Check if a specific UTXN is alive (exists and in active/preparing/committing stage)
// Returns 1 if alive, 0 if dead/unknown/completed
int8_t mndTxnIsAlive(SMnode *pMnode, utxn_id_t txnId) {
  STxnObj *pTxn = mndAcquireTxn(pMnode, txnId);
  if (pTxn == NULL) return 0;

  int8_t alive = 0;
  switch (pTxn->stage) {
    case UTXN_STAGE_ACTIVE:
    case UTXN_STAGE_PREPARING:
    case UTXN_STAGE_COMMITTING:
      alive = 1;
      break;
    default:
      alive = 0;
      break;
  }
  mndReleaseTxn(pMnode, pTxn);
  return alive;
}

void mndTxnRefreshKeepalive(SMnode *pMnode, utxn_id_t txnId) {
  STxnObj *pTxn = mndAcquireTxn(pMnode, txnId);
  if (pTxn == NULL) return;

  if (pTxn->stage == UTXN_STAGE_ACTIVE) {
    pTxn->lastActiveTime = taosGetTimestampMs();
    mTrace("txn:%" PRIu64 ", keepalive refreshed via client HB", txnId);
  }
  mndReleaseTxn(pMnode, pTxn);
}

/**
 * Rollback an orphan transaction on a specific VNode via Raft-safe STrans.
 *
 * Called when VNode reports an idle txn (via statusReq) that no longer exists
 * in MNode SDB. Instead of ACKing alive=0 for VNode to do local rollback
 * (which bypasses Raft replication), MNode creates an STrans that sends
 * TDMT_VND_TXN_ROLLBACK through the normal Raft-replicated write path.
 */
int32_t mndRollbackOrphanTxnOnVnode(SMnode *pMnode, utxn_id_t txnId, int32_t vgId) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;

  SRpcMsg synReq = {0};
  synReq.info.node = pMnode;

  TSDB_CHECK_NULL(
      (pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, &synReq, "orphan-txn-cleanup")), code,
      lino, _exit, terrno);
  mInfo("trans:%d, used to cleanup orphan txn %" PRIu64 " on vgId:%d", pTrans->id, txnId, vgId);

  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);

  // Build ROLLBACK request for the specific VNode
  SVTxnRollbackReq req = {0};
  req.txnId = txnId;
  req.term = mndGetTerm(pMnode);
  req.reason = TSDB_CODE_VND_TXN_EXPIRED;

  int32_t bodyLen = tSerializeSVTxnRollbackReq(NULL, 0, &req);
  if (bodyLen <= 0) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  int32_t   contLen = bodyLen + sizeof(SMsgHead);
  SMsgHead *pHead = taosMemoryMalloc(contLen);
  if (pHead == NULL) {
    code = terrno;
    goto _exit;
  }
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(vgId);
  tSerializeSVTxnRollbackReq(POINTER_SHIFT(pHead, sizeof(SMsgHead)), bodyLen, &req);

  STransAction action = {0};
  action.mTraceId = pTrans->mTraceId;
  action.epSet = mndGetVgroupEpsetById(pMnode, vgId);
  action.pCont = pHead;
  action.contLen = contLen;
  action.msgType = TDMT_VND_TXN_ROLLBACK;
  action.acceptableCode = TSDB_CODE_SUCCESS;
  action.groupId = vgId;

  TAOS_CHECK_EXIT(mndTransAppendRedoAction(pTrans, &action));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));

_exit:
  mndTransDrop(pTrans);
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txn:%" PRIu64 ", failed to rollback orphan on vgId:%d, code:0x%x", txnId, vgId, code);
  }
  TAOS_RETURN(code);
}

// ============================================================================
// MNode Shadow Operation Management (STB DDL undo-log)
// ============================================================================

/**
 * Record an STB shadow operation within the active user txn.
 * Called by mndStb.c after executing CREATE/DROP/ALTER STABLE within a batch txn.
 *
 * @param pMnode   The mnode
 * @param txnId    User batch txn ID
 * @param opType   EMndShadowOpType
 * @param stbName  Fully qualified STB name
 * @param uid      STB UID
 * @param dbName   DB name
 */
int32_t mndTxnAddShadowOp(SMnode *pMnode, utxn_id_t txnId, int8_t opType, const char *stbName, tb_uid_t uid,
                          const char *dbName, void *pReqData, int32_t reqDataLen) {
  STxnObj *pTxn = mndAcquireTxn(pMnode, txnId);
  if (pTxn == NULL) {
    mError("txn:%" PRIu64 ", not found, cannot add shadow op", txnId);taosMemoryFreeClear(pReqData);
    return TSDB_CODE_TXN_NOT_EXIST;
  }

  taosWLockLatch(&pTxn->lock);

  if (pTxn->pShadowOps == NULL) {
    pTxn->pShadowOps = taosArrayInit(4, sizeof(SMndShadowOp));
    if (pTxn->pShadowOps == NULL) {
      taosWUnLockLatch(&pTxn->lock);
      mndReleaseTxn(pMnode, pTxn);
      taosMemoryFreeClear(pReqData);
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }

  SMndShadowOp op = {0};
  op.opType = opType;
  op.uid = uid;
  tstrncpy(op.name, stbName, sizeof(op.name));
  tstrncpy(op.db, dbName, sizeof(op.db));
  op.pReqData = pReqData;  // ownership transferred
  op.reqDataLen = reqDataLen;

  if (taosArrayPush(pTxn->pShadowOps, &op) == NULL) {
    taosWUnLockLatch(&pTxn->lock);
    mndReleaseTxn(pMnode, pTxn);
    taosMemoryFreeClear(pReqData);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pTxn->lastActiveTime = taosGetTimestampMs();

  taosWUnLockLatch(&pTxn->lock);
  mndReleaseTxn(pMnode, pTxn);

  mInfo("txn:%" PRIu64 ", shadow op added (redo): opType=%d, stb=%s, uid=%" PRId64 ", dataLen:%d", txnId, opType,
        stbName, uid, reqDataLen);
  return TSDB_CODE_SUCCESS;
}

/**
 * Get ALTER STB shadow ops for a specific STB in a given txn.
 * Returns an SArray of SMndShadowOp* (pointers into the txn's pShadowOps).
 * Caller must destroy the SArray but NOT free the SMndShadowOp contents.
 * Returns NULL ppOps if no ALTER ops found (not an error).
 */
int32_t mndTxnGetAlterOpsForStb(SMnode *pMnode, utxn_id_t txnId, const char *stbFName, SArray **ppOps) {
  *ppOps = NULL;
  if (txnId == 0 || stbFName == NULL) return TSDB_CODE_SUCCESS;

  STxnObj *pTxn = mndAcquireTxn(pMnode, txnId);
  if (pTxn == NULL) return TSDB_CODE_SUCCESS;  // txn not found, not an error for this use

  taosRLockLatch(&pTxn->lock);

  if (pTxn->pShadowOps == NULL || taosArrayGetSize(pTxn->pShadowOps) == 0) {
    taosRUnLockLatch(&pTxn->lock);
    mndReleaseTxn(pMnode, pTxn);
    return TSDB_CODE_SUCCESS;
  }

  int32_t numOps = taosArrayGetSize(pTxn->pShadowOps);
  SArray *pResult = NULL;

  for (int32_t i = 0; i < numOps; i++) {
    SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);
    if (pOp->opType == MND_SHADOW_OP_ALTER_STB && strcmp(pOp->name, stbFName) == 0) {
      if (pResult == NULL) {
        pResult = taosArrayInit(4, sizeof(SMndShadowOp));
        if (pResult == NULL) {
          taosRUnLockLatch(&pTxn->lock);
          mndReleaseTxn(pMnode, pTxn);
          return terrno;
        }
      }
      // Copy the op struct (shallow copy - pReqData is NOT owned by the copy)
      taosArrayPush(pResult, pOp);
    }
  }

  taosRUnLockLatch(&pTxn->lock);
  mndReleaseTxn(pMnode, pTxn);

  *ppOps = pResult;
  return TSDB_CODE_SUCCESS;
}

/**
 * Get the active user txn ID for the requesting connection.
 * Currently checks if the RPC message carries a txnId in its extended info.
 * Returns txnId > 0 if within a batch txn, 0 otherwise.
 *
 * Scans all active STxnObj in SDB, finds the one whose createUser matches and is ACTIVE.
 * For now, since only one global user txn is supported, a simple scan is efficient enough.
 */
utxn_id_t mndTxnGetActiveTxnId(SMnode *pMnode, SRpcMsg *pReq) {
  if (pMnode == NULL || pReq == NULL) return 0;

  const char *user = RPC_MSG_USER(pReq);
  if (user == NULL || user[0] == '\0') return 0;

  SSdb     *pSdb = pMnode->pSdb;
  void     *pIter = NULL;
  STxnObj  *pTxn = NULL;
  utxn_id_t txnId = 0;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_TXN, pIter, (void **)&pTxn);
    if (pIter == NULL) break;
    if (pTxn->stage == UTXN_STAGE_ACTIVE && strcmp(pTxn->createUser, user) == 0) {
      txnId = pTxn->id;
      sdbRelease(pSdb, pTxn);
      sdbCancelFetch(pSdb, pIter);
      break;
    }
    sdbRelease(pSdb, pTxn);
  }
  return txnId;
}

/**
 * Check if any active txn (other than callerTxnId) has a shadow op on stbName.
 * Used for MNode-level conflict detection on STB DROP/ALTER operations.
 *
 * @return TSDB_CODE_TXN_RESOURCE_BUSY if conflict, 0 otherwise.
 */
int32_t mndTxnCheckStbConflict(SMnode *pMnode, const char *stbName, utxn_id_t callerTxnId) {
  SSdb    *pSdb = pMnode->pSdb;
  void    *pIter = NULL;
  STxnObj *pTxn = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_TXN, pIter, (void **)&pTxn);
    if (pIter == NULL) break;

    if (pTxn->id == callerTxnId || pTxn->stage != UTXN_STAGE_ACTIVE) {
      sdbRelease(pSdb, pTxn);
      continue;
    }

    taosRLockLatch(&pTxn->lock);
    if (pTxn->pShadowOps != NULL) {
      int32_t numOps = taosArrayGetSize(pTxn->pShadowOps);
      for (int32_t i = 0; i < numOps; i++) {
        SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);
        if (strcmp(pOp->name, stbName) == 0) {
          taosRUnLockLatch(&pTxn->lock);
          mInfo("stb:%s, conflict with txn:%" PRIu64 " shadow op type=%d", stbName, pTxn->id, pOp->opType);
          sdbRelease(pSdb, pTxn);
          sdbCancelFetch(pSdb, pIter);
          return TSDB_CODE_TXN_RESOURCE_BUSY;
        }
      }
    }
    taosRUnLockLatch(&pTxn->lock);
    sdbRelease(pSdb, pTxn);
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * Apply MNode shadow ops on COMMIT — embed SDB changes + VNode actions into the commit Trans.
 *
 * Instead of replaying via original message handlers (which create independent STrans
 * and cause TRN_CONFLICT_DB_INSIDE conflicts), we add the SDB prepare/commit logs
 * and VNode redo actions directly to the commit STrans using helper functions.
 *
 * @param pMnode     The mnode
 * @param pTrans     The commit STrans to append actions to
 * @param pTxn       The user batch txn being committed
 * @return 0 on success, error code on failure
 */

/**
 * Rebuild CREATE_STB shadow ops from SDB for the ACTIVE→timeout→ROLLBACK recovery path.
 *
 * pShadowOps is a runtime-only field (not persisted in STxnObj SDB encoding). After MNode
 * restart, an ACTIVE txn's pShadowOps is NULL. When timeout triggers ROLLBACK, we need to
 * know which CREATE_STBs to undo (DROP). We scan SDB_STB for SStbObj with matching txnId.
 *
 * Why only CREATE_STB?
 * - CREATE_STB uses undo-log model: STB was written to SDB immediately during ACTIVE,
 *   with SStbObj.txnId set. On ROLLBACK we must DROP it. SStbObj.txnId identifies these.
 * - ALTER_STB / DROP_STB use redo-log model: never applied to SDB during ACTIVE.
 *   On ROLLBACK, nothing to undo. On COMMIT, the STrans (persisted independently) has
 *   all the request data and will be retried by mndTransPullup.
 *
 * Why not persist pShadowOps?
 * - BEGIN persists STxnObj when pShadowOps is empty (shadow ops added later in memory).
 * - The only moment we could persist is COMMIT/ROLLBACK, but by then we're creating an
 *   STrans that already encodes all actions. Persisting in STxnObj would be redundant.
 * - SStbObj.txnId provides exactly the information needed for the only recovery path
 *   that requires reconstruction (ACTIVE→ROLLBACK).
 */
static void mndTxnRebuildShadowOpsFromSdb(SMnode *pMnode, STxnObj *pTxn) {
  if (pTxn->pShadowOps != NULL) return;  // already populated in memory

  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  SStbObj *pStb = NULL;
  int32_t  count = 0;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
    if (pIter == NULL) break;

    if (pStb->txnId != 0 && (utxn_id_t)pStb->txnId == pTxn->id) {
      if (pTxn->pShadowOps == NULL) {
        pTxn->pShadowOps = taosArrayInit(4, sizeof(SMndShadowOp));
        if (pTxn->pShadowOps == NULL) {
          sdbRelease(pSdb, pStb);
          sdbCancelFetch(pSdb, pIter);
          mError("txn:%" PRIu64 ", failed to alloc pShadowOps during rebuild", pTxn->id);
          return;
        }
      }

      // CREATE_STB: txnStatus has CREATED bit set
      if (pStb->txnStatus & MND_STB_TXN_CREATED) {
        SMndShadowOp op = {0};
        op.opType = MND_SHADOW_OP_CREATE_STB;
        op.uid = pStb->uid;
        tstrncpy(op.name, pStb->name, sizeof(op.name));
        tstrncpy(op.db, pStb->db, sizeof(op.db));
        op.pReqData = NULL;
        op.reqDataLen = 0;

        if (taosArrayPush(pTxn->pShadowOps, &op) == NULL) {
          sdbRelease(pSdb, pStb);
          sdbCancelFetch(pSdb, pIter);
          mError("txn:%" PRIu64 ", failed to push shadow op during rebuild", pTxn->id);
          return;
        }
        count++;
        mInfo("txn:%" PRIu64 ", rebuilt CREATE_STB shadow op: stb=%s uid=%" PRId64, pTxn->id, pStb->name, pStb->uid);
      }

      // DROP_STB: txnStatus has PRE_DROP bit set
      if (pStb->txnStatus & MND_STB_TXN_PRE_DROP) {
        SMndShadowOp op = {0};
        op.opType = MND_SHADOW_OP_DROP_STB;
        op.uid = pStb->uid;
        tstrncpy(op.name, pStb->name, sizeof(op.name));
        tstrncpy(op.db, pStb->db, sizeof(op.db));
        op.pReqData = NULL;
        op.reqDataLen = 0;

        if (taosArrayPush(pTxn->pShadowOps, &op) == NULL) {
          sdbRelease(pSdb, pStb);
          sdbCancelFetch(pSdb, pIter);
          mError("txn:%" PRIu64 ", failed to push DROP shadow op during rebuild", pTxn->id);
          return;
        }
        count++;
        mInfo("txn:%" PRIu64 ", rebuilt DROP_STB shadow op: stb=%s uid=%" PRId64, pTxn->id, pStb->name, pStb->uid);
      }

      // ALTER_STB: txnAlterReqsLen > 0 (chained ALTER request data)
      if (pStb->txnAlterReqsLen > (int32_t)sizeof(int32_t) && pStb->pTxnAlterReqs != NULL) {
        int32_t numEntries = 0;
        memcpy(&numEntries, pStb->pTxnAlterReqs, sizeof(int32_t));
        int32_t offset = sizeof(int32_t);

        for (int32_t j = 0; j < numEntries && offset < pStb->txnAlterReqsLen; j++) {
          int32_t entryLen = 0;
          if (offset + (int32_t)sizeof(int32_t) > pStb->txnAlterReqsLen) break;
          memcpy(&entryLen, (char *)pStb->pTxnAlterReqs + offset, sizeof(int32_t));
          offset += sizeof(int32_t);
          if (entryLen <= 0 || offset + entryLen > pStb->txnAlterReqsLen) break;

          void *pData = taosMemoryMalloc(entryLen);
          if (pData == NULL) {
            sdbRelease(pSdb, pStb);
            sdbCancelFetch(pSdb, pIter);
            mError("txn:%" PRIu64 ", failed to alloc ALTER data during rebuild", pTxn->id);
            return;
          }
          memcpy(pData, (char *)pStb->pTxnAlterReqs + offset, entryLen);
          offset += entryLen;

          SMndShadowOp op = {0};
          op.opType = MND_SHADOW_OP_ALTER_STB;
          op.uid = pStb->uid;
          tstrncpy(op.name, pStb->name, sizeof(op.name));
          tstrncpy(op.db, pStb->db, sizeof(op.db));
          op.pReqData = pData;
          op.reqDataLen = entryLen;

          if (taosArrayPush(pTxn->pShadowOps, &op) == NULL) {
            taosMemoryFree(pData);
            sdbRelease(pSdb, pStb);
            sdbCancelFetch(pSdb, pIter);
            mError("txn:%" PRIu64 ", failed to push ALTER shadow op during rebuild", pTxn->id);
            return;
          }
          count++;
          mInfo("txn:%" PRIu64 ", rebuilt ALTER_STB shadow op %d/%d: stb=%s dataLen=%d",
                pTxn->id, j + 1, numEntries, pStb->name, entryLen);
        }
      }
    }
    sdbRelease(pSdb, pStb);
  }

  if (count > 0) {
    mInfo("txn:%" PRIu64 ", rebuilt %d shadow ops from SDB (CREATE+DROP+ALTER)", pTxn->id, count);
  }
}

static int32_t mndTxnApplyShadowOps(SMnode *pMnode, STrans *pTrans, STxnObj *pTxn) {
  if (pTxn->pShadowOps == NULL || taosArrayGetSize(pTxn->pShadowOps) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t numOps = taosArrayGetSize(pTxn->pShadowOps);
  mInfo("txn:%" PRIu64 ", applying %d MNode STB shadow ops into commit trans:%d", pTxn->id, numOps, pTrans->id);

  for (int32_t i = 0; i < numOps; i++) {
    SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);
    int32_t       code = 0;

    mInfo("txn:%" PRIu64 ", applying shadow op %d/%d: opType=%d, stb=%s", pTxn->id, i + 1, numOps, pOp->opType,
          pOp->name);

    switch (pOp->opType) {
      case MND_SHADOW_OP_CREATE_STB: {
        // Undo-log model: STB was created immediately during txn, nothing to do at COMMIT.
        mInfo("txn:%" PRIu64 ", CREATE_STB shadow op %d/%d: no-op (already created), stb=%s", pTxn->id, i + 1, numOps,
              pOp->name);
        break;
      }
      case MND_SHADOW_OP_DROP_STB: {
        code = mndAppendDropStbToTrans(pMnode, pTrans, pOp->name);
        break;
      }
      case MND_SHADOW_OP_ALTER_STB: {
        code = mndAppendAlterStbToTrans(pMnode, pTrans, pOp->pReqData, pOp->reqDataLen);
        break;
      }
      default:
        mError("txn:%" PRIu64 ", unknown shadow op type %d", pTxn->id, pOp->opType);
        return TSDB_CODE_MND_TXN_ERROR;
    }

    if (code != 0) {
      mError("txn:%" PRIu64 ", shadow op %d/%d failed: %s", pTxn->id, i + 1, numOps, tstrerror(code));
      return code;
    }
    mInfo("txn:%" PRIu64 ", shadow op %d/%d applied", pTxn->id, i + 1, numOps);

    taosMemoryFreeClear(pOp->pReqData);
  }

  mInfo("txn:%" PRIu64 ", all %d MNode STB shadow ops applied into commit trans", pTxn->id, numOps);
  return TSDB_CODE_SUCCESS;
}

/**
 * Undo MNode shadow ops on ROLLBACK by appending actions to the rollback Trans.
 *
 * CREATE_STB uses undo-log model: STB was created immediately during txn,
 * so at ROLLBACK we append DROP STB commit logs + redo actions to pTrans.
 *
 * DROP_STB / ALTER_STB use redo-log model: not applied during txn,
 * so nothing to undo — just free and discard.
 */
static int32_t mndTxnUndoShadowOps(SMnode *pMnode, STrans *pTrans, STxnObj *pTxn) {
  if (pTxn->pShadowOps == NULL) return TSDB_CODE_SUCCESS;

  int32_t numOps = taosArrayGetSize(pTxn->pShadowOps);
  mInfo("txn:%" PRIu64 ", undoing %d MNode shadow ops (ROLLBACK)", pTxn->id, numOps);

  for (int32_t i = 0; i < numOps; i++) {
    SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);

    switch (pOp->opType) {
      case MND_SHADOW_OP_CREATE_STB: {
        // Undo-log: STB was created immediately, append DROP to rollback Trans
        mInfo("txn:%" PRIu64 ", undo CREATE_STB shadow op %d/%d: stb=%s uid=%" PRId64, pTxn->id, i + 1, numOps,
              pOp->name, pOp->uid);
        int32_t code = mndAppendDropStbToTrans(pMnode, pTrans, pOp->name);
        if (code != 0) {
          mError("txn:%" PRIu64 ", failed to append DROP STB for stb=%s: %s", pTxn->id, pOp->name, tstrerror(code));
          return code;
        }
        break;
      }
      case MND_SHADOW_OP_DROP_STB:
      case MND_SHADOW_OP_ALTER_STB: {
        // Clear txn markers (txnId, txnStatus, pTxnAlterReqs) from SStbObj via commit-log
        mInfo("txn:%" PRIu64 ", undo %s shadow op %d/%d: clearing markers on stb=%s",
              pTxn->id, pOp->opType == MND_SHADOW_OP_DROP_STB ? "DROP_STB" : "ALTER_STB",
              i + 1, numOps, pOp->name);
        SStbObj *pStb = mndAcquireStb(pMnode, pOp->name);
        if (pStb != NULL) {
          SStbObj stbClone;
          taosRLockLatch(&pStb->lock);
          memcpy(&stbClone, pStb, sizeof(SStbObj));
          taosRUnLockLatch(&pStb->lock);
          stbClone.lock = 0;
          stbClone.txnId = 0;
          stbClone.txnStatus = MND_STB_TXN_NORMAL;
          stbClone.pTxnAlterReqs = NULL;
          stbClone.txnAlterReqsLen = 0;
          SSdbRaw *pRaw = mndStbActionEncode(&stbClone);
          if (pRaw != NULL) {
            sdbSetRawStatus(pRaw, SDB_STATUS_READY);
            int32_t code = mndTransAppendCommitlog(pTrans, pRaw);
            if (code != 0) {
              mError("txn:%" PRIu64 ", failed to append marker cleanup for stb=%s: %s",
                     pTxn->id, pOp->name, tstrerror(code));
              mndReleaseStb(pMnode, pStb);
              return code;
            }
            mInfo("txn:%" PRIu64 ", append marker cleanup commit-log for stb=%s", pTxn->id, pOp->name);
          }
          mndReleaseStb(pMnode, pStb);
        }
        break;
      }
      default: {
        mInfo("txn:%" PRIu64 ", discard shadow op %d/%d: opType=%d, stb=%s", pTxn->id, i + 1, numOps, pOp->opType,
              pOp->name);
        break;
      }
    }
    taosMemoryFreeClear(pOp->pReqData);
  }

  return TSDB_CODE_SUCCESS;
}

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
  int32_t  code = 0;
  SSdbRaw *pPrepareRaw = mndTxnActionEncode(pTxn);
  if (pPrepareRaw == NULL) {
    code = TSDB_CODE_MND_RETURN_VALUE_NULL;
    if (terrno != 0) code = terrno;
    TAOS_RETURN(code);
  }

  TAOS_CHECK_RETURN(mndTransAppendPrepareLog(pTrans, pPrepareRaw));
  TAOS_CHECK_RETURN(sdbSetRawStatus(pPrepareRaw, SDB_STATUS_CREATING));
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

// ============================================================================
// MNode → VNode: Broadcast COMMIT/ROLLBACK to all participant VGroups
// ============================================================================

/**
 * Collect all VGroup IDs that need TXN_COMMIT/TXN_ROLLBACK messages.
 * Sources: (1) pTxn->pVgList (child/normal table VGroups), (2) DB VGroups from CREATE_STB shadow ops.
 * Fallback: if both sources are empty (e.g. after full cluster restart where pShadowOps are lost),
 * broadcast to ALL VGroups — VNodes with no matching txn entry will return success (idempotent).
 * Returns a deduplicated SHashObj (key=vgId, value=unused byte).  Caller must destroy with taosHashCleanup.
 */
static SHashObj *mndCollectTxnVgroupIds(SMnode *pMnode, STxnObj *pTxn) {
  SHashObj *pVgSet = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pVgSet == NULL) return NULL;

  // (1) Add VGroups from pVgList (child / normal table tracking)
  int32_t numVgs = (pTxn->pVgList != NULL) ? taosArrayGetSize(pTxn->pVgList) : 0;
  for (int32_t i = 0; i < numVgs; i++) {
    int32_t vgId = *(int32_t *)taosArrayGet(pTxn->pVgList, i);
    int8_t  dummy = 1;
    if (taosHashPut(pVgSet, &vgId, sizeof(vgId), &dummy, sizeof(dummy)) != 0) {
      taosHashCleanup(pVgSet);
      return NULL;
    }
  }

  // (2) Add DB VGroups from CREATE_STB shadow ops (undo-log model: STB already distributed to all DB VGroups)
  if (pTxn->pShadowOps != NULL) {
    int32_t numOps = taosArrayGetSize(pTxn->pShadowOps);
    for (int32_t i = 0; i < numOps; i++) {
      SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);
      if (pOp->opType != MND_SHADOW_OP_CREATE_STB) continue;

      SDbObj *pDb = mndAcquireDbByStb(pMnode, pOp->name);
      if (pDb == NULL) continue;

      SSdb   *pSdb = pMnode->pSdb;
      SVgObj *pVgroup = NULL;
      void   *pIter = NULL;
      while (1) {
        pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
        if (pIter == NULL) break;
        if (!mndVgroupInDb(pVgroup, pDb->uid)) {
          sdbRelease(pSdb, pVgroup);
          continue;
        }
        int32_t vgId = pVgroup->vgId;
        int8_t  dummy = 1;
        if (taosHashPut(pVgSet, &vgId, sizeof(vgId), &dummy, sizeof(dummy)) != 0) {
          sdbRelease(pSdb, pVgroup);
          mndReleaseDb(pMnode, pDb);
          taosHashCleanup(pVgSet);
          return NULL;
        }
        sdbRelease(pSdb, pVgroup);
      }
      mndReleaseDb(pMnode, pDb);
    }
  }

  // (3) Fallback: if no VGroups identified (e.g. after full cluster restart — pVgList only populated
  //     at COMMIT time by client, and pShadowOps are in-memory only), broadcast to ALL VGroups.
  //     VNodes without this txn's shadow entries will return success (idempotent).
  if (taosHashGetSize(pVgSet) == 0) {
    mInfo("txn:%" PRIu64 ", no VGroups from pVgList/pShadowOps, broadcasting to all VGroups", pTxn->id);
    SSdb   *pSdb = pMnode->pSdb;
    SVgObj *pVgroup = NULL;
    void   *pIter = NULL;
    while (1) {
      pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
      if (pIter == NULL) break;
      int32_t vgId = pVgroup->vgId;
      int8_t  dummy = 1;
      if (taosHashPut(pVgSet, &vgId, sizeof(vgId), &dummy, sizeof(dummy)) != 0) {
        sdbRelease(pSdb, pVgroup);
        taosHashCleanup(pVgSet);
        return NULL;
      }
      sdbRelease(pSdb, pVgroup);
    }
  }

  return pVgSet;
}

/**
 * Build a serialized SVTxnCommitReq message with SMsgHead for a given VGroup
 */
static void *mndBuildVTxnCommitReq(SMnode *pMnode, int32_t vgId, STxnObj *pTxn, int32_t *pContLen) {
  SVTxnCommitReq req = {0};
  req.txnId = pTxn->id;
  req.term = mndGetTerm(pMnode);

  int32_t bodyLen = tSerializeSVTxnCommitReq(NULL, 0, &req);
  if (bodyLen <= 0) return NULL;

  int32_t   contLen = bodyLen + sizeof(SMsgHead);
  SMsgHead *pHead = taosMemoryMalloc(contLen);
  if (pHead == NULL) {
    *pContLen = 0;
    return NULL;
  }
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(vgId);
  tSerializeSVTxnCommitReq(POINTER_SHIFT(pHead, sizeof(SMsgHead)), bodyLen, &req);
  *pContLen = contLen;
  return pHead;
}

/**
 * Build a serialized SVTxnRollbackReq message with SMsgHead for a given VGroup
 */
static void *mndBuildVTxnRollbackReq(SMnode *pMnode, int32_t vgId, STxnObj *pTxn, int32_t reason, int32_t *pContLen) {
  SVTxnRollbackReq req = {0};
  req.txnId = pTxn->id;
  req.term = mndGetTerm(pMnode);
  req.reason = reason;

  int32_t bodyLen = tSerializeSVTxnRollbackReq(NULL, 0, &req);
  if (bodyLen <= 0) return NULL;

  int32_t   contLen = bodyLen + sizeof(SMsgHead);
  SMsgHead *pHead = taosMemoryMalloc(contLen);
  if (pHead == NULL) {
    *pContLen = 0;
    return NULL;
  }
  pHead->contLen = htonl(contLen);
  pHead->vgId = htonl(vgId);
  tSerializeSVTxnRollbackReq(POINTER_SHIFT(pHead, sizeof(SMsgHead)), bodyLen, &req);
  *pContLen = contLen;
  return pHead;
}

/**
 * Commit a user transaction: broadcast TDMT_VND_TXN_COMMIT to all participant VGroups.
 * Uses the existing Trans framework for reliable delivery, retry, and ACK tracking.
 *
 * Flow: ACTIVE → COMMITTING (SDB redo log) + redo actions to VNodes → delete STxnObj (commit log).
 */
static int32_t mndCommitTxn(SMnode *pMnode, SRpcMsg *pReq, STxnObj *pTxn) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;

  // Rebuild shadow ops from SDB if needed (e.g. after MNode restart with ACTIVE txn → client reconnects → COMMIT)
  mndTxnRebuildShadowOpsFromSdb(pMnode, pTxn);

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "commit-txn")), code,
                  lino, _exit, terrno);
  mInfo("trans:%d, used to commit txn %" PRIu64, pTrans->id, pTxn->id);

  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);

  // Prepare log: update STxnObj stage → COMMITTING atomically with Raft proposal
  {
    STxnObj redoObj = *pTxn;
    redoObj.stage = UTXN_STAGE_COMMITTING;
    redoObj.lastActiveTime = taosGetTimestampMs();
    SSdbRaw *pRedoRaw = mndTxnActionEncode(&redoObj);
    if (pRedoRaw == NULL) {
      TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL);
    }
    TAOS_CHECK_EXIT(mndTransAppendPrepareLog(pTrans, pRedoRaw));
    TAOS_CHECK_EXIT(sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY));
  }

  // Commit log: delete STxnObj after all redo actions succeed
  {
    SSdbRaw *pDropRaw = mndTxnActionEncode(pTxn);
    if (pDropRaw == NULL) {
      TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL);
    }
    TAOS_CHECK_EXIT(mndTransAppendCommitlog(pTrans, pDropRaw));
    TAOS_CHECK_EXIT(sdbSetRawStatus(pDropRaw, SDB_STATUS_DROPPED));
  }

  // Commit log: promote CREATE_STB shadow ops by clearing txn markers on SStbObj
  if (pTxn->pShadowOps != NULL) {
    int32_t numOps = taosArrayGetSize(pTxn->pShadowOps);
    for (int32_t i = 0; i < numOps; i++) {
      SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);
      if (pOp->opType == MND_SHADOW_OP_CREATE_STB) {
        SStbObj *pStb = mndAcquireStb(pMnode, pOp->name);
        if (pStb != NULL) {
          // Shallow clone: clear all txn markers for COMMIT promotion
          SStbObj stbClone;
          memcpy(&stbClone, pStb, sizeof(SStbObj));
          stbClone.txnId = 0;
          stbClone.txnStatus = MND_STB_TXN_NORMAL;
          stbClone.pTxnAlterReqs = NULL;
          stbClone.txnAlterReqsLen = 0;
          SSdbRaw *pRaw = mndStbActionEncode(&stbClone);
          if (pRaw != NULL) {
            TAOS_CHECK_EXIT(sdbSetRawStatus(pRaw, SDB_STATUS_READY));
            TAOS_CHECK_EXIT(mndTransAppendCommitlog(pTrans, pRaw));
            mInfo("txn:%" PRIu64 ", append STB promote commit log for stb=%s", pTxn->id, pOp->name);
          }
          mndReleaseStb(pMnode, pStb);
        }
      }
    }
  }

  // Apply ALTER_STB / DROP_STB shadow ops: embed SDB logs + VNode actions into this commit STrans
  TAOS_CHECK_EXIT(mndTxnApplyShadowOps(pMnode, pTrans, pTxn));

  // Add redo actions: send COMMIT to each participant VGroup (pVgList + CREATE_STB DB VGroups)
  {
    SHashObj *pVgSet = mndCollectTxnVgroupIds(pMnode, pTxn);
    if (pVgSet == NULL) {
      TAOS_CHECK_EXIT(terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY);
    }

    void *pIter = taosHashIterate(pVgSet, NULL);
    while (pIter != NULL) {
      int32_t vgId = *(int32_t *)taosHashGetKey(pIter, NULL);
      int32_t contLen = 0;
      void   *pCont = mndBuildVTxnCommitReq(pMnode, vgId, pTxn, &contLen);
      if (pCont == NULL) {
        taosHashCancelIterate(pVgSet, pIter);
        taosHashCleanup(pVgSet);
        TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY);
      }

      STransAction action = {0};
      action.mTraceId = pTrans->mTraceId;
      action.epSet = mndGetVgroupEpsetById(pMnode, vgId);
      action.pCont = pCont;
      action.contLen = contLen;
      action.msgType = TDMT_VND_TXN_COMMIT;
      action.acceptableCode = TSDB_CODE_SUCCESS;  // idempotent
      action.groupId = vgId;

      code = mndTransAppendRedoAction(pTrans, &action);
      if (code != 0) {
        taosHashCancelIterate(pVgSet, pIter);
        taosHashCleanup(pVgSet);
        TAOS_CHECK_EXIT(code);
      }
      mInfo("txn:%" PRIu64 ", append commit action for vgId:%d", pTxn->id, vgId);
      pIter = taosHashIterate(pVgSet, pIter);
    }
    taosHashCleanup(pVgSet);
  }

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));

_exit:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

/**
 * Rollback a user transaction: broadcast TDMT_VND_TXN_ROLLBACK to all participant VGroups.
 *
 * Flow: ACTIVE/PREPARING → ROLLINGBACK (SDB update) + redo actions to VNodes.
 */
static int32_t mndRollbackTxn(SMnode *pMnode, SRpcMsg *pReq, STxnObj *pTxn, int32_t reason) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;

  // Rebuild shadow ops from SDB if needed (e.g. after MNode restart with ACTIVE txn → timeout rollback)
  mndTxnRebuildShadowOpsFromSdb(pMnode, pTxn);

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "rollback-txn")), code,
                  lino, _exit, terrno);
  mInfo("trans:%d, used to rollback txn %" PRIu64, pTrans->id, pTxn->id);

  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);

  // Prepare log: update STxnObj stage → ROLLINGBACK atomically with Raft proposal
  {
    STxnObj redoObj = *pTxn;
    redoObj.stage = UTXN_STAGE_ROLLINGBACK;
    redoObj.lastActiveTime = taosGetTimestampMs();
    SSdbRaw *pRedoRaw = mndTxnActionEncode(&redoObj);
    if (pRedoRaw == NULL) {
      TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL);
    }
    TAOS_CHECK_EXIT(mndTransAppendPrepareLog(pTrans, pRedoRaw));
    TAOS_CHECK_EXIT(sdbSetRawStatus(pRedoRaw, SDB_STATUS_READY));
  }

  // Commit log: delete STxnObj after all redo actions succeed
  {
    SSdbRaw *pDropRaw = mndTxnActionEncode(pTxn);
    if (pDropRaw == NULL) {
      TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL);
    }
    TAOS_CHECK_EXIT(mndTransAppendCommitlog(pTrans, pDropRaw));
    TAOS_CHECK_EXIT(sdbSetRawStatus(pDropRaw, SDB_STATUS_DROPPED));
  }

  // Add redo actions: send ROLLBACK to each participant VGroup (pVgList + CREATE_STB DB VGroups)
  {
    SHashObj *pVgSet = mndCollectTxnVgroupIds(pMnode, pTxn);
    if (pVgSet == NULL) {
      TAOS_CHECK_EXIT(terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY);
    }

    void *pIter = taosHashIterate(pVgSet, NULL);
    while (pIter != NULL) {
      int32_t vgId = *(int32_t *)taosHashGetKey(pIter, NULL);
      int32_t contLen = 0;
      void   *pCont = mndBuildVTxnRollbackReq(pMnode, vgId, pTxn, reason, &contLen);
      if (pCont == NULL) {
        taosHashCancelIterate(pVgSet, pIter);
        taosHashCleanup(pVgSet);
        TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_OUT_OF_MEMORY);
      }

      STransAction action = {0};
      action.mTraceId = pTrans->mTraceId;
      action.epSet = mndGetVgroupEpsetById(pMnode, vgId);
      action.pCont = pCont;
      action.contLen = contLen;
      action.msgType = TDMT_VND_TXN_ROLLBACK;
      action.acceptableCode = TSDB_CODE_SUCCESS;  // idempotent
      action.groupId = vgId;

      code = mndTransAppendRedoAction(pTrans, &action);
      if (code != 0) {
        taosHashCancelIterate(pVgSet, pIter);
        taosHashCleanup(pVgSet);
        TAOS_CHECK_EXIT(code);
      }
      mInfo("txn:%" PRIu64 ", append rollback action for vgId:%d", pTxn->id, vgId);
      pIter = taosHashIterate(pVgSet, pIter);
    }
    taosHashCleanup(pVgSet);
  }

  // Undo MNode-side shadow ops — CREATE_STB: append DROP to this Trans; DROP/ALTER: discard
  TAOS_CHECK_EXIT(mndTxnUndoShadowOps(pMnode, pTrans, pTxn));

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));

_exit:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

static int32_t mndBeginTxn(SMnode *pMnode, SRpcMsg *pReq, SUserObj *pUser, SMTransReq *pTransReq) {
  int32_t code = 0, lino = 0;
  STxnObj obj = {0};
  STrans *pTrans = NULL;

  (void)snprintf(obj.createUser, TSDB_USER_LEN, "%s", pUser->user);
  obj.ownerId = pUser->uid;
  obj.id = pTransReq->txnId;
  obj.createTime = taosGetTimestampMs();
  obj.lastActiveTime = obj.createTime;
  obj.term = mndGetTerm(pMnode);
  obj.timeoutSec = 30;  // pReq->timeoutSec;
  obj.stage = UTXN_STAGE_ACTIVE;

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "begin-txn")), code,
                  lino, _exit, terrno);
  mInfo("trans:%d, used to create txn %" PRIu64 " term:%" PRId64, pTrans->id, obj.id, obj.term);

  // mndTransSetDbName(pTrans, obj.dbFName, obj.name);
  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);
  TAOS_CHECK_EXIT(mndTransCheckConflict(pMnode, pTrans));

  mndTransSetOper(pTrans, MND_OPER_BEGIN_TXN);
  TAOS_CHECK_EXIT(mndSetCreateTxnCommitLogs(pMnode, pTrans, &obj));

  // Return txnId to client via RPC response (§3.2: <-- txnId ---)
  {
    SMTransReq rspReq = {0};
    rspReq.txnId = obj.id;
    int32_t rspLen = tSerializeSMTransReq(NULL, 0, &rspReq);
    if (rspLen > 0) {
      void *pRsp = taosMemoryCalloc(1, rspLen);
      if (pRsp != NULL) {
        tSerializeSMTransReq(pRsp, rspLen, &rspReq);
        mndTransSetRpcRsp(pTrans, pRsp, rspLen);
      }
    }
  }

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txn:%" PRIu64 ", failed at line %d to begin txn, since %s", obj.id, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

/**
 * Merge client-tracked pVgList into pTxn->pVgList using hash dedup — O(N+M).
 * Replaces the previous O(N*M) double-loop dedup.
 */
static int32_t mndMergeVgList(STxnObj *pTxn, SArray *pNewVgList) {
  if (pNewVgList == NULL || taosArrayGetSize(pNewVgList) == 0) return TSDB_CODE_SUCCESS;

  if (pTxn->pVgList == NULL) {
    pTxn->pVgList = taosArrayInit(taosArrayGetSize(pNewVgList), sizeof(int32_t));
    if (pTxn->pVgList == NULL) return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
  }

  // Build hash from existing entries for O(1) lookup
  SHashObj *pDedup = taosHashInit(taosArrayGetSize(pTxn->pVgList) + taosArrayGetSize(pNewVgList),
                                  taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pDedup == NULL) return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;

  int32_t nExist = (int32_t)taosArrayGetSize(pTxn->pVgList);
  for (int32_t i = 0; i < nExist; ++i) {
    int32_t vgId = *(int32_t *)taosArrayGet(pTxn->pVgList, i);
    int8_t  dummy = 1;
    taosHashPut(pDedup, &vgId, sizeof(vgId), &dummy, sizeof(dummy));
  }

  int32_t nNew = (int32_t)taosArrayGetSize(pNewVgList);
  for (int32_t i = 0; i < nNew; ++i) {
    int32_t vgId = *(int32_t *)taosArrayGet(pNewVgList, i);
    if (taosHashGet(pDedup, &vgId, sizeof(vgId)) == NULL) {
      if (taosArrayPush(pTxn->pVgList, &vgId) == NULL) {
        taosHashCleanup(pDedup);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
      int8_t dummy = 1;
      taosHashPut(pDedup, &vgId, sizeof(vgId), &dummy, sizeof(dummy));
      mInfo("txn:%" PRIu64 ", merged client vgId:%d into participant list", pTxn->id, vgId);
    }
  }

  taosHashCleanup(pDedup);
  return TSDB_CODE_SUCCESS;
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

  // Admission control: reject when active transaction count exceeds limit
  if (txnReq.txnId == 0) {
    int32_t activeCnt = sdbGetSize(pMnode->pSdb, SDB_TXN);
    if (activeCnt >= MND_TXN_MAX_ACTIVE) {
      mError("txn: too many active transactions (%d >= %d), reject BEGIN", activeCnt, MND_TXN_MAX_ACTIVE);
      code = TSDB_CODE_MND_TXN_FULL;
      goto _exit;
    }
  }

  if (txnReq.txnId != 0) {
    if (TXN_IS_REPLICATED(txnReq.txnId)) {
      // §35 taosX replication: accept known replicated txnId (auto-BEGIN from taosX)
    } else {
      mError("txn:%" PRIu64 ", client already has active transaction, reject double BEGIN", txnReq.txnId);
      code = TSDB_CODE_TXN_ALREADY_IN_PROGRESS;
      goto _exit;
    }
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
    if (TXN_IS_REPLICATED(txnReq.txnId)) {
      // §35 taosX: replicated txn already committed/rolled back, idempotent
      mInfo("txn:%" PRIu64 ", replicated txn not found, treat as already committed", txnReq.txnId);
      terrno = 0;
      goto _exit;
    }
    mError("txn:%" PRIu64 ", not found, cannot commit", txnReq.txnId);
    TAOS_CHECK_EXIT(TSDB_CODE_TXN_NOT_EXIST);
  }
  if (pTxn->stage != UTXN_STAGE_ACTIVE) {
    mError("txn:%" PRIu64 ", stage=%s, cannot commit", txnReq.txnId, mndUtxnStageStr(pTxn->stage));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_TXN_INVALID_STAGE);
  }

  // Merge client-tracked pVgList into pTxn->pVgList (O(N) hash dedup)
  TAOS_CHECK_EXIT(mndMergeVgList(pTxn, txnReq.pVgList));

  // Commit: embed all shadow ops + VNode COMMIT into a single STrans
  TAOS_CHECK_EXIT(mndCommitTxn(pMnode, pReq, pTxn));

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
  tFreeSMTransReq(&txnReq);

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
  if (pTxn->stage == UTXN_STAGE_COMMITTING) {
    mError("txn:%" PRIu64 ", stage=%s, cannot rollback after commit decision", txnReq.txnId,
           mndUtxnStageStr(pTxn->stage));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_TXN_INVALID_STAGE);
  }

  // Merge client-tracked pVgList into pTxn->pVgList (O(N) hash dedup)
  TAOS_CHECK_EXIT(mndMergeVgList(pTxn, txnReq.pVgList));

  TAOS_CHECK_EXIT(mndRollbackTxn(pMnode, pReq, pTxn, 0 /* user-initiated */));

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
  tFreeSMTransReq(&txnReq);
  mndReleaseUser(pMnode, pOperUser);

  TAOS_RETURN(code);
}

