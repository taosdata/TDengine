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

    // Clock regression protection: skip if clock moved backward (NTP correction)
    if (elapsed < 0) {
      mDebug("txn:%" PRIu64 ", clock regression detected, elapsed=%" PRId64 "ms, skip timeout check", pTxn->id,
             elapsed);
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
        // Refresh lastActiveTime; timeout scan will handle expiration if needed
        mInfo("txn:%" PRIu64 ", restored in ACTIVE stage, resetting lastActiveTime", pTxn->id);
        taosWLockLatch(&pTxn->lock);
        pTxn->lastActiveTime = taosGetTimestampMs();
        taosWUnLockLatch(&pTxn->lock);
        numRecovered++;
        break;
      }
      case UTXN_STAGE_COMMITTING: {
        mInfo("txn:%" PRIu64 ", restored in COMMITTING stage, re-broadcasting COMMIT", pTxn->id);
        int32_t code = mndCommitTxn(pMnode, &synReq, pTxn);
        if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
          mError("txn:%" PRIu64 ", failed to re-broadcast COMMIT after restore: %s", pTxn->id, tstrerror(code));
        }
        numRecovered++;
        break;
      }
      case UTXN_STAGE_ROLLINGBACK: {
        mInfo("txn:%" PRIu64 ", restored in ROLLINGBACK stage, re-broadcasting ROLLBACK", pTxn->id);
        int32_t code = mndRollbackTxn(pMnode, &synReq, pTxn, TSDB_CODE_VND_TXN_EXPIRED);
        if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
          mError("txn:%" PRIu64 ", failed to re-broadcast ROLLBACK after restore: %s", pTxn->id, tstrerror(code));
        }
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

  taosArrayPush(pTxn->pShadowOps, &op);
  pTxn->lastActiveTime = taosGetTimestampMs();

  taosWUnLockLatch(&pTxn->lock);
  mndReleaseTxn(pMnode, pTxn);

  mInfo("txn:%" PRIu64 ", shadow op added (redo): opType=%d, stb=%s, uid=%" PRId64 ", dataLen:%d", txnId, opType,
        stbName, uid, reqDataLen);
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
 * Apply MNode shadow ops on COMMIT — replay STB DDL via original message handlers.
 *
 * For each shadow op, we:
 *   1. Deserialize the stored request to clear the txnId field
 *   2. Re-serialize with txnId=0 so the handler takes the non-shadow (normal) path
 *   3. Build a synthetic SRpcMsg and dispatch through pMnode->msgFp[]
 *
 * Each STB DDL handler creates its own independent Trans internally.
 * mndTransPrepare commits the SDB change synchronously (via Raft);
 * VNode broadcast (schema push) is async but not needed before child table COMMIT.
 *
 * @param pMnode     The mnode
 * @param pReq       The original COMMIT RpcMsg (for conn info: user, traceId, etc.)
 * @param pTxn       The user batch txn being committed
 * @return 0 on success, error code on failure (partial replay is logged but stops)
 */
static int32_t mndTxnApplyShadowOps(SMnode *pMnode, SRpcMsg *pReq, STxnObj *pTxn) {
  if (pTxn->pShadowOps == NULL || taosArrayGetSize(pTxn->pShadowOps) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t numOps = taosArrayGetSize(pTxn->pShadowOps);
  mInfo("txn:%" PRIu64 ", applying %d MNode STB shadow ops (redo-log COMMIT)", pTxn->id, numOps);

  for (int32_t i = 0; i < numOps; i++) {
    SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);
    int32_t       code = 0;
    tmsg_t        msgType = 0;

    mInfo("txn:%" PRIu64 ", replaying shadow op %d/%d: opType=%d, stb=%s", pTxn->id, i + 1, numOps, pOp->opType,
          pOp->name);

    // Determine message type and clear txnId in serialized data
    switch (pOp->opType) {
      case MND_SHADOW_OP_CREATE_STB: {
        // Undo-log model: STB was created immediately during txn, nothing to do at COMMIT.
        mInfo("txn:%" PRIu64 ", CREATE_STB shadow op %d/%d: no-op (already created), stb=%s", pTxn->id, i + 1, numOps,
              pOp->name);
        taosMemoryFreeClear(pOp->pReqData);
        continue;
      }
      case MND_SHADOW_OP_DROP_STB: {
        msgType = TDMT_MND_DROP_STB;
        SMDropStbReq req = {0};
        if (tDeserializeSMDropStbReq(pOp->pReqData, pOp->reqDataLen, &req) != 0) {
          mError("txn:%" PRIu64 ", failed to deserialize DROP_STB shadow op %d", pTxn->id, i);
          tFreeSMDropStbReq(&req);
          return TSDB_CODE_INVALID_MSG;
        }
        req.txnId = 0;
        int32_t newLen = tSerializeSMDropStbReq(NULL, 0, &req);
        void   *pNewCont = rpcMallocCont(newLen);
        if (pNewCont == NULL) {
          tFreeSMDropStbReq(&req);
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        tSerializeSMDropStbReq(pNewCont, newLen, &req);
        tFreeSMDropStbReq(&req);

        SRpcMsg synMsg = {0};
        synMsg.msgType = msgType;
        synMsg.pCont = pNewCont;
        synMsg.contLen = newLen;
        synMsg.info = pReq->info;

        MndMsgFp fp = pMnode->msgFp[TMSG_INDEX(msgType)];
        if (fp == NULL) {
          rpcFreeCont(pNewCont);
          mError("txn:%" PRIu64 ", no handler for msgType %d", pTxn->id, msgType);
          return TSDB_CODE_MSG_NOT_PROCESSED;
        }
        code = fp(&synMsg);
        break;
      }
      case MND_SHADOW_OP_ALTER_STB: {
        msgType = TDMT_MND_ALTER_STB;
        SMAlterStbReq req = {0};
        if (tDeserializeSMAlterStbReq(pOp->pReqData, pOp->reqDataLen, &req) != 0) {
          mError("txn:%" PRIu64 ", failed to deserialize ALTER_STB shadow op %d", pTxn->id, i);
          tFreeSMAltertbReq(&req);
          return TSDB_CODE_INVALID_MSG;
        }
        req.txnId = 0;
        int32_t newLen = tSerializeSMAlterStbReq(NULL, 0, &req);
        void   *pNewCont = rpcMallocCont(newLen);
        if (pNewCont == NULL) {
          tFreeSMAltertbReq(&req);
          return TSDB_CODE_OUT_OF_MEMORY;
        }
        tSerializeSMAlterStbReq(pNewCont, newLen, &req);
        tFreeSMAltertbReq(&req);

        SRpcMsg synMsg = {0};
        synMsg.msgType = msgType;
        synMsg.pCont = pNewCont;
        synMsg.contLen = newLen;
        synMsg.info = pReq->info;

        MndMsgFp fp = pMnode->msgFp[TMSG_INDEX(msgType)];
        if (fp == NULL) {
          rpcFreeCont(pNewCont);
          mError("txn:%" PRIu64 ", no handler for msgType %d", pTxn->id, msgType);
          return TSDB_CODE_MSG_NOT_PROCESSED;
        }
        code = fp(&synMsg);
        break;
      }
      default:
        mError("txn:%" PRIu64 ", unknown shadow op type %d", pTxn->id, pOp->opType);
        return TSDB_CODE_MND_TXN_ERROR;
    }

    // ACTION_IN_PROGRESS is expected (Trans framework async completion)
    if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
      mError("txn:%" PRIu64 ", shadow op %d/%d failed: %s", pTxn->id, i + 1, numOps, tstrerror(code));
      return code;
    }
    mInfo("txn:%" PRIu64 ", shadow op %d/%d applied (code=%s)", pTxn->id, i + 1, numOps, tstrerror(code));

    // Free the shadow data now that it's been replayed
    taosMemoryFreeClear(pOp->pReqData);
  }

  mInfo("txn:%" PRIu64 ", all %d MNode STB shadow ops applied", pTxn->id, numOps);
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
        }
        break;
      }
      default: {
        // DROP_STB / ALTER_STB: redo-log model, not applied during txn, just discard
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
  SSdbRaw *pDbRaw = mndTxnActionEncode(pTxn);
  if (pDbRaw == NULL) return -1;

  if (mndTransAppendPrepareLog(pTrans, pDbRaw) != 0) return -1;
  if (sdbSetRawStatus(pDbRaw, SDB_STATUS_CREATING) != 0) return -1;
  return 0;
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
    taosHashPut(pVgSet, &vgId, sizeof(vgId), &dummy, sizeof(dummy));
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
        taosHashPut(pVgSet, &vgId, sizeof(vgId), &dummy, sizeof(dummy));
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
      taosHashPut(pVgSet, &vgId, sizeof(vgId), &dummy, sizeof(dummy));
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

  // Commit log: promote CREATE_STB shadow ops by clearing txnId on SStbObj
  if (pTxn->pShadowOps != NULL) {
    int32_t numOps = taosArrayGetSize(pTxn->pShadowOps);
    for (int32_t i = 0; i < numOps; i++) {
      SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);
      if (pOp->opType == MND_SHADOW_OP_CREATE_STB) {
        SStbObj *pStb = mndAcquireStb(pMnode, pOp->name);
        if (pStb != NULL) {
          // Shallow clone: only modify txnId, share pointer fields for encoding
          SStbObj stbClone;
          memcpy(&stbClone, pStb, sizeof(SStbObj));
          stbClone.txnId = 0;
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

  // Add redo actions: send COMMIT to each participant VGroup (pVgList + CREATE_STB DB VGroups)
  {
    SHashObj *pVgSet = mndCollectTxnVgroupIds(pMnode, pTxn);
    if (pVgSet != NULL) {
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
    if (pVgSet != NULL) {
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
  }

  // Undo MNode-side shadow ops — CREATE_STB: append DROP to this Trans; DROP/ALTER: discard
  mndTxnUndoShadowOps(pMnode, pTrans, pTxn);

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
    mError("txn:%" PRIu64 ", client already has active transaction, reject double BEGIN", txnReq.txnId);
    code = TSDB_CODE_TXN_ALREADY_IN_PROGRESS;
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

  // Merge client-tracked pVgList into pTxn->pVgList (dedup)
  if (txnReq.pVgList != NULL && taosArrayGetSize(txnReq.pVgList) > 0) {
    if (pTxn->pVgList == NULL) {
      pTxn->pVgList = taosArrayInit(taosArrayGetSize(txnReq.pVgList), sizeof(int32_t));
    }
    if (pTxn->pVgList != NULL) {
      int32_t nNew = (int32_t)taosArrayGetSize(txnReq.pVgList);
      for (int32_t i = 0; i < nNew; ++i) {
        int32_t vgId = *(int32_t *)taosArrayGet(txnReq.pVgList, i);
        // Dedup check
        bool    found = false;
        int32_t nExist = (int32_t)taosArrayGetSize(pTxn->pVgList);
        for (int32_t j = 0; j < nExist; ++j) {
          if (*(int32_t *)taosArrayGet(pTxn->pVgList, j) == vgId) {
            found = true;
            break;
          }
        }
        if (!found) {
          taosArrayPush(pTxn->pVgList, &vgId);
          mInfo("txn:%" PRIu64 ", merged client vgId:%d into participant list", pTxn->id, vgId);
        }
      }
    }
  }

  // Step 1: Apply MNode-side STB shadow ops (redo-log replay)
  // These must be applied BEFORE VNode COMMIT broadcast, because child tables
  // may depend on super tables that were created within the transaction.
  // Each STB DDL creates its own Trans internally; SDB changes are committed
  // synchronously via Raft, while VNode schema broadcast is async.
  TAOS_CHECK_EXIT(mndTxnApplyShadowOps(pMnode, pReq, pTxn));

  // Step 2: Broadcast COMMIT to participant VNodes (triggers child/normal table shadow replay)
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

  // Merge client-tracked pVgList into pTxn->pVgList (dedup)
  if (txnReq.pVgList != NULL && taosArrayGetSize(txnReq.pVgList) > 0) {
    if (pTxn->pVgList == NULL) {
      pTxn->pVgList = taosArrayInit(taosArrayGetSize(txnReq.pVgList), sizeof(int32_t));
    }
    if (pTxn->pVgList != NULL) {
      int32_t nNew = (int32_t)taosArrayGetSize(txnReq.pVgList);
      for (int32_t i = 0; i < nNew; ++i) {
        int32_t vgId = *(int32_t *)taosArrayGet(txnReq.pVgList, i);
        bool    found = false;
        int32_t nExist = (int32_t)taosArrayGetSize(pTxn->pVgList);
        for (int32_t j = 0; j < nExist; ++j) {
          if (*(int32_t *)taosArrayGet(pTxn->pVgList, j) == vgId) {
            found = true;
            break;
          }
        }
        if (!found) {
          taosArrayPush(pTxn->pVgList, &vgId);
        }
      }
    }
  }

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

