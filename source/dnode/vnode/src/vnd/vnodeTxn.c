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
#include "taoserror.h"
#include "tencode.h"
#include "tglobal.h"
#include "tmsg.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"

// ============================================================================
// VNode Transaction Context Management
// ============================================================================

// Shadow operation types are defined in vnodeInt.h (EShadowOpType)
//
// DDL Isolation Semantics (redo-log model):
//   - DDL within a transaction is NOT applied to real meta immediately.
//   - Instead, the full DDL request is stored as a shadow op (pending redo).
//   - Within the same transaction: shadow ops are visible (overlay on meta lookups).
//   - Other transactions / non-transactional: see only committed meta (invisible).
//   - On COMMIT: shadow ops are replayed against real meta (become permanent).
//   - On ROLLBACK: shadow ops are simply discarded (no meta changes to undo).
//
// Note: Super table (STB) DDL goes through MNode Trans framework (broadcast to VNodes),
// NOT through the client→VNode direct path. Therefore STB operations are NOT tracked
// as shadow ops here. Only child table and normal table DDL, which go directly from
// client to VNode via consistent hash, need shadow ops.
//
// Domain model:
//   - Super table: schema in MNode SDB, copy distributed to VNodes as template
//   - Child table:  created under a super table, shares its schema, stored in VNode
//   - Normal table: stored in VNode with its own dedicated schema

// Shadow operation record — stored in pShadowOps as redo-log for COMMIT.
// Carries the full serialized DDL request so it can be replayed on COMMIT.
// Only records child table and normal table operations (STB DDL handled by MNode Trans).
typedef struct SVnodeShadowOp {
  int8_t   opType;                      // EShadowOpType
  tb_uid_t uid;                         // Table UID
  tb_uid_t suid;                        // Super table UID (non-zero for child tables, 0 for normal tables)
  char     name[TSDB_TABLE_FNAME_LEN];  // Table name
  void    *pReqData;                    // Serialized DDL request (for COMMIT replay)
  int32_t  reqDataLen;                  // Length of serialized request data
} SVnodeShadowOp;

typedef struct SVnodeTxnEntry {
  int64_t txnId;       // Transaction ID
  int64_t term;        // Raft term when registered
  int64_t startTime;   // Transaction start time
  int64_t lastActive;  // Last active time
  int8_t  stage;       // EVtxnStage
  SArray *pShadowOps;  // Shadow operations (pending DDL)
  SArray *pLockedTables;  // Array of char* (table names locked by this txn)
} SVnodeTxnEntry;

// Initialize vnode transaction manager
int32_t vnodeTxnInit(SVnode *pVnode) {
  pVnode->pTxnHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  if (pVnode->pTxnHash == NULL) {
    vError("vgId:%d, failed to init txn hash", TD_VID(pVnode));
    return terrno;
  }

  pVnode->pTxnTableLock = taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);
  if (pVnode->pTxnTableLock == NULL) {
    vError("vgId:%d, failed to init txn table lock hash", TD_VID(pVnode));
    taosHashCleanup(pVnode->pTxnHash);
    pVnode->pTxnHash = NULL;
    return terrno;
  }

  if (taosThreadMutexInit(&pVnode->txnMutex, NULL) != 0) {
    taosHashCleanup(pVnode->pTxnTableLock);
    pVnode->pTxnTableLock = NULL;
    taosHashCleanup(pVnode->pTxnHash);
    pVnode->pTxnHash = NULL;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pVnode->maxSeenTerm = 0;
  vInfo("vgId:%d, txn manager initialized", TD_VID(pVnode));
  return TSDB_CODE_SUCCESS;
}

// Cleanup vnode transaction manager
void vnodeTxnCleanup(SVnode *pVnode) {
  if (pVnode->pTxnHash) {
    void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
    while (pIter) {
      SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;
      if (pEntry->pShadowOps) {
        int32_t sz = taosArrayGetSize(pEntry->pShadowOps);
        for (int32_t i = 0; i < sz; i++) {
          SVnodeShadowOp *pOp = (SVnodeShadowOp *)taosArrayGet(pEntry->pShadowOps, i);
          taosMemoryFreeClear(pOp->pReqData);
        }
        taosArrayDestroy(pEntry->pShadowOps);
      }
      if (pEntry->pLockedTables) {
        int32_t sz = taosArrayGetSize(pEntry->pLockedTables);
        for (int32_t i = 0; i < sz; i++) {
          taosMemoryFree(*(char **)taosArrayGet(pEntry->pLockedTables, i));
        }
        taosArrayDestroy(pEntry->pLockedTables);
      }
      pIter = taosHashIterate(pVnode->pTxnHash, pIter);
    }
    taosHashCleanup(pVnode->pTxnHash);
    pVnode->pTxnHash = NULL;
  }

  if (pVnode->pTxnTableLock) {
    taosHashCleanup(pVnode->pTxnTableLock);
    pVnode->pTxnTableLock = NULL;
  }

  taosThreadMutexDestroy(&pVnode->txnMutex);
  vInfo("vgId:%d, txn manager cleaned up", TD_VID(pVnode));
}

// Get transaction entry by txnId
static SVnodeTxnEntry *vnodeGetTxnEntry(SVnode *pVnode, int64_t txnId) {
  return (SVnodeTxnEntry *)taosHashGet(pVnode->pTxnHash, &txnId, sizeof(int64_t));
}

// Create new transaction entry
static int32_t vnodeCreateTxnEntry(SVnode *pVnode, int64_t txnId, int64_t term) {
  SVnodeTxnEntry entry = {0};
  entry.txnId = txnId;
  entry.term = term;
  entry.startTime = taosGetTimestampMs();
  entry.lastActive = entry.startTime;
  entry.stage = VTXN_STAGE_ACTIVE;
  entry.pShadowOps = taosArrayInit(8, sizeof(SVnodeShadowOp));
  entry.pLockedTables = taosArrayInit(8, sizeof(char *));

  if (entry.pShadowOps == NULL || entry.pLockedTables == NULL) {
    taosArrayDestroy(entry.pShadowOps);
    taosArrayDestroy(entry.pLockedTables);
    return terrno;
  }

  int32_t code = taosHashPut(pVnode->pTxnHash, &txnId, sizeof(int64_t), &entry, sizeof(SVnodeTxnEntry));
  if (code != 0) {
    taosArrayDestroy(entry.pShadowOps);
    taosArrayDestroy(entry.pLockedTables);
    return code;
  }

  return TSDB_CODE_SUCCESS;
}

// Release all table locks held by a transaction entry (caller must hold txnMutex)
static void vnodeReleaseTxnTableLocks(SVnode *pVnode, SVnodeTxnEntry *pEntry) {
  if (pEntry->pLockedTables == NULL) return;
  int32_t sz = taosArrayGetSize(pEntry->pLockedTables);
  for (int32_t i = 0; i < sz; i++) {
    char *name = *(char **)taosArrayGet(pEntry->pLockedTables, i);
    if (name) {
      taosHashRemove(pVnode->pTxnTableLock, name, strlen(name));
      taosMemoryFree(name);
    }
  }
  taosArrayDestroy(pEntry->pLockedTables);
  pEntry->pLockedTables = NULL;
}

// Remove transaction entry (caller must hold txnMutex)
static void vnodeRemoveTxnEntry(SVnode *pVnode, int64_t txnId) {
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry) {
    vnodeReleaseTxnTableLocks(pVnode, pEntry);
    if (pEntry->pShadowOps) {
      int32_t sz = taosArrayGetSize(pEntry->pShadowOps);
      for (int32_t i = 0; i < sz; i++) {
        SVnodeShadowOp *pOp = (SVnodeShadowOp *)taosArrayGet(pEntry->pShadowOps, i);
        taosMemoryFreeClear(pOp->pReqData);
      }
      taosArrayDestroy(pEntry->pShadowOps);
    }
    taosHashRemove(pVnode->pTxnHash, &txnId, sizeof(int64_t));
  }
}

// ============================================================================
// Shadow Operation Management
// ============================================================================

/**
 * Record a shadow operation for a transaction (redo-log model).
 * Called by DDL handlers when executing within an active transaction.
 * The DDL is NOT applied to meta; instead, the full request data is stored
 * so it can be replayed on COMMIT.
 *
 * Only child table and normal table operations are tracked here.
 * Super table DDL goes through MNode Trans framework and is NOT tracked.
 *
 * @param pVnode     The vnode
 * @param txnId      Transaction ID
 * @param opType     EShadowOpType (CREATE_TB / ALTER_TB / DROP_TB)
 * @param name       Table name
 * @param uid        Table UID
 * @param suid       Super table UID (non-zero for child table, 0 for normal table)
 * @param pReqData   Serialized DDL request data (ownership transferred to shadow op)
 * @param reqDataLen Length of serialized data
 * @return TSDB_CODE_SUCCESS on success
 */
int32_t vnodeTxnAddShadowOp(SVnode *pVnode, int64_t txnId, int8_t opType, const char *name, tb_uid_t uid, tb_uid_t suid,
                            void *pReqData, int32_t reqDataLen) {
  if (pVnode->pTxnHash == NULL) {
    taosMemoryFreeClear(pReqData);
    return TSDB_CODE_SUCCESS;
  }

  taosThreadMutexLock(&pVnode->txnMutex);

  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry == NULL) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    taosMemoryFreeClear(pReqData);
    return TSDB_CODE_VND_TXN_EXPIRED;
  }

  SVnodeShadowOp op = {0};
  op.opType = opType;
  op.uid = uid;
  op.suid = suid;
  tstrncpy(op.name, name, sizeof(op.name));
  op.pReqData = pReqData;  // ownership transferred
  op.reqDataLen = reqDataLen;

  taosArrayPush(pEntry->pShadowOps, &op);
  pEntry->lastActive = taosGetTimestampMs();

  taosThreadMutexUnlock(&pVnode->txnMutex);

  vDebug("vgId:%d, shadow op added (redo), txnId:%" PRId64 ", opType:%d, table:%s, uid:%" PRId64 ", dataLen:%d",
         TD_VID(pVnode), txnId, opType, name, uid, reqDataLen);
  return TSDB_CODE_SUCCESS;
}

/**
 * Apply shadow operations on COMMIT — replay redo-log to real meta.
 * DDL was NOT applied during the ACTIVE phase (deferred to shadow).
 * Now we replay each shadow op against meta to make changes permanent.
 *
 * Caller must NOT hold txnMutex.
 */
static int32_t vnodeTxnApplyShadowOps(SVnode *pVnode, SVnodeTxnEntry *pEntry) {
  if (pEntry->pShadowOps == NULL) return TSDB_CODE_SUCCESS;

  int32_t numOps = taosArrayGetSize(pEntry->pShadowOps);
  vInfo("vgId:%d, applying %d shadow ops (redo) for txn %" PRId64, TD_VID(pVnode), numOps, pEntry->txnId);

  // Replay in forward order (same order as original DDL execution)
  for (int32_t i = 0; i < numOps; i++) {
    SVnodeShadowOp *pOp = (SVnodeShadowOp *)taosArrayGet(pEntry->pShadowOps, i);

    switch (pOp->opType) {
      case SHADOW_OP_CREATE_TB: {
        // Replay CREATE TABLE: deserialize and apply to meta
        if (pOp->pReqData == NULL || pOp->reqDataLen == 0) {
          vError("vgId:%d, commit: no request data for create table %s", TD_VID(pVnode), pOp->name);
          break;
        }
        SDecoder           decoder = {0};
        SVCreateTbBatchReq batchReq = {0};
        tDecoderInit(&decoder, pOp->pReqData, pOp->reqDataLen);
        if (tDecodeSVCreateTbBatchReq(&decoder, &batchReq) < 0) {
          vError("vgId:%d, commit: failed to decode create table req for %s", TD_VID(pVnode), pOp->name);
          tDecoderClear(&decoder);
          break;
        }
        for (int32_t j = 0; j < batchReq.nReqs; j++) {
          SVCreateTbReq *pCreateReq = batchReq.pReqs + j;
          int32_t        code = metaCreateTable2(pVnode->pMeta, -1, pCreateReq, NULL);
          if (code < 0) {
            vWarn("vgId:%d, commit: metaCreateTable2 failed for %s, code:0x%x", TD_VID(pVnode), pCreateReq->name,
                  terrno);
          } else {
            vInfo("vgId:%d, commit: created table %s, uid:%" PRId64, TD_VID(pVnode), pCreateReq->name, pCreateReq->uid);
          }
        }
        tDecoderClear(&decoder);
        break;
      }
      case SHADOW_OP_DROP_TB: {
        // Replay DROP TABLE: apply to meta
        if (pOp->pReqData == NULL || pOp->reqDataLen == 0) {
          vError("vgId:%d, commit: no request data for drop table %s", TD_VID(pVnode), pOp->name);
          break;
        }
        SDecoder         decoder = {0};
        SVDropTbBatchReq batchReq = {0};
        tDecoderInit(&decoder, pOp->pReqData, pOp->reqDataLen);
        if (tDecodeSVDropTbBatchReq(&decoder, &batchReq) < 0) {
          vError("vgId:%d, commit: failed to decode drop table req for %s", TD_VID(pVnode), pOp->name);
          tDecoderClear(&decoder);
          break;
        }
        for (int32_t j = 0; j < batchReq.nReqs; j++) {
          SVDropTbReq *pDropReq = batchReq.pReqs + j;
          int32_t      code = metaDropTable2(pVnode->pMeta, -1, pDropReq);
          if (code < 0) {
            vWarn("vgId:%d, commit: metaDropTable2 failed for %s, code:0x%x", TD_VID(pVnode), pDropReq->name, terrno);
          } else {
            vInfo("vgId:%d, commit: dropped table %s", TD_VID(pVnode), pDropReq->name);
          }
        }
        tDecoderClear(&decoder);
        break;
      }
      case SHADOW_OP_ALTER_TB: {
        // Replay ALTER TABLE: apply to meta
        if (pOp->pReqData == NULL || pOp->reqDataLen == 0) {
          vError("vgId:%d, commit: no request data for alter table %s", TD_VID(pVnode), pOp->name);
          break;
        }
        SDecoder     decoder = {0};
        SVAlterTbReq alterReq = {0};
        tDecoderInit(&decoder, pOp->pReqData, pOp->reqDataLen);
        if (tDecodeSVAlterTbReq(&decoder, &alterReq) < 0) {
          vError("vgId:%d, commit: failed to decode alter table req for %s", TD_VID(pVnode), pOp->name);
          tDecoderClear(&decoder);
          break;
        }
        STableMetaRsp metaRsp = {0};
        int32_t       code = metaAlterTable(pVnode->pMeta, -1, &alterReq, &metaRsp);
        if (code < 0) {
          vWarn("vgId:%d, commit: metaAlterTable failed for %s, code:0x%x", TD_VID(pVnode), alterReq.tbName, terrno);
        } else {
          vInfo("vgId:%d, commit: altered table %s", TD_VID(pVnode), alterReq.tbName);
        }
        taosMemoryFreeClear(metaRsp.pSchemas);
        taosMemoryFreeClear(metaRsp.pSchemaExt);
        taosMemoryFreeClear(metaRsp.pColRefs);
        destroyAlterTbReq(&alterReq);
        tDecoderClear(&decoder);
        break;
      }
      default:
        vError("vgId:%d, unknown shadow op type %d", TD_VID(pVnode), pOp->opType);
        break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

/**
 * Discard shadow operations on ROLLBACK — simply discard all pending redo-log entries.
 * Since DDL was NOT applied to meta during the ACTIVE phase (redo-log model),
 * there is nothing to undo. We just free the shadow data.
 *
 * Caller must NOT hold txnMutex.
 */
static int32_t vnodeTxnDiscardShadowOps(SVnode *pVnode, SVnodeTxnEntry *pEntry) {
  if (pEntry->pShadowOps == NULL) return TSDB_CODE_SUCCESS;

  int32_t numOps = taosArrayGetSize(pEntry->pShadowOps);
  vInfo("vgId:%d, discarding %d shadow ops (redo-log) for txn %" PRId64, TD_VID(pVnode), numOps, pEntry->txnId);

  // Redo-log model: meta was never modified, so just log and free.
  // The actual pReqData memory is freed by vnodeRemoveTxnEntry.
  for (int32_t i = 0; i < numOps; i++) {
    SVnodeShadowOp *pOp = (SVnodeShadowOp *)taosArrayGet(pEntry->pShadowOps, i);
    vDebug("vgId:%d, discard shadow op %d/%d: type=%d, table=%s, uid:%" PRId64, TD_VID(pVnode), i + 1, numOps,
           pOp->opType, pOp->name, pOp->uid);
  }

  return TSDB_CODE_SUCCESS;
}

// ============================================================================
// VNode Transaction Message Handlers
// ============================================================================

/**
 * Process COMMIT request from MNode (TDMT_VND_TXN_COMMIT)
 * This finalizes the transaction and makes changes visible
 */
int32_t vnodeProcessTxnCommitReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t        code = TSDB_CODE_SUCCESS;
  SVTxnCommitReq req = {0};

  code = tDeserializeSVTxnCommitReq(pReq, len, &req);
  if (code != 0) {
    vError("vgId:%d, failed to decode txn commit req", TD_VID(pVnode));
    return TSDB_CODE_INVALID_MSG;
  }

  vInfo("vgId:%d, process txn commit, txnId:%" PRId64 ", term:%" PRId64, TD_VID(pVnode), req.txnId, req.term);

  taosThreadMutexLock(&pVnode->txnMutex);

  // Fencing: reject stale term
  if (req.term < pVnode->maxSeenTerm) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    vWarn("vgId:%d, reject txn commit due to stale term, txnId:%" PRId64 ", reqTerm:%" PRId64 ", maxTerm:%" PRId64,
          TD_VID(pVnode), req.txnId, req.term, pVnode->maxSeenTerm);
    return TSDB_CODE_VND_TXN_STALE_TERM;
  }
  if (req.term > pVnode->maxSeenTerm) {
    pVnode->maxSeenTerm = req.term;
  }

  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, req.txnId);
  if (pEntry == NULL) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    // Entry not found: txn expired and shadow data already cleaned up → reject commit
    vError("vgId:%d, txn expired, cannot commit (shadow data lost), txnId:%" PRId64, TD_VID(pVnode), req.txnId);
    return TSDB_CODE_VND_TXN_EXPIRED;
  }

  pEntry->stage = VTXN_STAGE_FINISHING;
  taosThreadMutexUnlock(&pVnode->txnMutex);

  // Apply shadow operations — promote pending changes to permanent
  code = vnodeTxnApplyShadowOps(pVnode, pEntry);
  if (code != 0) {
    vError("vgId:%d, failed to apply shadow ops, txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode), req.txnId, code);
  }

  // Note: WAL persistence is inherent — this handler is invoked via sync→WAL→apply.
  // On crash recovery, WAL replay re-invokes this handler. DDL messages carry txnId
  // so shadow ops are reconstructed from replayed DDLs before COMMIT arrives.

  // Cleanup entry
  taosThreadMutexLock(&pVnode->txnMutex);
  vnodeRemoveTxnEntry(pVnode, req.txnId);
  taosThreadMutexUnlock(&pVnode->txnMutex);

  vInfo("vgId:%d, txn committed, txnId:%" PRId64, TD_VID(pVnode), req.txnId);
  return code;
}

/**
 * Process ROLLBACK request from MNode (TDMT_VND_TXN_ROLLBACK)
 * This aborts the transaction and discards all shadow changes
 */
int32_t vnodeProcessTxnRollbackReq(SVnode *pVnode, int64_t ver, void *pReq, int32_t len, SRpcMsg *pRsp) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SVTxnRollbackReq req = {0};

  code = tDeserializeSVTxnRollbackReq(pReq, len, &req);
  if (code != 0) {
    vError("vgId:%d, failed to decode txn rollback req", TD_VID(pVnode));
    return TSDB_CODE_INVALID_MSG;
  }

  vInfo("vgId:%d, process txn rollback, txnId:%" PRId64 ", term:%" PRId64 ", reason:%d", TD_VID(pVnode), req.txnId,
        req.term, req.reason);

  taosThreadMutexLock(&pVnode->txnMutex);

  // Fencing: reject stale term
  if (req.term < pVnode->maxSeenTerm) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    vWarn("vgId:%d, reject txn rollback due to stale term, txnId:%" PRId64, TD_VID(pVnode), req.txnId);
    return TSDB_CODE_VND_TXN_STALE_TERM;
  }
  if (req.term > pVnode->maxSeenTerm) {
    pVnode->maxSeenTerm = req.term;
  }

  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, req.txnId);
  if (pEntry == NULL) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    // Idempotent: already rolled back or never existed
    vWarn("vgId:%d, txn not found for rollback (idempotent), txnId:%" PRId64, TD_VID(pVnode), req.txnId);
    return TSDB_CODE_SUCCESS;
  }

  pEntry->stage = VTXN_STAGE_FINISHING;
  taosThreadMutexUnlock(&pVnode->txnMutex);

  // Discard shadow operations — undo pending changes
  vnodeTxnDiscardShadowOps(pVnode, pEntry);

  // Note: WAL persistence is inherent — this handler is invoked via sync→WAL→apply.
  // On crash recovery, WAL replay re-invokes this handler. DDL messages carry txnId
  // so shadow ops are reconstructed from replayed DDLs before ROLLBACK arrives.

  // Cleanup entry
  taosThreadMutexLock(&pVnode->txnMutex);
  vnodeRemoveTxnEntry(pVnode, req.txnId);
  taosThreadMutexUnlock(&pVnode->txnMutex);

  vInfo("vgId:%d, txn rolled back, txnId:%" PRId64, TD_VID(pVnode), req.txnId);
  return code;
}

// ============================================================================
// Transaction Timeout Handling
// ============================================================================

/**
 * Check and cleanup expired transactions
 * Called periodically by background thread
 */
void vnodeTxnCheckTimeout(SVnode *pVnode) {
  if (pVnode->pTxnHash == NULL) {
    return;
  }

  int64_t now = taosGetTimestampMs();
  int64_t timeout = (int64_t)tsMetaTxnTimeout * 1000;

  SArray *expiredTxns = taosArrayInit(8, sizeof(int64_t));
  if (expiredTxns == NULL) {
    return;
  }

  taosThreadMutexLock(&pVnode->txnMutex);

  void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
  while (pIter) {
    SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;

    if (now - pEntry->lastActive > timeout) {
      vWarn("vgId:%d, txn expired, txnId:%" PRId64 ", lastActive:%" PRId64 ", now:%" PRId64,
            TD_VID(pVnode), pEntry->txnId, pEntry->lastActive, now);
      taosArrayPush(expiredTxns, &pEntry->txnId);
    }

    pIter = taosHashIterate(pVnode->pTxnHash, pIter);
  }

  int32_t numExpired = taosArrayGetSize(expiredTxns);
  for (int32_t i = 0; i < numExpired; i++) {
    int64_t txnId = *(int64_t *)taosArrayGet(expiredTxns, i);
    vnodeRemoveTxnEntry(pVnode, txnId);
    vInfo("vgId:%d, expired txn rolled back, txnId:%" PRId64, TD_VID(pVnode), txnId);
  }

  taosThreadMutexUnlock(&pVnode->txnMutex);

  taosArrayDestroy(expiredTxns);
}

// ============================================================================
// Fencing (Lock Preemption) Logic
// ============================================================================

/**
 * Preempt locks held by lower-term transactions
 * Called when a higher-term request arrives
 */
int32_t vnodeTxnFencing(SVnode *pVnode, int64_t newTerm, int64_t newTxnId) {
  int32_t code = TSDB_CODE_SUCCESS;

  taosThreadMutexLock(&pVnode->txnMutex);

  if (newTerm <= pVnode->maxSeenTerm) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    return TSDB_CODE_VND_TXN_STALE_TERM;
  }

  pVnode->maxSeenTerm = newTerm;

  SArray *toAbort = taosArrayInit(8, sizeof(int64_t));
  if (toAbort == NULL) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    return terrno;
  }

  void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
  while (pIter) {
    SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;

    if (pEntry->term < newTerm && pEntry->txnId != newTxnId) {
      vInfo("vgId:%d, fencing: abort txn, txnId:%" PRId64 ", term:%" PRId64 ", newTerm:%" PRId64, TD_VID(pVnode),
            pEntry->txnId, pEntry->term, newTerm);
      taosArrayPush(toAbort, &pEntry->txnId);
    }

    pIter = taosHashIterate(pVnode->pTxnHash, pIter);
  }

  int32_t numToAbort = taosArrayGetSize(toAbort);
  for (int32_t i = 0; i < numToAbort; i++) {
    int64_t txnId = *(int64_t *)taosArrayGet(toAbort, i);
    vnodeRemoveTxnEntry(pVnode, txnId);
  }

  taosThreadMutexUnlock(&pVnode->txnMutex);
  taosArrayDestroy(toAbort);

  vInfo("vgId:%d, fencing completed, aborted %d transactions", TD_VID(pVnode), numToAbort);
  return code;
}

// ============================================================================
// StatusReq Keepalive Support
// ============================================================================

/**
 * Collect idle transactions that need keepalive queries.
 * Called by DNode when building statusReq. Transactions silent longer than
 * tsMetaTxnQuietSec are returned for MNode to confirm liveness.
 */
int32_t vnodeCollectIdleTxns(SVnode *pVnode, SArray *pQueries) {
  if (pVnode->pTxnHash == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t now = taosGetTimestampMs();
  int64_t quietThreshold = (int64_t)tsMetaTxnQuietSec * 1000;

  taosThreadMutexLock(&pVnode->txnMutex);

  void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
  while (pIter) {
    SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;
    if (pEntry->stage == VTXN_STAGE_ACTIVE && (now - pEntry->lastActive > quietThreshold)) {
      STxnActiveQuery q = {.txnId = pEntry->txnId, .vgId = TD_VID(pVnode)};
      taosArrayPush(pQueries, &q);
    }
    pIter = taosHashIterate(pVnode->pTxnHash, pIter);
  }

  taosThreadMutexUnlock(&pVnode->txnMutex);
  return TSDB_CODE_SUCCESS;
}

/**
 * Process keepalive ACK from MNode (via statusRsp).
 * alive=1: refresh lastActive; alive=0: txn is dead, rollback locally.
 */
void vnodeTxnProcessActiveAck(SVnode *pVnode, utxn_id_t txnId, int8_t alive) {
  if (pVnode->pTxnHash == NULL) {
    return;
  }

  taosThreadMutexLock(&pVnode->txnMutex);

  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry == NULL) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    return;
  }

  if (alive) {
    pEntry->lastActive = taosGetTimestampMs();
    vDebug("vgId:%d, txn keepalive refreshed, txnId:%" PRId64, TD_VID(pVnode), txnId);
  } else {
    vInfo("vgId:%d, txn dead per MNode ack, rolling back locally, txnId:%" PRId64, TD_VID(pVnode), txnId);
    taosThreadMutexUnlock(&pVnode->txnMutex);
    vnodeTxnDiscardShadowOps(pVnode, pEntry);
    taosThreadMutexLock(&pVnode->txnMutex);
    vnodeRemoveTxnEntry(pVnode, txnId);
  }

  taosThreadMutexUnlock(&pVnode->txnMutex);
}

// ============================================================================
// Table-Level Lock Conflict Detection
// ============================================================================

/**
 * Acquire a table-level lock for a transaction.
 * If the table is already locked by the same txnId, returns SUCCESS (idempotent).
 * If locked by a different txnId, returns TSDB_CODE_VND_TXN_CONFLICT.
 *
 * @param pVnode    The vnode
 * @param tableName The fully qualified table name
 * @param txnId     The transaction ID requesting the lock
 * @return TSDB_CODE_SUCCESS or TSDB_CODE_VND_TXN_CONFLICT
 */
int32_t vnodeTxnLockTable(SVnode *pVnode, const char *tableName, int64_t txnId) {
  if (pVnode->pTxnTableLock == NULL || tableName == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t nameLen = strlen(tableName);

  taosThreadMutexLock(&pVnode->txnMutex);

  // Check if the table is already locked
  int64_t *pExistingTxnId = (int64_t *)taosHashGet(pVnode->pTxnTableLock, tableName, nameLen);
  if (pExistingTxnId != NULL) {
    if (*pExistingTxnId == txnId) {
      // Same transaction already holds the lock — idempotent
      taosThreadMutexUnlock(&pVnode->txnMutex);
      return TSDB_CODE_SUCCESS;
    }
    // Different transaction holds the lock — conflict
    vWarn("vgId:%d, table lock conflict, table:%s, existingTxn:%" PRId64 ", requestTxn:%" PRId64, TD_VID(pVnode),
          tableName, *pExistingTxnId, txnId);
    taosThreadMutexUnlock(&pVnode->txnMutex);
    return TSDB_CODE_VND_TXN_CONFLICT;
  }

  // Verify the requesting transaction exists
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry == NULL) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    vWarn("vgId:%d, cannot lock table, txn not found, table:%s, txnId:%" PRId64, TD_VID(pVnode), tableName, txnId);
    return TSDB_CODE_VND_TXN_EXPIRED;
  }

  // Acquire the lock: add tableName → txnId mapping
  taosHashPut(pVnode->pTxnTableLock, tableName, nameLen, &txnId, sizeof(int64_t));

  // Record the table name in the txn entry for reverse cleanup
  char *nameCopy = taosStrdup(tableName);
  if (nameCopy != NULL) {
    taosArrayPush(pEntry->pLockedTables, &nameCopy);
  }

  pEntry->lastActive = taosGetTimestampMs();

  taosThreadMutexUnlock(&pVnode->txnMutex);

  vDebug("vgId:%d, table locked, table:%s, txnId:%" PRId64, TD_VID(pVnode), tableName, txnId);
  return TSDB_CODE_SUCCESS;
}

/**
 * Release all table locks held by a transaction.
 * Typically called externally when a transaction is cleaned up outside vnodeTxn.c.
 *
 * @param pVnode  The vnode
 * @param txnId   The transaction ID whose locks to release
 */
void vnodeTxnUnlockTables(SVnode *pVnode, int64_t txnId) {
  if (pVnode->pTxnTableLock == NULL) {
    return;
  }

  taosThreadMutexLock(&pVnode->txnMutex);

  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry != NULL) {
    vnodeReleaseTxnTableLocks(pVnode, pEntry);
  }

  taosThreadMutexUnlock(&pVnode->txnMutex);
}

// ============================================================================
// Intra-Transaction Visibility (Shadow Meta Overlay)
// ============================================================================

/**
 * Query the shadow status of a table within a transaction.
 * This enables intra-txn visibility: DDL within the same txn is visible
 * to subsequent operations, while invisible to other txns.
 *
 * @param pVnode    The vnode
 * @param txnId     Transaction ID to check
 * @param name      Table name to look up
 * @return  1 = table was CREATED in this txn (exists in shadow)
 *         -1 = table was DROPPED in this txn (marked as deleted)
 *          0 = table has no shadow state (check real meta)
 */
int32_t vnodeTxnShadowTableStatus(SVnode *pVnode, int64_t txnId, const char *name) {
  if (pVnode->pTxnHash == NULL || txnId == 0 || name == NULL) {
    return 0;
  }

  int32_t result = 0;

  taosThreadMutexLock(&pVnode->txnMutex);

  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry == NULL || pEntry->pShadowOps == NULL) {
    taosThreadMutexUnlock(&pVnode->txnMutex);
    return 0;
  }

  // Scan shadow ops in reverse order to get the latest state
  int32_t numOps = taosArrayGetSize(pEntry->pShadowOps);
  for (int32_t i = numOps - 1; i >= 0; i--) {
    SVnodeShadowOp *pOp = (SVnodeShadowOp *)taosArrayGet(pEntry->pShadowOps, i);
    if (strncmp(pOp->name, name, TSDB_TABLE_FNAME_LEN) == 0) {
      if (pOp->opType == SHADOW_OP_CREATE_TB) {
        result = 1;  // table created in this txn
      } else if (pOp->opType == SHADOW_OP_DROP_TB) {
        result = -1;  // table dropped in this txn
      }
      // For ALTER: table exists but schema changed in shadow (visible within txn)
      // Return 0 so caller falls through to real meta + applies shadow schema
      break;
    }
  }

  taosThreadMutexUnlock(&pVnode->txnMutex);
  return result;
}

/**
 * Check if a non-transaction DDL/DML operation conflicts with any active txn shadow.
 *
 * Scans ALL active txn entries (there should be at most one per VNode in practice)
 * and checks if the target table has a shadow state that would conflict.
 *
 * Conflict matrix (from design doc §16):
 *   PREPARED_CREATE + non-txn CREATE → CONFLICT
 *   PREPARED_CREATE + non-txn SELECT/INSERT/DELETE/ALTER/DROP → TABLE_NOT_EXIST (shadow invisible)
 *   PREPARED_DROP   + non-txn DROP/ALTER/DELETE → CONFLICT (resource busy)
 *   PREPARED_DROP   + non-txn CREATE → TABLE_ALREADY_EXISTS
 *   PREPARED_DROP   + non-txn SELECT/INSERT → OK (read old data)
 *   PREPARED_ALTER  + non-txn ALTER/DROP → CONFLICT
 *   PREPARED_ALTER  + non-txn SELECT/INSERT/DELETE → OK (use old schema)
 *
 * @param pVnode      The vnode
 * @param tableName   The target table name
 * @param incomingOp  0=query/DML, EShadowOpType values for DDL (1=CREATE, 2=ALTER, 3=DROP)
 * @return TSDB_CODE_SUCCESS if no conflict, error code otherwise
 */
int32_t vnodeTxnCheckConflict(SVnode *pVnode, const char *tableName, int8_t incomingOp) {
  if (pVnode->pTxnHash == NULL || tableName == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  taosThreadMutexLock(&pVnode->txnMutex);

  // Scan all active txn entries for shadow state on this table
  void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
  while (pIter) {
    SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;
    if (pEntry->pShadowOps == NULL) {
      pIter = taosHashIterate(pVnode->pTxnHash, pIter);
      continue;
    }

    // Scan shadow ops in reverse order to get the latest state for this table
    int8_t  shadowState = 0;  // 0=none, SHADOW_OP_CREATE_TB/ALTER_TB/DROP_TB
    int32_t numOps = taosArrayGetSize(pEntry->pShadowOps);
    for (int32_t i = numOps - 1; i >= 0; i--) {
      SVnodeShadowOp *pOp = (SVnodeShadowOp *)taosArrayGet(pEntry->pShadowOps, i);
      if (strncmp(pOp->name, tableName, TSDB_TABLE_FNAME_LEN) == 0) {
        shadowState = pOp->opType;
        break;
      }
    }

    if (shadowState == 0) {
      pIter = taosHashIterate(pVnode->pTxnHash, pIter);
      continue;
    }

    // Found shadow state — apply conflict rules
    switch (shadowState) {
      case SHADOW_OP_CREATE_TB:
        // Table exists in shadow (not yet committed)
        if (incomingOp == SHADOW_OP_CREATE_TB) {
          code = TSDB_CODE_VND_TXN_CONFLICT;  // CREATE vs PREPARED_CREATE → conflict
        }
        // All other ops: table is invisible to non-txn → will naturally fail as "not exist"
        break;

      case SHADOW_OP_DROP_TB:
        // Table is logically deleted in shadow (not yet committed)
        if (incomingOp == SHADOW_OP_DROP_TB || incomingOp == SHADOW_OP_ALTER_TB) {
          code = TSDB_CODE_VND_TXN_CONFLICT;  // DROP/ALTER vs PREPARED_DROP → resource busy
        } else if (incomingOp == SHADOW_OP_CREATE_TB) {
          code = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;  // CREATE vs PREPARED_DROP → name occupied
        }
        // SELECT/INSERT (incomingOp=0): allowed (read old data), no conflict
        break;

      case SHADOW_OP_ALTER_TB:
        // Table schema is being modified in shadow
        if (incomingOp == SHADOW_OP_ALTER_TB || incomingOp == SHADOW_OP_DROP_TB) {
          code = TSDB_CODE_VND_TXN_CONFLICT;  // ALTER/DROP vs PREPARED_ALTER → conflict
        }
        // SELECT/INSERT/DELETE/CREATE (incomingOp=0 or CREATE): no conflict
        break;
    }

    if (code != TSDB_CODE_SUCCESS) {
      taosHashCancelIterate(pVnode->pTxnHash, pIter);
      vWarn("vgId:%d, txn conflict: table=%s, shadowState=%d, incomingOp=%d, txnId:%" PRId64, TD_VID(pVnode), tableName,
            shadowState, incomingOp, pEntry->txnId);
      break;
    }

    pIter = taosHashIterate(pVnode->pTxnHash, pIter);
  }

  taosThreadMutexUnlock(&pVnode->txnMutex);
  return code;
}

// ============================================================================
// Combined DDL Registration (lazy-create + lock + shadow op)
// ============================================================================

/**
 * Register a DDL operation within a batch transaction (redo-log model).
 * This is the main entry point called by VNode DDL handlers when txnId != 0.
 * The DDL is NOT applied to meta; it is stored as a shadow op for later COMMIT.
 *
 * Steps:
 *   1. Lazily create txn entry if not exists (enables WAL-replay reconstruction)
 *   2. Lock the table to prevent concurrent conflicting DDL
 *   3. Record shadow op with full serialized request (redo-log for COMMIT)
 *
 * @param pVnode      The vnode
 * @param txnId       Batch transaction ID (must be > 0)
 * @param opType      EShadowOpType (CREATE_TB, ALTER_TB, DROP_TB)
 * @param name        Fully-qualified table name
 * @param uid         Table UID
 * @param suid        Super table UID (0 for normal tables)
 * @param pReqData    Serialized DDL request data (ownership transferred)
 * @param reqDataLen  Length of serialized data
 * @return TSDB_CODE_SUCCESS, TSDB_CODE_VND_TXN_CONFLICT, or error
 */
int32_t vnodeTxnRegisterDdl(SVnode *pVnode, int64_t txnId, int8_t opType, const char *name, tb_uid_t uid, tb_uid_t suid,
                            void *pReqData, int32_t reqDataLen) {
  if (pVnode->pTxnHash == NULL || txnId == 0) {
    taosMemoryFreeClear(pReqData);
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;

  taosThreadMutexLock(&pVnode->txnMutex);

  // Step 1: Lazy-create txn entry if not exists
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry == NULL) {
    // Use current maxSeenTerm so the entry won't be prematurely fenced
    code = vnodeCreateTxnEntry(pVnode, txnId, pVnode->maxSeenTerm);
    if (code != 0) {
      taosThreadMutexUnlock(&pVnode->txnMutex);
      taosMemoryFreeClear(pReqData);
      vError("vgId:%d, failed to create txn entry for DDL, txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode), txnId, code);
      return code;
    }
    pEntry = vnodeGetTxnEntry(pVnode, txnId);
    vInfo("vgId:%d, txn entry lazily created for DDL, txnId:%" PRId64 ", term:%" PRId64, TD_VID(pVnode), txnId,
          pVnode->maxSeenTerm);
  }

  taosThreadMutexUnlock(&pVnode->txnMutex);

  // Step 2: Lock the table (acquires txnMutex internally)
  code = vnodeTxnLockTable(pVnode, name, txnId);
  if (code != 0) {
    taosMemoryFreeClear(pReqData);
    return code;
  }

  // Step 3: Record shadow op with serialized request data (acquires txnMutex internally)
  code = vnodeTxnAddShadowOp(pVnode, txnId, opType, name, uid, suid, pReqData, reqDataLen);
  return code;
}
