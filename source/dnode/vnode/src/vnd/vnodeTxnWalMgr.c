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

// vnodeTxnWalMgr.c
// Producer-side txn WAL cache for txn-atomic CDC delivery to tq/stream consumers.
//
// Design:
//   - Producer fills cache via txnMgrProducerPut / txnMgrReloadPut.
//   - Consumer calls txnMgrConsumerGet when it reads TXN_COMMIT from WAL.
//   - On rollback, pMsgs is freed immediately but the slot (tombstone) is kept
//     so the consumer does not mis-trigger lazy-load.
//   - Eviction removes committed slots that have been idle > gTxnWalEvictAfterIdleSec
//     AND totalMemBytes > gTxnWalMaxMemBytes. Eviction is triggered inline in the
//     producer put path, so no separate timer is needed.

#include "tcommon.h"
#include "vnd.h"
#include "vnodeInt.h"
#include "wal.h"

// ---------------------------------------------------------------------------
// Global config (extern declarations in vnd.h)
// ---------------------------------------------------------------------------
int32_t gTxnWalTtlDays = 30;
int32_t gTxnWalEvictAfterIdleSec = 3600;
int64_t gTxnWalMaxMemBytes = (int64_t)20 * 1024 * 1024;  // 20 MB

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

static void txnMgrFreeSlotMsgs(STxnCacheSlot *pSlot, STxnWalManager *pMgr) {
  if (pSlot->pMsgs == NULL) return;
  int32_t n = taosArrayGetSize(pSlot->pMsgs);
  for (int32_t i = 0; i < n; i++) {
    SWalContCopy *p = taosArrayGetP(pSlot->pMsgs, i);
    taosMemoryFree(p);
  }
  taosArrayDestroy(pSlot->pMsgs);
  if (pMgr) {
    atomic_sub_fetch_64(&pMgr->totalMemBytes, pSlot->slotMemBytes);
  }
  pSlot->pMsgs = NULL;
  pSlot->slotMemBytes = 0;
}

static void txnMgrFreeSlot(STxnCacheSlot *pSlot, STxnWalManager *pMgr) {
  txnMgrFreeSlotMsgs(pSlot, pMgr);
  taosMemoryFree(pSlot);
}

// Core put implementation shared by producer and reload paths.
// Returns TSDB_CODE_SUCCESS (0) on success, or a TSDB error code on failure.
static int32_t txnMgrPutImpl(STxnWalManager *pMgr, txn_id_t txnId, int64_t walIndex, tmsg_t msgType, const void *body,
                             int32_t bodyLen, bool isReload) {
  if (pMgr == NULL || gTxnWalTtlDays <= 0) return TSDB_CODE_SUCCESS;
  if (txnId == 0) return TSDB_CODE_SUCCESS;

  // Handle TXN_COMMIT / TXN_ROLLBACK first — no body to cache.
  if (msgType == TDMT_VND_TXN_COMMIT) {
    STxnCacheSlot **ppSlot = taosHashGet(pMgr->pTxnHash, &txnId, sizeof(txnId));
    STxnCacheSlot  *pSlot  = ppSlot ? *ppSlot : NULL;
    if (pSlot) {
      taosWLockLatch(&pSlot->slotLock);
      pSlot->committed = true;
      pSlot->commitIndex = walIndex;
      taosWUnLockLatch(&pSlot->slotLock);
    }
    // If slot doesn't exist yet (e.g. cache was disabled then re-enabled mid-txn),
    // create a committed tombstone. beginIndex=-1 signals "incomplete tombstone"
    // (no meta msgs were cached), so txnMgrConsumerGet returns NOT_READY.
    else {
      pSlot = taosMemoryCalloc(1, sizeof(STxnCacheSlot));
      if (pSlot == NULL) return terrno;
      taosInitRWLatch(&pSlot->slotLock);
      pSlot->txnId      = txnId;
      pSlot->beginIndex  = -1;
      pSlot->commitIndex = walIndex;
      pSlot->committed   = true;
      int32_t code = taosHashPut(pMgr->pTxnHash, &txnId, sizeof(txnId), &pSlot, sizeof(pSlot));
      if (code != 0) {
        taosMemoryFree(pSlot);
        return code;
      }
    }
    vDebug("txnMgr: txnId=%" PRId64 " committed at walIndex=%" PRId64 " (reload=%d)", txnId, walIndex, (int)isReload);
    return TSDB_CODE_SUCCESS;
  }

  if (msgType == TDMT_VND_TXN_ROLLBACK) {
    int64_t        nowMs   = taosGetTimestampMs();
    STxnCacheSlot **ppSlot = taosHashGet(pMgr->pTxnHash, &txnId, sizeof(txnId));
    STxnCacheSlot  *pSlot  = ppSlot ? *ppSlot : NULL;
    if (pSlot) {
      taosWLockLatch(&pSlot->slotLock);
      txnMgrFreeSlotMsgs(pSlot, pMgr);  // free msgs immediately, keep slot as tombstone
      pSlot->rolledBack = true;
      // Set lastConsumeTs to now so the eviction timer starts immediately.
      // Without this, lastConsumeTs stays 0 and txnMgrEvict never evicts the tombstone.
      atomic_store_64(&pSlot->lastConsumeTs, nowMs);
      taosWUnLockLatch(&pSlot->slotLock);
    }
    // If no slot, create a rolledBack tombstone. beginIndex=-1 marks it as
    // an incomplete tombstone; txnMgrConsumerGet checks rolledBack first so
    // the -1 only matters if rolledBack is somehow false (which it won't be).
    else {
      pSlot = taosMemoryCalloc(1, sizeof(STxnCacheSlot));
      if (pSlot == NULL) return terrno;
      taosInitRWLatch(&pSlot->slotLock);
      pSlot->txnId      = txnId;
      pSlot->beginIndex  = -1;
      pSlot->rolledBack  = true;
      atomic_store_64(&pSlot->lastConsumeTs, nowMs);
      int32_t code = taosHashPut(pMgr->pTxnHash, &txnId, sizeof(txnId), &pSlot, sizeof(pSlot));
      if (code != 0) {
        taosMemoryFree(pSlot);
        return code;
      }
    }
    vDebug("txnMgr: txnId=%" PRId64 " rolled back at walIndex=%" PRId64 " (reload=%d)", txnId, walIndex, (int)isReload);
    return TSDB_CODE_SUCCESS;
  }

  // IS_META_MSG: append a copy to the slot's pMsgs.
  if (!IS_META_MSG(msgType)) {
    return TSDB_CODE_SUCCESS;
  }

  STxnCacheSlot **ppSlot2 = taosHashGet(pMgr->pTxnHash, &txnId, sizeof(txnId));
  STxnCacheSlot  *pSlot   = ppSlot2 ? *ppSlot2 : NULL;
  if (pSlot == NULL) {
    // First message of this txn — create slot.
    pSlot = taosMemoryCalloc(1, sizeof(STxnCacheSlot));
    if (pSlot == NULL) return terrno;
    taosInitRWLatch(&pSlot->slotLock);
    pSlot->txnId = txnId;
    pSlot->beginIndex = walIndex;
    pSlot->pMsgs = taosArrayInit(8, POINTER_BYTES);
    if (pSlot->pMsgs == NULL) {
      taosMemoryFree(pSlot);
      return terrno;
    }
    int32_t code = taosHashPut(pMgr->pTxnHash, &txnId, sizeof(txnId), &pSlot, sizeof(pSlot));
    if (code != 0) {
      txnMgrFreeSlot(pSlot, NULL);
      return code;
    }
  }

  // If already rolled back, ignore further puts (can happen during reload).
  taosRLockLatch(&pSlot->slotLock);
  bool rolledBack = pSlot->rolledBack;
  taosRUnLockLatch(&pSlot->slotLock);
  if (rolledBack) return TSDB_CODE_SUCCESS;

  // Allocate and copy: walIndex + msgType + txnId + raw RPC body.
  SWalContCopy *pCopy = taosMemoryMalloc(sizeof(SWalContCopy) + bodyLen);
  if (pCopy == NULL) return terrno;
  pCopy->walIndex = walIndex;
  pCopy->msgType = msgType;
  pCopy->txnId = txnId;
  pCopy->bodyLen = bodyLen;
  if (bodyLen > 0 && body != NULL) {
    (void)memcpy(pCopy->body, body, bodyLen);
  }

  taosWLockLatch(&pSlot->slotLock);
  if (taosArrayPush(pSlot->pMsgs, &pCopy) == NULL) {
    taosWUnLockLatch(&pSlot->slotLock);
    taosMemoryFree(pCopy);
    return terrno;
  }
  pSlot->slotMemBytes += (int64_t)bodyLen;
  taosWUnLockLatch(&pSlot->slotLock);
  atomic_add_fetch_64(&pMgr->totalMemBytes, (int64_t)bodyLen);

  return TSDB_CODE_SUCCESS;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

STxnWalManager *txnMgrOpen(SWal *pWal, SVnode *pVnode, int64_t lastTxnConsumeTs) {
  STxnWalManager *pMgr = taosMemoryCalloc(1, sizeof(STxnWalManager));
  if (pMgr == NULL) return NULL;

  // HASH_ENTRY_LOCK: per-bucket locking, safe for concurrent get/put from different threads.
  pMgr->pTxnHash = taosHashInit(256, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_ENTRY_LOCK);
  if (pMgr->pTxnHash == NULL) {
    taosMemoryFree(pMgr);
    return NULL;
  }
  pMgr->pWal = pWal;
  pMgr->pVnode = pVnode;
  atomic_store_64(&pMgr->lastTxnConsumeTs, lastTxnConsumeTs);
  vInfo("txnMgr: opened, lastTxnConsumeTs=%" PRId64 " ttlDays=%d", lastTxnConsumeTs, gTxnWalTtlDays);
  return pMgr;
}

void txnMgrClose(STxnWalManager *pMgr) {
  if (pMgr == NULL) return;

  // Free all slots.  No concurrent access at close time.
  void *pIter = taosHashIterate(pMgr->pTxnHash, NULL);
  while (pIter) {
    STxnCacheSlot *pSlot = *(STxnCacheSlot **)pIter;
    txnMgrFreeSlotMsgs(pSlot, NULL);
    taosMemoryFree(pSlot);
    pIter = taosHashIterate(pMgr->pTxnHash, pIter);
  }
  taosHashCleanup(pMgr->pTxnHash);
  taosMemoryFree(pMgr);
}

int32_t txnMgrProducerPut(STxnWalManager *pMgr, txn_id_t txnId, int64_t walIndex, tmsg_t msgType, const void *body,
                          int32_t bodyLen) {
  if (!pMgr) return TSDB_CODE_SUCCESS;
  int32_t code = txnMgrPutImpl(pMgr, txnId, walIndex, msgType, body, bodyLen, false);

  // Inline eviction when memory pressure is high.
  if (atomic_load_64(&pMgr->totalMemBytes) > gTxnWalMaxMemBytes) {
    txnMgrEvict(pMgr, taosGetTimestampMs());
  }
  return code;
}

int32_t txnMgrReloadPut(STxnWalManager *pMgr, txn_id_t txnId, int64_t walIndex, tmsg_t msgType, const void *body,
                        int32_t bodyLen) {
  if (!pMgr) return TSDB_CODE_SUCCESS;
  return txnMgrPutImpl(pMgr, txnId, walIndex, msgType, body, bodyLen, true);
}

int32_t txnMgrConsumerGet(STxnWalManager *pMgr, txn_id_t txnId, int64_t nowMs, SArray **ppMsgs) {
  if (pMgr == NULL || ppMsgs == NULL) return TSDB_CODE_VND_TXN_MSGS_NOT_READY;

  STxnCacheSlot **ppSlot = taosHashGet(pMgr->pTxnHash, &txnId, sizeof(txnId));
  STxnCacheSlot  *pSlot  = ppSlot ? *ppSlot : NULL;
  if (pSlot == NULL) {
    // Begin message was never cached (startup reload miss, eviction under memory pressure,
    // or post-snapshot state).  Consumer will retry up to TXN_NOT_READY_MAX_RETRIES times.
    vError("txnMgr: consumer TXN_COMMIT txnId=%" PRId64
           " — begin not found in cache (slot missing; may be evicted or not yet loaded)",
           txnId);
    return TSDB_CODE_VND_TXN_MSGS_NOT_READY;
  }

  // Check rolledBack first — a ROLLBACK tombstone (rolledBack=true, beginIndex=-1) must
  // return 0 even if beginIndex signals "incomplete". Reading beginIndex is lock-free
  // (set once at creation time), but rolledBack needs the slot lock for correctness.
  taosRLockLatch(&pSlot->slotLock);
  bool    isRolledBack = pSlot->rolledBack;
  int64_t beginIndex   = pSlot->beginIndex;
  taosRUnLockLatch(&pSlot->slotLock);

  if (isRolledBack) {
    *ppMsgs = NULL;
    atomic_store_64(&pSlot->lastConsumeTs, nowMs);
    vDebug("txnMgr: consumer TXN_COMMIT txnId=%" PRId64 " — rolled back, skip", txnId);
    return 0;
  }

  // beginIndex < 0: incomplete tombstone — COMMIT arrived before any meta msgs were cached.
  // WAL index 0 is valid; use -1 as the "not yet seen" sentinel.
  if (beginIndex < 0) {
    vWarn("txnMgr: consumer TXN_COMMIT txnId=%" PRId64
          " — incomplete tombstone (beginIndex=-1); slot unusable",
          txnId);
    return TSDB_CODE_VND_TXN_MSGS_NOT_READY;
  }

  // Update consumer access timestamps atomically.
  atomic_store_64(&pSlot->lastConsumeTs, nowMs);
  int64_t prev = atomic_load_64(&pMgr->lastTxnConsumeTs);
  while (nowMs > prev) {
    int64_t old = atomic_val_compare_exchange_64(&pMgr->lastTxnConsumeTs, prev, nowMs);
    if (old == prev) break;
    prev = old;
  }

  // Read pMsgs under read lock.
  taosRLockLatch(&pSlot->slotLock);
  int32_t ret;
  *ppMsgs = pSlot->pMsgs;
  ret = (*ppMsgs != NULL) ? (int32_t)taosArrayGetSize(*ppMsgs) : 0;
  taosRUnLockLatch(&pSlot->slotLock);

  vDebug("txnMgr: consumer TXN_COMMIT txnId=%" PRId64
         " — found in cache, beginIndex=%" PRId64 " cachedMsgs=%d rolledBack=%d",
         txnId, pSlot->beginIndex, ret, (int)pSlot->rolledBack);

  return ret;
}

void txnMgrEvict(STxnWalManager *pMgr, int64_t nowMs) {
  if (pMgr == NULL) return;
  if (atomic_load_64(&pMgr->totalMemBytes) <= gTxnWalMaxMemBytes) return;

  int64_t idleThresholdMs = (int64_t)gTxnWalEvictAfterIdleSec * 1000;
  SArray *toDelete = taosArrayInit(16, sizeof(txn_id_t));
  if (toDelete == NULL) return;

  void *pIter = taosHashIterate(pMgr->pTxnHash, NULL);
  while (pIter) {
    STxnCacheSlot *pSlot = *(STxnCacheSlot **)pIter;
    // Snapshot slot state under read lock for the eviction eligibility check.
    taosRLockLatch(&pSlot->slotLock);
    bool    committed   = pSlot->committed;
    bool    rolledBack  = pSlot->rolledBack;
    int64_t lastConsume = atomic_load_64(&pSlot->lastConsumeTs);
    taosRUnLockLatch(&pSlot->slotLock);
    // Evict committed slots that have been idle long enough (consumer will lazy-load if needed).
    // Also evict rolled-back tombstones: lastConsumeTs is set at rollback time, so they are
    // cleaned up after the same idle threshold without holding memory or hash entries forever.
    if ((committed || rolledBack) && lastConsume > 0 && (nowMs - lastConsume) > idleThresholdMs) {
      if (taosArrayPush(toDelete, &pSlot->txnId) == NULL) {
        vWarn("txnMgr: evict failed to enqueue txnId=" PRId64 ", skipping", pSlot->txnId);
      }
    }
    pIter = taosHashIterate(pMgr->pTxnHash, pIter);
  }

  int32_t n = taosArrayGetSize(toDelete);
  for (int32_t i = 0; i < n; i++) {
    txn_id_t      *pId = taosArrayGet(toDelete, i);
    STxnCacheSlot **ppSlot = taosHashGet(pMgr->pTxnHash, pId, sizeof(*pId));
    STxnCacheSlot  *pSlot  = ppSlot ? *ppSlot : NULL;
    if (pSlot) {
      taosWLockLatch(&pSlot->slotLock);
      vDebug("txnMgr: evict txnId=%" PRId64 " slotMem=%" PRId64 " idle=%" PRId64 "ms", *pId, pSlot->slotMemBytes,
             nowMs - atomic_load_64(&pSlot->lastConsumeTs));
      txnMgrFreeSlotMsgs(pSlot, pMgr);
      taosWUnLockLatch(&pSlot->slotLock);
      taosMemoryFree(pSlot);
      int32_t removeCode = taosHashRemove(pMgr->pTxnHash, pId, sizeof(*pId));
      if (removeCode != 0) {
        vError("txnMgr: evict failed to remove txnId=" PRId64 " from hash, dangling pointer risk", *pId);
      }
    }
    if (atomic_load_64(&pMgr->totalMemBytes) <= gTxnWalMaxMemBytes) break;
  }
  taosArrayDestroy(toDelete);

  // Refresh WAL keep-version after eviction: some slots were removed, minimum
  // beginIndex may have advanced, allowing WAL segments to be trimmed.
  txnMgrRefreshWalKeepVersion(pMgr, pMgr->pWal, pMgr->pVnode);
}

// Callback for walTxnReadRange: feeds each .txn entry into the manager.
// txnMgrReloadPut (via txnMgrPutImpl) already filters: handles IS_META_MSG,
// TXN_COMMIT, TXN_ROLLBACK; silently ignores everything else.
static int32_t txnMgrTxnReadCb(int64_t walIndex, tmsg_t msgType, txn_id_t txnId,
                                const void *body, int32_t bodyLen, void *arg) {
  STxnWalManager *pMgr = (STxnWalManager *)arg;
  if (txnId == 0) return TSDB_CODE_SUCCESS;
  int32_t code = txnMgrReloadPut(pMgr, txnId, walIndex, msgType, body, bodyLen);
  if (code != 0) {
    vError("txnMgr: txnMgrTxnReadCb failed walIndex:%" PRId64 " txnId:%" PRId64 " since %s",
           walIndex, txnId, tstrerror(code));
  }
  return code;
}

int32_t txnMgrReloadFromWal(STxnWalManager *pMgr, SWal *pWal, int64_t beginVer, int64_t endVer) {
  if (pMgr == NULL || pWal == NULL) return TSDB_CODE_SUCCESS;
  if (gTxnWalTtlDays <= 0) return TSDB_CODE_SUCCESS;
  if (beginVer > endVer) return TSDB_CODE_SUCCESS;

  vInfo("txnMgr: reloadFromWal [%" PRId64 ", %" PRId64 "]", beginVer, endVer);

  // Fast path: read from .txn files (DDL-only, no INSERT entries to skip).
  if (pWal->cfg.enableTxnFile) {
    int32_t code = walTxnReadRange(pWal, beginVer, endVer, txnMgrTxnReadCb, pMgr);
    if (code == TSDB_CODE_SUCCESS) {
      vInfo("txnMgr: reloadFromWal done via .txn, totalMemBytes=%" PRId64, atomic_load_64(&pMgr->totalMemBytes));
      return TSDB_CODE_SUCCESS;
    }
    vWarn("txnMgr: walTxnReadRange failed (%s), falling back to main WAL scan", tstrerror(code));
  }

  // Fallback: scan main WAL (slower, skips INSERT entries).
  SWalReader *pReader = walOpenReader(pWal, 0);
  if (pReader == NULL) {
    vError("txnMgr: walOpenReader failed: %s", tstrerror(terrno));
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  for (int64_t ver = beginVer; ver <= endVer; ver++) {
    code = walFetchHead(pReader, ver);
    if (code < 0) break;

    SWalCont *pHead = &pReader->pHead->head;
    tmsg_t    msgType = pHead->msgType;

    if (!WAL_IS_TXN_MSG(pHead)) {
      (void)walSkipFetchBody(pReader);
      continue;
    }

    if (!IS_META_MSG(msgType)) {
      (void)walSkipFetchBody(pReader);
      continue;
    }

    code = walFetchBody(pReader);
    if (code < 0) break;

    pHead = &pReader->pHead->head;
    txn_id_t txnId = walContTxnId(pHead);
    if (txnId == 0) continue;

    const char *body = walContBody(pHead);
    int32_t     bodyLen = walContBodyLen(pHead);
    code = txnMgrReloadPut(pMgr, txnId, ver, msgType, body, bodyLen);
    if (code != 0) {
      vError("txnMgr: reloadFromWal: txnMgrReloadPut failed ver:%" PRId64 " since %s", ver, tstrerror(code));
      break;
    }
  }

  walCloseReader(pReader);
  if (code != 0) {
    vError("txnMgr: reloadFromWal (fallback) failed since %s", tstrerror(code));
    return code;
  }
  vInfo("txnMgr: reloadFromWal done (fallback), totalMemBytes=%" PRId64, atomic_load_64(&pMgr->totalMemBytes));
  return TSDB_CODE_SUCCESS;
}

int64_t txnMgrGetMinWalIndex(STxnWalManager *pMgr, SVnode *pVnode) {
  if (pMgr == NULL) return INT64_MAX;

  int64_t minIdx = INT64_MAX;

  // Scan CDC WAL cache slots (STxnCacheSlot.beginIndex).
  void *pIter = taosHashIterate(pMgr->pTxnHash, NULL);
  while (pIter) {
    STxnCacheSlot *pSlot = *(STxnCacheSlot **)pIter;
    // Skip incomplete tombstones (beginIndex=-1 means TXN_BEGIN never seen).
    // WAL index 0 is valid — use >= 0, not > 0.
    if (pSlot->beginIndex >= 0 && pSlot->beginIndex < minIdx) {
      minIdx = pSlot->beginIndex;
    }
    pIter = taosHashIterate(pMgr->pTxnHash, pIter);
  }

  // Also scan DDL txn entries (SVnodeTxnEntry.beginWalIndex) via helper.
  // These entries exist until vacuum completes, so their WAL segments must
  // not be trimmed before vacuum finishes.
  if (pVnode != NULL) {
    int64_t ddlMin = vnodeTxnGetMinBeginWalIndex(pVnode);
    if (ddlMin > 0 && ddlMin < minIdx) {
      minIdx = ddlMin;
    }
  }

  atomic_store_64(&pMgr->minTxnIndexNotVacuumed, (minIdx == INT64_MAX) ? 0 : minIdx);
  return minIdx;
}

void txnMgrRefreshWalKeepVersion(STxnWalManager *pMgr, SWal *pWal, SVnode *pVnode) {
  if (pMgr == NULL || pWal == NULL) return;
  int64_t minIdx = txnMgrGetMinWalIndex(pMgr, pVnode);
  int32_t code;
  if (minIdx == INT64_MAX) {
    // No active slots or DDL txns: lift the keep constraint (-1 = no constraint).
    code = walSetKeepVersion(pWal, -1);
    if (code != 0) {
      vError("txnMgr: walSetKeepVersion(-1) failed: %s", tstrerror(code));
    } else {
      vDebug("txnMgr: no active txns, walKeepVersion released");
    }
  } else {
    code = walSetKeepVersion(pWal, minIdx);
    if (code != 0) {
      vError("txnMgr: walSetKeepVersion(%" PRId64 ") failed: %s", minIdx, tstrerror(code));
    } else {
      vDebug("txnMgr: walKeepVersion set to %" PRId64 " (min of CDC+DDL beginWalIndex)", minIdx);
    }
  }
}

