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
#include "meta.h"
#include "taoserror.h"
#include "tencode.h"
#include "tglobal.h"
#include "tmsg.h"
#include "tsimplehash.h"
#include "vnd.h"
#include "vnode.h"
#include "vnodeInt.h"

// Forward declaration: async vacuum task on SCAN_TASK_ASYNC pool
static void vnodeTxnSubmitVacuumAsync(SVnode *pVnode);

// ============================================================================
// VNode Transaction Context Management
// ============================================================================
//
// Lock ordering (MUST NOT be violated):
//   txnMutex → metaWLock/metaRLock
//   (never acquire txnMutex while holding metaWLock or metaRLock)
//
// pTxnTableLock is a HASH_NO_LOCK hash map; it MUST only be accessed while
// holding txnMutex.  All call sites in vnodeSvr.c invoke vnodeTxnLockTable()
// before calling into meta, preserving the txnMutex → metaWLock order.
//

//
// DDL Isolation Semantics (shadow-in-B+tree model):
//   - DDL within a transaction IS applied to real meta immediately, but with
//     txnId/txnStatus set in the SMetaEntry (encoded via type bit 6).
//   - COMMIT:   promotes shadow entries (clear txnId→0; physically delete PRE_DROP).
//   - ROLLBACK: undoes shadow entries (delete PRE_CREATE; restore PRE_DROP/ALTER to NORMAL).
//   - Visibility filtering: queries skip PRE_CREATE; INSERT fails on PRE_DROP.
//
// Note: Super table (STB) DDL goes through MNode Trans framework (broadcast to VNodes),
// NOT through the client→VNode direct path. Therefore STB operations are NOT tracked
// here. Only child table and normal table DDL need txn tracking.
//
// Domain model:
//   - Super table: schema in MNode SDB, copy distributed to VNodes as template
//   - Child table:  created under a super table, shares its schema, stored in VNode
//   - Normal table: stored in VNode with its own dedicated schema

typedef struct SVnodeTxnEntry {
  int64_t    txnId;           // Transaction ID
  int64_t    term;            // Raft term when registered
  int64_t    startTime;       // Transaction start time
  int8_t     stage;           // EVtxnStage
  SSHashObj *pTouchedUids;    // SSHashObj: key=tb_uid_t, value=int8_t(dummy) — O(1) dedup
  SSHashObj *pAlterPrevVers;  // SSHashObj: key=tb_uid_t, value=int64_t(prevVersion) — O(1) lookup
  SArray    *pLockedTables;   // Array of char* (table names locked by this txn)
  // TMQ notification: UIDs tracked at DDL time, sent to TMQ at COMMIT
  SSHashObj *pCreatedUids;  // SSHashObj: key=tb_uid_t, value=int8_t(dummy) — O(1) same-txn undo
  SArray    *pDroppedUids;  // Array of tb_uid_t — tables dropped in this txn (pre-existing tables only)
  // Lazy vacuum fields (populated at finalization, consumed by vacuum)
  int8_t    status;         // ETxnMetaStatus: TXN_META_COMMITTED / TXN_META_ROLLEDBACK
  tb_uid_t *pVacuumUids;    // Array of UIDs to vacuum (converted from pTouchedUids)
  int32_t   numVacuumUids;  // Total UIDs in vacuum array
  int32_t   vacuumIdx;      // Next UID index to process
  // Bulk-drop flag: set when a DROP STB with child tables is registered.
  // Forces the lazy/vacuum path even if pTouchedUids is small, so the
  // cascade child deletion (done inside metaHandleEntry2) runs in the
  // background vacuum thread and does not block the COMMIT handler.
  bool hasBulkDrop;
  // Bulk-drop cascade progress: when non-zero, vacuumIdx still points to this
  // STB's UID entry and vnodeTxnVacuumOneTxn will drop children in batches
  // (up to maxOps per tick) rather than all at once. Reset to 0 when done.
  tb_uid_t bulkDropUid;
  // WAL index of the first DDL message that created this entry (the TXN_BEGIN
  // equivalent for DDL txns). Used by txnMgrGetMinWalIndex to protect WAL
  // segments from being trimmed before vacuum completes.
  int64_t beginWalIndex;
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

  // Thread-safe cache for finalized txn status (read by query threads, written by apply thread)
  pVnode->pTxnMetaCache = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  if (pVnode->pTxnMetaCache == NULL) {
    vError("vgId:%d, failed to init finalized txns hash", TD_VID(pVnode));
    taosHashCleanup(pVnode->pTxnTableLock);
    pVnode->pTxnTableLock = NULL;
    taosHashCleanup(pVnode->pTxnHash);
    pVnode->pTxnHash = NULL;
    return terrno;
  }

  if (taosThreadMutexInit(&pVnode->txnMutex, NULL) != 0) {
    taosHashCleanup(pVnode->pTxnMetaCache);
    pVnode->pTxnMetaCache = NULL;
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
      tSimpleHashCleanup(pEntry->pTouchedUids);
      tSimpleHashCleanup(pEntry->pAlterPrevVers);
      if (pEntry->pLockedTables) {
        int32_t sz = taosArrayGetSize(pEntry->pLockedTables);
        for (int32_t i = 0; i < sz; i++) {
          taosMemoryFree(*(char **)taosArrayGet(pEntry->pLockedTables, i));
        }
        taosArrayDestroy(pEntry->pLockedTables);
      }
      tSimpleHashCleanup(pEntry->pCreatedUids);
      taosArrayDestroy(pEntry->pDroppedUids);
      taosMemoryFreeClear(pEntry->pVacuumUids);
      pIter = taosHashIterate(pVnode->pTxnHash, pIter);
    }
    taosHashCleanup(pVnode->pTxnHash);
    pVnode->pTxnHash = NULL;
  }

  if (pVnode->pTxnTableLock) {
    taosHashCleanup(pVnode->pTxnTableLock);
    pVnode->pTxnTableLock = NULL;
  }

  if (pVnode->pTxnMetaCache) {
    taosHashCleanup(pVnode->pTxnMetaCache);
    pVnode->pTxnMetaCache = NULL;
  }

  (void)taosThreadMutexDestroy(&pVnode->txnMutex);
  vInfo("vgId:%d, txn manager cleaned up", TD_VID(pVnode));
}

/**
 * Reset in-memory txn state after snapshot apply on a follower.
 *
 * After a snapshot replaces all meta B+ trees, the old in-memory txn state
 * (pTxnHash, pTxnMetaCache, pTxnTableLock) is stale.  This function clears
 * those structures and rebuilds them from the new B+ tree content (txn.idx
 * and txn.meta populated during snapshot write).
 *
 * Must be called AFTER vnodeBegin (so pMeta->txn is active for any cleanup
 * deletes inside vnodeTxnRebuildFromMeta).
 */
int32_t vnodeTxnResetForSnapshot(SVnode *pVnode) {
  vInfo("vgId:%d, resetting txn state after snapshot apply", TD_VID(pVnode));

  (void)taosThreadMutexLock(&pVnode->txnMutex);

  // Free all SVnodeTxnEntry internals (same pattern as vnodeTxnCleanup)
  if (pVnode->pTxnHash) {
    void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
    while (pIter) {
      SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;
      tSimpleHashCleanup(pEntry->pTouchedUids);
      tSimpleHashCleanup(pEntry->pAlterPrevVers);
      if (pEntry->pLockedTables) {
        int32_t sz = taosArrayGetSize(pEntry->pLockedTables);
        for (int32_t i = 0; i < sz; i++) {
          taosMemoryFree(*(char **)taosArrayGet(pEntry->pLockedTables, i));
        }
        taosArrayDestroy(pEntry->pLockedTables);
      }
      tSimpleHashCleanup(pEntry->pCreatedUids);
      taosArrayDestroy(pEntry->pDroppedUids);
      taosMemoryFreeClear(pEntry->pVacuumUids);
      pIter = taosHashIterate(pVnode->pTxnHash, pIter);
    }
    taosHashClear(pVnode->pTxnHash);
  }

  if (pVnode->pTxnMetaCache) {
    taosHashClear(pVnode->pTxnMetaCache);
  }

  if (pVnode->pTxnTableLock) {
    taosHashClear(pVnode->pTxnTableLock);
  }

  pVnode->maxSeenTerm = 0;

  (void)taosThreadMutexUnlock(&pVnode->txnMutex);

  // Rebuild from the new snapshot's B+ tree content
  int32_t code = vnodeTxnRebuildFromMeta(pVnode);
  if (code != 0) {
    vError("vgId:%d, failed to rebuild txn state after snapshot, code:0x%x", TD_VID(pVnode), code);
  } else {
    vInfo("vgId:%d, txn state reset and rebuilt after snapshot apply", TD_VID(pVnode));
  }
  return code;
}

// ============================================================================
// Rebuild in-memory txn state from B+ tree (VNode startup / snapshot recovery)
// ============================================================================

// Forward declarations for static helpers used by vnodeTxnRebuildFromMeta
static SVnodeTxnEntry *vnodeGetTxnEntry(SVnode *pVnode, int64_t txnId);
static int32_t         vnodeCreateTxnEntry(SVnode *pVnode, int64_t txnId, int64_t term, int64_t ver);
static int32_t         vnodeTxnTrackUid(SVnodeTxnEntry *pEntry, tb_uid_t uid);
static int32_t         vnodeTxnPrepareVacuumArray(SVnodeTxnEntry *pEntry);

/**
 * After VNode restart or snapshot recovery, the B+ tree may contain entries
 * with txnId != 0 (PRE_CREATE / PRE_ALTER / PRE_DROP). The in-memory
 * SVnodeTxnEntry hash was lost. This function scans the B+ tree and
 * reconstructs SVnodeTxnEntry for each unique txnId found.
 *
 * Must be called AFTER metaOpen (B+ tree available) and vnodeTxnInit (hash ready).
 */
int32_t vnodeTxnRebuildFromMeta(SVnode *pVnode) {
  if (pVnode->pTxnHash == NULL || pVnode->pMeta == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SArray *pScanResult = NULL;
  int32_t code = metaScanTxnEntries(pVnode->pMeta, &pScanResult);
  if (code != 0) {
    vError("vgId:%d, failed to scan txn entries from meta, code:0x%x", TD_VID(pVnode), code);
    return code;
  }

  int32_t numEntries = taosArrayGetSize(pScanResult);
  if (numEntries == 0) {
    taosArrayDestroy(pScanResult);
    vInfo("vgId:%d, txn rebuild: no pending txn entries found in B+ tree", TD_VID(pVnode));
    return TSDB_CODE_SUCCESS;
  }

  vInfo("vgId:%d, txn rebuild: found %d entries with txnId != 0", TD_VID(pVnode), numEntries);

  // No need to lock txnMutex since no requests are being processed yet during startup.
  for (int32_t i = 0; i < numEntries; i++) {
    SMetaTxnScanEntry *pScan = (SMetaTxnScanEntry *)taosArrayGet(pScanResult, i);

    // Ensure SVnodeTxnEntry exists for this txnId
    SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, pScan->txnId);
    if (pEntry == NULL) {
      // Restore beginWalIndex from txn.meta (TXN_META_NONE begin-record) for WAL keepVersion protection.
      // Falls back to 0 if not present (old data or follower with no pending txns at crash time).
      int64_t      beginWalIndex = 0;
      STxnMetaVal finalVal = {0};
      if (metaTxnMetaGet(pVnode->pMeta, pScan->txnId, &finalVal) == 0) {
        beginWalIndex = finalVal.beginWalIndex;
      }
      // term=0: corrected at COMMIT/ROLLBACK time via "Lazy term correction".
      code = vnodeCreateTxnEntry(pVnode, pScan->txnId, 0 /* term: corrected at commit */, beginWalIndex);
      if (code != 0) {
        vError("vgId:%d, txn rebuild: failed to create entry for txnId:%" PRId64, TD_VID(pVnode), pScan->txnId);
        break;
      }
      pEntry = vnodeGetTxnEntry(pVnode, pScan->txnId);
      if (pEntry == NULL) {
        code = TSDB_CODE_OUT_OF_MEMORY;
        vError("vgId:%d, txn rebuild: entry missing after create for txnId:%" PRId64, TD_VID(pVnode), pScan->txnId);
        break;
      }
      vInfo("vgId:%d, txn rebuild: created entry txnId:%" PRId64 " beginWalIndex:%" PRId64, TD_VID(pVnode),
            pScan->txnId, beginWalIndex);
    }

    // Track this UID
    code = vnodeTxnTrackUid(pEntry, pScan->uid);
    if (code != 0) {
      vError("vgId:%d, txn rebuild: failed to track uid:%" PRId64 ", txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode),
             pScan->uid, pScan->txnId, code);
      break;
    }

    // If PRE_ALTER, also reconstruct the ALTER old version record
    if (pScan->txnStatus == META_TXN_PRE_ALTER && pScan->txnOrigVer >= 0) {
      int32_t putCode =
          tSimpleHashPut(pEntry->pAlterPrevVers, &pScan->uid, sizeof(tb_uid_t), &pScan->txnOrigVer, sizeof(int64_t));
      if (putCode != 0) {
        vError("vgId:%d, txn rebuild: failed to put alter record for uid:%" PRId64, TD_VID(pVnode), pScan->uid);
        code = putCode;
        break;
      }
    }

    vDebug("vgId:%d, txn rebuild: uid:%" PRId64 " txnId:%" PRId64 " status:%d oldVer:%" PRId64, TD_VID(pVnode),
           pScan->uid, pScan->txnId, pScan->txnStatus, pScan->txnOrigVer);
  }

  taosArrayDestroy(pScanResult);

  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // Log summary of active txns
  int32_t numTxns = taosHashGetSize(pVnode->pTxnHash);
  vInfo("vgId:%d, txn rebuild phase 1: %d unique txns, %d total entries from txn.idx", TD_VID(pVnode), numTxns,
        numEntries);

  // === Phase 2: Rebuild finalized txn cache from txn.meta ===
  // After a crash, some txns may have been finalized (COMMITTED/ROLLEDBACK) but
  // not fully vacuumed. Scan txn.meta and restore the in-memory cache so
  // visibility filters and vacuum can resume.
  SArray *pFinalResult = NULL;
  code = metaScanTxnMetaEntries(pVnode->pMeta, &pFinalResult);
  if (code != 0) {
    vError("vgId:%d, failed to scan txn.meta, code:0x%x", TD_VID(pVnode), code);
    return code;
  }

  int32_t numFinal = taosArrayGetSize(pFinalResult);
  int32_t numResumed = 0;
  int32_t numStale = 0;

  for (int32_t i = 0; i < numFinal; i++) {
    // Each entry is { int64_t txnId; STxnMetaVal val; }
    const void         *pElem = taosArrayGet(pFinalResult, i);
    int64_t             txnId = *(int64_t *)pElem;
    const STxnMetaVal *pFinalVal = (const STxnMetaVal *)((const char *)pElem + sizeof(int64_t));

    // Always populate the in-memory cache so visibility filters work immediately.
    // Skip TXN_META_NONE entries: these are "begin" records written at first DDL to
    // persist beginWalIndex — they represent in-progress txns, not finalized ones.
    // On failure (OOM during recovery), persistent txn.meta remains authoritative;
    // log a warning so the rare condition is visible.
    if (pFinalVal->status == TXN_META_NONE) {
      // In-progress begin record — skip visibility cache; beginWalIndex is read via metaTxnMetaGet in Phase 1.
      continue;
    }
    int32_t putCode =
        taosHashPut(pVnode->pTxnMetaCache, &txnId, sizeof(int64_t), &pFinalVal->status, sizeof(int8_t));
    if (putCode != 0) {
      vWarn("vgId:%d, txn rebuild: failed to cache finalized txn:%" PRId64 " status:%d, code:0x%x", TD_VID(pVnode),
            txnId, pFinalVal->status, putCode);
    } else {
      atomic_fetch_add_32(&pVnode->txnPendingCount, 1);
    }

    // Check if there are corresponding txn.idx entries (UIDs still needing vacuum)
    SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
    if (pEntry != NULL) {
      // This txn has un-vacuumed UIDs — prepare for vacuum resumption.
      // Set finalStatus/stage ONLY after vacuum array is ready, so that the
      // vacuum thread never sees a finalized entry with numVacuumUids=0 (which
      // would be misinterpreted as "fully vacuumed" via the 0>=0 guard).
      int32_t vacCode = vnodeTxnPrepareVacuumArray(pEntry);
      if (vacCode != 0) {
        vError("vgId:%d, txn rebuild: failed to prepare vacuum for txnId:%" PRId64
               " — entry left un-finalized, will retry on next restart",
               TD_VID(pVnode), txnId);
      } else {
        pEntry->status = pFinalVal->status;
        pEntry->stage = VTXN_STAGE_FINISHING;
        numResumed++;
        vInfo("vgId:%d, txn rebuild: resume vacuum for txnId:%" PRId64 " status:%d numUids:%d", TD_VID(pVnode), txnId,
              pFinalVal->status, pEntry->numVacuumUids);
      }
    } else {
      // No txn.idx entries remain — vacuum was complete, but txn.meta entry is stale.
      // Clean it up (delete from persistent idx; cache entry is harmless and will be ignored).
      (void)metaTxnMetaDelete(pVnode->pMeta, txnId);
      if (taosHashRemove(pVnode->pTxnMetaCache, &txnId, sizeof(int64_t)) != 0) {
        vWarn("vgId:%d, txn rebuild: failed to remove stale txnId:%" PRId64 " from finalized cache", TD_VID(pVnode),
              txnId);
      } else {
        atomic_fetch_sub_32(&pVnode->txnPendingCount, 1);
      }
      numStale++;
      vDebug("vgId:%d, txn rebuild: removed stale txn.meta entry for txnId:%" PRId64, TD_VID(pVnode), txnId);
    }
  }

  taosArrayDestroy(pFinalResult);

  if (numFinal > 0) {
    vInfo("vgId:%d, txn rebuild phase 2: %d finalized txns (%d resumed vacuum, %d stale removed)", TD_VID(pVnode),
          numFinal, numResumed, numStale);
  }

  // If there are finalized txns with pending vacuum, kick off async vacuum
  if (numResumed > 0) {
    vnodeTxnSubmitVacuumAsync(pVnode);
  }

  vInfo("vgId:%d, txn rebuild complete: %d active txns, %d pending vacuum", TD_VID(pVnode),
        taosHashGetSize(pVnode->pTxnHash), taosHashGetSize(pVnode->pTxnMetaCache));
  return TSDB_CODE_SUCCESS;
}

// Get transaction entry by txnId
static SVnodeTxnEntry *vnodeGetTxnEntry(SVnode *pVnode, int64_t txnId) {
  return (SVnodeTxnEntry *)taosHashGet(pVnode->pTxnHash, &txnId, sizeof(int64_t));
}

// Create new transaction entry
static int32_t vnodeCreateTxnEntry(SVnode *pVnode, int64_t txnId, int64_t term, int64_t ver) {
  SVnodeTxnEntry entry = {0};
  entry.txnId = txnId;
  entry.term = term;
  entry.startTime = taosGetTimestampMs();
  entry.stage = VTXN_STAGE_ACTIVE;
  entry.beginWalIndex = ver;  // WAL index of first DDL in this txn
  entry.pTouchedUids = tSimpleHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  entry.pAlterPrevVers = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  entry.pLockedTables = taosArrayInit(8, sizeof(char *));

  if (entry.pTouchedUids == NULL || entry.pAlterPrevVers == NULL || entry.pLockedTables == NULL) {
    tSimpleHashCleanup(entry.pTouchedUids);
    tSimpleHashCleanup(entry.pAlterPrevVers);
    taosArrayDestroy(entry.pLockedTables);
    return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = taosHashPut(pVnode->pTxnHash, &txnId, sizeof(int64_t), &entry, sizeof(SVnodeTxnEntry));
  if (code != 0) {
    tSimpleHashCleanup(entry.pTouchedUids);
    tSimpleHashCleanup(entry.pAlterPrevVers);
    taosArrayDestroy(entry.pLockedTables);
    return code;
  }
  atomic_fetch_add_32(&pVnode->txnPendingCount, 1);

  return TSDB_CODE_SUCCESS;
}

// Release all table locks held by a transaction entry (caller must hold txnMutex)
static void vnodeReleaseTxnTableLocks(SVnode *pVnode, SVnodeTxnEntry *pEntry) {
  if (pEntry->pLockedTables == NULL) return;
  int32_t sz = taosArrayGetSize(pEntry->pLockedTables);
  for (int32_t i = 0; i < sz; i++) {
    char *name = *(char **)taosArrayGet(pEntry->pLockedTables, i);
    if (name) {
      if (taosHashRemove(pVnode->pTxnTableLock, name, strlen(name)) != 0) {
        vWarn("vgId:%d, txn: failed to release table lock for:%s", TD_VID(pVnode), name);
      }
      taosMemoryFree(name);
    }
  }
  taosArrayDestroy(pEntry->pLockedTables);
  pEntry->pLockedTables = NULL;
}

// Returns the minimum beginWalIndex across all live DDL txn entries.
// Returns INT64_MAX when hash is empty or all entries have beginWalIndex==0.
int64_t vnodeTxnGetMinBeginWalIndex(SVnode *pVnode) {
  if (pVnode == NULL || pVnode->pTxnHash == NULL) return INT64_MAX;
  int64_t minIdx = INT64_MAX;
  (void)taosThreadMutexLock(&pVnode->txnMutex);
  void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
  while (pIter) {
    SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;
    if (pEntry->beginWalIndex > 0 && pEntry->beginWalIndex < minIdx) {
      minIdx = pEntry->beginWalIndex;
    }
    pIter = taosHashIterate(pVnode->pTxnHash, pIter);
  }
  (void)taosThreadMutexUnlock(&pVnode->txnMutex);
  return minIdx;
}

// Remove transaction entry (caller must hold txnMutex)
static void vnodeRemoveTxnEntry(SVnode *pVnode, int64_t txnId) {
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry) {
    vnodeReleaseTxnTableLocks(pVnode, pEntry);
    tSimpleHashCleanup(pEntry->pTouchedUids);
    tSimpleHashCleanup(pEntry->pAlterPrevVers);
    tSimpleHashCleanup(pEntry->pCreatedUids);
    taosArrayDestroy(pEntry->pDroppedUids);
    taosMemoryFreeClear(pEntry->pVacuumUids);
    if (taosHashRemove(pVnode->pTxnHash, &txnId, sizeof(int64_t)) != 0) {
      vWarn("vgId:%d, txn: failed to remove txnId:%" PRId64 " from hash", TD_VID(pVnode), txnId);
    } else {
      atomic_fetch_sub_32(&pVnode->txnPendingCount, 1);
    }
  }
}

// ============================================================================
// Shadow-in-B+tree: Entry Management & ALTER Tracking
// ============================================================================

/**
 * Ensure a txn entry exists for the given txnId (lazy create).
 * Called by DDL handlers in vnodeSvr.c before writing to meta.
 */
int32_t vnodeTxnEnsureEntry(SVnode *pVnode, int64_t txnId, int64_t ver) {
  if (pVnode->pTxnHash == NULL || txnId == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pVnode->txnMutex);

  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry == NULL) {
    // ver is the WAL index of the first DDL that triggers this txn entry creation.
    // Subsequent EnsureEntry calls for the same txn are no-ops (entry already exists).
    code = vnodeCreateTxnEntry(pVnode, txnId, pVnode->maxSeenTerm, ver);
    if (code == 0) {
      // Write a TXN_META_NONE begin-record to txn.meta to persist beginWalIndex.
      // This allows crash recovery to restore WAL keepVersion even before COMMIT/ROLLBACK.
      // The record is later overwritten (upserted) by vnodeTxnFinalizeLazy with the final status.
      STxnMetaVal beginVal = {.status = TXN_META_NONE, .timestamp = 0, .beginWalIndex = ver};
      int32_t      metaCode = metaTxnMetaUpsert(pVnode->pMeta, txnId, &beginVal);
      if (metaCode != 0) {
        vWarn("vgId:%d, txn: failed to write begin record to txn.meta txnId:%" PRId64
              " beginWalIndex:%" PRId64 " code:0x%x (WAL may be under-protected after crash)",
              TD_VID(pVnode), txnId, ver, metaCode);
        // non-fatal
      } else {
        vInfo("vgId:%d, txn entry lazily created, txnId:%" PRId64 " beginWalIndex:%" PRId64, TD_VID(pVnode), txnId,
              ver);
      }
    }
  }

  (void)taosThreadMutexUnlock(&pVnode->txnMutex);
  return code;
}

/**
 * Record a UID touched by this txn (for COMMIT/ROLLBACK iteration).
 * SSHashObj provides O(1) dedup (vs O(n) linear scan with SArray).
 */
static int32_t vnodeTxnTrackUid(SVnodeTxnEntry *pEntry, tb_uid_t uid) {
  if (pEntry->pTouchedUids == NULL) return TSDB_CODE_SUCCESS;
  if (tSimpleHashGet(pEntry->pTouchedUids, &uid, sizeof(tb_uid_t)) != NULL) {
    return TSDB_CODE_SUCCESS;  // already tracked
  }
  int8_t  dummy = 1;
  int32_t code = tSimpleHashPut(pEntry->pTouchedUids, &uid, sizeof(tb_uid_t), &dummy, sizeof(dummy));
  if (code != 0) {
    vError("vnodeTxnTrackUid: failed to put uid:%" PRId64, uid);
    return code;
  }
  return TSDB_CODE_SUCCESS;
}

/**
 * Track a table UID as modified by this txn. Called after DDL writes to meta.
 * Used to enumerate all shadow entries during COMMIT/ROLLBACK.
 */
/**
 * Mark a txn as containing a DROP STB with child tables.
 * Ensures COMMIT/ROLLBACK use the lazy/vacuum path so the per-child cascade
 * runs in the background thread instead of blocking the commit handler.
 */
int32_t vnodeTxnMarkBulkDrop(SVnode *pVnode, int64_t txnId) {
  if (pVnode->pTxnHash == NULL || txnId == 0) return TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pVnode->txnMutex);
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry) {
    pEntry->hasBulkDrop = true;
  }
  (void)taosThreadMutexUnlock(&pVnode->txnMutex);
  return TSDB_CODE_SUCCESS;
}

int32_t vnodeTxnTrackTable(SVnode *pVnode, int64_t txnId, tb_uid_t uid) {
  if (pVnode->pTxnHash == NULL || txnId == 0) return TSDB_CODE_SUCCESS;

  int32_t code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pVnode->txnMutex);
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry) {
    code = vnodeTxnTrackUid(pEntry, uid);
  }
  (void)taosThreadMutexUnlock(&pVnode->txnMutex);
  return code;
}

/**
 * Track ALTER's old version for rollback.
 * On ROLLBACK of PRE_ALTER, we need to delete the new-version entry and
 * restore pUidIdx to point at the old version.
 */
int32_t vnodeTxnTrackAlter(SVnode *pVnode, int64_t txnId, tb_uid_t uid, int64_t prevVersion) {
  if (pVnode->pTxnHash == NULL || txnId == 0) return TSDB_CODE_SUCCESS;

  int32_t code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pVnode->txnMutex);
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry) {
    code = tSimpleHashPut(pEntry->pAlterPrevVers, &uid, sizeof(tb_uid_t), &prevVersion, sizeof(int64_t));
    if (code != 0) {
      vError("vgId:%d, vnodeTxnTrackAlter: failed to put alter record for uid:%" PRId64, TD_VID(pVnode), uid);
    }
    if (code == TSDB_CODE_SUCCESS) {
      code = vnodeTxnTrackUid(pEntry, uid);
    }
  }
  (void)taosThreadMutexUnlock(&pVnode->txnMutex);
  return code;
}

// ============================================================================
// TMQ notification tracking: record created/dropped UIDs for post-COMMIT notify
// ============================================================================

int32_t vnodeTxnTrackCreate(SVnode *pVnode, int64_t txnId, tb_uid_t uid) {
  if (pVnode->pTxnHash == NULL || txnId == 0) return TSDB_CODE_SUCCESS;

  int32_t code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pVnode->txnMutex);
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry) {
    if (pEntry->pCreatedUids == NULL) {
      pEntry->pCreatedUids = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    }
    if (pEntry->pCreatedUids == NULL) {
      code = terrno;
      vError("vgId:%d, txn: failed to alloc pCreatedUids for txnId:%" PRId64, TD_VID(pVnode), txnId);
    } else {
      int8_t dummy = 1;
      code = tSimpleHashPut(pEntry->pCreatedUids, &uid, sizeof(tb_uid_t), &dummy, sizeof(dummy));
      if (code != 0) {
        vError("vgId:%d, txn: failed to track created uid:%" PRId64 " for txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode),
               uid, txnId, code);
      }
    }
  }
  (void)taosThreadMutexUnlock(&pVnode->txnMutex);
  return code;
}

int32_t vnodeTxnTrackDrop(SVnode *pVnode, int64_t txnId, tb_uid_t uid) {
  if (pVnode->pTxnHash == NULL || txnId == 0) return TSDB_CODE_SUCCESS;

  int32_t code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pVnode->txnMutex);
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry) {
    // Same-txn undo: if this UID was created in the same txn, remove from created hash
    // (net zero change — TMQ never knew about it). Otherwise add to dropped list.
    bool sameUndone = false;
    if (pEntry->pCreatedUids && tSimpleHashGet(pEntry->pCreatedUids, &uid, sizeof(tb_uid_t)) != NULL) {
      int32_t rc = tSimpleHashRemove(pEntry->pCreatedUids, &uid, sizeof(tb_uid_t));
      if (rc != 0) {
        vError("vgId:%d, txn: failed to remove created uid:%" PRId64 " for txnId:%" PRId64 ", code:0x%x",
               TD_VID(pVnode), uid, txnId, rc);
        code = rc;
      } else {
        sameUndone = true;
      }
    }
    if (code == TSDB_CODE_SUCCESS && !sameUndone) {
      if (pEntry->pDroppedUids == NULL) {
        pEntry->pDroppedUids = taosArrayInit(8, sizeof(int64_t));
      }
      if (pEntry->pDroppedUids == NULL) {
        code = terrno;
        vError("vgId:%d, txn: failed to alloc pDroppedUids for txnId:%" PRId64, TD_VID(pVnode), txnId);
      } else {
        if (taosArrayPush(pEntry->pDroppedUids, &uid) == NULL) {
          code = terrno;
          vError("vgId:%d, txn: failed to track dropped uid:%" PRId64 " for txnId:%" PRId64 ", code:0x%x",
                 TD_VID(pVnode), uid, txnId, code);
        }
      }
    }
  }
  (void)taosThreadMutexUnlock(&pVnode->txnMutex);
  return code;
}

// Notify TMQ about tables created/dropped by a committed txn.
// Called once after COMMIT (both inline and lazy paths).
// Failure is best-effort: COMMIT has already succeeded and the WAL is durable, so we
// log a warning instead of propagating. Consumers will catch up on the next refresh
// (project convention: vnodeSvr.c paths log on tqAddTbUidListForQuerySub/tqDeleteTbUidList failure).
static void vnodeTxnNotifyTmq(SVnode *pVnode, SVnodeTxnEntry *pEntry) {
  if (pEntry->pCreatedUids && tSimpleHashGetSize(pEntry->pCreatedUids) > 0 && pVnode->pTq != NULL) {
    int32_t numCreated = tSimpleHashGetSize(pEntry->pCreatedUids);
    SArray *tbUids = taosArrayInit(numCreated, sizeof(int64_t));
    if (tbUids) {
      int32_t iter = 0;
      void   *pData = NULL;
      while ((pData = tSimpleHashIterate(pEntry->pCreatedUids, pData, &iter))) {
        tb_uid_t uid = *(tb_uid_t *)tSimpleHashGetKey(pData, NULL);
        if (taosArrayPush(tbUids, &uid) == NULL) {
          vError("vgId:%d, txn %" PRId64 ": failed to push uid to TMQ list, since %s", TD_VID(pVnode), pEntry->txnId,
                 tstrerror(terrno));
        }
      }
      int32_t numBuilt = (int32_t)taosArrayGetSize(tbUids);
      if (numBuilt > 0) {
        vInfo("vgId:%d, txn %" PRId64 ": notifying TMQ about %d created tables", TD_VID(pVnode), pEntry->txnId,
              numBuilt);
        int32_t code = tqAddTbUidListForQuerySub(pVnode->pTq, tbUids);
        if (code != 0) {
          vWarn("vgId:%d, txn %" PRId64 ": tqAddTbUidList failed for %d uids, since %s (TMQ may miss create events)",
                TD_VID(pVnode), pEntry->txnId, numBuilt, tstrerror(code));
        }
      }
      taosArrayDestroy(tbUids);
    } else {
      vError("vgId:%d, txn %" PRId64 ": failed to alloc TMQ uid list for %d uids, since %s", TD_VID(pVnode),
             pEntry->txnId, numCreated, tstrerror(terrno));
    }
  }
  if (pEntry->pDroppedUids && taosArrayGetSize(pEntry->pDroppedUids) > 0) {
    int32_t numDropped = (int32_t)taosArrayGetSize(pEntry->pDroppedUids);
    vInfo("vgId:%d, txn %" PRId64 ": notifying TMQ about %d dropped tables", TD_VID(pVnode), pEntry->txnId, numDropped);
    int32_t code = tqDeleteTbUidList(pVnode->pTq, pEntry->pDroppedUids);
    if (code != 0) {
      vWarn("vgId:%d, txn %" PRId64 ": tqDeleteTbUidList failed for %d uids, since %s (TMQ may miss drop events)",
            TD_VID(pVnode), pEntry->txnId, numDropped, tstrerror(code));
    }
  }
}

// ============================================================================
// Shadow-in-B+tree: COMMIT — promote shadow entries
// ============================================================================

/**
 * Promote shadow entries on COMMIT.
 * For each UID touched by this txn, read the current entry from B+ tree:
 *   PRE_CREATE → clear txnId/txnStatus to NORMAL (table becomes visible)
 *   PRE_ALTER  → clear txnId/txnStatus to NORMAL (new schema becomes official)
 *   PRE_DROP   → physically delete the entry (call metaDropTable2 with txnId=0)
 *
 * Caller must NOT hold txnMutex.
 */
static int32_t vnodeTxnPromoteShadowEntries(SVnode *pVnode, SVnodeTxnEntry *pEntry) {
  if (pEntry->pTouchedUids == NULL) return TSDB_CODE_SUCCESS;

  int32_t numUids = tSimpleHashGetSize(pEntry->pTouchedUids);
  vInfo("vgId:%d, promoting %d shadow entries for txn %" PRId64, TD_VID(pVnode), numUids, pEntry->txnId);

  // Accumulate the first fatal error across all UIDs.
  // We MUST NOT early-return: the COMMITTING decision is already durable in WAL,
  // so every UID must be attempted.  Skipping remaining UIDs would leave them in
  // PRE_* state permanently (the in-memory pTxnHash entry is removed by the caller
  // regardless of our return code).
  int32_t lasterr = TSDB_CODE_SUCCESS;

  // tSimpleHashIterate semantics: pass NULL for `data` to start, pass the previous
  // returned data to advance.  Passing NULL repeatedly with a stable `iter` just
  // re-returns the same bucket's first node → infinite loop.  Use the same
  // "advance-at-end-of-body" pattern as vnodeTxnUndoShadowEntries below.
  int32_t iter = 0;
  void   *pData = NULL;
  while ((pData = tSimpleHashIterate(pEntry->pTouchedUids, pData, &iter))) {
    size_t   keyLen = 0;
    tb_uid_t uid = *(tb_uid_t *)tSimpleHashGetKey(pData, &keyLen);

    // Fetch the current entry from B+ tree
    SMetaEntry *pME = NULL;
    int32_t     code = metaFetchEntryByUid(pVnode->pMeta, uid, &pME);
    if (code != 0 || pME == NULL) {
      vWarn("vgId:%d, commit: uid %" PRId64 " not found in B+ tree, skip", TD_VID(pVnode), uid);
      // Clean up any stale pTxnIdx entry. This can happen when TDMT_VND_DROP_STB
      // (txnId=0, COMMIT-time physical drop) arrives before TDMT_VND_TXN_COMMIT and
      // physically removes the STB from pTbDb, leaving a zombie pTxnIdx entry behind.
      int32_t idxCode = metaTxnIdxDelete(pVnode->pMeta, uid);
      if (idxCode != 0) {
        vWarn("vgId:%d, commit: failed to delete stale txn.idx for uid %" PRId64 " since %s", TD_VID(pVnode), uid,
               tstrerror(idxCode));
      }
      continue;
    }

    if (pME->txnId != pEntry->txnId) {
      // Entry doesn't belong to this txn (maybe already committed/cleaned)
      vWarn("vgId:%d, commit: uid %" PRId64 " txnId mismatch: entry=%" PRId64 " expected=%" PRId64 " status=%d, skip",
            TD_VID(pVnode), uid, pME->txnId, pEntry->txnId, pME->txnStatus);
      metaFetchEntryFree(&pME);
      continue;
    }

    // Reset per-UID code; set to non-zero on any main B+ tree failure.
    code = TSDB_CODE_SUCCESS;

    switch (pME->txnStatus) {
      case META_TXN_PRE_CREATE:
      case META_TXN_PRE_ALTER:
        // Promote: clear txnId/txnStatus → NORMAL
        code = metaMarkTableTxnStatus(pVnode->pMeta, uid, 0, META_TXN_NORMAL, -1);
        if (code == 0) {
          vInfo("vgId:%d, commit: promoted uid %" PRId64 " (status %d → NORMAL)", TD_VID(pVnode), uid, pME->txnStatus);
        } else {
          vError("vgId:%d, commit: failed to promote uid %" PRId64 ", code:0x%x", TD_VID(pVnode), uid, code);
        }
        break;

      case META_TXN_PRE_DROP: {
        // Physically delete: reissue drop with txnId=0.
        // Both metaDropSuperTable and metaDropTable2 accept PRE_DROP entries when called
        // with version=-1 (internal) and txnId=0 (non-txn physical delete path).
        // metaCheckDropSuperTableReq does NOT check txnStatus, so no pre-mark is needed.
        // Removing the former two-step (mark→NORMAL then drop) eliminates a split-brain:
        // if mark succeeded but drop failed, the entry became permanently visible (NORMAL,
        // txnId=0), and WAL replay would skip it (txnId mismatch) — a silent DROP failure.
        if (pME->type == TSDB_SUPER_TABLE) {
          SVDropStbReq stbDropReq = {.name = pME->name, .suid = uid, .txnId = 0};
          code = metaDropSuperTable(pVnode->pMeta, -1, &stbDropReq);
        } else {
          SVDropTbReq dropReq = {0};
          dropReq.name = pME->name;
          dropReq.uid = uid;
          dropReq.suid =
              (pME->type == TSDB_CHILD_TABLE || pME->type == TSDB_VIRTUAL_CHILD_TABLE) ? pME->ctbEntry.suid : 0;
          dropReq.txnId = 0;  // non-txn drop = physical delete
          code = metaDropTable2(pVnode->pMeta, -1, &dropReq);
        }
        if (code == 0) {
          vInfo("vgId:%d, commit: physically dropped uid %" PRId64, TD_VID(pVnode), uid);
        } else if (code == TSDB_CODE_NOT_FOUND) {
          // Already cascade-deleted (e.g. STB drop cascaded to children).
          vInfo("vgId:%d, commit: uid %" PRId64 " already cascade-deleted, skip", TD_VID(pVnode), uid);
          code = 0;
        } else {
          vError("vgId:%d, commit: failed to drop uid %" PRId64 ", code:0x%x", TD_VID(pVnode), uid, code);
        }
        break;
      }

      default:
        vDebug("vgId:%d, commit: uid %" PRId64 " has status %d, skip", TD_VID(pVnode), uid, pME->txnStatus);
        break;
    }

    if (code != 0) {
      // Main B+ tree op failed: leave txn.idx entry intact.
      // On WAL replay (Raft), the still-PRE_* entry will be retried.
      lasterr = code;
      metaFetchEntryFree(&pME);
      continue;
    }

    // Main op succeeded: remove from txn.idx.
    // If the index delete fails, the stale entry is benign: on the next WAL replay
    // the now-NORMAL entry's txnId (==0) won't match pEntry->txnId, so it will be
    // cleanly skipped.  Log and continue rather than blocking remaining UIDs.
    int32_t idxCode = metaTxnIdxDelete(pVnode->pMeta, uid);
    if (idxCode != 0) {
      vWarn("vgId:%d, commit: failed to delete txn.idx for uid %" PRId64 " since %s", TD_VID(pVnode), uid, tstrerror(idxCode));
    }

    metaFetchEntryFree(&pME);
  }

  return lasterr;
}

// ============================================================================
// Shadow-in-B+tree: ROLLBACK — undo shadow entries
// ============================================================================

/**
 * Undo shadow entries on ROLLBACK.
 * For each UID touched by this txn, read the current entry from B+ tree:
 *   PRE_CREATE → physically delete (table was never committed)
 *   PRE_DROP   → clear txnId/txnStatus back to NORMAL (restore table)
 *   PRE_ALTER  → delete new version entry, restore pUidIdx to old version
 *
 * Caller must NOT hold txnMutex.
 */
static int32_t vnodeTxnUndoOneShadowEntry(SVnode *pVnode, SVnodeTxnEntry *pEntry, tb_uid_t uid, bool stbOnly,
                                          int32_t *lasterr);

static int32_t vnodeTxnUndoShadowEntries(SVnode *pVnode, SVnodeTxnEntry *pEntry) {
  if (pEntry->pTouchedUids == NULL) return TSDB_CODE_SUCCESS;

  int32_t numUids = tSimpleHashGetSize(pEntry->pTouchedUids);
  vInfo("vgId:%d, undoing %d shadow entries for txn %" PRId64, TD_VID(pVnode), numUids, pEntry->txnId);

  // Same invariant as promote: ROLLBACK is durable in WAL, every UID must be attempted.
  int32_t lasterr = TSDB_CODE_SUCCESS;

  // Two-pass rollback: delete child/normal tables first, then super tables.
  // If a PRE_CREATE STB is deleted before its PRE_CREATE children, the child
  // drop fails because metaHandleChildTableDrop needs the parent entry.
  // Hash iteration order is non-deterministic, so we must enforce ordering.

  // Pass 1: non-STB entries (child tables, normal tables, etc.)
  int32_t iter = 0;
  void   *pData = tSimpleHashIterate(pEntry->pTouchedUids, NULL, &iter);
  while (pData != NULL) {
    size_t   keyLen = 0;
    tb_uid_t uid = *(tb_uid_t *)tSimpleHashGetKey(pData, &keyLen);
    vnodeTxnUndoOneShadowEntry(pVnode, pEntry, uid, false, &lasterr);
    pData = tSimpleHashIterate(pEntry->pTouchedUids, pData, &iter);
  }

  // Pass 2: STB entries only
  iter = 0;
  pData = tSimpleHashIterate(pEntry->pTouchedUids, NULL, &iter);
  while (pData != NULL) {
    size_t   keyLen = 0;
    tb_uid_t uid = *(tb_uid_t *)tSimpleHashGetKey(pData, &keyLen);
    vnodeTxnUndoOneShadowEntry(pVnode, pEntry, uid, true, &lasterr);
    pData = tSimpleHashIterate(pEntry->pTouchedUids, pData, &iter);
  }

  return lasterr;
}

/**
 * Undo a single shadow entry during ROLLBACK.
 * If stbOnly==false, skip STB entries (process CTB/NTB).
 * If stbOnly==true, only process STB entries.
 */
static int32_t vnodeTxnUndoOneShadowEntry(SVnode *pVnode, SVnodeTxnEntry *pEntry, tb_uid_t uid, bool stbOnly,
                                          int32_t *lasterr) {
  // Fetch the current entry from B+ tree
  SMetaEntry *pME = NULL;
  int32_t     code = metaFetchEntryByUid(pVnode->pMeta, uid, &pME);
  if (code != 0 || pME == NULL) {
    if (!stbOnly) {
      vWarn("vgId:%d, rollback: uid %" PRId64 " not found in B+ tree, skip", TD_VID(pVnode), uid);
    }
    return 0;
  }

  if (pME->txnId != pEntry->txnId) {
    metaFetchEntryFree(&pME);
    return 0;
  }

  // Two-pass filter: skip STBs in pass 1, skip non-STBs in pass 2
  if (stbOnly != (pME->type == TSDB_SUPER_TABLE)) {
    metaFetchEntryFree(&pME);
    return 0;
  }

  switch (pME->txnStatus) {
    case META_TXN_PRE_CREATE: {
      // Table was created by this txn — physically delete it
      if (pME->type == TSDB_SUPER_TABLE) {
        // STB: use STB-specific delete path
        SMetaEntry delEntry = {.version = -1, .type = -TSDB_SUPER_TABLE, .uid = uid};
        code = metaHandleEntry2(pVnode->pMeta, &delEntry);
      } else {
        SVDropTbReq dropReq = {0};
        dropReq.name = pME->name;
        dropReq.uid = uid;
        dropReq.suid =
            (pME->type == TSDB_CHILD_TABLE || pME->type == TSDB_VIRTUAL_CHILD_TABLE) ? pME->ctbEntry.suid : 0;
        dropReq.isVirtual = (pME->type == TSDB_VIRTUAL_NORMAL_TABLE || pME->type == TSDB_VIRTUAL_CHILD_TABLE) ? 1 : 0;
        dropReq.txnId = 0;
        code = metaDropTable2(pVnode->pMeta, -1, &dropReq);
      }
      if (code == 0) {
        vInfo("vgId:%d, rollback: deleted PRE_CREATE uid %" PRId64, TD_VID(pVnode), uid);
      } else if (code == TSDB_CODE_NOT_FOUND) {
        // Table was already cascade-deleted (e.g. STB drop cascaded to its children).
        // Treat as success: the entry is effectively gone.
        vInfo("vgId:%d, rollback: PRE_CREATE uid %" PRId64 " already cascade-deleted, skip", TD_VID(pVnode), uid);
        code = 0;
      } else {
        vError("vgId:%d, rollback: failed to delete PRE_CREATE uid %" PRId64 ", code:0x%x", TD_VID(pVnode), uid, code);
      }
      break;
    }

    case META_TXN_PRE_DROP:
      // Table was marked for drop — restore to NORMAL
      code = metaMarkTableTxnStatus(pVnode->pMeta, uid, 0, META_TXN_NORMAL, -1);
      if (code == 0) {
        vInfo("vgId:%d, rollback: restored PRE_DROP uid %" PRId64 " to NORMAL", TD_VID(pVnode), uid);
      } else {
        vError("vgId:%d, rollback: failed to restore PRE_DROP uid %" PRId64, TD_VID(pVnode), uid);
      }
      break;

    case META_TXN_PRE_ALTER: {
      // ALTER created a new version — need to delete it and restore old version.
      // Primary source: txnOrigVer persisted in B+ tree entry (survives snapshot).
      // Fallback: in-memory pAlterPrevVers hash (O(1) lookup by uid).
      int64_t prevVersion = pME->txnOrigVer;
      if (prevVersion < 0 && pEntry->pAlterPrevVers) {
        int64_t *pPrevVer = (int64_t *)tSimpleHashGet(pEntry->pAlterPrevVers, &uid, sizeof(tb_uid_t));
        if (pPrevVer != NULL) {
          prevVersion = *pPrevVer;
        }
      }

      if (prevVersion >= 0) {
        code = metaRollbackAlterTable(pVnode->pMeta, uid, prevVersion);
        if (code == 0) {
          vInfo("vgId:%d, rollback: restored ALTER uid %" PRId64 " to version %" PRId64, TD_VID(pVnode), uid,
                prevVersion);
          // Chained undo: if restored entry is PRE_CREATE from same txn, also delete it
          // (handles CREATE→ALTER→ROLLBACK: after ALTER undo, PRE_CREATE must also be undone)
          SMetaEntry *pRestored = NULL;
          if (metaFetchEntryByUid(pVnode->pMeta, uid, &pRestored) == 0 && pRestored != NULL) {
            if (pRestored->txnId == pEntry->txnId && pRestored->txnStatus == META_TXN_PRE_CREATE) {
              int32_t dropCode;
              if (pRestored->type == TSDB_SUPER_TABLE) {
                SMetaEntry delEntry = {.version = -1, .type = -TSDB_SUPER_TABLE, .uid = uid};
                dropCode = metaHandleEntry2(pVnode->pMeta, &delEntry);
              } else {
                SVDropTbReq dropReq = {0};
                dropReq.name = pRestored->name;
                dropReq.uid = uid;
                dropReq.suid = (pRestored->type == TSDB_CHILD_TABLE || pRestored->type == TSDB_VIRTUAL_CHILD_TABLE)
                                   ? pRestored->ctbEntry.suid
                                   : 0;
                dropReq.isVirtual =
                    (pRestored->type == TSDB_VIRTUAL_NORMAL_TABLE || pRestored->type == TSDB_VIRTUAL_CHILD_TABLE) ? 1
                                                                                                                  : 0;
                dropReq.txnId = 0;
                dropCode = metaDropTable2(pVnode->pMeta, -1, &dropReq);
              }
              if (dropCode == 0) {
                vInfo("vgId:%d, rollback: chained delete PRE_CREATE uid %" PRId64, TD_VID(pVnode), uid);
              } else {
                vError("vgId:%d, rollback: chained delete PRE_CREATE uid %" PRId64 " failed, since %s", TD_VID(pVnode),
                       uid, tstrerror(dropCode));
                code = dropCode;
              }
            }
            metaFetchEntryFree(&pRestored);
          }
        } else {
          vError("vgId:%d, rollback: metaRollbackAlterTable failed for uid %" PRId64 ", since %s", TD_VID(pVnode), uid,
                 tstrerror(code));
        }
      } else {
        // Fallback: old version not available (common for TMQ snapshot targets where
        // only the latest pTbDb entry is replicated). Clear txnStatus to restore normal
        // accessibility. The table retains the post-ALTER schema — acceptable degraded
        // behavior for the extremely rare PRE_ALTER + Snapshot + ROLLBACK sequence.
        // Phase 2 fix: extend TMQ snapshot to also send old-version pTbDb entry.
        code = metaMarkTableTxnStatus(pVnode->pMeta, uid, 0, META_TXN_NORMAL, -1);
        if (code != 0) {
          vError("vgId:%d, rollback: failed to clear ALTER status for uid %" PRId64 ", code:0x%x", TD_VID(pVnode), uid,
                 code);
        }
        vWarn("vgId:%d, rollback: ALTER uid %" PRId64 " old version not found, cleared status (snapshot degraded)",
              TD_VID(pVnode), uid);
      }
      break;
    }

    default:
      vDebug("vgId:%d, rollback: uid %" PRId64 " has status %d, skip", TD_VID(pVnode), uid, pME->txnStatus);
      break;
  }

  if (code != 0) {
    *lasterr = code;
    metaFetchEntryFree(&pME);
    return code;
  }

  // Remove from txn.idx regardless of status.
  // A stale txn.idx entry after successful undo is benign: on WAL replay the
  // now-NORMAL/deleted entry won't match pEntry->txnId and will be skipped.
  int32_t idxCode = metaTxnIdxDelete(pVnode->pMeta, uid);
  if (idxCode != 0 && idxCode != TSDB_CODE_NOT_FOUND) {
    vWarn("vgId:%d, rollback: failed to delete txn.idx for uid %" PRId64 " since %s", TD_VID(pVnode), uid,
          tstrerror(idxCode));
    *lasterr = idxCode;
    metaFetchEntryFree(&pME);
    return idxCode;
  }

  metaFetchEntryFree(&pME);
  return 0;
}

// ============================================================================
// Lazy COMMIT/ROLLBACK: Finalize txn in O(1), vacuum later
// ============================================================================

/**
 * Prepare vacuum array: convert pTouchedUids hash to a flat UID array
 * for incremental batch processing by the vacuum.
 */
static int32_t vnodeTxnPrepareVacuumArray(SVnodeTxnEntry *pEntry) {
  int32_t numUids = tSimpleHashGetSize(pEntry->pTouchedUids);
  if (numUids == 0) {
    pEntry->pVacuumUids = NULL;
    pEntry->numVacuumUids = 0;
    pEntry->vacuumIdx = 0;
    return TSDB_CODE_SUCCESS;
  }

  pEntry->pVacuumUids = taosMemoryMalloc(numUids * sizeof(tb_uid_t));
  if (pEntry->pVacuumUids == NULL) {
    return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t idx = 0;
  int32_t iter = 0;
  void   *pData = tSimpleHashIterate(pEntry->pTouchedUids, NULL, &iter);
  while (pData != NULL) {
    size_t keyLen = 0;
    pEntry->pVacuumUids[idx++] = *(tb_uid_t *)tSimpleHashGetKey(pData, &keyLen);
    pData = tSimpleHashIterate(pEntry->pTouchedUids, pData, &iter);
  }

  pEntry->numVacuumUids = idx;
  pEntry->vacuumIdx = 0;
  return TSDB_CODE_SUCCESS;
}

/**
 * Finalize a txn lazily: write O(1) record to txn.meta + in-memory cache,
 * convert pTouchedUids to vacuum array. Does NOT modify the B+ tree shadow entries.
 * The actual cleanup is done incrementally by vnodeTxnVacuumBatch().
 */
static int32_t vnodeTxnFinalizeLazy(SVnode *pVnode, SVnodeTxnEntry *pEntry, int8_t finalStatus) {
  int32_t code = TSDB_CODE_SUCCESS;

  // 1. Write finalization record to persistent txn.meta (O(1))
  STxnMetaVal finalVal = {.status        = finalStatus,
                           .timestamp    = taosGetTimestampMs(),
                           .beginWalIndex = pEntry->beginWalIndex};
  code = metaTxnMetaUpsert(pVnode->pMeta, pEntry->txnId, &finalVal);
  if (code != 0) {
    vError("vgId:%d, failed to write txn.meta for txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode), pEntry->txnId,
           code);
    return code;
  }

  // 2. Update in-memory cache (thread-safe, visible to query threads immediately).
  // On failure (OOM), persistent txn.meta is still authoritative — visibility
  // queries will fall back to disk lookup, but log a warning so the issue is visible.
  int32_t cacheCode =
      taosHashPut(pVnode->pTxnMetaCache, &pEntry->txnId, sizeof(int64_t), &finalStatus, sizeof(int8_t));
  if (cacheCode != 0) {
    vWarn("vgId:%d, failed to cache finalized txn:%" PRId64 " status:%d in pTxnMetaCache, code:0x%x", TD_VID(pVnode),
          pEntry->txnId, finalStatus, cacheCode);
  } else {
    atomic_fetch_add_32(&pVnode->txnPendingCount, 1);
  }

  // 3. Prepare vacuum array for deferred cleanup
  code = vnodeTxnPrepareVacuumArray(pEntry);
  if (code != 0) {
    vError("vgId:%d, failed to prepare vacuum array for txnId:%" PRId64 " since %s", TD_VID(pVnode), pEntry->txnId,
           tstrerror(code));
    // Rollback step 2: remove cache entry and decrement counter to avoid drift on retry
    if (cacheCode == 0) {
      int32_t removeCode = taosHashRemove(pVnode->pTxnMetaCache, &pEntry->txnId, sizeof(int64_t));
      if(removeCode != 0) {
        vWarn("vgId:%d, failed to remove from finilize hash for txnId:%" PRId64 " since %s", TD_VID(pVnode),
              pEntry->txnId, tstrerror(code));
      }
      atomic_fetch_sub_32(&pVnode->txnPendingCount, 1);
    }
    return code;
  }

  // 4. Mark entry as finalized (keep in pTxnHash for vacuum, release table locks)
  pEntry->status = finalStatus;
  pEntry->stage = VTXN_STAGE_FINISHING;

  // Release table locks immediately (other txns can now operate on these tables)
  vnodeReleaseTxnTableLocks(pVnode, pEntry);

  return TSDB_CODE_SUCCESS;
}

/**
 * Drop up to maxChildren child tables of the given super-table.
 *
 * Uses a three-pass approach to match the throughput of the non-transactional
 * metaHandleSuperTableDrop path:
 *
 *   Pass 1 – collect UIDs: scan CTB cursor, collect up to maxChildren UIDs.
 *   Pass 2 – batch tsdb pre-clear: call tsdbCacheDropSubTables ONCE for the batch
 *             (one lruMutex acquisition, bulk RocksDB deletes). This mirrors what
 *             metaHandleSuperTableDrop does before its per-child tdb loop, so the
 *             per-child tsdbCacheDropTable calls inside metaDropTable2 (Pass 3) find
 *             nothing to delete and become cheap no-ops. Without this step the vacuum
 *             path would do N individual lruMutex+RocksDB-write cycles vs the non-txn
 *             path's single batch — the main cause of the 3–4× throughput gap.
 *   Pass 3 – tdb deletes: call metaDropTable2 for each UID in the batch.
 *
 * Dropped entries disappear from pCtbIdx, so re-opening the cursor from the suid
 * range head each call always starts at the first *remaining* child.
 *
 * Callers compare *pDropped against maxChildren:
 *   *pDropped >= maxChildren  →  quota hit, more children may remain
 *   *pDropped <  maxChildren  →  exhausted (all remaining children dropped)
 */
static void vnodeTxnBulkDropChildren(SVnode *pVnode, tb_uid_t suid, int32_t maxChildren, int32_t *pDropped) {
  *pDropped = 0;
  if (maxChildren <= 0) return;

  // Pass 1: collect up to maxChildren child UIDs.
  SArray *pBatchUids = taosArrayInit(maxChildren, sizeof(tb_uid_t));
  if (pBatchUids == NULL) {
    vError("vgId:%d, bulk-drop: OOM allocating uid batch for suid %" PRId64, TD_VID(pVnode), suid);
    return;
  }

  SMCtbCursor *pCur = metaOpenCtbCursor(pVnode, suid, 1, 0);
  if (pCur == NULL) {
    vError("vgId:%d, bulk-drop: failed to open ctb cursor for suid %" PRId64, TD_VID(pVnode), suid);
    taosArrayDestroy(pBatchUids);
    return;
  }
  while ((int32_t)taosArrayGetSize(pBatchUids) < maxChildren) {
    tb_uid_t childUid = metaCtbCursorNext(pCur);
    if (childUid == 0) break;
    if (taosArrayPush(pBatchUids, &childUid) == NULL) break;  // OOM: process what we have
  }
  metaCloseCtbCursor(pCur);

  int32_t batchSize = (int32_t)taosArrayGetSize(pBatchUids);
  if (batchSize == 0) {
    taosArrayDestroy(pBatchUids);
    return;
  }

  // Pass 2: pre-clear tsdb cache for the whole batch in one lruMutex acquisition.
  // STB schema is still available (STB is in PRE_DROP state, not yet physically deleted).
  if (!TSDB_CACHE_NO(pVnode->config) && pVnode->pTsdb) {
    int32_t rc = tsdbCacheDropSubTables(pVnode->pTsdb, pBatchUids, suid);
    if (rc != 0) {
      vWarn("vgId:%d, bulk-drop: tsdbCacheDropSubTables suid %" PRId64 " failed: %s", TD_VID(pVnode), suid,
            tstrerror(rc));
      // Non-fatal: continue with tdb deletions; tsdb cache is best-effort.
    }
  }

  // Pass 3: drop the tdb B+tree entries.
  for (int32_t i = 0; i < batchSize; i++) {
    tb_uid_t    childUid = *(tb_uid_t *)taosArrayGet(pBatchUids, i);
    SMetaEntry *pChild = NULL;
    metaRLock(pVnode->pMeta);
    int32_t fc = metaFetchEntryByUid(pVnode->pMeta, childUid, &pChild);
    metaULock(pVnode->pMeta);
    if (fc == 0 && pChild != NULL) {
      SVDropTbReq childReq = {
          .name = pChild->name,
          .uid = childUid,
          .suid = pChild->ctbEntry.suid,
          .txnId = 0,
          .isVirtual = (pChild->type == TSDB_VIRTUAL_CHILD_TABLE),
      };
      int32_t rc = metaDropTable2(pVnode->pMeta, -1, &childReq);
      if (rc != 0 && rc != TSDB_CODE_TDB_TABLE_NOT_EXIST) {
        vWarn("vgId:%d, bulk-drop: drop child %" PRId64 " (suid %" PRId64 ") failed: %s", TD_VID(pVnode), childUid,
              suid, tstrerror(rc));
      }
      metaFetchEntryFree(&pChild);
    }
    (*pDropped)++;  // count toward quota even on fetch-miss to guarantee forward progress
  }

  taosArrayDestroy(pBatchUids);
}

/**
 * Vacuum one batch of UIDs for a single finalized txn.
 * Returns number of UIDs processed. 0 means vacuum complete for this txn.
 */
static int32_t vnodeTxnVacuumOneTxn(SVnode *pVnode, SVnodeTxnEntry *pEntry, int32_t maxOps) {
  int32_t processed = 0;

  while (pEntry->vacuumIdx < pEntry->numVacuumUids && processed < maxOps) {
    tb_uid_t uid = pEntry->pVacuumUids[pEntry->vacuumIdx];

    // ---- Bulk child cascade: mid-progress continuation for a large DROP STB ----
    // When bulkDropUid is set, vacuumIdx still points to the STB's UID so that
    // we can keep retrying until all children are gone, then drop the STB itself.
    if (pEntry->status == TXN_META_COMMITTED && pEntry->bulkDropUid == uid) {
      int32_t quota = maxOps - processed;
      int32_t childDropped = 0;
      vnodeTxnBulkDropChildren(pVnode, uid, quota, &childDropped);
      processed += childDropped;

      if (childDropped >= quota) {
        // Quota exhausted; more children may remain. Yield to the next vacuum tick.
        break;
      }

      // All children dropped. Fetch STB entry for name, then drop STB itself (O(1)).
      pEntry->bulkDropUid = 0;
      SMetaEntry *pStb = NULL;
      metaRLock(pVnode->pMeta);
      int32_t fc = metaFetchEntryByUid(pVnode->pMeta, uid, &pStb);
      metaULock(pVnode->pMeta);
      if (fc == 0 && pStb != NULL) {
        SVDropStbReq stbDropReq = {.name = pStb->name, .suid = uid, .txnId = 0};
        int32_t      stbDropCode = metaDropSuperTable(pVnode->pMeta, -1, &stbDropReq);
        metaFetchEntryFree(&pStb);
        if (stbDropCode != 0) {
          vWarn("vgId:%d, vacuum bulk-drop: STB uid %" PRId64 " drop failed: %s, will retry next cycle", TD_VID(pVnode),
                uid, tstrerror(stbDropCode));
          break;
        }
      } else {
        vWarn("vgId:%d, vacuum bulk-drop: STB uid %" PRId64 " not found after child cascade, skip STB drop",
              TD_VID(pVnode), uid);
      }

      int32_t idxCode = metaTxnIdxDelete(pVnode->pMeta, uid);
      if (idxCode != 0) {
        vWarn("vgId:%d, vacuum: failed to delete stale txn.idx for uid %" PRId64 " since %s", TD_VID(pVnode), uid,
               tstrerror(idxCode));
      }
      pEntry->vacuumIdx++;
      processed++;
      continue;
    }

    SMetaEntry *pME = NULL;
    // Vacuum runs on the async-scan thread pool concurrently with the vnode-write thread,
    // which mutates pTbDb/pUidIdx under metaWLock. Without an rlock here, our tdb btree
    // traversal can observe a half-modified page (pgno=0) and trip an internal assert.
    // The mutating helpers below (metaMarkTableTxnStatus / metaDropTable2 / metaDropSuperTable /
    // metaRollbackAlterTable / metaTxnIdxDelete) take their own wlocks, so we drop the rlock
    // before calling them.
    metaRLock(pVnode->pMeta);
    int32_t code = metaFetchEntryByUid(pVnode->pMeta, uid, &pME);
    metaULock(pVnode->pMeta);
    if (code != 0 || pME == NULL) {
      vWarn("vgId:%d, vacuum: uid %" PRId64 " not found in B+ tree, skip", TD_VID(pVnode), uid);
      pEntry->vacuumIdx++;
      processed++;
      continue;
    }

    if (pME->txnId != pEntry->txnId) {
      metaFetchEntryFree(&pME);
      pEntry->vacuumIdx++;
      processed++;
      continue;
    }

    bool bulkYield = false;      // true when a bulk DROP STB hits quota mid-cascade
    bool skipTxnIdxDel = false;  // true when orphaned entry's pTxnIdx must be preserved

    if (pEntry->status == TXN_META_COMMITTED) {
      // === COMMIT vacuum: same logic as vnodeTxnPromoteShadowEntries ===
      switch (pME->txnStatus) {
        case META_TXN_PRE_CREATE:
        case META_TXN_PRE_ALTER:
          code = metaMarkTableTxnStatus(pVnode->pMeta, uid, 0, META_TXN_NORMAL, -1);
          if (code == 0) {
            vDebug("vgId:%d, vacuum-commit: promoted uid %" PRId64 " (status %d → NORMAL)", TD_VID(pVnode), uid,
                   pME->txnStatus);
          }
          break;
        case META_TXN_PRE_DROP: {
          if (pME->type == TSDB_SUPER_TABLE) {
            // Drop children in batches to avoid a single O(N) operation that would
            // block the vacuum thread for millions of operations in one tick.
            int32_t quota = maxOps - processed;
            int32_t childDropped = 0;
            vnodeTxnBulkDropChildren(pVnode, uid, quota, &childDropped);
            processed += childDropped;

            if (childDropped >= quota) {
              // Quota hit; more children may remain. Mark bulk drop in progress
              // and yield — vacuumIdx stays at this STB uid so the next tick
              // enters the bulk-continuation block at the top of this loop.
              pEntry->bulkDropUid = uid;
              bulkYield = true;
            } else {
              // All children dropped. Drop STB itself — no children remain, so
              // metaHandleSuperTableDrop is O(1) (empty child list).
              SVDropStbReq stbDropReq = {.name = pME->name, .suid = uid, .txnId = 0};
              code = metaDropSuperTable(pVnode->pMeta, -1, &stbDropReq);
            }
          } else {
            SVDropTbReq dropReq = {0};
            dropReq.name = pME->name;
            dropReq.uid = uid;
            dropReq.suid =
                (pME->type == TSDB_CHILD_TABLE || pME->type == TSDB_VIRTUAL_CHILD_TABLE) ? pME->ctbEntry.suid : 0;
            dropReq.txnId = 0;
            dropReq.isVirtual = (pME->type == TSDB_VIRTUAL_CHILD_TABLE || pME->type == TSDB_VIRTUAL_NORMAL_TABLE);
            code = metaDropTable2(pVnode->pMeta, -1, &dropReq);
          }
          break;
        }
        default:
          break;
      }
    } else {
      // === ROLLBACK vacuum: same logic as vnodeTxnUndoShadowEntries ===
      switch (pME->txnStatus) {
        case META_TXN_PRE_CREATE: {
          SVDropTbReq dropReq = {0};
          dropReq.name = pME->name;
          dropReq.uid = uid;
          dropReq.suid =
              (pME->type == TSDB_CHILD_TABLE || pME->type == TSDB_VIRTUAL_CHILD_TABLE) ? pME->ctbEntry.suid : 0;
          dropReq.txnId = 0;
          dropReq.isVirtual = (pME->type == TSDB_VIRTUAL_CHILD_TABLE || pME->type == TSDB_VIRTUAL_NORMAL_TABLE);
          if (pME->type == TSDB_SUPER_TABLE) {
            // No pre-mark needed: metaHandleEntry2 (-TSDB_SUPER_TABLE) does not check txnStatus.
            SVDropStbReq stbDropReq = {.name = pME->name, .suid = uid, .txnId = 0};
            code = metaDropSuperTable(pVnode->pMeta, -1, &stbDropReq);
            if (code == TSDB_CODE_TDB_STB_NOT_EXIST) {
              // Name was reclaimed by a new CREATE after ROLLBACK. Clean up the
              // orphaned B+ tree entries by uid, skipping the name index.
              vInfo("vgId:%d, vacuum-rollback: stb uid %" PRId64 " name '%s' reclaimed, cleaning orphaned entry",
                    TD_VID(pVnode), uid, pME->name);
              code = metaDropOrphanedEntry(pVnode->pMeta, uid, pME);
              if (code != 0) {
                // Cleanup failed — preserve pTxnIdx for visibility safety, retry next cycle
                vWarn("vgId:%d, vacuum-rollback: failed to clean orphan stb uid %" PRId64 " code:0x%x, will retry",
                      TD_VID(pVnode), uid, code);
                skipTxnIdxDel = true;
                code = 0;
              }
            }
          } else {
            code = metaDropTable2(pVnode->pMeta, -1, &dropReq);
            if (code == TSDB_CODE_TDB_TABLE_NOT_EXIST) {
              // Name was reclaimed by a new CREATE after ROLLBACK. Clean up the
              // orphaned B+ tree entries (pUidIdx, pTbDb, pCtbIdx, pTagIdx) by uid,
              // skipping the name index (which now belongs to the new table).
              vInfo("vgId:%d, vacuum-rollback: uid %" PRId64 " name '%s' reclaimed, cleaning orphaned entry",
                    TD_VID(pVnode), uid, pME->name);
              code = metaDropOrphanedEntry(pVnode->pMeta, uid, pME);
              if (code != 0) {
                // Cleanup failed — preserve pTxnIdx for visibility safety, retry next cycle
                vWarn("vgId:%d, vacuum-rollback: failed to clean orphan uid %" PRId64 " code:0x%x, will retry",
                      TD_VID(pVnode), uid, code);
                skipTxnIdxDel = true;
                code = 0;
              }
            }
          }
          // Handle chained undo (CREATE→ALTER in same txn)
          if (pEntry->pAlterPrevVers) {
            int64_t *pChainedPrevVer = (int64_t *)tSimpleHashGet(pEntry->pAlterPrevVers, &uid, sizeof(tb_uid_t));
            if (pChainedPrevVer != NULL && *pChainedPrevVer >= 0) {
              vDebug("vgId:%d, vacuum-rollback: chained CREATE→ALTER undo for uid %" PRId64, TD_VID(pVnode), uid);
            }
          }
          break;
        }
        case META_TXN_PRE_DROP:
          code = metaMarkTableTxnStatus(pVnode->pMeta, uid, 0, META_TXN_NORMAL, -1);
          break;
        case META_TXN_PRE_ALTER: {
          int64_t *pPrevVer = (int64_t *)tSimpleHashGet(pEntry->pAlterPrevVers, &uid, sizeof(tb_uid_t));
          int64_t  prevVer = (pPrevVer != NULL) ? *pPrevVer : -1;
          if (prevVer >= 0) {
            code = metaRollbackAlterTable(pVnode->pMeta, uid, prevVer);
          } else {
            code = metaMarkTableTxnStatus(pVnode->pMeta, uid, 0, META_TXN_NORMAL, -1);
          }
          break;
        }
        default:
          break;
      }
    }

    if (bulkYield) {
      // Quota hit during bulk child drop. Do NOT advance vacuumIdx; leave pME for
      // name lookup on the next tick (via the bulk-continuation block above).
      metaFetchEntryFree(&pME);
      break;
    }

    if (code != 0) {
      // Meta operation failed — do NOT delete txn.idx or advance vacuumIdx.
      // The entry stays so the next vacuum cycle can retry.
      vWarn("vgId:%d, vacuum: meta op failed for uid %" PRId64 ", code:0x%x, will retry next cycle", TD_VID(pVnode),
            uid, code);
      metaFetchEntryFree(&pME);
      break;
    }

    // Meta op succeeded — safe to remove from txn.idx now
    if (!skipTxnIdxDel) {
      int32_t vacIdxCode = metaTxnIdxDelete(pVnode->pMeta, uid);
      if (vacIdxCode != 0) {
        vWarn("vgId:%d, vacuum: failed to delete txn.idx for uid %" PRId64 ", code:0x%x", TD_VID(pVnode), uid,
              vacIdxCode);
      }
    }

    metaFetchEntryFree(&pME);
    pEntry->vacuumIdx++;
    processed++;
  }

  return processed;
}

/**
 * Process one batch of vacuum work across all finalized txns.
 * Called from the async vacuum task (vnode-scan thread pool) or inline.
 * Returns total UIDs processed in this batch.
 */
int32_t vnodeTxnVacuumBatch(SVnode *pVnode, int32_t maxOps) {
  if (pVnode->pTxnHash == NULL || pVnode->pTxnMetaCache == NULL) return 0;
  if (taosHashGetSize(pVnode->pTxnMetaCache) == 0) return 0;

  int32_t totalProcessed = 0;

  // Pre-allocate before entering the loop. Lazy allocation inside the loop causes a silent
  // stuck-entry bug: if taosArrayInit returns NULL on OOM mid-loop, a fully-vacuumed txn
  // is never added to the removal list, leaks in pTxnHash/pTxnMetaCache forever, and
  // causes vnodeTxnVacuumExecute to spin in infinite no-op retries.
  SArray *pCompletedTxns = taosArrayInit(4, sizeof(int64_t));
  if (pCompletedTxns == NULL) {
    vError("vgId:%d, out of memory allocating completed-txns array in vacuum batch", TD_VID(pVnode));
    return 0;
  }

  // Q-1 fix: snapshot finalized txnIds while holding the lock, then process each
  // by re-looking-up the entry under the lock-release/re-acquire boundary that
  // vnodeTxnVacuumOneTxn needs. Iterating with the iterator across an unlock
  // window would leave pEntry vulnerable to UAF if a future code path were to
  // delete entries from another thread.
  SArray *pPendingTxns = taosArrayInit(16, sizeof(int64_t));
  if (pPendingTxns == NULL) {
    vError("vgId:%d, out of memory allocating pending-txns array in vacuum batch", TD_VID(pVnode));
    taosArrayDestroy(pCompletedTxns);
    return 0;
  }

  (void)taosThreadMutexLock(&pVnode->txnMutex);
  {
    void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
    while (pIter != NULL) {
      SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;
      if (pEntry->status != TXN_META_NONE) {
        if (taosArrayPush(pPendingTxns, &pEntry->txnId) == NULL) {
          vWarn("vgId:%d, out of memory snapshotting txnId:%" PRId64 " in vacuum batch", TD_VID(pVnode), pEntry->txnId);
          // Continue: process whatever fit in the snapshot.
          taosHashCancelIterate(pVnode->pTxnHash, pIter);
          break;
        }
      }
      pIter = taosHashIterate(pVnode->pTxnHash, pIter);
    }
  }
  (void)taosThreadMutexUnlock(&pVnode->txnMutex);

  int32_t numPending = taosArrayGetSize(pPendingTxns);
  for (int32_t i = 0; i < numPending && totalProcessed < maxOps; i++) {
    int64_t txnId = *(int64_t *)taosArrayGet(pPendingTxns, i);

    // Re-resolve the entry under the lock; another thread (e.g. inline finalize)
    // could in principle have removed it. If gone, skip.
    (void)taosThreadMutexLock(&pVnode->txnMutex);
    SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)taosHashGet(pVnode->pTxnHash, &txnId, sizeof(int64_t));
    if (pEntry == NULL || pEntry->status == TXN_META_NONE) {
      (void)taosThreadMutexUnlock(&pVnode->txnMutex);
      continue;
    }
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);

    // vnodeTxnVacuumOneTxn currently mutates pEntry->vacuumIdx without holding
    // txnMutex. Safe today because only the vacuum thread touches FINALIZED
    // entries, but if this invariant changes the snapshot+re-lookup pattern
    // here will already be in place. See comment block above.
    int32_t processed = vnodeTxnVacuumOneTxn(pVnode, pEntry, maxOps - totalProcessed);
    totalProcessed += processed;

    // Re-check completion under the lock.
    (void)taosThreadMutexLock(&pVnode->txnMutex);
    pEntry = (SVnodeTxnEntry *)taosHashGet(pVnode->pTxnHash, &txnId, sizeof(int64_t));
    bool fullyVacuumed = (pEntry != NULL) && (pEntry->vacuumIdx >= pEntry->numVacuumUids);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);

    if (fullyVacuumed) {
      if (taosArrayPush(pCompletedTxns, &txnId) == NULL) {
        // OOM: txnId not recorded, entry will not be cleaned this round.
        // Next vacuum cycle will retry. Log so the condition is observable.
        vWarn("vgId:%d, out of memory recording completed txnId:%" PRId64 " in vacuum batch", TD_VID(pVnode), txnId);
      }
    }
  }
  taosArrayDestroy(pPendingTxns);

  // Remove fully-vacuumed txns (outside iteration)
  int32_t numCompleted = taosArrayGetSize(pCompletedTxns);
  for (int32_t i = 0; i < numCompleted; i++) {
    int64_t txnId = *(int64_t *)taosArrayGet(pCompletedTxns, i);

    // Remove from txn.meta
    int32_t delCode = metaTxnMetaDelete(pVnode->pMeta, txnId);
    if (delCode != 0) {
      // Persistent record survives; vacuum will retry on next restart (idempotent).
      vWarn("vgId:%d, failed to delete txn.meta for txnId:%" PRId64 ": %s", TD_VID(pVnode), txnId,
            tstrerror(delCode));
    }

    // Remove from in-memory cache
    int32_t rmCode = taosHashRemove(pVnode->pTxnMetaCache, &txnId, sizeof(int64_t));
    if (rmCode != 0) {
      // Stale key left in pTxnMetaCache; vacuum will be re-triggered unnecessarily.
      vWarn("vgId:%d, failed to remove txnId:%" PRId64 " from pTxnMetaCache: %s", TD_VID(pVnode), txnId,
            tstrerror(rmCode));
    } else {
      atomic_fetch_sub_32(&pVnode->txnPendingCount, 1);
    }

    // Remove SVnodeTxnEntry
    (void)taosThreadMutexLock(&pVnode->txnMutex);
    vnodeRemoveTxnEntry(pVnode, txnId);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);

    vInfo("vgId:%d, vacuum complete for txnId:%" PRId64, TD_VID(pVnode), txnId);
  }
  taosArrayDestroy(pCompletedTxns);

  // If any txns were fully vacuumed this round, the minimum beginIndex in the
  // txn WAL cache may have advanced — refresh the WAL keep-version so trim can proceed.
  if (numCompleted > 0 && pVnode->pTxnWalMgr != NULL) {
    txnMgrRefreshWalKeepVersion(pVnode->pTxnWalMgr, pVnode->pWal, pVnode);
  }

  if (totalProcessed > 0) {
    vDebug("vgId:%d, vacuum batch: processed %d UIDs", TD_VID(pVnode), totalProcessed);
  }

  return totalProcessed;
}

// ============================================================================
// Async Vacuum: submit vacuum work to SCAN_TASK_ASYNC thread pool
// ============================================================================
// Safety: pVnode lifetime is guaranteed by vnodeAWait(&pVnode->vacuumTask)
// in vnodeClose(), same pattern as commitTask / commitTask2.

/**
 * Async vacuum task executed on the vnode-scan thread pool.
 * Processes vacuum work in batches.  Between batches, checks whether other
 * scan tasks are queued — if so, re-submits itself (yielding the thread so
 * higher-priority scan tasks can run); otherwise continues in a tight loop
 * to avoid unnecessary re-enqueue overhead.
 */
static int32_t vnodeTxnVacuumExecute(void *arg) {
  SVnode *pVnode = (SVnode *)arg;
  int32_t totalProcessed = 0;

  while (pVnode->pTxnMetaCache && taosHashGetSize(pVnode->pTxnMetaCache) > 0) {
    if (atomic_load_8(&pVnode->closing)) {
      vDebug("vgId:%d, async vacuum interrupted by vnode close after %d UIDs", TD_VID(pVnode), totalProcessed);
      break;
    }

    int32_t processed = vnodeTxnVacuumBatch(pVnode, TSDB_TXN_VACUUM_BATCH_SIZE);
    totalProcessed += processed;
    if (processed == 0) break;  // No progress — all done

    // If other scan tasks are queued, yield the thread by re-submitting ourselves.
    // Skip re-submit if vnode is closing to prevent use-after-free race:
    // vnodeClose() calls vnodeAWait on the CURRENT vacuumTask; a re-submit
    // would overwrite vacuumTask with a new ID, causing vnodeClose to miss it.
    if (vnodeAsyncHasQueuedTask(SCAN_TASK_ASYNC) && !atomic_load_8(&pVnode->closing)) {
      vDebug("vgId:%d, async vacuum yielding after %d UIDs (scan tasks queued)", TD_VID(pVnode), totalProcessed);
      atomic_store_8(&pVnode->vacuumRunning, 0);
      vnodeTxnSubmitVacuumAsync(pVnode);
      return 0;
    }
  }

  if (totalProcessed > 0) {
    vDebug("vgId:%d, async vacuum done: processed %d UIDs total", TD_VID(pVnode), totalProcessed);
  }
  atomic_store_8(&pVnode->vacuumRunning, 0);
  if (totalProcessed > 0 && !atomic_load_8(&pVnode->closing) && pVnode->pTxnMetaCache &&
      taosHashGetSize(pVnode->pTxnMetaCache) > 0) {
    vnodeTxnSubmitVacuumAsync(pVnode);
  }
  return 0;
}

/**
 * Submit a vacuum task to the vnode-scan thread pool (non-blocking).
 * Uses EVA_PRIORITY_LOW to avoid competing with normal scan tasks.
 * Task ID stored in pVnode->vacuumTask; vnodeClose() calls vnodeAWait()
 * on it to prevent use-after-free.
 *
 * Safe to call multiple times: vacuumRunning ensures at most one vacuum task is
 * queued/running for a VNode, so vacuumIdx and txn.idx cleanup are serialized.
 */
static void vnodeTxnSubmitVacuumAsync(SVnode *pVnode) {
  if (atomic_load_8(&pVnode->closing)) {
    return;
  }

  if (atomic_val_compare_exchange_8(&pVnode->vacuumRunning, 0, 1) != 0) {
    return;
  }

  int32_t code =
      vnodeAsync(SCAN_TASK_ASYNC, EVA_PRIORITY_LOW, vnodeTxnVacuumExecute, NULL, pVnode, &pVnode->vacuumTask);
  if (code != 0) {
    atomic_store_8(&pVnode->vacuumRunning, 0);
    vError("vgId:%d, failed to submit async vacuum task, code:0x%x", TD_VID(pVnode), code);
    return;
  }

  // TOCTOU guard: vnodeClose may have set closing=1 and even completed its
  // vnodeAWait(&vacuumTask) between our initial closing-check and vnodeAsync
  // populating vacuumTask. In that case the close path will not wait for the
  // freshly-submitted task, leading to use-after-free of pVnode resources.
  // Re-check closing AFTER vnodeAsync has filled in vacuumTask; if a close
  // raced in, await our own submission here so vnodeClose's subsequent state
  // teardown is safe even if it already returned from its own vnodeAWait.
  if (atomic_load_8(&pVnode->closing)) {
    vnodeAWait(&pVnode->vacuumTask);
  }
}

// ============================================================================
// VNode Transaction Message Handlers
// ============================================================================

/**
 * Process COMMIT request from MNode (TDMT_VND_TXN_COMMIT)
 * This finalizes the transaction and makes changes visible.
 *
 * Shadow-in-Snapshot model: shadow ops are persisted via VNode snapshot.
 * Follower reconstructs shadow from WAL replay (normal) or snapshot load (catchup).
 * If shadow is missing and no snapshot source, the txn is treated as empty for this VGroup.
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

  // Fencing: if term advanced, abort old-term transactions first
  if ((code = vnodeTxnFencing(pVnode, req.term, req.txnId))) {
    vError("vgId:%d, fencing error on commit, txnId:%" PRId64 " since %s", TD_VID(pVnode), req.txnId, tstrerror(code));
    return code;
  }

  (void)taosThreadMutexLock(&pVnode->txnMutex);

  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, req.txnId);
  if (pEntry == NULL) {
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    // Shadow missing — empty txn on this VGroup (no DDL was routed here)
    vDebug("vgId:%d, txn entry not found on commit (no-op), txnId:%" PRId64, TD_VID(pVnode), req.txnId);
    return TSDB_CODE_SUCCESS;
  }

  // Idempotency: if already finalized (finalStatus set on successful vnodeTxnFinalizeLazy or
  // inline promote+remove), return success. Do NOT check stage==FINISHING here — that only
  // means we started finalization, not that it completed. A failed finalize must allow retries.
  if (pEntry->status != TXN_META_NONE) {
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    vInfo("vgId:%d, txn commit idempotent (already finalized), txnId:%" PRId64, TD_VID(pVnode), req.txnId);
    return TSDB_CODE_SUCCESS;
  }

  // Lazy term correction: entry was created with maxSeenTerm which may have been 0
  if (pEntry->term == 0 && req.term > 0) {
    pEntry->term = req.term;
  }
  pEntry->stage = VTXN_STAGE_FINISHING;

  int32_t numUids = pEntry->pTouchedUids ? tSimpleHashGetSize(pEntry->pTouchedUids) : 0;
  // hasBulkDrop: this txn dropped an STB that had child tables. The cascade
  // (metaHandleEntry2 deletes all children) happens inside metaDropSuperTable
  // at COMMIT time and may be very heavy (millions of children). Force the
  // lazy path so it runs in the background vacuum thread.
  bool forceVacuum = pEntry->hasBulkDrop;

  if (numUids <= TSDB_TXN_INLINE_THRESHOLD && !forceVacuum) {
    // ── Small txn: synchronous inline promote ──
    // O(k) B+ tree ops where k ≤ 128, typically < 1ms.
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);

    code = vnodeTxnPromoteShadowEntries(pVnode, pEntry);
    if (code != 0) {
      vWarn("vgId:%d, inline commit partial failure for txnId:%" PRId64 ", code:0x%x (continuing)", TD_VID(pVnode),
            req.txnId, code);

      // Preserve the txn entry via lazy finalize so async vacuum can retry any
      // remaining PRE_* entries. Removing the entry here would turn subsequent
      // COMMIT retries into no-ops while txn.idx still contains pending UIDs.
      (void)taosThreadMutexLock(&pVnode->txnMutex);
      int32_t finalizeCode = vnodeTxnFinalizeLazy(pVnode, pEntry, TXN_META_COMMITTED);
      (void)taosThreadMutexUnlock(&pVnode->txnMutex);
      if (finalizeCode != 0) {
        vError("vgId:%d, failed to fallback finalize commit for txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode),
               req.txnId, finalizeCode);
        return finalizeCode;
      }

      vnodeTxnNotifyTmq(pVnode, pEntry);
      vnodeTxnSubmitVacuumAsync(pVnode);
      return code;
    }

    // Notify TMQ about tables created/dropped in this txn (before entry removal)
    vnodeTxnNotifyTmq(pVnode, pEntry);

    (void)taosThreadMutexLock(&pVnode->txnMutex);
    vnodeRemoveTxnEntry(pVnode, req.txnId);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    // Inline path: entry removed, refresh WAL keep-version so min beginWalIndex can advance.
    if (pVnode->pTxnWalMgr != NULL) txnMgrRefreshWalKeepVersion(pVnode->pTxnWalMgr, pVnode->pWal, pVnode);

    vInfo("vgId:%d, txn commit done (inline), txnId:%" PRId64 ", numUids:%d", TD_VID(pVnode), req.txnId, numUids);
  } else {
    // ── Large txn: lazy finalize O(1) + async vacuum ──
    code = vnodeTxnFinalizeLazy(pVnode, pEntry, TXN_META_COMMITTED);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);

    if (code != 0) {
      vError("vgId:%d, failed to finalize commit for txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode), req.txnId, code);
      return code;
    }

    vInfo("vgId:%d, txn commit finalized (lazy), txnId:%" PRId64 ", numUids:%d", TD_VID(pVnode), req.txnId, numUids);

    // Notify TMQ now — commit decision is final, even though vacuum hasn't run yet.
    // TMQ needs UIDs ASAP so consumers can discover the new/dropped tables.
    vnodeTxnNotifyTmq(pVnode, pEntry);

    // Submit vacuum to vnode-scan thread pool (non-blocking)
    vnodeTxnSubmitVacuumAsync(pVnode);
  }

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

  // Fencing: if term advanced, abort old-term transactions first
  if ((code = vnodeTxnFencing(pVnode, req.term, req.txnId))) {
    vError("vgId:%d, fencing error on rollback, txnId:%" PRId64 ", since %s", TD_VID(pVnode), req.txnId,
           tstrerror(code));
    return code;
  }

  (void)taosThreadMutexLock(&pVnode->txnMutex);

  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, req.txnId);
  if (pEntry == NULL) {
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    // Idempotent: already rolled back or never existed
    vWarn("vgId:%d, txn not found for rollback (idempotent), txnId:%" PRId64, TD_VID(pVnode), req.txnId);
    return TSDB_CODE_SUCCESS;
  }

  // Idempotency: if already finalized, return success (WAL replay / MNode retry).
  // Do NOT check stage==FINISHING — a failed finalize must allow retries.
  if (pEntry->status != TXN_META_NONE) {
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    vInfo("vgId:%d, txn rollback idempotent (already finalized), txnId:%" PRId64, TD_VID(pVnode), req.txnId);
    return TSDB_CODE_SUCCESS;
  }

  // Lazy term correction: entry was created with maxSeenTerm which may have been 0
  if (pEntry->term == 0 && req.term > 0) {
    pEntry->term = req.term;
  }
  pEntry->stage = VTXN_STAGE_FINISHING;

  int32_t numUids = pEntry->pTouchedUids ? tSimpleHashGetSize(pEntry->pTouchedUids) : 0;
  bool    hasAlterRollback = pEntry->pAlterPrevVers && tSimpleHashGetSize(pEntry->pAlterPrevVers) > 0;
  bool    forceVacuum = pEntry->hasBulkDrop;

  if ((numUids <= TSDB_TXN_INLINE_THRESHOLD && !forceVacuum) || hasAlterRollback) {
    // ── Small txn: synchronous inline undo ──
    // Note: hasBulkDrop means children were NOT marked PRE_DROP (no per-child
    // undo needed), but we still use the lazy path for consistency with COMMIT.
    // PRE_ALTER rollback is kept on the write thread because metaRollbackAlterTable
    // is not safe to run from async vacuum today.
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);

    code = vnodeTxnUndoShadowEntries(pVnode, pEntry);
    if (code != 0) {
      vWarn("vgId:%d, inline rollback partial failure for txnId:%" PRId64 ", code:0x%x (continuing)", TD_VID(pVnode),
            req.txnId, code);

      if (hasAlterRollback) {
        (void)taosThreadMutexLock(&pVnode->txnMutex);
        if (pEntry->status == TXN_META_NONE) {
          pEntry->stage = VTXN_STAGE_ACTIVE;
        }
        (void)taosThreadMutexUnlock(&pVnode->txnMutex);
        return code;
      }

      (void)taosThreadMutexLock(&pVnode->txnMutex);
      int32_t finalizeCode = vnodeTxnFinalizeLazy(pVnode, pEntry, TXN_META_ROLLEDBACK);
      (void)taosThreadMutexUnlock(&pVnode->txnMutex);
      if (finalizeCode != 0) {
        vError("vgId:%d, failed to fallback finalize rollback for txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode),
               req.txnId, finalizeCode);
        return finalizeCode;
      }

      vnodeTxnSubmitVacuumAsync(pVnode);
      return code;
    }

    (void)taosThreadMutexLock(&pVnode->txnMutex);
    vnodeRemoveTxnEntry(pVnode, req.txnId);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    // Inline path: entry removed, refresh WAL keep-version so min beginWalIndex can advance.
    if (pVnode->pTxnWalMgr != NULL) txnMgrRefreshWalKeepVersion(pVnode->pTxnWalMgr, pVnode->pWal, pVnode);

    vInfo("vgId:%d, txn rollback done (inline), txnId:%" PRId64 ", numUids:%d", TD_VID(pVnode), req.txnId, numUids);
  } else {
    // ── Large txn: lazy finalize O(1) + async vacuum ──
    code = vnodeTxnFinalizeLazy(pVnode, pEntry, TXN_META_ROLLEDBACK);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);

    if (code != 0) {
      vError("vgId:%d, failed to finalize rollback for txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode), req.txnId, code);
      return code;
    }

    vInfo("vgId:%d, txn rollback finalized (lazy), txnId:%" PRId64 ", numUids:%d", TD_VID(pVnode), req.txnId, numUids);

    // Submit vacuum to vnode-scan thread pool (non-blocking)
    vnodeTxnSubmitVacuumAsync(pVnode);
  }

  return code;
}

// ============================================================================
// Fencing (Lock Preemption) Logic
// ============================================================================

/**
 * Preempt locks held by lower-term transactions.
 * Called from Raft-replicated COMMIT/ROLLBACK handlers, so all replicas
 * execute identical fencing deterministically — no Raft bypass issue.
 */
int32_t vnodeTxnFencing(SVnode *pVnode, int64_t newTerm, int64_t newTxnId) {
  int32_t code = TSDB_CODE_SUCCESS;

  (void)taosThreadMutexLock(&pVnode->txnMutex);

  if (newTerm < pVnode->maxSeenTerm) {
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    return TSDB_CODE_TXN_STALE_TERM;
  }

  if (newTerm == pVnode->maxSeenTerm) {
    // Same term — no fencing needed (common case: same MNode leader)
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    return TSDB_CODE_SUCCESS;
  }

  // newTerm > maxSeenTerm: term advanced, do fencing
  pVnode->maxSeenTerm = newTerm;

  SArray *toAbort = taosArrayInit(8, sizeof(int64_t));
  if (toAbort == NULL) {
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    return terrno;
  }

  void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
  while (pIter) {
    SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;

    // Skip entries with term=0 (unknown term — created before any COMMIT/ROLLBACK arrived,
    // or rebuilt after restart). They'll be cleaned up by their own explicit COMMIT/ROLLBACK.
    // Also skip entries already in FINISHING state or finalized (being vacuum'd or already done).
    if (pEntry->term > 0 && pEntry->term < newTerm && pEntry->txnId != newTxnId &&
        pEntry->stage != VTXN_STAGE_FINISHING && pEntry->status == TXN_META_NONE) {
      vInfo("vgId:%d, fencing: abort txn, txnId:%" PRId64 ", term:%" PRId64 ", newTerm:%" PRId64, TD_VID(pVnode),
            pEntry->txnId, pEntry->term, newTerm);
      if (taosArrayPush(toAbort, &pEntry->txnId) == NULL) {
        vError("vgId:%d, fencing: failed to push txnId:%" PRId64 " to abort list", TD_VID(pVnode), pEntry->txnId);
        taosHashCancelIterate(pVnode->pTxnHash, pIter);
        (void)taosThreadMutexUnlock(&pVnode->txnMutex);
        taosArrayDestroy(toAbort);
        return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
      }
    }

    pIter = taosHashIterate(pVnode->pTxnHash, pIter);
  }

  int32_t numToAbort = taosArrayGetSize(toAbort);
  for (int32_t i = 0; i < numToAbort; i++) {
    int64_t         txnId = *(int64_t *)taosArrayGet(toAbort, i);
    SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
    if (pEntry) {
      pEntry->stage = VTXN_STAGE_FINISHING;
      (void)taosThreadMutexUnlock(&pVnode->txnMutex);

      // Undo shadow entries in B+ tree before removing
      int32_t undoCode = vnodeTxnUndoShadowEntries(pVnode, pEntry);

      (void)taosThreadMutexLock(&pVnode->txnMutex);
      vnodeRemoveTxnEntry(pVnode, txnId);
      if (undoCode != 0) {
        vError("vgId:%d, fencing: failed to abort txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode), txnId, undoCode);
        (void)taosThreadMutexUnlock(&pVnode->txnMutex);
        taosArrayDestroy(toAbort);
        return undoCode;
      }
    }
  }

  (void)taosThreadMutexUnlock(&pVnode->txnMutex);
  taosArrayDestroy(toAbort);

  vInfo("vgId:%d, fencing completed, aborted %d transactions", TD_VID(pVnode), numToAbort);
  return code;
}

// ============================================================================
// StatusReq Keepalive Support
// ============================================================================

/**
 * Collect stale non-replicated transactions for MNode liveness check.
 * Called by DNode when building statusReq (hourly, defense-in-depth).
 * Only txns idle for > 30 minutes are reported: recently-active txns are
 * obviously still alive (MNode's 30s inactivity timeout guarantees that),
 * so querying MNode about them would be wasted work.
 * MNode checks mndTxnIsAlive for each entry; orphaned txns get a Raft-safe
 * ROLLBACK via mndRollbackOrphanTxnOnVnode.  Replicated (taosX) txns are
 * excluded — their lifecycle is governed by MNode's lifetime check.
 */
int32_t vnodeCollectIdleTxns(SVnode *pVnode, SArray **ppQueries) {
  if (pVnode->pTxnHash == NULL || taosHashGetSize(pVnode->pTxnHash) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  // Only the sync leader reports: all replicas share the same B+tree state and
  // would reconstruct identical pTxnHash entries after restart, so followers
  // would produce duplicate {txnId, vgId} reports and trigger redundant STrans.
  if (!vnodeIsLeader(pVnode)) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t now = taosGetTimestampMs();
  (void)taosThreadMutexLock(&pVnode->txnMutex);
  void *pIter = NULL;
  while ((pIter = taosHashIterate(pVnode->pTxnHash, pIter))) {
    SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;
    if (pEntry->stage == VTXN_STAGE_ACTIVE && (now - pEntry->startTime > TSDB_ORPHAN_TXN_SCAN_MS)) {
      STxnActiveQuery q = {.txnId = pEntry->txnId, .vgId = TD_VID(pVnode)};
      if (!(*ppQueries) && !(*ppQueries = taosArrayInit(32, sizeof(STxnActiveQuery)))) {
        vWarn("vgId:%d, failed to init to collect keepalive query for txnId:%" PRId64, TD_VID(pVnode), pEntry->txnId);
        taosHashCancelIterate(pVnode->pTxnHash, pIter);
        (void)taosThreadMutexUnlock(&pVnode->txnMutex);
        return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
      }
      if (taosArrayPush(*ppQueries, &q) == NULL) {
        vWarn("vgId:%d, failed to push keepalive query for txnId:%" PRId64, TD_VID(pVnode), pEntry->txnId);
        taosHashCancelIterate(pVnode->pTxnHash, pIter);
        (void)taosThreadMutexUnlock(&pVnode->txnMutex);
        return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
      }
    }
  }
  (void)taosThreadMutexUnlock(&pVnode->txnMutex);
  return TSDB_CODE_SUCCESS;
}

// ============================================================================
// Table-Level Lock Conflict Detection
// ============================================================================

/**
 * Acquire a table-level lock for a transaction.
 * If the table is already locked by the same txnId, returns SUCCESS (idempotent).
 * If locked by a different txnId, returns TSDB_CODE_TXN_RESOURCE_BUSY.
 *
 * @param pVnode    The vnode
 * @param tableName The fully qualified table name
 * @param txnId     The transaction ID requesting the lock
 * @return TSDB_CODE_SUCCESS or TSDB_CODE_TXN_RESOURCE_BUSY
 */
int32_t vnodeTxnLockTable(SVnode *pVnode, const char *tableName, int64_t txnId) {
  if (pVnode->pTxnTableLock == NULL || tableName == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t nameLen = strlen(tableName);

  (void)taosThreadMutexLock(&pVnode->txnMutex);

  // Check if the table is already locked
  int64_t *pExistingTxnId = (int64_t *)taosHashGet(pVnode->pTxnTableLock, tableName, nameLen);
  if (pExistingTxnId != NULL) {
    if (*pExistingTxnId == txnId) {
      // Same transaction already holds the lock — idempotent
      (void)taosThreadMutexUnlock(&pVnode->txnMutex);
      return TSDB_CODE_SUCCESS;
    }
    // Different transaction holds the lock — conflict
    vWarn("vgId:%d, table lock conflict, table:%s, existingTxn:%" PRId64 ", requestTxn:%" PRId64, TD_VID(pVnode),
          tableName, *pExistingTxnId, txnId);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    return TSDB_CODE_TXN_RESOURCE_BUSY;
  }

  // Verify the requesting transaction exists
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry == NULL) {
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    vWarn("vgId:%d, cannot lock table, txn not found, table:%s, txnId:%" PRId64, TD_VID(pVnode), tableName, txnId);
    return TSDB_CODE_TXN_NOT_EXIST;
  }

  // Acquire the lock: add tableName → txnId mapping
  int32_t putCode = taosHashPut(pVnode->pTxnTableLock, tableName, nameLen, &txnId, sizeof(int64_t));
  if (putCode != 0) {
    vError("vgId:%d, failed to put table lock, table:%s, txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode), tableName,
           txnId, putCode);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    return putCode;
  }

  // Record the table name in the txn entry for reverse cleanup
  char *nameCopy = taosStrdup(tableName);
  if (nameCopy == NULL) {
    vError("vgId:%d, failed to allocate locked table name:%s, txnId:%" PRId64, TD_VID(pVnode), tableName, txnId);
    if (taosHashRemove(pVnode->pTxnTableLock, tableName, nameLen) != 0) {
      vWarn("vgId:%d, txn: failed to release table lock for:%s on alloc failure", TD_VID(pVnode), tableName);
    }
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (taosArrayPush(pEntry->pLockedTables, &nameCopy) == NULL) {
    vError("vgId:%d, failed to track locked table:%s, txnId:%" PRId64, TD_VID(pVnode), tableName, txnId);
    taosMemoryFree(nameCopy);
    if (taosHashRemove(pVnode->pTxnTableLock, tableName, nameLen) != 0) {
      vWarn("vgId:%d, txn: failed to release table lock for:%s on push failure", TD_VID(pVnode), tableName);
    }
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  (void)taosThreadMutexUnlock(&pVnode->txnMutex);

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

  (void)taosThreadMutexLock(&pVnode->txnMutex);

  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry != NULL) {
    vnodeReleaseTxnTableLocks(pVnode, pEntry);
  }

  (void)taosThreadMutexUnlock(&pVnode->txnMutex);
}

// ============================================================================
// Shadow-in-B+tree: Conflict Detection via B+ tree reads
// ============================================================================

/**
 * Check if a non-transaction DDL/DML operation conflicts with any active txn shadow
 * in the B+ tree. Reads the table's txnStatus directly from meta.
 *
 * Conflict matrix (from design doc §16):
 *   PRE_CREATE + non-txn CREATE → CONFLICT
 *   PRE_CREATE + non-txn SELECT/INSERT/DELETE/ALTER/DROP → TABLE_NOT_EXIST (shadow invisible)
 *   PRE_DROP   + non-txn DROP/ALTER/DELETE → CONFLICT (resource busy)
 *   PRE_DROP   + non-txn CREATE → TABLE_ALREADY_EXISTS
 *   PRE_DROP   + non-txn SELECT/INSERT → OK (read old data)
 *   PRE_ALTER  + non-txn ALTER/DROP → CONFLICT
 *   PRE_ALTER  + non-txn SELECT/INSERT/DELETE → OK (use old schema)
 *
 * @param pVnode      The vnode
 * @param tableName   The target table name
 * @param incomingOp  0=query/DML, 1=CREATE, 2=ALTER, 3=DROP
 * @return TSDB_CODE_SUCCESS if no conflict, error code otherwise
 */
int32_t vnodeTxnCheckConflict(SVnode *pVnode, const char *tableName, int8_t incomingOp) {
  if (pVnode->pTxnHash == NULL || tableName == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  // Fast path: no active or finalized txns → no conflict possible
  if (!metaHasPendingTxnEntries(pVnode->pMeta)) {
    return TSDB_CODE_SUCCESS;
  }

  // Read the table entry from B+ tree to check txnStatus.
  // metaRLock: async vacuum thread may concurrently hold metaWLock and mutate
  // pNameIdx/pTbDb pages — unlocked tdb reads here race the B+ tree.
  SMetaEntry *pME = NULL;
  metaRLock(pVnode->pMeta);
  int32_t code = metaFetchEntryByName(pVnode->pMeta, tableName, &pME);
  metaULock(pVnode->pMeta);
  if (code != 0 || pME == NULL) {
    // Table not found in meta — no conflict possible
    return TSDB_CODE_SUCCESS;
  }

  int32_t ret = TSDB_CODE_SUCCESS;
  if (pME->txnId != 0) {
    // Check if the owning txn is finalized → no conflict (vacuum will clean up)
    int8_t finalStatus = metaGetTxnMetaStatus(pVnode->pMeta, pME->txnId);
    if (finalStatus == TXN_META_COMMITTED || finalStatus == TXN_META_ROLLEDBACK) {
      // For COMMITTED PRE_CREATE: table exists, CREATE should fail (TABLE_ALREADY_EXIST)
      // For ROLLEDBACK PRE_CREATE: table doesn't exist, CREATE should succeed (no conflict)
      // For COMMITTED PRE_DROP: table is gone, handled elsewhere
      // For ROLLEDBACK PRE_DROP: table restored, no conflict with new ops
      if (finalStatus == TXN_META_COMMITTED && pME->txnStatus == META_TXN_PRE_CREATE && incomingOp == 1) {
        ret = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
      }
      metaFetchEntryFree(&pME);
      return ret;
    }

    switch (pME->txnStatus) {
      case META_TXN_PRE_CREATE:
        if (incomingOp == 1) {  // CREATE vs PRE_CREATE
          ret = TSDB_CODE_TXN_RESOURCE_BUSY;
        }
        // Other ops: table is invisible to non-txn → will naturally fail as "not exist"
        break;

      case META_TXN_PRE_DROP:
        if (incomingOp == 3 || incomingOp == 2) {  // DROP/ALTER vs PRE_DROP
          ret = TSDB_CODE_TXN_RESOURCE_BUSY;
        } else if (incomingOp == 1) {  // CREATE vs PRE_DROP
          ret = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
        }
        // SELECT/INSERT (incomingOp=0): allowed, no conflict
        break;

      case META_TXN_PRE_ALTER:
        if (incomingOp == 2 || incomingOp == 3) {  // ALTER/DROP vs PRE_ALTER
          ret = TSDB_CODE_TXN_RESOURCE_BUSY;
        }
        break;

      default:
        break;
    }

    if (ret != TSDB_CODE_SUCCESS) {
      vWarn("vgId:%d, txn conflict: table=%s, txnStatus=%d, incomingOp=%d, txnId:%" PRId64, TD_VID(pVnode), tableName,
            pME->txnStatus, incomingOp, pME->txnId);
    }
  }

  metaFetchEntryFree(&pME);
  return ret;
}

/**
 * Check if a DELETE DML on a specific UID conflicts with any active txn shadow
 * in the B+ tree. If the table is in PRE_DROP state, DELETE should be blocked.
 *
 * @param pVnode  The vnode
 * @param uid     The table UID being deleted
 * @return TSDB_CODE_SUCCESS if no conflict, TSDB_CODE_TXN_RESOURCE_BUSY if blocked
 */
int32_t vnodeTxnCheckDeleteConflict(SVnode *pVnode, tb_uid_t uid) {
  if (pVnode->pTxnHash == NULL || uid == 0) {
    return TSDB_CODE_SUCCESS;
  }

  // Fast path: no active or finalized txns → no conflict possible
  if (!metaHasPendingTxnEntries(pVnode->pMeta)) {
    return TSDB_CODE_SUCCESS;
  }

  SMetaEntry *pME = NULL;
  // metaRLock: async vacuum thread may concurrently hold metaWLock and mutate
  // pUidIdx/pTbDb pages — unlocked tdb reads here race the B+ tree.
  metaRLock(pVnode->pMeta);
  int32_t code = metaFetchEntryByUid(pVnode->pMeta, uid, &pME);
  metaULock(pVnode->pMeta);
  if (code != 0 || pME == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t ret = TSDB_CODE_SUCCESS;
  if (pME->txnId != 0 && pME->txnStatus == META_TXN_PRE_DROP) {
    // Check if finalized → no conflict
    int8_t finalStatus = metaGetTxnMetaStatus(pVnode->pMeta, pME->txnId);
    if (finalStatus == TXN_META_NONE) {
      ret = TSDB_CODE_TXN_RESOURCE_BUSY;
      vWarn("vgId:%d, DELETE conflict: uid=%" PRId64 " is in PRE_DROP, txnId:%" PRId64, TD_VID(pVnode), uid,
            pME->txnId);
    }
    // COMMITTED PRE_DROP: table is logically gone → DELETE on non-existent is harmless
    // ROLLEDBACK PRE_DROP: table is restored → DELETE is allowed
  }

  metaFetchEntryFree(&pME);
  return ret;
}

// End of vnodeTxn.c
