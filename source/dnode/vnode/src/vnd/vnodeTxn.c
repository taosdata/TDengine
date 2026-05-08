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
  int64_t    lastActive;      // Last active time
  int8_t     stage;           // EVtxnStage
  SSHashObj *pTouchedUids;    // SSHashObj: key=tb_uid_t, value=int8_t(dummy) — O(1) dedup
  SSHashObj *pAlterPrevVers;  // SSHashObj: key=tb_uid_t, value=int64_t(prevVersion) — O(1) lookup
  SArray    *pLockedTables;   // Array of char* (table names locked by this txn)
  // Lazy vacuum fields (populated at finalization, consumed by vacuum)
  int8_t    finalStatus;    // ETxnFinalStatus: TXN_FINAL_COMMITTED / TXN_FINAL_ROLLEDBACK
  tb_uid_t *pVacuumUids;    // Array of UIDs to vacuum (converted from pTouchedUids)
  int32_t   numVacuumUids;  // Total UIDs in vacuum array
  int32_t   vacuumIdx;      // Next UID index to process
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
  pVnode->pFinalizedTxns = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_ENTRY_LOCK);
  if (pVnode->pFinalizedTxns == NULL) {
    vError("vgId:%d, failed to init finalized txns hash", TD_VID(pVnode));
    taosHashCleanup(pVnode->pTxnTableLock);
    pVnode->pTxnTableLock = NULL;
    taosHashCleanup(pVnode->pTxnHash);
    pVnode->pTxnHash = NULL;
    return terrno;
  }

  if (taosThreadMutexInit(&pVnode->txnMutex, NULL) != 0) {
    taosHashCleanup(pVnode->pFinalizedTxns);
    pVnode->pFinalizedTxns = NULL;
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

  if (pVnode->pFinalizedTxns) {
    taosHashCleanup(pVnode->pFinalizedTxns);
    pVnode->pFinalizedTxns = NULL;
  }

  (void)taosThreadMutexDestroy(&pVnode->txnMutex);
  vInfo("vgId:%d, txn manager cleaned up", TD_VID(pVnode));
}

// ============================================================================
// Rebuild in-memory txn state from B+ tree (VNode startup / snapshot recovery)
// ============================================================================

// Forward declarations for static helpers used by vnodeTxnRebuildFromMeta
static SVnodeTxnEntry *vnodeGetTxnEntry(SVnode *pVnode, int64_t txnId);
static int32_t         vnodeCreateTxnEntry(SVnode *pVnode, int64_t txnId, int64_t term);
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
      code = vnodeCreateTxnEntry(pVnode, pScan->txnId, 0 /* term unknown after restart */);
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
    }

    // Track this UID
    code = vnodeTxnTrackUid(pEntry, pScan->uid);
    if (code != 0) {
      vError("vgId:%d, txn rebuild: failed to track uid:%" PRId64 ", txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode),
             pScan->uid, pScan->txnId, code);
      break;
    }

    // If PRE_ALTER, also reconstruct the ALTER old version record
    if (pScan->txnStatus == META_TXN_PRE_ALTER && pScan->txnPrevVer >= 0) {
      int32_t putCode =
          tSimpleHashPut(pEntry->pAlterPrevVers, &pScan->uid, sizeof(tb_uid_t), &pScan->txnPrevVer, sizeof(int64_t));
      if (putCode != 0) {
        vError("vgId:%d, txn rebuild: failed to put alter record for uid:%" PRId64, TD_VID(pVnode), pScan->uid);
        code = putCode;
        break;
      }
    }

    vDebug("vgId:%d, txn rebuild: uid:%" PRId64 " txnId:%" PRId64 " status:%d oldVer:%" PRId64, TD_VID(pVnode),
           pScan->uid, pScan->txnId, pScan->txnStatus, pScan->txnPrevVer);
  }

  taosArrayDestroy(pScanResult);

  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  // Log summary of active txns
  int32_t numTxns = taosHashGetSize(pVnode->pTxnHash);
  vInfo("vgId:%d, txn rebuild phase 1: %d unique txns, %d total entries from txn.idx", TD_VID(pVnode), numTxns,
        numEntries);

  // === Phase 2: Rebuild finalized txn cache from txn_final.idx ===
  // After a crash, some txns may have been finalized (COMMITTED/ROLLEDBACK) but
  // not fully vacuumed. Scan txn_final.idx and restore the in-memory cache so
  // visibility filters and vacuum can resume.
  SArray *pFinalResult = NULL;
  code = metaScanTxnFinalEntries(pVnode->pMeta, &pFinalResult);
  if (code != 0) {
    vError("vgId:%d, failed to scan txn_final.idx, code:0x%x", TD_VID(pVnode), code);
    return code;
  }

  int32_t numFinal = taosArrayGetSize(pFinalResult);
  int32_t numResumed = 0;
  int32_t numStale = 0;

  for (int32_t i = 0; i < numFinal; i++) {
    // Each entry is { int64_t txnId; STxnFinalVal val; }
    const void         *pElem = taosArrayGet(pFinalResult, i);
    int64_t             txnId = *(int64_t *)pElem;
    const STxnFinalVal *pFinalVal = (const STxnFinalVal *)((const char *)pElem + sizeof(int64_t));

    // Always populate the in-memory cache so visibility filters work immediately.
    // On failure (OOM during recovery), persistent txn_final.idx remains authoritative;
    // log a warning so the rare condition is visible.
    int32_t putCode = taosHashPut(pVnode->pFinalizedTxns, &txnId, sizeof(int64_t), &pFinalVal->finalStatus, sizeof(int8_t));
    if (putCode != 0) {
      vWarn("vgId:%d, txn rebuild: failed to cache finalized txn:%" PRId64 " status:%d, code:0x%x",
            TD_VID(pVnode), txnId, pFinalVal->finalStatus, putCode);
    }

    // Check if there are corresponding txn.idx entries (UIDs still needing vacuum)
    SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
    if (pEntry != NULL) {
      // This txn has un-vacuumed UIDs — prepare for vacuum resumption
      pEntry->finalStatus = pFinalVal->finalStatus;
      pEntry->stage = VTXN_STAGE_FINISHING;
      int32_t vacCode = vnodeTxnPrepareVacuumArray(pEntry);
      if (vacCode != 0) {
        vError("vgId:%d, txn rebuild: failed to prepare vacuum for txnId:%" PRId64, TD_VID(pVnode), txnId);
      } else {
        numResumed++;
        vInfo("vgId:%d, txn rebuild: resume vacuum for txnId:%" PRId64 " status:%d numUids:%d", TD_VID(pVnode), txnId,
              pFinalVal->finalStatus, pEntry->numVacuumUids);
      }
    } else {
      // No txn.idx entries remain — vacuum was complete, but txn_final.idx entry is stale.
      // Clean it up (delete from persistent idx; cache entry is harmless and will be ignored).
      (void)metaTxnFinalIdxDelete(pVnode->pMeta, txnId);
      (void)taosHashRemove(pVnode->pFinalizedTxns, &txnId, sizeof(int64_t));
      numStale++;
      vDebug("vgId:%d, txn rebuild: removed stale txn_final.idx entry for txnId:%" PRId64, TD_VID(pVnode), txnId);
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
        taosHashGetSize(pVnode->pTxnHash), taosHashGetSize(pVnode->pFinalizedTxns));
  return TSDB_CODE_SUCCESS;
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

  return TSDB_CODE_SUCCESS;
}

// Release all table locks held by a transaction entry (caller must hold txnMutex)
static void vnodeReleaseTxnTableLocks(SVnode *pVnode, SVnodeTxnEntry *pEntry) {
  if (pEntry->pLockedTables == NULL) return;
  int32_t sz = taosArrayGetSize(pEntry->pLockedTables);
  for (int32_t i = 0; i < sz; i++) {
    char *name = *(char **)taosArrayGet(pEntry->pLockedTables, i);
    if (name) {
      (void)taosHashRemove(pVnode->pTxnTableLock, name, strlen(name));
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
    tSimpleHashCleanup(pEntry->pTouchedUids);
    tSimpleHashCleanup(pEntry->pAlterPrevVers);
    taosMemoryFreeClear(pEntry->pVacuumUids);
    (void)taosHashRemove(pVnode->pTxnHash, &txnId, sizeof(int64_t));
  }
}

// ============================================================================
// Shadow-in-B+tree: Entry Management & ALTER Tracking
// ============================================================================

/**
 * Ensure a txn entry exists for the given txnId (lazy create).
 * Called by DDL handlers in vnodeSvr.c before writing to meta.
 */
int32_t vnodeTxnEnsureEntry(SVnode *pVnode, int64_t txnId) {
  if (pVnode->pTxnHash == NULL || txnId == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pVnode->txnMutex);

  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry == NULL) {
    code = vnodeCreateTxnEntry(pVnode, txnId, pVnode->maxSeenTerm);
    if (code == 0) {
      vInfo("vgId:%d, txn entry lazily created, txnId:%" PRId64, TD_VID(pVnode), txnId);
    }
  } else {
    pEntry->lastActive = taosGetTimestampMs();
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
int32_t vnodeTxnTrackTable(SVnode *pVnode, int64_t txnId, tb_uid_t uid) {
  if (pVnode->pTxnHash == NULL || txnId == 0) return TSDB_CODE_SUCCESS;

  int32_t code = TSDB_CODE_SUCCESS;
  (void)taosThreadMutexLock(&pVnode->txnMutex);
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry) {
    // DDL count limit per VNode (skip for replicated txns — taosX WAL replay)
    if (!TXN_IS_REPLICATED(txnId) && pEntry->pTouchedUids &&
        tSimpleHashGetSize(pEntry->pTouchedUids) >= TSDB_META_TXN_MAX_DDL_OPS_PER_VG) {
      vError("vgId:%d, txnId:%" PRId64 " DDL op count %d >= limit %d, reject",
             TD_VID(pVnode), txnId, tSimpleHashGetSize(pEntry->pTouchedUids), TSDB_META_TXN_MAX_DDL_OPS_PER_VG);
      code = TSDB_CODE_TXN_TOO_MANY_DDL_OPS;
    } else {
      code = vnodeTxnTrackUid(pEntry, uid);
    }
    pEntry->lastActive = taosGetTimestampMs();
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
    // DDL count limit per VNode (skip for replicated txns — taosX WAL replay)
    if (!TXN_IS_REPLICATED(txnId) && pEntry->pTouchedUids &&
        tSimpleHashGetSize(pEntry->pTouchedUids) >= TSDB_META_TXN_MAX_DDL_OPS_PER_VG) {
      vError("vgId:%d, txnId:%" PRId64 " DDL op count %d >= limit %d, reject ALTER",
             TD_VID(pVnode), txnId, tSimpleHashGetSize(pEntry->pTouchedUids), TSDB_META_TXN_MAX_DDL_OPS_PER_VG);
      code = TSDB_CODE_TXN_TOO_MANY_DDL_OPS;
    } else {
      code = tSimpleHashPut(pEntry->pAlterPrevVers, &uid, sizeof(tb_uid_t), &prevVersion, sizeof(int64_t));
      if (code != 0) {
        vError("vgId:%d, vnodeTxnTrackAlter: failed to put alter record for uid:%" PRId64, TD_VID(pVnode), uid);
      }
      if (code == TSDB_CODE_SUCCESS) {
        code = vnodeTxnTrackUid(pEntry, uid);
      }
    }
    pEntry->lastActive = taosGetTimestampMs();
  }
  (void)taosThreadMutexUnlock(&pVnode->txnMutex);
  return code;
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

  int32_t iter = 0;
  void   *pData = tSimpleHashIterate(pEntry->pTouchedUids, NULL, &iter);
  while (pData != NULL) {
    size_t   keyLen = 0;
    tb_uid_t uid = *(tb_uid_t *)tSimpleHashGetKey(pData, &keyLen);

    // Fetch the current entry from B+ tree
    SMetaEntry *pME = NULL;
    int32_t     code = metaFetchEntryByUid(pVnode->pMeta, uid, &pME);
    if (code != 0 || pME == NULL) {
      vWarn("vgId:%d, commit: uid %" PRId64 " not found in B+ tree, skip", TD_VID(pVnode), uid);
      pData = tSimpleHashIterate(pEntry->pTouchedUids, pData, &iter);
      continue;
    }

    if (pME->txnId != pEntry->txnId) {
      // Entry doesn't belong to this txn (maybe already committed/cleaned)
      metaFetchEntryFree(&pME);
      pData = tSimpleHashIterate(pEntry->pTouchedUids, pData, &iter);
      continue;
    }

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
        // Physically delete: reissue drop with txnId=0
        if (pME->type == TSDB_SUPER_TABLE) {
          // STB: first clear txn status, then physically drop via STB path
          int32_t markCode = metaMarkTableTxnStatus(pVnode->pMeta, uid, 0, META_TXN_NORMAL, -1);
          if (markCode != 0) {
            vError("vgId:%d, commit: failed to clear PRE_DROP status for STB uid %" PRId64 ", code:0x%x",
                   TD_VID(pVnode), uid, markCode);
            if (txnShouldPropagateError(pEntry->txnId, markCode, TSDB_CODE_TXN_NOT_EXIST)) {
              metaFetchEntryFree(&pME);
              return markCode;
            }
          }
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
        } else {
          vError("vgId:%d, commit: failed to drop uid %" PRId64 ", code:0x%x", TD_VID(pVnode), uid, code);
        }
        break;
      }

      default:
        vDebug("vgId:%d, commit: uid %" PRId64 " has status %d, skip", TD_VID(pVnode), uid, pME->txnStatus);
        break;
    }

    if (code != 0 && txnShouldPropagateError(pEntry->txnId, code, TSDB_CODE_TXN_NOT_EXIST)) {
      metaFetchEntryFree(&pME);
      return code;
    }

    // Remove from txn.idx regardless of status
    int32_t idxCode = metaTxnIdxDelete(pVnode->pMeta, uid);
    if (idxCode != 0) {
      vError("vgId:%d, commit: failed to delete txn.idx for uid %" PRId64 ", code:0x%x", TD_VID(pVnode), uid, idxCode);
      if (txnShouldPropagateError(pEntry->txnId, idxCode, TSDB_CODE_TXN_NOT_EXIST)) {
        metaFetchEntryFree(&pME);
        return idxCode;
      }
    }

    metaFetchEntryFree(&pME);
    pData = tSimpleHashIterate(pEntry->pTouchedUids, pData, &iter);
  }

  return TSDB_CODE_SUCCESS;
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
static int32_t vnodeTxnUndoShadowEntries(SVnode *pVnode, SVnodeTxnEntry *pEntry) {
  if (pEntry->pTouchedUids == NULL) return TSDB_CODE_SUCCESS;

  int32_t numUids = tSimpleHashGetSize(pEntry->pTouchedUids);
  vInfo("vgId:%d, undoing %d shadow entries for txn %" PRId64, TD_VID(pVnode), numUids, pEntry->txnId);

  int32_t iter = 0;
  void   *pData = tSimpleHashIterate(pEntry->pTouchedUids, NULL, &iter);
  while (pData != NULL) {
    size_t   keyLen = 0;
    tb_uid_t uid = *(tb_uid_t *)tSimpleHashGetKey(pData, &keyLen);

    // Fetch the current entry from B+ tree
    SMetaEntry *pME = NULL;
    int32_t     code = metaFetchEntryByUid(pVnode->pMeta, uid, &pME);
    if (code != 0 || pME == NULL) {
      vWarn("vgId:%d, rollback: uid %" PRId64 " not found in B+ tree, skip", TD_VID(pVnode), uid);
      pData = tSimpleHashIterate(pEntry->pTouchedUids, pData, &iter);
      continue;
    }

    if (pME->txnId != pEntry->txnId) {
      metaFetchEntryFree(&pME);
      pData = tSimpleHashIterate(pEntry->pTouchedUids, pData, &iter);
      continue;
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
        } else {
          vError("vgId:%d, rollback: failed to delete PRE_CREATE uid %" PRId64 ", code:0x%x", TD_VID(pVnode), uid,
                 code);
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
        // Primary source: txnPrevVer persisted in B+ tree entry (survives snapshot).
        // Fallback: in-memory pAlterPrevVers hash (O(1) lookup by uid).
        int64_t prevVersion = pME->txnPrevVer;
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
                  vError("vgId:%d, rollback: chained delete PRE_CREATE uid %" PRId64 " failed, code:0x%x",
                         TD_VID(pVnode), uid, dropCode);
                  code = dropCode;
                }
              }
              metaFetchEntryFree(&pRestored);
            }
          } else {
            vError("vgId:%d, rollback: metaRollbackAlterTable failed for uid %" PRId64 ", code:0x%x", TD_VID(pVnode),
                   uid, code);
          }
        } else {
          // Fallback: just clear txnStatus on the current entry
          code = metaMarkTableTxnStatus(pVnode->pMeta, uid, 0, META_TXN_NORMAL, -1);
          if (code != 0) {
            vError("vgId:%d, rollback: failed to clear ALTER status for uid %" PRId64 ", code:0x%x", TD_VID(pVnode),
                   uid, code);
          }
          vWarn("vgId:%d, rollback: ALTER uid %" PRId64 " old version not found, cleared status", TD_VID(pVnode), uid);
        }
        break;
      }

      default:
        vDebug("vgId:%d, rollback: uid %" PRId64 " has status %d, skip", TD_VID(pVnode), uid, pME->txnStatus);
        break;
    }

    if (code != 0 && txnShouldPropagateError(pEntry->txnId, code, TSDB_CODE_TXN_NOT_EXIST)) {
      metaFetchEntryFree(&pME);
      return code;
    }

    // Remove from txn.idx regardless of status
    int32_t idxCode = metaTxnIdxDelete(pVnode->pMeta, uid);
    if (idxCode != 0) {
      vError("vgId:%d, rollback: failed to delete txn.idx for uid %" PRId64 ", code:0x%x", TD_VID(pVnode), uid,
             idxCode);
      if (txnShouldPropagateError(pEntry->txnId, idxCode, TSDB_CODE_TXN_NOT_EXIST)) {
        metaFetchEntryFree(&pME);
        return idxCode;
      }
    }

    metaFetchEntryFree(&pME);
    pData = tSimpleHashIterate(pEntry->pTouchedUids, pData, &iter);
  }

  return TSDB_CODE_SUCCESS;
}

// ============================================================================
// Lazy COMMIT/ROLLBACK: Finalize txn in O(1), vacuum later
// ============================================================================

/**
 * Prepare vacuum array: convert pTouchedUids hash to a flat UID array
 * for incremental batch processing by the vacuum.
 */
static int32_t vnodeTxnPrepareVacuumArray(SVnodeTxnEntry *pEntry) {
  if (pEntry->pTouchedUids == NULL) {
    pEntry->pVacuumUids = NULL;
    pEntry->numVacuumUids = 0;
    pEntry->vacuumIdx = 0;
    return TSDB_CODE_SUCCESS;
  }

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
 * Finalize a txn lazily: write O(1) record to txn_final.idx + in-memory cache,
 * convert pTouchedUids to vacuum array. Does NOT modify the B+ tree shadow entries.
 * The actual cleanup is done incrementally by vnodeTxnVacuumBatch().
 */
static int32_t vnodeTxnFinalizeLazy(SVnode *pVnode, SVnodeTxnEntry *pEntry, int8_t finalStatus) {
  int32_t code = TSDB_CODE_SUCCESS;

  // 1. Write finalization record to persistent txn_final.idx (O(1))
  STxnFinalVal finalVal = {.finalStatus = finalStatus, .timestamp = taosGetTimestampMs()};
  code = metaTxnFinalIdxUpsert(pVnode->pMeta, pEntry->txnId, &finalVal);
  if (code != 0) {
    vError("vgId:%d, failed to write txn_final.idx for txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode), pEntry->txnId,
           code);
    return code;
  }

  // 2. Update in-memory cache (thread-safe, visible to query threads immediately).
  // On failure (OOM), persistent txn_final.idx is still authoritative — visibility
  // queries will fall back to disk lookup, but log a warning so the issue is visible.
  int32_t cacheCode = taosHashPut(pVnode->pFinalizedTxns, &pEntry->txnId, sizeof(int64_t), &finalStatus, sizeof(int8_t));
  if (cacheCode != 0) {
    vWarn("vgId:%d, failed to cache finalized txn:%" PRId64 " status:%d in pFinalizedTxns, code:0x%x",
          TD_VID(pVnode), pEntry->txnId, finalStatus, cacheCode);
  }

  // 3. Prepare vacuum array for deferred cleanup
  code = vnodeTxnPrepareVacuumArray(pEntry);
  if (code != 0) {
    vError("vgId:%d, failed to prepare vacuum array for txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode), pEntry->txnId,
           code);
    return code;
  }

  // 4. Mark entry as finalized (keep in pTxnHash for vacuum, release table locks)
  pEntry->finalStatus = finalStatus;
  pEntry->stage = VTXN_STAGE_FINISHING;

  // Release table locks immediately (other txns can now operate on these tables)
  vnodeReleaseTxnTableLocks(pVnode, pEntry);

  return TSDB_CODE_SUCCESS;
}

/**
 * Vacuum one batch of UIDs for a single finalized txn.
 * Returns number of UIDs processed. 0 means vacuum complete for this txn.
 */
static int32_t vnodeTxnVacuumOneTxn(SVnode *pVnode, SVnodeTxnEntry *pEntry, int32_t maxOps) {
  int32_t processed = 0;

  while (pEntry->vacuumIdx < pEntry->numVacuumUids && processed < maxOps) {
    tb_uid_t uid = pEntry->pVacuumUids[pEntry->vacuumIdx];

    SMetaEntry *pME = NULL;
    int32_t     code = metaFetchEntryByUid(pVnode->pMeta, uid, &pME);
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

    if (pEntry->finalStatus == TXN_FINAL_COMMITTED) {
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
            int32_t markCode = metaMarkTableTxnStatus(pVnode->pMeta, uid, 0, META_TXN_NORMAL, -1);
            if (markCode == 0) {
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
            int32_t markCode = metaMarkTableTxnStatus(pVnode->pMeta, uid, 0, META_TXN_NORMAL, -1);
            if (markCode == 0) {
              SVDropStbReq stbDropReq = {.name = pME->name, .suid = uid, .txnId = 0};
              code = metaDropSuperTable(pVnode->pMeta, -1, &stbDropReq);
            }
          } else {
            code = metaDropTable2(pVnode->pMeta, -1, &dropReq);
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

    // Remove from txn.idx
    (void)metaTxnIdxDelete(pVnode->pMeta, uid);

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
  if (pVnode->pTxnHash == NULL || pVnode->pFinalizedTxns == NULL) return 0;
  if (taosHashGetSize(pVnode->pFinalizedTxns) == 0) return 0;

  int32_t totalProcessed = 0;

  // Pre-allocate before entering the loop. Lazy allocation inside the loop causes a silent
  // stuck-entry bug: if taosArrayInit returns NULL on OOM mid-loop, a fully-vacuumed txn
  // is never added to the removal list, leaks in pTxnHash/pFinalizedTxns forever, and
  // causes vnodeTxnVacuumExecute to spin in infinite no-op retries.
  SArray *pCompletedTxns = taosArrayInit(4, sizeof(int64_t));
  if (pCompletedTxns == NULL) {
    vError("vgId:%d, out of memory allocating completed-txns array in vacuum batch", TD_VID(pVnode));
    return 0;
  }

  (void)taosThreadMutexLock(&pVnode->txnMutex);

  void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
  while (pIter != NULL && totalProcessed < maxOps) {
    SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;

    if (pEntry->finalStatus == TXN_FINAL_NONE) {
      pIter = taosHashIterate(pVnode->pTxnHash, pIter);
      continue;
    }

    // Process a batch for this txn
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    int32_t processed = vnodeTxnVacuumOneTxn(pVnode, pEntry, maxOps - totalProcessed);
    (void)taosThreadMutexLock(&pVnode->txnMutex);

    totalProcessed += processed;

    // Check if this txn is fully vacuumed
    if (pEntry->vacuumIdx >= pEntry->numVacuumUids) {
      if (taosArrayPush(pCompletedTxns, &pEntry->txnId) == NULL) {
        // OOM: txnId not recorded, entry will not be cleaned this round.
        // Next vacuum cycle will retry. Log so the condition is observable.
        vWarn("vgId:%d, out of memory recording completed txnId:% " PRId64 " in vacuum batch", TD_VID(pVnode),
               pEntry->txnId);
      }
    }

    pIter = taosHashIterate(pVnode->pTxnHash, pIter);
  }

  (void)taosThreadMutexUnlock(&pVnode->txnMutex);

  // Remove fully-vacuumed txns (outside iteration)
  int32_t numCompleted = taosArrayGetSize(pCompletedTxns);
  for (int32_t i = 0; i < numCompleted; i++) {
    int64_t txnId = *(int64_t *)taosArrayGet(pCompletedTxns, i);

    // Remove from txn_final.idx
    int32_t delCode = metaTxnFinalIdxDelete(pVnode->pMeta, txnId);
    if (delCode != 0) {
      // Persistent record survives; vacuum will retry on next restart (idempotent).
      vWarn("vgId:%d, failed to delete txn_final.idx for txnId:% " PRId64 ": %s", TD_VID(pVnode), txnId,
             tstrerror(delCode));
    }

    // Remove from in-memory cache
    int32_t rmCode = taosHashRemove(pVnode->pFinalizedTxns, &txnId, sizeof(int64_t));
    if (rmCode != 0) {
      // Stale key left in pFinalizedTxns; vacuum will be re-triggered unnecessarily.
      vWarn("vgId:%d, failed to remove txnId:% " PRId64 " from pFinalizedTxns: %s", TD_VID(pVnode), txnId,
             tstrerror(rmCode));
    }

    // Remove SVnodeTxnEntry
    (void)taosThreadMutexLock(&pVnode->txnMutex);
    vnodeRemoveTxnEntry(pVnode, txnId);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);

    vInfo("vgId:%d, vacuum complete for txnId:%" PRId64, TD_VID(pVnode), txnId);
  }
  taosArrayDestroy(pCompletedTxns);

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

  while (pVnode->pFinalizedTxns && taosHashGetSize(pVnode->pFinalizedTxns) > 0) {
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
  if (totalProcessed > 0 && !atomic_load_8(&pVnode->closing) && pVnode->pFinalizedTxns &&
      taosHashGetSize(pVnode->pFinalizedTxns) > 0) {
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
  // Skip fencing for replicated transactions (no Raft term from source cluster)
  if (!TXN_IS_REPLICATED(req.txnId)) {
    code = vnodeTxnFencing(pVnode, req.term, req.txnId);
    if (code == TSDB_CODE_VND_TXN_STALE_TERM) {
      vWarn("vgId:%d, reject txn commit due to stale term, txnId:%" PRId64 ", reqTerm:%" PRId64, TD_VID(pVnode),
            req.txnId, req.term);
      return TSDB_CODE_VND_TXN_STALE_TERM;
    } else if (code != TSDB_CODE_SUCCESS) {
      vError("vgId:%d, fencing error on commit, txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode), req.txnId, code);
      return code;
    }
  }

  (void)taosThreadMutexLock(&pVnode->txnMutex);

  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, req.txnId);
  if (pEntry == NULL) {
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    // Shadow missing — empty txn on this VGroup (no DDL was routed here)
    vDebug("vgId:%d, txn entry not found on commit (no-op), txnId:%" PRId64, TD_VID(pVnode), req.txnId);
    return TSDB_CODE_SUCCESS;
  }

  // Lazy term correction: entry was created with maxSeenTerm which may have been 0
  if (pEntry->term == 0 && req.term > 0) {
    pEntry->term = req.term;
  }
  pEntry->stage = VTXN_STAGE_FINISHING;

  int32_t numUids = pEntry->pTouchedUids ? tSimpleHashGetSize(pEntry->pTouchedUids) : 0;

  if (numUids <= TSDB_TXN_INLINE_THRESHOLD) {
    // ── Small txn: synchronous inline promote ──
    // O(k) B+ tree ops where k ≤ 128, typically < 1ms.
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);

    code = vnodeTxnPromoteShadowEntries(pVnode, pEntry);
    if (code != 0) {
      // Non-critical: individual UID ops may fail (e.g., STB already removed by MNode STrans).
      // Log and continue — the txn entry must be cleaned up regardless.
      vWarn("vgId:%d, inline commit partial failure for txnId:%" PRId64 ", code:0x%x (continuing)", TD_VID(pVnode),
            req.txnId, code);
    }

    (void)taosThreadMutexLock(&pVnode->txnMutex);
    vnodeRemoveTxnEntry(pVnode, req.txnId);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);

    vInfo("vgId:%d, txn commit done (inline), txnId:%" PRId64 ", numUids:%d", TD_VID(pVnode), req.txnId, numUids);
  } else {
    // ── Large txn: lazy finalize O(1) + async vacuum ──
    code = vnodeTxnFinalizeLazy(pVnode, pEntry, TXN_FINAL_COMMITTED);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);

    if (code != 0) {
      vError("vgId:%d, failed to finalize commit for txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode), req.txnId, code);
      return code;
    }

    vInfo("vgId:%d, txn commit finalized (lazy), txnId:%" PRId64 ", numUids:%d", TD_VID(pVnode), req.txnId, numUids);

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
  // Skip fencing for replicated transactions (no Raft term from source cluster)
  if (!TXN_IS_REPLICATED(req.txnId)) {
    code = vnodeTxnFencing(pVnode, req.term, req.txnId);
    if (code == TSDB_CODE_VND_TXN_STALE_TERM) {
      vWarn("vgId:%d, reject txn rollback due to stale term, txnId:%" PRId64 ", reqTerm:%" PRId64, TD_VID(pVnode),
            req.txnId, req.term);
      return TSDB_CODE_VND_TXN_STALE_TERM;
    } else if (code != TSDB_CODE_SUCCESS) {
      vError("vgId:%d, fencing error on rollback, txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode), req.txnId, code);
      return code;
    }
  }

  (void)taosThreadMutexLock(&pVnode->txnMutex);

  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, req.txnId);
  if (pEntry == NULL) {
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    // Idempotent: already rolled back or never existed
    vWarn("vgId:%d, txn not found for rollback (idempotent), txnId:%" PRId64, TD_VID(pVnode), req.txnId);
    return TSDB_CODE_SUCCESS;
  }

  // Lazy term correction: entry was created with maxSeenTerm which may have been 0
  if (pEntry->term == 0 && req.term > 0) {
    pEntry->term = req.term;
  }
  pEntry->stage = VTXN_STAGE_FINISHING;

  int32_t numUids = pEntry->pTouchedUids ? tSimpleHashGetSize(pEntry->pTouchedUids) : 0;

  if (numUids <= TSDB_TXN_INLINE_THRESHOLD) {
    // ── Small txn: synchronous inline undo ──
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);

    code = vnodeTxnUndoShadowEntries(pVnode, pEntry);
    if (code != 0) {
      // Non-critical: individual UID ops may fail (e.g., STB already removed by MNode STrans).
      // Log and continue — the txn entry must be cleaned up regardless.
      vWarn("vgId:%d, inline rollback partial failure for txnId:%" PRId64 ", code:0x%x (continuing)", TD_VID(pVnode),
            req.txnId, code);
    }

    (void)taosThreadMutexLock(&pVnode->txnMutex);
    vnodeRemoveTxnEntry(pVnode, req.txnId);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);

    vInfo("vgId:%d, txn rollback done (inline), txnId:%" PRId64 ", numUids:%d", TD_VID(pVnode), req.txnId, numUids);
  } else {
    // ── Large txn: lazy finalize O(1) + async vacuum ──
    code = vnodeTxnFinalizeLazy(pVnode, pEntry, TXN_FINAL_ROLLEDBACK);
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
    return TSDB_CODE_VND_TXN_STALE_TERM;
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
    // Also skip replicated transactions (lifecycle controlled by source cluster, not local Raft term).
    if (pEntry->term > 0 && pEntry->term < newTerm && pEntry->txnId != newTxnId && !TXN_IS_REPLICATED(pEntry->txnId)) {
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
    int64_t txnId = *(int64_t *)taosArrayGet(toAbort, i);
    SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
    if (pEntry) {
      pEntry->stage = VTXN_STAGE_FINISHING;
      (void)taosThreadMutexUnlock(&pVnode->txnMutex);

      // Undo shadow entries in B+ tree before removing
      int32_t undoCode = vnodeTxnUndoShadowEntries(pVnode, pEntry);

      (void)taosThreadMutexLock(&pVnode->txnMutex);
      if (undoCode != 0 && txnShouldPropagateError(txnId, undoCode, TSDB_CODE_TXN_NOT_EXIST)) {
        vError("vgId:%d, fencing: failed to abort txnId:%" PRId64 ", code:0x%x", TD_VID(pVnode), txnId, undoCode);
        (void)taosThreadMutexUnlock(&pVnode->txnMutex);
        taosArrayDestroy(toAbort);
        return undoCode;
      }
      vnodeRemoveTxnEntry(pVnode, txnId);
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

  (void)taosThreadMutexLock(&pVnode->txnMutex);

  void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
  while (pIter) {
    SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;
    // Skip replicated transactions (lifecycle controlled by source WAL, no MNode keepalive)
    if (pEntry->stage == VTXN_STAGE_ACTIVE && !TXN_IS_REPLICATED(pEntry->txnId) &&
        (now - pEntry->lastActive > quietThreshold)) {
      STxnActiveQuery q = {.txnId = pEntry->txnId, .vgId = TD_VID(pVnode)};
      if (taosArrayPush(pQueries, &q) == NULL) {
        vError("vgId:%d, failed to push keepalive query for txnId:%" PRId64, TD_VID(pVnode), pEntry->txnId);
        taosHashCancelIterate(pVnode->pTxnHash, pIter);
        (void)taosThreadMutexUnlock(&pVnode->txnMutex);
        return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
      }
    }
    pIter = taosHashIterate(pVnode->pTxnHash, pIter);
  }

  (void)taosThreadMutexUnlock(&pVnode->txnMutex);
  return TSDB_CODE_SUCCESS;
}

/**
 * Scan for orphan transactions that have exceeded the hard timeout.
 * This catches transactions received via snapshot replication whose
 * COMMIT/ROLLBACK messages were lost (e.g. taosX disconnection).
 *
 * Called periodically alongside vnodeCollectIdleTxns.
 * Orphan transactions are rolled back to prevent permanent intermediate state.
 */
int32_t vnodeTxnTimeoutScan(SVnode *pVnode) {
  if (pVnode->pTxnHash == NULL) return TSDB_CODE_SUCCESS;

  // Only leader should execute timeout rollback — local (non-Raft) operation.
  // This is a safety net for orphan txns that MNode keepalive missed (e.g. taosX disconnect).
  // Followers must NOT execute this: they receive the equivalent rollback through WAL when
  // MNode issues mndRollbackOrphanTxnOnVnode via the normal keepalive path.
  if (!vnodeIsRoleLeader(pVnode)) return TSDB_CODE_SUCCESS;

  int64_t now = taosGetTimestampMs();
  int64_t hardTimeout = (int64_t)tsMetaTxnTimeout * 1000;

  SArray *toRollback = taosArrayInit(4, sizeof(int64_t));
  if (toRollback == NULL) return terrno;

  (void)taosThreadMutexLock(&pVnode->txnMutex);

  void *pIter = taosHashIterate(pVnode->pTxnHash, NULL);
  while (pIter) {
    SVnodeTxnEntry *pEntry = (SVnodeTxnEntry *)pIter;
    if (pEntry->stage == VTXN_STAGE_ACTIVE && (now - pEntry->startTime > hardTimeout)) {
      vWarn("vgId:%d, txn %" PRId64 " exceeded hard timeout (%" PRId64 "ms), scheduling rollback", TD_VID(pVnode),
            pEntry->txnId, now - pEntry->startTime);
      if (taosArrayPush(toRollback, &pEntry->txnId) == NULL) {
        vError("vgId:%d, timeout scan: failed to push txnId:%" PRId64 " to rollback list", TD_VID(pVnode),
               pEntry->txnId);
        taosHashCancelIterate(pVnode->pTxnHash, pIter);
        (void)taosThreadMutexUnlock(&pVnode->txnMutex);
        taosArrayDestroy(toRollback);
        return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
      }
    }
    pIter = taosHashIterate(pVnode->pTxnHash, pIter);
  }

  int32_t numRollback = taosArrayGetSize(toRollback);
  for (int32_t i = 0; i < numRollback; i++) {
    int64_t         txnId = *(int64_t *)taosArrayGet(toRollback, i);
    SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
    if (pEntry) {
      pEntry->stage = VTXN_STAGE_FINISHING;
      (void)taosThreadMutexUnlock(&pVnode->txnMutex);

      int32_t undoCode = vnodeTxnUndoShadowEntries(pVnode, pEntry);
      if (undoCode != 0) {
        vError("vgId:%d, timeout rollback failed for txn %" PRId64 ": %s", TD_VID(pVnode), txnId, tstrerror(undoCode));
      }

      (void)taosThreadMutexLock(&pVnode->txnMutex);
      vnodeRemoveTxnEntry(pVnode, txnId);
      vInfo("vgId:%d, orphan txn %" PRId64 " rolled back by timeout", TD_VID(pVnode), txnId);
    }
  }

  (void)taosThreadMutexUnlock(&pVnode->txnMutex);
  taosArrayDestroy(toRollback);

  return TSDB_CODE_SUCCESS;
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
    return TSDB_CODE_VND_TXN_CONFLICT;
  }

  // Verify the requesting transaction exists
  SVnodeTxnEntry *pEntry = vnodeGetTxnEntry(pVnode, txnId);
  if (pEntry == NULL) {
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    vWarn("vgId:%d, cannot lock table, txn not found, table:%s, txnId:%" PRId64, TD_VID(pVnode), tableName, txnId);
    return TSDB_CODE_VND_TXN_EXPIRED;
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
    (void)taosHashRemove(pVnode->pTxnTableLock, tableName, nameLen);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  if (taosArrayPush(pEntry->pLockedTables, &nameCopy) == NULL) {
    vError("vgId:%d, failed to track locked table:%s, txnId:%" PRId64, TD_VID(pVnode), tableName, txnId);
    taosMemoryFree(nameCopy);
    (void)taosHashRemove(pVnode->pTxnTableLock, tableName, nameLen);
    (void)taosThreadMutexUnlock(&pVnode->txnMutex);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pEntry->lastActive = taosGetTimestampMs();

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

  // Read the table entry from B+ tree to check txnStatus
  SMetaEntry *pME = NULL;
  int32_t     code = metaFetchEntryByName(pVnode->pMeta, tableName, &pME);
  if (code != 0 || pME == NULL) {
    // Table not found in meta — no conflict possible
    return TSDB_CODE_SUCCESS;
  }

  int32_t ret = TSDB_CODE_SUCCESS;
  if (pME->txnId != 0) {
    // Check if the owning txn is finalized → no conflict (vacuum will clean up)
    int8_t finalStatus = metaGetTxnFinalStatus(pVnode->pMeta, pME->txnId);
    if (finalStatus == TXN_FINAL_COMMITTED || finalStatus == TXN_FINAL_ROLLEDBACK) {
      // For COMMITTED PRE_CREATE: table exists, CREATE should fail (TABLE_ALREADY_EXIST)
      // For ROLLEDBACK PRE_CREATE: table doesn't exist, CREATE should succeed (no conflict)
      // For COMMITTED PRE_DROP: table is gone, handled elsewhere
      // For ROLLEDBACK PRE_DROP: table restored, no conflict with new ops
      if (finalStatus == TXN_FINAL_COMMITTED && pME->txnStatus == META_TXN_PRE_CREATE && incomingOp == 1) {
        ret = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
      }
      metaFetchEntryFree(&pME);
      return ret;
    }

    switch (pME->txnStatus) {
      case META_TXN_PRE_CREATE:
        if (incomingOp == 1) {  // CREATE vs PRE_CREATE
          ret = TSDB_CODE_VND_TXN_CONFLICT;
        }
        // Other ops: table is invisible to non-txn → will naturally fail as "not exist"
        break;

      case META_TXN_PRE_DROP:
        if (incomingOp == 3 || incomingOp == 2) {  // DROP/ALTER vs PRE_DROP
          ret = TSDB_CODE_VND_TXN_CONFLICT;
        } else if (incomingOp == 1) {  // CREATE vs PRE_DROP
          ret = TSDB_CODE_TDB_TABLE_ALREADY_EXIST;
        }
        // SELECT/INSERT (incomingOp=0): allowed, no conflict
        break;

      case META_TXN_PRE_ALTER:
        if (incomingOp == 2 || incomingOp == 3) {  // ALTER/DROP vs PRE_ALTER
          ret = TSDB_CODE_VND_TXN_CONFLICT;
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
 * @return TSDB_CODE_SUCCESS if no conflict, TSDB_CODE_VND_TXN_CONFLICT if blocked
 */
int32_t vnodeTxnCheckDeleteConflict(SVnode *pVnode, tb_uid_t uid) {
  if (pVnode->pTxnHash == NULL || uid == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SMetaEntry *pME = NULL;
  int32_t     code = metaFetchEntryByUid(pVnode->pMeta, uid, &pME);
  if (code != 0 || pME == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t ret = TSDB_CODE_SUCCESS;
  if (pME->txnId != 0 && pME->txnStatus == META_TXN_PRE_DROP) {
    // Check if finalized → no conflict
    int8_t finalStatus = metaGetTxnFinalStatus(pVnode->pMeta, pME->txnId);
    if (finalStatus == TXN_FINAL_NONE) {
      ret = TSDB_CODE_VND_TXN_CONFLICT;
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
