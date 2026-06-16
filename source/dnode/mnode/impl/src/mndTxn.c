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
#include "tsimplehash.h"

// Max concurrent active transactions allowed.
#define MND_TXN_MAX_ACTIVE 200
// Max GC STrans (Raft writes) initiated per single scan invocation.
// Caps the write-thread burst when many COMPLETED/ZOMBIE entries expire simultaneously.
// Remaining expired entries are picked up in subsequent scans (every 15 s).
#define MND_TXN_GC_PER_SCAN_MAX 15
// SDB serialisation version.  v1 = current (stage encodes terminal outcome; completedAt for TTL GC).
#define MND_TXN_VER_NUMBER 1

#define MND_TXN_LOG_RESERVE_SIZE 32
// SDB_TXN_LOG version:
//   v1 = compact record (id, stage, rollbackReason, completedAt, createUser, createTime) + 32-byte reserve
#define MND_TXN_LOG_VER_NUMBER 1
// TSDB_TXN_HB_TIMEOUT is defined in mndTxn.h (shared with mndMain.c for the pullup interval).

static SSdbRaw *mndTxnActionEncode(STxnObj *pTxn);
static SSdbRow *mndTxnActionDecode(SSdbRaw *pRaw);
static int32_t  mndTxnActionInsert(SSdb *pSdb, STxnObj *pTxn);
static int32_t  mndTxnActionDelete(SSdb *pSdb, STxnObj *pTxn);
static int32_t  mndTxnActionUpdate(SSdb *pSdb, STxnObj *pOld, STxnObj *pNew);

// SDB_TXN_LOG callbacks — compact terminal-txn log
static SSdbRaw *mndTxnLogActionEncode(STxnLogObj *pLog);
static SSdbRow *mndTxnLogActionDecode(SSdbRaw *pRaw);
static int32_t  mndTxnLogActionInsert(SSdb *pSdb, STxnLogObj *pLog);
static int32_t  mndTxnLogActionDelete(SSdb *pSdb, STxnLogObj *pLog);
static int32_t  mndGcTxnLog(SMnode *pMnode, STxnLogObj *pLog);

static int32_t mndProcessBeginTxnReq(SRpcMsg *pReq);
static int32_t mndProcessCommitTxnReq(SRpcMsg *pReq);
static int32_t mndProcessRollbackTxnReq(SRpcMsg *pReq);

static int32_t mndRetrieveTxn(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelRetrieveTxn(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveTxnLog(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelRetrieveTxnLog(SMnode *pMnode, void *pIter);
static int32_t mndRetrieveTxnOrphans(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);
static void    mndCancelRetrieveTxnOrphans(SMnode *pMnode, void *pIter);

// Forward declarations
static void    mndTxnTimeoutScanImpl(SMnode *pMnode);
static int32_t mndRollbackTxn(SMnode *pMnode, SRpcMsg *pReq, STxnObj *pTxn, int32_t reason);
static int32_t mndCommitTxn(SMnode *pMnode, SRpcMsg *pReq, STxnObj *pTxn);
static int32_t mndTxnAfterRestored(SMnode *pMnode);
static int32_t mndTxnRebuildShadowOpsFromSdb(SMnode *pMnode, STxnObj *pTxn, bool needAlterData);
static int32_t mndProcessTxnTimerReq(SRpcMsg *pReq);

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

  // Initialise the STxnMgmt runtime management structure
  STxnMgmt *pMgmt = &pMnode->txnMgmt;

  pMgmt->pOrphanRollbackTs = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pMgmt->pOrphanRollbackTs == NULL) {
    mError("txn, failed to init orphan rollback dedup hash");
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pMgmt->pOrphanTxnMap = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pMgmt->pOrphanTxnMap == NULL) {
    mError("txn, failed to init orphan txn map");
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pMgmt->pStbConflictMap =
      taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_ENTRY_LOCK);
  if (pMgmt->pStbConflictMap == NULL) {
    mError("txn, failed to init stb conflict map");
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  pMgmt->currentTxnId = -1;

  mndSetMsgHandle(pMnode, TDMT_MND_BEGIN_TXN, mndProcessBeginTxnReq);
  mndSetMsgHandle(pMnode, TDMT_MND_BEGIN_TXN_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_COMMIT_TXN, mndProcessCommitTxnReq);
  mndSetMsgHandle(pMnode, TDMT_MND_COMMIT_TXN_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_ROLLBACK_TXN, mndProcessRollbackTxnReq);
  mndSetMsgHandle(pMnode, TDMT_MND_ROLLBACK_TXN_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_TXN_COMMIT_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_VND_TXN_ROLLBACK_RSP, mndTransProcessRsp);
  mndSetMsgHandle(pMnode, TDMT_MND_TXN_TIMER, mndProcessTxnTimerReq);

  //   mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TXN, mndRetrieveTxn);
  //   mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TXN, mndCancelRetrieveTxn);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TXN_LOG, mndRetrieveTxnLog);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TXN_LOG, mndCancelRetrieveTxnLog);
  mndAddShowRetrieveHandle(pMnode, TSDB_MGMT_TABLE_TXN_ORPHANS, mndRetrieveTxnOrphans);
  mndAddShowFreeIterHandle(pMnode, TSDB_MGMT_TABLE_TXN_ORPHANS, mndCancelRetrieveTxnOrphans);

  int32_t code0 = sdbSetTable(pMnode->pSdb, table);
  if (code0 != 0) return code0;

  // Register SDB_TXN_LOG — compact persistent log of terminal non-replicated txns
  SSdbTable txnLogTable = {
      .sdbType = SDB_TXN_LOG,
      .keyType = SDB_KEY_INT64,
      .encodeFp = (SdbEncodeFp)mndTxnLogActionEncode,
      .decodeFp = (SdbDecodeFp)mndTxnLogActionDecode,
      .insertFp = (SdbInsertFp)mndTxnLogActionInsert,
      .deleteFp = (SdbDeleteFp)mndTxnLogActionDelete,
  };
  return sdbSetTable(pMnode->pSdb, txnLogTable);
}

/**
 * Mark a txn as ABORTED in SDB (persisted via Raft STrans).
 * Used during leader restore when conflict-map repopulation fails — the txn's shadow
 * ops are incomplete so it must not accept new DDL; only ROLLBACK is allowed.
 * Sets stage = UTXN_STAGE_ABORTED in both memory and SDB.
 */
static int32_t mndMarkTxnAbortedInSdb(SMnode *pMnode, STxnObj *pTxn) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;

  SRpcMsg synReq = {0};
  synReq.info.node = pMnode;

  TSDB_CHECK_NULL(
      (pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, &synReq, "mark-txn-aborted")), code,
      lino, _exit, terrno);
  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);

  {
    STxnObj abortObj = *pTxn;
    abortObj.stage = UTXN_STAGE_ABORTED;
    SSdbRaw *pRaw = mndTxnActionEncode(&abortObj);
    if (pRaw == NULL) {
      TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL);
    }
    TAOS_CHECK_EXIT(mndTransAppendPrepareLog(pTrans, pRaw));
    TAOS_CHECK_EXIT(sdbSetRawStatus(pRaw, SDB_STATUS_READY));
  }

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));

  // Update in-memory stage immediately so no new DDL is accepted before Raft commits.
  taosWLockLatch(&pTxn->lock);
  pTxn->stage = UTXN_STAGE_ABORTED;
  taosWUnLockLatch(&pTxn->lock);

_exit:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

/**
 * GC a COMPLETED or ZOMBIE STxnObj from SDB by creating a minimal drop-only STrans.
 * Called from timeout scan and after-restore when the TTL has expired.
 */
static int32_t mndGcCompletedTxn(SMnode *pMnode, STxnObj *pTxn) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;

  SRpcMsg synReq = {0};
  synReq.info.node = pMnode;

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, &synReq, "gc-txn")), code,
                  lino, _exit, terrno);

  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);

  {
    SSdbRaw *pDropRaw = mndTxnActionEncode(pTxn);
    if (pDropRaw == NULL) {
      TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL);
    }
    TAOS_CHECK_EXIT(mndTransAppendCommitlog(pTrans, pDropRaw));
    TAOS_CHECK_EXIT(sdbSetRawStatus(pDropRaw, SDB_STATUS_DROPPED));
  }

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));

_exit:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

// Returns true if every known DNode is currently online AND every VGroup has a
// leader with syncRestore=true (i.e., the VNode is fully caught up and can report
// orphan txns).  Used by mndAdaptiveGcTerminalTxns to determine safe GC aggressiveness.
static bool mndClusterHealthy(SMnode *pMnode) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  int64_t now = taosGetTimestampMs();

  // Check 1: all DNodes online.
  while (1) {
    SDnodeObj *pDnode = NULL;
    pIter = sdbFetch(pSdb, SDB_DNODE, pIter, (void **)&pDnode);
    if (pIter == NULL) break;
    if (!mndIsDnodeOnline(pDnode, now)) {
      sdbRelease(pSdb, pDnode);
      sdbCancelFetch(pSdb, pIter);
      return false;
    }
    sdbRelease(pSdb, pDnode);
  }

  // Check 2: every VGroup has a restored leader (VNode fully caught up → can report orphans).
  pIter = NULL;
  while (1) {
    SVgObj *pVgroup = NULL;
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) break;
    if (pVgroup->mountVgId != 0) {
      sdbRelease(pSdb, pVgroup);
      continue;
    }
    bool hasRestoredLeader = false;
    for (int32_t i = 0; i < pVgroup->replica; ++i) {
      if ((pVgroup->vnodeGid[i].syncState == TAOS_SYNC_STATE_LEADER ||
           pVgroup->vnodeGid[i].syncState == TAOS_SYNC_STATE_ASSIGNED_LEADER) &&
          pVgroup->vnodeGid[i].syncRestore) {
        hasRestoredLeader = true;
        break;
      }
    }
    if (!hasRestoredLeader) {
      sdbRelease(pSdb, pVgroup);
      sdbCancelFetch(pSdb, pIter);
      return false;
    }
    sdbRelease(pSdb, pVgroup);
  }

  return true;
}

/**
 * Adaptive GC for SDB_TXN_LOG entries (compact terminal-txn records).
 *
 * Non-replicated txns are now written to SDB_TXN_LOG (not kept in SDB_TXN) after completion
 * so that mndGetOrphanTxnAction can answer VNode orphan queries persistently across MNode restarts.
 * This function applies a cluster-health-aware policy to keep the record count bounded.
 *
 * Policy:
 *   < 1000 entries  : low pressure, 7-day retention.
 *   >= 1000 entries : need stronger GC.
 *     Cluster healthy (all DNodes online + all VGroups have restored leader):
 *       - > 10000:    keep 1 hour  (severe pressure)
 *       - 1000~10000: keep 3 days  (moderate pressure)
 *     Cluster degraded (some nodes offline/unhealthy):
 *       - > 10000:    keep 1 day   (emergency, prevent runaway growth)
 *       - <= 10000:   keep 7 days  (conservative, wait for cluster recovery)
 *
 * Per-scan limit scales with pressure to ensure GC rate can exceed creation rate:
 *   > 10000: 200/cycle  (13/s of lightweight Raft proposals)
 *   > 1000:  50/cycle
 *   <= 1000: 15/cycle
 * Timer fires every 15s, so at maximum 200 × 4/min × 60 = 48k/hour.
 */
static void mndAdaptiveGcTerminalTxns(SMnode *pMnode, int64_t now) {
  SSdb   *pSdb = pMnode->pSdb;
  int32_t total = sdbGetSize(pSdb, SDB_TXN_LOG);

  if (total < MND_TXN_GC_PER_SCAN_MAX) return;

  // Adaptive per-scan limit: scale with pressure so GC can keep up with creation rate.
  // Each mndGcTxnLog creates one lightweight STrans (no VNode actions, just SDB commit-log).
  // At 200/cycle × 4 cycles/min × 60 min = 48k/hour — clears a 10k backlog in ~13 min.
  int32_t gcLimit = MND_TXN_GC_PER_SCAN_MAX;
  if (total > 2000) {
    gcLimit = 200;
  } else if (total > 300) {
    gcLimit = total / 20;
  }

  // Determine age cutoff based on pressure and cluster health.
  int64_t ageCutoffMs;
  if (total < 500) {
    ageCutoffMs = 7LL * 24 * 3600 * 1000;  // 7 days — low pressure, no urgency
  } else if (mndClusterHealthy(pMnode)) {
    // Cluster healthy: all DNodes online + all VGroups have restored leader.
    // VNodes guaranteed to have reported orphan txns; safe to GC aggressively.
    if (total > 2000) {
      ageCutoffMs = 3600LL * 1000;  // 1 hour
    } else {
      ageCutoffMs = 1LL * 24 * 3600 * 1000;  // 1 days
    }
  } else {
    // Cluster degraded — be conservative, but prevent unbounded growth.
    if (total > 10000) {
      ageCutoffMs = 1LL * 24 * 3600 * 1000;  // 1 day — emergency, prevent runaway growth
    } else if (total > 5000) {
      ageCutoffMs = 3LL * 24 * 3600 * 1000;  // 3 days — wait for cluster recovery
    } else {
      ageCutoffMs = 7LL * 24 * 3600 * 1000;  // 7 days — wait for cluster recovery
    }
  }

  void   *pIter = NULL;
  int32_t gcCount = 0;
  while (1) {
    STxnLogObj *pLog = NULL;
    pIter = sdbFetch(pSdb, SDB_TXN_LOG, pIter, (void **)&pLog);
    if (pIter == NULL) break;
    if (pLog->completedAt != 0 && (now - pLog->completedAt) > ageCutoffMs) {
      if (gcCount >= gcLimit) {
        sdbRelease(pSdb, pLog);
        sdbCancelFetch(pSdb, pIter);
        break;  // hit per-cycle cap; continue in next timer cycle (15s)
      }
      int32_t code = mndGcTxnLog(pMnode, pLog);
      if (code == 0 || code == TSDB_CODE_ACTION_IN_PROGRESS) {
        gcCount++;
      }
    }
    sdbRelease(pSdb, pLog);
  }

  if (gcCount > 0) {
    mInfo("adaptive gc: removed %d/%d SDB_TXN_LOG entries (cutoff=%" PRId64 "ms, limit=%d)", gcCount, total,
          ageCutoffMs, gcLimit);
  }
}

// Timeout scan implementation (called periodically via TDMT_MND_TXN_TIMER on the write worker thread)
static void mndTxnTimeoutScanImpl(SMnode *pMnode) {
  SSdb   *pSdb = pMnode->pSdb;
  int64_t now = taosGetTimestampMs();
  void   *pIter = NULL;
  int32_t gcCount = 0;  // GC STrans issued this invocation; capped at MND_TXN_GC_PER_SCAN_MAX

  while (1) {
    STxnObj *pTxn = NULL;
    pIter = sdbFetch(pSdb, SDB_TXN, pIter, (void **)&pTxn);
    if (pIter == NULL) break;

    int64_t elapsed = now - pTxn->lastActiveTime;

    // Clock regression protection: skip if clock moved backward (NTP correction)
    if (elapsed < 0) {
      mDebug("txn:%" PRIi64 ", clock regression detected, elapsed=%" PRId64 "ms, skip timeout check", pTxn->id,
             elapsed);
      sdbRelease(pSdb, pTxn);
      continue;
    }
    mDebug("txn:%" PRIi64 ", stage=%s, elapsed=%" PRId64 "ms, lastActive:%" PRId64 "ms, checking timeout", pTxn->id,
          mndUtxnStageStr(pTxn->stage), elapsed, pTxn->lastActiveTime);

    // Non-replicated COMMITTED/ROLLEDBACK/ZOMBIE entries should not appear in SDB_TXN:
    // they are written to SDB_TXN_LOG and dropped from SDB_TXN at completion time.
    // Legacy entries (from before the SDB_TXN_LOG migration) are GC'd in mndTxnAfterRestored.
    // Adaptive GC of SDB_TXN_LOG is handled by mndAdaptiveGcTerminalTxns() at end of this scan.

    // §43 Two rollback conditions (checked in priority order):
    //   1. Absolute lifetime limit  – total age > tsTxnTimeout (regardless of activity)
    //   2. HB inactivity timeout    – idle since last active HB > TSDB_TXN_HB_TIMEOUT
    if (pTxn->stage == UTXN_STAGE_ACTIVE || pTxn->stage == UTXN_STAGE_PREPARING || pTxn->stage == UTXN_STAGE_ABORTED) {
      int64_t     hbTimeout = (int64_t)TSDB_TXN_HB_TIMEOUT * 1000;
      int64_t     lifetime = now - pTxn->createTime;
      int32_t     rollbackCode = 0;
      const char *rollbackLabel = NULL;
      if (elapsed > hbTimeout) {
        rollbackCode = TSDB_CODE_TXN_TIMEOUT_KILLED;
        rollbackLabel = "inactivity timeout";
        mWarn("txn:%" PRIi64 ", stage=%s, elapsed=%" PRId64 "ms > timeout=%" PRId64 "ms, triggering ROLLBACK", pTxn->id,
              mndUtxnStageStr(pTxn->stage), elapsed, hbTimeout);
      } else if (lifetime > (int64_t)tsTxnTimeout * 1000) {
        rollbackCode = TSDB_CODE_TXN_EXCEEDED_LIFETIME;
        rollbackLabel = "exceeded lifetime";
        mWarn("txn:%" PRIi64 ", stage=%s, lifetime=%" PRId64 "ms > max=%" PRId64
              "ms, triggering ROLLBACK due to exceeded lifetime",
              pTxn->id, mndUtxnStageStr(pTxn->stage), lifetime, (int64_t)tsTxnTimeout * 1000);
      }
      if (rollbackCode != 0) {
        // Build a synthetic SRpcMsg for the rollback (no real client connection)
        SRpcMsg synReq = {0};
        synReq.info.node = pMnode;
        int32_t code = mndRollbackTxn(pMnode, &synReq, pTxn, rollbackCode);
        if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
          mError("txn:%" PRIi64 ", %s rollback failed: %s", pTxn->id, rollbackLabel, tstrerror(code));
        } else {
          mDebug("txn:%" PRIi64 ", %s rollback initiated", pTxn->id, rollbackLabel);
        }
      }
    }

    sdbRelease(pSdb, pTxn);
  }

  // Evict stale entries from the orphan rollback dedup map.
  // An entry is stale when its cooldown has expired — either the VNode stopped reporting
  // (rollback succeeded) or 1 hour has elapsed and a retry is now allowed.
  // taosHashRemove is a lazy delete: it sets removed=1 and decrements refCount without freeing
  // the node while the iterator still holds a reference.  taosHashIterate skips removed nodes
  // and physically frees them in taosHashReleaseNode when advancing — so in-place deletion
  // during iteration is safe; no collect-then-delete needed.
  SHashObj *pOrphanTs = pMnode->txnMgmt.pOrphanRollbackTs;
  if (pOrphanTs != NULL && taosHashGetSize(pOrphanTs) > 0) {
    static const int64_t kOrphanRollbackCooldownMs = 3600LL * 1000;  // must match mndRollbackOrphanTxnOnVnode
    int32_t              nEvicted = 0;
    void                *pIter2 = taosHashIterate(pOrphanTs, NULL);
    while (pIter2 != NULL) {
      int64_t lastMs = *(int64_t *)pIter2;
      if ((now - lastMs) >= kOrphanRollbackCooldownMs) {
        SOrphanRbKey *pKey = taosHashGetKey(pIter2, NULL);
        if (taosHashRemove(pOrphanTs, pKey, sizeof(*pKey)) != 0) {
          mWarn("orphan rollback dedup: failed to remove stale key: txnId=%" PRIi64 ", vgId=%d", pKey->txnId,
                pKey->vgId);
        } else {
          nEvicted++;
        }
      }
      pIter2 = taosHashIterate(pOrphanTs, pIter2);
    }
    if (nEvicted > 0) {
      mDebug("orphan rollback dedup: evicted %d stale entries", nEvicted);
    }
  }

  // Evict stale entries from the mystery-orphan tracking map.
  // An entry is stale when the VNode has stopped reporting it for >1h (resolved or gone away).
  SHashObj *pOrphanMap = pMnode->txnMgmt.pOrphanTxnMap;
  if (pOrphanMap != NULL && taosHashGetSize(pOrphanMap) > 0) {
    static const int64_t kOrphanMapTtlMs = 3600LL * 1000;
    int32_t              nOrphanEvicted = 0;
    void                *pIter3 = taosHashIterate(pOrphanMap, NULL);
    while (pIter3 != NULL) {
      SOrphanTxnEntry *p = (SOrphanTxnEntry *)pIter3;
      if ((now - p->lastSeen) >= kOrphanMapTtlMs) {
        SOrphanRbKey evictKey = {.txnId = p->txnId, .vgId = p->vgId, ._pad = 0};
        if (taosHashRemove(pOrphanMap, &evictKey, sizeof(evictKey)) != 0) {
          mWarn("orphan txn map: failed to remove stale key: txnId=%" PRIi64 ", vgId=%d", p->txnId, p->vgId);
        } else {
          nOrphanEvicted++;
        }
      }
      pIter3 = taosHashIterate(pOrphanMap, pIter3);
    }
    if (nOrphanEvicted > 0) {
      mDebug("orphan txn map: evicted %d stale entries", nOrphanEvicted);
    }
  }

  mndAdaptiveGcTerminalTxns(pMnode, now);
}

// Invoked periodically from mndMain.c to trigger the timeout scan.
void mndTxnDoTimeoutScan(SMnode *pMnode) { mndTxnTimeoutScanImpl(pMnode); }

// Timer message handler: called on the MNode write worker thread, ensuring SDB writes
// run in the correct thread context.
static int32_t mndProcessTxnTimerReq(SRpcMsg *pReq) {
  mTrace("txn, processing timeout scan timer");
  mndTxnTimeoutScanImpl(pReq->info.node);
  return 0;
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

/**
 * Repopulate pStbConflictMap from a txn's pShadowOps after leader restore.
 * Must be called after pTxn->pShadowOps is populated; does nothing if NULL.
 */
static int32_t mndTxnRepopulateConflictMap(SMnode *pMnode, STxnObj *pTxn) {
  SHashObj *pConflictMap = pMnode->txnMgmt.pStbConflictMap;
  if (pConflictMap == NULL || pTxn->pShadowOps == NULL) return 0;

  int32_t sz = taosArrayGetSize(pTxn->pShadowOps);
  for (int32_t i = 0; i < sz; i++) {
    SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);
    size_t        nameLen = strnlen(pOp->name, TSDB_TABLE_FNAME_LEN);
    if (nameLen == 0) continue;
    // Only insert if no other txn owns it yet (first-write wins, same as mndTxnAddShadowOp).
    txn_id_t *pOwner = (txn_id_t *)taosHashGet(pConflictMap, pOp->name, nameLen);
    if (pOwner == NULL) {
      txn_id_t myId = pTxn->id;
      if (taosHashPut(pConflictMap, pOp->name, nameLen, &myId, sizeof(myId)) != 0) {
        mError("txn:%" PRIi64 ", failed to repopulate conflict map for stb:%s, conflict detection broken", pTxn->id,
               pOp->name);
        return TSDB_CODE_OUT_OF_MEMORY;
      }
    }
  }
  mDebug("txn:%" PRIi64 ", repopulated %d entries into stb conflict map", pTxn->id, sz);
  return 0;
}

static int32_t mndTxnAfterRestored(SMnode *pMnode) {
  SSdb   *pSdb = pMnode->pSdb;
  void   *pIter = NULL;
  int32_t numRecovered = 0;
  int32_t activeCnt = 0;

  mInfo("txn, scanning SDB for in-flight transactions after leader restore");

  // Phase 1: Scan SDB_TXN, collect ACTIVE txns (hold reference), handle others normally.
  SArray *pActiveTxns = taosArrayInit(4, sizeof(STxnObj *));

  while (1) {
    STxnObj *pTxn = NULL;
    pIter = sdbFetch(pSdb, SDB_TXN, pIter, (void **)&pTxn);
    if (pIter == NULL) break;

    SRpcMsg synReq = {0};
    synReq.info.node = pMnode;

    switch (pTxn->stage) {
      case UTXN_STAGE_ACTIVE:
      case UTXN_STAGE_PREPARING: {
        mInfo("txn:%" PRIi64 ", restored in ACTIVE stage, queued for batch shadow rebuild", pTxn->id);
        activeCnt++;
        if (pActiveTxns && pTxn->pShadowOps == NULL) {
          taosArrayPush(pActiveTxns, &pTxn);
          // Do NOT sdbRelease here — keep reference alive for Phase 2
          numRecovered++;
          continue;  // skip sdbRelease at bottom
        }
        numRecovered++;
        break;
      }
      case UTXN_STAGE_COMMITTING: {
        mInfo("txn:%" PRIi64 ", restored in COMMITTING stage, original STrans will be retried by mndTransPullup",
              pTxn->id);
        numRecovered++;
        break;
      }
      case UTXN_STAGE_ROLLINGBACK: {
        mInfo("txn:%" PRIi64 ", restored in ROLLINGBACK stage, original STrans will be retried by mndTransPullup",
              pTxn->id);
        numRecovered++;
        break;
      }
      case UTXN_STAGE_COMMITTED:
      case UTXN_STAGE_ROLLEDBACK: {
        // Terminal entries in SDB_TXN are legacy data from before the SDB_TXN_LOG migration.
        // New txns write compact records to SDB_TXN_LOG and drop from SDB_TXN immediately.
        // GC these legacy entries now; mndGetOrphanTxnAction falls back to SKIP for them (no SDB_TXN_LOG entry).
        mInfo("txn:%" PRIi64 ", restored in %s stage (legacy SDB_TXN entry), scheduling GC", pTxn->id,
              mndUtxnStageStr(pTxn->stage));
        int32_t gcCode = mndGcCompletedTxn(pMnode, pTxn);
        if (gcCode != 0 && gcCode != TSDB_CODE_ACTION_IN_PROGRESS) {
          mError("txn:%" PRIi64 ", restore-time legacy GC failed: %s", pTxn->id, tstrerror(gcCode));
        }
        numRecovered++;
        break;
      }
      default:
        break;
    }
    sdbRelease(pSdb, pTxn);
  }

  // Phase 2: Batch rebuild shadow ops for all ACTIVE txns.
  // Single SDB_STB scan dispatches entries by txnId — O(num_stbs) total regardless of N.
  int32_t numActive = pActiveTxns ? (int32_t)taosArrayGetSize(pActiveTxns) : 0;
  if (numActive == 1) {
    // Fast path: single txn, use existing function directly (no hash overhead)
    STxnObj *pTxn = *(STxnObj **)taosArrayGet(pActiveTxns, 0);
    int32_t  rebuildCode = mndTxnRebuildShadowOpsFromSdb(pMnode, pTxn, true);
    if (rebuildCode != 0) {
      mError("txn:%" PRIi64 ", failed to rebuild shadow ops: %s; marking txn ABORTED", pTxn->id,
             tstrerror(rebuildCode));
      int32_t markCode = mndMarkTxnAbortedInSdb(pMnode, pTxn);
      if (markCode != 0 && markCode != TSDB_CODE_ACTION_IN_PROGRESS) {
        mError("txn:%" PRIi64 ", failed to mark txn ABORTED in SDB: %s", pTxn->id, tstrerror(markCode));
      }
    } else {
      int32_t repopCode = mndTxnRepopulateConflictMap(pMnode, pTxn);
      if (repopCode != 0) {
        mError("txn:%" PRIi64 ", repopulate conflict map failed: %s; marking txn ABORTED", pTxn->id,
               tstrerror(repopCode));
        int32_t markCode = mndMarkTxnAbortedInSdb(pMnode, pTxn);
        if (markCode != 0 && markCode != TSDB_CODE_ACTION_IN_PROGRESS) {
          mError("txn:%" PRIi64 ", failed to mark txn ABORTED in SDB: %s", pTxn->id, tstrerror(markCode));
        }
      } else {
        taosWLockLatch(&pTxn->lock);
        pTxn->lastActiveTime = taosGetTimestampMs();
        taosWUnLockLatch(&pTxn->lock);
      }
    }
    sdbRelease(pSdb, pTxn);
  } else if (numActive > 1) {
    // Batch path: build txnId→index map, single SDB_STB scan, dispatch by hash lookup
    SHashObj *pIdxMap =
        taosHashInit(numActive * 2, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
    if (pIdxMap == NULL) {
      mError("txn batch rebuild: failed to alloc hash map, falling back to per-txn rebuild");
      for (int32_t i = 0; i < numActive; i++) {
        STxnObj *pTxn = *(STxnObj **)taosArrayGet(pActiveTxns, i);
        int32_t  rebuildCode = mndTxnRebuildShadowOpsFromSdb(pMnode, pTxn, true);
        if (rebuildCode == 0) {
          int32_t repopCode = mndTxnRepopulateConflictMap(pMnode, pTxn);
          if (repopCode != 0) {
            mError("txn:%" PRIi64 ", repopulate conflict map failed: %s; marking txn ABORTED", pTxn->id,
                   tstrerror(repopCode));
            int32_t markCode = mndMarkTxnAbortedInSdb(pMnode, pTxn);
            if (markCode != 0 && markCode != TSDB_CODE_ACTION_IN_PROGRESS) {
              mError("txn:%" PRIi64 ", failed to mark txn ABORTED in SDB: %s", pTxn->id, tstrerror(markCode));
            }
          } else {
            taosWLockLatch(&pTxn->lock);
            pTxn->lastActiveTime = taosGetTimestampMs();
            taosWUnLockLatch(&pTxn->lock);
          }
        } else {
          mError("txn:%" PRIi64 ", failed to rebuild shadow ops: %s; marking txn ABORTED", pTxn->id,
                 tstrerror(rebuildCode));
          int32_t markCode = mndMarkTxnAbortedInSdb(pMnode, pTxn);
          if (markCode != 0 && markCode != TSDB_CODE_ACTION_IN_PROGRESS) {
            mError("txn:%" PRIi64 ", failed to mark txn ABORTED in SDB: %s", pTxn->id, tstrerror(markCode));
          }
        }
        sdbRelease(pSdb, pTxn);
      }
    } else {
      // Per-txn temp arrays for collecting shadow ops
      SArray **pTmpOpsArr = taosMemoryCalloc(numActive, sizeof(SArray *));
      bool     batchFailed = (pTmpOpsArr == NULL);

      if (!batchFailed) {
        for (int32_t i = 0; i < numActive; i++) {
          STxnObj *pTxn = *(STxnObj **)taosArrayGet(pActiveTxns, i);
          if (taosHashPut(pIdxMap, &pTxn->id, sizeof(pTxn->id), &i, sizeof(i)) != 0) {
            mError("txn batch restore: failed to build index map for txn:%" PRIi64 ", falling back to per-txn rebuild",
                   pTxn->id);
            batchFailed = true;
            break;
          }
        }

        // Single SDB_STB scan
        void    *pStbIter = NULL;
        SStbObj *pStb = NULL;
        while (1) {
          pStbIter = sdbFetch(pSdb, SDB_STB, pStbIter, (void **)&pStb);
          if (pStbIter == NULL) break;

          if (pStb->txnId == 0) {
            sdbRelease(pSdb, pStb);
            continue;
          }

          txn_id_t stbTxnId = (txn_id_t)pStb->txnId;
          int32_t *pIdx = taosHashGet(pIdxMap, &stbTxnId, sizeof(stbTxnId));
          if (pIdx == NULL) {
            sdbRelease(pSdb, pStb);
            continue;
          }

          int32_t  idx = *pIdx;
          STxnObj *pTxn = *(STxnObj **)taosArrayGet(pActiveTxns, idx);
          if (pTmpOpsArr[idx] == NULL) {
            pTmpOpsArr[idx] = taosArrayInit(4, sizeof(SMndShadowOp));
            if (pTmpOpsArr[idx] == NULL) {
              sdbRelease(pSdb, pStb);
              sdbCancelFetch(pSdb, pStbIter);
              batchFailed = true;
              break;
            }
          }
          SArray *pOps = pTmpOpsArr[idx];

          // CREATE_STB
          if (pStb->txnStatus == META_TXN_PRE_CREATE || pStb->txnStatus == META_TXN_PRE_CREATE_DROP) {
            SMndShadowOp op = {0};
            op.opType = MND_SHADOW_OP_CREATE_STB;
            op.uid = pStb->uid;
            tstrncpy(op.name, pStb->name, sizeof(op.name));
            tstrncpy(op.db, pStb->db, sizeof(op.db));
            if (taosArrayPush(pOps, &op) == NULL) {
              sdbRelease(pSdb, pStb);
              sdbCancelFetch(pSdb, pStbIter);
              batchFailed = true;
              break;
            }
            mInfo("txn:%" PRIi64 ", rebuilt CREATE_STB shadow op: stb=%s uid=%" PRId64, pTxn->id, pStb->name,
                  pStb->uid);
          }

          // DROP_STB
          if (!batchFailed && (pStb->txnStatus == META_TXN_PRE_DROP || pStb->txnStatus == META_TXN_PRE_CREATE_DROP)) {
            SMndShadowOp op = {0};
            op.opType = MND_SHADOW_OP_DROP_STB;
            op.uid = pStb->uid;
            tstrncpy(op.name, pStb->name, sizeof(op.name));
            tstrncpy(op.db, pStb->db, sizeof(op.db));
            if (taosArrayPush(pOps, &op) == NULL) {
              sdbRelease(pSdb, pStb);
              sdbCancelFetch(pSdb, pStbIter);
              batchFailed = true;
              break;
            }
            mInfo("txn:%" PRIi64 ", rebuilt DROP_STB shadow op: stb=%s uid=%" PRId64, pTxn->id, pStb->name, pStb->uid);
          }

          // ALTER_STB chain
          if (!batchFailed && pStb->txnAlterReqsLen > (int32_t)sizeof(int32_t) && pStb->pTxnAlterReqs != NULL) {
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
                sdbCancelFetch(pSdb, pStbIter);
                batchFailed = true;
                break;
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
              if (taosArrayPush(pOps, &op) == NULL) {
                taosMemoryFree(pData);
                sdbRelease(pSdb, pStb);
                sdbCancelFetch(pSdb, pStbIter);
                batchFailed = true;
                break;
              }
              mInfo("txn:%" PRIi64 ", rebuilt ALTER_STB shadow op %d/%d: stb=%s", pTxn->id, j + 1, numEntries,
                    pStb->name);
            }
            if (batchFailed) break;
          }

          sdbRelease(pSdb, pStb);
        }
      }

      // Commit or rollback batch results
      int64_t now = taosGetTimestampMs();
      for (int32_t i = 0; i < numActive; i++) {
        STxnObj *pTxn = *(STxnObj **)taosArrayGet(pActiveTxns, i);
        if (!batchFailed && pTmpOpsArr && pTmpOpsArr[i] != NULL) {
          pTxn->pShadowOps = pTmpOpsArr[i];
          pTmpOpsArr[i] = NULL;  // ownership transferred
          mInfo("txn:%" PRIi64 ", batch rebuilt %d shadow ops", pTxn->id, (int32_t)taosArrayGetSize(pTxn->pShadowOps));
          int32_t repopCode = mndTxnRepopulateConflictMap(pMnode, pTxn);
          if (repopCode != 0) {
            mError("txn:%" PRIi64 ", repopulate conflict map failed: %s; marking txn ABORTED", pTxn->id,
                   tstrerror(repopCode));
            int32_t markCode = mndMarkTxnAbortedInSdb(pMnode, pTxn);
            if (markCode != 0 && markCode != TSDB_CODE_ACTION_IN_PROGRESS) {
              mError("txn:%" PRIi64 ", failed to mark txn ABORTED in SDB: %s", pTxn->id, tstrerror(markCode));
            }
          } else {
            taosWLockLatch(&pTxn->lock);
            pTxn->lastActiveTime = now;
            taosWUnLockLatch(&pTxn->lock);
          }
        } else if (batchFailed) {
          mError("txn:%" PRIi64 ", batch rebuild failed; marking txn ABORTED", pTxn->id);
          int32_t markCode = mndMarkTxnAbortedInSdb(pMnode, pTxn);
          if (markCode != 0 && markCode != TSDB_CODE_ACTION_IN_PROGRESS) {
            mError("txn:%" PRIi64 ", failed to mark txn ABORTED in SDB: %s", pTxn->id, tstrerror(markCode));
          }
        }
        sdbRelease(pSdb, pTxn);
      }

      // Cleanup on failure: free partially-built temp arrays
      if (batchFailed && pTmpOpsArr) {
        for (int32_t i = 0; i < numActive; i++) {
          if (pTmpOpsArr[i] != NULL) {
            int32_t n = taosArrayGetSize(pTmpOpsArr[i]);
            for (int32_t j = 0; j < n; j++) {
              SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTmpOpsArr[i], j);
              taosMemoryFreeClear(pOp->pReqData);
            }
            taosArrayDestroy(pTmpOpsArr[i]);
          }
        }
      }
      taosMemoryFreeClear(pTmpOpsArr);
      taosHashCleanup(pIdxMap);
    }
  }
  taosArrayDestroy(pActiveTxns);

  mInfo("txn, leader restore scan complete, recovered %d transactions, activeTxnCnt reset to %d", numRecovered,
        activeCnt);
  atomic_store_32(&pMnode->txnMgmt.activeTxnCnt, activeCnt);
  return 0;
}

void mndCleanupTxn(SMnode *pMnode) {
  STxnMgmt *pMgmt = &pMnode->txnMgmt;
  if (pMgmt->pOrphanRollbackTs) {
    taosHashCleanup(pMgmt->pOrphanRollbackTs);
    pMgmt->pOrphanRollbackTs = NULL;
  }
  if (pMgmt->pOrphanTxnMap) {
    taosHashCleanup(pMgmt->pOrphanTxnMap);
    pMgmt->pOrphanTxnMap = NULL;
  }
  if (pMgmt->pStbConflictMap) {
    taosHashCleanup(pMgmt->pStbConflictMap);
    pMgmt->pStbConflictMap = NULL;
  }
}

// Human-readable name for the MNode-side user transaction stage (used in log messages).
const char *mndUtxnStageStr(EUtxnStage stage) {
  switch (stage) {
    case UTXN_STAGE_IDLE:
      return "IDLE";
    case UTXN_STAGE_ACTIVE:
      return "ACTIVE";
    case UTXN_STAGE_ABORTED:
      return "ABORTED";
    case UTXN_STAGE_PREPARING:
      return "PREPARING";
    case UTXN_STAGE_COMMITTING:
      return "COMMITTING";
    case UTXN_STAGE_ROLLINGBACK:
      return "ROLLINGBACK";
    case UTXN_STAGE_COMMITTED:
      return "COMMITTED";
    case UTXN_STAGE_ROLLEDBACK:
      return "ROLLEDBACK";
    default:
      return "UNKNOWN";
  }
}

// Human-readable name for the VNode-side transaction stage (used in log messages).
const char *mndVtxnStageStr(EVtxnStage stage) {
  switch (stage) {
    case VTXN_STAGE_NONE:
      return "NONE";
    case VTXN_STAGE_ACTIVE:
      return "ACTIVE";
    case VTXN_STAGE_PREPARED:
      return "PREPARED";
    case VTXN_STAGE_FINISHING:
      return "FINISHING";
    default:
      return "UNKNOWN";
  }
}

void mndTxnFreeObj(STxnObj *pObj) {
  if (pObj) {
    if (pObj->pVgList) {
      tSimpleHashCleanup(pObj->pVgList);
      pObj->pVgList = NULL;
    }
    if (pObj->pDbList) {
      taosArrayDestroy(pObj->pDbList);
      pObj->pDbList = NULL;
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

  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->id));
  TAOS_CHECK_EXIT(tEncodeCStr(&encoder, pObj->createUser));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->ownerId));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->createTime));
  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->term));  // encode Raft term
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, pObj->stage));

  // NOTE: pShadowOps is NOT serialised — it is a runtime-only field rebuilt from SStbObj.txnId.

  TAOS_CHECK_EXIT(tEncodeI64v(&encoder, pObj->completedAt));

  // Serialise pDbList (participating DB fullNames for replicated txn VGroup resolution).
  int32_t dbNum = pObj->pDbList ? taosArrayGetSize(pObj->pDbList) : 0;
  TAOS_CHECK_EXIT(tEncodeI32v(&encoder, dbNum));
  for (int32_t i = 0; i < dbNum; ++i) {
    char *dbFName = taosArrayGet(pObj->pDbList, i);
    TAOS_CHECK_EXIT(tEncodeCStr(&encoder, dbFName));
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

static int32_t tDeserializeSTxnObj(void *buf, int32_t bufLen, STxnObj *pObj, int8_t sver) {
  int32_t  code = 0, lino = 0;
  SDecoder decoder = {0};
  tDecoderInit(&decoder, buf, bufLen);

  TAOS_CHECK_EXIT(tStartDecode(&decoder));

  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->id));
  TAOS_CHECK_EXIT(tDecodeCStrTo(&decoder, pObj->createUser));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->ownerId));
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->createTime));
  pObj->lastActiveTime = taosGetTimestampMs();          // initialize lastActiveTime to now on deserialization
  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->term));  // decode Raft term
  TAOS_CHECK_EXIT(tDecodeI32v(&decoder, (int32_t *)&pObj->stage));

  pObj->pShadowOps = NULL;  // runtime-only; rebuilt from SDB on restart

  TAOS_CHECK_EXIT(tDecodeI64v(&decoder, &pObj->completedAt));

  // Deserialise pDbList (backward-compatible: only present in newer versions).
  pObj->pDbList = NULL;
  if (!tDecodeIsEnd(&decoder)) {
    int32_t dbNum = 0;
    TAOS_CHECK_EXIT(tDecodeI32v(&decoder, &dbNum));
    if (dbNum > 0) {
      pObj->pDbList = taosArrayInit(dbNum, TSDB_DB_FNAME_LEN);
      if (pObj->pDbList == NULL) {
        TAOS_CHECK_EXIT(terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY);
      }
      for (int32_t i = 0; i < dbNum; ++i) {
        char *pStr = NULL;
        TAOS_CHECK_EXIT(tDecodeCStr(&decoder, &pStr));
        char dbFName[TSDB_DB_FNAME_LEN] = {0};
        tstrncpy(dbFName, pStr, TSDB_DB_FNAME_LEN);
        if (taosArrayPush(pObj->pDbList, dbFName) == NULL) {
          TAOS_CHECK_EXIT(terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY);
        }
      }
    }
  }

_exit:
  tEndDecode(&decoder);
  tDecoderClear(&decoder);
  if (code < 0) {
    mError("txn, %s failed at line %d since %s, row:%p", __func__, lino, tstrerror(code), pObj);
    if (pObj->pDbList) {
      taosArrayDestroy(pObj->pDbList);
      pObj->pDbList = NULL;
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

  if (sver < 1 || sver > MND_TXN_VER_NUMBER) {
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

  TAOS_CHECK_EXIT(tDeserializeSTxnObj(buf, tlen, pObj, sver));

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
  mTrace("txn:%" PRIi64 ", perform insert action, row:%p", pObj->id, pObj);
  // activeTxnCnt is pre-incremented by mndProcessBeginTxnReq at request time to prevent
  // TOCTOU races under concurrent BEGIN requests (the counter must be updated before Raft
  // commits, not after).  mndTxnAfterRestored sets the counter from actual SDB state on
  // leader restore.  Do NOT increment here to avoid double-counting.
  return 0;
}

static int32_t mndTxnActionDelete(SSdb *pSdb, STxnObj *pObj) {
  mTrace("txn:%" PRIi64 ", perform delete action, row:%p", pObj->id, pObj);
  // Guard: if a txn is dropped while still ACTIVE (abnormal path), keep counter consistent.
  if (pObj->stage == UTXN_STAGE_ACTIVE) {
    atomic_sub_fetch_32(&pSdb->pMnode->txnMgmt.activeTxnCnt, 1);
  }
  // Remove any shadow-op stb entries this txn owns from the global conflict map.
  SHashObj *pConflictMap = pSdb->pMnode->txnMgmt.pStbConflictMap;
  if (pConflictMap != NULL && pObj->pShadowOps != NULL) {
    int32_t sz = taosArrayGetSize(pObj->pShadowOps);
    for (int32_t i = 0; i < sz; i++) {
      SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pObj->pShadowOps, i);
      txn_id_t *pOwner = taosHashGet(pConflictMap, pOp->name, strnlen(pOp->name, TSDB_TABLE_FNAME_LEN));
      if (pOwner != NULL && *pOwner == pObj->id) {
        int32_t rmCode = taosHashRemove(pConflictMap, pOp->name, strnlen(pOp->name, TSDB_TABLE_FNAME_LEN));
        if (rmCode != 0) {
          mWarn("txn:%" PRIi64 ", failed to remove stb conflict map entry for stb:%s, code:0x%x",
                pObj->id, pOp->name, rmCode);
        }
      }
    }
  }
  mndTxnFreeObj(pObj);
  return 0;
}

static int32_t mndTxnActionUpdate(SSdb *pSdb, STxnObj *pOld, STxnObj *pNew) {
  mTrace("txn:%" PRIi64 ", perform update action, old row:%p new row:%p", pOld->id, pOld, pNew);
  taosWLockLatch(&pOld->lock);
  int8_t oldStage = pOld->stage;
  pOld->stage = pNew->stage;
  pOld->lastActiveTime = pNew->lastActiveTime;
  pOld->completedAt = pNew->completedAt;
  taosWUnLockLatch(&pOld->lock);
  // Maintain activeTxnCnt when a txn transitions through ACTIVE stage.
  // Normal path: ACTIVE → COMMITTING/ROLLINGBACK (decrement).
  if (oldStage == UTXN_STAGE_ACTIVE && pNew->stage != UTXN_STAGE_ACTIVE) {
    atomic_sub_fetch_32(&pSdb->pMnode->txnMgmt.activeTxnCnt, 1);
  } else if (oldStage != UTXN_STAGE_ACTIVE && pNew->stage == UTXN_STAGE_ACTIVE) {
    atomic_add_fetch_32(&pSdb->pMnode->txnMgmt.activeTxnCnt, 1);
  }
  // When a txn is marked ABORTED, remove its entries from the global stb conflict map.
  // mndTxnActionDelete handles the normal terminal path (COMMITTED/ROLLEDBACK), but ABORTED
  // is a special state where the txn still exists in SDB pending timeout-triggered rollback.
  // Without this cleanup, mndTxnCheckStbConflict would keep seeing a stale conflict entry
  // for the aborted txn and return TSDB_CODE_TXN_RESOURCE_BUSY to new DDL operations.
  if (pNew->stage == UTXN_STAGE_ABORTED && oldStage != UTXN_STAGE_ABORTED) {
    SHashObj *pConflictMap = pSdb->pMnode->txnMgmt.pStbConflictMap;
    if (pConflictMap != NULL && pOld->pShadowOps != NULL) {
      int32_t sz = taosArrayGetSize(pOld->pShadowOps);
      for (int32_t i = 0; i < sz; i++) {
        SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pOld->pShadowOps, i);
        size_t        nameLen = strnlen(pOp->name, TSDB_TABLE_FNAME_LEN);
        txn_id_t     *pOwner = taosHashGet(pConflictMap, pOp->name, nameLen);
        if (pOwner != NULL && *pOwner == pOld->id) {
          if (taosHashRemove(pConflictMap, pOp->name, nameLen) != 0) {
            mWarn("txn:%" PRIi64 ", failed to remove conflict map entry for stb:%s on ABORTED", pOld->id, pOp->name);
          }
        }
      }
    }
  }
  return 0;
}

// ─── SDB_TXN_LOG: compact persistent log of terminal non-replicated user transactions ──────────

static SSdbRaw *mndTxnLogActionEncode(STxnLogObj *pLog) {
  int32_t code = 0, lino = 0;
  // Total = 8(id) + 1(stage) + 1(rollbackReason) + 8(completedAt) + 24(createUser) + 8(createTime) + 32(reserve) = 82 bytes
  int32_t  size = sizeof(txn_id_t) + sizeof(int8_t) + sizeof(int64_t) + TSDB_USER_LEN + sizeof(int64_t) +
                  sizeof(int8_t) + MND_TXN_LOG_RESERVE_SIZE;
  SSdbRaw *pRaw = sdbAllocRaw(SDB_TXN_LOG, MND_TXN_LOG_VER_NUMBER, size);
  if (pRaw == NULL) return NULL;

  int32_t dataPos = 0;
  SDB_SET_INT64(pRaw, dataPos, pLog->id, _exit)
  SDB_SET_INT8(pRaw, dataPos, pLog->stage, _exit)
  SDB_SET_INT8(pRaw, dataPos, pLog->rollbackReason, _exit)
  SDB_SET_INT64(pRaw, dataPos, pLog->completedAt, _exit)
  SDB_SET_BINARY(pRaw, dataPos, pLog->createUser, TSDB_USER_LEN, _exit)
  SDB_SET_INT64(pRaw, dataPos, pLog->createTime, _exit)
  SDB_SET_RESERVE(pRaw, dataPos, MND_TXN_LOG_RESERVE_SIZE, _exit)
  SDB_SET_DATALEN(pRaw, dataPos, _exit)
  return pRaw;

_exit:
  sdbFreeRaw(pRaw);
  return NULL;
}

static SSdbRow *mndTxnLogActionDecode(SSdbRaw *pRaw) {
  int8_t sver = 0;
  if (sdbGetRawSoftVer(pRaw, &sver) != 0) return NULL;
  // v1 is the only defined version.  Reject unknown future versions.
  if (sver != MND_TXN_LOG_VER_NUMBER) {
    terrno = TSDB_CODE_SDB_INVALID_DATA_VER;
    return NULL;
  }

  SSdbRow    *pRow = sdbAllocRow(sizeof(STxnLogObj));
  STxnLogObj *pLog = sdbGetRowObj(pRow);
  if (pLog == NULL) return NULL;

  int32_t code = 0, lino = 0;
  int32_t dataPos = 0;
  SDB_GET_INT64(pRaw, dataPos, &pLog->id, _exit)
  SDB_GET_INT8(pRaw, dataPos, &pLog->stage, _exit)
  SDB_GET_INT8(pRaw, dataPos, &pLog->rollbackReason, _exit)
  SDB_GET_INT64(pRaw, dataPos, &pLog->completedAt, _exit)
  SDB_GET_BINARY(pRaw, dataPos, pLog->createUser, TSDB_USER_LEN, _exit)
  SDB_GET_INT64(pRaw, dataPos, &pLog->createTime, _exit)
  SDB_GET_RESERVE(pRaw, dataPos, MND_TXN_LOG_RESERVE_SIZE, _exit)
  return pRow;

_exit:
  taosMemoryFreeClear(pRow);
  return NULL;
}

static int32_t mndTxnLogActionInsert(SSdb *pSdb, STxnLogObj *pLog) {
  mTrace("txn_log:%" PRIi64 ", perform insert action, stage=%d, completedAt=%" PRId64, pLog->id, pLog->stage,
         pLog->completedAt);
  return 0;
}

static int32_t mndTxnLogActionDelete(SSdb *pSdb, STxnLogObj *pLog) {
  mTrace("txn_log:%" PRIi64 ", perform delete action", pLog->id);
  return 0;
}

/**
 * GC a completed STxnLogObj from SDB_TXN_LOG by creating a minimal drop-only STrans.
 */
static int32_t mndGcTxnLog(SMnode *pMnode, STxnLogObj *pLog) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;

  SRpcMsg synReq = {0};
  synReq.info.node = pMnode;

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, &synReq, "gc-txnlog")), code,
                  lino, _exit, terrno);
  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);

  {
    SSdbRaw *pDropRaw = mndTxnLogActionEncode(pLog);
    if (pDropRaw == NULL) {
      TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL);
    }
    TAOS_CHECK_EXIT(mndTransAppendCommitlog(pTrans, pDropRaw));
    TAOS_CHECK_EXIT(sdbSetRawStatus(pDropRaw, SDB_STATUS_DROPPED));
  }

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));

_exit:
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

STxnObj *mndAcquireTxn(SMnode *pMnode, txn_id_t id) {
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
      mFatal("txn:%" PRIi64 ", failed to acquire txn since %s", id, terrstr());
    }
  }
  return pObj;
}

void mndReleaseTxn(SMnode *pMnode, STxnObj *pTxn) {
  SSdb *pSdb = pMnode->pSdb;
  sdbRelease(pSdb, pTxn);
}

const char *mndTxnStr(EUtxnStage stage) { return mndUtxnStageStr(stage); }

// Check if a specific UTXN is alive (exists and in active/preparing/committing/rollingback stage).
// Returns 1 if MNode already owns the txn lifecycle (timeout scanner should NOT create a second
// mndRollbackTxn call), 0 if dead/unknown.
//
// NOTE: UTXN_STAGE_ROLLINGBACK is marked alive here to prevent the timeout scanner from calling
// mndRollbackTxn a second time while the existing rollback STrans is still in-flight.
// This is SEPARATE from mndGetOrphanTxnAction, which correctly returns ROLLBACK during ROLLINGBACK
// so that VNodes not in pVgList can be rolled back via the orphan path.
int8_t mndTxnIsAlive(SMnode *pMnode, txn_id_t txnId) {
  STxnObj *pTxn = mndAcquireTxn(pMnode, txnId);
  if (pTxn == NULL) return 0;

  int8_t alive = 0;
  switch (pTxn->stage) {
    case UTXN_STAGE_ACTIVE:
    case UTXN_STAGE_PREPARING:
    case UTXN_STAGE_COMMITTING:
    case UTXN_STAGE_ROLLINGBACK:  // rollback in-flight — timeout scanner must not create a second one
      alive = 1;
      break;
    default:
      alive = 0;
      break;
  }
  mndReleaseTxn(pMnode, pTxn);
  return alive;
}

// Determine what MNode should tell a VNode that is reporting an idle (30+ min
// ACTIVE) txn via the keepalive query.  Acquires the txn from SDB once so all
// stage checks use the same snapshot.
//
// ORPHAN_TXN_ACTION_COMMIT   – MNode committed; VNode missed the COMMIT message,
//                              re-deliver it so VNode can promote shadow data.
// ORPHAN_TXN_ACTION_ROLLBACK – MNode rolled back (including ROLLINGBACK in-flight),
//                              txn is a ZOMBIE, or not found (GC'd); VNode must
//                              discard its shadow data.
//                              ROLLINGBACK maps to ROLLBACK because: the rollback
//                              decision is final, the main STrans covers VNodes in
//                              pVgList, and VNodes NOT in pVgList must learn about
//                              the rollback via this orphan path.  mndRollbackOrphan-
//                              TxnOnVnode's 1-hour dedup prevents redundant STrans
//                              for VNodes already being rolled back by the main STrans.
// ORPHAN_TXN_ACTION_SKIP     – txn is still in-progress (ACTIVE/PREPARING/COMMITTING);
//                              MNode owns the lifecycle, VNode should keep waiting.
EOrphanTxnAction mndGetOrphanTxnAction(SMnode *pMnode, txn_id_t txnId) {
  STxnObj *pTxn = mndAcquireTxn(pMnode, txnId);
  if (pTxn == NULL) {
    // SDB_TXN record not found.  Check SDB_TXN_LOG: non-replicated txns that completed are
    // dropped from SDB_TXN and a compact record is written to SDB_TXN_LOG.
    SSdb       *pSdb = pMnode->pSdb;
    STxnLogObj *pLog = sdbAcquire(pSdb, SDB_TXN_LOG, &txnId);
    if (pLog != NULL) {
      EOrphanTxnAction action =
          (pLog->stage == UTXN_STAGE_COMMITTED) ? ORPHAN_TXN_ACTION_COMMIT : ORPHAN_TXN_ACTION_ROLLBACK;
      mDebug("txn:%" PRIi64 ", found in SDB_TXN_LOG (stage=%d), returning %s", txnId, pLog->stage,
             action == ORPHAN_TXN_ACTION_COMMIT ? "COMMIT" : "ROLLBACK");
      sdbRelease(pSdb, pLog);
      return action;
    }
    // Not found in either SDB_TXN or SDB_TXN_LOG:
    //   (a) record was GC'd after adaptive TTL
    //   (b) txnId is invalid / never existed
    // Use SKIP_UNKNOWN: preserve PRE_* shadow data for manual recovery; caller records this orphan for visibility.
    mWarn("txn:%" PRIi64 ", not found in SDB_TXN or SDB_TXN_LOG (GC'd or invalid), returning SKIP_UNKNOWN", txnId);
    return ORPHAN_TXN_ACTION_SKIP_UNKNOWN;
  }

  EOrphanTxnAction action;
  switch (pTxn->stage) {
    case UTXN_STAGE_COMMITTED:
      action = ORPHAN_TXN_ACTION_COMMIT;
      break;
    case UTXN_STAGE_ROLLEDBACK:
    case UTXN_STAGE_ROLLINGBACK:  // rollback decision is final; VNodes not in pVgList need this
      action = ORPHAN_TXN_ACTION_ROLLBACK;
      break;
    default:  // ACTIVE / PREPARING / COMMITTING
      action = ORPHAN_TXN_ACTION_SKIP;
      break;
  }
  mndReleaseTxn(pMnode, pTxn);
  return action;
}

void mndTxnRefreshKeepalive(SMnode *pMnode, txn_id_t txnId) {
  STxnObj *pTxn = mndAcquireTxn(pMnode, txnId);
  if (pTxn == NULL) return;

  if (pTxn->stage == UTXN_STAGE_ACTIVE) {
    taosWLockLatch(&pTxn->lock);
    pTxn->lastActiveTime = taosGetTimestampMs();
    taosWUnLockLatch(&pTxn->lock);
    mTrace("txn:%" PRIi64 ", keepalive refreshed via client HB", txnId);
  }
  mndReleaseTxn(pMnode, pTxn);
}

/**
 * Returns true if the given txnId was forcibly rolled back by the MNode due to inactivity timeout
 * or exceeded lifetime (i.e. rollbackReason != TXN_ROLLBACK_EXPLICIT in SDB_TXN_LOG).
 * Used by the HB handler to send HEARTBEAT_KEY_TXN_KILLED to the client so the client can update
 * its local txnState to UTXN_STAGE_TIMEOUT_KILLED.
 */
bool mndTxnIsTimeoutKilled(SMnode *pMnode, txn_id_t txnId) {
  SSdb       *pSdb = pMnode->pSdb;
  STxnLogObj *pLog = sdbAcquire(pSdb, SDB_TXN_LOG, &txnId);
  if (pLog == NULL) return false;
  bool killed = (pLog->stage == UTXN_STAGE_ROLLEDBACK && (pLog->rollbackReason == TXN_ROLLBACK_HB_TIMEOUT ||
                                                          pLog->rollbackReason == TXN_ROLLBACK_EXCEEDED_LIFETIME));
  sdbRelease(pSdb, pLog);
  return killed;
}

/**
 * Record (or update) a mystery orphan txn in the in-memory pOrphanTxnMap.
 * Called when mndGetOrphanTxnAction returns SKIP_UNKNOWN for a VNode-reported txn.
 * Written only from the MNode write-worker thread; no external lock required.
 */
void mndRecordOrphanTxn(SMnode *pMnode, txn_id_t txnId, int32_t vgId) {
  SOrphanRbKey     key = {.txnId = txnId, .vgId = vgId, ._pad = 0};
  int64_t          now = taosGetTimestampMs();
  SHashObj        *pMap = pMnode->txnMgmt.pOrphanTxnMap;
  SOrphanTxnEntry *pExist = taosHashGet(pMap, &key, sizeof(key));

  SOrphanTxnEntry entry;
  if (pExist != NULL) {
    entry = *pExist;
    entry.lastSeen = now;
    entry.reportCount += 1;
  } else {
    entry = (SOrphanTxnEntry){
        .txnId = txnId,
        .vgId = vgId,
        .firstSeen = now,
        .lastSeen = now,
        .reportCount = 1,
    };
    mWarn("txn:%" PRIi64 ", vgId:%d, first seen as mystery orphan — recorded in ins_transaction_logs", txnId, vgId);
  }
  int32_t code = taosHashPut(pMap, &key, sizeof(key), &entry, sizeof(entry));
  if (code != 0) {
    mWarn("txn:%" PRIi64 ", vgId:%d, failed to record orphan txn in map since %s", txnId, vgId, tstrerror(code));
  }
}

/**
 * Rollback an orphan transaction on a specific VNode via Raft-safe STrans.
 *
 * Called when VNode reports an idle txn (via statusReq) that no longer exists
 * in MNode SDB. Instead of ACKing alive=0 for VNode to do local rollback
 * (which bypasses Raft replication), MNode creates an STrans that sends
 * TDMT_VND_TXN_ROLLBACK through the normal Raft-replicated write path.
 */
int32_t mndRollbackOrphanTxnOnVnode(SMnode *pMnode, txn_id_t txnId, int32_t vgId) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;

  // Dedup: suppress repeated STrans creation for the same {txnId,vgId} pair.
  // Without this, every DNode heartbeat cycle would create a new STrans for each
  // reporting VNode, causing N_vnodes × K_heartbeats STrans objects until VNode
  // finishes the rollback.
  //
  // Key = SOrphanRbKey {txnId, vgId}, so each VNode still gets its own rollback
  // STrans (not txnId-only dedup which would skip 99 of 100 VNodes).
  //
  // Cooldown = 1 hour = 2× the VNode idle reporting threshold (30 min), giving
  // TRN_POLICY_RETRY ample time to succeed before we allow a retry.
  // Note: pOrphanRollbackTs is in-memory, so MNode failover naturally resets it —
  // the new leader will re-trigger orphan rollbacks as needed.
  static const int64_t kOrphanRollbackCooldownMs = 3600LL * 1000;  // 1 hour
  SOrphanRbKey         rbKey = {.txnId = txnId, .vgId = vgId, ._pad = 0};
  int64_t              now = taosGetTimestampMs();
  int64_t             *pLastMs = taosHashGet(pMnode->txnMgmt.pOrphanRollbackTs, &rbKey, sizeof(rbKey));
  if (pLastMs != NULL && (now - *pLastMs) < kOrphanRollbackCooldownMs) {
    mDebug("txn:%" PRIi64 ", orphan rollback on vgId:%d skipped (dedup, %" PRId64 "ms since last attempt)", txnId, vgId,
           now - *pLastMs);
    TAOS_RETURN(0);
  }
  if (taosHashPut(pMnode->txnMgmt.pOrphanRollbackTs, &rbKey, sizeof(rbKey), &now, sizeof(now)) != 0) {
    mWarn("txn:%" PRIi64 ", failed to record orphan rollback dedup entry, proceeding anyway", txnId);
  }

  SRpcMsg synReq = {0};
  synReq.info.node = pMnode;

  TSDB_CHECK_NULL(
      (pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, &synReq, "orphan-txn-cleanup")), code,
      lino, _exit, terrno);
  mInfo("trans:%d, used to cleanup orphan txn %" PRIi64 " on vgId:%d", pTrans->id, txnId, vgId);

  mndTransSetChangeless(pTrans);
  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);

  // Build ROLLBACK request for the specific VNode
  SVTxnRollbackReq req = {0};
  req.txnId = txnId;
  req.term = mndGetTerm(pMnode);
  req.reason = TSDB_CODE_TXN_TIMEOUT_KILLED;

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
  if (tSerializeSVTxnRollbackReq(POINTER_SHIFT(pHead, sizeof(SMsgHead)), bodyLen, &req) < 0) {
    taosMemoryFree(pHead);
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

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
    mError("txn:%" PRIi64 ", failed to rollback orphan on vgId:%d, code:0x%x", txnId, vgId, code);
  }
  TAOS_RETURN(code);
}

/**
 * Commit an orphan transaction on a specific VNode via Raft-safe STrans.
 *
 * Called when VNode reports an idle txn that MNode has already COMMITTED —
 * the VNode missed the original COMMIT (e.g. delivery race, node restart).
 * Re-delivering via a new STrans (TRN_POLICY_RETRY) ensures VNode promotes
 * its shadow data through the Raft-replicated write path.
 *
 * Uses the same pOrphanRollbackTs dedup hash with a 1-hour cooldown.
 */
int32_t mndCommitOrphanTxnOnVnode(SMnode *pMnode, txn_id_t txnId, int32_t vgId) {
  int32_t code = 0, lino = 0;
  STrans *pTrans = NULL;

  static const int64_t kOrphanCommitCooldownMs = 3600LL * 1000;  // 1 hour
  SOrphanRbKey         rbKey = {.txnId = txnId, .vgId = vgId, ._pad = 0};
  int64_t              now = taosGetTimestampMs();
  int64_t             *pLastMs = taosHashGet(pMnode->txnMgmt.pOrphanRollbackTs, &rbKey, sizeof(rbKey));
  if (pLastMs != NULL && (now - *pLastMs) < kOrphanCommitCooldownMs) {
    mDebug("txn:%" PRIi64 ", orphan commit on vgId:%d skipped (dedup, %" PRId64 "ms since last attempt)", txnId, vgId,
           now - *pLastMs);
    TAOS_RETURN(0);
  }
  if (taosHashPut(pMnode->txnMgmt.pOrphanRollbackTs, &rbKey, sizeof(rbKey), &now, sizeof(now)) != 0) {
    mWarn("txn:%" PRIi64 ", failed to record orphan commit dedup entry, proceeding anyway", txnId);
  }

  SRpcMsg synReq = {0};
  synReq.info.node = pMnode;

  TSDB_CHECK_NULL(
      (pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, &synReq, "orphan-txn-commit")), code,
      lino, _exit, terrno);
  mInfo("trans:%d, used to commit orphan txn %" PRIi64 " on vgId:%d", pTrans->id, txnId, vgId);

  mndTransSetChangeless(pTrans);
  mndTransSetKillMode(pTrans, TRN_KILL_MODE_SKIP);

  SVTxnCommitReq req = {0};
  req.txnId = txnId;
  req.term = mndGetTerm(pMnode);

  int32_t bodyLen = tSerializeSVTxnCommitReq(NULL, 0, &req);
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
  if (tSerializeSVTxnCommitReq(POINTER_SHIFT(pHead, sizeof(SMsgHead)), bodyLen, &req) < 0) {
    taosMemoryFree(pHead);
    code = TSDB_CODE_INVALID_MSG;
    goto _exit;
  }

  STransAction action = {0};
  action.mTraceId = pTrans->mTraceId;
  action.epSet = mndGetVgroupEpsetById(pMnode, vgId);
  action.pCont = pHead;
  action.contLen = contLen;
  action.msgType = TDMT_VND_TXN_COMMIT;
  action.acceptableCode = TSDB_CODE_SUCCESS;
  action.groupId = vgId;

  TAOS_CHECK_EXIT(mndTransAppendRedoAction(pTrans, &action));
  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));

_exit:
  mndTransDrop(pTrans);
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txn:%" PRIi64 ", failed to commit orphan on vgId:%d, code:0x%x", txnId, vgId, code);
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
int32_t mndTxnAddShadowOp(SMnode *pMnode, txn_id_t txnId, int8_t opType, const char *stbName, tb_uid_t uid,
                          const char *dbName, void *pReqData, int32_t reqDataLen) {
  STxnObj *pTxn = mndAcquireTxn(pMnode, txnId);
  if (pTxn == NULL) {
    mError("txn:%" PRIi64 ", not found, cannot add shadow op: opType=%d, stb=%s, uid=%" PRId64, txnId, opType, stbName,
           uid);
    return TSDB_CODE_TXN_NOT_EXIST;
  }

  taosWLockLatch(&pTxn->lock);

  if (pTxn->stage == UTXN_STAGE_ABORTED) {
    taosWUnLockLatch(&pTxn->lock);
    mndReleaseTxn(pMnode, pTxn);
    mError("txn:%" PRIi64 ", stage=ABORTED, reject new DDL shadow op: opType=%d, stb=%s", txnId, opType, stbName);
    return TSDB_CODE_MND_TXN_INVALID_STAGE;
  }

  if (pTxn->pShadowOps == NULL) {
    pTxn->pShadowOps = taosArrayInit(4, sizeof(SMndShadowOp));
    if (pTxn->pShadowOps == NULL) {
      taosWUnLockLatch(&pTxn->lock);
      mndReleaseTxn(pMnode, pTxn);
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
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  pTxn->lastActiveTime = taosGetTimestampMs();

  // Register in the global O(1) conflict map.  Only the first txn to touch an STB wins;
  // subsequent txns will hit mndTxnCheckStbConflict and get TSDB_CODE_TXN_RESOURCE_BUSY.
  SHashObj *pConflictMap = pMnode->txnMgmt.pStbConflictMap;
  if (pConflictMap != NULL) {
    txn_id_t myId    = pTxn->id;
    size_t   nameLen = strnlen(stbName, TSDB_TABLE_FNAME_LEN);
    int32_t  putCode = taosHashPut(pConflictMap, stbName, nameLen, &myId, sizeof(myId));
    if (putCode != 0) {
      // Roll back the shadow op we just pushed — the caller must fail this DDL and rollback.
      taosArrayPop(pTxn->pShadowOps);
      taosWUnLockLatch(&pTxn->lock);
      mndReleaseTxn(pMnode, pTxn);
      mError("txn:%" PRIi64 ", failed to register stb conflict map entry for stb:%s, code:0x%x",
             txnId, stbName, putCode);
      return putCode;
    }
  }

  taosWUnLockLatch(&pTxn->lock);
  mndReleaseTxn(pMnode, pTxn);

  mDebug("txn:%" PRIi64 ", shadow op added (redo): opType=%d, stb=%s, uid=%" PRId64 ", dataLen:%d", txnId, opType,
         stbName, uid, reqDataLen);
  return TSDB_CODE_SUCCESS;
}

/**
 * Get ALTER STB shadow ops for a specific STB in a given txn.
 * Returns an SArray of SMndShadowOp* (pointers into the txn's pShadowOps).
 * Caller must destroy the SArray but NOT free the SMndShadowOp contents.
 * Returns NULL ppOps if no ALTER ops found (not an error).
 */
int32_t mndTxnGetAlterOpsForStb(SMnode *pMnode, txn_id_t txnId, const char *stbFName, SArray **ppOps) {
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
      if (taosArrayPush(pResult, pOp) == NULL) {
        taosRUnLockLatch(&pTxn->lock);
        mndReleaseTxn(pMnode, pTxn);
        taosArrayDestroy(pResult);
        return terrno;
      }
    }
  }

  taosRUnLockLatch(&pTxn->lock);
  mndReleaseTxn(pMnode, pTxn);

  *ppOps = pResult;
  return TSDB_CODE_SUCCESS;
}

/**
 * Check if any active txn (other than callerTxnId) has a shadow op on stbName.
 * Used for MNode-level conflict detection on STB DROP/ALTER operations.
 *
 * O(1) lookup via pStbConflictMap (populated by mndTxnAddShadowOp, cleaned up
 * in mndTxnActionDelete when the STxnObj is destroyed by SDB).
 *
 * @return TSDB_CODE_TXN_RESOURCE_BUSY if conflict, 0 otherwise.
 */
int32_t mndTxnCheckStbConflict(SMnode *pMnode, const char *stbName, txn_id_t callerTxnId) {
  SHashObj *pConflictMap = pMnode->txnMgmt.pStbConflictMap;
  if (pConflictMap == NULL) return TSDB_CODE_SUCCESS;

  size_t    nameLen = strnlen(stbName, TSDB_TABLE_FNAME_LEN);
  txn_id_t *pOwner = taosHashGet(pConflictMap, stbName, nameLen);
  if (pOwner != NULL && *pOwner != callerTxnId) {
    // Double-check: if the owning txn is ABORTED it can no longer make progress,
    // so it is safe to ignore the stale conflict entry (the entry will be cleaned
    // up by mndTxnActionUpdate when stage transitions to ABORTED, and definitively
    // by mndTxnActionDelete when the txn is rolled back and removed from SDB).
    STxnObj *pOwnerTxn = mndAcquireTxn(pMnode, *pOwner);
    if (pOwnerTxn != NULL) {
      bool isAborted = (pOwnerTxn->stage == UTXN_STAGE_ABORTED);
      mndReleaseTxn(pMnode, pOwnerTxn);
      if (isAborted) {
        mInfo("stb:%s, owner txn:%" PRIi64 " is ABORTED, ignoring stale conflict entry", stbName, *pOwner);
        return TSDB_CODE_SUCCESS;
      }
    }
    mInfo("stb:%s, conflict with txn:%" PRIi64, stbName, *pOwner);
    return TSDB_CODE_TXN_RESOURCE_BUSY;
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
static int32_t mndTxnRebuildShadowOpsFromSdb(SMnode *pMnode, STxnObj *pTxn, bool needAlterData) {
  if (pTxn->pShadowOps != NULL) return TSDB_CODE_SUCCESS;  // already populated in memory

  SSdb    *pSdb = pMnode->pSdb;
  void    *pIter = NULL;
  SStbObj *pStb = NULL;
  int32_t  count = 0;
  int32_t  retCode = TSDB_CODE_SUCCESS;

  // Use a temporary array so that partial failure does NOT pollute pTxn->pShadowOps.
  // If we wrote directly into pTxn->pShadowOps and a mid-scan push failed, the
  // non-NULL but incomplete array would cause the guard above to skip rebuild on retry.
  SArray *pTmpOps = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_STB, pIter, (void **)&pStb);
    if (pIter == NULL) break;

    if (pStb->txnId != 0 && (txn_id_t)pStb->txnId == pTxn->id) {
      if (pTmpOps == NULL) {
        pTmpOps = taosArrayInit(4, sizeof(SMndShadowOp));
        if (pTmpOps == NULL) {
          sdbRelease(pSdb, pStb);
          sdbCancelFetch(pSdb, pIter);
          mError("txn:%" PRIi64 ", failed to alloc pShadowOps during rebuild", pTxn->id);
          return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
        }
      }

      // CREATE_STB: txnStatus indicates PRE_CREATE or PRE_CREATE_DROP
      if (pStb->txnStatus == META_TXN_PRE_CREATE || pStb->txnStatus == META_TXN_PRE_CREATE_DROP) {
        SMndShadowOp op = {0};
        op.opType = MND_SHADOW_OP_CREATE_STB;
        op.uid = pStb->uid;
        tstrncpy(op.name, pStb->name, sizeof(op.name));
        tstrncpy(op.db, pStb->db, sizeof(op.db));
        op.pReqData = NULL;
        op.reqDataLen = 0;

        if (taosArrayPush(pTmpOps, &op) == NULL) {
          sdbRelease(pSdb, pStb);
          sdbCancelFetch(pSdb, pIter);
          mError("txn:%" PRIi64 ", failed to push shadow op during rebuild", pTxn->id);
          retCode = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
          goto _rebuild_fail;
        }
        count++;
        mInfo("txn:%" PRIi64 ", rebuilt CREATE_STB shadow op: stb=%s uid=%" PRId64, pTxn->id, pStb->name, pStb->uid);
      }

      // DROP_STB: txnStatus indicates PRE_DROP or PRE_CREATE_DROP
      if (pStb->txnStatus == META_TXN_PRE_DROP || pStb->txnStatus == META_TXN_PRE_CREATE_DROP) {
        SMndShadowOp op = {0};
        op.opType = MND_SHADOW_OP_DROP_STB;
        op.uid = pStb->uid;
        tstrncpy(op.name, pStb->name, sizeof(op.name));
        tstrncpy(op.db, pStb->db, sizeof(op.db));
        op.pReqData = NULL;
        op.reqDataLen = 0;

        if (taosArrayPush(pTmpOps, &op) == NULL) {
          sdbRelease(pSdb, pStb);
          sdbCancelFetch(pSdb, pIter);
          mError("txn:%" PRIi64 ", failed to push DROP shadow op during rebuild", pTxn->id);
          retCode = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
          goto _rebuild_fail;
        }
        count++;
        mInfo("txn:%" PRIi64 ", rebuilt DROP_STB shadow op: stb=%s uid=%" PRId64, pTxn->id, pStb->name, pStb->uid);
      }

      // ALTER_STB: txnAlterReqsLen > 0 (chained ALTER request data)
      if (pStb->txnAlterReqsLen > (int32_t)sizeof(int32_t) && pStb->pTxnAlterReqs != NULL) {
        if (!needAlterData) {
          // ROLLBACK path: pReqData is never read by mndTxnUndoShadowOps — it only needs
          // the stb name/uid to clear txn markers.  One op per STB is enough; skip the
          // blob parse and the per-entry malloc entirely.
          SMndShadowOp op = {0};
          op.opType = MND_SHADOW_OP_ALTER_STB;
          op.uid = pStb->uid;
          tstrncpy(op.name, pStb->name, sizeof(op.name));
          tstrncpy(op.db, pStb->db, sizeof(op.db));
          /* pReqData = NULL, reqDataLen = 0 intentionally */
          if (taosArrayPush(pTmpOps, &op) == NULL) {
            sdbRelease(pSdb, pStb);
            sdbCancelFetch(pSdb, pIter);
            mError("txn:%" PRIi64 ", failed to push ALTER shadow op during rebuild", pTxn->id);
            retCode = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
            goto _rebuild_fail;
          }
          count++;
          mInfo("txn:%" PRIi64 ", rebuilt ALTER_STB shadow op (no data, rollback): stb=%s", pTxn->id, pStb->name);
        } else {
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
              mError("txn:%" PRIi64 ", failed to alloc ALTER data during rebuild", pTxn->id);
              retCode = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
              goto _rebuild_fail;
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

            if (taosArrayPush(pTmpOps, &op) == NULL) {
              taosMemoryFree(pData);
              sdbRelease(pSdb, pStb);
              sdbCancelFetch(pSdb, pIter);
              mError("txn:%" PRIi64 ", failed to push ALTER shadow op during rebuild", pTxn->id);
              retCode = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
              goto _rebuild_fail;
            }
            count++;
            mInfo("txn:%" PRIi64 ", rebuilt ALTER_STB shadow op %d/%d: stb=%s dataLen=%d", pTxn->id, j + 1, numEntries,
                  pStb->name, entryLen);
          }
        }
      }
    }
    sdbRelease(pSdb, pStb);
  }

  // All ops collected successfully — commit to pTxn
  if (pTmpOps != NULL) {
    pTxn->pShadowOps = pTmpOps;
  }
  if (count > 0) {
    mInfo("txn:%" PRIi64 ", rebuilt %d shadow ops from SDB (CREATE+DROP+ALTER)", pTxn->id, count);
  }
  return TSDB_CODE_SUCCESS;

_rebuild_fail:
  // Clean up partially-built temp array — free any allocated pReqData
  if (pTmpOps != NULL) {
    int32_t n = taosArrayGetSize(pTmpOps);
    for (int32_t i = 0; i < n; i++) {
      SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTmpOps, i);
      taosMemoryFreeClear(pOp->pReqData);
    }
    taosArrayDestroy(pTmpOps);
  }
  return retCode;
}

// Free a heap-allocated SStbObj produced by mndAppendAlterStbToTrans for accumulation.
// pAst1/pAst2 are NULL in the accumulated object (zeroed by mndAppendAlterStbToTrans before
// the ALTER switch), so mndFreeStb is safe to call here without risk of double-free.
static void mndFreeClearStb(SStbObj *pObj) {
  if (pObj == NULL) return;
  mndFreeStb(pObj);
  taosMemoryFree(pObj);
}

static int32_t mndTxnApplyShadowOps(SMnode *pMnode, STrans *pTrans, STxnObj *pTxn) {
  if (pTxn->pShadowOps == NULL || taosArrayGetSize(pTxn->pShadowOps) == 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t numOps = taosArrayGetSize(pTxn->pShadowOps);
  mDebug("txn:%" PRIi64 ", applying %d MNode STB shadow ops into commit trans:%d", pTxn->id, numOps, pTrans->id);

  // Accumulated schema map: stb uid (uint64_t) -> SStbObj* (heap-allocated, freed via
  // mndFreeClearStb).  Ensures each ALTER on the same STB within one COMMIT builds on
  // top of the previous one, preventing lost-update when multiple ALTERs on the same STB
  // are present in a single txn.
  SHashObj *pAccumMap = taosHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_UBIGINT), false, HASH_NO_LOCK);
  if (pAccumMap == NULL) {
    mError("txn:%" PRIi64 ", failed to alloc accumulated schema map", pTxn->id);
    return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t code = 0;
  for (int32_t i = 0; i < numOps; i++) {
    SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);
    code = 0;

    mDebug("txn:%" PRIi64 ", applying shadow op %d/%d: opType=%d, stb=%s", pTxn->id, i + 1, numOps, pOp->opType,
           pOp->name);

    switch (pOp->opType) {
      case MND_SHADOW_OP_CREATE_STB: {
        // Undo-log model: STB was written to SDB during txn with txnId set (PRE_CREATE).
        // At COMMIT we must clear txnId/txnStatus so the STB becomes a normal, fully
        // visible entry.  Without this commit-log the STB keeps txnId != 0 in SDB, and
        // any subsequent DROP/ALTER on it will be rejected with TSDB_CODE_TXN_RESOURCE_BUSY.
        mDebug("txn:%" PRIi64 ", CREATE_STB shadow op %d/%d: clearing txn markers, stb=%s", pTxn->id, i + 1, numOps,
               pOp->name);
        SStbObj *pStb = mndAcquireStb(pMnode, (char *)pOp->name);
        if (pStb == NULL) {
          // STB not found — may have been dropped already; treat as success.
          mDebug("txn:%" PRIi64 ", CREATE_STB stb=%s not found in SDB, skip marker clear", pTxn->id, pOp->name);
          break;
        }
        {
          SStbObj stbClone;
          taosRLockLatch(&pStb->lock);
          memcpy(&stbClone, pStb, sizeof(SStbObj));
          taosRUnLockLatch(&pStb->lock);
          stbClone.lock = 0;
          stbClone.txnId = 0;
          stbClone.txnStatus = META_TXN_NORMAL;
          stbClone.pTxnAlterReqs = NULL;
          stbClone.txnAlterReqsLen = 0;
          SSdbRaw *pRaw = mndStbActionEncode(&stbClone);
          if (pRaw == NULL) {
            mError("txn:%" PRIi64 ", mndStbActionEncode failed for CREATE_STB stb=%s (OOM)", pTxn->id, pOp->name);
            mndReleaseStb(pMnode, pStb);
            code = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
            break;
          }
          int32_t rawCode = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
          if (rawCode != 0) {
            sdbFreeRaw(pRaw);
            mndReleaseStb(pMnode, pStb);
            code = rawCode;
            break;
          }
          rawCode = mndTransAppendCommitlog(pTrans, pRaw);
          if (rawCode != 0) {
            mError("txn:%" PRIi64 ", failed to append marker clear commit-log for CREATE_STB stb=%s: %s", pTxn->id,
                   pOp->name, tstrerror(rawCode));
            mndReleaseStb(pMnode, pStb);
            code = rawCode;
            break;
          }
          mDebug("txn:%" PRIi64 ", appended marker clear commit-log for CREATE_STB stb=%s", pTxn->id, pOp->name);
        }
        mndReleaseStb(pMnode, pStb);
        break;
      }
      case MND_SHADOW_OP_DROP_STB: {
        code = mndAppendDropStbToTrans(pMnode, pTrans, pOp->name);
        // Discard accumulated ALTER schema for this STB: it's being dropped.
        SStbObj **ppOld = (SStbObj **)taosHashGet(pAccumMap, &pOp->uid, sizeof(pOp->uid));
        if (ppOld != NULL) {
          mndFreeClearStb(*ppOld);
          if (taosHashRemove(pAccumMap, &pOp->uid, sizeof(pOp->uid)) != 0) {
            mWarn("txn:%" PRIi64 ", failed to remove accum map entry for DROP_STB uid:%" PRId64
                  " (non-fatal, stale entry will be freed at cleanup)",
                  pTxn->id, pOp->uid);
          }
        }
        break;
      }
      case MND_SHADOW_OP_ALTER_STB: {
        // Retrieve any previously-accumulated schema for this STB (from an earlier ALTER
        // on the same STB in this txn's COMMIT).  Pass it as pAccumBase so this ALTER
        // builds on top of all preceding ALTERs rather than the original SDB schema.
        SStbObj  *pAccumBase = NULL;
        SStbObj **ppEntry = (SStbObj **)taosHashGet(pAccumMap, &pOp->uid, sizeof(pOp->uid));
        if (ppEntry != NULL) pAccumBase = *ppEntry;

        SStbObj *pAccumResult = NULL;
        code = mndAppendAlterStbToTrans(pMnode, pTrans, pOp->pReqData, pOp->reqDataLen, pAccumBase, &pAccumResult);
        if (code == 0) {
          // Replace the accumulated base with the new result.
          if (pAccumBase != NULL) {
            mndFreeClearStb(pAccumBase);
            if (taosHashRemove(pAccumMap, &pOp->uid, sizeof(pOp->uid)) != 0) {
              mWarn("txn:%" PRIi64 ", failed to remove old accum map entry for ALTER_STB uid:%" PRId64
                    " (non-fatal, stale entry will be freed at cleanup)",
                    pTxn->id, pOp->uid);
            }
          }
          if (pAccumResult != NULL) {
            if (taosHashPut(pAccumMap, &pOp->uid, sizeof(pOp->uid), &pAccumResult, sizeof(SStbObj *)) != 0) {
              mndFreeClearStb(pAccumResult);
              code = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
            }
          }
        } else {
          // On error, free the result (if any) and leave pAccumBase unchanged in the map
          // (it will be freed during the global cleanup below).
          mndFreeClearStb(pAccumResult);
        }
        break;
      }
      default:
        mError("txn:%" PRIi64 ", unknown shadow op type %d", pTxn->id, pOp->opType);
        code = TSDB_CODE_MND_TXN_ERROR;
        break;
    }

    if (code != 0) {
      mError("txn:%" PRIi64 ", shadow op %d/%d failed: %s", pTxn->id, i + 1, numOps, tstrerror(code));
      break;
    }
    mDebug("txn:%" PRIi64 ", shadow op %d/%d applied", pTxn->id, i + 1, numOps);
  }

  // Free all accumulated schemas (both success and failure paths).
  void *pIter = taosHashIterate(pAccumMap, NULL);
  while (pIter != NULL) {
    SStbObj *pObj = *(SStbObj **)pIter;
    mndFreeClearStb(pObj);
    pIter = taosHashIterate(pAccumMap, pIter);
  }
  taosHashCleanup(pAccumMap);

  if (code != 0) {
    return code;
  }

  // Free pReqData after ALL ops applied successfully to ensure retry safety.
  // If mndTransPrepare fails after this function returns, the caller retries COMMIT →
  // mndTxnRebuildShadowOpsFromSdb skips rebuild if pShadowOps != NULL. We must either:
  //   (a) free + destroy pShadowOps so rebuild regenerates from SDB on retry, or
  //   (b) leave pReqData intact for retry.
  // We choose (a): destroy the in-memory ops so any retry path regenerates cleanly.
  // Before destroying pShadowOps, remove this txn's entries from the global stb conflict
  // map.  mndTxnActionDelete also tries this cleanup, but by the time it runs pShadowOps
  // will already be NULL and the entries would be silently skipped, leaving stale entries
  // that block subsequent DDL with TSDB_CODE_TXN_RESOURCE_BUSY.
  SHashObj *pConflictMap = pMnode->txnMgmt.pStbConflictMap;
  if (pConflictMap != NULL) {
    for (int32_t i = 0; i < numOps; i++) {
      SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);
      size_t        nameLen = strnlen(pOp->name, TSDB_TABLE_FNAME_LEN);
      txn_id_t     *pOwner = taosHashGet(pConflictMap, pOp->name, nameLen);
      if (pOwner != NULL && *pOwner == pTxn->id) {
        if (taosHashRemove(pConflictMap, pOp->name, nameLen) != 0) {
          mWarn("txn:%" PRIi64 ", failed to remove conflict map entry for stb:%s on COMMIT (non-fatal)", pTxn->id,
                pOp->name);
        }
      }
    }
  }
  for (int32_t i = 0; i < numOps; i++) {
    SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);
    taosMemoryFreeClear(pOp->pReqData);
  }
  taosArrayDestroy(pTxn->pShadowOps);
  pTxn->pShadowOps = NULL;

  mDebug("txn:%" PRIi64 ", all %d MNode STB shadow ops applied into commit trans", pTxn->id, numOps);
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
  mDebug("txn:%" PRIi64 ", undoing %d MNode shadow ops (ROLLBACK)", pTxn->id, numOps);

  for (int32_t i = 0; i < numOps; i++) {
    SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);

    switch (pOp->opType) {
      case MND_SHADOW_OP_CREATE_STB: {
        // Undo-log: STB was created immediately, append DROP to rollback Trans
        mDebug("txn:%" PRIi64 ", undo CREATE_STB shadow op %d/%d: stb=%s uid=%" PRId64, pTxn->id, i + 1, numOps,
               pOp->name, pOp->uid);
        int32_t code = mndAppendDropStbToTrans(pMnode, pTrans, pOp->name);
        if (code != 0) {
          mError("txn:%" PRIi64 ", failed to append DROP STB for stb=%s: %s", pTxn->id, pOp->name, tstrerror(code));
          return code;
        }
        break;
      }
      case MND_SHADOW_OP_DROP_STB:
      case MND_SHADOW_OP_ALTER_STB: {
        // Clear txn markers (txnId, txnStatus, pTxnAlterReqs) from SStbObj via commit-log.
        // Skip for PRE_CREATE/PRE_CREATE_DROP: the CREATE_STB undo will delete the STB entirely.
        SStbObj *pStb = mndAcquireStb(pMnode, pOp->name);
        if (pStb != NULL && (pStb->txnStatus == META_TXN_PRE_CREATE || pStb->txnStatus == META_TXN_PRE_CREATE_DROP)) {
          mDebug("txn:%" PRIi64 ", skip %s undo for stb=%s (status=%d, CREATE undo handles deletion)", pTxn->id,
                 pOp->opType == MND_SHADOW_OP_DROP_STB ? "DROP_STB" : "ALTER_STB", pOp->name, pStb->txnStatus);
          mndReleaseStb(pMnode, pStb);
          break;
        }
        mDebug("txn:%" PRIi64 ", undo %s shadow op %d/%d: clearing markers on stb=%s", pTxn->id,
               pOp->opType == MND_SHADOW_OP_DROP_STB ? "DROP_STB" : "ALTER_STB", i + 1, numOps, pOp->name);
        if (pStb != NULL) {
          SStbObj stbClone;
          taosRLockLatch(&pStb->lock);
          memcpy(&stbClone, pStb, sizeof(SStbObj));
          taosRUnLockLatch(&pStb->lock);
          stbClone.lock = 0;
          stbClone.txnId = 0;
          stbClone.txnStatus = META_TXN_NORMAL;
          stbClone.pTxnAlterReqs = NULL;
          stbClone.txnAlterReqsLen = 0;
          SSdbRaw *pRaw = mndStbActionEncode(&stbClone);
          if (pRaw == NULL) {
            mError("txn:%" PRIi64 ", mndStbActionEncode failed for stb=%s (OOM), aborting rollback undo", pTxn->id,
                   pOp->name);
            mndReleaseStb(pMnode, pStb);
            return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
          }
          {
            int32_t rawCode = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
            if (rawCode != 0) {
              mError("txn:%" PRIi64 ", sdbSetRawStatus READY failed for stb=%s: %s", pTxn->id, pOp->name,
                     tstrerror(rawCode));
              mndReleaseStb(pMnode, pStb);
              return rawCode;
            }
            int32_t code = mndTransAppendCommitlog(pTrans, pRaw);
            if (code != 0) {
              mError("txn:%" PRIi64 ", failed to append marker cleanup for stb=%s: %s", pTxn->id, pOp->name,
                     tstrerror(code));
              mndReleaseStb(pMnode, pStb);
              return code;
            }
            mDebug("txn:%" PRIi64 ", append marker cleanup commit-log for stb=%s", pTxn->id, pOp->name);
          }
          mndReleaseStb(pMnode, pStb);
        }
        break;
      }
      default: {
        mDebug("txn:%" PRIi64 ", discard shadow op %d/%d: opType=%d, stb=%s", pTxn->id, i + 1, numOps, pOp->opType,
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
 * Sources:
 *   (1) pTxn->pVgList  — child/normal table VGroups tracked by client
 *   (2) DB VGroups     — from pShadowOps CREATE_STB + pDbList (merged, single scan)
 * Fallback: if both sources empty, broadcast to ALL VGroups (idempotent on VNode side).
 *
 * Optimization: paths (2a) shadow ops and (2b) pDbList first collect unique DB UIDs into
 * a hash set, then perform ONE `sdbFetch(SDB_VGROUP)` scan with O(1) hash membership check
 * per vgroup. This avoids the previous O(N_dbs × total_vgroups) repeated full scans.
 *
 * Returns a deduplicated SHashObj (key=vgId, value=unused byte).  Caller must taosHashCleanup.
 */
static SHashObj *mndCollectTxnVgroupIds(SMnode *pMnode, STxnObj *pTxn) {
  SHashObj *pVgSet = taosHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
  if (pVgSet == NULL) return NULL;

  // (1) Add VGroups from pVgList (child / normal table tracking)
  if (pTxn->pVgList != NULL) {
    int32_t vgIter = 0;
    void   *pVgData = tSimpleHashIterate(pTxn->pVgList, NULL, &vgIter);
    while (pVgData != NULL) {
      int32_t vgId = *(int32_t *)tSimpleHashGetKey(pVgData, NULL);
      int8_t  dummy = 1;
      if (taosHashPut(pVgSet, &vgId, sizeof(vgId), &dummy, sizeof(dummy)) != 0) {
        mError("txn:%" PRIi64 ", failed to add vgId:%d (from pVgList) to dedup set, code:0x%x", pTxn->id, vgId, terrno);
        taosHashCleanup(pVgSet);
        return NULL;
      }
      pVgData = tSimpleHashIterate(pTxn->pVgList, pVgData, &vgIter);
    }
  }

  // (2) Collect unique DB UIDs from pShadowOps (CREATE_STB) and pDbList, then do ONE VGroup scan.
  //     Previously each DB triggered an independent full SDB_VGROUP scan — O(N_dbs * total_vgroups).
  //     Now: O(N_dbs_acquire + total_vgroups) with hash lookup per vgroup.
  {
    // Use UBIGINT hash for db_uid_t (uint64_t)
    SHashObj *pDbUidSet = NULL;

    // (2a) From CREATE_STB shadow ops: resolve STB name → DB uid
    if (pTxn->pShadowOps != NULL) {
      int32_t numOps = taosArrayGetSize(pTxn->pShadowOps);
      for (int32_t i = 0; i < numOps; i++) {
        SMndShadowOp *pOp = (SMndShadowOp *)taosArrayGet(pTxn->pShadowOps, i);
        // Include both CREATE_STB and DROP_STB ops so their DB vnodes receive
        // COMMIT/ROLLBACK messages. DROP_STB vnodes now have a pTxnIdx entry
        // (from the BEGIN-time PRE_DROP broadcast) and need TXN_COMMIT / TXN_ROLLBACK
        // to clean up or restore state correctly.
        if (pOp->opType != MND_SHADOW_OP_CREATE_STB && pOp->opType != MND_SHADOW_OP_DROP_STB) continue;

        SDbObj *pDb = mndAcquireDbByStb(pMnode, pOp->name);
        if (pDb == NULL) continue;

        if (pDbUidSet == NULL) {
          pDbUidSet = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
          if (pDbUidSet == NULL) {
            mndReleaseDb(pMnode, pDb);
            taosHashCleanup(pVgSet);
            return NULL;
          }
        }
        int8_t  dummy = 1;
        int32_t putCode = taosHashPut(pDbUidSet, &pDb->uid, sizeof(pDb->uid), &dummy, sizeof(dummy));
        if (putCode != 0 && putCode != TSDB_CODE_DUP_KEY) {
          mError("txn:%" PRIi64 ", failed to add dbUid:%" PRId64 " (from shadow op %s) to dbUid set, code:0x%x",
                 pTxn->id, pDb->uid, pOp->name, putCode);
          mndReleaseDb(pMnode, pDb);
          taosHashCleanup(pDbUidSet);
          taosHashCleanup(pVgSet);
          return NULL;
        }
        mndReleaseDb(pMnode, pDb);
      }
    }

    // (2b) From pDbList (persisted DB fullNames for replicated txns)
    if (pTxn->pDbList != NULL) {
      int32_t dbListSize = taosArrayGetSize(pTxn->pDbList);
      for (int32_t i = 0; i < dbListSize; i++) {
        char   *dbFName = (char *)taosArrayGet(pTxn->pDbList, i);
        SDbObj *pDb = mndAcquireDb(pMnode, dbFName);
        if (pDb == NULL) continue;

        if (pDbUidSet == NULL) {
          pDbUidSet = taosHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
          if (pDbUidSet == NULL) {
            mndReleaseDb(pMnode, pDb);
            taosHashCleanup(pVgSet);
            return NULL;
          }
        }
        int8_t  dummy = 1;
        int32_t putCode = taosHashPut(pDbUidSet, &pDb->uid, sizeof(pDb->uid), &dummy, sizeof(dummy));
        if (putCode != 0 && putCode != TSDB_CODE_DUP_KEY) {
          mError("txn:%" PRIi64 ", failed to add dbUid:%" PRId64 " (from pDbList '%s') to dbUid set, code:0x%x",
                 pTxn->id, pDb->uid, dbFName, putCode);
          mndReleaseDb(pMnode, pDb);
          taosHashCleanup(pDbUidSet);
          taosHashCleanup(pVgSet);
          return NULL;
        }
        mndReleaseDb(pMnode, pDb);
      }
    }

    // (2c) Single VGroup scan: add all VGroups belonging to any collected DB
    if (pDbUidSet != NULL && taosHashGetSize(pDbUidSet) > 0) {
      SSdb   *pSdb = pMnode->pSdb;
      SVgObj *pVgroup = NULL;
      void   *pIter = NULL;
      while (1) {
        pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
        if (pIter == NULL) break;
        if (pVgroup->isTsma || taosHashGet(pDbUidSet, &pVgroup->dbUid, sizeof(pVgroup->dbUid)) == NULL) {
          sdbRelease(pSdb, pVgroup);
          continue;
        }
        int32_t vgId = pVgroup->vgId;
        int8_t  dummy = 1;
        if (taosHashPut(pVgSet, &vgId, sizeof(vgId), &dummy, sizeof(dummy)) != 0) {
          mError("txn:%" PRIi64 ", failed to add vgId:%d to dedup hash, code:0x%x", pTxn->id, vgId, terrno);
          sdbRelease(pSdb, pVgroup);
          sdbCancelFetch(pSdb, pIter);
          taosHashCleanup(pDbUidSet);
          taosHashCleanup(pVgSet);
          return NULL;
        }
        sdbRelease(pSdb, pVgroup);
      }
    }
    if (pDbUidSet != NULL) taosHashCleanup(pDbUidSet);
  }

  // (3) Fallback: if no VGroups identified (e.g. after full cluster restart — pVgList only populated
  //     at COMMIT time by client, and pShadowOps are in-memory only), broadcast to ALL VGroups.
  //     VNodes without this txn's shadow entries will return success (idempotent).
  if (taosHashGetSize(pVgSet) == 0) {
    mDebug("txn:%" PRIi64 ", no VGroups from pVgList/pShadowOps, broadcasting to all VGroups", pTxn->id);
    SSdb   *pSdb = pMnode->pSdb;
    SVgObj *pVgroup = NULL;
    void   *pIter = NULL;
    while (1) {
      pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
      if (pIter == NULL) break;
      int32_t vgId = pVgroup->vgId;
      int8_t  dummy = 1;
      if (taosHashPut(pVgSet, &vgId, sizeof(vgId), &dummy, sizeof(dummy)) != 0) {
        mError("txn:%" PRIi64 ", failed to add vgId:%d (broadcast fallback) to dedup hash, code:0x%x", pTxn->id, vgId,
               terrno);
        sdbRelease(pSdb, pVgroup);
        sdbCancelFetch(pSdb, pIter);
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
  if (tSerializeSVTxnCommitReq(POINTER_SHIFT(pHead, sizeof(SMsgHead)), bodyLen, &req) < 0) {
    taosMemoryFree(pHead);
    *pContLen = 0;
    return NULL;
  }
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
  if (tSerializeSVTxnRollbackReq(POINTER_SHIFT(pHead, sizeof(SMsgHead)), bodyLen, &req) < 0) {
    taosMemoryFree(pHead);
    *pContLen = 0;
    return NULL;
  }
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
  TAOS_CHECK_EXIT(mndTxnRebuildShadowOpsFromSdb(pMnode, pTxn, true));

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "commit-txn")), code,
                  lino, _exit, terrno);
  mInfo("trans:%d, used to commit txn %" PRIi64, pTrans->id, pTxn->id);

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

  // Commit log: finalize STxnObj as COMMITTED.
  // Drop from SDB_TXN immediately; write compact record to SDB_TXN_LOG so
  // mndGetOrphanTxnAction can still answer VNode orphan queries after MNode restart.
  {
    STxnObj completedObj = *pTxn;
    completedObj.stage = UTXN_STAGE_COMMITTED;
    completedObj.completedAt = taosGetTimestampMs();
    SSdbRaw *pCompletedRaw = mndTxnActionEncode(&completedObj);
    if (pCompletedRaw == NULL) {
      TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL);
    }
    TAOS_CHECK_EXIT(mndTransAppendCommitlog(pTrans, pCompletedRaw));
    // Drop from SDB_TXN; write compact record to SDB_TXN_LOG.
    TAOS_CHECK_EXIT(sdbSetRawStatus(pCompletedRaw, SDB_STATUS_DROPPED));

    STxnLogObj logObj = {0};
    logObj.id = pTxn->id;
    logObj.stage = UTXN_STAGE_COMMITTED;
    logObj.completedAt = completedObj.completedAt;
    logObj.createTime = pTxn->createTime;
    tstrncpy(logObj.createUser, pTxn->createUser, TSDB_USER_LEN);
    SSdbRaw *pLogRaw = mndTxnLogActionEncode(&logObj);
    if (pLogRaw == NULL) {
      TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL);
    }
    TAOS_CHECK_EXIT(mndTransAppendCommitlog(pTrans, pLogRaw));
    TAOS_CHECK_EXIT(sdbSetRawStatus(pLogRaw, SDB_STATUS_READY));
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
          stbClone.txnStatus = META_TXN_NORMAL;
          stbClone.pTxnAlterReqs = NULL;
          stbClone.txnAlterReqsLen = 0;
          SSdbRaw *pRaw = mndStbActionEncode(&stbClone);
          if (pRaw == NULL) {
            mError("txn:%" PRIi64 ", mndStbActionEncode failed for stb=%s (OOM), aborting commit", pTxn->id, pOp->name);
            mndReleaseStb(pMnode, pStb);
            code = terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
            goto _exit;
          }
          int32_t rawCode = sdbSetRawStatus(pRaw, SDB_STATUS_READY);
          if (rawCode != 0) {
            mError("txn:%" PRIi64 ", sdbSetRawStatus READY failed for stb=%s: %s", pTxn->id, pOp->name,
                   tstrerror(rawCode));
            mndReleaseStb(pMnode, pStb);
            code = rawCode;
            goto _exit;
          }
          rawCode = mndTransAppendCommitlog(pTrans, pRaw);
          if (rawCode != 0) {
            mError("txn:%" PRIi64 ", mndTransAppendCommitlog failed for stb=%s: %s", pTxn->id, pOp->name,
                   tstrerror(rawCode));
            mndReleaseStb(pMnode, pStb);
            code = rawCode;
            goto _exit;
          }
          mDebug("txn:%" PRIi64 ", append STB promote commit log for stb=%s", pTxn->id, pOp->name);
          mndReleaseStb(pMnode, pStb);
        }
      }
    }
  }

  // Collect participant VGroups BEFORE mndTxnApplyShadowOps destroys pShadowOps.
  // Otherwise CREATE_STB-only VGroups (those that received only the STB and no child tables)
  // would be missed, leaving the STB stuck in PRE_CREATE on those vnodes after COMMIT.
  SHashObj *pVgSet = mndCollectTxnVgroupIds(pMnode, pTxn);
  if (pVgSet == NULL) {
    TAOS_CHECK_EXIT(terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY);
  }

  // Apply ALTER_STB / DROP_STB shadow ops: embed SDB logs + VNode actions into this commit STrans
  if ((code = mndTxnApplyShadowOps(pMnode, pTrans, pTxn)) != 0) {
    taosHashCleanup(pVgSet);
    TAOS_CHECK_EXIT(code);
  }

  // Add redo actions: send COMMIT to each participant VGroup (pVgList + CREATE_STB DB VGroups)
  {
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
      mDebug("txn:%" PRIi64 ", append commit action for vgId:%d", pTxn->id, vgId);
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
  // needAlterData=false: ROLLBACK only clears SStbObj markers (name/uid); pReqData is never read.
  TAOS_CHECK_EXIT(mndTxnRebuildShadowOpsFromSdb(pMnode, pTxn, false));

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "rollback-txn")), code,
                  lino, _exit, terrno);
  mInfo("trans:%d, used to rollback txn %" PRIi64, pTrans->id, pTxn->id);

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

  // Commit log: finalize STxnObj as ROLLEDBACK.
  // Drop from SDB_TXN; write compact record to SDB_TXN_LOG (same rationale as mndCommitTxn).
  {
    STxnObj rollbackObj = *pTxn;
    rollbackObj.stage = UTXN_STAGE_ROLLEDBACK;
    rollbackObj.completedAt = taosGetTimestampMs();
    SSdbRaw *pRollbackRaw = mndTxnActionEncode(&rollbackObj);
    if (pRollbackRaw == NULL) {
      TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL);
    }
    TAOS_CHECK_EXIT(mndTransAppendCommitlog(pTrans, pRollbackRaw));
    TAOS_CHECK_EXIT(sdbSetRawStatus(pRollbackRaw, SDB_STATUS_DROPPED));

    STxnLogObj logObj = {0};
    logObj.id = pTxn->id;
    logObj.stage = UTXN_STAGE_ROLLEDBACK;
    logObj.completedAt = rollbackObj.completedAt;
    logObj.createTime = pTxn->createTime;
    tstrncpy(logObj.createUser, pTxn->createUser, TSDB_USER_LEN);
    // Map the rollback reason code to ETxnRollbackReason for the SDB_TXN_LOG record.
    if (reason == TSDB_CODE_TXN_TIMEOUT_KILLED) {
      logObj.rollbackReason = TXN_ROLLBACK_HB_TIMEOUT;
    } else if (reason == TSDB_CODE_TXN_EXCEEDED_LIFETIME) {
      logObj.rollbackReason = TXN_ROLLBACK_EXCEEDED_LIFETIME;
    } else {
      logObj.rollbackReason = TXN_ROLLBACK_EXPLICIT;
    }
    SSdbRaw *pLogRaw = mndTxnLogActionEncode(&logObj);
    if (pLogRaw == NULL) {
      TAOS_CHECK_EXIT(terrno ? terrno : TSDB_CODE_MND_RETURN_VALUE_NULL);
    }
    TAOS_CHECK_EXIT(mndTransAppendCommitlog(pTrans, pLogRaw));
    TAOS_CHECK_EXIT(sdbSetRawStatus(pLogRaw, SDB_STATUS_READY));
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
      mDebug("txn:%" PRIi64 ", append rollback action for vgId:%d", pTxn->id, vgId);
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
  obj.stage = UTXN_STAGE_ACTIVE;

  TSDB_CHECK_NULL((pTrans = mndTransCreate(pMnode, TRN_POLICY_RETRY, TRN_CONFLICT_NOTHING, pReq, "begin-txn")), code,
                  lino, _exit, terrno);
  mInfo("trans:%d, used to create txn %" PRIi64 " term:%" PRId64, pTrans->id, obj.id, obj.term);

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
        if (tSerializeSMTransReq(pRsp, rspLen, &rspReq) < 0) {
          mError("txn:%" PRIi64 ", failed to serialize begin txn response", obj.id);
          taosMemoryFree(pRsp);
        } else {
          mndTransSetRpcRsp(pTrans, pRsp, rspLen);
        }
      }
    }
  }

  TAOS_CHECK_EXIT(mndTransPrepare(pMnode, pTrans));
_exit:
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txn:%" PRIi64 ", failed at line %d to begin txn, since %s", obj.id, lino, tstrerror(code));
  }
  mndTransDrop(pTrans);
  TAOS_RETURN(code);
}

/**
 * Merge client-tracked pVgSet into pTxn->pVgList (SSHashObj, no lock — protected by caller's SRWLatch).
 * tSimpleHashPut is idempotent on duplicate keys, so dedup is automatic.
 */
static int32_t mndMergeVgList(STxnObj *pTxn, SSHashObj *pNewVgSet) {
  if (pNewVgSet == NULL || tSimpleHashGetSize(pNewVgSet) == 0) return TSDB_CODE_SUCCESS;

  if (pTxn->pVgList == NULL) {
    pTxn->pVgList = tSimpleHashInit(tSimpleHashGetSize(pNewVgSet), taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT));
    if (pTxn->pVgList == NULL) return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
  }

  int32_t iter = 0;
  void   *pData = tSimpleHashIterate(pNewVgSet, NULL, &iter);
  while (pData != NULL) {
    int32_t vgId = *(int32_t *)tSimpleHashGetKey(pData, NULL);
    if (tSimpleHashGet(pTxn->pVgList, &vgId, sizeof(vgId)) == NULL) {
      int8_t dummy = 1;
      if (tSimpleHashPut(pTxn->pVgList, &vgId, sizeof(vgId), &dummy, sizeof(dummy)) != 0) {
        return terrno != 0 ? terrno : TSDB_CODE_OUT_OF_MEMORY;
      }
      mTrace("txn:%" PRIi64 ", merged client vgId:%d into participant set", pTxn->id, vgId);
    }
    pData = tSimpleHashIterate(pNewVgSet, pData, &iter);
  }

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
  bool       reservedSlot = false;  // true if we pre-incremented activeTxnCnt

  TAOS_CHECK_EXIT(tDeserializeSMTransReq(pReq->pCont, pReq->contLen, &txnReq));

  // Admission control: atomically pre-increment to reserve a slot before proposing to Raft.
  // Using atomic pre-increment (rather than load+compare) prevents TOCTOU races when
  // concurrent BEGIN requests are all processed before any Raft commit fires the
  // mndTxnActionInsert callback.  The pre-increment is undone if BEGIN fails or is idempotent.
  if (txnReq.txnId == 0) {
    int32_t cnt = atomic_add_fetch_32(&pMnode->txnMgmt.activeTxnCnt, 1);
    if (cnt > MND_TXN_MAX_ACTIVE) {
      atomic_sub_fetch_32(&pMnode->txnMgmt.activeTxnCnt, 1);
      mError("txn: too many active transactions (%d > %d), reject BEGIN", cnt, MND_TXN_MAX_ACTIVE);
      code = TSDB_CODE_MND_TXN_FULL;
      goto _exit;
    }
    reservedSlot = true;
  }

  if (txnReq.txnId != 0) {
    mError("txn:%" PRIi64 ", client already has active transaction, reject double BEGIN", txnReq.txnId);
    code = TSDB_CODE_TXN_ALREADY_IN_PROGRESS;
    goto _exit;
  } else {
    txnReq.txnId = mndGenTxnId(pMnode);
    if (txnReq.txnId < 0) {
      code = (int32_t)txnReq.txnId;
      goto _exit;
    }
  }
  mInfo("start to begin txn: %" PRIi64, txnReq.txnId);
  TAOS_CHECK_EXIT(mndAcquireUser(pMnode, RPC_MSG_USER(pReq), &pOperUser));
  pTxn = mndAcquireTxn(pMnode, txnReq.txnId);
  if (pTxn != NULL) {
    if (pTxn->stage == UTXN_STAGE_COMMITTED) {
      // Txn committed and record is still within TTL window: inform client it was committed
      mInfo("txn:%" PRIi64 ", stage=COMMITTED, return TXN_COMMITTED", txnReq.txnId);
      code = TSDB_CODE_TXN_COMMITTED;
      goto _exit;  // pTxn released at _exit
    } else if (pTxn->stage == UTXN_STAGE_ROLLEDBACK) {
      // Txn rolled back (or timed out) and record still within TTL: inform client it was rolled back
      mInfo("txn:%" PRIi64 ", stage=%s, return TXN_ROLLEDBACK", txnReq.txnId, mndUtxnStageStr(pTxn->stage));
      code = TSDB_CODE_TXN_ROLLEDBACK;
      goto _exit;  // pTxn released at _exit
    }
    // Transaction already exists and is active — idempotent success (client retry).
    mInfo("txn:%" PRIi64 ", already exists (stage=%s), return success", txnReq.txnId, mndUtxnStageStr(pTxn->stage));
    mndReleaseTxn(pMnode, pTxn);
    pTxn = NULL;
    goto _exit;
  }
  terrno = 0;  // clear terrno set by sdbAcquire

  TAOS_CHECK_EXIT(mndBeginTxn(pMnode, pReq, pOperUser, &txnReq));

  if (code == 0) code = TSDB_CODE_ACTION_IN_PROGRESS;

  if (tsAuditLevel >= AUDIT_LEVEL_SYSTEM) {
    int64_t tse = taosGetTimestampMs();
    double  duration = (double)(tse - tss);
    duration = duration / 1000;
    // auditRecord(pReq, pMnode->clusterId, "createTxn", txnReq.name, txnReq.tbFName, "", 0, duration, 0);
  }
_exit:
  // Undo the pre-incremented slot reservation if BEGIN did not complete successfully.
  // ACTION_IN_PROGRESS means the STrans was submitted (success path); any other code means
  // failure or idempotent-success (txn already existed) — both require undoing the reservation.
  if (reservedSlot && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    atomic_sub_fetch_32(&pMnode->txnMgmt.activeTxnCnt, 1);
  }
  if (code != 0 && code != TSDB_CODE_ACTION_IN_PROGRESS) {
    mError("txn:%" PRIi64 ", failed at line %d to begin since %s", txnReq.txnId, lino, tstrerror(code));
  }
  if (pTxn) mndReleaseTxn(pMnode, pTxn);
  mndReleaseUser(pMnode, pOperUser);
  tFreeSMTransReq(&txnReq);

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
    mInfo("txn:%" PRIi64 ", is invalid, ignore commit request", txnReq.txnId);
    goto _exit;
  }
  mInfo("start to commit txn: %" PRIi64, txnReq.txnId);
  pTxn = mndAcquireTxn(pMnode, txnReq.txnId);
  if (pTxn == NULL) {
    mError("txn:%" PRIi64 ", not found, cannot commit", txnReq.txnId);
    TAOS_CHECK_EXIT(TSDB_CODE_TXN_NOT_EXIST);
  }

  if (strcmp(pTxn->createUser, RPC_MSG_USER(pReq)) != 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_NO_RIGHTS);
  }

  if (pTxn->stage != UTXN_STAGE_ACTIVE) {
    mError("txn:%" PRIi64 ", stage=%s, cannot commit", txnReq.txnId, mndUtxnStageStr(pTxn->stage));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_TXN_INVALID_STAGE);
  }

  // Merge client-tracked pVgSet into pTxn->pVgList (O(N) hash dedup)
  taosWLockLatch(&pTxn->lock);
  code = mndMergeVgList(pTxn, txnReq.pVgSet);
  taosWUnLockLatch(&pTxn->lock);
  TAOS_CHECK_EXIT(code);

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
    mError("txn:%" PRIi64 ", failed at line %d to commit since %s", txnReq.txnId, lino, tstrerror(code));
  }
  if (pTxn) mndReleaseTxn(pMnode, pTxn);
  tFreeSMTransReq(&txnReq);

  TAOS_RETURN(code);
}

static int32_t mndProcessRollbackTxnReq(SRpcMsg *pReq) {
  int32_t code = 0, lino = 0;

  SMnode    *pMnode = pReq->info.node;
  STxnObj   *pTxn = NULL;
  int64_t    mTraceId = TRACE_GET_ROOTID(&pReq->info.traceId);
  SMTransReq txnReq = {0};
  int64_t    tss = taosGetTimestampMs();

  TAOS_CHECK_EXIT(tDeserializeSMTransReq(pReq->pCont, pReq->contLen, &txnReq));

  if (txnReq.txnId == 0) {
    mInfo("txn:%" PRIi64 ", is invalid, ignore rollback request", txnReq.txnId);
    code = 0;
    goto _exit;
  }
  mInfo("start to rollback txn: %" PRIi64, txnReq.txnId);
  pTxn = mndAcquireTxn(pMnode, txnReq.txnId);
  if (pTxn == NULL) {
    // Transaction not found — treat as already rolled back (idempotent).
    mInfo("txn:%" PRIi64 ", not found, treat as already rolled back", txnReq.txnId);
    terrno = 0;
    goto _exit;
  }
  if (strcmp(pTxn->createUser, RPC_MSG_USER(pReq)) != 0) {
    TAOS_CHECK_EXIT(TSDB_CODE_MND_NO_RIGHTS);
  }
  if (pTxn->stage == UTXN_STAGE_COMMITTING) {
    mError("txn:%" PRIi64 ", stage=%s, cannot rollback after commit decision", txnReq.txnId,
           mndUtxnStageStr(pTxn->stage));
    TAOS_CHECK_EXIT(TSDB_CODE_MND_TXN_INVALID_STAGE);
  }
  if (pTxn->stage == UTXN_STAGE_ROLLINGBACK || pTxn->stage == UTXN_STAGE_ROLLEDBACK ||
      pTxn->stage == UTXN_STAGE_COMMITTED) {
    // Already rolling back, rolled back, committed, or zombie — idempotent return success
    mInfo("txn:%" PRIi64 ", stage=%s, rollback already in progress or terminal", txnReq.txnId,
          mndUtxnStageStr(pTxn->stage));
    goto _exit;
  }

  // Merge client-tracked pVgSet into pTxn->pVgList (O(N) hash dedup)
  taosWLockLatch(&pTxn->lock);
  code = mndMergeVgList(pTxn, txnReq.pVgSet);
  taosWUnLockLatch(&pTxn->lock);
  TAOS_CHECK_EXIT(code);

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
    mError("txn:%" PRIi64 ", failed at line %d to rollback since %s", txnReq.txnId, lino, tstrerror(code));
  }
  if (pTxn) mndReleaseTxn(pMnode, pTxn);
  tFreeSMTransReq(&txnReq);

  TAOS_RETURN(code);
}

// ── information_schema.ins_transaction_logs retriever ──────────────────────────────
// Schema (transactionLogsSchema 7 cols): id, create_user, create_time, status, complete_time, type, rollback_reason
static const char *mndTxnStateCommentStr(int8_t state, int8_t reason) {
  if (state == UTXN_STAGE_ROLLEDBACK) {
    switch (reason) {
      case TXN_ROLLBACK_EXPLICIT:
        return "user abort";
      case TXN_ROLLBACK_HB_TIMEOUT:
        return "client timeout";
      case TXN_ROLLBACK_EXCEEDED_LIFETIME:
        return "lifetime timeout";
      default:
        return "unknown";
    }
  }
  return "";
}

static int32_t mndRetrieveTxnLog(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode     *pMnode = pReq->info.node;
  SSdb       *pSdb = pMnode->pSdb;
  int32_t     numOfRows = 0;
  STxnLogObj *pLog = NULL;
  int32_t     cols = 0;
  int32_t     code = 0;
  int32_t     lino = 0;
  char        buf[128 + VARSTR_HEADER_SIZE + 16] = {0};

  while (numOfRows < rows) {
    pShow->pIter = sdbFetch(pSdb, SDB_TXN_LOG, pShow->pIter, (void **)&pLog);
    if (pShow->pIter == NULL) break;

    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&pLog->id, false, pLog, &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols);  // create_user
    STR_WITH_MAXSIZE_TO_VARSTR(buf, pLog->createUser, pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)buf, false, pLog, &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols);  // create_time
    COL_DATA_SET_VAL_GOTO((const char *)&pLog->createTime, false, pLog, &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols);  // complete_time
    COL_DATA_SET_VAL_GOTO((const char *)&pLog->completedAt, false, pLog, &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols);  // status
    STR_WITH_MAXSIZE_TO_VARSTR(buf, mndUtxnStageStr((EUtxnStage)pLog->stage), pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)buf, false, pLog, &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols);  // rollback_reason
    STR_WITH_MAXSIZE_TO_VARSTR(buf, mndTxnStateCommentStr(pLog->stage, pLog->rollbackReason),
                               pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)buf, false, pLog, &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols);  // type = "user"
    STR_WITH_MAXSIZE_TO_VARSTR(buf, "user", pShow->pMeta->pSchemas[cols].bytes);
    COL_DATA_SET_VAL_GOTO((const char *)buf, false, pLog, &lino, _exit);

    numOfRows++;
    sdbRelease(pSdb, pLog);
  }
_exit:
  pShow->numOfRows += numOfRows;
  if (code < 0) {
    mError("failed to retrieve txn logs at line:%d, since %s", lino, tstrerror(code));
    return code;
  }
  return numOfRows;
}

static void mndCancelRetrieveTxnLog(SMnode *pMnode, void *pIter) {
  if (pIter != NULL) sdbCancelFetchByType(pMnode->pSdb, pIter, SDB_TXN_LOG);
}

// ── information_schema.ins_transaction_orphans retriever ─────────────────────────────
// Schema (transactionOrphansSchema 5 cols): id, vgroup_id, first_seen, last_seen, report_count
static int32_t mndRetrieveTxnOrphans(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows) {
  SMnode   *pMnode = pReq->info.node;
  SSdb     *pSdb = pMnode->pSdb;  // required by COL_DATA_SET_VAL_GOTO macro
  SHashObj *pMap = pMnode->txnMgmt.pOrphanTxnMap;
  int32_t   numOfRows = 0;
  int32_t   cols = 0;
  int32_t   code = 0;
  int32_t   lino = 0;

  while (numOfRows < rows) {
    void *pVal = taosHashIterate(pMap, pShow->pIter);
    if (pVal == NULL) {
      pShow->pIter = NULL;
      break;
    }
    pShow->pIter = pVal;

    SOrphanTxnEntry *p = (SOrphanTxnEntry *)pVal;
    cols = 0;
    SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols);
    COL_DATA_SET_VAL_GOTO((const char *)&p->txnId, false, NULL, &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols);  // vgroup_id
    COL_DATA_SET_VAL_GOTO((const char *)&p->vgId, false, NULL, &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols);  // first_seen
    COL_DATA_SET_VAL_GOTO((const char *)&p->firstSeen, false, NULL, &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols);  // last_seen
    COL_DATA_SET_VAL_GOTO((const char *)&p->lastSeen, false, NULL, &lino, _exit);

    pColInfo = taosArrayGet(pBlock->pDataBlock, ++cols);  // report_count
    COL_DATA_SET_VAL_GOTO((const char *)&p->reportCount, false, NULL, &lino, _exit);

    numOfRows++;
  }
_exit:
  pShow->numOfRows += numOfRows;
  if (code < 0) {
    mError("failed to retrieve txn orphans at line:%d, since %s", lino, tstrerror(code));
    return code;
  }
  return numOfRows;
}

static void mndCancelRetrieveTxnOrphans(SMnode *pMnode, void *pIter) {
  if (pIter != NULL) taosHashCancelIterate(pMnode->txnMgmt.pOrphanTxnMap, pIter);
}
