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

#include <cstdlib>
#include <cstring>
#include <vector>

#include "gtest/gtest.h"

extern "C" {
#include "monitor.h"
#include "storageapi.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdef.h"
#include "thash.h"
#include "tmsg.h"
#include "vnd.h"
#include "vnodeInt.h"
}

SDmNotifyHandle dmNotifyHdl = {};  // zero-initialize: state=0, sem=zeroed

namespace {

struct MockMetaEntrySpec {
  tb_uid_t    uid;
  txn_id_t    txnId;
  uint8_t     txnStatus;
  int64_t     txnOrigVer;
  int8_t      type;
  int64_t     suid;
  const char* name;
};

struct MockMetaContext {
  std::vector<MockMetaEntrySpec> fetchEntries;
  std::vector<SMetaTxnScanEntry> scanEntries;
  size_t                         fetchIndex = 0;
  int32_t                        dropCode = TSDB_CODE_SUCCESS;
  int32_t                        rollbackCode = TSDB_CODE_SUCCESS;
  int32_t                        txnIdxDeleteCode = TSDB_CODE_SUCCESS;
  int32_t                        txnFinalIdxUpsertCode = TSDB_CODE_SUCCESS;
  int32_t                        vnodeAsyncCode = TSDB_CODE_SUCCESS;
  int32_t                        scanCode = TSDB_CODE_SUCCESS;
  int32_t                        markTxnStatusCode = TSDB_CODE_SUCCESS;
  int32_t                        dropCalls = 0;
  int32_t                        rollbackCalls = 0;
  int32_t                        txnIdxDeleteCalls = 0;
  int32_t                        txnFinalIdxUpsertCalls = 0;
  int32_t                        vnodeAsyncCalls = 0;
  int32_t                        markTxnStatusCalls = 0;
  bool                           failNextArrayAddBatch = false;
};

MockMetaContext g_ctx;

void resetMockContext() { g_ctx = MockMetaContext{}; }

SMetaEntry* cloneMetaEntry(const MockMetaEntrySpec& spec) {
  SMetaEntry* pEntry = static_cast<SMetaEntry*>(taosMemoryCalloc(1, sizeof(SMetaEntry)));
  pEntry->uid = spec.uid;
  pEntry->txnId = spec.txnId;
  pEntry->txnStatus = spec.txnStatus;
  pEntry->txnOrigVer = spec.txnOrigVer;
  pEntry->type = spec.type;
  pEntry->name = spec.name == nullptr ? nullptr : taosStrdup(spec.name);
  pEntry->ctbEntry.suid = spec.suid;
  return pEntry;
}

void initTestVnode(SVnode* pVnode, int64_t term = 1) {
  std::memset(pVnode, 0, sizeof(*pVnode));
  pVnode->config.vgId = 1;
  pVnode->pMeta = reinterpret_cast<SMeta*>(0x1);
  ASSERT_EQ(vnodeTxnInit(pVnode), TSDB_CODE_SUCCESS);
  pVnode->maxSeenTerm = term;
}

}  // namespace

extern "C" {

// ── Mock functions (GNU ld --wrap) ──
// These intercept real implementations to inject faults and track calls.
// Each __wrap_XXX delegates to __real_XXX unless a fault flag is set.

void* __real_taosArrayAddBatch(SArray* pArray, const void* pData, int32_t nEles);

// Mock taosArrayAddBatch: fails once when g_ctx.failNextArrayAddBatch is set
void* __wrap_taosArrayAddBatch(SArray* pArray, const void* pData, int32_t nEles) {
  if (g_ctx.failNextArrayAddBatch) {
    g_ctx.failNextArrayAddBatch = false;
    return nullptr;
  }
  return __real_taosArrayAddBatch(pArray, pData, nEles);
}

// Mock metaFetchEntryByUid: returns entries from g_ctx.fetchEntries in order,
// simulating B+ tree lookups for specific UIDs during fencing/undo operations
int32_t __wrap_metaFetchEntryByUid(SMeta* pMeta, int64_t uid, SMetaEntry** ppEntry) {
  (void)pMeta;
  if (g_ctx.fetchIndex >= g_ctx.fetchEntries.size()) {
    *ppEntry = nullptr;
    return TSDB_CODE_INVALID_PARA;
  }

  const MockMetaEntrySpec& spec = g_ctx.fetchEntries[g_ctx.fetchIndex++];
  if (spec.uid != uid) {
    *ppEntry = nullptr;
    return TSDB_CODE_INVALID_PARA;
  }

  *ppEntry = cloneMetaEntry(spec);
  return TSDB_CODE_SUCCESS;
}

// Mock metaFetchEntryFree: frees the cloned SMetaEntry allocated by __wrap_metaFetchEntryByUid
void __wrap_metaFetchEntryFree(SMetaEntry** ppEntry) {
  if (ppEntry == nullptr || *ppEntry == nullptr) {
    return;
  }
  taosMemoryFree((*ppEntry)->name);
  taosMemoryFree(*ppEntry);
  *ppEntry = nullptr;
}

// Mock metaDropTable2: tracks drop call count, returns configurable error code
int32_t __wrap_metaDropTable2(SMeta* pMeta, int64_t version, SVDropTbReq* pReq) {
  (void)pMeta;
  (void)version;
  (void)pReq;
  ++g_ctx.dropCalls;
  return g_ctx.dropCode;
}

// Mock metaRollbackAlterTable: tracks rollback call count for ALTER undo verification
int32_t __wrap_metaRollbackAlterTable(SMeta* pMeta, int64_t uid, int64_t prevVersion) {
  (void)pMeta;
  (void)uid;
  (void)prevVersion;
  ++g_ctx.rollbackCalls;
  return g_ctx.rollbackCode;
}

// Mock metaTxnIdxDelete: tracks index delete calls, supports NOT_FOUND tolerance
int32_t __wrap_metaTxnIdxDelete(SMeta* pMeta, tb_uid_t uid) {
  (void)pMeta;
  (void)uid;
  ++g_ctx.txnIdxDeleteCalls;
  if (g_ctx.txnIdxDeleteCode == TSDB_CODE_NOT_FOUND) {
    return TSDB_CODE_SUCCESS;
  }
  return g_ctx.txnIdxDeleteCode;
}

// Mock metaScanTxnEntries: returns g_ctx.scanEntries for txn rebuild testing
int32_t __wrap_metaScanTxnEntries(SMeta* pMeta, SArray** ppResult) {
  (void)pMeta;
  if (g_ctx.scanCode != TSDB_CODE_SUCCESS) {
    *ppResult = nullptr;
    return g_ctx.scanCode;
  }
  SArray* pResult =
      taosArrayInit(g_ctx.scanEntries.size() == 0 ? 1 : g_ctx.scanEntries.size(), sizeof(SMetaTxnScanEntry));
  if (pResult == nullptr) {
    *ppResult = nullptr;
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  bool failState = g_ctx.failNextArrayAddBatch;
  g_ctx.failNextArrayAddBatch = false;
  for (const auto& entry : g_ctx.scanEntries) {
    if (taosArrayPush(pResult, &entry) == nullptr) {
      taosArrayDestroy(pResult);
      *ppResult = nullptr;
      g_ctx.failNextArrayAddBatch = failState;
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  g_ctx.failNextArrayAddBatch = failState;
  *ppResult = pResult;
  return TSDB_CODE_SUCCESS;
}

// Mock metaMarkTableTxnStatus: tracks mark call count for PRE_DROP undo verification
int32_t __wrap_metaMarkTableTxnStatus(SMeta* pMeta, int64_t uid, int64_t txnId, int8_t txnStatus, int64_t txnOrigVer) {
  (void)pMeta;
  (void)uid;
  (void)txnId;
  (void)txnStatus;
  (void)txnOrigVer;
  ++g_ctx.markTxnStatusCalls;
  return g_ctx.markTxnStatusCode;
}

// Mock metaScanTxnMetaEntries: always returns empty (unit tests skip Phase 2 rebuild)
int32_t __wrap_metaScanTxnMetaEntries(SMeta* pMeta, SArray** ppResult) {
  (void)pMeta;
  // Always return an empty array — unit tests don't test Phase 2 rebuild
  SArray* pResult = taosArrayInit(1, sizeof(int64_t) + sizeof(STxnMetaVal));
  if (pResult == nullptr) {
    *ppResult = nullptr;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *ppResult = pResult;
  return TSDB_CODE_SUCCESS;
}

// Mock metaTxnMetaDelete: no-op stub for final index cleanup
int32_t __wrap_metaTxnMetaDelete(SMeta* pMeta, int64_t txnId) {
  (void)pMeta;
  (void)txnId;
  return TSDB_CODE_SUCCESS;
}

// Mock metaTxnMetaGet: returns NOT_FOUND so rebuild uses beginWalIndex=0 fallback.
// The real implementation dereferences pMeta->pTxnMeta which is invalid in test context.
int32_t __wrap_metaTxnMetaGet(SMeta* pMeta, int64_t txnId, STxnMetaVal* pVal) {
  (void)pMeta;
  (void)txnId;
  (void)pVal;
  return TSDB_CODE_NOT_FOUND;
}

// Mock metaTxnMetaUpsert: tracks lazy-finalize persistence calls without touching real meta.
// Begin-record writes (status == TXN_META_NONE) from vnodeTxnEnsureEntry are NOT counted,
// since txnFinalIdxUpsertCalls is intended to verify finalization behaviour only.
int32_t __wrap_metaTxnMetaUpsert(SMeta* pMeta, int64_t txnId, const STxnMetaVal* pVal) {
  (void)pMeta;
  (void)txnId;
  if (pVal != NULL && pVal->status != TXN_META_NONE) {
    ++g_ctx.txnFinalIdxUpsertCalls;
  }
  return g_ctx.txnFinalIdxUpsertCode;
}

// Mock vnodeAsync: unit tests verify scheduling intent only, not async execution.
int32_t __wrap_vnodeAsync(int64_t async, EVAPriority priority, int32_t (*execute)(void*), void (*complete)(void*),
                          void* arg, SVATaskID* taskID) {
  (void)async;
  (void)priority;
  (void)execute;
  (void)complete;
  (void)arg;
  ++g_ctx.vnodeAsyncCalls;
  if (taskID != nullptr) {
    taskID->id = 1;
    taskID->async = SCAN_TASK_ASYNC;
  }
  return g_ctx.vnodeAsyncCode;
}

// Mock metaRLock/metaULock: no-op stubs since unit tests use a dummy pMeta pointer.
void __wrap_metaRLock(SMeta* pMeta) { (void)pMeta; }
void __wrap_metaULock(SMeta* pMeta) { (void)pMeta; }

}  // extern "C"

namespace {

std::vector<char> serializeCommitReq(txn_id_t txnId, int64_t term) {
  SVTxnCommitReq    req = {.txnId = txnId, .term = term};
  std::vector<char> buf(32);
  int32_t           len = tSerializeSVTxnCommitReq(buf.data(), static_cast<int32_t>(buf.size()), &req);
  EXPECT_GT(len, 0);
  buf.resize(len);
  return buf;
}

std::vector<char> serializeRollbackReq(txn_id_t txnId, int64_t term, int32_t reason) {
  SVTxnRollbackReq  req = {.txnId = txnId, .term = term, .reason = reason};
  std::vector<char> buf(32);
  int32_t           len = tSerializeSVTxnRollbackReq(buf.data(), static_cast<int32_t>(buf.size()), &req);
  EXPECT_GT(len, 0);
  buf.resize(len);
  return buf;
}

}  // namespace

TEST(vnodeTxnCase, fencingPropagatesPreCreateUndoFailure) {
  resetMockContext();

  constexpr int64_t txnId = 1001;
  constexpr int64_t uid = 9001;

  g_ctx.fetchEntries.push_back(
      {uid, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_precreate")});
  g_ctx.dropCode = TSDB_CODE_TXN_RESOURCE_BUSY;

  SVnode vnode;
  initTestVnode(&vnode);

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, uid), TSDB_CODE_SUCCESS);

  int32_t code = vnodeTxnFencing(&vnode, 2, txnId + 1);
  EXPECT_EQ(code, g_ctx.dropCode);
  EXPECT_EQ(g_ctx.dropCalls, 1);
  // metaDropTable2 failed, so fencing returned early before reaching
  // metaTxnIdxDelete — the index cleanup step must NOT have been attempted.
  EXPECT_EQ(g_ctx.txnIdxDeleteCalls, 0);

  vnodeTxnCleanup(&vnode);
}

TEST(vnodeTxnCase, fencingPropagatesChainedPreCreateCleanupFailureAfterAlterRollback) {
  resetMockContext();

  constexpr int64_t txnId = 1002;
  constexpr int64_t uid = 9002;
  constexpr int64_t prevVersion = 77;

  g_ctx.fetchEntries.push_back(
      {uid, txnId, META_TXN_PRE_ALTER, prevVersion, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_prealter")});
  g_ctx.fetchEntries.push_back(
      {uid, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_prealter")});
  g_ctx.dropCode = TSDB_CODE_TXN_RESOURCE_BUSY;

  SVnode vnode;
  initTestVnode(&vnode);

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackAlter(&vnode, txnId, uid, prevVersion), TSDB_CODE_SUCCESS);

  int32_t code = vnodeTxnFencing(&vnode, 2, txnId + 1);
  EXPECT_EQ(code, g_ctx.dropCode);
  EXPECT_EQ(g_ctx.rollbackCalls, 1);
  EXPECT_EQ(g_ctx.dropCalls, 1);
  EXPECT_EQ(g_ctx.txnIdxDeleteCalls, 0);

  vnodeTxnCleanup(&vnode);
}

TEST(vnodeTxnCase, rebuildFailsFastWhenRecoveredTxnTrackingIsIncomplete) {
  resetMockContext();

  // Simulate failure during rebuild: metaScanTxnEntries returns an error.
  // vnodeTxnRebuildFromMeta should propagate the error immediately.
  g_ctx.scanCode = TSDB_CODE_OUT_OF_MEMORY;

  SVnode vnode;
  initTestVnode(&vnode, 0);

  int32_t code = vnodeTxnRebuildFromMeta(&vnode);
  EXPECT_NE(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 0);

  vnodeTxnCleanup(&vnode);
}

TEST(vnodeTxnCase, fencingPropagatesTxnIdxDeleteFailure) {
  resetMockContext();

  constexpr int64_t txnId = 1003;
  constexpr int64_t uid = 9003;

  g_ctx.fetchEntries.push_back(
      {uid, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_delete_fail")});
  g_ctx.dropCode = TSDB_CODE_SUCCESS;
  g_ctx.txnIdxDeleteCode = TSDB_CODE_OUT_OF_MEMORY;

  SVnode vnode;
  initTestVnode(&vnode);

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, uid), TSDB_CODE_SUCCESS);

  int32_t code = vnodeTxnFencing(&vnode, 2, txnId + 1);
  EXPECT_EQ(code, g_ctx.txnIdxDeleteCode);
  EXPECT_EQ(g_ctx.dropCalls, 1);
  EXPECT_EQ(g_ctx.txnIdxDeleteCalls, 1);

  vnodeTxnCleanup(&vnode);
}

TEST(vnodeTxnCase, fencingToleratesTxnIdxDeleteNotFound) {
  resetMockContext();

  constexpr int64_t txnId = 1004;
  constexpr int64_t uid = 9004;

  g_ctx.fetchEntries.push_back(
      {uid, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_delete_not_found")});
  g_ctx.dropCode = TSDB_CODE_SUCCESS;
  g_ctx.txnIdxDeleteCode = TSDB_CODE_NOT_FOUND;

  SVnode vnode;
  initTestVnode(&vnode);

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, uid), TSDB_CODE_SUCCESS);

  int32_t code = vnodeTxnFencing(&vnode, 2, txnId + 1);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(g_ctx.dropCalls, 1);
  EXPECT_EQ(g_ctx.txnIdxDeleteCalls, 1);

  vnodeTxnCleanup(&vnode);
}

// =========================================================================
// 6. Rebuild correctly populates txn hash from scan entries
// =========================================================================
TEST(vnodeTxnCase, rebuildPopulatesTxnHashFromScanEntries) {
  resetMockContext();

  // Simulate 3 entries from 2 different transactions in the txn.idx scan
  g_ctx.scanEntries.push_back({9010, 2001, META_TXN_PRE_CREATE, -1});
  g_ctx.scanEntries.push_back({9011, 2001, META_TXN_PRE_DROP, -1});
  g_ctx.scanEntries.push_back({9012, 2002, META_TXN_PRE_ALTER, 42});

  SVnode vnode;
  initTestVnode(&vnode, 0);

  int32_t code = vnodeTxnRebuildFromMeta(&vnode);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  // Should have 2 txn entries (txnId 2001 and 2002) in the hash
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 2);

  vnodeTxnCleanup(&vnode);
}

// =========================================================================
// 7. Rebuild with empty scan entries → empty txn hash
// =========================================================================
TEST(vnodeTxnCase, rebuildWithEmptyScanProducesEmptyHash) {
  resetMockContext();
  // No scan entries → empty hash

  SVnode vnode;
  initTestVnode(&vnode, 0);

  int32_t code = vnodeTxnRebuildFromMeta(&vnode);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 0);

  vnodeTxnCleanup(&vnode);
}

// =========================================================================
// 8. Fencing skips same-term (no-op)
// =========================================================================
TEST(vnodeTxnCase, fencingSameTermIsNoOp) {
  resetMockContext();

  constexpr int64_t txnId = 1010;
  constexpr int64_t uid = 9010;

  SVnode vnode;
  initTestVnode(&vnode, 5);  // maxSeenTerm = 5

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, uid), TSDB_CODE_SUCCESS);

  // Same term (5) with different txnId → no fencing, just no-op
  int32_t code = vnodeTxnFencing(&vnode, 5, txnId + 1);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  // The old txn entry should still be present (not rolled back)
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 1);
  EXPECT_EQ(g_ctx.dropCalls, 0);

  vnodeTxnCleanup(&vnode);
}

// =========================================================================
// 9. Fencing rejects stale term
// =========================================================================
TEST(vnodeTxnCase, fencingRejectsOldTerm) {
  resetMockContext();

  SVnode vnode;
  initTestVnode(&vnode, 10);  // maxSeenTerm = 10

  // Lower term should be rejected
  int32_t code = vnodeTxnFencing(&vnode, 5, 9999);
  EXPECT_EQ(code, TSDB_CODE_TXN_STALE_TERM);
  EXPECT_EQ(g_ctx.dropCalls, 0);

  vnodeTxnCleanup(&vnode);
}

// =========================================================================
// 10. Multiple txns in hash, fencing only removes non-excluded
// =========================================================================
TEST(vnodeTxnCase, fencingPreservesExcludedTxn) {
  resetMockContext();

  constexpr int64_t txnIdKeep = 3001;
  constexpr int64_t txnIdOld = 3002;
  constexpr int64_t uidKeep = 9020;
  constexpr int64_t uidOld = 9021;

  // When fencing fetches the old entry for undo, we need to provide its spec
  g_ctx.fetchEntries.push_back(
      {uidOld, txnIdOld, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_old")});
  g_ctx.dropCode = TSDB_CODE_SUCCESS;

  SVnode vnode;
  initTestVnode(&vnode, 1);

  // Register two txns
  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnIdKeep, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnIdKeep, uidKeep), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnIdOld, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnIdOld, uidOld), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 2);

  // Fencing with newTerm=2, excluding txnIdKeep → should rollback txnIdOld
  int32_t code = vnodeTxnFencing(&vnode, 2, txnIdKeep);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  // txnIdKeep should still be in hash; txnIdOld should be removed
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 1);
  EXPECT_EQ(g_ctx.dropCalls, 1);  // PRE_CREATE undo for txnIdOld

  vnodeTxnCleanup(&vnode);
}

// =========================================================================
// 11. PRE_DROP rollback restores table (via fencing)
// =========================================================================
TEST(vnodeTxnCase, fencingRollbackPreDropRestoresTable) {
  resetMockContext();

  constexpr int64_t txnId = 4001;
  constexpr int64_t uid = 9030;

  // PRE_DROP: fencing should call metaMarkTableTxnStatus to clear txn markers
  // (not metaDropTable2 — that's for PRE_CREATE cleanup)
  g_ctx.fetchEntries.push_back(
      {uid, txnId, META_TXN_PRE_DROP, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_predrop")});

  SVnode vnode;
  initTestVnode(&vnode, 1);

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, uid), TSDB_CODE_SUCCESS);

  // Fencing with higher term, excluding a different txnId
  int32_t code = vnodeTxnFencing(&vnode, 2, txnId + 1);
  EXPECT_EQ(code, TSDB_CODE_SUCCESS);
  // PRE_DROP rollback should NOT call metaDropTable2 (that destroys the table)
  // It should call metaMarkTableTxnStatus to clear txn markers
  EXPECT_EQ(g_ctx.dropCalls, 0);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 0);

  vnodeTxnCleanup(&vnode);
}

// =========================================================================
// 12. Ensure entry is idempotent (double call)
// =========================================================================
TEST(vnodeTxnCase, ensureEntryIdempotent) {
  resetMockContext();

  constexpr int64_t txnId = 5001;

  SVnode vnode;
  initTestVnode(&vnode);

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId, 0), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 1);

  // Double ensure should be idempotent
  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId, 0), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 1);

  vnodeTxnCleanup(&vnode);
}

// =========================================================================
// 13. Track multiple UIDs in same txn
// =========================================================================
TEST(vnodeTxnCase, trackMultipleUidsInSameTxn) {
  resetMockContext();

  constexpr int64_t txnId = 5002;

  SVnode vnode;
  initTestVnode(&vnode);

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, 9100), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, 9101), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, 9102), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackAlter(&vnode, txnId, 9103, 77), TSDB_CODE_SUCCESS);

  // Single txn entry with 4 tracked uids
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 1);

  vnodeTxnCleanup(&vnode);
}

TEST(vnodeTxnCase, inlineCommitPartialFailureFallsBackToLazyFinalize) {
  resetMockContext();

  constexpr int64_t txnId = 6001;
  constexpr int64_t uid = 9201;

  g_ctx.fetchEntries.push_back(
      {uid, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_commit_partial")});
  g_ctx.markTxnStatusCode = TSDB_CODE_OUT_OF_MEMORY;

  SVnode vnode;
  initTestVnode(&vnode, 1);

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, uid), TSDB_CODE_SUCCESS);

  std::vector<char> reqBuf = serializeCommitReq(txnId, 1);
  int32_t code = vnodeProcessTxnCommitReq(&vnode, 0, reqBuf.data(), static_cast<int32_t>(reqBuf.size()), nullptr);

  EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(g_ctx.markTxnStatusCalls, 1);
  EXPECT_EQ(g_ctx.txnIdxDeleteCalls, 0);
  EXPECT_EQ(g_ctx.txnFinalIdxUpsertCalls, 1);
  EXPECT_EQ(g_ctx.vnodeAsyncCalls, 1);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 1);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnMetaCache), 1);

  auto* pFinalStatus = static_cast<int8_t*>(taosHashGet(vnode.pTxnMetaCache, &txnId, sizeof(txnId)));
  ASSERT_NE(pFinalStatus, nullptr);
  EXPECT_EQ(*pFinalStatus, TXN_META_COMMITTED);

  vnodeTxnCleanup(&vnode);
}

TEST(vnodeTxnCase, rollbackAlterFailureKeepsTxnEntryRetryable) {
  resetMockContext();

  constexpr int64_t txnId = 6002;
  constexpr int64_t uid = 9202;
  constexpr int64_t prevVersion = 88;

  g_ctx.fetchEntries.push_back(
      {uid, txnId, META_TXN_PRE_ALTER, prevVersion, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_rollback_alter")});
  g_ctx.rollbackCode = TSDB_CODE_OUT_OF_MEMORY;

  SVnode vnode;
  initTestVnode(&vnode, 1);

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackAlter(&vnode, txnId, uid, prevVersion), TSDB_CODE_SUCCESS);

  std::vector<char> reqBuf = serializeRollbackReq(txnId, 1, 0);
  int32_t code = vnodeProcessTxnRollbackReq(&vnode, 0, reqBuf.data(), static_cast<int32_t>(reqBuf.size()), nullptr);

  EXPECT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);
  EXPECT_EQ(g_ctx.rollbackCalls, 1);
  EXPECT_EQ(g_ctx.txnIdxDeleteCalls, 0);
  EXPECT_EQ(g_ctx.txnFinalIdxUpsertCalls, 0);
  EXPECT_EQ(g_ctx.vnodeAsyncCalls, 0);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 1);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnMetaCache), 0);

  auto* pFinalStatus = static_cast<int8_t*>(taosHashGet(vnode.pTxnMetaCache, &txnId, sizeof(txnId)));
  EXPECT_EQ(pFinalStatus, nullptr);

  vnodeTxnCleanup(&vnode);
}

// =========================================================================
// 14. Vacuum failure midway: vacuumIdx only advances for successful ops
// =========================================================================
// Covers the gap: VNode async vacuum failure mid-way verification.
// Forces inline commit to fail (OOM) → lazy finalize → vacuum needed.
// Then runs vnodeTxnVacuumBatch with injected failures to verify partial
// progress and resumability.
TEST(vnodeTxnCase, vacuumFailureMidwayStopsAtFailedUid) {
  resetMockContext();

  constexpr int64_t  txnId = 7001;
  constexpr tb_uid_t uid1 = 9301;
  constexpr tb_uid_t uid2 = 9302;
  constexpr tb_uid_t uid3 = 9303;

  // Setup fetch entries for the inline commit path (all 3 fetched during promote).
  // markTxnStatusCode=OOM → all 3 inline promotes fail → fallback to lazy finalize.
  g_ctx.fetchEntries.push_back(
      {uid1, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_vac1")});
  g_ctx.fetchEntries.push_back(
      {uid2, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_vac2")});
  g_ctx.fetchEntries.push_back(
      {uid3, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_vac3")});
  g_ctx.markTxnStatusCode = TSDB_CODE_OUT_OF_MEMORY;  // force inline failure

  SVnode vnode;
  initTestVnode(&vnode, 1);

  // Setup: create entry, track 3 UIDs
  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId, 0), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, uid1), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, uid2), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, uid3), TSDB_CODE_SUCCESS);

  // Commit: inline promote fails for all UIDs → falls back to lazy finalize
  std::vector<char> reqBuf = serializeCommitReq(txnId, 1);
  int32_t code = vnodeProcessTxnCommitReq(&vnode, 0, reqBuf.data(), static_cast<int32_t>(reqBuf.size()), nullptr);
  ASSERT_EQ(code, TSDB_CODE_OUT_OF_MEMORY);  // inline failed → returned error

  // Verify lazy finalization occurred (entry preserved for async vacuum)
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 1);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnMetaCache), 1);
  auto* pFinalStatus = static_cast<int8_t*>(taosHashGet(vnode.pTxnMetaCache, &txnId, sizeof(txnId)));
  ASSERT_NE(pFinalStatus, nullptr);
  EXPECT_EQ(*pFinalStatus, TXN_META_COMMITTED);

  // ---- Vacuum phase 1: uid1 succeeds (maxOps=1 → process only uid1) ----
  resetMockContext();
  g_ctx.fetchEntries.push_back(
      {uid1, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_vac1")});
  g_ctx.markTxnStatusCode = TSDB_CODE_SUCCESS;

  int32_t processed = vnodeTxnVacuumBatch(&vnode, 1);
  EXPECT_EQ(processed, 1);
  EXPECT_EQ(g_ctx.markTxnStatusCalls, 1);
  EXPECT_EQ(g_ctx.txnIdxDeleteCalls, 1);
  // Entry still present (2 UIDs remaining)
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 1);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnMetaCache), 1);

  // ---- Vacuum phase 2: uid2 fails → stops, vacuumIdx stays ----
  resetMockContext();
  g_ctx.fetchEntries.push_back(
      {uid2, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_vac2")});
  g_ctx.markTxnStatusCode = TSDB_CODE_OUT_OF_MEMORY;

  processed = vnodeTxnVacuumBatch(&vnode, 10);
  EXPECT_EQ(processed, 0);                 // no successful progress
  EXPECT_EQ(g_ctx.markTxnStatusCalls, 1);  // attempted once
  EXPECT_EQ(g_ctx.txnIdxDeleteCalls, 0);   // not reached (op failed)
  // Entry still present (failure didn't advance)
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 1);

  // ---- Vacuum phase 3: retry uid2 + uid3 both succeed ----
  resetMockContext();
  g_ctx.fetchEntries.push_back(
      {uid2, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_vac2")});
  g_ctx.fetchEntries.push_back(
      {uid3, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_vac3")});
  g_ctx.markTxnStatusCode = TSDB_CODE_SUCCESS;

  processed = vnodeTxnVacuumBatch(&vnode, 10);
  EXPECT_EQ(processed, 2);
  // After full vacuum, vnodeTxnVacuumBatch removes the entry from hashes.
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 0);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnMetaCache), 0);

  vnodeTxnCleanup(&vnode);
}