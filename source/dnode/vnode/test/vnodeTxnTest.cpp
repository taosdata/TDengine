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
#include "vnodeInt.h"
}

SDmNotifyHandle dmNotifyHdl = {.state = 0};

namespace {

struct MockMetaEntrySpec {
  tb_uid_t    uid;
  utxn_id_t   txnId;
  uint8_t     txnStatus;
  int64_t     txnPrevVer;
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
  int32_t                        scanCode = TSDB_CODE_SUCCESS;
  int32_t                        markTxnStatusCode = TSDB_CODE_SUCCESS;
  int32_t                        dropCalls = 0;
  int32_t                        rollbackCalls = 0;
  int32_t                        txnIdxDeleteCalls = 0;
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
  pEntry->txnPrevVer = spec.txnPrevVer;
  pEntry->type = spec.type;
  pEntry->name = spec.name == nullptr ? nullptr : ::strdup(spec.name);
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

void* __real_taosArrayAddBatch(SArray* pArray, const void* pData, int32_t nEles);

void* __wrap_taosArrayAddBatch(SArray* pArray, const void* pData, int32_t nEles) {
  if (g_ctx.failNextArrayAddBatch) {
    g_ctx.failNextArrayAddBatch = false;
    return nullptr;
  }
  return __real_taosArrayAddBatch(pArray, pData, nEles);
}

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

void __wrap_metaFetchEntryFree(SMetaEntry** ppEntry) {
  if (ppEntry == nullptr || *ppEntry == nullptr) {
    return;
  }
  taosMemoryFree((*ppEntry)->name);
  taosMemoryFree(*ppEntry);
  *ppEntry = nullptr;
}

int32_t __wrap_metaDropTable2(SMeta* pMeta, int64_t version, SVDropTbReq* pReq) {
  (void)pMeta;
  (void)version;
  (void)pReq;
  ++g_ctx.dropCalls;
  return g_ctx.dropCode;
}

int32_t __wrap_metaRollbackAlterTable(SMeta* pMeta, int64_t uid, int64_t prevVersion) {
  (void)pMeta;
  (void)uid;
  (void)prevVersion;
  ++g_ctx.rollbackCalls;
  return g_ctx.rollbackCode;
}

int32_t __wrap_metaTxnIdxDelete(SMeta* pMeta, tb_uid_t uid) {
  (void)pMeta;
  (void)uid;
  ++g_ctx.txnIdxDeleteCalls;
  if (g_ctx.txnIdxDeleteCode == TSDB_CODE_NOT_FOUND) {
    return TSDB_CODE_SUCCESS;
  }
  return g_ctx.txnIdxDeleteCode;
}

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

int32_t __wrap_metaMarkTableTxnStatus(SMeta* pMeta, int64_t uid, int64_t txnId, int8_t txnStatus, int64_t txnPrevVer) {
  (void)pMeta;
  (void)uid;
  (void)txnId;
  (void)txnStatus;
  (void)txnPrevVer;
  ++g_ctx.markTxnStatusCalls;
  return g_ctx.markTxnStatusCode;
}

int32_t __wrap_metaScanTxnFinalEntries(SMeta* pMeta, SArray** ppResult) {
  (void)pMeta;
  // Always return an empty array — unit tests don't test Phase 2 rebuild
  SArray* pResult = taosArrayInit(1, sizeof(int64_t) + sizeof(STxnFinalVal));
  if (pResult == nullptr) {
    *ppResult = nullptr;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  *ppResult = pResult;
  return TSDB_CODE_SUCCESS;
}

int32_t __wrap_metaTxnFinalIdxDelete(SMeta* pMeta, int64_t txnId) {
  (void)pMeta;
  (void)txnId;
  return TSDB_CODE_SUCCESS;
}

}  // extern "C"

TEST(vnodeTxnCase, fencingPropagatesPreCreateUndoFailure) {
  resetMockContext();

  constexpr int64_t txnId = 1001;
  constexpr int64_t uid = 9001;

  g_ctx.fetchEntries.push_back(
      {uid, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_precreate")});
  g_ctx.dropCode = TSDB_CODE_VND_TXN_CONFLICT;

  SVnode vnode;
  initTestVnode(&vnode);

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, uid), TSDB_CODE_SUCCESS);

  int32_t code = vnodeTxnFencing(&vnode, 2, txnId + 1);
  EXPECT_EQ(code, g_ctx.dropCode);
  EXPECT_EQ(g_ctx.dropCalls, 1);
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
  g_ctx.dropCode = TSDB_CODE_VND_TXN_CONFLICT;

  SVnode vnode;
  initTestVnode(&vnode);

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId), TSDB_CODE_SUCCESS);
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

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId), TSDB_CODE_SUCCESS);
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

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId), TSDB_CODE_SUCCESS);
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

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId), TSDB_CODE_SUCCESS);
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
  EXPECT_EQ(code, TSDB_CODE_VND_TXN_STALE_TERM);
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
  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnIdKeep), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnIdKeep, uidKeep), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnIdOld), TSDB_CODE_SUCCESS);
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

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId), TSDB_CODE_SUCCESS);
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

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId), TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 1);

  // Double ensure should be idempotent
  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId), TSDB_CODE_SUCCESS);
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

  ASSERT_EQ(vnodeTxnEnsureEntry(&vnode, txnId), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, 9100), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, 9101), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackTable(&vnode, txnId, 9102), TSDB_CODE_SUCCESS);
  ASSERT_EQ(vnodeTxnTrackAlter(&vnode, txnId, 9103, 77), TSDB_CODE_SUCCESS);

  // Single txn entry with 4 tracked uids
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 1);

  vnodeTxnCleanup(&vnode);
}