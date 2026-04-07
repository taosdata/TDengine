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
#include "tmsg.h"
#include "thash.h"
#include "vnodeInt.h"
}

SDmNotifyHandle dmNotifyHdl = {.state = 0};

namespace {

struct MockMetaEntrySpec {
  tb_uid_t  uid;
  utxn_id_t txnId;
  uint8_t   txnStatus;
  int64_t   txnPrevVer;
  int8_t    type;
  int64_t   suid;
  const char* name;
};

struct MockMetaContext {
  std::vector<MockMetaEntrySpec> fetchEntries;
  std::vector<SMetaTxnScanEntry> scanEntries;
  size_t fetchIndex = 0;
  int32_t dropCode = TSDB_CODE_SUCCESS;
  int32_t rollbackCode = TSDB_CODE_SUCCESS;
  int32_t txnIdxDeleteCode = TSDB_CODE_SUCCESS;
  int32_t dropCalls = 0;
  int32_t rollbackCalls = 0;
  int32_t txnIdxDeleteCalls = 0;
  bool failNextArrayAddBatch = false;
};

MockMetaContext g_ctx;

void resetMockContext() {
  g_ctx = MockMetaContext{};
}

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
  return g_ctx.txnIdxDeleteCode;
}

int32_t __wrap_metaScanTxnEntries(SMeta* pMeta, SArray** ppResult) {
  (void)pMeta;
  SArray* pResult = taosArrayInit(g_ctx.scanEntries.size() == 0 ? 1 : g_ctx.scanEntries.size(), sizeof(SMetaTxnScanEntry));
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

}  // extern "C"

TEST(vnodeTxnCase, fencingPropagatesPreCreateUndoFailure) {
  resetMockContext();

  constexpr int64_t txnId = 1001;
  constexpr int64_t uid = 9001;

  g_ctx.fetchEntries.push_back({uid, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0, const_cast<char*>("d0.t_precreate")});
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

  g_ctx.fetchEntries.push_back({uid, txnId, META_TXN_PRE_ALTER, prevVersion, TSDB_NORMAL_TABLE, 0,
                                const_cast<char*>("d0.t_prealter")});
  g_ctx.fetchEntries.push_back({uid, txnId, META_TXN_PRE_CREATE, -1, TSDB_NORMAL_TABLE, 0,
                                const_cast<char*>("d0.t_prealter")});
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

  constexpr int64_t txnId = 1003;
  constexpr int64_t uid = 9003;

  g_ctx.scanEntries.push_back({uid, txnId, META_TXN_PRE_CREATE, -1});

  SVnode vnode;
  initTestVnode(&vnode, 0);

  g_ctx.failNextArrayAddBatch = true;
  int32_t code = vnodeTxnRebuildFromMeta(&vnode);
  EXPECT_NE(code, TSDB_CODE_SUCCESS);
  EXPECT_EQ(taosHashGetSize(vnode.pTxnHash), 1);

  vnodeTxnCleanup(&vnode);
}