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

/**
 * Unit tests for meta snapshot start-version adjustment when pending
 * meta transactions exist (§ incremental snapshot with txn).
 *
 * Tests verify:
 * 1. sver is adjusted to min(original_sver, min_version_of_txn_entries)
 * 2. PRE_ALTER prevVer is also considered in the minimum calculation
 * 3. Idempotent replay safety for CREATE/DROP/ALTER
 * 4. No adjustment when no pending txn entries exist
 * 5. No adjustment when all txn entries have version >= sver
 */

#include <cstdlib>
#include <cstring>
#include <vector>

#include "gtest/gtest.h"

extern "C" {
#include "meta.h"
#include "taosdef.h"
#include "taoserror.h"
#include "tarray.h"
#include "tdef.h"
#include "thash.h"
#include "tmsg.h"
#include "vnd.h"
#include "vnodeInt.h"
}

// SMetaSnapReader is defined in metaSnapshot.c (file-local).
// Redeclare here for field access in tests.
struct SMetaSnapReader {
  SMeta*    pMeta;
  int64_t   sver;
  int64_t   ever;
  TBC*      pTbc;
  int32_t   iLoop;
  SHashObj* pPrevVerNeeded;
};

SDmNotifyHandle dmNotifyHdl = {};

namespace {

// Mock state for controlling metaGetInfo and metaScanTxnEntries behavior
struct MockSnapContext {
  // Map of uid → version for metaGetInfo
  std::vector<std::pair<tb_uid_t, int64_t>> uidVersions;
  // Txn scan entries to return from metaScanTxnEntries
  std::vector<SMetaTxnScanEntry> scanEntries;
  // Error injection
  int32_t scanCode = TSDB_CODE_SUCCESS;
  int32_t getInfoCode = TSDB_CODE_SUCCESS;
  // TDB cursor behavior
  int32_t tbcOpenCode = TSDB_CODE_SUCCESS;
  int32_t tbcMoveToCode = TSDB_CODE_SUCCESS;
};

MockSnapContext g_snapCtx;

void resetSnapContext() { g_snapCtx = MockSnapContext{}; }

int64_t getVersionForUid(tb_uid_t uid) {
  for (const auto& p : g_snapCtx.uidVersions) {
    if (p.first == uid) return p.second;
  }
  return -1;
}

}  // namespace

extern "C" {

// ── Mock metaGetInfo: returns version from g_snapCtx.uidVersions ──
int32_t __wrap_metaGetInfo(SMeta* pMeta, int64_t uid, SMetaInfo* pInfo, SMetaReader* pReader) {
  (void)pMeta;
  (void)pReader;
  if (g_snapCtx.getInfoCode != TSDB_CODE_SUCCESS) {
    return g_snapCtx.getInfoCode;
  }
  int64_t ver = getVersionForUid(uid);
  if (ver < 0) {
    return TSDB_CODE_TDB_TABLE_NOT_EXIST;
  }
  pInfo->uid = uid;
  pInfo->version = ver;
  pInfo->suid = 0;
  pInfo->skmVer = 1;
  return TSDB_CODE_SUCCESS;
}

// ── Mock metaScanTxnEntries: returns g_snapCtx.scanEntries ──
int32_t __wrap_metaScanTxnEntries(SMeta* pMeta, SArray** ppResult) {
  (void)pMeta;
  if (g_snapCtx.scanCode != TSDB_CODE_SUCCESS) {
    *ppResult = nullptr;
    return g_snapCtx.scanCode;
  }
  size_t  n = g_snapCtx.scanEntries.size();
  SArray* pResult = taosArrayInit(n == 0 ? 1 : n, sizeof(SMetaTxnScanEntry));
  if (pResult == nullptr) {
    *ppResult = nullptr;
    return TSDB_CODE_OUT_OF_MEMORY;
  }
  for (const auto& entry : g_snapCtx.scanEntries) {
    if (taosArrayPush(pResult, &entry) == nullptr) {
      taosArrayDestroy(pResult);
      *ppResult = nullptr;
      return TSDB_CODE_OUT_OF_MEMORY;
    }
  }
  *ppResult = pResult;
  return TSDB_CODE_SUCCESS;
}

// ── Mock tdbTbcOpen: always succeeds, returns a dummy cursor ──
int32_t __wrap_tdbTbcOpen(TTB* pTb, TBC** ppTbc, TXN* pTxn) {
  (void)pTb;
  (void)pTxn;
  if (g_snapCtx.tbcOpenCode != TSDB_CODE_SUCCESS) {
    *ppTbc = nullptr;
    return g_snapCtx.tbcOpenCode;
  }
  // Allocate a minimal dummy cursor (just needs to be non-NULL and freeable)
  *ppTbc = (TBC*)taosMemoryCalloc(1, 64);
  return TSDB_CODE_SUCCESS;
}

// ── Mock tdbTbcMoveTo: always succeeds ──
int32_t __wrap_tdbTbcMoveTo(TBC* pTbc, const void* pKey, int32_t kLen, int32_t* c) {
  (void)pTbc;
  (void)pKey;
  (void)kLen;
  if (c) *c = 0;
  return g_snapCtx.tbcMoveToCode;
}

// ── Mock tdbTbcClose: free the dummy cursor ──
int32_t __wrap_tdbTbcClose(TBC* pTbc) {
  if (pTbc) taosMemoryFree(pTbc);
  return 0;
}

// ── Mock metaRLock/metaULock/metaWLock: no-op ──
void __wrap_metaRLock(SMeta* pMeta) { (void)pMeta; }
void __wrap_metaULock(SMeta* pMeta) { (void)pMeta; }
void __wrap_metaWLock(SMeta* pMeta) { (void)pMeta; }

// ── Mock metaCacheGet: always miss (force TDB lookup path) ──
int32_t __wrap_metaCacheGet(SMeta* pMeta, int64_t uid, SMetaInfo* pInfo) {
  (void)pMeta;
  (void)uid;
  (void)pInfo;
  return TSDB_CODE_NOT_FOUND;
}

// ── Mock metaCacheUpsert: no-op ──
int32_t __wrap_metaCacheUpsert(SMeta* pMeta, SMetaInfo* pInfo) {
  (void)pMeta;
  (void)pInfo;
  return TSDB_CODE_SUCCESS;
}

}  // extern "C"

// ============================================================================
// Test: No pending transactions → sver unchanged
// ============================================================================
TEST(MetaSnapTxn, NoTxnEntries_SverUnchanged) {
  resetSnapContext();
  // No txn entries
  g_snapCtx.scanEntries.clear();

  // Create a dummy SMeta with pTbDb pointing to something (won't be dereferenced
  // because tdbTbcOpen is mocked)
  SMeta  meta = {};
  SVnode vnode = {};
  vnode.config.vgId = 1;
  meta.pVnode = &vnode;
  meta.pTbDb = (TTB*)0x1;  // dummy, won't be dereferenced

  SMetaSnapReader* pReader = nullptr;
  int32_t          code = metaSnapReaderOpen(&meta, 100, 200, &pReader);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pReader, nullptr);
  EXPECT_EQ(pReader->sver, 100);  // unchanged
  EXPECT_EQ(pReader->ever, 200);

  metaSnapReaderClose(&pReader);
}

// ============================================================================
// Test: Pending PRE_CREATE with version < sver → sver adjusted
// ============================================================================
TEST(MetaSnapTxn, PreCreate_VersionBelowSver_AdjustsSver) {
  resetSnapContext();

  // txn entry: uid=1001, PRE_CREATE, version in pUidIdx is 50
  g_snapCtx.scanEntries.push_back({.uid = 1001, .txnId = 1, .txnStatus = META_TXN_PRE_CREATE, .txnPrevVer = -1});
  g_snapCtx.uidVersions.push_back({1001, 50});

  SMeta  meta = {};
  SVnode vnode = {};
  vnode.config.vgId = 2;
  meta.pVnode = &vnode;
  meta.pTbDb = (TTB*)0x1;

  SMetaSnapReader* pReader = nullptr;
  int32_t          code = metaSnapReaderOpen(&meta, 100, 200, &pReader);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pReader, nullptr);
  // sver should be adjusted to 50 (min of txn entry versions)
  EXPECT_EQ(pReader->sver, 50);

  metaSnapReaderClose(&pReader);
}

// ============================================================================
// Test: Pending PRE_DROP with version < sver → sver adjusted
// ============================================================================
TEST(MetaSnapTxn, PreDrop_VersionBelowSver_AdjustsSver) {
  resetSnapContext();

  g_snapCtx.scanEntries.push_back({.uid = 2001, .txnId = 2, .txnStatus = META_TXN_PRE_DROP, .txnPrevVer = -1});
  g_snapCtx.uidVersions.push_back({2001, 30});

  SMeta  meta = {};
  SVnode vnode = {};
  vnode.config.vgId = 3;
  meta.pVnode = &vnode;
  meta.pTbDb = (TTB*)0x1;

  SMetaSnapReader* pReader = nullptr;
  int32_t          code = metaSnapReaderOpen(&meta, 100, 200, &pReader);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pReader, nullptr);
  EXPECT_EQ(pReader->sver, 30);

  metaSnapReaderClose(&pReader);
}

// ============================================================================
// Test: PRE_ALTER with txnPrevVer < version < sver → sver uses prevVer
// ============================================================================
TEST(MetaSnapTxn, PreAlter_PrevVerBelowVersion_UsesPrevVer) {
  resetSnapContext();

  // uid=3001: current version=80 (below sver=100), but txnPrevVer=20 is even lower
  g_snapCtx.scanEntries.push_back({.uid = 3001, .txnId = 3, .txnStatus = META_TXN_PRE_ALTER, .txnPrevVer = 20});
  g_snapCtx.uidVersions.push_back({3001, 80});

  SMeta  meta = {};
  SVnode vnode = {};
  vnode.config.vgId = 4;
  meta.pVnode = &vnode;
  meta.pTbDb = (TTB*)0x1;

  SMetaSnapReader* pReader = nullptr;
  int32_t          code = metaSnapReaderOpen(&meta, 100, 200, &pReader);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pReader, nullptr);
  // Should use min(80, 20) = 20
  EXPECT_EQ(pReader->sver, 20);

  metaSnapReaderClose(&pReader);
}

// ============================================================================
// Test: All txn entry versions >= sver → no adjustment
// ============================================================================
TEST(MetaSnapTxn, AllVersionsAboveSver_NoAdjustment) {
  resetSnapContext();

  g_snapCtx.scanEntries.push_back({.uid = 4001, .txnId = 4, .txnStatus = META_TXN_PRE_CREATE, .txnPrevVer = -1});
  g_snapCtx.scanEntries.push_back({.uid = 4002, .txnId = 4, .txnStatus = META_TXN_PRE_DROP, .txnPrevVer = -1});
  g_snapCtx.uidVersions.push_back({4001, 150});
  g_snapCtx.uidVersions.push_back({4002, 180});

  SMeta  meta = {};
  SVnode vnode = {};
  vnode.config.vgId = 5;
  meta.pVnode = &vnode;
  meta.pTbDb = (TTB*)0x1;

  SMetaSnapReader* pReader = nullptr;
  int32_t          code = metaSnapReaderOpen(&meta, 100, 200, &pReader);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pReader, nullptr);
  EXPECT_EQ(pReader->sver, 100);  // unchanged

  metaSnapReaderClose(&pReader);
}

// ============================================================================
// Test: Multiple txn entries, min version wins
// ============================================================================
TEST(MetaSnapTxn, MultipleTxnEntries_MinVersionWins) {
  resetSnapContext();

  g_snapCtx.scanEntries.push_back({.uid = 5001, .txnId = 5, .txnStatus = META_TXN_PRE_CREATE, .txnPrevVer = -1});
  g_snapCtx.scanEntries.push_back({.uid = 5002, .txnId = 5, .txnStatus = META_TXN_PRE_DROP, .txnPrevVer = -1});
  g_snapCtx.scanEntries.push_back({.uid = 5003, .txnId = 6, .txnStatus = META_TXN_PRE_ALTER, .txnPrevVer = 10});
  g_snapCtx.uidVersions.push_back({5001, 70});
  g_snapCtx.uidVersions.push_back({5002, 40});
  g_snapCtx.uidVersions.push_back({5003, 90});

  SMeta  meta = {};
  SVnode vnode = {};
  vnode.config.vgId = 6;
  meta.pVnode = &vnode;
  meta.pTbDb = (TTB*)0x1;

  SMetaSnapReader* pReader = nullptr;
  int32_t          code = metaSnapReaderOpen(&meta, 100, 200, &pReader);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pReader, nullptr);
  // min(70, 40, 90, prevVer=10) = 10
  EXPECT_EQ(pReader->sver, 10);

  metaSnapReaderClose(&pReader);
}

// ============================================================================
// Test: metaScanTxnEntries fails → sver unchanged (best-effort)
// ============================================================================
TEST(MetaSnapTxn, ScanFails_SverUnchanged) {
  resetSnapContext();
  g_snapCtx.scanCode = TSDB_CODE_INTERNAL_ERROR;

  SMeta  meta = {};
  SVnode vnode = {};
  vnode.config.vgId = 7;
  meta.pVnode = &vnode;
  meta.pTbDb = (TTB*)0x1;

  SMetaSnapReader* pReader = nullptr;
  int32_t          code = metaSnapReaderOpen(&meta, 100, 200, &pReader);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pReader, nullptr);
  EXPECT_EQ(pReader->sver, 100);  // unchanged, best-effort

  metaSnapReaderClose(&pReader);
}

// ============================================================================
// Test: metaGetInfo fails for one uid → skip that uid, use others
// ============================================================================
TEST(MetaSnapTxn, GetInfoFailsForOneUid_SkipsIt) {
  resetSnapContext();

  // uid=6001 will fail lookup (not in uidVersions)
  // uid=6002 has version=60
  g_snapCtx.scanEntries.push_back({.uid = 6001, .txnId = 7, .txnStatus = META_TXN_PRE_CREATE, .txnPrevVer = -1});
  g_snapCtx.scanEntries.push_back({.uid = 6002, .txnId = 7, .txnStatus = META_TXN_PRE_CREATE, .txnPrevVer = -1});
  // Only uid=6002 has a version mapping
  g_snapCtx.uidVersions.push_back({6002, 60});

  SMeta  meta = {};
  SVnode vnode = {};
  vnode.config.vgId = 8;
  meta.pVnode = &vnode;
  meta.pTbDb = (TTB*)0x1;

  SMetaSnapReader* pReader = nullptr;
  int32_t          code = metaSnapReaderOpen(&meta, 100, 200, &pReader);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pReader, nullptr);
  EXPECT_EQ(pReader->sver, 60);  // only uid=6002's version is considered

  metaSnapReaderClose(&pReader);
}

// ============================================================================
// Test: PRE_ALTER with prevVer below adjusted sver → enters pPrevVerNeeded map
// ============================================================================
TEST(MetaSnapTxn, PreAlter_PrevVerBelowAdjustedSver_BuildsRescueMap) {
  resetSnapContext();

  // uid=7001: version=50, PRE_ALTER with prevVer=5
  // After sver adjustment: sver becomes 5 (from prevVer)
  // The prevVer < new sver check: 5 < 5 is false, so NOT in rescue map
  // uid=7002: version=60, PRE_ALTER with prevVer=3
  // minTxnVer = min(50, 5, 60, 3) = 3
  // After adjustment: sver=3. prevVer=5 >= 3, not in map. prevVer=3 >= 3, not in map.
  g_snapCtx.scanEntries.push_back({.uid = 7001, .txnId = 8, .txnStatus = META_TXN_PRE_ALTER, .txnPrevVer = 5});
  g_snapCtx.scanEntries.push_back({.uid = 7002, .txnId = 8, .txnStatus = META_TXN_PRE_ALTER, .txnPrevVer = 3});
  g_snapCtx.uidVersions.push_back({7001, 50});
  g_snapCtx.uidVersions.push_back({7002, 60});

  SMeta  meta = {};
  SVnode vnode = {};
  vnode.config.vgId = 9;
  meta.pVnode = &vnode;
  meta.pTbDb = (TTB*)0x1;

  SMetaSnapReader* pReader = nullptr;
  int32_t          code = metaSnapReaderOpen(&meta, 100, 200, &pReader);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pReader, nullptr);
  // min(50, 5, 60, 3) = 3
  EXPECT_EQ(pReader->sver, 3);
  // Both prevVers (5, 3) are >= adjusted sver (3), so no rescue map needed
  EXPECT_EQ(pReader->pPrevVerNeeded, nullptr);

  metaSnapReaderClose(&pReader);
}

// ============================================================================
// Test: sver=0 (full snapshot) → no adjustment needed
// ============================================================================
TEST(MetaSnapTxn, FullSnapshot_SverZero_NoAdjustment) {
  resetSnapContext();

  g_snapCtx.scanEntries.push_back({.uid = 8001, .txnId = 9, .txnStatus = META_TXN_PRE_CREATE, .txnPrevVer = -1});
  g_snapCtx.uidVersions.push_back({8001, 50});

  SMeta  meta = {};
  SVnode vnode = {};
  vnode.config.vgId = 10;
  meta.pVnode = &vnode;
  meta.pTbDb = (TTB*)0x1;

  SMetaSnapReader* pReader = nullptr;
  int32_t          code = metaSnapReaderOpen(&meta, 0, 200, &pReader);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_NE(pReader, nullptr);
  // sver=0 means full snapshot, 50 > 0 so no adjustment
  EXPECT_EQ(pReader->sver, 0);

  metaSnapReaderClose(&pReader);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
