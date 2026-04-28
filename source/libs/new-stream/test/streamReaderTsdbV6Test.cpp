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
 * Stream reader TSDB interface v6.1 unit tests (DS v3.4.2 sub-project C).
 *
 * Scope (TDD spec-as-code):
 *   - Pure-logic helpers from DS §6.1.3:
 *     * getFirstTypeFromNext: first/next type normalization for cache key.
 *     * isFirstPullType: distinguishes first-pull vs continuation requests.
 *
 * The helper implementations below are inline COPIES of the spec given in
 * DS §6.1.3 - they intentionally duplicate the to-be-implemented production
 * code so the spec is executable and locked-in BEFORE production code lands.
 *
 * After the production helpers are introduced (in streamReader.h or a new
 * header), the implementation here will be replaced with `extern "C"`
 * declarations referencing the production symbols, while the test cases
 * themselves stay unchanged - that is the green→green migration step.
 *
 * Out of scope (covered by Python system tests under
 * test/cases/18-StreamProcessing/): vnodeProcessStreamTsdbDataNewReq,
 * vnodeProcessStreamTsdbDataVTableNewReq -
 * these all depend on a live SVnode + tsdbReaderOpen, not unit-testable.
 */

#include <gtest/gtest.h>

extern "C" {
#include "streamMsg.h"
#include "streamReader.h"
}

// ----------------------------------------------------------------------------
// v3.4.2 sub-project C DS v6.1 §12.1 - reference helpers from production header.
// getFirstTypeFromNext / isFirstPullType are static inline in streamReader.h;
// these aliases keep test names stable while exercising the actual production
// implementation (no parallel test-only copy).
// ----------------------------------------------------------------------------

static inline ESTriggerPullType v61_getFirstTypeFromNext(ESTriggerPullType t) {
  return getFirstTypeFromNext(t);
}

static inline bool v61_isFirstPullType(ESTriggerPullType t) {
  return isFirstPullType(t);
}

// ----------------------------------------------------------------------------
// Test fixtures
// ----------------------------------------------------------------------------

class StreamReaderTsdbV6Helpers : public ::testing::Test {};

// ----------------------------------------------------------------------------
// getFirstTypeFromNext: 4 mappings + identity for first types
// ----------------------------------------------------------------------------

TEST_F(StreamReaderTsdbV6Helpers, getFirstTypeFromNext_NonVTableTrigger) {
  EXPECT_EQ(v61_getFirstTypeFromNext(STRIGGER_PULL_TSDB_DATA_NEW_NEXT),
            STRIGGER_PULL_TSDB_DATA_NEW);
}

TEST_F(StreamReaderTsdbV6Helpers, getFirstTypeFromNext_NonVTableCalc) {
  EXPECT_EQ(v61_getFirstTypeFromNext(STRIGGER_PULL_TSDB_DATA_NEW_CALC_NEXT),
            STRIGGER_PULL_TSDB_DATA_NEW_CALC);
}

TEST_F(StreamReaderTsdbV6Helpers, getFirstTypeFromNext_VTableTrigger) {
  EXPECT_EQ(v61_getFirstTypeFromNext(STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_NEXT),
            STRIGGER_PULL_TSDB_DATA_VTABLE_NEW);
}

TEST_F(StreamReaderTsdbV6Helpers, getFirstTypeFromNext_VTableCalc) {
  EXPECT_EQ(v61_getFirstTypeFromNext(STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_CALC_NEXT),
            STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_CALC);
}

TEST_F(StreamReaderTsdbV6Helpers, getFirstTypeFromNext_FirstTypesAreIdentity) {
  EXPECT_EQ(v61_getFirstTypeFromNext(STRIGGER_PULL_TSDB_DATA_NEW),
            STRIGGER_PULL_TSDB_DATA_NEW);
  EXPECT_EQ(v61_getFirstTypeFromNext(STRIGGER_PULL_TSDB_DATA_NEW_CALC),
            STRIGGER_PULL_TSDB_DATA_NEW_CALC);
  EXPECT_EQ(v61_getFirstTypeFromNext(STRIGGER_PULL_TSDB_DATA_VTABLE_NEW),
            STRIGGER_PULL_TSDB_DATA_VTABLE_NEW);
  EXPECT_EQ(v61_getFirstTypeFromNext(STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_CALC),
            STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_CALC);
}

TEST_F(StreamReaderTsdbV6Helpers, getFirstTypeFromNext_UnrelatedTypesAreIdentity) {
  // F9 set-table-history is unrelated to first/next normalization.
  EXPECT_EQ(v61_getFirstTypeFromNext(STRIGGER_PULL_SET_TABLE_HISTORY),
            STRIGGER_PULL_SET_TABLE_HISTORY);
  // Legacy types must also pass through unchanged (DS §10.2: legacy enums kept
  // for editor compatibility but reader-side default branch returns
  // TSDB_CODE_INVALID_MSG_TYPE on receipt).
  EXPECT_EQ(v61_getFirstTypeFromNext(STRIGGER_PULL_SET_TABLE),
            STRIGGER_PULL_SET_TABLE);
  EXPECT_EQ(v61_getFirstTypeFromNext(STRIGGER_PULL_TSDB_DATA),
            STRIGGER_PULL_TSDB_DATA);
}

// ----------------------------------------------------------------------------
// isFirstPullType: 4 first-types true / 4 next-types false
// ----------------------------------------------------------------------------

TEST_F(StreamReaderTsdbV6Helpers, isFirstPullType_AllFirstTypesAreTrue) {
  EXPECT_TRUE(v61_isFirstPullType(STRIGGER_PULL_TSDB_DATA_NEW));
  EXPECT_TRUE(v61_isFirstPullType(STRIGGER_PULL_TSDB_DATA_NEW_CALC));
  EXPECT_TRUE(v61_isFirstPullType(STRIGGER_PULL_TSDB_DATA_VTABLE_NEW));
  EXPECT_TRUE(v61_isFirstPullType(STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_CALC));
}

TEST_F(StreamReaderTsdbV6Helpers, isFirstPullType_AllNextTypesAreFalse) {
  EXPECT_FALSE(v61_isFirstPullType(STRIGGER_PULL_TSDB_DATA_NEW_NEXT));
  EXPECT_FALSE(v61_isFirstPullType(STRIGGER_PULL_TSDB_DATA_NEW_CALC_NEXT));
  EXPECT_FALSE(v61_isFirstPullType(STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_NEXT));
  EXPECT_FALSE(v61_isFirstPullType(STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_CALC_NEXT));
}

TEST_F(StreamReaderTsdbV6Helpers, isFirstPullType_UnrelatedTypesAreTrue) {
  // Types outside the v6.1 first/next pair (e.g. F9, legacy types) are treated
  // as first-types (no normalization applies).
  EXPECT_TRUE(v61_isFirstPullType(STRIGGER_PULL_SET_TABLE_HISTORY));
  EXPECT_TRUE(v61_isFirstPullType(STRIGGER_PULL_SET_TABLE));
}

// ----------------------------------------------------------------------------
// Spec invariant: first/next pair coverage matches the DS v6.1 §6.1.2
// request structs - exactly 4 first-types and 4 next-types are the v6.1 set,
// matching the 4 request struct flavors (F5/F6/F7/F8).
// ----------------------------------------------------------------------------

TEST_F(StreamReaderTsdbV6Helpers, FirstNextPairsAreSymmetric) {
  struct {
    ESTriggerPullType first;
    ESTriggerPullType next;
  } pairs[] = {
      {STRIGGER_PULL_TSDB_DATA_NEW, STRIGGER_PULL_TSDB_DATA_NEW_NEXT},
      {STRIGGER_PULL_TSDB_DATA_NEW_CALC, STRIGGER_PULL_TSDB_DATA_NEW_CALC_NEXT},
      {STRIGGER_PULL_TSDB_DATA_VTABLE_NEW, STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_NEXT},
      {STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_CALC, STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_CALC_NEXT},
  };
  for (auto& p : pairs) {
    EXPECT_TRUE(v61_isFirstPullType(p.first)) << "first=" << p.first;
    EXPECT_FALSE(v61_isFirstPullType(p.next)) << "next=" << p.next;
    EXPECT_EQ(v61_getFirstTypeFromNext(p.next), p.first) << "pair (" << p.first
                                                          << "," << p.next << ")";
    EXPECT_EQ(v61_getFirstTypeFromNext(p.first), p.first);
  }
}

// ----------------------------------------------------------------------------
// Sanity: v6.1 protocol structs from streamMsg.h are present and have the
// expected fields. These checks lock the wire-level shape so any accidental
// removal during refactors triggers a compile error.
// ----------------------------------------------------------------------------

TEST_F(StreamReaderTsdbV6Helpers, RequestStructsHaveExpectedFields) {
  SSTriggerTsdbDataNewRequest req{};
  req.ver = 1;
  req.gid = 0;
  req.skey = 100;
  req.ekey = 200;
  req.order = 1;
  EXPECT_EQ(req.ver, 1);
  EXPECT_EQ(req.gid, 0);   // gid==0 ⇒ cross-uid full table (DS §6.1.2)
  EXPECT_EQ(req.skey, 100);
  EXPECT_EQ(req.ekey, 200);
  EXPECT_EQ(req.order, 1);

  SSTriggerTsdbDataVTableNewRequest vreq{};
  vreq.ver = 7;
  vreq.uid = 2002;
  EXPECT_EQ(vreq.ver, 7);
  EXPECT_EQ(vreq.uid, 2002);
}

// DS v7.0: response no longer carries an explicit `bool eof` field.
// F5/F6 serialize a SArray<SSDataBlock*> via `buildArrayRsp` (sorted by uid via
// `compareBlockInfo`); F7/F8 return a single accumulated `pResBlockDst`.
// Implicit EOF: reader removes its cache entry on `!hasNext`, and the trigger
// side judges round end by "returned rows < STREAM_RETURN_ROWS_TSDB_NUM".

// ============================================================================
// v3.4.2 sub-project C DS v7.0 §6.6 - virtual-table slotId->colId mapping.
// Old "post-read block-level remap" via qStreamRemapBlockBySlotColMap was
// invented in v6.1 and never matched the production code. The real path is
// "pre-read injection": pickSchemasHistory() builds cids[] + slotIdList[] and
// feeds them into tsdbReaderOpen via options.schemas/pSlotList/isSchema=true,
// so tsdbReader internally lays each colId at its target slot. There is no
// reader-side block transform to test here. Coverage for pickSchemasHistory
// belongs in a vnodeStream-side fixture (see DS §12.1).
// ============================================================================
