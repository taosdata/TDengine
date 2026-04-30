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

#include <cstring>
#include <string>
#include <vector>

extern "C" {
#include "os.h"
#include "stream.h"
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

// ============================================================================
// DS §12.3 C1: 8 new ESTriggerPullType values must round-trip through the
// wire codec (tSerialize / tDeserialize / tDestroy) without losing fields.
// ============================================================================
class StreamMsgWireRoundTrip : public ::testing::Test {};

namespace {
struct PullPayload {
  int64_t v1, v2, v3, v4;  // gid/skey/ekey or suid/uid/skey/ekey
  int8_t  order;
};

static void encodeAndDecode(const SSTriggerPullRequest* in,
                            SSTriggerPullRequestUnion* out) {
  // Two-pass encode: probe size, then serialize into a sized buffer.
  int32_t tlen = tSerializeSTriggerPullRequest(NULL, 0, in);
  ASSERT_GT(tlen, 0);
  std::vector<char> buf(tlen);
  ASSERT_EQ(tSerializeSTriggerPullRequest(buf.data(), tlen, in), tlen);
  ASSERT_EQ(tDeserializeSTriggerPullRequest(buf.data(), tlen, out), 0);
}
}  // namespace

TEST_F(StreamMsgWireRoundTrip, NonVTableFirstAndCalcCarryGidSkeyEkeyOrder) {
  for (auto type : {STRIGGER_PULL_TSDB_DATA_NEW, STRIGGER_PULL_TSDB_DATA_NEW_CALC}) {
    SSTriggerTsdbDataNewRequest in = {};
    in.base.type = type;
    in.base.streamId = 0xaaaaaaaaLL;
    in.base.readerTaskId = 0xbbbbbbbbLL;
    in.base.sessionId = 0xccccccccLL;
    in.gid = 0x1111;
    in.skey = 0x2222;
    in.ekey = 0x3333;
    in.order = 1;

    SSTriggerPullRequestUnion out = {};
    encodeAndDecode((SSTriggerPullRequest*)&in, &out);
    EXPECT_EQ(out.base.type, type);
    EXPECT_EQ(out.base.streamId, in.base.streamId);
    EXPECT_EQ(out.base.readerTaskId, in.base.readerTaskId);
    EXPECT_EQ(out.base.sessionId, in.base.sessionId);
    EXPECT_EQ(out.tsdbDataNewReq.gid, in.gid);
    EXPECT_EQ(out.tsdbDataNewReq.skey, in.skey);
    EXPECT_EQ(out.tsdbDataNewReq.ekey, in.ekey);
    EXPECT_EQ(out.tsdbDataNewReq.order, in.order);
    tDestroySTriggerPullRequest(&out);
  }
}

TEST_F(StreamMsgWireRoundTrip, NonVTableNextHasEmptyPayload) {
  for (auto type :
       {STRIGGER_PULL_TSDB_DATA_NEW_NEXT, STRIGGER_PULL_TSDB_DATA_NEW_CALC_NEXT}) {
    SSTriggerPullRequest in = {};
    in.type = type;
    in.streamId = 1;
    in.readerTaskId = 2;
    in.sessionId = 3;

    SSTriggerPullRequestUnion out = {};
    encodeAndDecode(&in, &out);
    EXPECT_EQ(out.base.type, type);
    EXPECT_EQ(out.base.sessionId, 3);
    tDestroySTriggerPullRequest(&out);
  }
}

TEST_F(StreamMsgWireRoundTrip, VTableFirstAndCalcCarryFullPayload) {
  for (auto type : {STRIGGER_PULL_TSDB_DATA_VTABLE_NEW,
                    STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_CALC}) {
    SSTriggerTsdbDataVTableNewRequest in = {};
    in.base.type = type;
    in.base.streamId = 11;
    in.base.readerTaskId = 22;
    in.base.sessionId = 33;
    in.suid = 0x4444;
    in.uid = 0x5555;
    in.skey = 0x6666;
    in.ekey = 0x7777;
    in.order = 2;

    SSTriggerPullRequestUnion out = {};
    encodeAndDecode((SSTriggerPullRequest*)&in, &out);
    EXPECT_EQ(out.base.type, type);
    EXPECT_EQ(out.tsdbDataVTableNewReq.suid, in.suid);
    EXPECT_EQ(out.tsdbDataVTableNewReq.uid, in.uid);
    EXPECT_EQ(out.tsdbDataVTableNewReq.skey, in.skey);
    EXPECT_EQ(out.tsdbDataVTableNewReq.ekey, in.ekey);
    EXPECT_EQ(out.tsdbDataVTableNewReq.order, in.order);
    tDestroySTriggerPullRequest(&out);
  }
}

TEST_F(StreamMsgWireRoundTrip, VTableNextOnlyCarriesUid) {
  for (auto type : {STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_NEXT,
                    STRIGGER_PULL_TSDB_DATA_VTABLE_NEW_CALC_NEXT}) {
    SSTriggerTsdbDataVTableNewRequest in = {};
    in.base.type = type;
    in.base.streamId = 1;
    in.base.readerTaskId = 2;
    in.base.sessionId = 3;
    in.uid = 0x9999;
    // Other payload fields are not on the wire for NEXT requests.

    SSTriggerPullRequestUnion out = {};
    encodeAndDecode((SSTriggerPullRequest*)&in, &out);
    EXPECT_EQ(out.base.type, type);
    EXPECT_EQ(out.tsdbDataVTableNewReq.uid, in.uid);
    tDestroySTriggerPullRequest(&out);
  }
}

TEST_F(StreamMsgWireRoundTrip, SetTableHistorySharesEncodingWithSetTable) {
  // C1 fall-through: SET_TABLE_HISTORY must travel through the same encode/
  // decode case as SET_TABLE; tDestroy must release uidInfoTrigger/uidInfoCalc
  // for both types. We construct two empty hash maps so the codec walks the
  // SET_TABLE branch end-to-end without triggering meta lookups.
  for (auto type : {STRIGGER_PULL_SET_TABLE, STRIGGER_PULL_SET_TABLE_HISTORY}) {
    SSTriggerSetTableRequest in = {};
    in.base.type = type;
    in.base.streamId = 7;
    in.base.readerTaskId = 8;
    in.base.sessionId = 9;
    in.uidInfoTrigger = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    in.uidInfoCalc = tSimpleHashInit(8, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    ASSERT_NE(in.uidInfoTrigger, nullptr);
    ASSERT_NE(in.uidInfoCalc, nullptr);

    SSTriggerPullRequestUnion out = {};
    encodeAndDecode((SSTriggerPullRequest*)&in, &out);
    EXPECT_EQ(out.base.type, type);
    EXPECT_EQ(out.base.sessionId, 9);
    // Both ends own their hashes after decode; tDestroy must clean them up
    // without leaking, and likewise for the source side.
    tDestroySTriggerPullRequest(&out);
    tDestroySTriggerPullRequest((SSTriggerPullRequestUnion*)&in);
  }
}

// ============================================================================
// DS §12.3 C7: SCMCreateStreamReq.isOldPlan must NOT enter the JSON wire.
// The flag is only set on the receiving (mnode) side based on the loaded
// sver; if it ever leaks into the JSON serializer, the rolling-upgrade path
// silently breaks because old replicas would suddenly observe the field.
//
// Constructing a valid SCMCreateStreamReq end-to-end requires populating
// dozens of nested arrays and a valid triggerType. We therefore reverse-
// validate the invariant via source-level inspection: the codec source file
// must not contain the canonical field name on any tjsonAdd*-prefixed line.
// ============================================================================
TEST(StreamCreateReqWire, IsOldPlanIsNotInJsonCodec) {
  const char* candidates[] = {
      "community/source/common/src/msg/streamJson.c",
      "../community/source/common/src/msg/streamJson.c",
      "../../community/source/common/src/msg/streamJson.c",
      "../../../community/source/common/src/msg/streamJson.c",
      "../../../../community/source/common/src/msg/streamJson.c",
      "../../../../../community/source/common/src/msg/streamJson.c",
  };
  TdFilePtr fp = NULL;
  for (auto path : candidates) {
    fp = taosOpenFile(path, TD_FILE_READ);
    if (fp != NULL) break;
  }
  ASSERT_NE(fp, nullptr) << "streamJson.c not reachable from test cwd";

  std::string content;
  char        buf[4096];
  int64_t     n;
  while ((n = taosReadFile(fp, buf, sizeof(buf))) > 0) {
    content.append(buf, buf + n);
  }
  taosCloseFile(&fp);

  // The codec source must not reference isOldPlan at all - neither as a
  // tjsonAdd* serializer call nor as a json key constant. A bare textual
  // scan is sufficient: the field name is unique to SCMCreateStreamReq.
  EXPECT_EQ(content.find("isOldPlan"), std::string::npos)
      << "isOldPlan leaked into the JSON codec; rolling upgrade will break";
}

// ============================================================================
// DS §12.3 C8: F13 threshold constants must remain on their respective code
// paths and must keep their committed values. A drift in either constant is a
// silent perf / cache regression.
// ============================================================================
TEST(StreamThresholds, ConstantsHoldCommittedValues) {
  EXPECT_EQ(STREAM_RETURN_ROWS_NUM, 4096);
  EXPECT_EQ(STREAM_RETURN_ROWS_TSDB_NUM, 50000);
  EXPECT_GT(STREAM_RETURN_ROWS_TSDB_NUM, STREAM_RETURN_ROWS_NUM)
      << "TSDB threshold must dominate WAL threshold; otherwise the F13 "
         "history-fast-path no longer pays off";
}

// ============================================================================
// DS §12.3 C5/C6: destroySlotInfo and compareBlockInfo are file-static helpers
// in vnodeStream.c, so they cannot be linked from this test target. We
// reverse-validate the invariants by source-level inspection: the function
// bodies must contain the exact disposal calls (C5) and the strict uid-
// ascending comparison (C6). A drift in either body is caught at test time.
// ============================================================================
static std::string ReadVnodeStreamSource() {
  const char* candidates[] = {
      "community/source/dnode/vnode/src/vnd/vnodeStream.c",
      "../community/source/dnode/vnode/src/vnd/vnodeStream.c",
      "../../community/source/dnode/vnode/src/vnd/vnodeStream.c",
      "../../../community/source/dnode/vnode/src/vnd/vnodeStream.c",
      "../../../../community/source/dnode/vnode/src/vnd/vnodeStream.c",
      "../../../../../community/source/dnode/vnode/src/vnd/vnodeStream.c",
  };
  TdFilePtr fp = NULL;
  for (auto path : candidates) {
    fp = taosOpenFile(path, TD_FILE_READ);
    if (fp != NULL) break;
  }
  if (fp == NULL) return std::string();
  std::string content;
  char        buf[4096];
  int64_t     n;
  while ((n = taosReadFile(fp, buf, sizeof(buf))) > 0) {
    content.append(buf, buf + n);
  }
  taosCloseFile(&fp);
  return content;
}

static std::string ExtractFunctionBody(const std::string& src, const std::string& signature) {
  size_t start = src.find(signature);
  if (start == std::string::npos) return std::string();
  size_t brace = src.find('{', start);
  if (brace == std::string::npos) return std::string();
  int depth = 0;
  for (size_t i = brace; i < src.size(); ++i) {
    if (src[i] == '{') ++depth;
    else if (src[i] == '}') {
      if (--depth == 0) return src.substr(brace, i - brace + 1);
    }
  }
  return std::string();
}

TEST(StreamVnodeHelpers, DestroySlotInfoFreesMembersOnly) {
  std::string src = ReadVnodeStreamSource();
  ASSERT_FALSE(src.empty()) << "vnodeStream.c not reachable from test cwd";

  std::string body = ExtractFunctionBody(src, "destroySlotInfo(void* p)");
  ASSERT_FALSE(body.empty()) << "destroySlotInfo signature not found";

  // Must free the two members ...
  EXPECT_NE(body.find("taosArrayDestroy(info->schemas)"), std::string::npos)
      << "destroySlotInfo no longer frees info->schemas";
  EXPECT_NE(body.find("taosMemoryFree(info->slotIdList)"), std::string::npos)
      << "destroySlotInfo no longer frees info->slotIdList";

  // ... and must NOT free the SlotInfo container itself: doing so would
  // double-free the stack-allocated SlotInfo passed in at line ~2942 of
  // vnodeStream.c (destroySlotInfo(&(SlotInfo){slotSchema, slotIdList})).
  EXPECT_EQ(body.find("taosMemoryFree(info)"), std::string::npos)
      << "destroySlotInfo must not free the container; callers pass stack objects";
  EXPECT_EQ(body.find("taosMemoryFree(p)"), std::string::npos)
      << "destroySlotInfo must not free the container; callers pass stack objects";
}

TEST(StreamVnodeHelpers, CompareBlockInfoSortsByUidAscending) {
  std::string src = ReadVnodeStreamSource();
  ASSERT_FALSE(src.empty()) << "vnodeStream.c not reachable from test cwd";

  std::string body = ExtractFunctionBody(src, "compareBlockInfo(const void *p1, const void *p2)");
  ASSERT_FALSE(body.empty()) << "compareBlockInfo signature not found";

  // Must compare on info.id.uid (drift to any other field would silently
  // break the SET_TABLE_HISTORY block ordering contract).
  EXPECT_NE(body.find("info.id.uid"), std::string::npos)
      << "compareBlockInfo no longer compares info.id.uid";

  // Equal-uid branch must return 0 (stable sort contract).
  EXPECT_NE(body.find("return 0"), std::string::npos)
      << "compareBlockInfo equal-uid branch must return 0";

  // Strict-ascending branch must use '>' not '<' (a flip would invert the
  // order and corrupt every history-replay window seam).
  EXPECT_NE(body.find("v1->info.id.uid > v2->info.id.uid"), std::string::npos)
      << "compareBlockInfo must use ascending '>' comparison";
}
