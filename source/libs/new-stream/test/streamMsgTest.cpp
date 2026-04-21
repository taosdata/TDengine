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

#include <gtest/gtest.h>
#include <vector>

extern "C" {
#include "streamMsg.h"
#include "taoserror.h"
}

// ───────────────────────────────────────────────────────────
// Helper: build the base fields
// ───────────────────────────────────────────────────────────
static void fillBase(SSTriggerPullRequest* base, ESTriggerPullType type) {
  base->type         = type;
  base->streamId     = 100;
  base->readerTaskId = 200;
  base->sessionId    = 300;
}

// ───────────────────────────────────────────────────────────
// TEST 1: SetTable round-trip — base type distinguishes history vs realtime
// ───────────────────────────────────────────────────────────
TEST(StreamMsg, SetTable_TypeRoundTrip) {
  ESTriggerPullType types[] = {STRIGGER_PULL_SET_TABLE, STRIGGER_PULL_SET_TABLE_HISTORY};
  for (ESTriggerPullType t : types) {
    SSTriggerSetTableRequest req = {};
    fillBase(&req.base, t);
    req.uidInfoTrigger = tSimpleHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    req.uidInfoCalc    = tSimpleHashInit(4, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    ASSERT_NE(req.uidInfoTrigger, nullptr);
    ASSERT_NE(req.uidInfoCalc, nullptr);

    // serialize
    int32_t need = tSerializeSTriggerPullRequest(NULL, 0, (SSTriggerPullRequest*)&req);
    ASSERT_GT(need, 0);
    std::vector<char> buf(need);
    int32_t ret = tSerializeSTriggerPullRequest(buf.data(), need, (SSTriggerPullRequest*)&req);
    ASSERT_GT(ret, 0);

    // deserialize
    SSTriggerPullRequestUnion out = {};
    ASSERT_EQ(tDeserializeSTriggerPullRequest(buf.data(), need, &out), 0);
    EXPECT_EQ(out.base.type, t);

    tDestroySTriggerPullRequest((SSTriggerPullRequestUnion*)&req);
    tDestroySTriggerPullRequest(&out);
  }
}

// ───────────────────────────────────────────────────────────
// TEST 2: DiffRange round-trip for 4 variants (ranges contains 3 entries)
// ───────────────────────────────────────────────────────────
TEST(StreamMsg, DiffRange_4Variants_RoundTrip) {
  int32_t variants[] = {
    (int32_t)STRIGGER_PULL_TSDB_DATA_DIFF_RANGE,
    (int32_t)STRIGGER_PULL_TSDB_DATA_DIFF_RANGE_CALC,
    (int32_t)STRIGGER_PULL_TSDB_DATA_DIFF_RANGE_NEXT,
    (int32_t)STRIGGER_PULL_TSDB_DATA_DIFF_RANGE_CALC_NEXT,
  };

  for (int32_t type : variants) {
    SSTriggerTsdbDataDiffRangeRequest req = {};
    fillBase(&req.base, (ESTriggerPullType)type);

    bool hasPayload = (type == STRIGGER_PULL_TSDB_DATA_DIFF_RANGE ||
                       type == STRIGGER_PULL_TSDB_DATA_DIFF_RANGE_CALC);
    if (hasPayload) {
      req.ver   = 42;
      req.order = 1;
      req.ranges = taosArrayInit(3, sizeof(SSTriggerTableTimeRange));
      ASSERT_NE(req.ranges, nullptr);
      for (int i = 0; i < 3; i++) {
        SSTriggerTableTimeRange r = {(int64_t)(i * 10), (int64_t)(i * 10 + 1),
                                     (int64_t)(1000 + i), (int64_t)(2000 + i)};
        taosArrayPush(req.ranges, &r);
      }
    }

    int32_t need = tSerializeSTriggerPullRequest(NULL, 0, (SSTriggerPullRequest*)&req);
    ASSERT_GT(need, 0) << "type=" << type;
    std::vector<char> buf(need);
    ASSERT_GT(tSerializeSTriggerPullRequest(buf.data(), need, (SSTriggerPullRequest*)&req), 0);

    SSTriggerPullRequestUnion out = {};
    ASSERT_EQ(tDeserializeSTriggerPullRequest(buf.data(), need, &out), 0) << "type=" << type;
    EXPECT_EQ(out.base.type, type);

    if (hasPayload) {
      SSTriggerTsdbDataDiffRangeRequest* pOut = &out.tsdbDataDiffRangeReq;
      EXPECT_EQ(pOut->ver, 42);
      EXPECT_EQ(pOut->order, 1);
      ASSERT_NE(pOut->ranges, nullptr);
      ASSERT_EQ(taosArrayGetSize(pOut->ranges), 3);
      for (int i = 0; i < 3; i++) {
        SSTriggerTableTimeRange* r = (SSTriggerTableTimeRange*)taosArrayGet(pOut->ranges, i);
        EXPECT_EQ(r->suid, (int64_t)(i * 10));
        EXPECT_EQ(r->uid,  (int64_t)(i * 10 + 1));
        EXPECT_EQ(r->skey, (int64_t)(1000 + i));
        EXPECT_EQ(r->ekey, (int64_t)(2000 + i));
      }
    }

    tDestroySTriggerPullRequest((SSTriggerPullRequestUnion*)&req);
    tDestroySTriggerPullRequest(&out);
  }
}

// ───────────────────────────────────────────────────────────
// TEST 3: SameRange round-trip for 4 variants
// ───────────────────────────────────────────────────────────
TEST(StreamMsg, SameRange_4Variants_RoundTrip) {
  int32_t variants[] = {
    (int32_t)STRIGGER_PULL_TSDB_DATA_SAME_RANGE,
    (int32_t)STRIGGER_PULL_TSDB_DATA_SAME_RANGE_CALC,
    (int32_t)STRIGGER_PULL_TSDB_DATA_SAME_RANGE_NEXT,
    (int32_t)STRIGGER_PULL_TSDB_DATA_SAME_RANGE_CALC_NEXT,
  };

  for (int32_t type : variants) {
    SSTriggerTsdbDataSameRangeRequest req = {};
    fillBase(&req.base, (ESTriggerPullType)type);

    bool hasPayload = (type == STRIGGER_PULL_TSDB_DATA_SAME_RANGE ||
                       type == STRIGGER_PULL_TSDB_DATA_SAME_RANGE_CALC);
    if (hasPayload) {
      req.ver   = 77;
      req.gid   = 88;
      req.skey  = 1000;
      req.ekey  = 2000;
      req.order = 2;
    }

    int32_t need = tSerializeSTriggerPullRequest(NULL, 0, (SSTriggerPullRequest*)&req);
    ASSERT_GT(need, 0) << "type=" << type;
    std::vector<char> buf(need);
    ASSERT_GT(tSerializeSTriggerPullRequest(buf.data(), need, (SSTriggerPullRequest*)&req), 0);

    SSTriggerPullRequestUnion out = {};
    ASSERT_EQ(tDeserializeSTriggerPullRequest(buf.data(), need, &out), 0) << "type=" << type;
    EXPECT_EQ(out.base.type, type);

    if (hasPayload) {
      SSTriggerTsdbDataSameRangeRequest* pOut = &out.tsdbDataSameRangeReq;
      EXPECT_EQ(pOut->ver,   77);
      EXPECT_EQ(pOut->gid,   88);
      EXPECT_EQ(pOut->skey,  1000);
      EXPECT_EQ(pOut->ekey,  2000);
      EXPECT_EQ(pOut->order, 2);
    }

    tDestroySTriggerPullRequest((SSTriggerPullRequestUnion*)&req);
    tDestroySTriggerPullRequest(&out);
  }
}

// ───────────────────────────────────────────────────────────
// TEST 4: DiffRange with empty ranges (both NULL and empty SArray are accepted)
// ───────────────────────────────────────────────────────────
TEST(StreamMsg, DiffRange_EmptyRanges) {
  // Case A: ranges == NULL
  {
    SSTriggerTsdbDataDiffRangeRequest req = {};
    fillBase(&req.base, STRIGGER_PULL_TSDB_DATA_DIFF_RANGE);
    req.ver = 1; req.order = 1; req.ranges = NULL;

    int32_t need = tSerializeSTriggerPullRequest(NULL, 0, (SSTriggerPullRequest*)&req);
    ASSERT_GT(need, 0);
    std::vector<char> buf(need);
    ASSERT_GT(tSerializeSTriggerPullRequest(buf.data(), need, (SSTriggerPullRequest*)&req), 0);

    SSTriggerPullRequestUnion out = {};
    ASSERT_EQ(tDeserializeSTriggerPullRequest(buf.data(), need, &out), 0);
    EXPECT_EQ(out.tsdbDataDiffRangeReq.ranges, nullptr);
    tDestroySTriggerPullRequest(&out);
  }

  // Case B: ranges is an empty SArray
  {
    SSTriggerTsdbDataDiffRangeRequest req = {};
    fillBase(&req.base, STRIGGER_PULL_TSDB_DATA_DIFF_RANGE);
    req.ver = 2; req.order = 2;
    req.ranges = taosArrayInit(0, sizeof(SSTriggerTableTimeRange));
    ASSERT_NE(req.ranges, nullptr);

    int32_t need = tSerializeSTriggerPullRequest(NULL, 0, (SSTriggerPullRequest*)&req);
    ASSERT_GT(need, 0);
    std::vector<char> buf(need);
    ASSERT_GT(tSerializeSTriggerPullRequest(buf.data(), need, (SSTriggerPullRequest*)&req), 0);

    SSTriggerPullRequestUnion out = {};
    ASSERT_EQ(tDeserializeSTriggerPullRequest(buf.data(), need, &out), 0);
    // When nRanges==0, deserialize leaves it as NULL
    EXPECT_EQ(out.tsdbDataDiffRangeReq.ranges, nullptr);

    tDestroySTriggerPullRequest((SSTriggerPullRequestUnion*)&req);
    tDestroySTriggerPullRequest(&out);
  }
}

// ───────────────────────────────────────────────────────────
// TEST 5: DiffRange with many ranges (1000 entries)
// ───────────────────────────────────────────────────────────
TEST(StreamMsg, DiffRange_LargeRanges) {
  const int N = 1000;
  SSTriggerTsdbDataDiffRangeRequest req = {};
  fillBase(&req.base, STRIGGER_PULL_TSDB_DATA_DIFF_RANGE);
  req.ver = 99; req.order = 1;
  req.ranges = taosArrayInit(N, sizeof(SSTriggerTableTimeRange));
  ASSERT_NE(req.ranges, nullptr);

  for (int i = 0; i < N; i++) {
    SSTriggerTableTimeRange r = {(int64_t)i, (int64_t)(i + 1),
                                 (int64_t)(i * 100), (int64_t)(i * 100 + 50)};
    taosArrayPush(req.ranges, &r);
  }

  int32_t need = tSerializeSTriggerPullRequest(NULL, 0, (SSTriggerPullRequest*)&req);
  ASSERT_GT(need, 0);
  std::vector<char> buf(need);
  ASSERT_GT(tSerializeSTriggerPullRequest(buf.data(), need, (SSTriggerPullRequest*)&req), 0);

  SSTriggerPullRequestUnion out = {};
  ASSERT_EQ(tDeserializeSTriggerPullRequest(buf.data(), need, &out), 0);
  ASSERT_NE(out.tsdbDataDiffRangeReq.ranges, nullptr);
  ASSERT_EQ(taosArrayGetSize(out.tsdbDataDiffRangeReq.ranges), N);

  for (int i = 0; i < N; i++) {
    SSTriggerTableTimeRange* r = (SSTriggerTableTimeRange*)taosArrayGet(out.tsdbDataDiffRangeReq.ranges, i);
    EXPECT_EQ(r->suid, (int64_t)i);
    EXPECT_EQ(r->uid,  (int64_t)(i + 1));
    EXPECT_EQ(r->skey, (int64_t)(i * 100));
    EXPECT_EQ(r->ekey, (int64_t)(i * 100 + 50));
  }

  tDestroySTriggerPullRequest((SSTriggerPullRequestUnion*)&req);
  tDestroySTriggerPullRequest(&out);
}

// ───────────────────────────────────────────────────────────
// TEST 6: DiffRange consecutive destroy is safe (the second call should be a no-op)
// ───────────────────────────────────────────────────────────
TEST(StreamMsg, DiffRange_DoubleDestroySafe) {
  SSTriggerTsdbDataDiffRangeRequest req = {};
  fillBase(&req.base, STRIGGER_PULL_TSDB_DATA_DIFF_RANGE);
  req.ver = 1; req.order = 1;
  req.ranges = taosArrayInit(2, sizeof(SSTriggerTableTimeRange));
  ASSERT_NE(req.ranges, nullptr);
  SSTriggerTableTimeRange r = {1, 2, 100, 200};
  taosArrayPush(req.ranges, &r);

  int32_t need = tSerializeSTriggerPullRequest(NULL, 0, (SSTriggerPullRequest*)&req);
  ASSERT_GT(need, 0);
  std::vector<char> buf(need);
  ASSERT_GT(tSerializeSTriggerPullRequest(buf.data(), need, (SSTriggerPullRequest*)&req), 0);

  SSTriggerPullRequestUnion out = {};
  ASSERT_EQ(tDeserializeSTriggerPullRequest(buf.data(), need, &out), 0);

  // First destroy
  tDestroySTriggerPullRequest(&out);
  // After destroy, ranges has been set to NULL; a second call must not segfault
  tDestroySTriggerPullRequest(&out);

  tDestroySTriggerPullRequest((SSTriggerPullRequestUnion*)&req);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
