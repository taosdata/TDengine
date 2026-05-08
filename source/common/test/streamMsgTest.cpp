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
#include <cstring>

extern "C" {
#include "streamMsg.h"
#include "taoserror.h"
#include "tarray.h"
#include "tutil.h"
}

// Helper: serialize an old-format buffer (suid/uid/cid only, no resolved field)
// This simulates what an old VNode would produce before the vtable-ref-vtable feature.
static int32_t serializeOldFormatOTableInfoRsp(void* buf, int32_t bufLen, const SSTriggerOrigTableInfoRsp* pRsp) {
  SEncoder encoder = {0};
  int32_t  tlen = 0;
  int32_t  size = 0;

  tEncoderInit(&encoder, (uint8_t*)buf, (uint32_t)bufLen);
  if (tStartEncode(&encoder) != 0) goto _exit;

  size = taosArrayGetSize(pRsp->cols);
  if (tEncodeI32(&encoder, size) != 0) goto _exit;
  for (int32_t i = 0; i < size; ++i) {
    OTableInfoRsp* oInfo = (OTableInfoRsp*)taosArrayGet(pRsp->cols, i);
    if (tEncodeI64(&encoder, oInfo->suid) != 0) goto _exit;
    if (tEncodeI64(&encoder, oInfo->uid) != 0) goto _exit;
    if (tEncodeI16(&encoder, oInfo->cid) != 0) goto _exit;
    // NO resolved field, NO nextRef fields — this is the old format
  }

  tEndEncode(&encoder);

_exit:
  tlen = encoder.pos;
  tEncoderClear(&encoder);
  return tlen;
}

class OTableInfoRspTest : public ::testing::Test {
 protected:
  void SetUp() override { memset(buf_, 0, sizeof(buf_)); }
  void TearDown() override {}

  // Serialize → Deserialize round-trip
  void roundTrip(const SSTriggerOrigTableInfoRsp& src, SSTriggerOrigTableInfoRsp& dst) {
    int32_t tlen = tSerializeSTriggerOrigTableInfoRsp(NULL, 0, &src);
    ASSERT_GT(tlen, 0);
    ASSERT_LE(tlen, (int32_t)sizeof(buf_));

    tlen = tSerializeSTriggerOrigTableInfoRsp(buf_, sizeof(buf_), &src);
    ASSERT_GT(tlen, 0);

    int32_t code = tDserializeSTriggerOrigTableInfoRsp(buf_, tlen, &dst);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  // Old-format serialize → new deserialize
  void oldFormatRoundTrip(const SSTriggerOrigTableInfoRsp& src, SSTriggerOrigTableInfoRsp& dst) {
    int32_t tlen = serializeOldFormatOTableInfoRsp(NULL, 0, &src);
    ASSERT_GT(tlen, 0);
    ASSERT_LE(tlen, (int32_t)sizeof(buf_));

    tlen = serializeOldFormatOTableInfoRsp(buf_, sizeof(buf_), &src);
    ASSERT_GT(tlen, 0);

    int32_t code = tDserializeSTriggerOrigTableInfoRsp(buf_, tlen, &dst);
    ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  }

  char buf_[4096];
};

// Test 1: Single resolved entry round-trip
TEST_F(OTableInfoRspTest, SingleResolvedEntry) {
  SSTriggerOrigTableInfoRsp src = {0};
  src.cols = taosArrayInit(1, sizeof(OTableInfoRsp));
  ASSERT_NE(src.cols, nullptr);

  OTableInfoRsp entry = {0};
  entry.suid = 100;
  entry.uid = 200;
  entry.cid = 5;
  entry.resolved = 1;
  taosArrayPush(src.cols, &entry);

  SSTriggerOrigTableInfoRsp dst = {0};
  roundTrip(src, dst);

  ASSERT_EQ((int32_t)taosArrayGetSize(dst.cols), 1);
  OTableInfoRsp* out = (OTableInfoRsp*)taosArrayGet(dst.cols, 0);
  EXPECT_EQ(out->suid, 100);
  EXPECT_EQ(out->uid, 200);
  EXPECT_EQ(out->cid, 5);
  EXPECT_EQ(out->resolved, 1);

  tDestroySTriggerOrigTableInfoRsp(&src);
  tDestroySTriggerOrigTableInfoRsp(&dst);
}

// Test 2: Single forwarded (unresolved) entry round-trip
TEST_F(OTableInfoRspTest, SingleForwardedEntry) {
  SSTriggerOrigTableInfoRsp src = {0};
  src.cols = taosArrayInit(1, sizeof(OTableInfoRsp));
  ASSERT_NE(src.cols, nullptr);

  OTableInfoRsp entry = {0};
  entry.suid = 0;
  entry.uid = 0;
  entry.cid = 0;
  entry.resolved = 0;
  tstrncpy(entry.nextRefDbName, "db2", TSDB_DB_NAME_LEN);
  tstrncpy(entry.nextRefTableName, "vtable1", TSDB_TABLE_NAME_LEN);
  tstrncpy(entry.nextRefColName, "col_a", TSDB_COL_NAME_LEN);
  taosArrayPush(src.cols, &entry);

  SSTriggerOrigTableInfoRsp dst = {0};
  roundTrip(src, dst);

  ASSERT_EQ((int32_t)taosArrayGetSize(dst.cols), 1);
  OTableInfoRsp* out = (OTableInfoRsp*)taosArrayGet(dst.cols, 0);
  EXPECT_EQ(out->resolved, 0);
  EXPECT_STREQ(out->nextRefDbName, "db2");
  EXPECT_STREQ(out->nextRefTableName, "vtable1");
  EXPECT_STREQ(out->nextRefColName, "col_a");

  tDestroySTriggerOrigTableInfoRsp(&src);
  tDestroySTriggerOrigTableInfoRsp(&dst);
}

// Test 3: Mixed entries (resolved, forwarded, resolved)
TEST_F(OTableInfoRspTest, MixedEntries) {
  SSTriggerOrigTableInfoRsp src = {0};
  src.cols = taosArrayInit(3, sizeof(OTableInfoRsp));
  ASSERT_NE(src.cols, nullptr);

  // Entry 0: resolved
  OTableInfoRsp e0 = {0};
  e0.suid = 1000;
  e0.uid = 2000;
  e0.cid = 10;
  e0.resolved = 1;
  taosArrayPush(src.cols, &e0);

  // Entry 1: forwarded
  OTableInfoRsp e1 = {0};
  e1.resolved = 0;
  tstrncpy(e1.nextRefDbName, "remote_db", TSDB_DB_NAME_LEN);
  tstrncpy(e1.nextRefTableName, "remote_vtable", TSDB_TABLE_NAME_LEN);
  tstrncpy(e1.nextRefColName, "remote_col", TSDB_COL_NAME_LEN);
  taosArrayPush(src.cols, &e1);

  // Entry 2: resolved
  OTableInfoRsp e2 = {0};
  e2.suid = 3000;
  e2.uid = 4000;
  e2.cid = 20;
  e2.resolved = 1;
  taosArrayPush(src.cols, &e2);

  SSTriggerOrigTableInfoRsp dst = {0};
  roundTrip(src, dst);

  ASSERT_EQ((int32_t)taosArrayGetSize(dst.cols), 3);

  OTableInfoRsp* out0 = (OTableInfoRsp*)taosArrayGet(dst.cols, 0);
  EXPECT_EQ(out0->suid, 1000);
  EXPECT_EQ(out0->uid, 2000);
  EXPECT_EQ(out0->cid, 10);
  EXPECT_EQ(out0->resolved, 1);

  OTableInfoRsp* out1 = (OTableInfoRsp*)taosArrayGet(dst.cols, 1);
  EXPECT_EQ(out1->resolved, 0);
  EXPECT_STREQ(out1->nextRefDbName, "remote_db");
  EXPECT_STREQ(out1->nextRefTableName, "remote_vtable");
  EXPECT_STREQ(out1->nextRefColName, "remote_col");

  OTableInfoRsp* out2 = (OTableInfoRsp*)taosArrayGet(dst.cols, 2);
  EXPECT_EQ(out2->suid, 3000);
  EXPECT_EQ(out2->uid, 4000);
  EXPECT_EQ(out2->cid, 20);
  EXPECT_EQ(out2->resolved, 1);

  tDestroySTriggerOrigTableInfoRsp(&src);
  tDestroySTriggerOrigTableInfoRsp(&dst);
}

// Test 4: Backward compat — old format single entry
TEST_F(OTableInfoRspTest, BackwardCompatSingleEntry) {
  SSTriggerOrigTableInfoRsp src = {0};
  src.cols = taosArrayInit(1, sizeof(OTableInfoRsp));
  ASSERT_NE(src.cols, nullptr);

  OTableInfoRsp entry = {0};
  entry.suid = 500;
  entry.uid = 600;
  entry.cid = 7;
  taosArrayPush(src.cols, &entry);

  SSTriggerOrigTableInfoRsp dst = {0};
  oldFormatRoundTrip(src, dst);

  ASSERT_EQ((int32_t)taosArrayGetSize(dst.cols), 1);
  OTableInfoRsp* out = (OTableInfoRsp*)taosArrayGet(dst.cols, 0);
  EXPECT_EQ(out->suid, 500);
  EXPECT_EQ(out->uid, 600);
  EXPECT_EQ(out->cid, 7);
  EXPECT_EQ(out->resolved, 1);  // default when old format

  tDestroySTriggerOrigTableInfoRsp(&src);
  tDestroySTriggerOrigTableInfoRsp(&dst);
}

// Test 5: Backward compat — old format MULTIPLE entries (the critical case!)
// This verifies that old-format multi-entry buffers decode correctly.
TEST_F(OTableInfoRspTest, BackwardCompatMultipleEntries) {
  SSTriggerOrigTableInfoRsp src = {0};
  src.cols = taosArrayInit(3, sizeof(OTableInfoRsp));
  ASSERT_NE(src.cols, nullptr);

  OTableInfoRsp entries[3] = {0};
  entries[0].suid = 100;  entries[0].uid = 200;  entries[0].cid = 1;
  entries[1].suid = 300;  entries[1].uid = 400;  entries[1].cid = 2;
  entries[2].suid = 500;  entries[2].uid = 600;  entries[2].cid = 3;
  for (int i = 0; i < 3; i++) taosArrayPush(src.cols, &entries[i]);

  SSTriggerOrigTableInfoRsp dst = {0};
  oldFormatRoundTrip(src, dst);

  ASSERT_EQ((int32_t)taosArrayGetSize(dst.cols), 3);
  for (int i = 0; i < 3; i++) {
    OTableInfoRsp* out = (OTableInfoRsp*)taosArrayGet(dst.cols, i);
    EXPECT_EQ(out->suid, entries[i].suid) << "entry " << i;
    EXPECT_EQ(out->uid, entries[i].uid) << "entry " << i;
    EXPECT_EQ(out->cid, entries[i].cid) << "entry " << i;
    EXPECT_EQ(out->resolved, 1) << "entry " << i << " should default to resolved=1";
  }

  tDestroySTriggerOrigTableInfoRsp(&src);
  tDestroySTriggerOrigTableInfoRsp(&dst);
}

// Test 6: Empty response round-trip
TEST_F(OTableInfoRspTest, EmptyResponse) {
  SSTriggerOrigTableInfoRsp src = {0};
  src.cols = taosArrayInit(0, sizeof(OTableInfoRsp));
  ASSERT_NE(src.cols, nullptr);

  SSTriggerOrigTableInfoRsp dst = {0};
  roundTrip(src, dst);

  ASSERT_EQ((int32_t)taosArrayGetSize(dst.cols), 0);

  tDestroySTriggerOrigTableInfoRsp(&src);
  tDestroySTriggerOrigTableInfoRsp(&dst);
}

// Test 7: Large number of entries to stress the serialization
TEST_F(OTableInfoRspTest, ManyEntries) {
  const int N = 50;
  SSTriggerOrigTableInfoRsp src = {0};
  src.cols = taosArrayInit(N, sizeof(OTableInfoRsp));
  ASSERT_NE(src.cols, nullptr);

  for (int i = 0; i < N; i++) {
    OTableInfoRsp entry = {0};
    entry.suid = i * 100;
    entry.uid = i * 100 + 1;
    entry.cid = (col_id_t)(i + 1);
    entry.resolved = (i % 3 != 0) ? 1 : 0;  // every 3rd is forwarded
    if (!entry.resolved) {
      snprintf(entry.nextRefDbName, TSDB_DB_NAME_LEN, "db_%d", i);
      snprintf(entry.nextRefTableName, TSDB_TABLE_NAME_LEN, "tbl_%d", i);
      snprintf(entry.nextRefColName, TSDB_COL_NAME_LEN, "col_%d", i);
    }
    taosArrayPush(src.cols, &entry);
  }

  // Use heap-allocated buffer for large payload
  int32_t tlen = tSerializeSTriggerOrigTableInfoRsp(NULL, 0, &src);
  ASSERT_GT(tlen, 0);
  char* bigBuf = (char*)taosMemoryCalloc(1, tlen);
  ASSERT_NE(bigBuf, nullptr);

  tlen = tSerializeSTriggerOrigTableInfoRsp(bigBuf, tlen, &src);
  ASSERT_GT(tlen, 0);

  SSTriggerOrigTableInfoRsp dst = {0};
  int32_t code = tDserializeSTriggerOrigTableInfoRsp(bigBuf, tlen, &dst);
  ASSERT_EQ(code, TSDB_CODE_SUCCESS);
  ASSERT_EQ((int32_t)taosArrayGetSize(dst.cols), N);

  for (int i = 0; i < N; i++) {
    OTableInfoRsp* src_e = (OTableInfoRsp*)taosArrayGet(src.cols, i);
    OTableInfoRsp* dst_e = (OTableInfoRsp*)taosArrayGet(dst.cols, i);
    EXPECT_EQ(dst_e->suid, src_e->suid) << "entry " << i;
    EXPECT_EQ(dst_e->uid, src_e->uid) << "entry " << i;
    EXPECT_EQ(dst_e->cid, src_e->cid) << "entry " << i;
    EXPECT_EQ(dst_e->resolved, src_e->resolved) << "entry " << i;
    if (!dst_e->resolved) {
      EXPECT_STREQ(dst_e->nextRefDbName, src_e->nextRefDbName) << "entry " << i;
      EXPECT_STREQ(dst_e->nextRefTableName, src_e->nextRefTableName) << "entry " << i;
      EXPECT_STREQ(dst_e->nextRefColName, src_e->nextRefColName) << "entry " << i;
    }
  }

  taosMemoryFree(bigBuf);
  tDestroySTriggerOrigTableInfoRsp(&src);
  tDestroySTriggerOrigTableInfoRsp(&dst);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
