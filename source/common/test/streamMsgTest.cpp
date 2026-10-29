/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include <memory>
#include <vector>

#include "streamMsg.h"
#include "tjson.h"

extern "C" {
int32_t tEncodeSStreamTriggerDeployMsg(SEncoder* pEncoder, const SStreamTriggerDeployMsg* pMsg);
int32_t tDecodeSStreamTriggerDeployMsg(SDecoder* pDecoder, SStreamTriggerDeployMsg* pMsg);
void    tFreeSStreamTriggerDeployMsg(SStreamTriggerDeployMsg* pTrigger);
}

namespace {

struct CreateStreamReqCleanup {
  void operator()(SCMCreateStreamReq* pReq) const { tFreeSCMCreateStreamReq(pReq); }
};

struct TriggerDeployMsgCleanup {
  void operator()(SStreamTriggerDeployMsg* pMsg) const { tFreeSStreamTriggerDeployMsg(pMsg); }
};

struct TaosMemoryCleanup {
  void operator()(char* p) const { taosMemoryFree(p); }
};

}  // namespace

TEST(StreamMsgTest, CreateRequestTimeConfigJsonRoundTrip) {
  SCMCreateStreamReq src = {0};
  src.triggerType = WINDOW_TYPE_INTERVAL;
  tstrncpy(src.timeConfig.timezone, "Asia/Shanghai", sizeof(src.timeConfig.timezone));
  src.timeConfig.firstDayOfWeek = 1;
  char*   json = nullptr;
  int32_t len = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scmCreateStreamReqToJson(&src, false, &json, &len));
  std::unique_ptr<char, TaosMemoryCleanup>       jsonGuard(json);
  std::unique_ptr<SJson, decltype(&tjsonDelete)> rootGuard(tjsonParse(json), tjsonDelete);
  SJson*                                         root = rootGuard.get();
  ASSERT_NE(nullptr, root);
  SCMCreateStreamReq dst = {0};
  std::unique_ptr<SCMCreateStreamReq, CreateStreamReqCleanup> dstGuard(&dst);
  ASSERT_EQ(TSDB_CODE_SUCCESS, jsonToSCMCreateStreamReq(root, &dst));
  EXPECT_STREQ("Asia/Shanghai", dst.timeConfig.timezone);
  EXPECT_EQ(1, dst.timeConfig.firstDayOfWeek);
}

TEST(StreamMsgTest, OldCreateRequestJsonKeepsEmptyTimeConfig) {
  SCMCreateStreamReq src = {0};
  src.triggerType = WINDOW_TYPE_INTERVAL;
  char*   json = nullptr;
  int32_t len = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scmCreateStreamReqToJson(&src, false, &json, &len));
  std::unique_ptr<char, TaosMemoryCleanup>       jsonGuard(json);
  std::unique_ptr<SJson, decltype(&tjsonDelete)> rootGuard(tjsonParse(json), tjsonDelete);
  SJson*                                         root = rootGuard.get();
  ASSERT_NE(nullptr, root);
  tjsonDeleteItemFromObject(root, "TimeConfig");
  SCMCreateStreamReq dst = {0};
  std::unique_ptr<SCMCreateStreamReq, CreateStreamReqCleanup> dstGuard(&dst);
  ASSERT_EQ(TSDB_CODE_SUCCESS, jsonToSCMCreateStreamReq(root, &dst));
  EXPECT_EQ('\0', dst.timeConfig.timezone[0]);
  EXPECT_EQ(0, dst.timeConfig.firstDayOfWeek);
}

TEST(StreamMsgTest, CreateRequestTimeConfigRpcRoundTrip) {
  SCMCreateStreamReq src = {0};
  src.triggerType = WINDOW_TYPE_INTERVAL;
  tstrncpy(src.timeConfig.timezone, "America/New_York", sizeof(src.timeConfig.timezone));
  src.timeConfig.firstDayOfWeek = 0;
  int32_t encoded = tSerializeSCMCreateStreamReq(nullptr, 0, &src);
  ASSERT_GT(encoded, 0);
  std::vector<uint8_t> bytes(encoded, 0);
  ASSERT_EQ(encoded, tSerializeSCMCreateStreamReq(bytes.data(), encoded, &src));
  SCMCreateStreamReq dst = {0};
  std::unique_ptr<SCMCreateStreamReq, CreateStreamReqCleanup> dstGuard(&dst);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDeserializeSCMCreateStreamReq(bytes.data(), encoded, &dst));
  EXPECT_STREQ("America/New_York", dst.timeConfig.timezone);
  EXPECT_EQ(0, dst.timeConfig.firstDayOfWeek);
}

TEST(StreamMsgTest, TriggerDeployTimeConfigBinaryRoundTrip) {
  SStreamTriggerDeployMsg src = {0};
  std::unique_ptr<SStreamTriggerDeployMsg, TriggerDeployMsgCleanup> srcGuard(&src);
  src.triggerType = WINDOW_TYPE_INTERVAL;
  src.streamName = taosStrdup("s");
  ASSERT_NE(nullptr, src.streamName);
  src.addOptions = STREAM_OPTION_FLUSH_ON_OUTER_CLOSE;
  tstrncpy(src.timeConfig.timezone, "UTC+8", sizeof(src.timeConfig.timezone));
  src.timeConfig.firstDayOfWeek = 6;

  SEncoder sizeEncoder = {};
  tEncoderInit(&sizeEncoder, nullptr, 0);
  ASSERT_EQ(TSDB_CODE_SUCCESS, tEncodeSStreamTriggerDeployMsg(&sizeEncoder, &src));
  int32_t encoded = sizeEncoder.pos;
  ASSERT_GT(encoded, 0);
  tEncoderClear(&sizeEncoder);

  std::vector<uint8_t> bytes(encoded, 0);
  SEncoder             encoder = {};
  tEncoderInit(&encoder, bytes.data(), static_cast<int32_t>(bytes.size()));
  ASSERT_EQ(TSDB_CODE_SUCCESS, tEncodeSStreamTriggerDeployMsg(&encoder, &src));
  ASSERT_EQ(encoded, encoder.pos);
  tEncoderClear(&encoder);

  SStreamTriggerDeployMsg dst = {0};
  std::unique_ptr<SStreamTriggerDeployMsg, TriggerDeployMsgCleanup> dstGuard(&dst);
  SDecoder                decoder = {};
  tDecoderInit(&decoder, bytes.data(), static_cast<uint32_t>(bytes.size()));
  ASSERT_EQ(TSDB_CODE_SUCCESS, tDecodeSStreamTriggerDeployMsg(&decoder, &dst));
  tDecoderClear(&decoder);
  EXPECT_STREQ("UTC+8", dst.timeConfig.timezone);
  EXPECT_EQ(6, dst.timeConfig.firstDayOfWeek);
  EXPECT_EQ(src.addOptions, dst.addOptions);
}

// The resolve request carries the caller's metadata txn snapshot across the
// cross-vnode RPC. Losing it on the wire silently downgrades chain resolution
// to the committed snapshot, which is exactly the regression this frame must
// not allow: assert the field survives a full serialize/deserialize cycle.
TEST(StreamMsgTest, VTableRefResolveReqCarriesTxnId) {
  SVTableRefResolveReq req = {0};
  req.ver   = 7;
  req.txnId = 0x0123456789ABCDEFLL;
  req.groups = taosArrayInit(1, sizeof(SVTableRefResolveGroupItem));
  ASSERT_NE(nullptr, req.groups);

  SVTableRefResolveGroupItem g = {0};
  tstrncpy(g.dbName, "db1", TSDB_DB_NAME_LEN);
  tstrncpy(g.tableName, "vt1", TSDB_TABLE_NAME_LEN);
  g.cols = taosArrayInit(2, sizeof(SVTableRefResolveColSpec));
  ASSERT_NE(nullptr, g.cols);

  SVTableRefResolveColSpec c0 = {0};
  tstrncpy(c0.colName, "c1", TSDB_COL_NAME_LEN);
  c0.kind = STREAM_VREF_KIND_COL;
  ASSERT_NE(nullptr, taosArrayPush(g.cols, &c0));

  SVTableRefResolveColSpec c1 = {0};
  tstrncpy(c1.colName, "t1", TSDB_COL_NAME_LEN);
  c1.kind = STREAM_VREF_KIND_TAG;
  ASSERT_NE(nullptr, taosArrayPush(g.cols, &c1));

  ASSERT_NE(nullptr, taosArrayPush(req.groups, &g));

  int32_t encoded = tSerializeSVTableRefResolveReq(nullptr, 0, &req);
  ASSERT_GT(encoded, 0);

  std::vector<char> bytes(encoded, 0);
  ASSERT_EQ(encoded, tSerializeSVTableRefResolveReq(bytes.data(), encoded, &req));

  SVTableRefResolveReq dst = {0};
  ASSERT_EQ(0, tDeserializeSVTableRefResolveReq(bytes.data(), encoded, &dst));

  EXPECT_EQ(req.ver, dst.ver);
  EXPECT_EQ(req.txnId, dst.txnId);
  ASSERT_NE(nullptr, dst.groups);
  ASSERT_EQ(1, taosArrayGetSize(dst.groups));
  SVTableRefResolveGroupItem *gp = static_cast<SVTableRefResolveGroupItem *>(taosArrayGet(dst.groups, 0));
  ASSERT_NE(nullptr, gp);
  EXPECT_STREQ("db1", gp->dbName);
  EXPECT_STREQ("vt1", gp->tableName);
  ASSERT_NE(nullptr, gp->cols);
  ASSERT_EQ(2, taosArrayGetSize(gp->cols));
  SVTableRefResolveColSpec *cp0 = static_cast<SVTableRefResolveColSpec *>(taosArrayGet(gp->cols, 0));
  ASSERT_NE(nullptr, cp0);
  EXPECT_STREQ("c1", cp0->colName);
  EXPECT_EQ(STREAM_VREF_KIND_COL, cp0->kind);
  SVTableRefResolveColSpec *cp1 = static_cast<SVTableRefResolveColSpec *>(taosArrayGet(gp->cols, 1));
  ASSERT_NE(nullptr, cp1);
  EXPECT_STREQ("t1", cp1->colName);
  EXPECT_EQ(STREAM_VREF_KIND_TAG, cp1->kind);

  tFreeSVTableRefResolveReq(&dst);
  tFreeSVTableRefResolveReq(&req);
}

// A peer that predates txn propagation emits the same frame without the trailing
// txnId. Such a frame must still decode (falling back to the committed snapshot)
// instead of failing the RPC, otherwise a mixed-version cluster loses ref resolve.
TEST(StreamMsgTest, VTableRefResolveReqDecodesLegacyFrameWithoutTxnId) {
  const int64_t ver     = 9;
  const int32_t nGroups = 0;

  SEncoder sizeEncoder = {};
  tEncoderInit(&sizeEncoder, nullptr, 0);
  ASSERT_EQ(0, tStartEncode(&sizeEncoder));
  ASSERT_EQ(0, tEncodeI64(&sizeEncoder, ver));
  ASSERT_EQ(0, tEncodeI32(&sizeEncoder, nGroups));
  tEndEncode(&sizeEncoder);
  int32_t encoded = sizeEncoder.pos;
  ASSERT_GT(encoded, 0);
  tEncoderClear(&sizeEncoder);

  std::vector<uint8_t> bytes(encoded, 0);
  SEncoder             encoder = {};
  tEncoderInit(&encoder, bytes.data(), encoded);
  ASSERT_EQ(0, tStartEncode(&encoder));
  ASSERT_EQ(0, tEncodeI64(&encoder, ver));
  ASSERT_EQ(0, tEncodeI32(&encoder, nGroups));
  tEndEncode(&encoder);
  ASSERT_EQ(encoded, encoder.pos);
  tEncoderClear(&encoder);

  SVTableRefResolveReq dst = {0};
  ASSERT_EQ(0, tDeserializeSVTableRefResolveReq(bytes.data(), encoded, &dst));
  EXPECT_EQ(ver, dst.ver);
  EXPECT_EQ(0, dst.txnId);
  EXPECT_EQ(nullptr, dst.groups);
  tFreeSVTableRefResolveReq(&dst);
}
