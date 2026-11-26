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

#include <algorithm>
#include <memory>
#include <vector>

#include "streamMsg.h"
#include "tdatablock.h"
#include "tjson.h"

extern "C" {
int32_t tEncodeSStmTaskDeploy(SEncoder*, const SStmTaskDeploy*);
int32_t tDecodeSStmTaskDeploy(SDecoder*, SStmTaskDeploy*);
int32_t tDecodeStreamTask(SDecoder*, SStreamTask*);
void    tFreeSStmTaskDeploy(void*);
int32_t tEncodeSStreamReaderDeployMsg(SEncoder*, const SStreamReaderDeployMsg*);
int32_t tDecodeSStreamReaderDeployMsg(SDecoder*, SStreamReaderDeployMsg*);
void    tFreeSStreamReaderDeployMsg(SStreamReaderDeployMsg*);
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

namespace {
class StreamTimeAlignWire : public ::testing::Test {
 protected:
  SCMCreateStreamReq req = {};
  void               SetUp() override {
    req.triggerType = WINDOW_TYPE_INTERVAL;
    req.addOptions = STREAM_OPTION_TIME_ALIGN;
    req.pTimeAlign = static_cast<SStreamTimeAlignSpec*>(taosMemoryCalloc(1, sizeof(SStreamTimeAlignSpec)));
    ASSERT_NE(nullptr, req.pTimeAlign);
    auto* spec = req.pTimeAlign;
    spec->outputRaw = false;
    spec->precision = TSDB_TIME_PRECISION_MICRO;
    spec->pRowsetColIds = taosArrayInit(2, sizeof(col_id_t));
    spec->pColumns = taosArrayInit(2, sizeof(SStreamTimeAlignColumn));
    spec->timezone = taosStrdup("Asia/Shanghai");
    ASSERT_NE(nullptr, spec->pRowsetColIds);
    ASSERT_NE(nullptr, spec->pColumns);
    ASSERT_NE(nullptr, spec->timezone);
    for (col_id_t cid = 2; cid <= 3; ++cid) {
      ASSERT_NE(nullptr, taosArrayPush(spec->pRowsetColIds, &cid));
      SStreamTimeAlignColumn col = {};
      col.srcColId = cid;
      col.type.type = TSDB_DATA_TYPE_INT;
      col.type.bytes = sizeof(int32_t);
      tstrncpy(col.outName, cid == 2 ? "a_prev" : "b_prev", sizeof(col.outName));
      col.hasSpan = cid == 2;
      col.amount = col.hasSpan ? 8 : 0;
      col.unit = col.hasSpan ? 's' : 0;
      ASSERT_NE(nullptr, taosArrayPush(spec->pColumns, &col));
    }
  }
  void checkHeartbeat(int firstMode, int endMode, int64_t expectedAmount = 8, int streamCount = 2);
  void TearDown() override { tFreeSCMCreateStreamReq(&req); }
  void check(const SStreamTimeAlignSpec* spec, int64_t expectedAmount = 8, const char* name = "a_prev",
             const char* timezone = "Asia/Shanghai") {
    ASSERT_NE(nullptr, spec);
    EXPECT_FALSE(spec->outputRaw);
    EXPECT_EQ(TSDB_TIME_PRECISION_MICRO, spec->precision);
    ASSERT_EQ(2, taosArrayGetSize(spec->pRowsetColIds));
    EXPECT_EQ(3, *static_cast<col_id_t*>(taosArrayGet(spec->pRowsetColIds, 1)));
    ASSERT_EQ(2, taosArrayGetSize(spec->pColumns));
    auto* a = static_cast<SStreamTimeAlignColumn*>(taosArrayGet(spec->pColumns, 0));
    EXPECT_EQ(2, a->srcColId);
    EXPECT_EQ(TSDB_DATA_TYPE_INT, a->type.type);
    EXPECT_STREQ(name, a->outName);
    EXPECT_TRUE(a->hasSpan);
    EXPECT_EQ(expectedAmount, a->amount);
    EXPECT_EQ('s', a->unit);
    EXPECT_STREQ("b_prev", static_cast<SStreamTimeAlignColumn*>(taosArrayGet(spec->pColumns, 1))->outName);
    EXPECT_STREQ(timezone, spec->timezone);
  }
};
}  // namespace

TEST_F(StreamTimeAlignWire, TimeAlignCreateJsonRoundTrip) {
  char*   json = nullptr;
  int32_t len = 0;
  ASSERT_EQ(0, scmCreateStreamReqToJson(&req, false, &json, &len));
  std::unique_ptr<char, TaosMemoryCleanup>       jsonGuard(json);
  std::unique_ptr<SJson, decltype(&tjsonDelete)> root(tjsonParse(json), tjsonDelete);
  ASSERT_NE(nullptr, root);
  EXPECT_NE(nullptr, tjsonGetObjectItem(root.get(), "timeAlign"));
  SCMCreateStreamReq                                          dst = {};
  std::unique_ptr<SCMCreateStreamReq, CreateStreamReqCleanup> dstGuard(&dst);
  ASSERT_EQ(0, jsonToSCMCreateStreamReq(root.get(), &dst));
  EXPECT_EQ(WINDOW_TYPE_INTERVAL, dst.triggerType);
  SCMCreateStreamReq* clone = nullptr;
  ASSERT_EQ(0, tCloneStreamCreateDeployPointers(&req, &clone));
  ASSERT_NE(nullptr, clone);
  tDestroyStreamTimeAlignSpec(&req.pTimeAlign);
  root.reset();
  jsonGuard.reset();
  check(dst.pTimeAlign);
  check(clone->pTimeAlign);
  tFreeSCMCreateStreamReq(clone);
  taosMemoryFree(clone);
}

TEST_F(StreamTimeAlignWire, OnlyAlignedCreateRequestsUseTheVersionedFrame) {
  for (bool aligned : {false, true}) {
    SCMCreateStreamReq plain = {};
    plain.triggerType = WINDOW_TYPE_INTERVAL;
    auto* source = aligned ? &req : &plain;
    source->txnId = 1234;
    source->outTblExists = 1;
    int32_t size = tSerializeSCMCreateStreamReq(nullptr, 0, source);
    ASSERT_GT(size, 0);
    std::vector<uint8_t> bytes(size);
    ASSERT_EQ(size, tSerializeSCMCreateStreamReq(bytes.data(), size, source));
    SDecoder decoder;
    tDecoderInit(&decoder, bytes.data(), size);
    EXPECT_EQ(aligned ? TSDB_CODE_OUT_OF_RANGE : 0, tStartDecode(&decoder));
    tDecoderClear(&decoder);
    SCMCreateStreamReq                                          dst = {};
    std::unique_ptr<SCMCreateStreamReq, CreateStreamReqCleanup> guard(&dst);
    ASSERT_EQ(0, tDeserializeSCMCreateStreamReq(bytes.data(), size, &dst));
    EXPECT_EQ(source->txnId, dst.txnId);
    EXPECT_EQ(source->outTblExists, dst.outTblExists);
    EXPECT_EQ(WINDOW_TYPE_INTERVAL, dst.triggerType);
    if (aligned)
      check(dst.pTimeAlign);
    else
      EXPECT_EQ(nullptr, dst.pTimeAlign);
  }
}

TEST_F(StreamTimeAlignWire, WideCreateJsonPreservesOrderAndRejectsDuplicate) {
  constexpr int width = 4096;
  auto*         spec = req.pTimeAlign;
  auto          rule = *static_cast<SStreamTimeAlignColumn*>(taosArrayGet(spec->pColumns, 0));
  taosArrayClear(spec->pRowsetColIds);
  taosArrayClear(spec->pColumns);
  for (int i = 0; i < width; ++i) {
    col_id_t cid = width + 1 - i;
    ASSERT_NE(nullptr, taosArrayPush(spec->pRowsetColIds, &cid));
    rule.srcColId = i + 2;
    tsnprintf(rule.outName, sizeof(rule.outName), "prev_%d", i);
    ASSERT_NE(nullptr, taosArrayPush(spec->pColumns, &rule));
  }
  char*   json = nullptr;
  int32_t len = 0;
  ASSERT_EQ(0, scmCreateStreamReqToJson(&req, false, &json, &len));
  std::unique_ptr<char, TaosMemoryCleanup>       jsonGuard(json);
  std::unique_ptr<SJson, decltype(&tjsonDelete)> root(tjsonParse(json), tjsonDelete);
  ASSERT_NE(nullptr, root);
  SCMCreateStreamReq                                          dst = {};
  std::unique_ptr<SCMCreateStreamReq, CreateStreamReqCleanup> guard(&dst);
  ASSERT_EQ(0, jsonToSCMCreateStreamReq(root.get(), &dst));
  ASSERT_NE(nullptr, dst.pTimeAlign);
  ASSERT_EQ(width, taosArrayGetSize(dst.pTimeAlign->pRowsetColIds));
  ASSERT_EQ(width, taosArrayGetSize(dst.pTimeAlign->pColumns));
  for (int i = 0; i < width; ++i) {
    EXPECT_EQ(width + 1 - i, *static_cast<col_id_t*>(taosArrayGet(dst.pTimeAlign->pRowsetColIds, i)));
    auto* col = static_cast<SStreamTimeAlignColumn*>(taosArrayGet(dst.pTimeAlign->pColumns, i));
    EXPECT_EQ(i + 2, col->srcColId);
    EXPECT_EQ("prev_" + std::to_string(i), col->outName);
  }
  SJson* cids = tjsonGetObjectItem(tjsonGetObjectItem(root.get(), "timeAlign"), "RowsetColIds");
  SJson* duplicate = tjsonCreateObject();
  ASSERT_NE(nullptr, duplicate);
  ASSERT_EQ(0, tjsonAddItemToArray(cids, duplicate));
  ASSERT_EQ(0, tjsonAddIntegerToObject(duplicate, "Cid", width + 1));
  SCMCreateStreamReq                                          invalid = {};
  std::unique_ptr<SCMCreateStreamReq, CreateStreamReqCleanup> invalidGuard(&invalid);
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, jsonToSCMCreateStreamReq(root.get(), &invalid));
  EXPECT_EQ(nullptr, invalid.pTimeAlign);
}

TEST_F(StreamTimeAlignWire, JsonRequiresMarkerAndKnownVersion) {
  char*   json = nullptr;
  int32_t len = 0;
  ASSERT_EQ(0, scmCreateStreamReqToJson(&req, false, &json, &len));
  std::unique_ptr<char, TaosMemoryCleanup> jsonGuard(json);
  for (int mode = 0; mode < 6; ++mode) {
    std::unique_ptr<SJson, decltype(&tjsonDelete)> root(tjsonParse(json), tjsonDelete);
    ASSERT_NE(nullptr, root);
    if (mode == 0) tjsonDeleteItemFromObject(root.get(), "timeAlign");
    if (mode == 1 || mode == 2) {
      SJson* spec = tjsonGetObjectItem(root.get(), "timeAlign");
      tjsonDeleteItemFromObject(spec, "Version");
      if (mode == 1) ASSERT_EQ(0, tjsonAddIntegerToObject(spec, "Version", 99));
    }
    if (mode == 3) tjsonDeleteItemFromObject(tjsonGetObjectItem(root.get(), "timeAlign"), "Timezone");
    if (mode == 4) {
      SJson* col = tjsonGetArrayItem(tjsonGetObjectItem(tjsonGetObjectItem(root.get(), "timeAlign"), "Columns"), 0);
      tjsonDeleteItemFromObject(col, "SrcColId");
      ASSERT_EQ(0, tjsonAddIntegerToObject(col, "SrcColId", 65537));
    }
    if (mode == 5) {
      SJson* spec = tjsonGetObjectItem(root.get(), "timeAlign");
      tjsonDeleteItemFromObject(spec, "Version");
      ASSERT_EQ(0, tjsonAddIntegerToObject(spec, "Version", INT64_C(4294967297)));
    }
    SCMCreateStreamReq                                          dst = {};
    std::unique_ptr<SCMCreateStreamReq, CreateStreamReqCleanup> dstGuard(&dst);
    EXPECT_NE(0, jsonToSCMCreateStreamReq(root.get(), &dst)) << mode;
  }
}

TEST_F(StreamTimeAlignWire, InvalidCreateJsonPreservesRpcConversionError) {
  char*   json = nullptr;
  int32_t length = 0;
  ASSERT_EQ(0, scmCreateStreamReqToJson(&req, false, &json, &length));
  std::unique_ptr<char, TaosMemoryCleanup> jsonGuard(json);
  std::string                              malformed(json);
  auto                                     pos = malformed.find("\"OutputRaw\":false");
  ASSERT_NE(std::string::npos, pos);
  malformed.replace(pos, strlen("\"OutputRaw\":false"), "\"OutputRaw\":123");
  std::vector<uint8_t> bytes(malformed.size() + 64);
  SEncoder             encoder = {};
  tEncoderInit(&encoder, bytes.data(), bytes.size());
  ASSERT_EQ(0, tStartEncode(&encoder));
  ASSERT_EQ(0, tEncodeCStr(&encoder, malformed.c_str()));
  ASSERT_EQ(0, tEncodeI64(&encoder, 123));
  tEndEncode(&encoder);
  int32_t size = encoder.pos;
  tEncoderClear(&encoder);
  SCMCreateStreamReq                                          dst = {};
  std::unique_ptr<SCMCreateStreamReq, CreateStreamReqCleanup> guard(&dst);
  EXPECT_NE(0, tDeserializeSCMCreateStreamReq(bytes.data(), size, &dst));
  EXPECT_EQ(0, dst.txnId);
}

TEST_F(StreamTimeAlignWire, WideRowsetAndMultipleRulesPerSource) {
  auto* spec = req.pTimeAlign;
  auto  rule = *static_cast<SStreamTimeAlignColumn*>(taosArrayGet(spec->pColumns, 0));
  for (col_id_t id = 4; id < 4098; ++id) {
    ASSERT_NE(nullptr, taosArrayPush(spec->pRowsetColIds, &id));
    rule.srcColId = id;
    tsnprintf(rule.outName, sizeof(rule.outName), "previous_%d", static_cast<int>(id));
    ASSERT_NE(nullptr, taosArrayPush(spec->pColumns, &rule));
  }
  ASSERT_EQ(0, tValidateStreamTimeAlignSpec(spec));
  auto* last = static_cast<SStreamTimeAlignColumn*>(taosArrayGetLast(spec->pColumns));
  last->srcColId = 2;
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tValidateStreamTimeAlignSpec(spec));
  spec->outputRaw = true;
  EXPECT_EQ(0, tValidateStreamTimeAlignSpec(spec));
  tstrncpy(last->outName, "a_prev", sizeof(last->outName));
  EXPECT_EQ(TSDB_CODE_INVALID_MSG, tValidateStreamTimeAlignSpec(spec));
}

TEST_F(StreamTimeAlignWire, SetTableRoundTripsLegacyAndWiderBindings) {
  for (int mode = 0; mode < 5; ++mode) {
    SSTriggerPullRequestUnion msg = {};
    msg.base.type = STRIGGER_PULL_SET_TABLE;
    msg.setTableReq.timeAlignPrecision = mode == 0 ? 0 : 1;
    msg.setTableReq.physicalWalKeys = mode >= 3;
    if (mode == 2 || mode == 4) {
      msg.setTableReq.physicalWidths[0] = taosArrayInit(1, sizeof(SStreamWalSlotWidth));
      SStreamWalSlotWidth width = {3, 2050};
      ASSERT_NE(nullptr, taosArrayPush(msg.setTableReq.physicalWidths[0], &width));
    }
    int32_t length = tSerializeSTriggerPullRequest(nullptr, 0, &msg.base);
    ASSERT_GT(length, 0);
    std::vector<uint8_t> bytes(length);
    ASSERT_EQ(length, tSerializeSTriggerPullRequest(bytes.data(), length, &msg.base));
    SSTriggerPullRequestUnion decoded = {};
    ASSERT_EQ(0, tDeserializeSTriggerPullRequest(bytes.data(), length, &decoded));
    EXPECT_EQ(msg.setTableReq.timeAlignPrecision, decoded.setTableReq.timeAlignPrecision);
    EXPECT_EQ(msg.setTableReq.physicalWalKeys, decoded.setTableReq.physicalWalKeys);
    EXPECT_EQ(mode == 2 || mode == 4 ? 1U : 0U, taosArrayGetSize(decoded.setTableReq.physicalWidths[0]));
    if (mode == 2 || mode == 4) {
      const auto* width =
          static_cast<const SStreamWalSlotWidth*>(taosArrayGet(decoded.setTableReq.physicalWidths[0], 0));
      ASSERT_NE(nullptr, width);
      EXPECT_EQ(3, width->slot);
      EXPECT_EQ(2050, width->bytes);
    }
    tDestroySTriggerPullRequest(&decoded);
    tDestroySTriggerPullRequest(&msg);
  }
}

TEST_F(StreamTimeAlignWire, OriginalColumnMetadataAcceptsLegacyAndCurrentWidths) {
  uint8_t bytes[128] = {};
  for (bool withWidth : {false, true}) {
    SEncoder enc = {};
    tEncoderInit(&enc, bytes, sizeof(bytes));
    ASSERT_EQ(0, tStartEncode(&enc));
    ASSERT_EQ(0, tEncodeI32(&enc, 1));
    ASSERT_EQ(0, tEncodeI64(&enc, 0));
    ASSERT_EQ(0, tEncodeI64(&enc, 7));
    ASSERT_EQ(0, tEncodeI16(&enc, 2));
    if (withWidth) ASSERT_EQ(0, tEncodeI32(&enc, 2050));
    tEndEncode(&enc);
    int32_t size = enc.pos;
    tEncoderClear(&enc);
    SSTriggerOrigTableInfoRsp reply = {};
    ASSERT_EQ(0, tDserializeSTriggerOrigTableInfoRsp(bytes, size, &reply));
    const auto* col = static_cast<const OTableInfoRsp*>(taosArrayGet(reply.cols, 0));
    ASSERT_NE(nullptr, col);
    EXPECT_EQ(7, col->uid);
    EXPECT_EQ(2, col->cid);
    EXPECT_EQ(withWidth ? 2050 : 0, col->bytes);
    tDestroySTriggerOrigTableInfoRsp(&reply);
  }
}

TEST_F(StreamTimeAlignWire, BinaryRejectsInvalidTypeAndEveryTruncation) {
  SEncoder size = {};
  tEncoderInit(&size, nullptr, 0);
  ASSERT_EQ(0, tEncodeStreamTimeAlignSpec(&size, req.pTimeAlign));
  std::vector<uint8_t> wire(size.pos);
  tEncoderClear(&size);
  SEncoder encoder = {};
  tEncoderInit(&encoder, wire.data(), wire.size());
  ASSERT_EQ(0, tEncodeStreamTimeAlignSpec(&encoder, req.pTimeAlign));
  tEncoderClear(&encoder);
  for (size_t len = 0; len <= wire.size(); ++len) {
    SDecoder decoder = {};
    tDecoderInit(&decoder, wire.data(), len);
    SStreamTimeAlignSpec* dst = nullptr;
    int32_t               code = tDecodeStreamTimeAlignSpec(&decoder, &dst);
    if (len == wire.size()) {
      ASSERT_EQ(0, code);
      check(dst);
    } else {
      EXPECT_NE(0, code) << len;
      EXPECT_EQ(nullptr, dst);
    }
    tDestroyStreamTimeAlignSpec(&dst);
    tDecoderClear(&decoder);
  }
  auto* col = static_cast<SStreamTimeAlignColumn*>(taosArrayGet(req.pTimeAlign->pColumns, 0));
  col->type.bytes = 0;
  tEncoderInit(&encoder, nullptr, 0);
  EXPECT_NE(0, tEncodeStreamTimeAlignSpec(&encoder, req.pTimeAlign));
  tEncoderClear(&encoder);
}

void StreamTimeAlignWire::checkHeartbeat(int firstMode, int endMode, int64_t expectedAmount, int streamCount) {
  SMStreamHbRspMsg                                                        src = {};
  std::unique_ptr<SMStreamHbRspMsg, decltype(&tDeepFreeSMStreamHbRspMsg)> srcGuard(&src, tDeepFreeSMStreamHbRspMsg);
  src.deploy.streamList = taosArrayInit_s(sizeof(SStmStreamDeploy), streamCount);
  ASSERT_NE(nullptr, src.deploy.streamList);
  for (int i = 0; i < streamCount; ++i) {
    auto* stream = static_cast<SStmStreamDeploy*>(taosArrayGet(src.deploy.streamList, i));
    stream->streamId = 100 + i;
    stream->triggerTask = static_cast<SStmTaskDeploy*>(taosMemoryCalloc(1, sizeof(SStmTaskDeploy)));
    ASSERT_NE(nullptr, stream->triggerTask);
    stream->triggerTask->task.type = STREAM_TRIGGER_TASK;
    stream->triggerTask->task.streamId = stream->streamId;
    stream->triggerTask->task.taskId = 20 + i;
    stream->triggerTask->msg.trigger.streamName = taosStrdup("time_align");
    stream->triggerTask->msg.trigger.triggerType = WINDOW_TYPE_INTERVAL;
    stream->triggerTask->msg.trigger.addOptions = STREAM_OPTION_TIME_ALIGN;
    ASSERT_EQ(0, tCloneStreamTimeAlignSpec(req.pTimeAlign, &stream->triggerTask->msg.trigger.pTimeAlign));
    stream->readerTasks = taosArrayInit_s(sizeof(SStmTaskDeploy), 2);
    stream->runnerTasks = taosArrayInit_s(sizeof(SStmTaskDeploy), 2);
    ASSERT_NE(nullptr, stream->readerTasks);
    ASSERT_NE(nullptr, stream->runnerTasks);
    for (int j = 0; j < 2; ++j) {
      auto* reader = static_cast<SStmTaskDeploy*>(taosArrayGet(stream->readerTasks, j));
      reader->task.type = STREAM_READER_TASK;
      reader->task.taskId = 30 + j;
      if (i == 0 && j == 0 && firstMode == 0) {
        reader->msg.reader.triggerReader = 1;
        auto* spec = static_cast<SStreamExtTriggerSpec*>(taosMemoryCalloc(1, sizeof(SStreamExtTriggerSpec)));
        ASSERT_NE(nullptr, spec);
        reader->msg.reader.pExtSpec = spec;
        spec->sourceType = 1;
        tstrncpy(spec->sourceName, "ext_src", sizeof(spec->sourceName));
        tstrncpy(spec->extTable, "events", sizeof(spec->extTable));
        tstrncpy(spec->tsColumn, "ts", sizeof(spec->tsColumn));
        spec->triggerPrefilterAst = taosStrdup("reader_filter");
      } else {
        reader->msg.reader.msg.calc.calcScanPlan = taosStrdup("reader");
      }
      auto* runner = static_cast<SStmTaskDeploy*>(taosArrayGet(stream->runnerTasks, j));
      runner->task.type = STREAM_RUNNER_TASK;
      runner->task.taskId = 40 + j;
      runner->msg.runner.streamName = taosStrdup("time_align");
      runner->msg.runner.pPlan = taosStrdup("runner");
    }
  }
  auto encode = [&]() {
    SEncoder encoder = {};
    tEncoderInit(&encoder, nullptr, 0);
    EXPECT_EQ(0, tEncodeStreamHbRsp(&encoder, &src));
    std::vector<uint8_t> bytes(encoder.pos);
    tEncoderClear(&encoder);
    tEncoderInit(&encoder, bytes.data(), bytes.size());
    EXPECT_EQ(0, tEncodeStreamHbRsp(&encoder, &src));
    tEncoderClear(&encoder);
    return bytes;
  };
  auto   wire = encode();
  size_t offset = wire.size();
  for (size_t i = 4; i + 4 <= wire.size(); ++i) {
    uint32_t magic = 0;
    memcpy(&magic, wire.data() + i, 4);
    if (magic == STREAM_TIME_ALIGN_FRAME_MAGIC) {
      offset = i;
      break;
    }
  }
  ASSERT_LT(offset, wire.size());
  for (int mode = firstMode; mode < endMode; ++mode) {
    auto bytes = wire;
    if (mode == 1) {
      std::vector<uint8_t> frame(bytes.begin() + offset, bytes.end());
      bytes.insert(bytes.end(), frame.begin(), frame.end());
    } else if (mode == 2) {
      uint16_t version = 99;
      memcpy(bytes.data() + offset + 4, &version, 2);
    } else if (mode == 3) {
      bytes.resize(offset);
    } else if (mode == 4) {
      bytes.pop_back();
    } else if (mode == 5) {
      uint16_t flags = 1;
      memcpy(bytes.data() + offset + 6, &flags, sizeof(flags));
    } else if (mode == 6) {
      int64_t unknownTask = 1234;
      memcpy(bytes.data() + offset + 16 + sizeof(int64_t), &unknownTask, sizeof(unknownTask));
    } else if (mode == 7) {
      // Both entries contain the same configuration, hence have equal encoded length.
      size_t first = offset + 16;
      size_t second = first + (bytes.size() - first) / 2;
      memcpy(bytes.data() + second, bytes.data() + first, 2 * sizeof(int64_t));
    }
    int32_t length = bytes.size() - 4;
    memcpy(bytes.data(), &length, 4);
    SMStreamHbRspMsg                                                        dst = {};
    std::unique_ptr<SMStreamHbRspMsg, decltype(&tDeepFreeSMStreamHbRspMsg)> dstGuard(&dst, tDeepFreeSMStreamHbRspMsg);
    SDecoder                                                                decoder = {};
    tDecoderInit(&decoder, bytes.data(), bytes.size());
    int32_t code = tDecodeStreamHbRsp(&decoder, &dst);
    tDecoderClear(&decoder);
    if (mode != 0) {
      EXPECT_NE(0, code) << mode;
      continue;
    }
    ASSERT_EQ(0, code);
    ASSERT_EQ(streamCount, taosArrayGetSize(dst.deploy.streamList));
    for (int i = 0; i < streamCount; ++i) {
      auto* stream = static_cast<SStmStreamDeploy*>(taosArrayGet(dst.deploy.streamList, i));
      EXPECT_EQ(100 + i, stream->streamId);
      EXPECT_EQ(20 + i, stream->triggerTask->task.taskId);
      check(stream->triggerTask->msg.trigger.pTimeAlign, expectedAmount,
            static_cast<SStreamTimeAlignColumn*>(taosArrayGet(req.pTimeAlign->pColumns, 0))->outName,
            req.pTimeAlign->timezone);
      ASSERT_EQ(2, taosArrayGetSize(stream->readerTasks));
      ASSERT_EQ(2, taosArrayGetSize(stream->runnerTasks));
      auto* reader = static_cast<SStmTaskDeploy*>(taosArrayGet(stream->readerTasks, 1));
      auto* runner = static_cast<SStmTaskDeploy*>(taosArrayGet(stream->runnerTasks, 1));
      if (i == 0) {
        auto* ext = static_cast<SStmTaskDeploy*>(taosArrayGet(stream->readerTasks, 0))->msg.reader.pExtSpec;
        ASSERT_NE(nullptr, ext);
        EXPECT_STREQ("reader_filter", ext->triggerPrefilterAst);
      }
      EXPECT_STREQ("reader", static_cast<char*>(reader->msg.reader.msg.calc.calcScanPlan));
      EXPECT_STREQ("runner", static_cast<char*>(runner->msg.runner.pPlan));
    }
  }
  for (int i = 0; i < streamCount; ++i) {
    auto* trigger = &static_cast<SStmStreamDeploy*>(taosArrayGet(src.deploy.streamList, i))->triggerTask->msg.trigger;
    tDestroyStreamTimeAlignSpec(&trigger->pTimeAlign);
    trigger->addOptions = 0;
  }
  if (firstMode != 8) return;
  auto legacy = encode();
  EXPECT_EQ(offset, legacy.size());
  SMStreamHbRspMsg                                                        dst = {};
  std::unique_ptr<SMStreamHbRspMsg, decltype(&tDeepFreeSMStreamHbRspMsg)> dstGuard(&dst, tDeepFreeSMStreamHbRspMsg);
  SDecoder                                                                decoder = {};
  tDecoderInit(&decoder, legacy.data(), legacy.size());
  EXPECT_EQ(0, tDecodeStreamHbRsp(&decoder, &dst));
  tDecoderClear(&decoder);
  for (int i = 0; i < taosArrayGetSize(dst.deploy.streamList); ++i) {
    EXPECT_EQ(
        nullptr,
        static_cast<SStmStreamDeploy*>(taosArrayGet(dst.deploy.streamList, i))->triggerTask->msg.trigger.pTimeAlign);
  }
}

TEST_F(StreamTimeAlignWire, ZeroSpanSurvivesCreationAndDeployment) {
  auto* rule = static_cast<SStreamTimeAlignColumn*>(taosArrayGet(req.pTimeAlign->pColumns, 0));
  rule->amount = 0;
  char*   json = nullptr;
  int32_t len = 0;
  ASSERT_EQ(0, scmCreateStreamReqToJson(&req, false, &json, &len));
  std::unique_ptr<char, TaosMemoryCleanup>       jsonGuard(json);
  std::unique_ptr<SJson, decltype(&tjsonDelete)> root(tjsonParse(json), tjsonDelete);
  ASSERT_NE(nullptr, root);
  SCMCreateStreamReq                                          dst = {};
  std::unique_ptr<SCMCreateStreamReq, CreateStreamReqCleanup> dstGuard(&dst);
  ASSERT_EQ(0, jsonToSCMCreateStreamReq(root.get(), &dst));
  check(dst.pTimeAlign, 0);
  checkHeartbeat(0, 1, 0);
}

TEST_F(StreamTimeAlignWire, HeartbeatStringBoundaries) {
  auto* rule = static_cast<SStreamTimeAlignColumn*>(taosArrayGet(req.pTimeAlign->pColumns, 0));
  memset(rule->outName, 'a', sizeof(rule->outName) - 1);
  rule->outName[sizeof(rule->outName) - 1] = 0;
  for (size_t length : {size_t{0}, size_t{TD_TIMEZONE_LEN - 1}}) {
    taosMemoryFree(req.pTimeAlign->timezone);
    req.pTimeAlign->timezone = taosStrdup(std::string(length, 'z').c_str());
    ASSERT_NE(nullptr, req.pTimeAlign->timezone);
    checkHeartbeat(0, 1);
  }
}

TEST_F(StreamTimeAlignWire, TimeAlignHeartbeatRoundTrip) { checkHeartbeat(0, 1); }

TEST_F(StreamTimeAlignWire, BatchDeploysAllAlignedTriggers) { checkHeartbeat(0, 1, 8, 512); }

TEST_F(StreamTimeAlignWire, TimeAlignMissingOrDuplicateFrameRejected) { checkHeartbeat(1, 8); }

TEST_F(StreamTimeAlignWire, TimeAlignLegacyAbsent) { checkHeartbeat(8, 8); }

TEST(StreamMsgTest, CreateRequestTimeConfigJsonRoundTrip) {
  SCMCreateStreamReq src = {0};
  src.triggerType = WINDOW_TYPE_INTERVAL;
  tstrncpy(src.charset, "GBK", sizeof(src.charset));
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
  EXPECT_STREQ("GBK", dst.charset);
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

TEST(StreamMsgTest, WalScopeIsOptionalAndVersionZeroIsBounded) {
  for (auto type : {STRIGGER_PULL_WAL_META_NEW, STRIGGER_PULL_WAL_DATA_NEW, STRIGGER_PULL_WAL_META_DATA_NEW}) {
    SSTriggerPullRequestUnion req = {};
    req.base.type = type;
    for (uint64_t round : {0U, 12U}) {
      req.base.timeAlignWalRound = round;
      req.base.timeAlignWalRequest = 19;
      int32_t len = tSerializeSTriggerPullRequest(nullptr, 0, &req.base);
      ASSERT_GT(len, 0);
      std::vector<char> wire(len);
      ASSERT_EQ(len, tSerializeSTriggerPullRequest(wire.data(), len, &req.base));
      SSTriggerPullRequestUnion copy = {};
      ASSERT_EQ(0, tDeserializeSTriggerPullRequest(wire.data(), len, &copy));
      EXPECT_EQ(round, copy.base.timeAlignWalRound);
      EXPECT_EQ(round ? 19U : 0U, copy.base.timeAlignWalRequest);
      tDestroySTriggerPullRequest(&copy);
    }
  }
}

TEST(StreamMsgTest, ReaderDeploymentRetainsTimeAlignConsumerAndLegacyKind) {
  for (int kind : {0, 1, 2, 3}) {
    SStreamReaderDeployMsg source = {};
    source.triggerReader = kind != 0;
    if (source.triggerReader) {
      source.msg.trigger.timeAlignConsumer = kind == 2;
      source.msg.trigger.requireLocalVtableRefs = kind >= 2;
    }
    uint8_t  wire[256] = {};
    SEncoder encoder;
    tEncoderInit(&encoder, wire, sizeof(wire));
    ASSERT_EQ(0, tEncodeSStreamReaderDeployMsg(&encoder, &source));
    ASSERT_EQ(kind, wire[0]);
    int32_t length = encoder.pos;
    tEncoderClear(&encoder);

    SDecoder decoder;
    tDecoderInit(&decoder, wire, length);
    SStreamReaderDeployMsg decoded = {};
    ASSERT_EQ(0, tDecodeSStreamReaderDeployMsg(&decoder, &decoded));
    EXPECT_EQ(source.triggerReader, decoded.triggerReader);
    if (decoded.triggerReader) {
      EXPECT_EQ(kind == 2, decoded.msg.trigger.timeAlignConsumer);
      EXPECT_EQ(kind >= 2, decoded.msg.trigger.requireLocalVtableRefs);
    }
    EXPECT_EQ(length, decoder.pos);
    tFreeSStreamReaderDeployMsg(&decoded);
    tDecoderClear(&decoder);
  }
}

TEST(StreamMsgTest, OnlyAlignedReaderDeploymentsUseTheExtendedHeader) {
  for (int kind : {0, 1, 2, 3}) {
    SCOPED_TRACE(kind);
    SStmTaskDeploy source = {};
    source.task.type = STREAM_READER_TASK;
    auto& reader = source.msg.reader;
    reader.triggerReader = kind != 0;
    if (reader.triggerReader) {
      reader.msg.trigger.timeAlignConsumer = kind == 2;
      reader.msg.trigger.requireLocalVtableRefs = kind >= 2;
    }
    uint8_t  wire[256] = {};
    SEncoder encoder;
    tEncoderInit(&encoder, wire, sizeof(wire));
    ASSERT_EQ(0, tEncodeSStmTaskDeploy(&encoder, &source));
    const int length = encoder.pos;
    tEncoderClear(&encoder);

    SDecoder    decoder;
    SStreamTask header = {};
    tDecoderInit(&decoder, wire, length);
    EXPECT_EQ(kind >= 2 ? TSDB_CODE_MSG_DECODE_ERROR : 0, tDecodeStreamTask(&decoder, &header));
    tDecoderClear(&decoder);

    SStmTaskDeploy decoded = {};
    tDecoderInit(&decoder, wire, length);
    ASSERT_EQ(0, tDecodeSStmTaskDeploy(&decoder, &decoded));
    EXPECT_EQ(length, decoder.pos);
    EXPECT_EQ(STREAM_READER_TASK, decoded.task.type);
    EXPECT_EQ(kind != 0, decoded.msg.reader.triggerReader);
    if (decoded.msg.reader.triggerReader) {
      EXPECT_EQ(kind == 2, decoded.msg.reader.msg.trigger.timeAlignConsumer);
      EXPECT_EQ(kind >= 2, decoded.msg.reader.msg.trigger.requireLocalVtableRefs);
    }
    tFreeSStmTaskDeploy(&decoded);
    tDecoderClear(&decoder);
  }
}

TEST(StreamMsgTest, OnlyAlignedTriggersRequireTheExtendedDeploymentHeader) {
  for (bool aligned : {false, true}) {
    SStmTaskDeploy source = {};
    source.task.type = STREAM_TRIGGER_TASK;
    source.msg.trigger.streamName = const_cast<char*>("beats");
    source.msg.trigger.triggerType = WINDOW_TYPE_INTERVAL;
    source.msg.trigger.addOptions = aligned ? STREAM_OPTION_TIME_ALIGN : 0;
    uint8_t  wire[1024] = {};
    SEncoder encoder;
    tEncoderInit(&encoder, wire, sizeof(wire));
    ASSERT_EQ(0, tEncodeSStmTaskDeploy(&encoder, &source));
    const int length = encoder.pos;
    tEncoderClear(&encoder);

    SDecoder    decoder;
    SStreamTask header = {};
    tDecoderInit(&decoder, wire, length);
    EXPECT_EQ(aligned ? TSDB_CODE_MSG_DECODE_ERROR : 0, tDecodeStreamTask(&decoder, &header));
    tDecoderClear(&decoder);
    SStmTaskDeploy decoded = {};
    tDecoderInit(&decoder, wire, length);
    ASSERT_EQ(0, tDecodeSStmTaskDeploy(&decoder, &decoded));
    EXPECT_EQ(length, decoder.pos);
    EXPECT_EQ(STREAM_TRIGGER_TASK, decoded.task.type);
    EXPECT_EQ(source.msg.trigger.addOptions, decoded.msg.trigger.addOptions);
    tFreeSStmTaskDeploy(&decoded);
    tDecoderClear(&decoder);
  }
}

TEST(StreamMsgTest, OnlyAlignedTopRunnersRequireTheExtendedDeploymentHeader) {
  for (bool top : {false, true}) {
    for (bool aligned : {false, true}) {
      SStmTaskDeploy src = {};
      src.task.type = STREAM_RUNNER_TASK;
      src.msg.runner.streamName = const_cast<char*>("beats");
      src.msg.runner.pPlan = const_cast<char*>("runner");
      src.msg.runner.topPlan = top;
      src.msg.runner.addOptions = aligned ? STREAM_OPTION_TIME_ALIGN : 0;
      uint8_t  wire[1024] = {};
      SEncoder encoder;
      tEncoderInit(&encoder, wire, sizeof(wire));
      ASSERT_EQ(0, tEncodeSStmTaskDeploy(&encoder, &src));
      int length = encoder.pos;
      tEncoderClear(&encoder);

      SStreamTask header = {};
      SDecoder    decoder;
      tDecoderInit(&decoder, wire, length);
      EXPECT_EQ(top && aligned ? TSDB_CODE_MSG_DECODE_ERROR : 0, tDecodeStreamTask(&decoder, &header));
      tDecoderClear(&decoder);
      SStmTaskDeploy dst = {};
      tDecoderInit(&decoder, wire, length);
      ASSERT_EQ(0, tDecodeSStmTaskDeploy(&decoder, &dst));
      EXPECT_EQ(length, decoder.pos);
      EXPECT_EQ(STREAM_RUNNER_TASK, dst.task.type);
      EXPECT_EQ(top, dst.msg.runner.topPlan);
      EXPECT_EQ(src.msg.runner.addOptions, dst.msg.runner.addOptions);
      tFreeSStmTaskDeploy(&dst);
      tDecoderClear(&decoder);
    }
  }
}

TEST(StreamMsgTest, TimeAlignFinalEmptyBatchRoundTrip) {
  SSTriggerTimeAlignDataRsp source = {};
  source.scanId = UINT64_MAX;
  source.batchId = UINT64_MAX - 1;
  source.done = true;
  int len = tSerializeSTriggerTimeAlignDataRsp(nullptr, 0, &source);
  ASSERT_GT(len, 0);
  std::vector<char> bytes(len);
  ASSERT_EQ(len, tSerializeSTriggerTimeAlignDataRsp(bytes.data(), len, &source));
  SSTriggerTimeAlignDataRsp decoded = {};
  EXPECT_EQ(0, tDeserializeSTriggerTimeAlignDataRsp(bytes.data(), len, &decoded));
  EXPECT_EQ(source.scanId, decoded.scanId);
  EXPECT_EQ(source.batchId, decoded.batchId);
  EXPECT_TRUE(decoded.done);
  EXPECT_EQ(nullptr, decoded.pBlock);
  tDestroySTriggerTimeAlignDataRsp(&decoded);
  for (int cut = 0; cut < len; ++cut) {
    EXPECT_NE(0, tDeserializeSTriggerTimeAlignDataRsp(bytes.data(), cut, &decoded));
    tDestroySTriggerTimeAlignDataRsp(&decoded);
  }
}

TEST(StreamMsgTest, TimeAlignRawBatchRetainsPhysicalColumnId) {
  SSTriggerTimeAlignDataRsp src = {};
  ASSERT_EQ(0, createDataBlock(&src.pBlock));
  SColumnInfoData col = createColumnInfoData(TSDB_DATA_TYPE_INT, sizeof(int32_t), 72);
  ASSERT_EQ(0, blockDataAppendColInfo(src.pBlock, &col));
  ASSERT_EQ(0, blockDataEnsureCapacity(src.pBlock, 1));
  int32_t value = 80;
  ASSERT_EQ(0, colDataSetVal(static_cast<SColumnInfoData*>(taosArrayGet(src.pBlock->pDataBlock, 0)), 0,
                             reinterpret_cast<const char*>(&value), false));
  src.pBlock->info.rows = 1;
  int len = tSerializeSTriggerTimeAlignDataRsp(nullptr, 0, &src);
  ASSERT_GT(len, 0);
  std::vector<char> bytes(len);
  ASSERT_EQ(len, tSerializeSTriggerTimeAlignDataRsp(bytes.data(), len, &src));
  tDestroySTriggerTimeAlignDataRsp(&src);
  SSTriggerTimeAlignDataRsp decoded = {};
  ASSERT_EQ(0, tDeserializeSTriggerTimeAlignDataRsp(bytes.data(), len, &decoded));
  ASSERT_NE(nullptr, decoded.pBlock);
  EXPECT_EQ(1, decoded.pBlock->info.rows);
  auto* actual = static_cast<SColumnInfoData*>(taosArrayGet(decoded.pBlock->pDataBlock, 0));
  ASSERT_NE(nullptr, actual);
  EXPECT_EQ(72, actual->info.colId);
  EXPECT_EQ(0, memcmp(colDataGetData(actual, 0), &value, sizeof(value)));
  tDestroySTriggerTimeAlignDataRsp(&decoded);
}

TEST(StreamTimeAlignWalCheck, PagesBoundSixteenThousandPhysicalCells) {
  SSTriggerPullRequestUnion request = {};
  request.base.type = STRIGGER_PULL_TIME_ALIGN_WAL_CHECK;
  request.base.timeAlignWalRound = 7;
  request.base.timeAlignWalRequest = 10;
  request.walCheckReq.metaRequest = 9;
  const int                       cells = 1000 * 16;
  std::vector<STimeAlignWalProbe> all(cells);
  int                             pages = 0;
  int                             maxBytes = 0;
  for (int i = 0; i < cells; ++i) {
    all[i].version = 99;
    all[i].uid = i / 16 + 1;
    all[i].cid = static_cast<col_id_t>(i % 16 + 2);
    all[i].ts = 100;
  }
  for (int i = 0; i < cells; i += STREAM_TIME_ALIGN_WAL_CHECK_CELLS) {
    SArray slice = {};
    slice.size = slice.capacity = (std::min)(cells - i, STREAM_TIME_ALIGN_WAL_CHECK_CELLS);
    slice.elemSize = sizeof(all[0]);
    slice.pData = &all[i];
    request.walCheckReq.cells = &slice;
    int bytes = tSerializeSTriggerPullRequest(nullptr, 0, &request.base);
    ASSERT_GT(bytes, 0);
    ASSERT_LT(bytes, 7000);
    maxBytes = (std::max)(maxBytes, bytes);
    ++pages;
    std::vector<char> buf(bytes);
    ASSERT_EQ(bytes, tSerializeSTriggerPullRequest(buf.data(), bytes, &request.base));
    SSTriggerPullRequestUnion copy = {};
    ASSERT_EQ(0, tDeserializeSTriggerPullRequest(buf.data(), bytes, &copy));
    ASSERT_EQ(slice.size, taosArrayGetSize(copy.walCheckReq.cells));
    auto* last = static_cast<STimeAlignWalProbe*>(taosArrayGetLast(copy.walCheckReq.cells));
    EXPECT_EQ(0, tCompareTimeAlignWalProbe(last, &all[i + slice.size - 1]));
    tDestroySTriggerPullRequest(&copy);
  }
  EXPECT_EQ(63, pages);
  RecordProperty("maximum_page_bytes", maxBytes);
  request.base.type = STRIGGER_PULL_WAL_META_NEW;
  request.walMetaNewReq.lastVer = 99;
  request.walMetaNewReq.ctime = INT64_MAX;
  request.walMetaNewReq.withGroupColVals = false;
  request.walMetaNewReq.requestId = 0;
  int metaBytes = tSerializeSTriggerPullRequest(nullptr, 0, &request.base);
  EXPECT_GT(metaBytes, 0);
  EXPECT_LT(metaBytes, 128);
  RecordProperty("metadata_poll_bytes", metaBytes);
}

TEST(StreamTimeAlignWalCheck, MissingStaleAndMalformedPageAreRejected) {
  const uint64_t     id = 99;
  STimeAlignWalProbe cells[2] = {};
  uint8_t            reply[sizeof(id) + 2] = {};
  memcpy(reply, &id, sizeof(id));
  reply[sizeof(id)] = 1;
  EXPECT_NE(0, tDecodeTimeAlignWalCheckRsp(nullptr, 0, id, cells, 2));
  EXPECT_NE(0, tDecodeTimeAlignWalCheckRsp(reply, sizeof(reply) - 1, id, cells, 2));
  EXPECT_NE(0, tDecodeTimeAlignWalCheckRsp(reply, sizeof(reply), id + 1, cells, 2));
  reply[sizeof(id) + 1] = 2;
  EXPECT_NE(0, tDecodeTimeAlignWalCheckRsp(reply, sizeof(reply), id, cells, 2));
  EXPECT_FALSE(cells[0].written);
  reply[sizeof(id) + 1] = 0;
  ASSERT_EQ(0, tDecodeTimeAlignWalCheckRsp(reply, sizeof(reply), id, cells, 2));
  EXPECT_TRUE(cells[0].written);
  EXPECT_FALSE(cells[1].written);
}

TEST(StreamMsgTest, AlignedBoundRequiresReaderIdentity) {
  SStreamTsResponse response = {};
  response.ver = 12;
  uint8_t     buf[128] = {};
  SSDataBlock block = {};
  int32_t     len = tSerializeSStreamTsResponse(buf, sizeof(buf), &response);
  ASSERT_GT(len, 0);
  EXPECT_EQ(0, tDeserializeSStreamTsResponse(buf, len, &block));
  uint64_t sourceId = 0;
  EXPECT_NE(0, tDeserializeTimeAlignBoundRsp(buf, len, &block, &sourceId));
  response.sourceId = 73;
  len = tSerializeSStreamTsResponse(buf, sizeof(buf), &response);
  ASSERT_GT(len, 0);
  EXPECT_EQ(0, tDeserializeTimeAlignBoundRsp(buf, len, &block, &sourceId));
  EXPECT_EQ(response.sourceId, sourceId);
  EXPECT_EQ(12u, block.info.id.groupId);
  blockDataFreeRes(&block);
}

// A client that predates txn support sends the same frame without the trailing txnId.
// It must still decode, and txnId must stay 0 (= not in a txn), or STOP / START from old
// clients would fail the RPC in a mixed-version cluster.
TEST(StreamMsgTest, PauseStreamReqDecodesLegacyFrameWithoutTxnId) {
  const char legacyName[] = "db1.s3";
  const int32_t nameLen = (int32_t)sizeof(legacyName);

  auto build = [&](uint8_t* buf, int32_t cap) {
    SEncoder enc = {};
    tEncoderInit(&enc, buf, cap);
    EXPECT_EQ(0, tStartEncode(&enc));
    EXPECT_EQ(0, tEncodeBinary(&enc, (const uint8_t*)legacyName, nameLen));
    EXPECT_EQ(0, tEncodeI8(&enc, 1));
    tEndEncode(&enc);
    int32_t pos = enc.pos;
    tEncoderClear(&enc);
    return pos;
  };
  int32_t encoded = build(nullptr, 0);
  ASSERT_GT(encoded, 0);
  std::vector<uint8_t> bytes(encoded, 0);
  ASSERT_EQ(encoded, build(bytes.data(), encoded));

  SMPauseStreamReq dst = {0};
  ASSERT_EQ(0, tDeserializeSMPauseStreamReq(bytes.data(), encoded, &dst));
  EXPECT_STREQ(legacyName, dst.name);
  EXPECT_EQ(1, dst.igNotExists);
  EXPECT_EQ(0, dst.txnId);
  tFreeMPauseStreamReq(&dst);
}

TEST(StreamMsgTest, ResumeStreamReqDecodesLegacyFrameWithoutTxnId) {
  const char legacyName[] = "db1.s4";
  const int32_t nameLen = (int32_t)sizeof(legacyName);

  auto build = [&](uint8_t* buf, int32_t cap) {
    SEncoder enc = {};
    tEncoderInit(&enc, buf, cap);
    EXPECT_EQ(0, tStartEncode(&enc));
    EXPECT_EQ(0, tEncodeBinary(&enc, (const uint8_t*)legacyName, nameLen));
    EXPECT_EQ(0, tEncodeI8(&enc, 1));
    EXPECT_EQ(0, tEncodeI8(&enc, 1));
    tEndEncode(&enc);
    int32_t pos = enc.pos;
    tEncoderClear(&enc);
    return pos;
  };
  int32_t encoded = build(nullptr, 0);
  ASSERT_GT(encoded, 0);
  std::vector<uint8_t> bytes(encoded, 0);
  ASSERT_EQ(encoded, build(bytes.data(), encoded));

  SMResumeStreamReq dst = {0};
  ASSERT_EQ(0, tDeserializeSMResumeStreamReq(bytes.data(), encoded, &dst));
  EXPECT_STREQ(legacyName, dst.name);
  EXPECT_EQ(1, dst.igNotExists);
  EXPECT_EQ(1, dst.igUntreated);
  EXPECT_EQ(0, dst.txnId);
  tFreeMResumeStreamReq(&dst);
}

TEST(StreamMsgTest, PauseStreamReqCarriesTxnId) {
  SMPauseStreamReq src = {0};
  char nm5[] = "db1.s5";
  src.name = nm5;
  src.igNotExists = 1;
  src.txnId = 0x0123456789ABCDEFLL;
  int32_t len = tSerializeSMPauseStreamReq(nullptr, 0, &src);
  ASSERT_GT(len, 0);
  std::vector<uint8_t> bytes(len, 0);
  ASSERT_EQ(len, tSerializeSMPauseStreamReq(bytes.data(), len, &src));
  SMPauseStreamReq dst = {0};
  ASSERT_EQ(0, tDeserializeSMPauseStreamReq(bytes.data(), len, &dst));
  EXPECT_STREQ("db1.s5", dst.name);
  EXPECT_EQ(1, dst.igNotExists);
  EXPECT_EQ(src.txnId, dst.txnId);
  tFreeMPauseStreamReq(&dst);
}

TEST(StreamMsgTest, ResumeStreamReqCarriesTxnId) {
  SMResumeStreamReq src = {0};
  char nm6[] = "db1.s6";
  src.name = nm6;
  src.igNotExists = 1;
  src.igUntreated = 1;
  src.txnId = 0x0123456789ABCDEFLL;
  int32_t len = tSerializeSMResumeStreamReq(nullptr, 0, &src);
  ASSERT_GT(len, 0);
  std::vector<uint8_t> bytes(len, 0);
  ASSERT_EQ(len, tSerializeSMResumeStreamReq(bytes.data(), len, &src));
  SMResumeStreamReq dst = {0};
  ASSERT_EQ(0, tDeserializeSMResumeStreamReq(bytes.data(), len, &dst));
  EXPECT_STREQ("db1.s6", dst.name);
  EXPECT_EQ(1, dst.igNotExists);
  EXPECT_EQ(1, dst.igUntreated);
  EXPECT_EQ(src.txnId, dst.txnId);
  tFreeMResumeStreamReq(&dst);
}

TEST(StreamMsgTest, CreateRequestRejectsExplicitInvalidCharset) {
  SCMCreateStreamReq src = {0};
  src.triggerType = WINDOW_TYPE_INTERVAL;
  tstrncpy(src.charset, "invalid-charset", sizeof(src.charset));
  char *json = nullptr;
  int32_t length = 0;
  ASSERT_EQ(TSDB_CODE_SUCCESS, scmCreateStreamReqToJson(&src, false, &json, &length));
  std::unique_ptr<char, TaosMemoryCleanup> jsonGuard(json);
  std::unique_ptr<SJson, decltype(&tjsonDelete)> root(tjsonParse(json), tjsonDelete);
  ASSERT_NE(nullptr, root.get());
  SCMCreateStreamReq dst = {0};
  std::unique_ptr<SCMCreateStreamReq, CreateStreamReqCleanup> dstGuard(&dst);
  EXPECT_EQ(TSDB_CODE_CHARSET_CONVERT_ERROR, jsonToSCMCreateStreamReq(root.get(), &dst));
  tjsonDeleteItemFromObject(root.get(), "Charset");
  dst = {};
  EXPECT_EQ(TSDB_CODE_SUCCESS, jsonToSCMCreateStreamReq(root.get(), &dst));
  EXPECT_STREQ("", dst.charset);
}
