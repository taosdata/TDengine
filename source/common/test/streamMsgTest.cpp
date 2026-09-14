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
