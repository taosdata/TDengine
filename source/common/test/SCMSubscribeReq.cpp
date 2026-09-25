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
#include <cstring>
#include <string>
#include <vector>
#include "tmsg.h"

namespace {

struct SubscribeWire {
  std::string cgroup = "group";
  std::string clientId = "client";
  std::vector<std::string> topics = {"0.topic"};
  std::string user = "root";
  std::string fqdn = "localhost";
  int32_t topicNum = 1;
  bool extended = true;
};

std::vector<uint8_t> encodeSubscribeWire(const SubscribeWire &input) {
  auto encode = [&](void **buffer) {
    int32_t length = taosEncodeFixedI64(buffer, -123456789);
    length += taosEncodeString(buffer, input.cgroup.c_str());
    length += taosEncodeString(buffer, input.clientId.c_str());
    length += taosEncodeFixedI32(buffer, input.topicNum);
    for (const auto &topic : input.topics) {
      length += taosEncodeString(buffer, topic.c_str());
    }
    length += taosEncodeFixedI8(buffer, 1);
    length += taosEncodeFixedI8(buffer, 1);
    length += taosEncodeFixedI32(buffer, 5000);
    length += taosEncodeFixedI8(buffer, -2);
    length += taosEncodeFixedI8(buffer, 0);
    length += taosEncodeFixedI8(buffer, 1);
    if (input.extended) {
      length += taosEncodeFixedI32(buffer, 45000);
      length += taosEncodeFixedI32(buffer, 300000);
      length += taosEncodeString(buffer, input.user.c_str());
      length += taosEncodeString(buffer, input.fqdn.c_str());
    }
    return length;
  };
  std::vector<uint8_t> result(encode(nullptr));
  void *cursor = result.data();
  encode(&cursor);
  return result;
}

void expectSubscribeRejected(std::vector<uint8_t> wire) {
  struct GuardedRequest {
    uint64_t before = UINT64_MAX;
    SCMSubscribeReq request = {};
    uint64_t after = UINT64_MAX;
  } guarded;
  EXPECT_NE(tDeserializeSCMSubscribeReq(wire.data(), &guarded.request, wire.size()), 0);
  EXPECT_EQ(guarded.before, UINT64_MAX);
  EXPECT_EQ(guarded.after, UINT64_MAX);
  EXPECT_EQ(guarded.request.topicNames, nullptr);
  taosArrayDestroyP(guarded.request.topicNames, nullptr);
}

}  // namespace

TEST(td_msg_test, scm_subscribe_req_wire_compatibility) {
  for (bool extended : {false, true}) {
    SubscribeWire input;
    input.extended = extended;
    auto wire = encodeSubscribeWire(input);
    SCMSubscribeReq output = {};
    ASSERT_EQ(tDeserializeSCMSubscribeReq(wire.data(), &output, wire.size()), 0);
    EXPECT_EQ(output.consumerId, -123456789);
    EXPECT_STREQ(output.cgroup, input.cgroup.c_str());
    EXPECT_STREQ(output.clientId, input.clientId.c_str());
    ASSERT_EQ(taosArrayGetSize(output.topicNames), 1);
    EXPECT_STREQ(static_cast<char *>(taosArrayGetP(output.topicNames, 0)), "0.topic");
    EXPECT_EQ(output.withTbName, 1);
    EXPECT_EQ(output.autoCommit, 1);
    EXPECT_EQ(output.autoCommitInterval, 5000);
    EXPECT_EQ(output.resetOffsetCfg, -2);
    EXPECT_EQ(output.enableReplay, 0);
    EXPECT_EQ(output.enableBatchMeta, 1);
    EXPECT_EQ(output.sessionTimeoutMs, extended ? 45000 : DEFAULT_SESSION_TIMEOUT);
    EXPECT_EQ(output.maxPollIntervalMs, extended ? 300000 : DEFAULT_MAX_POLL_INTERVAL);
    EXPECT_STREQ(output.user, extended ? "root" : "");
    EXPECT_STREQ(output.fqdn, extended ? "localhost" : "");
    if (extended) {
      std::vector<uint8_t> serialized(tSerializeSCMSubscribeReq(nullptr, &output));
      void *cursor = serialized.data();
      ASSERT_EQ(tSerializeSCMSubscribeReq(&cursor, &output), serialized.size());
      EXPECT_EQ(serialized, wire);
    }
    taosArrayDestroyP(output.topicNames, nullptr);
  }
}

TEST(td_msg_test, scm_subscribe_req_rejects_truncated_messages) {
  SubscribeWire input;
  auto complete = encodeSubscribeWire(input);
  input.extended = false;
  size_t legacyLength = encodeSubscribeWire(input).size();
  for (size_t length = 0; length < complete.size(); ++length) {
    if (length == legacyLength) {
      continue;
    }
    SCOPED_TRACE(length);
    expectSubscribeRejected(std::vector<uint8_t>(complete.begin(), complete.begin() + length));
  }
}

TEST(td_msg_test, scm_subscribe_req_rejects_oversized_strings) {
  SubscribeWire input;
  input.cgroup.assign(4000, 'x');
  expectSubscribeRejected(encodeSubscribeWire(input));
  input = SubscribeWire();
  input.cgroup.assign(TSDB_CGROUP_LEN, 'x');
  expectSubscribeRejected(encodeSubscribeWire(input));
  input = SubscribeWire();
  input.clientId.assign(TSDB_CLIENT_ID_LEN, 'x');
  expectSubscribeRejected(encodeSubscribeWire(input));
  input = SubscribeWire();
  input.user.assign(TSDB_USER_LEN, 'x');
  expectSubscribeRejected(encodeSubscribeWire(input));
  input = SubscribeWire();
  input.fqdn.assign(TSDB_FQDN_LEN, 'x');
  expectSubscribeRejected(encodeSubscribeWire(input));
  input = SubscribeWire();
  input.topics.emplace_back(TSDB_TOPIC_FNAME_LEN, 'x');
  input.topicNum = 2;
  expectSubscribeRejected(encodeSubscribeWire(input));
}

TEST(td_msg_test, scm_subscribe_req_accepts_string_capacity_boundaries) {
  SubscribeWire input;
  input.cgroup.assign(TSDB_CGROUP_LEN - 1, 'g');
  input.clientId.assign(TSDB_CLIENT_ID_LEN - 1, 'c');
  input.user.assign(TSDB_USER_LEN - 1, 'u');
  input.fqdn.assign(TSDB_FQDN_LEN - 1, 'f');
  input.topics[0].assign(TSDB_TOPIC_FNAME_LEN - 1, 't');
  auto wire = encodeSubscribeWire(input);
  SCMSubscribeReq output = {};
  ASSERT_EQ(tDeserializeSCMSubscribeReq(wire.data(), &output, wire.size()), 0);
  EXPECT_STREQ(output.cgroup, input.cgroup.c_str());
  EXPECT_STREQ(output.clientId, input.clientId.c_str());
  EXPECT_STREQ(output.user, input.user.c_str());
  EXPECT_STREQ(output.fqdn, input.fqdn.c_str());
  EXPECT_STREQ(static_cast<char *>(taosArrayGetP(output.topicNames, 0)), input.topics[0].c_str());
  taosArrayDestroyP(output.topicNames, nullptr);
}

TEST(td_msg_test, scm_subscribe_req_validates_topic_count) {
  for (int32_t count : {-1, 10001, 1 << 30}) {
    SubscribeWire input;
    input.topicNum = count;
    expectSubscribeRejected(encodeSubscribeWire(input));
  }
  SubscribeWire input;
  input.topicNum = 10001;
  input.topics.assign(10001, "");
  expectSubscribeRejected(encodeSubscribeWire(input));
  input.topicNum = 10000;
  input.topics.clear();
  expectSubscribeRejected(encodeSubscribeWire(input));
  for (int32_t count : {0, 2, 10000}) {
    input.topicNum = count;
    input.topics.assign(count, "0.topic");
    auto wire = encodeSubscribeWire(input);
    SCMSubscribeReq output = {};
    ASSERT_EQ(tDeserializeSCMSubscribeReq(wire.data(), &output, wire.size()), 0);
    EXPECT_EQ(taosArrayGetSize(output.topicNames), count);
    taosArrayDestroyP(output.topicNames, nullptr);
  }
}

TEST(td_msg_test, scm_subscribe_req_rejects_invalid_string_lengths) {
  const std::vector<std::vector<uint8_t>> lengths = {
      {0x80},
      {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01},
      {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02},
      {0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00},
      {0x10, 'x'},
  };
  for (const auto &length : lengths) {
    std::vector<uint8_t> wire(sizeof(int64_t), 0);
    wire.insert(wire.end(), length.begin(), length.end());
    expectSubscribeRejected(wire);
  }
}

TEST(td_msg_test, scm_subscribe_req_rejects_invalid_arguments) {
  SCMSubscribeReq output = {};
  uint8_t byte = 0;
  EXPECT_NE(tDeserializeSCMSubscribeReq(nullptr, &output, 1), 0);
  EXPECT_NE(tDeserializeSCMSubscribeReq(&byte, nullptr, 1), 0);
  EXPECT_NE(tDeserializeSCMSubscribeReq(&byte, &output, -1), 0);
  EXPECT_NE(tDeserializeSCMSubscribeReq(&byte, &output, 0), 0);
}
