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
#include <atomic>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "transComm.h"
#include "transportInt.h"
#include "trpc.h"
#include "tversion.h"
#include "tmisce.h"
#include "tmsg.h"

#if defined(USE_UV) && !defined(TD_ASTRA_RPC)

using namespace std;

struct QueueElem {
  queue q;
  int   val;
};
class QueueObj {
 public:
  QueueObj() {
    // avoid formate
    QUEUE_INIT(&head);
  }
  void Push(QueueElem *el) {
    // avoid formate
    QUEUE_PUSH(&head, &el->q);
  }
  QueueElem *Pop() {
    QueueElem *el = NULL;
    if (!IsEmpty()) {
      queue *h = QUEUE_HEAD(&head);
      el = QUEUE_DATA(h, QueueElem, q);
      QUEUE_REMOVE(h);
    }
    return el;
  }
  bool IsEmpty() {
    // avoid formate
    return QUEUE_IS_EMPTY(&head);
  }
  void RmElem(QueueElem *el) {
    // impl
    QUEUE_REMOVE(&el->q);
  }
  void ForEach(std::vector<int> &result) {
    queue *h;
    QUEUE_FOREACH(h, &head) {
      // add more
      QueueElem *el = QUEUE_DATA(h, QueueElem, q);
      result.push_back(el->val);
    }
  }

 private:
  queue head;
};

class QueueEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    // TODO
    q = new QueueObj();
  }
  virtual void TearDown() {
    delete q;
    // formate
  }
  QueueObj *q;
};

TEST_F(QueueEnv, testPushAndPop) {
  // add more test
  TD_ALWAYS_ASSERT(q->IsEmpty());

  for (int i = 0; i < 100; i++) {
    QueueElem *el = (QueueElem *)taosMemoryMalloc(sizeof(QueueElem));
    el->val = i;
    q->Push(el);
  }
  int i = 0;
  while (!q->IsEmpty()) {
    QueueElem *el = q->Pop();
    TD_ALWAYS_ASSERT(el->val == i++);
    taosMemoryFree(el);
  }
  TD_ALWAYS_ASSERT(q->IsEmpty());
}
TEST_F(QueueEnv, testRm) {
  // add more test

  std::vector<QueueElem *> set;
  TD_ALWAYS_ASSERT(q->IsEmpty());

  for (int i = 0; i < 100; i++) {
    QueueElem *el = (QueueElem *)taosMemoryMalloc(sizeof(QueueElem));
    el->val = i;
    q->Push(el);
    set.push_back(el);
  }
  for (int i = set.size() - 1; i >= 0; i--) {
    QueueElem *el = set[i];
    q->RmElem(el);
    taosMemoryFree(el);
  }
  TD_ALWAYS_ASSERT(q->IsEmpty());
}
TEST_F(QueueEnv, testIter) {
  // add more test
  TD_ALWAYS_ASSERT(q->IsEmpty());
  std::vector<int> vals;
  for (int i = 0; i < 100; i++) {
    QueueElem *el = (QueueElem *)taosMemoryMalloc(sizeof(QueueElem));
    el->val = i;
    q->Push(el);
    vals.push_back(i);
  }
  std::vector<int> result;
  q->ForEach(result);
  TD_ALWAYS_ASSERT(result.size() == vals.size());
  while (!q->IsEmpty()) {
    taosMemoryFree(q->Pop());
  }
}

// GHSA-m4h8-2wjh-rrpq zero-length case: comp=1 with an empty content area, i.e. not even
// one byte of the STransCompMsg header is present. The minimum-length check in
// transValidateCompMsg rejects it before any field of that header is read.
TEST(TransCompressTest, RejectsCompressedPacketWithoutCompressionHeader) {
  int32_t msgLen = sizeof(STransMsgHead);
  char   *msg = (char *)taosMemoryCalloc(1, msgLen);
  ASSERT_NE(msg, nullptr);

  STransMsgHead *head = (STransMsgHead *)msg;
  head->version = TRANS_VER;
  head->comp = 1;
  head->msgLen = htonl(msgLen);

  char   *packet = msg;
  int32_t packetLen = msgLen;
  EXPECT_EQ(transDecompressMsg(&packet, &packetLen), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(packet, msg);
  EXPECT_EQ(packetLen, msgLen);

  char   *out = nullptr;
  int32_t outLen = 0;
  EXPECT_EQ(transDecompressMsgExt(msg, msgLen, &out, &outLen), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(out, nullptr);
  EXPECT_EQ(outLen, 0);

  taosMemoryFree(msg);
}

TEST(TransCompressTest, RejectsInvalidOriginalLength) {
  int32_t msgLen = sizeof(STransMsgHead) + sizeof(STransCompMsg) + 1;
  char   *msg = (char *)taosMemoryCalloc(1, msgLen);
  ASSERT_NE(msg, nullptr);

  STransMsgHead *head = (STransMsgHead *)msg;
  head->version = TRANS_VER;
  head->comp = 1;
  head->msgLen = htonl(msgLen);

  STransCompMsg *comp = (STransCompMsg *)transContFromHead(head);
  comp->contLen = htonl(0);

  char   *packet = msg;
  int32_t packetLen = msgLen;
  EXPECT_EQ(transDecompressMsg(&packet, &packetLen), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(packet, msg);
  EXPECT_EQ(packetLen, msgLen);

  char   *out = nullptr;
  int32_t outLen = 0;
  EXPECT_EQ(transDecompressMsgExt(msg, msgLen, &out, &outLen), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(out, nullptr);
  EXPECT_EQ(outLen, 0);

  comp->contLen = htonl(TRANS_MSG_LIMIT);
  EXPECT_EQ(transDecompressMsg(&packet, &packetLen), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(packet, msg);
  EXPECT_EQ(packetLen, msgLen);
  EXPECT_EQ(transDecompressMsgExt(msg, msgLen, &out, &outLen), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(out, nullptr);
  EXPECT_EQ(outLen, 0);

  comp->contLen = htonl(0x7fffffff);
  EXPECT_EQ(transDecompressMsg(&packet, &packetLen), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(packet, msg);
  EXPECT_EQ(packetLen, msgLen);
  EXPECT_EQ(transDecompressMsgExt(msg, msgLen, &out, &outLen), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(out, nullptr);
  EXPECT_EQ(outLen, 0);

  taosMemoryFree(msg);
}

namespace {
// Content length shared by the compression tests.
constexpr int32_t kTestContLen = 4096;

// Build a valid compressed rpc packet (head + STransCompMsg + lz4 block) from compressible
// content. Sets *expected to a copy of the original content for round-trip comparison.
char *buildCompressedPacket(int32_t contLen, char **expected, int32_t *compMsgLen) {
  int32_t allocLen = (int32_t)(sizeof(STransMsgHead)) + contLen;
  char *msg = (char *)taosMemoryCalloc(1, allocLen);
  if (msg == nullptr) {
    tError("buildCompressedPacket: packet alloc failed, len:%d", allocLen);
    return nullptr;
  }

  STransMsgHead *head = (STransMsgHead *)msg;
  head->version = TRANS_VER;

  char *cont = transContFromHead(head);
  memset(cont, 'A', contLen);

  *expected = (char *)taosMemoryMalloc(contLen);
  if (*expected == nullptr) {
    tError("buildCompressedPacket: expected alloc failed, len:%d", contLen);
    taosMemoryFree(msg);
    return nullptr;
  }
  memcpy(*expected, cont, contLen);

  int32_t newContLen = transCompressMsg(cont, contLen);
  if (newContLen <= (int32_t)sizeof(STransCompMsg) || head->comp != 1) {
    // The helper cannot use ASSERT_* (it returns a value); fail the build of the
    // packet explicitly so callers stop on their ASSERT_NE(msg, nullptr) instead
    // of continuing with a half-built packet.
    tError("buildCompressedPacket: compression failed, contLen:%d, compressedLen:%d, comp:%d", contLen, newContLen,
           head->comp);
    taosMemoryFree(*expected);
    *expected = nullptr;
    taosMemoryFree(msg);
    return nullptr;
  }

  *compMsgLen = (int32_t)(sizeof(STransMsgHead)) + newContLen;
  head->msgLen = htonl(*compMsgLen);
  return msg;
}

// A rejected packet must come back untouched from both entry points.
void assertRejected(char *msg, int32_t len) {
  char *packet = msg;
  int32_t packetLen = len;
  EXPECT_EQ(transDecompressMsg(&packet, &packetLen), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(packet, msg);
  EXPECT_EQ(packetLen, len);

  char *out = nullptr;
  int32_t outLen = 0;
  EXPECT_EQ(transDecompressMsgExt(msg, len, &out, &outLen), TSDB_CODE_INVALID_MSG);
  EXPECT_EQ(out, nullptr);
  EXPECT_EQ(outLen, 0);
}
}  // namespace

// GHSA-m4h8-2wjh-rrpq: the original trigger is a comp=1 frame whose content area is
// shorter than one STransCompMsg (8 bytes); the vulnerable code read pComp->contLen
// before checking the length, so the out-of-bounds read happened before lz4. The
// declared msgLen below equals the actual frame length on purpose: these packets must
// be rejected by the minimum compressed-header length check, not by the declared-length
// consistency hardening. Cover every 1..7 byte content area on both entry points.
TEST(TransCompressTest, TdSec2026045RejectsIncompleteCompressedHeader) {
  static const char content[] = "0123456";
  const int32_t     compHdrLen = (int32_t)sizeof(STransCompMsg);

  for (int32_t contLen = 1; contLen < compHdrLen; ++contLen) {
    SCOPED_TRACE(contLen);

    int32_t msgLen = (int32_t)(sizeof(STransMsgHead)) + contLen;
    char   *msg = (char *)taosMemoryCalloc(1, msgLen);
    ASSERT_NE(msg, nullptr);

    STransMsgHead *head = (STransMsgHead *)msg;
    head->version = TRANS_VER;
    head->comp = 1;
    head->msgLen = htonl(msgLen);  // declared length matches the actual frame
    memcpy(transContFromHead(head), content, contLen);

    assertRejected(msg, msgLen);

    taosMemoryFree(msg);
  }
}

// TD-SEC-2026-045 hardening (defense in depth): a declared msgLen that differs from the
// actual frame length is rejected in both directions. This consistency check is not the
// original GHSA-m4h8-2wjh-rrpq fix -- that advisory triggers with declared == actual when
// the content area is shorter than one STransCompMsg (covered by the incomplete-header
// cases below). Keeping both directions strict means no caller can derive the lz4 input
// boundary from an attacker-controlled header value.
TEST(TransCompressTest, TdSec2026045RejectsDeclaredLengthMismatch) {
  char *expected = nullptr;
  int32_t compMsgLen = 0;
  char *msg = buildCompressedPacket(kTestContLen, &expected, &compMsgLen);
  ASSERT_NE(msg, nullptr);

  STransMsgHead *head = (STransMsgHead *)msg;
  head->msgLen = htonl(compMsgLen + kTestContLen);  // lie: declared length exceeds the actual buffer

  assertRejected(msg, compMsgLen);

  taosMemoryFree(expected);
  taosMemoryFree(msg);
}

// TD-SEC-2026-045 regression: legit compressed traffic (declared == actual) still decompresses
// and round-trips the original content.
TEST(TransCompressTest, TdSec2026045RoundTripWithMatchingLength) {
  char *expected = nullptr;
  int32_t compMsgLen = 0;
  char *msg = buildCompressedPacket(kTestContLen, &expected, &compMsgLen);
  ASSERT_NE(msg, nullptr);

  ASSERT_NE(expected, nullptr);
  char *packet = msg;
  int32_t packetLen = compMsgLen;
  ASSERT_EQ(transDecompressMsg(&packet, &packetLen), 0);
  ASSERT_EQ(packetLen, (int32_t)(sizeof(STransMsgHead)) + kTestContLen);
  EXPECT_EQ(0, memcmp(transContFromHead((STransMsgHead *)packet), expected, kTestContLen));

  taosMemoryFree(packet);
  taosMemoryFree(expected);
}

// TD-SEC-2026-045: a declared msgLen shorter than the frame is rejected as well;
// callers always pass the exact length of one framed packet.
TEST(TransCompressTest, TdSec2026045RejectsShorterDeclaredLength) {
  char *expected = nullptr;
  int32_t compMsgLen = 0;
  char *msg = buildCompressedPacket(kTestContLen, &expected, &compMsgLen);
  ASSERT_NE(msg, nullptr);

  STransMsgHead *head = (STransMsgHead *)msg;
  head->msgLen = htonl(compMsgLen - 1);

  assertRejected(msg, compMsgLen);

  taosMemoryFree(expected);
  taosMemoryFree(msg);
}

// The Ext entry point keeps the caller's buffer. The client re-send path
// (cliConnMayAddUserInfo) passes the header-declared frame length -- the frame
// it holds was compressed in place and the request content length is stale --
// so cover its success path with the exact declared frame length as well.
TEST(TransCompressTest, TdSec2026045RoundTripExtWithMatchingLength) {
  char *expected = nullptr;
  int32_t compMsgLen = 0;
  char *msg = buildCompressedPacket(kTestContLen, &expected, &compMsgLen);
  ASSERT_NE(msg, nullptr);

  char *out = nullptr;
  int32_t outLen = 0;
  ASSERT_NE(expected, nullptr);
  ASSERT_EQ(transDecompressMsgExt(msg, compMsgLen, &out, &outLen), 0);
  ASSERT_NE(out, nullptr);
  EXPECT_EQ(outLen, (int32_t)(sizeof(STransMsgHead)) + kTestContLen);
  EXPECT_EQ(0, memcmp(transContFromHead((STransMsgHead *)out), expected, kTestContLen));

  taosMemoryFree(out);
  taosMemoryFree(expected);
  taosMemoryFree(msg);  // transDecompressMsgExt only borrows the input
}

// TD-SEC-2026-045: the re-send path (cliConnMayAddUserInfo) receives a frame that was
// compressed in place while the request content length stayed stale; driving the real
// client function with the stale length pins the contract that the header-declared
// frame length is the decompression boundary. testCliConnMayAddUserInfoStaleLen()
// lives in the test-only transportHeartbeatHarness.c translation unit.
extern "C" int32_t testCliConnMayAddUserInfoStaleLen(int32_t contLen);
TEST(TransCompressTest, TdSec2026045ResendPathWithStaleContLen) {
  EXPECT_EQ(testCliConnMayAddUserInfoStaleLen(kTestContLen), 1);
}

class TransCtxEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    ctx = (STransCtx *)taosMemoryCalloc(1, sizeof(STransCtx));
    transCtxInit(ctx);
    // TODO
  }
  virtual void TearDown() {
    transCtxCleanup(ctx);
    // formate
  }
  STransCtx *ctx;
};

int32_t cloneVal(void *src, void **dst) {
  int sz = (int)strlen((char *)src);
  *dst = taosMemoryCalloc(1, sz + 1);
  memcpy(*dst, src, sz);
  return 0;
}
// TEST_F(TransCtxEnv, mergeTest) {
//  int key = 1;
//  {
//    STransCtx *src = (STransCtx *)taosMemoryCalloc(1, sizeof(STransCtx));
//    transCtxInit(src);
//    {
//      STransCtxVal val1 = {NULL, NULL, (void (*)(const void *))taosMemoryFree};
//      val1.val = taosMemoryMalloc(12);
//
//      taosHashPut(src->args, &key, sizeof(key), &val1, sizeof(val1));
//      key++;
//    }
//    {
//      STransCtxVal val1 = {NULL, NULL, (void (*)(const void *))taosMemoryFree};
//      val1.val = taosMemoryMalloc(12);
//      taosHashPut(src->args, &key, sizeof(key), &val1, sizeof(val1));
//      key++;
//    }
//    transCtxMerge(ctx, src);
//    taosMemoryFree(src);
//  }
//  EXPECT_EQ(2, taosHashGetSize(ctx->args));
//  {
//    STransCtx *src = (STransCtx *)taosMemoryCalloc(1, sizeof(STransCtx));
//    transCtxInit(src);
//    {
//      STransCtxVal val1 = {NULL, NULL, (void (*)(const void *))taosMemoryFree};
//      val1.val = taosMemoryMalloc(12);
//
//      taosHashPut(src->args, &key, sizeof(key), &val1, sizeof(val1));
//      key++;
//    }
//    {
//      STransCtxVal val1 = {NULL, NULL, (void (*)(const void *))taosMemoryFree};
//      val1.val = taosMemoryMalloc(12);
//      taosHashPut(src->args, &key, sizeof(key), &val1, sizeof(val1));
//      key++;
//    }
//    transCtxMerge(ctx, src);
//    taosMemoryFree(src);
//  }
//  std::string val("Hello");
//  EXPECT_EQ(4, taosHashGetSize(ctx->args));
//  {
//    key = 1;
//    STransCtx *src = (STransCtx *)taosMemoryCalloc(1, sizeof(STransCtx));
//    transCtxInit(src);
//    {
//      STransCtxVal val1 = {NULL, NULL, (void (*)(const void *))taosMemoryFree};
//      val1.val = taosMemoryCalloc(1, 11);
//      val1.clone = cloneVal;
//      memcpy(val1.val, val.c_str(), val.size());
//
//      taosHashPut(src->args, &key, sizeof(key), &val1, sizeof(val1));
//      key++;
//    }
//    {
//      STransCtxVal val1 = {NULL, NULL, (void (*)(const void *))taosMemoryFree};
//      val1.val = taosMemoryCalloc(1, 11);
//      val1.clone = cloneVal;
//      memcpy(val1.val, val.c_str(), val.size());
//      taosHashPut(src->args, &key, sizeof(key), &val1, sizeof(val1));
//      key++;
//    }
//    transCtxMerge(ctx, src);
//    taosMemoryFree(src);
//  }
//  EXPECT_EQ(4, taosHashGetSize(ctx->args));
//
//  char *skey = (char *)transCtxDumpVal(ctx, 1);
//  EXPECT_EQ(0, strcmp(skey, val.c_str()));
//  taosMemoryFree(skey);
//
//  skey = (char *)transCtxDumpVal(ctx, 2);
//  EXPECT_EQ(0, strcmp(skey, val.c_str()));
//}
#endif

#ifndef TD_ASTRA_RPC
extern "C" int32_t runNativeHeartbeatScenario(int32_t, int32_t *, int32_t *, int32_t *, int64_t *);

class NativeHeartbeatTest : public testing::Test {
 protected:
  int32_t callbacks = 0;
  int32_t pending = 0;
  int32_t lastCode = 0;
  int64_t contextTime = 0;
  void    run(int32_t scenario) {
    ASSERT_EQ(runNativeHeartbeatScenario(scenario, &callbacks, &pending, &lastCode, &contextTime), 0);
  }
};
TEST_F(NativeHeartbeatTest, UnknownSequenceWithoutSessionIsRejected) {
  run(0);
  EXPECT_EQ(callbacks, 0);
  EXPECT_EQ(contextTime, 12345);
}
TEST_F(NativeHeartbeatTest, UnknownSequenceCannotUseRetainedContext) {
  run(1);
  EXPECT_EQ(callbacks, 0);
  EXPECT_EQ(contextTime, 12345);
}
TEST_F(NativeHeartbeatTest, ConsumedSequenceCannotUseRetainedContext) {
  run(2);
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(pending, 0);
}
TEST_F(NativeHeartbeatTest, WrongRequestTypeDoesNotConsumePendingOrContext) {
  run(3);
  EXPECT_EQ(callbacks, 0);
  EXPECT_EQ(pending, 1);
  EXPECT_EQ(contextTime, 12345);
}
TEST_F(NativeHeartbeatTest, WrongSequenceDoesNotConsumePendingOrContext) {
  run(4);
  EXPECT_EQ(callbacks, 0);
  EXPECT_EQ(pending, 1);
  EXPECT_EQ(contextTime, 12345);
}
TEST_F(NativeHeartbeatTest, MatchedResponseConsumesPendingOnce) {
  run(5);
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(pending, 0);
  EXPECT_EQ(lastCode, 0);
}
TEST_F(NativeHeartbeatTest, MatchedBodylessErrorStillNotifies) {
  run(6);
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(pending, 0);
  EXPECT_EQ(lastCode, TSDB_CODE_RPC_BROKEN_LINK);
}
TEST_F(NativeHeartbeatTest, OtherResponseKeepsContextFallback) {
  run(7);
  EXPECT_EQ(callbacks, 1);
}
#endif

#ifdef TD_ASTRA_RPC
extern "C" int32_t runAstraHeartbeatScenario(int32_t, int32_t *, int32_t *, int32_t *, int32_t *);

class AstraHeartbeatTest : public testing::Test {
 protected:
  int32_t callbacks = 0;
  int32_t pending = 0;
  int32_t lastCode = 0;
  int32_t bodyFrees = 0;
  void    run(int32_t scenario) {
    ASSERT_EQ(runAstraHeartbeatScenario(scenario, &callbacks, &pending, &lastCode, &bodyFrees), 0);
    EXPECT_EQ(bodyFrees, scenario == 6 ? 0 : (scenario == 2 ? 2 : 1));
  }
};
TEST_F(AstraHeartbeatTest, UnknownSequenceWithoutSessionIsRejected) {
  run(0);
  EXPECT_EQ(callbacks, 0);
}
TEST_F(AstraHeartbeatTest, UnknownSequenceCannotUseRetainedContext) {
  run(1);
  EXPECT_EQ(callbacks, 0);
}
TEST_F(AstraHeartbeatTest, ConsumedSequenceCannotUseRetainedContext) {
  run(2);
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(pending, 0);
}
TEST_F(AstraHeartbeatTest, WrongRequestTypeDoesNotConsumePendingOrContext) {
  run(3);
  EXPECT_EQ(callbacks, 0);
  EXPECT_EQ(pending, 1);
}
TEST_F(AstraHeartbeatTest, WrongSequenceDoesNotConsumePendingOrContext) {
  run(4);
  EXPECT_EQ(callbacks, 0);
  EXPECT_EQ(pending, 1);
}
TEST_F(AstraHeartbeatTest, MatchedResponseConsumesPendingOnce) {
  run(5);
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(pending, 0);
  EXPECT_EQ(lastCode, 0);
}
TEST_F(AstraHeartbeatTest, MatchedBodylessErrorStillNotifies) {
  run(6);
  EXPECT_EQ(callbacks, 1);
  EXPECT_EQ(pending, 0);
  EXPECT_EQ(lastCode, TSDB_CODE_RPC_BROKEN_LINK);
}
TEST_F(AstraHeartbeatTest, OtherResponseKeepsContextFallback) {
  run(7);
  EXPECT_EQ(callbacks, 1);
}
#endif

#ifndef TD_ASTRA_RPC
/*
 * TD-SEC-2026-001 regression tests.
 *
 * uvConnMayGetUserInfo() must reject any packet whose declared msgLen cannot
 * hold the rpc head plus the attached user info (or token), so that the body
 * length computation can never underflow into a giant unsigned value.
 * testUvConnMayGetUserInfo() lives in the test-only transSvrUserInfoHarness.c
 * translation unit (which includes transSvr.c) and drives the real check.
 */
extern "C" int32_t testUvConnMayGetUserInfo(int32_t msgLen, int8_t withUserInfo, int8_t isToken);

class UserInfoSecurityTest : public ::testing::Test {
 protected:
  static constexpr int32_t kHead = (int32_t)sizeof(STransMsgHead);
  static constexpr int32_t kUserOff = TSDB_UNI_LEN;  // sizeof(SSvrConn::user)
  static constexpr int32_t kTokenOff = 128;          // sizeof(SSvrConn::identifier)
};

TEST_F(UserInfoSecurityTest, ShortBody_User_10bytes) {
  EXPECT_EQ(testUvConnMayGetUserInfo(kHead + 10, 1, 0), 0);
}

TEST_F(UserInfoSecurityTest, ShortBody_Token_10bytes) {
  EXPECT_EQ(testUvConnMayGetUserInfo(kHead + 10, 1, 1), 0);
}

TEST_F(UserInfoSecurityTest, ZeroBody_User) {
  EXPECT_EQ(testUvConnMayGetUserInfo(kHead, 1, 0), 0);
}

TEST_F(UserInfoSecurityTest, BelowHead_User) {
  EXPECT_EQ(testUvConnMayGetUserInfo(kHead - 1, 1, 0), 0);
}

TEST_F(UserInfoSecurityTest, OffByOne_User) {
  EXPECT_EQ(testUvConnMayGetUserInfo(kHead + kUserOff - 1, 1, 0), 0);
}

TEST_F(UserInfoSecurityTest, OffByOne_Token) {
  EXPECT_EQ(testUvConnMayGetUserInfo(kHead + kTokenOff - 1, 1, 1), 0);
}

TEST_F(UserInfoSecurityTest, ExactSize_User) {
  EXPECT_EQ(testUvConnMayGetUserInfo(kHead + kUserOff, 1, 0), 1);
}

TEST_F(UserInfoSecurityTest, NormalPacket_User_WithPayload) {
  EXPECT_EQ(testUvConnMayGetUserInfo(kHead + kUserOff + 64, 1, 0), 1);
}

TEST_F(UserInfoSecurityTest, NormalPacket_Token_WithPayload) {
  EXPECT_EQ(testUvConnMayGetUserInfo(kHead + kTokenOff + 32, 1, 1), 1);
}

TEST_F(UserInfoSecurityTest, WithUserInfoNotSet) {
  EXPECT_EQ(testUvConnMayGetUserInfo(kHead + 1024, 0, 0), 0);
}

/*
 * Devon scan 7114722063 (engine-transport) regression tests.
 *
 * The entry points live in the test-only transCliSecHarness.c / thttpTelemSecHarness.c
 * translation units (which include the implementation files) and drive the real logic.
 */
extern "C" int32_t testCliTryUpdateEpsetShort(int32_t bodyLen, int canonicalLen);
extern "C" int32_t testCliReleaseRespGate(int64_t releaseQid, int64_t registeredQid);
extern "C" int32_t testTelemetryReportUrlBound(int32_t urlLen);

static int32_t devonCanonicalEpsetLen() {
  SEpSet epset = {0};
  epset.numOfEps = 1;
  return tSerializeSEpSet(NULL, 0, &epset);
}

TEST(TransDevonSecTest, TdItem324ValidReqEpsetBoundsInUseByNumOfEps) {
  SReqEpSet* p = (SReqEpSet*)taosMemoryCalloc(1, sizeof(SReqEpSet) + sizeof(SEp) * TSDB_MAX_REPLICA);
  ASSERT_NE(p, nullptr);

  // inUse indexing past (or before) the eps[] actually present must be rejected
  p->numOfEps = 1;
  p->inUse = 9;
  EXPECT_EQ(transValidReqEpset(p), TSDB_CODE_INVALID_PARA);
  p->inUse = -1;
  EXPECT_EQ(transValidReqEpset(p), TSDB_CODE_INVALID_PARA);

  // numOfEps beyond the fixed SEpSet source array must be rejected
  p->numOfEps = TSDB_MAX_REPLICA + 1;
  p->inUse = 0;
  EXPECT_EQ(transValidReqEpset(p), TSDB_CODE_INVALID_PARA);
  p->numOfEps = 0;
  EXPECT_EQ(transValidReqEpset(p), TSDB_CODE_INVALID_PARA);

  // well-formed sets pass
  p->numOfEps = 3;
  p->inUse = 2;
  EXPECT_EQ(transValidReqEpset(p), TSDB_CODE_SUCCESS);
  p->inUse = 0;
  EXPECT_EQ(transValidReqEpset(p), TSDB_CODE_SUCCESS);

  EXPECT_EQ(transValidReqEpset(NULL), TSDB_CODE_INVALID_PARA);
  taosMemoryFree(p);
}

TEST(TransDevonSecTest, TdItem195ShortEpsetBodyRejected) {
  int32_t canonical = devonCanonicalEpsetLen();
  ASSERT_GT(canonical, 0);
  // body shorter than the canonical serialized SEpSet must be rejected, not memcpy'd
  EXPECT_EQ(testCliTryUpdateEpsetShort(canonical - 31, canonical), 1);
  EXPECT_EQ(testCliTryUpdateEpsetShort(canonical - 1, canonical), 1);
  // a full-size body still goes through
  EXPECT_EQ(testCliTryUpdateEpsetShort(canonical, canonical), 2);
}

TEST(TransDevonSecTest, TdItem290TelemetryReportUrlBounded) {
  // value longer than the 257-byte destination must be rejected without writing to it
  EXPECT_EQ(testTelemetryReportUrlBound(300), 1);
  EXPECT_EQ(testTelemetryReportUrlBound(1000), 1);
  // a fitting value is copied normally
  EXPECT_EQ(testTelemetryReportUrlBound(64), 1);
}

TEST(TransDevonSecTest, TdItem351ReleaseRespUnknownQidIgnored) {
  // release frame for a qId not registered on this connection must not remove
  // the registered entry
  EXPECT_EQ(testCliReleaseRespGate(8, 7), 1);
  EXPECT_EQ(testCliReleaseRespGate(-1, 7), 1);
}

// ---------------------------------------------------------------------------------------------
// COMMIT / ROLLBACK refused with TRANS_CONFLICT must be resent by the RPC layer (with
// backoff) once the client's retry hook asks for it; other messages must not be.
// Own server returning the error a few times, own client carrying a retry hook shaped like the
// one the real client registers (the real one is static in clientMain.c and not reachable here).
// ---------------------------------------------------------------------------------------------
namespace {
const int32_t kConflictTestPort = 7100;

std::atomic<int32_t> gConflictReqCount(0);
std::atomic<int32_t> gConflictFailTimes(0);
std::atomic<int32_t> gConflictRespCode(-1);
tsem_t               gConflictSem;

void conflictServerProcess(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  int32_t n = ++gConflictReqCount;
  SRpcMsg rsp = {0};
  rsp.info = pMsg->info;
  rsp.code = (n <= gConflictFailTimes.load()) ? TSDB_CODE_MND_TRANS_CONFLICT : 0;
  rpcFreeCont(pMsg->pCont);
  rpcSendResponse(&rsp);
}

void conflictClientProcess(void *parent, SRpcMsg *pMsg, SEpSet *pEpSet) {
  gConflictRespCode = pMsg->code;
  rpcFreeCont(pMsg->pCont);
  (void)tsem_post(&gConflictSem);
}

// Must match the TRANS_CONFLICT branch of instanceRegisterRpcRfp's retry hook in clientMain.c.
bool conflictRetryHook(int32_t code, tmsg_t msgType) {
  return code == TSDB_CODE_MND_TRANS_CONFLICT &&
         (msgType == TDMT_MND_COMMIT_TXN || msgType == TDMT_MND_ROLLBACK_TXN);
}

int32_t SendOnce(tmsg_t msgType, int32_t failTimes, int32_t *pReqCount) {
  gConflictReqCount = 0;
  gConflictFailTimes = failTimes;
  gConflictRespCode = -1;
  *pReqCount = 0;
  EXPECT_EQ(tsem_init(&gConflictSem, 0, 0), 0);

  SRpcInit srvInit = {0};
  memcpy(srvInit.localFqdn, "localhost", strlen("localhost"));
  srvInit.localPort = kConflictTestPort;
  srvInit.label = (char *)"conflictSrv";
  srvInit.numOfThreads = 1;
  srvInit.cfp = conflictServerProcess;
  srvInit.user = (char *)"user";
  srvInit.connType = TAOS_CONN_SERVER;
  taosVersionStrToInt(td_version, &(srvInit.compatibilityVer));
  void *srv = rpcOpen(&srvInit);
  if (srv == nullptr) {
    ADD_FAILURE() << "failed to open conflict test server";
    (void)tsem_destroy(&gConflictSem);
    return -1;
  }
  taosMsleep(500);

  SRpcInit cliInit = {0};
  cliInit.localPort = 0;
  cliInit.label = (char *)"conflictCli";
  cliInit.numOfThreads = 1;
  cliInit.cfp = conflictClientProcess;
  cliInit.user = (char *)"user";
  cliInit.connType = TAOS_CONN_CLIENT;
  cliInit.shareConnLimit = 10;
  cliInit.rfp = conflictRetryHook;
  cliInit.retryMinInterval = 10;
  cliInit.retryStepFactor = 2;
  cliInit.retryMaxInterval = 100;
  cliInit.retryMaxTimeout = 10000;
  taosVersionStrToInt(td_version, &(cliInit.compatibilityVer));
  void *cli = rpcOpen(&cliInit);
  if (cli == nullptr) {
    ADD_FAILURE() << "failed to open conflict test client";
    rpcClose(srv);
    (void)tsem_destroy(&gConflictSem);
    return -1;
  }

  SEpSet epSet = {0};
  epSet.inUse = 0;
  addEpIntoEpSet(&epSet, "127.0.0.1", kConflictTestPort);
  SRpcMsg req = {0};
  req.msgType = msgType;
  req.pCont = rpcMallocCont(10);
  req.contLen = 10;
  EXPECT_EQ(rpcSendRequest(cli, &epSet, &req, NULL), 0);
  // Bounded wait: if the retry hook regresses and no final response arrives, fail instead of
  // hanging the runner. retryMaxTimeout plus margin.
  EXPECT_EQ(tsem_timewait(&gConflictSem, cliInit.retryMaxTimeout + 5000), 0);

  rpcClose(cli);
  rpcClose(srv);
  (void)tsem_destroy(&gConflictSem);
  *pReqCount = gConflictReqCount.load();
  return gConflictRespCode.load();
}
}  // namespace

TEST(TransConflictRetry, CommitIsResentUntilTheConflictClears) {
  int32_t reqCount = 0;
  EXPECT_EQ(SendOnce(TDMT_MND_COMMIT_TXN, 3, &reqCount), 0);
  EXPECT_EQ(reqCount, 4);
}

TEST(TransConflictRetry, RollbackIsResentUntilTheConflictClears) {
  int32_t reqCount = 0;
  EXPECT_EQ(SendOnce(TDMT_MND_ROLLBACK_TXN, 2, &reqCount), 0);
  EXPECT_EQ(reqCount, 3);
}

TEST(TransConflictRetry, OtherMessagesAreNotResent) {
  int32_t reqCount = 0;
  EXPECT_EQ(SendOnce(TDMT_MND_CREATE_DB, 3, &reqCount), TSDB_CODE_MND_TRANS_CONFLICT);
  EXPECT_EQ(reqCount, 1);
}
// ---------------------------------------------------------------------------
// transAsyncSendToIdx / transAsyncSendWithIdx: the task-release affinity fix
// relies on (a) the round-robin helper reporting the slot it used, (b) sending
// to an explicit slot without advancing the round-robin counter, and (c) the
// loop draining slot 0 before slot 7 for messages queued in the same pass.
// ---------------------------------------------------------------------------
namespace {

struct AsyncSlotMsg {
  queue q;
  int   val;
};

struct AsyncSlotProbe {
  SAsyncPool*            pool = nullptr;
  vector<pair<int, int>> order;  // (slot, val)
};

AsyncSlotProbe gAsyncProbe;

void asyncSlotTestCb(uv_async_t* handle) {
  SAsyncItem* item = (SAsyncItem*)handle->data;
  int         slot = (int)(handle - gAsyncProbe.pool->asyncs);

  queue tmp;
  QUEUE_INIT(&tmp);

  taosThreadMutexLock(&item->mtx);
  QUEUE_MOVE(&item->qmsg, &tmp);
  taosThreadMutexUnlock(&item->mtx);

  while (!QUEUE_IS_EMPTY(&tmp)) {
    queue* h = QUEUE_HEAD(&tmp);
    QUEUE_REMOVE(h);
    AsyncSlotMsg* m = QUEUE_DATA(h, AsyncSlotMsg, q);
    gAsyncProbe.order.push_back(make_pair(slot, m->val));
    taosMemoryFree(m);
  }
}

void asyncSlotCloseCb(uv_handle_t* handle, void* arg) {
  if (!uv_is_closing(handle)) {
    uv_close(handle, NULL);
  }
}

// The async handles stay active for the lifetime of the pool, so UV_RUN_DEFAULT would never
// return; poll the loop until the expected number of messages has been drained.
bool runLoopUntilDrained(uv_loop_t* loop, size_t expected, int timeoutMs = 3000) {
  auto deadline = chrono::steady_clock::now() + chrono::milliseconds(timeoutMs);
  while (gAsyncProbe.order.size() < expected) {
    uv_run(loop, UV_RUN_NOWAIT);
    if (chrono::steady_clock::now() > deadline) {
      return false;
    }
    this_thread::sleep_for(chrono::milliseconds(1));
  }
  return true;
}

AsyncSlotMsg* newAsyncSlotMsg(int val) {
  AsyncSlotMsg* m = (AsyncSlotMsg*)taosMemoryCalloc(1, sizeof(AsyncSlotMsg));
  EXPECT_NE(nullptr, m);
  if (m != nullptr) {
    QUEUE_INIT(&m->q);
    m->val = val;
  }
  return m;
}

class AsyncSlotEnv : public ::testing::Test {
 protected:
  void SetUp() override {
    ASSERT_EQ(0, uv_loop_init(&loop));
    ASSERT_EQ(0, transAsyncPoolCreate(&loop, 8, NULL, asyncSlotTestCb, &pool));
    ASSERT_NE(nullptr, pool);
    gAsyncProbe.pool = pool;
    gAsyncProbe.order.clear();
  }

  void TearDown() override {
    uv_walk(&loop, asyncSlotCloseCb, NULL);
    uv_run(&loop, UV_RUN_DEFAULT);
    uv_loop_close(&loop);
    transAsyncPoolDestroy(pool);
    pool = NULL;
    gAsyncProbe.pool = NULL;
  }

  uv_loop_t   loop;
  SAsyncPool* pool = nullptr;
};

TEST_F(AsyncSlotEnv, sendWithIdxReportsTheUsedSlot) {
  pool->index = 5;

  AsyncSlotMsg* m = newAsyncSlotMsg(1);
  ASSERT_NE(nullptr, m);

  int32_t idx = -1;
  ASSERT_EQ(0, transAsyncSendWithIdx(pool, &m->q, &idx));
  EXPECT_EQ(5, idx);
  EXPECT_EQ(6, pool->index);

  ASSERT_TRUE(runLoopUntilDrained(&loop, 1));
  ASSERT_EQ(1u, gAsyncProbe.order.size());
  EXPECT_EQ(make_pair(5, 1), gAsyncProbe.order[0]);
}

TEST_F(AsyncSlotEnv, toIdxDoesNotAdvanceIndexAndKeepsSameSlotOrder) {
  pool->index = 7;

  AsyncSlotMsg* first = newAsyncSlotMsg(1);
  ASSERT_NE(nullptr, first);
  int32_t idx = -1;
  ASSERT_EQ(0, transAsyncSendWithIdx(pool, &first->q, &idx));
  ASSERT_EQ(7, idx);
  int32_t afterFirst = pool->index;
  EXPECT_EQ(8, afterFirst);

  AsyncSlotMsg* second = newAsyncSlotMsg(2);
  ASSERT_NE(nullptr, second);
  ASSERT_EQ(0, transAsyncSendToIdx(pool, idx, &second->q));
  EXPECT_EQ(afterFirst, pool->index);  // explicit slot send must not touch the counter

  ASSERT_TRUE(runLoopUntilDrained(&loop, 2));
  ASSERT_EQ(2u, gAsyncProbe.order.size());
  EXPECT_EQ(make_pair(7, 1), gAsyncProbe.order[0]);
  EXPECT_EQ(make_pair(7, 2), gAsyncProbe.order[1]);
}

TEST_F(AsyncSlotEnv, lowerSlotIsDrainedBeforeHigherSlot) {
  AsyncSlotMsg* high = newAsyncSlotMsg(7);
  ASSERT_NE(nullptr, high);
  ASSERT_EQ(0, transAsyncSendToIdx(pool, 7, &high->q));

  AsyncSlotMsg* low = newAsyncSlotMsg(0);
  ASSERT_NE(nullptr, low);
  ASSERT_EQ(0, transAsyncSendToIdx(pool, 0, &low->q));

  ASSERT_TRUE(runLoopUntilDrained(&loop, 2));
  ASSERT_EQ(2u, gAsyncProbe.order.size());
  EXPECT_EQ(make_pair(0, 0), gAsyncProbe.order[0]);
  EXPECT_EQ(make_pair(7, 7), gAsyncProbe.order[1]);
}

TEST_F(AsyncSlotEnv, invalidSlotIsRejected) {
  AsyncSlotMsg* m = newAsyncSlotMsg(1);
  ASSERT_NE(nullptr, m);

  EXPECT_EQ(TSDB_CODE_INVALID_PARA, transAsyncSendToIdx(pool, 8, &m->q));
  EXPECT_EQ(TSDB_CODE_INVALID_PARA, transAsyncSendToIdx(pool, -1, &m->q));

  taosMemoryFree(m);
  uv_run(&loop, UV_RUN_NOWAIT);
  EXPECT_TRUE(gAsyncProbe.order.empty());
}

}  // namespace
#endif
