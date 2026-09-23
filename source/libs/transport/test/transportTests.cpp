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
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "transComm.h"
#include "transportInt.h"
#include "trpc.h"

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
