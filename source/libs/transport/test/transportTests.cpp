/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3 * or later ("AGPL"), as published by the Free
 * Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifdef USE_UV

#include <gtest/gtest.h>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "transComm.h"
#include "transportInt.h"
#include "trpc.h"

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
  assert(q->IsEmpty());

  for (int i = 0; i < 100; i++) {
    QueueElem *el = (QueueElem *)taosMemoryMalloc(sizeof(QueueElem));
    el->val = i;
    q->Push(el);
  }
  int i = 0;
  while (!q->IsEmpty()) {
    QueueElem *el = q->Pop();
    assert(el->val == i++);
    taosMemoryFree(el);
  }
  assert(q->IsEmpty());
}
TEST_F(QueueEnv, testRm) {
  // add more test

  std::vector<QueueElem *> set;
  assert(q->IsEmpty());

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
  assert(q->IsEmpty());
}
TEST_F(QueueEnv, testIter) {
  // add more test
  assert(q->IsEmpty());
  std::vector<int> vals;
  for (int i = 0; i < 100; i++) {
    QueueElem *el = (QueueElem *)taosMemoryMalloc(sizeof(QueueElem));
    el->val = i;
    q->Push(el);
    vals.push_back(i);
  }
  std::vector<int> result;
  q->ForEach(result);
  assert(result.size() == vals.size());
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
