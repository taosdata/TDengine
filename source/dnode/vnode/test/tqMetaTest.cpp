#include <gtest/gtest.h>
#include <cstring>
#include <iostream>
#include <queue>

#include "tqMetaStore.h"

struct Foo {
  int32_t a;
};

int FooSerializer(const void* pObj, STqSerializedHead** ppHead) {
  Foo* foo = (Foo*)pObj;
  if ((*ppHead) == NULL || (*ppHead)->ssize < sizeof(STqSerializedHead) + sizeof(int32_t)) {
    *ppHead = (STqSerializedHead*)taosMemoryRealloc(*ppHead, sizeof(STqSerializedHead) + sizeof(int32_t));
    (*ppHead)->ssize = sizeof(STqSerializedHead) + sizeof(int32_t);
  }
  *(int32_t*)(*ppHead)->content = foo->a;
  return (*ppHead)->ssize;
}

const void* FooDeserializer(const STqSerializedHead* pHead, void** ppObj) {
  if (*ppObj == NULL) {
    *ppObj = taosMemoryRealloc(*ppObj, sizeof(int32_t));
  }
  Foo* pFoo = *(Foo**)ppObj;
  pFoo->a = *(int32_t*)pHead->content;
  return NULL;
}

void FooDeleter(void* pObj) { taosMemoryFree(pObj); }

class TqMetaUpdateAppendTest : public ::testing::Test {
 protected:
  void SetUp() override {
    taosRemoveDir(pathName);
    pMeta = tqStoreOpen(pathName, FooSerializer, FooDeserializer, FooDeleter, TQ_UPDATE_APPEND);
    ASSERT(pMeta);
  }

  void TearDown() override { tqStoreClose(pMeta); }

  STqMetaStore* pMeta;
  const char*   pathName = "/tmp/tq_test";
};

TEST_F(TqMetaUpdateAppendTest, copyPutTest) {
  Foo foo;
  foo.a = 3;
  tqHandleCopyPut(pMeta, 1, &foo, sizeof(Foo));

  Foo* pFoo = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);

  tqHandleCommit(pMeta, 1);
  pFoo = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo->a, 3);
}

TEST_F(TqMetaUpdateAppendTest, persistTest) {
  Foo* pFoo = (Foo*)taosMemoryMalloc(sizeof(Foo));
  pFoo->a = 2;
  tqHandleMovePut(pMeta, 1, pFoo);
  Foo* pBar = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pBar == NULL, true);
  tqHandleCommit(pMeta, 1);
  pBar = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pBar->a, pFoo->a);
  pBar = (Foo*)tqHandleGet(pMeta, 2);
  EXPECT_EQ(pBar == NULL, true);

  tqStoreClose(pMeta);
  pMeta = tqStoreOpen(pathName, FooSerializer, FooDeserializer, FooDeleter, TQ_UPDATE_APPEND);
  ASSERT(pMeta);

  pBar = (Foo*)tqHandleGet(pMeta, 1);
  ASSERT_EQ(pBar != NULL, true);
  EXPECT_EQ(pBar->a, 2);

  pBar = (Foo*)tqHandleGet(pMeta, 2);
  EXPECT_EQ(pBar == NULL, true);
}

TEST_F(TqMetaUpdateAppendTest, uncommittedTest) {
  Foo* pFoo = (Foo*)taosMemoryMalloc(sizeof(Foo));
  pFoo->a = 3;
  tqHandleMovePut(pMeta, 1, pFoo);

  pFoo = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);
}

TEST_F(TqMetaUpdateAppendTest, abortTest) {
  Foo* pFoo = (Foo*)taosMemoryMalloc(sizeof(Foo));
  pFoo->a = 3;
  tqHandleMovePut(pMeta, 1, pFoo);

  pFoo = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);

  tqHandleAbort(pMeta, 1);
  pFoo = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);
}

TEST_F(TqMetaUpdateAppendTest, deleteTest) {
  Foo* pFoo = (Foo*)taosMemoryMalloc(sizeof(Foo));
  pFoo->a = 3;
  tqHandleMovePut(pMeta, 1, pFoo);

  pFoo = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);

  tqHandleCommit(pMeta, 1);

  pFoo = (Foo*)tqHandleGet(pMeta, 1);
  ASSERT_EQ(pFoo != NULL, true);
  EXPECT_EQ(pFoo->a, 3);

  tqHandleDel(pMeta, 1);
  pFoo = (Foo*)tqHandleGet(pMeta, 1);
  ASSERT_EQ(pFoo != NULL, true);
  EXPECT_EQ(pFoo->a, 3);

  tqHandleCommit(pMeta, 1);
  pFoo = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);

  tqStoreClose(pMeta);
  pMeta = tqStoreOpen(pathName, FooSerializer, FooDeserializer, FooDeleter, TQ_UPDATE_APPEND);
  ASSERT(pMeta);

  pFoo = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);
}

TEST_F(TqMetaUpdateAppendTest, intxnPersist) {
  Foo* pFoo = (Foo*)taosMemoryMalloc(sizeof(Foo));
  pFoo->a = 3;
  tqHandleMovePut(pMeta, 1, pFoo);
  tqHandleCommit(pMeta, 1);

  Foo* pBar = (Foo*)taosMemoryMalloc(sizeof(Foo));
  pBar->a = 4;
  tqHandleMovePut(pMeta, 1, pBar);

  Foo* pFoo1 = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo1->a, 3);

  tqStoreClose(pMeta);
  pMeta = tqStoreOpen(pathName, FooSerializer, FooDeserializer, FooDeleter, TQ_UPDATE_APPEND);
  ASSERT(pMeta);

  pFoo1 = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo1->a, 3);

  tqHandleCommit(pMeta, 1);

  pFoo1 = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo1->a, 4);

  tqStoreClose(pMeta);
  pMeta = tqStoreOpen(pathName, FooSerializer, FooDeserializer, FooDeleter, TQ_UPDATE_APPEND);
  ASSERT(pMeta);

  pFoo1 = (Foo*)tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo1->a, 4);
}

TEST_F(TqMetaUpdateAppendTest, multiplePage) {
  taosSeedRand(0);
  std::vector<int> v;
  for (int i = 0; i < 1000; i++) {
    v.push_back(taosRand());
    Foo foo;
    foo.a = v[i];
    tqHandleCopyPut(pMeta, i, &foo, sizeof(Foo));
  }
  for (int i = 0; i < 500; i++) {
    tqHandleCommit(pMeta, i);
    Foo* pFoo = (Foo*)tqHandleGet(pMeta, i);
    ASSERT_EQ(pFoo != NULL, true) << " at idx " << i << "\n";
    EXPECT_EQ(pFoo->a, v[i]);
  }

  tqStoreClose(pMeta);
  pMeta = tqStoreOpen(pathName, FooSerializer, FooDeserializer, FooDeleter, TQ_UPDATE_APPEND);
  ASSERT(pMeta);

  for (int i = 500; i < 1000; i++) {
    tqHandleCommit(pMeta, i);
    Foo* pFoo = (Foo*)tqHandleGet(pMeta, i);
    ASSERT_EQ(pFoo != NULL, true) << " at idx " << i << "\n";
    EXPECT_EQ(pFoo->a, v[i]);
  }

  for (int i = 0; i < 1000; i++) {
    Foo* pFoo = (Foo*)tqHandleGet(pMeta, i);
    ASSERT_EQ(pFoo != NULL, true) << " at idx " << i << "\n";
    EXPECT_EQ(pFoo->a, v[i]);
  }
}

TEST_F(TqMetaUpdateAppendTest, multipleRewrite) {
  taosSeedRand(0);
  std::vector<int> v;
  for (int i = 0; i < 1000; i++) {
    v.push_back(taosRand());
    Foo foo;
    foo.a = v[i];
    tqHandleCopyPut(pMeta, i, &foo, sizeof(Foo));
  }

  for (int i = 0; i < 500; i++) {
    tqHandleCommit(pMeta, i);
    v[i] = taosRand();
    Foo foo;
    foo.a = v[i];
    tqHandleCopyPut(pMeta, i, &foo, sizeof(Foo));
  }

  for (int i = 500; i < 1000; i++) {
    v[i] = taosRand();
    Foo foo;
    foo.a = v[i];
    tqHandleCopyPut(pMeta, i, &foo, sizeof(Foo));
  }

  for (int i = 0; i < 1000; i++) {
    tqHandleCommit(pMeta, i);
  }

  tqStoreClose(pMeta);
  pMeta = tqStoreOpen(pathName, FooSerializer, FooDeserializer, FooDeleter, TQ_UPDATE_APPEND);
  ASSERT(pMeta);

  for (int i = 500; i < 1000; i++) {
    v[i] = taosRand();
    Foo foo;
    foo.a = v[i];
    tqHandleCopyPut(pMeta, i, &foo, sizeof(Foo));
    tqHandleCommit(pMeta, i);
  }

  for (int i = 0; i < 1000; i++) {
    Foo* pFoo = (Foo*)tqHandleGet(pMeta, i);
    ASSERT_EQ(pFoo != NULL, true) << " at idx " << i << "\n";
    EXPECT_EQ(pFoo->a, v[i]);
  }
}

TEST_F(TqMetaUpdateAppendTest, dupCommit) {
  taosSeedRand(0);
  std::vector<int> v;
  for (int i = 0; i < 1000; i++) {
    v.push_back(taosRand());
    Foo foo;
    foo.a = v[i];
    tqHandleCopyPut(pMeta, i, &foo, sizeof(Foo));
  }

  for (int i = 0; i < 1000; i++) {
    int ret = tqHandleCommit(pMeta, i);
    EXPECT_EQ(ret, 0);
    ret = tqHandleCommit(pMeta, i);
    EXPECT_EQ(ret, -1);
  }

  for (int i = 0; i < 1000; i++) {
    int ret = tqHandleCommit(pMeta, i);
    EXPECT_EQ(ret, -1);
  }

  for (int i = 0; i < 1000; i++) {
    Foo* pFoo = (Foo*)tqHandleGet(pMeta, i);
    ASSERT_EQ(pFoo != NULL, true) << " at idx " << i << "\n";
    EXPECT_EQ(pFoo->a, v[i]);
  }
}
