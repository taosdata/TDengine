#include <gtest/gtest.h>
#include <cstring>
#include <iostream>
#include <queue>

#include "tqMetaStore.h"

struct Foo {
  int32_t a;
};

int FooSerializer(const void* pObj, TqSerializedHead** ppHead) {
  Foo* foo = (Foo*) pObj;
  if((*ppHead) == NULL || (*ppHead)->ssize < sizeof(TqSerializedHead) + sizeof(int32_t)) {
    *ppHead = (TqSerializedHead*)realloc(*ppHead, sizeof(TqSerializedHead) + sizeof(int32_t));
    (*ppHead)->ssize = sizeof(TqSerializedHead) + sizeof(int32_t);
  }
  *(int32_t*)(*ppHead)->content = foo->a;
  return (*ppHead)->ssize;
}

const void* FooDeserializer(const TqSerializedHead* pHead, void** ppObj) {
  if(*ppObj == NULL) {
    *ppObj = realloc(*ppObj, sizeof(int32_t));
  }
  Foo* pFoo = *(Foo**)ppObj;
  pFoo->a = *(int32_t*)pHead->content; 
  return NULL;
}

void FooDeleter(void* pObj) {
  free(pObj); 
}

class TqMetaTest : public ::testing::Test {
  protected:

    void SetUp() override {
      taosRemoveDir(pathName);
      pMeta = tqStoreOpen(pathName,
          FooSerializer, FooDeserializer, FooDeleter);
      ASSERT(pMeta);
    }

    void TearDown() override {
      tqStoreClose(pMeta);
    }

    TqMetaStore* pMeta;
    const char* pathName = "/tmp/tq_test";
};

TEST_F(TqMetaTest, copyPutTest) {
  Foo foo;
  foo.a = 3;
  tqHandleCopyPut(pMeta, 1, &foo, sizeof(Foo));

  Foo* pFoo = (Foo*) tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);
}

TEST_F(TqMetaTest, persistTest) {
  Foo* pFoo = (Foo*)malloc(sizeof(Foo));
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
  pMeta = tqStoreOpen(pathName,
      FooSerializer, FooDeserializer, FooDeleter);
  ASSERT(pMeta);

  pBar = (Foo*)tqHandleGet(pMeta, 1);
  ASSERT_EQ(pBar != NULL, true);
  EXPECT_EQ(pBar->a, 2);

  pBar = (Foo*)tqHandleGet(pMeta, 2);
  EXPECT_EQ(pBar == NULL, true);

  //taosRemoveDir(pathName);
}

TEST_F(TqMetaTest, uncommittedTest) {
  Foo* pFoo = (Foo*)malloc(sizeof(Foo));
  pFoo->a = 3;
  tqHandleMovePut(pMeta, 1, pFoo);

  pFoo = (Foo*) tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);
}

TEST_F(TqMetaTest, abortTest) {
  Foo* pFoo = (Foo*)malloc(sizeof(Foo));
  pFoo->a = 3;
  tqHandleMovePut(pMeta, 1, pFoo);

  pFoo = (Foo*) tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);

  tqHandleAbort(pMeta, 1);
  pFoo = (Foo*) tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);
}

TEST_F(TqMetaTest, deleteTest) {
  Foo* pFoo = (Foo*)malloc(sizeof(Foo));
  pFoo->a = 3;
  tqHandleMovePut(pMeta, 1, pFoo);

  pFoo = (Foo*) tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);

  tqHandleCommit(pMeta, 1);

  pFoo = (Foo*) tqHandleGet(pMeta, 1);
  ASSERT_EQ(pFoo != NULL, true);
  EXPECT_EQ(pFoo->a, 3);

  tqHandleDel(pMeta, 1);
  pFoo = (Foo*) tqHandleGet(pMeta, 1);
  ASSERT_EQ(pFoo != NULL, true);
  EXPECT_EQ(pFoo->a, 3);

  tqHandleCommit(pMeta, 1);
  pFoo = (Foo*) tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);
}
