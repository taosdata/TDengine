#include <gtest/gtest.h>
#include <cstring>
#include <iostream>
#include <queue>

#include "tqMetaStore.h"

struct Foo {
  int32_t a;
};

int FooSerializer(const void* pObj, void** ppBytes) {
  Foo* foo = (Foo*) pObj;
  *ppBytes = realloc(*ppBytes, sizeof(int32_t));
  **(int32_t**)ppBytes = foo->a;
  return sizeof(int32_t);
}

const void* FooDeserializer(const void* pBytes, void** ppObj) {
  if(*ppObj == NULL) {
    *ppObj = realloc(*ppObj, sizeof(int32_t));
  }
  Foo* pFoo = *(Foo**)ppObj;
  pFoo->a = *(int32_t*)pBytes; 
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

TEST_F(TqMetaTest, persistTest) {
  Foo* pFoo = (Foo*)malloc(sizeof(Foo));
  pFoo->a = 2;
  tqHandlePut(pMeta, 1, pFoo);
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
  tqHandlePut(pMeta, 1, pFoo);

  pFoo = (Foo*) tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);
}

TEST_F(TqMetaTest, abortTest) {
  Foo* pFoo = (Foo*)malloc(sizeof(Foo));
  pFoo->a = 3;
  tqHandlePut(pMeta, 1, pFoo);

  pFoo = (Foo*) tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);

  tqHandleAbort(pMeta, 1);
  pFoo = (Foo*) tqHandleGet(pMeta, 1);
  EXPECT_EQ(pFoo == NULL, true);
}
