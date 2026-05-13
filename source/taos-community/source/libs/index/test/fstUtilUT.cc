
#include <gtest/gtest.h>
#include <algorithm>
#include <iostream>
#include <string>
#include <thread>
#include <vector>
#include "index.h"
#include "indexCache.h"
#include "indexFst.h"
#include "indexFstDfa.h"
#include "indexFstRegex.h"
#include "indexFstSparse.h"
#include "indexFstUtil.h"
#include "indexInt.h"
#include "indexTfile.h"
#include "tglobal.h"
#include "tlog.h"
#include "tskiplist.h"
#include "tutil.h"
class FstUtilEnv : public ::testing::Test {
 protected:
  virtual void SetUp() {
    SArray *inst = taosArrayInit(4, sizeof(char));
    builder = dfaBuilderCreate(inst);
  }
  virtual void TearDown() { dfaBuilderDestroy(builder); }

  FstDfaBuilder *builder;
};

class FstRegexEnv : public ::testing::Test {
 protected:
  virtual void SetUp() { regex = regexCreate("test"); }
  virtual void TearDown() { regexDestroy(regex); }
  FstRegex    *regex;
};

class FstSparseSetEnv : public ::testing::Test {
 protected:
  virtual void SetUp() { set = sparSetCreate(256); }
  virtual void TearDown() {
    // tear down
    sparSetDestroy(set);
  }
  void ReBuild(int32_t sz) {
    sparSetDestroy(set);
    set = sparSetCreate(sz);
  }
  FstSparseSet *set;
};

// test FstDfaBuilder
TEST_F(FstUtilEnv, test1) {
  // test
}
TEST_F(FstUtilEnv, test2) {
  // test
}
TEST_F(FstUtilEnv, test3) {
  // test
}
TEST_F(FstUtilEnv, test4) {
  // test
}

// test FstRegex

TEST_F(FstRegexEnv, test1) {
  //
  EXPECT_EQ(regex != NULL, true);
}
TEST_F(FstRegexEnv, test2) {}
TEST_F(FstRegexEnv, test3) {}
TEST_F(FstRegexEnv, test4) {}

// test FstSparseSet
TEST_F(FstSparseSetEnv, test1) {
  for (int8_t i = 0; i < 20; i++) {
    int32_t val = -1;
    bool    succ = sparSetAdd(set, 'a' + i, &val);
  }
  EXPECT_EQ(sparSetLen(set), 20);
  for (int8_t i = 0; i < 20; i++) {
    int  val = -1;
    bool find = sparSetGet(set, i, &val);
    EXPECT_EQ(find, true);
    EXPECT_EQ(val, i + 'a');
  }
  for (int8_t i = 'a'; i < 'a' + 20; i++) {
    EXPECT_EQ(sparSetContains(set, i), true);
  }

  for (int8_t i = 'A'; i < 20; i++) {
    EXPECT_EQ(sparSetContains(set, 'A'), false);
  }

  for (int i = 512; i < 1000; i++) {
    EXPECT_EQ(sparSetAdd(set, i, NULL), false);

    EXPECT_EQ(sparSetGet(set, i, NULL), false);
    EXPECT_EQ(sparSetContains(set, i), false);
  }
  sparSetClear(set);

  for (int i = 'a'; i < 'a' + 20; i++) {
    EXPECT_EQ(sparSetGet(set, i, NULL), false);
  }
  for (int i = 1000; i < 2000; i++) {
    EXPECT_EQ(sparSetGet(set, i, NULL), false);
  }
}
