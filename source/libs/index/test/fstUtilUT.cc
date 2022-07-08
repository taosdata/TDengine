
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
  virtual void  SetUp() { set = sparSetCreate(256); }
  virtual void  TearDown() { sparSetDestroy(set); }
  FstSparseSet *set;
};

// test FstDfaBuilder
TEST_F(FstUtilEnv, test1) {}
TEST_F(FstUtilEnv, test2) {}
TEST_F(FstUtilEnv, test3) {}
TEST_F(FstUtilEnv, test4) {}

// test FstRegex

TEST_F(FstRegexEnv, test1) {}
TEST_F(FstRegexEnv, test2) {}
TEST_F(FstRegexEnv, test3) {}
TEST_F(FstRegexEnv, test4) {}

// test FstSparseSet
TEST_F(FstSparseSetEnv, test1) {}
TEST_F(FstSparseSetEnv, test2) {}
TEST_F(FstSparseSetEnv, test3) {}
TEST_F(FstSparseSetEnv, test4) {}
