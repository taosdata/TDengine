#include <gtest/gtest.h>
#include <sys/time.h>
#include <cassert>
#include <iostream>

#include "qAggMain.h"
#include "tcompare.h"

TEST(testCase, patternMatchTest) {
  SPatternCompareInfo info = PATTERN_COMPARE_INFO_INITIALIZER;

  const char* str = "abcdef";
  int32_t ret = patternMatch("a%b%", str, strlen(str), &info);
  EXPECT_EQ(ret, TSDB_PATTERN_MATCH);

  str = "tm01";
  ret = patternMatch("tm__", str, strlen(str), &info);
  EXPECT_EQ(ret, TSDB_PATTERN_MATCH);

  str = "tkm1";
  ret = patternMatch("t%m1", str, strlen(str), &info);
  EXPECT_EQ(ret, TSDB_PATTERN_MATCH);

  str = "tkm1";
  ret = patternMatch("%m1", str, strlen(str), &info);
  EXPECT_EQ(ret, TSDB_PATTERN_MATCH);

  str = "";
  ret = patternMatch("%_", str, strlen(str), &info);
  EXPECT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  str = "1";
  ret = patternMatch("%__", str, strlen(str), &info);
  EXPECT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  str = "";
  ret = patternMatch("%", str, strlen(str), &info);
  EXPECT_EQ(ret, TSDB_PATTERN_MATCH);

  str = " ";
  ret = patternMatch("_", str, strlen(str), &info);
  EXPECT_EQ(ret, TSDB_PATTERN_MATCH);

  str = "!";
  ret = patternMatch("%_", str, strlen(str), &info);
  EXPECT_EQ(ret, TSDB_PATTERN_MATCH);

  str = "abcdefg";
  ret = patternMatch("abc%fg", str, strlen(str), &info);
  EXPECT_EQ(ret, TSDB_PATTERN_MATCH);

  str = "abcdefgabcdeju";
  ret = patternMatch("abc%fg", str, 7, &info);
  EXPECT_EQ(ret, TSDB_PATTERN_MATCH);

  str = "abcdefgabcdeju";
  ret = patternMatch("abc%f_", str, 6, &info);
  EXPECT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  str = "abcdefgabcdeju";
  ret = patternMatch("abc%f_", str, 1, &info);  // pattern string is longe than the size
  EXPECT_EQ(ret, TSDB_PATTERN_NOMATCH);

  str = "abcdefgabcdeju";
  ret = patternMatch("ab", str, 2, &info);
  EXPECT_EQ(ret, TSDB_PATTERN_MATCH);

  str = "abcdefgabcdeju";
  ret = patternMatch("a%", str, 2, &info);
  EXPECT_EQ(ret, TSDB_PATTERN_MATCH);

  str = "abcdefgabcdeju";
  ret = patternMatch("a__", str, 2, &info);
  EXPECT_EQ(ret, TSDB_PATTERN_NOMATCH);
  
  str = "carzero";
  ret = patternMatch("%o", str, strlen(str), &info);
  EXPECT_EQ(ret, TSDB_PATTERN_MATCH);
  
  str = "19";
  ret = patternMatch("%9", str, 2, &info);
  EXPECT_EQ(ret, TSDB_PATTERN_MATCH);
}
