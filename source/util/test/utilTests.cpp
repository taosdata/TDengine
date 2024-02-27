#include <gtest/gtest.h>
#include <stdlib.h>
#include <tutil.h>
#include <random>

#include "tarray.h"
#include "tcompare.h"

namespace {
}  // namespace

TEST(utilTest, wchar_pattern_match_test) {
  const TdWchar* pattern = L"%1";

  int32_t ret = 0;
  SPatternCompareInfo pInfo = PATTERN_COMPARE_INFO_INITIALIZER;

  const TdWchar* str0 = L"14";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern), 2, reinterpret_cast<const TdUcs4*>(str0), wcslen(str0), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const TdWchar* str1 = L"11";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern), 2, reinterpret_cast<const TdUcs4*>(str1), wcslen(str1), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* str2 = L"41";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern), 2, reinterpret_cast<const TdUcs4*>(str2), wcslen(str2), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* pattern3 = L"%_";
  const TdWchar* str3 = L"88";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern3), 2, reinterpret_cast<const TdUcs4*>(str3), wcslen(str3), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* pattern4 = L"%___";
  const TdWchar* str4 = L"88";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern4), 4, reinterpret_cast<const TdUcs4*>(str4), wcslen(str4), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const TdWchar* pattern5 = L"%___";
  const TdWchar* str5 = L"883391";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern5), 4, reinterpret_cast<const TdUcs4*>(str5), wcslen(str5), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* pattern6 = L"%___66";
  const TdWchar* str6 = L"88339166";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern6), 6, reinterpret_cast<const TdUcs4*>(str6), wcslen(str6), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* pattern7 = L"%____66";
  const TdWchar* str7 = L"66166";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern7), 7, reinterpret_cast<const TdUcs4*>(str7), wcslen(str7), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const TdWchar* pattern8 = L"6%____66";
  const TdWchar* str8 = L"666166";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern8), 8, reinterpret_cast<const TdUcs4*>(str8), wcslen(str8), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const TdWchar* pattern9 = L"6\\__6";
  const TdWchar* str9 = L"6_66";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern9), 6, reinterpret_cast<const TdUcs4*>(str9), wcslen(str9), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* pattern10 = L"%";
  const TdWchar* str10 = L"";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern10), 1, reinterpret_cast<const TdUcs4*>(str10), 0, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* pattern11 = L"china%";
  const TdWchar* str11 = L"CHI ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern11), 6, reinterpret_cast<const TdUcs4*>(str11), 3, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOMATCH);

  const TdWchar* pattern12 = L"abc%";
  const TdWchar* str12 = L"";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern12), 4, reinterpret_cast<const TdUcs4*>(str12), 0, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOMATCH);

  const TdWchar* pattern13 = L"%\\_6 ";
  const TdWchar* str13 = L"6a6 ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern13), 6, reinterpret_cast<const TdUcs4*>(str13), 4, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const TdWchar* pattern14 = L"%\\%6 ";
  const TdWchar* str14 = L"6a6 ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern14), 6, reinterpret_cast<const TdUcs4*>(str14), 4, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);
}

TEST(utilTest, wchar_pattern_match_no_terminated) {
  const TdWchar* pattern = L"%1  ";

  int32_t ret = 0;
  SPatternCompareInfo pInfo = PATTERN_COMPARE_INFO_INITIALIZER;

  const TdWchar* str0 = L"14 ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern), 2, reinterpret_cast<const TdUcs4*>(str0), 2, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const TdWchar* str1 = L"11 ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern), 2, reinterpret_cast<const TdUcs4*>(str1), 2, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* str2 = L"41 ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern), 2, reinterpret_cast<const TdUcs4*>(str2), 2, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* pattern3 = L"%_  ";
  const TdWchar* str3 = L"88 ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern3), 2, reinterpret_cast<const TdUcs4*>(str3), 2, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* pattern4 = L"%___  ";
  const TdWchar* str4 = L"88  ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern4), 4, reinterpret_cast<const TdUcs4*>(str4), 2, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const TdWchar* pattern5 = L"%___  ";
  const TdWchar* str5 = L"883391  ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern5), 4, reinterpret_cast<const TdUcs4*>(str5), 6, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* pattern6 = L"%___66  ";
  const TdWchar* str6 = L"88339166  ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern6), 6, reinterpret_cast<const TdUcs4*>(str6), 8, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* pattern7 = L"%____66  ";
  const TdWchar* str7 = L"66166 ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern7), 7, reinterpret_cast<const TdUcs4*>(str7), 5, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const TdWchar* pattern8 = L"6%____66  ";
  const TdWchar* str8 = L"666166 ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern8), 8, reinterpret_cast<const TdUcs4*>(str8), 6, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const TdWchar* pattern9 = L"6\\_6 ";
  const TdWchar* str9 = L"6_6 ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern9), 6, reinterpret_cast<const TdUcs4*>(str9), 4, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* pattern10 = L"% ";
  const TdWchar* str10 = L"6_6 ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern10), 2, reinterpret_cast<const TdUcs4*>(str10), 4, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* pattern11 = L"%\\_6 ";
  const TdWchar* str11 = L"6_6 ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern11), 6, reinterpret_cast<const TdUcs4*>(str11), 4, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const TdWchar* pattern12 = L"%\\%6 ";
  const TdWchar* str12 = L"6%6 ";
  ret = wcsPatternMatch(reinterpret_cast<const TdUcs4*>(pattern12), 6, reinterpret_cast<const TdUcs4*>(str12), 4, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);
}

TEST(utilTest, char_pattern_match_test) {
  const char* pattern = "%1";

  int32_t ret = 0;
  SPatternCompareInfo pInfo = PATTERN_COMPARE_INFO_INITIALIZER;

  const char* str0 = "14";
  ret = patternMatch(pattern, 2, str0, strlen(str0), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const char* str1 = "11";
  ret = patternMatch(pattern, 2, str1, strlen(str1), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* str2 = "41";
  ret = patternMatch(pattern, 2, str2, strlen(str2), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern3 = "%_";
  const char* str3 = "88";
  ret = patternMatch(pattern3, 2, str3, strlen(str3), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern4 = "%___";
  const char* str4 = "88";
  ret = patternMatch(pattern4, 4, str4, strlen(str4), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const char* pattern5 = "%___";
  const char* str5 = "883391";
  ret = patternMatch(pattern5, 4, str5, strlen(str5), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern6 = "%___66";
  const char* str6 = "88339166";
  ret = patternMatch(pattern6, 6, str6, strlen(str6), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern7 = "%____66";
  const char* str7 = "66166";
  ret = patternMatch(pattern7, 7, str7, strlen(str7), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const char* pattern8 = "6%____66";
  const char* str8 = "666166";
  ret = patternMatch(pattern8, 8, str8, strlen(str8), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const char* pattern9 = "6\\_6";
  const char* str9 = "6_6";
  ret = patternMatch(pattern9, 5, str9, strlen(str9), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern10 = "%";
  const char* str10 = " ";
  ret = patternMatch(pattern10, 1, str10, 0, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern11 = "china%";
  const char* str11 = "abc ";
  ret = patternMatch(pattern11, 6, str11, 3, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOMATCH);

  const char* pattern12 = "abc%";
  const char* str12 = NULL;
  ret = patternMatch(pattern12, 4, str12, 0, &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOMATCH);

  const char* pattern13 = "a\\%c";
  const char* str13 = "a%c";
  ret = patternMatch(pattern13, 5, str13, strlen(str13), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern14 = "%a\\%c";
  const char* str14 = "a%c";
  ret = patternMatch(pattern14, strlen(pattern14), str14, strlen(str14), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern15 = "_a\\%c";
  const char* str15 = "ba%c";
  ret = patternMatch(pattern15, strlen(pattern15), str15, strlen(str15), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern16 = "_\\%c";
  const char* str16 = "a%c";
  ret = patternMatch(pattern16, strlen(pattern16), str16, strlen(str16), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern17 = "_\\%c";
  const char* str17 = "ba%c";
  ret = patternMatch(pattern17, strlen(pattern17), str17, strlen(str17), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOMATCH);

  const char* pattern18 = "%\\%c";
  const char* str18 = "abc";
  ret = patternMatch(pattern18, strlen(pattern18), str18, strlen(str18), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const char* pattern19 = "%\\_c";
  const char* str19 = "abc";
  ret = patternMatch(pattern19, strlen(pattern19), str19, strlen(str19), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);
}

TEST(utilTest, char_pattern_match_no_terminated) {
  const char* pattern = "%1  ";

  int32_t ret = 0;
  SPatternCompareInfo pInfo = PATTERN_COMPARE_INFO_INITIALIZER;

  const char* str0 = "14";
  ret = patternMatch(pattern, 2, str0, strlen(str0), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const char* str1 = "11";
  ret = patternMatch(pattern, 2, str1, strlen(str1), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* str2 = "41";
  ret = patternMatch(pattern, 2, str2, strlen(str2), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern3 = "%_  ";
  const char* str3 = "88";
  ret = patternMatch(pattern3, 2, str3, strlen(str3), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern4 = "%___  ";
  const char* str4 = "88";
  ret = patternMatch(pattern4, 4, str4, strlen(str4), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const char* pattern5 = "%___  ";
  const char* str5 = "883391";
  ret = patternMatch(pattern5, 4, str5, strlen(str5), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern6 = "%___66  ";
  const char* str6 = "88339166";
  ret = patternMatch(pattern6, 6, str6, strlen(str6), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern7 = "%____66  ";
  const char* str7 = "66166";
  ret = patternMatch(pattern7, 7, str7, strlen(str7), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const char* pattern8 = "6%____66  ";
  const char* str8 = "666166";
  ret = patternMatch(pattern8, 8, str8, strlen(str8), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_NOWILDCARDMATCH);

  const char* pattern9 = "6\\_6  ";
  const char* str9 = "6_6";
  ret = patternMatch(pattern9, 4, str9, strlen(str9), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);

  const char* pattern10 = "% ";
  const char* str10 = "6_6";
  ret = patternMatch(pattern10, 1, str10, strlen(str10), &pInfo);
  ASSERT_EQ(ret, TSDB_PATTERN_MATCH);
}

TEST(utilTest, tstrncspn) {
  const char* p1 = "abc";
  const char* reject = "d";
  size_t v = tstrncspn(p1, strlen(p1), reject, 1);
  ASSERT_EQ(v, 3);

  const char* reject1 = "a";
  v = tstrncspn(p1, strlen(p1), reject1, 1);
  ASSERT_EQ(v, 0);

  const char* reject2 = "de";
  v = tstrncspn(p1, strlen(p1), reject2, 2);
  ASSERT_EQ(v, 3);

  const char* p2 = "abcdefghijklmn";
  v = tstrncspn(p2, strlen(p2), reject2, 2);
  ASSERT_EQ(v, 3);

  const char* reject3 = "12345n";
  v = tstrncspn(p2, strlen(p2), reject3, 6);
  ASSERT_EQ(v, 13);

  const char* reject4 = "";
  v = tstrncspn(p2, strlen(p2), reject4, 0);
  ASSERT_EQ(v, 14);

  const char* reject5 = "911";
  v = tstrncspn(p2, strlen(p2), reject5, 0);
  ASSERT_EQ(v, 14);

  const char* reject6 = "Kk";
  v = tstrncspn(p2, strlen(p2), reject6, 2);
  ASSERT_EQ(v, 10);
}

TEST(utilTest, intToHextStr) {
  char buf[64] = {0};

  int64_t v = 0;
  tintToHex(0, buf);
  ASSERT_STREQ(buf, "0");

  v = 100000000;
  tintToHex(v, buf);

  char destBuf[128];
  sprintf(destBuf, "%" PRIx64, v);
  ASSERT_STREQ(buf, destBuf);

  taosSeedRand(taosGetTimestampSec());

  for(int32_t i = 0; i < 100000; ++i) {
    memset(buf, 0, tListLen(buf));
    memset(destBuf, 0, tListLen(destBuf));

    v = taosRand();
    tintToHex(v, buf);

    sprintf(destBuf, "%" PRIx64, v);
    ASSERT_STREQ(buf, destBuf);
  }
}
