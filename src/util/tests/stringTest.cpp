#include <gtest/gtest.h>
#include <limits.h>
#include <taosdef.h>
#include <iostream>

#include "taos.h"
#include "tutil.h"

TEST(testCase, str_escape_test) {
  char    t1[] = "\"\\\".dd";
  int32_t len = strDealWithEscape(t1, strlen(t1));
  printf("t1:%s, len:%d\n", t1, len);
  EXPECT_EQ(5, len);
  EXPECT_STRCASEEQ(t1, "\"\".dd");

  char    t2[] = "'\\\'.dd";
   len = strDealWithEscape(t2, strlen(t2));
  printf("t2:%s, len:%d\n", t2, len);
  EXPECT_EQ(5, len);
  EXPECT_STRCASEEQ(t2, "''.dd");

  char    t3[] = "\\\\.dd";
   len = strDealWithEscape(t3, strlen(t3));
  printf("t3:%s, len:%d\n", t3, len);
  EXPECT_EQ(4, len);
  EXPECT_STRCASEEQ(t3, "\\.dd");

  char    t4[] = "'\\n.dd";
   len = strDealWithEscape(t4, strlen(t4));
  printf("t4:%s, len:%d\n", t4, len);
  EXPECT_EQ(4, len);
  EXPECT_STRCASEEQ(t4, "\n.dd");

//  char    t2[] = "\"fsd\\\"fs\".dd";
//  len = strDealWithEscape(t2, strlen(t2));
//  printf("t2:%s, len:%d\n", t2, len);
//  EXPECT_EQ(11, len);
//  EXPECT_STRCASEEQ(t2, "\"fsd\"fs\".dd");
//
//  char    t3[] = "fs\\_d\\%.d\\d";
//  len = strRmquote(t3, strlen(t3));
//  printf("t3:%s, len:%d\n", t3, len);
//  EXPECT_EQ(10, len);
//  EXPECT_STRCASEEQ(t3, "fs\\_d\\%.dd");
//
//  char    t4[] = "\"fs\\_d\\%\".dd";
//  len = strRmquote(t4, strlen(t4));
//  printf("t4:%s, len:%d\n", t4, len);
//  EXPECT_EQ(10, len);
//  EXPECT_STRCASEEQ(t4, "fs\\_d\\%.dd");
//
//  char    t5[] = "\"fs\\_d\\%\"";
//  len = strRmquote(t5, strlen(t5));
//  printf("t5:%s, len:%d\n", t5, len);
//  EXPECT_EQ(7, len);
//  EXPECT_STRCASEEQ(t5, "fs\\_d\\%");
//
//  char    t6[] = "'fs\\_d\\%'";
//  len = strRmquote(t6, strlen(t6));
//  printf("t6:%s, len:%d\n", t6, len);
//  EXPECT_EQ(7, len);
//  EXPECT_STRCASEEQ(t6, "fs\\_d\\%");
}

TEST(testCase, string_dequote_test) {
  char    t1[] = "'ab''c'";
  int32_t len = stringProcess(t1, strlen(t1));

  EXPECT_EQ(4, len);
  EXPECT_STRCASEEQ(t1, "ab'c");

  char t2[] = "\"ab\"\"c\"";
  len = stringProcess(t2, strlen(t2));

  EXPECT_EQ(4, len);
  EXPECT_STRCASEEQ(t1, "ab\"c");

  char t3[] = "`ab``c`";
  len = stringProcess(t3, strlen(t3));

  EXPECT_EQ(3, len);
  EXPECT_STRCASEEQ(t1, "ab`c");

  char t21[] = " abc ";
  int32_t lx = strtrim(t21);

  EXPECT_STREQ("abc", t21);
  EXPECT_EQ(3, lx);
}

#if 0
TEST(testCase, string_replace_test) {
  char  t3[] = "abc01abc02abc";
  char* ret = strreplace(t3, "abc", "7");

  EXPECT_EQ(strlen(ret), 7);
  EXPECT_STREQ("7017027", ret);
  free(ret);

  char t4[] = "a01a02b03c04d05";
  ret = strreplace(t4, "0", "9999999999");

  EXPECT_EQ(strlen(ret), 5 * 10 + 10);
  EXPECT_STREQ("a99999999991a99999999992b99999999993c99999999994d99999999995", ret);
  free(ret);

  char t5[] = "abc";
  ret = strreplace(t5, "abc", "12345678901234567890");

  EXPECT_EQ(strlen(ret), 20);
  EXPECT_STREQ("12345678901234567890", ret);
  free(ret);

  char t6[] = "abc";
  ret = strreplace(t6, "def", "abc");

  EXPECT_EQ(strlen(ret), 3);
  EXPECT_STREQ("abc", ret);
  free(ret);

  char t7[] = "abcde000000000000001234";
  ret = strreplace(t7, "ab", "0000000");

  EXPECT_EQ(strlen(ret), 28);
  EXPECT_STREQ("0000000cde000000000000001234", ret);
  free(ret);

  char t8[] = "abc\ndef";
  char t[] = {10, 0};

  char    f1[] = "\\n";
  int32_t fx = strlen(f1);
  ret = strreplace(t8, "\n", "\\n");

  EXPECT_EQ(strlen(ret), 8);
  EXPECT_STREQ("abc\\ndef", ret);
  free(ret);

  char t9[] = "abc\\ndef";
  ret = strreplace(t9, "\\n", "\n");

  EXPECT_EQ(strlen(ret), 7);
  EXPECT_STREQ("abc\ndef", ret);
  free(ret);

  char t10[] = "abcdef";
  ret = strreplace(t10, "", "0");

  EXPECT_EQ(strlen(ret), 6);
  EXPECT_STREQ("abcdef", ret);
  free(ret);
}
#endif

TEST(testCase, string_tolower_test) {
  char t[1024] = {1};
  memset(t, 1, tListLen(t));

  const char* a1 = "ABC";
  strtolower(t, a1);
  EXPECT_STREQ(t, "abc");

  memset(t, 1, tListLen(t));
  const char* a2 = "ABC\'ABC\'D";
  strtolower(t, a2);
  EXPECT_STREQ(t, "abc\'ABC\'d");

  memset(t, 1, tListLen(t));
  const char* a3 = "";
  strtolower(t, a3);
  EXPECT_STREQ(t, "");

  memset(t, 1, tListLen(t));
  const char* a4 = "\"AbcDEF\"";
  strtolower(t, a4);
  EXPECT_STREQ(t, a4);

  memset(t, 1, tListLen(t));
  const char* a5 = "1234\"AbcDEF\"456";
  strtolower(t, a5);
  EXPECT_STREQ(t, a5);

  memset(t, 1, tListLen(t));
  const char* a6 = "1234";
  strtolower(t, a6);
  EXPECT_STREQ(t, a6);
}

TEST(testCase, string_strnchr_test) {
  char t[1024] = {0};
  memset(t, 1, tListLen(t));

  char a1[] = "AB.C";
  EXPECT_TRUE(strnchr(a1, '.', strlen(a1), true) != NULL);

  char a2[] = "abc.";
  EXPECT_TRUE(strnchr(a2, '.', strlen(a2), true) != NULL);

  char a8[] = "abc.";
  EXPECT_TRUE(strnchr(a8, '.', 1, true) == NULL);

  char a3[] = ".abc";
  EXPECT_TRUE(strnchr(a3, '.', strlen(a3), true) != NULL);

  char a4[] = "'.abc'";
  EXPECT_TRUE(strnchr(a4, '.', strlen(a4), true) == NULL);

  char a5[] = "'.abc.'abc";
  EXPECT_TRUE(strnchr(a5, '.', strlen(a5), true) == NULL);

  char a6[] = "0123456789.";
  EXPECT_TRUE(strnchr(a6, '.', strlen(a6), true) != NULL);

  char a7[] = "0123456789.";
  EXPECT_TRUE(strnchr(a7, '.', 3, true) == NULL);

  char a9[] = "0123456789.";
  EXPECT_TRUE(strnchr(a9, '.', 0, true) == NULL);

  char a10[] = "0123456789'.'";
  EXPECT_TRUE(strnchr(a10, '.', strlen(a10), true) == NULL);
}

// TEST(testCase, cache_resize_test) {
//   char a11[] = "abc'.'";
//   EXPECT_TRUE(strnchr(a11, '.', strlen(a11), false) != NULL);

//   char a12[] = "abc'-'";
//   EXPECT_TRUE(strnchr(a12, '-', strlen(a12), false) != NULL);

//   char a15[] = "abc'-'";
//   EXPECT_TRUE(strnchr(a15, '-', strlen(a15), true) == NULL);

//   char a13[] = "'-'";
//   EXPECT_TRUE(strnchr(a13, '-', strlen(a13), false) != NULL);

//   char a14[] = "'-'";
//   EXPECT_TRUE(strnchr(a14, '-', strlen(a14), true) == NULL);

//   char a16[] = "'-'.";
//   EXPECT_TRUE(strnchr(a16, '.', strlen(a16), true) != NULL);
// }