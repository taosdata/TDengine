/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
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
#include <iostream>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wwrite-strings"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-variable"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wsign-compare"
#pragma GCC diagnostic ignored "-Wformat"
#pragma GCC diagnostic ignored "-Wint-to-pointer-cast"
#pragma GCC diagnostic ignored "-Wpointer-arith"

#include "os.h"
#include "tlog.h"

#ifdef WINDOWS
TEST(osStringTests, strsepNormalInput) {
  char       str[] = "This is a test string.";
  char *     ptr = str;
  char *     tok = NULL;
  const char delim[] = " ";

  while ((tok = strsep(&ptr, delim)) != NULL) {
    printf("%s\n", tok);
  }
  EXPECT_STREQ(tok, nullptr);
  EXPECT_EQ(ptr, nullptr);
}

TEST(osStringTests, strsepEmptyInput) {
  char*      str = "";
  char*      ptr = str;
  char*      tok = NULL;
  const char delim[] = " ";

  while ((tok = strsep(&ptr, delim)) != NULL) {
    printf("%s\n", tok);
  }

  EXPECT_STREQ(tok, nullptr);
  EXPECT_EQ(ptr, nullptr);
}

TEST(osStringTests, strsepNullInput) {
  char *     str = NULL;
  char *     ptr = str;
  char *     tok = NULL;
  const char delim[] = " ";

  while ((tok = strsep(&ptr, delim)) != NULL) {
    printf("%s\n", tok);
  }

  EXPECT_STREQ(tok, nullptr);
  EXPECT_EQ(ptr, nullptr);
}

TEST(osStringTests, strndupNormalInput) {
  const char s[] = "This is a test string.";
  int        size = strlen(s) + 1;
  char *     s2 = strndup(s, size);

  EXPECT_STREQ(s, s2);

  free(s2);
}
#endif

TEST(osStringTests, osUcs4Tests1) {
  TdUcs4 f1_ucs4[] = {0x0048, 0x0065, 0x006C, 0x006C, 0x006F, 0x0000};
  TdUcs4 f2_ucs4[] = {0x0048, 0x0065, 0x006C, 0x006C, 0x006F, 0x0000};

  EXPECT_EQ(tasoUcs4Compare(f1_ucs4, f2_ucs4, sizeof(f1_ucs4)), 0);

  TdUcs4 f3_ucs4[] = {0x0048, 0x0065, 0x006C, 0x006C, 0x006F, 0x0020, 0x0077,
                      0x006F, 0x0072, 0x006C, 0x0064, 0x0021, 0x0000};
  TdUcs4 f4_ucs4[] = {0x0048, 0x0065, 0x006C, 0x006C, 0x006F, 0x0000};

  EXPECT_GT(tasoUcs4Compare(f3_ucs4, f4_ucs4, sizeof(f3_ucs4)), 0);

  TdUcs4 f5_ucs4[] = {0x0048, 0x0065, 0x006C, 0x006C, 0x006F, 0x0000};
  TdUcs4 f6_ucs4[] = {0x0048, 0x0065, 0x006C, 0x006C, 0x006F, 0x0020, 0x0077,
                      0x006F, 0x0072, 0x006C, 0x0064, 0x0021, 0x0000};

  EXPECT_LT(tasoUcs4Compare(f5_ucs4, f6_ucs4, sizeof(f5_ucs4)), 0);
}

TEST(osStringTests, osUcs4lenTests2) {
  TdUcs4 ucs4_1[] = {'H', 'e', 'l', 'l', 'o', '\0'};
  EXPECT_EQ(taosUcs4len(ucs4_1), 5);

  TdUcs4 ucs4_2[] = {'\0'};
  EXPECT_EQ(taosUcs4len(ucs4_2), 0);

  TdUcs4 ucs4_3[] = {'C', 'h', 'i', 'n', 'a', 0x4E2D, 0x6587, '\0'};
  EXPECT_EQ(taosUcs4len(ucs4_3), 7);
}
