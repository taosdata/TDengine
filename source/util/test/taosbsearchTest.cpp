/*
 * Copyright (c) 2020 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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

#include "talgo.h"

static int compareFunc(const void *arg1, const void *arg2) { return (*(int *)arg1) - (*(int *)arg2); }

TEST(testCase, taosbsearch_equal) {
  // For equal test
  int   key = 3;
  void *pRet = NULL;

  pRet = taosbsearch((void *)&key, NULL, 0, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(pRet, nullptr);

  // 1 element
  int array1[1] = {5};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(pRet, nullptr);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(pRet, nullptr);

  key = 5;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_EQ);
  ASSERT_NE(pRet, nullptr);
  ASSERT_EQ(*(int *)pRet, key);

  // 2 element
  int array2[2] = {3, 6};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(pRet, nullptr);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(*(int *)pRet, key);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(pRet, nullptr);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(pRet, nullptr);

  // 3 element
  int array3[3] = {3, 6, 8};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(pRet, nullptr);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(pRet, nullptr);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(pRet, nullptr);

  key = 8;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 9;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_EQ);
  ASSERT_EQ(pRet, nullptr);
}

TEST(testCase, taosbsearch_greater_or_equal) {
  // For equal test
  int   key = 3;
  void *pRet = NULL;

  pRet = taosbsearch((void *)&key, NULL, 0, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(pRet, nullptr);

  // 1 element
  int array1[1] = {5};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 5);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(pRet, nullptr);

  key = 5;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_GE);
  ASSERT_NE(pRet, nullptr);
  ASSERT_EQ(*(int *)pRet, 5);

  // 2 element
  int array2[2] = {3, 6};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(pRet, nullptr);

  // 3 element
  int array3[3] = {3, 6, 8};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 8;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 9;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(pRet, nullptr);

  // 4 element
  int array4[4] = {3, 6, 8, 11};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 8;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 9;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 11);

  key = 11;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 11);

  key = 13;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(pRet, nullptr);

  // 5 element
  int array5[5] = {3, 6, 8, 11, 15};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 8;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 9;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 11);

  key = 11;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 11);

  key = 13;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 15);

  key = 15;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(*(int *)pRet, 15);

  key = 17;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GE);
  ASSERT_EQ(pRet, nullptr);
}

TEST(testCase, taosbsearch_greater) {
  // For equal test
  int   key = 3;
  void *pRet = NULL;

  pRet = taosbsearch((void *)&key, NULL, 0, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(pRet, nullptr);

  // 1 element
  int array1[1] = {5};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 5);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(pRet, nullptr);

  key = 5;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(pRet, nullptr);

  // 2 element
  int array2[2] = {3, 6};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(pRet, nullptr);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(pRet, nullptr);

  // 3 element
  int array3[3] = {3, 6, 8};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 8;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(pRet, nullptr);

  key = 9;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(pRet, nullptr);

  // 4 element
  int array4[4] = {3, 6, 8, 11};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 8;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 11);

  key = 9;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 11);

  key = 11;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(pRet, nullptr);

  key = 13;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(pRet, nullptr);

  // 5 element
  int array5[5] = {3, 6, 8, 11, 15};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 8;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 11);

  key = 9;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 11);

  key = 11;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 15);

  key = 13;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(*(int *)pRet, 15);

  key = 15;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(pRet, nullptr);

  key = 17;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_GT);
  ASSERT_EQ(pRet, nullptr);
}

TEST(testCase, taosbsearch_less_or_equal) {
  // For equal test
  int   key = 3;
  void *pRet = NULL;

  pRet = taosbsearch((void *)&key, NULL, 0, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(pRet, nullptr);

  // 1 element
  int array1[1] = {5};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(pRet, nullptr);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 5);

  key = 5;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_LE);
  ASSERT_NE(pRet, nullptr);
  ASSERT_EQ(*(int *)pRet, 5);

  // 2 element
  int array2[2] = {3, 6};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(pRet, nullptr);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 6);

  // 3 element
  int array3[3] = {3, 6, 8};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(pRet, nullptr);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 8;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 9;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 8);

  // 4 element
  int array4[4] = {3, 6, 8, 11};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(pRet, nullptr);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 8;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 9;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 11;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 11);

  key = 13;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 11);

  // 5 element
  int array5[5] = {3, 6, 8, 11, 15};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(pRet, nullptr);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 8;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 9;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 11;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 11);

  key = 13;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 11);

  key = 15;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 15);

  key = 17;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LE);
  ASSERT_EQ(*(int *)pRet, 15);
}

TEST(testCase, taosbsearch_less) {
  // For equal test
  int   key = 3;
  void *pRet = NULL;

  pRet = taosbsearch((void *)&key, NULL, 0, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(pRet, nullptr);

  // 1 element
  int array1[1] = {5};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(pRet, nullptr);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 5);

  key = 5;
  pRet = taosbsearch((void *)&key, (void *)array1, 1, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(pRet, nullptr);

  // 2 element
  int array2[2] = {3, 6};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(pRet, nullptr);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(pRet, nullptr);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array2, 2, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 6);

  // 3 element
  int array3[3] = {3, 6, 8};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(pRet, nullptr);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(pRet, nullptr);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 8;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 9;
  pRet = taosbsearch((void *)&key, (void *)array3, 3, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 8);

  // 4 element
  int array4[4] = {3, 6, 8, 11};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(pRet, nullptr);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(pRet, nullptr);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 8;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 9;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 11;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 13;
  pRet = taosbsearch((void *)&key, (void *)array4, 4, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 11);

  // 5 element
  int array5[5] = {3, 6, 8, 11, 15};

  key = 1;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(pRet, nullptr);

  key = 3;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(pRet, nullptr);

  key = 4;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 6;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 3);

  key = 7;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 8;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 6);

  key = 9;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 11;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 8);

  key = 13;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 11);

  key = 15;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 11);

  key = 17;
  pRet = taosbsearch((void *)&key, (void *)array5, 5, sizeof(int), compareFunc, TD_LT);
  ASSERT_EQ(*(int *)pRet, 15);
}