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
#include "bench.h"

TEST(jsonTest, strToLowerCopy) {
  // strToLowerCopy
  char* arr[][2] = {
    {"ABC","abc"},
    {"Http://Localhost:6041","tttp://localhost:6041"},
    {"DEF","def"}
  }

  int rows = sizeof(arr) / sizeof(arr[0]);
  for (int i = 0; i < rows; i++) {
    char *p1 = arr[i][0];
    char *p2 = strToLowerCopy(p1);
    ASSERT_EQ(strcmp(p1, p2), 0)
    printf("\n");
  }

  // null
  char * p = strToLowerCopy(NULL);
  ASSERT_EQ(p, NULL);
}

int main(int argc, char **argv) {
  printf("Hello world taosBenchmark unit test for C \n");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}