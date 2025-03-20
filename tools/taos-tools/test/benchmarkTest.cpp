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


// lower
char* strToLowerCopy(const char *str) {
  if (str == NULL) {
      return NULL;
  }
  size_t len = strlen(str);
  char *result = (char*)malloc(len + 1);
  if (result == NULL) {
      return NULL;
  }
  for (size_t i = 0; i < len; i++) {
      result[i] = tolower((unsigned char)str[i]);
  }
  result[len] = '\0';
  return result;
}

// pase dsn
int32_t parseDsn(char* dsn, char **host, char **port, char **user, char **pwd);

TEST(jsonTest, strToLowerCopy) {
  // strToLowerCopy
  const char* arr[][2] = {
    {"ABC","abc"},
    {"Http://Localhost:6041","http://localhost:6041"},
    {"DEF","def"}
  };

  int rows = sizeof(arr) / sizeof(arr[0]);
  for (int i = 0; i < rows; i++) {
    char *p1 = (char *)arr[i][1];
    char *p2 = strToLowerCopy((char *)arr[i][0]);
    printf("p1: %s\n", p1);
    printf("p2: %s\n", p2);
    int32_t cmp = strcmp(p1, p2);
    if (p2) {
      free(p2);
    }    
    ASSERT_EQ(cmp, 0);
  }

  // null
  char * p = strToLowerCopy(NULL);
  ASSERT_EQ(p, nullptr);
}

int main(int argc, char **argv) {
  printf("Hello world taosBenchmark unit test for C \n");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}