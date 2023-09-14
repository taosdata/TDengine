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

struct TestStruct {
  int   a;
  float b;
};

// Define a custom comparison function for testing
int cmpFunc(const void* a, const void* b) {
  const TestStruct* pa = reinterpret_cast<const TestStruct*>(a);
  const TestStruct* pb = reinterpret_cast<const TestStruct*>(b);
  if (pa->a < pb->a) {
    return -1;
  } else if (pa->a > pb->a) {
    return 1;
  } else {
    return 0;
  }
}

TEST(osMathTests, taosSort) {
  // Create an array of test data
  TestStruct arr[] = {{4, 2.5}, {2, 1.5}, {1, 3.5}, {3, 4.5}};

  // Sort the array using taosSort
  taosSort(arr, 4, sizeof(TestStruct), cmpFunc);

  // Check that the array is sorted correctly
  EXPECT_EQ(arr[0].a, 1);
  EXPECT_EQ(arr[1].a, 2);
  EXPECT_EQ(arr[2].a, 3);
  EXPECT_EQ(arr[3].a, 4);
}

TEST(osMathTests, taosMSort) {
  // Create an array of test data
  TestStruct arr[] = {{4, 2.5}, {3, 6}, {2, 1.5}, {3, 2}, {1, 3.5}, {3, 5}};

  // Sort the array using taosSort
  taosMSort(arr, 6, sizeof(TestStruct), cmpFunc);

  for (int i = 0; i < sizeof(arr) / sizeof(TestStruct); i++) {
    printf("%d: %d  %f\n", i, arr[i].a, arr[i].b);
  }

  // Check that the array is sorted correctly
  EXPECT_EQ(arr[0].a, 1);
  EXPECT_EQ(arr[1].a, 2);
  EXPECT_EQ(arr[2].a, 3);
  EXPECT_EQ(arr[2].b, 6);
  EXPECT_EQ(arr[3].a, 3);
  EXPECT_EQ(arr[3].b, 2);
  EXPECT_EQ(arr[4].a, 3);
  EXPECT_EQ(arr[4].b, 5);
  EXPECT_EQ(arr[5].a, 4);
}

int cmpInt(const void* a, const void* b) {
  int int_a = *((int*)a);
  int int_b = *((int*)b);

  if (int_a == int_b)
    return 0;
  else if (int_a < int_b)
    return -1;
  else
    return 1;
}

TEST(osMathTests, taosMSort2) {
  clock_t start_time, end_time;
  double  cpu_time_used;

  int times = 10000;
  start_time = clock();
  for (int i = 0; i < 10000; i++) {
    TestStruct arr[] = {{4, 2.5}, {3, 6}, {2, 1.5}, {3, 2}, {1, 3.5}, {3, 5}};
    taosMSort(arr, 6, sizeof(TestStruct), cmpFunc);
  }
  end_time = clock();
  cpu_time_used = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
  printf("taosMSort %d times: %f s\n",times, cpu_time_used);

  start_time = clock();
  for (int i = 0; i < 10000; i++) {
    TestStruct arr[] = {{4, 2.5}, {3, 6}, {2, 1.5}, {3, 2}, {1, 3.5}, {3, 5}};
    taosSort(arr, 6, sizeof(TestStruct), cmpFunc);
  }
  end_time = clock();
  cpu_time_used = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
  printf("taosSort %d times: %f s\n", times, cpu_time_used);

  const int arraySize = 1000000;
  int       data1[arraySize];
  int       data2[arraySize];
  for (int i = 0; i < arraySize; ++i) {
    data1[i] = taosRand();
    data2[i] = data1[i];
  }
  start_time = clock();
  taosMSort(data1, arraySize, sizeof(int), cmpInt);
  end_time = clock();
  cpu_time_used = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
  printf("taosMSort length:%d  cost %f s\n", arraySize, cpu_time_used);

  start_time = clock();
  taosSort(data2, arraySize, sizeof(int), cmpInt);
  end_time = clock();
  cpu_time_used = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
  printf("taosSort length:%d  cost %f s\n", arraySize, cpu_time_used);

}
