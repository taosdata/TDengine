#include <gtest/gtest.h>
#include <stdlib.h>
#include "talgo.h"

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

TEST(utilTest, taosMSort) {
  // Create an array of test data
  TestStruct arr[] = {{4, 2.5}, {3, 6}, {2, 1.5}, {3, 2}, {1, 3.5}, {3, 5}};

  // Sort the array using taosSort
  taosMergeSort(arr, 6, sizeof(TestStruct), cmpFunc);

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

TEST(utilTest, taosMSort2) {
  clock_t start_time, end_time;
  double  cpu_time_used;

  int times = 10000;
  start_time = clock();
  for (int i = 0; i < 10000; i++) {
    TestStruct arr[] = {{4, 2.5}, {3, 6}, {2, 1.5}, {3, 2}, {1, 3.5}, {3, 5}};
    taosMergeSort(arr, 6, sizeof(TestStruct), cmpFunc);
  }
  end_time = clock();
  cpu_time_used = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
  printf("taosMSort %d times: %f s\n", times, cpu_time_used);

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
  taosMergeSort(data1, arraySize, sizeof(int), cmpInt);
  end_time = clock();
  cpu_time_used = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
  printf("taosMSort length:%d  cost: %f s\n", arraySize, cpu_time_used);

  start_time = clock();
  taosSort(data2, arraySize, sizeof(int), cmpInt);
  end_time = clock();
  cpu_time_used = ((double)(end_time - start_time)) / CLOCKS_PER_SEC;
  printf("taosSort length:%d  cost: %f s\n", arraySize, cpu_time_used);

  for (int i = 0; i < arraySize - 1; i++) {
    EXPECT_EQ(data1[i], data2[i]);
    ASSERT_LE(data1[i], data1[i+1]);
  }
}
