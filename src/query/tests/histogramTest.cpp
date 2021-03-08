#include <gtest/gtest.h>
#include <sys/time.h>
#include <cassert>
#include <iostream>

#include "taos.h"
#include "qHistogram.h"
namespace {
void doHistogramAddTest() {
  SHistogramInfo* pHisto = NULL;

  /**
   * use arrayList, elapsed time is:
   * before:
   * 10,000,000  45sec, bin:1000 (-O0) / 17sec. bin:1000, (-O3)
   *
   * after:
   *
   */
  struct timeval systemTime;
  gettimeofday(&systemTime, NULL);
  int64_t st =
      (int64_t)systemTime.tv_sec * 1000L + (uint64_t)systemTime.tv_usec / 1000;
  for (int32_t i = 0; i < 10000; ++i) {
    tHistogramAdd(&pHisto, i);
    //        tHistogramPrint(pHisto);
  }
  //
  gettimeofday(&systemTime, NULL);
  int64_t et =
      (int64_t)systemTime.tv_sec * 1000L + (uint64_t)systemTime.tv_usec / 1000;
  printf("total elapsed time: %ld\n", et - st);

  printf("elements: %d, slot:%d \n", pHisto->numOfElems, pHisto->numOfEntries);
  tHistogramPrint(pHisto);

  printf("%ld\n", tHistogramSum(pHisto, 1.5));
  printf("%ld\n", tHistogramSum(pHisto, 2));
  printf("%ld\n", tHistogramSum(pHisto, 3));
  printf("%ld\n", tHistogramSum(pHisto, 4));
  printf("%ld\n", tHistogramSum(pHisto, 5));
  printf("%ld\n", tHistogramSum(pHisto, 6));

  for (int32_t i = 399; i < 400; ++i) {
    printf("val:%d, %ld\n", i, tHistogramSum(pHisto, i));
  }

  double ratio[] = {0 / 100, 20.0 / 100, 88.0 / 100, 100 / 100};
  double* res = tHistogramUniform(pHisto, ratio, 4);
  for (int32_t i = 0; i < 4; ++i) {
    printf("%f\n", res[i]);
  }

  SHistogramInfo* pHisto1 = NULL;
  for (int32_t i = (90000 - 1); i >= 80000; --i) {
    tHistogramAdd(&pHisto1, i);
  }
  tHistogramPrint(pHisto1);

  SHistogramInfo* pRes = tHistogramMerge(pHisto1, pHisto, MAX_HISTOGRAM_BIN);
  assert(pRes->numOfElems == pHisto->numOfElems + pHisto1->numOfElems);
  tHistogramPrint(pRes);

  tHistogramDestroy(&pHisto);
  tHistogramDestroy(&pHisto1);
  tHistogramDestroy(&pRes);
  free(res);
}
void doHistogramRepeatTest() {
  SHistogramInfo* pHisto = NULL;
  struct timeval systemTime;

  gettimeofday(&systemTime, NULL);
  int64_t st =
      (int64_t)systemTime.tv_sec * 1000L + (uint64_t)systemTime.tv_usec / 1000;

  for (int32_t i = 0; i < 1000; ++i) {
    tHistogramAdd(&pHisto, -24 + i);
    //        tHistogramPrint(pHisto);
  }

  tHistogramDestroy(&pHisto);

}
}

/* test validate the names for table/database */
TEST(testCase, histogram_binary_search) {
  SHistogramInfo* pHisto = tHistogramCreate(MAX_HISTOGRAM_BIN);

  pHisto->numOfEntries = 10;
  for (int32_t i = 0; i < 10; ++i) {
    pHisto->elems[i].num = 1;
    pHisto->elems[i].val = i;
  }

  int32_t idx = histoBinarySearch(pHisto->elems, pHisto->numOfEntries, 1);
  assert(idx == 1);

  idx = histoBinarySearch(pHisto->elems, pHisto->numOfEntries, 9);
  assert(idx == 9);

  idx = histoBinarySearch(pHisto->elems, pHisto->numOfEntries, 20);
  assert(idx == 10);

  idx = histoBinarySearch(pHisto->elems, pHisto->numOfEntries, -1);
  assert(idx == 0);

  idx = histoBinarySearch(pHisto->elems, pHisto->numOfEntries, 3.9);
  assert(idx == 4);

  free(pHisto);
}

TEST(testCase, histogram_add) {
  doHistogramAddTest();
  doHistogramRepeatTest();
}

TEST(testCase, heapsort) {
  //    int32_t num = 20;
  //
  //    SHeapEntry* pEntry = tHeapCreate(num);
  //
  //    for(int32_t i=0; i<num; ++i) {
  //        pEntry[i].val = num - 1 - i;
  //    }
  //
  //    tHeapSort(pEntry, num);
  //
  //    for(int32_t i=0; i<num; ++i) {
  //        printf("%lf, ", pEntry[i].val);
  //    }
  //
  //    printf("\n");
  //
  //    free(pEntry);
}
